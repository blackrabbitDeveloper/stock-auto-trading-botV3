from __future__ import annotations

import logging
from datetime import timedelta, date

import pandas as pd
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import StrategyParams, MarketFilterConfig, UniverseConfig
from app.models.position import Position
from app.strategy import SIGNAL_GENERATORS, add_indicators, filter_universe, check_market_filter
from app.notifier.discord import DiscordNotifier

logger = logging.getLogger(__name__)


async def run_signal_job(
    session: AsyncSession,
    strategy_configs: dict[str, StrategyParams],
    market_filter_config: MarketFilterConfig,
    universe_config: UniverseConfig,
    notifier: DiscordNotifier,
):
    """Daily signal generation job (runs at 15:40)."""
    today = date.today()
    today_str = today.strftime("%Y-%m-%d")
    today_fmt = today.strftime("%Y%m%d")
    lookback_start = (today - timedelta(days=120)).strftime("%Y%m%d")

    logger.info(f"Signal job started for {today_str}")

    from pykrx import stock as pykrx_stock

    # Check market filter
    market_open = check_market_filter(market_filter_config, today_str)

    # Get all currently held/pending symbols across all strategies
    stmt = select(Position.symbol).where(Position.status.in_(["active", "pending_buy"]))
    existing_symbols = set((await session.execute(stmt)).scalars().all())

    alerts = {"date": today_str, "pending_buys": [], "pending_sells": []}

    for strategy_name, config in strategy_configs.items():
        gen_func = SIGNAL_GENERATORS.get(strategy_name)
        if not gen_func:
            logger.warning(f"No signal generator for {strategy_name}")
            continue

        # 1. Check exits for active positions of this strategy
        stmt = select(Position).where(
            Position.strategy == strategy_name,
            Position.status == "active",
        )
        active_positions = (await session.execute(stmt)).scalars().all()

        for pos in active_positions:
            try:
                df = pykrx_stock.get_market_ohlcv(lookback_start, today_fmt, pos.symbol)
                if df.empty:
                    continue

                df.columns = ["open", "high", "low", "close", "volume"]
                today_close = int(df["close"].iloc[-1])
                today_high = int(df["high"].iloc[-1])

                pos.holding_days += 1
                pos.peak_price = max(pos.peak_price or 0, today_high)
                pos.trail_price = int(pos.peak_price * (1 - config.trailing_stop_pct))

                exit_reason = _check_exit(pos, today_close, config)
                if exit_reason:
                    pos.status = "pending_sell"
                    pos.exit_reason = exit_reason
                    alerts["pending_sells"].append({
                        "symbol": pos.symbol,
                        "name": pos.name,
                        "exit_reason": exit_reason,
                        "return_pct": (today_close / pos.entry_price - 1) if pos.entry_price else 0,
                    })
            except Exception as e:
                logger.error(f"Exit check failed for {pos.symbol}: {e}")

        # 2. Generate new buy signals (if market allows)
        if not market_open:
            continue

        stmt = select(Position).where(
            Position.strategy == strategy_name,
            Position.status.in_(["active", "pending_buy"]),
        )
        current_count = len((await session.execute(stmt)).scalars().all())
        available_slots = config.max_positions - current_count

        if available_slots <= 0:
            continue

        try:
            # Load universe
            all_symbols = pykrx_stock.get_market_ticker_list(today_fmt, market="ALL")

            # Load OHLCV and add indicators
            data_map = {}
            name_map = {}
            for sym in all_symbols:
                try:
                    ohlcv = pykrx_stock.get_market_ohlcv(lookback_start, today_fmt, sym)
                    if ohlcv.empty or len(ohlcv) < 20:
                        continue
                    ohlcv.columns = ["open", "high", "low", "close", "volume"]
                    data_map[sym] = add_indicators(ohlcv)
                    name_map[sym] = pykrx_stock.get_market_ticker_name(sym)
                except Exception:
                    continue

            # Filter universe
            filtered = filter_universe(list(data_map.keys()), data_map, universe_config, name_map)

            # Generate signals
            candidates = []
            for sym in filtered:
                if sym in existing_symbols:
                    continue
                df = data_map[sym]
                signals = gen_func(df, config)
                if not signals.empty and df.index[-1] in signals.index:
                    row = signals.loc[df.index[-1]]
                    entry_atr = float(df["atr14"].iloc[-1]) if "atr14" in df.columns else 0.0
                    candidates.append({
                        "symbol": sym,
                        "name": name_map.get(sym, sym),
                        "score": float(row["score"]),
                        "close": float(df["close"].iloc[-1]),
                        "entry_atr": entry_atr,
                    })

            # Top N by score
            candidates.sort(key=lambda x: x["score"], reverse=True)
            for c in candidates[:available_slots]:
                sl_price = int(c["close"] - c["entry_atr"] * config.atr_sl_multiplier) if c["entry_atr"] > 0 else int(c["close"] * (1 - config.stop_loss_pct))

                pos = Position(
                    strategy=strategy_name,
                    symbol=c["symbol"],
                    name=c["name"],
                    status="pending_buy",
                    signal_date=today,
                    entry_atr=c["entry_atr"],
                    sl_price=sl_price,
                )
                session.add(pos)
                existing_symbols.add(c["symbol"])

                alerts["pending_buys"].append({
                    "symbol": c["symbol"],
                    "name": c["name"],
                    "score": c["score"],
                    "sl_price": sl_price,
                })

        except Exception as e:
            logger.error(f"Signal generation failed for {strategy_name}: {e}")

    await session.commit()

    # Send Discord alert
    await notifier.send_signal_alert(alerts)
    logger.info(f"Signal job complete: {len(alerts['pending_buys'])} buys, {len(alerts['pending_sells'])} sells")


def _check_exit(pos: Position, today_close: int, config: StrategyParams) -> str | None:
    """Check exit conditions. Returns reason or None."""
    if pos.holding_days <= config.sl_skip_days:
        return None

    if pos.sl_price and today_close <= pos.sl_price:
        return "stop_loss"

    if pos.trail_price and today_close <= pos.trail_price:
        return "trailing_stop"

    if pos.holding_days >= config.max_holding_days:
        return "time_exit"

    return None
