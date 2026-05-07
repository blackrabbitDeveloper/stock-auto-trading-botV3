from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import pandas as pd
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import StrategyParams, MarketFilterConfig, UniverseConfig
from app.models.position import Position
from app.strategy import SIGNAL_GENERATORS, add_indicators, filter_universe
from app.broker.client import KISClient
from app.broker.market import KISMarketAPI
from app.notifier.discord import DiscordNotifier

logger = logging.getLogger(__name__)

UNIVERSE_CSV = Path("data/universe.csv")


def load_universe() -> tuple[list[str], dict[str, str]]:
    """Load symbol list and name map from CSV."""
    if not UNIVERSE_CSV.exists():
        logger.error(f"Universe CSV not found: {UNIVERSE_CSV}")
        return [], {}
    df = pd.read_csv(UNIVERSE_CSV, dtype={"symbol": str})
    df["name"] = df["name"].fillna(df["symbol"])
    symbols = df["symbol"].tolist()
    name_map = dict(zip(df["symbol"], df["name"]))
    return symbols, name_map


async def run_signal_job(
    session: AsyncSession,
    strategy_configs: dict[str, StrategyParams],
    market_filter_config: MarketFilterConfig,
    universe_config: UniverseConfig,
    notifier: DiscordNotifier,
    kis_client: KISClient | None = None,
):
    """Daily signal generation job (runs at 15:40)."""
    today = date.today()
    today_str = today.strftime("%Y-%m-%d")

    logger.info(f"Signal job started for {today_str}")

    # Init market API
    if kis_client is None:
        from app.config import KISConfig
        kis_config = KISConfig()
        kis_client = KISClient(kis_config)
        await kis_client.refresh_token()

    market_api = KISMarketAPI(kis_client)

    # Check market filter
    market_open = await _check_market_filter_api(market_api, market_filter_config)

    # Get all currently held/pending symbols across all strategies
    stmt = select(Position.symbol).where(Position.status.in_(["active", "pending_buy"]))
    existing_symbols = set((await session.execute(stmt)).scalars().all())

    alerts = {"date": today_str, "pending_buys": [], "pending_sells": []}

    # ── 1. Check exits for all active positions ──
    for strategy_name, config in strategy_configs.items():
        stmt = select(Position).where(
            Position.strategy == strategy_name,
            Position.status == "active",
        )
        active_positions = (await session.execute(stmt)).scalars().all()

        for pos in active_positions:
            try:
                df = await market_api.get_daily_ohlcv(pos.symbol)
                if df.empty:
                    continue

                today_close = int(df["close"].iloc[-1])
                today_high = int(df["high"].iloc[-1])

                pos.holding_days += 1

                # Use peak BEFORE today for trail calc (avoid look-ahead bias)
                # Matches backtester: trail based on peak_before_today
                peak_before_today = pos.peak_price or 0
                pos.trail_price = int(peak_before_today * (1 - config.trailing_stop_pct))

                # Update peak with today's high AFTER trail calculation (for next day)
                pos.peak_price = max(peak_before_today, today_high)

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

    # ── 2. Load OHLCV data ONCE for all symbols (shared across strategies) ──
    if not market_open:
        logger.info("Market filter BLOCKED — skipping new signals")
        await session.commit()
        await notifier.send_signal_alert(alerts)
        return

    all_symbols, name_map = load_universe()
    logger.info(f"Loading OHLCV for {len(all_symbols)} symbols...")

    data_map = {}
    failed_symbols = []
    for sym in all_symbols:
        if sym in existing_symbols:
            continue
        try:
            ohlcv = await market_api.get_daily_ohlcv(sym)
            if ohlcv.empty or len(ohlcv) < 20:
                continue
            data_map[sym] = add_indicators(ohlcv)
        except Exception:
            failed_symbols.append(sym)
            continue

    logger.info(f"Loaded {len(data_map)} symbols (failed: {len(failed_symbols)})")

    # Filter universe once
    filtered = filter_universe(list(data_map.keys()), data_map, universe_config)
    logger.info(f"{len(filtered)} symbols pass universe filter")

    # ── 3. Run each strategy on shared data ──
    for strategy_name, config in strategy_configs.items():
        gen_func = SIGNAL_GENERATORS.get(strategy_name)
        if not gen_func:
            continue

        # Check available slots for this strategy
        stmt = select(Position).where(
            Position.strategy == strategy_name,
            Position.status.in_(["active", "pending_buy"]),
        )
        current_count = len((await session.execute(stmt)).scalars().all())
        available_slots = config.max_positions - current_count

        if available_slots <= 0:
            continue

        # Generate signals using shared data
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
        logger.info(f"[{strategy_name}] {len(candidates)} signals generated")

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

    await session.commit()

    # Send Discord alert
    await notifier.send_signal_alert(alerts)
    logger.info(f"Signal job complete: {len(alerts['pending_buys'])} buys, {len(alerts['pending_sells'])} sells")


async def _check_market_filter_api(market_api: KISMarketAPI, config: MarketFilterConfig) -> bool:
    """Check market filter using 한투 API."""
    if not config.enabled:
        return True

    try:
        index_code = "0001" if config.index_code == "KS11" else "1001"
        index_df = await market_api.get_index_price(index_code)

        if index_df.empty:
            logger.warning("Market filter: no index data, allowing entries")
            return True

        ma = index_df["close"].rolling(config.ma_period).mean()
        last_close = index_df["close"].iloc[-1]
        last_ma = ma.iloc[-1]

        if pd.isna(last_ma):
            return True

        allowed = last_close > last_ma
        logger.info(f"Market filter: {last_close:,.0f} vs MA{config.ma_period} {last_ma:,.0f} -> {'OPEN' if allowed else 'BLOCKED'}")
        return allowed

    except Exception as e:
        logger.error(f"Market filter error: {e}")
        return True


def _check_exit(pos: Position, today_close: int, config: StrategyParams) -> str | None:
    """Check exit conditions using max(sl, trail) single trigger."""
    if pos.holding_days <= config.sl_skip_days:
        return None

    sl_price = pos.sl_price or 0
    trail_price = pos.trail_price or 0

    # MAX(SL, Trail) — matches real trading with single reservation order
    exit_trigger = max(sl_price, trail_price)

    if exit_trigger > 0 and today_close <= exit_trigger:
        if trail_price > sl_price:
            return "trailing_stop"
        return "stop_loss"

    if pos.holding_days >= config.max_holding_days:
        return "time_exit"

    return None
