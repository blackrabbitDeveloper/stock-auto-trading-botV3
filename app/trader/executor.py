from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")


def _today_kst() -> date:
    return datetime.now(KST).date()

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.broker.order import KISOrderAPI
from app.broker.account import KISAccountAPI
from app.models.position import Position
from app.models.order import Order
from app.models.trade import Trade
from app.trader.quantity import calc_quantity
from app.trader.sl_manager import SLManager
from app.config import StrategyParams

logger = logging.getLogger(__name__)


class OrderExecutor:
    """Orchestrates buy/sell order execution."""

    def __init__(self, order_api: KISOrderAPI, account_api: KISAccountAPI, sl_manager: SLManager,
                 strategy_configs: dict[str, StrategyParams] | None = None,
                 notifier=None):
        self.order_api = order_api
        self.account_api = account_api
        self.sl_manager = sl_manager
        self.strategy_configs = strategy_configs or {}
        self.notifier = notifier

    async def execute_sells(self, session: AsyncSession) -> list[dict]:
        """Execute all pending sell orders."""
        result = await session.execute(
            select(Position).where(Position.status == "pending_sell")
        )
        positions = result.scalars().all()
        results = []

        for pos in positions:
            # qty=0이면 SL 모니터가 이미 매도 처리 → confirm_fills에서 정리
            if not pos.qty or pos.qty <= 0:
                logger.warning(f"Skipping sell for {pos.symbol}: qty={pos.qty} (already sold by SL monitor)")
                results.append({"symbol": pos.symbol, "name": pos.name, "success": False, "message": "SL 매도 완료 대기"})
                continue

            sell_qty = pos.qty
            await asyncio.sleep(1.0)  # rate limit protection
            order_result = await self.order_api.sell_market(pos.symbol, sell_qty)

            order = Order(
                position_id=pos.id,
                strategy=pos.strategy,
                symbol=pos.symbol,
                name=pos.name,
                side="sell",
                order_type="market",
                qty=sell_qty,
                price=0,
                order_no=order_result.order_no if order_result.success else None,
                status="submitted" if order_result.success else "failed",
            )
            session.add(order)

            if order_result.success:
                pos.qty = 0  # 이중 매도 방지

            results.append({
                "symbol": pos.symbol,
                "name": pos.name,
                "success": order_result.success,
                "order_no": order_result.order_no,
                "message": order_result.message,
            })

        await session.commit()
        return results

    async def execute_buys(self, session: AsyncSession, strategy_configs: dict[str, StrategyParams]) -> list[dict]:
        """Execute all pending buy orders."""

        result = await session.execute(
            select(Position).where(Position.status == "pending_buy")
        )
        positions = result.scalars().all()
        results = []

        # 현재 활성 포지션 수 확인
        active_count_result = await session.execute(
            select(Position).where(Position.status == "active")
        )
        active_count = len(active_count_result.scalars().all())

        balance = await self.account_api.get_balance()
        # 현금 10% 여유분 확보 (슬리피지/수수료 대비)
        cash_reserve = int(balance.total_eval * 0.10)
        spent = cash_reserve  # 여유분만큼 미리 차감

        MAX_TOTAL_POSITIONS = 15  # 전체 전략 합산 최대 보유

        for pos in positions:
            if active_count >= MAX_TOTAL_POSITIONS:
                logger.warning(f"Max positions reached ({MAX_TOTAL_POSITIONS}), skipping {pos.symbol}")
                results.append({"symbol": pos.symbol, "name": pos.name, "success": False, "message": f"최대 보유 {MAX_TOTAL_POSITIONS}개 초과"})
                continue

            config = strategy_configs.get(pos.strategy)
            if not config:
                logger.error(f"No config for strategy {pos.strategy}")
                continue

            # Estimate entry price from SL + ATR (avoids extra API call that causes rate limit)
            if pos.entry_atr and pos.entry_atr > 0 and pos.sl_price:
                price = int(pos.sl_price + pos.entry_atr * config.atr_sl_multiplier)
            elif pos.sl_price:
                price = int(pos.sl_price / (1 - config.stop_loss_pct))
            else:
                logger.warning(f"Cannot estimate price for {pos.symbol}, skipping")
                results.append({"symbol": pos.symbol, "name": pos.name, "success": False, "message": "가격 추정 불가"})
                continue
            available_eval = balance.total_eval - spent
            # 갭업/슬리피지 대비 10% 안전 마진
            safe_price = int(price * 1.10)
            qty = calc_quantity(available_eval, config.capital_allocation, config.position_weight, safe_price)

            if qty <= 0:
                logger.warning(f"Insufficient funds for {pos.symbol}, removing from pending")
                await session.delete(pos)
                results.append({"symbol": pos.symbol, "name": pos.name, "success": False, "message": "잔고 부족 (삭제)"})
                continue

            await asyncio.sleep(1.0)  # rate limit protection
            order_result = await self.order_api.buy_market(pos.symbol, qty)

            order = Order(
                position_id=pos.id,
                strategy=pos.strategy,
                symbol=pos.symbol,
                name=pos.name,
                side="buy",
                order_type="market",
                qty=qty,
                price=0,
                order_no=order_result.order_no if order_result.success else None,
                status="submitted" if order_result.success else "failed",
            )
            session.add(order)

            if order_result.success:
                spent += qty * safe_price  # Track spent for next position
                active_count += 1
                pos.qty = qty
                pos.status = "active"  # prevent re-buy on redeploy
                pos.entry_date = _today_kst()
                # entry_price, peak_price are set by confirm_fills() with actual fill price
                # Do NOT set them here — the estimated price can differ significantly

            results.append({
                "symbol": pos.symbol,
                "name": pos.name,
                "qty": qty,
                "success": order_result.success,
                "order_no": order_result.order_no,
                "message": order_result.message,
            })

        await session.commit()
        return results

    async def confirm_fills(self, session: AsyncSession, today: str, lookback_days: int = 2) -> list[dict]:
        """Check fills and update positions. Looks back multiple days to catch missed fills."""
        from datetime import datetime, timedelta

        all_filled = []
        base_date = datetime.strptime(today, "%Y-%m-%d")
        for i in range(lookback_days):
            query_date = (base_date - timedelta(days=i)).strftime("%Y-%m-%d")
            try:
                fills = await self.account_api.get_filled_orders(query_date)
                all_filled.extend(fills)
            except Exception as e:
                logger.warning(f"Failed to query fills for {query_date}: {e}")

        # Deduplicate by order_no
        seen = set()
        filled_orders = []
        for f in all_filled:
            if f.order_no not in seen:
                seen.add(f.order_no)
                filled_orders.append(f)

        results = []

        stmt = select(Order).where(Order.status == "submitted")
        db_orders = (await session.execute(stmt)).scalars().all()

        logger.info(f"Confirm: {len(filled_orders)} fills from API (lookback={lookback_days}d), {len(db_orders)} submitted orders in DB")
        if filled_orders:
            logger.info(f"API fills: {[(f.order_no, f.symbol, f.qty) for f in filled_orders[:5]]}")
        if db_orders:
            logger.info(f"DB orders: {[(o.order_no, o.symbol, o.qty) for o in db_orders[:5]]}")

        for db_order in db_orders:
            fill = next((f for f in filled_orders if f.order_no == db_order.order_no), None)
            if not fill:
                continue

            try:
                await self._process_fill(session, db_order, fill, results)
            except Exception as e:
                logger.error(f"Fill processing failed for {db_order.symbol} order={db_order.order_no}: {e}")
                await session.rollback()

        await session.commit()
        return results

    async def _process_fill(self, session: AsyncSession, db_order: Order, fill, results: list[dict]):
        """Process a single fill match. Isolated so one failure doesn't block others."""
        # 부분 체결 감지
        if fill.qty < db_order.qty:
            logger.warning(f"Partial fill: {db_order.symbol} filled {fill.qty}/{db_order.qty}")
            if self.notifier:
                await self.notifier.send_error(
                    f"⚠️ 부분 체결: {db_order.symbol} {fill.qty}/{db_order.qty}주 — 수동 확인 필요"
                )

        db_order.status = "filled"
        db_order.filled_price = fill.price
        db_order.filled_qty = fill.qty
        db_order.filled_at = datetime.now(KST)

        pos = await session.get(Position, db_order.position_id)
        if not pos:
            await session.flush()
            return

        if db_order.side == "buy":
            pos.status = "active"
            pos.entry_date = _today_kst()
            pos.entry_price = fill.price
            pos.peak_price = fill.price
            pos.qty = fill.qty

            config = self.strategy_configs.get(pos.strategy)
            atr_mult = config.atr_sl_multiplier if config else 0.5
            sl_pct = config.stop_loss_pct if config else 0.03

            atr_enabled = config.atr_sl_enabled if config else True
            if atr_enabled and pos.entry_atr and pos.entry_atr > 0:
                pos.sl_price = int(fill.price - pos.entry_atr * atr_mult)
            else:
                pos.sl_price = int(fill.price * (1 - sl_pct))

            # SL은 WebSocket 모니터가 실시간 감시 (브로커 지정가 SL은 즉시 체결 위험)
            logger.info(f"Buy filled: {pos.symbol} @ {fill.price:,} SL={pos.sl_price:,} (WebSocket monitored)")

            results.append({
                "type": "buy_filled",
                "symbol": pos.symbol,
                "name": pos.name,
                "price": fill.price,
                "qty": fill.qty,
                "sl_price": pos.sl_price,
            })

        elif db_order.side == "sell":
            trade = Trade(
                strategy=pos.strategy,
                symbol=pos.symbol,
                name=pos.name,
                entry_date=pos.entry_date or _today_kst(),
                exit_date=_today_kst(),
                entry_price=pos.entry_price,
                exit_price=fill.price,
                qty=fill.qty,
                return_pct=(fill.price / pos.entry_price - 1) if pos.entry_price else 0,
                pnl=(fill.price - pos.entry_price) * fill.qty if pos.entry_price else 0,
                holding_days=pos.holding_days,
                exit_reason=pos.exit_reason or "manual",
            )
            session.add(trade)
            await session.delete(pos)

            results.append({
                "type": "sell_filled",
                "symbol": trade.symbol,
                "name": trade.name,
                "price": fill.price,
                "return_pct": trade.return_pct,
                "pnl": trade.pnl,
            })

        await session.flush()
