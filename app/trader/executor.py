from __future__ import annotations

import logging
from datetime import date, datetime

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

    def __init__(self, order_api: KISOrderAPI, account_api: KISAccountAPI, sl_manager: SLManager):
        self.order_api = order_api
        self.account_api = account_api
        self.sl_manager = sl_manager

    async def execute_sells(self, session: AsyncSession) -> list[dict]:
        """Execute all pending sell orders."""
        result = await session.execute(
            select(Position).where(Position.status == "pending_sell")
        )
        positions = result.scalars().all()
        results = []

        for pos in positions:
            order_result = await self.order_api.sell_market(pos.symbol, pos.qty)

            order = Order(
                position_id=pos.id,
                strategy=pos.strategy,
                symbol=pos.symbol,
                side="sell",
                order_type="market",
                qty=pos.qty,
                price=0,
                order_no=order_result.order_no if order_result.success else None,
                status="submitted" if order_result.success else "failed",
            )
            session.add(order)

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

        balance = await self.account_api.get_balance()

        for pos in positions:
            config = strategy_configs.get(pos.strategy)
            if not config:
                logger.error(f"No config for strategy {pos.strategy}")
                continue

            price = await self.account_api.get_current_price(pos.symbol)
            qty = calc_quantity(balance.total_eval, config.capital_allocation, config.position_weight, price)

            if qty <= 0:
                logger.warning(f"Insufficient funds for {pos.symbol}, skipping")
                results.append({"symbol": pos.symbol, "name": pos.name, "success": False, "message": "잔고 부족"})
                continue

            order_result = await self.order_api.buy_market(pos.symbol, qty)

            order = Order(
                position_id=pos.id,
                strategy=pos.strategy,
                symbol=pos.symbol,
                side="buy",
                order_type="market",
                qty=qty,
                price=0,
                order_no=order_result.order_no if order_result.success else None,
                status="submitted" if order_result.success else "failed",
            )
            session.add(order)

            if order_result.success:
                pos.qty = qty

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

    async def confirm_fills(self, session: AsyncSession, today: str) -> list[dict]:
        """Check fills and update positions."""
        filled_orders = await self.account_api.get_filled_orders(today)
        results = []

        stmt = select(Order).where(Order.status == "submitted")
        db_orders = (await session.execute(stmt)).scalars().all()

        for db_order in db_orders:
            fill = next((f for f in filled_orders if f.order_no == db_order.order_no), None)
            if not fill:
                continue

            db_order.status = "filled"
            db_order.filled_price = fill.price
            db_order.filled_qty = fill.qty
            db_order.filled_at = datetime.now()

            pos = await session.get(Position, db_order.position_id)
            if not pos:
                continue

            if db_order.side == "buy":
                pos.status = "active"
                pos.entry_date = date.today()
                pos.entry_price = fill.price
                pos.peak_price = fill.price
                pos.qty = fill.qty

                if pos.entry_atr and pos.entry_atr > 0:
                    pos.sl_price = int(fill.price - pos.entry_atr * 0.5)
                else:
                    pos.sl_price = int(fill.price * 0.97)

                sl_result = await self.sl_manager.register_sl(pos)
                if sl_result.success:
                    pos.sl_order_no = sl_result.order_no

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
                    entry_date=pos.entry_date,
                    exit_date=date.today(),
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

        await session.commit()
        return results
