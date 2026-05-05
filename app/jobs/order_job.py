from __future__ import annotations

import logging

from sqlalchemy.ext.asyncio import AsyncSession

from app.trader.executor import OrderExecutor
from app.notifier.discord import DiscordNotifier
from app.config import StrategyParams

logger = logging.getLogger(__name__)


async def run_order_job(
    session: AsyncSession,
    executor: OrderExecutor,
    strategy_configs: dict[str, StrategyParams],
    notifier: DiscordNotifier,
):
    """Order submission job (runs at 08:59)."""
    logger.info("Order job started")

    # 1. Execute sells first
    sell_results = await executor.execute_sells(session)
    if sell_results:
        await notifier.send_order_alert(sell_results, "매도 주문")

    # 2. Execute buys
    buy_results = await executor.execute_buys(session, strategy_configs)
    if buy_results:
        await notifier.send_order_alert(buy_results, "매수 주문")

    logger.info(f"Order job complete: {len(sell_results)} sells, {len(buy_results)} buys")
