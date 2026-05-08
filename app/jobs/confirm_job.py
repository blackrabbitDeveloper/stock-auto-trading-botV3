from __future__ import annotations

import logging
from datetime import date

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.trader.executor import OrderExecutor
from app.notifier.discord import DiscordNotifier
from app.models.order import Order

logger = logging.getLogger(__name__)


async def run_confirm_job(
    session: AsyncSession,
    executor: OrderExecutor,
    notifier: DiscordNotifier,
):
    """Fill confirmation job (runs at 09:05)."""
    logger.info("Confirm job started")

    today_str = date.today().strftime("%Y-%m-%d")
    results = await executor.confirm_fills(session, today_str)

    if results:
        lines = ["## ✅ 체결 확인"]
        for r in results:
            if r["type"] == "buy_filled":
                lines.append(f"> 매수 {r['symbol']} {r['name']} @ {r['price']:,} x {r['qty']} → SL {r['sl_price']:,}")
            elif r["type"] == "sell_filled":
                lines.append(f"> 매도 {r['symbol']} {r['name']} @ {r['price']:,} | {r['return_pct']:+.1%} | PnL {r['pnl']:+,}")
        await notifier.send("\n".join(lines))

    unconfirmed_result = await session.execute(
        select(func.count()).select_from(Order).where(Order.status == "submitted")
    )
    unconfirmed = unconfirmed_result.scalar_one()

    if unconfirmed > 0:
        await notifier.send_error(f"⏳ 미체결 주문 {unconfirmed}건 — 다음 체결확인에서 재시도합니다")

    logger.info(f"Confirm job complete: {len(results)} fills confirmed, {unconfirmed} unconfirmed")
