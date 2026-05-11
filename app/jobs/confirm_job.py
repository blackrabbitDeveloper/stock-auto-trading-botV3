from __future__ import annotations

import logging
from datetime import date, datetime
from zoneinfo import ZoneInfo

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.trader.executor import OrderExecutor
from app.notifier.discord import DiscordNotifier
from app.models.order import Order
from app.models.position import Position

logger = logging.getLogger(__name__)


async def run_confirm_job(
    session: AsyncSession,
    executor: OrderExecutor,
    notifier: DiscordNotifier,
):
    """Fill confirmation job (runs at 09:05)."""
    logger.info("Confirm job started")

    today_str = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d")
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


async def run_stale_order_cleanup(
    session: AsyncSession,
    notifier: DiscordNotifier,
):
    """장마감 후 미체결 주문 정리 (15:35 실행).

    증권사에서 장마감 시 미체결 시장가 주문은 자동 취소되지만,
    봇 DB에는 submitted 상태로 남아있으므로 정리가 필요.
    """
    logger.info("Stale order cleanup started")

    stmt = select(Order).where(Order.status == "submitted")
    stale_orders = (await session.execute(stmt)).scalars().all()

    if not stale_orders:
        logger.info("No stale orders to clean up")
        return

    cleaned = []
    for order in stale_orders:
        order.status = "cancelled"

        # 매수 주문이 미체결 → 포지션도 정리
        if order.side == "buy" and order.position_id:
            pos = await session.get(Position, order.position_id)
            if pos and pos.status == "active" and not pos.entry_price:
                # entry_price가 없다 = 실제로 매수가 안 됨
                pos.status = "cancelled"
                cleaned.append(f"  {order.symbol}: 주문 취소 + 포지션 삭제")
                await session.delete(pos)
            else:
                cleaned.append(f"  {order.symbol}: 주문 취소 (포지션 유지)")
        elif order.side == "sell" and order.position_id:
            # 매도 미체결 → 포지션을 active로 복구 (다음날 재시도)
            pos = await session.get(Position, order.position_id)
            if pos and pos.status == "pending_sell":
                pos.status = "active"
                pos.exit_reason = None
                cleaned.append(f"  {order.symbol}: 매도 취소 → 포지션 active 복구")
            else:
                cleaned.append(f"  {order.symbol}: 매도 주문 취소")
        else:
            cleaned.append(f"  {order.symbol}: {order.side} 주문 취소")

    await session.commit()

    msg = f"🧹 장마감 미체결 정리: {len(stale_orders)}건\n" + "\n".join(cleaned)
    await notifier.send(msg)
    logger.info(f"Stale order cleanup: {len(stale_orders)} orders cancelled")
