from __future__ import annotations

import logging
from datetime import date, datetime
from zoneinfo import ZoneInfo

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.trader.executor import OrderExecutor
from app.notifier.discord import DiscordNotifier
from app.broker.account import KISAccountAPI
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
                from sqlalchemy import update as sql_update
                await session.execute(
                    sql_update(Order).where(Order.position_id == pos.id).values(position_id=None)
                )
                cleaned.append(f"  {order.symbol}: 주문 취소 + 포지션 삭제")
                await session.delete(pos)
            else:
                cleaned.append(f"  {order.symbol}: 주문 취소 (포지션 유지)")
        elif order.side == "sell" and order.position_id:
            # 매도 미체결 → 포지션 상태 확인 후 처리
            pos = await session.get(Position, order.position_id)
            if pos and pos.status == "pending_sell":
                if pos.qty and pos.qty > 0:
                    # 실제 보유 중 → active 복구 (다음날 재시도)
                    pos.status = "active"
                    pos.exit_reason = None
                    cleaned.append(f"  {order.symbol}: 매도 취소 → 포지션 active 복구")
                else:
                    # SL 모니터가 이미 매도 (qty=0) → 수동 확인 필요
                    pos.exit_reason = "sl_sell_unconfirmed"
                    cleaned.append(f"  {order.symbol}: SL 매도 미확인 — 수동 확인 필요")
                    await notifier.send_error(f"⚠️ {order.symbol} SL 매도 체결 미확인 — 브로커에서 수동 확인 필요")
            else:
                cleaned.append(f"  {order.symbol}: 매도 주문 취소")
        else:
            cleaned.append(f"  {order.symbol}: {order.side} 주문 취소")

    await session.commit()

    msg = f"🧹 장마감 미체결 정리: {len(stale_orders)}건\n" + "\n".join(cleaned)
    await notifier.send(msg)
    logger.info(f"Stale order cleanup: {len(stale_orders)} orders cancelled")


async def run_position_sync(
    session: AsyncSession,
    account_api: KISAccountAPI,
    notifier: DiscordNotifier,
):
    """DB ↔ 브로커 계좌 포지션 동기화 (09:35 실행).

    - 계좌에 없는데 DB에 active/pending_sell → 포지션 삭제
    - 계좌에 있는데 DB에 없음 → 포지션 생성 (미등록 상태)
    """
    logger.info("Position sync started")

    try:
        broker_holdings = await account_api.get_holdings()
    except Exception as e:
        logger.error(f"Position sync failed (API error): {e}")
        # API 실패해도 sl_sell_unconfirmed 포지션은 정리 (이미 매도 확인 불가 상태)
        stmt = select(Position).where(
            Position.status == "pending_sell",
            Position.exit_reason == "sl_sell_unconfirmed",
        )
        stale = (await session.execute(stmt)).scalars().all()
        if stale:
            from sqlalchemy import update as sql_update
            for pos in stale:
                await session.execute(
                    sql_update(Order).where(Order.position_id == pos.id).values(position_id=None)
                )
                await session.delete(pos)
            await session.commit()
            names = [f"{p.symbol} {p.name}" for p in stale]
            await notifier.send(f"🧹 sl_sell_unconfirmed 자동 정리: {', '.join(names)}")
            logger.info(f"Cleaned {len(stale)} sl_sell_unconfirmed positions despite API failure")
        return

    broker_map = {h.symbol: h for h in broker_holdings}

    stmt = select(Position).where(Position.status.in_(["active", "pending_sell"]))
    db_positions = (await session.execute(stmt)).scalars().all()
    db_map = {p.symbol: p for p in db_positions}

    actions = []

    # Case A: DB에 있는데 계좌에 없음 → 이미 매도됨
    for pos in db_positions:
        if pos.symbol not in broker_map:
            # submitted 매도 주문이 있으면 아직 체결 대기 중일 수 있음 → 건너뜀
            pending_sell_order = await session.execute(
                select(func.count()).select_from(Order).where(
                    Order.position_id == pos.id,
                    Order.side == "sell",
                    Order.status == "submitted",
                )
            )
            if pending_sell_order.scalar_one() > 0:
                continue

            from sqlalchemy import update as sql_update
            await session.execute(
                sql_update(Order).where(Order.position_id == pos.id).values(position_id=None)
            )
            await session.delete(pos)
            actions.append(f"  삭제: {pos.symbol} {pos.name} ({pos.status}) — 계좌에 없음")

    # Case B: 계좌에 있는데 DB에 없음 → 미등록 종목
    all_db_symbols = set(
        (await session.execute(
            select(Position.symbol).where(Position.status.in_(["active", "pending_buy", "pending_sell"]))
        )).scalars().all()
    )
    for symbol, holding in broker_map.items():
        if symbol not in all_db_symbols:
            new_pos = Position(
                strategy="manual",
                symbol=holding.symbol,
                name=holding.name,
                status="active",
                signal_date=date.today(),
                entry_date=date.today(),
                entry_price=holding.avg_price,
                qty=holding.qty,
                peak_price=holding.avg_price,
            )
            session.add(new_pos)
            actions.append(f"  생성: {holding.symbol} {holding.name} @ {holding.avg_price:,} x {holding.qty} (manual)")

    await session.commit()

    if actions:
        msg = f"🔄 포지션 동기화: {len(actions)}건\n" + "\n".join(actions)
        await notifier.send(msg)
        logger.info(f"Position sync: {len(actions)} actions")
    else:
        logger.info("Position sync: no mismatches")
