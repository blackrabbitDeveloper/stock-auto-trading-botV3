from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, Request

from app.config import KISConfig, KISPaperConfig, AppSettings, MarketFilterConfig, UniverseConfig, load_strategy_configs
from fastapi import Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.models import init_db, create_tables, get_session
from app.models import database as db_module
from app.broker.client import KISClient
from app.broker.order import KISOrderAPI
from app.broker.account import KISAccountAPI
from app.trader import OrderExecutor, SLManager
from app.notifier import DiscordNotifier
from app.dashboard import router as dashboard_router
from app.jobs import run_signal_job, run_order_job, run_confirm_job
from app.jobs.confirm_job import run_stale_order_cleanup

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)


_trading_day_cache: dict = {}


def _is_trading_day() -> bool:
    """Check if today is a Korean stock market trading day (not weekend/holiday).

    Uses `holidays` package for public holiday detection.
    Weekends are already filtered by CronTrigger(day_of_week="mon-fri"),
    but checked here as a safety net.
    """
    from datetime import date
    today = date.today()
    if today in _trading_day_cache:
        return _trading_day_cache[today]
    try:
        import holidays
        if today.weekday() >= 5:
            logger.info(f"Not a trading day: {today} (weekend)")
            _trading_day_cache[today] = False
            return False
        kr_holidays = holidays.KR(years=today.year)
        if today in kr_holidays:
            logger.info(f"Not a trading day: {today} (holiday: {kr_holidays[today]})")
            _trading_day_cache[today] = False
            return False
        _trading_day_cache[today] = True
        return True
    except Exception as e:
        logger.warning(f"Trading day check failed ({e}), assuming trading day")
        return True

scheduler = AsyncIOScheduler(timezone="Asia/Seoul")


def _fix_database_url(url: str) -> str:
    """Convert Railway's postgresql:// to asyncpg format."""
    if not url:
        return url
    if url.startswith("postgresql://") and "+asyncpg" not in url:
        url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return url


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown."""
    settings = AppSettings()
    kis_config = KISConfig()
    strategy_configs = load_strategy_configs("config")

    # Init DB
    db_url = _fix_database_url(settings.database_url)
    logger.info(f"DATABASE_URL configured: {'yes' if db_url else 'no'} (len={len(db_url)})")
    if db_url:
        init_db(db_url)
        await create_tables()
        logger.info("Database initialized successfully")
    else:
        logger.warning("DATABASE_URL not set — running without database")

    # Init broker - real API (always needed for market data/signals)
    real_client = KISClient(kis_config)
    try:
        await real_client.refresh_token()
        logger.info("Real API token OK")
    except Exception as e:
        logger.warning(f"Real API token failed (will retry later): {e}")

    # Init broker - trading API (paper or real based on KIS_ENV)
    if kis_config.env == "real":
        # 실전: 주문도 실전 API 사용
        trade_client = real_client
        logger.info(f"Trading mode: REAL (account: {kis_config.account_no})")
    else:
        # 모의: 주문은 모의투자 API 사용
        paper_config = KISPaperConfig()
        trade_client = KISClient(paper_config)
        try:
            await trade_client.refresh_token()
            logger.info(f"Trading mode: PAPER (account: {paper_config.account_no})")
        except Exception as e:
            logger.warning(f"Paper API token failed: {e}")

    order_api = KISOrderAPI(trade_client)
    account_api = KISAccountAPI(trade_client)
    sl_manager = SLManager(order_api)

    # Init notifier
    notifier = DiscordNotifier(settings.discord_webhook_url)

    executor = OrderExecutor(order_api, account_api, sl_manager, strategy_configs, notifier)

    # Store in app state
    app.state.real_client = real_client
    app.state.trade_client = trade_client
    app.state.executor = executor
    app.state.notifier = notifier
    app.state.strategy_configs = strategy_configs

    # Schedule jobs
    market_filter_config = MarketFilterConfig()
    universe_config = UniverseConfig()

    async def _build_sl_pos_map(session) -> dict[str, dict]:
        """Build position map for SL monitor (all active positions).

        Note: sl_skip_days filtering is handled in _on_sl_hit callback,
        so all active positions are subscribed for real-time price tracking.
        """
        from app.models.position import Position
        result = await session.execute(
            select(Position).where(Position.status == "active")
        )
        positions = result.scalars().all()
        pos_map = {}
        for p in positions:
            pos_map[p.symbol] = {
                "sl_price": p.sl_price or 0,
                "trail_price": p.trail_price or 0,
                "qty": p.qty or 0,
            }
        return pos_map

    async def _signal_job():
        if not _is_trading_day():
            return
        async with db_module.async_session_factory() as session:
            await run_signal_job(session, strategy_configs, market_filter_config, universe_config, notifier, real_client)
        # Update SL monitor with latest positions
        async with db_module.async_session_factory() as session:
            pos_map = await _build_sl_pos_map(session)
            if sl_monitor:
                await sl_monitor.update_positions(pos_map)

    async def _order_job():
        if not _is_trading_day():
            return
        async with db_module.async_session_factory() as session:
            await run_order_job(session, executor, strategy_configs, notifier)
        # Update SL monitor with newly bought positions
        async with db_module.async_session_factory() as session:
            pos_map = await _build_sl_pos_map(session)
            if sl_monitor:
                await sl_monitor.update_positions(pos_map)

    async def _confirm_job():
        if not _is_trading_day():
            return
        async with db_module.async_session_factory() as session:
            await run_confirm_job(session, executor, notifier)
        # Update SL monitor after confirm (sl_price now set from actual fill price)
        async with db_module.async_session_factory() as session:
            pos_map = await _build_sl_pos_map(session)
            if sl_monitor:
                await sl_monitor.update_positions(pos_map)

    async def _refresh_token():
        try:
            await real_client.refresh_token()
            if trade_client is not real_client:
                await trade_client.refresh_token()
            logger.info("Tokens refreshed successfully")
        except Exception as e:
            logger.error(f"Token refresh failed: {e}")
            await notifier.send_error(f"Token refresh failed: {e}")

    scheduler.add_job(_signal_job, CronTrigger(day_of_week="mon-fri", hour=15, minute=40), id="signal_job", misfire_grace_time=300)
    scheduler.add_job(_order_job, CronTrigger(day_of_week="mon-fri", hour=8, minute=59), id="order_job", misfire_grace_time=60)
    scheduler.add_job(_confirm_job, CronTrigger(day_of_week="mon-fri", hour=9, minute=5), id="confirm_job", misfire_grace_time=60)
    scheduler.add_job(_confirm_job, CronTrigger(day_of_week="mon-fri", hour=9, minute=30), id="confirm_job_retry", misfire_grace_time=60)
    scheduler.add_job(_refresh_token, CronTrigger(day_of_week="mon-fri", hour=8, minute=0), id="token_refresh")

    async def _stale_order_cleanup():
        if not _is_trading_day():
            return
        async with db_module.async_session_factory() as session:
            await run_stale_order_cleanup(session, notifier)

    scheduler.add_job(_stale_order_cleanup, CronTrigger(day_of_week="mon-fri", hour=15, minute=35), id="stale_order_cleanup", misfire_grace_time=300)

    # WebSocket SL monitor
    from app.broker.websocket import StopLossMonitor

    async def _on_sl_hit(symbol: str, price: int, reason: str):
        """Callback when SL/trail is hit — immediately sell."""
        async with db_module.async_session_factory() as session:
            from app.models.position import Position
            result = await session.execute(
                select(Position).where(Position.symbol == symbol, Position.status == "active")
            )
            pos = result.scalars().first()
            if not pos:
                logger.info(f"SL hit for {symbol} but no active position found (already sold?)")
                return
            if not pos.qty or pos.qty <= 0:
                logger.info(f"SL hit for {symbol} but qty=0 (already sold by order_job?)")
                return

            # sl_skip_days 체크: 보유 초기엔 SL 무시 (백테스터와 동일)
            config = strategy_configs.get(pos.strategy)
            sl_skip_days = config.sl_skip_days if config else 2
            if pos.holding_days <= sl_skip_days:
                logger.info(f"SL skip: {symbol} holding_days={pos.holding_days} <= sl_skip_days={sl_skip_days}")
                return

            # Mark as pending_sell FIRST to prevent order_job from also selling
            pos.status = "pending_sell"
            pos.exit_reason = reason
            await session.flush()

            # Cancel broker-side SL order before market sell
            if pos.sl_order_no:
                cancel_result = await order_api.cancel_order(pos.sl_order_no, pos.qty)
                if cancel_result.success:
                    logger.info(f"SL order cancelled for {symbol}: {pos.sl_order_no}")
                else:
                    logger.warning(f"SL cancel failed for {symbol}: {cancel_result.message}")

            sell_qty = pos.qty
            sell_result = await order_api.sell_market(pos.symbol, sell_qty)
            if sell_result.success:
                # DB에 sell Order 기록 → confirm_fills에서 매칭 후 포지션 삭제
                from app.models.order import Order
                sell_order = Order(
                    position_id=pos.id,
                    strategy=pos.strategy,
                    symbol=pos.symbol,
                    name=pos.name,
                    side="sell",
                    order_type="market",
                    qty=sell_qty,
                    price=0,
                    order_no=sell_result.order_no,
                    status="submitted",
                )
                session.add(sell_order)
                pos.qty = 0  # prevent double-sell by order_job
                pos.sl_order_no = None
                await session.commit()
                await notifier.send(f"🔻 SL HIT: {pos.symbol} {pos.name} | {reason} @ {price:,} | 시장가 매도")
                logger.warning(f"SL executed: {symbol} {reason} @ {price}")
            else:
                # Rollback status change — let order_job retry
                pos.status = "active"
                pos.exit_reason = None
                await session.commit()
                logger.error(f"SL sell failed for {symbol}: {sell_result.message}")
                await notifier.send_error(f"SL sell failed: {symbol} {sell_result.message}")

    sl_monitor = StopLossMonitor(real_client.config, _on_sl_hit)
    app.state.sl_monitor = sl_monitor

    async def _start_sl_monitor():
        """Load active positions and start monitoring."""
        try:
            async with db_module.async_session_factory() as session:
                pos_map = await _build_sl_pos_map(session)
                await sl_monitor.update_positions(pos_map)
            await sl_monitor.start()
        except Exception as e:
            logger.error(f"SL monitor error: {e}")

    scheduler.start()
    logger.info(f"Scheduler started with {len(strategy_configs)} strategies")

    # Launch SL monitor as background task (non-blocking)
    sl_task = asyncio.create_task(_start_sl_monitor())

    await notifier.send("🟢 Auto Trading Bot started (SL monitor active)")

    yield

    # Shutdown
    await sl_monitor.stop()
    sl_task.cancel()
    scheduler.shutdown()
    await real_client.close()
    if trade_client is not real_client:
        await trade_client.close()
    logger.info("Shutdown complete")


app = FastAPI(title="Stock Auto Trading Bot V3", lifespan=lifespan)
app.include_router(dashboard_router)


@app.get("/health")
async def health():
    jobs = {job.id: str(job.next_run_time) for job in scheduler.get_jobs()}
    return {"status": "ok", "jobs": jobs}


@app.post("/trigger/{job_name}")
async def trigger_job(job_name: str, request: Request):
    """수동으로 job 트리거 (예: POST /trigger/signal_job?token=xxx)."""
    settings = AppSettings()
    if settings.dashboard_token:
        token = request.query_params.get("token", "")
        if token != settings.dashboard_token:
            from fastapi import HTTPException
            raise HTTPException(status_code=401, detail="Unauthorized")

    job = scheduler.get_job(job_name)
    if not job:
        return {"error": f"Job '{job_name}' not found", "available": [j.id for j in scheduler.get_jobs()]}

    from datetime import datetime
    from zoneinfo import ZoneInfo
    job.modify(next_run_time=datetime.now(ZoneInfo("Asia/Seoul")))
    return {"status": "triggered", "job": job_name}


@app.post("/reset-positions")
async def reset_positions(request: Request, session: AsyncSession = Depends(get_session)):
    """Delete all pending positions (for clean re-scan)."""
    settings = AppSettings()
    if settings.dashboard_token:
        token = request.query_params.get("token", "")
        if token != settings.dashboard_token:
            from fastapi import HTTPException
            raise HTTPException(status_code=401, detail="Unauthorized")

    from app.models.position import Position
    from app.models.order import Order
    result = await session.execute(select(Position))
    positions = result.scalars().all()
    count = len(positions)
    # Delete related orders first (FK constraint)
    for pos in positions:
        orders = (await session.execute(select(Order).where(Order.position_id == pos.id))).scalars().all()
        for order in orders:
            await session.delete(order)
        await session.delete(pos)
    await session.commit()
    return {"status": "ok", "deleted": count}


@app.post("/remove-position/{position_id}")
async def remove_position(position_id: int, request: Request, session: AsyncSession = Depends(get_session)):
    """Delete a single pending position by ID."""
    settings = AppSettings()
    if settings.dashboard_token:
        token = request.query_params.get("token", "")
        if token != settings.dashboard_token:
            from fastapi import HTTPException
            raise HTTPException(status_code=401, detail="Unauthorized")

    from app.models.position import Position
    from app.models.order import Order
    pos = await session.get(Position, position_id)
    if not pos:
        return {"error": "Position not found"}
    # Delete related orders first (FK constraint)
    orders = (await session.execute(select(Order).where(Order.position_id == pos.id))).scalars().all()
    for order in orders:
        await session.delete(order)
    symbol = pos.symbol
    name = pos.name
    await session.delete(pos)
    await session.commit()
    return {"status": "ok", "removed": f"{symbol} {name}"}


@app.post("/sync-prices")
async def sync_prices(request: Request, session: AsyncSession = Depends(get_session)):
    """잔고조회 API에서 매입평균가를 가져와 active 포지션의 entry_price를 갱신."""
    settings = AppSettings()
    if settings.dashboard_token:
        token = request.query_params.get("token", "")
        if token != settings.dashboard_token:
            from fastapi import HTTPException
            raise HTTPException(status_code=401, detail="Unauthorized")

    from app.broker.account import KISAccountAPI
    from app.models.position import Position

    trade_client = getattr(request.app.state, "trade_client", None)
    if not trade_client:
        return {"error": "trade_client not initialized"}

    account_api = KISAccountAPI(trade_client)
    env = trade_client.config.env
    tr_id = "VTTC8434R" if env == "paper" else "TTTC8434R"

    data = await trade_client.request("GET", "/uapi/domestic-stock/v1/trading/inquire-balance", tr_id, params={
        "CANO": trade_client.config.account_prefix,
        "ACNT_PRDT_CD": trade_client.config.account_suffix,
        "AFHR_FLPR_YN": "N",
        "OFL_YN": "",
        "INQR_DVSN": "02",
        "UNPR_DVSN": "01",
        "FUND_STTL_ICLD_YN": "N",
        "FNCG_AMT_AUTO_RDPT_YN": "N",
        "PRCS_DVSN": "00",
        "CTX_AREA_FK100": "",
        "CTX_AREA_NK100": "",
    })

    holdings = {}
    for item in data.get("output1", []):
        symbol = item.get("pdno", "")
        avg_price = int(float(item.get("pchs_avg_pric", "0")))
        if symbol and avg_price > 0:
            holdings[symbol] = avg_price

    stmt = select(Position).where(Position.status == "active")
    positions = (await session.execute(stmt)).scalars().all()

    updated = []
    for pos in positions:
        if pos.symbol in holdings:
            old_price = pos.entry_price
            new_price = holdings[pos.symbol]
            if old_price != new_price:
                pos.entry_price = new_price
                pos.peak_price = max(pos.peak_price or 0, new_price)
                updated.append(f"{pos.symbol} {pos.name}: {old_price or 0:,} -> {new_price:,}")

    await session.commit()
    return {"status": "ok", "updated": len(updated), "details": updated}
