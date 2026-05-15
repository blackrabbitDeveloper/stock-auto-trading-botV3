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
from app.jobs.confirm_job import run_stale_order_cleanup, run_position_sync

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

    # 매도 동시 실행 방지 (WS SL콜백 + order_job 이중 매도 차단)
    sell_lock = asyncio.Lock()

    # Store in app state
    app.state.real_client = real_client
    app.state.trade_client = trade_client
    app.state.executor = executor
    app.state.notifier = notifier
    app.state.strategy_configs = strategy_configs
    app.state.job_history = {}  # {job_id: {last_run, status, message, duration_s}}
    app.state.trading_paused = False

    # Schedule jobs
    market_filter_config = MarketFilterConfig()
    universe_config = UniverseConfig()

    async def _track_job(job_id: str, coro):
        """Run a job coroutine and record execution result in app.state.job_history."""
        import time
        start = time.monotonic()
        try:
            await coro
            elapsed = time.monotonic() - start
            app.state.job_history[job_id] = {
                "last_run": datetime.now(ZoneInfo("Asia/Seoul")).strftime("%H:%M:%S"),
                "status": "ok",
                "message": "",
                "duration_s": round(elapsed, 1),
            }
        except Exception as e:
            elapsed = time.monotonic() - start
            app.state.job_history[job_id] = {
                "last_run": datetime.now(ZoneInfo("Asia/Seoul")).strftime("%H:%M:%S"),
                "status": "error",
                "message": str(e)[:100],
                "duration_s": round(elapsed, 1),
            }
            logger.error(f"Job {job_id} failed: {e}", exc_info=True)

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
            config = strategy_configs.get(p.strategy)
            pos_map[p.symbol] = {
                "sl_price": p.sl_price or 0,
                "trail_price": p.trail_price or 0,
                "peak_price": p.peak_price or 0,
                "trailing_stop_pct": config.trailing_stop_pct if config else 0.05,
                "qty": p.qty or 0,
            }
        return pos_map

    async def _signal_job():
        if not _is_trading_day():
            return
        if app.state.trading_paused:
            logger.info("Signal job skipped — trading paused")
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
        if app.state.trading_paused:
            logger.info("Order job skipped — trading paused")
            return
        async with sell_lock, db_module.async_session_factory() as session:
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

    async def _trail_update_job():
        """아침 장 시작 전 trail_price 갱신 (전일 peak 기준)."""
        if not _is_trading_day():
            return
        async with db_module.async_session_factory() as session:
            from app.models.position import Position
            result = await session.execute(
                select(Position).where(Position.status == "active")
            )
            positions = result.scalars().all()
            updated = []
            DEFAULT_TRAIL_PCT = 0.05
            for pos in positions:
                config = strategy_configs.get(pos.strategy)
                trail_pct = config.trailing_stop_pct if config else DEFAULT_TRAIL_PCT
                if not pos.peak_price:
                    continue
                sl_skip = config.sl_skip_days if config else 2
                if pos.holding_days <= sl_skip:
                    continue
                new_trail = int(pos.peak_price * (1 - trail_pct))
                if new_trail != (pos.trail_price or 0):
                    pos.trail_price = new_trail
                    updated.append(f"{pos.symbol}: trail={new_trail:,}")
            await session.commit()
            if updated:
                logger.info(f"Trail update: {', '.join(updated)}")
        # Refresh SL monitor
        async with db_module.async_session_factory() as session:
            pos_map = await _build_sl_pos_map(session)
            if sl_monitor:
                await sl_monitor.update_positions(pos_map)

    def _tracked(job_id, fn):
        async def wrapper():
            await _track_job(job_id, fn())
        return wrapper

    scheduler.add_job(_tracked("trail_update", _trail_update_job), CronTrigger(day_of_week="mon-fri", hour=8, minute=50), id="trail_update", misfire_grace_time=60)
    scheduler.add_job(_tracked("signal_job", _signal_job), CronTrigger(day_of_week="mon-fri", hour=15, minute=40), id="signal_job", misfire_grace_time=300)
    scheduler.add_job(_tracked("order_job", _order_job), CronTrigger(day_of_week="mon-fri", hour=8, minute=59), id="order_job", misfire_grace_time=60)
    if kis_config.env == "paper":
        scheduler.add_job(_tracked("confirm_job", _confirm_job), CronTrigger(day_of_week="mon-fri", hour=9, minute=1), id="confirm_job_early", misfire_grace_time=60)
    scheduler.add_job(_tracked("confirm_job", _confirm_job), CronTrigger(day_of_week="mon-fri", hour=9, minute=5), id="confirm_job", misfire_grace_time=60)
    scheduler.add_job(_tracked("confirm_job", _confirm_job), CronTrigger(day_of_week="mon-fri", hour=9, minute=30), id="confirm_job_retry", misfire_grace_time=60)
    scheduler.add_job(_tracked("token_refresh", _refresh_token), CronTrigger(day_of_week="mon-fri", hour=8, minute=0), id="token_refresh")
    scheduler.add_job(_tracked("token_refresh", _refresh_token), CronTrigger(day_of_week="mon-fri", hour=20, minute=0), id="token_refresh_evening")

    async def _stale_order_cleanup():
        if not _is_trading_day():
            return
        # 취소 전 마지막 체결 확인 시도 (API 속도 제한으로 놓친 체결 복구)
        try:
            async with db_module.async_session_factory() as session:
                today_str = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d")
                results = await executor.confirm_fills(session, today_str, lookback_days=3)
                if results:
                    lines = ["## ✅ 장마감 전 체결 확인 (복구)"]
                    for r in results:
                        if r["type"] == "buy_filled":
                            lines.append(f"> 매수 {r['symbol']} {r['name']} @ {r['price']:,} x {r['qty']}")
                        elif r["type"] == "sell_filled":
                            lines.append(f"> 매도 {r['symbol']} {r['name']} @ {r['price']:,} | {r['return_pct']:+.1%}")
                    await notifier.send("\n".join(lines))
                    logger.info(f"Pre-cleanup confirm: {len(results)} fills recovered")
        except Exception as e:
            logger.warning(f"Pre-cleanup confirm failed: {e}")
        async with db_module.async_session_factory() as session:
            await run_stale_order_cleanup(session, notifier)

    scheduler.add_job(_tracked("stale_cleanup", _stale_order_cleanup), CronTrigger(day_of_week="mon-fri", hour=15, minute=35), id="stale_order_cleanup", misfire_grace_time=300)

    async def _position_sync():
        if not _is_trading_day():
            return
        async with db_module.async_session_factory() as session:
            await run_position_sync(session, account_api, notifier)
        # Refresh SL monitor after sync
        async with db_module.async_session_factory() as session:
            pos_map = await _build_sl_pos_map(session)
            if sl_monitor:
                await sl_monitor.update_positions(pos_map)

    scheduler.add_job(_tracked("position_sync", _position_sync), CronTrigger(day_of_week="mon-fri", hour=9, minute=35), id="position_sync", misfire_grace_time=60)

    # WebSocket SL monitor
    from app.broker.websocket import StopLossMonitor

    async def _on_sl_hit(symbol: str, price: int, reason: str):
        """Callback when SL/trail is hit — immediately sell.

        Wrapped in try/except to prevent WebSocket session crash.
        """
        try:
            from datetime import datetime, time as dt_time
            from zoneinfo import ZoneInfo
            now_kst = datetime.now(ZoneInfo("Asia/Seoul")).time()
            if not (dt_time(9, 0) <= now_kst <= dt_time(15, 20)):
                logger.info(f"SL hit for {symbol} ignored — outside market hours ({now_kst})")
                return
            async with sell_lock, db_module.async_session_factory() as session:
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
        except Exception as e:
            logger.error(f"_on_sl_hit crashed for {symbol}: {e}", exc_info=True)
            try:
                await notifier.send_error(f"SL callback error: {symbol} — {e}")
            except Exception:
                pass

    # WS 환경을 거래 클라이언트와 일치시킴 (paper↔real 불일치 방지)
    sl_monitor = StopLossMonitor(trade_client.config, _on_sl_hit)
    app.state.sl_monitor = sl_monitor

    async def _start_sl_monitor():
        """Recover pending fills then start SL monitoring."""
        try:
            # 재시작 시 미확인 체결 복구 → SL 보호 누락 방지
            async with db_module.async_session_factory() as session:
                from datetime import datetime as dt
                from zoneinfo import ZoneInfo
                today_str = dt.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d")
                await executor.confirm_fills(session, today_str)
                logger.info("Startup confirm_fills complete")
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


@app.get("/api/token-cache")
async def token_cache_status(request: Request):
    """토큰 캐시 상태 확인 (디버깅용)."""
    settings = AppSettings()
    if settings.dashboard_token:
        token = request.query_params.get("token", "")
        if token != settings.dashboard_token:
            from fastapi import HTTPException
            raise HTTPException(status_code=401)
    from app.models.token_cache import TokenCache
    from app.models import database as db_module
    result = {}
    if db_module.async_session_factory:
        async with db_module.async_session_factory() as session:
            for env in ["real", "paper"]:
                row = await session.get(TokenCache, env)
                if row:
                    from datetime import timezone
                    age = (datetime.now(ZoneInfo("Asia/Seoul")) - row.issued_at.replace(tzinfo=timezone.utc).astimezone(ZoneInfo("Asia/Seoul")))
                    result[env] = {
                        "has_token": bool(row.token),
                        "token_prefix": row.token[:20] + "..." if row.token else "",
                        "issued_at": str(row.issued_at),
                        "age_minutes": round(age.total_seconds() / 60, 1),
                    }
                else:
                    result[env] = {"has_token": False, "reason": "no row in DB"}
    else:
        result["error"] = "async_session_factory is None"
    return result


@app.get("/api/debug-fills")
async def debug_fills(request: Request, session: AsyncSession = Depends(get_session)):
    """체결 확인 디버깅: API 응답 + DB 주문 상태 비교."""
    settings = AppSettings()
    if settings.dashboard_token:
        token = request.query_params.get("token", "")
        if token != settings.dashboard_token:
            from fastapi import HTTPException
            raise HTTPException(status_code=401)

    from app.models.order import Order
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    trade_client = getattr(request.app.state, "trade_client", None)
    if not trade_client:
        return {"error": "trade_client not initialized"}

    account_api = KISAccountAPI(trade_client)
    today = datetime.now(ZoneInfo("Asia/Seoul"))
    result = {"env": trade_client.config.env}

    # 최근 3일 체결 API 응답
    fills_by_date = {}
    for i in range(3):
        d = (today - timedelta(days=i)).strftime("%Y-%m-%d")
        try:
            fills = await account_api.get_filled_orders(d)
            fills_by_date[d] = [{"order_no": f.order_no, "symbol": f.symbol, "side": f.side, "qty": f.qty, "price": f.price} for f in fills]
        except Exception as e:
            fills_by_date[d] = {"error": str(e)}

    result["api_fills"] = fills_by_date

    # DB 주문 상태
    stmt = select(Order).order_by(Order.submitted_at.desc()).limit(20)
    orders = (await session.execute(stmt)).scalars().all()
    result["db_orders"] = [{
        "id": o.id, "symbol": o.symbol, "side": o.side, "qty": o.qty,
        "order_no": o.order_no, "status": o.status,
        "filled_price": o.filled_price, "submitted_at": str(o.submitted_at),
    } for o in orders]

    return result


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


@app.post("/toggle-trading")
async def toggle_trading(request: Request):
    """거래 일시정지/재개 토글."""
    settings = AppSettings()
    if settings.dashboard_token:
        token = request.query_params.get("token", "")
        if token != settings.dashboard_token:
            raise HTTPException(status_code=401)
    app.state.trading_paused = not app.state.trading_paused
    state = "paused" if app.state.trading_paused else "active"
    logger.info(f"Trading toggled: {state}")
    notifier = getattr(app.state, "notifier", None)
    if notifier:
        await notifier.send(f"{'⏸️' if app.state.trading_paused else '▶️'} 거래 {'일시정지' if app.state.trading_paused else '재개'}")
    return {"status": state}


@app.post("/api/recover-fills")
async def recover_fills(request: Request, session: AsyncSession = Depends(get_session)):
    """cancelled 매수 주문을 API 체결 데이터와 매칭 + 잘못된 Trade 정리."""
    import asyncio
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    from app.models.order import Order
    from app.models.position import Position
    from app.models.trade import Trade
    from app.broker.account import KISAccountAPI

    settings = AppSettings()
    if settings.dashboard_token:
        tok = request.query_params.get("token", "")
        if tok != settings.dashboard_token:
            from fastapi import HTTPException
            raise HTTPException(status_code=401)

    trade_client = getattr(request.app.state, "trade_client", None)
    if not trade_client:
        return {"error": "trade_client not initialized"}

    strat_configs = getattr(request.app.state, "strategy_configs", {})
    account_api = KISAccountAPI(trade_client)
    now_naive = datetime.now(ZoneInfo("Asia/Seoul")).replace(tzinfo=None)
    today_kst = datetime.now(ZoneInfo("Asia/Seoul"))

    # 1) 최근 7일 체결 데이터 수집
    all_fills = []
    for i in range(7):
        d = (today_kst - timedelta(days=i)).strftime("%Y-%m-%d")
        try:
            fills = await account_api.get_filled_orders(d)
            all_fills.extend(fills)
        except Exception:
            pass
        if i < 6:
            await asyncio.sleep(0.5)

    fill_map = {f.order_no: f for f in all_fills}
    recovered = []

    # 2) cancelled 매수 주문 → 체결 확인 (symbol로 현재 포지션 매칭)
    cancelled_buys = (await session.execute(
        select(Order).where(
            Order.status == "cancelled",
            Order.side == "buy",
            Order.order_no.isnot(None),
        )
    )).scalars().all()

    # 현재 active 포지션을 symbol로 인덱싱
    active_positions = (await session.execute(
        select(Position).where(Position.status == "active")
    )).scalars().all()
    pos_by_symbol = {p.symbol: p for p in active_positions}

    for order in cancelled_buys:
        fill = fill_map.get(order.order_no)
        if not fill or fill.qty <= 0:
            continue

        # position_id로 찾고, 없으면 symbol로 매칭
        pos = None
        if order.position_id:
            pos = await session.get(Position, order.position_id)
        if not pos:
            pos = pos_by_symbol.get(order.symbol)
        if not pos:
            continue

        # 주문 상태 업데이트
        order.status = "filled"
        order.filled_price = fill.price
        order.filled_qty = fill.qty
        order.filled_at = now_naive

        # 포지션 보정
        changes = []
        if not pos.entry_price or pos.entry_price <= 0:
            pos.entry_price = fill.price
            pos.peak_price = max(pos.peak_price or 0, fill.price)
            pos.qty = fill.qty
            changes.append(f"entry={fill.price:,}")
        if not pos.sl_price:
            config = strat_configs.get(pos.strategy)
            sl_pct = config.stop_loss_pct if config else 0.03
            pos.sl_price = int((pos.entry_price or fill.price) * (1 - sl_pct))
            changes.append(f"SL={pos.sl_price:,}")
        detail = f" ({', '.join(changes)})" if changes else ""
        recovered.append(f"매수확인: {order.symbol} @ {fill.price:,} x {fill.qty}{detail}")

    # 3) 잘못 생성된 Trade 정리 (브로커에 보유 중인 종목의 매도 기록)
    await asyncio.sleep(0.5)
    try:
        holdings = await account_api.get_holdings()
        broker_symbols = {h.symbol for h in holdings}
        if broker_symbols:
            bad_trades = (await session.execute(
                select(Trade).where(Trade.symbol.in_(broker_symbols))
            )).scalars().all()
            for t in bad_trades:
                await session.delete(t)
                recovered.append(f"잘못된매도기록삭제: {t.symbol} {t.name}")
    except Exception:
        pass  # API 실패 시 Trade 정리는 건너뜀

    await session.commit()

    # 4) SL 모니터 갱신
    sl_monitor = getattr(request.app.state, "sl_monitor", None)
    if sl_monitor and recovered:
        from app.models import database as db_module
        async with db_module.async_session_factory() as s2:
            result = await s2.execute(select(Position).where(Position.status == "active"))
            positions = result.scalars().all()
            pos_map = {}
            for p in positions:
                config = strat_configs.get(p.strategy)
                pos_map[p.symbol] = {
                    "sl_price": p.sl_price or 0,
                    "trail_price": p.trail_price or 0,
                    "peak_price": p.peak_price or 0,
                    "trailing_stop_pct": config.trailing_stop_pct if config else 0.05,
                    "qty": p.qty or 0,
                }
            await sl_monitor.update_positions(pos_map)

    return {"status": "ok", "recovered": len(recovered), "details": recovered}


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
    # active 포지션은 브로커 SL 주문이 살아있으므로 삭제하면 안 됨
    result = await session.execute(
        select(Position).where(Position.status.in_(["pending_buy", "pending_sell"]))
    )
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
    if pos.status == "active":
        return {"error": f"Active 포지션은 삭제 불가 ({pos.symbol} {pos.name})"}
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
    """잔고조회 API 1회로 DB 전체 정합성 맞춤 (reconcile).

    1. 유령 포지션 삭제 (DB에만 존재, 계좌에 없음)
    2. 미등록 종목 생성 (계좌에 있는데 DB에 없음)
    3. entry_price / qty 보정
    """
    settings = AppSettings()
    if settings.dashboard_token:
        token = request.query_params.get("token", "")
        if token != settings.dashboard_token:
            from fastapi import HTTPException
            raise HTTPException(status_code=401, detail="Unauthorized")

    from app.models.position import Position
    from app.models.order import Order
    from sqlalchemy import update as sql_update

    trade_client = getattr(request.app.state, "trade_client", None)
    if not trade_client:
        return {"error": "trade_client not initialized"}

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

    # Parse broker holdings
    broker_map = {}
    for item in data.get("output1", []):
        symbol = item.get("pdno", "")
        qty = int(item.get("hldg_qty", 0))
        if not symbol or qty <= 0:
            continue
        broker_map[symbol] = {
            "name": item.get("prdt_name", ""),
            "qty": qty,
            "avg_price": int(float(item.get("pchs_avg_pric", "0"))),
        }

    actions = []

    # 1. DB positions vs broker — 중복 제거 + 보정
    from datetime import date
    stmt = select(Position).where(Position.status.in_(["active", "pending_sell"]))
    db_positions = (await session.execute(stmt)).scalars().all()

    # Group by symbol to detect duplicates
    from collections import defaultdict
    symbol_groups: dict[str, list] = defaultdict(list)
    for pos in db_positions:
        symbol_groups[pos.symbol].append(pos)

    kept_symbols = set()
    for symbol, positions_list in symbol_groups.items():
        if symbol not in broker_map:
            # 유령 포지션 전부 삭제
            for pos in positions_list:
                await session.execute(
                    sql_update(Order).where(Order.position_id == pos.id).values(position_id=None)
                )
                await session.delete(pos)
                actions.append(f"삭제: {pos.symbol} {pos.name} ({pos.status})")
            continue

        broker = broker_map[symbol]
        # 중복이면 active 우선 보존, 나머지 삭제
        keep = None
        for pos in positions_list:
            if keep is None or (pos.status == "active" and keep.status != "active"):
                keep = pos
        for pos in positions_list:
            if pos is not keep:
                await session.execute(
                    sql_update(Order).where(Order.position_id == pos.id).values(position_id=None)
                )
                await session.delete(pos)
                actions.append(f"중복삭제: {pos.symbol} {pos.name} ({pos.status}, entry={pos.entry_price or 0:,})")

        # 보정
        changes = []
        if keep.status == "pending_sell":
            keep.status = "active"
            keep.exit_reason = None
            changes.append("pending_sell→active")
        if keep.entry_price != broker["avg_price"]:
            changes.append(f"가격 {keep.entry_price or 0:,}→{broker['avg_price']:,}")
            keep.holding_days = 0
            keep.entry_date = date.today()
            changes.append("holding_days→0")
            keep.entry_price = broker["avg_price"]
            keep.peak_price = broker["avg_price"]
        # entry_date 기반 holding_days 검증
        if keep.entry_date:
            expected_days = (date.today() - keep.entry_date).days
            if keep.holding_days != expected_days:
                changes.append(f"보유일 {keep.holding_days}→{expected_days}")
                keep.holding_days = expected_days
        elif keep.strategy == "manual":
            # sync로 생성된 manual 포지션은 entry_date=today
            keep.entry_date = date.today()
            if keep.holding_days != 0:
                changes.append(f"보유일 {keep.holding_days}→0")
                keep.holding_days = 0
        if keep.qty != broker["qty"]:
            changes.append(f"수량 {keep.qty or 0}→{broker['qty']}")
            keep.qty = broker["qty"]
        if changes:
            actions.append(f"보정: {keep.symbol} {keep.name} ({', '.join(changes)})")
        kept_symbols.add(symbol)

    # 2. 계좌에 있는데 DB에 없는 종목
    db_symbols = kept_symbols
    for symbol, info in broker_map.items():
        if symbol not in db_symbols:
            new_pos = Position(
                strategy="manual",
                symbol=symbol,
                name=info["name"],
                status="active",
                signal_date=date.today(),
                entry_date=date.today(),
                entry_price=info["avg_price"],
                qty=info["qty"],
                peak_price=info["avg_price"],
            )
            session.add(new_pos)
            actions.append(f"생성: {symbol} {info['name']} @ {info['avg_price']:,} x {info['qty']}")

    # 3. SL/Trail 보정 — 모든 active 포지션 (manual 포함)
    DEFAULT_SL_PCT = 0.03
    DEFAULT_TRAIL_PCT = 0.05
    all_active = (await session.execute(
        select(Position).where(Position.status == "active")
    )).scalars().all()
    strategy_configs = getattr(request.app.state, "strategy_configs", {})
    for pos in all_active:
        config = strategy_configs.get(pos.strategy)
        sl_pct = config.stop_loss_pct if config else DEFAULT_SL_PCT
        trail_pct = config.trailing_stop_pct if config else DEFAULT_TRAIL_PCT

        sl_changes = []
        if pos.entry_price and not pos.sl_price:
            pos.sl_price = int(pos.entry_price * (1 - sl_pct))
            sl_changes.append(f"SL={pos.sl_price:,}")
        if not pos.peak_price and pos.entry_price:
            pos.peak_price = pos.entry_price
            sl_changes.append(f"peak={pos.peak_price:,}")
        if pos.peak_price and not pos.trail_price:
            pos.trail_price = int(pos.peak_price * (1 - trail_pct))
            sl_changes.append(f"trail={pos.trail_price:,}")
        # peak 대비 trail 갱신 (더 높은 값으로)
        if pos.peak_price and pos.trail_price:
            new_trail = int(pos.peak_price * (1 - trail_pct))
            if new_trail > pos.trail_price:
                pos.trail_price = new_trail
                sl_changes.append(f"trail↑{new_trail:,}")
        if sl_changes:
            actions.append(f"SL보정: {pos.symbol} {pos.name} ({', '.join(sl_changes)})")

    await session.commit()

    # SL 모니터 갱신
    sl_monitor = getattr(request.app.state, "sl_monitor", None)
    if sl_monitor:
        from app.models import database as db_module
        async with db_module.async_session_factory() as s2:
            result = await s2.execute(select(Position).where(Position.status == "active"))
            positions = result.scalars().all()
            pos_map = {}
            strategy_configs = getattr(request.app.state, "strategy_configs", {})
            for p in positions:
                config = strategy_configs.get(p.strategy)
                pos_map[p.symbol] = {
                    "sl_price": p.sl_price or 0,
                    "trail_price": p.trail_price or 0,
                    "peak_price": p.peak_price or 0,
                    "trailing_stop_pct": config.trailing_stop_pct if config else 0.05,
                    "qty": p.qty or 0,
                }
            await sl_monitor.update_positions(pos_map)

    return {"status": "ok", "updated": len(actions), "details": actions}
