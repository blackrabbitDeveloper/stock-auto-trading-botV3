from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, Request

from app.config import KISConfig, AppSettings, MarketFilterConfig, UniverseConfig, load_strategy_configs
from app.models import init_db, create_tables
from app.models import database as db_module
from app.broker.client import KISClient
from app.broker.order import KISOrderAPI
from app.broker.account import KISAccountAPI
from app.trader import OrderExecutor, SLManager
from app.notifier import DiscordNotifier
from app.dashboard import router as dashboard_router
from app.jobs import run_signal_job, run_order_job, run_confirm_job

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

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

    # Init broker
    client = KISClient(kis_config)
    try:
        await client.refresh_token()
    except Exception as e:
        logger.warning(f"Broker token refresh failed (will retry later): {e}")
    order_api = KISOrderAPI(client)
    account_api = KISAccountAPI(client)
    sl_manager = SLManager(order_api)
    executor = OrderExecutor(order_api, account_api, sl_manager)

    # Init notifier
    notifier = DiscordNotifier(settings.discord_webhook_url)

    # Store in app state
    app.state.client = client
    app.state.executor = executor
    app.state.notifier = notifier
    app.state.strategy_configs = strategy_configs

    # Schedule jobs
    market_filter_config = MarketFilterConfig()
    universe_config = UniverseConfig()

    async def _signal_job():
        async with db_module.async_session_factory() as session:
            await run_signal_job(session, strategy_configs, market_filter_config, universe_config, notifier)

    async def _order_job():
        async with db_module.async_session_factory() as session:
            await run_order_job(session, executor, strategy_configs, notifier)

    async def _confirm_job():
        async with db_module.async_session_factory() as session:
            await run_confirm_job(session, executor, notifier)

    async def _refresh_token():
        try:
            await client.refresh_token()
            logger.info("Token refreshed successfully")
        except Exception as e:
            logger.error(f"Token refresh failed: {e}")
            await notifier.send_error(f"Token refresh failed: {e}")

    scheduler.add_job(_signal_job, CronTrigger(hour=15, minute=40), id="signal_job", misfire_grace_time=300)
    scheduler.add_job(_order_job, CronTrigger(hour=8, minute=59), id="order_job", misfire_grace_time=60)
    scheduler.add_job(_confirm_job, CronTrigger(hour=9, minute=5), id="confirm_job", misfire_grace_time=60)
    scheduler.add_job(_refresh_token, CronTrigger(hour=8, minute=0), id="token_refresh")

    scheduler.start()
    logger.info(f"Scheduler started with {len(strategy_configs)} strategies")
    await notifier.send("🟢 Auto Trading Bot started")

    yield

    # Shutdown
    scheduler.shutdown()
    await client.close()
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
