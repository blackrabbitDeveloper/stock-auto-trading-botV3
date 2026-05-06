from __future__ import annotations

from fastapi import APIRouter, Depends, Request, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select, func, desc
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import get_session, Position, Order, Trade
from app.config import AppSettings, KISConfig

router = APIRouter()
templates = Jinja2Templates(directory="app/dashboard/templates")


def verify_token(request: Request):
    """Simple bearer token auth for dashboard."""
    settings = AppSettings()
    if not settings.dashboard_token:
        return
    token = request.query_params.get("token", "")
    if token != settings.dashboard_token:
        auth = request.headers.get("Authorization", "")
        if auth != f"Bearer {settings.dashboard_token}":
            raise HTTPException(status_code=401, detail="Unauthorized")


async def _get_account_info(request: Request) -> dict:
    """Fetch account info using the trading client from app state."""
    try:
        from app.broker.account import KISAccountAPI

        # Use the trade_client from app.state (paper or real depending on KIS_ENV)
        trade_client = getattr(request.app.state, "trade_client", None)
        if not trade_client:
            raise RuntimeError("trade_client not initialized")

        account_api = KISAccountAPI(trade_client)
        balance = await account_api.get_balance()

        kis_config = KISConfig()
        env = kis_config.env

        return {
            "env": env,
            "env_label": "모의투자" if env == "paper" else "실전",
            "account_no": trade_client.config.account_no,
            "total_eval": balance.total_eval,
            "cash": balance.cash,
            "stock_eval": balance.stock_eval,
            "pnl_today": balance.pnl_today,
        }
    except Exception as e:
        kis_config = KISConfig()
        return {
            "env": kis_config.env,
            "env_label": "모의투자" if kis_config.env == "paper" else "실전",
            "account_no": "",
            "total_eval": 0,
            "cash": 0,
            "stock_eval": 0,
            "pnl_today": 0,
            "error": str(e),
        }


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Login page."""
    return templates.TemplateResponse("index.html", {"request": request})


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request, session: AsyncSession = Depends(get_session), _=Depends(verify_token)):
    # Account info
    account = await _get_account_info(request)

    # Active positions
    stmt = select(Position).where(Position.status.in_(["active", "pending_buy", "pending_sell"])).order_by(Position.strategy)
    positions = (await session.execute(stmt)).scalars().all()

    # Pending buys
    pending_buys = [p for p in positions if p.status == "pending_buy"]
    active_positions = [p for p in positions if p.status == "active"]
    pending_sells = [p for p in positions if p.status == "pending_sell"]

    # Get real-time prices from WebSocket monitor
    sl_monitor = getattr(request.app.state, "sl_monitor", None)
    current_prices = sl_monitor.current_prices if sl_monitor else {}

    # Recent trades
    stmt = select(Trade).order_by(desc(Trade.created_at)).limit(10)
    trades = (await session.execute(stmt)).scalars().all()

    # Today's orders
    stmt = select(Order).order_by(desc(Order.submitted_at)).limit(20)
    orders = (await session.execute(stmt)).scalars().all()

    # Strategy summary
    strategy_summary = {}
    for pos in positions:
        if pos.strategy not in strategy_summary:
            strategy_summary[pos.strategy] = {"active": 0, "pending_buy": 0, "pending_sell": 0}
        strategy_summary[pos.strategy][pos.status] += 1

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "account": account,
        "positions": active_positions,
        "pending_buys": pending_buys,
        "pending_sells": pending_sells,
        "trades": trades,
        "orders": orders,
        "strategy_summary": strategy_summary,
        "current_prices": current_prices,
    })


@router.get("/api/status")
async def api_status(session: AsyncSession = Depends(get_session), _=Depends(verify_token)):
    stmt = select(func.count()).select_from(Position).where(Position.status == "active")
    active_count = (await session.execute(stmt)).scalar()

    stmt = select(func.count()).select_from(Position).where(Position.status == "pending_buy")
    pending_buy_count = (await session.execute(stmt)).scalar()

    return {
        "active_positions": active_count,
        "pending_buys": pending_buy_count,
        "status": "running",
    }
