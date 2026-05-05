from __future__ import annotations

from fastapi import APIRouter, Depends, Request, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select, func, desc
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import get_session, Position, Order, Trade
from app.config import AppSettings

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


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request, session: AsyncSession = Depends(get_session), _=Depends(verify_token)):
    # Active positions
    stmt = select(Position).where(Position.status.in_(["active", "pending_buy", "pending_sell"])).order_by(Position.strategy)
    positions = (await session.execute(stmt)).scalars().all()

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
            strategy_summary[pos.strategy] = {"count": 0}
        strategy_summary[pos.strategy]["count"] += 1

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "positions": positions,
        "trades": trades,
        "orders": orders,
        "strategy_summary": strategy_summary,
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
