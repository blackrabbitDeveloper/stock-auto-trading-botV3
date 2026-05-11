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


def _to_kst(dt):
    """Convert naive UTC datetime to KST string."""
    if not dt:
        return ""
    from datetime import timezone, timedelta
    KST = timezone(timedelta(hours=9))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(KST).strftime("%m-%d %H:%M")


templates.env.filters["kst"] = _to_kst


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


async def _get_account_info(request: Request) -> tuple[dict, dict[str, int]]:
    """Fetch account info and per-stock prices from balance API.

    Returns (account_info, api_prices) where api_prices maps symbol -> current price.
    """
    try:
        trade_client = getattr(request.app.state, "trade_client", None)
        if not trade_client:
            raise RuntimeError("trade_client not initialized")

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

        output2 = data.get("output2", [{}])
        summary = output2[0] if output2 else {}

        # Extract per-stock current prices from output1
        api_prices = {}
        for item in data.get("output1", []):
            symbol = item.get("pdno", "")
            price = int(item.get("prpr", 0))
            if symbol and price > 0:
                api_prices[symbol] = price

        return {
            "env": env,
            "env_label": "모의투자" if env == "paper" else "실전",
            "account_no": trade_client.config.account_no,
            "total_eval": int(summary.get("tot_evlu_amt", 0)),
            "cash": int(summary.get("dnca_tot_amt", 0)),
            "stock_eval": int(summary.get("scts_evlu_amt", 0)),
            "pnl_today": int(summary.get("evlu_pfls_smtl_amt", 0)),
        }, api_prices
    except Exception as e:
        fallback_env = trade_client.config.env if trade_client else "paper"
        return {
            "env": fallback_env,
            "env_label": "모의투자" if fallback_env == "paper" else "실전",
            "account_no": "",
            "total_eval": 0,
            "cash": 0,
            "stock_eval": 0,
            "pnl_today": 0,
            "error": str(e),
        }, {}


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Login page."""
    return templates.TemplateResponse("index.html", {"request": request})


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request, session: AsyncSession = Depends(get_session), _=Depends(verify_token)):
    # Account info + API prices (fallback for after-hours)
    account, api_prices = await _get_account_info(request)

    # Active positions
    stmt = select(Position).where(Position.status.in_(["active", "pending_buy", "pending_sell"])).order_by(Position.strategy)
    positions = (await session.execute(stmt)).scalars().all()

    # Pending buys
    pending_buys = [p for p in positions if p.status == "pending_buy"]
    active_positions = [p for p in positions if p.status == "active"]
    pending_sells = [p for p in positions if p.status == "pending_sell"]

    # Merge prices: WebSocket (real-time) > API (fallback for after-hours)
    sl_monitor = getattr(request.app.state, "sl_monitor", None)
    ws_prices = sl_monitor.current_prices if sl_monitor else {}
    current_prices = {**api_prices, **ws_prices}  # WS overrides API

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
