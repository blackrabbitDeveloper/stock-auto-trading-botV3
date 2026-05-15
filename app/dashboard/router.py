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


async def _get_account_info(request: Request) -> tuple[dict, dict[str, int], list[dict]]:
    """Fetch account info, per-stock prices, and broker holdings from balance API.

    Returns (account_info, api_prices, broker_holdings).
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

        # Extract per-stock data from output1
        api_prices = {}
        broker_holdings = []
        for item in data.get("output1", []):
            symbol = item.get("pdno", "")
            qty = int(item.get("hldg_qty", 0))
            if not symbol or qty <= 0:
                continue
            price = int(item.get("prpr", 0))
            if price > 0:
                api_prices[symbol] = price
            broker_holdings.append({
                "symbol": symbol,
                "name": item.get("prdt_name", ""),
                "qty": qty,
                "avg_price": int(float(item.get("pchs_avg_pric", "0"))),
                "current_price": price,
                "eval_pnl": int(item.get("evlu_pfls_amt", 0)),
                "pnl_pct": float(item.get("evlu_pfls_rt", 0)),
            })

        return {
            "env": env,
            "env_label": "모의투자" if env == "paper" else "실전",
            "account_no": trade_client.config.account_no,
            "total_eval": int(summary.get("tot_evlu_amt", 0)),
            "cash": int(summary.get("dnca_tot_amt", 0)),
            "stock_eval": int(summary.get("scts_evlu_amt", 0)),
            "pnl_today": int(summary.get("evlu_pfls_smtl_amt", 0)),
        }, api_prices, broker_holdings
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
        }, {}, []


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Login page."""
    return templates.TemplateResponse("index.html", {"request": request})


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request, session: AsyncSession = Depends(get_session), _=Depends(verify_token)):
    # Account info + API prices + broker holdings
    account, api_prices, broker_holdings = await _get_account_info(request)

    # DB positions (for strategy/SL/trail metadata)
    stmt = select(Position).where(Position.status.in_(["active", "pending_buy", "pending_sell"])).order_by(Position.strategy)
    positions = (await session.execute(stmt)).scalars().all()

    pending_buys = [p for p in positions if p.status == "pending_buy"]
    pending_sells = [p for p in positions if p.status == "pending_sell"]

    # Merge prices: WebSocket (real-time) > API (fallback for after-hours)
    sl_monitor = getattr(request.app.state, "sl_monitor", None)
    ws_prices = sl_monitor.current_prices if sl_monitor else {}
    current_prices = {**api_prices, **ws_prices}  # WS overrides API

    # Build holdings view: broker holdings + DB position metadata
    db_pos_map = {p.symbol: p for p in positions if p.status in ("active", "pending_sell")}
    holdings = []
    broker_symbols = set()
    for h in broker_holdings:
        sym = h["symbol"]
        broker_symbols.add(sym)
        pos = db_pos_map.get(sym)
        cur_price = current_prices.get(sym, h["current_price"])
        holdings.append({
            "symbol": sym,
            "name": h["name"],
            "qty": h["qty"],
            "avg_price": h["avg_price"],
            "current_price": cur_price,
            "pnl_pct": ((cur_price / h["avg_price"] - 1) * 100) if (cur_price and h["avg_price"] > 0) else h["pnl_pct"],
            "strategy": pos.strategy if pos else None,
            "status": pos.status if pos else None,
            "sl_price": pos.sl_price if pos else None,
            "trail_price": pos.trail_price if pos else None,
            "holding_days": pos.holding_days if pos else None,
            "position_id": pos.id if pos else None,
            "has_ws": sym in ws_prices,
        })

    # DB에 active/pending_sell인데 브로커에 없는 종목 (이미 매도됐는데 DB 미반영)
    db_only = [p for p in positions if p.status in ("active", "pending_sell") and p.symbol not in broker_symbols]

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

    # Strategy performance stats from trades
    all_trades = (await session.execute(select(Trade).order_by(desc(Trade.created_at)))).scalars().all()
    strategy_stats = {}
    for t in all_trades:
        s = strategy_stats.setdefault(t.strategy, {"count": 0, "wins": 0, "total_pnl": 0, "total_return": 0.0})
        s["count"] += 1
        if t.pnl > 0:
            s["wins"] += 1
        s["total_pnl"] += t.pnl
        s["total_return"] += t.return_pct
    for s in strategy_stats.values():
        s["win_rate"] = (s["wins"] / s["count"] * 100) if s["count"] > 0 else 0
        s["avg_return"] = (s["total_return"] / s["count"] * 100) if s["count"] > 0 else 0
    # Trade에만 있는 전략도 strategy_summary에 추가
    for strat_name in strategy_stats:
        if strat_name not in strategy_summary:
            strategy_summary[strat_name] = {"active": 0, "pending_buy": 0, "pending_sell": 0}

    # Job history & WS status
    job_history = getattr(request.app.state, "job_history", {})
    ws_status = {
        "connected": sl_monitor._running if sl_monitor else False,
        "watching": len(sl_monitor._positions) if sl_monitor else 0,
        "prices_count": len(sl_monitor.current_prices) if sl_monitor else 0,
    }

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "account": account,
        "holdings": holdings,
        "db_only": db_only,
        "pending_buys": pending_buys,
        "pending_sells": pending_sells,
        "trades": trades,
        "orders": orders,
        "strategy_summary": strategy_summary,
        "current_prices": current_prices,
        "job_history": job_history,
        "ws_status": ws_status,
        "strategy_stats": strategy_stats,
        "trading_paused": getattr(request.app.state, "trading_paused", False),
    })


@router.post("/api/cancel-sell/{position_id}")
async def cancel_pending_sell(position_id: int, session: AsyncSession = Depends(get_session), _=Depends(verify_token)):
    """매도 예정 포지션을 active로 복구 (매도 취소)."""
    pos = await session.get(Position, position_id)
    if not pos or pos.status != "pending_sell":
        raise HTTPException(status_code=404, detail="Position not found or not pending_sell")
    pos.status = "active"
    pos.exit_reason = None
    await session.commit()
    return {"ok": True, "symbol": pos.symbol, "name": pos.name}


@router.post("/api/reset-holding/{position_id}")
async def reset_holding_days(position_id: int, session: AsyncSession = Depends(get_session), _=Depends(verify_token)):
    """보유일 리셋 (entry_date=today, holding_days=0)."""
    from datetime import date
    pos = await session.get(Position, position_id)
    if not pos:
        raise HTTPException(status_code=404, detail="Position not found")
    old_days = pos.holding_days
    pos.holding_days = 0
    pos.entry_date = date.today()
    await session.commit()
    return {"ok": True, "symbol": pos.symbol, "name": pos.name, "old_days": old_days}


@router.post("/api/delete-position/{position_id}")
async def delete_position(position_id: int, session: AsyncSession = Depends(get_session), _=Depends(verify_token)):
    """유령 포지션 삭제 (계좌에 없는 DB 레코드 정리)."""
    pos = await session.get(Position, position_id)
    if not pos:
        raise HTTPException(status_code=404, detail="Position not found")
    from sqlalchemy import update as sql_update
    await session.execute(
        sql_update(Order).where(Order.position_id == pos.id).values(position_id=None)
    )
    await session.delete(pos)
    await session.commit()
    return {"ok": True, "symbol": pos.symbol, "name": pos.name}


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
