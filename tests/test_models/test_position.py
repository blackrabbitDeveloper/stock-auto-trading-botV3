from app.models.position import Position
from app.models.order import Order
from app.models.trade import Trade
from datetime import date


def test_position_model_instantiation():
    pos = Position(
        strategy="volume_breakout",
        symbol="005930",
        name="삼성전자",
        status="pending_buy",
        signal_date=date(2026, 5, 5),
    )
    assert pos.strategy == "volume_breakout"
    assert pos.status == "pending_buy"


def test_order_model_instantiation():
    order = Order(
        strategy="volume_breakout",
        symbol="005930",
        side="buy",
        order_type="market",
        qty=10,
        price=0,
        status="submitted",
    )
    assert order.side == "buy"
    assert order.price == 0


def test_trade_model_instantiation():
    trade = Trade(
        strategy="volume_breakout",
        symbol="005930",
        name="삼성전자",
        entry_date=date(2026, 5, 1),
        exit_date=date(2026, 5, 5),
        entry_price=72000,
        exit_price=75000,
        qty=10,
        return_pct=0.0417,
        pnl=30000,
        holding_days=4,
        exit_reason="trailing_stop",
    )
    assert trade.return_pct == 0.0417
