import pytest
from unittest.mock import AsyncMock
from app.trader.sl_manager import SLManager
from app.broker.order import KISOrderAPI, OrderResult
from app.models.position import Position
from datetime import date


@pytest.fixture
def sl_manager():
    mock_order_api = AsyncMock(spec=KISOrderAPI)
    return SLManager(mock_order_api)


@pytest.mark.asyncio
async def test_register_sl_success(sl_manager):
    sl_manager.order_api.set_stop_loss.return_value = OrderResult(success=True, order_no="SL001")
    pos = Position(symbol="005930", qty=10, sl_price=70000, strategy="vol", name="삼성", status="active", signal_date=date.today())

    result = await sl_manager.register_sl(pos)
    assert result.success is True
    assert result.order_no == "SL001"


@pytest.mark.asyncio
async def test_register_sl_missing_price(sl_manager):
    pos = Position(symbol="005930", qty=10, sl_price=None, strategy="vol", name="삼성", status="active", signal_date=date.today())
    result = await sl_manager.register_sl(pos)
    assert result.success is False


@pytest.mark.asyncio
async def test_update_sl(sl_manager):
    sl_manager.order_api.cancel_order.return_value = OrderResult(success=True)
    sl_manager.order_api.set_stop_loss.return_value = OrderResult(success=True, order_no="SL002")
    pos = Position(symbol="005930", qty=10, sl_price=70000, sl_order_no="SL001", strategy="vol", name="삼성", status="active", signal_date=date.today())

    result = await sl_manager.update_sl(pos, 71000)
    assert result.success is True
    sl_manager.order_api.cancel_order.assert_called_once_with("SL001", 10)
