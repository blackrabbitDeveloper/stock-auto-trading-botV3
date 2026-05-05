import pytest
from unittest.mock import AsyncMock
from app.broker.order import KISOrderAPI, OrderResult
from app.broker.client import KISClient
from app.config import KISConfig


@pytest.fixture
def mock_client():
    config = KISConfig(app_key="k", app_secret="s", account_no="12345678-01", env="paper")
    client = KISClient(config)
    client.request = AsyncMock()
    return client


@pytest.mark.asyncio
async def test_buy_market_success(mock_client):
    mock_client.request.return_value = {"output": {"ODNO": "0001234567"}, "rt_cd": "0"}
    api = KISOrderAPI(mock_client)
    result = await api.buy_market("005930", 10)
    assert result.success is True
    assert result.order_no == "0001234567"


@pytest.mark.asyncio
async def test_sell_market_success(mock_client):
    mock_client.request.return_value = {"output": {"ODNO": "0001234568"}, "rt_cd": "0"}
    api = KISOrderAPI(mock_client)
    result = await api.sell_market("005930", 10)
    assert result.success is True


@pytest.mark.asyncio
async def test_set_stop_loss(mock_client):
    mock_client.request.return_value = {"output": {"ODNO": "0001234569"}, "rt_cd": "0"}
    api = KISOrderAPI(mock_client)
    result = await api.set_stop_loss("005930", 10, 70000)
    assert result.success is True


@pytest.mark.asyncio
async def test_buy_market_failure(mock_client):
    mock_client.request.side_effect = RuntimeError("주문 거부")
    api = KISOrderAPI(mock_client)
    result = await api.buy_market("005930", 10)
    assert result.success is False
    assert "주문 거부" in result.message
