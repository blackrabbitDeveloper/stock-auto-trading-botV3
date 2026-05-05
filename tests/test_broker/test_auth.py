import pytest
from unittest.mock import AsyncMock, MagicMock
from app.broker.auth import get_access_token, clear_token_cache
from app.config import KISConfig


@pytest.fixture(autouse=True)
def reset_cache():
    clear_token_cache()
    yield
    clear_token_cache()


def _make_mock_client(json_data):
    mock_client = AsyncMock()
    mock_resp = MagicMock()
    mock_resp.json.return_value = json_data
    mock_client.post.return_value = mock_resp
    return mock_client


@pytest.mark.asyncio
async def test_get_access_token_success():
    config = KISConfig(app_key="key", app_secret="secret", account_no="12345678-01", env="paper")
    mock_client = _make_mock_client({
        "access_token": "test_token_123",
        "token_type": "Bearer",
        "expires_in": 86400,
    })

    token = await get_access_token(config, mock_client)
    assert token == "test_token_123"


@pytest.mark.asyncio
async def test_get_access_token_cached():
    config = KISConfig(app_key="key", app_secret="secret", account_no="12345678-01", env="paper")
    mock_client = _make_mock_client({
        "access_token": "test_token_123",
        "token_type": "Bearer",
        "expires_in": 86400,
    })

    token1 = await get_access_token(config, mock_client)
    token2 = await get_access_token(config, mock_client)
    assert token1 == token2
    assert mock_client.post.call_count == 1


@pytest.mark.asyncio
async def test_get_access_token_failure():
    config = KISConfig(app_key="key", app_secret="secret", account_no="12345678-01", env="paper")
    mock_client = _make_mock_client({"error": "invalid_client"})

    with pytest.raises(RuntimeError, match="Token request failed"):
        await get_access_token(config, mock_client)
