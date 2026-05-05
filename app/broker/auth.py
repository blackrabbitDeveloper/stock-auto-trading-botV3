from __future__ import annotations

import logging
import time

import httpx

from app.config import KISConfig

logger = logging.getLogger(__name__)

_cached_token: str = ""
_token_expires_at: float = 0.0


async def get_access_token(config: KISConfig, client: httpx.AsyncClient) -> str:
    """Get or refresh OAuth access token (24h validity)."""
    global _cached_token, _token_expires_at

    if _cached_token and time.time() < _token_expires_at - 3600:
        return _cached_token

    path = "/oauth2/tokenP"
    body = {
        "grant_type": "client_credentials",
        "appkey": config.app_key,
        "appsecret": config.app_secret,
    }

    resp = await client.post(
        f"{config.base_url}{path}",
        json=body,
        timeout=10.0,
    )
    data = resp.json()

    if "access_token" not in data:
        raise RuntimeError(f"Token request failed: {data}")

    _cached_token = data["access_token"]
    _token_expires_at = time.time() + 23 * 3600

    logger.info("KIS access token refreshed")
    return _cached_token


def clear_token_cache():
    """Clear cached token (for testing)."""
    global _cached_token, _token_expires_at
    _cached_token = ""
    _token_expires_at = 0.0
