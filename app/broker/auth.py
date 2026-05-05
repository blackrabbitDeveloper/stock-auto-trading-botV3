from __future__ import annotations

import json
import logging
import time
from pathlib import Path

import httpx

from app.config import KISConfig

logger = logging.getLogger(__name__)

TOKEN_CACHE_DIR = Path("data")
TOKEN_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _cache_path(config: KISConfig) -> Path:
    """Per-environment token cache file."""
    env = "real" if "openapi.koreainvestment" in config.base_url and "vts" not in config.base_url else "paper"
    return TOKEN_CACHE_DIR / f"token_{env}.json"


def _load_cached_token(config) -> str:
    """Load token from file cache if still valid."""
    path = _cache_path(config)
    if not path.exists():
        return ""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        # Token valid for 24h, use 23h buffer
        if time.time() - data.get("ts", 0) < 23 * 3600:
            return data.get("token", "")
    except Exception:
        pass
    return ""


def _save_token_cache(config, token: str):
    """Save token to file cache."""
    path = _cache_path(config)
    path.write_text(json.dumps({"token": token, "ts": time.time()}), encoding="utf-8")


async def get_access_token(config, client: httpx.AsyncClient) -> str:
    """Get or refresh OAuth access token (24h validity). File-cached."""
    # Check file cache first
    cached = _load_cached_token(config)
    if cached:
        logger.info(f"Using cached token ({_cache_path(config).name})")
        return cached

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

    token = data["access_token"]
    _save_token_cache(config, token)

    logger.info(f"New token issued and cached ({_cache_path(config).name})")
    return token


def clear_token_cache():
    """Clear all cached tokens (for testing)."""
    for f in TOKEN_CACHE_DIR.glob("token_*.json"):
        f.unlink()
