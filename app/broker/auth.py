from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

import httpx

from app.config import KISConfig

logger = logging.getLogger(__name__)

TOKEN_MAX_AGE = 23 * 3600  # 23h (token valid 24h, use buffer)

# In-memory fallback (used when DB not available, e.g. during startup)
_mem_cache: dict[str, tuple[str, float]] = {}


def _env_key(config: KISConfig) -> str:
    """Per-environment cache key."""
    return "real" if "openapi.koreainvestment" in config.base_url and "vts" not in config.base_url else "paper"


async def _load_cached_token(config: KISConfig) -> str:
    """Load token from DB (primary) or memory (fallback)."""
    env = _env_key(config)
    # Try DB first
    try:
        from app.models import database as db_module
        if db_module.async_session_factory:
            from app.models.token_cache import TokenCache
            async with db_module.async_session_factory() as session:
                row = await session.get(TokenCache, env)
                if row:
                    age = (datetime.now(timezone.utc) - row.issued_at.replace(tzinfo=timezone.utc)).total_seconds()
                    if age < TOKEN_MAX_AGE:
                        _mem_cache[env] = (row.token, time.time())
                        return row.token
    except Exception as e:
        logger.debug(f"DB token cache read failed: {e}")
    # Fallback to memory
    if env in _mem_cache:
        token, ts = _mem_cache[env]
        if time.time() - ts < TOKEN_MAX_AGE:
            return token
    return ""


async def _save_token_cache(config: KISConfig, token: str):
    """Save token to DB + memory."""
    env = _env_key(config)
    _mem_cache[env] = (token, time.time())
    try:
        from app.models import database as db_module
        if not db_module.async_session_factory:
            return
        from app.models.token_cache import TokenCache
        async with db_module.async_session_factory() as session:
            existing = await session.get(TokenCache, env)
            if existing:
                existing.token = token
                existing.issued_at = datetime.now(timezone.utc)
            else:
                session.add(TokenCache(
                    env=env,
                    token=token,
                    issued_at=datetime.now(timezone.utc),
                ))
            await session.commit()
    except Exception as e:
        logger.warning(f"DB token cache write failed (memory cache still active): {e}")


async def get_access_token(config: KISConfig, client: httpx.AsyncClient) -> str:
    """Get or refresh OAuth access token (24h validity). DB-cached."""
    cached = await _load_cached_token(config)
    if cached:
        logger.info(f"Using cached token ({_env_key(config)})")
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
    await _save_token_cache(config, token)

    logger.info(f"New token issued and cached ({_env_key(config)})")
    return token


def clear_token_cache():
    """Clear all cached tokens (for testing)."""
    _mem_cache.clear()
    # File cache cleanup (legacy)
    from pathlib import Path
    cache_dir = Path("data")
    if cache_dir.exists():
        for f in cache_dir.glob("token_*.json"):
            f.unlink()
