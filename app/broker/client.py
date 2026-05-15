from __future__ import annotations

import logging
from typing import Any

import httpx

from app.config import KISConfig

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
TIMEOUT = 10.0


class KISClient:
    """Base HTTP client for 한투 OpenAPI."""

    def __init__(self, config: KISConfig):
        self.config = config
        self._client = httpx.AsyncClient(
            base_url=config.base_url,
            timeout=TIMEOUT,
        )
        self._token: str = ""

    async def close(self):
        await self._client.aclose()

    def _headers(self, tr_id: str) -> dict[str, str]:
        return {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {self._token}",
            "appkey": self.config.app_key,
            "appsecret": self.config.app_secret,
            "tr_id": tr_id,
        }

    async def request(
        self,
        method: str,
        path: str,
        tr_id: str,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Make authenticated request with retry logic."""
        headers = self._headers(tr_id)

        for attempt in range(MAX_RETRIES):
            try:
                if method == "GET":
                    resp = await self._client.get(path, headers=headers, params=params)
                else:
                    resp = await self._client.post(path, headers=headers, json=json_body)

                data = resp.json()

                # Token expired — clear cache and get new token
                if resp.status_code == 401 or data.get("msg_cd") == "EGW00123":
                    if attempt == 0:
                        from app.broker.auth import _save_token_cache, _env_key
                        logger.info(f"Token expired, clearing cache ({_env_key(self.config)})")
                        await _save_token_cache(self.config, "")  # invalidate
                        await self.refresh_token()
                        headers = self._headers(tr_id)
                        continue
                    raise RuntimeError(f"KIS auth failed after refresh: {data}")

                if resp.status_code != 200:
                    raise RuntimeError(f"KIS API error {resp.status_code}: {data}")

                if data.get("rt_cd") != "0":
                    raise RuntimeError(f"KIS biz error: {data.get('msg1', data)}")

                return data

            except httpx.TimeoutException:
                if attempt < MAX_RETRIES - 1:
                    logger.warning(f"Timeout on attempt {attempt + 1}, retrying...")
                    continue
                raise

        raise RuntimeError("Max retries exceeded")

    async def refresh_token(self):
        """Refresh access token."""
        from app.broker.auth import get_access_token
        self._token = await get_access_token(self.config, self._client)

    @property
    def token(self) -> str:
        return self._token

    @token.setter
    def token(self, value: str):
        self._token = value
