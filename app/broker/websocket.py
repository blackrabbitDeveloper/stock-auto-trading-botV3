"""WebSocket real-time price monitor for stop-loss execution.

Monitors active positions and triggers market sell when price hits max(sl, trail).
Runs during market hours (09:00 ~ 15:30 KST).
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, time as dt_time
from typing import Callable

import httpx
import websockets

from app.config import KISConfig

logger = logging.getLogger(__name__)

# Market hours (KST)
MARKET_OPEN = dt_time(9, 0)
MARKET_CLOSE = dt_time(15, 30)

# WebSocket URLs
WS_URL_REAL = "ws://ops.koreainvestment.com:21000"
WS_URL_PAPER = "ws://ops.koreainvestment.com:31000"


async def get_approval_key(config: KISConfig) -> str:
    """Get WebSocket approval key."""
    url = f"{config.base_url}/oauth2/Approval"
    body = {
        "grant_type": "client_credentials",
        "appkey": config.app_key,
        "secretkey": config.app_secret,
    }
    async with httpx.AsyncClient() as client:
        resp = await client.post(url, json=body, timeout=10.0)
        data = resp.json()
        key = data.get("approval_key", "")
        if not key:
            raise RuntimeError(f"WebSocket approval key failed: {data}")
        return key


class StopLossMonitor:
    """Real-time price monitor that triggers sell on SL/Trail hit."""

    def __init__(self, config: KISConfig, on_sl_hit: Callable):
        """
        Args:
            config: KIS API config (determines real/paper URL)
            on_sl_hit: async callback(symbol, price, reason) when SL triggered
        """
        self.config = config
        self.on_sl_hit = on_sl_hit
        self.ws_url = WS_URL_REAL if config.env == "real" else WS_URL_PAPER
        self._positions: dict[str, dict] = {}  # symbol -> {sl_price, trail_price, qty}
        self._running = False
        self._ws = None
        self.current_prices: dict[str, int] = {}  # symbol -> latest price (for dashboard)

    async def update_positions(self, positions: dict[str, dict]):
        """Update monitored positions. Call after signal_job updates trail prices.

        positions: {symbol: {"sl_price": int, "trail_price": int, "qty": int}}
        Subscribes to new symbols on the active WebSocket connection.
        """
        old_symbols = set(self._positions.keys())
        new_symbols = set(positions.keys()) - old_symbols
        self._positions = positions

        # Subscribe to newly added symbols on the live WebSocket
        if new_symbols and self._ws:
            try:
                approval_key = await get_approval_key(self.config)
                for symbol in new_symbols:
                    request = {
                        "header": {
                            "approval_key": approval_key,
                            "custtype": "P",
                            "tr_type": "1",
                            "content-type": "utf-8",
                        },
                        "body": {
                            "input": {
                                "tr_id": "H0STCNT0",
                                "tr_key": symbol,
                            }
                        },
                    }
                    await self._ws.send(json.dumps(request))
                    await asyncio.sleep(0.1)
                logger.info(f"Subscribed to {len(new_symbols)} new symbols: {new_symbols}")
            except Exception as e:
                logger.error(f"Failed to subscribe new symbols: {e}")

        logger.info(f"SL monitor: watching {len(positions)} positions")

    async def start(self):
        """Start the WebSocket monitor loop."""
        self._running = True
        while self._running:
            try:
                await self._run_session()
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                if self._running:
                    await asyncio.sleep(5)  # reconnect after 5s

    async def stop(self):
        """Stop the monitor."""
        self._running = False
        if self._ws:
            await self._ws.close()

    async def _run_session(self):
        """Single WebSocket session."""
        approval_key = await get_approval_key(self.config)
        logger.info(f"WebSocket connecting to {self.ws_url}")

        async with websockets.connect(self.ws_url, ping_interval=60) as ws:
            self._ws = ws
            logger.info("WebSocket connected")

            # Subscribe to active positions
            await self._subscribe_all(ws, approval_key)

            # Receive loop
            async for message in ws:
                if not self._running:
                    break
                if not self._is_market_hours():
                    continue
                await self._handle_message(message)

    async def _subscribe_all(self, ws, approval_key: str):
        """Subscribe to real-time prices for all monitored positions."""
        for symbol in self._positions:
            request = {
                "header": {
                    "approval_key": approval_key,
                    "custtype": "P",
                    "tr_type": "1",  # 1=subscribe
                    "content-type": "utf-8",
                },
                "body": {
                    "input": {
                        "tr_id": "H0STCNT0",  # 실시간 체결가
                        "tr_key": symbol,
                    }
                },
            }
            await ws.send(json.dumps(request))
            await asyncio.sleep(0.1)
        logger.info(f"Subscribed to {len(self._positions)} symbols")

    async def _handle_message(self, message: str):
        """Parse real-time price and check SL."""
        # Skip non-data messages (subscription confirmations, pings)
        if message.startswith("{"):
            return  # JSON control message

        # Data format: "0|H0STCNT0|004|005930^현재가^..." (pipe separated header, ^ separated data)
        parts = message.split("|")
        if len(parts) < 4:
            return

        tr_id = parts[1]
        if tr_id != "H0STCNT0":
            return

        # Parse data fields (^ separated)
        data_fields = parts[3].split("^")
        if len(data_fields) < 3:
            return

        symbol = data_fields[0]  # 종목코드
        current_price = int(data_fields[2])  # 체결가 (3번째 필드)

        # Store latest price for dashboard
        self.current_prices[symbol] = current_price

        # Check SL
        if symbol not in self._positions:
            return

        pos = self._positions[symbol]

        sl_price = pos.get("sl_price", 0)
        trail_price = pos.get("trail_price", 0)
        exit_trigger = max(sl_price, trail_price)

        if exit_trigger > 0 and current_price <= exit_trigger:
            reason = "trailing_stop" if trail_price > sl_price else "stop_loss"
            logger.warning(f"SL HIT: {symbol} price={current_price} trigger={exit_trigger} ({reason})")

            # Remove from monitoring (prevent duplicate triggers)
            del self._positions[symbol]

            # Trigger sell callback
            await self.on_sl_hit(symbol, current_price, reason)

    @staticmethod
    def _is_market_hours() -> bool:
        """Check if current time is within market hours (KST)."""
        from zoneinfo import ZoneInfo
        now = datetime.now(ZoneInfo("Asia/Seoul")).time()
        return MARKET_OPEN <= now <= MARKET_CLOSE
