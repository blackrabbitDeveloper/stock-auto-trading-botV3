from __future__ import annotations

import logging
import os

import httpx

logger = logging.getLogger(__name__)


class DiscordNotifier:
    """Send alerts via Discord webhook."""

    def __init__(self, webhook_url: str = ""):
        self.webhook_url = webhook_url or os.environ.get("DISCORD_WEBHOOK_URL", "")

    async def send(self, message: str) -> bool:
        if not self.webhook_url:
            logger.warning("Discord webhook not configured")
            return False

        chunks = self._split(message, 1900)
        async with httpx.AsyncClient() as client:
            for chunk in chunks:
                try:
                    resp = await client.post(
                        self.webhook_url,
                        json={"content": chunk},
                        timeout=10.0,
                    )
                    if resp.status_code not in (200, 204):
                        logger.error(f"Discord error: {resp.status_code}")
                        return False
                except Exception as e:
                    logger.error(f"Discord send failed: {e}")
                    return False
        return True

    async def send_signal_alert(self, alerts: dict) -> bool:
        msg = self._format_signal_alert(alerts)
        return await self.send(msg)

    async def send_order_alert(self, results: list[dict], title: str = "주문 결과") -> bool:
        lines = [f"## {title}"]
        for r in results:
            icon = "✅" if r.get("success") else "❌"
            lines.append(f"{icon} {r.get('symbol', '')} {r.get('name', '')} — {r.get('message', 'OK')}")
        return await self.send("\n".join(lines))

    async def send_error(self, error: str) -> bool:
        return await self.send(f"🚨 **ERROR:** {error}")

    def _format_signal_alert(self, alerts: dict) -> str:
        lines = [f"## 📊 Signal Alert {alerts.get('date', '')}"]

        if alerts.get("pending_sells"):
            lines.append("\n**🔻 SELL Tomorrow**")
            for s in alerts["pending_sells"]:
                lines.append(f"> {s['symbol']} {s['name']} | {s['exit_reason']} | {s.get('return_pct', 0):+.1%}")

        if alerts.get("pending_buys"):
            lines.append("\n**🔺 BUY Tomorrow**")
            for b in alerts["pending_buys"]:
                lines.append(f"> {b['symbol']} {b['name']} | score {b.get('score', 0):.1f} | SL {b.get('sl_price', 0):,}")

        if not alerts.get("pending_sells") and not alerts.get("pending_buys"):
            lines.append("\n💤 No action needed")

        return "\n".join(lines)

    @staticmethod
    def _split(text: str, max_len: int) -> list[str]:
        if len(text) <= max_len:
            return [text]
        chunks = []
        current = ""
        for line in text.split("\n"):
            if len(current) + len(line) + 1 > max_len:
                chunks.append(current)
                current = line
            else:
                current = f"{current}\n{line}" if current else line
        if current:
            chunks.append(current)
        return chunks
