from __future__ import annotations

import logging
from dataclasses import dataclass

from app.broker.client import KISClient

logger = logging.getLogger(__name__)


@dataclass
class OrderResult:
    success: bool
    order_no: str = ""
    message: str = ""


class KISOrderAPI:
    """한투 주문 API 래퍼."""

    TR_IDS = {
        "paper": {"buy": "VTTC0012U", "sell": "VTTC0011U"},
        "real": {"buy": "TTTC0012U", "sell": "TTTC0011U"},
    }

    def __init__(self, client: KISClient):
        self.client = client

    def _tr_id(self, side: str) -> str:
        env = self.client.config.env
        return self.TR_IDS[env][side]

    async def buy_market(self, symbol: str, qty: int) -> OrderResult:
        """시장가 매수."""
        return await self._place_order(symbol, qty, "buy", ord_type="01", price=0)

    async def sell_market(self, symbol: str, qty: int) -> OrderResult:
        """시장가 매도."""
        return await self._place_order(symbol, qty, "sell", ord_type="01", price=0)

    async def set_stop_loss(self, symbol: str, qty: int, price: int) -> OrderResult:
        """지정가 매도 (SL 예약주문)."""
        return await self._place_order(symbol, qty, "sell", ord_type="00", price=price)

    async def cancel_order(self, order_no: str, qty: int) -> OrderResult:
        """주문 취소."""
        env = self.client.config.env
        tr_id = "VTTC0013U" if env == "paper" else "TTTC0013U"

        body = {
            "CANO": self.client.config.account_prefix,
            "ACNT_PRDT_CD": self.client.config.account_suffix,
            "KRX_FWDG_ORD_ORGNO": "",
            "ORGN_ODNO": order_no,
            "ORD_DVSN": "00",
            "RVSE_CNCL_DVSN_CD": "02",
            "ORD_QTY": str(qty),
            "ORD_UNPR": "0",
        }

        try:
            data = await self.client.request("POST", "/uapi/domestic-stock/v1/trading/order-rvsecncl", tr_id, json_body=body)
            return OrderResult(success=True, order_no=data.get("output", {}).get("ODNO", ""))
        except Exception as e:
            logger.error(f"Cancel order failed: {e}")
            return OrderResult(success=False, message=str(e))

    async def _place_order(self, symbol: str, qty: int, side: str, ord_type: str, price: int) -> OrderResult:
        tr_id = self._tr_id(side)
        body = {
            "CANO": self.client.config.account_prefix,
            "ACNT_PRDT_CD": self.client.config.account_suffix,
            "PDNO": symbol,
            "ORD_DVSN": ord_type,
            "ORD_QTY": str(qty),
            "ORD_UNPR": str(price),
        }

        try:
            data = await self.client.request("POST", "/uapi/domestic-stock/v1/trading/order-cash", tr_id, json_body=body)
            output = data.get("output", {})
            return OrderResult(success=True, order_no=output.get("ODNO", ""))
        except Exception as e:
            logger.error(f"Order failed [{side} {symbol} qty={qty}]: {e}")
            return OrderResult(success=False, message=str(e))
