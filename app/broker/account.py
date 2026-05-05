from __future__ import annotations

import logging
from dataclasses import dataclass

from app.broker.client import KISClient

logger = logging.getLogger(__name__)


@dataclass
class AccountBalance:
    total_eval: int
    cash: int
    stock_eval: int
    pnl_today: int


@dataclass
class FilledOrder:
    order_no: str
    symbol: str
    side: str
    qty: int
    price: int
    total_amount: int


class KISAccountAPI:
    """한투 잔고/체결 조회 API."""

    def __init__(self, client: KISClient):
        self.client = client

    async def get_balance(self) -> AccountBalance:
        """계좌 잔고 조회."""
        env = self.client.config.env
        tr_id = "VTTC8434R" if env == "paper" else "TTTC8434R"

        params = {
            "CANO": self.client.config.account_prefix,
            "ACNT_PRDT_CD": self.client.config.account_suffix,
            "AFHR_FLPR_YN": "N",
            "OFL_YN": "",
            "INQR_DVSN": "02",
            "UNPR_DVSN": "01",
            "FUND_STTL_ICLD_YN": "N",
            "FNCG_AMT_AUTO_RDPT_YN": "N",
            "PRCS_DVSN": "00",
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": "",
        }

        data = await self.client.request("GET", "/uapi/domestic-stock/v1/trading/inquire-balance", tr_id, params=params)
        output2 = data.get("output2", [{}])
        summary = output2[0] if output2 else {}

        return AccountBalance(
            total_eval=int(summary.get("tot_evlu_amt", 0)),
            cash=int(summary.get("dnca_tot_amt", 0)),
            stock_eval=int(summary.get("scts_evlu_amt", 0)),
            pnl_today=int(summary.get("evlu_pfls_smtl_amt", 0)),
        )

    async def get_filled_orders(self, date_str: str) -> list[FilledOrder]:
        """당일 체결 내역 조회."""
        env = self.client.config.env
        tr_id = "VTTC8001R" if env == "paper" else "TTTC8001R"

        params = {
            "CANO": self.client.config.account_prefix,
            "ACNT_PRDT_CD": self.client.config.account_suffix,
            "INQR_STRT_DT": date_str.replace("-", ""),
            "INQR_END_DT": date_str.replace("-", ""),
            "SLL_BUY_DVSN_CD": "00",
            "INQR_DVSN": "00",
            "PDNO": "",
            "CCLD_DVSN": "01",
            "ORD_GNO_BRNO": "",
            "ODNO": "",
            "INQR_DVSN_3": "00",
            "INQR_DVSN_1": "",
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": "",
        }

        data = await self.client.request("GET", "/uapi/domestic-stock/v1/trading/inquire-daily-ccld", tr_id, params=params)
        results = []
        for item in data.get("output1", []):
            side = "buy" if item.get("sll_buy_dvsn_cd") == "02" else "sell"
            results.append(FilledOrder(
                order_no=item.get("odno", ""),
                symbol=item.get("pdno", ""),
                side=side,
                qty=int(item.get("tot_ccld_qty", 0)),
                price=int(item.get("avg_prvs", 0)),
                total_amount=int(item.get("tot_ccld_amt", 0)),
            ))
        return results

    async def get_current_price(self, symbol: str) -> int:
        """현재가 조회."""
        tr_id = "FHKST01010100"
        params = {
            "FID_COND_MRKT_DIV_CD": "J",
            "FID_INPUT_ISCD": symbol,
        }
        data = await self.client.request("GET", "/uapi/domestic-stock/v1/quotations/inquire-price", tr_id, params=params)
        output = data.get("output", {})
        return int(output.get("stck_prpr", 0))
