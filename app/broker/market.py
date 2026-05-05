from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pandas as pd

from app.broker.client import KISClient

logger = logging.getLogger(__name__)


class KISMarketAPI:
    """한투 시세 조회 API."""

    def __init__(self, client: KISClient):
        self.client = client

    async def get_daily_ohlcv(self, symbol: str, period: int = 100) -> pd.DataFrame:
        """일별 OHLCV 조회 (최대 100일).

        Returns DataFrame with columns: open, high, low, close, volume
        Index: DatetimeIndex
        """
        tr_id = "FHKST01010400"
        today = datetime.now().strftime("%Y%m%d")

        params = {
            "FID_COND_MRKT_DIV_CD": "J",
            "FID_INPUT_ISCD": symbol,
            "FID_INPUT_DATE_1": (datetime.now() - timedelta(days=period * 2)).strftime("%Y%m%d"),
            "FID_INPUT_DATE_2": today,
            "FID_PERIOD_DIV_CD": "D",
            "FID_ORG_ADJ_PRC": "0",
        }

        try:
            data = await self.client.request("GET", "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice", tr_id, params=params)
            items = data.get("output2", [])
            if not items:
                return pd.DataFrame()

            rows = []
            for item in items:
                try:
                    rows.append({
                        "date": pd.Timestamp(item["stck_bsop_date"]),
                        "open": int(item["stck_oprc"]),
                        "high": int(item["stck_hgpr"]),
                        "low": int(item["stck_lwpr"]),
                        "close": int(item["stck_clpr"]),
                        "volume": int(item["acml_vol"]),
                    })
                except (KeyError, ValueError):
                    continue

            if not rows:
                return pd.DataFrame()

            df = pd.DataFrame(rows).set_index("date").sort_index()
            return df

        except Exception as e:
            logger.debug(f"OHLCV fetch failed for {symbol}: {e}")
            return pd.DataFrame()

    async def get_index_price(self, index_code: str = "0001") -> pd.DataFrame:
        """지수 일봉 조회 (KOSPI=0001, KOSDAQ=1001).

        Returns DataFrame with columns: close
        """
        tr_id = "FHKUP03500100"

        today = datetime.now().strftime("%Y%m%d")
        start = (datetime.now() - timedelta(days=60)).strftime("%Y%m%d")

        params = {
            "FID_COND_MRKT_DIV_CD": "U",
            "FID_INPUT_ISCD": index_code,
            "FID_INPUT_DATE_1": start,
            "FID_INPUT_DATE_2": today,
            "FID_PERIOD_DIV_CD": "D",
        }

        try:
            data = await self.client.request("GET", "/uapi/domestic-stock/v1/quotations/inquire-daily-indexchartprice", tr_id, params=params)
            items = data.get("output2", [])
            if not items:
                return pd.DataFrame()

            rows = []
            for item in items:
                try:
                    rows.append({
                        "date": pd.Timestamp(item["stck_bsop_date"]),
                        "close": float(item["bstp_nmix_prpr"]),
                    })
                except (KeyError, ValueError):
                    continue

            if not rows:
                return pd.DataFrame()

            df = pd.DataFrame(rows).set_index("date").sort_index()
            return df

        except Exception as e:
            logger.warning(f"Index fetch failed: {e}")
            return pd.DataFrame()

    async def get_stock_name(self, symbol: str) -> str:
        """종목명 조회."""
        tr_id = "FHKST01010100"
        params = {
            "FID_COND_MRKT_DIV_CD": "J",
            "FID_INPUT_ISCD": symbol,
        }
        try:
            data = await self.client.request("GET", "/uapi/domestic-stock/v1/quotations/inquire-price", tr_id, params=params)
            output = data.get("output", {})
            return output.get("hts_kor_isnm", symbol)
        except Exception:
            return symbol
