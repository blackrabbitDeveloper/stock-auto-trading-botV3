from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pandas as pd
from pykrx import stock as pykrx_stock

from app.config import MarketFilterConfig

logger = logging.getLogger(__name__)


def check_market_filter(config: MarketFilterConfig, today_str: str) -> bool:
    """Check if market index is above MA. Returns True if entries are allowed."""
    if not config.enabled:
        return True

    try:
        end = today_str.replace("-", "")
        start_dt = datetime.strptime(today_str, "%Y-%m-%d") - timedelta(days=60)
        start = start_dt.strftime("%Y%m%d")

        index_df = pykrx_stock.get_index_ohlcv(start, end, config.index_code)
        if index_df.empty:
            logger.warning("Market filter: no index data, allowing entries")
            return True

        ma = index_df["종가"].rolling(config.ma_period).mean()
        last_close = index_df["종가"].iloc[-1]
        last_ma = ma.iloc[-1]

        if pd.isna(last_ma):
            return True

        allowed = last_close > last_ma
        logger.info(f"Market filter: KOSPI {last_close:,.0f} vs MA{config.ma_period} {last_ma:,.0f} -> {'OPEN' if allowed else 'BLOCKED'}")
        return allowed

    except Exception as e:
        logger.error(f"Market filter error: {e}")
        return True
