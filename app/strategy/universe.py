from __future__ import annotations

import pandas as pd

from app.config import UniverseConfig


def is_preferred_stock(symbol: str) -> bool:
    if len(symbol) == 6 and symbol[-1] in ("5", "7", "8", "9"):
        return True
    return False


def is_spac(name: str) -> bool:
    spac_keywords = ["스팩", "SPAC", "기업인수"]
    return any(kw in name for kw in spac_keywords)


def filter_universe(
    symbols: list[str],
    data_map: dict[str, pd.DataFrame],
    config: UniverseConfig,
    name_map: dict[str, str] | None = None,
) -> list[str]:
    name_map = name_map or {}
    filtered = []

    for symbol in symbols:
        if config.exclude_preferred and is_preferred_stock(symbol):
            continue
        if config.exclude_spac and is_spac(name_map.get(symbol, "")):
            continue
        if symbol not in data_map:
            continue

        df = data_map[symbol]
        if len(df) < 20:
            continue

        last_row = df.iloc[-1]
        if last_row["close"] < config.min_price:
            continue
        if "avg_trading_value_20" in df.columns:
            if last_row["avg_trading_value_20"] < config.min_avg_trading_value_20:
                continue

        filtered.append(symbol)

    return filtered
