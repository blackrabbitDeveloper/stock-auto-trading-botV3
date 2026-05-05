from __future__ import annotations

import pandas as pd


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Add technical indicators to OHLCV DataFrame.

    Expects columns: open, high, low, close, volume.
    """
    df = df.copy()

    # Moving averages
    df["ma5"] = df["close"].rolling(5).mean()
    df["ma10"] = df["close"].rolling(10).mean()
    df["ma20"] = df["close"].rolling(20).mean()
    df["ma60"] = df["close"].rolling(60).mean()

    # Volume
    df["avg_volume_20"] = df["volume"].rolling(20).mean()

    # Trading value
    df["trading_value"] = df["close"] * df["volume"]
    df["avg_trading_value_20"] = df["trading_value"].rolling(20).mean()

    # Returns
    df["return_1d"] = df["close"].pct_change(1)
    df["return_3d"] = df["close"] / df["close"].shift(3) - 1
    df["return_5d"] = df["close"] / df["close"].shift(5) - 1
    df["return_20d"] = df["close"] / df["close"].shift(20) - 1

    # High/Low ranges
    df["high_20"] = df["high"].rolling(20).max()
    df["low_20"] = df["low"].rolling(20).min()

    # ATR
    tr1 = df["high"] - df["low"]
    tr2 = (df["high"] - df["close"].shift(1)).abs()
    tr3 = (df["low"] - df["close"].shift(1)).abs()
    df["true_range"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df["atr14"] = df["true_range"].rolling(14).mean()

    return df
