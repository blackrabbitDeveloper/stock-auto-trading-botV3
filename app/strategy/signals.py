from __future__ import annotations

import pandas as pd
import numpy as np

from app.config import StrategyParams


def _check_consecutive_volume_increase(volume: pd.Series, days: int) -> pd.Series:
    result = pd.Series(True, index=volume.index)
    for i in range(1, days + 1):
        result = result & (volume > volume.shift(i))
    return result


def generate_volume_breakout_signals(df: pd.DataFrame, params: StrategyParams) -> pd.DataFrame:
    if len(df) < 20:
        return pd.DataFrame()

    cond_vol_increase = _check_consecutive_volume_increase(df["volume"], params.volume_increase_days)
    cond_vol_ratio = df["volume"] > df["avg_volume_20"] * params.volume_ratio_threshold
    cond_above_ma5 = df["close"] > df["ma5"]
    cond_above_ma20 = df["close"] > df["ma20"]
    cond_ma5_above_ma20 = df["ma5"] > df["ma20"]
    cond_close_above_3d = df["close"] > df["close"].shift(3)
    cond_not_overheat = df["return_5d"] < params.max_return_5d
    cond_bullish = df["close"] > df["open"]

    all_conditions = (
        cond_vol_increase & cond_vol_ratio & cond_above_ma5
        & cond_above_ma20 & cond_ma5_above_ma20
        & cond_close_above_3d & cond_not_overheat & cond_bullish
    )

    signal_df = df.loc[all_conditions].copy()
    if signal_df.empty:
        return pd.DataFrame()

    volume_ratio = signal_df["volume"] / signal_df["avg_volume_20"]
    signal_df["signal_type"] = "volume_breakout"
    signal_df["volume_ratio"] = volume_ratio
    signal_df["score"] = volume_ratio

    return signal_df[["signal_type", "score", "volume_ratio", "return_3d", "return_5d", "close", "ma5", "ma20", "avg_trading_value_20"]]


def generate_pullback_signals(df: pd.DataFrame, params: StrategyParams) -> pd.DataFrame:
    if len(df) < 20:
        return pd.DataFrame()

    signals = []
    trigger_idx = None
    trigger_close = None
    trigger_volume = None
    pullback_detected = False

    for i in range(20, len(df)):
        row = df.iloc[i]

        vol_increasing = all(
            df["volume"].iloc[i - j] > df["volume"].iloc[i - j - 1]
            for j in range(params.volume_increase_days)
        )
        if (
            vol_increasing
            and row["volume"] > row["avg_volume_20"] * 2.0
            and row["close"] > row["ma20"]
            and 0.05 <= row["return_5d"] <= 0.30
        ):
            trigger_idx = i
            trigger_close = row["close"]
            trigger_volume = row["volume"]
            pullback_detected = False
            continue

        if trigger_idx is not None and not pullback_detected:
            days_since = i - trigger_idx
            if days_since > 5:
                trigger_idx = None
                continue
            if (
                row["close"] <= row["ma5"] * 1.02
                and row["close"] > row["ma20"]
                and row["volume"] < trigger_volume * 0.7
                and row["close"] > trigger_close * 0.90
            ):
                pullback_detected = True
                continue

        if pullback_detected and trigger_idx is not None:
            days_since = i - trigger_idx
            if days_since > 5:
                trigger_idx = None
                pullback_detected = False
                continue
            prev = df.iloc[i - 1]
            if row["high"] > prev["high"] and row["close"] > row["open"]:
                volume_ratio = row["volume"] / row["avg_volume_20"]
                signals.append({
                    "date": df.index[i],
                    "signal_type": "pullback_buy",
                    "score": volume_ratio,
                    "volume_ratio": volume_ratio,
                    "return_3d": row.get("return_3d", 0),
                    "return_5d": row.get("return_5d", 0),
                    "close": row["close"],
                    "ma5": row["ma5"],
                    "ma20": row["ma20"],
                    "avg_trading_value_20": row.get("avg_trading_value_20", 0),
                })
                trigger_idx = None
                pullback_detected = False

    if not signals:
        return pd.DataFrame()
    result = pd.DataFrame(signals).set_index("date")
    return result


def generate_high_breakout_signals(df: pd.DataFrame, params: StrategyParams) -> pd.DataFrame:
    if len(df) < 21:
        return pd.DataFrame()

    cond_breakout = df["close"] > df["high_20"].shift(1)
    cond_vol_ratio = df["volume"] > df["avg_volume_20"] * params.volume_ratio_threshold
    cond_above_ma5 = df["close"] > df["ma5"]
    cond_ma5_above_ma20 = df["ma5"] > df["ma20"]
    cond_not_overheat = df["return_5d"] < 0.25
    cond_bullish = df["close"] > df["open"]

    all_conditions = (
        cond_breakout & cond_vol_ratio & cond_above_ma5
        & cond_ma5_above_ma20 & cond_not_overheat & cond_bullish
    )

    signal_df = df.loc[all_conditions].copy()
    if signal_df.empty:
        return pd.DataFrame()

    volume_ratio = signal_df["volume"] / signal_df["avg_volume_20"]
    breakout_score = signal_df["close"] / signal_df["high_20"].shift(1) - 1

    signal_df["signal_type"] = "high_breakout"
    signal_df["volume_ratio"] = volume_ratio
    signal_df["score"] = volume_ratio * 0.6 + breakout_score.fillna(0).rank(pct=True) * 0.4

    return signal_df[["signal_type", "score", "volume_ratio", "return_3d", "return_5d", "close", "ma5", "ma20", "avg_trading_value_20"]]


def generate_combined_signals(df: pd.DataFrame, params: StrategyParams) -> pd.DataFrame:
    if len(df) < 21:
        return pd.DataFrame()

    cond_vol_increase = _check_consecutive_volume_increase(df["volume"], params.volume_increase_days)
    cond_vol_ratio = df["volume"] > df["avg_volume_20"] * params.volume_ratio_threshold
    cond_above_ma5 = df["close"] > df["ma5"]
    cond_above_ma20 = df["close"] > df["ma20"]
    cond_ma5_above_ma20 = df["ma5"] > df["ma20"]
    cond_close_above_3d = df["close"] > df["close"].shift(3)
    cond_not_overheat = df["return_5d"] < params.max_return_5d
    cond_bullish = df["close"] > df["open"]
    cond_breakout = df["close"] > df["high_20"].shift(1)

    all_conditions = (
        cond_vol_increase & cond_vol_ratio & cond_above_ma5
        & cond_above_ma20 & cond_ma5_above_ma20 & cond_close_above_3d
        & cond_not_overheat & cond_bullish & cond_breakout
    )

    signal_df = df.loc[all_conditions].copy()
    if signal_df.empty:
        return pd.DataFrame()

    volume_ratio = signal_df["volume"] / signal_df["avg_volume_20"]
    breakout_score = signal_df["close"] / signal_df["high_20"].shift(1) - 1

    signal_df["signal_type"] = "combined_ac"
    signal_df["volume_ratio"] = volume_ratio
    signal_df["score"] = volume_ratio * 0.5 + breakout_score.fillna(0).rank(pct=True) * 0.5

    return signal_df[["signal_type", "score", "volume_ratio", "return_3d", "return_5d", "close", "ma5", "ma20", "avg_trading_value_20"]]


SIGNAL_GENERATORS = {
    "volume_breakout": generate_volume_breakout_signals,
    "pullback_buy": generate_pullback_signals,
    "high_breakout": generate_high_breakout_signals,
    "combined_ac": generate_combined_signals,
}
