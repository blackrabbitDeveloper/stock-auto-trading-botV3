import pandas as pd
import numpy as np
from app.strategy.indicators import add_indicators


def _make_ohlcv(n=30):
    dates = pd.date_range("2026-01-01", periods=n, freq="B")
    np.random.seed(42)
    close = 50000 + np.cumsum(np.random.randn(n) * 500)
    return pd.DataFrame({
        "open": close - 200,
        "high": close + 300,
        "low": close - 400,
        "close": close,
        "volume": np.random.randint(100000, 1000000, n),
    }, index=dates)


def test_add_indicators_columns():
    df = _make_ohlcv(30)
    result = add_indicators(df)
    assert "ma5" in result.columns
    assert "ma20" in result.columns
    assert "atr14" in result.columns
    assert "avg_volume_20" in result.columns
    assert "return_5d" in result.columns
    assert "high_20" in result.columns


def test_add_indicators_no_mutation():
    df = _make_ohlcv(30)
    original_cols = list(df.columns)
    add_indicators(df)
    assert list(df.columns) == original_cols


def test_ma5_calculation():
    df = _make_ohlcv(30)
    result = add_indicators(df)
    expected_ma5 = df["close"].iloc[-5:].mean()
    assert abs(result["ma5"].iloc[-1] - expected_ma5) < 0.01
