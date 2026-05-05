import pandas as pd
import numpy as np
from app.strategy.indicators import add_indicators
from app.strategy.signals import generate_volume_breakout_signals, SIGNAL_GENERATORS
from app.config import StrategyParams


def _make_breakout_data():
    n = 30
    dates = pd.date_range("2026-01-01", periods=n, freq="B")
    close = np.array([50000 + i * 100 for i in range(n)], dtype=float)
    volume = np.array([500000] * n, dtype=float)
    volume[-4] = 600000
    volume[-3] = 800000
    volume[-2] = 1200000
    volume[-1] = 5000000

    df = pd.DataFrame({
        "open": close - 100,
        "high": close + 200,
        "low": close - 200,
        "close": close,
        "volume": volume,
    }, index=dates)
    return add_indicators(df)


def test_volume_breakout_signal_generated():
    df = _make_breakout_data()
    params = StrategyParams(name="volume_breakout", volume_increase_days=3, volume_ratio_threshold=3.0, max_return_5d=0.20)
    signals = generate_volume_breakout_signals(df, params)
    assert not signals.empty
    assert signals.iloc[-1]["signal_type"] == "volume_breakout"


def test_volume_breakout_no_signal_low_volume():
    n = 30
    dates = pd.date_range("2026-01-01", periods=n, freq="B")
    close = np.array([50000 + i * 100 for i in range(n)], dtype=float)
    volume = np.array([500000] * n, dtype=float)

    df = pd.DataFrame({
        "open": close - 100, "high": close + 200,
        "low": close - 200, "close": close, "volume": volume,
    }, index=dates)
    df = add_indicators(df)
    params = StrategyParams(name="volume_breakout")
    signals = generate_volume_breakout_signals(df, params)
    assert signals.empty


def test_signal_generators_registry():
    assert "volume_breakout" in SIGNAL_GENERATORS
    assert "pullback_buy" in SIGNAL_GENERATORS
    assert "high_breakout" in SIGNAL_GENERATORS
    assert "combined_ac" in SIGNAL_GENERATORS
