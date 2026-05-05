import pandas as pd
import numpy as np
from app.strategy.universe import filter_universe, is_preferred_stock, is_spac
from app.config import UniverseConfig


def test_is_preferred_stock():
    assert is_preferred_stock("005935") is True
    assert is_preferred_stock("005930") is False


def test_is_spac():
    assert is_spac("교보11호스팩") is True
    assert is_spac("삼성전자") is False


def test_filter_universe_basic():
    config = UniverseConfig(min_price=1000, min_avg_trading_value_20=1_000_000)
    dates = pd.date_range("2026-01-01", periods=25, freq="B")
    good_df = pd.DataFrame({
        "close": [50000] * 25,
        "avg_trading_value_20": [10_000_000] * 25,
    }, index=dates)
    cheap_df = pd.DataFrame({
        "close": [500] * 25,
        "avg_trading_value_20": [10_000_000] * 25,
    }, index=dates)

    data_map = {"005930": good_df, "999990": cheap_df}
    result = filter_universe(["005930", "999990"], data_map, config)
    assert "005930" in result
    assert "999990" not in result
