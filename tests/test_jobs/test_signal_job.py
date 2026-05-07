import pandas as pd
from app.jobs.signal_job import _check_exit
from app.models.position import Position
from app.config import StrategyParams
from datetime import date


def _dummy_df(close: int, high: int, low: int, periods: int = 20) -> pd.DataFrame:
    """Create minimal OHLCV DataFrame for _check_exit."""
    data = {"close": [close] * periods, "high": [high] * periods, "low": [low] * periods}
    return pd.DataFrame(data)


def test_check_exit_stop_loss():
    # SL > trail → stop_loss (today_low hits SL)
    pos = Position(
        strategy="vol", symbol="005930", name="삼성", status="active",
        signal_date=date.today(), holding_days=3, sl_price=70000, trail_price=65000,
        entry_price=75000,
    )
    config = StrategyParams(name="vol", sl_skip_days=2, max_holding_days=10, trailing_stop_pct=0.05)
    df = _dummy_df(close=69000, high=72000, low=69000)
    result = _check_exit(pos, 69000, 72000, 69000, df, config)
    assert result == "stop_loss"


def test_check_exit_trailing_stop():
    # trail > SL → trailing_stop (today_low hits trail)
    pos = Position(
        strategy="vol", symbol="005930", name="삼성", status="active",
        signal_date=date.today(), holding_days=5, sl_price=68000, trail_price=72000,
        entry_price=75000,
    )
    config = StrategyParams(name="vol", sl_skip_days=2, max_holding_days=10, trailing_stop_pct=0.05)
    df = _dummy_df(close=71000, high=73000, low=71000)
    result = _check_exit(pos, 71000, 73000, 71000, df, config)
    assert result == "trailing_stop"


def test_check_exit_time():
    # holding_days >= max → time_exit
    pos = Position(
        strategy="vol", symbol="005930", name="삼성", status="active",
        signal_date=date.today(), holding_days=10, sl_price=68000, trail_price=72000,
        entry_price=75000,
    )
    config = StrategyParams(name="vol", sl_skip_days=2, max_holding_days=10, trailing_stop_pct=0.05)
    df = _dummy_df(close=80000, high=81000, low=79000)
    result = _check_exit(pos, 80000, 81000, 79000, df, config)
    assert result == "time_exit"


def test_check_exit_no_exit():
    # Price above all triggers → no exit
    pos = Position(
        strategy="vol", symbol="005930", name="삼성", status="active",
        signal_date=date.today(), holding_days=5, sl_price=68000, trail_price=70000,
        entry_price=72000,
    )
    config = StrategyParams(name="vol", sl_skip_days=2, max_holding_days=10, trailing_stop_pct=0.05)
    df = _dummy_df(close=75000, high=76000, low=74000)
    result = _check_exit(pos, 75000, 76000, 74000, df, config)
    assert result is None


def test_check_exit_breakeven():
    # SL raised to entry_price via breakeven_stop → breakeven
    pos = Position(
        strategy="vol", symbol="005930", name="삼성", status="active",
        signal_date=date.today(), holding_days=5, sl_price=75000, trail_price=70000,
        entry_price=75000,
    )
    config = StrategyParams(name="vol", sl_skip_days=2, max_holding_days=10,
                            trailing_stop_pct=0.05, breakeven_stop=True)
    df = _dummy_df(close=74000, high=76000, low=74000)
    result = _check_exit(pos, 74000, 76000, 74000, df, config)
    assert result == "breakeven"


def test_check_exit_dynamic_holding():
    # dynamic_holding + profitable → skip time_exit
    pos = Position(
        strategy="vol", symbol="005930", name="삼성", status="active",
        signal_date=date.today(), holding_days=10, sl_price=68000, trail_price=70000,
        entry_price=72000,
    )
    config = StrategyParams(name="vol", sl_skip_days=2, max_holding_days=10,
                            trailing_stop_pct=0.05, dynamic_holding=True)
    df = _dummy_df(close=80000, high=81000, low=79000)
    result = _check_exit(pos, 80000, 81000, 79000, df, config)
    assert result is None  # profitable, trailing stop manages


def test_check_exit_fixed_take_profit():
    # fixed method: today_high >= TP → take_profit
    pos = Position(
        strategy="vol", symbol="005930", name="삼성", status="active",
        signal_date=date.today(), holding_days=5, sl_price=68000, trail_price=0,
        entry_price=70000,
    )
    config = StrategyParams(name="vol", sl_skip_days=2, max_holding_days=10,
                            trailing_stop_pct=0.05, exit_method="fixed", take_profit_pct=0.10)
    df = _dummy_df(close=76000, high=77500, low=75000)
    # TP = 70000 * 1.10 = 77000, today_high=77500 >= 77000
    result = _check_exit(pos, 76000, 77500, 75000, df, config)
    assert result == "take_profit"


def test_check_exit_ma_exit():
    # ma_exit method: close < MA → ma_exit
    pos = Position(
        strategy="vol", symbol="005930", name="삼성", status="active",
        signal_date=date.today(), holding_days=5, sl_price=60000, trail_price=0,
        entry_price=70000,
    )
    config = StrategyParams(name="vol", sl_skip_days=2, max_holding_days=10,
                            trailing_stop_pct=0.05, exit_method="ma_exit", ma_exit_period=5)
    # Create df where close=68000 but MA5 of previous closes is 69000
    closes = [69000, 69500, 69200, 69300, 68000]
    df = pd.DataFrame({"close": closes, "high": [70000] * 5, "low": [68000] * 5})
    # MA5 = mean([69000,69500,69200,69300,68000]) = 69000
    result = _check_exit(pos, 68000, 70000, 68000, df, config)
    assert result == "ma_exit"
