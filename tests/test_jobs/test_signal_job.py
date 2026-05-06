from app.jobs.signal_job import _check_exit
from app.models.position import Position
from app.config import StrategyParams
from datetime import date


def test_check_exit_stop_loss():
    # SL > trail → stop_loss
    pos = Position(
        strategy="vol", symbol="005930", name="삼성", status="active",
        signal_date=date.today(), holding_days=3, sl_price=70000, trail_price=65000,
    )
    config = StrategyParams(name="vol", sl_skip_days=2, max_holding_days=10, trailing_stop_pct=0.05)
    result = _check_exit(pos, 69000, config)
    assert result == "stop_loss"


def test_check_exit_trailing_stop():
    pos = Position(
        strategy="vol", symbol="005930", name="삼성", status="active",
        signal_date=date.today(), holding_days=5, sl_price=68000, trail_price=72000,
    )
    config = StrategyParams(name="vol", sl_skip_days=2, max_holding_days=10, trailing_stop_pct=0.05)
    result = _check_exit(pos, 71000, config)
    assert result == "trailing_stop"


def test_check_exit_time():
    pos = Position(
        strategy="vol", symbol="005930", name="삼성", status="active",
        signal_date=date.today(), holding_days=10, sl_price=68000, trail_price=72000,
    )
    config = StrategyParams(name="vol", sl_skip_days=2, max_holding_days=10, trailing_stop_pct=0.05)
    result = _check_exit(pos, 80000, config)
    assert result == "time_exit"


def test_check_exit_skip_days():
    pos = Position(
        strategy="vol", symbol="005930", name="삼성", status="active",
        signal_date=date.today(), holding_days=1, sl_price=68000, trail_price=72000,
    )
    config = StrategyParams(name="vol", sl_skip_days=2, max_holding_days=10, trailing_stop_pct=0.05)
    result = _check_exit(pos, 60000, config)
    assert result is None


def test_check_exit_no_exit():
    pos = Position(
        strategy="vol", symbol="005930", name="삼성", status="active",
        signal_date=date.today(), holding_days=5, sl_price=68000, trail_price=70000,
    )
    config = StrategyParams(name="vol", sl_skip_days=2, max_holding_days=10, trailing_stop_pct=0.05)
    result = _check_exit(pos, 75000, config)
    assert result is None
