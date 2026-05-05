"""Smoke tests to verify all modules import correctly."""


def test_import_config():
    from app.config import KISConfig, AppSettings, load_strategy_configs, StrategyParams


def test_import_models():
    from app.models import Base, Position, Order, Trade


def test_import_broker():
    from app.broker import KISClient
    from app.broker.order import KISOrderAPI
    from app.broker.account import KISAccountAPI


def test_import_strategy():
    from app.strategy import SIGNAL_GENERATORS, add_indicators, filter_universe, check_market_filter


def test_import_trader():
    from app.trader import OrderExecutor, SLManager, calc_quantity


def test_import_jobs():
    from app.jobs import run_signal_job, run_order_job, run_confirm_job


def test_import_notifier():
    from app.notifier import DiscordNotifier


def test_import_dashboard():
    from app.dashboard import router


def test_signal_generators_complete():
    from app.strategy import SIGNAL_GENERATORS
    expected = {"volume_breakout", "pullback_buy", "high_breakout", "combined_ac"}
    assert set(SIGNAL_GENERATORS.keys()) == expected
