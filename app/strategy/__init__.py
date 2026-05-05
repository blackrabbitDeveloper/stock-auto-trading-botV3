from app.strategy.signals import SIGNAL_GENERATORS
from app.strategy.indicators import add_indicators
from app.strategy.universe import filter_universe


def check_market_filter(*args, **kwargs):
    from app.strategy.market_filter import check_market_filter as _check
    return _check(*args, **kwargs)


__all__ = ["SIGNAL_GENERATORS", "add_indicators", "filter_universe", "check_market_filter"]
