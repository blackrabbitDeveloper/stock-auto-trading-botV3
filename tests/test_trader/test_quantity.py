from app.trader.quantity import calc_quantity


def test_calc_quantity_basic():
    qty = calc_quantity(40_000_000, 0.25, 0.20, 72000)
    assert qty == 27


def test_calc_quantity_zero_price():
    assert calc_quantity(40_000_000, 0.25, 0.20, 0) == 0


def test_calc_quantity_expensive_within_allocation():
    # price 3M < allocation 10M (40M * 0.25) → allow 1 share
    qty = calc_quantity(40_000_000, 0.25, 0.20, 3_000_000)
    assert qty == 1


def test_calc_quantity_expensive_exceeds_allocation():
    # price 15M > allocation 10M (40M * 0.25) → reject
    qty = calc_quantity(40_000_000, 0.25, 0.20, 15_000_000)
    assert qty == 0


def test_calc_quantity_cheap_stock():
    qty = calc_quantity(40_000_000, 0.25, 0.20, 1000)
    assert qty == 2000
