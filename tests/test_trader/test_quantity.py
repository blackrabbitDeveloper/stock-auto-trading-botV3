from app.trader.quantity import calc_quantity


def test_calc_quantity_basic():
    qty = calc_quantity(40_000_000, 0.25, 0.20, 72000)
    assert qty == 27


def test_calc_quantity_zero_price():
    assert calc_quantity(40_000_000, 0.25, 0.20, 0) == 0


def test_calc_quantity_expensive_stock():
    qty = calc_quantity(40_000_000, 0.25, 0.20, 3_000_000)
    assert qty == 0


def test_calc_quantity_cheap_stock():
    qty = calc_quantity(40_000_000, 0.25, 0.20, 1000)
    assert qty == 2000
