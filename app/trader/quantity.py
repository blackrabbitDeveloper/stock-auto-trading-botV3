from __future__ import annotations

import math


def calc_quantity(total_eval: int, capital_allocation: float, position_weight: float, price: int) -> int:
    """Calculate buy quantity for a position.

    Args:
        total_eval: Total account evaluation amount
        capital_allocation: Fraction allocated to this strategy (e.g., 0.25)
        position_weight: Fraction per position (e.g., 0.20)
        price: Current stock price

    Returns:
        Number of shares to buy (0 if insufficient)
    """
    if price <= 0:
        return 0
    budget = total_eval * capital_allocation * position_weight
    qty = math.floor(budget / price)
    return max(0, qty)
