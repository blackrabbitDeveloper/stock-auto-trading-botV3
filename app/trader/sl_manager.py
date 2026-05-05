from __future__ import annotations

import logging

from app.broker.order import KISOrderAPI, OrderResult
from app.models.position import Position

logger = logging.getLogger(__name__)


class SLManager:
    """Manage stop-loss reservation orders."""

    def __init__(self, order_api: KISOrderAPI):
        self.order_api = order_api

    async def register_sl(self, position: Position) -> OrderResult:
        """Register initial SL order after buy fill."""
        if not position.sl_price or not position.qty:
            return OrderResult(success=False, message="Missing sl_price or qty")

        result = await self.order_api.set_stop_loss(
            position.symbol, position.qty, position.sl_price
        )
        if result.success:
            logger.info(f"SL registered: {position.symbol} qty={position.qty} @ {position.sl_price}")
        return result

    async def update_sl(self, position: Position, new_sl_price: int) -> OrderResult:
        """Update SL order (cancel old, place new)."""
        if position.sl_order_no:
            cancel_result = await self.order_api.cancel_order(position.sl_order_no, position.qty)
            if not cancel_result.success:
                logger.warning(f"Failed to cancel old SL for {position.symbol}: {cancel_result.message}")

        result = await self.order_api.set_stop_loss(
            position.symbol, position.qty, new_sl_price
        )
        if result.success:
            logger.info(f"SL updated: {position.symbol} new SL={new_sl_price}")
        return result
