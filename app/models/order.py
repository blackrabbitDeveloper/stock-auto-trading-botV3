from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import String, Integer, DateTime, ForeignKey, func
from sqlalchemy.orm import Mapped, mapped_column

from app.models.database import Base


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(primary_key=True)
    position_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("positions.id"), nullable=True)
    strategy: Mapped[str] = mapped_column(String(30))
    symbol: Mapped[str] = mapped_column(String(10))
    name: Mapped[str] = mapped_column(String(50), default="")
    side: Mapped[str] = mapped_column(String(4))
    order_type: Mapped[str] = mapped_column(String(10))
    qty: Mapped[int] = mapped_column(Integer)
    price: Mapped[int] = mapped_column(Integer, default=0)
    filled_price: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    filled_qty: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    order_no: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    status: Mapped[str] = mapped_column(String(10), default="submitted")
    submitted_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    filled_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
