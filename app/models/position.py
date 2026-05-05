from __future__ import annotations

from datetime import datetime, date
from typing import Optional

from sqlalchemy import String, Integer, Float, Date, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column

from app.models.database import Base


class Position(Base):
    __tablename__ = "positions"

    id: Mapped[int] = mapped_column(primary_key=True)
    strategy: Mapped[str] = mapped_column(String(30))
    symbol: Mapped[str] = mapped_column(String(10), index=True)
    name: Mapped[str] = mapped_column(String(50))
    status: Mapped[str] = mapped_column(String(15), index=True)
    signal_date: Mapped[date] = mapped_column(Date)
    entry_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    entry_price: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    qty: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    peak_price: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    sl_price: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    trail_price: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    sl_order_no: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    holding_days: Mapped[int] = mapped_column(Integer, default=0)
    exit_reason: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    entry_atr: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())
