from __future__ import annotations

from datetime import datetime, date

from sqlalchemy import String, Integer, Float, Date, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column

from app.models.database import Base


class Trade(Base):
    __tablename__ = "trades"

    id: Mapped[int] = mapped_column(primary_key=True)
    strategy: Mapped[str] = mapped_column(String(30))
    symbol: Mapped[str] = mapped_column(String(10))
    name: Mapped[str] = mapped_column(String(50))
    entry_date: Mapped[date] = mapped_column(Date)
    exit_date: Mapped[date] = mapped_column(Date)
    entry_price: Mapped[int] = mapped_column(Integer)
    exit_price: Mapped[int] = mapped_column(Integer)
    qty: Mapped[int] = mapped_column(Integer)
    return_pct: Mapped[float] = mapped_column(Float)
    pnl: Mapped[int] = mapped_column(Integer)
    holding_days: Mapped[int] = mapped_column(Integer)
    exit_reason: Mapped[str] = mapped_column(String(20))
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
