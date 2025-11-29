"""SQLAlchemy ORM models for repository layer."""
from __future__ import annotations

from datetime import date

from sqlalchemy import Date, Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Shared declarative base for ORM models."""


class ReconRunSummary(Base):
    """ORM mapping for security_master.recon_run_summary."""

    __tablename__ = "recon_run_summary"
    __table_args__ = {"schema": "security_master"}

    run_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    as_of_date: Mapped[date] = mapped_column(Date, primary_key=True)
    total_exceptions: Mapped[int] = mapped_column(Integer)
    unexplained_exceptions: Mapped[int] = mapped_column(Integer)


__all__ = ["Base", "ReconRunSummary"]
