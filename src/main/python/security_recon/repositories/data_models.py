"""SQLAlchemy ORM models for repository layer."""
from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import Date, DateTime, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Shared declarative base for ORM models."""

class ReconRunSummary(Base):
    """ORM mapping for security_master.recon_run_summary."""

    __tablename__ = "recon_run_summary"
    __table_args__ = {"schema": "security_master"}

    run_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    as_of_date: Mapped[date] = mapped_column(Date, primary_key=True)
    total_exceptions: Mapped[int] = mapped_column(Integer)
    unexplained_exceptions: Mapped[int] = mapped_column(Integer)


class ArtifactLog(Base):
    __tablename__ = "artifact_log"
    __table_args__ = {"schema": "security_master"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(255), index=True, nullable=False)
    as_of_date: Mapped[date] = mapped_column(Date, index=True, nullable=False)
    artifact_type: Mapped[str] = mapped_column(String(50), nullable=False)
    s3_uri: Mapped[str] = mapped_column(String(1024), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="uploaded")
    uploaded_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


__all__ = ["Base", "ReconRunSummary", "ArtifactLog"]
