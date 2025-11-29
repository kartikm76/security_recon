"""Domain schema for reconciliation metrics."""
from __future__ import annotations

from datetime import date

from pydantic import BaseModel


class MetricsPayload(BaseModel):
    run_id: str
    as_of_date: date
    total_exceptions: int
    unexplained_exceptions: int