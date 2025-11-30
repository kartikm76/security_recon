"""Persistence helpers for reconciliation metrics."""
from __future__ import annotations

from datetime import date

import pandas as pd
from sqlalchemy.orm import Session

from security_recon.domain.metrics import MetricsPayload
from security_recon.repositories.data_models import ReconRunSummary
from security_recon.support.database_service import DatabaseService

class MetricsRepository:
    def __init__(self) -> None:
        self.db_service = DatabaseService()

    def compute_metrics(self, ex_df: pd.DataFrame, run_id: str, as_of_date: date) -> MetricsPayload:
        total = len(ex_df)
        unexplained = total
        return MetricsPayload(
            run_id=run_id,
            as_of_date=as_of_date,
            total_exceptions=int(total),
            unexplained_exceptions=int(unexplained),
        )

    def persist_metrics(self, metrics: MetricsPayload) -> None:
        session: Session = self.db_service.postgres_session_factory()
        try:
            record = ReconRunSummary(**metrics.model_dump())
            session.merge(record)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            self.db_service.postgres_session_factory.remove()
