"""Persistence helpers for reconciliation metrics."""
from __future__ import annotations

from datetime import date
from typing import Dict

import pandas as pd
from sqlalchemy import text

from security_recon.service.database_service import DatabaseService

class MetricsRepository:
    def __init__(self) -> None:
        self.db_service = DatabaseService()

    def compute_metrics(self, ex_df: pd.DataFrame, run_id: int, as_of_date: date) -> Dict[str, int]:
        total = len(ex_df)
        unexplained = total
        return {
            "run_id": run_id,
            "as_of_date": as_of_date,
            "total_exceptions": int(total),
            "unexplained_exceptions": int(unexplained),
        }

    def persist_metrics(self, metrics: Dict[str, int]) -> None:
        sql = text(
            """
            INSERT INTO security_master.recon_run_summary
                   (run_id, as_of_date, total_exceptions, unexplained_exceptions)
            VALUES (:run_id, :as_of_date, :total_exceptions, :unexplained_exceptions)
            """
        )
        with self.db_service.postgres_engine.begin() as connection:
            connection.execute(sql, metrics)
