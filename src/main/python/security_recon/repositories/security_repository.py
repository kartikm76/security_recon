"""Data access repositories for security snapshots."""
from __future__ import annotations

from datetime import date

import pandas as pd
from sqlalchemy import text

from security_recon.support.database_service import DatabaseService

class LegacySecurityRepository:
    def __init__(self) -> None:
        self.db_service = DatabaseService()

    def load_by_date(self, as_of_date: date) -> pd.DataFrame:
        query = text(
            """
            SELECT
                instrument_id,
                as_of_date,
                isin,
                cfi_code,
                coupon,
                maturity_date,
                currency,
                callable_flag
            FROM legacy_security_master.security_master
            WHERE as_of_date = :as_of_date
            """
        )
        with self.db_service.mysql_engine.connect() as connection:
            return pd.read_sql(query, connection, params={"as_of_date": as_of_date})


class StrategicSecurityRepository:
    def __init__(self) -> None:
        self.db_service = DatabaseService()

    def load_by_date(self, as_of_date: date) -> pd.DataFrame:
        query = text(
            """
            SELECT instrument_id,
                as_of_date,
                isin,
                cfi_code,
                coupon,
                maturity_date,
                currency,
                callable_flag
            FROM security_master.security_master
            WHERE as_of_date = :as_of_date
            """
        )
        with self.db_service.postgres_engine.connect() as connection:
            return pd.read_sql(query, connection, params={"as_of_date": as_of_date})
