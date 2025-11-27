"""High-level helpers for working with both MySQL and Postgres."""
from __future__ import annotations

from sqlalchemy import text

from core.database import DatabaseManager, db_manager


class DatabaseService:
    def __init__(self, manager: DatabaseManager = db_manager):
        self.manager = manager
        self.mysql_engine = manager.get_engine("mysql")
        self.postgres_engine = manager.get_engine("postgres")
        self.mysql_session_factory = manager.get_session("mysql")
        self.postgres_session_factory = manager.get_session("postgres")

    def test_connections(self) -> dict:
        results = {}
        with self.mysql_engine.connect() as conn:
            results["mysql"] = conn.execute(text("SELECT 1")).scalar_one()
        with self.postgres_engine.connect() as conn:
            conn.execute(text("SET search_path TO security_master"))
            results["postgres"] = conn.execute(text("SELECT 1")).scalar_one()
        return results

    def counts(self) -> tuple[int, int]:
        mysql_session = self.mysql_session_factory()
        postgres_session = self.postgres_session_factory()
        try:
            mysql_count = mysql_session.execute(
                text("SELECT COUNT(*) FROM legacy_security_master.security_master")
            ).scalar_one()
        finally:
            self.mysql_session_factory.remove()

        try:
            postgres_count = postgres_session.execute(
                text("SELECT COUNT(*) FROM security_master.security_master")
            ).scalar_one()
        finally:
            self.postgres_session_factory.remove()

        return mysql_count, postgres_count

    def close(self) -> None:
        self.mysql_session_factory.remove()
        self.postgres_session_factory.remove()
        self.manager.dispose_all()


__all__ = ["DatabaseService"]
