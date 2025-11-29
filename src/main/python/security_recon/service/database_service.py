"""High-level helpers for working with both MySQL and Postgres."""
from __future__ import annotations

from security_recon.support.database_manager import DatabaseManager, db_manager

class DatabaseService:
    def __init__(self, manager: DatabaseManager = db_manager):
        self.manager = manager
        self.mysql_engine = manager.get_engine("mysql")
        self.postgres_engine = manager.get_engine("postgres")
        self.mysql_session_factory = manager.get_session("mysql")
        self.postgres_session_factory = manager.get_session("postgres")

    def close(self) -> None:
        self.mysql_session_factory.remove()
        self.postgres_session_factory.remove()
        self.manager.dispose_all()

__all__ = ["DatabaseService"]
