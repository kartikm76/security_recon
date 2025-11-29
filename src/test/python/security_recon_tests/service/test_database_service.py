"""Database connectivity checks for ``DatabaseService``."""
from __future__ import annotations

import os
from typing import Iterator

import pytest
from sqlalchemy import text

from security_recon.service.database_service import DatabaseService

pytestmark = pytest.mark.skipif(
    os.getenv("SECURITY_RECON_DB_TESTS", "0") != "1",
    reason="Database integration tests disabled; set SECURITY_RECON_DB_TESTS=1 to run",
)

@pytest.fixture()
def database_service() -> Iterator[DatabaseService]:
    service = DatabaseService()
    try:
        yield service
    finally:
        service.close()


def test_connections(database_service: DatabaseService) -> None:
    results = {}
    with database_service.mysql_engine.connect() as mysql_conn:
        results["mysql"] = mysql_conn.execute(text("SELECT 1")).scalar_one()

    with database_service.postgres_engine.connect() as postgres_conn:
        postgres_conn.execute(text("SET search_path TO security_master"))
        results["postgres"] = postgres_conn.execute(text("SELECT 1")).scalar_one()

    assert int(results["mysql"]) == 1
    assert int(results["postgres"]) == 1


def test_count(database_service: DatabaseService) -> None:
    mysql_session = database_service.mysql_session_factory()
    postgres_session = database_service.postgres_session_factory()
    try:
        mysql_count = mysql_session.execute(
            text("SELECT COUNT(*) FROM legacy_security_master.security_master")
        ).scalar_one()
        postgres_count = postgres_session.execute(
            text("SELECT COUNT(*) FROM security_master.security_master")
        ).scalar_one()
    finally:
        database_service.mysql_session_factory.remove()
        database_service.postgres_session_factory.remove()

    assert int(mysql_count) >= 0
    assert int(postgres_count) >= 0
