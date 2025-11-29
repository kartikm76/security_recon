"""Backend diagnostics entrypoint."""
from __future__ import annotations

from sqlalchemy import text

from security_recon.support.database_manager import (
    mysql_db,
    mysql_engine,
    postgres_db,
    postgres_engine,
)


def main() -> None:
    print("Backend diagnostics")

    with mysql_engine.connect() as mconn, postgres_engine.connect() as pconn:
        m_rows = mconn.execute(text("SELECT 1")).all()
        pconn.execute(text("SET search_path TO security_master"))
        p_rows = pconn.execute(text("SELECT 1")).all()
        print("mysql:", m_rows, "postgres:", p_rows)

    m_session = mysql_db()
    p_session = postgres_db()
    try:
        m_count = m_session.execute(
            text("SELECT COUNT(*) FROM legacy_security_master.security_master")
        ).scalar_one()
        p_count = p_session.execute(text("SELECT COUNT(*) FROM security_master.security_master")).scalar_one()
        print("mysql count:", m_count, "postgres count:", p_count)
    finally:
        mysql_db.remove()
        postgres_db.remove()


if __name__ == "__main__":
    main()
