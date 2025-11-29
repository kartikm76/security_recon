from datetime import date
from typing import Generator

import pandas as pd
import pytest
from sqlalchemy import text

from security_recon.domain.metrics import MetricsPayload
from security_recon.repositories.metrics_repository import MetricsRepository


def test_get_metrics_by_date(metrics_repo: MetricsRepository) -> None:
    run_id = "1"
    as_of_date = date(2023, 12, 29)
    metrics_payload = metrics_repo.compute_metrics(pd.DataFrame(), run_id=run_id, as_of_date=as_of_date)
    assert isinstance(metrics_payload, MetricsPayload)
    assert metrics_payload.run_id == run_id
    assert metrics_payload.as_of_date == as_of_date
    assert metrics_payload.total_exceptions == 0
    assert metrics_payload.unexplained_exceptions == 0


def test_persist_metrics(metrics_repo: MetricsRepository) -> None:
    payload = MetricsPayload(
        run_id="1",
        as_of_date=date(2023, 12, 29),
        total_exceptions=10,
        unexplained_exceptions=5,
    )
    try:
        metrics_repo.persist_metrics(payload)
    except Exception as exc:  # noqa: BLE001
        pytest.fail(f"persist_metrics raised an exception: {exc}")


@pytest.fixture
def metrics_repo() -> Generator[MetricsRepository, None, None]:
    repo = MetricsRepository()
    yield repo
    session = repo.db_service.postgres_session_factory()
    try:
        session.execute(
            text(
                """
                DELETE FROM security_master.recon_run_summary
                WHERE run_id = :run_id AND as_of_date = :as_of_date
                """
            ),
            {"run_id": "1", "as_of_date": date(2023, 12, 29)},
        )
        session.commit()
    finally:
        repo.db_service.postgres_session_factory.remove()
