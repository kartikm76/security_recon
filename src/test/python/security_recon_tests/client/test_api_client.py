import pytest
from pytest import fixture
from datetime import date
from security_recon.client.api_client import SecurityReconApiClient as APIClient
from security_recon.support.logging import configure_logging, get_logger

@fixture(autouse=True)
def configure_logging_for_tests() -> None:
    configure_logging()

logger = get_logger(__name__)

@fixture
def set_as_of_date() -> date:
    return date(2024, 6, 15)

@fixture
def api_client() -> APIClient:
    return APIClient()

def test_trigger_run(set_as_of_date: date, api_client: APIClient) -> None:
    response = api_client.trigger_run(as_of_date = set_as_of_date)
    assert "run_id" in response
    assert response["as_of_date"] == set_as_of_date.isoformat()
    assert response["status"] == "COMPLETED"

def test_list_run_ids(set_as_of_date: date, api_client: APIClient) -> None:
    logger.info (api_client.list_run_ids(as_of_date = set_as_of_date))
    # run_ids = api_client.list_run_ids(as_of_date = set_as_of_date)
    # logger.info(f"Run IDs for {set_as_of_date}: {run_ids}")
    # assert isinstance(run_ids, list)
    # assert all(isinstance(run_id, str) for run_id in run_ids)

# def test_get_run_artifact(set_as_of_date: date, api_client: APIClient) -> None:
#     run_ids = api_client.list_run_ids(as_of_date = set_as_of_date)
#     if not run_ids:
#         pytest.skip("No run IDs available for the specified as-of date.")
#     run_id = run_ids[0]
#     artifact = api_client.get_run_artifact(run_id = run_id)
#     assert artifact is not None
#     assert artifact.run_id == run_id
#     assert artifact.status in {"uploaded"}