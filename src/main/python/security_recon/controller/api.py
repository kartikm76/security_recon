from __future__ import annotations

from datetime import date

from fastapi import FastAPI, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from sqlalchemy import select
from sqlalchemy.orm import Session

from security_recon.controller.pipeline_orchestrator import Orchestration
from security_recon.domain.api_models import (
    RunCreateRequest,
    RunCreateResponse,
    RunIdsForDateResponse,
    RunS3URIResponse,
)
from security_recon.repositories.artifact_repository import ArtifactRepository
from security_recon.repositories.models import ArtifactLog, ReconRunSummary
from security_recon.service.run import ReconResult
from security_recon.support.database_service import DatabaseService

app = FastAPI(title="Security Recon API", version="1.0.0")

db_service = DatabaseService()
artifact_repo = ArtifactRepository(db_service=db_service)
orchestration = Orchestration(artifact_repo=artifact_repo)


def _run_recon_pipeline(as_of_date: date) -> ReconResult:
    """Execute the full pipeline for the supplied date."""
    return orchestration.pipeline_orchestrator(as_of_date=as_of_date)


def _get_runids_as_of_date(as_of_date: date) -> list[str]:
    session: Session = db_service.postgres_session_factory()
    try:
        stmt = (
            select(ReconRunSummary.run_id)
            .where(ReconRunSummary.as_of_date == as_of_date)
            .order_by(ReconRunSummary.run_id.desc())
        )
        return list(session.execute(stmt).scalars().all())
    finally:
        session.close()
        db_service.postgres_session_factory.remove()


def _get_latest_artifact(run_id: str) -> ArtifactLog | None:
    return artifact_repo.fetch_latest(run_id, artifact_type="exceptions")


@app.post("/runs/", response_model=RunCreateResponse)
async def create_run(payload: RunCreateRequest) -> RunCreateResponse:
    result = await run_in_threadpool(_run_recon_pipeline, payload.as_of_date)
    return RunCreateResponse(
        run_id=result.run_id,
        as_of_date=result.as_of_date,
        status="COMPLETED",
    )

@app.get(
    "/run-ids/",
    response_model=RunIdsForDateResponse,
    summary="List run identifiers for a specific as-of date",
)
async def list_runs(
    as_of_date: date = Query(..., description="Return run identifiers for this date"),
) -> RunIdsForDateResponse:
    run_ids = await run_in_threadpool(_get_runids_as_of_date, as_of_date)
    return RunIdsForDateResponse(as_of_date=as_of_date, run_ids=run_ids)

@app.get("/runs/{run_id}/artifact", response_model=RunS3URIResponse)
async def get_run_artifact(run_id: str) -> RunS3URIResponse:
    artifact = await run_in_threadpool(_get_latest_artifact, run_id)
    if artifact is None:
        raise HTTPException(status_code=404, detail="Artifact not found")
    return RunS3URIResponse(
        run_id=run_id,
        as_of_date=artifact.as_of_date.isoformat(),
        status=artifact.status,
        s3_uri=artifact.s3_uri,
    )