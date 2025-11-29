"""API-facing request/response schemas."""
from __future__ import annotations

from datetime import date
from typing import List

from pydantic import BaseModel, Field


class RunCreateRequest(BaseModel):
    as_of_date: date


class RunCreateResponse(BaseModel):
    run_id: str
    as_of_date: date
    status: str = "PENDING"


class RunIdsForDateResponse(BaseModel):
    as_of_date: date
    run_ids: List[str] = Field(default_factory=list)


class RunS3URIResponse(BaseModel):
    run_id: str
    as_of_date: str | None = None
    status: str
    s3_uri: str | None = None
