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

class RunSummaryResponse(BaseModel):
    run_id: str
    as_of_date: date    
    total_exceptions: int
    unexplained_exceptions: int

class RunIdsForDateResponse(BaseModel):
    as_of_date: date
    runs: List[RunSummaryResponse]

class RunS3URIResponse(BaseModel):
    run_id: str
    as_of_date: str | None = None
    status: str
    s3_uri: str | None = None