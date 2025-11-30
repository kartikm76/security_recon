from __future__ import annotations
from dataclasses import dataclass

@dataclass
class Artifact:
    run_id: str
    as_of_date: str
    status: str
    s3_uri: str