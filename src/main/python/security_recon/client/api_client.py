from __future__ import annotations

import os
import requests
from dataclasses import dataclass
from datetime import date
from typing import List, Optional
from security_recon.domain import Artifact

API_BASE_URL_DEFAULT = "http://localhost:8000"

class SecurityReconApiClient:
    """
    Thin client that talks to the Security Recon FastAPI service.
    """
    
    def __init__(self, api_base_url: Optional[str] = None) -> None:
        self.api_base_url = api_base_url or os.getenv(
            "SECURITY_RECON_API_BASE_URL",
            API_BASE_URL_DEFAULT,
        )

    #---- Runs ----
    def trigger_run(self, as_of_date: date) -> dict:
        """
        Trigger a new reconciliation run for the specified as-of date.
        """
        url = f"{self.api_base_url}/runs/"
        payload = {"as_of_date": as_of_date.isoformat()}
        response = requests.post(url, json=payload, timeout=30)
        response.raise_for_status()
        return response.json()
    
    def list_run_ids(self, as_of_date: date) -> List[str]:
        """
        List all run IDs for the specified as-of date.
        """
        url = f"{self.api_base_url}/run-ids/"
        params = {"as_of_date": as_of_date.isoformat()}
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data.get("run_ids", [])
    
    def get_run_artifact(self, run_id: str) -> Optional[Artifact]:
        """
        Retrieve the latest artifact information for the specified run ID.
        """
        url = f"{self.api_base_url}/runs/{run_id}/artifact/"
        response = requests.get(url, timeout=30)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        data = response.json()
        return Artifact(
            run_id = data["run_id"],
            as_of_date = data.get("as_of_date"),
            status = data["status"],
            s3_uri = data.get("s3_uri"),
        )
