from __future__ import annotations

from datetime import date

import pandas as pd
import streamlit as st

from .api_client import Artifact, SecurityReconApiClient

class SecurityReconUI:
    """
    Streamlit-based UI that uses SecurityReconApiClient to talk to the API
    and renders the exceptions view.
    """

    def __init__(self, api_client: SecurityReconApiClient) -> None:
        self.api_client = api_client

    def _load_artifact(self, s3_uri: str) -> pd.DataFrame
        """
        Load the exceptions parquet file from S3 into a DataFrame.
        Assumes AWS credentials are available.
        """
        return pd.read_parquet(s3_uri)
    
    #---- Streamlit UI Methods ----
    def render_exceptions_view(self) -> None:
        st.set_page_config(page_title="Security Recon Viewer", layout="wide")
        st.title("Security Reconciliation Exceptions Viewer")
        
