"""Core reconciliation pipeline orchestration."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Optional
from uuid import uuid4

import pandas as pd

from security_recon.domain.dictionary import AttributeRuleSet
from security_recon.domain.dictionary_loader import load_rules_from_resource
from security_recon.domain.metrics import MetricsPayload
from security_recon.integration.parquet_writer import ParquetWriter
from security_recon.repositories.metrics_repository import MetricsRepository
from security_recon.repositories.security_repository import (
    LegacySecurityRepository,
    StrategicSecurityRepository,
)
from security_recon.service.recon import DataFrameDiffer
from security_recon.support import get_logger

logger = get_logger(__name__)


@dataclass
class ReconResult:
    run_id: str
    as_of_date: date
    exceptions_path: Path
    exceptions_file: str
    exception_count: int
    metrics: Optional[MetricsPayload] = None


class ReconPipeline:
    """Coordinates data fetching, diffing, persistence, and metrics."""

    def __init__(
        self,
        *,
        base_output_dir: str | Path | None = None,
        legacy_repo: LegacySecurityRepository | None = None,
        strategic_repo: StrategicSecurityRepository | None = None,
        metrics_repo: MetricsRepository | None = None,
        parquet_writer: ParquetWriter | None = None,
        rule_set: AttributeRuleSet | None = None,
    ) -> None:
        self.legacy_repo = legacy_repo or LegacySecurityRepository()
        self.strategic_repo = strategic_repo or StrategicSecurityRepository()
        self.metrics_repo = metrics_repo or MetricsRepository()
        self.rule_set = rule_set or load_rules_from_resource("data_dictionary.yml")
        self._differ = DataFrameDiffer(self.rule_set)

        if parquet_writer is not None:
            self.parquet_writer = parquet_writer
        else:
            self.parquet_writer = ParquetWriter(base_output_dir)

    @property
    def base_output_dir(self) -> Path:
        return self.parquet_writer.base_dir

    @base_output_dir.setter
    def base_output_dir(self, value: str | Path) -> None:
        self.parquet_writer = ParquetWriter(value)

    def run(self, as_of_date: date, *, persist_metrics: bool = True) -> ReconResult:
        run_id = str(uuid4())
        logger.info("Starting reconciliation for %s (run_id=%s)", as_of_date, run_id)

        exceptions_df = self._build_exceptions(as_of_date, run_id)
        exception_count = len(exceptions_df)

        exceptions_path = self.parquet_writer.write_exceptions(
            exceptions_df,
            run_date=as_of_date,
            run_id=run_id,
        )
        logger.info("Exceptions written to %s", exceptions_path)

        metrics_payload: MetricsPayload | None = None
        if persist_metrics:
            metrics_payload = self.metrics_repo.compute_metrics(
                exceptions_df,
                run_id=run_id,
                as_of_date=as_of_date,
            )
            self.metrics_repo.persist_metrics(metrics_payload)
            logger.info("Metrics persisted for run_id=%s", run_id)

        return ReconResult(
            run_id=run_id,
            as_of_date=as_of_date,
            exceptions_path=exceptions_path,
            exceptions_file=exceptions_path.name,
            exception_count=exception_count,
            metrics=metrics_payload,
        )

    def _build_exceptions(self, as_of_date: date, run_id: str) -> pd.DataFrame:
        legacy_df, strategic_df = self._load_source_frames(as_of_date)
        exceptions_df = self._differ.build_exceptions_df(
            legacy_df,
            strategic_df,
            self.rule_set,
            run_id,
            as_of_date,
        )

        if "run_id" not in exceptions_df.columns:
            exceptions_df = exceptions_df.assign(run_id=run_id)

        for value_col in ("source_value", "target_value"):
            if value_col in exceptions_df.columns:
                exceptions_df[value_col] = (
                    exceptions_df[value_col]
                    .astype("string")
                    .where(exceptions_df[value_col].notna(), None)
                )

        return exceptions_df

    def _load_source_frames(self, as_of_date: date) -> tuple[pd.DataFrame, pd.DataFrame]:
        legacy_df = self.legacy_repo.load_by_date(as_of_date)
        strategic_df = self.strategic_repo.load_by_date(as_of_date)
        return legacy_df, strategic_df