"""Pipeline runner for reconciliation."""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from uuid import uuid4

from security_recon.domain.dictionary import AttributeRuleSet
from security_recon.domain.dictionary_loader import load_rules_from_resource
from security_recon.integration.parquet_writer import ParquetWriter
from security_recon.repositories.metrics_repository import MetricsRepository
from security_recon.repositories.security_repository import (
    LegacySecurityRepository,
    StrategicSecurityRepository,
)
from security_recon.support.database_service import DatabaseService
from security_recon.service.recon import DataFrameDiffer

class PipelineRunner:
    def __init__(self) -> None:
        self.db_service = DatabaseService()

        self.parquet_writer = ParquetWriter()
        self._base_output_dir = self.parquet_writer.base_dir
        self.run_id = uuid4()
        self.run_date = datetime.utcnow().date()

        self.legacy_repo = LegacySecurityRepository()
        self.strategic_repo = StrategicSecurityRepository()

        self.rule_set = load_rules_from_resource("data_dictionary.yml")

        self.metrics_repo = MetricsRepository()

    @property
    def base_output_dir(self) -> Path:
        return self._base_output_dir

    @base_output_dir.setter
    def base_output_dir(self, value: str | Path) -> None:
        path = Path(value)
        self._base_output_dir = path
        self.parquet_writer = ParquetWriter(self._base_output_dir)

    def run_recon(self, as_of_date: date) -> None:
        legacy_df = self.legacy_repo.load_by_date(as_of_date)
        strategic_df = self.strategic_repo.load_by_date(as_of_date)

        differ = DataFrameDiffer(self.rule_set)
        run_id_str = str(self.run_id)
        exceptions_df = differ.build_exceptions_df(
            legacy_df,
            strategic_df,
            self.rule_set,
            run_id_str,
            as_of_date,
        )

        if "run_id" in exceptions_df.columns:
            exceptions_df = exceptions_df.assign(run_id=run_id_str)

        for value_col in ("source_value", "target_value"):
            if value_col in exceptions_df.columns:
                exceptions_df[value_col] = (
                    exceptions_df[value_col]
                    .astype("string")
                    .where(exceptions_df[value_col].notna(), None)
                )

        details_df = exceptions_df.drop(columns=["run_id"], errors="ignore")
        print(
            f"Exceptions found: {len(exceptions_df)}, Details:\n{details_df.to_string(index=False)}"
        )

        output_path = self.parquet_writer.write_exceptions(
            exceptions_df,
            run_date=as_of_date,
            run_id=run_id_str,
        )
        print(f"Exceptions written to: {output_path}")

        # metrics = self.metrics_repo.compute_metrics(
        #     exceptions_df, run_id=self.run_id.int, as_of_date=as_of_date
        # )
