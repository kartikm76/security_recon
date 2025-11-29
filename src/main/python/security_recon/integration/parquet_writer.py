"""Parquet IO helpers."""
from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from security_recon.support.config import load_config


def _load_exception_config() -> Dict[str, Any]:
    return load_config().get("exception_file", {}) or {}


class ParquetWriter:
    def __init__(self, base_dir: str | Path | None = None, filename_pattern: str | None = None) -> None:
        cfg = _load_exception_config()
        directory = base_dir or cfg.get("directory", "./parquet")
        pattern = filename_pattern or cfg.get("filename", "exceptions.<runid>.<yyyymmdd>.parquet")

        self.base_dir = Path(directory)
        self.filename_pattern = pattern

    def write_exceptions(
        self,
        df: pd.DataFrame,
        run_date: date,
        run_id: str | int,
    ) -> Path:
        """Persist exceptions under the configured directory using the run metadata."""

        target_dir = self.base_dir
        target_dir.mkdir(parents=True, exist_ok=True)

        file_name = self._render_filename(run_id, run_date)
        out_path = target_dir / file_name
        df.to_parquet(out_path, index=False)
        return out_path

    def _render_filename(self, run_id: str | int, run_date: date) -> str:
        run_id_token = self._format_run_id(run_id)
        date_tokens = self._format_dates(run_date)

        filename = self.filename_pattern
        filename = filename.replace("<runid>", run_id_token)
        filename = filename.replace("<yyyy-mm-dd>", date_tokens["yyyy-mm-dd"])
        filename = filename.replace("<yyyymmdd>", date_tokens["yyyymmdd"])
        return filename

    @staticmethod
    def _format_run_id(run_id: str | int) -> str:
        if isinstance(run_id, int):
            return f"{run_id:02d}"
        try:
            as_int = int(run_id)
        except (TypeError, ValueError):
            return str(run_id)
        return f"{as_int:02d}"

    @staticmethod
    def _format_dates(run_date: date) -> Dict[str, str]:
        if isinstance(run_date, date):
            return {
                "yyyy-mm-dd": run_date.isoformat(),
                "yyyymmdd": run_date.strftime("%Y%m%d"),
            }
        value = str(run_date)
        return {"yyyy-mm-dd": value, "yyyymmdd": value}
