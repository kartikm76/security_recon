"""Parquet IO helpers."""
from pathlib import Path
import pandas as pd
from datetime import date, datetime

class ParquetWriter:
    def __init__(self, base_dir: str | Path) -> None:
        self.base_dir = Path(base_dir)

    def write_exceptions(self, df: pd.DataFrame, domain: str, run_date: date, run_id: str) -> Path:
        out_dir = self.base_dir / "results" / f"domain={domain}" / {"run_date={run_date}"} / f"run_id={run_id}"
        out_dir.mkdir(parents = True, exist_ok = True)
        out_path = out_dir / "exceptions.parquet"
        df.to_parquet(out_path, index=False)
        return out_path