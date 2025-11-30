from datetime import date
from pathlib import Path
from security_recon.integration.parquet_writer import ParquetWriter
import pandas as pd


def test_write_to_parquet(tmp_path: Path) -> None:
    output_root = tmp_path / "parquet"
    writer = ParquetWriter(output_root)

    sample_data = {
        "instrument_id": ["US0001", "US0002"],
        "as_of_date": ["2023-12-29", "2023-12-29"],
        "coupon": [5.0, 3.5],
    }
    df = pd.DataFrame(sample_data)

    output_path = writer.write_exceptions(df, date(2023, 12, 29), 1)

    expected_path = output_root / "exceptions.01.20231229.parquet"
    assert output_path == expected_path
    assert output_path.exists()
