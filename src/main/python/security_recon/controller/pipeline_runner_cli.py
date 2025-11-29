"""Command-line entrypoint for reconciliation utilities."""
from __future__ import annotations

from datetime import date

from security_recon.service.run import PipelineRunner


def main() -> None:
    print("Jai Guruji")
    runner = PipelineRunner()
    runner.base_output_dir = "parquet"
    runner.run_recon(as_of_date=date(2023, 12, 30))


if __name__ == "__main__":
    main()
