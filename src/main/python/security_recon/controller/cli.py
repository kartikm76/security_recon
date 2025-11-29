"""Minimal CLI entrypoint for manual pipeline runs."""
from __future__ import annotations

from datetime import date

from security_recon.service.run import ReconPipeline
from security_recon.support import configure_logging, get_logger


def main() -> None:
    configure_logging()
    logger = get_logger(__name__)

    pipeline = ReconPipeline(base_output_dir="parquet")
    result = pipeline.run(as_of_date=date(2023, 12, 30))

    logger.info(
        "Completed manual run %s with %s exceptions (file=%s)",
        result.run_id,
        result.exception_count,
        result.exceptions_path,
    )
    print("Jai Guruji")

if __name__ == "__main__":
    main()
