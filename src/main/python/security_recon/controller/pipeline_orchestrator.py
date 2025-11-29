"""Command-line entrypoint wiring the pipeline, metrics, and S3 upload."""
from __future__ import annotations

from datetime import date

from security_recon.integration.s3_uploader import S3Uploader
from security_recon.service.run import ReconPipeline
from security_recon.support import configure_logging, get_logger


def main() -> None:
    configure_logging()
    logger = get_logger(__name__)

    as_of_date = date(2023, 12, 30)
    pipeline = ReconPipeline(base_output_dir="parquet")

    result = pipeline.run(as_of_date=as_of_date)
    logger.info(
        "Run %s generated %s exceptions (file=%s)",
        result.run_id,
        result.exception_count,
        result.exceptions_path,
    )

    try:
        uploader = S3Uploader(source_dir=result.exceptions_path.parent)
        s3_url = uploader.upload(result.exceptions_file)
        logger.info("Uploaded exceptions file to %s", s3_url)
    except ValueError as exc:
        logger.warning("Skipping S3 upload: %s", exc)
    except Exception:  # noqa: BLE001 - surface details in logs
        logger.exception("S3 upload failed")

if __name__ == "__main__":
    main()
