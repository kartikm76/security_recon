"""Command-line entrypoint wiring the pipeline, metrics, and S3 upload."""
from __future__ import annotations

from datetime import date

from security_recon.integration.s3_uploader import S3Uploader
from security_recon.repositories.artifact_repository import ArtifactRepository
from security_recon.service.run import ReconPipeline, ReconResult
from security_recon.support import configure_logging, get_logger

class Orchestration:
    def __init__(self, artifact_repo: ArtifactRepository | None = None) -> None:
        self.artifact_repo = artifact_repo or ArtifactRepository()

    def pipeline_orchestrator(self, as_of_date: date) -> ReconResult:
        configure_logging()
        logger = get_logger(__name__)

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
            self.artifact_repo.record_upload(
                run_id=result.run_id,
                as_of_date=result.as_of_date,
                artifact_type="exceptions",
                s3_uri=s3_url,
            )
        except ValueError as exc:
            logger.warning("Skipping S3 upload: %s", exc)
        except Exception:  # noqa: BLE001 - surface details in logs
            logger.exception("S3 upload failed")

        return result
