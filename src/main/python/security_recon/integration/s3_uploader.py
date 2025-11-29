"""Helpers for uploading parquet outputs to Amazon S3."""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import boto3
from botocore.exceptions import ClientError

from security_recon.support.config import load_config

class S3Uploader:
    """Streams parquet artifacts to S3 and archives them locally."""

    def __init__(
        self,
        bucket: Optional[str] = None,
        prefix: Optional[str] = None,
        source_dir: Optional[str | Path] = None,
    ) -> None:
        self._s3 = boto3.client("s3")
        config = load_config().get("s3", {})

        self.bucket = bucket or config.get("bucket")
        self.prefix = prefix or config.get("prefix", "results")

        if not self.bucket:
            raise ValueError(
                "S3 bucket not configured. Provide it explicitly or set the `s3.bucket` value in application.yml."
            )
        self.source_dir = Path(source_dir or "parquet").resolve()
        self.uploaded_dir = self.source_dir / "uploaded"

    def upload(self, file_name: str) -> str:
        """Upload a parquet file to S3 and move it to ``parquet/uploaded`` locally."""
        source_path = self.source_dir / file_name
        if not source_path.exists():
            raise FileNotFoundError(f"Parquet file not found: {source_path}")

        key = self._build_key(file_name)

        with source_path.open("rb") as file_handle:
            self._s3.upload_fileobj(file_handle, self.bucket, key)

        self._verify_upload(key)

        self.uploaded_dir.mkdir(parents=True, exist_ok=True)
        destination_path = self.uploaded_dir / file_name
        source_path.replace(destination_path)

        return f"s3://{self.bucket}/{key}"

    def _build_key(self, file_name: str) -> str:
        prefix = self.prefix.rstrip("/")
        return f"{prefix}/{file_name}" if prefix else file_name

    def _verify_upload(self, key: str) -> None:
        try:
            self._s3.head_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:  # noqa: BLE001
            raise RuntimeError(f"Failed to verify S3 upload for {key}") from exc
