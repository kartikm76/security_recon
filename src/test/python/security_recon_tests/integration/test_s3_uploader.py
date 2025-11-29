from __future__ import annotations

from pathlib import Path

import pytest
from botocore.exceptions import ClientError

from security_recon.integration.s3_uploader import S3Uploader


class _FakeS3Client:
    def __init__(self) -> None:
        self._uploaded: set[tuple[str, str]] = set()

    def upload_fileobj(self, file_handle, bucket: str, key: str) -> None:
        file_handle.read()  # exhaust the stream like boto does
        self._uploaded.add((bucket, key))

    def head_object(self, Bucket: str, Key: str) -> None:  # noqa: N803 - boto naming
        if (Bucket, Key) not in self._uploaded:
            raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject")


def test_s3_upload(tmp_path: Path) -> None:
    file_name = "exceptions.test-run.20231230.parquet"
    source_dir = tmp_path / "parquet"
    source_dir.mkdir()
    (source_dir / file_name).write_text("dummy parquet contents", encoding="utf-8")

    uploader = S3Uploader(bucket="security-recon-bucket", prefix="test-prefix", source_dir=source_dir)
    uploader._s3 = _FakeS3Client()  # type: ignore[attr-defined] - test double

    uploaded_url = uploader.upload(file_name)

    assert uploaded_url == "s3://security-recon-bucket/test-prefix/exceptions.test-run.20231230.parquet"
    assert not (source_dir / file_name).exists()
    assert (source_dir / "uploaded" / file_name).exists()