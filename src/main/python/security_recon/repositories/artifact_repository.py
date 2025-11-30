"""Repository for recording and retrieving artifact upload records."""
from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from security_recon.repositories.data_models import ArtifactLog
from security_recon.support.database_service import DatabaseService


class ArtifactRepository:
    """Encapsulates access to the artifact_log table."""

    def __init__(self, db_service: DatabaseService | None = None) -> None:
        self.db_service = db_service or DatabaseService()

    def record_upload(
        self,
        *,
        run_id: str,
        as_of_date: date,
        artifact_type: str,
        s3_uri: str,
        status: str = "uploaded",
        uploaded_at: Optional[datetime] = None,
    ) -> ArtifactLog:
        session: Session = self.db_service.postgres_session_factory()
        try:
            record = ArtifactLog(
                run_id=run_id,
                as_of_date=as_of_date,
                artifact_type=artifact_type,
                s3_uri=s3_uri,
                status=status,
                uploaded_at=uploaded_at or datetime.utcnow(),
            )
            session.add(record)
            session.commit()
            session.refresh(record)
            return record
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def fetch_latest(self, run_id: str, *, artifact_type: str) -> Optional[ArtifactLog]:
        session: Session = self.db_service.postgres_session_factory()
        try:
            stmt = (
                select(ArtifactLog)
                .where(
                    ArtifactLog.run_id == run_id,
                    ArtifactLog.artifact_type == artifact_type,
                )
                .order_by(ArtifactLog.uploaded_at.desc())
            )
            return session.execute(stmt).scalars().first()
        finally:
            session.close()
