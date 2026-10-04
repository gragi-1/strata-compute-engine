"""Storage admission has a separate lock from job admission and worker reservations."""

from datetime import UTC, datetime

from sqlalchemy import func, select, true
from sqlalchemy.orm import Session

from control_plane.access import principal
from control_plane.errors import DomainError
from control_plane.models import Artifact, DatasetFile, Project, ProjectStorageLock, UploadSession


def storage_admission(
    session: Session,
    project_id: str | None,
    size: int,
    *,
    now: datetime | None = None,
    exclude_upload: str | None = None,
) -> None:
    if project_id is None:
        return
    lock = session.scalar(
        select(ProjectStorageLock)
        .where(
            ProjectStorageLock.project_id == project_id,
        )
        .with_for_update()
    )
    project = session.get(Project, project_id)
    if lock is None or project is None or (not project.enabled and principal() is not None):
        raise DomainError(404, "project not found")
    # Charge logical references, even when content-addressing deduplicates the physical bytes.
    used = sum(
        session.scalar(
            select(func.coalesce(func.sum(model.size), 0))
            .where(
                model.project_id == project_id,
            )
            .execution_options(strata_unscoped=True)
        )
        or 0
        for model in (DatasetFile, Artifact)
    )
    reserved = (
        session.scalar(
            select(func.coalesce(func.sum(UploadSession.total_bytes), 0))
            .where(
                UploadSession.project_id == project_id,
                UploadSession.status == "OPEN",
                UploadSession.expires_at > (now or datetime.now(UTC)),
                UploadSession.id != exclude_upload if exclude_upload else true(),
            )
            .execution_options(strata_unscoped=True)
        )
        or 0
    )
    if used + reserved + size > project.storage_limit_bytes:
        raise DomainError(413, "project storage quota reached")
