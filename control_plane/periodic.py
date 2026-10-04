"""Durable periodic jobs with transactional firing and bounded restart catch-up."""

import hashlib
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from croniter import croniter
from pydantic import Field, ValidationError, model_validator
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from control_plane.access import Principal, access_scope, actor_id, audit, project_id
from control_plane.errors import AdmissionPaused
from control_plane.identity import IdentityService
from control_plane.models import (
    Admission,
    JobSchedule,
    Project,
    ScheduleFire,
    User,
    Webhook,
    WorkflowExpansion,
    identifier,
)
from control_plane.schemas import JobSubmit, StrictModel
from control_plane.services import DomainError, EngineService


def next_occurrence(expression: str, timezone: str, after: datetime) -> datetime:
    try:
        value: datetime = croniter(
            expression, after.astimezone(ZoneInfo(timezone)), max_years_between_matches=5
        ).get_next(datetime)
    except (ValueError, ZoneInfoNotFoundError, OverflowError) as exc:
        raise DomainError(
            422, "cron expression needs an occurrence within five years and a valid timezone"
        ) from exc
    return value.astimezone(UTC)


class ScheduleSubmit(StrictModel):
    name: str = Field(min_length=1, max_length=128)
    cron: str = Field(min_length=1, max_length=128)
    timezone: str = Field(default="UTC", min_length=1, max_length=128)
    job: JobSubmit
    catch_up: bool = False

    @model_validator(mode="after")
    def valid_cron(self) -> "ScheduleSubmit":
        if len(self.cron.split()) != 5 or not croniter.is_valid(self.cron):
            raise ValueError("use a valid five-field cron expression")
        try:
            next_occurrence(self.cron, self.timezone, datetime.now(UTC))
        except DomainError as exc:
            raise ValueError(str(exc)) from exc
        return self


class PeriodicService:
    def __init__(self, svc: EngineService) -> None:
        self.svc = svc

    def capacity(self, session: Session) -> None:
        count = (
            session.scalar(
                select(func.count())
                .select_from(JobSchedule)
                .where(JobSchedule.enabled.is_(True))
                .execution_options(strata_unscoped=True)
            )
            or 0
        )
        if count >= self.svc.settings.schedule_max_active:
            raise DomainError(429, "active schedule capacity reached")

    def create(self, body: ScheduleSubmit) -> JobSchedule:
        if body.job.image not in self.svc.settings.allowed_images:
            raise DomainError(422, "schedule image is not allowlisted")
        with self.svc.factory.begin() as session:
            session.scalar(select(Admission).where(Admission.id == 1).with_for_update())
            self.capacity(session)
            now = self.svc.now(session)
            # Validate all input/dependency references without admitting a job yet.
            # Scheduled jobs are independent occurrences; workflows can be submitted separately.
            if (
                body.job.depends_on
                or body.job.artifact_inputs
                or body.job.dependency_policy != "all_succeeded"
            ):
                raise DomainError(
                    422,
                    "periodic job templates require sealed dataset inputs and no job dependencies",
                )
            from control_plane.models import DatasetVersion

            for item in body.job.inputs:
                version = session.get(DatasetVersion, item.version_id)
                if version is None or version.status != "SEALED":
                    raise DomainError(422, "schedule inputs must refer to sealed dataset versions")
            row = JobSchedule(
                id=identifier(),
                project_id=project_id(),
                created_by=actor_id(),
                name=body.name,
                cron=body.cron,
                timezone=body.timezone,
                specification=body.job.model_dump(exclude_none=True),
                enabled=True,
                catch_up=body.catch_up,
                next_run_at=next_occurrence(body.cron, body.timezone, now),
                next_check_at=now,
                created_at=now,
            )
            session.add(row)
            audit(session, now, "SCHEDULE_CREATED", row.id, row.project_id)
            return row

    def row(self, session: Session, schedule_id: str, *, lock: bool = False) -> JobSchedule:
        query = select(JobSchedule).where(JobSchedule.id == schedule_id)
        row = session.scalar(query.with_for_update() if lock else query)
        if row is None:
            raise DomainError(404, "schedule not found")
        return row

    def action(self, schedule_id: str, enabled: bool) -> JobSchedule:
        with self.svc.factory.begin() as session:
            session.scalar(select(Admission).where(Admission.id == 1).with_for_update())
            row = self.row(session, schedule_id, lock=True)
            if enabled == row.enabled:
                return row
            if enabled:
                self.capacity(session)
                if row.created_by is None and row.project_id:
                    if not actor_id():
                        raise DomainError(
                            403, "resuming this schedule requires an individual owner"
                        )
                    row.created_by = actor_id()
            row.enabled = enabled
            row.last_error = None
            row.next_check_at = self.svc.now(session)
            if enabled:
                row.next_run_at = next_occurrence(row.cron, row.timezone, self.svc.now(session))
            audit(
                session,
                self.svc.now(session),
                "SCHEDULE_RESUMED" if enabled else "SCHEDULE_PAUSED",
                row.id,
                row.project_id,
            )
            return row

    def actor(
        self, session: Session, row: JobSchedule | WorkflowExpansion | Webhook
    ) -> Principal | None:
        if row.project_id is None:
            if self.svc.settings.identity_enabled or session.scalar(select(Project.id).limit(1)):
                raise DomainError(403, "legacy schedules require an explicit project owner")
            return None
        user = session.get(User, row.created_by) if row.created_by else None
        if user is None or not user.enabled:
            raise DomainError(403, "schedule owner is disabled or unavailable")
        actor = Principal(user.id, user.username, user.is_admin, "scheduled-job")
        return IdentityService(self.svc).project_access(actor, row.project_id, "POST")

    def tick(self, *, limit: int = 20) -> int:
        fired = 0
        # Separate short transactions prevent one rejected template delaying other schedules.
        with self.svc.factory() as session:
            now = self.svc.now(session)
            ids = list(
                session.scalars(
                    select(JobSchedule.id)
                    .where(
                        JobSchedule.enabled.is_(True),
                        JobSchedule.next_run_at <= now,
                        JobSchedule.next_check_at <= now,
                    )
                    .order_by(JobSchedule.next_run_at, JobSchedule.id)
                    .limit(limit)
                )
            )
        for schedule_id in ids:
            with self.svc.factory.begin() as session:
                # Admission -> schedule -> project is shared with creation/resume.
                session.scalar(select(Admission).where(Admission.id == 1).with_for_update())
                row = session.scalar(
                    select(JobSchedule)
                    .where(JobSchedule.id == schedule_id)
                    .with_for_update(skip_locked=True)
                )
                now = self.svc.now(session)
                if (
                    row is None
                    or not row.enabled
                    or row.next_run_at > now
                    or row.next_check_at > now
                ):
                    continue
                try:
                    actor = self.actor(session, row)
                    try:
                        spec = JobSubmit.model_validate(row.specification)
                    except ValidationError as exc:
                        raise DomainError(422, "schedule template is invalid") from exc
                    if spec.image not in self.svc.settings.allowed_images:
                        raise DomainError(422, "scheduled image is no longer allowlisted")
                    due = row.next_run_at
                    if not row.catch_up:
                        due = (
                            croniter(
                                row.cron,
                                (now + timedelta(microseconds=1)).astimezone(
                                    ZoneInfo(row.timezone)
                                ),
                                max_years_between_matches=5,
                            )
                            .get_prev(datetime)
                            .astimezone(UTC)
                        )
                    with access_scope(actor):
                        self.svc.admission(session)
                        key = hashlib.sha256(f"{row.id}:{due.isoformat()}".encode()).hexdigest()
                        job = self.svc.create_job(
                            session,
                            spec,
                            None,
                            key,
                            parameters={"schedule_id": row.id, "scheduled_at": due.isoformat()},
                        )
                        session.add(
                            ScheduleFire(
                                schedule_id=row.id, scheduled_at=due, job_id=job.id, created_at=now
                            )
                        )
                        audit(session, now, "SCHEDULE_FIRED", row.id, row.project_id, job_id=job.id)
                    row.next_run_at = next_occurrence(row.cron, row.timezone, due)
                    row.next_check_at = now
                    row.last_error = None
                    fired += 1
                except DomainError as exc:
                    row.last_error = str(exc)[:256]
                    # Preserve the due occurrence on capacity failure, with bounded polling.
                    deferred = exc.code == 429 or isinstance(exc, AdmissionPaused)
                    if not deferred:
                        row.enabled = False
                    else:
                        row.next_check_at = now + timedelta(seconds=30)
                    audit(
                        session,
                        now,
                        "SCHEDULE_DEFERRED" if deferred else "SCHEDULE_BLOCKED",
                        row.id,
                        row.project_id,
                        code=exc.code,
                    )
        return fired
