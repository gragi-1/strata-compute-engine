from datetime import datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    CheckConstraint,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from control_plane.database import Base, UTCDateTime


def identifier() -> str:
    return str(uuid4())


class Admission(Base):
    __tablename__ = "admission"
    id: Mapped[int] = mapped_column(primary_key=True)
    cluster_id: Mapped[str] = mapped_column(String(36), default=identifier)
    accepting_jobs: Mapped[bool] = mapped_column(Boolean, default=True, server_default=text("true"))
    scheduling_enabled: Mapped[bool] = mapped_column(
        Boolean, default=True, server_default=text("true")
    )
    reason: Mapped[str] = mapped_column(String(256), default="", server_default=text("''"))
    changed_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    changed_by: Mapped[str | None] = mapped_column(ForeignKey("users.id"))


class WorkerPool(Base):
    __tablename__ = "worker_pools"
    __table_args__ = (
        CheckConstraint("minimum >= 0 AND maximum >= minimum AND maximum <= hard_limit"),
    )
    id: Mapped[str] = mapped_column(String(48), primary_key=True)
    host_id: Mapped[str] = mapped_column(String(128), unique=True)
    configuration_hash: Mapped[str] = mapped_column(String(64))
    image: Mapped[str] = mapped_column(String(256))
    kind: Mapped[str] = mapped_column(String(16))
    cpu_per_worker: Mapped[float] = mapped_column(Float)
    memory_per_worker_mb: Mapped[int] = mapped_column(Integer)
    host_cpu_budget: Mapped[float] = mapped_column(Float)
    host_memory_budget_mb: Mapped[int] = mapped_column(Integer)
    hard_limit: Mapped[int] = mapped_column(Integer)
    minimum: Mapped[int] = mapped_column(Integer, default=0)
    maximum: Mapped[int] = mapped_column(Integer)
    enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    desired: Mapped[int] = mapped_column(Integer, default=0)
    last_seen_at: Mapped[datetime] = mapped_column(UTCDateTime)
    last_error: Mapped[str | None] = mapped_column(String(128))


class ProvisionedWorker(Base):
    __tablename__ = "provisioned_workers"
    worker_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    pool_id: Mapped[str] = mapped_column(ForeignKey("worker_pools.id"), index=True)
    phase: Mapped[str] = mapped_column(String(16))
    container_id: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)
    next_check_at: Mapped[datetime] = mapped_column(UTCDateTime)
    idle_since: Mapped[datetime | None] = mapped_column(UTCDateTime)
    removed_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    last_error: Mapped[str | None] = mapped_column(String(128))


class Job(Base):
    __tablename__ = "jobs"
    __table_args__ = (
        Index("ix_jobs_schedule", "status", "eligible_at", "priority", "created_at"),
        CheckConstraint("cpu_required > 0 AND memory_required_mb > 0"),
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=identifier)
    project_id: Mapped[str | None] = mapped_column(ForeignKey("projects.id"), index=True)
    created_by: Mapped[str | None] = mapped_column(ForeignKey("users.id"))
    name: Mapped[str] = mapped_column(String(128))
    image: Mapped[str] = mapped_column(String(256))
    expected_image_digest: Mapped[str | None] = mapped_column(String(71))
    dependency_policy: Mapped[str] = mapped_column(
        String(32), default="all_succeeded", server_default=text("'all_succeeded'")
    )
    execution_kind: Mapped[str] = mapped_column(
        String(16), default="container", server_default=text("'container'")
    )
    command: Mapped[list[str]] = mapped_column(JSON)
    capabilities: Mapped[list[str]] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(String(32))
    priority: Mapped[int] = mapped_column(Integer)
    cpu_required: Mapped[float] = mapped_column(Float)
    memory_required_mb: Mapped[int] = mapped_column(Integer)
    gpu_required: Mapped[int] = mapped_column(Integer, default=0, server_default=text("0"))
    gpu_memory_mb: Mapped[int] = mapped_column(Integer, default=0, server_default=text("0"))
    max_retries: Mapped[int] = mapped_column(Integer)
    timeout_seconds: Mapped[int] = mapped_column(Integer)
    idempotency_key: Mapped[str | None] = mapped_column(String(256), unique=True)
    request_hash: Mapped[str] = mapped_column(String(64))
    traceparent: Mapped[str] = mapped_column(String(128), default="")
    campaign_id: Mapped[str | None] = mapped_column(
        ForeignKey("campaigns.id", name="jobs_campaign_id_fkey"), index=True
    )
    parameters: Mapped[dict[str, str | int | float | bool]] = mapped_column(
        JSON, default=dict, server_default=text("'{}'")
    )
    inputs: Mapped[list[dict[str, str]]] = mapped_column(
        JSON, default=list, server_default=text("'[]'")
    )
    depends_on: Mapped[list[str]] = mapped_column(JSON, default=list, server_default=text("'[]'"))
    attempts_count: Mapped[int] = mapped_column(default=0)
    retry_count: Mapped[int] = mapped_column(default=0)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)
    eligible_at: Mapped[datetime] = mapped_column(UTCDateTime)
    scheduled_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    started_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    finished_at: Mapped[datetime | None] = mapped_column(UTCDateTime)


class ComputeGroup(Base):
    __tablename__ = "compute_groups"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=identifier)
    project_id: Mapped[str | None] = mapped_column(ForeignKey("projects.id"), index=True)
    created_by: Mapped[str | None] = mapped_column(ForeignKey("users.id"))
    name: Mapped[str] = mapped_column(String(128))
    nodes: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(16))
    specification: Mapped[dict[str, Any]] = mapped_column(JSON)
    next_sequence: Mapped[int] = mapped_column(Integer, default=0)
    idempotency_key: Mapped[str | None] = mapped_column(String(256), unique=True)
    request_hash: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)


class GroupMember(Base):
    __tablename__ = "group_members"
    __table_args__ = (Index("ix_group_member_job", "job_id", unique=True),)
    group_id: Mapped[str] = mapped_column(ForeignKey("compute_groups.id"), primary_key=True)
    rank: Mapped[int] = mapped_column(Integer, primary_key=True)
    job_id: Mapped[str] = mapped_column(ForeignKey("jobs.id"))


class CollectiveRound(Base):
    __tablename__ = "collective_rounds"
    group_id: Mapped[str] = mapped_column(ForeignKey("compute_groups.id"), primary_key=True)
    sequence: Mapped[int] = mapped_column(Integer, primary_key=True)
    operation: Mapped[str] = mapped_column(String(16))
    contributions: Mapped[dict[str, list[float]]] = mapped_column(JSON)
    result: Mapped[list[float] | None] = mapped_column(JSON, nullable=True)


class InteractiveSession(Base):
    __tablename__ = "interactive_sessions"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=identifier)
    project_id: Mapped[str | None] = mapped_column(ForeignKey("projects.id"), index=True)
    created_by: Mapped[str | None] = mapped_column(ForeignKey("users.id"))
    job_id: Mapped[str] = mapped_column(ForeignKey("jobs.id"), unique=True)
    kind: Mapped[str] = mapped_column(String(16))
    specification: Mapped[dict[str, Any]] = mapped_column(JSON)
    idle_seconds: Mapped[int] = mapped_column(Integer)
    next_sequence: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)
    last_activity_at: Mapped[datetime] = mapped_column(UTCDateTime)
    idempotency_key: Mapped[str | None] = mapped_column(String(256), unique=True)
    request_hash: Mapped[str] = mapped_column(String(64))


class SessionCell(Base):
    __tablename__ = "session_cells"
    __table_args__ = (
        Index("ix_session_cell_sequence", "session_id", "sequence", unique=True),
        Index("ix_session_cell_key", "session_id", "idempotency_key", unique=True),
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=identifier)
    project_id: Mapped[str | None] = mapped_column(ForeignKey("projects.id"), index=True)
    session_id: Mapped[str] = mapped_column(ForeignKey("interactive_sessions.id"))
    sequence: Mapped[int] = mapped_column(Integer)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)
    result: Mapped[dict[str, Any]] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(String(16))
    timeout_seconds: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)
    started_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    finished_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    idempotency_key: Mapped[str] = mapped_column(String(256))
    request_hash: Mapped[str] = mapped_column(String(64))


class Worker(Base):
    __tablename__ = "workers"
    __table_args__ = (
        CheckConstraint("cpu_total > 0 AND memory_total_mb > 0"),
        CheckConstraint("cpu_reserved >= 0 AND memory_reserved_mb >= 0"),
    )
    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    session_id: Mapped[str] = mapped_column(String(36))
    status: Mapped[str] = mapped_column(String(16))
    cpu_total: Mapped[float] = mapped_column(Float)
    memory_total_mb: Mapped[int] = mapped_column(Integer)
    cpu_available: Mapped[float] = mapped_column(Float)
    memory_available_mb: Mapped[int] = mapped_column(Integer)
    cpu_reserved: Mapped[float] = mapped_column(Float, default=0)
    memory_reserved_mb: Mapped[int] = mapped_column(Integer, default=0)
    running_jobs: Mapped[int] = mapped_column(Integer, default=0)
    capabilities: Mapped[list[str]] = mapped_column(JSON)
    last_heartbeat: Mapped[datetime] = mapped_column(UTCDateTime)
    heartbeats_count: Mapped[int] = mapped_column(Integer, default=0)
    failures_count: Mapped[int] = mapped_column(Integer, default=0)


class Attempt(Base):
    __tablename__ = "job_attempts"
    __table_args__ = (
        Index("ix_attempt_active", "status", "lease_expires_at"),
        Index("ix_attempt_job_number", "job_id", "number", unique=True),
        Index(
            "uq_active_attempt_job",
            "job_id",
            unique=True,
            postgresql_where=text("status IN ('SCHEDULED','RUNNING','CANCEL_REQUESTED')"),
            sqlite_where=text("status IN ('SCHEDULED','RUNNING','CANCEL_REQUESTED')"),
        ),
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=identifier)
    job_id: Mapped[str] = mapped_column(ForeignKey("jobs.id"))
    number: Mapped[int] = mapped_column(Integer)
    worker_id: Mapped[str] = mapped_column(ForeignKey("workers.id"))
    worker_session: Mapped[str] = mapped_column(String(36))
    lease_token: Mapped[str] = mapped_column(String(36), default=identifier)
    lease_expires_at: Mapped[datetime] = mapped_column(UTCDateTime)
    status: Mapped[str] = mapped_column(String(32))
    scheduled_at: Mapped[datetime] = mapped_column(UTCDateTime)
    eligible_at: Mapped[datetime] = mapped_column(UTCDateTime)
    started_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    finished_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    exit_code: Mapped[int | None] = mapped_column(Integer)
    reason: Mapped[str | None] = mapped_column(String(256))
    logs: Mapped[str] = mapped_column(Text, default="")
    gpu_ids: Mapped[list[str]] = mapped_column(JSON, default=list, server_default=text("'[]'"))
    provenance: Mapped[dict[str, Any]] = mapped_column(
        JSON, default=dict, server_default=text("'{}'")
    )
    cpu_reserved: Mapped[float] = mapped_column(Float, default=0, server_default=text("0"))
    memory_reserved_mb: Mapped[int] = mapped_column(Integer, default=0, server_default=text("0"))


class JobEvent(Base):
    __tablename__ = "job_events"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(ForeignKey("jobs.id"), index=True)
    attempt_id: Mapped[str | None] = mapped_column(ForeignKey("job_attempts.id"))
    kind: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)
    details: Mapped[dict[str, str | int | float]] = mapped_column(JSON)
    event_uid: Mapped[str | None] = mapped_column(String(36), unique=True)


class Artifact(Base):
    __tablename__ = "artifacts"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=identifier)
    project_id: Mapped[str | None] = mapped_column(ForeignKey("projects.id"), index=True)
    job_id: Mapped[str] = mapped_column(ForeignKey("jobs.id"), index=True)
    attempt_id: Mapped[str] = mapped_column(ForeignKey("job_attempts.id"))
    name: Mapped[str] = mapped_column(String(128))
    sha256: Mapped[str] = mapped_column(String(64))
    size: Mapped[int] = mapped_column(Integer)
    content_type: Mapped[str] = mapped_column(String(128))
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)


class WorkerHeartbeat(Base):
    __tablename__ = "worker_heartbeats"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    worker_id: Mapped[str] = mapped_column(ForeignKey("workers.id"), index=True)
    session_id: Mapped[str] = mapped_column(String(36))
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)
    cpu_available: Mapped[float] = mapped_column(Float)
    memory_available_mb: Mapped[int] = mapped_column(Integer)
    running_jobs: Mapped[int] = mapped_column(Integer)


class Campaign(Base):
    __tablename__ = "campaigns"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=identifier)
    project_id: Mapped[str | None] = mapped_column(ForeignKey("projects.id"), index=True)
    created_by: Mapped[str | None] = mapped_column(ForeignKey("users.id"))
    name: Mapped[str] = mapped_column(String(128))
    description: Mapped[str] = mapped_column(Text)
    specification: Mapped[dict[str, Any]] = mapped_column(JSON)
    request_hash: Mapped[str] = mapped_column(String(64))
    idempotency_key: Mapped[str | None] = mapped_column(String(256), unique=True)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)


class Dataset(Base):
    __tablename__ = "datasets"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=identifier)
    project_id: Mapped[str | None] = mapped_column(ForeignKey("projects.id"), index=True)
    created_by: Mapped[str | None] = mapped_column(ForeignKey("users.id"))
    name: Mapped[str] = mapped_column(String(128))
    description: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)


class DatasetVersion(Base):
    __tablename__ = "dataset_versions"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=identifier)
    project_id: Mapped[str | None] = mapped_column(ForeignKey("projects.id"), index=True)
    dataset_id: Mapped[str] = mapped_column(ForeignKey("datasets.id"), index=True)
    label: Mapped[str] = mapped_column(String(128))
    status: Mapped[str] = mapped_column(String(16), default="DRAFT")
    manifest_hash: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)


class DatasetFile(Base):
    __tablename__ = "dataset_files"
    __table_args__ = (Index("uq_dataset_file", "version_id", "name", unique=True),)
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=identifier)
    project_id: Mapped[str | None] = mapped_column(ForeignKey("projects.id"), index=True)
    version_id: Mapped[str] = mapped_column(ForeignKey("dataset_versions.id"), index=True)
    name: Mapped[str] = mapped_column(String(128))
    sha256: Mapped[str] = mapped_column(String(64), index=True)
    size: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)


class JobDependency(Base):
    __tablename__ = "job_dependencies"
    job_id: Mapped[str] = mapped_column(ForeignKey("jobs.id"), primary_key=True)
    parent_id: Mapped[str] = mapped_column(ForeignKey("jobs.id"), primary_key=True, index=True)


class User(Base):
    __tablename__ = "users"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=identifier)
    username: Mapped[str] = mapped_column(String(128), unique=True)
    password_hash: Mapped[str | None] = mapped_column(String(512))
    display_name: Mapped[str] = mapped_column(String(128))
    is_admin: Mapped[bool] = mapped_column(Boolean, default=False, server_default=text("false"))
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, server_default=text("true"))
    oidc_subject: Mapped[str | None] = mapped_column(String(512), unique=True)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)


class AccessToken(Base):
    __tablename__ = "access_tokens"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=identifier)
    user_id: Mapped[str] = mapped_column(ForeignKey("users.id"), index=True)
    token_hash: Mapped[str] = mapped_column(String(64), unique=True)
    name: Mapped[str] = mapped_column(String(128))
    kind: Mapped[str] = mapped_column(String(16), default="session")
    project_id: Mapped[str | None] = mapped_column(ForeignKey("projects.id"), index=True)
    role: Mapped[str | None] = mapped_column(String(16))
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)
    expires_at: Mapped[datetime] = mapped_column(UTCDateTime, index=True)
    revoked_at: Mapped[datetime | None] = mapped_column(UTCDateTime)


class LoginThrottle(Base):
    __tablename__ = "login_throttles"
    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    failures: Mapped[int] = mapped_column(Integer, default=0)
    window_start: Mapped[datetime] = mapped_column(UTCDateTime)


class Project(Base):
    __tablename__ = "projects"
    __table_args__ = (
        CheckConstraint("queue_limit > 0 AND cpu_limit > 0 AND memory_limit_mb > 0"),
        CheckConstraint("storage_limit_bytes > 0"),
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=identifier)
    name: Mapped[str] = mapped_column(String(128))
    description: Mapped[str] = mapped_column(Text, default="")
    queue_limit: Mapped[int] = mapped_column(Integer)
    cpu_limit: Mapped[float] = mapped_column(Float)
    memory_limit_mb: Mapped[int] = mapped_column(Integer)
    gpu_limit: Mapped[int] = mapped_column(Integer, default=8, server_default=text("8"))
    storage_limit_bytes: Mapped[int] = mapped_column(BigInteger)
    dispatch_count: Mapped[int] = mapped_column(BigInteger, default=0, server_default=text("0"))
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, server_default=text("true"))
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)


class ProjectMembership(Base):
    __tablename__ = "project_memberships"
    __table_args__ = (CheckConstraint("role IN ('viewer', 'operator', 'admin')"),)
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"), primary_key=True)
    user_id: Mapped[str] = mapped_column(ForeignKey("users.id"), primary_key=True)
    role: Mapped[str] = mapped_column(String(16))


class ProjectStorageLock(Base):
    __tablename__ = "project_storage_locks"
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"), primary_key=True)


class AuditEvent(Base):
    __tablename__ = "audit_events"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    actor_id: Mapped[str | None] = mapped_column(ForeignKey("users.id"), index=True)
    project_id: Mapped[str | None] = mapped_column(ForeignKey("projects.id"), index=True)
    action: Mapped[str] = mapped_column(String(128))
    resource_id: Mapped[str | None] = mapped_column(String(128))
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, index=True)
    details: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class UploadSession(Base):
    __tablename__ = "upload_sessions"
    __table_args__ = (
        CheckConstraint(
            "total_bytes >= 0 AND received_bytes >= 0 AND received_bytes <= total_bytes"
        ),
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=identifier)
    project_id: Mapped[str | None] = mapped_column(ForeignKey("projects.id"), index=True)
    version_id: Mapped[str] = mapped_column(ForeignKey("dataset_versions.id"), index=True)
    created_by: Mapped[str | None] = mapped_column(ForeignKey("users.id"))
    name: Mapped[str] = mapped_column(String(128))
    total_bytes: Mapped[int] = mapped_column(BigInteger)
    received_bytes: Mapped[int] = mapped_column(BigInteger, default=0)
    expected_sha256: Mapped[str | None] = mapped_column(String(64))
    chunk_bytes: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(16), default="OPEN")
    file_id: Mapped[str | None] = mapped_column(ForeignKey("dataset_files.id"))
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)
    expires_at: Mapped[datetime] = mapped_column(UTCDateTime, index=True)


class UploadPart(Base):
    __tablename__ = "upload_parts"
    upload_id: Mapped[str] = mapped_column(ForeignKey("upload_sessions.id"), primary_key=True)
    offset: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    sha256: Mapped[str] = mapped_column(String(64))
    size: Mapped[int] = mapped_column(Integer)


class Experiment(Base):
    __tablename__ = "experiments"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=identifier)
    project_id: Mapped[str | None] = mapped_column(ForeignKey("projects.id"), index=True)
    created_by: Mapped[str | None] = mapped_column(ForeignKey("users.id"))
    name: Mapped[str] = mapped_column(String(128))
    description: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)


class ExperimentRun(Base):
    __tablename__ = "experiment_runs"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=identifier)
    project_id: Mapped[str | None] = mapped_column(ForeignKey("projects.id"), index=True)
    experiment_id: Mapped[str] = mapped_column(ForeignKey("experiments.id"), index=True)
    job_id: Mapped[str] = mapped_column(ForeignKey("jobs.id"), unique=True)
    created_by: Mapped[str | None] = mapped_column(ForeignKey("users.id"))
    source_revision: Mapped[str | None] = mapped_column(String(64))
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON)
    specification: Mapped[dict[str, Any]] = mapped_column(JSON)
    metrics: Mapped[dict[str, float]] = mapped_column(JSON, default=dict)
    request_hash: Mapped[str] = mapped_column(String(64))
    idempotency_key: Mapped[str | None] = mapped_column(String(256), unique=True)
    replay_of: Mapped[str | None] = mapped_column(ForeignKey("experiment_runs.id"))
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)


class JobSchedule(Base):
    __tablename__ = "job_schedules"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=identifier)
    project_id: Mapped[str | None] = mapped_column(ForeignKey("projects.id"), index=True)
    created_by: Mapped[str | None] = mapped_column(ForeignKey("users.id"))
    name: Mapped[str] = mapped_column(String(128))
    cron: Mapped[str] = mapped_column(String(128))
    timezone: Mapped[str] = mapped_column(String(128))
    specification: Mapped[dict[str, Any]] = mapped_column(JSON)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    catch_up: Mapped[bool] = mapped_column(Boolean, default=False)
    next_run_at: Mapped[datetime] = mapped_column(UTCDateTime, index=True)
    next_check_at: Mapped[datetime] = mapped_column(UTCDateTime)
    last_error: Mapped[str | None] = mapped_column(String(256))
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)


class ScheduleFire(Base):
    __tablename__ = "schedule_fires"
    schedule_id: Mapped[str] = mapped_column(ForeignKey("job_schedules.id"), primary_key=True)
    scheduled_at: Mapped[datetime] = mapped_column(UTCDateTime, primary_key=True)
    job_id: Mapped[str] = mapped_column(ForeignKey("jobs.id"), unique=True)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)


class WorkflowExpansion(Base):
    __tablename__ = "workflow_expansions"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=identifier)
    project_id: Mapped[str | None] = mapped_column(ForeignKey("projects.id"), index=True)
    created_by: Mapped[str | None] = mapped_column(ForeignKey("users.id"))
    campaign_id: Mapped[str] = mapped_column(ForeignKey("campaigns.id"), index=True)
    source_job_id: Mapped[str] = mapped_column(ForeignKey("jobs.id"))
    gate_job_id: Mapped[str] = mapped_column(ForeignKey("jobs.id"), unique=True)
    name: Mapped[str] = mapped_column(String(64))
    artifact_name: Mapped[str] = mapped_column(String(128))
    template: Mapped[dict[str, Any]] = mapped_column(JSON)
    max_jobs: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(16), default="WAITING")
    generated_count: Mapped[int] = mapped_column(Integer, default=0)
    manifest_sha256: Mapped[str | None] = mapped_column(String(64))
    next_check_at: Mapped[datetime] = mapped_column(UTCDateTime)
    last_error: Mapped[str | None] = mapped_column(String(256))
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)


class Webhook(Base):
    __tablename__ = "webhooks"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=identifier)
    project_id: Mapped[str | None] = mapped_column(ForeignKey("projects.id"), index=True)
    created_by: Mapped[str | None] = mapped_column(ForeignKey("users.id"))
    name: Mapped[str] = mapped_column(String(128))
    target: Mapped[str] = mapped_column(String(64))
    events: Mapped[list[str]] = mapped_column(JSON)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)


class WebhookDelivery(Base):
    __tablename__ = "webhook_deliveries"
    __table_args__ = (
        Index("ix_webhook_delivery_due", "status", "next_attempt_at"),
        Index("uq_webhook_delivery_event", "webhook_id", "event_uid", unique=True),
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=identifier)
    webhook_id: Mapped[str] = mapped_column(ForeignKey("webhooks.id"), index=True)
    event_uid: Mapped[str] = mapped_column(ForeignKey("job_events.event_uid"))
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(String(16), default="PENDING")
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    attempt_limit: Mapped[int] = mapped_column(Integer)
    next_attempt_at: Mapped[datetime] = mapped_column(UTCDateTime)
    lease_token: Mapped[str | None] = mapped_column(String(36))
    lease_until: Mapped[datetime | None] = mapped_column(UTCDateTime)
    last_status: Mapped[int | None] = mapped_column(Integer)
    last_error: Mapped[str | None] = mapped_column(String(128))
    completed_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)


class OIDCIdentity(Base):
    __tablename__ = "oidc_identities"
    __table_args__ = (Index("uq_oidc_subject", "issuer", "subject", unique=True),)
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=identifier)
    issuer: Mapped[str] = mapped_column(String(512))
    subject: Mapped[str] = mapped_column(String(255))
    user_id: Mapped[str] = mapped_column(ForeignKey("users.id"), index=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)


class OIDCState(Base):
    __tablename__ = "oidc_states"
    state_hash: Mapped[str] = mapped_column(String(64), primary_key=True)
    browser_hash: Mapped[str] = mapped_column(String(64))
    address_hash: Mapped[str] = mapped_column(String(64), index=True)
    nonce_hash: Mapped[str] = mapped_column(String(64))
    encrypted_verifier: Mapped[str] = mapped_column(String(256))
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)
    expires_at: Mapped[datetime] = mapped_column(UTCDateTime, index=True)
    consumed_at: Mapped[datetime | None] = mapped_column(UTCDateTime)


class OIDCHandoff(Base):
    __tablename__ = "oidc_handoffs"
    token_hash: Mapped[str] = mapped_column(String(64), primary_key=True)
    identity_id: Mapped[str] = mapped_column(ForeignKey("oidc_identities.id"))
    browser_hash: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)
    expires_at: Mapped[datetime] = mapped_column(UTCDateTime, index=True)
    consumed_at: Mapped[datetime | None] = mapped_column(UTCDateTime)


class MaintenanceState(Base):
    __tablename__ = "maintenance_states"
    name: Mapped[str] = mapped_column(String(64), primary_key=True)
    status: Mapped[str] = mapped_column(String(16))
    next_run_at: Mapped[datetime] = mapped_column(UTCDateTime)
    started_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    succeeded_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    last_error: Mapped[str | None] = mapped_column(String(256))
    result: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class GPUDevice(Base):
    __tablename__ = "gpu_devices"
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    worker_id: Mapped[str] = mapped_column(ForeignKey("workers.id"), index=True)
    name: Mapped[str] = mapped_column(String(128))
    memory_mb: Mapped[int] = mapped_column(Integer)
    enabled: Mapped[bool] = mapped_column(Boolean)
    allocated_to: Mapped[str | None] = mapped_column(ForeignKey("job_attempts.id"), index=True)
