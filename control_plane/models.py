from datetime import datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import JSON, CheckConstraint, Float, ForeignKey, Index, Integer, String, Text, text
from sqlalchemy.orm import Mapped, mapped_column

from control_plane.database import Base, UTCDateTime


def identifier() -> str:
    return str(uuid4())


class Admission(Base):
    __tablename__ = "admission"
    id: Mapped[int] = mapped_column(primary_key=True)


class Job(Base):
    __tablename__ = "jobs"
    __table_args__ = (
        Index("ix_jobs_schedule", "status", "eligible_at", "priority", "created_at"),
        CheckConstraint("cpu_required > 0 AND memory_required_mb > 0"),
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=identifier)
    name: Mapped[str] = mapped_column(String(128))
    image: Mapped[str] = mapped_column(String(256))
    command: Mapped[list[str]] = mapped_column(JSON)
    capabilities: Mapped[list[str]] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(String(32))
    priority: Mapped[int] = mapped_column(Integer)
    cpu_required: Mapped[float] = mapped_column(Float)
    memory_required_mb: Mapped[int] = mapped_column(Integer)
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


class JobEvent(Base):
    __tablename__ = "job_events"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(ForeignKey("jobs.id"), index=True)
    attempt_id: Mapped[str | None] = mapped_column(ForeignKey("job_attempts.id"))
    kind: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)
    details: Mapped[dict[str, str | int | float]] = mapped_column(JSON)


class Artifact(Base):
    __tablename__ = "artifacts"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=identifier)
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
    name: Mapped[str] = mapped_column(String(128))
    description: Mapped[str] = mapped_column(Text)
    specification: Mapped[dict[str, Any]] = mapped_column(JSON)
    request_hash: Mapped[str] = mapped_column(String(64))
    idempotency_key: Mapped[str | None] = mapped_column(String(256), unique=True)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)


class Dataset(Base):
    __tablename__ = "datasets"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=identifier)
    name: Mapped[str] = mapped_column(String(128))
    description: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)


class DatasetVersion(Base):
    __tablename__ = "dataset_versions"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=identifier)
    dataset_id: Mapped[str] = mapped_column(ForeignKey("datasets.id"), index=True)
    label: Mapped[str] = mapped_column(String(128))
    status: Mapped[str] = mapped_column(String(16), default="DRAFT")
    manifest_hash: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)


class DatasetFile(Base):
    __tablename__ = "dataset_files"
    __table_args__ = (Index("uq_dataset_file", "version_id", "name", unique=True),)
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=identifier)
    version_id: Mapped[str] = mapped_column(ForeignKey("dataset_versions.id"), index=True)
    name: Mapped[str] = mapped_column(String(128))
    sha256: Mapped[str] = mapped_column(String(64), index=True)
    size: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)


class JobDependency(Base):
    __tablename__ = "job_dependencies"
    job_id: Mapped[str] = mapped_column(ForeignKey("jobs.id"), primary_key=True)
    parent_id: Mapped[str] = mapped_column(ForeignKey("jobs.id"), primary_key=True, index=True)
