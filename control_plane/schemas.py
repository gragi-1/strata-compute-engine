from datetime import datetime
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, model_validator

from control_plane.domain import JobStatus


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Resources(StrictModel):
    cpu: float = Field(default=1, gt=0, le=256, allow_inf_nan=False)
    memory_mb: int = Field(default=256, gt=0, le=1048576)


class JobSubmit(StrictModel):
    name: str = Field(min_length=1, max_length=128)
    image: str = Field(min_length=1, max_length=256)
    command: list[Annotated[str, Field(min_length=1, max_length=4096)]] = Field(
        min_length=1,
        max_length=128,
    )
    resources: Resources = Field(default_factory=Resources)
    capabilities: list[str] = Field(default_factory=list, max_length=32)
    priority: int = Field(default=0, ge=0, le=100)
    max_retries: int = Field(default=3, ge=0, le=20)
    timeout_seconds: int = Field(default=600, ge=1, le=86400)


class JobView(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: str
    name: str
    image: str
    command: list[str]
    status: JobStatus
    priority: int
    cpu_required: float
    memory_required_mb: int
    max_retries: int
    timeout_seconds: int
    attempts_count: int
    retry_count: int
    created_at: datetime
    scheduled_at: datetime | None
    started_at: datetime | None
    finished_at: datetime | None


class WorkerRegister(StrictModel):
    worker_id: str = Field(pattern=r"^[a-zA-Z0-9_-]{1,128}$")
    cpu_total: float = Field(gt=0, le=256, allow_inf_nan=False)
    memory_total_mb: int = Field(gt=0, le=1048576)
    capabilities: list[str] = Field(default_factory=lambda: ["python", "cpp"], max_length=32)


class LeaseRef(StrictModel):
    attempt_id: str
    lease_token: str


class Heartbeat(StrictModel):
    session_id: str
    cpu_available: float = Field(ge=0, allow_inf_nan=False)
    memory_available_mb: int = Field(ge=0)
    leases: list[LeaseRef] = Field(default_factory=list, max_length=1000)


class AttemptCredentials(StrictModel):
    session_id: str
    lease_token: str


class Completion(AttemptCredentials):
    outcome: JobStatus
    exit_code: int | None = None
    reason: str | None = Field(default=None, max_length=256)

    @model_validator(mode="after")
    def valid_outcome(self) -> "Completion":
        if self.outcome not in {
            JobStatus.SUCCEEDED,
            JobStatus.FAILED,
            JobStatus.TIMED_OUT,
            JobStatus.CANCELLED,
        }:
            raise ValueError("invalid completion outcome")
        if self.outcome == JobStatus.SUCCEEDED and self.exit_code != 0:
            raise ValueError("success requires exit_code=0")
        return self
