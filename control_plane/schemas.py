from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from control_plane.domain import JobStatus


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class AdmissionUpdate(StrictModel):
    accepting_jobs: bool
    scheduling_enabled: bool
    reason: str = Field(default="", max_length=256, pattern=r"^[^\x00-\x1f\x7f]*$")


class Resources(StrictModel):
    cpu: float = Field(default=1, gt=0, le=256, allow_inf_nan=False)
    memory_mb: int = Field(default=256, gt=0, le=1048576)
    gpus: int = Field(default=0, ge=0, le=64)
    gpu_memory_mb: int = Field(default=0, ge=0, le=1048576)

    @model_validator(mode="after")
    def gpu_memory_requires_device(self) -> "Resources":
        if self.gpu_memory_mb and not self.gpus:
            raise ValueError("GPU memory requirements need at least one GPU")
        return self


class JobSubmit(StrictModel):
    name: str = Field(min_length=1, max_length=128)
    image: str = Field(min_length=1, max_length=256)
    expected_image_digest: str | None = Field(default=None, pattern=r"^sha256:[0-9a-f]{64}$")
    dependency_policy: Literal["all_succeeded", "all_terminal", "any_failed"] = "all_succeeded"
    command: list[Annotated[str, Field(min_length=1, max_length=4096)]] = Field(
        min_length=1,
        max_length=128,
    )
    resources: Resources = Field(default_factory=Resources)
    capabilities: list[str] = Field(default_factory=list, max_length=32)
    priority: int = Field(default=0, ge=0, le=100)
    max_retries: int = Field(default=3, ge=0, le=20)
    timeout_seconds: int = Field(default=600, ge=1, le=86400)
    inputs: list["DatasetInput"] = Field(default_factory=list, max_length=16)
    depends_on: list[str] = Field(default_factory=list, max_length=100)
    artifact_inputs: list["ArtifactInput"] = Field(default_factory=list, max_length=1000)

    @model_validator(mode="after")
    def unique_inputs(self) -> "JobSubmit":
        aliases = [item.alias for item in self.inputs] + [
            item.alias for item in self.artifact_inputs
        ]
        if len(set(aliases)) != len(aliases):
            raise ValueError("input aliases must be unique")
        return self


class JobView(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: str
    project_id: str | None = None
    created_by: str | None = None
    name: str
    image: str
    expected_image_digest: str | None = None
    dependency_policy: str = "all_succeeded"
    execution_kind: str = "container"
    command: list[str]
    status: JobStatus
    priority: int
    cpu_required: float
    memory_required_mb: int
    gpu_required: int = 0
    gpu_memory_mb: int = 0
    max_retries: int
    timeout_seconds: int
    attempts_count: int
    retry_count: int
    created_at: datetime
    scheduled_at: datetime | None
    started_at: datetime | None
    finished_at: datetime | None
    campaign_id: str | None
    parameters: dict[str, str | int | float | bool]
    inputs: list[dict[str, str]]
    depends_on: list[str]


class DatasetInput(StrictModel):
    version_id: str = Field(min_length=1, max_length=36)
    alias: str = Field(pattern=r"^[a-zA-Z][a-zA-Z0-9_-]{0,31}$")


class ArtifactInput(StrictModel):
    job_id: str = Field(min_length=1, max_length=128)
    name: str = Field(pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$")
    alias: str = Field(pattern=r"^[a-zA-Z][a-zA-Z0-9_-]{0,31}$")
    artifact_id: str | None = Field(default=None, min_length=1, max_length=36)


class NamedResource(StrictModel):
    name: str = Field(min_length=1, max_length=128)
    description: str = Field(default="", max_length=4096)


class VersionCreate(StrictModel):
    label: str = Field(min_length=1, max_length=128)


class CampaignSubmit(NamedResource):
    template: JobSubmit
    matrix: dict[str, list[str | int | float | bool]] = Field(default_factory=dict, max_length=16)
    repeats: int = Field(default=1, ge=1, le=1000)

    @model_validator(mode="after")
    def valid_matrix(self) -> "CampaignSubmit":
        import math
        import re

        for key, values in self.matrix.items():
            if not re.fullmatch(r"[a-zA-Z][a-zA-Z0-9_]{0,31}", key) or key == "repeat":
                raise ValueError("invalid or reserved matrix parameter")
            if not values or len(values) > 1000:
                raise ValueError("matrix dimensions need 1..1000 values")
            if any(isinstance(v, float) and not math.isfinite(v) for v in values):
                raise ValueError("matrix values must be finite")
        return self


class DynamicExpansion(StrictModel):
    source: str = Field(min_length=1, max_length=64)
    artifact: str = Field(pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$")
    template: JobSubmit
    max_jobs: int = Field(default=1000, ge=1, le=1000)

    @model_validator(mode="after")
    def independent_template(self) -> "DynamicExpansion":
        if (
            self.template.depends_on
            or self.template.artifact_inputs
            or self.template.dependency_policy != "all_succeeded"
        ):
            raise ValueError("expansion templates accept datasets and no existing job dependencies")
        return self


class WorkflowSubmit(NamedResource):
    nodes: dict[str, JobSubmit] = Field(min_length=1, max_length=1000)
    expansions: dict[str, DynamicExpansion] = Field(default_factory=dict, max_length=100)


class GPURegistration(StrictModel):
    id: str = Field(pattern=r"^GPU-[0-9a-fA-F]{8}-(?:[0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$")
    name: str = Field(min_length=1, max_length=128)
    memory_mb: int = Field(ge=1, le=1048576)

    @field_validator("id")
    @classmethod
    def canonical_id(cls, value: str) -> str:
        return "GPU-" + value[4:].lower()


class WorkerRegister(StrictModel):
    worker_id: str = Field(pattern=r"^[a-zA-Z0-9_-]{1,128}$")
    cpu_total: float = Field(gt=0, le=256, allow_inf_nan=False)
    memory_total_mb: int = Field(gt=0, le=1048576)
    capabilities: list[str] = Field(default_factory=lambda: ["python", "cpp"], max_length=32)
    gpus: list[GPURegistration] = Field(default_factory=list, max_length=64)

    @model_validator(mode="after")
    def unique_gpus(self) -> "WorkerRegister":
        if len({gpu.id for gpu in self.gpus}) != len(self.gpus):
            raise ValueError("GPU device IDs must be unique")
        return self


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
