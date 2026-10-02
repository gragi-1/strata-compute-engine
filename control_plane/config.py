from pathlib import Path

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="STRATA_", extra="ignore")
    database_url: str = "postgresql+psycopg://strata:strata@localhost:5432/strata"
    worker_token: str = "local-development-token"
    allowed_images: list[str] = ["strata/python-workloads:local", "strata/wave-solver:local"]
    queue_limit: int = Field(default=10000, ge=1)
    scheduler_batch_size: int = Field(default=100, ge=1, le=1000)
    worker_max_jobs: int = Field(default=16, ge=1, le=128)
    scheduler_interval: float = Field(default=0.5, gt=0)
    heartbeat_interval: float = Field(default=5, gt=0)
    worker_timeout: float = Field(default=15, gt=0)
    lease_seconds: float = Field(default=30, gt=0)
    retry_base_seconds: float = Field(default=2, gt=0)
    retry_max_seconds: float = Field(default=60, gt=0)
    retry_jitter_seconds: float = Field(default=1, ge=0)
    termination_grace_seconds: int = Field(default=5, ge=0, le=60)
    artifact_root: Path = Path("data/artifacts")
    artifact_max_bytes: int = Field(default=16 * 1024 * 1024, ge=1)
    logs_max_bytes: int = Field(default=1024 * 1024, ge=1)
    scheduling_policy: str = "least_loaded"

    @model_validator(mode="after")
    def timing(self) -> "Settings":
        if self.heartbeat_interval >= min(self.worker_timeout, self.lease_seconds) / 2:
            raise ValueError("heartbeat interval must be less than half both liveness windows")
        if self.scheduling_policy not in {"least_loaded", "best_fit", "fifo"}:
            raise ValueError("unknown scheduling policy")
        if self.retry_base_seconds > self.retry_max_seconds:
            raise ValueError("retry base cannot exceed retry cap")
        return self
