from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="STRATA_", extra="ignore")
    database_url: str = "postgresql+psycopg://strata:strata@localhost:5432/strata"
    worker_token: str = "local-development-token"
    api_keys: dict[str, str] = {}
    production: bool = False
    identity_enabled: bool = False
    oidc_issuer: str = Field(default="", max_length=512)
    oidc_client_id: str = Field(default="", max_length=255)
    oidc_client_secret: str = Field(default="", repr=False)
    oidc_token_auth_method: Literal["none", "client_secret_basic", "client_secret_post"] = "none"
    oidc_redirect_uri: str = Field(default="", max_length=2048)
    oidc_state_encryption_key: str = Field(default="", repr=False)
    oidc_allowed_origins: list[str] = Field(default_factory=list, max_length=10)
    session_lifetime_seconds: int = Field(default=28800, ge=60, le=604800)
    login_attempt_limit: int = Field(default=5, ge=1, le=100)
    login_window_seconds: int = Field(default=300, ge=30, le=3600)
    project_queue_limit: int = Field(default=1000, ge=1)
    project_cpu_limit: float = Field(default=32, gt=0, allow_inf_nan=False)
    project_memory_limit_mb: int = Field(default=65536, ge=1)
    project_gpu_limit: int = Field(default=8, ge=0, le=1024)
    project_storage_limit_bytes: int = Field(default=10 * 1024**3, ge=1)
    tls_cert: Path | None = None
    tls_key: Path | None = None
    dataset_max_bytes: int = Field(default=1024 * 1024 * 1024, ge=1)
    dataset_max_files: int = Field(default=1000, ge=1, le=10000)
    upload_chunk_bytes: int = Field(default=8 * 1024**2, ge=65536, le=64 * 1024**2)
    upload_lifetime_seconds: int = Field(default=86400, ge=60, le=604800)
    upload_max_active: int = Field(default=1000, ge=1, le=100000)
    preview_max_bytes: int = Field(default=8 * 1024 * 1024, ge=1024)
    query_memory_mb: int = Field(default=256, ge=64, le=4096)
    query_timeout_seconds: int = Field(default=15, ge=1, le=300)
    query_max_concurrent: int = Field(default=2, ge=1, le=16)
    query_response_bytes: int = Field(default=2 * 1024**2, ge=65536, le=16 * 1024**2)
    campaign_max_jobs: int = Field(default=10000, ge=1, le=100000)
    schedule_max_active: int = Field(default=1000, ge=1, le=100000)
    webhook_targets: dict[str, str] = {}
    webhook_secrets: dict[str, str] = Field(default_factory=dict, repr=False)
    webhook_max_active: int = Field(default=1000, ge=1, le=10000)
    webhook_attempt_limit: int = Field(default=8, ge=1, le=30)
    allowed_images: list[str] = ["strata/python-workloads:local", "strata/wave-solver:local"]
    gpu_discovery_image: str = Field(default="", max_length=256)
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
    storage_backend: str = "filesystem"
    s3_bucket: str = ""
    s3_prefix: str = "strata/blobs/"
    s3_endpoint_url: str | None = None
    s3_region: str = "us-east-1"
    storage_min_free_bytes: int = Field(default=256 * 1024 * 1024, ge=0)
    storage_max_local_bytes: int = Field(default=20 * 1024**3, ge=1)
    storage_cache_bytes: int = Field(default=4 * 1024**3, ge=1)
    worker_cache_root: Path = Path("data/worker-cache")
    worker_cache_bytes: int = Field(default=4 * 1024**3, ge=1)
    storage_keeper_image: str = Field(
        default="strata/control-plane:local", min_length=1, max_length=256
    )
    worker_output_bytes: int = Field(default=64 * 1024**2, ge=1024**2, le=16 * 1024**3)
    worker_output_inodes: int = Field(default=4096, ge=64, le=1048576)
    storage_lock_timeout_seconds: int = Field(default=30, ge=1, le=300)
    blob_retention_seconds: int = Field(default=86400, ge=60)
    history_retention_seconds: int = Field(default=30 * 86400, ge=60)
    maintenance_interval_seconds: int = Field(default=60, ge=10, le=86400)
    maintenance_apply_history: bool = False
    maintenance_apply_blobs: bool = False
    backup_root: Path | None = None
    backup_interval_seconds: int = Field(default=86400, ge=60)
    backup_keep: int = Field(default=7, ge=1, le=1000)
    backup_max_bytes: int = Field(default=100 * 1024**3, ge=1024**2)
    backup_timeout_seconds: int = Field(default=3600, ge=10, le=86400)
    backup_restore_drill: bool = False
    artifact_max_bytes: int = Field(default=16 * 1024 * 1024, ge=1)
    logs_max_bytes: int = Field(default=1024 * 1024, ge=1)
    scheduling_policy: str = "least_loaded"
    priority_aging_seconds: int = Field(default=60, ge=1, le=86400)

    @model_validator(mode="after")
    def timing(self) -> "Settings":
        import re
        from urllib.parse import urlsplit

        if self.oidc_issuer:
            from cryptography.fernet import Fernet

            if not self.identity_enabled or not self.oidc_client_id or not self.oidc_redirect_uri:
                raise ValueError("OIDC requires individual identity, client ID and callback URI")
            if (self.oidc_token_auth_method == "none") != (not self.oidc_client_secret):
                raise ValueError("OIDC confidential clients require an explicit token auth method")
            try:
                Fernet(self.oidc_state_encryption_key.encode())
            except ValueError as exc:
                raise ValueError("OIDC requires a valid Fernet state encryption key") from exc
            for url in [self.oidc_issuer, self.oidc_redirect_uri, *self.oidc_allowed_origins]:
                endpoint = urlsplit(url)
                if (
                    endpoint.scheme not in {"http", "https"}
                    or not endpoint.hostname
                    or endpoint.username
                    or endpoint.password
                    or endpoint.query
                    or endpoint.fragment
                    or not url.isascii()
                    or endpoint.scheme == "http"
                    and (
                        self.production
                        or endpoint.hostname not in {"127.0.0.1", "localhost", "::1"}
                    )
                ):
                    raise ValueError("OIDC URLs require HTTPS or loopback HTTP in development")
            if urlsplit(self.oidc_redirect_uri).path != "/auth/oidc/callback":
                raise ValueError("OIDC callback URI must end with /auth/oidc/callback")
            if any(urlsplit(url).path not in {"", "/"} for url in self.oidc_allowed_origins):
                raise ValueError("OIDC allowed origins cannot contain a path")
            self.oidc_allowed_origins = [url.rstrip("/") for url in self.oidc_allowed_origins]

        for target, url in self.webhook_targets.items():
            endpoint = urlsplit(url)
            if (
                not re.fullmatch(r"[a-zA-Z][a-zA-Z0-9_-]{0,63}", target)
                or endpoint.scheme not in ({"https"} if self.production else {"http", "https"})
                or not endpoint.hostname
                or endpoint.username
                or endpoint.password
                or endpoint.fragment
                or len(url) > 2048
                or len(self.webhook_secrets.get(target, "")) < 32
            ):
                raise ValueError(
                    "webhook targets require a valid URL and a signing secret of at least "
                    "32 characters"
                )
        if self.storage_backend not in {"filesystem", "s3"}:
            raise ValueError("storage backend must be filesystem or s3")
        if self.storage_backend == "s3" and not self.s3_bucket:
            raise ValueError("S3 storage requires a bucket")
        if self.s3_prefix.startswith("/") or ".." in self.s3_prefix.split("/"):
            raise ValueError("S3 prefix must be a relative object prefix")
        if self.s3_endpoint_url:
            from urllib.parse import urlsplit

            endpoint = urlsplit(self.s3_endpoint_url)
            if endpoint.scheme not in {"http", "https"} or not endpoint.hostname:
                raise ValueError("S3 endpoint must be an HTTP(S) URL")
            if self.production and endpoint.scheme != "https":
                raise ValueError("production S3 endpoints require HTTPS")
        if any(role not in {"viewer", "operator", "admin"} for role in self.api_keys.values()):
            raise ValueError("API key roles must be viewer, operator or admin")
        if any(len(key) < 24 for key in self.api_keys):
            raise ValueError("API keys must contain at least 24 characters")
        if bool(self.tls_cert) != bool(self.tls_key):
            raise ValueError("TLS certificate and key must be configured together")
        if self.production and (
            not (self.api_keys or self.identity_enabled)
            or len(self.worker_token) < 32
            or self.worker_token == "local-development-token"
            or not self.tls_cert
        ):
            raise ValueError("production requires API keys, a strong worker token and RPC TLS")
        if self.heartbeat_interval >= min(self.worker_timeout, self.lease_seconds) / 2:
            raise ValueError("heartbeat interval must be less than half both liveness windows")
        if self.scheduling_policy not in {"least_loaded", "best_fit", "fifo"}:
            raise ValueError("unknown scheduling policy")
        if self.retry_base_seconds > self.retry_max_seconds:
            raise ValueError("retry base cannot exceed retry cap")
        return self
