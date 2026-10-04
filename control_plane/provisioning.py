"""Bounded CPU worker pools on an explicitly reserved Docker host partition."""

import hashlib
import json
import logging
import math
import re
import signal
import threading
from datetime import timedelta
from pathlib import Path
from typing import Any, Literal

import docker
from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import Session, aliased

from control_plane.access import audit
from control_plane.config import Settings
from control_plane.database import make_engine, sessions
from control_plane.domain import ACTIVE, TERMINAL, WAITING
from control_plane.errors import DomainError
from control_plane.logging import configure_logging
from control_plane.models import (
    Admission,
    Attempt,
    Job,
    JobDependency,
    Project,
    ProvisionedWorker,
    Worker,
    WorkerPool,
    identifier,
)
from control_plane.operations import OperationsService
from control_plane.schemas import StrictModel
from control_plane.services import EngineService

logger = logging.getLogger(__name__)
LIVE_PHASES = ("REQUESTED", "STARTING", "ACTIVE", "DRAINING")


class PoolSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="STRATA_POOL_", extra="ignore")
    id: str = Field(pattern=r"^[a-zA-Z][a-zA-Z0-9_-]{0,47}$")
    image: str = Field(pattern=r"^(?:[a-zA-Z0-9_.:/-]+@)?sha256:[0-9a-f]{64}$", max_length=256)
    kind: Literal["python", "cpp"] = "python"
    rpc_target: str = Field(min_length=1, max_length=256)
    rpc_ca: Path | None = None
    network: str = Field(default="bridge", min_length=1, max_length=128)
    cpu_per_worker: float = Field(default=1, gt=0, le=256, allow_inf_nan=False)
    memory_per_worker_mb: int = Field(default=1024, ge=128, le=1048576)
    host_cpu_budget: float = Field(gt=0, le=4096, allow_inf_nan=False)
    host_memory_budget_mb: int = Field(ge=384, le=16777216)
    maximum_workers: int = Field(default=8, ge=1, le=32)
    idle_seconds: int = Field(default=120, ge=1, le=86400)
    startup_seconds: int = Field(default=90, ge=10, le=600)
    interval_seconds: float = Field(default=5, ge=0.2, le=300)
    cache_bytes: int = Field(default=128 * 1024**2, ge=1024**2, le=1024**3)

    @model_validator(mode="after")
    def reserved_partition(self) -> "PoolSettings":
        if self.hard_limit < 1:
            raise ValueError("host partition cannot fit a worker plus its agent overhead")
        return self

    @property
    def hard_limit(self) -> int:
        # Agent limits are separate from the resources advertised for workload containers.
        return min(
            self.maximum_workers,
            math.floor(self.host_cpu_budget / (self.cpu_per_worker + 0.25)),
            self.host_memory_budget_mb // (self.memory_per_worker_mb + 256),
        )


class PoolUpdate(StrictModel):
    enabled: bool
    minimum: int = Field(ge=0, le=32)
    maximum: int = Field(ge=0, le=32)

    @model_validator(mode="after")
    def ordered(self) -> "PoolUpdate":
        if self.minimum > self.maximum:
            raise ValueError("minimum must not exceed maximum")
        return self


def pool_view(row: WorkerPool) -> dict[str, Any]:
    return {
        column.name: getattr(row, column.name)
        for column in row.__table__.columns
        if column.name not in {"configuration_hash", "host_id"}
    }


class PoolService:
    def __init__(self, svc: EngineService) -> None:
        self.svc = svc

    def list_pools(self) -> list[dict[str, Any]]:
        with self.svc.factory() as session:
            return [
                pool_view(row)
                for row in session.scalars(select(WorkerPool).order_by(WorkerPool.id))
            ]

    def workers(self, pool_id: str) -> list[dict[str, Any]]:
        with self.svc.factory() as session:
            if session.get(WorkerPool, pool_id) is None:
                raise DomainError(404, "worker pool not found")
            return [
                {column.name: getattr(row, column.name) for column in row.__table__.columns}
                for row in session.scalars(
                    select(ProvisionedWorker)
                    .where(ProvisionedWorker.pool_id == pool_id)
                    .order_by(ProvisionedWorker.created_at.desc())
                    .limit(100)
                )
            ]

    def update(self, pool_id: str, body: PoolUpdate) -> dict[str, Any]:
        with self.svc.factory.begin() as session:
            row = session.scalar(
                select(WorkerPool).where(WorkerPool.id == pool_id).with_for_update()
            )
            if row is None:
                raise DomainError(404, "worker pool not found")
            if body.maximum > row.hard_limit:
                raise DomainError(422, "maximum exceeds the host's configured hard capacity")
            changes = body.model_dump()
            if any(getattr(row, key) != value for key, value in changes.items()):
                for key, value in changes.items():
                    setattr(row, key, value)
                audit(session, self.svc.now(session), "WORKER_POOL_UPDATED", pool_id, **changes)
            return pool_view(row)


class DockerPoolAdapter:
    def __init__(self, svc: EngineService, config: PoolSettings, client: Any = None) -> None:
        self.svc, self.config = svc, config
        if svc.settings.production and not re.fullmatch(
            r"(?:[a-zA-Z0-9_.:/-]+@)?sha256:[0-9a-f]{64}", svc.settings.storage_keeper_image
        ):
            raise ValueError("production pools require an immutable output keeper image")
        self.client = client or docker.from_env(timeout=10)
        info = self.client.info()
        if (
            config.host_cpu_budget > info["NCPU"]
            or config.host_memory_budget_mb * 1024**2 > info["MemTotal"]
        ):
            raise ValueError("reserved partition exceeds the Docker host's physical capacity")
        if svc.settings.production and (not config.rpc_ca or config.network == "host"):
            raise ValueError(
                "production pools require a trusted RPC CA and a private Docker network"
            )
        self.host_id = info["ID"]
        self.image = self.client.images.get(config.image).id
        self.keeper_image = self.client.images.get(svc.settings.storage_keeper_image).id
        with svc.factory() as session:
            self.cluster_id = session.execute(
                select(Admission.cluster_id).where(Admission.id == 1)
            ).scalar_one()

    def labels(self, worker_id: str) -> dict[str, str]:
        return {
            "strata.cluster": self.cluster_id,
            "strata.pool": self.config.id,
            "strata.provisioned-worker": worker_id,
        }

    def name(self, worker_id: str) -> str:
        return "strata-pool-" + worker_id

    def container(self, worker_id: str) -> Any:
        try:
            item = self.client.containers.get(self.name(worker_id))
        except docker.errors.NotFound:
            return None
        if any(item.labels.get(key) != value for key, value in self.labels(worker_id).items()):
            raise ValueError("container ownership does not match the provisioning intent")
        if item.image.id != self.image:
            raise ValueError("container image does not match the immutable pool configuration")
        return item

    def ensure(self, worker_id: str) -> Any:
        existing = self.container(worker_id)
        if existing is not None:
            if existing.status == "created":
                existing.start()
            return existing
        config = self.config
        name = "strata-pool-cache-" + worker_id
        labels = self.labels(worker_id)
        try:
            cache = self.client.volumes.get(name)
            if any(
                cache.attrs.get("Labels", {}).get(key) != value for key, value in labels.items()
            ):
                raise ValueError("cache ownership does not match the provisioning intent")
        except docker.errors.NotFound:
            self.client.volumes.create(name=name, labels=labels)
        volumes = {
            "/var/run/docker.sock": {"bind": "/var/run/docker.sock", "mode": "rw"},
            name: {"bind": "/var/lib/strata/input-cache", "mode": "rw"},
        }
        environment = {
            "STRATA_RPC_TARGET": config.rpc_target,
            "STRATA_WORKER_TOKEN": self.svc.settings.worker_token,
            "STRATA_WORKER_ID": worker_id,
            "STRATA_WORKER_CPU": str(config.cpu_per_worker),
            "STRATA_WORKER_MEMORY_MB": str(config.memory_per_worker_mb),
            "STRATA_ALLOWED_IMAGES": json.dumps(self.svc.settings.allowed_images),
            "STRATA_WORKER_CACHE_ROOT": "/var/lib/strata/input-cache",
            "STRATA_WORKER_CACHE_BYTES": str(config.cache_bytes),
            "STRATA_STORAGE_KEEPER_IMAGE": self.keeper_image,
            "STRATA_WORKER_OUTPUT_BYTES": str(self.svc.settings.worker_output_bytes),
            "STRATA_WORKER_OUTPUT_INODES": str(self.svc.settings.worker_output_inodes),
        }
        if config.rpc_ca:
            volumes[str(config.rpc_ca)] = {"bind": "/run/strata/ca.pem", "mode": "ro"}
            environment["STRATA_RPC_CA"] = "/run/strata/ca.pem"
        item = self.client.containers.create(
            self.image,
            entrypoint=[
                "strata-worker" if config.kind == "python" else "/usr/local/bin/strata-worker-cpp"
            ],
            name=self.name(worker_id),
            environment=environment,
            labels=labels,
            network_mode=config.network,
            volumes=volumes,
            user="root",
            read_only=True,
            cap_drop=["ALL"],
            security_opt=["no-new-privileges:true"],
            nano_cpus=250000000,
            mem_limit="256m",
            memswap_limit="256m",
            pids_limit=128,
            tmpfs={"/tmp": "rw,nosuid,nodev,size=64m"},
            log_config=docker.types.LogConfig(
                type="json-file", config={"max-size": "1m", "max-file": "1"}
            ),
        )
        item.start()
        return item

    def remove(self, worker_id: str) -> None:
        item = self.container(worker_id)
        if item is not None:
            item.remove(force=True)
        try:
            cache = self.client.volumes.get("strata-pool-cache-" + worker_id)
        except docker.errors.NotFound:
            return
        if any(
            cache.attrs.get("Labels", {}).get(key) != value
            for key, value in self.labels(worker_id).items()
        ):
            raise ValueError("cache ownership does not match the provisioning intent")
        cache.remove()


class PoolController:
    def __init__(
        self, svc: EngineService, config: PoolSettings, adapter: DockerPoolAdapter
    ) -> None:
        self.svc, self.config, self.adapter = svc, config, adapter

    def register(self, session: Session) -> WorkerPool:
        spec = self.config.model_dump(
            mode="json", exclude={"interval_seconds", "idle_seconds", "startup_seconds"}
        )
        spec.update(
            host_id=self.adapter.host_id,
            image=self.adapter.image,
            allowed_images=sorted(self.svc.settings.allowed_images),
            storage_keeper_image=self.adapter.keeper_image,
            worker_output_bytes=self.svc.settings.worker_output_bytes,
            worker_output_inodes=self.svc.settings.worker_output_inodes,
        )
        digest = hashlib.sha256(json.dumps(spec, sort_keys=True).encode()).hexdigest()
        row = session.scalar(
            select(WorkerPool).where(WorkerPool.id == self.config.id).with_for_update()
        )
        if row is None or row.configuration_hash != digest:
            if row and session.scalar(
                select(ProvisionedWorker.worker_id)
                .where(
                    ProvisionedWorker.pool_id == row.id, ProvisionedWorker.phase.in_(LIVE_PHASES)
                )
                .limit(1)
            ):
                raise DomainError(
                    409, "drain all pool workers before changing the host configuration"
                )
            if session.scalar(
                select(WorkerPool.id).where(
                    WorkerPool.host_id == self.adapter.host_id, WorkerPool.id != self.config.id
                )
            ):
                raise DomainError(409, "this Docker host is already assigned to another pool")
            if row is None:
                row = WorkerPool(id=self.config.id)
                session.add(row)
            row.configuration_hash, row.host_id = digest, self.adapter.host_id
            row.image, row.kind = self.adapter.image, self.config.kind
            row.cpu_per_worker, row.memory_per_worker_mb = (
                self.config.cpu_per_worker,
                self.config.memory_per_worker_mb,
            )
            row.host_cpu_budget, row.host_memory_budget_mb = (
                self.config.host_cpu_budget,
                self.config.host_memory_budget_mb,
            )
            row.hard_limit = self.config.hard_limit
            row.minimum, row.maximum, row.enabled, row.desired = 0, row.hard_limit, False, 0
        row.last_seen_at = self.svc.now(session)
        return row

    def desired(self, session: Session, pool: WorkerPool, records: list[ProvisionedWorker]) -> int:
        if not pool.enabled:
            return 0
        workers = {
            row.id: row
            for row in session.scalars(
                select(Worker).where(Worker.id.in_([row.worker_id for row in records]))
            )
        }
        busy = [worker for worker in workers.values() if worker.running_jobs > 0]
        bins = [
            [
                max(0, self.config.cpu_per_worker - worker.cpu_reserved),
                max(0, self.config.memory_per_worker_mb - worker.memory_reserved_mb),
            ]
            for worker in busy
        ]
        now = self.svc.now(session)
        parent = aliased(Job)
        dependencies = select(JobDependency.job_id).join(
            parent, parent.id == JobDependency.parent_id
        )
        unfinished = dependencies.where(parent.status.not_in(TERMINAL))
        unsuccessful = dependencies.where(parent.status != "SUCCEEDED")
        failed = dependencies.where(parent.status.in_(["FAILED", "TIMED_OUT", "CANCELLED"]))
        # A bounded packing estimate; only the scheduler can reserve jobs and project budgets.
        queued = list(
            session.scalars(
                select(Job)
                .outerjoin(Project, Project.id == Job.project_id)
                .where(
                    Job.status.in_(WAITING),
                    Job.eligible_at <= now,
                    Job.execution_kind.in_(["container", "collective"]),
                    Job.gpu_required == 0,
                    Job.id.not_in(unfinished),
                    or_(
                        and_(Job.dependency_policy == "all_succeeded", Job.id.not_in(unsuccessful)),
                        Job.dependency_policy == "all_terminal",
                        and_(Job.dependency_policy == "any_failed", Job.id.in_(failed)),
                    ),
                    (Job.project_id.is_(None) | Project.enabled.is_(True)),
                )
                .order_by(Job.priority.desc(), Job.created_at, Job.id)
                .limit(1000)
            )
        )
        supported = {
            "python",
            "cpp",
            "dataset-inputs",
            "image-pinning",
            "bounded-output",
            "runtime-bridge",
            "worker-" + self.config.kind,
        }
        projects = {row.id: row for row in session.scalars(select(Project))}
        from control_plane.models import ComputeGroup, GroupMember

        group_members = {
            job_id: group
            for job_id, group in session.execute(
                select(GroupMember.job_id, ComputeGroup)
                .join(ComputeGroup)
                .where(GroupMember.job_id.in_([j.id for j in queued]))
            )
        }
        group_bins: dict[str, set[int]] = {}
        usage = {
            key: [float(cpu), int(memory)]
            for key, cpu, memory in session.execute(
                select(Job.project_id, func.sum(Job.cpu_required), func.sum(Job.memory_required_mb))
                .where(Job.status.in_(ACTIVE), Job.project_id.is_not(None))
                .group_by(Job.project_id)
            )
        }
        for job in queued:
            if (
                job.cpu_required + 0.01 > self.config.cpu_per_worker + 1e-9
                or job.memory_required_mb + 32 > self.config.memory_per_worker_mb
                or job.image not in self.svc.settings.allowed_images
                or not set(job.capabilities).issubset(supported)
            ):
                continue
            project = projects.get(job.project_id or "")
            used = usage.setdefault(job.project_id, [0.0, 0])
            group = group_members.get(job.id)
            if group and (group.status != "QUEUED" or group.nodes > pool.maximum):
                continue
            if (
                group
                and project
                and (
                    job.cpu_required * group.nodes > project.cpu_limit
                    or job.memory_required_mb * group.nodes > project.memory_limit_mb
                )
            ):
                continue
            if project and (
                used[0] + job.cpu_required > project.cpu_limit
                or used[1] + job.memory_required_mb > project.memory_limit_mb
            ):
                continue
            destination = next(
                (
                    capacity
                    for index, capacity in enumerate(bins)
                    if capacity[0] + 1e-9 >= job.cpu_required + 0.01
                    and capacity[1] >= job.memory_required_mb + 32
                    and (not group or index not in group_bins.get(group.id, set()))
                ),
                None,
            )
            if destination is None:
                if len(bins) >= pool.maximum:
                    break
                destination = [self.config.cpu_per_worker, self.config.memory_per_worker_mb]
                bins.append(destination)
            if group:
                index = next(i for i, capacity in enumerate(bins) if capacity is destination)
                group_bins.setdefault(group.id, set()).add(index)
            destination[0] -= job.cpu_required + 0.01
            destination[1] -= job.memory_required_mb + 32
            used[0] += job.cpu_required
            used[1] += job.memory_required_mb
        return min(pool.maximum, max(pool.minimum, len(bins)))

    def tick(self) -> dict[str, Any]:
        with OperationsService(self.svc).guard("worker-pool-" + self.config.id) as owned:
            if not owned:
                return {"leader": False}
            with self.svc.factory.begin() as session:
                state = session.execute(
                    select(Admission).where(Admission.id == 1).with_for_update(read=True)
                ).scalar_one()
                pool = self.register(session)
                records = list(
                    session.scalars(
                        select(ProvisionedWorker)
                        .where(
                            ProvisionedWorker.pool_id == pool.id,
                            ProvisionedWorker.phase.in_(LIVE_PHASES),
                        )
                        .order_by(ProvisionedWorker.created_at)
                    )
                )
                desired = self.desired(session, pool, records)
                pool.desired = desired
                pool.last_error = None
                now = self.svc.now(session)
                if state.scheduling_enabled and pool.enabled:
                    # Durable intent precedes every external create; draining slots remain charged.
                    for _ in range(min(2, max(0, desired - len(records)))):
                        row = ProvisionedWorker(
                            worker_id="elastic-" + identifier(),
                            pool_id=pool.id,
                            phase="REQUESTED",
                            created_at=now,
                            next_check_at=now,
                        )
                        session.add(row)
                        records.append(row)
                        audit(
                            session,
                            now,
                            "WORKER_PROVISION_REQUESTED",
                            row.worker_id,
                            pool_id=pool.id,
                        )
                ids = [row.worker_id for row in records]
            errors = 0
            for worker_id in ids:
                try:
                    self.reconcile(worker_id, desired)
                except Exception as exc:
                    errors += 1
                    with self.svc.factory.begin() as session:
                        failed_intent = session.get(ProvisionedWorker, worker_id)
                        if failed_intent:
                            failed_intent.last_error = type(exc).__name__[:128]
                            failed_intent.next_check_at = self.svc.now(session) + timedelta(
                                seconds=30
                            )
                        failed_pool = session.get(WorkerPool, self.config.id)
                        assert failed_pool is not None
                        failed_pool.last_error = type(exc).__name__[:128]
                    logger.warning("pool_reconcile_failed: %s", type(exc).__name__)
            return {"leader": True, "desired": desired, "errors": errors}

    def reconcile(self, worker_id: str, desired: int) -> None:
        removing = False
        with self.svc.factory.begin() as session:
            admission = session.execute(
                select(Admission).where(Admission.id == 1).with_for_update(read=True)
            ).scalar_one()
            pool = session.execute(
                select(WorkerPool).where(WorkerPool.id == self.config.id).with_for_update()
            ).scalar_one()
            worker = session.scalar(select(Worker).where(Worker.id == worker_id).with_for_update())
            row = session.get(ProvisionedWorker, worker_id)
            assert row is not None
            now = self.svc.now(session)
            if row.phase == "REMOVED" or row.next_check_at > now:
                return
            active = (
                session.scalar(
                    select(func.count())
                    .select_from(Attempt)
                    .where(Attempt.worker_id == worker_id, Attempt.status.in_(ACTIVE))
                )
                or 0
            )
            if active:
                row.idle_since = None
            elif row.idle_since is None:
                row.idle_since = now
            ready = (
                worker is not None
                and worker.status in {"HEALTHY", "DRAINING"}
                and (now - worker.last_heartbeat).total_seconds() < self.svc.settings.worker_timeout
            )
            slots = (
                session.scalar(
                    select(func.count())
                    .select_from(ProvisionedWorker)
                    .where(
                        ProvisionedWorker.pool_id == pool.id,
                        ProvisionedWorker.phase.in_(LIVE_PHASES),
                        ProvisionedWorker.phase != "DRAINING",
                    )
                )
                or 0
            )
            retire = (
                not pool.enabled
                or slots > pool.maximum
                or slots > desired
                and not active
                and row.idle_since is not None
                and (now - row.idle_since).total_seconds() >= self.config.idle_seconds
            )
            expired_start = (
                not ready and (now - row.created_at).total_seconds() >= self.config.startup_seconds
            )
            if retire or expired_start:
                row.phase = "DRAINING"
            if row.phase == "DRAINING":
                if worker is not None:
                    worker.status = "DRAINING"
                if active:
                    return
                removing = True
            elif row.phase in {"REQUESTED", "STARTING"}:
                if not admission.scheduling_enabled:
                    return
                item = self.adapter.ensure(worker_id)
                if item.status not in {"created", "running"}:
                    row.phase = "DRAINING"
                    return
                row.container_id = item.id
                row.phase = "ACTIVE" if ready else "STARTING"
                row.last_error = None
            elif row.phase == "ACTIVE":
                item = self.adapter.container(worker_id)
                if item is None or item.status != "running" or not ready:
                    row.phase = "DRAINING"
                    if worker is not None:
                        worker.status = "DRAINING"
        if removing:
            self.remove_drained(worker_id)

    def remove_drained(self, worker_id: str) -> None:
        # DRAINING was committed before external removal. A crash or Docker failure cannot
        # reopen a worker to scheduling or lose the still-charged provisioning intent.
        with self.svc.factory.begin() as session:
            session.execute(
                select(Admission).where(Admission.id == 1).with_for_update(read=True)
            ).scalar_one()
            worker = session.scalar(select(Worker).where(Worker.id == worker_id).with_for_update())
            row = session.get(ProvisionedWorker, worker_id)
            if row is None or row.phase != "DRAINING":
                return
            active = (
                session.scalar(
                    select(func.count())
                    .select_from(Attempt)
                    .where(Attempt.worker_id == worker_id, Attempt.status.in_(ACTIVE))
                )
                or 0
            )
            if active:
                return
            self.adapter.remove(worker_id)
            if worker is not None:
                worker.status = "LOST"
            now = self.svc.now(session)
            row.phase, row.removed_at, row.last_error = "REMOVED", now, None
            audit(session, now, "WORKER_PROVISION_REMOVED", worker_id, pool_id=row.pool_id)


def main() -> None:
    configure_logging()
    settings = Settings()
    config = PoolSettings()  # type: ignore[call-arg]  # Required fields come from deployment env.
    engine = make_engine(settings.database_url)
    svc = EngineService(sessions(engine), settings)
    controller = PoolController(svc, config, DockerPoolAdapter(svc, config))
    stopped = threading.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stopped.set())
    try:
        while not stopped.is_set():
            try:
                controller.tick()
            except Exception as exc:
                logger.warning("pool_tick_failed: %s", type(exc).__name__)
            stopped.wait(config.interval_seconds)
    finally:
        controller.adapter.client.close()
        engine.dispose()


if __name__ == "__main__":
    main()
