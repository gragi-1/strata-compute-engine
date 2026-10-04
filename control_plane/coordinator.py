"""A supervised API/RPC/scheduler replica with shared durable state and no local job authority."""

import logging
import signal
import threading
from time import monotonic

import uvicorn
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError

from control_plane.api import create_app
from control_plane.config import Settings
from control_plane.database import make_engine, sessions
from control_plane.logging import configure_logging, database_failure
from control_plane.periodic import PeriodicService
from control_plane.rpc.server import make_server
from control_plane.services import EngineService
from control_plane.tracing import configure_tracing
from control_plane.workflows import WorkflowService
from scheduler.core import Scheduler

logger = logging.getLogger(__name__)


class ReplicaSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="STRATA_", extra="ignore")
    replica_id: str = Field(default="coordinator", pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
    api_bind: str = "127.0.0.1"
    api_port: int = Field(default=8000, ge=1, le=65535)
    rpc_bind: str = "127.0.0.1:50051"
    shutdown_seconds: int = Field(default=10, ge=1, le=60)


class Coordinator:
    def __init__(self, settings: Settings, replica: ReplicaSettings) -> None:
        if settings.production:
            address = make_url(settings.database_url)
            if (
                address.get_backend_name() != "postgresql"
                or address.query.get("sslmode") != "verify-full"
            ):
                raise ValueError("production replicas require PostgreSQL with sslmode=verify-full")
            if settings.storage_backend != "s3":
                raise ValueError("production replicas require shared S3 storage")
        self.settings, self.replica = settings, replica
        self.engine = make_engine(settings.database_url)
        self.svc = EngineService(sessions(self.engine), settings)
        self.stopped = threading.Event()
        self.last_scheduled = 0.0
        self.rpc_live = threading.Event()
        app = create_app(settings, self.svc)
        app.state.replica_id = replica.replica_id
        app.state.replica_ready = self.ready
        self.web = uvicorn.Server(
            uvicorn.Config(
                app,
                host=replica.api_bind,
                port=replica.api_port,
                access_log=False,
                timeout_graceful_shutdown=replica.shutdown_seconds,
            )
        )
        try:
            self.rpc = make_server(self.svc, replica.rpc_bind)
        except BaseException:
            self.engine.dispose()
            raise
        self.threads = [
            threading.Thread(target=self.web.run, name="replica-api", daemon=True),
            threading.Thread(target=self.schedule, name="replica-scheduler", daemon=True),
            threading.Thread(target=self.watch_rpc, name="replica-rpc", daemon=True),
        ]

    def ready(self) -> bool:
        return (
            not self.stopped.is_set()
            and self.web.started
            and self.rpc_live.is_set()
            and all(thread.is_alive() for thread in self.threads)
            and monotonic() - self.last_scheduled < max(10, self.settings.scheduler_interval * 5)
        )

    def watch_rpc(self) -> None:
        self.rpc_live.set()
        try:
            self.rpc.wait_for_termination()
        finally:
            self.rpc_live.clear()

    def schedule(self) -> None:
        scheduler, periodic, workflows = (
            Scheduler(self.svc),
            PeriodicService(self.svc),
            WorkflowService(self.svc),
        )
        while not self.stopped.is_set():
            try:
                scheduler.tick()
                periodic.tick()
                workflows.tick()
                self.last_scheduled = monotonic()
            except SQLAlchemyError as exc:
                logger.warning("replica_store_failed: %s", database_failure(exc))
            except Exception as exc:
                # No submitted commands, SQL parameters or connection strings in daemon errors.
                logger.error("replica_scheduler_failed: %s", type(exc).__name__)
            self.stopped.wait(self.settings.scheduler_interval)

    def run(self) -> None:
        try:
            self.rpc.start()
            for thread in self.threads:
                thread.start()
            logger.info("coordinator_started", extra={"replica_id": self.replica.replica_id})
            while not self.stopped.wait(0.2):
                if any(not thread.is_alive() for thread in self.threads):
                    raise RuntimeError("coordinator component stopped unexpectedly")
        finally:
            # Withdraw readiness first; existing workers remain governed by their leases.
            self.stopped.set()
            self.web.should_exit = True
            self.rpc.stop(self.replica.shutdown_seconds).wait()
            for thread in self.threads:
                if thread.ident is not None:
                    thread.join(timeout=self.replica.shutdown_seconds)
            self.engine.dispose()
            logger.info("coordinator_stopped")


def main() -> None:
    configure_logging()
    configure_tracing("strata-coordinator")
    node = Coordinator(Settings(), ReplicaSettings())
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: node.stopped.set())
    node.run()


if __name__ == "__main__":
    main()
