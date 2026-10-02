import logging
import mimetypes
import os
import signal
import threading
import time
from dataclasses import dataclass, field
from typing import Any

import grpc
from opentelemetry.propagate import extract

from control_plane.config import Settings
from control_plane.logging import configure_logging
from control_plane.rpc import engine_pb2 as pb
from control_plane.tracing import configure_tracing, tracer
from worker.executor import DockerExecutor, Result
from worker.resources import available_cpu, available_memory_mb
from worker.transport import Transport

logger = logging.getLogger(__name__)


@dataclass
class Running:
    assignment: Any
    container: Any
    started: float
    lease_deadline: float
    result: Result | None = None
    fenced: bool = False
    terminating: bool = False
    lock: threading.RLock = field(default_factory=threading.RLock, repr=False)


class Agent:
    def __init__(
        self,
        transport: Transport,
        executor: DockerExecutor,
        worker_id: str,
        cpu: float,
        memory: int,
        settings: Settings,
    ) -> None:
        self.transport = transport
        self.executor = executor
        self.worker_id = worker_id
        self.cpu = cpu
        self.memory = memory
        self.settings = settings
        self.running: dict[str, Running] = {}
        self.lock = threading.RLock()
        self.stopped = False
        self.next_heartbeat = 0.0
        self.heartbeat_interval = settings.heartbeat_interval

    def register(self) -> None:
        reply = self.transport.call(
            "Register",
            pb.RegisterRequest(
                worker_id=self.worker_id,
                cpu_total=self.cpu,
                memory_total_mb=self.memory,
                capabilities=["python", "cpp", "worker-python", f"node:{self.worker_id}"],
            ),
        )
        # A rejected duplicate ID must never stop the live owner's containers.
        self.executor.cleanup_orphans(self.worker_id)
        self.transport.session_id = reply.session_id
        self.heartbeat_interval = reply.heartbeat_interval
        self.executor.grace = reply.termination_grace_seconds

    def heartbeat(self) -> None:
        before = time.monotonic()
        reply = self.transport.call(
            "Heartbeat",
            pb.HeartbeatRequest(
                worker_id=self.worker_id,
                session_id=self.transport.session_id,
                # Capacity accounting remains authoritative in the control plane.
                cpu_available=available_cpu(
                    self.cpu, sum(r.assignment.cpu for r in self.snapshot())
                ),
                memory_available_mb=available_memory_mb(self.memory),
                leases=[
                    pb.Lease(
                        attempt_id=r.assignment.attempt_id, lease_token=r.assignment.lease_token
                    )
                    for r in self.snapshot()
                    if not r.fenced
                ],
            ),
        )
        for command in reply.commands:
            running = self.running.get(command.attempt_id)
            if running is None:
                continue
            if command.valid:
                running.lease_deadline = before + command.lease_seconds
            else:
                running.fenced = True
            if command.cancel:
                self.executor.stop(running.container)
                running.result = Result("CANCELLED", 137, "cancellation requested")
        self.next_heartbeat = before + self.heartbeat_interval

    def snapshot(self) -> list[Running]:
        with self.lock:
            return list(self.running.values())

    def launch(self, assignment: Any, poll_started: float | None = None) -> None:
        if assignment.attempt_id in self.running:
            return
        before = poll_started if poll_started is not None else time.monotonic()
        try:
            container = self.executor.create(assignment, self.worker_id)
        except Exception:
            self.launch_failed(assignment, "container create failed")
            logger.exception("container_create_failed", extra={"job_id": assignment.job_id})
            return
        running = Running(assignment, container, before, before + assignment.lease_seconds)
        with self.lock:
            self.running[assignment.attempt_id] = running
        try:
            self.transport.call("Start", self.transport.credentials(assignment))
            with running.lock:
                if (
                    time.monotonic() >= running.lease_deadline
                    or running.fenced
                    or running.terminating
                    or running.result is not None
                ):
                    raise RuntimeError("lease or execution deadline expired before container start")
                container.start()
                running.started = time.monotonic()
        except Exception:
            running.fenced = True
            self.launch_failed(assignment, "container launch failed or acknowledgement lost")
            logger.exception("container_launch_failed", extra={"job_id": assignment.job_id})
            return
        logger.info(
            "job_started",
            extra={
                "job_id": assignment.job_id,
                "worker_id": self.worker_id,
                "attempt_id": assignment.attempt_id,
            },
        )

    def launch_failed(self, assignment: Any, reason: str) -> None:
        try:
            self.transport.call(
                "Complete",
                pb.CompleteRequest(
                    credentials=self.transport.credentials(assignment),
                    outcome=pb.FAILED,
                    exit_code=-1,
                    reason=reason,
                ),
            )
        except grpc.RpcError:
            logger.warning("launch_failure_report_unavailable", extra={"job_id": assignment.job_id})

    def report(self, running: Running) -> None:
        assignment = running.assignment
        credentials = self.transport.credentials(assignment)
        with tracer.start_as_current_span(
            "worker.report", context=extract({"traceparent": assignment.traceparent})
        ):
            raw = running.container.logs(tail=10000)
            content = raw[-self.settings.logs_max_bytes :].decode("utf-8", errors="replace")
            content = content.encode()[-self.settings.logs_max_bytes :].decode(
                "utf-8", errors="ignore"
            )
            self.transport.call("PutLogs", pb.LogsRequest(credentials=credentials, content=content))
            for name, data in self.executor.artifacts(
                running.container, self.settings.artifact_max_bytes
            ):
                if time.monotonic() >= self.next_heartbeat:
                    self.heartbeat()
                if running.fenced:
                    return
                self.transport.call(
                    "PutArtifact",
                    pb.ArtifactRequest(
                        credentials=credentials,
                        name=name,
                        content_type=mimetypes.guess_type(name)[0] or "application/octet-stream",
                        content=data,
                    ),
                )
            assert running.result is not None
            if running.fenced:
                return
            self.transport.call(
                "Complete",
                pb.CompleteRequest(
                    credentials=credentials,
                    outcome=pb.Outcome.Value(running.result.outcome),
                    exit_code=running.result.exit_code,
                    reason=running.result.reason,
                ),
            )

    def guard(self) -> None:
        # Fence containers before attempting network traffic; a partition never renews local time.
        for running in self.snapshot():
            terminate = False
            timed_out = False
            with running.lock:
                if not running.fenced and time.monotonic() >= running.lease_deadline:
                    running.fenced = True
                    terminate = True
                if (
                    running.result is None
                    and not running.fenced
                    and not running.terminating
                    and time.monotonic() - running.started >= running.assignment.timeout_seconds
                ):
                    running.terminating = True
                    terminate = timed_out = True
            if terminate:
                try:
                    self.executor.stop(running.container)
                except Exception:
                    with running.lock:
                        running.fenced = True
                        running.terminating = False
                    raise
            if timed_out:
                with running.lock:
                    running.result = Result("TIMED_OUT", 137, "execution deadline")
                    running.terminating = False

    def tick(self) -> None:
        self.guard()
        if time.monotonic() >= self.next_heartbeat:
            self.heartbeat()
        for running in self.snapshot():
            attempt_id = running.assignment.attempt_id
            if running.terminating:
                continue
            if running.result is None and not running.fenced:
                result = self.executor.inspect(running.container)
                with running.lock:
                    if running.result is None and not running.fenced and not running.terminating:
                        running.result = result
            if running.fenced or running.result is not None:
                if not running.fenced:
                    try:
                        self.report(running)
                    except grpc.RpcError as exc:
                        if exc.code() != grpc.StatusCode.FAILED_PRECONDITION:
                            raise
                try:
                    self.executor.remove(running.container)
                except Exception:
                    logger.exception(
                        "container_cleanup_failed", extra={"job_id": running.assignment.job_id}
                    )
                    continue
                with self.lock:
                    del self.running[attempt_id]
        poll_started = time.monotonic()
        reply = self.transport.call(
            "Poll", pb.PollRequest(worker_id=self.worker_id, session_id=self.transport.session_id)
        )
        for assignment in reply.assignments:
            self.launch(assignment, poll_started)

    def run(self) -> None:
        guard_stopped = threading.Event()

        def watchdog() -> None:
            while not guard_stopped.wait(0.1):
                try:
                    self.guard()
                except Exception:
                    logger.exception("worker_guard_failed")

        guardian = threading.Thread(target=watchdog, daemon=True, name="lease-watchdog")
        guardian.start()
        while not self.stopped:
            try:
                if not self.transport.session_id:
                    self.register()
                self.tick()
            except grpc.RpcError as exc:
                logger.warning("worker_rpc_failed: %s", exc.code())
                if exc.code() == grpc.StatusCode.FAILED_PRECONDITION:
                    for running in self.snapshot():
                        self.executor.stop(running.container)
                        self.executor.remove(running.container)
                    with self.lock:
                        self.running.clear()
                    self.transport.session_id = ""
            except Exception:
                logger.exception("worker_tick_failed")
            time.sleep(0.2)
        guard_stopped.set()
        guardian.join(timeout=self.executor.grace + 4)
        for running in self.snapshot():
            self.executor.stop(running.container)
            self.executor.remove(running.container)


def main() -> None:
    configure_logging()
    configure_tracing("strata-worker-python")
    config = Settings()
    agent = Agent(
        Transport.from_env(),
        DockerExecutor(config.allowed_images),
        os.getenv("STRATA_WORKER_ID", "worker-python-1"),
        float(os.getenv("STRATA_WORKER_CPU", "2")),
        int(os.getenv("STRATA_WORKER_MEMORY_MB", "1024")),
        config,
    )
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: setattr(agent, "stopped", True))
    agent.run()


if __name__ == "__main__":
    main()
