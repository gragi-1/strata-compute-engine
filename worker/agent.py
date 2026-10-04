import hashlib
import logging
import mimetypes
import os
import signal
import tarfile
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
from worker.cache import InputCache
from worker.executor import DockerExecutor, Result, start_container
from worker.resources import available_cpu, available_memory_mb
from worker.transport import Transport

logger = logging.getLogger(__name__)


@dataclass
class Pending:
    assignment: Any
    deadline: float
    valid: bool = True


@dataclass
class Running:
    assignment: Any
    container: Any
    started: float
    lease_deadline: float
    result: Result | None = None
    fenced: bool = False
    terminating: bool = False
    executing: bool = True
    last_logs_at: float = 0.0
    storage_lost: bool = False
    last_runtime_at: float = 0.0
    runtime_replied: bytes = b""
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
        self.cache = InputCache(settings, worker_id)
        self.running: dict[str, Running] = {}
        self.pending: dict[str, Pending] = {}
        self.launching = ""
        self.executor.progress = self.launch_progress
        self.lock = threading.RLock()
        self.stopped = False
        self.next_heartbeat = 0.0
        self.heartbeat_interval = settings.heartbeat_interval

    def register(self) -> None:
        devices = self.executor.discover_gpus(self.settings.gpu_discovery_image)
        reply = self.transport.call(
            "Register",
            pb.RegisterRequest(
                worker_id=self.worker_id,
                cpu_total=self.cpu,
                memory_total_mb=self.memory,
                capabilities=[
                    "python",
                    "cpp",
                    "dataset-inputs",
                    "worker-python",
                    "image-pinning",
                    "bounded-output",
                    "runtime-bridge",
                    f"node:{self.worker_id}",
                ],
                gpus=[
                    pb.GPUDevice(id=device.id, name=device.name, memory_mb=device.memory_mb)
                    for device in devices
                ],
            ),
        )
        # A rejected duplicate ID must never stop the live owner's containers.
        self.executor.cluster_id = reply.cluster_id
        self.executor.cleanup_orphans(self.worker_id)
        self.transport.session_id = reply.session_id
        self.heartbeat_interval = reply.heartbeat_interval
        self.executor.grace = reply.termination_grace_seconds

    def heartbeat(self) -> None:
        # The scheduler can commit further assignments while Docker is starting the
        # current batch. Discover them during the pump so they do not expire unseen.
        polled_at = time.monotonic()
        assignments = self.transport.call(
            "Poll", pb.PollRequest(worker_id=self.worker_id, session_id=self.transport.session_id)
        )
        for assignment in assignments.assignments:
            if assignment.attempt_id not in self.running:
                self.pending.setdefault(
                    assignment.attempt_id,
                    Pending(assignment, polled_at + assignment.lease_seconds),
                )
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
                ]
                + [
                    pb.Lease(
                        attempt_id=p.assignment.attempt_id, lease_token=p.assignment.lease_token
                    )
                    for p in self.pending.values()
                    if p.valid
                ],
            ),
        )
        for command in reply.commands:
            running = self.running.get(command.attempt_id)
            if running is None:
                pending = self.pending.get(command.attempt_id)
                if pending:
                    pending.valid = command.valid and not command.cancel
                    if pending.valid:
                        pending.deadline = before + command.lease_seconds
                continue
            if command.valid:
                running.lease_deadline = before + command.lease_seconds
                try:
                    self.executor.renew_storage(command.attempt_id)
                except Exception:
                    running.storage_lost = True
            else:
                running.fenced = True
            if command.cancel:
                if running.executing:
                    self.executor.stop(running.container)
                running.result = Result("CANCELLED", 137, "cancellation requested")
        self.next_heartbeat = before + self.heartbeat_interval

    def snapshot(self) -> list[Running]:
        with self.lock:
            return list(self.running.values())

    def launch_progress(self) -> None:
        pending = self.pending.get(self.launching)
        if pending and (not pending.valid or time.monotonic() >= pending.deadline):
            raise RuntimeError("pending assignment lost its lease")
        self.startup_heartbeat()
        if pending and (not pending.valid or time.monotonic() >= pending.deadline):
            raise RuntimeError("pending assignment was fenced or cancelled")

    def startup_heartbeat(self) -> None:
        if not self.transport.session_id or time.monotonic() < self.next_heartbeat:
            return
        try:
            self.heartbeat()
        except grpc.RpcError as exc:
            if exc.code() not in {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED}:
                raise
            # A temporary coordinator disconnect does not revoke an unexpired lease.
            # Never extend its local deadline without an authenticated renewal.
            self.next_heartbeat = time.monotonic() + min(1, self.heartbeat_interval)
            logger.warning("startup_heartbeat_unavailable", extra={"rpc_code": exc.code().name})

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
        pending = self.pending.pop(assignment.attempt_id, None)
        running = Running(
            assignment,
            container,
            before,
            pending.deadline if pending else before + assignment.lease_seconds,
            executing=False,
        )
        with self.lock:
            self.running[assignment.attempt_id] = running
        try:
            container.reload()
            resolved_image = container.attrs.get("Image", "")
            if not isinstance(resolved_image, str):
                resolved_image = ""
            if (
                assignment.expected_image_digest
                and resolved_image != assignment.expected_image_digest
            ):
                raise ValueError("resolved image does not match the pinned execution")
            if assignment.has_inputs:
                self.prepare_inputs(running)
            if getattr(assignment, "runtime_code", b""):
                self.executor.write_runtime(container, "runtime.py", assignment.runtime_code)
            credentials = self.transport.credentials(assignment)
            credentials.image_digest = resolved_image
            self.transport.call("Start", credentials)
            with running.lock:
                if (
                    time.monotonic() >= running.lease_deadline
                    or running.fenced
                    or running.terminating
                    or running.result is not None
                ):
                    raise RuntimeError("lease or execution deadline expired before container start")
                running.started = time.monotonic()
                running.executing = True

            def start_progress() -> None:
                with running.lock:
                    if (
                        running.fenced
                        or running.terminating
                        or running.result is not None
                        or time.monotonic() >= running.lease_deadline
                    ):
                        raise RuntimeError("workload start lost its lease or was cancelled")
                self.startup_heartbeat()
                with running.lock:
                    if (
                        running.fenced
                        or running.result is not None
                        or time.monotonic() >= running.lease_deadline
                    ):
                        raise RuntimeError("workload start was fenced or cancelled")

            start_container(container, start_progress)
        except Exception:
            with running.lock:
                decided = running.result is not None
                if not decided:
                    running.fenced = True
            if not decided:
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

    def prepare_inputs(self, running: Running) -> None:
        assignment = running.assignment
        credentials = self.transport.credentials(assignment)
        reply = self.transport.call("InputManifest", credentials)

        def progress() -> None:
            if running.fenced or time.monotonic() >= running.lease_deadline or running.result:
                raise RuntimeError("input staging lost its lease")
            if time.monotonic() >= self.next_heartbeat:
                self.heartbeat()

        for file in reply.files:

            def download(stream: Any, file: Any = file) -> None:
                digest, offset = hashlib.sha256(), 0
                while offset < file.size:
                    progress()
                    count = 0
                    for chunk in self.transport.call(
                        "ReadInput",
                        pb.ReadInputRequest(
                            credentials=credentials,
                            sha256=file.sha256,
                            offset=offset,
                            max_bytes=min(4 * 1024 * 1024, file.size - offset),
                        ),
                    ):
                        if offset + count + len(chunk.content) > file.size:
                            raise ValueError("input size mismatch")
                        stream.write(chunk.content)
                        digest.update(chunk.content)
                        count += len(chunk.content)
                    if not count or offset + count > file.size:
                        raise ValueError("input size mismatch")
                    offset += count
                if digest.hexdigest() != file.sha256:
                    raise ValueError("input SHA-256 mismatch")

            with self.cache.open(file.sha256, file.size, download, progress) as stream:
                progress()
                self.executor.stage(
                    assignment, self.worker_id, file.alias, file.name, stream, file.size, progress
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

    def publish_logs(self, running: Running) -> None:
        raw = running.container.logs(tail=10000)
        content = raw[-self.settings.logs_max_bytes :].decode("utf-8", errors="replace")
        content = content.encode()[-self.settings.logs_max_bytes :].decode("utf-8", errors="ignore")
        self.transport.call(
            "PutLogs",
            pb.LogsRequest(
                credentials=self.transport.credentials(running.assignment), content=content
            ),
        )
        running.last_logs_at = time.monotonic()

    def exchange_runtime(self, running: Running) -> None:
        if not getattr(running.assignment, "runtime_context", ""):
            return
        if time.monotonic() - running.last_runtime_at < 0.5:
            return
        running.last_runtime_at = time.monotonic()
        try:
            request = self.executor.read_runtime(running.container)
            if not request or request == running.runtime_replied:
                return
            reply = self.transport.call(
                "RuntimeExchange",
                pb.RuntimeRequest(
                    credentials=self.transport.credentials(running.assignment), message=request
                ),
            )
            if reply.message:
                self.executor.write_runtime(running.container, "reply.json", reply.message)
                running.runtime_replied = request
        except grpc.RpcError as exc:
            if exc.code() not in {
                grpc.StatusCode.INVALID_ARGUMENT,
                grpc.StatusCode.RESOURCE_EXHAUSTED,
                grpc.StatusCode.FAILED_PRECONDITION,
            }:
                raise
            self.executor.stop(running.container)
            running.executing = False
            running.result = Result("FAILED", 1, "runtime protocol rejected")
        except (ValueError, tarfile.TarError):
            self.executor.stop(running.container)
            running.executing = False
            running.result = Result("FAILED", 1, "invalid runtime message")

    def report(self, running: Running) -> None:
        assignment = running.assignment
        credentials = self.transport.credentials(assignment)
        with tracer.start_as_current_span(
            "worker.report", context=extract({"traceparent": assignment.traceparent})
        ):
            self.publish_logs(running)
            outputs = []
            if not running.storage_lost:
                try:
                    outputs = list(
                        self.executor.artifacts(running.container, self.settings.artifact_max_bytes)
                    )
                except (ValueError, tarfile.TarError):
                    running.result = Result("FAILED", 1, "output exceeds artifact transfer limit")
                if not self.executor.storage_alive(assignment.attempt_id):
                    running.storage_lost = True
                    running.result = Result("FAILED", 137, "bounded output storage lost")
                    outputs = []
            for name, data in outputs:
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
                    and running.executing
                    and not running.fenced
                    and not running.terminating
                    and time.monotonic() - running.started >= running.assignment.timeout_seconds
                ):
                    running.terminating = True
                    terminate = timed_out = True
            if terminate and running.executing:
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
        for attempt_id, pending in list(self.pending.items()):
            if not pending.valid:
                del self.pending[attempt_id]
        self.guard()
        if time.monotonic() >= self.next_heartbeat:
            self.heartbeat()
        for running in self.snapshot():
            attempt_id = running.assignment.attempt_id
            if running.terminating:
                continue
            if not running.fenced and not self.executor.storage_alive(attempt_id):
                running.storage_lost = True
            if running.storage_lost and not running.fenced:
                if running.executing:
                    self.executor.stop(running.container)
                    running.executing = False
                running.result = Result("FAILED", 137, "bounded output storage lost")
            if running.result is None and not running.fenced:
                self.exchange_runtime(running)
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
            elif running.executing and time.monotonic() - running.last_logs_at >= 2:
                try:
                    self.publish_logs(running)
                except grpc.RpcError as exc:
                    if exc.code() == grpc.StatusCode.FAILED_PRECONDITION:
                        with running.lock:
                            running.fenced = True
                    else:
                        raise
        poll_started = time.monotonic()
        reply = self.transport.call(
            "Poll", pb.PollRequest(worker_id=self.worker_id, session_id=self.transport.session_id)
        )
        for assignment in reply.assignments:
            if assignment.attempt_id not in self.running:
                self.pending[assignment.attempt_id] = Pending(
                    assignment, poll_started + assignment.lease_seconds
                )
        if reply.assignments:
            # Scheduling can precede this poll by almost a lease. Renew the complete
            # received batch before any Docker call, even after a recent heartbeat.
            self.heartbeat()
        for assignment in reply.assignments:
            self.launching = assignment.attempt_id
            try:
                self.launch_progress()
                self.launch(assignment, poll_started)
            finally:
                self.pending.pop(assignment.attempt_id, None)
                self.launching = ""

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
                    self.pending.clear()
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
        DockerExecutor(config.allowed_images, settings=config),
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
