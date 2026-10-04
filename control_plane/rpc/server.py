import logging
import os
import secrets
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from typing import Any, TypeVar

import grpc
from opentelemetry.propagate import extract
from pydantic import ValidationError
from sqlalchemy.exc import SQLAlchemyError

from control_plane.config import Settings
from control_plane.database import make_engine, sessions
from control_plane.domain import ACTIVE, JobStatus
from control_plane.logging import configure_logging, database_failure
from control_plane.models import Admission, Attempt
from control_plane.rpc import engine_pb2 as pb
from control_plane.rpc import engine_pb2_grpc as rpc
from control_plane.schemas import Completion, GPURegistration, Heartbeat, LeaseRef, WorkerRegister
from control_plane.services import DomainError, EngineService
from control_plane.tracing import configure_tracing, tracer

T = TypeVar("T")
ERRORS = {
    401: grpc.StatusCode.UNAUTHENTICATED,
    404: grpc.StatusCode.NOT_FOUND,
    409: grpc.StatusCode.FAILED_PRECONDITION,
    413: grpc.StatusCode.RESOURCE_EXHAUSTED,
    422: grpc.StatusCode.INVALID_ARGUMENT,
    429: grpc.StatusCode.RESOURCE_EXHAUSTED,
    503: grpc.StatusCode.UNAVAILABLE,
}


class WorkerControl(rpc.WorkerControlServicer):  # type: ignore[misc]
    def __init__(self, service: EngineService) -> None:
        self.svc = service

    def cluster_id(self) -> str:
        with self.svc.factory() as session:
            admission = session.get(Admission, 1)
            if admission is None:
                raise DomainError(503, "cluster identity is unavailable")
            return admission.cluster_id

    def Cluster(self, request: Any, context: Any) -> Any:
        return self.invoke(
            context, "Cluster", lambda: pb.ClusterReply(cluster_id=self.cluster_id())
        )

    def InspectOrphans(self, request: Any, context: Any) -> Any:
        def action() -> Any:
            if not 1 <= len(request.candidates) <= 500:
                raise DomainError(422, "orphan inspection requires 1..500 candidates")
            decisions = []
            for candidate in request.candidates:
                if not 1 <= len(candidate.worker_id) <= 128 or len(candidate.attempt_id) != 36:
                    raise DomainError(422, "invalid orphan candidate")
                with self.svc.factory.begin() as session:
                    attempt = session.get(Attempt, candidate.attempt_id)
                    removable = True
                    if attempt is not None:
                        # Same lock order as lease renewal. Re-read after concurrent renewals.
                        job = self.svc.job(session, attempt.job_id, lock=True)
                        worker = self.svc.worker(session, attempt.worker_id, lock=True)
                        session.refresh(attempt)
                        now = self.svc.now(session)
                        removable = (
                            attempt.worker_id != candidate.worker_id
                            or attempt.status not in ACTIVE
                            or job.status not in ACTIVE
                            or attempt.worker_session != worker.session_id
                            or attempt.lease_expires_at <= now
                        )
                    decisions.append(
                        pb.OrphanDecision(
                            worker_id=candidate.worker_id,
                            attempt_id=candidate.attempt_id,
                            remove=removable,
                        )
                    )
            return pb.OrphanReply(decisions=decisions)

        return self.invoke(context, "InspectOrphans", action)

    def invoke(self, context: Any, name: str, action: Callable[[], T]) -> T:
        metadata = dict(context.invocation_metadata())
        if not secrets.compare_digest(
            metadata.get("authorization", ""), f"Bearer {self.svc.settings.worker_token}"
        ):
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "invalid worker token")
        try:
            with tracer.start_as_current_span(
                f"rpc.{name}",
                context=extract(metadata),
                record_exception=False,
                set_status_on_exception=False,
            ):
                return action()
        except DomainError as exc:
            context.abort(ERRORS.get(exc.code, grpc.StatusCode.INTERNAL), str(exc))
        except (ValidationError, ValueError) as exc:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        except SQLAlchemyError as exc:
            logging.getLogger(__name__).warning("rpc_store_failed: %s", database_failure(exc))
            context.abort(grpc.StatusCode.UNAVAILABLE, "durable store is temporarily unavailable")
        except Exception:
            logging.getLogger(__name__).exception("rpc_failed")
            context.abort(grpc.StatusCode.INTERNAL, "internal error")
        raise RuntimeError("RPC abort returned unexpectedly")

    def Register(self, request: Any, context: Any) -> Any:
        def action() -> Any:
            worker = self.svc.register(
                WorkerRegister(
                    worker_id=request.worker_id,
                    cpu_total=request.cpu_total,
                    memory_total_mb=request.memory_total_mb,
                    capabilities=list(request.capabilities),
                    gpus=[
                        GPURegistration(id=gpu.id, name=gpu.name, memory_mb=gpu.memory_mb)
                        for gpu in request.gpus
                    ],
                )
            )
            return pb.RegisterReply(
                session_id=worker.session_id,
                heartbeat_interval=self.svc.settings.heartbeat_interval,
                lease_seconds=self.svc.settings.lease_seconds,
                termination_grace_seconds=self.svc.settings.termination_grace_seconds,
                cluster_id=self.cluster_id(),
            )

        return self.invoke(context, "Register", action)

    def Heartbeat(self, request: Any, context: Any) -> Any:
        def action() -> Any:
            result = self.svc.heartbeat(
                request.worker_id,
                Heartbeat(
                    session_id=request.session_id,
                    cpu_available=request.cpu_available,
                    memory_available_mb=request.memory_available_mb,
                    leases=[
                        LeaseRef(attempt_id=r.attempt_id, lease_token=r.lease_token)
                        for r in request.leases
                    ],
                ),
            )
            return pb.HeartbeatReply(commands=[pb.LeaseCommand(**c) for c in result["commands"]])

        return self.invoke(context, "Heartbeat", action)

    def Poll(self, request: Any, context: Any) -> Any:
        return self.invoke(
            context,
            "Poll",
            lambda: pb.PollReply(
                assignments=[
                    pb.Assignment(**a)
                    for a in self.svc.assignments(request.worker_id, request.session_id)
                ]
            ),
        )

    def RuntimeExchange(self, request: Any, context: Any) -> Any:
        from control_plane.runtimes import RuntimeService

        def action() -> Any:
            c = request.credentials
            return pb.RuntimeReply(
                message=RuntimeService(self.svc).exchange(
                    c.attempt_id, c.session_id, c.lease_token, request.message
                )
            )

        return self.invoke(context, "RuntimeExchange", action)

    def Start(self, request: Any, context: Any) -> Any:
        def action() -> Any:
            self.svc.start(
                request.attempt_id, request.session_id, request.lease_token, request.image_digest
            )
            return pb.Empty()

        return self.invoke(context, "Start", action)

    def Complete(self, request: Any, context: Any) -> Any:
        def action() -> Any:
            c = request.credentials
            self.svc.complete(
                c.attempt_id,
                Completion(
                    session_id=c.session_id,
                    lease_token=c.lease_token,
                    outcome=JobStatus(pb.Outcome.Name(request.outcome)),
                    exit_code=request.exit_code,
                    reason=request.reason,
                ),
            )
            return pb.Empty()

        return self.invoke(context, "Complete", action)

    def PutLogs(self, request: Any, context: Any) -> Any:
        def action() -> Any:
            c = request.credentials
            self.svc.logs(c.attempt_id, c.session_id, c.lease_token, request.content)
            return pb.Empty()

        return self.invoke(context, "PutLogs", action)

    def PutArtifact(self, request: Any, context: Any) -> Any:
        def action() -> Any:
            c = request.credentials
            a = self.svc.artifact(
                c.attempt_id,
                c.session_id,
                c.lease_token,
                request.name,
                request.content_type,
                request.content,
            )
            return pb.ArtifactReply(id=a.id, sha256=a.sha256, size=a.size)

        return self.invoke(context, "PutArtifact", action)

    def InputManifest(self, request: Any, context: Any) -> Any:
        from control_plane.inputs import manifest

        return self.invoke(
            context,
            "InputManifest",
            lambda: pb.InputManifestReply(
                files=[pb.InputFile(**f) for f in manifest(self.svc, request)]
            ),
        )

    def ReadInput(self, request: Any, context: Any) -> Iterator[Any]:
        from contextlib import ExitStack

        from control_plane.inputs import open_input

        # Short bounded range streams leave leases renewable between transfers.
        with ExitStack() as pins:

            def action() -> tuple[Any, int]:
                if request.offset < 0 or not 1 <= request.max_bytes <= 4 * 1024 * 1024:
                    raise DomainError(422, "invalid input byte range")
                return pins.enter_context(open_input(self.svc, request.credentials, request.sha256))

            path, size = self.invoke(context, "ReadInput", action)
            if request.offset > size:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "offset exceeds file size")
            remaining = min(request.max_bytes, size - request.offset)
            with path.open("rb") as stream:
                stream.seek(request.offset)
                while remaining and context.is_active():
                    chunk = stream.read(min(65536, remaining))
                    if not chunk:
                        context.abort(grpc.StatusCode.DATA_LOSS, "input file is truncated")
                    remaining -= len(chunk)
                    yield pb.InputChunk(content=chunk)


def make_server(service: EngineService, address: str = "[::]:50051") -> Any:
    server = grpc.server(
        ThreadPoolExecutor(max_workers=16),
        maximum_concurrent_rpcs=64,
        options=[("grpc.max_receive_message_length", service.settings.artifact_max_bytes + 65536)],
    )
    rpc.add_WorkerControlServicer_to_server(WorkerControl(service), server)
    config = service.settings
    bound = (
        server.add_secure_port(
            address,
            grpc.ssl_server_credentials(
                [(config.tls_key.read_bytes(), config.tls_cert.read_bytes())]
            ),
        )
        if config.tls_cert and config.tls_key
        else server.add_insecure_port(address)
    )
    if bound == 0:
        raise RuntimeError(f"cannot bind RPC server: {address}")
    return server


def main() -> None:
    configure_logging()
    configure_tracing("strata-rpc")
    settings = Settings()
    server = make_server(
        EngineService(sessions(make_engine(settings.database_url)), settings),
        os.getenv("STRATA_RPC_BIND", "[::]:50051"),
    )
    server.start()
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(5).wait()


if __name__ == "__main__":
    main()
