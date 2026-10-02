import logging
import os
import secrets
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any, TypeVar

import grpc
from opentelemetry.propagate import extract
from pydantic import ValidationError

from control_plane.config import Settings
from control_plane.database import make_engine, sessions
from control_plane.domain import JobStatus
from control_plane.logging import configure_logging
from control_plane.rpc import engine_pb2 as pb
from control_plane.rpc import engine_pb2_grpc as rpc
from control_plane.schemas import Completion, Heartbeat, LeaseRef, WorkerRegister
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
}


class WorkerControl(rpc.WorkerControlServicer):  # type: ignore[misc]
    def __init__(self, service: EngineService) -> None:
        self.svc = service

    def invoke(self, context: Any, name: str, action: Callable[[], T]) -> T:
        metadata = dict(context.invocation_metadata())
        if not secrets.compare_digest(
            metadata.get("authorization", ""), f"Bearer {self.svc.settings.worker_token}"
        ):
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "invalid worker token")
        try:
            with tracer.start_as_current_span(f"rpc.{name}", context=extract(metadata)):
                return action()
        except DomainError as exc:
            context.abort(ERRORS.get(exc.code, grpc.StatusCode.INTERNAL), str(exc))
        except (ValidationError, ValueError) as exc:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
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
                )
            )
            return pb.RegisterReply(
                session_id=worker.session_id,
                heartbeat_interval=self.svc.settings.heartbeat_interval,
                lease_seconds=self.svc.settings.lease_seconds,
                termination_grace_seconds=self.svc.settings.termination_grace_seconds,
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

    def Start(self, request: Any, context: Any) -> Any:
        def action() -> Any:
            self.svc.start(request.attempt_id, request.session_id, request.lease_token)
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


def make_server(service: EngineService, address: str = "[::]:50051") -> Any:
    server = grpc.server(
        ThreadPoolExecutor(max_workers=16),
        maximum_concurrent_rpcs=64,
        options=[("grpc.max_receive_message_length", service.settings.artifact_max_bytes + 65536)],
    )
    rpc.add_WorkerControlServicer_to_server(WorkerControl(service), server)
    if server.add_insecure_port(address) == 0:
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
