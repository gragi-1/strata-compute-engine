import os
from typing import Any

import grpc
from opentelemetry.propagate import inject

from control_plane.rpc import engine_pb2 as pb
from control_plane.rpc import engine_pb2_grpc as rpc


class Transport:
    def __init__(self, target: str, token: str) -> None:
        self.channel = grpc.insecure_channel(
            target, options=[("grpc.max_send_message_length", 17 * 1024 * 1024)]
        )
        self.stub = rpc.WorkerControlStub(self.channel)
        self.token = token
        self.session_id = ""

    def call(self, method: str, request: Any) -> Any:
        carrier: dict[str, str] = {}
        inject(carrier)
        return getattr(self.stub, method)(
            request,
            timeout=3,
            metadata=[("authorization", f"Bearer {self.token}"), *carrier.items()],
        )

    def credentials(self, assignment: Any) -> Any:
        return pb.AttemptRequest(
            attempt_id=assignment.attempt_id,
            session_id=self.session_id,
            lease_token=assignment.lease_token,
        )

    @classmethod
    def from_env(cls) -> "Transport":
        return cls(
            os.getenv("STRATA_RPC_TARGET", "localhost:50051"),
            os.getenv("STRATA_WORKER_TOKEN", "local-development-token"),
        )
