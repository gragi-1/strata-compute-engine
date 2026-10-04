import socket

import grpc
import pytest

from control_plane.rpc import engine_pb2 as pb
from control_plane.rpc.server import make_server
from scheduler.core import Scheduler
from tests.helpers import submit
from worker.transport import Transport


def test_real_grpc_protocol_auth_leases_results(service):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    server = make_server(service, f"127.0.0.1:{port}")
    server.start()
    transport = Transport(f"127.0.0.1:{port}", service.settings.worker_token)
    try:
        bad = Transport(f"127.0.0.1:{port}", "wrong")
        with pytest.raises(grpc.RpcError) as exc:
            bad.call(
                "Register", pb.RegisterRequest(worker_id="bad", cpu_total=1, memory_total_mb=512)
            )
        assert exc.value.code() == grpc.StatusCode.UNAUTHENTICATED
        bad.channel.close()
        with pytest.raises(grpc.RpcError):
            transport.call(
                "Register", pb.RegisterRequest(worker_id="../bad", cpu_total=1, memory_total_mb=512)
            )
        registered = transport.call(
            "Register",
            pb.RegisterRequest(
                worker_id="worker",
                cpu_total=4,
                memory_total_mb=4096,
                capabilities=["python", "cpp"],
            ),
        )
        transport.session_id = registered.session_id
        assert transport.call("Cluster", pb.Empty()).cluster_id == registered.cluster_id
        job = submit(service)
        Scheduler(service).schedule()
        poll = transport.call(
            "Poll", pb.PollRequest(worker_id="worker", session_id=transport.session_id)
        )
        a = poll.assignments[0]
        assert a.job_id == job.id
        credentials = transport.credentials(a)
        orphan = pb.OrphanRequest(
            candidates=[pb.OrphanCandidate(worker_id="worker", attempt_id=a.attempt_id)]
        )
        assert not transport.call("InspectOrphans", orphan).decisions[0].remove
        transport.call("Start", credentials)
        heart = transport.call(
            "Heartbeat",
            pb.HeartbeatRequest(
                worker_id="worker",
                session_id=transport.session_id,
                cpu_available=4,
                memory_available_mb=4096,
                leases=[pb.Lease(attempt_id=a.attempt_id, lease_token=a.lease_token)],
            ),
        )
        assert heart.commands[0].valid
        transport.call("PutLogs", pb.LogsRequest(credentials=credentials, content="calculated"))
        artifact = transport.call(
            "PutArtifact",
            pb.ArtifactRequest(
                credentials=credentials,
                name="result.json",
                content_type="application/json",
                content=b"{}",
            ),
        )
        assert artifact.size == 2
        transport.call(
            "Complete",
            pb.CompleteRequest(credentials=credentials, outcome=pb.SUCCEEDED, exit_code=0),
        )
        assert service.get_job(job.id).status == "SUCCEEDED"
        assert transport.call("InspectOrphans", orphan).decisions[0].remove
    finally:
        transport.channel.close()
        server.stop(0).wait()
