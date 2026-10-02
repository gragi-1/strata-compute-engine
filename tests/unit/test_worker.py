import io
import tarfile
import time
from unittest.mock import MagicMock

import pytest

from control_plane.config import Settings
from control_plane.rpc import engine_pb2 as pb
from worker.agent import Agent, Running
from worker.executor import DockerExecutor


def assignment():
    return pb.Assignment(
        attempt_id="attempt",
        job_id="job",
        lease_token="token",
        image="strata/python-workloads:local",
        command=["python", "main.py"],
        cpu=1.5,
        memory_mb=512,
        timeout_seconds=600,
        lease_seconds=30,
    )


def test_executor_applies_isolation_and_hard_resource_limits():
    client = MagicMock()
    executor = DockerExecutor(["strata/python-workloads:local"], client=client)
    a = assignment()
    executor.create(a, "worker")
    args, kwargs = client.containers.create.call_args
    assert kwargs["nano_cpus"] == 1500000000
    assert kwargs["mem_limit"] == kwargs["memswap_limit"] == "512m"
    assert kwargs["network_disabled"] and kwargs["read_only"]
    assert kwargs["init"]
    assert kwargs["user"] == "65534:65534" and kwargs["cap_drop"] == ["ALL"]
    assert "strata-output-attempt" in kwargs["volumes"]
    a.image = "untrusted"
    with pytest.raises(ValueError, match="allowlisted"):
        executor.create(a, "worker")


def test_artifact_archive_never_extracts_symlinks_or_traversal():
    data = io.BytesIO()
    with tarfile.open(fileobj=data, mode="w") as archive:
        for name in ["output/result.csv", "output/../escape", "output/nested/file"]:
            member = tarfile.TarInfo(name)
            member.size = 3
            archive.addfile(member, io.BytesIO(b"1,2"))
        link = tarfile.TarInfo("output/link")
        link.type = tarfile.SYMTYPE
        link.linkname = "/etc/passwd"
        archive.addfile(link)
    container = MagicMock()
    container.get_archive.return_value = ([data.getvalue()], {})
    executor = DockerExecutor([], client=MagicMock())
    assert list(executor.artifacts(container, 1024)) == [("result.csv", b"1,2")]
    with pytest.raises(ValueError, match="exceeds"):
        list(executor.artifacts(container, 2))


def test_local_lease_guard_stops_during_a_network_partition():
    executor = MagicMock()
    agent = Agent(MagicMock(), executor, "worker", 2, 1024, Settings())
    a = assignment()
    running = Running(a, MagicMock(), time.monotonic(), time.monotonic() - 1)
    agent.running[a.attempt_id] = running
    agent.guard()
    assert running.fenced
    executor.stop.assert_called_once_with(running.container)
    agent.guard()
    assert executor.stop.call_count == 1


def test_runtime_guard_stops_before_reporting_timeout():
    executor = MagicMock()
    agent = Agent(MagicMock(), executor, "worker", 2, 1024, Settings())
    a = assignment()
    a.timeout_seconds = 1
    running = Running(a, MagicMock(), time.monotonic() - 2, time.monotonic() + 30)
    agent.running[a.attempt_id] = running
    agent.guard()
    assert running.result.outcome == "TIMED_OUT" and not running.terminating
    executor.stop.assert_called_once()


def test_heartbeat_uses_send_time_for_local_lease(monkeypatch):
    executor = MagicMock()
    transport = MagicMock()
    transport.session_id = "session"
    agent = Agent(transport, executor, "worker", 2, 1024, Settings())
    a = assignment()
    running = Running(a, MagicMock(), 10, 20)
    agent.running[a.attempt_id] = running
    monkeypatch.setattr("worker.agent.time.monotonic", lambda: 100)
    transport.call.return_value = pb.HeartbeatReply(
        commands=[pb.LeaseCommand(attempt_id=a.attempt_id, valid=True, lease_seconds=30)]
    )
    agent.heartbeat()
    assert running.lease_deadline == 130
    transport.call.return_value = pb.HeartbeatReply(
        commands=[pb.LeaseCommand(attempt_id=a.attempt_id, valid=False, cancel=True)]
    )
    agent.heartbeat()
    assert running.fenced and running.result.outcome == "CANCELLED"
