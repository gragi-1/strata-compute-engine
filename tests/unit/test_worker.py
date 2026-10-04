import io
import tarfile
import time
from unittest.mock import MagicMock

import grpc
import pytest

from control_plane.config import Settings
from control_plane.rpc import engine_pb2 as pb
from worker.agent import Agent, Pending, Running
from worker.executor import DockerExecutor, Result, start_container


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


@pytest.mark.parametrize("valid", [True, False])
def test_poll_renews_a_nearly_expired_lease_before_any_container_creation(monkeypatch, valid):
    moment = [100.0]
    monkeypatch.setattr(time, "monotonic", lambda: moment[0])
    a = assignment()
    a.lease_seconds = 0.05
    transport, executor = MagicMock(), MagicMock()
    transport.session_id = "session"
    agent = Agent(transport, executor, "worker", 2, 1024, Settings())
    # The previous batch just sent a heartbeat, so the normal periodic timer is not due.
    agent.next_heartbeat = 101.0

    def rpc(method, request):
        if method == "Poll":
            return pb.PollReply(assignments=[a])
        if method == "Heartbeat":
            assert [lease.attempt_id for lease in request.leases] == [a.attempt_id]
            return pb.HeartbeatReply(
                commands=[
                    pb.LeaseCommand(
                        attempt_id=a.attempt_id, valid=valid, cancel=not valid, lease_seconds=30
                    )
                ]
            )
        return MagicMock()

    transport.call.side_effect = rpc

    def create(*_):
        moment[0] += 0.2  # Docker spends longer than the initial remaining scheduling lease.
        agent.launch_progress()
        return MagicMock()

    executor.create.side_effect = create
    if valid:
        agent.tick()
        assert agent.running[a.attempt_id].lease_deadline == 130.0
        executor.create.assert_called_once()
    else:
        with pytest.raises(RuntimeError, match="lost its lease"):
            agent.tick()
        executor.create.assert_not_called()


def test_executor_applies_isolation_and_hard_resource_limits():
    client = MagicMock()
    # Model the daemon's real allocated volume and keeper readiness response.
    config = Settings()
    client.volumes.create.return_value.attrs = {
        "Options": {
            "type": "tmpfs",
            "device": "tmpfs",
            "o": "rw,nosuid,nodev,noexec,noswap,uid=65534,gid=65534,mode=0700,"
            f"size={config.worker_output_bytes},nr_inodes={config.worker_output_inodes}",
        },
        "Labels": {"strata.worker": "worker", "strata.attempt": "attempt"},
    }
    client.containers.create.return_value.logs.return_value = b"storage keeper ready"
    client.containers.create.return_value.status = "running"
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


def test_launch_progress_discovers_new_assignments_while_starting_the_current_batch():
    current, following = assignment(), assignment()
    following.attempt_id = "newly-scheduled"
    transport = MagicMock()
    transport.session_id = "session"
    agent = Agent(transport, MagicMock(), "worker", 2, 1024, Settings())
    agent.pending[current.attempt_id] = Pending(current, time.monotonic() + 30)
    agent.launching = current.attempt_id

    def rpc(method, request):
        if method == "Poll":
            return pb.PollReply(assignments=[following])
        assert {lease.attempt_id for lease in request.leases} == {
            current.attempt_id,
            following.attempt_id,
        }
        return pb.HeartbeatReply(
            commands=[
                pb.LeaseCommand(attempt_id=lease.attempt_id, valid=True, lease_seconds=30)
                for lease in request.leases
            ]
        )

    transport.call.side_effect = rpc
    agent.launch_progress()
    assert set(agent.pending) == {current.attempt_id, following.attempt_id}
    assert all(p.deadline > time.monotonic() + 20 for p in agent.pending.values())


@pytest.mark.parametrize("cancel", [False, True])
def test_keeper_reconciles_in_progress_start_without_repeating_it(monkeypatch, cancel):
    keeper = MagicMock()
    keeper.start.side_effect = OSError("start response timed out")
    keeper.status = "created"
    statuses = iter(["created", "running"])
    keeper.reload.side_effect = lambda: setattr(keeper, "status", next(statuses))
    keeper.logs.return_value = b"storage keeper ready"
    client = MagicMock()
    client.containers.create.return_value = keeper
    executor = DockerExecutor([], client=client)
    executor.renew_storage = MagicMock()

    def progress():
        if cancel and keeper.reload.called:
            raise RuntimeError("assignment fenced")

    executor.progress = progress
    monkeypatch.setattr("worker.executor.time.sleep", lambda _: None)
    if cancel:
        with pytest.raises(RuntimeError, match="fenced"):
            executor.create_keeper(assignment(), "worker", "output")
        keeper.remove.assert_called_once_with(force=True)
        executor.renew_storage.assert_not_called()
    else:
        assert executor.create_keeper(assignment(), "worker", "output") is keeper
        assert keeper.reload.call_count == 2
        executor.renew_storage.assert_called_once_with("attempt")
        keeper.remove.assert_not_called()
    keeper.start.assert_called_once()


def test_keeper_does_not_retry_an_explicit_start_rejection():
    from docker.errors import APIError

    keeper = MagicMock()
    keeper.start.side_effect = APIError("start rejected")
    client = MagicMock()
    client.containers.create.return_value = keeper
    executor = DockerExecutor([], client=client)
    with pytest.raises(APIError, match="rejected"):
        executor.create_keeper(assignment(), "worker", "output")
    keeper.start.assert_called_once()
    keeper.reload.assert_not_called()
    keeper.remove.assert_called_once_with(force=True)


@pytest.mark.parametrize("status", ["running", "exited", "dead"])
def test_workload_start_reconciles_created_then_started_or_already_finished(monkeypatch, status):
    container = MagicMock()
    container.start.side_effect = OSError("lost start reply")
    statuses = iter(["created", status])
    container.reload.side_effect = lambda: setattr(container, "status", next(statuses))
    progress = MagicMock()
    monkeypatch.setattr("worker.executor.time.sleep", lambda _: None)
    start_container(container, progress)
    container.start.assert_called_once()
    assert container.reload.call_count == 2 and progress.call_count >= 3


def test_workload_start_honors_fencing_without_repeating_start(monkeypatch):
    container = MagicMock()
    container.start.side_effect = OSError("lost start reply")
    container.status = "created"

    def progress():
        if container.reload.called:
            raise RuntimeError("assignment fenced")

    monkeypatch.setattr("worker.executor.time.sleep", lambda _: None)
    with pytest.raises(RuntimeError, match="fenced"):
        start_container(container, progress)
    container.start.assert_called_once()
    container.reload.assert_called_once()


def test_workload_start_rejects_explicit_daemon_failure():
    from docker.errors import APIError

    container = MagicMock()
    container.start.side_effect = APIError("start rejected")
    with pytest.raises(APIError, match="rejected"):
        start_container(container, MagicMock())
    container.start.assert_called_once()
    container.reload.assert_not_called()


def test_unresolved_workload_start_has_a_bounded_inspection_deadline(monkeypatch):
    container = MagicMock()
    container.start.side_effect = OSError("lost start reply")
    container.status = "created"
    moments = iter([0, 0, 11])
    monkeypatch.setattr("worker.executor.time.monotonic", lambda: next(moments))
    monkeypatch.setattr("worker.executor.time.sleep", lambda _: None)
    with pytest.raises(RuntimeError, match="deadline"):
        start_container(container, MagicMock())
    container.start.assert_called_once()
    container.reload.assert_called_once()


@pytest.mark.parametrize("outcome", ["TIMED_OUT", "CANCELLED"])
def test_launch_keeps_a_watchdog_or_cancellation_decision_during_lost_start_reply(outcome):
    transport = MagicMock()
    transport.credentials.return_value = pb.AttemptRequest()
    executor = MagicMock()
    container = executor.create.return_value
    container.attrs = {"Image": "reviewed-image"}
    agent = Agent(transport, executor, "worker", 2, 1024, Settings())
    agent.next_heartbeat = time.monotonic() + 100
    a = assignment()

    def timed_start():
        agent.running[a.attempt_id].result = Result(outcome, 137, "execution decision")
        raise OSError("lost start reply")

    container.start.side_effect = timed_start
    agent.launch(a)
    running = agent.running[a.attempt_id]
    assert not running.fenced and running.result.outcome == outcome
    assert all(call.args[0] != "Complete" for call in transport.call.call_args_list)
    container.start.assert_called_once()


@pytest.mark.parametrize(
    "code",
    [
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.UNAUTHENTICATED,
        grpc.StatusCode.PERMISSION_DENIED,
        grpc.StatusCode.FAILED_PRECONDITION,
    ],
)
def test_startup_disconnect_preserves_only_an_unexpired_lease(monkeypatch, code):
    class Disconnected(grpc.RpcError):
        def code(self):
            return code

    moment = [100.0]
    monkeypatch.setattr("worker.agent.time.monotonic", lambda: moment[0])
    transport = MagicMock()
    transport.session_id = "session"
    agent = Agent(transport, MagicMock(), "worker", 2, 1024, Settings())
    a = assignment()
    agent.pending[a.attempt_id] = Pending(a, 130.0)
    agent.launching = a.attempt_id
    agent.heartbeat = MagicMock(side_effect=Disconnected())
    if code in {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED}:
        agent.launch_progress()
        assert agent.pending[a.attempt_id].deadline == 130.0
        assert agent.next_heartbeat == 101.0
        moment[0] = 131.0
        with pytest.raises(RuntimeError, match="lease"):
            agent.launch_progress()
        agent.heartbeat.assert_called_once()
    else:
        with pytest.raises(Disconnected):
            agent.launch_progress()


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


def test_output_keeper_rejects_mutable_production_images():
    settings = Settings().model_copy(
        update={"production": True, "storage_keeper_image": "mutable:latest"}
    )
    with pytest.raises(ValueError, match="immutable"):
        DockerExecutor([], client=MagicMock(), settings=settings)


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
    reply = pb.HeartbeatReply(
        commands=[pb.LeaseCommand(attempt_id=a.attempt_id, valid=True, lease_seconds=30)]
    )
    transport.call.side_effect = lambda method, _: pb.PollReply() if method == "Poll" else reply
    agent.heartbeat()
    assert running.lease_deadline == 130
    reply = pb.HeartbeatReply(
        commands=[pb.LeaseCommand(attempt_id=a.attempt_id, valid=False, cancel=True)]
    )
    agent.heartbeat()
    assert running.fenced and running.result.outcome == "CANCELLED"
