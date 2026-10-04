from unittest.mock import MagicMock

from control_plane.config import Settings
from control_plane.models import Worker
from control_plane.rpc import engine_pb2 as pb
from tests.helpers import assigned, complete
from worker.agent import Agent


def test_container_failure_before_start_retries_and_releases_slot(service):
    job, worker, assignment = assigned(service, start=False)
    complete(service, worker, assignment, "FAILED", -1)
    assert service.get_job(job.id).status == "RETRYING"
    with service.factory() as session:
        assert session.get(Worker, worker.id).running_jobs == 0


def test_agent_reports_uncertain_launch_without_stopping_other_jobs():
    transport = MagicMock()
    transport.session_id = "s"
    transport.call.side_effect = lambda method, *args: (
        pb.PollReply()
        if method == "Poll"
        else pb.HeartbeatReply()
        if method == "Heartbeat"
        else pb.Empty()
    )
    transport.credentials.return_value = pb.AttemptRequest(
        attempt_id="a", session_id="s", lease_token="token"
    )
    executor = MagicMock()
    executor.create.return_value.start.side_effect = TimeoutError("Docker start response lost")
    executor.create.return_value.status = "unknown"
    agent = Agent(transport, executor, "worker", 2, 1024, Settings())
    a = pb.Assignment(
        attempt_id="a",
        job_id="j",
        lease_token="token",
        image="strata/python-workloads:local",
        command=["run"],
        cpu=1,
        memory_mb=128,
        lease_seconds=30,
        timeout_seconds=600,
    )
    agent.launch(a)
    assert agent.running["a"].fenced
    method, request = transport.call.call_args.args
    assert method == "Complete" and request.outcome == pb.FAILED
    assert request.exit_code == -1
    executor.create.return_value.start.assert_called_once()
    executor.stop.assert_not_called()
