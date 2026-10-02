import time
from unittest.mock import MagicMock

from control_plane.config import Settings
from control_plane.rpc import engine_pb2 as pb
from worker.agent import Agent, Running


def test_failed_container_cleanup_does_not_block_other_attempts():
    transport = MagicMock()
    transport.session_id = "session"
    transport.call.return_value = pb.PollReply()
    executor = MagicMock()
    executor.remove.side_effect = [TimeoutError("daemon unavailable for one container"), None]
    agent = Agent(transport, executor, "worker", 2, 1024, Settings())
    agent.next_heartbeat = time.monotonic() + 30
    for attempt_id in ["a", "b"]:
        assignment = pb.Assignment(
            attempt_id=attempt_id, job_id=attempt_id, cpu=1, timeout_seconds=600
        )
        agent.running[attempt_id] = Running(
            assignment, MagicMock(), time.monotonic(), time.monotonic() + 30, fenced=True
        )
    agent.tick()
    assert set(agent.running) == {"a"}
    assert executor.remove.call_count == 2
