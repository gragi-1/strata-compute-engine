from unittest.mock import MagicMock

import pytest

from control_plane.config import Settings
from worker.agent import Agent


def test_rejected_duplicate_worker_does_not_destroy_live_owners_containers():
    transport = MagicMock()
    transport.call.side_effect = RuntimeError("worker ID already has a live session")
    executor = MagicMock()
    agent = Agent(transport, executor, "duplicate", 2, 1024, Settings())
    with pytest.raises(RuntimeError, match="live session"):
        agent.register()
    executor.cleanup_orphans.assert_not_called()
