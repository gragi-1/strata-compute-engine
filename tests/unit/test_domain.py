import math

import pytest
from pydantic import ValidationError

from control_plane.config import Settings
from control_plane.domain import TRANSITIONS, JobStatus, fits, retry_delay, validate_transition
from control_plane.schemas import Completion, JobSubmit


@pytest.mark.parametrize("current", list(JobStatus))
def test_state_machine_rejects_every_illegal_edge(current):
    for target in JobStatus:
        if target in TRANSITIONS[current]:
            validate_transition(current, target)
        else:
            with pytest.raises(ValueError, match="illegal"):
                validate_transition(current, target)


def test_backoff_is_bounded_with_jitter():
    assert [retry_delay(n, 2, 60, 0) for n in range(1, 8)] == [2, 4, 8, 16, 32, 60, 60]
    for _ in range(50):
        assert 60 <= retry_delay(10000, 2, 60, 3) <= 63


@pytest.mark.parametrize(
    "cpu,memory,free_cpu,free_memory,expected",
    [
        (4, 256, 4, 256, True),
        (8, 256, 4, 1024, False),
        (1, 2048, 8, 1024, False),
        (0.3, 1, 0.1 + 0.2, 1, True),
    ],
)
def test_resource_matching(cpu, memory, free_cpu, free_memory, expected):
    assert fits(cpu, memory, free_cpu, free_memory) == expected


@pytest.mark.parametrize(
    "resources", [{"cpu": 0}, {"cpu": math.nan}, {"cpu": math.inf}, {"memory_mb": -1}]
)
def test_invalid_job_resources(resources):
    with pytest.raises(ValidationError):
        JobSubmit(name="bad", image="image", command=["run"], resources=resources)


@pytest.mark.parametrize(
    "values",
    [{"heartbeat_interval": 15}, {"scheduling_policy": "random"}, {"retry_base_seconds": 100}],
)
def test_invalid_configuration(values):
    with pytest.raises(ValidationError):
        Settings(**values)


def test_invalid_completion():
    with pytest.raises(ValidationError):
        Completion(session_id="a", lease_token="a", outcome="RUNNING")
    with pytest.raises(ValidationError):
        Completion(session_id="a", lease_token="a", outcome="SUCCEEDED", exit_code=1)
