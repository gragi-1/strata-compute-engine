from enum import StrEnum
from random import uniform


class JobStatus(StrEnum):
    PENDING = "PENDING"
    QUEUED = "QUEUED"
    SCHEDULED = "SCHEDULED"
    RUNNING = "RUNNING"
    RETRYING = "RETRYING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    CANCELLED = "CANCELLED"
    TIMED_OUT = "TIMED_OUT"


TERMINAL = {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED, JobStatus.TIMED_OUT}
ACTIVE = {JobStatus.SCHEDULED, JobStatus.RUNNING, JobStatus.CANCEL_REQUESTED}
WAITING = {JobStatus.QUEUED, JobStatus.RETRYING}
TRANSITIONS: dict[JobStatus, set[JobStatus]] = {
    JobStatus.PENDING: {JobStatus.QUEUED},
    JobStatus.QUEUED: {JobStatus.SCHEDULED, JobStatus.CANCELLED},
    JobStatus.RETRYING: {JobStatus.SCHEDULED, JobStatus.CANCELLED},
    JobStatus.SCHEDULED: {
        JobStatus.RUNNING,
        JobStatus.RETRYING,
        JobStatus.FAILED,
        JobStatus.CANCELLED,
    },
    JobStatus.RUNNING: {
        JobStatus.SUCCEEDED,
        JobStatus.FAILED,
        JobStatus.RETRYING,
        JobStatus.TIMED_OUT,
        JobStatus.CANCEL_REQUESTED,
    },
    JobStatus.CANCEL_REQUESTED: {JobStatus.CANCELLED},
    JobStatus.FAILED: {JobStatus.QUEUED},
    JobStatus.TIMED_OUT: {JobStatus.QUEUED},
    JobStatus.CANCELLED: {JobStatus.QUEUED},
    JobStatus.SUCCEEDED: set(),
}


def validate_transition(current: str, target: JobStatus) -> None:
    if target not in TRANSITIONS[JobStatus(current)]:
        raise ValueError(f"illegal job transition: {current} -> {target}")


def retry_delay(attempt: int, base: float, cap: float, jitter: float) -> float:
    return float(min(cap, base * 2 ** min(max(attempt - 1, 0), 30)) + uniform(0, jitter))


def fits(cpu: float, memory: int, free_cpu: float, free_memory: int) -> bool:
    return cpu <= free_cpu + 1e-9 and memory <= free_memory
