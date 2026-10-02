from scheduler.core import Scheduler
from tests.helpers import register, submit


def test_fractional_cpu_jobs_cannot_exceed_container_capacity(service):
    service.settings.worker_max_jobs = 2
    worker = register(service, cpu=64, memory=65536)
    jobs = [submit(service, resources={"cpu": 0.001, "memory_mb": 1}) for _ in range(5)]
    assert Scheduler(service).schedule() == 2
    assert len(service.assignments(worker.id, worker.session_id)) == 2
    scheduled = next(j for j in jobs if service.get_job(j.id).status == "SCHEDULED")
    service.cancel(scheduled.id)
    assert Scheduler(service).schedule() == 1
