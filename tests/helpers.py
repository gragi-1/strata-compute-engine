from control_plane.schemas import Completion, Heartbeat, JobSubmit, LeaseRef, WorkerRegister
from scheduler.core import Scheduler


def submit(service, **kwargs):
    values = {
        "name": "test-compute",
        "image": "strata/python-workloads:local",
        "command": ["python", "main.py"],
    }
    values.update(kwargs)
    return service.submit(JobSubmit(**values))[0]


def register(service, worker_id="worker-1", cpu=4, memory=4096, capabilities=None):
    return service.register(
        WorkerRegister(
            worker_id=worker_id,
            cpu_total=cpu,
            memory_total_mb=memory,
            capabilities=capabilities or ["python", "cpp", "dataset-inputs"],
        )
    )


def assigned(service, job=None, worker=None, start=True):
    job = job or submit(service)
    worker = worker or register(service)
    Scheduler(service).schedule()
    assignment = service.assignments(worker.id, worker.session_id)[0]
    if start:
        service.start(assignment["attempt_id"], worker.session_id, assignment["lease_token"])
    return job, worker, assignment


def complete(service, worker, assignment, outcome="SUCCEEDED", code=0):
    service.complete(
        assignment["attempt_id"],
        Completion(
            session_id=worker.session_id,
            lease_token=assignment["lease_token"],
            outcome=outcome,
            exit_code=code,
        ),
    )


def heartbeat(service, worker, assignment=None):
    return service.heartbeat(
        worker.id,
        Heartbeat(
            session_id=worker.session_id,
            cpu_available=worker.cpu_total,
            memory_available_mb=worker.memory_total_mb,
            leases=[
                LeaseRef(attempt_id=assignment["attempt_id"], lease_token=assignment["lease_token"])
            ]
            if assignment
            else [],
        ),
    )
