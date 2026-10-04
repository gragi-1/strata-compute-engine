import json
from concurrent.futures import ThreadPoolExecutor

import pytest
from sqlalchemy import select

from control_plane.models import Attempt, Worker
from control_plane.runtimes import CellSubmit, GroupSubmit, RuntimeService, SessionSubmit
from control_plane.schemas import JobSubmit
from control_plane.services import DomainError
from scheduler.core import Scheduler
from tests.helpers import complete, register
from tests.integration.test_identity import headers, prepare


def spec(**kwargs):
    return JobSubmit(
        name="Compute",
        image="strata/python-workloads:local",
        command=["python", "-c", "pass"],
        resources={"cpu": 0.5, "memory_mb": 64},
        **kwargs,
    )


def start_all(svc):
    with svc.factory() as s:
        workers = list(s.scalars(select(Worker)))
    assignments = []
    for worker in workers:
        for a in svc.assignments(worker.id, worker.session_id):
            svc.start(a["attempt_id"], worker.session_id, a["lease_token"])
            assignments.append((worker, a))
    return assignments


def exchange(runtime, pair, value):
    w, a = pair
    data = runtime.exchange(
        a["attempt_id"], w.session_id, a["lease_token"], json.dumps(value).encode()
    )
    return json.loads(data) if data else None


@pytest.mark.parametrize("backend", ["service", "postgres_service"])
def test_gang_reserves_all_or_none_with_two_concurrent_schedulers(request, backend):
    svc = request.getfixturevalue(backend)
    runtime = RuntimeService(svc)
    group, _ = runtime.create_group(GroupSubmit(name="Reduction", nodes=2, job=spec()), "group-key")
    one = register(svc, "one", cpu=1, memory=128, capabilities=["runtime-bridge", "bounded-output"])
    assert Scheduler(svc).schedule() == 0
    with svc.factory() as s:
        assert list(s.scalars(select(Attempt))) == []
        assert s.get(Worker, one.id).cpu_reserved == 0
    register(svc, "two", cpu=1, memory=128, capabilities=["runtime-bridge", "bounded-output"])
    if backend == "postgres_service":
        with ThreadPoolExecutor(2) as pool:
            assert sum(pool.map(lambda _: Scheduler(svc).schedule(), range(2))) == 2
    else:
        assert Scheduler(svc).schedule() == 2
        assert Scheduler(svc).schedule() == 0
    with svc.factory() as s:
        attempts = list(s.scalars(select(Attempt)))
        assert len(attempts) == 2 and len({a.worker_id for a in attempts}) == 2
        assert all(
            a.cpu_reserved == pytest.approx(0.51) and a.memory_reserved_mb == 96 for a in attempts
        )
    original, created = runtime.create_group(
        GroupSubmit(name="Reduction", nodes=2, job=spec()), "group-key"
    )
    assert not created and original.id == group.id
    with pytest.raises(DomainError, match="different"):
        runtime.create_group(GroupSubmit(name="Other", nodes=2, job=spec()), "group-key")
    pairs = start_all(svc)
    message = {"kind": "collective", "sequence": 0, "operation": "sum", "values": [1.0, 2.0]}
    assert exchange(runtime, pairs[0], message) is None
    assert exchange(runtime, pairs[1], message)["values"] == [2.0, 4.0]
    assert exchange(runtime, pairs[0], message)["values"] == [2.0, 4.0]
    with pytest.raises(DomainError, match="different"):
        exchange(runtime, pairs[0], {**message, "values": [9.0, 2.0]})
    with pytest.raises(DomainError, match="sequence"):
        exchange(runtime, pairs[0], {**message, "sequence": 2})
    # A failing rank fences the entire group; retry creates new identities only after all finish.
    complete(svc, *pairs[0], outcome="FAILED", code=1)
    runtime.tick()
    assert svc.get_job(pairs[1][1]["job_id"]).status == "CANCEL_REQUESTED"
    with pytest.raises(DomainError, match="terminal"):
        runtime.retry_group(group.id, "retry-key")
    complete(svc, *pairs[1], outcome="CANCELLED", code=137)
    with svc.factory() as s:
        assert all(
            w.cpu_reserved == 0 and w.memory_reserved_mb == 0 for w in s.scalars(select(Worker))
        )
    replacement, created = runtime.retry_group(group.id, "retry-key")
    assert created and replacement.id != group.id
    again, created = runtime.retry_group(group.id, "retry-key")
    assert not created and again.id == replacement.id
    original_key_retry, created = runtime.retry_group(group.id, "group-key")
    assert created and original_key_retry.id != group.id
    with pytest.raises(DomainError, match="whole"):
        svc.retry(pairs[0][1]["job_id"])


def test_cancelling_a_queued_or_scheduled_rank_cancels_the_whole_group(service):
    runtime = RuntimeService(service)
    group, _ = runtime.create_group(GroupSubmit(name="Cancel", job=spec()), None)
    service.cancel(runtime.group(group.id)["members"][0]["id"])
    runtime.tick()
    assert runtime.group(group.id)["status"] == "CANCELLED"
    assert all(j["status"] == "CANCELLED" for j in runtime.group(group.id)["members"])
    group, _ = runtime.create_group(GroupSubmit(name="Scheduled", job=spec()), None)
    for name in ("one", "two"):
        register(service, name, capabilities=["runtime-bridge"])
    assert Scheduler(service).schedule() == 2
    service.cancel(runtime.group(group.id)["members"][0]["id"])
    with service.factory() as s:
        assert all(w.running_jobs == 0 for w in s.scalars(select(Worker)))


@pytest.mark.parametrize("backend", ["service", "postgres_service"])
def test_session_cells_replay_order_bounds_idle_and_deadline(request, backend):
    svc = request.getfixturevalue(backend)
    runtime = RuntimeService(svc)
    session, _ = runtime.create_session(SessionSubmit(job=spec(), idle_seconds=10), "notebook")
    register(svc, capabilities=["runtime-bridge", "python"])
    assert Scheduler(svc).schedule() == 1
    pair = start_all(svc)[0]
    cell = runtime.submit_cell(
        session.id, CellSubmit(code="x=2; print(x)", timeout_seconds=2), "cell"
    )
    assert (
        runtime.submit_cell(
            session.id, CellSubmit(code="x=2; print(x)", timeout_seconds=2), "cell"
        ).id
        == cell.id
    )
    with pytest.raises(DomainError, match="different"):
        runtime.submit_cell(session.id, CellSubmit(code="x=3"), "cell")
    message = {"kind": "session", "sequence": 0, "result": None}
    dispatched = exchange(runtime, pair, message)
    assert dispatched["cell_id"] == cell.id
    assert exchange(runtime, pair, message) == dispatched
    result = {"cell_id": cell.id, "stdout": "2\n", "stderr": "", "error": None}
    assert exchange(runtime, pair, {"kind": "session", "sequence": 1, "result": result}) is None
    assert exchange(runtime, pair, {"kind": "session", "sequence": 1, "result": result}) is None
    next_cell = runtime.submit_cell(
        session.id, CellSubmit(code="while True: pass", timeout_seconds=2), "next"
    )
    assert (
        exchange(runtime, pair, {"kind": "session", "sequence": 1, "result": result})["cell_id"]
        == next_cell.id
    )
    svc.clock.advance(3)
    runtime.tick()
    assert svc.get_job(session.job_id).status == "CANCEL_REQUESTED"
    with pytest.raises(DomainError, match="closed"):
        runtime.submit_cell(session.id, CellSubmit(code="pass"), "closed")
    complete(svc, *pair, outcome="CANCELLED", code=137)
    with pytest.raises(DomainError, match="stale"):
        exchange(runtime, pair, {"kind": "session", "sequence": 1, "result": result})


def test_runtime_project_isolation_viewer_read_only_and_export(service):
    client, _, root, alpha, beta, _, tokens = prepare(service)
    operator, viewer = headers(tokens["alice"], alpha), headers(tokens["bob"], beta)
    body = {"job": spec().model_dump(), "idle_seconds": 10}
    response = client.post("/interactive-sessions", headers=operator, json=body)
    assert response.status_code == 201, response.text
    session = response.json()
    assert client.get(f"/interactive-sessions/{session['id']}", headers=viewer).status_code == 404
    assert client.post("/interactive-sessions", headers=viewer, json=body).status_code == 403
    cell = client.post(
        f"/interactive-sessions/{session['id']}/cells",
        headers={**operator, "Idempotency-Key": "cell"},
        json={"code": "print('safe')"},
    )
    assert cell.status_code == 201
    assert (
        client.get(f"/interactive-sessions/{session['id']}/notebook", headers=operator).json()[
            "nbformat"
        ]
        == 4
    )
    assert (
        client.post(
            f"/interactive-sessions/{session['id']}/proxy",
            headers={**operator, "Idempotency-Key": "proxy"},
            json={"path": "//outside.example"},
        ).status_code
        == 422
    )
    group = client.post(
        "/compute-groups", headers=operator, json={"name": "Group", "job": spec().model_dump()}
    )
    assert group.status_code == 201
    assert client.get(f"/compute-groups/{group.json()['id']}", headers=viewer).status_code == 404
    assert client.get("/compute-groups", headers=viewer).json() == []
    assert (
        client.post(f"/interactive-sessions/{session['id']}/stop", headers=operator).status_code
        == 200
    )
    client.close()


def test_idle_sessions_and_lost_workers_close_inflight_cells(service):
    runtime = RuntimeService(service)
    row, _ = runtime.create_session(SessionSubmit(job=spec(), idle_seconds=10), None)
    register(service, capabilities=["runtime-bridge", "python"])
    Scheduler(service).schedule()
    pair = start_all(service)[0]
    cell = runtime.submit_cell(row.id, CellSubmit(code="pass"), "pending")
    service.clock.advance(11)
    runtime.tick()
    assert service.get_job(row.job_id).status == "CANCEL_REQUESTED"
    complete(service, *pair, outcome="CANCELLED", code=137)
    runtime.tick()
    with service.factory() as s:
        from control_plane.models import SessionCell

        assert s.get(SessionCell, cell.id).status == "FAILED"
    # A new session is never resumed into another kernel after worker replacement.
    row, _ = runtime.create_session(SessionSubmit(job=spec()), None)
    Scheduler(service).schedule()
    pair = start_all(service)[0]
    cell = runtime.submit_cell(row.id, CellSubmit(code="x=1"), "lost")
    assert exchange(runtime, pair, {"kind": "session", "sequence": 0, "result": None})
    service.clock.advance(31)
    Scheduler(service).tick()
    assert service.get_job(row.job_id).status == "FAILED"
    with service.factory() as s:
        assert s.get(SessionCell, cell.id).status == "FAILED"


def test_gang_lost_rank_fences_peers_and_bad_runtime_messages_do_not_mutate(service):
    runtime = RuntimeService(service)
    group, _ = runtime.create_group(GroupSubmit(name="Lost rank", job=spec()), None)
    for name in ("one", "two"):
        register(service, name, capabilities=["runtime-bridge"])
    Scheduler(service).schedule()
    pairs = start_all(service)
    worker, assignment = pairs[0]
    with pytest.raises(DomainError, match="stale"):
        runtime.exchange(
            assignment["attempt_id"],
            worker.session_id,
            "incorrect-token",
            b'{"kind":"collective","sequence":0,"operation":"barrier","values":[]}',
        )
    with pytest.raises(DomainError, match="16 KiB"):
        runtime.exchange(
            assignment["attempt_id"], worker.session_id, assignment["lease_token"], b"x" * 16385
        )
    for raw in (b"invalid", b"[]", b'{"kind":"unknown"}'):
        with pytest.raises(DomainError):
            runtime.exchange(
                assignment["attempt_id"], worker.session_id, assignment["lease_token"], raw
            )
    with service.factory.begin() as s:
        s.get(Worker, worker.id).status = "LOST"
    Scheduler(service).tick()
    assert runtime.group(group.id)["status"] == "FAILED"
    assert service.get_job(pairs[1][1]["job_id"]).status == "CANCEL_REQUESTED"


def test_pool_packing_keeps_group_participants_on_distinct_nodes(service):
    from control_plane.models import WorkerPool
    from control_plane.provisioning import PoolService, PoolUpdate
    from tests.integration.test_provisioning import registered_pool

    config, controller = registered_pool(service)
    PoolService(service).update(config.id, PoolUpdate(enabled=True, minimum=0, maximum=4))
    RuntimeService(service).create_group(
        GroupSubmit(name="Distinct nodes", nodes=3, job=spec()), None
    )
    with service.factory() as s:
        assert controller.desired(s, s.get(WorkerPool, config.id), []) == 3
    PoolService(service).update(config.id, PoolUpdate(enabled=True, minimum=0, maximum=2))
    with service.factory() as s:
        assert controller.desired(s, s.get(WorkerPool, config.id), []) == 0
