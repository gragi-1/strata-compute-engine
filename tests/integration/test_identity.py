from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select

from control_plane.api import create_app
from control_plane.identity import IdentityService
from control_plane.models import AccessToken, AuditEvent, User
from scheduler.core import Scheduler
from tests.helpers import complete, register
from tests.integration.test_platform import spec

PASSWORD = "test-only-password-with-entropy"


def headers(token, project=None):
    result = {"Authorization": f"Bearer {token}"}
    if project:
        result["X-Strata-Project"] = project
    return result


def login(client, username, password=PASSWORD):
    response = client.post("/auth/login", json={"username": username, "password": password})
    assert response.status_code == 200, response.text
    return response.json()["access_token"]


def prepare(service):
    service.settings.identity_enabled = True
    root = IdentityService(service).create_user("root", PASSWORD, is_admin=True, bootstrap=True)
    client = TestClient(create_app(service.settings, service))
    token = login(client, "root")
    admin = headers(token)
    projects = [
        client.post("/projects", headers=admin, json={"name": name}).json()["id"]
        for name in ("Alpha", "Beta")
    ]
    users = {}
    tokens = {}
    for name, project, role in zip(("alice", "bob"), projects, ("operator", "viewer"), strict=True):
        response = client.post(
            "/auth/users", headers=admin, json={"username": name, "password": PASSWORD}
        )
        assert response.status_code == 201, response.text
        users[name] = response.json()["id"]
        assert (
            client.put(
                f"/projects/{project}/members/{users[name]}", headers=admin, json={"role": role}
            ).status_code
            == 200
        )
        tokens[name] = login(client, name)
    return client, root, token, *projects, users, tokens


@pytest.fixture
def workspace(service):
    result = prepare(service)
    yield result
    result[0].close()


def dataset(client, auth, content=b"x,y\n1,2\n"):
    data = client.post("/datasets", headers=auth, json={"name": "Measurements"}).json()
    version = client.post(
        f"/datasets/{data['id']}/versions", headers=auth, json={"label": "v1"}
    ).json()
    file = client.put(
        f"/dataset-versions/{version['id']}/files/data.csv", headers=auth, content=content
    ).json()
    assert client.post(f"/dataset-versions/{version['id']}/seal", headers=auth).status_code == 200
    return data, version, file


def test_sessions_are_hashed_revocable_and_expire(service, workspace):
    client, root, token, *_ = workspace
    with service.factory() as session:
        user = session.get(User, root.id)
        assert user.password_hash.startswith("$argon2id$")
        assert PASSWORD not in user.password_hash
        assert all(row.token_hash != token for row in session.scalars(select(AccessToken)))
        event = session.scalar(select(AuditEvent).where(AuditEvent.action == "LOGIN_SUCCEEDED"))
        assert event.actor_id == root.id
    assert client.get("/auth/me", headers=headers(token)).json()["user"]["username"] == "root"
    assert client.post("/auth/logout", headers=headers(token)).status_code == 204
    assert client.get("/auth/me", headers=headers(token)).status_code == 401
    replacement = login(client, "root")
    service.clock.advance(service.settings.session_lifetime_seconds)
    assert client.get("/auth/me", headers=headers(replacement)).status_code == 401


def test_login_throttling_persists_and_uses_generic_errors(service, workspace):
    client = workspace[0]
    for _ in range(service.settings.login_attempt_limit):
        response = client.post("/auth/login", json={"username": "alice", "password": "wrong"})
        assert response.status_code == 401
    response = client.post("/auth/login", json={"username": "alice", "password": PASSWORD})
    assert response.status_code == 429
    service.clock.advance(service.settings.login_window_seconds)
    assert login(client, "alice")
    unknown = client.post("/auth/login", json={"username": "unknown", "password": "wrong"})
    known = client.post("/auth/login", json={"username": "bob", "password": "wrong"})
    assert unknown.status_code == known.status_code == 401
    assert unknown.json() == known.json()


def test_jobs_are_private_in_every_read_and_mutation(workspace):
    client, _, root, alpha, beta, users, tokens = workspace
    alice = headers(tokens["alice"], alpha)
    assert client.get("/jobs", headers=headers(tokens["alice"])).status_code == 400
    assert client.get("/jobs", headers=headers(tokens["bob"], alpha)).status_code == 404
    response = client.post("/jobs", headers=alice, json=spec())
    assert response.status_code == 201, response.text
    job = response.json()
    assert job["project_id"] == alpha and job["created_by"] == users["alice"]
    other = headers(root, beta)
    assert client.get("/jobs", headers=other).json() == []
    for suffix in ("", "/attempts", "/events", "/logs", "/artifacts"):
        assert client.get(f"/jobs/{job['id']}{suffix}", headers=other).status_code == 404
    for suffix in ("/cancel", "/retry"):
        assert client.post(f"/jobs/{job['id']}{suffix}", headers=other).status_code == 404
    assert [row["id"] for row in client.get("/projects", headers=alice).json()] == [alpha]


def test_dataset_inputs_and_nested_routes_cannot_cross_projects(workspace):
    client, _, root, alpha, beta, _, tokens = workspace
    data, version, file = dataset(client, headers(root, beta))
    auth = headers(tokens["alice"], alpha)
    for path in (
        f"/datasets/{data['id']}/versions",
        f"/dataset-versions/{version['id']}/files",
        f"/dataset-files/{file['id']}",
        f"/dataset-files/{file['id']}/preview",
    ):
        assert client.get(path, headers=auth).status_code == 404
    assert (
        client.put(
            f"/dataset-versions/{version['id']}/files/secret.csv", headers=auth, content=b"x"
        ).status_code
        == 404
    )
    response = client.post(
        "/jobs",
        headers=auth,
        json=spec(inputs=[{"version_id": version["id"], "mount_path": "/data"}]),
    )
    assert response.status_code == 422
    assert client.get("/datasets", headers=auth).json() == []


def test_idempotency_is_independent_for_each_project(workspace):
    client, _, root, alpha, beta, *_ = workspace
    bodies = {
        "/jobs": spec(),
        "/campaigns": {"name": "Sweep", "template": spec(), "matrix": {"seed": [1, 2]}},
        "/workflows": {"name": "Pipeline", "nodes": {"first": spec()}},
    }
    for path, body in bodies.items():
        ids = []
        for project in (alpha, beta):
            auth = headers(root, project) | {"Idempotency-Key": f"same-key-{path}"}
            first = client.post(path, headers=auth, json=body)
            assert first.status_code == 201, first.text
            again = client.post(path, headers=auth, json=body)
            assert again.status_code == 200, again.text
            assert first.json()["id"] == again.json()["id"]
            ids.append(first.json()["id"])
        assert ids[0] != ids[1]


def test_roles_and_platform_administration_are_separate(workspace):
    client, _, root, alpha, beta, users, tokens = workspace
    alice = headers(tokens["alice"], alpha)
    assert (
        client.post("/jobs", headers=headers(tokens["bob"], beta), json=spec()).status_code == 403
    )
    assert client.get("/auth/users", headers=alice).status_code == 403
    assert (
        client.patch(f"/projects/{alpha}", headers=alice, json={"cpu_limit": 4}).status_code == 403
    )
    assert (
        client.put(
            f"/projects/{alpha}/members/{users['bob']}", headers=alice, json={"role": "viewer"}
        ).status_code
        == 403
    )
    client.put(
        f"/projects/{alpha}/members/{users['alice']}", headers=headers(root), json={"role": "admin"}
    )
    assert client.post("/workers/missing/drain", headers=alice).status_code == 403


def test_project_admission_and_storage_quotas_are_atomic(workspace):
    client, _, root, alpha, _, _, tokens = workspace
    client.patch(
        f"/projects/{alpha}",
        headers=headers(root),
        json={"queue_limit": 1, "storage_limit_bytes": 5},
    ).raise_for_status()
    auth = headers(tokens["alice"], alpha)
    response = client.post(
        "/campaigns",
        headers=auth,
        json={"name": "Too large", "template": spec(), "matrix": {"seed": [1, 2]}},
    )
    assert response.status_code == 429
    assert client.get("/jobs", headers=auth).json() == []
    keyed = auth | {"Idempotency-Key": "quota"}
    assert client.post("/jobs", headers=keyed, json=spec()).status_code == 201
    assert client.post("/jobs", headers=keyed, json=spec()).status_code == 200
    assert client.post("/jobs", headers=auth, json=spec()).status_code == 429
    data = client.post("/datasets", headers=auth, json={"name": "Quota"}).json()
    version = client.post(
        f"/datasets/{data['id']}/versions", headers=auth, json={"label": "v1"}
    ).json()
    path = f"/dataset-versions/{version['id']}/files/"
    assert client.put(path + "first.csv", headers=auth, content=b"1234").status_code == 201
    assert client.put(path + "second.csv", headers=auth, content=b"5678").status_code == 413
    assert len(client.get(f"/dataset-versions/{version['id']}/files", headers=auth).json()) == 1


def test_personal_tokens_cannot_gain_permissions_and_follow_membership(workspace):
    client, _, root, alpha, beta, users, tokens = workspace
    alice = headers(tokens["alice"], alpha)
    body = {"name": "Automation", "project_id": alpha, "role": "admin"}
    assert client.post("/auth/tokens", headers=alice, json=body).status_code == 403
    body["role"] = "operator"
    response = client.post("/auth/tokens", headers=alice, json=body)
    assert response.status_code == 201, response.text
    issued = response.json()
    pat = issued["access_token"]
    assert client.get("/jobs", headers=headers(pat, beta)).status_code == 404
    assert client.get("/auth/users", headers=headers(pat)).status_code == 403
    listing = client.get("/auth/tokens", headers=alice).text
    assert pat not in listing and "token_hash" not in listing
    client.put(
        f"/projects/{alpha}/members/{users['alice']}",
        headers=headers(root),
        json={"role": "viewer"},
    )
    assert client.post("/jobs", headers=headers(pat, alpha), json=spec()).status_code == 403
    assert client.delete(f"/auth/tokens/{issued['id']}", headers=alice).status_code == 204
    assert client.get("/jobs", headers=headers(pat, alpha)).status_code == 401


def test_admin_resets_disable_accounts_and_preserve_last_admin(workspace):
    client, root_user, root, _, _, users, tokens = workspace
    admin = headers(root)
    assert (
        client.patch(
            f"/auth/users/{root_user.id}", headers=admin, json={"enabled": False}
        ).status_code
        == 409
    )
    assert (
        client.patch(
            f"/auth/users/{users['alice']}", headers=admin, json={"enabled": False}
        ).status_code
        == 200
    )
    assert client.get("/auth/me", headers=headers(tokens["alice"])).status_code == 401
    assert (
        client.post("/auth/login", json={"username": "alice", "password": PASSWORD}).status_code
        == 401
    )
    new = PASSWORD + "-changed"
    assert (
        client.patch(
            f"/auth/users/{users['bob']}", headers=admin, json={"password": new}
        ).status_code
        == 200
    )
    assert client.get("/auth/me", headers=headers(tokens["bob"])).status_code == 401
    assert login(client, "bob", new)


def test_password_change_revokes_sessions_and_validation_never_echoes_secrets(workspace):
    client, _, root, _, _, _, tokens = workspace
    response = client.post(
        "/auth/users", headers=headers(root), json={"username": "weak", "password": "SECRET"}
    )
    assert response.status_code == 422 and "SECRET" not in response.text
    alice = headers(tokens["alice"])
    body = {"current_password": "wrong", "new_password": PASSWORD + "-new"}
    assert client.post("/auth/password", headers=alice, json=body).status_code == 401
    body["current_password"] = PASSWORD
    assert client.post("/auth/password", headers=alice, json=body).status_code == 204
    assert client.get("/auth/me", headers=alice).status_code == 401
    assert login(client, "alice", body["new_password"])


def test_audit_records_actor_and_last_project_admin_is_protected(workspace):
    client, root_user, root, alpha, _, users, tokens = workspace
    job = client.post("/jobs", headers=headers(tokens["alice"], alpha), json=spec()).json()
    response = client.get(f"/projects/{alpha}/audit", headers=headers(root))
    events = response.json()
    event = next(row for row in events if row["action"] == "JOB_CREATED")
    assert event["actor_id"] == users["alice"] and event["resource_id"] == job["id"]
    assert event["project_id"] == alpha
    assert PASSWORD not in response.text and tokens["alice"] not in response.text
    assert (
        client.delete(
            f"/projects/{alpha}/members/{root_user.id}", headers=headers(root)
        ).status_code
        == 409
    )


def test_postgres_concurrent_project_admission(postgres_service):
    client, _, root, alpha, _, _, tokens = prepare(postgres_service)
    try:
        client.patch(
            f"/projects/{alpha}", headers=headers(root), json={"queue_limit": 3}
        ).raise_for_status()
        auth = headers(tokens["alice"], alpha)
        with ThreadPoolExecutor(max_workers=8) as pool:
            codes = list(
                pool.map(
                    lambda i: (
                        client.post("/jobs", headers=auth, json=spec(name=f"Job {i}")).status_code
                    ),
                    range(20),
                )
            )
        assert codes.count(201) == 3 and codes.count(429) == 17
        assert len(client.get("/jobs", headers=auth).json()) == 3
    finally:
        client.close()


def test_project_execution_budget_is_released_on_completion(service, workspace):
    client, _, root, alpha, beta, *_ = workspace
    client.patch(
        f"/projects/{alpha}",
        headers=headers(root),
        json={
            "cpu_limit": 1,
            "memory_limit_mb": 128,
        },
    ).raise_for_status()
    auth = headers(root, alpha)
    jobs = [
        client.post("/jobs", headers=auth, json=spec(resources={"cpu": 1, "memory_mb": 128})).json()
        for _ in range(3)
    ]
    other = client.post("/jobs", headers=headers(root, beta), json=spec()).json()
    worker = register(service, cpu=16, memory=16000)
    scheduler = Scheduler(service)
    assert scheduler.schedule() == 2
    assignments = service.assignments(worker.id, worker.session_id)
    assert other["id"] in {row["job_id"] for row in assignments}
    chosen = next(row for row in assignments if row["job_id"] != other["id"])
    assert chosen["job_id"] in {row["id"] for row in jobs}
    service.start(chosen["attempt_id"], worker.session_id, chosen["lease_token"])
    complete(service, worker, chosen)
    assert scheduler.schedule() == 1
    assert scheduler.schedule() == 0


def test_small_scheduler_batches_give_each_project_a_turn(service, workspace):
    client, _, root, alpha, beta, *_ = workspace
    service.settings.scheduler_batch_size = 1
    for project in (alpha, beta):
        for _ in range(4):
            client.post("/jobs", headers=headers(root, project), json=spec()).raise_for_status()
    register(service, cpu=16, memory=16000)
    scheduler = Scheduler(service)
    assert scheduler.schedule() == scheduler.schedule() == 1
    assert (
        len(
            [
                j
                for j in client.get("/jobs", headers=headers(root, alpha)).json()
                if j["status"] == "SCHEDULED"
            ]
        )
        == 1
    )
    assert (
        len(
            [
                j
                for j in client.get("/jobs", headers=headers(root, beta)).json()
                if j["status"] == "SCHEDULED"
            ]
        )
        == 1
    )


def test_postgres_concurrent_project_execution_budget(postgres_service):
    client, _, root, alpha, _, _, _ = prepare(postgres_service)
    try:
        client.patch(
            f"/projects/{alpha}",
            headers=headers(root),
            json={
                "cpu_limit": 3,
                "memory_limit_mb": 300,
            },
        ).raise_for_status()
        for _ in range(20):
            client.post(
                "/jobs",
                headers=headers(root, alpha),
                json=spec(
                    resources={"cpu": 1, "memory_mb": 100},
                ),
            ).raise_for_status()
        register(postgres_service, cpu=64, memory=64000)
        with ThreadPoolExecutor(max_workers=2) as pool:
            assert sum(pool.map(lambda _: Scheduler(postgres_service).schedule(), range(2))) == 3
        rows = client.get("/jobs", headers=headers(root, alpha)).json()
        assert len([row for row in rows if row["status"] == "SCHEDULED"]) == 3
    finally:
        client.close()


def test_unrunnable_work_does_not_block_small_batches(service, workspace):
    client, _, root, alpha, beta, *_ = workspace
    service.settings.scheduler_batch_size = 1
    client.patch(f"/projects/{alpha}", headers=headers(root), json={"cpu_limit": 1})
    client.post(
        "/jobs", headers=headers(root, alpha), json=spec(resources={"cpu": 2, "memory_mb": 256})
    ).raise_for_status()
    client.post(
        "/jobs", headers=headers(root, beta), json=spec(capabilities=["unavailable-device"])
    ).raise_for_status()
    small = client.post("/jobs", headers=headers(root, beta), json=spec()).json()
    register(service)
    assert Scheduler(service).schedule() == 1
    assert (
        client.get(f"/jobs/{small['id']}", headers=headers(root, beta)).json()["status"]
        == "SCHEDULED"
    )


def test_priority_aging_eventually_serves_old_low_priority_jobs(service, workspace):
    client, _, root, alpha, *_ = workspace
    service.settings.scheduler_batch_size = 1
    auth = headers(root, alpha)
    old = client.post("/jobs", headers=auth, json=spec(priority=0)).json()
    service.clock.advance(101 * service.settings.priority_aging_seconds)
    client.post("/jobs", headers=auth, json=spec(priority=100)).raise_for_status()
    register(service)
    assert Scheduler(service).schedule() == 1
    assert client.get(f"/jobs/{old['id']}", headers=auth).json()["status"] == "SCHEDULED"


def test_disabling_identity_cannot_expose_project_data(service, workspace):
    client, _, root, alpha, *_ = workspace
    client.post("/jobs", headers=headers(root, alpha), json=spec()).raise_for_status()
    service.settings.identity_enabled = False
    assert client.get("/jobs").status_code == 503
    assert client.get("/ready").status_code == 503


def test_project_disable_allows_existing_worker_results_and_protects_artifact_reads(
    service, workspace
):
    client, _, root, alpha, beta, *_ = workspace
    job = client.post("/jobs", headers=headers(root, alpha), json=spec()).json()
    worker = register(service)
    assert Scheduler(service).schedule() == 1
    assignment = service.assignments(worker.id, worker.session_id)[0]
    service.start(assignment["attempt_id"], worker.session_id, assignment["lease_token"])
    client.patch(f"/projects/{alpha}", headers=headers(root), json={"enabled": False})
    artifact = service.artifact(
        assignment["attempt_id"],
        worker.session_id,
        assignment["lease_token"],
        "result.json",
        "application/json",
        b'{"value":42}',
    )
    complete(service, worker, assignment)
    assert client.get(f"/jobs/{job['id']}", headers=headers(root, alpha)).status_code == 404
    assert client.get(f"/artifacts/{artifact.id}", headers=headers(root, beta)).status_code == 404
    client.patch(f"/projects/{alpha}", headers=headers(root), json={"enabled": True})
    assert (
        client.get(f"/artifacts/{artifact.id}", headers=headers(root, alpha)).content
        == b'{"value":42}'
    )
