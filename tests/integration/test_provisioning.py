"""Policy/registration boundaries; live infrastructure evidence is in the Docker tests."""

from types import SimpleNamespace

import pytest
from pydantic import ValidationError
from sqlalchemy import select

from control_plane.models import AuditEvent, ProvisionedWorker
from control_plane.provisioning import PoolController, PoolService, PoolSettings, PoolUpdate
from control_plane.schemas import WorkerRegister
from control_plane.services import DomainError
from tests.integration.test_identity import headers, prepare


def configuration(**updates):
    return PoolSettings(
        id="test-host",
        image="sha256:" + "a" * 64,
        rpc_target="private-rpc:50051",
        cpu_per_worker=1,
        memory_per_worker_mb=1024,
        host_cpu_budget=5,
        host_memory_budget_mb=5120,
        maximum_workers=8,
        **updates,
    )


def registered_pool(service):
    config = configuration()
    # No infrastructure is provisioned by this policy fixture.
    adapter = SimpleNamespace(
        host_id="policy-test-daemon", image=config.image, keeper_image="sha256:" + "b" * 64
    )
    controller = PoolController(service, config, adapter)
    with service.factory.begin() as session:
        controller.register(session)
    return config, controller


def test_policy_hard_capacity_authorization_and_idempotent_audit(service):
    config, _ = registered_pool(service)
    assert config.hard_limit == 4  # Includes separate CPU/RAM agent overhead.
    client, _, token, _, _, _, tokens = prepare(service)
    body = {"enabled": True, "minimum": 1, "maximum": 4}
    path = "/cluster/pools/" + config.id
    for credential in tokens.values():
        assert client.get("/cluster/pools", headers=headers(credential)).status_code == 403
        assert client.get(path + "/workers", headers=headers(credential)).status_code == 403
        assert client.patch(path, headers=headers(credential), json=body).status_code == 403
    admin = headers(token)
    assert client.patch(path, headers=admin, json={**body, "maximum": 5}).status_code == 422
    assert client.patch(path, headers=admin, json={**body, "minimum": 5}).status_code == 422
    assert client.patch(path, headers=admin, json={**body, "image": "arbitrary"}).status_code == 422
    for _ in range(2):
        assert client.patch(path, headers=admin, json=body).status_code == 200
    listed = client.get("/cluster/pools", headers=admin).json()
    assert listed[0]["hard_limit"] == 4 and "host_id" not in listed[0]
    assert "configuration_hash" not in listed[0]
    assert client.get(path + "/workers", headers=admin).json() == []
    assert client.patch("/cluster/pools/missing", headers=admin, json=body).status_code == 404
    with service.factory() as session:
        assert (
            len(
                list(
                    session.scalars(
                        select(AuditEvent).where(AuditEvent.action == "WORKER_POOL_UPDATED")
                    )
                )
            )
            == 1
        )


def test_retired_workers_cannot_register_or_resume_and_pool_shape_is_enforced(service):
    config, _ = registered_pool(service)
    worker_id = "elastic-policy-test"
    with service.factory.begin() as session:
        session.add(
            ProvisionedWorker(
                worker_id=worker_id,
                pool_id=config.id,
                phase="DRAINING",
                created_at=service.clock(),
                next_check_at=service.clock(),
            )
        )
    specification = {
        "worker_id": worker_id,
        "cpu_total": 1,
        "memory_total_mb": 1024,
        "capabilities": ["worker-python", "python"],
    }
    with pytest.raises(DomainError, match="allocation"):
        service.register(WorkerRegister(**{**specification, "cpu_total": 4}))
    row = service.register(WorkerRegister(**specification))
    assert row.status == "DRAINING"
    client, _, token, *_ = prepare(service)
    assert client.post(f"/workers/{worker_id}/resume", headers=headers(token)).status_code == 409
    with service.factory.begin() as session:
        session.get(ProvisionedWorker, worker_id).phase = "REMOVED"
    with pytest.raises(DomainError, match="retired"):
        service.register(WorkerRegister(**specification))


def test_configuration_requires_reserved_partition_immutable_image_and_safe_changes(service):
    with pytest.raises(ValidationError):
        PoolSettings(
            id="host",
            image="mutable:latest",
            rpc_target="rpc:50051",
            host_cpu_budget=1,
            host_memory_budget_mb=1024,
        )
    with pytest.raises(ValidationError, match="overhead"):
        PoolSettings(
            id="host",
            image="sha256:" + "a" * 64,
            rpc_target="rpc:50051",
            host_cpu_budget=1,
            host_memory_budget_mb=1024,
        )
    config, controller = registered_pool(service)
    with service.factory.begin() as session:
        session.add(
            ProvisionedWorker(
                worker_id="pending-allocation",
                pool_id=config.id,
                phase="REQUESTED",
                created_at=service.clock(),
                next_check_at=service.clock(),
            )
        )
    changed = PoolController(
        service, config.model_copy(update={"cpu_per_worker": 2}), controller.adapter
    )
    with pytest.raises(DomainError, match="drain"), service.factory.begin() as session:
        changed.register(session)
    other = PoolController(
        service, config.model_copy(update={"id": "second-host"}), controller.adapter
    )
    with pytest.raises(DomainError, match="already assigned"), service.factory.begin() as session:
        other.register(session)
    assert PoolService(service).list_pools()[0]["enabled"] is False
    with pytest.raises(ValidationError):
        PoolUpdate(enabled=True, minimum=2, maximum=1)
