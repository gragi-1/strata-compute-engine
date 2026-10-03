import errno
import io
import socket
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from unittest.mock import patch

import grpc
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select

from control_plane.api import create_app
from control_plane.campaigns import CampaignService
from control_plane.config import Settings
from control_plane.datasets import DatasetService
from control_plane.models import Campaign, Job
from control_plane.rpc import engine_pb2 as pb
from control_plane.rpc.server import make_server
from control_plane.schemas import CampaignSubmit, JobSubmit, NamedResource, WorkflowSubmit
from control_plane.services import DomainError
from scheduler.core import Scheduler
from tests.helpers import register
from worker.transport import Transport


def spec(**changes):
    return {
        "name": "compute",
        "image": "strata/python-workloads:local",
        "command": ["true"],
        "max_retries": 0,
        **changes,
    }


def version(service, name="measurements.csv", content=b"x,y\n1,2\n3,4\n"):
    datasets = DatasetService(service)
    dataset = datasets.create(NamedResource(name="Measurements"))
    draft = datasets.version(dataset.id, "v1")
    file = datasets.upload(draft.id, name, [content])
    datasets.seal(draft.id)
    return draft, file


def test_dataset_lifecycle_streaming_preview_and_immutability(service):
    client = TestClient(create_app(service.settings, service))
    dataset = client.post("/datasets", json={"name": "Measurements"}).json()
    draft = client.post(f"/datasets/{dataset['id']}/versions", json={"label": "v1"}).json()
    path = f"/dataset-versions/{draft['id']}"
    assert client.post(path + "/seal").status_code == 409
    row = client.put(path + "/files/data.csv", content=b"x,y\n1,2\n3,4\n").json()
    assert client.put(path + "/files/data.csv", content=b"different").status_code == 409
    assert (
        client.put(path + "/files/data.csv", content=b"x,y\n1,2\n3,4\n").json()["id"] == row["id"]
    )
    assert client.get(f"/dataset-files/{row['id']}/preview?limit=1").json()["rows"] == [["1", "2"]]
    assert client.get(f"/dataset-files/{row['id']}").content == b"x,y\n1,2\n3,4\n"
    sealed = client.post(path + "/seal").json()
    assert sealed["status"] == "SEALED" and len(sealed["manifest_hash"]) == 64
    assert client.post(path + "/seal").json() == sealed
    assert client.put(path + "/files/other.csv", content=b"x").status_code == 409
    assert len(client.get("/datasets").json()) == 1
    assert client.get(f"/datasets/{dataset['id']}/versions").json()[0]["id"] == draft["id"]
    assert client.get(path + "/files").json()[0]["id"] == row["id"]
    for missing in [
        "/datasets/missing/versions",
        "/dataset-versions/missing/files",
        "/dataset-files/missing",
        "/dataset-files/missing/preview",
    ]:
        assert client.get(missing).status_code == 404


@pytest.mark.parametrize("kind", ["json", "npy", "parquet", "tsv"])
def test_bounded_native_previews(service, kind):
    stream = io.BytesIO()
    if kind == "npy":
        np.save(stream, np.array([[1, 2], [3, 4]]))
    elif kind == "parquet":
        pq.write_table(pa.table({"x": [1, 2], "y": [3, 4]}), stream)
    else:
        stream.write(b"[1,2,3]" if kind == "json" else b"x\ty\n1\t2\n")
    _, file = version(service, "data." + kind, stream.getvalue())
    preview = DatasetService(service).preview(file.id, 1)
    assert preview["format"] in {"json", "table", "numpy"}
    if kind == "npy":
        assert preview["shape"] == [2, 2] and preview["values"] == ["1"]
    if kind == "json":
        assert preview["value"] == [1] and preview["truncated"]


def test_dataset_validation_and_missing_bytes(service):
    ds = DatasetService(service)
    dataset = ds.create(NamedResource(name="test"))
    with pytest.raises(DomainError):
        ds.version("missing", "v1")
    draft = ds.version(dataset.id, "v1")
    for name in ["../file", ".hidden", "dir/file"]:
        with pytest.raises(DomainError):
            ds.upload(draft.id, name, [b"x"])
    service.settings.dataset_max_bytes = 3
    with pytest.raises(DomainError, match="limit"):
        ds.upload(draft.id, "data.csv", [b"xx", b"xx"])
    service.settings.dataset_max_bytes = 100
    with pytest.raises(DomainError):
        ds.upload("missing", "data.csv", [b"x"])
    file = ds.upload(draft.id, "data.csv", [b"x,y\n1,2\n"])
    service.settings.dataset_max_files = 1
    with pytest.raises(DomainError):
        ds.upload(draft.id, "other.csv", [b"x"])
    service.settings.preview_max_bytes = 2
    with pytest.raises(DomainError, match="preview"):
        ds.preview(file.id)
    (service.settings.artifact_root / file.sha256).unlink()
    with pytest.raises(DomainError, match="unavailable"):
        ds.file(file.id)
    assert not list(service.settings.artifact_root.glob("upload-*"))


def test_parquet_preview_nonfinite_decimal_and_binary_values(service):
    stream = io.BytesIO()
    pq.write_table(
        pa.table(
            {
                "value": [float("nan"), float("inf"), 3.5],
                "amount": [Decimal("1.25"), Decimal("2.50"), None],
                "binary": [b"\xff", b"\x00", None],
            }
        ),
        stream,
    )
    _, file = version(service, "native.parquet", stream.getvalue())
    client = TestClient(create_app(service.settings, service))
    response = client.get(f"/dataset-files/{file.id}/preview")
    assert response.status_code == 200
    assert response.json()["rows"] == [
        [None, "1.25", "hex:ff"],
        [None, "2.50", "hex:00"],
        [3.5, None, None],
    ]


def test_dataset_storage_exhaustion_returns_503_and_keeps_draft(service):
    datasets = DatasetService(service)
    dataset = datasets.create(NamedResource(name="Storage probe"))
    draft = datasets.version(dataset.id, "v1")
    client = TestClient(create_app(service.settings, service))
    with patch(
        "control_plane.resource_api.tempfile.TemporaryFile",
        side_effect=OSError(errno.ENOSPC, "full"),
    ):
        response = client.put(f"/dataset-versions/{draft.id}/files/data.csv", content=b"x\n1\n")
    assert response.status_code == 503 and "storage capacity" in response.json()["detail"]
    assert client.get(f"/dataset-versions/{draft.id}/files").json() == []


def test_campaign_retry_admission_is_all_or_none(service):
    campaigns = CampaignService(service)
    campaign, _ = campaigns.create(
        CampaignSubmit(name="Retry probe", template=spec(), matrix={"seed": [1, 2]}), None
    )
    jobs = campaigns.jobs(campaign.id)
    for job in jobs:
        service.cancel(job.id)
    service.submit(JobSubmit.model_validate(spec()))
    service.settings.queue_limit = 2
    with pytest.raises(DomainError, match="capacity"):
        campaigns.retry(campaign.id)
    assert all(service.get_job(job.id).status == "CANCELLED" for job in jobs)
    service.settings.queue_limit = 3
    assert campaigns.retry(campaign.id) == 2
    assert all(service.get_job(job.id).status == "QUEUED" for job in jobs)


def test_auth_roles_and_worker_drain_preserve_running_leases(service):
    viewer, operator, admin = "v" * 32, "o" * 32, "a" * 32
    service.settings.api_keys = {viewer: "viewer", operator: "operator", admin: "admin"}
    client = TestClient(create_app(service.settings, service))
    assert client.get("/").status_code == 200
    assert client.get("/health").status_code == 200
    assert client.get("/jobs").status_code == 401
    assert client.get("/jobs", headers={"Authorization": viewer}).status_code == 401
    headers = {"Authorization": f"Bearer {viewer}"}
    assert client.get("/session", headers=headers).json()["role"] == "viewer"
    assert client.get("/jobs", headers=headers).status_code == 200
    assert client.post("/jobs", json=spec(), headers=headers).status_code == 403
    headers = {"Authorization": f"Bearer {operator}"}
    assert client.post("/jobs", json=spec(), headers=headers).status_code == 201
    worker = register(service)
    assert client.post(f"/workers/{worker.id}/drain", headers=headers).status_code == 403
    headers = {"Authorization": f"Bearer {admin}"}
    assert (
        client.post(f"/workers/{worker.id}/drain", headers=headers).json()["status"] == "DRAINING"
    )
    assert Scheduler(service).schedule() == 0
    assert client.post(f"/workers/{worker.id}/resume", headers=headers).status_code == 200
    assert Scheduler(service).schedule() == 1
    assignment = service.assignments(worker.id, worker.session_id)[0]
    client.post(f"/workers/{worker.id}/drain", headers=headers)
    service.start(assignment["attempt_id"], worker.session_id, assignment["lease_token"])
    service.clock.advance(16)
    Scheduler(service).recover()
    assert client.post(f"/workers/{worker.id}/resume", headers=headers).status_code == 409


def test_production_config_fails_closed():
    with pytest.raises(ValueError):
        Settings(production=True)
    with pytest.raises(ValueError):
        Settings(api_keys={"short": "admin"})
    with pytest.raises(ValueError):
        Settings(api_keys={"x" * 32: "unknown"})
    with pytest.raises(ValueError):
        Settings(tls_cert="certificate.pem")


def test_campaign_atomic_admission_idempotency_and_exports(service):
    client = TestClient(create_app(service.settings, service))
    body = {
        "name": "Sweep",
        "template": spec(command=["echo", "${size}", "${repeat}"]),
        "matrix": {"size": [10, 20]},
        "repeats": 2,
    }
    service.settings.queue_limit = 3
    assert client.post("/campaigns", json=body).status_code == 429
    with service.factory() as session:
        assert session.scalar(select(func.count()).select_from(Campaign)) == 0
        assert session.scalar(select(func.count()).select_from(Job)) == 0
    service.settings.queue_limit = 10
    response = client.post("/campaigns", json=body, headers={"Idempotency-Key": "sweep"})
    row = response.json()
    assert response.status_code == 201 and row["total"] == 4
    assert (
        client.post("/campaigns", json=body, headers={"Idempotency-Key": "sweep"}).status_code
        == 200
    )
    assert (
        client.post(
            "/campaigns", json=body | {"name": "Different"}, headers={"Idempotency-Key": "sweep"}
        ).status_code
        == 409
    )
    jobs = client.get(f"/campaigns/{row['id']}/jobs").json()
    assert sorted(tuple(j["command"][1:]) for j in jobs) == [
        ("10", "0"),
        ("10", "1"),
        ("20", "0"),
        ("20", "1"),
    ]
    assert client.get(f"/campaigns/{row['id']}/results?format=csv").text.startswith("attempts,")
    assert len(client.get(f"/campaigns/{row['id']}/results").json()) == 4
    assert len(client.get("/campaigns").json()) == 1
    action = client.post(f"/campaigns/{row['id']}/cancel").json()
    assert action["campaign"]["completed"] == 4
    assert client.post(f"/campaigns/{row['id']}/retry").json()["processed"] == 4
    assert client.post(f"/campaigns/{row['id']}/unknown").status_code == 404
    body["template"]["command"] = ["${unknown}"]
    assert client.post("/campaigns", json=body).status_code == 422
    service.settings.campaign_max_jobs = 1
    assert client.post("/campaigns", json=body).status_code == 413


def test_workflow_acyclic_atomic_dependency_gating_and_failure_propagation(service):
    cs = CampaignService(service)
    for nodes in [
        {"a": spec(depends_on=["b"])},
        {"a": spec(depends_on=["b"]), "b": spec(depends_on=["a"])},
    ]:
        with pytest.raises(DomainError):
            cs.workflow(WorkflowSubmit(name="bad", nodes=nodes), None)
    workflow = WorkflowSubmit(
        name="Pipeline", nodes={"first": spec(), "second": spec(depends_on=["first"])}
    )
    campaign, _ = cs.workflow(workflow, "pipeline")
    assert cs.workflow(workflow, "pipeline")[1] is False
    jobs = {j.parameters["node"]: j for j in cs.jobs(campaign.id)}
    register(service)
    assert Scheduler(service).schedule() == 1
    assert service.get_job(jobs["second"].id).status == "QUEUED"
    service.cancel(jobs["first"].id)
    Scheduler(service).schedule()
    assert service.get_job(jobs["second"].id).status == "FAILED"
    service.retry(jobs["first"].id)
    service.retry(jobs["second"].id)
    assert Scheduler(service).schedule() == 1
    with pytest.raises(DomainError):
        cs.get("missing")


def test_real_rpc_input_range_authorization_and_expired_leases(service):
    draft, file = version(service, content=b"a" * 150000)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    server = make_server(service, f"127.0.0.1:{port}")
    server.start()
    transport = Transport(f"127.0.0.1:{port}", service.settings.worker_token)
    try:
        worker = register(service)
        transport.session_id = worker.session_id
        job, _ = service.submit(
            JobSubmit.model_validate(spec(inputs=[{"version_id": draft.id, "alias": "data"}]))
        )
        Scheduler(service).schedule()
        credentials = transport.credentials(
            pb.Assignment(**service.assignments(worker.id, worker.session_id)[0])
        )
        assert transport.call("InputManifest", credentials).files[0].sha256 == file.sha256
        request = pb.ReadInputRequest(
            credentials=credentials, sha256=file.sha256, offset=10, max_bytes=100000
        )
        assert b"".join(c.content for c in transport.call("ReadInput", request)) == b"a" * 100000
        request.sha256 = "unknown"
        with pytest.raises(grpc.RpcError) as exc:
            list(transport.call("ReadInput", request))
        assert exc.value.code() == grpc.StatusCode.NOT_FOUND
        request.sha256 = file.sha256
        request.offset = 200000
        with pytest.raises(grpc.RpcError):
            list(transport.call("ReadInput", request))
        request.offset = -1
        with pytest.raises(grpc.RpcError):
            list(transport.call("ReadInput", request))
        service.clock.advance(31)
        with pytest.raises(grpc.RpcError) as exc:
            transport.call("InputManifest", credentials)
        assert exc.value.code() == grpc.StatusCode.FAILED_PRECONDITION
        assert service.get_job(job.id).inputs[0]["version_id"] == draft.id
    finally:
        transport.channel.close()
        server.stop(0).wait()


@pytest.mark.postgres
def test_concurrent_campaign_admission_cannot_oversubscribe(postgres_service):
    service = postgres_service
    service.settings.queue_limit = 3
    request = CampaignSubmit(name="sweep", template=spec(), matrix={"seed": [1, 2]})

    def create(_):
        try:
            return CampaignService(service).create(request, None)[0].id
        except DomainError as exc:
            return exc.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(create, range(2)))
    assert sum(isinstance(result, str) for result in results) == 1
    assert 429 in results
    assert len(service.list_jobs(None, 100, 0)) == 2
