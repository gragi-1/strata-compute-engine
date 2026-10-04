import hashlib
import hmac
import json
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
from sqlalchemy import func, select

from control_plane.config import Settings
from control_plane.domain import JobStatus
from control_plane.models import WebhookDelivery
from control_plane.webhooks import WebhookService, WebhookSubmit
from tests.helpers import submit
from tests.integration.test_identity import headers, prepare

SECRET = "synthetic-test-signing-secret-32-characters"


@pytest.fixture
def receiver():
    records, codes = [], deque()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            data = self.rfile.read(int(self.headers["Content-Length"]))
            timestamp = self.headers["X-Strata-Timestamp"]
            signature = hmac.new(
                SECRET.encode(), timestamp.encode() + b"." + data, hashlib.sha256
            ).hexdigest()
            assert self.headers["X-Strata-Signature"] == "sha256=" + signature
            records.append((dict(self.headers), json.loads(data)))
            self.send_response(codes.popleft() if codes else 204)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}/receive", records, codes
    server.shutdown()
    server.server_close()
    thread.join(timeout=5)


def configured(service, receiver):
    service.settings.webhook_targets = {"local-test": receiver[0]}
    service.settings.webhook_secrets = {"local-test": SECRET}
    dispatcher = WebhookService(service)
    webhook = dispatcher.create(
        WebhookSubmit(name="Experiment completion", target="local-test", events=["JOB_CANCELLED"])
    )
    return dispatcher, webhook


def test_outbox_is_atomic_and_signed_retry_preserves_delivery_identity(service, receiver):
    dispatcher, webhook = configured(service, receiver)
    job = submit(service)
    with pytest.raises(RuntimeError), service.factory.begin() as session:
        row = service.job(session, job.id, lock=True)
        service.transition(session, row, JobStatus.CANCELLED, service.now(session))
        raise RuntimeError("transaction failure")
    assert service.get_job(job.id).status == "QUEUED"
    with service.factory() as session:
        assert session.scalar(select(func.count()).select_from(WebhookDelivery)) == 0
    service.cancel(job.id)
    service.cancel(job.id)
    receiver[2].extend([503, 204])
    assert dispatcher.tick() == 1
    assert dispatcher.tick() == 0
    with service.factory() as session:
        row = session.scalar(select(WebhookDelivery))
        assert row.status == "PENDING" and row.attempts == 1 and row.last_status == 503
    service.clock.advance(2)
    assert dispatcher.tick() == 1
    assert len(receiver[1]) == 2
    assert receiver[1][0][0]["X-Strata-Delivery"] == receiver[1][1][0]["X-Strata-Delivery"]
    assert receiver[1][0][1] == receiver[1][1][1]
    assert receiver[1][0][1]["job"] == {"id": job.id, "status": "CANCELLED", "campaign_id": None}
    with service.factory() as session:
        row = session.scalar(select(WebhookDelivery))
        assert row.status == "DELIVERED" and row.attempts == 2 and row.lease_token is None


def test_expired_dispatch_claim_recovers_and_fences_old_writer(service, receiver):
    dispatcher, _ = configured(service, receiver)
    service.cancel(submit(service).id)
    old = dispatcher.claim()
    assert dispatcher.claim() is None
    service.clock.advance(31)
    recovered = dispatcher.claim()
    assert recovered.id == old.id and recovered.lease_token != old.lease_token
    dispatcher.finish(old, 204, None)
    with service.factory() as session:
        assert session.get(WebhookDelivery, old.id).status == "SENDING"
    dispatcher.deliver(recovered)
    with service.factory() as session:
        assert session.get(WebhookDelivery, old.id).status == "DELIVERED"


def test_dead_letter_bounded_retries_and_explicit_redelivery(service, receiver):
    service.settings.webhook_attempt_limit = 1
    dispatcher, webhook = configured(service, receiver)
    service.cancel(submit(service).id)
    receiver[2].append(400)
    dispatcher.tick()
    with service.factory() as session:
        row = session.scalar(select(WebhookDelivery))
        assert row.status == "DEAD" and row.last_status == 400
    assert dispatcher.retry(webhook.id, row.id).attempt_limit == 2
    dispatcher.tick()
    with service.factory() as session:
        assert session.get(WebhookDelivery, row.id).status == "DELIVERED"
    # Crash loops cannot bypass the configured attempt budget.
    service.cancel(submit(service).id)
    claimed = dispatcher.claim()
    service.clock.advance(31)
    assert dispatcher.claim() is None
    with service.factory() as session:
        assert session.get(WebhookDelivery, claimed.id).status == "DEAD"


def test_private_project_admin_controls_and_owner_disable_prevent_delivery(service, receiver):
    service.settings.webhook_targets = {"local-test": receiver[0]}
    service.settings.webhook_secrets = {"local-test": SECRET}
    client, root, token, alpha, beta, _, tokens = prepare(service)
    admin, alice, bob = (
        headers(token, alpha),
        headers(tokens["alice"], alpha),
        headers(tokens["bob"], beta),
    )
    body = {"name": "Private completion", "target": "local-test", "events": ["JOB_CANCELLED"]}
    assert client.post("/webhooks", headers=alice, json=body).status_code == 403
    response = client.post("/webhooks", headers=admin, json=body)
    assert response.status_code == 201, response.text
    webhook = response.json()
    assert SECRET not in response.text
    assert client.get("/webhooks", headers=bob).json() == []
    assert client.get(f"/webhooks/{webhook['id']}/deliveries", headers=bob).status_code == 404
    job = client.post(
        "/jobs",
        headers=alice,
        json={"name": "Private", "image": "strata/python-workloads:local", "command": ["true"]},
    ).json()
    client.post(f"/jobs/{job['id']}/cancel", headers=alice)
    assert (
        "lease_token" not in client.get(f"/webhooks/{webhook['id']}/deliveries", headers=admin).text
    )
    dispatcher = WebhookService(service)
    dispatcher.action(webhook["id"], False)
    dispatcher.tick()
    assert receiver[1] == []
    with service.factory() as session:
        assert session.scalar(select(WebhookDelivery)).status == "DEAD"


def test_webhook_target_configuration_rejects_credentials_and_production_http():
    for url in [
        "http://user:password@localhost/receive",
        "file:///tmp/receive",
        "https://example.com/#fragment",
    ]:
        with pytest.raises(ValueError, match="webhook targets"):
            Settings(webhook_targets={"receiver": url}, webhook_secrets={"receiver": SECRET})
    with pytest.raises(ValueError, match="webhook targets"):
        Settings(
            production=True,
            webhook_targets={"receiver": "http://example.com/receive"},
            webhook_secrets={"receiver": SECRET},
            api_keys={"x" * 32: "admin"},
            worker_token="x" * 32,
            tls_cert=Path("cert.pem"),
            tls_key=Path("key.pem"),
        )


@pytest.mark.postgres
def test_concurrent_dispatchers_claim_each_delivery_once(postgres_service, receiver):
    dispatcher, _ = configured(postgres_service, receiver)
    postgres_service.cancel(submit(postgres_service).id)
    barrier = threading.Barrier(2)

    def claim(_):
        barrier.wait(timeout=10)
        return WebhookService(postgres_service).claim()

    with ThreadPoolExecutor(max_workers=2) as pool:
        claims = [row for row in pool.map(claim, range(2)) if row is not None]
    assert len(claims) == 1
    dispatcher.deliver(claims[0])
    assert len(receiver[1]) == 1
