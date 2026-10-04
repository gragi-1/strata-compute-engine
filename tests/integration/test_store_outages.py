"""Bounded real socket failures and rollback on an explicitly owned PostgreSQL session."""

import json
import socket
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from time import monotonic

import grpc
import httpx
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import OperationalError

from control_plane.api import create_app
from control_plane.database import make_engine, sessions
from control_plane.models import Job
from control_plane.rpc import engine_pb2 as pb
from control_plane.rpc import engine_pb2_grpc as rpc
from control_plane.rpc.server import make_server
from strata_sdk import Client
from tests.helpers import submit

pytestmark = pytest.mark.postgres


def test_read_only_postgres_connection_is_not_a_ready_writer(postgres_service):
    svc = postgres_service
    original = svc.factory
    with svc.factory() as session:
        schema = session.scalar(text("SELECT current_schema()"))
    address = make_url(svc.settings.database_url).update_query_dict(
        {
            "options": f"-csearch_path={schema} -cdefault_transaction_read_only=on",
            "connect_timeout": "2",
        }
    )
    engine = make_engine(address.render_as_string(hide_password=False))
    try:
        with pytest.raises(OperationalError), engine.connect():
            pytest.fail("a read-only session must not be accepted as the default writer")
    finally:
        engine.dispose()
    # Even an explicit operator override cannot make read-only readiness succeed.
    engine = make_engine(
        address.update_query_dict({"target_session_attrs": "any"}).render_as_string(
            hide_password=False
        )
    )
    try:
        svc.factory = sessions(engine)
        with TestClient(create_app(svc.settings, svc)) as client:
            assert client.get("/ready").status_code == 503
    finally:
        svc.factory = original
        engine.dispose()


@contextmanager
def silent_postgres_peer():
    """Accept TCP but deliberately never answer libpq's SSL/startup handshake."""
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    listener.settimeout(0.1)
    sockets = []
    stopped = threading.Event()

    def accept():
        while not stopped.is_set():
            try:
                connection, _ = listener.accept()
                sockets.append(connection)
            except TimeoutError:
                continue
            except OSError:
                break

    thread = threading.Thread(target=accept)
    thread.start()
    try:
        yield listener.getsockname()[1]
    finally:
        stopped.set()
        thread.join(timeout=2)
        listener.close()
        for connection in sockets:
            connection.close()
        assert not thread.is_alive()


def test_postgres_handshake_has_default_timeout_and_preserves_operator_override():
    with silent_postgres_peer() as port:
        engine = make_engine(f"postgresql+psycopg://test@127.0.0.1:{port}/test")
        assert engine.url.query["connect_timeout"] == "5"
        started = monotonic()
        try:
            with pytest.raises(OperationalError), engine.connect():
                pytest.fail("the deliberately silent peer must never establish a session")
            assert 4 <= monotonic() - started < 9
        finally:
            engine.dispose()
        overridden = make_engine(
            f"postgresql+psycopg://test@127.0.0.1:{port}/test?connect_timeout=2"
        )
        try:
            assert overridden.url.query["connect_timeout"] == "2"
        finally:
            overridden.dispose()


def test_api_and_rpc_outage_fail_closed_without_disclosing_sql_or_credentials(
    postgres_service, capsys
):
    svc = postgres_service
    job = submit(svc, name="Durable pre-outage job")
    original_factory = svc.factory
    with silent_postgres_peer() as port:
        engine = make_engine(
            f"postgresql+psycopg://test:synthetic-hidden-password@127.0.0.1:{port}"
            "/test?connect_timeout=2"
        )
        svc.factory = sessions(engine)
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            rpc_port = sock.getsockname()[1]
        server = make_server(svc, f"127.0.0.1:{rpc_port}")
        server.start()
        channel = grpc.insecure_channel(f"127.0.0.1:{rpc_port}")
        try:
            for identity in (False, True):
                svc.settings.identity_enabled = identity
                with TestClient(create_app(svc.settings, svc)) as client:
                    # Legacy project lookup and native authentication are middleware paths.
                    paths = ["/jobs", "/metrics"] if not identity else ["/jobs"]
                    for path in paths:
                        started = monotonic()
                        response = client.get(path, headers={"Authorization": "Bearer abc"})
                        assert monotonic() - started < 6
                        assert response.status_code == 503
                        assert response.headers["Retry-After"] == "2"
                        assert response.json()["detail"].startswith("durable store")
                    if identity:
                        # Login is public: the exception originates inside its endpoint.
                        response = client.post(
                            "/auth/login",
                            json={"username": "qa", "password": "synthetic-hidden-password"},
                        )
                        assert response.status_code == 503
                        assert response.headers["Retry-After"] == "2"
            with pytest.raises(grpc.RpcError) as error:
                rpc.WorkerControlStub(channel).Cluster(
                    pb.Empty(),
                    timeout=6,
                    metadata=[("authorization", f"Bearer {svc.settings.worker_token}")],
                )
            assert error.value.code() == grpc.StatusCode.UNAVAILABLE
            assert error.value.details() == "durable store is temporarily unavailable"
            captured = capsys.readouterr()
            diagnostic_output = captured.out + captured.err
            assert "store_failed" in diagnostic_output
            assert "synthetic-hidden-password" not in diagnostic_output
            assert "SELECT" not in diagnostic_output
        finally:
            channel.close()
            server.stop(0).wait()
            engine.dispose()
            svc.factory = original_factory
            svc.settings.identity_enabled = False
    assert svc.get_job(job.id).name == "Durable pre-outage job"
    with TestClient(create_app(svc.settings, svc)) as client:
        assert client.get("/jobs").status_code == 200
    with svc.factory() as session:
        assert len(list(session.scalars(select(Job)))) == 1


def test_terminated_own_postgres_transaction_rolls_back_and_engine_recovers(postgres_service):
    svc = postgres_service
    job = submit(svc, name="Committed job")
    engine = svc.factory.kw["bind"]
    with engine.connect() as connection:
        pid = connection.scalar(text("SELECT pg_backend_pid()"))
        connection.execute(
            text("UPDATE jobs SET name=:name WHERE id=:id"),
            {"name": "Uncommitted secret job", "id": job.id},
        )
        with engine.connect() as killer:
            # Only this checked-out fixture connection is terminated, never a server/process.
            assert killer.scalar(text("SELECT pg_terminate_backend(:pid)"), {"pid": pid})
        with pytest.raises(OperationalError):
            connection.execute(text("SELECT 1"))
    assert svc.get_job(job.id).name == "Committed job"
    assert submit(svc, name="Post-reconnection job").status == "QUEUED"


def test_sdk_and_cli_replay_lost_committed_response_but_never_repeat_unkeyed_create(
    postgres_service, tmp_path, monkeypatch
):
    from typer.testing import CliRunner

    from cli.main import app as cli

    svc = postgres_service
    replies, received_keys = [], []
    lose_reply = threading.Event()
    with TestClient(create_app(svc.settings, svc)) as api:

        class Proxy(BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers["Content-Length"]))
                headers = {}
                key = self.headers.get("Idempotency-Key")
                if key:
                    headers["Idempotency-Key"] = key
                received_keys.append(key)
                response = api.post(self.path, json=json.loads(body), headers=headers)
                replies.append(response.json())
                if lose_reply.is_set():
                    lose_reply.clear()
                    # The PostgreSQL commit has finished; deliberately discard its HTTP reply.
                    self.close_connection = True
                    self.connection.shutdown(socket.SHUT_RDWR)
                    self.connection.close()
                    return
                content = response.content
                self.send_response(response.status_code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(content)))
                self.end_headers()
                self.wfile.write(content)

            def log_message(self, *_):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Proxy)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        url = f"http://127.0.0.1:{server.server_port}"
        spec = {
            "name": "Lost response",
            "image": svc.settings.allowed_images[0],
            "command": ["true"],
        }
        try:
            with Client(url=url) as sdk:
                lose_reply.set()
                result = sdk.submit(spec, "lost-sdk-response")
                assert result["id"] == replies[0]["id"]
                assert received_keys == ["lost-sdk-response", "lost-sdk-response"]
                lose_reply.set()
                with pytest.raises(httpx.RemoteProtocolError):
                    sdk.submit(spec)  # No key: the client must not duplicate a committed create.
                assert len(received_keys) == 3
            monkeypatch.setenv("STRATA_API_URL", url)
            monkeypatch.delenv("STRATA_PROJECT_ID", raising=False)
            file = tmp_path / "job.json"
            file.write_text(json.dumps(spec))
            lose_reply.set()
            result = CliRunner().invoke(
                cli, ["submit", str(file), "--idempotency-key", "lost-cli-response"]
            )
            assert result.exit_code == 0, result.output
            assert received_keys[-2:] == ["lost-cli-response", "lost-cli-response"]
            assert json.loads(result.output)["id"] == replies[-2]["id"]
            with svc.factory() as session:
                assert len(list(session.scalars(select(Job)))) == 3
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=3)
            assert not thread.is_alive()
