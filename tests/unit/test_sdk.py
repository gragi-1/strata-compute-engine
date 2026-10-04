import hashlib
from pathlib import Path

import httpx
import pytest

from strata_sdk import Client
from strata_sdk.transport import request_with_retry


def client_for(rows, content):
    def respond(request):
        if request.url.path.endswith("/artifacts"):
            return httpx.Response(200, json=rows)
        return httpx.Response(200, content=content)

    client = Client()
    client.http.close()
    client.http = httpx.Client(
        base_url="http://strata.test", transport=httpx.MockTransport(respond)
    )
    return client


def artifact(content, **changes):
    return {
        "name": "result.json",
        "attempt_id": "attempt-1",
        "uri": "/artifacts/artifact-1",
        "sha256": hashlib.sha256(content).hexdigest(),
        "size": len(content),
        **changes,
    }


def test_download_preserves_artifacts_from_multiple_attempts(tmp_path):
    content = b'{"value": 3}'
    rows = [artifact(content), artifact(content, attempt_id="attempt-2")]
    with client_for(rows, content) as client:
        assert client.artifacts("job", tmp_path) == rows
    assert (tmp_path / "attempt-attempt-1/result.json").read_bytes() == content
    assert (tmp_path / "attempt-attempt-2/result.json").read_bytes() == content


@pytest.mark.parametrize("changes", [{"name": "../escape"}, {"sha256": "wrong"}, {"size": 100}])
def test_invalid_download_keeps_existing_files_and_removes_partial_data(tmp_path, changes):
    target = Path(tmp_path) / "result.json"
    target.write_bytes(b"previous result")
    with (
        client_for([artifact(b"new data", **changes)], b"new data") as client,
        pytest.raises(ValueError),
    ):
        client.artifacts("job", tmp_path)
    assert target.read_bytes() == b"previous result"
    assert list(tmp_path.iterdir()) == [target]


@pytest.mark.parametrize(
    ("method", "path", "headers", "count"),
    [
        ("GET", "/jobs", {}, 4),
        ("POST", "/jobs", {}, 1),
        ("POST", "/jobs", {"Idempotency-Key": "persisted-key"}, 4),
        ("POST", "/auth/password", {"Idempotency-Key": "unsupported-key"}, 1),
        ("POST", "/projects", {"Idempotency-Key": "unsupported-key"}, 1),
        ("POST", "/dataset-files/id/statistics", {}, 4),
        ("PUT", "/uploads/id/chunks/0", {"X-Chunk-SHA256": "abc"}, 4),
    ],
)
def test_retry_boundaries_and_budget_never_repeat_unsupported_mutations(
    monkeypatch, method, path, headers, count
):
    calls = []
    monkeypatch.setattr("strata_sdk.transport.time.sleep", lambda _: None)

    def unavailable(request):
        calls.append((request.method, request.url.path, request.content))
        return httpx.Response(503, json={"detail": "unavailable"})

    with httpx.Client(
        base_url="http://strata.test", transport=httpx.MockTransport(unavailable)
    ) as client:
        result = request_with_retry(client, method, path, headers=headers, json={"stable": True})
        assert result.status_code == 503
        assert len(calls) == count
        assert len(set(calls)) == 1
        calls.clear()
        request_with_retry(client, "GET", "/jobs", retry_window_seconds=0)
        assert len(calls) == 1


def test_long_retry_after_and_streaming_bodies_are_not_automatically_replayed(monkeypatch):
    calls = []
    monkeypatch.setattr("strata_sdk.transport.time.sleep", lambda _: pytest.fail("unexpected wait"))

    def unavailable(request):
        calls.append(request)
        return httpx.Response(503, headers={"Retry-After": "30"}, json={"detail": "paused"})

    with httpx.Client(
        base_url="http://strata.test", transport=httpx.MockTransport(unavailable)
    ) as client:
        request_with_retry(client, "GET", "/jobs")
        assert len(calls) == 1
        request_with_retry(
            client,
            "POST",
            "/jobs",
            content=iter([b"one-shot"]),
            headers={"Idempotency-Key": "key"},
        )
        assert len(calls) == 2
