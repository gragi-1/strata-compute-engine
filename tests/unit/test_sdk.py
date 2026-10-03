import hashlib
from pathlib import Path

import httpx
import pytest

from strata_sdk import Client


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
