import httpx
import pytest

from scripts.e2e_live import wait_job


@pytest.mark.parametrize("status", ["QUEUED", "RUNNING"])
def test_job_wait_timeout_reports_the_last_observed_state(monkeypatch, status):
    times = iter([0, 0, 2])
    monkeypatch.setattr("scripts.e2e_live.time.monotonic", lambda: next(times))
    monkeypatch.setattr("scripts.e2e_live.time.sleep", lambda _: None)
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json={"status": status}))
    with (
        httpx.Client(base_url="http://probe.invalid", transport=transport) as client,
        pytest.raises(TimeoutError, match=f"last observed status: {status}"),
    ):
        wait_job(client, "test-job", timeout=1)
