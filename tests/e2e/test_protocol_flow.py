"""Full REST -> scheduler -> real gRPC -> agent flow with a simulated container runtime.

The Docker deployment is validated separately by scripts/e2e_live.py in CI.
"""

import socket
from unittest.mock import MagicMock

from fastapi.testclient import TestClient

from control_plane.api import create_app
from control_plane.rpc.server import make_server
from scheduler.core import Scheduler
from worker.agent import Agent
from worker.executor import Result
from worker.transport import Transport


def test_submit_schedule_execute_result_with_real_protocol(service):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    server = make_server(service, f"127.0.0.1:{port}")
    server.start()
    transport = Transport(f"127.0.0.1:{port}", service.settings.worker_token)
    executor = MagicMock()
    executor.grace = 5
    container = MagicMock()
    container.logs.return_value = b"pi approximation finished"
    executor.create.return_value = container
    executor.inspect.return_value = Result("SUCCEEDED", 0, "exit code 0")
    executor.artifacts.return_value = [("result.json", b'{"pi":3.14159}')]
    agent = Agent(transport, executor, "e2e-worker", 2, 1024, service.settings)
    try:
        agent.register()
        with TestClient(create_app(service.settings, service)) as client:
            job = client.post(
                "/jobs",
                json={
                    "name": "monte-carlo",
                    "image": "strata/python-workloads:local",
                    "command": ["python", "/app/main.py", "monte-carlo"],
                },
            ).json()
            Scheduler(service).tick()
            agent.tick()
            agent.tick()
            assert client.get(f"/jobs/{job['id']}").json()["status"] == "SUCCEEDED"
            assert "pi approximation" in client.get(f"/jobs/{job['id']}/logs").text
            artifacts = client.get(f"/jobs/{job['id']}/artifacts").json()
            assert client.get(artifacts[0]["uri"]).json() == {"pi": 3.14159}
            container.start.assert_called_once()
    finally:
        transport.channel.close()
        server.stop(0).wait()
