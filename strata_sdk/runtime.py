"""Dependency-free workload IPC client and stateful Python kernel.

Strata installs this file in /output/.strata/runtime.py for managed runtimes.
The agent relays bounded messages using its own lease; workload credentials and
network access are unnecessary. Copy this module into custom Python workloads or
import it from the installed runtime directory.
"""

import base64
import contextlib
import io
import json
import math
import os
import subprocess
import sys
import time
import traceback
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path("/output/.strata")
MESSAGE_BYTES = 16384


def exchange(payload: dict[str, Any], timeout: float = 300) -> dict[str, Any]:
    if timeout <= 0:
        raise ValueError("runtime timeout must be positive")
    ROOT.mkdir(exist_ok=True)
    data = json.dumps(payload, allow_nan=False, ensure_ascii=False).encode()
    if len(data) > MESSAGE_BYTES:
        raise ValueError("runtime message exceeds 16 KiB")
    temporary = ROOT / "request.tmp"
    temporary.write_bytes(data)
    temporary.replace(ROOT / "request.json")
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            response = json.loads((ROOT / "reply.json").read_bytes()[: MESSAGE_BYTES + 1])
            if response.get("sequence") == payload["sequence"]:
                if response.get("error"):
                    raise RuntimeError(response["error"])
                return dict(response)
        except (FileNotFoundError, json.JSONDecodeError):
            pass
        time.sleep(0.05)
    raise TimeoutError("runtime exchange timed out")


class Collective:
    """Ordered barriers and vector reductions across the entire execution group."""

    def __init__(self) -> None:
        context = json.loads(os.environ["STRATA_RUNTIME_CONTEXT"])
        if context["kind"] != "collective":
            raise ValueError("this job is not a collective participant")
        self.rank, self.size, self.group_id = context["rank"], context["size"], context["group_id"]
        self.sequence = 0

    def reduce(
        self, values: list[float], operation: str = "sum", timeout: float = 300
    ) -> list[float]:
        if operation not in {"sum", "min", "max"}:
            raise ValueError("operation must be sum, min or max")
        if not 1 <= len(values) <= 256 or not all(math.isfinite(v) for v in values):
            raise ValueError("reduce requires 1..256 finite numeric values")
        reply = exchange(
            {
                "kind": "collective",
                "sequence": self.sequence,
                "operation": operation,
                "values": values,
            },
            timeout,
        )
        self.sequence += 1
        return list(reply["values"])

    def barrier(self, timeout: float = 300) -> None:
        exchange(
            {"kind": "collective", "sequence": self.sequence, "operation": "barrier", "values": []},
            timeout,
        )
        self.sequence += 1


class BoundedText(io.TextIOBase):
    """Bound captured output before execution finishes, including runaway prints."""

    def __init__(self, limit: int = 4096) -> None:
        self.limit, self.value = limit, ""

    def write(self, content: str) -> int:
        room = max(0, self.limit - len(self.value.encode()))
        self.value += content.encode()[:room].decode(errors="ignore")
        return len(content)

    def flush(self) -> None:
        pass


def kernel() -> None:
    """Persistent namespace; agent enforces session, idle and cell deadlines."""
    namespace: dict[str, Any] = {"__name__": "__strata_notebook__"}
    sequence, result = 0, None
    while True:
        reply = exchange({"kind": "session", "sequence": sequence, "result": result}, 604800)
        stdout, stderr = BoundedText(), BoundedText()
        error = None
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            try:
                exec(compile(reply["code"], "<strata-cell>", "exec"), namespace)
            except BaseException as exc:
                error = type(exc).__name__
                traceback.print_exc(limit=5)
        result = {
            "cell_id": reply["cell_id"],
            "stdout": stdout.value,
            "stderr": stderr.value,
            "error": error,
        }
        sequence += 1


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(
        self, req: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str
    ) -> None:
        return None


def service() -> None:
    """Relay bounded HTTP messages only to the configured container-local port."""
    context = json.loads(os.environ["STRATA_RUNTIME_CONTEXT"])
    process = subprocess.Popen(context["command"])
    opener = urllib.request.build_opener(NoRedirect(), urllib.request.ProxyHandler({}))
    sequence, result = 0, None
    try:
        while process.poll() is None:
            reply = exchange({"kind": "session", "sequence": sequence, "result": result}, 604800)
            request = urllib.request.Request(
                f"http://127.0.0.1:{context['port']}{reply['path']}",
                data=reply["body"].encode() if reply["body"] else None,
                method=reply["method"],
            )
            result = {"cell_id": reply["cell_id"], "error": None}
            try:
                try:
                    response = opener.open(request, timeout=2)
                except urllib.error.HTTPError as exc:
                    response = exc
                with response:
                    data = response.read(8193)
                    if len(data) > 8192:
                        raise ValueError("service response exceeds 8 KiB")
                    result.update(
                        status=response.status,
                        content_type=response.headers.get(
                            "Content-Type", "application/octet-stream"
                        )[:256],
                        body_base64=base64.b64encode(data).decode(),
                    )
            except (OSError, ValueError) as exc:
                result["error"] = type(exc).__name__
            sequence += 1
    finally:
        process.terminate()
        process.wait(timeout=3)


if __name__ == "__main__" and sys.argv[1:] == ["python"]:
    kernel()
elif __name__ == "__main__" and sys.argv[1:] == ["service"]:
    service()
