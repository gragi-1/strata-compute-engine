"""A small synchronous client for scripts, notebooks and reproducible studies."""

import hashlib
import os
import re
import tempfile
import time
from collections import Counter
from contextlib import suppress
from pathlib import Path
from typing import Any, cast

import httpx


class Client:
    def __init__(self, url: str | None = None, api_key: str | None = None) -> None:
        key = api_key or os.getenv("STRATA_API_KEY")
        api_url: str = url or os.environ.get("STRATA_API_URL") or "http://localhost:8000"
        self.http = httpx.Client(
            base_url=api_url,
            headers={"Authorization": f"Bearer {key}"} if key else {},
            timeout=60,
        )

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *args: Any) -> None:
        self.http.close()

    def request(self, method: str, path: str, **kwargs: Any) -> Any:
        response = self.http.request(method, path, **kwargs)
        response.raise_for_status()
        return response.json()

    def request_object(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        value = self.request(method, path, **kwargs)
        if not isinstance(value, dict):
            raise ValueError("expected a JSON object from Strata")
        return value

    def submit(self, specification: dict[str, Any], key: str | None = None) -> dict[str, Any]:
        return self.request_object(
            "POST", "/jobs", json=specification, headers={"Idempotency-Key": key} if key else {}
        )

    def campaign(self, specification: dict[str, Any], key: str | None = None) -> dict[str, Any]:
        return self.request_object(
            "POST",
            "/campaigns",
            json=specification,
            headers={"Idempotency-Key": key} if key else {},
        )

    def workflow(self, specification: dict[str, Any], key: str | None = None) -> dict[str, Any]:
        return self.request_object(
            "POST",
            "/workflows",
            json=specification,
            headers={"Idempotency-Key": key} if key else {},
        )

    def wait(
        self, identifier: str, campaign: bool = False, timeout: float = 3600
    ) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        path = f"/{'campaigns' if campaign else 'jobs'}/{identifier}"
        while time.monotonic() < deadline:
            result = self.request_object("GET", path)
            if result["status"] in {"COMPLETED", "SUCCEEDED", "FAILED", "TIMED_OUT", "CANCELLED"}:
                return result
            time.sleep(0.5)
        raise TimeoutError(f"Strata wait timeout: {identifier}")

    def dataset(self, name: str, files: list[Path], label: str = "v1") -> dict[str, Any]:
        dataset = self.request("POST", "/datasets", json={"name": name})
        version = self.request("POST", f"/datasets/{dataset['id']}/versions", json={"label": label})
        for file in files:
            with file.open("rb") as stream:
                row = self.request(
                    "PUT",
                    f"/dataset-versions/{version['id']}/files/{file.name}",
                    content=iter(lambda: stream.read(1024 * 1024), b""),
                )
            digest = hashlib.sha256()
            with file.open("rb") as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(chunk)
            if digest.hexdigest() != row["sha256"]:
                raise ValueError("dataset upload checksum mismatch")
        return self.request_object("POST", f"/dataset-versions/{version['id']}/seal")

    def artifacts(self, job_id: str, output: Path) -> list[dict[str, Any]]:
        output.mkdir(parents=True, exist_ok=True)
        rows: list[dict[str, Any]] = []
        while True:
            page = self.request(
                "GET", f"/jobs/{job_id}/artifacts", params={"limit": 1000, "offset": len(rows)}
            )
            rows.extend(page)
            if len(page) < 1000:
                break
        names = Counter(row["name"] for row in rows)
        for row in rows:
            if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}", row["name"]):
                raise ValueError("invalid artifact file name")
            if not re.fullmatch(r"/artifacts/[a-zA-Z0-9_-]{1,128}", row["uri"]):
                raise ValueError("invalid artifact download URI")
            directory = output
            if names[row["name"]] > 1:
                if not re.fullmatch(r"[a-zA-Z0-9_-]{1,128}", row["attempt_id"]):
                    raise ValueError("invalid artifact attempt identifier")
                directory = output / ("attempt-" + row["attempt_id"])
                directory.mkdir(exist_ok=True)
            digest, size = hashlib.sha256(), 0
            with tempfile.NamedTemporaryFile(dir=directory, delete=False) as stream:
                temporary = Path(stream.name)
            try:
                with self.http.stream("GET", row["uri"]) as response:
                    response.raise_for_status()
                    with temporary.open("wb") as stream:
                        for chunk in response.iter_bytes():
                            digest.update(chunk)
                            size += len(chunk)
                            stream.write(chunk)
                if digest.hexdigest() != row["sha256"] or size != row["size"]:
                    raise ValueError("artifact download checksum or size mismatch")
                temporary.replace(directory / row["name"])
            finally:
                with suppress(FileNotFoundError):
                    temporary.unlink()
        return rows

    def results(self, campaign_id: str) -> list[dict[str, Any]]:
        return cast(list[dict[str, Any]], self.request("GET", f"/campaigns/{campaign_id}/results"))
