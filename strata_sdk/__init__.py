"""A small synchronous client for scripts, notebooks and reproducible studies."""

import hashlib
import os
import re
import tempfile
import time
from collections import Counter
from collections.abc import Iterator
from contextlib import suppress
from pathlib import Path
from typing import Any, cast

import httpx

from strata_sdk.transport import request_with_retry


class Client:
    def __init__(
        self,
        url: str | None = None,
        api_key: str | None = None,
        *,
        access_token: str | None = None,
        project_id: str | None = None,
        max_retries: int = 3,
        retry_window_seconds: float = 10,
    ) -> None:
        key = (
            access_token
            or api_key
            or os.getenv("STRATA_ACCESS_TOKEN")
            or os.getenv("STRATA_API_KEY")
        )
        project = project_id or os.getenv("STRATA_PROJECT_ID")
        api_url: str = url or os.environ.get("STRATA_API_URL") or "http://localhost:8000"
        headers = {"Authorization": f"Bearer {key}"} if key else {}
        if project:
            headers["X-Strata-Project"] = project
        self.http = httpx.Client(
            base_url=api_url,
            headers=headers,
            timeout=60,
        )
        self.max_retries, self.retry_window_seconds = max_retries, retry_window_seconds

    def login(self, username: str, password: str) -> dict[str, Any]:
        session = self.request_object(
            "POST", "/auth/login", json={"username": username, "password": password}
        )
        self.http.headers["Authorization"] = "Bearer " + session["access_token"]
        return session

    def logout(self) -> None:
        response = self.http.post("/auth/logout")
        response.raise_for_status()
        self.http.headers.pop("Authorization", None)

    def projects(self) -> list[dict[str, Any]]:
        return cast(list[dict[str, Any]], self.request("GET", "/projects"))

    def oidc_identities(self) -> list[dict[str, Any]]:
        return cast(list[dict[str, Any]], self.request("GET", "/auth/oidc/identities"))

    def operations(self) -> list[dict[str, Any]]:
        return cast(list[dict[str, Any]], self.request("GET", "/operations"))

    def cluster_admission(self) -> dict[str, Any]:
        return self.request_object("GET", "/cluster/admission")

    def worker_pools(self) -> list[dict[str, Any]]:
        return cast(list[dict[str, Any]], self.request("GET", "/cluster/pools"))

    def execution_config(self) -> dict[str, Any]:
        return self.request_object("GET", "/execution/config")

    def pool_workers(self, pool_id: str) -> list[dict[str, Any]]:
        return cast(list[dict[str, Any]], self.request("GET", f"/cluster/pools/{pool_id}/workers"))

    def update_worker_pool(
        self, pool_id: str, *, enabled: bool, minimum: int, maximum: int
    ) -> dict[str, Any]:
        return self.request_object(
            "PATCH",
            f"/cluster/pools/{pool_id}",
            json={
                "enabled": enabled,
                "minimum": minimum,
                "maximum": maximum,
            },
        )

    def update_admission(
        self, *, accepting_jobs: bool, scheduling_enabled: bool, reason: str = ""
    ) -> dict[str, Any]:
        return self.request_object(
            "PATCH",
            "/cluster/admission",
            json={
                "accepting_jobs": accepting_jobs,
                "scheduling_enabled": scheduling_enabled,
                "reason": reason,
            },
        )

    def worker_gpus(self, worker_id: str) -> list[dict[str, Any]]:
        return cast(list[dict[str, Any]], self.request("GET", f"/workers/{worker_id}/gpus"))

    def compute_groups(self) -> list[dict[str, Any]]:
        return cast(list[dict[str, Any]], self.request("GET", "/compute-groups"))

    def submit_group(self, specification: dict[str, Any], key: str | None = None) -> dict[str, Any]:
        return self.request_object(
            "POST",
            "/compute-groups",
            json=specification,
            headers={"Idempotency-Key": key} if key else {},
        )

    def group(self, group_id: str) -> dict[str, Any]:
        return self.request_object("GET", f"/compute-groups/{group_id}")

    def cancel_group(self, group_id: str) -> dict[str, Any]:
        return self.request_object("POST", f"/compute-groups/{group_id}/cancel")

    def retry_group(self, group_id: str, key: str) -> dict[str, Any]:
        return self.request_object(
            "POST", f"/compute-groups/{group_id}/retry", headers={"Idempotency-Key": key}
        )

    def interactive_sessions(self) -> list[dict[str, Any]]:
        return cast(list[dict[str, Any]], self.request("GET", "/interactive-sessions"))

    def create_session(
        self, specification: dict[str, Any], key: str | None = None
    ) -> dict[str, Any]:
        return self.request_object(
            "POST",
            "/interactive-sessions",
            json=specification,
            headers={"Idempotency-Key": key} if key else {},
        )

    def session(self, session_id: str) -> dict[str, Any]:
        return self.request_object("GET", f"/interactive-sessions/{session_id}")

    def execute_cell(
        self, session_id: str, code: str, key: str, timeout: int = 30
    ) -> dict[str, Any]:
        return self.request_object(
            "POST",
            f"/interactive-sessions/{session_id}/cells",
            json={"code": code, "timeout_seconds": timeout},
            headers={"Idempotency-Key": key},
        )

    def session_cells(self, session_id: str) -> list[dict[str, Any]]:
        return cast(
            list[dict[str, Any]], self.request("GET", f"/interactive-sessions/{session_id}/cells")
        )

    def service_request(
        self, session_id: str, key: str, *, path: str = "/", method: str = "GET", body: str = ""
    ) -> dict[str, Any]:
        return self.request_object(
            "POST",
            f"/interactive-sessions/{session_id}/proxy",
            json={"path": path, "method": method, "body": body},
            headers={"Idempotency-Key": key},
        )

    def stop_session(self, session_id: str) -> dict[str, Any]:
        return self.request_object("POST", f"/interactive-sessions/{session_id}/stop")

    def session_cell(self, session_id: str, cell_id: str) -> dict[str, Any]:
        return self.request_object("GET", f"/interactive-sessions/{session_id}/cells/{cell_id}")

    def wait_cell(self, session_id: str, cell_id: str, timeout: float = 600) -> dict[str, Any]:
        if timeout <= 0:
            raise ValueError("cell watch timeout must be positive")
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            cell = self.session_cell(session_id, cell_id)
            if cell["status"] in {"SUCCEEDED", "FAILED"}:
                return cell
            time.sleep(0.5)
        raise TimeoutError("session cell did not finish")

    def export_notebook(self, session_id: str, output: Path) -> None:
        output.write_bytes(
            self.request_response("GET", f"/interactive-sessions/{session_id}/notebook").content
        )

    def log_snapshot(self, job_id: str, cursor: str = "") -> dict[str, Any]:
        return self.request_object("GET", f"/jobs/{job_id}/log-snapshot", params={"cursor": cursor})

    def follow_logs(self, job_id: str, timeout: float = 3600) -> Iterator[dict[str, Any]]:
        if timeout <= 0:
            raise ValueError("log timeout must be positive")
        deadline, cursor = time.monotonic() + timeout, ""
        while time.monotonic() < deadline:
            snapshot = self.log_snapshot(job_id, cursor)
            if snapshot["changed"]:
                cursor = snapshot["revision"]
                yield snapshot
            if snapshot["status"] in {"SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT"}:
                return
            time.sleep(2)
        raise TimeoutError(f"Strata log watch timeout: {job_id}")

    def link_oidc(self, user_id: str, subject: str) -> dict[str, Any]:
        return self.request_object(
            "POST", "/auth/oidc/identities", json={"user_id": user_id, "subject": subject}
        )

    def disable_oidc(self, identity_id: str) -> None:
        response = self.http.delete(f"/auth/oidc/identities/{identity_id}")
        response.raise_for_status()

    def select_project(self, project_id: str) -> None:
        self.request_object("GET", "/session", headers={"X-Strata-Project": project_id})
        self.http.headers["X-Strata-Project"] = project_id

    def query_dataset(self, file_id: str, **query: Any) -> dict[str, Any]:
        return self.request_object("POST", f"/dataset-files/{file_id}/query", json=query)

    def dataset_statistics(self, file_id: str, **query: Any) -> dict[str, Any]:
        return self.request_object("POST", f"/dataset-files/{file_id}/statistics", json=query)

    def experiment(self, name: str, description: str = "") -> dict[str, Any]:
        return self.request_object(
            "POST", "/experiments", json={"name": name, "description": description}
        )

    def experiment_run(
        self, experiment_id: str, specification: dict[str, Any], key: str | None = None
    ) -> dict[str, Any]:
        return self.request_object(
            "POST",
            f"/experiments/{experiment_id}/runs",
            json=specification,
            headers={"Idempotency-Key": key} if key else {},
        )

    def record_metrics(self, run_id: str, **values: float) -> dict[str, Any]:
        return self.request_object(
            "PUT", f"/experiment-runs/{run_id}/metrics", json={"values": values}
        )

    def compare_runs(self, run_ids: list[str]) -> list[dict[str, Any]]:
        return cast(
            list[dict[str, Any]],
            self.request("GET", "/experiment-runs/compare", params={"ids": ",".join(run_ids)}),
        )

    def replay_run(self, run_id: str, key: str | None = None, **options: Any) -> dict[str, Any]:
        return self.request_object(
            "POST",
            f"/experiment-runs/{run_id}/replay",
            json=options,
            headers={"Idempotency-Key": key} if key else {},
        )

    def create_schedule(self, specification: dict[str, Any]) -> dict[str, Any]:
        return self.request_object("POST", "/schedules", json=specification)

    def webhook_targets(self) -> list[str]:
        return cast(list[str], self.request("GET", "/webhook-targets"))

    def create_webhook(self, specification: dict[str, Any]) -> dict[str, Any]:
        return self.request_object("POST", "/webhooks", json=specification)

    def webhooks(self, limit: int = 50, offset: int = 0) -> list[dict[str, Any]]:
        return cast(
            list[dict[str, Any]],
            self.request("GET", "/webhooks", params={"limit": limit, "offset": offset}),
        )

    def webhook_deliveries(
        self, webhook_id: str, limit: int = 50, offset: int = 0
    ) -> list[dict[str, Any]]:
        return cast(
            list[dict[str, Any]],
            self.request(
                "GET",
                f"/webhooks/{webhook_id}/deliveries",
                params={"limit": limit, "offset": offset},
            ),
        )

    def enable_webhook(self, webhook_id: str, enabled: bool = True) -> dict[str, Any]:
        return self.request_object(
            "POST", f"/webhooks/{webhook_id}/{'enable' if enabled else 'disable'}"
        )

    def retry_webhook_delivery(self, webhook_id: str, delivery_id: str) -> dict[str, Any]:
        return self.request_object("POST", f"/webhooks/{webhook_id}/deliveries/{delivery_id}/retry")

    def schedules(self, limit: int = 50, offset: int = 0) -> list[dict[str, Any]]:
        return cast(
            list[dict[str, Any]],
            self.request("GET", "/schedules", params={"limit": limit, "offset": offset}),
        )

    def schedule(self, schedule_id: str) -> dict[str, Any]:
        return self.request_object("GET", f"/schedules/{schedule_id}")

    def schedule_fires(
        self, schedule_id: str, limit: int = 50, offset: int = 0
    ) -> list[dict[str, Any]]:
        return cast(
            list[dict[str, Any]],
            self.request(
                "GET", f"/schedules/{schedule_id}/fires", params={"limit": limit, "offset": offset}
            ),
        )

    def pause_schedule(self, schedule_id: str) -> dict[str, Any]:
        return self.request_object("POST", f"/schedules/{schedule_id}/pause")

    def resume_schedule(self, schedule_id: str) -> dict[str, Any]:
        return self.request_object("POST", f"/schedules/{schedule_id}/resume")

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *args: Any) -> None:
        self.http.close()

    def request(self, method: str, path: str, **kwargs: Any) -> Any:
        response = self.request_response(method, path, **kwargs)
        return response.json()

    def request_response(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        response = request_with_retry(
            self.http,
            method,
            path,
            max_retries=self.max_retries,
            retry_window_seconds=self.retry_window_seconds,
            **kwargs,
        )
        response.raise_for_status()
        return response

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

    def workflow_expansions(self, campaign_id: str) -> list[dict[str, Any]]:
        return cast(
            list[dict[str, Any]], self.request("GET", f"/campaigns/{campaign_id}/expansions")
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

    def upload_resumable(
        self, version_id: str, file: Path, upload_id: str | None = None
    ) -> dict[str, Any]:
        """Resume by a persistent upload ID and verify each chunk plus the complete file."""
        with file.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
        size = file.stat().st_size
        row = (
            self.request_object("GET", f"/uploads/{upload_id}")
            if upload_id
            else self.request_object(
                "POST",
                f"/dataset-versions/{version_id}/uploads",
                json={"name": file.name, "total_bytes": size, "sha256": digest},
            )
        )
        if (
            row["version_id"] != version_id
            or row["name"] != file.name
            or row["total_bytes"] != size
        ):
            raise ValueError("upload session does not match this dataset file")
        if row["expected_sha256"] != digest:
            raise ValueError("upload session declares a different file checksum")
        if row["status"] == "COMPLETED":
            return self.request_object("POST", f"/uploads/{row['id']}/complete")
        with file.open("rb") as stream:
            stream.seek(row["received_bytes"])
            offset = row["received_bytes"]
            while chunk := stream.read(row["chunk_bytes"]):
                chunk_hash = hashlib.sha256(chunk).hexdigest()
                acknowledged = self.request_object(
                    "PUT",
                    f"/uploads/{row['id']}/chunks/{offset}",
                    content=chunk,
                    headers={"X-Chunk-SHA256": chunk_hash},
                )
                offset += len(chunk)
                if acknowledged["received_bytes"] != offset:
                    raise ValueError("upload acknowledgement offset mismatch")
        result = self.request_object("POST", f"/uploads/{row['id']}/complete")
        if result["sha256"] != digest or result["size"] != size:
            raise ValueError("completed upload checksum or size mismatch")
        return result

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
