import json
import time
from pathlib import Path
from typing import Any

import httpx
import typer
import yaml

from strata_sdk import Client

app = typer.Typer(help="Submit, inspect and control Strata compute jobs.", no_args_is_help=True)


@app.command("version")
def show_version() -> None:
    """Print the installed client package version."""
    from importlib.metadata import version

    typer.echo(version("strata-compute-engine"))


def request(method: str, path: str, **kwargs: Any) -> httpx.Response:
    try:
        with Client() as client:
            kwargs.setdefault("timeout", 10)
            return client.request_response(method, path, **kwargs)
    except httpx.HTTPError as exc:
        typer.echo(f"Strata request failed: {exc}", err=True)
        raise typer.Exit(1) from exc


def display(value: Any) -> None:
    typer.echo(json.dumps(value, indent=2))


@app.command()
def submit(file: Path, idempotency_key: str | None = None) -> None:
    """Submit a YAML or JSON job specification."""
    try:
        body = yaml.safe_load(file.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    headers = {"Idempotency-Key": idempotency_key} if idempotency_key else {}
    display(request("POST", "/jobs", json=body, headers=headers).json())


@app.command()
def status(job_id: str, watch: bool = False, timeout: float = 600) -> None:
    """Inspect a job, optionally waiting for a terminal state."""
    deadline = time.monotonic() + timeout
    previous = ""
    while True:
        job = request("GET", f"/jobs/{job_id}").json()
        if job["status"] != previous:
            display(job)
            previous = job["status"]
        if not watch or previous in {"SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT"}:
            if watch and previous != "SUCCEEDED":
                raise typer.Exit(1)
            return
        if time.monotonic() >= deadline:
            typer.echo("Watch timeout exceeded", err=True)
            raise typer.Exit(1)
        time.sleep(0.5)


@app.command()
def logs(
    job_id: str, attempt: int | None = None, follow: bool = False, timeout: float = 3600
) -> None:
    """Print the bounded log snapshot of an attempt."""
    if follow:
        if attempt is not None:
            raise typer.BadParameter("follow selects the latest attempt; omit --attempt")
        with Client() as client:
            for snapshot in client.follow_logs(job_id, timeout):
                typer.echo(f"Attempt {snapshot['attempt']} · {snapshot['status']}")
                typer.echo(snapshot["text"] or "")
        return
    typer.echo(
        request("GET", f"/jobs/{job_id}/logs", params={"attempt": attempt} if attempt else {}).text
    )


@app.command()
def cancel(job_id: str) -> None:
    display(request("POST", f"/jobs/{job_id}/cancel").json())


@app.command()
def retry(job_id: str) -> None:
    display(request("POST", f"/jobs/{job_id}/retry").json())


@app.command()
def workers() -> None:
    display(request("GET", "/workers").json())


@app.command()
def attempts(job_id: str) -> None:
    display(request("GET", f"/jobs/{job_id}/attempts").json())


@app.command()
def operations() -> None:
    """Inspect administrator-only supervised operations and backup evidence."""
    display(request("GET", "/operations").json())


@app.command()
def admission() -> None:
    """Inspect administrator-only cluster admission and scheduling controls."""
    display(request("GET", "/cluster/admission").json())


@app.command()
def pools() -> None:
    """Inspect bounded Docker host worker pools."""
    display(request("GET", "/cluster/pools").json())


@app.command()
def execution_config() -> None:
    """List the deployment's approved execution images for the selected project."""
    display(request("GET", "/execution/config").json())


@app.command()
def pool_workers(pool_id: str) -> None:
    display(request("GET", f"/cluster/pools/{pool_id}/workers").json())


@app.command()
def set_pool(pool_id: str, enabled: bool, minimum: int, maximum: int) -> None:
    """Set a pool's policy within its configured host capacity."""
    display(
        request(
            "PATCH",
            f"/cluster/pools/{pool_id}",
            json={
                "enabled": enabled,
                "minimum": minimum,
                "maximum": maximum,
            },
        ).json()
    )


@app.command()
def set_admission(accepting_jobs: bool, scheduling_enabled: bool, reason: str = "") -> None:
    """Set admission and scheduling independently; running attempts continue."""
    display(
        request(
            "PATCH",
            "/cluster/admission",
            json={
                "accepting_jobs": accepting_jobs,
                "scheduling_enabled": scheduling_enabled,
                "reason": reason,
            },
        ).json()
    )


@app.command()
def worker_gpus(worker_id: str) -> None:
    """Inspect discovered devices and their current exclusive assignments."""
    display(request("GET", f"/workers/{worker_id}/gpus").json())


@app.command()
def events(job_id: str) -> None:
    display(request("GET", f"/jobs/{job_id}/events").json())


@app.command()
def artifacts(job_id: str, output: Path | None = None) -> None:
    if output:
        from strata_sdk import Client

        with Client() as client:
            rows = client.artifacts(job_id, output)
    else:
        rows = request("GET", f"/jobs/{job_id}/artifacts").json()
    display(rows)


from cli.experiments import install as install_experiments  # noqa: E402
from cli.identity import install as install_identity  # noqa: E402
from cli.platform import install  # noqa: E402
from cli.runtimes import install as install_runtimes  # noqa: E402
from cli.schedules import install as install_schedules  # noqa: E402
from cli.webhooks import install as install_webhooks  # noqa: E402

install(app)
install_identity(app)
install_experiments(app)
install_schedules(app)
install_webhooks(app)
install_runtimes(app)

if __name__ == "__main__":
    app()
