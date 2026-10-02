import json
import os
import time
from pathlib import Path
from typing import Any

import httpx
import typer
import yaml

app = typer.Typer(help="Submit, inspect and control Strata compute jobs.", no_args_is_help=True)


def request(method: str, path: str, **kwargs: Any) -> httpx.Response:
    try:
        response = httpx.request(
            method,
            os.getenv("STRATA_API_URL", "http://localhost:8000") + path,
            timeout=10,
            **kwargs,
        )
        response.raise_for_status()
        return response
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
def logs(job_id: str, attempt: int | None = None) -> None:
    """Print the bounded log snapshot of an attempt."""
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
def events(job_id: str) -> None:
    display(request("GET", f"/jobs/{job_id}/events").json())


@app.command()
def artifacts(job_id: str, output: Path | None = None) -> None:
    rows = request("GET", f"/jobs/{job_id}/artifacts").json()
    if output:
        output.mkdir(parents=True, exist_ok=True)
        for row in rows:
            (output / row["name"]).write_bytes(request("GET", row["uri"]).content)
    display(rows)


if __name__ == "__main__":
    app()
