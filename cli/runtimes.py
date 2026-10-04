"""Computation groups and interactive runtimes."""

from pathlib import Path

import typer
import yaml


def install(app: typer.Typer) -> None:
    from cli.main import display, request

    groups = typer.Typer(help="Admit and control cooperating computation groups.")
    sessions = typer.Typer(help="Run stateful Python notebooks and bounded HTTP services.")
    app.add_typer(groups, name="groups")
    app.add_typer(sessions, name="sessions")

    @groups.command("submit")
    def submit_group(file: Path, idempotency_key: str | None = None) -> None:
        headers = {"Idempotency-Key": idempotency_key} if idempotency_key else {}
        display(
            request(
                "POST",
                "/compute-groups",
                json=yaml.safe_load(file.read_text(encoding="utf-8")),
                headers=headers,
            ).json()
        )

    @groups.command("list")
    def list_groups() -> None:
        display(request("GET", "/compute-groups").json())

    @groups.command("show")
    def group(group_id: str) -> None:
        display(request("GET", f"/compute-groups/{group_id}").json())

    @groups.command("cancel")
    def cancel_group(group_id: str) -> None:
        display(request("POST", f"/compute-groups/{group_id}/cancel").json())

    @groups.command("retry")
    def retry_group(group_id: str, idempotency_key: str) -> None:
        display(
            request(
                "POST",
                f"/compute-groups/{group_id}/retry",
                headers={"Idempotency-Key": idempotency_key},
            ).json()
        )

    @sessions.command("create")
    def create_session(file: Path, idempotency_key: str | None = None) -> None:
        display(
            request(
                "POST",
                "/interactive-sessions",
                json=yaml.safe_load(file.read_text(encoding="utf-8")),
                headers={"Idempotency-Key": idempotency_key} if idempotency_key else {},
            ).json()
        )

    @sessions.command("list")
    def list_sessions() -> None:
        display(request("GET", "/interactive-sessions").json())

    @sessions.command("show")
    def session(session_id: str) -> None:
        display(request("GET", f"/interactive-sessions/{session_id}").json())

    @sessions.command("execute")
    def execute(session_id: str, file: Path, idempotency_key: str, timeout: int = 30) -> None:
        display(
            request(
                "POST",
                f"/interactive-sessions/{session_id}/cells",
                json={"code": file.read_text(encoding="utf-8"), "timeout_seconds": timeout},
                headers={"Idempotency-Key": idempotency_key},
            ).json()
        )

    @sessions.command("cells")
    def cells(session_id: str) -> None:
        display(request("GET", f"/interactive-sessions/{session_id}/cells").json())

    @sessions.command("proxy")
    def proxy(
        session_id: str, idempotency_key: str, path: str = "/", method: str = "GET", body: str = ""
    ) -> None:
        display(
            request(
                "POST",
                f"/interactive-sessions/{session_id}/proxy",
                json={"path": path, "method": method, "body": body},
                headers={"Idempotency-Key": idempotency_key},
            ).json()
        )

    @sessions.command("export")
    def export(session_id: str, output: Path) -> None:
        output.write_bytes(request("GET", f"/interactive-sessions/{session_id}/notebook").content)
        typer.echo(str(output))

    @sessions.command("stop")
    def stop(session_id: str) -> None:
        display(request("POST", f"/interactive-sessions/{session_id}/stop").json())
