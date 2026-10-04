from pathlib import Path

import typer
import yaml


def install(app: typer.Typer) -> None:
    from cli.main import display, request

    schedules = typer.Typer(help="Create and inspect durable periodic jobs.")
    app.add_typer(schedules, name="schedules")

    @schedules.command("create")
    def create(file: Path) -> None:
        display(
            request(
                "POST", "/schedules", json=yaml.safe_load(file.read_text(encoding="utf-8"))
            ).json()
        )

    @schedules.command("list")
    def listing(limit: int = 50, offset: int = 0) -> None:
        display(request("GET", "/schedules", params={"limit": limit, "offset": offset}).json())

    @schedules.command("inspect")
    def inspect(schedule_id: str) -> None:
        display(request("GET", f"/schedules/{schedule_id}").json())

    @schedules.command("fires")
    def fires(schedule_id: str, limit: int = 50, offset: int = 0) -> None:
        display(
            request(
                "GET", f"/schedules/{schedule_id}/fires", params={"limit": limit, "offset": offset}
            ).json()
        )

    @schedules.command("pause")
    def pause(schedule_id: str) -> None:
        display(request("POST", f"/schedules/{schedule_id}/pause").json())

    @schedules.command("resume")
    def resume(schedule_id: str) -> None:
        display(request("POST", f"/schedules/{schedule_id}/resume").json())
