from pathlib import Path

import typer
import yaml


def install(app: typer.Typer) -> None:
    from cli.main import display, request

    webhooks = typer.Typer(help="Configure approved event receivers and inspect durable delivery.")
    app.add_typer(webhooks, name="webhooks")

    @webhooks.command("targets")
    def targets() -> None:
        display(request("GET", "/webhook-targets").json())

    @webhooks.command("create")
    def create(file: Path) -> None:
        display(
            request(
                "POST", "/webhooks", json=yaml.safe_load(file.read_text(encoding="utf-8"))
            ).json()
        )

    @webhooks.command("list")
    def listing(limit: int = 50, offset: int = 0) -> None:
        display(request("GET", "/webhooks", params={"limit": limit, "offset": offset}).json())

    @webhooks.command("deliveries")
    def deliveries(webhook_id: str, limit: int = 50, offset: int = 0) -> None:
        display(
            request(
                "GET",
                f"/webhooks/{webhook_id}/deliveries",
                params={"limit": limit, "offset": offset},
            ).json()
        )

    @webhooks.command("enable")
    def enable(webhook_id: str) -> None:
        display(request("POST", f"/webhooks/{webhook_id}/enable").json())

    @webhooks.command("disable")
    def disable(webhook_id: str) -> None:
        display(request("POST", f"/webhooks/{webhook_id}/disable").json())

    @webhooks.command("retry")
    def retry(webhook_id: str, delivery_id: str) -> None:
        display(request("POST", f"/webhooks/{webhook_id}/deliveries/{delivery_id}/retry").json())
