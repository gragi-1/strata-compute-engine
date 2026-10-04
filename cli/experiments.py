from pathlib import Path

import typer
import yaml


def install(app: typer.Typer) -> None:
    from cli.main import display, request

    experiments = typer.Typer(help="Register, compare and replay reproducible experiment runs.")
    app.add_typer(experiments, name="experiments")

    @experiments.command("create")
    def create(name: str, description: str = "") -> None:
        display(
            request("POST", "/experiments", json={"name": name, "description": description}).json()
        )

    @experiments.command("list")
    def listing() -> None:
        display(request("GET", "/experiments").json())

    @experiments.command("run")
    def run(experiment_id: str, file: Path, idempotency_key: str | None = None) -> None:
        display(
            request(
                "POST",
                f"/experiments/{experiment_id}/runs",
                json=yaml.safe_load(file.read_text(encoding="utf-8")),
                headers={"Idempotency-Key": idempotency_key} if idempotency_key else {},
            ).json()
        )

    @experiments.command("runs")
    def runs(
        experiment_id: str,
        metric: str | None = None,
        minimum: float | None = None,
        maximum: float | None = None,
    ) -> None:
        params = {
            key: value
            for key, value in {"metric": metric, "minimum": minimum, "maximum": maximum}.items()
            if value is not None
        }
        display(request("GET", f"/experiments/{experiment_id}/runs", params=params).json())

    @experiments.command("inspect")
    def inspect(run_id: str) -> None:
        display(request("GET", f"/experiment-runs/{run_id}").json())

    @experiments.command("metrics")
    def metrics(run_id: str, file: Path) -> None:
        display(
            request(
                "PUT",
                f"/experiment-runs/{run_id}/metrics",
                json={"values": yaml.safe_load(file.read_text(encoding="utf-8"))},
            ).json()
        )

    @experiments.command("compare")
    def compare(run_ids: list[str]) -> None:
        display(
            request("GET", "/experiment-runs/compare", params={"ids": ",".join(run_ids)}).json()
        )

    @experiments.command("replay")
    def replay(
        run_id: str, options: Path | None = None, idempotency_key: str | None = None
    ) -> None:
        display(
            request(
                "POST",
                f"/experiment-runs/{run_id}/replay",
                json=yaml.safe_load(options.read_text(encoding="utf-8")) if options else {},
                headers={"Idempotency-Key": idempotency_key} if idempotency_key else {},
            ).json()
        )
