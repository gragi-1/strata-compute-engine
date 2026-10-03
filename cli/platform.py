import csv
import json
import platform
from pathlib import Path
from typing import Any

import typer
import yaml


def install(app: typer.Typer) -> None:
    from cli.main import display, request

    @app.command("campaign")
    def campaign(file: Path, idempotency_key: str | None = None) -> None:
        """Submit a parameter sweep from a YAML or JSON specification."""
        body = yaml.safe_load(file.read_text(encoding="utf-8"))
        display(
            request(
                "POST",
                "/campaigns",
                json=body,
                headers={"Idempotency-Key": idempotency_key} if idempotency_key else {},
            ).json()
        )

    @app.command("workflow")
    def workflow(file: Path, idempotency_key: str | None = None) -> None:
        """Submit a dependency graph with artifact inputs."""
        body = yaml.safe_load(file.read_text(encoding="utf-8"))
        display(
            request(
                "POST",
                "/workflows",
                json=body,
                headers={"Idempotency-Key": idempotency_key} if idempotency_key else {},
            ).json()
        )

    @app.command("campaign-status")
    def campaign_status(campaign_id: str) -> None:
        display(request("GET", f"/campaigns/{campaign_id}").json())

    @app.command("dataset-upload")
    def dataset_upload(name: str, files: list[Path], label: str = "v1") -> None:
        """Create a dataset, stream its files and seal the version."""
        from strata_sdk import Client

        with Client() as client:
            display(client.dataset(name, files, label))

    @app.command("datasets")
    def datasets() -> None:
        display(request("GET", "/datasets").json())

    @app.command("preview")
    def preview(file_id: str, limit: int = 50) -> None:
        display(request("GET", f"/dataset-files/{file_id}/preview", params={"limit": limit}).json())

    @app.command("report")
    def report(
        campaign_id: str,
        output: Path,
        x: str = "seed",
        y: str = "result.pi",
        group: str | None = None,
    ) -> None:
        """Export evidence and a scientific figure (install the analysis extra)."""
        rows = request("GET", f"/campaigns/{campaign_id}/results").json()
        specification = request("GET", f"/campaigns/{campaign_id}").json()
        output.mkdir(parents=True, exist_ok=True)
        write_report(rows, specification, output, x, y, group)
        typer.echo(f"Report saved to {output}")


def write_report(
    rows: list[dict[str, Any]],
    specification: dict[str, Any],
    output: Path,
    x: str,
    y: str,
    group: str | None = None,
) -> None:
    points = [
        (r[x], r[y])
        for r in rows
        if r["status"] == "SUCCEEDED"
        and isinstance(r.get(x), (int, float))
        and isinstance(r.get(y), (int, float))
    ]
    if not points:
        raise ValueError("no successful rows contain the requested numerical columns")
    try:
        import matplotlib

        matplotlib.use("Agg")
        from matplotlib import pyplot as plt
    except ImportError as exc:
        raise ValueError("install Strata with the analysis extra to generate figures") from exc
    (output / "results.json").write_text(json.dumps(rows, indent=2), encoding="utf-8")
    (output / "provenance.json").write_text(
        json.dumps(
            {
                "campaign": specification,
                "python": platform.python_version(),
                "strata": "2.0.0",
                "x": x,
                "y": y,
                "group": group,
                "matplotlib": matplotlib.__version__,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    with (output / "results.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, sorted({k for r in rows for k in r}))
        writer.writeheader()
        writer.writerows(
            {
                key: "'" + value
                if isinstance(value, str) and value.startswith(("=", "+", "-", "@"))
                else value
                for key, value in row.items()
            }
            for row in rows
        )
    fig, axes = plt.subplots(figsize=(7, 4), layout="constrained")
    if group:
        for value in sorted({str(r.get(group)) for r in rows}):
            selected = [
                (r[x], r[y])
                for r in rows
                if str(r.get(group)) == value
                and r["status"] == "SUCCEEDED"
                and isinstance(r.get(x), (int, float))
                and isinstance(r.get(y), (int, float))
            ]
            axes.scatter(
                [p[0] for p in selected],
                [p[1] for p in selected],
                s=24,
                alpha=0.7,
                label=f"{group} = {value}",
            )
        axes.legend()
    else:
        axes.scatter(
            [p[0] for p in points], [p[1] for p in points], color="#16654b", s=24, alpha=0.7
        )
    axes.set(xlabel=x, ylabel=y, title=specification["name"])
    axes.grid(alpha=0.2)
    fig.savefig(output / "comparison.png", dpi=200)
    fig.savefig(output / "comparison.pdf")
    plt.close(fig)
