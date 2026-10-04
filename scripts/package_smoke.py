"""Install a release wheel in a fresh environment and exercise it outside the checkout."""

import argparse
import hashlib
import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import zipfile
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url


@contextmanager
def disposable_database(url: str):
    source = make_url(url)
    if not source.drivername.startswith("postgresql"):
        raise ValueError("installed migration verification requires PostgreSQL")
    name = "strata_package_" + uuid4().hex
    admin = create_engine(
        source.set(database="postgres").update_query_dict({"options": ""}),
        isolation_level="AUTOCOMMIT",
    )
    created = False
    try:
        with admin.connect() as connection:
            connection.execute(text(f'CREATE DATABASE "{name}"'))
            created = True
        yield (
            source.set(database=name)
            .update_query_dict({"options": ""})
            .render_as_string(hide_password=False)
        )
    finally:
        try:
            if created:
                with admin.connect() as connection:
                    connection.execute(text(f'DROP DATABASE "{name}" WITH (FORCE)'))
        finally:
            admin.dispose()


def smoke(
    wheel: Path, requirements: Path, output: Path, uv: str, database_url: str
) -> dict[str, object]:
    wheel, requirements, output = wheel.resolve(), requirements.resolve(), output.resolve()
    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        required = {"control_plane/web/index.html", "control_plane/_migrations/env.py"}
        if not required.issubset(names) or not any("/versions/" in name for name in names):
            raise ValueError("wheel is missing the web workspace or migrations")
        if any("__pycache__" in name or name.endswith(".pyc") for name in names):
            raise ValueError("wheel contains interpreter cache files")
    output.mkdir(parents=True, exist_ok=False)
    environment = output / "environment"
    python = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    clean = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("STRATA_", "AWS_", "OTEL_"))
        and key not in {"PYTHONPATH", "PYTHONHOME", "VIRTUAL_ENV"}
    }

    def run(arguments: list[str]) -> None:
        subprocess.run(arguments, cwd=output, env=clean, check=True)

    run([uv, "venv", str(environment), "--python", sys.executable])
    run(
        [uv, "pip", "install", "--python", str(python), "--require-hashes", "-r", str(requirements)]
    )
    run([uv, "pip", "install", "--python", str(python), "--no-deps", str(wheel)])
    clean.update(
        STRATA_DATABASE_URL=database_url,
        STRATA_ARTIFACT_ROOT=str(output / "artifacts"),
    )
    scripts = python.parent
    commands = [
        "strata",
        "strata-admin",
        "strata-worker",
        "strata-rpc",
        "strata-scheduler",
        "strata-events",
        "strata-ops",
        "strata-provisioner",
        "strata-coordinator",
        "strata-reaper",
    ]
    # Daemons have no --help contract; import their installed entrypoints without starting them.
    for name in ["strata", "strata-admin", "strata-reaper"]:
        run([str(scripts / (name + (".exe" if os.name == "nt" else ""))), "--help"])
    run([str(scripts / ("strata.exe" if os.name == "nt" else "strata")), "version"])
    for group in ("groups", "sessions"):
        run([str(scripts / ("strata.exe" if os.name == "nt" else "strata")), group, "--help"])
    run(
        [
            str(python),
            "-I",
            "-c",
            "import importlib.metadata as m; from pathlib import Path; import control_plane; "
            f"root = Path({str(environment)!r}); "
            "assert Path(control_plane.__file__).resolve().is_relative_to(root); "
            "[e.load() for e in m.distribution('strata-compute-engine').entry_points]",
        ]
    )
    admin = str(scripts / ("strata-admin.exe" if os.name == "nt" else "strata-admin"))
    run([admin, "migrate"])
    run([admin, "migrate", "--check"])
    installed_heads = json.loads(
        subprocess.check_output(
            [
                str(python),
                "-I",
                "-c",
                "import json; from sqlalchemy import create_engine,text; "
                "from control_plane.config import Settings; "
                "e=create_engine(Settings().database_url); "
                "c=e.connect(); print(json.dumps(list(c.scalars(text("
                "'SELECT version_num FROM alembic_version'))))); c.close(); e.dispose()",
            ],
            cwd=output,
            env=clean,
            text=True,
        )
    )
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    with (output / "server.log").open("wb") as log:
        server = subprocess.Popen(
            [
                str(python),
                "-I",
                "-m",
                "uvicorn",
                "control_plane.api:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
                "--no-access-log",
            ],
            cwd=output,
            env=clean,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=log,
        )
        try:
            deadline = time.monotonic() + 30
            while True:
                try:
                    with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=2) as r:
                        assert r.status == 200
                    break
                except (urllib.error.URLError, TimeoutError):
                    if server.poll() is not None or time.monotonic() > deadline:
                        raise RuntimeError(
                            "installed API did not start; inspect server.log"
                        ) from None
                    time.sleep(0.2)
            for route in ["/", "/app/app.js", "/ready", "/openapi.json"]:
                with urllib.request.urlopen(f"http://127.0.0.1:{port}{route}", timeout=5) as r:
                    assert r.status == 200
                    if route == "/openapi.json":
                        schema = json.load(r)
                        version = schema["info"]["version"]
                        assert {"/compute-groups", "/interactive-sessions"}.issubset(
                            schema["paths"]
                        )
        finally:
            server.terminate()
            try:
                server.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait(timeout=5)
    result: dict[str, object] = {
        "wheel": wheel.name,
        "version": version,
        "python": sys.version.split()[0],
        "sha256": hashlib.sha256(wheel.read_bytes()).hexdigest(),
        "entrypoints": commands,
        "installed_schema_check": True,
        "installed_migration_heads": installed_heads,
        "installed_http_and_web": True,
    }
    (output / "evidence.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheel", type=Path, required=True)
    parser.add_argument("--requirements", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--uv", default="uv")
    parser.add_argument("--postgres-url", default=os.getenv("STRATA_TEST_POSTGRES_URL"))
    args = parser.parse_args()
    if not args.postgres_url:
        parser.error("set STRATA_TEST_POSTGRES_URL to an isolated cluster with CREATEDB privilege")
    with disposable_database(args.postgres_url) as database_url:
        print(
            json.dumps(
                smoke(args.wheel, args.requirements, args.output, args.uv, database_url), indent=2
            )
        )
