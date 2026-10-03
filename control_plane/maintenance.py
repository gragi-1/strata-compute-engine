"""Verified backups and storage inspection; no automatic destructive retention."""

import hashlib
import json
import os
import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import typer
from sqlalchemy import create_engine, select, text
from sqlalchemy.engine import make_url

from control_plane.config import Settings
from control_plane.models import Artifact, DatasetFile

app = typer.Typer(help="Back up, verify and inspect Strata persistence.", no_args_is_help=True)


def checksum(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def postgres_environment(url: str) -> dict[str, str]:
    parsed = make_url(url)
    if not parsed.drivername.startswith("postgresql"):
        raise ValueError("maintenance requires PostgreSQL")
    extra = {
        env: str(parsed.query[key])
        for key, env in {
            "sslmode": "PGSSLMODE",
            "sslrootcert": "PGSSLROOTCERT",
            "options": "PGOPTIONS",
        }.items()
        if key in parsed.query
    }
    return (
        os.environ
        | extra
        | {
            "PGHOST": parsed.host or "localhost",
            "PGPORT": str(parsed.port or 5432),
            "PGUSER": parsed.username or "strata",
            "PGPASSWORD": parsed.password or "",
            "PGDATABASE": parsed.database or "strata",
        }
    )


def binary(name: str) -> str:
    found = shutil.which(name)
    windows = Path(r"C:\Program Files\PostgreSQL\17\bin") / f"{name}.exe"
    if found:
        return found
    if windows.is_file():
        return str(windows)
    raise ValueError(f"{name} is required (PostgreSQL client tools)")


def backup_store(config: Settings, output: Path) -> dict[str, Any]:
    if output.exists():
        raise ValueError("backup destination must be a new directory")
    output.mkdir(parents=True)
    blobs = output / "blobs"
    blobs.mkdir()
    engine = create_engine(config.database_url)
    try:
        with (
            engine.connect().execution_options(isolation_level="REPEATABLE READ") as connection,
            connection.begin(),
        ):
            snapshot = connection.execute(text("SELECT pg_export_snapshot()")).scalar_one()
            subprocess.run(
                [
                    binary("pg_dump"),
                    "--format=custom",
                    "--no-owner",
                    "--no-acl",
                    "--snapshot",
                    snapshot,
                    "--file",
                    str(output / "database.dump"),
                ],
                env=postgres_environment(config.database_url),
                check=True,
                capture_output=True,
            )
            hashes = set(connection.scalars(select(Artifact.sha256))) | set(
                connection.scalars(select(DatasetFile.sha256))
            )
            manifest = {}
            for digest in sorted(hashes):
                source = config.artifact_root / digest
                if not source.is_file() or checksum(source) != digest:
                    raise ValueError(f"missing or corrupt referenced blob: {digest}")
                shutil.copyfile(source, blobs / digest)
                manifest[digest] = source.stat().st_size
            record = {
                "format": 1,
                "created_at": datetime.now(UTC).isoformat(),
                "blobs": manifest,
                "database_sha256": checksum(output / "database.dump"),
                "database_bytes": (output / "database.dump").stat().st_size,
            }
            (output / "manifest.json").write_text(json.dumps(record, indent=2), encoding="utf-8")
            return record
    finally:
        engine.dispose()


def verify_store(output: Path) -> dict[str, Any]:
    record: dict[str, Any] = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
    if record.get("format") != 1:
        raise ValueError("unsupported backup format")
    if checksum(output / "database.dump") != record["database_sha256"]:
        raise ValueError("database dump checksum mismatch")
    import re

    for digest, size in record["blobs"].items():
        if not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ValueError("invalid backup blob name")
        path = output / "blobs" / digest
        if not path.is_file() or path.stat().st_size != size or checksum(path) != digest:
            raise ValueError(f"backup blob checksum mismatch: {digest}")
    return record


def restore_store(output: Path, config: Settings) -> None:
    record = verify_store(output)
    if config.artifact_root.exists() and any(config.artifact_root.iterdir()):
        raise ValueError("restore artifact directory must be empty")
    engine = create_engine(config.database_url)
    try:
        with engine.connect() as connection:
            count = connection.execute(
                text(
                    "SELECT count(*) FROM information_schema.tables "
                    "WHERE table_schema NOT IN ('pg_catalog','information_schema')"
                )
            ).scalar_one()
            if count:
                raise ValueError("restore database must be empty")
        # A single restore transaction makes SQL failures roll back. Existing databases are refused.
        subprocess.run(
            [
                binary("pg_restore"),
                "--single-transaction",
                "--exit-on-error",
                "--no-owner",
                "--no-acl",
                "--dbname",
                make_url(config.database_url).database or "strata",
                str(output / "database.dump"),
            ],
            env=postgres_environment(config.database_url),
            check=True,
            capture_output=True,
        )
        config.artifact_root.mkdir(parents=True, exist_ok=True)
        for digest in record["blobs"]:
            shutil.copyfile(output / "blobs" / digest, config.artifact_root / digest)
    finally:
        engine.dispose()


@app.command("backup")
def backup_command(output: Path) -> None:
    typer.echo(json.dumps(backup_store(Settings(), output), indent=2))


@app.command("verify-backup")
def verify_command(output: Path) -> None:
    record = verify_store(output)
    typer.echo(f"Verified {len(record['blobs'])} immutable blobs and database dump")


@app.command("restore")
def restore_command(backup: Path) -> None:
    """Restore into an explicitly configured EMPTY database and artifact directory."""
    restore_store(backup, Settings())
    typer.echo("Restore completed; keep workers stopped until recovery has expired old sessions.")


@app.command("storage-audit")
def storage_audit() -> None:
    config = Settings()
    engine = create_engine(config.database_url)
    try:
        with engine.connect() as connection:
            hashes = set(connection.scalars(select(Artifact.sha256))) | set(
                connection.scalars(select(DatasetFile.sha256))
            )
        missing = [digest for digest in hashes if not (config.artifact_root / digest).is_file()]
        unreferenced = (
            [p.name for p in config.artifact_root.iterdir() if p.is_file() and p.name not in hashes]
            if config.artifact_root.exists()
            else []
        )
        typer.echo(
            json.dumps(
                {
                    "referenced_blobs": len(hashes),
                    "missing": missing,
                    "unreferenced": unreferenced,
                    "action": "inspection only; no files deleted",
                },
                indent=2,
            )
        )
    finally:
        engine.dispose()
