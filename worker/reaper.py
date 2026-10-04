"""Independent host cleanup; never renews a worker lease or registers as an agent."""

import json
import logging
import re
import signal
import threading
from typing import Any

import docker
import typer

from control_plane.logging import configure_logging
from control_plane.rpc import engine_pb2 as pb
from worker.transport import Transport

app = typer.Typer(help="Inspect and reap expired Strata containers on this Docker host.")
UUID = re.compile(r"[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}")
logger = logging.getLogger(__name__)


class HostReaper:
    def __init__(self, transport: Transport, client: Any = None) -> None:
        self.transport, self.client = transport, client or docker.from_env(timeout=3)
        self.cursor = 0

    @staticmethod
    def owner(labels: dict[str, str]) -> tuple[str, str] | None:
        worker, attempt = labels.get("strata.worker", ""), labels.get("strata.attempt", "")
        if not re.fullmatch(r"[a-zA-Z0-9_.-]{1,128}", worker) or not UUID.fullmatch(attempt):
            return None
        return worker, attempt

    def tick(self, *, apply: bool = False) -> dict[str, Any]:
        cluster = self.transport.call("Cluster", pb.Empty()).cluster_id
        if not UUID.fullmatch(cluster):
            raise ValueError("coordinator did not return a valid deployment identity")
        selector = {"label": f"strata.cluster={cluster}"}
        containers = self.client.containers.list(all=True, filters=selector)
        volumes = self.client.volumes.list(filters=selector)
        owners = sorted(
            {owner for container in containers if (owner := self.owner(container.labels))}
            | {
                owner
                for volume in volumes
                if (owner := self.owner(volume.attrs.get("Labels") or {}))
            }
        )
        batch = (owners[self.cursor :] + owners[: self.cursor])[:100]
        self.cursor = (self.cursor + len(batch)) % len(owners) if owners else 0
        removable = set()
        if batch:
            reply = self.transport.call(
                "InspectOrphans",
                pb.OrphanRequest(
                    candidates=[
                        pb.OrphanCandidate(worker_id=worker, attempt_id=attempt)
                        for worker, attempt in batch
                    ]
                ),
            )
            removable = {
                (item.worker_id, item.attempt_id) for item in reply.decisions if item.remove
            }
            if not removable.issubset(set(batch)):
                raise ValueError("coordinator returned an unexpected cleanup decision")
        candidates, removed, errors = [], [], 0
        for container in containers:
            owner = self.owner(container.labels)
            if owner not in removable:
                continue
            candidates.append(container.id)
            if apply:
                try:
                    if container.status not in {"exited", "dead", "created"}:
                        container.stop(timeout=5)
                    container.remove(force=True)
                    removed.append(container.id)
                except docker.errors.DockerException:
                    errors += 1
        volume_candidates = []
        for volume in volumes:
            owner = self.owner(volume.attrs.get("Labels") or {})
            if owner not in removable or volume.name not in {
                f"strata-output-{owner[1]}",
                f"strata-input-{owner[1]}",
            }:
                continue
            volume_candidates.append(volume.name)
            if apply:
                try:
                    volume.remove(force=False)  # A still-mounted volume remains protected.
                except docker.errors.DockerException:
                    errors += 1
        return {
            "dry_run": not apply,
            "inspected_attempts": len(batch),
            "container_candidates": candidates,
            "removed_containers": removed,
            "volume_candidates": volume_candidates,
            "errors": errors,
        }


@app.command("once")
def once(apply: bool = False) -> None:
    transport = Transport.from_env()
    try:
        typer.echo(json.dumps(HostReaper(transport).tick(apply=apply), indent=2))
    finally:
        transport.channel.close()


@app.command("run")
def run(interval_seconds: int = 15) -> None:
    if not 5 <= interval_seconds <= 3600:
        raise typer.BadParameter("interval must be 5..3600 seconds")
    configure_logging()
    transport, stopped = Transport.from_env(), threading.Event()
    reaper = HostReaper(transport)
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stopped.set())
    try:
        while not stopped.is_set():
            try:
                result = reaper.tick(apply=True)
                if result["removed_containers"] or result["errors"]:
                    logger.info("host_reaper: %s", json.dumps(result))
            except Exception as exc:
                logger.warning("host_reaper_unavailable: %s", type(exc).__name__)
            stopped.wait(interval_seconds)
    finally:
        transport.channel.close()
