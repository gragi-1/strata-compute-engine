import io
import tarfile
import tempfile
import time
from collections.abc import Callable, Iterator
from contextlib import suppress
from dataclasses import dataclass
from typing import Any

import docker


@dataclass
class Result:
    outcome: str
    exit_code: int
    reason: str


class DockerExecutor:
    def __init__(self, allowed_images: list[str], grace: int = 5, client: Any = None) -> None:
        self.client = client or docker.from_env(timeout=3)
        self.allowed_images = allowed_images
        self.grace = grace

    def create(self, assignment: Any, worker_id: str) -> Any:
        if assignment.image not in self.allowed_images:
            raise ValueError("image is not allowlisted on worker")
        self.client.volumes.create(
            name=f"strata-output-{assignment.attempt_id}", labels={"strata.worker": worker_id}
        )
        volumes = {f"strata-output-{assignment.attempt_id}": {"bind": "/output", "mode": "rw"}}
        if assignment.has_inputs:
            self.client.volumes.create(
                name=f"strata-input-{assignment.attempt_id}", labels={"strata.worker": worker_id}
            )
            volumes[f"strata-input-{assignment.attempt_id}"] = {"bind": "/inputs", "mode": "ro"}
        return self.client.containers.create(
            assignment.image,
            list(assignment.command),
            name=f"strata-{assignment.attempt_id}",
            detach=True,
            nano_cpus=int(assignment.cpu * 1e9),
            mem_limit=f"{assignment.memory_mb}m",
            memswap_limit=f"{assignment.memory_mb}m",
            user="65534:65534",
            network_disabled=True,
            read_only=True,
            cap_drop=["ALL"],
            security_opt=["no-new-privileges:true"],
            pids_limit=128,
            init=True,
            tmpfs={"/tmp": "rw,nosuid,nodev,size=64m"},
            volumes=volumes,
            labels={"strata.worker": worker_id, "strata.attempt": assignment.attempt_id},
            log_config=docker.types.LogConfig(
                type="json-file", config={"max-size": "1m", "max-file": "1"}
            ),
        )

    def stage(
        self,
        assignment: Any,
        worker_id: str,
        alias: str,
        name: str,
        stream: Any,
        size: int,
        progress: Callable[[], None],
    ) -> None:
        helper = self.client.containers.create(
            assignment.image,
            ["true"],
            network_disabled=True,
            cap_drop=["ALL"],
            security_opt=["no-new-privileges:true"],
            user="65534:65534",
            mem_limit="64m",
            volumes={f"strata-input-{assignment.attempt_id}": {"bind": "/staging", "mode": "rw"}},
            labels={"strata.worker": worker_id, "strata.attempt": assignment.attempt_id},
        )
        try:
            with tempfile.TemporaryFile() as archive:
                with tarfile.open(fileobj=archive, mode="w") as tar:
                    directory = tarfile.TarInfo(alias)
                    directory.type, directory.mode = tarfile.DIRTYPE, 0o755
                    tar.addfile(directory)
                    item = tarfile.TarInfo(f"{alias}/{name}")
                    item.size, item.mode, item.uid, item.gid = size, 0o444, 65534, 65534
                    stream.seek(0)
                    tar.addfile(item, stream)
                archive.seek(0)

                class Pump:
                    def read(self, length: int = -1) -> bytes:
                        progress()
                        return archive.read(length if length >= 0 else 65536)

                    def __iter__(self) -> "Pump":
                        return self

                    def __next__(self) -> bytes:
                        data = self.read(65536)
                        if not data:
                            raise StopIteration
                        return data

                # The helper is never started. Docker writes the verified archive into its volume.
                self.client.api.put_archive(helper.id, "/staging", Pump())
        finally:
            helper.remove(force=True)

    def stop(self, container: Any) -> None:
        try:
            container.stop(timeout=self.grace)
        except docker.errors.APIError:
            container.kill()

    def inspect(self, container: Any) -> Result | None:
        container.reload()
        if container.status in {"exited", "dead"}:
            state = container.attrs["State"]
            code = int(state.get("ExitCode", 1))
            return Result(
                "SUCCEEDED" if code == 0 else "FAILED",
                code,
                "OOM killed" if state.get("OOMKilled") else f"exit code {code}",
            )
        return None

    def artifacts(self, container: Any, max_bytes: int) -> Iterator[tuple[str, bytes]]:
        import tarfile

        try:
            stream, _ = container.get_archive("/output")
            data = io.BytesIO()
            for chunk in stream:
                if data.tell() + len(chunk) > max_bytes + 65536:
                    raise ValueError("artifact archive exceeds limit")
                data.write(chunk)
            data.seek(0)
            with tarfile.open(fileobj=data) as archive:
                for member in archive:
                    # Never extract worker-controlled archives onto the host.
                    if not member.isfile() or "/" in member.name.removeprefix("output/"):
                        continue
                    if member.size > max_bytes:
                        raise ValueError("artifact exceeds limit")
                    file = archive.extractfile(member)
                    if file:
                        yield member.name.removeprefix("output/"), file.read(max_bytes + 1)
        except docker.errors.NotFound:
            return

    def cleanup_orphans(self, worker_id: str) -> None:
        for container in self.client.containers.list(
            all=True, filters={"label": f"strata.worker={worker_id}"}
        ):
            self.stop(container)
            self.remove(container)
        for volume in self.client.volumes.list(filters={"label": f"strata.worker={worker_id}"}):
            volume.remove(force=True)

    def remove(self, container: Any) -> None:
        attempt_id = container.labels.get("strata.attempt")
        with suppress(docker.errors.NotFound):
            container.remove(force=True)
        if attempt_id:
            with suppress(docker.errors.NotFound):
                self.client.volumes.get(f"strata-output-{attempt_id}").remove(force=True)
            with suppress(docker.errors.NotFound):
                self.client.volumes.get(f"strata-input-{attempt_id}").remove(force=True)

    @staticmethod
    def monotonic() -> float:
        return time.monotonic()
