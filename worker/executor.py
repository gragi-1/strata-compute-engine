import csv
import io
import re
import tarfile
import tempfile
import time
from collections.abc import Callable, Iterator
from contextlib import suppress
from dataclasses import dataclass
from typing import Any

import docker

from control_plane.config import Settings
from control_plane.schemas import GPURegistration


@dataclass
class Result:
    outcome: str
    exit_code: int
    reason: str


def start_container(container: Any, progress: Callable[[], None]) -> None:
    """Reconcile a lost start reply without repeating an owned workload's start."""
    progress()
    try:
        container.start()
    except docker.errors.APIError:
        raise
    except OSError as exc:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            progress()
            try:
                container.reload()
                if container.status in {"running", "exited", "dead"}:
                    progress()
                    return
                if container.status != "created":
                    break
            except docker.errors.APIError:
                raise
            except OSError:
                pass
            time.sleep(0.05)
        raise RuntimeError("owned workload start did not resolve before its deadline") from exc
    progress()


class DockerExecutor:
    def __init__(
        self,
        allowed_images: list[str],
        grace: int = 5,
        client: Any = None,
        settings: Settings | None = None,
    ) -> None:
        self.client = client or docker.from_env(timeout=3)
        self.allowed_images = allowed_images
        self.grace = grace
        self.cluster_id = ""
        self.settings = settings or Settings()
        self.progress: Callable[[], None] = lambda: None
        if self.settings.production and not re.fullmatch(
            r"(?:[a-zA-Z0-9_.:/-]+@)?sha256:[0-9a-f]{64}", self.settings.storage_keeper_image
        ):
            raise ValueError("production output retention requires an immutable keeper image")

    def labels(self, worker_id: str, attempt_id: str) -> dict[str, str]:
        labels = {"strata.worker": worker_id, "strata.attempt": attempt_id}
        if self.cluster_id:
            labels["strata.cluster"] = self.cluster_id
        return labels

    def discover_gpus(self, image: str) -> list[GPURegistration]:
        if not image:
            return []
        if image not in self.allowed_images:
            raise ValueError("GPU discovery image is not allowlisted on worker")
        probe = self.client.containers.run(
            image,
            ["--query-gpu=uuid,name,memory.total", "--format=csv,noheader,nounits"],
            entrypoint=["nvidia-smi"],
            detach=True,
            device_requests=[docker.types.DeviceRequest(count=-1, capabilities=[["gpu"]])],
            user="65534:65534",
            read_only=True,
            network_disabled=True,
            cap_drop=["ALL"],
            security_opt=["no-new-privileges:true"],
            mem_limit="128m",
            nano_cpus=100000000,
            pids_limit=32,
            labels={"strata.purpose": "gpu-discovery"},
        )
        try:
            if probe.wait(timeout=5)["StatusCode"]:
                raise ValueError("GPU discovery failed; inspect NVIDIA runtime configuration")
            content = probe.logs(tail=64)
            if len(content) > 65536:
                raise ValueError("GPU discovery response exceeds limit")
            devices = [
                GPURegistration(
                    id=row[0].strip(), name=row[1].strip(), memory_mb=int(row[2].strip())
                )
                for row in csv.reader(io.StringIO(content.decode()))
                if row
            ]
            if not devices or len(devices) > 64:
                raise ValueError("GPU discovery returned an invalid device inventory")
            return devices
        finally:
            probe.remove(force=True)

    def create(self, assignment: Any, worker_id: str) -> Any:
        self.progress()
        if assignment.image not in self.allowed_images:
            raise ValueError("image is not allowlisted on worker")
        output = f"strata-output-{assignment.attempt_id}"
        options = {
            "type": "tmpfs",
            "device": "tmpfs",
            "o": "rw,nosuid,nodev,noexec,noswap,uid=65534,gid=65534,mode=0700,"
            f"size={self.settings.worker_output_bytes},nr_inodes={self.settings.worker_output_inodes}",
        }
        volume = self.client.volumes.create(
            name=output,
            driver="local",
            driver_opts=options,
            labels=self.labels(worker_id, assignment.attempt_id),
        )
        if volume.attrs.get("Options") != options or volume.attrs.get("Labels") != self.labels(
            worker_id, assignment.attempt_id
        ):
            raise ValueError("output volume ownership or quota does not match the assignment")
        keeper = None
        try:
            keeper = self.create_keeper(assignment, worker_id, output)
            return self.create_workload(assignment, worker_id)
        except BaseException:
            if keeper is not None:
                with suppress(docker.errors.NotFound):
                    keeper.remove(force=True)
            with suppress(docker.errors.NotFound):
                volume.remove()
            with suppress(docker.errors.NotFound):
                self.client.volumes.get(f"strata-input-{assignment.attempt_id}").remove()
            raise

    def create_keeper(self, assignment: Any, worker_id: str, output: str) -> Any:
        self.progress()
        image = self.client.images.get(self.settings.storage_keeper_image).id
        self.progress()
        ttl = min(604800, max(5, assignment.lease_seconds) + self.grace + 3)
        keeper = self.client.containers.create(
            image,
            ["-m", "worker.storage_keeper", str(ttl)],
            entrypoint=["python"],
            name=f"strata-keeper-{assignment.attempt_id}",
            user="65534:65534",
            network_disabled=True,
            read_only=True,
            cap_drop=["ALL"],
            security_opt=["no-new-privileges:true"],
            mem_limit="32m",
            memswap_limit="32m",
            pids_limit=8,
            nano_cpus=10000000,
            volumes={output: {"bind": "/retained", "mode": "ro"}},
            labels={
                **self.labels(worker_id, assignment.attempt_id),
                "strata.role": "output-keeper",
            },
            log_config=docker.types.LogConfig(
                type="json-file", config={"max-size": "4k", "max-file": "1"}
            ),
        )
        try:
            self.progress()
            start_error: OSError | None = None
            try:
                keeper.start()
            except docker.errors.APIError:
                raise
            except OSError as exc:
                # Docker can still be starting this exact ID after a lost response.
                start_error = exc
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                self.progress()
                try:
                    keeper.reload()
                    if keeper.status == "running":
                        if b"storage keeper ready" in keeper.logs(tail=1):
                            self.renew_storage(assignment.attempt_id)
                            return keeper
                    elif keeper.status != "created":
                        break
                except docker.errors.APIError:
                    raise
                except OSError:
                    # Retrying bounded inspection does not repeat the start mutation.
                    pass
                time.sleep(0.05)
            raise RuntimeError("bounded output keeper did not become ready") from start_error
        except BaseException:
            with suppress(docker.errors.NotFound):
                keeper.remove(force=True)
            raise

    def storage_alive(self, attempt_id: str) -> bool:
        try:
            keeper = self.client.containers.get(f"strata-keeper-{attempt_id}")
            return bool(keeper.status == "running")
        except docker.errors.NotFound:
            return False

    def renew_storage(self, attempt_id: str) -> None:
        self.client.containers.get(f"strata-keeper-{attempt_id}").kill(signal="SIGUSR1")

    def create_workload(self, assignment: Any, worker_id: str) -> Any:
        volumes = {f"strata-output-{assignment.attempt_id}": {"bind": "/output", "mode": "rw"}}
        if assignment.has_inputs:
            self.client.volumes.create(
                name=f"strata-input-{assignment.attempt_id}",
                labels=self.labels(worker_id, assignment.attempt_id),
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
            device_requests=[
                docker.types.DeviceRequest(
                    device_ids=list(assignment.gpu_ids), capabilities=[["gpu"]]
                )
            ]
            if assignment.gpu_ids
            else [],
            environment={
                "NVIDIA_VISIBLE_DEVICES": ",".join(assignment.gpu_ids) or "void",
                "NVIDIA_DRIVER_CAPABILITIES": "compute,utility",
                "STRATA_RUNTIME_CONTEXT": getattr(assignment, "runtime_context", ""),
            },
            user="65534:65534",
            network_disabled=True,
            read_only=True,
            cap_drop=["ALL"],
            security_opt=["no-new-privileges:true"],
            pids_limit=128,
            init=True,
            tmpfs={"/tmp": "rw,nosuid,nodev,size=64m"},
            volumes=volumes,
            labels=self.labels(worker_id, assignment.attempt_id),
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
            environment={"NVIDIA_VISIBLE_DEVICES": "void"},
            network_disabled=True,
            cap_drop=["ALL"],
            security_opt=["no-new-privileges:true"],
            user="65534:65534",
            mem_limit="64m",
            volumes={f"strata-input-{assignment.attempt_id}": {"bind": "/staging", "mode": "rw"}},
            labels=self.labels(worker_id, assignment.attempt_id),
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

    def write_runtime(self, container: Any, name: str, content: bytes) -> None:
        if name not in {"runtime.py", "reply.json"} or len(content) > 16384:
            raise ValueError("invalid runtime response")
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w") as archive:
            directory = tarfile.TarInfo(".strata")
            directory.type, directory.mode = tarfile.DIRTYPE, 0o700
            directory.uid = directory.gid = 65534
            archive.addfile(directory)
            entry = tarfile.TarInfo(".strata/" + name)
            entry.size, entry.mode = len(content), 0o600
            entry.uid = entry.gid = 65534
            archive.addfile(entry, io.BytesIO(content))
        if not container.put_archive("/output", buffer.getvalue()):
            raise RuntimeError("runtime response was not written")

    def read_runtime(self, container: Any) -> bytes:
        try:
            stream, _ = container.get_archive("/output/.strata/request.json")
            data = io.BytesIO()
            for chunk in stream:
                if data.tell() + len(chunk) > 65536:
                    raise ValueError("runtime archive exceeds limit")
                data.write(chunk)
            data.seek(0)
            with tarfile.open(fileobj=data) as archive:
                entries = archive.getmembers()
                if len(entries) != 1 or not entries[0].isfile() or entries[0].size > 16384:
                    raise ValueError("invalid runtime request file")
                file = archive.extractfile(entries[0])
                assert file is not None
                return file.read(16385)
        except docker.errors.NotFound:
            return b""

    def cleanup_orphans(self, worker_id: str) -> None:
        labels = [f"strata.worker={worker_id}"]
        if self.cluster_id:
            labels.append(f"strata.cluster={self.cluster_id}")
        for container in self.client.containers.list(all=True, filters={"label": labels}):
            self.stop(container)
            container.remove(force=True)
        for volume in self.client.volumes.list(filters={"label": labels}):
            volume.remove(force=True)

    def remove(self, container: Any) -> None:
        attempt_id = container.labels.get("strata.attempt")
        with suppress(docker.errors.NotFound):
            container.remove(force=True)
        if attempt_id:
            with suppress(docker.errors.NotFound):
                self.client.containers.get(f"strata-keeper-{attempt_id}").remove(force=True)
            with suppress(docker.errors.NotFound):
                self.client.volumes.get(f"strata-output-{attempt_id}").remove(force=True)
            with suppress(docker.errors.NotFound):
                self.client.volumes.get(f"strata-input-{attempt_id}").remove(force=True)

    @staticmethod
    def monotonic() -> float:
        return time.monotonic()
