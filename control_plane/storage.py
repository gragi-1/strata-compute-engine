"""Content-addressed storage with verified, bounded local materialization."""

import hashlib
import os
import re
import shutil
import tempfile
import time
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from contextvars import ContextVar
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, BinaryIO
from uuid import uuid4

from control_plane.config import Settings
from control_plane.errors import DomainError

_held_roots: ContextVar[frozenset[str]] = ContextVar("strata_storage_locks", default=frozenset())


def try_lock(stream: BinaryIO) -> bool:
    """Take a nonblocking OS lock; the kernel releases it when the owner exits."""
    try:
        stream.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            posix: Any = fcntl
            posix.flock(stream.fileno(), posix.LOCK_EX | posix.LOCK_NB)
        return True
    except (BlockingIOError, PermissionError):
        return False


def unlock(stream: BinaryIO) -> None:
    stream.seek(0)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
    else:
        import fcntl

        posix: Any = fcntl
        posix.flock(stream.fileno(), posix.LOCK_UN)


def checksum(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


class BlobStore:
    def __init__(self, config: Settings) -> None:
        self.config = config
        self.root = config.artifact_root
        self.remote = config.storage_backend == "s3"
        self._client: Any = None

    @property
    def client(self) -> Any:
        if self._client is None:
            import boto3
            from botocore.config import Config

            self._client = boto3.client(
                "s3",
                endpoint_url=self.config.s3_endpoint_url or None,
                region_name=self.config.s3_region,
                config=Config(
                    connect_timeout=5,
                    read_timeout=30,
                    retries={"max_attempts": 3},
                    s3={"addressing_style": "path"},
                ),
            )
        return self._client

    def key(self, digest: str) -> str:
        if not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise DomainError(422, "invalid blob digest")
        return self.config.s3_prefix.rstrip("/") + "/" + digest if self.config.s3_prefix else digest

    def ensure_space(self, size: int = 0) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        if shutil.disk_usage(self.root).free < size + self.config.storage_min_free_bytes:
            raise DomainError(503, "storage capacity exhausted; contact the administrator")
        used = 0
        for file in self.root.rglob("*"):
            with suppress(FileNotFoundError):
                if file.is_file() and not file.is_symlink() and file.name != ".storage.lock":
                    used += file.stat().st_size
        if used + size > self.config.storage_max_local_bytes:
            raise DomainError(503, "local storage budget exhausted; contact the administrator")

    def write(self, stream: BinaryIO, content: bytes) -> None:
        with self.local_guard():
            self.ensure_space(len(content))
            stream.write(content)
            stream.flush()

    @contextmanager
    def temporary(self, prefix: str, *, directory: bool = False) -> Iterator[Path]:
        """Protect staging/pins with an OS lifetime lease, even during long idle reads."""
        if prefix not in {"http-", "upload-", "staging-", "assembly-", "pin-"}:
            raise ValueError("unsupported temporary storage prefix")
        name = prefix + uuid4().hex
        path, lease = self.root / name, self.root / (".lease-" + name)
        with self.local_guard():
            self.ensure_space(1)
            handle = lease.open("x+b")
            locked = False
            try:
                handle.write(b"0")
                handle.flush()
                locked = try_lock(handle)
                if not locked:
                    raise DomainError(503, "temporary storage lease is unavailable")
                if directory:
                    path.mkdir()
                else:
                    path.touch(exist_ok=False)
            except BaseException:
                if locked:
                    unlock(handle)
                handle.close()
                lease.unlink()
                raise
        try:
            yield path
        finally:
            with self.local_guard():
                try:
                    if directory and path.exists():
                        self.remove_temporary(path)
                    else:
                        with suppress(FileNotFoundError):
                            path.unlink()
                finally:
                    unlock(handle)
                    handle.close()
                    lease.unlink(missing_ok=True)

    def remove_temporary(self, path: Path) -> None:
        """Remove only our immediate child, refusing linked paths or descendants."""
        root = self.root.resolve()
        if path.is_symlink() or path.resolve().parent != root:
            raise DomainError(503, "refusing a linked or external temporary storage path")
        if path.is_dir():
            if any(
                child.is_symlink() or not child.resolve().is_relative_to(root)
                for child in path.rglob("*")
            ):
                raise DomainError(503, "refusing linked temporary storage contents")
            shutil.rmtree(path)
        else:
            path.unlink(missing_ok=True)

    def collect_temporary(
        self, *, apply: bool = False, limit: int = 1000, now: datetime | None = None
    ) -> dict[str, Any]:
        """Reap crashed managed lifetimes; active OS leases survive regardless of age."""
        if not 1 <= limit <= 10000:
            raise DomainError(422, "temporary collection limit must be 1..10000")
        cutoff = (now or datetime.now(UTC)).timestamp() - self.config.blob_retention_seconds
        candidates, deleted, active = [], [], 0
        with self.local_guard():
            for lease in sorted(self.root.glob(".lease-*")):
                name = lease.name.removeprefix(".lease-")
                if (
                    lease.is_symlink()
                    or not lease.is_file()
                    or not re.fullmatch(r"(?:http|upload|staging|assembly|pin)-[0-9a-f]{32}", name)
                    or lease.stat().st_mtime > cutoff
                ):
                    continue
                handle = lease.open("r+b")
                if not try_lock(handle):
                    handle.close()
                    active += 1
                    continue
                try:
                    path = self.root / name
                    candidates.append(name)
                    if apply:
                        self.remove_temporary(path)
                        deleted.append(name)
                finally:
                    unlock(handle)
                    handle.close()
                if apply:
                    lease.unlink()
                if len(candidates) >= limit:
                    break
            # These writers hold the root lock for their entire lifetime.
            for path in sorted(self.root.iterdir()):
                if len(candidates) >= limit:
                    break
                if (
                    path.is_symlink()
                    or not path.is_file()
                    or not re.fullmatch(r"(?:blob-[0-9a-f]{32}|download-[a-zA-Z0-9_]+)", path.name)
                    or path.stat().st_mtime > cutoff
                ):
                    continue
                candidates.append(path.name)
                if apply:
                    self.remove_temporary(path)
                    deleted.append(path.name)
        return {
            "dry_run": not apply,
            "candidates": candidates,
            "deleted": deleted,
            "active_skipped": active,
            "grace_seconds": self.config.blob_retention_seconds,
        }

    @contextmanager
    def local_guard(self) -> Iterator[None]:
        """Serialize physical allocations across processes sharing this local root."""
        self.root.mkdir(parents=True, exist_ok=True)
        root = str(self.root.resolve())
        held = _held_roots.get()
        if root in held:
            yield
            return
        with (self.root / ".storage.lock").open("a+b") as lock:
            if lock.tell() == 0:
                lock.write(b"0")
                lock.flush()
            deadline = time.monotonic() + self.config.storage_lock_timeout_seconds
            while True:
                try:
                    if os.name == "nt":
                        import msvcrt

                        lock.seek(0)
                        msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
                    else:
                        import fcntl

                        posix_lock: Any = fcntl
                        posix_lock.flock(lock.fileno(), posix_lock.LOCK_EX | posix_lock.LOCK_NB)
                    break
                except OSError as exc:
                    if time.monotonic() >= deadline:
                        raise DomainError(503, "local storage is busy; retry later") from exc
                    time.sleep(0.05)
            token = _held_roots.set(held | {root})
            try:
                yield
            finally:
                _held_roots.reset(token)
                if os.name == "nt":
                    lock.seek(0)
                    msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    posix_lock.flock(lock.fileno(), posix_lock.LOCK_UN)

    def put_file(self, digest: str, source: Path) -> None:
        with self.local_guard():
            self._put_file(digest, source)

    def _put_file(self, digest: str, source: Path) -> None:
        key = self.key(digest)
        if checksum(source) != digest:
            raise DomainError(422, "blob checksum mismatch")
        if self.remote:
            from boto3.s3.transfer import TransferConfig
            from botocore.exceptions import BotoCoreError, ClientError

            try:
                self.client.upload_file(
                    str(source),
                    self.config.s3_bucket,
                    key,
                    ExtraArgs={"Metadata": {"sha256": digest}},
                    Config=TransferConfig(
                        multipart_threshold=8 * 1024**2,
                        multipart_chunksize=8 * 1024**2,
                        max_concurrency=2,
                    ),
                )
            except (BotoCoreError, ClientError) as exc:
                raise DomainError(503, "object storage upload failed") from exc
            return
        target = self.root / digest
        if target.is_file() and checksum(target) == digest:
            return
        self.ensure_space(source.stat().st_size)
        temporary = self.root / ("blob-" + uuid4().hex)
        try:
            shutil.copyfile(source, temporary)
            temporary.replace(target)
        finally:
            with suppress(FileNotFoundError):
                temporary.unlink()

    def put_bytes(self, digest: str, content: bytes) -> None:
        with self.local_guard():
            self.ensure_space(len(content))
            with self.temporary("staging-", directory=True) as directory:
                path = directory / "content"
                path.write_bytes(content)
                self.put_file(digest, path)

    def get_path(self, digest: str, size: int | None = None) -> Path:
        with self.local_guard():
            return self._get_path(digest, size)

    def _get_path(self, digest: str, size: int | None = None) -> Path:
        key = self.key(digest)
        target = self.root / digest
        if target.is_file() and (size is None or target.stat().st_size == size):
            if checksum(target) == digest:
                if self.remote:
                    os.utime(target, None)
                return target
            if not self.remote:
                raise DomainError(503, "stored blob checksum mismatch")
        if not self.remote:
            raise DomainError(503, "blob bytes are unavailable")
        from botocore.exceptions import BotoCoreError, ClientError

        temporary: Path | None = None
        try:
            response = self.client.get_object(Bucket=self.config.s3_bucket, Key=key)
            with response["Body"] as body:
                expected = int(response["ContentLength"])
                if size is not None and expected != size:
                    raise DomainError(503, "object storage size mismatch")
                if expected > self.config.storage_cache_bytes:
                    raise DomainError(413, "blob exceeds the configured local cache budget")
                self.trim_cache(max(0, self.config.storage_cache_bytes - expected))
                self.ensure_space(expected)
                digestor, actual = hashlib.sha256(), 0
                with tempfile.NamedTemporaryFile(
                    dir=self.root, prefix="download-", delete=False
                ) as stream:
                    temporary = Path(stream.name)
                    for chunk in iter(lambda: body.read(1024**2), b""):
                        actual += len(chunk)
                        if actual > expected:
                            raise DomainError(503, "object storage size mismatch")
                        digestor.update(chunk)
                        stream.write(chunk)
                if actual != expected or digestor.hexdigest() != digest:
                    raise DomainError(503, "object storage checksum or size mismatch")
                temporary.replace(target)
                return target
        except (BotoCoreError, ClientError) as exc:
            raise DomainError(503, "object storage download failed") from exc
        finally:
            if temporary:
                with suppress(FileNotFoundError):
                    temporary.unlink()

    @contextmanager
    def materialized(self, digest: str, size: int | None = None) -> Iterator[Path]:
        """Pin a verified S3 cache file while an external reader needs its path."""
        if not self.remote:
            yield self.get_path(digest, size)
            return
        with self.temporary("pin-") as pin:
            with self.local_guard():
                path = self._get_path(digest, size)
                pin.unlink()
                try:
                    os.link(path, pin)
                except OSError:
                    self.ensure_space(path.stat().st_size)
                    shutil.copyfile(path, pin)
            yield pin

    def trim_cache(self, target_bytes: int = 0, *, apply: bool = True) -> dict[str, Any]:
        """Evict local S3 copies; pinned reads and authoritative objects survive."""
        if not self.remote:
            raise DomainError(409, "filesystem blobs are authoritative and cannot be evicted")
        with self.local_guard():
            files = sorted(
                (
                    file
                    for file in self.root.iterdir()
                    if file.is_file()
                    and not file.is_symlink()
                    and re.fullmatch(r"[0-9a-f]{64}", file.name)
                ),
                key=lambda file: (file.stat().st_mtime, file.name),
            )
            used = sum(file.stat().st_size for file in files)
            candidates = []
            freed = 0
            for file in files:
                if used - freed <= target_bytes:
                    break
                candidates.append(file.name)
                freed += file.stat().st_size
                if apply:
                    file.unlink()
            return {
                "dry_run": not apply,
                "candidates": candidates,
                "freed_bytes": freed if apply else 0,
                "candidate_bytes": freed,
            }

    def inventory(self) -> Iterator[tuple[str, int]]:
        for digest, size, _ in self.inventory_details():
            yield digest, size

    def inventory_details(self) -> Iterator[tuple[str, int, datetime]]:
        if self.remote:
            prefix = self.config.s3_prefix.rstrip("/") + "/" if self.config.s3_prefix else ""
            pages = self.client.get_paginator("list_objects_v2").paginate(
                Bucket=self.config.s3_bucket,
                Prefix=prefix,
            )
            for page in pages:
                for row in page.get("Contents", []):
                    name = row["Key"][len(prefix) :]
                    if re.fullmatch(r"[0-9a-f]{64}", name):
                        yield name, int(row["Size"]), row["LastModified"].astimezone(UTC)
        elif self.root.exists():
            for file in self.root.iterdir():
                if (
                    not file.is_symlink()
                    and file.is_file()
                    and re.fullmatch(r"[0-9a-f]{64}", file.name)
                ):
                    info = file.stat()
                    yield file.name, info.st_size, datetime.fromtimestamp(info.st_mtime, UTC)

    def delete(self, digest: str) -> None:
        key = self.key(digest)
        with self.local_guard():
            target = self.root / digest
            if target.is_symlink():
                raise DomainError(503, "refusing to remove a symbolic storage path")
            if self.remote:
                from botocore.exceptions import BotoCoreError, ClientError

                try:
                    self.client.delete_object(Bucket=self.config.s3_bucket, Key=key)
                except (BotoCoreError, ClientError) as exc:
                    raise DomainError(503, "object storage deletion failed") from exc
            with suppress(FileNotFoundError):
                target.unlink()
