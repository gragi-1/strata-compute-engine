"""Bounded content-addressed input cache; every hit verifies bytes before staging."""

import hashlib
import os
import re
import tempfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import BinaryIO, cast

from control_plane.config import Settings
from control_plane.storage import BlobStore


class InputCache:
    def __init__(self, settings: Settings, worker_id: str) -> None:
        config = settings.model_copy(
            update={
                "artifact_root": settings.worker_cache_root
                / hashlib.sha256(worker_id.encode()).hexdigest(),
                "storage_backend": "filesystem",
                "storage_max_local_bytes": settings.worker_cache_bytes,
            }
        )
        self.store = BlobStore(config)

    @contextmanager
    def open(
        self,
        digest: str,
        size: int,
        fill: Callable[[BinaryIO], None],
        progress: Callable[[], None],
    ) -> Iterator[BinaryIO]:
        self.store.key(digest)
        if size < 0 or size > self.store.config.storage_max_local_bytes:
            raise ValueError("input exceeds the worker cache budget")
        temporary: Path | None = None
        with self.store.local_guard():
            target = self.store.root / digest
            if target.is_symlink():
                raise ValueError("symbolic input cache paths are forbidden")
            valid = False
            if target.is_file() and target.stat().st_size == size:
                actual = hashlib.sha256()
                with target.open("rb") as stream:
                    for chunk in iter(lambda: stream.read(1024**2), b""):
                        progress()
                        actual.update(chunk)
                valid = actual.hexdigest() == digest
            if not valid:
                with suppress(FileNotFoundError):
                    target.unlink()
                self.evict(size)
                self.store.ensure_space(size)
                try:
                    with tempfile.NamedTemporaryFile(
                        dir=self.store.root, prefix="input-", delete=False
                    ) as stream:
                        temporary = Path(stream.name)
                        fill(cast(BinaryIO, stream))
                    if temporary.stat().st_size != size:
                        raise ValueError("input cache size mismatch")
                    with temporary.open("rb") as stream:
                        actual = hashlib.sha256()
                        for chunk in iter(lambda: stream.read(1024**2), b""):
                            progress()
                            actual.update(chunk)
                    if actual.hexdigest() != digest:
                        raise ValueError("input cache SHA-256 mismatch")
                    temporary.replace(target)
                finally:
                    if temporary:
                        with suppress(FileNotFoundError):
                            temporary.unlink()
            os.utime(target, None)
            # The shared process lock pins this path through container staging.
            with target.open("rb") as stream:
                yield stream

    def evict(self, required: int) -> None:
        files = sorted(
            (
                file
                for file in self.store.root.iterdir()
                if not file.is_symlink()
                and file.is_file()
                and re.fullmatch(r"[0-9a-f]{64}", file.name)
            ),
            key=lambda file: (file.stat().st_mtime, file.name),
        )
        used = sum(file.stat().st_size for file in files)
        for file in files:
            if used + required <= self.store.config.storage_max_local_bytes:
                break
            used -= file.stat().st_size
            file.unlink()
