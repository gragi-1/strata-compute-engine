import hashlib
from concurrent.futures import ThreadPoolExecutor

import pytest

from control_plane.config import Settings
from worker.cache import InputCache


def test_verified_hits_corruption_recovery_eviction_and_failed_transfer(tmp_path):
    cache = InputCache(Settings(worker_cache_root=tmp_path, worker_cache_bytes=12), "worker")
    content = b"measurements"
    digest = hashlib.sha256(content).hexdigest()
    fills = []

    def fill(stream):
        fills.append(1)
        stream.write(content)

    for _ in range(2):
        with cache.open(digest, len(content), fill, lambda: None) as stream:
            assert stream.read() == content
    assert len(fills) == 1
    (cache.store.root / digest).write_bytes(b"bad-contents")
    with cache.open(digest, len(content), fill, lambda: None) as stream:
        assert stream.read() == content
    assert len(fills) == 2
    other = hashlib.sha256(b"new").hexdigest()
    with cache.open(other, 3, lambda stream: stream.write(b"new"), lambda: None):
        assert not (cache.store.root / digest).exists()
    with (
        pytest.raises(ValueError, match="mismatch"),
        cache.open(digest, len(content), lambda stream: stream.write(b"bad"), lambda: None),
    ):
        pass
    assert not list(cache.store.root.glob("input-*"))
    with pytest.raises(ValueError, match="budget"), cache.open(digest, 13, fill, lambda: None):
        pass


def test_concurrent_cache_users_share_one_verified_download(tmp_path):
    config = Settings(worker_cache_root=tmp_path)
    first, second = InputCache(config, "worker"), InputCache(config, "worker")
    data = b"concurrent-cache"
    digest = hashlib.sha256(data).hexdigest()
    fills = []

    def use(cache):
        def fill(stream):
            fills.append(1)
            stream.write(data)

        with cache.open(digest, len(data), fill, lambda: None) as stream:
            return stream.read()

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert list(pool.map(use, [first, second])) == [data, data]
    assert len(fills) == 1
