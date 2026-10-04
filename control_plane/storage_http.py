"""Verified file responses whose local cache pins survive the entire transfer."""

from starlette.concurrency import run_in_threadpool
from starlette.responses import FileResponse
from starlette.types import Receive, Scope, Send

from control_plane.storage import BlobStore


class BlobResponse(FileResponse):
    def __init__(
        self,
        store: BlobStore,
        digest: str,
        size: int,
        filename: str,
        media_type: str | None = None,
    ) -> None:
        super().__init__(
            store.root / digest,
            filename=filename,
            media_type=media_type,
            headers={"ETag": f'"{digest}"'},
        )
        self.store, self.digest, self.size = store, digest, size

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        pin = self.store.materialized(self.digest, self.size)
        path = await run_in_threadpool(pin.__enter__)
        try:
            self.path = str(path)
            await super().__call__(scope, receive, send)
        finally:
            await run_in_threadpool(pin.__exit__, None, None, None)
