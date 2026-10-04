"""Bounded resumable upload endpoints; authentication is enforced by the main router."""

from typing import Annotated, Any

from fastapi import APIRouter, Header, Query, Request
from pydantic import Field
from sqlalchemy import select
from starlette.concurrency import run_in_threadpool

from control_plane.errors import DomainError
from control_plane.models import UploadSession
from control_plane.schemas import StrictModel
from control_plane.services import EngineService
from control_plane.uploads import UploadService


class UploadCreate(StrictModel):
    name: str = Field(pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$")
    total_bytes: int = Field(ge=0, le=2**50)
    sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")


def upload_router(svc: EngineService) -> APIRouter:
    from control_plane.api import row_view

    router = APIRouter()
    uploads = UploadService(svc)

    @router.post("/dataset-versions/{version_id}/uploads", status_code=201)
    def create(version_id: str, body: UploadCreate) -> dict[str, Any]:
        return row_view(uploads.create(version_id, body.name, body.total_bytes, body.sha256))

    @router.get("/uploads")
    def listing(
        limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)
    ) -> list[dict[str, Any]]:
        with svc.factory() as session:
            return [
                row_view(row)
                for row in session.scalars(
                    select(UploadSession)
                    .order_by(UploadSession.created_at.desc())
                    .limit(limit)
                    .offset(offset)
                )
            ]

    @router.get("/uploads/{upload_id}")
    def get(upload_id: str) -> dict[str, Any]:
        return row_view(uploads.get(upload_id))

    @router.put("/uploads/{upload_id}/chunks/{offset}")
    async def chunk(
        upload_id: str,
        offset: int,
        request: Request,
        sha256: Annotated[str, Header(alias="X-Chunk-SHA256")],
    ) -> dict[str, Any]:
        upload = await run_in_threadpool(uploads.get, upload_id)
        content = bytearray()
        async for piece in request.stream():
            if len(content) + len(piece) > upload.chunk_bytes:
                raise DomainError(413, "chunk exceeds configured limit")
            content.extend(piece)
        row = await run_in_threadpool(uploads.chunk, upload_id, offset, bytes(content), sha256)
        return row_view(row)

    @router.post("/uploads/{upload_id}/complete")
    def complete(upload_id: str) -> dict[str, Any]:
        return row_view(uploads.complete(upload_id))

    @router.delete("/uploads/{upload_id}", status_code=204)
    def cancel(upload_id: str) -> None:
        uploads.cancel(upload_id)

    return router
