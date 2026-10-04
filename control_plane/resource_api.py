import csv
import io
from typing import Annotated, Any, BinaryIO, cast

from fastapi import APIRouter, Header, Query, Request, Response
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from sqlalchemy import select

from control_plane.analytics import DatasetAnalytics, DatasetQuery
from control_plane.campaigns import CampaignService
from control_plane.datasets import DatasetService, storage_error
from control_plane.models import Campaign, Dataset, DatasetFile, DatasetVersion, WorkflowExpansion
from control_plane.schemas import (
    CampaignSubmit,
    JobView,
    NamedResource,
    VersionCreate,
    WorkflowSubmit,
)
from control_plane.services import DomainError, EngineService
from control_plane.storage import BlobStore
from control_plane.storage_http import BlobResponse


def resource_router(svc: EngineService) -> APIRouter:
    from control_plane.api import row_view

    router = APIRouter()
    datasets, campaigns = DatasetService(svc), CampaignService(svc)
    analytics = DatasetAnalytics(svc)

    @router.get("/campaigns/{campaign_id}/expansions")
    def expansions(campaign_id: str) -> list[dict[str, Any]]:
        campaigns.get(campaign_id)
        with svc.factory() as session:
            return [
                row_view(row)
                for row in session.scalars(
                    select(WorkflowExpansion)
                    .where(WorkflowExpansion.campaign_id == campaign_id)
                    .order_by(WorkflowExpansion.name)
                )
            ]

    @router.post("/datasets", status_code=201)
    def create_dataset(body: NamedResource) -> dict[str, Any]:
        return row_view(datasets.create(body))

    @router.get("/datasets")
    def list_datasets(
        limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)
    ) -> list[dict[str, Any]]:
        with svc.factory() as session:
            return [
                row_view(row)
                for row in session.scalars(
                    select(Dataset).order_by(Dataset.created_at.desc()).limit(limit).offset(offset)
                )
            ]

    @router.get("/datasets/{dataset_id}/versions")
    def versions(
        dataset_id: str, limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)
    ) -> list[dict[str, Any]]:
        with svc.factory() as session:
            if session.get(Dataset, dataset_id) is None:
                raise DomainError(404, "dataset not found")
            return [
                row_view(row)
                for row in session.scalars(
                    select(DatasetVersion)
                    .where(DatasetVersion.dataset_id == dataset_id)
                    .order_by(DatasetVersion.created_at.desc())
                    .limit(limit)
                    .offset(offset)
                )
            ]

    @router.post("/datasets/{dataset_id}/versions", status_code=201)
    def create_version(dataset_id: str, body: VersionCreate) -> dict[str, Any]:
        return row_view(datasets.version(dataset_id, body.label))

    @router.put("/dataset-versions/{version_id}/files/{name}", status_code=201)
    async def upload(version_id: str, name: str, request: Request) -> dict[str, Any]:
        # Raw streaming body avoids multipart spooling or loading an entire dataset into memory.
        datasets.validate_upload(version_id)
        from starlette.concurrency import run_in_threadpool

        store = BlobStore(svc.settings)
        await run_in_threadpool(store.ensure_space)
        try:
            with store.temporary("http-") as path, path.open("w+b") as stream:
                size = 0
                async for chunk in request.stream():
                    size += len(chunk)
                    if size > svc.settings.dataset_max_bytes:
                        raise DomainError(413, "dataset file exceeds configured limit")
                    await run_in_threadpool(store.write, cast(BinaryIO, stream), chunk)
                stream.seek(0)
                row = await run_in_threadpool(
                    datasets.upload, version_id, name, iter(lambda: stream.read(1024 * 1024), b"")
                )
        except OSError as exc:
            storage_error(exc)
        return row_view(row)

    @router.post("/dataset-versions/{version_id}/seal")
    def seal(version_id: str) -> dict[str, Any]:
        return row_view(datasets.seal(version_id))

    @router.get("/dataset-versions/{version_id}/files")
    def files(
        version_id: str, limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)
    ) -> list[dict[str, Any]]:
        with svc.factory() as session:
            if session.get(DatasetVersion, version_id) is None:
                raise DomainError(404, "dataset version not found")
            return [
                row_view(row)
                for row in session.scalars(
                    select(DatasetFile)
                    .where(DatasetFile.version_id == version_id)
                    .order_by(DatasetFile.name)
                    .limit(limit)
                    .offset(offset)
                )
            ]

    @router.get("/dataset-files/{file_id}/preview")
    def preview(file_id: str, limit: int = Query(50, ge=1, le=200)) -> JSONResponse:
        # Non-JSON scalar types (timestamps/decimals) in Parquet are encoded explicitly.
        return JSONResponse(jsonable_encoder(datasets.preview(file_id, limit)))

    @router.get("/dataset-files/{file_id}")
    def download(file_id: str) -> BlobResponse:
        with svc.factory() as session:
            row = session.get(DatasetFile, file_id)
            if row is None:
                raise DomainError(404, "dataset file not found")
        return BlobResponse(BlobStore(svc.settings), row.sha256, row.size, row.name)

    @router.post("/dataset-files/{file_id}/query")
    def query_dataset(file_id: str, body: DatasetQuery) -> JSONResponse:
        return JSONResponse(jsonable_encoder(analytics.query(file_id, body)))

    @router.post("/dataset-files/{file_id}/statistics")
    def dataset_statistics(file_id: str, body: DatasetQuery) -> JSONResponse:
        return JSONResponse(jsonable_encoder(analytics.statistics(file_id, body)))

    @router.post("/campaigns", status_code=201)
    def create_campaign(
        body: CampaignSubmit,
        response: Response,
        idempotency_key: Annotated[str | None, Header()] = None,
    ) -> dict[str, Any]:
        row, created = campaigns.create(body, idempotency_key)
        response.status_code = 201 if created else 200
        return campaigns.get(row.id)

    @router.post("/workflows", status_code=201)
    def workflow(
        body: WorkflowSubmit,
        response: Response,
        idempotency_key: Annotated[str | None, Header()] = None,
    ) -> dict[str, Any]:
        row, created = campaigns.workflow(body, idempotency_key)
        response.status_code = 201 if created else 200
        return campaigns.get(row.id)

    @router.get("/campaigns")
    def list_campaigns(
        limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)
    ) -> list[dict[str, Any]]:
        with svc.factory() as session:
            ids = list(
                session.scalars(
                    select(Campaign.id)
                    .order_by(Campaign.created_at.desc())
                    .limit(limit)
                    .offset(offset)
                )
            )
        return [campaigns.get(cid) for cid in ids]

    @router.get("/campaigns/{campaign_id}")
    def campaign(campaign_id: str) -> dict[str, Any]:
        return campaigns.get(campaign_id)

    @router.get("/campaigns/{campaign_id}/jobs", response_model=list[JobView])
    def campaign_jobs(
        campaign_id: str, limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)
    ) -> Any:
        return campaigns.jobs(campaign_id, limit, offset)

    @router.post("/campaigns/{campaign_id}/{action}")
    def campaign_action(campaign_id: str, action: str) -> dict[str, Any]:
        if action not in {"cancel", "retry"}:
            raise DomainError(404, "unknown campaign action")
        if action == "retry":
            return {
                "processed": campaigns.retry(campaign_id),
                "errors": [],
                "campaign": campaigns.get(campaign_id),
            }
        changed, errors = 0, []
        for job in campaigns.jobs(campaign_id, svc.settings.campaign_max_jobs):
            try:
                svc.cancel(job.id)
                changed += 1
            except DomainError as exc:
                errors.append({"job_id": job.id, "detail": str(exc)})
        return {"processed": changed, "errors": errors, "campaign": campaigns.get(campaign_id)}

    @router.get("/campaigns/{campaign_id}/results")
    def results(campaign_id: str, format: str = Query("json", pattern="^(json|csv)$")) -> Any:
        rows = campaigns.results(campaign_id)
        if format == "json":
            return rows
        fields = sorted({key for row in rows for key in row})
        stream = io.StringIO(newline="")
        writer = csv.DictWriter(stream, fields)
        writer.writeheader()
        # Neutralize spreadsheet formula injection in user-supplied result strings.
        writer.writerows(
            {
                k: "'" + v if isinstance(v, str) and v.startswith(("=", "+", "-", "@")) else v
                for k, v in row.items()
            }
            for row in rows
        )
        return Response(
            stream.getvalue(),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="campaign-{campaign_id}.csv"'},
        )

    return router
