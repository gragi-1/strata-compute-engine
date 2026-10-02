import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, Any

from fastapi import Depends, FastAPI, Header, Query, Request, Response
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
from opentelemetry.propagate import extract
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, generate_latest
from sqlalchemy import select, text
from sqlalchemy.exc import SQLAlchemyError

from control_plane.config import Settings
from control_plane.database import make_engine, sessions
from control_plane.domain import JobStatus
from control_plane.logging import configure_logging
from control_plane.metrics import DurableCollector
from control_plane.models import Artifact, Attempt, JobEvent, Worker
from control_plane.schemas import JobSubmit, JobView
from control_plane.services import DomainError, EngineService
from control_plane.tracing import configure_tracing, tracer


def row_view(row: Any, exclude: set[str] | None = None) -> dict[str, Any]:
    return {
        c.name: getattr(row, c.name)
        for c in row.__table__.columns
        if c.name not in (exclude or set())
    }


def create_app(settings: Settings | None = None, service: EngineService | None = None) -> FastAPI:
    config = settings or Settings()
    svc = service or EngineService(sessions(make_engine(config.database_url)), config)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        configure_logging()
        configure_tracing("strata-api")
        yield

    app = FastAPI(title="Strata Compute Engine", version="1.0.0", lifespan=lifespan)
    app.state.service = svc
    registry = CollectorRegistry()
    registry.register(DurableCollector(svc))

    @app.exception_handler(DomainError)
    async def domain_error(request: Request, exc: DomainError) -> JSONResponse:
        return JSONResponse(
            {"detail": str(exc)},
            status_code=exc.code,
            headers={"Retry-After": "2"} if exc.code == 429 else None,
        )

    @app.middleware("http")
    async def traced(request: Request, call_next: Any) -> Response:
        with tracer.start_as_current_span(
            f"{request.method} {request.url.path}",
            context=extract(
                dict(request.headers),
            ),
        ):
            return await call_next(request)  # type: ignore[no-any-return]

    @app.post("/jobs", response_model=JobView, status_code=201)
    def submit(
        body: JobSubmit,
        response: Response,
        idempotency_key: Annotated[str | None, Header()] = None,
    ) -> Job:
        job, created = svc.submit(body, idempotency_key)
        response.status_code = 201 if created else 200
        return job

    @app.get("/jobs", response_model=list[JobView])
    def jobs(
        status: JobStatus | None = None,
        limit: int = Query(100, ge=1, le=1000),
        offset: int = Query(0, ge=0),
    ) -> list[Job]:
        return svc.list_jobs(status, limit, offset)

    @app.get("/jobs/{job_id}", response_model=JobView)
    def job(job_id: str) -> Job:
        return svc.get_job(job_id)

    @app.post("/jobs/{job_id}/cancel", response_model=JobView)
    def cancel(job_id: str) -> Job:
        return svc.cancel(job_id)

    @app.post("/jobs/{job_id}/retry", response_model=JobView)
    def retry(job_id: str) -> Job:
        return svc.retry(job_id)

    @app.get("/jobs/{job_id}/attempts")
    def attempts(job_id: str) -> list[dict[str, Any]]:
        with svc.factory() as session:
            svc.job(session, job_id)
            return [
                row_view(a, {"lease_token", "worker_session", "logs"})
                for a in session.scalars(
                    select(Attempt).where(Attempt.job_id == job_id).order_by(Attempt.number),
                )
            ]

    @app.get("/jobs/{job_id}/events")
    def events(
        job_id: str, after: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=1000)
    ) -> list[dict[str, Any]]:
        with svc.factory() as session:
            svc.job(session, job_id)
            return [
                row_view(e)
                for e in session.scalars(
                    select(JobEvent)
                    .where(
                        JobEvent.job_id == job_id,
                        JobEvent.id > after,
                    )
                    .order_by(JobEvent.id)
                    .limit(limit)
                )
            ]

    @app.get("/jobs/{job_id}/logs", response_class=PlainTextResponse)
    def logs(job_id: str, attempt: int | None = Query(None, ge=1)) -> str:
        with svc.factory() as session:
            svc.job(session, job_id)
            query = select(Attempt).where(Attempt.job_id == job_id).order_by(Attempt.number.desc())
            if attempt is not None:
                query = query.where(Attempt.number == attempt)
            row = session.scalar(query.limit(1))
            return row.logs if row else ""

    @app.get("/jobs/{job_id}/artifacts")
    def artifacts(
        job_id: str, limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)
    ) -> list[dict[str, Any]]:
        with svc.factory() as session:
            svc.job(session, job_id)
            return [
                {**row_view(a), "uri": f"/artifacts/{a.id}"}
                for a in session.scalars(
                    select(Artifact)
                    .where(Artifact.job_id == job_id)
                    .order_by(Artifact.created_at)
                    .limit(limit)
                    .offset(offset),
                )
            ]

    @app.get("/artifacts/{artifact_id}")
    def artifact(artifact_id: str) -> FileResponse:
        with svc.factory() as session:
            row = session.get(Artifact, artifact_id)
            if row is None:
                raise DomainError(404, "artifact not found")
            path = config.artifact_root / row.sha256
            if not path.is_file():
                raise DomainError(503, "artifact bytes are unavailable")
            return FileResponse(
                path,
                filename=row.name,
                media_type=row.content_type,
                headers={"ETag": f'"{row.sha256}"'},
            )

    @app.get("/workers")
    def workers() -> list[dict[str, Any]]:
        with svc.factory() as session:
            return [
                row_view(w, {"session_id"})
                for w in session.scalars(select(Worker).order_by(Worker.id))
            ]

    @app.get("/workers/{worker_id}")
    def worker(worker_id: str) -> dict[str, Any]:
        with svc.factory() as session:
            return row_view(svc.worker(session, worker_id), {"session_id"})

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/ready")
    def ready() -> JSONResponse:
        try:
            with svc.factory() as session:
                session.execute(text("SELECT id FROM admission WHERE id=1")).scalar_one()
            return JSONResponse({"status": "ready"})
        except SQLAlchemyError:
            return JSONResponse({"status": "unavailable"}, status_code=503)

    @app.get("/metrics")
    def metrics() -> Response:
        return Response(generate_latest(registry), headers={"Content-Type": CONTENT_TYPE_LATEST})

    # Internal credentials are never returned by client-facing endpoints.
    def worker_auth(authorization: Annotated[str | None, Header()] = None) -> None:
        if authorization is None or not secrets.compare_digest(
            authorization,
            f"Bearer {config.worker_token}",
        ):
            raise DomainError(401, "invalid worker token")

    @app.post("/internal/scheduler/tick", dependencies=[Depends(worker_auth)])
    def tick() -> dict[str, int]:
        from scheduler.core import Scheduler

        recovered, assigned = Scheduler(svc).tick()
        return {"recovered": recovered, "assigned": assigned}

    return app


from control_plane.models import Job  # noqa: E402

app = create_app()
