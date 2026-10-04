import hashlib
import logging
import re
import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, Any

from fastapi import Depends, FastAPI, Header, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from opentelemetry.propagate import extract
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, generate_latest
from sqlalchemy import select, text
from sqlalchemy.exc import SQLAlchemyError

from control_plane import __version__
from control_plane.access import access_scope, audit, require_platform_admin
from control_plane.auth import authorize
from control_plane.cluster import ClusterService
from control_plane.config import Settings
from control_plane.database import make_engine, sessions
from control_plane.domain import JobStatus
from control_plane.errors import AdmissionPaused
from control_plane.identity import IdentityService
from control_plane.logging import configure_logging, database_failure
from control_plane.metrics import DurableCollector
from control_plane.models import (
    Artifact,
    Attempt,
    JobEvent,
    MaintenanceState,
    Project,
    ProvisionedWorker,
    User,
    Worker,
)
from control_plane.provisioning import PoolService, PoolUpdate
from control_plane.schemas import AdmissionUpdate, JobSubmit, JobView
from control_plane.services import DomainError, EngineService
from control_plane.storage_http import BlobResponse
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
    identity = IdentityService(svc)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        configure_logging()
        configure_tracing("strata-api")
        yield

    app = FastAPI(title="Strata Compute Engine", version=__version__, lifespan=lifespan)
    app.state.service = svc
    registry = CollectorRegistry()
    registry.register(DurableCollector(svc))

    @app.exception_handler(DomainError)
    async def domain_error(request: Request, exc: DomainError) -> JSONResponse:
        return JSONResponse(
            {"detail": str(exc)},
            status_code=exc.code,
            headers={"Retry-After": "30"}
            if isinstance(exc, AdmissionPaused)
            else {"Retry-After": "2"}
            if exc.code == 429
            else None,
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        # Validation responses must not reflect submitted passwords or other credential values.
        return JSONResponse(
            {
                "detail": [
                    {key: error[key] for key in ("loc", "msg", "type") if key in error}
                    for error in exc.errors()
                ]
            },
            status_code=422,
        )

    @app.exception_handler(SQLAlchemyError)
    async def store_error(request: Request, exc: SQLAlchemyError) -> JSONResponse:
        # DBAPI messages can contain SQL, connection addresses and submitted values.
        logging.getLogger(__name__).warning("durable_store_failed: %s", database_failure(exc))
        return JSONResponse(
            {
                "detail": "durable store is temporarily unavailable; "
                "retry with the same idempotency key"
            },
            status_code=503,
            headers={"Retry-After": "2"},
        )

    @app.middleware("http")
    async def traced(request: Request, call_next: Any) -> Response:
        public = request.url.path in {
            "/",
            "/health",
            "/ready",
            "/metrics",
            "/docs",
            "/openapi.json",
            "/docs/oauth2-redirect",
            "/auth/config",
            "/auth/login",
            "/auth/oidc/start",
            "/auth/oidc/callback",
            "/auth/oidc/session",
        } or request.url.path.startswith("/app/")
        actor = None
        access_method = (
            "GET"
            if request.method == "POST"
            and re.fullmatch(r"/dataset-files/[^/]+/(query|statistics)", request.url.path)
            else request.method
        )
        if not public and not request.url.path.startswith("/internal/"):
            try:
                if config.identity_enabled:
                    actor = identity.authenticate(request.headers.get("authorization"))
                    managed = request.url.path.startswith(
                        ("/auth/", "/projects", "/workers", "/cluster/")
                    ) or request.url.path in {"/session", "/operations"}
                    if not managed:
                        target = request.headers.get("x-strata-project")
                        if not target:
                            raise DomainError(400, "select a project using X-Strata-Project")
                        actor = identity.project_access(actor, target, access_method)
                    elif request.url.path == "/session" and request.headers.get("x-strata-project"):
                        actor = identity.project_access(
                            actor, request.headers["x-strata-project"], "GET"
                        )
                    request.state.role = actor.role or (
                        "admin" if actor.platform_admin else "viewer"
                    )
                else:
                    with svc.factory() as session:
                        if session.scalar(select(Project.id).limit(1)):
                            raise DomainError(
                                503, "enable individual identity for project-owned data"
                            )
                    request.state.role = authorize(
                        config, access_method, request.headers.get("authorization")
                    )
            except DomainError as exc:
                return JSONResponse(
                    {"detail": str(exc)},
                    status_code=exc.code,
                    headers={"WWW-Authenticate": "Bearer"} if exc.code == 401 else {},
                )
            except SQLAlchemyError as exc:
                # Authentication happens outside FastAPI's endpoint exception handlers.
                return await store_error(request, exc)
        with tracer.start_as_current_span(
            f"{request.method} {request.url.path}",
            context=extract(
                dict(request.headers),
            ),
        ):
            with access_scope(actor):
                response: Response = await call_next(request)
            response.headers["X-Content-Type-Options"] = "nosniff"
            response.headers["Referrer-Policy"] = "same-origin"
            if replica_id := getattr(app.state, "replica_id", None):
                response.headers["X-Strata-Replica"] = replica_id
            if request.url.path == "/" or request.url.path.startswith("/app/"):
                response.headers["Cache-Control"] = "no-cache"
            return response

    @app.get("/session")
    def session_role(request: Request) -> dict[str, Any]:
        return {
            "role": request.state.role,
            "authentication_enabled": bool(config.api_keys) or config.identity_enabled,
            "identity_enabled": config.identity_enabled,
        }

    @app.get("/operations")
    def operations(request: Request) -> list[dict[str, Any]]:
        if config.identity_enabled:
            require_platform_admin()
        elif request.state.role != "admin":
            raise DomainError(403, "administrator access is required")
        with svc.factory() as session:
            return [
                row_view(row)
                for row in session.scalars(select(MaintenanceState).order_by(MaintenanceState.name))
            ]

    @app.get("/cluster/admission")
    def cluster_admission(request: Request) -> dict[str, Any]:
        if config.identity_enabled:
            require_platform_admin()
        elif request.state.role != "admin":
            raise DomainError(403, "administrator access is required")
        return ClusterService(svc).admission()

    @app.patch("/cluster/admission")
    def update_admission(body: AdmissionUpdate, request: Request) -> dict[str, Any]:
        if config.identity_enabled:
            require_platform_admin()
        elif request.state.role != "admin":
            raise DomainError(403, "administrator access is required")
        return ClusterService(svc).update(body)

    @app.post("/workers/{worker_id}/{action}")
    def worker_action(worker_id: str, action: str, request: Request) -> dict[str, Any]:
        if config.identity_enabled:
            require_platform_admin()
        if request.state.role != "admin":
            raise DomainError(403, "administrator access is required")
        if action not in {"drain", "resume"}:
            raise DomainError(404, "unknown worker action")
        with svc.factory.begin() as session:
            worker = svc.worker(session, worker_id, lock=True)
            managed = session.get(ProvisionedWorker, worker_id)
            if action == "resume" and managed and managed.phase in {"DRAINING", "REMOVED"}:
                raise DomainError(409, "worker is being retired by its pool controller")
            if (
                worker.status == "LOST"
                or (svc.now(session) - worker.last_heartbeat).total_seconds()
                >= config.worker_timeout
            ):
                raise DomainError(409, "worker is not live")
            worker.status = "DRAINING" if action == "drain" else "HEALTHY"
            audit(session, svc.now(session), "WORKER_" + action.upper(), worker.id)
            return row_view(worker, {"session_id"})

    def pool_admin(request: Request) -> None:
        if config.identity_enabled:
            require_platform_admin()
        elif request.state.role != "admin":
            raise DomainError(403, "administrator access is required")

    @app.get("/cluster/pools")
    def worker_pools(request: Request) -> list[dict[str, Any]]:
        pool_admin(request)
        return PoolService(svc).list_pools()

    @app.get("/cluster/pools/{pool_id}/workers")
    def provisioned_workers(pool_id: str, request: Request) -> list[dict[str, Any]]:
        pool_admin(request)
        return PoolService(svc).workers(pool_id)

    @app.patch("/cluster/pools/{pool_id}")
    def update_worker_pool(pool_id: str, body: PoolUpdate, request: Request) -> dict[str, Any]:
        pool_admin(request)
        return PoolService(svc).update(pool_id, body)

    @app.post("/jobs", response_model=JobView, status_code=201)
    def submit(
        body: JobSubmit,
        response: Response,
        idempotency_key: Annotated[str | None, Header()] = None,
    ) -> Job:
        job, created = svc.submit(body, idempotency_key)
        response.status_code = 201 if created else 200
        return job

    @app.get("/execution/config")
    def execution_config() -> dict[str, Any]:
        return {"approved_images": config.allowed_images}

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

    @app.get("/jobs/{job_id}/log-snapshot")
    def log_snapshot(job_id: str, cursor: str = Query("", max_length=64)) -> dict[str, Any]:
        with svc.factory() as session:
            job = svc.job(session, job_id)
            attempt = session.scalar(
                select(Attempt)
                .where(Attempt.job_id == job_id)
                .order_by(Attempt.number.desc())
                .limit(1)
            )
            number, content = (attempt.number, attempt.logs) if attempt else (None, "")
            revision = hashlib.sha256(f"{number}:{job.status}:{content}".encode()).hexdigest()
            return {
                "revision": revision,
                "attempt": number,
                "status": job.status,
                "started_at": job.started_at,
                "finished_at": job.finished_at,
                "retry_count": job.retry_count,
                "changed": revision != cursor,
                "text": content if revision != cursor else None,
            }

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
                    .order_by(Artifact.created_at, Artifact.id)
                    .limit(limit)
                    .offset(offset),
                )
            ]

    @app.get("/artifacts/{artifact_id}")
    def artifact(artifact_id: str) -> BlobResponse:
        with svc.factory() as session:
            row = session.get(Artifact, artifact_id)
            if row is None:
                raise DomainError(404, "artifact not found")
            from control_plane.storage import BlobStore

            return BlobResponse(BlobStore(config), row.sha256, row.size, row.name, row.content_type)

    @app.get("/workers")
    def workers() -> list[dict[str, Any]]:
        from control_plane.models import GPUDevice

        with svc.factory() as session:
            devices = list(session.scalars(select(GPUDevice).where(GPUDevice.enabled.is_(True))))
            return [
                row_view(w, {"session_id"})
                | {
                    "gpu_total": sum(device.worker_id == w.id for device in devices),
                    "gpu_reserved": sum(
                        device.worker_id == w.id and device.allocated_to is not None
                        for device in devices
                    ),
                }
                for w in session.scalars(select(Worker).order_by(Worker.id))
            ]

    @app.get("/workers/{worker_id}")
    def worker(worker_id: str) -> dict[str, Any]:
        with svc.factory() as session:
            return row_view(svc.worker(session, worker_id), {"session_id"})

    @app.get("/workers/{worker_id}/gpus")
    def worker_gpus(worker_id: str) -> list[dict[str, Any]]:
        from control_plane.models import GPUDevice

        with svc.factory() as session:
            svc.worker(session, worker_id)
            return [
                row_view(device)
                for device in session.scalars(
                    select(GPUDevice).where(GPUDevice.worker_id == worker_id).order_by(GPUDevice.id)
                )
            ]

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/ready")
    def ready() -> JSONResponse:
        if (replica_ready := getattr(app.state, "replica_ready", None)) and not replica_ready():
            return JSONResponse({"status": "unavailable"}, status_code=503)
        try:
            with svc.factory() as session:
                if session.bind is not None and session.bind.dialect.name == "postgresql":
                    writable = session.scalar(
                        text(
                            "SELECT NOT pg_is_in_recovery() "
                            "AND current_setting('transaction_read_only') = 'off'"
                        )
                    )
                    if not writable:
                        return JSONResponse({"status": "unavailable"}, status_code=503)
                session.execute(text("SELECT id FROM admission WHERE id=1")).scalar_one()
                if not config.identity_enabled and session.scalar(select(Project.id).limit(1)):
                    return JSONResponse({"status": "individual identity required"}, status_code=503)
                if config.identity_enabled and not session.scalar(
                    select(User.id).where(User.is_admin.is_(True), User.enabled.is_(True)).limit(1)
                ):
                    return JSONResponse(
                        {"status": "administrator bootstrap required"}, status_code=503
                    )
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
        from control_plane.periodic import PeriodicService
        from control_plane.workflows import WorkflowService
        from scheduler.core import Scheduler

        recovered, assigned = Scheduler(svc).tick()
        return {
            "recovered": recovered,
            "assigned": assigned,
            "periodic": PeriodicService(svc).tick(),
            "expanded": WorkflowService(svc).tick(),
        }

    from pathlib import Path

    from fastapi.staticfiles import StaticFiles

    from control_plane.experiment_api import experiment_router
    from control_plane.identity_api import identity_router
    from control_plane.oidc_api import oidc_router
    from control_plane.resource_api import resource_router
    from control_plane.schedule_api import schedule_router
    from control_plane.upload_api import upload_router
    from control_plane.webhook_api import webhook_router

    app.include_router(resource_router(svc))
    app.include_router(identity_router(svc))
    app.include_router(oidc_router(svc))
    app.include_router(upload_router(svc))
    app.include_router(experiment_router(svc))
    app.include_router(schedule_router(svc))
    app.include_router(webhook_router(svc))
    from control_plane.runtime_api import runtime_router

    app.include_router(runtime_router(svc))
    static = Path(__file__).parent / "web"
    html = (static / "index.html").read_text(encoding="utf-8")
    for asset in ("style.css", "app.js"):
        revision = hashlib.sha256((static / asset).read_bytes()).hexdigest()[:16]
        html = html.replace(f"/app/{asset}", f"/app/{asset}?v={revision}")
    app.mount("/app", StaticFiles(directory=static, html=True), name="web")

    @app.get("/", include_in_schema=False)
    def home() -> HTMLResponse:
        return HTMLResponse(
            html,
            headers={
                "Content-Security-Policy": "default-src 'self'; script-src 'self'; "
                "style-src 'self'; object-src 'none'; base-uri 'self'; frame-ancestors 'none'"
            },
        )

    return app


from control_plane.models import Job  # noqa: E402

app = create_app()
