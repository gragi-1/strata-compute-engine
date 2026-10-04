from typing import Annotated, Any

from fastapi import APIRouter, Header, Query, Response
from sqlalchemy import select

from control_plane.experiments import ExperimentService, MetricsUpdate, Replay, RunSubmit
from control_plane.models import Experiment, ExperimentRun
from control_plane.schemas import NamedResource
from control_plane.services import DomainError, EngineService


def experiment_router(svc: EngineService) -> APIRouter:
    from control_plane.api import row_view

    router = APIRouter()
    service = ExperimentService(svc)

    @router.post("/experiments", status_code=201)
    def create(body: NamedResource) -> dict[str, Any]:
        return row_view(service.create(body))

    @router.get("/experiments")
    def experiments(limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0)) -> Any:
        with svc.factory() as session:
            return [
                row_view(row)
                for row in session.scalars(
                    select(Experiment)
                    .order_by(Experiment.created_at.desc(), Experiment.id)
                    .limit(limit)
                    .offset(offset)
                )
            ]

    @router.get("/experiments/{experiment_id}/runs")
    def runs(
        experiment_id: str,
        metric: str | None = Query(None, pattern=r"^[a-zA-Z][a-zA-Z0-9_.-]{0,63}$"),
        minimum: float | None = Query(None, allow_inf_nan=False),
        maximum: float | None = Query(None, allow_inf_nan=False),
        limit: int = Query(50, ge=1, le=200),
        offset: int = Query(0, ge=0),
    ) -> Any:
        with svc.factory() as session:
            if session.get(Experiment, experiment_id) is None:
                raise DomainError(404, "experiment not found")
            query = select(ExperimentRun.id).where(ExperimentRun.experiment_id == experiment_id)
            if metric:
                value = ExperimentRun.metrics[metric].as_float()
                if minimum is not None:
                    query = query.where(value >= minimum)
                if maximum is not None:
                    query = query.where(value <= maximum)
            elif minimum is not None or maximum is not None:
                raise DomainError(422, "select a metric when specifying numeric bounds")
            ids = list(
                session.scalars(
                    query.order_by(ExperimentRun.created_at.desc(), ExperimentRun.id)
                    .limit(limit)
                    .offset(offset)
                )
            )
        return [service.get(run_id) for run_id in ids]

    @router.post("/experiments/{experiment_id}/runs", status_code=201)
    def run(
        experiment_id: str,
        body: RunSubmit,
        response: Response,
        idempotency_key: Annotated[str | None, Header()] = None,
    ) -> Any:
        row, created = service.run(experiment_id, body, idempotency_key)
        response.status_code = 201 if created else 200
        return service.get(row.id)

    @router.get("/experiment-runs/compare")
    def compare(ids: str = Query(max_length=1500)) -> Any:
        selected = ids.split(",")
        if not 1 <= len(selected) <= 20 or len(set(selected)) != len(selected):
            raise DomainError(422, "select 1..20 distinct run IDs")
        return [service.get(run_id) for run_id in selected]

    @router.get("/experiment-runs/{run_id}")
    def get(run_id: str) -> Any:
        return service.get(run_id)

    @router.put("/experiment-runs/{run_id}/metrics")
    def metrics(run_id: str, body: MetricsUpdate) -> Any:
        return service.metrics(run_id, body)

    @router.post("/experiment-runs/{run_id}/replay", status_code=201)
    def replay(
        run_id: str,
        body: Replay,
        response: Response,
        idempotency_key: Annotated[str | None, Header()] = None,
    ) -> Any:
        row, created = service.replay(run_id, body, idempotency_key)
        response.status_code = 201 if created else 200
        return service.get(row.id)

    return router
