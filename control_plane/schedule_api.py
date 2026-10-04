from typing import Any

from fastapi import APIRouter, Query
from sqlalchemy import select

from control_plane.models import JobSchedule, ScheduleFire
from control_plane.periodic import PeriodicService, ScheduleSubmit
from control_plane.services import DomainError, EngineService


def schedule_router(svc: EngineService) -> APIRouter:
    from control_plane.api import row_view

    router = APIRouter()
    service = PeriodicService(svc)

    @router.post("/schedules", status_code=201)
    def create(body: ScheduleSubmit) -> Any:
        return row_view(service.create(body))

    @router.get("/schedules")
    def listing(limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0)) -> Any:
        with svc.factory() as session:
            return [
                row_view(row)
                for row in session.scalars(
                    select(JobSchedule)
                    .order_by(JobSchedule.created_at.desc())
                    .limit(limit)
                    .offset(offset)
                )
            ]

    @router.get("/schedules/{schedule_id}")
    def get(schedule_id: str) -> Any:
        with svc.factory() as session:
            return row_view(service.row(session, schedule_id))

    @router.get("/schedules/{schedule_id}/fires")
    def fires(
        schedule_id: str, limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0)
    ) -> Any:
        with svc.factory() as session:
            service.row(session, schedule_id)
            return [
                row_view(row)
                for row in session.scalars(
                    select(ScheduleFire)
                    .where(ScheduleFire.schedule_id == schedule_id)
                    .order_by(ScheduleFire.scheduled_at.desc())
                    .limit(limit)
                    .offset(offset)
                )
            ]

    @router.post("/schedules/{schedule_id}/{action}")
    def action(schedule_id: str, action: str) -> Any:
        if action not in {"pause", "resume"}:
            raise DomainError(404, "unknown schedule action")
        return row_view(service.action(schedule_id, action == "resume"))

    return router
