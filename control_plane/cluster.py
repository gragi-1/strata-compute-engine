"""Durable cluster admission controls with transactional scheduler fencing."""

from typing import Any

from sqlalchemy import select

from control_plane.access import actor_id, audit
from control_plane.models import Admission
from control_plane.schemas import AdmissionUpdate
from control_plane.services import EngineService


def view(row: Admission) -> dict[str, Any]:
    return {
        "accepting_jobs": row.accepting_jobs,
        "scheduling_enabled": row.scheduling_enabled,
        "reason": row.reason,
        "changed_at": row.changed_at,
        "changed_by": row.changed_by,
    }


class ClusterService:
    def __init__(self, service: EngineService) -> None:
        self.svc = service

    def admission(self) -> dict[str, Any]:
        with self.svc.factory() as session:
            row = session.execute(select(Admission).where(Admission.id == 1)).scalar_one()
            return view(row)

    def update(self, body: AdmissionUpdate) -> dict[str, Any]:
        with self.svc.factory.begin() as session:
            row = session.execute(
                select(Admission).where(Admission.id == 1).with_for_update()
            ).scalar_one()
            changes = body.model_dump()
            if any(getattr(row, key) != value for key, value in changes.items()):
                for key, value in changes.items():
                    setattr(row, key, value)
                row.changed_at = self.svc.now(session)
                row.changed_by = actor_id()
                audit(session, row.changed_at, "CLUSTER_ADMISSION_UPDATED", "cluster", **changes)
            return view(row)
