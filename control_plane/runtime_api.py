"""Project-authorized computation groups and interactive message proxy."""

from typing import Annotated, Any

from fastapi import APIRouter, Header, Query, Response
from sqlalchemy import select

from control_plane.models import ComputeGroup, InteractiveSession, SessionCell
from control_plane.runtimes import (
    CellSubmit,
    GroupSubmit,
    ProxySubmit,
    RuntimeService,
    SessionSubmit,
)
from control_plane.services import DomainError, EngineService


def runtime_router(svc: EngineService) -> APIRouter:
    from control_plane.api import row_view

    router, runtime = APIRouter(), RuntimeService(svc)

    @router.post("/compute-groups", status_code=201)
    def create_group(
        body: GroupSubmit,
        response: Response,
        idempotency_key: Annotated[str | None, Header()] = None,
    ) -> Any:
        row, created = runtime.create_group(body, idempotency_key)
        response.status_code = 201 if created else 200
        return runtime.group(row.id)

    @router.get("/compute-groups")
    def groups(limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0)) -> Any:
        with svc.factory() as s:
            return [
                row_view(r)
                for r in s.scalars(
                    select(ComputeGroup)
                    .order_by(ComputeGroup.created_at.desc(), ComputeGroup.id)
                    .limit(limit)
                    .offset(offset)
                )
            ]

    @router.get("/compute-groups/{group_id}")
    def group(group_id: str) -> Any:
        return runtime.group(group_id)

    @router.post("/compute-groups/{group_id}/cancel")
    def cancel(group_id: str) -> Any:
        return runtime.cancel_group(group_id)

    @router.post("/compute-groups/{group_id}/retry", status_code=201)
    def retry(group_id: str, response: Response, idempotency_key: Annotated[str, Header()]) -> Any:
        row, created = runtime.retry_group(group_id, idempotency_key)
        response.status_code = 201 if created else 200
        return runtime.group(row.id)

    @router.post("/interactive-sessions", status_code=201)
    def create_session(
        body: SessionSubmit,
        response: Response,
        idempotency_key: Annotated[str | None, Header()] = None,
    ) -> Any:
        row, created = runtime.create_session(body, idempotency_key)
        response.status_code = 201 if created else 200
        return runtime.session(row.id)

    @router.get("/interactive-sessions")
    def sessions(limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0)) -> Any:
        with svc.factory() as s:
            return [
                runtime.session(r.id)
                for r in s.scalars(
                    select(InteractiveSession)
                    .order_by(InteractiveSession.created_at.desc(), InteractiveSession.id)
                    .limit(limit)
                    .offset(offset)
                )
            ]

    @router.get("/interactive-sessions/{session_id}")
    def session(session_id: str) -> Any:
        return runtime.session(session_id)

    @router.post("/interactive-sessions/{session_id}/stop")
    def stop(session_id: str) -> Any:
        return runtime.stop_session(session_id)

    @router.post("/interactive-sessions/{session_id}/cells", status_code=201)
    def execute(
        session_id: str, body: CellSubmit, idempotency_key: Annotated[str, Header()]
    ) -> Any:
        return row_view(runtime.submit_cell(session_id, body, idempotency_key))

    @router.post("/interactive-sessions/{session_id}/proxy", status_code=201)
    def proxy(session_id: str, body: ProxySubmit, idempotency_key: Annotated[str, Header()]) -> Any:
        return row_view(runtime.submit_cell(session_id, body, idempotency_key))

    @router.get("/interactive-sessions/{session_id}/cells")
    def cells(
        session_id: str, limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0)
    ) -> Any:
        runtime.session(session_id)
        with svc.factory() as s:
            return [
                row_view(r)
                for r in s.scalars(
                    select(SessionCell)
                    .where(SessionCell.session_id == session_id)
                    .order_by(SessionCell.sequence)
                    .limit(limit)
                    .offset(offset)
                )
            ]

    @router.get("/interactive-sessions/{session_id}/cells/{cell_id}")
    def cell(session_id: str, cell_id: str) -> Any:
        runtime.session(session_id)
        with svc.factory() as s:
            row = s.scalar(
                select(SessionCell).where(
                    SessionCell.session_id == session_id, SessionCell.id == cell_id
                )
            )
            if row is None:
                raise DomainError(404, "session cell not found")
            return row_view(row)

    @router.get("/interactive-sessions/{session_id}/notebook")
    def notebook(session_id: str) -> Any:
        row = runtime.session(session_id)
        if row["kind"] != "python":
            raise DomainError(422, "only Python sessions can be exported as notebooks")
        with svc.factory() as s:
            rows = list(
                s.scalars(
                    select(SessionCell)
                    .where(SessionCell.session_id == session_id)
                    .order_by(SessionCell.sequence)
                )
            )
        return {
            "nbformat": 4,
            "nbformat_minor": 5,
            "metadata": {
                "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
                "strata": {"session_id": session_id, "job_id": row["job_id"]},
            },
            "cells": [
                {
                    "cell_type": "code",
                    "id": c.id,
                    "execution_count": c.sequence + 1 if c.started_at else None,
                    "metadata": {"strata_status": c.status},
                    "source": c.payload["code"],
                    "outputs": [
                        {"output_type": "stream", "name": name, "text": c.result[name]}
                        for name in ("stdout", "stderr")
                        if c.result.get(name)
                    ],
                }
                for c in rows
            ],
        }

    return router
