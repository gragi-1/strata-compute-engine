from typing import Any

from fastapi import APIRouter, Query, Request
from sqlalchemy import select

from control_plane.models import Webhook, WebhookDelivery
from control_plane.services import DomainError, EngineService
from control_plane.webhooks import WebhookService, WebhookSubmit


def webhook_router(svc: EngineService) -> APIRouter:
    from control_plane.api import row_view

    router = APIRouter()
    service = WebhookService(svc)

    def admin(request: Request) -> None:
        if request.state.role != "admin":
            raise DomainError(403, "project administrator access is required")

    @router.get("/webhook-targets")
    def targets(request: Request) -> Any:
        admin(request)
        return sorted(svc.settings.webhook_targets)

    @router.post("/webhooks", status_code=201)
    def create(body: WebhookSubmit, request: Request) -> Any:
        admin(request)
        return row_view(service.create(body))

    @router.get("/webhooks")
    def listing(limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0)) -> Any:
        with svc.factory() as session:
            return [
                row_view(row)
                for row in session.scalars(
                    select(Webhook).order_by(Webhook.created_at.desc()).limit(limit).offset(offset)
                )
            ]

    @router.get("/webhooks/{webhook_id}/deliveries")
    def deliveries(
        webhook_id: str, limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0)
    ) -> Any:
        with svc.factory() as session:
            service.row(session, webhook_id)
            return [
                row_view(row, {"lease_token", "lease_until"})
                for row in session.scalars(
                    select(WebhookDelivery)
                    .where(WebhookDelivery.webhook_id == webhook_id)
                    .order_by(WebhookDelivery.created_at.desc(), WebhookDelivery.id)
                    .limit(limit)
                    .offset(offset)
                )
            ]

    @router.post("/webhooks/{webhook_id}/deliveries/{delivery_id}/retry")
    def retry(webhook_id: str, delivery_id: str, request: Request) -> Any:
        admin(request)
        return row_view(service.retry(webhook_id, delivery_id), {"lease_token", "lease_until"})

    @router.post("/webhooks/{webhook_id}/{action}")
    def action(webhook_id: str, action: str, request: Request) -> Any:
        admin(request)
        if action not in {"enable", "disable"}:
            raise DomainError(404, "unknown webhook action")
        return row_view(service.action(webhook_id, action == "enable"))

    return router
