"""Transactional terminal-event outbox and a separately supervised dispatcher."""

import hashlib
import hmac
import json
import logging
import signal
import threading
from datetime import datetime, timedelta
from typing import Literal

import httpx
from pydantic import Field, model_validator
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from control_plane.access import actor_id, audit, project_id
from control_plane.config import Settings
from control_plane.database import make_engine, sessions
from control_plane.logging import configure_logging
from control_plane.models import Admission, Attempt, Job, Webhook, WebhookDelivery, identifier
from control_plane.periodic import PeriodicService
from control_plane.schemas import StrictModel
from control_plane.services import DomainError, EngineService


class WebhookSubmit(StrictModel):
    name: str = Field(min_length=1, max_length=128)
    target: str = Field(pattern=r"^[a-zA-Z][a-zA-Z0-9_-]{0,63}$")
    events: list[Literal["JOB_SUCCEEDED", "JOB_FAILED", "JOB_CANCELLED", "JOB_TIMED_OUT"]] = Field(
        min_length=1, max_length=4
    )

    @model_validator(mode="after")
    def unique_events(self) -> "WebhookSubmit":
        if len(set(self.events)) != len(self.events):
            raise ValueError("webhook event types must be unique")
        return self


def enqueue(
    svc: EngineService,
    session: Session,
    job: Job,
    kind: str,
    event_uid: str,
    now: datetime,
    attempt: Attempt | None,
) -> None:
    subscriptions = list(
        session.scalars(
            select(Webhook).where(Webhook.enabled.is_(True), Webhook.project_id == job.project_id)
        )
    )
    for webhook in subscriptions:
        if kind not in webhook.events:
            continue
        session.add(
            WebhookDelivery(
                id=identifier(),
                webhook_id=webhook.id,
                event_uid=event_uid,
                payload={
                    "id": event_uid,
                    "type": kind,
                    "occurred_at": now.isoformat(),
                    "project_id": job.project_id,
                    "job": {"id": job.id, "status": job.status, "campaign_id": job.campaign_id},
                    "attempt_id": attempt.id if attempt else None,
                },
                status="PENDING",
                attempts=0,
                attempt_limit=svc.settings.webhook_attempt_limit,
                next_attempt_at=now,
                created_at=now,
            )
        )


class WebhookService:
    def __init__(self, svc: EngineService) -> None:
        self.svc = svc

    def row(self, session: Session, webhook_id: str, *, lock: bool = False) -> Webhook:
        query = select(Webhook).where(Webhook.id == webhook_id)
        row = session.scalar(query.with_for_update() if lock else query)
        if row is None:
            raise DomainError(404, "webhook not found")
        return row

    def capacity(self, session: Session) -> None:
        count = (
            session.scalar(
                select(func.count())
                .select_from(Webhook)
                .where(Webhook.enabled.is_(True))
                .execution_options(strata_unscoped=True)
            )
            or 0
        )
        if count >= self.svc.settings.webhook_max_active:
            raise DomainError(429, "active webhook capacity reached")

    def create(self, body: WebhookSubmit) -> Webhook:
        if body.target not in self.svc.settings.webhook_targets:
            raise DomainError(422, "webhook target is not configured by the platform administrator")
        with self.svc.factory.begin() as session:
            session.scalar(select(Admission).where(Admission.id == 1).with_for_update())
            self.capacity(session)
            now = self.svc.now(session)
            row = Webhook(
                id=identifier(),
                project_id=project_id(),
                created_by=actor_id(),
                name=body.name,
                target=body.target,
                events=body.events,
                enabled=True,
                created_at=now,
            )
            session.add(row)
            audit(session, now, "WEBHOOK_CREATED", row.id, row.project_id, target=row.target)
            return row

    def action(self, webhook_id: str, enabled: bool) -> Webhook:
        with self.svc.factory.begin() as session:
            session.scalar(select(Admission).where(Admission.id == 1).with_for_update())
            row = self.row(session, webhook_id, lock=True)
            if row.enabled == enabled:
                return row
            if enabled:
                self.capacity(session)
                if row.target not in self.svc.settings.webhook_targets:
                    raise DomainError(422, "webhook target is unavailable")
                row.created_by = actor_id()
            row.enabled = enabled
            audit(
                session,
                self.svc.now(session),
                "WEBHOOK_ENABLED" if enabled else "WEBHOOK_DISABLED",
                row.id,
                row.project_id,
            )
            return row

    def retry(self, webhook_id: str, delivery_id: str) -> WebhookDelivery:
        with self.svc.factory.begin() as session:
            webhook = self.row(session, webhook_id)
            delivery = session.scalar(
                select(WebhookDelivery)
                .where(WebhookDelivery.id == delivery_id, WebhookDelivery.webhook_id == webhook.id)
                .with_for_update()
            )
            if delivery is None:
                raise DomainError(404, "delivery not found")
            if delivery.status != "DEAD" or not webhook.enabled:
                raise DomainError(409, "retry requires a dead delivery and an enabled webhook")
            delivery.status = "PENDING"
            delivery.attempt_limit = delivery.attempts + self.svc.settings.webhook_attempt_limit
            delivery.next_attempt_at = self.svc.now(session)
            delivery.completed_at = None
            delivery.last_error = None
            audit(
                session,
                self.svc.now(session),
                "WEBHOOK_DELIVERY_RETRIED",
                delivery.id,
                webhook.project_id,
            )
            return delivery

    def claim(self) -> WebhookDelivery | None:
        with self.svc.factory.begin() as session:
            now = self.svc.now(session)
            row = session.scalar(
                select(WebhookDelivery)
                .where(
                    WebhookDelivery.next_attempt_at <= now,
                    or_(
                        WebhookDelivery.status == "PENDING",
                        (WebhookDelivery.status == "SENDING")
                        & (WebhookDelivery.lease_until <= now),
                    ),
                )
                .order_by(WebhookDelivery.next_attempt_at, WebhookDelivery.id)
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            if row is None:
                return None
            if row.attempts >= row.attempt_limit:
                row.status = "DEAD"
                row.completed_at = now
                row.last_error = "dispatcher lease expired after the attempt limit"
                row.lease_token = None
                row.lease_until = None
                return None
            row.status = "SENDING"
            row.lease_token = identifier()
            row.lease_until = now + timedelta(seconds=30)
            row.attempts += 1
            return row

    def finish(
        self,
        claimed: WebhookDelivery,
        status: int | None,
        error: str | None,
        *,
        permanent: bool = False,
    ) -> None:
        with self.svc.factory.begin() as session:
            row = session.scalar(
                select(WebhookDelivery).where(WebhookDelivery.id == claimed.id).with_for_update()
            )
            now = self.svc.now(session)
            if (
                row is None
                or row.status != "SENDING"
                or row.lease_token != claimed.lease_token
                or row.lease_until is None
                or row.lease_until <= now
            ):
                return  # A recovered dispatcher owns this delivery now.
            row.last_status, row.last_error = status, error
            row.lease_token = None
            row.lease_until = None
            if status is not None and 200 <= status < 300:
                row.status, row.completed_at = "DELIVERED", now
            elif permanent or row.attempts >= row.attempt_limit:
                row.status, row.completed_at = "DEAD", now
            else:
                row.status = "PENDING"
                row.next_attempt_at = now + timedelta(seconds=min(3600, 2 ** min(row.attempts, 12)))

    def deliver(self, claimed: WebhookDelivery) -> None:
        try:
            with self.svc.factory() as session:
                webhook = session.get(Webhook, claimed.webhook_id)
                if webhook is None or not webhook.enabled:
                    raise DomainError(403, "webhook is disabled")
                actor = PeriodicService(self.svc).actor(session, webhook)
                if actor is not None and actor.role != "admin":
                    raise DomainError(403, "webhook owner no longer administers the project")
                url = self.svc.settings.webhook_targets.get(webhook.target)
                secret = self.svc.settings.webhook_secrets.get(webhook.target)
                if not url or not secret:
                    raise DomainError(403, "webhook target is unavailable")
                timestamp = str(int(self.svc.now(session).timestamp()))
            content = json.dumps(
                claimed.payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
            ).encode()
            signature = hmac.new(
                secret.encode(), timestamp.encode() + b"." + content, hashlib.sha256
            ).hexdigest()
            # Read status/headers only. A receiver cannot spool a large response into memory.
            with (
                httpx.Client(timeout=3, follow_redirects=False, trust_env=False) as client,
                client.stream(
                    "POST",
                    url,
                    content=content,
                    headers={
                        "Content-Type": "application/json",
                        "X-Strata-Delivery": claimed.id,
                        "X-Strata-Event": claimed.event_uid,
                        "X-Strata-Timestamp": timestamp,
                        "X-Strata-Signature": "sha256=" + signature,
                    },
                ) as response,
            ):
                status = response.status_code
            self.finish(
                claimed,
                status,
                None if 200 <= status < 300 else f"receiver returned HTTP {status}",
                permanent=300 <= status < 500 and status not in {408, 429},
            )
        except DomainError as exc:
            self.finish(claimed, None, str(exc), permanent=True)
        except httpx.HTTPError as exc:
            self.finish(claimed, None, type(exc).__name__)

    def tick(self, *, limit: int = 20) -> int:
        delivered = 0
        for _ in range(limit):
            claimed = self.claim()
            if claimed is None:
                break
            self.deliver(claimed)
            delivered += 1
        return delivered


def main() -> None:
    configure_logging()
    settings = Settings()
    engine = make_engine(settings.database_url)
    service = WebhookService(EngineService(sessions(engine), settings))
    stopped = threading.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stopped.set())
    try:
        while not stopped.is_set():
            try:
                service.tick()
            except Exception:
                logging.getLogger(__name__).exception("webhook_dispatch_failed")
            stopped.wait(1)
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
