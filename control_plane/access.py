"""Request-local project authorization shared by HTTP, services and ORM queries."""

import hashlib
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, replace
from typing import Any

from sqlalchemy import event, false
from sqlalchemy.orm import ORMExecuteState, Session, with_loader_criteria

from control_plane.errors import DomainError
from control_plane.models import (
    Artifact,
    AuditEvent,
    Campaign,
    ComputeGroup,
    Dataset,
    DatasetFile,
    DatasetVersion,
    Experiment,
    ExperimentRun,
    InteractiveSession,
    Job,
    JobSchedule,
    SessionCell,
    UploadSession,
    Webhook,
    WorkflowExpansion,
)


@dataclass(frozen=True)
class Principal:
    user_id: str
    username: str
    platform_admin: bool
    token_id: str
    token_project_id: str | None = None
    token_role: str | None = None
    project_id: str | None = None
    role: str | None = None


_principal: ContextVar[Principal | None] = ContextVar("strata_principal", default=None)
PROJECT_RESOURCES = (
    Job,
    Campaign,
    Dataset,
    DatasetVersion,
    DatasetFile,
    Artifact,
    UploadSession,
    Experiment,
    ExperimentRun,
    JobSchedule,
    WorkflowExpansion,
    Webhook,
    ComputeGroup,
    InteractiveSession,
    SessionCell,
)


def principal() -> Principal | None:
    return _principal.get()


def project_id() -> str | None:
    actor = principal()
    return actor.project_id if actor else None


def actor_id() -> str | None:
    actor = principal()
    return actor.user_id if actor else None


@contextmanager
def access_scope(actor: Principal | None, project: str | None = None) -> Iterator[None]:
    value = replace(actor, project_id=project) if actor and project else actor
    token = _principal.set(value)
    try:
        yield
    finally:
        _principal.reset(token)


def require_actor() -> Principal:
    actor = principal()
    if actor is None:
        raise DomainError(401, "individual authentication is required")
    return actor


def require_platform_admin() -> Principal:
    actor = require_actor()
    if not actor.platform_admin or actor.token_project_id:
        raise DomainError(403, "platform administrator access is required")
    return actor


def require_project_admin(target: str) -> Principal:
    actor = require_actor()
    if actor.project_id != target or actor.role != "admin":
        raise DomainError(403, "project administrator access is required")
    return actor


def scoped_key(key: str | None, project: str | None = None) -> str | None:
    scope = project or project_id()
    return f"project:{scope}:{hashlib.sha256(key.encode()).hexdigest()}" if key and scope else key


def audit(
    session: Session,
    now: Any,
    action: str,
    resource: str | None = None,
    project: str | None = None,
    **details: Any,
) -> None:
    session.add(
        AuditEvent(
            actor_id=actor_id(),
            project_id=project or project_id(),
            action=action,
            resource_id=resource,
            created_at=now,
            details=details,
        )
    )


@event.listens_for(Session, "do_orm_execute")
def restrict_project_reads(state: ORMExecuteState) -> None:
    actor = principal()
    if actor is None or state.execution_options.get("strata_unscoped"):
        return  # Trusted worker/maintenance calls use no HTTP principal.
    if not state.is_select:
        return
    scope = actor.project_id
    for model in PROJECT_RESOURCES:
        criterion = model.project_id == scope if scope else false()
        state.statement = state.statement.options(
            with_loader_criteria(model, criterion, include_aliases=True)
        )
