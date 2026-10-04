"""Account and project administration; all returned credentials are explicitly bounded."""

from typing import Any, Literal

from fastapi import APIRouter, Query, Request
from pydantic import Field, SecretStr
from sqlalchemy import select

from control_plane.access import require_actor, require_platform_admin
from control_plane.identity import IdentityService, project_view, user_view
from control_plane.models import AccessToken, ProjectMembership, User
from control_plane.schemas import NamedResource, StrictModel
from control_plane.services import EngineService

Role = Literal["viewer", "operator", "admin"]


class Login(StrictModel):
    username: str = Field(min_length=1, max_length=128)
    password: SecretStr = Field(min_length=1, max_length=1024)


class UserCreate(Login):
    display_name: str = Field(default="", max_length=128)
    is_admin: bool = False


class PasswordChange(StrictModel):
    current_password: SecretStr = Field(min_length=1, max_length=1024)
    new_password: SecretStr = Field(min_length=12, max_length=1024)


class UserUpdate(StrictModel):
    display_name: str | None = Field(default=None, min_length=1, max_length=128)
    password: SecretStr | None = Field(default=None, min_length=12, max_length=1024)
    is_admin: bool | None = None
    enabled: bool | None = None


class ProjectUpdate(StrictModel):
    name: str | None = Field(default=None, min_length=1, max_length=128)
    description: str | None = Field(default=None, max_length=4096)
    enabled: bool | None = None
    queue_limit: int | None = Field(default=None, ge=1, le=1000000)
    cpu_limit: float | None = Field(default=None, gt=0, le=1000000, allow_inf_nan=False)
    memory_limit_mb: int | None = Field(default=None, ge=1, le=1073741824)
    gpu_limit: int | None = Field(default=None, ge=0, le=1024)
    storage_limit_bytes: int | None = Field(default=None, ge=1, le=2**60)


class MembershipUpdate(StrictModel):
    role: Role


class TokenCreate(StrictModel):
    name: str = Field(min_length=1, max_length=128)
    project_id: str = Field(min_length=1, max_length=36)
    role: Role = "operator"
    lifetime_seconds: int = Field(default=86400, ge=60, le=90 * 86400)


def identity_router(svc: EngineService) -> APIRouter:
    router = APIRouter()
    identity = IdentityService(svc)

    @router.get("/auth/config")
    def config() -> dict[str, Any]:
        return {
            "identity_enabled": svc.settings.identity_enabled,
            "oidc_enabled": bool(svc.settings.oidc_issuer),
        }

    @router.post("/auth/login")
    def login(body: Login, request: Request) -> dict[str, Any]:
        return identity.login(
            body.username,
            body.password.get_secret_value(),
            request.client.host if request.client else "unknown",
        )

    @router.post("/auth/logout", status_code=204)
    def logout() -> None:
        identity.revoke_token(require_actor().token_id)

    @router.get("/auth/me")
    def me() -> dict[str, Any]:
        actor = require_actor()
        with svc.factory() as session:
            user = session.get(User, actor.user_id)
            assert user is not None
            return {"user": user_view(user), "projects": identity.projects()}

    @router.post("/auth/password", status_code=204)
    def change_password(body: PasswordChange) -> None:
        identity.change_password(
            body.current_password.get_secret_value(), body.new_password.get_secret_value()
        )

    @router.get("/auth/users")
    def users(
        limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)
    ) -> list[dict[str, Any]]:
        require_platform_admin()
        with svc.factory() as session:
            return [
                user_view(row)
                for row in session.scalars(
                    select(User).order_by(User.username).limit(limit).offset(offset),
                )
            ]

    @router.post("/auth/users", status_code=201)
    def create_user(body: UserCreate) -> dict[str, Any]:
        return user_view(
            identity.create_user(
                body.username, body.password.get_secret_value(), body.display_name, body.is_admin
            )
        )

    @router.patch("/auth/users/{user_id}")
    def update_user(user_id: str, body: UserUpdate) -> dict[str, Any]:
        changes = body.model_dump(exclude_none=True)
        if body.password is not None:
            changes["password"] = body.password.get_secret_value()
        return user_view(identity.update_user(user_id, changes))

    @router.get("/auth/tokens")
    def tokens() -> list[dict[str, Any]]:
        actor = require_actor()
        with svc.factory() as session:
            query = select(AccessToken).where(AccessToken.user_id == actor.user_id)
            if actor.token_project_id:
                query = query.where(AccessToken.project_id == actor.token_project_id)
            return [
                {
                    key: getattr(row, key)
                    for key in (
                        "id",
                        "name",
                        "kind",
                        "project_id",
                        "role",
                        "created_at",
                        "expires_at",
                        "revoked_at",
                    )
                }
                for row in session.scalars(
                    query.order_by(AccessToken.created_at.desc()).limit(1000)
                )
            ]

    @router.post("/auth/tokens", status_code=201)
    def issue_token(body: TokenCreate) -> dict[str, Any]:
        return identity.issue_token(body.name, body.project_id, body.role, body.lifetime_seconds)

    @router.delete("/auth/tokens/{token_id}", status_code=204)
    def revoke_token(token_id: str) -> None:
        identity.revoke_token(token_id)

    @router.get("/projects")
    def projects() -> list[dict[str, Any]]:
        return identity.projects()

    @router.post("/projects", status_code=201)
    def create_project(body: NamedResource) -> dict[str, Any]:
        return project_view(identity.create_project(body.name, body.description))

    @router.patch("/projects/{project_id}")
    def update_project(project_id: str, body: ProjectUpdate) -> dict[str, Any]:
        return project_view(identity.update_project(project_id, body.model_dump(exclude_none=True)))

    @router.get("/projects/{project_id}/members")
    def members(project_id: str) -> list[dict[str, Any]]:
        actor = identity.project_access(require_actor(), project_id, "GET")
        if actor.role != "admin":
            from control_plane.errors import DomainError

            raise DomainError(403, "project administrator access is required")
        with svc.factory() as session:
            return [
                {"user": user_view(user), "role": member.role}
                for member, user in session.execute(
                    select(ProjectMembership, User)
                    .join(User)
                    .where(
                        ProjectMembership.project_id == project_id,
                    )
                    .order_by(User.username)
                )
            ]

    @router.put("/projects/{project_id}/members/{user_id}")
    def membership(project_id: str, user_id: str, body: MembershipUpdate) -> dict[str, str]:
        identity.membership(project_id, user_id, body.role)
        return {"role": body.role}

    @router.delete("/projects/{project_id}/members/{user_id}", status_code=204)
    def remove_membership(project_id: str, user_id: str) -> None:
        identity.membership(project_id, user_id, None)

    @router.get("/projects/{project_id}/audit")
    def audit(
        project_id: str, after: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=1000)
    ) -> list[dict[str, Any]]:
        return identity.audit_events(project_id, after, limit)

    return router
