"""Individual identity, revocable credentials and explicit project membership."""

import hashlib
import secrets
from dataclasses import replace
from datetime import datetime, timedelta
from typing import Any

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError
from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from control_plane.access import (
    Principal,
    access_scope,
    audit,
    require_actor,
    require_platform_admin,
)
from control_plane.errors import DomainError
from control_plane.models import (
    AccessToken,
    Admission,
    AuditEvent,
    LoginThrottle,
    Project,
    ProjectMembership,
    ProjectStorageLock,
    User,
    identifier,
)
from control_plane.services import EngineService

PASSWORDS = PasswordHasher(time_cost=3, memory_cost=65536, parallelism=1)
DUMMY_HASH = PASSWORDS.hash(secrets.token_urlsafe(32))
ROLES = {"viewer": 0, "operator": 1, "admin": 2}


def user_view(user: User) -> dict[str, Any]:
    return {
        key: getattr(user, key)
        for key in (
            "id",
            "username",
            "display_name",
            "is_admin",
            "enabled",
            "created_at",
        )
    }


def project_view(project: Project) -> dict[str, Any]:
    return {column.name: getattr(project, column.name) for column in project.__table__.columns}


def hash_token(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def password_hash(password: str) -> str:
    if not 12 <= len(password) <= 1024:
        raise DomainError(422, "passwords must contain 12..1024 characters")
    return PASSWORDS.hash(password)


def verify_password(encoded: str, password: str) -> bool:
    try:
        return PASSWORDS.verify(encoded, password)
    except (VerificationError, InvalidHashError):
        return False


class IdentityService:
    def __init__(self, service: EngineService) -> None:
        self.svc = service

    def issue_session(
        self, session: Session, user: User, now: datetime, *, source: str = "password"
    ) -> dict[str, Any]:
        token = "strata_session_" + secrets.token_urlsafe(48)
        credential = AccessToken(
            id=identifier(),
            user_id=user.id,
            token_hash=hash_token(token),
            name="Browser or client session",
            kind="session",
            created_at=now,
            expires_at=now + timedelta(seconds=self.svc.settings.session_lifetime_seconds),
        )
        session.add(credential)
        with access_scope(Principal(user.id, user.username, user.is_admin, credential.id)):
            audit(session, now, "LOGIN_SUCCEEDED", user.id, source=source)
        return {
            "access_token": token,
            "token_type": "bearer",
            "expires_at": credential.expires_at,
            "user": user_view(user),
        }

    def create_user(
        self,
        username: str,
        password: str,
        display_name: str = "",
        is_admin: bool = False,
        *,
        bootstrap: bool = False,
    ) -> User:
        if not bootstrap:
            require_platform_admin()
        elif not is_admin:
            raise DomainError(422, "the bootstrap user must be a platform administrator")
        encoded = password_hash(password)
        username = username.strip().lower()
        if not username or len(username) > 128 or any(c.isspace() for c in username):
            raise DomainError(422, "username must contain 1..128 non-whitespace characters")
        with self.svc.factory.begin() as session:
            session.scalar(select(Admission).where(Admission.id == 1).with_for_update())
            if bootstrap and session.scalar(select(func.count()).select_from(User)):
                raise DomainError(409, "bootstrap requires an empty user table")
            if session.scalar(select(User).where(User.username == username)):
                raise DomainError(409, "username already exists")
            user = User(
                id=identifier(),
                username=username,
                password_hash=encoded,
                display_name=display_name or username,
                is_admin=is_admin,
                enabled=True,
                created_at=self.svc.now(session),
            )
            session.add(user)
            session.flush()
            audit(session, self.svc.now(session), "USER_CREATED", user.id, username=username)
            return user

    def login(self, username: str, password: str, address: str) -> dict[str, Any]:
        if not self.svc.settings.identity_enabled:
            raise DomainError(404, "individual authentication is disabled")
        username = username.strip().lower()
        result: dict[str, Any] | None = None
        error: DomainError | None = None
        with self.svc.factory.begin() as session:
            now = self.svc.now(session)
            assert session.bind is not None
            insert = postgres_insert if session.bind.dialect.name == "postgresql" else sqlite_insert
            buckets = []
            for key, limit in sorted(
                [
                    (hash_token("account:" + username), self.svc.settings.login_attempt_limit),
                    (hash_token("address:" + address), self.svc.settings.login_attempt_limit * 10),
                ]
            ):
                session.execute(
                    insert(LoginThrottle)
                    .values(
                        key=key,
                        failures=0,
                        window_start=now,
                    )
                    .on_conflict_do_nothing()
                )
                bucket = session.scalar(
                    select(LoginThrottle)
                    .where(
                        LoginThrottle.key == key,
                    )
                    .with_for_update()
                )
                assert bucket is not None
                if now >= bucket.window_start + timedelta(
                    seconds=self.svc.settings.login_window_seconds
                ):
                    bucket.window_start, bucket.failures = now, 0
                buckets.append(bucket)
                if bucket.failures >= limit:
                    error = DomainError(429, "login attempt limit reached; retry later")
            if error is None:
                user = session.scalar(
                    select(User).where(User.username == username).with_for_update()
                )
                encoded = user.password_hash if user and user.password_hash else DUMMY_HASH
                valid = verify_password(encoded, password)
                if not valid or user is None or not user.enabled or not user.password_hash:
                    for bucket in buckets:
                        bucket.failures += 1
                    audit(session, now, "LOGIN_REJECTED", account_hash=hash_token(username))
                    error = DomainError(401, "invalid username or password")
                else:
                    if PASSWORDS.check_needs_rehash(user.password_hash):
                        user.password_hash = PASSWORDS.hash(password)
                    # Keep address failures across successful logins to protect other accounts.
                    for bucket in buckets:
                        if bucket.key == hash_token("account:" + username):
                            bucket.failures = 0
                    result = self.issue_session(session, user, now)
        if error:
            raise error  # Persist failed attempts before returning the error.
        assert result is not None
        return result

    def authenticate(self, authorization: str | None) -> Principal:
        if not authorization or not authorization.startswith("Bearer ") or len(authorization) > 512:
            raise DomainError(401, "a valid individual access token is required")
        with self.svc.factory() as session:
            row = session.execute(
                select(AccessToken, User)
                .join(User)
                .where(
                    AccessToken.token_hash == hash_token(authorization[7:]),
                    AccessToken.revoked_at.is_(None),
                    AccessToken.expires_at > self.svc.now(session),
                    User.enabled.is_(True),
                )
            ).first()
            if row is None:
                raise DomainError(401, "access token is invalid, expired or revoked")
            token, user = row
            return Principal(
                user.id, user.username, user.is_admin, token.id, token.project_id, token.role
            )

    def project_access(self, actor: Principal, target: str, method: str) -> Principal:
        with self.svc.factory() as session:
            project = session.get(Project, target)
            member = session.get(ProjectMembership, (target, actor.user_id))
            if (
                project is None
                or not project.enabled
                or (member is None and not actor.platform_admin)
                or (actor.token_project_id and actor.token_project_id != target)
            ):
                raise DomainError(404, "project not found")
            role = "admin" if actor.platform_admin else member.role if member else "viewer"
            if actor.token_role:
                role = min((role, actor.token_role), key=lambda value: ROLES[value])
            if method not in {"GET", "HEAD", "OPTIONS"} and role == "viewer":
                raise DomainError(403, "project operator access is required")
            return replace(actor, project_id=target, role=role)

    def create_project(self, name: str, description: str = "") -> Project:
        actor = require_platform_admin()
        settings = self.svc.settings
        with self.svc.factory.begin() as session:
            row = Project(
                id=identifier(),
                name=name,
                description=description,
                enabled=True,
                queue_limit=settings.project_queue_limit,
                cpu_limit=settings.project_cpu_limit,
                memory_limit_mb=settings.project_memory_limit_mb,
                gpu_limit=settings.project_gpu_limit,
                storage_limit_bytes=settings.project_storage_limit_bytes,
                created_at=self.svc.now(session),
            )
            session.add(row)
            session.flush()
            session.add(ProjectMembership(project_id=row.id, user_id=actor.user_id, role="admin"))
            session.add(ProjectStorageLock(project_id=row.id))
            audit(session, self.svc.now(session), "PROJECT_CREATED", row.id, row.id)
            return row

    def change_password(self, current: str, replacement: str) -> None:
        actor = require_actor()
        if actor.token_project_id:
            raise DomainError(403, "a full user session is required")
        encoded = password_hash(replacement)
        with self.svc.factory.begin() as session:
            user = session.execute(
                select(User).where(User.id == actor.user_id).with_for_update()
            ).scalar_one()
            if not user.password_hash or not verify_password(user.password_hash, current):
                raise DomainError(401, "current password is incorrect")
            user.password_hash = encoded
            now = self.svc.now(session)
            session.execute(
                update(AccessToken)
                .where(AccessToken.user_id == user.id, AccessToken.revoked_at.is_(None))
                .values(revoked_at=now)
            )
            audit(session, now, "PASSWORD_CHANGED", user.id)

    def projects(self) -> list[dict[str, Any]]:
        actor = require_actor()
        with self.svc.factory() as session:
            query = select(Project)
            if not actor.platform_admin:
                query = query.join(ProjectMembership).where(
                    ProjectMembership.user_id == actor.user_id
                )
            if actor.token_project_id:
                query = query.where(Project.id == actor.token_project_id)
            return [
                project_view(row) for row in session.scalars(query.order_by(Project.created_at))
            ]

    def update_user(self, target: str, changes: dict[str, Any]) -> User:
        require_platform_admin()
        if "password" in changes:
            changes["password_hash"] = password_hash(changes.pop("password"))
        with self.svc.factory.begin() as session:
            session.scalar(select(Admission).where(Admission.id == 1).with_for_update())
            user = session.scalar(select(User).where(User.id == target).with_for_update())
            if user is None:
                raise DomainError(404, "user not found")
            for key, value in changes.items():
                setattr(user, key, value)
            if not session.scalar(
                select(func.count())
                .select_from(User)
                .where(
                    User.is_admin.is_(True),
                    User.enabled.is_(True),
                )
            ):
                raise DomainError(409, "at least one enabled platform administrator is required")
            if not user.enabled or "password_hash" in changes:
                for token in session.scalars(
                    select(AccessToken).where(AccessToken.user_id == user.id)
                ):
                    token.revoked_at = self.svc.now(session)
            audit(
                session,
                self.svc.now(session),
                "USER_UPDATED",
                user.id,
                fields=[key for key in changes if key != "password_hash"],
            )
            return user

    def membership(self, target: str, user_id: str, role: str | None) -> None:
        actor = require_actor()
        if not (actor.platform_admin and not actor.token_project_id):
            actor = self.project_access(actor, target, "POST")
            if actor.role != "admin":
                raise DomainError(403, "project administrator access is required")
        with self.svc.factory.begin() as session:
            project = session.scalar(select(Project).where(Project.id == target).with_for_update())
            user = session.get(User, user_id)
            if project is None or user is None:
                raise DomainError(404, "project or user not found")
            member = session.get(ProjectMembership, (target, user_id))
            if role:
                if role not in ROLES:
                    raise DomainError(422, "invalid project role")
                if member:
                    member.role = role
                else:
                    session.add(ProjectMembership(project_id=target, user_id=user_id, role=role))
            elif member:
                session.delete(member)
            session.flush()
            if not session.scalar(
                select(func.count())
                .select_from(ProjectMembership)
                .where(
                    ProjectMembership.project_id == target,
                    ProjectMembership.role == "admin",
                )
            ):
                raise DomainError(409, "at least one project administrator is required")
            audit(
                session,
                self.svc.now(session),
                "PROJECT_MEMBERSHIP_CHANGED",
                user_id,
                target,
                role=role,
            )

    def update_project(self, target: str, changes: dict[str, Any]) -> Project:
        require_platform_admin()  # Members cannot grant themselves larger resource budgets.
        with self.svc.factory.begin() as session:
            project = session.scalar(select(Project).where(Project.id == target).with_for_update())
            if project is None:
                raise DomainError(404, "project not found")
            for key, value in changes.items():
                setattr(project, key, value)
            audit(
                session,
                self.svc.now(session),
                "PROJECT_UPDATED",
                target,
                target,
                fields=list(changes),
            )
            return project

    def issue_token(
        self, name: str, target: str, role: str, lifetime_seconds: int
    ) -> dict[str, Any]:
        actor = self.project_access(require_actor(), target, "GET")
        assert actor.role is not None
        if ROLES[role] > ROLES[actor.role]:
            raise DomainError(403, "token privileges exceed project membership")
        with self.svc.factory.begin() as session:
            now = self.svc.now(session)
            raw = "strata_pat_" + secrets.token_urlsafe(48)
            row = AccessToken(
                id=identifier(),
                user_id=actor.user_id,
                token_hash=hash_token(raw),
                name=name,
                kind="personal",
                project_id=target,
                role=role,
                created_at=now,
                expires_at=now + timedelta(seconds=lifetime_seconds),
            )
            session.add(row)
            audit(session, now, "ACCESS_TOKEN_CREATED", row.id, target, role=role)
            return {"id": row.id, "access_token": raw, "expires_at": row.expires_at}

    def revoke_token(self, target: str) -> None:
        actor = require_actor()
        with self.svc.factory.begin() as session:
            row = session.get(AccessToken, target)
            if (
                row is None
                or row.user_id != actor.user_id
                or (actor.token_project_id and row.project_id != actor.token_project_id)
            ):
                raise DomainError(404, "access token not found")
            row.revoked_at = self.svc.now(session)
            audit(session, row.revoked_at, "ACCESS_TOKEN_REVOKED", row.id, row.project_id)

    def audit_events(self, target: str, after: int, limit: int) -> list[dict[str, Any]]:
        actor = self.project_access(require_actor(), target, "GET")
        if actor.role != "admin":
            raise DomainError(403, "project administrator access is required")
        with self.svc.factory() as session:
            return [
                {column.name: getattr(row, column.name) for column in row.__table__.columns}
                for row in session.scalars(
                    select(AuditEvent)
                    .where(
                        AuditEvent.project_id == target,
                        AuditEvent.id > after,
                    )
                    .order_by(AuditEvent.id)
                    .limit(limit)
                )
            ]
