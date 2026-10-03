import secrets
from typing import Annotated

from fastapi import Header

from control_plane.config import Settings
from control_plane.services import DomainError


def authorize(config: Settings, method: str, authorization: str | None) -> str:
    if not config.api_keys:
        return "admin"  # Explicit local development mode; production rejects this configuration.
    supplied = (authorization or "").removeprefix("Bearer ")
    role = next(
        (role for key, role in config.api_keys.items() if secrets.compare_digest(key, supplied)),
        None,
    )
    if role is None or not (authorization or "").startswith("Bearer "):
        raise DomainError(401, "a valid API key is required")
    if method not in {"GET", "HEAD", "OPTIONS"} and role == "viewer":
        raise DomainError(403, "operator access is required")
    return role


def worker_authorization(config: Settings, authorization: Annotated[str | None, Header()]) -> None:
    if not secrets.compare_digest(authorization or "", f"Bearer {config.worker_token}"):
        raise DomainError(401, "invalid worker token")
