"""Authorization-code OIDC with PKCE, browser-bound state and explicit account links."""

import base64
import hashlib
import json
import secrets
import threading
import time
from datetime import timedelta
from typing import Any
from urllib.parse import quote_plus, urlencode, urlsplit

import httpx
import jwt
from cryptography.fernet import Fernet, InvalidToken
from sqlalchemy import func, select, update

from control_plane.access import audit, require_platform_admin
from control_plane.identity import IdentityService, hash_token
from control_plane.models import (
    AccessToken,
    Admission,
    OIDCHandoff,
    OIDCIdentity,
    OIDCState,
    User,
    identifier,
)
from control_plane.services import DomainError, EngineService


def origin(url: str) -> str:
    value = urlsplit(url)
    return f"{value.scheme}://{value.netloc}"


class OIDCService:
    def __init__(self, svc: EngineService) -> None:
        self.svc = svc
        self._metadata: dict[str, Any] = {}
        self._keys: list[dict[str, Any]] = []
        self._expires = 0.0
        self._lock = threading.RLock()

    def enabled(self) -> None:
        if not self.svc.settings.identity_enabled or not self.svc.settings.oidc_issuer:
            raise DomainError(404, "federated authentication is disabled")

    def endpoint(self, url: Any) -> str:
        if not isinstance(url, str):
            raise DomainError(503, "identity provider metadata is invalid")
        parsed = urlsplit(url)
        allowed = {origin(self.svc.settings.oidc_issuer), *self.svc.settings.oidc_allowed_origins}
        if origin(url) not in allowed or parsed.username or parsed.password or parsed.fragment:
            raise DomainError(503, "identity provider endpoint origin is not approved")
        return url

    def document(
        self,
        url: str,
        *,
        data: dict[str, str] | None = None,
        auth: httpx.BasicAuth | None = None,
    ) -> dict[str, Any]:
        try:
            with (
                httpx.Client(timeout=5, follow_redirects=False, trust_env=False) as client,
                client.stream(
                    "POST" if data is not None else "GET", url, data=data, auth=auth
                ) as response,
            ):
                if data is not None and response.status_code in {400, 401}:
                    raise DomainError(401, "identity provider rejected the authorization code")
                response.raise_for_status()
                content = bytearray()
                for chunk in response.iter_bytes():
                    content.extend(chunk)
                    if len(content) > 262144:
                        raise DomainError(503, "identity provider response exceeds 256 KiB")
            value = json.loads(content)
            if not isinstance(value, dict):
                raise ValueError("expected object")
            return value
        except (httpx.HTTPError, ValueError) as exc:
            raise DomainError(503, "identity provider request failed") from exc

    def metadata(self) -> dict[str, Any]:
        self.enabled()
        with self._lock:
            if self._metadata and time.monotonic() < self._expires:
                return self._metadata
            value = self.document(
                self.svc.settings.oidc_issuer.rstrip("/") + "/.well-known/openid-configuration"
            )
            if value.get("issuer") != self.svc.settings.oidc_issuer or "S256" not in value.get(
                "code_challenge_methods_supported", []
            ):
                raise DomainError(503, "identity provider issuer or PKCE support is invalid")
            for name in ("authorization_endpoint", "token_endpoint", "jwks_uri"):
                self.endpoint(value.get(name))
            supported = value.get("token_endpoint_auth_methods_supported", ["client_secret_basic"])
            if self.svc.settings.oidc_token_auth_method not in supported:
                raise DomainError(503, "identity provider does not support the token auth method")
            self._metadata, self._keys = value, []
            self._expires = time.monotonic() + 300
            return value

    def start(self, address: str) -> tuple[str, str]:
        self.enabled()
        state, browser, nonce, verifier = (secrets.token_urlsafe(48) for _ in range(4))
        with self.svc.factory.begin() as session:
            session.scalar(select(Admission).where(Admission.id == 1).with_for_update())
            now = self.svc.now(session)
            count = (
                session.scalar(
                    select(func.count())
                    .select_from(OIDCState)
                    .where(OIDCState.expires_at > now, OIDCState.consumed_at.is_(None))
                )
                or 0
            )
            address_hash = hash_token(address)
            recent = (
                session.scalar(
                    select(func.count())
                    .select_from(OIDCState)
                    .where(
                        OIDCState.address_hash == address_hash,
                        OIDCState.created_at
                        > now - timedelta(seconds=self.svc.settings.login_window_seconds),
                    )
                )
                or 0
            )
            if count >= 1000 or recent >= self.svc.settings.login_attempt_limit * 10:
                raise DomainError(429, "federated login capacity reached; retry later")
            session.add(
                OIDCState(
                    state_hash=hash_token(state),
                    browser_hash=hash_token(browser),
                    address_hash=address_hash,
                    nonce_hash=hash_token(nonce),
                    encrypted_verifier=Fernet(self.svc.settings.oidc_state_encryption_key.encode())
                    .encrypt(verifier.encode())
                    .decode(),
                    created_at=now,
                    expires_at=now + timedelta(minutes=10),
                )
            )
        metadata = self.metadata()
        params = {
            "client_id": self.svc.settings.oidc_client_id,
            "response_type": "code",
            "scope": "openid",
            "redirect_uri": self.svc.settings.oidc_redirect_uri,
            "state": state,
            "nonce": nonce,
            "code_challenge_method": "S256",
            "code_challenge": base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest())
            .decode()
            .rstrip("="),
        }
        return metadata["authorization_endpoint"] + (
            "&" if "?" in metadata["authorization_endpoint"] else "?"
        ) + urlencode(params), browser

    def identity(self, token: str, nonce_hash: str, access_token: str) -> str:
        try:
            if len(token) > 32768:
                raise ValueError("token exceeds limit")
            header = jwt.get_unverified_header(token)
            algorithm, kid = header.get("alg"), header.get("kid")
            if (
                algorithm not in {"RS256", "ES256"}
                or not isinstance(kid, str)
                or not 1 <= len(kid) <= 128
            ):
                raise ValueError("unsupported signature")
            metadata = self.metadata()
            with self._lock:
                if not self._keys or not any(key.get("kid") == kid for key in self._keys):
                    keys = self.document(metadata["jwks_uri"]).get("keys")
                    if (
                        not isinstance(keys, list)
                        or len(keys) > 50
                        or any(not isinstance(key, dict) for key in keys)
                    ):
                        raise ValueError("invalid keys")
                    self._keys = keys
                matches = [
                    key
                    for key in self._keys
                    if key.get("kid") == kid
                    and key.get("use", "sig") == "sig"
                    and key.get("alg", algorithm) == algorithm
                ]
                if len(matches) != 1:
                    raise ValueError("signing key unavailable")
                key = jwt.PyJWK.from_dict(matches[0], algorithm=algorithm).key
            claims = jwt.decode(
                token,
                key,
                algorithms=[algorithm],
                issuer=self.svc.settings.oidc_issuer,
                audience=self.svc.settings.oidc_client_id,
                leeway=30,
                options={"require": ["iss", "sub", "aud", "exp", "iat", "nonce"]},
            )
            audience = claims["aud"] if isinstance(claims["aud"], list) else [claims["aud"]]
            if (
                audience != [self.svc.settings.oidc_client_id]
                or claims.get("azp", self.svc.settings.oidc_client_id)
                != self.svc.settings.oidc_client_id
            ):
                raise ValueError("untrusted audience or authorized party")
            subject = claims["sub"]
            if (
                not isinstance(subject, str)
                or not 1 <= len(subject) <= 255
                or not subject.isascii()
            ):
                raise ValueError("invalid subject")
            if not isinstance(claims["nonce"], str) or not secrets.compare_digest(
                hash_token(claims["nonce"]), nonce_hash
            ):
                raise ValueError("nonce mismatch")
            if "at_hash" in claims:
                expected = (
                    base64.urlsafe_b64encode(
                        hashlib.sha256(access_token.encode("ascii")).digest()[:16]
                    )
                    .decode()
                    .rstrip("=")
                )
                if not isinstance(claims["at_hash"], str) or not secrets.compare_digest(
                    claims["at_hash"], expected
                ):
                    raise ValueError("access token hash mismatch")
            return subject
        except (jwt.PyJWTError, ValueError, TypeError, KeyError, OverflowError) as exc:
            raise DomainError(401, "identity provider token validation failed") from exc

    def callback(self, state: str, browser: str, code: str) -> str:
        self.enabled()
        with self.svc.factory.begin() as session:
            row = session.scalar(
                select(OIDCState).where(OIDCState.state_hash == hash_token(state)).with_for_update()
            )
            now = self.svc.now(session)
            if (
                row is None
                or row.consumed_at
                or row.expires_at <= now
                or not secrets.compare_digest(row.browser_hash, hash_token(browser))
            ):
                raise DomainError(
                    401, "login state is invalid, expired or belongs to another browser"
                )
            row.consumed_at = now
            nonce_hash, encrypted = row.nonce_hash, row.encrypted_verifier
        try:
            verifier = (
                Fernet(self.svc.settings.oidc_state_encryption_key.encode())
                .decrypt(encrypted.encode())
                .decode()
            )
        except InvalidToken as exc:
            raise DomainError(401, "login configuration changed; start again") from exc
        body = {
            "grant_type": "authorization_code",
            "code": code,
            "client_id": self.svc.settings.oidc_client_id,
            "redirect_uri": self.svc.settings.oidc_redirect_uri,
            "code_verifier": verifier,
        }
        auth = None
        if self.svc.settings.oidc_token_auth_method == "client_secret_post":
            body["client_secret"] = self.svc.settings.oidc_client_secret
        elif self.svc.settings.oidc_token_auth_method == "client_secret_basic":
            auth = httpx.BasicAuth(
                quote_plus(self.svc.settings.oidc_client_id),
                quote_plus(self.svc.settings.oidc_client_secret),
            )
        reply = self.document(self.metadata()["token_endpoint"], data=body, auth=auth)
        if (
            not isinstance(reply.get("id_token"), str)
            or not isinstance(reply.get("access_token"), str)
            or str(reply.get("token_type", "")).lower() != "bearer"
        ):
            raise DomainError(401, "identity provider response is invalid")
        subject = self.identity(reply["id_token"], nonce_hash, reply["access_token"])
        handoff = secrets.token_urlsafe(48)
        with self.svc.factory.begin() as session:
            row_identity = session.scalar(
                select(OIDCIdentity)
                .join(User, User.id == OIDCIdentity.user_id)
                .where(
                    OIDCIdentity.issuer == self.svc.settings.oidc_issuer,
                    OIDCIdentity.subject == subject,
                    OIDCIdentity.enabled.is_(True),
                    User.enabled.is_(True),
                )
            )
            if row_identity is None:
                raise DomainError(403, "federated account has not been provisioned")
            now = self.svc.now(session)
            session.add(
                OIDCHandoff(
                    token_hash=hash_token(handoff),
                    identity_id=row_identity.id,
                    browser_hash=hash_token(browser),
                    created_at=now,
                    expires_at=now + timedelta(seconds=60),
                )
            )
        return handoff

    def exchange(self, handoff: str, browser: str) -> dict[str, Any]:
        self.enabled()
        with self.svc.factory.begin() as session:
            session.scalar(select(Admission).where(Admission.id == 1).with_for_update())
            row = session.scalar(
                select(OIDCHandoff)
                .where(OIDCHandoff.token_hash == hash_token(handoff))
                .with_for_update()
            )
            now = self.svc.now(session)
            if (
                row is None
                or row.consumed_at
                or row.expires_at <= now
                or not secrets.compare_digest(row.browser_hash, hash_token(browser))
            ):
                raise DomainError(401, "federated login handoff is invalid or expired")
            identity = session.get(OIDCIdentity, row.identity_id)
            user = (
                session.scalar(select(User).where(User.id == identity.user_id).with_for_update())
                if identity and identity.enabled
                else None
            )
            if user is None or not user.enabled:
                raise DomainError(403, "federated account is unavailable")
            row.consumed_at = now
            return IdentityService(self.svc).issue_session(session, user, now, source="oidc")

    def link(self, user_id: str, subject: str) -> OIDCIdentity:
        require_platform_admin()
        self.enabled()
        if not 1 <= len(subject) <= 255 or not subject.isascii():
            raise DomainError(422, "subject must contain 1..255 ASCII characters")
        with self.svc.factory.begin() as session:
            session.scalar(select(Admission).where(Admission.id == 1).with_for_update())
            if session.get(User, user_id) is None:
                raise DomainError(404, "user not found")
            row = session.scalar(
                select(OIDCIdentity).where(
                    OIDCIdentity.issuer == self.svc.settings.oidc_issuer,
                    OIDCIdentity.subject == subject,
                )
            )
            if row is not None and row.user_id != user_id:
                raise DomainError(409, "federated subject already belongs to another account")
            if row is None:
                row = OIDCIdentity(
                    id=identifier(),
                    issuer=self.svc.settings.oidc_issuer,
                    subject=subject,
                    user_id=user_id,
                    enabled=True,
                    created_at=self.svc.now(session),
                )
                session.add(row)
            else:
                row.enabled = True
            audit(session, self.svc.now(session), "OIDC_IDENTITY_LINKED", row.id, user_id=user_id)
            return row

    def disable(self, identity_id: str) -> None:
        require_platform_admin()
        with self.svc.factory.begin() as session:
            session.scalar(select(Admission).where(Admission.id == 1).with_for_update())
            row = session.get(OIDCIdentity, identity_id)
            if row is None:
                raise DomainError(404, "federated identity not found")
            row.enabled = False
            now = self.svc.now(session)
            session.execute(
                update(AccessToken)
                .where(AccessToken.user_id == row.user_id, AccessToken.revoked_at.is_(None))
                .values(revoked_at=now)
            )
            audit(session, now, "OIDC_IDENTITY_DISABLED", row.id, user_id=row.user_id)
