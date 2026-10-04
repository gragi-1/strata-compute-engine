from typing import Any

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import Field
from sqlalchemy import select

from control_plane.access import audit, require_platform_admin
from control_plane.models import OIDCIdentity
from control_plane.oidc import OIDCService, origin
from control_plane.schemas import StrictModel
from control_plane.services import DomainError, EngineService

COOKIE = "strata_oidc_browser"
HANDOFF = "strata_oidc_handoff"
COOKIE_PATH = "/auth/oidc"


class IdentityLink(StrictModel):
    user_id: str = Field(min_length=1, max_length=36)
    subject: str = Field(min_length=1, max_length=255)


def oidc_router(svc: EngineService) -> APIRouter:
    from control_plane.api import row_view

    router = APIRouter()
    service = OIDCService(svc)

    @router.get("/auth/oidc/start")
    def start(request: Request) -> RedirectResponse:
        location, browser = service.start(request.client.host if request.client else "unknown")
        response = RedirectResponse(
            location, status_code=303, headers={"Cache-Control": "no-store"}
        )
        response.set_cookie(
            COOKIE,
            browser,
            httponly=True,
            secure=svc.settings.oidc_redirect_uri.startswith("https://"),
            samesite="lax",
            path=COOKIE_PATH,
            max_age=600,
        )
        return response

    @router.get("/auth/oidc/callback")
    def callback(
        request: Request,
        state: str = Query("", max_length=256),
        code: str = Query("", max_length=4096),
        error: str | None = Query(None, max_length=128),
    ) -> RedirectResponse:
        try:
            if error or not code or not state:
                raise DomainError(401, "identity provider did not authorize this login")
            handoff = service.callback(state, request.cookies.get(COOKIE, ""), code)
        except DomainError as exc:
            with svc.factory.begin() as session:
                audit(session, svc.now(session), "OIDC_LOGIN_REJECTED", code=exc.code)
            response = RedirectResponse(
                f"/?sso=failed&code={exc.code}",
                status_code=303,
                headers={"Cache-Control": "no-store"},
            )
            response.delete_cookie(COOKIE, path=COOKIE_PATH)
            response.delete_cookie(HANDOFF, path=COOKIE_PATH)
            return response
        response = RedirectResponse(
            "/?sso=complete", status_code=303, headers={"Cache-Control": "no-store"}
        )
        response.set_cookie(
            HANDOFF,
            handoff,
            httponly=True,
            secure=svc.settings.oidc_redirect_uri.startswith("https://"),
            samesite="strict",
            path=COOKIE_PATH,
            max_age=60,
        )
        return response

    @router.post("/auth/oidc/session")
    def session(request: Request) -> JSONResponse:
        if request.headers.get("origin") != origin(svc.settings.oidc_redirect_uri):
            raise DomainError(403, "federated session exchange requires the workspace origin")
        value = service.exchange(request.cookies.get(HANDOFF, ""), request.cookies.get(COOKIE, ""))
        from fastapi.encoders import jsonable_encoder

        response = JSONResponse(jsonable_encoder(value), headers={"Cache-Control": "no-store"})
        response.delete_cookie(COOKIE, path=COOKIE_PATH)
        response.delete_cookie(HANDOFF, path=COOKIE_PATH)
        return response

    @router.get("/auth/oidc/identities")
    def identities() -> Any:
        require_platform_admin()
        with svc.factory() as session:
            return [
                row_view(row)
                for row in session.scalars(
                    select(OIDCIdentity).order_by(OIDCIdentity.created_at).limit(1000)
                )
            ]

    @router.post("/auth/oidc/identities", status_code=201)
    def link(body: IdentityLink) -> Any:
        return row_view(service.link(body.user_id, body.subject))

    @router.delete("/auth/oidc/identities/{identity_id}", status_code=204)
    def disable(identity_id: str) -> None:
        service.disable(identity_id)

    return router
