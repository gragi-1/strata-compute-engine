"""Disposable single-process browser fixture; no execution agent or production database."""

import argparse
import tempfile
from pathlib import Path

import uvicorn

from control_plane.access import Principal, access_scope
from control_plane.api import create_app
from control_plane.config import Settings
from control_plane.database import Base, make_engine, sessions
from control_plane.identity import IdentityService
from control_plane.models import Admission
from control_plane.services import EngineService


def serve(port: int) -> None:
    directory = Path("build/browser")
    directory.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="fixture-", dir=directory) as temporary:
        root = Path(temporary).resolve()
        config = Settings(
            database_url=f"sqlite:///{root / 'browser.db'}",
            artifact_root=root / "artifacts",
            identity_enabled=True,
            allowed_images=["strata/python-workloads:local"],
        )
        engine = make_engine(config.database_url)
        Base.metadata.create_all(engine)
        factory = sessions(engine)
        with factory.begin() as session:
            session.add(Admission(id=1))
        service = EngineService(factory, config)
        identity = IdentityService(service)
        admin = identity.create_user(
            "browser-admin", "Browser-test-password-1004", bootstrap=True, is_admin=True
        )
        with access_scope(Principal(admin.id, admin.username, True, "fixture")):
            first = identity.create_project("Browser research")
            identity.create_project("Private project")
            viewer = identity.create_user("browser-viewer", "Browser-test-password-1004")
            identity.membership(first.id, viewer.id, "viewer")
        try:
            uvicorn.run(create_app(config, service), host="127.0.0.1", port=port, access_log=False)
        finally:
            engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, required=True)
    serve(parser.parse_args().port)
