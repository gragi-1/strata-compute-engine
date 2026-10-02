import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text

from control_plane.config import Settings
from control_plane.database import Base, make_engine, sessions
from control_plane.models import Admission
from control_plane.services import EngineService


class Clock:
    def __init__(self):
        self.value = datetime(2026, 10, 2, tzinfo=UTC)

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += timedelta(seconds=seconds)


@pytest.fixture
def service(tmp_path):
    config = Settings(
        database_url=f"sqlite:///{tmp_path / 'test.db'}",
        artifact_root=tmp_path / "artifacts",
        retry_jitter_seconds=0,
    )
    engine = make_engine(config.database_url)
    Base.metadata.create_all(engine)
    factory = sessions(engine)
    with factory.begin() as session:
        session.add(Admission(id=1))
    svc = EngineService(factory, config, Clock())
    yield svc
    engine.dispose()


@pytest.fixture
def postgres_service(tmp_path):
    url = os.getenv("STRATA_TEST_POSTGRES_URL")
    if not url:
        pytest.skip("set STRATA_TEST_POSTGRES_URL for real PostgreSQL concurrency tests")
    schema = f"strata_test_{uuid4().hex}"
    admin = create_engine(url)
    with admin.begin() as connection:
        connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(
        url, connect_args={"options": f"-csearch_path={schema}"}, pool_size=12, max_overflow=20
    )
    Base.metadata.create_all(engine)
    config = Settings(
        database_url=url, artifact_root=tmp_path / "artifacts", retry_jitter_seconds=0
    )
    factory = sessions(engine)
    with factory.begin() as session:
        session.add(Admission(id=1))
    yield EngineService(factory, config, Clock())
    engine.dispose()
    with admin.begin() as connection:
        connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    admin.dispose()
