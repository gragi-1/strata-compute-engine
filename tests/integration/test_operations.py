import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from unittest.mock import patch

import pytest
from sqlalchemy import select, text
from sqlalchemy.engine import make_url

from control_plane.datasets import DatasetService
from control_plane.maintenance import verify_store
from control_plane.models import MaintenanceState
from control_plane.operations import OperationsService
from control_plane.schemas import NamedResource


@pytest.mark.postgres
def test_supervision_backup_restore_drill_and_verified_snapshot_retention(
    postgres_service, tmp_path
):
    svc = postgres_service
    with svc.factory() as session:
        schema = session.scalar(text("SELECT current_schema()"))
    svc.settings.database_url = (
        make_url(svc.settings.database_url)
        .update_query_dict({"options": f"-csearch_path={schema}"})
        .render_as_string(hide_password=False)
    )
    svc.settings.backup_root = tmp_path / "backups"
    svc.settings.backup_keep = 1
    svc.settings.backup_restore_drill = True
    datasets = DatasetService(svc)
    row = datasets.create(NamedResource(name="Operational recovery evidence"))
    version = datasets.version(row.id, "v1")
    file = datasets.upload(version.id, "measurements.csv", [b"x,y\n1,2\n"])
    service = OperationsService(svc)
    assert service.tick() == {"maintenance": True, "backup": True}
    with svc.factory() as session:
        state = session.get(MaintenanceState, "backup")
        assert state.status == "SUCCEEDED", state.last_error
        first = state.result["snapshot"]
        assert state.result["restore_drill"] == {"verified_blobs": 1, "database_restored": True}
    assert verify_store(svc.settings.backup_root / first)["blobs"] == {file.sha256: file.size}
    assert service.tick() == {"maintenance": False, "backup": False}
    # Failed work leaves the last verified copy intact and does not expose credentials.
    svc.clock.advance(svc.settings.backup_interval_seconds + 1)
    with patch("control_plane.operations.backup_store", side_effect=ValueError("secret-url")):
        service.tick()
    with svc.factory() as session:
        state = session.get(MaintenanceState, "backup")
        assert state.status == "FAILED" and state.last_error == "ValueError"
        assert "secret-url" not in str(state.result)
    assert (svc.settings.backup_root / first).is_dir()
    svc.clock.advance(61)
    assert service.tick()["backup"]
    with svc.factory() as session:
        state = session.get(MaintenanceState, "backup")
        assert state.status == "SUCCEEDED"
        assert state.result["pruned_snapshots"] == [first]
        assert state.result["snapshot"] != first
    assert not (svc.settings.backup_root / first).exists()
    with svc.factory() as session:
        assert not session.scalars(
            select(MaintenanceState).where(MaintenanceState.status == "FAILED")
        ).all()
    assert not list(svc.settings.backup_root.glob(".drill-*"))
    with svc.factory() as session:
        assert not session.scalars(
            text("SELECT datname FROM pg_database WHERE datname LIKE 'strata_drill_%'")
        ).all()


@pytest.mark.postgres
def test_operation_lock_coalesces_runners_and_recovers_a_crashed_status(postgres_service):
    svc, entered, release = postgres_service, threading.Event(), threading.Event()
    ops = OperationsService(svc)

    def action():
        entered.set()
        assert release.wait(10)
        return {"evidence": "one owner"}

    with ThreadPoolExecutor(2) as pool:
        first = pool.submit(ops.operation, "evidence", 300, action)
        assert entered.wait(10)
        try:
            assert pool.submit(ops.operation, "evidence", 300, action).result(timeout=5) is False
        finally:
            release.set()
        assert first.result(timeout=10) is True
    with svc.factory.begin() as session:
        row = session.get(MaintenanceState, "evidence")
        row.status = "RUNNING"
        row.next_run_at = svc.clock() + timedelta(days=1)
    assert ops.operation("evidence", 300, lambda: {"recovered": True})
    with svc.factory() as session:
        assert session.get(MaintenanceState, "evidence").result == {"recovered": True}
