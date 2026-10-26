"""Build actual synthetic inventory and test bounded old-application rollback read compatibility."""

import asyncio
import json
import os
import runpy
import sqlite3
import subprocess
import sys
from contextlib import closing
from dataclasses import asdict
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from pathlib import Path
from shutil import copytree
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid7

import pytest
from sqlalchemy.engine import URL

from scripts import verify_inventory_rollback as probe
from snapper.application.services.settings import SettingsService
from snapper.application.trade.futures_inventory_read import refresh_current_futures_inventory
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.data.futures_inventory_read_store import begin_futures_inventory_read
from snapper.data.models import Instrument
from snapper.data.models import Setting
from snapper.data.repository import SQLAlchemyRepository
from tests.helpers.futures_inventory_migrated import migrated_inventory_database
from tests.helpers.futures_inventory_migrated import migrated_inventory_scenario


async def build_rollback_fixture(directory: Path) -> None:
    """Use actual current migrated storage and installed SDK with synthetic local HTTP only.

    Args:
        directory: Newly owned output namespace supplied exclusively by preparation mode.
    """
    url = URL.create("sqlite+aiosqlite", database=str(directory / "inventory.db"))
    with pytest.MonkeyPatch.context() as patch:
        async with (
            migrated_inventory_database(url) as database,
            migrated_inventory_scenario(directory, patch, database) as scenario,
        ):
            native = scenario.native
            captured = await refresh_current_futures_inventory(
                native.repository, native.client, "kraken_futures.open_orders", native.tracker
            )
            assert captured.available and captured.outcome is not None
            pending = await begin_futures_inventory_read(
                native.repository,
                captured.outcome.attempt.key_proof,
                "kraken_futures.open_positions",
                ExecutionEvidenceIdentity(
                    public_id=str(uuid7()),
                    session_id=native.tracker.session_id,
                    sequence_id=900,
                ),
            )
            assert pending.payload.family_sequence == 1
            assert len(database.observer.seen) == len(scenario.adapter.inventory_calls) == 1
            async with native.repository.session() as session:
                session.add(
                    Setting(
                        public_id=str(uuid7()),
                        session_id=native.tracker.session_id,
                        sequence_id=901,
                        timestamp=datetime.now(UTC),
                        key=probe.FIXTURE_SETTING,
                        value=json.dumps(probe.FIXTURE_VALUE),
                        category="synthetic-rollback",
                        is_encrypted=False,
                    )
                )
                session.add(
                    Instrument(
                        public_id=probe.FIXTURE_INSTRUMENT,
                        session_id=native.tracker.session_id,
                        sequence_id=902,
                        timestamp=datetime.now(UTC),
                        symbol_public_id=database.native.catalog_symbol.public_id,
                        exchange="kraken_futures",
                    )
                )
                await session.commit()
    with closing(sqlite3.connect(directory / "inventory.db")) as connection:
        assert connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone() == (0, 0, 0)


@pytest.fixture(scope="module")
def rollback_template(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Create the genuine SDK/migrated template once without touching any preexisting database."""
    directory = tmp_path_factory.mktemp("inventory-rollback") / "new-fixture"
    asyncio.run(probe.prepare_fixture(directory))
    return directory


@pytest.fixture
def rollback_fixture(rollback_template: Path, tmp_path: Path) -> Path:
    """Copy only closed, checkpointed synthetic fixture bytes into each private test namespace."""
    return Path(copytree(rollback_template, tmp_path / "fixture"))


async def test_actual_application_initializes_and_reads_while0066_inventory_stays_exact(
    rollback_fixture: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Run real SettingsService initialization and old read APIs on actual pending/captured history.

    Given: A genuinely migrated SDK fixture with one captured and one pending endpoint,
    When: Application startup and existing settings/wallet/recovery reads use that read-only file,
    Then: Evidence remains exact, inherited settings are excluded and resources are closed.
    """
    before = probe.fixture_snapshot(rollback_fixture)
    monkeypatch.setenv("DB_URL", "postgresql://synthetic-must-not-be-used")
    monkeypatch.setenv("UNRELATED_PRIVATE_SETTING", "synthetic-not-forwarded")
    original = dict(os.environ)
    result = await probe.probe_fixture(rollback_fixture)
    assert result == before and result.pending == result.captured == 1
    assert dict(os.environ) == original
    assert SettingsService._instance is not None
    assert SettingsService._instance._zmq_context is None
    assert SettingsService._instance._msg_publisher is None
    with pytest.raises(FileExistsError):
        await probe.prepare_fixture(rollback_fixture)


@pytest.mark.parametrize("kind", ("missing", "symlink"))
def test_snapshot_refuses_missing_or_symlinked_database(tmp_path: Path, kind: str) -> None:
    """Given an absent or redirected database, when checked, then no external DB is opened.

    Given: A new fixture directory without a real owned database file,
    When: Snapshot validation sees a missing path or a symlink to another file,
    Then: The explicit synthetic-fixture requirement refuses that target.
    """
    if kind == "symlink":
        target = tmp_path / "other.db"
        target.touch()
        (tmp_path / "inventory.db").symlink_to(target)
    with pytest.raises(probe.InventoryRollbackProbeError, match="database_required"):
        probe.fixture_snapshot(tmp_path)


@pytest.mark.parametrize("fault", ("revision", "captured"))
def test_snapshot_requires_actual0066_pending_and_captured_rows(
    rollback_fixture: Path, fault: str
) -> None:
    """Given missing physical prerequisites, when measured, then metadata cannot pretend acceptance.

    Given: A private copy of genuine synthetic inventory with one deliberately invalid precondition,
    When: Its actual migration revision or captured history no longer meets the fixture contract,
    Then: The snapshot refuses the real altered database before application startup.
    """
    with closing(sqlite3.connect(rollback_fixture / "inventory.db")) as connection:
        if fault == "revision":
            connection.execute("UPDATE alembic_version SET version_num='0065'")
        else:
            connection.execute("DROP TRIGGER futures_inventory_read_outcomes_reject_delete")
            connection.execute("DELETE FROM futures_inventory_read_outcomes")
        connection.commit()
    with pytest.raises(probe.InventoryRollbackProbeError):
        probe.fixture_snapshot(rollback_fixture)


@pytest.mark.parametrize("fault", ("shape", "kind", "digest"))
async def test_manifest_mismatch_refuses_before_application_import(
    rollback_fixture: Path, fault: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Given an invalid synthetic manifest, when probed, then no application connection opens.

    Given: A genuine copied fixture whose manifest shape, kind or fingerprint was changed,
    When: The old-application probe validates its explicitly supplied input,
    Then: It refuses before loading an application repository or settings service.
    """
    manifest: object = {"kind": probe.FIXTURE_KIND, "snapshot": {}}
    if fault == "shape":
        manifest = []
    elif fault == "kind":
        manifest = {"kind": "not-the-synthetic-fixture"}
    (rollback_fixture / "manifest.json").write_text(json.dumps(manifest))
    application = AsyncMock()
    monkeypatch.setattr(probe, "_read_application", application)
    with pytest.raises(probe.InventoryRollbackProbeError):
        await probe.probe_fixture(rollback_fixture)
    application.assert_not_called()


@pytest.mark.parametrize(
    "fault",
    ("cache", "setting_missing", "setting_type", "setting_value", "wallet", "symbol", "orders"),
)
async def test_actual_startup_read_requirements_refuse_incomplete_application_results(
    rollback_fixture: Path, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    """Given a failed application read, when initialized, then success cannot come from SELECT1.

    Given: Real SettingsService initialization and a deliberately wrong single application result,
    When: The bounded compatibility probe checks settings cache and repository reads,
    Then: It refuses that missing behavior while closing actual background resources.
    """
    if fault == "cache":
        monkeypatch.setattr(SettingsService, "get_setting", lambda self, key: None)
    elif fault.startswith("setting"):
        value = (
            None
            if fault == "setting_missing"
            else {"value": 7 if fault == "setting_type" else "{}"}
        )
        monkeypatch.setattr(
            SQLAlchemyRepository, "get_setting_by_key", AsyncMock(return_value=value)
        )
    elif fault == "wallet":
        monkeypatch.setattr(SQLAlchemyRepository, "list_active_wallets", AsyncMock(return_value=[]))
    elif fault == "symbol":
        monkeypatch.setattr(
            SQLAlchemyRepository, "get_symbol_for_instrument", AsyncMock(return_value=None)
        )
    else:
        monkeypatch.setattr(
            SQLAlchemyRepository, "get_active_orders_for_recovery", AsyncMock(return_value=[{}])
        )
    with pytest.raises(probe.InventoryRollbackProbeError):
        await probe.probe_fixture(rollback_fixture)
    assert SettingsService._instance is not None and SettingsService._instance._zmq_context is None


async def test_probe_detects_changed_evidence_after_real_application_reads(
    rollback_fixture: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Given changed post-startup evidence, when checked, then application compatibility is refused.

    Given: Original fixture metadata and actual installed application reads,
    When: The independent post-read fingerprint differs from the original evidence,
    Then: The probe refuses acceptance despite successful settings startup.
    """
    original = probe.fixture_snapshot(rollback_fixture)
    calls = 0

    def changed_snapshot(directory: Path) -> probe.InventoryRollbackSnapshot:
        """Supply a contradictory independently observed post-read fingerprint only on the second read."""
        nonlocal calls
        calls += 1
        return original if calls == 1 else replace(original, evidence_sha256="f" * 64)

    monkeypatch.setattr(probe, "fixture_snapshot", changed_snapshot)
    with pytest.raises(probe.InventoryRollbackProbeError, match="changed_inventory"):
        await probe.probe_fixture(rollback_fixture)


def test_application_loader_refuses_non_sqlalchemy_repository(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Given a wrong installed repository, when loaded, then no substituted reader is accepted.

    Given: The installed repository factory unexpectedly returns an unrelated object,
    When: The probe requires the actual SQLAlchemy-backed application path,
    Then: The explicit type guard refuses before a settings service is initialized.
    """
    module = SimpleNamespace(
        get_repository=lambda url: object(), SQLAlchemyRepository=SQLAlchemyRepository
    )
    monkeypatch.setattr(probe, "import_module", lambda name: module)
    with pytest.raises(probe.InventoryRollbackProbeError, match="sqlalchemy_repository"):
        probe._application("sqlite+aiosqlite:///unused.db")


@pytest.mark.parametrize("mode", ("prepare", "probe"))
def test_cli_success_runs_selected_operation_and_emits_bounded_receipt(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path, mode: str
) -> None:
    """Given either explicit CLI mode, when successful, then only its bounded result is printed.

    Given: One successful operation result and no source-tree override for probe mode,
    When: The CLI dispatches the selected preparation or application read,
    Then: It returns zero with exact safe counts and evidence fingerprint metadata.
    """
    snapshot = probe.InventoryRollbackSnapshot("0066", 2, 1, 1, 1, "a" * 64)
    operation = AsyncMock(return_value=snapshot)
    monkeypatch.delenv("PYTHONPATH", raising=False)
    monkeypatch.setattr(
        probe, "prepare_fixture" if mode == "prepare" else "probe_fixture", operation
    )
    monkeypatch.setattr(sys, "argv", ["verify_inventory_rollback", mode, str(tmp_path)])
    assert probe.main() == 0
    assert json.loads(capsys.readouterr().out) == {"mode": mode, "snapshot": asdict(snapshot)}


def test_cli_probe_refuses_source_override_before_loading_application(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    """Given a source override, when old-image mode starts, then installed-code proof is refused.

    Given: Probe mode inherits a PYTHONPATH that could point at new source,
    When: The CLI checks its installed-code prerequisite,
    Then: It exits two before application loading and names the fixed refusal.
    """
    monkeypatch.setenv("PYTHONPATH", "/synthetic-new-source")
    monkeypatch.setattr(sys, "argv", ["verify_inventory_rollback", "probe", str(tmp_path)])
    assert probe.main() == 2
    assert "unset_PYTHONPATH" in capsys.readouterr().err


def test_main_guard_probes_actual_fixture_with_real_installed_application(
    rollback_fixture: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Given standalone execution, when probing real inventory, then the canonical guard propagates zero.

    Given: A genuinely migrated fixture and the actual application already installed in this test run,
    When: The script executes its normal standalone entry point without PYTHONPATH,
    Then: Actual SettingsService/repository reads pass and process status is zero.
    """
    monkeypatch.delenv("PYTHONPATH", raising=False)
    monkeypatch.setattr(sys, "argv", ["verify_inventory_rollback", "probe", str(rollback_fixture)])
    with pytest.raises(SystemExit) as exit_status:
        runpy.run_path(str(Path(probe.__file__)), run_name="__main__")
    assert exit_status.value.code == 0


def test_fresh_process_prepare_and_installed_probe_work_without_preloaded_test_modules(
    tmp_path: Path,
) -> None:
    """Given separate interpreter processes, when prepared then probed, then no pytest import cache helps.

    Given: A new private namespace and explicit current-source paths only for fixture preparation,
    When: Fresh interpreters prepare genuine evidence and probe with only installed application code,
    Then: Both commands succeed with identical inventory and no source override in the probe process.
    """
    root = Path(probe.__file__).resolve().parents[1]
    directory = tmp_path / "standalone-fixture"
    environment = {
        key: value for key, value in os.environ.items() if key in {"PATH", "HOME", "LANG"}
    }
    preparation_environment = dict(environment)
    preparation_environment["PYTHONPATH"] = os.pathsep.join(
        str(root / relative)
        for relative in (".", "src", "proprietary/src", "integrations/snapper-delegate/src")
    )
    prepared = subprocess.run(
        [sys.executable, "-m", "scripts.verify_inventory_rollback", "prepare", str(directory)],
        cwd=root,
        env=preparation_environment,
        text=True,
        capture_output=True,
        check=True,
        timeout=30,
    )
    probed = subprocess.run(
        [
            sys.executable,
            str(root / "scripts/verify_inventory_rollback.py"),
            "probe",
            str(directory),
        ],
        cwd=directory,
        env=environment,
        text=True,
        capture_output=True,
        check=True,
        timeout=30,
    )
    assert json.loads(prepared.stdout)["snapshot"] == json.loads(probed.stdout)["snapshot"]
    assert directory.stat().st_mode & 0o777 == 0o700
    assert (directory / "inventory.db").stat().st_mode & 0o777 == 0o600
