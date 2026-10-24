"""Actual original unit rows remain fully rebound through corrupted-restore and exact-retry paths."""

from datetime import timedelta
from pathlib import Path
from typing import Literal

import pytest
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError

from snapper.data.execution_unit_snapshot_store import SnapshotBindingError
from snapper.data.execution_unit_snapshot_triggers import install_execution_unit_snapshot_triggers
from snapper.data.models import ExecutionUnitSnapshot
from tests.helpers.native_fill_unit_admission import NativeFillUnitScenario
from tests.helpers.native_fill_unit_admission import capture_native_fill_unit
from tests.helpers.native_fill_unit_admission import native_fill_unit_scenario
from tests.helpers.native_fill_unit_admission import read_native_fill_unit

type UnitDamage = Literal["known_to", "timestamp", "wallet_public_id", "spec_version"]


async def _restore_unit(
    scenario: NativeFillUnitScenario, public_id: str, field: UnitDamage
) -> None:
    """Alter only the genuine retained unit row and reinstall real guards before public binding."""
    repository = scenario.source.repository
    async with repository.session() as session:
        row = (
            await session.execute(
                select(ExecutionUnitSnapshot).where(ExecutionUnitSnapshot.public_id == public_id)
            )
        ).scalar_one()
        values: dict[str, object] = {
            "known_to": scenario.provenance.timestamp + timedelta(seconds=1),
            "timestamp": row.timestamp - timedelta(microseconds=1),
            "wallet_public_id": scenario.account.legacy.authority.scope.wallet_public_id,
            "spec_version": "damaged-original-unit-projection",
        }
    statement = (
        update(ExecutionUnitSnapshot)
        .where(ExecutionUnitSnapshot.public_id == public_id)
        .values(**{field: values[field]})
    )
    async with repository.engine.begin() as connection:
        assert await connection.scalar(text("PRAGMA ignore_check_constraints")) == 0
        with pytest.raises(IntegrityError, match="append-only: UPDATE forbidden"):
            async with connection.begin_nested():
                await connection.execute(statement)
        await connection.execute(text("DROP TRIGGER execution_unit_snapshots_reject_update"))
        try:
            if field == "known_to":
                with pytest.raises(IntegrityError, match="ck_unit_snapshot_known_to_max"):
                    async with connection.begin_nested():
                        await connection.execute(statement)
                await connection.execute(text("PRAGMA ignore_check_constraints=ON"))
            await connection.execute(statement)
            await connection.execute(text("PRAGMA ignore_check_constraints=OFF"))
            assert await connection.scalar(text("PRAGMA ignore_check_constraints")) == 0
            if field == "known_to":
                with pytest.raises(IntegrityError, match="ck_unit_snapshot_known_to_max"):
                    async with connection.begin_nested():
                        await connection.execute(statement)
        finally:
            try:
                await connection.execute(text("PRAGMA ignore_check_constraints=OFF"))
            finally:
                await connection.run_sync(install_execution_unit_snapshot_triggers)
        assert await connection.scalar(text("PRAGMA ignore_check_constraints")) == 0
        with pytest.raises(IntegrityError, match="append-only: UPDATE forbidden"):
            async with connection.begin_nested():
                await connection.execute(statement)


@pytest.mark.parametrize(
    "target,field",
    [
        ("command", "known_to"),
        ("command", "timestamp"),
        ("fill", "known_to"),
        ("fill", "timestamp"),
        ("fill", "wallet_public_id"),
        ("fill", "spec_version"),
    ],
)
async def test_actual_historical_unit_damage_blocks_both_capture_retry_and_envelope_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    target: Literal["command", "fill"],
    field: UnitDamage,
) -> None:
    """A same-content retry cannot evade full original row projection and chronology validation.

    Given: Actual captured native source, admission, original command unit and fill unit,
    When: A local restore damages one original unit's indexed projection or immutable recording clock,
    Then: Both the actual capture retry and historical envelope reader reject the damaged graph.

    Args:
        tmp_path: Isolated actual SQLite namespace.
        monkeypatch: Original actual signing and source fixture lifetime.
        target: Original command unit or actual returned fill-unit row to corrupt.
        field: Actual indexed or temporal field changed while canonical payload remains intact.
    """
    async with native_fill_unit_scenario(tmp_path, monkeypatch) as scenario:
        captured = await capture_native_fill_unit(scenario)
        public_id = (
            scenario.admission.payload.units.command_snapshot_public_id
            if target == "command"
            else captured.result.public_id
        )
        await _restore_unit(scenario, public_id, field)
        with pytest.raises(SnapshotBindingError, match="historical"):
            await capture_native_fill_unit(scenario)
        with pytest.raises(SnapshotBindingError, match="historical"):
            await read_native_fill_unit(scenario, captured.evidence)
