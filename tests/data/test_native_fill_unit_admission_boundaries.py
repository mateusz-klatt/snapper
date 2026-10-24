"""Actual source-backed targets and historical SQL damage cannot create admitted fill-unit evidence."""

from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Literal
from uuid import UUID

import pytest
from sqlalchemy import delete
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.sql.dml import Update

from snapper.application.trade.account_bound_submission_admission import (
    capture_account_submission_admission,
)
from snapper.application.trade.native_fill_units_v2 import NativeFillUnitCaptureRequestV2
from snapper.application.trade.native_fill_units_v2 import capture_bound_native_fill_units_v2
from snapper.core.execution_unit_admission import ExecutionRouteReference
from snapper.core.execution_unit_snapshot import SubmissionSnapshotTarget
from snapper.core.native_fill_unit_admission import NativeFillUnitAdmissionEvidence
from snapper.core.native_fill_unit_admission import NativeUnitRecordReference
from snapper.data.account_bound_submission_admission_store import (
    get_bound_account_submission_admission,
)
from snapper.data.execution_unit_snapshot_history import get_bound_execution_unit_snapshot
from snapper.data.execution_unit_snapshot_store import SnapshotBindingError
from snapper.data.execution_unit_snapshot_triggers import install_execution_unit_snapshot_triggers
from snapper.data.ledger_triggers import install_execution_immutability_triggers
from snapper.data.models import Execution
from snapper.data.models import ExecutionUnitSnapshot
from snapper.data.models import TradeCommand
from snapper.data.models import VenueEvent
from tests.helpers.native_cash_accounting_v2 import fresh_native_cash_authority_v2
from tests.helpers.native_fill_unit_admission import NativeFillUnitScenario
from tests.helpers.native_fill_unit_admission import capture_native_fill_unit
from tests.helpers.native_fill_unit_admission import native_fill_unit_scenario
from tests.helpers.native_fill_unit_admission import read_native_fill_unit
from tests.helpers.unit_admission import admission_id
from tests.helpers.unit_admission import admission_identity


async def _unit_count(scenario: NativeFillUnitScenario) -> int:
    """Count actual retained unit rows without interpreting a public result as a committed insert."""
    async with scenario.source.repository.session() as session:
        count = await session.scalar(select(func.count()).select_from(ExecutionUnitSnapshot))
        return int(count or 0)


async def _execution_trigger_catalog(
    connection: AsyncConnection,
) -> tuple[tuple[str, str], ...]:
    """Retain every actual execution trigger and its literal DDL before isolated restore damage."""
    result = await connection.execute(
        text(
            "SELECT name, sql FROM sqlite_master WHERE type='trigger' "
            "AND tbl_name='executions' ORDER BY name"
        )
    )
    catalog: list[tuple[str, str]] = []
    for name, ddl in result:
        assert isinstance(name, str) and isinstance(ddl, str)
        catalog.append((name, ddl))
    return tuple(catalog)


async def _assert_execution_mutations_refused(
    connection: AsyncConnection, public_id: UUID, statement: Update
) -> None:
    """Prove both real execution mutation guards inside independently rolled-back savepoints."""
    with pytest.raises(IntegrityError, match="executions is append-only: UPDATE is physically"):
        async with connection.begin_nested():
            await connection.execute(statement)
    with pytest.raises(IntegrityError, match="executions is append-only: DELETE is physically"):
        async with connection.begin_nested():
            await connection.execute(delete(Execution).where(Execution.public_id == public_id))


async def _restore_execution_damage(
    scenario: NativeFillUnitScenario, damage: Literal["execution_id", "fee_decimal"]
) -> None:
    """Stage one isolated restored row and restore original ledger guards before public readers.

    Args:
        scenario: Genuine original source, authority, admission and actual execution target.
        damage: One target operand corrupted without changing its independent original source.
    """
    public_id = scenario.request.execution_public_id
    values: dict[str, object] = (
        {"exec_id": "another-actual-execution"}
        if damage == "execution_id"
        else {"fee_decimal": None}
    )
    statement = update(Execution).where(Execution.public_id == public_id).values(**values)
    operands = select(Execution.exec_id, Execution.fee_decimal).where(
        Execution.public_id == public_id
    )
    async with scenario.source.repository.engine.begin() as connection:
        original = tuple((await connection.execute(operands)).one())
        assert original[0] != "another-actual-execution" and original[1] is not None
        catalog = await _execution_trigger_catalog(connection)
        assert {"executions_reject_update", "executions_reject_delete"} <= {
            name for name, _ddl in catalog
        }
        await _assert_execution_mutations_refused(connection, public_id, statement)
        await connection.execute(text("DROP TRIGGER executions_reject_update"))
        try:
            await connection.execute(statement)
        finally:
            await connection.run_sync(install_execution_immutability_triggers)
        assert await _execution_trigger_catalog(connection) == catalog
        await _assert_execution_mutations_refused(connection, public_id, statement)
        actual = tuple((await connection.execute(operands)).one())
        expected = (
            ("another-actual-execution", original[1])
            if damage == "execution_id"
            else (original[0], None)
        )
        assert actual == expected


async def test_actual_fill_unit_capture_requires_an_existing_caller_transaction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A complete genuine request does not let the producer invent its caller's transaction.

    Given: Original signed source, actual targets and committed account admission,
    When: The public producer receives a fresh session before any transaction begins,
    Then: It refuses immediately and retains only the original command unit.

    Args:
        tmp_path: Isolated actual SQLite namespace.
        monkeypatch: Original SDK and local HTTP fixture lifetime.
    """
    async with native_fill_unit_scenario(tmp_path, monkeypatch) as scenario:
        async with scenario.source.repository.session() as session:
            assert not session.in_transaction()
            with pytest.raises(RuntimeError, match="caller wallet-fenced transaction"):
                await capture_bound_native_fill_units_v2(
                    session, scenario.request, scenario.provenance
                )
            assert not session.in_transaction()
        assert await _unit_count(scenario) == 1


@pytest.mark.parametrize("field", ["execution_public_id", "venue_event_public_id"])
async def test_absent_actual_target_reference_cannot_create_fill_units(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: Literal["execution_public_id", "venue_event_public_id"],
) -> None:
    """Each actual target must exist before the source-backed producer can create a unit.

    Given: Genuine source and admission with independently retained Execution and VenueEvent,
    When: One public target reference names a valid UUID absent from the actual SQL tables,
    Then: The real producer refuses the missing target without adding a fill unit.

    Args:
        tmp_path: Isolated actual SQLite namespace.
        monkeypatch: Original SDK and local HTTP fixture lifetime.
        field: Actual target identity replaced by a valid absent public reference.
    """
    async with native_fill_unit_scenario(tmp_path, monkeypatch) as scenario:
        missing = admission_id(7998)
        request = NativeFillUnitCaptureRequestV2.model_validate(
            {**scenario.request.model_dump(mode="python"), field: missing}
        )
        async with scenario.source.repository.session() as session:
            for model in (Execution, VenueEvent):
                actual = await session.scalar(select(model.id).where(model.public_id == missing))
                assert actual is None
        with pytest.raises(SnapshotBindingError, match="native_fill_actual_unit_targets_missing"):
            await capture_native_fill_unit(scenario, request)
        assert await _unit_count(scenario) == 1


@pytest.mark.parametrize(
    "damage,reason",
    [
        ("execution_id", "native_fill_actual_target_source_identity_mismatch"),
        ("client_order_id", "native_fill_actual_client_order_identity_mismatch"),
        ("fee_decimal", "native_fill_actual_raw_decimal_operands_missing"),
    ],
)
async def test_actual_target_sql_damage_is_refused_against_original_signed_fill(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    damage: Literal["execution_id", "client_order_id", "fee_decimal"],
    reason: str,
) -> None:
    """Actual durable target corruption cannot override independently authenticated fill bytes.

    Given: Real original fill, retained source ownership and actual target rows before unit capture,
    When: An isolated restore changes one target operand and original execution guards are restored,
    Then: The public producer reaches the corresponding actual-source refusal and inserts no fill unit.

    Args:
        tmp_path: Isolated actual SQLite namespace.
        monkeypatch: Original SDK and local HTTP fixture lifetime.
        damage: Durable target operand corrupted without replacing its original source.
        reason: Exact expected real target-binding refusal.
    """
    async with native_fill_unit_scenario(tmp_path, monkeypatch) as scenario:
        if damage == "client_order_id":
            async with scenario.source.repository.session() as session:
                await session.execute(
                    update(VenueEvent)
                    .where(VenueEvent.public_id == scenario.request.venue_event_public_id)
                    .values(venue_client_id="another-actual-client-order")
                )
                await session.commit()
        else:
            await _restore_execution_damage(scenario, damage)
        with pytest.raises(SnapshotBindingError, match=reason):
            await capture_native_fill_unit(scenario)
        assert await _unit_count(scenario) == 1


async def test_original_command_must_exist_at_the_actual_admission_clock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A command row outside its original admission interval cannot supply historical intent.

    Given: Genuine source, original command and separately retained admission,
    When: Isolated SQL closes the command exactly at that admission's interpretation clock,
    Then: The public producer refuses the missing historical command before creating fill units.

    Args:
        tmp_path: Isolated actual SQLite namespace.
        monkeypatch: Original SDK and local HTTP fixture lifetime.
    """
    async with native_fill_unit_scenario(tmp_path, monkeypatch) as scenario:
        units = scenario.admission.payload.units
        async with scenario.source.repository.session() as session:
            command = (
                await session.execute(
                    select(TradeCommand).where(
                        TradeCommand.public_id == units.request_material.command_public_id
                    )
                )
            ).scalar_one()
            assert command.timestamp < units.interpreted_at < command.known_to
            await session.execute(
                update(TradeCommand)
                .where(TradeCommand.id == command.id)
                .values(known_to=units.interpreted_at)
            )
            await session.commit()
        with pytest.raises(
            SnapshotBindingError, match="native_fill_original_admitted_command_missing"
        ):
            await capture_native_fill_unit(scenario)
        assert await _unit_count(scenario) == 1


async def test_restored_missing_original_command_unit_blocks_fill_capture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A real admission cannot substitute for an original unit row lost during restore.

    Given: Actual signed fill and admitted command with one genuine immutable command unit,
    When: Normal deletion is refused and an isolated restore then removes that unit,
    Then: Real guards are restored before the public producer refuses the missing original units.

    Args:
        tmp_path: Isolated actual SQLite namespace.
        monkeypatch: Original SDK and local HTTP fixture lifetime.
    """
    async with native_fill_unit_scenario(tmp_path, monkeypatch) as scenario:
        identity = scenario.admission.payload.units.command_snapshot_public_id
        statement = delete(ExecutionUnitSnapshot).where(ExecutionUnitSnapshot.public_id == identity)
        async with scenario.source.repository.engine.begin() as connection:
            with pytest.raises(IntegrityError, match="append-only: DELETE forbidden"):
                async with connection.begin_nested():
                    await connection.execute(statement)
            await connection.execute(text("DROP TRIGGER execution_unit_snapshots_reject_delete"))
            try:
                await connection.execute(statement)
            finally:
                await connection.run_sync(install_execution_unit_snapshot_triggers)
            names = set(
                (
                    await connection.execute(
                        text(
                            "SELECT name FROM sqlite_master WHERE type='trigger' "
                            "AND tbl_name='execution_unit_snapshots'"
                        )
                    )
                ).scalars()
            )
            assert names == {
                "execution_unit_snapshots_reject_update",
                "execution_unit_snapshots_reject_delete",
            }
        assert await _unit_count(scenario) == 0
        with pytest.raises(
            SnapshotBindingError, match="native_fill_admission_original_units_missing"
        ):
            await capture_native_fill_unit(scenario)
        assert await _unit_count(scenario) == 0


async def test_genuine_other_command_unit_cannot_be_read_as_a_native_fill_unit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A fully verified later command unit is still the wrong target kind for a fill envelope.

    Given: Genuine original admission and a later real unit for the fixture's other original command,
    When: A chronological public envelope selects that actual submission unit,
    Then: Full unit history succeeds before the native reader refuses its non-fill target.

    Args:
        tmp_path: Isolated actual SQLite namespace.
        monkeypatch: Original SDK and local HTTP fixture lifetime.
    """
    async with native_fill_unit_scenario(tmp_path, monkeypatch) as scenario:
        reference, authority = await fresh_native_cash_authority_v2(scenario.source, 7810)
        route = ExecutionRouteReference(
            authority_public_id=reference.public_id,
            authority_content_digest=reference.content_digest,
            scope=authority.scope,
            route_revision=authority.route_revision,
        )
        result = await capture_account_submission_admission(
            scenario.source.repository,
            scenario.account.native_order,
            route,
            admission_identity(7811),
            scenario.source.policy,
        )
        async with scenario.source.repository.session() as session:
            other = await get_bound_account_submission_admission(session, result.public_id)
            public_id = other.payload.units.command_snapshot_public_id
            actual = await get_bound_execution_unit_snapshot(session, public_id)
            assert isinstance(actual.target, SubmissionSnapshotTarget)
            row = (
                await session.execute(
                    select(ExecutionUnitSnapshot).where(
                        ExecutionUnitSnapshot.public_id == public_id
                    )
                )
            ).scalar_one()
            unit = NativeUnitRecordReference(
                public_id=row.public_id,
                content_digest=row.content_digest,
                recorded_at=row.timestamp,
            )
        original = scenario.request.submission_admission
        assert original.recorded_at < unit.recorded_at
        evidence = NativeFillUnitAdmissionEvidence(
            submission_admission=original, unit_snapshot=unit
        )
        with pytest.raises(SnapshotBindingError, match="native_fill_unit_fill_target_required"):
            await read_native_fill_unit(scenario, evidence, datetime.now(UTC))
        assert await _unit_count(scenario) == 2
