"""Tests for paired-execution repository data access.

The repository layer owns the SCD2 close-and-insert semantics for the
paired-execution guard. These tests exercise inserts, active reads,
compare-and-swap transitions, and halt clearing against an in-memory
aiosqlite database.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.exc import IntegrityError

from snapper.core.types import PairedExecutionGroupStatusEnum
from snapper.core.types import PairedExecutionLegStatusEnum
from snapper.core.types import PairedExecutionPolicyEnum
from snapper.core.types import PairedFillProjection
from snapper.core.types import PairedGroupTerminalizeOutcome
from snapper.core.types import TradeCommandStatusEnum
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import PairedExecutionGroup
from snapper.data.models import PairedExecutionHalt
from snapper.data.models import PairedExecutionLeg
from snapper.data.models import TradeCommand
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import where_active
from snapper.data.repository_types import PairedExecutionGroupInsertRow
from snapper.data.repository_types import PairedExecutionHaltInsertRow
from snapper.data.repository_types import PairedExecutionLegInsertRow

pytestmark = pytest.mark.timeout(60)

_T0 = datetime(2026, 6, 8, 12, 0, tzinfo=UTC)
_T1 = _T0 + timedelta(seconds=1)
_T2 = _T0 + timedelta(seconds=2)
_T3 = _T0 + timedelta(seconds=3)
_WALLET_ID = "00000000-0000-0000-0000-000000000001"
_OPERATOR_ID = "00000000-0000-0000-0000-000000000002"
_SESSION_ID = "00000000-0000-0000-0000-000000000003"
_NEXT_SESSION_ID = "00000000-0000-0000-0000-000000000004"
_STRATEGY_ID = "pairs-alpha"
_GROUP_KEY = "kraken:BTC-USD:live|kraken:ETH-USD:live"
_OTHER_GROUP_KEY = "kraken:ADA-USD:live|kraken:SOL-USD:live"


@pytest.fixture
async def _repo() -> AsyncIterator[SQLAlchemyRepository]:
    """Provide a fresh in-memory repository."""
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    await repo.create_all()
    try:
        yield repo
    finally:
        await repo.engine.dispose()


def _pid(slot: int) -> str:
    """Return a deterministic UUID-shaped public id."""
    return f"00000000-0000-0000-0000-{slot:012d}"


def _group_insert_row(
    *,
    public_id: str | None = None,
    wallet_public_id: str = _WALLET_ID,
    operator_public_id: str | None = _OPERATOR_ID,
    strategy_id: str = _STRATEGY_ID,
    policy: str = PairedExecutionPolicyEnum.SIMULTANEOUS.value,
    expected_leg_count: int = 2,
    group_key: str = _GROUP_KEY,
    status: str = PairedExecutionGroupStatusEnum.ASSEMBLING.value,
    assembly_deadline: datetime = _T1,
    fill_deadline: datetime = _T2,
    failure_reason: str | None = None,
    halted_at: datetime | None = None,
    created_at: datetime = _T0,
    sequence_id: int = 1,
    timestamp: datetime = _T0,
) -> PairedExecutionGroupInsertRow:
    """Build a paired-execution group insert row."""
    row = PairedExecutionGroupInsertRow(
        wallet_public_id=wallet_public_id,
        operator_public_id=operator_public_id,
        strategy_id=strategy_id,
        policy=policy,
        expected_leg_count=expected_leg_count,
        group_key=group_key,
        status=status,
        assembly_deadline=assembly_deadline,
        fill_deadline=fill_deadline,
        failure_reason=failure_reason,
        halted_at=halted_at,
        created_at=created_at,
        session_id=_SESSION_ID,
        sequence_id=sequence_id,
        timestamp=timestamp,
    )
    if public_id is not None:
        row["public_id"] = public_id
    return row


def _leg_insert_row(
    *,
    public_id: str | None = None,
    group_public_id: str = "00000000-0000-0000-0000-000000000100",
    leg_index: int = 0,
    exchange: str = "kraken",
    mode: str = "live",
    instrument: str = "BTC-USD",
    shard_key: str = "kraken:BTC-USD:live",
    side: str = "buy",
    target_qty: float = 1.5,
    signal_public_id: str = "00000000-0000-0000-0000-000000000900",
    command_public_id: str | None = None,
    client_order_id: str | None = None,
    exchange_order_id: str | None = None,
    status: str = PairedExecutionLegStatusEnum.PENDING.value,
    filled_signed_qty: float = 0.0,
    compensated_signed_qty: float = 0.0,
    compensation_seq: int = 0,
    last_venue_event_id: int | None = None,
    wallet_public_id: str = _WALLET_ID,
    operator_public_id: str | None = _OPERATOR_ID,
    created_at: datetime = _T0,
    sequence_id: int = 1,
    timestamp: datetime = _T0,
) -> PairedExecutionLegInsertRow:
    """Build a paired-execution leg insert row."""
    row = PairedExecutionLegInsertRow(
        group_public_id=group_public_id,
        leg_index=leg_index,
        exchange=exchange,
        mode=mode,
        instrument=instrument,
        shard_key=shard_key,
        side=side,
        target_qty=target_qty,
        signal_public_id=signal_public_id,
        command_public_id=command_public_id,
        client_order_id=client_order_id,
        exchange_order_id=exchange_order_id,
        status=status,
        filled_signed_qty=filled_signed_qty,
        compensated_signed_qty=compensated_signed_qty,
        compensation_seq=compensation_seq,
        last_venue_event_id=last_venue_event_id,
        wallet_public_id=wallet_public_id,
        operator_public_id=operator_public_id,
        created_at=created_at,
        session_id=_SESSION_ID,
        sequence_id=sequence_id,
        timestamp=timestamp,
    )
    if public_id is not None:
        row["public_id"] = public_id
    return row


def _halt_insert_row(
    *,
    public_id: str | None = None,
    wallet_public_id: str = _WALLET_ID,
    operator_public_id: str | None = _OPERATOR_ID,
    strategy_id: str = _STRATEGY_ID,
    mode: str = "live",
    group_key: str = _GROUP_KEY,
    group_public_id: str = "00000000-0000-0000-0000-000000000100",
    reason: str = "paired group broken",
    created_at: datetime = _T0,
    sequence_id: int = 1,
    timestamp: datetime = _T0,
) -> PairedExecutionHaltInsertRow:
    """Build a paired-execution halt insert row."""
    row = PairedExecutionHaltInsertRow(
        wallet_public_id=wallet_public_id,
        operator_public_id=operator_public_id,
        strategy_id=strategy_id,
        mode=mode,
        group_key=group_key,
        group_public_id=group_public_id,
        reason=reason,
        created_at=created_at,
        session_id=_SESSION_ID,
        sequence_id=sequence_id,
        timestamp=timestamp,
    )
    if public_id is not None:
        row["public_id"] = public_id
    return row


async def _group_versions(
    repo: SQLAlchemyRepository,
    public_id: str,
) -> list[PairedExecutionGroup]:
    """Return all SCD2 versions for one paired-execution group."""
    async with repo.session() as session:
        result = await session.execute(
            select(PairedExecutionGroup)
            .where(PairedExecutionGroup.public_id == public_id)
            .order_by(PairedExecutionGroup.id)
        )
        return list(result.scalars().all())


async def _leg_versions(
    repo: SQLAlchemyRepository,
    public_id: str,
) -> list[PairedExecutionLeg]:
    """Return all SCD2 versions for one paired-execution leg."""
    async with repo.session() as session:
        result = await session.execute(
            select(PairedExecutionLeg)
            .where(PairedExecutionLeg.public_id == public_id)
            .order_by(PairedExecutionLeg.id)
        )
        return list(result.scalars().all())


async def _halt_versions(
    repo: SQLAlchemyRepository,
    public_id: str,
) -> list[PairedExecutionHalt]:
    """Return all SCD2 versions for one paired-execution halt."""
    async with repo.session() as session:
        result = await session.execute(
            select(PairedExecutionHalt)
            .where(PairedExecutionHalt.public_id == public_id)
            .order_by(PairedExecutionHalt.id)
        )
        return list(result.scalars().all())


async def _active_group_versions(
    repo: SQLAlchemyRepository,
    public_id: str,
    as_of: datetime,
) -> list[PairedExecutionGroup]:
    """Return active SCD2 group versions for one public id at a point in time."""
    async with repo.session() as session:
        result = await session.execute(
            select(PairedExecutionGroup).where(
                PairedExecutionGroup.public_id == public_id,
                *where_active(PairedExecutionGroup, as_of),
            )
        )
        return list(result.scalars().all())


async def _active_leg_versions(
    repo: SQLAlchemyRepository,
    public_id: str,
    as_of: datetime,
) -> list[PairedExecutionLeg]:
    """Return active SCD2 leg versions for one public id at a point in time."""
    async with repo.session() as session:
        result = await session.execute(
            select(PairedExecutionLeg).where(
                PairedExecutionLeg.public_id == public_id,
                *where_active(PairedExecutionLeg, as_of),
            )
        )
        return list(result.scalars().all())


async def _active_halts(
    repo: SQLAlchemyRepository,
    as_of: datetime,
) -> list[PairedExecutionHalt]:
    """Return all active SCD2 halt versions at a point in time."""
    async with repo.session() as session:
        result = await session.execute(
            select(PairedExecutionHalt).where(*where_active(PairedExecutionHalt, as_of))
        )
        return list(result.scalars().all())


@pytest.mark.asyncio
async def test_insert_and_read_methods_round_trip_filter_and_order(
    _repo: SQLAlchemyRepository,
) -> None:
    """Given inserted guard rows, when read APIs run, then they filter and order them."""
    group_a = _pid(100)
    group_b = _pid(200)
    group_c = _pid(300)
    leg_a_1 = _pid(401)
    leg_a_0 = _pid(400)
    leg_b_0 = _pid(500)
    halt_id = _pid(600)
    returned_group = await _repo.insert_paired_execution_group(
        _group_insert_row(
            public_id=group_b,
            status=PairedExecutionGroupStatusEnum.ARMED.value,
            created_at=_T1,
        )
    )
    await _repo.insert_paired_execution_group(
        _group_insert_row(
            public_id=group_a,
            status=PairedExecutionGroupStatusEnum.ARMED.value,
            created_at=_T0,
        )
    )
    await _repo.insert_paired_execution_group(
        _group_insert_row(
            public_id=group_c,
            group_key=_OTHER_GROUP_KEY,
            status=PairedExecutionGroupStatusEnum.COMPLETED.value,
            created_at=_T2,
        )
    )
    returned_leg = await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=leg_a_1,
            group_public_id=group_a,
            leg_index=1,
            instrument="ETH-USD",
            shard_key="kraken:ETH-USD:live",
            side="sell",
            target_qty=2.5,
            signal_public_id=_pid(901),
        )
    )
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=leg_a_0,
            group_public_id=group_a,
            leg_index=0,
            instrument="BTC-USD",
            shard_key="kraken:BTC-USD:live",
            signal_public_id=_pid(902),
        )
    )
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=leg_b_0,
            group_public_id=group_b,
            leg_index=0,
            instrument="BTC-USD",
            shard_key="kraken:BTC-USD:live",
            signal_public_id=_pid(903),
        )
    )
    returned_halt = await _repo.insert_paired_execution_halt(
        _halt_insert_row(public_id=halt_id, group_public_id=group_a)
    )

    group = await _repo.get_paired_execution_group(group_a, _T2)
    absent_group = await _repo.get_paired_execution_group(_pid(999), _T2)
    group_legs = await _repo.get_paired_execution_legs(group_a, _T2)
    absent_group_legs = await _repo.get_paired_execution_legs(_pid(999), _T2)
    shard_legs = await _repo.list_active_paired_execution_legs_for_shards(
        ["kraken:BTC-USD:live"],
        _T2,
    )
    empty_shard_legs = await _repo.list_active_paired_execution_legs_for_shards([], _T2)
    armed_groups = await _repo.list_active_paired_execution_groups(
        [PairedExecutionGroupStatusEnum.ARMED.value],
        _T2,
    )
    broken_groups = await _repo.list_active_paired_execution_groups(
        [PairedExecutionGroupStatusEnum.BROKEN.value],
        _T2,
    )
    empty_status_groups = await _repo.list_active_paired_execution_groups([], _T2)
    halt = await _repo.get_active_paired_execution_halt(
        _WALLET_ID,
        _STRATEGY_ID,
        _GROUP_KEY,
    )
    absent_halt = await _repo.get_active_paired_execution_halt(
        _WALLET_ID,
        _STRATEGY_ID,
        _OTHER_GROUP_KEY,
    )

    assert returned_group == group_b
    assert returned_leg == leg_a_1
    assert returned_halt == halt_id
    assert group is not None
    assert group["public_id"] == group_a
    assert group["known_to"] == KNOWN_TO_MAX
    assert group["status"] == PairedExecutionGroupStatusEnum.ARMED.value
    assert absent_group is None
    assert [leg["public_id"] for leg in group_legs] == [leg_a_0, leg_a_1]
    assert absent_group_legs == []
    assert [leg["public_id"] for leg in shard_legs] == [leg_a_0, leg_b_0]
    assert empty_shard_legs == []
    assert [group_row["public_id"] for group_row in armed_groups] == [group_a, group_b]
    assert broken_groups == []
    assert empty_status_groups == []
    assert halt is not None
    assert halt["public_id"] == halt_id
    assert halt["reason"] == "paired group broken"
    assert absent_halt is None


@pytest.mark.asyncio
async def test_group_cas_returns_false_when_row_missing(_repo: SQLAlchemyRepository) -> None:
    """Given no matching group, when CAS runs, then it returns False."""
    applied = await _repo.cas_paired_execution_group_status(
        _pid(101),
        PairedExecutionGroupStatusEnum.ASSEMBLING.value,
        PairedExecutionGroupStatusEnum.ARMED.value,
        _T1,
        _NEXT_SESSION_ID,
        2,
    )

    assert applied is False
    assert await _group_versions(_repo, _pid(101)) == []


@pytest.mark.asyncio
async def test_group_cas_returns_false_when_status_mismatches(
    _repo: SQLAlchemyRepository,
) -> None:
    """Given an active group in a different state, when CAS runs, then it is a no-op."""
    public_id = _pid(102)
    await _repo.insert_paired_execution_group(
        _group_insert_row(
            public_id=public_id,
            status=PairedExecutionGroupStatusEnum.ASSEMBLING.value,
        )
    )

    applied = await _repo.cas_paired_execution_group_status(
        public_id,
        PairedExecutionGroupStatusEnum.ARMED.value,
        PairedExecutionGroupStatusEnum.BROKEN.value,
        _T1,
        _NEXT_SESSION_ID,
        2,
    )
    versions = await _group_versions(_repo, public_id)

    assert applied is False
    assert len(versions) == 1
    assert versions[0].status == PairedExecutionGroupStatusEnum.ASSEMBLING.value
    assert versions[0].known_to == KNOWN_TO_MAX


@pytest.mark.asyncio
async def test_group_cas_success_applies_updates_and_closes_old_version(
    _repo: SQLAlchemyRepository,
) -> None:
    """Given an expected active group, when CAS has updates, then SCD2 advances."""
    public_id = _pid(103)
    await _repo.insert_paired_execution_group(_group_insert_row(public_id=public_id))

    applied = await _repo.cas_paired_execution_group_status(
        public_id,
        PairedExecutionGroupStatusEnum.ASSEMBLING.value,
        PairedExecutionGroupStatusEnum.BROKEN.value,
        _T1,
        _NEXT_SESSION_ID,
        2,
        {"failure_reason": "leg rejected", "halted_at": _T1},
    )
    versions = await _group_versions(_repo, public_id)
    active_versions = await _active_group_versions(_repo, public_id, _T2)
    projected = await _repo.get_paired_execution_group(public_id, _T2)

    assert applied is True
    assert len(versions) == 2
    assert versions[0].public_id == public_id
    assert versions[0].known_to == _T1
    assert versions[1].public_id == public_id
    assert versions[1].known_to == KNOWN_TO_MAX
    assert len(active_versions) == 1
    assert active_versions[0].id == versions[1].id
    assert projected is not None
    assert projected["status"] == PairedExecutionGroupStatusEnum.BROKEN.value
    assert projected["failure_reason"] == "leg rejected"
    assert projected["halted_at"] == _T1
    assert projected["session_id"] == _NEXT_SESSION_ID
    assert projected["sequence_id"] == 2
    assert projected["timestamp"] == _T1


@pytest.mark.asyncio
async def test_group_cas_success_without_updates_carries_fields(
    _repo: SQLAlchemyRepository,
) -> None:
    """Given an expected group, when CAS has no updates, then mutable fields carry forward."""
    public_id = _pid(104)
    await _repo.insert_paired_execution_group(
        _group_insert_row(
            public_id=public_id,
            status=PairedExecutionGroupStatusEnum.BROKEN.value,
            failure_reason="existing failure",
            halted_at=_T1,
        )
    )

    applied = await _repo.cas_paired_execution_group_status(
        public_id,
        PairedExecutionGroupStatusEnum.BROKEN.value,
        PairedExecutionGroupStatusEnum.COMPENSATING.value,
        _T2,
        _NEXT_SESSION_ID,
        3,
        None,
    )
    projected = await _repo.get_paired_execution_group(public_id, _T3)

    assert applied is True
    assert projected is not None
    assert projected["status"] == PairedExecutionGroupStatusEnum.COMPENSATING.value
    assert projected["failure_reason"] == "existing failure"
    assert projected["halted_at"] == _T1


@pytest.mark.asyncio
async def test_leg_cas_returns_false_when_row_missing(_repo: SQLAlchemyRepository) -> None:
    """Given no matching leg, when CAS runs, then it returns False."""
    applied = await _repo.cas_paired_execution_leg_status(
        _pid(201),
        PairedExecutionLegStatusEnum.PENDING.value,
        PairedExecutionLegStatusEnum.ARMED.value,
        _T1,
        _NEXT_SESSION_ID,
        2,
    )

    assert applied is False
    assert await _leg_versions(_repo, _pid(201)) == []


@pytest.mark.asyncio
async def test_leg_cas_returns_false_when_status_mismatches(
    _repo: SQLAlchemyRepository,
) -> None:
    """Given an active leg in a different state, when CAS runs, then it is a no-op."""
    public_id = _pid(202)
    await _repo.insert_paired_execution_leg(_leg_insert_row(public_id=public_id))

    applied = await _repo.cas_paired_execution_leg_status(
        public_id,
        PairedExecutionLegStatusEnum.ARMED.value,
        PairedExecutionLegStatusEnum.WORKING.value,
        _T1,
        _NEXT_SESSION_ID,
        2,
    )
    versions = await _leg_versions(_repo, public_id)

    assert applied is False
    assert len(versions) == 1
    assert versions[0].status == PairedExecutionLegStatusEnum.PENDING.value
    assert versions[0].known_to == KNOWN_TO_MAX


@pytest.mark.asyncio
async def test_leg_cas_success_applies_updates_and_closes_old_version(
    _repo: SQLAlchemyRepository,
) -> None:
    """Given an expected active leg, when CAS has updates, then SCD2 advances."""
    public_id = _pid(203)
    await _repo.insert_paired_execution_leg(_leg_insert_row(public_id=public_id))

    applied = await _repo.cas_paired_execution_leg_status(
        public_id,
        PairedExecutionLegStatusEnum.PENDING.value,
        PairedExecutionLegStatusEnum.WORKING.value,
        _T1,
        _NEXT_SESSION_ID,
        2,
        {
            "command_public_id": _pid(700),
            "client_order_id": "client-1",
            "exchange_order_id": "exchange-1",
            "filled_signed_qty": 1.25,
            "compensated_signed_qty": 0.25,
            "compensation_seq": 1,
            "last_venue_event_id": 42,
        },
    )
    versions = await _leg_versions(_repo, public_id)
    active_versions = await _active_leg_versions(_repo, public_id, _T2)
    projected = await _repo.get_paired_execution_legs(
        "00000000-0000-0000-0000-000000000100",
        _T2,
    )

    assert applied is True
    assert len(versions) == 2
    assert versions[0].public_id == public_id
    assert versions[0].known_to == _T1
    assert versions[1].public_id == public_id
    assert versions[1].known_to == KNOWN_TO_MAX
    assert len(active_versions) == 1
    assert active_versions[0].id == versions[1].id
    assert len(projected) == 1
    assert projected[0]["status"] == PairedExecutionLegStatusEnum.WORKING.value
    assert projected[0]["command_public_id"] == _pid(700)
    assert projected[0]["client_order_id"] == "client-1"
    assert projected[0]["exchange_order_id"] == "exchange-1"
    assert projected[0]["filled_signed_qty"] == 1.25
    assert projected[0]["compensated_signed_qty"] == 0.25
    assert projected[0]["compensation_seq"] == 1
    assert projected[0]["last_venue_event_id"] == 42
    assert projected[0]["session_id"] == _NEXT_SESSION_ID
    assert projected[0]["sequence_id"] == 2
    assert projected[0]["timestamp"] == _T1


@pytest.mark.asyncio
async def test_leg_cas_success_without_updates_carries_fields(
    _repo: SQLAlchemyRepository,
) -> None:
    """Given an expected leg, when CAS has no updates, then mutable fields carry forward."""
    public_id = _pid(204)
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=public_id,
            status=PairedExecutionLegStatusEnum.WORKING.value,
            command_public_id=_pid(701),
            client_order_id="client-existing",
            exchange_order_id="exchange-existing",
            filled_signed_qty=2.0,
            compensated_signed_qty=0.5,
            compensation_seq=3,
            last_venue_event_id=77,
        )
    )

    applied = await _repo.cas_paired_execution_leg_status(
        public_id,
        PairedExecutionLegStatusEnum.WORKING.value,
        PairedExecutionLegStatusEnum.PARTIALLY_FILLED.value,
        _T2,
        _NEXT_SESSION_ID,
        3,
        None,
    )
    projected = await _repo.get_paired_execution_legs(
        "00000000-0000-0000-0000-000000000100",
        _T3,
    )

    assert applied is True
    assert len(projected) == 1
    assert projected[0]["status"] == PairedExecutionLegStatusEnum.PARTIALLY_FILLED.value
    assert projected[0]["command_public_id"] == _pid(701)
    assert projected[0]["client_order_id"] == "client-existing"
    assert projected[0]["exchange_order_id"] == "exchange-existing"
    assert projected[0]["filled_signed_qty"] == 2.0
    assert projected[0]["compensated_signed_qty"] == 0.5
    assert projected[0]["compensation_seq"] == 3
    assert projected[0]["last_venue_event_id"] == 77


@pytest.mark.asyncio
async def test_clear_halt_closes_present_row_and_absent_is_false(
    _repo: SQLAlchemyRepository,
) -> None:
    """Given active and missing halts, when clear runs, then only the active halt closes."""
    public_id = _pid(301)
    await _repo.insert_paired_execution_halt(_halt_insert_row(public_id=public_id))

    absent_applied = await _repo.clear_paired_execution_halt(_pid(302), _T1)
    present_applied = await _repo.clear_paired_execution_halt(public_id, _T1)
    active_halt = await _repo.get_active_paired_execution_halt(
        _WALLET_ID,
        _STRATEGY_ID,
        _GROUP_KEY,
    )
    versions = await _halt_versions(_repo, public_id)

    assert absent_applied is False
    assert present_applied is True
    assert active_halt is None
    assert len(versions) == 1
    assert versions[0].known_to == _T1


@pytest.mark.asyncio
async def test_group_cas_rejects_unknown_update_fields(
    _repo: SQLAlchemyRepository,
) -> None:
    """Given a forbidden update key, when group CAS runs, then it raises before any write.

    A dynamic caller passing a typo or a forbidden SCD2 column (here
    ``status``) must be rejected; the active version stays untouched.
    """
    public_id = _pid(206)
    await _repo.insert_paired_execution_group(_group_insert_row(public_id=public_id))
    bad_update: dict[str, object] = {"status": "armed"}

    with pytest.raises(ValueError, match="unknown paired-execution group update fields"):
        await _repo.cas_paired_execution_group_status(
            public_id,
            PairedExecutionGroupStatusEnum.ASSEMBLING.value,
            PairedExecutionGroupStatusEnum.ARMED.value,
            _T1,
            _NEXT_SESSION_ID,
            2,
            bad_update,
        )

    active = await _active_group_versions(_repo, public_id, _T2)
    versions = await _group_versions(_repo, public_id)
    assert len(active) == 1
    assert active[0].status == PairedExecutionGroupStatusEnum.ASSEMBLING.value
    assert len(versions) == 1
    assert versions[0].known_to == KNOWN_TO_MAX


@pytest.mark.asyncio
async def test_leg_cas_rejects_unknown_update_fields(
    _repo: SQLAlchemyRepository,
) -> None:
    """Given a typo update key, when leg CAS runs, then it raises before any write.

    A mistyped field (``filled_signed_quantity``) must not silently become a
    non-persisted attribute; the active version stays untouched.
    """
    public_id = _pid(207)
    await _repo.insert_paired_execution_leg(_leg_insert_row(public_id=public_id))
    bad_update: dict[str, object] = {"filled_signed_quantity": 1.0}

    with pytest.raises(ValueError, match="unknown paired-execution leg update fields"):
        await _repo.cas_paired_execution_leg_status(
            public_id,
            PairedExecutionLegStatusEnum.PENDING.value,
            PairedExecutionLegStatusEnum.WORKING.value,
            _T1,
            _NEXT_SESSION_ID,
            2,
            bad_update,
        )

    active = await _active_leg_versions(_repo, public_id, _T2)
    versions = await _leg_versions(_repo, public_id)
    assert len(active) == 1
    assert active[0].status == PairedExecutionLegStatusEnum.PENDING.value
    assert active[0].filled_signed_qty == 0.0
    assert len(versions) == 1
    assert versions[0].known_to == KNOWN_TO_MAX


@pytest.mark.asyncio
async def test_leg_cas_carries_every_non_status_column(
    _repo: SQLAlchemyRepository,
) -> None:
    """Given a fully-populated leg, when CAS changes only status, then all other columns carry.

    Introspects the ORM columns so a future leg column added without a
    matching carry-forward assignment is caught here rather than silently
    reset on every status transition.
    """
    public_id = _pid(208)
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=public_id,
            command_public_id=_pid(702),
            client_order_id="client-cf",
            exchange_order_id="exchange-cf",
            filled_signed_qty=3.5,
            compensated_signed_qty=1.5,
            compensation_seq=4,
            last_venue_event_id=99,
        )
    )
    before = (await _active_leg_versions(_repo, public_id, _T0))[0]
    carried = {
        column.name: getattr(before, column.name) for column in PairedExecutionLeg.__table__.columns
    }

    applied = await _repo.cas_paired_execution_leg_status(
        public_id,
        PairedExecutionLegStatusEnum.PENDING.value,
        PairedExecutionLegStatusEnum.BROKEN.value,
        _T1,
        _NEXT_SESSION_ID,
        2,
        None,
    )
    after = (await _active_leg_versions(_repo, public_id, _T2))[0]

    assert applied is True
    advanced = {"id", "known_to", "session_id", "sequence_id", "timestamp", "status"}
    for column in PairedExecutionLeg.__table__.columns:
        if column.name in advanced:
            continue
        assert getattr(after, column.name) == carried[column.name], column.name
    assert after.status == PairedExecutionLegStatusEnum.BROKEN.value


@pytest.mark.asyncio
async def test_group_cas_carries_every_non_status_column(
    _repo: SQLAlchemyRepository,
) -> None:
    """Given a populated group, when CAS changes only status, then all other columns carry.

    Mirror of the leg carry-forward guard: a future group column added
    without a matching carry-forward assignment is caught here rather than
    silently reset on every status transition.
    """
    public_id = _pid(209)
    await _repo.insert_paired_execution_group(
        _group_insert_row(
            public_id=public_id,
            status=PairedExecutionGroupStatusEnum.BROKEN.value,
            failure_reason="leg rejected",
            halted_at=_T0,
        )
    )
    before = (await _active_group_versions(_repo, public_id, _T0))[0]
    carried = {
        column.name: getattr(before, column.name)
        for column in PairedExecutionGroup.__table__.columns
    }

    applied = await _repo.cas_paired_execution_group_status(
        public_id,
        PairedExecutionGroupStatusEnum.BROKEN.value,
        PairedExecutionGroupStatusEnum.COMPENSATING.value,
        _T1,
        _NEXT_SESSION_ID,
        2,
        None,
    )
    after = (await _active_group_versions(_repo, public_id, _T2))[0]

    assert applied is True
    advanced = {"id", "known_to", "session_id", "sequence_id", "timestamp", "status"}
    for column in PairedExecutionGroup.__table__.columns:
        if column.name in advanced:
            continue
        assert getattr(after, column.name) == carried[column.name], column.name
    assert after.status == PairedExecutionGroupStatusEnum.COMPENSATING.value


async def _insert_created_command(
    repo: SQLAlchemyRepository,
    *,
    public_id: str,
    correlation_id: str,
    supersedes_command_id: str | None = None,
    now: datetime = _T0,
) -> None:
    """Insert a 'created' submit command with an explicit public_id.

    Inserts the ORM row directly (rather than via ``insert_trade_command``)
    so the outbox-gate tests can pin the command's ``public_id`` and bind a
    paired-execution leg to it.
    """
    async with repo.session() as session:
        session.add(
            TradeCommand(
                public_id=public_id,
                command_type="submit",
                shard_key="kraken.BTC-USD.live",
                exchange="kraken",
                instrument="BTC-USD",
                mode="live",
                strategy_id="engine",
                client_order_id=public_id,
                venue_client_id=public_id,
                side="buy",
                order_type="market",
                quantity=1.0,
                price=None,
                status="created",
                created_at=now,
                correlation_id=correlation_id,
                supersedes_command_id=supersedes_command_id,
                wallet_public_id="",
                session_id=_SESSION_ID,
                sequence_id=1,
                timestamp=now,
            )
        )
        await session.commit()


async def _seed_two_leg_group(
    repo: SQLAlchemyRepository,
    *,
    group_public_id: str,
    status: str = PairedExecutionGroupStatusEnum.ASSEMBLING.value,
    assembly_deadline: datetime = _T2,
    leg0_instrument: str = "BTC-USD",
    leg1_instrument: str = "ETH-USD",
    leg0_command: str | None = "cmd-0",
    leg1_command: str | None = "cmd-1",
    leg0_index: int = 0,
    leg1_index: int = 1,
) -> None:
    """Seed an assembling 2-leg group with its two legs (BTC + ETH by default)."""
    await repo.insert_paired_execution_group(
        _group_insert_row(
            public_id=group_public_id,
            status=status,
            assembly_deadline=assembly_deadline,
        )
    )
    await repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=_pid(200),
            group_public_id=group_public_id,
            leg_index=leg0_index,
            instrument=leg0_instrument,
            shard_key=f"kraken:{leg0_instrument}:live",
            command_public_id=leg0_command,
        )
    )
    await repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=_pid(201),
            group_public_id=group_public_id,
            leg_index=leg1_index,
            instrument=leg1_instrument,
            shard_key=f"kraken:{leg1_instrument}:live",
            command_public_id=leg1_command,
        )
    )


@pytest.mark.asyncio
async def test_ensure_group_creates_then_skips_active_duplicate(
    _repo: SQLAlchemyRepository,
) -> None:
    """ensure_paired_execution_group is idempotent on the active public_id.

    Given: no active group for a public_id,
    When: ensure is called twice for the same public_id,
    Then: the first call creates it (True) and the second loses the
        active-unique race (False), leaving exactly one active group.
    """
    group_id = _pid(100)
    assert await _repo.ensure_paired_execution_group(_group_insert_row(public_id=group_id)) is True
    assert await _repo.ensure_paired_execution_group(_group_insert_row(public_id=group_id)) is False
    active = await _active_group_versions(_repo, group_id, _T0)
    assert len(active) == 1


@pytest.mark.asyncio
async def test_try_arm_succeeds_when_leg_set_complete(_repo: SQLAlchemyRepository) -> None:
    """A complete, command-bearing, key-matching leg set arms the group.

    Given: an assembling group whose two active legs (BTC + ETH) each
        carry a command and whose tokens match group_key,
    When: try_arm is called before the assembly deadline,
    Then: it returns True and the active group transitions to armed.
    """
    group_id = _pid(100)
    await _seed_two_leg_group(_repo, group_public_id=group_id)
    assert (
        await _repo.try_arm_paired_execution_group_if_complete(group_id, _T1, _NEXT_SESSION_ID, 2)
        is True
    )
    armed = await _repo.get_paired_execution_group(group_id, _T2)
    assert armed is not None
    assert armed["status"] == PairedExecutionGroupStatusEnum.ARMED.value


@pytest.mark.asyncio
async def test_try_arm_false_when_group_missing(_repo: SQLAlchemyRepository) -> None:
    """Arming a non-existent group returns False."""
    assert (
        await _repo.try_arm_paired_execution_group_if_complete(_pid(999), _T1, _NEXT_SESSION_ID, 2)
        is False
    )


@pytest.mark.asyncio
async def test_try_arm_false_when_not_assembling(_repo: SQLAlchemyRepository) -> None:
    """A group that is not assembling cannot be armed again."""
    group_id = _pid(100)
    await _seed_two_leg_group(
        _repo, group_public_id=group_id, status=PairedExecutionGroupStatusEnum.ARMED.value
    )
    assert (
        await _repo.try_arm_paired_execution_group_if_complete(group_id, _T1, _NEXT_SESSION_ID, 2)
        is False
    )


@pytest.mark.asyncio
async def test_try_arm_false_after_assembly_deadline(_repo: SQLAlchemyRepository) -> None:
    """A complete group past its assembly deadline does not arm."""
    group_id = _pid(100)
    await _seed_two_leg_group(_repo, group_public_id=group_id, assembly_deadline=_T0)
    assert (
        await _repo.try_arm_paired_execution_group_if_complete(group_id, _T1, _NEXT_SESSION_ID, 2)
        is False
    )


@pytest.mark.asyncio
async def test_try_arm_false_when_leg_count_incomplete(_repo: SQLAlchemyRepository) -> None:
    """A group missing a sibling leg never arms (no naked dispatch)."""
    group_id = _pid(100)
    await _repo.insert_paired_execution_group(_group_insert_row(public_id=group_id))
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(public_id=_pid(200), group_public_id=group_id, command_public_id="cmd-0")
    )
    assert (
        await _repo.try_arm_paired_execution_group_if_complete(group_id, _T1, _NEXT_SESSION_ID, 2)
        is False
    )


@pytest.mark.asyncio
async def test_try_arm_false_when_indices_not_contiguous(_repo: SQLAlchemyRepository) -> None:
    """A leg set with a gap in indices never arms even at the right count."""
    group_id = _pid(100)
    await _seed_two_leg_group(_repo, group_public_id=group_id, leg1_index=2)
    assert (
        await _repo.try_arm_paired_execution_group_if_complete(group_id, _T1, _NEXT_SESSION_ID, 2)
        is False
    )


@pytest.mark.asyncio
async def test_try_arm_false_when_a_leg_has_no_command(_repo: SQLAlchemyRepository) -> None:
    """A leg without a durable command blocks arming (atomic pair release)."""
    group_id = _pid(100)
    await _seed_two_leg_group(_repo, group_public_id=group_id, leg1_command=None)
    assert (
        await _repo.try_arm_paired_execution_group_if_complete(group_id, _T1, _NEXT_SESSION_ID, 2)
        is False
    )


@pytest.mark.asyncio
async def test_try_arm_false_when_leg_set_key_mismatches(_repo: SQLAlchemyRepository) -> None:
    """A wrong leg set (two same-instrument legs) never arms a BTC+ETH group.

    Defends the count-only-arming hole: two BTC legs at indices 0 and 1
    each carry a command and number exactly expected_leg_count, but their
    canonical key is BTC|BTC which does not equal the group's BTC|ETH
    group_key, so the group cannot arm with the ETH sibling absent.
    """
    group_id = _pid(100)
    await _seed_two_leg_group(_repo, group_public_id=group_id, leg1_instrument="BTC-USD")
    assert (
        await _repo.try_arm_paired_execution_group_if_complete(group_id, _T1, _NEXT_SESSION_ID, 2)
        is False
    )


@pytest.mark.asyncio
async def test_outbox_dispatches_non_grouped_command(_repo: SQLAlchemyRepository) -> None:
    """A command whose correlation_id names no group is always dispatchable.

    Given: a 'created' command with a correlation_id that matches no
        active paired-execution group,
    When: get_undispatched_commands runs,
    Then: the command is returned (the gate never holds non-grouped
        traffic), preserving the pre-guard dispatch behaviour.
    """
    await _insert_created_command(_repo, public_id=_pid(300), correlation_id="solo-corr")
    rows = await _repo.get_undispatched_commands(as_of=_T1, limit=10)
    assert [row["public_id"] for row in rows] == [_pid(300)]


@pytest.mark.asyncio
async def test_outbox_holds_grouped_command_until_group_is_armed(
    _repo: SQLAlchemyRepository,
) -> None:
    """A grouped leg command dispatches only once its group is armed.

    Given: an assembling group with two legs, each bound to a 'created'
        command (correlation_id = group id),
    When: get_undispatched_commands runs before and after arming,
    Then: neither command is dispatchable while assembling, and both
        become dispatchable once the group is armed.
    """
    group_id = _pid(100)
    await _seed_two_leg_group(
        _repo, group_public_id=group_id, leg0_command=_pid(300), leg1_command=_pid(301)
    )
    await _insert_created_command(_repo, public_id=_pid(300), correlation_id=group_id)
    await _insert_created_command(_repo, public_id=_pid(301), correlation_id=group_id)
    held = await _repo.get_undispatched_commands(as_of=_T1, limit=10)
    assert held == []
    assert (
        await _repo.try_arm_paired_execution_group_if_complete(group_id, _T1, _NEXT_SESSION_ID, 2)
        is True
    )
    released = await _repo.get_undispatched_commands(as_of=_T3, limit=10)
    assert sorted(row["public_id"] for row in released) == [_pid(300), _pid(301)]


@pytest.mark.asyncio
async def test_outbox_never_dispatches_orphan_grouped_command(
    _repo: SQLAlchemyRepository,
) -> None:
    """An armed group never releases a command no active leg points at.

    Defends the crash/replay orphan hole: an extra 'created' command
    carrying the group's correlation_id but bound to no leg
    (command_public_id) must never dispatch, even after the group arms,
    so a stale duplicate can never go naked alongside the real legs.
    """
    group_id = _pid(100)
    await _seed_two_leg_group(
        _repo,
        group_public_id=group_id,
        status=PairedExecutionGroupStatusEnum.ARMED.value,
        leg0_command=_pid(300),
        leg1_command=_pid(301),
    )
    await _insert_created_command(_repo, public_id=_pid(300), correlation_id=group_id)
    await _insert_created_command(_repo, public_id=_pid(301), correlation_id=group_id)
    await _insert_created_command(_repo, public_id=_pid(399), correlation_id=group_id)
    rows = await _repo.get_undispatched_commands(as_of=_T1, limit=10)
    assert sorted(row["public_id"] for row in rows) == [_pid(300), _pid(301)]


@pytest.mark.asyncio
async def test_outbox_dispatches_compensation_command_despite_unarmed_group(
    _repo: SQLAlchemyRepository,
) -> None:
    """A compensation command (supersedes set) bypasses the arming gate.

    Given: an assembling (unarmed) group and a 'created' command that
        carries the group's correlation_id AND a supersedes_command_id,
    When: get_undispatched_commands runs,
    Then: the compensation command is dispatchable despite the group not
        being armed, so reduce-only flattening is never held by the gate.
    """
    group_id = _pid(100)
    await _repo.insert_paired_execution_group(_group_insert_row(public_id=group_id))
    await _insert_created_command(
        _repo, public_id=_pid(400), correlation_id=group_id, supersedes_command_id=_pid(300)
    )
    rows = await _repo.get_undispatched_commands(as_of=_T1, limit=10)
    assert [row["public_id"] for row in rows] == [_pid(400)]


@pytest.mark.asyncio
async def test_ensure_reraises_non_collision_integrity_error(
    _repo: SQLAlchemyRepository,
) -> None:
    """A non-uniqueness integrity failure is surfaced, not masked as duplicate.

    Given: a group insert row that violates the immutable policy CHECK
        constraint (not the active-unique public_id index), with no active
        group present,
    When: ensure_paired_execution_group runs,
    Then: the IntegrityError propagates rather than being swallowed as
        'already exists', so a malformed group can never silently leave its
        legs' commands to dispatch ungrouped (naked).
    """
    with pytest.raises(IntegrityError):
        await _repo.ensure_paired_execution_group(
            _group_insert_row(public_id=_pid(100), policy="bogus-policy")
        )


@pytest.mark.asyncio
async def test_ensure_halt_creates_then_skips_active_duplicate(
    _repo: SQLAlchemyRepository,
) -> None:
    """ensure_paired_execution_halt is idempotent on the active scope.

    Given: no active halt for a (wallet, strategy, group_key) scope,
    When: ensure is called twice for that scope with distinct public ids,
    Then: the first call creates the halt (True) and the second loses the
        active-unique scope race (False), leaving exactly one active halt — the
        first — so racing scanners project a single durable halt per pair.
    """
    assert await _repo.ensure_paired_execution_halt(_halt_insert_row(public_id=_pid(100))) is True
    assert await _repo.ensure_paired_execution_halt(_halt_insert_row(public_id=_pid(101))) is False
    active = await _active_halts(_repo, _T0)
    assert len(active) == 1
    assert active[0].public_id == _pid(100)


@pytest.mark.asyncio
async def test_ensure_halt_creates_distinct_scopes_independently(
    _repo: SQLAlchemyRepository,
) -> None:
    """Halts on different pair scopes do not collide.

    Given: an active halt for one group_key,
    When: ensure is called for a DIFFERENT group_key under the same wallet,
    Then: it creates a second active halt, because the active-unique scope is
        per (wallet, strategy, group_key), not one global halt.
    """
    assert await _repo.ensure_paired_execution_halt(_halt_insert_row(public_id=_pid(100))) is True
    assert (
        await _repo.ensure_paired_execution_halt(
            _halt_insert_row(public_id=_pid(101), group_key=_OTHER_GROUP_KEY)
        )
        is True
    )
    active = await _active_halts(_repo, _T0)
    assert len(active) == 2


@pytest.mark.asyncio
async def test_ensure_halt_reraises_non_collision_integrity_error(
    _repo: SQLAlchemyRepository,
) -> None:
    """A non-uniqueness integrity failure is surfaced, not masked as duplicate.

    Given: a halt insert row that violates the immutable mode CHECK constraint
        (not the active-unique scope index), with no active halt present,
    When: ensure_paired_execution_halt runs,
    Then: the IntegrityError propagates rather than being swallowed as 'already
        exists', so a malformed halt can never be silently dropped.
    """
    with pytest.raises(IntegrityError):
        await _repo.ensure_paired_execution_halt(
            _halt_insert_row(public_id=_pid(100), mode="bogus-mode")
        )


@pytest.mark.asyncio
async def test_list_active_halts_returns_active_ordered_excludes_cleared(
    _repo: SQLAlchemyRepository,
) -> None:
    """list_active_paired_execution_halts returns current-active halts, cleared excluded.

    Given: two active halts on distinct scopes (same created_at, so ordered by
        id), the first later cleared,
    When: list_active_paired_execution_halts runs before and after the clear,
    Then: before it returns both in id order; after it returns only the still-
        active halt, because the cleared row's known_to is no longer MAX — so
        startup recovery never re-halts a deliberately-freed pair.
    """
    halt_a = _pid(100)
    halt_b = _pid(101)
    await _repo.insert_paired_execution_halt(_halt_insert_row(public_id=halt_a))
    await _repo.insert_paired_execution_halt(
        _halt_insert_row(public_id=halt_b, group_key=_OTHER_GROUP_KEY)
    )
    before = await _repo.list_active_paired_execution_halts()
    assert [h["public_id"] for h in before] == [halt_a, halt_b]
    await _repo.clear_paired_execution_halt(halt_a, _T1)
    after = await _repo.list_active_paired_execution_halts()
    assert [h["public_id"] for h in after] == [halt_b]


@pytest.mark.asyncio
async def test_get_current_legs_includes_future_stamped_leg_excluded_by_temporal_read(
    _repo: SQLAlchemyRepository,
) -> None:
    """get_current_paired_execution_legs returns current-active legs regardless of timestamp.

    Given: a group with leg 0 stamped at _T0 and leg 1 stamped in the FUTURE
        (_T2, a clock-skewed sibling write),
    When: the current-active reader and the temporal as-of reader both run,
    Then: the current reader returns both legs ordered by index, while the
        temporal read at _T0 excludes the future-stamped leg — pinning why
        startup recovery uses the current-active reader so a clock-skewed leg's
        shard is still mirrored.
    """
    group_id = _pid(100)
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(public_id=_pid(200), group_public_id=group_id, leg_index=0, shard_key="s0")
    )
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=_pid(201),
            group_public_id=group_id,
            leg_index=1,
            shard_key="s1",
            timestamp=_T2,
        )
    )
    current = await _repo.get_current_paired_execution_legs(group_id)
    assert [leg["leg_index"] for leg in current] == [0, 1]
    temporal = await _repo.get_paired_execution_legs(group_id, _T0)
    assert [leg["leg_index"] for leg in temporal] == [0]


@pytest.mark.asyncio
async def test_project_leg_fill_sets_signed_cumulative_status_and_venue_ids(
    _repo: SQLAlchemyRepository,
) -> None:
    """A grouped fill sets the leg's signed cumulative qty, status and venue ids.

    Given: an active armed buy leg with zero fill,
    When: project_paired_execution_leg_fill runs for its client_order_id with a
        positive signed cumulative and a 'filled' status,
    Then: it returns ORIGINAL_APPLIED and the active successor carries the new
        filled_signed_qty, status, exchange_order_id and last_venue_event_id, so
        the guard scanner sees real exposure.
    """
    leg_id = _pid(200)
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=leg_id,
            leg_index=0,
            side="buy",
            client_order_id="cid-1",
            status=PairedExecutionLegStatusEnum.ARMED.value,
        )
    )
    applied = await _repo.project_paired_execution_leg_fill(
        "cid-1",
        5.0,
        PairedExecutionLegStatusEnum.FILLED.value,
        _T1,
        _NEXT_SESSION_ID,
        2,
        exchange_order_id="exch-1",
        last_venue_event_id=42,
    )
    assert applied == PairedFillProjection.ORIGINAL_APPLIED
    active = await _active_leg_versions(_repo, leg_id, _T2)
    assert len(active) == 1
    assert active[0].filled_signed_qty == 5.0
    assert active[0].status == PairedExecutionLegStatusEnum.FILLED.value
    assert active[0].exchange_order_id == "exch-1"
    assert active[0].last_venue_event_id == 42


@pytest.mark.asyncio
async def test_project_leg_fill_is_monotonic_skipping_stale_and_duplicate(
    _repo: SQLAlchemyRepository,
) -> None:
    """Projection only grows the cumulative; a stale or duplicate replay is a no-op.

    Given: an active leg already at filled_signed_qty=5,
    When: projections arrive for the same (5) and a smaller (3) cumulative, then a
        larger (8) one,
    Then: the equal and smaller replays return ORIGINAL_NOOP without regressing
        the leg, and only the larger cumulative is applied — so out-of-order /
        duplicate fills never shrink the recorded exposure.
    """
    leg_id = _pid(200)
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=leg_id,
            leg_index=0,
            side="buy",
            client_order_id="cid-1",
            status=PairedExecutionLegStatusEnum.PARTIALLY_FILLED.value,
            filled_signed_qty=5.0,
        )
    )
    dup = await _repo.project_paired_execution_leg_fill(
        "cid-1", 5.0, PairedExecutionLegStatusEnum.FILLED.value, _T1, _NEXT_SESSION_ID, 2
    )
    stale = await _repo.project_paired_execution_leg_fill(
        "cid-1", 3.0, PairedExecutionLegStatusEnum.FILLED.value, _T1, _NEXT_SESSION_ID, 3
    )
    grow = await _repo.project_paired_execution_leg_fill(
        "cid-1", 8.0, PairedExecutionLegStatusEnum.FILLED.value, _T1, _NEXT_SESSION_ID, 4
    )
    assert dup == PairedFillProjection.ORIGINAL_NOOP
    assert stale == PairedFillProjection.ORIGINAL_NOOP
    assert grow == PairedFillProjection.ORIGINAL_APPLIED
    active = await _active_leg_versions(_repo, leg_id, _T2)
    assert active[0].filled_signed_qty == 8.0


@pytest.mark.asyncio
async def test_project_leg_fill_clamps_bus_time_for_future_stamped_leg(
    _repo: SQLAlchemyRepository,
) -> None:
    """A future-stamped active leg keeps a non-inverted SCD2 interval on projection.

    Given: a current-active armed leg whose ``timestamp`` is in the future (a
        sibling coordinator armed it with a clock running ahead),
    When: project_paired_execution_leg_fill runs with an EARLIER ``bus_time``
        (the recovering coordinator's wall clock),
    Then: the close / successor bus time is clamped to the leg's own timestamp,
        so the closed predecessor's ``known_to`` is not set before its
        ``timestamp`` and the fill successor is not backdated before its
        predecessor — the SCD2 validity intervals stay ordered.
    """
    future = datetime(2099, 1, 1, tzinfo=UTC)
    leg_id = _pid(220)
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=leg_id,
            leg_index=0,
            side="buy",
            client_order_id="cid-future",
            status=PairedExecutionLegStatusEnum.ARMED.value,
            timestamp=future,
        )
    )
    applied = await _repo.project_paired_execution_leg_fill(
        "cid-future",
        4.0,
        PairedExecutionLegStatusEnum.FILLED.value,
        _T1,
        _NEXT_SESSION_ID,
        2,
    )
    assert applied == PairedFillProjection.ORIGINAL_APPLIED
    async with _repo.session() as session:
        versions = list(
            (
                await session.execute(
                    select(PairedExecutionLeg)
                    .where(PairedExecutionLeg.public_id == leg_id)
                    .order_by(PairedExecutionLeg.id)
                )
            )
            .scalars()
            .all()
        )
    assert len(versions) == 2
    predecessor, successor = versions
    assert predecessor.known_to == future
    assert predecessor.known_to >= predecessor.timestamp
    assert successor.timestamp == future
    assert successor.timestamp >= predecessor.timestamp
    assert successor.known_to == KNOWN_TO_MAX
    assert successor.filled_signed_qty == 4.0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("terminal_status", "expected_status"),
    [
        (
            PairedExecutionLegStatusEnum.REJECTED.value,
            PairedExecutionLegStatusEnum.REJECTED.value,
        ),
        (
            PairedExecutionLegStatusEnum.CANCELLED.value,
            PairedExecutionLegStatusEnum.CANCELLED.value,
        ),
        (
            PairedExecutionLegStatusEnum.EXPIRED.value,
            PairedExecutionLegStatusEnum.EXPIRED.value,
        ),
        (
            PairedExecutionLegStatusEnum.BROKEN.value,
            PairedExecutionLegStatusEnum.FILLED.value,
        ),
        (
            PairedExecutionLegStatusEnum.COMPENSATING.value,
            PairedExecutionLegStatusEnum.COMPENSATING.value,
        ),
        (
            PairedExecutionLegStatusEnum.FLATTENED.value,
            PairedExecutionLegStatusEnum.FILLED.value,
        ),
        (
            PairedExecutionLegStatusEnum.MANUAL_INTERVENTION.value,
            PairedExecutionLegStatusEnum.MANUAL_INTERVENTION.value,
        ),
    ],
)
async def test_project_leg_fill_late_reentry_per_terminal_status(
    _repo: SQLAlchemyRepository, terminal_status: str, expected_status: str
) -> None:
    """A LATE original fill re-enters each post-break leg status per the 5d.2 FSM.

    Given: an active leg already in each fill-terminal status with zero fill,
    When: a LATE growing original fill arrives for its client_order_id,
    Then: filled_signed_qty is updated (real venue exposure only grows) and the
        successor status follows the re-entry FSM — flattened/broken reopen to
        ``filled`` (so the sweep re-flattens the residual), while
        compensating/cancelled/expired/rejected/manual_intervention keep their
        status. Covers the whole terminal set so a new terminal status added
        without a re-entry policy is caught.
    """
    leg_id = _pid(200)
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=leg_id,
            leg_index=0,
            side="buy",
            client_order_id="cid-1",
            status=terminal_status,
        )
    )
    applied = await _repo.project_paired_execution_leg_fill(
        "cid-1", 5.0, PairedExecutionLegStatusEnum.FILLED.value, _T1, _NEXT_SESSION_ID, 2
    )
    assert applied == PairedFillProjection.ORIGINAL_APPLIED
    active = await _active_leg_versions(_repo, leg_id, _T2)
    assert active[0].filled_signed_qty == 5.0
    assert active[0].status == expected_status


@pytest.mark.asyncio
async def test_project_leg_fill_late_reentry_keeps_flattened_when_no_residual(
    _repo: SQLAlchemyRepository,
) -> None:
    """A late fill that does not re-open exposure leaves a flattened leg flattened.

    Given: a flattened leg already over-compensated (filled=3, compensated=5),
    When: a late original fill grows filled to exactly the compensated magnitude
        (5), so open_qty collapses to zero,
    Then: the leg stays FLATTENED (no spurious reopen) — the reopen only fires when
        real residual exposure reappears.
    """
    leg_id = _pid(201)
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=leg_id,
            leg_index=0,
            side="buy",
            client_order_id="cid-nores",
            status=PairedExecutionLegStatusEnum.FLATTENED.value,
            filled_signed_qty=3.0,
            compensated_signed_qty=5.0,
        )
    )
    applied = await _repo.project_paired_execution_leg_fill(
        "cid-nores", 5.0, PairedExecutionLegStatusEnum.FILLED.value, _T1, _NEXT_SESSION_ID, 2
    )
    assert applied == PairedFillProjection.ORIGINAL_APPLIED
    active = await _active_leg_versions(_repo, leg_id, _T2)
    assert active[0].filled_signed_qty == 5.0
    assert active[0].status == PairedExecutionLegStatusEnum.FLATTENED.value


@pytest.mark.asyncio
async def test_project_leg_fill_late_reentry_reopens_short_flattened_leg(
    _repo: SQLAlchemyRepository,
) -> None:
    """A late SELL fill on a flattened SHORT leg reopens it (sign-independent reopen).

    Given: a flattened short leg (filled=−10, compensated=−10, open 0),
    When: a late original SELL fill grows the signed cumulative to −15,
    Then: filled becomes −15, the residual reappears (open=−5) and the leg reopens
        to FILLED — the reopen uses abs(open_qty) so it is symmetric to the long
        case.
    """
    leg_id = _pid(202)
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=leg_id,
            leg_index=0,
            side="sell",
            client_order_id="cid-short",
            status=PairedExecutionLegStatusEnum.FLATTENED.value,
            filled_signed_qty=-10.0,
            compensated_signed_qty=-10.0,
        )
    )
    applied = await _repo.project_paired_execution_leg_fill(
        "cid-short", -15.0, PairedExecutionLegStatusEnum.FILLED.value, _T1, _NEXT_SESSION_ID, 2
    )
    assert applied == PairedFillProjection.ORIGINAL_APPLIED
    active = await _active_leg_versions(_repo, leg_id, _T2)
    assert active[0].filled_signed_qty == -15.0
    assert active[0].status == PairedExecutionLegStatusEnum.FILLED.value


@pytest.mark.asyncio
async def test_project_leg_fill_carries_every_other_column(_repo: SQLAlchemyRepository) -> None:
    """Given a fully-populated leg, projection changes only fill fields; the rest carry.

    Introspects the ORM columns so a future leg column added without a matching
    carry-forward assignment in project_paired_execution_leg_fill is caught here
    rather than silently reset on every fill projection.
    """
    leg_id = _pid(208)
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=leg_id,
            side="buy",
            command_public_id=_pid(702),
            client_order_id="cid-cf",
            exchange_order_id="exchange-existing",
            filled_signed_qty=0.0,
            compensated_signed_qty=1.5,
            compensation_seq=4,
            last_venue_event_id=11,
            status=PairedExecutionLegStatusEnum.WORKING.value,
        )
    )
    before = (await _active_leg_versions(_repo, leg_id, _T0))[0]
    carried = {
        column.name: getattr(before, column.name) for column in PairedExecutionLeg.__table__.columns
    }
    applied = await _repo.project_paired_execution_leg_fill(
        "cid-cf",
        6.0,
        PairedExecutionLegStatusEnum.FILLED.value,
        _T1,
        _NEXT_SESSION_ID,
        2,
        exchange_order_id="exchange-new",
        last_venue_event_id=42,
    )
    after = (await _active_leg_versions(_repo, leg_id, _T2))[0]
    assert applied == PairedFillProjection.ORIGINAL_APPLIED
    advanced = {
        "id",
        "known_to",
        "session_id",
        "sequence_id",
        "timestamp",
        "status",
        "filled_signed_qty",
        "exchange_order_id",
        "last_venue_event_id",
    }
    for column in PairedExecutionLeg.__table__.columns:
        if column.name in advanced:
            continue
        assert getattr(after, column.name) == carried[column.name], column.name
    assert after.status == PairedExecutionLegStatusEnum.FILLED.value
    assert after.filled_signed_qty == 6.0
    assert after.exchange_order_id == "exchange-new"
    assert after.last_venue_event_id == 42


@pytest.mark.asyncio
async def test_project_leg_fill_raises_on_duplicate_active_client_order_id(
    _repo: SQLAlchemyRepository,
) -> None:
    """Two active legs sharing a client_order_id is a data-integrity violation, not silent.

    Given: two active legs in one group that share a client_order_id (the index is
        not unique; order ids are UUID7 so this should be impossible),
    When: project_paired_execution_leg_fill resolves the leg,
    Then: it raises rather than silently picking the first, so a corrupt
        client-order identity is surfaced (recovery catches it per leg).
    """
    group_id = _pid(100)
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=_pid(200), group_public_id=group_id, leg_index=0, client_order_id="dup"
        )
    )
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=_pid(201), group_public_id=group_id, leg_index=1, client_order_id="dup"
        )
    )
    with pytest.raises(ValueError, match="data integrity"):
        await _repo.project_paired_execution_leg_fill(
            "dup", 5.0, PairedExecutionLegStatusEnum.FILLED.value, _T1, _NEXT_SESSION_ID, 2
        )


async def _insert_fill_event(
    repo: SQLAlchemyRepository,
    *,
    client_order_id: str,
    cum_fill_size: float | None,
    event_type: str = "fill_observed",
    side: str = "buy",
    status: str = "filled",
) -> int:
    """Insert a venue event and return its auto-increment id (id order = insert order)."""
    new_id: int = await repo.insert_venue_event(
        {
            "event_type": event_type,
            "shard_key": "kraken.BTC-USD.live",
            "wallet_public_id": _WALLET_ID,
            "exchange": "kraken",
            "instrument": "BTC-USD",
            "mode": "live",
            "client_order_id": client_order_id,
            "exchange_order_id": f"exch-{client_order_id}",
            "side": side,
            "status": status,
            "fill_size": cum_fill_size,
            "cum_fill_size": cum_fill_size,
            "received_at": _T0,
            "session_id": _SESSION_ID,
            "sequence_id": 1,
            "timestamp": _T0,
        }
    )
    return new_id


@pytest.mark.asyncio
async def test_get_max_cumulative_fill_returns_largest_cumulative_not_latest_id(
    _repo: SQLAlchemyRepository,
) -> None:
    """The recovery fill lookup returns the MAX cumulative event, not the latest id.

    Given: three fill_observed events for one client order with cumulatives 3, 8, 5
        inserted in that id order (so the largest, 8, is NOT the latest id), plus a
        null-cumulative event, a non-fill event, and a different order's event,
    When: get_max_cumulative_fill_venue_event runs,
    Then: it returns the cum_fill_size=8 event (the true cumulative), ignoring the
        later-but-smaller, null-cumulative, non-fill, and other-order rows — so an
        out-of-order executor write cannot make recovery read a smaller cumulative.
    """
    await _insert_fill_event(_repo, client_order_id="cid-1", cum_fill_size=3.0)
    id_of_max = await _insert_fill_event(_repo, client_order_id="cid-1", cum_fill_size=8.0)
    await _insert_fill_event(_repo, client_order_id="cid-1", cum_fill_size=5.0)
    await _insert_fill_event(_repo, client_order_id="cid-1", cum_fill_size=None)
    await _insert_fill_event(
        _repo, client_order_id="cid-1", cum_fill_size=99.0, event_type="order_accepted"
    )
    await _insert_fill_event(_repo, client_order_id="cid-other", cum_fill_size=42.0)
    event = await _repo.get_max_cumulative_fill_venue_event("cid-1")
    assert event is not None
    assert event["cum_fill_size"] == 8.0
    assert event["id"] == id_of_max
    assert await _repo.get_max_cumulative_fill_venue_event("cid-absent") is None


@pytest.mark.asyncio
async def test_list_current_paired_execution_groups_empty_statuses_is_noop(
    _repo: SQLAlchemyRepository,
) -> None:
    """An empty status set short-circuits to an empty list without querying.

    Given: an empty ``statuses`` argument,
    When: list_current_paired_execution_groups runs,
    Then: it returns ``[]`` without issuing an ``IN ()`` query — mirroring the
        defensive guard on the sibling ``list_active_paired_execution_groups``.
    """
    assert await _repo.list_current_paired_execution_groups([]) == []


@pytest.mark.asyncio
async def test_project_leg_fill_no_matching_leg_is_noop(_repo: SQLAlchemyRepository) -> None:
    """A fill whose client_order_id matches no leg is a no-op (non-grouped order).

    Given: no leg bound to the fill's client_order_id,
    When: project_paired_execution_leg_fill runs,
    Then: it returns NO_MATCH without error, so a non-grouped fill is harmlessly
        ignored (and the live hook can then try the compensation projection).
    """
    applied = await _repo.project_paired_execution_leg_fill(
        "cid-absent", 5.0, PairedExecutionLegStatusEnum.FILLED.value, _T1, _NEXT_SESSION_ID, 2
    )
    assert applied == PairedFillProjection.NO_MATCH


@pytest.mark.asyncio
async def test_project_leg_fill_sell_leg_records_negative_signed_qty(
    _repo: SQLAlchemyRepository,
) -> None:
    """A sell leg's fill is stored as a negative signed cumulative.

    Given: an active sell leg,
    When: a projection arrives with a negative signed cumulative,
    Then: filled_signed_qty is the negative value, so the signed-qty model
        (buy +, sell −) is preserved for the open-qty accounting.
    """
    leg_id = _pid(200)
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=leg_id,
            leg_index=0,
            side="sell",
            client_order_id="cid-1",
            status=PairedExecutionLegStatusEnum.WORKING.value,
        )
    )
    applied = await _repo.project_paired_execution_leg_fill(
        "cid-1",
        -5.0,
        PairedExecutionLegStatusEnum.FILLED.value,
        _T1,
        _NEXT_SESSION_ID,
        2,
    )
    assert applied == PairedFillProjection.ORIGINAL_APPLIED
    active = await _active_leg_versions(_repo, leg_id, _T2)
    assert active[0].filled_signed_qty == -5.0


@pytest.mark.asyncio
async def test_project_leg_fill_carries_forward_venue_ids_when_none(
    _repo: SQLAlchemyRepository,
) -> None:
    """Omitted venue ids carry forward the leg's existing values.

    Given: an active leg already carrying an exchange_order_id and
        last_venue_event_id,
    When: a growing projection arrives with both venue ids omitted (None),
    Then: the successor keeps the existing venue ids while applying the new
        filled_signed_qty.
    """
    leg_id = _pid(200)
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=leg_id,
            leg_index=0,
            side="buy",
            client_order_id="cid-1",
            status=PairedExecutionLegStatusEnum.WORKING.value,
            exchange_order_id="exch-existing",
            last_venue_event_id=7,
        )
    )
    applied = await _repo.project_paired_execution_leg_fill(
        "cid-1", 5.0, PairedExecutionLegStatusEnum.FILLED.value, _T1, _NEXT_SESSION_ID, 2
    )
    assert applied == PairedFillProjection.ORIGINAL_APPLIED
    active = await _active_leg_versions(_repo, leg_id, _T2)
    assert active[0].exchange_order_id == "exch-existing"
    assert active[0].last_venue_event_id == 7
    assert active[0].filled_signed_qty == 5.0


@pytest.mark.asyncio
async def test_project_leg_terminal_sets_status_and_preserves_fill(
    _repo: SQLAlchemyRepository,
) -> None:
    """A venue terminal projects the leg status while preserving exposure.

    Given: an active partially-filled buy leg carrying filled_signed_qty,
    When: project_paired_execution_leg_terminal runs for its client_order_id with
        a 'cancelled' status and a new exchange_order_id,
    Then: it returns True and the active successor carries the cancelled status
        and the new exchange_order_id while PRESERVING filled_signed_qty, so the
        compensator still sees the real partial exposure.
    """
    leg_id = _pid(240)
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=leg_id,
            leg_index=0,
            side="buy",
            client_order_id="cid-term",
            status=PairedExecutionLegStatusEnum.PARTIALLY_FILLED.value,
            filled_signed_qty=2.0,
        )
    )
    applied = await _repo.project_paired_execution_leg_terminal(
        "cid-term",
        PairedExecutionLegStatusEnum.CANCELLED.value,
        _T1,
        _NEXT_SESSION_ID,
        2,
        exchange_order_id="exch-term",
    )
    assert applied is True
    active = await _active_leg_versions(_repo, leg_id, _T2)
    assert len(active) == 1
    assert active[0].status == PairedExecutionLegStatusEnum.CANCELLED.value
    assert active[0].filled_signed_qty == 2.0
    assert active[0].exchange_order_id == "exch-term"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "skip_status",
    [
        PairedExecutionLegStatusEnum.FILLED.value,
        PairedExecutionLegStatusEnum.REJECTED.value,
        PairedExecutionLegStatusEnum.CANCELLED.value,
        PairedExecutionLegStatusEnum.EXPIRED.value,
        PairedExecutionLegStatusEnum.BROKEN.value,
        PairedExecutionLegStatusEnum.COMPENSATING.value,
        PairedExecutionLegStatusEnum.FLATTENED.value,
        PairedExecutionLegStatusEnum.MANUAL_INTERVENTION.value,
    ],
)
async def test_project_leg_terminal_skips_filled_and_terminal(
    _repo: SQLAlchemyRepository, skip_status: str
) -> None:
    """A leg already FILLED or in any terminal state is not re-projected.

    Given: an active leg already FILLED or in a terminal state,
    When: project_paired_execution_leg_terminal runs,
    Then: it returns False without writing a successor — a fully filled order
        needs no breakage and a re-delivered terminal event is idempotent.
    """
    leg_id = _pid(241)
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=leg_id,
            leg_index=0,
            side="buy",
            client_order_id="cid-skip",
            status=skip_status,
        )
    )
    applied = await _repo.project_paired_execution_leg_terminal(
        "cid-skip",
        PairedExecutionLegStatusEnum.EXPIRED.value,
        _T1,
        _NEXT_SESSION_ID,
        2,
    )
    assert applied is False
    active = await _active_leg_versions(_repo, leg_id, _T2)
    assert len(active) == 1
    assert active[0].status == skip_status


@pytest.mark.asyncio
async def test_project_leg_terminal_no_matching_leg_is_noop(
    _repo: SQLAlchemyRepository,
) -> None:
    """A terminal whose client_order_id matches no leg is a no-op (non-grouped)."""
    applied = await _repo.project_paired_execution_leg_terminal(
        "cid-absent",
        PairedExecutionLegStatusEnum.REJECTED.value,
        _T1,
        _NEXT_SESSION_ID,
        1,
    )
    assert applied is False


@pytest.mark.asyncio
async def test_project_leg_terminal_raises_on_duplicate_active_client_order_id(
    _repo: SQLAlchemyRepository,
) -> None:
    """Two active legs sharing a client_order_id is a data-integrity error.

    Given: two active legs bound to the same client_order_id,
    When: project_paired_execution_leg_terminal resolves the leg,
    Then: it raises ValueError rather than terminalizing an arbitrary leg — the
        scanner isolates this per leg so it never blocks the rest.
    """
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=_pid(242),
            leg_index=0,
            side="buy",
            client_order_id="cid-dup-term",
            status=PairedExecutionLegStatusEnum.WORKING.value,
        )
    )
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=_pid(243),
            group_public_id=_pid(101),
            leg_index=1,
            side="sell",
            client_order_id="cid-dup-term",
            status=PairedExecutionLegStatusEnum.WORKING.value,
        )
    )
    with pytest.raises(ValueError, match="2 active legs share client_order_id cid-dup-term"):
        await _repo.project_paired_execution_leg_terminal(
            "cid-dup-term",
            PairedExecutionLegStatusEnum.CANCELLED.value,
            _T1,
            _NEXT_SESSION_ID,
            2,
        )


@pytest.mark.asyncio
async def test_project_leg_terminal_clamps_bus_time_for_future_stamped_leg(
    _repo: SQLAlchemyRepository,
) -> None:
    """A future-stamped active leg keeps a non-inverted SCD2 interval on terminal.

    Given: a current-active working leg whose ``timestamp`` is in the future,
    When: project_paired_execution_leg_terminal runs with an EARLIER ``bus_time``,
    Then: the close / successor bus time is clamped to the leg's own timestamp so
        the SCD2 validity intervals stay ordered — the same clock-skew guard the
        fill projection applies.
    """
    future = datetime(2099, 1, 1, tzinfo=UTC)
    leg_id = _pid(244)
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=leg_id,
            leg_index=0,
            side="buy",
            client_order_id="cid-term-future",
            status=PairedExecutionLegStatusEnum.WORKING.value,
            timestamp=future,
        )
    )
    applied = await _repo.project_paired_execution_leg_terminal(
        "cid-term-future",
        PairedExecutionLegStatusEnum.EXPIRED.value,
        _T1,
        _NEXT_SESSION_ID,
        2,
    )
    assert applied is True
    async with _repo.session() as session:
        versions = list(
            (
                await session.execute(
                    select(PairedExecutionLeg)
                    .where(PairedExecutionLeg.public_id == leg_id)
                    .order_by(PairedExecutionLeg.id)
                )
            )
            .scalars()
            .all()
        )
    assert len(versions) == 2
    predecessor, successor = versions
    assert predecessor.known_to == future
    assert successor.timestamp == future
    assert successor.timestamp >= predecessor.timestamp
    assert successor.known_to == KNOWN_TO_MAX
    assert successor.status == PairedExecutionLegStatusEnum.EXPIRED.value


@pytest.mark.asyncio
async def test_outbox_ignores_historical_leg_command_binding(
    _repo: SQLAlchemyRepository,
) -> None:
    """Only an ACTIVE leg binding releases a grouped command.

    Given: an armed group whose two active legs bind C0/C1, plus a CLOSED
        (historical) leg version that once bound an orphan command, and that
        orphan command in 'created',
    When: get_undispatched_commands runs,
    Then: the orphan command is held — the gate matches only active legs, so
        a superseded historical binding never releases a stale command.
    """
    group_id = _pid(100)
    await _seed_two_leg_group(
        _repo,
        group_public_id=group_id,
        status=PairedExecutionGroupStatusEnum.ARMED.value,
        leg0_command=_pid(300),
        leg1_command=_pid(301),
    )
    async with _repo.session() as session:
        session.add(
            PairedExecutionLeg(
                public_id=_pid(250),
                group_public_id=group_id,
                leg_index=0,
                exchange="kraken",
                mode="live",
                instrument="BTC-USD",
                shard_key="kraken:BTC-USD:live",
                side="buy",
                target_qty=1.0,
                signal_public_id=_pid(900),
                command_public_id=_pid(399),
                status=PairedExecutionLegStatusEnum.PENDING.value,
                filled_signed_qty=0.0,
                compensated_signed_qty=0.0,
                compensation_seq=0,
                wallet_public_id=_WALLET_ID,
                operator_public_id=_OPERATOR_ID,
                created_at=_T0,
                session_id=_SESSION_ID,
                sequence_id=1,
                timestamp=_T0,
                known_to=_T1,
            )
        )
        await session.commit()
    await _insert_created_command(_repo, public_id=_pid(300), correlation_id=group_id)
    await _insert_created_command(_repo, public_id=_pid(301), correlation_id=group_id)
    await _insert_created_command(_repo, public_id=_pid(399), correlation_id=group_id)
    rows = await _repo.get_undispatched_commands(as_of=_T1, limit=10)
    assert sorted(row["public_id"] for row in rows) == [_pid(300), _pid(301)]


@pytest.mark.asyncio
async def test_try_arm_carries_group_columns_forward(
    _repo: SQLAlchemyRepository,
) -> None:
    """Arming carries every group column forward except the SCD2/status set.

    Given: a complete assembling group,
    When: try_arm arms it,
    Then: the new active (armed) version preserves every column except
        id/known_to/session_id/sequence_id/timestamp/status, and those carry
        the new bus-time provenance.
    """
    group_id = _pid(100)
    await _seed_two_leg_group(_repo, group_public_id=group_id)
    before = (await _active_group_versions(_repo, group_id, _T0))[0]
    carried = {
        column.name: getattr(before, column.name)
        for column in PairedExecutionGroup.__table__.columns
    }
    assert (
        await _repo.try_arm_paired_execution_group_if_complete(group_id, _T1, _NEXT_SESSION_ID, 7)
        is True
    )
    after = (await _active_group_versions(_repo, group_id, _T2))[0]
    advanced = {"id", "known_to", "session_id", "sequence_id", "timestamp", "status"}
    for column in PairedExecutionGroup.__table__.columns:
        if column.name in advanced:
            continue
        assert getattr(after, column.name) == carried[column.name], column.name
    assert after.status == PairedExecutionGroupStatusEnum.ARMED.value
    assert after.session_id == _NEXT_SESSION_ID
    assert after.sequence_id == 7
    assert after.timestamp == _T1


async def _insert_flatten_command(
    repo: SQLAlchemyRepository,
    *,
    public_id: str,
    supersedes_command_id: str,
    client_order_id: str,
    side: str,
    quantity: float = 1.0,
    idempotency_key: str | None = None,
    correlation_id: str = "00000000-0000-0000-0000-000000000100",
    now: datetime = _T0,
) -> None:
    """Insert an active reduce-only MARKET flatten command superseding a leg's order.

    Mirrors what the guard scanner's ``claim_leg_and_insert_flatten_command``
    emits: a ``reduce_only`` ``submit`` whose
    ``supersedes_command_id`` is the leg's ORIGINAL command and whose
    ``client_order_id`` is a fresh venue order id, so its fill routes back to the
    leg's ``compensated_signed_qty`` by command identity. ``idempotency_key``
    defaults to a unique per-order key; pass the real
    ``paired:{group}:{leg}:flatten:{seq}`` form to make the flatten the leg's
    CURRENT-``compensation_seq`` order, the one terminal-event routing
    (``_current_flatten_is_terminal``) treats as live.
    """
    async with repo.session() as session:
        session.add(
            TradeCommand(
                public_id=public_id,
                command_type="submit",
                shard_key="kraken:BTC-USD:live",
                exchange="kraken",
                instrument="BTC-USD",
                mode="live",
                strategy_id="engine",
                client_order_id=client_order_id,
                venue_client_id=client_order_id,
                side=side,
                order_type="market",
                quantity=quantity,
                price=None,
                reduce_only=True,
                status=TradeCommandStatusEnum.CREATED.value,
                created_at=now,
                correlation_id=correlation_id,
                supersedes_command_id=supersedes_command_id,
                idempotency_key=(
                    f"flatten:{client_order_id}" if idempotency_key is None else idempotency_key
                ),
                wallet_public_id="",
                session_id=_SESSION_ID,
                sequence_id=1,
                timestamp=now,
            )
        )
        await session.commit()


@pytest.mark.asyncio
async def test_compensation_fill_flattens_fully_compensated_long_leg(
    _repo: SQLAlchemyRepository,
) -> None:
    """A long leg whose sell-flatten fully fills lands compensated positive and FLATTENED.

    Given: a compensating long leg (filled_signed_qty=+10) with a reduce-only
        SELL flatten order that filled cumulatively 10,
    When: project_paired_execution_compensation_fill routes the flatten fill by
        command identity,
    Then: it returns COMPENSATION_APPLIED, compensated_signed_qty becomes +10
        (negated sell fill, so open_qty = filled − compensated = 0) and the leg
        is FLATTENED, while filled_signed_qty is preserved.
    """
    leg_id = _pid(300)
    await _insert_created_command(_repo, public_id=_pid(700), correlation_id=_pid(100))
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=leg_id,
            side="buy",
            command_public_id=_pid(700),
            client_order_id="orig-cid",
            status=PairedExecutionLegStatusEnum.COMPENSATING.value,
            filled_signed_qty=10.0,
        )
    )
    await _insert_flatten_command(
        _repo,
        public_id=_pid(800),
        supersedes_command_id=_pid(700),
        client_order_id="flat-1",
        side="sell",
    )
    await _insert_fill_event(_repo, client_order_id="flat-1", cum_fill_size=10.0, side="sell")
    result = await _repo.project_paired_execution_compensation_fill(
        "flat-1", _T1, _NEXT_SESSION_ID, 2
    )
    assert result == PairedFillProjection.COMPENSATION_APPLIED
    active = await _active_leg_versions(_repo, leg_id, _T2)
    assert len(active) == 1
    assert active[0].compensated_signed_qty == 10.0
    assert active[0].filled_signed_qty == 10.0
    assert active[0].status == PairedExecutionLegStatusEnum.FLATTENED.value


@pytest.mark.asyncio
async def test_compensation_fill_flattens_fully_compensated_short_leg(
    _repo: SQLAlchemyRepository,
) -> None:
    """A short leg whose buy-flatten fully fills lands compensated negative and FLATTENED.

    Given: a compensating short leg (filled_signed_qty=−10) with a reduce-only
        BUY flatten order that filled cumulatively 10,
    When: the compensation fill is routed,
    Then: compensated_signed_qty becomes −10 (negated buy fill), open_qty =
        −10 − (−10) = 0 and the leg is FLATTENED — the sign mirror of the long
        case.
    """
    leg_id = _pid(301)
    await _insert_created_command(_repo, public_id=_pid(701), correlation_id=_pid(100))
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=leg_id,
            side="sell",
            command_public_id=_pid(701),
            client_order_id="orig-cid-s",
            status=PairedExecutionLegStatusEnum.COMPENSATING.value,
            filled_signed_qty=-10.0,
        )
    )
    await _insert_flatten_command(
        _repo,
        public_id=_pid(801),
        supersedes_command_id=_pid(701),
        client_order_id="flat-s",
        side="buy",
    )
    await _insert_fill_event(_repo, client_order_id="flat-s", cum_fill_size=10.0, side="buy")
    result = await _repo.project_paired_execution_compensation_fill(
        "flat-s", _T1, _NEXT_SESSION_ID, 2
    )
    assert result == PairedFillProjection.COMPENSATION_APPLIED
    active = await _active_leg_versions(_repo, leg_id, _T2)
    assert active[0].compensated_signed_qty == -10.0
    assert active[0].status == PairedExecutionLegStatusEnum.FLATTENED.value


@pytest.mark.asyncio
async def test_compensation_fill_partial_stays_compensating(_repo: SQLAlchemyRepository) -> None:
    """A partially-filled flatten leaves residual exposure and the leg COMPENSATING.

    Given: a compensating long leg (filled=+10) whose sell-flatten filled only 4,
    When: the compensation fill is routed,
    Then: compensated_signed_qty is +4 (open residual +6) so the leg stays
        COMPENSATING — completion waits until the residual collapses to zero.
    """
    leg_id = _pid(302)
    await _insert_created_command(_repo, public_id=_pid(702), correlation_id=_pid(100))
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=leg_id,
            side="buy",
            command_public_id=_pid(702),
            client_order_id="orig-p",
            status=PairedExecutionLegStatusEnum.COMPENSATING.value,
            filled_signed_qty=10.0,
        )
    )
    await _insert_flatten_command(
        _repo,
        public_id=_pid(802),
        supersedes_command_id=_pid(702),
        client_order_id="flat-p",
        side="sell",
    )
    await _insert_fill_event(_repo, client_order_id="flat-p", cum_fill_size=4.0, side="sell")
    result = await _repo.project_paired_execution_compensation_fill(
        "flat-p", _T1, _NEXT_SESSION_ID, 2
    )
    assert result == PairedFillProjection.COMPENSATION_APPLIED
    active = await _active_leg_versions(_repo, leg_id, _T2)
    assert active[0].compensated_signed_qty == 4.0
    assert active[0].status == PairedExecutionLegStatusEnum.COMPENSATING.value


@pytest.mark.asyncio
async def test_compensation_fill_sums_multiple_flatten_orders_additively(
    _repo: SQLAlchemyRepository,
) -> None:
    """Compensated accumulates across re-compensation rounds, not just the last order.

    Given: a compensating long leg (filled=+10) with TWO reduce-only sell-flatten
        orders (one filled 6, a second round filled 4) plus a third flatten order
        with no fills,
    When: the compensation fill for the second order is routed,
    Then: compensated_signed_qty is the additive sum +10 (6 + 4; the unfilled
        order contributes nothing) and the leg is FLATTENED — proving a single
        monotonic scalar (which would ignore round 2's restart-at-zero cumulative)
        is not used.
    """
    leg_id = _pid(303)
    await _insert_created_command(_repo, public_id=_pid(703), correlation_id=_pid(100))
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=leg_id,
            side="buy",
            command_public_id=_pid(703),
            client_order_id="orig-m",
            status=PairedExecutionLegStatusEnum.COMPENSATING.value,
            filled_signed_qty=10.0,
        )
    )
    await _insert_flatten_command(
        _repo,
        public_id=_pid(803),
        supersedes_command_id=_pid(703),
        client_order_id="flat-m1",
        side="sell",
    )
    await _insert_flatten_command(
        _repo,
        public_id=_pid(804),
        supersedes_command_id=_pid(703),
        client_order_id="flat-m2",
        side="sell",
    )
    await _insert_flatten_command(
        _repo,
        public_id=_pid(805),
        supersedes_command_id=_pid(703),
        client_order_id="flat-m3",
        side="sell",
    )
    await _insert_fill_event(_repo, client_order_id="flat-m1", cum_fill_size=6.0, side="sell")
    await _insert_fill_event(_repo, client_order_id="flat-m2", cum_fill_size=4.0, side="sell")
    result = await _repo.project_paired_execution_compensation_fill(
        "flat-m2", _T1, _NEXT_SESSION_ID, 2
    )
    assert result == PairedFillProjection.COMPENSATION_APPLIED
    active = await _active_leg_versions(_repo, leg_id, _T2)
    assert active[0].compensated_signed_qty == 10.0
    assert active[0].status == PairedExecutionLegStatusEnum.FLATTENED.value


@pytest.mark.asyncio
async def test_compensation_fill_sign_uses_command_side_not_venue_side(
    _repo: SQLAlchemyRepository,
) -> None:
    """The compensation sign follows the COMMAND side, not a venue-event echo.

    Given: a compensating long leg (filled=+10) with a SELL flatten command whose
        venue fill event is mislabelled side='buy',
    When: the compensation fill is routed,
    Then: compensated_signed_qty is +10 (from the command's SELL side), not −10 —
        a mislabelled venue echo cannot invert the accounting.
    """
    leg_id = _pid(304)
    await _insert_created_command(_repo, public_id=_pid(706), correlation_id=_pid(100))
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=leg_id,
            side="buy",
            command_public_id=_pid(706),
            client_order_id="orig-sg",
            status=PairedExecutionLegStatusEnum.COMPENSATING.value,
            filled_signed_qty=10.0,
        )
    )
    await _insert_flatten_command(
        _repo,
        public_id=_pid(806),
        supersedes_command_id=_pid(706),
        client_order_id="flat-sg",
        side="sell",
    )
    await _insert_fill_event(_repo, client_order_id="flat-sg", cum_fill_size=10.0, side="buy")
    result = await _repo.project_paired_execution_compensation_fill(
        "flat-sg", _T1, _NEXT_SESSION_ID, 2
    )
    assert result == PairedFillProjection.COMPENSATION_APPLIED
    active = await _active_leg_versions(_repo, leg_id, _T2)
    assert active[0].compensated_signed_qty == 10.0


@pytest.mark.asyncio
async def test_compensation_fill_is_idempotent_noop_on_replay(
    _repo: SQLAlchemyRepository,
) -> None:
    """A re-delivered flatten fill recomputes the same value and writes no successor.

    Given: a leg already FLATTENED with compensated=+10 from a prior projection,
    When: the same flatten fill is routed again,
    Then: it returns COMPENSATION_NOOP and no new SCD2 version is written — the
        recompute is replay-safe.
    """
    leg_id = _pid(305)
    await _insert_created_command(_repo, public_id=_pid(707), correlation_id=_pid(100))
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=leg_id,
            side="buy",
            command_public_id=_pid(707),
            client_order_id="orig-i",
            status=PairedExecutionLegStatusEnum.FLATTENED.value,
            filled_signed_qty=10.0,
            compensated_signed_qty=10.0,
        )
    )
    await _insert_flatten_command(
        _repo,
        public_id=_pid(807),
        supersedes_command_id=_pid(707),
        client_order_id="flat-i",
        side="sell",
    )
    await _insert_fill_event(_repo, client_order_id="flat-i", cum_fill_size=10.0, side="sell")
    result = await _repo.project_paired_execution_compensation_fill(
        "flat-i", _T1, _NEXT_SESSION_ID, 2
    )
    assert result == PairedFillProjection.COMPENSATION_NOOP
    active = await _active_leg_versions(_repo, leg_id, _T2)
    assert len(active) == 1
    assert active[0].timestamp == _T0


@pytest.mark.asyncio
async def test_compensation_fill_preserves_manual_intervention_status(
    _repo: SQLAlchemyRepository,
) -> None:
    """A flatten fill records compensated but never auto-clears manual_intervention.

    Given: a manual_intervention long leg (filled=+10) whose sell-flatten filled 10,
    When: the compensation fill is routed,
    Then: compensated_signed_qty is updated to +10 (authoritative accounting) but
        the status STAYS manual_intervention — auto-flatten must not override an
        operator escalation.
    """
    leg_id = _pid(306)
    await _insert_created_command(_repo, public_id=_pid(708), correlation_id=_pid(100))
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=leg_id,
            side="buy",
            command_public_id=_pid(708),
            client_order_id="orig-mi",
            status=PairedExecutionLegStatusEnum.MANUAL_INTERVENTION.value,
            filled_signed_qty=10.0,
        )
    )
    await _insert_flatten_command(
        _repo,
        public_id=_pid(808),
        supersedes_command_id=_pid(708),
        client_order_id="flat-mi",
        side="sell",
    )
    await _insert_fill_event(_repo, client_order_id="flat-mi", cum_fill_size=10.0, side="sell")
    result = await _repo.project_paired_execution_compensation_fill(
        "flat-mi", _T1, _NEXT_SESSION_ID, 2
    )
    assert result == PairedFillProjection.COMPENSATION_APPLIED
    active = await _active_leg_versions(_repo, leg_id, _T2)
    assert active[0].compensated_signed_qty == 10.0
    assert active[0].status == PairedExecutionLegStatusEnum.MANUAL_INTERVENTION.value


@pytest.mark.asyncio
async def test_compensation_fill_no_match_when_no_flatten_command(
    _repo: SQLAlchemyRepository,
) -> None:
    """A fill whose client_order_id is no reduce-only flatten command is NO_MATCH.

    Given: no flatten command bound to the fill's client_order_id,
    When: the compensation projection runs,
    Then: it returns NO_MATCH so an ordinary fill is harmlessly ignored.
    """
    result = await _repo.project_paired_execution_compensation_fill(
        "absent", _T1, _NEXT_SESSION_ID, 2
    )
    assert result == PairedFillProjection.NO_MATCH


@pytest.mark.asyncio
async def test_compensation_fill_no_match_when_superseded_command_is_not_a_leg(
    _repo: SQLAlchemyRepository,
) -> None:
    """A flatten superseding a non-leg command resolves no leg and is NO_MATCH.

    Given: a reduce-only flatten command whose supersedes_command_id matches no
        leg's command_public_id,
    When: the compensation projection runs,
    Then: it returns NO_MATCH without writing anything.
    """
    await _insert_flatten_command(
        _repo,
        public_id=_pid(809),
        supersedes_command_id=_pid(999),
        client_order_id="flat-orphan",
        side="sell",
    )
    await _insert_fill_event(_repo, client_order_id="flat-orphan", cum_fill_size=5.0, side="sell")
    result = await _repo.project_paired_execution_compensation_fill(
        "flat-orphan", _T1, _NEXT_SESSION_ID, 2
    )
    assert result == PairedFillProjection.NO_MATCH


@pytest.mark.asyncio
async def test_compensation_fill_raises_on_duplicate_active_flatten_command(
    _repo: SQLAlchemyRepository,
) -> None:
    """Two active flatten commands sharing the flatten client_order_id is a hard error.

    Given: two active reduce-only flatten commands with the same client_order_id,
    When: the compensation projection resolves the command,
    Then: it raises ValueError rather than silently picking one — a data-integrity
        guard mirroring the leg-fill duplicate guard.
    """
    await _insert_flatten_command(
        _repo,
        public_id=_pid(810),
        supersedes_command_id=_pid(700),
        client_order_id="flat-dup",
        side="sell",
    )
    async with _repo.session() as session:
        session.add(
            TradeCommand(
                public_id=_pid(811),
                command_type="submit",
                shard_key="kraken:BTC-USD:live",
                exchange="kraken",
                instrument="BTC-USD",
                mode="live",
                strategy_id="engine",
                client_order_id="flat-dup",
                venue_client_id="flat-dup",
                side="sell",
                order_type="market",
                quantity=1.0,
                price=None,
                reduce_only=True,
                status=TradeCommandStatusEnum.CREATED.value,
                created_at=_T0,
                correlation_id=_pid(100),
                supersedes_command_id=_pid(700),
                idempotency_key="flatten:flat-dup-2",
                wallet_public_id="",
                session_id=_SESSION_ID,
                sequence_id=1,
                timestamp=_T0,
            )
        )
        await session.commit()
    with pytest.raises(ValueError, match="active flatten commands share"):
        await _repo.project_paired_execution_compensation_fill("flat-dup", _T1, _NEXT_SESSION_ID, 2)


@pytest.mark.asyncio
async def test_compensation_fill_clamps_bus_time_for_future_stamped_leg(
    _repo: SQLAlchemyRepository,
) -> None:
    """A future-stamped compensating leg keeps a non-inverted SCD2 interval.

    Given: a compensating leg whose timestamp is in the future (a sibling
        coordinator's clock ran ahead),
    When: the compensation fill is routed with an EARLIER bus_time,
    Then: the close / successor bus time is clamped to the leg's timestamp so the
        predecessor known_to is never set before its timestamp and the successor
        is never backdated — the same clamp the fill projection uses.
    """
    future = _T0 + timedelta(seconds=10)
    leg_id = _pid(309)
    await _insert_created_command(_repo, public_id=_pid(713), correlation_id=_pid(100))
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=leg_id,
            side="buy",
            command_public_id=_pid(713),
            client_order_id="orig-fut",
            status=PairedExecutionLegStatusEnum.COMPENSATING.value,
            filled_signed_qty=10.0,
            timestamp=future,
            created_at=future,
        )
    )
    await _insert_flatten_command(
        _repo,
        public_id=_pid(813),
        supersedes_command_id=_pid(713),
        client_order_id="flat-fut",
        side="sell",
    )
    await _insert_fill_event(_repo, client_order_id="flat-fut", cum_fill_size=10.0, side="sell")
    result = await _repo.project_paired_execution_compensation_fill(
        "flat-fut", _T1, _NEXT_SESSION_ID, 2
    )
    assert result == PairedFillProjection.COMPENSATION_APPLIED
    async with _repo.session() as session:
        versions = list(
            (
                await session.execute(
                    select(PairedExecutionLeg)
                    .where(PairedExecutionLeg.public_id == leg_id)
                    .order_by(PairedExecutionLeg.id)
                )
            )
            .scalars()
            .all()
        )
    assert len(versions) == 2
    predecessor, successor = versions
    assert predecessor.known_to == future
    assert predecessor.known_to >= predecessor.timestamp
    assert successor.timestamp == future
    assert successor.known_to == KNOWN_TO_MAX


@pytest.mark.asyncio
async def test_reproject_leg_compensation_recovers_downtime_flatten_fill(
    _repo: SQLAlchemyRepository,
) -> None:
    """Recovery re-derives a leg's compensated qty from venue_events on restart.

    Given: a compensating long leg recovered with a STALE compensated=0 while its
        sell-flatten order had actually filled 10 during downtime,
    When: reproject_paired_execution_leg_compensation runs for the leg,
    Then: it returns COMPENSATION_APPLIED, restores compensated=+10 from the
        authoritative venue_events and FLATTENS the leg — recovery parity for the
        compensation path.
    """
    leg_id = _pid(310)
    await _insert_created_command(_repo, public_id=_pid(714), correlation_id=_pid(100))
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=leg_id,
            side="buy",
            command_public_id=_pid(714),
            client_order_id="orig-rec",
            status=PairedExecutionLegStatusEnum.COMPENSATING.value,
            filled_signed_qty=10.0,
        )
    )
    await _insert_flatten_command(
        _repo,
        public_id=_pid(814),
        supersedes_command_id=_pid(714),
        client_order_id="flat-rec",
        side="sell",
    )
    await _insert_fill_event(_repo, client_order_id="flat-rec", cum_fill_size=10.0, side="sell")
    result = await _repo.reproject_paired_execution_leg_compensation(
        leg_id, _T1, _NEXT_SESSION_ID, 2
    )
    assert result == PairedFillProjection.COMPENSATION_APPLIED
    active = await _active_leg_versions(_repo, leg_id, _T2)
    assert active[0].compensated_signed_qty == 10.0
    assert active[0].status == PairedExecutionLegStatusEnum.FLATTENED.value


@pytest.mark.asyncio
async def test_reproject_leg_compensation_no_match_when_leg_absent(
    _repo: SQLAlchemyRepository,
) -> None:
    """Recovery for a vanished leg is NO_MATCH without error."""
    result = await _repo.reproject_paired_execution_leg_compensation(
        _pid(999), _T1, _NEXT_SESSION_ID, 2
    )
    assert result == PairedFillProjection.NO_MATCH


@pytest.mark.asyncio
async def test_reproject_leg_compensation_noop_when_leg_has_no_command(
    _repo: SQLAlchemyRepository,
) -> None:
    """A leg with no bound command has no flatten orders, so recovery is a NOOP.

    Given: a compensating leg whose command_public_id is None (no original
        command, so no flatten orders can supersede it),
    When: reproject_paired_execution_leg_compensation runs,
    Then: it returns COMPENSATION_NOOP and writes no successor.
    """
    leg_id = _pid(311)
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=leg_id,
            side="buy",
            command_public_id=None,
            client_order_id="orig-nocmd",
            status=PairedExecutionLegStatusEnum.COMPENSATING.value,
            filled_signed_qty=10.0,
        )
    )
    result = await _repo.reproject_paired_execution_leg_compensation(
        leg_id, _T1, _NEXT_SESSION_ID, 2
    )
    assert result == PairedFillProjection.COMPENSATION_NOOP
    active = await _active_leg_versions(_repo, leg_id, _T2)
    assert len(active) == 1
    assert active[0].timestamp == _T0


@pytest.mark.asyncio
async def test_compensation_recompute_raises_on_duplicate_flatten_client_order_id(
    _repo: SQLAlchemyRepository,
) -> None:
    """Two flatten commands sharing a client_order_id fail closed, never double-count.

    Given: a compensating leg with TWO active reduce-only flatten commands that
        supersede its original command and share the SAME client_order_id (a
        corruption — each would read the same venue max-cumulative),
    When: the compensation recompute runs,
    Then: it raises ValueError rather than counting that order's fill twice (which
        would falsely zero open_qty and FLATTEN an exposed leg).
    """
    leg_id = _pid(320)
    await _insert_created_command(_repo, public_id=_pid(720), correlation_id=_pid(100))
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=leg_id,
            side="buy",
            command_public_id=_pid(720),
            client_order_id="orig-dupcid",
            status=PairedExecutionLegStatusEnum.COMPENSATING.value,
            filled_signed_qty=10.0,
        )
    )
    for cmd_pid, idem in ((_pid(820), "a"), (_pid(821), "b")):
        async with _repo.session() as session:
            session.add(
                TradeCommand(
                    public_id=cmd_pid,
                    command_type="submit",
                    shard_key="kraken:BTC-USD:live",
                    exchange="kraken",
                    instrument="BTC-USD",
                    mode="live",
                    strategy_id="engine",
                    client_order_id="flat-shared",
                    venue_client_id="flat-shared",
                    side="sell",
                    order_type="market",
                    quantity=1.0,
                    price=None,
                    reduce_only=True,
                    status=TradeCommandStatusEnum.CREATED.value,
                    created_at=_T0,
                    correlation_id=_pid(100),
                    supersedes_command_id=_pid(720),
                    idempotency_key=f"flatten:flat-shared:{idem}",
                    wallet_public_id="",
                    session_id=_SESSION_ID,
                    sequence_id=1,
                    timestamp=_T0,
                )
            )
            await session.commit()
    await _insert_fill_event(_repo, client_order_id="flat-shared", cum_fill_size=10.0, side="sell")
    with pytest.raises(ValueError, match="duplicate active flatten client_order_id"):
        await _repo.reproject_paired_execution_leg_compensation(leg_id, _T1, _NEXT_SESSION_ID, 2)


@pytest.mark.asyncio
async def test_compensation_recompute_raises_on_unexpected_flatten_side(
    _repo: SQLAlchemyRepository,
) -> None:
    """A flatten command with a non-buy/sell side fails closed.

    Given: a compensating leg whose reduce-only flatten command carries a
        malformed side that BYPASSED the ``ck_trade_commands_side`` DB
        CHECK (the recompute's ValueError is defense-in-depth for a row
        written by raw SQL or predating migration 0010 — the CHECK now
        rejects such a side on normal inserts, so the test plants it with
        ``PRAGMA ignore_check_constraints=ON`` to reach the guard),
    When: the compensation recompute runs,
    Then: it raises ValueError rather than silently treating the side as a buy
        (which would invert the compensation sign).
    """
    leg_id = _pid(321)
    await _insert_created_command(_repo, public_id=_pid(721), correlation_id=_pid(100))
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=leg_id,
            side="buy",
            command_public_id=_pid(721),
            client_order_id="orig-badside",
            status=PairedExecutionLegStatusEnum.COMPENSATING.value,
            filled_signed_qty=10.0,
        )
    )
    async with _repo.session() as session:
        await session.execute(text("PRAGMA ignore_check_constraints=ON"))
        session.add(
            TradeCommand(
                public_id=_pid(822),
                command_type="submit",
                shard_key="kraken:BTC-USD:live",
                exchange="kraken",
                instrument="BTC-USD",
                mode="live",
                strategy_id="engine",
                client_order_id="flat-bad",
                venue_client_id="flat-bad",
                side="hold",
                order_type="market",
                quantity=1.0,
                price=None,
                reduce_only=True,
                status=TradeCommandStatusEnum.CREATED.value,
                created_at=_T0,
                correlation_id=_pid(100),
                supersedes_command_id=_pid(721),
                idempotency_key="flatten:flat-bad",
                wallet_public_id="",
                session_id=_SESSION_ID,
                sequence_id=1,
                timestamp=_T0,
            )
        )
        await session.commit()
    with pytest.raises(ValueError, match="unexpected side"):
        await _repo.reproject_paired_execution_leg_compensation(leg_id, _T1, _NEXT_SESSION_ID, 2)


_DEFAULT_GROUP_PID = "00000000-0000-0000-0000-000000000100"


def _current_seq_key(leg_public_id: str, seq: int) -> str:
    """Build the flatten idempotency key the leg's CURRENT compensation_seq expects."""
    return f"paired:{_DEFAULT_GROUP_PID}:{leg_public_id}:flatten:{seq}"


@pytest.mark.asyncio
async def test_compensation_reopens_leg_when_current_flatten_fully_fills_with_residual(
    _repo: SQLAlchemyRepository,
) -> None:
    """A fully-filled current flatten with residual reopens the leg for the next round.

    Given: a compensating long leg whose exposure grew to +15 (a late original
        fill) while its CURRENT-seq sell-flatten (quantity 10) fully filled 10,
    When: the compensation projection settles the leg,
    Then: compensated is +10, open residual is +5, and because the current flatten
        is terminal (filled to its quantity) the leg reopens to FILLED so the sweep
        re-flattens the residual on the next compensation_seq.
    """
    leg_id = _pid(330)
    await _insert_created_command(_repo, public_id=_pid(730), correlation_id=_pid(100))
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=leg_id,
            side="buy",
            command_public_id=_pid(730),
            client_order_id="orig-reopen",
            status=PairedExecutionLegStatusEnum.COMPENSATING.value,
            filled_signed_qty=15.0,
            compensation_seq=1,
        )
    )
    await _insert_flatten_command(
        _repo,
        public_id=_pid(830),
        supersedes_command_id=_pid(730),
        client_order_id="flat-r1",
        side="sell",
        quantity=10.0,
        idempotency_key=_current_seq_key(leg_id, 1),
    )
    await _insert_fill_event(_repo, client_order_id="flat-r1", cum_fill_size=10.0, side="sell")
    result = await _repo.reproject_paired_execution_leg_compensation(
        leg_id, _T1, _NEXT_SESSION_ID, 2
    )
    assert result == PairedFillProjection.COMPENSATION_APPLIED
    active = await _active_leg_versions(_repo, leg_id, _T2)
    assert active[0].compensated_signed_qty == 10.0
    assert active[0].status == PairedExecutionLegStatusEnum.FILLED.value


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("event_type", "status"),
    [
        ("order_terminal", "cancelled"),
        ("order_rejected", "rejected"),
        ("order_breaker_open", "failed"),
        ("order_interlock_blocked", "failed"),
    ],
)
async def test_compensation_reopens_leg_when_current_flatten_aborts_with_residual(
    _repo: SQLAlchemyRepository, event_type: str, status: str
) -> None:
    """A cancelled / rejected current flatten leaving residual reopens the leg.

    Given: a compensating long leg (filled +10) whose CURRENT-seq sell-flatten
        (quantity 10) only partially filled 4 before the venue cancelled / rejected
        it (a durable order_terminal / order_rejected event),
    When: the compensation projection settles the leg,
    Then: compensated is +4, residual +6 remains, and because the current flatten
        terminalized the leg reopens to FILLED for the next round.
    """
    leg_id = _pid(331)
    await _insert_created_command(_repo, public_id=_pid(731), correlation_id=_pid(100))
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=leg_id,
            side="buy",
            command_public_id=_pid(731),
            client_order_id="orig-abort",
            status=PairedExecutionLegStatusEnum.COMPENSATING.value,
            filled_signed_qty=10.0,
            compensation_seq=1,
        )
    )
    await _insert_flatten_command(
        _repo,
        public_id=_pid(831),
        supersedes_command_id=_pid(731),
        client_order_id="flat-abort",
        side="sell",
        quantity=10.0,
        idempotency_key=_current_seq_key(leg_id, 1),
    )
    await _insert_fill_event(_repo, client_order_id="flat-abort", cum_fill_size=4.0, side="sell")
    await _insert_fill_event(
        _repo,
        client_order_id="flat-abort",
        cum_fill_size=None,
        event_type=event_type,
        status=status,
        side="sell",
    )
    result = await _repo.reproject_paired_execution_leg_compensation(
        leg_id, _T1, _NEXT_SESSION_ID, 2
    )
    assert result == PairedFillProjection.COMPENSATION_APPLIED
    active = await _active_leg_versions(_repo, leg_id, _T2)
    assert active[0].compensated_signed_qty == 4.0
    assert active[0].status == PairedExecutionLegStatusEnum.FILLED.value


@pytest.mark.asyncio
async def test_compensation_partial_current_flatten_in_flight_stays_compensating(
    _repo: SQLAlchemyRepository,
) -> None:
    """A partially-filled, still-live current flatten keeps the leg compensating.

    Given: a compensating long leg (filled +10) whose CURRENT-seq sell-flatten
        (quantity 10) has only partially filled 4 and is NOT terminal (no
        cancel/expire/reject event),
    When: the compensation projection settles the leg,
    Then: compensated is +4 (open residual +6) but the leg stays COMPENSATING —
        re-flattening now would double-flatten the in-flight order.
    """
    leg_id = _pid(332)
    await _insert_created_command(_repo, public_id=_pid(732), correlation_id=_pid(100))
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=leg_id,
            side="buy",
            command_public_id=_pid(732),
            client_order_id="orig-inflight",
            status=PairedExecutionLegStatusEnum.COMPENSATING.value,
            filled_signed_qty=10.0,
            compensation_seq=1,
        )
    )
    await _insert_flatten_command(
        _repo,
        public_id=_pid(832),
        supersedes_command_id=_pid(732),
        client_order_id="flat-inflight",
        side="sell",
        quantity=10.0,
        idempotency_key=_current_seq_key(leg_id, 1),
    )
    await _insert_fill_event(_repo, client_order_id="flat-inflight", cum_fill_size=4.0, side="sell")
    result = await _repo.reproject_paired_execution_leg_compensation(
        leg_id, _T1, _NEXT_SESSION_ID, 2
    )
    assert result == PairedFillProjection.COMPENSATION_APPLIED
    active = await _active_leg_versions(_repo, leg_id, _T2)
    assert active[0].compensated_signed_qty == 4.0
    assert active[0].status == PairedExecutionLegStatusEnum.COMPENSATING.value


@pytest.mark.asyncio
async def test_compensation_stale_terminal_does_not_reopen_while_current_seq_in_flight(
    _repo: SQLAlchemyRepository,
) -> None:
    """A terminal for an OLD round never reopens while the current round is in flight.

    Given: a compensating long leg at compensation_seq=2 (filled +20) whose seq-1
        flatten fully filled 10 (terminal) but whose CURRENT seq-2 flatten (quantity
        10) has only partially filled 4 and is still live,
    When: the compensation projection settles the leg,
    Then: compensated is +14 (10+4), residual +6, but because terminality is judged
        on the CURRENT seq-2 flatten (still in flight) the leg stays COMPENSATING —
        the stale seq-1 terminal cannot trigger a double-flatten.
    """
    leg_id = _pid(333)
    await _insert_created_command(_repo, public_id=_pid(733), correlation_id=_pid(100))
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=leg_id,
            side="buy",
            command_public_id=_pid(733),
            client_order_id="orig-stale",
            status=PairedExecutionLegStatusEnum.COMPENSATING.value,
            filled_signed_qty=20.0,
            compensation_seq=2,
        )
    )
    await _insert_flatten_command(
        _repo,
        public_id=_pid(834),
        supersedes_command_id=_pid(733),
        client_order_id="flat-seq1",
        side="sell",
        quantity=10.0,
        idempotency_key=_current_seq_key(leg_id, 1),
    )
    await _insert_flatten_command(
        _repo,
        public_id=_pid(835),
        supersedes_command_id=_pid(733),
        client_order_id="flat-seq2",
        side="sell",
        quantity=10.0,
        idempotency_key=_current_seq_key(leg_id, 2),
    )
    await _insert_fill_event(_repo, client_order_id="flat-seq1", cum_fill_size=10.0, side="sell")
    await _insert_fill_event(_repo, client_order_id="flat-seq2", cum_fill_size=4.0, side="sell")
    result = await _repo.reproject_paired_execution_leg_compensation(
        leg_id, _T1, _NEXT_SESSION_ID, 2
    )
    assert result == PairedFillProjection.COMPENSATION_APPLIED
    active = await _active_leg_versions(_repo, leg_id, _T2)
    assert active[0].compensated_signed_qty == 14.0
    assert active[0].status == PairedExecutionLegStatusEnum.COMPENSATING.value


async def _seed_completion_group(
    repo: SQLAlchemyRepository,
    *,
    group_id: str,
    status: str,
    leg_statuses: list[tuple[str, float, float]],
    timestamp: datetime = _T0,
) -> None:
    """Seed a 2-leg group for completion-DAL tests.

    ``leg_statuses`` is one ``(status, filled_signed_qty,
    compensated_signed_qty)`` tuple per leg; legs are indexed 0..n-1 on
    BTC-USD / ETH-USD so a 2-leg set matches the default ``group_key``
    (the armed completion predicate validates the full leg set).
    """
    await repo.insert_paired_execution_group(
        _group_insert_row(public_id=group_id, status=status, timestamp=timestamp)
    )
    instruments = ["BTC-USD", "ETH-USD"]
    for index, (leg_status, filled, compensated) in enumerate(leg_statuses):
        await repo.insert_paired_execution_leg(
            _leg_insert_row(
                public_id=f"00000000-0000-0000-0001-{index:012d}",
                group_public_id=group_id,
                leg_index=index,
                instrument=instruments[index],
                shard_key=f"kraken:{instruments[index]}:live",
                command_public_id=f"00000000-0000-0000-0002-{index:012d}",
                client_order_id=f"cid-comp-{index}",
                status=leg_status,
                filled_signed_qty=filled,
                compensated_signed_qty=compensated,
            )
        )


@pytest.mark.asyncio
async def test_complete_group_armed_all_filled_completes(_repo: SQLAlchemyRepository) -> None:
    """An armed group whose complete leg set fully filled completes (happy path).

    Given: an armed 2-leg group whose legs are both FILLED,
    When: complete_paired_execution_group_if_settled runs with expected 'armed',
    Then: the group's active successor is COMPLETED — the succeeded pair leaves
        the in-flight set without any halt logic.
    """
    gid = _pid(400)
    await _seed_completion_group(
        _repo,
        group_id=gid,
        status=PairedExecutionGroupStatusEnum.ARMED.value,
        leg_statuses=[
            (PairedExecutionLegStatusEnum.FILLED.value, 1.0, 0.0),
            (PairedExecutionLegStatusEnum.FILLED.value, -1.0, 0.0),
        ],
    )
    completed = await _repo.complete_paired_execution_group_if_settled(
        gid, PairedExecutionGroupStatusEnum.ARMED.value, _T1, _NEXT_SESSION_ID, 2
    )
    assert completed is True
    group = (await _active_group_versions(_repo, gid, _T2))[0]
    assert group.status == PairedExecutionGroupStatusEnum.COMPLETED.value


@pytest.mark.asyncio
async def test_complete_group_armed_rejects_partial_fill_and_incomplete_set(
    _repo: SQLAlchemyRepository,
) -> None:
    """An armed group with a partial leg or a partial leg SET never completes.

    Given: an armed group whose second leg is only partially filled, and another
        armed group with a single leg (incomplete set for expected_leg_count=2),
    When: the completion DAL runs for each,
    Then: both return False and stay armed — completion can never bless a
        wrong or unfinished leg set (the validated-set check also rejects a
        vacuous set).
    """
    gid = _pid(401)
    await _seed_completion_group(
        _repo,
        group_id=gid,
        status=PairedExecutionGroupStatusEnum.ARMED.value,
        leg_statuses=[
            (PairedExecutionLegStatusEnum.FILLED.value, 1.0, 0.0),
            (PairedExecutionLegStatusEnum.PARTIALLY_FILLED.value, -0.4, 0.0),
        ],
    )
    partial = await _repo.complete_paired_execution_group_if_settled(
        gid, PairedExecutionGroupStatusEnum.ARMED.value, _T1, _NEXT_SESSION_ID, 2
    )
    gid_short = _pid(402)
    await _repo.insert_paired_execution_group(
        _group_insert_row(public_id=gid_short, status=PairedExecutionGroupStatusEnum.ARMED.value)
    )
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=_pid(403),
            group_public_id=gid_short,
            leg_index=0,
            command_public_id=_pid(404),
            client_order_id="cid-short-set",
            status=PairedExecutionLegStatusEnum.FILLED.value,
            filled_signed_qty=1.0,
        )
    )
    incomplete = await _repo.complete_paired_execution_group_if_settled(
        gid_short, PairedExecutionGroupStatusEnum.ARMED.value, _T1, _NEXT_SESSION_ID, 3
    )
    assert partial is False
    assert incomplete is False
    assert (await _active_group_versions(_repo, gid, _T2))[
        0
    ].status == PairedExecutionGroupStatusEnum.ARMED.value
    assert (await _active_group_versions(_repo, gid_short, _T2))[
        0
    ].status == PairedExecutionGroupStatusEnum.ARMED.value


@pytest.mark.asyncio
async def test_complete_group_compensating_settled_mix_completes(
    _repo: SQLAlchemyRepository,
) -> None:
    """A compensating group whose legs all settled at zero open completes.

    Given: a compensating group with one FLATTENED leg (filled=+10,
        compensated=+10) and one FILLED leg whose residual was zeroed by a late
        flatten fill report (filled=-5, compensated=-5 — reachable because only
        COMPENSATING legs transition in the compensation recompute),
    When: the completion DAL runs with expected 'compensating',
    Then: the group completes — both legs are terminal with zero open exposure.
    """
    gid = _pid(405)
    await _seed_completion_group(
        _repo,
        group_id=gid,
        status=PairedExecutionGroupStatusEnum.COMPENSATING.value,
        leg_statuses=[
            (PairedExecutionLegStatusEnum.FLATTENED.value, 10.0, 10.0),
            (PairedExecutionLegStatusEnum.FILLED.value, -5.0, -5.0),
        ],
    )
    completed = await _repo.complete_paired_execution_group_if_settled(
        gid, PairedExecutionGroupStatusEnum.COMPENSATING.value, _T1, _NEXT_SESSION_ID, 2
    )
    assert completed is True
    group = (await _active_group_versions(_repo, gid, _T2))[0]
    assert group.status == PairedExecutionGroupStatusEnum.COMPLETED.value


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("blocking_status", "filled", "compensated"),
    [
        (PairedExecutionLegStatusEnum.COMPENSATING.value, 10.0, 4.0),
        (PairedExecutionLegStatusEnum.PENDING.value, 0.0, 0.0),
        (PairedExecutionLegStatusEnum.MANUAL_INTERVENTION.value, 10.0, 10.0),
        (PairedExecutionLegStatusEnum.FILLED.value, 10.0, 4.0),
    ],
)
async def test_complete_group_compensating_blocked_by_unsettled_leg(
    _repo: SQLAlchemyRepository, blocking_status: str, filled: float, compensated: float
) -> None:
    """Any unsettled leg blocks completion of a compensating group.

    Given: a compensating group with one fully settled flattened leg and one
        blocking leg — a flatten in flight, a pending leg, a manual_intervention
        leg (even at zero open — the operator owns it), or residual open
        exposure on a filled leg,
    When: the completion DAL runs,
    Then: it returns False and the group stays compensating.
    """
    gid = _pid(406)
    await _seed_completion_group(
        _repo,
        group_id=gid,
        status=PairedExecutionGroupStatusEnum.COMPENSATING.value,
        leg_statuses=[
            (PairedExecutionLegStatusEnum.FLATTENED.value, 10.0, 10.0),
            (blocking_status, filled, compensated),
        ],
    )
    completed = await _repo.complete_paired_execution_group_if_settled(
        gid, PairedExecutionGroupStatusEnum.COMPENSATING.value, _T1, _NEXT_SESSION_ID, 2
    )
    assert completed is False
    group = (await _active_group_versions(_repo, gid, _T2))[0]
    assert group.status == PairedExecutionGroupStatusEnum.COMPENSATING.value


@pytest.mark.asyncio
async def test_complete_group_broken_zero_exposure_completes(
    _repo: SQLAlchemyRepository,
) -> None:
    """A broken group whose legs were all cancelled at zero fill completes.

    Given: a broken group (e.g. an assembly-timeout break) whose two legs are
        both CANCELLED with zero fills,
    When: the completion DAL runs with expected 'broken',
    Then: the group completes, so a fully resolved zero-exposure break stops
        bloating every scanner listing and its pair scope quiets.
    """
    gid = _pid(407)
    await _seed_completion_group(
        _repo,
        group_id=gid,
        status=PairedExecutionGroupStatusEnum.BROKEN.value,
        leg_statuses=[
            (PairedExecutionLegStatusEnum.CANCELLED.value, 0.0, 0.0),
            (PairedExecutionLegStatusEnum.CANCELLED.value, 0.0, 0.0),
        ],
    )
    completed = await _repo.complete_paired_execution_group_if_settled(
        gid, PairedExecutionGroupStatusEnum.BROKEN.value, _T1, _NEXT_SESSION_ID, 2
    )
    assert completed is True
    group = (await _active_group_versions(_repo, gid, _T2))[0]
    assert group.status == PairedExecutionGroupStatusEnum.COMPLETED.value


@pytest.mark.asyncio
async def test_complete_group_guards_vacuous_held_and_status_mismatch(
    _repo: SQLAlchemyRepository,
) -> None:
    """No-leg groups, held created commands and status mismatches block completion.

    Given: a broken group with NO legs (corruption — never vacuously completed),
        a settled broken group with a HELD created command carrying its
        correlation_id (completing would strand the command in the outbox
        backlog forever), and a settled group whose status changed since it was
        observed,
    When: the completion DAL runs for each,
    Then: every call returns False, and an expected_status outside
        armed/broken/compensating raises ValueError.
    """
    gid_empty = _pid(408)
    await _repo.insert_paired_execution_group(
        _group_insert_row(public_id=gid_empty, status=PairedExecutionGroupStatusEnum.BROKEN.value)
    )
    vacuous = await _repo.complete_paired_execution_group_if_settled(
        gid_empty, PairedExecutionGroupStatusEnum.BROKEN.value, _T1, _NEXT_SESSION_ID, 2
    )
    gid_held = _pid(409)
    await _seed_completion_group(
        _repo,
        group_id=gid_held,
        status=PairedExecutionGroupStatusEnum.BROKEN.value,
        leg_statuses=[
            (PairedExecutionLegStatusEnum.CANCELLED.value, 0.0, 0.0),
            (PairedExecutionLegStatusEnum.CANCELLED.value, 0.0, 0.0),
        ],
    )
    await _insert_created_command(_repo, public_id=_pid(410), correlation_id=gid_held)
    held = await _repo.complete_paired_execution_group_if_settled(
        gid_held, PairedExecutionGroupStatusEnum.BROKEN.value, _T1, _NEXT_SESSION_ID, 3
    )
    mismatch = await _repo.complete_paired_execution_group_if_settled(
        gid_held, PairedExecutionGroupStatusEnum.COMPENSATING.value, _T1, _NEXT_SESSION_ID, 4
    )
    assert vacuous is False
    assert held is False
    assert mismatch is False
    with pytest.raises(ValueError, match="not allowed"):
        await _repo.complete_paired_execution_group_if_settled(
            gid_held,
            PairedExecutionGroupStatusEnum.MANUAL_INTERVENTION.value,
            _T1,
            _NEXT_SESSION_ID,
            5,
        )


@pytest.mark.asyncio
async def test_complete_group_clamps_bus_time_for_future_stamped_group(
    _repo: SQLAlchemyRepository,
) -> None:
    """A future-stamped group keeps a non-inverted SCD2 interval on completion.

    Given: a settled compensating group whose timestamp is ahead of the scan
        clock (sibling clock skew),
    When: the completion DAL runs with an earlier bus_time,
    Then: the close / successor time is clamped to the group's own timestamp so
        the predecessor known_to never precedes its timestamp.
    """
    future = _T0 + timedelta(seconds=30)
    gid = _pid(411)
    await _seed_completion_group(
        _repo,
        group_id=gid,
        status=PairedExecutionGroupStatusEnum.COMPENSATING.value,
        leg_statuses=[
            (PairedExecutionLegStatusEnum.FLATTENED.value, 1.0, 1.0),
            (PairedExecutionLegStatusEnum.CANCELLED.value, 0.0, 0.0),
        ],
        timestamp=future,
    )
    completed = await _repo.complete_paired_execution_group_if_settled(
        gid, PairedExecutionGroupStatusEnum.COMPENSATING.value, _T1, _NEXT_SESSION_ID, 2
    )
    assert completed is True
    async with _repo.session() as session:
        versions = list(
            (
                await session.execute(
                    select(PairedExecutionGroup)
                    .where(PairedExecutionGroup.public_id == gid)
                    .order_by(PairedExecutionGroup.id)
                )
            )
            .scalars()
            .all()
        )
    predecessor, successor = versions
    assert predecessor.known_to == future
    assert predecessor.known_to >= predecessor.timestamp
    assert successor.timestamp == future
    assert successor.known_to == KNOWN_TO_MAX


@pytest.mark.asyncio
async def test_clear_halt_if_scope_quiet_clears_only_quiet_scope(
    _repo: SQLAlchemyRepository,
) -> None:
    """The scope-quiet clear closes a quiet scope's halt and spares an exposed one.

    Given: an active halt on a scope whose only group is COMPLETED, and another
        halt on a scope that still carries a BROKEN group,
    When: clear_paired_execution_halt_if_scope_quiet runs for each scope,
    Then: the quiet scope's halt is closed (True) while the exposed scope's halt
        survives (False) — a sibling group still failing keeps its protection.
    """
    quiet_gid = _pid(420)
    await _repo.insert_paired_execution_group(
        _group_insert_row(
            public_id=quiet_gid, status=PairedExecutionGroupStatusEnum.COMPLETED.value
        )
    )
    await _repo.ensure_paired_execution_halt(_halt_insert_row(group_public_id=quiet_gid))
    exposed_gid = _pid(421)
    await _repo.insert_paired_execution_group(
        _group_insert_row(
            public_id=exposed_gid,
            status=PairedExecutionGroupStatusEnum.BROKEN.value,
            group_key=_OTHER_GROUP_KEY,
        )
    )
    await _repo.ensure_paired_execution_halt(
        _halt_insert_row(group_public_id=exposed_gid, group_key=_OTHER_GROUP_KEY)
    )
    quiet = await _repo.clear_paired_execution_halt_if_scope_quiet(
        _WALLET_ID, _STRATEGY_ID, _GROUP_KEY, _T1
    )
    exposed = await _repo.clear_paired_execution_halt_if_scope_quiet(
        _WALLET_ID, _STRATEGY_ID, _OTHER_GROUP_KEY, _T1
    )
    assert quiet is True
    assert exposed is False
    assert (
        await _repo.get_active_paired_execution_halt(_WALLET_ID, _STRATEGY_ID, _GROUP_KEY) is None
    )
    assert (
        await _repo.get_active_paired_execution_halt(_WALLET_ID, _STRATEGY_ID, _OTHER_GROUP_KEY)
        is not None
    )


@pytest.mark.asyncio
async def test_clear_halt_if_scope_quiet_without_halt_is_false_and_clamps(
    _repo: SQLAlchemyRepository,
) -> None:
    """A quiet scope without a halt is False; a future-stamped halt clamps its close.

    Given: a quiet scope with no halt row, then a quiet scope whose halt is
        stamped ahead of the clock,
    When: the scope-quiet clear runs,
    Then: the missing halt returns False, and the future-stamped halt closes at
        its own timestamp (clamped, never inverting the SCD2 interval).
    """
    missing = await _repo.clear_paired_execution_halt_if_scope_quiet(
        _WALLET_ID, _STRATEGY_ID, _GROUP_KEY, _T1
    )
    assert missing is False
    future = _T0 + timedelta(seconds=30)
    await _repo.ensure_paired_execution_halt(
        _halt_insert_row(public_id=_pid(422), timestamp=future)
    )
    cleared = await _repo.clear_paired_execution_halt_if_scope_quiet(
        _WALLET_ID, _STRATEGY_ID, _GROUP_KEY, _T1
    )
    assert cleared is True
    async with _repo.session() as session:
        halt = (
            (
                await session.execute(
                    select(PairedExecutionHalt).where(PairedExecutionHalt.public_id == _pid(422))
                )
            )
            .scalars()
            .one()
        )
    assert halt.known_to == future


@pytest.mark.asyncio
async def test_late_fill_reopens_completed_group_to_compensating(
    _repo: SQLAlchemyRepository,
) -> None:
    """A late original fill with residual reopens a COMPLETED group in one tx.

    Given: a COMPLETED group whose flattened leg (filled=+10, compensated=+10)
        receives a late ORIGINAL fill growing the cumulative to +15,
    When: project_paired_execution_leg_fill runs,
    Then: the leg reopens to FILLED with the residual (+5) and the group's
        active successor is COMPENSATING with a stamped failure_reason — so the
        scanner (which never lists completed groups) sees and re-flattens it.
    """
    gid = _pid(430)
    await _repo.insert_paired_execution_group(
        _group_insert_row(public_id=gid, status=PairedExecutionGroupStatusEnum.COMPLETED.value)
    )
    leg_id = _pid(431)
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=leg_id,
            group_public_id=gid,
            side="buy",
            command_public_id=_pid(432),
            client_order_id="cid-reopen",
            status=PairedExecutionLegStatusEnum.FLATTENED.value,
            filled_signed_qty=10.0,
            compensated_signed_qty=10.0,
        )
    )
    applied = await _repo.project_paired_execution_leg_fill(
        "cid-reopen", 15.0, PairedExecutionLegStatusEnum.FILLED.value, _T1, _NEXT_SESSION_ID, 2
    )
    assert applied == PairedFillProjection.ORIGINAL_APPLIED
    leg = (await _active_leg_versions(_repo, leg_id, _T2))[0]
    assert leg.filled_signed_qty == 15.0
    assert leg.status == PairedExecutionLegStatusEnum.FILLED.value
    group = (await _active_group_versions(_repo, gid, _T2))[0]
    assert group.status == PairedExecutionGroupStatusEnum.COMPENSATING.value
    assert group.failure_reason == "late fill after completion"


@pytest.mark.asyncio
async def test_late_fill_with_zero_residual_keeps_group_completed(
    _repo: SQLAlchemyRepository,
) -> None:
    """A late fill that lands at zero open never reopens a completed group.

    Given: a COMPLETED group whose flattened leg was over-compensated
        (filled=+3, compensated=+5) and a late fill grows filled to exactly +5,
    When: the fill projects,
    Then: the leg accounting updates but open is zero, so the group's active
        row stays COMPLETED with a single version (no successor written).
    """
    gid = _pid(433)
    await _repo.insert_paired_execution_group(
        _group_insert_row(public_id=gid, status=PairedExecutionGroupStatusEnum.COMPLETED.value)
    )
    leg_id = _pid(434)
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=leg_id,
            group_public_id=gid,
            side="buy",
            command_public_id=_pid(435),
            client_order_id="cid-zero-reopen",
            status=PairedExecutionLegStatusEnum.FLATTENED.value,
            filled_signed_qty=3.0,
            compensated_signed_qty=5.0,
        )
    )
    applied = await _repo.project_paired_execution_leg_fill(
        "cid-zero-reopen", 5.0, PairedExecutionLegStatusEnum.FILLED.value, _T1, _NEXT_SESSION_ID, 2
    )
    assert applied == PairedFillProjection.ORIGINAL_APPLIED
    leg = (await _active_leg_versions(_repo, leg_id, _T2))[0]
    assert leg.filled_signed_qty == 5.0
    assert leg.status == PairedExecutionLegStatusEnum.FLATTENED.value
    async with _repo.session() as session:
        versions = list(
            (
                await session.execute(
                    select(PairedExecutionGroup).where(PairedExecutionGroup.public_id == gid)
                )
            )
            .scalars()
            .all()
        )
    assert len(versions) == 1
    assert versions[0].status == PairedExecutionGroupStatusEnum.COMPLETED.value


@pytest.mark.asyncio
async def test_late_fill_on_filled_leg_of_completed_group_reopens(
    _repo: SQLAlchemyRepository,
) -> None:
    """A growing fill on a FILLED (non-terminal-status) leg still reopens the group.

    Given: a COMPLETED group whose FILLED leg sits at zero open (filled=+10,
        compensated=+10) and the original order reports MORE cumulative (+12),
    When: the fill projects,
    Then: the group reopens to COMPENSATING — the reopen triggers on residual
        exposure, not on the leg's status family.
    """
    gid = _pid(436)
    await _repo.insert_paired_execution_group(
        _group_insert_row(public_id=gid, status=PairedExecutionGroupStatusEnum.COMPLETED.value)
    )
    leg_id = _pid(437)
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=leg_id,
            group_public_id=gid,
            side="buy",
            command_public_id=_pid(438),
            client_order_id="cid-filled-reopen",
            status=PairedExecutionLegStatusEnum.FILLED.value,
            filled_signed_qty=10.0,
            compensated_signed_qty=10.0,
        )
    )
    applied = await _repo.project_paired_execution_leg_fill(
        "cid-filled-reopen",
        12.0,
        PairedExecutionLegStatusEnum.FILLED.value,
        _T1,
        _NEXT_SESSION_ID,
        2,
    )
    assert applied == PairedFillProjection.ORIGINAL_APPLIED
    group = (await _active_group_versions(_repo, gid, _T2))[0]
    assert group.status == PairedExecutionGroupStatusEnum.COMPENSATING.value


@pytest.mark.asyncio
async def test_grouped_fill_path_handles_no_longer_completed_and_absent_groups(
    _repo: SQLAlchemyRepository,
) -> None:
    """The group-first path writes the leg even when the group moved on or is gone.

    Given: a leg whose group is COMPENSATING (no longer completed — the fast
        path's detection raced a reopen) and another leg whose group row does
        not exist at all,
    When: the group-first projection path runs directly for each,
    Then: both legs' fills apply without a group successor — the scanner
        already sees a compensating group, and a missing group is corruption
        that must not block fill accounting.
    """
    gid = _pid(440)
    await _repo.insert_paired_execution_group(
        _group_insert_row(public_id=gid, status=PairedExecutionGroupStatusEnum.COMPENSATING.value)
    )
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=_pid(441),
            group_public_id=gid,
            side="buy",
            command_public_id=_pid(442),
            client_order_id="cid-grp-moved",
            status=PairedExecutionLegStatusEnum.FLATTENED.value,
            filled_signed_qty=10.0,
            compensated_signed_qty=10.0,
        )
    )
    moved = await _repo._project_original_fill_grouped(
        "cid-grp-moved",
        15.0,
        PairedExecutionLegStatusEnum.FILLED.value,
        _T1,
        _NEXT_SESSION_ID,
        2,
        None,
        None,
    )
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=_pid(443),
            group_public_id=_pid(449),
            leg_index=1,
            side="buy",
            command_public_id=_pid(444),
            client_order_id="cid-grp-gone",
            status=PairedExecutionLegStatusEnum.FLATTENED.value,
            filled_signed_qty=10.0,
            compensated_signed_qty=10.0,
        )
    )
    gone = await _repo._project_original_fill_grouped(
        "cid-grp-gone",
        15.0,
        PairedExecutionLegStatusEnum.FILLED.value,
        _T1,
        _NEXT_SESSION_ID,
        3,
        None,
        None,
    )
    assert moved == PairedFillProjection.ORIGINAL_APPLIED
    assert gone == PairedFillProjection.ORIGINAL_APPLIED
    group_versions = await _active_group_versions(_repo, gid, _T2)
    assert group_versions[0].status == PairedExecutionGroupStatusEnum.COMPENSATING.value


@pytest.mark.asyncio
async def test_grouped_fill_path_no_match_and_noop_guards(
    _repo: SQLAlchemyRepository,
) -> None:
    """The group-first path repeats the NO_MATCH and monotonic guards under lock.

    Given: no leg for the order id, then a completed group's leg replayed with a
        non-growing cumulative,
    When: the group-first projection path runs directly,
    Then: NO_MATCH and ORIGINAL_NOOP are returned exactly like the fast path.
    """
    absent = await _repo._project_original_fill_grouped(
        "cid-none",
        5.0,
        PairedExecutionLegStatusEnum.FILLED.value,
        _T1,
        _NEXT_SESSION_ID,
        2,
        None,
        None,
    )
    gid = _pid(445)
    await _repo.insert_paired_execution_group(
        _group_insert_row(public_id=gid, status=PairedExecutionGroupStatusEnum.COMPLETED.value)
    )
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=_pid(446),
            group_public_id=gid,
            side="buy",
            command_public_id=_pid(447),
            client_order_id="cid-grp-noop",
            status=PairedExecutionLegStatusEnum.FLATTENED.value,
            filled_signed_qty=10.0,
            compensated_signed_qty=10.0,
        )
    )
    stale = await _repo._project_original_fill_grouped(
        "cid-grp-noop",
        10.0,
        PairedExecutionLegStatusEnum.FILLED.value,
        _T1,
        _NEXT_SESSION_ID,
        3,
        None,
        None,
    )
    assert absent == PairedFillProjection.NO_MATCH
    assert stale == PairedFillProjection.ORIGINAL_NOOP


@pytest.mark.asyncio
async def test_list_recent_completed_groups_windows_by_completion_time(
    _repo: SQLAlchemyRepository,
) -> None:
    """The completed-group listing returns only groups completed inside the window.

    Given: one group completed at T0 and another whose completion successor is
        stamped 10 days earlier,
    When: list_recent_completed_paired_execution_groups runs with a cutoff
        between the two,
    Then: only the recent completion is returned — the bounded recovery window.
    """
    recent = _pid(450)
    await _repo.insert_paired_execution_group(
        _group_insert_row(public_id=recent, status=PairedExecutionGroupStatusEnum.COMPLETED.value)
    )
    old = _pid(451)
    await _repo.insert_paired_execution_group(
        _group_insert_row(
            public_id=old,
            status=PairedExecutionGroupStatusEnum.COMPLETED.value,
            timestamp=_T0 - timedelta(days=10),
            created_at=_T0 - timedelta(days=10),
        )
    )
    rows = await _repo.list_recent_completed_paired_execution_groups(_T0 - timedelta(days=7))
    assert [row["public_id"] for row in rows] == [recent]


@pytest.mark.asyncio
async def test_complete_group_ignores_created_compensation_command(
    _repo: SQLAlchemyRepository,
) -> None:
    """A momentarily-created COMPENSATION command never blocks completion.

    Given: a settled broken group whose correlation_id is carried by a created
        command that SUPERSEDES an original (a compensation cancel / flatten the
        outbox dispatches regardless of group status),
    When: the completion DAL runs,
    Then: the group completes — only a held created ORIGINAL (which the outbox
        gate would strand forever) blocks completion.
    """
    gid = _pid(460)
    await _seed_completion_group(
        _repo,
        group_id=gid,
        status=PairedExecutionGroupStatusEnum.BROKEN.value,
        leg_statuses=[
            (PairedExecutionLegStatusEnum.CANCELLED.value, 0.0, 0.0),
            (PairedExecutionLegStatusEnum.CANCELLED.value, 0.0, 0.0),
        ],
    )
    await _insert_created_command(
        _repo,
        public_id=_pid(461),
        correlation_id=gid,
        supersedes_command_id=_pid(462),
    )
    completed = await _repo.complete_paired_execution_group_if_settled(
        gid, PairedExecutionGroupStatusEnum.BROKEN.value, _T1, _NEXT_SESSION_ID, 2
    )
    assert completed is True
    group = (await _active_group_versions(_repo, gid, _T2))[0]
    assert group.status == PairedExecutionGroupStatusEnum.COMPLETED.value


@pytest.mark.asyncio
async def test_grouped_fill_path_no_match_when_leg_vanishes_under_lock(
    _repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A leg that vanishes between the unlocked peek and the lock is NO_MATCH.

    Given: the group-first projection path whose unlocked peek resolves a leg
        but whose subsequent locked re-read finds it gone (a defensive
        re-validation against concurrent mutation),
    When: the path runs with the lock helper stubbed to that sequence,
    Then: it returns NO_MATCH without writing anything.
    """
    fake_peek = SimpleNamespace(group_public_id=_pid(999))
    monkeypatch.setattr(
        _repo,
        "_lock_leg_by_client_order_id",
        AsyncMock(side_effect=[fake_peek, None]),
    )
    result = await _repo._project_original_fill_grouped(
        "cid-vanish",
        5.0,
        PairedExecutionLegStatusEnum.FILLED.value,
        _T1,
        _NEXT_SESSION_ID,
        2,
        None,
        None,
    )
    assert result == PairedFillProjection.NO_MATCH


@pytest.mark.asyncio
async def test_clear_halt_if_scope_quiet_fails_safe_on_lock_conflict(
    _repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A lock conflict on the scope's group rows aborts the clear fail-safe.

    Given: the scope-group FOR UPDATE NOWAIT read raising a database lock
        error (a concurrent reopen / completion holds a scope group row),
    When: clear_paired_execution_halt_if_scope_quiet runs,
    Then: it returns False without touching the halt — the quiet decision is
        never made on a snapshot a concurrent SCD2 transition is invalidating,
        and the next sweep cycle simply retries.
    """

    @asynccontextmanager
    async def _conflicted_session() -> AsyncIterator[Any]:
        yield SimpleNamespace(
            execute=AsyncMock(side_effect=DBAPIError("stmt", None, Exception("lock not available")))
        )

    monkeypatch.setattr(_repo, "session", _conflicted_session)
    cleared = await _repo.clear_paired_execution_halt_if_scope_quiet(
        _WALLET_ID, _STRATEGY_ID, _GROUP_KEY, _T1
    )
    assert cleared is False


@pytest.mark.asyncio
async def test_terminalize_manual_group_records_attestation(
    _repo: SQLAlchemyRepository,
) -> None:
    """Terminalizing a manual group completes it with the attestation stamp.

    Given: a manual_intervention group with one manual leg carrying real open
        exposure and one cancelled zero-exposure sibling,
    When: terminalize_paired_execution_group runs with an attesting user id,
    Then: the group's active successor is COMPLETED with
        'terminalized_by=<uid>' stamped into failure_reason, while BOTH legs
        keep their true statuses and signed accounting (no book falsification).
    """
    gid = _pid(470)
    await _seed_completion_group(
        _repo,
        group_id=gid,
        status=PairedExecutionGroupStatusEnum.MANUAL_INTERVENTION.value,
        leg_statuses=[
            (PairedExecutionLegStatusEnum.MANUAL_INTERVENTION.value, 10.0, 4.0),
            (PairedExecutionLegStatusEnum.CANCELLED.value, 0.0, 0.0),
        ],
    )
    outcome = await _repo.terminalize_paired_execution_group(
        gid, "user-7", _T1, _NEXT_SESSION_ID, 2
    )
    assert outcome == PairedGroupTerminalizeOutcome.TERMINALIZED
    group = (await _active_group_versions(_repo, gid, _T2))[0]
    assert group.status == PairedExecutionGroupStatusEnum.COMPLETED.value
    assert group.failure_reason is not None
    assert group.failure_reason.endswith("; terminalized_by=user-7")
    legs = await _repo.get_current_paired_execution_legs(gid)
    assert legs[0]["status"] == PairedExecutionLegStatusEnum.MANUAL_INTERVENTION.value
    assert legs[0]["filled_signed_qty"] == 10.0
    assert legs[0]["compensated_signed_qty"] == 4.0


@pytest.mark.asyncio
async def test_terminalize_reopened_compensating_group_with_manual_leg(
    _repo: SQLAlchemyRepository,
) -> None:
    """The re-attestation path: a compensating group with a manual leg terminalizes.

    Given: a compensating group (a late fill reopened a previously terminalized
        group) whose legs are one manual_intervention leg with residual and one
        flattened zero-open sibling,
    When: terminalize runs,
    Then: it returns TERMINALIZED — the operator can attest again without the
        FSM wedging on the manual leg automation can never touch.
    """
    gid = _pid(471)
    await _seed_completion_group(
        _repo,
        group_id=gid,
        status=PairedExecutionGroupStatusEnum.COMPENSATING.value,
        leg_statuses=[
            (PairedExecutionLegStatusEnum.MANUAL_INTERVENTION.value, 15.0, 10.0),
            (PairedExecutionLegStatusEnum.FLATTENED.value, -5.0, -5.0),
        ],
    )
    outcome = await _repo.terminalize_paired_execution_group(
        gid, "user-7", _T1, _NEXT_SESSION_ID, 2
    )
    assert outcome == PairedGroupTerminalizeOutcome.TERMINALIZED
    group = (await _active_group_versions(_repo, gid, _T2))[0]
    assert group.status == PairedExecutionGroupStatusEnum.COMPLETED.value


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("group_status", "leg_statuses"),
    [
        (
            PairedExecutionGroupStatusEnum.ARMED.value,
            [
                (PairedExecutionLegStatusEnum.FILLED.value, 1.0, 0.0),
                (PairedExecutionLegStatusEnum.FILLED.value, -1.0, 0.0),
            ],
        ),
        (
            PairedExecutionGroupStatusEnum.BROKEN.value,
            [
                (PairedExecutionLegStatusEnum.CANCELLED.value, 0.0, 0.0),
                (PairedExecutionLegStatusEnum.CANCELLED.value, 0.0, 0.0),
            ],
        ),
        (
            PairedExecutionGroupStatusEnum.COMPENSATING.value,
            [
                (PairedExecutionLegStatusEnum.COMPENSATING.value, 10.0, 4.0),
                (PairedExecutionLegStatusEnum.FLATTENED.value, -5.0, -5.0),
            ],
        ),
    ],
)
async def test_terminalize_refuses_non_attestable_group_states(
    _repo: SQLAlchemyRepository,
    group_status: str,
    leg_statuses: list[tuple[str, float, float]],
) -> None:
    """Armed, broken and manual-less compensating groups are not attestable.

    Given: a group that is healthy (armed), automation-owned (broken), or
        compensating WITHOUT any manual leg,
    When: terminalize runs,
    Then: NOT_TERMINALIZABLE — the operator surface can never bless a group
        the automation still owns end-to-end.
    """
    gid = _pid(472)
    await _seed_completion_group(
        _repo, group_id=gid, status=group_status, leg_statuses=leg_statuses
    )
    outcome = await _repo.terminalize_paired_execution_group(
        gid, "user-7", _T1, _NEXT_SESSION_ID, 2
    )
    assert outcome == PairedGroupTerminalizeOutcome.NOT_TERMINALIZABLE
    group = (await _active_group_versions(_repo, gid, _T2))[0]
    assert group.status == group_status


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("sibling_status", "sibling_filled", "sibling_compensated"),
    [
        (PairedExecutionLegStatusEnum.COMPENSATING.value, -5.0, -2.0),
        (PairedExecutionLegStatusEnum.FILLED.value, -5.0, 0.0),
        (PairedExecutionLegStatusEnum.PENDING.value, 0.0, 0.0),
    ],
)
async def test_terminalize_refuses_inflight_sibling_leg(
    _repo: SQLAlchemyRepository,
    sibling_status: str,
    sibling_filled: float,
    sibling_compensated: float,
) -> None:
    """A sibling leg with automation in flight blocks the attestation.

    Given: a manual_intervention group whose second leg is still in
        automation's hands — a flatten in flight, a filled leg with residual
        the sweep will flatten, or a pending leg,
    When: terminalize runs,
    Then: NOT_TERMINALIZABLE — the operator must wait for the sibling to
        settle rather than attest over a live order.
    """
    gid = _pid(473)
    await _seed_completion_group(
        _repo,
        group_id=gid,
        status=PairedExecutionGroupStatusEnum.MANUAL_INTERVENTION.value,
        leg_statuses=[
            (PairedExecutionLegStatusEnum.MANUAL_INTERVENTION.value, 10.0, 4.0),
            (sibling_status, sibling_filled, sibling_compensated),
        ],
    )
    outcome = await _repo.terminalize_paired_execution_group(
        gid, "user-7", _T1, _NEXT_SESSION_ID, 2
    )
    assert outcome == PairedGroupTerminalizeOutcome.NOT_TERMINALIZABLE


@pytest.mark.asyncio
async def test_terminalize_refuses_group_without_legs(
    _repo: SQLAlchemyRepository,
) -> None:
    """A manual group with NO current legs is never attestable.

    Given: a manual_intervention group carrying zero legs (corruption),
    When: terminalize runs,
    Then: NOT_TERMINALIZABLE — a vacuous attestation would clear the scope's
        halt while erasing the operator-visible incident.
    """
    gid = _pid(478)
    await _repo.insert_paired_execution_group(
        _group_insert_row(
            public_id=gid,
            status=PairedExecutionGroupStatusEnum.MANUAL_INTERVENTION.value,
        )
    )
    outcome = await _repo.terminalize_paired_execution_group(
        gid, "user-7", _T1, _NEXT_SESSION_ID, 2
    )
    assert outcome == PairedGroupTerminalizeOutcome.NOT_TERMINALIZABLE


@pytest.mark.asyncio
async def test_terminalize_refuses_held_original_command(
    _repo: SQLAlchemyRepository,
) -> None:
    """A held created ORIGINAL command blocks the attestation, mirroring auto-complete.

    Given: an otherwise attestable manual group whose correlation_id is carried
        by a current-active created command with no supersedes (a gate-held
        original the outbox would strand forever once the group completes),
    When: terminalize runs,
    Then: NOT_TERMINALIZABLE — the operator path must never create the stuck
        outbox backlog the automatic completion path guards against.
    """
    gid = _pid(479)
    await _seed_completion_group(
        _repo,
        group_id=gid,
        status=PairedExecutionGroupStatusEnum.MANUAL_INTERVENTION.value,
        leg_statuses=[
            (PairedExecutionLegStatusEnum.MANUAL_INTERVENTION.value, 10.0, 4.0),
            (PairedExecutionLegStatusEnum.CANCELLED.value, 0.0, 0.0),
        ],
    )
    await _insert_created_command(_repo, public_id=_pid(480), correlation_id=gid)
    outcome = await _repo.terminalize_paired_execution_group(
        gid, "user-7", _T1, _NEXT_SESSION_ID, 2
    )
    assert outcome == PairedGroupTerminalizeOutcome.NOT_TERMINALIZABLE


@pytest.mark.asyncio
async def test_terminalize_clamps_long_attestation_subject(
    _repo: SQLAlchemyRepository,
) -> None:
    """A pathologically long attesting subject is clamped to 64 chars in the stamp.

    Given: an attestable manual group and a 200-char attesting subject,
    When: terminalize runs,
    Then: the stamp suffix carries only the subject's first 64 chars, so the
        suffix can never consume the whole 512-char reason column.
    """
    gid = _pid(481)
    await _seed_completion_group(
        _repo,
        group_id=gid,
        status=PairedExecutionGroupStatusEnum.MANUAL_INTERVENTION.value,
        leg_statuses=[
            (PairedExecutionLegStatusEnum.MANUAL_INTERVENTION.value, 1.0, 0.0),
            (PairedExecutionLegStatusEnum.CANCELLED.value, 0.0, 0.0),
        ],
    )
    outcome = await _repo.terminalize_paired_execution_group(
        gid, "u" * 200, _T1, _NEXT_SESSION_ID, 2
    )
    assert outcome == PairedGroupTerminalizeOutcome.TERMINALIZED
    group = (await _active_group_versions(_repo, gid, _T2))[0]
    assert group.failure_reason is not None
    assert group.failure_reason.endswith("terminalized_by=" + "u" * 64)
    assert len(group.failure_reason) <= 512


@pytest.mark.asyncio
async def test_terminalize_not_found_and_reason_truncation(
    _repo: SQLAlchemyRepository,
) -> None:
    """An absent group is NOT_FOUND; a long prior reason truncates to 512 chars.

    Given: no group for one id, and a manual group whose failure_reason is
        already at the 512-char column limit,
    When: terminalize runs for each,
    Then: the absent id returns NOT_FOUND, and the stamped successor's
        failure_reason still fits 512 chars while ending with the attestation
        suffix.
    """
    absent = await _repo.terminalize_paired_execution_group(
        _pid(998), "user-7", _T1, _NEXT_SESSION_ID, 2
    )
    assert absent == PairedGroupTerminalizeOutcome.NOT_FOUND
    gid = _pid(474)
    await _repo.insert_paired_execution_group(
        _group_insert_row(
            public_id=gid,
            status=PairedExecutionGroupStatusEnum.MANUAL_INTERVENTION.value,
            failure_reason="x" * 512,
        )
    )
    await _repo.insert_paired_execution_leg(
        _leg_insert_row(
            public_id=_pid(475),
            group_public_id=gid,
            side="buy",
            command_public_id=_pid(476),
            client_order_id="cid-trunc",
            status=PairedExecutionLegStatusEnum.MANUAL_INTERVENTION.value,
            filled_signed_qty=1.0,
        )
    )
    outcome = await _repo.terminalize_paired_execution_group(
        gid, "user-7", _T1, _NEXT_SESSION_ID, 3
    )
    assert outcome == PairedGroupTerminalizeOutcome.TERMINALIZED
    group = (await _active_group_versions(_repo, gid, _T2))[0]
    assert group.failure_reason is not None
    assert len(group.failure_reason) == 512
    assert group.failure_reason.endswith("; terminalized_by=user-7")


@pytest.mark.asyncio
async def test_get_current_paired_execution_group_reads_current_active(
    _repo: SQLAlchemyRepository,
) -> None:
    """The current-active group read sees a future-stamped row; absent is None.

    Given: a group stamped with a future timestamp (sibling clock skew) and a
        non-existent id,
    When: get_current_paired_execution_group runs for each,
    Then: the future-stamped group is returned (a temporal as_of read would
        skip it and 404 the operator) and the absent id yields None.
    """
    gid = _pid(477)
    await _repo.insert_paired_execution_group(
        _group_insert_row(
            public_id=gid,
            status=PairedExecutionGroupStatusEnum.MANUAL_INTERVENTION.value,
            timestamp=_T0 + timedelta(seconds=600),
        )
    )
    found = await _repo.get_current_paired_execution_group(gid)
    assert found is not None
    assert found["public_id"] == gid
    assert await _repo.get_current_paired_execution_group(_pid(997)) is None


class TestHasOrderSubmitEvidence:
    """Durable evidence probe for the duplicate-submit guard."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "event_type",
        [
            "order_accepted",
            "fill_observed",
            "order_terminal",
            "order_submit_unknown",
            "order_breaker_open",
            "order_interlock_blocked",
        ],
    )
    async def test_evidence_event_types_return_true(
        self, _repo: SQLAlchemyRepository, event_type: str
    ) -> None:
        """Each evidence event type proves the submit may have reached the venue.

        Given: A venue event of an evidence type for the client id,
        When: has_order_submit_evidence is queried,
        Then: True — a replayed command for this id must be dropped.
        """
        await _insert_fill_event(
            _repo, client_order_id="cid-ev-1", cum_fill_size=None, event_type=event_type
        )
        assert await _repo.has_order_submit_evidence("cid-ev-1") is True

    @pytest.mark.asyncio
    async def test_rejection_only_history_is_not_evidence(
        self, _repo: SQLAlchemyRepository
    ) -> None:
        """A definitive rejection never blocks a legitimate outbox retry.

        Given: Only an order_rejected venue event for the client id,
        When: has_order_submit_evidence is queried,
        Then: False — the original submit definitively never placed.
        """
        await _insert_fill_event(
            _repo, client_order_id="cid-ev-2", cum_fill_size=None, event_type="order_rejected"
        )
        assert await _repo.has_order_submit_evidence("cid-ev-2") is False

    @pytest.mark.asyncio
    async def test_no_rows_and_foreign_rows_are_not_evidence(
        self, _repo: SQLAlchemyRepository
    ) -> None:
        """Absence of rows for the id answers False.

        Given: No venue events for the queried id and an evidence row
            for a DIFFERENT client id,
        When: has_order_submit_evidence is queried,
        Then: False — foreign evidence never leaks across ids.
        """
        await _insert_fill_event(_repo, client_order_id="cid-ev-other", cum_fill_size=1.0)
        assert await _repo.has_order_submit_evidence("cid-ev-3") is False


class TestGetFillVenueEventsForOrder:
    """Ordered durable fill history for recovery watermark seeding."""

    @pytest.mark.asyncio
    async def test_returns_cum_fills_ordered_by_cum_then_id(
        self, _repo: SQLAlchemyRepository
    ) -> None:
        """Rows come back monotonic in cum regardless of write order.

        Given: fill_observed rows inserted OUT of cumulative order, plus
            a null-cum fill row and an order_accepted row,
        When: get_fill_venue_events_for_order is queried,
        Then: Only the cumulative fill rows return, ordered
            (cum_fill_size asc, id asc) — the walkable durable history.
        """
        cid = "cid-hist-1"
        await _insert_fill_event(_repo, client_order_id=cid, cum_fill_size=0.7)
        await _insert_fill_event(_repo, client_order_id=cid, cum_fill_size=0.3)
        await _insert_fill_event(_repo, client_order_id=cid, cum_fill_size=None)
        await _insert_fill_event(
            _repo, client_order_id=cid, cum_fill_size=None, event_type="order_accepted"
        )
        await _insert_fill_event(_repo, client_order_id=cid, cum_fill_size=1.0)
        rows = await _repo.get_fill_venue_events_for_order(cid)
        assert [r["cum_fill_size"] for r in rows] == [0.3, 0.7, 1.0]
        assert all(r["event_type"] == "fill_observed" for r in rows)

    @pytest.mark.asyncio
    async def test_equal_cums_tiebreak_by_id(self, _repo: SQLAlchemyRepository) -> None:
        """Identical cumulatives keep insert order via the id tiebreak."""
        cid = "cid-hist-2"
        first = await _insert_fill_event(_repo, client_order_id=cid, cum_fill_size=0.5)
        second = await _insert_fill_event(_repo, client_order_id=cid, cum_fill_size=0.5)
        rows = await _repo.get_fill_venue_events_for_order(cid)
        assert [r["id"] for r in rows] == [first, second]

    @pytest.mark.asyncio
    async def test_empty_and_foreign_isolation(self, _repo: SQLAlchemyRepository) -> None:
        """No rows for the id returns an empty list; foreign ids never leak."""
        await _insert_fill_event(_repo, client_order_id="cid-hist-other", cum_fill_size=2.0)
        assert await _repo.get_fill_venue_events_for_order("cid-hist-3") == []
