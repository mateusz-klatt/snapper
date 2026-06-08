"""Tests for paired-execution repository data access.

The repository layer owns the SCD2 close-and-insert semantics for the
paired-execution guard. These tests exercise inserts, active reads,
compare-and-swap transitions, and halt clearing against an in-memory
aiosqlite database.
"""

from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest
from sqlalchemy import select

from snapper.core.types import PairedExecutionGroupStatusEnum
from snapper.core.types import PairedExecutionLegStatusEnum
from snapper.core.types import PairedExecutionPolicyEnum
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import PairedExecutionGroup
from snapper.data.models import PairedExecutionHalt
from snapper.data.models import PairedExecutionLeg
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
        _T2,
    )
    absent_halt = await _repo.get_active_paired_execution_halt(
        _WALLET_ID,
        _STRATEGY_ID,
        _OTHER_GROUP_KEY,
        _T2,
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
        _T1,
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
