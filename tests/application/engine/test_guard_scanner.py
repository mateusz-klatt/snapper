"""Tests for the paired-execution guard scanner (Phase 4a) + cas_trade_command_status.

Exercises the DB-only liveness loop against a real in-memory repository: breaking
expired assembling groups, retrying arming complete groups, and cancelling owned
held commands / terminalizing owned legs of broken groups, plus the guarded
``cas_trade_command_status`` primitive it relies on.
"""

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from sqlalchemy import select

from snapper.application.engine.guard_scanner import PairedExecutionGuardScanner
from snapper.application.trade.trade_service import TradeService
from snapper.core.partitioning import ShardOwnership
from snapper.core.types import PairedExecutionGroupStatusEnum
from snapper.core.types import PairedExecutionLegStatusEnum
from snapper.core.types import PairedExecutionPolicyEnum
from snapper.data.models import PairedExecutionHalt
from snapper.data.models import TradeCommand
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import where_active

_T0 = datetime(2026, 6, 8, 12, 0, tzinfo=UTC)
_T1 = _T0 + timedelta(seconds=1)
_PAST = _T0 - timedelta(seconds=10)
_FUTURE = _T0 + timedelta(seconds=600)
_WALLET = "00000000-0000-0000-0000-000000000001"
_OPERATOR = "00000000-0000-0000-0000-000000000002"
_SESSION = "00000000-0000-0000-0000-000000000003"
_GROUP_KEY = "kraken:BTC-USD:live|kraken:ETH-USD:live"
_BTC_SHARD = "kraken.BTC-USD.live"
_ETH_SHARD = "kraken.ETH-USD.live"


@pytest.fixture
async def _repo() -> AsyncIterator[SQLAlchemyRepository]:
    """Provide a fresh in-memory repository."""
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    await repo.create_all()
    try:
        yield repo
    finally:
        await repo.engine.dispose()


def _scanner(
    repo: SQLAlchemyRepository,
    *,
    instance_count: int = 1,
    trade_service: TradeService | None = None,
) -> PairedExecutionGuardScanner:
    """Build a scanner owning everything (instance_count=1) by default."""
    return PairedExecutionGuardScanner(
        repository=repo,
        ownership=ShardOwnership(instance_id=0, instance_count=instance_count),
        trade_service=trade_service if trade_service is not None else TradeService(),
        interval_seconds=0.01,
    )


async def _insert_group(
    repo: SQLAlchemyRepository,
    *,
    public_id: str,
    status: str = PairedExecutionGroupStatusEnum.ASSEMBLING.value,
    assembly_deadline: datetime = _FUTURE,
    fill_deadline: datetime = _FUTURE,
    policy: str = PairedExecutionPolicyEnum.SIMULTANEOUS.value,
    expected_leg_count: int = 2,
    failure_reason: str | None = None,
) -> None:
    """Insert a paired-execution group."""
    await repo.insert_paired_execution_group(
        {
            "public_id": public_id,
            "wallet_public_id": _WALLET,
            "operator_public_id": _OPERATOR,
            "strategy_id": "pairs-alpha",
            "policy": policy,
            "expected_leg_count": expected_leg_count,
            "group_key": _GROUP_KEY,
            "status": status,
            "assembly_deadline": assembly_deadline,
            "fill_deadline": fill_deadline,
            "failure_reason": failure_reason,
            "created_at": _T0,
            "session_id": _SESSION,
            "sequence_id": 1,
            "timestamp": _T0,
        }
    )


async def _insert_leg(
    repo: SQLAlchemyRepository,
    *,
    public_id: str,
    group_public_id: str,
    leg_index: int,
    instrument: str,
    shard_key: str,
    command_public_id: str | None,
    status: str = PairedExecutionLegStatusEnum.PENDING.value,
    filled_signed_qty: float = 0.0,
) -> None:
    """Insert a paired-execution leg."""
    await repo.insert_paired_execution_leg(
        {
            "public_id": public_id,
            "group_public_id": group_public_id,
            "leg_index": leg_index,
            "exchange": "kraken",
            "mode": "live",
            "instrument": instrument,
            "shard_key": shard_key,
            "side": "buy",
            "target_qty": 1.0,
            "signal_public_id": f"sig-{public_id}",
            "command_public_id": command_public_id,
            "client_order_id": command_public_id,
            "status": status,
            "filled_signed_qty": filled_signed_qty,
            "wallet_public_id": _WALLET,
            "operator_public_id": _OPERATOR,
            "created_at": _T0,
            "session_id": _SESSION,
            "sequence_id": 1,
            "timestamp": _T0,
        }
    )


async def _insert_command(
    repo: SQLAlchemyRepository,
    *,
    public_id: str,
    correlation_id: str,
    shard_key: str,
    status: str = "created",
) -> None:
    """Insert a held trade command via the ORM (explicit public_id)."""
    async with repo.session() as session:
        session.add(
            TradeCommand(
                public_id=public_id,
                command_type="submit",
                shard_key=shard_key,
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
                status=status,
                created_at=_T0,
                correlation_id=correlation_id,
                source_surface="strategy",
                wallet_public_id=_WALLET,
                session_id=_SESSION,
                sequence_id=1,
                timestamp=_T0,
            )
        )
        await session.commit()


async def _active_command(
    repo: SQLAlchemyRepository, public_id: str, as_of: datetime
) -> TradeCommand:
    """Return the active trade command ORM row for a public_id."""
    async with repo.session() as session:
        result = await session.execute(
            select(TradeCommand).where(
                TradeCommand.public_id == public_id, *where_active(TradeCommand, as_of)
            )
        )
        return result.scalars().one()


async def _command_version_count(repo: SQLAlchemyRepository, public_id: str) -> int:
    """Return how many SCD2 versions exist for a trade command."""
    async with repo.session() as session:
        result = await session.execute(
            select(TradeCommand).where(TradeCommand.public_id == public_id)
        )
        return len(list(result.scalars().all()))


async def _command_status(repo: SQLAlchemyRepository, public_id: str) -> str | None:
    """Return the active trade command's status, or None if absent."""
    cmd = await repo.get_trade_command_by_public_id(public_id, as_of=_FUTURE)
    return cmd["status"] if cmd is not None else None


async def test_cas_trade_command_status_transitions_on_match(_repo: SQLAlchemyRepository) -> None:
    """A created command transitions to cancelled under a matching CAS.

    Given: an active 'created' trade command,
    When: cas_trade_command_status is called with expected 'created',
    Then: it returns True and the active row becomes 'cancelled'.
    """
    await _insert_command(_repo, public_id="cmd-1", correlation_id="grp-1", shard_key=_BTC_SHARD)
    applied = await _repo.cas_trade_command_status(
        "cmd-1", "created", "cancelled", _T0, _SESSION, 5, terminal_at=_T0
    )
    assert applied is True
    assert await _command_status(_repo, "cmd-1") == "cancelled"


async def test_cas_trade_command_status_false_on_status_mismatch(
    _repo: SQLAlchemyRepository,
) -> None:
    """A non-'created' command is not cancelled by a 'created'-guarded CAS.

    Given: an active 'dispatched' trade command,
    When: cas_trade_command_status expects 'created',
    Then: it returns False and the command is untouched, so the guard never
        clobbers a command that already reached the venue.
    """
    await _insert_command(
        _repo, public_id="cmd-2", correlation_id="grp-1", shard_key=_BTC_SHARD, status="dispatched"
    )
    applied = await _repo.cas_trade_command_status(
        "cmd-2", "created", "cancelled", _T0, _SESSION, 5
    )
    assert applied is False
    assert await _command_status(_repo, "cmd-2") == "dispatched"


async def test_cas_trade_command_status_false_when_missing(_repo: SQLAlchemyRepository) -> None:
    """A CAS on an absent command returns False.

    Given: no command with the given public_id,
    When: cas_trade_command_status is called,
    Then: it returns False.
    """
    applied = await _repo.cas_trade_command_status("nope", "created", "cancelled", _T0, _SESSION, 5)
    assert applied is False


async def test_scanner_breaks_expired_assembling_group(_repo: SQLAlchemyRepository) -> None:
    """An assembling group past its assembly deadline is broken.

    Given: an assembling group whose assembly_deadline is in the past,
    When: a scan cycle runs,
    Then: the group transitions to broken with a failure reason.
    """
    await _insert_group(_repo, public_id="grp-1", assembly_deadline=_PAST)
    await _scanner(_repo)._scan_cycle(_T0)
    group = await _repo.get_paired_execution_group("grp-1", _FUTURE)
    assert group is not None
    assert group["status"] == PairedExecutionGroupStatusEnum.BROKEN.value
    assert group["failure_reason"] == "assembly timeout"


async def test_scanner_arms_complete_group_within_deadline(_repo: SQLAlchemyRepository) -> None:
    """A complete assembling group still within deadline is armed by the scanner.

    Given: a complete two-leg assembling group (both legs command-bound,
        key matches) whose deadline has not passed,
    When: a scan cycle runs,
    Then: the arm-retry transitions the group to armed (the last-committer
        crash liveness fix).
    """
    await _insert_group(_repo, public_id="grp-1", assembly_deadline=_FUTURE)
    await _insert_leg(
        _repo,
        public_id="leg-0",
        group_public_id="grp-1",
        leg_index=0,
        instrument="BTC-USD",
        shard_key=_BTC_SHARD,
        command_public_id="cmd-0",
    )
    await _insert_leg(
        _repo,
        public_id="leg-1",
        group_public_id="grp-1",
        leg_index=1,
        instrument="ETH-USD",
        shard_key=_ETH_SHARD,
        command_public_id="cmd-1",
    )
    await _scanner(_repo)._scan_cycle(_T0)
    group = await _repo.get_paired_execution_group("grp-1", _FUTURE)
    assert group is not None
    assert group["status"] == PairedExecutionGroupStatusEnum.ARMED.value


async def test_scanner_skips_sequential_handoff_policy(_repo: SQLAlchemyRepository) -> None:
    """A sequential_handoff group is left untouched by the 4a scanner.

    Given: an expired assembling group with sequential_handoff policy,
    When: a scan cycle runs,
    Then: it stays assembling (4a scopes to simultaneous only).
    """
    await _insert_group(
        _repo,
        public_id="grp-1",
        assembly_deadline=_PAST,
        policy=PairedExecutionPolicyEnum.SEQUENTIAL_HANDOFF.value,
    )
    await _scanner(_repo)._scan_cycle(_T0)
    group = await _repo.get_paired_execution_group("grp-1", _FUTURE)
    assert group is not None
    assert group["status"] == PairedExecutionGroupStatusEnum.ASSEMBLING.value


async def test_scanner_breaks_armed_group_on_fill_timeout(_repo: SQLAlchemyRepository) -> None:
    """An armed group past its fill deadline with an unfilled leg is broken.

    Given: an armed group whose fill_deadline is in the past, with one leg filled
        and one still working,
    When: a scan cycle runs,
    Then: the group transitions to broken with the 'fill timeout' reason — the
        atomicity failure where one sibling never completed by the deadline.
    """
    await _insert_group(
        _repo,
        public_id="grp-1",
        status=PairedExecutionGroupStatusEnum.ARMED.value,
        fill_deadline=_PAST,
    )
    await _insert_leg(
        _repo,
        public_id="leg-0",
        group_public_id="grp-1",
        leg_index=0,
        instrument="BTC-USD",
        shard_key=_BTC_SHARD,
        command_public_id="cmd-0",
        status=PairedExecutionLegStatusEnum.FILLED.value,
        filled_signed_qty=1.0,
    )
    await _insert_leg(
        _repo,
        public_id="leg-1",
        group_public_id="grp-1",
        leg_index=1,
        instrument="ETH-USD",
        shard_key=_ETH_SHARD,
        command_public_id="cmd-1",
        status=PairedExecutionLegStatusEnum.WORKING.value,
    )
    await _scanner(_repo)._scan_cycle(_T0)
    group = await _repo.get_paired_execution_group("grp-1", _FUTURE)
    assert group is not None
    assert group["status"] == PairedExecutionGroupStatusEnum.BROKEN.value
    assert group["failure_reason"] == "fill timeout"


@pytest.mark.parametrize(
    "terminal_status",
    [
        PairedExecutionLegStatusEnum.REJECTED.value,
        PairedExecutionLegStatusEnum.CANCELLED.value,
        PairedExecutionLegStatusEnum.EXPIRED.value,
    ],
)
async def test_scanner_breaks_armed_group_on_terminal_leg(
    _repo: SQLAlchemyRepository, terminal_status: str
) -> None:
    """An armed group with a terminal-without-fill leg breaks before the deadline.

    Given: an armed group still within its fill_deadline, with one leg in a
        terminal-without-fill state (rejected / cancelled / expired),
    When: a scan cycle runs,
    Then: the group breaks immediately with the 'leg terminal before fill' reason
        — a sibling that can never complete should not keep the filled leg
        exposed until the deadline.
    """
    await _insert_group(
        _repo,
        public_id="grp-1",
        status=PairedExecutionGroupStatusEnum.ARMED.value,
        fill_deadline=_FUTURE,
    )
    await _insert_leg(
        _repo,
        public_id="leg-0",
        group_public_id="grp-1",
        leg_index=0,
        instrument="BTC-USD",
        shard_key=_BTC_SHARD,
        command_public_id="cmd-0",
        status=PairedExecutionLegStatusEnum.FILLED.value,
        filled_signed_qty=1.0,
    )
    await _insert_leg(
        _repo,
        public_id="leg-1",
        group_public_id="grp-1",
        leg_index=1,
        instrument="ETH-USD",
        shard_key=_ETH_SHARD,
        command_public_id="cmd-1",
        status=terminal_status,
    )
    await _scanner(_repo)._scan_cycle(_T0)
    group = await _repo.get_paired_execution_group("grp-1", _FUTURE)
    assert group is not None
    assert group["status"] == PairedExecutionGroupStatusEnum.BROKEN.value
    assert group["failure_reason"] == "leg terminal before fill"


async def test_scanner_leaves_armed_group_with_all_legs_filled(
    _repo: SQLAlchemyRepository,
) -> None:
    """An armed group whose legs all fully filled stays armed (the success path).

    Given: an armed group past its fill_deadline whose two legs are both filled,
    When: a scan cycle runs,
    Then: it stays armed — the deadline is irrelevant once every leg completed.
    """
    await _insert_group(
        _repo,
        public_id="grp-1",
        status=PairedExecutionGroupStatusEnum.ARMED.value,
        fill_deadline=_PAST,
    )
    await _insert_leg(
        _repo,
        public_id="leg-0",
        group_public_id="grp-1",
        leg_index=0,
        instrument="BTC-USD",
        shard_key=_BTC_SHARD,
        command_public_id="cmd-0",
        status=PairedExecutionLegStatusEnum.FILLED.value,
        filled_signed_qty=1.0,
    )
    await _insert_leg(
        _repo,
        public_id="leg-1",
        group_public_id="grp-1",
        leg_index=1,
        instrument="ETH-USD",
        shard_key=_ETH_SHARD,
        command_public_id="cmd-1",
        status=PairedExecutionLegStatusEnum.FILLED.value,
        filled_signed_qty=1.0,
    )
    await _scanner(_repo)._scan_cycle(_T0)
    group = await _repo.get_paired_execution_group("grp-1", _FUTURE)
    assert group is not None
    assert group["status"] == PairedExecutionGroupStatusEnum.ARMED.value


async def test_scanner_leaves_armed_group_working_before_deadline(
    _repo: SQLAlchemyRepository,
) -> None:
    """An armed group still within its fill deadline with working legs stays armed.

    Given: an armed group whose fill_deadline has not passed and whose legs are
        still working (no fill yet, none terminal),
    When: a scan cycle runs,
    Then: it stays armed — there is no failure to break on yet.
    """
    await _insert_group(
        _repo,
        public_id="grp-1",
        status=PairedExecutionGroupStatusEnum.ARMED.value,
        fill_deadline=_FUTURE,
    )
    await _insert_leg(
        _repo,
        public_id="leg-0",
        group_public_id="grp-1",
        leg_index=0,
        instrument="BTC-USD",
        shard_key=_BTC_SHARD,
        command_public_id="cmd-0",
        status=PairedExecutionLegStatusEnum.WORKING.value,
    )
    await _insert_leg(
        _repo,
        public_id="leg-1",
        group_public_id="grp-1",
        leg_index=1,
        instrument="ETH-USD",
        shard_key=_ETH_SHARD,
        command_public_id="cmd-1",
        status=PairedExecutionLegStatusEnum.WORKING.value,
    )
    await _scanner(_repo)._scan_cycle(_T0)
    group = await _repo.get_paired_execution_group("grp-1", _FUTURE)
    assert group is not None
    assert group["status"] == PairedExecutionGroupStatusEnum.ARMED.value


async def test_scanner_skips_armed_sequential_handoff(_repo: SQLAlchemyRepository) -> None:
    """An armed sequential_handoff group is left untouched by the armed sweep.

    Given: an armed sequential_handoff group past its fill_deadline with a working
        leg,
    When: a scan cycle runs,
    Then: it stays armed — the sweep is scoped to simultaneous policy.
    """
    await _insert_group(
        _repo,
        public_id="grp-1",
        status=PairedExecutionGroupStatusEnum.ARMED.value,
        fill_deadline=_PAST,
        policy=PairedExecutionPolicyEnum.SEQUENTIAL_HANDOFF.value,
    )
    await _insert_leg(
        _repo,
        public_id="leg-0",
        group_public_id="grp-1",
        leg_index=0,
        instrument="BTC-USD",
        shard_key=_BTC_SHARD,
        command_public_id="cmd-0",
        status=PairedExecutionLegStatusEnum.WORKING.value,
    )
    await _scanner(_repo)._scan_cycle(_T0)
    group = await _repo.get_paired_execution_group("grp-1", _FUTURE)
    assert group is not None
    assert group["status"] == PairedExecutionGroupStatusEnum.ARMED.value


async def test_scanner_leaves_armed_group_with_no_legs(_repo: SQLAlchemyRepository) -> None:
    """An armed group with no legs (corruption) is not broken by the armed sweep.

    Given: an armed group past its fill_deadline carrying no active legs,
    When: a scan cycle runs,
    Then: it stays armed — an empty leg set carries no exposure and is left to
        the assembling-stage validation rather than broken here.
    """
    await _insert_group(
        _repo,
        public_id="grp-1",
        status=PairedExecutionGroupStatusEnum.ARMED.value,
        fill_deadline=_PAST,
    )
    await _scanner(_repo)._scan_cycle(_T0)
    group = await _repo.get_paired_execution_group("grp-1", _FUTURE)
    assert group is not None
    assert group["status"] == PairedExecutionGroupStatusEnum.ARMED.value


async def test_scanner_armed_break_halts_exposed_group_same_cycle(
    _repo: SQLAlchemyRepository,
) -> None:
    """An armed group broken by fill timeout with a filled leg is halted same cycle.

    Given: an armed group past its fill_deadline with one filled (exposed) leg and
        one working leg,
    When: a single scan cycle runs (armed sweep then halt sweep),
    Then: the group is broken AND a durable halt is projected within the same
        cycle, with the owned filled-leg shard mirrored into the trade service —
        the exposed atomicity failure is halted immediately, not a cycle later.
    """
    trade_service = TradeService()
    await _insert_group(
        _repo,
        public_id="grp-1",
        status=PairedExecutionGroupStatusEnum.ARMED.value,
        fill_deadline=_PAST,
    )
    await _insert_leg(
        _repo,
        public_id="leg-0",
        group_public_id="grp-1",
        leg_index=0,
        instrument="BTC-USD",
        shard_key=_BTC_SHARD,
        command_public_id="cmd-0",
        status=PairedExecutionLegStatusEnum.FILLED.value,
        filled_signed_qty=1.0,
    )
    await _insert_leg(
        _repo,
        public_id="leg-1",
        group_public_id="grp-1",
        leg_index=1,
        instrument="ETH-USD",
        shard_key=_ETH_SHARD,
        command_public_id="cmd-1",
        status=PairedExecutionLegStatusEnum.WORKING.value,
    )
    await _scanner(_repo, trade_service=trade_service)._scan_cycle(_T0)
    group = await _repo.get_paired_execution_group("grp-1", _FUTURE)
    assert group is not None
    assert group["status"] == PairedExecutionGroupStatusEnum.BROKEN.value
    halt = await _repo.get_active_paired_execution_halt(_WALLET, "pairs-alpha", _GROUP_KEY)
    assert halt is not None
    assert trade_service.is_halted(_BTC_SHARD) is True


async def test_scanner_cancels_owned_held_command_and_terminalizes_leg(
    _repo: SQLAlchemyRepository,
) -> None:
    """A broken group's owned held command is cancelled and its leg terminalized.

    Given: a broken group with an owned leg bound to a held 'created' command,
    When: a scan cycle runs,
    Then: the command becomes 'cancelled' and the leg becomes 'cancelled', so a
        broken group's held command never lingers or dispatches.
    """
    await _insert_group(
        _repo, public_id="grp-1", status=PairedExecutionGroupStatusEnum.BROKEN.value
    )
    await _insert_leg(
        _repo,
        public_id="leg-0",
        group_public_id="grp-1",
        leg_index=0,
        instrument="BTC-USD",
        shard_key=_BTC_SHARD,
        command_public_id="cmd-0",
    )
    await _insert_command(_repo, public_id="cmd-0", correlation_id="grp-1", shard_key=_BTC_SHARD)
    await _scanner(_repo)._scan_cycle(_T0)
    assert await _command_status(_repo, "cmd-0") == "cancelled"
    legs = await _repo.get_paired_execution_legs("grp-1", _FUTURE)
    assert legs[0]["status"] == PairedExecutionLegStatusEnum.CANCELLED.value


async def test_scanner_leaves_non_owned_leg_untouched(_repo: SQLAlchemyRepository) -> None:
    """A broken group's leg on a non-owned shard is left for its owner.

    Given: a broken group with a leg whose shard this 2-instance coordinator
        does NOT own, bound to a held command,
    When: a scan cycle runs,
    Then: that command stays 'created' (the owning coordinator cancels it).
    """
    not_owned = next(
        shard
        for shard in (_BTC_SHARD, _ETH_SHARD)
        if not ShardOwnership(instance_id=0, instance_count=2).owns(shard)
    )
    await _insert_group(
        _repo, public_id="grp-1", status=PairedExecutionGroupStatusEnum.BROKEN.value
    )
    await _insert_leg(
        _repo,
        public_id="leg-0",
        group_public_id="grp-1",
        leg_index=0,
        instrument="BTC-USD",
        shard_key=not_owned,
        command_public_id="cmd-0",
    )
    await _insert_command(_repo, public_id="cmd-0", correlation_id="grp-1", shard_key=not_owned)
    await _scanner(_repo, instance_count=2)._scan_cycle(_T0)
    assert await _command_status(_repo, "cmd-0") == "created"


async def test_scanner_run_stops_on_stop(_repo: SQLAlchemyRepository) -> None:
    """The scan loop exits cleanly when stop() is called.

    Given: a running scanner with a short interval,
    When: stop() is called after at least one cycle,
    Then: run() returns and the scanner is no longer running.
    """
    scanner = _scanner(_repo)
    task = asyncio.create_task(scanner.run())
    await asyncio.sleep(0.05)
    scanner.stop()
    await asyncio.wait_for(task, timeout=2.0)
    assert scanner._running is False


async def test_scanner_run_propagates_cancellation(_repo: SQLAlchemyRepository) -> None:
    """The scan loop re-raises CancelledError when its task is cancelled.

    Given: a running scanner,
    When: its task is cancelled,
    Then: run() re-raises CancelledError.
    """
    scanner = PairedExecutionGuardScanner(
        repository=_repo,
        ownership=ShardOwnership(instance_id=0, instance_count=1),
        trade_service=TradeService(),
        interval_seconds=10.0,
    )
    task = asyncio.create_task(scanner.run())
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_scanner_skips_leg_without_command_or_already_terminal(
    _repo: SQLAlchemyRepository,
) -> None:
    """A broken group's owned leg with no command and a terminal status is left as-is.

    Given: a broken group whose owned leg has no bound command and is already
        cancelled,
    When: a scan cycle runs,
    Then: nothing changes for that leg (no command to cancel, leg already
        terminal), confirming the broken sweep is idempotent.
    """
    await _insert_group(
        _repo, public_id="grp-1", status=PairedExecutionGroupStatusEnum.BROKEN.value
    )
    await _insert_leg(
        _repo,
        public_id="leg-0",
        group_public_id="grp-1",
        leg_index=0,
        instrument="BTC-USD",
        shard_key=_BTC_SHARD,
        command_public_id=None,
        status=PairedExecutionLegStatusEnum.CANCELLED.value,
    )
    await _scanner(_repo)._scan_cycle(_T0)
    legs = await _repo.get_paired_execution_legs("grp-1", _FUTURE)
    assert legs[0]["status"] == PairedExecutionLegStatusEnum.CANCELLED.value


async def test_cas_trade_command_status_carries_every_non_status_column(
    _repo: SQLAlchemyRepository,
) -> None:
    """Cancelling a command preserves every non-status column.

    Given: an active 'created' strategy command (source_surface='strategy'),
    When: cas_trade_command_status cancels it,
    Then: the new active row preserves every column except the SCD2/status
        set, so audit provenance (source_surface) survives the cancel rather
        than resetting to the DB default.
    """
    await _insert_command(_repo, public_id="cmd-1", correlation_id="grp-1", shard_key=_BTC_SHARD)
    before = await _active_command(_repo, "cmd-1", _T0)
    carried = {
        column.name: getattr(before, column.name) for column in TradeCommand.__table__.columns
    }
    applied = await _repo.cas_trade_command_status(
        "cmd-1", "created", "cancelled", _T0, _SESSION, 5, terminal_at=_T0, last_error="broken"
    )
    assert applied is True
    after = await _active_command(_repo, "cmd-1", _FUTURE)
    advanced = {
        "id",
        "known_to",
        "session_id",
        "sequence_id",
        "timestamp",
        "status",
        "terminal_at",
        "last_error",
    }
    for column in TradeCommand.__table__.columns:
        if column.name in advanced:
            continue
        assert getattr(after, column.name) == carried[column.name], column.name
    assert after.source_surface == "strategy"


async def test_scanner_cleans_up_sibling_broken_group(_repo: SQLAlchemyRepository) -> None:
    """A CAS-loser coordinator still cancels its own legs of a sibling-broken group.

    Given: a group already broken by a sibling coordinator, in which a
        2-instance coordinator owns exactly one leg bound to a held command,
    When: this coordinator scans,
    Then: it cancels its OWNED leg's held command even though it never broke
        the group (CAS-loser cleanup), proving no owned held command lingers.
    """
    owned = next(
        shard
        for shard in (_BTC_SHARD, _ETH_SHARD)
        if ShardOwnership(instance_id=0, instance_count=2).owns(shard)
    )
    not_owned = _ETH_SHARD if owned == _BTC_SHARD else _BTC_SHARD
    await _insert_group(
        _repo, public_id="grp-1", status=PairedExecutionGroupStatusEnum.BROKEN.value
    )
    await _insert_leg(
        _repo,
        public_id="leg-owned",
        group_public_id="grp-1",
        leg_index=0,
        instrument="BTC-USD",
        shard_key=owned,
        command_public_id="cmd-owned",
    )
    await _insert_leg(
        _repo,
        public_id="leg-sibling",
        group_public_id="grp-1",
        leg_index=1,
        instrument="ETH-USD",
        shard_key=not_owned,
        command_public_id="cmd-sibling",
    )
    await _insert_command(_repo, public_id="cmd-owned", correlation_id="grp-1", shard_key=owned)
    await _insert_command(
        _repo, public_id="cmd-sibling", correlation_id="grp-1", shard_key=not_owned
    )
    await _scanner(_repo, instance_count=2)._scan_cycle(_T0)
    assert await _command_status(_repo, "cmd-owned") == "cancelled"
    assert await _command_status(_repo, "cmd-sibling") == "created"


async def test_scanner_broken_sweep_is_idempotent(_repo: SQLAlchemyRepository) -> None:
    """Re-scanning a broken group creates no extra command/leg versions.

    Given: a broken group with an owned leg + held command cancelled by a
        first scan,
    When: a second scan cycle runs,
    Then: the command still has exactly two SCD2 versions (created, cancelled)
        and the leg stays cancelled — the sweep does not churn state.
    """
    await _insert_group(
        _repo, public_id="grp-1", status=PairedExecutionGroupStatusEnum.BROKEN.value
    )
    await _insert_leg(
        _repo,
        public_id="leg-0",
        group_public_id="grp-1",
        leg_index=0,
        instrument="BTC-USD",
        shard_key=_BTC_SHARD,
        command_public_id="cmd-0",
    )
    await _insert_command(_repo, public_id="cmd-0", correlation_id="grp-1", shard_key=_BTC_SHARD)
    scanner = _scanner(_repo)
    await scanner._scan_cycle(_T0)
    await scanner._scan_cycle(_T0)
    assert await _command_version_count(_repo, "cmd-0") == 2
    assert await _command_status(_repo, "cmd-0") == "cancelled"
    legs = await _repo.get_paired_execution_legs("grp-1", _FUTURE)
    assert legs[0]["status"] == PairedExecutionLegStatusEnum.CANCELLED.value


async def test_cas_trade_command_status_sees_current_active_row(
    _repo: SQLAlchemyRepository,
) -> None:
    """A stale bus_time cannot clobber a command that already moved on.

    Given: a command transitioned created->dispatched (the dispatched row is
        current-active),
    When: cas_trade_command_status expects 'created' with a bus_time from
        BEFORE the dispatch (when 'created' was still active as-of that time),
    Then: it returns False because the guard reads the CURRENT active row's
        'dispatched' status, not the historical 'created' view, and the
        command stays dispatched.
    """
    await _insert_command(
        _repo, public_id="cmd-stale", correlation_id="grp-1", shard_key=_BTC_SHARD
    )
    await _repo.update_trade_command_status("cmd-stale", "dispatched", _T1, _SESSION, 2)
    applied = await _repo.cas_trade_command_status(
        "cmd-stale", "created", "cancelled", _T0, _SESSION, 3
    )
    assert applied is False
    assert await _command_status(_repo, "cmd-stale") == "dispatched"


async def test_scanner_run_survives_cycle_error(_repo: SQLAlchemyRepository) -> None:
    """A failing scan cycle is logged and the loop keeps running.

    Given: a scanner whose repository raises on its first query,
    When: it runs,
    Then: the loop catches the error and stays alive (a transient DB/race
        error never kills the scan loop), stopping cleanly on stop().
    """
    scanner = _scanner(_repo)
    failing_repo = MagicMock()
    failing_repo.list_active_paired_execution_groups = AsyncMock(side_effect=RuntimeError("boom"))
    scanner._repo = failing_repo
    task = asyncio.create_task(scanner.run())
    await asyncio.sleep(0.05)
    assert not task.done()
    scanner.stop()
    await asyncio.wait_for(task, timeout=2.0)
    assert scanner._running is False


async def test_sweep_halts_skips_assembly_timeout_broken_group(
    _repo: SQLAlchemyRepository,
) -> None:
    """A broken group with no fills (assembly timeout) is NOT durably halted.

    Given: a broken group whose owned leg has zero filled_signed_qty (a pure
        assembly-timeout break, no venue exposure),
    When: a scan cycle runs,
    Then: no durable halt row is projected and the leg's shard stays un-halted,
        so the strategy pair is free to re-assemble on the next tick.
    """
    trade_service = TradeService()
    await _insert_group(
        _repo, public_id="grp-1", status=PairedExecutionGroupStatusEnum.BROKEN.value
    )
    await _insert_leg(
        _repo,
        public_id="leg-0",
        group_public_id="grp-1",
        leg_index=0,
        instrument="BTC-USD",
        shard_key=_BTC_SHARD,
        command_public_id=None,
        status=PairedExecutionLegStatusEnum.CANCELLED.value,
        filled_signed_qty=0.0,
    )
    await _scanner(_repo, trade_service=trade_service)._scan_cycle(_T0)
    halt = await _repo.get_active_paired_execution_halt(_WALLET, "pairs-alpha", _GROUP_KEY)
    assert halt is None
    assert trade_service.is_halted(_BTC_SHARD) is False


async def test_sweep_halts_projects_durable_halt_and_mirrors_owned_shard(
    _repo: SQLAlchemyRepository,
) -> None:
    """A broken group with a filled leg projects a durable halt + owned shard mirror.

    Given: a broken group with a non-zero filled owned leg and a failure_reason,
    When: a scan cycle runs,
    Then: a durable halt row is inserted for the (wallet, strategy, group_key)
        scope carrying the leg's mode and the group's failure_reason, and the
        owned leg's shard is halted in memory for the cheap _on_signal gate.
    """
    trade_service = TradeService()
    await _insert_group(
        _repo,
        public_id="grp-1",
        status=PairedExecutionGroupStatusEnum.BROKEN.value,
        failure_reason="fill timeout",
    )
    await _insert_leg(
        _repo,
        public_id="leg-0",
        group_public_id="grp-1",
        leg_index=0,
        instrument="BTC-USD",
        shard_key=_BTC_SHARD,
        command_public_id=None,
        filled_signed_qty=1.0,
    )
    await _scanner(_repo, trade_service=trade_service)._scan_cycle(_T0)
    halt = await _repo.get_active_paired_execution_halt(_WALLET, "pairs-alpha", _GROUP_KEY)
    assert halt is not None
    assert halt["mode"] == "live"
    assert halt["group_public_id"] == "grp-1"
    assert halt["reason"] == "fill timeout"
    assert trade_service.is_halted(_BTC_SHARD) is True


async def test_sweep_halts_projects_for_compensating_group_without_fills(
    _repo: SQLAlchemyRepository,
) -> None:
    """A compensating group is halted even with zero fills; reason falls back.

    Given: a compensating group (status exposure) whose leg has zero fills and
        whose failure_reason is None,
    When: a scan cycle runs,
    Then: a durable halt is still projected with the generic broken reason and
        the owned shard is halted, because compensating means flatten-in-flight.
    """
    trade_service = TradeService()
    await _insert_group(
        _repo, public_id="grp-1", status=PairedExecutionGroupStatusEnum.COMPENSATING.value
    )
    await _insert_leg(
        _repo,
        public_id="leg-0",
        group_public_id="grp-1",
        leg_index=0,
        instrument="BTC-USD",
        shard_key=_BTC_SHARD,
        command_public_id=None,
        filled_signed_qty=0.0,
    )
    await _scanner(_repo, trade_service=trade_service)._scan_cycle(_T0)
    halt = await _repo.get_active_paired_execution_halt(_WALLET, "pairs-alpha", _GROUP_KEY)
    assert halt is not None
    assert halt["reason"] == "paired-execution group broken"
    assert trade_service.is_halted(_BTC_SHARD) is True


async def test_sweep_halts_inserts_global_halt_but_skips_non_owned_shard_mirror(
    _repo: SQLAlchemyRepository,
) -> None:
    """The durable halt is global; only owned shards are mirrored in memory.

    Given: a broken, filled group whose only leg sits on a shard this 2-instance
        coordinator does NOT own,
    When: a scan cycle runs,
    Then: the durable halt row is still inserted (any coordinator may win the
        idempotent scope), but the non-owned shard is left un-halted in memory
        for its owning coordinator to mirror.
    """
    not_owned = next(
        shard
        for shard in (_BTC_SHARD, _ETH_SHARD)
        if not ShardOwnership(instance_id=0, instance_count=2).owns(shard)
    )
    trade_service = TradeService()
    await _insert_group(
        _repo, public_id="grp-1", status=PairedExecutionGroupStatusEnum.BROKEN.value
    )
    await _insert_leg(
        _repo,
        public_id="leg-0",
        group_public_id="grp-1",
        leg_index=0,
        instrument="BTC-USD",
        shard_key=not_owned,
        command_public_id=None,
        filled_signed_qty=2.0,
    )
    await _scanner(_repo, instance_count=2, trade_service=trade_service)._scan_cycle(_T0)
    halt = await _repo.get_active_paired_execution_halt(_WALLET, "pairs-alpha", _GROUP_KEY)
    assert halt is not None
    assert trade_service.is_halted(not_owned) is False


async def test_sweep_halts_skips_compensating_group_with_no_legs(
    _repo: SQLAlchemyRepository,
) -> None:
    """A compensating group with no legs is skipped and logged, never guessed.

    Given: a compensating group (exposure by status) that has no leg rows, so
        the halt row's mode cannot be derived,
    When: a scan cycle runs,
    Then: no durable halt is projected (the corruption is skipped + logged
        rather than inserting a halt with a guessed mode).
    """
    trade_service = TradeService()
    await _insert_group(
        _repo, public_id="grp-1", status=PairedExecutionGroupStatusEnum.COMPENSATING.value
    )
    await _scanner(_repo, trade_service=trade_service)._scan_cycle(_T0)
    halt = await _repo.get_active_paired_execution_halt(_WALLET, "pairs-alpha", _GROUP_KEY)
    assert halt is None


async def test_sweep_halts_mirrors_only_owned_shard_in_mixed_two_leg_group(
    _repo: SQLAlchemyRepository,
) -> None:
    """A 2-leg exposed group halts only the owned leg's shard, not its sibling.

    Given: a broken, filled group with one leg on an owned shard and one on a
        non-owned shard for this 2-instance coordinator,
    When: a scan cycle runs,
    Then: the durable halt is inserted (global) and only the owned shard is
        halted in memory, so each coordinator mirrors exactly its own legs.
    """
    ownership = ShardOwnership(instance_id=0, instance_count=2)
    owned = next(shard for shard in (_BTC_SHARD, _ETH_SHARD) if ownership.owns(shard))
    not_owned = next(shard for shard in (_BTC_SHARD, _ETH_SHARD) if not ownership.owns(shard))
    trade_service = TradeService()
    await _insert_group(
        _repo, public_id="grp-1", status=PairedExecutionGroupStatusEnum.BROKEN.value
    )
    await _insert_leg(
        _repo,
        public_id="leg-0",
        group_public_id="grp-1",
        leg_index=0,
        instrument="BTC-USD",
        shard_key=owned,
        command_public_id=None,
        filled_signed_qty=1.0,
    )
    await _insert_leg(
        _repo,
        public_id="leg-1",
        group_public_id="grp-1",
        leg_index=1,
        instrument="ETH-USD",
        shard_key=not_owned,
        command_public_id=None,
        filled_signed_qty=0.0,
    )
    await _scanner(_repo, instance_count=2, trade_service=trade_service)._scan_cycle(_T0)
    halt = await _repo.get_active_paired_execution_halt(_WALLET, "pairs-alpha", _GROUP_KEY)
    assert halt is not None
    assert trade_service.is_halted(owned) is True
    assert trade_service.is_halted(not_owned) is False


async def test_sweep_halts_is_idempotent_across_cycles(
    _repo: SQLAlchemyRepository,
) -> None:
    """Re-scanning the same exposed group leaves exactly one active halt.

    Given: a broken, filled group already halted by a prior scan,
    When: a second scan cycle runs,
    Then: the active-unique scope admits no duplicate (exactly one active halt)
        and the owned shard stays halted, so racing/repeated scans converge.
    """
    trade_service = TradeService()
    await _insert_group(
        _repo, public_id="grp-1", status=PairedExecutionGroupStatusEnum.BROKEN.value
    )
    await _insert_leg(
        _repo,
        public_id="leg-0",
        group_public_id="grp-1",
        leg_index=0,
        instrument="BTC-USD",
        shard_key=_BTC_SHARD,
        command_public_id=None,
        filled_signed_qty=1.0,
    )
    scanner = _scanner(_repo, trade_service=trade_service)
    await scanner._scan_cycle(_T0)
    await scanner._scan_cycle(_T1)
    async with _repo.session() as session:
        active = (
            (
                await session.execute(
                    select(PairedExecutionHalt).where(*where_active(PairedExecutionHalt, _T1))
                )
            )
            .scalars()
            .all()
        )
    assert len(list(active)) == 1
    assert trade_service.is_halted(_BTC_SHARD) is True
