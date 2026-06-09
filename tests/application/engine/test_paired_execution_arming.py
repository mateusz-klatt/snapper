"""Tests for the coordinator's paired-execution arming wiring (Phase 3b-wire).

Exercises ``TraderCoordinator._ensure_paired_execution_group`` and
``_register_and_arm_paired_leg`` against a real in-memory repository, plus the
``_on_signal`` integration that ensures the group, registers the owned leg, and
arms the group once its full leg set is durably registered.
"""

from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from types import SimpleNamespace
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import Mock

import pytest

import snapper.application.engine.trader as trader_module
from snapper.application.engine.trader import TraderCoordinator
from snapper.core.partitioning import ShardOwnership
from snapper.core.types import PairedExecutionLegStatusEnum
from snapper.core.types import TradeCommandStatusEnum
from snapper.data.models import TradeCommand
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import VenueEventRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.schemas.data import OrderData
from snapper.messaging.schemas.data import OrderEventData
from snapper.messaging.schemas.data import SignalData

_GROUP_KEY = "kraken:BTC-USD:live|kraken:ETH-USD:live"
_BTC_SHARD = "kraken.BTC-USD.live"
_ETH_SHARD = "kraken.ETH-USD.live"


@pytest.fixture
async def _coord(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[TraderCoordinator]:
    """Build a TraderCoordinator backed by a real in-memory repository."""
    settings = MagicMock()
    settings.db_url = "sqlite:///:memory:"
    settings.zmq_broker_xpub = "tcp://broker.xpub"
    settings.risk_r_per_trade = 0.01
    settings.risk_max_leverage = 2.0
    settings.risk_max_drawdown = 0.15
    monkeypatch.setattr(trader_module, "get_settings", lambda: settings, raising=True)
    monkeypatch.setattr(trader_module, "get_repository", lambda _url: AsyncMock(), raising=True)
    monkeypatch.setattr(
        trader_module, "resolve_symbol_public_id", AsyncMock(return_value="stub-spid")
    )
    monkeypatch.setattr(trader_module, "is_tradeable", lambda _i, _e: True)
    coord = TraderCoordinator()
    coord.msg_publisher = cast(Any, MagicMock(tracker=Mock(session_id="s1")))
    coord.execution_publisher = MagicMock()
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    await repo.create_all()
    coord.repository = repo
    coord._tracker = SequenceTracker()
    coord.outbox = MagicMock()
    try:
        yield coord
    finally:
        await repo.engine.dispose()


def _standalone_signal(instrument: str = "BTC-USD", strength: float = 0.0) -> SignalData:
    """Build a standalone (non-grouped) signal (strength 0 places no order)."""
    return SignalData(
        type="signal",
        public_id=f"sig-{instrument}",
        timestamp=datetime(2026, 6, 8, 12, 0, tzinfo=UTC),
        session_id="",
        sequence_id=0,
        instrument=instrument,
        exchange="kraken",
        side="buy",
        strength=strength,
        reason="test",
        price=50000.0,
        strategy_name="pairs-alpha",
        fired_at=datetime(2026, 6, 8, 12, 0, tzinfo=UTC),
    )


def _grouped_signal(
    *,
    instrument: str,
    index: int,
    group_id: str = "grp-1",
    size: int = 2,
    group_key: str = _GROUP_KEY,
) -> SignalData:
    """Build a grouped (paired-execution) signal leg."""
    return SignalData(
        type="signal",
        public_id=f"sig-{instrument}-{index}",
        timestamp=datetime(2026, 6, 8, 12, 0, tzinfo=UTC),
        session_id="",
        sequence_id=0,
        instrument=instrument,
        exchange="kraken",
        side="buy",
        strength=0.5,
        reason="test",
        price=50000.0,
        strategy_name="pairs-alpha",
        fired_at=datetime(2026, 6, 8, 12, 0, tzinfo=UTC),
        paired_group_id=group_id,
        paired_group_size=size,
        paired_group_index=index,
        paired_group_policy="simultaneous",
        paired_group_key=group_key,
    )


def _engine_stub(instrument: str) -> Any:
    """Return a minimal engine exposing the attrs the leg row reads."""
    return SimpleNamespace(exchange="kraken", mode="live", _shard_key=f"kraken.{instrument}.live")


def _outbox_mock(coord: TraderCoordinator) -> MagicMock:
    """Return the coordinator's outbox as a MagicMock for assertions."""
    return cast(MagicMock, coord.outbox)


def _sql_repo(coord: TraderCoordinator) -> SQLAlchemyRepository:
    """Return the coordinator's repository narrowed to SQLAlchemyRepository."""
    return cast(SQLAlchemyRepository, coord.repository)


@pytest.mark.asyncio
async def test_ensure_group_returns_none_for_standalone_signal(
    _coord: TraderCoordinator,
) -> None:
    """A standalone signal creates no group.

    Given: a non-grouped signal,
    When: the coordinator ensures the paired-execution group,
    Then: no group is created and no correlation id is returned.
    """
    assert await _coord._ensure_paired_execution_group(_standalone_signal()) is None


@pytest.mark.asyncio
async def test_ensure_group_returns_none_for_non_sql_repository(
    _coord: TraderCoordinator,
) -> None:
    """The grouped path is skipped without a SQL repository.

    Given: a grouped signal but a non-SQL repository,
    When: the coordinator ensures the group,
    Then: no group is created and None is returned (tests only).
    """
    _coord.repository = AsyncMock()
    signal = _grouped_signal(instrument="BTC-USD", index=0)
    assert await _coord._ensure_paired_execution_group(signal) is None


@pytest.mark.asyncio
async def test_ensure_group_returns_none_for_inconsistent_descriptor(
    _coord: TraderCoordinator,
) -> None:
    """A group id with missing size/policy/key is skipped fail-safe.

    Given: a malformed grouped descriptor (group id set, size/policy/key
        unset) that bypassed the SignalData validator,
    When: the coordinator ensures the group,
    Then: no group is created and None is returned, so a malformed
        descriptor can never half-create a group.
    """
    signal = SignalData.model_construct(
        type="signal",
        public_id="sig-bad",
        timestamp=datetime(2026, 6, 8, 12, 0, tzinfo=UTC),
        session_id="",
        sequence_id=0,
        instrument="BTC-USD",
        exchange="kraken",
        side="buy",
        strength=0.5,
        reason="test",
        price=50000.0,
        strategy_name="pairs-alpha",
        fired_at=datetime(2026, 6, 8, 12, 0, tzinfo=UTC),
        paired_group_id="grp-1",
        paired_group_size=None,
        paired_group_index=0,
        paired_group_policy=None,
        paired_group_key=None,
    )
    assert await _coord._ensure_paired_execution_group(signal) is None


@pytest.mark.asyncio
async def test_ensure_group_creates_assembling_group(_coord: TraderCoordinator) -> None:
    """A grouped signal creates an assembling group.

    Given: a complete grouped signal,
    When: the coordinator ensures the group,
    Then: an assembling group is created keyed by the group id with the
        expected leg count and canonical group key.
    """
    signal = _grouped_signal(instrument="BTC-USD", index=0)
    result = await _coord._ensure_paired_execution_group(signal)
    assert result == "grp-1"
    group = await _sql_repo(_coord).get_paired_execution_group("grp-1", datetime.now(UTC))
    assert group is not None
    assert group["status"] == "assembling"
    assert group["expected_leg_count"] == 2
    assert group["group_key"] == _GROUP_KEY


@pytest.mark.asyncio
async def test_register_and_arm_noop_for_non_sql_repository(
    _coord: TraderCoordinator,
) -> None:
    """Leg registration is skipped without a SQL repository.

    Given: a grouped signal but a non-SQL repository,
    When: the coordinator registers and arms the leg,
    Then: nothing is registered and the outbox is not notified.
    """
    _coord.repository = AsyncMock()
    await _coord._register_and_arm_paired_leg(
        _grouped_signal(instrument="BTC-USD", index=0),
        _engine_stub("BTC-USD"),
        group_public_id="grp-1",
        command_public_id="cmd-0",
        client_order_id="oid-0",
    )
    _outbox_mock(_coord).notify.assert_not_called()


@pytest.mark.asyncio
async def test_register_and_arm_noop_when_leg_index_missing(
    _coord: TraderCoordinator,
) -> None:
    """A leg with no index is skipped fail-safe.

    Given: a grouped signal whose paired_group_index is unset,
    When: the coordinator registers and arms the leg,
    Then: no leg is registered and the outbox is not notified.
    """
    signal = SignalData.model_construct(
        type="signal",
        public_id="sig-noidx",
        timestamp=datetime(2026, 6, 8, 12, 0, tzinfo=UTC),
        session_id="",
        sequence_id=0,
        instrument="BTC-USD",
        exchange="kraken",
        side="buy",
        strength=0.5,
        reason="test",
        price=50000.0,
        strategy_name="pairs-alpha",
        fired_at=datetime(2026, 6, 8, 12, 0, tzinfo=UTC),
        paired_group_id="grp-1",
        paired_group_size=2,
        paired_group_index=None,
        paired_group_policy="simultaneous",
        paired_group_key=_GROUP_KEY,
    )
    await _coord._register_and_arm_paired_leg(
        signal,
        _engine_stub("BTC-USD"),
        group_public_id="grp-1",
        command_public_id="cmd-0",
        client_order_id="oid-0",
    )
    _outbox_mock(_coord).notify.assert_not_called()
    legs = await _sql_repo(_coord).get_paired_execution_legs("grp-1", datetime.now(UTC))
    assert legs == []


@pytest.mark.asyncio
async def test_register_leg_does_not_arm_when_incomplete(
    _coord: TraderCoordinator,
) -> None:
    """One leg of a two-leg group registers but does not arm.

    Given: an assembling two-leg group with only its first leg available,
    When: the coordinator registers and arms that leg,
    Then: the leg is registered bound to its command, the group stays
        assembling, and the outbox is not notified.
    """
    await _coord._ensure_paired_execution_group(_grouped_signal(instrument="BTC-USD", index=0))
    await _coord._register_and_arm_paired_leg(
        _grouped_signal(instrument="BTC-USD", index=0),
        _engine_stub("BTC-USD"),
        group_public_id="grp-1",
        command_public_id="cmd-0",
        client_order_id="oid-0",
    )
    _outbox_mock(_coord).notify.assert_not_called()
    group = await _sql_repo(_coord).get_paired_execution_group("grp-1", datetime.now(UTC))
    assert group is not None
    assert group["status"] == "assembling"
    legs = await _sql_repo(_coord).get_paired_execution_legs("grp-1", datetime.now(UTC))
    assert len(legs) == 1
    assert legs[0]["command_public_id"] == "cmd-0"


@pytest.mark.asyncio
async def test_second_leg_arms_group_and_notifies_outbox(
    _coord: TraderCoordinator,
) -> None:
    """The last leg to register arms the group and notifies the outbox.

    Given: an assembling two-leg group whose first leg is already
        registered (cross-coordinator handoff),
    When: the second, completing leg (matching group_key, both commands
        bound) is registered,
    Then: the validated CAS arms the group exactly once and the outbox is
        notified so the held commands dispatch together.
    """
    await _coord._ensure_paired_execution_group(_grouped_signal(instrument="BTC-USD", index=0))
    await _coord._register_and_arm_paired_leg(
        _grouped_signal(instrument="BTC-USD", index=0),
        _engine_stub("BTC-USD"),
        group_public_id="grp-1",
        command_public_id="cmd-0",
        client_order_id="oid-0",
    )
    _outbox_mock(_coord).notify.assert_not_called()
    await _coord._register_and_arm_paired_leg(
        _grouped_signal(instrument="ETH-USD", index=1),
        _engine_stub("ETH-USD"),
        group_public_id="grp-1",
        command_public_id="cmd-1",
        client_order_id="oid-1",
    )
    _outbox_mock(_coord).notify.assert_called_once()
    group = await _sql_repo(_coord).get_paired_execution_group("grp-1", datetime.now(UTC))
    assert group is not None
    assert group["status"] == "armed"


@pytest.mark.asyncio
async def test_on_signal_grouped_ensures_group_and_registers_leg(
    _coord: TraderCoordinator,
) -> None:
    """_on_signal on a grouped signal ensures the group and registers its leg.

    Given: a coordinator whose stub engine returns a DB command public id
        DISTINCT from the in-flight client order id, and a grouped signal,
    When: _on_signal processes the signal,
    Then: the group is ensured and the owned leg binds the DB command id
        (not the client order id) so the outbox gate can match the leg to
        its command, with the two-leg group staying assembling (not armed).
    """
    _coord._current_topic = "signals.kraken.BTC-USD.live"
    await _coord._on_signal(_standalone_signal())
    engine = _coord.engines["BTC-USD@kraken-live"]

    async def _stub_execute(*_args: Any, **_kwargs: Any) -> str:
        engine.pending_client_order_id = "oid-btc"
        return "cmd-btc"

    engine.execute_desired_units = AsyncMock(side_effect=_stub_execute)
    _coord._current_topic = "signals.kraken.BTC-USD.live"
    await _coord._on_signal(_grouped_signal(instrument="BTC-USD", index=0))

    _outbox_mock(_coord).notify.assert_not_called()
    legs = await _sql_repo(_coord).get_paired_execution_legs("grp-1", datetime.now(UTC))
    assert len(legs) == 1
    assert legs[0]["command_public_id"] == "cmd-btc"
    assert legs[0]["client_order_id"] == "oid-btc"
    assert legs[0]["leg_index"] == 0
    assert legs[0]["shard_key"] == "kraken.BTC-USD.live"


async def _insert_scope_halt(
    repo: SQLAlchemyRepository,
    *,
    wallet_public_id: str = "",
    strategy_id: str = "pairs-alpha",
    group_key: str = _GROUP_KEY,
    group_public_id: str = "grp-prior",
    reason: str = "paired-execution group broken",
) -> None:
    """Insert an active durable halt for a (wallet, strategy, group_key) scope."""
    halt_time = datetime(2026, 6, 8, 12, 0, tzinfo=UTC)
    await repo.insert_paired_execution_halt(
        {
            "wallet_public_id": wallet_public_id,
            "operator_public_id": None,
            "strategy_id": strategy_id,
            "mode": "live",
            "group_key": group_key,
            "group_public_id": group_public_id,
            "reason": reason,
            "created_at": halt_time,
            "session_id": "00000000-0000-0000-0000-000000000003",
            "sequence_id": 1,
            "timestamp": halt_time,
        }
    )


async def _insert_guard_group(
    repo: SQLAlchemyRepository,
    *,
    public_id: str,
    status: str,
    timestamp: datetime | None = None,
) -> None:
    """Insert a paired-execution group for recovery tests."""
    now = datetime(2026, 6, 8, 12, 0, tzinfo=UTC)
    stamp = timestamp if timestamp is not None else now
    await repo.insert_paired_execution_group(
        {
            "public_id": public_id,
            "wallet_public_id": "",
            "operator_public_id": None,
            "strategy_id": "pairs-alpha",
            "policy": "simultaneous",
            "expected_leg_count": 2,
            "group_key": _GROUP_KEY,
            "status": status,
            "assembly_deadline": now,
            "fill_deadline": now,
            "created_at": now,
            "session_id": "00000000-0000-0000-0000-000000000003",
            "sequence_id": 1,
            "timestamp": stamp,
        }
    )


async def _insert_guard_leg(
    repo: SQLAlchemyRepository,
    *,
    public_id: str,
    group_public_id: str,
    leg_index: int,
    shard_key: str,
    instrument: str = "BTC-USD",
    timestamp: datetime | None = None,
    side: str = "buy",
    client_order_id: str | None = None,
    command_public_id: str | None = None,
    filled_signed_qty: float = 0.0,
    compensated_signed_qty: float = 0.0,
    status: str = "pending",
) -> None:
    """Insert a paired-execution leg for recovery / fill-projection tests."""
    stamp = timestamp if timestamp is not None else datetime(2026, 6, 8, 12, 0, tzinfo=UTC)
    await repo.insert_paired_execution_leg(
        {
            "public_id": public_id,
            "group_public_id": group_public_id,
            "leg_index": leg_index,
            "exchange": "kraken",
            "mode": "live",
            "instrument": instrument,
            "shard_key": shard_key,
            "side": side,
            "target_qty": 1.0,
            "signal_public_id": f"sig-{public_id}",
            "command_public_id": command_public_id,
            "client_order_id": client_order_id,
            "filled_signed_qty": filled_signed_qty,
            "compensated_signed_qty": compensated_signed_qty,
            "status": status,
            "wallet_public_id": "",
            "operator_public_id": None,
            "created_at": stamp,
            "session_id": "00000000-0000-0000-0000-000000000003",
            "sequence_id": 1,
            "timestamp": stamp,
        }
    )


async def _insert_flatten_cmd(
    repo: SQLAlchemyRepository,
    *,
    public_id: str,
    supersedes_command_id: str,
    client_order_id: str,
    side: str,
) -> None:
    """Insert an active reduce-only flatten command superseding a leg's original."""
    stamp = datetime(2026, 6, 8, 12, 0, tzinfo=UTC)
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
                client_order_id=client_order_id,
                venue_client_id=client_order_id,
                side=side,
                order_type="market",
                quantity=1.0,
                price=None,
                reduce_only=True,
                status=TradeCommandStatusEnum.CREATED.value,
                created_at=stamp,
                correlation_id="00000000-0000-0000-0000-000000000111",
                supersedes_command_id=supersedes_command_id,
                idempotency_key=f"flatten:{client_order_id}",
                wallet_public_id="",
                session_id="00000000-0000-0000-0000-000000000003",
                sequence_id=1,
                timestamp=stamp,
            )
        )
        await session.commit()


def _fill(
    *,
    client_order_id: str,
    side: str = "buy",
    size: float = 5.0,
    status: str = "filled",
    instrument: str = "BTC-USD",
) -> ExecutionData:
    """Build an ExecutionData venue fill for a paired-execution leg."""
    return ExecutionData(
        public_id=f"exec-{client_order_id}",
        timestamp=datetime(2026, 6, 8, 12, 0, tzinfo=UTC),
        session_id="",
        sequence_id=0,
        trade_id=f"trade-{client_order_id}",
        exchange_order_id=f"exch-{client_order_id}",
        client_order_id=client_order_id,
        instrument=instrument,
        exchange="kraken",
        side=side,
        size=size,
        price=50000.0,
        last_size=size,
        last_price=50000.0,
        fee=0.0,
        fee_asset="USD",
        status=status,
        executed_at=datetime(2026, 6, 8, 12, 0, tzinfo=UTC),
    )


@pytest.mark.asyncio
async def test_halt_block_returns_false_for_standalone_signal(
    _coord: TraderCoordinator,
) -> None:
    """A non-grouped signal is never halt-checked.

    Given: a standalone signal (no paired_group_id),
    When: the grouped halt fast-reject runs,
    Then: it returns False so the cheap in-memory shard gate alone governs
        non-grouped signals.
    """
    assert await _coord._grouped_signal_blocked_by_halt(_standalone_signal()) is False


@pytest.mark.asyncio
async def test_halt_block_returns_false_when_group_key_missing(
    _coord: TraderCoordinator,
) -> None:
    """A grouped descriptor with no group key cannot be scoped, so it is allowed.

    Given: a grouped signal whose paired_group_key bypassed the validator as
        None,
    When: the grouped halt fast-reject runs,
    Then: it returns False (no scope to query) rather than guessing a halt.
    """
    signal = SignalData.model_construct(
        type="signal",
        public_id="sig-bad",
        timestamp=datetime(2026, 6, 8, 12, 0, tzinfo=UTC),
        session_id="",
        sequence_id=0,
        instrument="BTC-USD",
        exchange="kraken",
        side="buy",
        strength=0.5,
        reason="test",
        price=50000.0,
        strategy_name="pairs-alpha",
        fired_at=datetime(2026, 6, 8, 12, 0, tzinfo=UTC),
        paired_group_id="grp-1",
        paired_group_size=2,
        paired_group_index=0,
        paired_group_policy="simultaneous",
        paired_group_key=None,
    )
    assert await _coord._grouped_signal_blocked_by_halt(signal) is False


@pytest.mark.asyncio
async def test_halt_block_returns_false_for_non_sql_repository(
    _coord: TraderCoordinator,
) -> None:
    """The grouped halt check is skipped without a SQL repository.

    Given: a grouped signal but a non-SQL repository (tests),
    When: the grouped halt fast-reject runs,
    Then: it returns False — the guard tables do not exist, so there is no
        fail-closed obligation on the no-repository test path.
    """
    _coord.repository = AsyncMock()
    assert (
        await _coord._grouped_signal_blocked_by_halt(_grouped_signal(instrument="BTC-USD", index=0))
        is False
    )


@pytest.mark.asyncio
async def test_halt_block_returns_false_when_no_active_halt(
    _coord: TraderCoordinator,
) -> None:
    """A grouped signal with no halt on its scope is allowed through.

    Given: a SQL repository with no active halt for the signal's pair scope,
    When: the grouped halt fast-reject runs,
    Then: it returns False so the group can assemble normally.
    """
    assert (
        await _coord._grouped_signal_blocked_by_halt(_grouped_signal(instrument="BTC-USD", index=0))
        is False
    )


@pytest.mark.asyncio
async def test_halt_block_returns_true_when_pair_scope_halted(
    _coord: TraderCoordinator,
) -> None:
    """A grouped signal whose pair scope carries an active halt is rejected.

    Given: an active durable halt on the (wallet, strategy, group_key) scope,
    When: the grouped halt fast-reject runs for a signal on that scope,
    Then: it returns True so no new group is opened on a halted pair.
    """
    await _insert_scope_halt(_sql_repo(_coord))
    assert (
        await _coord._grouped_signal_blocked_by_halt(_grouped_signal(instrument="BTC-USD", index=0))
        is True
    )


@pytest.mark.asyncio
async def test_halt_block_fails_closed_on_db_error(
    _coord: TraderCoordinator,
) -> None:
    """A DB error during the halt check drops the grouped signal (fail closed).

    Given: a SQL repository whose halt query raises,
    When: the grouped halt fast-reject runs,
    Then: it returns True so a DB outage can never let a new grouped group open
        unchecked on a possibly-halted pair.
    """
    _sql_repo(_coord).get_active_paired_execution_halt = AsyncMock(
        side_effect=RuntimeError("db down")
    )
    assert (
        await _coord._grouped_signal_blocked_by_halt(_grouped_signal(instrument="BTC-USD", index=0))
        is True
    )


@pytest.mark.asyncio
async def test_on_signal_drops_grouped_signal_when_pair_scope_halted(
    _coord: TraderCoordinator,
) -> None:
    """_on_signal drops a grouped signal before opening a group on a halted pair.

    Given: a coordinator with an engine and an active halt on the signal's pair
        scope (but no in-memory shard halt),
    When: _on_signal processes a grouped signal for that scope,
    Then: the signal is dropped before _ensure_paired_execution_group, so no new
        group is created on a pair whose prior group broke with exposure.
    """
    _coord._current_topic = "signals.kraken.BTC-USD.live"
    await _coord._on_signal(_standalone_signal())
    await _insert_scope_halt(_sql_repo(_coord))
    _coord._current_topic = "signals.kraken.BTC-USD.live"
    await _coord._on_signal(_grouped_signal(instrument="BTC-USD", index=0))
    group = await _sql_repo(_coord).get_paired_execution_group("grp-1", datetime.now(UTC))
    assert group is None


@pytest.mark.asyncio
async def test_guard_recovery_skips_non_sql_repository(_coord: TraderCoordinator) -> None:
    """Guard-state recovery is a no-op without a SQL repository.

    Given: a non-SQL repository (tests),
    When: paired-execution guard recovery runs,
    Then: nothing is halted and the outbox is not woken — the guard tables do
        not exist on a non-SQL repo.
    """
    _coord.repository = AsyncMock()
    _coord._ownership = ShardOwnership(instance_id=0, instance_count=1)
    await _coord._recover_paired_execution_guard_state()
    assert _coord.trade_service.is_halted(_BTC_SHARD) is False
    _outbox_mock(_coord).notify.assert_not_called()


@pytest.mark.asyncio
async def test_guard_recovery_skips_without_ownership(_coord: TraderCoordinator) -> None:
    """Guard-state recovery is a no-op without shard ownership.

    Given: a SQL repository with an active halt but no shard ownership wired,
    When: paired-execution guard recovery runs,
    Then: nothing is halted — ownership is required to scope owned legs.
    """
    _coord._ownership = None
    await _insert_guard_group(_sql_repo(_coord), public_id="grp-b", status="broken")
    await _insert_guard_leg(
        _sql_repo(_coord),
        public_id="leg-0",
        group_public_id="grp-b",
        leg_index=0,
        shard_key=_BTC_SHARD,
    )
    await _insert_scope_halt(_sql_repo(_coord), group_public_id="grp-b")
    await _coord._recover_paired_execution_guard_state()
    assert _coord.trade_service.is_halted(_BTC_SHARD) is False


@pytest.mark.asyncio
async def test_guard_recovery_mirrors_owned_halt_and_wakes_outbox(
    _coord: TraderCoordinator,
) -> None:
    """Recovery re-halts owned shards from durable halts and wakes the outbox.

    Given: an active durable halt over a broken group with an owned leg, plus an
        armed group with an owned leg,
    When: paired-execution guard recovery runs,
    Then: the owned leg's shard is halted in memory (mirror restored from the
        authoritative durable halt) and the outbox is woken once so the armed
        group's held commands re-dispatch without waiting for the first poll.
    """
    _coord._ownership = ShardOwnership(instance_id=0, instance_count=1)
    await _insert_guard_group(_sql_repo(_coord), public_id="grp-b", status="broken")
    await _insert_guard_leg(
        _sql_repo(_coord),
        public_id="leg-b",
        group_public_id="grp-b",
        leg_index=0,
        shard_key=_BTC_SHARD,
    )
    await _insert_scope_halt(_sql_repo(_coord), group_public_id="grp-b")
    await _insert_guard_group(_sql_repo(_coord), public_id="grp-a", status="armed")
    await _insert_guard_leg(
        _sql_repo(_coord),
        public_id="leg-a",
        group_public_id="grp-a",
        leg_index=0,
        shard_key=_ETH_SHARD,
        instrument="ETH-USD",
    )
    await _coord._recover_paired_execution_guard_state()
    assert _coord.trade_service.is_halted(_BTC_SHARD) is True
    _outbox_mock(_coord).notify.assert_called_once()


@pytest.mark.asyncio
async def test_guard_recovery_skips_non_owned_shard(_coord: TraderCoordinator) -> None:
    """Recovery does not halt a shard whose leg this coordinator does not own.

    Given: a 2-instance coordinator and an active halt whose only leg sits on a
        non-owned shard,
    When: paired-execution guard recovery runs,
    Then: the non-owned shard is left un-halted for its owning coordinator.
    """
    _coord._ownership = ShardOwnership(instance_id=0, instance_count=2)
    await _insert_guard_group(_sql_repo(_coord), public_id="grp-b", status="broken")
    await _insert_guard_leg(
        _sql_repo(_coord),
        public_id="leg-b",
        group_public_id="grp-b",
        leg_index=0,
        shard_key=_BTC_SHARD,
    )
    await _insert_scope_halt(_sql_repo(_coord), group_public_id="grp-b")
    await _coord._recover_paired_execution_guard_state()
    assert _coord.trade_service.is_halted(_BTC_SHARD) is False


@pytest.mark.asyncio
async def test_guard_recovery_does_not_restore_cleared_halt(_coord: TraderCoordinator) -> None:
    """A cleared durable halt is never re-halted on restart.

    Given: a durable halt over a broken group with an owned leg, then cleared,
    When: paired-execution guard recovery runs,
    Then: the owned shard is NOT halted — recovery restores from active durable
        halts only, so an operator/Phase-5 clear is honoured across a restart
        (a group-driven restore would wrongly re-wedge the still-broken group).
    """
    _coord._ownership = ShardOwnership(instance_id=0, instance_count=1)
    await _insert_guard_group(_sql_repo(_coord), public_id="grp-b", status="broken")
    await _insert_guard_leg(
        _sql_repo(_coord),
        public_id="leg-b",
        group_public_id="grp-b",
        leg_index=0,
        shard_key=_BTC_SHARD,
    )
    await _insert_scope_halt(_sql_repo(_coord), group_public_id="grp-b")
    halt = await _sql_repo(_coord).get_active_paired_execution_halt("", "pairs-alpha", _GROUP_KEY)
    assert halt is not None
    await _sql_repo(_coord).clear_paired_execution_halt(halt["public_id"], datetime.now(UTC))
    await _coord._recover_paired_execution_guard_state()
    assert _coord.trade_service.is_halted(_BTC_SHARD) is False


@pytest.mark.asyncio
async def test_guard_recovery_does_not_wake_outbox_without_owned_armed_leg(
    _coord: TraderCoordinator,
) -> None:
    """The outbox is not woken when no armed group has an owned leg.

    Given: a 2-instance coordinator and an armed group whose only leg is on a
        non-owned shard, with no halts,
    When: paired-execution guard recovery runs,
    Then: the outbox is not woken — there is nothing of this coordinator's to
        re-dispatch.
    """
    _coord._ownership = ShardOwnership(instance_id=0, instance_count=2)
    await _insert_guard_group(_sql_repo(_coord), public_id="grp-a", status="armed")
    await _insert_guard_leg(
        _sql_repo(_coord),
        public_id="leg-a",
        group_public_id="grp-a",
        leg_index=0,
        shard_key=_BTC_SHARD,
    )
    await _coord._recover_paired_execution_guard_state()
    _outbox_mock(_coord).notify.assert_not_called()


@pytest.mark.asyncio
async def test_guard_recovery_mirrors_halt_even_without_outbox(_coord: TraderCoordinator) -> None:
    """Recovery still restores the halt mirror when the outbox is disabled.

    Given: a SQL repository with an active halt over an owned leg but no outbox,
    When: paired-execution guard recovery runs,
    Then: the owned shard is halted and no error is raised (the outbox wake is
        skipped when the outbox is None).
    """
    _coord._ownership = ShardOwnership(instance_id=0, instance_count=1)
    _coord.outbox = None
    await _insert_guard_group(_sql_repo(_coord), public_id="grp-b", status="broken")
    await _insert_guard_leg(
        _sql_repo(_coord),
        public_id="leg-b",
        group_public_id="grp-b",
        leg_index=0,
        shard_key=_BTC_SHARD,
    )
    await _insert_scope_halt(_sql_repo(_coord), group_public_id="grp-b")
    await _insert_guard_group(_sql_repo(_coord), public_id="grp-a", status="armed")
    await _insert_guard_leg(
        _sql_repo(_coord),
        public_id="leg-a",
        group_public_id="grp-a",
        leg_index=0,
        shard_key=_ETH_SHARD,
        instrument="ETH-USD",
    )
    await _coord._recover_paired_execution_guard_state()
    assert _coord.trade_service.is_halted(_BTC_SHARD) is True


@pytest.mark.asyncio
async def test_guard_recovery_halt_mirror_failure_still_wakes_outbox(
    _coord: TraderCoordinator,
) -> None:
    """A halt-mirror DB error is isolated and the independent outbox wake still fires.

    Given: a real SQL repository with an armed owned group, whose active-halt
        query is forced to raise,
    When: paired-execution guard recovery runs,
    Then: the mirror error is swallowed (logged) and the outbox is still woken
        for the owned armed group, so a transient mirror error never suppresses
        the independent re-dispatch kick nor blocks startup.
    """
    _coord._ownership = ShardOwnership(instance_id=0, instance_count=1)
    await _insert_guard_group(_sql_repo(_coord), public_id="grp-a", status="armed")
    await _insert_guard_leg(
        _sql_repo(_coord),
        public_id="leg-a",
        group_public_id="grp-a",
        leg_index=0,
        shard_key=_ETH_SHARD,
        instrument="ETH-USD",
    )
    _sql_repo(_coord).list_active_paired_execution_halts = AsyncMock(
        side_effect=RuntimeError("db down")
    )
    await _coord._recover_paired_execution_guard_state()
    _outbox_mock(_coord).notify.assert_called_once()


@pytest.mark.asyncio
async def test_guard_recovery_outbox_wake_failure_keeps_halt_mirror(
    _coord: TraderCoordinator,
) -> None:
    """An outbox-wake DB error is swallowed after the halt mirror already ran.

    Given: a real SQL repository with an active owned halt, whose armed-group
        query is forced to raise,
    When: paired-execution guard recovery runs,
    Then: the mirror restored the owned shard halt FIRST, and the wake error is
        swallowed (logged), so a wake failure neither undoes the safety-critical
        mirror nor blocks startup.
    """
    _coord._ownership = ShardOwnership(instance_id=0, instance_count=1)
    await _insert_guard_group(_sql_repo(_coord), public_id="grp-b", status="broken")
    await _insert_guard_leg(
        _sql_repo(_coord),
        public_id="leg-b",
        group_public_id="grp-b",
        leg_index=0,
        shard_key=_BTC_SHARD,
    )
    await _insert_scope_halt(_sql_repo(_coord), group_public_id="grp-b")
    _sql_repo(_coord).list_active_paired_execution_groups = AsyncMock(
        side_effect=RuntimeError("db down")
    )
    await _coord._recover_paired_execution_guard_state()
    assert _coord.trade_service.is_halted(_BTC_SHARD) is True


@pytest.mark.asyncio
async def test_guard_recovery_mirrors_owned_halt_for_clock_skewed_future_leg(
    _coord: TraderCoordinator,
) -> None:
    """Recovery halts an owned shard whose leg has a future (clock-skewed) timestamp.

    Given: an active durable halt over a broken group whose only owned leg was
        stamped with a timestamp far in the FUTURE relative to recovery's wall
        clock (a sibling coordinator's clock skew),
    When: paired-execution guard recovery runs,
    Then: the owned shard is still halted, because the mirror reads CURRENT active
        legs (known_to==MAX) rather than a temporal as-of view that would exclude
        a future-stamped leg and leave the shard un-halted until the next scan.
    """
    _coord._ownership = ShardOwnership(instance_id=0, instance_count=1)
    await _insert_guard_group(_sql_repo(_coord), public_id="grp-b", status="broken")
    await _insert_guard_leg(
        _sql_repo(_coord),
        public_id="leg-b",
        group_public_id="grp-b",
        leg_index=0,
        shard_key=_BTC_SHARD,
        timestamp=datetime(2099, 1, 1, tzinfo=UTC),
    )
    await _insert_scope_halt(_sql_repo(_coord), group_public_id="grp-b")
    await _coord._recover_paired_execution_guard_state()
    assert _coord.trade_service.is_halted(_BTC_SHARD) is True


@pytest.mark.asyncio
async def test_guard_recovery_per_halt_leg_error_does_not_skip_other_halts(
    _coord: TraderCoordinator,
) -> None:
    """A per-halt leg-read error skips only that halt; other halted pairs still mirror.

    Given: two active halts on distinct pair scopes whose leg reads are ordered so
        the first raises and the second returns an owned leg,
    When: paired-execution guard recovery runs,
    Then: the first halt is logged and skipped while the SECOND halt's owned shard
        is still halted, so one transient per-halt error never drops the mirror for
        the other halted pairs.
    """
    _coord._ownership = ShardOwnership(instance_id=0, instance_count=1)
    await _insert_scope_halt(_sql_repo(_coord), group_key=_GROUP_KEY, group_public_id="grp-bad")
    await _insert_scope_halt(
        _sql_repo(_coord),
        group_key="kraken:ADA-USD:live|kraken:SOL-USD:live",
        group_public_id="grp-good",
    )

    async def _legs(group_public_id: str) -> list[dict[str, str]]:
        if group_public_id == "grp-bad":
            raise RuntimeError("db down")
        return [{"shard_key": _BTC_SHARD}]

    _sql_repo(_coord).get_current_paired_execution_legs = AsyncMock(side_effect=_legs)
    await _coord._recover_paired_execution_guard_state()
    assert _coord.trade_service.is_halted(_BTC_SHARD) is True


@pytest.mark.asyncio
async def test_project_leg_fill_sets_signed_cumulative_on_owned_buy_leg(
    _coord: TraderCoordinator,
) -> None:
    """A buy-leg fill sets the leg's positive signed cumulative and FILLED status.

    Given: an armed buy leg bound to a client_order_id,
    When: _project_paired_execution_leg_fill runs for a 'filled' fill on that
        client_order_id,
    Then: the leg records filled_signed_qty=+size and status FILLED, so the guard
        scanner sees real exposure on the pair.
    """
    await _insert_guard_group(_sql_repo(_coord), public_id="grp-f", status="armed")
    await _insert_guard_leg(
        _sql_repo(_coord),
        public_id="leg-f",
        group_public_id="grp-f",
        leg_index=0,
        shard_key=_BTC_SHARD,
        side="buy",
        client_order_id="cid-1",
        status="armed",
    )
    await _coord._project_paired_execution_leg_fill(
        _fill(client_order_id="cid-1", side="buy", size=5.0, status="filled"),
        cast(VenueEventRow, {"id": 99}),
    )
    legs = await _sql_repo(_coord).get_current_paired_execution_legs("grp-f")
    assert legs[0]["filled_signed_qty"] == 5.0
    assert legs[0]["status"] == "filled"
    assert legs[0]["last_venue_event_id"] == 99


@pytest.mark.asyncio
async def test_project_leg_fill_sell_leg_negative_qty_and_partial_status(
    _coord: TraderCoordinator,
) -> None:
    """A sell-leg partial fill records a negative signed cumulative and PARTIALLY_FILLED.

    Given: a working sell leg bound to a client_order_id,
    When: _project_paired_execution_leg_fill runs for a 'partial' fill,
    Then: filled_signed_qty=-size and status PARTIALLY_FILLED, preserving the
        signed-qty model and the partial-vs-filled distinction.
    """
    await _insert_guard_group(_sql_repo(_coord), public_id="grp-f", status="armed")
    await _insert_guard_leg(
        _sql_repo(_coord),
        public_id="leg-f",
        group_public_id="grp-f",
        leg_index=0,
        shard_key=_ETH_SHARD,
        instrument="ETH-USD",
        side="sell",
        client_order_id="cid-2",
        status="working",
    )
    await _coord._project_paired_execution_leg_fill(
        _fill(
            client_order_id="cid-2", side="sell", size=4.0, status="partial", instrument="ETH-USD"
        ),
        cast(VenueEventRow, {"id": 7}),
    )
    legs = await _sql_repo(_coord).get_current_paired_execution_legs("grp-f")
    assert legs[0]["filled_signed_qty"] == -4.0
    assert legs[0]["status"] == "partially_filled"


@pytest.mark.asyncio
async def test_project_leg_fill_noop_without_sql_repository(_coord: TraderCoordinator) -> None:
    """Fill projection is skipped without a SQL repository.

    Given: a non-SQL repository (tests),
    When: _project_paired_execution_leg_fill runs,
    Then: the DAL projection is never called — the paired-execution tables do not
        exist on a non-SQL repo.
    """
    repo = AsyncMock()
    _coord.repository = repo
    await _coord._project_paired_execution_leg_fill(
        _fill(client_order_id="cid-1"),
        cast(VenueEventRow, {"id": 1}),
    )
    repo.project_paired_execution_leg_fill.assert_not_called()


@pytest.mark.asyncio
async def test_project_leg_fill_routes_flatten_fill_to_compensation_when_enabled(
    _coord: TraderCoordinator, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A flatten order's live fill lands on the leg's compensated qty when the guard is on.

    Given: the guard enabled, a compensating long leg (filled=+5) bound to original
        command 'cmd-x', and a reduce-only SELL flatten order (fresh client_order_id
        'flat') superseding 'cmd-x' that filled 5,
    When: _project_paired_execution_leg_fill runs for the flatten order's fill (which
        matches no leg directly, so the original projection is NO_MATCH),
    Then: the fill is re-routed to the compensation projection, compensated_signed_qty
        becomes +5 and the leg is FLATTENED.
    """
    monkeypatch.setattr(
        trader_module._bootstrap_settings, "paired_execution_guard_enabled", True, raising=True
    )
    await _insert_guard_group(_sql_repo(_coord), public_id="grp-c", status="compensating")
    await _insert_guard_leg(
        _sql_repo(_coord),
        public_id="leg-c",
        group_public_id="grp-c",
        leg_index=0,
        shard_key=_BTC_SHARD,
        side="buy",
        client_order_id="orig-c",
        command_public_id="cmd-x",
        filled_signed_qty=5.0,
        status=PairedExecutionLegStatusEnum.COMPENSATING.value,
    )
    await _insert_flatten_cmd(
        _sql_repo(_coord),
        public_id="flatcmd-1",
        supersedes_command_id="cmd-x",
        client_order_id="flat",
        side="sell",
    )
    await _insert_fill_event(_sql_repo(_coord), client_order_id="flat", cum_fill_size=5.0)
    await _coord._project_paired_execution_leg_fill(
        _fill(client_order_id="flat", side="sell", size=5.0, status="filled"),
        cast(VenueEventRow, {"id": 1}),
    )
    legs = await _sql_repo(_coord).get_current_paired_execution_legs("grp-c")
    assert legs[0]["compensated_signed_qty"] == 5.0
    assert legs[0]["status"] == PairedExecutionLegStatusEnum.FLATTENED.value


@pytest.mark.asyncio
async def test_project_leg_fill_skips_compensation_when_guard_disabled(
    _coord: TraderCoordinator,
) -> None:
    """A non-matching fill never reaches the compensation projection while the guard is dark.

    Given: the guard disabled (default) and a fill whose client_order_id matches no
        leg,
    When: _project_paired_execution_leg_fill runs,
    Then: the original projection returns NO_MATCH but the compensation projection is
        NOT called, so a non-paired deployment never pays the second lookup.
    """
    _sql_repo(_coord).project_paired_execution_compensation_fill = AsyncMock()
    await _coord._project_paired_execution_leg_fill(
        _fill(client_order_id="unmatched", side="sell", size=5.0, status="filled"),
        cast(VenueEventRow, {"id": 1}),
    )
    _sql_repo(_coord).project_paired_execution_compensation_fill.assert_not_called()


@pytest.mark.asyncio
async def test_recover_leg_fills_reprojects_compensation_when_enabled(
    _coord: TraderCoordinator, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Recovery restores a leg's compensated qty from a downtime flatten fill when enabled.

    Given: the guard enabled, an owned compensating long leg (filled=+5, stale
        compensated=0) whose SELL flatten order filled 5 while the coordinator was
        down,
    When: the recovery leg-fill pass runs,
    Then: it re-derives compensated=+5 from venue_events and FLATTENS the leg —
        compensation recovery parity, gated on the feature flag.
    """
    monkeypatch.setattr(
        trader_module._bootstrap_settings, "paired_execution_guard_enabled", True, raising=True
    )
    _coord._ownership = ShardOwnership(instance_id=0, instance_count=1)
    await _insert_guard_group(_sql_repo(_coord), public_id="grp-r", status="compensating")
    await _insert_guard_leg(
        _sql_repo(_coord),
        public_id="leg-r",
        group_public_id="grp-r",
        leg_index=0,
        shard_key=_BTC_SHARD,
        side="buy",
        client_order_id="orig-r",
        command_public_id="cmd-r",
        filled_signed_qty=5.0,
        status=PairedExecutionLegStatusEnum.COMPENSATING.value,
    )
    await _insert_flatten_cmd(
        _sql_repo(_coord),
        public_id="flatcmd-r",
        supersedes_command_id="cmd-r",
        client_order_id="flat-r",
        side="sell",
    )
    await _insert_fill_event(_sql_repo(_coord), client_order_id="flat-r", cum_fill_size=5.0)
    await _coord._recover_paired_execution_leg_fills()
    legs = await _sql_repo(_coord).get_current_paired_execution_legs("grp-r")
    assert legs[0]["compensated_signed_qty"] == 5.0
    assert legs[0]["status"] == PairedExecutionLegStatusEnum.FLATTENED.value


@pytest.mark.asyncio
async def test_recover_leg_fills_covers_manual_intervention_group(
    _coord: TraderCoordinator, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Recovery re-derives compensation for a manual_intervention group's leg too.

    Given: the guard enabled and an owned manual_intervention long leg (filled=+5,
        stale compensated=0) whose SELL flatten filled 5 during downtime — a leg can
        be manual while a sibling's flatten is still in flight,
    When: the recovery leg-fill pass runs,
    Then: it restores compensated=+5 but leaves the status manual_intervention
        (auto-flatten never clears an operator escalation), proving the recovery
        group list covers manual_intervention.
    """
    monkeypatch.setattr(
        trader_module._bootstrap_settings, "paired_execution_guard_enabled", True, raising=True
    )
    _coord._ownership = ShardOwnership(instance_id=0, instance_count=1)
    await _insert_guard_group(_sql_repo(_coord), public_id="grp-m", status="manual_intervention")
    await _insert_guard_leg(
        _sql_repo(_coord),
        public_id="leg-m",
        group_public_id="grp-m",
        leg_index=0,
        shard_key=_BTC_SHARD,
        side="buy",
        client_order_id="orig-m",
        command_public_id="cmd-m",
        filled_signed_qty=5.0,
        status=PairedExecutionLegStatusEnum.MANUAL_INTERVENTION.value,
    )
    await _insert_flatten_cmd(
        _sql_repo(_coord),
        public_id="flatcmd-m",
        supersedes_command_id="cmd-m",
        client_order_id="flat-m",
        side="sell",
    )
    await _insert_fill_event(_sql_repo(_coord), client_order_id="flat-m", cum_fill_size=5.0)
    await _coord._recover_paired_execution_leg_fills()
    legs = await _sql_repo(_coord).get_current_paired_execution_legs("grp-m")
    assert legs[0]["compensated_signed_qty"] == 5.0
    assert legs[0]["status"] == PairedExecutionLegStatusEnum.MANUAL_INTERVENTION.value


def _order_status(*, client_order_id: str, status: str, instrument: str = "BTC-USD") -> OrderData:
    """Build an OrderData status frame for a paired-execution leg's order."""
    return OrderData(
        public_id=f"os-{client_order_id}",
        timestamp=datetime(2026, 6, 8, 12, 0, tzinfo=UTC),
        session_id="",
        sequence_id=0,
        exchange_order_id=f"exch-{client_order_id}",
        client_order_id=client_order_id,
        instrument=instrument,
        exchange="kraken",
        side="buy",
        size=1.0,
        price=50000.0,
        order_type="market",
        status=status,
        filled_size=0.0,
        created_at=datetime(2026, 6, 8, 12, 0, tzinfo=UTC),
    )


def _order_event(
    *, client_order_id: str, event: str, instrument: str = "BTC-USD"
) -> OrderEventData:
    """Build an OrderEventData frame for a paired-execution leg's order."""
    return OrderEventData(
        type="order_event",
        public_id=f"oe-{client_order_id}",
        timestamp=datetime(2026, 6, 8, 12, 0, tzinfo=UTC),
        session_id="",
        sequence_id=0,
        event=event,
        client_order_id=client_order_id,
        exchange_order_id=f"exch-{client_order_id}",
        instrument=instrument,
        exchange="kraken",
    )


@pytest.mark.asyncio
async def test_project_leg_terminal_marks_owned_leg_terminal(
    _coord: TraderCoordinator,
) -> None:
    """A terminal projection records the leg's terminal status, preserving fill.

    Given: a working leg bound to a client_order_id,
    When: _project_paired_execution_leg_terminal runs with a 'cancelled' status,
    Then: the leg records the cancelled status and the new exchange_order_id, so
        the guard scanner's armed sweep can break the group on a terminal leg.
    """
    await _insert_guard_group(_sql_repo(_coord), public_id="grp-t", status="armed")
    await _insert_guard_leg(
        _sql_repo(_coord),
        public_id="leg-t",
        group_public_id="grp-t",
        leg_index=0,
        shard_key=_BTC_SHARD,
        side="buy",
        client_order_id="cid-t",
        status="working",
    )
    await _coord._project_paired_execution_leg_terminal("cid-t", "cancelled", "exch-new")
    legs = await _sql_repo(_coord).get_current_paired_execution_legs("grp-t")
    assert legs[0]["status"] == "cancelled"
    assert legs[0]["exchange_order_id"] == "exch-new"


@pytest.mark.asyncio
async def test_project_leg_terminal_noop_without_sql_repository(
    _coord: TraderCoordinator,
) -> None:
    """Terminal projection is skipped without a SQL repository.

    Given: a non-SQL repository (tests),
    When: _project_paired_execution_leg_terminal runs,
    Then: the DAL projection is never called.
    """
    repo = AsyncMock()
    _coord.repository = repo
    await _coord._project_paired_execution_leg_terminal("cid-t", "cancelled", "exch")
    repo.project_paired_execution_leg_terminal.assert_not_called()


@pytest.mark.asyncio
async def test_handle_order_status_rejected_projects_leg_rejected(
    _coord: TraderCoordinator,
) -> None:
    """A submit rejection projects REJECTED onto the leg via the live handler.

    Given: a working leg bound to a client_order_id,
    When: a 'rejected' order-status frame for that client_order_id is handled,
    Then: the leg becomes rejected — the armed sweep can break the group.
    """
    await _insert_guard_group(_sql_repo(_coord), public_id="grp-t", status="armed")
    await _insert_guard_leg(
        _sql_repo(_coord),
        public_id="leg-t",
        group_public_id="grp-t",
        leg_index=0,
        shard_key=_BTC_SHARD,
        client_order_id="cid-t",
        status="working",
    )
    await _coord._handle_order_status(
        "orders.events.kraken.BTC-USD.rejected",
        _order_status(client_order_id="cid-t", status="rejected"),
    )
    legs = await _sql_repo(_coord).get_current_paired_execution_legs("grp-t")
    assert legs[0]["status"] == "rejected"


@pytest.mark.asyncio
async def test_handle_order_status_accepted_does_not_project_leg(
    _coord: TraderCoordinator,
) -> None:
    """A non-terminal status leaves the leg unchanged.

    Given: a working leg bound to a client_order_id,
    When: an 'accepted' order-status frame is handled,
    Then: the leg stays working — only terminal statuses break the leg.
    """
    await _insert_guard_group(_sql_repo(_coord), public_id="grp-t", status="armed")
    await _insert_guard_leg(
        _sql_repo(_coord),
        public_id="leg-t",
        group_public_id="grp-t",
        leg_index=0,
        shard_key=_BTC_SHARD,
        client_order_id="cid-t",
        status="working",
    )
    await _coord._handle_order_status(
        "orders.events.kraken.BTC-USD.accepted",
        _order_status(client_order_id="cid-t", status="accepted"),
    )
    legs = await _sql_repo(_coord).get_current_paired_execution_legs("grp-t")
    assert legs[0]["status"] == "working"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("event", "expected_status"),
    [("cancelled", "cancelled"), ("expired", "expired")],
)
async def test_handle_order_event_terminal_projects_leg(
    _coord: TraderCoordinator, event: str, expected_status: str
) -> None:
    """A cancelled / expired event projects the matching terminal leg status.

    Given: a working leg bound to a client_order_id,
    When: a 'cancelled' / 'expired' order event for that id is handled,
    Then: the leg records the corresponding terminal status.
    """
    await _insert_guard_group(_sql_repo(_coord), public_id="grp-t", status="armed")
    await _insert_guard_leg(
        _sql_repo(_coord),
        public_id="leg-t",
        group_public_id="grp-t",
        leg_index=0,
        shard_key=_BTC_SHARD,
        client_order_id="cid-t",
        status="working",
    )
    await _coord._handle_order_event(
        f"orders.events.kraken.BTC-USD.{event}",
        _order_event(client_order_id="cid-t", event=event),
    )
    legs = await _sql_repo(_coord).get_current_paired_execution_legs("grp-t")
    assert legs[0]["status"] == expected_status


@pytest.mark.asyncio
async def test_handle_order_event_rejected_does_not_project_leg(
    _coord: TraderCoordinator,
) -> None:
    """A cancel/replace rejection leaves the original order's leg unchanged.

    Given: a working leg bound to a client_order_id,
    When: a 'rejected' order EVENT (a rejected cancel/replace, not a submit
        rejection) is handled,
    Then: the leg stays working — the original order is still live.
    """
    await _insert_guard_group(_sql_repo(_coord), public_id="grp-t", status="armed")
    await _insert_guard_leg(
        _sql_repo(_coord),
        public_id="leg-t",
        group_public_id="grp-t",
        leg_index=0,
        shard_key=_BTC_SHARD,
        client_order_id="cid-t",
        status="working",
    )
    await _coord._handle_order_event(
        "orders.events.kraken.BTC-USD.rejected",
        _order_event(client_order_id="cid-t", event="rejected"),
    )
    legs = await _sql_repo(_coord).get_current_paired_execution_legs("grp-t")
    assert legs[0]["status"] == "working"


async def _insert_fill_event(
    repo: SQLAlchemyRepository,
    *,
    client_order_id: str,
    cum_fill_size: float | None,
    shard_key: str = _BTC_SHARD,
    side: str = "buy",
    status: str = "filled",
    instrument: str = "BTC-USD",
) -> None:
    """Insert a fill_observed venue event for recovery fill-parity tests."""
    await repo.insert_venue_event(
        {
            "event_type": "fill_observed",
            "shard_key": shard_key,
            "wallet_public_id": "",
            "exchange": "kraken",
            "instrument": instrument,
            "mode": "live",
            "client_order_id": client_order_id,
            "exchange_order_id": f"exch-{client_order_id}",
            "side": side,
            "status": status,
            "fill_size": cum_fill_size,
            "cum_fill_size": cum_fill_size,
            "received_at": datetime(2026, 6, 8, 12, 0, tzinfo=UTC),
            "session_id": "00000000-0000-0000-0000-000000000003",
            "sequence_id": 1,
            "timestamp": datetime(2026, 6, 8, 12, 0, tzinfo=UTC),
        }
    )


async def _leg_fill_qty(coord: TraderCoordinator, *, group: str, leg_index: int = 0) -> float:
    """Return a recovered group's leg filled_signed_qty."""
    legs = await _sql_repo(coord).get_current_paired_execution_legs(group)
    qty: float = next(leg["filled_signed_qty"] for leg in legs if leg["leg_index"] == leg_index)
    return qty


@pytest.mark.asyncio
async def test_recover_leg_fills_projects_owned_leg_from_venue_event(
    _coord: TraderCoordinator,
) -> None:
    """A downtime fill in venue_events is re-projected onto its owned leg on restart.

    Given: an armed group with an owned buy leg whose order has a fill_observed
        venue event (cumulative 5, filled) but a leg still at filled_signed_qty 0,
    When: the recovery leg-fill pass runs,
    Then: the leg's filled_signed_qty becomes 5 and status FILLED, so a fill
        observed while the coordinator was down is restored for the guard scanner.
    """
    _coord._ownership = ShardOwnership(instance_id=0, instance_count=1)
    await _insert_guard_group(_sql_repo(_coord), public_id="grp-a", status="armed")
    await _insert_guard_leg(
        _sql_repo(_coord),
        public_id="leg-a",
        group_public_id="grp-a",
        leg_index=0,
        shard_key=_BTC_SHARD,
        side="buy",
        client_order_id="cid-1",
        status="armed",
    )
    await _insert_fill_event(_sql_repo(_coord), client_order_id="cid-1", cum_fill_size=5.0)
    await _coord._recover_paired_execution_leg_fills()
    legs = await _sql_repo(_coord).get_current_paired_execution_legs("grp-a")
    assert legs[0]["filled_signed_qty"] == 5.0
    assert legs[0]["status"] == "filled"


@pytest.mark.asyncio
async def test_recover_leg_fills_noop_when_no_venue_event(_coord: TraderCoordinator) -> None:
    """A leg with no fill event is left at zero (no order filled while down).

    Given: an armed group with an owned leg but NO fill_observed venue event,
    When: the recovery leg-fill pass runs,
    Then: the leg stays at filled_signed_qty 0 — nothing to restore.
    """
    _coord._ownership = ShardOwnership(instance_id=0, instance_count=1)
    await _insert_guard_group(_sql_repo(_coord), public_id="grp-a", status="armed")
    await _insert_guard_leg(
        _sql_repo(_coord),
        public_id="leg-a",
        group_public_id="grp-a",
        leg_index=0,
        shard_key=_BTC_SHARD,
        client_order_id="cid-1",
        status="armed",
    )
    await _coord._recover_paired_execution_leg_fills()
    assert await _leg_fill_qty(_coord, group="grp-a") == 0.0


@pytest.mark.asyncio
async def test_recover_leg_fills_skips_non_owned_leg(_coord: TraderCoordinator) -> None:
    """A leg on a non-owned shard is left for its owning coordinator to recover.

    Given: a 2-instance coordinator and an armed group whose leg is on a non-owned
        shard with a fill event,
    When: the recovery leg-fill pass runs,
    Then: the non-owned leg is not projected (filled stays 0).
    """
    ownership = ShardOwnership(instance_id=0, instance_count=2)
    not_owned = next(s for s in (_BTC_SHARD, _ETH_SHARD) if not ownership.owns(s))
    _coord._ownership = ownership
    await _insert_guard_group(_sql_repo(_coord), public_id="grp-a", status="armed")
    await _insert_guard_leg(
        _sql_repo(_coord),
        public_id="leg-a",
        group_public_id="grp-a",
        leg_index=0,
        shard_key=not_owned,
        client_order_id="cid-1",
        status="armed",
    )
    await _insert_fill_event(
        _sql_repo(_coord), client_order_id="cid-1", cum_fill_size=5.0, shard_key=not_owned
    )
    await _coord._recover_paired_execution_leg_fills()
    assert await _leg_fill_qty(_coord, group="grp-a") == 0.0


@pytest.mark.asyncio
async def test_recover_leg_fills_skips_leg_without_client_order_id(
    _coord: TraderCoordinator,
) -> None:
    """A leg with no client_order_id is skipped (never dispatched).

    Given: an armed group with an owned leg whose client_order_id is None,
    When: the recovery leg-fill pass runs,
    Then: it is skipped without error.
    """
    _coord._ownership = ShardOwnership(instance_id=0, instance_count=1)
    await _insert_guard_group(_sql_repo(_coord), public_id="grp-a", status="armed")
    await _insert_guard_leg(
        _sql_repo(_coord),
        public_id="leg-a",
        group_public_id="grp-a",
        leg_index=0,
        shard_key=_BTC_SHARD,
        client_order_id=None,
        status="armed",
    )
    await _coord._recover_paired_execution_leg_fills()
    assert await _leg_fill_qty(_coord, group="grp-a") == 0.0


@pytest.mark.asyncio
async def test_recover_leg_fills_monotonic_noop_for_already_projected_leg(
    _coord: TraderCoordinator,
) -> None:
    """An already-projected leg is not regressed by a smaller recovered cumulative.

    Given: an owned leg already at filled_signed_qty 8 and a fill event with a
        smaller cumulative 5,
    When: the recovery leg-fill pass runs,
    Then: the monotonic projection DAL leaves the leg at 8 (no regression), so a
        live-projected leg is a recovery no-op.
    """
    _coord._ownership = ShardOwnership(instance_id=0, instance_count=1)
    await _insert_guard_group(_sql_repo(_coord), public_id="grp-a", status="armed")
    await _sql_repo(_coord).insert_paired_execution_leg(
        {
            "public_id": "leg-a",
            "group_public_id": "grp-a",
            "leg_index": 0,
            "exchange": "kraken",
            "mode": "live",
            "instrument": "BTC-USD",
            "shard_key": _BTC_SHARD,
            "side": "buy",
            "target_qty": 10.0,
            "signal_public_id": "sig-leg-a",
            "command_public_id": None,
            "client_order_id": "cid-1",
            "status": "partially_filled",
            "filled_signed_qty": 8.0,
            "wallet_public_id": "",
            "operator_public_id": None,
            "created_at": datetime(2026, 6, 8, 12, 0, tzinfo=UTC),
            "session_id": "00000000-0000-0000-0000-000000000003",
            "sequence_id": 1,
            "timestamp": datetime(2026, 6, 8, 12, 0, tzinfo=UTC),
        }
    )
    await _insert_fill_event(_sql_repo(_coord), client_order_id="cid-1", cum_fill_size=5.0)
    await _coord._recover_paired_execution_leg_fills()
    assert await _leg_fill_qty(_coord, group="grp-a") == 8.0


@pytest.mark.asyncio
async def test_recover_leg_fills_sell_leg_partial_and_zero_cumulative(
    _coord: TraderCoordinator,
) -> None:
    """A sell-leg partial fill records a negative qty; a zero-cumulative event is a no-op.

    Given: an armed group with an owned sell leg (partial fill cumulative 4) and a
        second owned buy leg whose only fill event has cumulative 0,
    When: the recovery leg-fill pass runs,
    Then: the sell leg is filled_signed_qty -4 with status PARTIALLY_FILLED, and the
        zero-cumulative leg stays at 0 (a 0 cumulative is a monotonic no-op).
    """
    _coord._ownership = ShardOwnership(instance_id=0, instance_count=1)
    await _insert_guard_group(_sql_repo(_coord), public_id="grp-a", status="armed")
    await _insert_guard_leg(
        _sql_repo(_coord),
        public_id="leg-sell",
        group_public_id="grp-a",
        leg_index=0,
        shard_key=_ETH_SHARD,
        instrument="ETH-USD",
        side="sell",
        client_order_id="cid-sell",
        status="working",
    )
    await _insert_guard_leg(
        _sql_repo(_coord),
        public_id="leg-zero",
        group_public_id="grp-a",
        leg_index=1,
        shard_key=_BTC_SHARD,
        side="buy",
        client_order_id="cid-zero",
        status="armed",
    )
    await _insert_fill_event(
        _sql_repo(_coord),
        client_order_id="cid-sell",
        cum_fill_size=4.0,
        shard_key=_ETH_SHARD,
        side="sell",
        status="partial",
        instrument="ETH-USD",
    )
    await _insert_fill_event(_sql_repo(_coord), client_order_id="cid-zero", cum_fill_size=0.0)
    await _coord._recover_paired_execution_leg_fills()
    legs = await _sql_repo(_coord).get_current_paired_execution_legs("grp-a")
    by_index = {leg["leg_index"]: leg for leg in legs}
    assert by_index[0]["filled_signed_qty"] == -4.0
    assert by_index[0]["status"] == "partially_filled"
    assert by_index[1]["filled_signed_qty"] == 0.0


@pytest.mark.asyncio
async def test_recover_leg_fills_per_leg_failure_is_isolated(_coord: TraderCoordinator) -> None:
    """A per-leg recovery error is logged and the other owned legs still recover.

    Given: two owned legs where the fill lookup raises for the first leg's order,
    When: the recovery leg-fill pass runs,
    Then: the failing leg is logged and skipped while the second leg is still
        projected, so one bad leg never blocks the rest nor startup.
    """
    _coord._ownership = ShardOwnership(instance_id=0, instance_count=1)
    await _insert_guard_group(_sql_repo(_coord), public_id="grp-a", status="armed")
    await _insert_guard_leg(
        _sql_repo(_coord),
        public_id="leg-bad",
        group_public_id="grp-a",
        leg_index=0,
        shard_key=_BTC_SHARD,
        client_order_id="cid-bad",
        status="armed",
    )
    await _insert_guard_leg(
        _sql_repo(_coord),
        public_id="leg-good",
        group_public_id="grp-a",
        leg_index=1,
        shard_key=_ETH_SHARD,
        instrument="ETH-USD",
        client_order_id="cid-good",
        status="armed",
    )
    await _insert_fill_event(
        _sql_repo(_coord),
        client_order_id="cid-good",
        cum_fill_size=3.0,
        shard_key=_ETH_SHARD,
        instrument="ETH-USD",
    )
    real = _sql_repo(_coord).get_max_cumulative_fill_venue_event

    async def _maybe_raise(client_order_id: str) -> object:
        if client_order_id == "cid-bad":
            raise RuntimeError("db down")
        return await real(client_order_id)

    _sql_repo(_coord).get_max_cumulative_fill_venue_event = AsyncMock(side_effect=_maybe_raise)
    await _coord._recover_paired_execution_leg_fills()
    legs = await _sql_repo(_coord).get_current_paired_execution_legs("grp-a")
    by_index = {leg["leg_index"]: leg for leg in legs}
    assert by_index[0]["filled_signed_qty"] == 0.0
    assert by_index[1]["filled_signed_qty"] == 3.0


@pytest.mark.asyncio
async def test_recover_leg_fills_noop_without_sql_repository(_coord: TraderCoordinator) -> None:
    """The recovery leg-fill pass is skipped without a SQL repository.

    Given: a non-SQL repository,
    When: the recovery leg-fill pass runs,
    Then: nothing is queried and it returns without error.
    """
    repo = AsyncMock()
    _coord.repository = repo
    _coord._ownership = ShardOwnership(instance_id=0, instance_count=1)
    await _coord._recover_paired_execution_leg_fills()
    repo.list_current_paired_execution_groups.assert_not_called()


@pytest.mark.asyncio
async def test_recover_leg_fills_skips_without_ownership(_coord: TraderCoordinator) -> None:
    """The recovery leg-fill pass is skipped without shard ownership.

    Given: a SQL repository with a leg + fill event but no shard ownership,
    When: the recovery leg-fill pass runs,
    Then: nothing is projected.
    """
    _coord._ownership = None
    await _insert_guard_group(_sql_repo(_coord), public_id="grp-a", status="armed")
    await _insert_guard_leg(
        _sql_repo(_coord),
        public_id="leg-a",
        group_public_id="grp-a",
        leg_index=0,
        shard_key=_BTC_SHARD,
        client_order_id="cid-1",
        status="armed",
    )
    await _insert_fill_event(_sql_repo(_coord), client_order_id="cid-1", cum_fill_size=5.0)
    await _coord._recover_paired_execution_leg_fills()
    assert await _leg_fill_qty(_coord, group="grp-a") == 0.0


@pytest.mark.asyncio
async def test_recover_leg_fills_includes_future_timestamped_group(
    _coord: TraderCoordinator,
) -> None:
    """A clock-skewed future-timestamped current group's leg is still recovered.

    Given: a current armed group stamped far in the FUTURE (a sibling's clock
        skew) with an owned leg and a fill event,
    When: the recovery leg-fill pass runs,
    Then: the leg is still projected, because group discovery uses the
        current-active (known_to==MAX) read — a temporal as-of list would exclude
        the future-stamped group and leave its downtime fill unprojected.
    """
    _coord._ownership = ShardOwnership(instance_id=0, instance_count=1)
    await _insert_guard_group(
        _sql_repo(_coord),
        public_id="grp-a",
        status="armed",
        timestamp=datetime(2099, 1, 1, tzinfo=UTC),
    )
    await _insert_guard_leg(
        _sql_repo(_coord),
        public_id="leg-a",
        group_public_id="grp-a",
        leg_index=0,
        shard_key=_BTC_SHARD,
        client_order_id="cid-1",
        status="armed",
    )
    await _insert_fill_event(_sql_repo(_coord), client_order_id="cid-1", cum_fill_size=5.0)
    await _coord._recover_paired_execution_leg_fills()
    assert await _leg_fill_qty(_coord, group="grp-a") == 5.0


@pytest.mark.asyncio
async def test_recover_leg_fills_group_list_failure_is_fail_soft(
    _coord: TraderCoordinator,
) -> None:
    """A failure listing groups is logged and never blocks startup.

    Given: a SQL repository whose current-group list raises,
    When: the recovery leg-fill pass runs,
    Then: the error is swallowed (logged) and the method returns without raising,
        so a transient DB error never blocks coordinator startup.
    """
    _coord._ownership = ShardOwnership(instance_id=0, instance_count=1)
    _sql_repo(_coord).list_current_paired_execution_groups = AsyncMock(
        side_effect=RuntimeError("db down")
    )
    await _coord._recover_paired_execution_leg_fills()
    _sql_repo(_coord).list_current_paired_execution_groups.assert_awaited_once()


@pytest.mark.asyncio
async def test_recover_leg_fills_per_group_leg_read_failure_is_isolated(
    _coord: TraderCoordinator,
) -> None:
    """A per-group leg-read failure skips only that group; other groups still recover.

    Given: two current armed groups where the leg read raises for the first group
        and returns an owned filled leg for the second,
    When: the recovery leg-fill pass runs,
    Then: the failing group is logged and skipped while the second group's leg is
        still projected, so one bad group never blocks the rest.
    """
    _coord._ownership = ShardOwnership(instance_id=0, instance_count=1)
    await _insert_guard_group(_sql_repo(_coord), public_id="grp-bad", status="armed")
    await _insert_guard_group(_sql_repo(_coord), public_id="grp-good", status="armed")
    await _insert_guard_leg(
        _sql_repo(_coord),
        public_id="leg-good",
        group_public_id="grp-good",
        leg_index=0,
        shard_key=_BTC_SHARD,
        client_order_id="cid-good",
        status="armed",
    )
    await _insert_fill_event(_sql_repo(_coord), client_order_id="cid-good", cum_fill_size=6.0)
    real = _sql_repo(_coord).get_current_paired_execution_legs

    async def _maybe_raise(group_public_id: str) -> object:
        if group_public_id == "grp-bad":
            raise RuntimeError("db down")
        return await real(group_public_id)

    _sql_repo(_coord).get_current_paired_execution_legs = AsyncMock(side_effect=_maybe_raise)
    await _coord._recover_paired_execution_leg_fills()
    assert await real("grp-good") is not None
    legs = await real("grp-good")
    assert next(leg["filled_signed_qty"] for leg in legs if leg["leg_index"] == 0) == 6.0
