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
from snapper.data.repository import SQLAlchemyRepository
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.data import SignalData

_GROUP_KEY = "kraken:BTC-USD:live|kraken:ETH-USD:live"


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
            "group_public_id": "grp-prior",
            "reason": "paired-execution group broken",
            "created_at": halt_time,
            "session_id": "00000000-0000-0000-0000-000000000003",
            "sequence_id": 1,
            "timestamp": halt_time,
        }
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
