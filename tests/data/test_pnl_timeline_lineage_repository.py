"""Tests for the P&L timeline execution-lineage repository read."""

from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import create_engine

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Instrument
from snapper.data.models import Order
from snapper.data.models import Signal
from snapper.data.models import TradeCommand
from snapper.data.repository import SQLAlchemyRepository

_NOW = datetime(2026, 7, 20, 12, 0, tzinfo=UTC)
_SIGNAL_SWITCH = _NOW - timedelta(hours=1)
_BASE = _NOW - timedelta(days=1)
_SESSION = "00000000-0000-7000-8000-000000000001"
_USER = "00000000-0000-7000-8000-000000000002"
_OPERATOR_A = "00000000-0000-7000-8000-000000000003"
_OPERATOR_B = "00000000-0000-7000-8000-000000000004"
_WALLET = "00000000-0000-7000-8000-000000000101"
_INSTRUMENT = "00000000-0000-7000-8000-000000000201"
_SYMBOL = "00000000-0000-7000-8000-000000000301"
_SIGNAL = "00000000-0000-7000-8000-000000000401"
_OPERATOR_SIGNAL = "00000000-0000-7000-8000-000000000402"
_MANUAL_ORDER = "00000000-0000-7000-8000-000000000501"
_STRATEGY_ORDER = "00000000-0000-7000-8000-000000000502"
_MISSING_ORDER = "00000000-0000-7000-8000-000000000503"
_AMBIGUOUS_ORDER = "00000000-0000-7000-8000-000000000504"
_LEGACY_PLAN_ORDER = "00000000-0000-7000-8000-000000000505"
_MANUAL_DIRECT_ORDER = "00000000-0000-7000-8000-000000000506"
_LEGACY_PLANLESS_ORDER = "00000000-0000-7000-8000-000000000507"
_COMMAND_OPERATOR_MISMATCH_ORDER = "00000000-0000-7000-8000-000000000508"
_SIGNAL_OPERATOR_MISMATCH_ORDER = "00000000-0000-7000-8000-000000000509"
_MANUAL_PLAN = "00000000-0000-7000-8000-000000000601"
_STRATEGY_PLAN = "00000000-0000-7000-8000-000000000602"
_UNRESOLVED_PLAN = "00000000-0000-7000-8000-000000000603"


def _instrument() -> Instrument:
    """Build the sentinel-current instrument used for command safety."""
    return Instrument(
        public_id=_INSTRUMENT,
        symbol_public_id=_SYMBOL,
        exchange="kraken",
        source_exchange=None,
        timestamp=_BASE,
        session_id=_SESSION,
        sequence_id=1,
        known_to=KNOWN_TO_MAX,
    )


def _order(
    public_id: str,
    client_order_id: str,
    sequence_id: int,
    plan_public_id: str | None = None,
    operator_public_id: str | None = None,
) -> Order:
    """Build one sentinel-current execution order."""
    return Order(
        public_id=public_id,
        instrument_public_id=_INSTRUMENT,
        wallet_public_id=_WALLET,
        operator_public_id=operator_public_id,
        mode="live",
        client_order_id=client_order_id,
        created_at=_BASE,
        timestamp=_BASE,
        side="buy",
        order_type="market",
        price=None,
        size=1.0,
        status="filled",
        plan_public_id=plan_public_id,
        session_id=_SESSION,
        sequence_id=sequence_id,
        known_to=KNOWN_TO_MAX,
    )


def _command(
    public_id: str,
    client_order_id: str,
    sequence_id: int,
    source_surface: str,
    command_type: str = "submit",
    exchange: str = "kraken",
    plan_public_id: str | None = None,
    signal_public_id: str | None = None,
    origin: str = "live",
    user_public_id: str | None = None,
    operator_public_id: str | None = None,
) -> TradeCommand:
    """Build one sentinel-current command candidate."""
    return TradeCommand(
        public_id=public_id,
        command_type=command_type,
        shard_key=f"{exchange}.BTC-USD.live.{sequence_id}",
        wallet_public_id=_WALLET,
        operator_public_id=operator_public_id,
        user_public_id=user_public_id,
        exchange=exchange,
        instrument="BTC-USD",
        mode="live",
        strategy_id="operational-reason-not-a-label",
        client_order_id=client_order_id,
        venue_client_id=f"venue-{sequence_id}",
        idempotency_key=None,
        side="buy",
        order_type="market",
        quantity=1.0,
        price=None,
        stop_price=None,
        leverage=None,
        reduce_only=False,
        status="created",
        attempt_count=0,
        last_error=None,
        created_at=_BASE,
        dispatched_at=None,
        acked_at=None,
        terminal_at=None,
        exchange_order_id=None,
        supersedes_command_id=None,
        correlation_id=f"00000000-0000-7000-8000-{sequence_id + 0x700:012x}",
        plan_public_id=plan_public_id,
        source_surface=source_surface,
        signal_public_id=signal_public_id,
        ai_review_public_id=None,
        submitted_notional_usd=None,
        origin=origin,
        replay_window_start=None,
        replay_window_end=None,
        timestamp=_BASE,
        session_id=_SESSION,
        sequence_id=sequence_id,
        known_to=KNOWN_TO_MAX,
    )


def _signal(
    strategy_name: str,
    timestamp: datetime,
    known_to: datetime,
    public_id: str = _SIGNAL,
    operator_public_id: str | None = None,
) -> Signal:
    """Build one temporal version of the strategy signal."""
    return Signal(
        public_id=public_id,
        instrument_public_id=_INSTRUMENT,
        wallet_public_id=_WALLET,
        operator_public_id=operator_public_id,
        fired_at=_BASE,
        side="buy",
        strength=0.75,
        reason="threshold crossed",
        strategy_name=strategy_name,
        price=100.0,
        timestamp=timestamp,
        session_id=_SESSION,
        sequence_id=1,
        known_to=known_to,
    )


@pytest.fixture()
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Create an isolated repository containing lineage candidates."""
    db_path = tmp_path / "pnl-timeline-lineage.db"
    schema_engine = create_engine(f"sqlite:///{db_path}")
    Instrument.__table__.create(schema_engine)
    Order.__table__.create(schema_engine)
    Signal.__table__.create(schema_engine)
    TradeCommand.__table__.create(schema_engine)
    schema_engine.dispose()
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    async with repo.session() as session:
        session.add_all(
            [
                _instrument(),
                _order(_MANUAL_ORDER, "cid-manual", 1, _MANUAL_PLAN),
                _order(_STRATEGY_ORDER, "cid-strategy", 2, _STRATEGY_PLAN),
                _order(_MISSING_ORDER, "cid-missing", 3, _UNRESOLVED_PLAN),
                _order(_AMBIGUOUS_ORDER, "cid-ambiguous", 4),
                _order(_LEGACY_PLAN_ORDER, "cid-legacy-plan", 5, _STRATEGY_PLAN),
                _order(_MANUAL_DIRECT_ORDER, "cid-manual-direct", 6),
                _order(_LEGACY_PLANLESS_ORDER, "cid-legacy-planless", 7),
                _order(
                    _COMMAND_OPERATOR_MISMATCH_ORDER,
                    "cid-command-operator-mismatch",
                    8,
                    operator_public_id=_OPERATOR_A,
                ),
                _order(
                    _SIGNAL_OPERATOR_MISMATCH_ORDER,
                    "cid-signal-operator-mismatch",
                    9,
                    operator_public_id=_OPERATOR_A,
                ),
                _command(
                    "00000000-0000-7000-8000-000000000701",
                    "cid-manual",
                    1,
                    "rest",
                    command_type="create",
                    plan_public_id=_MANUAL_PLAN,
                    user_public_id=_USER,
                ),
                _command(
                    "00000000-0000-7000-8000-000000000702",
                    "cid-strategy",
                    2,
                    "strategy",
                    plan_public_id=_STRATEGY_PLAN,
                    signal_public_id=_SIGNAL,
                ),
                _command(
                    "00000000-0000-7000-8000-000000000703",
                    "cid-missing",
                    3,
                    "rest",
                    exchange="zonda",
                ),
                _command(
                    "00000000-0000-7000-8000-000000000704",
                    "cid-ambiguous",
                    4,
                    "rest",
                    command_type="create",
                    user_public_id=_USER,
                ),
                _command(
                    "00000000-0000-7000-8000-000000000705",
                    "cid-ambiguous",
                    5,
                    "strategy",
                ),
                _command(
                    "00000000-0000-7000-8000-000000000706",
                    "cid-ambiguous",
                    6,
                    "mcp",
                    command_type="cancel",
                ),
                _command(
                    "00000000-0000-7000-8000-000000000707",
                    "cid-ambiguous",
                    7,
                    "mcp",
                    command_type="replace",
                ),
                _command(
                    "00000000-0000-7000-8000-000000000708",
                    "cid-legacy-plan",
                    8,
                    "rest",
                    plan_public_id=_STRATEGY_PLAN,
                ),
                _command(
                    "00000000-0000-7000-8000-000000000709",
                    "cid-manual-direct",
                    9,
                    "rest",
                    user_public_id=_USER,
                ),
                _command(
                    "00000000-0000-7000-8000-000000000710",
                    "cid-legacy-planless",
                    10,
                    "rest",
                ),
                _command(
                    "00000000-0000-7000-8000-000000000711",
                    "cid-command-operator-mismatch",
                    11,
                    "strategy",
                    signal_public_id=_OPERATOR_SIGNAL,
                    operator_public_id=_OPERATOR_B,
                ),
                _command(
                    "00000000-0000-7000-8000-000000000712",
                    "cid-signal-operator-mismatch",
                    12,
                    "strategy",
                    signal_public_id=_OPERATOR_SIGNAL,
                    operator_public_id=_OPERATOR_A,
                ),
                _signal("momentum-v1", _BASE, _SIGNAL_SWITCH),
                _signal("momentum-v2", _SIGNAL_SWITCH, KNOWN_TO_MAX),
                _signal(
                    "foreign-operator-strategy",
                    _BASE,
                    KNOWN_TO_MAX,
                    public_id=_OPERATOR_SIGNAL,
                    operator_public_id=_OPERATOR_B,
                ),
            ]
        )
        await session.commit()
    try:
        yield repo
    finally:
        await repo.engine.dispose()


async def test_empty_order_scope_is_empty(repository: SQLAlchemyRepository) -> None:
    """An empty execution-order scope avoids a database query."""
    assert await repository.get_pnl_timeline_execution_lineage([], _NOW) == []


async def test_manual_once_retains_manual_source_despite_plan(
    repository: SQLAlchemyRepository,
) -> None:
    """A REST manual-once command exposes its source and non-null plan ID."""
    rows = await repository.get_pnl_timeline_execution_lineage([_MANUAL_ORDER], _NOW)
    assert rows == [
        {
            "order_public_id": _MANUAL_ORDER,
            "source_surface": "rest",
            "plan_public_id": _MANUAL_PLAN,
            "signal_public_id": None,
            "origin": "live",
            "strategy_name": None,
        }
    ]


async def test_direct_manual_rest_source_remains_resolved(
    repository: SQLAlchemyRepository,
) -> None:
    """An authenticated planless REST command remains genuine manual lineage."""
    rows = await repository.get_pnl_timeline_execution_lineage([_MANUAL_DIRECT_ORDER], _NOW)
    assert rows == [
        {
            "order_public_id": _MANUAL_DIRECT_ORDER,
            "source_surface": "rest",
            "plan_public_id": None,
            "signal_public_id": None,
            "origin": "live",
            "strategy_name": None,
        }
    ]


async def test_strategy_command_resolves_signal_strategy_name(
    repository: SQLAlchemyRepository,
) -> None:
    """Strategy identity comes from Signal rather than command strategy_id."""
    rows = await repository.get_pnl_timeline_execution_lineage([_STRATEGY_ORDER], _NOW)
    assert rows == [
        {
            "order_public_id": _STRATEGY_ORDER,
            "source_surface": "strategy",
            "plan_public_id": _STRATEGY_PLAN,
            "signal_public_id": _SIGNAL,
            "origin": "live",
            "strategy_name": "momentum-v2",
        }
    ]


async def test_null_operator_scope_matches_null_command_and_signal(
    repository: SQLAlchemyRepository,
) -> None:
    """Null operators on the order, command, and signal remain one scope."""
    rows = await repository.get_pnl_timeline_execution_lineage([_STRATEGY_ORDER], _NOW)
    assert rows[0]["source_surface"] == "strategy"
    assert rows[0]["strategy_name"] == "momentum-v2"


async def test_legacy_plan_executor_rest_default_is_unresolved(
    repository: SQLAlchemyRepository,
) -> None:
    """A plan-linked REST row without a human actor is conflicting lineage."""
    rows = await repository.get_pnl_timeline_execution_lineage([_LEGACY_PLAN_ORDER], _NOW)
    assert rows == [
        {
            "order_public_id": _LEGACY_PLAN_ORDER,
            "source_surface": None,
            "plan_public_id": _STRATEGY_PLAN,
            "signal_public_id": None,
            "origin": "live",
            "strategy_name": None,
        }
    ]


async def test_legacy_planless_rest_default_is_unresolved(
    repository: SQLAlchemyRepository,
) -> None:
    """A planless automated REST default without a human actor fails closed."""
    rows = await repository.get_pnl_timeline_execution_lineage([_LEGACY_PLANLESS_ORDER], _NOW)
    assert rows == [
        {
            "order_public_id": _LEGACY_PLANLESS_ORDER,
            "source_surface": None,
            "plan_public_id": None,
            "signal_public_id": None,
            "origin": "live",
            "strategy_name": None,
        }
    ]


async def test_different_command_operator_cannot_supply_lineage(
    repository: SQLAlchemyRepository,
) -> None:
    """A matching client ID cannot cross the order's operator boundary."""
    rows = await repository.get_pnl_timeline_execution_lineage(
        [_COMMAND_OPERATOR_MISMATCH_ORDER], _NOW
    )
    assert rows == [
        {
            "order_public_id": _COMMAND_OPERATOR_MISMATCH_ORDER,
            "source_surface": None,
            "plan_public_id": None,
            "signal_public_id": None,
            "origin": None,
            "strategy_name": None,
        }
    ]


async def test_different_signal_operator_cannot_supply_strategy_name(
    repository: SQLAlchemyRepository,
) -> None:
    """A correctly scoped command cannot inherit a foreign operator's label."""
    rows = await repository.get_pnl_timeline_execution_lineage(
        [_SIGNAL_OPERATOR_MISMATCH_ORDER], _NOW
    )
    assert rows == [
        {
            "order_public_id": _SIGNAL_OPERATOR_MISMATCH_ORDER,
            "source_surface": "strategy",
            "plan_public_id": None,
            "signal_public_id": _OPERATOR_SIGNAL,
            "origin": "live",
            "strategy_name": None,
        }
    ]


async def test_missing_or_instrument_mismatched_command_is_explicitly_unresolved(
    repository: SQLAlchemyRepository,
) -> None:
    """A wrong-exchange command and the Order plan cannot supply lineage."""
    rows = await repository.get_pnl_timeline_execution_lineage([_MISSING_ORDER], _NOW)
    assert rows == [
        {
            "order_public_id": _MISSING_ORDER,
            "source_surface": None,
            "plan_public_id": None,
            "signal_public_id": None,
            "origin": None,
            "strategy_name": None,
        }
    ]


async def test_ambiguous_initiating_commands_are_preserved_deterministically(
    repository: SQLAlchemyRepository,
) -> None:
    """Create and submit candidates remain while cancel and replace do not."""
    rows = await repository.get_pnl_timeline_execution_lineage([_AMBIGUOUS_ORDER], _NOW)
    assert [row["source_surface"] for row in rows] == ["rest", "strategy"]
    assert [row["order_public_id"] for row in rows] == [
        _AMBIGUOUS_ORDER,
        _AMBIGUOUS_ORDER,
    ]


async def test_signal_strategy_name_uses_version_active_at_as_of(
    repository: SQLAlchemyRepository,
) -> None:
    """Historical reads project the strategy label known at their horizon."""
    historical = await repository.get_pnl_timeline_execution_lineage(
        [_STRATEGY_ORDER], _SIGNAL_SWITCH - timedelta(minutes=1)
    )
    current = await repository.get_pnl_timeline_execution_lineage([_STRATEGY_ORDER], _NOW)
    assert historical[0]["strategy_name"] == "momentum-v1"
    assert current[0]["strategy_name"] == "momentum-v2"


async def test_multiple_orders_are_sorted_by_public_id(
    repository: SQLAlchemyRepository,
) -> None:
    """Input ordering does not alter the deterministic result ordering."""
    rows = await repository.get_pnl_timeline_execution_lineage(
        [_STRATEGY_ORDER, _MANUAL_ORDER], _NOW
    )
    assert [row["order_public_id"] for row in rows] == [_MANUAL_ORDER, _STRATEGY_ORDER]
