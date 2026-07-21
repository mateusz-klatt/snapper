"""Repository tests for the read-only AI-review aftermath projection."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import TypedDict
from uuid import uuid7

import pytest

from snapper.data.models import AiReview
from snapper.data.models import Execution
from snapper.data.models import Instrument
from snapper.data.models import Order
from snapper.data.models import Position
from snapper.data.models import PositionCycle
from snapper.data.models import Symbol
from snapper.data.repository import SQLAlchemyRepository


class _ScopeIds(TypedDict):
    """Stable identifiers shared by one aftermath fixture."""

    review: str
    wallet: str
    other_wallet: str
    instrument: str
    other_instrument: str
    operator: str
    delegate: str


def _uuid() -> str:
    """Return a fresh UUID7 string for typed ORM fixtures."""
    return str(uuid7())


async def _build_repo(tmp_path: Path, name: str) -> SQLAlchemyRepository:
    """Create a fresh SQLite repository for one projection test.

    Args:
        tmp_path: Pytest-managed temporary directory.
        name: Unique database filename for the case.

    Returns:
        Initialized asynchronous repository with the complete schema.
    """
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / name}")
    await repo.create_all()
    return repo


def _terminal_review(ids: _ScopeIds, created_at: datetime) -> AiReview:
    """Build a terminal timeout review without invoking state transitions.

    Args:
        ids: Fixture identities for the review scope.
        created_at: Inclusive aftermath window start.

    Returns:
        Constraint-valid terminal :class:`AiReview` ORM row.
    """
    resolved_at = created_at + timedelta(seconds=5)
    return AiReview(
        public_id=ids["review"],
        session_id=_uuid(),
        sequence_id=1,
        user_public_id=_uuid(),
        operator_public_id=ids["operator"],
        wallet_public_id=ids["wallet"],
        instrument_public_id=ids["instrument"],
        strategy_public_id=_uuid(),
        selected_delegate_public_id=ids["delegate"],
        responding_delegate_public_id=None,
        resolution_mode="timeout_no_response",
        status="timeout",
        signal_envelope={"side": "buy", "thesis": "breakout"},
        signal_snapshot_hash="a" * 64,
        instrument_metadata={"spread_bps": 8.0},
        deadline=created_at + timedelta(seconds=4),
        fanout_after=created_at + timedelta(seconds=2),
        decision=None,
        rationale=None,
        dispatch_version=1,
        counter_decremented_at=resolved_at,
        created_at=created_at,
        updated_at=resolved_at,
        resolved_at=resolved_at,
    )


def _order(
    *,
    instrument_public_id: str,
    wallet_public_id: str,
    created_at: datetime,
    client_order_id: str,
    sequence_id: int,
) -> Order:
    """Build an active order fixture.

    Args:
        instrument_public_id: Stable instrument identity.
        wallet_public_id: Wallet owning the order.
        created_at: Order placement and temporal timestamp.
        client_order_id: Client identity used by execution lineage.
        sequence_id: Fixture provenance sequence.

    Returns:
        Active :class:`Order` row.
    """
    return Order(
        public_id=_uuid(),
        instrument_public_id=instrument_public_id,
        mode="live",
        wallet_public_id=wallet_public_id,
        operator_public_id=None,
        client_order_id=client_order_id,
        exchange_order_id=f"exchange-{client_order_id}",
        created_at=created_at,
        updated_at=created_at,
        side="buy",
        order_type="market",
        price=None,
        size=1.0,
        status="filled",
        time_in_force=None,
        filled_size=1.0,
        average_price=101.0,
        error=None,
        leverage=None,
        reduce_only=False,
        plan_public_id=None,
        session_id=_uuid(),
        sequence_id=sequence_id,
        timestamp=created_at,
    )


def _execution(
    *,
    order: Order,
    wallet_public_id: str,
    timestamp: datetime,
    scope_sequence: int,
) -> Execution:
    """Build an append-only execution fixture.

    Args:
        order: Parent order supplying the stable public identity.
        wallet_public_id: Execution wallet scope.
        timestamp: Bus-time membership axis for the aftermath window.
        scope_sequence: Unique commit-order counter in the wallet scope.

    Returns:
        Active :class:`Execution` fill row.
    """
    return Execution(
        public_id=_uuid(),
        order_public_id=order.public_id,
        wallet_public_id=wallet_public_id,
        operator_public_id=None,
        exchange="kraken",
        mode="live",
        scope_sequence=scope_sequence,
        exec_id=f"exec-{scope_sequence}",
        trade_id=f"trade-{scope_sequence}",
        side="buy",
        status="filled",
        price=101.0,
        size=1.0,
        fee=0.1,
        fee_asset="USD",
        price_decimal="101.0",
        size_decimal="1.0",
        fee_decimal="0.1",
        counter_amount_decimal="101.0",
        numeric_provenance="venue_raw",
        executed_at=timestamp - timedelta(milliseconds=1),
        liquidity_role="taker",
        session_id=_uuid(),
        sequence_id=scope_sequence,
        timestamp=timestamp,
    )


def _cycle(
    *,
    ids: _ScopeIds,
    status: str,
    opened_at: datetime,
    closed_at: datetime | None,
    sequence_id: int,
    wallet_public_id: str | None = None,
    instrument_public_id: str | None = None,
) -> PositionCycle:
    """Build one active-as-of position-cycle lifecycle row.

    Args:
        ids: Fixture identities supplying default wallet and instrument.
        status: Lifecycle state active at the query anchor.
        opened_at: Cycle-open domain timestamp.
        closed_at: Optional cycle-close domain timestamp.
        sequence_id: Fixture provenance sequence.
        wallet_public_id: Optional wallet override for filtering tests.
        instrument_public_id: Optional instrument override for filtering tests.

    Returns:
        Active :class:`PositionCycle` row retaining lifecycle timestamps.
    """
    cycle_instrument = instrument_public_id or ids["instrument"]
    cycle_wallet = wallet_public_id or ids["wallet"]
    transition_at = closed_at if closed_at is not None else opened_at
    return PositionCycle(
        public_id=_uuid(),
        instrument_public_id=cycle_instrument,
        exchange="kraken",
        mode="live",
        shard_key=f"kraken.{cycle_instrument}.{sequence_id}",
        wallet_public_id=cycle_wallet,
        operator_public_id=ids["operator"],
        direction="long",
        max_qty=float(sequence_id),
        status=status,
        opened_at=opened_at,
        closed_at=closed_at,
        opening_command_public_id=_uuid(),
        closing_command_public_id=_uuid() if closed_at is not None else None,
        session_id=_uuid(),
        sequence_id=sequence_id,
        timestamp=transition_at,
    )


async def _seed_projection_scope(
    repo: SQLAlchemyRepository,
    *,
    created_at: datetime,
) -> tuple[_ScopeIds, PositionCycle, datetime]:
    """Seed exact-scope, out-of-scope, and out-of-window aftermath data.

    Args:
        repo: Repository receiving the ORM fixtures.
        created_at: Review creation and inclusive window start.

    Returns:
        Scope identities, the current open cycle, and the full ``as_of``.
    """
    ids: _ScopeIds = {
        "review": _uuid(),
        "wallet": _uuid(),
        "other_wallet": _uuid(),
        "instrument": _uuid(),
        "other_instrument": _uuid(),
        "operator": _uuid(),
        "delegate": _uuid(),
    }
    symbol_id = _uuid()
    other_symbol_id = _uuid()
    metadata_time = created_at - timedelta(days=1)
    as_of = created_at + timedelta(seconds=10)
    exact_order = _order(
        instrument_public_id=ids["instrument"],
        wallet_public_id=ids["wallet"],
        created_at=created_at,
        client_order_id="exact",
        sequence_id=10,
    )
    older_order = _order(
        instrument_public_id=ids["instrument"],
        wallet_public_id=ids["wallet"],
        created_at=created_at - timedelta(seconds=1),
        client_order_id="older",
        sequence_id=11,
    )
    wrong_wallet_order = _order(
        instrument_public_id=ids["instrument"],
        wallet_public_id=ids["other_wallet"],
        created_at=created_at + timedelta(seconds=1),
        client_order_id="wrong-wallet",
        sequence_id=12,
    )
    wrong_instrument_order = _order(
        instrument_public_id=ids["other_instrument"],
        wallet_public_id=ids["wallet"],
        created_at=created_at + timedelta(seconds=1),
        client_order_id="wrong-instrument",
        sequence_id=13,
    )
    future_order = _order(
        instrument_public_id=ids["instrument"],
        wallet_public_id=ids["wallet"],
        created_at=as_of + timedelta(seconds=1),
        client_order_id="future",
        sequence_id=14,
    )
    opened_and_closed = _cycle(
        ids=ids,
        status="closed",
        opened_at=created_at + timedelta(seconds=2),
        closed_at=created_at + timedelta(seconds=3),
        sequence_id=20,
    )
    closed_in_window = _cycle(
        ids=ids,
        status="closed",
        opened_at=created_at - timedelta(seconds=2),
        closed_at=created_at + timedelta(seconds=4),
        sequence_id=21,
    )
    liquidated = _cycle(
        ids=ids,
        status="liquidated",
        opened_at=created_at + timedelta(seconds=5),
        closed_at=created_at + timedelta(seconds=6),
        sequence_id=22,
    )
    current_cycle = _cycle(
        ids=ids,
        status="open",
        opened_at=created_at + timedelta(seconds=8),
        closed_at=None,
        sequence_id=23,
    )
    wrong_cycle = _cycle(
        ids=ids,
        status="open",
        opened_at=created_at + timedelta(seconds=1),
        closed_at=None,
        sequence_id=24,
        wallet_public_id=ids["other_wallet"],
    )
    async with repo.session() as session:
        session.add_all(
            [
                Symbol(
                    public_id=symbol_id,
                    native_symbol="BTC-USD",
                    base="BTC",
                    quote="USD",
                    asset_type="crypto",
                    created_at=metadata_time,
                    session_id=_uuid(),
                    sequence_id=1,
                    timestamp=metadata_time,
                ),
                Symbol(
                    public_id=other_symbol_id,
                    native_symbol="ETH-USD",
                    base="ETH",
                    quote="USD",
                    asset_type="crypto",
                    created_at=metadata_time,
                    session_id=_uuid(),
                    sequence_id=2,
                    timestamp=metadata_time,
                ),
                Instrument(
                    public_id=ids["instrument"],
                    symbol_public_id=symbol_id,
                    exchange="kraken",
                    source_exchange=None,
                    requires_ai_review=True,
                    session_id=_uuid(),
                    sequence_id=3,
                    timestamp=metadata_time,
                ),
                Instrument(
                    public_id=ids["other_instrument"],
                    symbol_public_id=other_symbol_id,
                    exchange="kraken",
                    source_exchange=None,
                    requires_ai_review=False,
                    session_id=_uuid(),
                    sequence_id=4,
                    timestamp=metadata_time,
                ),
                _terminal_review(ids, created_at),
                exact_order,
                older_order,
                wrong_wallet_order,
                wrong_instrument_order,
                future_order,
                _execution(
                    order=exact_order,
                    wallet_public_id=ids["wallet"],
                    timestamp=as_of,
                    scope_sequence=1,
                ),
                _execution(
                    order=older_order,
                    wallet_public_id=ids["wallet"],
                    timestamp=created_at + timedelta(seconds=7),
                    scope_sequence=2,
                ),
                opened_and_closed,
                closed_in_window,
                liquidated,
                current_cycle,
                wrong_cycle,
                Position(
                    public_id=_uuid(),
                    instrument_public_id=ids["instrument"],
                    mode="live",
                    wallet_public_id=ids["wallet"],
                    quantity=2.5,
                    average_price=101.0,
                    unrealized_pnl=4.5,
                    realized_pnl=3.0,
                    mark_price=103.0,
                    marked_at=created_at + timedelta(seconds=9),
                    source_venue_event_id=42,
                    session_id=_uuid(),
                    sequence_id=30,
                    timestamp=created_at + timedelta(seconds=9),
                ),
            ]
        )
        await session.commit()
    return ids, current_cycle, as_of


@pytest.mark.asyncio
async def test_get_ai_review_aftermath_returns_none_when_review_is_unknown(
    tmp_path: Path,
) -> None:
    """Unknown review returns ``None`` without creating any state.

    Given an initialized repository with no matching AI review,
    When the aftermath projection is requested,
    Then the repository returns ``None``.
    """
    repo = await _build_repo(tmp_path, "aftermath-missing.db")
    result = await repo.get_ai_review_aftermath(_uuid(), datetime(2026, 7, 20, tzinfo=UTC))
    assert result is None


@pytest.mark.asyncio
async def test_get_ai_review_aftermath_returns_empty_activity_and_positions(
    tmp_path: Path,
) -> None:
    """Terminal review with no scoped activity returns empty collections.

    Given a terminal review whose wallet and instrument have no downstream rows,
    When its aftermath is projected,
    Then orders, executions, cycle transitions, and current positions are empty.
    """
    repo = await _build_repo(tmp_path, "aftermath-empty.db")
    created_at = datetime(2026, 7, 20, 10, 0, tzinfo=UTC)
    ids: _ScopeIds = {
        "review": _uuid(),
        "wallet": _uuid(),
        "other_wallet": _uuid(),
        "instrument": _uuid(),
        "other_instrument": _uuid(),
        "operator": _uuid(),
        "delegate": _uuid(),
    }
    async with repo.session() as session:
        session.add(_terminal_review(ids, created_at))
        await session.commit()
    as_of = created_at + timedelta(minutes=1)
    result = await repo.get_ai_review_aftermath(ids["review"], as_of)
    assert result is not None
    assert result["review"]["status"] == "timeout"
    assert result["window_started_at"] == created_at
    assert result["as_of"] == as_of
    assert result["orders"] == []
    assert result["executions"] == []
    assert result["position_cycle_transitions"] == []
    assert result["current_positions"] == []


@pytest.mark.asyncio
async def test_get_ai_review_aftermath_filters_scope_and_replays_as_of(
    tmp_path: Path,
) -> None:
    """One anchor deterministically bounds exact-scope aftermath evidence.

    Given exact-scope activity plus wrong-wallet, wrong-instrument, old, and
        future rows,
    When the projection is read at an early anchor and at the full anchor,
    Then only inclusive exact-scope evidence known at each anchor is returned.
    """
    repo = await _build_repo(tmp_path, "aftermath-full.db")
    created_at = datetime(2026, 7, 20, 10, 0, tzinfo=UTC)
    ids, current_cycle, as_of = await _seed_projection_scope(repo, created_at=created_at)

    early = await repo.get_ai_review_aftermath(ids["review"], created_at + timedelta(seconds=7))
    assert early is not None
    assert [order["client_order_id"] for order in early["orders"]] == ["exact"]
    assert [execution["client_order_id"] for execution in early["executions"]] == ["older"]
    assert early["current_positions"] == []
    assert [row["transition"] for row in early["position_cycle_transitions"]] == [
        "opened",
        "closed",
        "closed",
        "opened",
        "liquidated",
    ]

    result = await repo.get_ai_review_aftermath(ids["review"], as_of)
    assert result is not None
    assert [order["client_order_id"] for order in result["orders"]] == ["exact"]
    assert [execution["client_order_id"] for execution in result["executions"]] == [
        "older",
        "exact",
    ]
    assert [row["transition"] for row in result["position_cycle_transitions"]] == [
        "opened",
        "closed",
        "closed",
        "opened",
        "liquidated",
        "opened",
    ]
    assert len(result["current_positions"]) == 1
    position = result["current_positions"][0]
    assert position["quantity"] == 2.5
    assert position["position_cycle_public_id"] == current_cycle.public_id
    assert position["instrument_public_id"] == ids["instrument"]
