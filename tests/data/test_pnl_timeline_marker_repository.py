"""Tests for scoped P&L timeline marker repository reads."""

from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import create_engine

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import AiReview
from snapper.data.models import AiReviewEvent
from snapper.data.models import Execution
from snapper.data.models import Instrument
from snapper.data.models import Order
from snapper.data.models import Signal
from snapper.data.models import TradeCommand
from snapper.data.repository import SQLAlchemyRepository

_TO = datetime(2026, 7, 20, 12, 0, tzinfo=UTC)
_FROM = _TO - timedelta(hours=2)
_AS_OF = _TO + timedelta(minutes=1)
_SESSION = "00000000-0000-7000-8000-000000000001"
_WALLET = "00000000-0000-7000-8000-000000000101"
_OTHER_WALLET = "00000000-0000-7000-8000-000000000102"
_LIVE_INSTRUMENT = "00000000-0000-7000-8000-000000000201"
_PAPER_INSTRUMENT = "00000000-0000-7000-8000-000000000202"
_LIVE_SYMBOL = "00000000-0000-7000-8000-000000000301"
_PAPER_SYMBOL = "00000000-0000-7000-8000-000000000302"
_SIGNAL_EXECUTED = "00000000-0000-7000-8000-000000000401"
_SIGNAL_NO_FILL = "00000000-0000-7000-8000-000000000402"
_SIGNAL_TIE_LOW = "00000000-0000-7000-8000-000000000403"
_SIGNAL_TIE_HIGH = "00000000-0000-7000-8000-000000000404"
_SIGNAL_FROM = "00000000-0000-7000-8000-000000000405"
_SIGNAL_TO = "00000000-0000-7000-8000-000000000406"
_SIGNAL_BEFORE = "00000000-0000-7000-8000-000000000407"
_SIGNAL_AFTER = "00000000-0000-7000-8000-000000000408"
_SIGNAL_PAPER = "00000000-0000-7000-8000-000000000409"
_SIGNAL_OTHER = "00000000-0000-7000-8000-000000000410"
_AI_EXECUTED = "00000000-0000-7000-8000-000000000501"
_AI_REJECTED = "00000000-0000-7000-8000-000000000502"
_AI_TIE_LOW = "00000000-0000-7000-8000-000000000503"
_AI_TIE_HIGH = "00000000-0000-7000-8000-000000000504"
_AI_FROM = "00000000-0000-7000-8000-000000000505"
_AI_TO = "00000000-0000-7000-8000-000000000506"
_AI_BEFORE = "00000000-0000-7000-8000-000000000507"
_AI_AFTER = "00000000-0000-7000-8000-000000000508"
_AI_PAPER = "00000000-0000-7000-8000-000000000509"
_AI_OTHER = "00000000-0000-7000-8000-000000000510"
_AI_NON_DECISION = "00000000-0000-7000-8000-000000000511"


def _instrument(
    public_id: str,
    symbol_public_id: str,
    exchange: str,
    sequence_id: int,
) -> Instrument:
    """Build one active instrument used to derive marker mode."""
    return Instrument(
        public_id=public_id,
        symbol_public_id=symbol_public_id,
        exchange=exchange,
        source_exchange="kraken" if exchange == "paper" else None,
        timestamp=_FROM - timedelta(days=1),
        session_id=_SESSION,
        sequence_id=sequence_id,
        known_to=KNOWN_TO_MAX,
    )


def _signal(
    public_id: str,
    instrument_public_id: str,
    wallet_public_id: str,
    fired_at: datetime,
    sequence_id: int,
    strategy_name: str | None = "momentum",
    price: float | None = 100.0,
) -> Signal:
    """Build one active signal marker source."""
    return Signal(
        public_id=public_id,
        instrument_public_id=instrument_public_id,
        wallet_public_id=wallet_public_id,
        operator_public_id=None,
        fired_at=fired_at,
        side="buy",
        strength=0.75,
        reason="threshold crossed",
        strategy_name=strategy_name,
        price=price,
        timestamp=fired_at,
        session_id=_SESSION,
        sequence_id=sequence_id,
        known_to=KNOWN_TO_MAX,
    )


def _review(
    public_id: str,
    instrument_public_id: str,
    wallet_public_id: str,
    sequence_id: int,
    decision: str,
) -> AiReview:
    """Build one terminal AI review joined by an append-only event."""
    status = "resolved_rejected" if decision == "reject" else "resolved_approved"
    return AiReview(
        public_id=public_id,
        session_id=_SESSION,
        sequence_id=sequence_id,
        user_public_id="00000000-0000-7000-8000-000000000601",
        operator_public_id="00000000-0000-7000-8000-000000000602",
        wallet_public_id=wallet_public_id,
        instrument_public_id=instrument_public_id,
        strategy_public_id=f"00000000-0000-7000-8000-{sequence_id + 0x700:012x}",
        selected_delegate_public_id="00000000-0000-7000-8000-000000000603",
        responding_delegate_public_id="00000000-0000-7000-8000-000000000603",
        resolution_mode="pick_one_primary",
        status=status,
        signal_envelope={"side": "buy"},
        signal_snapshot_hash="a" * 64,
        instrument_metadata={"last_price": 100.0},
        deadline=_TO + timedelta(days=1),
        fanout_after=_FROM - timedelta(hours=1),
        decision=decision,
        rationale="risk assessment",
        dispatch_version=0,
        counter_decremented_at=_TO,
        created_at=_FROM - timedelta(hours=2),
        updated_at=_TO,
        resolved_at=_TO,
    )


def _ai_event(
    public_id: str,
    review_public_id: str,
    occurred_at: datetime,
    decision: str,
    event_type: str = "decision_recorded",
) -> AiReviewEvent:
    """Build one append-only AI review event marker source."""
    new_status = "resolved_rejected" if decision == "reject" else "resolved_approved"
    return AiReviewEvent(
        public_id=public_id,
        review_public_id=review_public_id,
        event_type=event_type,
        actor_delegate_public_id="00000000-0000-7000-8000-000000000603",
        previous_status="pending",
        new_status=new_status,
        payload={"decision": decision, "rationale": "risk assessment"},
        occurred_at=occurred_at,
    )


def _trade_command(
    public_id: str,
    client_order_id: str,
    wallet_public_id: str,
    signal_public_id: str | None,
    ai_review_public_id: str | None,
    sequence_id: int,
) -> TradeCommand:
    """Build one active command carrying marker lineage."""
    return TradeCommand(
        public_id=public_id,
        command_type="submit",
        shard_key=f"kraken.BTC-USD.live.{sequence_id}",
        wallet_public_id=wallet_public_id,
        operator_public_id=None,
        user_public_id=None,
        exchange="kraken",
        instrument="BTC-USD",
        mode="live",
        strategy_id="momentum",
        client_order_id=client_order_id,
        venue_client_id=f"venue-{sequence_id}",
        idempotency_key=None,
        side="buy",
        order_type="market",
        quantity=1.0,
        price=100.0,
        stop_price=None,
        leverage=None,
        reduce_only=False,
        status="filled",
        attempt_count=1,
        last_error=None,
        created_at=_FROM,
        dispatched_at=_FROM,
        acked_at=_FROM,
        terminal_at=_FROM,
        exchange_order_id=f"exchange-{sequence_id}",
        supersedes_command_id=None,
        correlation_id=f"00000000-0000-7000-8000-{sequence_id + 0x800:012x}",
        signal_public_id=signal_public_id,
        ai_review_public_id=ai_review_public_id,
        timestamp=_FROM,
        session_id=_SESSION,
        sequence_id=sequence_id,
        known_to=KNOWN_TO_MAX,
    )


def _order(
    public_id: str,
    client_order_id: str,
    wallet_public_id: str,
    sequence_id: int,
) -> Order:
    """Build one active order matched to a marker command."""
    return Order(
        public_id=public_id,
        instrument_public_id=_LIVE_INSTRUMENT,
        wallet_public_id=wallet_public_id,
        mode="live",
        client_order_id=client_order_id,
        exchange_order_id=f"exchange-{sequence_id}",
        created_at=_FROM,
        updated_at=_FROM,
        side="buy",
        order_type="market",
        price=100.0,
        size=1.0,
        status="filled",
        timestamp=_FROM,
        session_id=_SESSION,
        sequence_id=sequence_id,
        known_to=KNOWN_TO_MAX,
    )


def _execution(
    public_id: str,
    order_public_id: str,
    wallet_public_id: str,
    scope_sequence: int,
) -> Execution:
    """Build one append-only execution completing marker lineage."""
    return Execution(
        public_id=public_id,
        order_public_id=order_public_id,
        wallet_public_id=wallet_public_id,
        operator_public_id=None,
        exchange="kraken",
        mode="live",
        scope_sequence=scope_sequence,
        exec_id=f"exec-{scope_sequence}",
        trade_id=f"trade-{scope_sequence}",
        side="buy",
        status="filled",
        price=100.0,
        size=1.0,
        fee=0.1,
        fee_asset="USD",
        executed_at=_FROM,
        liquidity_role="maker",
        timestamp=_FROM,
        session_id=_SESSION,
        sequence_id=scope_sequence,
        known_to=KNOWN_TO_MAX,
    )


async def _seed(repository: SQLAlchemyRepository) -> None:
    """Seed marker sources plus two complete execution lineages."""
    signals = [
        _signal(_SIGNAL_EXECUTED, _LIVE_INSTRUMENT, _WALLET, _TO - timedelta(minutes=10), 1),
        _signal(
            _SIGNAL_NO_FILL,
            _LIVE_INSTRUMENT,
            _WALLET,
            _TO - timedelta(minutes=20),
            2,
            strategy_name=None,
            price=None,
        ),
        _signal(_SIGNAL_TIE_LOW, _LIVE_INSTRUMENT, _WALLET, _TO - timedelta(minutes=30), 3),
        _signal(_SIGNAL_TIE_HIGH, _LIVE_INSTRUMENT, _WALLET, _TO - timedelta(minutes=30), 4),
        _signal(_SIGNAL_FROM, _LIVE_INSTRUMENT, _WALLET, _FROM, 5),
        _signal(_SIGNAL_TO, _LIVE_INSTRUMENT, _WALLET, _TO, 6),
        _signal(_SIGNAL_BEFORE, _LIVE_INSTRUMENT, _WALLET, _FROM - timedelta(seconds=1), 7),
        _signal(_SIGNAL_AFTER, _LIVE_INSTRUMENT, _WALLET, _TO + timedelta(seconds=1), 8),
        _signal(_SIGNAL_PAPER, _PAPER_INSTRUMENT, _WALLET, _TO - timedelta(minutes=5), 9),
        _signal(_SIGNAL_OTHER, _LIVE_INSTRUMENT, _OTHER_WALLET, _TO - timedelta(minutes=5), 10),
    ]
    review_specs = [
        (_AI_EXECUTED, _LIVE_INSTRUMENT, _WALLET, 21, "approve"),
        (_AI_REJECTED, _LIVE_INSTRUMENT, _WALLET, 22, "reject"),
        (_AI_TIE_LOW, _LIVE_INSTRUMENT, _WALLET, 23, "approve"),
        (_AI_TIE_HIGH, _LIVE_INSTRUMENT, _WALLET, 24, "approve"),
        (_AI_FROM, _LIVE_INSTRUMENT, _WALLET, 25, "approve"),
        (_AI_TO, _LIVE_INSTRUMENT, _WALLET, 26, "approve"),
        (_AI_BEFORE, _LIVE_INSTRUMENT, _WALLET, 27, "approve"),
        (_AI_AFTER, _LIVE_INSTRUMENT, _WALLET, 28, "approve"),
        (_AI_PAPER, _PAPER_INSTRUMENT, _WALLET, 29, "approve"),
        (_AI_OTHER, _LIVE_INSTRUMENT, _OTHER_WALLET, 30, "approve"),
        (_AI_NON_DECISION, _LIVE_INSTRUMENT, _WALLET, 31, "approve"),
    ]
    reviews = [_review(*spec) for spec in review_specs]
    events = [
        _ai_event(_AI_EXECUTED, _AI_EXECUTED, _TO - timedelta(minutes=10), "approve"),
        _ai_event(_AI_REJECTED, _AI_REJECTED, _TO - timedelta(minutes=20), "reject"),
        _ai_event(_AI_TIE_LOW, _AI_TIE_LOW, _TO - timedelta(minutes=30), "approve"),
        _ai_event(_AI_TIE_HIGH, _AI_TIE_HIGH, _TO - timedelta(minutes=30), "approve"),
        _ai_event(_AI_FROM, _AI_FROM, _FROM, "approve"),
        _ai_event(_AI_TO, _AI_TO, _TO, "approve"),
        _ai_event(_AI_BEFORE, _AI_BEFORE, _FROM - timedelta(seconds=1), "approve"),
        _ai_event(_AI_AFTER, _AI_AFTER, _TO + timedelta(seconds=1), "approve"),
        _ai_event(_AI_PAPER, _AI_PAPER, _TO - timedelta(minutes=5), "approve"),
        _ai_event(_AI_OTHER, _AI_OTHER, _TO - timedelta(minutes=5), "approve"),
        _ai_event(
            _AI_NON_DECISION,
            _AI_NON_DECISION,
            _TO - timedelta(minutes=2),
            "approve",
            event_type="created",
        ),
    ]
    signal_command_id = "00000000-0000-7000-8000-000000000801"
    signal_order_id = "00000000-0000-7000-8000-000000000802"
    ai_command_id = "00000000-0000-7000-8000-000000000803"
    ai_order_id = "00000000-0000-7000-8000-000000000804"
    lineage = [
        _trade_command(signal_command_id, "signal-cid", _WALLET, _SIGNAL_EXECUTED, None, 41),
        _order(signal_order_id, "signal-cid", _WALLET, 41),
        _execution("00000000-0000-7000-8000-000000000805", signal_order_id, _WALLET, 1),
        _trade_command(ai_command_id, "ai-cid", _WALLET, None, _AI_EXECUTED, 42),
        _order(ai_order_id, "ai-cid", _WALLET, 42),
        _execution("00000000-0000-7000-8000-000000000806", ai_order_id, _WALLET, 2),
    ]
    async with repository.session() as session:
        session.add_all(
            [
                _instrument(_LIVE_INSTRUMENT, _LIVE_SYMBOL, "kraken", 1),
                _instrument(_PAPER_INSTRUMENT, _PAPER_SYMBOL, "paper", 2),
                *signals,
                *reviews,
                *events,
                *lineage,
            ]
        )
        await session.commit()


@pytest.fixture()
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Create an isolated real-SQLite repository with marker fixtures."""
    db_path = tmp_path / "pnl-timeline-markers.db"
    schema_engine = create_engine(f"sqlite:///{db_path}")
    Instrument.__table__.create(schema_engine)
    Signal.__table__.create(schema_engine)
    TradeCommand.__table__.create(schema_engine)
    Order.__table__.create(schema_engine)
    Execution.__table__.create(schema_engine)
    AiReview.__table__.create(schema_engine)
    AiReviewEvent.__table__.create(schema_engine)
    schema_engine.dispose()
    result = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await _seed(result)
    try:
        yield result
    finally:
        await result.engine.dispose()


async def test_signal_markers_preserve_no_fill_and_scope(
    repository: SQLAlchemyRepository,
) -> None:
    """Signals obey inclusive bounds, scope, lineage outcome, and stable limit."""
    rows = await repository.get_pnl_timeline_signals(_WALLET, "live", _FROM, _TO, _AS_OF, 100)
    assert [row["public_id"] for row in rows] == [
        _SIGNAL_TO,
        _SIGNAL_EXECUTED,
        _SIGNAL_NO_FILL,
        _SIGNAL_TIE_HIGH,
        _SIGNAL_TIE_LOW,
        _SIGNAL_FROM,
    ]
    by_id = {row["public_id"]: row for row in rows}
    assert by_id[_SIGNAL_EXECUTED]["has_execution"] is True
    no_fill = by_id[_SIGNAL_NO_FILL]
    assert no_fill["has_execution"] is False
    assert no_fill["instrument_public_id"] == _LIVE_INSTRUMENT
    assert no_fill["fired_at"] == _TO - timedelta(minutes=20)
    assert no_fill["side"] == "buy"
    assert no_fill["strategy_name"] is None
    assert no_fill["strength"] == 0.75
    assert no_fill["reason"] == "threshold crossed"
    assert no_fill["price"] is None
    tied = await repository.get_pnl_timeline_signals(
        _WALLET,
        "live",
        _TO - timedelta(minutes=30),
        _TO - timedelta(minutes=30),
        _AS_OF,
        1,
    )
    assert [row["public_id"] for row in tied] == [_SIGNAL_TIE_HIGH]
    paper = await repository.get_pnl_timeline_signals(_WALLET, "paper", _FROM, _TO, _AS_OF, 100)
    assert [row["public_id"] for row in paper] == [_SIGNAL_PAPER]
    other = await repository.get_pnl_timeline_signals(
        _OTHER_WALLET, "live", _FROM, _TO, _AS_OF, 100
    )
    assert [row["public_id"] for row in other] == [_SIGNAL_OTHER]


async def test_ai_decision_markers_preserve_rejections_and_scope(
    repository: SQLAlchemyRepository,
) -> None:
    """AI decisions obey inclusive bounds, scope, lineage outcome, and stable limit."""
    rows = await repository.get_pnl_timeline_ai_decisions(_WALLET, "live", _FROM, _TO, _AS_OF, 100)
    assert [row["event_public_id"] for row in rows] == [
        _AI_TO,
        _AI_EXECUTED,
        _AI_REJECTED,
        _AI_TIE_HIGH,
        _AI_TIE_LOW,
        _AI_FROM,
    ]
    by_id = {row["event_public_id"]: row for row in rows}
    assert by_id[_AI_EXECUTED]["has_execution"] is True
    rejected = by_id[_AI_REJECTED]
    assert rejected["has_execution"] is False
    assert rejected["review_public_id"] == _AI_REJECTED
    assert rejected["instrument_public_id"] == _LIVE_INSTRUMENT
    assert rejected["strategy_public_id"]
    assert rejected["occurred_at"] == _TO - timedelta(minutes=20)
    assert rejected["new_status"] == "resolved_rejected"
    assert rejected["payload"] == {"decision": "reject", "rationale": "risk assessment"}
    tied = await repository.get_pnl_timeline_ai_decisions(
        _WALLET,
        "live",
        _TO - timedelta(minutes=30),
        _TO - timedelta(minutes=30),
        _AS_OF,
        1,
    )
    assert [row["event_public_id"] for row in tied] == [_AI_TIE_HIGH]
    paper = await repository.get_pnl_timeline_ai_decisions(
        _WALLET, "paper", _FROM, _TO, _AS_OF, 100
    )
    assert [row["event_public_id"] for row in paper] == [_AI_PAPER]
    other = await repository.get_pnl_timeline_ai_decisions(
        _OTHER_WALLET, "live", _FROM, _TO, _AS_OF, 100
    )
    assert [row["event_public_id"] for row in other] == [_AI_OTHER]
