"""Tests for historical P&L fill-gap evidence."""

from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import create_engine

from snapper.application.portfolio.pnl_timeline_service import build_wallet_pnl_series
from snapper.data.models import Execution
from snapper.data.models import Order
from snapper.data.models import VenueEvent
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import PnlTimelineAccrualRow

_AS_OF = datetime(2026, 7, 20, 12, 0, tzinfo=UTC)
_SESSION = "00000000-0000-7000-8000-000000000901"
_WALLET = "0000face-0000-7000-8000-0000000000a1"
_FOREIGN_WALLET = "0000face-0000-7000-8000-0000000000a2"
_INSTRUMENT = "00000000-0000-7000-8000-000000000b01"
_ORDER = "00000000-0000-7000-8000-000000000c01"


def _order(client_order_id: str, timestamp: datetime) -> Order:
    """Build one sentinel-current order carrying immutable client lineage."""
    return Order(
        public_id=_ORDER,
        instrument_public_id=_INSTRUMENT,
        wallet_public_id=_WALLET,
        mode="live",
        client_order_id=client_order_id,
        exchange_order_id="venue-order-1",
        created_at=timestamp,
        updated_at=timestamp,
        side="buy",
        order_type="market",
        price=100.0,
        size=2.0,
        status="filled",
        timestamp=timestamp,
        session_id=_SESSION,
        sequence_id=1,
    )


def _execution(scope_sequence: int, timestamp: datetime) -> Execution:
    """Build one immutable execution in the wallet's Kraken scope."""
    return Execution(
        public_id=f"00000000-0000-7000-8000-{scope_sequence:012x}",
        order_public_id=_ORDER,
        wallet_public_id=_WALLET,
        operator_public_id=None,
        exchange="kraken",
        mode="live",
        scope_sequence=scope_sequence,
        exec_id=f"execution-{scope_sequence}",
        trade_id=f"trade-{scope_sequence}",
        side="buy",
        status="filled",
        price=100.0,
        size=1.0,
        fee=0.0,
        fee_asset="USD",
        executed_at=timestamp,
        liquidity_role="taker",
        timestamp=timestamp,
        session_id=_SESSION,
        sequence_id=scope_sequence,
    )


def _venue_event(
    shard_key: str,
    wallet_public_id: str,
    client_order_id: str,
    timestamp: datetime,
    sequence_id: int,
) -> VenueEvent:
    """Build one append-only venue observation in commit insertion order."""
    return VenueEvent(
        public_id=f"00000000-0000-7000-8001-{sequence_id:012x}",
        event_type="fill_observed",
        shard_key=shard_key,
        wallet_public_id=wallet_public_id,
        command_public_id=None,
        exchange="kraken",
        instrument="BTC-USD",
        mode="live",
        exchange_order_id="venue-order-1",
        client_order_id=client_order_id,
        venue_client_id=client_order_id,
        side="buy",
        status="filled",
        fill_price=100.0,
        fill_size=1.0,
        cum_fill_size=1.0,
        fee=0.0,
        fee_asset="USD",
        exec_id=f"venue-execution-{sequence_id}",
        trade_id=f"venue-trade-{sequence_id}",
        error=None,
        venue_timestamp=timestamp,
        received_at=timestamp,
        payload_json=None,
        liquidity_role="taker",
        paired_group_id=None,
        timestamp=timestamp,
        session_id=_SESSION,
        sequence_id=sequence_id,
    )


@pytest.fixture()
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Create a repository containing only the two gap-evidence ledgers."""
    db_path = tmp_path / "pnl-timeline-gap.db"
    schema_engine = create_engine(f"sqlite:///{db_path}")
    Order.__table__.create(schema_engine)
    Execution.__table__.create(schema_engine)
    VenueEvent.__table__.create(schema_engine)
    schema_engine.dispose()
    result = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    try:
        yield result
    finally:
        await result.engine.dispose()


async def test_gap_evidence_uses_exact_clock_skewed_prefixes(
    repository: SQLAlchemyRepository,
) -> None:
    """Both ledgers keep skewed prefixes without foreign quantity leakage."""
    shard = "kraken.BTC-USD.live.skewed"
    future_shard = "kraken.ETH-USD.live.future"
    foreign_only_shard = "kraken.SOL-USD.live.foreign"
    future = _AS_OF + timedelta(minutes=5)
    past = _AS_OF - timedelta(minutes=5)
    async with repository.session() as session:
        session.add_all(
            [
                _order("skew-client", future),
                _venue_event(shard, _WALLET, "skew-client", future, 10),
                _venue_event(shard, _WALLET, "skew-client", past, 11),
                _venue_event(shard, _FOREIGN_WALLET, "foreign-client", past, 13),
                _venue_event(future_shard, _WALLET, "future-client", future, 14),
                _venue_event(
                    foreign_only_shard,
                    _FOREIGN_WALLET,
                    "foreign-only-client",
                    past,
                    15,
                ),
                _execution(1, future),
                _execution(2, past),
            ]
        )
        await session.commit()
    assert await repository.get_fill_shard_keys_for_scope(_WALLET, "live", _AS_OF) == [shard]
    assert (
        await repository.pnl_timeline_shard_has_fill_gap(
            shard,
            _WALLET,
            "live",
            _AS_OF,
        )
        is False
    )
    assert (
        await repository.pnl_timeline_shard_has_fill_gap(
            foreign_only_shard,
            _WALLET,
            "live",
            _AS_OF,
        )
        is False
    )
    assert (
        await repository.pnl_timeline_shard_has_fill_gap(
            "missing-shard",
            _WALLET,
            "live",
            _AS_OF,
        )
        is False
    )


async def test_fill_above_execution_horizon_withholds_the_series(
    repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A suffix twin seals a skewed fill, producing honest withholding."""
    shard = "kraken.BTC-USD.live.gapped"
    async with repository.session() as session:
        session.add_all(
            [
                _order("gapped-client", _AS_OF + timedelta(minutes=5)),
                _venue_event(
                    shard,
                    _WALLET,
                    "gapped-client",
                    _AS_OF + timedelta(minutes=1),
                    20,
                ),
                _venue_event(
                    shard,
                    _FOREIGN_WALLET,
                    "foreign-client",
                    _AS_OF - timedelta(minutes=1),
                    21,
                ),
                _execution(1, _AS_OF + timedelta(minutes=5)),
            ]
        )
        await session.commit()
    assert await repository.get_fill_shard_keys_for_scope(_WALLET, "live", _AS_OF) == [shard]
    assert (
        await repository.pnl_timeline_shard_has_fill_gap(
            shard,
            _WALLET,
            "live",
            _AS_OF,
        )
        is True
    )

    async def no_accruals(
        wallet_public_id: str,
        mode: str,
        as_of: datetime,
    ) -> list[PnlTimelineAccrualRow]:
        """Return the empty accrual plane for this isolated ledger fixture."""
        del wallet_public_id, mode, as_of
        return []

    monkeypatch.setattr(repository, "get_accruals_for_pnl", no_accruals)
    assert await repository.get_pnl_timeline_executions(_WALLET, "live", _AS_OF) == []
    result = await build_wallet_pnl_series(
        repository,
        _WALLET,
        "live",
        _AS_OF - timedelta(minutes=1),
        _AS_OF,
        "1m",
        _AS_OF,
    )
    for point in result.points:
        assert point.valuation_status == "incomplete"
        assert point.per_instrument == ()
        assert point.realized_pnl is None
        assert point.fee_pnl is None
        assert point.accrual_pnl is None
        assert point.unrealized_pnl is None
        assert point.net_pnl is None
