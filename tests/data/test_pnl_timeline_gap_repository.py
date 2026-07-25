"""Tests for historical P&L fill-gap evidence."""

import json
import math
import os
from collections.abc import AsyncIterator
from collections.abc import Iterable
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import cast

import pytest
from sqlalchemy import create_engine
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError

from snapper.application.portfolio.execution_chain import ExecutionChainError
from snapper.application.portfolio.pnl_anchor_identity import portfolio_pnl_anchor_public_id
from snapper.application.portfolio.pnl_timeline_service import PNL_TIMELINE_CALC_VERSION
from snapper.application.portfolio.pnl_timeline_service import PNL_TIMELINE_MARK_SOURCE
from snapper.application.portfolio.pnl_timeline_service import build_wallet_pnl_series
from snapper.data.models import Execution
from snapper.data.models import ExecutionAnnulment
from snapper.data.models import ExecutionAnnulmentVisibility
from snapper.data.models import Instrument
from snapper.data.models import Order
from snapper.data.models import PortfolioPnlPoint
from snapper.data.models import Symbol
from snapper.data.models import VenueEvent
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import venue_event_fill_identity
from snapper.data.repository_types import PnlTimelineAccrualRow
from snapper.data.repository_types import PnlTimelineExecutionPrefix
from snapper.data.repository_types import PortfolioPnlAnchorRow

_AS_OF = datetime(2026, 7, 20, 12, 0, tzinfo=UTC)
_SESSION = "00000000-0000-7000-8000-000000000901"
_WALLET = "0000face-0000-7000-8000-0000000000a1"
_FOREIGN_WALLET = "0000face-0000-7000-8000-0000000000a2"
_SYMBOL = "00000000-0000-7000-8000-000000000a01"
_SECOND_SYMBOL = "00000000-0000-7000-8000-000000000a02"
_INSTRUMENT = "00000000-0000-7000-8000-000000000b01"
_SECOND_INSTRUMENT = "00000000-0000-7000-8000-000000000b02"
_ORDER = "00000000-0000-7000-8000-000000000c01"
_SECOND_ORDER = "00000000-0000-7000-8000-000000000c02"
_SQL_FILL_IDENTITY_CASES: tuple[tuple[str | None, str | None, str | None], ...] = (
    (" ", " padded-trade ", "trade:padded-trade"),
    ("\t", "\ttrade-tab\t", "trade:trade-tab"),
    ("\n", "\ntrade-lf\n", "trade:trade-lf"),
    ("\r", "\rtrade-cr\r", "trade:trade-cr"),
    ("\f", "\ftrade-ff\f", "trade:trade-ff"),
    ("\v", "\vtrade-vt\v", "trade:trade-vt"),
    (" \t\n\r\f\v ", "\v\f\r\n\t ", None),
    ("\t padded-exec \n", "ignored-trade", "exec:padded-exec"),
    ("embedded\texec", "ignored-trade", "exec:embedded\texec"),
    ("\N{NO-BREAK SPACE}", "ignored-trade", "exec:\N{NO-BREAK SPACE}"),
)


def _configured_postgresql_url() -> str | None:
    """Return the opt-in live PostgreSQL URL or ``None`` for default runs."""
    database_url = os.environ.get("PNL_TEST_POSTGRES_URL") or os.environ.get("DB_URL")
    if database_url is None:
        return None
    try:
        if make_url(database_url).get_backend_name() == "postgresql":
            return database_url
    except ArgumentError:
        return None
    return None


def _symbol(
    public_id: str = _SYMBOL,
    native_symbol: str = "BTC-USD",
) -> Symbol:
    """Build one stable symbol identity for exact shard proof."""
    return Symbol(
        public_id=public_id,
        native_symbol=native_symbol,
        base="BTC",
        quote="USD",
        asset_type="crypto",
        created_at=_AS_OF - timedelta(days=1),
        timestamp=_AS_OF - timedelta(days=1),
        session_id=_SESSION,
        sequence_id=1,
    )


def _instrument(
    public_id: str = _INSTRUMENT,
    symbol_public_id: str = _SYMBOL,
) -> Instrument:
    """Build one active Kraken instrument lineage row."""
    return Instrument(
        public_id=public_id,
        symbol_public_id=symbol_public_id,
        exchange="kraken",
        timestamp=_AS_OF - timedelta(days=1),
        session_id=_SESSION,
        sequence_id=1,
    )


def _order(
    client_order_id: str,
    timestamp: datetime,
    public_id: str = _ORDER,
    instrument_public_id: str = _INSTRUMENT,
) -> Order:
    """Build one sentinel-current order carrying immutable client lineage."""
    return Order(
        public_id=public_id,
        instrument_public_id=instrument_public_id,
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


def _execution(
    scope_sequence: int,
    timestamp: datetime,
    order_public_id: str = _ORDER,
) -> Execution:
    """Build one immutable execution in the wallet's Kraken scope."""
    return Execution(
        public_id=f"00000000-0000-7000-8000-{scope_sequence:012x}",
        order_public_id=order_public_id,
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


def _empty_anchor(point_time: datetime) -> PortfolioPnlAnchorRow:
    """Build one canonical empty anchor before the post-activation test window."""
    public_id = portfolio_pnl_anchor_public_id(_WALLET, "live", "USD")
    return {
        "public_id": public_id,
        "session_id": _SESSION,
        "sequence_id": 1,
        "timestamp": point_time,
        "wallet_public_id": _WALLET,
        "mode": "live",
        "valuation_ccy": "USD",
        "point_time": point_time,
        "point_kind": "anchor",
        "epoch_public_id": public_id,
        "calc_version": PNL_TIMELINE_CALC_VERSION,
        "valuation_status": "complete",
        "realized_pnl": 0.0,
        "fee_pnl": 0.0,
        "accrual_pnl": 0.0,
        "unrealized_pnl": 0.0,
        "external_flow_adjustment": 0.0,
        "cash_usd": None,
        "position_value_usd": None,
        "drawdown": None,
        "mark_source": PNL_TIMELINE_MARK_SOURCE,
        "mark_time": point_time,
        "watermarks_json": "{}",
        "opening_basket_json": json.dumps(
            {"annulments": [], "native_basket": {}, "pools": [], "schema_version": 3},
            separators=(",", ":"),
            sort_keys=True,
        ),
        "contributions_json": json.dumps(
            {"pools": [], "schema_version": 3},
            separators=(",", ":"),
            sort_keys=True,
        ),
    }


@pytest.fixture()
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Create a repository containing the complete gap-proof lineage."""
    db_path = tmp_path / "pnl-timeline-gap.db"
    schema_engine = create_engine(f"sqlite:///{db_path}")
    Symbol.__table__.create(schema_engine)
    Instrument.__table__.create(schema_engine)
    Order.__table__.create(schema_engine)
    Execution.__table__.create(schema_engine)
    ExecutionAnnulment.__table__.create(schema_engine)
    ExecutionAnnulmentVisibility.__table__.create(schema_engine)
    VenueEvent.__table__.create(schema_engine)
    PortfolioPnlPoint.__table__.create(schema_engine)
    schema_engine.dispose()
    result = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    try:
        async with result.session() as session:
            session.add_all([_symbol(), _instrument()])
            await session.commit()
        yield result
    finally:
        await result.engine.dispose()


async def test_gap_evidence_uses_exact_clock_skewed_prefixes(
    repository: SQLAlchemyRepository,
) -> None:
    """Both ledgers keep skewed prefixes without foreign quantity leakage."""
    shard = "kraken.BTC-USD.live"
    future_shard = "kraken.ETH-USD.live.future"
    foreign_only_shard = "kraken.SOL-USD.live.foreign"
    future = _AS_OF + timedelta(minutes=5)
    past = _AS_OF - timedelta(minutes=5)
    first_fill = _venue_event(shard, _WALLET, "skew-client", future, 10)
    second_fill = _venue_event(shard, _WALLET, "skew-client", past, 11)
    first_fill.exec_id = "execution-1"
    first_fill.trade_id = "trade-1"
    second_fill.exec_id = "execution-2"
    second_fill.trade_id = "trade-2"
    async with repository.session() as session:
        session.add_all(
            [
                _order("skew-client", future),
                first_fill,
                second_fill,
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
        await repository.pnl_timeline_scope_has_fill_gap(
            _WALLET,
            "live",
            _AS_OF,
        )
        is False
    )


async def test_shared_client_id_cannot_move_consumption_between_shards(
    repository: SQLAlchemyRepository,
) -> None:
    """An execution on shard B cannot hide shard A's same-CID fill gap."""
    client_order_id = "shared-client"
    first_shard = "kraken.BTC-USD.live"
    second_shard = "kraken.XBT-USD.live"
    first_fill = _venue_event(
        first_shard,
        _WALLET,
        client_order_id,
        _AS_OF - timedelta(minutes=1),
        30,
    )
    second_fill = _venue_event(
        second_shard,
        _WALLET,
        client_order_id,
        _AS_OF - timedelta(minutes=1),
        31,
    )
    first_fill.exec_id = "execution-unconsumed"
    first_fill.trade_id = "trade-unconsumed"
    second_fill.instrument = "XBT-USD"
    second_fill.exec_id = "execution-1"
    second_fill.trade_id = "trade-1"
    async with repository.session() as session:
        session.add_all(
            [
                _symbol(_SECOND_SYMBOL, "XBT-USD"),
                _instrument(_SECOND_INSTRUMENT, _SECOND_SYMBOL),
                _order(client_order_id, _AS_OF, _ORDER, _INSTRUMENT),
                _order(client_order_id, _AS_OF, _SECOND_ORDER, _SECOND_INSTRUMENT),
                _execution(1, _AS_OF, _SECOND_ORDER),
                first_fill,
                second_fill,
            ]
        )
        await session.commit()

    assert (
        await repository.pnl_timeline_scope_has_fill_gap(
            _WALLET,
            "live",
            _AS_OF,
        )
        is True
    )


@pytest.mark.parametrize("execution_size", [0.5, 2.0])
async def test_quantity_mismatch_is_a_bidirectional_fill_gap(
    repository: SQLAlchemyRepository,
    execution_size: float,
) -> None:
    """Under- and over-consumed exact quantities both fail closed."""
    shard = "kraken.BTC-USD.live"
    execution = _execution(1, _AS_OF)
    execution.size = execution_size
    fill = _venue_event(shard, _WALLET, "quantity-client", _AS_OF, 40)
    fill.exec_id = "execution-1"
    fill.trade_id = "trade-1"
    async with repository.session() as session:
        session.add_all(
            [
                _order("quantity-client", _AS_OF),
                execution,
                fill,
            ]
        )
        await session.commit()

    assert (
        await repository.pnl_timeline_scope_has_fill_gap(
            _WALLET,
            "live",
            _AS_OF,
        )
        is True
    )


async def test_conflicting_redelivery_quantity_refuses_prefix_and_scope(
    repository: SQLAlchemyRepository,
) -> None:
    """Conflicting duplicates cannot certify an execution matching their max."""
    shard = "kraken.BTC-USD.live"
    execution = _execution(1, _AS_OF)
    execution.size = 2.0
    first = _venue_event(shard, _WALLET, "redelivery-client", _AS_OF, 45)
    second = _venue_event(shard, _WALLET, "redelivery-client", _AS_OF, 46)
    for fill in (first, second):
        fill.exec_id = "execution-1"
        fill.trade_id = "trade-1"
    second.fill_size = 2.0
    async with repository.session() as session:
        session.add_all(
            [
                _order("redelivery-client", _AS_OF),
                execution,
                first,
                second,
            ]
        )
        await session.commit()

    with pytest.raises(ExecutionChainError, match="conflicting_execution_fill_quantity"):
        await repository.get_pnl_timeline_execution_prefix(_WALLET, "live", _AS_OF)
    assert await repository.pnl_timeline_scope_has_fill_gap(_WALLET, "live", _AS_OF) is True


async def test_exec_and_trade_text_collision_remains_two_fill_witnesses(
    repository: SQLAlchemyRepository,
) -> None:
    """Identifier namespaces keep a lost trade-only fill visible as a gap."""
    shard = "kraken.BTC-USD.live"
    client_order_id = "cross-kind-client"
    execution = _execution(1, _AS_OF)
    execution.exec_id = "shared-venue-text"
    execution.trade_id = "consumed-trade"
    consumed = _venue_event(shard, _WALLET, client_order_id, _AS_OF, 410)
    consumed.exec_id = "shared-venue-text"
    consumed.trade_id = "consumed-trade"
    lost = _venue_event(shard, _WALLET, client_order_id, _AS_OF, 411)
    lost.exec_id = None
    lost.trade_id = "shared-venue-text"
    async with repository.session() as session:
        session.add_all(
            [
                _order(client_order_id, _AS_OF),
                execution,
                consumed,
                lost,
            ]
        )
        await session.commit()

    execution_prefix = await repository.get_pnl_timeline_execution_prefix(
        _WALLET,
        "live",
        _AS_OF,
    )
    recorded = repository._pnl_timeline_recorded_shard_quantities([consumed, lost])

    assert recorded == [(shard, 1.0), (shard, 1.0)]
    assert len(execution_prefix["executions"]) == 1
    assert (
        await repository.pnl_timeline_scope_has_fill_gap(
            _WALLET,
            "live",
            _AS_OF,
            execution_prefix,
        )
        is True
    )


@pytest.mark.parametrize(
    ("exec_id", "trade_id", "expected_identity"),
    _SQL_FILL_IDENTITY_CASES,
)
async def test_sql_fill_identity_has_explicit_ascii_whitespace_semantics_on_sqlite(
    repository: SQLAlchemyRepository,
    exec_id: str | None,
    trade_id: str | None,
    expected_identity: str | None,
) -> None:
    """SQLite executes the same six-character ASCII blank contract as Python."""
    fill = _venue_event(
        "kraken.BTC-USD.live",
        _WALLET,
        "sql-identity-client",
        _AS_OF,
        700,
    )
    fill.public_id = "AAAAAAAA-AAAA-7AAA-8AAA-AAAAAAAAAAAA"
    fill.exec_id = exec_id
    fill.trade_id = trade_id
    async with repository.session() as session:
        session.add(fill)
        await session.flush()
        identity = await session.scalar(
            select(venue_event_fill_identity()).where(VenueEvent.id == fill.id)
        )

    assert identity == (
        "event:aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa"
        if expected_identity is None
        else expected_identity
    )


async def test_sql_and_python_canonicalize_uuid_aliases_without_losing_a_fill(
    repository: SQLAlchemyRepository,
) -> None:
    """Compact and hyphen aliases dedup while a distinct idless fill survives."""
    aliases = [
        "AAAAAAAAAAAA7AAA8AAAAAAAAAAAAAAA",
        "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa",
        "BBBBBBBB-BBBB-7BBB-8BBB-BBBBBBBBBBBB",
    ]
    fills = [
        _venue_event(
            "kraken.BTC-USD.live",
            _WALLET,
            "uuid-alias-client",
            _AS_OF,
            710 + ordinal,
        )
        for ordinal in range(len(aliases))
    ]
    for fill, public_id in zip(fills, aliases, strict=True):
        fill.public_id = public_id
        fill.exec_id = " "
        fill.trade_id = "\t"
    async with repository.session() as session:
        session.add_all(fills)
        await session.flush()
        identity = venue_event_fill_identity()
        rows = (
            await session.execute(
                select(identity, func.max(VenueEvent.fill_size))
                .where(VenueEvent.id.in_([fill.id for fill in fills]))
                .group_by(identity)
                .order_by(identity)
            )
        ).all()

    python_identities = [
        SQLAlchemyRepository._pnl_timeline_fill_identity(fill)[3] for fill in fills
    ]
    assert python_identities == [
        "event:aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa",
        "event:aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa",
        "event:bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb",
    ]
    assert rows == [
        ("event:aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa", 1.0),
        ("event:bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb", 1.0),
    ]


async def test_invalid_stored_fallback_uuid_fails_closed_on_sqlite(
    repository: SQLAlchemyRepository,
) -> None:
    """A malformed idless UUID becomes NULL evidence and forces both gap paths."""
    shard = "kraken.BTC-USD.live"
    fill = _venue_event(shard, _WALLET, "invalid-uuid-client", _AS_OF, 720)
    fill.public_id = "not-a-public-uuid"
    fill.exec_id = ""
    fill.trade_id = " "
    async with repository.session() as session:
        session.add(fill)
        await session.commit()
        identity = await session.scalar(
            select(venue_event_fill_identity()).where(VenueEvent.id == fill.id)
        )

    assert identity is None
    assert await repository.shard_has_fill_gap(shard, _AS_OF) is True
    assert await repository.pnl_timeline_scope_has_fill_gap(_WALLET, "live", _AS_OF) is True


@pytest.mark.skipif(
    _configured_postgresql_url() is None,
    reason="live PostgreSQL semantic proof requires a PostgreSQL DB_URL",
)
async def test_sql_fill_identity_has_explicit_ascii_whitespace_semantics_on_postgresql() -> None:
    """PostgreSQL executes the same identity cases against a real SQL session."""
    database_url = _configured_postgresql_url()
    assert database_url is not None
    repository = SQLAlchemyRepository(database_url)
    rows = [
        {
            "ordinal": ordinal,
            "exec_id": exec_id,
            "trade_id": trade_id,
            "public_id": f"aaaaaaaa-aaaa-7aaa-8aaa-{ordinal:012x}",
        }
        for ordinal, (exec_id, trade_id, _) in enumerate(
            _SQL_FILL_IDENTITY_CASES,
            start=1,
        )
    ]
    expected = [
        f"event:{row['public_id']}" if expected_identity is None else expected_identity
        for row, (_, _, expected_identity) in zip(
            rows,
            _SQL_FILL_IDENTITY_CASES,
            strict=True,
        )
    ]
    try:
        async with repository.engine.connect() as connection, connection.begin():
            await connection.execute(
                text(
                    "CREATE TEMPORARY TABLE venue_events "
                    "(ordinal integer NOT NULL, exec_id text, trade_id text, "
                    "public_id uuid NOT NULL) ON COMMIT DROP"
                )
            )
            await connection.execute(
                text(
                    "INSERT INTO venue_events "
                    "(ordinal, exec_id, trade_id, public_id) "
                    "VALUES (:ordinal, :exec_id, :trade_id, CAST(:public_id AS uuid))"
                ),
                rows,
            )
            result = await connection.execute(
                select(venue_event_fill_identity())
                .select_from(VenueEvent)
                .order_by(text("ordinal"))
            )
            assert list(result.scalars()) == expected
    finally:
        await repository.engine.dispose()


@pytest.mark.skipif(
    _configured_postgresql_url() is None,
    reason="live PostgreSQL UUID proof requires a PostgreSQL DB_URL",
)
async def test_postgresql_uuid_alias_collision_keeps_distinct_lost_fill() -> None:
    """Live PG canonicalizes aliases, retains another fill, and flags invalid UUID."""
    database_url = _configured_postgresql_url()
    assert database_url is not None
    repository = SQLAlchemyRepository(database_url)
    rows = [
        {
            "ordinal": 1,
            "exec_id": " ",
            "trade_id": "\t",
            "public_id": "AAAAAAAAAAAA7AAA8AAAAAAAAAAAAAAA",
            "fill_size": 1.0,
        },
        {
            "ordinal": 2,
            "exec_id": "",
            "trade_id": "\n",
            "public_id": "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa",
            "fill_size": 1.0,
        },
        {
            "ordinal": 3,
            "exec_id": "\r",
            "trade_id": "\v",
            "public_id": "BBBBBBBB-BBBB-7BBB-8BBB-BBBBBBBBBBBB",
            "fill_size": 1.0,
        },
        {
            "ordinal": 4,
            "exec_id": None,
            "trade_id": "\f",
            "public_id": "not-a-public-uuid",
            "fill_size": 1.0,
        },
    ]
    try:
        async with repository.engine.connect() as connection, connection.begin():
            await connection.execute(
                text(
                    "CREATE TEMPORARY TABLE venue_events "
                    "(ordinal integer NOT NULL, exec_id text, trade_id text, "
                    "public_id text NOT NULL, fill_size double precision) ON COMMIT DROP"
                )
            )
            await connection.execute(
                text(
                    "INSERT INTO venue_events "
                    "(ordinal, exec_id, trade_id, public_id, fill_size) "
                    "VALUES (:ordinal, :exec_id, :trade_id, :public_id, :fill_size)"
                ),
                rows,
            )
            identity = venue_event_fill_identity()
            identities = list(
                (
                    await connection.execute(
                        select(identity).select_from(VenueEvent).order_by(text("ordinal"))
                    )
                ).scalars()
            )
            grouped = (
                await connection.execute(
                    select(identity, func.max(VenueEvent.fill_size))
                    .select_from(VenueEvent)
                    .where(identity.is_not(None))
                    .group_by(identity)
                    .order_by(identity)
                )
            ).all()
            invalid_count = await connection.scalar(
                select(func.count()).select_from(VenueEvent).where(identity.is_(None))
            )

            assert identities == [
                "event:aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa",
                "event:aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa",
                "event:bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb",
                None,
            ]
            assert grouped == [
                ("event:aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa", 1.0),
                ("event:bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb", 1.0),
            ]
            assert invalid_count == 1
    finally:
        await repository.engine.dispose()


@pytest.mark.parametrize(
    "blank_exec_id",
    ["", "\t", "\n", "\r", "\f", "\v", " \t\n\r\f\v "],
)
async def test_blank_exec_id_cannot_collapse_a_lost_trade_fill(
    repository: SQLAlchemyRepository,
    blank_exec_id: str,
) -> None:
    """Blank exec ids fall through to trade ids without false completeness."""
    shard = "kraken.BTC-USD.live"
    execution = _execution(1, _AS_OF)
    execution.exec_id = None
    execution.trade_id = "consumed-trade"
    consumed = _venue_event(shard, _WALLET, "blank-exec-client", _AS_OF, 47)
    lost = _venue_event(shard, _WALLET, "blank-exec-client", _AS_OF, 48)
    consumed.exec_id = blank_exec_id
    consumed.trade_id = "consumed-trade"
    lost.exec_id = blank_exec_id
    lost.trade_id = "lost-trade"
    async with repository.session() as session:
        session.add_all(
            [
                _order("blank-exec-client", _AS_OF),
                execution,
                consumed,
                lost,
            ]
        )
        await session.commit()

    execution_prefix = await repository.get_pnl_timeline_execution_prefix(
        _WALLET,
        "live",
        _AS_OF,
    )

    assert len(execution_prefix["executions"]) == 1
    assert (
        await repository.pnl_timeline_scope_has_fill_gap(
            _WALLET,
            "live",
            _AS_OF,
            execution_prefix,
        )
        is True
    )


async def test_whitespace_fill_ids_match_normalized_execution_trade_id(
    repository: SQLAlchemyRepository,
) -> None:
    """Exact matching normalizes blank exec and surrounding trade whitespace."""
    shard = "kraken.BTC-USD.live"
    execution = _execution(1, _AS_OF)
    execution.exec_id = " "
    execution.trade_id = "normalized-trade"
    fill = _venue_event(shard, _WALLET, "normalized-id-client", _AS_OF, 49)
    fill.exec_id = "\t"
    fill.trade_id = " normalized-trade "
    async with repository.session() as session:
        session.add_all(
            [
                _order("normalized-id-client", _AS_OF),
                execution,
                fill,
            ]
        )
        await session.commit()

    execution_prefix = await repository.get_pnl_timeline_execution_prefix(
        _WALLET,
        "live",
        _AS_OF,
    )

    assert (
        await repository.pnl_timeline_scope_has_fill_gap(
            _WALLET,
            "live",
            _AS_OF,
            execution_prefix,
        )
        is False
    )


@pytest.mark.parametrize(
    ("exec_id", "trade_id", "expected_identity"),
    [
        (" normalized-exec ", " normalized-trade ", "exec:normalized-exec"),
        (" ", " normalized-trade ", "trade:normalized-trade"),
        ("\t", "", None),
    ],
)
def test_fill_identity_uses_normalized_fallback_precedence(
    exec_id: str,
    trade_id: str,
    expected_identity: str | None,
) -> None:
    """Identity selection namespaces exec, trade, then canonical event UUID."""
    fill = _venue_event(
        "kraken.BTC-USD.live",
        _WALLET.upper(),
        "identity-client",
        _AS_OF,
        49,
    )
    fill.exec_id = exec_id
    fill.trade_id = trade_id

    identity = SQLAlchemyRepository._pnl_timeline_fill_identity(fill)

    assert identity[:3] == (_WALLET, "kraken", "live")
    assert identity[3] == (
        f"event:{fill.public_id}" if expected_identity is None else expected_identity
    )


@pytest.mark.parametrize(("field_name", "invalid_value"), [("exchange", " "), ("mode", "")])
def test_fill_identity_rejects_blank_scope_components(
    field_name: str,
    invalid_value: str,
) -> None:
    """Blank exchange and mode components cannot enter a witness identity."""
    fill = _venue_event(
        "kraken.BTC-USD.live",
        _WALLET,
        "invalid-scope-client",
        _AS_OF,
        50,
    )
    setattr(fill, field_name, invalid_value)

    with pytest.raises(ExecutionChainError, match=field_name):
        SQLAlchemyRepository._pnl_timeline_fill_identity(fill)


def test_fill_identity_rejects_invalid_wallet_uuid() -> None:
    """A malformed wallet cannot alias an account-scoped witness identity."""
    fill = _venue_event(
        "kraken.BTC-USD.live",
        "not-a-wallet-uuid",
        "invalid-wallet-client",
        _AS_OF,
        51,
    )

    with pytest.raises(ExecutionChainError, match="invalid_execution_fill_identity"):
        SQLAlchemyRepository._pnl_timeline_fill_identity(fill)


def test_idless_fill_identity_rejects_invalid_public_uuid() -> None:
    """The final fallback must be a canonicalizable durable public UUID."""
    fill = _venue_event(
        "kraken.BTC-USD.live",
        _WALLET,
        "invalid-public-client",
        _AS_OF,
        52,
    )
    fill.exec_id = ""
    fill.trade_id = " "
    fill.public_id = "not-a-public-uuid"

    with pytest.raises(ExecutionChainError, match="invalid_execution_fill_identity"):
        SQLAlchemyRepository._pnl_timeline_fill_identity(fill)


@pytest.mark.parametrize("fill_size", [None, 0.0, math.inf])
async def test_invalid_recorded_quantity_is_a_fill_gap(
    repository: SQLAlchemyRepository,
    fill_size: float | None,
) -> None:
    """A null, non-positive, or non-finite sealed fill fails closed."""
    shard = "kraken.BTC-USD.live"
    fill = _venue_event(shard, _WALLET, "invalid-client", _AS_OF, 50)
    fill.fill_size = fill_size
    async with repository.session() as session:
        session.add(fill)
        await session.commit()

    assert (
        await repository.pnl_timeline_scope_has_fill_gap(
            _WALLET,
            "live",
            _AS_OF,
        )
        is True
    )


@pytest.mark.parametrize(
    ("shard_key", "quantity"),
    [("", 1.0), ("kraken.BTC-USD.live", 0.0), ("kraken.BTC-USD.live", math.inf)],
)
def test_quantity_totals_reject_invalid_normalized_input(
    shard_key: str,
    quantity: float,
) -> None:
    """Defensive aggregation rejects malformed pre-normalized evidence."""
    with pytest.raises(ExecutionChainError, match="invalid_execution_fill_quantity"):
        SQLAlchemyRepository._pnl_timeline_quantity_totals_by_shard([(shard_key, quantity)])


async def test_one_fill_identity_cannot_span_recorded_shards(
    repository: SQLAlchemyRepository,
) -> None:
    """Conflicting shard redeliveries make the complete scope unprovable."""
    first = _venue_event(
        "kraken.BTC-USD.live",
        _WALLET,
        "cross-shard-client",
        _AS_OF,
        55,
    )
    second = _venue_event(
        "kraken.XBT-USD.live",
        _WALLET,
        "cross-shard-client",
        _AS_OF,
        56,
    )
    for fill in (first, second):
        fill.exec_id = "cross-shard-execution"
        fill.trade_id = "cross-shard-trade"
    async with repository.session() as session:
        session.add_all([first, second])
        await session.commit()

    assert await repository.pnl_timeline_scope_has_fill_gap(_WALLET, "live", _AS_OF) is True


@pytest.mark.parametrize("shard_key", ["", "kraken BTC-USD live"])
async def test_invalid_recorded_shard_identity_is_a_fill_gap(
    repository: SQLAlchemyRepository,
    shard_key: str,
) -> None:
    """Empty and whitespace-bearing recorded shard identities fail closed."""
    async with repository.session() as session:
        session.add(_venue_event(shard_key, _WALLET, "invalid-shard-client", _AS_OF, 57))
        await session.commit()

    assert await repository.pnl_timeline_scope_has_fill_gap(_WALLET, "live", _AS_OF) is True


async def test_recorded_quantity_overflow_is_a_fill_gap(
    repository: SQLAlchemyRepository,
) -> None:
    """A finite witness multiset whose exact sum overflows fails closed."""
    shard = "kraken.BTC-USD.live"
    first = _venue_event(shard, _WALLET, "overflow-client", _AS_OF, 60)
    second = _venue_event(shard, _WALLET, "overflow-client", _AS_OF, 61)
    first.fill_size = 1e308
    second.fill_size = 1e308
    async with repository.session() as session:
        session.add_all([first, second])
        await session.commit()

    assert (
        await repository.pnl_timeline_scope_has_fill_gap(
            _WALLET,
            "live",
            _AS_OF,
        )
        is True
    )


async def test_nonfinite_recorded_total_is_a_fill_gap(
    repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A defensive non-finite aggregate result fails closed."""
    shard = "kraken.BTC-USD.live"
    async with repository.session() as session:
        session.add(_venue_event(shard, _WALLET, "nonfinite-total-client", _AS_OF, 65))
        await session.commit()

    def nonfinite_sum(values: Iterable[float]) -> float:
        """Return a poisoned aggregate after accepting the typed iterable."""
        del values
        return math.inf

    monkeypatch.setattr(math, "fsum", nonfinite_sum)

    assert await repository.pnl_timeline_scope_has_fill_gap(_WALLET, "live", _AS_OF) is True


@pytest.mark.parametrize(
    "consumed_sizes",
    [[0.0], [math.inf], [1e308, 1e308]],
)
async def test_invalid_or_overflowing_consumed_quantity_is_a_fill_gap(
    repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
    consumed_sizes: list[float],
) -> None:
    """Defensive shard aggregation refuses invalid injected prefix quantities."""
    shard = "kraken.BTC-USD.live"
    async with repository.session() as session:
        session.add(_venue_event(shard, _WALLET, "defensive-client", _AS_OF, 70))
        await session.commit()

    async def injected_prefix(
        wallet_public_id: str,
        mode: str,
        as_of: datetime,
    ) -> PnlTimelineExecutionPrefix:
        """Return the deliberately invalid already-resolved prefix."""
        del wallet_public_id, mode, as_of
        return cast(
            PnlTimelineExecutionPrefix,
            {
                "watermarks": {},
                "executions": [{"shard_key": shard, "size": size} for size in consumed_sizes],
            },
        )

    monkeypatch.setattr(repository, "get_pnl_timeline_execution_prefix", injected_prefix)

    assert (
        await repository.pnl_timeline_scope_has_fill_gap(
            _WALLET,
            "live",
            _AS_OF,
        )
        is True
    )


async def test_scope_gap_analysis_loads_one_prefix_for_many_shards(
    repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A large shard set performs one exact execution-prefix lookup."""
    shard_keys = [f"kraken.BTC-{index}-USD.live" for index in range(100)]
    fill_rows = [
        _venue_event(
            shard_key,
            _WALLET,
            f"bounded-client-{index}",
            _AS_OF,
            100 + index,
        )
        for index, shard_key in enumerate(shard_keys)
    ]
    async with repository.session() as session:
        session.add_all(fill_rows)
        await session.commit()
    prefix_call_count = 0

    async def injected_prefix(
        wallet_public_id: str,
        mode: str,
        as_of: datetime,
    ) -> PnlTimelineExecutionPrefix:
        """Return one resolved execution for every recorded shard."""
        nonlocal prefix_call_count
        prefix_call_count += 1
        assert (wallet_public_id, mode, as_of) == (_WALLET, "live", _AS_OF)
        return cast(
            PnlTimelineExecutionPrefix,
            {
                "watermarks": {},
                "executions": [{"shard_key": shard_key, "size": 1.0} for shard_key in shard_keys],
            },
        )

    monkeypatch.setattr(repository, "get_pnl_timeline_execution_prefix", injected_prefix)

    execution_prefix = await repository.get_pnl_timeline_execution_prefix(
        _WALLET,
        "live",
        _AS_OF,
    )
    assert (
        await repository.pnl_timeline_scope_has_fill_gap(
            _WALLET,
            "live",
            _AS_OF,
            execution_prefix,
        )
        is False
    )
    assert prefix_call_count == 1


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
    await repository.record_portfolio_pnl_anchor(_empty_anchor(_AS_OF - timedelta(minutes=2)))
    assert await repository.get_fill_shard_keys_for_scope(_WALLET, "live", _AS_OF) == [shard]
    assert (
        await repository.pnl_timeline_scope_has_fill_gap(
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
