"""Tests for the clock-safe truthful position projection DAL (Phase 2 S1).

Pins the D9 consensus contract: the upsert selects the CURRENT version
by the current-version sentinel filter (never a bus-time-scoped
lookup), clamps the effective timestamp to
``max(bus_time, existing.timestamp)`` so successors never travel back
in time, carries the predecessor's ``public_id``, retries a lost
first-insert unique race exactly once, and ``close_position_projection``
closes the active row WITHOUT a successor while SCD2 history stays
queryable.
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import AccrualLedger
from snapper.data.models import Position
from snapper.data.models import Symbol
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import PositionProjectionUpsertRow
from snapper.data.repository_types import TradeCommandInsertRow
from snapper.data.repository_types import VenueEventInsertRow

_INSTRUMENT = "00000000-0000-7000-8000-00000000000a"
_WALLET = "00000000-0000-7000-8000-000000000001"


_UNSET = object()


def _row(
    bus_time: datetime,
    *,
    instrument_public_id: str = _INSTRUMENT,
    wallet_public_id: str = _WALLET,
    quantity: float = 0.5,
    average_price: float | None = 50000.0,
    unrealized_pnl: float | None = 25.0,
    realized_pnl: float = 10.0,
    mark_price: float | None = 50050.0,
    marked_at: datetime | None | object = _UNSET,
    source_venue_event_id: int | None = 42,
) -> PositionProjectionUpsertRow:
    """Build a full-state projection upsert row with sane defaults.

    Args:
        bus_time: Bus time of the projecting event.
        instrument_public_id: Instrument identity.
        wallet_public_id: Wallet identity.
        quantity: Aggregate quantity.
        average_price: Entry price or honest NULL.
        unrealized_pnl: Mark-based unrealized PnL or honest NULL.
        realized_pnl: Cumulative realized PnL.
        mark_price: Stale-visible mark or honest NULL.
        marked_at: Mark timestamp; defaults to ``bus_time``.
        source_venue_event_id: Durable watermark or honest NULL.

    Returns:
        Complete TYPED upsert row for the test identity — fixture
        drift against the TypedDict fails static checking.
    """
    resolved_marked_at = bus_time if marked_at is _UNSET else marked_at
    assert resolved_marked_at is None or isinstance(resolved_marked_at, datetime)
    return {
        "instrument_public_id": instrument_public_id,
        "mode": "paper",
        "wallet_public_id": wallet_public_id,
        "quantity": quantity,
        "average_price": average_price,
        "unrealized_pnl": unrealized_pnl,
        "realized_pnl": realized_pnl,
        "mark_price": mark_price,
        "marked_at": resolved_marked_at,
        "source_venue_event_id": source_venue_event_id,
        "session_id": "00000000-0000-7000-8000-0000000000aa",
        "sequence_id": 1,
        "bus_time": bus_time,
    }


async def _make_repo(tmp_path: Path) -> SQLAlchemyRepository:
    """Create a throwaway SQLite repository with the full schema.

    Args:
        tmp_path: Pytest-provided temporary directory.

    Returns:
        Repository bound to a fresh database.
    """
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'positions.db'}")
    await repo.create_all()
    return repo


async def _all_rows(repo: SQLAlchemyRepository) -> list[Position]:
    """Return every position row ordered by id.

    Args:
        repo: Repository under test.

    Returns:
        All rows, historical and active.
    """
    async with repo.session() as s:
        result = await s.execute(select(Position).order_by(Position.id))
        return list(result.scalars().all())


async def test_upsert_inserts_new_active_row(tmp_path: Path) -> None:
    """First upsert creates one active row carrying the full snapshot.

    Given: an empty database,
    When: upsert_position_projection runs once,
    Then: exactly one active row exists with every value including the
        mark trio and watermark, timestamp equals bus_time, and a
        public_id was generated.
    """
    repo = await _make_repo(tmp_path)
    now = datetime.now(UTC)
    new_id = await repo.upsert_position_projection(_row(now))
    assert new_id > 0
    rows = await _all_rows(repo)
    assert len(rows) == 1
    row = rows[0]
    assert row.known_to == KNOWN_TO_MAX
    assert row.timestamp == now
    assert row.public_id
    assert row.quantity == 0.5
    assert row.average_price == 50000.0
    assert row.unrealized_pnl == 25.0
    assert row.realized_pnl == 10.0
    assert row.mark_price == 50050.0
    assert row.marked_at == now
    assert row.source_venue_event_id == 42


async def test_upsert_closes_predecessor_and_carries_public_id(tmp_path: Path) -> None:
    """Second upsert closes the old version and reuses its public_id.

    Given: an existing active row,
    When: a later upsert runs for the same identity,
    Then: the predecessor is closed at the new bus_time, the successor
        carries the same public_id with updated values, and exactly one
        active row remains.
    """
    repo = await _make_repo(tmp_path)
    t1 = datetime.now(UTC)
    t2 = t1 + timedelta(seconds=5)
    first_id = await repo.upsert_position_projection(_row(t1))
    second_id = await repo.upsert_position_projection(
        _row(t2, quantity=1.5, realized_pnl=20.0, source_venue_event_id=57)
    )
    assert second_id != first_id
    rows = await _all_rows(repo)
    assert len(rows) == 2
    old, new = rows
    assert old.known_to == t2
    assert new.known_to == KNOWN_TO_MAX
    assert new.public_id == old.public_id
    assert new.timestamp == t2
    assert new.quantity == 1.5
    assert new.source_venue_event_id == 57


async def test_upsert_clamps_lagging_bus_clock(tmp_path: Path) -> None:
    """A bus_time older than the stored timestamp is clamped forward.

    Given: an active row stamped at t1,
    When: an upsert arrives with bus_time five seconds BEFORE t1
        (the Phase-0 clock-skew class),
    Then: the successor is stamped at t1 (never travelling back), the
        predecessor closes at t1, and exactly one row stays active.
    """
    repo = await _make_repo(tmp_path)
    t1 = datetime.now(UTC)
    skewed = t1 - timedelta(seconds=5)
    await repo.upsert_position_projection(_row(t1))
    await repo.upsert_position_projection(_row(skewed, quantity=0.0))
    rows = await _all_rows(repo)
    assert len(rows) == 2
    old, new = rows
    assert old.known_to == t1
    assert new.timestamp == t1
    assert new.known_to == KNOWN_TO_MAX
    active = [r for r in rows if r.known_to == KNOWN_TO_MAX]
    assert len(active) == 1
    assert active[0].quantity == 0.0


async def test_upsert_persists_honest_nulls(tmp_path: Path) -> None:
    """NULL valuation and mark fields survive the round trip.

    Given: an aggregate with opposing directions and no usable mark,
    When: the upsert stores NULL entry, unrealized, mark trio, and
        watermark,
    Then: every one of them reads back as None — zero is never
        substituted.
    """
    repo = await _make_repo(tmp_path)
    now = datetime.now(UTC)
    await repo.upsert_position_projection(
        _row(
            now,
            average_price=None,
            unrealized_pnl=None,
            mark_price=None,
            marked_at=None,
            source_venue_event_id=None,
        )
    )
    rows = await _all_rows(repo)
    assert rows[0].average_price is None
    assert rows[0].unrealized_pnl is None
    assert rows[0].mark_price is None
    assert rows[0].marked_at is None
    assert rows[0].source_venue_event_id is None


async def test_upsert_retries_lost_first_insert_race(tmp_path: Path) -> None:
    """A lost partial-unique first-insert race re-reads the winner.

    Given: a concurrent writer commits the winning active row while the
        first close-and-insert pass believed the identity was empty, so
        flushing a conflicting active row dies on the REAL partial
        unique index (exercising failed-transaction rollback and
        pending-state cleanup),
    When: upsert_position_projection retries,
    Then: the retry finds and closes the committed winner, the
        successor carries the WINNER's public_id, and exactly one
        active row remains.
    """
    repo = await _make_repo(tmp_path)
    rival = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'positions.db'}")
    t1 = datetime.now(UTC)
    t2 = t1 + timedelta(seconds=1)
    original = repo._position_projection_close_and_insert
    attempts: list[int] = []

    async def flaky(s: AsyncSession, row: PositionProjectionUpsertRow) -> int:
        attempts.append(1)
        if len(attempts) == 1:
            await rival.upsert_position_projection(_row(t1, quantity=9.9))
            s.add(
                Position(
                    instrument_public_id=_INSTRUMENT,
                    mode="paper",
                    wallet_public_id=_WALLET,
                    quantity=0.1,
                    realized_pnl=0.0,
                    session_id="00000000-0000-7000-8000-0000000000aa",
                    sequence_id=2,
                    timestamp=t1,
                    known_to=KNOWN_TO_MAX,
                )
            )
            await s.flush()
            raise AssertionError("partial unique index must have rejected the insert")
        return await original(s, row)

    with patch.object(repo, "_position_projection_close_and_insert", side_effect=flaky):
        new_id = await repo.upsert_position_projection(_row(t2, quantity=1.0))
    assert new_id > 0
    assert len(attempts) == 2
    rows = await _all_rows(repo)
    assert len(rows) == 2
    winner, successor = rows
    assert winner.quantity == 9.9
    assert winner.known_to == t2
    assert successor.known_to == KNOWN_TO_MAX
    assert successor.public_id == winner.public_id
    assert successor.quantity == 1.0


async def test_close_position_projection_closes_without_successor(tmp_path: Path) -> None:
    """Closing a proven-flat identity leaves history but no active row.

    Given: an active row stamped at t1,
    When: close_position_projection runs at t2,
    Then: True is returned, the row's known_to becomes t2, no successor
        exists, and the closed version stays queryable.
    """
    repo = await _make_repo(tmp_path)
    t1 = datetime.now(UTC)
    t2 = t1 + timedelta(seconds=3)
    await repo.upsert_position_projection(_row(t1))
    closed = await repo.close_position_projection(_INSTRUMENT, "paper", _WALLET, t2)
    assert closed is True
    rows = await _all_rows(repo)
    assert len(rows) == 1
    assert rows[0].known_to == t2
    assert rows[0].quantity == 0.5


async def test_close_position_projection_returns_false_when_absent(tmp_path: Path) -> None:
    """Closing a non-existent identity reports False.

    Given: an empty database,
    When: close_position_projection runs,
    Then: False is returned and nothing is written.
    """
    repo = await _make_repo(tmp_path)
    closed = await repo.close_position_projection(_INSTRUMENT, "paper", _WALLET, datetime.now(UTC))
    assert closed is False
    assert await _all_rows(repo) == []


async def test_close_position_projection_clamps_lagging_bus_clock(tmp_path: Path) -> None:
    """A close with a lagging bus clock never rewinds known_to.

    Given: an active row stamped at t1,
    When: close_position_projection runs with bus_time BEFORE t1,
    Then: the row closes at t1 (zero-width version, never negative).
    """
    repo = await _make_repo(tmp_path)
    t1 = datetime.now(UTC)
    await repo.upsert_position_projection(_row(t1))
    closed = await repo.close_position_projection(
        _INSTRUMENT, "paper", _WALLET, t1 - timedelta(seconds=7)
    )
    assert closed is True
    rows = await _all_rows(repo)
    assert rows[0].known_to == t1


async def test_projection_rows_surface_wallet_scoped_with_provenance(tmp_path: Path) -> None:
    """Delegate-scoped reads see only accessible wallets, marks intact.

    Given: projection rows written for two different wallets on a real
        repository (the exact write path the trader uses),
    When: get_positions runs scoped to one wallet — the parameter the
        MCP list_positions tool passes from the delegate's wallet
        grants,
    Then: only that wallet's row is returned and it carries the full
        mark provenance trio and watermark; the unscoped read sees
        both.
    """
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'scoped.db'}")
    await repo.create_all()
    now = datetime.now(UTC)
    async with repo.session() as s:
        sym = Symbol(
            native_symbol="BTC-USD",
            base="BTC",
            quote="USD",
            asset_type="crypto",
            created_at=now,
            timestamp=now,
            session_id="s1",
            sequence_id=1,
        )
        s.add(sym)
        await s.commit()
        await s.refresh(sym)
    _, inst_pid = await repo.ensure_instrument(
        symbol_public_id=sym.public_id,
        exchange="kraken",
        session_id="s1",
        sequence_id=2,
        timestamp=now,
    )
    other_wallet = "00000000-0000-7000-8000-000000000002"
    await repo.upsert_position_projection(
        _row(now, instrument_public_id=inst_pid, marked_at=now, source_venue_event_id=42)
    )
    await repo.upsert_position_projection(
        _row(
            now,
            instrument_public_id=inst_pid,
            wallet_public_id=other_wallet,
            quantity=9.0,
        )
    )
    scoped = await repo.get_positions(as_of=now, wallet_public_ids=[_WALLET])
    assert len(scoped) == 1
    row = scoped[0]
    assert row["wallet_public_id"] == _WALLET
    assert row["quantity"] == 0.5
    assert row["mark_price"] == 50050.0
    assert row["marked_at"] == now
    assert row["source_venue_event_id"] == 42
    everything = await repo.get_positions(as_of=now)
    assert {r["wallet_public_id"] for r in everything} == {_WALLET, other_wallet}


async def test_current_position_read_uses_fresh_tick_without_rewriting_history(
    tmp_path: Path,
) -> None:
    """A current read overlays fresh market truth while history stays immutable.

    Given: a live instrument whose stored position mark is old and whose
        durable tick stream has a newer usable price,
    When: current and historical position reads run,
    Then: only the current read returns the tick mark and recomputed
        unrealized PnL; the stored SCD2 row remains unchanged.
    """
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'current-mark.db'}")
    await repo.create_all()
    now = datetime.now(UTC)
    old_marked_at = now - timedelta(hours=15)
    async with repo.session() as s:
        symbol = Symbol(
            native_symbol="EUR-PLN",
            base="EUR",
            quote="PLN",
            asset_type="forex",
            created_at=now,
            timestamp=now,
            session_id="s1",
            sequence_id=1,
        )
        s.add(symbol)
        await s.commit()
        await s.refresh(symbol)
    _, instrument_public_id = await repo.ensure_instrument(
        symbol_public_id=symbol.public_id,
        exchange="walutomat",
        session_id="s1",
        sequence_id=2,
        timestamp=now,
    )
    await repo.upsert_position_projection(
        _row(
            now,
            instrument_public_id=instrument_public_id,
            quantity=21.0,
            average_price=4.382,
            unrealized_pnl=-1.2201,
            mark_price=4.3239,
            marked_at=old_marked_at,
        )
    )
    tick_at = now + timedelta(minutes=1)
    await repo.upsert_ticks(
        [
            {
                "instrument_public_id": instrument_public_id,
                "timestamp": tick_at,
                "bid": 4.34,
                "ask": 4.35,
                "last": 4.345,
                "volume": 1.0,
                "session_id": "s1",
                "sequence_id": 3,
            }
        ]
    )
    current = (await repo.get_positions(as_of=tick_at, current_marks=True))[0]
    historical = (await repo.get_positions(as_of=tick_at))[0]
    assert current["mark_price"] == 4.345
    assert current["marked_at"] == tick_at
    assert current["unrealized_pnl"] == pytest.approx(21.0 * (4.345 - 4.382))
    assert historical["mark_price"] == 4.3239
    assert historical["marked_at"] == old_marked_at
    stored = await _all_rows(repo)
    assert len(stored) == 1
    assert stored[0].mark_price == 4.3239
    invalid_at = tick_at + timedelta(seconds=1)
    await repo.upsert_ticks(
        [
            {
                "instrument_public_id": instrument_public_id,
                "timestamp": invalid_at,
                "bid": 4.34,
                "ask": 4.35,
                "last": 0.0,
                "volume": 1.0,
                "session_id": "s1",
                "sequence_id": 4,
            }
        ]
    )
    refused = (await repo.get_positions(as_of=invalid_at, current_marks=True))[0]
    assert refused["mark_price"] is None
    assert refused["unrealized_pnl"] is None
    entry_unknown_at = invalid_at + timedelta(seconds=1)
    await repo.upsert_position_projection(
        _row(
            entry_unknown_at,
            instrument_public_id=instrument_public_id,
            average_price=None,
            unrealized_pnl=None,
        )
    )
    await repo.upsert_ticks(
        [
            {
                "instrument_public_id": instrument_public_id,
                "timestamp": entry_unknown_at,
                "bid": 4.34,
                "ask": 4.35,
                "last": 4.345,
                "volume": 1.0,
                "session_id": "s1",
                "sequence_id": 5,
            }
        ]
    )
    entry_unknown = (await repo.get_positions(as_of=entry_unknown_at, current_marks=True))[0]
    assert entry_unknown["mark_price"] == 4.345
    assert entry_unknown["unrealized_pnl"] is None


async def test_current_position_read_clears_stale_mark_without_fresh_tick(tmp_path: Path) -> None:
    """A current-money read refuses to carry a stale stored mark forward.

    Given: a position with a confident stored valuation but no tick in
        the ten-minute freshness window,
    When: the current position read runs,
    Then: the mark trio is NULL while entry and quantity remain visible.
    """
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'missing-mark.db'}")
    await repo.create_all()
    now = datetime.now(UTC)
    async with repo.session() as s:
        symbol = Symbol(
            native_symbol="EUR-PLN",
            base="EUR",
            quote="PLN",
            asset_type="forex",
            created_at=now,
            timestamp=now,
            session_id="s1",
            sequence_id=1,
        )
        s.add(symbol)
        await s.commit()
        await s.refresh(symbol)
    _, instrument_public_id = await repo.ensure_instrument(
        symbol_public_id=symbol.public_id,
        exchange="paper",
        session_id="s1",
        sequence_id=2,
        timestamp=now,
        source_exchange="walutomat",
    )
    await repo.upsert_position_projection(_row(now, instrument_public_id=instrument_public_id))
    current = (await repo.get_positions(as_of=now, current_marks=True))[0]
    assert current["mark_price"] is None
    assert current["marked_at"] is None
    assert current["unrealized_pnl"] is None
    assert current["average_price"] == 50000.0
    async with repo.session() as s:
        assert await repo._position_valuation_instrument(s, "missing") is None
        unmapped_symbol = Symbol(
            native_symbol="USD-PLN",
            base="USD",
            quote="PLN",
            asset_type="forex",
            created_at=now,
            timestamp=now,
            session_id="s1",
            sequence_id=3,
        )
        s.add(unmapped_symbol)
        await s.commit()
        await s.refresh(unmapped_symbol)
    _, unmapped_public_id = await repo.ensure_instrument(
        symbol_public_id=unmapped_symbol.public_id,
        exchange="paper",
        session_id="s1",
        sequence_id=4,
        timestamp=now,
    )
    async with repo.session() as s:
        assert await repo._position_valuation_instrument(s, unmapped_public_id) is None


async def test_current_position_read_refuses_a_quote_only_tick(tmp_path: Path) -> None:
    """A tick carrying no traded price cannot value a position.

    Given: a fresh tick inside the freshness window whose bid and ask are
        present but whose last is NULL, as a quote-only update leaves it,
    When: the current position read runs,
    Then: the mark trio is NULL rather than derived from a price the venue
        never traded at, because a confident wrong mark misleads where an
        absent one merely reports incompleteness.
    """
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'quote-only.db'}")
    await repo.create_all()
    now = datetime.now(UTC)
    async with repo.session() as s:
        symbol = Symbol(
            native_symbol="EUR-PLN",
            base="EUR",
            quote="PLN",
            asset_type="forex",
            created_at=now,
            timestamp=now,
            session_id="s1",
            sequence_id=1,
        )
        s.add(symbol)
        await s.commit()
        await s.refresh(symbol)
    _, instrument_public_id = await repo.ensure_instrument(
        symbol_public_id=symbol.public_id,
        exchange="walutomat",
        session_id="s1",
        sequence_id=2,
        timestamp=now,
    )
    await repo.upsert_position_projection(_row(now, instrument_public_id=instrument_public_id))
    await repo.upsert_ticks(
        [
            {
                "instrument_public_id": instrument_public_id,
                "timestamp": now,
                "bid": 4.34,
                "ask": 4.35,
                "last": None,
                "volume": 1.0,
                "session_id": "s1",
                "sequence_id": 3,
            }
        ]
    )

    current = (await repo.get_positions(as_of=now, current_marks=True))[0]

    assert current["mark_price"] is None
    assert current["marked_at"] is None
    assert current["unrealized_pnl"] is None
    assert current["quantity"] == 0.5


async def test_active_identity_scan_is_clock_free(tmp_path: Path) -> None:
    """The recovery diagnostic sees every active row, even future-stamped.

    Given: one active row stamped in the FUTURE relative to the caller
        (a clamped clock-skew write) and one closed identity,
    When: get_active_position_identities runs,
    Then: the future-stamped active row is returned (a temporal as_of
        query would hide it) and the closed identity is not.
    """
    repo = await _make_repo(tmp_path)
    future = datetime.now(UTC) + timedelta(minutes=10)
    await repo.upsert_position_projection(_row(future))
    other = "00000000-0000-7000-8000-00000000000f"
    now = datetime.now(UTC)
    await repo.upsert_position_projection(_row(now, instrument_public_id=other))
    await repo.close_position_projection(other, "paper", _WALLET, now)
    active = await repo.get_active_position_identities()
    assert [(row[1], row[2], row[3]) for row in active] == [(_INSTRUMENT, "paper", _WALLET)]
    assert active[0][0]


async def test_watermark_matches_trade_id_only_and_exec_id_only_fills(tmp_path: Path) -> None:
    """Both venue identifier columns resolve the durable watermark.

    Given: one fill event carrying ONLY a venue exec id and another
        carrying ONLY a trade id (venues populate either),
    When: the consumed-fill watermark resolves with both identifiers
        passed the way execution-replay recovery now does,
    Then: each fill matches its own venue-event row — a multi-partial
        order recovered from executions can never leave the watermark
        stuck on the first partial.
    """
    repo = await _make_repo(tmp_path)
    base: VenueEventInsertRow = {
        "event_type": "fill_observed",
        "shard_key": "kraken.BTC-USD.live",
        "wallet_public_id": _WALLET,
        "exchange": "kraken",
        "instrument": "BTC-USD",
        "mode": "live",
        "client_order_id": "cid-1",
        "side": "buy",
        "status": "filled",
        "fill_price": 50000.0,
        "fill_size": 0.5,
        "session_id": "s1",
        "sequence_id": 1,
        "timestamp": datetime.now(UTC),
        "received_at": datetime.now(UTC),
    }
    exec_only = await repo.insert_venue_event(
        {**base, "exec_id": "E-1", "trade_id": None, "cum_fill_size": 0.5}
    )
    trade_only = await repo.insert_venue_event(
        {**base, "exec_id": None, "trade_id": "T-2", "cum_fill_size": 1.0}
    )
    matched_exec = await repo.get_consumed_fill_venue_event_id(
        shard_key="kraken.BTC-USD.live",
        client_order_id="cid-1",
        exec_id="E-1",
        cum_fill_size=0.5,
        trade_id=None,
    )
    matched_trade = await repo.get_consumed_fill_venue_event_id(
        shard_key="kraken.BTC-USD.live",
        client_order_id="cid-1",
        exec_id=None,
        cum_fill_size=1.0,
        trade_id="T-2",
    )
    assert matched_exec == exec_only
    assert matched_trade == trade_only
    assert matched_trade > matched_exec


async def test_shard_has_any_accruals_is_clock_free(tmp_path: Path) -> None:
    """The certification accrual probe sees future-stamped rows.

    Given: an accrual ledger row whose coordinator-clock timestamp sits
        ten minutes in the FUTURE (a skewed writer's crash-window row),
    When: shard_has_any_accruals probes the scope,
    Then: the row is visible (a temporal window would hide it) and a
        different scope stays clean.
    """
    repo = await _make_repo(tmp_path)
    future = datetime.now(UTC) + timedelta(minutes=10)
    async with repo.session() as s:
        s.add(
            AccrualLedger(
                instrument_public_id=_INSTRUMENT,
                wallet_public_id=_WALLET,
                operator_public_id=None,
                mode="live",
                accrual_type="funding",
                accrued_at=future,
                amount=-1.25,
                amount_asset="USD",
                rate=0.0001,
                notional=50000.0,
                position_quantity_at_accrual=0.5,
                exchange="kraken_futures",
                timestamp=future,
                session_id="s1",
                sequence_id=1,
            )
        )
        await s.commit()
    assert await repo.shard_has_any_accruals(_WALLET, "kraken_futures", "live") is True
    assert await repo.shard_has_any_accruals(_WALLET, "kraken_futures", "paper") is False
    assert await repo.shard_has_any_accruals(_WALLET, "kraken", "live") is False


async def test_fill_shard_lineage_maps_and_drops_ambiguity(tmp_path: Path) -> None:
    """Durable fill lineage maps cids and drops disagreeing ones.

    Given: one client order whose fills agree on a tagged paper shard
        and another whose fills disagree between two shards,
    When: get_fill_shard_keys_by_client_order_ids resolves them,
    Then: the agreeing cid maps to its exact shard key and the
        ambiguous cid is absent; empty input short-circuits.
    """
    repo = await _make_repo(tmp_path)
    tagged = f"paper.BTC-USD.paper.w{_WALLET[-12:]}.strat_a"
    base: VenueEventInsertRow = {
        "event_type": "fill_observed",
        "wallet_public_id": _WALLET,
        "exchange": "paper",
        "instrument": "BTC-USD",
        "mode": "paper",
        "side": "buy",
        "status": "filled",
        "fill_price": 50000.0,
        "fill_size": 0.5,
        "cum_fill_size": 0.5,
        "session_id": "s1",
        "sequence_id": 1,
        "timestamp": datetime.now(UTC),
        "received_at": datetime.now(UTC),
    }
    await repo.insert_venue_event(
        {**base, "shard_key": tagged, "client_order_id": "cid-ok", "exec_id": "E-1"}
    )
    await repo.insert_venue_event(
        {**base, "shard_key": tagged, "client_order_id": "cid-ok", "exec_id": "E-2"}
    )
    await repo.insert_venue_event(
        {**base, "shard_key": tagged, "client_order_id": "cid-dup", "exec_id": "E-3"}
    )
    await repo.insert_venue_event(
        {
            **base,
            "shard_key": "paper.BTC-USD.paper",
            "client_order_id": "cid-dup",
            "exec_id": "E-4",
        }
    )
    await repo.insert_venue_event(
        {**base, "shard_key": "", "client_order_id": "cid-empty", "exec_id": "E-5"}
    )
    resolved, ambiguous = await repo.get_fill_shard_keys_by_client_order_ids(
        ["cid-ok", "cid-dup", "cid-x", "cid-empty"]
    )
    assert resolved == {"cid-ok": (tagged, _WALLET)}
    assert ambiguous == {"cid-dup", "cid-empty"}
    assert await repo.get_fill_shard_keys_by_client_order_ids([]) == ({}, set())


def _command_row(
    client_order_id: str,
    shard_key: str,
    wallet_public_id: str = _WALLET,
    command_type: str = "create",
) -> TradeCommandInsertRow:
    """Build a minimal trade-command insert row for lineage tests.

    Args:
        client_order_id: Venue client order id under test.
        shard_key: Exact dispatched shard key (empty for the
            missing-shard ambiguity case).
        wallet_public_id: Full owning wallet.
        command_type: Command type (a later cancel legally shares the
            create's shard and wallet).

    Returns:
        Typed insert row accepted by ``insert_trade_command``.
    """
    now = datetime.now(UTC)
    return {
        "command_type": command_type,
        "shard_key": shard_key,
        "exchange": "paper",
        "instrument": "BTC-USD",
        "mode": "paper",
        "strategy_id": "strat_a",
        "client_order_id": client_order_id,
        "venue_client_id": client_order_id,
        "side": "buy",
        "order_type": "market",
        "quantity": 0.5,
        "price": None,
        "status": "created",
        "created_at": now,
        "session_id": "s1",
        "sequence_id": 1,
        "timestamp": now,
        "wallet_public_id": wallet_public_id,
    }


async def test_command_shard_lineage_maps_and_drops_ambiguity(tmp_path: Path) -> None:
    """Durable command lineage maps cids and drops disagreeing ones.

    Given: one client order whose create and cancel commands agree on a
        tagged paper shard, one whose commands disagree between two
        shards, one whose commands disagree on the full wallet, and one
        whose command carries an empty shard,
    When: get_command_shard_keys_by_client_order_ids resolves them,
    Then: the agreeing cid maps to its exact (shard, wallet) pair and
        every disagreeing or shard-less cid is ambiguous; empty input
        short-circuits.
    """
    repo = await _make_repo(tmp_path)
    tagged = f"paper.BTC-USD.paper.w{_WALLET[-12:]}.strat_a"
    other_wallet = "00000000-0000-7000-8000-000000000002"
    await repo.insert_trade_command(_command_row("cid-ok", tagged))
    await repo.insert_trade_command(_command_row("cid-ok", tagged, command_type="cancel"))
    await repo.insert_trade_command(_command_row("cid-dup", tagged))
    await repo.insert_trade_command(_command_row("cid-dup", "paper.BTC-USD.paper"))
    await repo.insert_trade_command(_command_row("cid-wallet", tagged))
    await repo.insert_trade_command(
        _command_row("cid-wallet", tagged, wallet_public_id=other_wallet)
    )
    await repo.insert_trade_command(_command_row("cid-empty", ""))
    resolved, ambiguous = await repo.get_command_shard_keys_by_client_order_ids(
        ["cid-ok", "cid-dup", "cid-wallet", "cid-x", "cid-empty"]
    )
    assert resolved == {"cid-ok": (tagged, _WALLET)}
    assert ambiguous == {"cid-dup", "cid-wallet", "cid-empty"}
    assert await repo.get_command_shard_keys_by_client_order_ids([]) == ({}, set())
