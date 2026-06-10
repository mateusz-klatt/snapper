"""SCD2 batch atomicity regression tests.

Pins the all-or-nothing transaction contract for true SCD2 batch writes
(``upsert_candles``, ``upsert_market_snapshots``) and the contention
outcome under concurrent writers on the same natural key.

The atomicity contract is documented in ``docs/architecture.md``
subsection "Batch atomicity guarantees" under ``## Bitemporal Model``.
The three tests below pin the same contract at the integration layer:

1. ``test_upsert_candles_mid_batch_failure_rolls_back_all`` — a
   ``RuntimeError`` injected on the UPDATE-close step for the SECOND
   row of a 2-row batch, where both rows target existing active
   candles, must roll back the entire batch: row 1's UPDATE-close
   (already emitted at wrapper call 2) AND row 1's INSERT (already
   flushed by the autoflush that fires at the start of wrapper call 3
   for row 2's SELECT-FOR-UPDATE) AND the aborted UPDATE for row 2.
   Both seeded candles must remain active with their original
   ``public_id`` values; no new candle rows land.
2. ``test_upsert_market_snapshots_mid_batch_failure_rolls_back_all`` —
   same contract for market snapshots (the other true-SCD2 batch path),
   with two seeded snapshots and a 2-instrument batch.
3. ``test_concurrent_upsert_candles_serializes`` — two concurrent tasks
   writing the same ``(instrument, timeframe, open_at)`` must preserve
   the SCD2 chain invariant in the final DB state regardless of
   dialect. Accepted outcomes: both tasks succeed (Postgres MVCC) OR
   one task succeeds and the other raises ``IntegrityError`` on the
   ``candles.public_id`` UNIQUE constraint under SQLite's default transaction mode.
   Invariant asserted: exactly one active row, every row shares the
   single carried-forward ``public_id``, ``#closed_rows == #successful
   upserts``, no orphan inserts.

The mid-batch fault injection uses ``monkeypatch.setattr`` on
``AsyncSession.execute``. In the ``upsert_candles`` flow for a 2-row
batch where both rows target unique existing natural keys, the
per-batch execute sequence is (staging by ``s.add(...)`` and
autoflushed INSERTs fire at the Connection layer, below
``AsyncSession.execute``, so only Session-level ``execute`` calls are
counted):

- call 1 — SELECT ... FOR UPDATE for both natural keys
- call 2 — UPDATE Candle SET known_to=bus_time WHERE id=A.id
- (s.add(new_row_1) stages the new candle in the identity map)
- call 3 — UPDATE Candle SET known_to=bus_time WHERE id=B.id

Raising on call 3 aborts the transaction AFTER new_row_1's INSERT has
been flushed to the connection (uncommitted) and before row 2's close
UPDATE lands. The transaction rollback must undo both the UPDATE-close
and the flushed-but-uncommitted INSERT from row 1, plus leave row 2
completely untouched. This exercises the full close+insert rollback
path rather than a bare UPDATE-only rollback.
"""

import asyncio
import tempfile
from collections.abc import Awaitable
from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import and_
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Candle
from snapper.data.models import MarketSnapshot
from snapper.data.models import Symbol
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import CandleUpsertRow
from snapper.data.repository_types import MarketSnapshotUpsertRow

SEED_TIME = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)


async def _create_repo_with_instrument(
    db_path: Path,
) -> tuple[SQLAlchemyRepository, int, str]:
    """Create a repository with a single instrument ready for SCD2 tests.

    Inline duplicate of the helper in ``tests/data/test_bitemporal.py``
    — the helper has no shared home in ``tests/helpers/db.py`` today and
    the copy matches the existing convention across the data test suite.
    """
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await repo.create_all()
    symbol = Symbol(
        native_symbol="BTC-USD",
        base="BTC",
        quote="USD",
        asset_type="crypto",
        created_at=SEED_TIME,
        timestamp=SEED_TIME,
        session_id="test-session",
        sequence_id=1,
    )
    async with repo.session() as s:
        s.add(symbol)
        await s.commit()
    spid = symbol.public_id
    assert spid is not None
    inst_id, inst_public_id = await repo.ensure_instrument(
        symbol_public_id=spid,
        exchange="kraken",
        timestamp=SEED_TIME,
        session_id="test-session",
        sequence_id=1,
    )
    return repo, inst_id, inst_public_id


def _candle_row(
    instrument_public_id: str,
    open_at: datetime,
    timestamp: datetime,
    close: float = 1.5,
) -> CandleUpsertRow:
    """Build a minimal candle upsert row for atomicity assertions."""
    return {
        "instrument_public_id": instrument_public_id,
        "timeframe": "1m",
        "open_at": open_at,
        "timestamp": timestamp,
        "open": 1.0,
        "high": 2.0,
        "low": 0.5,
        "close": close,
        "volume": 10.0,
        "vwap": None,
        "trades": 1,
        "session_id": "test-session",
        "sequence_id": 1,
    }


def _snapshot_row(
    instrument_public_id: str,
    timestamp: datetime,
    *,
    bid: float,
    ask: float,
    last_price: float,
) -> MarketSnapshotUpsertRow:
    """Build a minimal market-snapshot upsert row for atomicity assertions."""
    return {
        "instrument_public_id": instrument_public_id,
        "timestamp": timestamp,
        "session_id": "test-session",
        "sequence_id": 1,
        "bid": bid,
        "bid_volume": 1.0,
        "ask": ask,
        "ask_volume": 1.0,
        "last_price": last_price,
        "volume_24h": 100.0,
        "vwap_24h": last_price,
        "low_24h": last_price - 1.0,
        "high_24h": last_price + 1.0,
        "change_24h": 0.0,
        "spread": ask - bid,
        "spread_pct": (ask - bid) / last_price,
    }


def _fail_on_nth_execute(
    target: int,
    message: str = "injected mid-batch fault",
) -> tuple[Callable[..., Awaitable[Any]], Callable[[], int]]:
    """Return an async execute wrapper that raises on the Nth call.

    The wrapper captures the original ``AsyncSession.execute`` via a
    module-level reference so the patched version can delegate pre- and
    post-injection calls through.
    """
    original = AsyncSession.execute
    counter = {"n": 0}

    async def wrapped(self: AsyncSession, *args: Any, **kwargs: Any) -> Any:
        counter["n"] += 1
        if counter["n"] == target:
            raise RuntimeError(message)
        return await original(self, *args, **kwargs)

    return wrapped, lambda: counter["n"]


@pytest.mark.asyncio
async def test_upsert_candles_mid_batch_failure_rolls_back_all(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A RuntimeError on row 2's UPDATE-close rolls back row 1's close+insert too.

    Both seeded candles (``open_at=t0`` and ``open_at=t1``) must keep
    their original ``public_id`` with ``known_to == KNOWN_TO_MAX``;
    no closed versions exist; no new candle rows land — meaning the
    autoflushed INSERT for row 1's new version was rolled back along
    with row 1's UPDATE-close and the aborted row 2 UPDATE-close.
    """
    db_path = tmp_path / "scd2_candles.db"
    repo, _inst_id, inst_public_id = await _create_repo_with_instrument(db_path)
    t0 = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
    t1 = datetime(2024, 6, 1, 12, 1, 0, tzinfo=UTC)
    seed_ts = datetime(2024, 6, 1, 12, 0, 10, tzinfo=UTC)
    batch_ts = datetime(2024, 6, 1, 12, 0, 20, tzinfo=UTC)

    seed_batch: list[CandleUpsertRow] = [
        _candle_row(inst_public_id, t0, seed_ts, close=100.0),
        _candle_row(inst_public_id, t1, seed_ts, close=200.0),
    ]
    inserted = await repo.upsert_candles(seed_batch)
    assert inserted == 2

    async with repo.session() as s:
        seeded_rows = (
            (
                await s.execute(
                    select(Candle)
                    .where(Candle.instrument_public_id == inst_public_id)
                    .order_by(Candle.open_at)
                )
            )
            .scalars()
            .all()
        )
    assert len(seeded_rows) == 2
    original_public_ids = {row.open_at: row.public_id for row in seeded_rows}

    wrapped, get_n = _fail_on_nth_execute(target=3)
    monkeypatch.setattr(AsyncSession, "execute", wrapped)

    batch: list[CandleUpsertRow] = [
        _candle_row(inst_public_id, t0, batch_ts, close=111.0),
        _candle_row(inst_public_id, t1, batch_ts, close=222.0),
    ]
    with pytest.raises(RuntimeError, match="injected mid-batch fault"):
        await repo.upsert_candles(batch)
    assert get_n() == 3

    monkeypatch.undo()

    async with repo.session() as s:
        all_rows = (
            (
                await s.execute(
                    select(Candle)
                    .where(Candle.instrument_public_id == inst_public_id)
                    .order_by(Candle.open_at)
                )
            )
            .scalars()
            .all()
        )
    assert len(all_rows) == 2
    for row in all_rows:
        assert row.known_to == KNOWN_TO_MAX
        assert row.public_id == original_public_ids[row.open_at]
    assert all_rows[0].close == pytest.approx(100.0)
    assert all_rows[1].close == pytest.approx(200.0)


@pytest.mark.asyncio
async def test_upsert_market_snapshots_mid_batch_failure_rolls_back_all(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A RuntimeError on row 2's UPDATE-close rolls back row 1's close+insert too.

    Same contract as candles: both seeded snapshots keep their original
    ``public_id`` values with ``known_to == KNOWN_TO_MAX``; no closed
    versions are created; no new snapshot rows land — the autoflushed
    INSERT for row 1's new version was rolled back along with row 1's
    UPDATE-close and the aborted row 2 UPDATE-close.
    """
    db_path = tmp_path / "scd2_snapshots.db"
    repo, _inst_id, inst_public_id = await _create_repo_with_instrument(db_path)

    second_inst_id, second_inst_public_id = await repo.ensure_instrument(
        symbol_public_id=(
            await _seed_second_symbol(repo, native_symbol="ETH-USD", base="ETH", quote="USD")
        ),
        exchange="kraken",
        timestamp=SEED_TIME,
        session_id="test-session",
        sequence_id=2,
    )
    assert second_inst_id != 0

    seed_ts = datetime(2024, 6, 1, 12, 0, 10, tzinfo=UTC)
    seed_batch: list[MarketSnapshotUpsertRow] = [
        _snapshot_row(inst_public_id, seed_ts, bid=100.0, ask=101.0, last_price=100.5),
        _snapshot_row(
            second_inst_public_id,
            seed_ts,
            bid=2000.0,
            ask=2001.0,
            last_price=2000.5,
        ),
    ]
    inserted = await repo.upsert_market_snapshots(seed_batch)
    assert inserted == 2

    async with repo.session() as s:
        seeded_rows = (
            (await s.execute(select(MarketSnapshot).order_by(MarketSnapshot.instrument_public_id)))
            .scalars()
            .all()
        )
    assert len(seeded_rows) == 2
    original_public_ids = {row.instrument_public_id: row.public_id for row in seeded_rows}
    original_bids = {row.instrument_public_id: row.bid for row in seeded_rows}

    wrapped, get_n = _fail_on_nth_execute(target=3)
    monkeypatch.setattr(AsyncSession, "execute", wrapped)

    batch_ts = datetime(2024, 6, 1, 12, 0, 20, tzinfo=UTC)
    batch: list[MarketSnapshotUpsertRow] = [
        _snapshot_row(inst_public_id, batch_ts, bid=110.0, ask=111.0, last_price=110.5),
        _snapshot_row(
            second_inst_public_id,
            batch_ts,
            bid=2100.0,
            ask=2101.0,
            last_price=2100.5,
        ),
    ]
    with pytest.raises(RuntimeError, match="injected mid-batch fault"):
        await repo.upsert_market_snapshots(batch)
    assert get_n() == 3

    monkeypatch.undo()

    async with repo.session() as s:
        all_rows = (
            (await s.execute(select(MarketSnapshot).order_by(MarketSnapshot.instrument_public_id)))
            .scalars()
            .all()
        )
    assert len(all_rows) == 2
    for row in all_rows:
        assert row.known_to == KNOWN_TO_MAX
        assert row.public_id == original_public_ids[row.instrument_public_id]
        assert row.bid == pytest.approx(original_bids[row.instrument_public_id])


@pytest.mark.asyncio
async def test_concurrent_upsert_candles_serializes() -> None:
    """Two concurrent tasks on the same natural key preserve the SCD2 chain invariant.

    Uses a file-based SQLite database (not ``:memory:`` — which shares a
    single connection through the StaticPool and hides real
    multi-connection serialization) with two separate ``SQLAlchemyRepository``
    instances to exercise the actual write-lock contention path.

    Accepted outcomes (per the documented SQLite isolation semantics):

    - **PostgreSQL**: both tasks complete without exception thanks to
      MVCC row locks acquired by ``SELECT ... FOR UPDATE``. Final DB
      state carries one active row + two closed versions, all rows
      sharing the single carried-forward ``public_id``.
    - **SQLite** (aiosqlite): default transaction startup mode
      makes ``SELECT ... FOR UPDATE`` a no-op. Both tasks can read the
      same seeded active row before either writes, so the second task
      to INSERT raises ``IntegrityError`` on ``candles.public_id``
      UNIQUE (both tasks try to reuse the carried-forward
      ``public_id``). Final DB state carries one active row + one
      closed version.

    Invariants pinned (dialect-agnostic): exactly one active row; every
    row shares the single carried-forward ``public_id``; no orphan row
    with a different ``public_id`` landed; the number of closed rows
    equals the number of successful upserts; any failure is
    ``IntegrityError`` (not some other unexpected error).
    """
    with tempfile.TemporaryDirectory() as td:
        seed_repo: SQLAlchemyRepository | None = None
        repo_a: SQLAlchemyRepository | None = None
        repo_b: SQLAlchemyRepository | None = None
        db_path = Path(td) / "scd2_concurrent.db"
        try:
            seed_repo, _inst_id, inst_public_id = await _create_repo_with_instrument(db_path)

            open_at = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
            seed_ts = datetime(2024, 6, 1, 12, 0, 10, tzinfo=UTC)
            inserted = await seed_repo.upsert_candles(
                [_candle_row(inst_public_id, open_at, seed_ts, close=100.0)]
            )
            assert inserted == 1

            async with seed_repo.session() as s:
                seeded = (
                    (await s.execute(select(Candle).where(Candle.open_at == open_at)))
                    .scalars()
                    .first()
                )
            assert seeded is not None
            seed_public_id = seeded.public_id

            db_url = f"sqlite+aiosqlite:///{db_path}"
            repo_a = SQLAlchemyRepository(db_url)
            repo_b = SQLAlchemyRepository(db_url)

            ts_a = datetime(2024, 6, 1, 12, 0, 20, tzinfo=UTC)
            ts_b = datetime(2024, 6, 1, 12, 0, 30, tzinfo=UTC)
            task_a = asyncio.create_task(
                repo_a.upsert_candles([_candle_row(inst_public_id, open_at, ts_a, close=111.0)])
            )
            task_b = asyncio.create_task(
                repo_b.upsert_candles([_candle_row(inst_public_id, open_at, ts_b, close=222.0)])
            )
            results = await asyncio.gather(task_a, task_b, return_exceptions=True)

            successes = [r for r in results if isinstance(r, int)]
            failures = [r for r in results if isinstance(r, BaseException)]
            assert successes, "at least one concurrent upsert must succeed"
            for failure in failures:
                assert isinstance(
                    failure, IntegrityError
                ), f"unexpected failure type under contention: {type(failure).__name__}"

            async with seed_repo.session() as s:
                rows = (
                    (
                        await s.execute(
                            select(Candle).where(
                                and_(
                                    Candle.instrument_public_id == inst_public_id,
                                    Candle.open_at == open_at,
                                )
                            )
                        )
                    )
                    .scalars()
                    .all()
                )

            active = [r for r in rows if r.known_to == KNOWN_TO_MAX]
            closed = [r for r in rows if r.known_to != KNOWN_TO_MAX]
            assert len(active) == 1, f"SCD2 chain broken: expected 1 active row, got {len(active)}"
            public_ids = {r.public_id for r in rows}
            assert public_ids == {
                seed_public_id
            }, f"orphan public_id introduced under contention: {public_ids}"
            expected_rows = len(successes) + 1
            assert (
                len(rows) == expected_rows
            ), f"expected seed + {len(successes)} successful version(s), got {len(rows)}"
            assert len(closed) == len(
                successes
            ), f"expected {len(successes)} closed version(s), got {len(closed)}"
        finally:
            for repo in (repo_b, repo_a, seed_repo):
                if repo is not None:
                    await repo.engine.dispose()


async def _seed_second_symbol(
    repo: SQLAlchemyRepository, *, native_symbol: str, base: str, quote: str
) -> str:
    """Create a second Symbol so the snapshot test can batch two instruments."""
    sym = Symbol(
        native_symbol=native_symbol,
        base=base,
        quote=quote,
        asset_type="crypto",
        created_at=SEED_TIME,
        timestamp=SEED_TIME,
        session_id="test-session",
        sequence_id=2,
    )
    async with repo.session() as s:
        s.add(sym)
        await s.commit()
    assert sym.public_id is not None
    return sym.public_id
