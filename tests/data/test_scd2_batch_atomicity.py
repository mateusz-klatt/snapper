"""Protect real SCD2 batch rollback and concurrent candle version chains.

Candle and market-snapshot faults must roll back earlier staged changes in
that batch. The candle fault counts actual predecessor-close updates rather
than writer-fence or lookup statements, flushes the first successor, then
fails the second close to prove durable transaction atomicity.

Concurrent candle writers reserve their transaction before checking current
authors. Both calls complete normally; an older queued author may return zero
when the newer author committed first. Actual positive write counts determine
the number of closed versions, with one carried public identity and an exact
contiguous history ending at the newest author.
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
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.dml import Update

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
    match: Callable[[object], bool] | None = None,
    flush_before_fault: bool = False,
) -> tuple[Callable[..., Awaitable[Any]], Callable[[], int]]:
    """Fault on a selected execute call after optionally flushing its predecessor.

    An optional statement predicate excludes serialization and lookup work.
    Flushing before the injected fault proves an earlier staged successor was
    actually sent to the database before the transaction rolls back.
    """
    original = AsyncSession.execute
    counter = {"n": 0}

    async def wrapped(self: AsyncSession, *args: Any, **kwargs: Any) -> Any:
        """Delegate unrelated statements and fault only at the selected write boundary."""
        if match is not None and not match(args[0]):
            return await original(self, *args, **kwargs)
        counter["n"] += 1
        if counter["n"] == target:
            if flush_before_fault:
                await self.flush()
            raise RuntimeError(message)
        return await original(self, *args, **kwargs)

    return wrapped, lambda: counter["n"]


def _is_candle_close(statement: object, timestamp: datetime) -> bool:
    """Match a real candle predecessor close while excluding the empty writer fence."""
    return (
        isinstance(statement, Update)
        and getattr(statement.table, "name", None) == "candles"
        and statement.compile().params.get("known_to") == timestamp
    )


@pytest.mark.asyncio
async def test_upsert_candles_mid_batch_failure_rolls_back_all(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A RuntimeError on row 2's UPDATE-close rolls back row 1's close+insert too.

    Both seeded candles (``open_at=t0`` and ``open_at=t1``) must keep
    their original ``public_id`` with ``known_to == KNOWN_TO_MAX``;
    no closed versions exist; no new candle rows land — meaning the
    explicitly flushed INSERT for row 1's new version was rolled back along
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

    wrapped, get_n = _fail_on_nth_execute(
        target=2,
        match=lambda statement: _is_candle_close(statement, batch_ts),
        flush_before_fault=True,
    )
    monkeypatch.setattr(AsyncSession, "execute", wrapped)

    batch: list[CandleUpsertRow] = [
        _candle_row(inst_public_id, t0, batch_ts, close=111.0),
        _candle_row(inst_public_id, t1, batch_ts, close=222.0),
    ]
    with pytest.raises(RuntimeError, match="injected mid-batch fault"):
        await repo.upsert_candles(batch)
    assert get_n() == 2

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
    """Real concurrent SQLite writers preserve the newest author and exact SCD2 chain.

    Given: Independent file-backed connections and one seeded candle version,
    When: Older and newer author batches contend for the same natural key,
    Then: Both calls finish without errors, stale work can return zero, and
        actual positive writes exactly match the contiguous carried-ID history.
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
            invariant_fields = {
                column.name: getattr(seeded, column.name)
                for column in Candle.__table__.columns
                if column.name not in {"id", "timestamp", "known_to", "close"}
            }

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
            results = await asyncio.gather(task_a, task_b)
            assert all(type(result) is int and result in (0, 1) for result in results)
            assert results[1] == 1
            changed_versions = sum(results)
            assert 1 <= changed_versions <= 2

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
            assert len(active) == 1, f"SCD2 chain broken: expected 1 active row, got {len(active)}"
            assert {r.public_id for r in rows} == {
                seed_public_id
            }, "orphan public_id introduced under contention"
            assert len(rows) == changed_versions + 1
            assert sum(row.known_to != KNOWN_TO_MAX for row in rows) == changed_versions
            assert active[0].timestamp == ts_b
            assert active[0].close == 222.0
            expected_versions = [(seed_ts, 100.0)]
            if results[0] == 1:
                expected_versions.append((ts_a, 111.0))
            expected_versions.append((ts_b, 222.0))
            ordered = sorted(rows, key=lambda row: row.timestamp)
            assert [(row.timestamp, row.close) for row in ordered] == expected_versions
            assert [row.known_to for row in ordered] == [
                timestamp for timestamp, _close in expected_versions[1:]
            ] + [KNOWN_TO_MAX]
            assert all(
                all(getattr(row, name) == value for name, value in invariant_fields.items())
                for row in ordered
            )
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
