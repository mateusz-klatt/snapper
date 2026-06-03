"""Tests for the ``instrument_feed_health`` repository methods.

Covers :meth:`SQLAlchemyRepository.upsert_instrument_feed_health` and
:meth:`SQLAlchemyRepository.list_instrument_feed_health`:

* insert of new natural keys,
* last-write-wins upsert idempotency on the
  ``(coordinator, exchange, channel, symbol)`` key,
* the empty-rows no-op short-circuit,
* the optional ``exchange`` read filter,
* read ordering by ``(exchange, channel, symbol)``.

Uses the in-memory aiosqlite fixture so the cross-dialect upsert is
exercised on the SQLite ``on_conflict_do_update`` path.
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest
from sqlalchemy.exc import IntegrityError

from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import InstrumentFeedHealthUpsertRow

_T0 = datetime(2026, 6, 3, 12, 0, tzinfo=UTC)
_T1 = datetime(2026, 6, 3, 12, 5, tzinfo=UTC)


@pytest.fixture
async def _repo() -> SQLAlchemyRepository:
    """Async fixture yielding a fresh in-memory aiosqlite repository."""
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    await repo.create_all()
    return repo


def _row(
    *,
    coordinator: str = "coord-0",
    exchange: str = "kraken",
    channel: str = "ticker",
    symbol: str = "BTC/USD",
    status: str = "confirmed",
    requested_at: datetime = _T0,
    confirmed_at: datetime | None = _T0,
    last_seen_data_at: datetime | None = _T0,
    last_error: str | None = None,
    retry_count: int = 0,
    snapshot_at: datetime = _T0,
) -> InstrumentFeedHealthUpsertRow:
    """Build one feed-health upsert row with overridable fields."""
    return InstrumentFeedHealthUpsertRow(
        coordinator=coordinator,
        exchange=exchange,
        channel=channel,
        symbol=symbol,
        status=status,
        requested_at=requested_at,
        confirmed_at=confirmed_at,
        last_seen_data_at=last_seen_data_at,
        last_error=last_error,
        retry_count=retry_count,
        snapshot_at=snapshot_at,
    )


class TestUpsertInstrumentFeedHealth:
    """Insert, idempotent overwrite, and no-op behaviours."""

    @pytest.mark.asyncio
    async def test_empty_rows_is_noop(self, _repo: SQLAlchemyRepository) -> None:
        """An empty row list writes nothing and the table stays empty."""
        await _repo.upsert_instrument_feed_health([])
        assert await _repo.list_instrument_feed_health() == []

    @pytest.mark.asyncio
    async def test_insert_new_keys(self, _repo: SQLAlchemyRepository) -> None:
        """New natural keys insert one row each, projected back faithfully."""
        await _repo.upsert_instrument_feed_health(
            [
                _row(symbol="BTC/USD"),
                _row(symbol="ETH/USD", status="pending", confirmed_at=None),
            ]
        )
        rows = await _repo.list_instrument_feed_health()
        assert len(rows) == 2
        symbols = {row["symbol"] for row in rows}
        assert symbols == {"BTC/USD", "ETH/USD"}
        eth = next(row for row in rows if row["symbol"] == "ETH/USD")
        assert eth["status"] == "pending"
        assert eth["confirmed_at"] is None

    @pytest.mark.asyncio
    async def test_upsert_is_last_write_wins(self, _repo: SQLAlchemyRepository) -> None:
        """Re-upserting the same key overwrites every non-key column."""
        await _repo.upsert_instrument_feed_health([_row()])
        await _repo.upsert_instrument_feed_health(
            [
                _row(
                    status="failed",
                    confirmed_at=None,
                    last_seen_data_at=None,
                    last_error="boom",
                    retry_count=3,
                    requested_at=_T1,
                    snapshot_at=_T1,
                )
            ]
        )
        rows = await _repo.list_instrument_feed_health()
        assert len(rows) == 1
        row = rows[0]
        assert row["status"] == "failed"
        assert row["confirmed_at"] is None
        assert row["last_seen_data_at"] is None
        assert row["last_error"] == "boom"
        assert row["retry_count"] == 3
        assert row["requested_at"] == _T1
        assert row["snapshot_at"] == _T1

    @pytest.mark.asyncio
    async def test_distinct_coordinators_do_not_collide(self, _repo: SQLAlchemyRepository) -> None:
        """Same exchange/channel/symbol under two coordinators are distinct rows."""
        await _repo.upsert_instrument_feed_health(
            [_row(coordinator="coord-0"), _row(coordinator="coord-1")]
        )
        rows = await _repo.list_instrument_feed_health()
        assert {row["coordinator"] for row in rows} == {"coord-0", "coord-1"}

    @pytest.mark.asyncio
    async def test_chunks_large_snapshot_over_param_limit(
        self, _repo: SQLAlchemyRepository
    ) -> None:
        """A snapshot far past SQLite's 999-parameter cap upserts via chunking."""
        big = [_row(symbol=f"SYM-{i}") for i in range(200)]
        await _repo.upsert_instrument_feed_health(big)
        rows = await _repo.list_instrument_feed_health()
        assert len(rows) == 200

    @pytest.mark.asyncio
    async def test_long_last_error_persists_untruncated(self, _repo: SQLAlchemyRepository) -> None:
        """An over-512-char exchange error round-trips intact (Text column)."""
        long_error = "x" * 2000
        await _repo.upsert_instrument_feed_health([_row(status="failed", last_error=long_error)])
        rows = await _repo.list_instrument_feed_health()
        assert rows[0]["last_error"] == long_error

    @pytest.mark.asyncio
    async def test_uppercase_exchange_rejected(self, _repo: SQLAlchemyRepository) -> None:
        """The lowercase-exchange CHECK rejects a mixed-case exchange."""
        with pytest.raises(IntegrityError):
            await _repo.upsert_instrument_feed_health([_row(exchange="Kraken")])


class TestListInstrumentFeedHealth:
    """Read filtering and ordering."""

    @pytest.mark.asyncio
    async def test_exchange_filter(self, _repo: SQLAlchemyRepository) -> None:
        """The optional exchange filter restricts rows to that exchange."""
        await _repo.upsert_instrument_feed_health(
            [
                _row(exchange="kraken", symbol="BTC/USD"),
                _row(exchange="kraken_futures", symbol="PF_XBTUSD"),
            ]
        )
        kraken_rows = await _repo.list_instrument_feed_health(exchange="kraken")
        assert len(kraken_rows) == 1
        assert kraken_rows[0]["exchange"] == "kraken"

    @pytest.mark.asyncio
    async def test_ordering_by_exchange_channel_symbol(self, _repo: SQLAlchemyRepository) -> None:
        """Rows come back ordered by (exchange, channel, symbol)."""
        await _repo.upsert_instrument_feed_health(
            [
                _row(exchange="kraken", channel="trade", symbol="ETH/USD"),
                _row(exchange="kraken", channel="ticker", symbol="ETH/USD"),
                _row(exchange="kraken", channel="ticker", symbol="BTC/USD"),
            ]
        )
        rows = await _repo.list_instrument_feed_health()
        keys = [(row["exchange"], row["channel"], row["symbol"]) for row in rows]
        assert keys == [
            ("kraken", "ticker", "BTC/USD"),
            ("kraken", "ticker", "ETH/USD"),
            ("kraken", "trade", "ETH/USD"),
        ]

    @pytest.mark.asyncio
    async def test_fresh_within_seconds_drops_stale_rows(self, _repo: SQLAlchemyRepository) -> None:
        """The freshness filter excludes rows snapshotted before the window."""
        now = datetime.now(UTC)
        await _repo.upsert_instrument_feed_health(
            [
                _row(symbol="FRESH", snapshot_at=now - timedelta(seconds=30)),
                _row(symbol="STALE", snapshot_at=now - timedelta(hours=2)),
            ]
        )
        all_rows = await _repo.list_instrument_feed_health()
        assert {row["symbol"] for row in all_rows} == {"FRESH", "STALE"}
        fresh = await _repo.list_instrument_feed_health(fresh_within_seconds=300)
        assert {row["symbol"] for row in fresh} == {"FRESH"}
