"""Service-level tests for :mod:`snapper.application.services.candle_query`.

Tests cover the three public entry points (:func:`fetch_candles`,
:func:`fetch_db_only`, :func:`fetch_cache_only`) plus the projection
helpers (:func:`row_from_snap`, :func:`row_from_db`,
:func:`derive_snaps`). The smart-routing decision tree is exercised by
:class:`TestFetchCandlesSmartRouter`; cache-only diagnostic semantics
by :class:`TestFetchCacheOnly`; DB-only escape hatch by
:class:`TestFetchDbOnly`; backfill + dedup logic by
:class:`TestFetchCandlesBackfill`.
"""

from datetime import UTC
from datetime import datetime
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.application.services.candle_query import CACHE_ELIGIBLE_TIMEFRAMES
from snapper.application.services.candle_query import DB_FALLBACK_TIMEFRAMES
from snapper.application.services.candle_query import VALID_TIMEFRAMES
from snapper.application.services.candle_query import CacheUnavailableError
from snapper.application.services.candle_query import derive_snaps
from snapper.application.services.candle_query import fetch_cache_only
from snapper.application.services.candle_query import fetch_candles
from snapper.application.services.candle_query import fetch_db_only
from snapper.application.services.candle_query import row_from_db
from snapper.application.services.candle_query import row_from_snap
from snapper.application.services.market_cache import CandleSnap
from snapper.core.types import AllExchange
from snapper.data.repository import Repository
from snapper.data.repository_types import CandleRow


def _snap(open_at_ms: int, close: float) -> CandleSnap:
    """Tiny :class:`CandleSnap` builder."""
    return CandleSnap(
        open_at_ms=open_at_ms,
        open=close,
        high=close + 1.0,
        low=close - 1.0,
        close=close,
        volume=10.0,
    )


def _row(open_at_ms: int, close: float, timeframe: str = "1m") -> CandleRow:
    """Stub :class:`CandleRow` for the DB path."""
    return CandleRow(
        open_at=datetime.fromtimestamp(open_at_ms / 1000, tz=UTC),
        timeframe=timeframe,
        open=close,
        high=close + 1.0,
        low=close - 1.0,
        close=close,
        volume=10.0,
        vwap=0.5,
        trades=7,
        public_id="00000000-0000-7000-8000-0000000000aa",
        timestamp=datetime.fromtimestamp(open_at_ms / 1000, tz=UTC),
        session_id="seed-sid",
        sequence_id=1,
    )


def _stub_cache(snaps: list[CandleSnap]) -> MagicMock:
    """Mock :class:`MarketCacheService` honouring ``limit`` like the real cache.

    The real cache slices its deque via ``list(cached)[-limit:]`` so the
    mock mirrors that contract; tests asserting cache-only responses
    depend on the slice happening at the service level, not in the
    cache mock.
    """

    def _get_1m_candles(
        _exchange: AllExchange, _symbol: str, *, limit: int = 100
    ) -> list[CandleSnap]:
        if limit >= len(snaps):
            return list(snaps)
        return list(snaps[-limit:])

    cache = MagicMock()
    cache.get_1m_candles = AsyncMock(side_effect=_get_1m_candles)
    cache.cache_capacity_per_instrument = MagicMock(return_value=100)
    return cache


def _stub_repo(rows: list[CandleRow]) -> MagicMock:
    """Mock :class:`Repository` returning the given rows for ``get_candles``."""
    repo = MagicMock()
    repo.get_candles = AsyncMock(return_value=rows)
    return repo


class TestConstants:
    """Module-level constants form the contract surface for routes."""

    def test_cache_eligible_set_is_one_through_thirty(self) -> None:
        """Only 1m/5m/15m/30m can be cache-served."""
        assert frozenset({"1m", "5m", "15m", "30m"}) == CACHE_ELIGIBLE_TIMEFRAMES

    def test_db_fallback_set_is_long_frames(self) -> None:
        """1h/4h/1d cannot be cache-served from a 100-minute deque."""
        assert frozenset({"1h", "4h", "1d"}) == DB_FALLBACK_TIMEFRAMES

    def test_valid_timeframes_is_union(self) -> None:
        """All seven supported timeframes accepted."""
        assert VALID_TIMEFRAMES == CACHE_ELIGIBLE_TIMEFRAMES | DB_FALLBACK_TIMEFRAMES


class TestDeriveSnaps:
    """``derive_snaps`` aggregates fixed-size windows + drops partial tails."""

    def test_aggregates_full_windows(self) -> None:
        """Five 1m bars aggregate into one 5m bar with correct OHLCV."""
        snaps = [
            _snap(0, 1.0),
            _snap(60_000, 2.0),
            _snap(120_000, 1.5),
            _snap(180_000, 0.5),
            _snap(240_000, 3.0),
        ]
        derived = derive_snaps(snaps, minutes_per_bar=5)
        assert len(derived) == 1
        bar = derived[0]
        assert bar.open == pytest.approx(1.0)
        assert bar.close == pytest.approx(3.0)
        assert bar.high == pytest.approx(4.0)
        assert bar.low == pytest.approx(-0.5)
        assert bar.volume == pytest.approx(50.0)

    def test_drops_partial_trailing_bucket(self) -> None:
        """A 7-snap input with bucket size 5 yields one bar (5 used, 2 dropped)."""
        snaps = [_snap(i * 60_000, float(i)) for i in range(7)]
        assert len(derive_snaps(snaps, minutes_per_bar=5)) == 1

    def test_empty_input_returns_empty(self) -> None:
        """No snaps means no derived bars."""
        assert derive_snaps([], minutes_per_bar=5) == []

    def test_minutes_per_bar_one_is_identity(self) -> None:
        """``minutes_per_bar=1`` passes input through unchanged."""
        snaps = [_snap(0, 1.0), _snap(60_000, 2.0)]
        assert derive_snaps(snaps, minutes_per_bar=1) == snaps

    def test_skips_off_boundary_leading_snaps(self) -> None:
        """Snaps starting off a canonical boundary are skipped until alignment.

        A cache window that began at 60_000 (one minute past a 5m
        boundary) cannot synthesise a canonical 5m bar at that
        timestamp; the bucket must start at 300_000.
        """
        snaps = [_snap((i + 1) * 60_000, 1.0) for i in range(5)]
        derived = derive_snaps(snaps, minutes_per_bar=5)
        assert derived == []

    def test_skips_bucket_with_internal_gap(self) -> None:
        """A gap inside the bucket aborts that bucket and advances by one.

        Five candidate 1m bars are spaced ``[0, 60k, 180k, 240k, 300k]``
        — the missing 120k slot prevents the bucket from completing.
        The walker then advances to the next snap and tries again;
        none of the remaining starts land on a 5m boundary that has
        a full gap-free run, so no derived bar appears.
        """
        snaps = [
            _snap(0, 1.0),
            _snap(60_000, 2.0),
            _snap(180_000, 3.0),
            _snap(240_000, 4.0),
            _snap(300_000, 5.0),
        ]
        derived = derive_snaps(snaps, minutes_per_bar=5)
        assert derived == []

    def test_resumes_after_gap_on_next_boundary(self) -> None:
        """A gap followed by a full bucket on a later boundary still emits.

        First bucket at ``0`` is incomplete due to a gap; second
        bucket at ``300_000`` is contiguous and complete; only the
        second bar materialises.
        """
        snaps = [
            _snap(0, 1.0),
            _snap(60_000, 2.0),
            _snap(300_000, 10.0),
            _snap(360_000, 11.0),
            _snap(420_000, 12.0),
            _snap(480_000, 13.0),
            _snap(540_000, 14.0),
        ]
        derived = derive_snaps(snaps, minutes_per_bar=5)
        assert len(derived) == 1
        assert derived[0].open_at_ms == 300_000


class TestRowFromSnap:
    """``row_from_snap`` projects cache snaps with ``None`` provenance."""

    def test_provenance_fields_are_none(self) -> None:
        """Cache rows carry no DB-row identity."""
        snap = _snap(60_000, 1.5)
        row = row_from_snap(snap, "1m")
        assert row.public_id is None
        assert row.timestamp is None
        assert row.session_id is None
        assert row.sequence_id is None
        assert row.vwap is None
        assert row.trades is None

    def test_ohlcv_preserved(self) -> None:
        """OHLCV survives the projection."""
        snap = _snap(0, 2.0)
        row = row_from_snap(snap, "5m")
        assert row.open == pytest.approx(2.0)
        assert row.high == pytest.approx(3.0)
        assert row.low == pytest.approx(1.0)
        assert row.close == pytest.approx(2.0)
        assert row.volume == pytest.approx(10.0)
        assert row.timeframe == "5m"


class TestRowFromDb:
    """``row_from_db`` preserves DB-row provenance + optional vwap/trades."""

    def test_provenance_populated(self) -> None:
        """DB rows pass their session/sequence identity through."""
        db = _row(0, 1.0, timeframe="1h")
        row = row_from_db(db, "1h")
        assert row.public_id == "00000000-0000-7000-8000-0000000000aa"
        assert row.session_id == "seed-sid"
        assert row.sequence_id == 1
        assert row.timestamp is not None


class TestFetchDbOnly:
    """``fetch_db_only`` always reads the repository, regardless of cache."""

    @pytest.mark.asyncio
    async def test_returns_db_rows_with_source_db(self) -> None:
        """Three DB rows come back chronologically with ``source='db'``."""
        rows = [_row(i * 3600_000, float(i), timeframe="1h") for i in range(3)]
        repo = _stub_repo(rows)
        result = await fetch_db_only(
            repo=cast(Repository, repo),
            exchange="kraken",
            native_symbol="BTC-USD",
            timeframe="1h",
            limit=10,
        )
        assert result.source == "db"
        assert result.sample_count == 3
        assert result.is_warm is False
        repo.get_candles.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_is_warm_true_when_limit_satisfied(self) -> None:
        """``is_warm`` flips ``True`` once ``sample_count >= limit``."""
        rows = [_row(i * 3600_000, 1.0) for i in range(5)]
        repo = _stub_repo(rows)
        result = await fetch_db_only(
            repo=cast(Repository, repo),
            exchange="kraken",
            native_symbol="BTC-USD",
            timeframe="1h",
            limit=5,
        )
        assert result.is_warm is True

    @pytest.mark.asyncio
    async def test_chronological_order_after_reverse(self) -> None:
        """``order='desc'`` from repo gets reversed to oldest-first."""
        rows = [_row(2_000, 2.0), _row(1_000, 1.0)]
        repo = _stub_repo(rows)
        result = await fetch_db_only(
            repo=cast(Repository, repo),
            exchange="kraken",
            native_symbol="BTC-USD",
            timeframe="1h",
            limit=10,
        )
        assert [row.open for row in result.rows] == [1.0, 2.0]

    @pytest.mark.asyncio
    async def test_explicit_as_of_overrides_now(self) -> None:
        """An ``as_of`` argument is passed to the repo verbatim."""
        repo = _stub_repo([])
        explicit = datetime(2026, 1, 1, tzinfo=UTC)
        await fetch_db_only(
            repo=cast(Repository, repo),
            exchange="kraken",
            native_symbol="BTC-USD",
            timeframe="1h",
            limit=10,
            as_of=explicit,
        )
        kwargs = repo.get_candles.await_args.kwargs
        assert kwargs["as_of"] == explicit


class TestFetchCacheOnly:
    """``fetch_cache_only`` mirrors the diagnostic route's strict cache semantics."""

    @pytest.mark.asyncio
    async def test_one_minute_returns_cache_source(self) -> None:
        """``1m`` reads the cache deque with ``source='cache'``."""
        snaps = [_snap(i * 60_000, float(i)) for i in range(50)]
        cache = _stub_cache(snaps)
        result = await fetch_cache_only(
            cache=cache,
            repo=cast(Repository, _stub_repo([])),
            exchange="kraken",
            native_symbol="BTC-USD",
            timeframe="1m",
            limit=50,
        )
        assert result.source == "cache"
        assert result.sample_count == 50
        assert result.is_warm is True

    @pytest.mark.asyncio
    async def test_cold_cache_returns_is_warm_false(self) -> None:
        """A short cache returns the available rows + ``is_warm=False``."""
        cache = _stub_cache([_snap(0, 1.0)])
        result = await fetch_cache_only(
            cache=cache,
            repo=cast(Repository, _stub_repo([])),
            exchange="kraken",
            native_symbol="BTC-USD",
            timeframe="1m",
            limit=100,
        )
        assert result.is_warm is False
        assert result.sample_count == 1

    @pytest.mark.asyncio
    async def test_derived_five_minute_source_derived(self) -> None:
        """``5m`` aggregates the 1m deque + flags ``source='derived'``."""
        snaps = [_snap(i * 60_000, float(i)) for i in range(50)]
        cache = _stub_cache(snaps)
        result = await fetch_cache_only(
            cache=cache,
            repo=cast(Repository, _stub_repo([])),
            exchange="kraken",
            native_symbol="BTC-USD",
            timeframe="5m",
            limit=10,
        )
        assert result.source == "derived"
        assert result.sample_count == 10

    @pytest.mark.asyncio
    async def test_derived_truncates_to_limit(self) -> None:
        """The derived series gets sliced to the requested ``limit``."""
        snaps = [_snap(i * 60_000, float(i)) for i in range(100)]
        cache = _stub_cache(snaps)
        result = await fetch_cache_only(
            cache=cache,
            repo=cast(Repository, _stub_repo([])),
            exchange="kraken",
            native_symbol="BTC-USD",
            timeframe="5m",
            limit=5,
        )
        assert result.sample_count == 5

    @pytest.mark.asyncio
    async def test_one_hour_falls_back_to_db(self) -> None:
        """``1h`` cannot live in the cache; falls through to DB."""
        rows = [_row(i * 3600_000, float(i), timeframe="1h") for i in range(3)]
        result = await fetch_cache_only(
            cache=_stub_cache([_snap(0, 1.0)]),
            repo=cast(Repository, _stub_repo(rows)),
            exchange="kraken",
            native_symbol="BTC-USD",
            timeframe="1h",
            limit=10,
        )
        assert result.source == "db"
        assert result.sample_count == 3

    @pytest.mark.asyncio
    async def test_cache_unavailable_for_cache_eligible_raises(self) -> None:
        """``cache is None`` on 1m raises :class:`CacheUnavailableError`."""
        with pytest.raises(CacheUnavailableError):
            await fetch_cache_only(
                cache=None,
                repo=cast(Repository, _stub_repo([])),
                exchange="kraken",
                native_symbol="BTC-USD",
                timeframe="1m",
                limit=100,
            )

    @pytest.mark.asyncio
    async def test_cache_unavailable_for_derived_raises(self) -> None:
        """``cache is None`` on 5m raises :class:`CacheUnavailableError`."""
        with pytest.raises(CacheUnavailableError):
            await fetch_cache_only(
                cache=None,
                repo=cast(Repository, _stub_repo([])),
                exchange="kraken",
                native_symbol="BTC-USD",
                timeframe="5m",
                limit=100,
            )

    @pytest.mark.asyncio
    async def test_cache_unavailable_long_frame_still_falls_back(self) -> None:
        """``cache is None`` on 1h still serves DB (no raise)."""
        rows = [_row(0, 1.0, timeframe="1h")]
        result = await fetch_cache_only(
            cache=None,
            repo=cast(Repository, _stub_repo(rows)),
            exchange="kraken",
            native_symbol="BTC-USD",
            timeframe="1h",
            limit=10,
        )
        assert result.source == "db"
        assert result.sample_count == 1


class TestFetchCandlesSmartRouter:
    """``fetch_candles`` smart-routes between cache / cache+backfill / DB."""

    @pytest.mark.asyncio
    async def test_as_of_hard_routes_to_db(self) -> None:
        """``as_of`` set always reads DB even when cache could serve."""
        snaps = [_snap(i * 60_000, float(i)) for i in range(100)]
        cache = _stub_cache(snaps)
        rows = [_row(60_000, 99.0, timeframe="1m")]
        repo = _stub_repo(rows)
        result = await fetch_candles(
            cache=cache,
            repo=cast(Repository, repo),
            exchange="kraken",
            native_symbol="BTC-USD",
            timeframe="1m",
            limit=10,
            as_of=datetime(2026, 1, 1, tzinfo=UTC),
        )
        assert result.source == "db"
        cache.get_1m_candles.assert_not_called()
        repo.get_candles.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_cache_none_routes_to_db(self) -> None:
        """No cache wired → DB path even without ``as_of``."""
        rows = [_row(60_000, 1.0)]
        repo = _stub_repo(rows)
        result = await fetch_candles(
            cache=None,
            repo=cast(Repository, repo),
            exchange="kraken",
            native_symbol="BTC-USD",
            timeframe="1m",
            limit=10,
        )
        assert result.source == "db"
        assert result.sample_count == 1

    @pytest.mark.asyncio
    async def test_long_frame_routes_to_db_with_warm_cache(self) -> None:
        """``1h`` reads DB even when cache has 1m data."""
        snaps = [_snap(i * 60_000, float(i)) for i in range(100)]
        cache = _stub_cache(snaps)
        rows = [_row(i * 3600_000, float(i), timeframe="1h") for i in range(3)]
        repo = _stub_repo(rows)
        result = await fetch_candles(
            cache=cache,
            repo=cast(Repository, repo),
            exchange="kraken",
            native_symbol="BTC-USD",
            timeframe="1h",
            limit=10,
        )
        assert result.source == "db"
        assert result.sample_count == 3

    @pytest.mark.asyncio
    async def test_warm_cache_serves_one_minute_directly(self) -> None:
        """Cache has enough 1m bars → returns them without touching DB."""
        snaps = [_snap(i * 60_000, float(i)) for i in range(100)]
        cache = _stub_cache(snaps)
        repo = _stub_repo([])
        result = await fetch_candles(
            cache=cache,
            repo=cast(Repository, repo),
            exchange="kraken",
            native_symbol="BTC-USD",
            timeframe="1m",
            limit=50,
        )
        assert result.source == "cache"
        assert result.sample_count == 50
        assert result.is_warm is True
        repo.get_candles.assert_not_called()

    @pytest.mark.asyncio
    async def test_derived_serves_five_minute_directly(self) -> None:
        """Cache aggregates 5m bars without DB backfill when long enough."""
        snaps = [_snap(i * 60_000, float(i)) for i in range(100)]
        cache = _stub_cache(snaps)
        repo = _stub_repo([])
        result = await fetch_candles(
            cache=cache,
            repo=cast(Repository, repo),
            exchange="kraken",
            native_symbol="BTC-USD",
            timeframe="5m",
            limit=15,
        )
        assert result.source == "derived"
        assert result.sample_count == 15
        repo.get_candles.assert_not_called()


class TestFetchCandlesBackfill:
    """``fetch_candles`` merges cache + DB when cache is short."""

    @pytest.mark.asyncio
    async def test_backfill_prepends_older_db_rows(self) -> None:
        """Cache holds 2 newest; DB holds 3 older; merged result has all 5 chronological."""
        snaps = [_snap(3 * 60_000, 30.0), _snap(4 * 60_000, 40.0)]
        cache = _stub_cache(snaps)
        rows = [
            _row(2 * 60_000, 20.0),
            _row(1 * 60_000, 10.0),
            _row(0, 0.0),
        ]
        repo = _stub_repo(rows)
        result = await fetch_candles(
            cache=cache,
            repo=cast(Repository, repo),
            exchange="kraken",
            native_symbol="BTC-USD",
            timeframe="1m",
            limit=5,
        )
        assert result.sample_count == 5
        assert [row.open for row in result.rows] == [0.0, 10.0, 20.0, 30.0, 40.0]
        assert result.source == "cache"

    @pytest.mark.asyncio
    async def test_backfill_dedups_overlapping_db_rows(self) -> None:
        """DB row at same ``open_at_ms`` as a cache snap is dropped."""
        snaps = [_snap(60_000, 99.0), _snap(120_000, 100.0)]
        cache = _stub_cache(snaps)
        rows = [_row(60_000, 11.0), _row(0, 0.0)]
        repo = _stub_repo(rows)
        result = await fetch_candles(
            cache=cache,
            repo=cast(Repository, repo),
            exchange="kraken",
            native_symbol="BTC-USD",
            timeframe="1m",
            limit=3,
        )
        assert result.sample_count == 3
        assert [row.open for row in result.rows] == [0.0, 99.0, 100.0]
        assert result.source == "cache"

    @pytest.mark.asyncio
    async def test_backfill_with_empty_cache_falls_back_to_db_source(self) -> None:
        """No cache rows but DB rows present → ``source='db'``."""
        cache = _stub_cache([])
        rows = [_row(i * 60_000, float(i)) for i in range(3)]
        repo = _stub_repo(rows)
        result = await fetch_candles(
            cache=cache,
            repo=cast(Repository, repo),
            exchange="kraken",
            native_symbol="BTC-USD",
            timeframe="1m",
            limit=10,
        )
        assert result.source == "db"
        assert result.sample_count == 3

    @pytest.mark.asyncio
    async def test_backfill_trims_to_limit(self) -> None:
        """Cache + DB total exceeds ``limit``; the oldest rows are dropped."""
        snaps = [_snap(5 * 60_000, 50.0)]
        cache = _stub_cache(snaps)
        rows = [
            _row(4 * 60_000, 40.0),
            _row(3 * 60_000, 30.0),
            _row(2 * 60_000, 20.0),
            _row(1 * 60_000, 10.0),
        ]
        repo = _stub_repo(rows)
        result = await fetch_candles(
            cache=cache,
            repo=cast(Repository, repo),
            exchange="kraken",
            native_symbol="BTC-USD",
            timeframe="1m",
            limit=3,
        )
        assert result.sample_count == 3
        assert [row.open for row in result.rows] == [30.0, 40.0, 50.0]

    @pytest.mark.asyncio
    async def test_backfill_dedups_overlapping_derived_db_row(self) -> None:
        """Derived 5m cache bar at canonical boundary dedupes a DB 5m row at same open_at_ms.

        Closes the dedup hole the 2026-05-14 reviewer pair flagged:
        when the derived bar's ``open_at_ms`` lands on a canonical
        boundary (e.g. ``300_000``) and the DB returns a 5m row at
        the same boundary, the cache wins and the DB row is dropped.
        """
        snaps = [_snap((i + 5) * 60_000, 1.0) for i in range(5)]
        cache = _stub_cache(snaps)
        rows = [
            _row(300_000, 99.0, timeframe="5m"),
            _row(0, 88.0, timeframe="5m"),
        ]
        repo = _stub_repo(rows)
        result = await fetch_candles(
            cache=cache,
            repo=cast(Repository, repo),
            exchange="kraken",
            native_symbol="BTC-USD",
            timeframe="5m",
            limit=10,
        )
        assert result.source == "derived"
        opens = [row.open for row in result.rows]
        assert pytest.approx(99.0) not in opens
        assert pytest.approx(1.0) in opens
        assert pytest.approx(88.0) in opens

    @pytest.mark.asyncio
    async def test_backfill_derived_five_minute(self) -> None:
        """5m short cache backfills from DB with ``source='derived'``.

        Cache holds one derived 5m bar at ``open_at_ms=300_000``
        (built from five 1m bars at indices 5..9 → 300_000 / 360_000 /
        420_000 / 480_000 / 540_000, with the canonical-boundary
        chunker emitting one 5m bar at the first 1m bar's open_at_ms).
        DB returns three older non-overlapping 5m rows so the merged
        chronological slice is ``[db, db, db, cache]`` — exactly the
        requested limit.
        """
        snaps = [_snap((i + 5) * 60_000, 1.0) for i in range(5)]
        cache = _stub_cache(snaps)
        rows = [
            _row(2 * 60_000, 12.0, timeframe="5m"),
            _row(1 * 60_000, 11.0, timeframe="5m"),
            _row(0, 10.0, timeframe="5m"),
        ]
        repo = _stub_repo(rows)
        result = await fetch_candles(
            cache=cache,
            repo=cast(Repository, repo),
            exchange="kraken",
            native_symbol="BTC-USD",
            timeframe="5m",
            limit=4,
        )
        assert result.source == "derived"
        assert result.sample_count == 4
