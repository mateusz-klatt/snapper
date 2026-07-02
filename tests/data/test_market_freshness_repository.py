"""Tests for :meth:`SQLAlchemyRepository.get_latest_candle_open_at_by_exchange`.

Pins the per-exchange newest-candle contract consumed by the
market-data watchdog:

* one row per requested exchange with >=1 ACTIVE instrument, ordered
  by exchange;
* ``latest_open_at`` = maximum ``open_at`` across the exchange's
  active instruments, any timeframe, returned timezone-aware (the
  SQLite fixture round-trips through :class:`TZDateTime`, which is
  exactly the dialect the watchdog's silence arithmetic must survive);
* exchanges outside the requested set and instruments that are
  bitemporally closed at the reference instant are excluded;
* an exchange whose active instruments have no candle rows reports
  ``latest_open_at is None``.

A fixed reference ``now`` is injected into every call so the
active-instrument boundary assertions are deterministic.
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Candle
from snapper.data.models import Instrument
from snapper.data.repository import SQLAlchemyRepository

_NOW = datetime(2026, 7, 2, 12, 0, tzinfo=UTC)
_PAST = _NOW - timedelta(hours=1)


@pytest.fixture
async def _repo() -> SQLAlchemyRepository:
    """Async fixture yielding a fresh in-memory aiosqlite repository."""
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    await repo.create_all()
    return repo


async def _add_instrument(
    repo: SQLAlchemyRepository,
    *,
    public_id: str,
    exchange: str,
    known_to: datetime = KNOWN_TO_MAX,
    timestamp: datetime = _PAST,
) -> None:
    """Insert one instrument row (active unless ``known_to`` is closed)."""
    async with repo.session() as s:
        s.add(
            Instrument(
                public_id=public_id,
                symbol_public_id=f"sym-{public_id}",
                exchange=exchange,
                session_id="s-1",
                sequence_id=1,
                timestamp=timestamp,
                known_to=known_to,
            )
        )
        await s.commit()


async def _add_candle(
    repo: SQLAlchemyRepository,
    *,
    public_id: str,
    instrument_public_id: str,
    open_at: datetime,
    timeframe: str = "1m",
) -> None:
    """Insert one candle row opening at ``open_at`` for the instrument."""
    async with repo.session() as s:
        s.add(
            Candle(
                public_id=public_id,
                instrument_public_id=instrument_public_id,
                open_at=open_at,
                timeframe=timeframe,
                open=1.0,
                high=1.2,
                low=0.9,
                close=1.1,
                volume=5.0,
                vwap=None,
                trades=None,
                session_id="s-1",
                sequence_id=1,
                timestamp=_PAST,
            )
        )
        await s.commit()


class TestLatestCandleOpenAtByExchange:
    """Contract of the per-exchange newest-candle freshness query."""

    @pytest.mark.asyncio
    async def test_returns_max_open_at_per_exchange_tz_aware(
        self, _repo: SQLAlchemyRepository
    ) -> None:
        """Newest candle wins per exchange and comes back timezone-aware.

        Given: Two kraken instruments with candles at different ages and
            a walutomat instrument with one candle,
        When: The freshness query runs over both exchanges,
        Then: Each exchange reports its maximum ``open_at`` as an aware
            UTC datetime, ordered by exchange name.
        """
        await _add_instrument(_repo, public_id="i-k1", exchange="kraken")
        await _add_instrument(_repo, public_id="i-k2", exchange="kraken")
        await _add_instrument(_repo, public_id="i-w1", exchange="walutomat")
        newest = _NOW - timedelta(minutes=2)
        await _add_candle(
            _repo, public_id="c-1", instrument_public_id="i-k1", open_at=_NOW - timedelta(hours=3)
        )
        await _add_candle(_repo, public_id="c-2", instrument_public_id="i-k2", open_at=newest)
        await _add_candle(
            _repo,
            public_id="c-3",
            instrument_public_id="i-w1",
            open_at=_NOW - timedelta(minutes=30),
        )
        rows = await _repo.get_latest_candle_open_at_by_exchange(
            exchanges=("kraken", "walutomat"), now=_NOW
        )
        assert [row["exchange"] for row in rows] == ["kraken", "walutomat"]
        kraken_latest = rows[0]["latest_open_at"]
        assert kraken_latest == newest
        assert kraken_latest is not None
        assert kraken_latest.tzinfo is not None
        walutomat_latest = rows[1]["latest_open_at"]
        assert walutomat_latest == _NOW - timedelta(minutes=30)

    @pytest.mark.asyncio
    async def test_any_timeframe_counts_toward_the_maximum(
        self, _repo: SQLAlchemyRepository
    ) -> None:
        """The maximum spans all timeframes, not only 1m.

        Given: One instrument whose newest candle row is a 5m candle,
        When: The freshness query runs,
        Then: The 5m candle's ``open_at`` is reported.
        """
        await _add_instrument(_repo, public_id="i-k1", exchange="kraken")
        await _add_candle(
            _repo,
            public_id="c-1m",
            instrument_public_id="i-k1",
            open_at=_NOW - timedelta(minutes=10),
        )
        await _add_candle(
            _repo,
            public_id="c-5m",
            instrument_public_id="i-k1",
            open_at=_NOW - timedelta(minutes=5),
            timeframe="5m",
        )
        rows = await _repo.get_latest_candle_open_at_by_exchange(exchanges=("kraken",), now=_NOW)
        assert rows[0]["latest_open_at"] == _NOW - timedelta(minutes=5)

    @pytest.mark.asyncio
    async def test_excludes_unrequested_exchanges_and_closed_instruments(
        self, _repo: SQLAlchemyRepository
    ) -> None:
        """Only requested exchanges and active instruments participate.

        Given: A polygon instrument with the newest candle overall, and
            a kraken exchange whose ONLY fresh candle hangs off a
            bitemporally closed instrument row,
        When: The freshness query runs for kraken alone,
        Then: Polygon produces no row and the closed instrument's fresh
            candle does not advance kraken's maximum.
        """
        await _add_instrument(_repo, public_id="i-p1", exchange="polygon")
        await _add_candle(
            _repo,
            public_id="c-p",
            instrument_public_id="i-p1",
            open_at=_NOW - timedelta(seconds=30),
        )
        await _add_instrument(_repo, public_id="i-k-live", exchange="kraken")
        await _add_instrument(
            _repo, public_id="i-k-closed", exchange="kraken", known_to=_NOW - timedelta(days=1)
        )
        await _add_candle(
            _repo,
            public_id="c-old",
            instrument_public_id="i-k-live",
            open_at=_NOW - timedelta(hours=2),
        )
        await _add_candle(
            _repo,
            public_id="c-fresh-on-closed",
            instrument_public_id="i-k-closed",
            open_at=_NOW - timedelta(minutes=1),
        )
        rows = await _repo.get_latest_candle_open_at_by_exchange(exchanges=("kraken",), now=_NOW)
        assert [row["exchange"] for row in rows] == ["kraken"]
        assert rows[0]["latest_open_at"] == _NOW - timedelta(hours=2)
        no_instruments = await _repo.get_latest_candle_open_at_by_exchange(
            exchanges=("kraken_futures",), now=_NOW
        )
        assert no_instruments == []

    @pytest.mark.asyncio
    async def test_exchange_without_candles_reports_none(self, _repo: SQLAlchemyRepository) -> None:
        """An active-but-candleless exchange reports ``None``.

        Given: A kraken_futures instrument with zero candle rows,
        When: The freshness query runs,
        Then: The exchange row is present with ``latest_open_at`` None.
        """
        await _add_instrument(_repo, public_id="i-f1", exchange="kraken_futures")
        rows = await _repo.get_latest_candle_open_at_by_exchange(
            exchanges=("kraken_futures",), now=_NOW
        )
        assert rows == [{"exchange": "kraken_futures", "latest_open_at": None}]

    @pytest.mark.asyncio
    async def test_defaults_reference_to_wall_clock_now(self, _repo: SQLAlchemyRepository) -> None:
        """Omitting ``now`` evaluates the active predicate at wall clock.

        Given: One currently-active instrument with one candle, its
            ``timestamp`` far in the past so ``timestamp <= now`` holds
            at any real wall-clock instant,
        When: The freshness query runs without an injected ``now``,
        Then: The exchange row is returned (the sentinel ``known_to``
            is active at any real wall-clock instant).
        """
        await _add_instrument(
            _repo,
            public_id="i-k1",
            exchange="kraken",
            timestamp=datetime(2020, 1, 1, tzinfo=UTC),
        )
        await _add_candle(
            _repo,
            public_id="c-1",
            instrument_public_id="i-k1",
            open_at=_NOW - timedelta(minutes=1),
        )
        rows = await _repo.get_latest_candle_open_at_by_exchange(exchanges=("kraken",))
        assert [row["exchange"] for row in rows] == ["kraken"]
