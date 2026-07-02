"""Tests for the split-repair repository methods.

Pins :meth:`SQLAlchemyRepository.list_instrument_symbols` (active
instrument + symbol join, exchange filter, ordering) and
:meth:`SQLAlchemyRepository.supersede_current_candles` (SCD2 close of
current rows scoped to one instrument + timeframe, idempotent second
call, rows preserved rather than deleted).
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest
from sqlalchemy import func
from sqlalchemy import select

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Candle
from snapper.data.models import Instrument
from snapper.data.models import Symbol
from snapper.data.repository import SQLAlchemyRepository

_NOW = datetime(2026, 7, 2, 12, 0, tzinfo=UTC)
_PAST = _NOW - timedelta(days=2)


@pytest.fixture
async def _repo() -> SQLAlchemyRepository:
    """Async fixture yielding a fresh in-memory aiosqlite repository."""
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    await repo.create_all()
    return repo


async def _add_symbol(
    repo: SQLAlchemyRepository,
    *,
    public_id: str,
    native_symbol: str,
    known_to: datetime = KNOWN_TO_MAX,
) -> None:
    """Insert one symbol row."""
    async with repo.session() as s:
        s.add(
            Symbol(
                public_id=public_id,
                native_symbol=native_symbol,
                base=native_symbol,
                quote=None,
                asset_type="equity",
                created_at=_PAST,
                session_id="s-1",
                sequence_id=1,
                timestamp=_PAST,
                known_to=known_to,
            )
        )
        await s.commit()


async def _add_instrument(
    repo: SQLAlchemyRepository,
    *,
    public_id: str,
    symbol_public_id: str,
    exchange: str,
    known_to: datetime = KNOWN_TO_MAX,
) -> None:
    """Insert one instrument row."""
    async with repo.session() as s:
        s.add(
            Instrument(
                public_id=public_id,
                symbol_public_id=symbol_public_id,
                exchange=exchange,
                session_id="s-1",
                sequence_id=1,
                timestamp=_PAST,
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
    known_to: datetime = KNOWN_TO_MAX,
) -> None:
    """Insert one candle row."""
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
                known_to=known_to,
            )
        )
        await s.commit()


class TestListInstrumentSymbols:
    """Active instrument + symbol listing for one exchange."""

    @pytest.mark.asyncio
    async def test_lists_active_rows_ordered_by_native_symbol(
        self, _repo: SQLAlchemyRepository
    ) -> None:
        """Active rows come back ordered; other exchanges and closed rows drop.

        Given: Two active polygon instruments (inserted out of order), a
            kraken instrument, and a polygon instrument whose symbol row
            is bitemporally closed,
        When: ``list_instrument_symbols`` runs for polygon,
        Then: Only the two active polygon rows return, ordered by
            native symbol, carrying both public ids.
        """
        await _add_symbol(_repo, public_id="sym-z", native_symbol="ZZT")
        await _add_symbol(_repo, public_id="sym-a", native_symbol="AAA")
        await _add_symbol(_repo, public_id="sym-k", native_symbol="KRK")
        await _add_symbol(
            _repo, public_id="sym-old", native_symbol="OLD", known_to=_PAST + timedelta(hours=1)
        )
        await _add_instrument(_repo, public_id="i-z", symbol_public_id="sym-z", exchange="polygon")
        await _add_instrument(_repo, public_id="i-a", symbol_public_id="sym-a", exchange="polygon")
        await _add_instrument(_repo, public_id="i-k", symbol_public_id="sym-k", exchange="kraken")
        await _add_instrument(
            _repo, public_id="i-old", symbol_public_id="sym-old", exchange="polygon"
        )
        rows = await _repo.list_instrument_symbols(exchange="polygon", now=_NOW)
        assert rows == [
            {
                "native_symbol": "AAA",
                "instrument_public_id": "i-a",
                "symbol_public_id": "sym-a",
            },
            {
                "native_symbol": "ZZT",
                "instrument_public_id": "i-z",
                "symbol_public_id": "sym-z",
            },
        ]

    @pytest.mark.asyncio
    async def test_wall_clock_default_reference(self, _repo: SQLAlchemyRepository) -> None:
        """Omitting ``now`` evaluates active rows at wall clock.

        Given: One active polygon instrument whose rows use a past
            timestamp,
        When: ``list_instrument_symbols`` runs without ``now``,
        Then: The row is returned.
        """
        await _add_symbol(_repo, public_id="sym-a", native_symbol="AAA")
        await _add_instrument(_repo, public_id="i-a", symbol_public_id="sym-a", exchange="polygon")
        rows = await _repo.list_instrument_symbols(exchange="polygon")
        assert [r["native_symbol"] for r in rows] == ["AAA"]


class TestSupersedeCurrentCandles:
    """SCD2 close of current candle rows per instrument + timeframe."""

    @pytest.mark.asyncio
    async def test_supersedes_only_matching_current_rows(self, _repo: SQLAlchemyRepository) -> None:
        """Scope is instrument + timeframe + open sentinel; rows survive closed.

        Given: Current 1m and 1d rows for the target instrument, a 1m
            row for another instrument, and an already-closed 1m row,
        When: ``supersede_current_candles`` runs for the target's 1m,
        Then: Exactly the two current target 1m rows close (row count
            preserved — nothing is deleted), the 1d and foreign rows
            stay current, and a repeat call supersedes nothing.
        """
        await _add_candle(
            _repo, public_id="c-1", instrument_public_id="i-x", open_at=_NOW - timedelta(hours=1)
        )
        await _add_candle(
            _repo, public_id="c-2", instrument_public_id="i-x", open_at=_NOW - timedelta(hours=2)
        )
        await _add_candle(
            _repo,
            public_id="c-1d",
            instrument_public_id="i-x",
            open_at=_NOW - timedelta(days=1),
            timeframe="1d",
        )
        await _add_candle(
            _repo,
            public_id="c-other",
            instrument_public_id="i-y",
            open_at=_NOW - timedelta(hours=1),
        )
        await _add_candle(
            _repo,
            public_id="c-closed",
            instrument_public_id="i-x",
            open_at=_NOW - timedelta(hours=3),
            known_to=_PAST + timedelta(hours=1),
        )
        superseded_at = _NOW - timedelta(minutes=5)
        count = await _repo.supersede_current_candles(
            instrument_public_id="i-x", timeframe="1m", superseded_at=superseded_at
        )
        assert count == 2
        async with _repo.session() as s:
            total = await s.scalar(select(func.count()).select_from(Candle))
            current = await s.scalar(
                select(func.count()).select_from(Candle).where(Candle.known_to == KNOWN_TO_MAX)
            )
        assert total == 5
        assert current == 2
        repeat = await _repo.supersede_current_candles(
            instrument_public_id="i-x", timeframe="1m", superseded_at=superseded_at
        )
        assert repeat == 0

    @pytest.mark.asyncio
    async def test_wall_clock_default_close_instant(self, _repo: SQLAlchemyRepository) -> None:
        """Omitting ``superseded_at`` closes rows at wall clock.

        Given: One current 1m row,
        When: ``supersede_current_candles`` runs without an explicit
            close instant,
        Then: The row is superseded.
        """
        await _add_candle(
            _repo, public_id="c-1", instrument_public_id="i-x", open_at=_NOW - timedelta(hours=1)
        )
        count = await _repo.supersede_current_candles(instrument_public_id="i-x", timeframe="1m")
        assert count == 1
