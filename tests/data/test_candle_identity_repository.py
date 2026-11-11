"""Tests for streaming the stored identities of active candle versions."""

from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import pytest
import pytest_asyncio

from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import CandleIdentityRow
from snapper.data.repository_types import CandleUpsertRow

_INSTRUMENT = "00000000-0000-4000-8000-0000000d0001"
_SESSION = "00000000-0000-4000-8000-0000000d0002"
_START = datetime(2026, 11, 12, 23, 0, tzinfo=UTC)


def _row(open_at: datetime, public_id: str, close: float = 11.0) -> CandleUpsertRow:
    """Build one complete native 1m row authored after its minute closed.

    Args:
        open_at: Candle opening.
        public_id: Public id the row carries.
        close: Close price, varied to create a new version of the same key.

    Returns:
        A row ready for :meth:`SQLAlchemyRepository.upsert_candles`.
    """
    return {
        "instrument_public_id": _INSTRUMENT,
        "open_at": open_at,
        "timestamp": open_at + timedelta(minutes=1, seconds=close),
        "timeframe": "1m",
        "open": 10.0,
        "high": 12.0,
        "low": 9.0,
        "close": close,
        "volume": 3.0,
        "vwap": 10.75,
        "trades": 7,
        "source": "native",
        "complete": True,
        "price_basis": "last",
        "session_id": _SESSION,
        "sequence_id": 1,
        "public_id": public_id,
    }


@pytest_asyncio.fixture
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Provide a disposable file-backed repository for real reads.

    Args:
        tmp_path: Per-test directory.

    Yields:
        A repository with the full schema.
    """
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'identities.db'}")
    await repo.create_all()
    try:
        yield repo
    finally:
        await repo.engine.dispose()


async def _collect(
    repository: SQLAlchemyRepository, start: datetime, end: datetime
) -> list[CandleIdentityRow]:
    """Drain the identity stream of one range.

    Args:
        repository: Repository under test.
        start: Inclusive lower bound on the opening.
        end: Exclusive upper bound on the opening.

    Returns:
        Every streamed row, in stream order.
    """
    return [row async for row in repository.iter_active_candle_identities(start, end)]


@pytest.mark.asyncio
async def test_streams_the_active_versions_opening_in_a_half_open_range(
    repository: SQLAlchemyRepository,
) -> None:
    """Only active versions opening in ``[start, end)`` are streamed, across days.

    Given: Keys one minute before the range, at its start, across the UTC day
        boundary, one minute before its end and at its end, with the start key
        versioned twice.
    When: The range is streamed.
    Then: Exactly the keys inside it are returned, each once with its active id,
        in opening order.
    """
    end = _START + timedelta(hours=2)
    openings = [
        _START - timedelta(minutes=1),
        _START,
        _START + timedelta(hours=1),
        end - timedelta(minutes=1),
        end,
    ]
    await repository.upsert_candles(
        [
            _row(opening, f"00000000-0000-4000-8000-0000000e{index:04d}")
            for index, opening in enumerate(openings)
        ]
    )
    await repository.upsert_candles([_row(_START, "00000000-0000-4000-8000-0000000effff", 11.5)])

    streamed = await _collect(repository, _START, end)

    assert streamed == [
        {
            "instrument_public_id": _INSTRUMENT,
            "timeframe": "1m",
            "open_at": opening,
            "public_id": f"00000000-0000-4000-8000-0000000e{index:04d}",
        }
        for index, opening in enumerate(openings)
        if _START <= opening < end
    ]


@pytest.mark.asyncio
async def test_an_empty_range_streams_nothing(repository: SQLAlchemyRepository) -> None:
    """A range whose end is not after its start reads no day at all.

    Given: A stored key at the range's start.
    When: A range from that instant to itself is streamed.
    Then: Nothing is returned.
    """
    await repository.upsert_candles([_row(_START, "00000000-0000-4000-8000-0000000e0001")])

    assert await _collect(repository, _START, _START) == []


@pytest.mark.asyncio
async def test_a_naive_bound_is_refused(repository: SQLAlchemyRepository) -> None:
    """A bound without a time zone cannot name an instant.

    Given: A naive start.
    When: The range is streamed.
    Then: A ValueError is raised before any read.
    """
    with pytest.raises(ValueError):
        await _collect(repository, _START.replace(tzinfo=None), _START + timedelta(hours=1))
