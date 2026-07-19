"""Tests for ``Repository.get_latest_candles_for_instruments``.

The market cache's prewarm replaced ~3000 sequential per-instrument
reads with this batched reader. The predicate must stay byte-equivalent
to ``get_candles`` latest-as-of mode — same ``timeframe`` equality, same
``where_active`` SCD2 pair, same ``as_of`` threading — and the optional
``open_at_floor`` must be a pure optimisation: dropping it can only add
rows, which is what lets prewarm run a floored pass followed by an
unfloored one without ever shrinking cache coverage.
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import pytest

from snapper.data.models import Symbol
from snapper.data.repository import SQLAlchemyRepository
from snapper.infrastructure.symbols.functions import resolve_symbol_public_id

_BASE_TS = datetime(2026, 7, 1, tzinfo=UTC)


async def _instrument(repo: SQLAlchemyRepository, native_symbol: str) -> str:
    """Create one symbol + kraken instrument and return its public id.

    Args:
        repo: Repository to seed.
        native_symbol: Native symbol string to create.

    Returns:
        The instrument public id.
    """
    async with repo.session() as s:
        s.add(
            Symbol(
                native_symbol=native_symbol,
                base=native_symbol.split("-", maxsplit=1)[0],
                quote="USD",
                asset_type="crypto",
                created_at=datetime.now(UTC),
                timestamp=datetime.now(UTC),
                session_id="test-session",
                sequence_id=1,
            )
        )
        await s.commit()
    symbol_public_id = await resolve_symbol_public_id(repo, native_symbol, as_of=datetime.now(UTC))
    assert symbol_public_id is not None
    _, instrument_public_id = await repo.ensure_instrument(
        symbol_public_id=symbol_public_id,
        exchange="kraken",
        session_id="test-session",
        sequence_id=1,
        timestamp=datetime.now(UTC),
    )
    return instrument_public_id


def _candle_rows(
    instrument_public_id: str,
    count: int,
    *,
    timeframe: str = "1m",
    first_open_at: datetime = _BASE_TS,
) -> list[dict[str, object]]:
    """Build ``count`` consecutive one-minute candle rows for upsert.

    Args:
        instrument_public_id: Owning instrument.
        count: Number of rows to build.
        timeframe: Candle timeframe to stamp.
        first_open_at: ``open_at`` of the earliest row.

    Returns:
        Rows ready for ``upsert_candles``.
    """
    return [
        {
            "instrument_public_id": instrument_public_id,
            "timeframe": timeframe,
            "open_at": first_open_at + timedelta(minutes=index),
            "timestamp": first_open_at + timedelta(minutes=index),
            "open": 10.0 + index,
            "high": 11.0 + index,
            "low": 9.0 + index,
            "close": 10.5 + index,
            "volume": 5.0,
            "vwap": 10.4 + index,
            "trades": 3,
            "session_id": "test-session",
            "sequence_id": index + 1,
            "complete": True,
        }
        for index in range(count)
    ]


async def _repo(tmp_path: Path) -> SQLAlchemyRepository:
    """Create an empty SQLite repository.

    Args:
        tmp_path: Per-test temporary directory.

    Returns:
        The created repository.
    """
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'candles.db'}")
    await repo.create_all()
    return repo


@pytest.mark.asyncio
async def test_empty_input_returns_empty_mapping(tmp_path: Path) -> None:
    """Verify an empty instrument list short-circuits before any query.

    Given: No instrument public ids,
    When: get_latest_candles_for_instruments runs,
    Then: An empty mapping returns.
    """
    repo = await _repo(tmp_path)

    result = await repo.get_latest_candles_for_instruments([], "1m", datetime.now(UTC), 100)

    assert result == {}


@pytest.mark.asyncio
async def test_respects_limit_and_returns_descending_rows(tmp_path: Path) -> None:
    """Verify the per-instrument cap and DESC ordering match get_candles.

    Given: One instrument holding 150 one-minute bars,
    When: the batch reader runs with limit 100,
    Then: Exactly the newest 100 rows return, newest first, fully projected.
    """
    repo = await _repo(tmp_path)
    instrument_public_id = await _instrument(repo, "BTC-USD")
    await repo.upsert_candles(_candle_rows(instrument_public_id, 150))

    result = await repo.get_latest_candles_for_instruments(
        [instrument_public_id], "1m", datetime.now(UTC), 100
    )

    rows = result[instrument_public_id]
    assert len(rows) == 100
    assert [row["open_at"] for row in rows] == sorted(
        (row["open_at"] for row in rows), reverse=True
    )
    assert rows[0]["open_at"] == _BASE_TS + timedelta(minutes=149)
    assert set(rows[0]) == {
        "open_at",
        "timeframe",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "vwap",
        "trades",
        "source",
        "complete",
        "public_id",
        "timestamp",
        "session_id",
        "sequence_id",
    }


@pytest.mark.asyncio
async def test_instrument_without_matching_timeframe_is_omitted(tmp_path: Path) -> None:
    """Verify the timeframe predicate holds and misses are omitted, not empty.

    Given: An instrument holding only 1d bars,
    When: the batch reader asks for 1m,
    Then: The instrument is absent from the mapping rather than mapped to [].
    """
    repo = await _repo(tmp_path)
    instrument_public_id = await _instrument(repo, "ETH-USD")
    await repo.upsert_candles(_candle_rows(instrument_public_id, 3, timeframe="1d"))

    result = await repo.get_latest_candles_for_instruments(
        [instrument_public_id], "1m", datetime.now(UTC), 100
    )

    assert instrument_public_id not in result


@pytest.mark.asyncio
async def test_floor_excludes_rows_then_unfloored_read_returns_them(tmp_path: Path) -> None:
    """Verify the floor bounds the read and dropping it is a strict superset.

    Given: An instrument whose only bars predate the floor,
    When: the batch reader runs floored and then unfloored,
    Then: The floored read omits it and the unfloored read returns its rows,
        which is the property prewarm's two-pass design depends on.
    """
    repo = await _repo(tmp_path)
    instrument_public_id = await _instrument(repo, "SOL-USD")
    await repo.upsert_candles(_candle_rows(instrument_public_id, 5))
    as_of = datetime.now(UTC)

    floored = await repo.get_latest_candles_for_instruments(
        [instrument_public_id], "1m", as_of, 100, _BASE_TS + timedelta(days=365)
    )
    unfloored = await repo.get_latest_candles_for_instruments(
        [instrument_public_id], "1m", as_of, 100, None
    )

    assert instrument_public_id not in floored
    assert len(unfloored[instrument_public_id]) == 5


@pytest.mark.asyncio
async def test_superseded_versions_are_excluded_at_both_as_of_points(tmp_path: Path) -> None:
    """Verify SCD2 threading selects one version per bar at any as_of.

    Given: One bar upserted twice, so the first version is closed,
    When: the batch reader runs at a current as_of,
    Then: Only the successor returns, and a historical as_of taken before
        the supersede still returns the original version.
    """
    repo = await _repo(tmp_path)
    instrument_public_id = await _instrument(repo, "ADA-USD")
    first = _candle_rows(instrument_public_id, 1)
    await repo.upsert_candles(first)
    before_supersede = datetime.now(UTC)
    second = _candle_rows(instrument_public_id, 1)
    second[0]["close"] = 999.0
    second[0]["timestamp"] = datetime.now(UTC)
    await repo.upsert_candles(second)

    current = await repo.get_latest_candles_for_instruments(
        [instrument_public_id], "1m", datetime.now(UTC), 100
    )
    historical = await repo.get_latest_candles_for_instruments(
        [instrument_public_id], "1m", before_supersede, 100
    )

    assert [row["close"] for row in current[instrument_public_id]] == [999.0]
    assert [row["close"] for row in historical[instrument_public_id]] == [10.5]


@pytest.mark.asyncio
async def test_unknown_instrument_does_not_corrupt_the_batch(tmp_path: Path) -> None:
    """Verify one unknown id leaves the real instruments' rows intact.

    Given: A batch mixing a seeded instrument with a well-formed but
        unused instrument id,
    When: the batch reader runs,
    Then: The seeded instrument returns its rows and the unused id is absent.

    The unknown id must be a syntactically valid UUID: ``instrument_public_id``
    is a ``UUIDColumn``, so Postgres rejects a malformed literal before
    executing the statement while SQLite accepts it, and an arbitrary
    string here would pass under the test fixture yet fail in production.
    """
    repo = await _repo(tmp_path)
    instrument_public_id = await _instrument(repo, "DOT-USD")
    await repo.upsert_candles(_candle_rows(instrument_public_id, 4))
    unused_public_id = "00000000-0000-7000-8000-0000000000ff"

    result = await repo.get_latest_candles_for_instruments(
        [instrument_public_id, unused_public_id], "1m", datetime.now(UTC), 100
    )

    assert len(result[instrument_public_id]) == 4
    assert unused_public_id not in result
