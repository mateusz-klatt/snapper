"""Tests for the ``complete`` predicate on ``Repository.get_candles``.

P2 warmup reads latest-N COMPLETE bars; the predicate must exclude
provisional intermediate persists exactly and keep the legacy
unfiltered read byte-identical when the parameter is omitted.
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


async def _seeded_repo(tmp_path: Path) -> tuple[SQLAlchemyRepository, str]:
    """Create a SQLite repo with BTC-USD@kraken and three 1h candles.

    The newest candle is provisional (``complete=False``); the two older
    ones are final.

    Args:
        tmp_path: Per-test temporary directory.

    Returns:
        Tuple of repository and instrument public id.
    """
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'candles.db'}")
    await repo.create_all()
    async with repo.session() as s:
        s.add(
            Symbol(
                native_symbol="BTC-USD",
                base="BTC",
                quote="USD",
                asset_type="crypto",
                created_at=datetime.now(UTC),
                timestamp=datetime.now(UTC),
                session_id="test-session",
                sequence_id=1,
            )
        )
        await s.commit()
    spid = await resolve_symbol_public_id(repo, "BTC-USD", as_of=datetime.now(UTC))
    assert spid is not None
    _, instrument_public_id = await repo.ensure_instrument(
        symbol_public_id=spid,
        exchange="kraken",
        session_id="test-session",
        sequence_id=1,
        timestamp=datetime.now(UTC),
    )
    rows = [
        {
            "instrument_public_id": instrument_public_id,
            "timeframe": "1h",
            "open_at": _BASE_TS + timedelta(hours=i),
            "timestamp": _BASE_TS + timedelta(hours=i),
            "open": 10.0 + i,
            "high": 11.0 + i,
            "low": 9.0 + i,
            "close": 10.5 + i,
            "volume": 5.0,
            "vwap": 10.4 + i,
            "trades": 3,
            "session_id": "test-session",
            "sequence_id": i + 1,
            "complete": i < 2,
        }
        for i in range(3)
    ]
    inserted = await repo.upsert_candles(rows)
    assert inserted == 3
    return repo, instrument_public_id


@pytest.mark.asyncio
async def test_complete_true_excludes_provisional_rows(tmp_path: Path) -> None:
    """Verify complete=True returns only final bars in latest-N mode.

    Given: Two final bars and one provisional newest bar,
    When: get_candles runs with complete=True, limit=2, order desc,
    Then: Exactly the two final bars return (provisional excluded).
    """
    repo, _ = await _seeded_repo(tmp_path)
    rows = await repo.get_candles(
        "BTC-USD",
        "1h",
        None,
        None,
        exchange="kraken",
        as_of=datetime.now(UTC),
        limit=2,
        order="desc",
        complete=True,
    )
    assert [r["open_at"] for r in rows] == [
        _BASE_TS + timedelta(hours=1),
        _BASE_TS,
    ]
    assert all(r["complete"] for r in rows)


@pytest.mark.asyncio
async def test_complete_none_keeps_legacy_unfiltered_read(tmp_path: Path) -> None:
    """Verify the omitted parameter keeps every row visible.

    Given: Two final bars and one provisional newest bar,
    When: get_candles runs without the complete parameter,
    Then: All three rows return, provisional included.
    """
    repo, _ = await _seeded_repo(tmp_path)
    rows = await repo.get_candles(
        "BTC-USD",
        "1h",
        None,
        None,
        exchange="kraken",
        as_of=datetime.now(UTC),
        limit=3,
        order="desc",
    )
    assert len(rows) == 3
    assert [bool(r["complete"]) for r in rows] == [False, True, True]


@pytest.mark.asyncio
async def test_complete_false_returns_only_provisional_rows(tmp_path: Path) -> None:
    """Verify complete=False selects exactly the provisional rows.

    Given: Two final bars and one provisional newest bar,
    When: get_candles runs with complete=False,
    Then: Only the provisional bar returns.
    """
    repo, _ = await _seeded_repo(tmp_path)
    rows = await repo.get_candles(
        "BTC-USD",
        "1h",
        None,
        None,
        exchange="kraken",
        as_of=datetime.now(UTC),
        limit=3,
        order="desc",
        complete=False,
    )
    assert [r["open_at"] for r in rows] == [_BASE_TS + timedelta(hours=2)]
