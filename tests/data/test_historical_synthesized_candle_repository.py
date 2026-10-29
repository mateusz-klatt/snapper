"""Protect historical synthesis against native winners and acknowledgement ambiguity."""

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import cast

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Candle
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import CandleUpsertRow

_OPEN = datetime(2026, 1, 10, 12, tzinfo=UTC)
_AUTHOR = datetime(2026, 7, 1, tzinfo=UTC)


def _row(offset: int = 0) -> CandleUpsertRow:
    """Build one complete canonical higher-timeframe candidate."""
    return {
        "instrument_public_id": "00000000-0000-4000-8000-000000000501",
        "open_at": _OPEN + timedelta(minutes=5 * offset),
        "timestamp": _AUTHOR,
        "timeframe": "5m",
        "open": 10.0,
        "high": 12.0,
        "low": 9.0,
        "close": 11.0,
        "volume": 3.0,
        "vwap": 10.75,
        "trades": 7,
        "source": "synthesized",
        "complete": True,
        "session_id": "00000000-0000-4000-8000-000000000502",
        "sequence_id": offset + 1,
    }


@pytest_asyncio.fixture
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Provide a disposable file-backed database with independent transactions."""
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'historical-synth.db'}")
    await repo.create_all()
    try:
        yield repo
    finally:
        await repo.engine.dispose()


async def _snapshot(repo: SQLAlchemyRepository) -> list[dict[str, object]]:
    """Read every physical column without relying on identifier uniqueness."""
    async with repo.session() as session:
        result = await session.execute(
            select(Candle.__table__).order_by(Candle.open_at, Candle.timestamp)
        )
        return [dict(record) for record in result.mappings().all()]


@pytest.mark.asyncio
async def test_insert_noop_and_synthesized_revision_preserve_prior_asof_and_logical_id(
    repository: SQLAlchemyRepository,
) -> None:
    """Historical synthesis commits real changes and leaves every earlier projection intact."""
    candidate = _row()
    assert await repository.upsert_historical_synthesized_candles([candidate]) == 1
    before = await _snapshot(repository)
    assert await repository.upsert_historical_synthesized_candles([candidate]) == 0
    assert await _snapshot(repository) == before
    unchanged = _row()
    unchanged["timestamp"] = _AUTHOR + timedelta(hours=1)
    assert await repository.upsert_historical_synthesized_candles([unchanged]) == 0
    assert await _snapshot(repository) == before
    replacement = _row()
    replacement.update({"timestamp": _AUTHOR + timedelta(days=1), "close": 10.5})
    assert await repository.upsert_historical_synthesized_candles([replacement]) == 1
    after = await _snapshot(repository)
    assert len(after) == 2
    assert after[0] == before[0] | {"known_to": replacement["timestamp"]}
    assert after[1]["public_id"] == before[0]["public_id"]
    assert after[1]["close"] == 10.5
    assert after[1]["known_to"] == KNOWN_TO_MAX
    horizon = _AUTHOR + timedelta(hours=1)
    assert [
        r["close"]
        for r in after
        if cast(datetime, r["timestamp"]) <= horizon < cast(datetime, r["known_to"])
    ] == [11.0]
    assert candidate == _row()


@pytest.mark.asyncio
@pytest.mark.parametrize("source", ["native", "calculated"])
async def test_existing_authoritative_source_is_preserved(
    repository: SQLAlchemyRepository, source: str
) -> None:
    """A replay-derived candidate never versions a native or calculated current winner."""
    existing = _row()
    existing["source"] = source
    await repository.upsert_candles([existing])
    before = await _snapshot(repository)
    candidate = _row()
    candidate.update({"timestamp": _AUTHOR + timedelta(days=1), "close": 10.5})
    assert await repository.upsert_historical_synthesized_candles([candidate]) == 0
    assert await _snapshot(repository) == before


@pytest.mark.asyncio
async def test_closed_history_future_and_equal_authors_are_not_resurrected_or_regressed(
    repository: SQLAlchemyRepository,
) -> None:
    """Closed-only anomalies and non-newer corrections cannot create an invalid interval."""
    existing = [_row(offset) for offset in range(3)]
    existing[0].update(
        {"timestamp": _AUTHOR - timedelta(days=2), "known_to": _AUTHOR - timedelta(days=1)}
    )
    existing[1]["timestamp"] = _AUTHOR + timedelta(days=1)
    await repository.upsert_candles(existing)
    before = await _snapshot(repository)
    candidates = [_row(offset) for offset in range(3)]
    for candidate in candidates:
        candidate["close"] = 10.5
    assert await repository.upsert_historical_synthesized_candles(candidates) == 0
    assert await _snapshot(repository) == before


@pytest.mark.asyncio
async def test_concurrent_native_replacement_wins_at_actual_historical_write(
    repository: SQLAlchemyRepository,
) -> None:
    """The write-time source check sees a winner committed after the repair was computed."""
    await repository.upsert_candles([_row()])
    candidate = _row()
    candidate.update({"timestamp": _AUTHOR + timedelta(days=2), "close": 10.5})
    async with repository.session() as live:
        native = _row()
        native.update({"source": "native", "timestamp": _AUTHOR + timedelta(days=1)})
        await repository.upsert_candles([native], session=live)
        await live.flush()
        repair = asyncio.create_task(repository.upsert_historical_synthesized_candles([candidate]))
        await asyncio.sleep(0.05)
        assert not repair.done()
        await live.commit()
    before = await _snapshot(repository)
    assert await repair == 0
    assert await _snapshot(repository) == before
    assert len(before) == 2 and before[-1]["source"] == "native"


@pytest.mark.asyncio
async def test_later_integrity_error_rolls_back_a_staged_scd2_close(
    repository: SQLAlchemyRepository,
) -> None:
    """Failure after a synthesized close cannot commit a partial historical repair."""
    existing = _row()
    protected = _row(2)
    protected.update({"source": "native", "public_id": "00000000-0000-4000-8000-000000000503"})
    await repository.upsert_candles([existing, protected])
    before = await _snapshot(repository)
    first, second = _row(), _row(1)
    first.update({"timestamp": _AUTHOR + timedelta(days=1), "close": 10.5})
    second["public_id"] = protected["public_id"]
    with pytest.raises(IntegrityError):
        await repository.upsert_historical_synthesized_candles([first, second])
    assert await _snapshot(repository) == before


@pytest.mark.asyncio
async def test_commit_ack_interruption_propagates_and_reconciliation_is_idempotent(
    repository: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An interrupted acknowledgement never fabricates a success or duplicates a committed row."""
    original_commit = AsyncSession.commit

    async def commit_then_interrupt(session: AsyncSession) -> None:
        """Commit the real transaction and interrupt only its acknowledgement boundary."""
        await original_commit(session)
        raise asyncio.CancelledError

    with monkeypatch.context() as patch:
        patch.setattr(AsyncSession, "commit", commit_then_interrupt)
        with pytest.raises(asyncio.CancelledError):
            await repository.upsert_historical_synthesized_candles([_row()])
    before = await _snapshot(repository)
    assert len(before) == 1
    assert await repository.upsert_historical_synthesized_candles([_row()]) == 0
    assert await _snapshot(repository) == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"timeframe": "1m"},
        {"timeframe": "3h"},
        {"source": "native"},
        {"complete": False},
        {"open_at": _OPEN + timedelta(minutes=1)},
        {"timestamp": _OPEN},
    ],
)
async def test_invalid_historical_candidate_refuses_batch_before_writes(
    repository: SQLAlchemyRepository, changes: CandleUpsertRow
) -> None:
    """Only complete canonical supported synthesis windows enter the guarded transaction."""
    invalid = _row(1)
    invalid.update(changes)
    with pytest.raises(ValueError):
        await repository.upsert_historical_synthesized_candles([_row(), invalid])
    assert await _snapshot(repository) == []


@pytest.mark.asyncio
async def test_empty_duplicate_and_over_budget_historical_batches(
    repository: SQLAlchemyRepository,
) -> None:
    """Empty work stays empty and ambiguous or unbounded repair batches are refused."""
    assert await repository.upsert_historical_synthesized_candles([]) == 0
    with pytest.raises(ValueError):
        await repository.upsert_historical_synthesized_candles([_row(), _row()])
    with pytest.raises(ValueError):
        await repository.upsert_historical_synthesized_candles(
            [_row(offset) for offset in range(721)]
        )


@pytest.mark.asyncio
async def test_lost_exact_version_closure_aborts_without_inserting_a_successor(
    repository: SQLAlchemyRepository,
) -> None:
    """A database-side refusal of the precise close never becomes a false durable ACK."""
    await repository.upsert_candles([_row()])
    before = await _snapshot(repository)
    async with repository.session() as session:
        await session.execute(
            text(
                "CREATE TRIGGER refuse_candle_close BEFORE UPDATE OF known_to ON candles BEGIN SELECT RAISE(IGNORE); END"
            )
        )
        await session.commit()
    candidate = _row()
    candidate.update({"timestamp": _AUTHOR + timedelta(days=1), "close": 10.5})
    with pytest.raises(RuntimeError, match="exact source-owned version"):
        await repository.upsert_historical_synthesized_candles([candidate])
    assert await _snapshot(repository) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("timeframe", ["5m", "15m", "30m", "1h", "4h", "1d"])
async def test_each_supported_historical_window_preserves_full_fields(
    repository: SQLAlchemyRepository, timeframe: str
) -> None:
    """The guarded writer accepts exactly the canonical synthesis widths used by the caller."""
    candidate = _row()
    candidate["timeframe"] = timeframe
    candidate["open_at"] = _OPEN.replace(hour=0)
    assert await repository.upsert_historical_synthesized_candles([candidate]) == 1
    stored = await _snapshot(repository)
    assert len(stored) == 1
    assert all(stored[0][field] == value for field, value in candidate.items())
