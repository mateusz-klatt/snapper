"""Verify native trade evidence corrects synthetic zeros without losing knowledge history."""

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import cast

import pytest
import pytest_asyncio
from sqlalchemy import event
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Candle
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import CandleUpsertRow
from snapper.data.repository_types import CandleVersionRow

_OPEN = datetime(2026, 1, 10, 12, tzinfo=UTC)
_AUTHOR = datetime(2026, 7, 1, tzinfo=UTC)


def _native(offset: int = 0) -> CandleUpsertRow:
    """Build an exact closed native minute with observed trade count and acquisition time."""
    return {
        "instrument_public_id": "00000000-0000-4000-8000-000000000701",
        "open_at": _OPEN + timedelta(minutes=offset),
        "timestamp": _AUTHOR,
        "timeframe": "1m",
        "open": 10.0,
        "high": 12.0,
        "low": 9.0,
        "close": 11.0,
        "volume": 3.0,
        "vwap": 10.75,
        "trades": 7,
        "source": "native",
        "complete": True,
        "price_basis": "last",
        "session_id": "00000000-0000-4000-8000-000000000702",
        "sequence_id": offset + 1,
    }


def _zero(offset: int = 0) -> CandleUpsertRow:
    """Build an earlier synthetic flat carry without trade evidence."""
    return _native(offset) | {
        "timestamp": _AUTHOR - timedelta(hours=1),
        "source": "synthesized",
        "open": 10.0,
        "high": 10.0,
        "low": 10.0,
        "close": 10.0,
        "volume": 0.0,
        "vwap": 10.0,
        "trades": 0,
    }


@pytest_asyncio.fixture
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Provide a real disposable file-backed candle repository."""
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'native-reconciliation.db'}")
    await repo.create_all()
    try:
        yield repo
    finally:
        await repo.engine.dispose()


async def _snapshot(repo: SQLAlchemyRepository) -> list[dict[str, object]]:
    """Read all physical columns without conflating version identity and row identity."""
    async with repo.session() as session:
        result = await session.execute(
            select(Candle.__table__).order_by(Candle.open_at, Candle.timestamp)
        )
        return [dict(record) for record in result.mappings().all()]


@pytest.mark.asyncio
async def test_native_trades_correct_zero_and_preserve_bitemporal_history_and_identity(
    repository: SQLAlchemyRepository,
) -> None:
    """Exact native trade evidence versions a false zero while prior as-of reads keep it."""
    await repository.upsert_candles([_zero()])
    before = await _snapshot(repository)
    native = _native()
    original = native.copy()

    changed = await repository.reconcile_native_candles([native])

    assert native == original
    assert len(changed) == 1
    after = await _snapshot(repository)
    assert len(after) == 2
    assert after[0] == before[0] | {"known_to": _AUTHOR}
    assert after[1]["public_id"] == before[0]["public_id"] == changed[0]["public_id"]
    assert all(after[1][field] == value for field, value in native.items())
    assert after[1]["known_to"] == KNOWN_TO_MAX
    horizon = _AUTHOR - timedelta(minutes=1)
    assert [
        row["trades"]
        for row in after
        if cast(datetime, row["timestamp"]) <= horizon < cast(datetime, row["known_to"])
    ] == [0]
    assert await repository.reconcile_native_candles([native]) == []
    assert await _snapshot(repository) == after


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"source": "native"},
        {"source": "calculated"},
        {"volume": 0.1},
        {"trades": 1},
        {"trades": None},
        {"high": 11.0},
        {"timestamp": _AUTHOR},
        {"timestamp": _AUTHOR + timedelta(hours=1)},
    ],
)
async def test_protected_current_values_or_provenance_are_untouched(
    repository: SQLAlchemyRepository, changes: CandleUpsertRow
) -> None:
    """Native, trade-bearing, unknown, non-flat and non-older versions cannot be replaced."""
    stored = _zero() | changes
    await repository.upsert_candles([stored])
    before = await _snapshot(repository)
    assert await repository.reconcile_native_candles([_native()]) == []
    assert await _snapshot(repository) == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"source": "native", "volume": 0.0, "trades": 0},
        {"source": "calculated", "volume": 0.0, "trades": 0},
        {"source": "synthesized", "volume": 0.1, "trades": 0},
        {"source": "synthesized", "volume": 0.0, "trades": 1},
        {"source": "synthesized", "volume": 0.0, "trades": None},
    ],
)
async def test_historical_real_or_unknown_trade_provenance_protects_current_zero(
    repository: SQLAlchemyRepository, changes: CandleUpsertRow
) -> None:
    """A later synthetic zero never erases evidence that the minute already had trades."""
    prior = _zero() | changes | {"timestamp": _AUTHOR - timedelta(hours=2)}
    await repository.upsert_candles([prior])
    await repository.upsert_candles([_zero()])
    before = await _snapshot(repository)
    assert await repository.reconcile_native_candles([_native()]) == []
    assert await _snapshot(repository) == before


@pytest.mark.asyncio
async def test_no_trade_response_and_unavailable_response_leave_synthetic_zero(
    repository: SQLAlchemyRepository,
) -> None:
    """A native no-trade observation or no acquired rows provides no replacement evidence."""
    await repository.upsert_candles([_zero()])
    before = await _snapshot(repository)
    assert await repository.reconcile_native_candles([_native() | {"trades": 0}]) == []
    assert await repository.reconcile_native_candles([]) == []
    assert await _snapshot(repository) == before


@pytest.mark.asyncio
async def test_explicit_trade_count_is_evidence_even_if_volume_rounds_to_zero(
    repository: SQLAlchemyRepository,
) -> None:
    """The native trade count is authoritative even where volume has finite precision."""
    await repository.upsert_candles([_zero()])
    changed = await repository.reconcile_native_candles([_native() | {"volume": 0.0}])
    assert len(changed) == 1
    assert changed[0]["trades"] == 7 and changed[0]["volume"] == 0


@pytest.mark.asyncio
async def test_absent_key_inserts_but_closed_only_history_is_not_resurrected(
    repository: SQLAlchemyRepository,
) -> None:
    """All-history absence proof remains necessary for an insertion without a current row."""
    closed = _zero() | {"known_to": _AUTHOR - timedelta(minutes=30)}
    await repository.upsert_candles([closed])
    before = await _snapshot(repository)
    changed = await repository.reconcile_native_candles([_native(), _native(1)])
    assert len(changed) == 1 and changed[0]["open_at"] == _OPEN + timedelta(minutes=1)
    assert (await _snapshot(repository))[0] == before[0]
    assert await repository.reconcile_native_candles([_native(), _native(1)]) == []


@pytest.mark.asyncio
async def test_later_failure_rolls_back_staged_zero_correction(
    repository: SQLAlchemyRepository,
) -> None:
    """A partial successful close and successor never survive a later batch failure."""
    protected = _native(2) | {"public_id": "00000000-0000-4000-8000-000000000703"}
    await repository.upsert_candles([_zero(), protected])
    before = await _snapshot(repository)
    invalid = _native(1) | {"public_id": protected["public_id"]}
    with pytest.raises(IntegrityError):
        await repository.reconcile_native_candles([_native(), invalid])
    assert await _snapshot(repository) == before


@pytest.mark.asyncio
async def test_concurrent_native_winner_is_seen_after_writer_fence(
    repository: SQLAlchemyRepository,
) -> None:
    """A queued correction observes the committed live winner and preserves it."""
    await repository.upsert_candles([_zero()])
    async with repository.session() as session:
        winner = _native() | {"timestamp": _AUTHOR - timedelta(minutes=1)}
        await repository.upsert_candles([winner], session=session)
        await session.flush()
        pending = asyncio.create_task(repository.reconcile_native_candles([_native()]))
        await asyncio.sleep(0.05)
        assert not pending.done()
        await session.commit()
    before = await _snapshot(repository)
    assert await pending == []
    assert await _snapshot(repository) == before


@pytest.mark.asyncio
async def test_lost_close_refuses_false_acknowledgement(
    repository: SQLAlchemyRepository,
) -> None:
    """A failed exact-version closure cannot insert a conflicting successor."""
    await repository.upsert_candles([_zero()])
    before = await _snapshot(repository)
    async with repository.session() as session:
        await session.execute(
            text(
                "CREATE TRIGGER refuse_native_close BEFORE UPDATE OF known_to ON candles BEGIN SELECT RAISE(IGNORE); END"
            )
        )
        await session.commit()
    with pytest.raises(RuntimeError, match="exact source-owned version"):
        await repository.reconcile_native_candles([_native()])
    assert await _snapshot(repository) == before


@pytest.mark.asyncio
async def test_uncertain_commit_is_restart_safe_and_idempotent(
    repository: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A committed correction with interrupted acknowledgement remains a safe replay noop."""
    await repository.upsert_candles([_zero()])
    original_commit = AsyncSession.commit

    async def commit_then_interrupt(session: AsyncSession) -> None:
        """Interrupt only the acknowledgement after the real durable commit."""
        await original_commit(session)
        raise asyncio.CancelledError

    with monkeypatch.context() as patch:
        patch.setattr(AsyncSession, "commit", commit_then_interrupt)
        with pytest.raises(asyncio.CancelledError):
            await repository.reconcile_native_candles([_native()])
    before = await _snapshot(repository)
    assert len(before) == 2
    assert await repository.reconcile_native_candles([_native()]) == []
    assert await _snapshot(repository) == before


@pytest.mark.asyncio
async def test_invalid_or_unbounded_native_batches_refuse_before_write(
    repository: SQLAlchemyRepository,
) -> None:
    """Native source, closure, statistics, exact minutes and finite batch size stay required."""
    with pytest.raises(ValueError, match="720"):
        await repository.reconcile_native_candles([_native(offset) for offset in range(721)])
    for changes in (
        {"trades": None},
        {"source": "synthesized"},
        {"open_at": _OPEN + timedelta(seconds=1)},
        {"complete": False},
    ):
        invalid = _native() | cast(CandleUpsertRow, changes)
        with pytest.raises(ValueError):
            await repository.reconcile_native_candles([invalid])
    with pytest.raises(ValueError, match="duplicate"):
        await repository.reconcile_native_candles([_native(), _native()])
    assert await _snapshot(repository) == []


def test_zero_guard_defensively_requires_synthetic_empty_fields() -> None:
    """The exact-row correction predicate refuses every independently unsafe predecessor."""
    existing = cast(CandleVersionRow, _zero())
    assert SQLAlchemyRepository._can_correct_synthetic_zero(existing, _native())
    for changes in (
        {"source": "native"},
        {"volume": 1.0},
        {"trades": 1},
        {"high": 11.0},
        {"timestamp": _AUTHOR},
    ):
        protected = cast(CandleVersionRow, _zero() | cast(CandleUpsertRow, changes))
        assert not SQLAlchemyRepository._can_correct_synthetic_zero(protected, _native())
    assert not SQLAlchemyRepository._can_correct_synthetic_zero(
        existing, _native() | {"trades": None}
    )
    assert not SQLAlchemyRepository._can_correct_synthetic_zero(existing, _native() | {"trades": 0})


@pytest.mark.asyncio
async def test_full_batch_replay_uses_six_indexed_reads_without_candle_writes(
    repository: SQLAlchemyRepository,
) -> None:
    """A maximum native replay stays inside three chunks of two historical lookups."""
    rows = [_native(offset) for offset in range(720)]
    await repository.upsert_candles(rows)
    statements: list[str] = []

    def capture(*arguments: object) -> None:
        """Capture SQL statements without inspecting driver data or test secrets."""
        statement = cast(str, arguments[2])
        statements.append(statement.strip().upper())

    event.listen(repository.engine.sync_engine, "before_cursor_execute", capture)
    try:
        assert await repository.reconcile_native_candles(rows) == []
    finally:
        event.remove(repository.engine.sync_engine, "before_cursor_execute", capture)
    assert sum(statement.startswith("SELECT") for statement in statements) == 6
    assert not any(statement.startswith(("INSERT", "UPDATE", "DELETE")) for statement in statements)


@pytest.mark.asyncio
async def test_adjacent_native_minute_does_not_correct_zero_without_exact_minute_evidence(
    repository: SQLAlchemyRepository,
) -> None:
    """Trade evidence for the next minute inserts only that key and leaves the zero intact."""
    await repository.upsert_candles([_zero()])
    before = await _snapshot(repository)
    changed = await repository.reconcile_native_candles([_native(1)])
    assert len(changed) == 1 and changed[0]["open_at"] == _OPEN + timedelta(minutes=1)
    assert (await _snapshot(repository))[0] == before[0]
