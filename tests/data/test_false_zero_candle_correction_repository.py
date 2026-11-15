"""Verify native trade evidence corrects false synthesized zeros without losing history."""

import asyncio
from collections.abc import AsyncIterator
from collections.abc import Iterator
from contextlib import contextmanager
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
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.core.candle_identity import deterministic_candle_public_id
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Candle
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import CandleUpsertRow
from snapper.data.repository_types import CandleVersionRow

_OPEN = datetime(2026, 1, 10, 12, tzinfo=UTC)
_LEGACY_ID = "0197ffff-0000-7000-8000-00000000c0de"
_AUTHOR = datetime(2026, 7, 1, tzinfo=UTC)


def _native(offset: int = 0) -> CandleUpsertRow:
    """Build an exact closed native minute with an observed trade count and acquisition time."""
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
    """Build an earlier flat synthesized bar that asserts nobody traded."""
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
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'false-zero.db'}")
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


@contextmanager
def _statements(repo: SQLAlchemyRepository) -> Iterator[list[str]]:
    """Capture the SQL text sent to the driver, without its bound parameters."""
    statements: list[str] = []

    def capture(*arguments: object) -> None:
        """Record one statement's text in upper case."""
        statements.append(cast(str, arguments[2]).strip().upper())

    event.listen(repo.engine.sync_engine, "before_cursor_execute", capture)
    try:
        yield statements
    finally:
        event.remove(repo.engine.sync_engine, "before_cursor_execute", capture)


@pytest.mark.asyncio
async def test_native_trades_correct_zero_and_preserve_bitemporal_history_and_identity(
    repository: SQLAlchemyRepository,
) -> None:
    """Exact native trade evidence versions a false zero while earlier as-of reads keep it.

    Given: A current flat synthesized zero and a later native row with trades.
    When: The zero is corrected, then the same native row is replayed.
    Then: The zero is closed at the native acquisition time, the native row keeps its
        public ID, an earlier as-of cut still reads the zero and the replay is a no-op.
    """
    zero = _zero()
    zero["public_id"] = _LEGACY_ID
    await repository.upsert_candles([zero])
    before = await _snapshot(repository)
    native = _native()
    original = native.copy()

    changed = await repository.correct_false_zero_candles([native])

    assert native == original
    assert len(changed) == 1
    after = await _snapshot(repository)
    assert len(after) == 2
    assert after[0] == before[0] | {"known_to": _AUTHOR}
    assert after[1]["public_id"] == before[0]["public_id"] == changed[0]["public_id"] == _LEGACY_ID
    assert (
        deterministic_candle_public_id(
            native["instrument_public_id"], native["timeframe"], native["open_at"]
        )
        != _LEGACY_ID
    )
    assert all(after[1][field] == value for field, value in native.items())
    assert after[1]["known_to"] == KNOWN_TO_MAX
    horizon = _AUTHOR - timedelta(minutes=1)
    assert [
        row["trades"]
        for row in after
        if cast(datetime, row["timestamp"]) <= horizon < cast(datetime, row["known_to"])
    ] == [0]
    assert await repository.correct_false_zero_candles([native]) == []
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
    """Native, trade-bearing, unknown, non-flat and non-older versions cannot be replaced.

    Given: A current version that differs from a correctable zero in one field.
    When: A native row with trades is reconciled.
    Then: Nothing is corrected and the stored history is unchanged.
    """
    await repository.upsert_candles([_zero() | changes])
    before = await _snapshot(repository)
    assert await repository.correct_false_zero_candles([_native()]) == []
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
    """A later synthesized zero never erases evidence that another source wrote the minute.

    Given: An earlier version from another source, with a volume, with trades or
        with an unknown trade count, replaced by a current flat zero.
    When: A native row with trades is reconciled.
    Then: The zero is protected by its history and nothing changes.
    """
    prior = _zero() | changes | {"timestamp": _AUTHOR - timedelta(hours=2)}
    await repository.upsert_candles([prior])
    await repository.upsert_candles([_zero()])
    before = await _snapshot(repository)
    assert await repository.correct_false_zero_candles([_native()]) == []
    assert await _snapshot(repository) == before


@pytest.mark.asyncio
async def test_no_trade_response_and_unavailable_response_leave_synthetic_zero(
    repository: SQLAlchemyRepository,
) -> None:
    """A native no-trade observation or an empty response provides no replacement evidence.

    Given: A current flat synthesized zero.
    When: A native row without trades, then an empty batch, is reconciled.
    Then: Nothing is corrected.
    """
    await repository.upsert_candles([_zero()])
    before = await _snapshot(repository)
    assert await repository.correct_false_zero_candles([_native() | {"trades": 0}]) == []
    assert await repository.correct_false_zero_candles([]) == []
    assert await _snapshot(repository) == before


@pytest.mark.asyncio
async def test_explicit_trade_count_is_evidence_even_if_volume_rounds_to_zero(
    repository: SQLAlchemyRepository,
) -> None:
    """The native trade count is authoritative even where volume has finite precision.

    Given: A current flat zero and a native row with trades but a zero volume.
    When: The native row is reconciled.
    Then: The zero is corrected with the native trade count.
    """
    await repository.upsert_candles([_zero()])
    changed = await repository.correct_false_zero_candles([_native() | {"volume": 0.0}])
    assert len(changed) == 1
    assert changed[0]["trades"] == 7 and changed[0]["volume"] == 0


@pytest.mark.asyncio
async def test_absent_or_closed_only_minutes_are_never_inserted_by_correction(
    repository: SQLAlchemyRepository,
) -> None:
    """Correction replaces current zeros only; absent minutes stay for insertion.

    Given: A minute whose only zero version is closed, and a minute with no history.
    When: Native rows with trades for both minutes are reconciled.
    Then: Nothing is written; neither the closed zero nor the absent minute is touched.
    """
    await repository.upsert_candles([_zero() | {"known_to": _AUTHOR - timedelta(minutes=30)}])
    before = await _snapshot(repository)
    assert await repository.correct_false_zero_candles([_native(), _native(1)]) == []
    assert await _snapshot(repository) == before


@pytest.mark.asyncio
async def test_one_commit_corrects_every_disproved_zero_in_input_order(
    repository: SQLAlchemyRepository,
) -> None:
    """A batch corrects each disproved zero and skips the rest.

    Given: Two correctable zeros, one non-flat synthesized bar and one absent minute.
    When: Native rows for all four minutes are reconciled in one call.
    Then: Exactly the two zeros are corrected, in input order.
    """
    await repository.upsert_candles([_zero(2), _zero(), _zero(1) | {"high": 11.0}])
    rows = [_native(3), _native(2), _native(1), _native()]
    changed = await repository.correct_false_zero_candles(rows)
    assert [row["open_at"] for row in changed] == [
        _OPEN + timedelta(minutes=2),
        _OPEN,
    ]
    current = [row for row in await _snapshot(repository) if row["known_to"] == KNOWN_TO_MAX]
    assert [row["source"] for row in current] == ["native", "synthesized", "native"]


@pytest.mark.asyncio
async def test_dry_run_reports_the_correction_without_a_fence_or_a_write(
    repository: SQLAlchemyRepository,
) -> None:
    """The preflight names the correctable rows without locking or writing anything.

    Given: One correctable zero, one non-flat synthesized bar and one absent minute.
    When: The three native rows are reconciled in dry-run.
    Then: Only the correctable row is returned and no write or fence statement runs.
    """
    await repository.upsert_candles([_zero(), _zero(1) | {"high": 11.0}])
    before = await _snapshot(repository)
    with _statements(repository) as statements:
        candidates = await repository.correct_false_zero_candles(
            [_native(), _native(1), _native(2)], dry_run=True
        )
    assert [row["open_at"] for row in candidates] == [_OPEN]
    assert await _snapshot(repository) == before
    assert not any(
        statement.startswith(("INSERT", "UPDATE", "DELETE", "BEGIN")) for statement in statements
    )


@pytest.mark.asyncio
async def test_later_failure_rolls_back_an_earlier_staged_correction(
    repository: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A correction staged earlier in the batch never survives a later failure.

    Given: Two correctable zeros and a replacement that fails on the second.
    When: Both are corrected in one call.
    Then: The failure propagates and the first correction is rolled back.
    """
    await repository.upsert_candles([_zero(), _zero(1)])
    before = await _snapshot(repository)
    original = SQLAlchemyRepository._replace_synthesized_candle_version
    calls = 0

    async def fail_second(
        session: AsyncSession, row: CandleUpsertRow, existing: CandleVersionRow
    ) -> None:
        """Stage the first replacement, then fail before the second."""
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("second replacement failed")
        await original(session, row, existing)

    monkeypatch.setattr(
        SQLAlchemyRepository, "_replace_synthesized_candle_version", staticmethod(fail_second)
    )
    with pytest.raises(RuntimeError, match="second replacement failed"):
        await repository.correct_false_zero_candles([_native(), _native(1)])
    assert calls == 2
    assert await _snapshot(repository) == before


@pytest.mark.asyncio
async def test_concurrent_native_winner_is_seen_after_writer_fence(
    repository: SQLAlchemyRepository,
) -> None:
    """A correction queued behind a live writer re-reads the minute and keeps the winner.

    Given: A zero that a concurrent, not yet committed writer replaces with a native row.
    When: The correction runs while that writer holds its transaction.
    Then: The correction waits for the fence, sees the winner and changes nothing.
    """
    await repository.upsert_candles([_zero()])
    async with repository.session() as session:
        winner = _native() | {"timestamp": _AUTHOR - timedelta(minutes=1)}
        await repository.upsert_candles([winner], session=session)
        await session.flush()
        pending = asyncio.create_task(repository.correct_false_zero_candles([_native()]))
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
    """A failed exact-version close cannot insert a conflicting successor.

    Given: A correctable zero and a database that silently ignores the close.
    When: The zero is corrected.
    Then: The correction refuses and the stored history is unchanged.
    """
    await repository.upsert_candles([_zero()])
    before = await _snapshot(repository)
    async with repository.session() as session:
        await session.execute(
            text(
                "CREATE TRIGGER refuse_zero_close BEFORE UPDATE OF known_to ON candles "
                "BEGIN SELECT RAISE(IGNORE); END"
            )
        )
        await session.commit()
    with pytest.raises(RuntimeError, match="exact source-owned version"):
        await repository.correct_false_zero_candles([_native()])
    assert await _snapshot(repository) == before


@pytest.mark.asyncio
async def test_uncertain_commit_is_restart_safe_and_idempotent(
    repository: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A committed correction whose acknowledgement was lost replays as a no-op.

    Given: A correction whose commit succeeds but whose acknowledgement is interrupted.
    When: The same native row is reconciled again.
    Then: Nothing more is written.
    """
    await repository.upsert_candles([_zero()])
    original_commit = AsyncSession.commit

    async def commit_then_interrupt(session: AsyncSession) -> None:
        """Interrupt only the acknowledgement after the real durable commit."""
        await original_commit(session)
        raise asyncio.CancelledError

    with monkeypatch.context() as patch:
        patch.setattr(AsyncSession, "commit", commit_then_interrupt)
        with pytest.raises(asyncio.CancelledError):
            await repository.correct_false_zero_candles([_native()])
    before = await _snapshot(repository)
    assert len(before) == 2
    assert await repository.correct_false_zero_candles([_native()]) == []
    assert await _snapshot(repository) == before


@pytest.mark.asyncio
async def test_invalid_or_unbounded_native_batches_refuse_before_any_read(
    repository: SQLAlchemyRepository,
) -> None:
    """Native source, closure, statistics, exact minutes and a finite batch stay required.

    Given: Oversized, malformed or duplicated native batches.
    When: Each is reconciled.
    Then: Each is refused with a validation error and nothing is stored.
    """
    with pytest.raises(ValueError, match="720"):
        await repository.correct_false_zero_candles([_native(offset) for offset in range(721)])
    for changes in (
        {"trades": None},
        {"source": "synthesized"},
        {"open_at": _OPEN + timedelta(seconds=1)},
        {"complete": False},
    ):
        invalid = _native() | cast(CandleUpsertRow, changes)
        with pytest.raises(ValueError):
            await repository.correct_false_zero_candles([invalid])
    with pytest.raises(ValueError, match="duplicate"):
        await repository.correct_false_zero_candles([_native(), _native()])
    assert await _snapshot(repository) == []


def test_zero_predicate_requires_every_false_zero_condition() -> None:
    """The row predicate refuses every independently unsafe predecessor or native row.

    Given: A correctable zero and variants that each break one condition.
    When: The predicate is evaluated against a native row with trades.
    Then: Only the unchanged zero and a native row with trades qualify.
    """
    existing = cast(CandleVersionRow, _zero())
    assert SQLAlchemyRepository._disproves_false_zero(existing, _native())
    for changes in (
        {"source": "native"},
        {"volume": 1.0},
        {"trades": 1},
        {"high": 11.0},
        {"timestamp": _AUTHOR},
    ):
        protected = cast(CandleVersionRow, _zero() | cast(CandleUpsertRow, changes))
        assert not SQLAlchemyRepository._disproves_false_zero(protected, _native())
    assert not SQLAlchemyRepository._disproves_false_zero(existing, _native() | {"trades": None})
    assert not SQLAlchemyRepository._disproves_false_zero(existing, _native() | {"trades": 0})


@pytest.mark.asyncio
async def test_full_replay_without_zeros_needs_one_read_and_no_fence(
    repository: SQLAlchemyRepository,
) -> None:
    """A maximum native replay over present minutes costs one indexed read.

    Given: 720 present native minutes and no synthesized zero.
    When: The same 720 rows are reconciled.
    Then: One SELECT runs, with no fence, insert, update or delete.
    """
    rows = [_native(offset) for offset in range(720)]
    await repository.upsert_candles(rows)
    with _statements(repository) as statements:
        assert await repository.correct_false_zero_candles(rows) == []
    assert sum(statement.startswith("SELECT") for statement in statements) == 1
    assert not any(
        statement.startswith(("INSERT", "UPDATE", "DELETE", "BEGIN")) for statement in statements
    )


@pytest.mark.asyncio
async def test_adjacent_native_minute_does_not_correct_zero_without_exact_minute_evidence(
    repository: SQLAlchemyRepository,
) -> None:
    """Trade evidence for the next minute leaves the zero intact.

    Given: A current zero and a native row with trades for the following minute only.
    When: That native row is reconciled.
    Then: Nothing is corrected.
    """
    await repository.upsert_candles([_zero()])
    before = await _snapshot(repository)
    assert await repository.correct_false_zero_candles([_native(1)]) == []
    assert await _snapshot(repository) == before
