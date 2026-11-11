"""Verify the guarded writers the candle repair driver uses for calculated rows and voted zeros.

``insert_missing_calculated_candles`` applies the native repair's rules to
calculated rows, which are one-minute bars only. ``correct_false_zero_candles`` takes the rows' source and,
optionally, the exact zero version each correction was approved to replace;
that check is repeated under the writer fence.
"""

from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
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
from sqlalchemy import insert
from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Candle
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import CandlePredecessor
from snapper.data.repository_types import CandleUpsertRow
from snapper.data.repository_types import CandleVersionRow

_INSTRUMENT = "00000000-0000-4000-8000-000000000a01"
_SESSION = "00000000-0000-4000-8000-000000000a02"
_OPEN = datetime(2026, 1, 10, 12, tzinfo=UTC)
_AUTHOR = datetime(2026, 7, 1, tzinfo=UTC)


def _calculated(offset: int = 0) -> CandleUpsertRow:
    """Build a complete closed calculated minute with trades, as the trade builder emits it."""
    return {
        "instrument_public_id": _INSTRUMENT,
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
        "source": "calculated",
        "complete": True,
        "session_id": _SESSION,
        "sequence_id": offset + 1,
    }


def _native(offset: int = 0) -> CandleUpsertRow:
    """Build the same minute as a native row."""
    return _calculated(offset) | {"source": "native"}


def _zero(offset: int = 0) -> CandleUpsertRow:
    """Build an earlier flat synthesized bar that claims nobody traded."""
    return _calculated(offset) | {
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
    """Provide a disposable file-backed repository."""
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'repair-guards.db'}")
    await repo.create_all()
    try:
        yield repo
    finally:
        await repo.engine.dispose()


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


async def _versions(repo: SQLAlchemyRepository) -> list[dict[str, object]]:
    """Read every stored version with all physical columns."""
    async with repo.session() as session:
        result = await session.execute(
            select(Candle.__table__).order_by(Candle.open_at, Candle.timestamp, Candle.id)
        )
        return [dict(row) for row in result.mappings().all()]


async def _current_zero(repo: SQLAlchemyRepository, offset: int = 0) -> CandlePredecessor:
    """Return the identity of a minute's current version as an expected predecessor."""
    for version in await _versions(repo):
        if version["open_at"] == _OPEN + timedelta(minutes=offset) and (
            version["known_to"] == KNOWN_TO_MAX
        ):
            return {
                "id": cast(int, version["id"]),
                "public_id": cast(str, version["public_id"]),
                "timestamp": cast(datetime, version["timestamp"]),
            }
    raise AssertionError("no current version")


def _key(offset: int = 0) -> tuple[str, str, datetime]:
    """Return a minute's full natural key."""
    return _INSTRUMENT, "1m", _OPEN + timedelta(minutes=offset)


async def _replace_zero_at_same_time(repo: SQLAlchemyRepository, offset: int = 0) -> None:
    """Close the current zero and insert another zero with the same time and public ID.

    The successor differs from the closed version only in its physical id.
    """
    async with repo.session() as session:
        stored = (
            (
                await session.execute(
                    select(Candle.__table__).where(
                        Candle.open_at == _OPEN + timedelta(minutes=offset),
                        Candle.known_to == KNOWN_TO_MAX,
                    )
                )
            )
            .mappings()
            .one()
        )
        await session.execute(
            update(Candle).where(Candle.id == stored["id"]).values(known_to=stored["timestamp"])
        )
        successor = {name: value for name, value in stored.items() if name != "id"}
        await session.execute(insert(Candle).values(**successor))
        await session.commit()


@pytest.mark.asyncio
async def test_calculated_insert_adds_only_absent_minutes(
    repository: SQLAlchemyRepository,
) -> None:
    """Calculated rows fill absent minutes and never touch any existing version.

    Given: A current native minute, a closed-only zero minute and an absent minute.
    When: Calculated rows for all three are inserted.
    Then: Only the absent minute is inserted, as calculated, at the repository's time,
        and the stored history of the other two is unchanged.
    """
    await repository.upsert_candles([_native(0)])
    await repository.upsert_candles([_zero(1) | {"known_to": _AUTHOR - timedelta(minutes=30)}])
    before = await _versions(repository)
    started = datetime.now(UTC)

    inserted = await repository.insert_missing_calculated_candles(
        [_calculated(0), _calculated(1), _calculated(2)]
    )

    assert [row["open_at"] for row in inserted] == [_OPEN + timedelta(minutes=2)]
    after = await _versions(repository)
    assert all(row in after for row in before)
    added = [row for row in after if row not in before]
    assert len(added) == 1
    assert added[0]["source"] == "calculated"
    assert added[0]["trades"] == 7
    assert cast(datetime, added[0]["timestamp"]) >= started
    assert await repository.insert_missing_calculated_candles([_calculated(2)]) == []


@pytest.mark.asyncio
async def test_calculated_insert_dry_run_reports_absence_without_writing(
    repository: SQLAlchemyRepository,
) -> None:
    """The dry run names the absent calculated minutes and stores nothing.

    Given: One present minute and one absent minute.
    When: Both calculated rows are inserted in dry-run mode.
    Then: Only the absent row is returned and the table is unchanged.
    """
    await repository.upsert_candles([_native(0)])
    before = await _versions(repository)
    candidates = await repository.insert_missing_calculated_candles(
        [_calculated(0), _calculated(1)], dry_run=True
    )
    assert [row["open_at"] for row in candidates] == [_OPEN + timedelta(minutes=1)]
    assert await _versions(repository) == before


@pytest.mark.asyncio
async def test_each_insert_mode_refuses_the_other_source(
    repository: SQLAlchemyRepository,
) -> None:
    """A native repair cannot insert calculated rows and a calculated repair cannot insert native ones.

    Given: An empty table.
    When: Native rows go to the calculated insert and calculated rows to the native insert.
    Then: Both batches are refused before anything is stored.
    """
    with pytest.raises(ValueError, match="source-owned"):
        await repository.insert_missing_calculated_candles([_native(0)])
    with pytest.raises(ValueError, match="source-owned"):
        await repository.insert_missing_native_candles([_calculated(0)])
    assert await _versions(repository) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("dry_run", [False, True])
async def test_calculated_insert_refuses_a_batch_holding_a_five_minute_row(
    repository: SQLAlchemyRepository, dry_run: bool
) -> None:
    """A calculated row that is not one minute refuses its whole batch before any statement.

    Given: An empty table and a batch of a valid calculated minute and a closed,
        aligned calculated 5m bar.
    When: The batch is inserted, as a write or as a dry run.
    Then: The whole batch is refused as an unsupported timeframe before any SQL
        statement is sent, and nothing is stored.
    """
    five_minutes = _calculated() | {"timeframe": "5m"}
    with (
        _statements(repository) as statements,
        pytest.raises(ValueError, match="timeframe is not supported for its source"),
    ):
        await repository.insert_missing_calculated_candles(
            [_calculated(1), five_minutes], dry_run=dry_run
        )
    assert statements == []
    assert await _versions(repository) == []


@pytest.mark.parametrize("timeframe", ["5m", "15m", "30m", "1h", "4h", "1d"])
def test_calculated_rows_are_never_venue_bars_while_native_ones_still_are(
    timeframe: str,
) -> None:
    """Only native repairs may hold venue bars above one minute.

    Given: A supported higher timeframe in the bounded repair's validation.
    When: Its bar length is resolved for a calculated and for a native row.
    Then: The calculated row is refused, the native row keeps its venue bar length and
        a calculated minute is still sixty seconds.
    """
    with pytest.raises(ValueError, match="timeframe is not supported for its source"):
        SQLAlchemyRepository._reconciled_candle_seconds(timeframe, "calculated", venue_bars=True)
    assert (
        SQLAlchemyRepository._reconciled_candle_seconds(timeframe, "native", venue_bars=True) > 60
    )
    assert (
        SQLAlchemyRepository._reconciled_candle_seconds("1m", "calculated", venue_bars=True) == 60
    )


@pytest.mark.asyncio
async def test_native_insert_still_accepts_a_five_minute_venue_bar(
    repository: SQLAlchemyRepository,
) -> None:
    """A native 5m venue bar remains repairable through the native insert.

    Given: An empty table and a closed, aligned native 5m bar.
    When: It is inserted as a dry run and then as a write.
    Then: The dry run reports it and the write stores it as a native 5m bar.
    """
    five_minutes = _native() | {"timeframe": "5m"}
    candidates = await repository.insert_missing_native_candles([five_minutes], dry_run=True)
    inserted = await repository.insert_missing_native_candles([five_minutes])
    assert [row["timeframe"] for row in candidates] == ["5m"]
    assert [(row["timeframe"], row["source"]) for row in inserted] == [("5m", "native")]
    stored = await _versions(repository)
    assert [(row["timeframe"], row["source"], row["open_at"]) for row in stored] == [
        ("5m", "native", _OPEN)
    ]


@pytest.mark.asyncio
async def test_calculated_correction_replaces_a_flat_zero_and_keeps_its_source(
    repository: SQLAlchemyRepository,
) -> None:
    """A calculated row with trades corrects a false zero under the zero's public ID.

    Given: A current flat synthesized zero.
    When: A later calculated row with trades corrects it in calculated mode.
    Then: The zero is closed at the row's time and the stored successor is calculated
        and carries the zero's public ID.
    """
    await repository.upsert_candles([_zero()])
    zero = await _current_zero(repository)

    corrected = await repository.correct_false_zero_candles([_calculated()], source="calculated")

    assert len(corrected) == 1
    after = await _versions(repository)
    assert after[0]["known_to"] == _AUTHOR
    assert after[1]["source"] == "calculated"
    assert after[1]["public_id"] == zero["public_id"]
    assert after[1]["known_to"] == KNOWN_TO_MAX


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
async def test_calculated_correction_refuses_a_zero_with_protected_history(
    repository: SQLAlchemyRepository, changes: CandleUpsertRow
) -> None:
    """History from another source, with volume or with trades protects the zero.

    Given: An earlier version of another kind replaced by a current flat zero.
    When: A calculated row with trades is reconciled in calculated mode.
    Then: Nothing is corrected and the history is unchanged.
    """
    await repository.upsert_candles(
        [_zero() | changes | {"timestamp": _AUTHOR - timedelta(hours=2)}]
    )
    await repository.upsert_candles([_zero()])
    before = await _versions(repository)
    assert await repository.correct_false_zero_candles([_calculated()], source="calculated") == []
    assert await _versions(repository) == before


@pytest.mark.asyncio
async def test_correction_refuses_rows_of_the_other_source(
    repository: SQLAlchemyRepository,
) -> None:
    """Calculated mode refuses native rows and native mode refuses calculated rows.

    Given: A current flat zero.
    When: Native rows are corrected in calculated mode, and calculated rows by default.
    Then: Both batches are refused and the zero stays current.
    """
    await repository.upsert_candles([_zero()])
    before = await _versions(repository)
    with pytest.raises(ValueError, match="source-owned"):
        await repository.correct_false_zero_candles([_native()], source="calculated")
    with pytest.raises(ValueError, match="source-owned"):
        await repository.correct_false_zero_candles([_calculated()])
    assert await _versions(repository) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("changes", [{"vwap": None}, {"trades": None}, {"timeframe": "5m"}])
async def test_calculated_correction_requires_complete_one_minute_statistics(
    repository: SQLAlchemyRepository, changes: CandleUpsertRow
) -> None:
    """Calculated corrections keep the native rule of known VWAP, trades and one-minute bars.

    Given: A current flat zero.
    When: A calculated row without VWAP, without trades or of another timeframe is corrected.
    Then: The batch is refused.
    """
    await repository.upsert_candles([_zero()])
    with pytest.raises(ValueError):
        await repository.correct_false_zero_candles([_calculated() | changes], source="calculated")


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["id", "public_id", "timestamp", "missing"])
async def test_expected_predecessor_that_differs_skips_the_row(
    repository: SQLAlchemyRepository, field: str
) -> None:
    """A correction applies only to the voted zero, in dry run and write alike.

    Given: Two correctable zeros, one expected exactly and one with a differing or
        missing expectation.
    When: Both are corrected with expected predecessors, first in dry run.
    Then: Only the exactly expected zero is reported and corrected.
    """
    await repository.upsert_candles([_zero(0), _zero(1)])
    exact = await _current_zero(repository, 0)
    other = await _current_zero(repository, 1)
    differing: dict[str, object] = {
        "id": other["id"] + 1000,
        "public_id": "00000000-0000-4000-8000-00000000ffff",
        "timestamp": other["timestamp"] - timedelta(seconds=1),
    }
    expected = {_key(0): exact}
    if field != "missing":
        expected[_key(1)] = cast(CandlePredecessor, other | {field: differing[field]})
    rows = [_native(0), _native(1)]

    candidates = await repository.correct_false_zero_candles(
        rows, dry_run=True, expected_predecessors=expected
    )
    corrected = await repository.correct_false_zero_candles(rows, expected_predecessors=expected)

    assert [row["open_at"] for row in candidates] == [_OPEN]
    assert [row["open_at"] for row in corrected] == [_OPEN]
    current = [row for row in await _versions(repository) if row["known_to"] == KNOWN_TO_MAX]
    assert [row["source"] for row in current] == ["native", "synthesized"]


@pytest.mark.asyncio
async def test_expected_predecessor_catches_a_same_timestamp_replacement(
    repository: SQLAlchemyRepository,
) -> None:
    """A zero replaced by an identical zero under a new physical id is not the voted one.

    Given: A voted zero later replaced by a zero with the same time and public ID.
    When: The correction runs with the voted zero, then without any expectation.
    Then: The voted correction is skipped; without the expectation the successor is corrected.
    """
    await repository.upsert_candles([_zero()])
    voted = await _current_zero(repository)
    await _replace_zero_at_same_time(repository)
    successor = await _current_zero(repository)
    assert (successor["public_id"], successor["timestamp"]) == (
        voted["public_id"],
        voted["timestamp"],
    )
    assert successor["id"] != voted["id"]

    skipped = await repository.correct_false_zero_candles(
        [_native()], expected_predecessors={_key(): voted}
    )
    corrected = await repository.correct_false_zero_candles([_native()])

    assert skipped == []
    assert len(corrected) == 1


@pytest.mark.asyncio
async def test_expected_predecessor_is_checked_again_under_the_writer_fence(
    repository: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A zero replaced between the preflight and the fence is caught in the write transaction.

    Given: A voted zero that matches the preflight and is replaced at the same time just
        before the writer fence is taken.
    When: The correction runs with the voted zero.
    Then: The fenced re-check skips the row and the successor zero stays current.
    """
    await repository.upsert_candles([_zero()])
    voted = await _current_zero(repository)
    original = SQLAlchemyRepository._begin_candle_reconciliation_write
    replaced: list[bool] = []

    async def replace_then_fence(
        self: SQLAlchemyRepository, session: AsyncSession, **options: bool | str
    ) -> None:
        """Replace the zero from another session, then take the real fence."""
        if not replaced:
            replaced.append(True)
            await _replace_zero_at_same_time(repository)
        fence = cast(Callable[..., Awaitable[None]], original)
        await fence(self, session, **options)

    monkeypatch.setattr(
        SQLAlchemyRepository, "_begin_candle_reconciliation_write", replace_then_fence
    )
    result = await repository.correct_false_zero_candles(
        [_native()], expected_predecessors={_key(): voted}
    )

    assert replaced == [True]
    assert result == []
    current = [row for row in await _versions(repository) if row["known_to"] == KNOWN_TO_MAX]
    assert [row["source"] for row in current] == ["synthesized"]
    assert current[0]["id"] != voted["id"]


def test_retaining_expected_predecessors_without_expectations_keeps_every_target() -> None:
    """Without expectations the filter is the identity, so the defaults behave as before.

    Given: Two targets.
    When: They are filtered without expectations.
    Then: The same list is returned unchanged.
    """
    targets = [
        (_native(0), cast(CandleVersionRow, _zero(0) | {"id": 1})),
        (_native(1), cast(CandleVersionRow, _zero(1) | {"id": 2})),
    ]
    kept = SQLAlchemyRepository._retain_expected_predecessors(targets, None)
    assert kept is targets


@pytest.mark.asyncio
async def test_session_versions_are_marked_current_by_the_same_read(
    repository: SQLAlchemyRepository,
) -> None:
    """One writer session's versions of the requested keys come back with their currentness.

    Given: A writer session's current minute, its minute later versioned by another
        writer, another session's minute and the session's minute that is not requested.
    When: The session's versions of three keys, and then of no key, are read.
    Then: Exactly the session's two requested versions return, the first current and the
        second superseded, without the computed column; no key returns nothing.
    """
    mine = "00000000-0000-4000-8000-00000000aaaa"
    await repository.upsert_candles(
        [_native(offset) | {"session_id": mine} for offset in (0, 1, 3)] + [_native(2)]
    )
    await repository.upsert_candles(
        [_native(1) | {"timestamp": _AUTHOR + timedelta(hours=1), "close": 11.5}]
    )
    async with repository.session() as session:
        versions = await repository.get_session_candle_versions(
            session, [_key(0), _key(1), _key(2)], mine
        )
        nothing = await repository.get_session_candle_versions(session, [], mine)
    assert sorted((row["open_at"], current) for row, current in versions) == [
        (_OPEN, True),
        (_OPEN + timedelta(minutes=1), False),
    ]
    assert {row["session_id"] for row, _ in versions} == {mine}
    assert all("is_current" not in row for row, _ in versions)
    assert nothing == []
