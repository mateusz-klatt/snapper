"""Exercise the voted predecessor, input fingerprint and writer fence."""

from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from hashlib import sha256
from math import inf
from math import nan
from math import nextafter
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
import pytest_asyncio
from sqlalchemy import delete
from sqlalchemy import event
from sqlalchemy import insert
from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Executable

from snapper.core.candle_identity import deterministic_candle_public_id
from snapper.data import repository as repository_module
from snapper.data.models import Candle
from snapper.data.models import is_open_ended_version
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import sample_other_client_backends
from snapper.data.repository import synthesis_inputs_sha256
from snapper.data.repository import synthesized_observation_loss
from snapper.data.repository_types import CandlePredecessor
from snapper.data.repository_types import CandleUpsertRow
from snapper.data.repository_types import CandleVersionRow
from snapper.data.repository_types import ClientSample
from snapper.data.repository_types import SynthesizedRebuildExpectation
from snapper.data.repository_types import SynthesizedRebuildOutcome
from snapper.data.repository_types import SynthesizedRebuildResult

_DAY = datetime(2024, 1, 1, tzinfo=UTC)
_AUTHOR = _DAY + timedelta(days=3)
_WRITE = _AUTHOR + timedelta(hours=1)
_INSTRUMENT = "00000000-0000-4000-8000-000000000501"
_OTHER = "00000000-0000-4000-8000-000000000502"
_SESSION = "00000000-0000-4000-8000-000000000503"
_LEGACY = "00000000-0000-4000-8000-000000000504"

type _Key = tuple[str, str, datetime]


def _row(open_at: datetime = _DAY, timeframe: str = "5m") -> CandleUpsertRow:
    """Build one complete synthesized candidate on a closed grid."""
    return {
        "instrument_public_id": _INSTRUMENT,
        "timeframe": timeframe,
        "open_at": open_at,
        "timestamp": _WRITE,
        "open": 10.0,
        "high": 12.0,
        "low": 9.0,
        "close": 11.0,
        "volume": 3.0,
        "vwap": 10.75,
        "trades": 7,
        "source": "synthesized",
        "complete": True,
        "session_id": _SESSION,
        "sequence_id": 1,
    }


def _key(row: CandleUpsertRow) -> _Key:
    """Return the window's natural key."""
    return row["instrument_public_id"], row["timeframe"], row["open_at"]


def _predecessor(version: CandleVersionRow) -> CandlePredecessor:
    """Extract the exact predecessor identity from one version."""
    return CandlePredecessor(
        id=version["id"], public_id=version["public_id"], timestamp=version["timestamp"]
    )


def _expectation(
    predecessor: CandlePredecessor | None = None, *inputs: CandleVersionRow
) -> SynthesizedRebuildExpectation:
    """Vote for a predecessor or an absence, with the given minute versions."""
    return SynthesizedRebuildExpectation(
        predecessor=predecessor, inputs_sha256=synthesis_inputs_sha256(inputs)
    )


@pytest_asyncio.fixture
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Provide a private file-backed SQLite database for independent transactions."""
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'rebuild.db'}")
    await repo.create_all()
    try:
        yield repo
    finally:
        await repo.engine.dispose()


async def _seed(repo: SQLAlchemyRepository, row: CandleUpsertRow) -> CandleVersionRow:
    """Commit a particular version, including legacy rows that the rebuild cannot write."""
    async with repo.session() as session:
        result = await session.execute(insert(Candle).values(**row).returning(Candle.__table__))
        version = cast(CandleVersionRow, dict(result.mappings().one()))
        await session.commit()
        return version


async def _snapshot(repo: SQLAlchemyRepository) -> list[CandleVersionRow]:
    """Read every version in insertion order without ORM identity projection."""
    async with repo.session() as session:
        result = await session.execute(select(Candle.__table__).order_by(Candle.id))
        return [cast(CandleVersionRow, dict(row)) for row in result.mappings()]


def _input_version() -> CandleVersionRow:
    """Build a stored minute representation for pure fingerprint and loss tests."""
    return cast(
        CandleVersionRow,
        _row(timeframe="1m") | {"id": 17, "public_id": _LEGACY, "source": "native"},
    )


def test_fingerprint_encodes_the_ordered_payload_as_canonical_json() -> None:
    """Encode dates, ids, a nullable integer and the exact representations of the floats."""
    first = _input_version()
    second = first.copy()
    second.update({"open_at": _DAY + timedelta(minutes=1), "vwap": None, "trades": None})
    canonical = (
        f'[["2024-01-01T00:00:00+00:00",17,"{_LEGACY}","2024-01-04T01:00:00+00:00",'
        '"native",true,"10.0","12.0","9.0","11.0","3.0","10.75",7],'
        f'["2024-01-01T00:01:00+00:00",17,"{_LEGACY}","2024-01-04T01:00:00+00:00",'
        '"native",true,"10.0","12.0","9.0","11.0","3.0",null,null]]'
    )
    digest = synthesis_inputs_sha256([second, first])
    assert digest == sha256(canonical.encode()).hexdigest()
    assert synthesis_inputs_sha256([first.copy(), second.copy()]) == digest
    assert synthesis_inputs_sha256([first]) != digest
    assert synthesis_inputs_sha256([]) == sha256(b"[]").hexdigest()


@pytest.mark.parametrize(
    "change",
    [
        {"open_at": _DAY + timedelta(minutes=1)},
        {"id": 18},
        {"public_id": _OTHER},
        {"timestamp": _AUTHOR},
        {"source": "calculated"},
        {"complete": False},
        {"open": 10.5},
        {"high": 12.5},
        {"low": 8.5},
        {"close": 11.5},
        {"volume": 3.5},
        {"vwap": 11.0},
        {"trades": 8},
    ],
)
def test_every_fingerprinted_field_matters(change: dict[str, object]) -> None:
    """Changed input versions alter the hash even when their physical id is reused."""
    version = _input_version()
    modified = cast(CandleVersionRow, dict(version) | change)
    assert synthesis_inputs_sha256([version]) != synthesis_inputs_sha256([modified])


@pytest.mark.parametrize(
    ("current_volume", "current_trades", "rebuilt", "reason"),
    [
        (3.0, 7, {"trades": None}, None),
        (nan, 7, {}, "unknown_current"),
        (inf, 7, {}, "unknown_current"),
        (-inf, 7, {}, "unknown_current"),
        (3.0, inf, {}, "unknown_current"),
        (3.0, nan, {}, "unknown_current"),
        (3.0, None, {"volume": 2.0}, "lower_volume+unknown_current"),
        (3.0, 7, {"volume": nextafter(3.0, 0.0)}, "lower_volume"),
        (3.0, 7, {"trades": 6}, "lower_trades"),
        (3.0, 7, {"volume": 2.0, "trades": 6}, "lower_volume+lower_trades"),
    ],
)
def test_observation_loss_only_compares_known_values_exactly(
    current_volume: float,
    current_trades: int | float | None,
    rebuilt: CandleUpsertRow,
    reason: str | None,
) -> None:
    """Compare finite observations and refuse unknown current values."""
    current = cast(
        CandleVersionRow,
        dict(_input_version()) | {"volume": current_volume, "trades": current_trades},
    )
    row = _row()
    row.update(rebuilt)
    assert synthesized_observation_loss(current, row) == reason


@pytest.mark.parametrize(
    "change",
    [
        {"timeframe": "1m"},
        {"open_at": _DAY + timedelta(minutes=1)},
        {"timestamp": _DAY + timedelta(minutes=4)},
        {"vwap": None},
        {"trades": None},
        {"complete": False},
    ],
)
@pytest.mark.asyncio
async def test_malformed_rebuild_rows_never_read(
    repository: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch, change: CandleUpsertRow
) -> None:
    """Reject malformed candidates before opening either transaction."""
    session = MagicMock()
    monkeypatch.setattr(repository, "session", session)
    row = _row()
    row.update(change)
    with pytest.raises(ValueError):
        await repository.rebuild_synthesized_candles([row], expected={_key(row): _expectation()})
    session.assert_not_called()


@pytest.mark.asyncio
async def test_rebuild_batch_checks_counts_duplicates_and_expectations_before_reads(
    repository: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Enforce both batch ceilings, unique keys and exactly matching expectation keys."""
    session = MagicMock()
    monkeypatch.setattr(repository, "session", session)
    row = _row()
    daily = _row(timeframe="1d")
    eligible = {_key(row): _expectation()}
    with pytest.raises(ValueError, match="100 rows"):
        await repository.rebuild_synthesized_candles([row] * 101, expected={})
    with pytest.raises(ValueError, match="1,440 grid minutes"):
        await repository.rebuild_synthesized_candles(
            [row, daily], expected=eligible | {_key(daily): _expectation()}
        )
    with pytest.raises(ValueError, match="duplicate"):
        await repository.rebuild_synthesized_candles([row, row], expected=eligible)
    with pytest.raises(ValueError, match="match the batch keys"):
        await repository.rebuild_synthesized_candles([row], expected={})
    with pytest.raises(ValueError, match="match the batch keys"):
        await repository.rebuild_synthesized_candles([], expected=eligible)
    assert await repository.rebuild_synthesized_candles(
        [], expected={}
    ) == SynthesizedRebuildResult({})
    session.assert_not_called()


@pytest.mark.asyncio
async def test_dry_run_is_lock_free_and_insert_and_replacement_commit_together(
    repository: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Use both limits at their boundaries in separate batches and preserve the legacy id."""
    current = await _seed(
        repository, _row() | {"timestamp": _AUTHOR, "public_id": _LEGACY, "close": 10.0}
    )
    replacement = _row()
    inserts = [_row(_DAY + timedelta(minutes=5 * offset)) for offset in range(1, 100)]
    rows = [replacement, *inserts]
    expected = {_key(row): _expectation() for row in inserts}
    expected[_key(replacement)] = _expectation(_predecessor(current))
    fence = AsyncMock(wraps=repository._begin_candle_reconciliation_write)
    monkeypatch.setattr(repository, "_begin_candle_reconciliation_write", fence)

    dry_run = await repository.rebuild_synthesized_candles(rows, expected=expected, dry_run=True)
    assert dry_run.outcomes == {
        _key(replacement): SynthesizedRebuildOutcome("replace"),
        **{_key(row): SynthesizedRebuildOutcome("insert") for row in inserts},
    }
    fence.assert_not_called()
    assert await _snapshot(repository) == [current]

    committed = await repository.rebuild_synthesized_candles(rows, expected=expected)
    fence.assert_awaited_once()
    assert fence.call_args.kwargs == {"sqlite_write_lock": True}
    assert committed.sample is None
    assert committed.outcomes == {
        _key(replacement): SynthesizedRebuildOutcome("replaced"),
        **{_key(row): SynthesizedRebuildOutcome("inserted") for row in inserts},
    }
    original, successor, first_insert, *_ = await _snapshot(repository)
    assert original == current | {"known_to": _WRITE}
    assert successor["public_id"] == _LEGACY
    assert successor["timestamp"] == _WRITE
    assert successor["close"] == replacement["close"]
    assert is_open_ended_version(successor["known_to"])
    assert first_insert["public_id"] == deterministic_candle_public_id(*_key(inserts[0]))
    assert "public_id" not in replacement

    daily = _row(timeframe="1d")
    daily_result = await repository.rebuild_synthesized_candles(
        [daily], expected={_key(daily): _expectation()}
    )
    assert daily_result.outcomes == {_key(daily): SynthesizedRebuildOutcome("inserted")}


@pytest.mark.parametrize(
    ("stored", "predecessor_change", "status", "reason"),
    [
        ({"known_to": _WRITE}, {"id": 999}, "history_without_current", None),
        ({"source": "native"}, {"id": 999}, "predecessor_changed", None),
        ({"source": "native"}, {"public_id": _OTHER}, "predecessor_changed", None),
        ({"source": "native"}, {"timestamp": _DAY}, "predecessor_changed", None),
        ({"source": "native"}, {}, "protected_source", None),
        ({"source": "calculated"}, {}, "protected_source", None),
        ({"timestamp": _WRITE, "volume": 4.0}, {}, "not_newer", None),
        ({}, {}, "unchanged", None),
        ({"volume": 4.0}, {}, "would_lose_observations", "lower_volume"),
        ({"trades": 8}, {}, "would_lose_observations", "lower_trades"),
        ({"volume": 4.0, "trades": 8}, {}, "would_lose_observations", "lower_volume+lower_trades"),
        ({"trades": None}, {}, "would_lose_observations", "unknown_current"),
        (
            {"volume": 4.0, "trades": None},
            {},
            "would_lose_observations",
            "lower_volume+unknown_current",
        ),
        ({"volume": inf}, {}, "would_lose_observations", "unknown_current"),
        ({"trades": inf}, {}, "would_lose_observations", "unknown_current"),
        ({"close": 10.0}, {}, "inputs_changed", None),
    ],
)
@pytest.mark.asyncio
async def test_first_guard_refusal_wins_and_does_not_take_the_lock(
    repository: SQLAlchemyRepository,
    stored: dict[str, object],
    predecessor_change: dict[str, object],
    status: str,
    reason: str | None,
) -> None:
    """Use a competing fingerprint refusal to check the order of the output guards."""
    row = _row()
    data = cast(CandleUpsertRow, dict(row) | {"timestamp": _AUTHOR} | stored)
    version = await _seed(repository, data)
    predecessor = cast(CandlePredecessor, dict(_predecessor(version)) | predecessor_change)
    expected = {
        _key(row): SynthesizedRebuildExpectation(predecessor=predecessor, inputs_sha256="stale")
    }
    for dry_run in (True, False):
        result = await repository.rebuild_synthesized_candles(
            [row], expected=expected, dry_run=dry_run
        )
        outcome = result.outcomes[_key(row)]
        assert (outcome.status, outcome.reason) == (status, reason)
        assert result.sample is None
    assert await _snapshot(repository) == [version]


@pytest.mark.asyncio
async def test_absence_must_match_the_vote_in_both_directions(
    repository: SQLAlchemyRepository,
) -> None:
    """Reject a lost predecessor and an output that appeared where the vote expected absence."""
    row = _row()
    key = _key(row)
    missing = CandlePredecessor(id=999, public_id=_LEGACY, timestamp=_AUTHOR)
    outcome = await repository.rebuild_synthesized_candles(
        [row], expected={key: _expectation(missing)}
    )
    assert outcome.outcomes[key].status == "predecessor_changed"
    await _seed(repository, row | {"timestamp": _AUTHOR, "source": "native"})
    outcome = await repository.rebuild_synthesized_candles([row], expected={key: _expectation()})
    assert outcome.outcomes[key].status == "predecessor_changed"


@pytest.mark.parametrize(
    "previous",
    [{"vwap": 10.5}, {"trades": 6}, {"complete": False}, {"price_basis": "mid"}, {"volume": 2.0}],
)
@pytest.mark.asyncio
async def test_business_fields_and_increasing_observations_are_versioned(
    repository: SQLAlchemyRepository, previous: CandleUpsertRow
) -> None:
    """Write meaningful changes in fields outside OHLCV as well as additional observations."""
    row = _row()
    version = await _seed(repository, row | {"timestamp": _AUTHOR} | previous)
    result = await repository.rebuild_synthesized_candles(
        [row], expected={_key(row): _expectation(_predecessor(version))}
    )
    assert result.outcomes[_key(row)].status == "replaced"
    closed, current = await _snapshot(repository)
    assert closed["known_to"] == _WRITE
    assert current["vwap"] == row["vwap"]
    assert current["trades"] == row["trades"]
    assert current["complete"]
    assert current["price_basis"] is None
    assert current["volume"] == row["volume"]


@pytest.mark.asyncio
async def test_input_queries_cover_half_open_grids_by_instrument_and_utc_day(
    repository: SQLAlchemyRepository,
) -> None:
    """Only current one-minute versions inside each individual window enter its fingerprint."""
    first = await _seed(repository, _row(timeframe="1m"))
    later = await _seed(repository, _row(_DAY + timedelta(minutes=10), "1m"))
    tomorrow = _DAY + timedelta(days=1)
    next_day = await _seed(repository, _row(tomorrow, "1m"))
    other = await _seed(repository, _row(timeframe="1m") | {"instrument_public_id": _OTHER})
    await _seed(repository, _row(_DAY + timedelta(minutes=5), "1m"))
    await _seed(repository, _row(_DAY + timedelta(minutes=1), "1m") | {"known_to": _WRITE})
    windows = [
        (_row(), first),
        (_row(_DAY + timedelta(minutes=10)), later),
        (_row(tomorrow), next_day),
        (_row() | {"instrument_public_id": _OTHER}, other),
    ]
    expected = {_key(row): _expectation(None, minute) for row, minute in windows}
    rows = [row for row, _ in windows]
    statements: list[str] = []

    def capture(
        connection: Connection,
        statement: Executable,
        multiparams: object,
        params: object,
        execution_options: object,
    ) -> None:
        """Record the ORM statements submitted inside the dry run."""
        del connection, multiparams, params, execution_options
        statements.append(str(statement))

    engine = repository.engine.sync_engine
    event.listen(engine, "before_execute", capture)
    try:
        result = await repository.rebuild_synthesized_candles(rows, expected=expected, dry_run=True)
    finally:
        event.remove(engine, "before_execute", capture)
    assert result.outcomes == {_key(row): SynthesizedRebuildOutcome("insert") for row in rows}
    assert len(statements) == 4
    assert all("candles.instrument_public_id =" in statement for statement in statements[1:])


async def _change_input(
    repository: SQLAlchemyRepository, change: str, minute: CandleVersionRow
) -> None:
    """Commit an appearance, a correction or a close of a minute in a voted grid."""
    if change == "appear":
        await _seed(repository, _row(_DAY + timedelta(minutes=1), "1m"))
        return
    async with repository.session() as session:
        await session.execute(
            update(Candle).where(Candle.id == minute["id"]).values(known_to=_WRITE)
        )
        await session.commit()
    if change == "correct":
        await _seed(repository, _row(timeframe="1m") | {"volume": 4.0})


@pytest.mark.parametrize("change", ["appear", "correct", "close"])
@pytest.mark.parametrize("during_fence", [False, True])
@pytest.mark.asyncio
async def test_any_input_change_skips_a_voted_window(
    repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
    change: str,
    during_fence: bool,
) -> None:
    """Check the same fingerprint after a vote and after the write's preflight."""
    row = _row()
    key = _key(row)
    minute = await _seed(repository, _row(timeframe="1m") | {"timestamp": _AUTHOR})
    expected = {key: _expectation(None, minute)}
    preview = await repository.rebuild_synthesized_candles([row], expected=expected, dry_run=True)
    assert preview.outcomes[key].status == "insert"
    fence = repository._begin_candle_reconciliation_write

    async def change_then_lock(session: AsyncSession, *, sqlite_write_lock: bool) -> None:
        """Let a competing minute writer commit before taking the real fence."""
        await _change_input(repository, change, minute)
        await fence(session, sqlite_write_lock=sqlite_write_lock)

    if during_fence:
        monkeypatch.setattr(repository, "_begin_candle_reconciliation_write", change_then_lock)
    else:
        await _change_input(repository, change, minute)
    sample = AsyncMock()
    monkeypatch.setattr(repository_module, "sample_other_client_backends", sample)
    result = await repository.rebuild_synthesized_candles([row], expected=expected)
    assert result.outcomes[key].status == "inputs_changed"
    assert result.sample is None
    sample.assert_not_called()
    assert {version["timeframe"] for version in await _snapshot(repository)} == {"1m"}


@pytest.mark.asyncio
async def test_locked_read_detects_same_timestamp_new_physical_predecessor(
    repository: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Never replace a different physical version even when both other identity fields match."""
    row = _row()
    older = row | {"timestamp": _AUTHOR, "close": 10.0, "public_id": _LEGACY}
    original = await _seed(repository, older)
    fence = repository._begin_candle_reconciliation_write

    async def supersede_then_lock(session: AsyncSession, *, sqlite_write_lock: bool) -> None:
        """Substitute another physical row under the same public identity and timestamp."""
        async with repository.session() as concurrent:
            await concurrent.execute(
                update(Candle).where(Candle.id == original["id"]).values(known_to=_WRITE)
            )
            await concurrent.commit()
        await _seed(repository, older)
        await fence(session, sqlite_write_lock=sqlite_write_lock)

    monkeypatch.setattr(repository, "_begin_candle_reconciliation_write", supersede_then_lock)
    key = _key(row)
    result = await repository.rebuild_synthesized_candles(
        [row], expected={key: _expectation(_predecessor(original))}
    )
    assert result.outcomes[key].status == "predecessor_changed"
    closed, current = await _snapshot(repository)
    assert closed["id"] == original["id"] != current["id"]
    assert _predecessor(current) == _predecessor(original) | {"id": current["id"]}
    assert current["timestamp"] == _AUTHOR


@pytest.mark.parametrize(
    ("changed", "reason"),
    [
        ({"volume": 4.0}, "lower_volume"),
        ({"trades": 8}, "lower_trades"),
        ({"volume": 4.0, "trades": 8}, "lower_volume+lower_trades"),
        ({"trades": None}, "unknown_current"),
        ({"volume": 4.0, "trades": inf}, "lower_volume+unknown_current"),
        ({"volume": inf}, "unknown_current"),
    ],
)
@pytest.mark.asyncio
async def test_observation_loss_is_checked_again_under_the_fence(
    repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
    changed: dict[str, object],
    reason: str,
) -> None:
    """Recheck the stored observation values even when the predecessor identity still matches."""
    row = _row()
    original = await _seed(repository, row | {"timestamp": _AUTHOR, "close": 10.0})
    fence = repository._begin_candle_reconciliation_write

    async def change_then_lock(session: AsyncSession, *, sqlite_write_lock: bool) -> None:
        """Change observations before the fence while leaving identifying columns intact."""
        async with repository.session() as concurrent:
            await concurrent.execute(
                update(Candle).where(Candle.id == original["id"]).values(**changed)
            )
            await concurrent.commit()
        await fence(session, sqlite_write_lock=sqlite_write_lock)

    monkeypatch.setattr(repository, "_begin_candle_reconciliation_write", change_then_lock)
    key = _key(row)
    result = await repository.rebuild_synthesized_candles(
        [row], expected={key: _expectation(_predecessor(original))}
    )
    assert result.outcomes[key] == SynthesizedRebuildOutcome("would_lose_observations", reason)
    assert await _snapshot(repository) == [dict(original) | changed]


@pytest.mark.asyncio
async def test_locked_rebuild_keeps_preflight_refusals_and_samples_after_staging(
    repository: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep a refused key refused even when eligible by the time the chunk takes the fence."""
    refused, eligible = _row(), _row(_DAY + timedelta(minutes=5))
    current = await _seed(repository, refused | {"timestamp": _AUTHOR, "source": "native"})
    fence = repository._begin_candle_reconciliation_write
    observed = ClientSample(2, _WRITE)

    async def delete_then_lock(session: AsyncSession, *, sqlite_write_lock: bool) -> None:
        """Allow a formerly protected key to become absent just before locking."""
        async with repository.session() as concurrent:
            await concurrent.execute(delete(Candle).where(Candle.id == current["id"]))
            await concurrent.commit()
        await fence(session, sqlite_write_lock=sqlite_write_lock)

    async def sample_staged_rows(session: AsyncSession) -> ClientSample:
        """Observe the staged insert in the still-open writer transaction."""
        result = await session.execute(select(Candle.open_at))
        assert result.scalars().all() == [eligible["open_at"]]
        return observed

    monkeypatch.setattr(repository, "_begin_candle_reconciliation_write", delete_then_lock)
    sample = AsyncMock(side_effect=sample_staged_rows)
    monkeypatch.setattr(repository_module, "sample_other_client_backends", sample)
    expected = {_key(refused): _expectation(), _key(eligible): _expectation()}
    result = await repository.rebuild_synthesized_candles([refused, eligible], expected=expected)
    assert result == SynthesizedRebuildResult(
        {
            _key(refused): SynthesizedRebuildOutcome("predecessor_changed"),
            _key(eligible): SynthesizedRebuildOutcome("inserted"),
        },
        observed,
    )
    sample.assert_awaited_once()


@pytest.mark.asyncio
async def test_sample_failure_rolls_back_both_insert_and_replacement(
    repository: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failure after staging changes must leave the entire chunk retryable."""
    row, absent = _row(), _row(_DAY + timedelta(minutes=5))
    version = await _seed(repository, row | {"timestamp": _AUTHOR, "close": 10.0})
    expected = {_key(row): _expectation(_predecessor(version)), _key(absent): _expectation()}
    with monkeypatch.context() as patch:
        patch.setattr(
            repository_module,
            "sample_other_client_backends",
            AsyncMock(side_effect=RuntimeError("sampling failed")),
        )
        with pytest.raises(RuntimeError, match="sampling failed"):
            await repository.rebuild_synthesized_candles([row, absent], expected=expected)
    assert await _snapshot(repository) == [version]
    result = await repository.rebuild_synthesized_candles([row, absent], expected=expected)
    assert result.outcomes == {
        _key(row): SynthesizedRebuildOutcome("replaced"),
        _key(absent): SynthesizedRebuildOutcome("inserted"),
    }


@pytest.mark.asyncio
async def test_client_sample_counts_visible_and_hidden_client_backends_in_one_read() -> None:
    """Check the PostgreSQL statement and timestamp mapping through a mocked caller session."""
    result = MagicMock()
    result.one.return_value = (3, _WRITE)
    session = MagicMock(spec=AsyncSession)
    session.get_bind.return_value.dialect.name = "postgresql"
    session.execute = AsyncMock(return_value=result)
    assert await sample_other_client_backends(session) == ClientSample(3, _WRITE)
    session.execute.assert_awaited_once()
    assert str(session.execute.call_args.args[0]) == (
        "SELECT count(*), clock_timestamp() FROM pg_stat_activity "
        "WHERE pid <> pg_backend_pid() AND "
        "(backend_type = 'client backend' OR "
        "(backend_type IS NULL AND usename IS NOT NULL AND datname IS NOT NULL))"
    )


@pytest.mark.parametrize("name", ["x" * 64, "snowman \u2603", "multi\nline", "DEL\x7f"])
def test_invalid_application_name_raises_before_creating_an_engine(
    monkeypatch: pytest.MonkeyPatch, name: str
) -> None:
    """Refuse names that would truncate or carry non-printable or non-ASCII characters."""
    create_engine = MagicMock()
    monkeypatch.setattr(repository_module, "create_async_engine", create_engine)
    with pytest.raises(ValueError, match="63 bytes of printable ASCII"):
        SQLAlchemyRepository("postgresql+asyncpg://localhost/scratch", application_name=name)
    create_engine.assert_not_called()


@pytest.mark.parametrize(
    ("application_name", "settings"),
    [(None, {"timezone": "UTC"}), ("x" * 63, {"timezone": "UTC", "application_name": "x" * 63})],
)
def test_postgres_engine_sets_the_given_startup_name_without_connecting(
    monkeypatch: pytest.MonkeyPatch, application_name: str | None, settings: dict[str, str]
) -> None:
    """Connection startup parameters preserve UTC and set a valid name for every connection."""
    create_engine = MagicMock()
    monkeypatch.setattr(repository_module, "create_async_engine", create_engine)
    url = "postgresql+asyncpg://localhost/scratch"
    SQLAlchemyRepository(url, application_name=application_name)
    assert create_engine.call_args.args == (url,)
    assert create_engine.call_args.kwargs["connect_args"] == {"server_settings": settings}


def test_named_postgres_engine_preserves_pool_limits_and_uses_the_url_scheme(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Apply the existing pool controls even with a dialect keyword in the database name."""
    create_engine = MagicMock()
    monkeypatch.setattr(repository_module, "create_async_engine", create_engine)
    monkeypatch.setenv("DB_POOL_SIZE", "1")
    monkeypatch.setenv("DB_MAX_OVERFLOW", "0")
    SQLAlchemyRepository("postgresql+asyncpg://localhost/sqlite_fixture", application_name="test")
    kwargs = create_engine.call_args.kwargs
    assert kwargs["connect_args"] == {
        "server_settings": {"timezone": "UTC", "application_name": "test"}
    }
    assert (kwargs["pool_size"], kwargs["max_overflow"]) == (1, 0)


def test_unrelated_dialects_keep_the_default_engine_arguments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep the constructor's existing fallback for a non-SQLite, non-PostgreSQL URL."""
    create_engine = MagicMock()
    monkeypatch.setattr(repository_module, "create_async_engine", create_engine)
    SQLAlchemyRepository("mysql+aiomysql://localhost/scratch")
    assert create_engine.call_args.kwargs["connect_args"] == {}
