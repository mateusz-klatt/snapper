"""Tests for the read-only ``snapper check-candle-identities`` command."""

import asyncio
import json
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid7

import pytest
from click.testing import Result
from typer.testing import CliRunner

from snapper.cli import candle_identity_check
from snapper.cli.app import app
from snapper.core.candle_identity import CANDLE_IDENTITY_ALGORITHM
from snapper.core.candle_identity import deterministic_candle_public_id
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import CandleUpsertRow

_INSTRUMENT = "00000000-0000-4000-8000-0000000a0001"
_SESSION = "00000000-0000-4000-8000-0000000a0002"
_OPEN = datetime(2026, 11, 10, 14, 0, tzinfo=UTC)
_FROM = "2026-11-10T14:00:00Z"
_TO = "2026-11-10T15:00:00Z"


def _row(timeframe: str, open_at: datetime, public_id: str) -> CandleUpsertRow:
    """Build one complete native row with a chosen public id.

    Args:
        timeframe: Candle timeframe label.
        open_at: Candle opening.
        public_id: Stored public id.

    Returns:
        A row ready for :meth:`SQLAlchemyRepository.upsert_candles`.
    """
    return {
        "instrument_public_id": _INSTRUMENT,
        "open_at": open_at,
        "timestamp": open_at + timedelta(hours=1),
        "timeframe": timeframe,
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
        "session_id": _SESSION,
        "sequence_id": 1,
        "public_id": public_id,
    }


def _seed(url: str, rows: list[CandleUpsertRow]) -> None:
    """Create the schema and store the rows.

    Args:
        url: SQLite database URL.
        rows: Rows to store.
    """

    async def seed() -> None:
        """Create the schema and the rows."""
        repository = SQLAlchemyRepository(url)
        await repository.create_all()
        await repository.upsert_candles(rows)
        await repository.engine.dispose()

    asyncio.run(seed())


def _select(monkeypatch: pytest.MonkeyPatch, url: str) -> None:
    """Point the command's bootstrap settings at a database.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        url: Database URL the command reads.
    """
    monkeypatch.setattr(
        candle_identity_check, "get_bootstrap_settings", lambda: SimpleNamespace(db_url=url)
    )


def _invoke(*arguments: str) -> Result:
    """Run the command with the given arguments.

    Args:
        *arguments: Command-line arguments after the command name.

    Returns:
        The CLI result.
    """
    return CliRunner().invoke(app, ["check-candle-identities", *arguments])


@pytest.fixture
def no_database(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fail the test if the command reads its settings.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """

    def forbidden() -> object:
        """Refuse to be called.

        Raises:
            AssertionError: Always.
        """
        raise AssertionError("database reached")

    monkeypatch.setattr(candle_identity_check, "get_bootstrap_settings", forbidden)


def test_reports_counts_per_timeframe_and_fails_on_a_mismatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A random id inside the range is a mismatch; keys outside it are not read.

    Given: A deterministic 1m key and a random-id 5m key inside the range, and a
        random-id key opening before it.
    When: The range is checked.
    Then: The report counts both keys in range, names the 5m key as the only
        mismatch, carries the algorithm, module hashes and database identity, and
        the command exits 1.
    """
    url = f"sqlite+aiosqlite:///{tmp_path / 'check.db'}"
    random_id = str(uuid7())
    _seed(
        url,
        [
            _row("1m", _OPEN, deterministic_candle_public_id(_INSTRUMENT, "1m", _OPEN)),
            _row("5m", _OPEN, random_id),
            _row("1m", _OPEN - timedelta(minutes=1), str(uuid7())),
        ],
    )
    _select(monkeypatch, url)

    result = _invoke("--from", _FROM, "--to", _TO)

    assert result.exit_code == 1
    report = json.loads(result.output)
    assert report["algorithm"] == CANDLE_IDENTITY_ALGORITHM
    assert report["database"].startswith("dialect=sqlite")
    assert set(report["modules"]) == {
        "snapper/core/candle_identity.py",
        "snapper/data/repository.py",
        "snapper/cli/candle_identity_check.py",
    }
    assert all(len(digest) == 64 for digest in report["modules"].values())
    assert (report["from"], report["to"]) == (_OPEN.isoformat(), "2026-11-10T15:00:00+00:00")
    assert report["timeframes"] == {
        "1m": {"scanned": 1, "mismatched": 0},
        "5m": {"scanned": 1, "mismatched": 1},
    }
    assert (report["scanned"], report["mismatched"]) == (2, 1)
    assert report["sample"] == [
        {
            "instrument_public_id": _INSTRUMENT,
            "timeframe": "5m",
            "open_at": _OPEN.isoformat(),
            "public_id": random_id,
        }
    ]


def test_passes_when_every_key_is_deterministic_and_defaults_the_end_to_now(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Deterministic keys pass, and an omitted end is the instant the command starts.

    Given: Only deterministic keys, and no --to.
    When: The range from before them is checked.
    Then: The command exits 0, mismatches nothing, and its end is after its start.
    """
    url = f"sqlite+aiosqlite:///{tmp_path / 'check.db'}"
    _seed(url, [_row("1m", _OPEN, deterministic_candle_public_id(_INSTRUMENT, "1m", _OPEN))])
    _select(monkeypatch, url)

    result = _invoke("--from", _FROM)

    assert result.exit_code == 0
    report = json.loads(result.output)
    assert (report["scanned"], report["mismatched"], report["sample"]) == (1, 0, [])
    assert datetime.fromisoformat(report["to"]) > _OPEN


def test_the_sample_is_bounded_but_every_mismatch_is_counted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """More mismatches than the sample holds are all counted.

    Given: One more random-id key than the sample size.
    When: The range is checked.
    Then: Every key counts as a mismatch, and the sample stops at its bound.
    """
    url = f"sqlite+aiosqlite:///{tmp_path / 'check.db'}"
    count = candle_identity_check.MISMATCH_SAMPLE_SIZE + 1
    _seed(
        url, [_row("1m", _OPEN + timedelta(minutes=index), str(uuid7())) for index in range(count)]
    )
    _select(monkeypatch, url)

    result = _invoke("--from", _FROM, "--to", _TO)

    report = json.loads(result.output)
    assert report["mismatched"] == count
    assert len(report["sample"]) == candle_identity_check.MISMATCH_SAMPLE_SIZE


@pytest.mark.usefixtures("no_database")
@pytest.mark.parametrize(
    "arguments",
    [
        ("--from", "yesterday"),
        ("--from", "2026-11-10T14:00:00"),
        ("--from", "2026-11-10T15:00:00+01:00"),
        ("--from", _FROM, "--to", "later"),
        ("--from", _FROM, "--to", _FROM),
        ("--from", _TO, "--to", _FROM),
    ],
)
def test_refuses_a_bound_that_is_not_a_utc_instant_or_an_empty_range(
    arguments: tuple[str, ...],
) -> None:
    """A malformed bound or an empty range is refused before any database work.

    Given: Free text, a naive time, a non-UTC offset, or a --to not after --from.
    When: The command runs.
    Then: It exits 1 without reading its settings.
    """
    result = _invoke(*arguments)

    assert result.exit_code == 1
