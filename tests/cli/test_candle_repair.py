"""Tests for the ``snapper repair-candles`` operator command."""

import asyncio
import json
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from click.testing import Result
from loguru import logger
from sqlalchemy.exc import OperationalError
from typer.testing import CliRunner

from snapper.application.updaters.historical.candle_repair import RepairRefusedError
from snapper.application.updaters.historical.candle_repair import WriteReport
from snapper.cli import candle_repair as candle_repair_cli
from snapper.cli.app import app
from snapper.cli.operator_support import configured_repository
from snapper.data.repository import SQLAlchemyRepository
from tests.helpers.candle_repair import OPEN
from tests.helpers.candle_repair import XRP
from tests.helpers.candle_repair import candle
from tests.helpers.candle_repair import kraken_row
from tests.helpers.candle_repair import seed_instrument
from tests.helpers.candle_repair import trade
from tests.helpers.candle_repair import write_snapshot

_START = "2026-11-10T14:00:00+00:00"
_END = "2026-11-10T16:00:00Z"
_SENTINELS = ("SENTINEL_USER", "SENTINEL_PASSWORD", "SENTINEL_ACCOUNT")
_SENTINEL_URL = "postgresql+asyncpg://SENTINEL_USER:SENTINEL_PASSWORD@db.example:5432/snapper"


def _invoke(*arguments: str) -> Result:
    """Run the command."""
    return CliRunner().invoke(app, ["repair-candles", *arguments])


def _snapshot_arguments(snapshot: Path, output: Path, *extra: str) -> list[str]:
    """Return the arguments of a snapshot run for XRP over the usual window."""
    return [
        "--source",
        "snapshot",
        "--snapshot-dir",
        str(snapshot),
        "--symbol",
        "XRP-EUR",
        "--start",
        _START,
        "--end",
        _END,
        "--output-dir",
        str(output),
        *extra,
    ]


@pytest.fixture
def database(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> str:
    """Seed a SQLite database with the XRP symbol and one zero, and select it.

    Returns:
        The database URL.
    """
    url = f"sqlite+aiosqlite:///{tmp_path / 'repair.db'}"

    async def seed() -> None:
        """Create the schema and the rows."""
        repository = SQLAlchemyRepository(url)
        await repository.create_all()
        await seed_instrument(repository, XRP, "XRP-EUR", "XXRPZEUR")
        await repository.upsert_candles([candle(XRP, OPEN + timedelta(minutes=1))])
        await repository.engine.dispose()

    asyncio.run(seed())
    monkeypatch.setattr(
        candle_repair_cli, "get_bootstrap_settings", lambda: SimpleNamespace(db_url=url)
    )
    return url


@pytest.fixture
def snapshot(tmp_path: Path) -> Path:
    """Save a snapshot of three traded closed minutes and the open one.

    Returns:
        The snapshot root.
    """
    rows = [kraken_row(OPEN + timedelta(minutes=offset), offset + 1) for offset in range(3)]
    rows.append(kraken_row(OPEN + timedelta(minutes=3), 1))
    fetched = OPEN + timedelta(minutes=3, seconds=30)
    write_snapshot(tmp_path / "snap", "XXRPZEUR", rows, fetched.timestamp())
    return tmp_path / "snap"


@pytest.fixture
def no_database(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fail the test if the command reads its settings or builds a repository."""

    def forbidden(*arguments: object) -> object:
        """Refuse to be called."""
        raise AssertionError(f"database reached with {arguments!r}")

    monkeypatch.setattr(candle_repair_cli, "get_bootstrap_settings", forbidden)
    monkeypatch.setattr(candle_repair_cli, "configured_repository", forbidden)


def test_command_is_registered_with_its_own_argument_parser() -> None:
    """The command takes every argument, ``--help`` included, through its parser.

    Given: The CLI application.
    When: Its commands are listed.
    Then: ``repair-candles`` is registered with extra arguments allowed and no help option.
    """
    command = next(item for item in app.registered_commands if item.name == "repair-candles")
    assert command.context_settings == {
        "allow_extra_args": True,
        "ignore_unknown_options": True,
        "help_option_names": [],
    }


def test_help_lists_every_option(no_database: None) -> None:
    """``--help`` prints the parser's usage without touching the database.

    Given: The command.
    When: It is run with ``--help``.
    Then: It exits 0 and lists the source, window, output and write options.
    """
    result = _invoke("--help")
    assert result.exit_code == 0
    for option in (
        "--source",
        "--snapshot-dir",
        "--exchange",
        "--all-mapped",
        "--output-dir",
        "--voted-preview",
        "--expect-digest",
        "--max-consecutive-lock-timeouts",
    ):
        assert option in result.stdout


_SNAPSHOT = ["--source", "snapshot", "--snapshot-dir", "snap"]
_TRADES = ["--source", "trades", "--exchange", "kraken"]
_WINDOW = ["--start", _START, "--end", _END, "--output-dir", "out"]
_CONFIRM = ["--confirm", "--voted-preview", "voted", "--expect-digest", "a" * 64]


@pytest.mark.parametrize(
    ("arguments", "refusal"),
    [
        ([], "the following arguments are required"),
        ([*_SNAPSHOT, "--symbol", "XRP-EUR", *_WINDOW, "--bogus", "1"], "unrecognized"),
        (["--source", "archive", "--symbol", "X-Y", *_WINDOW], "invalid choice"),
        (["--source", "snapshot", "--symbol", "XRP-EUR", *_WINDOW], "--source snapshot takes"),
        (
            [*_SNAPSHOT, "--exchange", "kraken", "--symbol", "XRP-EUR", *_WINDOW],
            "--source snapshot takes",
        ),
        (["--source", "trades", "--symbol", "XRP-EUR", *_WINDOW], "--source trades takes"),
        (
            [*_TRADES, "--snapshot-dir", "snap", "--symbol", "XRP-EUR", *_WINDOW],
            "--source trades takes",
        ),
        ([*_TRADES, "--all-mapped", *_WINDOW], "--source trades takes"),
        ([*_SNAPSHOT, *_WINDOW], "either --symbol"),
        ([*_SNAPSHOT, "--symbol", "XRP-EUR", "--all-mapped", *_WINDOW], "either --symbol"),
        ([*_SNAPSHOT, "--symbol", " ", *_WINDOW], "--symbol must not be empty"),
        (
            [
                *_SNAPSHOT,
                "--symbol",
                "XRP-EUR",
                "--start",
                "yesterday",
                "--end",
                _END,
                "--output-dir",
                "out",
            ],
            "--start is not an ISO",
        ),
        (
            [
                *_SNAPSHOT,
                "--symbol",
                "XRP-EUR",
                "--start",
                _START,
                "--end",
                "later",
                "--output-dir",
                "out",
            ],
            "--end is not an ISO",
        ),
        (
            [
                *_SNAPSHOT,
                "--symbol",
                "XRP-EUR",
                "--start",
                "2026-11-10T14:00:00",
                "--end",
                _END,
                "--output-dir",
                "out",
            ],
            "UTC minute",
        ),
        (
            [
                *_SNAPSHOT,
                "--symbol",
                "XRP-EUR",
                "--start",
                "2026-11-10T14:00:30Z",
                "--end",
                _END,
                "--output-dir",
                "out",
            ],
            "UTC minute",
        ),
        (
            [
                *_SNAPSHOT,
                "--symbol",
                "XRP-EUR",
                "--start",
                _END,
                "--end",
                _END,
                "--output-dir",
                "out",
            ],
            "must precede",
        ),
        (
            [*_SNAPSHOT, "--symbol", "XRP-EUR", *_WINDOW, "--max-rows", "5"],
            "--max-rows only apply with --confirm",
        ),
        (
            [
                *_SNAPSHOT,
                "--symbol",
                "XRP-EUR",
                *_WINDOW,
                "--expect-digest",
                "a" * 64,
                "--chunk-pause-seconds",
                "0",
            ],
            "--expect-digest, --chunk-pause-seconds only apply",
        ),
        ([*_SNAPSHOT, "--symbol", "XRP-EUR", *_WINDOW, "--confirm"], "--confirm needs"),
        (
            [
                *_SNAPSHOT,
                "--symbol",
                "XRP-EUR",
                *_WINDOW,
                "--confirm",
                "--voted-preview",
                "voted",
                "--expect-digest",
                "xyz",
            ],
            "--confirm needs",
        ),
        (
            [*_SNAPSHOT, "--symbol", "XRP-EUR", *_WINDOW, *_CONFIRM, "--max-rows", "0"],
            "--max-rows must be positive",
        ),
        (
            [*_SNAPSHOT, "--symbol", "XRP-EUR", *_WINDOW, *_CONFIRM, "--chunk-pause-seconds", "-1"],
            "--max-rows must be positive",
        ),
        (
            [
                *_SNAPSHOT,
                "--symbol",
                "XRP-EUR",
                *_WINDOW,
                *_CONFIRM,
                "--chunk-pause-seconds",
                "nan",
            ],
            "--max-rows must be positive",
        ),
        (
            [
                *_SNAPSHOT,
                "--symbol",
                "XRP-EUR",
                *_WINDOW,
                *_CONFIRM,
                "--max-consecutive-lock-timeouts",
                "0",
            ],
            "at least 1",
        ),
        (
            [*_SNAPSHOT, "--symbol", "XRP-EUR", *_WINDOW, *_CONFIRM, "--max-rows", "many"],
            "invalid int value",
        ),
    ],
)
def test_invalid_options_refuse_before_the_database(
    no_database: None, arguments: list[str], refusal: str
) -> None:
    """Every malformed request refuses with one line before settings or database are read.

    Given: A missing, unknown, contradictory or malformed option.
    When: The command runs.
    Then: It exits 1 with a ``refused:`` line naming the problem.
    """
    result = _invoke(*arguments)
    assert result.exit_code == 1, result.stdout
    assert result.stderr.startswith("refused: ")
    assert refusal in result.stderr


def test_used_output_directory_refuses_before_a_repository_is_built(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, snapshot: Path
) -> None:
    """The output directory must be new or empty.

    Given: An output directory that already holds a file, and a path that is a file.
    When: The command runs with each.
    Then: It refuses both without building a repository.
    """
    monkeypatch.setattr(
        candle_repair_cli,
        "get_bootstrap_settings",
        lambda: SimpleNamespace(db_url="sqlite+aiosqlite:///unused.db"),
    )

    def forbidden(db_url: str) -> SQLAlchemyRepository:
        """Refuse to be called."""
        raise AssertionError(db_url)

    monkeypatch.setattr(candle_repair_cli, "configured_repository", forbidden)
    used = tmp_path / "used"
    used.mkdir()
    (used / "preview.csv").write_text("old")
    first = _invoke(*_snapshot_arguments(snapshot, used))
    second = _invoke(*_snapshot_arguments(snapshot, used / "preview.csv"))
    assert (first.exit_code, second.exit_code) == (1, 1)
    assert "refused: --output-dir must be empty" in first.stderr
    assert "refused: --output-dir is not a directory" in second.stderr


def test_dry_run_then_confirmed_write_then_equal_rerun(
    database: str, snapshot: Path, tmp_path: Path
) -> None:
    """The operator flow previews, writes the voted rows and previews equal minutes.

    Given: A snapshot with two absent minutes and one zero.
    When: The dry run runs, its digest is passed to a confirmed write, and the dry run
        runs again.
    Then: The dry run writes its three files and prints the digest; the write applies
        the three rows; the second dry run plans nothing but equal minutes.
    """
    dry = _invoke(*_snapshot_arguments(snapshot, tmp_path / "dry"))
    assert dry.exit_code == 0, dry.stderr
    assert "target database: dialect=sqlite" in dry.stdout
    assert "DRY RUN: nothing was written" in dry.stdout
    summary = json.loads((tmp_path / "dry" / "summary.json").read_text())
    assert f"digest: {summary['digest']}" in dry.stdout
    assert summary["actions"]["insert"] == 2 and summary["actions"]["correct"] == 1

    write = _invoke(
        *_snapshot_arguments(snapshot, tmp_path / "write"),
        "--confirm",
        "--voted-preview",
        str(tmp_path / "dry"),
        "--expect-digest",
        summary["digest"].upper(),
        "--chunk-pause-seconds",
        "0",
    )
    assert write.exit_code == 0, write.stderr
    assert f"voted digest: {summary['digest']}" in write.stdout
    assert "stop reason: completed" in write.stdout
    receipt = json.loads((tmp_path / "write" / "receipt.json").read_text())
    assert receipt["totals"]["applied"] == 3

    rerun = _invoke(*_snapshot_arguments(snapshot, tmp_path / "rerun"))
    assert rerun.exit_code == 0, rerun.stderr
    assert json.loads((tmp_path / "rerun" / "summary.json").read_text())["actions"]["equal"] == 3


def test_stopped_write_exits_with_one_and_points_at_the_receipt(
    database: str, snapshot: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A write that stops before its end is a failure for the operator.

    Given: A write that stops after consecutive lock timeouts.
    When: The command runs it.
    Then: It prints the outcome, names the stop on stderr and exits 1.
    """

    async def stopped(*arguments: object) -> WriteReport:
        """Report a stopped write."""
        del arguments
        return WriteReport("a" * 64, "b" * 64, "lock_timeouts", {"applied": 0})

    monkeypatch.setattr(candle_repair_cli, "write_repair", stopped)
    result = _invoke(*_snapshot_arguments(snapshot, tmp_path / "write"), *_CONFIRM)
    assert result.exit_code == 1
    assert "stop reason: lock_timeouts" in result.stdout
    assert "stopped: lock_timeouts; see receipt.json" in result.stderr


@pytest.mark.parametrize(
    ("error", "message"),
    [
        (RepairRefusedError("voted approval.json differs"), "refused: voted approval.json differs"),
        (
            OperationalError(
                "SELECT 1", {}, Exception("password authentication failed for SENTINEL_USER")
            ),
            "failed: OperationalError",
        ),
        (
            OperationalError("SELECT 1", {}, SimpleNamespace(sqlstate="57014")),
            "failed: OperationalError sqlstate=57014",
        ),
        (ValueError("SENTINEL_PASSWORD"), "failed: ValueError"),
    ],
)
@pytest.mark.usefixtures("database")
def test_failures_are_reported_by_class_without_their_text(
    snapshot: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    error: Exception,
    message: str,
) -> None:
    """A refusal keeps its own text; any other failure is named by class and SQLSTATE.

    Given: A dry run that refuses or fails with texts that carry sentinels.
    When: The command runs it.
    Then: It exits 1 with the expected line and no sentinel anywhere.
    """

    async def failing(*arguments: object) -> object:
        """Raise the configured failure."""
        del arguments
        raise error

    monkeypatch.setattr(candle_repair_cli, "plan_repair", failing)
    result = _invoke(*_snapshot_arguments(snapshot, tmp_path / "out"))
    assert result.exit_code == 1
    assert result.stderr.strip() == message
    assert not any(sentinel in result.stdout + result.stderr for sentinel in _SENTINELS)


@pytest.fixture
def captured_logs() -> Iterator[list[str]]:
    """Capture every log message written while the test runs.

    Yields:
        The formatted messages.
    """
    messages: list[str] = []
    sink = logger.add(lambda message: messages.append(str(message)), level="TRACE")
    try:
        yield messages
    finally:
        logger.remove(sink)


def test_no_credential_reaches_output_files_or_logs(
    database: str,
    snapshot: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    captured_logs: list[str],
) -> None:
    """A credential-bearing database URL never appears in output, files or logs.

    Given: A configured PostgreSQL URL with user and password sentinels and the allowed
        ``ssl`` option, and a repository that actually reads the seeded SQLite database.
    When: A dry run, a run whose URL carries a query sentinel in a refused option and a
        refused unsupported-dialect run are made.
    Then: The approval names host, port and database only, the refusals name no value,
        and no sentinel appears on stdout, stderr, in any output file or in any log message.
    """
    monkeypatch.setattr(
        candle_repair_cli,
        "get_bootstrap_settings",
        lambda: SimpleNamespace(db_url=_SENTINEL_URL + "?ssl=require"),
    )
    monkeypatch.setattr(
        candle_repair_cli, "configured_repository", lambda db_url: configured_repository(database)
    )
    result = _invoke(*_snapshot_arguments(snapshot, tmp_path / "out"))
    assert result.exit_code == 0, result.stderr
    approval = json.loads((tmp_path / "out" / "approval.json").read_text())
    assert approval["database"] == {
        "dialect": "postgresql",
        "host": "db.example",
        "port": 5432,
        "database": "snapper",
    }
    monkeypatch.setattr(
        candle_repair_cli,
        "get_bootstrap_settings",
        lambda: SimpleNamespace(db_url=_SENTINEL_URL + "?application_name=SENTINEL_ACCOUNT"),
    )
    optioned = _invoke(*_snapshot_arguments(snapshot, tmp_path / "optioned"))
    assert optioned.exit_code == 1
    assert optioned.stderr.strip() == (
        "refused: PostgreSQL target URL options are not allowed: application_name"
    )
    monkeypatch.setattr(
        candle_repair_cli,
        "get_bootstrap_settings",
        lambda: SimpleNamespace(db_url="mysql+aiomysql://SENTINEL_USER:SENTINEL_PASSWORD@h/db"),
    )
    refused = _invoke(*_snapshot_arguments(snapshot, tmp_path / "refused"))
    assert refused.exit_code == 1
    assert refused.stderr.strip() == "refused: database dialect mysql is not supported"
    texts = [result.stdout, result.stderr, refused.stdout, refused.stderr, *captured_logs]
    texts.extend([optioned.stdout, optioned.stderr])
    texts.extend(path.read_text() for path in (tmp_path / "out").iterdir())
    assert not any(sentinel in text for sentinel in _SENTINELS for text in texts)
    for path in (tmp_path / "out").iterdir():
        assert path.stat().st_mode & 0o777 == 0o600


def test_trades_dry_run_rebuilds_bars_from_stored_trades(database: str, tmp_path: Path) -> None:
    """The trades source plans from the stored trades of the requested symbol.

    Given: Stored trades for two minutes, the second of which holds a flat zero.
    When: A trades dry run covers both minutes.
    Then: It plans one insert and one correction, both calculated.
    """

    async def store_trades() -> None:
        """Store the two trades."""
        repository = SQLAlchemyRepository(database)
        await repository.upsert_trades(
            [
                trade(XRP, OPEN + timedelta(seconds=10), 1.0, 2.0, "t1"),
                trade(XRP, OPEN + timedelta(minutes=1, seconds=5), 1.5, 1.0, "t2"),
            ]
        )
        await repository.engine.dispose()

    asyncio.run(store_trades())
    result = _invoke(
        "--source",
        "trades",
        "--exchange",
        "kraken",
        "--symbol",
        "XRP-EUR",
        "--start",
        _START,
        "--end",
        _END,
        "--output-dir",
        str(tmp_path / "out"),
    )
    assert result.exit_code == 0, result.stderr
    preview = (tmp_path / "out" / "preview.csv").read_text().splitlines()[1:]
    assert [line.split(",")[3] for line in preview] == ["insert", "correct"]
    assert {line.split(",")[10] for line in preview} == {"calculated"}
