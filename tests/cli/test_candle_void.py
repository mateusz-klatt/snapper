"""Check that candle-void argument refusals never disclose raw credential-shaped tokens."""

from pathlib import Path
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock

import pytest
from click.testing import Result
from sqlalchemy.exc import DBAPIError
from typer.testing import CliRunner

from snapper.application.updaters.historical.candle_repair import RepairRefusedError
from snapper.application.updaters.historical.candle_void import VoidContext
from snapper.application.updaters.historical.candle_void import VoidOutcome
from snapper.application.updaters.historical.candle_void import VoidPlan
from snapper.application.updaters.historical.candle_void import VoidRefusedError
from snapper.application.updaters.historical.candle_void import VoidRequest
from snapper.application.updaters.historical.candle_void import VoteBinding
from snapper.cli import candle_void
from snapper.cli.app import app
from snapper.data.repository import Repository

_SECRET = "synthetic-C5-password"
_URL = f"postgresql+asyncpg://synthetic-user:{_SECRET}@db.example:5432/synthetic"


def _context() -> VoidContext:
    """Build a command context that never opens a database connection."""
    return VoidContext(
        repository=cast(Repository, object()),
        database={"dialect": "postgresql", "host": "db.example", "database": "synthetic"},
        code_revision={"synthetic.py": "0" * 64},
    )


def _request(tmp_path: Path) -> VoidRequest:
    """Build a minimal valid application request."""
    return VoidRequest(
        instrument_public_ids=("synthetic-instrument",), evidence=(), output_dir=tmp_path / "output"
    )


def _plan() -> VoidPlan:
    """Build a deterministic dry-run result."""
    return VoidPlan(
        targets=(),
        keys=(),
        preview=b"preview",
        summary={"candles": 0},
        approval={"schema": "candle-void/1"},
        digest="a" * 64,
    )


def _invoke(monkeypatch: pytest.MonkeyPatch, execute: AsyncMock, arguments: list[str]) -> Result:
    """Invoke the real command with configuration and execution boundaries mocked."""

    def settings() -> SimpleNamespace:
        """Return only the synthetic database URL required by the command."""
        return SimpleNamespace(db_url=_URL)

    def configured_repository(db_url: str) -> Repository:
        """Return a connection-free repository double."""
        return cast(Repository, object())

    monkeypatch.setattr(candle_void, "get_bootstrap_settings", settings)
    monkeypatch.setattr(candle_void, "configured_repository", configured_repository)
    monkeypatch.setattr(candle_void, "_execute", execute)
    return CliRunner().invoke(app, ["void-candles", *arguments])


@pytest.mark.parametrize(
    "tail",
    [
        ["--unknown-option", _URL],
        ["--evidence", _URL],
    ],
)
def test_invalid_arguments_refuse_without_printing_raw_input(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, tail: list[str]
) -> None:
    """Malformed arguments refuse before bootstrap and redact stdout and stderr.

    Given invalid command arguments containing sensitive synthetic text.
    When the command parses the arguments.
    Then it refuses without printing the raw input.
    """

    def forbidden_bootstrap() -> None:
        """Fail if invalid input reaches configuration or any database."""
        pytest.fail("invalid arguments must refuse before bootstrap")

    monkeypatch.setattr(candle_void, "get_bootstrap_settings", forbidden_bootstrap)
    result = CliRunner().invoke(
        app,
        [
            "void-candles",
            "--instrument",
            "synthetic-instrument",
            "--output-dir",
            str(tmp_path / "output"),
            *tail,
        ],
    )
    assert result.exit_code == 1
    combined = result.stdout + result.stderr
    assert "refused" in combined
    assert _URL not in combined
    assert _SECRET not in combined
    assert "synthetic-user" not in combined
    assert not (tmp_path / "output").exists()


@pytest.mark.parametrize(
    "tail",
    [
        ["--confirm"],
        ["--voted-preview", "voted"],
        ["--expect-digest", "a" * 64],
        ["--confirm", "--voted-preview", "voted", "--expect-digest", "bad"],
        ["--evidence", "bad-shape"],
        ["--reconcile", "run"],
    ],
)
def test_write_option_refusals_happen_before_execution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, tail: list[str]
) -> None:
    """Incomplete or mixed write options refuse before repository setup or artifacts.

    Given incomplete or mixed write options.
    When the command parses them.
    Then refusal precedes execution and artifacts.
    """
    execute = AsyncMock(return_value=0)
    output = tmp_path / "output"
    arguments = [
        "--instrument",
        "synthetic-instrument",
        "--output-dir",
        str(output),
        *tail,
    ]

    result = _invoke(monkeypatch, execute, arguments)

    assert result.exit_code == 1
    assert "refused" in result.stdout + result.stderr
    execute.assert_not_awaited()
    assert not output.exists()


@pytest.mark.parametrize("missing", ["instrument", "output-dir"])
def test_required_options_refuse_before_execution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, missing: str
) -> None:
    """A dry run needs both an instrument and an output directory.

    Given a missing instrument or output directory.
    When the command parses it.
    Then refusal precedes execution.
    """
    execute = AsyncMock(return_value=0)
    arguments = (
        ["--output-dir", str(tmp_path / "output")]
        if missing == "instrument"
        else ["--instrument", "synthetic-instrument"]
    )

    result = _invoke(monkeypatch, execute, arguments)

    assert result.exit_code == 1
    assert "refused" in result.stdout + result.stderr
    execute.assert_not_awaited()


def test_valid_dry_run_reaches_execution_and_parses_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A valid dry run binds its evidence and reaches the application boundary.

    Given a valid dry run with evidence.
    When the command runs.
    Then parsed evidence reaches the application boundary.
    """
    execute = AsyncMock(return_value=0)
    evidence = tmp_path / "cessation.json"
    evidence.write_text("synthetic")

    result = _invoke(
        monkeypatch,
        execute,
        [
            "--instrument",
            "synthetic-instrument",
            "--output-dir",
            str(tmp_path / "output"),
            "--evidence",
            f"writer_cessation={evidence}",
        ],
    )

    assert result.exit_code == 0
    await_args = execute.await_args
    assert await_args is not None
    request = await_args.args[1]
    assert request.evidence[0].name == "writer_cessation"
    assert request.evidence[0].path == evidence


def test_valid_write_reaches_execution_with_a_normalized_digest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A confirmed run binds the voted directory and lowercase sha256.

    Given a confirmed vote with an uppercase digest.
    When the command runs.
    Then the vote binds its directory and lowercase digest.
    """
    execute = AsyncMock(return_value=0)

    result = _invoke(
        monkeypatch,
        execute,
        [
            "--instrument",
            "synthetic-instrument",
            "--output-dir",
            str(tmp_path / "output"),
            "--confirm",
            "--voted-preview",
            str(tmp_path / "voted"),
            "--expect-digest",
            "A" * 64,
        ],
    )

    assert result.exit_code == 0
    await_args = execute.await_args
    assert await_args is not None
    binding = await_args.args[2]
    assert binding.voted_dir == tmp_path / "voted"
    assert binding.expect_digest == "a" * 64


def test_valid_reconciliation_reaches_execution_without_a_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reconcile-only mode builds no plan request or vote binding.

    Given only a reconciliation directory.
    When the command runs.
    Then execution receives no plan or vote binding.
    """
    execute = AsyncMock(return_value=0)

    result = _invoke(monkeypatch, execute, ["--reconcile", str(tmp_path / "run")])

    assert result.exit_code == 0
    await_args = execute.await_args
    assert await_args is not None
    assert await_args.args[1] is None
    assert await_args.args[2] is None
    assert await_args.args[3] == tmp_path / "run"


async def test_execute_dry_run_prints_digest_and_always_disposes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The application dry run prints its digest and disposes repositories in finally.

    Given a dry-run application result.
    When it is executed.
    Then the digest is printed and repositories are disposed.
    """
    request = _request(tmp_path)
    plan = _plan()
    planner = AsyncMock(return_value=plan)
    disposer = AsyncMock()
    monkeypatch.setattr(candle_void, "plan_void", planner)
    monkeypatch.setattr(candle_void, "dispose_repositories", disposer)

    context = _context()
    code = await candle_void._execute(context, request, None, None)

    captured = capsys.readouterr()
    assert code == 0
    assert f"digest: {plan.digest}" in captured.out
    assert "DRY RUN: no candle was changed" in captured.out
    planner.assert_awaited_once_with(context, request)
    disposer.assert_awaited_once()


@pytest.mark.parametrize(("status", "expected"), [("acknowledged", 0), ("refused", 1)])
async def test_execute_write_reports_outcome_and_always_disposes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    status: str,
    expected: int,
) -> None:
    """Write exit status follows whether the voted versions are actually closed.

    Given acknowledged and refused write outcomes.
    When they are reported.
    Then exit status reflects closure and repositories are disposed.
    """
    request = _request(tmp_path)
    binding = VoteBinding(tmp_path / "voted", "a" * 64)
    writer = AsyncMock(return_value=VoidOutcome(status=status, record={"status": status}))
    disposer = AsyncMock()
    monkeypatch.setattr(candle_void, "write_void", writer)
    monkeypatch.setattr(candle_void, "dispose_repositories", disposer)

    code = await candle_void._execute(_context(), request, binding, None)

    captured = capsys.readouterr()
    assert code == expected
    assert f"outcome: {status}" in captured.out
    if expected:
        assert f"stopped: {status}" in captured.err
    writer.assert_awaited_once()
    disposer.assert_awaited_once()


async def test_execute_reconciliation_reports_outcome_and_always_disposes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Reconcile-only execution reports a non-success outcome and still disposes.

    Given a not-applied recovery outcome.
    When it is reported.
    Then exit is nonzero and repositories are disposed.
    """
    reconciler = AsyncMock(
        return_value=VoidOutcome(status="not_applied", record={"status": "not_applied"})
    )
    disposer = AsyncMock()
    monkeypatch.setattr(candle_void, "reconcile_void", reconciler)
    monkeypatch.setattr(candle_void, "dispose_repositories", disposer)

    code = await candle_void._execute(_context(), None, None, tmp_path / "run")

    captured = capsys.readouterr()
    assert code == 1
    assert "outcome: not_applied" in captured.out
    assert "stopped: not_applied" in captured.err
    reconciler.assert_awaited_once()
    disposer.assert_awaited_once()


def test_failure_text_contains_only_error_class_and_sqlstate() -> None:
    """Unexpected failures never expose statement or driver text.

    Given errors with sensitive text and a SQLSTATE.
    When failure text is formatted.
    Then only the class and SQLSTATE remain.
    """
    error = DBAPIError(_SECRET, {}, SimpleNamespace(sqlstate="55P03"))

    assert candle_void._failure_text(error) == "DBAPIError sqlstate=55P03"
    assert candle_void._failure_text(RuntimeError(_SECRET)) == "RuntimeError"
    assert _SECRET not in candle_void._failure_text(error)


@pytest.mark.parametrize(
    "error",
    [VoidRefusedError("synthetic refusal"), RepairRefusedError("synthetic refusal")],
)
def test_command_reports_expected_refusals(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, error: Exception
) -> None:
    """Application and shared repair refusals use the one-line refused path.

    Given application and shared repair refusals.
    When the command handles them.
    Then each uses the one-line refused path.
    """
    execute = AsyncMock(side_effect=error)

    result = _invoke(
        monkeypatch,
        execute,
        ["--instrument", "synthetic-instrument", "--output-dir", str(tmp_path / "output")],
    )

    assert result.exit_code == 1
    assert "refused: synthetic refusal" in result.stdout + result.stderr


def test_command_reports_unexpected_failure_without_sensitive_text(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The CLI prints only the unexpected error class and SQLSTATE.

    Given an unexpected database error with sensitive text.
    When the command handles it.
    Then only its class and SQLSTATE are printed.
    """
    execute = AsyncMock(side_effect=DBAPIError(_SECRET, {}, SimpleNamespace(sqlstate="55P03")))

    result = _invoke(
        monkeypatch,
        execute,
        ["--instrument", "synthetic-instrument", "--output-dir", str(tmp_path / "output")],
    )

    combined = result.stdout + result.stderr
    assert result.exit_code == 1
    assert "failed: DBAPIError sqlstate=55P03" in combined
    assert _SECRET not in combined
