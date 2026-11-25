"""Exercise command registration, argument checks and the named single database connection."""

import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from uuid import UUID

import pytest
from click.testing import Result
from typer.testing import CliRunner

from snapper.application.updaters.historical import candle_repair as repair_app
from snapper.application.updaters.historical.candle_repair import RepairRefusedError
from snapper.application.updaters.historical.candle_repair import RunContext
from snapper.application.updaters.historical.candle_repair import WriteReport
from snapper.application.updaters.historical.synthesized_candle_rebuild import RebuildRequest
from snapper.cli import operator_support
from snapper.cli import synthesized_candle_rebuild as cli
from snapper.cli.app import app
from snapper.core.json_types import JsonObject
from snapper.data.repository import SQLAlchemyRepository

_END = "2026-11-11T00:00:00Z"
_SECRET = "SENTINEL_DATABASE_PASSWORD"
_URL = f"postgresql+asyncpg://fixture:{_SECRET}@db.example:5432/scratch"


def _arguments(output: Path, end: str = _END) -> list[str]:
    """Construct the required options without reading the evidence yet."""
    return [
        "--repair-run",
        "one",
        "--repair-run",
        "two",
        "--run-records",
        "runs.json",
        "--end",
        end,
        "--output-dir",
        str(output),
    ]


def _invoke(*arguments: str) -> Result:
    """Invoke the registered command through the actual Snapper application."""
    return CliRunner().invoke(app, ["rebuild-synthesized-candles", *arguments])


def test_help_describes_live_edge_verification_and_the_run_order_note(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reach help without reading settings and show both distinct end constraints.

    Given: The rebuild CLI and a settings loader that must stay unused.
    When: Request command help.
    Then: Help describes verification and the live-edge and run-order bounds without loading
        settings.
    """
    settings = MagicMock()
    monkeypatch.setattr(cli, "get_bootstrap_settings", settings)
    result = _invoke("--help")
    assert result.exit_code == 0
    assert "180 minutes" in result.stdout
    assert "719 minutes" in result.stdout
    assert "--verify-receipt" in result.stdout
    settings.assert_not_called()


@pytest.mark.parametrize(
    ("end", "extra", "message"),
    [
        ("invalid", [], "--end is not an ISO 8601 time"),
        ("2026-11-11T00:00:00+02:00", [], "--end must be a UTC minute boundary"),
        ("2026-11-11T00:00:01Z", [], "--end must be a UTC minute boundary"),
        ("2999-11-11T00:00:00Z", [], "--end must be at least 180 minutes before the run"),
        (_END, ["--verify-record", "rec.json"], "must be supplied together"),
        (_END, ["--verify-receipt", "write-dir"], "must be supplied together"),
        (
            _END,
            ["--verify-receipt", "write-dir", "--verify-record", "r.json", "--confirm"],
            "only available in a dry run",
        ),
        (_END, ["--max-rows", "1"], "only apply with --confirm"),
        (_END, ["--confirm"], "--confirm needs --voted-preview"),
        (_END, ["--unknown", _SECRET], "invalid rebuild options"),
    ],
)
def test_bad_arguments_refuse_without_reading_settings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, end: str, extra: list[str], message: str
) -> None:
    """Reject malformed command-line evidence and keep arbitrary option text out of the output.

    Given: Malformed options, including an option value containing a credential sentinel.
    When: Invoke the rebuild command.
    Then: The command refuses before loading settings and keeps the sentinel out of its output.
    """
    settings = MagicMock()
    monkeypatch.setattr(cli, "get_bootstrap_settings", settings)
    result = _invoke(*_arguments(tmp_path / "out", end), *extra)
    assert result.exit_code == 1
    assert "refused:" in result.stderr and message in result.stderr
    assert _SECRET not in result.output
    settings.assert_not_called()


@pytest.fixture
def named_configuration(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Record the name with pool settings clamped before construction."""
    names = []

    def configure(url: str, *, application_name: str) -> SQLAlchemyRepository:
        """Capture startup arguments without allowing a database connection."""
        assert url == _URL
        assert os.environ["DB_POOL_SIZE"] == "1"
        assert os.environ["DB_MAX_OVERFLOW"] == "0"
        names.append(application_name)
        return MagicMock(spec=SQLAlchemyRepository)

    monkeypatch.setenv("DB_POOL_SIZE", "8")
    monkeypatch.delenv("DB_MAX_OVERFLOW", raising=False)
    monkeypatch.setattr(cli, "configured_repository", configure)
    monkeypatch.setattr(cli, "get_bootstrap_settings", lambda: SimpleNamespace(db_url=_URL))
    monkeypatch.setattr(cli, "dispose_repositories", AsyncMock())
    return names


@pytest.mark.parametrize("verify", [False, True])
def test_dry_run_is_named_and_passes_both_recorded_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, named_configuration: list[str], verify: bool
) -> None:
    """Use one named connection and print only short counts and the credential-free target.

    Given: Two recorded runs, a configured database URL and optional verification arguments.
    When: Invoke a dry run through the rebuild command.
    Then: Both runs reach the planner and the CLI uses a single named connection and safe summary
        output.
    """
    summary: JsonObject = {
        "actions": {"insert": 1},
        "by_timeframe": {},
        "by_day": {},
        "runs": [
            {"run_id": "test", "applied": 1, "applied_superseded": 0, "resolved_uncertain": 0}
        ],
    }
    if verify:
        summary["verification"] = {"write-run": {"counts": {"verified": 1, "regressed": 0}}}
    plan = AsyncMock(return_value=cli.rebuild.PlanArtifacts("a" * 64, summary))
    monkeypatch.setattr(cli.rebuild, "plan_rebuild", plan)
    args = ["--verify-receipt", "write", "--verify-record", "binding.json"] if verify else []
    result = _invoke(*_arguments(tmp_path / "out"), *args)
    assert result.exit_code == 0, result.output
    assert "db.example database=scratch" in result.stdout
    assert "digest: " + "a" * 64 in result.stdout
    assert ("verified" in result.stdout) is verify
    assert ("recorded snapshots" in result.stdout) is verify
    assert _SECRET not in result.output
    assert os.environ["DB_POOL_SIZE"] == "8" and "DB_MAX_OVERFLOW" not in os.environ
    context: RunContext
    request: RebuildRequest
    context, request = plan.call_args.args
    assert named_configuration == [f"snapper:rebuild-synth:{context.run_id}"]
    assert UUID(context.run_id).version == 7
    assert request.repair_runs == (Path("one"), Path("two"))
    assert bool(request.verification) is verify
    assert set(context.code_revision) == {
        "snapper/data/repository.py",
        "snapper/messaging/publishers/candle_aggregator.py",
        "snapper/messaging/publishers/base.py",
        "snapper/messaging/publishers/native_candle_finalizer.py",
        "snapper/application/updaters/historical/guarded_candle_write.py",
        "snapper/application/updaters/historical/candle_repair.py",
        "snapper/application/updaters/historical/synthesized_candle_rebuild.py",
        "snapper/cli/operator_support.py",
        "snapper/cli/synthesized_candle_rebuild.py",
    }


@pytest.mark.parametrize(
    ("stop", "code"), [("completed", 0), ("max_rows", 0), ("unique_violation", 1)]
)
def test_write_exit_status_and_invocation_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    named_configuration: list[str],
    stop: str,
    code: int,
) -> None:
    """Report stable write statuses using the same generated id as the configured repository.

    Given: A stubbed confirmed write with a complete or stopped outcome.
    When: Invoke the rebuild command with the voted preview.
    Then: Its exit status reflects the outcome and the write run ID matches the connection name.
    """
    write = AsyncMock(return_value=WriteReport("a" * 64, "b" * 64, stop, {"applied": 1}))
    monkeypatch.setattr(cli.rebuild, "write_rebuild", write)
    result = _invoke(
        *_arguments(tmp_path / "out"),
        "--confirm",
        "--voted-preview",
        "voted",
        "--expect-digest",
        "a" * 64,
    )
    assert result.exit_code == code
    assert f"stop reason: {stop}" in result.stdout
    assert ("see receipt.json" in result.stderr) is (code == 1)
    context = write.call_args.args[0]
    assert named_configuration == [f"snapper:rebuild-synth:{context.run_id}"]


@pytest.mark.parametrize(
    ("error", "message"),
    [
        (RepairRefusedError("invalid evidence"), "refused: invalid evidence"),
        (RuntimeError(_SECRET), "failed: RuntimeError"),
    ],
)
def test_errors_dispose_the_repository_without_echoing_credentials(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    named_configuration: list[str],
    error: Exception,
    message: str,
) -> None:
    """Use a safe refusal or error class and always dispose the repository.

    Given: A safe refusal or an unexpected failure with credential text.
    When: Invoke the rebuild command.
    Then: The repository is disposed and only safe failure details reach the output.
    """
    monkeypatch.setattr(cli.rebuild, "plan_rebuild", AsyncMock(side_effect=error))
    result = _invoke(*_arguments(tmp_path / "out"))
    assert result.exit_code == 1
    assert result.stderr.strip() == message
    assert _SECRET not in result.output
    assert len(named_configuration) == 1


def test_shared_operator_source_changes_both_command_revisions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bind the exact imported operator-helper bytes into both approval digests.

    Given: Both command revisions computed from the installed shared helper.
    When: Change only the helper source and recompute the revisions.
    Then: Both revisions change only in the shared helper's digest.
    """
    changed_source = tmp_path / "operator_support.py"
    original = Path(str(operator_support.__file__))
    before_repair = repair_app.code_revision([])
    before_rebuild = cli.rebuild.code_revision(cli)
    changed_source.write_bytes(original.read_bytes() + b"\n")
    monkeypatch.setattr(operator_support, "__file__", str(changed_source))
    after_repair = repair_app.code_revision([])
    after_rebuild = cli.rebuild.code_revision(cli)
    assert {key for key, digest in before_repair.items() if digest != after_repair[key]} == {
        "snapper/cli/operator_support.py"
    }
    assert {key for key, digest in before_rebuild.items() if digest != after_rebuild[key]} == {
        "snapper/cli/operator_support.py"
    }
