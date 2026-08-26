"""CLI tests for bounded read-only candle audits."""

import asyncio
import json
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from types import SimpleNamespace
from typing import NoReturn
from typing import cast
from unittest.mock import AsyncMock

import pytest
from click.testing import Result
from typer.testing import CliRunner

import snapper.cli.candle_audit as candle_audit_cli
from snapper.application.data_quality.candle_audit import CandleAnomaly
from snapper.application.data_quality.candle_audit import CandleAnomalyType
from snapper.application.data_quality.candle_audit_service import CandleAuditReport
from snapper.application.data_quality.candle_audit_service import CandleAuditRequest
from snapper.application.data_quality.candle_audit_service import CandleAuditRequestError
from snapper.application.data_quality.candle_audit_service import CandleAuditTargetError
from snapper.cli.app import app
from snapper.core.types import AllExchange
from snapper.data.repository import Repository

_EXCHANGE: AllExchange = "polygon"
_START = datetime(2026, 7, 1, 13, 30, tzinfo=UTC)
_END = _START + timedelta(minutes=1)
_AS_OF = _END + timedelta(minutes=1)


@pytest.fixture
def runner() -> CliRunner:
    """Provide an isolated Typer runner."""
    return CliRunner()


def _request() -> CandleAuditRequest:
    """Build the request represented by the common CLI arguments."""
    return CandleAuditRequest(
        exchange=_EXCHANGE,
        symbol="AAPL",
        timeframe="1m",
        window_start=_START,
        window_end=_END,
        as_of=_AS_OF,
    )


def _report(*anomalies: CandleAnomaly) -> CandleAuditReport:
    """Build one completed report for CLI rendering tests."""
    return CandleAuditReport(
        request=_request(),
        slots_expected=2,
        candles_scanned=2,
        oldest_open_at=_START,
        newest_open_at=_END,
        anomalies=anomalies,
        status="anomalies" if anomalies else "clean",
    )


def _arguments(*extra: str) -> list[str]:
    """Return the common command line plus optional trailing flags."""
    return [
        "audit-candles",
        "--target",
        "polygon",
        "AAPL",
        "--timeframe",
        "1m",
        "--window",
        _START.isoformat(),
        _END.isoformat(),
        *extra,
    ]


def _invoke_with_report(
    monkeypatch: pytest.MonkeyPatch,
    runner: CliRunner,
    report: CandleAuditReport,
    *extra: str,
) -> Result:
    """Invoke the registered command with one canned completed audit."""
    monkeypatch.setattr(candle_audit_cli, "_now_utc", lambda: _AS_OF)
    run_audit = AsyncMock(return_value=report)
    monkeypatch.setattr(candle_audit_cli, "_run_audit", run_audit)
    result = runner.invoke(app, _arguments(*extra))
    run_audit.assert_awaited_once_with(_request())
    return result


def test_command_horizon_is_aware_utc() -> None:
    """The default command horizon is captured as an aware UTC instant.

    Given: The command's production clock helper.
    When: It captures the current horizon.
    Then: The result carries the UTC timezone required by request validation.
    """
    assert candle_audit_cli._now_utc().tzinfo is UTC


def test_clean_json_report_exits_zero_without_extra_output(
    monkeypatch: pytest.MonkeyPatch,
    runner: CliRunner,
) -> None:
    """Machine mode emits one deterministic document for a clean audit.

    Given: A completed clean report.
    When: The registered command runs with ``--json``.
    Then: It exits zero and emits exactly one JSON document.
    """
    result = _invoke_with_report(monkeypatch, runner, _report(), "--json")
    assert result.exit_code == 0, result.stderr
    assert result.stderr == ""
    payload = json.loads(result.stdout)
    assert payload["status"] == "clean"
    assert payload["anomaly_count"] == 0
    assert payload["window_start"] == _START.isoformat()
    assert result.stdout.count("\n") == 1


def test_anomalous_json_report_exits_one_after_emission(
    monkeypatch: pytest.MonkeyPatch,
    runner: CliRunner,
) -> None:
    """A completed audit with findings remains machine-readable and nonzero.

    Given: A completed report containing a gap.
    When: The command emits machine output.
    Then: The JSON remains parseable and the process exits one.
    """
    anomaly = CandleAnomaly(CandleAnomalyType.GAP, _END, "one missing bar")
    result = _invoke_with_report(monkeypatch, runner, _report(anomaly), "--json")
    assert result.exit_code == 1
    assert result.stderr == ""
    assert json.loads(result.stdout)["anomalies"] == [
        {
            "detail": "one missing bar",
            "open_at": _END.isoformat(),
            "type": "gap",
        }
    ]


def test_human_report_uses_fixed_fields_and_anomaly_lines(
    monkeypatch: pytest.MonkeyPatch,
    runner: CliRunner,
) -> None:
    """Human mode is line-oriented and independent of terminal width.

    Given: A completed report containing one incomplete candle.
    When: The command emits human output.
    Then: Fixed metadata fields precede one stable anomaly line.
    """
    anomaly = CandleAnomaly(CandleAnomalyType.INCOMPLETE_CANDLE, _START, "not complete")
    result = _invoke_with_report(monkeypatch, runner, _report(anomaly))
    assert result.exit_code == 1
    lines = result.stdout.splitlines()
    assert lines[:4] == [
        "status: anomalies",
        "exchange: polygon",
        "symbol: AAPL",
        "timeframe: 1m",
    ]
    assert lines[-1] == f"anomaly: {_START.isoformat()} incomplete_candle not complete"


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        (
            [
                "audit-candles",
                "--target",
                "unknown",
                "AAPL",
                "--timeframe",
                "1m",
                "--window",
                _START.isoformat(),
                _END.isoformat(),
            ],
            "unsupported exchange",
        ),
        (
            _arguments()[:-2] + ["not-a-date", _END.isoformat()],
            "ISO 8601",
        ),
        (
            _arguments()[:-2] + [_START.replace(tzinfo=None).isoformat(), _END.isoformat()],
            "must include a UTC offset",
        ),
        (
            _arguments()[:-2] + ["0001-01-01T00:00:00+14:00", _END.isoformat()],
            "cannot be normalized to UTC",
        ),
        (
            _arguments()[:-2]
            + [
                (_START + timedelta(seconds=1)).isoformat(),
                _END.isoformat(),
            ],
            "timeframe grid",
        ),
        (
            _arguments("--anchor-offset-seconds", "-1"),
            "anchor offset",
        ),
    ],
)
def test_invalid_usage_exits_two_before_repository_use(
    monkeypatch: pytest.MonkeyPatch,
    runner: CliRunner,
    arguments: list[str],
    message: str,
) -> None:
    """CLI parsing and request validation happen before any database access.

    Given: An invalid exchange or malformed window coordinate.
    When: The operator invokes the command.
    Then: Typer exits two without opening a repository.
    """
    monkeypatch.setattr(candle_audit_cli, "_now_utc", lambda: _AS_OF)

    def unexpected_repository(_database_url: str) -> NoReturn:
        raise AssertionError("repository must not open for invalid usage")

    monkeypatch.setattr(candle_audit_cli, "get_repository", unexpected_repository)
    result = runner.invoke(app, arguments)
    assert result.exit_code == 2
    assert message in result.stderr


def test_extreme_valid_iso_window_exits_two_without_a_traceback(
    monkeypatch: pytest.MonkeyPatch,
    runner: CliRunner,
) -> None:
    """Datetime arithmetic remains total at the representable upper bound.

    Given: A syntactically valid last-minute ISO window at year 9999.
    When: The request checks whether the final candle has closed.
    Then: It exits as invalid usage without leaking an OverflowError traceback.
    """
    upper_end = datetime(9999, 12, 31, 23, 59, tzinfo=UTC)
    monkeypatch.setattr(
        candle_audit_cli,
        "_now_utc",
        lambda: datetime.max.replace(tzinfo=UTC),
    )

    def unexpected_repository(_database_url: str) -> NoReturn:
        raise AssertionError("repository must not open for invalid usage")

    monkeypatch.setattr(candle_audit_cli, "get_repository", unexpected_repository)
    arguments = _arguments()[:-2] + [upper_end.isoformat(), upper_end.isoformat()]
    result = runner.invoke(app, arguments)
    assert result.exit_code == 2
    assert "closed by as_of" in result.stderr
    assert "Traceback" not in result.stderr


@pytest.mark.parametrize(
    ("error", "message"),
    [
        (CandleAuditTargetError("unknown target"), "unknown target"),
        (CandleAuditRequestError("oversized response"), "oversized response"),
        (RuntimeError("database URL must stay hidden"), "repository read failed"),
    ],
)
def test_incomplete_audit_exits_three_without_a_false_report(
    monkeypatch: pytest.MonkeyPatch,
    runner: CliRunner,
    error: Exception,
    message: str,
) -> None:
    """Target, capacity, and read failures cannot be mislabeled clean.

    Given: An audit that cannot produce a complete report.
    When: The command receives the refusal or read failure.
    Then: It exits three on stderr without emitting false JSON or sensitive detail.
    """
    monkeypatch.setattr(candle_audit_cli, "_now_utc", lambda: _AS_OF)
    monkeypatch.setattr(candle_audit_cli, "_run_audit", AsyncMock(side_effect=error))
    result = runner.invoke(app, _arguments("--json"))
    assert result.exit_code == 3
    assert result.stdout == ""
    assert message in result.stderr
    assert "database URL" not in result.stderr


def test_repository_lifecycle_is_disposed_after_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The one-shot command always closes the cached repository lifecycle.

    Given: A repository read that completes successfully.
    When: The async command helper returns its report.
    Then: The shared repository registry is disposed exactly once.
    """
    repository = AsyncMock(spec=Repository)
    monkeypatch.setattr(
        candle_audit_cli,
        "get_bootstrap_settings",
        lambda: SimpleNamespace(db_url="sqlite+aiosqlite:///audit.db"),
    )
    monkeypatch.setattr(
        candle_audit_cli,
        "get_repository",
        lambda _database_url: cast(Repository, repository),
    )
    audit = AsyncMock(return_value=_report())
    dispose = AsyncMock()
    monkeypatch.setattr(candle_audit_cli, "audit_candle_window", audit)
    monkeypatch.setattr(candle_audit_cli, "dispose_repositories", dispose)
    result = asyncio.run(candle_audit_cli._run_audit(_request()))
    assert result == _report()
    audit.assert_awaited_once_with(repository, _request())
    dispose.assert_awaited_once_with()


def test_repository_lifecycle_is_disposed_after_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed read closes repository resources before propagating.

    Given: A repository read that raises an exception.
    When: The async command helper propagates the failure.
    Then: Repository disposal still completes exactly once.
    """
    repository = AsyncMock(spec=Repository)
    monkeypatch.setattr(
        candle_audit_cli,
        "get_bootstrap_settings",
        lambda: SimpleNamespace(db_url="sqlite+aiosqlite:///audit.db"),
    )
    monkeypatch.setattr(
        candle_audit_cli,
        "get_repository",
        lambda _database_url: cast(Repository, repository),
    )
    monkeypatch.setattr(
        candle_audit_cli,
        "audit_candle_window",
        AsyncMock(side_effect=RuntimeError("read failed")),
    )
    dispose = AsyncMock()
    monkeypatch.setattr(candle_audit_cli, "dispose_repositories", dispose)
    with pytest.raises(RuntimeError, match="read failed"):
        asyncio.run(candle_audit_cli._run_audit(_request()))
    dispose.assert_awaited_once_with()
