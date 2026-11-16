"""Standalone monitor CLI witnesses without real HTTP or host-alert delivery."""

import runpy
import subprocess
import sys
from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.application.market_snapshot_refresh import monitor
from snapper.application.market_snapshot_refresh.monitor import FreshnessMonitorError
from snapper.cli import market_snapshot_freshness_monitor as cli


def _arguments(tmp_path: Path) -> list[str]:
    """Build explicit monitor arguments with an inert local executable hook.

    Args:
        tmp_path: Isolated test directory.

    Returns:
        Arguments containing paths and scope metadata but no secret values.
    """
    hook = tmp_path / "alert-hook"
    hook.write_text("inert test hook", encoding="utf-8")
    hook.chmod(0o700)
    return [
        "--endpoint",
        "https://monitor.invalid/api/metrics/market-snapshots/freshness",
        "--token-file",
        str(tmp_path / "token"),
        "--instrument-public-id",
        "feed0000-1234-4567-89ab-0123456789ab",
        "--fx-exchange",
        "kraken",
        "--scope-digest",
        "a" * 64,
        "--alert-hook",
        str(hook),
    ]


def test_ready_cli_never_invokes_alert(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Keep successful read-only monitoring independent of any alert subprocess.

    Given: A successful injected monitor and explicit public configuration.
    When: The standalone CLI runs once.
    Then: It prints readiness, returns zero and never invokes the alert hook.
    """
    check = AsyncMock(return_value=None)
    run = MagicMock()
    monkeypatch.setattr(cli, "check_market_snapshot_freshness", check)
    monkeypatch.setattr(cli.subprocess, "run", run)
    assert cli.main(_arguments(tmp_path)) == 0
    check.assert_awaited_once()
    run.assert_not_called()
    output = capsys.readouterr()
    assert output.out == "OK market-snapshot-freshness: ready\n"
    assert output.err == ""


def test_refusal_invokes_bounded_existing_alert_hook(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Propagate non-readiness into a bounded and observable host-alert invocation.

    Given: A failed independent freshness observation.
    When: The CLI handles its fixed diagnostic code.
    Then: The hook receives only that code and the CLI remains nonzero.
    """
    check = AsyncMock(side_effect=FreshnessMonitorError("not_ready"))
    run = MagicMock(return_value=subprocess.CompletedProcess([], 0))
    monkeypatch.setattr(cli, "check_market_snapshot_freshness", check)
    monkeypatch.setattr(cli.subprocess, "run", run)
    args = _arguments(tmp_path)
    assert cli.main(args) == 1
    run.assert_called_once_with(
        [args[-1], "not_ready"],
        check=False,
        timeout=5,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    output = capsys.readouterr()
    assert "REFUSE market-snapshot-freshness: not_ready" in output.err
    assert "ALERT requested" in output.err
    assert output.out == ""


@pytest.mark.parametrize(
    "error",
    [
        OSError("do-not-print-private-path"),
        subprocess.TimeoutExpired("do-not-print-private-command", 5),
    ],
)
def test_alert_execution_failure_is_visible_and_sanitized(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    error: Exception,
) -> None:
    """Keep an alert timeout or launch error visible without leaking child details.

    Given: A monitor failure and an alert adapter that cannot complete.
    When: The CLI attempts failure delivery.
    Then: It reports undelivered alert status while preserving failure exit status.
    """
    monkeypatch.setattr(
        cli,
        "check_market_snapshot_freshness",
        AsyncMock(side_effect=FreshnessMonitorError("request_timeout")),
    )
    monkeypatch.setattr(cli.subprocess, "run", MagicMock(side_effect=error))
    assert cli.main(_arguments(tmp_path)) == 1
    output = capsys.readouterr()
    assert "ALERT-UNDELIVERED: hook execution failed" in output.err
    assert "do-not-print" not in output.err


def test_alert_nonzero_exit_is_visible(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Do not report delivery when the existing alert adapter itself refuses.

    Given: A non-ready monitor and a hook returning failure.
    When: The CLI handles both outcomes.
    Then: The log explicitly reports an undelivered alert.
    """
    monkeypatch.setattr(
        cli,
        "check_market_snapshot_freshness",
        AsyncMock(side_effect=FreshnessMonitorError("not_ready")),
    )
    monkeypatch.setattr(
        cli.subprocess, "run", MagicMock(return_value=subprocess.CompletedProcess([], 2))
    )
    assert cli.main(_arguments(tmp_path)) == 1
    assert "ALERT-UNDELIVERED: hook returned failure" in capsys.readouterr().err


def test_invalid_configuration_alerts_without_monitor_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Fail closed and alert when a monitor template has not been configured.

    Given: A malformed frozen scope digest.
    When: The CLI validates its public configuration.
    Then: No HTTP observation starts and the refusal invokes the existing hook.
    """
    check = AsyncMock()
    run = MagicMock(return_value=subprocess.CompletedProcess([], 0))
    monkeypatch.setattr(cli, "check_market_snapshot_freshness", check)
    monkeypatch.setattr(cli.subprocess, "run", run)
    args = _arguments(tmp_path)
    args[args.index("--scope-digest") + 1] = "not-digest"
    assert cli.main(args) == 1
    check.assert_not_called()
    assert run.call_args.args[0][-1] == "invalid_scope_digest"
    assert "not-digest" not in capsys.readouterr().err


@pytest.mark.parametrize("kind", ["relative", "missing", "not-executable"])
def test_unavailable_hook_prevents_a_false_healthy_monitor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], kind: str
) -> None:
    """Require an executable absolute alert adapter before reporting readiness.

    Given: An invalid hook path or a nonexecutable adapter.
    When: The CLI checks its alert integration.
    Then: No monitor call runs and an undelivered failure is visible.
    """
    check = AsyncMock()
    run = MagicMock(side_effect=OSError("private-detail"))
    monkeypatch.setattr(cli, "check_market_snapshot_freshness", check)
    monkeypatch.setattr(cli.subprocess, "run", run)
    args = _arguments(tmp_path)
    if kind == "relative":
        args[-1] = "relative-hook"
    elif kind == "missing":
        args[-1] = str(tmp_path / "absent")
    else:
        Path(args[-1]).chmod(0o600)
    assert cli.main(args) == 1
    check.assert_not_called()
    output = capsys.readouterr()
    assert "alert_unavailable" in output.err
    assert "ALERT-UNDELIVERED" in output.err
    assert "private-detail" not in output.err
    if kind == "relative":
        run.assert_not_called()


def test_module_entrypoint_propagates_success_exit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Exercise the supported module main guard without launching another process.

    Given: Explicit arguments and a successful injected observation.
    When: The module source runs as the main entrypoint.
    Then: It exits through the canonical guard with status zero.
    """
    monkeypatch.setattr(monitor, "check_market_snapshot_freshness", AsyncMock(return_value=None))
    monkeypatch.setattr(sys, "argv", ["monitor", *_arguments(tmp_path)])
    with pytest.raises(SystemExit) as result:
        runpy.run_path(str(Path(cli.__file__)), run_name="__main__")
    assert result.value.code == 0
