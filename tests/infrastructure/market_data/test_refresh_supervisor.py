"""Real subprocess and focused OS-failure witnesses for group-only supervision."""

import os
import signal
import stat
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from snapper.core.market_snapshot_refresh_budget import RefreshBudget
from snapper.infrastructure.market_data import refresh_supervisor as supervisor
from snapper.infrastructure.market_data.refresh_supervisor import RefreshSupervisorError
from snapper.infrastructure.market_data.refresh_supervisor import run_supervised_worker


def _budget(seconds: float = 7.0) -> RefreshBudget:
    """Create one absolute test deadline without changing the pytest timeout.

    Args:
        seconds: Total supervision duration including the cleanup reserve.

    Returns:
        A monotonic absolute refresh budget.
    """
    return RefreshBudget(time.monotonic() + seconds)


def test_normal_exit_signals_group_before_reaping_and_writes_private_log(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Keep a normally exited leader reserved through all descendant cleanup signals.

    Given: A real child prints a private environment value and exits with status seventeen.
    When: The supervisor observes its exit and cleans its original group.
    Then: Both signals precede reaping, output stays private, and exact success evidence returns.
    """
    original_killpg = os.killpg
    observed: list[tuple[int, signal.Signals]] = []

    def traced_signal(pid: int, requested: signal.Signals) -> None:
        """Prove the signalled leader remains an unreaped child.

        Args:
            pid: Reserved process-group leader.
            requested: Actual signal being sent.
        """
        status = os.waitid(os.P_PID, pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
        assert status is not None
        assert status.si_pid == pid
        observed.append((pid, requested))
        original_killpg(pid, requested)

    monkeypatch.setattr(supervisor.os, "killpg", traced_signal)
    output = tmp_path / "run"
    result = run_supervised_worker(
        [
            sys.executable,
            "-c",
            "import os; print(os.environ['PRIVATE_TEST']); raise SystemExit(17)",
        ],
        {"PRIVATE_TEST": "never-public-test-value"},
        _budget(),
        output,
    )
    assert result.returncode == 17
    assert result.leader_reaped and result.process_group_empty and result.deadline_met
    assert not result.timed_out
    assert [requested for _, requested in observed] == [signal.SIGTERM, signal.SIGKILL]
    with pytest.raises(ChildProcessError):
        os.waitpid(observed[0][0], os.WNOHANG)
    assert stat.S_IMODE(output.stat().st_mode) == 0o700
    assert stat.S_IMODE((output / "worker.log").stat().st_mode) == 0o600
    assert "never-public-test-value" in (output / "worker.log").read_text()
    captured = capsys.readouterr()
    assert "never-public-test-value" not in captured.out + captured.err


def test_work_timeout_retains_cleanup_inside_original_budget(tmp_path: Path) -> None:
    """Use the inherited deadline rather than giving cleanup a replacement budget.

    Given: A real child ignores TERM and would run well beyond its work allowance.
    When: The work portion expires while five seconds remain for cleanup.
    Then: KILL and reaping finish before the original deadline and timeout remains explicit.
    """
    budget = _budget(6.0)
    result = run_supervised_worker(
        [
            sys.executable,
            "-c",
            "import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); print('ready',flush=True); time.sleep(12)",
        ],
        {},
        budget,
        tmp_path / "run",
    )
    assert result.timed_out
    assert result.returncode == -signal.SIGKILL
    assert result.leader_reaped and result.process_group_empty and result.deadline_met
    assert budget.remaining() > 0
    assert "ready" in (tmp_path / "run" / "worker.log").read_text()


def test_normal_leader_exit_still_kills_term_resistant_orphan(tmp_path: Path) -> None:
    """Clean the original group even when its leader already exited normally.

    Given: A real leader leaves a TERM-resistant child in the original group.
    When: Supervision observes the successful leader exit.
    Then: The orphan cannot keep running, while unreaped zombie evidence remains conservative.
    """
    ready = tmp_path / "orphan-ready"
    natural_exit = tmp_path / "natural-exit"
    code = "\n".join(
        [
            "import os,pathlib,signal,sys,time",
            "ready=pathlib.Path(sys.argv[1]); natural=pathlib.Path(sys.argv[2])",
            "pid=os.fork()",
            "if pid == 0:",
            " signal.signal(signal.SIGTERM,signal.SIG_IGN)",
            " ready.write_text(str(os.getpid()))",
            " time.sleep(3)",
            " natural.write_text('not-killed')",
            " os._exit(0)",
            "while not ready.exists(): time.sleep(0.005)",
        ]
    )
    result = run_supervised_worker(
        [sys.executable, "-c", code, str(ready), str(natural_exit)],
        {},
        _budget(6.0),
        tmp_path / "run",
    )
    assert result.returncode == 0
    assert result.leader_reaped
    assert not natural_exit.exists()
    child_stat = Path("/proc") / ready.read_text() / "stat"
    if child_stat.exists():
        assert child_stat.read_text().rsplit(")", 1)[1].split()[0] == "Z"
        assert not result.process_group_empty


def test_interrupt_cleans_owned_process_before_propagating(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Retain cleanup responsibility when the coordinator interrupts the worker wait.

    Given: A launched real child and an interrupted supervision wait.
    When: KeyboardInterrupt propagates through the primitive.
    Then: The owned leader has already been reaped before control returns.
    """
    original_spawn = supervisor._spawn_worker
    children: list[supervisor._OwnedChild] = []

    def spawn(
        command: list[str], environment: dict[str, str], descriptor: int
    ) -> supervisor._OwnedChild:
        """Retain the actual child handle for the post-interruption ownership assertion.

        Args:
            command: Real child executable and arguments.
            environment: Exact child environment.
            descriptor: Owned private output descriptor.

        Returns:
            The actual exclusively owned child.
        """
        child = original_spawn(command, environment, descriptor)
        children.append(child)
        return child

    monkeypatch.setattr(supervisor, "_spawn_worker", spawn)
    monkeypatch.setattr(supervisor, "_wait_for_worker", MagicMock(side_effect=KeyboardInterrupt))
    with pytest.raises(KeyboardInterrupt):
        run_supervised_worker(
            [sys.executable, "-c", "import time; time.sleep(3)"], {}, _budget(), tmp_path / "run"
        )
    with pytest.raises(ChildProcessError):
        os.waitpid(children[0].pid, os.WNOHANG)


@pytest.mark.parametrize("command", [[], ["relative-command"], "shell-like-command"])
def test_invalid_command_refuses_before_output_creation(
    tmp_path: Path, command: list[str] | str
) -> None:
    """Require an explicit absolute executable rather than interpreting a shell string.

    Given: A missing, relative or string-shaped command.
    When: Launch validation runs.
    Then: It refuses before creating output or starting a child.
    """
    with pytest.raises(RefreshSupervisorError, match="^invalid_worker_command$"):
        run_supervised_worker(command, {}, _budget(), tmp_path / "run")
    assert not (tmp_path / "run").exists()


def test_platform_and_reaper_requirements_refuse(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Refuse when Linux group ownership or reserved-zombie semantics are unavailable.

    Given: An unsupported platform followed by an autoreaping SIGCHLD disposition.
    When: The supervisor validates each launch.
    Then: Both refuse without starting any process.
    """
    with monkeypatch.context() as patch:
        patch.setattr(supervisor.sys, "platform", "win32")
        with pytest.raises(RefreshSupervisorError, match="^unsupported_supervision_platform$"):
            run_supervised_worker([sys.executable], {}, _budget(), tmp_path / "run")
    monkeypatch.setattr(supervisor.signal, "getsignal", lambda requested: signal.SIG_IGN)
    with pytest.raises(RefreshSupervisorError, match="^exclusive_child_ownership_required$"):
        run_supervised_worker([sys.executable], {}, _budget(), tmp_path / "run")


def test_existing_or_relative_output_is_not_reused(tmp_path: Path) -> None:
    """Prevent log overwrites and accidental reuse of a previous run directory.

    Given: An existing output directory and a relative alternate path.
    When: Each launch attempts to establish private output ownership.
    Then: Both refuse before launching the worker.
    """
    with pytest.raises(RefreshSupervisorError, match="^private_output_creation_failed$"):
        run_supervised_worker([sys.executable], {}, _budget(), tmp_path)
    with pytest.raises(RefreshSupervisorError, match="^invalid_output_directory$"):
        run_supervised_worker([sys.executable], {}, _budget(), Path("relative-output"))


@pytest.mark.parametrize("deadline", [time.monotonic() - 1, float("nan")])
def test_expired_or_invalid_budget_does_not_spawn(tmp_path: Path, deadline: float) -> None:
    """Keep invalid and exhausted budgets from acquiring a fresh cleanup period.

    Given: An already expired or invalid absolute deadline.
    When: A child launch is requested.
    Then: It refuses without creating its output directory.
    """
    with pytest.raises(RefreshSupervisorError, match="^insufficient_cleanup_budget$"):
        run_supervised_worker([sys.executable], {}, RefreshBudget(deadline), tmp_path / "run")
    assert not (tmp_path / "run").exists()


def test_budget_consumed_during_setup_is_not_reset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Recheck the same inherited deadline after private output setup.

    Given: Setup consumes the remaining worker allowance.
    When: Launch would otherwise start a process.
    Then: It refuses rather than allocating a new phase budget.
    """
    times = iter([0.0, 6.0])
    spawn = MagicMock()
    monkeypatch.setattr(supervisor, "_spawn_worker", spawn)
    with pytest.raises(RefreshSupervisorError, match="^insufficient_cleanup_budget$"):
        run_supervised_worker(
            [sys.executable], {}, RefreshBudget(10.0, clock=lambda: next(times)), tmp_path / "run"
        )
    spawn.assert_not_called()


@pytest.mark.parametrize(
    "command", [["/nonexistent/private-worker-secret"], [sys.executable, "bad\x00private-secret"]]
)
def test_spawn_errors_are_sanitized(tmp_path: Path, command: list[str]) -> None:
    """Hide executable, argument and native exception contents from launch failures.

    Given: An absent executable or an invalid argument containing private test text.
    When: Native process creation fails.
    Then: Only the fixed launch failure code escapes.
    """
    with pytest.raises(RefreshSupervisorError, match="^worker_launch_failed$"):
        run_supervised_worker(command, {"PRIVATE": "also-private"}, _budget(), tmp_path / "run")


def test_lost_child_ownership_never_signals_reused_pid(monkeypatch: pytest.MonkeyPatch) -> None:
    """Avoid any group signal after another reaper invalidates child ownership.

    Given: waitid reports that the formerly owned PID is no longer a child.
    When: Cleanup attempts to signal its group.
    Then: Reservation is withdrawn and no signal is sent to a potentially reused PID.
    """
    monkeypatch.setattr(supervisor.os, "waitid", MagicMock(side_effect=ChildProcessError))
    kill = MagicMock()
    monkeypatch.setattr(supervisor.os, "killpg", kill)
    child = supervisor._OwnedChild(123456)
    result = supervisor._cleanup_child(child, _budget(), False)
    assert not result.leader_reaped and not result.process_group_empty
    assert result.returncode is None
    assert child.exited() is None
    kill.assert_not_called()


def test_wrong_waitid_identity_withdraws_reservation(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fail closed if OS observation does not identify the reserved child.

    Given: An anomalous waitid result names another PID.
    When: The supervisor checks child ownership.
    Then: It refuses to use the original PID for cleanup signalling.
    """
    monkeypatch.setattr(supervisor.os, "waitid", MagicMock(return_value=MagicMock(si_pid=999)))
    child = supervisor._OwnedChild(123456)
    assert child.exited() is None
    assert not child.reserved


@pytest.mark.parametrize("error,issued", [(ProcessLookupError(), True), (PermissionError(), False)])
def test_group_signal_os_failures_remain_conservative(
    monkeypatch: pytest.MonkeyPatch, error: OSError, issued: bool
) -> None:
    """Distinguish an already absent group from a failed cleanup signal.

    Given: The leader remains owned but killpg fails at the OS boundary.
    When: One cleanup signal is attempted.
    Then: A missing group is harmless while permission failure cannot certify cleanup.
    """
    monkeypatch.setattr(supervisor.os, "waitid", MagicMock(return_value=None))
    monkeypatch.setattr(supervisor.os, "killpg", MagicMock(side_effect=error))
    assert supervisor._OwnedChild(123456).send_group_signal(signal.SIGKILL) is issued


def test_unknown_worker_observation_stops_waiting(monkeypatch: pytest.MonkeyPatch) -> None:
    """Do not wait indefinitely when exclusive child ownership has already been lost.

    Given: A child can no longer be observed through waitid.
    When: The worker wait polls its state.
    Then: It returns with the reservation withdrawn for conservative cleanup handling.
    """
    monkeypatch.setattr(supervisor.os, "waitid", MagicMock(side_effect=OSError))
    child = supervisor._OwnedChild(123456)
    assert not supervisor._wait_for_worker(child, _budget())
    assert not child.reserved


def test_reap_failure_or_exhausted_budget_never_claims_reaped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep failed or incomplete waitpid evidence distinct from exit status zero.

    Given: A waitpid failure followed by a still-running child at an expired deadline.
    When: The final reaping phase handles each outcome.
    Then: Neither path manufactures a successful leader exit.
    """
    monkeypatch.setattr(supervisor.os, "waitpid", MagicMock(side_effect=ChildProcessError))
    assert supervisor._reap_after_signals(supervisor._OwnedChild(123456), _budget()) is None
    monkeypatch.setattr(supervisor.os, "waitpid", MagicMock(return_value=(0, 0)))
    assert (
        supervisor._reap_after_signals(supervisor._OwnedChild(123456), RefreshBudget(0.0)) is None
    )


@pytest.mark.parametrize(
    "contents,expected",
    [
        ("100 (name with ) parentheses) Z 1 456 456 0", 456),
        ("not-a-stat-record", None),
        ("100 (worker) S 1 invalid 100", None),
    ],
)
def test_proc_parser_does_not_assume_simple_process_names(
    tmp_path: Path, contents: str, expected: int | None
) -> None:
    """Parse process-group identity without confusing spaces or parentheses in comm.

    Given: A kernel-shaped or malformed stat record.
    When: The group-membership parser reads it.
    Then: It identifies the correct group or conservatively refuses the evidence.
    """
    path = tmp_path / "stat"
    path.write_text(contents)
    assert supervisor._read_process_group(path) == expected
    assert supervisor._read_process_group(tmp_path / "missing") == 0


def test_unreadable_proc_evidence_never_proves_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fail closed when process-group membership cannot be inspected.

    Given: An unreadable stat record and an inaccessible proc directory.
    When: The supervisor tries to establish emptiness.
    Then: Unknown evidence never becomes a clean process group.
    """
    assert supervisor._read_process_group(tmp_path) is None
    monkeypatch.setattr(supervisor.os, "scandir", MagicMock(side_effect=PermissionError))
    assert not supervisor._group_empty(123456, _budget())


def test_expired_proc_scan_cannot_extend_the_absolute_deadline() -> None:
    """Keep post-reap inspection inside the same deadline as execution and cleanup.

    Given: The inherited deadline is already exhausted.
    When: Group inspection or its wait helper is requested.
    Then: Neither operation creates a new polling period.
    """
    assert not supervisor._group_empty(123456, RefreshBudget(0.0))
    assert not supervisor._wait_for_empty_group(123456, RefreshBudget(0.0))
