"""Supervise one Linux child process group inside an inherited absolute budget.

The leader remains an unreaped child until the final group signal, reserving its
PID against reuse. The caller must exclusively own child reaping and use the
default SIGCHLD disposition. This primitive observes only the original process
group: a descendant can escape with setsid. It does not prove container teardown,
database writer cessation, or a hard end-to-end 45-second completion guarantee.
The caller must independently verify its private container before finalizing DB
ownership. No command arguments, environment values or child output are logged
outside the newly created private output directory.
"""

import os
import signal
import sys
import time
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from snapper.core.market_snapshot_refresh_budget import RefreshBudget

CLEANUP_RESERVE_SECONDS = 5.0
_TERM_GRACE_SECONDS = 0.2
_POLL_SECONDS = 0.01
_REAP_RESERVE_SECONDS = 1.0


@dataclass(frozen=True, slots=True)
class SupervisedWorkerOutcome:
    """Immutable evidence about one child leader and its original process group.

    Attributes:
        returncode: Reaped leader's exit status, negative for a signal, or unknown.
        timed_out: Whether work consumed its cleanup reserve or the final deadline elapsed.
        leader_reaped: Whether this supervisor itself reaped its owned leader.
        process_group_empty: Whether the original group was absent in a bounded proc scan.
        deadline_met: Whether supervision completed before the inherited deadline.
    """

    returncode: int | None
    timed_out: bool
    leader_reaped: bool
    process_group_empty: bool
    deadline_met: bool


class RefreshSupervisorError(RuntimeError):
    """A fixed prelaunch refusal code containing no command or credential data."""


@dataclass(slots=True)
class _OwnedChild:
    """Track exclusive unreaped child ownership until the final cleanup signals."""

    pid: int
    reserved: bool = True

    def exited(self) -> bool | None:
        """Observe the leader without releasing its PID reservation.

        Returns:
            Exit state, or unknown when child ownership cannot be established.
        """
        if not self.reserved:
            return None
        try:
            status = os.waitid(os.P_PID, self.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
        except OSError:
            self.reserved = False
            return None
        if status is not None and status.si_pid != self.pid:
            self.reserved = False
            return None
        return status is not None

    def send_group_signal(self, requested_signal: signal.Signals) -> bool:
        """Signal only while the leader is still our unreaped child.

        Args:
            requested_signal: TERM or the final KILL for the original group.

        Returns:
            Whether the signal was issued or the group already disappeared.
        """
        if self.exited() is None:
            return False
        try:
            os.killpg(self.pid, requested_signal)
        except ProcessLookupError:
            return True
        except OSError:
            return False
        return True


def _remaining(budget: RefreshBudget, reserve: float = 0.0) -> float:
    """Read remaining time without creating a replacement phase deadline.

    Args:
        budget: Shared absolute refresh budget.
        reserve: Seconds retained for later cleanup inside the same deadline.

    Returns:
        Positive remaining seconds, or zero when expired or invalid.
    """
    try:
        return budget.remaining(reserve=reserve)
    except (TimeoutError, ValueError):
        return 0.0


def _validate_launch(command: Sequence[str], budget: RefreshBudget, output: Path) -> None:
    """Reject unsupported platforms, unsafe reaping and insufficient cleanup time.

    Args:
        command: Explicit executable and arguments, without shell interpretation.
        budget: Inherited absolute budget.
        output: New absolute private directory to create exclusively.

    Raises:
        RefreshSupervisorError: With a fixed refusal code before any child is launched.
    """
    if sys.platform != "linux" or os.name != "posix":
        raise RefreshSupervisorError("unsupported_supervision_platform")
    if signal.getsignal(signal.SIGCHLD) != signal.SIG_DFL:
        raise RefreshSupervisorError("exclusive_child_ownership_required")
    if isinstance(command, str) or not command or not Path(command[0]).is_absolute():
        raise RefreshSupervisorError("invalid_worker_command")
    if not output.is_absolute():
        raise RefreshSupervisorError("invalid_output_directory")
    if not _remaining(budget, CLEANUP_RESERVE_SECONDS):
        raise RefreshSupervisorError("insufficient_cleanup_budget")


def _open_private_log(output: Path) -> int:
    """Create a new private directory and an exclusive non-symlink worker log.

    Args:
        output: Previously nonexistent output directory with an existing parent.

    Returns:
        Owned write-only log descriptor, which the caller must close.

    Raises:
        RefreshSupervisorError: If any path cannot be exclusively created.
    """
    try:
        output.mkdir(mode=0o700)
        directory_descriptor = os.open(output, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            descriptor = os.open(
                "worker.log",
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                0o600,
                dir_fd=directory_descriptor,
            )
        finally:
            os.close(directory_descriptor)
    except (OSError, ValueError):
        raise RefreshSupervisorError("private_output_creation_failed") from None
    return descriptor


def _spawn_worker(
    command: Sequence[str], environment: Mapping[str, str], log_descriptor: int
) -> _OwnedChild:
    """Create a session leader without Popen's implicit polling or reaping paths.

    Args:
        command: Explicit absolute executable and argument sequence.
        environment: Exact inherited environment selected by the caller.
        log_descriptor: Private worker output file descriptor.

    Returns:
        Exclusive unreaped ownership of the new session and process-group leader.

    Raises:
        RefreshSupervisorError: If the process cannot be created with a separate session.
    """
    try:
        null_descriptor = os.open(os.devnull, os.O_RDONLY)
        try:
            actions = [
                (os.POSIX_SPAWN_DUP2, null_descriptor, 0),
                (os.POSIX_SPAWN_DUP2, log_descriptor, 1),
                (os.POSIX_SPAWN_DUP2, log_descriptor, 2),
                (os.POSIX_SPAWN_CLOSE, null_descriptor),
                (os.POSIX_SPAWN_CLOSE, log_descriptor),
            ]
            pid = os.posix_spawn(
                command[0],
                tuple(command),
                dict(environment),
                file_actions=actions,
                setsid=True,
                setsigmask=(),
            )
        finally:
            os.close(null_descriptor)
    except (OSError, ValueError, TypeError, NotImplementedError):
        raise RefreshSupervisorError("worker_launch_failed") from None
    return _OwnedChild(pid)


def _wait_for_worker(child: _OwnedChild, budget: RefreshBudget) -> bool:
    """Wait for the leader while preserving the cleanup reserve and PID ownership.

    Args:
        child: Unreaped owned session leader.
        budget: Shared deadline with the fixed cleanup reserve.

    Returns:
        Whether the worker exhausted its portion of the inherited deadline.
    """
    while True:
        remaining = _remaining(budget, CLEANUP_RESERVE_SECONDS)
        if not remaining:
            return True
        exited = child.exited()
        if exited is None or exited:
            return False
        time.sleep(min(_POLL_SECONDS, remaining))


def _reap_after_signals(child: _OwnedChild, budget: RefreshBudget) -> int | None:
    """Reap only after the final group signal and never signal this PID again.

    Args:
        child: Leader whose owned group has completed its signal phase.
        budget: Remaining cleanup time within the original deadline.

    Returns:
        Exact leader exit code, or unknown if reaping cannot be proven.
    """
    while child.reserved:
        try:
            pid, status = os.waitpid(child.pid, os.WNOHANG)
        except OSError:
            child.reserved = False
            return None
        if pid == child.pid:
            child.reserved = False
            return os.waitstatus_to_exitcode(status)
        remaining = _remaining(budget)
        if not remaining:
            return None
        time.sleep(min(_POLL_SECONDS, remaining))
    return None


def _group_empty(group_id: int, budget: RefreshBudget) -> bool:
    """Inspect Linux proc membership without sending any post-reap signal.

    Args:
        group_id: Original owned process-group identity.
        budget: Inherited absolute deadline bounding the scan.

    Returns:
        True only when a complete bounded scan sees no group member, including zombies.
    """
    try:
        with os.scandir("/proc") as entries:
            for entry in entries:
                if not _remaining(budget):
                    return False
                if not entry.name.isdecimal():
                    continue
                group = _read_process_group(Path(entry.path) / "stat")
                if group is None or group == group_id:
                    return False
    except OSError:
        return False
    return True


def _read_process_group(path: Path) -> int | None:
    """Read one kernel stat record without assuming the process name has no spaces.

    Args:
        path: Linux proc stat file for one numeric process directory.

    Returns:
        Its process group, zero for a vanished process, or unknown for unreadable evidence.
    """
    try:
        with path.open(encoding="utf-8", errors="replace") as handle:
            fields = handle.read(8192).rsplit(")", 1)[1].split()
        return int(fields[2])
    except FileNotFoundError:
        return 0
    except (OSError, IndexError, ValueError):
        return None


def _wait_for_empty_group(group_id: int, budget: RefreshBudget) -> bool:
    """Allow already-killed group members to disappear without further signals.

    Args:
        group_id: Original process group after leader reaping.
        budget: Remaining time before the inherited deadline.

    Returns:
        Whether a complete proc scan proved the group empty before expiry.
    """
    while _remaining(budget):
        if _group_empty(group_id, budget):
            return True
        remaining = _remaining(budget)
        if remaining:
            time.sleep(min(_POLL_SECONDS, remaining))
    return False


def _cleanup_child(
    child: _OwnedChild, budget: RefreshBudget, timed_out: bool
) -> SupervisedWorkerOutcome:
    """Always clean descendants, including after a normal leader exit.

    Args:
        child: Exclusively owned leader whose PID has not been reaped.
        budget: The original absolute budget, never renewed for cleanup.
        timed_out: Whether worker execution consumed the reserved cleanup interval.

    Returns:
        Conservative group-only evidence; any unknown cleanup remains false.
    """
    signalled = child.send_group_signal(signal.SIGTERM)
    grace = min(_TERM_GRACE_SECONDS, _remaining(budget, _REAP_RESERVE_SECONDS))
    if signalled and grace:
        time.sleep(grace)
    killed = child.send_group_signal(signal.SIGKILL)
    returncode = _reap_after_signals(child, budget)
    reaped = returncode is not None
    empty = reaped and killed and _wait_for_empty_group(child.pid, budget)
    deadline_met = bool(_remaining(budget))
    return SupervisedWorkerOutcome(
        returncode, timed_out or not deadline_met, reaped, empty, deadline_met
    )


def run_supervised_worker(
    command: Sequence[str], environment: Mapping[str, str], budget: RefreshBudget, output: Path
) -> SupervisedWorkerOutcome:
    """Run one owned process group while retaining time for cleanup and reaping.

    The caller must be the exclusive child reaper. Do not install a SIGCHLD
    handler or call waitpid for this child concurrently. A true process-group
    result cannot certify escaped sessions or the independently owned container.

    Args:
        command: Absolute executable and explicit arguments, never a shell string.
        environment: Exact environment supplied to the child without logging it.
        budget: Previously established absolute monotonic deadline.
        output: New absolute private directory for the exclusive worker.log file.

    Returns:
        Immutable exit, deadline, reaping and original-group evidence.

    Raises:
        RefreshSupervisorError: For sanitized prelaunch refusals or spawn failures.
    """
    _validate_launch(command, budget, output)
    descriptor = _open_private_log(output)
    try:
        if not _remaining(budget, CLEANUP_RESERVE_SECONDS):
            raise RefreshSupervisorError("insufficient_cleanup_budget")
        child = _spawn_worker(command, environment, descriptor)
        try:
            timed_out = _wait_for_worker(child, budget)
        except BaseException:
            _cleanup_child(child, budget, True)
            raise
        return _cleanup_child(child, budget, timed_out)
    finally:
        os.close(descriptor)
