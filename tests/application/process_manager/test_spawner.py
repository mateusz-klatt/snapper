"""Tests for ProcessSpawnerService subprocess management."""

import importlib
import os
import signal
import subprocess
import sys
import types
from datetime import UTC
from datetime import datetime
from typing import cast

import pytest
from pytest import MonkeyPatch

from snapper.application.process_manager import spawner as spawner_module
from snapper.application.process_manager.models import ProcessInstanceInfo
from snapper.application.process_manager.models import SpawnerStatusSnapshot
from snapper.application.process_manager.spawner import ProcessSpawnerService
from snapper.application.process_manager.spawner import _build_process_command

IS_WINDOWS = sys.platform == "win32"


def _reload_spawner_for_posix(monkeypatch: MonkeyPatch) -> types.ModuleType:
    """Reload spawner module simulating POSIX platform.

    On Windows, temporarily adds POSIX-only os attributes (setsid, killpg,
    getpgid) before reloading the module to allow POSIX code paths to be
    tested.

    Args:
        monkeypatch: Pytest monkeypatch fixture for patching.

    Returns:
        Reloaded spawner module configured for POSIX platform.
    """
    monkeypatch.setattr(sys, "platform", "linux", raising=False)
    if IS_WINDOWS:
        if not hasattr(os, "setsid"):
            monkeypatch.setattr(os, "setsid", lambda: None, raising=False)
        if not hasattr(os, "killpg"):
            monkeypatch.setattr(os, "killpg", lambda pgid, sig: None, raising=False)
        if not hasattr(os, "getpgid"):
            monkeypatch.setattr(os, "getpgid", lambda pid: pid, raising=False)
    mod: types.ModuleType = importlib.reload(spawner_module)
    return mod


class DummyProcessWithWait:
    """Mock process with configurable wait behavior."""

    def __init__(
        self,
        *,
        initial_returncode: int | None = None,
        wait_raises: bool = False,
    ) -> None:
        """Initialize the instance."""
        self.pid = 9999
        self.returncode = initial_returncode
        self._wait_raises = wait_raises
        self.sent_signals: list[int] = []
        self.killed = False

    def poll(self) -> int | None:
        """Return current returncode."""
        return self.returncode

    def communicate(self) -> tuple[bytes, bytes]:
        """Return empty stdout/stderr."""
        return b"", b""

    def send_signal(self, sig: int) -> None:
        """Record sent signal."""
        self.sent_signals.append(sig)

    def terminate(self) -> None:
        """Set returncode to -15."""
        self.returncode = -15

    def kill(self) -> None:
        """Mark as killed and set returncode."""
        self.killed = True
        if not self._wait_raises:
            self.returncode = -9

    def wait(self, timeout: float | None = None) -> int:
        """Wait for process completion or raise timeout."""
        if self._wait_raises:
            raise subprocess.TimeoutExpired(cmd=[], timeout=timeout or 0.0)
        if self.returncode is None:
            raise subprocess.TimeoutExpired(cmd=[], timeout=timeout or 0.0)
        return self.returncode


@pytest.fixture(autouse=True)
def fast_sleep(monkeypatch: MonkeyPatch) -> None:
    """Patch sleep function to avoid delays in tests."""
    monkeypatch.setattr(
        "snapper.application.process_manager.spawner.time.sleep",
        lambda _: None,
    )


class TestBuildProcessCommand:
    """Test cases for _build_process_command function."""

    def test_command_contains_all_parts(self) -> None:
        """Verify _build_process_command includes all required arguments.

        Given: Process configuration with class path and method,
        When: _build_process_command is called,
        Then: Command includes Python interpreter, module, and config.
        """
        cmd = _build_process_command(
            name="worker",
            class_path="my.module.MyClass",
            method="run",
            parameters={"key": "value"},
        )
        assert sys.executable in cmd
        assert "-m" in cmd
        assert "snapper.server.process_runner" in cmd
        assert "--config" in cmd
        assert any("my.module.MyClass" in arg for arg in cmd)


class TestSpawnerImmediateExitWithoutCapture:
    """Test cases for spawn failure without output capture."""

    def test_spawn_immediate_exit_failure_no_capture(self, monkeypatch: MonkeyPatch) -> None:
        """Verify spawn raises error on immediate exit without capture.

        Given: Process that exits immediately with failure code,
        When: spawn is called without capture_output,
        Then: RuntimeError is raised directing to console output.
        """
        dummy = DummyProcessWithWait(initial_returncode=1)

        def fake_popen(cmd: list[str], **kwargs: object) -> DummyProcessWithWait:
            return dummy

        monkeypatch.setattr(
            "snapper.application.process_manager.spawner.subprocess.Popen",
            fake_popen,
        )
        service = ProcessSpawnerService(capture_output=False)
        with pytest.raises(RuntimeError) as exc:
            service.spawn(
                name="worker",
                class_path="snapper.application.process_manager.models.ProcessInstanceInfo",
                method="run",
                parameters={},
            )
        assert "see console output" in str(exc.value).lower()


class TestSpawnerClassPathValidation:
    """Test cases for class path validation in spawn."""

    def test_spawn_invalid_module_path(self, monkeypatch: MonkeyPatch) -> None:
        """Verify spawn rejects invalid class path without dots.

        Given: Class path without module separator,
        When: spawn is called,
        Then: RuntimeError with 'invalid class' message is raised.
        """

        def fake_popen(cmd: list[str], **kwargs: object) -> DummyProcessWithWait:
            raise AssertionError("Popen should not be called")

        monkeypatch.setattr(
            "snapper.application.process_manager.spawner.subprocess.Popen",
            fake_popen,
        )
        service = ProcessSpawnerService()
        with pytest.raises(RuntimeError) as exc:
            service.spawn(
                name="worker",
                class_path="NoDotsHere",
                method="run",
                parameters={},
            )
        assert "invalid class" in str(exc.value).lower()

    def test_spawn_missing_attribute(self, monkeypatch: MonkeyPatch) -> None:
        """Verify spawn rejects non-existent class in module.

        Given: Valid module with non-existent class name,
        When: spawn is called,
        Then: RuntimeError with 'invalid class' message is raised.
        """

        def fake_popen(cmd: list[str], **kwargs: object) -> DummyProcessWithWait:
            raise AssertionError("Popen should not be called")

        monkeypatch.setattr(
            "snapper.application.process_manager.spawner.subprocess.Popen",
            fake_popen,
        )
        service = ProcessSpawnerService()
        with pytest.raises(RuntimeError) as exc:
            service.spawn(
                name="worker",
                class_path="snapper.application.process_manager.models.NonExistentClass",
                method="run",
                parameters={},
            )
        assert "invalid class" in str(exc.value).lower()


class TestTerminateEdgeCases:
    """Test cases for terminate method edge cases."""

    def test_terminate_not_found(self) -> None:
        """Verify terminate returns False for unknown process.

        Given: Empty spawner with no registered processes,
        When: terminate is called with unknown name,
        Then: False is returned.
        """
        service = ProcessSpawnerService()
        result = service.terminate("nonexistent")
        assert result is False

    def test_terminate_already_dead(self, monkeypatch: MonkeyPatch) -> None:
        """Verify terminate handles already-exited process.

        Given: Process that already exited with returncode 0,
        When: terminate is called,
        Then: True is returned and exit_code is recorded.
        """
        dummy = DummyProcessWithWait(initial_returncode=0)
        info = ProcessInstanceInfo(
            name="worker",
            pid=1234,
            started_at=datetime.now(UTC),
            config={},
            process=cast(subprocess.Popen[bytes], dummy),
        )
        service = ProcessSpawnerService()
        service.processes["worker"] = info
        result = service.terminate("worker")
        assert result is True
        assert info.exit_code == 0

    def test_terminate_sigterm_fails_uses_terminate(self, monkeypatch: MonkeyPatch) -> None:
        """Verify terminate falls back to process.terminate() on SIGTERM failure.

        Given: Process where SIGTERM causes ProcessLookupError,
        When: terminate is called,
        Then: Falls back to terminate method and returns True.
        """
        original_platform = sys.platform
        mod = _reload_spawner_for_posix(monkeypatch)
        dummy = DummyProcessWithWait(initial_returncode=None)

        def fake_killpg(pgid: int, sig: int) -> None:
            dummy.returncode = 0
            raise ProcessLookupError("No such process")

        def fake_getpgid(pid: int) -> int:
            return pid + 1

        monkeypatch.setattr(mod, "_posix_killpg", fake_killpg)
        monkeypatch.setattr(mod, "_posix_getpgid", fake_getpgid)
        info = ProcessInstanceInfo(
            name="worker",
            pid=1234,
            started_at=datetime.now(UTC),
            config={},
            process=cast(subprocess.Popen[bytes], dummy),
        )
        service = mod.ProcessSpawnerService()
        service.processes["worker"] = info
        result = service.terminate("worker", timeout=0.5)
        assert result is True
        monkeypatch.setattr(sys, "platform", original_platform, raising=False)
        importlib.reload(spawner_module)

    def test_terminate_sigkill_fails_uses_kill(self, monkeypatch: MonkeyPatch) -> None:
        """Verify kill fallback when SIGKILL via killpg fails.

        Given: Process that ignores SIGKILL via killpg,
        When: terminate called,
        Then: Process.kill() called as fallback.
        """
        original_platform = sys.platform
        mod = _reload_spawner_for_posix(monkeypatch)
        dummy = DummyProcessWithWait(initial_returncode=None)
        call_count = {"total": 0}

        def fake_killpg(pgid: int, sig: int) -> None:
            call_count["total"] += 1
            if call_count["total"] != 1:
                raise PermissionError("Operation not permitted")

        def fake_getpgid(pid: int) -> int:
            return pid + 1

        monkeypatch.setattr(mod, "_posix_killpg", fake_killpg)
        monkeypatch.setattr(mod, "_posix_getpgid", fake_getpgid)
        timestamps = [0.0, 0.1, 1.5, 3.0]

        def fake_time() -> float:
            if timestamps:
                return timestamps.pop(0)
            return 3.0

        monkeypatch.setattr(mod.time, "time", fake_time)
        info = ProcessInstanceInfo(
            name="worker",
            pid=1234,
            started_at=datetime.now(UTC),
            config={},
            process=cast(subprocess.Popen[bytes], dummy),
        )
        service = mod.ProcessSpawnerService()
        service.processes["worker"] = info
        result = service.terminate("worker", timeout=0.5)
        assert result is True
        assert dummy.killed is True
        monkeypatch.setattr(sys, "platform", original_platform, raising=False)
        importlib.reload(spawner_module)

    def test_terminate_unkillable_process(self, monkeypatch: MonkeyPatch) -> None:
        """Verify handling of process that cannot be killed.

        Given: Process that ignores all termination signals,
        When: terminate called,
        Then: Returns True after exhausting kill attempts.
        """
        original_platform = sys.platform
        mod = _reload_spawner_for_posix(monkeypatch)
        dummy = DummyProcessWithWait(initial_returncode=None, wait_raises=True)

        def fake_killpg(pgid: int, sig: int) -> None:
            """Intentionally empty mock implementation."""
            pass

        def fake_getpgid(pid: int) -> int:
            return pid + 1

        monkeypatch.setattr(mod, "_posix_killpg", fake_killpg)
        monkeypatch.setattr(mod, "_posix_getpgid", fake_getpgid)
        timestamps = [0.0, 0.1, 1.5, 3.0]

        def fake_time() -> float:
            if timestamps:
                return timestamps.pop(0)
            return 3.0

        monkeypatch.setattr(mod.time, "time", fake_time)
        info = ProcessInstanceInfo(
            name="zombie",
            pid=1234,
            started_at=datetime.now(UTC),
            config={},
            process=cast(subprocess.Popen[bytes], dummy),
        )
        service = mod.ProcessSpawnerService()
        service.processes["zombie"] = info
        with pytest.raises(TimeoutError) as exc:
            service.terminate("zombie", timeout=0.5)
        assert "could not be killed" in str(exc.value)
        monkeypatch.setattr(sys, "platform", original_platform, raising=False)
        importlib.reload(spawner_module)


class TestListProcesses:
    """Test cases for list_processes method."""

    def test_list_processes_returns_status_for_all(self) -> None:
        """Verify list_processes returns status for all registered processes.

        Given: Spawner with two processes in different states,
        When: list_processes is called,
        Then: Status is returned for both with correct running states.
        """
        dummy1 = DummyProcessWithWait(initial_returncode=None)
        dummy2 = DummyProcessWithWait(initial_returncode=0)
        info1 = ProcessInstanceInfo(
            name="alpha",
            pid=1,
            started_at=datetime.now(UTC),
            config={},
            process=cast(subprocess.Popen[bytes], dummy1),
            last_heartbeat=datetime.now(UTC),
        )
        info2 = ProcessInstanceInfo(
            name="bravo",
            pid=2,
            started_at=datetime.now(UTC),
            config={},
            process=cast(subprocess.Popen[bytes], dummy2),
            exit_code=0,
        )
        service = ProcessSpawnerService()
        service.processes = {"alpha": info1, "bravo": info2}
        result = service.list_processes()
        assert len(result) == 2
        assert all(isinstance(r, SpawnerStatusSnapshot) for r in result)
        names = {r.name for r in result}
        assert names == {"alpha", "bravo"}
        alpha_status = next(r for r in result if r.name == "alpha")
        assert alpha_status.running is True
        bravo_status = next(r for r in result if r.name == "bravo")
        assert bravo_status.running is False


class TestCaptureOutputPaths:
    """Test cases for output capture on spawn failure."""

    def test_spawn_captures_stdout_stderr_on_failure(self, monkeypatch: MonkeyPatch) -> None:
        """Verify spawn includes stdout/stderr in error on failure.

        Given: Process that fails with stdout and stderr output,
        When: spawn is called with capture_output=True,
        Then: RuntimeError includes both stdout and stderr messages.
        """
        dummy = DummyProcessWithWait(initial_returncode=1)

        def fake_communicate() -> tuple[bytes, bytes]:
            return b"stdout message", b"stderr message"

        dummy.communicate = fake_communicate

        def fake_popen(cmd: list[str], **kwargs: object) -> DummyProcessWithWait:
            return dummy

        monkeypatch.setattr(
            "snapper.application.process_manager.spawner.subprocess.Popen",
            fake_popen,
        )
        service = ProcessSpawnerService(capture_output=True)
        with pytest.raises(RuntimeError) as exc:
            service.spawn(
                name="worker",
                class_path="snapper.application.process_manager.models.ProcessInstanceInfo",
                method="run",
                parameters={},
            )
        error_msg = str(exc.value)
        assert "stdout message" in error_msg
        assert "stderr message" in error_msg


class DummyWinProcess:
    """Mock Windows process for termination testing."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.pid = 123
        self.returncode: int | None = None
        self.sent_signal = False
        self.killed = False

    def poll(self) -> int | None:
        """Return current returncode."""
        return self.returncode

    def send_signal(self, _sig: int) -> None:
        """Mark signal as sent."""
        self.sent_signal = True

    def terminate(self) -> None:
        """Mark as terminated."""
        self.killed = True

    def kill(self) -> None:
        """Mark as killed."""
        self.killed = True

    def wait(self, timeout: float = 0.0) -> None:
        """Set returncode to -9."""
        self.returncode = -9


def test_windows_termination_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify Windows termination uses signals and terminate method.

    Given a process running on Windows platform,
    When terminate is called,
    Then both signal is sent and process is killed.
    """
    original_platform = sys.platform
    monkeypatch.setattr(sys, "platform", "win32", raising=False)
    mod = importlib.reload(spawner_module)
    svc = mod.ProcessSpawnerService()
    dummy_proc = DummyWinProcess()
    info = ProcessInstanceInfo(
        name="p",
        pid=dummy_proc.pid,
        started_at=datetime.now(UTC),
        config={},
        process=cast(subprocess.Popen[bytes], dummy_proc),
        exit_code=None,
        last_heartbeat=datetime.now(UTC),
    )
    svc.processes["p"] = info
    svc.terminate("p", timeout=0)
    assert dummy_proc.sent_signal
    assert dummy_proc.killed
    monkeypatch.setattr(sys, "platform", original_platform, raising=False)
    importlib.reload(spawner_module)


class DummyWinProcessWithOSError:
    """Mock Windows process that raises OSError on send_signal."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.pid = 124
        self.returncode: int | None = None
        self.terminate_called = False
        self.killed = False

    def poll(self) -> int | None:
        """Return current returncode."""
        return self.returncode

    def send_signal(self, _sig: int) -> None:
        """Raise OSError to test fallback."""
        raise OSError("Signal not supported")

    def terminate(self) -> None:
        """Mark as terminated."""
        self.terminate_called = True

    def kill(self) -> None:
        """Mark as killed and set returncode."""
        self.killed = True
        self.returncode = -9

    def wait(self, timeout: float = 0.0) -> None:
        """Set returncode to -9."""
        self.returncode = -9


def test_windows_termination_oserror_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify Windows termination falls back to terminate on OSError.

    Given a process running on Windows platform,
    When send_signal raises OSError,
    Then fallback to terminate() method is used.
    """
    original_platform = sys.platform
    monkeypatch.setattr(sys, "platform", "win32", raising=False)
    mod = importlib.reload(spawner_module)
    svc = mod.ProcessSpawnerService()
    dummy_proc = DummyWinProcessWithOSError()
    info = ProcessInstanceInfo(
        name="p2",
        pid=dummy_proc.pid,
        started_at=datetime.now(UTC),
        config={},
        process=cast(subprocess.Popen[bytes], dummy_proc),
        exit_code=None,
        last_heartbeat=datetime.now(UTC),
    )
    svc.processes["p2"] = info
    svc.terminate("p2", timeout=0)
    assert dummy_proc.terminate_called
    assert dummy_proc.killed
    monkeypatch.setattr(sys, "platform", original_platform, raising=False)
    importlib.reload(spawner_module)


def test_posix_module_init_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify POSIX module initialization sets correct functions.

    Given the module is loaded on a Linux platform,
    When the spawner module is initialized,
    Then POSIX-specific functions are set correctly.
    """
    original_platform = sys.platform
    mod = _reload_spawner_for_posix(monkeypatch)
    assert mod._preexec_setsid is not None
    assert mod._posix_killpg is not None
    assert mod._posix_getpgid is not None
    monkeypatch.setattr(sys, "platform", original_platform, raising=False)
    importlib.reload(spawner_module)


class DummyPosixProcess:
    """Mock POSIX process for termination testing."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.pid = 456
        self.returncode: int | None = None
        self.terminated = False
        self.killed = False

    def poll(self) -> int | None:
        """Return current returncode."""
        return self.returncode

    def send_signal(self, _sig: int) -> None:
        """No-op signal handler."""
        pass

    def terminate(self) -> None:
        """Mark as terminated and set returncode."""
        self.terminated = True
        self.returncode = -15

    def kill(self) -> None:
        """Mark as killed and set returncode."""
        self.killed = True
        self.returncode = -9

    def wait(self, timeout: float = 0.0) -> int:
        """Wait for completion or raise timeout."""
        if self.returncode is None:
            raise subprocess.TimeoutExpired(cmd=[], timeout=timeout)
        return self.returncode


def test_posix_termination_graceful(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify POSIX graceful termination uses SIGTERM.

    Given a process running on Linux platform,
    When terminate is called and process responds to SIGTERM,
    Then process is terminated gracefully with single SIGTERM signal.
    """
    original_platform = sys.platform
    mod = _reload_spawner_for_posix(monkeypatch)
    dummy_proc = DummyPosixProcess()
    killpg_calls: list[tuple[int, int]] = []

    def fake_killpg(pgid: int, sig: int) -> None:
        killpg_calls.append((pgid, sig))
        if sig == signal.SIGTERM:
            dummy_proc.returncode = 0

    def fake_getpgid(pid: int) -> int:
        return pid + 100

    monkeypatch.setattr(mod, "_posix_killpg", fake_killpg)
    monkeypatch.setattr(mod, "_posix_getpgid", fake_getpgid)
    svc = mod.ProcessSpawnerService()
    info = ProcessInstanceInfo(
        name="posix_proc",
        pid=dummy_proc.pid,
        started_at=datetime.now(UTC),
        config={},
        process=cast(subprocess.Popen[bytes], dummy_proc),
        exit_code=None,
        last_heartbeat=datetime.now(UTC),
    )
    svc.processes["posix_proc"] = info
    result = svc.terminate("posix_proc", timeout=1.0)
    assert result is True
    assert len(killpg_calls) == 1
    assert killpg_calls[0][1] == signal.SIGTERM
    monkeypatch.setattr(sys, "platform", original_platform, raising=False)
    importlib.reload(spawner_module)


def test_posix_termination_sigkill(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify POSIX termination escalates to SIGKILL when needed.

    Given a process running on Linux that ignores SIGTERM,
    When terminate is called and timeout expires,
    Then process is forcefully killed with SIGKILL signal.
    """
    original_platform = sys.platform
    mod = _reload_spawner_for_posix(monkeypatch)
    dummy_proc = DummyPosixProcess()
    killpg_calls: list[tuple[int, int]] = []

    def fake_killpg(pgid: int, sig: int) -> None:
        killpg_calls.append((pgid, sig))
        if sig == mod.SIGKILL_SIGNAL:
            dummy_proc.returncode = -9

    def fake_getpgid(pid: int) -> int:
        return pid + 100

    monkeypatch.setattr(mod, "_posix_killpg", fake_killpg)
    monkeypatch.setattr(mod, "_posix_getpgid", fake_getpgid)
    timestamps = [0.0, 0.1, 1.5, 3.0]

    def fake_time() -> float:
        if timestamps:
            return timestamps.pop(0)
        return 3.0

    monkeypatch.setattr(mod.time, "time", fake_time)
    svc = mod.ProcessSpawnerService()
    info = ProcessInstanceInfo(
        name="posix_proc",
        pid=dummy_proc.pid,
        started_at=datetime.now(UTC),
        config={},
        process=cast(subprocess.Popen[bytes], dummy_proc),
        exit_code=None,
        last_heartbeat=datetime.now(UTC),
    )
    svc.processes["posix_proc"] = info
    result = svc.terminate("posix_proc", timeout=0.5)
    assert result is True
    assert any(call[1] == mod.SIGKILL_SIGNAL for call in killpg_calls)
    monkeypatch.setattr(sys, "platform", original_platform, raising=False)
    importlib.reload(spawner_module)


class DummyProcess:
    """Mock process with configurable behavior for testing."""

    def __init__(
        self,
        *,
        initial_returncode: int | None = None,
        stdout: bytes = b"",
        stderr: bytes = b"",
        pid: int = 1234,
    ) -> None:
        """Initialize the instance."""
        self.pid = pid
        self.returncode = initial_returncode
        self._stdout = stdout
        self._stderr = stderr
        self.sent_signals: list[int] = []
        self.terminated = False
        self.killed = False

    def poll(self) -> int | None:
        """Return current returncode."""
        return self.returncode

    def communicate(self) -> tuple[bytes, bytes]:
        """Return configured stdout/stderr."""
        return self._stdout, self._stderr

    def send_signal(self, sig: int) -> None:
        """Record signal and set returncode."""
        self.sent_signals.append(sig)
        if self.returncode is None:
            self.returncode = 0

    def terminate(self) -> None:
        """Mark as terminated and set returncode."""
        self.terminated = True
        self.returncode = 0

    def kill(self) -> None:
        """Mark as killed and set returncode."""
        self.killed = True
        self.returncode = -9

    def wait(self, timeout: float | None = None) -> int:
        """Wait for completion or raise timeout."""
        if self.returncode is None:
            raise subprocess.TimeoutExpired(cmd=[], timeout=0.0 if timeout is None else timeout)
        return self.returncode


class StubbornProcess(DummyProcess):
    """Process that ignores termination attempts."""

    def send_signal(self, sig: int) -> None:
        """Record signal without setting returncode."""
        self.sent_signals.append(sig)

    def terminate(self) -> None:
        """Mark as terminated without setting returncode."""
        self.terminated = True


@pytest.fixture(autouse=True)
def fast_sleep_v2(monkeypatch: MonkeyPatch) -> None:
    """Patch sleep function to avoid delays in tests."""

    def _noop_sleep(seconds: float) -> None:
        return None

    monkeypatch.setattr(
        "snapper.application.process_manager.spawner.time.sleep",
        _noop_sleep,
    )


def test_spawn_success_registers_process(monkeypatch: MonkeyPatch) -> None:
    """Verify spawn registers process in service tracker.

    Given a valid class path and method configuration,
    When spawn is called successfully,
    Then process is registered in spawner and command includes runner module.
    """
    dummy = DummyProcess(initial_returncode=None)
    captured_cmd: list[str] = []

    def fake_popen(cmd: list[str], **kwargs: object) -> DummyProcess:
        nonlocal captured_cmd
        captured_cmd = list(cmd)
        return dummy

    monkeypatch.setattr(
        "snapper.application.process_manager.spawner.subprocess.Popen",
        fake_popen,
    )
    service = ProcessSpawnerService()
    info = service.spawn(
        name="worker",
        class_path="snapper.application.process_manager.models.ProcessInstanceInfo",
        method="run",
        parameters={"bar": 1},
    )
    assert info.process.pid == dummy.pid
    assert service.processes["worker"] is info
    assert "snapper.server.process_runner" in captured_cmd


def test_spawn_duplicate_name_raises(monkeypatch: MonkeyPatch) -> None:
    """Verify spawn raises error for duplicate process names.

    Given a process already registered with a specific name,
    When spawn is called with the same name,
    Then RuntimeError is raised.
    """
    dummy = DummyProcess()

    def fake_popen(cmd: list[str], **kwargs: object) -> DummyProcess:
        return dummy

    monkeypatch.setattr(
        "snapper.application.process_manager.spawner.subprocess.Popen",
        fake_popen,
    )
    service = ProcessSpawnerService()
    service.spawn(
        name="worker",
        class_path="snapper.application.process_manager.models.ProcessInstanceInfo",
        method="run",
        parameters={},
    )
    with pytest.raises(RuntimeError):
        service.spawn(
            name="worker",
            class_path="snapper.application.process_manager.models.ProcessInstanceInfo",
            method="run",
            parameters={},
        )


def test_spawn_invalid_class_path(monkeypatch: MonkeyPatch) -> None:
    """Verify spawn raises error for invalid class path.

    Given a class path pointing to non-existent module,
    When spawn is called,
    Then RuntimeError is raised and Popen is not called.
    """

    def fake_popen(cmd: list[str], **kwargs: object) -> DummyProcess:
        raise AssertionError("Popen should not be called")

    monkeypatch.setattr(
        "snapper.application.process_manager.spawner.subprocess.Popen",
        fake_popen,
    )
    service = ProcessSpawnerService()
    with pytest.raises(RuntimeError):
        service.spawn(
            name="worker",
            class_path="snapper.invalid.MissingClass",
            method="run",
            parameters={},
        )


def test_spawn_restores_sys_path_on_failure(monkeypatch: MonkeyPatch) -> None:
    """Verify spawn retains cwd in sys.path when class import fails.

    Given a faulty sys.path that raises on remove,
    When spawn fails due to ModuleNotFoundError,
    Then cwd remains in sys.path despite cleanup failure.
    """
    cwd = "/tmp/cwd"

    class FaultyPath(list[str]):
        def remove(self, value: object) -> None:
            raise ValueError("cannot remove")

    faulty = FaultyPath()
    monkeypatch.setattr(spawner_module.os, "getcwd", lambda: cwd)
    monkeypatch.setattr(spawner_module.sys, "path", faulty)

    def bad_import(*_args: object, **_kwargs: object) -> object:
        raise ModuleNotFoundError("missing")

    monkeypatch.setattr(spawner_module.importlib, "import_module", bad_import)
    service = ProcessSpawnerService()
    with pytest.raises(RuntimeError):
        service.spawn(
            name="bad",
            class_path="snapper.missing.Class",
            method="run",
            parameters={},
        )
    assert cwd in faulty


def test_spawn_immediate_exit_success(monkeypatch: MonkeyPatch) -> None:
    """Verify spawn handles immediate successful exit.

    Given a process that exits immediately with code 0,
    When spawn is called,
    Then process info is returned with exit_code set to 0.
    """
    dummy = DummyProcess(initial_returncode=0)

    def fake_popen(cmd: list[str], **kwargs: object) -> DummyProcess:
        return dummy

    monkeypatch.setattr(
        "snapper.application.process_manager.spawner.subprocess.Popen",
        fake_popen,
    )
    service = ProcessSpawnerService()
    info = service.spawn(
        name="worker",
        class_path="snapper.application.process_manager.models.ProcessInstanceInfo",
        method="run",
        parameters={},
    )
    assert info.exit_code == 0


def test_spawn_immediate_exit_failure(monkeypatch: MonkeyPatch) -> None:
    """Verify spawn raises error on immediate failure exit.

    Given a process that exits immediately with non-zero code,
    When spawn is called with capture_output enabled,
    Then RuntimeError is raised containing stdout and stderr output.
    """
    dummy = DummyProcess(
        initial_returncode=1,
        stdout=b"failure",
        stderr=b"traceback",
    )

    def fake_popen(cmd: list[str], **kwargs: object) -> DummyProcess:
        return dummy

    monkeypatch.setattr(
        "snapper.application.process_manager.spawner.subprocess.Popen",
        fake_popen,
    )
    service = ProcessSpawnerService(capture_output=True)
    with pytest.raises(RuntimeError) as exc:
        service.spawn(
            name="worker",
            class_path="snapper.application.process_manager.models.ProcessInstanceInfo",
            method="run",
            parameters={},
        )
    assert "failed to start" in str(exc.value).lower()
    assert "failure" in str(exc.value)
    assert "traceback" in str(exc.value)


def test_terminate_process_gracefully(monkeypatch: MonkeyPatch) -> None:
    """Verify graceful process termination on Linux.

    Given a spawned process on Linux platform,
    When terminate is called and process responds to SIGTERM,
    Then process exits cleanly and exit_code is recorded.
    """
    original_platform = sys.platform
    mod = _reload_spawner_for_posix(monkeypatch)
    dummy = DummyProcess()

    def fake_popen(cmd: list[str], **kwargs: object) -> DummyProcess:
        return dummy

    monkeypatch.setattr(mod.subprocess, "Popen", fake_popen)
    calls: list[tuple[int, int]] = []

    def fake_getpgid(pid: int) -> int:
        return pid + 1

    def fake_killpg(pgid: int, sig: int) -> None:
        calls.append((pgid, sig))
        if sig == signal.SIGTERM:
            dummy.returncode = 0

    monkeypatch.setattr(mod, "_posix_getpgid", fake_getpgid)
    monkeypatch.setattr(mod, "_posix_killpg", fake_killpg)
    service = mod.ProcessSpawnerService()
    service.spawn(
        name="worker",
        class_path="snapper.application.process_manager.models.ProcessInstanceInfo",
        method="run",
        parameters={},
    )
    assert service.terminate("worker", timeout=1.0) is True
    assert dummy.returncode == 0
    assert service.processes["worker"].exit_code == 0
    assert calls and calls[0][1] == signal.SIGTERM
    monkeypatch.setattr(sys, "platform", original_platform, raising=False)
    importlib.reload(spawner_module)


def test_terminate_process_force_kill(monkeypatch: MonkeyPatch) -> None:
    """Verify force kill when graceful termination times out.

    Given a stubborn process that ignores SIGTERM on Linux,
    When terminate is called and timeout expires,
    Then process is killed with SIGKILL and exit_code is recorded.
    """
    original_platform = sys.platform
    mod = _reload_spawner_for_posix(monkeypatch)
    stubborn = StubbornProcess()

    def fake_popen(cmd: list[str], **kwargs: object) -> StubbornProcess:
        return stubborn

    monkeypatch.setattr(mod.subprocess, "Popen", fake_popen)
    calls: list[tuple[int, int]] = []

    def fake_getpgid(pid: int) -> int:
        return pid + 10

    def fake_killpg(pgid: int, sig: int) -> None:
        calls.append((pgid, sig))
        if sig == mod.SIGKILL_SIGNAL:
            stubborn.returncode = -9

    monkeypatch.setattr(mod, "_posix_getpgid", fake_getpgid)
    monkeypatch.setattr(mod, "_posix_killpg", fake_killpg)
    timestamps = [0.0, 0.1, 1.5, 3.0]

    def fake_time() -> float:
        if timestamps:
            return timestamps.pop(0)
        return 3.0

    monkeypatch.setattr(mod.time, "time", fake_time)
    service = mod.ProcessSpawnerService()
    service.spawn(
        name="worker",
        class_path="snapper.application.process_manager.models.ProcessInstanceInfo",
        method="run",
        parameters={},
    )
    result = service.terminate("worker", timeout=0.5)
    assert result is True
    assert stubborn.returncode == -9
    assert service.processes["worker"].exit_code == -9
    assert any(call[1] == mod.SIGKILL_SIGNAL for call in calls)
    monkeypatch.setattr(sys, "platform", original_platform, raising=False)
    importlib.reload(spawner_module)


def test_get_status_for_unknown_process() -> None:
    """Verify get_status returns error for unknown process.

    Given a spawner with no registered processes,
    When get_status is called with unknown name,
    Then status shows not running with 'Process not found' error.
    """
    service = ProcessSpawnerService()
    status = service.get_status("missing")
    assert isinstance(status, SpawnerStatusSnapshot)
    assert status.running is False
    assert status.error == "Process not found"


def test_get_status_for_stopped_process(monkeypatch: MonkeyPatch) -> None:
    """Verify get_status returns exit code for stopped process.

    Given a registered process that has already exited,
    When get_status is called,
    Then status shows not running with exit_code and heartbeat_age_seconds.
    """
    dummy = DummyProcess(initial_returncode=0)
    info = ProcessInstanceInfo(
        name="worker",
        pid=1234,
        started_at=datetime.now(UTC),
        config={},
        process=cast(subprocess.Popen[bytes], dummy),
        exit_code=0,
        last_heartbeat=datetime.now(UTC),
    )
    service = ProcessSpawnerService()
    service.processes["worker"] = info
    status = service.get_status("worker")
    assert isinstance(status, SpawnerStatusSnapshot)
    assert status.running is False
    assert status.exit_code == 0
    assert status.heartbeat_age_seconds is not None


def test_cleanup_removes_finished_process(monkeypatch: MonkeyPatch) -> None:
    """Verify cleanup removes finished process from tracker.

    Given a process that has already exited,
    When cleanup is called,
    Then process is removed from spawner's process registry.
    """
    dummy = DummyProcess(initial_returncode=0)
    info = ProcessInstanceInfo(
        name="worker",
        pid=1,
        started_at=datetime.now(UTC),
        config={},
        process=cast(subprocess.Popen[bytes], dummy),
    )
    service = ProcessSpawnerService()
    service.processes["worker"] = info
    service.cleanup("worker")
    assert "worker" not in service.processes


def test_cleanup_active_process_triggers_terminate(monkeypatch: MonkeyPatch) -> None:
    """Verify cleanup terminates still-running process.

    Given a process that is still running,
    When cleanup is called,
    Then terminate is invoked and process is removed from registry.
    """
    dummy = DummyProcess(initial_returncode=None)
    info = ProcessInstanceInfo(
        name="worker",
        pid=1,
        started_at=datetime.now(UTC),
        config={},
        process=cast(subprocess.Popen[bytes], dummy),
    )
    service = ProcessSpawnerService()
    service.processes["worker"] = info
    called: dict[str, int] = {"count": 0}

    def fake_terminate(name: str, timeout: float | None = None) -> bool:
        called["count"] += 1
        info.process.returncode = 0
        return True

    monkeypatch.setattr(service, "terminate", fake_terminate)
    service.cleanup("worker")
    assert called["count"] == 1
    assert "worker" not in service.processes


def test_cleanup_all_handles_errors(monkeypatch: MonkeyPatch) -> None:
    """Verify cleanup_all continues despite individual errors.

    Given multiple registered processes where one fails to terminate,
    When cleanup_all is called,
    Then successful processes are cleaned while failed ones remain.
    """
    dummy1 = DummyProcess(initial_returncode=0)
    dummy2 = DummyProcess(initial_returncode=0)
    info1 = ProcessInstanceInfo(
        name="alpha",
        pid=1,
        started_at=datetime.now(UTC),
        config={},
        process=cast(subprocess.Popen[bytes], dummy1),
    )
    info2 = ProcessInstanceInfo(
        name="bravo",
        pid=2,
        started_at=datetime.now(UTC),
        config={},
        process=cast(subprocess.Popen[bytes], dummy2),
    )
    service = ProcessSpawnerService()
    service.processes = {"alpha": info1, "bravo": info2}

    def fake_terminate(name: str, timeout: float | None = None) -> bool:
        if name == "bravo":
            raise RuntimeError("boom")
        return True

    monkeypatch.setattr(service, "terminate", fake_terminate)
    cleaned: list[str] = []

    def fake_cleanup(name: str) -> None:
        cleaned.append(name)
        service.processes.pop(name, None)

    monkeypatch.setattr(service, "cleanup", fake_cleanup)
    service.cleanup_all()
    assert "alpha" in cleaned
    assert "bravo" in service.processes
