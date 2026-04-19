"""Tests for ProcessSpawnerService process isolation and crash handling."""

import os
import signal
import subprocess
import time
from datetime import UTC
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from snapper.application.process_manager.models import ProcessInstanceInfo
from snapper.application.process_manager.models import SpawnerStatusSnapshot
from snapper.application.process_manager.spawner import ProcessSpawnerService


class CrashingProcess:
    """Test process that crashes in configurable ways."""

    def __init__(self, crash_type: str) -> None:
        """Initialize the instance."""
        self.crash_type = crash_type

    def start(self) -> None:
        """Start process and crash according to configured type."""
        time.sleep(0.1)
        if self.crash_type == "exception":
            raise RuntimeError("Intentional crash!")
        elif self.crash_type == "exit":
            exit(42)
        elif self.crash_type == "sigterm":
            os.kill(os.getpid(), signal.SIGTERM)


class InfiniteProcess:
    """Test process that runs indefinitely."""

    def __init__(self) -> None:
        """Initialize the instance."""
        pass

    def start(self) -> None:
        """Run infinite loop until terminated."""
        while True:
            time.sleep(1)


def _no_sleep(seconds: float) -> None:
    return None


def test_crashing_process_exception_path(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify CrashingProcess raises RuntimeError for exception type.

    Given: CrashingProcess with crash_type='exception',
    When: start() is called,
    Then: RuntimeError is raised.
    """
    monkeypatch.setattr(time, "sleep", _no_sleep)
    process = CrashingProcess("exception")
    with pytest.raises(RuntimeError):
        process.start()


def test_crashing_process_exit_path(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify CrashingProcess exits with code 42 for exit type.

    Given: CrashingProcess with crash_type='exit',
    When: start() is called,
    Then: SystemExit with code 42 is raised.
    """
    monkeypatch.setattr(time, "sleep", _no_sleep)
    process = CrashingProcess("exit")
    with pytest.raises(SystemExit) as exc_info:
        process.start()
    assert exc_info.value.code == 42


def test_crashing_process_signal_path(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify CrashingProcess sends SIGTERM for signal type.

    Given: CrashingProcess with crash_type='sigterm',
    When: start() is called,
    Then: SIGTERM is sent to own PID.
    """
    monkeypatch.setattr(time, "sleep", _no_sleep)
    expected_pid = os.getpid()
    captured: list[tuple[int, int]] = []

    def fake_kill(pid: int, sig: int) -> None:
        captured.append((pid, sig))

    monkeypatch.setattr(os, "kill", fake_kill)
    process = CrashingProcess("sigterm")
    process.start()
    assert captured == [(expected_pid, signal.SIGTERM)]


def test_infinite_process_start_interrupt(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify InfiniteProcess handles KeyboardInterrupt.

    Given: InfiniteProcess with sleep that raises interrupt,
    When: start() is called,
    Then: KeyboardInterrupt propagates after second sleep call.
    """
    call_counter: dict[str, int] = {"count": 0}

    def fake_sleep(seconds: float) -> None:
        call_counter["count"] += 1
        if call_counter["count"] >= 2:
            raise KeyboardInterrupt()

    monkeypatch.setattr(time, "sleep", fake_sleep)
    process = InfiniteProcess()
    with pytest.raises(KeyboardInterrupt):
        process.start()
    assert call_counter["count"] == 2


class TestProcessIsolation:
    """Test cases for process isolation and parent survival."""

    def test_parent_survives_child_exception(self) -> None:
        """Verify parent process survives child exception crash.

        Given: Child process that raises RuntimeError,
        When: Process crashes,
        Then: Parent continues running with original PID.
        """
        spawner = ProcessSpawnerService()
        info = spawner.spawn(
            name="crash_exception",
            class_path="tests.application.process_manager.test_process_spawner.CrashingProcess",
            method="start",
            parameters={"crash_type": "exception"},
        )
        assert info.pid > 0
        parent_pid = os.getpid()
        assert info.pid != parent_pid
        max_wait = 5.0
        wait_time = 0.1
        elapsed = 0.0
        status = spawner.get_status("crash_exception")
        while status.running and elapsed < max_wait:
            time.sleep(wait_time)
            elapsed += wait_time
            wait_time = min(wait_time * 1.5, 0.5)
            status = spawner.get_status("crash_exception")
        assert status.running is False, f"Process still running after {elapsed:.1f}s"
        assert status.exit_code != 0
        assert os.getpid() == parent_pid

    def test_parent_survives_child_exit(self) -> None:
        """Verify parent process survives child exit(42) call.

        Given: Child process that calls exit(42),
        When: Process exits,
        Then: Parent continues and exit code 42 is captured.
        """
        spawner = ProcessSpawnerService()
        info = spawner.spawn(
            name="crash_exit",
            class_path="tests.application.process_manager.test_process_spawner.CrashingProcess",
            method="start",
            parameters={"crash_type": "exit"},
        )
        assert info.pid > 0
        parent_pid = os.getpid()
        max_wait = 5.0
        wait_time = 0.1
        elapsed = 0.0
        status = spawner.get_status("crash_exit")
        while status.running and elapsed < max_wait:
            time.sleep(wait_time)
            elapsed += wait_time
            wait_time = min(wait_time * 1.5, 0.5)
            status = spawner.get_status("crash_exit")
        assert status.running is False, f"Process still running after {elapsed:.1f}s"
        assert status.exit_code == 42
        assert os.getpid() == parent_pid

    def test_parent_survives_child_signal(self) -> None:
        """Verify parent process survives child SIGTERM.

        Given: Child process that sends SIGTERM to itself,
        When: Process terminates,
        Then: Parent continues and SIGTERM exit code is captured.
        """
        spawner = ProcessSpawnerService()
        info = spawner.spawn(
            name="crash_signal",
            class_path="tests.application.process_manager.test_process_spawner.CrashingProcess",
            method="start",
            parameters={"crash_type": "sigterm"},
        )
        assert info.pid > 0
        parent_pid = os.getpid()
        max_wait = 5.0
        wait_time = 0.1
        elapsed = 0.0
        status = spawner.get_status("crash_signal")
        while status.running and elapsed < max_wait:
            time.sleep(wait_time)
            elapsed += wait_time
            wait_time = min(wait_time * 1.5, 0.5)
            status = spawner.get_status("crash_signal")
        assert status.running is False, f"Process still running after {elapsed:.1f}s"
        exit_code = status.exit_code
        assert exit_code in {signal.SIGTERM, -signal.SIGTERM}
        assert os.getpid() == parent_pid

    def test_parent_can_kill_hanging_child(self) -> None:
        """Verify parent can terminate hanging child process.

        Given: Child process running infinite loop,
        When: terminate is called,
        Then: Process is stopped and parent continues.
        """
        spawner = ProcessSpawnerService()
        info = spawner.spawn(
            name="hanging_process",
            class_path="tests.application.process_manager.test_process_spawner.InfiniteProcess",
            method="start",
            parameters={},
        )
        assert info.pid > 0
        time.sleep(0.2)
        status = spawner.get_status("hanging_process")
        assert status.running is True
        spawner.terminate("hanging_process", timeout=0.1)
        max_wait = 3.0
        wait_time = 0.1
        elapsed = 0.0
        status = spawner.get_status("hanging_process")
        while status.running and elapsed < max_wait:
            time.sleep(wait_time)
            elapsed += wait_time
            wait_time = min(wait_time * 1.5, 0.5)
            status = spawner.get_status("hanging_process")
        assert status.running is False, f"Process still running after {elapsed:.1f}s"
        assert os.getpid() > 0

    def test_multiple_child_crashes_dont_affect_parent(self) -> None:
        """Verify parent survives multiple concurrent child crashes.

        Given: Five child processes that all crash,
        When: All processes fail,
        Then: Parent continues with original PID.
        """
        spawner = ProcessSpawnerService()
        parent_pid = os.getpid()
        for i in range(5):
            info = spawner.spawn(
                name=f"crash_{i}",
                class_path="tests.application.process_manager.test_process_spawner.CrashingProcess",
                method="start",
                parameters={"crash_type": "exception"},
            )
            assert info.pid > 0
        max_wait = 10.0
        wait_time = 0.1
        elapsed = 0.0
        all_dead = False
        while not all_dead and elapsed < max_wait:
            time.sleep(wait_time)
            elapsed += wait_time
            wait_time = min(wait_time * 1.5, 0.5)
            all_dead = all(not spawner.get_status(f"crash_{i}").running for i in range(5))
        for i in range(5):
            status = spawner.get_status(f"crash_{i}")
            assert status.running is False, f"Process crash_{i} still running after {elapsed:.1f}s"
            assert status.exit_code != 0
        assert os.getpid() == parent_pid


class DummyProcess:
    """Simple test process that sleeps for specified duration."""

    def __init__(self, duration: float = 0.5) -> None:
        """Initialize the instance."""
        self.duration = duration

    def start(self) -> None:
        """Sleep for configured duration."""
        time.sleep(self.duration)


def _wait_until_not_running(
    spawner: ProcessSpawnerService, name: str, timeout: float = 2.0
) -> SpawnerStatusSnapshot:
    deadline = time.time() + timeout
    last_status: SpawnerStatusSnapshot = spawner.get_status(name)
    while last_status.running and time.time() < deadline:
        time.sleep(0.05)
        last_status = spawner.get_status(name)
    return last_status


class TestProcessSpawner:
    """Test cases for ProcessSpawnerService functionality."""

    def test_spawn_process_creates_pid(self) -> None:
        """Verify spawn creates process with valid PID.

        Given: ProcessSpawnerService instance,
        When: spawn called with valid parameters,
        Then: Process created with PID and running status.
        """
        spawner = ProcessSpawnerService()
        process_info = spawner.spawn(
            name="test_process",
            class_path="tests.application.process_manager.test_process_spawner.DummyProcess",
            method="start",
            parameters={"duration": 0.5},
        )
        assert process_info.pid is not None
        assert process_info.pid > 0
        assert process_info.started_at is not None
        status = spawner.get_status("test_process")
        assert status.running is True
        assert status.pid == process_info.pid
        spawner.terminate("test_process")

    def test_process_status_tracking(self) -> None:
        """Verify process status tracking reports correct state.

        Given: Running spawned process,
        When: get_status called,
        Then: Correct running status and exit code returned.
        """
        spawner = ProcessSpawnerService()
        spawner.spawn(
            name="status_test",
            class_path="tests.application.process_manager.test_process_spawner.DummyProcess",
            method="start",
            parameters={"duration": 1.0},
        )
        status = spawner.get_status("status_test")
        assert status.running is True
        assert status.exit_code is None
        status = _wait_until_not_running(spawner, "status_test", timeout=5.0)
        assert status.running is False
        assert status.exit_code == 0

    def test_terminate_process_graceful(self) -> None:
        """Verify graceful process termination.

        Given: Running process,
        When: terminate called,
        Then: Process stops and status updated.
        """
        spawner = ProcessSpawnerService()
        process_info = spawner.spawn(
            name="graceful_test",
            class_path="tests.application.process_manager.test_process_spawner.DummyProcess",
            method="start",
            parameters={"duration": 5.0},
        )
        pid = process_info.pid
        assert pid is not None
        start = time.time()
        success = spawner.terminate("graceful_test", timeout=2.0)
        elapsed = time.time() - start
        assert success is True
        assert elapsed < 3.0
        status = spawner.get_status("graceful_test")
        assert status.running is False

    def test_terminate_process_forced_kill(self) -> None:
        """Verify forced process termination.

        Given: Running process,
        When: terminate called with force,
        Then: Process killed and status updated.
        """
        spawner = ProcessSpawnerService()
        spawner.spawn(
            name="forced_kill_test",
            class_path="tests.application.process_manager.test_process_spawner.DummyProcess",
            method="start",
            parameters={"duration": 10.0},
        )
        start = time.time()
        success = spawner.terminate("forced_kill_test", timeout=0.1)
        elapsed = time.time() - start
        assert success is True
        assert elapsed < 1.0
        status = spawner.get_status("forced_kill_test")
        assert status.running is False

    def test_get_status_nonexistent_process(self) -> None:
        """Verify get_status returns error for unknown process.

        Given: No process registered,
        When: get_status called with unknown name,
        Then: Error key present in response.
        """
        spawner = ProcessSpawnerService()
        status = spawner.get_status("nonexistent")
        assert status.running is False
        assert status.error is not None

    def test_terminate_nonexistent_process(self) -> None:
        """Verify terminate returns False for unknown process.

        Given: No process registered,
        When: terminate called with unknown name,
        Then: False returned.
        """
        spawner = ProcessSpawnerService()
        success = spawner.terminate("nonexistent")
        assert success is False

    def test_spawn_failure_handling(self) -> None:
        """Verify spawn raises error for invalid class path.

        Given: ProcessSpawnerService instance,
        When: spawn called with invalid class,
        Then: RuntimeError raised.
        """
        spawner = ProcessSpawnerService()
        with pytest.raises(RuntimeError, match="failed to start"):
            spawner.spawn(
                name="failing_spawn",
                class_path="nonexistent.module.Class",
                method="start",
                parameters={},
            )

    def test_multiple_processes(self) -> None:
        """Verify spawner manages multiple processes correctly.

        Given: ProcessSpawnerService instance,
        When: multiple processes spawned,
        Then: All processes tracked independently.
        """
        spawner = ProcessSpawnerService()
        for i in range(3):
            spawner.spawn(
                name=f"multi_test_{i}",
                class_path="tests.application.process_manager.test_process_spawner.DummyProcess",
                method="start",
                parameters={"duration": 1.0},
            )
        for i in range(3):
            status = spawner.get_status(f"multi_test_{i}")
            assert status.running is True
        for i in range(3):
            spawner.terminate(f"multi_test_{i}")
        for i in range(3):
            status = spawner.get_status(f"multi_test_{i}")
            assert status.running is False

    def test_no_zombie_processes(self) -> None:
        """Verify terminated processes are cleaned up properly.

        Given: Multiple short-lived processes,
        When: processes complete,
        Then: No zombie processes remain.
        """
        spawner = ProcessSpawnerService()
        for i in range(5):
            spawner.spawn(
                name=f"zombie_test_{i}",
                class_path="tests.application.process_manager.test_process_spawner.DummyProcess",
                method="start",
                parameters={"duration": 0.1},
            )
            spawner.terminate(f"zombie_test_{i}")
        for i in range(5):
            status = spawner.get_status(f"zombie_test_{i}")
            assert status.running is False

    def test_process_exit_code_captured(self) -> None:
        """Verify exit code captured after process completion.

        Given: Completed process,
        When: get_status called,
        Then: Correct exit code returned.
        """
        spawner = ProcessSpawnerService()
        spawner.spawn(
            name="exitcode_test",
            class_path="tests.application.process_manager.test_process_spawner.DummyProcess",
            method="start",
            parameters={"duration": 0.2},
        )
        status = _wait_until_not_running(spawner, "exitcode_test", timeout=5.0)
        assert status.running is False
        assert status.exit_code == 0

    def test_spawner_init(self) -> None:
        """Verify spawner initializes with empty processes dict.

        Given: New ProcessSpawnerService,
        When: initialized,
        Then: processes dict is empty.
        """
        spawner = ProcessSpawnerService()
        assert spawner.processes == {}

    def test_concurrent_spawns(self) -> None:
        """Verify concurrent process spawning works correctly.

        Given: ProcessSpawnerService instance,
        When: multiple processes spawned concurrently,
        Then: All processes running with unique PIDs.
        """
        spawner = ProcessSpawnerService()
        names: list[str] = []
        for i in range(5):
            name = f"concurrent_{i}"
            names.append(name)
            spawner.spawn(
                name=name,
                class_path="tests.application.process_manager.test_process_spawner.DummyProcess",
                method="start",
                parameters={"duration": 2.0},
            )
        pids: set[int] = set()
        for name in names:
            status = spawner.get_status(name)
            assert status.running is True
            pids.add(status.pid)
        assert len(pids) == 5
        for name in names:
            spawner.terminate(name)

    def test_uptime_tracking(self) -> None:
        """Verify process uptime is tracked correctly.

        Given: Running process,
        When: get_status called after delay,
        Then: Uptime reflects elapsed time.
        """
        spawner = ProcessSpawnerService()
        spawner.spawn(
            name="uptime_test",
            class_path="tests.application.process_manager.test_process_spawner.DummyProcess",
            method="start",
            parameters={"duration": 1.0},
        )
        time.sleep(0.3)
        status = spawner.get_status("uptime_test")
        assert status.running is True
        assert status.uptime_seconds is not None
        assert status.uptime_seconds >= 0.15
        assert status.uptime_seconds < 1.0
        spawner.terminate("uptime_test")


class TestProcessInfo:
    """Test cases for ProcessInstanceInfo data structure."""

    def test_process_info_structure(self) -> None:
        """Verify ProcessInstanceInfo contains expected fields.

        Given: ProcessInstanceInfo with all fields,
        When: fields accessed,
        Then: Correct values returned.
        """
        mock_popen = MagicMock(spec=subprocess.Popen)
        info = ProcessInstanceInfo(
            name="test",
            pid=12345,
            started_at=datetime.now(UTC),
            config={"key": "value"},
            process=mock_popen,
        )
        assert info.name == "test"
        assert info.pid == 12345
        assert info.started_at is not None
        assert info.config == {"key": "value"}
        assert info.process is not None
        assert info.exit_code is None
        assert info.last_heartbeat is None
