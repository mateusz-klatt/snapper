"""Process spawner service module.

This module provides subprocess spawning and management functionality.
It handles cross-platform (Windows/POSIX) process creation, termination,
and cleanup with proper process group management.
"""

import contextlib
import importlib
import json
import os
import signal
import subprocess
import sys
import time
from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from typing import Any
from typing import Final
from typing import cast

from loguru import logger

from snapper.application.process_manager.models import ProcessInstanceInfo
from snapper.application.process_manager.models import SpawnerStatusSnapshot

IS_WINDOWS = sys.platform == "win32"
CREATE_NEW_PROCESS_GROUP = 0x00000200 if IS_WINDOWS else 0
CTRL_BREAK_EVENT: Final[int] = getattr(signal, "CTRL_BREAK_EVENT", signal.SIGTERM)
SIGKILL_SIGNAL: Final[int] = getattr(signal, "SIGKILL", signal.SIGTERM)
_preexec_setsid: Callable[[], None] | None
_posix_killpg: Callable[[int, int], None] | None
_posix_getpgid: Callable[[int], int] | None
if IS_WINDOWS:
    _preexec_setsid = None
    _posix_killpg = None
    _posix_getpgid = None
else:
    _posix_os = cast(Any, os)
    _preexec_setsid = cast("Callable[[], None]", _posix_os.setsid)
    _posix_killpg = cast("Callable[[int, int], None]", _posix_os.killpg)
    _posix_getpgid = cast("Callable[[int], int]", _posix_os.getpgid)


def _build_process_command(
    name: str,
    class_path: str,
    method: str,
    args: list[Any],
    kwargs: dict[str, Any],
) -> list[str]:
    """Build command line for subprocess execution.

    Creates command to run process_runner module with JSON config.

    Args:
        name: Process name identifier.
        class_path: Fully qualified class path.
        method: Method name to execute.
        args: Positional arguments for constructor.
        kwargs: Keyword arguments for constructor.

    Returns:
        Command list for subprocess.Popen.
    """
    config: dict[str, Any] = {
        "name": name,
        "class_path": class_path,
        "method": method,
        "args": args,
        "kwargs": kwargs,
    }
    config_json = json.dumps(config)
    return [
        sys.executable,
        "-m",
        "snapper.server.process_runner",
        "--config",
        config_json,
    ]


class ProcessSpawnerService:
    """Service for spawning and managing subprocesses.

    Provides cross-platform subprocess management with:
    - Process group creation for clean termination
    - Graceful shutdown with SIGTERM followed by SIGKILL
    - Status monitoring and health checks
    - Output capture (optional)

    Attributes:
        processes: Dict mapping process name to ProcessInstanceInfo.
    """

    def __init__(self, capture_output: bool = False) -> None:
        """Initialize the spawner service.

        Args:
            capture_output: Whether to capture stdout/stderr.
                Defaults to False for cleaner console output.
        """
        self.processes: dict[str, ProcessInstanceInfo] = {}
        self._shutdown_timeout = 10.0
        self._startup_grace_period = 0.1
        self._capture_output = capture_output

    def _validate_class_path(self, name: str, class_path: str) -> None:
        """Validate that a class path is importable.

        Temporarily adds cwd to sys.path if needed for import resolution.

        Args:
            name: Process name for error messages.
            class_path: Fully qualified class path.

        Raises:
            RuntimeError: If class cannot be imported.
        """
        validation_path_added = False
        cwd = os.getcwd()
        if cwd not in sys.path:
            sys.path.insert(0, cwd)
            validation_path_added = True
        try:
            module_path, class_name = class_path.rsplit(".", 1)
            module = importlib.import_module(module_path)
            getattr(module, class_name)
        except (ValueError, ModuleNotFoundError, AttributeError) as exc:
            raise RuntimeError(
                f"Process '{name}' failed to start (invalid class '{class_path}')"
            ) from exc
        finally:
            if validation_path_added:
                with contextlib.suppress(ValueError):
                    sys.path.remove(cwd)

    def _build_process_info(
        self,
        name: str,
        class_path: str,
        method: str,
        args: list[Any],
        kwargs: dict[str, Any],
        process: subprocess.Popen[bytes],
        exit_code: int | None = None,
    ) -> ProcessInstanceInfo:
        """Build a ProcessInstanceInfo instance.

        Args:
            name: Process name.
            class_path: Fully qualified class path.
            method: Entry method name.
            args: Positional arguments.
            kwargs: Keyword arguments.
            process: Subprocess handle.
            exit_code: Optional exit code if process already exited.

        Returns:
            ProcessInstanceInfo with all fields populated.
        """
        now = datetime.now(UTC)
        info = ProcessInstanceInfo(
            name=name,
            pid=process.pid,
            started_at=now,
            config={
                "class_path": class_path,
                "method": method,
                "args": args,
                "kwargs": kwargs,
            },
            process=process,
            spawner=self,
            last_heartbeat=now,
        )
        if exit_code is not None:
            info.exit_code = exit_code
        return info

    def _build_early_exit_detail(
        self,
        process: subprocess.Popen[bytes],
        status: int,
    ) -> str:
        """Build detail suffix for a process that exited immediately.

        Args:
            process: Subprocess handle.
            status: Exit code.

        Returns:
            Detail suffix string for error messages.
        """
        stdout_msg = ""
        stderr_msg = ""
        if self._capture_output:
            stdout, stderr = process.communicate()
            stdout_msg = stdout.decode().strip() if stdout else ""
            stderr_msg = stderr.decode().strip() if stderr else ""
        detail_parts = [part for part in [stdout_msg, stderr_msg] if part]
        detail_suffix = f": {' '.join(detail_parts)}" if detail_parts else ""
        if not self._capture_output and detail_suffix == "" and status != 0:
            detail_suffix = "; see console output for details"
        return detail_suffix

    def _launch_subprocess(self, cmd: list[str]) -> subprocess.Popen[bytes]:
        """Create and start a subprocess with platform-appropriate settings.

        Args:
            cmd: Command list for subprocess.Popen.

        Returns:
            Started Popen handle.
        """
        preexec_fn: Callable[[], None] | None = _preexec_setsid
        creation_flags = CREATE_NEW_PROCESS_GROUP if IS_WINDOWS else 0
        stdout_stream: int | None = subprocess.PIPE if self._capture_output else None
        stderr_stream: int | None = subprocess.PIPE if self._capture_output else None
        return subprocess.Popen(
            cmd,
            stdout=stdout_stream,
            stderr=stderr_stream,
            preexec_fn=preexec_fn,
            creationflags=creation_flags,
        )

    def _register_spawned_process(
        self,
        name: str,
        class_path: str,
        method: str,
        args: list[Any],
        kwargs: dict[str, Any],
        process: subprocess.Popen[bytes],
        exit_code: int | None = None,
    ) -> ProcessInstanceInfo:
        """Build process info, store it in the registry, and return it.

        Args:
            name: Process name.
            class_path: Fully qualified class path.
            method: Entry method name.
            args: Positional arguments.
            kwargs: Keyword arguments.
            process: Subprocess handle.
            exit_code: Optional exit code if process already exited.

        Returns:
            Registered ProcessInstanceInfo.
        """
        info = self._build_process_info(
            name, class_path, method, args, kwargs, process, exit_code=exit_code
        )
        self.processes[name] = info
        return info

    def spawn(
        self,
        name: str,
        class_path: str,
        method: str,
        args: list[Any],
        kwargs: dict[str, Any],
    ) -> ProcessInstanceInfo:
        """Spawn a new subprocess.

        Validates the class path, builds the command, and starts
        the subprocess in a new process group.

        Args:
            name: Unique process identifier.
            class_path: Fully qualified class path to instantiate.
            method: Method to call on the instantiated class.
            args: Positional arguments for constructor.
            kwargs: Keyword arguments for constructor.

        Returns:
            ProcessInstanceInfo with subprocess details.

        Raises:
            RuntimeError: If process already exists or fails to start.
        """
        if name in self.processes:
            raise RuntimeError(f"Process '{name}' already exists")
        logger.info(f"Spawning process '{name}' (class: {class_path}, method: {method})")
        self._validate_class_path(name, class_path)
        cmd = _build_process_command(name, class_path, method, args, kwargs)
        process = self._launch_subprocess(cmd)
        time.sleep(0.1)
        status = process.poll()
        if status is None:
            info = self._register_spawned_process(name, class_path, method, args, kwargs, process)
            logger.info(f"Process '{name}' spawned with PID {info.pid}")
            return info
        if status == 0:
            logger.info("Process '{}' exited immediately after start", name)
            return self._register_spawned_process(
                name, class_path, method, args, kwargs, process, exit_code=status
            )
        detail_suffix = self._build_early_exit_detail(process, status)
        raise RuntimeError(f"Process '{name}' failed to start (exit code: {status}){detail_suffix}")

    @staticmethod
    def _send_signal_to_process(
        process: subprocess.Popen[bytes],
        sig: int,
        name: str,
        *,
        force: bool = False,
    ) -> None:
        """Send a signal to a process or its process group.

        On POSIX, sends to the process group. On Windows, sends
        directly to the process. Falls back to process.terminate()
        or process.kill() on OSError.

        Args:
            process: Subprocess handle.
            sig: Signal number to send.
            name: Process name for logging.
            force: If True, use kill() instead of graceful termination.
        """
        try:
            if IS_WINDOWS:
                if force:
                    process.kill()
                else:
                    process.send_signal(CTRL_BREAK_EVENT)
            else:
                assert _posix_killpg is not None
                assert _posix_getpgid is not None
                _posix_killpg(_posix_getpgid(process.pid), sig)
        except OSError as e:
            fallback_name = "kill" if force else "terminate"
            logger.debug(
                f"Signal {sig} failed for '{name}': {e}, falling back to {fallback_name}()"
            )
            if force:
                process.kill()
            else:
                process.terminate()

    def _wait_for_graceful_exit(
        self,
        name: str,
        info: ProcessInstanceInfo,
        timeout: float,
    ) -> bool:
        """Wait for process to exit gracefully within timeout.

        Args:
            name: Process name for logging.
            info: Process info with subprocess handle.
            timeout: Seconds to wait.

        Returns:
            True if process exited within timeout, False otherwise.
        """
        process = info.process
        t0 = time.time()
        while time.time() - t0 < timeout:
            if process.poll() is not None:
                info.exit_code = process.returncode
                logger.info(
                    f"Process '{name}' terminated gracefully (exit code: {process.returncode})"
                )
                return True
            time.sleep(0.1)
        return False

    def terminate(self, name: str, timeout: float | None = None) -> bool:
        """Terminate a process gracefully.

        First sends SIGTERM (or CTRL_BREAK on Windows), waits for
        graceful shutdown, then sends SIGKILL if timeout exceeded.

        Args:
            name: Process name to terminate.
            timeout: Seconds to wait for graceful shutdown.
                Defaults to _shutdown_timeout (10s).

        Returns:
            True if process was terminated, False if not found.

        Raises:
            TimeoutError: If process could not be killed.
        """
        if name not in self.processes:
            logger.warning(f"Process '{name}' not found")
            return False
        info = self.processes[name]
        process = info.process
        if process.poll() is not None:
            logger.info(f"Process '{name}' already dead (exit code: {process.returncode})")
            info.exit_code = process.returncode
            return True
        effective_timeout = self._shutdown_timeout if timeout is None else timeout
        logger.info(f"Terminating process '{name}' (PID: {info.pid})")
        self._send_signal_to_process(process, signal.SIGTERM, name)
        if self._wait_for_graceful_exit(name, info, effective_timeout):
            return True
        logger.warning(
            f"Process '{name}' didn't stop within {effective_timeout}s, force killing (SIGKILL)"
        )
        self._send_signal_to_process(process, SIGKILL_SIGNAL, name, force=True)
        try:
            process.wait(timeout=2.0)
        except subprocess.TimeoutExpired:
            raise TimeoutError(f"Process '{name}' (PID: {info.pid}) could not be killed") from None
        info.exit_code = process.returncode
        logger.info(f"Process '{name}' force killed (exit code: {process.returncode})")
        return True

    def get_status(self, name: str) -> SpawnerStatusSnapshot:
        """Get status information for a process.

        Args:
            name: Process name to query.

        Returns:
            SpawnerStatusSnapshot with running state, pid, uptime,
            exit_code (if terminated), and heartbeat info.
        """
        if name not in self.processes:
            return SpawnerStatusSnapshot(
                name=name,
                running=False,
                error="Process not found",
            )
        info = self.processes[name]
        is_alive = info.process.poll() is None
        snapshot = SpawnerStatusSnapshot(
            name=name,
            running=is_alive,
            pid=info.pid,
            started_at=info.started_at.isoformat(),
            uptime_seconds=(datetime.now(UTC) - info.started_at).total_seconds(),
        )
        if not is_alive:
            snapshot.exit_code = info.process.returncode
            snapshot.stopped_at = datetime.now(UTC).isoformat()
        if info.last_heartbeat:
            snapshot.last_heartbeat = info.last_heartbeat.isoformat()
            snapshot.heartbeat_age_seconds = (
                datetime.now(UTC) - info.last_heartbeat
            ).total_seconds()
        return snapshot

    def cleanup(self, name: str) -> None:
        """Clean up resources for a terminated process.

        Terminates if still running and removes from registry.

        Args:
            name: Process name to clean up.
        """
        if name in self.processes:
            info = self.processes[name]
            if info.process.poll() is None:
                logger.warning(f"Process '{name}' still alive during cleanup, terminating")
                self.terminate(name)
            del self.processes[name]
            logger.debug(f"Process '{name}' cleaned up")

    def cleanup_all(self) -> None:
        """Terminate and clean up all managed processes.

        Iterates through all processes and terminates/cleans each.
        Continues on errors to ensure all processes are attempted.
        """
        logger.info(f"Cleaning up {len(self.processes)} processes")
        for name in tuple(self.processes):
            try:
                self.terminate(name)
                self.cleanup(name)
            except Exception as e:
                logger.error(f"Error cleaning up process '{name}': {e}")

    def list_processes(self) -> list[SpawnerStatusSnapshot]:
        """List status of all managed processes.

        Returns:
            List of SpawnerStatusSnapshot for each process.
        """
        return [self.get_status(name) for name in self.processes]
