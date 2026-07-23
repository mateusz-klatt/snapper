"""Tests for the disconnected runner-only delegate PID1."""

import asyncio
import os
import signal
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

import snapper_delegate.pid1 as pid1


def _ready_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, str]:
    """Create two ready file references and their exact environment."""
    api_key_file = tmp_path / "api_key"
    delegate_token_file = tmp_path / "delegate_token"
    api_key_file.write_bytes(b"k")
    delegate_token_file.write_bytes(b"t")
    monkeypatch.setattr(pid1, "_API_KEY_PATH", api_key_file)
    monkeypatch.setattr(pid1, "_DELEGATE_TOKEN_PATH", delegate_token_file)
    return {
        "SNAPPER_DELEGATE_MODEL_ALIAS": "research-primary",
        "SNAPPER_DELEGATE_BASE_URL": "https://api.example.invalid/",
        "SNAPPER_DELEGATE_API_KEY_FILE": str(api_key_file),
        "SNAPPER_DELEGATE_TOKEN_FILE": str(delegate_token_file),
        "SNAPPER_DELEGATE_MAX_TOOL_ROUNDS": "4",
    }


def test_load_runner_configuration_accepts_one_ready_model(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A complete reference-only route becomes one normalized configuration.

    Given: One model alias, one HTTPS origin, and two readable secret files,
    When: The PID1 loads its environment,
    Then: It returns strict runner parameters and strips the origin slash.
    """
    environment = _ready_environment(tmp_path, monkeypatch)
    configuration = pid1.load_runner_configuration(environment)
    assert configuration.model_dump() == {
        "model_alias": "research-primary",
        "base_url": "https://api.example.invalid",
        "api_key_file": environment["SNAPPER_DELEGATE_API_KEY_FILE"],
        "delegate_token_file": environment["SNAPPER_DELEGATE_TOKEN_FILE"],
        "max_tool_rounds": 4,
    }


def test_load_runner_configuration_accepts_explicit_https_port(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An explicit standard TLS port remains a valid origin.

    Given: A complete configuration using explicit TCP port 443,
    When: The PID1 validates the endpoint,
    Then: The configuration is accepted without rewriting the port.
    """
    environment = _ready_environment(tmp_path, monkeypatch)
    environment["SNAPPER_DELEGATE_BASE_URL"] = "https://api.example.invalid:443"
    configuration = pid1.load_runner_configuration(environment)
    assert configuration.base_url == "https://api.example.invalid:443"


@pytest.mark.parametrize(
    "missing_name",
    [
        "SNAPPER_DELEGATE_MODEL_ALIAS",
        "SNAPPER_DELEGATE_BASE_URL",
        "SNAPPER_DELEGATE_API_KEY_FILE",
        "SNAPPER_DELEGATE_TOKEN_FILE",
        "SNAPPER_DELEGATE_MAX_TOOL_ROUNDS",
    ],
)
def test_load_runner_configuration_refuses_missing_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    missing_name: str,
) -> None:
    """Every one-model environment field is required.

    Given: An otherwise valid environment missing one required name,
    When: The PID1 loads it,
    Then: It raises the same value-independent refusal.
    """
    environment = _ready_environment(tmp_path, monkeypatch)
    del environment[missing_name]
    with pytest.raises(pid1.RunnerOnlyConfigurationError) as caught:
        pid1.load_runner_configuration(environment)
    assert str(caught.value) == "Runner-only delegate configuration is incomplete or invalid"


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("SNAPPER_DELEGATE_MODEL_ALIAS", ""),
        ("SNAPPER_DELEGATE_MODEL_ALIAS", "two,models"),
        ("SNAPPER_DELEGATE_MODEL_ALIAS", " route"),
        ("SNAPPER_DELEGATE_MODEL_ALIAS", "a" * 129),
        ("SNAPPER_DELEGATE_MODEL_ALIAS", "sk-abcdefghijklmnop"),
        ("SNAPPER_DELEGATE_MODEL_ALIAS", "aaaaaaaa.bbbbbbbb.cccccccc"),
        ("SNAPPER_DELEGATE_MODEL_ALIAS", "a" * 32),
        ("SNAPPER_DELEGATE_BASE_URL", "http://api.example.invalid"),
        ("SNAPPER_DELEGATE_BASE_URL", " https://api.example.invalid"),
        ("SNAPPER_DELEGATE_BASE_URL", "https://api.example.invalid/a path"),
        ("SNAPPER_DELEGATE_BASE_URL", "https://user@api.example.invalid"),
        ("SNAPPER_DELEGATE_BASE_URL", "https://api.example.invalid/v1"),
        ("SNAPPER_DELEGATE_BASE_URL", "https://api.example.invalid?key=value"),
        ("SNAPPER_DELEGATE_BASE_URL", "https://api.example.invalid#fragment"),
        ("SNAPPER_DELEGATE_BASE_URL", "https://api.example.invalid:8443"),
        ("SNAPPER_DELEGATE_BASE_URL", "https://api.example.invalid:"),
        ("SNAPPER_DELEGATE_BASE_URL", "https://api.example.invalid:/"),
        ("SNAPPER_DELEGATE_BASE_URL", "https://api.example.invalid:0443"),
        ("SNAPPER_DELEGATE_BASE_URL", "https://127.0.0.1"),
        ("SNAPPER_DELEGATE_BASE_URL", "https://localhost"),
        ("SNAPPER_DELEGATE_BASE_URL", "https://api.example.invalid:bad"),
        ("SNAPPER_DELEGATE_API_KEY_FILE", "/tmp/key"),
        ("SNAPPER_DELEGATE_TOKEN_FILE", "/tmp/token"),
        ("SNAPPER_DELEGATE_MAX_TOOL_ROUNDS", "0"),
        ("SNAPPER_DELEGATE_MAX_TOOL_ROUNDS", "9"),
        ("SNAPPER_DELEGATE_MAX_TOOL_ROUNDS", "04"),
        ("SNAPPER_DELEGATE_MAX_TOOL_ROUNDS", "+4"),
        ("SNAPPER_DELEGATE_MAX_TOOL_ROUNDS", "4.0"),
        ("SNAPPER_DELEGATE_MAX_TOOL_ROUNDS", "４"),
        ("SNAPPER_DELEGATE_MAX_TOOL_ROUNDS", "9" * 5000),
    ],
)
def test_load_runner_configuration_refuses_malformed_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    value: str,
) -> None:
    """Malformed routes, origins, references, and bounds fail closed.

    Given: One unsafe or coercive environment value,
    When: The PID1 validates the one-model route,
    Then: It emits only the generic configuration refusal.
    """
    environment = _ready_environment(tmp_path, monkeypatch)
    environment[name] = value
    with pytest.raises(pid1.RunnerOnlyConfigurationError) as caught:
        pid1.load_runner_configuration(environment)
    assert str(caught.value) == "Runner-only delegate configuration is incomplete or invalid"


def test_load_runner_configuration_refuses_unknown_delegate_names(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A plural or future delegate variable cannot add a second model.

    Given: A valid route plus an unrecognized prefixed environment name,
    When: The PID1 loads it,
    Then: Strict environment validation refuses the entire configuration.
    """
    environment = _ready_environment(tmp_path, monkeypatch)
    environment["SNAPPER_DELEGATE_MODEL_ALIASES"] = "research-primary,research-secondary"
    with pytest.raises(pid1.RunnerOnlyConfigurationError):
        pid1.load_runner_configuration(environment)


@pytest.mark.parametrize("reference_name", ["api", "token"])
def test_load_runner_configuration_refuses_missing_secret_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reference_name: str,
) -> None:
    """A syntactically valid path does not count as a ready secret.

    Given: A complete environment whose selected secret file disappears,
    When: The PID1 loads it,
    Then: Configuration remains refused rather than starting the runner.
    """
    environment = _ready_environment(tmp_path, monkeypatch)
    environment_name = (
        "SNAPPER_DELEGATE_API_KEY_FILE"
        if reference_name == "api"
        else "SNAPPER_DELEGATE_TOKEN_FILE"
    )
    Path(environment[environment_name]).unlink()
    with pytest.raises(pid1.RunnerOnlyConfigurationError):
        pid1.load_runner_configuration(environment)


@pytest.mark.parametrize("reference_name", ["api", "token"])
def test_load_runner_configuration_refuses_empty_secret_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reference_name: str,
) -> None:
    """An empty mounted file keeps the canary unconfigured.

    Given: A complete environment with one zero-byte secret file,
    When: The PID1 verifies file readiness,
    Then: It refuses to instantiate the runner.
    """
    environment = _ready_environment(tmp_path, monkeypatch)
    environment_name = (
        "SNAPPER_DELEGATE_API_KEY_FILE"
        if reference_name == "api"
        else "SNAPPER_DELEGATE_TOKEN_FILE"
    )
    Path(environment[environment_name]).write_bytes(b"")
    with pytest.raises(pid1.RunnerOnlyConfigurationError):
        pid1.load_runner_configuration(environment)


def test_file_reference_read_error_is_not_exposed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unreadable reference becomes a generic readiness failure.

    Given: A regular file whose open operation raises an OS error,
    When: File readiness is checked,
    Then: The helper returns false without exposing the failure detail.
    """
    path = tmp_path / "api_key"
    path.write_bytes(b"k")
    monkeypatch.setattr(Path, "open", MagicMock(side_effect=PermissionError("denied")))
    assert pid1._file_reference_is_ready(path) is False


def test_configuration_model_is_strict() -> None:
    """Direct model construction does not coerce round-count strings.

    Given: A payload with a string where a strict integer is required,
    When: The configuration model validates it,
    Then: Pydantic rejects the coercive input.
    """
    with pytest.raises(ValidationError):
        pid1.RunnerOnlyConfiguration.model_validate(
            {
                "model_alias": "research-primary",
                "base_url": "https://api.example.invalid",
                "api_key_file": str(pid1._API_KEY_PATH),
                "delegate_token_file": str(pid1._DELEGATE_TOKEN_PATH),
                "max_tool_rounds": "4",
            }
        )


@pytest.mark.asyncio
async def test_run_pid1_stays_idle_when_unconfigured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Invalid configuration never instantiates the managed runner.

    Given: An empty environment and an injected finite idle waiter,
    When: The runner-only lifecycle starts,
    Then: It idles once and never constructs a DelegateRunner.
    """
    idle = AsyncMock()
    runner_factory = MagicMock()
    monkeypatch.setattr(pid1, "_idle_unconfigured", idle)
    monkeypatch.setattr(pid1, "DelegateRunner", runner_factory)
    await pid1.run_pid1({})
    idle.assert_awaited_once_with()
    runner_factory.assert_not_called()


@pytest.mark.asyncio
async def test_run_pid1_instantiates_existing_runner_directly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A ready route runs the existing delegate without a coordinator.

    Given: A complete environment and a fake existing runner,
    When: The PID1 starts,
    Then: It passes only the five runner parameters and awaits start directly.
    """
    environment = _ready_environment(tmp_path, monkeypatch)
    fake_runner = MagicMock()
    fake_runner.start = AsyncMock()
    runner_factory = MagicMock(return_value=fake_runner)
    monkeypatch.setattr(pid1, "DelegateRunner", runner_factory)
    await pid1.run_pid1(environment)
    runner_factory.assert_called_once_with(
        model_alias="research-primary",
        base_url="https://api.example.invalid",
        api_key_file=environment["SNAPPER_DELEGATE_API_KEY_FILE"],
        delegate_token_file=environment["SNAPPER_DELEGATE_TOKEN_FILE"],
        max_tool_rounds=4,
    )
    fake_runner.start.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_run_pid1_reads_process_environment_by_default(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The production entrypoint reads its own scrubbed environment.

    Given: No injected mapping and a complete process environment,
    When: The PID1 starts,
    Then: It resolves that environment and awaits the direct runner.
    """
    environment = _ready_environment(tmp_path, monkeypatch)
    fake_runner = MagicMock()
    fake_runner.start = AsyncMock()
    monkeypatch.setattr("snapper_delegate.pid1.os.environ", environment)
    monkeypatch.setattr(pid1, "DelegateRunner", MagicMock(return_value=fake_runner))
    await pid1.run_pid1()
    fake_runner.start.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_unconfigured_idle_handles_termination_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The fail-closed idle state exits cleanly on container termination.

    Given: An event loop supporting POSIX signal handlers,
    When: The captured termination callback runs,
    Then: The idle coroutine exits and removes its installed handler.
    """
    callbacks: dict[signal.Signals, Callable[[], None]] = {}
    removed: list[signal.Signals] = []

    class _SignalLoop:
        def add_signal_handler(
            self,
            stop_signal: signal.Signals,
            callback: Callable[[], None],
        ) -> None:
            callbacks[stop_signal] = callback

        def remove_signal_handler(self, stop_signal: signal.Signals) -> bool:
            removed.append(stop_signal)
            return True

    monkeypatch.setattr(asyncio, "get_running_loop", lambda: _SignalLoop())
    task = asyncio.create_task(pid1._idle_unconfigured())
    await asyncio.sleep(0)
    callbacks[signal.SIGTERM]()
    await task
    assert removed == [signal.SIGTERM]


@pytest.mark.parametrize("signal_error", [NotImplementedError, RuntimeError])
@pytest.mark.asyncio
async def test_unconfigured_idle_propagates_cancellation_without_signal_support(
    monkeypatch: pytest.MonkeyPatch,
    signal_error: type[Exception],
) -> None:
    """Unsupported signal registration still leaves a cancellable idle task.

    Given: A loop whose signal registration is unavailable,
    When: The owning task is cancelled,
    Then: Cancellation propagates and no nonexistent handler is removed.
    """
    removed: list[signal.Signals] = []

    class _SignalLoop:
        def add_signal_handler(
            self,
            stop_signal: signal.Signals,
            callback: Callable[[], None],
        ) -> None:
            del stop_signal, callback
            raise signal_error

        def remove_signal_handler(self, stop_signal: signal.Signals) -> bool:
            removed.append(stop_signal)
            return True

    monkeypatch.setattr(asyncio, "get_running_loop", lambda: _SignalLoop())
    task = asyncio.create_task(pid1._idle_unconfigured())
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert removed == []


@pytest.mark.asyncio
async def test_unconfigured_idle_can_finish_without_installed_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A finite fallback waiter exits without removing an absent handler.

    Given: Unsupported signal registration and an immediately completed wait,
    When: The fail-closed idle helper runs,
    Then: It returns normally without attempting handler removal.
    """
    removed: list[signal.Signals] = []

    class _ImmediateEvent:
        def set(self) -> None:
            return

        async def wait(self) -> None:
            await asyncio.sleep(0)

    class _SignalLoop:
        def add_signal_handler(
            self,
            stop_signal: signal.Signals,
            callback: Callable[[], None],
        ) -> None:
            del stop_signal, callback
            raise NotImplementedError

        def remove_signal_handler(self, stop_signal: signal.Signals) -> bool:
            removed.append(stop_signal)
            return True

    monkeypatch.setattr("snapper_delegate.pid1.asyncio.Event", _ImmediateEvent)
    monkeypatch.setattr("snapper_delegate.pid1.asyncio.get_running_loop", lambda: _SignalLoop())
    await pid1._idle_unconfigured()
    assert removed == []


def test_main_runs_async_pid1(monkeypatch: pytest.MonkeyPatch) -> None:
    """The module entrypoint delegates exactly once to the async lifecycle.

    Given: A finite fake runner-only coroutine,
    When: ``main`` is invoked,
    Then: It returns success after awaiting that coroutine.
    """
    run_pid1 = AsyncMock()
    configure_file_logging = MagicMock()
    monkeypatch.setattr(pid1, "run_pid1", run_pid1)
    monkeypatch.setattr(pid1, "_configure_file_logging", configure_file_logging)
    assert pid1.main() == 0
    configure_file_logging.assert_called_once_with(os.environ)
    run_pid1.assert_awaited_once_with()


def test_file_logging_uses_only_exact_model_log_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The mounted model logfile is attached without dynamic path selection.

    Given: The exact expected log path and a fake Loguru sink installer,
    When: PID1 logging is configured,
    Then: The path is added with non-diagnostic, synchronous options.
    """
    log_file = tmp_path / "delegate.log"
    add = MagicMock()
    monkeypatch.setattr(pid1, "_LOG_FILE_PATH", log_file)
    monkeypatch.setattr("snapper_delegate.pid1.logger.add", add)
    pid1._configure_file_logging({"SNAPPER_LOG_FILE": str(log_file)})
    add.assert_called_once_with(
        str(log_file),
        level="INFO",
        backtrace=False,
        diagnose=False,
        enqueue=False,
    )


def test_file_logging_ignores_unapproved_path(monkeypatch: pytest.MonkeyPatch) -> None:
    """An arbitrary environment path cannot redirect runner logs.

    Given: A logfile value outside the fixed model mount,
    When: PID1 logging is configured,
    Then: No file sink is attached.
    """
    add = MagicMock()
    monkeypatch.setattr("snapper_delegate.pid1.logger.add", add)
    pid1._configure_file_logging({"SNAPPER_LOG_FILE": "/tmp/unapproved.log"})
    add.assert_not_called()


@pytest.mark.parametrize("logging_error", [OSError, ValueError])
def test_file_logging_failure_keeps_standard_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    logging_error: type[Exception],
) -> None:
    """A mount or sink failure does not create a PID1 restart loop.

    Given: The approved path and a file sink that fails to initialize,
    When: PID1 configures logging,
    Then: It records a value-independent standard-output fallback.
    """
    log_file = tmp_path / "delegate.log"
    info = MagicMock()
    monkeypatch.setattr(pid1, "_LOG_FILE_PATH", log_file)
    monkeypatch.setattr(
        "snapper_delegate.pid1.logger.add",
        MagicMock(side_effect=logging_error("failed")),
    )
    monkeypatch.setattr("snapper_delegate.pid1.logger.info", info)
    pid1._configure_file_logging({"SNAPPER_LOG_FILE": str(log_file)})
    info.assert_called_once_with(
        "Runner-only delegate logfile is unavailable; using standard output"
    )


def test_pid1_source_has_no_coordinator_data_or_bus_imports() -> None:
    """The dedicated PID1 source cannot import management or transport planes.

    Given: The shipped runner-only module source,
    When: Its import text is inspected,
    Then: Coordinator, database, messaging, and ZMQ paths are absent.
    """
    source = Path(pid1.__file__).read_text(encoding="utf-8")
    forbidden = (
        "process_manager.launcher",
        "process_manager.spawner",
        "snapper.data",
        "snapper.messaging",
        "import zmq",
    )
    assert all(candidate not in source for candidate in forbidden)


def test_pid1_import_does_not_load_database_bus_or_coordinator() -> None:
    """A clean interpreter imports only the dependency-light agent lifecycle.

    Given: A fresh Python process with the integration and Snapper on its path,
    When: It imports the runner-only PID1 and audits loaded module names,
    Then: No registration, DB, messaging, ZMQ, launcher, or spawner has loaded.
    """
    repository_root = Path(__file__).resolve().parents[3]
    child_environment = os.environ.copy()
    child_environment["PYTHONPATH"] = os.pathsep.join(
        (
            str(repository_root / "src"),
            str(repository_root / "integrations" / "snapper-delegate" / "src"),
        )
    )
    audit = (
        "import sys\n"
        "import snapper_delegate.pid1\n"
        "forbidden = ('snapper_delegate.registration', 'snapper.data', "
        "'snapper.messaging', "
        "'snapper.application.process_manager.launcher', "
        "'snapper.application.process_manager.spawner', 'zmq')\n"
        "loaded = sorted(name for name in sys.modules "
        "if any(name == root or name.startswith(root + '.') for root in forbidden))\n"
        "print('\\n'.join(loaded))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", audit],
        capture_output=True,
        text=True,
        env=child_environment,
        check=False,
    )
    assert result.returncode == 0
    assert result.stdout == "\n"
    assert result.stderr == ""
