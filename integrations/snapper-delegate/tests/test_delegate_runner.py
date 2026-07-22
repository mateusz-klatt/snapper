"""Tests for the managed delegate process foundation."""

import asyncio
import signal
import sys
from collections.abc import Callable
from types import ModuleType
from types import SimpleNamespace
from typing import cast

import pytest
from pydantic import ValidationError
from typer.testing import CliRunner

import snapper.application.process_manager.registry as registry_module
import snapper.cli.app as app_module
import snapper_delegate.runner as runner_module
from snapper.application.process_manager.launcher import CoreProcessStartupError
from snapper.application.process_manager.launcher import ProcessLauncherService
from snapper.application.process_manager.launcher import is_delegate_process
from snapper.application.process_manager.models import ProcessConfigModel
from snapper.application.process_manager.models import ProcessRegistryEntry
from snapper.application.process_manager.process_parameters import DelegateProcessParameters
from snapper.application.process_manager.registry import get_registered_processes
from snapper.cli.app import app
from snapper.config.app import AppSettings
from snapper.config.bootstrap import BootstrapSettingsLoader
from snapper.core.json_types import JsonObject
from snapper.core.types import ProcessAutostartProfileEnum
from snapper.core.types import ProcessLifecycleEnum
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRoleEnum
from snapper.core.types import StartProcessStatusEnum
from snapper_delegate.runner import DelegateRunner

_VALID_PARAMETERS: dict[str, object] = {
    "model_alias": "research-primary",
    "base_url": "https://delegate.invalid/v1",
    "api_key_file": "/run/secrets/delegate-key",
    "delegate_token_file": "/run/secrets/delegate-token",
    "max_tool_rounds": 4,
}

_INVALID_PARAMETERS: tuple[dict[str, object], ...] = (
    {**_VALID_PARAMETERS, "model_alias": ""},
    {**_VALID_PARAMETERS, "base_url": ""},
    {**_VALID_PARAMETERS, "api_key_file": ""},
    {**_VALID_PARAMETERS, "delegate_token_file": ""},
    {**_VALID_PARAMETERS, "max_tool_rounds": 0},
    {**_VALID_PARAMETERS, "max_tool_rounds": "4"},
    {**_VALID_PARAMETERS, "unexpected": True},
)


class _CliSettingsService:
    def __init__(self, calls: list[str]) -> None:
        self._calls = calls

    async def shutdown(self) -> None:
        self._calls.append("settings_shutdown")


class _CliLauncher:
    def __init__(
        self,
        calls: list[str],
        start_error: CoreProcessStartupError | None,
    ) -> None:
        self._calls = calls
        self._start_error = start_error

    def set_msg_publisher(self, publisher: object | None) -> None:
        self._calls.append(f"set_publisher:{publisher is not None}")

    async def sync_registry_to_database(self) -> None:
        self._calls.append("sync")

    async def start_all_processes(self) -> None:
        self._calls.append("start_all")
        if self._start_error is not None:
            raise self._start_error

    async def emit_summary_snapshot(self) -> None:
        self._calls.append("summary")

    async def reconcile_desired_state(self) -> None:
        self._calls.append("reconcile")

    async def stop_all_processes(self) -> None:
        self._calls.append("stop_all")


class _CliCommandListener:
    def __init__(self, calls: list[str]) -> None:
        self._calls = calls

    async def start(self, endpoint: str) -> None:
        self._calls.append(f"listener_start:{endpoint}")

    async def stop(self) -> None:
        self._calls.append("listener_stop")


def _delegate_config(tags: tuple[str, ...] = ("delegate", "runner")) -> ProcessConfigModel:
    return ProcessConfigModel(
        name="delegate_runner",
        enabled=True,
        mode=ProcessModeEnum.PROCESS,
        class_path="snapper_delegate.runner.DelegateRunner",
        method="start",
        parameters={},
        lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
        role=ProcessRoleEnum.CORE,
        tags=tags,
    )


def _core_config() -> ProcessConfigModel:
    return ProcessConfigModel(
        name="core_service",
        enabled=True,
        mode=ProcessModeEnum.THREAD,
        class_path="test.CoreService",
        method="start",
        parameters={},
        lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
        role=ProcessRoleEnum.CORE,
        tags=("core",),
    )


def _install_cli_fakes(
    monkeypatch: pytest.MonkeyPatch,
    calls: list[str],
    profile: ProcessAutostartProfileEnum,
    *,
    start_error: CoreProcessStartupError | None = None,
    publisher_error: bool = False,
) -> None:
    service = _CliSettingsService(calls)
    launcher = _CliLauncher(calls, start_error)

    def _get_settings() -> SimpleNamespace:
        return SimpleNamespace(
            db_url="sqlite+aiosqlite:///:memory:",
            zmq_broker_xsub="tcp://broker:7500",
            process_autostart_profile=profile,
        )

    async def _get_service(db_url: str, xsub: str) -> _CliSettingsService:
        calls.append(f"get_service:{db_url}:{xsub}")
        return service

    def _get_app_settings(settings_service: _CliSettingsService) -> SimpleNamespace:
        assert settings_service is service
        return SimpleNamespace(
            zmq_broker_xsub="tcp://broker:7500",
            zmq_broker_xpub="tcp://broker:7501",
            process_autostart_profile=profile,
        )

    def _launcher_factory(settings: object) -> _CliLauncher:
        del settings
        calls.append("launcher")
        return launcher

    def _build_publisher(xsub: str) -> tuple[None, None]:
        calls.append(f"publisher_build:{xsub}")
        if publisher_error:
            raise RuntimeError("publisher unavailable")
        return None, None

    def _listener_factory(created_launcher: object) -> _CliCommandListener:
        assert created_launcher is launcher
        calls.append("listener")
        return _CliCommandListener(calls)

    async def _await_shutdown() -> None:
        calls.append("wait")
        await asyncio.sleep(0)

    def _shutdown_publisher(publisher: object | None, context: object | None) -> None:
        del publisher, context
        calls.append("publisher_shutdown")

    monkeypatch.setattr(app_module, "get_settings", _get_settings)
    monkeypatch.setattr(app_module, "get_settings_service", _get_service)
    monkeypatch.setattr(app_module, "get_settings_with_service", _get_app_settings)
    monkeypatch.setattr(app_module, "discover_processes", lambda: calls.append("discover"))
    monkeypatch.setattr(app_module, "ProcessLauncherService", _launcher_factory)
    monkeypatch.setattr(app_module, "build_audit_publisher", _build_publisher)
    monkeypatch.setattr(app_module, "shutdown_audit_publisher", _shutdown_publisher)
    monkeypatch.setattr(app_module, "ProcessCommandListener", _listener_factory)
    monkeypatch.setattr(app_module, "_await_shutdown_signal", _await_shutdown)


def test_delegate_parameters_accept_valid_values() -> None:
    """Valid delegate parameters retain JSON-native values.

    Given: Every required delegate parameter with its strict type,
    When: The process parameter model validates the payload,
    Then: The serialized values match the spawn-safe input exactly.
    """
    parameters = DelegateProcessParameters.model_validate(_VALID_PARAMETERS)
    assert parameters.model_dump() == _VALID_PARAMETERS


@pytest.mark.parametrize("payload", _INVALID_PARAMETERS)
def test_delegate_parameters_reject_invalid_values(payload: dict[str, object]) -> None:
    """Invalid delegate configuration fails before process spawning.

    Given: A blank, non-positive, mistyped, or unexpected field,
    When: The strict delegate parameter model validates it,
    Then: Pydantic raises a validation error.
    """
    with pytest.raises(ValidationError):
        DelegateProcessParameters.model_validate(payload)


def test_delegate_extra_package_discovery_registers_runner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Extra-package discovery imports the delegate runner fail-soft path.

    Given: The delegate package on Python path and listed as an extra package,
    When: Process discovery walks its submodules from a clean module entry,
    Then: The generic runner appears in the shared process registry.
    """
    previous_entry = registry_module._PROCESS_REGISTRY.pop("delegate_runner", None)
    previous_module: ModuleType | None = sys.modules.pop("snapper_delegate.runner", None)
    bootstrap = SimpleNamespace(strategy_extra_packages="snapper_delegate")
    monkeypatch.setattr(registry_module, "get_bootstrap_settings", lambda: bootstrap)
    try:
        registry_module.discover_processes()
        entry = get_registered_processes()["delegate_runner"]
        assert entry.class_path == "snapper_delegate.runner.DelegateRunner"
    finally:
        registry_module._PROCESS_REGISTRY.pop("delegate_runner", None)
        if previous_entry is not None:
            registry_module._PROCESS_REGISTRY["delegate_runner"] = previous_entry
        if previous_module is not None:
            sys.modules["snapper_delegate.runner"] = previous_module


def test_delegate_runner_registration_is_process_managed() -> None:
    """Runner metadata selects the existing subprocess manager.

    Given: The imported delegate integration package,
    When: Its generic runner registration is inspected,
    Then: It is a disabled long-running PROCESS template with strict parameters.
    """
    entry: ProcessRegistryEntry = get_registered_processes()["delegate_runner"]
    assert entry.class_ref is DelegateRunner
    assert entry.parameters_model is DelegateProcessParameters
    assert entry.mode is ProcessModeEnum.PROCESS
    assert entry.lifecycle is ProcessLifecycleEnum.LONG_RUNNING
    assert entry.role is ProcessRoleEnum.CORE
    assert entry.tags == ("delegate", "runner")
    assert entry.enabled is False


def test_delegate_template_parameters_validate_at_spawn_boundary() -> None:
    """Custom delegate instances retain their template parameter contract.

    Given: A custom process config sourced from the registered delegate template,
    When: The launcher validates valid and invalid spawn parameters,
    Then: Valid primitives pass and an invalid round limit fails before spawning.
    """
    settings = AppSettings(
        BootstrapSettingsLoader(DB_URL="sqlite+aiosqlite:///:memory:"),
        settings_service=None,
    )
    launcher = ProcessLauncherService(settings)
    valid_config = _delegate_config()
    valid_config.name = "delegate_research_primary"
    valid_config.template = "delegate_runner"
    valid_config.parameters = cast(JsonObject, dict(_VALID_PARAMETERS))
    assert launcher._validate_parameters(valid_config) == _VALID_PARAMETERS
    invalid_config = _delegate_config()
    invalid_config.name = "delegate_research_invalid"
    invalid_config.template = "delegate_runner"
    invalid_config.parameters = {**valid_config.parameters, "max_tool_rounds": 0}
    with pytest.raises(ValidationError):
        launcher._validate_parameters(invalid_config)


@pytest.mark.asyncio
async def test_delegate_runner_idles_and_stops_cleanly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The lifecycle stub stays alive across a heartbeat and stops cleanly.

    Given: A delegate runner with a short local heartbeat interval,
    When: It starts, idles through multiple heartbeats, and receives stop,
    Then: The task exits without error and reports a stopped state.
    """
    monkeypatch.setattr(runner_module, "_HEARTBEAT_INTERVAL_SECONDS", 0.001)
    monkeypatch.setattr(DelegateRunner, "_install_stop_signal_handlers", lambda self: ())
    runner = DelegateRunner(
        model_alias="research-primary",
        base_url="https://delegate.invalid/v1",
        api_key_file="/run/secrets/delegate-key",
        delegate_token_file="/run/secrets/delegate-token",
        max_tool_rounds=4,
    )
    assert runner.get_status() == {
        "state": "stopped",
        "running": False,
        "heartbeat_count": 0,
    }
    task = asyncio.create_task(runner.start())
    async with asyncio.timeout(1.0):
        while cast(int, runner.get_status()["heartbeat_count"]) < 2:
            await asyncio.sleep(0)
    assert runner.get_status()["state"] == "idle"
    await runner.stop()
    await task
    assert runner.get_status()["state"] == "stopped"
    assert runner.get_status()["running"] is False


@pytest.mark.asyncio
async def test_delegate_runner_cancellation_restores_stopped_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cancellation propagates while restoring lifecycle status.

    Given: A running lifecycle-only delegate task,
    When: Its owning process task is cancelled,
    Then: Cancellation propagates and the runner reports stopped.
    """
    monkeypatch.setattr(DelegateRunner, "_install_stop_signal_handlers", lambda self: ())
    runner = DelegateRunner(
        model_alias="research-primary",
        base_url="https://delegate.invalid/v1",
        api_key_file="/run/secrets/delegate-key",
        delegate_token_file="/run/secrets/delegate-token",
        max_tool_rounds=4,
    )
    task = asyncio.create_task(runner.start())
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert runner.get_status()["state"] == "stopped"


@pytest.mark.parametrize("supports_signals", [True, False])
@pytest.mark.asyncio
async def test_delegate_runner_routes_process_signals_through_clean_stop(
    monkeypatch: pytest.MonkeyPatch,
    supports_signals: bool,
) -> None:
    """Managed termination reaches the runner's clean lifecycle exit.

    Given: An event loop with supported or unsupported signal handlers,
    When: The runner receives termination or an explicit fallback stop,
    Then: It exits normally and removes every handler it installed.
    """
    callbacks: dict[signal.Signals, Callable[[], None]] = {}
    removed: list[signal.Signals] = []

    class _SignalLoop:
        def add_signal_handler(
            self,
            stop_signal: signal.Signals,
            callback: Callable[[], None],
        ) -> None:
            if not supports_signals:
                raise NotImplementedError
            callbacks[stop_signal] = callback

        def remove_signal_handler(self, stop_signal: signal.Signals) -> bool:
            removed.append(stop_signal)
            return True

    signal_loop = _SignalLoop()
    monkeypatch.setattr(asyncio, "get_running_loop", lambda: signal_loop)
    runner = DelegateRunner(
        model_alias="research-primary",
        base_url="https://delegate.invalid/v1",
        api_key_file="/run/secrets/delegate-key",
        delegate_token_file="/run/secrets/delegate-token",
        max_tool_rounds=4,
    )
    task = asyncio.create_task(runner.start())
    await asyncio.sleep(0)
    if supports_signals:
        callbacks[signal.SIGTERM]()
    else:
        await runner.stop()
    await task
    expected_removed = [signal.SIGTERM] if supports_signals else []
    assert removed == expected_removed
    assert runner.get_status()["state"] == "stopped"


@pytest.mark.asyncio
async def test_delegate_runner_rejects_thread_mode() -> None:
    """Delegate lifecycle signals remain isolated to a subprocess.

    Given: A delegate-tagged runner configured with thread mode,
    When: The process launcher validates the start request,
    Then: It rejects the unsafe override before creating a process run.
    """
    settings = AppSettings(
        BootstrapSettingsLoader(DB_URL="sqlite+aiosqlite:///:memory:"),
        settings_service=None,
    )
    launcher = ProcessLauncherService(settings)
    config = _delegate_config()
    config.mode = ProcessModeEnum.THREAD
    with pytest.raises(ValueError, match="requires process mode"):
        await launcher.start_process(config)


def test_delegate_profile_owns_only_delegate_processes() -> None:
    """The delegate profile isolates ownership by stable runner tags.

    Given: Delegate, API, and single-container launcher profiles,
    When: Each launcher evaluates delegate and ordinary core configs,
    Then: Delegate owns only runners, API excludes them, and ALL remains permissive.
    """
    delegate_settings = AppSettings(
        BootstrapSettingsLoader(
            DB_URL="sqlite+aiosqlite:///:memory:",
            PROCESS_AUTOSTART_PROFILE=ProcessAutostartProfileEnum.DELEGATE,
        ),
        settings_service=None,
    )
    api_settings = AppSettings(
        BootstrapSettingsLoader(
            DB_URL="sqlite+aiosqlite:///:memory:",
            PROCESS_AUTOSTART_PROFILE=ProcessAutostartProfileEnum.API,
        ),
        settings_service=None,
    )
    all_settings = AppSettings(
        BootstrapSettingsLoader(
            DB_URL="sqlite+aiosqlite:///:memory:",
            PROCESS_AUTOSTART_PROFILE=ProcessAutostartProfileEnum.ALL,
        ),
        settings_service=None,
    )
    delegate_launcher = ProcessLauncherService(delegate_settings)
    api_launcher = ProcessLauncherService(api_settings)
    all_launcher = ProcessLauncherService(all_settings)
    assert delegate_settings.process_autostart_profile is ProcessAutostartProfileEnum.DELEGATE
    assert delegate_settings.coordinator_label == "Delegate"
    assert is_delegate_process(("delegate", "runner")) is True
    assert is_delegate_process(("delegate",)) is False
    assert delegate_launcher.autostart_includes(_delegate_config()) is True
    assert delegate_launcher.autostart_includes(_core_config()) is False
    assert api_launcher.autostart_includes(_delegate_config()) is False
    assert api_launcher.autostart_includes(_core_config()) is True
    assert all_launcher.autostart_includes(_delegate_config()) is True


@pytest.mark.parametrize(
    ("config_tags", "raw_tags"),
    [
        (("delegate", "runner"), ()),
        ((), ("delegate", "runner")),
    ],
)
def test_api_profile_rejects_direct_delegate_start(
    config_tags: tuple[str, ...],
    raw_tags: tuple[str, ...],
) -> None:
    """API ownership cannot bypass the delegate coordinator through direct start.

    Given: Delegate identity in either resolved or persisted raw tags,
    When: An API-profile launcher evaluates a direct local start,
    Then: It returns an ownership error instead of duplicating the runner.
    """
    settings = AppSettings(
        BootstrapSettingsLoader(
            DB_URL="sqlite+aiosqlite:///:memory:",
            PROCESS_AUTOSTART_PROFILE=ProcessAutostartProfileEnum.API,
        ),
        settings_service=None,
    )
    launcher = ProcessLauncherService(settings)
    config_dict: JsonObject = {"tags": list(raw_tags)}
    result = launcher._reject_external_process_start(
        "delegate_runner", _delegate_config(config_tags), config_dict
    )
    assert result is not None
    assert result.status == StartProcessStatusEnum.ERROR
    assert "delegate engine" in result.message


def test_delegate_engine_wires_management_and_reconciliation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The CLI engine wires discovery, summaries, reconcile, and clean shutdown.

    Given: A delegate-profile coordinator with fail-soft in-memory test doubles,
    When: The delegate-engine command runs through one scheduling turn,
    Then: It starts managed processes and tears down every management service.
    """
    calls: list[str] = []
    _install_cli_fakes(monkeypatch, calls, ProcessAutostartProfileEnum.DELEGATE)
    result = CliRunner().invoke(app, ["delegate-engine"])
    assert result.exit_code == 0
    assert "discover" in calls
    assert "sync" in calls
    assert "start_all" in calls
    assert "summary" in calls
    assert "reconcile" in calls
    assert "listener_start:tcp://broker:7501" in calls
    assert calls[-4:] == [
        "listener_stop",
        "stop_all",
        "settings_shutdown",
        "publisher_shutdown",
    ]


def test_delegate_engine_degrades_without_summary_publisher(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Publisher failure remains fail-soft for a delegate coordinator.

    Given: An unavailable summary publisher and a delegate profile,
    When: The delegate-engine command runs,
    Then: Workloads and reconciliation still run and cleanup completes.
    """
    calls: list[str] = []
    _install_cli_fakes(
        monkeypatch,
        calls,
        ProcessAutostartProfileEnum.DELEGATE,
        publisher_error=True,
    )
    result = CliRunner().invoke(app, ["delegate-engine"])
    assert result.exit_code == 0
    assert "summary publisher unavailable" in result.output
    assert "start_all" in calls
    assert "listener" in calls
    assert "reconcile" in calls
    assert calls.count("publisher_shutdown") == 2


def test_delegate_engine_rejects_mismatched_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The delegate command fails closed before process discovery.

    Given: A delegate-engine command configured with the API profile,
    When: The command validates coordinator ownership,
    Then: It exits nonzero without discovering or starting any workload.
    """
    calls: list[str] = []
    _install_cli_fakes(monkeypatch, calls, ProcessAutostartProfileEnum.API)
    result = CliRunner().invoke(app, ["delegate-engine"])
    assert result.exit_code == 1
    assert "requires PROCESS_AUTOSTART_PROFILE=delegate" in result.output
    assert "settings_shutdown" not in calls
    assert not any(call.startswith("get_service:") for call in calls)
    assert "discover" not in calls
    assert "launcher" not in calls
    assert "start_all" not in calls


def test_delegate_engine_exits_nonzero_on_core_startup_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A genuine core runner startup failure exits the engine command.

    Given: A delegate launcher whose core process fails during startup,
    When: The delegate-engine command is invoked,
    Then: It exits nonzero after stopping processes and shutting down services.
    """
    calls: list[str] = []
    _install_cli_fakes(
        monkeypatch,
        calls,
        ProcessAutostartProfileEnum.DELEGATE,
        start_error=CoreProcessStartupError(["delegate_runner"]),
    )
    result = CliRunner().invoke(app, ["delegate-engine"])
    assert result.exit_code == 1
    assert "Delegate engine startup failed" in result.output
    assert "stop_all" in calls
    assert "settings_shutdown" in calls
    assert "publisher_shutdown" in calls
    assert "listener" not in calls
