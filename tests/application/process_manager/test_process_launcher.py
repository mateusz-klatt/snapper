"""Tests for process launcher functionality."""

import asyncio
import contextlib
import json
import subprocess
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from types import SimpleNamespace
from typing import Any
from typing import cast
from unittest import mock
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch
from uuid import UUID

import psutil
import pytest
from pydantic import BaseModel

from snapper.application.process_manager import launcher as launcher_module
from snapper.application.process_manager.launcher import CoreProcessStartupError
from snapper.application.process_manager.launcher import ProcessLauncherService
from snapper.application.process_manager.launcher import _DesiredState
from snapper.application.process_manager.launcher import is_market_data_publisher
from snapper.application.process_manager.models import ProcessConfigModel
from snapper.application.process_manager.models import ProcessInstanceInfo
from snapper.application.process_manager.models import ProcessRegistryEntry
from snapper.application.process_manager.models import ProcessStartResult
from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.process_manager.models import SpawnerStatusSnapshot
from snapper.application.process_manager.registry import discover_processes
from snapper.application.process_manager.registry import get_registered_processes
from snapper.application.process_manager.spawner import ProcessSpawnerService
from snapper.application.process_manager.strategy_scope import StrategyProcessClassification
from snapper.application.process_manager.strategy_scope import StrategyScopeError
from snapper.application.process_manager.strategy_scope import classify_strategy_process
from snapper.application.process_manager.strategy_scope import resolve_classified_strategy_scope
from snapper.application.process_manager.strategy_scope import resolve_strategy_process_scope
from snapper.config.app import AppSettings
from snapper.config.bootstrap import BootstrapSettingsLoader
from snapper.config.settings import get_settings
from snapper.core.json_types import JsonObject
from snapper.core.types import HealthStatusEnum
from snapper.core.types import ProcessLifecycleEnum
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRestartPolicyEnum
from snapper.core.types import ProcessRoleEnum
from snapper.core.types import ProcessRunStatusEnum
from snapper.data.models import ProcessRun
from snapper.data.models import Setting
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import WalletRow


class TestProcessConfig:
    """Unit tests for ProcessConfigModel creation and attributes."""

    def test_process_config_creation(self) -> None:
        """Verify ProcessConfigModel stores all provided attributes.

        Given: A full set of process configuration parameters,
        When: ProcessConfigModel is instantiated with these parameters,
        Then: All attributes are correctly stored and accessible.
        """
        config = ProcessConfigModel(
            name="zmq_broker",
            enabled=True,
            mode="thread",
            class_path="snapper.ipc.zmq_broker.ZmqBrokerThread",
            method="run",
            parameters={"endpoint": "tcp://0.0.0.0:5555"},
            note="Test broker",
        )
        assert config.name == "zmq_broker"
        assert config.enabled is True
        assert config.mode == "thread"
        assert config.class_path == "snapper.ipc.zmq_broker.ZmqBrokerThread"
        assert config.method == "run"
        assert config.parameters == {"endpoint": "tcp://0.0.0.0:5555"}
        assert config.note == "Test broker"

    def test_process_config_without_note(self) -> None:
        """Verify ProcessConfigModel defaults note to None when not provided.

        Given: Process configuration parameters without a note field,
        When: ProcessConfigModel is instantiated,
        Then: The note attribute defaults to None.
        """
        config = ProcessConfigModel(
            name="test",
            enabled=False,
            mode="process",
            class_path="test.Class",
            method="start",
            parameters={"key": "value"},
        )
        assert config.note is None


class TestProcessFactoryInit:
    """Unit tests for ProcessLauncherService initialization."""

    def test_init(self) -> None:
        """Verify ProcessLauncherService initializes with empty tracking dicts.

        Given: A mock settings object,
        When: ProcessLauncherService is instantiated,
        Then: Settings are stored and tracking dictionaries are empty.
        """
        settings = MagicMock()
        factory = ProcessLauncherService(settings)
        assert factory.settings is settings
        assert factory.started_processes == {}
        assert factory.process_tasks == {}
        assert factory.process_lifecycles == {}


class TestImportClass:
    """Unit tests for ProcessLauncherService.import_class method."""

    @patch("snapper.application.process_manager.config_resolver.get_registered_processes")
    def test_import_class_from_registry(self, mock_get_registry: MagicMock) -> None:
        """Verify import_class retrieves class from process registry.

        Given: A process class registered in the process registry,
        When: import_class is called with matching class_path and process_name,
        Then: The class reference from the registry is returned.
        """
        mock_class = MagicMock(spec=type)
        mock_get_registry.return_value = {
            "zmq_broker": ProcessRegistryEntry(
                class_ref=mock_class,
                class_path="snapper.ipc.zmq_broker.ZmqBrokerThread",
                method="run",
                description="",
                priority=0,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.CORE,
                tags=(),
                parameters_model=None,
                parameters_schema=None,
                enabled=True,
                mode="thread",
            )
        }
        settings = MagicMock()
        factory = ProcessLauncherService(settings)
        result = factory.import_class(
            "snapper.ipc.zmq_broker.ZmqBrokerThread", process_name="zmq_broker"
        )
        assert result is mock_class
        mock_get_registry.assert_called_once()

    @patch("snapper.application.process_manager.config_resolver.get_registered_processes")
    def test_import_class_registry_not_a_class(self, mock_get_registry: MagicMock) -> None:
        """Verify import_class raises TypeError for non-class registry entry.

        Given: A registry entry with class_ref that is not a class type,
        When: import_class is called for that entry,
        Then: TypeError is raised with 'is not a class' message.
        """
        mock_get_registry.return_value = {
            "bad_entry": ProcessRegistryEntry(
                class_ref="not_a_class",
                class_path="test.BadClass",
                method="run",
                description="",
                priority=0,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.CORE,
                tags=(),
                parameters_model=None,
                parameters_schema=None,
                enabled=True,
                mode="thread",
            )
        }
        settings = MagicMock()
        factory = ProcessLauncherService(settings)
        with pytest.raises(TypeError, match="is not a class"):
            factory.import_class("test.BadClass", process_name="bad_entry")

    @patch("snapper.application.process_manager.config_resolver.importlib.import_module")
    def test_import_class_via_importlib(self, mock_import: MagicMock) -> None:
        """Verify import_class falls back to importlib when not in registry.

        Given: A class_path not found in registry but valid module exists,
        When: import_class is called without process_name,
        Then: Class is imported via importlib.import_module.
        """
        mock_class = MagicMock(spec=type)
        mock_module = MagicMock()
        mock_module.ZmqBrokerThread = mock_class
        mock_import.return_value = mock_module
        settings = MagicMock()
        factory = ProcessLauncherService(settings)
        result = factory.import_class("snapper.ipc.zmq_broker.ZmqBrokerThread")
        assert result is mock_class
        mock_import.assert_called_once_with("snapper.ipc.zmq_broker")

    @patch("snapper.application.process_manager.config_resolver.importlib.import_module")
    def test_import_class_module_not_found(self, mock_import: MagicMock) -> None:
        """Verify import_class raises ImportError for nonexistent module.

        Given: A class_path referencing a nonexistent module,
        When: import_class is called,
        Then: ImportError is raised with 'Failed to import class' message.
        """
        mock_import.side_effect = ModuleNotFoundError("No module named 'nonexistent'")
        settings = MagicMock()
        factory = ProcessLauncherService(settings)
        with pytest.raises(ImportError, match="Failed to import class"):
            factory.import_class("nonexistent.module.Class")

    @patch("snapper.application.process_manager.config_resolver.importlib.import_module")
    def test_import_class_attribute_error(self, mock_import: MagicMock) -> None:
        """Verify import_class raises ImportError for missing class attribute.

        Given: A valid module that lacks the specified class attribute,
        When: import_class is called for that class,
        Then: ImportError is raised with 'Failed to import class' message.
        """
        mock_module = MagicMock()
        del mock_module.NonexistentClass
        mock_import.return_value = mock_module
        settings = MagicMock()
        factory = ProcessLauncherService(settings)
        with pytest.raises(ImportError, match="Failed to import class"):
            factory.import_class("snapper.ipc.zmq_broker.NonexistentClass")

    @patch("snapper.application.process_manager.config_resolver.importlib.import_module")
    def test_import_class_not_a_type(self, mock_import: MagicMock) -> None:
        """Verify import_class raises TypeError when attribute is not a class.

        Given: A module attribute that is a string instead of a class,
        When: import_class is called for that attribute,
        Then: TypeError is raised with 'is not a class' message.
        """
        mock_module = MagicMock()
        mock_module.NotAClass = "some_string"
        mock_import.return_value = mock_module
        settings = MagicMock()
        factory = ProcessLauncherService(settings)
        with pytest.raises(TypeError, match="is not a class"):
            factory.import_class("snapper.ipc.zmq_broker.NotAClass")


class TestStartProcess:
    """Unit tests for ProcessLauncherService.start_process method."""

    @pytest.mark.asyncio
    @patch("snapper.application.process_manager.launcher.ProcessLauncherService.import_class")
    async def test_start_process_disabled_still_runs(self, mock_import: MagicMock) -> None:
        """Verify disabled process config still gets started by start_process.

        Given: A process config with enabled=False,
        When: start_process is called with that config,
        Then: The process is still started and tracked in started_processes.
        """

        async def long_running_task() -> None:
            await asyncio.sleep(10)

        mock_class = MagicMock()
        mock_instance = MagicMock()
        mock_instance.run = long_running_task
        mock_class.return_value = mock_instance
        mock_import.return_value = mock_class
        settings = MagicMock()
        factory = ProcessLauncherService(settings)
        config = ProcessConfigModel(
            name="disabled_process",
            enabled=False,
            mode="thread",
            class_path="test.Class",
            method="run",
            parameters={},
        )
        await factory.start_process(config)
        mock_import.assert_called_once_with("test.Class", "disabled_process")
        assert "disabled_process" in factory.started_processes
        task = factory.process_tasks["disabled_process"]
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    @pytest.mark.asyncio
    @patch("snapper.application.process_manager.launcher.ProcessLauncherService.import_class")
    async def test_start_process_empty_parameters_allowed(self, mock_import: MagicMock) -> None:
        """Verify start_process handles empty parameters correctly.

        Given: A process config with empty parameters dictionary,
        When: start_process is called,
        Then: Class is instantiated without keyword arguments and process starts.
        """

        async def long_running_task() -> None:
            await asyncio.sleep(10)

        mock_class = MagicMock()
        mock_instance = MagicMock()
        mock_instance.run = long_running_task
        mock_class.return_value = mock_instance
        mock_import.return_value = mock_class
        settings = MagicMock()
        factory = ProcessLauncherService(settings)
        config = ProcessConfigModel(
            name="no_kwargs",
            enabled=True,
            mode="thread",
            class_path="test.Class",
            method="run",
            parameters={},
        )
        await factory.start_process(config)
        mock_class.assert_called_once_with()
        assert "no_kwargs" in factory.started_processes
        task = factory.process_tasks["no_kwargs"]
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    @pytest.mark.asyncio
    @patch("snapper.application.process_manager.launcher.ProcessLauncherService.import_class")
    async def test_start_process_async_method_thread_mode(self, mock_import: MagicMock) -> None:
        """Verify start_process handles async method in thread mode.

        Given: A process with async run method and mode='thread',
        When: start_process is called with parameters,
        Then: Process is started as asyncio task and tracked correctly.
        """

        async def long_running_task() -> None:
            await asyncio.sleep(10)

        mock_class = MagicMock()
        mock_instance = MagicMock()
        mock_instance.run = long_running_task
        mock_class.return_value = mock_instance
        mock_import.return_value = mock_class
        settings = MagicMock()
        factory = ProcessLauncherService(settings)
        config = ProcessConfigModel(
            name="async_process",
            enabled=True,
            mode="thread",
            class_path="test.AsyncClass",
            method="run",
            parameters={"key": "value"},
        )
        await factory.start_process(config)
        mock_import.assert_called_once_with("test.AsyncClass", "async_process")
        mock_class.assert_called_once_with(key="value")
        assert "async_process" in factory.started_processes
        assert "async_process" in factory.process_tasks
        task = factory.process_tasks["async_process"]
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    @pytest.mark.asyncio
    @patch("snapper.application.process_manager.launcher.ProcessLauncherService.import_class")
    async def test_start_async_task_cancelled_in_grace_window_raises_cancelled(
        self, mock_import: MagicMock
    ) -> None:
        """Cancellation during the 100ms startup grace surfaces cleanly.

        Given: A process whose async ``run`` task gets cancelled
            within the 100ms grace window after ``start_process``
            launches it (e.g. by a concurrent ``stop_all_processes``),
        When: ``start_process`` examines ``task.done()``,
        Then: It raises :class:`asyncio.CancelledError` with a clear
            message instead of letting the bare ``task.exception()``
            call leak the cancellation as an uncaught
            :class:`BaseException`.
        """

        async def cancellable_task() -> None:
            await asyncio.sleep(60)

        cancel_target: dict[str, asyncio.Task[object]] = {}

        async def patched_sleep(_seconds: float) -> None:
            task = cancel_target.get("task")
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

        mock_class = MagicMock()
        mock_instance = MagicMock()
        mock_instance.run = cancellable_task
        mock_class.return_value = mock_instance
        mock_import.return_value = mock_class
        settings = MagicMock()
        factory = ProcessLauncherService(settings)
        config = ProcessConfigModel(
            name="grace_cancel_proc",
            enabled=True,
            mode="thread",
            class_path="test.GraceCancel",
            method="run",
            parameters={},
        )
        original_create_task = asyncio.create_task

        def capturing_create_task(coro: object) -> asyncio.Task[object]:
            task = original_create_task(cast(Any, coro))
            cancel_target["task"] = task
            return task

        with (
            mock.patch("asyncio.create_task", side_effect=capturing_create_task),
            mock.patch("asyncio.sleep", side_effect=patched_sleep),
            pytest.raises(asyncio.CancelledError),
        ):
            await factory.start_process(config)

    @pytest.mark.asyncio
    async def test_start_process_async_method_process_mode(self) -> None:
        """Verify start_process spawns subprocess in process mode.

        Given: A process config with mode='process' and valid class_path,
        When: start_process is called,
        Then: Process is spawned via ProcessSpawnerService and tracked.
        """
        settings = MagicMock()
        factory = ProcessLauncherService(settings)
        config = ProcessConfigModel(
            name="process_mode",
            enabled=True,
            mode="process",
            class_path="tests.application.process_manager.test_process_spawner.DummyProcess",
            method="start",
            parameters={"duration": 0.5},
        )
        await factory.start_process(config)
        assert "process_mode" in factory.started_processes
        if isinstance(factory.started_processes["process_mode"], ProcessInstanceInfo):
            factory.spawner.terminate("process_mode")

    @pytest.mark.asyncio
    @patch("snapper.application.process_manager.launcher.ProcessLauncherService.import_class")
    async def test_start_process_sync_method(self, mock_import: MagicMock) -> None:
        """Verify start_process runs sync method via executor.

        Given: A process with synchronous run method,
        When: start_process is called in thread mode,
        Then: Method is executed via event loop's run_in_executor.
        """
        mock_class = MagicMock()
        mock_instance = MagicMock()
        mock_method = MagicMock()
        mock_instance.run = mock_method
        mock_class.return_value = mock_instance
        mock_import.return_value = mock_class
        settings = MagicMock()
        factory = ProcessLauncherService(settings)
        config = ProcessConfigModel(
            name="sync_process",
            enabled=True,
            mode="thread",
            class_path="test.SyncClass",
            method="run",
            parameters={"config": "value"},
        )
        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock()
            await factory.start_process(config)
        assert "sync_process" in factory.started_processes
        mock_loop.return_value.run_in_executor.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("snapper.application.process_manager.launcher.ProcessLauncherService.import_class")
    async def test_start_process_with_note(self, mock_import: MagicMock) -> None:
        """Verify start_process accepts config with optional note field.

        Given: A process config that includes a note field,
        When: start_process is called,
        Then: Process starts successfully regardless of note presence.
        """

        async def long_running_task() -> None:
            await asyncio.sleep(10)

        mock_class = MagicMock()
        mock_instance = MagicMock()
        mock_instance.run = long_running_task
        mock_class.return_value = mock_instance
        mock_import.return_value = mock_class
        settings = MagicMock()
        factory = ProcessLauncherService(settings)
        config = ProcessConfigModel(
            name="noted_process",
            enabled=True,
            mode="thread",
            class_path="test.NotedClass",
            method="run",
            parameters={"key": "value"},
            note="This is a test note",
        )
        await factory.start_process(config)
        assert "noted_process" in factory.started_processes
        task = factory.process_tasks["noted_process"]
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    @pytest.mark.asyncio
    @patch("snapper.application.process_manager.launcher.ProcessLauncherService.import_class")
    async def test_start_process_import_error_raises(self, mock_import: MagicMock) -> None:
        """Verify start_process propagates ImportError and skips tracking.

        Given: import_class raises ImportError,
        When: start_process is called,
        Then: ImportError is raised and process is not added to started_processes.
        """
        mock_import.side_effect = ImportError("Failed to import")
        settings = MagicMock()
        factory = ProcessLauncherService(settings)
        config = ProcessConfigModel(
            name="error_process",
            enabled=True,
            mode="thread",
            class_path="test.ErrorClass",
            method="run",
            parameters={"key": "value"},
        )
        with pytest.raises(ImportError, match="Failed to import"):
            await factory.start_process(config)
        assert "error_process" not in factory.started_processes


class TestStartAllProcesses:
    """Unit tests for ProcessLauncherService.start_all_processes method."""

    @pytest.mark.asyncio
    @patch(
        "snapper.application.process_manager.launcher.ProcessLauncherService.get_process_configs"
    )
    @patch("snapper.application.process_manager.launcher.ProcessLauncherService.start_process")
    async def test_start_all_processes(
        self, mock_start: AsyncMock, mock_get_configs: AsyncMock
    ) -> None:
        """Verify start_all_processes starts only enabled processes.

        Given: Three process configs where one is disabled,
        When: start_all_processes is called,
        Then: Only enabled processes are started via start_process.
        """
        config1 = ProcessConfigModel(
            name="process1",
            enabled=True,
            mode="thread",
            class_path="test.Class1",
            method="run",
            parameters={"key": "value"},
        )
        config2 = ProcessConfigModel(
            name="process2",
            enabled=False,
            mode="thread",
            class_path="test.Class2",
            method="run",
            parameters={"key": "value"},
        )
        config3 = ProcessConfigModel(
            name="process3",
            enabled=True,
            mode="thread",
            class_path="test.Class3",
            method="run",
            parameters={"key": "value"},
        )
        mock_get_configs.return_value = [config1, config2, config3]
        settings = MagicMock()
        factory = ProcessLauncherService(settings)
        await factory.start_all_processes()
        assert mock_start.call_count == 2
        mock_start.assert_any_call(config1)
        mock_start.assert_any_call(config3)

    @pytest.mark.asyncio
    @patch(
        "snapper.application.process_manager.launcher.ProcessLauncherService.get_process_configs"
    )
    async def test_start_all_processes_empty(self, mock_get_configs: AsyncMock) -> None:
        """Verify start_all_processes handles empty config list gracefully.

        Given: No process configurations available,
        When: start_all_processes is called,
        Then: No processes are started and started_processes remains empty.
        """
        mock_get_configs.return_value = []
        settings = MagicMock()
        factory = ProcessLauncherService(settings)
        await factory.start_all_processes()
        assert factory.started_processes == {}

    @pytest.mark.asyncio
    @patch("snapper.application.process_manager.launcher.ProcessLauncherService.import_class")
    @patch(
        "snapper.application.process_manager.launcher.ProcessLauncherService.get_process_configs"
    )
    async def test_start_all_processes_continues_on_failure(
        self, mock_get_configs: AsyncMock, mock_import: MagicMock
    ) -> None:
        """Verify start_all_processes continues starting other processes on failure.

        Given: Multiple enabled process configs where one fails to import,
        When: start_all_processes is called,
        Then: Remaining processes are still started despite the failure.
        """

        async def long_running_task() -> None:
            await asyncio.sleep(10)

        config1 = ProcessConfigModel(
            name="good_process",
            enabled=True,
            mode="thread",
            class_path="test.GoodClass",
            method="run",
            parameters={},
        )
        config2 = ProcessConfigModel(
            name="bad_process",
            enabled=True,
            mode="thread",
            class_path="test.BadClass",
            method="run",
            parameters={},
            role=ProcessRoleEnum.TASK,
        )
        config3 = ProcessConfigModel(
            name="another_good_process",
            enabled=True,
            mode="thread",
            class_path="test.AnotherGoodClass",
            method="run",
            parameters={},
        )
        mock_get_configs.return_value = [config1, config2, config3]
        mock_good_class = MagicMock()
        mock_good_instance = MagicMock()
        mock_good_instance.run = long_running_task
        mock_good_class.return_value = mock_good_instance
        mock_another_good_class = MagicMock()
        mock_another_good_instance = MagicMock()
        mock_another_good_instance.run = long_running_task
        mock_another_good_class.return_value = mock_another_good_instance

        def import_side_effect(class_path: str, process_name: str | None = None) -> MagicMock:
            if "BadClass" in class_path:
                raise ImportError("Failed to import bad class")
            elif "AnotherGoodClass" in class_path:
                return mock_another_good_class
            else:
                return mock_good_class

        mock_import.side_effect = import_side_effect
        settings = MagicMock()
        factory = ProcessLauncherService(settings)
        await factory.start_all_processes()
        assert "good_process" in factory.started_processes
        assert "another_good_process" in factory.started_processes
        assert "bad_process" not in factory.started_processes
        for process_name in ("good_process", "another_good_process"):
            task = factory.process_tasks[process_name]
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    @pytest.mark.asyncio
    async def test_start_all_processes_resolves_strategy_wallet_for_operator(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Autostart resolves an empty strategy wallet in operator scope."""
        config = _strategy_autostart_config()
        repository = _WalletLookupRepository(operator_wallets=[_wallet_row("wallet-live")])
        factory = ProcessLauncherService(_create_settings())
        start_mock = mock.AsyncMock()
        monkeypatch.setattr(factory, "get_process_configs", mock.AsyncMock(return_value=[config]))
        monkeypatch.setattr(factory, "start_process", start_mock)
        monkeypatch.setattr(factory, "_start_native_process_monitoring", mock.MagicMock())
        monkeypatch.setattr(launcher_module, "get_repository", lambda _url: repository)

        await factory.start_all_processes()

        start_mock.assert_awaited_once()
        await_args = start_mock.await_args
        assert await_args is not None
        started_config = await_args.args[0]
        assert isinstance(started_config, ProcessConfigModel)
        assert started_config.parameters["wallet_public_id"] == "wallet-live"
        assert config.parameters["wallet_public_id"] == ""
        assert repository.operator_public_ids == ["op-1"]
        assert repository.operator_lookup_count == 1
        assert repository.active_lookup_count == 0

    @pytest.mark.asyncio
    async def test_start_all_processes_resolves_strategy_wallet_from_admin_catalog(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Autostart resolves a paper strategy wallet from admin scope."""
        config = _strategy_autostart_config(operator_public_id="", exchange="paper")
        repository = _WalletLookupRepository(
            active_wallets=[
                _wallet_row("wallet-paper", is_paper=True),
                _wallet_row("wallet-live"),
            ]
        )
        factory = ProcessLauncherService(_create_settings())
        start_mock = mock.AsyncMock()
        monkeypatch.setattr(factory, "get_process_configs", mock.AsyncMock(return_value=[config]))
        monkeypatch.setattr(factory, "start_process", start_mock)
        monkeypatch.setattr(factory, "_start_native_process_monitoring", mock.MagicMock())
        monkeypatch.setattr(launcher_module, "get_repository", lambda _url: repository)

        await factory.start_all_processes()

        start_mock.assert_awaited_once()
        await_args = start_mock.await_args
        assert await_args is not None
        started_config = await_args.args[0]
        assert isinstance(started_config, ProcessConfigModel)
        assert started_config.parameters["wallet_public_id"] == "wallet-paper"
        assert repository.active_lookup_count == 1
        assert repository.operator_lookup_count == 0

    @pytest.mark.asyncio
    async def test_start_all_processes_skips_live_empty_wallet_without_operator(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Autostart skips a live strategy with no accountable operator."""
        config = _strategy_autostart_config(operator_public_id="")
        repository = _WalletLookupRepository(active_wallets=[_wallet_row("wallet-live")])
        factory = ProcessLauncherService(_create_settings())
        start_mock = mock.AsyncMock()
        warning_mock = mock.MagicMock()
        monkeypatch.setattr(factory, "get_process_configs", mock.AsyncMock(return_value=[config]))
        monkeypatch.setattr(factory, "start_process", start_mock)
        monkeypatch.setattr(factory, "_start_native_process_monitoring", mock.MagicMock())
        monkeypatch.setattr(launcher_module, "get_repository", lambda _url: repository)
        monkeypatch.setattr(launcher_module.logger, "warning", warning_mock)

        await factory.start_all_processes()

        start_mock.assert_not_awaited()
        assert repository.active_lookup_count == 0
        assert repository.operator_lookup_count == 0
        warning_mock.assert_called_once_with(
            "strategy {} not started: wallet unresolved/ambiguous; set wallet_public_id",
            "strategy",
        )

    @pytest.mark.asyncio
    async def test_start_all_processes_skips_ambiguous_strategy_wallet(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Autostart skips one strategy and warns when wallet lookup is ambiguous."""
        config = _strategy_autostart_config()
        repository = _WalletLookupRepository(
            operator_wallets=[_wallet_row("wallet-a"), _wallet_row("wallet-b")]
        )
        factory = ProcessLauncherService(_create_settings())
        start_mock = mock.AsyncMock()
        warning_mock = mock.MagicMock()
        monkeypatch.setattr(factory, "get_process_configs", mock.AsyncMock(return_value=[config]))
        monkeypatch.setattr(factory, "start_process", start_mock)
        monkeypatch.setattr(factory, "_start_native_process_monitoring", mock.MagicMock())
        monkeypatch.setattr(launcher_module, "get_repository", lambda _url: repository)
        monkeypatch.setattr(launcher_module.logger, "warning", warning_mock)

        await factory.start_all_processes()

        start_mock.assert_not_awaited()
        warning_mock.assert_called_once_with(
            "strategy {} not started: wallet unresolved/ambiguous; set wallet_public_id",
            "strategy",
        )

    @pytest.mark.asyncio
    async def test_start_all_processes_skips_live_explicit_wallet_without_operator(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Autostart skips an explicit live wallet without an operator."""
        config = _strategy_autostart_config(operator_public_id="", wallet_public_id="wallet-pinned")
        repository = _WalletLookupRepository()
        factory = ProcessLauncherService(_create_settings())
        start_mock = mock.AsyncMock()
        warning_mock = mock.MagicMock()
        monkeypatch.setattr(factory, "get_process_configs", mock.AsyncMock(return_value=[config]))
        monkeypatch.setattr(factory, "start_process", start_mock)
        monkeypatch.setattr(factory, "_start_native_process_monitoring", mock.MagicMock())
        monkeypatch.setattr(launcher_module, "get_repository", lambda _url: repository)
        monkeypatch.setattr(launcher_module.logger, "warning", warning_mock)

        await factory.start_all_processes()

        start_mock.assert_not_awaited()
        assert repository.active_lookup_count == 0
        assert repository.operator_lookup_count == 0
        warning_mock.assert_called_once_with(
            "strategy {} not started: wallet unresolved/ambiguous; set wallet_public_id",
            "strategy",
        )

    @pytest.mark.asyncio
    async def test_start_all_processes_skips_explicit_strategy_wallet_without_grant(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Autostart skips an explicit strategy wallet without an active grant."""
        config = _strategy_autostart_config(wallet_public_id="wallet-pinned")
        repository = MagicMock(spec=SQLAlchemyRepository)
        repository.list_active_scope_grants_for_wallet = AsyncMock(return_value=[])
        factory = ProcessLauncherService(_create_settings())
        start_mock = mock.AsyncMock()
        warning_mock = mock.MagicMock()
        monkeypatch.setattr(factory, "get_process_configs", mock.AsyncMock(return_value=[config]))
        monkeypatch.setattr(factory, "start_process", start_mock)
        monkeypatch.setattr(factory, "_start_native_process_monitoring", mock.MagicMock())
        monkeypatch.setattr(launcher_module, "get_repository", lambda _url: repository)
        monkeypatch.setattr(launcher_module.logger, "warning", warning_mock)

        await factory.start_all_processes()

        start_mock.assert_not_awaited()
        repository.list_active_scope_grants_for_wallet.assert_awaited_once()
        warning_mock.assert_called_once_with(
            "strategy {} not started: wallet unresolved/ambiguous; set wallet_public_id",
            "strategy",
        )

    @pytest.mark.asyncio
    async def test_start_all_processes_skips_autoresolved_wallet_without_output_coverage(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Autostart skips a resolved wallet whose grant does not cover outputs."""
        config = _strategy_autostart_config()
        repository = MagicMock(spec=SQLAlchemyRepository)
        repository.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[_wallet_row("wallet-live")]
        )
        repository.list_active_scope_grants_for_wallet = AsyncMock(
            return_value=[{"operator_public_id": "op-1"}]
        )
        repository.list_grant_covered_instrument_public_ids = AsyncMock(
            return_value={"instrument-other"}
        )
        repository.get_instrument_public_ids_by_symbols = AsyncMock(
            return_value={"orders.BTC-USD": "instrument-btc"}
        )
        factory = ProcessLauncherService(_create_settings())
        start_mock = mock.AsyncMock()
        warning_mock = mock.MagicMock()
        monkeypatch.setattr(factory, "get_process_configs", mock.AsyncMock(return_value=[config]))
        monkeypatch.setattr(factory, "start_process", start_mock)
        monkeypatch.setattr(factory, "_start_native_process_monitoring", mock.MagicMock())
        monkeypatch.setattr(launcher_module, "get_repository", lambda _url: repository)
        monkeypatch.setattr(launcher_module.logger, "warning", warning_mock)

        await factory.start_all_processes()

        start_mock.assert_not_awaited()
        repository.list_active_scope_grants_for_wallet.assert_awaited_once()
        repository.list_grant_covered_instrument_public_ids.assert_awaited_once()
        warning_mock.assert_called_once_with(
            "strategy {} not started: wallet unresolved/ambiguous; set wallet_public_id",
            "strategy",
        )

    @pytest.mark.asyncio
    async def test_start_all_processes_starts_valid_single_wallet_strategy(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Autostart starts a strategy after complete wallet and grant enforcement."""
        config = _strategy_autostart_config()
        repository = MagicMock(spec=SQLAlchemyRepository)
        repository.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[_wallet_row("wallet-live")]
        )
        repository.list_active_scope_grants_for_wallet = AsyncMock(
            return_value=[{"operator_public_id": "op-1"}]
        )
        repository.list_grant_covered_instrument_public_ids = AsyncMock(
            return_value={"instrument-btc"}
        )
        repository.get_instrument_public_ids_by_symbols = AsyncMock(
            return_value={"orders.BTC-USD": "instrument-btc"}
        )
        factory = ProcessLauncherService(_create_settings())
        start_mock = mock.AsyncMock()
        monkeypatch.setattr(factory, "get_process_configs", mock.AsyncMock(return_value=[config]))
        monkeypatch.setattr(factory, "start_process", start_mock)
        monkeypatch.setattr(factory, "_start_native_process_monitoring", mock.MagicMock())
        monkeypatch.setattr(launcher_module, "get_repository", lambda _url: repository)

        await factory.start_all_processes()

        start_mock.assert_awaited_once()
        await_args = start_mock.await_args
        assert await_args is not None
        started_config = await_args.args[0]
        assert isinstance(started_config, ProcessConfigModel)
        assert started_config.parameters["wallet_public_id"] == "wallet-live"
        repository.list_active_scope_grants_for_wallet.assert_awaited_once()
        repository.list_grant_covered_instrument_public_ids.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_start_all_processes_enforces_registry_strategy_over_core_row(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Autostart treats registry strategy classification as authoritative."""
        config = _strategy_autostart_config(role=ProcessRoleEnum.CORE)
        repository = _WalletLookupRepository(operator_wallets=[_wallet_row("wallet-live")])
        registry = {
            "strategy": ProcessRegistryEntry(
                class_ref=MagicMock(),
                class_path="snapper.fake.StrategyProcess",
                method="start",
                description="",
                priority=50,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.STRATEGY,
                tags=("strategy",),
                parameters_model=None,
                parameters_schema={"type": "object"},
                enabled=True,
                mode="thread",
            )
        }
        factory = ProcessLauncherService(_create_settings())
        start_mock = mock.AsyncMock()
        monkeypatch.setattr(factory, "get_process_configs", mock.AsyncMock(return_value=[config]))
        monkeypatch.setattr(factory, "start_process", start_mock)
        monkeypatch.setattr(factory, "_start_native_process_monitoring", mock.MagicMock())
        monkeypatch.setattr(launcher_module, "get_repository", lambda _url: repository)
        monkeypatch.setattr(
            "snapper.application.process_manager.strategy_scope.get_registered_processes",
            lambda: registry,
        )

        await factory.start_all_processes()

        start_mock.assert_awaited_once()
        await_args = start_mock.await_args
        assert await_args is not None
        started_config = await_args.args[0]
        assert isinstance(started_config, ProcessConfigModel)
        assert started_config.parameters["wallet_public_id"] == "wallet-live"

    @pytest.mark.asyncio
    async def test_start_all_processes_leaves_non_strategy_unchanged(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Autostart does not apply wallet resolution to non-strategy configs."""
        config = ProcessConfigModel(
            name="backfill",
            enabled=True,
            mode="thread",
            class_path="snapper.fake.Backfill",
            method="start",
            parameters={"exchange": "kraken"},
            role=ProcessRoleEnum.TASK,
        )
        factory = ProcessLauncherService(_create_settings())
        start_mock = mock.AsyncMock()
        get_repository_mock = mock.MagicMock()
        monkeypatch.setattr(factory, "get_process_configs", mock.AsyncMock(return_value=[config]))
        monkeypatch.setattr(factory, "start_process", start_mock)
        monkeypatch.setattr(factory, "_start_native_process_monitoring", mock.MagicMock())
        monkeypatch.setattr(launcher_module, "get_repository", get_repository_mock)

        await factory.start_all_processes()

        start_mock.assert_awaited_once_with(config)
        get_repository_mock.assert_not_called()

    @pytest.mark.asyncio
    async def test_start_all_processes_skips_unclassified_strategy_shape(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Autostart fails closed when strategy-shaped params lack trusted role."""
        config = _strategy_autostart_config(role=ProcessRoleEnum.CORE)
        factory = ProcessLauncherService(_create_settings())
        start_mock = mock.AsyncMock()
        warning_mock = mock.MagicMock()
        monkeypatch.setattr(factory, "get_process_configs", mock.AsyncMock(return_value=[config]))
        monkeypatch.setattr(factory, "start_process", start_mock)
        monkeypatch.setattr(factory, "_start_native_process_monitoring", mock.MagicMock())
        monkeypatch.setattr(launcher_module.logger, "warning", warning_mock)

        await factory.start_all_processes()

        start_mock.assert_not_awaited()
        warning_mock.assert_called_once_with(
            "strategy {} not started: wallet unresolved/ambiguous; set wallet_public_id",
            "strategy",
        )


class TestStopAllProcesses:
    """Test suite for ProcessLauncherService.stop_all_processes method."""

    @pytest.mark.asyncio
    async def test_stop_all_processes_with_tasks(self) -> None:
        """Verify all tracked tasks are cancelled and cleaned up.

        Given: Factory with multiple async tasks in process_tasks.
        When: stop_all_processes is called.
        Then: All tasks are cancelled and tracking dicts cleared.
        """
        settings = MagicMock()
        factory = ProcessLauncherService(settings)
        task1 = asyncio.create_task(asyncio.sleep(100))
        task2 = asyncio.create_task(asyncio.sleep(100))
        factory.process_tasks["task1"] = task1
        factory.process_tasks["task2"] = task2
        factory.started_processes["task1"] = MagicMock(spec=[])
        factory.started_processes["task2"] = MagicMock(spec=[])
        await factory.stop_all_processes()
        assert factory.process_tasks == {}
        assert factory.started_processes == {}
        assert task1.cancelled()
        assert task2.cancelled()

    @pytest.mark.asyncio
    async def test_stop_all_processes_with_stop_method(self) -> None:
        """Verify async stop method is awaited on process instance.

        Given: Factory with process having async stop method.
        When: stop_all_processes is called.
        Then: stop method is awaited and process removed.
        """
        settings = MagicMock()
        factory = ProcessLauncherService(settings)
        mock_instance = MagicMock()
        mock_instance.stop = AsyncMock()
        factory.started_processes["process1"] = mock_instance
        await factory.stop_all_processes()
        mock_instance.stop.assert_awaited_once()
        assert factory.started_processes == {}

    @pytest.mark.asyncio
    async def test_stop_all_processes_with_sync_stop_method(self) -> None:
        """Verify synchronous stop method is called on process instance.

        Given: Factory with process having sync stop method.
        When: stop_all_processes is called.
        Then: stop method is called and process removed.
        """
        settings = MagicMock()
        factory = ProcessLauncherService(settings)
        mock_instance = MagicMock()
        mock_instance.stop = MagicMock()
        factory.started_processes["process1"] = mock_instance
        await factory.stop_all_processes()
        mock_instance.stop.assert_called_once()
        assert factory.started_processes == {}

    @pytest.mark.asyncio
    async def test_stop_all_processes_stop_error(self) -> None:
        """Verify stop errors are handled gracefully.

        Given: Factory with process whose stop method raises exception.
        When: stop_all_processes is called.
        Then: Error is caught and process still removed from tracking.
        """
        settings = MagicMock()
        factory = ProcessLauncherService(settings)
        mock_instance = MagicMock()
        mock_instance.stop = MagicMock(side_effect=Exception("Stop failed"))
        factory.started_processes["failing_process"] = mock_instance
        await factory.stop_all_processes()
        assert factory.started_processes == {}


class TestStartProcessByName:
    """Test suite for ProcessLauncherService.start_process_by_name method."""

    @pytest.mark.asyncio
    async def test_start_process_by_name_already_running(self) -> None:
        """Verify already_running status returned for running process.

        Given: Process already exists in started_processes.
        When: start_process_by_name is called with same name.
        Then: Returns status 'already_running' without restarting.
        """
        settings = MagicMock()
        factory = ProcessLauncherService(settings)
        factory.started_processes["zmq_broker"] = MagicMock()
        result = await factory.start_process_by_name("zmq_broker")
        assert result.status == "already_running"
        assert "already running" in result.message

    @pytest.mark.asyncio
    @patch("snapper.application.process_manager.launcher.get_repository")
    async def test_start_process_by_name_not_found(self, mock_get_repo: MagicMock) -> None:
        """Verify error status when process config not found in database.

        Given: No configuration exists for the requested process name.
        When: start_process_by_name is called.
        Then: Returns status 'error' with 'not found' message.
        """
        mock_repo = MagicMock()
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=mock_result)
        mock_repo.session.return_value.__aenter__.return_value = mock_session
        mock_repo.session.return_value.__aexit__.return_value = AsyncMock()
        mock_get_repo.return_value = mock_repo
        settings = MagicMock()
        settings.db_url = "sqlite:///:memory:"
        factory = ProcessLauncherService(settings)
        result = await factory.start_process_by_name("nonexistent")
        assert result.status == "error"
        assert "not found" in result.message

    @pytest.mark.asyncio
    @patch("snapper.application.process_manager.launcher.ProcessLauncherService.start_process")
    @patch("snapper.application.process_manager.launcher.get_repository")
    async def test_start_process_by_name_success(
        self, mock_get_repo: MagicMock, mock_start: AsyncMock
    ) -> None:
        """Verify successful process start returns success.

        Given: Valid process configuration in database.
        When: start_process_by_name is called.
        Then: Returns 'success', calls start_process with correct config.
        """
        mock_setting = MagicMock()
        mock_setting.value = json.dumps(
            {
                "class": "snapper.ipc.zmq_broker.ZmqBrokerThread",
                "method": "run",
                "mode": "thread",
                "parameters": {"endpoint": "tcp://0.0.0.0:5555"},
                "enabled": False,
            }
        )
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_setting
        mock_session = MagicMock()
        mock_session.execute = AsyncMock(return_value=mock_result)
        mock_repo = MagicMock()
        mock_repo.session.return_value.__aenter__.return_value = mock_session
        mock_repo.session.return_value.__aexit__.return_value = AsyncMock()
        mock_get_repo.return_value = mock_repo
        settings = MagicMock()
        settings.db_url = "sqlite:///:memory:"
        factory = ProcessLauncherService(settings)
        result = await factory.start_process_by_name("zmq_broker")
        assert result.status == "success"
        assert "started successfully" in result.message
        mock_start.assert_awaited_once()
        call_config = mock_start.call_args[0][0]
        assert call_config.enabled is False

    @pytest.mark.asyncio
    @patch("snapper.application.process_manager.launcher.ProcessLauncherService.start_process")
    @patch("snapper.application.process_manager.launcher.get_repository")
    async def test_start_process_by_name_one_shot_stays_disabled(
        self, mock_get_repo: MagicMock, mock_start: AsyncMock
    ) -> None:
        """Verify one_shot lifecycle processes report executed successfully.

        Given: Process config with lifecycle='one_shot' and enabled=False.
        When: start_process_by_name is called.
        Then: Process executes, returns 'success' with 'executed successfully'.
        """
        mock_setting = MagicMock()
        mock_setting.value = json.dumps(
            {
                "class": "snapper.services.symbol_updater.SymbolUpdaterService",
                "method": "start",
                "mode": "thread",
                "parameters": {"update_threshold_hours": 6},
                "enabled": False,
                "lifecycle": "one_shot",
            }
        )
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_setting
        mock_session = MagicMock()
        mock_session.execute = AsyncMock(return_value=mock_result)
        mock_repo = MagicMock()
        mock_repo.session.return_value.__aenter__.return_value = mock_session
        mock_repo.session.return_value.__aexit__.return_value = AsyncMock()
        mock_get_repo.return_value = mock_repo
        settings = MagicMock()
        settings.db_url = "sqlite:///:memory:"
        factory = ProcessLauncherService(settings)
        result = await factory.start_process_by_name("symbol_updater")
        assert result.status == "success"
        assert "executed successfully" in result.message
        mock_start.assert_awaited_once()
        call_config = mock_start.call_args[0][0]
        assert call_config.enabled is False
        assert call_config.lifecycle == ProcessLifecycleEnum.ONE_SHOT

    @pytest.mark.asyncio
    @patch("snapper.application.process_manager.launcher.ProcessLauncherService.start_process")
    @patch("snapper.application.process_manager.launcher.get_repository")
    async def test_start_process_by_name_with_overrides(
        self, mock_get_repo: MagicMock, mock_start: AsyncMock
    ) -> None:
        """Verify config overrides are applied at runtime.

        Given: Process config in database with default values.
        When: start_process_by_name called with mode and parameters overrides.
        Then: Overrides applied to config passed to start_process.
        """
        mock_setting = MagicMock()
        mock_setting.value = json.dumps(
            {
                "class": "test.Class",
                "method": "run",
                "mode": "thread",
                "parameters": {"default": "value"},
            }
        )
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_setting
        mock_session = MagicMock()
        mock_session.execute = AsyncMock(return_value=mock_result)
        mock_repo = MagicMock()
        mock_repo.session.return_value.__aenter__.return_value = mock_session
        mock_repo.session.return_value.__aexit__.return_value = AsyncMock()
        mock_get_repo.return_value = mock_repo
        settings = MagicMock()
        settings.db_url = "sqlite:///:memory:"
        factory = ProcessLauncherService(settings)
        result = await factory.start_process_by_name(
            "test_process",
            mode="process",
            parameters={"override": "value"},
        )
        assert result.status == "success"
        call_config = mock_start.call_args[0][0]
        assert call_config.mode == "process"
        assert call_config.parameters == {"override": "value"}

    @pytest.mark.asyncio
    @patch("snapper.application.process_manager.launcher.ProcessLauncherService.start_process")
    @patch("snapper.application.process_manager.launcher.get_repository")
    async def test_start_process_by_name_start_fails(
        self, mock_get_repo: MagicMock, mock_start: AsyncMock
    ) -> None:
        """Verify error handling when start_process raises exception.

        Given: Valid config but start_process mock raises RuntimeError.
        When: start_process_by_name is called.
        Then: Returns status 'error' with exception message.
        """
        mock_start.side_effect = Exception("Failed to start")
        mock_setting = MagicMock()
        mock_setting.value = json.dumps(
            {
                "class": "test.Class",
                "method": "run",
                "parameters": {"key": "value"},
            }
        )
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_setting
        mock_session = MagicMock()
        mock_session.execute = AsyncMock(return_value=mock_result)
        mock_repo = MagicMock()
        mock_repo.session.return_value.__aenter__.return_value = mock_session
        mock_repo.session.return_value.__aexit__.return_value = AsyncMock()
        mock_get_repo.return_value = mock_repo
        settings = MagicMock()
        settings.db_url = "sqlite:///:memory:"
        factory = ProcessLauncherService(settings)
        result = await factory.start_process_by_name("failing_process")
        assert result.status == "error"
        assert "Failed to start" in result.message


class TestStopProcessByName:
    """Test suite for ProcessLauncherService.stop_process_by_name method."""

    @pytest.mark.asyncio
    async def test_stop_process_by_name_not_running(self) -> None:
        """Verify not_running status for process not in started_processes.

        Given: Process name not in started_processes dict.
        When: stop_process_by_name is called.
        Then: Returns status 'not_running'.
        """
        settings = MagicMock()
        factory = ProcessLauncherService(settings)
        result = await factory.stop_process_by_name("nonexistent")
        assert result.status == "not_running"
        assert "not running" in result.message

    @pytest.mark.asyncio
    async def test_stop_process_by_name_success(self) -> None:
        """Verify successful stop removes process from tracking.

        Given: Running process with async stop method.
        When: stop_process_by_name is called.
        Then: Process stopped, removed from tracking.
        """
        mock_instance = MagicMock()
        mock_instance.stop = AsyncMock()
        settings = MagicMock()
        settings.db_url = "sqlite:///:memory:"
        factory = ProcessLauncherService(settings)
        factory.started_processes["test_process"] = mock_instance
        factory._finalize_process_run = AsyncMock()
        result = await factory.stop_process_by_name("test_process")
        assert result.status == "success"
        assert "stopped successfully" in result.message
        mock_instance.stop.assert_awaited_once()
        assert "test_process" not in factory.started_processes

    @pytest.mark.asyncio
    async def test_stop_process_by_name_with_task(self) -> None:
        """Verify process with associated task cancels task on stop.

        Given: Running process with associated asyncio task.
        When: stop_process_by_name is called.
        Then: Task cancelled, process removed from tracking.
        """
        mock_instance = MagicMock(spec=[])
        mock_instance.stop = AsyncMock()
        task = asyncio.create_task(asyncio.sleep(100))
        settings = MagicMock()
        settings.db_url = "sqlite:///:memory:"
        factory = ProcessLauncherService(settings)
        factory.started_processes["task_process"] = mock_instance
        factory.process_tasks["task_process"] = task
        factory._finalize_process_run = AsyncMock()
        result = await factory.stop_process_by_name("task_process")
        assert result.status == "success"
        assert "stopped successfully" in result.message
        assert task.cancelled()
        assert "task_process" not in factory.process_tasks
        assert "task_process" not in factory.started_processes

    @pytest.mark.asyncio
    async def test_stop_process_by_name_stop_error(self) -> None:
        """Verify stop errors return error status with message.

        Given: Running process whose stop method raises exception.
        When: stop_process_by_name is called.
        Then: Returns status 'error' with exception message.
        """
        mock_instance = MagicMock()
        mock_instance.stop = AsyncMock(side_effect=Exception("Stop failed"))
        settings = MagicMock()
        settings.db_url = "sqlite:///:memory:"
        factory = ProcessLauncherService(settings)
        factory.started_processes["failing_process"] = mock_instance
        result = await factory.stop_process_by_name("failing_process")
        assert result.status == "error"
        assert "Stop failed" in result.message


class TestGetProcessStatus:
    """Test suite for ProcessLauncherService.get_process_status method."""

    @pytest.mark.asyncio
    async def test_get_process_status_not_running(self) -> None:
        """Verify status for non-running process shows running=False.

        Given: Process name not in started_processes.
        When: get_process_status is called.
        Then: Returns dict with running=False, no details.
        """
        settings = MagicMock()
        factory = ProcessLauncherService(settings)
        result = await factory.get_process_status("nonexistent")
        assert result.name == "nonexistent"
        assert result.running is False
        assert result.details is None

    @pytest.mark.asyncio
    async def test_get_process_status_running_simple(self) -> None:
        """Verify status for running process without get_status method.

        Given: Process in started_processes without get_status method.
        When: get_process_status is called.
        Then: Returns dict with running=True, no details.
        """
        settings = MagicMock()
        factory = ProcessLauncherService(settings)
        mock_instance = MagicMock(spec=[])
        factory.started_processes["running_process"] = mock_instance
        result = await factory.get_process_status("running_process")
        assert result.name == "running_process"
        assert result.running is True
        assert result.details is None

    @pytest.mark.asyncio
    async def test_get_process_status_with_details(self) -> None:
        """Verify status includes details from process get_status method.

        Given: Process in started_processes with get_status method.
        When: get_process_status is called.
        Then: Returns running=True with details from get_status.
        """
        settings = MagicMock()
        factory = ProcessLauncherService(settings)
        mock_instance = MagicMock()
        mock_instance.get_status = MagicMock(return_value={"custom": "status", "count": 42})
        factory.started_processes["detailed_process"] = mock_instance
        result = await factory.get_process_status("detailed_process")
        assert result.name == "detailed_process"
        assert result.running is True
        assert result.details == {"custom": "status", "count": 42}
        mock_instance.get_status.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_process_status_get_status_fails(self) -> None:
        """Verify error in get_status is handled gracefully.

        Given: Process with get_status method that raises exception.
        When: get_process_status is called.
        Then: Returns running=True without details, no exception raised.
        """
        settings = MagicMock()
        factory = ProcessLauncherService(settings)
        mock_instance = MagicMock()
        mock_instance.get_status = MagicMock(side_effect=Exception("Status error"))
        factory.started_processes["error_process"] = mock_instance
        result = await factory.get_process_status("error_process")
        assert result.name == "error_process"
        assert result.running is True
        assert result.details is None


@dataclass
class _DummySettingsService:
    """Test dummy for settings service."""

    def get_setting(self, key: str, default: Any) -> Any:
        return default


def _create_settings() -> AppSettings:
    bootstrap = BootstrapSettingsLoader()
    return AppSettings(bootstrap, _DummySettingsService())


def _wallet_row(public_id: str, *, is_paper: bool = False) -> WalletRow:
    """Build a wallet row for autostart wallet-resolution tests."""
    return WalletRow(
        public_id=public_id,
        label=public_id,
        description=None,
        is_paper=is_paper,
        timestamp=datetime(2026, 1, 1, tzinfo=UTC),
        session_id="test-session",
        sequence_id=1,
    )


class _WalletLookupRepository:
    """Wallet-resolution repository double for autostart tests."""

    def __init__(
        self,
        *,
        active_wallets: list[WalletRow] | None = None,
        operator_wallets: list[WalletRow] | None = None,
    ) -> None:
        """Store wallet lookup fixtures."""
        self.active_wallets = active_wallets or []
        self.operator_wallets = operator_wallets or []
        self.active_lookup_count = 0
        self.operator_lookup_count = 0
        self.operator_public_ids: list[str] = []

    async def list_active_wallets(self, as_of: datetime) -> list[WalletRow]:
        """Return active wallet fixtures."""
        self.active_lookup_count += 1
        return list(self.active_wallets)

    async def list_accessible_wallets_for_operators(
        self,
        operator_public_ids: list[str],
        as_of: datetime,
    ) -> list[WalletRow]:
        """Return operator-scoped wallet fixtures."""
        self.operator_lookup_count += 1
        self.operator_public_ids = list(operator_public_ids)
        return list(self.operator_wallets)


def _strategy_autostart_config(
    *,
    operator_public_id: str = "op-1",
    wallet_public_id: str = "",
    exchange: str | None = "kraken",
    role: ProcessRoleEnum = ProcessRoleEnum.STRATEGY,
) -> ProcessConfigModel:
    """Build an enabled strategy config for autostart tests."""
    parameters: JsonObject = {
        "name": "strategy",
        "inputs": ["candles.BTC-USD"],
        "outputs": ["orders.BTC-USD"],
        "operator_public_id": operator_public_id,
        "wallet_public_id": wallet_public_id,
    }
    if exchange is not None:
        parameters["exchange"] = exchange
    return ProcessConfigModel(
        name="strategy",
        enabled=True,
        mode="thread",
        class_path="snapper.fake.StrategyProcess",
        method="start",
        parameters=parameters,
        role=role,
    )


def test_classify_strategy_process_rejects_non_string_parameter_key() -> None:
    """Reject strategy-shaped parameters with non-string keys.

    Given: Strategy-shaped persisted parameters with a non-string key,
    When: The shared classifier runs,
    Then: It fails closed with the persisted-parameter error.
    """
    raw_parameters: dict[object, object] = {
        "name": "strategy",
        "inputs": ["candles.BTC-USD"],
        "outputs": ["orders.BTC-USD"],
        1: "invalid",
    }
    with pytest.raises(StrategyScopeError) as exc_info:
        classify_strategy_process(
            raw_role=ProcessRoleEnum.STRATEGY,
            class_path="",
            raw_parameters=raw_parameters,
        )
    assert exc_info.value.detail == "invalid persisted strategy parameters"


@pytest.mark.asyncio
async def test_resolve_classified_strategy_scope_skips_non_strategy() -> None:
    """Return an empty wallet scope for classified non-strategies.

    Given: A classification that is not a strategy,
    When: The shared wallet-scope resolver runs,
    Then: It returns an empty scope without touching wallet lookups.
    """
    repository = _WalletLookupRepository(active_wallets=[_wallet_row("wallet-live")])
    classification = StrategyProcessClassification(
        treat_as_strategy=False,
        parameters=None,
        row_role=ProcessRoleEnum.CORE,
        registry_role=None,
    )
    scope = await resolve_classified_strategy_scope(
        repository,
        classification=classification,
        principal_operator_public_ids=None,
        allow_admin_lookup_without_operator=True,
        allow_unscoped_paper=False,
        require_operator_for_explicit_wallet=False,
    )
    assert scope.treat_as_strategy is False
    assert scope.parameters is None
    assert repository.active_lookup_count == 0


@pytest.mark.asyncio
async def test_resolve_classified_strategy_scope_rejects_live_admin_lookup_without_operator() -> (
    None
):
    """Live strategies cannot admin-resolve a wallet without an operator.

    Given: Strategy parameters with live exchange and no operator,
    When: The shared resolver is called with admin lookup allowed,
    Then: It fails closed before querying the wallet catalogue.
    """
    repository = _WalletLookupRepository(active_wallets=[_wallet_row("wallet-live")])
    classification = StrategyProcessClassification(
        treat_as_strategy=True,
        parameters=dict(_strategy_autostart_config(operator_public_id="").parameters),
        row_role=ProcessRoleEnum.STRATEGY,
        registry_role=None,
    )
    with pytest.raises(StrategyScopeError) as exc_info:
        await resolve_classified_strategy_scope(
            repository,
            classification=classification,
            principal_operator_public_ids=None,
            allow_admin_lookup_without_operator=True,
            allow_unscoped_paper=False,
            require_operator_for_explicit_wallet=False,
        )
    assert (
        exc_info.value.detail == "operator_public_id required for live strategy wallet resolution"
    )
    assert repository.active_lookup_count == 0


@pytest.mark.asyncio
async def test_resolve_classified_strategy_scope_rejects_live_explicit_wallet_without_operator() -> (
    None
):
    """Live explicit wallets still require an accountable operator.

    Given: Strategy parameters with live exchange, explicit wallet, and no operator,
    When: The shared resolver is called with explicit wallets otherwise allowed,
    Then: It fails closed before returning the wallet override.
    """
    repository = _WalletLookupRepository()
    classification = StrategyProcessClassification(
        treat_as_strategy=True,
        parameters=dict(
            _strategy_autostart_config(
                operator_public_id="",
                wallet_public_id="wallet-pinned",
            ).parameters
        ),
        row_role=ProcessRoleEnum.STRATEGY,
        registry_role=None,
    )
    with pytest.raises(StrategyScopeError) as exc_info:
        await resolve_classified_strategy_scope(
            repository,
            classification=classification,
            principal_operator_public_ids=None,
            allow_admin_lookup_without_operator=True,
            allow_unscoped_paper=False,
            require_operator_for_explicit_wallet=False,
        )
    assert exc_info.value.detail == "wallet_public_id supplied without operator_public_id"
    assert repository.active_lookup_count == 0


@pytest.mark.asyncio
async def test_resolve_classified_strategy_scope_rejects_paper_without_operator_when_no_fallback() -> (
    None
):
    """Paper strategies need an allowed fallback when no operator is pinned.

    Given: Paper strategy parameters with no operator and no wallet,
    When: The shared resolver forbids both unscoped paper and admin lookup,
    Then: It rejects the launch before querying wallets.
    """
    repository = _WalletLookupRepository(
        active_wallets=[_wallet_row("wallet-paper", is_paper=True)]
    )
    classification = StrategyProcessClassification(
        treat_as_strategy=True,
        parameters=dict(
            _strategy_autostart_config(
                operator_public_id="",
                exchange="paper",
            ).parameters
        ),
        row_role=ProcessRoleEnum.STRATEGY,
        registry_role=None,
    )
    with pytest.raises(StrategyScopeError) as exc_info:
        await resolve_classified_strategy_scope(
            repository,
            classification=classification,
            principal_operator_public_ids=None,
            allow_admin_lookup_without_operator=False,
            allow_unscoped_paper=False,
            require_operator_for_explicit_wallet=False,
        )
    assert (
        exc_info.value.detail == "operator_public_id required for live strategy wallet resolution"
    )
    assert repository.active_lookup_count == 0


@pytest.mark.asyncio
async def test_resolve_classified_strategy_scope_allows_paper_explicit_wallet_without_operator() -> (
    None
):
    """Paper explicit wallet launches can bypass operator binding when allowed.

    Given: Paper strategy parameters with a wallet and no operator,
    When: Explicit wallets without operators are allowed,
    Then: The resolver returns the pinned wallet without catalog lookup.
    """
    repository = _WalletLookupRepository()
    classification = StrategyProcessClassification(
        treat_as_strategy=True,
        parameters=dict(
            _strategy_autostart_config(
                operator_public_id="",
                wallet_public_id="wallet-paper",
                exchange="paper",
            ).parameters
        ),
        row_role=ProcessRoleEnum.STRATEGY,
        registry_role=None,
    )
    scope = await resolve_classified_strategy_scope(
        repository,
        classification=classification,
        principal_operator_public_ids=[],
        allow_admin_lookup_without_operator=False,
        allow_unscoped_paper=False,
        require_operator_for_explicit_wallet=False,
    )
    assert scope.treat_as_strategy is True
    assert scope.operator_public_id == ""
    assert scope.wallet_public_id == "wallet-paper"
    assert repository.active_lookup_count == 0
    assert repository.operator_lookup_count == 0


@pytest.mark.asyncio
async def test_resolve_strategy_process_scope_classifies_non_strategy() -> None:
    """Classify-and-resolve returns an empty scope for non-strategies.

    Given: A persisted process role that is not a strategy,
    When: The classify-and-resolve helper runs,
    Then: It returns an empty scope without binding any wallet.
    """
    scope = await resolve_strategy_process_scope(
        _WalletLookupRepository(),
        raw_role=ProcessRoleEnum.CORE,
        class_path="",
        raw_parameters={"exchange": "kraken"},
        principal_operator_public_ids=None,
        allow_admin_lookup_without_operator=False,
        allow_unscoped_paper=False,
        require_operator_for_explicit_wallet=True,
    )
    assert scope.treat_as_strategy is False
    assert scope.parameters is None
    assert scope.operator_public_id == ""
    assert scope.wallet_public_id == ""
    assert scope.mode is None


def _stub_run_tracking(factory: ProcessLauncherService) -> None:
    cast(Any, factory)._create_process_run_record = mock.AsyncMock(return_value="test-run-id")
    cast(Any, factory)._update_process_run_record = mock.AsyncMock(return_value=None)
    cast(Any, factory)._run_recorder.update_run_record = mock.AsyncMock(return_value=None)


class SyncProcess:
    """Helper class for testing synchronous process start."""

    def __init__(self, tracker: list[str]) -> None:
        """Initialize with tracker list for recording invocations."""
        self.tracker = tracker

    def start(self) -> None:
        """Record sync invocation to tracker."""
        self.tracker.append("sync")


class AsyncProcess:
    """Helper class for testing async process start with delay."""

    def __init__(self, tracker: list[str]) -> None:
        """Initialize with tracker list for recording invocations."""
        self.tracker = tracker

    async def start(self) -> None:
        """Record async invocation and sleep to simulate long-running task."""
        self.tracker.append("async")
        await asyncio.sleep(10)


class FailingAsyncProcess:
    """Helper class for testing async process that fails after delay."""

    def __init__(self, tracker: list[str]) -> None:
        """Initialize with tracker list for recording invocations."""
        self.tracker = tracker

    async def start(self) -> None:
        """Record invocation, delay, then raise RuntimeError."""
        self.tracker.append("failing")
        await asyncio.sleep(0.2)
        raise RuntimeError("boom")


class ImmediateFailAsyncProcess:
    """Helper class for testing async process that fails immediately."""

    def __init__(self, tracker: list[str]) -> None:
        """Initialize with tracker list for recording invocations."""
        self.tracker = tracker

    async def start(self) -> None:
        """Record invocation and immediately raise RuntimeError."""
        self.tracker.append("fail")
        raise RuntimeError("boom-immediate")


class _DummyResult:
    """Test dummy for SQLAlchemy result."""

    def __init__(self, setting: Setting | list[Setting] | None) -> None:
        self.setting = setting

    def scalar_one_or_none(self) -> Setting | None:
        if isinstance(self.setting, list):
            return self.setting[0] if self.setting else None
        return self.setting

    def scalars(self) -> _DummyResult:
        return self

    def all(self) -> list[Setting]:
        if self.setting is None:
            return []
        if isinstance(self.setting, list):
            return self.setting
        return [self.setting]

    def first(self) -> Setting | None:
        """Return the first result or None."""
        if self.setting is None:
            return None
        if isinstance(self.setting, list):
            return self.setting[0] if self.setting else None
        return self.setting


class _DummySession:
    """Test dummy for async database session."""

    def __init__(self, setting: Setting | list[Setting] | None) -> None:
        self.setting = setting
        self.commit_called = False
        self.added: list[Any] = []

    async def __aenter__(self) -> _DummySession:
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        return False

    async def execute(self, _stmt: Any) -> _DummyResult:
        return _DummyResult(self.setting)

    async def commit(self) -> None:
        self.commit_called = True

    def add(self, _item: Any) -> None:
        self.added.append(_item)


class _DummyRepository:
    """Test dummy for database repository."""

    def __init__(self, setting: Setting | list[Setting] | None) -> None:
        self.setting = setting
        self.last_session: _DummySession | None = None

    def session(self) -> contextlib.AbstractAsyncContextManager[_DummySession]:
        self.last_session = _DummySession(self.setting)
        return self.last_session


class _RunsResult:
    """Test stub for process runs result."""

    def __init__(self, runs: list[Any]) -> None:
        self.runs = runs

    def scalars(self) -> _RunsResult:
        return self

    def all(self) -> list[Any]:
        return self.runs


class _RunsSession:
    """Test stub for process runs session."""

    def __init__(self, runs: list[Any]) -> None:
        self.runs = runs
        self.committed = False

    async def __aenter__(self) -> _RunsSession:
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        return False

    async def execute(self, _stmt: Any) -> _RunsResult:
        return _RunsResult(self.runs)

    async def commit(self) -> None:
        self.committed = True

    def add(self, _item: Any) -> None:
        return None


class _RunsRepository:
    """Test stub for process runs repository."""

    def __init__(self, runs: list[Any]) -> None:
        self.runs = runs
        self.last_session: _RunsSession | None = None

    def session(self) -> contextlib.AbstractAsyncContextManager[_RunsSession]:
        self.last_session = _RunsSession(self.runs)
        return self.last_session


@pytest.mark.asyncio()
async def test_start_process_process_mode_filters_parameters() -> None:
    """Verify process mode spawns via ProcessSpawner with filtered parameters.

    Given: ProcessConfig with mode='process' and parameters.
    When: start_process is called.
    Then: ProcessSpawner.spawn called with parameters, process tracked.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    _stub_run_tracking(factory)
    spawner_mock = mock.create_autospec(ProcessSpawnerService, instance=True)
    spawn_mock = spawner_mock.spawn
    spawn_mock.return_value = SimpleNamespace(pid=1234)
    factory.spawner = cast(ProcessSpawnerService, spawner_mock)
    config = ProcessConfigModel(
        name="os_process",
        enabled=True,
        mode="process",
        class_path="tests.application.process_manager.test_process_launcher.SyncProcess",
        method="start",
        parameters={"keep": "value"},
    )
    await factory.start_process(config)
    spawn_mock.assert_called_once_with(
        name="os_process",
        class_path="tests.application.process_manager.test_process_launcher.SyncProcess",
        method="start",
        parameters={"keep": "value"},
    )
    assert factory.started_processes["os_process"].pid == 1234
    assert factory.process_tasks == {}
    assert factory.active_runs["os_process"] == "test-run-id"


@pytest.mark.asyncio()
async def test_start_process_async_method_creates_task() -> None:
    """Verify async method creates tracked asyncio task.

    Given: ProcessConfig with async start method.
    When: start_process is called.
    Then: Task created, tracked in process_tasks, instance stored.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    _stub_run_tracking(factory)
    tracker: list[str] = []
    config = ProcessConfigModel(
        name="async_proc",
        enabled=True,
        mode="thread",
        class_path="tests.application.process_manager.test_process_launcher.AsyncProcess",
        method="start",
        parameters=cast(JsonObject, {"tracker": tracker}),
    )
    await factory.start_process(config)
    assert tracker == ["async"]
    assert "async_proc" in factory.process_tasks
    task = factory.process_tasks["async_proc"]
    assert isinstance(task, asyncio.Task)
    assert not task.cancelled()
    assert factory.started_processes["async_proc"].tracker is tracker
    assert factory.active_runs["async_proc"] == "test-run-id"
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


@pytest.mark.asyncio()
async def test_start_process_sync_method_uses_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify sync method runs in executor without blocking event loop.

    Given: ProcessConfig with sync start method and mode='thread'.
    When: start_process is called.
    Then: Method run via run_in_executor, instance stored.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    _stub_run_tracking(factory)
    tracker: list[str] = []
    config = ProcessConfigModel(
        name="sync_proc",
        enabled=True,
        mode="thread",
        class_path="tests.application.process_manager.test_process_launcher.SyncProcess",
        method="start",
        parameters=cast(JsonObject, {"tracker": tracker}),
    )

    class DummyLoop:
        def __init__(self) -> None:
            self.called = False

        async def run_in_executor(self, _executor: Any, func: Any) -> None:
            self.called = True
            func()

    dummy_loop = DummyLoop()
    monkeypatch.setattr(asyncio, "get_event_loop", lambda: dummy_loop)
    await factory.start_process(config)
    assert tracker == ["sync"]
    assert dummy_loop.called
    assert factory.started_processes["sync_proc"].tracker is tracker
    assert "sync_proc" not in factory.active_runs


@pytest.mark.asyncio()
async def test_start_process_async_failure_cleans_up() -> None:
    """Verify async process failure triggers cleanup of all tracking.

    Given: Async process that fails after short delay.
    When: start_process is called and process fails.
    Then: Process removed from all tracking dicts.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    _stub_run_tracking(factory)
    tracker: list[str] = []
    config = ProcessConfigModel(
        name="failing_proc",
        enabled=True,
        mode="thread",
        class_path="tests.application.process_manager.test_process_launcher.FailingAsyncProcess",
        method="start",
        parameters=cast(JsonObject, {"tracker": tracker}),
    )
    await factory.start_process(config)
    for _ in range(20):
        if "failing_proc" not in factory.process_tasks:
            break
        await asyncio.sleep(0.01)
    for _ in range(20):
        if "failing_proc" not in factory.started_processes:
            break
        await asyncio.sleep(0.01)
    assert tracker == ["failing"]
    assert "failing_proc" not in factory.started_processes
    assert "failing_proc" not in factory.process_tasks
    assert "failing_proc" not in factory.process_lifecycles
    assert "failing_proc" not in factory.active_runs


@pytest.mark.asyncio()
async def test_start_process_by_name_handles_missing_setting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify error returned when process setting not in database.

    Given: Repository returns None for setting lookup.
    When: start_process_by_name is called.
    Then: Returns status 'error' with 'not found' message.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    repo = _DummyRepository(None)
    monkeypatch.setattr(
        "snapper.application.process_manager.launcher.get_repository", lambda _url: repo
    )
    result = await factory.start_process_by_name("nonexistent")
    assert result.status == "error"
    assert "not found" in result.message


@pytest.mark.asyncio()
async def test_start_process_by_name_when_already_running() -> None:
    """Verify already_running status when process exists in started_processes.

    Given: Process already in started_processes dict.
    When: start_process_by_name is called.
    Then: Returns status 'already_running'.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    factory.started_processes["worker"] = SimpleNamespace()
    result = await factory.start_process_by_name("worker")
    assert result.status == "already_running"


@pytest.mark.asyncio()
async def test_start_process_by_name_reports_start_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify start_process exception is caught and reported as error.

    Given: Valid config but start_process raises RuntimeError.
    When: start_process_by_name is called.
    Then: Returns status 'error' with error message.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    cast(Any, factory).start_process = mock.AsyncMock(side_effect=RuntimeError("boom"))
    raw_config = {
        "enabled": True,
        "mode": "thread",
        "class": "tests.application.process_manager.test_process_launcher.SyncProcess",
        "parameters": {},
    }
    setting = Setting(
        key="process_worker", value=json.dumps(raw_config), session_id="test-session", sequence_id=1
    )
    repo = _DummyRepository(setting)
    monkeypatch.setattr(
        "snapper.application.process_manager.launcher.get_repository", lambda _url: repo
    )
    monkeypatch.setattr(
        "snapper.application.process_manager.launcher.get_registered_processes",
        lambda: {
            "drop_tags": ProcessRegistryEntry(
                class_ref=MagicMock(),
                class_path="",
                method="",
                description="",
                priority=0,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.CORE,
                tags=(),
                parameters_model=None,
                parameters_schema=None,
                enabled=True,
                mode="thread",
            )
        },
    )
    result = await factory.start_process_by_name("worker")
    assert result.status == "error"
    assert "Failed to start process 'worker'" in result.message


@pytest.mark.asyncio()
async def test_start_process_by_name_one_shot_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify one_shot lifecycle returns 'executed successfully' message.

    Given: Process config with lifecycle=one_shot.
    When: start_process_by_name is called.
    Then: Returns success with 'executed successfully' message.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    cast(Any, factory).start_process = mock.AsyncMock()
    raw_config = {
        "enabled": True,
        "mode": "thread",
        "class": "tests.application.process_manager.test_process_launcher.SyncProcess",
        "parameters": {},
        "lifecycle": ProcessLifecycleEnum.ONE_SHOT.value,
    }
    setting = Setting(
        key="process_once", value=json.dumps(raw_config), session_id="test-session", sequence_id=1
    )
    repo = _DummyRepository(setting)
    monkeypatch.setattr(
        "snapper.application.process_manager.launcher.get_repository", lambda _url: repo
    )
    monkeypatch.setattr(
        "snapper.application.process_manager.launcher.get_registered_processes", lambda: {}
    )
    result = await factory.start_process_by_name("once")
    assert result.status == "success"
    assert "executed successfully" in result.message
    cast(mock.AsyncMock, factory.start_process).assert_awaited_once()


@pytest.mark.asyncio()
async def test_start_process_by_name_updates_config_and_persists_overrides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify overrides update config and persist to database.

    Given: Config with invalid lifecycle/role/tags in database.
    When: start_process_by_name called with overrides and autostart=True.
    Then: Overrides applied, metadata fixed from registry, db committed.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    cast(Any, factory).start_process = mock.AsyncMock()
    mock_start = cast(mock.AsyncMock, factory.start_process)
    raw_config = {
        "enabled": False,
        "mode": "thread",
        "class": "tests.application.process_manager.test_process_launcher.SyncProcess",
        "parameters": {},
        "lifecycle": "invalid",
        "role": "invalid",
        "tags": "oops",
    }
    setting = Setting(
        key="process_worker", value=json.dumps(raw_config), session_id="test-session", sequence_id=1
    )
    repo = _DummyRepository(setting)
    monkeypatch.setattr(
        "snapper.application.process_manager.launcher.get_repository", lambda _url: repo
    )
    monkeypatch.setattr(
        "snapper.application.process_manager.launcher.get_registered_processes",
        lambda: {
            "worker": ProcessRegistryEntry(
                class_ref=SyncProcess,
                class_path="tests.application.process_manager.test_process_launcher.SyncProcess",
                method="start",
                description="",
                priority=0,
                lifecycle=ProcessLifecycleEnum.ONE_SHOT,
                role=ProcessRoleEnum.CORE,
                tags=("a", "b"),
                parameters_model=None,
                parameters_schema=None,
                enabled=True,
                mode="thread",
            )
        },
    )
    result = await factory.start_process_by_name(
        "worker",
        mode="process",
        parameters={"x": 1},
    )
    assert result.status == "success"
    assert mock_start.called
    call_config = mock_start.call_args[0][0]
    assert call_config.mode == "process"
    assert call_config.parameters == {"x": 1}


@pytest.mark.asyncio()
async def test_start_process_by_name_keeps_tags_when_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify existing tags in config are preserved on start.

    Given: Config with tags=['keep'] and parameters_schema in database.
    When: start_process_by_name is called.
    Then: Tags remain ('keep',) in the ProcessConfigModel passed to start_process.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    mock_start = mock.AsyncMock()
    cast(Any, factory).start_process = mock_start
    config_dict = {
        "enabled": True,
        "mode": "thread",
        "class": "tests.application.process_manager.test_process_launcher.SyncProcess",
        "parameters": {},
        "tags": ["keep"],
        "parameters_schema": {},
    }
    setting = Setting(
        key="process_tagged",
        value=json.dumps(config_dict),
        session_id="test-session",
        sequence_id=1,
    )
    repo = _DummyRepository(setting)
    monkeypatch.setattr(
        "snapper.application.process_manager.launcher.get_repository", lambda _url: repo
    )
    monkeypatch.setattr(
        "snapper.application.process_manager.launcher.get_registered_processes", lambda: {}
    )
    result = await factory.start_process_by_name("tagged")
    assert result.status == "success"
    call_config = mock_start.call_args[0][0]
    assert call_config.tags == ("keep",)


@pytest.mark.asyncio()
async def test_stop_process_by_name_not_running() -> None:
    """Verify not_running status for process not in started_processes.

    Given: Process name not tracked in started_processes.
    When: stop_process_by_name is called.
    Then: Returns status 'not_running'.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    result = await factory.stop_process_by_name("worker")
    assert result.status == "not_running"


@pytest.mark.asyncio()
async def test_stop_process_by_name_async_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify async stop method awaited and process removed from tracking.

    Given: Running process with async stop method.
    When: stop_process_by_name is called.
    Then: stop awaited, process removed from tracking.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)

    class _AsyncProcess:
        def __init__(self) -> None:
            self.stopped = False

        async def stop(self) -> None:
            self.stopped = True

    proc = _AsyncProcess()
    factory.started_processes["worker"] = proc
    monkeypatch.setattr(factory, "_finalize_process_run", mock.AsyncMock())
    result = await factory.stop_process_by_name("worker")
    assert result.status == "success"
    assert "stopped successfully" in result.message
    assert proc.stopped is True
    assert "worker" not in factory.started_processes


@pytest.mark.asyncio()
async def test_stop_process_by_name_with_coroutine_stop_and_no_setting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify stop works when db setting is missing.

    Given: Running process with done task, no db setting.
    When: stop_process_by_name is called.
    Then: Process stopped and removed, no db error.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)

    class _Process:
        def __init__(self) -> None:
            self.stopped = False

        async def stop(self) -> None:
            self.stopped = True

    proc = _Process()
    done_task = asyncio.create_task(asyncio.sleep(0))
    await done_task
    factory.process_tasks["worker"] = done_task
    factory.started_processes["worker"] = proc
    monkeypatch.setattr(
        "snapper.application.process_manager.launcher.get_repository",
        lambda _url: _DummyRepository(None),
    )
    monkeypatch.setattr(factory, "_finalize_process_run", mock.AsyncMock())
    result = await factory.stop_process_by_name("worker")
    assert result.status == "success"
    assert proc.stopped is True
    assert "worker" not in factory.started_processes


@pytest.mark.asyncio()
async def test_stop_process_by_name_when_instance_is_none_cleans_up(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify None instance is cleaned up from tracking on stop.

    Given: Process tracked but instance is None.
    When: stop_process_by_name is called.
    Then: Process removed from tracking.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    factory.started_processes["ghost"] = None
    monkeypatch.setattr(factory, "_finalize_process_run", mock.AsyncMock())
    result = await factory.stop_process_by_name("ghost")
    assert result.status == "success"
    assert "stopped successfully" in result.message
    assert "ghost" not in factory.started_processes


@pytest.mark.asyncio()
async def test_stop_process_by_name_when_stop_handler_removes_instance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify stop handles process that removes itself during stop.

    Given: Process whose stop method removes itself from started_processes.
    When: stop_process_by_name is called.
    Then: Returns success, process not in tracking.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)

    class _SelfRemoving:
        def __init__(self, launcher: ProcessLauncherService) -> None:
            self.launcher = launcher

        async def stop(self) -> None:
            self.launcher.started_processes.pop("selfrem", None)

    proc = _SelfRemoving(factory)
    factory.started_processes["selfrem"] = proc
    setting = Setting(
        key="process_selfrem",
        value=json.dumps({"class": "x", "enabled": True, "parameters": {}}),
        session_id="test-session",
        sequence_id=1,
    )
    repo = _DummyRepository(setting)
    monkeypatch.setattr(
        "snapper.application.process_manager.launcher.get_repository", lambda _url: repo
    )
    monkeypatch.setattr(factory, "_finalize_process_run", mock.AsyncMock())
    result = await factory.stop_process_by_name("selfrem")
    assert result.status == "success"
    assert "selfrem" not in factory.started_processes


@pytest.mark.asyncio()
async def test_handle_task_completion_unexpected_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify successful completion finalizes run and cleans up.

    Given: Completed task for long_running process.
    When: _handle_task_completion is called.
    Then: Finalize called with SUCCEEDED, process removed from tracking.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    task = asyncio.create_task(asyncio.sleep(0))
    await task
    factory.process_tasks["job"] = task
    factory.started_processes["job"] = object()
    factory.process_lifecycles["job"] = ProcessLifecycleEnum.LONG_RUNNING
    factory.process_roles["job"] = ProcessRoleEnum.CORE
    finalize_mock = mock.AsyncMock()
    monkeypatch.setattr(factory, "_finalize_process_run", finalize_mock)
    await factory._handle_task_completion("job", task)
    finalize_mock.assert_awaited_once_with("job", ProcessRunStatusEnum.SUCCEEDED, error=None)
    assert "job" not in factory.started_processes
    assert "job" not in factory.process_tasks


@pytest.mark.asyncio()
async def test_handle_task_completion_skips_finalize_on_generator_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify GeneratorExit does not trigger finalize.

    Given: Task that raised GeneratorExit.
    When: _handle_task_completion is called.
    Then: Finalize not called, process cleaned up.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)

    async def _gen_exit() -> None:
        raise GeneratorExit()

    task = asyncio.create_task(_gen_exit())
    with contextlib.suppress(GeneratorExit):
        await task
    factory.process_tasks["job"] = task
    factory.started_processes["job"] = object()
    factory.process_lifecycles["job"] = ProcessLifecycleEnum.LONG_RUNNING
    factory.process_roles["job"] = ProcessRoleEnum.CORE
    finalize_mock = mock.AsyncMock()
    monkeypatch.setattr(factory, "_finalize_process_run", finalize_mock)
    await factory._handle_task_completion("job", task)
    finalize_mock.assert_not_awaited()
    assert "job" not in factory.started_processes
    assert "job" not in factory.process_tasks


@pytest.mark.asyncio()
async def test_handle_task_completion_cancelled_finalizes_run_record(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify cancelled tasks finalize their DB run record.

    Given: A task that was cancelled (e.g. by ``stop_all_processes``),
    When: ``_handle_task_completion`` is called via the done-callback,
    Then: ``_finalize_process_run`` IS called with CANCELLED so the
        DB run record does not stay stuck as "running" — guards
        ``task.exception()`` raising ``CancelledError`` (a
        ``BaseException`` not caught by the surrounding ``except
        Exception``).
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)

    async def _runs_until_cancelled() -> None:
        await asyncio.sleep(60)

    task = asyncio.create_task(_runs_until_cancelled())
    await asyncio.sleep(0)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    factory.process_tasks["job"] = task
    factory.started_processes["job"] = object()
    factory.process_lifecycles["job"] = ProcessLifecycleEnum.LONG_RUNNING
    factory.process_roles["job"] = ProcessRoleEnum.CORE
    finalize_mock = mock.AsyncMock()
    monkeypatch.setattr(factory, "_finalize_process_run", finalize_mock)
    await factory._handle_task_completion("job", task)
    finalize_mock.assert_awaited_once_with("job", ProcessRunStatusEnum.CANCELLED, error=None)
    assert "job" not in factory.started_processes
    assert "job" not in factory.process_tasks


@pytest.mark.asyncio()
async def test_handle_task_completion_failure_records_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify task exception finalizes with FAILED status and error.

    Given: Task that raised RuntimeError('boom').
    When: _handle_task_completion is called.
    Then: Finalize called with FAILED and error='boom'.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)

    async def _boom() -> None:
        raise RuntimeError("boom")

    task = asyncio.create_task(_boom())
    with contextlib.suppress(RuntimeError):
        await task
    factory.process_tasks["job"] = task
    factory.started_processes["job"] = object()
    factory.process_lifecycles["job"] = ProcessLifecycleEnum.LONG_RUNNING
    factory.process_roles["job"] = ProcessRoleEnum.CORE
    finalize_mock = mock.AsyncMock()
    monkeypatch.setattr(factory, "_finalize_process_run", finalize_mock)
    await factory._handle_task_completion("job", task)
    finalize_mock.assert_awaited_once_with("job", ProcessRunStatusEnum.FAILED, error="boom")
    assert "job" not in factory.started_processes
    assert "job" not in factory.process_tasks


@pytest.mark.asyncio()
async def test_handle_task_completion_logs_finalize_error_and_keeps_other_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify finalize error logged but other task preserved.

    Given: Task completed but finalize raises error, another task exists.
    When: _handle_task_completion is called with old task.
    Then: Error logged, other task kept in process_tasks.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    completed_task = asyncio.create_task(asyncio.sleep(0))
    await completed_task
    other_task = asyncio.create_task(asyncio.sleep(0))
    await other_task
    factory.process_tasks["job"] = other_task
    factory.started_processes["job"] = object()
    factory.process_lifecycles["job"] = ProcessLifecycleEnum.ONE_SHOT
    factory.process_roles["job"] = ProcessRoleEnum.CORE
    finalize_mock = mock.AsyncMock(side_effect=RuntimeError("finalize-error"))
    monkeypatch.setattr(factory, "_finalize_process_run", finalize_mock)
    await factory._handle_task_completion("job", completed_task)
    assert factory.process_tasks.get("job") is other_task


@pytest.mark.asyncio()
async def test_handle_task_completion_suppresses_cancelled_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify CancelledError during finalize is suppressed.

    Given: Finalize mock raises CancelledError.
    When: _handle_task_completion is called.
    Then: Error suppressed, process removed from tracking.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    task = asyncio.create_task(asyncio.sleep(0))
    await task
    factory.process_tasks["job"] = task
    factory.started_processes["job"] = object()
    factory.process_lifecycles["job"] = ProcessLifecycleEnum.ONE_SHOT
    factory.process_roles["job"] = ProcessRoleEnum.CORE

    class _CaughtCancelledError(Exception):
        pass

    monkeypatch.setattr(
        "snapper.application.process_manager.launcher.asyncio.CancelledError",
        _CaughtCancelledError,
    )
    finalize_mock = mock.AsyncMock(side_effect=_CaughtCancelledError())
    monkeypatch.setattr(factory, "_finalize_process_run", finalize_mock)
    await factory._handle_task_completion("job", task)
    assert "job" not in factory.process_tasks


def test_import_class_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify import_class raises appropriate errors for invalid inputs.

    Given: Registry with invalid class_ref or missing module.
    When: import_class is called.
    Then: Raises TypeError for invalid ref, ImportError for missing module.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    monkeypatch.setattr(
        "snapper.application.process_manager.config_resolver.get_registered_processes",
        lambda: {
            "bad": ProcessRegistryEntry(
                class_ref=object(),
                class_path="",
                method="",
                description="",
                priority=0,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.CORE,
                tags=(),
                parameters_model=None,
                parameters_schema=None,
                enabled=True,
                mode="thread",
            )
        },
    )
    with pytest.raises(TypeError):
        factory.import_class("module.Class", process_name="bad")
    with pytest.raises(ImportError):
        factory.import_class("no.such.module.Missing")


@pytest.mark.asyncio()
async def test_monitor_native_processes_exits_when_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify monitor loop exits when no native processes tracked.

    Given: No ProcessInstanceInfo in started_processes.
    When: _monitor_native_processes is called.
    Then: Loop sleeps once and returns.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    factory.spawner = cast(
        ProcessSpawnerService, mock.create_autospec(ProcessSpawnerService, instance=True)
    )
    sleep_mock = mock.AsyncMock()
    monkeypatch.setattr("snapper.application.process_manager.launcher.asyncio.sleep", sleep_mock)
    await factory._monitor_native_processes()
    sleep_mock.assert_awaited()


@pytest.mark.asyncio()
async def test_monitor_native_processes_skips_running_and_exits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify monitor exits loop when process clears from tracking.

    Given: A native process in started_processes that reports running status,
    When: _monitor_native_processes is called and process clears itself,
    Then: The monitor sleeps once and exits the loop.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    spawner_mock = mock.create_autospec(ProcessSpawnerService, instance=True)
    factory.spawner = cast(ProcessSpawnerService, spawner_mock)
    proc_info = ProcessInstanceInfo(
        name="native",
        pid=3,
        started_at=datetime.now(UTC),
        config={},
        process=cast(subprocess.Popen[bytes], SimpleNamespace(returncode=0)),
    )
    factory.started_processes["native"] = proc_info

    def status_running(_name: str) -> SpawnerStatusSnapshot:
        factory.started_processes.clear()
        return SpawnerStatusSnapshot(name=_name, running=True)

    spawner_mock.get_status.side_effect = status_running
    sleep_calls: list[float] = []

    async def sleep_fake(delay: float) -> None:
        sleep_calls.append(delay)

    monkeypatch.setattr("snapper.application.process_manager.launcher.asyncio.sleep", sleep_fake)
    await factory._monitor_native_processes()
    assert sleep_calls[0] == 5


@pytest.mark.asyncio()
async def test_handle_process_completion_expected_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify expected termination finalizes as CANCELLED status.

    Given: A native process marked in expected_terminations set,
    When: _handle_process_completion is called after process exit,
    Then: Run is finalized with CANCELLED status and process is cleaned up.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    spawner_mock = mock.create_autospec(ProcessSpawnerService, instance=True)
    factory.spawner = cast(ProcessSpawnerService, spawner_mock)
    proc_info = ProcessInstanceInfo(
        name="native",
        pid=123,
        started_at=datetime.now(UTC),
        config={},
        process=cast(subprocess.Popen[bytes], SimpleNamespace(returncode=1)),
    )
    factory.started_processes["native"] = proc_info
    factory.process_lifecycles["native"] = ProcessLifecycleEnum.LONG_RUNNING
    factory.process_roles["native"] = ProcessRoleEnum.CORE
    factory.expected_terminations.add("native")
    finalize_mock = mock.AsyncMock()
    monkeypatch.setattr(factory, "_finalize_process_run", finalize_mock)
    await factory._handle_process_completion("native", proc_info)
    finalize_mock.assert_awaited_once_with(
        "native", ProcessRunStatusEnum.CANCELLED, error=None, exit_code=1
    )
    assert "native" not in factory.started_processes
    spawner_mock.cleanup.assert_called_once_with("native")


@pytest.mark.asyncio()
async def test_handle_process_completion_unexpected_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify unexpected exit finalizes as FAILED with exit code.

    Given: A native process not in expected_terminations with non-zero exit,
    When: _handle_process_completion is called,
    Then: Run is finalized with FAILED status, exit code on the run-event
        payload, AND a stringified ``error="exit_code=N"`` for the DB run
        record (the launcher still folds the code into error to preserve
        the earlier update_run_record contract).
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    spawner_mock = mock.create_autospec(ProcessSpawnerService, instance=True)
    factory.spawner = cast(ProcessSpawnerService, spawner_mock)
    proc_info = ProcessInstanceInfo(
        name="native",
        pid=123,
        started_at=datetime.now(UTC),
        config={},
        process=cast(subprocess.Popen[bytes], SimpleNamespace(returncode=2)),
    )
    factory.started_processes["native"] = proc_info
    factory.process_lifecycles["native"] = ProcessLifecycleEnum.LONG_RUNNING
    factory.process_roles["native"] = ProcessRoleEnum.CORE
    finalize_mock = mock.AsyncMock()
    monkeypatch.setattr(factory, "_finalize_process_run", finalize_mock)
    await factory._handle_process_completion("native", proc_info)
    finalize_mock.assert_awaited_once_with(
        "native", ProcessRunStatusEnum.FAILED, error="exit_code=2", exit_code=2
    )
    assert "native" not in factory.started_processes
    spawner_mock.cleanup.assert_called_once_with("native")


@pytest.mark.asyncio()
async def test_handle_process_completion_logs_finalize_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify finalize error is caught and process still cleaned up.

    Given: A process completion where _finalize_process_run raises exception,
    When: _handle_process_completion is called,
    Then: Error is logged but process is still removed and cleaned up.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    spawner_mock = mock.create_autospec(ProcessSpawnerService, instance=True)
    factory.spawner = cast(ProcessSpawnerService, spawner_mock)
    proc_info = ProcessInstanceInfo(
        name="native",
        pid=9,
        started_at=datetime.now(UTC),
        config={},
        process=cast(subprocess.Popen[bytes], SimpleNamespace(returncode=0)),
    )
    factory.started_processes["native"] = proc_info
    factory.process_lifecycles["native"] = ProcessLifecycleEnum.ONE_SHOT
    factory.process_roles["native"] = ProcessRoleEnum.CORE
    factory.expected_terminations.add("native")
    finalize_mock = mock.AsyncMock(side_effect=RuntimeError("finalize-fail"))
    monkeypatch.setattr(factory, "_finalize_process_run", finalize_mock)
    await factory._handle_process_completion("native", proc_info)
    assert "native" not in factory.started_processes
    spawner_mock.cleanup.assert_called_once_with("native")


@pytest.mark.asyncio()
async def test_start_all_processes_continues_on_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify start_all_processes continues after individual failures.

    Given: Multiple process configs where one raises error during start,
    When: start_all_processes is called,
    Then: Other processes are started and monitoring is initiated.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    cast(Any, factory)._start_native_process_monitoring = mock.Mock()
    config_ok = ProcessConfigModel(
        name="ok",
        enabled=True,
        mode="thread",
        class_path="tests.application.process_manager.test_process_launcher.SyncProcess",
        method="start",
        parameters={},
    )
    config_fail = ProcessConfigModel(
        name="fail",
        enabled=True,
        mode="thread",
        class_path="tests.application.process_manager.test_process_launcher.SyncProcess",
        method="start",
        parameters={},
        role=ProcessRoleEnum.TASK,
    )
    config_disabled = ProcessConfigModel(
        name="disabled",
        enabled=False,
        mode="thread",
        class_path="tests.application.process_manager.test_process_launcher.SyncProcess",
        method="start",
        parameters={},
    )
    monkeypatch.setattr(
        factory,
        "get_process_configs",
        mock.AsyncMock(return_value=[config_ok, config_fail, config_disabled]),
    )
    start_process_mock = mock.AsyncMock(side_effect=[None, RuntimeError("boom")])
    monkeypatch.setattr(factory, "start_process", start_process_mock)
    await factory.start_all_processes()
    assert start_process_mock.await_count == 2
    cast(mock.Mock, factory._start_native_process_monitoring).assert_called_once()


@pytest.mark.asyncio()
async def test_stop_all_processes_cancels_tasks_and_processes() -> None:
    """Verify stop_all_processes cancels tasks and terminates native processes.

    Given: Factory with async tasks and native processes tracked,
    When: stop_all_processes is called,
    Then: Tasks are cancelled, native processes terminated, and tracking cleared.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    spawner_mock = mock.create_autospec(ProcessSpawnerService, instance=True)
    factory.spawner = cast(ProcessSpawnerService, spawner_mock)
    task = asyncio.create_task(asyncio.sleep(1))
    factory.process_tasks["async_task"] = task
    factory.started_processes["async_task"] = object()
    factory.process_lifecycles["async_task"] = ProcessLifecycleEnum.LONG_RUNNING
    factory.process_roles["async_task"] = ProcessRoleEnum.CORE
    proc_info = ProcessInstanceInfo(
        name="native",
        pid=123,
        started_at=datetime.now(UTC),
        config={},
        process=cast(subprocess.Popen[bytes], SimpleNamespace(returncode=0)),
        spawner=spawner_mock,
    )
    factory.started_processes["native"] = proc_info
    factory.process_lifecycles["native"] = ProcessLifecycleEnum.LONG_RUNNING
    factory.process_roles["native"] = ProcessRoleEnum.CORE
    await factory.stop_all_processes()
    assert factory.started_processes == {}
    assert factory.process_tasks == {}
    assert factory.expected_terminations == set()
    spawner_mock.terminate.assert_called_once_with("native")
    spawner_mock.cleanup.assert_called_with("native")


@pytest.mark.asyncio()
async def test_stop_all_processes_handles_done_tasks_and_cleanup_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify stop_all_processes handles done tasks and cleanup errors.

    Given: A completed task and native process with failing cleanup,
    When: stop_all_processes is called,
    Then: Done tasks are skipped, cleanup errors are caught, tracking cleared.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    spawner_mock = mock.create_autospec(ProcessSpawnerService, instance=True)
    spawner_mock.cleanup.side_effect = RuntimeError("cleanup-fail")
    factory.spawner = cast(ProcessSpawnerService, spawner_mock)
    done_task = asyncio.create_task(asyncio.sleep(0))
    await done_task
    factory.process_tasks["done"] = done_task
    proc_info = ProcessInstanceInfo(
        name="native",
        pid=2,
        started_at=datetime.now(UTC),
        config={},
        process=cast(subprocess.Popen[bytes], SimpleNamespace(returncode=0)),
        spawner=spawner_mock,
    )
    factory.started_processes["native"] = proc_info
    await factory.stop_all_processes()
    spawner_mock.terminate.assert_called_once_with("native")
    spawner_mock.cleanup.assert_called_with("native")
    assert factory.started_processes == {}
    assert factory.process_tasks == {}


@pytest.mark.asyncio()
async def test_stop_all_processes_when_nothing_tracked() -> None:
    """Verify stop_all_processes handles empty tracking gracefully.

    Given: Factory with no processes or tasks tracked,
    When: stop_all_processes is called,
    Then: Method completes without error and tracking remains empty.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    factory.spawner = cast(
        ProcessSpawnerService, mock.create_autospec(ProcessSpawnerService, instance=True)
    )
    await factory.stop_all_processes()
    assert factory.expected_terminations == set()
    assert factory.started_processes == {}
    assert factory.process_tasks == {}


@pytest.mark.asyncio()
async def test_get_process_configs_uses_metadata_parameters_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify get_process_configs applies registry parameters_schema.

    Given: A process config without schema and registry with schema defined,
    When: get_process_configs is called,
    Then: Config receives parameters_schema from registry metadata.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    setting = Setting(
        key="process_demo",
        value=json.dumps({"class": "module.Class"}),
        session_id="test-session",
        sequence_id=1,
    )
    repo = _DummyRepository([setting])
    monkeypatch.setattr(
        "snapper.application.process_manager.config_resolver.get_repository", lambda _url: repo
    )
    monkeypatch.setattr(
        "snapper.application.process_manager.config_resolver.get_registered_processes",
        lambda: {
            "demo": ProcessRegistryEntry(
                class_ref=MagicMock(),
                class_path="",
                method="",
                description="",
                priority=0,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.CORE,
                tags=(),
                parameters_model=None,
                parameters_schema={"field": "value"},
                enabled=True,
                mode="thread",
            )
        },
    )
    configs = await factory.get_process_configs()
    assert configs[0].parameters_schema == {"field": "value"}


@pytest.mark.asyncio()
async def test_get_process_configs_preserves_existing_parameters_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify existing parameters_schema is not overwritten by registry.

    Given: A process config with existing schema and registry with different schema,
    When: get_process_configs is called,
    Then: Config keeps its own parameters_schema, registry is ignored.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    setting = Setting(
        key="process_demo",
        value=json.dumps({"class": "module.Class", "parameters_schema": {"own": True}}),
        session_id="test-session",
        sequence_id=1,
    )
    repo = _DummyRepository([setting])
    monkeypatch.setattr(
        "snapper.application.process_manager.config_resolver.get_repository", lambda _url: repo
    )
    monkeypatch.setattr(
        "snapper.application.process_manager.config_resolver.get_registered_processes",
        lambda: {
            "demo": ProcessRegistryEntry(
                class_ref=MagicMock(),
                class_path="",
                method="",
                description="",
                priority=0,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.CORE,
                tags=(),
                parameters_model=None,
                parameters_schema={"other": False},
                enabled=True,
                mode="thread",
            )
        },
    )
    configs = await factory.get_process_configs()
    assert configs[0].parameters_schema == {"own": True}


@pytest.mark.asyncio()
async def test_start_process_process_mode_with_note(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify start_process in process mode spawns subprocess correctly.

    Given: A process config with mode='process' and note field,
    When: start_process is called,
    Then: Spawner.spawn is called and process is tracked by PID.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    _stub_run_tracking(factory)
    _stub_run_tracking(factory)
    spawner_mock = mock.create_autospec(ProcessSpawnerService, instance=True)
    proc_info = ProcessInstanceInfo(
        name="proc",
        pid=99,
        started_at=datetime.now(UTC),
        config={},
        process=cast(subprocess.Popen[bytes], SimpleNamespace(returncode=0)),
    )
    spawner_mock.spawn.return_value = proc_info
    factory.spawner = cast(ProcessSpawnerService, spawner_mock)
    config = ProcessConfigModel(
        name="proc",
        enabled=True,
        mode="process",
        class_path="module.Class",
        method="start",
        parameters={},
        note="remember",
    )
    await factory.start_process(config)
    assert factory.started_processes["proc"].pid == 99


@pytest.mark.asyncio()
async def test_start_process_immediate_async_failure_raises() -> None:
    """Verify immediate async failure raises exception and clears run.

    Given: An async process that fails immediately upon start,
    When: start_process is called,
    Then: RuntimeError is raised and process is not tracked in active_runs.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    _stub_run_tracking(factory)
    tracker: list[str] = []
    config = ProcessConfigModel(
        name="immediate",
        enabled=True,
        mode="thread",
        class_path="tests.application.process_manager.test_process_launcher.ImmediateFailAsyncProcess",
        method="start",
        parameters=cast(JsonObject, {"tracker": tracker}),
    )
    with pytest.raises(RuntimeError, match="boom-immediate"):
        await factory.start_process(config)
    assert "immediate" not in factory.active_runs
    assert tracker == ["fail"]


@pytest.mark.asyncio()
async def test_start_process_rejects_invalid_mode() -> None:
    """Verify start_process raises ValueError for invalid mode.

    Given: A sync process config with mode='sequential' (not a valid ProcessMode),
    When: start_process is called,
    Then: ValueError is raised describing valid modes.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    _stub_run_tracking(factory)
    config = ProcessConfigModel(
        name="sync_non_thread",
        enabled=True,
        mode="sequential",
        class_path="tests.application.process_manager.test_process_launcher.SyncProcess",
        method="start",
        parameters={},
    )
    with pytest.raises(ValueError, match="Invalid mode 'sequential'"):
        await factory.start_process(config)


@pytest.mark.asyncio()
async def test_register_task_completion_handles_returned_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify task completion handler chains returned task objects.

    Given: A task that returns another task as its result,
    When: _register_task_completion callback fires,
    Then: Returned task is tracked and completion handler called for it.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    completion_mock = mock.AsyncMock()
    monkeypatch.setattr(factory, "_handle_task_completion", completion_mock)

    async def inner() -> str:
        return "done"

    async def outer() -> asyncio.Task[str]:
        return asyncio.create_task(inner())

    outer_task = asyncio.create_task(outer())
    factory.process_tasks["chain"] = outer_task
    factory._register_task_completion("chain", outer_task)
    await outer_task
    await asyncio.sleep(0)
    chained = factory.process_tasks["chain"]
    assert isinstance(chained, asyncio.Task)
    await chained
    await asyncio.sleep(0.01)
    assert completion_mock.await_count >= 1


@pytest.mark.asyncio()
async def test_register_task_completion_handles_returned_coroutine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify task completion handler chains returned coroutine objects.

    Given: A task that returns a coroutine (not wrapped in task) as result,
    When: _register_task_completion callback fires,
    Then: Coroutine is wrapped in task, tracked, and completion handler called.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    completion_mock = mock.AsyncMock()
    monkeypatch.setattr(factory, "_handle_task_completion", completion_mock)

    async def inner() -> str:
        return "ok"

    async def outer() -> Any:
        return inner()

    outer_task = asyncio.create_task(outer())
    factory.process_tasks["coroutine_chain"] = outer_task
    factory._register_task_completion("coroutine_chain", outer_task)
    await outer_task
    await asyncio.sleep(0)
    chained = factory.process_tasks["coroutine_chain"]
    assert isinstance(chained, asyncio.Task)
    await chained
    await asyncio.sleep(0)
    assert completion_mock.await_count >= 1


@pytest.mark.asyncio()
async def test_monitor_native_processes_handles_cancelled(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify monitor exits cleanly on CancelledError.

    Given: A native process being monitored and sleep raises CancelledError,
    When: _monitor_native_processes is called,
    Then: Monitor exits without adding itself to process_tasks.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    factory.spawner = cast(
        ProcessSpawnerService, mock.create_autospec(ProcessSpawnerService, instance=True)
    )
    proc_info = ProcessInstanceInfo(
        name="native",
        pid=1,
        started_at=datetime.now(UTC),
        config={},
        process=cast(subprocess.Popen[bytes], SimpleNamespace(returncode=0)),
    )
    factory.started_processes["native"] = proc_info

    async def sleep_cancel(_delay: float) -> None:
        raise asyncio.CancelledError()

    monkeypatch.setattr("snapper.application.process_manager.launcher.asyncio.sleep", sleep_cancel)
    with pytest.raises(asyncio.CancelledError):
        await factory._monitor_native_processes()
    assert "_native_monitor" not in factory.process_tasks


@pytest.mark.asyncio()
async def test_monitor_native_processes_logs_error_and_recovers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify monitor recovers from errors with extended sleep interval.

    Given: A native process where get_status raises RuntimeError,
    When: _monitor_native_processes is called,
    Then: Error is caught, extended sleep (10s) occurs, loop continues.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    spawner_mock = mock.create_autospec(ProcessSpawnerService, instance=True)
    factory.spawner = cast(ProcessSpawnerService, spawner_mock)
    proc_info = ProcessInstanceInfo(
        name="native",
        pid=1,
        started_at=datetime.now(UTC),
        config={},
        process=cast(subprocess.Popen[bytes], SimpleNamespace(returncode=0)),
    )
    factory.started_processes["native"] = proc_info

    def status_side_effect(_name: str) -> SpawnerStatusSnapshot:
        factory.started_processes.clear()
        raise RuntimeError("broken")

    spawner_mock.get_status.side_effect = status_side_effect
    sleep_calls: list[float] = []

    async def sleep_fake(delay: float) -> None:
        sleep_calls.append(delay)
        if delay == 10:
            raise asyncio.CancelledError()

    monkeypatch.setattr("snapper.application.process_manager.launcher.asyncio.sleep", sleep_fake)
    with contextlib.suppress(asyncio.CancelledError):
        await factory._monitor_native_processes()
    assert 10 in sleep_calls


@pytest.mark.asyncio()
async def test_handle_process_completion_warns_for_long_running_non_native(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify long-running process completion finalizes with success.

    Given: A long-running process with attached spawner that completes,
    When: _handle_process_completion is called,
    Then: Run is finalized with SUCCEEDED status and process removed.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    factory.spawner = cast(
        ProcessSpawnerService, mock.create_autospec(ProcessSpawnerService, instance=True)
    )
    proc_info = ProcessInstanceInfo(
        name="job",
        pid=1,
        started_at=datetime.now(UTC),
        config={},
        process=cast(subprocess.Popen[bytes], SimpleNamespace(returncode=0)),
        spawner=factory.spawner,
    )
    factory.started_processes["job"] = proc_info
    factory.process_lifecycles["job"] = ProcessLifecycleEnum.LONG_RUNNING
    finalize_mock = mock.AsyncMock()
    monkeypatch.setattr(factory, "_finalize_process_run", finalize_mock)
    await factory._handle_process_completion("job", proc_info)
    finalize_mock.assert_awaited_once_with(
        "job", ProcessRunStatusEnum.SUCCEEDED, error=None, exit_code=0
    )
    assert "job" not in factory.started_processes


@pytest.mark.asyncio()
async def test_start_process_by_name_removes_empty_tags_and_updates_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify empty tags resolved to empty tuple and schema preserved.

    Given: A config with empty tags array and existing parameters_schema,
    When: start_process_by_name is called,
    Then: ProcessConfigModel has empty tags and parameters_schema from config.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    mock_start = mock.AsyncMock()
    cast(Any, factory).start_process = mock_start
    config_dict = {
        "enabled": True,
        "mode": "thread",
        "class": "tests.application.process_manager.test_process_launcher.SyncProcess",
        "parameters": {},
        "tags": [],
        "parameters_schema": {"p": 1},
    }
    setting = Setting(
        key="process_clean", value=json.dumps(config_dict), session_id="test-session", sequence_id=1
    )
    repo = _DummyRepository(setting)
    monkeypatch.setattr(
        "snapper.application.process_manager.launcher.get_repository", lambda _url: repo
    )
    monkeypatch.setattr(
        "snapper.application.process_manager.launcher.get_registered_processes", lambda: {}
    )
    result = await factory.start_process_by_name("clean")
    assert result.status == "success"
    call_config = mock_start.call_args[0][0]
    assert call_config.tags == ()
    assert call_config.parameters_schema == {"p": 1}


@pytest.mark.asyncio()
async def test_start_process_by_name_skips_persisting_schema_when_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify parameters_schema is None when not present in config or registry.

    Given: A config without parameters_schema field,
    When: start_process_by_name is called,
    Then: ProcessConfigModel has parameters_schema=None.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    mock_start = mock.AsyncMock()
    cast(Any, factory).start_process = mock_start
    config_dict = {
        "enabled": True,
        "mode": "thread",
        "class": "tests.application.process_manager.test_process_launcher.SyncProcess",
        "parameters": {},
    }
    setting = Setting(
        key="process_plain", value=json.dumps(config_dict), session_id="test-session", sequence_id=1
    )
    repo = _DummyRepository(setting)
    monkeypatch.setattr(
        "snapper.application.process_manager.launcher.get_repository", lambda _url: repo
    )
    monkeypatch.setattr(
        "snapper.application.process_manager.launcher.get_registered_processes", lambda: {}
    )
    result = await factory.start_process_by_name("plain")
    assert result.status == "success"
    call_config = mock_start.call_args[0][0]
    assert call_config.parameters_schema is None


@pytest.mark.asyncio()
async def test_start_process_by_name_removes_stale_tags_without_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify stale tags are cleared when no schema in registry.

    Given: A config with tags but registry has no parameters_schema,
    When: start_process_by_name is called,
    Then: ProcessConfigModel has empty tags tuple.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    mock_start = mock.AsyncMock()
    cast(Any, factory).start_process = mock_start
    config_dict = {
        "enabled": True,
        "mode": "thread",
        "class": "tests.application.process_manager.test_process_launcher.SyncProcess",
        "parameters": {},
        "tags": ["stale"],
    }
    setting = Setting(
        key="process_drop_tags",
        value=json.dumps(config_dict),
        session_id="test-session",
        sequence_id=1,
    )
    repo = _DummyRepository(setting)
    monkeypatch.setattr(
        "snapper.application.process_manager.launcher.get_repository", lambda _url: repo
    )
    monkeypatch.setattr(
        "snapper.application.process_manager.launcher.get_registered_processes", lambda: {}
    )
    result = await factory.start_process_by_name("drop_tags")
    assert result.status == "success"
    call_config = mock_start.call_args[0][0]
    assert call_config.tags == ()


@pytest.mark.asyncio()
async def test_start_process_by_name_drops_metadata_tags_when_schema_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify metadata tags are dropped when schema is absent.

    Given: A config without tags and registry with tags but no schema,
    When: start_process_by_name is called,
    Then: ProcessConfigModel has empty tags tuple.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    mock_start = mock.AsyncMock()
    cast(Any, factory).start_process = mock_start
    config_dict = {
        "enabled": True,
        "mode": "thread",
        "class": "tests.application.process_manager.test_process_launcher.SyncProcess",
        "parameters": {},
    }
    setting = Setting(
        key="process_meta_drop",
        value=json.dumps(config_dict),
        session_id="test-session",
        sequence_id=1,
    )
    repo = _DummyRepository(setting)
    monkeypatch.setattr(
        "snapper.application.process_manager.launcher.get_repository", lambda _url: repo
    )
    monkeypatch.setattr(
        "snapper.application.process_manager.launcher.get_registered_processes",
        lambda: {
            "meta_drop": ProcessRegistryEntry(
                class_ref=MagicMock(),
                class_path="",
                method="",
                description="",
                priority=0,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.CORE,
                tags=("meta",),
                parameters_model=None,
                parameters_schema=None,
                enabled=True,
                mode="thread",
            )
        },
    )
    result = await factory.start_process_by_name("meta_drop")
    assert result.status == "success"
    call_config = mock_start.call_args[0][0]
    assert call_config.tags == ()


@pytest.mark.asyncio()
async def test_stop_process_by_name_cancels_task_and_terminates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify stop cancels task, terminates native process, and cleans up.

    Given: A native process with associated task tracked by factory,
    When: stop_process_by_name is called,
    Then: Task cancelled, spawner terminate/cleanup called, tracking cleared.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    spawner_mock = mock.create_autospec(ProcessSpawnerService, instance=True)
    factory.spawner = cast(ProcessSpawnerService, spawner_mock)
    task = asyncio.create_task(asyncio.sleep(0.05))
    factory.process_tasks["native"] = task
    proc_info = ProcessInstanceInfo(
        name="native",
        pid=5,
        started_at=datetime.now(UTC),
        config={},
        process=cast(subprocess.Popen[bytes], SimpleNamespace(returncode=0)),
        spawner=spawner_mock,
    )
    factory.started_processes["native"] = proc_info
    setting = Setting(
        key="process_native",
        value=json.dumps({"class": "x", "enabled": True, "parameters": {}}),
        session_id="test-session",
        sequence_id=1,
    )
    repo = _DummyRepository(setting)
    monkeypatch.setattr(
        "snapper.application.process_manager.launcher.get_repository", lambda _url: repo
    )
    monkeypatch.setattr(factory, "_finalize_process_run", mock.AsyncMock())
    result = await factory.stop_process_by_name("native")
    assert result.status == "success"
    spawner_mock.terminate.assert_called_once_with("native")
    spawner_mock.cleanup.assert_called_once_with("native")
    assert "native" not in factory.process_tasks


@pytest.mark.asyncio()
async def test_get_process_status_handles_get_status_error() -> None:
    """Verify get_process_status handles get_status method errors gracefully.

    Given: A process instance whose get_status raises RuntimeError,
    When: get_process_status is called,
    Then: Status shows running=True with active_run_id, no exception raised.
    """

    class _BadStatus:
        def get_status(self) -> None:
            raise RuntimeError("boom")

    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    factory.started_processes["bad"] = _BadStatus()
    factory.process_roles["bad"] = ProcessRoleEnum.CORE
    factory.process_lifecycles["bad"] = ProcessLifecycleEnum.LONG_RUNNING
    factory.active_runs["bad"] = "run-id"
    status = await factory.get_process_status("bad")
    assert status.running is True
    assert status.active_public_id == "run-id"


@pytest.mark.asyncio()
async def test_get_recent_runs_with_filter(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify get_recent_runs filters by process name.

    Given: A repository with process runs for specific process name,
    When: get_recent_runs is called with name filter,
    Then: Only matching runs are returned in result list.
    """
    now = datetime.now(UTC)
    runs = [
        SimpleNamespace(
            public_id="1",
            session_id="test-sid",
            sequence_id=1,
            timestamp=now,
            process_name="demo",
            status="ok",
            role="core",
            lifecycle="long_running",
            parameters=None,
            result=None,
            error=None,
            tags=["t"],
            started_at=now,
            completed_at=None,
        )
    ]
    repo = _RunsRepository(runs)
    monkeypatch.setattr(
        "snapper.application.process_manager.run_recorder.get_repository", lambda _url: repo
    )
    factory = ProcessLauncherService(_create_settings())
    result = await factory.get_recent_runs(name="demo")
    assert result[0]["process_name"] == "demo"


class _RegistryClass:
    """Test class for registry operations."""

    @classmethod
    def get_default_parameters(cls, _settings: Any) -> dict[str, Any]:
        return {"default": True}


@pytest.mark.asyncio()
async def test_sync_registry_creates_missing_configs(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify sync creates config for registry entry missing from database.

    Given: A registered process not present in database settings,
    When: sync_registry_to_database is called,
    Then: Config is created with default parameters and schema from registry.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    create_mock = mock.AsyncMock()
    monkeypatch.setattr(factory._registry_syncer, "_create_process_config_in_db", create_mock)
    entry = ProcessRegistryEntry(
        class_ref=_RegistryClass,
        class_path="module.Class",
        method="start",
        description="",
        priority=0,
        lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
        role=ProcessRoleEnum.CORE,
        tags=("x",),
        parameters_model=None,
        parameters_schema={"schema": True},
        enabled=True,
        mode="thread",
    )
    monkeypatch.setattr(
        "snapper.application.process_manager.registry_syncer.get_registered_processes",
        lambda: {"new_proc": entry},
    )
    monkeypatch.setattr(
        "snapper.application.process_manager.registry_syncer.get_repository",
        lambda _url: _DummyRepository(None),
    )
    await factory.sync_registry_to_database()
    defaults_arg = create_mock.call_args.kwargs["defaults"]
    assert defaults_arg["parameters"] == {"default": True}
    assert defaults_arg["parameters_schema"] == {"schema": True}
    create_mock.assert_awaited_once()


class _RegistryClassFailingKwargs:
    """Test class that fails when getting default parameters."""

    @classmethod
    def get_default_parameters(cls, _settings: Any) -> dict[str, Any]:
        raise RuntimeError("nope")


@pytest.mark.asyncio()
async def test_sync_registry_creates_missing_configs_even_when_parameters_fail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify sync creates config even when get_default_parameters fails.

    Given: A registered process whose get_default_parameters raises exception,
    When: sync_registry_to_database is called,
    Then: Config is created with empty parameters instead of failing.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    create_mock = mock.AsyncMock()
    monkeypatch.setattr(factory._registry_syncer, "_create_process_config_in_db", create_mock)
    entry = ProcessRegistryEntry(
        class_ref=_RegistryClassFailingKwargs,
        class_path="module.Class",
        method="start",
        description="",
        priority=0,
        lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
        role=ProcessRoleEnum.CORE,
        tags=(),
        parameters_model=None,
        parameters_schema=None,
        enabled=True,
        mode="thread",
    )
    monkeypatch.setattr(
        "snapper.application.process_manager.registry_syncer.get_registered_processes",
        lambda: {"new_proc": entry},
    )
    monkeypatch.setattr(
        "snapper.application.process_manager.registry_syncer.get_repository",
        lambda _url: _DummyRepository(None),
    )
    await factory.sync_registry_to_database()
    defaults_arg = create_mock.call_args.kwargs["defaults"]
    assert defaults_arg["parameters"] == {}


class _RegistryNoKwargs:
    """Test class without get_default_parameters method."""

    pass


@pytest.mark.asyncio()
async def test_sync_registry_skips_update_when_no_changes(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify sync does not commit when config matches registry.

    Given: An existing config that matches registry metadata exactly,
    When: sync_registry_to_database is called,
    Then: No changes are made and session is not committed.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    existing_setting = Setting(
        key="process_existing",
        value=json.dumps(
            {
                "enabled": False,
                "mode": "thread",
                "class": "module.Class",
                "method": "start",
                "parameters": {},
                "lifecycle": ProcessLifecycleEnum.LONG_RUNNING.value,
                "role": ProcessRoleEnum.CORE.value,
            }
        ),
        session_id="test-session",
        sequence_id=1,
    )
    repo = _DummyRepository(existing_setting)
    monkeypatch.setattr(
        "snapper.application.process_manager.registry_syncer.get_repository", lambda _url: repo
    )
    entry = ProcessRegistryEntry(
        class_ref=_RegistryNoKwargs,
        class_path="module.Class",
        method="start",
        description="",
        priority=0,
        lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
        role=ProcessRoleEnum.CORE,
        tags=(),
        parameters_model=None,
        parameters_schema=None,
        enabled=True,
        mode="thread",
    )
    monkeypatch.setattr(
        "snapper.application.process_manager.registry_syncer.get_registered_processes",
        lambda: {"existing": entry},
    )
    await factory.sync_registry_to_database()
    updated_setting = cast(Setting, repo.setting)
    assert json.loads(updated_setting.value)["parameters"] == {}


class _RegistryClassNoKwargs:
    """Test class with default parameters returning filled value."""

    @classmethod
    def get_default_parameters(cls, _settings: Any) -> dict[str, Any]:
        return {"filled": True}


@pytest.mark.asyncio()
async def test_sync_registry_updates_existing_config(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify sync updates existing config with registry metadata.

    Given: An existing config with different lifecycle and missing tags/schema,
    When: sync_registry_to_database is called,
    Then: Config is updated with parameters, lifecycle, tags, and schema.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    existing_setting = Setting(
        key="process_existing",
        value=json.dumps(
            {
                "enabled": False,
                "mode": "thread",
                "class": "module.Class",
                "method": "start",
                "parameters": {},
                "lifecycle": ProcessLifecycleEnum.LONG_RUNNING.value,
                "role": ProcessRoleEnum.CORE.value,
            }
        ),
        session_id="test-session",
        sequence_id=1,
    )
    repo = _DummyRepository(existing_setting)
    monkeypatch.setattr(
        "snapper.application.process_manager.registry_syncer.get_repository", lambda _url: repo
    )
    entry = ProcessRegistryEntry(
        class_ref=_RegistryClassNoKwargs,
        class_path="module.Class",
        method="start",
        description="",
        priority=0,
        lifecycle=ProcessLifecycleEnum.ONE_SHOT,
        role=ProcessRoleEnum.CORE,
        tags=("a",),
        parameters_model=None,
        parameters_schema={"shape": "x"},
        enabled=True,
        mode="thread",
    )
    monkeypatch.setattr(
        "snapper.application.process_manager.registry_syncer.get_registered_processes",
        lambda: {"existing": entry},
    )
    await factory.sync_registry_to_database()
    assert repo.last_session is not None
    new_row = repo.last_session.added[-1]
    updated = json.loads(new_row.value)
    assert updated["parameters"] == {"filled": True}
    assert updated["lifecycle"] == ProcessLifecycleEnum.ONE_SHOT.value
    assert updated["tags"] == ["a"]
    assert updated["parameters_schema"] == {"shape": "x"}


@pytest.mark.asyncio()
async def test_sync_registry_adds_tags_and_schema_when_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify sync adds missing tags and schema from registry.

    Given: An existing config without tags and parameters_schema,
    When: sync_registry_to_database is called with registry containing both,
    Then: Tags and schema are added to config and committed.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    existing_setting = Setting(
        key="process_existing",
        value=json.dumps(
            {
                "enabled": False,
                "mode": "thread",
                "class": "module.Class",
                "method": "start",
                "parameters": {},
                "lifecycle": ProcessLifecycleEnum.LONG_RUNNING.value,
                "role": ProcessRoleEnum.CORE.value,
            }
        ),
        session_id="test-session",
        sequence_id=1,
    )
    repo = _DummyRepository(existing_setting)
    monkeypatch.setattr(
        "snapper.application.process_manager.registry_syncer.get_repository", lambda _url: repo
    )
    entry = ProcessRegistryEntry(
        class_ref=_RegistryClass,
        class_path="module.Class",
        method="start",
        description="",
        priority=0,
        lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
        role=ProcessRoleEnum.CORE,
        tags=("sync",),
        parameters_model=None,
        parameters_schema={"p": 1},
        enabled=True,
        mode="thread",
    )
    monkeypatch.setattr(
        "snapper.application.process_manager.registry_syncer.get_registered_processes",
        lambda: {"existing": entry},
    )
    await factory.sync_registry_to_database()
    assert repo.last_session is not None and repo.last_session.commit_called is True
    new_row = repo.last_session.added[-1]
    updated = json.loads(new_row.value)
    assert updated["tags"] == ["sync"]
    assert updated["parameters_schema"] == {"p": 1}


class _RegistryClassKwargsFailingUpdate:
    """Test class that raises during parameters retrieval."""

    @classmethod
    def get_default_parameters(cls, _settings: Any) -> dict[str, Any]:
        raise RuntimeError("nope")


class _TwoPhaseRepository:
    """Test repository that returns different sessions on subsequent calls."""

    def __init__(self, setting: Setting) -> None:
        self.setting = setting
        self.first_session: _DummySession | None = None
        self.second_session: _DummySession | None = None

    def session(self) -> contextlib.AbstractAsyncContextManager[_DummySession]:
        if self.first_session is None:
            self.first_session = _DummySession(self.setting)
            return self.first_session
        self.second_session = _DummySession(None)
        return self.second_session


@pytest.mark.asyncio()
async def test_sync_registry_update_handles_default_parameters_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify sync handles get_default_parameters failure during update.

    Given: An existing config where registry class get_default_parameters raises,
    When: sync_registry_to_database is called,
    Then: Update uses empty parameters and does not commit invalid state.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    existing_setting = Setting(
        key="process_existing",
        value=json.dumps(
            {
                "enabled": False,
                "mode": "thread",
                "class": "module.Class",
                "method": "start",
                "parameters": {},
                "lifecycle": ProcessLifecycleEnum.LONG_RUNNING.value,
                "role": ProcessRoleEnum.CORE.value,
            }
        ),
        session_id="test-session",
        sequence_id=1,
    )
    repo = _DummyRepository(existing_setting)
    monkeypatch.setattr(
        "snapper.application.process_manager.registry_syncer.get_repository", lambda _url: repo
    )
    entry = ProcessRegistryEntry(
        class_ref=_RegistryClassKwargsFailingUpdate,
        class_path="module.Class",
        method="start",
        description="",
        priority=0,
        lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
        role=ProcessRoleEnum.CORE,
        tags=(),
        parameters_model=None,
        parameters_schema=None,
        enabled=True,
        mode="thread",
    )
    monkeypatch.setattr(
        "snapper.application.process_manager.registry_syncer.get_registered_processes",
        lambda: {"existing": entry},
    )
    await factory.sync_registry_to_database()
    updated_setting = cast(Setting, repo.setting)
    persisted = json.loads(updated_setting.value)
    assert persisted["parameters"] == {}
    assert repo.last_session is not None and repo.last_session.commit_called is False


@pytest.mark.asyncio()
async def test_sync_registry_update_handles_missing_record_on_second_fetch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify sync inserts fresh row when record deleted between fetch and update.

    Given: A config that exists on first fetch but is deleted before update,
    When: sync_registry_to_database is called,
    Then: close_and_insert inserts a fresh row and second session commits.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    existing_setting = Setting(
        key="process_existing",
        value=json.dumps(
            {
                "enabled": False,
                "mode": "thread",
                "class": "module.Class",
                "method": "start",
                "parameters": {},
                "lifecycle": ProcessLifecycleEnum.ONE_SHOT.value,
                "role": ProcessRoleEnum.CORE.value,
            }
        ),
        session_id="test-session",
        sequence_id=1,
    )
    repo = _TwoPhaseRepository(existing_setting)
    monkeypatch.setattr(
        "snapper.application.process_manager.registry_syncer.get_repository", lambda _url: repo
    )
    entry = ProcessRegistryEntry(
        class_ref=_RegistryClassNoKwargs,
        class_path="module.Class",
        method="start",
        description="",
        priority=0,
        lifecycle=ProcessLifecycleEnum.ONE_SHOT,
        role=ProcessRoleEnum.CORE,
        tags=("t",),
        parameters_model=None,
        parameters_schema=None,
        enabled=True,
        mode="thread",
    )
    monkeypatch.setattr(
        "snapper.application.process_manager.registry_syncer.get_registered_processes",
        lambda: {"existing": entry},
    )
    await factory.sync_registry_to_database()
    assert repo.first_session is not None and repo.first_session.commit_called is False
    assert repo.second_session is not None and repo.second_session.commit_called is True


@pytest.mark.asyncio()
async def test_sync_registry_handles_invalid_json(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify sync skips config with invalid JSON value.

    Given: A setting with malformed JSON value,
    When: sync_registry_to_database is called,
    Then: Setting is skipped and value remains unchanged.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    bad_setting = Setting(key="process_bad", value="{", session_id="test-session", sequence_id=1)
    repo = _DummyRepository(bad_setting)
    monkeypatch.setattr(
        "snapper.application.process_manager.registry_syncer.get_repository", lambda _url: repo
    )
    entry = ProcessRegistryEntry(
        class_ref=_RegistryNoKwargs,
        class_path="module.Class",
        method="start",
        description="",
        priority=0,
        lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
        role=ProcessRoleEnum.CORE,
        tags=(),
        parameters_model=None,
        parameters_schema=None,
        enabled=True,
        mode="thread",
    )
    monkeypatch.setattr(
        "snapper.application.process_manager.registry_syncer.get_registered_processes",
        lambda: {"bad": entry},
    )
    await factory.sync_registry_to_database()
    assert cast(Setting, repo.setting).value == "{"


class _RegistryWithTagsAlready:
    """Test class with pre-existing tags for sync testing."""

    pass


@pytest.mark.asyncio()
async def test_sync_registry_skips_tag_update_when_already_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify sync does not update tags when already matching.

    Given: A config with tags that match registry tags exactly,
    When: sync_registry_to_database is called,
    Then: Tags remain unchanged and session is not committed.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    existing_setting = Setting(
        key="process_tagged",
        value=json.dumps(
            {
                "enabled": True,
                "mode": "thread",
                "class": "module.Class",
                "method": "start",
                "parameters": {},
                "tags": ["keep"],
                "lifecycle": ProcessLifecycleEnum.LONG_RUNNING.value,
                "role": ProcessRoleEnum.CORE.value,
            }
        ),
        session_id="test-session",
        sequence_id=1,
    )
    repo = _DummyRepository(existing_setting)
    monkeypatch.setattr(
        "snapper.application.process_manager.registry_syncer.get_repository", lambda _url: repo
    )
    entry = ProcessRegistryEntry(
        class_ref=_RegistryWithTagsAlready,
        class_path="module.Class",
        method="start",
        description="",
        priority=0,
        lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
        role=ProcessRoleEnum.CORE,
        tags=("keep",),
        parameters_model=None,
        parameters_schema=None,
        enabled=True,
        mode="thread",
    )
    monkeypatch.setattr(
        "snapper.application.process_manager.registry_syncer.get_registered_processes",
        lambda: {"tagged": entry},
    )
    await factory.sync_registry_to_database()
    updated_setting = cast(Setting, repo.setting)
    persisted = json.loads(updated_setting.value)
    assert persisted["tags"] == ["keep"]
    assert repo.last_session is not None and repo.last_session.commit_called is False


@pytest.mark.asyncio()
async def test_sync_registry_adds_missing_tags_from_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify sync adds tags from registry when config has none.

    Given: A config without tags and registry with tags defined,
    When: sync_registry_to_database is called,
    Then: Tags from registry are added to persisted config.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    existing_setting = Setting(
        key="process_tagless",
        value=json.dumps(
            {
                "enabled": True,
                "mode": "thread",
                "class": "module.Class",
                "method": "start",
                "parameters": {},
                "lifecycle": ProcessLifecycleEnum.LONG_RUNNING.value,
                "role": ProcessRoleEnum.CORE.value,
            }
        ),
        session_id="test-session",
        sequence_id=1,
    )
    repo = _DummyRepository(existing_setting)
    monkeypatch.setattr(
        "snapper.application.process_manager.registry_syncer.get_repository", lambda _url: repo
    )
    entry = ProcessRegistryEntry(
        class_ref=_RegistryNoKwargs,
        class_path="module.Class",
        method="start",
        description="",
        priority=0,
        lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
        role=ProcessRoleEnum.CORE,
        tags=("new",),
        parameters_model=None,
        parameters_schema=None,
        enabled=True,
        mode="thread",
    )
    monkeypatch.setattr(
        "snapper.application.process_manager.registry_syncer.get_registered_processes",
        lambda: {"tagless": entry},
    )
    await factory.sync_registry_to_database()
    assert repo.last_session is not None
    new_row = repo.last_session.added[-1]
    persisted = json.loads(new_row.value)
    assert persisted["tags"] == ["new"]


@pytest.mark.asyncio()
async def test_create_process_config_in_db_includes_tags_and_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify _create_process_config_in_db persists tags and schema.

    Given: Defaults dict containing tags and parameters_schema,
    When: _create_process_config_in_db is called,
    Then: Setting is added with tags and schema in JSON value.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)

    class _CaptureSession(_RunsSession):
        def __init__(self) -> None:
            super().__init__([])
            self.added_items: list[Any] = []

        async def __aenter__(self) -> _CaptureSession:
            return self

        def add(self, item: Any) -> None:
            self.added_items.append(item)

    class _CaptureRepo:
        def __init__(self) -> None:
            self.session_obj = _CaptureSession()

        def session(self) -> contextlib.AbstractAsyncContextManager[_CaptureSession]:
            return self.session_obj

    repo = _CaptureRepo()
    monkeypatch.setattr(
        "snapper.application.process_manager.registry_syncer.get_repository", lambda _url: repo
    )
    await factory._registry_syncer._create_process_config_in_db(
        name="new",
        class_path="module.Class",
        method="start",
        defaults={
            "enabled": True,
            "mode": "thread",
            "parameters": {"k": 1},
            "lifecycle": ProcessLifecycleEnum.ONE_SHOT,
            "role": ProcessRoleEnum.CORE,
            "tags": ["t"],
            "parameters_schema": {"p": True},
        },
    )
    assert repo.session_obj.committed is True
    assert len(repo.session_obj.added_items) == 1
    added = repo.session_obj.added_items[0]
    assert json.loads(added.value)["tags"] == ["t"]
    assert json.loads(added.value)["parameters_schema"] == {"p": True}


@pytest.mark.asyncio()
async def test_create_process_config_in_db_omits_absent_optional_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify _create_process_config_in_db omits absent optional fields.

    Given: Defaults dict without tags and parameters_schema,
    When: _create_process_config_in_db is called,
    Then: Persisted JSON does not include tags or parameters_schema keys.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)

    class _MinimalSession(_RunsSession):
        def __init__(self) -> None:
            super().__init__([])
            self.added_item: Any | None = None

        async def __aenter__(self) -> _MinimalSession:
            return self

        def add(self, item: Any) -> None:
            self.added_item = item

    class _MinimalRepo:
        def __init__(self) -> None:
            self.session_obj = _MinimalSession()

        def session(self) -> contextlib.AbstractAsyncContextManager[_MinimalSession]:
            return self.session_obj

    repo = _MinimalRepo()
    monkeypatch.setattr(
        "snapper.application.process_manager.registry_syncer.get_repository", lambda _url: repo
    )
    await factory._registry_syncer._create_process_config_in_db(
        name="minimal",
        class_path="module.Class",
        method="start",
        defaults={
            "enabled": True,
            "mode": "thread",
            "parameters": {},
            "lifecycle": ProcessLifecycleEnum.LONG_RUNNING,
            "role": ProcessRoleEnum.CORE,
        },
    )
    saved = json.loads(cast(Setting, repo.session_obj.added_item).value)
    assert "tags" not in saved
    assert "parameters_schema" not in saved


@pytest.mark.asyncio()
async def test_create_process_config_raises_if_exists(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify create_process_config raises ValueError for duplicate name.

    Given: A process config already exists in database,
    When: create_process_config is called with same name,
    Then: ValueError is raised indicating duplicate.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    existing = Setting(key="process_dup", value="{}", session_id="test-session", sequence_id=1)
    repo = _DummyRepository(existing)
    monkeypatch.setattr(
        "snapper.application.process_manager.registry_syncer.get_repository", lambda _url: repo
    )
    with pytest.raises(ValueError):
        await factory.create_process_config(
            name="dup",
            class_path="module.Class",
            method="start",
            enabled=True,
            mode="thread",
            parameters={},
            lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
            role=ProcessRoleEnum.CORE,
            tags=(),
        )


class TestProcessFactoryDatabasePersistence:
    """Test suite for ProcessLauncherService database persistence methods."""

    @pytest.fixture
    def factory(self) -> ProcessLauncherService:
        """Provide a ProcessLauncherService instance with real settings."""
        settings = get_settings()
        return ProcessLauncherService(settings)

    @pytest.fixture
    def sample_config(self) -> ProcessConfigModel:
        """Provide a sample ProcessConfigModel for testing persistence."""
        return ProcessConfigModel(
            name="test_process",
            enabled=True,
            mode="thread",
            class_path="test.module.TestClass",
            method="start",
            parameters={"param": "value"},
            note="Test process",
            lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
            role=ProcessRoleEnum.CORE,
            tags=("test", "coverage"),
            parameters_schema={"type": "object"},
        )

    @patch("snapper.application.process_manager.run_recorder.get_repository")
    async def test_create_process_run_record_success(
        self,
        mock_get_repo: MagicMock,
        factory: ProcessLauncherService,
        sample_config: ProcessConfigModel,
    ) -> None:
        """Verify _create_process_run_record persists run to database.

        Given: A ProcessConfigModel and parameters dict,
        When: _create_process_run_record is called,
        Then: A ProcessRun is added to session with correct attributes and committed.
        """
        mock_repo = MagicMock()
        mock_session = MagicMock()
        mock_session.add = MagicMock()
        mock_session.commit = AsyncMock()
        mock_repo.session.return_value.__aenter__.return_value = mock_session
        mock_get_repo.return_value = mock_repo
        parameters: dict[str, Any] = {"mode": "thread", "parameters": {"param": "value"}}
        run_id = await factory._create_process_run_record(sample_config, parameters)
        assert isinstance(UUID(run_id), UUID)
        mock_session.add.assert_called_once()
        added_run = mock_session.add.call_args[0][0]
        assert isinstance(added_run, ProcessRun)
        assert added_run.public_id == run_id
        assert added_run.process_name == "test_process"
        assert added_run.role == ProcessRoleEnum.CORE.value
        assert added_run.lifecycle == ProcessLifecycleEnum.LONG_RUNNING.value
        assert added_run.status == ProcessRunStatusEnum.RUNNING.value
        assert added_run.parameters == parameters
        assert added_run.tags == ["test", "coverage"]
        assert isinstance(added_run.started_at, datetime)
        mock_session.commit.assert_called_once()

    @patch("snapper.application.process_manager.run_recorder.get_repository")
    async def test_create_process_run_record_with_none_parameters(
        self,
        mock_get_repo: MagicMock,
        factory: ProcessLauncherService,
        sample_config: ProcessConfigModel,
    ) -> None:
        """Verify _create_process_run_record handles None parameters.

        Given: A ProcessConfigModel and None as parameters,
        When: _create_process_run_record is called,
        Then: The ProcessRun is created with parameters set to None.
        """
        mock_repo = MagicMock()
        mock_session = MagicMock()
        mock_session.add = MagicMock()
        mock_session.commit = AsyncMock()
        mock_repo.session.return_value.__aenter__.return_value = mock_session
        mock_get_repo.return_value = mock_repo
        run_id = await factory._create_process_run_record(sample_config, None)
        assert isinstance(UUID(run_id), UUID)
        added_run = mock_session.add.call_args[0][0]
        assert added_run.parameters is None

    @patch("snapper.application.process_manager.run_recorder.get_repository")
    async def test_update_process_run_record_success(
        self,
        mock_get_repo: MagicMock,
        factory: ProcessLauncherService,
    ) -> None:
        """Verify _update_process_run_record close+inserts with result.

        Given: An existing ProcessRun in the database,
        When: _update_process_run_record is called with SUCCEEDED status and result,
        Then: Old row closed via UPDATE, new row added with updated fields.
        """
        mock_repo = MagicMock()
        mock_session = AsyncMock()
        mock_session.add = MagicMock()
        mock_repo.session.return_value.__aenter__.return_value = mock_session
        mock_get_repo.return_value = mock_repo
        mock_process_run = MagicMock()
        mock_process_run.id = 10
        mock_process_run.public_id = "test-run-id-123"
        mock_process_run.process_name = "test"
        mock_process_run.role = "worker"
        mock_process_run.lifecycle = "transient"
        mock_process_run.parameters = None
        mock_process_run.result = None
        mock_process_run.error = None
        mock_process_run.tags = []
        mock_process_run.started_at = datetime(2024, 1, 1, tzinfo=UTC)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_process_run
        mock_session.execute.return_value = mock_result
        result_data: dict[str, Any] = {"output": "success", "metrics": {"count": 42}}
        await factory._update_process_run_record(
            "test-run-id-123",
            ProcessRunStatusEnum.SUCCEEDED,
            result=result_data,
        )
        assert mock_session.execute.call_count == 2
        mock_session.add.assert_called_once()
        mock_session.commit.assert_called_once()

    @patch("snapper.application.process_manager.run_recorder.get_repository")
    async def test_update_process_run_record_with_error(
        self,
        mock_get_repo: MagicMock,
        factory: ProcessLauncherService,
    ) -> None:
        """Verify _update_process_run_record close+inserts with error.

        Given: An existing ProcessRun in the database,
        When: _update_process_run_record is called with FAILED status and error,
        Then: Old row closed, new row added with error field.
        """
        mock_repo = MagicMock()
        mock_session = AsyncMock()
        mock_session.add = MagicMock()
        mock_repo.session.return_value.__aenter__.return_value = mock_session
        mock_get_repo.return_value = mock_repo
        mock_process_run = MagicMock()
        mock_process_run.id = 11
        mock_process_run.public_id = "test-run-id-456"
        mock_process_run.process_name = "test"
        mock_process_run.role = "worker"
        mock_process_run.lifecycle = "transient"
        mock_process_run.parameters = None
        mock_process_run.result = None
        mock_process_run.error = None
        mock_process_run.tags = []
        mock_process_run.started_at = datetime(2024, 1, 1, tzinfo=UTC)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_process_run
        mock_session.execute.return_value = mock_result
        await factory._update_process_run_record(
            "test-run-id-456",
            ProcessRunStatusEnum.FAILED,
            error="Connection failed: timeout after 30s",
        )
        assert mock_session.execute.call_count == 2
        mock_session.add.assert_called_once()
        mock_session.commit.assert_called_once()

    @patch("snapper.application.process_manager.run_recorder.get_repository")
    async def test_update_process_run_record_truncates_long_error(
        self,
        mock_get_repo: MagicMock,
        factory: ProcessLauncherService,
    ) -> None:
        """Verify _update_process_run_record truncates error to 1024 chars.

        Given: An error message longer than 1024 characters,
        When: _update_process_run_record is called with the long error,
        Then: The new row's error is truncated to exactly 1024 characters.
        """
        mock_repo = MagicMock()
        mock_session = AsyncMock()
        mock_session.add = MagicMock()
        mock_repo.session.return_value.__aenter__.return_value = mock_session
        mock_get_repo.return_value = mock_repo
        mock_process_run = MagicMock()
        mock_process_run.id = 12
        mock_process_run.public_id = "test-id"
        mock_process_run.process_name = "test"
        mock_process_run.role = "worker"
        mock_process_run.lifecycle = "transient"
        mock_process_run.parameters = None
        mock_process_run.result = None
        mock_process_run.error = None
        mock_process_run.tags = []
        mock_process_run.started_at = datetime(2024, 1, 1, tzinfo=UTC)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_process_run
        mock_session.execute.return_value = mock_result
        long_error = "X" * 2000
        await factory._update_process_run_record(
            "test-id",
            ProcessRunStatusEnum.FAILED,
            error=long_error,
        )
        added_obj = mock_session.add.call_args[0][0]
        assert len(added_obj.error) == 1024

    @patch("snapper.application.process_manager.run_recorder.get_repository")
    async def test_update_process_run_record_handles_missing_run(
        self,
        mock_get_repo: MagicMock,
        factory: ProcessLauncherService,
    ) -> None:
        """Verify _update_process_run_record handles non-existent run gracefully.

        Given: A run_id that does not exist in the database,
        When: _update_process_run_record is called,
        Then: No commit is performed and no error is raised.
        """
        mock_repo = MagicMock()
        mock_session = AsyncMock()
        mock_repo.session.return_value.__aenter__.return_value = mock_session
        mock_get_repo.return_value = mock_repo
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result
        await factory._update_process_run_record(
            "nonexistent-run-id",
            ProcessRunStatusEnum.SUCCEEDED,
        )
        mock_session.commit.assert_not_called()

    @patch("snapper.application.process_manager.run_recorder.get_repository")
    async def test_finalize_process_run_success(
        self,
        mock_get_repo: MagicMock,
        factory: ProcessLauncherService,
    ) -> None:
        """Verify _finalize_process_run close+inserts and removes from active_runs.

        Given: A process with an active run tracked in factory.active_runs,
        When: _finalize_process_run is called with SUCCEEDED status,
        Then: Old row closed, new row added, run removed from active_runs.
        """
        mock_repo = MagicMock()
        mock_session = AsyncMock()
        mock_session.add = MagicMock()
        mock_repo.session.return_value.__aenter__.return_value = mock_session
        mock_get_repo.return_value = mock_repo
        mock_process_run = MagicMock()
        mock_process_run.id = 13
        mock_process_run.public_id = "run-id-789"
        mock_process_run.process_name = "test_process"
        mock_process_run.role = "worker"
        mock_process_run.lifecycle = "transient"
        mock_process_run.parameters = None
        mock_process_run.result = None
        mock_process_run.error = None
        mock_process_run.tags = []
        mock_process_run.started_at = datetime(2024, 1, 1, tzinfo=UTC)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_process_run
        mock_session.execute.return_value = mock_result
        factory.active_runs["test_process"] = "run-id-789"
        await factory._finalize_process_run(
            "test_process",
            ProcessRunStatusEnum.SUCCEEDED,
            result={"status": "done"},
        )
        assert "test_process" not in factory.active_runs
        mock_session.add.assert_called_once()
        mock_session.commit.assert_called_once()

    @patch("snapper.application.process_manager.run_recorder.get_repository")
    async def test_finalize_process_run_no_active_run(
        self,
        mock_get_repo: MagicMock,
        factory: ProcessLauncherService,
    ) -> None:
        """Verify _finalize_process_run does nothing when no active run exists.

        Given: A process name not present in factory.active_runs,
        When: _finalize_process_run is called,
        Then: No database session is created and method returns silently.
        """
        mock_repo = MagicMock()
        mock_get_repo.return_value = mock_repo
        await factory._finalize_process_run(
            "nonexistent_process",
            ProcessRunStatusEnum.SUCCEEDED,
        )
        mock_repo.session.assert_not_called()


@pytest.mark.asyncio()
@patch("snapper.application.process_manager.launcher.get_registered_processes")
@patch("snapper.application.process_manager.launcher.get_repository")
async def test_start_process_by_name_clears_tags_when_schema_missing(
    mock_get_repo: MagicMock,
    mock_get_registry: MagicMock,
) -> None:
    """Verify tags cleared in ProcessConfigModel when schema is missing.

    Given: A registered process with tags but no parameters_schema,
    When: start_process_by_name is called,
    Then: ProcessConfigModel has empty tags, correct lifecycle/role/mode/parameters.
    """
    factory = ProcessLauncherService(get_settings())
    mock_get_registry.return_value = {
        "test_process": ProcessRegistryEntry(
            class_path="test.module.TestClass",
            class_ref=MagicMock(),
            method="start",
            description="",
            priority=0,
            lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
            role=ProcessRoleEnum.CORE,
            tags=("legacy",),
            parameters_model=None,
            parameters_schema=None,
            enabled=True,
            mode="thread",
        )
    }
    existing_config: dict[str, Any] = {
        "enabled": True,
        "mode": "thread",
        "class": "test.module.TestClass",
        "method": "start",
        "parameters": {},
        "tags": ["stale"],
    }
    setting = MagicMock()
    setting.value = json.dumps(existing_config)
    select_result = MagicMock()
    select_result.scalar_one_or_none.return_value = setting
    session = AsyncMock()
    session.add = MagicMock()
    session.execute.return_value = select_result
    mock_repo = MagicMock()
    mock_repo.session.return_value.__aenter__.return_value = session
    mock_get_repo.return_value = mock_repo
    mock_start = AsyncMock()
    cast(Any, factory).start_process = mock_start
    cast(Any, factory)._start_native_process_monitoring = MagicMock()
    response = await factory.start_process_by_name("test_process")
    assert response.status == "success"
    call_config = mock_start.call_args[0][0]
    assert call_config.tags == ()
    assert call_config.lifecycle == ProcessLifecycleEnum.LONG_RUNNING
    assert call_config.mode == "thread"
    assert call_config.parameters == {}
    assert call_config.role == ProcessRoleEnum.CORE


@pytest.mark.asyncio()
@patch("snapper.application.process_manager.registry_syncer.get_registered_processes")
@patch("snapper.application.process_manager.registry_syncer.get_repository")
async def test_sync_registry_to_database_adds_missing_tags(
    mock_get_repo: MagicMock,
    mock_get_registry: MagicMock,
) -> None:
    """Verify sync adds tags and schema from registry to existing config.

    Given: An existing config without tags and registry with tags and schema,
    When: sync_registry_to_database is called,
    Then: Tags and schema are added and changes committed to database.
    """
    factory = ProcessLauncherService(get_settings())

    class DummyProcess:
        pass

    mock_get_registry.return_value = {
        "reg_process": ProcessRegistryEntry(
            class_ref=DummyProcess,
            class_path="test.module.RegClass",
            method="start",
            description="",
            priority=0,
            lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
            role=ProcessRoleEnum.CORE,
            tags=("alpha", "beta"),
            parameters_model=None,
            parameters_schema={"type": "object"},
            enabled=True,
            mode="thread",
        )
    }
    existing_value: dict[str, Any] = {
        "enabled": True,
        "mode": "thread",
        "class": "test.module.RegClass",
        "method": "start",
        "parameters": {},
        "lifecycle": ProcessLifecycleEnum.LONG_RUNNING.value,
        "role": ProcessRoleEnum.CORE.value,
    }
    existing_setting = MagicMock()
    existing_setting.value = json.dumps(existing_value)
    existing_setting.timestamp = None
    existing_setting.updated_by = None
    first_result = MagicMock()
    first_result.scalar_one_or_none.return_value = existing_setting
    update_result = MagicMock()
    update_result.scalar_one_or_none.return_value = existing_setting
    first_session = AsyncMock()
    first_session.execute.return_value = first_result
    update_session = AsyncMock()
    update_session.add = MagicMock()
    update_session.execute.return_value = update_result
    update_session.commit = AsyncMock()
    mock_repo = MagicMock()
    mock_repo.session.return_value.__aenter__.side_effect = [first_session, update_session]
    mock_get_repo.return_value = mock_repo
    await factory.sync_registry_to_database()
    update_session.add.assert_called_once()
    new_row = update_session.add.call_args[0][0]
    updated_config = json.loads(new_row.value)
    assert updated_config["tags"] == ["alpha", "beta"]
    assert updated_config["parameters_schema"] == {"type": "object"}
    assert new_row.updated_by == "sync_registry"
    update_session.commit.assert_awaited_once()


class TestProcessFactoryConfigLoading:
    """Test suite for process configuration loading from database."""

    @pytest.fixture
    def factory(self) -> ProcessLauncherService:
        """Provide ProcessLauncherService instance."""
        settings = get_settings()
        return ProcessLauncherService(settings)

    @patch("snapper.application.process_manager.config_resolver.get_registered_processes")
    @patch("snapper.application.process_manager.config_resolver.get_repository")
    async def test_get_process_configs_success(
        self,
        mock_get_repo: MagicMock,
        mock_get_registry: MagicMock,
        factory: ProcessLauncherService,
    ) -> None:
        """Test process config loading parses all fields correctly.

        Given: Database contains valid process configuration with all fields.
        When: get_process_configs is called.
        Then: Returns ProcessConfig with correctly parsed fields.
        """
        mock_registry: dict[str, ProcessRegistryEntry] = {
            "test_process": ProcessRegistryEntry(
                class_ref=MagicMock(),
                class_path="",
                method="",
                description="",
                priority=0,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.CORE,
                tags=("test", "coverage"),
                parameters_model=None,
                parameters_schema={"type": "object"},
                enabled=True,
                mode="thread",
            )
        }
        mock_get_registry.return_value = mock_registry
        mock_repo = MagicMock()
        mock_session = AsyncMock()
        mock_session.add = MagicMock()
        mock_repo.session.return_value.__aenter__.return_value = mock_session
        mock_get_repo.return_value = mock_repo
        mock_setting = MagicMock()
        mock_setting.key = "process_test_process"
        mock_setting.value = """{
            "enabled": true,
            "mode": "thread",
            "class": "test.module.TestClass",
            "method": "start",
            "parameters": {"param": "value"},
            "note": "Test process"
        }"""
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [mock_setting]
        mock_session.execute.return_value = mock_result
        configs = await factory.get_process_configs()
        assert len(configs) == 1
        config = configs[0]
        assert config.name == "test_process"
        assert config.enabled is True
        assert config.mode == "thread"
        assert config.class_path == "test.module.TestClass"
        assert config.method == "start"
        assert config.parameters == {"param": "value"}
        assert config.note == "Test process"
        assert config.lifecycle == ProcessLifecycleEnum.LONG_RUNNING
        assert config.role == ProcessRoleEnum.CORE
        assert config.tags == ("test", "coverage")
        assert config.parameters_schema == {"type": "object"}

    @patch("snapper.application.process_manager.config_resolver.get_registered_processes")
    @patch("snapper.application.process_manager.config_resolver.get_repository")
    async def test_get_process_configs_unknown_lifecycle_defaults(
        self,
        mock_get_repo: MagicMock,
        mock_get_registry: MagicMock,
        factory: ProcessLauncherService,
    ) -> None:
        """Test unknown lifecycle value defaults to LONG_RUNNING.

        Given: Database config has invalid lifecycle value.
        When: get_process_configs is called.
        Then: ProcessConfig uses LONG_RUNNING as default lifecycle.
        """
        mock_get_registry.return_value = {}
        mock_repo = MagicMock()
        mock_session = AsyncMock()
        mock_repo.session.return_value.__aenter__.return_value = mock_session
        mock_get_repo.return_value = mock_repo
        mock_setting = MagicMock()
        mock_setting.key = "process_test"
        mock_setting.value = """{
            "enabled": true,
            "mode": "thread",
            "class": "test.TestClass",
            "lifecycle": "invalid_lifecycle_value"
        }"""
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [mock_setting]
        mock_session.execute.return_value = mock_result
        configs = await factory.get_process_configs()
        assert len(configs) == 1
        assert configs[0].lifecycle == ProcessLifecycleEnum.LONG_RUNNING

    @patch("snapper.application.process_manager.config_resolver.get_registered_processes")
    @patch("snapper.application.process_manager.config_resolver.get_repository")
    async def test_get_process_configs_unknown_role_defaults(
        self,
        mock_get_repo: MagicMock,
        mock_get_registry: MagicMock,
        factory: ProcessLauncherService,
    ) -> None:
        """Test unknown role value defaults to CORE.

        Given: Database config has invalid role value.
        When: get_process_configs is called.
        Then: ProcessConfig uses CORE as default role.
        """
        mock_get_registry.return_value = {}
        mock_repo = MagicMock()
        mock_session = AsyncMock()
        mock_repo.session.return_value.__aenter__.return_value = mock_session
        mock_get_repo.return_value = mock_repo
        mock_setting = MagicMock()
        mock_setting.key = "process_test"
        mock_setting.value = """{
            "enabled": true,
            "mode": "thread",
            "class": "test.TestClass",
            "role": "invalid_role_value"
        }"""
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [mock_setting]
        mock_session.execute.return_value = mock_result
        configs = await factory.get_process_configs()
        assert len(configs) == 1
        assert configs[0].role == ProcessRoleEnum.CORE

    @patch("snapper.application.process_manager.config_resolver.get_registered_processes")
    @patch("snapper.application.process_manager.config_resolver.get_repository")
    async def test_get_process_configs_handles_tags_conversion(
        self,
        mock_get_repo: MagicMock,
        mock_get_registry: MagicMock,
        factory: ProcessLauncherService,
    ) -> None:
        """Test tags list is converted to tuple.

        Given: Database config has tags as JSON array.
        When: get_process_configs is called.
        Then: ProcessConfig.tags is a tuple of strings.
        """
        mock_get_registry.return_value = {}
        mock_repo = MagicMock()
        mock_session = AsyncMock()
        mock_repo.session.return_value.__aenter__.return_value = mock_session
        mock_get_repo.return_value = mock_repo
        mock_setting = MagicMock()
        mock_setting.key = "process_test"
        mock_setting.value = """{
            "enabled": true,
            "mode": "thread",
            "class": "test.TestClass",
            "tags": ["tag1", "tag2", "tag3"]
        }"""
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [mock_setting]
        mock_session.execute.return_value = mock_result
        configs = await factory.get_process_configs()
        assert len(configs) == 1
        assert configs[0].tags == ("tag1", "tag2", "tag3")
        assert isinstance(configs[0].tags, tuple)

    @patch("snapper.application.process_manager.config_resolver.get_registered_processes")
    @patch("snapper.application.process_manager.config_resolver.get_repository")
    async def test_get_process_configs_skips_invalid_json(
        self,
        mock_get_repo: MagicMock,
        mock_get_registry: MagicMock,
        factory: ProcessLauncherService,
    ) -> None:
        """Test invalid JSON configs are skipped gracefully.

        Given: Database contains both valid and invalid JSON configs.
        When: get_process_configs is called.
        Then: Only valid configs are returned, invalid ones skipped.
        """
        mock_get_registry.return_value = {}
        mock_repo = MagicMock()
        mock_session = AsyncMock()
        mock_repo.session.return_value.__aenter__.return_value = mock_session
        mock_get_repo.return_value = mock_repo
        mock_setting_invalid = MagicMock()
        mock_setting_invalid.key = "process_invalid"
        mock_setting_invalid.value = "{ invalid json"
        mock_setting_valid = MagicMock()
        mock_setting_valid.key = "process_valid"
        mock_setting_valid.value = """{
            "enabled": true,
            "mode": "thread",
            "class": "test.TestClass"
        }"""
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [
            mock_setting_invalid,
            mock_setting_valid,
        ]
        mock_session.execute.return_value = mock_result
        configs = await factory.get_process_configs()
        assert len(configs) == 1
        assert configs[0].name == "valid"

    @patch("snapper.application.process_manager.config_resolver.get_registered_processes")
    @patch("snapper.application.process_manager.config_resolver.get_repository")
    async def test_get_process_configs_skips_missing_required_fields(
        self,
        mock_get_repo: MagicMock,
        mock_get_registry: MagicMock,
        factory: ProcessLauncherService,
    ) -> None:
        """Test configs missing required fields are skipped.

        Given: Database config is missing 'class' field.
        When: get_process_configs is called.
        Then: Config is skipped and empty list returned.
        """
        mock_get_registry.return_value = {}
        mock_repo = MagicMock()
        mock_session = AsyncMock()
        mock_repo.session.return_value.__aenter__.return_value = mock_session
        mock_get_repo.return_value = mock_repo
        mock_setting = MagicMock()
        mock_setting.key = "process_incomplete"
        mock_setting.value = """{
            "enabled": true,
            "mode": "thread"
        }"""
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [mock_setting]
        mock_session.execute.return_value = mock_result
        configs = await factory.get_process_configs()
        assert len(configs) == 0


class TestProcessFactoryNativeProcessCompletion:
    """Test suite for native process completion handling."""

    @pytest.fixture
    def factory(self) -> ProcessLauncherService:
        """Provide ProcessLauncherService instance."""
        settings = get_settings()
        return ProcessLauncherService(settings)

    @pytest.fixture
    def mock_process_info(self) -> MagicMock:
        """Provide mock ProcessInstanceInfo with exit code 0."""
        mock_info = MagicMock(spec=ProcessInstanceInfo)
        mock_info.process = MagicMock()
        mock_info.process.returncode = 0
        return mock_info

    @patch("snapper.application.process_manager.run_recorder.get_repository")
    async def test_handle_process_completion_success_exit_code_zero(
        self,
        mock_get_repo: MagicMock,
        factory: ProcessLauncherService,
        mock_process_info: MagicMock,
    ) -> None:
        """Test successful completion with exit code 0 marks run as SUCCEEDED.

        Given: Process completes with exit code 0.
        When: _handle_process_completion is called.
        Then: Process is removed from tracking and run status is SUCCEEDED.
        """
        mock_repo = MagicMock()
        mock_session = MagicMock()
        mock_session.execute = AsyncMock()
        mock_session.commit = AsyncMock()
        mock_session.add = MagicMock()
        mock_repo.session.return_value.__aenter__.return_value = mock_session
        mock_get_repo.return_value = mock_repo
        mock_process_run = MagicMock()
        mock_process_run.id = 20
        mock_process_run.public_id = "run-id-123"
        mock_process_run.process_name = "test_process"
        mock_process_run.role = "core"
        mock_process_run.lifecycle = "one_shot"
        mock_process_run.parameters = None
        mock_process_run.result = None
        mock_process_run.error = None
        mock_process_run.tags = []
        mock_process_run.started_at = datetime(2024, 1, 1, tzinfo=UTC)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_process_run
        mock_session.execute.return_value = mock_result
        factory.started_processes["test_process"] = mock_process_info
        factory.process_lifecycles["test_process"] = ProcessLifecycleEnum.ONE_SHOT
        factory.process_roles["test_process"] = ProcessRoleEnum.CORE
        factory.active_runs["test_process"] = "run-id-123"
        mock_process_info.process.returncode = 0
        await factory._handle_process_completion("test_process", mock_process_info)
        assert "test_process" not in factory.started_processes
        assert "test_process" not in factory.process_lifecycles
        assert "test_process" not in factory.process_roles
        assert "test_process" not in factory.active_runs
        mock_session.add.assert_called_once()

    @patch("snapper.application.process_manager.run_recorder.get_repository")
    async def test_handle_process_completion_failure_exit_code_nonzero(
        self,
        mock_get_repo: MagicMock,
        factory: ProcessLauncherService,
        mock_process_info: MagicMock,
    ) -> None:
        """Test non-zero exit code marks run as FAILED with error message.

        Given: Process completes with exit code 1.
        When: _handle_process_completion is called.
        Then: Run status is FAILED and error contains exit code.
        """
        mock_repo = MagicMock()
        mock_session = MagicMock()
        mock_session.execute = AsyncMock()
        mock_session.commit = AsyncMock()
        mock_session.add = MagicMock()
        mock_repo.session.return_value.__aenter__.return_value = mock_session
        mock_get_repo.return_value = mock_repo
        mock_process_run = MagicMock()
        mock_process_run.id = 21
        mock_process_run.public_id = "run-id-456"
        mock_process_run.process_name = "test_process"
        mock_process_run.role = "core"
        mock_process_run.lifecycle = "long_running"
        mock_process_run.parameters = None
        mock_process_run.result = None
        mock_process_run.error = None
        mock_process_run.tags = []
        mock_process_run.started_at = datetime(2024, 1, 1, tzinfo=UTC)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_process_run
        mock_session.execute.return_value = mock_result
        factory.started_processes["test_process"] = mock_process_info
        factory.process_lifecycles["test_process"] = ProcessLifecycleEnum.LONG_RUNNING
        factory.process_roles["test_process"] = ProcessRoleEnum.CORE
        factory.active_runs["test_process"] = "run-id-456"
        mock_process_info.process.returncode = 1
        await factory._handle_process_completion("test_process", mock_process_info)
        assert "test_process" not in factory.started_processes
        mock_session.add.assert_called_once()

    @patch("snapper.application.process_manager.run_recorder.get_repository")
    async def test_handle_process_completion_expected_termination(
        self,
        mock_get_repo: MagicMock,
        factory: ProcessLauncherService,
        mock_process_info: MagicMock,
    ) -> None:
        """Test expected termination marks run as CANCELLED.

        Given: Process is in expected_terminations set.
        When: _handle_process_completion is called.
        Then: Run status is CANCELLED and process removed from set.
        """
        mock_repo = MagicMock()
        mock_session = MagicMock()
        mock_session.execute = AsyncMock()
        mock_session.commit = AsyncMock()
        mock_session.add = MagicMock()
        mock_repo.session.return_value.__aenter__.return_value = mock_session
        mock_get_repo.return_value = mock_repo
        mock_process_run = MagicMock()
        mock_process_run.id = 22
        mock_process_run.public_id = "run-id-789"
        mock_process_run.process_name = "test_process"
        mock_process_run.role = "core"
        mock_process_run.lifecycle = "long_running"
        mock_process_run.parameters = None
        mock_process_run.result = None
        mock_process_run.error = None
        mock_process_run.tags = []
        mock_process_run.started_at = datetime(2024, 1, 1, tzinfo=UTC)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_process_run
        mock_session.execute.return_value = mock_result
        factory.started_processes["test_process"] = mock_process_info
        factory.process_lifecycles["test_process"] = ProcessLifecycleEnum.LONG_RUNNING
        factory.expected_terminations.add("test_process")
        factory.active_runs["test_process"] = "run-id-789"
        mock_process_info.process.returncode = 0
        await factory._handle_process_completion("test_process", mock_process_info)
        assert "test_process" not in factory.expected_terminations
        mock_session.add.assert_called_once()

    @patch("snapper.application.process_manager.run_recorder.get_repository")
    async def test_handle_process_completion_long_running_unexpected_exit(
        self,
        mock_get_repo: MagicMock,
        factory: ProcessLauncherService,
        mock_process_info: MagicMock,
    ) -> None:
        """Test long-running process unexpected exit with code 0.

        Given: Long-running process exits unexpectedly with code 0.
        When: _handle_process_completion is called.
        Then: Run status is SUCCEEDED.
        """
        mock_repo = MagicMock()
        mock_session = MagicMock()
        mock_session.execute = AsyncMock()
        mock_session.commit = AsyncMock()
        mock_session.add = MagicMock()
        mock_repo.session.return_value.__aenter__.return_value = mock_session
        mock_get_repo.return_value = mock_repo
        mock_process_run = MagicMock()
        mock_process_run.id = 23
        mock_process_run.public_id = "run-id-999"
        mock_process_run.process_name = "test_process"
        mock_process_run.role = "core"
        mock_process_run.lifecycle = "long_running"
        mock_process_run.parameters = None
        mock_process_run.result = None
        mock_process_run.error = None
        mock_process_run.tags = []
        mock_process_run.started_at = datetime(2024, 1, 1, tzinfo=UTC)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_process_run
        mock_session.execute.return_value = mock_result
        factory.started_processes["test_process"] = mock_process_info
        factory.process_lifecycles["test_process"] = ProcessLifecycleEnum.LONG_RUNNING
        factory.active_runs["test_process"] = "run-id-999"
        mock_process_info.process.returncode = 0
        await factory._handle_process_completion("test_process", mock_process_info)
        mock_session.add.assert_called_once()

    @patch("snapper.application.process_manager.run_recorder.get_repository")
    async def test_handle_process_completion_non_processinfo_object(
        self,
        mock_get_repo: MagicMock,
        factory: ProcessLauncherService,
    ) -> None:
        """Test completion handling for non-ProcessInstanceInfo objects.

        Given: Started process is a plain object, not ProcessInstanceInfo.
        When: _handle_process_completion is called.
        Then: Process is cleaned up from tracking dictionaries.
        """
        mock_repo = MagicMock()
        mock_session = MagicMock()
        mock_session.execute = AsyncMock()
        mock_session.commit = AsyncMock()
        mock_repo.session.return_value.__aenter__.return_value = mock_session
        mock_get_repo.return_value = mock_repo
        plain_object = object()
        factory.started_processes["test_process"] = plain_object
        factory.process_lifecycles["test_process"] = ProcessLifecycleEnum.ONE_SHOT
        factory.active_runs["test_process"] = "run-id-abc"
        await factory._handle_process_completion("test_process", plain_object)
        assert "test_process" not in factory.started_processes
        assert "test_process" not in factory.process_lifecycles

    @patch("snapper.application.process_manager.run_recorder.get_repository")
    async def test_handle_process_completion_cleanup_exception_suppressed(
        self,
        mock_get_repo: MagicMock,
        factory: ProcessLauncherService,
        mock_process_info: MagicMock,
    ) -> None:
        """Test cleanup exceptions are suppressed during completion.

        Given: Spawner cleanup raises RuntimeError.
        When: _handle_process_completion is called.
        Then: Exception is suppressed and process is removed from tracking.
        """
        mock_repo = MagicMock()
        mock_session = MagicMock()
        mock_session.execute = AsyncMock()
        mock_session.commit = AsyncMock()
        mock_repo.session.return_value.__aenter__.return_value = mock_session
        mock_get_repo.return_value = mock_repo
        mock_process_run = MagicMock(spec=ProcessRun)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_process_run
        mock_session.execute.return_value = mock_result
        factory.started_processes["test_process"] = mock_process_info
        factory.active_runs["test_process"] = "run-id-def"
        mock_cleanup = MagicMock(side_effect=RuntimeError("Cleanup failed"))
        factory.spawner.cleanup = mock_cleanup
        mock_process_info.process.returncode = 0
        await factory._handle_process_completion("test_process", mock_process_info)
        mock_cleanup.assert_called_once_with("test_process")
        assert "test_process" not in factory.started_processes


class TestProcessFactoryRegistrySync:
    """Test suite for registry synchronization to database."""

    @pytest.fixture
    def factory(self) -> ProcessLauncherService:
        """Provide ProcessLauncherService instance."""
        settings = get_settings()
        return ProcessLauncherService(settings)

    @pytest.fixture
    def mock_process_class(self) -> type:
        """Provide mock process class with get_default_parameters method."""

        class MockProcess:
            @staticmethod
            def get_default_parameters(settings: Any) -> dict[str, Any]:
                return {"param1": "value1", "param2": 42}

        return MockProcess

    @patch("snapper.application.process_manager.registry_syncer.get_registered_processes")
    @patch("snapper.application.process_manager.registry_syncer.get_repository")
    async def test_sync_registry_creates_new_config(
        self,
        mock_get_repo: MagicMock,
        mock_get_registry: MagicMock,
        factory: ProcessLauncherService,
        mock_process_class: type,
    ) -> None:
        """Test sync creates new database config for registered process.

        Given: Registry contains process not in database.
        When: sync_registry_to_database is called.
        Then: New setting is created with all fields from registry.
        """
        mock_registry: dict[str, ProcessRegistryEntry] = {
            "new_process": ProcessRegistryEntry(
                class_ref=mock_process_class,
                class_path="test.module.MockProcess",
                method="start",
                description="",
                priority=0,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.CORE,
                tags=("test", "new"),
                parameters_model=None,
                parameters_schema={"type": "object"},
                enabled=True,
                mode="thread",
            )
        }
        mock_get_registry.return_value = mock_registry
        mock_repo = MagicMock()
        mock_session = MagicMock()
        mock_session.add = MagicMock()
        mock_session.commit = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None

        async def execute_stub(*args: Any, **kwargs: Any) -> Any:
            return mock_result

        mock_session.execute = execute_stub

        @asynccontextmanager
        async def session_cm() -> AsyncIterator[MagicMock]:
            yield mock_session

        mock_repo.session.side_effect = session_cm
        mock_get_repo.return_value = mock_repo
        await factory.sync_registry_to_database()
        mock_session.add.assert_called_once()
        added_setting = mock_session.add.call_args[0][0]
        assert added_setting.key == "process_new_process"
        config_dict = json.loads(added_setting.value)
        assert config_dict["enabled"] is True
        assert config_dict["class"] == "test.module.MockProcess"
        assert config_dict["method"] == "start"
        assert config_dict["parameters"] == {"param1": "value1", "param2": 42}
        assert config_dict["lifecycle"] == ProcessLifecycleEnum.LONG_RUNNING.value
        assert config_dict["role"] == ProcessRoleEnum.CORE.value
        assert config_dict["tags"] == ["test", "new"]
        assert config_dict["parameters_schema"] == {"type": "object"}

    @patch("snapper.application.process_manager.registry_syncer.get_registered_processes")
    @patch("snapper.application.process_manager.registry_syncer.get_repository")
    async def test_sync_registry_updates_empty_parameters(
        self,
        mock_get_repo: MagicMock,
        mock_get_registry: MagicMock,
        factory: ProcessLauncherService,
        mock_process_class: type,
    ) -> None:
        """Test sync updates existing config with empty parameters.

        Given: Database config has empty parameters dict.
        When: sync_registry_to_database is called.
        Then: Config is updated with default parameters from process class.
        """
        mock_registry: dict[str, ProcessRegistryEntry] = {
            "existing_process": ProcessRegistryEntry(
                class_ref=mock_process_class,
                class_path="",
                method="",
                description="",
                priority=0,
                lifecycle=ProcessLifecycleEnum.ONE_SHOT,
                role=ProcessRoleEnum.BACKTEST,
                tags=("updated",),
                parameters_model=None,
                parameters_schema=None,
                enabled=True,
                mode="thread",
            )
        }
        mock_get_registry.return_value = mock_registry
        mock_repo = MagicMock()
        existing_setting = MagicMock()
        existing_setting.key = "process_existing_process"
        existing_setting.value = json.dumps(
            {
                "enabled": True,
                "mode": "thread",
                "class": "test.module.MockProcess",
                "parameters": {},
                "lifecycle": "long_running",
                "role": "core",
            }
        )
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = existing_setting
        mock_session = AsyncMock()
        mock_session.add = MagicMock()
        mock_session.execute.return_value = mock_result
        mock_session.commit = AsyncMock()
        mock_repo.session.return_value.__aenter__.return_value = mock_session
        mock_get_repo.return_value = mock_repo
        await factory.sync_registry_to_database()
        mock_session.add.assert_called_once()
        new_row = mock_session.add.call_args[0][0]
        updated_config = json.loads(new_row.value)
        assert updated_config["parameters"] == {"param1": "value1", "param2": 42}
        assert updated_config["lifecycle"] == ProcessLifecycleEnum.ONE_SHOT.value
        assert updated_config["role"] == ProcessRoleEnum.BACKTEST.value
        mock_session.commit.assert_called()

    @patch("snapper.application.process_manager.registry_syncer.get_registered_processes")
    @patch("snapper.application.process_manager.registry_syncer.get_repository")
    async def test_sync_registry_skips_config_with_existing_parameters(
        self,
        mock_get_repo: MagicMock,
        mock_get_registry: MagicMock,
        factory: ProcessLauncherService,
    ) -> None:
        """Test sync skips config that already has parameters.

        Given: Database config has non-empty parameters.
        When: sync_registry_to_database is called.
        Then: Existing parameters are preserved, not overwritten.
        """
        mock_registry: dict[str, ProcessRegistryEntry] = {
            "process_with_kwargs": ProcessRegistryEntry(
                class_ref=type("DummyClass", (), {}),
                class_path="",
                method="",
                description="",
                priority=0,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.CORE,
                tags=(),
                parameters_model=None,
                parameters_schema=None,
                enabled=True,
                mode="thread",
            )
        }
        mock_get_registry.return_value = mock_registry
        mock_repo = MagicMock()
        existing_setting = MagicMock()
        existing_setting.value = json.dumps(
            {
                "enabled": True,
                "mode": "thread",
                "class": "test.Class",
                "parameters": {"existing": "value"},
                "lifecycle": "long_running",
                "role": "core",
            }
        )
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = existing_setting
        mock_session = MagicMock()
        mock_session.execute = AsyncMock(return_value=mock_result)
        mock_session.commit = AsyncMock()
        mock_repo.session.return_value.__aenter__.return_value = mock_session
        mock_get_repo.return_value = mock_repo
        await factory.sync_registry_to_database()
        original_config = json.loads(existing_setting.value)
        assert original_config["parameters"] == {"existing": "value"}

    @patch("snapper.application.process_manager.registry_syncer.get_registered_processes")
    @patch("snapper.application.process_manager.registry_syncer.get_repository")
    async def test_sync_registry_handles_json_decode_error(
        self,
        mock_get_repo: MagicMock,
        mock_get_registry: MagicMock,
        factory: ProcessLauncherService,
    ) -> None:
        """Test sync handles corrupted JSON in existing config.

        Given: Database config contains invalid JSON.
        When: sync_registry_to_database is called.
        Then: Error is handled gracefully without raising exception.
        """
        mock_registry: dict[str, ProcessRegistryEntry] = {
            "corrupted_process": ProcessRegistryEntry(
                class_ref=type("TestClass", (), {}),
                class_path="",
                method="",
                description="",
                priority=0,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.CORE,
                tags=(),
                parameters_model=None,
                parameters_schema=None,
                enabled=True,
                mode="thread",
            )
        }
        mock_get_registry.return_value = mock_registry
        mock_repo = MagicMock()
        existing_setting = MagicMock()
        existing_setting.key = "process_corrupted_process"
        existing_setting.value = "{ invalid json"
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = existing_setting
        mock_session = MagicMock()
        mock_session.execute = AsyncMock(return_value=mock_result)
        mock_repo.session.return_value.__aenter__.return_value = mock_session
        mock_get_repo.return_value = mock_repo
        await factory.sync_registry_to_database()


@pytest.mark.asyncio()
async def test_start_all_processes_core_failure_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify CoreProcessStartupError raised when enabled CORE fails.

    Given: An enabled CORE process that raises on start,
    When: start_all_processes is called,
    Then: CoreProcessStartupError is raised with the failed process name.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    cast(Any, factory)._start_native_process_monitoring = mock.Mock()
    config = ProcessConfigModel(
        name="zmq_broker",
        enabled=True,
        mode="thread",
        class_path="test.Broker",
        method="start",
        parameters={},
        role=ProcessRoleEnum.CORE,
        lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
    )
    monkeypatch.setattr(factory, "get_process_configs", mock.AsyncMock(return_value=[config]))
    monkeypatch.setattr(factory, "start_process", mock.AsyncMock(side_effect=RuntimeError("boom")))
    with pytest.raises(CoreProcessStartupError) as exc_info:
        await factory.start_all_processes()
    assert "zmq_broker" in exc_info.value.failed_processes


@pytest.mark.asyncio()
async def test_start_all_processes_non_core_failure_continues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify non-CORE failure does not raise.

    Given: An enabled TASK process that raises on start,
    When: start_all_processes is called,
    Then: No exception is raised.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    cast(Any, factory)._start_native_process_monitoring = mock.Mock()
    config = ProcessConfigModel(
        name="backfill",
        enabled=True,
        mode="thread",
        class_path="test.Backfill",
        method="start",
        parameters={},
        role=ProcessRoleEnum.TASK,
    )
    monkeypatch.setattr(factory, "get_process_configs", mock.AsyncMock(return_value=[config]))
    monkeypatch.setattr(factory, "start_process", mock.AsyncMock(side_effect=RuntimeError("boom")))
    await factory.start_all_processes()


@pytest.mark.asyncio()
async def test_start_all_processes_one_shot_core_failure_does_not_raise(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify one-shot CORE failure does not abort startup.

    Given: An enabled ONE_SHOT CORE process that raises on start,
    When: start_all_processes is called,
    Then: No CoreProcessStartupError is raised (one-shot CORE is not fail-closed).
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    cast(Any, factory)._start_native_process_monitoring = mock.Mock()
    config = ProcessConfigModel(
        name="init_db",
        enabled=True,
        mode="thread",
        class_path="test.InitDb",
        method="start",
        parameters={},
        role=ProcessRoleEnum.CORE,
        lifecycle=ProcessLifecycleEnum.ONE_SHOT,
    )
    monkeypatch.setattr(factory, "get_process_configs", mock.AsyncMock(return_value=[config]))
    monkeypatch.setattr(factory, "start_process", mock.AsyncMock(side_effect=RuntimeError("boom")))
    await factory.start_all_processes()


@pytest.mark.asyncio()
async def test_get_core_health_all_running(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify healthy when all enabled long-running CORE running.

    Given: An enabled long-running CORE process is in started_processes,
    When: get_core_health is called,
    Then: Returns "healthy".
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    config = ProcessConfigModel(
        name="zmq_broker",
        enabled=True,
        mode="thread",
        class_path="test.Broker",
        method="start",
        parameters={},
        role=ProcessRoleEnum.CORE,
        lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
    )
    factory.started_processes["zmq_broker"] = mock.MagicMock()
    monkeypatch.setattr(factory, "get_process_configs", mock.AsyncMock(return_value=[config]))
    assert await factory.get_core_health() == "healthy"


@pytest.mark.asyncio()
async def test_get_core_health_core_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify error when enabled long-running CORE is missing.

    Given: An enabled long-running CORE process is NOT in started_processes,
    When: get_core_health is called,
    Then: Returns "error".
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    config = ProcessConfigModel(
        name="zmq_broker",
        enabled=True,
        mode="thread",
        class_path="test.Broker",
        method="start",
        parameters={},
        role=ProcessRoleEnum.CORE,
        lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
    )
    monkeypatch.setattr(factory, "get_process_configs", mock.AsyncMock(return_value=[config]))
    assert await factory.get_core_health() == "error"


@pytest.mark.asyncio()
async def test_get_core_health_disabled_core_ignored(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify disabled CORE does not cause error.

    Given: A disabled CORE process is not running,
    When: get_core_health is called,
    Then: Returns "healthy" because disabled is intentional.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    config = ProcessConfigModel(
        name="zmq_broker",
        enabled=False,
        mode="thread",
        class_path="test.Broker",
        method="start",
        parameters={},
        role=ProcessRoleEnum.CORE,
        lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
    )
    monkeypatch.setattr(factory, "get_process_configs", mock.AsyncMock(return_value=[config]))
    assert await factory.get_core_health() == "healthy"


@pytest.mark.asyncio()
async def test_get_core_health_no_core_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify healthy when no CORE processes exist.

    Given: Only TASK processes are configured,
    When: get_core_health is called,
    Then: Returns "healthy".
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    config = ProcessConfigModel(
        name="backfill",
        enabled=True,
        mode="thread",
        class_path="test.Backfill",
        method="start",
        parameters={},
        role=ProcessRoleEnum.TASK,
    )
    monkeypatch.setattr(factory, "get_process_configs", mock.AsyncMock(return_value=[config]))
    assert await factory.get_core_health() == "healthy"


@pytest.mark.asyncio()
async def test_get_core_health_one_shot_completed_ignored(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify one-shot CORE is not treated as missing.

    Given: An enabled one-shot CORE process not in started_processes,
    When: get_core_health is called,
    Then: Returns "healthy" because one-shot is not long-running.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    config = ProcessConfigModel(
        name="init_task",
        enabled=True,
        mode="thread",
        class_path="test.Init",
        method="start",
        parameters={},
        role=ProcessRoleEnum.CORE,
        lifecycle=ProcessLifecycleEnum.ONE_SHOT,
    )
    monkeypatch.setattr(factory, "get_process_configs", mock.AsyncMock(return_value=[config]))
    assert await factory.get_core_health() == "healthy"


@pytest.mark.asyncio()
async def test_get_core_health_skips_executor_templates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bare executor templates do not contribute to the CORE health check.

    Given: An enabled CORE long-running ``executor_kraken`` template
        config that is NOT in ``started_processes``,
    When: ``get_core_health`` is called,
    Then: It returns "healthy" — templates are config-only and the
        per-wallet spawner expands them into runnable instances.
        Instance-level CORE startup failures already escalate via
        :class:`CoreProcessStartupError` at boot.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    template_config = ProcessConfigModel(
        name="executor_kraken",
        enabled=True,
        mode="thread",
        class_path="test.KrakenExecutor",
        method="start",
        parameters={},
        role=ProcessRoleEnum.CORE,
        lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
    )
    monkeypatch.setattr(
        factory, "get_process_configs", mock.AsyncMock(return_value=[template_config])
    )
    assert await factory.get_core_health() == "healthy"


@pytest.mark.asyncio()
async def test_get_core_health_non_executor_core_still_checked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-executor CORE missing still flips health to error.

    Given: An enabled CORE long-running ``zmq_broker`` config that is
        NOT in ``started_processes``,
    When: ``get_core_health`` is called,
    Then: It returns "error" — the template-skip is scoped strictly
        to ``executor_<exchange>`` names; other CORE long-running
        processes (broker, feeds) keep their original semantic.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    config = ProcessConfigModel(
        name="zmq_broker",
        enabled=True,
        mode="thread",
        class_path="test.Broker",
        method="start",
        parameters={},
        role=ProcessRoleEnum.CORE,
        lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
    )
    monkeypatch.setattr(factory, "get_process_configs", mock.AsyncMock(return_value=[config]))
    assert await factory.get_core_health() == "error"


@pytest.mark.asyncio()
async def test_get_core_health_api_only_returns_healthy() -> None:
    """Verify healthy in API-only mode.

    Given: server_api_only is True and no processes started,
    When: get_core_health is called,
    Then: Returns "healthy" because processes are intentionally not started.
    """
    settings = mock.MagicMock()
    settings.server_api_only = True
    settings.db_url = "sqlite:///:memory:"
    factory = ProcessLauncherService(settings)
    assert await factory.get_core_health() == "healthy"


@pytest.mark.asyncio()
async def test_get_core_health_caches_result_within_ttl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A pre-warmed cache short-circuits the DB scan.

    Given: ``_core_health_cache`` already holds a fresh healthy
        entry (timestamp == current monotonic clock),
    When: ``get_core_health`` is called,
    Then: ``get_process_configs`` is NOT invoked — the cache hit
        avoids the Postgres roundtrip that otherwise stalls under
        tick-writer pool contention.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    configs_mock = mock.AsyncMock(return_value=[])
    monkeypatch.setattr(factory, "get_process_configs", configs_mock)
    factory._core_health_cache = (time.monotonic(), "healthy")
    assert await factory.get_core_health() == "healthy"
    assert configs_mock.await_count == 0


@pytest.mark.asyncio()
async def test_get_core_health_refreshes_after_ttl_expiry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stale cache entry is bypassed and refreshed.

    Given: ``_core_health_cache`` holds a healthy entry timestamped
        far enough in the past to exceed ``_CORE_HEALTH_CACHE_TTL_S``,
    When: ``get_core_health`` is called with no enabled CORE
        processes in ``started_processes``,
    Then: ``get_process_configs`` is invoked and the fresh "error"
        result replaces the cached "healthy" entry — proving the TTL
        gate works and that the result is re-cached for the next
        window.
    """
    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    config = ProcessConfigModel(
        name="zmq_broker",
        enabled=True,
        mode="thread",
        class_path="test.Broker",
        method="start",
        parameters={},
        role=ProcessRoleEnum.CORE,
        lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
    )
    configs_mock = mock.AsyncMock(return_value=[config])
    monkeypatch.setattr(factory, "get_process_configs", configs_mock)
    factory._core_health_cache = (time.monotonic() - 3600.0, "healthy")
    assert await factory.get_core_health() == "error"
    assert configs_mock.await_count == 1
    assert factory._core_health_cache is not None
    assert factory._core_health_cache[1] == "error"


def test_validate_parameters_with_model(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify _validate_parameters applies Pydantic model when available.

    Given: A process with parameters_model in its registry entry,
    When: _validate_parameters is called,
    Then: Parameters are validated and dumped through the model.
    """

    class _TestParams(BaseModel):
        """Test parameter model."""

        endpoint: str = "default"

    settings = _create_settings()
    factory = ProcessLauncherService(settings)
    config = ProcessConfigModel(
        name="validated_proc",
        enabled=True,
        mode="thread",
        class_path="test.Proc",
        method="start",
        parameters={"endpoint": "tcp://localhost:5555"},
    )
    entry = ProcessRegistryEntry(
        class_ref=SyncProcess,
        class_path="test.Proc",
        method="start",
        description="test",
        priority=50,
        lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
        role=ProcessRoleEnum.CORE,
        tags=(),
        parameters_model=_TestParams,
        parameters_schema=None,
        enabled=True,
        mode="thread",
    )
    monkeypatch.setattr(
        "snapper.application.process_manager.launcher.get_registered_processes",
        lambda: {"validated_proc": entry},
    )
    result = factory._validate_parameters(config)
    assert result == {"endpoint": "tcp://localhost:5555"}


class TestSpawnPerWalletExecutors:
    """Dynamic per-wallet executor spawner coverage.

    The spawner queries ``wallet_credentials`` and starts one
    ``ProcessConfigModel`` per ``(exchange, wallet)`` pair via
    :meth:`ProcessLauncherService.start_process`. The tests cover the
    happy path, the empty-credentials short-circuit, the missing
    template skip, the duplicate-instance skip, and the per-instance
    failure isolation. ``list_active_wallet_credentials`` and
    ``start_process`` are mocked so the tests stay pure unit tests.
    """

    def _make_factory(self) -> ProcessLauncherService:
        settings = MagicMock()
        settings.db_url = "sqlite+aiosqlite:///:memory:"
        return ProcessLauncherService(settings)

    def _make_entry(self, class_path: str = "test.PaperExecutor") -> ProcessRegistryEntry:
        return ProcessRegistryEntry(
            class_ref=cast(Any, MagicMock()),
            class_path=class_path,
            method="start",
            description="paper executor template",
            priority=30,
            lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
            role=ProcessRoleEnum.CORE,
            tags=("execution", "orders", "paper"),
            parameters_model=None,
            parameters_schema=None,
            enabled=True,
            mode="thread",
        )

    @pytest.mark.asyncio
    async def test_spawn_returns_zero_when_no_credentials(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Empty credential list short-circuits the spawner.

        Given: A repository with zero active wallet credentials,
        When: ``spawn_per_wallet_executors`` is called,
        Then: It returns 0 and does not call ``start_process``.
        """
        factory = self._make_factory()
        repo = MagicMock()
        repo.list_active_wallet_credentials = AsyncMock(return_value=[])
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_repository",
            lambda _url: repo,
        )
        start_mock = AsyncMock()
        monkeypatch.setattr(factory, "start_process", start_mock)
        result = await factory.spawn_per_wallet_executors()
        assert result == 0
        start_mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_spawn_returns_zero_when_repository_query_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Repository errors are logged and the spawner returns 0.

        Given: A repository whose ``list_active_wallet_credentials``
            raises (e.g. DB connection issue at boot),
        When: ``spawn_per_wallet_executors`` is called,
        Then: The exception is caught, the spawner returns 0, and
            ``start_process`` is never called.
        """
        factory = self._make_factory()
        repo = MagicMock()
        repo.list_active_wallet_credentials = AsyncMock(side_effect=RuntimeError("db down"))
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_repository",
            lambda _url: repo,
        )
        start_mock = AsyncMock()
        monkeypatch.setattr(factory, "start_process", start_mock)
        result = await factory.spawn_per_wallet_executors()
        assert result == 0
        start_mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_spawn_starts_one_instance_per_credential(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Each active credential row produces one start_process call.

        Given: Two credential rows for two different exchanges and a
            registered template for each,
        When: ``spawn_per_wallet_executors`` is called,
        Then: ``start_process`` is invoked twice with deterministic
            instance names of the form
            ``executor_{exchange}_w{wallet_short_12hex}`` and
            ``parameters={"wallet_public_id": ...}`` carrying the
            wallet ID.
        """
        factory = self._make_factory()
        wallet_a = "00000000-0000-7000-8000-0000000000a1"
        wallet_b = "00000000-0000-7000-8000-0000000000b2"
        repo = MagicMock()
        repo.list_active_wallet_credentials = AsyncMock(
            return_value=[
                {
                    "public_id": "cred-a",
                    "wallet_public_id": wallet_a,
                    "exchange": "kraken",
                    "credential_type": "api_key_secret",
                    "encrypted_payload": "enc",
                    "label": None,
                    "timestamp": datetime.now(UTC),
                    "session_id": "s",
                    "sequence_id": 1,
                },
                {
                    "public_id": "cred-b",
                    "wallet_public_id": wallet_b,
                    "exchange": "paper",
                    "credential_type": "paper",
                    "encrypted_payload": "enc",
                    "label": None,
                    "timestamp": datetime.now(UTC),
                    "session_id": "s",
                    "sequence_id": 2,
                },
            ]
        )
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_repository",
            lambda _url: repo,
        )
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_registered_processes",
            lambda: {
                "executor_kraken": self._make_entry("snapper.executors.KrakenExecutor"),
                "executor_paper": self._make_entry("snapper.executors.PaperExecutor"),
            },
        )
        start_mock = AsyncMock()
        monkeypatch.setattr(factory, "start_process", start_mock)
        monkeypatch.setattr(factory, "_load_template_setting", AsyncMock(return_value={}))
        result = await factory.spawn_per_wallet_executors()
        assert result == 2
        assert start_mock.await_count == 2
        names = [call.args[0].name for call in start_mock.await_args_list]
        assert names == [
            "executor_kraken_w0000000000a1",
            "executor_paper_w0000000000b2",
        ]
        params = [call.args[0].parameters for call in start_mock.await_args_list]
        assert params == [
            {"wallet_public_id": wallet_a},
            {"wallet_public_id": wallet_b},
        ]

    @pytest.mark.asyncio
    async def test_spawn_skips_credentials_without_template(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Credentials for unregistered exchanges are skipped with a warning.

        Given: A credential for ``exchange="bitstamp"`` but the
            registry contains no ``executor_bitstamp`` template,
        When: ``spawn_per_wallet_executors`` is called,
        Then: ``start_process`` is not called and the spawner returns 0.
        """
        factory = self._make_factory()
        repo = MagicMock()
        repo.list_active_wallet_credentials = AsyncMock(
            return_value=[
                {
                    "public_id": "cred-x",
                    "wallet_public_id": "00000000-0000-7000-8000-0000000000c1",
                    "exchange": "bitstamp",
                    "credential_type": "api_key_secret",
                    "encrypted_payload": "enc",
                    "label": None,
                    "timestamp": datetime.now(UTC),
                    "session_id": "s",
                    "sequence_id": 1,
                },
            ]
        )
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_repository",
            lambda _url: repo,
        )
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_registered_processes",
            lambda: {"executor_kraken": self._make_entry()},
        )
        start_mock = AsyncMock()
        monkeypatch.setattr(factory, "start_process", start_mock)
        result = await factory.spawn_per_wallet_executors()
        assert result == 0
        start_mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_spawn_skips_already_started_instances(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Idempotent: instances already running are not respawned.

        Given: A credential whose computed instance name is already
            present in ``self.started_processes`` (e.g. spawner
            re-invoked after a failed boot),
        When: ``spawn_per_wallet_executors`` is called,
        Then: ``start_process`` is not invoked for that instance and
            the spawner returns 0.
        """
        factory = self._make_factory()
        wallet = "00000000-0000-7000-8000-0000000000d1"
        factory.started_processes["executor_paper_w0000000000d1"] = cast(Any, MagicMock())
        repo = MagicMock()
        repo.list_active_wallet_credentials = AsyncMock(
            return_value=[
                {
                    "public_id": "cred-d",
                    "wallet_public_id": wallet,
                    "exchange": "paper",
                    "credential_type": "paper",
                    "encrypted_payload": "enc",
                    "label": None,
                    "timestamp": datetime.now(UTC),
                    "session_id": "s",
                    "sequence_id": 1,
                },
            ]
        )
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_repository",
            lambda _url: repo,
        )
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_registered_processes",
            lambda: {"executor_paper": self._make_entry()},
        )
        start_mock = AsyncMock()
        monkeypatch.setattr(factory, "start_process", start_mock)
        result = await factory.spawn_per_wallet_executors()
        assert result == 0
        start_mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_spawn_continues_after_per_instance_failure_then_raises_for_core(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """CORE per-instance failures are isolated AND propagated.

        Given: Two credentials whose template is registered as
            ``role=CORE`` + ``lifecycle=LONG_RUNNING`` and the first
            credential fails ``start_process``,
        When: ``spawn_per_wallet_executors`` is called,
        Then: The spawner logs the failure, continues with the second
            credential (so a misconfigured wallet does not block all
            other wallets), and after the loop completes raises
            :class:`CoreProcessStartupError` listing the failed CORE
            instance — matching :meth:`start_all_processes` semantics.
        """
        factory = self._make_factory()
        wallet_a = "00000000-0000-7000-8000-0000000000e1"
        wallet_b = "00000000-0000-7000-8000-0000000000e2"
        repo = MagicMock()
        repo.list_active_wallet_credentials = AsyncMock(
            return_value=[
                {
                    "public_id": "cred-e1",
                    "wallet_public_id": wallet_a,
                    "exchange": "kraken",
                    "credential_type": "api_key_secret",
                    "encrypted_payload": "enc",
                    "label": None,
                    "timestamp": datetime.now(UTC),
                    "session_id": "s",
                    "sequence_id": 1,
                },
                {
                    "public_id": "cred-e2",
                    "wallet_public_id": wallet_b,
                    "exchange": "kraken",
                    "credential_type": "api_key_secret",
                    "encrypted_payload": "enc",
                    "label": None,
                    "timestamp": datetime.now(UTC),
                    "session_id": "s",
                    "sequence_id": 2,
                },
            ]
        )
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_repository",
            lambda _url: repo,
        )
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_registered_processes",
            lambda: {"executor_kraken": self._make_entry()},
        )
        start_mock = AsyncMock(side_effect=[RuntimeError("client init"), None])
        monkeypatch.setattr(factory, "start_process", start_mock)
        monkeypatch.setattr(factory, "_load_template_setting", AsyncMock(return_value={}))
        with pytest.raises(CoreProcessStartupError) as exc_info:
            await factory.spawn_per_wallet_executors()
        assert start_mock.await_count == 2
        assert "executor_kraken_w0000000000e1" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_spawn_continues_after_non_core_per_instance_failure(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Non-CORE per-instance failures stay isolated and do NOT raise.

        Given: A non-CORE template (e.g. ``role=TASK``) where the
            first credential fails ``start_process``,
        When: ``spawn_per_wallet_executors`` is called,
        Then: The spawner logs the failure, continues with the second
            credential, and returns the count of successful spawns
            without raising — only LONG_RUNNING CORE failures escalate.
        """
        factory = self._make_factory()
        wallet_a = "00000000-0000-7000-8000-0000000000f1"
        wallet_b = "00000000-0000-7000-8000-0000000000f2"
        repo = MagicMock()
        repo.list_active_wallet_credentials = AsyncMock(
            return_value=[
                {
                    "public_id": "cred-f1",
                    "wallet_public_id": wallet_a,
                    "exchange": "kraken",
                    "credential_type": "api_key_secret",
                    "encrypted_payload": "enc",
                    "label": None,
                    "timestamp": datetime.now(UTC),
                    "session_id": "s",
                    "sequence_id": 1,
                },
                {
                    "public_id": "cred-f2",
                    "wallet_public_id": wallet_b,
                    "exchange": "kraken",
                    "credential_type": "api_key_secret",
                    "encrypted_payload": "enc",
                    "label": None,
                    "timestamp": datetime.now(UTC),
                    "session_id": "s",
                    "sequence_id": 2,
                },
            ]
        )
        non_core_entry = ProcessRegistryEntry(
            class_ref=cast(Any, MagicMock()),
            class_path="test.NonCoreExecutor",
            method="start",
            description="non-core executor template",
            priority=30,
            lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
            role=ProcessRoleEnum.TASK,
            tags=("execution", "orders", "kraken"),
            parameters_model=None,
            parameters_schema=None,
            enabled=True,
            mode="thread",
        )
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_repository",
            lambda _url: repo,
        )
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_registered_processes",
            lambda: {"executor_kraken": non_core_entry},
        )
        start_mock = AsyncMock(side_effect=[RuntimeError("client init"), None])
        monkeypatch.setattr(factory, "start_process", start_mock)
        monkeypatch.setattr(factory, "_load_template_setting", AsyncMock(return_value={}))
        result = await factory.spawn_per_wallet_executors()
        assert result == 1
        assert start_mock.await_count == 2

    @pytest.mark.asyncio
    async def test_spawn_inherits_template_parameters_from_setting(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Active template Setting params propagate to per-wallet instance.

        Given: An active ``process_executor_kraken`` Setting carrying a
            ``parameters`` dict {"max_qty": 1.5},
        When: ``spawn_per_wallet_executors`` is called for a kraken wallet,
        Then: The instance config passed to ``start_process`` carries
            both the template's ``max_qty=1.5`` AND the per-instance
            ``wallet_public_id`` overlay.
        """
        factory = self._make_factory()
        wallet = "00000000-0000-7000-8000-0000000000a1"
        repo = MagicMock()
        repo.list_active_wallet_credentials = AsyncMock(
            return_value=[
                {
                    "public_id": "cred-a",
                    "wallet_public_id": wallet,
                    "exchange": "kraken",
                    "credential_type": "api_key_secret",
                    "encrypted_payload": "enc",
                    "label": None,
                    "timestamp": datetime.now(UTC),
                    "session_id": "s",
                    "sequence_id": 1,
                },
            ]
        )
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_repository",
            lambda _url: repo,
        )
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_registered_processes",
            lambda: {"executor_kraken": self._make_entry()},
        )
        start_mock = AsyncMock()
        monkeypatch.setattr(factory, "start_process", start_mock)
        monkeypatch.setattr(
            factory,
            "_load_template_setting",
            AsyncMock(return_value={"parameters": {"max_qty": 1.5}}),
        )
        result = await factory.spawn_per_wallet_executors()
        assert result == 1
        instance_config = start_mock.await_args_list[0].args[0]
        assert instance_config.parameters == {
            "max_qty": 1.5,
            "wallet_public_id": wallet,
        }

    @pytest.mark.asyncio
    async def test_spawn_template_class_path_override_ignored(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Setting attempts to override registry-fixed fields are dropped.

        Given: A template Setting carrying a malicious ``class``
            override pointing at an arbitrary class path,
        When: ``spawn_per_wallet_executors`` is called,
        Then: The instance ``class_path`` is the registry value, not
            the Setting override — registry decorator wins for code
            identity.
        """
        factory = self._make_factory()
        wallet = "00000000-0000-7000-8000-0000000000a2"
        repo = MagicMock()
        repo.list_active_wallet_credentials = AsyncMock(
            return_value=[
                {
                    "public_id": "cred-a",
                    "wallet_public_id": wallet,
                    "exchange": "kraken",
                    "credential_type": "api_key_secret",
                    "encrypted_payload": "enc",
                    "label": None,
                    "timestamp": datetime.now(UTC),
                    "session_id": "s",
                    "sequence_id": 1,
                },
            ]
        )
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_repository",
            lambda _url: repo,
        )
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_registered_processes",
            lambda: {"executor_kraken": self._make_entry("snapper.executors.RegistryClass")},
        )
        start_mock = AsyncMock()
        monkeypatch.setattr(factory, "start_process", start_mock)
        monkeypatch.setattr(
            factory,
            "_load_template_setting",
            AsyncMock(return_value={"class": "evil.Hacked", "parameters": {"safe": True}}),
        )
        result = await factory.spawn_per_wallet_executors()
        assert result == 1
        instance_config = start_mock.await_args_list[0].args[0]
        assert instance_config.class_path == "snapper.executors.RegistryClass"
        assert instance_config.parameters == {"safe": True, "wallet_public_id": wallet}

    @pytest.mark.asyncio
    async def test_spawn_populates_instance_configs(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Successful spawn registers the live config in ``instance_configs``.

        The route layer reads ``instance_configs`` to synthesize
        per-wallet rows in ``/processes/configured`` and to count
        running executors in ``/processes/summary``. This test pins
        the contract.
        """
        factory = self._make_factory()
        wallet = "00000000-0000-7000-8000-0000000000aa"
        repo = MagicMock()
        repo.list_active_wallet_credentials = AsyncMock(
            return_value=[
                {
                    "public_id": "cred-aa",
                    "wallet_public_id": wallet,
                    "exchange": "kraken",
                    "credential_type": "api_key_secret",
                    "encrypted_payload": "enc",
                    "label": None,
                    "timestamp": datetime.now(UTC),
                    "session_id": "s",
                    "sequence_id": 1,
                },
            ]
        )
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_repository",
            lambda _url: repo,
        )
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_registered_processes",
            lambda: {"executor_kraken": self._make_entry()},
        )
        monkeypatch.setattr(factory, "start_process", AsyncMock())
        monkeypatch.setattr(factory, "_load_template_setting", AsyncMock(return_value={}))
        result = await factory.spawn_per_wallet_executors()
        assert result == 1
        instance_name = "executor_kraken_w0000000000aa"
        assert instance_name in factory.instance_configs
        registered_config = factory.instance_configs[instance_name]
        assert registered_config.parameters["wallet_public_id"] == wallet
        assert registered_config.name == instance_name

    @pytest.mark.asyncio
    async def test_spawn_failure_does_not_populate_instance_configs(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Failed spawn leaves ``instance_configs`` clean (registered only on success)."""
        factory = self._make_factory()
        wallet = "00000000-0000-7000-8000-0000000000bb"
        repo = MagicMock()
        repo.list_active_wallet_credentials = AsyncMock(
            return_value=[
                {
                    "public_id": "cred-bb",
                    "wallet_public_id": wallet,
                    "exchange": "kraken",
                    "credential_type": "api_key_secret",
                    "encrypted_payload": "enc",
                    "label": None,
                    "timestamp": datetime.now(UTC),
                    "session_id": "s",
                    "sequence_id": 1,
                },
            ]
        )
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_repository",
            lambda _url: repo,
        )
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_registered_processes",
            lambda: {"executor_kraken": self._make_entry()},
        )
        monkeypatch.setattr(
            factory,
            "start_process",
            AsyncMock(side_effect=RuntimeError("client init")),
        )
        monkeypatch.setattr(factory, "_load_template_setting", AsyncMock(return_value={}))
        with pytest.raises(CoreProcessStartupError):
            await factory.spawn_per_wallet_executors()
        assert factory.instance_configs == {}

    @pytest.mark.asyncio
    async def test_spawn_same_millisecond_wallets_get_distinct_names(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Wallets sharing the UUID7 timestamp prefix get distinct instance names.

        Given: Two wallets whose UUID7 first-12 hex chars (timestamp +
            version + random_a) are IDENTICAL — what happens when two
            wallets are created in the same millisecond on the same
            exchange,
        When: ``spawn_per_wallet_executors`` is called,
        Then: Both wallets spawn distinct instances. The canonical
            ``wallet_short`` derives from the LAST 12 hex chars (the
            random portion) so collisions reduce to ~1 in 2^48 rather
            than the deterministic timestamp collision the legacy
            first-12 algorithm produced.
        """
        factory = self._make_factory()
        wallet_first_ms_a = "01975a8b-3c7d-7000-8000-aaaaaaaaaaaa"
        wallet_first_ms_b = "01975a8b-3c7d-cccc-8000-bbbbbbbbbbbb"
        repo = MagicMock()
        repo.list_active_wallet_credentials = AsyncMock(
            return_value=[
                {
                    "public_id": "cred-a",
                    "wallet_public_id": wallet_first_ms_a,
                    "exchange": "kraken",
                    "credential_type": "api_key_secret",
                    "encrypted_payload": "enc",
                    "label": None,
                    "timestamp": datetime.now(UTC),
                    "session_id": "s",
                    "sequence_id": 1,
                },
                {
                    "public_id": "cred-b",
                    "wallet_public_id": wallet_first_ms_b,
                    "exchange": "kraken",
                    "credential_type": "api_key_secret",
                    "encrypted_payload": "enc",
                    "label": None,
                    "timestamp": datetime.now(UTC),
                    "session_id": "s",
                    "sequence_id": 2,
                },
            ]
        )
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_repository",
            lambda _url: repo,
        )
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_registered_processes",
            lambda: {"executor_kraken": self._make_entry()},
        )
        start_mock = AsyncMock()
        monkeypatch.setattr(factory, "start_process", start_mock)
        monkeypatch.setattr(factory, "_load_template_setting", AsyncMock(return_value={}))
        result = await factory.spawn_per_wallet_executors()
        assert result == 2
        names = {call.args[0].name for call in start_mock.await_args_list}
        assert names == {
            "executor_kraken_waaaaaaaaaaaa",
            "executor_kraken_wbbbbbbbbbbbb",
        }

    @pytest.mark.asyncio
    async def test_spawn_two_wallets_share_template_parameters(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Two wallets on the same exchange share template parameters.

        Given: Two credentials for the same kraken exchange and one
            active template Setting with ``parameters={"throttle": 5}``,
        When: ``spawn_per_wallet_executors`` is called,
        Then: Both instance configs carry the same ``throttle=5`` value
            (from the shared template), but distinct ``wallet_public_id``
            overlays — and the loader is consulted only once thanks to
            per-exchange caching.
        """
        factory = self._make_factory()
        wallet_a = "00000000-0000-7000-8000-0000000000a3"
        wallet_b = "00000000-0000-7000-8000-0000000000a4"
        repo = MagicMock()
        repo.list_active_wallet_credentials = AsyncMock(
            return_value=[
                {
                    "public_id": "cred-a",
                    "wallet_public_id": wallet_a,
                    "exchange": "kraken",
                    "credential_type": "api_key_secret",
                    "encrypted_payload": "enc",
                    "label": None,
                    "timestamp": datetime.now(UTC),
                    "session_id": "s",
                    "sequence_id": 1,
                },
                {
                    "public_id": "cred-b",
                    "wallet_public_id": wallet_b,
                    "exchange": "kraken",
                    "credential_type": "api_key_secret",
                    "encrypted_payload": "enc",
                    "label": None,
                    "timestamp": datetime.now(UTC),
                    "session_id": "s",
                    "sequence_id": 2,
                },
            ]
        )
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_repository",
            lambda _url: repo,
        )
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_registered_processes",
            lambda: {"executor_kraken": self._make_entry()},
        )
        start_mock = AsyncMock()
        monkeypatch.setattr(factory, "start_process", start_mock)
        load_mock = AsyncMock(return_value={"parameters": {"throttle": 5}})
        monkeypatch.setattr(factory, "_load_template_setting", load_mock)
        result = await factory.spawn_per_wallet_executors()
        assert result == 2
        assert load_mock.await_count == 1
        params = [call.args[0].parameters for call in start_mock.await_args_list]
        assert params == [
            {"throttle": 5, "wallet_public_id": wallet_a},
            {"throttle": 5, "wallet_public_id": wallet_b},
        ]


class TestGetProcessConfigsExecutorTemplateNormalization:
    """``get_process_configs`` normalizes executor templates at read time.

    Templates are config-only — never directly runnable — so a
    persisted ``enabled=True`` flag from the legacy single-wallet era
    must not flow through. The launcher's ``get_process_configs``
    forces ``enabled=False`` and strips any leaked
    ``wallet_public_id`` from template ``parameters``.
    """

    @pytest.mark.asyncio
    async def test_executor_template_enabled_forced_false(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Persisted ``enabled=True`` on template is forced to False at read."""
        settings = _create_settings()
        factory = ProcessLauncherService(settings)
        template_config = ProcessConfigModel(
            name="executor_kraken",
            enabled=True,
            mode="thread",
            class_path="test.KrakenExecutor",
            method="start",
            parameters={"throttle": 5},
        )
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_process_configs",
            mock.AsyncMock(return_value=[template_config]),
        )
        configs = await factory.get_process_configs()
        assert len(configs) == 1
        assert configs[0].name == "executor_kraken"
        assert configs[0].enabled is False
        assert configs[0].parameters == {"throttle": 5}

    @pytest.mark.asyncio
    async def test_executor_template_strips_wallet_leak(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Leaked ``wallet_public_id`` in template params is stripped at read."""
        settings = _create_settings()
        factory = ProcessLauncherService(settings)
        template_config = ProcessConfigModel(
            name="executor_kraken",
            enabled=False,
            mode="thread",
            class_path="test.KrakenExecutor",
            method="start",
            parameters={
                "throttle": 5,
                "wallet_public_id": "00000000-0000-7000-8000-000000000999",
            },
        )
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_process_configs",
            mock.AsyncMock(return_value=[template_config]),
        )
        configs = await factory.get_process_configs()
        assert configs[0].parameters == {"throttle": 5}

    @pytest.mark.asyncio
    async def test_non_template_config_passes_through_unchanged(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Non-executor configs (broker, feeds) are not touched by normalization."""
        settings = _create_settings()
        factory = ProcessLauncherService(settings)
        broker_config = ProcessConfigModel(
            name="zmq_broker",
            enabled=True,
            mode="thread",
            class_path="test.Broker",
            method="start",
            parameters={"endpoint": "tcp://0.0.0.0:5555"},
        )
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_process_configs",
            mock.AsyncMock(return_value=[broker_config]),
        )
        configs = await factory.get_process_configs()
        assert configs[0].enabled is True
        assert configs[0].parameters == {"endpoint": "tcp://0.0.0.0:5555"}

    @pytest.mark.asyncio
    async def test_executor_instance_name_in_db_passes_through(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Per-wallet instance names (if persisted) bypass normalization.

        Per-wallet instances are normally synthesized at runtime, but
        if one ever ended up in the ``Setting`` table (e.g. legacy
        data) the normalization should NOT strip its
        ``wallet_public_id`` — the instance needs it.
        """
        settings = _create_settings()
        factory = ProcessLauncherService(settings)
        instance_config = ProcessConfigModel(
            name="executor_kraken_w0000000000a1",
            enabled=True,
            mode="thread",
            class_path="test.KrakenExecutor",
            method="start",
            parameters={"wallet_public_id": "00000000-0000-7000-8000-0000000000a1"},
        )
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_process_configs",
            mock.AsyncMock(return_value=[instance_config]),
        )
        configs = await factory.get_process_configs()
        assert configs[0].enabled is True
        assert configs[0].parameters == {"wallet_public_id": "00000000-0000-7000-8000-0000000000a1"}


class TestStartPerWalletInstanceByName:
    """Coverage for ``start_per_wallet_instance_by_name`` resolver.

    Manual restart of ``executor_<exchange>_w<short>`` parses the
    name, looks up the matching credential, rebuilds the config via
    ``_build_per_wallet_instance_config`` (so Setting edits since boot
    take effect), and registers the instance on success.
    """

    _WALLET = "00000000-0000-7000-8000-0000000000a1"
    _INSTANCE_NAME = "executor_kraken_w0000000000a1"

    def _make_factory(self) -> ProcessLauncherService:
        settings = MagicMock()
        settings.db_url = "sqlite+aiosqlite:///:memory:"
        return ProcessLauncherService(settings)

    def _make_entry(self, class_path: str = "test.KrakenExecutor") -> ProcessRegistryEntry:
        return ProcessRegistryEntry(
            class_ref=cast(Any, MagicMock()),
            class_path=class_path,
            method="start",
            description="kraken executor template",
            priority=30,
            lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
            role=ProcessRoleEnum.CORE,
            tags=("execution", "orders", "kraken"),
            parameters_model=None,
            parameters_schema=None,
            enabled=True,
            mode="thread",
        )

    def _credential_for_wallet(self, wallet: str, exchange: str) -> dict[str, Any]:
        return {
            "public_id": "cred-1",
            "wallet_public_id": wallet,
            "exchange": exchange,
            "credential_type": "api_key_secret",
            "encrypted_payload": "enc",
            "label": None,
            "timestamp": datetime.now(UTC),
            "session_id": "s",
            "sequence_id": 1,
        }

    @pytest.mark.asyncio
    async def test_already_running_returns_already_running(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Idempotent: instance already in ``started_processes`` short-circuits."""
        factory = self._make_factory()
        factory.started_processes[self._INSTANCE_NAME] = cast(Any, MagicMock())
        result = await factory.start_per_wallet_instance_by_name(self._INSTANCE_NAME)
        assert result.status == "already_running"

    @pytest.mark.asyncio
    async def test_invalid_instance_name_returns_error(self) -> None:
        """Names that do not match the instance pattern produce ERROR."""
        factory = self._make_factory()
        result = await factory.start_per_wallet_instance_by_name("zmq_broker")
        assert result.status == "error"
        assert "not a per-wallet executor instance" in result.message

    @pytest.mark.asyncio
    async def test_template_not_registered_returns_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """No registry entry → ERROR with template-name context."""
        factory = self._make_factory()
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_registered_processes",
            lambda: {},
        )
        result = await factory.start_per_wallet_instance_by_name(self._INSTANCE_NAME)
        assert result.status == "error"
        assert "executor_kraken" in result.message

    @pytest.mark.asyncio
    async def test_credential_query_fails_returns_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """DB error during credential lookup propagates as ERROR."""
        factory = self._make_factory()
        repo = MagicMock()
        repo.list_active_wallet_credentials = AsyncMock(side_effect=RuntimeError("db down"))
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_repository",
            lambda _url: repo,
        )
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_registered_processes",
            lambda: {"executor_kraken": self._make_entry()},
        )
        result = await factory.start_per_wallet_instance_by_name(self._INSTANCE_NAME)
        assert result.status == "error"
        assert "wallet credentials" in result.message

    @pytest.mark.asyncio
    async def test_no_matching_credential_returns_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Active credentials exist but none match the wallet prefix → ERROR."""
        factory = self._make_factory()
        repo = MagicMock()
        repo.list_active_wallet_credentials = AsyncMock(
            return_value=[
                self._credential_for_wallet("abcdef12-3456-7890-abcd-ef0123456789", "kraken"),
            ]
        )
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_repository",
            lambda _url: repo,
        )
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_registered_processes",
            lambda: {"executor_kraken": self._make_entry()},
        )
        result = await factory.start_per_wallet_instance_by_name(self._INSTANCE_NAME)
        assert result.status == "error"
        assert "No active wallet credential" in result.message

    @pytest.mark.asyncio
    async def test_happy_path_starts_and_registers_instance(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Resolver builds config, starts process, and registers in ``instance_configs``.

        Asserts both the success status AND the side effect on
        ``instance_configs`` so a subsequent ``/configured`` call sees
        the live row.
        """
        factory = self._make_factory()
        repo = MagicMock()
        repo.list_active_wallet_credentials = AsyncMock(
            return_value=[self._credential_for_wallet(self._WALLET, "kraken")]
        )
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_repository",
            lambda _url: repo,
        )
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_registered_processes",
            lambda: {"executor_kraken": self._make_entry()},
        )
        start_mock = AsyncMock()
        monkeypatch.setattr(factory, "start_process", start_mock)
        monkeypatch.setattr(
            factory,
            "_load_template_setting",
            AsyncMock(return_value={"parameters": {"max_qty": 2.5}}),
        )
        result = await factory.start_per_wallet_instance_by_name(self._INSTANCE_NAME)
        assert result.status == "success"
        assert self._INSTANCE_NAME in factory.instance_configs
        registered = factory.instance_configs[self._INSTANCE_NAME]
        assert registered.parameters == {
            "max_qty": 2.5,
            "wallet_public_id": self._WALLET,
        }
        start_mock.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_mode_override_applied_to_instance_config(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Operator-selected ``mode`` overrides the merged template/registry mode.

        Given: A ``mode='process'`` argument from the execution-mode
            modal forwarded through ``start_process_by_name`` to the
            per-wallet resolver, with a registry entry whose default
            mode is ``thread``,
        When: ``start_per_wallet_instance_by_name`` is invoked,
        Then: The instance config registered in ``instance_configs``
            and handed to ``start_process`` carries the operator's
            ``process`` mode — not the registry default — so the
            operator's selection actually takes effect.
        """
        factory = self._make_factory()
        repo = MagicMock()
        repo.list_active_wallet_credentials = AsyncMock(
            return_value=[self._credential_for_wallet(self._WALLET, "kraken")]
        )
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_repository",
            lambda _url: repo,
        )
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_registered_processes",
            lambda: {"executor_kraken": self._make_entry()},
        )
        start_mock = AsyncMock()
        monkeypatch.setattr(factory, "start_process", start_mock)
        monkeypatch.setattr(factory, "_load_template_setting", AsyncMock(return_value={}))
        result = await factory.start_per_wallet_instance_by_name(
            self._INSTANCE_NAME, mode="process"
        )
        assert result.status == "success"
        registered = factory.instance_configs[self._INSTANCE_NAME]
        assert registered.mode == "process"

    @pytest.mark.asyncio
    async def test_start_process_failure_returns_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``start_process`` raise → ERROR result and no entry in ``instance_configs``."""
        factory = self._make_factory()
        repo = MagicMock()
        repo.list_active_wallet_credentials = AsyncMock(
            return_value=[self._credential_for_wallet(self._WALLET, "kraken")]
        )
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_repository",
            lambda _url: repo,
        )
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_registered_processes",
            lambda: {"executor_kraken": self._make_entry()},
        )
        monkeypatch.setattr(
            factory, "start_process", AsyncMock(side_effect=RuntimeError("client init"))
        )
        monkeypatch.setattr(factory, "_load_template_setting", AsyncMock(return_value={}))
        result = await factory.start_per_wallet_instance_by_name(self._INSTANCE_NAME)
        assert result.status == "error"
        assert "client init" in result.message
        assert self._INSTANCE_NAME not in factory.instance_configs

    @pytest.mark.asyncio
    async def test_start_process_failure_restores_prior_instance_config(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A failed manual restart preserves the previously stopped instance.

        The original register-before-start pattern unconditionally
        popped the instance on failure, dropping a stopped-but-configured
        per-wallet executor from the summary/configured snapshots on a
        transient startup hiccup. The current behaviour captures the
        prior entry and restores it on exception instead of popping.

        Given: A factory with an existing ``instance_configs[name]``
            entry (the legitimate stopped instance),
        When: ``start_per_wallet_instance_by_name`` fails inside
            ``start_process``,
        Then: ``instance_configs[name]`` still holds the PRIOR config
            (not the overwritten one) and the ERROR result carries
            the failure detail.
        """
        factory = self._make_factory()
        prior_config = ProcessConfigModel(
            name=self._INSTANCE_NAME,
            enabled=True,
            mode="thread",
            class_path="legacy.X",
            method="start",
            parameters={"wallet_public_id": self._WALLET, "note": "prior"},
            note="prior",
            lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
            role=ProcessRoleEnum.CORE,
            tags=(),
            parameters_schema=None,
        )
        factory.instance_configs[self._INSTANCE_NAME] = prior_config

        repo = MagicMock()
        repo.list_active_wallet_credentials = AsyncMock(
            return_value=[self._credential_for_wallet(self._WALLET, "kraken")]
        )
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_repository",
            lambda _url: repo,
        )
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_registered_processes",
            lambda: {"executor_kraken": self._make_entry()},
        )
        monkeypatch.setattr(
            factory, "start_process", AsyncMock(side_effect=RuntimeError("transient"))
        )
        monkeypatch.setattr(factory, "_load_template_setting", AsyncMock(return_value={}))

        result = await factory.start_per_wallet_instance_by_name(self._INSTANCE_NAME)

        assert result.status == "error"
        assert "transient" in result.message
        assert self._INSTANCE_NAME in factory.instance_configs
        assert factory.instance_configs[self._INSTANCE_NAME] is prior_config


class TestStartProcessByNameDispatch:
    """``start_process_by_name`` dispatches by name pattern.

    - ``executor_<exchange>_w<short>`` → per-wallet resolver
    - ``executor_<exchange>`` → ERROR (template, never directly runnable)
    - everything else → existing DB-Setting flow
    """

    def _make_factory(self) -> ProcessLauncherService:
        settings = MagicMock()
        settings.db_url = "sqlite+aiosqlite:///:memory:"
        return ProcessLauncherService(settings)

    @pytest.mark.asyncio
    async def test_executor_instance_name_dispatches_to_resolver(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Per-wallet name routes to ``start_per_wallet_instance_by_name``."""
        factory = self._make_factory()
        resolver = AsyncMock(
            return_value=ProcessStartResult(status="success", message="ok"),
        )
        monkeypatch.setattr(factory, "start_per_wallet_instance_by_name", resolver)
        result = await factory.start_process_by_name("executor_kraken_w0000000000a1")
        assert result.status == "success"
        resolver.assert_awaited_once_with("executor_kraken_w0000000000a1", mode=None)

    @pytest.mark.asyncio
    async def test_executor_instance_dispatch_forwards_mode_override(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``mode`` from the modal flows through dispatch to the resolver.

        Given: ``start_process_by_name`` invoked with
            ``mode='process'`` for a per-wallet instance name,
        When: dispatch routes to ``start_per_wallet_instance_by_name``,
        Then: The resolver receives the ``mode`` keyword argument so
            the operator's selection actually applies to the rebuilt
            instance config.
        """
        factory = self._make_factory()
        resolver = AsyncMock(
            return_value=ProcessStartResult(status="success", message="ok"),
        )
        monkeypatch.setattr(factory, "start_per_wallet_instance_by_name", resolver)
        await factory.start_process_by_name("executor_kraken_w0000000000a1", mode="process")
        resolver.assert_awaited_once_with("executor_kraken_w0000000000a1", mode="process")

    @pytest.mark.asyncio
    async def test_executor_template_name_returns_error(self) -> None:
        """Bare ``executor_<exchange>`` returns ERROR with template-rejected message."""
        factory = self._make_factory()
        result = await factory.start_process_by_name("executor_kraken")
        assert result.status == "error"
        assert "is an executor template" in result.message
        assert "executor_kraken_w<wallet_short>" in result.message


class TestInstanceConfigsCleanup:
    """``instance_configs`` survives individual stops; clears on full reset.

    Per-wallet instance entries are intentionally retained in
    ``instance_configs`` after individual stops so the
    ``/processes/configured`` API can render a stopped row with a
    working Start button. The dict is cleared only on full
    :meth:`stop_all_processes` (system reset) and on
    :meth:`_cleanup_failed_start` (the entry was never legitimately
    inserted).
    """

    def _make_factory(self) -> ProcessLauncherService:
        settings = MagicMock()
        settings.db_url = "sqlite+aiosqlite:///:memory:"
        return ProcessLauncherService(settings)

    def _seed_instance(self, factory: ProcessLauncherService, name: str) -> None:
        factory.instance_configs[name] = ProcessConfigModel(
            name=name,
            enabled=True,
            mode="thread",
            class_path="test.PaperExecutor",
            method="start",
            parameters={"wallet_public_id": "00000000-0000-7000-8000-0000000000a1"},
        )

    @pytest.mark.asyncio
    async def test_stop_process_by_name_keeps_instance_config(self) -> None:
        """``stop_process_by_name`` retains the entry so UI can offer Start.

        The stopped instance still appears in ``/processes/configured``
        with ``running=False`` and ``kind="instance"`` — the operator
        clicks Start and the resolver rebuilds the live config.
        """
        factory = self._make_factory()
        instance_name = "executor_paper_w0000000000a1"
        instance = MagicMock()
        instance.stop = AsyncMock()
        factory.started_processes[instance_name] = cast(Any, instance)
        self._seed_instance(factory, instance_name)
        result = await factory.stop_process_by_name(instance_name)
        assert result.status == "success"
        assert instance_name in factory.instance_configs
        assert instance_name not in factory.started_processes

    @pytest.mark.asyncio
    async def test_stop_all_processes_clears_instance_configs(self) -> None:
        """``stop_all_processes`` empties ``instance_configs`` entirely."""
        factory = self._make_factory()
        for short in ("a", "b"):
            instance_name = f"executor_kraken_w00000000000{short}"
            instance = MagicMock()
            instance.stop = AsyncMock()
            factory.started_processes[instance_name] = cast(Any, instance)
            self._seed_instance(factory, instance_name)
        await factory.stop_all_processes()
        assert factory.instance_configs == {}


class TestBuildPerWalletInstanceConfig:
    """Pure-merge helper coverage.

    ``_build_per_wallet_instance_config`` merges a registry entry, the
    parsed template Setting JSON, and the per-instance overlay into a
    single ``ProcessConfigModel``. The tests cover registry-only
    fallback, template inheritance for allowed fields, override
    rejection for registry-fixed fields, and the wallet-overlay
    sanitization of leaked ``wallet_public_id`` keys.
    """

    _WALLET_A = "00000000-0000-7000-8000-0000000000a1"

    def _make_factory(self) -> ProcessLauncherService:
        settings = MagicMock()
        settings.db_url = "sqlite+aiosqlite:///:memory:"
        return ProcessLauncherService(settings)

    def _make_entry(
        self,
        *,
        class_path: str = "test.PaperExecutor",
        method: str = "start",
        mode: str = "thread",
        parameters_schema: JsonObject | None = None,
        role: ProcessRoleEnum = ProcessRoleEnum.CORE,
        lifecycle: ProcessLifecycleEnum = ProcessLifecycleEnum.LONG_RUNNING,
        tags: tuple[str, ...] = ("execution", "orders"),
    ) -> ProcessRegistryEntry:
        return ProcessRegistryEntry(
            class_ref=cast(Any, MagicMock()),
            class_path=class_path,
            method=method,
            description="paper executor template",
            priority=30,
            lifecycle=lifecycle,
            role=role,
            tags=tags,
            parameters_model=None,
            parameters_schema=parameters_schema,
            enabled=True,
            mode=cast(Any, mode),
        )

    def test_no_template_uses_registry_defaults(self) -> None:
        """Empty template_config → registry defaults + wallet overlay."""
        factory = self._make_factory()
        entry = self._make_entry(parameters_schema={"type": "object"})
        config = factory._build_per_wallet_instance_config(
            exchange="paper",
            wallet_public_id=self._WALLET_A,
            entry=entry,
            template_config={},
        )
        assert config.name == "executor_paper_w0000000000a1"
        assert config.enabled is True
        assert config.mode == "thread"
        assert config.class_path == "test.PaperExecutor"
        assert config.method == "start"
        assert config.parameters == {"wallet_public_id": self._WALLET_A}
        assert config.note == (f"Per-wallet executor for exchange=paper wallet={self._WALLET_A}")
        assert config.lifecycle is ProcessLifecycleEnum.LONG_RUNNING
        assert config.role is ProcessRoleEnum.CORE
        assert config.tags == ("execution", "orders")
        assert config.parameters_schema == {"type": "object"}

    def test_template_parameters_merged(self) -> None:
        """Template params propagate, wallet overlay wins on collision."""
        factory = self._make_factory()
        entry = self._make_entry()
        config = factory._build_per_wallet_instance_config(
            exchange="kraken",
            wallet_public_id=self._WALLET_A,
            entry=entry,
            template_config={"parameters": {"throttle": 5, "max_qty": 1.5}},
        )
        assert config.parameters == {
            "throttle": 5,
            "max_qty": 1.5,
            "wallet_public_id": self._WALLET_A,
        }

    def test_template_leaked_wallet_id_stripped(self) -> None:
        """Template ``parameters.wallet_public_id`` is stripped before overlay."""
        factory = self._make_factory()
        entry = self._make_entry()
        leaked = "leaked-wallet-id"
        config = factory._build_per_wallet_instance_config(
            exchange="kraken",
            wallet_public_id=self._WALLET_A,
            entry=entry,
            template_config={
                "parameters": {"throttle": 5, "wallet_public_id": leaked},
            },
        )
        assert config.parameters == {"throttle": 5, "wallet_public_id": self._WALLET_A}

    def test_template_parameters_non_dict_falls_back_empty(self) -> None:
        """Malformed template parameters (list) → ignored, only wallet overlay."""
        factory = self._make_factory()
        entry = self._make_entry()
        config = factory._build_per_wallet_instance_config(
            exchange="kraken",
            wallet_public_id=self._WALLET_A,
            entry=entry,
            template_config={"parameters": ["not", "a", "dict"]},
        )
        assert config.parameters == {"wallet_public_id": self._WALLET_A}

    def test_class_path_override_rejected(self) -> None:
        """Template ``class`` override is dropped; registry class_path wins."""
        factory = self._make_factory()
        entry = self._make_entry(class_path="snapper.executors.RegistryClass")
        config = factory._build_per_wallet_instance_config(
            exchange="kraken",
            wallet_public_id=self._WALLET_A,
            entry=entry,
            template_config={"class": "evil.Hacked"},
        )
        assert config.class_path == "snapper.executors.RegistryClass"

    def test_class_path_alias_override_rejected(self) -> None:
        """Template ``class_path`` (alias key) override also dropped."""
        factory = self._make_factory()
        entry = self._make_entry(class_path="snapper.executors.RegistryClass")
        config = factory._build_per_wallet_instance_config(
            exchange="kraken",
            wallet_public_id=self._WALLET_A,
            entry=entry,
            template_config={"class_path": "evil.Hacked"},
        )
        assert config.class_path == "snapper.executors.RegistryClass"

    def test_method_override_rejected(self) -> None:
        """Template ``method`` override dropped; registry method wins."""
        factory = self._make_factory()
        entry = self._make_entry(method="start")
        config = factory._build_per_wallet_instance_config(
            exchange="kraken",
            wallet_public_id=self._WALLET_A,
            entry=entry,
            template_config={"method": "shutdown_now"},
        )
        assert config.method == "start"

    def test_role_override_rejected(self) -> None:
        """Template ``role`` override dropped; registry role wins."""
        factory = self._make_factory()
        entry = self._make_entry(role=ProcessRoleEnum.CORE)
        config = factory._build_per_wallet_instance_config(
            exchange="kraken",
            wallet_public_id=self._WALLET_A,
            entry=entry,
            template_config={"role": "task"},
        )
        assert config.role is ProcessRoleEnum.CORE

    def test_lifecycle_override_rejected(self) -> None:
        """Template ``lifecycle`` override dropped; registry lifecycle wins."""
        factory = self._make_factory()
        entry = self._make_entry(lifecycle=ProcessLifecycleEnum.LONG_RUNNING)
        config = factory._build_per_wallet_instance_config(
            exchange="kraken",
            wallet_public_id=self._WALLET_A,
            entry=entry,
            template_config={"lifecycle": "one_shot"},
        )
        assert config.lifecycle is ProcessLifecycleEnum.LONG_RUNNING

    def test_tags_override_rejected(self) -> None:
        """Template ``tags`` override dropped; registry tags win."""
        factory = self._make_factory()
        entry = self._make_entry(tags=("execution", "orders"))
        config = factory._build_per_wallet_instance_config(
            exchange="kraken",
            wallet_public_id=self._WALLET_A,
            entry=entry,
            template_config={"tags": ["evil-tag"]},
        )
        assert config.tags == ("execution", "orders")

    def test_template_note_overrides_default(self) -> None:
        """Non-empty template note replaces auto-generated description."""
        factory = self._make_factory()
        entry = self._make_entry()
        config = factory._build_per_wallet_instance_config(
            exchange="kraken",
            wallet_public_id=self._WALLET_A,
            entry=entry,
            template_config={"note": "Bumped throttle for peak hours"},
        )
        assert config.note == "Bumped throttle for peak hours"

    def test_template_empty_note_falls_back_default(self) -> None:
        """Empty template note → default per-wallet description used."""
        factory = self._make_factory()
        entry = self._make_entry()
        config = factory._build_per_wallet_instance_config(
            exchange="kraken",
            wallet_public_id=self._WALLET_A,
            entry=entry,
            template_config={"note": ""},
        )
        assert config.note == (f"Per-wallet executor for exchange=kraken wallet={self._WALLET_A}")

    def test_template_non_string_note_falls_back_default(self) -> None:
        """Non-string template note → default per-wallet description used."""
        factory = self._make_factory()
        entry = self._make_entry()
        config = factory._build_per_wallet_instance_config(
            exchange="kraken",
            wallet_public_id=self._WALLET_A,
            entry=entry,
            template_config={"note": 12345},
        )
        assert config.note == (f"Per-wallet executor for exchange=kraken wallet={self._WALLET_A}")

    def test_template_mode_overrides_registry(self) -> None:
        """Template ``mode`` overrides registry default."""
        factory = self._make_factory()
        entry = self._make_entry(mode="thread")
        config = factory._build_per_wallet_instance_config(
            exchange="kraken",
            wallet_public_id=self._WALLET_A,
            entry=entry,
            template_config={"mode": "process"},
        )
        assert config.mode == "process"

    def test_invalid_mode_falls_back_to_registry(self) -> None:
        """Invalid template mode → warning logged + registry mode used."""
        factory = self._make_factory()
        entry = self._make_entry(mode="thread")
        config = factory._build_per_wallet_instance_config(
            exchange="kraken",
            wallet_public_id=self._WALLET_A,
            entry=entry,
            template_config={"mode": "garbage"},
        )
        assert config.mode == "thread"

    def test_template_schema_overrides_registry(self) -> None:
        """Template parameters_schema overrides registry default."""
        factory = self._make_factory()
        entry = self._make_entry(parameters_schema={"type": "object", "from": "registry"})
        config = factory._build_per_wallet_instance_config(
            exchange="kraken",
            wallet_public_id=self._WALLET_A,
            entry=entry,
            template_config={"parameters_schema": {"type": "object", "from": "template"}},
        )
        assert config.parameters_schema == {"type": "object", "from": "template"}

    def test_template_non_dict_schema_falls_back_to_registry(self) -> None:
        """Malformed parameters_schema → registry value used."""
        factory = self._make_factory()
        entry = self._make_entry(parameters_schema={"type": "object", "from": "registry"})
        config = factory._build_per_wallet_instance_config(
            exchange="kraken",
            wallet_public_id=self._WALLET_A,
            entry=entry,
            template_config={"parameters_schema": "not-a-dict"},
        )
        assert config.parameters_schema == {"type": "object", "from": "registry"}

    def test_enabled_always_true(self) -> None:
        """Per-wallet instances are always ``enabled=True`` regardless of template."""
        factory = self._make_factory()
        entry = self._make_entry()
        config = factory._build_per_wallet_instance_config(
            exchange="kraken",
            wallet_public_id=self._WALLET_A,
            entry=entry,
            template_config={"enabled": False},
        )
        assert config.enabled is True

    def test_instance_name_uses_lowercase_12_hex_suffix(self) -> None:
        """Instance suffix is the last 12 lowercase hex chars of the wallet UUID.

        The last 12 chars are the random portion of UUID7; using them
        avoids the deterministic same-millisecond collision that the
        first-12 (timestamp) portion has.
        """
        factory = self._make_factory()
        entry = self._make_entry()
        config = factory._build_per_wallet_instance_config(
            exchange="kraken",
            wallet_public_id="ABCDEF12-3456-7890-ABCD-EF0123456789",
            entry=entry,
            template_config={},
        )
        assert config.name == "executor_kraken_wef0123456789"


class TestLoadTemplateSetting:
    """``_load_template_setting`` async DB helper coverage."""

    def _make_factory(self) -> ProcessLauncherService:
        settings = MagicMock()
        settings.db_url = "sqlite+aiosqlite:///:memory:"
        return ProcessLauncherService(settings)

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_setting_row(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Missing Setting row → empty dict (caller treats as registry-only)."""
        factory = self._make_factory()
        repo = _DummyRepository(setting=None)
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_repository",
            lambda _url: repo,
        )
        result = await factory._load_template_setting("executor_kraken")
        assert result == {}

    @pytest.mark.asyncio
    async def test_returns_parsed_dict_when_setting_present(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Active Setting row → parsed JSON dict returned."""
        factory = self._make_factory()
        setting = Setting(
            key="process_executor_kraken",
            value=json.dumps({"parameters": {"throttle": 7}, "note": "tuning"}),
            session_id="test-session",
            sequence_id=1,
        )
        repo = _DummyRepository(setting=setting)
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_repository",
            lambda _url: repo,
        )
        result = await factory._load_template_setting("executor_kraken")
        assert result == {"parameters": {"throttle": 7}, "note": "tuning"}

    @pytest.mark.asyncio
    async def test_returns_empty_on_invalid_json(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Malformed JSON in Setting value → empty dict + warning."""
        factory = self._make_factory()
        setting = Setting(
            key="process_executor_kraken",
            value="not-json{",
            session_id="test-session",
            sequence_id=1,
        )
        repo = _DummyRepository(setting=setting)
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_repository",
            lambda _url: repo,
        )
        result = await factory._load_template_setting("executor_kraken")
        assert result == {}

    @pytest.mark.asyncio
    async def test_returns_empty_when_top_level_not_dict(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Top-level JSON list (not object) → empty dict + warning."""
        factory = self._make_factory()
        setting = Setting(
            key="process_executor_kraken",
            value=json.dumps(["not", "a", "dict"]),
            session_id="test-session",
            sequence_id=1,
        )
        repo = _DummyRepository(setting=setting)
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_repository",
            lambda _url: repo,
        )
        result = await factory._load_template_setting("executor_kraken")
        assert result == {}


def _settings_with_profile(profile: str) -> AppSettings:
    """Build AppSettings whose bootstrap carries the given autostart profile."""
    bootstrap = BootstrapSettingsLoader(PROCESS_AUTOSTART_PROFILE=profile)
    return AppSettings(bootstrap, _DummySettingsService())


def _publisher_config(name: str = "kraken_equities_feed_publisher") -> ProcessConfigModel:
    """A market-data publisher config (CORE, long-running, dual-tagged)."""
    return ProcessConfigModel(
        name=name,
        enabled=True,
        mode="thread",
        class_path="test.Publisher",
        method="start",
        parameters={},
        role=ProcessRoleEnum.CORE,
        lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
        tags=("market-data", "publisher", "kraken_equities"),
    )


def _non_publisher_config(name: str = "zmq_broker") -> ProcessConfigModel:
    """A non-publisher CORE config (broker-style tags)."""
    return ProcessConfigModel(
        name=name,
        enabled=True,
        mode="thread",
        class_path="test.Broker",
        method="start",
        parameters={},
        role=ProcessRoleEnum.CORE,
        lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
        tags=("zmq", "broker", "infrastructure"),
    )


class TestIsMarketDataPublisher:
    """The tag predicate that the autostart profiles split on."""

    def test_true_when_both_tags_present(self) -> None:
        """Both ``market-data`` and ``publisher`` tags → True."""
        assert is_market_data_publisher(("market-data", "publisher")) is True

    def test_true_with_extra_venue_tag(self) -> None:
        """A superset (with venue tag) still qualifies."""
        assert is_market_data_publisher(("market-data", "publisher", "walutomat")) is True

    def test_false_when_only_market_data(self) -> None:
        """The ``market-data`` tag alone is insufficient."""
        assert is_market_data_publisher(("market-data",)) is False

    def test_false_when_only_publisher(self) -> None:
        """The ``publisher`` tag alone is insufficient."""
        assert is_market_data_publisher(("publisher",)) is False

    def test_false_when_empty(self) -> None:
        """An untagged process is never a market-data publisher."""
        assert is_market_data_publisher(()) is False


class TestAutostartIncludes:
    """Profile-driven inclusion predicate on the launcher."""

    def test_all_profile_includes_everything(self) -> None:
        """``ALL`` selects both publishers and non-publishers."""
        factory = ProcessLauncherService(_settings_with_profile("all"))
        assert factory.autostart_includes(_publisher_config()) is True
        assert factory.autostart_includes(_non_publisher_config()) is True

    def test_api_profile_excludes_publishers(self) -> None:
        """``API`` excludes market-data publishers, keeps the rest."""
        factory = ProcessLauncherService(_settings_with_profile("api"))
        assert factory.autostart_includes(_publisher_config()) is False
        assert factory.autostart_includes(_non_publisher_config()) is True

    def test_feed_profile_includes_only_publishers(self) -> None:
        """``FEED`` selects only market-data publishers."""
        factory = ProcessLauncherService(_settings_with_profile("feed"))
        assert factory.autostart_includes(_publisher_config()) is True
        assert factory.autostart_includes(_non_publisher_config()) is False

    def test_mocked_settings_falls_through_to_all(self) -> None:
        """A non-enum profile attribute (mocked settings) defaults to ALL."""
        factory = ProcessLauncherService(MagicMock())
        assert factory.autostart_includes(_publisher_config()) is True
        assert factory.autostart_includes(_non_publisher_config()) is True


class TestStartAllProcessesProfileFilter:
    """``start_all_processes`` honours the autostart profile."""

    @pytest.mark.asyncio
    async def test_api_profile_skips_publishers(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Under ``API`` only the non-publisher is started."""
        factory = ProcessLauncherService(_settings_with_profile("api"))
        publisher = _publisher_config()
        broker = _non_publisher_config()
        monkeypatch.setattr(
            factory, "get_process_configs", mock.AsyncMock(return_value=[publisher, broker])
        )
        start_mock = mock.AsyncMock()
        monkeypatch.setattr(factory, "start_process", start_mock)
        monkeypatch.setattr(factory, "_start_native_process_monitoring", mock.MagicMock())
        await factory.start_all_processes()
        start_mock.assert_awaited_once_with(broker)

    @pytest.mark.asyncio
    async def test_feed_profile_skips_non_publishers(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Under ``FEED`` only the publisher is started."""
        factory = ProcessLauncherService(_settings_with_profile("feed"))
        publisher = _publisher_config()
        broker = _non_publisher_config()
        monkeypatch.setattr(
            factory, "get_process_configs", mock.AsyncMock(return_value=[publisher, broker])
        )
        start_mock = mock.AsyncMock()
        monkeypatch.setattr(factory, "start_process", start_mock)
        monkeypatch.setattr(factory, "_start_native_process_monitoring", mock.MagicMock())
        await factory.start_all_processes()
        start_mock.assert_awaited_once_with(publisher)


@pytest.mark.asyncio()
async def test_get_core_health_profile_filtered_publisher_ignored(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A publisher deliberately not started under ``API`` is not an error.

    Given: ``API`` profile with an enabled CORE long-running publisher
        that is filtered out of autostart and absent from
        ``started_processes``,
    When: get_core_health is called,
    Then: Returns "healthy" — the publisher belongs to the feed
        container, so the backend not running it is intentional.
    """
    factory = ProcessLauncherService(_settings_with_profile("api"))
    publisher = _publisher_config()
    monkeypatch.setattr(factory, "get_process_configs", mock.AsyncMock(return_value=[publisher]))
    assert await factory.get_core_health() == "healthy"


def _make_proc_info(name: str, returncode: int) -> ProcessInstanceInfo:
    """Build a ProcessInstanceInfo whose subprocess reports a fixed returncode.

    Args:
        name: Process name for the instance.
        returncode: The subprocess return code to expose.

    Returns:
        A ProcessInstanceInfo wired with a stub Popen exposing returncode.
    """
    return ProcessInstanceInfo(
        name=name,
        pid=4242,
        started_at=datetime.now(UTC),
        config={},
        process=cast(subprocess.Popen[bytes], SimpleNamespace(returncode=returncode)),
    )


def _watchdog_config(
    name: str = "kraken_equities_feed_publisher",
    *,
    role: ProcessRoleEnum = ProcessRoleEnum.CORE,
    lifecycle: ProcessLifecycleEnum = ProcessLifecycleEnum.LONG_RUNNING,
    restart_policy: ProcessRestartPolicyEnum = ProcessRestartPolicyEnum.ALWAYS,
    tags: tuple[str, ...] = ("market-data", "publisher", "kraken_equities"),
    enabled: bool = True,
) -> ProcessConfigModel:
    """Build a process config for watchdog tests with sensible defaults.

    Args:
        name: Process name.
        role: Process role (CORE for escalation paths).
        lifecycle: Lifecycle (LONG_RUNNING by default).
        restart_policy: Restart policy under test.
        tags: Tags (market-data publisher tags by default).
        enabled: Whether the config is enabled.

    Returns:
        A ProcessConfigModel for the watchdog under test.
    """
    return ProcessConfigModel(
        name=name,
        enabled=enabled,
        mode="process",
        class_path="test.Publisher",
        method="start",
        parameters={},
        role=role,
        lifecycle=lifecycle,
        restart_policy=restart_policy,
        tags=tags,
    )


def _arm_watchdog(
    factory: ProcessLauncherService,
    config: ProcessConfigModel,
    *,
    uptime_start: float = 0.0,
) -> None:
    """Register a process with the watchdog as if it had been started.

    Sets desired-state RUNNING and the respawn config + uptime origin so a
    subsequent death is reconciled, without going through start_process.

    Args:
        factory: The launcher under test.
        config: The config to arm.
        uptime_start: The monotonic uptime origin to record.
    """
    factory._desired_state[config.name] = _DesiredState.RUNNING
    factory._restart_configs[config.name] = config
    factory._restart_uptime_start[config.name] = uptime_start


@pytest.fixture(autouse=False)
def _no_real_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    """Patch asyncio.sleep in the launcher to return immediately.

    Keeps _delayed_restart deterministic with no wall-clock waits.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """

    async def _instant(_delay: float) -> None:
        return None

    monkeypatch.setattr(launcher_module.asyncio, "sleep", _instant)


class TestStartFeedPublishers:
    """The dedicated feed-container entrypoint on the launcher."""

    @pytest.mark.asyncio
    async def test_starts_only_publishers_forced_to_process_mode(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Only enabled market-data publishers start, each forced PROCESS.

        Given: a config list with a (THREAD-registered) publisher, a
            non-publisher broker, and a disabled publisher,
        When: start_feed_publishers runs,
        Then: only the enabled publisher is started, with mode coerced
            to PROCESS regardless of its registered THREAD mode.
        """
        factory = ProcessLauncherService(MagicMock())
        publisher = _publisher_config()
        broker = _non_publisher_config()
        disabled = ProcessConfigModel(
            name="walutomat_feed_publisher",
            enabled=False,
            mode="thread",
            class_path="test.Publisher",
            method="start",
            parameters={},
            role=ProcessRoleEnum.CORE,
            lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
            tags=("market-data", "publisher", "walutomat"),
        )
        started: list[ProcessConfigModel] = []

        async def _capture(config: ProcessConfigModel) -> None:
            started.append(config)

        monkeypatch.setattr(
            factory,
            "get_process_configs",
            mock.AsyncMock(return_value=[publisher, broker, disabled]),
        )
        monkeypatch.setattr(factory, "start_process", _capture)
        monitor = mock.MagicMock()
        monkeypatch.setattr(factory, "_start_native_process_monitoring", monitor)
        await factory.start_feed_publishers()
        assert [c.name for c in started] == [publisher.name]
        assert started[0].mode == ProcessModeEnum.PROCESS
        monitor.assert_called_once()

    @pytest.mark.asyncio
    async def test_core_publisher_failure_escalates(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A failing CORE long-running publisher raises CoreProcessStartupError.

        Given: a single CORE long-running publisher whose start_process raises,
        When: start_feed_publishers runs,
        Then: CoreProcessStartupError is raised.
        """
        factory = ProcessLauncherService(MagicMock())
        publisher = _publisher_config()
        monkeypatch.setattr(
            factory, "get_process_configs", mock.AsyncMock(return_value=[publisher])
        )
        monkeypatch.setattr(
            factory, "start_process", mock.AsyncMock(side_effect=RuntimeError("boom"))
        )
        monkeypatch.setattr(factory, "_start_native_process_monitoring", mock.MagicMock())
        with pytest.raises(CoreProcessStartupError):
            await factory.start_feed_publishers()

    @pytest.mark.asyncio
    async def test_non_core_publisher_failure_does_not_escalate(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A failing non-CORE publisher is logged but does not raise.

        Given: a single TASK publisher whose start_process raises,
        When: start_feed_publishers runs,
        Then: no exception is raised (only CORE failures escalate).
        """
        factory = ProcessLauncherService(MagicMock())
        task_publisher = ProcessConfigModel(
            name="paper_feed_publisher",
            enabled=True,
            mode="thread",
            class_path="test.Publisher",
            method="start",
            parameters={},
            role=ProcessRoleEnum.TASK,
            lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
            tags=("market-data", "publisher", "paper"),
        )
        monkeypatch.setattr(
            factory, "get_process_configs", mock.AsyncMock(return_value=[task_publisher])
        )
        monkeypatch.setattr(
            factory, "start_process", mock.AsyncMock(side_effect=RuntimeError("boom"))
        )
        monkeypatch.setattr(factory, "_start_native_process_monitoring", mock.MagicMock())
        await factory.start_feed_publishers()

    @pytest.mark.asyncio
    async def test_wait_for_feed_publisher_failure_returns_recorded_name(self) -> None:
        """wait_for_feed_publisher_failure resolves with the escalated name.

        Given: the failure event is set with a recorded publisher name,
        When: wait_for_feed_publisher_failure is awaited,
        Then: it returns that name without blocking.
        """
        factory = ProcessLauncherService(MagicMock())
        factory._feed_failed_publisher = "kraken_equities_feed_publisher"
        factory._feed_failure_event.set()
        result = await asyncio.wait_for(factory.wait_for_feed_publisher_failure(), timeout=1.0)
        assert result == "kraken_equities_feed_publisher"


class TestRestartHelpers:
    """Module-level deterministic helpers used by the watchdog."""

    def test_monotonic_returns_float(self) -> None:
        """_monotonic returns a float clock reading.

        Given: the module-level clock indirection,
        When: _monotonic is called,
        Then: a float is returned.
        """
        assert isinstance(launcher_module._monotonic(), float)

    def test_jitter_is_additive_and_bounded(self) -> None:
        """_jitter is non-negative and below the jitter fraction of base.

        Given: a process name and a base delay,
        When: _jitter is computed,
        Then: it lies in [0, base * fraction) and is deterministic per name.
        """
        base = 8.0
        jitter = launcher_module._jitter("kraken", base)
        assert 0.0 <= jitter < base * launcher_module._RESTART_JITTER_FRACTION
        assert launcher_module._jitter("kraken", base) == jitter

    def test_jitter_is_de_correlated_across_names(self) -> None:
        """_jitter differs across names so a fleet does not stampede.

        Given: two distinct names with the same base,
        When: _jitter is computed for each,
        Then: the jitters differ (de-correlated by crc32).
        """
        base = 16.0
        assert launcher_module._jitter("kraken", base) != launcher_module._jitter("walutomat", base)

    def test_backoff_grows_exponentially(self) -> None:
        """_compute_backoff_delay grows by the factor per attempt.

        Given: increasing attempt counts below the cap,
        When: the backoff is computed,
        Then: each delay is at least the exponential term for that attempt.
        """
        d0 = launcher_module._compute_backoff_delay("kraken", 0)
        d1 = launcher_module._compute_backoff_delay("kraken", 1)
        assert d0 >= launcher_module._RESTART_BASE_DELAY_S
        assert d1 >= launcher_module._RESTART_BASE_DELAY_S * launcher_module._RESTART_FACTOR

    def test_backoff_is_capped_plus_jitter(self) -> None:
        """_compute_backoff_delay caps the exponential term, then adds jitter.

        Given: a large attempt count that overflows the cap,
        When: the backoff is computed,
        Then: the delay equals the cap plus the additive jitter and never
            exceeds cap * (1 + jitter fraction).
        """
        attempts = 50
        delay = launcher_module._compute_backoff_delay("kraken", attempts)
        cap = launcher_module._RESTART_MAX_DELAY_S
        expected = cap + launcher_module._jitter("kraken", cap)
        assert delay == expected
        assert cap <= delay < cap * (1 + launcher_module._RESTART_JITTER_FRACTION)


class TestMaybeScheduleRestart:
    """Policy + escalation accounting in _maybe_schedule_restart."""

    @pytest.mark.asyncio
    async def test_desired_stopped_suppresses_restart(self) -> None:
        """A death while desired=STOPPED schedules no restart and clears nothing.

        Given: a process whose desired state is STOPPED (a deliberate stop
            owns cleanup),
        When: _maybe_schedule_restart runs for a FAILED death,
        Then: no restart task is scheduled.
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config()
        _arm_watchdog(factory, config)
        factory._desired_state[config.name] = _DesiredState.STOPPED
        await factory._maybe_schedule_restart(config.name, ProcessRunStatusEnum.FAILED)
        assert config.name not in factory._restart_tasks

    @pytest.mark.asyncio
    async def test_expected_termination_suppresses_restart(self) -> None:
        """A death in expected_terminations schedules no restart.

        Given: a RUNNING process listed in expected_terminations,
        When: _maybe_schedule_restart runs,
        Then: no restart task is scheduled (a deliberate stop owns cleanup).
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config()
        _arm_watchdog(factory, config)
        factory.expected_terminations.add(config.name)
        await factory._maybe_schedule_restart(config.name, ProcessRunStatusEnum.FAILED)
        assert config.name not in factory._restart_tasks

    @pytest.mark.asyncio
    async def test_missing_config_returns_without_scheduling(self) -> None:
        """A death with no recorded config schedules no restart.

        Given: a RUNNING desired-state but no snapshot config,
        When: _maybe_schedule_restart runs,
        Then: it returns without scheduling.
        """
        factory = ProcessLauncherService(MagicMock())
        factory._desired_state["ghost"] = _DesiredState.RUNNING
        await factory._maybe_schedule_restart("ghost", ProcessRunStatusEnum.FAILED)
        assert "ghost" not in factory._restart_tasks

    @pytest.mark.asyncio
    async def test_one_shot_never_restarts(self) -> None:
        """A ONE_SHOT process is never restarted and its state is cleared.

        Given: a ONE_SHOT process that completed,
        When: _maybe_schedule_restart runs,
        Then: no restart is scheduled and the watchdog state is cleared.
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(name="oneshot_job", lifecycle=ProcessLifecycleEnum.ONE_SHOT)
        _arm_watchdog(factory, config)
        await factory._maybe_schedule_restart(config.name, ProcessRunStatusEnum.SUCCEEDED)
        assert config.name not in factory._restart_tasks
        assert config.name not in factory._desired_state

    @pytest.mark.asyncio
    async def test_never_policy_does_not_restart(self) -> None:
        """A NEVER-policy process is never restarted and its state is cleared.

        Given: a process registered restart_policy=NEVER that died FAILED,
        When: _maybe_schedule_restart runs,
        Then: no restart is scheduled and the watchdog state is cleared.
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(restart_policy=ProcessRestartPolicyEnum.NEVER)
        _arm_watchdog(factory, config)
        await factory._maybe_schedule_restart(config.name, ProcessRunStatusEnum.FAILED)
        assert config.name not in factory._restart_tasks
        assert config.name not in factory._desired_state

    @pytest.mark.asyncio
    async def test_on_failure_clean_exit_does_not_restart(self) -> None:
        """ON_FAILURE + a non-FAILED death does not restart and clears state.

        Given: an ON_FAILURE process that exited cleanly (SUCCEEDED),
        When: _maybe_schedule_restart runs,
        Then: no restart is scheduled and the watchdog state is cleared.
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(restart_policy=ProcessRestartPolicyEnum.ON_FAILURE)
        _arm_watchdog(factory, config)
        await factory._maybe_schedule_restart(config.name, ProcessRunStatusEnum.SUCCEEDED)
        assert config.name not in factory._restart_tasks
        assert config.name not in factory._desired_state

    @pytest.mark.asyncio
    async def test_on_failure_failed_death_restarts(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """ON_FAILURE + a FAILED death schedules a restart and counts it.

        Given: an ON_FAILURE process that died FAILED with short uptime,
        When: _maybe_schedule_restart runs,
        Then: a restart task is scheduled and both counters increment.
        """
        factory = ProcessLauncherService(MagicMock())
        monkeypatch.setattr(launcher_module, "_monotonic", lambda: 10.0)
        config = _watchdog_config(restart_policy=ProcessRestartPolicyEnum.ON_FAILURE)
        _arm_watchdog(factory, config, uptime_start=0.0)
        await factory._maybe_schedule_restart(config.name, ProcessRunStatusEnum.FAILED)
        task = factory._restart_tasks[config.name]
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        assert factory._restart_attempts[config.name] == 1
        assert factory._total_failed_restarts[config.name] == 1

    @pytest.mark.asyncio
    async def test_always_clean_exit_restarts_without_counting(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """ALWAYS + a clean exit-0 restarts but touches NEITHER counter.

        Given: an ALWAYS process that exited cleanly (SUCCEEDED) with short
            uptime,
        When: _maybe_schedule_restart runs,
        Then: a restart is scheduled but no escalation counter is set (the
            paper false-escalation hole stays closed).
        """
        factory = ProcessLauncherService(MagicMock())
        monkeypatch.setattr(launcher_module, "_monotonic", lambda: 10.0)
        config = _watchdog_config(restart_policy=ProcessRestartPolicyEnum.ALWAYS)
        _arm_watchdog(factory, config, uptime_start=0.0)
        await factory._maybe_schedule_restart(config.name, ProcessRunStatusEnum.SUCCEEDED)
        task = factory._restart_tasks[config.name]
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        assert config.name not in factory._restart_attempts
        assert config.name not in factory._total_failed_restarts

    @pytest.mark.asyncio
    async def test_always_clean_exit_long_healthy_resets_counters(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A clean ALWAYS exit after a long healthy run zeroes both counters.

        Given: an ALWAYS process with prior accumulated counters that exited
            cleanly after a > 20 min healthy uptime,
        When: _maybe_schedule_restart runs,
        Then: both counters are reset to 0 (recovery is demonstrated).
        """
        factory = ProcessLauncherService(MagicMock())
        monkeypatch.setattr(launcher_module, "_monotonic", lambda: 1300.0)
        config = _watchdog_config(restart_policy=ProcessRestartPolicyEnum.ALWAYS)
        _arm_watchdog(factory, config, uptime_start=0.0)
        factory._restart_attempts[config.name] = 3
        factory._total_failed_restarts[config.name] = 5
        await factory._maybe_schedule_restart(config.name, ProcessRunStatusEnum.SUCCEEDED)
        task = factory._restart_tasks[config.name]
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        assert factory._restart_attempts[config.name] == 0
        assert factory._total_failed_restarts[config.name] == 0

    @pytest.mark.asyncio
    async def test_healthy_uptime_resets_consecutive_attempts(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A FAILED death after a healthy uptime zeroes consecutive attempts.

        Given: a FAILED death after uptime > _RESTART_HEALTHY_UPTIME_S with
            prior accumulated consecutive attempts,
        When: _maybe_schedule_restart runs,
        Then: _restart_attempts resets to 0 (fresh backoff budget) while
            _total_failed_restarts still increments.
        """
        factory = ProcessLauncherService(MagicMock())
        monkeypatch.setattr(launcher_module, "_monotonic", lambda: 200.0)
        config = _watchdog_config(restart_policy=ProcessRestartPolicyEnum.ALWAYS)
        _arm_watchdog(factory, config, uptime_start=0.0)
        factory._restart_attempts[config.name] = 4
        factory._total_failed_restarts[config.name] = 4
        await factory._maybe_schedule_restart(config.name, ProcessRunStatusEnum.FAILED)
        task = factory._restart_tasks[config.name]
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        assert factory._restart_attempts[config.name] == 0
        assert factory._total_failed_restarts[config.name] == 5

    @pytest.mark.asyncio
    async def test_double_schedule_suppressed_by_lock(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A live pending restart task suppresses a second schedule.

        Given: a FAILED death that scheduled a restart task still asleep in
            its backoff (not done),
        When: a second FAILED death for the same name reaches
            _maybe_schedule_restart before the first task finished,
        Then: the live-task guard returns without replacing the task — the
            same single _delayed_restart stays armed (no two sleeping tasks,
            no double-spawn).
        """
        factory = ProcessLauncherService(MagicMock())
        monkeypatch.setattr(launcher_module, "_monotonic", lambda: 5.0)
        config = _watchdog_config(restart_policy=ProcessRestartPolicyEnum.ALWAYS)
        _arm_watchdog(factory, config, uptime_start=0.0)
        await factory._maybe_schedule_restart(config.name, ProcessRunStatusEnum.FAILED)
        first = factory._restart_tasks[config.name]
        assert not first.done()
        await factory._maybe_schedule_restart(config.name, ProcessRunStatusEnum.FAILED)
        second = factory._restart_tasks[config.name]
        assert second is first
        first.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await first


class TestRestartEscalation:
    """The two-counter escalation model (consecutive + lifetime backstop)."""

    @pytest.mark.asyncio
    async def test_fast_crash_loop_escalates_to_feed_event(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Six consecutive instant FAILED deaths escalate to the feed event.

        Given: a CORE market-data publisher that crashes instantly (uptime 0)
            repeatedly,
        When: _maybe_schedule_restart processes six consecutive FAILED deaths,
        Then: the sixth trips the feed failure event with the publisher name.
        """
        factory = ProcessLauncherService(MagicMock())
        monkeypatch.setattr(launcher_module, "_monotonic", lambda: 0.0)
        config = _watchdog_config(restart_policy=ProcessRestartPolicyEnum.ALWAYS)
        for _ in range(launcher_module._MAX_RESTART_ATTEMPTS):
            _arm_watchdog(factory, config, uptime_start=0.0)
            await factory._maybe_schedule_restart(config.name, ProcessRunStatusEnum.FAILED)
            task = factory._restart_tasks.get(config.name)
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        assert factory._feed_failure_event.is_set()
        assert factory._feed_failed_publisher == config.name

    @pytest.mark.asyncio
    async def test_slow_crash_loop_below_healthy_escalates(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A steady ~95s crasher (below healthy) still escalates at 6 (livelock fix).

        Given: a CORE publisher whose uptime is always 95s (< 120s healthy),
        When: _maybe_schedule_restart processes six consecutive FAILED deaths,
        Then: the feed failure event trips — the slow-crash livelock case is closed.
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(restart_policy=ProcessRestartPolicyEnum.ALWAYS)
        for _ in range(launcher_module._MAX_RESTART_ATTEMPTS):
            monkeypatch.setattr(launcher_module, "_monotonic", lambda: 95.0)
            _arm_watchdog(factory, config, uptime_start=0.0)
            await factory._maybe_schedule_restart(config.name, ProcessRunStatusEnum.FAILED)
            task = factory._restart_tasks.get(config.name)
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        assert factory._feed_failure_event.is_set()
        assert factory._feed_failed_publisher == config.name

    @pytest.mark.asyncio
    async def test_just_above_healthy_escalates_via_ceiling(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A 121s crasher resets attempts each time but escalates via the ceiling.

        Given: a CORE publisher whose uptime is always 121s (just above the
            120s healthy threshold but below the 1200s long-healthy reset),
        When: _maybe_schedule_restart processes 20 FAILED deaths,
        Then: _restart_attempts resets each cycle yet _total_failed_restarts
            climbs to the ceiling and escalates (residual escape closed).
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(restart_policy=ProcessRestartPolicyEnum.ALWAYS)
        for _ in range(launcher_module._MAX_TOTAL_FAILED_RESTARTS):
            monkeypatch.setattr(launcher_module, "_monotonic", lambda: 121.0)
            _arm_watchdog(factory, config, uptime_start=0.0)
            await factory._maybe_schedule_restart(config.name, ProcessRunStatusEnum.FAILED)
            task = factory._restart_tasks.get(config.name)
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        assert factory._feed_failure_event.is_set()
        assert factory._feed_failed_publisher == config.name

    @pytest.mark.asyncio
    async def test_blipping_healthy_venue_never_escalates(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A venue that fails once then runs 1300s clean, x30, never escalates.

        Given: a CORE publisher that fails once then recovers for > 1200s
            healthy uptime, repeated 30 times,
        When: _maybe_schedule_restart processes each FAILED death,
        Then: the long healthy run resets the backstop every cycle and the
            feed failure event never trips (the false-escalation guard holds).
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(restart_policy=ProcessRestartPolicyEnum.ALWAYS)
        for _ in range(30):
            monkeypatch.setattr(launcher_module, "_monotonic", lambda: 1300.0)
            _arm_watchdog(factory, config, uptime_start=0.0)
            await factory._maybe_schedule_restart(config.name, ProcessRunStatusEnum.FAILED)
            task = factory._restart_tasks.get(config.name)
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        assert not factory._feed_failure_event.is_set()
        assert factory._total_failed_restarts[config.name] == 1

    @pytest.mark.asyncio
    async def test_clean_exit_always_never_escalates(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Repeated clean exit-0 under ALWAYS restarts but never escalates.

        Given: a CORE publisher that exits cleanly (SUCCEEDED) instantly,
            repeated well beyond the escalation budget,
        When: _maybe_schedule_restart processes each death,
        Then: neither escalation counter is touched and the feed event stays
            clear (a healthy cleanly-exiting publisher cannot trip the exit).
        """
        factory = ProcessLauncherService(MagicMock())
        monkeypatch.setattr(launcher_module, "_monotonic", lambda: 0.0)
        config = _watchdog_config(restart_policy=ProcessRestartPolicyEnum.ALWAYS)
        for _ in range(launcher_module._MAX_TOTAL_FAILED_RESTARTS + 5):
            _arm_watchdog(factory, config, uptime_start=0.0)
            await factory._maybe_schedule_restart(config.name, ProcessRunStatusEnum.SUCCEEDED)
            task = factory._restart_tasks.get(config.name)
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        assert not factory._feed_failure_event.is_set()
        assert config.name not in factory._total_failed_restarts

    @pytest.mark.asyncio
    async def test_core_non_publisher_gives_up_without_feed_event(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A CORE non-publisher exhausting its budget gives up, no feed event.

        Given: a CORE process WITHOUT market-data publisher tags crashing
            instantly six times,
        When: the budget is exhausted,
        Then: it gives up (logged) without tripping the feed failure event.
        """
        factory = ProcessLauncherService(MagicMock())
        monkeypatch.setattr(launcher_module, "_monotonic", lambda: 0.0)
        config = _watchdog_config(
            name="backend_core",
            restart_policy=ProcessRestartPolicyEnum.ALWAYS,
            tags=("infrastructure",),
        )
        for _ in range(launcher_module._MAX_RESTART_ATTEMPTS):
            _arm_watchdog(factory, config, uptime_start=0.0)
            await factory._maybe_schedule_restart(config.name, ProcessRunStatusEnum.FAILED)
            task = factory._restart_tasks.get(config.name)
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        assert not factory._feed_failure_event.is_set()
        assert config.name not in factory._desired_state

    @pytest.mark.asyncio
    async def test_non_core_publisher_gives_up_without_feed_event(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A non-CORE publisher exhausting its budget gives up, no feed event.

        Given: a TASK-role market-data publisher crashing instantly six times,
        When: the budget is exhausted,
        Then: it gives up without tripping the feed failure event (only CORE
            publishers escalate to the container exit).
        """
        factory = ProcessLauncherService(MagicMock())
        monkeypatch.setattr(launcher_module, "_monotonic", lambda: 0.0)
        config = _watchdog_config(role=ProcessRoleEnum.TASK)
        for _ in range(launcher_module._MAX_RESTART_ATTEMPTS):
            _arm_watchdog(factory, config, uptime_start=0.0)
            await factory._maybe_schedule_restart(config.name, ProcessRunStatusEnum.FAILED)
            task = factory._restart_tasks.get(config.name)
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        assert not factory._feed_failure_event.is_set()
        assert config.name not in factory._desired_state


class TestDelayedRestart:
    """The locked respawn task: stop-during-start, failed-respawn, teardown."""

    @pytest.mark.asyncio
    async def test_respawn_calls_start_process(
        self, monkeypatch: pytest.MonkeyPatch, _no_real_sleep: None
    ) -> None:
        """A delayed restart respawns via start_process while desired=RUNNING.

        Given: a RUNNING process with a recorded enabled config,
        When: _delayed_restart fires (sleep patched instant),
        Then: start_process is awaited with the recorded config.
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(restart_policy=ProcessRestartPolicyEnum.ALWAYS)
        _arm_watchdog(factory, config)
        start_mock = mock.AsyncMock()
        monkeypatch.setattr(factory, "start_process", start_mock)
        await factory._delayed_restart(config.name, 1.0)
        start_mock.assert_awaited_once_with(config)

    @pytest.mark.asyncio
    async def test_cancelled_during_backoff_cancels_task(self) -> None:
        """A cancel during the backoff sleep propagates task cancellation.

        Given: a _delayed_restart task suspended in its backoff sleep,
        When: the task is cancelled,
        Then: it unwinds as cancelled and does not respawn.
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config()
        _arm_watchdog(factory, config)
        start_mock = mock.AsyncMock()
        factory.start_process = start_mock
        task = asyncio.create_task(factory._delayed_restart(config.name, 100.0))
        await asyncio.sleep(0)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        assert task.cancelled()
        start_mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_desired_stopped_skips_respawn(
        self, monkeypatch: pytest.MonkeyPatch, _no_real_sleep: None
    ) -> None:
        """A restart that wakes to desired=STOPPED does not respawn.

        Given: desired state flipped to STOPPED before the locked region,
        When: _delayed_restart fires,
        Then: start_process is not called.
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config()
        _arm_watchdog(factory, config)
        factory._desired_state[config.name] = _DesiredState.STOPPED
        start_mock = mock.AsyncMock()
        monkeypatch.setattr(factory, "start_process", start_mock)
        await factory._delayed_restart(config.name, 1.0)
        start_mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_disabled_config_clears_state(
        self, monkeypatch: pytest.MonkeyPatch, _no_real_sleep: None
    ) -> None:
        """A restart whose config became disabled clears state and skips respawn.

        Given: the recorded config is now disabled,
        When: _delayed_restart fires,
        Then: state is cleared and start_process is not called.
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(enabled=False)
        _arm_watchdog(factory, config)
        start_mock = mock.AsyncMock()
        monkeypatch.setattr(factory, "start_process", start_mock)
        await factory._delayed_restart(config.name, 1.0)
        start_mock.assert_not_awaited()
        assert config.name not in factory._desired_state

    @pytest.mark.asyncio
    async def test_missing_config_clears_state(
        self, monkeypatch: pytest.MonkeyPatch, _no_real_sleep: None
    ) -> None:
        """A restart with no recorded config clears state and skips respawn.

        Given: desired=RUNNING but the snapshot config was dropped,
        When: _delayed_restart fires,
        Then: state is cleared and start_process is not called.
        """
        factory = ProcessLauncherService(MagicMock())
        factory._desired_state["ghost"] = _DesiredState.RUNNING
        start_mock = mock.AsyncMock()
        monkeypatch.setattr(factory, "start_process", start_mock)
        await factory._delayed_restart("ghost", 1.0)
        start_mock.assert_not_awaited()
        assert "ghost" not in factory._desired_state

    @pytest.mark.asyncio
    async def test_stop_during_start_tears_down_just_spawned(
        self, monkeypatch: pytest.MonkeyPatch, _no_real_sleep: None
    ) -> None:
        """A stop that lands during the spawn tears down the just-spawned process.

        Given: start_process registers a live ProcessInstanceInfo and the
            desired state flips to STOPPED before the post-spawn re-check,
        When: _delayed_restart fires,
        Then: the just-spawned process is stopped and cleaned (spawner.cleanup
            called via instance.stop) and not left in started_processes.
        """
        factory = ProcessLauncherService(MagicMock())
        spawner_mock = mock.create_autospec(ProcessSpawnerService, instance=True)
        factory.spawner = cast(ProcessSpawnerService, spawner_mock)
        config = _watchdog_config()
        _arm_watchdog(factory, config)

        async def _fake_start(cfg: ProcessConfigModel) -> None:
            factory.started_processes[cfg.name] = ProcessInstanceInfo(
                name=cfg.name,
                pid=999,
                started_at=datetime.now(UTC),
                config={},
                process=cast(subprocess.Popen[bytes], SimpleNamespace(returncode=None)),
                spawner=factory.spawner,
            )
            factory._desired_state[cfg.name] = _DesiredState.STOPPED

        monkeypatch.setattr(factory, "start_process", _fake_start)
        await factory._delayed_restart(config.name, 1.0)
        spawner_mock.terminate.assert_called_once_with(config.name)
        spawner_mock.cleanup.assert_called_once_with(config.name)
        assert config.name not in factory.started_processes
        assert config.name not in factory._desired_state

    @pytest.mark.asyncio
    async def test_failed_respawn_reschedules_then_escalates(
        self, monkeypatch: pytest.MonkeyPatch, _no_real_sleep: None
    ) -> None:
        """A respawn that keeps raising re-schedules then eventually escalates.

        Given: a CORE publisher whose start_process always raises,
        When: _delayed_restart is driven repeatedly,
        Then: the first failures re-schedule the next backoff (never abandon)
            and the budget exhaustion trips the feed failure event.
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(restart_policy=ProcessRestartPolicyEnum.ALWAYS)
        _arm_watchdog(factory, config)
        monkeypatch.setattr(
            factory, "start_process", mock.AsyncMock(side_effect=RuntimeError("nope"))
        )
        await factory._delayed_restart(config.name, 1.0)
        assert factory._restart_attempts[config.name] == 1
        assert config.name in factory._restart_tasks
        pending = factory._restart_tasks[config.name]
        pending.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await pending
        for _ in range(launcher_module._MAX_RESTART_ATTEMPTS):
            _arm_watchdog(factory, config)
            await factory._delayed_restart(config.name, 1.0)
            task = factory._restart_tasks.get(config.name)
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        assert factory._feed_failure_event.is_set()
        assert factory._feed_failed_publisher == config.name

    @pytest.mark.asyncio
    async def test_delayed_restart_pops_only_own_task(
        self, monkeypatch: pytest.MonkeyPatch, _no_real_sleep: None
    ) -> None:
        """The finally clause pops the tasks entry only when it is the own task.

        Given: a successful respawn whose _restart_tasks entry was replaced by
            an unrelated sentinel before the finally clause runs,
        When: _delayed_restart finishes,
        Then: the sentinel is preserved (only the current task pops itself).
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(restart_policy=ProcessRestartPolicyEnum.ALWAYS)
        _arm_watchdog(factory, config)
        sentinel = asyncio.create_task(asyncio.sleep(100))

        async def _fake_start(_cfg: ProcessConfigModel) -> None:
            factory._restart_tasks[config.name] = sentinel

        monkeypatch.setattr(factory, "start_process", _fake_start)
        await factory._delayed_restart(config.name, 1.0)
        assert factory._restart_tasks.get(config.name) is sentinel
        sentinel.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await sentinel


class TestTeardownStartedProcess:
    """The lock-free internal stop primitive."""

    @pytest.mark.asyncio
    async def test_teardown_stops_and_drops_instance(self) -> None:
        """Teardown stops the instance and drops it from tracking dicts.

        Given: a tracked ProcessInstanceInfo with a spawner reference,
        When: _teardown_started_process runs,
        Then: the instance is stopped and removed from started_processes.
        """
        factory = ProcessLauncherService(MagicMock())
        spawner_mock = mock.create_autospec(ProcessSpawnerService, instance=True)
        factory.spawner = cast(ProcessSpawnerService, spawner_mock)
        info = ProcessInstanceInfo(
            name="pub",
            pid=1,
            started_at=datetime.now(UTC),
            config={},
            process=cast(subprocess.Popen[bytes], SimpleNamespace(returncode=None)),
            spawner=factory.spawner,
        )
        factory.started_processes["pub"] = info
        factory.process_lifecycles["pub"] = ProcessLifecycleEnum.LONG_RUNNING
        factory.process_roles["pub"] = ProcessRoleEnum.CORE
        await factory._teardown_started_process("pub")
        spawner_mock.terminate.assert_called_once_with("pub")
        assert "pub" not in factory.started_processes
        assert "pub" not in factory.process_lifecycles

    @pytest.mark.asyncio
    async def test_teardown_absent_instance_is_noop(self) -> None:
        """Teardown of an absent process is a clean no-op.

        Given: no tracked instance for the name,
        When: _teardown_started_process runs,
        Then: it returns without error and tracking stays empty.
        """
        factory = ProcessLauncherService(MagicMock())
        await factory._teardown_started_process("missing")
        assert "missing" not in factory.started_processes

    @pytest.mark.asyncio
    async def test_teardown_suppresses_stop_error(self) -> None:
        """A stop that raises is suppressed and the instance is still dropped.

        Given: a tracked instance whose stop raises,
        When: _teardown_started_process runs,
        Then: the error is suppressed and the instance is removed.
        """
        factory = ProcessLauncherService(MagicMock())
        instance = MagicMock()
        instance.stop = AsyncMock(side_effect=RuntimeError("stop boom"))
        factory.started_processes["pub"] = instance
        await factory._teardown_started_process("pub")
        assert "pub" not in factory.started_processes


class TestWatchdogStop:
    """Deliberate stops own cleanup and cancel pending restarts."""

    @pytest.mark.asyncio
    async def test_stop_cancels_pending_restart(self) -> None:
        """stop_process_by_name cancels a pending restart task and clears state.

        Given: a running process with a pending (suspended) restart task,
        When: stop_process_by_name runs,
        Then: the pending restart is cancelled and watchdog state is cleared.
        """
        factory = ProcessLauncherService(MagicMock())
        spawner_mock = mock.create_autospec(ProcessSpawnerService, instance=True)
        factory.spawner = cast(ProcessSpawnerService, spawner_mock)
        config = _watchdog_config(name="pub")
        _arm_watchdog(factory, config)
        instance = MagicMock()
        instance.stop = AsyncMock()
        factory.started_processes["pub"] = instance
        factory.process_lifecycles["pub"] = ProcessLifecycleEnum.LONG_RUNNING
        factory.process_roles["pub"] = ProcessRoleEnum.CORE
        pending = asyncio.create_task(asyncio.sleep(100))
        factory._restart_tasks["pub"] = pending
        factory._finalize_process_run = AsyncMock()
        factory._emit_summary_snapshot = AsyncMock()
        result = await asyncio.wait_for(factory.stop_process_by_name("pub"), timeout=2.0)
        assert result.status == "success"
        assert pending.cancelled()
        assert "pub" not in factory._desired_state
        assert "pub" not in factory._restart_tasks

    @pytest.mark.asyncio
    async def test_stop_sets_desired_stopped_before_await(self) -> None:
        """stop_process_by_name marks desired=STOPPED synchronously.

        Given: a running process,
        When: stop_process_by_name completes,
        Then: the desired-state marker is cleared (STOPPED owned the stop and
            the terminal cleanup removed the entry).
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(name="pub")
        _arm_watchdog(factory, config)
        instance = MagicMock()
        instance.stop = AsyncMock()
        factory.started_processes["pub"] = instance
        factory.process_lifecycles["pub"] = ProcessLifecycleEnum.LONG_RUNNING
        factory.process_roles["pub"] = ProcessRoleEnum.CORE
        factory._finalize_process_run = AsyncMock()
        factory._emit_summary_snapshot = AsyncMock()
        await factory.stop_process_by_name("pub")
        assert "pub" not in factory._desired_state

    @pytest.mark.asyncio
    async def test_stop_while_respawn_in_flight_no_deadlock(self) -> None:
        """Stopping while a respawn holds the lock does not deadlock.

        Given: a _delayed_restart task suspended inside its locked region
            (mid-respawn, awaiting a blocked start_process),
        When: stop_process_by_name is invoked,
        Then: the stop sets desired=STOPPED, cancels the pending task, and
            completes within the timeout (no self-deadlock on the name lock).
        """
        factory = ProcessLauncherService(MagicMock())
        spawner_mock = mock.create_autospec(ProcessSpawnerService, instance=True)
        factory.spawner = cast(ProcessSpawnerService, spawner_mock)
        config = _watchdog_config(name="pub", restart_policy=ProcessRestartPolicyEnum.ALWAYS)
        _arm_watchdog(factory, config)
        instance = MagicMock()
        instance.stop = AsyncMock()
        factory.started_processes["pub"] = instance
        factory.process_lifecycles["pub"] = ProcessLifecycleEnum.LONG_RUNNING
        factory.process_roles["pub"] = ProcessRoleEnum.CORE
        factory._finalize_process_run = AsyncMock()
        factory._emit_summary_snapshot = AsyncMock()
        pending = asyncio.create_task(asyncio.sleep(100))
        factory._restart_tasks["pub"] = pending
        result = await asyncio.wait_for(factory.stop_process_by_name("pub"), timeout=2.0)
        assert result.status == "success"
        assert pending.cancelled()

    @pytest.mark.asyncio
    async def test_stop_not_running_returns_not_running(self) -> None:
        """Stopping an absent process returns NOT_RUNNING.

        Given: a name absent from started_processes,
        When: stop_process_by_name runs,
        Then: it returns NOT_RUNNING.
        """
        factory = ProcessLauncherService(MagicMock())
        result = await factory.stop_process_by_name("absent")
        assert result.status == "not_running"

    @pytest.mark.asyncio
    async def test_stop_error_returns_error_status(self) -> None:
        """A stop whose instance.stop raises returns ERROR.

        Given: a running process whose instance.stop raises,
        When: stop_process_by_name runs,
        Then: it returns ERROR and still discards expected_terminations.
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(name="pub")
        _arm_watchdog(factory, config)
        instance = MagicMock()
        instance.stop = AsyncMock(side_effect=RuntimeError("boom"))
        factory.started_processes["pub"] = instance
        factory.process_lifecycles["pub"] = ProcessLifecycleEnum.LONG_RUNNING
        factory.process_roles["pub"] = ProcessRoleEnum.CORE
        factory._finalize_process_run = AsyncMock()
        result = await factory.stop_process_by_name("pub")
        assert result.status == "error"
        assert "pub" not in factory.expected_terminations


class TestStartProcessWatchdogMarkers:
    """start_process records watchdog markers and retains them on failure."""

    @pytest.mark.asyncio
    async def test_records_config_and_uptime_but_not_desired(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A successful start records config + uptime, NOT desired.

        Given: a config started via start_process (subprocess spawn stubbed),
        When: start_process completes,
        Then: the respawn config snapshot and uptime origin are recorded, but
            start_process does NOT own the desired=RUNNING transition — that
            belongs to the lock-holding callers / boot spawners via
            ``_arm_desired_running``, so a start can never clobber a concurrent
            stop's STOPPED during the stop's pre-lock window.
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(name="pub")
        monkeypatch.setattr(factory, "_try_create_run_record", mock.AsyncMock(return_value=None))
        monkeypatch.setattr(factory, "_start_as_subprocess", mock.MagicMock())
        monkeypatch.setattr(factory, "_finalize_one_shot", mock.AsyncMock())
        monkeypatch.setattr(factory, "_emit_summary_snapshot", mock.AsyncMock())
        await factory.start_process(config)
        assert factory._desired_state.get("pub") is None
        assert factory._restart_configs["pub"] is config
        assert "pub" in factory._restart_uptime_start

    @pytest.mark.asyncio
    async def test_arm_desired_running_sets_running(self) -> None:
        """``_arm_desired_running`` is the lock-owned desired=RUNNING write.

        Given: a launcher with no desired state for a name,
        When: ``_arm_desired_running`` is called,
        Then: the name's desired state becomes RUNNING (the only entry point
            that declares RUNNING, so the transition is lock-ownable).
        """
        factory = ProcessLauncherService(MagicMock())
        factory._arm_desired_running("pub")
        assert factory._desired_state["pub"] is _DesiredState.RUNNING

    @pytest.mark.asyncio
    async def test_start_failure_retains_watchdog_markers(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A startup failure leaves the watchdog markers in place.

        Given: a name already armed desired=RUNNING (a respawn / manual caller
            arms it before calling start_process) whose subprocess spawn raises,
        When: start_process raises,
        Then: the desired/config/uptime markers are RETAINED — the primitive
            never clears watchdog desired-state ownership on failure, so a
            respawn failure cannot clobber a concurrent stop's STOPPED marker.
            The lock-taking manual callers own the first-start-leak cleanup for
            names they armed.
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(name="pub")
        factory._arm_desired_running("pub")
        monkeypatch.setattr(factory, "_try_create_run_record", mock.AsyncMock(return_value=None))
        monkeypatch.setattr(
            factory, "_start_as_subprocess", mock.MagicMock(side_effect=RuntimeError("spawn boom"))
        )
        monkeypatch.setattr(factory, "_handle_start_failure", mock.AsyncMock())
        with pytest.raises(RuntimeError, match="spawn boom"):
            await factory.start_process(config)
        assert factory._desired_state["pub"] is _DesiredState.RUNNING
        assert factory._restart_configs["pub"] is config
        assert "pub" in factory._restart_uptime_start


class TestStartByNameLock:
    """start_process_by_name serializes with the watchdog under the lock."""

    @pytest.mark.asyncio
    async def test_start_by_name_acquires_lock(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """start_process_by_name starts under the per-name lock.

        Given: a configured process resolvable from a Setting,
        When: start_process_by_name runs,
        Then: start_process is awaited (the manual start serializes with a
            watchdog respawn of the same name).
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(name="pub")
        captured: list[ProcessConfigModel] = []

        async def _capture(cfg: ProcessConfigModel) -> None:
            captured.append(cfg)

        monkeypatch.setattr(factory, "_build_config_for_start_by_name", lambda *a, **k: config)
        monkeypatch.setattr(factory, "_apply_overrides_to_config_dict", lambda *a, **k: True)
        monkeypatch.setattr(factory, "start_process", _capture)
        monkeypatch.setattr(factory, "_start_native_process_monitoring", mock.MagicMock())

        setting = SimpleNamespace(value=json.dumps({"class_path": "x", "method": "start"}))

        @asynccontextmanager
        async def _session() -> AsyncIterator[Any]:
            session = MagicMock()
            session.execute = AsyncMock(
                return_value=SimpleNamespace(scalar_one_or_none=lambda: setting)
            )
            yield session

        repo = MagicMock()
        repo.session = _session
        monkeypatch.setattr(launcher_module, "get_repository", lambda _url: repo)
        result = await factory.start_process_by_name("pub")
        assert result.status == "success"
        assert captured == [config]


class TestStopAllSweepsWatchdog:
    """stop_all_processes sweeps live subprocesses and watchdog state."""

    @pytest.mark.asyncio
    async def test_stop_all_cleans_live_process_instance(self) -> None:
        """stop_all_processes stops + cleans a live ProcessInstanceInfo.

        Given: a tracked live ProcessInstanceInfo plus watchdog state,
        When: stop_all_processes runs,
        Then: the instance is stopped, spawner.cleanup is invoked, and all
            watchdog dicts are emptied (no orphan subprocess).
        """
        factory = ProcessLauncherService(MagicMock())
        spawner_mock = mock.create_autospec(ProcessSpawnerService, instance=True)
        factory.spawner = cast(ProcessSpawnerService, spawner_mock)
        config = _watchdog_config(name="pub")
        _arm_watchdog(factory, config)
        info = ProcessInstanceInfo(
            name="pub",
            pid=7,
            started_at=datetime.now(UTC),
            config={},
            process=cast(subprocess.Popen[bytes], SimpleNamespace(returncode=None)),
            spawner=factory.spawner,
        )
        factory.started_processes["pub"] = info
        factory._restart_attempts["pub"] = 2
        factory._emit_summary_snapshot = AsyncMock()
        await factory.stop_all_processes()
        spawner_mock.cleanup.assert_any_call("pub")
        assert factory.started_processes == {}
        assert factory._desired_state == {}
        assert factory._restart_attempts == {}
        assert factory._restart_tasks == {}

    @pytest.mark.asyncio
    async def test_stop_all_cancels_pending_restart_task(self) -> None:
        """stop_all_processes cancels a pending restart task.

        Given: a pending (suspended) restart task and no live instances,
        When: stop_all_processes runs,
        Then: the pending restart is cancelled and the dicts are cleared.
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(name="pub")
        _arm_watchdog(factory, config)
        pending = asyncio.create_task(asyncio.sleep(100))
        factory._restart_tasks["pub"] = pending
        factory._emit_summary_snapshot = AsyncMock()
        await factory.stop_all_processes()
        assert pending.cancelled()
        assert factory._restart_tasks == {}
        assert factory._desired_state == {}


class TestCompletionHookSchedulesRestart:
    """_handle_process_completion drives the watchdog after finalize."""

    @pytest.mark.asyncio
    async def test_completion_schedules_restart_for_failed_always(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A FAILED native death under ALWAYS schedules a restart.

        Given: a RUNNING ALWAYS publisher whose subprocess exited non-zero,
        When: _handle_process_completion runs,
        Then: a restart task is scheduled (the completion hook fires the
            watchdog AFTER finalize).
        """
        factory = ProcessLauncherService(MagicMock())
        monkeypatch.setattr(launcher_module, "_monotonic", lambda: 0.0)
        spawner_mock = mock.create_autospec(ProcessSpawnerService, instance=True)
        factory.spawner = cast(ProcessSpawnerService, spawner_mock)
        config = _watchdog_config(name="pub", restart_policy=ProcessRestartPolicyEnum.ALWAYS)
        _arm_watchdog(factory, config, uptime_start=0.0)
        factory.process_lifecycles["pub"] = ProcessLifecycleEnum.LONG_RUNNING
        factory.process_roles["pub"] = ProcessRoleEnum.CORE
        proc_info = _make_proc_info("pub", 3)
        factory.started_processes["pub"] = proc_info
        monkeypatch.setattr(factory, "_finalize_process_run", mock.AsyncMock())
        monkeypatch.setattr(factory, "_emit_summary_snapshot", mock.AsyncMock())
        await factory._handle_process_completion("pub", proc_info)
        task = factory._restart_tasks.get("pub")
        assert task is not None
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    @pytest.mark.asyncio
    async def test_completion_spares_paper_clean_idle_exit(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """REGRESSION: a CORE ON_FAILURE publisher exiting 0 never restarts.

        The paper publisher is CORE long-running and idle-exits 0 in live
        mode WITHOUT being in expected_terminations. Under ON_FAILURE its
        clean exit resolves to SUCCEEDED and must not restart — the exact
        crash-loop the live feed container hit.

        Given: a CORE ON_FAILURE paper publisher, exit code 0, not expected,
        When: _handle_process_completion runs,
        Then: no restart is scheduled and the feed event stays clear.
        """
        factory = ProcessLauncherService(MagicMock())
        spawner_mock = mock.create_autospec(ProcessSpawnerService, instance=True)
        factory.spawner = cast(ProcessSpawnerService, spawner_mock)
        config = _watchdog_config(
            name="paper_feed_publisher",
            restart_policy=ProcessRestartPolicyEnum.ON_FAILURE,
            tags=("market-data", "publisher", "paper"),
        )
        _arm_watchdog(factory, config, uptime_start=0.0)
        factory.process_lifecycles["paper_feed_publisher"] = ProcessLifecycleEnum.LONG_RUNNING
        factory.process_roles["paper_feed_publisher"] = ProcessRoleEnum.CORE
        proc_info = _make_proc_info("paper_feed_publisher", 0)
        factory.started_processes["paper_feed_publisher"] = proc_info
        monkeypatch.setattr(factory, "_finalize_process_run", mock.AsyncMock())
        monkeypatch.setattr(factory, "_emit_summary_snapshot", mock.AsyncMock())
        await factory._handle_process_completion("paper_feed_publisher", proc_info)
        assert "paper_feed_publisher" not in factory._restart_tasks
        assert not factory._feed_failure_event.is_set()

    @pytest.mark.asyncio
    async def test_completion_expected_stop_does_not_restart(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A graceful (expected) stop does not schedule a restart.

        Given: a RUNNING publisher in expected_terminations exiting cleanly,
        When: _handle_process_completion runs,
        Then: no restart is scheduled (a deliberate stop owns the lifecycle).
        """
        factory = ProcessLauncherService(MagicMock())
        spawner_mock = mock.create_autospec(ProcessSpawnerService, instance=True)
        factory.spawner = cast(ProcessSpawnerService, spawner_mock)
        config = _watchdog_config(name="pub", restart_policy=ProcessRestartPolicyEnum.ALWAYS)
        _arm_watchdog(factory, config, uptime_start=0.0)
        factory.process_lifecycles["pub"] = ProcessLifecycleEnum.LONG_RUNNING
        factory.process_roles["pub"] = ProcessRoleEnum.CORE
        factory.expected_terminations.add("pub")
        proc_info = _make_proc_info("pub", 0)
        factory.started_processes["pub"] = proc_info
        monkeypatch.setattr(factory, "_finalize_process_run", mock.AsyncMock())
        monkeypatch.setattr(factory, "_emit_summary_snapshot", mock.AsyncMock())
        await factory._handle_process_completion("pub", proc_info)
        assert "pub" not in factory._restart_tasks


class TestClearWatchdogState:
    """The single terminal-cleanup helper."""

    def test_clear_removes_all_entries(self) -> None:
        """_clear_watchdog_state drops every watchdog entry except the lock.

        Given: a name populated across all watchdog dicts (lock included),
        When: _clear_watchdog_state runs,
        Then: every per-name dict drops the name EXCEPT _restart_locks — the
            lock is deliberately retained so a caller queued on it can never
            be split onto a second freshly-minted lock. The lock is
            garbage collected only in the fully-drained stop_all_processes
            context.
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(name="pub")
        _arm_watchdog(factory, config)
        factory._restart_attempts["pub"] = 1
        factory._total_failed_restarts["pub"] = 2
        factory._restart_tasks["pub"] = cast("asyncio.Task[None]", SimpleNamespace())
        factory._restart_locks["pub"] = asyncio.Lock()
        factory._clear_watchdog_state("pub")
        assert "pub" not in factory._desired_state
        assert "pub" not in factory._restart_attempts
        assert "pub" not in factory._restart_uptime_start
        assert "pub" not in factory._restart_tasks
        assert "pub" not in factory._restart_configs
        assert "pub" in factory._restart_locks
        assert "pub" not in factory._total_failed_restarts

    def test_clear_drops_metrics_and_psutil_handle(self) -> None:
        """_clear_watchdog_state drops per-child metrics + the psutil handle.

        Given: a name carrying sampled metrics and a live psutil handle,
        When: _clear_watchdog_state runs,
        Then: both the _process_metrics entry and the _psutil_handles entry
            are dropped so a dead process never carries stale RSS/CPU and a
            respawn under the same name mints a fresh handle.
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(name="pub")
        _arm_watchdog(factory, config)
        factory._process_metrics["pub"] = (1024, 7.5)
        factory._psutil_handles["pub"] = cast("psutil.Process", MagicMock())
        factory._clear_watchdog_state("pub")
        assert "pub" not in factory._process_metrics
        assert "pub" not in factory._psutil_handles


class TestSampleProcessMetrics:
    """_sample_process_metrics samples RSS + CPU for native children."""

    def test_sample_populates_rss_and_cpu(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A native child sample populates _process_metrics with rss + cpu.

        Given: one native ProcessInstanceInfo and a patched psutil.Process
            whose handle reports a cpu_percent and memory_info().rss,
        When: _sample_process_metrics runs,
        Then: _process_metrics carries (rss, cpu) and the handle is cached
            keyed by process name.
        """
        factory = ProcessLauncherService(MagicMock())
        proc_info = _make_proc_info("pub", 0)
        factory.started_processes["pub"] = proc_info
        handle = MagicMock()
        handle.pid = proc_info.pid
        handle.cpu_percent.return_value = 33.0
        handle.memory_info.return_value = SimpleNamespace(rss=2048)
        process_factory = MagicMock(return_value=handle)
        monkeypatch.setattr(launcher_module.psutil, "Process", process_factory)
        factory._sample_process_metrics()
        assert factory._process_metrics["pub"] == (2048, 33.0)
        assert factory._psutil_handles["pub"] is handle
        process_factory.assert_called_once_with(proc_info.pid)
        handle.cpu_percent.assert_called_once_with(interval=None)

    def test_first_sample_cpu_is_zero(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The first cpu_percent reading is 0.0 (psutil delta contract).

        Given: a freshly created psutil handle whose first cpu_percent
            call returns 0.0 (no prior reading to delta against),
        When: _sample_process_metrics runs once,
        Then: _process_metrics records cpu_percent == 0.0 with a real rss.
        """
        factory = ProcessLauncherService(MagicMock())
        proc_info = _make_proc_info("pub", 0)
        factory.started_processes["pub"] = proc_info
        handle = MagicMock()
        handle.pid = proc_info.pid
        handle.cpu_percent.return_value = 0.0
        handle.memory_info.return_value = SimpleNamespace(rss=4096)
        monkeypatch.setattr(launcher_module.psutil, "Process", MagicMock(return_value=handle))
        factory._sample_process_metrics()
        assert factory._process_metrics["pub"] == (4096, 0.0)

    def test_pid_change_recreates_handle(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A new pid for the same name recreates the psutil handle.

        Given: a cached handle whose pid differs from the live child's pid
            (a watchdog respawn gave the name a new pid),
        When: _sample_process_metrics runs,
        Then: a fresh handle is constructed for the new pid and cached.
        """
        factory = ProcessLauncherService(MagicMock())
        proc_info = _make_proc_info("pub", 0)
        factory.started_processes["pub"] = proc_info
        stale_handle = MagicMock()
        stale_handle.pid = proc_info.pid + 1
        factory._psutil_handles["pub"] = cast("psutil.Process", stale_handle)
        fresh_handle = MagicMock()
        fresh_handle.pid = proc_info.pid
        fresh_handle.cpu_percent.return_value = 5.0
        fresh_handle.memory_info.return_value = SimpleNamespace(rss=512)
        process_factory = MagicMock(return_value=fresh_handle)
        monkeypatch.setattr(launcher_module.psutil, "Process", process_factory)
        factory._sample_process_metrics()
        process_factory.assert_called_once_with(proc_info.pid)
        assert factory._psutil_handles["pub"] is fresh_handle
        assert factory._process_metrics["pub"] == (512, 5.0)

    def test_reuses_cached_handle_when_pid_matches(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A cached handle with a matching pid is reused, not recreated.

        Given: a cached handle whose pid equals the live child's pid,
        When: _sample_process_metrics runs,
        Then: psutil.Process is not called again and the cached handle is
            sampled in place (preserving the cpu delta baseline).
        """
        factory = ProcessLauncherService(MagicMock())
        proc_info = _make_proc_info("pub", 0)
        factory.started_processes["pub"] = proc_info
        cached_handle = MagicMock()
        cached_handle.pid = proc_info.pid
        cached_handle.cpu_percent.return_value = 21.0
        cached_handle.memory_info.return_value = SimpleNamespace(rss=8192)
        factory._psutil_handles["pub"] = cast("psutil.Process", cached_handle)
        process_factory = MagicMock()
        monkeypatch.setattr(launcher_module.psutil, "Process", process_factory)
        factory._sample_process_metrics()
        process_factory.assert_not_called()
        assert factory._process_metrics["pub"] == (8192, 21.0)

    @pytest.mark.parametrize(
        "error_cls",
        [psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess],
    )
    def test_psutil_error_records_none_and_prunes_handle(
        self,
        error_cls: type[Exception],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A vanished/inaccessible child records (None, None) + prunes handle.

        Given: a native child whose handle raises NoSuchProcess /
            AccessDenied / ZombieProcess on cpu_percent,
        When: _sample_process_metrics runs,
        Then: _process_metrics records (None, None) and the dead handle is
            pruned so a later respawn mints a fresh one.
        """
        factory = ProcessLauncherService(MagicMock())
        proc_info = _make_proc_info("pub", 0)
        factory.started_processes["pub"] = proc_info
        handle = MagicMock()
        handle.pid = proc_info.pid
        handle.cpu_percent.side_effect = error_cls(proc_info.pid)
        monkeypatch.setattr(launcher_module.psutil, "Process", MagicMock(return_value=handle))
        factory._sample_process_metrics()
        assert factory._process_metrics["pub"] == (None, None)
        assert "pub" not in factory._psutil_handles

    def test_thread_mode_process_is_skipped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A non-ProcessInstanceInfo (thread-mode) process gets no metrics.

        Given: a started process that is NOT a ProcessInstanceInfo,
        When: _sample_process_metrics runs,
        Then: no _process_metrics entry is created (its summary row stays
            None) and psutil.Process is never constructed.
        """
        factory = ProcessLauncherService(MagicMock())
        factory.started_processes["worker"] = cast(RegisterableProcess, SimpleNamespace(pid=123))
        process_factory = MagicMock()
        monkeypatch.setattr(launcher_module.psutil, "Process", process_factory)
        factory._sample_process_metrics()
        assert "worker" not in factory._process_metrics
        process_factory.assert_not_called()


class TestMonitorTickSamplesAndEmits:
    """The monitor loop samples metrics + emits a summary each tick."""

    @pytest.mark.asyncio
    async def test_tick_calls_sampler_then_emits_summary(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Each monitor tick samples metrics then emits a summary snapshot.

        Given: one live native process whose status stays running for one
            tick before clearing on the next,
        When: _monitor_native_processes runs,
        Then: _sample_process_metrics is invoked and _emit_summary_snapshot
            is awaited on the tick that detected the live child (the ~5s
            RAM/CPU heartbeat the frontend slice consumes).
        """
        factory = ProcessLauncherService(MagicMock())
        spawner_mock = mock.create_autospec(ProcessSpawnerService, instance=True)
        factory.spawner = cast(ProcessSpawnerService, spawner_mock)
        proc_info = _make_proc_info("pub", 0)
        factory.started_processes["pub"] = proc_info
        tick = {"count": 0}

        def status(_name: str) -> SpawnerStatusSnapshot:
            tick["count"] += 1
            if tick["count"] >= 2:
                factory.started_processes.clear()
            return SpawnerStatusSnapshot(name=_name, running=True)

        spawner_mock.get_status.side_effect = status

        async def sleep_fake(_delay: float) -> None:
            return None

        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.asyncio.sleep", sleep_fake
        )
        sample_mock = MagicMock()
        emit_mock = mock.AsyncMock()
        monkeypatch.setattr(factory, "_sample_process_metrics", sample_mock)
        monkeypatch.setattr(factory, "_emit_summary_snapshot", emit_mock)
        await factory._monitor_native_processes()
        sample_mock.assert_called()
        emit_mock.assert_awaited()


class TestRestartLockFor:
    """The synchronous per-name lock accessor."""

    def test_lock_is_created_and_memoized(self) -> None:
        """_restart_lock_for creates a lock once and returns the same instance.

        Given: a fresh launcher,
        When: _restart_lock_for is called twice for one name,
        Then: the same Lock is returned both times.
        """
        factory = ProcessLauncherService(MagicMock())
        first = factory._restart_lock_for("pub")
        second = factory._restart_lock_for("pub")
        assert isinstance(first, asyncio.Lock)
        assert first is second


class TestMarketDataPublishersRestartPolicyGuard:
    """Registry guard: no market-data publisher is registered NEVER."""

    def test_no_market_data_publisher_is_registered_never(self) -> None:
        """No registered market-data publisher carries restart_policy=NEVER.

        A CORE market-data publisher registered NEVER would neither restart
        nor escalate, silently blocking wait_for_feed_publisher_failure
        forever. This makes that misconfiguration impossible by construction.

        Given: the full process registry after discovery,
        When: every market-data publisher entry is inspected,
        Then: none is registered restart_policy=NEVER.
        """
        discover_processes()
        registry = get_registered_processes()
        offenders = [
            name
            for name, entry in registry.items()
            if is_market_data_publisher(entry.tags)
            and entry.restart_policy is ProcessRestartPolicyEnum.NEVER
        ]
        assert offenders == []


class TestWatchdogStopDuringStartSyncCoverage:
    """Sync-driven anchors for the stop-during-start await-lines.

    The post-spawn teardown await inside _delayed_restart's locked region
    is attributed reliably by coverage's thread tracer when the coroutine
    is driven from a synchronous frame via ``asyncio.run`` rather than an
    already-running loop, so this sync test keeps those lines covered
    under parallel (xdist) runs.
    """

    def test_stop_during_start_teardown_via_asyncio_run(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Driving the stop-during-start path via asyncio.run covers the teardown.

        Given: a respawn that registers a live instance then flips desired
            to STOPPED before the post-spawn re-check,
        When: _delayed_restart is driven through asyncio.run,
        Then: the just-spawned process is torn down and its state cleared.
        """
        factory = ProcessLauncherService(MagicMock())
        spawner_mock = mock.create_autospec(ProcessSpawnerService, instance=True)
        factory.spawner = cast(ProcessSpawnerService, spawner_mock)
        config = _watchdog_config(restart_policy=ProcessRestartPolicyEnum.ALWAYS)
        _arm_watchdog(factory, config)

        async def _instant(_delay: float) -> None:
            return None

        monkeypatch.setattr(launcher_module.asyncio, "sleep", _instant)

        async def _fake_start(cfg: ProcessConfigModel) -> None:
            factory.started_processes[cfg.name] = ProcessInstanceInfo(
                name=cfg.name,
                pid=999,
                started_at=datetime.now(UTC),
                config={},
                process=cast(subprocess.Popen[bytes], SimpleNamespace(returncode=None)),
                spawner=factory.spawner,
            )
            factory._desired_state[cfg.name] = _DesiredState.STOPPED

        monkeypatch.setattr(factory, "start_process", _fake_start)
        asyncio.run(factory._delayed_restart(config.name, 1.0))
        spawner_mock.terminate.assert_called_once_with(config.name)
        assert config.name not in factory.started_processes
        assert config.name not in factory._desired_state


class TestStopAllDoneRestartTask:
    """stop_all_processes skips an already-completed restart task."""

    @pytest.mark.asyncio
    async def test_stop_all_skips_done_restart_task(self) -> None:
        """A done restart task in _restart_tasks is not cancelled by stop_all.

        Given: a restart task that has already completed,
        When: stop_all_processes runs,
        Then: it skips the done task (no cancel needed) and clears the dicts.
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(name="pub")
        _arm_watchdog(factory, config)
        done_restart = asyncio.create_task(asyncio.sleep(0))
        await done_restart
        factory._restart_tasks["pub"] = done_restart
        factory._emit_summary_snapshot = AsyncMock()
        await factory.stop_all_processes()
        assert factory._restart_tasks == {}
        assert factory._desired_state == {}


class TestDelayedRestartFinallyPop:
    """The finally-clause pops the own task entry on a successful respawn."""

    @pytest.mark.asyncio
    async def test_own_task_entry_is_popped_on_success(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A respawn whose own task is the registered entry pops it in finally.

        Given: _delayed_restart launched as a real task and registered as
            _restart_tasks[name] (so asyncio.current_task() matches),
        When: the respawn succeeds,
        Then: the finally clause pops the own entry (line guarded by the
            current-task identity check executes).
        """
        factory = ProcessLauncherService(MagicMock())

        async def _instant(_delay: float) -> None:
            return None

        monkeypatch.setattr(launcher_module.asyncio, "sleep", _instant)
        config = _watchdog_config(restart_policy=ProcessRestartPolicyEnum.ALWAYS)
        _arm_watchdog(factory, config)
        monkeypatch.setattr(factory, "start_process", mock.AsyncMock())
        task = asyncio.create_task(factory._delayed_restart(config.name, 1.0))
        factory._restart_tasks[config.name] = task
        await task
        assert config.name not in factory._restart_tasks


class _RaceLock:
    """A per-name lock that simulates a watchdog respawn winning a race.

    On ``__aenter__`` it deterministically inserts ``name`` into the
    shared ``started_processes`` mapping, modelling a watchdog respawn
    that started the process while a manual caller was waiting to acquire
    the lock. This drives the in-lock re-check branch without
    spawning helper tasks, so the test leaks no pending coroutine.
    """

    def __init__(self, started: dict[str, Any], name: str) -> None:
        """Store the shared registry and the name to mark as running.

        Args:
            started: The launcher's ``started_processes`` mapping.
            name: The process name the simulated respawn starts.
        """
        self._started = started
        self._name = name
        self._inner = asyncio.Lock()

    async def __aenter__(self) -> _RaceLock:
        """Acquire the lock and mark the process as already running.

        Returns:
            The lock instance, after simulating the respawn win.
        """
        await self._inner.acquire()
        self._started[self._name] = MagicMock()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        """Release the inner lock and propagate any exception.

        Args:
            exc_type: Exception type raised in the context, if any.
            exc: Exception instance raised in the context, if any.
            tb: Traceback for the exception, if any.

        Returns:
            ``False`` so any exception propagates unchanged.
        """
        self._inner.release()
        return False


class TestWatchdogConcurrencyRegressions:
    """Focused regressions for the six watchdog concurrency fixes.

    Each test drives one of the exact races identified on the
    production feed-container watchdog: double-schedule, abandoned
    failed-respawn, lock-pop split, manual-start TOCTOU,
    stop-during-backoff, and terminal-state leak on non-native paths.
    """

    @pytest.mark.asyncio()
    async def test_fix1_pending_restart_blocks_second_schedule(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A live pending restart task suppresses a second schedule.

        Given: a FAILED death scheduled a restart task still asleep in its
            backoff (not done), with create_task tracked,
        When: a second FAILED death reaches _maybe_schedule_restart before
            the first task finishes,
        Then: the live-task guard returns without creating a second
            _delayed_restart and the stored task ref is unchanged — exactly
            one spawn path stays armed (no double-spawn).
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(name="pub")
        _arm_watchdog(factory, config)
        created: list[asyncio.Task[None]] = []
        real_create_task = launcher_module.asyncio.create_task

        def _track(coro: Any) -> asyncio.Task[None]:
            task: asyncio.Task[None] = real_create_task(coro)
            created.append(task)
            return task

        async def _never(_delay: float) -> None:
            await asyncio.Event().wait()

        monkeypatch.setattr(launcher_module.asyncio, "create_task", _track)
        monkeypatch.setattr(launcher_module.asyncio, "sleep", _never)
        await factory._maybe_schedule_restart("pub", ProcessRunStatusEnum.FAILED)
        first = factory._restart_tasks["pub"]
        assert not first.done()
        await factory._maybe_schedule_restart("pub", ProcessRunStatusEnum.FAILED)
        assert factory._restart_tasks["pub"] is first
        assert len(created) == 1
        first.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await first

    @pytest.mark.asyncio()
    async def test_fix2_failed_respawn_rearms_so_next_attempt_proceeds(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A failed respawn reschedules so the next attempt spawns.

        Given: start_process RETAINS desired/config/uptime on a failing
            call (as the production primitive now does — it no longer
            clears watchdog ownership on failure) then succeeds,
        When: _delayed_restart runs and the first respawn raises below the
            escalation ceiling with desired still RUNNING,
        Then: the failed-respawn branch confirms desired is still RUNNING,
            bumps the counters, refreshes the uptime origin, and schedules
            the next attempt, and that next _delayed_restart proceeds past
            the desired/config guard and actually respawns rather than
            silently exiting (the CORE publisher is not abandoned).
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(name="pub")
        _arm_watchdog(factory, config)
        _stub_run_tracking(factory)
        calls: list[str] = []

        async def _flaky_start(cfg: ProcessConfigModel) -> None:
            calls.append(cfg.name)
            if len(calls) == 1:
                raise RuntimeError("first respawn boom")
            factory.started_processes[cfg.name] = MagicMock()

        monkeypatch.setattr(factory, "start_process", _flaky_start)
        monkeypatch.setattr(launcher_module.asyncio, "sleep", AsyncMock())
        await factory._delayed_restart("pub", 0.0)
        next_task = factory._restart_tasks.get("pub")
        assert next_task is not None
        await next_task
        assert calls == ["pub", "pub"]
        assert "pub" in factory.started_processes
        assert not factory._feed_failure_event.is_set()

    @pytest.mark.asyncio()
    async def test_fix3_clear_preserves_lock_identity_for_waiter(self) -> None:
        """clear_watchdog_state keeps the lock so a waiter is not split.

        Given: a per-name lock that a caller currently holds, with another
            coroutine queued to acquire it,
        When: _clear_watchdog_state runs while the lock is held,
        Then: the lock object is NOT popped, so _restart_lock_for returns
            the SAME live lock — the queued waiter and any later caller share
            one lock and mutual exclusion is preserved (no double-spawn).
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(name="pub")
        _arm_watchdog(factory, config)
        lock = factory._restart_lock_for("pub")
        order: list[str] = []

        async with lock:
            order.append("holder-in")

            async def _waiter() -> None:
                async with factory._restart_lock_for("pub"):
                    order.append("waiter-in")

            waiter_task = asyncio.create_task(_waiter())
            await asyncio.sleep(0)
            factory._clear_watchdog_state("pub")
            assert factory._restart_lock_for("pub") is lock
            order.append("holder-out")
        await waiter_task
        assert order == ["holder-in", "holder-out", "waiter-in"]
        assert factory._restart_lock_for("pub") is lock

    @pytest.mark.asyncio()
    async def test_fix4_manual_start_rechecks_running_inside_lock(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Manual start re-checks started_processes inside the lock.

        Given: a manual start_process_by_name that passes the pre-lock
            not-running check, then blocks on the per-name lock held by a
            simulated watchdog respawn which inserts the process into
            started_processes before releasing,
        When: the manual start finally acquires the lock,
        Then: the in-lock re-check sees the name now running and returns
            ALREADY_RUNNING without a second start_process call — no
            double-spawn — and the pending restart task was cancelled first
            (before the lock was awaited).
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(name="zmq_broker", role=ProcessRoleEnum.CORE)
        started: list[str] = []

        async def _fake_start(cfg: ProcessConfigModel) -> None:
            started.append(cfg.name)

        cancelled = asyncio.Event()

        async def _pending() -> None:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise

        pending = asyncio.create_task(_pending())
        await asyncio.sleep(0)
        factory._restart_tasks["zmq_broker"] = pending
        race_lock = _RaceLock(factory.started_processes, "zmq_broker")
        monkeypatch.setattr(factory, "start_process", _fake_start)
        monkeypatch.setattr(factory, "_build_config_for_start_by_name", lambda *a, **k: config)
        monkeypatch.setattr(factory, "_apply_overrides_to_config_dict", lambda *a, **k: True)
        monkeypatch.setattr(factory, "_start_native_process_monitoring", lambda: None)
        monkeypatch.setattr(factory, "_restart_lock_for", lambda _name: race_lock)
        repo = MagicMock()
        session = MagicMock()
        setting = MagicMock()
        setting.value = json.dumps({"class": "test.Publisher", "method": "run"})
        session.execute = AsyncMock(return_value=MagicMock())
        session.execute.return_value.scalar_one_or_none.return_value = setting
        repo.session.return_value.__aenter__.return_value = session
        repo.session.return_value.__aexit__ = AsyncMock(return_value=False)
        monkeypatch.setattr(launcher_module, "get_repository", lambda _url: repo)
        result = await factory.start_process_by_name("zmq_broker")
        assert result.status == "already_running"
        assert started == []
        assert cancelled.is_set()
        assert "zmq_broker" not in factory._restart_tasks

    @pytest.mark.asyncio()
    async def test_fix4_per_wallet_start_rechecks_running_inside_lock(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Per-wallet manual start re-checks running inside the lock.

        Given: a per-wallet start that passes the pre-lock not-running check,
            then blocks on the per-name lock held by a simulated watchdog
            respawn which inserts the instance into started_processes before
            releasing,
        When: the per-wallet start finally acquires the lock,
        Then: the in-lock re-check sees the instance now running and returns
            ALREADY_RUNNING without calling start_process (no double-spawn),
            restoring the prior instance config rather than overwriting it.
        """
        name = "executor_kraken_w0123456789ab"
        settings = MagicMock()
        settings.db_url = "sqlite+aiosqlite:///:memory:"
        factory = ProcessLauncherService(settings)
        prior = _watchdog_config(name=name)
        factory.instance_configs[name] = prior
        entry = ProcessRegistryEntry(
            class_ref=cast(Any, MagicMock()),
            class_path="test.KrakenExecutor",
            method="run",
            description="",
            priority=0,
            lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
            role=ProcessRoleEnum.CORE,
            tags=("market-data", "publisher"),
            parameters_model=None,
            parameters_schema=None,
            enabled=True,
            mode="process",
        )
        monkeypatch.setattr(
            launcher_module, "get_registered_processes", lambda: {"executor_kraken": entry}
        )
        credential = {"exchange": "kraken", "wallet_public_id": "wallet-0123456789ab"}
        repo = MagicMock()
        repo.list_active_wallet_credentials = AsyncMock(return_value=[credential])
        monkeypatch.setattr(launcher_module, "get_repository", lambda _url: repo)
        monkeypatch.setattr(factory, "_load_template_setting", AsyncMock(return_value={}))
        monkeypatch.setattr(
            factory,
            "_build_per_wallet_instance_config",
            lambda **kwargs: _watchdog_config(name=name),
        )
        started: list[str] = []

        async def _fake_start(cfg: ProcessConfigModel) -> None:
            started.append(cfg.name)

        monkeypatch.setattr(factory, "start_process", _fake_start)
        monkeypatch.setattr(factory, "_start_native_process_monitoring", lambda: None)
        race_lock = _RaceLock(factory.started_processes, name)
        monkeypatch.setattr(factory, "_restart_lock_for", lambda _name: race_lock)
        result = await factory.start_per_wallet_instance_by_name(name)
        assert result.status == "already_running"
        assert started == []
        assert factory.instance_configs[name] is prior

    @pytest.mark.asyncio()
    async def test_fix4_per_wallet_in_lock_recheck_without_prior_config(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """In-lock recheck with no prior instance config still short-circuits.

        Given: a per-wallet start for an instance NOT previously registered
            in instance_configs (so prior_instance_config is None); the
            per-name lock is replaced by a _RaceLock that marks the instance
            running on acquire,
        When: the per-wallet start acquires the lock,
        Then: the in-lock re-check returns ALREADY_RUNNING without calling
            start_process and without a prior-config restore (the no-prior
            branch).
        """
        name = "executor_kraken_w0123456789ab"
        settings = MagicMock()
        settings.db_url = "sqlite+aiosqlite:///:memory:"
        factory = ProcessLauncherService(settings)
        entry = ProcessRegistryEntry(
            class_ref=cast(Any, MagicMock()),
            class_path="test.KrakenExecutor",
            method="run",
            description="",
            priority=0,
            lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
            role=ProcessRoleEnum.CORE,
            tags=("market-data", "publisher"),
            parameters_model=None,
            parameters_schema=None,
            enabled=True,
            mode="process",
        )
        monkeypatch.setattr(
            launcher_module, "get_registered_processes", lambda: {"executor_kraken": entry}
        )
        credential = {"exchange": "kraken", "wallet_public_id": "wallet-0123456789ab"}
        repo = MagicMock()
        repo.list_active_wallet_credentials = AsyncMock(return_value=[credential])
        monkeypatch.setattr(launcher_module, "get_repository", lambda _url: repo)
        monkeypatch.setattr(factory, "_load_template_setting", AsyncMock(return_value={}))
        instance_config = _watchdog_config(name=name)
        monkeypatch.setattr(
            factory,
            "_build_per_wallet_instance_config",
            lambda **kwargs: instance_config,
        )
        started: list[str] = []

        async def _fake_start(cfg: ProcessConfigModel) -> None:
            started.append(cfg.name)

        monkeypatch.setattr(factory, "start_process", _fake_start)
        monkeypatch.setattr(factory, "_start_native_process_monitoring", lambda: None)
        race_lock = _RaceLock(factory.started_processes, name)
        monkeypatch.setattr(factory, "_restart_lock_for", lambda _name: race_lock)
        result = await factory.start_per_wallet_instance_by_name(name)
        assert result.status == "already_running"
        assert started == []
        assert factory.instance_configs[name] is instance_config

    @pytest.mark.asyncio()
    async def test_fix5_stop_wins_when_process_already_gone(self) -> None:
        """A stop cancels a pending respawn even with no live process.

        Given: a process already gone from started_processes but with
            desired=RUNNING and a pending restart task still queued (a
            respawn scheduled while the old process died),
        When: stop_process_by_name runs,
        Then: desired flips STOPPED and the pending restart is cancelled
            BEFORE the not_running early-return, so the respawn never fires;
            the result is NOT_RUNNING and the watchdog state is cleared.
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(name="pub")
        _arm_watchdog(factory, config)
        _stub_run_tracking(factory)

        async def _never(_d: float) -> None:
            await asyncio.Event().wait()

        pending = asyncio.create_task(_never(0.0))
        factory._restart_tasks["pub"] = pending
        result = await factory.stop_process_by_name("pub")
        assert result.status == "not_running"
        assert pending.cancelled()
        assert "pub" not in factory._restart_tasks
        assert "pub" not in factory._desired_state
        assert "pub" not in factory._restart_configs

    @pytest.mark.asyncio()
    async def test_fix6_one_shot_completion_clears_watchdog_state(self) -> None:
        """A completed non-native ONE_SHOT clears its watchdog state.

        Given: a ONE_SHOT thread-mode process whose markers start_process
            armed before spawning,
        When: _finalize_one_shot runs after the one-shot completed,
        Then: the watchdog state is cleared so a ONE_SHOT never leaks
            desired/config/uptime entries and can never be respawned.
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(
            name="updater",
            role=ProcessRoleEnum.CORE,
            lifecycle=ProcessLifecycleEnum.ONE_SHOT,
            tags=(),
        )
        config.mode = "thread"
        _arm_watchdog(factory, config)
        _stub_run_tracking(factory)
        await factory._finalize_one_shot(config)
        assert "updater" not in factory._desired_state
        assert "updater" not in factory._restart_configs
        assert "updater" not in factory._restart_uptime_start

    @pytest.mark.asyncio()
    async def test_fix6_task_completion_drives_watchdog_restart(self) -> None:
        """A died asyncio-task process is respawned by the watchdog.

        Given: a thread/async-task process with watchdog markers armed
            whose LONG_RUNNING task completed unexpectedly,
        When: _handle_task_completion runs on its finished task,
        Then: a restart is scheduled through the same machinery as native
            subprocesses — previously task processes were finalized and
            silently never respawned (a died executor meant a permanent
            invisible order-execution outage).
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(name="job", role=ProcessRoleEnum.CORE)
        _arm_watchdog(factory, config)
        _stub_run_tracking(factory)
        schedule_spy = MagicMock()
        cast(Any, factory)._schedule_delayed_restart = schedule_spy

        async def _done() -> None:
            return None

        task = asyncio.create_task(_done())
        await task
        factory.process_tasks["job"] = task
        factory.process_lifecycles["job"] = ProcessLifecycleEnum.LONG_RUNNING
        await factory._handle_task_completion("job", task)
        schedule_spy.assert_called_once()
        assert factory._desired_state.get("job") is _DesiredState.RUNNING

    @pytest.mark.asyncio()
    async def test_fix6_task_restart_runs_before_tracking_cleanup(self) -> None:
        """The restart decision consumes expected_terminations pre-cleanup.

        Given: a watchdog-armed task process whose name sits in
            expected_terminations (a deliberate stop in flight) while its
            task FAILED,
        When: _handle_task_completion runs,
        Then: no restart is scheduled — which can only hold when
            _maybe_schedule_restart reads expected_terminations BEFORE
            _cleanup_task_tracking discards it.
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(name="job", role=ProcessRoleEnum.CORE)
        _arm_watchdog(factory, config)
        _stub_run_tracking(factory)
        schedule_spy = MagicMock()
        cast(Any, factory)._schedule_delayed_restart = schedule_spy

        async def _boom() -> None:
            raise RuntimeError("died during stop")

        task = asyncio.create_task(_boom())
        with contextlib.suppress(RuntimeError):
            await task
        factory.process_tasks["job"] = task
        factory.process_lifecycles["job"] = ProcessLifecycleEnum.LONG_RUNNING
        factory.expected_terminations.add("job")
        await factory._handle_task_completion("job", task)
        schedule_spy.assert_not_called()
        assert "job" not in factory.expected_terminations

    @pytest.mark.asyncio()
    async def test_fix6_failed_task_restarts_via_start_process(self) -> None:
        """A FAILED task death flows through to a fresh start_process.

        Given: an armed watchdog, zero backoff, and a task that raised,
        When: completion handling schedules and the delayed restart runs,
        Then: start_process is awaited once with the stored config.
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(name="job", role=ProcessRoleEnum.CORE)
        _arm_watchdog(factory, config)
        _stub_run_tracking(factory)
        cast(Any, factory)._compute_backoff_delay = MagicMock(return_value=0.0)
        start_spy = mock.AsyncMock(return_value=True)
        cast(Any, factory).start_process = start_spy

        async def _boom() -> None:
            raise RuntimeError("task died")

        task = asyncio.create_task(_boom())
        with contextlib.suppress(RuntimeError):
            await task
        factory.process_tasks["job"] = task
        factory.process_lifecycles["job"] = ProcessLifecycleEnum.LONG_RUNNING
        await factory._handle_task_completion("job", task)
        restart_task = factory._restart_tasks.get("job")
        assert restart_task is not None
        await restart_task
        start_spy.assert_awaited_once()
        assert start_spy.await_args is not None
        assert start_spy.await_args.args[0] is config

    @pytest.mark.asyncio()
    async def test_fix6_cancelled_deliberate_stop_never_restarts(self) -> None:
        """A deliberate stop's cancellation schedules no restart.

        Given: desired=STOPPED with the name in expected_terminations and
            a cancelled task (the deliberate-stop shape),
        When: _handle_task_completion runs,
        Then: nothing is scheduled and desired stays STOPPED — ownership
            clearing belongs to the stop call itself, exactly like the
            native-subprocess path.
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(name="job", role=ProcessRoleEnum.CORE)
        _arm_watchdog(factory, config)
        _stub_run_tracking(factory)
        factory._desired_state["job"] = _DesiredState.STOPPED
        factory.expected_terminations.add("job")
        schedule_spy = MagicMock()
        cast(Any, factory)._schedule_delayed_restart = schedule_spy

        async def _forever() -> None:
            await asyncio.Event().wait()

        task = asyncio.create_task(_forever())
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        factory.process_tasks["job"] = task
        factory.process_lifecycles["job"] = ProcessLifecycleEnum.LONG_RUNNING
        await factory._handle_task_completion("job", task)
        schedule_spy.assert_not_called()
        assert factory._desired_state.get("job") is _DesiredState.STOPPED

    @pytest.mark.asyncio()
    async def test_r2_1_failed_respawn_does_not_resurrect_a_stopped_process(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A failing respawn that races a stop neither resurrects nor orphans.

        Given: a watchdog-managed publisher whose respawn is in flight; the
            respawning start_process RAISES, and while it runs a concurrent
            stop flips desired=STOPPED (modelling the interleaving where the
            stop set STOPPED before the respawn's failure path resumes),
        When: _delayed_restart's failed-respawn branch resumes,
        Then: it re-checks desired, sees it is no longer RUNNING, clears the
            watchdog state and returns WITHOUT re-arming desired=RUNNING and
            WITHOUT scheduling a successor task — so the stopped process is
            never resurrected and no orphan retry task is left behind.
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(name="pub")
        _arm_watchdog(factory, config)
        _stub_run_tracking(factory)
        monkeypatch.setattr(launcher_module.asyncio, "sleep", AsyncMock())

        async def _stop_wins_then_fail(cfg: ProcessConfigModel) -> None:
            factory._desired_state[cfg.name] = _DesiredState.STOPPED
            raise RuntimeError("respawn boom")

        monkeypatch.setattr(factory, "start_process", _stop_wins_then_fail)
        await factory._delayed_restart("pub", 0.0)
        assert "pub" not in factory._restart_tasks
        assert "pub" not in factory._desired_state
        assert "pub" not in factory._restart_configs
        assert "pub" not in factory.started_processes
        assert not factory._feed_failure_event.is_set()

    @pytest.mark.asyncio()
    async def test_r2_1_cancel_pending_restart_cancels_successor_task(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A stop's cancel loops until the successor task is also gone.

        Given: a pending restart task T1 that, when cancelled+awaited,
            installs a fresh successor task T2 into _restart_tasks[name]
            (modelling a failed-respawn re-schedule that lands during the
            stop's cancel/await window), AND a third already-completed task
            T3 that the loop must pop-and-skip without cancelling,
        When: _cancel_pending_restart runs,
        Then: it loops — cancelling T1, observing the live successor T2 and
            cancelling it too, then popping the already-done T3 via the
            not-done False branch — so no live task remains for the name
            (no orphan retry survives the stop).
        """
        factory = ProcessLauncherService(MagicMock())
        successor_cancelled = asyncio.Event()

        async def _done_task() -> None:
            return None

        async def _successor() -> None:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                done_task = asyncio.create_task(_done_task())
                await done_task
                factory._restart_tasks["pub"] = done_task
                successor_cancelled.set()
                raise

        async def _first() -> None:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                factory._restart_tasks["pub"] = asyncio.create_task(_successor())
                raise

        first = asyncio.create_task(_first())
        await asyncio.sleep(0)
        factory._restart_tasks["pub"] = first
        await factory._cancel_pending_restart("pub")
        assert "pub" not in factory._restart_tasks
        assert first.cancelled()
        assert successor_cancelled.is_set()

    @pytest.mark.asyncio()
    async def test_r2_2_duplicate_completion_while_pending_is_noop(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A duplicate FAILED completion while a restart sleeps is a no-op.

        Given: a watchdog-managed publisher with a live pending restart task
            (still asleep in its backoff) and the escalation counters parked
            one tick below the ceiling,
        When: a second FAILED completion reaches _maybe_schedule_restart,
        Then: the live-task guard at the TOP returns before any counter or
            escalation mutation — the counters are unchanged, no escalation
            fires, and the stored task ref is the same (no budget burn, no
            spurious container trip).
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(name="pub")
        _arm_watchdog(factory, config)
        factory._restart_attempts["pub"] = launcher_module._MAX_RESTART_ATTEMPTS - 1
        factory._total_failed_restarts["pub"] = 1

        async def _never(_delay: float) -> None:
            await asyncio.Event().wait()

        monkeypatch.setattr(launcher_module.asyncio, "sleep", _never)
        await factory._maybe_schedule_restart("pub", ProcessRunStatusEnum.FAILED)
        first = factory._restart_tasks["pub"]
        assert not first.done()
        attempts_after_first = factory._restart_attempts["pub"]
        total_after_first = factory._total_failed_restarts["pub"]
        await factory._maybe_schedule_restart("pub", ProcessRunStatusEnum.FAILED)
        assert factory._restart_tasks["pub"] is first
        assert factory._restart_attempts["pub"] == attempts_after_first
        assert factory._total_failed_restarts["pub"] == total_after_first
        assert not factory._feed_failure_event.is_set()
        first.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await first

    @pytest.mark.asyncio()
    async def test_r2_3_cancel_mid_start_finalizes_run_record(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A cancel landing mid-start_process finalizes the run record.

        Given: start_process that has created the active run record and is
            suspended in the spawn region (before the process is live) when
            its task is cancelled — CancelledError is a BaseException that
            bypasses the except Exception cleanup,
        When: the task is cancelled and awaited,
        Then: the finally clause finalizes the dangling active run (drops it
            from active_runs and records a terminal status) so no RUNNING run
            record leaks.
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(name="pub")
        _stub_run_tracking(factory)
        finalized: list[ProcessRunStatusEnum] = []
        real_finalize = factory._finalize_process_run

        async def _spy_finalize(name: str, status: ProcessRunStatusEnum, **kwargs: Any) -> None:
            finalized.append(status)
            await real_finalize(name, status, **kwargs)

        monkeypatch.setattr(factory, "_finalize_process_run", _spy_finalize)
        spawn_started = asyncio.Event()

        async def _slow_create(_cfg: ProcessConfigModel) -> str:
            factory.active_runs["pub"] = "run-id"
            factory.active_run_started_at["pub"] = datetime.now(UTC)
            return "run-id"

        async def _block_in_spawn(_cfg: ProcessConfigModel) -> None:
            spawn_started.set()
            await asyncio.Event().wait()

        monkeypatch.setattr(factory, "_try_create_run_record", _slow_create)
        monkeypatch.setattr(factory, "_start_in_process", _block_in_spawn)
        config.mode = "thread"
        task = asyncio.create_task(factory.start_process(config))
        await spawn_started.wait()
        assert "pub" in factory.active_runs
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        assert "pub" not in factory.active_runs
        assert finalized == [ProcessRunStatusEnum.FAILED]

    @pytest.mark.asyncio()
    async def test_r2_4_failed_manual_start_keeps_managed_name_recoverable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A failed manual start of a managed name stays recoverable.

        Given: a watchdog-managed publisher (desired=RUNNING + config armed)
            with a pending restart during backoff; an operator triggers a
            manual start_process_by_name which cancels the pending restart,
            then start_process RAISES,
        When: start_process_by_name returns ERROR,
        Then: because the name was already watchdog-managed before the manual
            start, the watchdog markers are NOT cleared — desired stays
            RUNNING and the config is retained — and recovery is re-armed
            with a fresh pending restart task so the publisher is not
            left permanently disarmed (it can still be recovered) rather than
            left dead with no desired/config.
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(name="zmq_broker", role=ProcessRoleEnum.CORE)
        _arm_watchdog(factory, config)

        async def _failing_start(_cfg: ProcessConfigModel) -> None:
            raise RuntimeError("manual start boom")

        async def _never() -> None:
            await asyncio.Event().wait()

        pending = asyncio.create_task(_never())
        await asyncio.sleep(0)
        factory._restart_tasks["zmq_broker"] = pending
        monkeypatch.setattr(factory, "start_process", _failing_start)
        monkeypatch.setattr(factory, "_build_config_for_start_by_name", lambda *a, **k: config)
        monkeypatch.setattr(factory, "_apply_overrides_to_config_dict", lambda *a, **k: True)
        monkeypatch.setattr(factory, "_start_native_process_monitoring", lambda: None)
        repo = MagicMock()
        session = MagicMock()
        setting = MagicMock()
        setting.value = json.dumps({"class": "test.Publisher", "method": "run"})
        session.execute = AsyncMock(return_value=MagicMock())
        session.execute.return_value.scalar_one_or_none.return_value = setting
        repo.session.return_value.__aenter__.return_value = session
        repo.session.return_value.__aexit__ = AsyncMock(return_value=False)
        monkeypatch.setattr(launcher_module, "get_repository", lambda _url: repo)
        result = await factory.start_process_by_name("zmq_broker")
        assert result.status == "error"
        assert factory._desired_state["zmq_broker"] is _DesiredState.RUNNING
        assert factory._restart_configs["zmq_broker"] is config
        rearmed = factory._restart_tasks["zmq_broker"]
        assert rearmed is not pending
        assert not rearmed.done()
        rearmed.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await rearmed

    @pytest.mark.asyncio()
    async def test_r2_4_failed_manual_start_of_new_name_clears_markers(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A failed manual start of a brand-new name leaks no markers.

        Given: a name that is NOT watchdog-managed before the manual start;
            the manual start_process_by_name calls start_process which arms
            fresh markers then RAISES (start_process no longer clears them),
        When: start_process_by_name returns ERROR,
        Then: because the name was not managed before, the manual caller owns
            the first-start-leak cleanup and clears the watchdog markers — no
            desired/config leak for a never-managed name.
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(name="zmq_broker", role=ProcessRoleEnum.CORE)

        async def _arm_then_fail(cfg: ProcessConfigModel) -> None:
            factory._desired_state[cfg.name] = _DesiredState.RUNNING
            factory._restart_configs[cfg.name] = cfg
            factory._restart_uptime_start[cfg.name] = 0.0
            raise RuntimeError("manual start boom")

        monkeypatch.setattr(factory, "start_process", _arm_then_fail)
        monkeypatch.setattr(factory, "_build_config_for_start_by_name", lambda *a, **k: config)
        monkeypatch.setattr(factory, "_apply_overrides_to_config_dict", lambda *a, **k: True)
        monkeypatch.setattr(factory, "_start_native_process_monitoring", lambda: None)
        repo = MagicMock()
        session = MagicMock()
        setting = MagicMock()
        setting.value = json.dumps({"class": "test.Publisher", "method": "run"})
        session.execute = AsyncMock(return_value=MagicMock())
        session.execute.return_value.scalar_one_or_none.return_value = setting
        repo.session.return_value.__aenter__.return_value = session
        repo.session.return_value.__aexit__ = AsyncMock(return_value=False)
        monkeypatch.setattr(launcher_module, "get_repository", lambda _url: repo)
        result = await factory.start_process_by_name("zmq_broker")
        assert result.status == "error"
        assert "zmq_broker" not in factory._desired_state
        assert "zmq_broker" not in factory._restart_configs
        assert "zmq_broker" not in factory._restart_uptime_start

    @pytest.mark.asyncio()
    async def test_r2_4_failed_per_wallet_start_keeps_managed_instance_recoverable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A failed per-wallet start of a managed instance stays recoverable.

        Given: a watchdog-managed per-wallet executor instance (desired=RUNNING
            + config armed) whose manual start_per_wallet_instance_by_name then
            has start_process RAISE,
        When: the per-wallet start returns ERROR,
        Then: because the instance was already watchdog-managed before the
            manual start, the markers are NOT cleared — desired stays RUNNING
            and the config is retained — and recovery is re-armed with a
            fresh pending restart task, so the instance is not
            permanently disarmed (it can still be recovered), exercising the
            managed (re-arm) branch of the per-wallet failure path.
        """
        name = "executor_kraken_w0123456789ab"
        settings = MagicMock()
        settings.db_url = "sqlite+aiosqlite:///:memory:"
        factory = ProcessLauncherService(settings)
        managed_config = _watchdog_config(name=name)
        _arm_watchdog(factory, managed_config)
        entry = ProcessRegistryEntry(
            class_ref=cast(Any, MagicMock()),
            class_path="test.KrakenExecutor",
            method="run",
            description="",
            priority=0,
            lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
            role=ProcessRoleEnum.CORE,
            tags=("market-data", "publisher"),
            parameters_model=None,
            parameters_schema=None,
            enabled=True,
            mode="process",
        )
        monkeypatch.setattr(
            launcher_module, "get_registered_processes", lambda: {"executor_kraken": entry}
        )
        credential = {"exchange": "kraken", "wallet_public_id": "wallet-0123456789ab"}
        repo = MagicMock()
        repo.list_active_wallet_credentials = AsyncMock(return_value=[credential])
        monkeypatch.setattr(launcher_module, "get_repository", lambda _url: repo)
        monkeypatch.setattr(factory, "_load_template_setting", AsyncMock(return_value={}))
        monkeypatch.setattr(
            factory,
            "_build_per_wallet_instance_config",
            lambda **kwargs: _watchdog_config(name=name),
        )

        async def _failing_start(_cfg: ProcessConfigModel) -> None:
            raise RuntimeError("per-wallet start boom")

        async def _never() -> None:
            await asyncio.Event().wait()

        pending = asyncio.create_task(_never())
        await asyncio.sleep(0)
        factory._restart_tasks[name] = pending
        monkeypatch.setattr(factory, "start_process", _failing_start)
        monkeypatch.setattr(factory, "_start_native_process_monitoring", lambda: None)
        result = await factory.start_per_wallet_instance_by_name(name)
        assert result.status == "error"
        assert factory._desired_state[name] is _DesiredState.RUNNING
        assert factory._restart_configs[name] is managed_config
        assert "job" not in factory._restart_uptime_start
        rearmed = factory._restart_tasks[name]
        assert rearmed is not pending
        assert not rearmed.done()
        rearmed.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await rearmed

    @pytest.mark.asyncio()
    async def test_r3_1_failed_manual_start_rearms_recovery_for_managed_name(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A failed manual start of a managed name re-arms recovery.

        Given: a watchdog-managed publisher with a pending restart task armed
            during backoff; an operator's manual start_process_by_name cancels
            that pending task (via _cancel_pending_restart) and then
            start_process RAISES,
        When: start_process_by_name returns ERROR,
        Then: recovery is re-armed — a NEW _restart_tasks entry (distinct from
            the cancelled one, not done) is scheduled under the existing
            per-name lock — and the watchdog markers are retained
            (desired=RUNNING, config kept), closing the INERT-state hole where
            the watchdog would otherwise never re-fire.
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(name="zmq_broker", role=ProcessRoleEnum.CORE)
        _arm_watchdog(factory, config)
        factory._restart_attempts["zmq_broker"] = 2

        async def _failing_start(_cfg: ProcessConfigModel) -> None:
            raise RuntimeError("manual start boom")

        async def _never() -> None:
            await asyncio.Event().wait()

        pending = asyncio.create_task(_never())
        await asyncio.sleep(0)
        factory._restart_tasks["zmq_broker"] = pending
        monkeypatch.setattr(factory, "start_process", _failing_start)
        monkeypatch.setattr(factory, "_build_config_for_start_by_name", lambda *a, **k: config)
        monkeypatch.setattr(factory, "_apply_overrides_to_config_dict", lambda *a, **k: True)
        monkeypatch.setattr(factory, "_start_native_process_monitoring", lambda: None)
        repo = MagicMock()
        session = MagicMock()
        setting = MagicMock()
        setting.value = json.dumps({"class": "test.Publisher", "method": "run"})
        session.execute = AsyncMock(return_value=MagicMock())
        session.execute.return_value.scalar_one_or_none.return_value = setting
        repo.session.return_value.__aenter__.return_value = session
        repo.session.return_value.__aexit__ = AsyncMock(return_value=False)
        monkeypatch.setattr(launcher_module, "get_repository", lambda _url: repo)
        result = await factory.start_process_by_name("zmq_broker")
        assert result.status == "error"
        assert factory._desired_state["zmq_broker"] is _DesiredState.RUNNING
        assert factory._restart_configs["zmq_broker"] is config
        assert pending.cancelled()
        rearmed = factory._restart_tasks["zmq_broker"]
        assert rearmed is not pending
        assert not rearmed.done()
        rearmed.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await rearmed

    @pytest.mark.asyncio()
    async def test_r3_1_failed_manual_start_of_new_name_does_not_rearm(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A failed manual start of an unmanaged name re-arms nothing.

        Given: a name that is NOT watchdog-managed before the manual start;
            start_process arms fresh markers then RAISES,
        When: start_process_by_name returns ERROR,
        Then: because the name was not managed before, the manual caller clears
            the watchdog markers and schedules NO replacement restart task —
            the re-arm is reserved for already-managed names only.
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(name="zmq_broker", role=ProcessRoleEnum.CORE)

        async def _arm_then_fail(cfg: ProcessConfigModel) -> None:
            factory._desired_state[cfg.name] = _DesiredState.RUNNING
            factory._restart_configs[cfg.name] = cfg
            factory._restart_uptime_start[cfg.name] = 0.0
            raise RuntimeError("manual start boom")

        monkeypatch.setattr(factory, "start_process", _arm_then_fail)
        monkeypatch.setattr(factory, "_build_config_for_start_by_name", lambda *a, **k: config)
        monkeypatch.setattr(factory, "_apply_overrides_to_config_dict", lambda *a, **k: True)
        monkeypatch.setattr(factory, "_start_native_process_monitoring", lambda: None)
        repo = MagicMock()
        session = MagicMock()
        setting = MagicMock()
        setting.value = json.dumps({"class": "test.Publisher", "method": "run"})
        session.execute = AsyncMock(return_value=MagicMock())
        session.execute.return_value.scalar_one_or_none.return_value = setting
        repo.session.return_value.__aenter__.return_value = session
        repo.session.return_value.__aexit__ = AsyncMock(return_value=False)
        monkeypatch.setattr(launcher_module, "get_repository", lambda _url: repo)
        result = await factory.start_process_by_name("zmq_broker")
        assert result.status == "error"
        assert "zmq_broker" not in factory._desired_state
        assert "zmq_broker" not in factory._restart_configs
        assert "zmq_broker" not in factory._restart_tasks

    @pytest.mark.asyncio()
    async def test_r3_1_rearm_does_not_stack_when_task_already_live(self) -> None:
        """Re-arm is a no-op when a live restart task is already pending.

        Given: a managed name whose _restart_tasks entry is still a live (not
            done) task when _rearm_recovery_after_manual_start_failure is
            invoked directly,
        When: the re-arm runs,
        Then: the live-task guard returns without scheduling a second task —
            the existing task ref is preserved (no stacking of two sleeping
            restart tasks).
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(name="pub")
        _arm_watchdog(factory, config)

        async def _never() -> None:
            await asyncio.Event().wait()

        live = asyncio.create_task(_never())
        await asyncio.sleep(0)
        factory._restart_tasks["pub"] = live
        factory._rearm_recovery_after_manual_start_failure("pub")
        assert factory._restart_tasks["pub"] is live
        live.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await live

    @pytest.mark.asyncio()
    async def test_r3_2_cancel_during_create_run_record_finalizes_run(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A cancel inside _try_create_run_record finalizes the run.

        Given: start_process whose _try_create_run_record has stamped
            active_runs and is suspended awaiting the run-event emit when a
            CancelledError lands — a BaseException that the inner
            except-Exception of _try_create_run_record does not catch and that
            bypasses start_process's except-Exception cleanup,
        When: the task is cancelled and awaited,
        Then: the try/finally that now opens BEFORE the run-record creation
            finalizes the dangling active run FAILED and drops it from
            active_runs, so a cancel landing during run-record creation never
            leaks a RUNNING run record.
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(name="pub")
        _stub_run_tracking(factory)
        finalized: list[ProcessRunStatusEnum] = []
        real_finalize = factory._finalize_process_run

        async def _spy_finalize(name: str, status: ProcessRunStatusEnum, **kwargs: Any) -> None:
            finalized.append(status)
            await real_finalize(name, status, **kwargs)

        monkeypatch.setattr(factory, "_finalize_process_run", _spy_finalize)
        create_blocking = asyncio.Event()

        async def _block_in_create(_cfg: ProcessConfigModel) -> str:
            factory.active_runs["pub"] = "run-id"
            factory.active_run_started_at["pub"] = datetime.now(UTC)
            create_blocking.set()
            await asyncio.Event().wait()
            return "run-id"

        monkeypatch.setattr(factory, "_try_create_run_record", _block_in_create)
        config.mode = "thread"
        task = asyncio.create_task(factory.start_process(config))
        await create_blocking.wait()
        assert "pub" in factory.active_runs
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        assert "pub" not in factory.active_runs
        assert finalized == [ProcessRunStatusEnum.FAILED]

    @pytest.mark.asyncio()
    async def test_r3_2_normal_exception_failure_does_not_double_finalize(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A normal start_process failure does not double-finalize.

        Given: start_process whose spawn raises an ordinary Exception after the
            run record was created — the normal except-Exception path runs
            _handle_start_failure, which records the run FAILED and pops
            active_runs,
        When: start_process raises,
        Then: the widened finally is a no-op because active_runs was already
            popped by _handle_start_failure, so _finalize_process_run is never
            called by the cancel-guard — the run is finalized FAILED exactly
            once (via _handle_start_failure / _update_process_run_record) with
            no double-finalize from the broadened guard.
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(name="pub")
        _stub_run_tracking(factory)
        finalize_calls: list[ProcessRunStatusEnum] = []
        update_calls: list[ProcessRunStatusEnum] = []

        async def _spy_finalize(name: str, status: ProcessRunStatusEnum, **kwargs: Any) -> None:
            finalize_calls.append(status)

        async def _spy_update(public_id: str, status: ProcessRunStatusEnum, **kwargs: Any) -> None:
            update_calls.append(status)

        async def _create(_cfg: ProcessConfigModel) -> str:
            factory.active_runs["pub"] = "run-id"
            factory.active_run_started_at["pub"] = datetime.now(UTC)
            return "run-id"

        monkeypatch.setattr(factory, "_try_create_run_record", _create)
        monkeypatch.setattr(factory, "_finalize_process_run", _spy_finalize)
        monkeypatch.setattr(factory, "_update_process_run_record", _spy_update)

        async def _failing_spawn(_cfg: ProcessConfigModel) -> None:
            raise RuntimeError("spawn boom")

        monkeypatch.setattr(factory, "_start_in_process", _failing_spawn)
        config.mode = "thread"
        with pytest.raises(RuntimeError, match="spawn boom"):
            await factory.start_process(config)
        assert "pub" not in factory.active_runs
        assert finalize_calls == []
        assert update_calls == [ProcessRunStatusEnum.FAILED]

    @pytest.mark.asyncio()
    async def test_r4_1_handle_manual_start_stop_race_returns_none_when_running(self) -> None:
        """The post-spawn re-check is a no-op when desired stays RUNNING.

        Given: a watchdog-managed name whose desired state is still RUNNING
            after the in-flight manual start spawned it (no racing stop),
        When: ``_handle_manual_start_stop_race`` runs its post-spawn re-check,
        Then: it returns ``None`` (the start won) and the just-started process
            is left running.
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(name="pub")
        _arm_watchdog(factory, config)
        factory.started_processes["pub"] = MagicMock()
        result = await factory._handle_manual_start_stop_race("pub")
        assert result is None
        assert "pub" in factory.started_processes
        assert factory._desired_state.get("pub") is _DesiredState.RUNNING

    @pytest.mark.asyncio()
    async def test_r4_1_manual_start_post_spawn_recheck_tears_down(self) -> None:
        """A manual start whose post-spawn re-check sees STOPPED tears down.

        Given: the in-flight manual start spawned a live process, but a
            concurrent stop flipped desired to STOPPED before the post-spawn
            re-check,
        When: ``_handle_manual_start_stop_race`` runs,
        Then: the just-started process is torn down (its ``stop`` awaited, no
            ``started_processes`` entry, watchdog state cleared) and an ERROR
            result is returned so the stop wins.
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(name="pub")
        _arm_watchdog(factory, config)
        stop_calls: list[str] = []
        instance = MagicMock()
        instance.stop = AsyncMock(side_effect=lambda: stop_calls.append("pub"))
        factory.started_processes["pub"] = instance
        factory._desired_state["pub"] = _DesiredState.STOPPED
        result = await factory._handle_manual_start_stop_race("pub")
        assert result is not None
        assert result.status == "error"
        assert stop_calls == ["pub"]
        assert "pub" not in factory.started_processes
        assert factory._desired_state.get("pub") is None

    @pytest.mark.asyncio()
    async def test_r4_1_bare_start_post_spawn_stopped_tears_down(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A bare manual start whose stop won post-spawn returns the stop.

        Given: a manual ``start_process_by_name`` where ``start_process``
            registers the live process but a concurrent stop set desired to
            STOPPED by the time the post-spawn re-check runs,
        When: the in-lock re-check fires,
        Then: ``start_process_by_name`` returns the not-running ERROR result
            from the teardown (the stop wins), exercising the bare-start
            post-spawn return path.
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(name="pub")
        instance = MagicMock()
        instance.stop = AsyncMock()

        async def _stop_wins_start(cfg: ProcessConfigModel) -> None:
            factory._desired_state["pub"] = _DesiredState.STOPPED
            factory.started_processes["pub"] = instance

        monkeypatch.setattr(factory, "start_process", _stop_wins_start)
        monkeypatch.setattr(factory, "_build_config_for_start_by_name", lambda *a, **k: config)
        monkeypatch.setattr(factory, "_apply_overrides_to_config_dict", lambda *a, **k: True)
        monkeypatch.setattr(factory, "_start_native_process_monitoring", lambda: None)
        repo = MagicMock()
        session = MagicMock()
        setting = MagicMock()
        setting.value = json.dumps({"class": "test.Publisher", "method": "run"})
        session.execute = AsyncMock(return_value=MagicMock())
        session.execute.return_value.scalar_one_or_none.return_value = setting
        repo.session.return_value.__aenter__.return_value = session
        repo.session.return_value.__aexit__ = AsyncMock(return_value=False)
        monkeypatch.setattr(launcher_module, "get_repository", lambda _url: repo)
        result = await asyncio.wait_for(factory.start_process_by_name("pub"), timeout=2.0)
        assert result.status == "error"
        assert "pub" not in factory.started_processes
        assert factory._desired_state.get("pub") is None

    @pytest.mark.asyncio()
    async def test_r4_1_per_wallet_start_post_spawn_stopped_tears_down(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A per-wallet manual start whose stop won post-spawn tears down.

        Given: a per-wallet executor manual start where ``start_process``
            registers the live instance but a concurrent stop set desired to
            STOPPED by the time the post-spawn re-check runs,
        When: ``start_per_wallet_instance_by_name`` runs its in-lock re-check,
        Then: the just-started instance is torn down, ``instance_configs`` is
            dropped, and an ERROR result is returned so the stop wins.
        """
        name = "executor_kraken_w0123456789ab"
        settings = MagicMock()
        settings.db_url = "sqlite+aiosqlite:///:memory:"
        factory = ProcessLauncherService(settings)
        entry = ProcessRegistryEntry(
            class_ref=cast(Any, MagicMock()),
            class_path="test.KrakenExecutor",
            method="run",
            description="",
            priority=0,
            lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
            role=ProcessRoleEnum.CORE,
            tags=("market-data", "publisher"),
            parameters_model=None,
            parameters_schema=None,
            enabled=True,
            mode="process",
        )
        monkeypatch.setattr(
            launcher_module, "get_registered_processes", lambda: {"executor_kraken": entry}
        )
        credential = {"exchange": "kraken", "wallet_public_id": "wallet-0123456789ab"}
        repo = MagicMock()
        repo.list_active_wallet_credentials = AsyncMock(return_value=[credential])
        monkeypatch.setattr(launcher_module, "get_repository", lambda _url: repo)
        monkeypatch.setattr(factory, "_load_template_setting", AsyncMock(return_value={}))
        monkeypatch.setattr(
            factory,
            "_build_per_wallet_instance_config",
            lambda **kwargs: _watchdog_config(name=name),
        )
        monkeypatch.setattr(factory, "_start_native_process_monitoring", lambda: None)
        stop_calls: list[str] = []
        instance = MagicMock()
        instance.stop = AsyncMock(side_effect=lambda: stop_calls.append(name))

        async def _stop_wins_start(cfg: ProcessConfigModel) -> None:
            factory._desired_state[name] = _DesiredState.STOPPED
            factory.started_processes[name] = instance

        monkeypatch.setattr(factory, "start_process", _stop_wins_start)
        result = await asyncio.wait_for(
            factory.start_per_wallet_instance_by_name(name), timeout=2.0
        )
        assert result.status == "error"
        assert stop_calls == [name]
        assert name not in factory.started_processes
        assert name not in factory.instance_configs
        assert factory._desired_state.get(name) is None

    @pytest.mark.asyncio()
    async def test_r4_1_rearm_gated_when_stop_set_desired_stopped(self) -> None:
        """The recovery re-arm is suppressed when a stop set desired=STOPPED.

        Given: a watchdog-managed name whose desired state was flipped to
            STOPPED by a concurrent stop, with no pending restart task,
        When: ``_rearm_recovery_after_manual_start_failure`` runs (the failed
            manual-start re-arm path),
        Then: no restart task is created — the stop owns the lifecycle and the
            re-arm must not resurrect it.
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(name="pub")
        _arm_watchdog(factory, config)
        factory._desired_state["pub"] = _DesiredState.STOPPED
        factory._rearm_recovery_after_manual_start_failure("pub")
        assert factory._restart_tasks.get("pub") is None

    @pytest.mark.asyncio()
    async def test_r4_1_stop_racing_inflight_manual_start_wins_via_lock(self) -> None:
        """Stop racing an in-flight manual start wins and leaves no orphan.

        Given: a watchdog-managed publisher; an in-flight manual start holds the
            per-name lock and is suspended inside ``start_process`` after it set
            desired=RUNNING and registered a live process,
        When: a concurrent ``stop_process_by_name`` runs (sets desired=STOPPED,
            then blocks on the same per-name lock) and the start then finishes,
        Then: the manual start's post-spawn re-check sees STOPPED and tears the
            process down; the stop then acquires the lock, finds nothing
            running, and clears state — end state is STOPPED, not running,
            watchdog state cleared, and NO orphan restart task remains.
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(name="pub")
        _arm_watchdog(factory, config)
        config_for_start = _watchdog_config(name="pub")
        gate = asyncio.Event()
        released = asyncio.Event()

        async def _suspending_start(_cfg: ProcessConfigModel) -> None:
            factory._desired_state["pub"] = _DesiredState.RUNNING
            instance = MagicMock()
            instance.stop = AsyncMock()
            factory.started_processes["pub"] = instance
            released.set()
            await gate.wait()

        async def _manual_start_under_lock() -> str:
            await factory._cancel_pending_restart("pub")
            async with factory._restart_lock_for("pub"):
                await _suspending_start(config_for_start)
                stopped = await factory._handle_manual_start_stop_race("pub")
                if stopped is not None:
                    return stopped.status
            return "success"

        start_task = asyncio.create_task(_manual_start_under_lock())
        await released.wait()
        stop_task = asyncio.create_task(factory.stop_process_by_name("pub"))
        await asyncio.sleep(0)
        gate.set()
        start_status = await asyncio.wait_for(start_task, timeout=2.0)
        stop_result = await asyncio.wait_for(stop_task, timeout=2.0)
        assert start_status == "error"
        assert stop_result.status == "not_running"
        assert factory._desired_state.get("pub") is None
        assert "pub" not in factory.started_processes
        assert "pub" not in factory._restart_configs
        assert factory._restart_tasks.get("pub") is None

    @pytest.mark.asyncio()
    async def test_r4_1_stop_takes_lock_no_self_deadlock(self) -> None:
        """Stop takes the lock for its decision without self-deadlock.

        Given: a watchdog-managed publisher with a live process and a pending
            delayed-restart task,
        When: ``stop_process_by_name`` runs — it cancels the pending restart
            outside the lock, then acquires the per-name lock for the real stop
            and ``_clear_watchdog_state``,
        Then: the call completes within a timeout (proving the lock acquisition
            does not deadlock against the cancelled task) and the process is
            stopped with watchdog state cleared.
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(name="pub")
        _arm_watchdog(factory, config)
        _stub_run_tracking(factory)
        instance = MagicMock()
        instance.stop = AsyncMock()
        factory.started_processes["pub"] = instance
        factory._restart_tasks["pub"] = asyncio.create_task(factory._delayed_restart("pub", 999.0))
        await asyncio.sleep(0)
        result = await asyncio.wait_for(factory.stop_process_by_name("pub"), timeout=2.0)
        assert result.status == "success"
        assert "pub" not in factory.started_processes
        assert factory._desired_state.get("pub") is None
        assert factory._restart_tasks.get("pub") is None

    @pytest.mark.asyncio()
    async def test_r5_1_stop_recancels_task_created_in_prelock_window(self) -> None:
        """Stop kills a restart task armed during its pre-lock window.

        Given: a watchdog-managed, not-running publisher with a pending sleeping
            ``_restart_tasks[name]``; a manual start fails and re-arms recovery
            DURING the stop's pre-lock ``_cancel_pending_restart`` window, and
            the failing manual start clobbered desired back to RUNNING (the
            re-arm race),
        When: the stop acquires the per-name lock,
        Then: the in-lock re-cancel (:meth:`_cancel_restart_tasks_locked`) kills
            the window-created task; after both settle there is NO surviving
            ``_restart_tasks[name]``, desired is cleared, and advancing time
            triggers NO respawn (the orphan sleeper can never wake and spawn
            against an already-running name).
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(name="pub")
        _arm_watchdog(factory, config)
        _stub_run_tracking(factory)
        respawns: list[str] = []

        async def _fake_respawn(name: str, delay: float) -> None:
            await asyncio.sleep(delay)
            async with factory._restart_lock_for(name):
                respawns.append(name)

        gate = asyncio.Event()
        entered = asyncio.Event()
        original_cancel = factory._cancel_pending_restart

        async def _slow_cancel(name: str) -> None:
            await original_cancel(name)
            entered.set()
            await gate.wait()

        pending_task = asyncio.create_task(_fake_respawn("pub", 999.0))
        factory._restart_tasks["pub"] = pending_task
        await asyncio.sleep(0)
        factory._cancel_pending_restart = _slow_cancel
        stop_task = asyncio.create_task(factory.stop_process_by_name("pub"))
        await entered.wait()
        factory._desired_state["pub"] = _DesiredState.RUNNING
        window_task = asyncio.create_task(_fake_respawn("pub", 999.0))
        factory._restart_tasks["pub"] = window_task
        gate.set()
        result = await asyncio.wait_for(stop_task, timeout=2.0)
        assert result.status == "not_running"
        assert pending_task.cancelled()
        assert window_task.cancelled()
        assert factory._desired_state.get("pub") is None
        assert factory._restart_tasks.get("pub") is None
        await asyncio.sleep(0)
        assert respawns == []

    @pytest.mark.asyncio()
    async def test_r5_1_start_process_does_not_arm_desired(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """start_process never sets desired=RUNNING by itself.

        Given: a stop has declared desired=STOPPED for a name,
        When: ``start_process`` runs the LOCK-FREE primitive for that name,
        Then: it records the respawn config/uptime but leaves desired=STOPPED
            untouched — only the lock-holding callers / boot spawners own the
            ``desired=RUNNING`` transition (via ``_arm_desired_running``), so a
            start can never clobber a concurrent stop's STOPPED marker.
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(name="pub")
        factory._desired_state["pub"] = _DesiredState.STOPPED
        monkeypatch.setattr(factory, "_try_create_run_record", mock.AsyncMock(return_value=None))
        monkeypatch.setattr(factory, "_start_as_subprocess", mock.MagicMock())
        monkeypatch.setattr(factory, "_finalize_one_shot", mock.AsyncMock())
        monkeypatch.setattr(factory, "_emit_summary_snapshot", mock.AsyncMock())
        await factory.start_process(config)
        assert factory._desired_state.get("pub") is _DesiredState.STOPPED
        assert factory._restart_configs["pub"] is config

    @pytest.mark.asyncio()
    async def test_r5_1_in_lock_recancel_terminates_no_deadlock(self) -> None:
        """In-lock re-cancel of a window task terminates without deadlock.

        Given: a not-running name whose ``_restart_tasks`` entry is a live
            delayed-restart task sleeping its backoff OUTSIDE the per-name lock,
        When: ``stop_process_by_name`` acquires the lock and re-cancels it via
            :meth:`_cancel_restart_tasks_locked`,
        Then: the stop completes within a timeout (the cancelled task unwinds at
            its sleep/acquire suspension point and never contends for the held
            lock) and no restart task survives.
        """
        factory = ProcessLauncherService(MagicMock())
        config = _watchdog_config(name="pub")
        _arm_watchdog(factory, config)
        _stub_run_tracking(factory)
        factory._restart_tasks["pub"] = asyncio.create_task(factory._delayed_restart("pub", 999.0))
        await asyncio.sleep(0)
        result = await asyncio.wait_for(factory.stop_process_by_name("pub"), timeout=2.0)
        assert result.status == "not_running"
        assert factory._restart_tasks.get("pub") is None
        assert factory._desired_state.get("pub") is None


class TestParkedExecutorDetection:
    """Give-up branch: synthetic park heartbeats + /health flip."""

    def _factory(self) -> ProcessLauncherService:
        """Launcher with a stubbed publisher capturing sends."""
        factory = ProcessLauncherService(MagicMock())
        publisher = MagicMock()
        publisher.tracker = MagicMock()
        publisher.tracker.session_id = "s1"
        publisher.tracker.next_sequence = MagicMock(return_value=7)
        publisher.send = mock.AsyncMock()
        factory.set_msg_publisher(publisher)
        return factory

    @pytest.mark.asyncio
    async def test_executor_park_publishes_three_error_frames(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A parked per-wallet executor bursts 3 ERROR frames on its topic.

        Given: _escalate_restart's give-up branch for an executor
            instance name,
        When: The park burst task runs,
        Then: Exactly 3 ERROR heartbeats go out on the instance's own
            5-segment topic with forensic meta — the existing
            critical-system-error pipeline alerts with NO new AlertType.
        """
        factory = self._factory()
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher._PARK_HEARTBEAT_SPACING_S",
            0.0,
        )
        factory._restart_attempts["executor_kraken_wabc123def456"] = 6
        factory._total_failed_restarts["executor_kraken_wabc123def456"] = 9
        config = _watchdog_config(
            name="executor_kraken_wabc123def456", role=ProcessRoleEnum.CORE, tags=("orders",)
        )
        publisher = cast(Any, factory)._msg_publisher
        sent = asyncio.Event()

        async def count_sends(topic: str, frame: Any) -> None:
            if publisher.send.await_count >= 3:
                sent.set()

        publisher.send = mock.AsyncMock(side_effect=count_sends)
        factory._escalate_restart("executor_kraken_wabc123def456", config)
        assert "executor_kraken_wabc123def456" in factory._parked_processes
        assert "executor_kraken_wabc123def456" in factory._park_heartbeat_tasks
        await asyncio.wait_for(sent.wait(), timeout=5.0)
        burst = factory._park_heartbeat_tasks["executor_kraken_wabc123def456"]
        factory._unpark("executor_kraken_wabc123def456")
        with contextlib.suppress(asyncio.CancelledError):
            await burst
        assert publisher.send.await_count == 3
        topics = {c.args[0] for c in publisher.send.await_args_list}
        assert topics == {"system.heartbeats.executor.kraken.abc123def456"}
        frames = [c.args[1] for c in publisher.send.await_args_list]
        assert [f.sequence for f in frames] == [1, 2, 3]
        assert all(f.status == "error" for f in frames)
        assert frames[0].meta["synthetic"] is True
        assert frames[0].meta["reason"] == "restart_budget_exhausted"
        assert frames[0].meta["consecutive_failures"] == 6

    def test_non_executor_park_flips_health_without_frames(self) -> None:
        """Non-executor give-ups join the /health flip but emit nothing."""
        factory = self._factory()
        config = _watchdog_config(name="some_strategy_job", role=ProcessRoleEnum.STRATEGY)
        factory._escalate_restart("some_strategy_job", config)
        assert "some_strategy_job" in factory._parked_processes
        assert not factory._park_heartbeat_tasks

    @pytest.mark.asyncio
    async def test_parked_name_rebursts_until_unparked(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A still-parked name re-bursts after the reburst pause.

        Given: A parked executor whose first 3-frame burst the sidecar
            may have missed entirely,
        When: The reburst period elapses while still parked,
        Then: Another burst goes out — parked alerting is level-triggered
            and survives notify restarts; unparking ends the loop.
        """
        factory = self._factory()
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher._PARK_HEARTBEAT_SPACING_S", 0.0
        )
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher._PARK_REBURST_PERIOD_S", 0.0
        )
        factory._parked_processes.add("executor_kraken_wabc")
        publisher = cast(Any, factory)._msg_publisher
        calls = 0

        async def count_and_stop(topic: str, frame: Any) -> None:
            nonlocal calls
            calls += 1
            if calls >= 6:
                factory._parked_processes.discard("executor_kraken_wabc")

        publisher.send = mock.AsyncMock(side_effect=count_and_stop)
        await factory._publish_park_heartbeats("executor_kraken_wabc", "kraken", "abc", 6, 9)
        assert calls == 6

    @pytest.mark.asyncio
    async def test_unpark_mid_burst_aborts_remaining_frames(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An operator stop between frames silences the rest of the burst.

        Given: A park burst whose first frame went out,
        When: The name is unparked before the next frame,
        Then: Frames 2-3 are never sent — an accepted parking can never
            complete the rule's 3-consecutive gate and page anyway.
        """
        factory = self._factory()
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher._PARK_HEARTBEAT_SPACING_S",
            0.0,
        )
        factory._parked_processes.add("executor_kraken_wabc")
        publisher = cast(Any, factory)._msg_publisher

        async def send_then_unpark(topic: str, frame: Any) -> None:
            factory._unpark("executor_kraken_wabc")

        publisher.send = mock.AsyncMock(side_effect=send_then_unpark)
        await factory._publish_park_heartbeats("executor_kraken_wabc", "kraken", "abc", 6, 9)
        assert publisher.send.await_count == 1

    @pytest.mark.asyncio
    async def test_unpark_cancels_suspended_send(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A frame suspended in its socket send dies with the unpark.

        Given: A burst frame whose send is blocked on backpressure,
        When: The operator unparks the name while the send is in flight,
        Then: The burst task is CANCELLED — the blocked frame can never
            resume and complete the rule's 3-consecutive gate after the
            parking was accepted.
        """
        factory = self._factory()
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher._PARK_HEARTBEAT_SPACING_S",
            0.0,
        )
        factory._parked_processes.add("executor_kraken_wabc")
        publisher = cast(Any, factory)._msg_publisher
        entered = asyncio.Event()

        async def blocked_send(topic: str, frame: Any) -> None:
            entered.set()
            await asyncio.Event().wait()

        publisher.send = mock.AsyncMock(side_effect=blocked_send)
        burst = asyncio.create_task(
            factory._publish_park_heartbeats("executor_kraken_wabc", "kraken", "abc", 6, 9)
        )
        factory._park_heartbeat_tasks["executor_kraken_wabc"] = burst
        await entered.wait()
        factory._unpark("executor_kraken_wabc")
        with contextlib.suppress(asyncio.CancelledError):
            await burst
        assert burst.cancelled()
        assert publisher.send.await_count == 1
        assert "executor_kraken_wabc" not in factory._park_heartbeat_tasks

    @pytest.mark.asyncio
    async def test_send_failure_is_swallowed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The give-up path never raises through a failing publisher."""
        factory = self._factory()
        calls = 0

        async def failing_send(topic: str, frame: Any) -> None:
            nonlocal calls
            calls += 1
            if calls >= 3:
                factory._parked_processes.discard("executor_kraken_wabc")
            raise RuntimeError("bus down")

        cast(Any, factory)._msg_publisher.send = mock.AsyncMock(side_effect=failing_send)
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher._PARK_HEARTBEAT_SPACING_S",
            0.0,
        )
        factory._parked_processes.add("executor_kraken_wabc")
        await factory._publish_park_heartbeats("executor_kraken_wabc", "kraken", "abc", 6, 9)
        assert calls == 3

    @pytest.mark.asyncio
    async def test_publisher_none_is_noop(self) -> None:
        """No publisher wired (boot edge) — burst degrades to nothing."""
        factory = ProcessLauncherService(MagicMock())
        factory._parked_processes.add("executor_kraken_wabc")
        await factory._publish_park_heartbeats("executor_kraken_wabc", "kraken", "abc", 6, 9)

    @pytest.mark.asyncio
    async def test_parked_set_flips_core_health_until_restart(self) -> None:
        """/health reports ERROR while parked and recovers on start.

        Given: A parked process in the set,
        When: get_core_health runs,
        Then: ERROR without any config scan; start_process for the name
            clears the set and health recovers.
        """
        factory = self._factory()
        factory.settings.server_api_only = False
        factory._parked_processes.add("executor_kraken_wabc")
        status = await factory.get_core_health()
        assert status == HealthStatusEnum.ERROR
        factory._core_health_cache = None
        factory._parked_processes.discard("executor_kraken_wabc")
        factory.get_process_configs = mock.AsyncMock(return_value=[])
        status = await factory.get_core_health()
        assert status == HealthStatusEnum.HEALTHY

    @pytest.mark.asyncio
    async def test_failed_restart_keeps_parked_marker(self) -> None:
        """A failed start of a parked name never clears the marker.

        Given: A parked executor whose manual restart raises,
        When: start_process fails,
        Then: The name stays parked (and /health stays ERROR) — the old
            up-front discard reported HEALTHY while nothing ran.
        """
        factory = self._factory()
        _stub_run_tracking(factory)
        factory._parked_processes.add("executor_kraken_wabc")
        config = _watchdog_config(name="executor_kraken_wabc", role=ProcessRoleEnum.CORE)
        with (
            mock.patch.object(
                factory, "_start_as_subprocess", side_effect=RuntimeError("boot poison")
            ),
            mock.patch.object(factory, "_finalize_process_run", new=mock.AsyncMock()),
            pytest.raises(RuntimeError, match="boot poison"),
        ):
            await factory.start_process(config)
        assert "executor_kraken_wabc" in factory._parked_processes

    @pytest.mark.asyncio
    async def test_deliberate_stop_unparks_and_invalidates_cache(self) -> None:
        """Stopping a parked name drops the marker and the health cache.

        Given: A parked name pinning /health to ERROR via the cache,
        When: stop_process_by_name runs for it,
        Then: The marker and cache are dropped — a lingering marker would
            pin ERROR forever after the operator accepted the state.
        """
        factory = self._factory()
        factory._parked_processes.add("executor_kraken_wabc")
        factory._core_health_cache = (0.0, HealthStatusEnum.ERROR)
        await factory.stop_process_by_name("executor_kraken_wabc")
        assert "executor_kraken_wabc" not in factory._parked_processes
        assert factory._core_health_cache is None

    @pytest.mark.asyncio
    async def test_stop_all_unparks_everything(self) -> None:
        """A full shutdown drops every parked marker."""
        factory = self._factory()
        factory._parked_processes.update({"executor_kraken_wa", "executor_kraken_wb"})
        await factory.stop_all_processes()
        assert factory._parked_processes == set()

    @pytest.mark.asyncio
    async def test_stale_burst_callback_never_evicts_newer_task(self) -> None:
        """A finished old burst cannot evict a re-parked name's new burst."""
        factory = self._factory()

        async def noop() -> None:
            return None

        old_task = asyncio.create_task(noop())
        new_task = asyncio.create_task(noop())
        await asyncio.gather(old_task, new_task)
        factory._park_heartbeat_tasks["executor_kraken_wabc"] = new_task
        factory._discard_park_task("executor_kraken_wabc", old_task)
        assert factory._park_heartbeat_tasks["executor_kraken_wabc"] is new_task
        factory._discard_park_task("executor_kraken_wabc", new_task)
        assert "executor_kraken_wabc" not in factory._park_heartbeat_tasks

    @pytest.mark.asyncio
    async def test_parked_flips_health_even_in_api_only_mode(self) -> None:
        """API-only deployments still surface a parked process.

        Given: server_api_only=True (manual REST-started processes can
            still park) and a parked name,
        When: get_core_health runs,
        Then: ERROR — the api-only early-return must not bypass the
            parked backstop, or the publisher-None/no-burst residual
            loses its only signal.
        """
        factory = self._factory()
        factory.settings.server_api_only = True
        factory._parked_processes.add("executor_kraken_wabc")
        assert await factory.get_core_health() == HealthStatusEnum.ERROR
        factory._unpark("executor_kraken_wabc")
        assert await factory.get_core_health() == HealthStatusEnum.HEALTHY

    def test_park_invalidates_health_cache(self) -> None:
        """Parking drops a cached HEALTHY so /health flips promptly."""
        factory = self._factory()
        factory._core_health_cache = (0.0, HealthStatusEnum.HEALTHY)
        factory._park("executor_kraken_wabc")
        assert factory._core_health_cache is None

    @pytest.mark.asyncio
    async def test_publisher_branch_unchanged(self) -> None:
        """A CORE market-data publisher still trips the feed-exit path."""
        factory = self._factory()
        config = _watchdog_config(
            name="kraken_feed_publisher",
            role=ProcessRoleEnum.CORE,
            tags=("market-data", "publisher", "kraken"),
        )
        factory._escalate_restart("kraken_feed_publisher", config)
        assert factory._feed_failed_publisher == "kraken_feed_publisher"
        assert factory._feed_failure_event.is_set()
        assert not factory._park_heartbeat_tasks
