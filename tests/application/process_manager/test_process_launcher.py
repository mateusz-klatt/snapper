"""Tests for process launcher functionality."""

import asyncio
import contextlib
import json
import subprocess
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

import pytest
from pydantic import BaseModel

from snapper.application.process_manager.enums import ProcessLifecycleEnum
from snapper.application.process_manager.enums import ProcessRoleEnum
from snapper.application.process_manager.enums import ProcessRunStatusEnum
from snapper.application.process_manager.launcher import CoreProcessStartupError
from snapper.application.process_manager.launcher import ProcessLauncherService
from snapper.application.process_manager.models import ProcessConfigModel
from snapper.application.process_manager.models import ProcessInstanceInfo
from snapper.application.process_manager.models import ProcessRegistryEntry
from snapper.application.process_manager.models import SpawnerStatusSnapshot
from snapper.application.process_manager.spawner import ProcessSpawnerService
from snapper.config.app import AppSettings
from snapper.config.bootstrap import BootstrapSettingsLoader
from snapper.config.settings import get_settings
from snapper.core.json_types import JsonObject
from snapper.data.models import ProcessRun
from snapper.data.models import Setting


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
    @pytest.mark.asyncio
    @patch("snapper.application.process_manager.launcher.ProcessLauncherService.import_class")
    async def test_start_process_empty_parameters_allowed(self, mock_import: MagicMock) -> None:
        """Verify start_process handles empty parameters correctly.

        Given: A process config with empty parameters dictionary,
        When: start_process is called,
        Then: Class is instantiated without keyword arguments and process starts.
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
        mock_good_instance.run = MagicMock()
        mock_good_class.return_value = mock_good_instance
        mock_another_good_class = MagicMock()
        mock_another_good_instance = MagicMock()
        mock_another_good_instance.run = MagicMock()
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
    finalize_mock.assert_awaited_once_with("native", ProcessRunStatusEnum.CANCELLED, error=None)
    assert "native" not in factory.started_processes
    spawner_mock.cleanup.assert_called_once_with("native")


@pytest.mark.asyncio()
async def test_handle_process_completion_unexpected_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify unexpected exit finalizes as FAILED with exit code.

    Given: A native process not in expected_terminations with non-zero exit,
    When: _handle_process_completion is called,
    Then: Run is finalized with FAILED status and exit code in error message.
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
        "native", ProcessRunStatusEnum.FAILED, error="exit_code=2"
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
    spawner_mock.cleanup.assert_called_once_with("native")
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
    finalize_mock.assert_awaited_once_with("job", ProcessRunStatusEnum.SUCCEEDED, error=None)
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
