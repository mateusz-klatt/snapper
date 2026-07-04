"""Tests for ProcessLauncherService core functionality."""

import asyncio
import contextlib
import json
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from types import SimpleNamespace
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch
from uuid import uuid4

import pytest

from snapper.api.schemas.base import StrictDataSchema
from snapper.application.process_manager import launcher as launcher_module
from snapper.application.process_manager.config_resolver import resolve_mode
from snapper.application.process_manager.launcher import ProcessLauncherService
from snapper.application.process_manager.launcher import _DesiredState
from snapper.application.process_manager.models import ProcessConfigModel
from snapper.application.process_manager.models import ProcessInstanceInfo
from snapper.application.process_manager.models import ProcessRegistryEntry
from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.process_manager.models import SpawnerStatusSnapshot
from snapper.config.app import AppSettings
from snapper.config.bootstrap import BootstrapSettingsLoader
from snapper.core.json_types import JsonObject
from snapper.core.types import ProcessLifecycleEnum
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRestartPolicyEnum
from snapper.core.types import ProcessRoleEnum
from snapper.core.types import ProcessRunStatusEnum
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.data import ProcessConfiguredEventData
from snapper.messaging.schemas.data import ProcessRunEventData
from snapper.messaging.schemas.data import ProcessSummaryEventData
from snapper.messaging.schemas.data import StrategyListEventData
from snapper.messaging.topics.validation import validate_topic


class DummySettings(SimpleNamespace):
    """Simple namespace settings stub for testing."""

    db_url: str = "sqlite:///:memory:"


class DummyProcess:
    """Async process stub that returns immediately."""

    async def start(self) -> str:
        """Async start method that completes quickly."""
        await asyncio.sleep(0)
        return "done"


@pytest.mark.asyncio
async def test_import_class_prefers_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify import_class prefers registry over dynamic import.

    Given: Process registered in process registry,
    When: import_class is called with process name,
    Then: Class from registry is returned.
    """
    launcher: Any = ProcessLauncherService(settings=cast(Any, DummySettings()))
    monkeypatch.setattr(
        "snapper.application.process_manager.config_resolver.get_registered_processes",
        lambda: {
            "foo": ProcessRegistryEntry(
                class_ref=DummyProcess,
                class_path="test.DummyProcess",
                method="start",
                description="Test process",
                priority=10,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.CORE,
                tags=(),
                parameters_model=None,
                parameters_schema=None,
                enabled=False,
                mode="thread",
            )
        },
    )
    cls = launcher.import_class("ignored.path.DummyProcess", process_name="foo")
    assert cls is DummyProcess


def test_import_class_raises_on_missing() -> None:
    """Verify import_class raises ImportError for missing module.

    Given: Non-existent module path,
    When: import_class is called,
    Then: ImportError is raised.
    """
    launcher: Any = ProcessLauncherService(settings=cast(Any, DummySettings()))
    with pytest.raises(ImportError):
        launcher.import_class("not_a_module.Class")


@pytest.mark.asyncio
async def test_start_process_handles_run_record_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify start_process continues when run record creation fails.

    Given: _create_process_run_record raises ValueError,
    When: start_process is called,
    Then: Process starts successfully despite DB error.
    """
    launcher: Any = ProcessLauncherService(settings=cast(Any, DummySettings()))
    launcher._create_process_run_record = AsyncMock(side_effect=ValueError("fail"))
    launcher._update_process_run_record = AsyncMock()
    launcher._finalize_process_run = AsyncMock()
    launcher.import_class = lambda path, name=None, template=None: DummyProcess
    launcher._register_task_completion = lambda name, task: None
    config = ProcessConfigModel(
        name="dummy",
        enabled=True,
        mode="thread",
        class_path="dummy.path.DummyProcess",
        method="start",
        parameters={},
        note=None,
        lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
        role=ProcessRoleEnum.CORE,
        tags=(),
        parameters_schema=None,
    )
    await launcher.start_process(config)
    assert "dummy" in launcher.process_tasks
    assert "dummy" in launcher.started_processes
    assert launcher.process_lifecycles["dummy"] == ProcessLifecycleEnum.LONG_RUNNING


class AsyncDummyProcess:
    """Async process stub that returns None."""

    async def start(self) -> None:
        """Async start method that returns None."""
        return None


@pytest.mark.asyncio
async def test_start_process_rejects_invalid_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify start_process raises ValueError for invalid mode.

    Given: Process config with mode='worker' (not a valid ProcessMode),
    When: start_process is called,
    Then: ValueError is raised with descriptive message.
    """
    launcher: Any = ProcessLauncherService(settings=cast(Any, DummySettings()))
    launcher._create_process_run_record = AsyncMock()
    launcher._update_process_run_record = AsyncMock()
    launcher._finalize_process_run = AsyncMock()
    launcher._register_task_completion = lambda name, task: None
    launcher.import_class = lambda path, name=None, template=None: AsyncDummyProcess
    config = ProcessConfigModel(
        name="async_proc",
        enabled=True,
        mode="worker",
        class_path="dummy.path.AsyncDummyProcess",
        method="start",
        parameters={},
        note=None,
        lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
        role=ProcessRoleEnum.CORE,
        tags=(),
        parameters_schema=None,
    )
    with pytest.raises(ValueError, match="Invalid mode 'worker'"):
        await launcher.start_process(config)


class StopRaises:
    """Process stub whose stop method raises RuntimeError."""

    def stop(self) -> None:
        """Raise RuntimeError when called."""
        raise RuntimeError("stop failed")


@pytest.mark.asyncio
async def test_stop_all_processes_handles_stop_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify stop_all_processes continues when stop() raises error.

    Given: Process with stop method that raises RuntimeError,
    When: stop_all_processes is called,
    Then: Process is cleaned up despite error.
    """
    launcher: Any = ProcessLauncherService(settings=cast(Any, DummySettings()))
    launcher.started_processes["proc"] = StopRaises()
    launcher.process_roles["proc"] = ProcessRoleEnum.CORE
    launcher.process_lifecycles["proc"] = ProcessLifecycleEnum.LONG_RUNNING
    launcher.process_tasks["task"] = asyncio.create_task(asyncio.sleep(0.1))
    monkeypatch.setattr(
        "snapper.application.process_manager.launcher.get_registered_processes", lambda: {}
    )
    await launcher.stop_all_processes()
    assert launcher.started_processes == {}
    assert launcher.process_tasks == {}
    assert launcher.expected_terminations == set()


@pytest.mark.asyncio
async def test_register_task_completion_handles_closed_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify _register_task_completion handles closed event loop.

    Given: Event loop that raises RuntimeError on access,
    When: _register_task_completion is called,
    Then: No exception propagates.
    """
    launcher: Any = ProcessLauncherService(settings=cast(Any, DummySettings()))
    task = asyncio.create_task(asyncio.sleep(0))
    monkeypatch.setattr(
        "snapper.application.process_manager.launcher.asyncio.get_running_loop",
        lambda: (_ for _ in ()).throw(RuntimeError("loop closed")),
    )
    launcher._register_task_completion("dummy", task)
    await task


class TestStartProcessByNameNoSetting:
    """Tests for start_process_by_name when setting is missing."""

    @pytest.mark.asyncio
    async def test_start_process_by_name_one_shot_succeeds(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test one-shot process starts without persisting config.

        Given: Process config exists with one_shot lifecycle.
        When: start_process_by_name is called.
        Then: Process starts successfully without DB config writes.
        """
        launcher: Any = ProcessLauncherService(settings=cast(Any, DummySettings()))
        launcher.start_process = AsyncMock()
        launcher._start_native_process_monitoring = MagicMock()
        initial_config = {
            "class": "dummy.path.DummyProcess",
            "method": "start",
            "enabled": True,
            "mode": "thread",
            "parameters": {},
            "lifecycle": "one_shot",
            "role": "core",
        }
        mock_setting = MagicMock()
        mock_setting.value = json.dumps(initial_config)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_setting
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=mock_result)
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        mock_repo = MagicMock()
        mock_repo.session.return_value = mock_context
        registry = {
            "test_proc": ProcessRegistryEntry(
                class_ref=DummyProcess,
                class_path="dummy.path.DummyProcess",
                method="start",
                description="Test process",
                priority=10,
                lifecycle=ProcessLifecycleEnum.ONE_SHOT,
                role=ProcessRoleEnum.CORE,
                tags=(),
                parameters_model=None,
                parameters_schema=None,
                enabled=False,
                mode="thread",
            )
        }
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_registered_processes",
            lambda: registry,
        )
        with patch(
            "snapper.application.process_manager.launcher.get_repository",
            return_value=mock_repo,
        ):
            result = await launcher.start_process_by_name("test_proc")
        assert result.status == "success"
        assert "executed successfully" in result.message


class TestSyncRegistryTagsNotIterable:
    """Tests for sync_registry with non-iterable tags."""

    @pytest.mark.asyncio
    async def test_sync_registry_tags_as_string_skips_tags_block(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test sync handles tags as string gracefully.

        Given: Registry entry with tags as a single string (not iterable).
        When: sync_registry_to_database is called.
        Then: Tags block is skipped, no error raised.
        """
        launcher: Any = ProcessLauncherService(settings=cast(Any, DummySettings()))
        existing_config = {
            "name": "test_proc",
            "lifecycle": "long_running",
            "role": "core",
            "parameters": {"key": "value"},
        }
        mock_class = MagicMock()
        mock_class.get_default_parameters.return_value = {}
        registry = {
            "test_proc": ProcessRegistryEntry(
                class_ref=mock_class,
                class_path="test.TestProc",
                method="start",
                description="Test process",
                priority=10,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.CORE,
                tags=("not_a_list",),
                parameters_model=None,
                parameters_schema=None,
                enabled=False,
                mode="thread",
            )
        }
        monkeypatch.setattr(
            "snapper.application.process_manager.registry_syncer.get_registered_processes",
            lambda: registry,
        )
        mock_setting = MagicMock()
        mock_setting.value = json.dumps(existing_config)
        mock_session = AsyncMock()
        mock_session.add = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_setting
        mock_session.execute = AsyncMock(return_value=mock_result)
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        mock_repo = MagicMock()
        mock_repo.session.return_value = mock_context
        with patch(
            "snapper.application.process_manager.registry_syncer.get_repository",
            return_value=mock_repo,
        ):
            await launcher.sync_registry_to_database()

    async def test_sync_registry_skips_process_that_fails_to_serialize(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A non-serializable default is logged and skipped, sync continues.

        Given: Two new processes, the first returning a datetime default
            (not JSON-serializable, since the syncer json.dumps the config).
        When: sync_registry_to_database runs.
        Then: The failing process does not abort the sync and the second
            valid process is still reached (fault isolation per process).
        """
        launcher: Any = ProcessLauncherService(settings=cast(Any, DummySettings()))
        bad_class = MagicMock()
        bad_class.get_default_parameters.return_value = {"start": datetime(2024, 1, 1, tzinfo=UTC)}
        good_class = MagicMock()
        good_class.get_default_parameters.return_value = {"symbols": ["BTC-USD"]}

        def _entry(class_ref: MagicMock) -> ProcessRegistryEntry:
            return ProcessRegistryEntry(
                class_ref=class_ref,
                class_path="test.Proc",
                method="start",
                description="Test process",
                priority=10,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.CORE,
                tags=(),
                parameters_model=None,
                parameters_schema=None,
                enabled=False,
                mode="thread",
            )

        registry = {"bad_proc": _entry(bad_class), "good_proc": _entry(good_class)}
        monkeypatch.setattr(
            "snapper.application.process_manager.registry_syncer.get_registered_processes",
            lambda: registry,
        )
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session = AsyncMock(add=MagicMock())
        mock_session.execute = AsyncMock(return_value=mock_result)
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        mock_repo = MagicMock()
        mock_repo.session.return_value = mock_context
        with patch(
            "snapper.application.process_manager.registry_syncer.get_repository",
            return_value=mock_repo,
        ):
            await launcher.sync_registry_to_database()
        assert good_class.get_default_parameters.called


@dataclass
class _DummySettingsService:
    """Stub settings service that returns defaults."""

    def get_setting(self, key: str, default: Any) -> Any:
        """Return default value for any key."""
        return default


def _create_settings() -> AppSettings:
    """Create AppSettings instance with stub dependencies."""
    bootstrap = BootstrapSettingsLoader()
    return AppSettings(bootstrap, _DummySettingsService())


def _stub_run_tracking(launcher: ProcessLauncherService) -> None:
    """Replace run tracking methods with async mocks."""
    cast(Any, launcher)._create_process_run_record = AsyncMock(return_value="test-run-id")
    cast(Any, launcher)._update_process_run_record = AsyncMock(return_value=None)
    cast(Any, launcher)._finalize_process_run = AsyncMock(return_value=None)


@pytest.fixture
def settings() -> AppSettings:
    """Provide AppSettings instance for process launcher tests."""
    return _create_settings()


@pytest.fixture
def launcher(settings: AppSettings) -> ProcessLauncherService:
    """Provide ProcessLauncherService instance for tests."""
    return ProcessLauncherService(settings)


class TestProcessLauncherServiceInit:
    """Tests for ProcessLauncherService initialization."""

    def test_init_creates_empty_tracking_dicts(self, settings: AppSettings) -> None:
        """Verify init creates empty tracking dictionaries.

        Given: Valid AppSettings,
        When: ProcessLauncherService is instantiated,
        Then: All tracking dicts are empty.
        """
        service = ProcessLauncherService(settings)
        assert service.started_processes == {}
        assert service.process_tasks == {}
        assert service.process_lifecycles == {}
        assert service.process_roles == {}
        assert service.active_runs == {}
        assert service.active_run_started_at == {}
        assert service.expected_terminations == set()
        assert service._msg_publisher is None

    def test_init_creates_spawner(self, settings: AppSettings) -> None:
        """Verify init creates ProcessSpawnerService instance.

        Given: Valid AppSettings,
        When: ProcessLauncherService is instantiated,
        Then: Spawner attribute is initialized.
        """
        service = ProcessLauncherService(settings)
        assert service.spawner is not None


class TestImportClass:
    """Tests for import_class method."""

    def test_import_class_from_registry(self, launcher: ProcessLauncherService) -> None:
        """Test class import from process registry.

        Given: Process registered with class reference.
        When: import_class is called with process name.
        Then: Class from registry is returned.
        """

        class MockProcessClass:
            pass

        with patch(
            "snapper.application.process_manager.config_resolver.get_registered_processes"
        ) as mock_registry:
            mock_registry.return_value = {
                "test_process": ProcessRegistryEntry(
                    class_ref=MockProcessClass,
                    class_path="some.module.MockProcessClass",
                    method="start",
                    description="Mock process",
                    priority=10,
                    lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                    role=ProcessRoleEnum.CORE,
                    tags=(),
                    parameters_model=None,
                    parameters_schema=None,
                    enabled=False,
                    mode="thread",
                )
            }
            result = launcher.import_class("some.module.MockProcessClass", "test_process")
            assert result is MockProcessClass

    def test_import_class_not_a_class_raises_type_error(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Test TypeError raised for non-class registry entry.

        Given: Registry entry with string instead of class.
        When: import_class is called.
        Then: TypeError is raised.
        """
        with patch(
            "snapper.application.process_manager.config_resolver.get_registered_processes"
        ) as mock_registry:
            mock_registry.return_value = {
                "test_process": ProcessRegistryEntry(
                    class_ref=cast(Any, "not_a_class"),
                    class_path="some.path",
                    method="start",
                    description="Invalid process",
                    priority=10,
                    lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                    role=ProcessRoleEnum.CORE,
                    tags=(),
                    parameters_model=None,
                    parameters_schema=None,
                    enabled=False,
                    mode="thread",
                )
            }
            with pytest.raises(TypeError, match="is not a class"):
                launcher.import_class("some.path", "test_process")

    def test_import_class_fallback_to_importlib(self, launcher: ProcessLauncherService) -> None:
        """Test fallback to importlib when not in registry.

        Given: Empty process registry.
        When: import_class is called with valid module path.
        Then: Class is imported via importlib.
        """
        with patch(
            "snapper.application.process_manager.launcher.get_registered_processes"
        ) as mock_registry:
            mock_registry.return_value = {}
            result = launcher.import_class(
                "snapper.application.process_manager.launcher.ProcessLauncherService"
            )
            assert result is ProcessLauncherService

    def test_import_class_invalid_path_raises_import_error(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Test ImportError for nonexistent module.

        Given: Invalid module path.
        When: import_class is called.
        Then: ImportError is raised.
        """
        with patch(
            "snapper.application.process_manager.launcher.get_registered_processes"
        ) as mock_registry:
            mock_registry.return_value = {}
            with pytest.raises(ImportError, match="Failed to import class"):
                launcher.import_class("nonexistent.module.Class", "test")

    def test_import_class_non_class_attribute_raises_type_error(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Test TypeError for non-class module attribute.

        Given: Module path pointing to non-class attribute.
        When: import_class is called.
        Then: TypeError is raised.
        """
        with patch(
            "snapper.application.process_manager.launcher.get_registered_processes"
        ) as mock_registry:
            mock_registry.return_value = {}
            with pytest.raises(TypeError, match="is not a class"):
                launcher.import_class("snapper.application.process_manager.launcher.logger")


class TestStartProcess:
    """Tests for start_process method."""

    @pytest.mark.asyncio
    async def test_start_process_mode_process_spawns_subprocess(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Test process mode spawns subprocess.

        Given: Config with mode='process'.
        When: start_process is called.
        Then: Spawner creates subprocess and process is tracked.
        """
        config = ProcessConfigModel(
            name="test_subprocess",
            enabled=True,
            mode="process",
            class_path="some.module.TestClass",
            method="run",
            parameters={},
            lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
            role=ProcessRoleEnum.CORE,
        )
        mock_process_info = MagicMock(spec=ProcessInstanceInfo)
        mock_process_info.pid = 12345
        with (
            patch.object(
                launcher, "_create_process_run_record", new_callable=AsyncMock
            ) as mock_create_run,
            patch.object(launcher.spawner, "spawn") as mock_spawn,
        ):
            mock_create_run.return_value = str(uuid4())
            mock_spawn.return_value = mock_process_info
            await launcher.start_process(config)
            mock_spawn.assert_called_once_with(
                name="test_subprocess",
                class_path="some.module.TestClass",
                method="run",
                parameters={},
                template_name=None,
            )
            assert launcher.started_processes["test_subprocess"] is mock_process_info

    @pytest.mark.asyncio
    async def test_start_process_async_method_creates_task(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Test async method creates asyncio task.

        Given: Config with async method in thread mode.
        When: start_process is called.
        Then: Asyncio task is created and process tracked.
        """

        class AsyncProcess:
            async def start(self) -> None:
                await asyncio.sleep(10)

        config = ProcessConfigModel(
            name="async_process",
            enabled=True,
            mode="thread",
            class_path="test.AsyncProcess",
            method="start",
            parameters={},
            lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
            role=ProcessRoleEnum.CORE,
        )
        with (
            patch.object(
                launcher, "_create_process_run_record", new_callable=AsyncMock
            ) as mock_create_run,
            patch.object(launcher, "import_class") as mock_import,
            patch.object(launcher, "_finalize_process_run", new_callable=AsyncMock),
        ):
            mock_create_run.return_value = str(uuid4())
            mock_import.return_value = AsyncProcess
            await launcher.start_process(config)
            assert "async_process" in launcher.process_tasks
            assert "async_process" in launcher.started_processes
            task = launcher.process_tasks["async_process"]
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            await asyncio.sleep(0)

    @pytest.mark.asyncio
    async def test_start_process_sync_method_runs_in_executor(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Test sync method runs in thread executor.

        Given: Config with synchronous method.
        When: start_process is called.
        Then: Method executes in executor and completes.
        """
        executed: list[bool] = []

        async def run_in_executor(_executor: object, method: object) -> None:
            if callable(method):
                method()

        class SyncProcess:
            def start(self) -> None:
                executed.append(True)

        config = ProcessConfigModel(
            name="sync_process",
            enabled=True,
            mode="thread",
            class_path="test.SyncProcess",
            method="start",
            parameters={},
            lifecycle=ProcessLifecycleEnum.ONE_SHOT,
            role=ProcessRoleEnum.CORE,
        )
        with (
            patch.object(
                launcher, "_create_process_run_record", new_callable=AsyncMock
            ) as mock_create_run,
            patch.object(launcher, "import_class") as mock_import,
            patch.object(launcher, "_finalize_process_run", new_callable=AsyncMock),
            patch(
                "snapper.application.process_manager.launcher.asyncio.get_event_loop"
            ) as mock_loop,
        ):
            mock_create_run.return_value = str(uuid4())
            mock_import.return_value = SyncProcess
            mock_loop.return_value.run_in_executor = AsyncMock(side_effect=run_in_executor)
            await launcher.start_process(config)
            assert executed == [True]

    @pytest.mark.asyncio
    async def test_start_process_exception_cleans_up_tracking(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Test exception during start cleans up tracking.

        Given: Config with module that fails to import.
        When: start_process is called.
        Then: ImportError raised and tracking dicts cleaned up.
        """
        config = ProcessConfigModel(
            name="failing_process",
            enabled=True,
            mode="thread",
            class_path="test.FailingProcess",
            method="start",
            parameters={},
            lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
            role=ProcessRoleEnum.CORE,
        )
        with (
            patch.object(
                launcher, "_create_process_run_record", new_callable=AsyncMock
            ) as mock_create_run,
            patch.object(launcher, "import_class") as mock_import,
            patch.object(launcher, "_update_process_run_record", new_callable=AsyncMock),
        ):
            mock_create_run.return_value = str(uuid4())
            mock_import.side_effect = ImportError("Module not found")
            with pytest.raises(ImportError):
                await launcher.start_process(config)
            assert "failing_process" not in launcher.started_processes
            assert "failing_process" not in launcher.process_tasks
            assert "failing_process" not in launcher.process_lifecycles

    @pytest.mark.asyncio
    async def test_start_process_one_shot_cleans_up_after_completion(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Test one-shot process cleans up after completion.

        Given: Config with one-shot lifecycle.
        When: start_process is called and completes.
        Then: Process run is finalized.
        """

        async def run_in_executor(_executor: object, method: object) -> None:
            if callable(method):
                method()

        class OneShotProcess:
            def run(self) -> None:
                """No-op run for OneShotProcess test stub."""
                pass

        config = ProcessConfigModel(
            name="oneshot",
            enabled=True,
            mode="thread",
            class_path="test.OneShotProcess",
            method="run",
            parameters={},
            lifecycle=ProcessLifecycleEnum.ONE_SHOT,
            role=ProcessRoleEnum.CORE,
        )
        with (
            patch.object(
                launcher, "_create_process_run_record", new_callable=AsyncMock
            ) as mock_create_run,
            patch.object(launcher, "import_class") as mock_import,
            patch.object(
                launcher, "_finalize_process_run", new_callable=AsyncMock
            ) as mock_finalize,
            patch(
                "snapper.application.process_manager.launcher.asyncio.get_event_loop"
            ) as mock_loop,
        ):
            mock_create_run.return_value = str(uuid4())
            mock_import.return_value = OneShotProcess
            mock_loop.return_value.run_in_executor = AsyncMock(side_effect=run_in_executor)
            await launcher.start_process(config)
            mock_finalize.assert_called()

    @pytest.mark.asyncio
    async def test_start_process_run_record_creation_failure_continues(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Test process starts even if run record creation fails.

        Given: Run record creation raises exception.
        When: start_process is called.
        Then: Process starts despite DB error.
        """

        async def run_in_executor(_executor: object, method: object) -> None:
            if callable(method):
                method()

        class SimpleProcess:
            def start(self) -> None:
                """No-op start for SimpleProcess test stub."""
                pass

        config = ProcessConfigModel(
            name="simple_process",
            enabled=True,
            mode="thread",
            class_path="test.SimpleProcess",
            method="start",
            parameters={},
            lifecycle=ProcessLifecycleEnum.ONE_SHOT,
            role=ProcessRoleEnum.CORE,
        )
        with (
            patch.object(
                launcher, "_create_process_run_record", new_callable=AsyncMock
            ) as mock_create_run,
            patch.object(launcher, "import_class") as mock_import,
            patch.object(launcher, "_finalize_process_run", new_callable=AsyncMock),
            patch(
                "snapper.application.process_manager.launcher.asyncio.get_event_loop"
            ) as mock_loop,
        ):
            mock_create_run.side_effect = Exception("DB error")
            mock_import.return_value = SimpleProcess
            mock_loop.return_value.run_in_executor = AsyncMock(side_effect=run_in_executor)
            await launcher.start_process(config)


class TestStopAllProcesses:
    """Tests for stop_all_processes method."""

    @pytest.mark.asyncio
    async def test_stop_all_processes_stops_native_processes(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Test native processes are stopped.

        Given: Native process in started_processes.
        When: stop_all_processes is called.
        Then: Process stop method is called.
        """
        mock_proc_info = MagicMock(spec=ProcessInstanceInfo)
        mock_proc_info.pid = 1234
        mock_proc_info.stop = AsyncMock()
        launcher.started_processes["native_proc"] = mock_proc_info
        launcher.process_lifecycles["native_proc"] = ProcessLifecycleEnum.LONG_RUNNING
        launcher.process_roles["native_proc"] = ProcessRoleEnum.CORE
        await launcher.stop_all_processes()
        mock_proc_info.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_all_processes_calls_stop_method(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Test stop method is called on stoppable processes.

        Given: Process with sync stop method.
        When: stop_all_processes is called.
        Then: Stop method called and process removed.
        """

        class StoppableProcess:
            def __init__(self) -> None:
                self.stopped = False

            def stop(self) -> None:
                self.stopped = True

        process = StoppableProcess()
        launcher.started_processes["stoppable"] = process
        launcher.process_lifecycles["stoppable"] = ProcessLifecycleEnum.LONG_RUNNING
        launcher.process_roles["stoppable"] = ProcessRoleEnum.CORE
        await launcher.stop_all_processes()
        assert process.stopped is True
        assert launcher.started_processes == {}

    @pytest.mark.asyncio
    async def test_stop_all_processes_async_stop_method(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Test async stop method is awaited.

        Given: Process with async stop method.
        When: stop_all_processes is called.
        Then: Stop method awaited and process stopped.
        """

        class AsyncStoppableProcess:
            def __init__(self) -> None:
                self.stopped = False

            async def stop(self) -> None:
                self.stopped = True

        process = AsyncStoppableProcess()
        launcher.started_processes["async_stoppable"] = process
        launcher.process_lifecycles["async_stoppable"] = ProcessLifecycleEnum.LONG_RUNNING
        launcher.process_roles["async_stoppable"] = ProcessRoleEnum.CORE
        await launcher.stop_all_processes()
        assert process.stopped is True

    @pytest.mark.asyncio
    async def test_stop_all_processes_error_handling(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Test error during stop is handled gracefully.

        Given: Process whose stop raises RuntimeError.
        When: stop_all_processes is called.
        Then: Error suppressed and process cleaned up.
        """

        class FailingStopProcess:
            def stop(self) -> None:
                raise RuntimeError("Stop failed")

        process = FailingStopProcess()
        launcher.started_processes["failing"] = process
        launcher.process_lifecycles["failing"] = ProcessLifecycleEnum.LONG_RUNNING
        launcher.process_roles["failing"] = ProcessRoleEnum.CORE
        await launcher.stop_all_processes()
        assert launcher.started_processes == {}


class TestGetProcessConfigs:
    """Tests for get_process_configs method."""

    @pytest.mark.asyncio
    async def test_get_process_configs_parses_json(self, launcher: ProcessLauncherService) -> None:
        """Test JSON settings are parsed to config models.

        Given: Database with process setting as JSON.
        When: get_process_configs is called.
        Then: ProcessConfigModel created from JSON.
        """
        mock_setting = MagicMock()
        mock_setting.key = "process_test"
        mock_setting.value = json.dumps(
            {
                "enabled": True,
                "mode": "thread",
                "class": "test.TestClass",
                "method": "start",
                "parameters": {},
                "lifecycle": "long-running",
                "role": "core",
            }
        )
        with patch(
            "snapper.application.process_manager.config_resolver.get_repository"
        ) as mock_get_repo:
            mock_repo = MagicMock()
            mock_session = AsyncMock()
            mock_result = MagicMock()
            mock_result.scalars.return_value.all.return_value = [mock_setting]
            mock_session.execute.return_value = mock_result
            mock_repo.session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_repo.session.return_value.__aexit__ = AsyncMock()
            mock_get_repo.return_value = mock_repo
            with patch(
                "snapper.application.process_manager.config_resolver.get_registered_processes"
            ) as mock_registry:
                mock_registry.return_value = {}
                configs = await launcher.get_process_configs()
                assert len(configs) == 1
                assert configs[0].name == "test"
                assert configs[0].enabled is True

    @pytest.mark.asyncio
    async def test_get_process_configs_invalid_json_logged(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Test invalid JSON settings are skipped.

        Given: Database with invalid JSON setting.
        When: get_process_configs is called.
        Then: Invalid config skipped, empty list returned.
        """
        mock_setting = MagicMock()
        mock_setting.key = "process_invalid"
        mock_setting.value = "not valid json {"
        with patch(
            "snapper.application.process_manager.config_resolver.get_repository"
        ) as mock_get_repo:
            mock_repo = MagicMock()
            mock_session = AsyncMock()
            mock_result = MagicMock()
            mock_result.scalars.return_value.all.return_value = [mock_setting]
            mock_session.execute.return_value = mock_result
            mock_repo.session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_repo.session.return_value.__aexit__ = AsyncMock()
            mock_get_repo.return_value = mock_repo
            with patch(
                "snapper.application.process_manager.config_resolver.get_registered_processes"
            ) as mock_registry:
                mock_registry.return_value = {}
                configs = await launcher.get_process_configs()
                assert len(configs) == 0

    @pytest.mark.asyncio
    async def test_get_process_configs_unknown_lifecycle_defaults(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Test unknown lifecycle defaults to LONG_RUNNING.

        Given: Config with unrecognized lifecycle value.
        When: get_process_configs is called.
        Then: Lifecycle defaults to LONG_RUNNING.
        """
        mock_setting = MagicMock()
        mock_setting.key = "process_test"
        mock_setting.value = json.dumps(
            {
                "enabled": True,
                "mode": "thread",
                "class": "test.TestClass",
                "method": "start",
                "lifecycle": "unknown_lifecycle",
                "role": "core",
            }
        )
        with patch(
            "snapper.application.process_manager.config_resolver.get_repository"
        ) as mock_get_repo:
            mock_repo = MagicMock()
            mock_session = AsyncMock()
            mock_result = MagicMock()
            mock_result.scalars.return_value.all.return_value = [mock_setting]
            mock_session.execute.return_value = mock_result
            mock_repo.session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_repo.session.return_value.__aexit__ = AsyncMock()
            mock_get_repo.return_value = mock_repo
            with patch(
                "snapper.application.process_manager.config_resolver.get_registered_processes"
            ) as mock_registry:
                mock_registry.return_value = {}
                configs = await launcher.get_process_configs()
                assert len(configs) == 1
                assert configs[0].lifecycle == ProcessLifecycleEnum.LONG_RUNNING

    @pytest.mark.asyncio
    async def test_get_process_configs_unknown_role_defaults(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Test unknown role defaults to CORE.

        Given: Config with unrecognized role value.
        When: get_process_configs is called.
        Then: Role defaults to CORE.
        """
        mock_setting = MagicMock()
        mock_setting.key = "process_test"
        mock_setting.value = json.dumps(
            {
                "enabled": True,
                "mode": "thread",
                "class": "test.TestClass",
                "method": "start",
                "lifecycle": "long-running",
                "role": "unknown_role",
            }
        )
        with patch(
            "snapper.application.process_manager.config_resolver.get_repository"
        ) as mock_get_repo:
            mock_repo = MagicMock()
            mock_session = AsyncMock()
            mock_result = MagicMock()
            mock_result.scalars.return_value.all.return_value = [mock_setting]
            mock_session.execute.return_value = mock_result
            mock_repo.session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_repo.session.return_value.__aexit__ = AsyncMock()
            mock_get_repo.return_value = mock_repo
            with patch(
                "snapper.application.process_manager.config_resolver.get_registered_processes"
            ) as mock_registry:
                mock_registry.return_value = {}
                configs = await launcher.get_process_configs()
                assert len(configs) == 1
                assert configs[0].role == ProcessRoleEnum.CORE

    @pytest.mark.asyncio
    async def test_get_process_configs_uses_metadata_parameters_schema(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Test parameters_schema from registry metadata is used.

        Given: Config with non-iterable tags and registry metadata.
        When: get_process_configs is called.
        Then: Schema from metadata used, invalid tags ignored.
        """
        mock_setting = MagicMock()
        mock_setting.key = "process_test"
        mock_setting.value = json.dumps(
            {
                "enabled": True,
                "mode": "thread",
                "class": "test.TestClass",
                "method": "start",
                "lifecycle": "long-running",
                "role": "core",
                "tags": "oops-not-iterable",
                "parameters_schema": None,
            }
        )
        metadata = {
            "test": ProcessRegistryEntry(
                class_ref=cast(Any, type),
                class_path="test.TestClass",
                method="start",
                description="Test",
                priority=1,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.CORE,
                tags=("meta_tag",),
                parameters_model=None,
                parameters_schema={"type": "object"},
                enabled=False,
                mode="thread",
            )
        }
        with patch(
            "snapper.application.process_manager.config_resolver.get_repository"
        ) as mock_get_repo:
            mock_repo = MagicMock()
            mock_session = AsyncMock()
            mock_result = MagicMock()
            mock_result.scalars.return_value.all.return_value = [mock_setting]
            mock_session.execute.return_value = mock_result
            mock_repo.session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_repo.session.return_value.__aexit__ = AsyncMock()
            mock_get_repo.return_value = mock_repo
            with patch(
                "snapper.application.process_manager.config_resolver.get_registered_processes"
            ) as mock_registry:
                mock_registry.return_value = metadata
                configs = await launcher.get_process_configs()
                assert len(configs) == 1
                assert configs[0].tags == ()
                assert configs[0].parameters_schema == {"type": "object"}


class TestProcessRunRecords:
    """Tests for process run record management."""

    @pytest.mark.asyncio
    async def test_create_process_run_record(self, launcher: ProcessLauncherService) -> None:
        """Test process run record is created in database.

        Given: Valid process configuration.
        When: _create_process_run_record is called.
        Then: Record added to database and run_id returned.
        """
        config = ProcessConfigModel(
            name="test_process",
            enabled=True,
            mode="thread",
            class_path="test.TestClass",
            method="start",
            parameters={},
            lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
            role=ProcessRoleEnum.CORE,
            tags=("tag1", "tag2"),
        )
        with patch(
            "snapper.application.process_manager.run_recorder.get_repository"
        ) as mock_get_repo:
            mock_repo = MagicMock()
            mock_session = MagicMock()
            mock_session.commit = AsyncMock()
            mock_repo.session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_repo.session.return_value.__aexit__ = AsyncMock()
            mock_get_repo.return_value = mock_repo
            run_id = await launcher._create_process_run_record(config, {"mode": "thread"})
            assert run_id is not None
            mock_session.add.assert_called_once()
            mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_process_run_record_not_found(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Test update handles missing run record gracefully.

        Given: Non-existent run_id.
        When: _update_process_run_record is called.
        Then: No exception raised, operation completes.
        """
        with patch(
            "snapper.application.process_manager.run_recorder.get_repository"
        ) as mock_get_repo:
            mock_repo = MagicMock()
            mock_session = AsyncMock()
            mock_result = MagicMock()
            mock_result.scalar_one_or_none.return_value = None
            mock_session.execute.return_value = mock_result
            mock_repo.session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_repo.session.return_value.__aexit__ = AsyncMock()
            mock_get_repo.return_value = mock_repo
            await launcher._update_process_run_record(
                "nonexistent-run-id",
                ProcessRunStatusEnum.FAILED,
                error="Test error",
            )

    @pytest.mark.asyncio
    async def test_update_process_run_record_with_result(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Test run record close+insert with result.

        Given: Existing run record in database.
        When: _update_process_run_record is called with result.
        Then: Old row closed via UPDATE, new row added via session.add.
        """
        mock_run = MagicMock()
        mock_run.public_id = "test-run-id"
        mock_run.process_name = "test-proc"
        mock_run.role = "worker"
        mock_run.lifecycle = "transient"
        mock_run.parameters = None
        mock_run.result = None
        mock_run.error = None
        mock_run.tags = []
        mock_run.started_at = datetime(2024, 1, 1, tzinfo=UTC)
        mock_run.id = 10
        with patch(
            "snapper.application.process_manager.run_recorder.get_repository"
        ) as mock_get_repo:
            mock_repo = MagicMock()
            mock_session = AsyncMock()
            mock_session.add = MagicMock()
            mock_result = MagicMock()
            mock_result.scalar_one_or_none.return_value = mock_run
            mock_session.execute.return_value = mock_result
            mock_repo.session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_repo.session.return_value.__aexit__ = AsyncMock()
            mock_get_repo.return_value = mock_repo
            await launcher._update_process_run_record(
                "test-run-id",
                ProcessRunStatusEnum.SUCCEEDED,
                result={"output": "success"},
            )
            assert mock_session.execute.call_count == 2
            mock_session.add.assert_called_once()
            mock_session.commit.assert_called_once()


class TestStartProcessByName:
    """Tests for start_process_by_name method."""

    @pytest.mark.asyncio
    async def test_start_process_by_name_already_running(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Test starting already running process returns status.

        Given: Process already in started_processes.
        When: start_process_by_name is called.
        Then: Returns already_running status.
        """
        launcher.started_processes["running_process"] = MagicMock()
        result = await launcher.start_process_by_name("running_process")
        assert result.status == "already_running"

    @pytest.mark.asyncio
    async def test_start_process_by_name_not_found(self, launcher: ProcessLauncherService) -> None:
        """Test starting nonexistent process returns error.

        Given: No process config in database.
        When: start_process_by_name is called.
        Then: Returns error status with not found message.
        """
        with patch("snapper.application.process_manager.launcher.get_repository") as mock_get_repo:
            mock_repo = MagicMock()
            mock_session = AsyncMock()
            mock_result = MagicMock()
            mock_result.scalar_one_or_none.return_value = None
            mock_session.execute.return_value = mock_result
            mock_repo.session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_repo.session.return_value.__aexit__ = AsyncMock()
            mock_get_repo.return_value = mock_repo
            result = await launcher.start_process_by_name("nonexistent")
            assert result.status == "error"
            assert "not found" in result.message


class TestStopProcessByName:
    """Tests for stop_process_by_name method."""

    @pytest.mark.asyncio
    async def test_stop_process_by_name_not_running(self, launcher: ProcessLauncherService) -> None:
        """Test stopping non-running process returns status.

        Given: Process not in started_processes.
        When: stop_process_by_name is called.
        Then: Returns not_running status.
        """
        result = await launcher.stop_process_by_name("not_running")
        assert result.status == "not_running"

    @pytest.mark.asyncio
    async def test_stop_process_by_name_cancels_task(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Test stopping process cancels its asyncio task.

        Given: Running process with associated task.
        When: stop_process_by_name is called.
        Then: Task cancelled, success status returned.
        """

        async def long_running() -> None:
            await asyncio.sleep(100)

        task = asyncio.create_task(long_running())
        mock_instance = MagicMock()
        mock_instance.stop = AsyncMock()
        launcher.started_processes["task_process"] = mock_instance
        launcher.process_tasks["task_process"] = task
        launcher.process_lifecycles["task_process"] = ProcessLifecycleEnum.LONG_RUNNING
        with patch("snapper.application.process_manager.launcher.get_repository") as mock_get_repo:
            mock_repo = MagicMock()
            mock_session = AsyncMock()
            mock_session.add = MagicMock()
            mock_result = MagicMock()
            mock_setting = MagicMock()
            mock_setting.value = json.dumps({"enabled": True})
            mock_result.scalar_one_or_none.return_value = mock_setting
            mock_session.execute.return_value = mock_result
            mock_repo.session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_repo.session.return_value.__aexit__ = AsyncMock()
            mock_get_repo.return_value = mock_repo
            with patch.object(launcher, "_finalize_process_run", new_callable=AsyncMock):
                result = await launcher.stop_process_by_name("task_process")
            assert result.status == "success"
            assert task.cancelled()

    @pytest.mark.asyncio
    async def test_stop_process_by_name_native_process(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Test stopping native subprocess.

        Given: Native process with ProcessInstanceInfo.
        When: stop_process_by_name is called.
        Then: Process stop method called, success returned.
        """
        mock_proc_info = MagicMock(spec=ProcessInstanceInfo)
        mock_proc_info.pid = 1234
        mock_proc_info.stop = AsyncMock()
        launcher.started_processes["native"] = mock_proc_info
        launcher.process_lifecycles["native"] = ProcessLifecycleEnum.LONG_RUNNING
        with (
            patch("snapper.application.process_manager.launcher.get_repository") as mock_get_repo,
            patch.object(launcher, "_finalize_process_run", new_callable=AsyncMock),
        ):
            mock_repo = MagicMock()
            mock_session = AsyncMock()
            mock_session.add = MagicMock()
            mock_result = MagicMock()
            mock_setting = MagicMock()
            mock_setting.value = json.dumps({"enabled": True})
            mock_result.scalar_one_or_none.return_value = mock_setting
            mock_session.execute.return_value = mock_result
            mock_repo.session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_repo.session.return_value.__aexit__ = AsyncMock()
            mock_get_repo.return_value = mock_repo
            result = await launcher.stop_process_by_name("native")
            assert result.status == "success"
            mock_proc_info.stop.assert_called_once()


class TestGetProcessStatus:
    """Tests for get_process_status method."""

    @pytest.mark.asyncio
    async def test_get_process_status_running(self, launcher: ProcessLauncherService) -> None:
        """Test status for running process.

        Given: Process in started_processes dict.
        When: get_process_status is called.
        Then: Returns status with running=True and run_id.
        """
        launcher.started_processes["running"] = MagicMock()
        launcher.process_roles["running"] = ProcessRoleEnum.CORE
        launcher.process_lifecycles["running"] = ProcessLifecycleEnum.LONG_RUNNING
        launcher.active_runs["running"] = "test-run-id"
        status = await launcher.get_process_status("running")
        assert status.name == "running"
        assert status.running is True
        assert status.active_public_id == "test-run-id"

    @pytest.mark.asyncio
    async def test_get_process_status_not_running(self, launcher: ProcessLauncherService) -> None:
        """Test status for non-running process.

        Given: Process not in started_processes.
        When: get_process_status is called.
        Then: Returns status with running=False.
        """
        status = await launcher.get_process_status("not_running")
        assert status.name == "not_running"
        assert status.running is False

    @pytest.mark.asyncio
    async def test_get_process_status_with_details(self, launcher: ProcessLauncherService) -> None:
        """Test status includes process details.

        Given: Process with get_status method.
        When: get_process_status is called.
        Then: Returns status with details from process.
        """

        class ProcessWithStatus:
            def get_status(self) -> dict[str, Any]:
                return {"connections": 5, "messages": 100}

        process = ProcessWithStatus()
        launcher.started_processes["with_status"] = process
        launcher.process_roles["with_status"] = ProcessRoleEnum.CORE
        launcher.process_lifecycles["with_status"] = ProcessLifecycleEnum.LONG_RUNNING
        status = await launcher.get_process_status("with_status")
        assert status.details == {"connections": 5, "messages": 100}

    @pytest.mark.asyncio
    async def test_get_process_status_details_error_handled(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Test error in get_status is handled.

        Given: Process with broken get_status method.
        When: get_process_status is called.
        Then: Returns status without details, no exception.
        """

        class ProcessWithBrokenStatus:
            def get_status(self) -> dict[str, Any]:
                raise RuntimeError("Status unavailable")

        process = ProcessWithBrokenStatus()
        launcher.started_processes["broken_status"] = process
        launcher.process_roles["broken_status"] = ProcessRoleEnum.CORE
        launcher.process_lifecycles["broken_status"] = ProcessLifecycleEnum.LONG_RUNNING
        status = await launcher.get_process_status("broken_status")
        assert status.running is True
        assert status.details is None


class TestGetRecentRuns:
    """Tests for get_recent_runs method."""

    @pytest.mark.asyncio
    async def test_get_recent_runs_returns_formatted_data(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Test recent runs are returned with formatted data.

        Given: Database with process run records.
        When: get_recent_runs is called.
        Then: Returns list of formatted run dictionaries.
        """
        mock_run = MagicMock()
        mock_run.public_id = "run-123"
        mock_run.session_id = "test-sid"
        mock_run.sequence_id = 1
        mock_run.timestamp = datetime(2024, 1, 1, tzinfo=UTC)
        mock_run.process_name = "test_process"
        mock_run.status = "succeeded"
        mock_run.role = "core"
        mock_run.lifecycle = "long-running"
        mock_run.parameters = {"key": "value"}
        mock_run.result = None
        mock_run.error = None
        mock_run.tags = ["tag1"]
        mock_run.started_at = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        mock_run.completed_at = datetime(2024, 1, 1, 12, 5, 0, tzinfo=UTC)
        with patch(
            "snapper.application.process_manager.run_recorder.get_repository"
        ) as mock_get_repo:
            mock_repo = MagicMock()
            mock_session = AsyncMock()
            mock_result = MagicMock()
            mock_result.scalars.return_value.all.return_value = [mock_run]
            mock_session.execute.return_value = mock_result
            mock_repo.session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_repo.session.return_value.__aexit__ = AsyncMock()
            mock_get_repo.return_value = mock_repo
            runs = await launcher.get_recent_runs(limit=10)
            assert len(runs) == 1
            assert runs[0]["public_id"] == "run-123"
            assert runs[0]["process_name"] == "test_process"
            assert runs[0]["tags"] == ["tag1"]


class TestTaskCompletion:
    """Tests for task completion handling."""

    @pytest.mark.asyncio
    async def test_handle_task_completion_success(self, launcher: ProcessLauncherService) -> None:
        """Test successful task completion updates status.

        Given: Task that completes successfully.
        When: _handle_task_completion is called.
        Then: Run finalized with SUCCEEDED status.
        """

        async def completing_task() -> str:
            return "done"

        task = asyncio.create_task(completing_task())
        await task
        launcher.process_tasks["completed_task"] = task
        launcher.started_processes["completed_task"] = MagicMock()
        launcher.process_lifecycles["completed_task"] = ProcessLifecycleEnum.ONE_SHOT
        with patch.object(
            launcher, "_finalize_process_run", new_callable=AsyncMock
        ) as mock_finalize:
            await launcher._handle_task_completion("completed_task", task)
            mock_finalize.assert_called_once()
            call_args = mock_finalize.call_args
            assert call_args[0][1] == ProcessRunStatusEnum.SUCCEEDED
        assert "completed_task" not in launcher.started_processes

    @pytest.mark.asyncio
    async def test_handle_task_completion_with_exception(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Test task with exception sets failed status.

        Given: Task that raises ValueError.
        When: _handle_task_completion is called.
        Then: Run finalized with FAILED status.
        """

        async def failing_task() -> None:
            raise ValueError("Task failed")

        task = asyncio.create_task(failing_task())
        with contextlib.suppress(ValueError):
            await task
        launcher.process_tasks["failed_task"] = task
        launcher.started_processes["failed_task"] = MagicMock()
        launcher.process_lifecycles["failed_task"] = ProcessLifecycleEnum.LONG_RUNNING
        with patch.object(
            launcher, "_finalize_process_run", new_callable=AsyncMock
        ) as mock_finalize:
            await launcher._handle_task_completion("failed_task", task)
            mock_finalize.assert_called_once()
            call_args = mock_finalize.call_args
            assert call_args[0][1] == ProcessRunStatusEnum.FAILED

    @pytest.mark.asyncio
    async def test_handle_task_completion_expected_termination(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Test expected termination sets cancelled status.

        Given: Task in expected_terminations set.
        When: _handle_task_completion is called.
        Then: Run finalized with CANCELLED status.
        """

        async def completing_task() -> None:
            """Intentionally empty async stub for testing."""
            pass

        task = asyncio.create_task(completing_task())
        await task
        launcher.process_tasks["expected_stop"] = task
        launcher.started_processes["expected_stop"] = MagicMock()
        launcher.process_lifecycles["expected_stop"] = ProcessLifecycleEnum.LONG_RUNNING
        launcher.expected_terminations.add("expected_stop")
        with patch.object(
            launcher, "_finalize_process_run", new_callable=AsyncMock
        ) as mock_finalize:
            await launcher._handle_task_completion("expected_stop", task)
            mock_finalize.assert_called_once()
            call_args = mock_finalize.call_args
            assert call_args[0][1] == ProcessRunStatusEnum.CANCELLED


class TestNativeProcessMonitoring:
    """Tests for native process monitoring."""

    @pytest.mark.asyncio
    async def test_start_native_process_monitoring_no_native_processes(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Test monitoring not started without native processes.

        Given: No native processes in started_processes.
        When: _start_native_process_monitoring is called.
        Then: No monitor task created.
        """
        launcher._start_native_process_monitoring()
        assert "_native_monitor" not in launcher.process_tasks

    @pytest.mark.asyncio
    async def test_start_native_process_monitoring_with_native_processes(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Test monitoring started for native processes.

        Given: ProcessInstanceInfo in started_processes.
        When: _start_native_process_monitoring is called.
        Then: Monitor task created.
        """
        proc_info = ProcessInstanceInfo(
            name="native_proc",
            pid=1234,
            started_at=datetime.now(UTC),
            config={},
            process=MagicMock(),
            spawner=launcher.spawner,
        )
        launcher.started_processes["native_proc"] = proc_info
        launcher._start_native_process_monitoring()
        assert "_native_monitor" in launcher.process_tasks
        launcher.process_tasks["_native_monitor"].cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await launcher.process_tasks["_native_monitor"]

    @pytest.mark.asyncio
    async def test_start_native_process_monitoring_already_running(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Test monitoring not duplicated if already running.

        Given: Monitor task already exists.
        When: _start_native_process_monitoring is called again.
        Then: Same monitor task retained.
        """
        proc_info = ProcessInstanceInfo(
            name="native_proc",
            pid=1234,
            started_at=datetime.now(UTC),
            config={},
            process=MagicMock(),
            spawner=launcher.spawner,
        )
        launcher.started_processes["native_proc"] = proc_info
        launcher._start_native_process_monitoring()
        first_monitor = launcher.process_tasks["_native_monitor"]
        launcher._start_native_process_monitoring()
        assert launcher.process_tasks["_native_monitor"] is first_monitor
        first_monitor.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await first_monitor


class TestSyncRegistryToDatabase:
    """Tests for sync_registry_to_database method."""

    @pytest.mark.asyncio
    async def test_sync_registry_creates_new_config(self, launcher: ProcessLauncherService) -> None:
        """Test sync creates config for new registry entry.

        Given: Registry with process not in database.
        When: sync_registry_to_database is called.
        Then: New config created in database.
        """

        class TestProcess(RegisterableProcess):
            async def start(self) -> None:
                """No-op start for TestProcess test stub."""
                pass

        with (
            patch(
                "snapper.application.process_manager.registry_syncer.get_registered_processes"
            ) as mock_registry,
            patch(
                "snapper.application.process_manager.registry_syncer.get_repository"
            ) as mock_get_repo,
            patch.object(
                launcher._registry_syncer, "_create_process_config_in_db", new_callable=AsyncMock
            ) as mock_create,
        ):
            mock_registry.return_value = {
                "new_process": ProcessRegistryEntry(
                    class_ref=TestProcess,
                    class_path="test.TestProcess",
                    method="start",
                    description="Test process",
                    priority=10,
                    lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                    role=ProcessRoleEnum.CORE,
                    tags=(),
                    parameters_model=None,
                    parameters_schema=None,
                    enabled=False,
                    mode="thread",
                )
            }
            mock_repo = MagicMock()
            mock_session = AsyncMock()
            mock_result = MagicMock()
            mock_result.scalar_one_or_none.return_value = None
            mock_session.execute.return_value = mock_result
            mock_repo.session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_repo.session.return_value.__aexit__ = AsyncMock()
            mock_get_repo.return_value = mock_repo
            await launcher.sync_registry_to_database()
            mock_create.assert_called_once()


class TestCreateProcessConfig:
    """Tests for create_process_config method."""

    @pytest.mark.asyncio
    async def test_create_process_config_success(self, launcher: ProcessLauncherService) -> None:
        """Test process config is created in database.

        Given: Valid process configuration parameters.
        When: create_process_config is called.
        Then: Setting added to database and committed.
        """
        with patch(
            "snapper.application.process_manager.registry_syncer.get_repository"
        ) as mock_get_repo:
            mock_repo = MagicMock()
            mock_session = MagicMock()
            mock_result = MagicMock()
            mock_result.scalar_one_or_none.return_value = None
            mock_session.execute = AsyncMock(return_value=mock_result)
            mock_session.commit = AsyncMock()
            mock_repo.session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_repo.session.return_value.__aexit__ = AsyncMock()
            mock_get_repo.return_value = mock_repo
            await launcher.create_process_config(
                name="new_process",
                class_path="test.NewProcess",
                method="start",
                enabled=True,
                mode="thread",
                parameters={},
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.CORE,
                tags=["tag1"],
                parameters_schema={"type": "object"},
                note="Test note",
            )
            mock_session.add.assert_called_once()
            mock_session.commit.assert_called_once()


class TestUpdateProcessConfig:
    """Tests for update_process_config (desired-state PATCH DAL)."""

    @staticmethod
    def _mock_repo_with_existing(existing: object) -> tuple[MagicMock, MagicMock]:
        """Build a mocked repository whose active-row query returns ``existing``.

        Args:
            existing: The Setting the active-now query resolves to (or None).

        Returns:
            A ``(mock_get_repo_value, mock_session)`` pair.
        """
        mock_repo = MagicMock()
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = existing
        mock_session.execute = AsyncMock(return_value=mock_result)
        mock_session.commit = AsyncMock()
        mock_repo.session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
        mock_repo.session.return_value.__aexit__ = AsyncMock(return_value=False)
        return mock_repo, mock_session

    @staticmethod
    def _existing_setting(value: JsonObject) -> MagicMock:
        """Return a mock Setting whose JSON value is ``value``.

        Args:
            value: The config dict serialised into the Setting value.

        Returns:
            A mock Setting with category/description/is_encrypted set.
        """
        existing = MagicMock()
        existing.value = json.dumps(value)
        existing.category = "process"
        existing.description = None
        existing.is_encrypted = False
        return existing

    @pytest.mark.asyncio
    async def test_update_flips_enabled_preserving_other_keys(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Flipping enabled rewrites only that key; other JSON keys round-trip.

        Given: an active config with enabled=True and extra parameters,
        When: update_process_config(enabled=False) is called,
        Then: close_and_insert persists enabled=False with parameters intact,
            no restart_nonce added, and the operator principal stamped.
        """
        existing = self._existing_setting(
            {"class": "a.B", "enabled": True, "mode": "thread", "parameters": {"k": "v"}}
        )
        mock_repo, mock_session = self._mock_repo_with_existing(existing)
        with (
            patch(
                "snapper.application.process_manager.registry_syncer.get_repository",
                return_value=mock_repo,
            ),
            patch(
                "snapper.application.process_manager.registry_syncer.close_and_insert",
                new_callable=AsyncMock,
            ) as mock_cai,
        ):
            await launcher._registry_syncer.update_process_config(
                name="p", enabled=False, updated_by="alice"
            )
        await_args = mock_cai.await_args
        assert await_args is not None
        written = json.loads(await_args.kwargs["new_values"]["value"])
        assert written["enabled"] is False
        assert written["parameters"] == {"k": "v"}
        assert "restart_nonce" not in written
        assert await_args.kwargs["new_values"]["updated_by"] == "alice"
        mock_session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_update_sets_restart_nonce_leaving_enabled_untouched(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Setting only the restart nonce leaves enabled at its stored value.

        Given: an active config with enabled=True,
        When: update_process_config(restart_nonce='n1') is called (enabled=None),
        Then: restart_nonce is written and enabled stays True (unchanged).
        """
        existing = self._existing_setting({"class": "a.B", "enabled": True, "parameters": {}})
        mock_repo, _ = self._mock_repo_with_existing(existing)
        with (
            patch(
                "snapper.application.process_manager.registry_syncer.get_repository",
                return_value=mock_repo,
            ),
            patch(
                "snapper.application.process_manager.registry_syncer.close_and_insert",
                new_callable=AsyncMock,
            ) as mock_cai,
        ):
            await launcher._registry_syncer.update_process_config(
                name="p", restart_nonce="n1", updated_by="bob"
            )
        await_args = mock_cai.await_args
        assert await_args is not None
        written = json.loads(await_args.kwargs["new_values"]["value"])
        assert written["restart_nonce"] == "n1"
        assert written["enabled"] is True

    @pytest.mark.asyncio
    async def test_update_raises_keyerror_when_config_absent(
        self, launcher: ProcessLauncherService
    ) -> None:
        """A missing active config raises KeyError (the REST layer maps to 404)."""
        mock_repo, _ = self._mock_repo_with_existing(None)
        with (
            patch(
                "snapper.application.process_manager.registry_syncer.get_repository",
                return_value=mock_repo,
            ),
            pytest.raises(KeyError),
        ):
            await launcher._registry_syncer.update_process_config(
                name="ghost", enabled=True, updated_by="alice"
            )


class TestHandleProcessCompletion:
    """Tests for _handle_process_completion method."""

    @pytest.mark.asyncio
    async def test_handle_process_completion_success_exit_code(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Test successful completion with exit code 0.

        Given: Native process with returncode 0.
        When: _handle_process_completion is called.
        Then: Run finalized with SUCCEEDED status.
        """
        mock_process = MagicMock()
        mock_process.returncode = 0
        mock_proc_info = MagicMock(spec=ProcessInstanceInfo)
        mock_proc_info.process = mock_process
        launcher.process_lifecycles["native"] = ProcessLifecycleEnum.ONE_SHOT
        launcher.started_processes["native"] = mock_proc_info
        with (
            patch.object(launcher.spawner, "cleanup"),
            patch.object(
                launcher, "_finalize_process_run", new_callable=AsyncMock
            ) as mock_finalize,
        ):
            await launcher._handle_process_completion("native", mock_proc_info)
            mock_finalize.assert_called_once()
            call_args = mock_finalize.call_args
            assert call_args[0][1] == ProcessRunStatusEnum.SUCCEEDED

    @pytest.mark.asyncio
    async def test_handle_process_completion_failure_exit_code(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Test completion with non-zero exit sets failed status.

        Given: Native process with returncode 1.
        When: _handle_process_completion is called.
        Then: Run finalized with FAILED status.
        """
        mock_process = MagicMock()
        mock_process.returncode = 1
        mock_proc_info = MagicMock(spec=ProcessInstanceInfo)
        mock_proc_info.process = mock_process
        launcher.process_lifecycles["native"] = ProcessLifecycleEnum.LONG_RUNNING
        launcher.started_processes["native"] = mock_proc_info
        with (
            patch.object(launcher.spawner, "cleanup"),
            patch.object(
                launcher, "_finalize_process_run", new_callable=AsyncMock
            ) as mock_finalize,
        ):
            await launcher._handle_process_completion("native", mock_proc_info)
            mock_finalize.assert_called_once()
            call_args = mock_finalize.call_args
            assert call_args[0][1] == ProcessRunStatusEnum.FAILED

    @pytest.mark.asyncio
    async def test_handle_process_completion_expected_termination(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Test expected termination sets cancelled status.

        Given: Process in expected_terminations set.
        When: _handle_process_completion is called.
        Then: Run finalized with CANCELLED status.
        """
        mock_process = MagicMock()
        mock_process.returncode = 0
        mock_proc_info = MagicMock(spec=ProcessInstanceInfo)
        mock_proc_info.process = mock_process
        launcher.process_lifecycles["native"] = ProcessLifecycleEnum.LONG_RUNNING
        launcher.started_processes["native"] = mock_proc_info
        launcher.expected_terminations.add("native")
        with (
            patch.object(launcher.spawner, "cleanup"),
            patch.object(
                launcher, "_finalize_process_run", new_callable=AsyncMock
            ) as mock_finalize,
        ):
            await launcher._handle_process_completion("native", mock_proc_info)
            mock_finalize.assert_called_once()
            call_args = mock_finalize.call_args
            assert call_args[0][1] == ProcessRunStatusEnum.CANCELLED


class TestGetDefaultsFromMetadata:
    """Tests for _get_defaults_from_entry method."""

    def test_get_defaults_from_metadata_with_values(self, launcher: ProcessLauncherService) -> None:
        """Test metadata values are extracted correctly.

        Given: ProcessRegistryEntry with all fields.
        When: _get_defaults_from_entry is called.
        Then: All values extracted to defaults dict.
        """
        entry = ProcessRegistryEntry(
            class_ref=cast(Any, type),
            class_path="test.TestClass",
            method="start",
            description="Test",
            priority=10,
            lifecycle=ProcessLifecycleEnum.ONE_SHOT,
            role=ProcessRoleEnum.TASK,
            tags=("tag1", "tag2"),
            parameters_model=None,
            parameters_schema={"type": "object"},
            enabled=True,
            mode="process",
        )
        defaults = launcher._registry_syncer._get_defaults_from_entry(entry)
        assert defaults["enabled"] is True
        assert defaults["mode"] == "process"
        assert defaults["lifecycle"] == ProcessLifecycleEnum.ONE_SHOT
        assert defaults["role"] == ProcessRoleEnum.TASK
        assert defaults["tags"] == ["tag1", "tag2"]
        assert defaults["parameters_schema"] == {"type": "object"}

    def test_get_defaults_from_metadata_empty(self, launcher: ProcessLauncherService) -> None:
        """Test entry with default values.

        Given: ProcessRegistryEntry with minimal/default values.
        When: _get_defaults_from_entry is called.
        Then: Default values extracted from entry.
        """
        entry = ProcessRegistryEntry(
            class_ref=cast(Any, type),
            class_path="test.TestClass",
            method="start",
            description="Test",
            priority=10,
            lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
            role=ProcessRoleEnum.CORE,
            tags=(),
            parameters_model=None,
            parameters_schema=None,
            enabled=False,
            mode="thread",
        )
        defaults = launcher._registry_syncer._get_defaults_from_entry(entry)
        assert defaults["enabled"] is False
        assert defaults["mode"] == "thread"
        assert defaults["parameters"] == {}
        assert defaults["lifecycle"] == ProcessLifecycleEnum.LONG_RUNNING
        assert defaults["role"] == ProcessRoleEnum.CORE
        assert defaults["tags"] == []
        assert defaults["parameters_schema"] is None


class _DummySettingsServiceV2:
    """Alternative stub settings service."""

    def get_setting(self, key: str, default: Any) -> Any:
        """Return default value for any key."""
        return default


def _settings() -> AppSettings:
    """Create AppSettings with stub dependencies."""
    return AppSettings(BootstrapSettingsLoader(), _DummySettingsService())


@pytest.mark.asyncio
async def test_start_native_process_monitoring_skips_when_monitor_exists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify native process monitoring skips task creation when monitor exists.

    Given a ProcessLauncherService with an active _native_monitor task,
    When _start_native_process_monitoring is called,
    Then the existing monitor task is preserved and no new task is created.
    """
    launcher = ProcessLauncherService(_settings())
    existing_task = MagicMock()
    existing_task.done.return_value = False
    launcher.process_tasks["_native_monitor"] = existing_task
    launcher._start_native_process_monitoring()
    assert launcher.process_tasks["_native_monitor"] is existing_task


@pytest.mark.asyncio
async def test_start_native_process_monitoring_creates_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify native process monitoring creates task for active processes.

    Given a ProcessLauncherService with native processes in started_processes,
    When _start_native_process_monitoring is called,
    Then a new monitor task is created and registered in process_tasks.
    """
    launcher = ProcessLauncherService(_settings())
    proc = ProcessInstanceInfo(
        name="native",
        pid=1,
        started_at=datetime.now(UTC),
        config={},
        process=MagicMock(),
    )
    launcher.started_processes["native"] = proc
    monkeypatch.setattr(launcher, "_monitor_native_processes", AsyncMock(return_value=None))
    created: list[asyncio.Task[None]] = []
    original_create_task = asyncio.create_task

    def fake_create_task(coro: Any) -> asyncio.Task[None]:
        task = original_create_task(coro)
        created.append(task)
        return task

    monkeypatch.setattr(asyncio, "create_task", fake_create_task)
    launcher._start_native_process_monitoring()
    await asyncio.sleep(0)
    assert created, "monitor task should be scheduled"
    assert launcher.process_tasks.get("_native_monitor") in created


@pytest.mark.asyncio
async def test_monitor_native_processes_triggers_completion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify native process monitor triggers completion handler on exit.

    Given a ProcessLauncherService with a running native process,
    When the process exits and _monitor_native_processes detects it,
    Then the completion handler is called and the process is removed.
    """
    launcher = ProcessLauncherService(_settings())
    proc = ProcessInstanceInfo(
        name="native",
        pid=2,
        started_at=datetime.now(UTC),
        config={},
        process=MagicMock(returncode=0),
        spawner=launcher.spawner,
    )
    launcher.started_processes["native"] = proc

    def mock_get_status(name: str) -> SpawnerStatusSnapshot:
        return SpawnerStatusSnapshot(name=name, running=False)

    launcher.spawner.get_status = mock_get_status
    called: list[str] = []

    async def fake_handle(name: str, info: ProcessInstanceInfo) -> None:
        called.append(name)
        launcher.started_processes.pop(name, None)

    monkeypatch.setattr(launcher, "_handle_process_completion", fake_handle)
    monkeypatch.setattr(asyncio, "sleep", AsyncMock(return_value=None))
    await launcher._monitor_native_processes()
    assert called == ["native"]
    assert "native" not in launcher.started_processes


@pytest.mark.asyncio
async def test_handle_process_completion_long_running_unexpected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify unexpected termination of long-running process triggers finalization.

    Given a ProcessLauncherService with a long-running process,
    When the process terminates unexpectedly with a non-zero return code,
    Then the process is finalized and removed from active processes.
    """
    launcher = ProcessLauncherService(_settings())
    process = MagicMock()
    process.returncode = 5
    proc_info = ProcessInstanceInfo(
        name="worker",
        pid=10,
        started_at=datetime.now(UTC),
        config={},
        process=process,
        spawner=launcher.spawner,
    )
    launcher.process_lifecycles["worker"] = ProcessLifecycleEnum.LONG_RUNNING
    launcher.started_processes["worker"] = proc_info
    finalize_calls: list[tuple[str, ProcessLifecycleEnum]] = []

    async def fake_finalize(
        name: str,
        status: Any,
        error: str | None = None,
        exit_code: int | None = None,
    ) -> None:
        del error, exit_code
        finalize_calls.append((name, status))

    monkeypatch.setattr(launcher.spawner, "cleanup", lambda name: None)
    monkeypatch.setattr(launcher, "_finalize_process_run", fake_finalize)
    await launcher._handle_process_completion("worker", proc_info)
    assert finalize_calls
    assert finalize_calls[0][0] == "worker"


def test_resolve_lifecycle_returns_default_for_none() -> None:
    """Verify _resolve_lifecycle returns LONG_RUNNING when raw is None.

    Given a None lifecycle value,
    When _resolve_lifecycle is called,
    Then it returns ProcessLifecycleEnum.LONG_RUNNING.
    """
    result = ProcessLauncherService._resolve_lifecycle(None, "test_process")
    assert result == ProcessLifecycleEnum.LONG_RUNNING


def test_resolve_role_returns_default_for_none() -> None:
    """Verify _resolve_role returns CORE when raw is None.

    Given a None role value,
    When _resolve_role is called,
    Then it returns ProcessRoleEnum.CORE.
    """
    result = ProcessLauncherService._resolve_role(None, "test_process")
    assert result == ProcessRoleEnum.CORE


def test_resolve_mode_returns_thread_for_none() -> None:
    """Verify resolve_mode returns 'thread' when raw is None.

    Given a None mode value,
    When resolve_mode is called,
    Then it returns 'thread' as the default.
    """
    result = resolve_mode(None, "test_process")
    assert result == "thread"


def test_resolve_mode_raises_on_invalid_value() -> None:
    """Verify resolve_mode raises ValueError for unknown mode.

    Given an invalid mode value 'worker',
    When resolve_mode is called,
    Then it raises ValueError with a descriptive message.
    """
    with pytest.raises(ValueError, match="Invalid mode 'worker'"):
        resolve_mode("worker", "test_process")


def test_resolve_mode_accepts_valid_modes() -> None:
    """Verify resolve_mode accepts 'thread' and 'process'.

    Given valid ProcessMode values,
    When resolve_mode is called,
    Then it returns the value unchanged.
    """
    assert resolve_mode("thread", "test_process") == "thread"
    assert resolve_mode("process", "test_process") == "process"


def test_build_config_for_start_by_name_honors_restart_policy(
    launcher: ProcessLauncherService,
) -> None:
    """A restart_policy in the config dict is applied by the start-by-name builder.

    Given: A config dict carrying restart_policy for an unregistered process,
    When: _build_config_for_start_by_name resolves it,
    Then: The resulting ProcessConfigModel carries the requested policy.
    """
    config = launcher._build_config_for_start_by_name(
        "unregistered_proc",
        {"class": "a.B", "restart_policy": "always"},
        True,
    )
    assert config.restart_policy == ProcessRestartPolicyEnum.ALWAYS


def test_build_config_for_start_by_name_inherits_restart_policy_from_entry(
    launcher: ProcessLauncherService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the config omits restart_policy, the builder inherits it from the registry entry.

    Given: A registered entry with restart_policy ALWAYS and a config dict without it,
    When: _build_config_for_start_by_name resolves the config,
    Then: The resulting ProcessConfigModel inherits ALWAYS from the entry.
    """
    entry = ProcessRegistryEntry(
        class_ref=MagicMock(),
        class_path="a.B",
        method="start",
        description="",
        priority=0,
        lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
        role=ProcessRoleEnum.CORE,
        tags=(),
        parameters_model=None,
        parameters_schema=None,
        enabled=False,
        mode="thread",
        restart_policy=ProcessRestartPolicyEnum.ALWAYS,
    )
    monkeypatch.setattr(
        "snapper.application.process_manager.launcher.get_registered_processes",
        lambda: {"regd": entry},
    )
    config = launcher._build_config_for_start_by_name("regd", {"class": "a.B"}, True)
    assert config.restart_policy == ProcessRestartPolicyEnum.ALWAYS


def test_set_market_persist_policy_round_trip(launcher: ProcessLauncherService) -> None:
    """``set_market_persist_policy`` installs + clears the policy reference.

    Given: A launcher with no policy bound,
    When: ``set_market_persist_policy(policy)`` then ``set_market_persist_policy(None)``,
    Then: ``self._market_persist_policy`` mirrors each assignment.
    """
    policy = MagicMock()
    launcher.set_market_persist_policy(policy)
    assert launcher._market_persist_policy is policy
    launcher.set_market_persist_policy(None)
    assert launcher._market_persist_policy is None


def test_inject_market_persist_policy_calls_setter(launcher: ProcessLauncherService) -> None:
    """Process instance with ``set_persist_policy`` receives the policy.

    Given: A launcher with a configured policy + a process mock that
        exposes ``set_persist_policy``,
    When: ``_inject_market_persist_policy`` runs,
    Then: ``process.set_persist_policy`` is called once with the
        configured policy.
    """
    policy = MagicMock()
    launcher.set_market_persist_policy(policy)
    process = MagicMock()
    launcher._inject_market_persist_policy(process, "pub:test")
    process.set_persist_policy.assert_called_once_with(policy)


def test_inject_market_persist_policy_skips_without_policy(
    launcher: ProcessLauncherService,
) -> None:
    """No policy on the launcher leaves the process untouched.

    Given: A launcher cleared via ``set_market_persist_policy(None)``,
    When: ``_inject_market_persist_policy`` runs against a publisher mock,
    Then: ``process.set_persist_policy`` is never called.
    """
    launcher.set_market_persist_policy(None)
    process = MagicMock()
    launcher._inject_market_persist_policy(process, "pub:test")
    process.set_persist_policy.assert_not_called()


def test_inject_market_persist_policy_skips_non_publisher(
    launcher: ProcessLauncherService,
) -> None:
    """Process instances without ``set_persist_policy`` silently no-op.

    Given: A launcher with a configured policy + a process mock with
        no ``set_persist_policy`` attribute,
    When: ``_inject_market_persist_policy`` runs,
    Then: No exception is raised (duck-type guard via getattr).
    """
    launcher.set_market_persist_policy(MagicMock())
    process = MagicMock(spec=[])
    launcher._inject_market_persist_policy(process, "pub:test")


class _RecordingPublisher:
    """Stub MessagePublisher capturing every (topic, payload) pair.

    Mirrors :class:`snapper.messaging.infrastructure.publisher.MessagePublisher`
    well enough for the launcher emit paths — exposes ``tracker``
    (with ``session_id`` + ``next_sequence``) plus an ``async`` ``send``.

    Payload typing matches the production publisher's ``data:
    StrictDataSchema[Any]`` signature so test assertions can introspect
    the same schema base every emit site builds against. ``Any`` here
    is the generic type-literal slot inherited from the production
    contract, not an opaque ``Any`` escape hatch.
    """

    def __init__(self) -> None:
        """Initialize the capture buffer + a deterministic tracker."""
        self.sent: list[tuple[str, StrictDataSchema[Any]]] = []
        self._tracker = SequenceTracker()

    @property
    def tracker(self) -> SequenceTracker:
        """Expose the SequenceTracker for launcher emit helpers."""
        return self._tracker

    async def send(self, stream_key: str, data: StrictDataSchema[Any]) -> None:
        """Record the (topic, payload) pair for assertion."""
        self.sent.append((stream_key, data))


class _RaisingPublisher(_RecordingPublisher):
    """Publisher whose ``send`` raises after recording the call.

    Lets the emit-failure tests exercise the launcher's best-effort
    contract — a broker hiccup must NOT propagate into start / stop
    control flow.
    """

    async def send(self, stream_key: str, data: StrictDataSchema[Any]) -> None:
        """Record the call then raise to simulate a broker hiccup."""
        await super().send(stream_key, data)
        raise RuntimeError("broker unreachable")


class TestEmitHelpersNoPublisher:
    """All emit helpers no-op cleanly when no publisher is wired."""

    @pytest.mark.asyncio
    async def test_summary_emit_without_publisher_is_noop(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Summary emit silently returns when no publisher is wired.

        Given: A launcher with ``self._msg_publisher is None``,
        When: ``_emit_summary_snapshot`` is awaited,
        Then: No exception is raised.
        """
        await launcher._emit_summary_snapshot()

    @pytest.mark.asyncio
    async def test_configured_emit_without_publisher_is_noop(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Configured emit silently returns when no publisher is wired."""
        await launcher._emit_configured_snapshot()

    @pytest.mark.asyncio
    async def test_strategy_list_emit_without_publisher_is_noop(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Strategy-list emit silently returns when no publisher is wired."""
        await launcher._emit_strategy_list_snapshot()

    @pytest.mark.asyncio
    async def test_run_event_emit_without_publisher_is_noop(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Run-event emit silently returns when no publisher is wired."""
        await launcher._emit_run_event(
            process_name="proc",
            run_id="run-1",
            status=ProcessRunStatusEnum.RUNNING,
            started_at=datetime.now(UTC),
            completed_at=None,
            error=None,
        )


class TestEmitHelpersWithPublisher:
    """Each emit helper sends one (topic, payload) pair with the right shape."""

    @pytest.mark.asyncio
    async def test_set_msg_publisher_round_trip(self, launcher: ProcessLauncherService) -> None:
        """``set_msg_publisher`` installs + clears the publisher reference.

        Given: A launcher with no publisher bound,
        When: ``set_msg_publisher(publisher)`` then ``set_msg_publisher(None)``,
        Then: ``self._msg_publisher`` mirrors each assignment.
        """
        publisher = _RecordingPublisher()
        launcher.set_msg_publisher(publisher)
        assert launcher._msg_publisher is publisher
        assert launcher.message_publisher is publisher
        launcher.set_msg_publisher(None)
        assert launcher._msg_publisher is None
        assert launcher.message_publisher is None

    def test_coordinator_topic_slug_default(self, launcher: ProcessLauncherService) -> None:
        """Default ``coordinator_instance_id`` of ``0`` slugifies to ``coord-0``.

        Given: A launcher built from default ``BootstrapSettingsLoader``,
        When: ``coordinator_topic_slug()`` is called,
        Then: It returns ``"coord-0"`` and the result satisfies the
            registry validator pattern.
        """
        slug = launcher.coordinator_topic_slug()
        assert slug == "coord-0"
        valid, err = validate_topic(f"processes.events.summary.{slug}")
        assert valid, err

    def test_coordinator_topic_slug_with_nonzero_instance_id(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Non-zero ``coordinator_instance_id`` slugifies to ``coord-{n}``.

        Given: A launcher whose ``settings.coordinator_instance_id`` is ``7``,
        When: ``coordinator_topic_slug()`` is called,
        Then: It returns ``"coord-7"`` and the result satisfies the
            registry validator pattern (leading-letter constraint preserved
            even for numeric ids).
        """
        launcher.settings = SimpleNamespace(coordinator_instance_id=7)
        slug = launcher.coordinator_topic_slug()
        assert slug == "coord-7"
        valid, err = validate_topic(f"strategies.events.list.{slug}")
        assert valid, err

    @pytest.mark.asyncio
    async def test_summary_emit_publishes_snapshot(
        self, launcher: ProcessLauncherService, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Summary emit sends a snapshot on ``processes.events.summary.{id}``.

        Given: Two persisted process configs (one running, one stopped),
        When: ``_emit_summary_snapshot`` runs,
        Then: Publisher captures one ``ProcessSummaryEventData`` on the
            instance-id-suffixed topic, with both items present and
            ``running`` set correctly.
        """
        publisher = _RecordingPublisher()
        launcher.set_msg_publisher(publisher)
        running_cfg = ProcessConfigModel(
            name="trader_coordinator",
            enabled=True,
            mode="thread",
            class_path="x.Y",
            method="start",
            parameters={},
            note=None,
            lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
            role=ProcessRoleEnum.CORE,
            tags=(),
            parameters_schema=None,
        )
        stopped_cfg = ProcessConfigModel(
            name="paper_backfill",
            enabled=False,
            mode="thread",
            class_path="x.Z",
            method="start",
            parameters={},
            note=None,
            lifecycle=ProcessLifecycleEnum.ONE_SHOT,
            role=ProcessRoleEnum.STRATEGY,
            tags=(),
            parameters_schema=None,
        )
        launcher.get_process_configs = AsyncMock(return_value=[running_cfg, stopped_cfg])
        launcher.started_processes["trader_coordinator"] = MagicMock()

        await launcher._emit_summary_snapshot()

        assert len(publisher.sent) == 1
        topic, payload = publisher.sent[0]
        assert topic.startswith("processes.events.summary.")
        valid, err = validate_topic(topic)
        assert valid, f"emitted topic {topic!r} must satisfy registry validator: {err}"
        assert isinstance(payload, ProcessSummaryEventData)
        names = {item.name: item for item in payload.processes}
        assert names["trader_coordinator"].running is True
        assert names["paper_backfill"].running is False
        assert names["paper_backfill"].role == "strategy"

    @pytest.mark.asyncio
    async def test_summary_emit_send_failure_is_logged_not_raised(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Best-effort: a broker hiccup must not propagate.

        Given: A publisher whose ``send`` raises,
        When: ``_emit_summary_snapshot`` is awaited,
        Then: No exception escapes; the failure was logged.
        """
        publisher = _RaisingPublisher()
        launcher.set_msg_publisher(publisher)
        launcher.get_process_configs = AsyncMock(return_value=[])
        await launcher._emit_summary_snapshot()
        assert len(publisher.sent) == 1

    @pytest.mark.asyncio
    async def test_configured_emit_publishes_names(self, launcher: ProcessLauncherService) -> None:
        """Configured emit sends the sorted union of config + instance names."""
        publisher = _RecordingPublisher()
        launcher.set_msg_publisher(publisher)
        cfg_a = ProcessConfigModel(
            name="executor_kraken",
            enabled=True,
            mode="thread",
            class_path="x.Y",
            method="start",
            parameters={},
            note=None,
            lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
            role=ProcessRoleEnum.CORE,
            tags=(),
            parameters_schema=None,
        )
        instance_cfg = ProcessConfigModel(
            name="executor_kraken_w019dbb34f439",
            enabled=True,
            mode="thread",
            class_path="x.Y",
            method="start",
            parameters={"wallet_public_id": "wal-1"},
            note=None,
            lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
            role=ProcessRoleEnum.CORE,
            tags=(),
            parameters_schema=None,
        )
        launcher.get_process_configs = AsyncMock(return_value=[cfg_a])
        launcher.instance_configs["executor_kraken_w019dbb34f439"] = instance_cfg
        await launcher._emit_configured_snapshot()
        assert len(publisher.sent) == 1
        topic, payload = publisher.sent[0]
        assert topic.startswith("processes.events.configured.")
        valid, err = validate_topic(topic)
        assert valid, f"emitted topic {topic!r} must satisfy registry validator: {err}"
        assert isinstance(payload, ProcessConfiguredEventData)
        assert payload.process_names == [
            "executor_kraken",
            "executor_kraken_w019dbb34f439",
        ]

    @pytest.mark.asyncio
    async def test_strategy_list_emit_filters_to_strategy_role(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Strategy emit carries class_path of STRATEGY-role configs only."""
        publisher = _RecordingPublisher()
        launcher.set_msg_publisher(publisher)
        strategy_cfg = ProcessConfigModel(
            name="momentum",
            enabled=True,
            mode="thread",
            class_path="snapper.strategies.momentum.Momentum",
            method="start",
            parameters={},
            note=None,
            lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
            role=ProcessRoleEnum.STRATEGY,
            tags=(),
            parameters_schema=None,
        )
        non_strategy_cfg = ProcessConfigModel(
            name="trader_coordinator",
            enabled=True,
            mode="thread",
            class_path="snapper.coordinators.trader.Trader",
            method="start",
            parameters={},
            note=None,
            lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
            role=ProcessRoleEnum.CORE,
            tags=(),
            parameters_schema=None,
        )
        launcher.get_process_configs = AsyncMock(return_value=[strategy_cfg, non_strategy_cfg])
        await launcher._emit_strategy_list_snapshot()
        assert len(publisher.sent) == 1
        topic, payload = publisher.sent[0]
        assert topic.startswith("strategies.events.list.")
        valid, err = validate_topic(topic)
        assert valid, f"emitted topic {topic!r} must satisfy registry validator: {err}"
        assert isinstance(payload, StrategyListEventData)
        assert payload.strategy_classes == [
            "snapper.strategies.momentum.Momentum",
        ]

    @pytest.mark.asyncio
    async def test_run_event_emit_publishes_lifecycle_frame(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Run event carries the per-run lifecycle transition."""
        publisher = _RecordingPublisher()
        launcher.set_msg_publisher(publisher)
        started_at = datetime(2026, 5, 14, 12, tzinfo=UTC)
        completed_at = datetime(2026, 5, 14, 12, 5, tzinfo=UTC)
        await launcher._emit_run_event(
            process_name="trader_coordinator",
            run_id="run-42",
            status=ProcessRunStatusEnum.SUCCEEDED,
            started_at=started_at,
            completed_at=completed_at,
            error=None,
        )
        assert len(publisher.sent) == 1
        topic, payload = publisher.sent[0]
        assert topic == "processes.events.runs.trader_coordinator"
        assert isinstance(payload, ProcessRunEventData)
        assert payload.process_name == "trader_coordinator"
        assert payload.run_id == "run-42"
        assert payload.status == "succeeded"
        assert payload.started_at == started_at
        assert payload.completed_at == completed_at

    @pytest.mark.asyncio
    async def test_run_event_emit_logs_error_message(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Run event includes the error string when terminal status is failure.

        Exercises the debug-log branch in ``_emit_run_event`` so coverage
        sees both the error-present and error-absent paths.
        """
        publisher = _RecordingPublisher()
        launcher.set_msg_publisher(publisher)
        started_at = datetime(2026, 5, 14, 12, tzinfo=UTC)
        completed_at = datetime(2026, 5, 14, 12, 5, tzinfo=UTC)
        await launcher._emit_run_event(
            process_name="trader_coordinator",
            run_id="run-42",
            status=ProcessRunStatusEnum.FAILED,
            started_at=started_at,
            completed_at=completed_at,
            error="boom",
        )
        assert len(publisher.sent) == 1


class TestEmitFailurePaths:
    """`send` failures on configured/strategy/run emit helpers must not propagate.

    Mirrors the :class:`ScopeGrantService` resilience contract for the
    three non-summary emit helpers.
    """

    @pytest.mark.asyncio
    async def test_configured_emit_send_failure_is_logged_not_raised(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Broker hiccup on configured emit logs + suppresses."""
        publisher = _RaisingPublisher()
        launcher.set_msg_publisher(publisher)
        launcher.get_process_configs = AsyncMock(return_value=[])
        await launcher._emit_configured_snapshot()
        assert len(publisher.sent) == 1

    @pytest.mark.asyncio
    async def test_strategy_list_emit_send_failure_is_logged_not_raised(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Broker hiccup on strategy-list emit logs + suppresses."""
        publisher = _RaisingPublisher()
        launcher.set_msg_publisher(publisher)
        launcher.get_process_configs = AsyncMock(return_value=[])
        await launcher._emit_strategy_list_snapshot()
        assert len(publisher.sent) == 1

    @pytest.mark.asyncio
    async def test_run_event_emit_send_failure_is_logged_not_raised(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Broker hiccup on run-event emit logs + suppresses."""
        publisher = _RaisingPublisher()
        launcher.set_msg_publisher(publisher)
        await launcher._emit_run_event(
            process_name="proc",
            run_id="run-1",
            status=ProcessRunStatusEnum.SUCCEEDED,
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
            error=None,
        )
        assert len(publisher.sent) == 1


class TestSummarySnapshotInstanceConfigs:
    """`build_process_summary_items` joins persisted + instance configs cleanly."""

    @pytest.mark.asyncio
    async def test_instance_config_only_appears_in_snapshot(
        self, launcher: ProcessLauncherService
    ) -> None:
        """An instance_config absent from persisted configs is appended.

        Given: ``instance_configs`` carries a per-wallet executor but
            ``get_process_configs`` is empty,
        When: the snapshot is built,
        Then: the instance entry is present.
        """
        publisher = _RecordingPublisher()
        launcher.set_msg_publisher(publisher)
        launcher.get_process_configs = AsyncMock(return_value=[])
        instance_cfg = ProcessConfigModel(
            name="executor_kraken_w019dbb34f439",
            enabled=True,
            mode="thread",
            class_path="x.Y",
            method="start",
            parameters={"wallet_public_id": "wal-1"},
            note=None,
            lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
            role=ProcessRoleEnum.CORE,
            tags=(),
            parameters_schema=None,
        )
        launcher.instance_configs["executor_kraken_w019dbb34f439"] = instance_cfg

        await launcher._emit_summary_snapshot()

        assert len(publisher.sent) == 1
        _, payload = publisher.sent[0]
        assert payload.coordinator == "coord-0"
        names = {item.name: item for item in payload.processes}
        assert "executor_kraken_w019dbb34f439" in names
        assert names["executor_kraken_w019dbb34f439"].running is False

    @pytest.mark.asyncio
    async def test_instance_config_duplicate_is_skipped(
        self, launcher: ProcessLauncherService
    ) -> None:
        """An instance_config name also in persisted configs is NOT duplicated."""
        publisher = _RecordingPublisher()
        launcher.set_msg_publisher(publisher)
        instance_cfg = ProcessConfigModel(
            name="executor_kraken_w019dbb34f439",
            enabled=True,
            mode="thread",
            class_path="x.Y",
            method="start",
            parameters={"wallet_public_id": "wal-1"},
            note=None,
            lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
            role=ProcessRoleEnum.CORE,
            tags=(),
            parameters_schema=None,
        )
        launcher.instance_configs["executor_kraken_w019dbb34f439"] = instance_cfg
        launcher.get_process_configs = AsyncMock(return_value=[instance_cfg])
        await launcher._emit_summary_snapshot()
        _, payload = publisher.sent[0]
        names = [item.name for item in payload.processes]
        assert names.count("executor_kraken_w019dbb34f439") == 1

    @pytest.mark.asyncio
    async def test_snapshot_carries_sampled_metrics_and_none_fallback(
        self, launcher: ProcessLauncherService
    ) -> None:
        """`build_process_summary_items` surfaces sampled RSS/CPU per process.

        Given: a persisted config WITH sampled metrics and an
            ``instance_configs`` entry WITHOUT sampled metrics,
        When: the snapshot is built,
        Then: the persisted row carries its (rss_bytes, cpu_percent) and the
            unsampled instance row falls back to (None, None).
        """
        publisher = _RecordingPublisher()
        launcher.set_msg_publisher(publisher)
        persisted = ProcessConfigModel(
            name="kraken_feed_publisher",
            enabled=True,
            mode="process",
            class_path="x.Y",
            method="start",
            parameters={},
            note=None,
            lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
            role=ProcessRoleEnum.CORE,
            tags=(),
            parameters_schema=None,
        )
        launcher.get_process_configs = AsyncMock(return_value=[persisted])
        instance_cfg = ProcessConfigModel(
            name="executor_kraken_w019dbb34f439",
            enabled=True,
            mode="thread",
            class_path="x.Y",
            method="start",
            parameters={"wallet_public_id": "wal-1"},
            note=None,
            lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
            role=ProcessRoleEnum.CORE,
            tags=(),
            parameters_schema=None,
        )
        launcher.instance_configs["executor_kraken_w019dbb34f439"] = instance_cfg
        launcher._process_metrics["kraken_feed_publisher"] = (98_304, 12.5)
        items = await launcher.build_process_summary_items()
        by_name = {item.name: item for item in items}
        assert by_name["kraken_feed_publisher"].rss_bytes == 98_304
        assert by_name["kraken_feed_publisher"].cpu_percent == pytest.approx(12.5)
        assert by_name["executor_kraken_w019dbb34f439"].rss_bytes is None
        assert by_name["executor_kraken_w019dbb34f439"].cpu_percent is None


class TestCompletionEmitBranches:
    """Strategy-role completion fires both summary + strategy-list emits."""

    @pytest.mark.asyncio
    async def test_handle_task_completion_strategy_emits_strategy_list(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Async strategy task completion fires the strategy-list event.

        Given: A strategy-role task that finished normally,
        When: ``_handle_task_completion`` runs,
        Then: Publisher captures BOTH a summary + strategy-list frame.
        """
        publisher = _RecordingPublisher()
        launcher.set_msg_publisher(publisher)
        launcher.get_process_configs = AsyncMock(return_value=[])
        launcher._finalize_process_run = AsyncMock()
        launcher.process_roles["momentum"] = ProcessRoleEnum.STRATEGY
        launcher.process_lifecycles["momentum"] = ProcessLifecycleEnum.LONG_RUNNING

        async def _noop() -> None:
            return None

        task: asyncio.Task[None] = asyncio.create_task(_noop())
        await task
        launcher.process_tasks["momentum"] = task

        await launcher._handle_task_completion("momentum", task)

        topics = [topic for topic, _ in publisher.sent]
        assert any(t.startswith("processes.events.summary.") for t in topics)
        assert any(t.startswith("strategies.events.list.") for t in topics)

    @pytest.mark.asyncio
    async def test_handle_process_completion_strategy_emits_strategy_list_only(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Native strategy subprocess completion fires only the strategy event.

        Given: A strategy subprocess that exited cleanly,
        When: ``_handle_process_completion`` runs,
        Then: Publisher captures the strategy-list frame but NOT a summary
            frame — the per-tick summary emit now belongs to the monitor
            loop, so completion no longer double-emits the summary.
        """
        publisher = _RecordingPublisher()
        launcher.set_msg_publisher(publisher)
        launcher.get_process_configs = AsyncMock(return_value=[])
        launcher._finalize_process_run = AsyncMock()
        launcher.spawner = MagicMock()
        launcher.spawner.cleanup = MagicMock()
        launcher.process_roles["momentum"] = ProcessRoleEnum.STRATEGY
        launcher.process_lifecycles["momentum"] = ProcessLifecycleEnum.LONG_RUNNING
        launcher.expected_terminations.add("momentum")
        proc_info = MagicMock()
        proc_info.process = MagicMock()
        proc_info.process.returncode = 0

        await launcher._handle_process_completion("momentum", proc_info)

        topics = [topic for topic, _ in publisher.sent]
        assert not any(t.startswith("processes.events.summary.") for t in topics)
        assert any(t.startswith("strategies.events.list.") for t in topics)


class TestEmitSitesIntegration:
    """Emit sites fire from the lifecycle methods that mutate launcher state."""

    @pytest.mark.asyncio
    async def test_start_process_emits_summary_and_run_event(
        self, launcher: ProcessLauncherService
    ) -> None:
        """`start_process` emits one ``run`` (RUNNING) + one ``summary``.

        Given: A wired publisher + a STRATEGY-role config,
        When: ``start_process`` succeeds,
        Then: Publisher captures the RUNNING run frame, the summary
            snapshot, AND the strategy-list snapshot (because role is
            STRATEGY).
        """

        def _import_dummy_process(
            _path: str, name: str | None = None, template: str | None = None
        ) -> type[DummyProcess]:
            del name
            return DummyProcess

        def _no_op_register(_name: str, _task: asyncio.Task[Any]) -> None:
            return None

        publisher = _RecordingPublisher()
        launcher.set_msg_publisher(publisher)
        launcher._create_process_run_record = AsyncMock(return_value="run-1")
        launcher.import_class = _import_dummy_process
        launcher._register_task_completion = _no_op_register
        launcher.get_process_configs = AsyncMock(return_value=[])
        config = ProcessConfigModel(
            name="momentum",
            enabled=True,
            mode="thread",
            class_path="snapper.strategies.momentum.Momentum",
            method="start",
            parameters={},
            note=None,
            lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
            role=ProcessRoleEnum.STRATEGY,
            tags=(),
            parameters_schema=None,
        )

        await launcher.start_process(config)

        topics = [topic for topic, _ in publisher.sent]
        assert "processes.events.runs.momentum" in topics
        assert any(t.startswith("processes.events.summary.") for t in topics)
        assert any(t.startswith("strategies.events.list.") for t in topics)
        assert launcher.active_run_started_at["momentum"] is not None

    @pytest.mark.asyncio
    async def test_start_process_failure_emits_failed_run_event(
        self, launcher: ProcessLauncherService
    ) -> None:
        """`_handle_start_failure` emits a FAILED run-event frame.

        Given: A wired publisher + a config whose mode is invalid,
        When: ``start_process`` is awaited (which raises ValueError),
        Then: Publisher captured both a STARTED and a FAILED run-event
            frame.
        """
        publisher = _RecordingPublisher()
        launcher.set_msg_publisher(publisher)
        launcher._create_process_run_record = AsyncMock(return_value="run-1")
        launcher._update_process_run_record = AsyncMock()
        launcher.get_process_configs = AsyncMock(return_value=[])
        config = ProcessConfigModel(
            name="oops",
            enabled=True,
            mode="worker",
            class_path="x.Y",
            method="start",
            parameters={},
            note=None,
            lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
            role=ProcessRoleEnum.CORE,
            tags=(),
            parameters_schema=None,
        )
        with pytest.raises(ValueError):
            await launcher.start_process(config)

        run_frames = [
            payload for topic, payload in publisher.sent if topic == "processes.events.runs.oops"
        ]
        assert len(run_frames) == 2
        statuses = {frame.status for frame in run_frames}
        assert statuses == {"running", "failed"}
        assert "oops" not in launcher.active_run_started_at

    @pytest.mark.asyncio
    async def test_stop_process_by_name_emits_summary(
        self, launcher: ProcessLauncherService
    ) -> None:
        """`stop_process_by_name` emits one summary + strategy frame on success."""
        publisher = _RecordingPublisher()
        launcher.set_msg_publisher(publisher)
        launcher.get_process_configs = AsyncMock(return_value=[])
        launcher._finalize_process_run = AsyncMock()
        instance = MagicMock()
        instance.stop = AsyncMock()
        launcher.started_processes["momentum"] = instance
        launcher.process_roles["momentum"] = ProcessRoleEnum.STRATEGY
        launcher.process_lifecycles["momentum"] = ProcessLifecycleEnum.LONG_RUNNING

        result = await launcher.stop_process_by_name("momentum")

        assert result.status.value == "success"
        topics = [topic for topic, _ in publisher.sent]
        assert any(t.startswith("processes.events.summary.") for t in topics)
        assert any(t.startswith("strategies.events.list.") for t in topics)

    @pytest.mark.asyncio
    async def test_stop_process_by_name_not_running_skips_emit(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Stop for an unknown process skips emit (NOT_RUNNING branch)."""
        publisher = _RecordingPublisher()
        launcher.set_msg_publisher(publisher)
        result = await launcher.stop_process_by_name("nonexistent")
        assert result.status.value == "not_running"
        assert publisher.sent == []

    @pytest.mark.asyncio
    async def test_create_process_config_emits_configured_summary_and_strategy(
        self, launcher: ProcessLauncherService
    ) -> None:
        """`create_process_config` emits configured + summary + strategy-list events.

        The summary emit is required: without it, REST
        `useProcessSummary` totals stay stale after a create until the
        next reconnect because the hook now uses `staleTime: Infinity`.
        """
        publisher = _RecordingPublisher()
        launcher.set_msg_publisher(publisher)
        launcher._registry_syncer.create_process_config = AsyncMock()
        launcher.get_process_configs = AsyncMock(return_value=[])

        await launcher.create_process_config(
            name="momentum",
            class_path="x.Y",
            method="start",
            enabled=True,
            mode="thread",
            parameters={},
            lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
            role=ProcessRoleEnum.STRATEGY,
            tags=(),
        )
        topics = [topic for topic, _ in publisher.sent]
        assert any(t.startswith("processes.events.configured.") for t in topics)
        assert any(t.startswith("processes.events.summary.") for t in topics)
        assert any(t.startswith("strategies.events.list.") for t in topics)

    @pytest.mark.asyncio
    async def test_create_process_config_non_strategy_skips_strategy_emit(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Non-strategy create still emits configured + summary but not strategy-list."""
        publisher = _RecordingPublisher()
        launcher.set_msg_publisher(publisher)
        launcher._registry_syncer.create_process_config = AsyncMock()
        launcher.get_process_configs = AsyncMock(return_value=[])

        await launcher.create_process_config(
            name="trader_coordinator",
            class_path="x.Y",
            method="start",
            enabled=True,
            mode="thread",
            parameters={},
            lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
            role=ProcessRoleEnum.CORE,
            tags=(),
        )
        topics = [topic for topic, _ in publisher.sent]
        assert any(t.startswith("processes.events.configured.") for t in topics)
        assert any(t.startswith("processes.events.summary.") for t in topics)
        assert not any(t.startswith("strategies.events.list.") for t in topics)

    @pytest.mark.asyncio
    async def test_update_process_config_emits_configured_summary_and_strategy(
        self, launcher: ProcessLauncherService
    ) -> None:
        """`update_process_config(is_strategy=True)` emits configured + summary + strategy-list.

        The desired-state PATCH must refresh the same snapshots as create so
        the UI reflects an enable/disable/restart without polling.
        """
        publisher = _RecordingPublisher()
        launcher.set_msg_publisher(publisher)
        launcher._registry_syncer.update_process_config = AsyncMock()
        launcher.get_process_configs = AsyncMock(return_value=[])

        await launcher.update_process_config(
            name="momentum", enabled=False, updated_by="op", is_strategy=True
        )
        topics = [topic for topic, _ in publisher.sent]
        assert any(t.startswith("processes.events.configured.") for t in topics)
        assert any(t.startswith("processes.events.summary.") for t in topics)
        assert any(t.startswith("strategies.events.list.") for t in topics)

    @pytest.mark.asyncio
    async def test_update_process_config_non_strategy_skips_strategy_emit(
        self, launcher: ProcessLauncherService
    ) -> None:
        """A non-strategy update emits configured + summary but not the strategy list."""
        publisher = _RecordingPublisher()
        launcher.set_msg_publisher(publisher)
        launcher._registry_syncer.update_process_config = AsyncMock()
        launcher.get_process_configs = AsyncMock(return_value=[])

        await launcher.update_process_config(
            name="kraken_feed_publisher", restart_nonce="n1", updated_by="op"
        )
        topics = [topic for topic, _ in publisher.sent]
        assert any(t.startswith("processes.events.configured.") for t in topics)
        assert any(t.startswith("processes.events.summary.") for t in topics)
        assert not any(t.startswith("strategies.events.list.") for t in topics)

    @pytest.mark.asyncio
    async def test_stop_all_processes_clears_active_runs(
        self, launcher: ProcessLauncherService, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`stop_all_processes` clears both active_runs + active_run_started_at.

        Without the explicit `active_runs.clear()`, a task that never
        completes via its done-callback leaves a stale
        `(name, run_public_id)` entry and the next emit would re-attach
        the dead run_public_id to a new launch.
        """
        publisher = _RecordingPublisher()
        launcher.set_msg_publisher(publisher)
        launcher.get_process_configs = AsyncMock(return_value=[])
        launcher.active_runs["ghost"] = "run-orphan"
        launcher.active_run_started_at["ghost"] = datetime(2026, 5, 14, tzinfo=UTC)
        monkeypatch.setattr(
            "snapper.application.process_manager.launcher.get_registered_processes", lambda: {}
        )
        await launcher.stop_all_processes()
        assert launcher.active_runs == {}
        assert launcher.active_run_started_at == {}


class TestReconcileDesiredState:
    """Tests for the desired-state reconcile loop (control plane P0.3)."""

    @staticmethod
    def _cfg(
        name: str,
        *,
        enabled: bool,
        role: ProcessRoleEnum = ProcessRoleEnum.CORE,
        tags: tuple[str, ...] = (),
        restart_nonce: str | None = None,
    ) -> ProcessConfigModel:
        """Build a minimal owned config for reconcile tests.

        Args:
            name: Process name.
            enabled: Desired-state enabled flag.
            role: Process role (STRATEGY drives the scope path).
            tags: Tags (market-data+publisher drives the PROCESS-mode path).
            restart_nonce: Persisted operator restart nonce.

        Returns:
            A populated ProcessConfigModel.
        """
        return ProcessConfigModel(
            name=name,
            enabled=enabled,
            mode="thread",
            class_path="x.Y",
            method="start",
            parameters={},
            role=role,
            tags=tags,
            restart_nonce=restart_nonce,
        )

    @staticmethod
    def _instrument(launcher: ProcessLauncherService) -> None:
        """Replace the spawn/stop/scope primitives so decisions are observable.

        The stop mock also removes the name from ``started_processes`` (as the
        real stop would) so a restart's subsequent start is not short-circuited
        by the in-lock already-running re-check.

        Args:
            launcher: The launcher under test.
        """

        async def _stop(name: str) -> None:
            launcher.started_processes.pop(name, None)

        launcher.start_process = AsyncMock()
        launcher.stop_process_by_name = AsyncMock(side_effect=_stop)
        launcher._resolve_autostart_strategy_scope = AsyncMock(side_effect=lambda config: config)
        launcher._cancel_pending_restart = AsyncMock()
        launcher._cancel_restart_tasks_locked = AsyncMock()
        launcher._start_native_process_monitoring = MagicMock()

    @staticmethod
    def _pending_task() -> MagicMock:
        """Return a mock delayed-restart task that is not done.

        Returns:
            A MagicMock whose ``done()`` returns False.
        """
        task = MagicMock()
        task.done.return_value = False
        return task

    def test_is_parked_reflects_parked_set(self, launcher: ProcessLauncherService) -> None:
        """is_parked mirrors the parked set."""
        launcher._parked_processes.add("p")
        assert launcher.is_parked("p") is True
        assert launcher.is_parked("q") is False

    def test_has_pending_restart(self, launcher: ProcessLauncherService) -> None:
        """_has_pending_restart is True only for a live restart task."""
        assert launcher._has_pending_restart("p") is False
        launcher._restart_tasks["p"] = self._pending_task()
        assert launcher._has_pending_restart("p") is True
        done = MagicMock()
        done.done.return_value = True
        launcher._restart_tasks["q"] = done
        assert launcher._has_pending_restart("q") is False

    @pytest.mark.asyncio
    async def test_prepare_publisher_forces_process_mode(
        self, launcher: ProcessLauncherService
    ) -> None:
        """A market-data publisher is forced to PROCESS mode before start."""
        self._instrument(launcher)
        cfg = self._cfg("kraken_feed_publisher", enabled=True, tags=("market-data", "publisher"))
        prepared = await launcher._prepare_owned_config_for_start(cfg)
        assert prepared.mode == ProcessModeEnum.PROCESS

    @pytest.mark.asyncio
    async def test_prepare_strategy_scope_resolved_mode_unchanged(
        self, launcher: ProcessLauncherService
    ) -> None:
        """A strategy goes through scope resolution and keeps its (thread) mode."""
        self._instrument(launcher)
        cfg = self._cfg("strategy_x", enabled=True, role=ProcessRoleEnum.STRATEGY)
        prepared = await launcher._prepare_owned_config_for_start(cfg)
        launcher._resolve_autostart_strategy_scope.assert_awaited_once()
        assert prepared.mode == "thread"

    @pytest.mark.asyncio
    async def test_start_owned_spawns_when_stopped(self, launcher: ProcessLauncherService) -> None:
        """_reconcile_start_owned spawns, re-arms the monitor, re-cancels stale restarts."""
        self._instrument(launcher)
        cfg = self._cfg("p", enabled=True, restart_nonce="n1")
        started = await launcher._reconcile_start_owned(cfg)
        assert started is True
        launcher.start_process.assert_awaited_once()
        launcher._start_native_process_monitoring.assert_called_once()
        launcher._cancel_restart_tasks_locked.assert_awaited_once_with("p")
        assert launcher._last_applied_restart_nonce["p"] == "n1"

    @pytest.mark.asyncio
    async def test_start_owned_noop_when_already_running(
        self, launcher: ProcessLauncherService
    ) -> None:
        """_reconcile_start_owned returns False and no-ops if already running."""
        self._instrument(launcher)
        launcher.started_processes["p"] = MagicMock()
        started = await launcher._reconcile_start_owned(self._cfg("p", enabled=True))
        assert started is False
        launcher.start_process.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_restart_running_stops_then_starts_and_records_nonce(
        self, launcher: ProcessLauncherService
    ) -> None:
        """A running restart resets budget, stops, starts, and records the nonce."""
        self._instrument(launcher)
        launcher.started_processes["p"] = MagicMock()
        launcher._restart_attempts["p"] = 3
        launcher._total_failed_restarts["p"] = 9
        await launcher._reconcile_restart(self._cfg("p", enabled=True, restart_nonce="n2"))
        launcher._cancel_pending_restart.assert_awaited_once_with("p")
        launcher.stop_process_by_name.assert_awaited_once_with("p")
        launcher.start_process.assert_awaited_once()
        assert "p" not in launcher._restart_attempts
        assert "p" not in launcher._total_failed_restarts
        assert launcher._last_applied_restart_nonce["p"] == "n2"

    @pytest.mark.asyncio
    async def test_restart_not_recorded_when_stop_leaves_process_running(
        self, launcher: ProcessLauncherService
    ) -> None:
        """A failed stop (process still running) leaves the nonce unapplied for retry.

        Regression (review): stop_process_by_name returns an ERROR result
        rather than raising, so if the stop fails the process stays running,
        the subsequent start no-ops, and the nonce must NOT be marked applied
        — otherwise the operator restart would be permanently swallowed.
        """
        self._instrument(launcher)
        launcher.started_processes["p"] = MagicMock()
        launcher.stop_process_by_name = AsyncMock()
        await launcher._reconcile_restart(self._cfg("p", enabled=True, restart_nonce="n2"))
        launcher.start_process.assert_not_awaited()
        assert "p" not in launcher._last_applied_restart_nonce

    @pytest.mark.asyncio
    async def test_restart_parked_does_not_stop_before_start(
        self, launcher: ProcessLauncherService
    ) -> None:
        """A parked (not-running) restart skips the stop so the marker survives a failed start."""
        self._instrument(launcher)
        launcher._parked_processes.add("p")
        await launcher._reconcile_restart(self._cfg("p", enabled=True, restart_nonce="n2"))
        launcher.stop_process_by_name.assert_not_awaited()
        launcher.start_process.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_enabled_running_unchanged_nonce_noop(
        self, launcher: ProcessLauncherService
    ) -> None:
        """An enabled, running process with an unchanged nonce is a no-op."""
        self._instrument(launcher)
        launcher.started_processes["p"] = MagicMock()
        launcher._last_applied_restart_nonce["p"] = "n1"
        await launcher._reconcile_one(self._cfg("p", enabled=True, restart_nonce="n1"))
        launcher.start_process.assert_not_awaited()
        launcher.stop_process_by_name.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_enabled_stopped_parked_does_not_auto_unpark(
        self, launcher: ProcessLauncherService
    ) -> None:
        """An enabled but parked process is left parked (watchdog gave up)."""
        self._instrument(launcher)
        launcher._parked_processes.add("p")
        await launcher._reconcile_one(self._cfg("p", enabled=True))
        launcher.start_process.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_enabled_stopped_pending_restart_noop(
        self, launcher: ProcessLauncherService
    ) -> None:
        """An enabled process mid-backoff is left to the watchdog."""
        self._instrument(launcher)
        launcher._restart_tasks["p"] = self._pending_task()
        await launcher._reconcile_one(self._cfg("p", enabled=True))
        launcher.start_process.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_enabled_stopped_clean_starts(self, launcher: ProcessLauncherService) -> None:
        """An enabled, stopped, unparked, no-pending process is started."""
        self._instrument(launcher)
        await launcher._reconcile_one(self._cfg("p", enabled=True))
        launcher.start_process.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_enabled_nonce_advanced_bounces(self, launcher: ProcessLauncherService) -> None:
        """An advanced restart nonce bounces the process."""
        self._instrument(launcher)
        launcher.started_processes["p"] = MagicMock()
        launcher._last_applied_restart_nonce["p"] = "n1"
        await launcher._reconcile_one(self._cfg("p", enabled=True, restart_nonce="n2"))
        launcher.stop_process_by_name.assert_awaited_once_with("p")
        launcher.start_process.assert_awaited_once()
        assert launcher._last_applied_restart_nonce["p"] == "n2"

    @pytest.mark.asyncio
    async def test_enabled_seeded_nonce_unchanged_noop(
        self, launcher: ProcessLauncherService
    ) -> None:
        """A running process whose nonce equals the boot-seeded baseline is a no-op."""
        self._instrument(launcher)
        launcher.started_processes["p"] = MagicMock()
        launcher._last_applied_restart_nonce["p"] = "n1"
        await launcher._reconcile_one(self._cfg("p", enabled=True, restart_nonce="n1"))
        launcher.stop_process_by_name.assert_not_awaited()
        launcher.start_process.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_parked_first_operator_nonce_recovers(
        self, launcher: ProcessLauncherService
    ) -> None:
        """A parked process with an unapplied restart nonce is recovered (bounced).

        Regression (review): the first operator restart of a parked process
        must NOT be swallowed as an adopted baseline — it is the recovery
        signal. With the baseline seeded at boot, an unrecorded nonce here is a
        genuine restart request.
        """
        self._instrument(launcher)
        launcher._parked_processes.add("p")
        await launcher._reconcile_one(self._cfg("p", enabled=True, restart_nonce="n1"))
        launcher.start_process.assert_awaited_once()
        assert launcher._last_applied_restart_nonce["p"] == "n1"

    @pytest.mark.asyncio
    async def test_disabled_running_stops(self, launcher: ProcessLauncherService) -> None:
        """A disabled, running process is stopped."""
        self._instrument(launcher)
        launcher.started_processes["p"] = MagicMock()
        await launcher._reconcile_one(self._cfg("p", enabled=False))
        launcher.stop_process_by_name.assert_awaited_once_with("p")

    @pytest.mark.asyncio
    async def test_disabled_parked_stops(self, launcher: ProcessLauncherService) -> None:
        """A disabled, parked process is stopped (clears the parked marker)."""
        self._instrument(launcher)
        launcher._parked_processes.add("p")
        await launcher._reconcile_one(self._cfg("p", enabled=False))
        launcher.stop_process_by_name.assert_awaited_once_with("p")

    @pytest.mark.asyncio
    async def test_disabled_pending_restart_stops(self, launcher: ProcessLauncherService) -> None:
        """A disabled process mid-backoff is stopped (cancels the pending restart)."""
        self._instrument(launcher)
        launcher._restart_tasks["p"] = self._pending_task()
        await launcher._reconcile_one(self._cfg("p", enabled=False))
        launcher.stop_process_by_name.assert_awaited_once_with("p")

    @pytest.mark.asyncio
    async def test_disabled_desired_running_stops(self, launcher: ProcessLauncherService) -> None:
        """A disabled process still marked desired-RUNNING is stopped."""
        self._instrument(launcher)
        launcher._desired_state["p"] = _DesiredState.RUNNING
        await launcher._reconcile_one(self._cfg("p", enabled=False))
        launcher.stop_process_by_name.assert_awaited_once_with("p")

    @pytest.mark.asyncio
    async def test_disabled_stopped_clean_noop(self, launcher: ProcessLauncherService) -> None:
        """A disabled, stopped, clean process is a no-op."""
        self._instrument(launcher)
        await launcher._reconcile_one(self._cfg("p", enabled=False))
        launcher.stop_process_by_name.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_reconcile_skips_non_owned_and_is_fail_soft(
        self, launcher: ProcessLauncherService
    ) -> None:
        """reconcile_desired_state skips non-owned configs and survives a per-config failure."""
        self._instrument(launcher)
        owned = self._cfg("owned", enabled=True)
        not_owned = self._cfg("not_owned", enabled=True)
        boom = self._cfg("boom", enabled=True)
        launcher.get_process_configs = AsyncMock(return_value=[owned, not_owned, boom])
        launcher.autostart_includes = MagicMock(
            side_effect=lambda config: config.name != "not_owned"
        )
        original_start = launcher.start_process

        async def _start(config: ProcessConfigModel) -> None:
            if config.name == "boom":
                raise RuntimeError("spawn failed")
            await original_start(config)

        launcher.start_process = AsyncMock(side_effect=_start)
        await launcher.reconcile_desired_state()
        started = {call.args[0].name for call in launcher.start_process.await_args_list}
        assert "owned" in started
        assert "not_owned" not in started


class TestReconcileHardening:
    """Failure damping, pass timeout, and §8 concurrency guarantees."""

    @staticmethod
    def _cfg(
        name: str,
        *,
        enabled: bool = True,
        role: ProcessRoleEnum = ProcessRoleEnum.CORE,
        restart_nonce: str | None = None,
    ) -> ProcessConfigModel:
        """Build a minimal owned config (delegates to the P0.3 helper).

        Args:
            name: Process name.
            enabled: Desired-state enabled flag.
            role: Process role.
            restart_nonce: Persisted operator restart nonce.

        Returns:
            A populated ProcessConfigModel.
        """
        return TestReconcileDesiredState._cfg(
            name, enabled=enabled, role=role, restart_nonce=restart_nonce
        )

    @staticmethod
    def _instrument(launcher: ProcessLauncherService) -> None:
        """Replace spawn/stop/scope primitives (delegates to the P0.3 helper).

        Args:
            launcher: The launcher under test.
        """
        TestReconcileDesiredState._instrument(launcher)

    @pytest.mark.asyncio
    async def test_parks_after_consecutive_start_failures(
        self, launcher: ProcessLauncherService
    ) -> None:
        """A permanently failing enabled config parks after the budget.

        Without damping every ~10s tick would fail-start forever
        (run-record churn); after the budget the parked no-op row takes
        over and no further spawn is attempted.
        """
        self._instrument(launcher)
        launcher.start_process = AsyncMock(side_effect=RuntimeError("boom"))
        cfg = self._cfg("p")
        budget = launcher_module._RECONCILE_START_FAILURE_BUDGET
        for _ in range(budget):
            with pytest.raises(RuntimeError):
                await launcher._reconcile_one(cfg)
        assert launcher.is_parked("p") is True
        assert "p" not in launcher._desired_state
        assert "p" not in launcher._restart_configs
        await launcher._reconcile_one(cfg)
        assert launcher.start_process.await_count == budget

    @pytest.mark.asyncio
    async def test_wedged_start_counts_toward_budget_and_parks(
        self, launcher: ProcessLauncherService, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A start that HANGS (timeout-cancelled) burns budget like a raise.

        wait_for injects CancelledError, which _reconcile_one's except
        Exception never sees — without counting in the TimeoutError
        branch a perpetually hanging start would be cancelled and
        retried every tick forever, orphaning executor work each time.
        """
        self._instrument(launcher)
        monkeypatch.setattr(launcher_module, "_RECONCILE_ONE_TIMEOUT_S", 0.02)

        async def _hang(config: ProcessConfigModel) -> None:
            await asyncio.sleep(5)

        launcher.start_process = AsyncMock(side_effect=_hang)
        cfg = self._cfg("p")
        launcher.get_process_configs = AsyncMock(return_value=[cfg])
        launcher.autostart_includes = MagicMock(return_value=True)
        budget = launcher_module._RECONCILE_START_FAILURE_BUDGET
        for _ in range(budget):
            await launcher.reconcile_desired_state()
        assert launcher.is_parked("p") is True
        assert "p" not in launcher._desired_state
        await launcher.reconcile_desired_state()
        assert launcher.start_process.await_count == budget

    @pytest.mark.asyncio
    async def test_failure_counter_resets_on_success(
        self, launcher: ProcessLauncherService
    ) -> None:
        """A successful start wipes the consecutive-failure counter."""
        self._instrument(launcher)
        launcher.start_process = AsyncMock(side_effect=RuntimeError("boom"))
        cfg = self._cfg("p")
        for _ in range(2):
            with pytest.raises(RuntimeError):
                await launcher._reconcile_one(cfg)
        assert launcher._reconcile_start_failures["p"] == 2
        launcher.start_process = AsyncMock()
        await launcher._reconcile_one(cfg)
        assert "p" not in launcher._reconcile_start_failures

    @pytest.mark.asyncio
    async def test_failed_restart_stays_parked_and_abandons_nonce_after_budget(
        self, launcher: ProcessLauncherService
    ) -> None:
        """A failing operator restart of a parked process ends in a bounded park.

        The parked marker survives every failed attempt (§8: parked stays
        parked on a FAILED explicit restart), and after the budget the
        nonce is recorded as applied so the bounce loop ends; a further
        tick is a parked no-op.
        """
        self._instrument(launcher)
        launcher.start_process = AsyncMock(side_effect=RuntimeError("boom"))
        launcher._park("p")
        cfg = self._cfg("p", restart_nonce="n1")
        budget = launcher_module._RECONCILE_START_FAILURE_BUDGET
        for _ in range(budget):
            with pytest.raises(RuntimeError):
                await launcher._reconcile_one(cfg)
            assert launcher.is_parked("p") is True
        assert launcher._last_applied_restart_nonce["p"] == "n1"
        await launcher._reconcile_one(cfg)
        assert launcher.start_process.await_count == budget

    @pytest.mark.asyncio
    async def test_new_nonce_rearms_the_failure_budget(
        self, launcher: ProcessLauncherService
    ) -> None:
        """A changed restart nonce resets the consecutive-failure counter."""
        self._instrument(launcher)
        launcher.start_process = AsyncMock(side_effect=RuntimeError("boom"))
        budget = launcher_module._RECONCILE_START_FAILURE_BUDGET
        cfg_n1 = self._cfg("p", restart_nonce="n1")
        for _ in range(budget - 1):
            with pytest.raises(RuntimeError):
                await launcher._reconcile_one(cfg_n1)
        assert launcher._reconcile_start_failures["p"] == budget - 1
        cfg_n2 = self._cfg("p", restart_nonce="n2")
        with pytest.raises(RuntimeError):
            await launcher._reconcile_one(cfg_n2)
        assert launcher._reconcile_start_failures["p"] == 1
        assert launcher.is_parked("p") is False

    @pytest.mark.asyncio
    async def test_disable_clears_the_failure_counter(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Disabling a failing config wipes its counter for a fresh re-enable."""
        self._instrument(launcher)
        launcher._reconcile_start_failures["p"] = 3
        await launcher._reconcile_one(self._cfg("p", enabled=False))
        assert "p" not in launcher._reconcile_start_failures

    @pytest.mark.asyncio
    async def test_strategy_scope_failure_fails_closed_without_spawn(
        self, launcher: ProcessLauncherService
    ) -> None:
        """§8: a scope failure at reconcile-start never spawns the strategy."""
        self._instrument(launcher)
        launcher._resolve_autostart_strategy_scope = AsyncMock(
            side_effect=RuntimeError("grant revoked")
        )
        cfg = self._cfg("strategy_x", role=ProcessRoleEnum.STRATEGY)
        with pytest.raises(RuntimeError):
            await launcher._reconcile_one(cfg)
        launcher.start_process.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_wedged_convergence_times_out_and_pass_continues(
        self, launcher: ProcessLauncherService, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A hung stop is cancelled by the per-process ceiling; the pass moves on.

        Without the ceiling a wedged ``instance.stop()`` would hold
        ``_reconcile_lock`` forever, stalling every future tick and nudge.
        """
        self._instrument(launcher)
        monkeypatch.setattr(launcher_module, "_RECONCILE_ONE_TIMEOUT_S", 0.05)

        async def _hang(name: str) -> None:
            await asyncio.sleep(5)

        launcher.stop_process_by_name = AsyncMock(side_effect=_hang)
        launcher.started_processes["wedged"] = MagicMock()
        wedged = self._cfg("wedged", enabled=False)
        healthy = self._cfg("healthy")
        launcher.get_process_configs = AsyncMock(return_value=[wedged, healthy])
        launcher.autostart_includes = MagicMock(return_value=True)
        await launcher.reconcile_desired_state()
        started = {call.args[0].name for call in launcher.start_process.await_args_list}
        assert "healthy" in started

    @pytest.mark.asyncio
    async def test_concurrent_passes_spawn_exactly_once(
        self, launcher: ProcessLauncherService
    ) -> None:
        """§8 tick-vs-nudge: two concurrent passes produce a single spawn.

        The passes serialize on ``_reconcile_lock``; the second pass sees
        the name running (the first pass's spawn registered it) and no-ops.
        """
        self._instrument(launcher)
        cfg = self._cfg("p")

        async def _spawn(config: ProcessConfigModel) -> None:
            await asyncio.sleep(0.02)
            launcher.started_processes[config.name] = MagicMock()

        launcher.start_process = AsyncMock(side_effect=_spawn)
        launcher.get_process_configs = AsyncMock(return_value=[cfg])
        launcher.autostart_includes = MagicMock(return_value=True)
        await asyncio.gather(launcher.reconcile_desired_state(), launcher.reconcile_desired_state())
        assert launcher.start_process.await_count == 1

    @pytest.mark.asyncio
    async def test_same_nonce_bounces_exactly_once(self, launcher: ProcessLauncherService) -> None:
        """§8 retry idempotency: re-seeing an applied nonce never re-bounces."""
        self._instrument(launcher)
        cfg = self._cfg("p", restart_nonce="n1")

        async def _spawn(config: ProcessConfigModel) -> None:
            launcher.started_processes[config.name] = MagicMock()

        launcher.start_process = AsyncMock(side_effect=_spawn)
        launcher.started_processes["p"] = MagicMock()
        await launcher._reconcile_one(cfg)
        await launcher._reconcile_one(cfg)
        assert launcher.start_process.await_count == 1
        assert launcher.stop_process_by_name.await_count == 1

    def test_clear_watchdog_state_drops_reconcile_counters(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Terminal cleanup forgets the damping counter and attempted nonce."""
        launcher._reconcile_start_failures["p"] = 4
        launcher._reconcile_attempted_nonce["p"] = "n1"
        launcher._clear_watchdog_state("p")
        assert "p" not in launcher._reconcile_start_failures
        assert "p" not in launcher._reconcile_attempted_nonce

    @pytest.mark.asyncio
    async def test_park_cancels_a_pending_watchdog_restart_task(
        self, launcher: ProcessLauncherService
    ) -> None:
        """Crossing the budget cancels a delayed-restart scheduled in the race window.

        A watchdog _maybe_schedule_restart can slip in between a failed
        (or cancelled) start and the park — the helper runs outside the
        per-name lock's original hold. _clear_watchdog_state pops
        _restart_tasks WITHOUT cancelling, so without the explicit
        locked cancel the sleeper would survive untracked and later
        respawn the parked name.
        """
        cfg = self._cfg("p")
        launcher._reconcile_start_failures["p"] = (
            launcher_module._RECONCILE_START_FAILURE_BUDGET - 1
        )
        sleeper = asyncio.get_running_loop().create_task(asyncio.sleep(30))
        launcher._restart_tasks["p"] = sleeper
        await launcher._register_reconcile_start_failure(cfg)
        await asyncio.sleep(0)
        assert sleeper.cancelled()
        assert "p" not in launcher._restart_tasks
        assert launcher.is_parked("p") is True
