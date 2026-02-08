"""Tests for process management REST API routes."""

from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from fastapi import HTTPException
from fastapi import Request

from snapper.api.schemas.process import ProcessCreateRequest
from snapper.api.schemas.process import ProcessStartRequest
from snapper.application.process_manager.enums import ProcessLifecycleEnum
from snapper.application.process_manager.enums import ProcessRoleEnum
from snapper.application.process_manager.models import ProcessConfigModel
from snapper.application.process_manager.models import ProcessStartResult
from snapper.application.process_manager.models import ProcessStopResult
from snapper.server.process_routes import create_process_configuration
from snapper.server.process_routes import get_process_factory
from snapper.server.process_routes import get_process_schema
from snapper.server.process_routes import list_available_processes
from snapper.server.process_routes import list_configured_processes
from snapper.server.process_routes import list_process_runs
from snapper.server.process_routes import start_process
from snapper.server.process_routes import stop_process


class TestGetProcessFactory:
    """Tests for process factory retrieval from application state."""

    def test_get_process_factory_success(self) -> None:
        """Test factory retrieval from app state.

        Given: A request with process factory in app state,
        When: get_process_factory is called,
        Then: The factory instance is returned.
        """
        mock_factory = MagicMock()
        mock_request = MagicMock(spec=Request)
        mock_request.app.state.process_factory = mock_factory
        result = get_process_factory(mock_request)
        assert result is mock_factory

    def test_get_process_factory_not_initialized(self) -> None:
        """Test AttributeError when factory not initialized.

        Given: A request without process factory in state,
        When: get_process_factory is called,
        Then: AttributeError is raised.
        """
        mock_request = MagicMock(spec=Request)
        del mock_request.app.state.process_factory
        with pytest.raises(AttributeError):
            get_process_factory(mock_request)


class TestListAvailableProcesses:
    """Tests for listing available process types from registry."""

    @pytest.mark.asyncio
    @patch("snapper.server.process_routes.get_registered_processes")
    async def test_list_available_processes(self, mock_get_registry: MagicMock) -> None:
        """Test listing all registered process types.

        Given: Multiple processes registered in the registry,
        When: list_available_processes is called,
        Then: All processes are returned with name, path, and description.
        """
        mock_get_registry.return_value = {
            "zmq_broker": {
                "class_path": "snapper.ipc.zmq_broker.ZmqBrokerThread",
                "method": "run",
                "description": "ZMQ message broker",
                "class_ref": MagicMock(),
            },
            "feed_publisher": {
                "class_path": "snapper.ipc.feed_publisher.MarketDataPublisherService",
                "method": "run",
                "description": "Market data publisher",
                "class_ref": MagicMock(),
            },
        }
        result = await list_available_processes(_user=MagicMock())
        assert result.count == 2
        assert len(result.processes) == 2
        assert result.processes[0].name == "zmq_broker"
        assert result.processes[0].class_path == "snapper.ipc.zmq_broker.ZmqBrokerThread"
        assert result.processes[0].method == "run"
        assert result.processes[0].description == "ZMQ message broker"

    @pytest.mark.asyncio
    @patch("snapper.server.process_routes.get_registered_processes")
    async def test_list_available_processes_empty(self, mock_get_registry: MagicMock) -> None:
        """Test listing returns empty when no processes registered.

        Given: Empty process registry,
        When: list_available_processes is called,
        Then: Empty list with zero count is returned.
        """
        mock_get_registry.return_value = {}
        result = await list_available_processes(_user=MagicMock())
        assert result.count == 0
        assert result.processes == []


class TestListConfiguredProcesses:
    """Tests for listing configured process instances."""

    @pytest.mark.asyncio
    async def test_list_configured_processes(self) -> None:
        """Test listing configured process instances.

        Given: A factory with configured processes,
        When: list_configured_processes is called,
        Then: All configurations are returned with running status.
        """
        mock_factory = MagicMock()
        mock_factory.get_process_configs = AsyncMock(
            return_value=[
                ProcessConfigModel(
                    name="zmq_broker",
                    enabled=True,
                    mode="thread",
                    class_path="snapper.ipc.zmq_broker.ZmqBrokerThread",
                    method="run",
                    args=[],
                    kwargs={"endpoint": "tcp://0.0.0.0:5555"},
                    note="Test broker",
                    lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                )
            ]
        )
        mock_factory.started_processes = {"zmq_broker": MagicMock()}
        mock_factory.active_runs = {}
        result = await list_configured_processes(factory=mock_factory, _user=MagicMock())
        assert result.count == 1
        assert len(result.processes) == 1
        process = result.processes[0]
        assert process.name == "zmq_broker"
        assert process.enabled is True
        assert process.mode == "thread"
        assert process.class_path == "snapper.ipc.zmq_broker.ZmqBrokerThread"
        assert process.method == "run"
        assert process.args == []
        assert process.kwargs == {"endpoint": "tcp://0.0.0.0:5555"}
        assert process.note == "Test broker"
        assert process.lifecycle == "long_running"
        assert process.running is True
        assert process.is_one_shot is False

    @pytest.mark.asyncio
    async def test_list_configured_processes_empty(self) -> None:
        """Test listing returns empty when no processes configured.

        Given: A factory with no configured processes,
        When: list_configured_processes is called,
        Then: Empty list with zero count is returned.
        """
        mock_factory = MagicMock()
        mock_factory.get_process_configs = AsyncMock(return_value=[])
        mock_factory.started_processes = {}
        result = await list_configured_processes(factory=mock_factory, _user=MagicMock())
        assert result.count == 0
        assert result.processes == []


class TestGetProcessSchema:
    """Tests for retrieving process configuration schemas."""

    @pytest.mark.asyncio
    @patch("snapper.server.process_routes.get_registered_processes")
    async def test_get_process_schema_with_defaults(self, mock_get_registry: MagicMock) -> None:
        """Test schema returns default kwargs from class method.

        Given: A process class with get_default_kwargs method,
        When: get_process_schema is called,
        Then: Schema includes default kwargs and configuration.
        """
        mock_class = MagicMock()
        mock_class.get_default_kwargs = MagicMock(return_value={"endpoint": "tcp://0.0.0.0:5555"})
        mock_get_registry.return_value = {
            "zmq_broker": {
                "class_path": "snapper.ipc.zmq_broker.ZmqBrokerThread",
                "method": "run",
                "description": "ZMQ message broker",
                "class_ref": mock_class,
                "enabled": True,
                "mode": "thread",
                "args": [],
            }
        }
        settings = MagicMock()
        result = await get_process_schema(name="zmq_broker", settings=settings, _user=MagicMock())
        assert result.name == "zmq_broker"
        assert result.description == "ZMQ message broker"
        assert result.class_path == "snapper.ipc.zmq_broker.ZmqBrokerThread"
        assert result.method == "run"
        assert result.default_enabled is True
        assert result.default_mode == "thread"
        assert result.default_args == []
        assert result.default_kwargs == {"endpoint": "tcp://0.0.0.0:5555"}

    @pytest.mark.asyncio
    @patch("snapper.server.process_routes.get_registered_processes")
    async def test_get_process_schema_without_defaults(self, mock_get_registry: MagicMock) -> None:
        """Test schema falls back when get_default_kwargs missing.

        Given: A process class without get_default_kwargs method,
        When: get_process_schema is called,
        Then: Schema returns empty kwargs defaults.
        """
        mock_class = MagicMock()
        del mock_class.get_default_kwargs
        mock_get_registry.return_value = {
            "custom_process": {
                "class_path": "custom.Process",
                "method": "run",
                "description": "Custom process",
                "class_ref": mock_class,
            }
        }
        settings = MagicMock()
        result = await get_process_schema(
            name="custom_process", settings=settings, _user=MagicMock()
        )
        assert result.name == "custom_process"
        assert result.default_enabled is False
        assert result.default_mode == "thread"
        assert result.default_args == []
        assert result.default_kwargs == {}

    @pytest.mark.asyncio
    @patch("snapper.server.process_routes.get_registered_processes")
    async def test_get_process_schema_get_default_kwargs_raises(
        self, mock_get_registry: MagicMock
    ) -> None:
        """Test schema handles get_default_kwargs exceptions.

        Given: A process class where get_default_kwargs raises an error,
        When: get_process_schema is called,
        Then: Schema returns empty kwargs without propagating error.
        """
        mock_class = MagicMock()
        mock_class.get_default_kwargs = MagicMock(side_effect=Exception("DB error"))
        mock_get_registry.return_value = {
            "failing_process": {
                "class_path": "failing.Process",
                "method": "run",
                "description": "Process with failing get_default_kwargs",
                "class_ref": mock_class,
                "enabled": True,
                "mode": "process",
                "args": ["arg1"],
            }
        }
        settings = MagicMock()
        result = await get_process_schema(
            name="failing_process", settings=settings, _user=MagicMock()
        )
        assert result.default_kwargs == {}
        assert result.default_enabled is True
        assert result.default_mode == "process"
        assert result.default_args == ["arg1"]

    @pytest.mark.asyncio
    @patch("snapper.server.process_routes.get_registered_processes")
    async def test_get_process_schema_not_found(self, mock_get_registry: MagicMock) -> None:
        """Test 404 is raised for unknown process name.

        Given: An empty process registry,
        When: get_process_schema is called with unknown name,
        Then: HTTPException 404 is raised.
        """
        mock_get_registry.return_value = {}
        settings = MagicMock()
        with pytest.raises(HTTPException) as exc_info:
            await get_process_schema(name="nonexistent", settings=settings, _user=MagicMock())
        assert exc_info.value.status_code == 404
        assert "not found in registry" in exc_info.value.detail


class TestStartProcess:
    """Tests for starting process instances."""

    @pytest.mark.asyncio
    async def test_start_process_with_overrides(self) -> None:
        """Test starting process with parameter overrides.

        Given: A factory and process start request with overrides,
        When: start_process is called,
        Then: Process is started with specified parameters.
        """
        mock_factory = MagicMock()
        mock_factory.start_process_by_name = AsyncMock(
            return_value=ProcessStartResult(status="success", message="started", run_id="run-001")
        )
        request = ProcessStartRequest(
            mode="process",
            args=["arg1"],
            kwargs={"endpoint": "tcp://0.0.0.0:6666"},
            autostart=True,
        )
        result = await start_process(
            name="zmq_broker",
            request=request,
            factory=mock_factory,
            _user=MagicMock(),
            _csrf=None,
        )
        assert result.status == "success"
        assert result.name == "zmq_broker"
        assert result.run_id == "run-001"
        mock_factory.start_process_by_name.assert_awaited_once_with(
            name="zmq_broker",
            mode="process",
            args=["arg1"],
            kwargs={"endpoint": "tcp://0.0.0.0:6666"},
            autostart=True,
        )

    @pytest.mark.asyncio
    async def test_start_process_without_overrides(self) -> None:
        """Test starting process with default parameters.

        Given: A factory and process start request without overrides,
        When: start_process is called,
        Then: Process is started with None parameters.
        """
        mock_factory = MagicMock()
        mock_factory.start_process_by_name = AsyncMock(
            return_value=ProcessStartResult(status="success", message="started")
        )
        request = ProcessStartRequest(mode=None, args=None, kwargs=None, autostart=None)
        result = await start_process(
            name="zmq_broker",
            request=request,
            factory=mock_factory,
            _user=MagicMock(),
            _csrf=None,
        )
        assert result.status == "success"
        mock_factory.start_process_by_name.assert_awaited_once_with(
            name="zmq_broker", mode=None, args=None, kwargs=None, autostart=None
        )


class TestStopProcess:
    """Tests for stopping running process instances."""

    @pytest.mark.asyncio
    async def test_stop_process(self) -> None:
        """Test stopping a running process.

        Given: A factory with a running process,
        When: stop_process is called,
        Then: Process is stopped and status is returned.
        """
        mock_factory = MagicMock()
        mock_factory.stop_process_by_name = AsyncMock(
            return_value=ProcessStopResult(status="success", message="stopped")
        )
        result = await stop_process(
            name="zmq_broker", factory=mock_factory, _user=MagicMock(), _csrf=None
        )
        assert result.status == "success"
        assert result.name == "zmq_broker"
        mock_factory.stop_process_by_name.assert_awaited_once_with("zmq_broker")


class TestProcessStartRequest:
    """Tests for ProcessStartRequest schema validation."""

    def test_process_start_request_all_fields(self) -> None:
        """Test ProcessStartRequest with all fields populated.

        Given: All request parameters specified,
        When: ProcessStartRequest is created,
        Then: All fields are correctly assigned.
        """
        request = ProcessStartRequest(
            mode="process",
            args=["arg1", 2],
            kwargs={"key": "value"},
            autostart=False,
        )
        assert request.mode == "process"
        assert request.args == ["arg1", 2]
        assert request.kwargs == {"key": "value"}
        assert request.autostart is False

    def test_process_start_request_defaults(self) -> None:
        """Test ProcessStartRequest with None defaults.

        Given: All parameters set to None,
        When: ProcessStartRequest is created,
        Then: All fields are None.
        """
        request = ProcessStartRequest(mode=None, args=None, kwargs=None, autostart=None)
        assert request.mode is None
        assert request.args is None
        assert request.kwargs is None
        assert request.autostart is None


class TestCreateProcessConfiguration:
    """Tests for creating new process configurations."""

    @pytest.mark.asyncio
    @patch("snapper.server.process_routes.get_registered_processes")
    async def test_create_process_configuration_success(self, mock_get_registry: MagicMock) -> None:
        """Test creating new process configuration.

        Given: A valid template in the registry,
        When: create_process_configuration is called,
        Then: Configuration is created in database.
        """
        mock_factory = MagicMock()
        mock_factory.get_class_defaults.return_value = {
            "enabled": False,
            "mode": "thread",
            "args": [],
            "kwargs": {
                "name": "default_strategy",
                "inputs": ["BTC-USD:1h"],
                "output": "signals.default_strategy",
            },
            "lifecycle": ProcessLifecycleEnum.LONG_RUNNING,
            "role": ProcessRoleEnum.STRATEGY,
            "tags": ("strategy",),
            "parameters_schema": {"type": "object"},
        }
        mock_factory.create_process_config = AsyncMock()
        strategy_class = MagicMock()
        strategy_class.get_default_kwargs.return_value = {
            "name": "default_strategy",
            "inputs": ["BTC-USD:1h"],
            "output": "signals.default_strategy",
        }
        mock_get_registry.return_value = {
            "strategy_macd_btc_1h": {
                "class_ref": strategy_class,
                "class_path": "snapper.strategies.process_wrapper.MACDStrategyBTC",
                "method": "start",
                "description": "MACD strategy",
                "lifecycle": ProcessLifecycleEnum.LONG_RUNNING,
                "role": ProcessRoleEnum.STRATEGY,
                "tags": ("strategy",),
                "parameters_schema": {"type": "object"},
            }
        }
        request = ProcessCreateRequest(
            name="strategy_macd_custom",
            template="strategy_macd_btc_1h",
            enabled=True,
            mode="process",
            args=[],
            kwargs={"name": "macd_custom"},
            note="UI created",
        )
        settings = MagicMock()
        result = await create_process_configuration(
            request=request,
            factory=mock_factory,
            settings=settings,
            _user=MagicMock(),
            _csrf=None,
        )
        mock_factory.create_process_config.assert_awaited_once_with(
            name="strategy_macd_custom",
            class_path="snapper.strategies.process_wrapper.MACDStrategyBTC",
            method="start",
            enabled=True,
            mode="process",
            args=[],
            kwargs={
                "name": "macd_custom",
                "inputs": ["BTC-USD:1h"],
                "output": "signals.default_strategy",
            },
            lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
            role=ProcessRoleEnum.STRATEGY,
            tags=("strategy",),
            parameters_schema={"type": "object"},
            note="UI created",
        )
        assert result.status == "created"
        assert result.process.name == "strategy_macd_custom"

    @pytest.mark.asyncio
    @patch("snapper.server.process_routes.get_registered_processes")
    async def test_create_process_configuration_template_not_found(
        self, mock_get_registry: MagicMock
    ) -> None:
        """Test 404 when template not found in registry.

        Given: An empty process registry,
        When: create_process_configuration is called with unknown template,
        Then: HTTPException 404 is raised.
        """
        mock_get_registry.return_value = {}
        request = ProcessCreateRequest(
            name="unknown",
            template="missing",
            enabled=None,
            mode=None,
            args=None,
            kwargs=None,
            note=None,
        )
        factory = MagicMock()
        settings = MagicMock()
        with pytest.raises(HTTPException) as exc_info:
            await create_process_configuration(
                request=request,
                factory=factory,
                settings=settings,
                _user=MagicMock(),
                _csrf=None,
            )
        assert exc_info.value.status_code == 404
        assert "Template" in exc_info.value.detail

    @pytest.mark.asyncio
    @patch("snapper.server.process_routes.get_registered_processes")
    async def test_create_process_configuration_conflict(
        self, mock_get_registry: MagicMock
    ) -> None:
        """Test 409 when configuration already exists.

        Given: A factory that raises ValueError for duplicate config,
        When: create_process_configuration is called,
        Then: HTTPException 409 is raised.
        """
        mock_factory = MagicMock()
        mock_factory.get_class_defaults.return_value = {
            "enabled": False,
            "mode": "thread",
            "args": [],
            "kwargs": {},
            "lifecycle": ProcessLifecycleEnum.LONG_RUNNING,
            "role": ProcessRoleEnum.CORE,
            "tags": (),
        }
        mock_factory.create_process_config = AsyncMock(side_effect=ValueError("exists"))
        strategy_class = MagicMock()
        mock_get_registry.return_value = {
            "strategy_macd_btc_1h": {
                "class_ref": strategy_class,
                "class_path": "path",
                "method": "start",
                "description": "desc",
                "lifecycle": ProcessLifecycleEnum.LONG_RUNNING,
                "role": ProcessRoleEnum.STRATEGY,
            }
        }
        request = ProcessCreateRequest(
            name="strategy_macd_custom",
            template="strategy_macd_btc_1h",
            enabled=None,
            mode=None,
            args=None,
            kwargs=None,
            note=None,
        )
        with pytest.raises(HTTPException) as exc_info:
            await create_process_configuration(
                request=request,
                factory=mock_factory,
                settings=MagicMock(),
                _user=MagicMock(),
                _csrf=None,
            )
        assert exc_info.value.status_code == 409
        assert "exists" in exc_info.value.detail


class TestProcessRoutesEdgeCases:
    """Tests for edge cases in process route handling."""

    @pytest.mark.asyncio
    @patch("snapper.server.process_routes.get_registered_processes")
    async def test_list_available_processes_tags_not_iterable(
        self, mock_get_registry: MagicMock
    ) -> None:
        """Verify tags fallback when not iterable.

        Given: A process with non-iterable tags string,
        When: list_available_processes is called,
        Then: Empty tags list is returned.
        """
        mock_get_registry.return_value = {
            "test_process": {
                "class_path": "snapper.test.TestProcess",
                "method": "run",
                "description": "Test process",
                "class_ref": MagicMock(),
                "lifecycle": ProcessLifecycleEnum.LONG_RUNNING,
                "role": ProcessRoleEnum.CORE,
                "tags": "not_iterable_string",
            },
        }
        result = await list_available_processes(_user=MagicMock())
        assert result.count == 1
        assert result.processes[0].tags == []

    @pytest.mark.asyncio
    @patch("snapper.server.process_routes.get_registered_processes")
    async def test_create_process_get_default_kwargs_exception(
        self, mock_get_registry: MagicMock
    ) -> None:
        """Verify graceful handling of get_default_kwargs exception.

        Given: A process class where get_default_kwargs raises exception,
        When: create_process_configuration is called,
        Then: Configuration is created with provided kwargs only.
        """
        mock_factory = MagicMock()
        mock_factory.get_templates = AsyncMock(return_value=["test_template"])
        mock_factory.create_process_config = AsyncMock(return_value=MagicMock())

        class FailingKwargsClass:
            @staticmethod
            def get_default_kwargs(settings: MagicMock) -> dict[str, object]:
                raise RuntimeError("Failed to load defaults")

        mock_get_registry.return_value = {
            "test_template": {
                "class_ref": FailingKwargsClass,
                "class_path": "snapper.test.FailingKwargsClass",
                "method": "run",
                "description": "Test with failing get_default_kwargs",
                "lifecycle": ProcessLifecycleEnum.LONG_RUNNING,
                "role": ProcessRoleEnum.CORE,
                "args": [],
                "kwargs": {},
            }
        }
        request = ProcessCreateRequest(
            name="test_process",
            template="test_template",
            enabled=True,
            mode="thread",
            args=None,
            kwargs={"custom": "value"},
            note=None,
        )
        await create_process_configuration(
            request=request,
            factory=mock_factory,
            settings=MagicMock(),
            _user=MagicMock(),
            _csrf=None,
        )
        mock_factory.create_process_config.assert_called_once()
        call_args = mock_factory.create_process_config.call_args
        assert call_args[1]["kwargs"] == {"custom": "value"}

    @pytest.mark.asyncio
    @patch("snapper.server.process_routes.get_registered_processes")
    async def test_create_process_no_get_default_kwargs(self, mock_get_registry: MagicMock) -> None:
        """Verify creation succeeds without get_default_kwargs method.

        Given: A process class without get_default_kwargs method,
        When: create_process_configuration is called,
        Then: Configuration is created with provided kwargs.
        """
        mock_factory = MagicMock()
        mock_factory.get_templates = AsyncMock(return_value=["test_template"])
        mock_factory.create_process_config = AsyncMock(return_value=MagicMock())
        mock_class = MagicMock(spec=[])
        mock_get_registry.return_value = {
            "test_template": {
                "class_ref": mock_class,
                "class_path": "snapper.test.NoKwargsClass",
                "method": "run",
                "description": "Test without get_default_kwargs",
                "lifecycle": ProcessLifecycleEnum.LONG_RUNNING,
                "role": ProcessRoleEnum.CORE,
                "args": [],
                "kwargs": {},
            }
        }
        request = ProcessCreateRequest(
            name="test_process",
            template="test_template",
            enabled=True,
            mode="thread",
            args=None,
            kwargs={"custom": "value"},
            note=None,
        )
        await create_process_configuration(
            request=request,
            factory=mock_factory,
            settings=MagicMock(),
            _user=MagicMock(),
            _csrf=None,
        )
        mock_factory.create_process_config.assert_called_once()
        call_args = mock_factory.create_process_config.call_args
        assert call_args[1]["kwargs"] == {"custom": "value"}

    @pytest.mark.asyncio
    @patch("snapper.server.process_routes.get_registered_processes")
    async def test_create_process_tags_not_iterable(self, mock_get_registry: MagicMock) -> None:
        """Verify tags fallback to empty tuple when not iterable.

        Given: A process with non-iterable tags string,
        When: create_process_configuration is called,
        Then: Configuration is created with empty tags tuple.
        """
        mock_factory = MagicMock()
        mock_factory.get_templates = AsyncMock(return_value=["test_template"])
        mock_factory.create_process_config = AsyncMock(return_value=MagicMock())
        mock_class = MagicMock()
        mock_class.get_default_kwargs = MagicMock(return_value={})
        mock_get_registry.return_value = {
            "test_template": {
                "class_ref": mock_class,
                "class_path": "snapper.test.TestClass",
                "method": "run",
                "description": "Test with non-iterable tags",
                "lifecycle": ProcessLifecycleEnum.LONG_RUNNING,
                "role": ProcessRoleEnum.CORE,
                "args": [],
                "kwargs": {},
                "tags": "not_iterable_string",
            }
        }
        request = ProcessCreateRequest(
            name="test_process",
            template="test_template",
            enabled=True,
            mode="thread",
            args=None,
            kwargs=None,
            note=None,
        )
        await create_process_configuration(
            request=request,
            factory=mock_factory,
            settings=MagicMock(),
            _user=MagicMock(),
            _csrf=None,
        )
        mock_factory.create_process_config.assert_called_once()
        call_args = mock_factory.create_process_config.call_args
        assert call_args[1]["tags"] == ()

    @pytest.mark.asyncio
    async def test_list_process_runs(self) -> None:
        """Test listing historical process run records.

        Given: A factory with recent run history,
        When: list_process_runs is called,
        Then: All run records are returned with details.
        """
        mock_factory = MagicMock()
        mock_factory.get_recent_runs = AsyncMock(
            return_value=[
                {
                    "run_id": "run-001",
                    "process_name": "zmq_broker",
                    "status": "succeeded",
                    "role": "core",
                    "lifecycle": "long_running",
                    "parameters": None,
                    "result": None,
                    "error": None,
                    "tags": [],
                    "started_at": "2026-01-04T10:00:00Z",
                    "completed_at": "2026-01-04T11:00:00Z",
                },
                {
                    "run_id": "run-002",
                    "process_name": "feed_publisher",
                    "status": "running",
                    "role": "core",
                    "lifecycle": "long_running",
                    "parameters": None,
                    "result": None,
                    "error": None,
                    "tags": [],
                    "started_at": "2026-01-04T10:00:00Z",
                    "completed_at": None,
                },
            ]
        )
        result = await list_process_runs(
            factory=mock_factory,
            _user=MagicMock(),
            limit=50,
            name=None,
        )
        assert result.count == 2
        assert len(result.runs) == 2
        mock_factory.get_recent_runs.assert_awaited_once_with(limit=50, name=None)

    @pytest.mark.asyncio
    async def test_list_process_runs_filtered(self) -> None:
        """Test listing runs filtered by process name.

        Given: A factory with run history for specific process,
        When: list_process_runs is called with name filter,
        Then: Only matching run records are returned.
        """
        mock_factory = MagicMock()
        mock_factory.get_recent_runs = AsyncMock(
            return_value=[
                {
                    "run_id": "run-001",
                    "process_name": "zmq_broker",
                    "status": "succeeded",
                    "role": "core",
                    "lifecycle": "long_running",
                    "parameters": None,
                    "result": None,
                    "error": None,
                    "tags": [],
                    "started_at": "2026-01-04T10:00:00Z",
                    "completed_at": "2026-01-04T11:00:00Z",
                },
            ]
        )
        result = await list_process_runs(
            factory=mock_factory,
            _user=MagicMock(),
            limit=10,
            name="zmq_broker",
        )
        assert result.count == 1
        mock_factory.get_recent_runs.assert_awaited_once_with(limit=10, name="zmq_broker")
