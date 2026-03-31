"""Tests for process management REST API routes."""

from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from fastapi import HTTPException
from fastapi import Request

from snapper.api.schemas.process import ProcessCreateBody
from snapper.api.schemas.process import ProcessCreateRequest
from snapper.api.schemas.process import ProcessStartBody
from snapper.api.schemas.process import ProcessStartRequest
from snapper.application.process_manager.models import ProcessConfigModel
from snapper.application.process_manager.models import ProcessRegistryEntry
from snapper.application.process_manager.models import ProcessStartResult
from snapper.application.process_manager.models import ProcessStopResult
from snapper.core.types import ProcessLifecycleEnum
from snapper.core.types import ProcessRoleEnum
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.process_routes import create_process_configuration
from snapper.server.process_routes import get_process_factory
from snapper.server.process_routes import get_process_schema
from snapper.server.process_routes import get_process_summary
from snapper.server.process_routes import list_available_processes
from snapper.server.process_routes import list_configured_processes
from snapper.server.process_routes import list_process_runs
from snapper.server.process_routes import start_process
from snapper.server.process_routes import stop_process


def _make_rest_request() -> MagicMock:
    """Create a mock FastAPI Request with rest_tracker."""
    mock_request = MagicMock()
    mock_request.app.state.rest_tracker = SequenceTracker()
    return mock_request


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
        mock_class = MagicMock()
        mock_get_registry.return_value = {
            "zmq_broker": ProcessRegistryEntry(
                class_ref=mock_class,
                class_path="snapper.ipc.zmq_broker.ZmqBrokerThread",
                method="run",
                description="ZMQ message broker",
                priority=50,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.CORE,
                tags=(),
                parameters_model=None,
                parameters_schema=None,
                enabled=False,
                mode="thread",
            ),
            "feed_publisher": ProcessRegistryEntry(
                class_ref=mock_class,
                class_path="snapper.ipc.feed_publisher.MarketDataPublisherService",
                method="run",
                description="Market data publisher",
                priority=50,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.CORE,
                tags=(),
                parameters_model=None,
                parameters_schema=None,
                enabled=False,
                mode="thread",
            ),
        }
        result = await list_available_processes(request=_make_rest_request(), _user=MagicMock())
        assert result.count == 2
        assert len(result.payload) == 2
        assert result.payload[0].name == "zmq_broker"
        assert result.payload[0].class_path == "snapper.ipc.zmq_broker.ZmqBrokerThread"
        assert result.payload[0].method == "run"
        assert result.payload[0].description == "ZMQ message broker"

    @pytest.mark.asyncio
    @patch("snapper.server.process_routes.get_registered_processes")
    async def test_list_available_processes_empty(self, mock_get_registry: MagicMock) -> None:
        """Test listing returns empty when no processes registered.

        Given: Empty process registry,
        When: list_available_processes is called,
        Then: Empty list with zero count is returned.
        """
        mock_get_registry.return_value = {}
        result = await list_available_processes(request=_make_rest_request(), _user=MagicMock())
        assert result.count == 0
        assert result.payload == []


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
                    parameters={"endpoint": "tcp://0.0.0.0:5555"},
                    note="Test broker",
                    lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                )
            ]
        )
        mock_factory.started_processes = {"zmq_broker": MagicMock()}
        mock_factory.active_runs = {}
        result = await list_configured_processes(
            request=_make_rest_request(), factory=mock_factory, _user=MagicMock()
        )
        assert result.count == 1
        assert len(result.payload) == 1
        process = result.payload[0]
        assert process.name == "zmq_broker"
        assert process.enabled is True
        assert process.mode == "thread"
        assert process.class_path == "snapper.ipc.zmq_broker.ZmqBrokerThread"
        assert process.method == "run"
        assert process.parameters == {"endpoint": "tcp://0.0.0.0:5555"}
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
        result = await list_configured_processes(
            request=_make_rest_request(), factory=mock_factory, _user=MagicMock()
        )
        assert result.count == 0
        assert result.payload == []


class TestGetProcessSummary:
    """Tests for lightweight process summary endpoint."""

    @pytest.mark.asyncio
    async def test_empty_processes(self) -> None:
        """Test summary with no configured processes returns all zeros."""
        mock_factory = MagicMock()
        mock_factory.get_process_configs = AsyncMock(return_value=[])
        mock_factory.started_processes = {}
        result = await get_process_summary(
            request=_make_rest_request(), factory=mock_factory, _user=MagicMock()
        )
        assert result.payload.feeds.running == 0
        assert result.payload.feeds.total == 0
        assert result.payload.strategies.running == 0
        assert result.payload.strategies.total == 0
        assert result.payload.executors.running == 0
        assert result.payload.executors.total == 0
        assert result.payload.brokers.running == 0
        assert result.payload.brokers.total == 0

    @pytest.mark.asyncio
    async def test_mixed_processes_categorization(self) -> None:
        """Test processes are correctly categorized by name and role."""
        mock_factory = MagicMock()
        mock_factory.get_process_configs = AsyncMock(
            return_value=[
                ProcessConfigModel(
                    name="kraken_feed_publisher",
                    enabled=True,
                    mode="process",
                    class_path="snapper.feeds.KrakenFeed",
                    method="run",
                    parameters={},
                ),
                ProcessConfigModel(
                    name="polygon_feed_publisher",
                    enabled=True,
                    mode="process",
                    class_path="snapper.feeds.PolygonFeed",
                    method="run",
                    parameters={},
                ),
                ProcessConfigModel(
                    name="momentum_strategy",
                    enabled=True,
                    mode="process",
                    class_path="snapper.strategies.Momentum",
                    method="run",
                    parameters={},
                    role=ProcessRoleEnum.STRATEGY,
                ),
                ProcessConfigModel(
                    name="executor_kraken",
                    enabled=True,
                    mode="thread",
                    class_path="snapper.executors.Kraken",
                    method="run",
                    parameters={},
                ),
                ProcessConfigModel(
                    name="zmq_broker",
                    enabled=True,
                    mode="thread",
                    class_path="snapper.ipc.zmq_broker.ZmqBrokerThread",
                    method="run",
                    parameters={},
                ),
            ]
        )
        mock_factory.started_processes = {
            "kraken_feed_publisher": MagicMock(),
            "momentum_strategy": MagicMock(),
            "zmq_broker": MagicMock(),
        }
        result = await get_process_summary(
            request=_make_rest_request(), factory=mock_factory, _user=MagicMock()
        )
        assert result.payload.feeds.running == 1
        assert result.payload.feeds.total == 2
        assert result.payload.strategies.running == 1
        assert result.payload.strategies.total == 1
        assert result.payload.executors.running == 0
        assert result.payload.executors.total == 1
        assert result.payload.brokers.running == 1
        assert result.payload.brokers.total == 1

    @pytest.mark.asyncio
    async def test_uncategorized_process_not_counted(self) -> None:
        """Test processes that match no category are excluded from counts."""
        mock_factory = MagicMock()
        mock_factory.get_process_configs = AsyncMock(
            return_value=[
                ProcessConfigModel(
                    name="backfill_symbols",
                    enabled=True,
                    mode="process",
                    class_path="snapper.tasks.Backfill",
                    method="run",
                    parameters={},
                    role=ProcessRoleEnum.TASK,
                ),
            ]
        )
        mock_factory.started_processes = {"backfill_symbols": MagicMock()}
        result = await get_process_summary(
            request=_make_rest_request(), factory=mock_factory, _user=MagicMock()
        )
        assert result.payload.feeds.total == 0
        assert result.payload.strategies.total == 0
        assert result.payload.executors.total == 0
        assert result.payload.brokers.total == 0


class TestGetProcessSchema:
    """Tests for retrieving process configuration schemas."""

    @pytest.mark.asyncio
    @patch("snapper.server.process_routes.get_registered_processes")
    async def test_get_process_schema_with_defaults(self, mock_get_registry: MagicMock) -> None:
        """Test schema returns default parameters from class method.

        Given: A process class with get_default_parameters method,
        When: get_process_schema is called,
        Then: Schema includes default parameters and configuration.
        """
        mock_class = MagicMock()
        mock_class.get_default_parameters = MagicMock(
            return_value={"endpoint": "tcp://0.0.0.0:5555"}
        )
        mock_get_registry.return_value = {
            "zmq_broker": ProcessRegistryEntry(
                class_ref=mock_class,
                class_path="snapper.ipc.zmq_broker.ZmqBrokerThread",
                method="run",
                description="ZMQ message broker",
                priority=50,
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
        result = await get_process_schema(
            request=_make_rest_request(), name="zmq_broker", settings=settings, _user=MagicMock()
        )
        assert result.payload.name == "zmq_broker"
        assert result.payload.description == "ZMQ message broker"
        assert result.payload.class_path == "snapper.ipc.zmq_broker.ZmqBrokerThread"
        assert result.payload.method == "run"
        assert result.payload.default_enabled is True
        assert result.payload.default_mode == "thread"
        assert result.payload.default_parameters == {"endpoint": "tcp://0.0.0.0:5555"}

    @pytest.mark.asyncio
    @patch("snapper.server.process_routes.get_registered_processes")
    async def test_get_process_schema_without_defaults(self, mock_get_registry: MagicMock) -> None:
        """Test schema falls back when get_default_parameters missing.

        Given: A process class without get_default_parameters method,
        When: get_process_schema is called,
        Then: Schema returns empty parameters defaults.
        """
        mock_class = MagicMock()
        del mock_class.get_default_parameters
        mock_get_registry.return_value = {
            "custom_process": ProcessRegistryEntry(
                class_ref=mock_class,
                class_path="custom.Process",
                method="run",
                description="Custom process",
                priority=50,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.CORE,
                tags=(),
                parameters_model=None,
                parameters_schema=None,
                enabled=False,
                mode="thread",
            )
        }
        settings = MagicMock()
        result = await get_process_schema(
            request=_make_rest_request(),
            name="custom_process",
            settings=settings,
            _user=MagicMock(),
        )
        assert result.payload.name == "custom_process"
        assert result.payload.default_enabled is False
        assert result.payload.default_mode == "thread"
        assert result.payload.default_parameters == {}

    @pytest.mark.asyncio
    @patch("snapper.server.process_routes.get_registered_processes")
    async def test_get_process_schema_get_default_parameters_raises(
        self, mock_get_registry: MagicMock
    ) -> None:
        """Test schema handles get_default_parameters exceptions.

        Given: A process class where get_default_parameters raises an error,
        When: get_process_schema is called,
        Then: Schema returns empty parameters without propagating error.
        """
        mock_class = MagicMock()
        mock_class.get_default_parameters = MagicMock(side_effect=Exception("DB error"))
        mock_get_registry.return_value = {
            "failing_process": ProcessRegistryEntry(
                class_ref=mock_class,
                class_path="failing.Process",
                method="run",
                description="Process with failing get_default_parameters",
                priority=50,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.CORE,
                tags=(),
                parameters_model=None,
                parameters_schema=None,
                enabled=True,
                mode="process",
            )
        }
        settings = MagicMock()
        result = await get_process_schema(
            request=_make_rest_request(),
            name="failing_process",
            settings=settings,
            _user=MagicMock(),
        )
        assert result.payload.default_parameters == {}
        assert result.payload.default_enabled is True
        assert result.payload.default_mode == "process"

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
            await get_process_schema(
                request=_make_rest_request(),
                name="nonexistent",
                settings=settings,
                _user=MagicMock(),
            )
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
            return_value=ProcessStartResult(
                status="success", message="started", public_id="run-001"
            )
        )
        body = ProcessStartRequest(
            session_id="test-sid",
            sequence_id=1,
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessStartBody(
                mode="process",
                parameters={"endpoint": "tcp://0.0.0.0:6666"},
            ),
        )
        result = await start_process(
            http_request=_make_rest_request(),
            name="zmq_broker",
            body=body,
            factory=mock_factory,
            _user=MagicMock(),
            _csrf=None,
        )
        assert result.payload.status == "success"
        assert result.payload.name == "zmq_broker"
        assert result.payload.process_public_id == "run-001"
        mock_factory.start_process_by_name.assert_awaited_once_with(
            name="zmq_broker",
            mode="process",
            parameters={"endpoint": "tcp://0.0.0.0:6666"},
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
        body = ProcessStartRequest(
            session_id="test-sid",
            sequence_id=1,
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessStartBody(
                mode=None,
                parameters=None,
            ),
        )
        result = await start_process(
            http_request=_make_rest_request(),
            name="zmq_broker",
            body=body,
            factory=mock_factory,
            _user=MagicMock(),
            _csrf=None,
        )
        assert result.payload.status == "success"
        mock_factory.start_process_by_name.assert_awaited_once_with(
            name="zmq_broker", mode=None, parameters=None
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
            request=_make_rest_request(),
            name="zmq_broker",
            factory=mock_factory,
            _user=MagicMock(),
            _csrf=None,
        )
        assert result.payload.status == "success"
        assert result.payload.name == "zmq_broker"
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
            session_id="test-sid",
            sequence_id=1,
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessStartBody(
                mode="process",
                parameters={"key": "value"},
            ),
        )
        assert request.payload.mode == "process"
        assert request.payload.parameters == {"key": "value"}

    def test_process_start_request_defaults(self) -> None:
        """Test ProcessStartRequest with None defaults.

        Given: All parameters set to None,
        When: ProcessStartRequest is created,
        Then: All fields are None.
        """
        request = ProcessStartRequest(
            session_id="test-sid",
            sequence_id=1,
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessStartBody(
                mode=None,
                parameters=None,
            ),
        )
        assert request.payload.mode is None
        assert request.payload.parameters is None


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
            "parameters": {
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
        strategy_class.get_default_parameters.return_value = {
            "name": "default_strategy",
            "inputs": ["BTC-USD:1h"],
            "output": "signals.default_strategy",
        }
        mock_get_registry.return_value = {
            "strategy_macd_btc_1h": ProcessRegistryEntry(
                class_ref=strategy_class,
                class_path="snapper.strategies.process_wrapper.MACDStrategyBTC",
                method="start",
                description="MACD strategy",
                priority=50,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.STRATEGY,
                tags=("strategy",),
                parameters_model=None,
                parameters_schema={"type": "object"},
                enabled=False,
                mode="thread",
            )
        }
        request = ProcessCreateRequest(
            session_id="test-sid",
            sequence_id=1,
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessCreateBody(
                name="strategy_macd_custom",
                template="strategy_macd_btc_1h",
                enabled=True,
                mode="process",
                parameters={"name": "macd_custom"},
                note="UI created",
            ),
        )
        settings = MagicMock()
        result = await create_process_configuration(
            http_request=_make_rest_request(),
            body=request,
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
            parameters={
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
        assert result.payload.status == "created"
        assert result.payload.process.name == "strategy_macd_custom"

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
            session_id="test-sid",
            sequence_id=1,
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessCreateBody(
                name="unknown",
                template="missing",
                enabled=None,
                mode=None,
                parameters=None,
                note=None,
            ),
        )
        factory = MagicMock()
        settings = MagicMock()
        with pytest.raises(HTTPException) as exc_info:
            await create_process_configuration(
                http_request=_make_rest_request(),
                body=request,
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
            "parameters": {},
            "lifecycle": ProcessLifecycleEnum.LONG_RUNNING,
            "role": ProcessRoleEnum.CORE,
            "tags": (),
        }
        mock_factory.create_process_config = AsyncMock(side_effect=ValueError("exists"))
        strategy_class = MagicMock()
        mock_get_registry.return_value = {
            "strategy_macd_btc_1h": ProcessRegistryEntry(
                class_ref=strategy_class,
                class_path="path",
                method="start",
                description="desc",
                priority=50,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.STRATEGY,
                tags=(),
                parameters_model=None,
                parameters_schema=None,
                enabled=False,
                mode="thread",
            )
        }
        request = ProcessCreateRequest(
            session_id="test-sid",
            sequence_id=1,
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessCreateBody(
                name="strategy_macd_custom",
                template="strategy_macd_btc_1h",
                enabled=None,
                mode=None,
                parameters=None,
                note=None,
            ),
        )
        with pytest.raises(HTTPException) as exc_info:
            await create_process_configuration(
                http_request=_make_rest_request(),
                body=request,
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
    async def test_list_available_processes_with_empty_tags(
        self, mock_get_registry: MagicMock
    ) -> None:
        """Verify empty tags are handled correctly.

        Given: A process with empty tags tuple,
        When: list_available_processes is called,
        Then: Empty tags list is returned.
        """
        mock_class = MagicMock()
        mock_get_registry.return_value = {
            "test_process": ProcessRegistryEntry(
                class_ref=mock_class,
                class_path="snapper.test.TestProcess",
                method="run",
                description="Test process",
                priority=50,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.CORE,
                tags=(),
                parameters_model=None,
                parameters_schema=None,
                enabled=False,
                mode="thread",
            ),
        }
        result = await list_available_processes(request=_make_rest_request(), _user=MagicMock())
        assert result.count == 1
        assert result.payload[0].tags == []

    @pytest.mark.asyncio
    @patch("snapper.server.process_routes.get_registered_processes")
    async def test_create_process_get_default_parameters_exception(
        self, mock_get_registry: MagicMock
    ) -> None:
        """Verify graceful handling of get_default_parameters exception.

        Given: A process class where get_default_parameters raises exception,
        When: create_process_configuration is called,
        Then: Configuration is created with provided parameters only.
        """
        mock_factory = MagicMock()
        mock_factory.get_templates = AsyncMock(return_value=["test_template"])
        mock_factory.create_process_config = AsyncMock(return_value=MagicMock())

        class FailingKwargsClass:
            @staticmethod
            def get_default_parameters(settings: MagicMock) -> dict[str, object]:
                raise RuntimeError("Failed to load defaults")

        mock_get_registry.return_value = {
            "test_template": ProcessRegistryEntry(
                class_ref=FailingKwargsClass,
                class_path="snapper.test.FailingKwargsClass",
                method="run",
                description="Test with failing get_default_parameters",
                priority=50,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.CORE,
                tags=(),
                parameters_model=None,
                parameters_schema=None,
                enabled=False,
                mode="thread",
            )
        }
        request = ProcessCreateRequest(
            session_id="test-sid",
            sequence_id=1,
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessCreateBody(
                name="test_process",
                template="test_template",
                enabled=True,
                mode="thread",
                parameters={"custom": "value"},
                note=None,
            ),
        )
        await create_process_configuration(
            http_request=_make_rest_request(),
            body=request,
            factory=mock_factory,
            settings=MagicMock(),
            _user=MagicMock(),
            _csrf=None,
        )
        mock_factory.create_process_config.assert_called_once()
        call_args = mock_factory.create_process_config.call_args
        assert call_args[1]["parameters"] == {"custom": "value"}

    @pytest.mark.asyncio
    @patch("snapper.server.process_routes.get_registered_processes")
    async def test_create_process_no_get_default_parameters(
        self, mock_get_registry: MagicMock
    ) -> None:
        """Verify creation succeeds without get_default_parameters method.

        Given: A process class without get_default_parameters method,
        When: create_process_configuration is called,
        Then: Configuration is created with provided parameters.
        """
        mock_factory = MagicMock()
        mock_factory.get_templates = AsyncMock(return_value=["test_template"])
        mock_factory.create_process_config = AsyncMock(return_value=MagicMock())
        mock_class = MagicMock(spec=[])
        mock_get_registry.return_value = {
            "test_template": ProcessRegistryEntry(
                class_ref=mock_class,
                class_path="snapper.test.NoKwargsClass",
                method="run",
                description="Test without get_default_parameters",
                priority=50,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.CORE,
                tags=(),
                parameters_model=None,
                parameters_schema=None,
                enabled=False,
                mode="thread",
            )
        }
        request = ProcessCreateRequest(
            session_id="test-sid",
            sequence_id=1,
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessCreateBody(
                name="test_process",
                template="test_template",
                enabled=True,
                mode="thread",
                parameters={"custom": "value"},
                note=None,
            ),
        )
        await create_process_configuration(
            http_request=_make_rest_request(),
            body=request,
            factory=mock_factory,
            settings=MagicMock(),
            _user=MagicMock(),
            _csrf=None,
        )
        mock_factory.create_process_config.assert_called_once()
        call_args = mock_factory.create_process_config.call_args
        assert call_args[1]["parameters"] == {"custom": "value"}

    @pytest.mark.asyncio
    @patch("snapper.server.process_routes.get_registered_processes")
    async def test_create_process_with_empty_tags(self, mock_get_registry: MagicMock) -> None:
        """Verify empty tags are handled correctly.

        Given: A process with empty tags tuple,
        When: create_process_configuration is called,
        Then: Configuration is created with empty tags tuple.
        """
        mock_factory = MagicMock()
        mock_factory.get_templates = AsyncMock(return_value=["test_template"])
        mock_factory.create_process_config = AsyncMock(return_value=MagicMock())
        mock_class = MagicMock()
        mock_class.get_default_parameters = MagicMock(return_value={})
        mock_get_registry.return_value = {
            "test_template": ProcessRegistryEntry(
                class_ref=mock_class,
                class_path="snapper.test.TestClass",
                method="run",
                description="Test with empty tags",
                priority=50,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.CORE,
                tags=(),
                parameters_model=None,
                parameters_schema=None,
                enabled=False,
                mode="thread",
            )
        }
        request = ProcessCreateRequest(
            session_id="test-sid",
            sequence_id=1,
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessCreateBody(
                name="test_process",
                template="test_template",
                enabled=True,
                mode="thread",
                parameters=None,
                note=None,
            ),
        )
        await create_process_configuration(
            http_request=_make_rest_request(),
            body=request,
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
                    "public_id": "run-001",
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
                    "session_id": "test-sid",
                    "sequence_id": 1,
                    "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
                },
                {
                    "public_id": "run-002",
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
                    "session_id": "test-sid",
                    "sequence_id": 2,
                    "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
                },
            ]
        )
        result = await list_process_runs(
            request=_make_rest_request(),
            factory=mock_factory,
            _user=MagicMock(),
            limit=50,
            name=None,
        )
        assert result.count == 2
        assert len(result.payload) == 2
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
                    "public_id": "run-001",
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
                    "session_id": "test-sid",
                    "sequence_id": 1,
                    "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
                },
            ]
        )
        result = await list_process_runs(
            request=_make_rest_request(),
            factory=mock_factory,
            _user=MagicMock(),
            limit=10,
            name="zmq_broker",
        )
        assert result.count == 1
        mock_factory.get_recent_runs.assert_awaited_once_with(limit=10, name="zmq_broker")
