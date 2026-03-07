"""Tests for FastAPI server application and API router."""

import asyncio
import contextlib
import datetime as dt
import json
import os
import sys
import tempfile
from collections.abc import Generator
from datetime import datetime
from pathlib import Path
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi.testclient import TestClient

from snapper.auth.dependencies import require_authentication
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.user import UserProfile
from snapper.data.models import Candle
from snapper.data.models import Instrument
from snapper.interface.websocket.models import ConnectionStats
from snapper.interface.websocket.models import WsStatsSnapshot
from snapper.server import process_runner
from snapper.server.app import _build_strategy_payload
from snapper.server.app import create_api_router
from snapper.server.app import create_app
from snapper.server.app import get_repository_dependency
from snapper.server.app import get_settings_dependency
from snapper.server.app import lifespan
from snapper.utils.logging import setup_logging
from tests.server import dummy_processes


class TestLifespan:
    """Tests for application lifespan management."""

    @pytest.mark.asyncio
    async def test_lifespan_startup_and_shutdown(self) -> None:
        """Test application lifespan manages startup and shutdown.

        Given: A mock FastAPI app with manager and settings,
        When: The lifespan context manager runs,
        Then: Processes are discovered, synced, started, and stopped.
        """
        mock_app = MagicMock()
        mock_manager = MagicMock()
        mock_manager.cleanup = AsyncMock()
        mock_zmq_bridge = MagicMock()
        mock_zmq_bridge.start = AsyncMock()
        mock_zmq_bridge.stop = AsyncMock()
        mock_manager.zmq_bridge = mock_zmq_bridge
        mock_app.state.manager = mock_manager
        with (
            patch("snapper.server.app.discover_processes") as mock_discover,
            patch("snapper.server.app.ProcessLauncherService") as mock_factory_cls,
            patch("snapper.server.app.get_settings_service") as mock_get_settings_service,
        ):
            mock_settings_service = MagicMock()
            mock_settings_service.shutdown = AsyncMock()
            mock_get_settings_service.return_value = mock_settings_service
            mock_factory = MagicMock()
            mock_factory.sync_registry_to_database = AsyncMock()
            mock_factory.start_all_processes = AsyncMock()
            mock_factory.stop_all_processes = AsyncMock()
            mock_factory_cls.return_value = mock_factory
            async with lifespan(mock_app):
                """Consumed by iteration to trigger exception."""
                pass
        mock_discover.assert_called_once()
        mock_factory.sync_registry_to_database.assert_awaited_once()
        mock_factory.start_all_processes.assert_awaited_once()
        mock_factory.stop_all_processes.assert_awaited_once()
        mock_manager.cleanup.assert_called_once()

    @pytest.mark.asyncio
    async def test_lifespan_cleanup_error_propagates(self) -> None:
        """Test cleanup errors propagate through lifespan.

        Given: A manager cleanup that raises an exception,
        When: The lifespan context exits,
        Then: The exception propagates to the caller.
        """
        mock_app = MagicMock()
        mock_manager = MagicMock()
        mock_manager.cleanup = AsyncMock(side_effect=Exception("Cleanup error"))
        mock_app.state.manager = mock_manager
        with (
            patch("snapper.server.app.discover_processes") as mock_discover,
            patch("snapper.server.app.ProcessLauncherService") as mock_factory_cls,
            patch("snapper.server.app.get_settings_service") as mock_get_settings_service,
        ):
            mock_settings_service = MagicMock()
            mock_get_settings_service.return_value = mock_settings_service
            mock_factory = MagicMock()
            mock_factory.sync_registry_to_database = AsyncMock()
            mock_factory.start_all_processes = AsyncMock()
            mock_factory.stop_all_processes = AsyncMock()
            mock_factory_cls.return_value = mock_factory
            with pytest.raises(Exception, match="Cleanup error"):
                async with lifespan(mock_app):
                    """Consumed by iteration to trigger exception."""
                    pass
        mock_discover.assert_called_once()
        mock_factory.stop_all_processes.assert_awaited_once()


class TestCreateApp:
    """Tests for FastAPI application factory."""

    def test_create_app_returns_fastapi_instance(self) -> None:
        """Test create_app returns configured FastAPI instance.

        Given: The application factory,
        When: create_app is called,
        Then: A FastAPI instance with correct title and version is returned.
        """
        app = create_app()
        assert isinstance(app, FastAPI)
        assert app.title == "Snapper Trading Dashboard"
        assert app.version is not None

    def test_create_app_mounts_static_when_available(self) -> None:
        """Test static files are mounted when directory exists.

        Given: Static directory exists on filesystem,
        When: create_app is called,
        Then: Static files are mounted to the app.
        """
        with (
            patch("snapper.server.app.os.path.exists", return_value=True),
            patch.object(FastAPI, "mount") as mock_mount,
        ):
            create_app()
        mock_mount.assert_called_once()

    def test_create_app_skips_static_when_missing(self) -> None:
        """Test static files mounting is skipped when directory missing.

        Given: Static directory does not exist,
        When: create_app is called,
        Then: No static files are mounted.
        """
        with (
            patch("snapper.server.app.os.path.exists", return_value=False),
            patch.object(FastAPI, "mount") as mock_mount,
        ):
            create_app()
        mock_mount.assert_not_called()


class TestCreateApiRouter:
    """Tests for API router creation and endpoint accessibility."""

    def setup_method(self) -> None:
        """Initialize test client with dependency overrides."""
        self.app = create_app()

        def skip_csrf_validation() -> None:
            return None

        def skip_authentication() -> UserProfile:
            return UserProfile(id="test_id", username="test_user", role=UserRole.ADMIN)

        self.app.dependency_overrides[validate_csrf_token] = skip_csrf_validation
        self.app.dependency_overrides[require_authentication] = skip_authentication
        self.client = TestClient(self.app)

    def test_router_creation_without_errors(self) -> None:
        """Test API router creation succeeds.

        Given: A WebSocket connection manager,
        When: create_api_router is called,
        Then: A valid router object is returned.
        """
        with patch("snapper.server.app.WebSocketConnectionManager") as mock_manager_class:
            mock_manager = MagicMock()
            mock_manager_class.return_value = mock_manager
            router = create_api_router(mock_manager)
            assert router is not None

    @patch("snapper.server.app.get_settings")
    @patch("snapper.server.app.get_repository")
    def test_health_check_endpoint(
        self, mock_get_repo: MagicMock, mock_get_settings: MagicMock
    ) -> None:
        """Test health check endpoint returns healthy status.

        Given: Mocked settings and repository,
        When: GET /api/health is called,
        Then: Response contains healthy status and timestamp.
        """
        mock_settings = MagicMock()
        mock_get_settings.return_value = mock_settings
        mock_repo = MagicMock()
        mock_get_repo.return_value = mock_repo
        response = self.client.get("/api/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "timestamp" in data

    @patch("snapper.server.app.get_settings")
    @patch("snapper.server.app.get_repository")
    def test_get_candles_success(
        self, mock_get_repo: MagicMock, mock_get_settings: MagicMock
    ) -> None:
        """Test candles endpoint returns OHLCV data.

        Given: A valid instrument with candle data in repository,
        When: GET /api/candles is called with parameters,
        Then: Response contains candle data array with OHLCV values.
        """
        mock_settings = MagicMock()
        mock_settings.db_url = "sqlite:///test.db"
        mock_get_settings.return_value = mock_settings
        mock_repo = MagicMock()
        mock_session = MagicMock()
        mock_session_context = AsyncMock()
        mock_session_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session_context.__aexit__ = AsyncMock(return_value=None)
        mock_repo.session.return_value = mock_session_context
        mock_get_repo.return_value = mock_repo
        mock_instrument = MagicMock(spec=Instrument)
        mock_instrument.id = 1
        mock_instrument.symbol = "BTC-USD"
        mock_inst_result = MagicMock()
        mock_inst_result.scalars.return_value.first.return_value = mock_instrument
        mock_candle = MagicMock(spec=Candle)
        mock_candle.timeframe = "1h"
        mock_candle.timestamp = datetime(2023, 1, 1, 12, 0)
        mock_candle.open = 50000.0
        mock_candle.high = 51000.0
        mock_candle.low = 49000.0
        mock_candle.close = 50500.0
        mock_candle.volume = 1000.0
        mock_candle.vwap = 50250.0
        mock_candle.trades = 10
        mock_candles_result = MagicMock()
        mock_candles_result.scalars.return_value.all.return_value = [mock_candle]

        async def mock_execute(query: Any) -> Any:
            if "Instrument" in str(query):
                return mock_inst_result
            else:
                return mock_candles_result

        mock_session.execute = mock_execute
        response = self.client.get(
            "/api/candles?instrument=BTC-USD&exchange=kraken&timeframe=1h&limit=10"
        )
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["instrument"] == "BTC-USD"
        assert data[0]["timeframe"] == "1h"
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["instrument"] == "BTC-USD"
        assert data[0]["open"] == pytest.approx(50000.0)
        assert data[0]["close"] == pytest.approx(50500.0)

    @patch("snapper.server.app.get_settings")
    @patch("snapper.server.app.get_repository")
    def test_get_candles_no_data_returns_empty_array(
        self, mock_get_repo: MagicMock, mock_get_settings: MagicMock
    ) -> None:
        """Test candles endpoint returns empty array when no data.

        Given: A valid instrument with no candle data,
        When: GET /api/candles is called,
        Then: Response contains an empty array.
        """
        mock_settings = MagicMock()
        mock_settings.db_url = "sqlite:///test.db"
        mock_get_settings.return_value = mock_settings
        mock_repo = MagicMock()
        mock_get_repo.return_value = mock_repo
        mock_session = MagicMock()
        mock_session_context = AsyncMock()
        mock_session_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session_context.__aexit__ = AsyncMock(return_value=None)
        mock_repo.session.return_value = mock_session_context
        mock_instrument = MagicMock(spec=Instrument)
        mock_instrument.id = 1
        mock_instrument.symbol = "BTC-USD"
        mock_inst_result = MagicMock()
        mock_inst_result.scalars.return_value.first.return_value = mock_instrument
        mock_candles_result = MagicMock()
        mock_candles_result.scalars.return_value.all.return_value = []

        async def mock_execute(query: Any) -> Any:
            if "Instrument" in str(query):
                return mock_inst_result
            else:
                return mock_candles_result

        mock_session.execute = mock_execute
        response = self.client.get(
            "/api/candles?instrument=BTC-USD&exchange=kraken&timeframe=1h&limit=10"
        )
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 0

    @patch("snapper.server.app.get_settings")
    @patch("snapper.server.app.get_repository")
    def test_get_candles_instrument_not_found_returns_204(
        self, mock_get_repo: MagicMock, mock_get_settings: MagicMock
    ) -> None:
        """Test candles endpoint returns 204 for unknown instrument.

        Given: An instrument that does not exist in the database,
        When: GET /api/candles is called,
        Then: Response status is 204 No Content.
        """
        mock_settings = MagicMock()
        mock_settings.db_url = "sqlite:///test.db"
        mock_get_settings.return_value = mock_settings
        mock_repo = MagicMock()
        mock_session = MagicMock()
        mock_session_context = AsyncMock()
        mock_session_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session_context.__aexit__ = AsyncMock(return_value=None)
        mock_repo.session.return_value = mock_session_context
        mock_get_repo.return_value = mock_repo
        mock_inst_result = MagicMock()
        mock_inst_result.scalars.return_value.first.return_value = None

        async def mock_execute(query: Any) -> Any:
            return mock_inst_result

        mock_session.execute = mock_execute
        response = self.client.get("/api/candles?instrument=INVALID&exchange=kraken&timeframe=1h")
        assert response.status_code == 204
        assert response.content == b""

    def test_get_system_status_success(self) -> None:
        """Test system status returns running processes info.

        Given: Process factory with running strategy processes,
        When: GET /api/status is called,
        Then: Response contains trader status and strategies array.
        """
        mock_process_factory = MagicMock()
        mock_strategy_process = MagicMock()
        mock_strategy_process.get_status = MagicMock(
            return_value={
                "strategy_name": "macd_btc_1h",
                "status": "running",
                "signals_generated": 42,
            }
        )
        mock_other_process = MagicMock()
        mock_other_process.get_status = MagicMock(
            return_value={
                "status": "running",
                "messages_sent": 100,
            }
        )
        mock_no_status_process = MagicMock(spec=[])
        mock_process_factory.started_processes = {
            "strategy_macd_btc_1h": mock_strategy_process,
            "zmq_broker": mock_other_process,
            "executor": mock_no_status_process,
        }
        self.app.state.process_factory = mock_process_factory
        response = self.client.get("/api/status")
        assert response.status_code == 200
        data = response.json()
        assert "trader" in data
        assert data["trader"]["status"] == "not_running"
        assert "backtests" in data
        assert data["backtests"] == {}
        assert "strategies" in data
        assert len(data["strategies"]) == 1
        assert data["strategies"][0]["strategy_name"] == "macd_btc_1h"
        assert data["strategies"][0]["status"] == "running"
        assert data["strategies"][0]["signals_generated"] == 42


class TestWebSocketEndpoints:
    """Tests for WebSocket-related API endpoints."""

    def setup_method(self) -> None:
        """Initialize test client with dependency overrides."""
        self.app = create_app()

        def skip_csrf_validation() -> None:
            return None

        def skip_authentication() -> UserProfile:
            return UserProfile(id="test_id", username="test_user", role=UserRole.ADMIN)

        self.app.dependency_overrides[validate_csrf_token] = skip_csrf_validation
        self.app.dependency_overrides[require_authentication] = skip_authentication
        self.client = TestClient(self.app)

    def test_zmq_health_check_success(self) -> None:
        """Test ZMQ health check returns healthy status.

        Given: A configured WebSocket manager with ZMQ bridge,
        When: GET /api/zmq/health is called,
        Then: Response indicates healthy status and ok components.
        """
        response = self.client.get("/api/zmq/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "timestamp" in data
        assert data["components"]["websocket_manager"] == "ok"


class TestStaticFileServing:
    """Tests for static file serving behavior."""

    def test_root_serves_static_files_when_dist_exists(self, tmp_path: Path) -> None:
        """Test root path serves static files when frontend/dist exists.

        Given: The FastAPI application with frontend/dist directory,
        When: GET / is called,
        Then: Response serves index.html from static files.
        """
        dist_dir = tmp_path / "frontend" / "dist"
        dist_dir.mkdir(parents=True)
        index_html = dist_dir / "index.html"
        index_html.write_text("<html><body>Snapper UI</body></html>")

        with (
            patch("os.path.exists", return_value=True),
            patch("snapper.server.app.StaticFiles") as mock_static,
        ):
            create_app()
            assert mock_static.called


class TestMainAppIntegration:
    """Integration tests for main application endpoints."""

    def setup_method(self) -> None:
        """Initialize test client with application instance."""
        self.app = create_app()
        self.client = TestClient(self.app)

    @patch("snapper.server.app.get_settings")
    @patch("snapper.server.app.get_repository")
    def test_app_creation_and_basic_endpoints(
        self, mock_get_repo: MagicMock, mock_get_settings: MagicMock
    ) -> None:
        """Test app creation and basic endpoint accessibility.

        Given: Mocked settings and repository,
        When: Health endpoint is accessed,
        Then: Health returns 200.
        """
        mock_settings = MagicMock()
        mock_get_settings.return_value = mock_settings
        mock_repo = MagicMock()
        mock_get_repo.return_value = mock_repo
        response = self.client.get("/api/health")
        assert response.status_code == 200


class TestDependencyFunctions:
    """Tests for FastAPI dependency injection functions."""

    @patch("snapper.server.app.get_repository")
    def test_get_repository_dependency(self, mock_get_repo: MagicMock) -> None:
        """Test repository dependency injection function.

        Given: A mocked repository factory,
        When: get_repository_dependency is called,
        Then: The repository instance is returned.
        """
        mock_repo = MagicMock()
        mock_get_repo.return_value = mock_repo
        result = get_repository_dependency()
        assert result == mock_repo
        mock_get_repo.assert_called_once()

    @patch("snapper.server.app.get_settings")
    def test_get_settings_dependency(self, mock_get_settings: MagicMock) -> None:
        """Test settings dependency injection function.

        Given: A mocked settings factory,
        When: get_settings_dependency is called,
        Then: The settings instance is returned.
        """
        mock_settings = MagicMock()
        mock_get_settings.return_value = mock_settings
        result = get_settings_dependency()
        assert result == mock_settings
        mock_get_settings.assert_called_once()


class TestApiEndpointsEnhanced:
    """Tests for enhanced API endpoint behaviors."""

    def setup_method(self) -> None:
        """Initialize test client with CSRF validation bypassed."""
        self.app = create_app()

        def skip_csrf_validation() -> None:
            return None

        self.app.dependency_overrides[validate_csrf_token] = skip_csrf_validation
        self.client = TestClient(self.app)

    def test_missing_endpoint_returns_404(self) -> None:
        """Test nonexistent endpoint returns 404.

        Given: The API router,
        When: A nonexistent endpoint is requested,
        Then: Response status is 404 Not Found.
        """
        response = self.client.get("/api/nonexistent")
        assert response.status_code == 404

    def test_invalid_method_returns_405(self) -> None:
        """Test invalid HTTP method returns 405.

        Given: The health endpoint expecting GET,
        When: PATCH method is used,
        Then: Response status is 405 Method Not Allowed.
        """
        response = self.client.patch("/api/health")
        assert response.status_code == 405


class MockRepository:
    """Mock repository for database session simulation."""

    def __init__(self, session_result: Any = None, error: Exception | None = None) -> None:
        """Initialize the instance."""
        self._session_result = session_result
        self._error = error

    def session(self) -> MockSession:
        """Return mock session with configured result or error."""
        return MockSession(self._session_result, self._error)


class MockSession:
    """Mock database session for async context management."""

    def __init__(self, result: Any = None, error: Exception | None = None) -> None:
        """Initialize the instance."""
        self._result = result
        self._error = error

    async def __aenter__(self) -> MockSession:
        """Magic method."""
        if self._error:
            raise self._error
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Magic method."""
        pass

    async def execute(self, query: Any) -> MockResult:
        """Execute mock query and return configured result."""
        if self._error:
            raise self._error
        return MockResult(self._result)


class MockResult:
    """Mock query result for database operations."""

    def __init__(self, data: Any = None) -> None:
        """Initialize the instance."""
        self._data = data or []

    def all(self) -> list[Any]:
        """Return all result data as list."""
        return self._data

    def scalars(self) -> MockResult:
        """Return self for scalar result chaining."""
        return self

    def first(self) -> Any:
        """Return first element or None if empty."""
        return self._data[0] if self._data else None


def create_test_client() -> TestClient:
    """Create a test client with CSRF and auth bypassed."""
    app = create_app()

    def skip_csrf_validation() -> None:
        return None

    def skip_authentication() -> UserProfile:
        return UserProfile(id="test_id", username="test_user", role=UserRole.ADMIN)

    app.dependency_overrides[validate_csrf_token] = skip_csrf_validation
    app.dependency_overrides[require_authentication] = skip_authentication
    return TestClient(app)


class TestLifespanCancellation:
    """Tests for lifespan cancellation and error handling."""

    @pytest.mark.asyncio
    async def test_lifespan_handles_cancellation(self) -> None:
        """Test lifespan handles CancelledError gracefully.

        Given: A running lifespan context,
        When: CancelledError is raised,
        Then: CancelledError propagates after cleanup completes.
        """
        mock_app = MagicMock()
        mock_manager = MagicMock()
        mock_manager.cleanup = AsyncMock()
        mock_zmq_bridge = MagicMock()
        mock_zmq_bridge.start = AsyncMock()
        mock_zmq_bridge.stop = AsyncMock()
        mock_manager.zmq_bridge = mock_zmq_bridge
        mock_bridge_task = MagicMock()
        mock_bridge_task.cancel = MagicMock()
        mock_bridge_task.cancelled.return_value = False
        mock_app.state.manager = mock_manager
        mock_app.state.zmq_bridge_task = mock_bridge_task
        with (
            patch("snapper.server.app.discover_processes"),
            patch("snapper.server.app.ProcessLauncherService") as mock_factory_cls,
            patch("snapper.server.app.get_settings_service") as mock_get_settings_service,
        ):
            mock_settings_service = MagicMock()
            mock_settings_service.shutdown = AsyncMock()
            mock_get_settings_service.return_value = mock_settings_service
            mock_factory = MagicMock()
            mock_factory.sync_registry_to_database = AsyncMock()
            mock_factory.start_all_processes = AsyncMock()
            mock_factory.stop_all_processes = AsyncMock()
            mock_factory_cls.return_value = mock_factory
            with pytest.raises(asyncio.CancelledError):
                async with lifespan(mock_app):
                    raise asyncio.CancelledError()
            mock_factory.stop_all_processes.assert_awaited_once()
            mock_manager.cleanup.assert_awaited()

    @pytest.mark.asyncio
    async def test_lifespan_logs_warning_when_zmq_bridge_task_fails(self) -> None:
        """Test warning is logged when ZMQ bridge task fails.

        Given: A ZMQ bridge that raises an error during start,
        When: The lifespan context runs,
        Then: A warning is logged about the bridge failure.
        """
        mock_app = MagicMock()
        mock_manager = MagicMock()
        mock_manager.cleanup = AsyncMock()
        mock_zmq_bridge = MagicMock()
        started_event = asyncio.Event()

        async def failing_bridge_start() -> None:
            started_event.set()
            await asyncio.sleep(0)
            raise RuntimeError("ZMQ bridge error")

        mock_zmq_bridge.start = failing_bridge_start
        mock_zmq_bridge.stop = AsyncMock()
        mock_manager.zmq_bridge = mock_zmq_bridge
        mock_app.state.manager = mock_manager
        with (
            patch("snapper.server.app.discover_processes"),
            patch("snapper.server.app.ProcessLauncherService") as mock_factory_cls,
            patch("snapper.server.app.get_settings_service") as mock_get_settings_service,
            patch("snapper.server.app.logger") as mock_logger,
        ):
            mock_settings_service = MagicMock()
            mock_settings_service.shutdown = AsyncMock()
            mock_get_settings_service.return_value = mock_settings_service
            mock_factory = MagicMock()
            mock_factory.sync_registry_to_database = AsyncMock()
            mock_factory.start_all_processes = AsyncMock()
            mock_factory.stop_all_processes = AsyncMock()
            mock_factory_cls.return_value = mock_factory
            async with lifespan(mock_app):
                await started_event.wait()
            mock_factory.stop_all_processes.assert_awaited_once()
            warning_calls = [
                call
                for call in mock_logger.warning.call_args_list
                if "ZMQ bridge task failed" in str(call)
            ]
            assert len(warning_calls) == 1

    @pytest.mark.asyncio
    async def test_lifespan_handles_zmq_bridge_task_cancellation(self) -> None:
        """Verify lifespan handles ZMQ bridge task cancellation.

        Given: A ZMQ bridge that raises CancelledError,
        When: The lifespan context runs,
        Then: Cleanup completes and shutdown proceeds normally.
        """
        mock_app = MagicMock()
        mock_manager = MagicMock()
        mock_manager.cleanup = AsyncMock()
        mock_zmq_bridge = MagicMock()
        started_event = asyncio.Event()

        async def cancelled_bridge_start() -> None:
            started_event.set()
            await asyncio.sleep(0)
            raise asyncio.CancelledError()

        mock_zmq_bridge.start = cancelled_bridge_start
        mock_zmq_bridge.stop = AsyncMock()
        mock_manager.zmq_bridge = mock_zmq_bridge
        mock_app.state.manager = mock_manager
        with (
            patch("snapper.server.app.discover_processes"),
            patch("snapper.server.app.ProcessLauncherService") as mock_factory_cls,
            patch("snapper.server.app.get_settings_service") as mock_get_settings_service,
        ):
            mock_settings_service = MagicMock()
            mock_settings_service.shutdown = AsyncMock()
            mock_get_settings_service.return_value = mock_settings_service
            mock_factory = MagicMock()
            mock_factory.sync_registry_to_database = AsyncMock()
            mock_factory.start_all_processes = AsyncMock()
            mock_factory.stop_all_processes = AsyncMock()
            mock_factory_cls.return_value = mock_factory
            async with lifespan(mock_app):
                await started_event.wait()
            mock_factory.stop_all_processes.assert_awaited_once()


class TestOrdersEndpointWithErrors:
    """Tests for orders endpoint error handling."""

    def test_get_orders_handles_database_error(self) -> None:
        """Verify orders endpoint returns 500 on database error.

        Given: A repository that raises database exception,
        When: GET /orders is called,
        Then: Response is 500 with error detail.
        """
        app = create_app()

        def skip_csrf_validation() -> None:
            return None

        def skip_authentication() -> UserProfile:
            return UserProfile(id="test_id", username="test_user", role=UserRole.ADMIN)

        def get_error_repo() -> MockRepository:
            return MockRepository(error=Exception("Database connection failed"))

        app.dependency_overrides[validate_csrf_token] = skip_csrf_validation
        app.dependency_overrides[require_authentication] = skip_authentication
        app.dependency_overrides[get_repository_dependency] = get_error_repo
        client = TestClient(app)
        response = client.get("/api/orders")
        assert response.status_code == 500
        assert "Failed to fetch orders" in response.json()["detail"]


class TestSignalsEndpointWithErrors:
    """Tests for signals endpoint error handling."""

    def test_get_signals_handles_database_error(self) -> None:
        """Verify signals endpoint returns 500 on database error.

        Given: A repository that raises query exception,
        When: GET /signals is called,
        Then: Response is 500 with error detail.
        """
        app = create_app()

        def skip_csrf_validation() -> None:
            return None

        def skip_authentication() -> UserProfile:
            return UserProfile(id="test_id", username="test_user", role=UserRole.ADMIN)

        def get_error_repo() -> MockRepository:
            return MockRepository(error=Exception("Signal query failed"))

        app.dependency_overrides[validate_csrf_token] = skip_csrf_validation
        app.dependency_overrides[require_authentication] = skip_authentication
        app.dependency_overrides[get_repository_dependency] = get_error_repo
        client = TestClient(app)
        response = client.get("/api/signals")
        assert response.status_code == 500
        assert "Failed to fetch signals" in response.json()["detail"]


class TestExecutionsEndpointWithErrors:
    """Tests for executions endpoint error handling."""

    def test_get_executions_handles_database_error(self) -> None:
        """Verify executions endpoint returns 500 on database error.

        Given: A repository that raises query exception,
        When: GET /executions is called,
        Then: Response is 500 with error detail.
        """
        app = create_app()

        def skip_csrf_validation() -> None:
            return None

        def skip_authentication() -> UserProfile:
            return UserProfile(id="test_id", username="test_user", role=UserRole.ADMIN)

        def get_error_repo() -> MockRepository:
            return MockRepository(error=Exception("Execution query failed"))

        app.dependency_overrides[validate_csrf_token] = skip_csrf_validation
        app.dependency_overrides[require_authentication] = skip_authentication
        app.dependency_overrides[get_repository_dependency] = get_error_repo
        client = TestClient(app)
        response = client.get("/api/executions")
        assert response.status_code == 500
        assert "Failed to fetch executions" in response.json()["detail"]


class TestPositionsEndpointWithErrors:
    """Tests for positions endpoint error handling."""

    def test_get_positions_handles_database_error(self) -> None:
        """Verify positions endpoint returns 500 on database error.

        Given: A repository that raises query exception,
        When: GET /positions is called,
        Then: Response is 500 with error detail.
        """
        app = create_app()

        def skip_csrf_validation() -> None:
            return None

        def skip_authentication() -> UserProfile:
            return UserProfile(id="test_id", username="test_user", role=UserRole.ADMIN)

        def get_error_repo() -> MockRepository:
            return MockRepository(error=Exception("Position query failed"))

        app.dependency_overrides[validate_csrf_token] = skip_csrf_validation
        app.dependency_overrides[require_authentication] = skip_authentication
        app.dependency_overrides[get_repository_dependency] = get_error_repo
        client = TestClient(app)
        response = client.get("/api/positions")
        assert response.status_code == 500
        assert "Failed to fetch positions" in response.json()["detail"]


class TestCandlesEndpointWithErrors:
    """Tests for candles endpoint error handling."""

    def test_get_candles_handles_database_error(self) -> None:
        """Verify candles endpoint returns 500 on database error.

        Given: A repository that raises query exception,
        When: GET /candles is called,
        Then: Response is 500 with error detail.
        """
        app = create_app()

        def skip_csrf_validation() -> None:
            return None

        def skip_authentication() -> UserProfile:
            return UserProfile(id="test_id", username="test_user", role=UserRole.ADMIN)

        app.dependency_overrides[validate_csrf_token] = skip_csrf_validation
        app.dependency_overrides[require_authentication] = skip_authentication
        client = TestClient(app)
        with patch("snapper.server.app.get_repository") as mock_get_repo:
            mock_repo = MagicMock()
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(side_effect=Exception("Candle query failed"))
            mock_session.__aexit__ = AsyncMock()
            mock_repo.session.return_value = mock_session
            mock_get_repo.return_value = mock_repo
            response = client.get("/api/candles?instrument=BTC-USD&exchange=kraken&timeframe=1h")
            assert response.status_code == 500
            assert "Failed to fetch candle data" in response.json()["detail"]


class TestZmqHealthCheckErrors:
    """Tests for ZMQ health check error scenarios."""

    def test_zmq_health_check_with_none_context(self) -> None:
        """Verify health check handles None ZMQ context.

        Given: ZMQ bridge with None context,
        When: GET /zmq/health is called,
        Then: Response indicates status gracefully.
        """
        app = create_app()

        def skip_csrf_validation() -> None:
            return None

        def skip_authentication() -> UserProfile:
            return UserProfile(id="test_id", username="test_user", role=UserRole.ADMIN)

        app.dependency_overrides[validate_csrf_token] = skip_csrf_validation
        app.dependency_overrides[require_authentication] = skip_authentication
        client = TestClient(app)
        original_context = app.state.manager.zmq_bridge.context
        app.state.manager.zmq_bridge.context = None
        try:
            response = client.get("/api/zmq/health")
            assert response.status_code == 200
            data = response.json()
            assert "status" in data
        finally:
            app.state.manager.zmq_bridge.context = original_context

    def test_zmq_health_check_with_active_context(self) -> None:
        """Verify health check with active ZMQ context.

        Given: ZMQ bridge with active context,
        When: GET /zmq/health is called,
        Then: Response is 200 and socket is closed.
        """
        client = create_test_client()
        app = cast(FastAPI, client.app)
        manager = app.state.manager
        context_mock = MagicMock()
        test_socket = MagicMock()
        context_mock.socket.return_value = test_socket
        manager.zmq_bridge.context = context_mock
        manager.zmq_bridge.available_topics = {"t": "topic"}
        manager.get_stats = MagicMock(
            return_value=WsStatsSnapshot(connections=ConnectionStats(), topics={})
        )
        response = client.get("/api/zmq/health")
        assert response.status_code == 200
        test_socket.close.assert_called_once()


class TestSystemStatusEdgeCases:
    """Tests for system status endpoint edge cases."""

    def test_system_status_returns_valid_response(self) -> None:
        """Verify status returns valid response structure.

        Given: Application with process factory,
        When: GET /status is called,
        Then: Response contains trader and strategies keys.
        """
        app = create_app()

        def skip_csrf_validation() -> None:
            return None

        def skip_authentication() -> UserProfile:
            return UserProfile(id="test_id", username="test_user", role=UserRole.ADMIN)

        app.dependency_overrides[validate_csrf_token] = skip_csrf_validation
        app.dependency_overrides[require_authentication] = skip_authentication
        mock_factory = MagicMock()
        mock_factory.started_processes = {}
        app.state.process_factory = mock_factory
        client = TestClient(app)
        response = client.get("/api/status")
        assert response.status_code == 200
        data = response.json()
        assert "trader" in data
        assert data["trader"]["status"] == "not_running"
        assert "strategies" in data

    def test_system_status_handles_process_error(self) -> None:
        """Verify status handles process status error gracefully.

        Given: A process that raises exception on get_status,
        When: GET /status is called,
        Then: Response is 200 with partial status data.
        """
        app = create_app()

        def skip_csrf_validation() -> None:
            return None

        def skip_authentication() -> UserProfile:
            return UserProfile(id="test_id", username="test_user", role=UserRole.ADMIN)

        app.dependency_overrides[validate_csrf_token] = skip_csrf_validation
        app.dependency_overrides[require_authentication] = skip_authentication
        mock_process = MagicMock()
        mock_process.name = "test_strategy"
        mock_process.get_status = MagicMock(side_effect=Exception("Status error"))
        mock_factory = MagicMock()
        mock_factory.started_processes = {"test_strategy": mock_process}
        app.state.process_factory = mock_factory
        client = TestClient(app)
        response = client.get("/api/status")
        assert response.status_code == 200
        data = response.json()
        assert "trader" in data
        assert "strategies" in data

    def test_system_status_trader_running_when_coordinator_started(self) -> None:
        """Verify trader status reflects trader_coordinator process state.

        Given: trader_coordinator process is in started_processes,
        When: GET /status is called,
        Then: trader.status is 'running'.
        """
        app = create_app()

        def skip_csrf_validation() -> None:
            return None

        def skip_authentication() -> UserProfile:
            return UserProfile(id="test_id", username="test_user", role=UserRole.ADMIN)

        app.dependency_overrides[validate_csrf_token] = skip_csrf_validation
        app.dependency_overrides[require_authentication] = skip_authentication
        mock_factory = MagicMock()
        mock_factory.started_processes = {"trader_coordinator": MagicMock()}
        app.state.process_factory = mock_factory
        client = TestClient(app)
        response = client.get("/api/status")
        assert response.status_code == 200
        data = response.json()
        assert data["trader"]["status"] == "running"

    def test_system_status_trader_not_running_when_coordinator_absent(self) -> None:
        """Verify trader status is not_running when coordinator is absent.

        Given: trader_coordinator is NOT in started_processes,
        When: GET /status is called,
        Then: trader.status is 'not_running'.
        """
        app = create_app()

        def skip_csrf_validation() -> None:
            return None

        def skip_authentication() -> UserProfile:
            return UserProfile(id="test_id", username="test_user", role=UserRole.ADMIN)

        app.dependency_overrides[validate_csrf_token] = skip_csrf_validation
        app.dependency_overrides[require_authentication] = skip_authentication
        mock_factory = MagicMock()
        mock_factory.started_processes = {"some_other_process": MagicMock()}
        app.state.process_factory = mock_factory
        client = TestClient(app)
        response = client.get("/api/status")
        assert response.status_code == 200
        data = response.json()
        assert data["trader"]["status"] == "not_running"


class TestAppCoverageImprovement:
    """Tests for improving application code coverage."""

    def setup_method(self) -> None:
        """Initialize test client with dependency overrides."""
        self.app = create_app()
        self.client = TestClient(self.app)

        def skip_csrf_validation() -> None:
            return None

        def skip_authentication() -> UserProfile:
            return UserProfile(id="test_id", username="test_user", role=UserRole.ADMIN)

        self.app.dependency_overrides[validate_csrf_token] = skip_csrf_validation
        self.app.dependency_overrides[require_authentication] = skip_authentication

    def teardown_method(self) -> None:
        """Clean up test client resources."""
        with contextlib.suppress(Exception):
            self.client.close()

    def test_websocket_stats_endpoint(self) -> None:
        """Verify WebSocket stats endpoint returns connection data.

        Given: Application with WebSocket manager,
        When: GET /ws/stats is called,
        Then: Response contains websocket and zmq_bridge keys.
        """
        response = self.client.get("/api/ws/stats")
        assert response.status_code == 200
        data = response.json()
        assert "websocket" in data
        assert "zmq_bridge" in data
        assert "config" in data

    def test_zmq_health_check_success(self) -> None:
        """Verify ZMQ health check returns healthy status.

        Given: Application with ZMQ bridge,
        When: GET /zmq/health is called,
        Then: Response indicates healthy with ok components.
        """
        response = self.client.get("/api/zmq/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["components"]["zmq_context"] == "ok"

    def test_zmq_health_check_error(self) -> None:
        """Verify ZMQ health check includes error information.

        Given: Application with ZMQ bridge,
        When: GET /zmq/health is called,
        Then: Response contains components and errors keys.
        """
        response = self.client.get("/api/zmq/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "components" in data
        assert "errors" in data

    def test_health_endpoint(self) -> None:
        """Verify health endpoint returns healthy status.

        Given: Running application,
        When: GET /health is called,
        Then: Response is 200 with healthy status and timestamp.
        """
        response = self.client.get("/api/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "timestamp" in data

    def test_root_dashboard_endpoint(self) -> None:
        """Verify root endpoint returns HTML dashboard.

        Given: Running application with static files,
        When: GET / is called,
        Then: Response is 200 with HTML content type.
        """
        response = self.client.get("/")
        assert response.status_code == 200
        assert "text/html" in response.headers.get("content-type", "")

    def test_static_files_mounting(self) -> None:
        """Verify static files are mounted correctly.

        Given: Application with static files directory,
        When: App is created and health endpoint accessed,
        Then: Application is valid and responds correctly.
        """
        app = create_app()
        assert app is not None
        test_client = TestClient(app)
        response = test_client.get("/api/health")
        assert response.status_code == 200
        test_client.close()

    def test_additional_utils_coverage(self) -> None:
        """Verify logging setup with file path works correctly.

        Given: A temporary log file path,
        When: setup_logging is called with logfile,
        Then: Logging is configured without error.
        """
        with tempfile.NamedTemporaryFile(suffix=".log", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            setup_logging(logfile=tmp_path)
        finally:
            with contextlib.suppress(OSError, FileNotFoundError):
                os.unlink(tmp_path)


class MockInstrument:
    """Mock instrument for testing endpoint responses."""

    def __init__(self, inst_id: int = 1, symbol: str = "BTC-USD", exchange: str = "kraken") -> None:
        """Initialize the instance."""
        self.id = inst_id
        self.symbol = symbol
        self.exchange = exchange


class MockOrderRecord:
    """Mock order record for testing orders endpoint."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.id = 1
        self.instrument_id = 1
        self.client_order_id = "client_123"
        self.exchange_order_id = "exch_456"
        self.created_at = dt.datetime(2024, 1, 1, 12, 0, tzinfo=dt.UTC)
        self.updated_at = dt.datetime(2024, 1, 1, 12, 5, tzinfo=dt.UTC)
        self.side = "buy"
        self.type = "limit"
        self.price = 50000.0
        self.size = 1.0
        self.status = "filled"
        self.time_in_force = "GTC"
        self.error = None


class MockSignalEvent:
    """Mock signal event for testing signals endpoint."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.id = 1
        self.instrument_id = 1
        self.timestamp = dt.datetime(2024, 1, 1, 12, 0, tzinfo=dt.UTC)
        self.side = "buy"
        self.strength = 0.8
        self.reason = "RSI oversold"
        self.strategy_name = "rsi_strategy"
        self.price = 49500.0


class MockExecution:
    """Mock execution for testing executions endpoint."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.id = 1
        self.order_id = 1
        self.timestamp = dt.datetime(2024, 1, 1, 12, 1, tzinfo=dt.UTC)
        self.price = 50000.0
        self.size = 1.0
        self.fee = 10.0
        self.fee_asset = "USD"


class MockPosition:
    """Mock position for testing positions endpoint."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.id = 1
        self.instrument_id = 1
        self.quantity = 1.5
        self.average_price = 48000.0
        self.unrealized_pnl = 3000.0
        self.realized_pnl = 500.0
        self.updated_at = dt.datetime(2024, 1, 1, 12, 0, tzinfo=dt.UTC)


class MockRepositoryV2:
    """Mock repository version 2 for session simulation."""

    def __init__(self, session_result: Any = None) -> None:
        """Initialize the instance."""
        self._session_result = session_result

    def session(self) -> MockSession:
        """Return mock session with configured result."""
        return MockSession(self._session_result)


class MockSessionV2:
    """Mock database session version 2 for async operations."""

    def __init__(self, result: Any = None) -> None:
        """Initialize the instance."""
        self._result = result

    async def __aenter__(self) -> MockSessionV2:
        """Magic method."""
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Magic method."""
        pass

    async def execute(self, query: Any) -> MockResult:
        """Execute mock query and return configured result."""
        return MockResult(self._result)


class MockResultV2:
    """Mock query result version 2 for database operations."""

    def __init__(self, data: Any = None) -> None:
        """Initialize the instance."""
        self._data = data or []

    def all(self) -> list[Any]:
        """Return all result data as list."""
        return self._data

    def scalars(self) -> MockResultV2:
        """Return self for scalar result chaining."""
        return self

    def first(self) -> Any:
        """Return first element or None if empty."""
        return self._data[0] if self._data else None


def create_app_with_overrides(repo: MockRepository | None = None) -> TestClient:
    """Create a test client with optional repository override."""
    app = create_app()

    def skip_csrf_validation() -> None:
        return None

    def skip_authentication() -> UserProfile:
        return UserProfile(id="test_id", username="test_user", role=UserRole.ADMIN)

    app.dependency_overrides[validate_csrf_token] = skip_csrf_validation
    app.dependency_overrides[require_authentication] = skip_authentication
    if repo:
        app.dependency_overrides[get_repository_dependency] = lambda: repo
    return TestClient(app)


class TestOrdersSuccessPath:
    """Tests for orders endpoint success scenarios."""

    def test_get_orders_returns_data(self) -> None:
        """Verify orders endpoint returns order data.

        Given: Repository with order and instrument records,
        When: GET /orders is called,
        Then: Response contains order data with instrument symbol.
        """
        order = MockOrderRecord()
        instrument = MockInstrument()
        repo = MockRepository(session_result=[(order, instrument)])
        client = create_app_with_overrides(repo)
        response = client.get("/api/orders")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["id"] == 1
        assert data[0]["instrument"] == "BTC-USD"
        assert data[0]["exchange"] == "kraken"
        assert data[0]["side"] == "buy"
        assert data[0]["status"] == "filled"

    def test_get_orders_with_symbol_filter(self) -> None:
        """Verify orders endpoint filters by symbol.

        Given: Repository with order for specific instrument,
        When: GET /orders is called with symbol filter,
        Then: Response contains only matching orders.
        """
        order = MockOrderRecord()
        instrument = MockInstrument(symbol="ETH-USD")
        repo = MockRepository(session_result=[(order, instrument)])
        client = create_app_with_overrides(repo)
        response = client.get("/api/orders?symbol=ETH-USD")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["instrument"] == "ETH-USD"

    def test_get_orders_empty(self) -> None:
        """Verify orders endpoint returns empty list when no data.

        Given: Repository with no order records,
        When: GET /orders is called,
        Then: Response is 200 with empty list.
        """
        repo = MockRepository(session_result=[])
        client = create_app_with_overrides(repo)
        response = client.get("/api/orders")
        assert response.status_code == 200
        data = response.json()
        assert data == []


class TestSignalsSuccessPath:
    """Tests for signals endpoint success scenarios."""

    def test_get_signals_returns_data(self) -> None:
        """Verify signals endpoint returns signal data.

        Given: Repository with signal and instrument records,
        When: GET /signals is called,
        Then: Response contains signal data with strategy name.
        """
        signal = MockSignalEvent()
        instrument = MockInstrument()
        repo = MockRepository(session_result=[(signal, instrument)])
        client = create_app_with_overrides(repo)
        response = client.get("/api/signals")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["id"] == 1
        assert data[0]["instrument"] == "BTC-USD"
        assert data[0]["side"] == "buy"
        assert data[0]["strength"] == pytest.approx(0.8)
        assert data[0]["reason"] == "RSI oversold"
        assert data[0]["strategy_name"] == "rsi_strategy"

    def test_get_signals_with_filters(self) -> None:
        """Verify signals endpoint filters by instrument and strategy.

        Given: Repository with signal records,
        When: GET /signals is called with filters,
        Then: Response contains only matching signals.
        """
        signal = MockSignalEvent()
        instrument = MockInstrument()
        repo = MockRepository(session_result=[(signal, instrument)])
        client = create_app_with_overrides(repo)
        response = client.get("/api/signals?instrument=BTC-USD&strategy=rsi_strategy")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1

    def test_get_signals_empty(self) -> None:
        """Verify signals endpoint returns empty list when no data.

        Given: Repository with no signal records,
        When: GET /signals is called,
        Then: Response is 200 with empty list.
        """
        repo = MockRepository(session_result=[])
        client = create_app_with_overrides(repo)
        response = client.get("/api/signals")
        assert response.status_code == 200
        data = response.json()
        assert data == []


class TestExecutionsSuccessPath:
    """Tests for executions endpoint success scenarios."""

    def test_get_executions_returns_data(self) -> None:
        """Verify executions endpoint returns execution data.

        Given: Repository with execution and order records,
        When: GET /executions is called,
        Then: Response contains execution data with fee info.
        """
        execution = MockExecution()
        order = MockOrderRecord()
        instrument = MockInstrument()
        repo = MockRepository(session_result=[(execution, order, instrument)])
        client = create_app_with_overrides(repo)
        response = client.get("/api/executions")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["id"] == 1
        assert data[0]["order_id"] == 1
        assert data[0]["price"] == pytest.approx(50000.0)
        assert data[0]["size"] == pytest.approx(1.0)
        assert data[0]["fee"] == pytest.approx(10.0)
        assert data[0]["fee_asset"] == "USD"

    def test_get_executions_empty(self) -> None:
        """Verify executions endpoint returns empty list when no data.

        Given: Repository with no execution records,
        When: GET /executions is called,
        Then: Response is 200 with empty list.
        """
        repo = MockRepository(session_result=[])
        client = create_app_with_overrides(repo)
        response = client.get("/api/executions")
        assert response.status_code == 200
        data = response.json()
        assert data == []


class TestPositionsSuccessPath:
    """Tests for positions endpoint success scenarios."""

    def test_get_positions_returns_data(self) -> None:
        """Verify positions endpoint returns position data.

        Given: Repository with position and instrument records,
        When: GET /positions is called,
        Then: Response contains position data with PnL.
        """
        position = MockPosition()
        instrument = MockInstrument()
        repo = MockRepository(session_result=[(position, instrument)])
        client = create_app_with_overrides(repo)
        response = client.get("/api/positions")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["id"] == 1
        assert data[0]["instrument"] == "BTC-USD"
        assert data[0]["quantity"] == pytest.approx(1.5)
        assert data[0]["average_price"] == pytest.approx(48000.0)
        assert data[0]["unrealized_pnl"] == pytest.approx(3000.0)
        assert data[0]["realized_pnl"] == pytest.approx(500.0)

    def test_get_positions_empty(self) -> None:
        """Verify positions endpoint returns empty list when no data.

        Given: Repository with no position records,
        When: GET /positions is called,
        Then: Response is 200 with empty list.
        """
        repo = MockRepository(session_result=[])
        client = create_app_with_overrides(repo)
        response = client.get("/api/positions")
        assert response.status_code == 200
        data = response.json()
        assert data == []


class TestZmqHealthCheckContextError:
    """Tests for ZMQ health check context error handling."""

    def test_zmq_health_check_context_socket_error(self) -> None:
        """Verify health check handles ZMQ socket creation error.

        Given: ZMQ context that raises on socket creation,
        When: GET /zmq/health is called,
        Then: Response indicates unhealthy status with error.
        """
        app = create_app()

        def skip_csrf_validation() -> None:
            return None

        def skip_authentication() -> UserProfile:
            return UserProfile(id="test_id", username="test_user", role=UserRole.ADMIN)

        app.dependency_overrides[validate_csrf_token] = skip_csrf_validation
        app.dependency_overrides[require_authentication] = skip_authentication
        mock_context = MagicMock()
        mock_context.socket.side_effect = Exception("ZMQ socket creation failed")
        original_context = app.state.manager.zmq_bridge.context
        app.state.manager.zmq_bridge.context = mock_context
        client = TestClient(app)
        try:
            response = client.get("/api/zmq/health")
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "error"
            assert data["components"]["zmq_context"] == "error"
            assert len(data["errors"]) > 0
            assert "ZMQ context error" in data["errors"][0]
        finally:
            app.state.manager.zmq_bridge.context = original_context


class TestSystemStatusProcessError:
    """Tests for system status process error handling."""

    def test_system_status_logs_warning_on_process_error(self) -> None:
        """Verify warning is logged when process status fails.

        Given: A process that raises exception on get_status,
        When: GET /status is called,
        Then: Warning is logged and response is 200.
        """
        app = create_app()

        def skip_csrf_validation() -> None:
            return None

        def skip_authentication() -> UserProfile:
            return UserProfile(id="test_id", username="test_user", role=UserRole.ADMIN)

        app.dependency_overrides[validate_csrf_token] = skip_csrf_validation
        app.dependency_overrides[require_authentication] = skip_authentication
        mock_process = MagicMock()
        mock_process.name = "failing_strategy"
        mock_process.get_status.side_effect = RuntimeError("Status unavailable")
        mock_factory = MagicMock()
        mock_factory.started_processes = {"failing_strategy": mock_process}
        app.state.process_factory = mock_factory
        client = TestClient(app)
        with patch("snapper.server.app.logger") as mock_logger:
            response = client.get("/api/status")
            assert response.status_code == 200
            data = response.json()
            assert "strategies" in data
            assert "trader" in data
            mock_logger.warning.assert_called()

    def test_system_status_with_valid_process_status(self) -> None:
        """Verify status includes valid process information.

        Given: A process with working get_status method,
        When: GET /status is called,
        Then: Response contains strategy status data.
        """
        app = create_app()

        def skip_csrf_validation() -> None:
            return None

        def skip_authentication() -> UserProfile:
            return UserProfile(id="test_id", username="test_user", role=UserRole.ADMIN)

        app.dependency_overrides[validate_csrf_token] = skip_csrf_validation
        app.dependency_overrides[require_authentication] = skip_authentication
        mock_process = MagicMock()
        mock_process.name = "test_strategy"
        mock_process.get_status = MagicMock(
            return_value={
                "strategy_name": "test_strategy",
                "status": "running",
                "signals_generated": 10,
                "trades_executed": 5,
                "pnl": 100.50,
            }
        )
        mock_factory = MagicMock()
        mock_factory.started_processes = {"test_strategy": mock_process}
        app.state.process_factory = mock_factory
        client = TestClient(app)
        response = client.get("/api/status")
        assert response.status_code == 200
        data = response.json()
        assert len(data["strategies"]) == 1
        assert data["strategies"][0]["strategy_name"] == "test_strategy"
        assert data["strategies"][0]["status"] == "running"


class TestSignalsExchangeFilter:
    """Tests for signals endpoint exchange filtering."""

    def test_get_signals_with_exchange_filter(self) -> None:
        """Verify signals endpoint filters by exchange.

        Given: Repository with signal records for a specific exchange,
        When: GET /signals is called with exchange filter,
        Then: Response contains only matching signals.
        """
        signal = MockSignalEvent()
        instrument = MockInstrument()
        repo = MockRepository(session_result=[(signal, instrument)])
        client = create_app_with_overrides(repo)
        response = client.get("/api/signals?exchange=kraken")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["exchange"] == "kraken"


class TestOrdersExchangeFilter:
    """Tests for orders endpoint exchange filtering."""

    def test_get_orders_with_exchange_filter(self) -> None:
        """Verify orders endpoint filters by exchange.

        Given: Repository with order records for a specific exchange,
        When: GET /orders is called with exchange filter,
        Then: Response contains only matching orders.
        """
        order = MockOrderRecord()
        instrument = MockInstrument()
        repo = MockRepository(session_result=[(order, instrument)])
        client = create_app_with_overrides(repo)
        response = client.get("/api/orders?exchange=kraken")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["exchange"] == "kraken"


class MockSymbolAlias:
    """Mock symbol alias for testing exchange discovery endpoints."""

    def __init__(self, exchange: str = "kraken", native_symbol: str = "BTC-USD") -> None:
        """Initialize the instance."""
        self.id = 1
        self.exchange = exchange
        self.native_symbol = native_symbol
        self.channel = "ws"
        self.exchange_symbol = "XXBTZUSD"


class TestExchangesEndpoint:
    """Tests for exchanges discovery endpoint."""

    def test_get_exchanges_returns_list(self) -> None:
        """Verify exchanges endpoint returns distinct exchange names.

        Given: Repository with symbol alias records,
        When: GET /exchanges is called,
        Then: Response contains distinct exchange names as strings.
        """
        repo = MockRepository(session_result=["kraken", "zonda"])
        client = create_app_with_overrides(repo)
        response = client.get("/api/exchanges")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert "kraken" in data
        assert "zonda" in data

    def test_get_exchanges_empty(self) -> None:
        """Verify exchanges endpoint returns empty list when no data.

        Given: Repository with no symbol alias records,
        When: GET /exchanges is called,
        Then: Response is 200 with empty list.
        """
        repo = MockRepository(session_result=[])
        client = create_app_with_overrides(repo)
        response = client.get("/api/exchanges")
        assert response.status_code == 200
        data = response.json()
        assert data == []

    def test_get_exchanges_handles_database_error(self) -> None:
        """Verify exchanges endpoint returns 500 on database error.

        Given: A repository that raises database exception,
        When: GET /exchanges is called,
        Then: Response is 500 with error detail.
        """
        repo = MockRepository(error=Exception("Database connection failed"))
        client = create_app_with_overrides(repo)
        response = client.get("/api/exchanges")
        assert response.status_code == 500
        assert "Failed to fetch exchanges" in response.json()["detail"]


class TestExchangeInstrumentsEndpoint:
    """Tests for exchange instruments discovery endpoint."""

    def test_get_exchange_instruments_returns_list(self) -> None:
        """Verify instruments endpoint returns native symbols for exchange.

        Given: Repository with symbol alias records for an exchange,
        When: GET /exchanges/kraken/instruments is called,
        Then: Response contains native symbol strings.
        """
        repo = MockRepository(session_result=["BTC-USD", "ETH-USD"])
        client = create_app_with_overrides(repo)
        response = client.get("/api/exchanges/kraken/instruments")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert "BTC-USD" in data
        assert "ETH-USD" in data

    def test_get_exchange_instruments_empty(self) -> None:
        """Verify instruments endpoint returns empty list for unknown exchange.

        Given: Repository with no symbol alias records for the exchange,
        When: GET /exchanges/unknown/instruments is called,
        Then: Response is 200 with empty list.
        """
        repo = MockRepository(session_result=[])
        client = create_app_with_overrides(repo)
        response = client.get("/api/exchanges/unknown/instruments")
        assert response.status_code == 200
        data = response.json()
        assert data == []

    def test_get_exchange_instruments_handles_database_error(self) -> None:
        """Verify instruments endpoint returns 500 on database error.

        Given: A repository that raises database exception,
        When: GET /exchanges/kraken/instruments is called,
        Then: Response is 500 with error detail.
        """
        repo = MockRepository(error=Exception("Database connection failed"))
        client = create_app_with_overrides(repo)
        response = client.get("/api/exchanges/kraken/instruments")
        assert response.status_code == 500
        assert "Failed to fetch instruments" in response.json()["detail"]


class TestCandlesHttpExceptionReraise:
    """Tests for candles endpoint HTTP exception propagation."""

    def test_candles_reraises_http_exception(self) -> None:
        """Verify candles endpoint re-raises HTTPException.

        Given: Repository session that raises HTTPException,
        When: GET /candles is called,
        Then: HTTPException is propagated with original status.
        """
        app = create_app()

        def skip_csrf_validation() -> None:
            return None

        def skip_authentication() -> UserProfile:
            return UserProfile(id="test_id", username="test_user", role=UserRole.ADMIN)

        app.dependency_overrides[validate_csrf_token] = skip_csrf_validation
        app.dependency_overrides[require_authentication] = skip_authentication
        client = TestClient(app)
        with patch("snapper.server.app.get_repository") as mock_get_repo:
            mock_repo = MagicMock()
            mock_session = MagicMock()
            mock_session.__aenter__ = AsyncMock(
                side_effect=HTTPException(status_code=403, detail="Forbidden")
            )
            mock_session.__aexit__ = AsyncMock()
            mock_repo.session.return_value = mock_session
            mock_get_repo.return_value = mock_repo
            response = client.get("/api/candles?instrument=BTC-USD&exchange=kraken&timeframe=1h")
            assert response.status_code == 403
            assert "Forbidden" in response.json()["detail"]


@pytest.fixture(autouse=True)
def reset_dummy_process_log() -> Generator[None]:
    """Provide clean process log for each test."""
    dummy_processes.reset_call_log()
    yield
    dummy_processes.reset_call_log()


@contextlib.contextmanager
def _set_argv(arguments: list[str]) -> Generator[None]:
    original_argv = sys.argv[:]
    sys.argv = arguments
    try:
        yield
    finally:
        sys.argv = original_argv


def test_main_handles_invalid_json() -> None:
    """Test process_runner returns exit code 1 on invalid JSON config.

    Given: Invalid JSON string as config argument,
    When: main() is called,
    Then: Returns exit code 1.
    """
    with _set_argv(["process_runner", "--config", "{"]):
        result = process_runner.main()
    assert result == 1


def test_main_runs_sync_method_success() -> None:
    """Test process_runner successfully executes synchronous methods.

    Given a config pointing to a SyncProcess class,
    When main() is called with valid configuration,
    Then the sync method executes and logs the expected call.
    """
    config: dict[str, object] = {
        "name": "sync_test",
        "class_path": "tests.server.dummy_processes.SyncProcess",
        "method": "start",
        "args": ["one"],
        "kwargs": {},
    }
    with _set_argv(["process_runner", "--config", json.dumps(config)]):
        process_runner.main()
    assert dummy_processes.CALL_LOG == ["sync:one"]


def test_main_runs_async_method_success() -> None:
    """Test process_runner successfully executes asynchronous methods.

    Given a config pointing to an AsyncProcess class,
    When main() is called with valid configuration,
    Then the async method executes and logs the expected call.
    """
    config: dict[str, object] = {
        "name": "async_test",
        "class_path": "tests.server.dummy_processes.AsyncProcess",
        "method": "start",
        "args": ["two"],
        "kwargs": {},
    }
    with _set_argv(["process_runner", "--config", json.dumps(config)]):
        process_runner.main()
    assert dummy_processes.CALL_LOG == ["async:two"]


def test_main_runs_sync_returning_awaitable() -> None:
    """Test process_runner handles synchronous methods returning awaitables.

    Given a config pointing to SyncReturnsAwaitableProcess,
    When main() is called,
    Then the returned awaitable is properly awaited and executed.
    """
    config: dict[str, object] = {
        "name": "awaitable_test",
        "class_path": "tests.server.dummy_processes.SyncReturnsAwaitableProcess",
        "method": "start",
        "args": ["three"],
        "kwargs": {},
    }
    with _set_argv(["process_runner", "--config", json.dumps(config)]):
        process_runner.main()
    assert dummy_processes.CALL_LOG == ["awaitable:three"]


def test_main_exits_on_process_exception() -> None:
    """Test process_runner returns exit code 1 on process exception.

    Given: Config pointing to FailingProcess that raises exception,
    When: main() is called,
    Then: Returns exit code 1 and CALL_LOG remains empty.
    """
    config: dict[str, object] = {
        "name": "failing_test",
        "class_path": "tests.server.dummy_processes.FailingProcess",
        "method": "start",
        "args": ["four"],
        "kwargs": {},
    }
    with _set_argv(["process_runner", "--config", json.dumps(config)]):
        result = process_runner.main()
    assert result == 1
    assert dummy_processes.CALL_LOG == []


@pytest.mark.asyncio
async def test_await_result_returns_value() -> None:
    """Test _await_result correctly awaits and returns coroutine values.

    Given an async coroutine that returns a string,
    When _await_result is called,
    Then it returns the coroutine's result.
    """

    async def coro() -> str:
        return "ok"

    assert await process_runner._await_result(coro()) == "ok"


@pytest.mark.asyncio
async def test_run_async_method_handles_nested_awaitable() -> None:
    """Test _run_async_method handles methods returning nested awaitables.

    Given an async method that returns another awaitable,
    When _run_async_method is called,
    Then it recursively awaits and returns the final value.
    """

    async def nested() -> str:
        return "nested"

    async def method() -> object:
        return nested()

    assert await process_runner._run_async_method(method) == "nested"


def test_build_strategy_payload_returns_none_for_non_dict() -> None:
    """Verify _build_strategy_payload returns None for non-dict input.

    Given a non-dict value passed as raw_status,
    When _build_strategy_payload is called,
    Then it returns None without raising.
    """
    assert _build_strategy_payload("not_a_dict") is None
