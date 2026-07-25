"""Tests for FastAPI server application and API router."""

import asyncio
import contextlib
import datetime as dt
import json
import os
import sys
import tempfile
from collections.abc import AsyncGenerator
from collections.abc import Generator
from datetime import UTC
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi.testclient import TestClient

from snapper.application.services.candle_query import CandleQueryRow
from snapper.auth.dependencies import require_authentication
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.core.partitioning import ShardOwnership
from snapper.core.types import ExchangeEnum
from snapper.data.repository_types import PortfolioReconciliationReadContextRow
from snapper.data.repository_types import VenueAccountStateRow
from snapper.infrastructure.rest.tracker import get_rest_call_tracker
from snapper.infrastructure.rest.tracker import reset_rest_call_tracker_for_tests
from snapper.interface.websocket.models import ConnectionStats
from snapper.interface.websocket.models import WsStatsSnapshot
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server import process_runner
from snapper.server.app import _CACHE_CANDLE_SESSION_ID
from snapper.server.app import _build_strategy_payload
from snapper.server.app import _build_user_service_publisher
from snapper.server.app import _clear_runtime_singletons
from snapper.server.app import _reconcile_stale_backtests
from snapper.server.app import _resolve_candle_single_source
from snapper.server.app import _safe_get_caps_enforcer
from snapper.server.app import _shutdown_user_service_publisher
from snapper.server.app import _sync_process_registry_for_instance
from snapper.server.app import _warn_on_tradfi_near_expiry
from snapper.server.app import create_api_router
from snapper.server.app import create_app
from snapper.server.app import get_repository_dependency
from snapper.server.app import get_settings_dependency
from snapper.server.app import lifespan
from snapper.server.app import project_query_row_to_cached_candle
from snapper.server.app import project_query_row_to_candle_data
from snapper.server.dependencies import reset_caps_enforcer_singleton
from snapper.utils.logging import setup_logging
from tests.server import dummy_processes


def _make_ai_review_service_mock() -> MagicMock:
    """Build an AiReviewService mock safe for the lifespan path.

    The real singleton's :meth:`start_bus_listener` opens a ZMQ
    socket which would hang forever when handed a MagicMock-coerced
    URL from the settings stub, so every lifespan-entry test that
    does not exercise ai-review wiring directly MUST patch this
    factory in via ``snapper.server.app.get_ai_review_service``.
    """
    return MagicMock(
        set_msg_publisher=MagicMock(),
        set_repository_factory=MagicMock(),
        set_shard_ownership=MagicMock(),
        start_bus_listener=AsyncMock(),
        stop_bus_listener=AsyncMock(),
    )


def _make_token_listener_recorder(recorded: list[str]) -> MagicMock:
    """Build a TokenManager mock whose listener calls append to ``recorded``.

    Mirrors the ``mock_ws_auth`` helper in the lifespan-ordering test
    so the ``TokenManager.start_admin_listener`` /
    ``stop_admin_listener`` calls land in the same event sequence as
    the ``WebSocketAuthManager`` calls. Each call appends a
    distinct prefix (`tm_*` vs `ws_*`) so the ordering assertion can
    pin the relative sequence of both listeners' lifecycle hooks.
    """
    mock = MagicMock()

    async def _start(_xpub: str) -> None:
        recorded.append("tm_start_admin_listener")

    async def _stop() -> None:
        recorded.append("tm_stop_admin_listener")

    mock.start_admin_listener = _start
    mock.stop_admin_listener = _stop
    return mock


TEST_DB_URL = "sqlite:///:memory:"

_tracked_test_clients: list[TestClient] = []


def _track_test_client(client: TestClient) -> TestClient:
    """Track a TestClient so module teardown can close it deterministically."""
    _tracked_test_clients.append(client)
    return client


def teardown_module(module: object) -> None:
    """Close all TestClient instances created in this module."""
    for client in _tracked_test_clients:
        client.close()
    _tracked_test_clients.clear()


def _build_mock_process_factory(started_processes: dict[str, object]) -> MagicMock:
    """Create a mock process launcher with async lifecycle methods."""
    mock_factory = MagicMock()
    mock_factory.started_processes = started_processes
    mock_factory.sync_registry_to_database = AsyncMock()
    mock_factory.start_all_processes = AsyncMock()
    mock_factory.spawn_per_wallet_executors = AsyncMock(return_value=0)
    mock_factory.stop_all_processes = AsyncMock()
    mock_factory.get_core_health = AsyncMock(return_value="healthy")
    return mock_factory


@contextlib.asynccontextmanager
async def _noop_lifespan(_app: FastAPI) -> AsyncGenerator[None]:
    """Disable application lifespan for endpoint-only tests."""
    yield


class TestLifespan:
    """Tests for application lifespan management."""

    @pytest.mark.asyncio
    async def test_sync_process_registry_runs_in_single_instance_mode(self) -> None:
        """Single-instance deployments keep the previous unconditional registry sync.

        Given: coordinator instance count is one,
        When: the process registry sync gate runs,
        Then: registry sync is awaited.
        """
        process_factory = MagicMock()
        process_factory.sync_registry_to_database = AsyncMock()
        settings = MagicMock()
        settings.coordinator_instance_id = 0
        settings.coordinator_instance_count = 1
        await _sync_process_registry_for_instance(process_factory, settings)
        process_factory.sync_registry_to_database.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_sync_process_registry_runs_on_instance_zero_when_partitioned(self) -> None:
        """Instance zero owns replicated registry sync in multi-instance deployments.

        Given: coordinator instance count is greater than one and this instance is zero,
        When: the process registry sync gate runs,
        Then: registry sync is awaited once.
        """
        process_factory = MagicMock()
        process_factory.sync_registry_to_database = AsyncMock()
        settings = MagicMock()
        settings.coordinator_instance_id = 0
        settings.coordinator_instance_count = 3
        await _sync_process_registry_for_instance(process_factory, settings)
        process_factory.sync_registry_to_database.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_sync_process_registry_skips_nonzero_instance_when_partitioned(self) -> None:
        """Non-owner instances skip replicated registry sync at N greater than one.

        Given: coordinator instance count is greater than one and this instance is not zero,
        When: the process registry sync gate runs,
        Then: registry sync is not awaited.
        """
        process_factory = MagicMock()
        process_factory.sync_registry_to_database = AsyncMock()
        settings = MagicMock()
        settings.coordinator_instance_id = 2
        settings.coordinator_instance_count = 3
        await _sync_process_registry_for_instance(process_factory, settings)
        process_factory.sync_registry_to_database.assert_not_awaited()

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
        mock_app.state.mcp_sub_app.router.lifespan_context = _noop_lifespan
        lifespan_settings = MagicMock()
        lifespan_settings.db_url = TEST_DB_URL
        lifespan_settings.zmq_broker_xsub = "tcp://broker.xsub"
        lifespan_settings.zmq_broker_xpub = "tcp://broker.xpub"
        lifespan_settings.server_api_only = False
        lifespan_settings.coordinator_instance_id = 0
        lifespan_settings.coordinator_instance_count = 1
        start_ai_research_trigger = AsyncMock()
        stop_ai_research_trigger = AsyncMock()
        start_ai_review_maintenance = AsyncMock()
        stop_ai_review_maintenance = AsyncMock()
        with (
            patch("snapper.server.app.discover_processes") as mock_discover,
            patch(
                "snapper.server.app.get_ai_review_service",
                return_value=_make_ai_review_service_mock(),
            ),
            patch("snapper.server.app.ProcessLauncherService") as mock_factory_cls,
            patch("snapper.server.app.get_settings_service") as mock_get_settings_service,
            patch(
                "snapper.server.app.get_settings_with_service",
                return_value=lifespan_settings,
            ),
            patch(
                "snapper.server.app._build_user_service_publisher",
                return_value=(MagicMock(), MagicMock()),
            ),
            patch("snapper.server.app._shutdown_user_service_publisher"),
            patch(
                "snapper.server.app.MarketPersistPolicy",
                return_value=MagicMock(
                    initial_rebuild=AsyncMock(),
                    start_admin_listener=AsyncMock(),
                    stop=AsyncMock(),
                ),
            ),
            patch(
                "snapper.server.app.MarketCacheService",
                return_value=MagicMock(
                    start=AsyncMock(),
                    stop=AsyncMock(),
                ),
            ),
            patch(
                "snapper.server.app.MarketStatsWorker",
                return_value=MagicMock(
                    start=AsyncMock(),
                    stop=AsyncMock(),
                ),
            ),
            patch("snapper.server.app.get_repository"),
            patch.multiple(
                "snapper.server.app",
                _start_system_metrics_snapshotter=AsyncMock(),
                _stop_system_metrics_snapshotter=AsyncMock(),
                _start_market_data_watchdog=AsyncMock(),
                _stop_market_data_watchdog=AsyncMock(),
                _start_ai_research_trigger=start_ai_research_trigger,
                _stop_ai_research_trigger=stop_ai_research_trigger,
                _start_ai_review_maintenance=start_ai_review_maintenance,
                _stop_ai_review_maintenance=stop_ai_review_maintenance,
                _start_ai_delegate_watchdog=MagicMock(),
                _stop_ai_delegate_watchdog=AsyncMock(),
                _start_retention_scheduler=AsyncMock(),
                _stop_retention_scheduler=AsyncMock(),
                _start_background_writers=AsyncMock(),
                _stop_background_writers=AsyncMock(),
            ),
            patch(
                "snapper.server.app.get_ws_auth_manager",
                return_value=MagicMock(
                    start_admin_listener=AsyncMock(),
                    stop_admin_listener=AsyncMock(),
                    cancel_pending_offline_tasks=AsyncMock(),
                ),
            ),
            patch(
                "snapper.server.app.get_token_manager",
                return_value=MagicMock(
                    start_admin_listener=AsyncMock(),
                    stop_admin_listener=AsyncMock(),
                    cancel_pending_offline_tasks=AsyncMock(),
                ),
            ),
        ):
            mock_settings_service = MagicMock()
            mock_settings_service.shutdown = AsyncMock()
            mock_get_settings_service.return_value = mock_settings_service
            mock_factory = MagicMock()
            mock_factory.sync_registry_to_database = AsyncMock()
            mock_factory.start_all_processes = AsyncMock()
            mock_factory.spawn_per_wallet_executors = AsyncMock(return_value=0)
            mock_factory.stop_all_processes = AsyncMock()
            mock_factory_cls.return_value = mock_factory
            async with lifespan(mock_app):
                """Consumed by iteration to trigger exception."""
                pass
        mock_discover.assert_called_once()
        mock_factory.sync_registry_to_database.assert_awaited_once()
        mock_factory.start_all_processes.assert_awaited_once()
        mock_factory.stop_all_processes.assert_awaited_once()
        start_ai_research_trigger.assert_awaited_once()
        stop_ai_research_trigger.assert_awaited_once()
        start_ai_review_maintenance.assert_awaited_once()
        stop_ai_review_maintenance.assert_awaited_once()
        mock_manager.cleanup.assert_called_once()

    @pytest.mark.asyncio
    async def test_lifespan_propagates_unexpected_runtime_error_from_mcp_sub_app(
        self,
    ) -> None:
        """Lifespan re-raises RuntimeErrors from the MCP sub-app that are NOT the known re-entry error.

        Background: the MCP sub-app lifespan entry catches
        ``RuntimeError("... can only be called once ...")`` which is
        FastMCP's signature for a re-entered session manager (observed
        in module-scoped test fixtures that spawn nested ``TestClient``
        instances on the same app). Any OTHER RuntimeError signals a
        real failure and must propagate so operators see it.

        Given: the MCP sub-app's ``lifespan_context`` raises
            ``RuntimeError("unexpected downstream error")`` at startup,
        When: the parent lifespan enters,
        Then: the exception propagates (not swallowed by the
            nested-re-entry guard).
        """

        class _FailingLifespanCtx:
            async def __aenter__(self) -> None:
                raise RuntimeError("unexpected downstream error")

            async def __aexit__(self, *_: object) -> None:
                return None

        mock_app = MagicMock()
        mock_manager = MagicMock()
        mock_manager.cleanup = AsyncMock()
        mock_zmq_bridge = MagicMock()
        mock_zmq_bridge.start = AsyncMock()
        mock_zmq_bridge.stop = AsyncMock()
        mock_manager.zmq_bridge = mock_zmq_bridge
        mock_app.state.manager = mock_manager
        mock_app.state.mcp_sub_app.router.lifespan_context = MagicMock(
            return_value=_FailingLifespanCtx(),
        )
        with (
            patch("snapper.server.app.discover_processes"),
            patch(
                "snapper.server.app.get_ai_review_service",
                return_value=_make_ai_review_service_mock(),
            ),
            patch("snapper.server.app.ProcessLauncherService") as mock_factory_cls,
            patch("snapper.server.app.get_settings_service") as mock_get_settings_service,
            patch(
                "snapper.server.app._build_user_service_publisher",
                return_value=(MagicMock(), MagicMock()),
            ),
            patch("snapper.server.app._shutdown_user_service_publisher"),
            patch(
                "snapper.server.app.get_ws_auth_manager",
                return_value=MagicMock(
                    start_admin_listener=AsyncMock(),
                    stop_admin_listener=AsyncMock(),
                    cancel_pending_offline_tasks=AsyncMock(),
                ),
            ),
            patch(
                "snapper.server.app.get_token_manager",
                return_value=MagicMock(
                    start_admin_listener=AsyncMock(),
                    stop_admin_listener=AsyncMock(),
                    cancel_pending_offline_tasks=AsyncMock(),
                ),
            ),
        ):
            mock_settings_service = MagicMock()
            mock_settings_service.shutdown = AsyncMock()
            mock_get_settings_service.return_value = mock_settings_service
            mock_factory = _build_mock_process_factory(started_processes={})
            mock_factory_cls.return_value = mock_factory
            with pytest.raises(RuntimeError, match="unexpected downstream error"):
                async with lifespan(mock_app):
                    pass

    @pytest.mark.asyncio
    async def test_lifespan_tolerates_mcp_sub_app_reentry(self) -> None:
        """Lifespan swallows the known MCP session-manager re-entry error.

        Background: FastMCP's session manager raises
        ``RuntimeError("... can only be called once ...")`` when its
        lifespan is entered a second time on the same app object —
        historically hit by module-scoped test fixtures spawning nested
        ``TestClient`` instances. The parent lifespan must treat that
        signature as benign (warn + continue serving) instead of
        failing startup.

        Given: the MCP sub-app's ``lifespan_context`` raises the
            re-entry RuntimeError at startup,
        When: the parent lifespan enters,
        Then: startup completes (the body runs) and shutdown proceeds
            normally — the guard yielded instead of re-raising.
        """

        class _ReentryLifespanCtx:
            async def __aenter__(self) -> None:
                raise RuntimeError("StreamableHTTPSessionManager .run() can only be called once")

            async def __aexit__(self, *_: object) -> None:
                return None

        mock_app = MagicMock()
        mock_manager = MagicMock()
        mock_manager.cleanup = AsyncMock()
        mock_zmq_bridge = MagicMock()
        mock_zmq_bridge.start = AsyncMock()
        mock_zmq_bridge.stop = AsyncMock()
        mock_manager.zmq_bridge = mock_zmq_bridge
        mock_app.state.manager = mock_manager
        mock_app.state.mcp_sub_app.router.lifespan_context = MagicMock(
            return_value=_ReentryLifespanCtx(),
        )
        with (
            patch("snapper.server.app.discover_processes"),
            patch(
                "snapper.server.app.get_ai_review_service",
                return_value=_make_ai_review_service_mock(),
            ),
            patch("snapper.server.app.ProcessLauncherService") as mock_factory_cls,
            patch("snapper.server.app.get_settings_service") as mock_get_settings_service,
            patch(
                "snapper.server.app._build_user_service_publisher",
                return_value=(MagicMock(), MagicMock()),
            ),
            patch("snapper.server.app._shutdown_user_service_publisher"),
            patch(
                "snapper.server.app.get_ws_auth_manager",
                return_value=MagicMock(
                    start_admin_listener=AsyncMock(),
                    stop_admin_listener=AsyncMock(),
                    cancel_pending_offline_tasks=AsyncMock(),
                ),
            ),
            patch(
                "snapper.server.app.get_token_manager",
                return_value=MagicMock(
                    start_admin_listener=AsyncMock(),
                    stop_admin_listener=AsyncMock(),
                    cancel_pending_offline_tasks=AsyncMock(),
                ),
            ),
        ):
            mock_settings_service = MagicMock()
            mock_settings_service.shutdown = AsyncMock()
            mock_get_settings_service.return_value = mock_settings_service
            mock_factory = _build_mock_process_factory(started_processes={})
            mock_factory_cls.return_value = mock_factory
            body_ran: list[bool] = []
            async with lifespan(mock_app):
                body_ran.append(True)
            assert body_ran == [True]
        mock_manager.cleanup.assert_called_once()

    @pytest.mark.asyncio
    async def test_lifespan_passes_xsub_endpoint_to_user_service_publisher(
        self,
    ) -> None:
        """Lifespan passes `settings.zmq_broker_xsub` to publisher (not xpub).

        The wrong endpoint here would be `settings.zmq_broker_xpub`,
        which violates the broker proxy contract (publishers connect to
        XSUB, not XPUB). This test pins the call argument so a future
        refactor that flips it back fails CI loudly.
        """
        mock_app = MagicMock()
        mock_manager = MagicMock()
        mock_manager.cleanup = AsyncMock()
        mock_zmq_bridge = MagicMock()
        mock_zmq_bridge.start = AsyncMock()
        mock_zmq_bridge.stop = AsyncMock()
        mock_manager.zmq_bridge = mock_zmq_bridge
        mock_app.state.manager = mock_manager
        api_only_settings = MagicMock()
        api_only_settings.server_api_only = True
        api_only_settings.zmq_broker_xsub = "tcp://test-broker-xsub:7500"
        api_only_settings.zmq_broker_xpub = "tcp://test-broker-xpub:7501"
        api_only_settings.coordinator_instance_id = 0
        api_only_settings.coordinator_instance_count = 1
        with (
            patch("snapper.server.app.discover_processes"),
            patch(
                "snapper.server.app.get_ai_review_service",
                return_value=_make_ai_review_service_mock(),
            ),
            patch("snapper.server.app.ProcessLauncherService") as mock_factory_cls,
            patch("snapper.server.app.get_settings_service") as mock_get_settings_service,
            patch(
                "snapper.server.app.get_settings_with_service",
                return_value=api_only_settings,
            ),
            patch(
                "snapper.server.app._build_user_service_publisher",
                return_value=(MagicMock(), MagicMock()),
            ) as mock_build,
            patch("snapper.server.app._shutdown_user_service_publisher"),
            patch(
                "snapper.server.app.MarketPersistPolicy",
                return_value=MagicMock(
                    initial_rebuild=AsyncMock(),
                    start_admin_listener=AsyncMock(),
                    stop=AsyncMock(),
                ),
            ),
            patch(
                "snapper.server.app.MarketCacheService",
                return_value=MagicMock(
                    start=AsyncMock(),
                    stop=AsyncMock(),
                ),
            ),
            patch(
                "snapper.server.app.MarketStatsWorker",
                return_value=MagicMock(
                    start=AsyncMock(),
                    stop=AsyncMock(),
                ),
            ),
            patch("snapper.server.app.get_repository"),
            patch(
                "snapper.server.app.get_ws_auth_manager",
                return_value=MagicMock(
                    start_admin_listener=AsyncMock(),
                    stop_admin_listener=AsyncMock(),
                    cancel_pending_offline_tasks=AsyncMock(),
                ),
            ),
            patch(
                "snapper.server.app.get_token_manager",
                return_value=MagicMock(
                    start_admin_listener=AsyncMock(),
                    stop_admin_listener=AsyncMock(),
                    cancel_pending_offline_tasks=AsyncMock(),
                ),
            ),
        ):
            mock_settings_service = MagicMock()
            mock_settings_service.shutdown = AsyncMock()
            mock_get_settings_service.return_value = mock_settings_service
            mock_factory = MagicMock()
            mock_factory.sync_registry_to_database = AsyncMock()
            mock_factory.start_all_processes = AsyncMock()
            mock_factory.spawn_per_wallet_executors = AsyncMock(return_value=0)
            mock_factory.stop_all_processes = AsyncMock()
            mock_factory_cls.return_value = mock_factory
            async with lifespan(mock_app):
                pass
        mock_build.assert_called_once_with("tcp://test-broker-xsub:7500")

    @pytest.mark.asyncio
    async def test_lifespan_pins_publisher_and_listener_relative_ordering(
        self,
    ) -> None:
        """Pin the relative order of publisher injection vs listener start/stop.

        The other lifespan tests stub the helpers but don't assert call
        sequence. The ordering matters here — the WS subscriber
        must come up after the publisher socket is injected (so any
        immediate publish event reaches a live subscriber) and the
        subscriber must shut down before the publisher socket so the
        broker side never sees a half-broken pair. Records calls via
        a parent `MagicMock` and asserts the exact order.
        """
        mock_app = MagicMock()
        mock_manager = MagicMock()
        mock_manager.cleanup = AsyncMock()
        mock_zmq_bridge = MagicMock()
        mock_zmq_bridge.start = AsyncMock()
        mock_zmq_bridge.stop = AsyncMock()
        mock_manager.zmq_bridge = mock_zmq_bridge
        mock_app.state.manager = mock_manager
        recorded: list[str] = []
        mock_user_service = MagicMock()
        mock_user_service.set_msg_publisher.side_effect = lambda _value: recorded.append(
            "set_msg_publisher"
        )
        mock_ws_auth = MagicMock()

        async def _start_listener(_xpub: str) -> None:
            recorded.append("ws_start_admin_listener")

        async def _stop_listener() -> None:
            recorded.append("ws_stop_admin_listener")

        async def _cancel_pending_offline() -> None:
            recorded.append("ws_cancel_pending_offline_tasks")

        mock_ws_auth.start_admin_listener = _start_listener
        mock_ws_auth.stop_admin_listener = _stop_listener
        mock_ws_auth.cancel_pending_offline_tasks = _cancel_pending_offline

        def _shutdown_publisher(_app: FastAPI) -> None:
            recorded.append("shutdown_publisher")

        with (
            patch("snapper.server.app.discover_processes"),
            patch(
                "snapper.server.app.get_ai_review_service",
                return_value=_make_ai_review_service_mock(),
            ),
            patch("snapper.server.app.ProcessLauncherService") as mock_factory_cls,
            patch("snapper.server.app.get_settings_service") as mock_get_settings_service,
            patch(
                "snapper.server.app._build_user_service_publisher",
                return_value=(MagicMock(), MagicMock()),
            ),
            patch(
                "snapper.server.app._shutdown_user_service_publisher",
                side_effect=_shutdown_publisher,
            ),
            patch(
                "snapper.server.app.get_user_service",
                return_value=mock_user_service,
            ),
            patch(
                "snapper.server.app.get_ws_auth_manager",
                return_value=mock_ws_auth,
            ),
            patch(
                "snapper.server.app.get_token_manager",
                return_value=_make_token_listener_recorder(recorded),
            ),
        ):
            mock_settings_service = MagicMock()
            mock_settings_service.shutdown = AsyncMock()
            mock_get_settings_service.return_value = mock_settings_service
            mock_factory = MagicMock()
            mock_factory.sync_registry_to_database = AsyncMock()
            mock_factory.start_all_processes = AsyncMock()
            mock_factory.spawn_per_wallet_executors = AsyncMock(return_value=0)
            mock_factory.stop_all_processes = AsyncMock()
            mock_factory_cls.return_value = mock_factory
            async with lifespan(mock_app):
                pass
        shutdown_anchor = recorded.index("ws_cancel_pending_offline_tasks")
        startup_order = recorded[:shutdown_anchor]
        shutdown_order = recorded[shutdown_anchor:]
        assert startup_order == [
            "set_msg_publisher",
            "ws_start_admin_listener",
            "tm_start_admin_listener",
        ]
        assert shutdown_order == [
            "ws_cancel_pending_offline_tasks",
            "ws_stop_admin_listener",
            "tm_stop_admin_listener",
            "shutdown_publisher",
        ]

    @pytest.mark.asyncio
    async def test_lifespan_injects_scope_grant_service_publisher(
        self,
    ) -> None:
        """ScopeGrantService shares the UserService publisher socket.

        After ``UserService.set_msg_publisher(user_publisher)`` the
        lifespan MUST call ``ScopeGrantService.set_msg_publisher(user_publisher)``
        with the SAME publisher instance — single ZMQ PUB socket serves
        both ``admin.user_deactivated`` and ``admin.scope_revoked``
        (vs. opening a second socket and doubling broker connection
        count).
        """
        mock_app = MagicMock()
        mock_manager = MagicMock()
        mock_manager.cleanup = AsyncMock()
        mock_zmq_bridge = MagicMock()
        mock_zmq_bridge.start = AsyncMock()
        mock_zmq_bridge.stop = AsyncMock()
        mock_manager.zmq_bridge = mock_zmq_bridge
        mock_app.state.manager = mock_manager
        user_publisher = MagicMock()
        mock_user_service = MagicMock()
        mock_scope_grant_service = MagicMock()
        with (
            patch("snapper.server.app.discover_processes"),
            patch(
                "snapper.server.app.get_ai_review_service",
                return_value=_make_ai_review_service_mock(),
            ),
            patch("snapper.server.app.ProcessLauncherService") as mock_factory_cls,
            patch("snapper.server.app.get_settings_service") as mock_get_settings_service,
            patch(
                "snapper.server.app._build_user_service_publisher",
                return_value=(user_publisher, MagicMock()),
            ),
            patch("snapper.server.app._shutdown_user_service_publisher"),
            patch(
                "snapper.server.app.get_user_service",
                return_value=mock_user_service,
            ),
            patch(
                "snapper.server.app.get_scope_grant_service",
                return_value=mock_scope_grant_service,
            ),
            patch(
                "snapper.server.app.get_ws_auth_manager",
                return_value=MagicMock(
                    start_admin_listener=AsyncMock(),
                    stop_admin_listener=AsyncMock(),
                    cancel_pending_offline_tasks=AsyncMock(),
                ),
            ),
            patch(
                "snapper.server.app.get_token_manager",
                return_value=MagicMock(
                    start_admin_listener=AsyncMock(),
                    stop_admin_listener=AsyncMock(),
                    cancel_pending_offline_tasks=AsyncMock(),
                ),
            ),
        ):
            mock_settings_service = MagicMock()
            mock_settings_service.shutdown = AsyncMock()
            mock_get_settings_service.return_value = mock_settings_service
            mock_factory = MagicMock()
            mock_factory.sync_registry_to_database = AsyncMock()
            mock_factory.start_all_processes = AsyncMock()
            mock_factory.spawn_per_wallet_executors = AsyncMock(return_value=0)
            mock_factory.stop_all_processes = AsyncMock()
            mock_factory_cls.return_value = mock_factory
            async with lifespan(mock_app):
                pass
        mock_user_service.set_msg_publisher.assert_called_once_with(user_publisher)
        mock_scope_grant_service.set_msg_publisher.assert_called_once_with(user_publisher)

    @pytest.mark.asyncio
    async def test_lifespan_injects_ai_review_service_publisher(
        self,
    ) -> None:
        """AiReviewService shares the same publisher socket as UserService.

        ``handle_caps_violation_bus_message`` re-fanouts internal
        caps-violation events onto the external
        ``ai_reviews.{user}.{strategy}.caps_violation`` WS topic. The
        FastAPI lifespan MUST inject the publisher singleton so the
        handler is non-degraded by the time the bus subscriber loop
        delivers the first event; without this wiring the handler logs
        a warning and silently drops every fanout. Mirrors the
        ScopeGrantService wiring (single ZMQ PUB socket fans out
        admin.* + ai_reviews.* topics; opening a second socket would
        double broker connection count for no gain) and asserts the
        teardown clears the slot for graceful shutdown.
        """
        mock_app = MagicMock()
        mock_manager = MagicMock()
        mock_manager.cleanup = AsyncMock()
        mock_zmq_bridge = MagicMock()
        mock_zmq_bridge.start = AsyncMock()
        mock_zmq_bridge.stop = AsyncMock()
        mock_manager.zmq_bridge = mock_zmq_bridge
        mock_app.state.manager = mock_manager
        user_publisher = MagicMock()
        mock_user_service = MagicMock()
        mock_scope_grant_service = MagicMock()
        mock_ai_review_service = _make_ai_review_service_mock()
        with (
            patch("snapper.server.app.discover_processes"),
            patch(
                "snapper.server.app.get_ai_review_service",
                return_value=_make_ai_review_service_mock(),
            ),
            patch("snapper.server.app.ProcessLauncherService") as mock_factory_cls,
            patch("snapper.server.app.get_settings_service") as mock_get_settings_service,
            patch(
                "snapper.server.app._build_user_service_publisher",
                return_value=(user_publisher, MagicMock()),
            ),
            patch("snapper.server.app._shutdown_user_service_publisher"),
            patch(
                "snapper.server.app.get_user_service",
                return_value=mock_user_service,
            ),
            patch(
                "snapper.server.app.get_scope_grant_service",
                return_value=mock_scope_grant_service,
            ),
            patch(
                "snapper.server.app.get_ai_review_service",
                return_value=mock_ai_review_service,
            ),
            patch(
                "snapper.server.app.get_ws_auth_manager",
                return_value=MagicMock(
                    start_admin_listener=AsyncMock(),
                    stop_admin_listener=AsyncMock(),
                    cancel_pending_offline_tasks=AsyncMock(),
                ),
            ),
            patch(
                "snapper.server.app.get_token_manager",
                return_value=MagicMock(
                    start_admin_listener=AsyncMock(),
                    stop_admin_listener=AsyncMock(),
                ),
            ),
        ):
            mock_settings_service = MagicMock()
            mock_settings_service.shutdown = AsyncMock()
            mock_get_settings_service.return_value = mock_settings_service
            mock_factory = MagicMock()
            mock_factory.sync_registry_to_database = AsyncMock()
            mock_factory.start_all_processes = AsyncMock()
            mock_factory.spawn_per_wallet_executors = AsyncMock(return_value=0)
            mock_factory.stop_all_processes = AsyncMock()
            mock_factory_cls.return_value = mock_factory
            async with lifespan(mock_app):
                pass
        mock_ai_review_service.set_msg_publisher.assert_called_once_with(user_publisher)
        ownership_calls = [
            call.args[0]
            for call in mock_ai_review_service.set_shard_ownership.call_args_list
            if call.args and isinstance(call.args[0], ShardOwnership)
        ]
        assert len(ownership_calls) == 1, (
            f"expected exactly one ShardOwnership startup injection (clear-on-shutdown "
            f"is None), got call list={mock_ai_review_service.set_shard_ownership.call_args_list!r}"
        )

    @pytest.mark.asyncio
    async def test_lifespan_injects_ws_auth_manager_publisher(self) -> None:
        """WebSocketAuthManager shares the UserService publisher socket.

        :meth:`WebSocketAuthManager.set_msg_publisher` exists for the
        layer-1 ``bus.delegate_offline`` fast-path emit. Without this
        injection :meth:`_publish_delegate_offline` observes ``None``
        + logs a warning, so AI delegates that drop their WS would
        never trigger the AI Review fanout fast-path until the natural
        fanout timer eventually fired. Mirrors the AiReviewService
        wiring assertion above.
        """
        mock_app = MagicMock()
        mock_manager = MagicMock()
        mock_manager.cleanup = AsyncMock()
        mock_zmq_bridge = MagicMock()
        mock_zmq_bridge.start = AsyncMock()
        mock_zmq_bridge.stop = AsyncMock()
        mock_manager.zmq_bridge = mock_zmq_bridge
        mock_app.state.manager = mock_manager
        user_publisher = MagicMock()
        ws_auth_manager_mock = MagicMock(
            start_admin_listener=AsyncMock(),
            stop_admin_listener=AsyncMock(),
            cancel_pending_offline_tasks=AsyncMock(),
        )
        with (
            patch("snapper.server.app.discover_processes"),
            patch(
                "snapper.server.app.get_ai_review_service",
                return_value=_make_ai_review_service_mock(),
            ),
            patch("snapper.server.app.ProcessLauncherService") as mock_factory_cls,
            patch("snapper.server.app.get_settings_service") as mock_get_settings_service,
            patch(
                "snapper.server.app._build_user_service_publisher",
                return_value=(user_publisher, MagicMock()),
            ),
            patch("snapper.server.app._shutdown_user_service_publisher"),
            patch("snapper.server.app.get_user_service", return_value=MagicMock()),
            patch("snapper.server.app.get_scope_grant_service", return_value=MagicMock()),
            patch(
                "snapper.server.app.get_ai_review_service",
                return_value=_make_ai_review_service_mock(),
            ),
            patch(
                "snapper.server.app.get_ws_auth_manager",
                return_value=ws_auth_manager_mock,
            ),
            patch(
                "snapper.server.app.get_token_manager",
                return_value=MagicMock(
                    start_admin_listener=AsyncMock(),
                    stop_admin_listener=AsyncMock(),
                ),
            ),
        ):
            mock_settings_service = MagicMock()
            mock_settings_service.shutdown = AsyncMock()
            mock_get_settings_service.return_value = mock_settings_service
            mock_factory = MagicMock()
            mock_factory.sync_registry_to_database = AsyncMock()
            mock_factory.start_all_processes = AsyncMock()
            mock_factory.spawn_per_wallet_executors = AsyncMock(return_value=0)
            mock_factory.stop_all_processes = AsyncMock()
            mock_factory_cls.return_value = mock_factory
            async with lifespan(mock_app):
                pass
        ws_auth_manager_mock.set_msg_publisher.assert_called_once_with(user_publisher)

    @pytest.mark.asyncio
    async def test_shutdown_clears_all_publisher_slots(self) -> None:
        """``_shutdown_user_service_publisher`` clears every publisher slot.

        The shutdown helper closes the single shared publisher socket
        used by UserService, ScopeGrantService, AiReviewService,
        WebSocketAuthManager AND TradingCapsEnforcer. It MUST clear
        all five publisher slots BEFORE closing the socket so any
        in-flight handler observes ``None`` and degrades gracefully
        rather than calling ``.send()`` on a torn-down ZMQ socket.
        Mirrors the existing UserService / ScopeGrantService teardown
        contract.
        """
        mock_app = MagicMock()
        mock_app.state.user_service_publisher = None
        mock_app.state.user_service_publisher_context = None
        mock_user_service = MagicMock()
        mock_scope_grant_service = MagicMock()
        mock_ai_review_service = _make_ai_review_service_mock()
        mock_ws_auth_manager = MagicMock()
        mock_caps_enforcer = MagicMock()
        with (
            patch(
                "snapper.server.app.get_user_service",
                return_value=mock_user_service,
            ),
            patch(
                "snapper.server.app.get_scope_grant_service",
                return_value=mock_scope_grant_service,
            ),
            patch(
                "snapper.server.app.get_ai_review_service",
                return_value=mock_ai_review_service,
            ),
            patch(
                "snapper.server.app.get_ws_auth_manager",
                return_value=mock_ws_auth_manager,
            ),
            patch(
                "snapper.server.app._safe_get_caps_enforcer",
                return_value=mock_caps_enforcer,
            ),
        ):
            _shutdown_user_service_publisher(mock_app)
        mock_user_service.set_msg_publisher.assert_called_once_with(None)
        mock_scope_grant_service.set_msg_publisher.assert_called_once_with(None)
        mock_ai_review_service.set_msg_publisher.assert_called_once_with(None)
        mock_ws_auth_manager.set_msg_publisher.assert_called_once_with(None)
        mock_caps_enforcer.set_msg_publisher.assert_called_once_with(None)

    @pytest.mark.asyncio
    async def test_shutdown_tolerates_caps_enforcer_unavailable(self) -> None:
        """``_safe_get_caps_enforcer`` returns None pre-startup -> no crash.

        Pre-startup window (or non-SQLAlchemy repo in tests) makes the
        enforcer singleton unavailable. The shutdown helper must
        tolerate this and skip the slot clear without raising.
        """
        mock_app = MagicMock()
        mock_app.state.user_service_publisher = None
        mock_app.state.user_service_publisher_context = None
        with (
            patch("snapper.server.app.get_user_service", return_value=MagicMock()),
            patch("snapper.server.app.get_scope_grant_service", return_value=MagicMock()),
            patch(
                "snapper.server.app.get_ai_review_service",
                return_value=_make_ai_review_service_mock(),
            ),
            patch("snapper.server.app.get_ws_auth_manager", return_value=MagicMock()),
            patch("snapper.server.app._safe_get_caps_enforcer", return_value=None),
        ):
            _shutdown_user_service_publisher(mock_app)

    @pytest.mark.asyncio
    async def test_lifespan_injects_caps_enforcer_publisher(self) -> None:
        """TradingCapsEnforcer shares the UserService publisher socket.

        Lifespan must inject the ZMQ publisher onto the caps enforcer
        at startup (so CapsViolationError raised against AI-approved
        submissions can fan out bus.caps_violation_after_ai_approve to
        AiReviewService) AND clear the slot at shutdown (so an
        in-flight cap evaluation observes ``None`` and degrades
        gracefully). Mirrors the AiReviewService / WebSocketAuthManager
        wiring assertions above. Uses a recorder side-effect on
        ``set_msg_publisher`` so both startup + shutdown calls are
        pinned in a single sequence.
        """
        mock_app = MagicMock()
        mock_manager = MagicMock()
        mock_manager.cleanup = AsyncMock()
        mock_zmq_bridge = MagicMock()
        mock_zmq_bridge.start = AsyncMock()
        mock_zmq_bridge.stop = AsyncMock()
        mock_manager.zmq_bridge = mock_zmq_bridge
        mock_app.state.manager = mock_manager
        user_publisher = MagicMock()
        mock_caps_enforcer = MagicMock()
        recorded_publisher_calls: list[object] = []
        mock_caps_enforcer.set_msg_publisher.side_effect = lambda value: (
            recorded_publisher_calls.append(value)
        )

        def _shutdown_user_pub_passthrough(_app: FastAPI) -> None:
            mock_caps_enforcer.set_msg_publisher(None)

        with (
            patch("snapper.server.app.discover_processes"),
            patch("snapper.server.app.ProcessLauncherService") as mock_factory_cls,
            patch("snapper.server.app.get_settings_service") as mock_get_settings_service,
            patch(
                "snapper.server.app._build_user_service_publisher",
                return_value=(user_publisher, MagicMock()),
            ),
            patch(
                "snapper.server.app._shutdown_user_service_publisher",
                side_effect=_shutdown_user_pub_passthrough,
            ),
            patch("snapper.server.app.get_user_service", return_value=MagicMock()),
            patch("snapper.server.app.get_scope_grant_service", return_value=MagicMock()),
            patch(
                "snapper.server.app.get_ai_review_service",
                return_value=_make_ai_review_service_mock(),
            ),
            patch(
                "snapper.server.app._safe_get_caps_enforcer",
                return_value=mock_caps_enforcer,
            ),
            patch(
                "snapper.server.app.get_ws_auth_manager",
                return_value=MagicMock(
                    start_admin_listener=AsyncMock(),
                    stop_admin_listener=AsyncMock(),
                    cancel_pending_offline_tasks=AsyncMock(),
                ),
            ),
            patch(
                "snapper.server.app.get_token_manager",
                return_value=MagicMock(
                    start_admin_listener=AsyncMock(),
                    stop_admin_listener=AsyncMock(),
                ),
            ),
        ):
            mock_settings_service = MagicMock()
            mock_settings_service.shutdown = AsyncMock()
            mock_get_settings_service.return_value = mock_settings_service
            mock_factory = MagicMock()
            mock_factory.sync_registry_to_database = AsyncMock()
            mock_factory.start_all_processes = AsyncMock()
            mock_factory.spawn_per_wallet_executors = AsyncMock(return_value=0)
            mock_factory.stop_all_processes = AsyncMock()
            mock_factory_cls.return_value = mock_factory
            async with lifespan(mock_app):
                pass
        assert recorded_publisher_calls == [user_publisher, None]

    @pytest.mark.asyncio
    async def test_lifespan_skips_caps_enforcer_publisher_when_not_initialised(self) -> None:
        """``_safe_get_caps_enforcer`` returns None pre-startup -> no crash.

        The enforcer is built lazily when the first cap-evaluating
        request hits ``get_caps_enforcer_dependency``; the lifespan
        must tolerate the brief pre-startup window where the singleton
        does not yet exist (or is unavailable for non-SQLAlchemy
        repos in tests).
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
            patch("snapper.server.app.discover_processes"),
            patch("snapper.server.app.ProcessLauncherService") as mock_factory_cls,
            patch("snapper.server.app.get_settings_service") as mock_get_settings_service,
            patch(
                "snapper.server.app._build_user_service_publisher",
                return_value=(MagicMock(), MagicMock()),
            ),
            patch("snapper.server.app._shutdown_user_service_publisher"),
            patch("snapper.server.app.get_user_service", return_value=MagicMock()),
            patch("snapper.server.app.get_scope_grant_service", return_value=MagicMock()),
            patch(
                "snapper.server.app.get_ai_review_service",
                return_value=_make_ai_review_service_mock(),
            ),
            patch("snapper.server.app._safe_get_caps_enforcer", return_value=None),
            patch(
                "snapper.server.app.get_ws_auth_manager",
                return_value=MagicMock(
                    start_admin_listener=AsyncMock(),
                    stop_admin_listener=AsyncMock(),
                    cancel_pending_offline_tasks=AsyncMock(),
                ),
            ),
            patch(
                "snapper.server.app.get_token_manager",
                return_value=MagicMock(
                    start_admin_listener=AsyncMock(),
                    stop_admin_listener=AsyncMock(),
                ),
            ),
        ):
            mock_settings_service = MagicMock()
            mock_settings_service.shutdown = AsyncMock()
            mock_get_settings_service.return_value = mock_settings_service
            mock_factory = MagicMock()
            mock_factory.sync_registry_to_database = AsyncMock()
            mock_factory.start_all_processes = AsyncMock()
            mock_factory.spawn_per_wallet_executors = AsyncMock(return_value=0)
            mock_factory.stop_all_processes = AsyncMock()
            mock_factory_cls.return_value = mock_factory
            async with lifespan(mock_app):
                pass

    @pytest.mark.asyncio
    async def test_lifespan_starts_and_stops_ai_review_bus_listener(self) -> None:
        """AiReviewService bus listener is started + stopped by lifespan.

        Lifespan must spin up the
        ``bus.delegate_offline`` + ``bus.caps_violation_after_ai_approve``
        subscriber so the handlers actually drain events in production.
        Pins the load-bearing relative ordering recorded into a single
        sequence so a regression that moves any of the four hooks
        fails this test:

        - ``set_msg_publisher`` (caps_violation handler publish path)
          MUST run BEFORE ``start_bus_listener`` so the first event
          the listener dispatches finds a non-None publisher slot.
        - ``set_repository_factory`` MUST run BEFORE
          ``start_bus_listener`` so the first ``bus.delegate_offline``
          frame finds a non-None factory.
        - ``stop_bus_listener`` MUST run BEFORE
          ``_shutdown_user_service_publisher`` (which clears the
          AiReviewService publisher slot) so an in-flight handler
          dispatched by the listener never observes a torn-down
          publisher.
        - ``set_repository_factory(None)`` runs AFTER
          ``stop_bus_listener`` so the listener cannot dispatch a
          new frame against a None factory.
        """
        mock_app = MagicMock()
        mock_manager = MagicMock()
        mock_manager.cleanup = AsyncMock()
        mock_zmq_bridge = MagicMock()
        mock_zmq_bridge.start = AsyncMock()
        mock_zmq_bridge.stop = AsyncMock()
        mock_manager.zmq_bridge = mock_zmq_bridge
        mock_app.state.manager = mock_manager
        recorded: list[str] = []
        mock_ai_review = _make_ai_review_service_mock()
        mock_ai_review.set_repository_factory.side_effect = lambda _f: recorded.append(
            "set_factory" if _f is not None else "clear_factory"
        )
        mock_ai_review.set_msg_publisher.side_effect = lambda _p: recorded.append(
            "set_publisher" if _p is not None else "clear_publisher"
        )

        async def _start(_xpub: str) -> None:
            recorded.append("start_bus_listener")

        async def _stop() -> None:
            recorded.append("stop_bus_listener")

        mock_ai_review.start_bus_listener = _start
        mock_ai_review.stop_bus_listener = _stop

        def _shutdown_publisher_recorder(_app: FastAPI) -> None:
            recorded.append("shutdown_publisher")

        with (
            patch("snapper.server.app.discover_processes"),
            patch("snapper.server.app.ProcessLauncherService") as mock_factory_cls,
            patch("snapper.server.app.get_settings_service") as mock_get_settings_service,
            patch(
                "snapper.server.app._build_user_service_publisher",
                return_value=(MagicMock(), MagicMock()),
            ),
            patch(
                "snapper.server.app._shutdown_user_service_publisher",
                side_effect=_shutdown_publisher_recorder,
            ),
            patch("snapper.server.app.get_user_service", return_value=MagicMock()),
            patch("snapper.server.app.get_scope_grant_service", return_value=MagicMock()),
            patch(
                "snapper.server.app.get_ai_review_service",
                return_value=mock_ai_review,
            ),
            patch(
                "snapper.server.app.get_ws_auth_manager",
                return_value=MagicMock(
                    start_admin_listener=AsyncMock(),
                    stop_admin_listener=AsyncMock(),
                    cancel_pending_offline_tasks=AsyncMock(),
                ),
            ),
            patch(
                "snapper.server.app.get_token_manager",
                return_value=MagicMock(
                    start_admin_listener=AsyncMock(),
                    stop_admin_listener=AsyncMock(),
                ),
            ),
        ):
            mock_settings_service = MagicMock()
            mock_settings_service.shutdown = AsyncMock()
            mock_get_settings_service.return_value = mock_settings_service
            mock_factory = MagicMock()
            mock_factory.sync_registry_to_database = AsyncMock()
            mock_factory.start_all_processes = AsyncMock()
            mock_factory.spawn_per_wallet_executors = AsyncMock(return_value=0)
            mock_factory.stop_all_processes = AsyncMock()
            mock_factory_cls.return_value = mock_factory
            async with lifespan(mock_app):
                pass
        publisher_idx = recorded.index("set_publisher")
        factory_idx = recorded.index("set_factory")
        start_idx = recorded.index("start_bus_listener")
        stop_idx = recorded.index("stop_bus_listener")
        shutdown_pub_idx = recorded.index("shutdown_publisher")
        clear_factory_idx = recorded.index("clear_factory")
        assert (
            publisher_idx < start_idx
        ), f"set_msg_publisher must precede start_bus_listener: {recorded}"
        assert (
            factory_idx < start_idx
        ), f"set_repository_factory must precede start_bus_listener: {recorded}"
        assert (
            stop_idx < shutdown_pub_idx
        ), f"stop_bus_listener must precede shutdown_publisher: {recorded}"
        assert (
            stop_idx < clear_factory_idx
        ), f"stop_bus_listener must precede set_repository_factory(None): {recorded}"

    @pytest.mark.asyncio
    async def test_lifespan_wires_ws_auth_manager_before_start_admin_listener(
        self,
    ) -> None:
        """``set_wiring`` MUST run BEFORE ``start_admin_listener``.

        Ordering is load-bearing — the admin subscriber
        must never receive an ``admin.scope_revoked`` event before the
        revalidation path (connection_manager + zmq_bridge +
        repository_factory) is ready, otherwise the handler would
        silently no-op with a warning and miss the fanout. This test
        pins the call order at the lifespan seam.
        """
        mock_app = MagicMock()
        mock_manager = MagicMock()
        mock_manager.cleanup = AsyncMock()
        mock_zmq_bridge = MagicMock()
        mock_zmq_bridge.start = AsyncMock()
        mock_zmq_bridge.stop = AsyncMock()
        mock_manager.zmq_bridge = mock_zmq_bridge
        mock_app.state.manager = mock_manager
        recorded: list[str] = []
        ws_auth_manager = MagicMock()
        ws_auth_manager.set_wiring = MagicMock(
            side_effect=lambda **_kwargs: recorded.append("set_wiring")
        )

        async def _start_listener(_xpub: str) -> None:
            recorded.append("ws_start_admin_listener")

        async def _stop_listener() -> None:
            recorded.append("ws_stop_admin_listener")

        ws_auth_manager.start_admin_listener = _start_listener
        ws_auth_manager.stop_admin_listener = _stop_listener
        ws_auth_manager.cancel_pending_offline_tasks = AsyncMock()

        with (
            patch("snapper.server.app.discover_processes"),
            patch(
                "snapper.server.app.get_ai_review_service",
                return_value=_make_ai_review_service_mock(),
            ),
            patch("snapper.server.app.ProcessLauncherService") as mock_factory_cls,
            patch("snapper.server.app.get_settings_service") as mock_get_settings_service,
            patch(
                "snapper.server.app._build_user_service_publisher",
                return_value=(MagicMock(), MagicMock()),
            ),
            patch("snapper.server.app._shutdown_user_service_publisher"),
            patch(
                "snapper.server.app.get_ws_auth_manager",
                return_value=ws_auth_manager,
            ),
            patch(
                "snapper.server.app.get_token_manager",
                return_value=MagicMock(
                    start_admin_listener=AsyncMock(),
                    stop_admin_listener=AsyncMock(),
                    cancel_pending_offline_tasks=AsyncMock(),
                ),
            ),
        ):
            mock_settings_service = MagicMock()
            mock_settings_service.shutdown = AsyncMock()
            mock_get_settings_service.return_value = mock_settings_service
            mock_factory = MagicMock()
            mock_factory.sync_registry_to_database = AsyncMock()
            mock_factory.start_all_processes = AsyncMock()
            mock_factory.spawn_per_wallet_executors = AsyncMock(return_value=0)
            mock_factory.stop_all_processes = AsyncMock()
            mock_factory_cls.return_value = mock_factory
            async with lifespan(mock_app):
                pass

        wiring_idx = recorded.index("set_wiring")
        listener_idx = recorded.index("ws_start_admin_listener")
        assert (
            wiring_idx < listener_idx
        ), f"set_wiring must precede start_admin_listener; recorded: {recorded}"
        ws_auth_manager.set_wiring.assert_called_once()
        call_kwargs = ws_auth_manager.set_wiring.call_args.kwargs
        assert call_kwargs["connection_manager"] is mock_manager
        assert call_kwargs["zmq_bridge"] is mock_zmq_bridge
        assert callable(call_kwargs["repository_factory"])

    @pytest.mark.asyncio
    async def test_lifespan_finally_runs_when_startup_raises_after_partial_init(
        self,
    ) -> None:
        """Shutdown hooks fire even when startup raises mid-init.

        In an earlier shape, publisher build + listener start ran
        BEFORE the `try` block. If `discover_processes` (or anything
        else after the publisher socket was opened) raised, the
        `finally` block was never reached → publisher socket + ZMQ
        context leaked.

        Fix: all setup work moved INSIDE the `try`, with state
        locals (`settings_service`, `process_factory`)
        pre-initialised to ``None`` so the `finally` can guard them.
        This test forces `discover_processes` to raise after the
        publisher + listener have been started, then asserts
        `_shutdown_user_service_publisher` and
        `stop_admin_listener` STILL ran during teardown.
        """
        mock_app = MagicMock()
        mock_manager = MagicMock()
        mock_manager.cleanup = AsyncMock()
        mock_zmq_bridge = MagicMock()
        mock_zmq_bridge.start = AsyncMock()
        mock_zmq_bridge.stop = AsyncMock()
        mock_manager.zmq_bridge = mock_zmq_bridge
        mock_app.state.manager = mock_manager
        recorded: list[str] = []
        mock_user_service = MagicMock()
        mock_user_service.set_msg_publisher.side_effect = lambda _value: recorded.append(
            "set_msg_publisher"
        )
        mock_ws_auth = MagicMock()

        async def _start_listener(_xpub: str) -> None:
            recorded.append("ws_start_admin_listener")

        async def _stop_listener() -> None:
            recorded.append("ws_stop_admin_listener")

        mock_ws_auth.start_admin_listener = _start_listener
        mock_ws_auth.stop_admin_listener = _stop_listener
        mock_ws_auth.cancel_pending_offline_tasks = AsyncMock()

        def _shutdown_publisher(_app: FastAPI) -> None:
            recorded.append("shutdown_publisher")

        def _explode_discover() -> None:
            recorded.append("discover_processes_raised")
            raise RuntimeError("autostart registry corrupt")

        with (
            patch(
                "snapper.server.app.discover_processes",
                side_effect=_explode_discover,
            ),
            patch("snapper.server.app.ProcessLauncherService"),
            patch("snapper.server.app.get_settings_service") as mock_get_settings_service,
            patch(
                "snapper.server.app._build_user_service_publisher",
                return_value=(MagicMock(), MagicMock()),
            ),
            patch(
                "snapper.server.app._shutdown_user_service_publisher",
                side_effect=_shutdown_publisher,
            ),
            patch(
                "snapper.server.app.get_user_service",
                return_value=mock_user_service,
            ),
            patch(
                "snapper.server.app.get_ws_auth_manager",
                return_value=mock_ws_auth,
            ),
            patch(
                "snapper.server.app.get_token_manager",
                return_value=_make_token_listener_recorder(recorded),
            ),
        ):
            mock_settings_service = MagicMock()
            mock_settings_service.shutdown = AsyncMock()
            mock_get_settings_service.return_value = mock_settings_service
            with pytest.raises(RuntimeError, match="autostart registry corrupt"):
                async with lifespan(mock_app):
                    pass
        assert "discover_processes_raised" in recorded
        assert "ws_stop_admin_listener" in recorded
        assert "tm_stop_admin_listener" in recorded
        assert "shutdown_publisher" in recorded
        assert mock_settings_service.shutdown.await_count == 1

    @pytest.mark.asyncio
    async def test_lifespan_finally_tolerates_settings_service_init_failure(
        self,
    ) -> None:
        """Finally runs even if settings_service init fails.

        Worst-case partial init: `_initialize_settings_service`
        itself raises (e.g. DB unreachable on boot). At that point
        `settings_service` is still ``None``, `process_factory`
        is still ``None``, and `app.state.manager` may also be
        unattached. The `finally` MUST tolerate every missing
        state without raising AttributeError or NoneType.

        `mock_app.state` is constructed with `spec=[]` so attribute
        access for `manager` (and other state attrs the lifespan
        might read) raises AttributeError unless explicitly set —
        forcing the `getattr(..., None)` guards to be exercised.
        """
        mock_app_state = MagicMock(spec=[])
        mock_app = MagicMock()
        mock_app.state = mock_app_state
        with (
            patch(
                "snapper.server.app._initialize_settings_service",
                new=AsyncMock(side_effect=RuntimeError("db unreachable")),
            ),
            patch("snapper.server.app._build_user_service_publisher"),
            patch("snapper.server.app._shutdown_user_service_publisher") as mock_shutdown_pub,
            patch(
                "snapper.server.app.get_ws_auth_manager",
                return_value=MagicMock(
                    start_admin_listener=AsyncMock(),
                    stop_admin_listener=AsyncMock(),
                    cancel_pending_offline_tasks=AsyncMock(),
                ),
            ),
            patch(
                "snapper.server.app.get_token_manager",
                return_value=MagicMock(
                    start_admin_listener=AsyncMock(),
                    stop_admin_listener=AsyncMock(),
                    cancel_pending_offline_tasks=AsyncMock(),
                ),
            ),
            pytest.raises(RuntimeError, match="db unreachable"),
        ):
            async with lifespan(mock_app):
                pass
        mock_shutdown_pub.assert_called_once_with(mock_app)

    @pytest.mark.asyncio
    async def test_lifespan_api_only_skips_engine(self) -> None:
        """Test api-only mode skips process autostart but starts bridge.

        Given: SERVER_API_ONLY=true in settings,
        When: Lifespan runs,
        Then: start_all_processes is not called, bridge still starts.
        """
        mock_app = MagicMock()
        mock_manager = MagicMock()
        mock_manager.cleanup = AsyncMock()
        mock_zmq_bridge = MagicMock()
        mock_zmq_bridge.start = AsyncMock()
        mock_zmq_bridge.stop = AsyncMock()
        mock_manager.zmq_bridge = mock_zmq_bridge
        mock_app.state.manager = mock_manager
        api_only_settings = MagicMock()
        api_only_settings.server_api_only = True
        api_only_settings.coordinator_instance_id = 0
        api_only_settings.coordinator_instance_count = 1
        with (
            patch("snapper.server.app.discover_processes"),
            patch(
                "snapper.server.app.get_ai_review_service",
                return_value=_make_ai_review_service_mock(),
            ),
            patch("snapper.server.app.ProcessLauncherService") as mock_factory_cls,
            patch("snapper.server.app.get_settings_service") as mock_get_settings_service,
            patch(
                "snapper.server.app.get_settings_with_service",
                return_value=api_only_settings,
            ),
            patch(
                "snapper.server.app._build_user_service_publisher",
                return_value=(MagicMock(), MagicMock()),
            ),
            patch("snapper.server.app._shutdown_user_service_publisher"),
            patch(
                "snapper.server.app.MarketPersistPolicy",
                return_value=MagicMock(
                    initial_rebuild=AsyncMock(),
                    start_admin_listener=AsyncMock(),
                    stop=AsyncMock(),
                ),
            ),
            patch(
                "snapper.server.app.MarketCacheService",
                return_value=MagicMock(
                    start=AsyncMock(),
                    stop=AsyncMock(),
                ),
            ),
            patch(
                "snapper.server.app.MarketStatsWorker",
                return_value=MagicMock(
                    start=AsyncMock(),
                    stop=AsyncMock(),
                ),
            ),
            patch("snapper.server.app.get_repository"),
            patch(
                "snapper.server.app.get_ws_auth_manager",
                return_value=MagicMock(
                    start_admin_listener=AsyncMock(),
                    stop_admin_listener=AsyncMock(),
                    cancel_pending_offline_tasks=AsyncMock(),
                ),
            ),
            patch(
                "snapper.server.app.get_token_manager",
                return_value=MagicMock(
                    start_admin_listener=AsyncMock(),
                    stop_admin_listener=AsyncMock(),
                    cancel_pending_offline_tasks=AsyncMock(),
                ),
            ),
        ):
            mock_settings_service = MagicMock()
            mock_settings_service.shutdown = AsyncMock()
            mock_get_settings_service.return_value = mock_settings_service
            mock_factory = MagicMock()
            mock_factory.sync_registry_to_database = AsyncMock()
            mock_factory.start_all_processes = AsyncMock()
            mock_factory.spawn_per_wallet_executors = AsyncMock(return_value=0)
            mock_factory.stop_all_processes = AsyncMock()
            mock_factory_cls.return_value = mock_factory
            async with lifespan(mock_app):
                pass
        mock_factory.sync_registry_to_database.assert_awaited_once()
        mock_factory.start_all_processes.assert_not_awaited()
        mock_zmq_bridge.start.assert_awaited_once()

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
            patch(
                "snapper.server.app._build_user_service_publisher",
                return_value=(MagicMock(), MagicMock()),
            ),
            patch("snapper.server.app._shutdown_user_service_publisher"),
            patch(
                "snapper.server.app.get_ws_auth_manager",
                return_value=MagicMock(
                    start_admin_listener=AsyncMock(),
                    stop_admin_listener=AsyncMock(),
                    cancel_pending_offline_tasks=AsyncMock(),
                ),
            ),
            patch(
                "snapper.server.app.get_token_manager",
                return_value=MagicMock(
                    start_admin_listener=AsyncMock(),
                    stop_admin_listener=AsyncMock(),
                    cancel_pending_offline_tasks=AsyncMock(),
                ),
            ),
        ):
            mock_settings_service = MagicMock()
            mock_get_settings_service.return_value = mock_settings_service
            mock_factory = MagicMock()
            mock_factory.sync_registry_to_database = AsyncMock()
            mock_factory.start_all_processes = AsyncMock()
            mock_factory.spawn_per_wallet_executors = AsyncMock(return_value=0)
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
        mount_paths = [call.args[0] for call in mock_mount.call_args_list]
        assert "/" in mount_paths
        assert "/api/mcp" in mount_paths

    def test_create_app_skips_static_when_missing(self) -> None:
        """Test static files mounting is skipped when directory missing.

        Given: Static directory does not exist,
        When: create_app is called,
        Then: The only mount is the unconditional MCP sub-app; the
            ``/`` static mount is skipped per the ``os.path.exists``
            False branch. ``/api/mcp`` is always mounted because the
            flag-off semantics are implemented inside the
            sub-app, not at mount time.
        """
        with (
            patch("snapper.server.app.os.path.exists", return_value=False),
            patch.object(FastAPI, "mount") as mock_mount,
        ):
            create_app()
        mount_paths = [call.args[0] for call in mock_mount.call_args_list]
        assert mount_paths == ["/api/mcp"]

    def test_safe_get_caps_enforcer_returns_none_pre_lifespan(self) -> None:
        """``_safe_get_caps_enforcer`` swallows pre-lifespan RuntimeError.

        Given: the caps dependency raises RuntimeError (lifespan
            hasn't attached a SQLAlchemyRepository to the cache yet),
        When: the MCP-tool-safe wrapper is invoked,
        Then: ``None`` is returned — individual tools raise a clearer
            "lifespan not ready" error at invocation time rather than
            the raw RuntimeError leaking out of mount construction.
        """
        reset_caps_enforcer_singleton()
        with patch("snapper.server.app.get_caps_enforcer_dependency") as mock_dep:
            mock_dep.side_effect = RuntimeError("Not a SQLAlchemyRepository")
            assert _safe_get_caps_enforcer() is None

    def test_build_user_service_publisher_connects_to_broker_xsub(self) -> None:
        """Helper connects PUB socket to broker's XSUB endpoint.

        Connecting the PUB socket to ``zmq_broker_xpub``
        (subscriber-facing) silently drops every message — publishers
        MUST connect to the broker's XSUB side per the proxy contract
        documented in :class:`BootstrapSettingsLoader` and
        :class:`ZmqBrokerProcess`. The lifespan passes
        ``settings.zmq_broker_xsub`` so the kill-switch
        ``admin.user_deactivated`` event actually reaches subscribers.

        This test pins the endpoint so a future refactor that
        accidentally swaps the value back to ``zmq_broker_xpub``
        fails CI loudly instead of silently breaking the kill switch.
        """
        with patch(
            "snapper.messaging.infrastructure.publisher.zmq.asyncio.Context"
        ) as mock_context_cls:
            mock_socket = MagicMock()
            mock_context = MagicMock()
            mock_context.socket.return_value = mock_socket
            mock_context_cls.return_value = mock_context
            publisher, context = _build_user_service_publisher("tcp://broker:7500")
        assert context is mock_context
        mock_context.socket.assert_called_once()
        mock_socket.connect.assert_called_once_with("tcp://broker:7500")
        assert publisher is not None

    def test_shutdown_user_service_publisher_closes_socket_and_clears_singleton(
        self,
    ) -> None:
        """Shutdown helper releases the publisher and clears the service ref.

        Given: a FastAPI app with a publisher + context attached,
        When: the shutdown helper runs,
        Then: (a) the publisher is closed; (b) the context is
            terminated; (c) the UserService singleton's publisher ref
            is cleared so any in-flight `deactivate_user` call after
            shutdown observes a None publisher (graceful degradation).
        """
        mock_publisher = MagicMock()
        mock_context = MagicMock()
        mock_user_service = MagicMock()
        mock_app = MagicMock()
        mock_app.state.user_service_publisher = mock_publisher
        mock_app.state.user_service_publisher_context = mock_context
        with patch("snapper.server.app.get_user_service", return_value=mock_user_service):
            _shutdown_user_service_publisher(mock_app)
        mock_user_service.set_msg_publisher.assert_called_once_with(None)
        mock_publisher.close.assert_called_once()
        mock_context.term.assert_called_once()
        assert mock_app.state.user_service_publisher is None
        assert mock_app.state.user_service_publisher_context is None

    def test_shutdown_user_service_publisher_singleton_clear_before_close_before_term(
        self,
    ) -> None:
        """Disposal order MUST be singleton-clear → publisher.close → context.term.

        An in-flight `deactivate_user` call MUST observe a None
        publisher BEFORE the socket is closed; the socket MUST be closed
        BEFORE the context terminates so libzmq sees a clean LINGER
        cycle. An earlier test verified each call happened but did not
        pin relative order — this test does.
        """
        recorded: list[str] = []
        mock_publisher = MagicMock()
        mock_publisher.close.side_effect = lambda: recorded.append("close")
        mock_publisher.setsockopt.side_effect = lambda *_args: None
        mock_context = MagicMock()
        mock_context.term.side_effect = lambda: recorded.append("term")
        mock_user_service = MagicMock()
        mock_user_service.set_msg_publisher.side_effect = lambda _value: recorded.append(
            "set_msg_publisher"
        )
        mock_app = MagicMock()
        mock_app.state.user_service_publisher = mock_publisher
        mock_app.state.user_service_publisher_context = mock_context
        with patch("snapper.server.app.get_user_service", return_value=mock_user_service):
            _shutdown_user_service_publisher(mock_app)
        assert recorded == ["set_msg_publisher", "close", "term"]

    def test_shutdown_user_service_publisher_handles_missing_state(self) -> None:
        """Shutdown is a no-op-on-failure when state was never attached.

        Given: an app whose state has neither publisher nor context
            (lifespan startup raised before attaching them),
        When: the shutdown helper runs,
        Then: no exception escapes; the singleton is still cleared.
        """
        mock_user_service = MagicMock()
        mock_app = MagicMock()
        mock_app.state.user_service_publisher = None
        mock_app.state.user_service_publisher_context = None
        with patch("snapper.server.app.get_user_service", return_value=mock_user_service):
            _shutdown_user_service_publisher(mock_app)
        mock_user_service.set_msg_publisher.assert_called_once_with(None)

    def test_clear_runtime_singletons_continues_after_clear_error(self) -> None:
        """Test runtime singleton cleanup continues after one clear_instance failure.

        Given: One singleton clear operation raises an exception,
        When: runtime singletons are cleared,
        Then: later singleton clear operations still run.
        """
        with (
            patch("snapper.server.app.SymbolMapperService") as mock_symbol_mapper,
            patch("snapper.server.app.WebSocketAuthManager") as mock_ws_auth,
            patch("snapper.server.app.WebSocketTokenRotator") as mock_token_rotator,
            patch("snapper.server.app.UserService") as mock_user_service,
            patch("snapper.server.app.WsTokenService") as mock_ws_token_service,
            patch("snapper.server.app.CSRFManager") as mock_csrf_manager,
            patch("snapper.server.app.TokenManager") as mock_token_manager,
            patch("snapper.server.app.SettingsService") as mock_settings_service,
        ):
            mock_ws_auth.clear_instance.side_effect = RuntimeError("boom")

            _clear_runtime_singletons()

        mock_symbol_mapper.clear_instance.assert_called_once()
        mock_ws_auth.clear_instance.assert_called_once()
        mock_token_rotator.clear_instance.assert_called_once()
        mock_user_service.clear_instance.assert_called_once()
        mock_ws_token_service.clear_instance.assert_called_once()
        mock_csrf_manager.clear_instance.assert_called_once()
        mock_token_manager.clear_instance.assert_called_once()
        mock_settings_service.clear_instance.assert_called_once()


class TestCreateApiRouter:
    """Tests for API router creation and endpoint accessibility."""

    def setup_method(self) -> None:
        """Initialize test client with dependency overrides."""
        self.app = create_app()

        def skip_csrf_validation() -> None:
            return None

        def skip_authentication() -> AuthPrincipal:
            return AuthPrincipal(username="test_user", role=UserRole.ADMIN)

        self.app.dependency_overrides[validate_csrf_token] = skip_csrf_validation
        self.app.dependency_overrides[require_authentication] = skip_authentication
        self.app.state.process_factory = _build_mock_process_factory({})
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
    @patch("snapper.server.dependencies.get_repository")
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
        assert data["payload"]["status"] == "healthy"
        assert "timestamp" in data

    def test_get_candles_success(self) -> None:
        """Test candles endpoint returns OHLCV data.

        Given: A valid instrument with candle data in repository,
        When: GET /api/candles is called with parameters,
        Then: Response contains candle data array with OHLCV values.
        """
        candle_rows = [
            {
                "public_id": "candle-uuid-1234",
                "timestamp": datetime(2023, 1, 1, 12, 0, tzinfo=dt.UTC),
                "session_id": "sess-1",
                "sequence_id": 1,
                "timeframe": "1h",
                "open_at": datetime(2023, 1, 1, 12, 0, tzinfo=dt.UTC),
                "open": 50000.0,
                "high": 51000.0,
                "low": 49000.0,
                "close": 50500.0,
                "volume": 1000.0,
                "vwap": 50250.0,
                "trades": 10,
            }
        ]
        repo = MockRepository(session_result=candle_rows)
        client = create_app_with_overrides(repo)
        response = client.get(
            "/api/candles?instrument=BTC-USD&exchange=kraken&timeframe=1h&limit=10"
        )
        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "candle_list"
        assert data["count"] == 1
        items = data["payload"]
        assert isinstance(items, list)
        assert len(items) == 1
        assert items[0]["instrument"] == "BTC-USD"
        assert items[0]["timeframe"] == "1h"
        assert items[0]["open"] == pytest.approx(50000.0)
        assert items[0]["close"] == pytest.approx(50500.0)
        assert items[0]["timestamp"] is not None

    def test_get_candles_market_time_range_returns_window(self) -> None:
        """Test candles endpoint serves a market-time ``open_at`` window.

        Given: A valid instrument with candle data in repository,
        When: GET /api/candles is called with paired ``start`` and ``end``,
        Then: Response returns the range read (the scrubber's DB window).
        """
        candle_rows = [
            {
                "public_id": "candle-uuid-range",
                "timestamp": datetime(2023, 1, 2, tzinfo=dt.UTC),
                "session_id": "sess-1",
                "sequence_id": 1,
                "timeframe": "1d",
                "open_at": datetime(2023, 1, 2, tzinfo=dt.UTC),
                "open": 50000.0,
                "high": 51000.0,
                "low": 49000.0,
                "close": 50500.0,
                "volume": 1000.0,
                "vwap": 50250.0,
                "trades": 10,
            }
        ]
        repo = MockRepository(session_result=candle_rows)
        client = create_app_with_overrides(repo)
        response = client.get(
            "/api/candles?instrument=BTC-USD&exchange=kraken&timeframe=1d"
            "&start=2023-01-01T00:00:00Z&end=2023-01-04T00:00:00Z"
        )
        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "candle_list"
        assert data["count"] == 1
        assert data["payload"][0]["timeframe"] == "1d"

    def test_get_candles_range_requires_both_bounds(self) -> None:
        """Test candles endpoint rejects a half-specified range.

        Given: A running app,
        When: GET /api/candles is called with ``start`` but no ``end``,
        Then: Response is 400 (start and end must be paired).
        """
        repo = MockRepository(session_result=[])
        client = create_app_with_overrides(repo)
        response = client.get(
            "/api/candles?instrument=BTC-USD&exchange=kraken&timeframe=1d"
            "&start=2023-01-01T00:00:00Z"
        )
        assert response.status_code == 400

    def test_get_candles_range_rejects_inverted_window(self) -> None:
        """Test candles endpoint rejects a range whose start is not before end.

        Given: A running app,
        When: GET /api/candles is called with ``start`` >= ``end``,
        Then: Response is 400 (malformed range).
        """
        repo = MockRepository(session_result=[])
        client = create_app_with_overrides(repo)
        response = client.get(
            "/api/candles?instrument=BTC-USD&exchange=kraken&timeframe=1d"
            "&start=2023-01-04T00:00:00Z&end=2023-01-01T00:00:00Z"
        )
        assert response.status_code == 400

    def test_get_candles_no_data_returns_empty_array(self) -> None:
        """Test candles endpoint returns empty array when no data.

        Given: A valid instrument with no candle data,
        When: GET /api/candles is called,
        Then: Response contains an empty array.
        """
        repo = MockRepository(session_result=[])
        client = create_app_with_overrides(repo)
        response = client.get(
            "/api/candles?instrument=BTC-USD&exchange=kraken&timeframe=1h&limit=10"
        )
        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "candle_list"
        assert data["count"] == 0
        assert data["payload"] == []

    def test_get_candles_symbol_not_found_returns_empty_payload(self) -> None:
        """Test candles endpoint returns empty payload when Symbol is missing.

        Given: No active Symbol row for the requested instrument,
        When: GET /api/candles is called,
        Then: Response status is 200 with empty payload list.
        """
        repo = MockRepository(session_result=[])
        client = create_app_with_overrides(repo)
        response = client.get("/api/candles?instrument=NOSYMBOL&exchange=kraken&timeframe=1h")
        assert response.status_code == 200
        data = response.json()
        assert data["payload"] == []
        assert data["count"] == 0

    def test_get_candles_instrument_not_found_returns_empty_payload(self) -> None:
        """Test candles endpoint returns empty payload for unknown instrument.

        Given: Symbol exists but no matching Instrument in the database,
        When: GET /api/candles is called,
        Then: Response status is 200 with empty payload list.
        """
        repo = MockRepository(session_result=[])
        client = create_app_with_overrides(repo)
        response = client.get("/api/candles?instrument=INVALID&exchange=kraken&timeframe=1h")
        assert response.status_code == 200
        data = response.json()
        assert data["payload"] == []
        assert data["count"] == 0

    def test_get_candles_db_returns_same_shape_as_smart_route(self) -> None:
        """``/api/candles/db`` returns the same envelope shape as ``/api/candles``.

        Given: A valid instrument with one candle row in repository,
        When: GET /api/candles/db is called,
        Then: Response is a CandleListResponse with one row, exactly
              matching the legacy /api/candles contract.
        """
        candle_rows = [
            {
                "public_id": "candle-uuid-db-1",
                "timestamp": datetime(2023, 1, 1, 12, 0, tzinfo=dt.UTC),
                "session_id": "sess-db",
                "sequence_id": 7,
                "timeframe": "1h",
                "open_at": datetime(2023, 1, 1, 12, 0, tzinfo=dt.UTC),
                "open": 100.0,
                "high": 110.0,
                "low": 95.0,
                "close": 105.0,
                "volume": 50.0,
                "vwap": None,
                "trades": None,
            }
        ]
        repo = MockRepository(session_result=candle_rows)
        client = create_app_with_overrides(repo)
        response = client.get(
            "/api/candles/db?instrument=BTC-USD&exchange=kraken&timeframe=1h&limit=10"
        )
        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "candle_list"
        assert data["count"] == 1
        assert data["payload"][0]["timeframe"] == "1h"
        assert data["payload"][0]["public_id"] == "candle-uuid-db-1"

    def test_get_candles_db_empty_payload_when_no_rows(self) -> None:
        """``/api/candles/db`` returns empty payload for unknown instrument.

        Given: No DB rows match the requested instrument,
        When: GET /api/candles/db is called,
        Then: Response is 200 with empty payload list.
        """
        repo = MockRepository(session_result=[])
        client = create_app_with_overrides(repo)
        response = client.get("/api/candles/db?instrument=NOTHING&exchange=kraken&timeframe=1d")
        assert response.status_code == 200
        data = response.json()
        assert data["payload"] == []
        assert data["count"] == 0

    def test_get_candles_cache_returns_503_when_cache_not_initialized(self) -> None:
        """``/api/candles/cache`` raises 503 when cache is not wired.

        Given: TestClient setup has no market_cache attached to app.state,
        When: GET /api/candles/cache is called with a cache-eligible
              timeframe (1m),
        Then: Response is 503 SERVICE_UNAVAILABLE.
        """
        repo = MockRepository(session_result=[])
        client = create_app_with_overrides(repo)
        response = client.get(
            "/api/candles/cache?instrument=BTC-USD&exchange=kraken&timeframe=1m&limit=10"
        )
        assert response.status_code == 503

    def test_get_candles_cache_falls_back_to_db_for_long_frames(self) -> None:
        """``/api/candles/cache`` serves 1h frames from DB with ``source='db'``.

        Given: Cache not wired but DB has a 1h row,
        When: GET /api/candles/cache is called with timeframe=1h,
        Then: Response is 200 with ``payload.source='db'`` and one candle.
        """
        candle_rows = [
            {
                "public_id": "candle-uuid-cache-fallback",
                "timestamp": datetime(2023, 1, 1, 12, 0, tzinfo=dt.UTC),
                "session_id": "sess-fb",
                "sequence_id": 1,
                "timeframe": "1h",
                "open_at": datetime(2023, 1, 1, 12, 0, tzinfo=dt.UTC),
                "open": 50.0,
                "high": 55.0,
                "low": 45.0,
                "close": 52.0,
                "volume": 10.0,
                "vwap": None,
                "trades": None,
            }
        ]
        repo = MockRepository(session_result=candle_rows)
        client = create_app_with_overrides(repo)
        response = client.get(
            "/api/candles/cache?instrument=BTC-USD&exchange=kraken&timeframe=1h&limit=10"
        )
        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "cached_candles"
        assert data["payload"]["source"] == "db"
        assert data["payload"]["sample_count"] == 1

    def test_get_candles_cache_rejects_invalid_timeframe(self) -> None:
        """``/api/candles/cache`` raises 400 for unsupported timeframes.

        Given: A timeframe outside the supported seven,
        When: GET /api/candles/cache is called,
        Then: Response is 400 with a descriptive detail.
        """
        repo = MockRepository(session_result=[])
        client = create_app_with_overrides(repo)
        response = client.get(
            "/api/candles/cache?instrument=BTC-USD&exchange=kraken&timeframe=2m&limit=10"
        )
        assert response.status_code == 400

    def test_get_candles_rejects_invalid_timeframe(self) -> None:
        """``/api/candles`` raises 400 for unsupported timeframes.

        Symmetry with ``/api/candles/cache``: unknown frames are
        rejected at the route boundary rather than passed silently
        to the repo.
        """
        repo = MockRepository(session_result=[])
        client = create_app_with_overrides(repo)
        response = client.get(
            "/api/candles?instrument=BTC-USD&exchange=kraken&timeframe=2m&limit=10"
        )
        assert response.status_code == 400

    def test_get_candles_db_rejects_invalid_timeframe(self) -> None:
        """``/api/candles/db`` raises 400 for unsupported timeframes."""
        repo = MockRepository(session_result=[])
        client = create_app_with_overrides(repo)
        response = client.get(
            "/api/candles/db?instrument=BTC-USD&exchange=kraken&timeframe=2m&limit=10"
        )
        assert response.status_code == 400

    def test_get_candles_rejects_zero_limit(self) -> None:
        """``/api/candles`` enforces ``limit >= 1`` via Query validation."""
        repo = MockRepository(session_result=[])
        client = create_app_with_overrides(repo)
        response = client.get(
            "/api/candles?instrument=BTC-USD&exchange=kraken&timeframe=1m&limit=0"
        )
        assert response.status_code == 422

    def test_get_candles_db_rejects_zero_limit(self) -> None:
        """``/api/candles/db`` enforces ``limit >= 1`` via Query validation."""
        repo = MockRepository(session_result=[])
        client = create_app_with_overrides(repo)
        response = client.get(
            "/api/candles/db?instrument=BTC-USD&exchange=kraken&timeframe=1m&limit=0"
        )
        assert response.status_code == 422

    def test_get_candles_returns_500_on_repo_failure(self) -> None:
        """``/api/candles`` maps repo exceptions to HTTP 500 with a generic detail.

        Given: Repository raises during ``get_candles``,
        When: GET /api/candles is called,
        Then: Response is 500 with a redacted error detail (no internal
              exception leakage).
        """
        repo = MockRepository(session_result=[], error=RuntimeError("boom"))
        client = create_app_with_overrides(repo)
        response = client.get(
            "/api/candles?instrument=BTC-USD&exchange=kraken&timeframe=1m&limit=10"
        )
        assert response.status_code == 500
        assert "Failed to fetch candle data" in response.text

    def test_get_candles_db_returns_500_on_repo_failure(self) -> None:
        """``/api/candles/db`` maps repo exceptions to HTTP 500."""
        repo = MockRepository(session_result=[], error=RuntimeError("boom"))
        client = create_app_with_overrides(repo)
        response = client.get(
            "/api/candles/db?instrument=BTC-USD&exchange=kraken&timeframe=1m&limit=10"
        )
        assert response.status_code == 500
        assert "Failed to fetch candle data" in response.text

    def test_get_candles_cache_returns_500_on_db_fallback_failure(self) -> None:
        """``/api/candles/cache`` maps DB-fallback exceptions to HTTP 500.

        For long timeframes (1h/4h/1d) the cache route falls back to
        the persisted ``candles`` table because the in-process 100-bar
        deque can't synthesise them. An exception in that fallback path
        previously bypassed the redacted 500 mapping used by the
        adjacent candle routes; the generic exception handler closes
        that hole.
        """
        repo = MockRepository(session_result=[], error=RuntimeError("boom"))
        client = create_app_with_overrides(repo)
        response = client.get(
            "/api/candles/cache?instrument=BTC-USD&exchange=kraken&timeframe=1h&limit=10"
        )
        assert response.status_code == 500
        assert "Failed to fetch cache candle data" in response.text

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
        mock_process_factory = _build_mock_process_factory(
            {
                "strategy_macd_btc_1h": mock_strategy_process,
                "zmq_broker": mock_other_process,
                "executor": mock_no_status_process,
            }
        )
        with patch("snapper.server.app.ProcessLauncherService", return_value=mock_process_factory):
            self.app.state.process_factory = mock_process_factory
            response = self.client.get("/api/status")
        assert response.status_code == 200
        data = response.json()
        payload = data["payload"]
        assert "trader" in payload
        assert payload["trader"]["status"] == "not_running"
        assert "backtests" in payload
        assert payload["backtests"] == {}
        assert "strategies" in payload
        assert len(payload["strategies"]) == 1
        assert payload["strategies"][0]["strategy_name"] == "macd_btc_1h"
        assert payload["strategies"][0]["status"] == "running"
        assert payload["strategies"][0]["signals_generated"] == 42


class TestCandleProjectionHelpers:
    """Tests for ``project_query_row_to_candle_data`` + ``project_query_row_to_cached_candle``.

    Verifies that cache-sourced rows (provenance = ``None``) get
    deterministic synthetic provenance (same logical bar → same
    ``public_id`` across calls), and that DB-sourced rows (provenance
    populated) pass their original identity through.
    """

    def _cache_row(self, open_at_ms: int) -> CandleQueryRow:
        """Build a cache-sourced query row with ``None`` provenance."""
        return CandleQueryRow(
            open_at=datetime.fromtimestamp(open_at_ms / 1000, tz=dt.UTC),
            timeframe="1m",
            open=100.0,
            high=110.0,
            low=95.0,
            close=105.0,
            volume=10.0,
            vwap=None,
            trades=None,
            public_id=None,
            timestamp=None,
            session_id=None,
            sequence_id=None,
        )

    def _db_row(self, open_at_ms: int) -> CandleQueryRow:
        """Build a DB-sourced query row with original provenance."""
        return CandleQueryRow(
            open_at=datetime.fromtimestamp(open_at_ms / 1000, tz=dt.UTC),
            timeframe="1h",
            open=200.0,
            high=210.0,
            low=195.0,
            close=205.0,
            volume=20.0,
            vwap=202.5,
            trades=42,
            public_id="original-public-id",
            timestamp=datetime.fromtimestamp(open_at_ms / 1000, tz=dt.UTC),
            session_id="original-session",
            sequence_id=99,
        )

    def test_cache_row_synthetic_provenance_is_deterministic(self) -> None:
        """Same logical 1m bar → same ``public_id`` across two calls."""
        row = self._cache_row(60_000)
        first = project_query_row_to_candle_data(row, instrument="BTC-USD", exchange="kraken")
        second = project_query_row_to_candle_data(row, instrument="BTC-USD", exchange="kraken")
        assert first.public_id == second.public_id
        assert first.sequence_id == 60_000
        assert second.sequence_id == 60_000
        assert first.session_id == _CACHE_CANDLE_SESSION_ID

    def test_cache_row_different_open_at_yields_different_public_id(self) -> None:
        """Bars at different ``open_at_ms`` mint distinct synthetic ids."""
        a = project_query_row_to_candle_data(
            self._cache_row(60_000), instrument="BTC-USD", exchange="kraken"
        )
        b = project_query_row_to_candle_data(
            self._cache_row(120_000), instrument="BTC-USD", exchange="kraken"
        )
        assert a.public_id != b.public_id

    def test_cache_row_different_instrument_yields_different_public_id(self) -> None:
        """Same ``open_at_ms`` on a different instrument mints a distinct id."""
        a = project_query_row_to_candle_data(
            self._cache_row(60_000), instrument="BTC-USD", exchange="kraken"
        )
        b = project_query_row_to_candle_data(
            self._cache_row(60_000), instrument="ETH-USD", exchange="kraken"
        )
        assert a.public_id != b.public_id

    def test_cache_row_timestamp_equals_open_at(self) -> None:
        """Cache row's ``timestamp`` field tracks the bar's domain open."""
        row = self._cache_row(180_000)
        candle = project_query_row_to_candle_data(row, instrument="BTC-USD", exchange="kraken")
        assert candle.timestamp == row.open_at

    def test_db_row_preserves_original_provenance(self) -> None:
        """DB-sourced rows pass their original identity unmodified."""
        row = self._db_row(3_600_000)
        candle = project_query_row_to_candle_data(row, instrument="BTC-USD", exchange="kraken")
        assert candle.public_id == "original-public-id"
        assert candle.session_id == "original-session"
        assert candle.sequence_id == 99
        assert candle.vwap == pytest.approx(202.5)
        assert candle.trades == 42

    def test_cached_candle_projection_drops_provenance(self) -> None:
        """``CachedCandle`` is provenance-free OHLCV + ``open_at_ms``."""
        row = self._cache_row(240_000)
        candle = project_query_row_to_cached_candle(row)
        assert candle.open_at_ms == 240_000
        assert candle.timeframe == "1m"
        assert candle.open == pytest.approx(100.0)
        assert candle.close == pytest.approx(105.0)


class TestWebSocketEndpoints:
    """Tests for WebSocket-related API endpoints."""

    def setup_method(self) -> None:
        """Initialize test client with dependency overrides."""
        self.app = create_app()

        def skip_csrf_validation() -> None:
            return None

        def skip_authentication() -> AuthPrincipal:
            return AuthPrincipal(username="test_user", role=UserRole.ADMIN)

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
        assert data["payload"]["status"] == "healthy"
        assert "timestamp" in data
        assert data["payload"]["components"]["websocket_manager"] == "ok"

    def test_rest_rate_metrics_empty_snapshot(self) -> None:
        """REST rate endpoint returns empty exchange map on a fresh process.

        Given:
            No REST calls have been recorded since the tracker reset,

        When:
            GET /api/metrics/rest-rate is called,

        Then:
            Response is 200 with ``payload.exchanges == {}`` — the
            tracker only materialises an exchange entry after the
            first recorded call.
        """
        reset_rest_call_tracker_for_tests()
        response = self.client.get("/api/metrics/rest-rate")
        assert response.status_code == 200
        data = response.json()
        assert data["payload"]["type"] == "rest_rate"
        assert data["payload"]["exchanges"] == {}

    def test_rest_rate_metrics_reports_per_exchange(self) -> None:
        """REST rate endpoint projects tracker snapshot into typed envelope.

        Given:
            The tracker has recorded 3 calls for Walutomat and 0 for
            all other exchanges,

        When:
            GET /api/metrics/rest-rate is called,

        Then:
            Response payload has a single ``walutomat`` entry with
            ``rps_1s >= 3.0``, ``limit_rps == 20.0``, and
            ``utilization == rps_1s / 20.0``.
        """
        reset_rest_call_tracker_for_tests()
        tracker = get_rest_call_tracker()
        with patch(
            "snapper.infrastructure.rest.tracker.time.monotonic",
            return_value=100.0,
        ):
            for _ in range(3):
                tracker.record_call(ExchangeEnum.WALUTOMAT)
            response = self.client.get("/api/metrics/rest-rate")
        assert response.status_code == 200
        payload = response.json()["payload"]
        assert set(payload["exchanges"]) == {ExchangeEnum.WALUTOMAT}
        wlm = payload["exchanges"][ExchangeEnum.WALUTOMAT]
        assert wlm["rps_1s"] >= 3.0
        assert wlm["limit_rps"] == 20.0
        assert wlm["utilization"] == wlm["rps_1s"] / 20.0


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
        self.app.state.process_factory = _build_mock_process_factory({})
        self.client = TestClient(self.app)

    @patch("snapper.server.app.get_settings")
    @patch("snapper.server.dependencies.get_repository")
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

    @patch("snapper.server.dependencies.get_repository")
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
    """Mock repository for testing REST endpoints.

    Provides async read methods matching the Repository contract.
    Accepts ORM-like mock tuples and converts them to dicts.
    """

    def __init__(
        self,
        session_result: Any = None,
        error: Exception | None = None,
        account_state_rows: list[VenueAccountStateRow] | None = None,
        accessible_wallet_ids: list[str] | None = None,
        duplicate_active_rows: bool = False,
    ) -> None:
        """Initialize the instance."""
        self._session_result = session_result or []
        self._error = error
        self._account_state_rows = account_state_rows or []
        self._accessible_wallet_ids = accessible_wallet_ids or []
        self._read_granted_wallet_ids: list[str] = []
        self._duplicate_active_rows = duplicate_active_rows
        self.reconciliation_context_calls: list[list[str] | None] = []

    def grant_read_access(self, wallet_public_ids: list[str]) -> None:
        """Make wallets visible through the personal read-grant plane only.

        Kept off ``__init__`` so the constructor stays within the argument
        budget; the read plane is an additive disjunct anyway.
        """
        self._read_granted_wallet_ids = wallet_public_ids

    def session(self) -> MockSession:
        """Return mock session with configured result or error."""
        return MockSession(self._session_result, self._error)

    def _raise_if_error(self) -> None:
        """Raise stored error if configured."""
        if self._error:
            raise self._error

    async def get_exchanges(self, as_of: Any) -> list[str]:
        """Return mock exchange list."""
        self._raise_if_error()
        return list(self._session_result)

    async def get_exchange_instruments(self, exchange: str, as_of: Any) -> list[str]:
        """Return mock instrument list."""
        self._raise_if_error()
        return list(self._session_result)

    async def get_exchange_instruments_detail(
        self, exchange: str, as_of: Any
    ) -> list[dict[str, Any]]:
        """Return mock capability-aware instrument detail rows."""
        self._raise_if_error()
        return list(self._session_result)

    async def get_signals(self, **kwargs: Any) -> list[dict[str, Any]]:
        """Return mock signal dicts."""
        self._raise_if_error()
        return [
            {
                "public_id": sig.public_id,
                "timestamp": sig.timestamp,
                "session_id": sig.session_id,
                "sequence_id": sig.sequence_id,
                "instrument": sym.native_symbol,
                "exchange": inst.exchange,
                "side": sig.side,
                "strength": sig.strength,
                "reason": sig.reason,
                "strategy_name": sig.strategy_name,
                "price": sig.price,
                "fired_at": sig.fired_at,
            }
            for sig, inst, sym in self._session_result
        ]

    async def get_orders(self, **kwargs: Any) -> list[dict[str, Any]]:
        """Return mock order dicts."""
        self._raise_if_error()
        return [
            {
                "public_id": order.public_id,
                "timestamp": order.timestamp,
                "session_id": order.session_id,
                "sequence_id": order.sequence_id,
                "instrument": sym.native_symbol,
                "exchange": inst.exchange,
                "client_order_id": order.client_order_id or "",
                "exchange_order_id": order.exchange_order_id,
                "created_at": order.created_at,
                "updated_at": order.updated_at,
                "side": order.side,
                "order_type": order.order_type,
                "price": order.price,
                "size": order.size,
                "filled_size": order.filled_size,
                "average_price": order.average_price,
                "status": order.status,
                "time_in_force": order.time_in_force,
                "error": order.error,
            }
            for order, inst, sym in self._session_result
        ]

    async def get_executions(self, **kwargs: Any) -> list[dict[str, Any]]:
        """Return mock execution dicts."""
        self._raise_if_error()
        return [
            {
                "public_id": exe.public_id,
                "timestamp": exe.timestamp,
                "session_id": exe.session_id,
                "sequence_id": exe.sequence_id,
                "trade_id": exe.trade_id,
                "exec_id": exe.exec_id,
                "exchange_order_id": order.exchange_order_id,
                "client_order_id": order.client_order_id or "",
                "instrument": sym.native_symbol,
                "exchange": inst.exchange,
                "side": exe.side,
                "size": exe.size,
                "price": exe.price,
                "last_size": exe.size,
                "last_price": exe.price,
                "fee": exe.fee,
                "fee_asset": exe.fee_asset,
                "price_decimal": "100.5",
                "size_decimal": "2.0",
                "fee_decimal": "0.1",
                "status": exe.status,
                "executed_at": exe.executed_at or exe.timestamp,
            }
            for exe, order, inst, sym in self._session_result
        ]

    async def get_positions(self, **kwargs: Any) -> list[dict[str, Any]]:
        """Return mock position dicts."""
        self._raise_if_error()
        return [
            {
                "public_id": pos.public_id,
                "timestamp": pos.timestamp,
                "session_id": pos.session_id,
                "sequence_id": pos.sequence_id,
                "instrument": sym.native_symbol,
                "exchange": inst.exchange,
                "quantity": pos.quantity,
                "average_price": pos.average_price,
                "unrealized_pnl": pos.unrealized_pnl,
                "realized_pnl": pos.realized_pnl,
                "position_cycle_public_id": None,
            }
            for pos, inst, sym in self._session_result
        ]

    async def list_accessible_wallets_for_operators(
        self, operator_public_ids: list[str], as_of: Any
    ) -> list[dict[str, Any]]:
        """Return mock accessible-wallet rows for the given operators.

        Mirrors the real trade-plane short-circuit: an empty operator list
        can never yield a wallet.
        """
        self._raise_if_error()
        if not operator_public_ids:
            return []
        return [{"public_id": wid} for wid in self._accessible_wallet_ids]

    async def list_readable_wallets_for_user(
        self, user_public_id: str, operator_public_ids: list[str], as_of: Any
    ) -> list[dict[str, Any]]:
        """Return the mock read plane: operator-covered UNION read-granted.

        Mirrors the real read-plane contract by NOT short-circuiting on an
        empty operator list — the read-grant disjunct always participates.
        """
        self._raise_if_error()
        covered = self._accessible_wallet_ids if operator_public_ids else []
        ordered = list(dict.fromkeys([*covered, *self._read_granted_wallet_ids]))
        return [{"public_id": wid} for wid in ordered]

    async def get_venue_account_states(
        self, wallet_public_ids: list[str] | None
    ) -> list[VenueAccountStateRow]:
        """Return mock venue account-state rows, wallet-scoped when filtered.

        Mirrors the real repository contract: ``None`` (ADMIN, unscoped)
        returns every configured row, a wallet list narrows to matching
        rows only.
        """
        self._raise_if_error()
        if wallet_public_ids is None:
            return list(self._account_state_rows)
        return [
            row for row in self._account_state_rows if row["wallet_public_id"] in wallet_public_ids
        ]

    async def get_portfolio_reconciliation_read_contexts(
        self, wallet_public_ids: list[str] | None
    ) -> list[PortfolioReconciliationReadContextRow]:
        """Return one batched no-state context per scoped account row."""
        self.reconciliation_context_calls.append(wallet_public_ids)
        self._raise_if_error()
        rows = self._account_state_rows
        if wallet_public_ids is not None:
            rows = [row for row in rows if row["wallet_public_id"] in wallet_public_ids]
        return [
            PortfolioReconciliationReadContextRow(
                account_state=row,
                duplicate_active_rows=self._duplicate_active_rows,
                state=None,
                observations=[],
                config=None,
                latest_ordered_observation_id=None,
                latest_appended_observation_id=None,
                open_drift_episode=None,
                spot_anchor=None,
            )
            for row in rows
        ]

    async def get_candles(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        """Return mock candle dicts."""
        self._raise_if_error()
        return list(self._session_result)

    async def get_settings(self, **kwargs: Any) -> list[dict[str, Any]]:
        """Return mock settings dicts."""
        self._raise_if_error()
        return list(self._session_result)

    async def get_setting_by_key(self, key: str, as_of: Any) -> dict[str, Any] | None:
        """Return mock setting dict or None."""
        self._raise_if_error()
        return self._session_result[0] if self._session_result else None

    async def get_setting_categories(self, as_of: Any) -> list[str]:
        """Return mock setting categories."""
        self._raise_if_error()
        return list(self._session_result)


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
    app.router.lifespan_context = _noop_lifespan

    def skip_csrf_validation() -> None:
        return None

    def skip_authentication() -> AuthPrincipal:
        return AuthPrincipal(username="test_user", role=UserRole.ADMIN)

    app.dependency_overrides[validate_csrf_token] = skip_csrf_validation
    app.dependency_overrides[require_authentication] = skip_authentication
    return _track_test_client(TestClient(app))


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
            patch(
                "snapper.server.app.get_ai_review_service",
                return_value=_make_ai_review_service_mock(),
            ),
            patch("snapper.server.app.ProcessLauncherService") as mock_factory_cls,
            patch("snapper.server.app.get_settings_service") as mock_get_settings_service,
        ):
            mock_settings_service = MagicMock()
            mock_settings_service.shutdown = AsyncMock()
            mock_get_settings_service.return_value = mock_settings_service
            mock_factory = MagicMock()
            mock_factory.sync_registry_to_database = AsyncMock()
            mock_factory.start_all_processes = AsyncMock()
            mock_factory.spawn_per_wallet_executors = AsyncMock(return_value=0)
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
            patch(
                "snapper.server.app.get_ai_review_service",
                return_value=_make_ai_review_service_mock(),
            ),
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
            mock_factory.spawn_per_wallet_executors = AsyncMock(return_value=0)
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
            patch(
                "snapper.server.app.get_ai_review_service",
                return_value=_make_ai_review_service_mock(),
            ),
            patch("snapper.server.app.ProcessLauncherService") as mock_factory_cls,
            patch("snapper.server.app.get_settings_service") as mock_get_settings_service,
        ):
            mock_settings_service = MagicMock()
            mock_settings_service.shutdown = AsyncMock()
            mock_get_settings_service.return_value = mock_settings_service
            mock_factory = MagicMock()
            mock_factory.sync_registry_to_database = AsyncMock()
            mock_factory.start_all_processes = AsyncMock()
            mock_factory.spawn_per_wallet_executors = AsyncMock(return_value=0)
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

        def skip_authentication() -> AuthPrincipal:
            return AuthPrincipal(username="test_user", role=UserRole.ADMIN)

        def get_error_repo() -> MockRepository:
            return MockRepository(error=Exception("Database connection failed"))

        app.dependency_overrides[validate_csrf_token] = skip_csrf_validation
        app.dependency_overrides[require_authentication] = skip_authentication
        app.dependency_overrides[get_repository_dependency] = get_error_repo
        client = _track_test_client(TestClient(app))
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

        def skip_authentication() -> AuthPrincipal:
            return AuthPrincipal(username="test_user", role=UserRole.ADMIN)

        def get_error_repo() -> MockRepository:
            return MockRepository(error=Exception("Signal query failed"))

        app.dependency_overrides[validate_csrf_token] = skip_csrf_validation
        app.dependency_overrides[require_authentication] = skip_authentication
        app.dependency_overrides[get_repository_dependency] = get_error_repo
        client = _track_test_client(TestClient(app))
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

        def skip_authentication() -> AuthPrincipal:
            return AuthPrincipal(username="test_user", role=UserRole.ADMIN)

        def get_error_repo() -> MockRepository:
            return MockRepository(error=Exception("Execution query failed"))

        app.dependency_overrides[validate_csrf_token] = skip_csrf_validation
        app.dependency_overrides[require_authentication] = skip_authentication
        app.dependency_overrides[get_repository_dependency] = get_error_repo
        client = _track_test_client(TestClient(app))
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

        def skip_authentication() -> AuthPrincipal:
            return AuthPrincipal(username="test_user", role=UserRole.ADMIN)

        def get_error_repo() -> MockRepository:
            return MockRepository(error=Exception("Position query failed"))

        app.dependency_overrides[validate_csrf_token] = skip_csrf_validation
        app.dependency_overrides[require_authentication] = skip_authentication
        app.dependency_overrides[get_repository_dependency] = get_error_repo
        client = _track_test_client(TestClient(app))
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
        repo = MockRepository(error=Exception("Candle query failed"))
        client = create_app_with_overrides(repo)
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

        def skip_authentication() -> AuthPrincipal:
            return AuthPrincipal(username="test_user", role=UserRole.ADMIN)

        app.dependency_overrides[validate_csrf_token] = skip_csrf_validation
        app.dependency_overrides[require_authentication] = skip_authentication
        client = _track_test_client(TestClient(app))
        original_context = app.state.manager.zmq_bridge.context
        app.state.manager.zmq_bridge.context = None
        try:
            response = client.get("/api/zmq/health")
            assert response.status_code == 200
            data = response.json()
            assert "status" in data["payload"]
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

        def skip_authentication() -> AuthPrincipal:
            return AuthPrincipal(username="test_user", role=UserRole.ADMIN)

        app.dependency_overrides[validate_csrf_token] = skip_csrf_validation
        app.dependency_overrides[require_authentication] = skip_authentication
        mock_factory = _build_mock_process_factory({})
        with patch("snapper.server.app.ProcessLauncherService", return_value=mock_factory):
            app.state.process_factory = mock_factory
            with TestClient(app) as client:
                response = client.get("/api/status")
                assert response.status_code == 200
                data = response.json()
                payload = data["payload"]
                assert "trader" in payload
                assert payload["trader"]["status"] == "not_running"
                assert "strategies" in payload

    def test_system_status_handles_process_error(self) -> None:
        """Verify status handles process status error gracefully.

        Given: A process that raises exception on get_status,
        When: GET /status is called,
        Then: Response is 200 with partial status data.
        """
        app = create_app()

        def skip_csrf_validation() -> None:
            return None

        def skip_authentication() -> AuthPrincipal:
            return AuthPrincipal(username="test_user", role=UserRole.ADMIN)

        app.dependency_overrides[validate_csrf_token] = skip_csrf_validation
        app.dependency_overrides[require_authentication] = skip_authentication
        mock_process = MagicMock()
        mock_process.name = "test_strategy"
        mock_process.get_status = MagicMock(side_effect=Exception("Status error"))
        mock_factory = _build_mock_process_factory({"test_strategy": mock_process})
        with patch("snapper.server.app.ProcessLauncherService", return_value=mock_factory):
            app.state.process_factory = mock_factory
            with TestClient(app) as client:
                response = client.get("/api/status")
                assert response.status_code == 200
                data = response.json()
                assert "trader" in data["payload"]
                assert "strategies" in data["payload"]

    def test_system_status_trader_running_when_coordinator_started(self) -> None:
        """Verify trader status reflects trader_coordinator process state.

        Given: trader_coordinator process is in started_processes,
        When: GET /status is called,
        Then: trader.status is 'running'.
        """
        app = create_app()

        def skip_csrf_validation() -> None:
            return None

        def skip_authentication() -> AuthPrincipal:
            return AuthPrincipal(username="test_user", role=UserRole.ADMIN)

        app.dependency_overrides[validate_csrf_token] = skip_csrf_validation
        app.dependency_overrides[require_authentication] = skip_authentication
        mock_factory = _build_mock_process_factory({"trader_coordinator": MagicMock()})
        with patch("snapper.server.app.ProcessLauncherService", return_value=mock_factory):
            app.state.process_factory = mock_factory
            with TestClient(app) as client:
                response = client.get("/api/status")
                assert response.status_code == 200
                data = response.json()
                assert data["payload"]["trader"]["status"] == "running"

    def test_system_status_trader_not_running_when_coordinator_absent(self) -> None:
        """Verify trader status is not_running when coordinator is absent.

        Given: trader_coordinator is NOT in started_processes,
        When: GET /status is called,
        Then: trader.status is 'not_running'.
        """
        app = create_app()

        def skip_csrf_validation() -> None:
            return None

        def skip_authentication() -> AuthPrincipal:
            return AuthPrincipal(username="test_user", role=UserRole.ADMIN)

        app.dependency_overrides[validate_csrf_token] = skip_csrf_validation
        app.dependency_overrides[require_authentication] = skip_authentication
        mock_factory = _build_mock_process_factory({"some_other_process": MagicMock()})
        with patch("snapper.server.app.ProcessLauncherService", return_value=mock_factory):
            app.state.process_factory = mock_factory
            with TestClient(app) as client:
                response = client.get("/api/status")
                assert response.status_code == 200
                data = response.json()
                assert data["payload"]["trader"]["status"] == "not_running"


class TestAppCoverageImprovement:
    """Tests for improving application code coverage."""

    def setup_method(self) -> None:
        """Initialize test client with dependency overrides."""
        self.app = create_app()
        self.app.state.process_factory = _build_mock_process_factory({})
        self.client = TestClient(self.app)

        def skip_csrf_validation() -> None:
            return None

        def skip_authentication() -> AuthPrincipal:
            return AuthPrincipal(username="test_user", role=UserRole.ADMIN)

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
        assert "websocket" in data["payload"]
        assert "zmq_bridge" in data["payload"]
        assert "config" in data["payload"]

    def test_websocket_stats_endpoint_falls_back_when_settings_db_unavailable(
        self,
    ) -> None:
        """Verify the heartbeat interval falls back to default on DB-less settings.

        Given: Application with ``state.settings`` whose
            ``zmq_heartbeat_interval_ms`` raises ``RuntimeError`` because
            no ``SettingsService`` was wired,
        When: GET /ws/stats is called,
        Then: The endpoint returns 200 with the bootstrap default of 1000ms
            instead of bubbling the RuntimeError into a 500.
        """

        class _SettingsStub:
            @property
            def zmq_heartbeat_interval_ms(self) -> int:
                raise RuntimeError("SettingsService not initialized")

        self.app.state.settings = _SettingsStub()
        response = self.client.get("/api/ws/stats")
        assert response.status_code == 200
        data = response.json()
        assert data["payload"]["config"]["heartbeat_interval_ms"] == 1000

    def test_zmq_health_check_success(self) -> None:
        """Verify ZMQ health check returns healthy status.

        Given: Application with ZMQ bridge,
        When: GET /zmq/health is called,
        Then: Response indicates healthy with ok components.
        """
        response = self.client.get("/api/zmq/health")
        assert response.status_code == 200
        data = response.json()
        assert data["payload"]["status"] == "healthy"
        assert data["payload"]["components"]["zmq_context"] == "ok"

    def test_zmq_health_check_error(self) -> None:
        """Verify ZMQ health check includes error information.

        Given: Application with ZMQ bridge,
        When: GET /zmq/health is called,
        Then: Response contains components and errors keys.
        """
        response = self.client.get("/api/zmq/health")
        assert response.status_code == 200
        data = response.json()
        assert data["payload"]["status"] == "healthy"
        assert "components" in data["payload"]
        assert "errors" in data["payload"]

    def test_health_endpoint(self) -> None:
        """Verify health endpoint returns healthy status when all CORE running.

        Given: Running application with all enabled long-running CORE processes up,
        When: GET /health is called,
        Then: Response is 200 with healthy status and timestamp.
        """
        response = self.client.get("/api/health")
        assert response.status_code == 200
        data = response.json()
        assert data["payload"]["status"] == "healthy"
        assert "timestamp" in data

    def test_health_endpoint_reflects_core_error(self) -> None:
        """Verify health endpoint propagates error status from get_core_health.

        Given: An enabled long-running CORE process that is not running,
        When: GET /health is called,
        Then: Response is 200 with error status.
        """
        mock_factory = _build_mock_process_factory({})
        mock_factory.get_core_health = AsyncMock(return_value="error")
        self.app.state.process_factory = mock_factory
        response = self.client.get("/api/health")
        assert response.status_code == 200
        data = response.json()
        assert data["payload"]["status"] == "error"

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
        app.state.process_factory = _build_mock_process_factory({})
        test_client = _track_test_client(TestClient(app))
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


class MockSymbol:
    """Mock symbol for testing endpoint responses."""

    def __init__(self, native_symbol: str = "BTC-USD") -> None:
        """Initialize the instance."""
        self.native_symbol = native_symbol
        self.public_id = "test-symbol-public-id"


class MockInstrument:
    """Mock instrument for testing endpoint responses."""

    def __init__(self, inst_id: int = 1, exchange: str = "kraken") -> None:
        """Initialize the instance."""
        self.id = inst_id
        self.public_id = "test-instrument-public-id"
        self.symbol_public_id = "test-symbol-public-id"
        self.exchange = exchange


class MockOrder:
    """Mock order record for testing orders endpoint."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.id = 1
        self.public_id = "order-uuid-1234"
        self.instrument_public_id = "test-instrument-public-id"
        self.timestamp = dt.datetime(2024, 1, 1, 12, 0, tzinfo=dt.UTC)
        self.client_order_id = "client_123"
        self.exchange_order_id = "exch_456"
        self.created_at = dt.datetime(2024, 1, 1, 12, 0, tzinfo=dt.UTC)
        self.updated_at = dt.datetime(2024, 1, 1, 12, 5, tzinfo=dt.UTC)
        self.side = "buy"
        self.order_type = "limit"
        self.price = 50000.0
        self.size = 1.0
        self.status = "filled"
        self.filled_size = 1.0
        self.average_price = 50000.0
        self.time_in_force = "GTC"
        self.error = None
        self.session_id = ""
        self.sequence_id = 0


class MockSignal:
    """Mock signal event for testing signals endpoint."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.id = 1
        self.public_id = "signal-uuid-1234"
        self.instrument_public_id = "test-instrument-public-id"
        self.timestamp = dt.datetime(2024, 1, 1, 12, 0, tzinfo=dt.UTC)
        self.fired_at = dt.datetime(2024, 1, 1, 11, 59, tzinfo=dt.UTC)
        self.side = "buy"
        self.strength = 0.8
        self.reason = "RSI oversold"
        self.strategy_name = "rsi_strategy"
        self.price = 49500.0
        self.session_id = ""
        self.sequence_id = 0


class MockExecution:
    """Mock execution for testing executions endpoint."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.id = 1
        self.public_id = "execution-uuid-1234"
        self.order_public_id = "order-uuid-1234"
        self.exec_id = "exec-001"
        self.trade_id = "trade-001"
        self.timestamp = dt.datetime(2024, 1, 1, 12, 1, tzinfo=dt.UTC)
        self.side = "buy"
        self.status = "filled"
        self.executed_at = dt.datetime(2024, 1, 1, 12, 1, tzinfo=dt.UTC)
        self.price = 50000.0
        self.size = 1.0
        self.fee = 10.0
        self.fee_asset = "USD"
        self.session_id = ""
        self.sequence_id = 0


class MockPosition:
    """Mock position for testing positions endpoint."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.id = 1
        self.public_id = "position-uuid-1234"
        self.instrument_public_id = "test-instrument-public-id"
        self.quantity = 1.5
        self.average_price = 48000.0
        self.unrealized_pnl = 3000.0
        self.realized_pnl = 500.0
        self.timestamp = dt.datetime(2024, 1, 1, 12, 0, tzinfo=dt.UTC)
        self.session_id = ""
        self.sequence_id = 0


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
    app.router.lifespan_context = _noop_lifespan

    def skip_csrf_validation() -> None:
        return None

    def skip_authentication() -> AuthPrincipal:
        return AuthPrincipal(username="test_user", role=UserRole.ADMIN)

    app.dependency_overrides[validate_csrf_token] = skip_csrf_validation
    app.dependency_overrides[require_authentication] = skip_authentication
    if repo:
        app.dependency_overrides[get_repository_dependency] = lambda: repo
    return _track_test_client(TestClient(app))


class TestOrdersSuccessPath:
    """Tests for orders endpoint success scenarios."""

    def test_get_orders_returns_data(self) -> None:
        """Verify orders endpoint returns order data.

        Given: Repository with order and instrument records,
        When: GET /orders is called,
        Then: Response contains order data with instrument symbol.
        """
        order = MockOrder()
        instrument = MockInstrument()
        symbol = MockSymbol()
        repo = MockRepository(session_result=[(order, instrument, symbol)])
        client = create_app_with_overrides(repo)
        response = client.get("/api/orders")
        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "order_list"
        assert data["count"] == 1
        items = data["payload"]
        assert len(items) == 1
        assert items[0]["instrument"] == "BTC-USD"
        assert items[0]["exchange"] == "kraken"
        assert items[0]["side"] == "buy"
        assert items[0]["order_type"] == "limit"
        assert items[0]["filled_size"] == pytest.approx(1.0)
        assert items[0]["average_price"] == pytest.approx(50000.0)
        assert items[0]["status"] == "filled"
        assert items[0]["timestamp"] == "2024-01-01T12:00:00Z"

    def test_get_orders_with_symbol_filter(self) -> None:
        """Verify orders endpoint filters by symbol.

        Given: Repository with order for specific instrument,
        When: GET /orders is called with symbol filter,
        Then: Response contains only matching orders.
        """
        order = MockOrder()
        instrument = MockInstrument()
        symbol = MockSymbol(native_symbol="ETH-USD")
        repo = MockRepository(session_result=[(order, instrument, symbol)])
        client = create_app_with_overrides(repo)
        response = client.get("/api/orders?symbol=ETH-USD")
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 1
        assert data["payload"][0]["instrument"] == "ETH-USD"

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
        assert data["payload"] == []
        assert data["count"] == 0


class TestSignalsSuccessPath:
    """Tests for signals endpoint success scenarios."""

    def test_get_signals_returns_data(self) -> None:
        """Verify signals endpoint returns signal data.

        Given: Repository with signal and instrument records,
        When: GET /signals is called,
        Then: Response contains signal data with strategy name.
        """
        signal = MockSignal()
        instrument = MockInstrument()
        symbol = MockSymbol()
        repo = MockRepository(session_result=[(signal, instrument, symbol)])
        client = create_app_with_overrides(repo)
        response = client.get("/api/signals")
        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "signal_list"
        assert data["count"] == 1
        items = data["payload"]
        assert len(items) == 1
        assert items[0]["instrument"] == "BTC-USD"
        assert items[0]["exchange"] == "kraken"
        assert items[0]["side"] == "buy"
        assert items[0]["strength"] == pytest.approx(0.8)
        assert items[0]["reason"] == "RSI oversold"
        assert items[0]["strategy_name"] == "rsi_strategy"
        assert items[0]["timestamp"] == "2024-01-01T12:00:00Z"
        assert items[0]["fired_at"] == "2024-01-01T11:59:00Z"

    def test_get_signals_with_filters(self) -> None:
        """Verify signals endpoint filters by instrument and strategy.

        Given: Repository with signal records,
        When: GET /signals is called with filters,
        Then: Response contains only matching signals.
        """
        signal = MockSignal()
        instrument = MockInstrument()
        symbol = MockSymbol()
        repo = MockRepository(session_result=[(signal, instrument, symbol)])
        client = create_app_with_overrides(repo)
        response = client.get("/api/signals?instrument=BTC-USD&strategy=rsi_strategy")
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 1

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
        assert data["payload"] == []
        assert data["count"] == 0


class TestExecutionsSuccessPath:
    """Tests for executions endpoint success scenarios."""

    def test_get_executions_returns_data(self) -> None:
        """Verify executions endpoint returns execution data.

        Given: Repository with execution and order records,
        When: GET /executions is called,
        Then: Response contains execution data with fee info.
        """
        execution = MockExecution()
        order = MockOrder()
        instrument = MockInstrument()
        symbol = MockSymbol()
        repo = MockRepository(session_result=[(execution, order, instrument, symbol)])
        client = create_app_with_overrides(repo)
        response = client.get("/api/executions")
        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "execution_list"
        assert data["count"] == 1
        items = data["payload"]
        assert len(items) == 1
        assert items[0]["price"] == pytest.approx(50000.0)
        assert items[0]["size"] == pytest.approx(1.0)
        assert items[0]["fee"] == pytest.approx(10.0)
        assert items[0]["fee_asset"] == "USD"
        assert items[0]["trade_id"] == "trade-001"
        assert items[0]["client_order_id"] == "client_123"
        assert items[0]["instrument"] == "BTC-USD"
        assert items[0]["exchange"] == "kraken"
        assert items[0]["side"] == "buy"
        assert items[0]["status"] == "filled"
        assert items[0]["timestamp"] == "2024-01-01T12:01:00Z"
        assert items[0]["executed_at"] is not None

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
        assert data["payload"] == []
        assert data["count"] == 0


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
        symbol = MockSymbol()
        repo = MockRepository(session_result=[(position, instrument, symbol)])
        client = create_app_with_overrides(repo)
        response = client.get("/api/positions")
        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "position_list"
        assert data["count"] == 1
        items = data["payload"]
        assert len(items) == 1
        assert items[0]["instrument"] == "BTC-USD"
        assert items[0]["exchange"] == "kraken"
        assert items[0]["quantity"] == pytest.approx(1.5)
        assert items[0]["average_price"] == pytest.approx(48000.0)
        assert items[0]["unrealized_pnl"] == pytest.approx(3000.0)
        assert items[0]["realized_pnl"] == pytest.approx(500.0)

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
        assert data["payload"] == []
        assert data["count"] == 0


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

        def skip_authentication() -> AuthPrincipal:
            return AuthPrincipal(username="test_user", role=UserRole.ADMIN)

        app.dependency_overrides[validate_csrf_token] = skip_csrf_validation
        app.dependency_overrides[require_authentication] = skip_authentication
        mock_context = MagicMock()
        mock_context.socket.side_effect = Exception("ZMQ socket creation failed")
        original_context = app.state.manager.zmq_bridge.context
        app.state.manager.zmq_bridge.context = mock_context
        client = _track_test_client(TestClient(app))
        try:
            response = client.get("/api/zmq/health")
            assert response.status_code == 200
            data = response.json()
            assert data["payload"]["status"] == "error"
            assert data["payload"]["components"]["zmq_context"] == "error"
            assert len(data["payload"]["errors"]) > 0
            assert "ZMQ context error" in data["payload"]["errors"][0]
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

        def skip_authentication() -> AuthPrincipal:
            return AuthPrincipal(username="test_user", role=UserRole.ADMIN)

        app.dependency_overrides[validate_csrf_token] = skip_csrf_validation
        app.dependency_overrides[require_authentication] = skip_authentication
        mock_process = MagicMock()
        mock_process.name = "failing_strategy"
        mock_process.get_status.side_effect = RuntimeError("Status unavailable")
        mock_factory = _build_mock_process_factory({"failing_strategy": mock_process})
        with (
            patch("snapper.server.app.ProcessLauncherService", return_value=mock_factory),
            patch("snapper.server.app.logger") as mock_logger,
        ):
            app.state.process_factory = mock_factory
            with TestClient(app) as client:
                response = client.get("/api/status")
                assert response.status_code == 200
                data = response.json()
                assert "strategies" in data["payload"]
                assert "trader" in data["payload"]
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

        def skip_authentication() -> AuthPrincipal:
            return AuthPrincipal(username="test_user", role=UserRole.ADMIN)

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
        mock_factory = _build_mock_process_factory({"test_strategy": mock_process})
        with patch("snapper.server.app.ProcessLauncherService", return_value=mock_factory):
            app.state.process_factory = mock_factory
            with TestClient(app) as client:
                response = client.get("/api/status")
                assert response.status_code == 200
                data = response.json()
                payload = data["payload"]
                assert len(payload["strategies"]) == 1
                assert payload["strategies"][0]["strategy_name"] == "test_strategy"
                assert payload["strategies"][0]["status"] == "running"


class TestSignalsExchangeFilter:
    """Tests for signals endpoint exchange filtering."""

    def test_get_signals_with_exchange_filter(self) -> None:
        """Verify signals endpoint filters by exchange.

        Given: Repository with signal records for a specific exchange,
        When: GET /signals is called with exchange filter,
        Then: Response contains only matching signals.
        """
        signal = MockSignal()
        instrument = MockInstrument()
        symbol = MockSymbol()
        repo = MockRepository(session_result=[(signal, instrument, symbol)])
        client = create_app_with_overrides(repo)
        response = client.get("/api/signals?exchange=kraken")
        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "signal_list"
        assert data["count"] == 1
        assert data["payload"][0]["exchange"] == "kraken"


class TestOrdersExchangeFilter:
    """Tests for orders endpoint exchange filtering."""

    def test_get_orders_with_exchange_filter(self) -> None:
        """Verify orders endpoint filters by exchange.

        Given: Repository with order records for a specific exchange,
        When: GET /orders is called with exchange filter,
        Then: Response contains only matching orders.
        """
        order = MockOrder()
        instrument = MockInstrument()
        symbol = MockSymbol()
        repo = MockRepository(session_result=[(order, instrument, symbol)])
        client = create_app_with_overrides(repo)
        response = client.get("/api/orders?exchange=kraken")
        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "order_list"
        assert data["count"] == 1
        assert data["payload"][0]["exchange"] == "kraken"


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
        repo = MockRepository(session_result=["kraken", "walutomat"])
        client = create_app_with_overrides(repo)
        response = client.get("/api/exchanges")
        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "exchange_list"
        assert "kraken" in data["payload"]
        assert "walutomat" in data["payload"]

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
        assert data["payload"] == []
        assert data["count"] == 0

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
        assert data["type"] == "instrument_list"
        assert "BTC-USD" in data["payload"]
        assert "ETH-USD" in data["payload"]

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
        assert data["payload"] == []
        assert data["count"] == 0

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


class TestExchangeInstrumentsDetailEndpoint:
    """Tests for the capability-aware instrument-detail endpoint."""

    def _make_row(
        self,
        *,
        symbol: str = "MNQM6-CME",
        can_trade: bool = False,
        can_market_data: bool = True,
        instrument_kind: str | None = "future",
        expiry_at: datetime | None = datetime(2026, 6, 19, 20, 0, 0, tzinfo=UTC),
    ) -> dict[str, Any]:
        """Produce a typed mock detail row matching ``InstrumentDetailRow``."""
        return {
            "instrument_public_id": "00000000-0000-7000-8000-0000000000f1",
            "symbol_public_id": "00000000-0000-7000-8000-0000000000e1",
            "symbol": symbol,
            "exchange": "kraken_equities",
            "can_trade": can_trade,
            "can_market_data": can_market_data,
            "instrument_resolved": True,
            "instrument_kind": instrument_kind,
            "expiry_at": expiry_at,
        }

    def test_returns_detail_rows_with_capability_flags(self) -> None:
        """Endpoint projects repo rows through InstrumentDetailData items.

        Given: Repository returning two rows (one tradable, one market-data only),
        When: GET /exchanges/kraken_equities/instruments/detail is called,
        Then: Response is 200 with two items whose can_trade flags match.
        """
        rows = [
            self._make_row(symbol="MNQM6-CME", can_trade=False),
            self._make_row(symbol="BTC-USD", can_trade=True, instrument_kind="spot"),
        ]
        repo = MockRepository(session_result=rows)
        client = create_app_with_overrides(repo)
        response = client.get("/api/exchanges/kraken_equities/instruments/detail")
        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "instrument_detail_list"
        assert data["count"] == 2
        symbols = {item["symbol"]: item["can_trade"] for item in data["payload"]}
        assert symbols == {"MNQM6-CME": False, "BTC-USD": True}

    def test_empty_result(self) -> None:
        """Endpoint returns an empty list when the exchange has no rows.

        Given: Repository returning an empty list,
        When: GET /exchanges/unknown/instruments/detail is called,
        Then: Response is 200 with empty payload and count=0.
        """
        repo = MockRepository(session_result=[])
        client = create_app_with_overrides(repo)
        response = client.get("/api/exchanges/unknown/instruments/detail")
        assert response.status_code == 200
        data = response.json()
        assert data["payload"] == []
        assert data["count"] == 0

    def test_database_error_returns_500(self) -> None:
        """Endpoint returns 500 when the repository raises.

        Given: A repository raising a generic database exception,
        When: the detail endpoint is called,
        Then: Response is 500 with ``Failed to fetch instrument detail`` detail.
        """
        repo = MockRepository(error=Exception("Database connection failed"))
        client = create_app_with_overrides(repo)
        response = client.get("/api/exchanges/kraken_equities/instruments/detail")
        assert response.status_code == 500
        assert "Failed to fetch instrument detail" in response.json()["detail"]


class TestCandlesHttpExceptionReraise:
    """Tests for candles endpoint HTTP exception propagation."""

    def test_candles_reraises_http_exception(self) -> None:
        """Verify candles endpoint re-raises HTTPException.

        Given: Repository that raises HTTPException,
        When: GET /candles is called,
        Then: HTTPException is propagated with original status.
        """
        repo = MockRepository(error=HTTPException(status_code=403, detail="Forbidden"))
        client = create_app_with_overrides(repo)
        response = client.get("/api/candles?instrument=BTC-USD&exchange=kraken&timeframe=1h")
        assert response.status_code == 403
        assert "Forbidden" in response.json()["detail"]

    def test_candles_db_reraises_http_exception(self) -> None:
        """``/api/candles/db`` propagates HTTPException without swallowing.

        Mirrors :meth:`test_candles_reraises_http_exception` for the
        explicit DB-only route; the ``except HTTPException: raise``
        branch preserves dep-layer statuses (403/404/etc.) instead of
        converting them to a generic 500.
        """
        repo = MockRepository(error=HTTPException(status_code=403, detail="Forbidden"))
        client = create_app_with_overrides(repo)
        response = client.get("/api/candles/db?instrument=BTC-USD&exchange=kraken&timeframe=1h")
        assert response.status_code == 403
        assert "Forbidden" in response.json()["detail"]

    def test_candles_cache_reraises_http_exception(self) -> None:
        """``/api/candles/cache`` propagates HTTPException without swallowing.

        The DB-fallback path inside ``fetch_cache_only`` (used for
        1h/4h/1d timeframes) can surface auth/permission HTTPExceptions
        from upstream deps. The ``except HTTPException: raise`` branch
        preserves the original status instead of folding it into the
        generic 500 mapping.
        """
        repo = MockRepository(error=HTTPException(status_code=403, detail="Forbidden"))
        client = create_app_with_overrides(repo)
        response = client.get("/api/candles/cache?instrument=BTC-USD&exchange=kraken&timeframe=1h")
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
        "parameters": {"identifier": "one"},
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
        "parameters": {"identifier": "two"},
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
        "parameters": {"identifier": "three"},
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
        "parameters": {"identifier": "four"},
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


def _create_scoped_client(role: UserRole, operator_ids: list[str]) -> TestClient:
    """Create a test client with a specific role and operator set."""
    app = create_app()
    app.router.lifespan_context = _noop_lifespan

    def skip_csrf() -> None:
        return None

    def scoped_auth() -> AuthPrincipal:
        return AuthPrincipal(
            username="scoped_user",
            role=role,
            operator_public_ids=operator_ids,
        )

    app.dependency_overrides[validate_csrf_token] = skip_csrf
    app.dependency_overrides[require_authentication] = scoped_auth
    app.dependency_overrides[get_repository_dependency] = lambda: MockRepository()
    return _track_test_client(TestClient(app))


class TestScopedEndpoints403Propagation:
    """HTTPException(403) from resolve_readable_wallets must NOT be swallowed.

    Before this fix, the broad ``except Exception`` in the signals /
    orders / executions / positions handlers remapped the 403 to 500.
    The ``except HTTPException: raise`` guard ensures the 403 propagates.
    """

    def test_signals_403_propagates(self) -> None:
        """GET /signals with foreign operator_public_id returns 403."""
        client = _create_scoped_client(UserRole.OPERATOR, ["op-1"])
        response = client.get("/api/signals?operator_public_id=op-foreign")

        assert response.status_code == 403

    def test_orders_403_propagates(self) -> None:
        """GET /orders with foreign operator_public_id returns 403."""
        client = _create_scoped_client(UserRole.OPERATOR, ["op-1"])
        response = client.get("/api/orders?operator_public_id=op-foreign")

        assert response.status_code == 403

    def test_executions_403_propagates(self) -> None:
        """GET /executions with foreign operator_public_id returns 403."""
        client = _create_scoped_client(UserRole.OPERATOR, ["op-1"])
        response = client.get("/api/executions?operator_public_id=op-foreign")

        assert response.status_code == 403

    def test_positions_403_propagates(self) -> None:
        """GET /positions with foreign operator_public_id returns 403."""
        client = _create_scoped_client(UserRole.OPERATOR, ["op-1"])
        response = client.get("/api/positions?operator_public_id=op-foreign")

        assert response.status_code == 403


class TestReconcileStaleBacktests:
    """Tests for _reconcile_stale_backtests boot helper."""

    @pytest.mark.asyncio
    @patch("snapper.server.app.get_repository_dependency")
    @patch("snapper.server.app.BacktestRepository")
    async def test_reconciles_stale_runs(
        self, mock_bt_cls: MagicMock, mock_get_repo: MagicMock
    ) -> None:
        """Calls reconcile_stale_runs and logs when count > 0."""
        mock_repo = MagicMock()
        mock_repo.session_factory = MagicMock()
        mock_get_repo.return_value = mock_repo

        mock_bt = AsyncMock()
        mock_bt.reconcile_stale_runs = AsyncMock(return_value=2)
        mock_bt_cls.return_value = mock_bt

        app = FastAPI()
        app.state.rest_tracker = SequenceTracker()

        await _reconcile_stale_backtests(app)

        mock_bt.reconcile_stale_runs.assert_called_once()

    @pytest.mark.asyncio
    @patch("snapper.server.app.get_repository_dependency")
    @patch("snapper.server.app.BacktestRepository")
    async def test_zero_stale_runs_no_log(
        self, mock_bt_cls: MagicMock, mock_get_repo: MagicMock
    ) -> None:
        """Zero stale runs skips the count>0 log branch."""
        mock_repo = MagicMock()
        mock_repo.session_factory = MagicMock()
        mock_get_repo.return_value = mock_repo

        mock_bt = AsyncMock()
        mock_bt.reconcile_stale_runs = AsyncMock(return_value=0)
        mock_bt_cls.return_value = mock_bt

        app = FastAPI()
        app.state.rest_tracker = SequenceTracker()

        await _reconcile_stale_backtests(app)

        mock_bt.reconcile_stale_runs.assert_called_once()

    @pytest.mark.asyncio
    @patch("snapper.server.app.get_repository_dependency")
    async def test_exception_is_non_fatal(self, mock_get_repo: MagicMock) -> None:
        """Exception during reconciliation is caught (non-fatal)."""
        mock_get_repo.side_effect = RuntimeError("db down")

        app = FastAPI()
        app.state.rest_tracker = SequenceTracker()

        await _reconcile_stale_backtests(app)


class TestWarnOnTradfiNearExpiry:
    """Tests for the TradFi 14-day expiry WARN-log boot helper."""

    @pytest.mark.asyncio
    @patch("snapper.server.app.get_repository_dependency")
    async def test_logs_warning_when_symbol_expires_within_window(
        self, mock_get_repo: MagicMock
    ) -> None:
        """A default TradFi symbol with ``expiry_at`` within 14 days logs WARN.

        Given: AppSettings.instruments[KRAKEN_EQUITIES]=['MNQM6-CME'] +
            a detail row whose ``expiry_at`` is 5 days from now,
        When: the boot helper runs,
        Then: ``get_exchange_instruments_detail`` is awaited once and
            the helper completes without raising.
        """
        now = datetime.now(UTC)
        soon = now + dt.timedelta(days=5)
        mock_repo = AsyncMock()
        mock_repo.get_exchange_instruments_detail = AsyncMock(
            return_value=[
                {
                    "instrument_public_id": "i-1",
                    "symbol_public_id": "s-1",
                    "symbol": "MNQM6-CME",
                    "exchange": "kraken_equities",
                    "can_trade": False,
                    "can_market_data": True,
                    "instrument_resolved": True,
                    "instrument_kind": "future",
                    "expiry_at": soon,
                }
            ]
        )
        mock_get_repo.return_value = mock_repo
        settings = MagicMock()
        settings.instruments = {"kraken_equities": ["MNQM6-CME"]}
        await _warn_on_tradfi_near_expiry(settings)
        mock_repo.get_exchange_instruments_detail.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("snapper.server.app.get_repository_dependency")
    async def test_no_warning_when_symbol_is_not_near_expiry(
        self, mock_get_repo: MagicMock
    ) -> None:
        """Symbols with ``expiry_at`` > 14 days in the future do not trigger a WARN.

        Given: a detail row whose ``expiry_at`` is 90 days out,
        When: the boot helper runs,
        Then: the helper completes silently (no crash) and the repo
            query is still awaited once.
        """
        now = datetime.now(UTC)
        far = now + dt.timedelta(days=90)
        mock_repo = AsyncMock()
        mock_repo.get_exchange_instruments_detail = AsyncMock(
            return_value=[
                {
                    "instrument_public_id": "i-1",
                    "symbol_public_id": "s-1",
                    "symbol": "MNQU6-CME",
                    "exchange": "kraken_equities",
                    "can_trade": False,
                    "can_market_data": True,
                    "instrument_resolved": True,
                    "instrument_kind": "future",
                    "expiry_at": far,
                }
            ]
        )
        mock_get_repo.return_value = mock_repo
        settings = MagicMock()
        settings.instruments = {"kraken_equities": ["MNQU6-CME"]}
        await _warn_on_tradfi_near_expiry(settings)
        mock_repo.get_exchange_instruments_detail.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("snapper.server.app.get_repository_dependency")
    async def test_no_defaults_configured_skips_query(self, mock_get_repo: MagicMock) -> None:
        """Empty defaults list short-circuits before touching the repository."""
        settings = MagicMock()
        settings.instruments = {"kraken_equities": []}
        await _warn_on_tradfi_near_expiry(settings)
        mock_get_repo.assert_not_called()

    @pytest.mark.asyncio
    @patch("snapper.server.app.get_repository_dependency")
    async def test_ignores_non_default_symbols(self, mock_get_repo: MagicMock) -> None:
        """Rows whose symbol is not in defaults are skipped even if near expiry."""
        now = datetime.now(UTC)
        soon = now + dt.timedelta(days=3)
        mock_repo = AsyncMock()
        mock_repo.get_exchange_instruments_detail = AsyncMock(
            return_value=[
                {
                    "instrument_public_id": "i-1",
                    "symbol_public_id": "s-1",
                    "symbol": "UNUSED-CME",
                    "exchange": "kraken_equities",
                    "can_trade": False,
                    "can_market_data": True,
                    "instrument_resolved": True,
                    "instrument_kind": "future",
                    "expiry_at": soon,
                }
            ]
        )
        mock_get_repo.return_value = mock_repo
        settings = MagicMock()
        settings.instruments = {"kraken_equities": ["MNQM6-CME"]}
        await _warn_on_tradfi_near_expiry(settings)
        mock_repo.get_exchange_instruments_detail.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("snapper.server.app.get_repository_dependency")
    async def test_rows_without_expiry_are_skipped(self, mock_get_repo: MagicMock) -> None:
        """Rows with ``expiry_at=None`` (perpetuals) are skipped safely."""
        mock_repo = AsyncMock()
        mock_repo.get_exchange_instruments_detail = AsyncMock(
            return_value=[
                {
                    "instrument_public_id": "i-1",
                    "symbol_public_id": "s-1",
                    "symbol": "MNQM6-CME",
                    "exchange": "kraken_equities",
                    "can_trade": False,
                    "can_market_data": True,
                    "instrument_resolved": True,
                    "instrument_kind": "future",
                    "expiry_at": None,
                }
            ]
        )
        mock_get_repo.return_value = mock_repo
        settings = MagicMock()
        settings.instruments = {"kraken_equities": ["MNQM6-CME"]}
        await _warn_on_tradfi_near_expiry(settings)

    @pytest.mark.asyncio
    @patch("snapper.server.app.get_repository_dependency")
    async def test_exception_is_non_fatal(self, mock_get_repo: MagicMock) -> None:
        """Exception during the expiry check is caught (non-fatal)."""
        mock_get_repo.side_effect = RuntimeError("db down")
        settings = MagicMock()
        settings.instruments = {"kraken_equities": ["MNQM6-CME"]}
        await _warn_on_tradfi_near_expiry(settings)


class _RaisingSettings:
    """Settings stub whose DB-backed flag raises (no SettingsService wired)."""

    @property
    def candle_single_source(self) -> bool:
        """Raise like a bootstrap-only AppSettings reading a DB setting."""
        raise RuntimeError("no settings service")


def _request_with_settings(settings: Any) -> Any:
    """Build a minimal request whose app.state carries (or omits) settings."""
    state = SimpleNamespace() if settings is None else SimpleNamespace(settings=settings)
    return SimpleNamespace(app=SimpleNamespace(state=state))


class TestResolveCandleSingleSource:
    """``_resolve_candle_single_source`` reads the DB-aware flag, falling back to OFF."""

    def test_missing_settings_falls_back_to_off(self) -> None:
        """Return False when app.state has no settings (lifespan bypassed).

        Given: a request whose app.state carries no settings,
        When: the flag is resolved,
        Then: it falls back to False (OFF) rather than raising.
        """
        assert _resolve_candle_single_source(_request_with_settings(None)) is False

    def test_reads_configured_flag(self) -> None:
        """Return the DB-aware settings value when present.

        Given: app.state.settings exposing candle_single_source=True,
        When: the flag is resolved,
        Then: True is returned.
        """
        settings = SimpleNamespace(candle_single_source=True)
        assert _resolve_candle_single_source(_request_with_settings(settings)) is True

    def test_runtime_error_falls_back_to_off(self) -> None:
        """Return False when the flag lookup raises (service-less settings).

        Given: settings whose candle_single_source raises RuntimeError,
        When: the flag is resolved,
        Then: it falls back to False rather than 500ing the candle route.
        """
        assert _resolve_candle_single_source(_request_with_settings(_RaisingSettings())) is False


def _account_state_row(**overrides: object) -> VenueAccountStateRow:
    """Build a venue account-state row dict with fail-closed defaults.

    Args:
        **overrides: Fields to override on the default observed row.

    Returns:
        A row dict shaped like ``VenueAccountStateRow`` for the read map.
    """
    now = datetime.now(UTC)
    base: dict[str, object] = {
        "wallet_public_id": "w-1",
        "exchange": "kraken",
        "mode": "live",
        "sync_status": "observed",
        "balance_status": "observed",
        "position_status": "not_applicable",
        "valuation_status": "native_only",
        "balances_json": '[{"currency": "USD", "total": 100.0, "free": 100.0, "used": 0.0}]',
        "open_positions_json": None,
        "balance_observed_at": now - dt.timedelta(seconds=30),
        "position_observed_at": None,
        "current_attempt_observation_id": 1,
        "balance_payload_source_observation_id": 1,
        "position_payload_source_observation_id": None,
        "authoritative_until": now + dt.timedelta(minutes=4),
        "error": None,
        "public_id": "acct-1",
        "timestamp": now,
        "session_id": "sess-1",
        "sequence_id": 1,
    }
    base.update(overrides)
    return cast(VenueAccountStateRow, base)


def _account_state_client(
    role: UserRole,
    repo: MockRepository,
    operator_ids: list[str] | None = None,
) -> TestClient:
    """Build a client scoped to a role/operator set with the given repo.

    Args:
        role: The principal role to authenticate as.
        repo: The repository override backing the request.
        operator_ids: The caller's operator membership set.

    Returns:
        A test client wired with CSRF, auth, and repository overrides.
    """
    app = create_app()
    app.router.lifespan_context = _noop_lifespan

    def skip_csrf() -> None:
        return None

    def scoped_auth() -> AuthPrincipal:
        return AuthPrincipal(
            username="acct_user",
            role=role,
            operator_public_ids=operator_ids or [],
        )

    app.dependency_overrides[validate_csrf_token] = skip_csrf
    app.dependency_overrides[require_authentication] = scoped_auth
    app.dependency_overrides[get_repository_dependency] = lambda: repo
    return _track_test_client(TestClient(app))


class TestPortfolioAccountsEndpoint:
    """Tests for GET /api/portfolio/accounts (PnL Phase 3 venue account truth)."""

    def test_ai_delegate_forbidden(self) -> None:
        """AI_DELEGATE lacks READ_ACCOUNT_STATE and is denied.

        Given: an AI_DELEGATE principal (no read:account_state grant),
        When: GET /portfolio/accounts is called,
        Then: the response is 403 before the route body runs.
        """
        repo = MockRepository()
        client = _account_state_client(UserRole.AI_DELEGATE, repo)
        response = client.get("/api/portfolio/accounts")
        assert response.status_code == 403
        assert repo.reconciliation_context_calls == []

    def test_returns_mapped_states_with_provenance(self) -> None:
        """Effective status and authority flags propagate through the map.

        Given: a stale observed row and a fresh observed row,
        When: an ADMIN calls GET /portfolio/accounts,
        Then: the stale row maps to effective_status=stale/is_authoritative
            False and the fresh row to observed/True, and the provenance
            envelope is present.
        """
        past = datetime(2026, 7, 13, 11, 0, tzinfo=UTC)
        future = datetime(2099, 1, 1, tzinfo=UTC)
        stale = _account_state_row(
            public_id="acct-stale",
            sync_status="observed",
            authoritative_until=past,
        )
        fresh = _account_state_row(
            public_id="acct-fresh",
            sync_status="observed",
            authoritative_until=future,
            balances_json=json.dumps(
                [{"currency": "USD", "total": 1000.0, "free": 900.0, "used": 100.0}]
            ),
        )
        repo = MockRepository(account_state_rows=[stale, fresh])
        client = _account_state_client(UserRole.ADMIN, repo)
        response = client.get("/api/portfolio/accounts")
        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "portfolio_account_state_list"
        assert data["count"] == 2
        by_id = {item["public_id"]: item for item in data["payload"]}
        assert by_id["acct-stale"]["effective_status"] == "stale"
        assert by_id["acct-stale"]["is_authoritative"] is False
        assert by_id["acct-fresh"]["effective_status"] == "observed"
        assert by_id["acct-fresh"]["is_authoritative"] is True
        assert by_id["acct-fresh"]["balances"][0]["currency"] == "USD"
        assert by_id["acct-fresh"]["reconciliation"]["effective_status"] == "incomplete"
        assert by_id["acct-fresh"]["reconciliation"]["is_authoritative"] is False
        assert data["session_id"]
        assert data["sequence_id"] >= 1
        assert data["public_id"]
        assert data["timestamp"]
        assert repo.reconciliation_context_calls == [None]

    def test_wallet_scoping_returns_only_accessible_rows(self) -> None:
        """resolve_readable_wallets narrows results to the readable set.

        Given: an OPERATOR whose accessible set is only ``w-visible`` and a
            repo holding rows for ``w-visible`` and ``w-hidden``,
        When: GET /portfolio/accounts is called,
        Then: only the accessible wallet's row is returned.
        """
        visible = _account_state_row(public_id="acct-visible", wallet_public_id="w-visible")
        hidden = _account_state_row(public_id="acct-hidden", wallet_public_id="w-hidden")
        repo = MockRepository(
            account_state_rows=[visible, hidden],
            accessible_wallet_ids=["w-visible"],
        )
        client = _account_state_client(UserRole.OPERATOR, repo, operator_ids=["op-1"])
        response = client.get("/api/portfolio/accounts")
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 1
        assert data["payload"][0]["public_id"] == "acct-visible"
        assert data["payload"][0]["wallet_public_id"] == "w-visible"
        assert repo.reconciliation_context_calls == [["w-visible"]]

    def test_read_granted_wallet_is_visible_without_membership(self) -> None:
        """A membership-less caller still sees their read-granted wallet.

        Given: an OPERATOR with an EMPTY operator set holding a read grant
            on ``w-granted``, and a repo carrying rows for ``w-granted``
            and ``w-hidden``,
        When: GET /portfolio/accounts is called,
        Then: only the read-granted row is returned — the read plane must
            not inherit the trade plane's empty-operator short-circuit,
            which would have produced an empty payload.
        """
        granted = _account_state_row(public_id="acct-granted", wallet_public_id="w-granted")
        hidden = _account_state_row(public_id="acct-hidden", wallet_public_id="w-hidden")
        repo = MockRepository(
            account_state_rows=[granted, hidden],
            accessible_wallet_ids=["w-hidden"],
        )
        repo.grant_read_access(["w-granted"])
        client = _account_state_client(UserRole.OPERATOR, repo, operator_ids=[])
        response = client.get("/api/portfolio/accounts")
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 1
        assert data["payload"][0]["wallet_public_id"] == "w-granted"
        assert repo.reconciliation_context_calls == [["w-granted"]]

    def test_duplicate_active_rows_corrupt_account_and_reconciliation(self) -> None:
        """Active multiplicity fails closed across the whole REST account item.

        Given: A coherent fresh observed row in a context whose loader detected
            duplicate active truth.
        When: An ADMIN reads the portfolio account route.
        Then: Both the account and nested reconciliation are corrupt and
            non-authoritative, and account payloads are cleared.
        """
        repo = MockRepository(
            account_state_rows=[_account_state_row()],
            duplicate_active_rows=True,
        )
        client = _account_state_client(UserRole.ADMIN, repo)

        response = client.get("/api/portfolio/accounts")

        assert response.status_code == 200
        state = response.json()["payload"][0]
        assert state["effective_status"] == "corrupt"
        assert state["is_authoritative"] is False
        assert state["balances"] is None
        assert state["open_positions"] is None
        assert state["reconciliation"]["effective_status"] == "corrupt"
        assert state["reconciliation"]["is_authoritative"] is False

    def test_empty_returns_empty_list(self) -> None:
        """No rows yields an empty payload with count 0.

        Given: a repo with no account-state rows,
        When: an ADMIN calls GET /portfolio/accounts,
        Then: the response is 200 with an empty payload and count 0.
        """
        repo = MockRepository()
        client = _account_state_client(UserRole.ADMIN, repo)
        response = client.get("/api/portfolio/accounts")
        assert response.status_code == 200
        data = response.json()
        assert data["payload"] == []
        assert data["count"] == 0
        assert repo.reconciliation_context_calls == [None]

    def test_foreign_operator_scope_403_propagates(self) -> None:
        """A 403 from resolve_readable_wallets is not remapped to 500.

        Given: an OPERATOR scoped to ``op-1`` querying a foreign operator,
        When: GET /portfolio/accounts?operator_public_id=op-foreign is called,
        Then: the 403 propagates through the ``except HTTPException`` guard.
        """
        repo = MockRepository()
        client = _account_state_client(UserRole.OPERATOR, repo, operator_ids=["op-1"])
        response = client.get("/api/portfolio/accounts?operator_public_id=op-foreign")
        assert response.status_code == 403
        assert repo.reconciliation_context_calls == []

    def test_database_error_returns_500(self) -> None:
        """A repository failure surfaces as a 500 with a stable detail.

        Given: a repository whose reads raise,
        When: an ADMIN calls GET /portfolio/accounts,
        Then: the response is 500 with the account-fetch error detail.
        """
        repo = MockRepository(error=Exception("account query failed"))
        client = _account_state_client(UserRole.ADMIN, repo)
        response = client.get("/api/portfolio/accounts")
        assert response.status_code == 500
        assert "Failed to fetch portfolio accounts" in response.json()["detail"]
        assert repo.reconciliation_context_calls == [None]
