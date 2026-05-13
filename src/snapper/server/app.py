"""FastAPI application factory and REST API endpoints.

This module provides the main FastAPI application factory ``create_app()``
and REST API endpoints for the Snapper trading dashboard.

Application Components:
    - **Lifespan management**: Database connections, ZMQ bridge, process manager.
    - **Authentication**: JWT tokens, CSRF protection, WebSocket auth.
    - **REST endpoints**: Health, candles, orders, signals, executions, positions.
    - **WebSocket**: Real-time market data streaming via ZMQ bridge.
    - **Static files**: Frontend dashboard served from ``frontend/dist``.

Lifespan Events:
    On startup:
        1. Initialize SettingsService with database access
        2. Configure TokenManager, CSRFManager, WsTokenService
        3. Discover and autostart registered processes
        4. Start ZMQ-to-WebSocket bridge

    On shutdown:
        1. Stop ZMQ bridge and all processes
        2. Cleanup WebSocket connections
        3. Dispose database connections

Example:
    Running the server::

        from snapper.server.app import create_app
        import uvicorn

        app = create_app()
        uvicorn.run(app, host="0.0.0.0", port=8000)

    Or via CLI::

        snapper server --host 0.0.0.0 --port 8000
"""

import asyncio
import contextlib
import datetime as dt
import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Annotated
from typing import Any
from typing import cast
from uuid import uuid7

import zmq
import zmq.asyncio
from fastapi import APIRouter
from fastapi import Depends
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Query
from fastapi import Request
from fastapi import Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from loguru import logger
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

from snapper.api.auth.services.ws_token_service import WsTokenService
from snapper.api.auth.services.ws_token_service import get_ws_token_service
from snapper.api.schemas.data_responses import CandleListResponse
from snapper.api.schemas.data_responses import ContinuousCandleListResponse
from snapper.api.schemas.data_responses import ContractListResponse
from snapper.api.schemas.data_responses import ExchangeListResponse
from snapper.api.schemas.data_responses import ExecutionListResponse
from snapper.api.schemas.data_responses import FrontMonthResponse
from snapper.api.schemas.data_responses import InstrumentCapabilityListResponse
from snapper.api.schemas.data_responses import InstrumentDetailListResponse
from snapper.api.schemas.data_responses import InstrumentListResponse
from snapper.api.schemas.data_responses import OrderListResponse
from snapper.api.schemas.data_responses import PositionListResponse
from snapper.api.schemas.data_responses import RelatedInstrumentsResponse
from snapper.api.schemas.data_responses import SignalListResponse
from snapper.api.schemas.data_responses import UnderlyingAssetListResponse
from snapper.api.schemas.data_responses import UnderlyingInstrumentListResponse
from snapper.api.schemas.data_responses import VenueFeeScheduleListResponse
from snapper.api.schemas.health import ConnectionStats
from snapper.api.schemas.health import GapDetectionStats
from snapper.api.schemas.health import GapStats
from snapper.api.schemas.health import HealthCheckData
from snapper.api.schemas.health import HealthCheckResponse
from snapper.api.schemas.health import HealthTopics
from snapper.api.schemas.health import RestRateData
from snapper.api.schemas.health import RestRateExchangeStats
from snapper.api.schemas.health import RestRateResponse
from snapper.api.schemas.health import SubscriptionsStats
from snapper.api.schemas.health import TopicMetricSnapshot
from snapper.api.schemas.health import WebSocketStats
from snapper.api.schemas.health import WsStatsConfig
from snapper.api.schemas.health import WsStatsData
from snapper.api.schemas.health import WsStatsResponse
from snapper.api.schemas.health import ZmqBridgeStats
from snapper.api.schemas.health import ZmqComponents
from snapper.api.schemas.health import ZmqConfig
from snapper.api.schemas.health import ZmqHealthData
from snapper.api.schemas.health import ZmqHealthResponse
from snapper.api.schemas.process import ProcessStatus
from snapper.api.schemas.process import StrategyStatusPayload
from snapper.api.schemas.process import SystemStatusData
from snapper.api.schemas.process import SystemStatusResponse
from snapper.application.ai_review.service import AiReviewService
from snapper.application.ai_review.service import get_ai_review_service
from snapper.application.db_stats.snapshotter import DbStatsSnapshotter
from snapper.application.db_stats.snapshotter import (
    resolve_disabled as _resolve_db_metrics_disabled,
)
from snapper.application.process_manager.launcher import ProcessLauncherService
from snapper.application.process_manager.registry import discover_processes
from snapper.application.retention.scheduler import RetentionScheduler
from snapper.application.services.continuous_contract_builder import ContinuousContractBuilder
from snapper.application.services.market_cache import MarketCacheService
from snapper.application.services.market_persist_policy import MarketPersistPolicy
from snapper.application.services.market_stats import MarketStatsWorker
from snapper.application.services.settings import SettingsService
from snapper.application.services.settings import get_settings_service
from snapper.application.system_metrics.snapshotter import SystemMetricsSnapshotter
from snapper.application.trade.caps_enforcer import TradingCapsEnforcer
from snapper.auth.dependencies import CSRFManager
from snapper.auth.dependencies import get_csrf_manager
from snapper.auth.dependencies import require_permission
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.permissions import Permission
from snapper.auth.routes import router as auth_router
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.scope_grant_service import ScopeGrantService
from snapper.auth.scope_grant_service import get_scope_grant_service
from snapper.auth.tokens import TokenManager
from snapper.auth.tokens import WebSocketTokenRotator
from snapper.auth.tokens import get_token_manager
from snapper.auth.user_service import UserService
from snapper.auth.user_service import get_user_service
from snapper.auth.websocket_auth import WebSocketAuthManager
from snapper.auth.websocket_auth import get_ws_auth_manager
from snapper.config.settings import AppSettings
from snapper.config.settings import get_settings
from snapper.config.settings import get_settings_with_service
from snapper.config.settings_routes import router as settings_router
from snapper.core.partitioning import ShardOwnership
from snapper.core.types import ComponentStatusEnum
from snapper.core.types import ExchangeEnum
from snapper.core.types import HealthStatus
from snapper.core.types import HealthStatusEnum
from snapper.core.types import MarketDataExchange
from snapper.core.types import OrderExchange
from snapper.core.types import RelationshipTypeEnum
from snapper.core.types import SpawnerProcessStatus
from snapper.core.types import SpawnerProcessStatusEnum
from snapper.data.backtest_repository import BacktestRepository
from snapper.data.repository import Repository
from snapper.data.repository import dispose_repositories
from snapper.data.repository import get_repository
from snapper.data.repository_types import ContinuousCandleRow
from snapper.data.repository_types import InstrumentContractRow
from snapper.data.repository_types import InstrumentRelatedRow
from snapper.data.repository_types import InstrumentUnderlyingRow
from snapper.data.repository_types import UnderlyingAssetRow
from snapper.infrastructure.rest.tracker import get_rest_call_tracker
from snapper.infrastructure.symbols.mapper import SymbolMapperService
from snapper.interface.websocket.connection_manager import WebSocketConnectionManager
from snapper.interface.websocket.helpers import build_allowed_origins
from snapper.mcp.server import build_mcp_app
from snapper.messaging.infrastructure.gap_detector import GapDetectorStats
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.infrastructure.validated_socket import HWM_AUDIT
from snapper.messaging.infrastructure.validated_socket import ValidatedPublisher
from snapper.messaging.infrastructure.validated_socket import apply_hwm
from snapper.messaging.schemas.data import CandleData
from snapper.messaging.schemas.data import ContinuousCandleData
from snapper.messaging.schemas.data import ContinuousSeriesPartialResponse
from snapper.messaging.schemas.data import ContractData
from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.schemas.data import FrontMonthData
from snapper.messaging.schemas.data import InstrumentCapabilityData
from snapper.messaging.schemas.data import InstrumentDetailData
from snapper.messaging.schemas.data import OrderData
from snapper.messaging.schemas.data import PositionData
from snapper.messaging.schemas.data import RelatedInstrumentData
from snapper.messaging.schemas.data import RelatedInstrumentsGroup
from snapper.messaging.schemas.data import RelatedInstrumentsPayloadData
from snapper.messaging.schemas.data import RelatedInstrumentsSelected
from snapper.messaging.schemas.data import RelatedInstrumentsUnderlying
from snapper.messaging.schemas.data import RollPointDetail
from snapper.messaging.schemas.data import SignalData
from snapper.messaging.schemas.data import UnderlyingAssetData
from snapper.messaging.schemas.data import UnderlyingInstrumentData
from snapper.messaging.schemas.data import VenueFeeScheduleData
from snapper.server.ai_delegate_routes import AiIntegrationDisabledError
from snapper.server.ai_delegate_routes import ai_integration_disabled_handler
from snapper.server.ai_delegate_routes import router as ai_delegate_router
from snapper.server.ai_review_routes import router as ai_review_router
from snapper.server.alert_default_routes import router as alert_default_router
from snapper.server.alerts_routes import router as alerts_router
from snapper.server.authenticated_websocket import create_authenticated_websocket_router
from snapper.server.backtest_routes import router as backtest_router
from snapper.server.credential_routes import router as credential_router
from snapper.server.dependencies import get_caps_enforcer_dependency
from snapper.server.dependencies import get_repository_dependency
from snapper.server.device_routes import router as device_router
from snapper.server.execution_plan_routes import router as execution_plan_router
from snapper.server.json_body import patch_openapi
from snapper.server.metrics_routes import router as metrics_router
from snapper.server.operator_routes import router as operator_router
from snapper.server.order_routes import router as order_router
from snapper.server.position_cycle_routes import router as position_cycle_router
from snapper.server.process_routes import router as process_router
from snapper.server.provenance_middleware import ClientProvenanceMiddleware
from snapper.server.rate_limiting import limiter
from snapper.server.scope_grant_routes import router as scope_grant_router
from snapper.server.scoping import resolve_target_wallets
from snapper.server.strategy_routes import router as strategy_router
from snapper.server.trailing_stop_routes import router as trailing_stop_router
from snapper.server.wallet_routes import router as wallet_router
from snapper.utils.logging import set_log_context

API_PREFIX = "/api"
_INTERNAL_SERVER_ERROR_DESCRIPTION = "Internal server error"
_UNDERLYING_NOT_FOUND_DESCRIPTION = "Underlying not found"


def handle_rate_limit_exceeded(request: Request, exc: Exception) -> Response:
    """Return 429 response when rate limit is exceeded.

    Args:
        request: Incoming HTTP request.
        exc: Rate limit exceeded exception.

    Returns:
        JSON response with 429 status code and retry-after header.
    """
    detail = getattr(exc, "detail", str(exc))
    resp = Response(f"Rate limit exceeded: {detail}", status_code=429)
    retry_after = getattr(request.state, "view_rate_limit", None)
    if retry_after:
        resp.headers["Retry-After"] = str(retry_after)
    return resp


def get_settings_dependency() -> AppSettings:
    """FastAPI dependency for application settings.

    Returns:
        Cached AppSettings instance (bootstrap only, no DB access).
    """
    return get_settings()


async def _initialize_settings_service(settings: AppSettings) -> SettingsService:
    """Initialize and configure SettingsService with ZMQ synchronization.

    Args:
        settings: Bootstrap application settings.

    Returns:
        Initialized SettingsService instance.
    """
    settings_service = await get_settings_service(
        settings.db_url,
        settings.zmq_broker_xsub,
    )
    logger.info("AppSettings initialized with database access (cached, ZMQ-synced)")
    return settings_service


def _configure_auth_services(settings_service: SettingsService) -> None:
    """Configure authentication services with SettingsService.

    Args:
        settings_service: Initialized SettingsService instance.
    """
    token_manager = get_token_manager()
    token_manager.set_settings_service(settings_service)
    logger.info("TokenManager initialized with database settings")
    csrf_manager = get_csrf_manager()
    csrf_manager.set_settings_service(settings_service)
    logger.info("CSRFManager initialized with database settings")
    ws_token_service = get_ws_token_service()
    ws_token_service.set_settings_service(settings_service)
    logger.info("WsTokenService initialized with database settings")


def _build_user_service_publisher(
    zmq_broker_xsub: str,
) -> tuple[MessagePublisher, zmq.asyncio.Context]:
    """Open a fresh ZMQ PUB socket for `admin.user_deactivated` fanout.

    UserService is the SOLE publisher of `admin.user_deactivated`.
    Created
    here and disposed in the lifespan `finally` so the broker side never
    sees a half-closed socket between requests.
    Per the broker contract documented in
    `snapper.config.bootstrap.BootstrapSettingsLoader` and the
    `ZmqBrokerProcess` proxy, publishers connect to the broker's XSUB
    endpoint (publishers ──[connect]──> XSUB ── proxy ── XPUB
    ──[connect]──> Subscribers). Wiring the PUB socket to the XPUB
    endpoint silently drops every message.

    Args:
        zmq_broker_xsub: Address of the broker's XSUB endpoint
            (publishers connect here per the broker contract).

    Returns:
        Tuple of `(MessagePublisher, zmq.asyncio.Context)`. The context
        is returned so the lifespan can `term()` it on shutdown.
    """
    context = zmq.asyncio.Context()
    raw_socket = context.socket(zmq.PUB)
    apply_hwm(raw_socket, sndhwm=HWM_AUDIT)
    raw_socket.connect(zmq_broker_xsub)
    publisher = MessagePublisher(ValidatedPublisher(raw_socket), SequenceTracker())
    return publisher, context


def _shutdown_user_service_publisher(app: FastAPI) -> None:
    """Close UserService's `admin.user_deactivated` publisher socket.

    Mirrors `SettingsService.shutdown` ordering: clear all four
    singleton publisher references (UserService, ScopeGrantService,
    AiReviewService and WebSocketAuthManager share the same socket by
    design) so any in-flight `deactivate_user` / `revoke_grant` /
    `handle_caps_violation_bus_message` /
    `_publish_delegate_offline` call observes a None publisher
    (graceful degradation), then close the socket and terminate the
    context. ``contextlib.suppress(Exception)`` mirrors the
    `SettingsService.shutdown` resilience contract.
    """
    get_user_service().set_msg_publisher(None)
    get_scope_grant_service().set_msg_publisher(None)
    get_ai_review_service().set_msg_publisher(None)
    get_ws_auth_manager().set_msg_publisher(None)
    caps_enforcer_for_shutdown = _safe_get_caps_enforcer()
    if caps_enforcer_for_shutdown is not None:
        caps_enforcer_for_shutdown.set_msg_publisher(None)
    publisher = getattr(app.state, "user_service_publisher", None)
    context = getattr(app.state, "user_service_publisher_context", None)
    if publisher is not None:
        with contextlib.suppress(Exception):
            publisher.setsockopt(zmq.LINGER, 0)
        with contextlib.suppress(Exception):
            publisher.close()
    if context is not None:
        with contextlib.suppress(Exception):
            context.term()
    app.state.user_service_publisher = None
    app.state.user_service_publisher_context = None


def _clear_runtime_singletons() -> None:
    """Clear process-local auth and settings singletons during app shutdown."""
    for singleton_cls in (
        SymbolMapperService,
        WebSocketAuthManager,
        WebSocketTokenRotator,
        UserService,
        ScopeGrantService,
        AiReviewService,
        WsTokenService,
        CSRFManager,
        TokenManager,
        SettingsService,
    ):
        try:
            singleton_cls.clear_instance()
        except Exception:
            continue


async def _reconcile_stale_backtests(app: FastAPI) -> None:
    """Mark orphaned backtest runs as failed at boot time.

    Args:
        app: FastAPI application instance.
    """
    try:
        repo: Repository = get_repository_dependency()
        bt_repo = BacktestRepository(cast(Any, repo).session_factory)
        tracker: SequenceTracker = app.state.rest_tracker
        now = datetime.now(UTC)
        count = await bt_repo.reconcile_stale_runs(
            bus_time=now,
            session_id=tracker.session_id,
            sequence_id=tracker.next_sequence("backtest_reconcile"),
        )
        if count > 0:
            logger.info("Reconciled {} stale backtest run(s) as failed", count)
    except Exception as exc:
        logger.warning("Backtest reconciliation failed (non-fatal): {}", exc)


_TRADFI_EXPIRY_ALERT_WINDOW_DAYS = 14


async def _warn_on_tradfi_near_expiry(settings: AppSettings) -> None:
    """Log WARN for each configured TradFi default symbol near expiry.

     Mandates a 14-day expiry alert. Operators must
    rotate the ``AppSettings.instruments[KRAKEN_EQUITIES]`` default list
    before contracts drop below that window; this check turns the
    docstring-only guidance into a runtime signal on every server
    restart so the operator sees the warning in startup logs.
    The check is best-effort: any query failure is logged at WARN and
    swallowed so a partially-initialised database cannot prevent
    application startup.

    Args:
        settings: Application settings; the KRAKEN_EQUITIES default
            list is sourced from ``settings.instruments``.
    """
    try:
        exchange = str(ExchangeEnum.KRAKEN_EQUITIES)
        defaults = settings.instruments.get(exchange, [])
        if not defaults:
            return
        repo: Repository = get_repository_dependency()
        now = datetime.now(UTC)
        threshold = now + timedelta(days=_TRADFI_EXPIRY_ALERT_WINDOW_DAYS)
        detail_rows = await repo.get_exchange_instruments_detail(exchange=exchange, as_of=now)
        for row in detail_rows:
            if row["symbol"] not in defaults:
                continue
            expiry_at = row["expiry_at"]
            if expiry_at is None:
                continue
            if expiry_at <= threshold:
                days_remaining = max(0, (expiry_at - now).days)
                logger.warning(
                    "TradFi default symbol {} expires in {} day(s) "
                    "(expiry_at={}); rotate AppSettings.instruments["
                    "KRAKEN_EQUITIES] per docs/operations.md 'Kraken "
                    "Equities (TradFi) market data' section.",
                    row["symbol"],
                    days_remaining,
                    expiry_at.isoformat(),
                )
    except Exception as exc:
        logger.warning("TradFi expiry check failed (non-fatal): {}", exc)


async def _start_system_metrics_snapshotter(app: FastAPI) -> None:
    """Build + start the :class:`SystemMetricsSnapshotter` singleton.

    The attribute is assigned to ``app.state`` ONLY after a successful
    :meth:`SystemMetricsSnapshotter.start` call (B22 — no
    half-initialized object can bypass the route layer's 503 fallback).
    On any exception, the attribute is left absent and the route layer
    falls through to HTTP 503 ``"system metrics snapshotter not
    available"``. Failure does NOT block the rest of the lifespan
    startup; the app still serves other endpoints.

    Args:
        app: FastAPI application instance whose ``state`` will hold the
            singleton on successful start.
    """
    try:
        snapshotter = SystemMetricsSnapshotter()
        await snapshotter.start()
    except Exception:
        logger.exception(
            "SystemMetricsSnapshotter startup failed — metrics endpoints will return 503"
        )
        return
    app.state.system_metrics_snapshotter = snapshotter
    logger.info("SystemMetricsSnapshotter started (eager sample buffered)")


async def _stop_system_metrics_snapshotter(app: FastAPI) -> None:
    """Stop the :class:`SystemMetricsSnapshotter` singleton if attached.

    Tolerates partial-init state where startup failed before the
    attribute was assigned.

    Args:
        app: FastAPI application instance.
    """
    snapshotter: SystemMetricsSnapshotter | None = getattr(
        app.state, "system_metrics_snapshotter", None
    )
    if snapshotter is None:
        return
    await snapshotter.stop()


async def _start_retention_scheduler(app: FastAPI, *, db_url: str) -> None:
    """Build + start the :class:`RetentionScheduler` singleton.

    The attribute is assigned to ``app.state`` ONLY after a successful
    :meth:`RetentionScheduler.start` call (B22 — no half-initialized
    object can bypass the route layer's 503 fallback). On any
    exception, the attribute is left absent and the route layer falls
    through to HTTP 503. Failure does NOT block the rest of the
    lifespan startup; the app still serves other endpoints.

    Args:
        app: FastAPI application instance whose ``state`` will hold
            the scheduler on successful start.
        db_url: SQLAlchemy URL for the underlying sync repository.
    """
    try:
        scheduler = RetentionScheduler(db_url=db_url)
        await scheduler.start()
    except Exception:
        logger.exception(
            "RetentionScheduler startup failed — retention metrics endpoint will return 503"
        )
        return
    app.state.retention_scheduler = scheduler
    if scheduler.disabled:
        logger.info("RetentionScheduler started in disabled mode (RETENTION_DISABLED=true)")
    else:
        logger.info(
            "RetentionScheduler started (eager run buffered, interval=%.1fs)",
            scheduler.interval_seconds,
        )


async def _stop_retention_scheduler(app: FastAPI) -> None:
    """Stop the :class:`RetentionScheduler` singleton if attached.

    Tolerates partial-init state where startup failed before the
    attribute was assigned.

    Args:
        app: FastAPI application instance.
    """
    scheduler: RetentionScheduler | None = getattr(app.state, "retention_scheduler", None)
    if scheduler is None:
        return
    await scheduler.stop()


async def _start_db_stats_snapshotter(app: FastAPI, *, db_url: str) -> None:
    """Build + start the :class:`DbStatsSnapshotter` singleton.

    Mirrors the B22 attribute-absent contract used for the system
    metrics snapshotter and retention scheduler: the attribute is
    assigned to ``app.state`` ONLY after a successful
    :meth:`DbStatsSnapshotter.start` call. Disabled mode still
    constructs the snapshotter and assigns it so the metrics route can
    distinguish disabled-via-env from missing-init from
    no-sample-yet — the underlying repo is ``None`` and the loop is
    skipped.

    Args:
        app: FastAPI application instance whose ``state`` will hold
            the snapshotter on successful start.
        db_url: SQLAlchemy URL for the underlying async repository.
    """
    try:
        disabled = _resolve_db_metrics_disabled(os.environ.get("DB_METRICS_DISABLED"))
        repo = None if disabled else get_repository(db_url)
        snapshotter = DbStatsSnapshotter(repo=repo, disabled=disabled)
        await snapshotter.start()
    except Exception:
        logger.exception("DbStatsSnapshotter startup failed — DB metrics endpoint will return 503")
        return
    app.state.db_stats_snapshotter = snapshotter
    if snapshotter.disabled:
        logger.info("DbStatsSnapshotter started in disabled mode (DB_METRICS_DISABLED=true)")
    else:
        logger.info("DbStatsSnapshotter started (interval=%.1fs)", snapshotter.interval_seconds)


async def _stop_db_stats_snapshotter(app: FastAPI) -> None:
    """Stop the :class:`DbStatsSnapshotter` singleton if attached.

    Tolerates partial-init state where startup failed before the
    attribute was assigned.

    Args:
        app: FastAPI application instance.
    """
    snapshotter: DbStatsSnapshotter | None = getattr(app.state, "db_stats_snapshotter", None)
    if snapshotter is None:
        return
    await snapshotter.stop()


async def _shutdown_zmq_bridge(app: FastAPI) -> None:
    """Stop ZMQ bridge and await its task during shutdown.

    Tolerates partial-init state where
    `app.state.manager` was never attached (lifespan startup
    raised before the WebSocket connection manager was wired).
    Returns silently in that case so the rest of the lifespan
    `finally` chain still runs.

    Args:
        app: FastAPI application instance.
    """
    ws_manager: WebSocketConnectionManager | None = getattr(app.state, "manager", None)
    bridge_task = getattr(app.state, "zmq_bridge_task", None)
    if ws_manager is None or not ws_manager.zmq_bridge or bridge_task is None:
        return
    await ws_manager.zmq_bridge.stop()
    try:
        await bridge_task
    except (Exception, asyncio.CancelledError) as e:
        logger.warning(f"ZMQ bridge task failed during shutdown: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    """Application lifespan context manager.

    Manages startup and shutdown sequences including:
        - SettingsService initialization with ZMQ synchronization
        - TokenManager and CSRFManager configuration
        - Process discovery and autostart
        - ZMQ-WebSocket bridge startup

    Args:
        app: FastAPI application instance.

    Yields:
        Control to the application during its lifetime.
    """
    set_log_context("api")
    settings = get_settings()
    settings_service: SettingsService | None = None
    process_factory: ProcessLauncherService | None = None
    app.state.zmq_bridge_task = None
    app.state.system_metrics_snapshotter = None
    app.state.retention_scheduler = None
    app.state.db_stats_snapshotter = None
    app.state.market_persist_policy = None
    app.state.market_cache = None
    app.state.market_stats_worker = None
    try:
        settings_service = await _initialize_settings_service(settings)
        settings = get_settings_with_service(settings_service)
        app.state.settings = settings
        app.state.settings_service = settings_service
        _configure_auth_services(settings_service)
        user_publisher, user_publisher_context = _build_user_service_publisher(
            settings.zmq_broker_xsub
        )
        app.state.user_service_publisher = user_publisher
        app.state.user_service_publisher_context = user_publisher_context
        get_user_service().set_msg_publisher(user_publisher)
        logger.info("UserService publisher wired to ZMQ broker for admin.user_deactivated")
        get_scope_grant_service().set_msg_publisher(user_publisher)
        logger.info("ScopeGrantService publisher wired to ZMQ broker for admin.scope_revoked")
        get_ai_review_service().set_msg_publisher(user_publisher)
        logger.info("AiReviewService publisher wired to ZMQ broker for ai_reviews.* fanout")
        caps_enforcer_for_publisher = _safe_get_caps_enforcer()
        if caps_enforcer_for_publisher is not None:
            caps_enforcer_for_publisher.set_msg_publisher(user_publisher)
            logger.info(
                "TradingCapsEnforcer publisher wired to ZMQ broker for bus.caps_violation_after_ai_approve"
            )
        ws_auth_manager = get_ws_auth_manager()
        ws_auth_manager.set_msg_publisher(user_publisher)
        logger.info("WebSocketAuthManager publisher wired to ZMQ broker for bus.delegate_offline")
        manager_for_wiring: WebSocketConnectionManager = app.state.manager
        ws_auth_manager.set_wiring(
            connection_manager=manager_for_wiring,
            zmq_bridge=manager_for_wiring.zmq_bridge,
            repository_factory=lambda: get_repository(settings.db_url),
        )
        await ws_auth_manager.start_admin_listener(settings.zmq_broker_xpub)
        await get_token_manager().start_admin_listener(settings.zmq_broker_xpub)
        ai_review_service = get_ai_review_service()
        ai_review_service.set_repository_factory(lambda: get_repository(settings.db_url))
        ai_review_service.set_shard_ownership(
            ShardOwnership(
                instance_id=settings.coordinator_instance_id,
                instance_count=settings.coordinator_instance_count,
            )
        )
        await ai_review_service.start_bus_listener(settings.zmq_broker_xpub)
        logger.info(
            "AiReviewService bus listener subscribed to bus.delegate_offline + bus.caps_violation_after_ai_approve "
            "(caps-violation fanout gated by ShardOwnership instance {}/{})",
            settings.coordinator_instance_id,
            settings.coordinator_instance_count,
        )
        market_persist_policy = MarketPersistPolicy(
            repository=get_repository(settings.db_url),
            settings_service=settings_service,
        )
        await market_persist_policy.initial_rebuild()
        await market_persist_policy.start_admin_listener(settings.zmq_broker_xpub)
        app.state.market_persist_policy = market_persist_policy
        logger.info(
            "MarketPersistPolicy initialized and listening on admin.scope_* + system.settings"
        )
        market_cache = MarketCacheService(
            repository=get_repository(settings.db_url),
            persist_policy=market_persist_policy,
        )
        await market_cache.start(settings.zmq_broker_xpub)
        app.state.market_cache = market_cache
        logger.info("MarketCacheService initialized and ingesting market.* candle frames")
        market_stats_worker = MarketStatsWorker(
            cache=market_cache,
            settings_service=settings_service,
        )
        await market_stats_worker.start(settings.zmq_broker_xpub)
        app.state.market_stats_worker = market_stats_worker
        logger.info("MarketStatsWorker started (Pearson 60s + cointegration 300s)")
        discover_processes()
        process_factory = ProcessLauncherService(settings)
        process_factory.set_market_persist_policy(market_persist_policy)
        app.state.process_factory = process_factory
        logger.info("Starting application lifespan - checking autostart settings")
        await process_factory.sync_registry_to_database()
        if settings.server_api_only:
            logger.info("API-only mode (SERVER_API_ONLY=true) — skipping process autostart")
        else:
            await process_factory.start_all_processes()
            await process_factory.spawn_per_wallet_executors()
            await _reconcile_stale_backtests(app)
        plan_executor = process_factory.started_processes.get("plan_executor")
        app.state.plan_executor = plan_executor
        manager_ref: WebSocketConnectionManager = app.state.manager
        app.state.zmq_bridge_task = asyncio.create_task(manager_ref.zmq_bridge.start())
        await _start_system_metrics_snapshotter(app)
        await _start_retention_scheduler(app, db_url=settings.db_url)
        await _start_db_stats_snapshotter(app, db_url=settings.db_url)
        logger.info("Application startup complete")
        await _warn_on_tradfi_near_expiry(settings)
        mcp_sub_app = app.state.mcp_sub_app
        try:
            async with mcp_sub_app.router.lifespan_context(mcp_sub_app):
                logger.info("MCP sub-app session manager started")
                yield
        except RuntimeError as exc:
            if "can only be called once" not in str(exc):
                raise
            logger.warning(
                "MCP sub-app session manager already running — nested lifespan enter (test fixture reuse)"
            )
            yield
    except asyncio.CancelledError:
        logger.info("Application lifespan cancelled by shutdown signal")
        raise
    finally:
        logger.info("Starting application shutdown sequence")
        await _stop_db_stats_snapshotter(app)
        await _stop_retention_scheduler(app)
        await _stop_system_metrics_snapshotter(app)
        await _shutdown_zmq_bridge(app)
        if process_factory is not None:
            await process_factory.stop_all_processes()
        market_stats_worker_for_shutdown = getattr(app.state, "market_stats_worker", None)
        if market_stats_worker_for_shutdown is not None:
            await market_stats_worker_for_shutdown.stop()
        market_cache_for_shutdown = getattr(app.state, "market_cache", None)
        if market_cache_for_shutdown is not None:
            await market_cache_for_shutdown.stop()
        market_persist_policy_for_shutdown = getattr(app.state, "market_persist_policy", None)
        if market_persist_policy_for_shutdown is not None:
            await market_persist_policy_for_shutdown.stop()
        manager = getattr(app.state, "manager", None)
        if manager is not None:
            await manager.cleanup()
        if settings_service is not None:
            await settings_service.shutdown()
        ws_auth_manager_for_shutdown = get_ws_auth_manager()
        await ws_auth_manager_for_shutdown.cancel_pending_offline_tasks()
        await ws_auth_manager_for_shutdown.stop_admin_listener()
        await get_token_manager().stop_admin_listener()
        await get_ai_review_service().stop_bus_listener()
        get_ai_review_service().set_repository_factory(None)
        get_ai_review_service().set_shard_ownership(None)
        _shutdown_user_service_publisher(app)
        _clear_runtime_singletons()
        await dispose_repositories()
        logger.info("Application shutdown complete")


def _safe_get_caps_enforcer() -> TradingCapsEnforcer | None:
    """Return the caps-enforcer singleton if available, else ``None``.

    MCP tools wire the enforcer via a lazy getter so that the sub-app
    can be mounted before the FastAPI lifespan has attached a
    SQLAlchemyRepository to the dependency cache. The underlying
    :func:`get_caps_enforcer_dependency` raises ``RuntimeError`` on
    that condition; we convert the pre-startup window into ``None``
    and let individual tools raise a clearer
    "lifespan not ready" error at invocation time.

    Returns:
        The cached :class:`TradingCapsEnforcer` singleton, or ``None``
        during the brief pre-lifespan window.
    """
    try:
        return get_caps_enforcer_dependency()
    except RuntimeError:
        return None


def create_app() -> FastAPI:
    """Create and configure the FastAPI application.

    Sets up:
        - CORS middleware with allowed origins from settings
        - Authentication routes (login, logout, refresh)
        - Settings management routes (admin only)
        - Process management routes
        - REST API for market data, orders, signals
        - WebSocket endpoint for real-time streaming
        - Static file serving for frontend dashboard

    Returns:
        Configured FastAPI application instance.
    """
    app = FastAPI(
        title="Snapper Trading Dashboard",
        description="Real-time trading data and analytics dashboard",
        version="0.1.0",
        lifespan=lifespan,
    )

    @app.middleware("http")
    async def set_request_context(request: Request, call_next: Any) -> Any:
        set_log_context("api")
        response = await call_next(request)
        return response

    settings = get_settings()
    allowed_origins = list(build_allowed_origins(settings))
    provenance_gap_detectors: dict[str, Any] = {}
    app.state.limiter = limiter
    app.state.provenance_gap_detectors = provenance_gap_detectors
    app.add_exception_handler(RateLimitExceeded, handle_rate_limit_exceeded)
    app.add_exception_handler(AiIntegrationDisabledError, ai_integration_disabled_handler)
    app.add_middleware(SlowAPIMiddleware)
    rest_tracker = SequenceTracker()
    app.state.rest_tracker = rest_tracker
    app.add_middleware(
        ClientProvenanceMiddleware,
        db_url=settings.db_url,
        telemetry_enabled=settings.telemetry_recording_enabled,
        gap_detectors=provenance_gap_detectors,
        tracker=rest_tracker,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=["Content-Type", "Authorization", "X-CSRF-Token"],
    )
    manager = WebSocketConnectionManager()
    app.include_router(auth_router, prefix=API_PREFIX)
    app.include_router(settings_router, prefix=API_PREFIX)
    app.include_router(ai_delegate_router, prefix=API_PREFIX)
    app.include_router(ai_review_router, prefix=API_PREFIX)
    app.include_router(process_router, prefix=API_PREFIX)
    app.include_router(strategy_router, prefix=API_PREFIX)
    app.include_router(wallet_router, prefix=API_PREFIX)
    app.include_router(operator_router, prefix=API_PREFIX)
    app.include_router(scope_grant_router, prefix=API_PREFIX)
    app.include_router(credential_router, prefix=API_PREFIX)
    app.include_router(order_router, prefix=API_PREFIX)
    app.include_router(execution_plan_router, prefix=API_PREFIX)
    app.include_router(position_cycle_router, prefix=API_PREFIX)
    app.include_router(trailing_stop_router, prefix=API_PREFIX)
    app.include_router(backtest_router, prefix=API_PREFIX)
    app.include_router(device_router, prefix=API_PREFIX)
    app.include_router(alert_default_router, prefix=API_PREFIX)
    app.include_router(metrics_router, prefix=API_PREFIX)
    app.include_router(alerts_router, prefix=API_PREFIX)
    app.include_router(create_api_router(manager), prefix=API_PREFIX)
    app.include_router(create_authenticated_websocket_router(manager), prefix=API_PREFIX)

    mcp_sub_app = build_mcp_app(
        settings_service_getter=lambda: getattr(app.state, "settings_service", None),
        repository_getter=get_repository_dependency,
        caps_enforcer_getter=_safe_get_caps_enforcer,
        tracker_getter=lambda: getattr(app.state, "rest_tracker", None),
    )
    app.state.mcp_sub_app = mcp_sub_app
    app.mount("/api/mcp", mcp_sub_app)

    patch_openapi(app)

    app.state.manager = manager
    if os.path.exists("frontend/dist"):
        app.mount("/", StaticFiles(directory="frontend/dist", html=True), name="static")
    return app


_STRATEGY_STATUS_KEYS: tuple[str, ...] = (
    "signals_generated",
    "trades_executed",
    "last_signal",
    "last_signal_time",
    "pnl",
    "pid",
    "uptime",
)


def _normalize_strategy_status(status: dict[str, Any]) -> dict[str, Any]:
    """Normalize strategy status dictionary keys to strings.

    Args:
        status: Raw strategy status dict.

    Returns:
        New dict with all keys converted to strings.
    """
    return {str(key): value for key, value in status.items()}


_ZMQ_HEARTBEAT_INTERVAL_DEFAULT_MS = 1000


def _resolve_zmq_heartbeat_interval_ms(request: Request) -> int:
    """Resolve the ZMQ heartbeat interval from DB-aware settings when available.

    The ``zmq_bridge.settings`` instance is a bootstrap-only ``AppSettings``
    without a ``SettingsService``, so reading
    ``zmq_heartbeat_interval_ms`` directly off it raises ``RuntimeError``.
    The lifespan startup hook attaches the DB-aware settings instance to
    ``request.app.state.settings``; fall back to the static default
    when state is missing (e.g. ``TestClient`` setups that bypass the
    lifespan).

    Args:
        request: FastAPI request whose app state we consult.

    Returns:
        Heartbeat interval in milliseconds.
    """
    settings = getattr(request.app.state, "settings", None)
    if settings is None:
        return _ZMQ_HEARTBEAT_INTERVAL_DEFAULT_MS
    try:
        return int(settings.zmq_heartbeat_interval_ms)
    except RuntimeError:
        return _ZMQ_HEARTBEAT_INTERVAL_DEFAULT_MS


def _build_strategy_payload(
    raw_status: dict[str, Any],
) -> StrategyStatusPayload | None:
    """Build a strategy payload from a raw process status.

    Returns None if the raw status does not represent a strategy.

    Args:
        raw_status: Raw process status dictionary.

    Returns:
        StrategyStatusPayload or None if not a strategy status.
    """
    if not isinstance(raw_status, dict):
        return None
    if "strategy_name" not in raw_status:
        return None
    normalized = _normalize_strategy_status(raw_status)
    extra: dict[str, Any] = {}
    for key in _STRATEGY_STATUS_KEYS:
        if key in normalized:
            extra[key] = normalized[key]
    return StrategyStatusPayload(
        strategy_name=str(normalized.get("strategy_name", "unknown")),
        status=normalized.get("status", "unknown"),
        details=normalized,
        **extra,
    )


TRADER_COORDINATOR_PROCESS = "trader_coordinator"
_REST_HEALTH_STREAM = "rest.health"
_REST_DATA_STREAM = "rest.data"


def _resolve_trader_status(
    process_factory: ProcessLauncherService,
) -> ProcessStatus:
    """Derive trader coordinator status from the process launcher.

    Checks whether the trader_coordinator process is currently running
    and returns an appropriate ProcessStatus.

    Args:
        process_factory: Process launcher service with started processes.

    Returns:
        ProcessStatus reflecting actual trader coordinator state.
    """
    ps_status: SpawnerProcessStatus = (
        SpawnerProcessStatusEnum.RUNNING
        if TRADER_COORDINATOR_PROCESS in process_factory.started_processes
        else SpawnerProcessStatusEnum.NOT_RUNNING
    )
    return ProcessStatus(
        status=ps_status,
    )


def _collect_strategy_statuses(
    process_factory: ProcessLauncherService,
) -> list[StrategyStatusPayload]:
    """Collect strategy statuses from all running processes.

    Args:
        process_factory: Process launcher service with started processes.

    Returns:
        List of StrategyStatusPayload instances.
    """
    strategies: list[StrategyStatusPayload] = []
    for process_name, process_instance in process_factory.started_processes.items():
        try:
            raw = process_instance.get_status()
            payload = _build_strategy_payload(raw)
            if payload is not None:
                strategies.append(payload)
        except Exception as exc:
            logger.warning(f"Failed to get status from process '{process_name}': {exc}")
    return strategies


def _create_candles_signals_router() -> APIRouter:
    """Create router for candles and signals endpoints.

    Returns:
        APIRouter with candles and signals endpoints.
    """
    router = APIRouter()

    @router.get(
        "/candles",
        response_model=None,
        responses={500: {"description": _INTERNAL_SERVER_ERROR_DESCRIPTION}},
    )
    async def get_candles(
        request: Request,
        _auth: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_MARKET_DATA))],
        _csrf: Annotated[None, Depends(validate_csrf_token)],
        repo: Annotated[Repository, Depends(get_repository_dependency)],
        instrument: Annotated[str, Query(description="Instrument symbol")],
        exchange: Annotated[MarketDataExchange, Query(description="Exchange name")],
        timeframe: Annotated[str, Query(description="Timeframe")],
        limit: Annotated[int, Query(le=1000, description="Number of candles to return")] = 100,
        as_of: Annotated[datetime | None, Query(description="Point-in-time query (UTC)")] = None,
    ) -> CandleListResponse:
        """Fetch historical candle data for an instrument.

        Args:
            request: FastAPI request (provides REST tracker for provenance).
            _auth: Authenticated user with READ_MARKET_DATA permission.
            _csrf: CSRF token validation.
            repo: Database repository.
            instrument: Instrument symbol to query.
            exchange: Exchange name to query.
            timeframe: Candle timeframe (e.g. '1m', '1h').
            limit: Maximum number of candles to return.
            as_of: Optional point-in-time query timestamp.

        Returns:
            CandleListResponse wrapping the candle data (empty payload if no instrument found).
        """
        processing_date = as_of or datetime.now(UTC)
        try:
            rows = await repo.get_candles(
                instrument,
                timeframe,
                start=None,
                end=None,
                exchange=exchange,
                as_of=processing_date,
                limit=limit,
                order="desc",
            )
            items = [
                CandleData(
                    public_id=r["public_id"],
                    timestamp=r["timestamp"],
                    session_id=r["session_id"],
                    sequence_id=r["sequence_id"],
                    instrument=instrument,
                    exchange=exchange,
                    timeframe=r["timeframe"],
                    open_at=r["open_at"],
                    open=r["open"],
                    high=r["high"],
                    low=r["low"],
                    close=r["close"],
                    volume=r["volume"],
                    vwap=r["vwap"],
                    trades=r["trades"],
                )
                for r in reversed(rows)
            ]
            tracker: SequenceTracker = request.app.state.rest_tracker
            sid = tracker.session_id
            seq = tracker.next_sequence(_REST_DATA_STREAM)
            ts = dt.datetime.now(dt.UTC)
            pid = str(uuid7())
            return CandleListResponse(
                session_id=sid,
                sequence_id=seq,
                public_id=pid,
                timestamp=ts,
                payload=items,
                count=len(items),
            )
        except HTTPException:
            raise
        except Exception as exc:
            logger.error(f"Failed to fetch candles for {instrument}: {exc}")
            raise HTTPException(status_code=500, detail="Failed to fetch candle data") from exc

    @router.get("/signals", responses={500: {"description": _INTERNAL_SERVER_ERROR_DESCRIPTION}})
    async def get_signals(
        request: Request,
        _auth: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_MARKET_DATA))],
        _csrf: Annotated[None, Depends(validate_csrf_token)],
        repo: Annotated[Repository, Depends(get_repository_dependency)],
        instrument: Annotated[str | None, Query(description="Filter by instrument")] = None,
        strategy: Annotated[str | None, Query(description="Filter by strategy")] = None,
        exchange: Annotated[OrderExchange | None, Query(description="Filter by exchange")] = None,
        hours: Annotated[int, Query(le=168, description="Hours of history to return")] = 24,
        limit: Annotated[int, Query(le=1000, description="Number of signals to return")] = 100,
        as_of: Annotated[datetime | None, Query(description="Point-in-time query (UTC)")] = None,
        operator_public_id: Annotated[str | None, Query(description="Scope to operator")] = None,
        wallet_public_id: Annotated[str | None, Query(description="Scope to wallet")] = None,
    ) -> SignalListResponse:
        """Fetch trading signals with optional filters.

        Args:
            request: FastAPI request (provides REST tracker for provenance).
            _auth: Authenticated user with READ_MARKET_DATA permission.
            _csrf: CSRF token validation.
            repo: Database repository.
            instrument: Optional instrument symbol filter.
            strategy: Optional strategy name filter.
            exchange: Optional exchange filter.
            hours: Hours of history to return.
            limit: Maximum number of signals to return.
            as_of: Optional point-in-time query timestamp.
            operator_public_id: Optional operator scope (403 if foreign).
            wallet_public_id: Optional wallet scope (403 if inaccessible).

        Returns:
            SignalListResponse wrapping the signal data.
        """
        processing_date = as_of or datetime.now(UTC)
        try:
            target_wallets = await resolve_target_wallets(
                _auth, repo, operator_public_id, wallet_public_id
            )
            since = processing_date - timedelta(hours=hours)
            rows = await repo.get_signals(
                since=since,
                limit=limit,
                as_of=processing_date,
                instrument=instrument,
                strategy=strategy,
                exchange=exchange,
                wallet_public_ids=target_wallets,
            )
            items = [SignalData(**cast(dict[str, Any], r)) for r in rows]
            tracker: SequenceTracker = request.app.state.rest_tracker
            sid = tracker.session_id
            seq = tracker.next_sequence(_REST_DATA_STREAM)
            ts = dt.datetime.now(dt.UTC)
            pid = str(uuid7())
            return SignalListResponse(
                session_id=sid,
                sequence_id=seq,
                public_id=pid,
                timestamp=ts,
                payload=items,
                count=len(items),
            )
        except HTTPException:
            raise
        except Exception as exc:
            logger.error(f"Failed to fetch signals: {exc}")
            raise HTTPException(status_code=500, detail="Failed to fetch signals") from exc

    return router


def _create_exchange_router() -> APIRouter:
    """Create router for exchange and instrument discovery endpoints.

    Returns:
        APIRouter with exchanges and instruments-per-exchange endpoints.
    """
    router = APIRouter()

    @router.get(
        "/exchanges",
        responses={500: {"description": _INTERNAL_SERVER_ERROR_DESCRIPTION}},
    )
    async def get_exchanges(
        request: Request,
        _auth: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_MARKET_DATA))],
        _csrf: Annotated[None, Depends(validate_csrf_token)],
        repo: Annotated[Repository, Depends(get_repository_dependency)],
        as_of: Annotated[datetime | None, Query(description="Point-in-time query (UTC)")] = None,
    ) -> ExchangeListResponse:
        """Return distinct exchange names from symbol_aliases.

        Args:
            request: FastAPI request (provides REST tracker for provenance).
            _auth: Authenticated user with READ_MARKET_DATA permission.
            _csrf: CSRF token validation.
            repo: Database repository.
            as_of: Optional point-in-time query timestamp.

        Returns:
            ExchangeListResponse wrapping the exchange name list.
        """
        try:
            now = as_of or datetime.now(UTC)
            items = await repo.get_exchanges(as_of=now)
            tracker: SequenceTracker = request.app.state.rest_tracker
            sid = tracker.session_id
            seq = tracker.next_sequence(_REST_DATA_STREAM)
            ts = dt.datetime.now(dt.UTC)
            pid = str(uuid7())
            return ExchangeListResponse(
                session_id=sid,
                sequence_id=seq,
                public_id=pid,
                timestamp=ts,
                payload=items,
                count=len(items),
            )
        except Exception as exc:
            logger.error(f"Failed to fetch exchanges: {exc}")
            raise HTTPException(status_code=500, detail="Failed to fetch exchanges") from exc

    @router.get(
        "/exchanges/{exchange}/instruments",
        responses={500: {"description": _INTERNAL_SERVER_ERROR_DESCRIPTION}},
    )
    async def get_exchange_instruments(
        request: Request,
        exchange: str,
        _auth: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_MARKET_DATA))],
        _csrf: Annotated[None, Depends(validate_csrf_token)],
        repo: Annotated[Repository, Depends(get_repository_dependency)],
        as_of: Annotated[datetime | None, Query(description="Point-in-time query (UTC)")] = None,
    ) -> InstrumentListResponse:
        """Return distinct native symbols available on a given exchange.

        Args:
            request: FastAPI request (provides REST tracker for provenance).
            exchange: Exchange name to query instruments for.
            _auth: Authenticated user with READ_MARKET_DATA permission.
            _csrf: CSRF token validation.
            repo: Database repository.
            as_of: Optional point-in-time query timestamp.

        Returns:
            InstrumentListResponse wrapping the instrument symbol list.
        """
        try:
            now = as_of or datetime.now(UTC)
            items = await repo.get_exchange_instruments(exchange=exchange, as_of=now)
            tracker: SequenceTracker = request.app.state.rest_tracker
            sid = tracker.session_id
            seq = tracker.next_sequence(_REST_DATA_STREAM)
            ts = dt.datetime.now(dt.UTC)
            pid = str(uuid7())
            return InstrumentListResponse(
                session_id=sid,
                sequence_id=seq,
                public_id=pid,
                timestamp=ts,
                payload=items,
                count=len(items),
            )
        except Exception as exc:
            logger.error(f"Failed to fetch instruments for {exchange}: {exc}")
            raise HTTPException(status_code=500, detail="Failed to fetch instruments") from exc

    @router.get(
        "/exchanges/{exchange}/instruments/detail",
        responses={500: {"description": _INTERNAL_SERVER_ERROR_DESCRIPTION}},
    )
    async def get_exchange_instruments_detail(
        request: Request,
        exchange: str,
        _auth: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_MARKET_DATA))],
        _csrf: Annotated[None, Depends(validate_csrf_token)],
        repo: Annotated[Repository, Depends(get_repository_dependency)],
        as_of: Annotated[datetime | None, Query(description="Point-in-time query (UTC)")] = None,
    ) -> InstrumentDetailListResponse:
        """Return capability-aware instrument rows for a given exchange.

        Each row carries ``can_trade``, ``can_market_data``,
        ``instrument_kind``, and ``expiry_at`` so the frontend can render
        a "Market-data only" badge and disable order-entry for
        TradFi-style instruments without a second round-trip.

        Args:
            request: FastAPI request (provides REST tracker for provenance).
            exchange: Exchange name to query instruments for.
            _auth: Authenticated user with READ_MARKET_DATA permission.
            _csrf: CSRF token validation.
            repo: Database repository.
            as_of: Optional point-in-time query timestamp.

        Returns:
            InstrumentDetailListResponse wrapping the instrument detail list.
        """
        try:
            now = as_of or datetime.now(UTC)
            rows = await repo.get_exchange_instruments_detail(exchange=exchange, as_of=now)
            tracker: SequenceTracker = request.app.state.rest_tracker
            sid = tracker.session_id
            ts = dt.datetime.now(dt.UTC)
            payload = [
                InstrumentDetailData(
                    session_id=sid,
                    sequence_id=tracker.next_sequence(_REST_DATA_STREAM),
                    public_id=str(uuid7()),
                    timestamp=ts,
                    instrument_public_id=row["instrument_public_id"],
                    symbol_public_id=row["symbol_public_id"],
                    symbol=row["symbol"],
                    exchange=row["exchange"],
                    can_trade=row["can_trade"],
                    can_market_data=row["can_market_data"],
                    instrument_resolved=row["instrument_resolved"],
                    instrument_kind=row["instrument_kind"],
                    expiry_at=row["expiry_at"],
                )
                for row in rows
            ]
            return InstrumentDetailListResponse(
                session_id=sid,
                sequence_id=tracker.next_sequence(_REST_DATA_STREAM),
                public_id=str(uuid7()),
                timestamp=ts,
                payload=payload,
                count=len(payload),
            )
        except Exception as exc:
            logger.error(f"Failed to fetch instrument detail for {exchange}: {exc}")
            raise HTTPException(
                status_code=500, detail="Failed to fetch instrument detail"
            ) from exc

    return router


def _create_orders_executions_router() -> APIRouter:
    """Create router for orders, executions, and positions endpoints.

    Returns:
        APIRouter with orders, executions, and positions endpoints.
    """
    router = APIRouter()

    @router.get("/orders", responses={500: {"description": _INTERNAL_SERVER_ERROR_DESCRIPTION}})
    async def get_orders(
        request: Request,
        _auth: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_ORDERS))],
        _csrf: Annotated[None, Depends(validate_csrf_token)],
        repo: Annotated[Repository, Depends(get_repository_dependency)],
        symbol: Annotated[str | None, Query(description="Symbol to filter by")] = None,
        exchange: Annotated[OrderExchange | None, Query(description="Filter by exchange")] = None,
        limit: Annotated[int, Query(ge=1, le=1000, description="Number of orders to return")] = 100,
        offset: Annotated[int, Query(ge=0, description="Number of orders to skip")] = 0,
        as_of: Annotated[datetime | None, Query(description="Point-in-time query (UTC)")] = None,
        operator_public_id: Annotated[str | None, Query(description="Scope to operator")] = None,
        wallet_public_id: Annotated[str | None, Query(description="Scope to wallet")] = None,
    ) -> OrderListResponse:
        """Fetch orders with optional filters.

        Args:
            request: FastAPI request (provides REST tracker for provenance).
            _auth: Authenticated user with READ_ORDERS permission.
            _csrf: CSRF token validation.
            repo: Database repository.
            symbol: Optional symbol filter.
            exchange: Optional exchange filter.
            limit: Maximum number of orders to return.
            offset: Number of orders to skip.
            as_of: Optional point-in-time query timestamp.
            operator_public_id: Optional operator scope (403 if foreign).
            wallet_public_id: Optional wallet scope (403 if inaccessible).

        Returns:
            OrderListResponse wrapping the order data.
        """
        processing_date = as_of or datetime.now(UTC)
        try:
            target_wallets = await resolve_target_wallets(
                _auth, repo, operator_public_id, wallet_public_id
            )
            rows = await repo.get_orders(
                limit=limit,
                offset=offset,
                as_of=processing_date,
                symbol=symbol,
                exchange=exchange,
                wallet_public_ids=target_wallets,
            )
            items = [OrderData(**cast(dict[str, Any], r)) for r in rows]
            tracker: SequenceTracker = request.app.state.rest_tracker
            sid = tracker.session_id
            seq = tracker.next_sequence(_REST_DATA_STREAM)
            ts = dt.datetime.now(dt.UTC)
            pid = str(uuid7())
            return OrderListResponse(
                session_id=sid,
                sequence_id=seq,
                public_id=pid,
                timestamp=ts,
                payload=items,
                count=len(items),
            )
        except HTTPException:
            raise
        except Exception as exc:
            logger.error(f"Failed to fetch orders: {exc}")
            raise HTTPException(status_code=500, detail="Failed to fetch orders") from exc

    @router.get(
        "/executions",
        responses={500: {"description": _INTERNAL_SERVER_ERROR_DESCRIPTION}},
    )
    async def get_executions(
        request: Request,
        _auth: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_ORDERS))],
        _csrf: Annotated[None, Depends(validate_csrf_token)],
        repo: Annotated[Repository, Depends(get_repository_dependency)],
        limit: Annotated[int, Query(le=1000, description="Number of executions to return")] = 100,
        as_of: Annotated[datetime | None, Query(description="Point-in-time query (UTC)")] = None,
        operator_public_id: Annotated[str | None, Query(description="Scope to operator")] = None,
        wallet_public_id: Annotated[str | None, Query(description="Scope to wallet")] = None,
    ) -> ExecutionListResponse:
        """Fetch execution (fill) records.

        Args:
            request: FastAPI request (provides REST tracker for provenance).
            _auth: Authenticated user with READ_ORDERS permission.
            _csrf: CSRF token validation.
            repo: Database repository.
            limit: Maximum number of executions to return.
            as_of: Optional point-in-time query timestamp.
            operator_public_id: Optional operator scope (403 if foreign).
            wallet_public_id: Optional wallet scope (403 if inaccessible).

        Returns:
            ExecutionListResponse wrapping the execution data.
        """
        processing_date = as_of or datetime.now(UTC)
        try:
            target_wallets = await resolve_target_wallets(
                _auth, repo, operator_public_id, wallet_public_id
            )
            rows = await repo.get_executions(
                limit=limit, as_of=processing_date, wallet_public_ids=target_wallets
            )
            items = [
                ExecutionData(
                    **{
                        "last_size": r["size"],
                        "last_price": r["price"],
                        **cast(dict[str, Any], r),
                    }
                )
                for r in rows
            ]
            tracker: SequenceTracker = request.app.state.rest_tracker
            sid = tracker.session_id
            seq = tracker.next_sequence(_REST_DATA_STREAM)
            ts = dt.datetime.now(dt.UTC)
            pid = str(uuid7())
            return ExecutionListResponse(
                session_id=sid,
                sequence_id=seq,
                public_id=pid,
                timestamp=ts,
                payload=items,
                count=len(items),
            )
        except HTTPException:
            raise
        except Exception as exc:
            logger.error(f"Failed to fetch executions: {exc}")
            raise HTTPException(status_code=500, detail="Failed to fetch executions") from exc

    @router.get(
        "/positions",
        responses={500: {"description": _INTERNAL_SERVER_ERROR_DESCRIPTION}},
    )
    async def get_positions(
        request: Request,
        _auth: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_POSITIONS))],
        _csrf: Annotated[None, Depends(validate_csrf_token)],
        repo: Annotated[Repository, Depends(get_repository_dependency)],
        as_of: Annotated[datetime | None, Query(description="Point-in-time query (UTC)")] = None,
        operator_public_id: Annotated[str | None, Query(description="Scope to operator")] = None,
        wallet_public_id: Annotated[str | None, Query(description="Scope to wallet")] = None,
    ) -> PositionListResponse:
        """Fetch current portfolio positions.

        Args:
            request: FastAPI request (provides REST tracker for provenance).
            _auth: Authenticated user with READ_POSITIONS permission.
            _csrf: CSRF token validation.
            repo: Database repository.
            as_of: Optional point-in-time query timestamp.
            operator_public_id: Optional operator scope (403 if foreign).
            wallet_public_id: Optional wallet scope (403 if inaccessible).

        Returns:
            PositionListResponse wrapping the position data.
        """
        processing_date = as_of or datetime.now(UTC)
        try:
            target_wallets = await resolve_target_wallets(
                _auth, repo, operator_public_id, wallet_public_id
            )
            rows = await repo.get_positions(as_of=processing_date, wallet_public_ids=target_wallets)
            items = [PositionData(**cast(dict[str, Any], r)) for r in rows]
            tracker: SequenceTracker = request.app.state.rest_tracker
            sid = tracker.session_id
            seq = tracker.next_sequence(_REST_DATA_STREAM)
            ts = dt.datetime.now(dt.UTC)
            pid = str(uuid7())
            return PositionListResponse(
                session_id=sid,
                sequence_id=seq,
                public_id=pid,
                timestamp=ts,
                payload=items,
                count=len(items),
            )
        except HTTPException:
            raise
        except Exception as exc:
            logger.error(f"Failed to fetch positions: {exc}")
            raise HTTPException(status_code=500, detail="Failed to fetch positions") from exc

    return router


def _gap_detector_stats_to_schema(
    stats: GapDetectorStats,
) -> GapStats:
    """Convert a GapDetectorStats dataclass to a GapStats model.

    Args:
        stats: Gap detector statistics dataclass.

    Returns:
        Pydantic model with the same counter values.
    """
    return GapStats(
        gaps_detected=stats.gaps_detected,
        session_resets=stats.session_resets,
        duplicates=stats.duplicates,
        mid_stream_joins=stats.mid_stream_joins,
        rejected_unstamped=stats.rejected_unstamped,
    )


def _collect_gap_detection_stats(
    manager: WebSocketConnectionManager,
    middleware_gap_detectors: dict[str, Any] | None,
) -> GapDetectionStats:
    """Aggregate gap detection stats from bridge and REST middleware.

    Args:
        manager: WebSocket connection manager with ZMQ bridge.
        middleware_gap_detectors: Per-session gap detectors from the
            provenance middleware (may be None).

    Returns:
        Aggregated gap detection statistics.
    """
    bridge_stats = _gap_detector_stats_to_schema(
        manager.zmq_bridge._gap_detector.stats,
    )
    rest_clients: dict[str, GapStats] = {}
    if middleware_gap_detectors:
        for det_session_id, detector in middleware_gap_detectors.items():
            rest_clients[det_session_id] = _gap_detector_stats_to_schema(
                detector.stats,
            )
    return GapDetectionStats(
        bridge=bridge_stats,
        rest_clients=rest_clients,
    )


def _create_monitoring_endpoints_router(
    manager: WebSocketConnectionManager,
) -> APIRouter:
    """Create router for monitoring endpoints (health, ws/stats, zmq/health, status).

    Args:
        manager: WebSocket connection manager.

    Returns:
        APIRouter with monitoring endpoints.
    """
    router = APIRouter()
    zmq_bridge = manager.zmq_bridge

    @router.get("/health")
    async def health_check(request: Request) -> HealthCheckResponse:
        tracker: SequenceTracker = request.app.state.rest_tracker
        sid = tracker.session_id
        seq = tracker.next_sequence(_REST_HEALTH_STREAM)
        ts = dt.datetime.now(dt.UTC)
        pid = str(uuid7())
        stats = manager.get_stats()
        middleware_detectors = getattr(request.app.state, "provenance_gap_detectors", None)
        gap_stats = _collect_gap_detection_stats(
            manager,
            middleware_detectors,
        )
        factory: ProcessLauncherService = request.app.state.process_factory
        core_status: HealthStatus = await factory.get_core_health()
        return HealthCheckResponse(
            session_id=sid,
            sequence_id=seq,
            public_id=pid,
            timestamp=ts,
            payload=HealthCheckData(
                session_id=sid,
                sequence_id=seq,
                public_id=str(uuid7()),
                timestamp=ts,
                status=core_status,
                version="0.1.0",
                connections=ConnectionStats(
                    **asdict(stats.connections),
                ),
                topics=HealthTopics(
                    active=stats.connections.active_topics,
                ),
                gap_detection=gap_stats,
            ),
        )

    @router.get("/ws/stats")
    async def websocket_stats(
        request: Request,
        _auth: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_SYSTEM_STATUS))],
        _csrf: Annotated[None, Depends(validate_csrf_token)],
    ) -> WsStatsResponse:
        tracker: SequenceTracker = request.app.state.rest_tracker
        sid = tracker.session_id
        seq = tracker.next_sequence(_REST_HEALTH_STREAM)
        ts = dt.datetime.now(dt.UTC)
        pid = str(uuid7())
        stats = manager.get_stats()
        return WsStatsResponse(
            session_id=sid,
            sequence_id=seq,
            public_id=pid,
            timestamp=ts,
            payload=WsStatsData(
                session_id=sid,
                sequence_id=seq,
                public_id=str(uuid7()),
                timestamp=ts,
                websocket=WebSocketStats(
                    active_connections=len(manager.active_connections),
                    topic_subscribers={
                        topic: len(subs) for topic, subs in manager.topic_subscribers.items()
                    },
                    client_count=len(manager.active_connections),
                ),
                zmq_bridge=ZmqBridgeStats(
                    active_topics=len(zmq_bridge.topic_subscriptions),
                    subscriber_tasks=len(zmq_bridge.subscriber_tasks),
                    available_topics=list(zmq_bridge.available_topics),
                ),
                connections=ConnectionStats(
                    **asdict(stats.connections),
                ),
                topics={
                    k: TopicMetricSnapshot(
                        **asdict(v),
                    )
                    for k, v in stats.topics.items()
                },
                subscriptions=SubscriptionsStats(
                    per_topic={
                        topic: len(subs) for topic, subs in manager.topic_subscribers.items()
                    },
                    per_client={
                        str(id(ws)): list(subs) for ws, subs in manager.client_subscriptions.items()
                    },
                ),
                config=WsStatsConfig(
                    broker_xpub=zmq_bridge.settings.zmq_broker_xpub,
                    heartbeat_interval_ms=_resolve_zmq_heartbeat_interval_ms(request),
                ),
            ),
        )

    @router.get("/zmq/health")
    async def zmq_health_check(
        request: Request,
        _auth: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_SYSTEM_STATUS))],
        _csrf: Annotated[None, Depends(validate_csrf_token)],
    ) -> ZmqHealthResponse:
        error_messages: list[str] = []
        available_topics: list[str] = []
        try:
            if zmq_bridge.context is not None:
                test_socket = zmq_bridge.context.socket(zmq.SUB)
                test_socket.close()
            available_topics = zmq_bridge.get_available_topics()
        except Exception as exc:
            error_messages.append(f"ZMQ context error: {exc}")
            available_topics = zmq_bridge.get_available_topics()
        stats = manager.get_stats()
        status: HealthStatus = (
            HealthStatusEnum.HEALTHY if not error_messages else HealthStatusEnum.ERROR
        )
        tracker: SequenceTracker = request.app.state.rest_tracker
        sid = tracker.session_id
        seq = tracker.next_sequence(_REST_HEALTH_STREAM)
        ts = dt.datetime.now(dt.UTC)
        pid = str(uuid7())
        return ZmqHealthResponse(
            session_id=sid,
            sequence_id=seq,
            public_id=pid,
            timestamp=ts,
            payload=ZmqHealthData(
                session_id=sid,
                sequence_id=seq,
                public_id=str(uuid7()),
                timestamp=ts,
                status=status,
                components=ZmqComponents(
                    zmq_context=(
                        ComponentStatusEnum.OK if not error_messages else ComponentStatusEnum.ERROR
                    ),
                    websocket_manager=ComponentStatusEnum.OK,
                    active_connections=stats.connections.active_connections,
                ),
                config=ZmqConfig(
                    available_topics=available_topics,
                ),
                connections=ConnectionStats(
                    **asdict(stats.connections),
                ),
                message_stats={
                    k: TopicMetricSnapshot(
                        **asdict(v),
                    )
                    for k, v in stats.topics.items()
                },
                errors=error_messages,
            ),
        )

    @router.get("/metrics/rest-rate")
    async def rest_rate_metrics(
        request: Request,
        _auth: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_SYSTEM_STATUS))],
        _csrf: Annotated[None, Depends(validate_csrf_token)],
    ) -> RestRateResponse:
        """Return rolling REST call rates + utilization per exchange.

        Reads the process-scoped ``RestCallTracker`` snapshot and
        projects it into the typed envelope used by the other
        monitoring endpoints. Gated behind ``READ_SYSTEM_STATUS`` so a
        viewer role can see rate-limit health without having any
        trading permission.
        """
        tracker: SequenceTracker = request.app.state.rest_tracker
        sid = tracker.session_id
        seq = tracker.next_sequence(_REST_HEALTH_STREAM)
        ts = dt.datetime.now(dt.UTC)
        pid = str(uuid7())
        snapshot = get_rest_call_tracker().snapshot()
        exchanges: dict[str, RestRateExchangeStats] = {}
        for exchange, row in snapshot.items():
            exchanges[exchange] = RestRateExchangeStats(
                rps_1s=float(row.get("rps_1s") or 0.0),
                rps_10s=float(row.get("rps_10s") or 0.0),
                rps_60s=float(row.get("rps_60s") or 0.0),
                limit_rps=row.get("limit_rps"),
                utilization=row.get("utilization"),
            )
        return RestRateResponse(
            session_id=sid,
            sequence_id=seq,
            public_id=pid,
            timestamp=ts,
            payload=RestRateData(
                session_id=sid,
                sequence_id=seq,
                public_id=str(uuid7()),
                timestamp=ts,
                exchanges=exchanges,
            ),
        )

    @router.get("/status")
    async def get_system_status(
        request: Request,
        _auth: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_SYSTEM_STATUS))],
        _csrf: Annotated[None, Depends(validate_csrf_token)],
    ) -> SystemStatusResponse:
        process_factory: ProcessLauncherService = request.app.state.process_factory
        tracker: SequenceTracker = request.app.state.rest_tracker
        sid = tracker.session_id
        seq = tracker.next_sequence(_REST_HEALTH_STREAM)
        ts = dt.datetime.now(dt.UTC)
        pid = str(uuid7())
        trader_status = _resolve_trader_status(process_factory)
        data = SystemStatusData(
            session_id=sid,
            sequence_id=seq,
            public_id=str(uuid7()),
            timestamp=ts,
            trader=trader_status,
            backtests={},
            strategies=_collect_strategy_statuses(process_factory),
        )
        return SystemStatusResponse(
            session_id=sid,
            sequence_id=seq,
            public_id=pid,
            timestamp=ts,
            payload=data,
        )

    return router


def _resolve_underlying_query_time(as_of: datetime | None) -> datetime:
    """Return the requested point-in-time or the current UTC instant."""
    return as_of or datetime.now(UTC)


def _normalize_utc(timestamp: datetime) -> datetime:
    """Normalize a required datetime to UTC, assuming naive values are UTC."""
    if timestamp.tzinfo:
        return timestamp.astimezone(UTC)
    return timestamp.replace(tzinfo=UTC)


def _normalize_optional_utc(as_of: datetime | None) -> datetime:
    """Normalize an optional datetime to UTC, defaulting missing values to now."""
    if as_of is None:
        return datetime.now(UTC)
    return _normalize_utc(as_of)


def _get_rest_data_response_metadata(
    request: Request,
) -> tuple[SequenceTracker, str, int, datetime, str]:
    """Return tracker state for one REST data response envelope."""
    tracker = cast(SequenceTracker, request.app.state.rest_tracker)
    return (
        tracker,
        tracker.session_id,
        tracker.next_sequence(_REST_DATA_STREAM),
        dt.datetime.now(dt.UTC),
        str(uuid7()),
    )


def _build_underlying_asset_items(assets: list[UnderlyingAssetRow]) -> list[UnderlyingAssetData]:
    """Project underlying rows into API payload items."""
    return [
        UnderlyingAssetData(
            public_id=asset["public_id"],
            session_id=asset["session_id"],
            sequence_id=asset["sequence_id"],
            timestamp=asset["timestamp"],
            ticker=asset["ticker"],
            name=asset["name"],
            asset_class=asset["asset_class"],
            sector=asset["sector"],
            instrument_count=asset["instrument_count"],
        )
        for asset in assets
    ]


def _build_underlying_instrument_items(
    rows: list[InstrumentUnderlyingRow],
) -> list[UnderlyingInstrumentData]:
    """Project underlying-instrument mapping rows into API payload items."""
    return [
        UnderlyingInstrumentData(
            public_id=row["public_id"],
            session_id=row["session_id"],
            sequence_id=row["sequence_id"],
            timestamp=row["timestamp"],
            instrument_public_id=row["instrument_public_id"],
            native_symbol=row["native_symbol"],
            exchange=row["exchange"],
            asset_type=row["asset_type"],
            relationship_type=row["relationship_type"],
            contract_family=row["contract_family"],
        )
        for row in rows
    ]


def _build_contract_items(
    rows: list[InstrumentContractRow],
    tracker: SequenceTracker,
    session_id: str,
) -> list[ContractData]:
    """Project contract rows into API payload items with fresh provenance."""
    return [
        ContractData(
            public_id=str(uuid7()),
            session_id=session_id,
            sequence_id=tracker.next_sequence(_REST_DATA_STREAM),
            timestamp=dt.datetime.now(dt.UTC),
            instrument_public_id=row["instrument_public_id"],
            native_symbol=row["native_symbol"],
            exchange=row["exchange"],
            expiry_at=row["expiry_at"],
            instrument_kind=row["instrument_kind"],
            relationship_type=row["relationship_type"],
            contract_family=row["contract_family"],
            is_front_month=row["is_front_month"],
        )
        for row in rows
    ]


def _build_continuous_candle_items(
    candles: list[ContinuousCandleRow],
    tracker: SequenceTracker,
    session_id: str,
) -> list[ContinuousCandleData]:
    """Project continuous-candle rows into API payload items."""
    return [
        ContinuousCandleData(
            public_id=str(uuid7()),
            session_id=session_id,
            sequence_id=tracker.next_sequence(_REST_DATA_STREAM),
            timestamp=dt.datetime.now(dt.UTC),
            open_at=candle["open_at"],
            timeframe=candle["timeframe"],
            open=candle["open"],
            high=candle["high"],
            low=candle["low"],
            close=candle["close"],
            volume=candle["volume"],
            vwap=candle["vwap"],
            trades=candle["trades"],
            source_contract=candle["source_contract"],
            adjustment_factor=candle["adjustment_factor"],
        )
        for candle in candles
    ]


async def _get_underlyings(
    request: Request,
    _auth: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_MARKET_DATA))],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    as_of: Annotated[datetime | None, Query(description="Point-in-time query (UTC)")] = None,
) -> UnderlyingAssetListResponse:
    """Return all underlying assets with instrument counts."""
    try:
        assets = await repo.get_underlying_assets(as_of=_resolve_underlying_query_time(as_of))
        _tracker, session_id, sequence_id, timestamp, public_id = _get_rest_data_response_metadata(
            request
        )
        items = _build_underlying_asset_items(assets)
        return UnderlyingAssetListResponse(
            session_id=session_id,
            sequence_id=sequence_id,
            public_id=public_id,
            timestamp=timestamp,
            payload=items,
            count=len(items),
        )
    except Exception as exc:
        logger.error(f"Failed to fetch underlyings: {exc}")
        raise HTTPException(status_code=500, detail="Failed to fetch underlyings") from exc


async def _get_underlying_instruments(
    request: Request,
    ticker: str,
    _auth: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_MARKET_DATA))],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    as_of: Annotated[datetime | None, Query(description="Point-in-time query (UTC)")] = None,
    relationship_type: Annotated[
        RelationshipTypeEnum | None, Query(description="Filter by relationship type")
    ] = None,
) -> UnderlyingInstrumentListResponse:
    """Return instruments mapped to an underlying asset."""
    try:
        now = _resolve_underlying_query_time(as_of)
        underlying = await repo.get_underlying_by_ticker(ticker, now)
        if underlying is None:
            raise HTTPException(
                status_code=404,
                detail=f"Underlying not found: {ticker}",
            )
        rows = await repo.get_instruments_by_underlying(
            underlying["public_id"],
            now,
            relationship_types=[relationship_type.value] if relationship_type else None,
        )
        _tracker, session_id, sequence_id, timestamp, public_id = _get_rest_data_response_metadata(
            request
        )
        items = _build_underlying_instrument_items(rows)
        return UnderlyingInstrumentListResponse(
            session_id=session_id,
            sequence_id=sequence_id,
            public_id=public_id,
            timestamp=timestamp,
            payload=items,
            count=len(items),
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Failed to fetch instruments for {ticker}: {exc}")
        raise HTTPException(
            status_code=500, detail="Failed to fetch underlying instruments"
        ) from exc


_RELATED_GROUP_ORDER: tuple[RelationshipTypeEnum, ...] = (
    RelationshipTypeEnum.EXACT,
    RelationshipTypeEnum.DERIVATIVE,
    RelationshipTypeEnum.PROXY,
)


_RELATED_GROUP_LABELS: dict[RelationshipTypeEnum, str] = {
    RelationshipTypeEnum.EXACT: "Same underlying",
    RelationshipTypeEnum.DERIVATIVE: "Derivatives",
    RelationshipTypeEnum.PROXY: "Proxies",
}


def _build_related_instrument_items(
    rows: list[InstrumentRelatedRow],
    tracker: SequenceTracker,
    session_id: str,
) -> list[RelatedInstrumentData]:
    """Project related-row dicts into API payload items with fresh provenance."""
    return [
        RelatedInstrumentData(
            public_id=str(uuid7()),
            session_id=session_id,
            sequence_id=tracker.next_sequence(_REST_DATA_STREAM),
            timestamp=dt.datetime.now(dt.UTC),
            instrument_public_id=row["instrument_public_id"],
            native_symbol=row["native_symbol"],
            exchange=row["exchange"],
            asset_type=row["asset_type"],
            relationship_type=row["relationship_type"],
            contract_family=row["contract_family"],
            is_selected=row["is_selected"],
        )
        for row in rows
    ]


def _group_related_items(
    items: list[RelatedInstrumentData],
    *,
    selected_exchange: str,
) -> list[RelatedInstrumentsGroup]:
    """Partition items by ``relationship_type`` and sort per group rules.

    Ordering rules per the related-row design:

    - Groups appear in fixed order EXACT -> DERIVATIVE -> PROXY; empty
      groups are omitted so an underlying with only derivatives renders
      one group, not three.
    - Within DERIVATIVE: ``contract_family`` first (perpetuals + dated
      futures grouped per product root), then ``native_symbol``.
    - Within EXACT and PROXY: selected chip first, then siblings on the
      same exchange as the selection, then alphabetic by
      ``(exchange, native_symbol)`` for deterministic tests.
    """
    by_rel: dict[str, list[RelatedInstrumentData]] = {}
    for item in items:
        by_rel.setdefault(item.relationship_type, []).append(item)
    groups: list[RelatedInstrumentsGroup] = []
    for rel in _RELATED_GROUP_ORDER:
        bucket = by_rel.get(rel.value, [])
        if not bucket:
            continue
        if rel is RelationshipTypeEnum.DERIVATIVE:
            bucket.sort(key=lambda r: (r.contract_family or "", r.native_symbol))
        else:
            bucket.sort(
                key=lambda r: (
                    not r.is_selected,
                    r.exchange != selected_exchange,
                    r.exchange,
                    r.native_symbol,
                )
            )
        groups.append(
            RelatedInstrumentsGroup(
                relationship_type=rel.value,
                label=_RELATED_GROUP_LABELS[rel],
                items=bucket,
            )
        )
    return groups


async def _get_related_instruments(
    request: Request,
    exchange: str,
    native_symbol: str,
    _auth: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_MARKET_DATA))],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    as_of: Annotated[datetime | None, Query(description="Point-in-time query (UTC)")] = None,
) -> RelatedInstrumentsResponse:
    """Return the related-instruments row payload for a UI-selected symbol.

    Mapped symbols return ``underlying`` populated + grouped ``items``;
    orphan symbols (mapping gap or unknown symbol) return ``underlying =
    None`` + ``groups = []`` so the frontend can render a single placeholder
    line ("No related instruments configured") that surfaces YAML coverage
    gaps to operators instead of hiding them.
    """
    try:
        now = _resolve_underlying_query_time(as_of)
        underlying_row, related_rows = await repo.get_related_instruments_for_symbol(
            exchange, native_symbol, now
        )
        tracker, session_id, sequence_id, timestamp, public_id = _get_rest_data_response_metadata(
            request
        )
        items = _build_related_instrument_items(related_rows, tracker, session_id)
        groups = _group_related_items(items, selected_exchange=exchange)
        underlying = (
            None
            if underlying_row is None
            else RelatedInstrumentsUnderlying(
                public_id=underlying_row["public_id"],
                ticker=underlying_row["ticker"],
                name=underlying_row["name"],
                asset_class=underlying_row["asset_class"],
                sector=underlying_row["sector"],
            )
        )
        payload = RelatedInstrumentsPayloadData(
            selected=RelatedInstrumentsSelected(
                exchange=exchange,
                native_symbol=native_symbol,
            ),
            underlying=underlying,
            groups=groups,
        )
        return RelatedInstrumentsResponse(
            public_id=public_id,
            session_id=session_id,
            sequence_id=sequence_id,
            timestamp=timestamp,
            payload=payload,
        )
    except Exception as exc:
        logger.error(f"Failed to fetch related instruments for {exchange}/{native_symbol}: {exc}")
        raise HTTPException(status_code=500, detail="Failed to fetch related instruments") from exc


async def _get_front_month(
    request: Request,
    ticker: str,
    _auth: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_MARKET_DATA))],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    as_of: Annotated[datetime | None, Query(description="Point-in-time query (UTC)")] = None,
    exchange: Annotated[str | None, Query(description="Filter by exchange")] = None,
    contract_family: Annotated[
        str | None, Query(description="Filter by product root (e.g. ES, MES)")
    ] = None,
) -> FrontMonthResponse:
    """Return the front-month (nearest non-expired) futures contract."""
    try:
        now = _resolve_underlying_query_time(as_of)
        underlying = await repo.get_underlying_by_ticker(ticker, now)
        if underlying is None:
            raise HTTPException(status_code=404, detail=f"Underlying not found: {ticker}")
        row = await repo.get_front_month_instrument(
            underlying["public_id"],
            now,
            exchange=exchange,
            contract_family=contract_family,
        )
        if row is None:
            raise HTTPException(
                status_code=404,
                detail=f"No active futures contracts for {ticker}",
            )
        _tracker, session_id, sequence_id, timestamp, public_id = _get_rest_data_response_metadata(
            request
        )
        item = FrontMonthData(
            public_id=public_id,
            session_id=session_id,
            sequence_id=sequence_id,
            timestamp=timestamp,
            instrument_public_id=row["instrument_public_id"],
            native_symbol=row["native_symbol"],
            exchange=row["exchange"],
            expiry_at=row["expiry_at"],
            relationship_type=row["relationship_type"],
            contract_family=row["contract_family"],
        )
        return FrontMonthResponse(
            session_id=session_id,
            sequence_id=sequence_id,
            public_id=public_id,
            timestamp=timestamp,
            payload=item,
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Failed to fetch front-month for {ticker}: {exc}")
        raise HTTPException(
            status_code=500, detail="Failed to fetch front-month instrument"
        ) from exc


async def _get_contracts(
    request: Request,
    ticker: str,
    _auth: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_MARKET_DATA))],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    as_of: Annotated[datetime | None, Query(description="Point-in-time query (UTC)")] = None,
    exchange: Annotated[str | None, Query(description="Filter by exchange")] = None,
    contract_family: Annotated[
        str | None, Query(description="Filter by product root (e.g. ES, MES)")
    ] = None,
    include_expired: Annotated[bool, Query(description="Include expired contracts")] = False,
) -> ContractListResponse:
    """Return all futures contracts for an underlying asset."""
    try:
        now = _resolve_underlying_query_time(as_of)
        underlying = await repo.get_underlying_by_ticker(ticker, now)
        if underlying is None:
            raise HTTPException(status_code=404, detail=f"Underlying not found: {ticker}")
        rows = await repo.get_contracts_for_underlying(
            underlying["public_id"],
            now,
            exchange=exchange,
            contract_family=contract_family,
            include_expired=include_expired,
        )
        tracker, session_id, sequence_id, timestamp, public_id = _get_rest_data_response_metadata(
            request
        )
        items = _build_contract_items(rows, tracker, session_id)
        return ContractListResponse(
            session_id=session_id,
            sequence_id=sequence_id,
            public_id=public_id,
            timestamp=timestamp,
            payload=items,
            count=len(items),
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Failed to fetch contracts for {ticker}: {exc}")
        raise HTTPException(status_code=500, detail="Failed to fetch contracts") from exc


async def _get_continuous_series(
    request: Request,
    ticker: str,
    _auth: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_MARKET_DATA))],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    exchange: Annotated[str, Query(description="Exchange to source contracts from")],
    contract_family: Annotated[str, Query(description="Product root (e.g. ES, GC)")],
    timeframe: Annotated[str, Query(description="Candle timeframe (e.g. 1h, 1d)")],
    start: Annotated[datetime, Query(description="Series start time (UTC)")],
    end: Annotated[datetime, Query(description="Series end time (UTC)")],
    method: Annotated[str, Query(description="Adjustment method")] = "panama",
    rollover_days_before: Annotated[
        int,
        Query(ge=0, le=365, description="Days before expiry to roll (0-365)"),
    ] = 0,
    as_of: Annotated[datetime | None, Query(description="Point-in-time query (UTC)")] = None,
) -> ContinuousCandleListResponse | ContinuousSeriesPartialResponse:
    """Build and return a continuous contract candle series."""
    try:
        if method not in ("unadjusted", "ratio", "panama"):
            raise HTTPException(status_code=400, detail=f"Invalid method: {method}")
        max_days = 3650
        if (end - start).days > max_days:
            raise HTTPException(
                status_code=400,
                detail=f"Date range too large: max {max_days} days",
            )
        now = _normalize_optional_utc(as_of)
        underlying = await repo.get_underlying_by_ticker(ticker, now)
        if underlying is None:
            raise HTTPException(status_code=404, detail=f"Underlying not found: {ticker}")
        result = await ContinuousContractBuilder(repository=repo).build(
            underlying_public_id=underlying["public_id"],
            exchange=exchange,
            contract_family=contract_family,
            timeframe=timeframe,
            start=_normalize_utc(start),
            end=_normalize_utc(end),
            method=method,
            rollover_days_before=rollover_days_before,
            as_of=now,
        )
        tracker, session_id, _sequence_id, _timestamp, _public_id = (
            _get_rest_data_response_metadata(request)
        )
        items = _build_continuous_candle_items(result.candles, tracker, session_id)
        _tracker, sequence_id, timestamp, public_id = None, None, None, None
        if result.failed_roll is not None:
            _tracker, session_id, sequence_id, timestamp, public_id = (
                _get_rest_data_response_metadata(request)
            )
            return ContinuousSeriesPartialResponse(
                session_id=session_id,
                sequence_id=sequence_id,
                public_id=public_id,
                timestamp=timestamp,
                payload=items,
                count=len(items),
                failed_roll=RollPointDetail(
                    from_contract=result.failed_roll.from_contract,
                    to_contract=result.failed_roll.to_contract,
                    roll_at=result.failed_roll.roll_at.isoformat(),
                ),
                message=(
                    f"Series truncated at roll {result.failed_roll.from_contract} "
                    f"-> {result.failed_roll.to_contract}: gap too large"
                ),
            )
        _tracker, session_id, sequence_id, timestamp, public_id = _get_rest_data_response_metadata(
            request
        )
        return ContinuousCandleListResponse(
            session_id=session_id,
            sequence_id=sequence_id,
            public_id=public_id,
            timestamp=timestamp,
            payload=items,
            count=len(items),
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.error(f"Failed to build continuous series for {ticker}: {exc}")
        raise HTTPException(status_code=500, detail="Failed to build continuous series") from exc


def _create_underlying_router() -> APIRouter:
    """Create router for underlying asset discovery endpoints."""
    router = APIRouter()
    router.add_api_route(
        "/underlyings",
        _get_underlyings,
        methods=["GET"],
        responses={500: {"description": _INTERNAL_SERVER_ERROR_DESCRIPTION}},
    )
    router.add_api_route(
        "/underlyings/{ticker}/instruments",
        _get_underlying_instruments,
        methods=["GET"],
        responses={
            404: {"description": _UNDERLYING_NOT_FOUND_DESCRIPTION},
            500: {"description": _INTERNAL_SERVER_ERROR_DESCRIPTION},
        },
    )
    router.add_api_route(
        "/underlyings/{ticker}/front-month",
        _get_front_month,
        methods=["GET"],
        responses={
            404: {"description": "No active futures contracts or underlying not found"},
            500: {"description": _INTERNAL_SERVER_ERROR_DESCRIPTION},
        },
    )
    router.add_api_route(
        "/underlyings/{ticker}/contracts",
        _get_contracts,
        methods=["GET"],
        responses={
            404: {"description": _UNDERLYING_NOT_FOUND_DESCRIPTION},
            500: {"description": _INTERNAL_SERVER_ERROR_DESCRIPTION},
        },
    )
    router.add_api_route(
        "/underlyings/{ticker}/continuous",
        _get_continuous_series,
        methods=["GET"],
        responses={
            400: {"description": "Invalid parameters"},
            404: {"description": _UNDERLYING_NOT_FOUND_DESCRIPTION},
            500: {"description": _INTERNAL_SERVER_ERROR_DESCRIPTION},
        },
    )
    router.add_api_route(
        "/instruments/{exchange}/{native_symbol}/related",
        _get_related_instruments,
        methods=["GET"],
        responses={500: {"description": _INTERNAL_SERVER_ERROR_DESCRIPTION}},
    )
    return router


def _create_capabilities_router() -> APIRouter:
    """Create router for instrument capabilities and venue fee schedule endpoints.

    Returns:
        APIRouter with capability matrix and fee schedule query endpoints.
    """
    router = APIRouter()

    @router.get(
        "/instrument-capabilities",
        response_model=None,
        responses={500: {"description": _INTERNAL_SERVER_ERROR_DESCRIPTION}},
    )
    async def get_instrument_capabilities(
        request: Request,
        _auth: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_MARKET_DATA))],
        _csrf: Annotated[None, Depends(validate_csrf_token)],
        repo: Annotated[Repository, Depends(get_repository_dependency)],
        exchange: Annotated[str | None, Query(description="Filter by exchange name")] = None,
        instrument_public_id: Annotated[
            str | None, Query(description="Filter by instrument public ID")
        ] = None,
        as_of: Annotated[datetime | None, Query(description="Point-in-time query (UTC)")] = None,
    ) -> InstrumentCapabilityListResponse:
        """Fetch instrument order capability matrix.

        Args:
            request: FastAPI request (provides REST tracker for provenance).
            _auth: Authenticated user with READ_MARKET_DATA permission.
            _csrf: CSRF token validation.
            repo: Database repository.
            exchange: Optional exchange name filter.
            instrument_public_id: Optional instrument UUID filter.
            as_of: Optional point-in-time query timestamp.

        Returns:
            InstrumentCapabilityListResponse wrapping capability rows.
        """
        processing_date = as_of or datetime.now(UTC)
        try:
            rows = await repo.get_instrument_capabilities(
                as_of=processing_date,
                exchange=exchange,
                instrument_public_id=instrument_public_id,
            )
            items = [InstrumentCapabilityData(**cast(dict[str, Any], r)) for r in rows]
            tracker: SequenceTracker = request.app.state.rest_tracker
            sid = tracker.session_id
            seq = tracker.next_sequence(_REST_DATA_STREAM)
            ts = dt.datetime.now(dt.UTC)
            pid = str(uuid7())
            return InstrumentCapabilityListResponse(
                session_id=sid,
                sequence_id=seq,
                public_id=pid,
                timestamp=ts,
                payload=items,
                count=len(items),
            )
        except HTTPException:
            raise
        except Exception as exc:
            logger.error(f"Failed to fetch instrument capabilities: {exc}")
            raise HTTPException(
                status_code=500, detail="Failed to fetch instrument capabilities"
            ) from exc

    @router.get(
        "/venue-fee-schedules",
        response_model=None,
        responses={500: {"description": _INTERNAL_SERVER_ERROR_DESCRIPTION}},
    )
    async def get_venue_fee_schedules(
        request: Request,
        _auth: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_MARKET_DATA))],
        _csrf: Annotated[None, Depends(validate_csrf_token)],
        repo: Annotated[Repository, Depends(get_repository_dependency)],
        exchange: Annotated[str | None, Query(description="Filter by exchange name")] = None,
        as_of: Annotated[datetime | None, Query(description="Point-in-time query (UTC)")] = None,
    ) -> VenueFeeScheduleListResponse:
        """Fetch venue fee schedules.

        Args:
            request: FastAPI request (provides REST tracker for provenance).
            _auth: Authenticated user with READ_MARKET_DATA permission.
            _csrf: CSRF token validation.
            repo: Database repository.
            exchange: Optional exchange name filter.
            as_of: Optional point-in-time query timestamp.

        Returns:
            VenueFeeScheduleListResponse wrapping fee schedule rows.
        """
        processing_date = as_of or datetime.now(UTC)
        try:
            rows = await repo.get_venue_fee_schedules(
                as_of=processing_date,
                exchange=exchange,
            )
            items = [VenueFeeScheduleData(**cast(dict[str, Any], r)) for r in rows]
            tracker: SequenceTracker = request.app.state.rest_tracker
            sid = tracker.session_id
            seq = tracker.next_sequence(_REST_DATA_STREAM)
            ts = dt.datetime.now(dt.UTC)
            pid = str(uuid7())
            return VenueFeeScheduleListResponse(
                session_id=sid,
                sequence_id=seq,
                public_id=pid,
                timestamp=ts,
                payload=items,
                count=len(items),
            )
        except HTTPException:
            raise
        except Exception as exc:
            logger.error(f"Failed to fetch venue fee schedules: {exc}")
            raise HTTPException(
                status_code=500, detail="Failed to fetch venue fee schedules"
            ) from exc

    return router


def create_api_router(
    manager: WebSocketConnectionManager,
) -> APIRouter:
    """Create the main API router with all REST endpoints.

    Composes data-query and monitoring sub-routers into a single router.

    Args:
        manager: WebSocket connection manager for accessing ZMQ bridge.

    Returns:
        APIRouter with health, candles, orders, signals, executions,
        positions, WebSocket stats, and ZMQ health endpoints.
    """
    router = APIRouter()
    router.include_router(_create_candles_signals_router())
    router.include_router(_create_exchange_router())
    router.include_router(_create_underlying_router())
    router.include_router(_create_orders_executions_router())
    router.include_router(_create_capabilities_router())
    router.include_router(_create_monitoring_endpoints_router(manager))
    return router


__all__ = ["create_app"]
app = create_app()
