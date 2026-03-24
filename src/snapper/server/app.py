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
from uuid import uuid7

import zmq
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
from sqlalchemy import Select
from sqlalchemy import and_
from sqlalchemy import desc
from sqlalchemy import distinct
from sqlalchemy import select

from snapper.api.auth.services.ws_token_service import get_ws_token_service
from snapper.api.schemas.data_responses import CandleListResponse
from snapper.api.schemas.data_responses import ExchangeListResponse
from snapper.api.schemas.data_responses import ExecutionListResponse
from snapper.api.schemas.data_responses import InstrumentListResponse
from snapper.api.schemas.data_responses import OrderListResponse
from snapper.api.schemas.data_responses import PositionListResponse
from snapper.api.schemas.data_responses import SignalListResponse
from snapper.api.schemas.health import ConnectionStats
from snapper.api.schemas.health import GapDetectionStats
from snapper.api.schemas.health import GapStats
from snapper.api.schemas.health import HealthCheckData
from snapper.api.schemas.health import HealthCheckResponse
from snapper.api.schemas.health import HealthTopics
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
from snapper.application.process_manager.launcher import ProcessLauncherService
from snapper.application.process_manager.registry import discover_processes
from snapper.application.services.settings import SettingsService
from snapper.application.services.settings import get_settings_service
from snapper.auth.dependencies import get_csrf_manager
from snapper.auth.dependencies import require_permission
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.permissions import Permission
from snapper.auth.routes import router as auth_router
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.tokens import get_token_manager
from snapper.config.settings import AppSettings
from snapper.config.settings import get_settings
from snapper.config.settings import get_settings_with_service
from snapper.config.settings_routes import router as settings_router
from snapper.core.types import HealthStatus
from snapper.core.types import MarketDataExchange
from snapper.core.types import OrderExchange
from snapper.core.types import SpawnerProcessStatus
from snapper.data.models import Candle
from snapper.data.models import Execution
from snapper.data.models import Instrument
from snapper.data.models import Order
from snapper.data.models import Position
from snapper.data.models import Signal
from snapper.data.models import Symbol
from snapper.data.models import SymbolAlias
from snapper.data.repository import Repository
from snapper.data.repository import dispose_repositories
from snapper.data.repository import get_repository
from snapper.data.repository import where_active
from snapper.interface.websocket.connection_manager import WebSocketConnectionManager
from snapper.interface.websocket.helpers import build_allowed_origins
from snapper.messaging.infrastructure.gap_detector import GapDetectorStats
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.data import CandleData
from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.schemas.data import OrderData
from snapper.messaging.schemas.data import PositionData
from snapper.messaging.schemas.data import SignalData
from snapper.server.authenticated_websocket import create_authenticated_websocket_router
from snapper.server.json_body import patch_openapi
from snapper.server.process_routes import router as process_router
from snapper.server.provenance_middleware import ClientProvenanceMiddleware
from snapper.server.rate_limiting import limiter
from snapper.server.strategy_routes import router as strategy_router
from snapper.utils.logging import set_log_context

API_PREFIX = "/api"


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


def get_repository_dependency() -> Repository:
    """FastAPI dependency for database repository.

    Returns:
        Repository instance for the configured database.
    """
    settings = get_settings()
    return get_repository(settings.db_url)


async def _initialize_settings_service(settings: AppSettings) -> SettingsService:
    """Initialize and configure SettingsService with ZMQ synchronization.

    Args:
        settings: Bootstrap application settings.

    Returns:
        Initialized SettingsService instance.
    """
    settings_service = await get_settings_service(
        settings.db_url,
        settings.zmq_broker_xpub,
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


async def _shutdown_zmq_bridge(app: FastAPI) -> None:
    """Stop ZMQ bridge and await its task during shutdown.

    Args:
        app: FastAPI application instance.
    """
    ws_manager: WebSocketConnectionManager = app.state.manager
    if not (ws_manager.zmq_bridge and app.state.zmq_bridge_task):
        return
    await ws_manager.zmq_bridge.stop()
    try:
        await app.state.zmq_bridge_task
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
    settings_service = await _initialize_settings_service(settings)
    settings = get_settings_with_service(settings_service)
    app.state.settings = settings
    _configure_auth_services(settings_service)
    discover_processes()
    process_factory = ProcessLauncherService(settings)
    app.state.process_factory = process_factory
    app.state.zmq_bridge_task = None
    try:
        logger.info("Starting application lifespan - checking autostart settings")
        await process_factory.sync_registry_to_database()
        if settings.server_api_only:
            logger.info("API-only mode (SERVER_API_ONLY=true) — skipping process autostart")
        else:
            await process_factory.start_all_processes()
        manager_ref: WebSocketConnectionManager = app.state.manager
        app.state.zmq_bridge_task = asyncio.create_task(manager_ref.zmq_bridge.start())
        logger.info("Application startup complete")
        yield
    except asyncio.CancelledError:
        logger.info("Application lifespan cancelled by shutdown signal")
        raise
    finally:
        logger.info("Starting application shutdown sequence")
        await _shutdown_zmq_bridge(app)
        await process_factory.stop_all_processes()
        manager = app.state.manager
        await manager.cleanup()
        await settings_service.shutdown()
        await dispose_repositories()
        logger.info("Application shutdown complete")


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
    app.include_router(process_router, prefix=API_PREFIX)
    app.include_router(strategy_router, prefix=API_PREFIX)
    app.include_router(create_api_router(manager), prefix=API_PREFIX)
    app.include_router(create_authenticated_websocket_router(manager), prefix=API_PREFIX)

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
        "running"
        if TRADER_COORDINATOR_PROCESS in process_factory.started_processes
        else "not_running"
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


def _apply_signal_filters(
    query: Select[Any],
    instrument: str | None,
    strategy: str | None,
    exchange: OrderExchange | None,
    at: datetime,
) -> Select[Any]:
    """Apply optional instrument/strategy/exchange filters to a signal query.

    Args:
        query: Base signal query.
        instrument: Filter by instrument symbol.
        strategy: Filter by strategy name.
        exchange: Filter by exchange.
        at: Point-in-time for temporal filtering.

    Returns:
        Filtered query.
    """
    if instrument:
        s_ts, s_kt = where_active(Symbol, at)
        sym_subq = (
            select(Symbol.public_id)
            .where(Symbol.native_symbol == instrument, s_ts, s_kt)
            .scalar_subquery()
        )
        query = query.where(Instrument.symbol_public_id == sym_subq)
    if strategy:
        query = query.where(Signal.strategy_name == strategy)
    if exchange:
        query = query.where(Instrument.exchange == exchange)
    return query


def _create_candles_signals_router() -> APIRouter:
    """Create router for candles and signals endpoints.

    Returns:
        APIRouter with candles and signals endpoints.
    """
    router = APIRouter()

    @router.get(
        "/candles",
        response_model=None,
        responses={500: {"description": "Internal server error"}},
    )
    async def get_candles(
        request: Request,
        _auth: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_MARKET_DATA))],
        _csrf: Annotated[None, Depends(validate_csrf_token)],
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
            instrument: Instrument symbol to query.
            exchange: Exchange name to query.
            timeframe: Candle timeframe (e.g. '1m', '1h').
            limit: Maximum number of candles to return.
            as_of: Optional point-in-time query timestamp.

        Returns:
            CandleListResponse wrapping the candle data (empty payload if no instrument found).
        """
        settings = get_settings()
        repo = get_repository(settings.db_url)
        processing_date = as_of or datetime.now(UTC)
        try:
            async with repo.session() as session:
                s_ts, s_kt = where_active(Symbol, processing_date)
                sym_q = await session.execute(
                    select(Symbol.public_id).where(Symbol.native_symbol == instrument, s_ts, s_kt)
                )
                symbol_pid = sym_q.scalar_one_or_none()
                tracker: SequenceTracker = request.app.state.rest_tracker
                if symbol_pid is None:
                    return CandleListResponse(
                        payload=[],
                        count=0,
                        session_id=tracker.session_id,
                        sequence_id=tracker.next_sequence(_REST_DATA_STREAM),
                        public_id=str(uuid7()),
                        timestamp=dt.datetime.now(dt.UTC),
                    )
                inst_query = await session.execute(
                    select(Instrument).where(
                        Instrument.symbol_public_id == symbol_pid,
                        Instrument.exchange == exchange,
                        Instrument.timestamp <= processing_date,
                        Instrument.known_to > processing_date,
                    )
                )
                inst = inst_query.scalars().first()
                if not inst:
                    return CandleListResponse(
                        payload=[],
                        count=0,
                        session_id=tracker.session_id,
                        sequence_id=tracker.next_sequence(_REST_DATA_STREAM),
                        public_id=str(uuid7()),
                        timestamp=dt.datetime.now(dt.UTC),
                    )
                candles_query = await session.execute(
                    select(Candle)
                    .where(
                        Candle.instrument_public_id == inst.public_id,
                        Candle.timeframe == timeframe,
                        Candle.timestamp <= processing_date,
                        Candle.known_to > processing_date,
                    )
                    .order_by(desc(Candle.open_at))
                    .limit(limit)
                )
                candles = candles_query.scalars().all()
                items = [
                    CandleData(
                        public_id=candle.public_id,
                        timestamp=candle.timestamp,
                        session_id=candle.session_id,
                        sequence_id=candle.sequence_id,
                        instrument=instrument,
                        exchange=exchange,
                        timeframe=candle.timeframe,
                        open_at=candle.open_at,
                        open=candle.open,
                        high=candle.high,
                        low=candle.low,
                        close=candle.close,
                        volume=candle.volume,
                        vwap=candle.vwap,
                        trades=candle.trades,
                    )
                    for candle in reversed(candles)
                ]
                tracker = request.app.state.rest_tracker
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

    @router.get("/signals", responses={500: {"description": "Internal server error"}})
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

        Returns:
            SignalListResponse wrapping the signal data.
        """
        processing_date = as_of or datetime.now(UTC)
        try:
            async with repo.session() as session:
                since = processing_date - timedelta(hours=hours)
                query = (
                    select(Signal, Instrument, Symbol)
                    .join(
                        Instrument,
                        and_(
                            Signal.instrument_public_id == Instrument.public_id,
                            *where_active(Instrument, processing_date),
                        ),
                    )
                    .join(
                        Symbol,
                        and_(
                            Instrument.symbol_public_id == Symbol.public_id,
                            *where_active(Symbol, processing_date),
                        ),
                    )
                )
                query = query.where(Signal.fired_at >= since)
                query = query.where(
                    Signal.timestamp <= processing_date, Signal.known_to > processing_date
                )
                query = _apply_signal_filters(
                    query, instrument, strategy, exchange, processing_date
                )
                query = query.order_by(desc(Signal.fired_at)).limit(limit)
                result = await session.execute(query)
                signals_with_instruments = result.all()
                items = [
                    SignalData(
                        public_id=signal.public_id,
                        timestamp=signal.timestamp,
                        session_id=signal.session_id,
                        sequence_id=signal.sequence_id,
                        instrument=sym.native_symbol,
                        exchange=inst.exchange,
                        side=signal.side,
                        strength=signal.strength,
                        reason=signal.reason,
                        strategy_name=signal.strategy_name,
                        price=signal.price,
                        fired_at=signal.fired_at,
                    )
                    for signal, inst, sym in signals_with_instruments
                ]
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

    @router.get("/exchanges", responses={500: {"description": "Internal server error"}})
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
            async with repo.session() as session:
                now = as_of or datetime.now(UTC)
                result = await session.execute(
                    select(distinct(SymbolAlias.exchange))
                    .where(
                        SymbolAlias.timestamp <= now,
                        SymbolAlias.known_to > now,
                    )
                    .order_by(SymbolAlias.exchange)
                )
                items = list(result.scalars().all())
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
        responses={500: {"description": "Internal server error"}},
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
            async with repo.session() as session:
                now = as_of or datetime.now(UTC)
                result = await session.execute(
                    select(distinct(Symbol.native_symbol))
                    .select_from(SymbolAlias)
                    .join(Symbol, Symbol.public_id == SymbolAlias.symbol_public_id)
                    .where(
                        SymbolAlias.exchange == exchange,
                        SymbolAlias.timestamp <= now,
                        SymbolAlias.known_to > now,
                        Symbol.timestamp <= now,
                        Symbol.known_to > now,
                    )
                    .order_by(Symbol.native_symbol)
                )
                items = list(result.scalars().all())
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

    return router


def _apply_order_filters(
    query: Select[Any],
    symbol: str | None,
    exchange: OrderExchange | None,
    at: datetime,
) -> Select[Any]:
    """Apply optional symbol/exchange filters to an orders query.

    Args:
        query: Base orders query.
        symbol: Filter by instrument symbol.
        exchange: Filter by exchange.
        at: Point-in-time for temporal filtering.

    Returns:
        Filtered query.
    """
    if symbol:
        s_ts, s_kt = where_active(Symbol, at)
        sym_subq = (
            select(Symbol.public_id)
            .where(Symbol.native_symbol == symbol, s_ts, s_kt)
            .scalar_subquery()
        )
        query = query.where(Instrument.symbol_public_id == sym_subq)
    if exchange:
        query = query.where(Instrument.exchange == exchange)
    return query


def _create_orders_executions_router() -> APIRouter:
    """Create router for orders, executions, and positions endpoints.

    Returns:
        APIRouter with orders, executions, and positions endpoints.
    """
    router = APIRouter()

    @router.get("/orders", responses={500: {"description": "Internal server error"}})
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

        Returns:
            OrderListResponse wrapping the order data.
        """
        processing_date = as_of or datetime.now(UTC)
        try:
            async with repo.session() as session:
                query = (
                    select(Order, Instrument, Symbol)
                    .join(
                        Instrument,
                        and_(
                            Order.instrument_public_id == Instrument.public_id,
                            *where_active(Instrument, processing_date),
                        ),
                    )
                    .join(
                        Symbol,
                        and_(
                            Instrument.symbol_public_id == Symbol.public_id,
                            *where_active(Symbol, processing_date),
                        ),
                    )
                )
                query = query.where(
                    Order.timestamp <= processing_date, Order.known_to > processing_date
                )
                query = _apply_order_filters(query, symbol, exchange, processing_date)
                query = query.order_by(desc(Order.created_at)).offset(offset).limit(limit)
                result = await session.execute(query)
                orders_with_instruments = result.all()
                items = [
                    OrderData(
                        public_id=order.public_id,
                        timestamp=order.timestamp,
                        session_id=order.session_id,
                        sequence_id=order.sequence_id,
                        instrument=sym.native_symbol,
                        exchange=inst.exchange,
                        client_order_id=order.client_order_id or "",
                        exchange_order_id=order.exchange_order_id,
                        created_at=order.created_at,
                        updated_at=order.updated_at,
                        side=order.side,
                        order_type=order.order_type,
                        price=order.price,
                        size=order.size,
                        filled_size=order.filled_size,
                        average_price=order.average_price,
                        status=order.status,
                        time_in_force=order.time_in_force,
                        error=order.error,
                    )
                    for order, inst, sym in orders_with_instruments
                ]
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
        except Exception as exc:
            logger.error(f"Failed to fetch orders: {exc}")
            raise HTTPException(status_code=500, detail="Failed to fetch orders") from exc

    @router.get("/executions", responses={500: {"description": "Internal server error"}})
    async def get_executions(
        request: Request,
        _auth: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_ORDERS))],
        _csrf: Annotated[None, Depends(validate_csrf_token)],
        repo: Annotated[Repository, Depends(get_repository_dependency)],
        limit: Annotated[int, Query(le=1000, description="Number of executions to return")] = 100,
        as_of: Annotated[datetime | None, Query(description="Point-in-time query (UTC)")] = None,
    ) -> ExecutionListResponse:
        """Fetch execution (fill) records.

        Args:
            request: FastAPI request (provides REST tracker for provenance).
            _auth: Authenticated user with READ_ORDERS permission.
            _csrf: CSRF token validation.
            repo: Database repository.
            limit: Maximum number of executions to return.
            as_of: Optional point-in-time query timestamp.

        Returns:
            ExecutionListResponse wrapping the execution data.
        """
        processing_date = as_of or datetime.now(UTC)
        try:
            async with repo.session() as session:
                query = (
                    select(Execution, Order, Instrument, Symbol)
                    .join(
                        Order,
                        and_(
                            Execution.order_public_id == Order.public_id,
                            Order.timestamp <= processing_date,
                            Order.known_to > processing_date,
                        ),
                    )
                    .join(
                        Instrument,
                        and_(
                            Order.instrument_public_id == Instrument.public_id,
                            *where_active(Instrument, processing_date),
                        ),
                    )
                    .join(
                        Symbol,
                        and_(
                            Instrument.symbol_public_id == Symbol.public_id,
                            *where_active(Symbol, processing_date),
                        ),
                    )
                    .where(Execution.timestamp <= processing_date)
                    .where(Execution.known_to > processing_date)
                    .order_by(desc(Execution.timestamp))
                    .limit(limit)
                )
                result = await session.execute(query)
                rows = result.all()
                items = [
                    ExecutionData(
                        public_id=execution.public_id,
                        timestamp=execution.timestamp,
                        session_id=execution.session_id,
                        sequence_id=execution.sequence_id,
                        trade_id=execution.trade_id,
                        exchange_order_id=order.exchange_order_id,
                        client_order_id=order.client_order_id or "",
                        instrument=sym.native_symbol,
                        exchange=inst.exchange,
                        side=execution.side,
                        size=execution.size,
                        price=execution.price,
                        fee=execution.fee,
                        fee_asset=execution.fee_asset,
                        status=execution.status,
                        executed_at=execution.executed_at or execution.timestamp,
                    )
                    for execution, order, inst, sym in rows
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
        except Exception as exc:
            logger.error(f"Failed to fetch executions: {exc}")
            raise HTTPException(status_code=500, detail="Failed to fetch executions") from exc

    @router.get("/positions", responses={500: {"description": "Internal server error"}})
    async def get_positions(
        request: Request,
        _auth: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_POSITIONS))],
        _csrf: Annotated[None, Depends(validate_csrf_token)],
        repo: Annotated[Repository, Depends(get_repository_dependency)],
        as_of: Annotated[datetime | None, Query(description="Point-in-time query (UTC)")] = None,
    ) -> PositionListResponse:
        """Fetch current portfolio positions.

        Args:
            request: FastAPI request (provides REST tracker for provenance).
            _auth: Authenticated user with READ_POSITIONS permission.
            _csrf: CSRF token validation.
            repo: Database repository.
            as_of: Optional point-in-time query timestamp.

        Returns:
            PositionListResponse wrapping the position data.
        """
        processing_date = as_of or datetime.now(UTC)
        try:
            async with repo.session() as session:
                query = (
                    select(Position, Instrument, Symbol)
                    .join(
                        Instrument,
                        and_(
                            Position.instrument_public_id == Instrument.public_id,
                            *where_active(Instrument, processing_date),
                        ),
                    )
                    .join(
                        Symbol,
                        and_(
                            Instrument.symbol_public_id == Symbol.public_id,
                            *where_active(Symbol, processing_date),
                        ),
                    )
                )
                query = query.where(
                    Position.timestamp <= processing_date,
                    Position.known_to > processing_date,
                )
                result = await session.execute(query)
                positions_with_instruments = result.all()
                items = [
                    PositionData(
                        public_id=position.public_id,
                        timestamp=position.timestamp,
                        session_id=position.session_id,
                        sequence_id=position.sequence_id,
                        instrument=sym.native_symbol,
                        exchange=inst.exchange,
                        quantity=position.quantity,
                        average_price=position.average_price,
                        unrealized_pnl=position.unrealized_pnl,
                        realized_pnl=position.realized_pnl,
                    )
                    for position, inst, sym in positions_with_instruments
                ]
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
                status="healthy",
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
                    heartbeat_interval_ms=zmq_bridge.settings.zmq_heartbeat_interval_ms,
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
        status: HealthStatus = "healthy" if not error_messages else "error"
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
                    zmq_context="ok" if not error_messages else "error",
                    websocket_manager="ok",
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
    router.include_router(_create_orders_executions_router())
    router.include_router(_create_monitoring_endpoints_router(manager))
    return router


__all__ = ["create_app"]
app = create_app()
