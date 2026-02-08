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
from datetime import timedelta
from typing import Annotated
from typing import Any

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
from sqlalchemy import and_
from sqlalchemy import desc
from sqlalchemy import distinct
from sqlalchemy import select

from snapper.api.auth.services.ws_token_service import get_ws_token_service
from snapper.api.schemas.executions import ExecutionRecord
from snapper.api.schemas.health import HealthCheckResponse
from snapper.api.schemas.health import HealthTopics
from snapper.api.schemas.health import SubscriptionsStats
from snapper.api.schemas.health import WebSocketStats
from snapper.api.schemas.health import WsStatsConfig
from snapper.api.schemas.health import WsStatsResponse
from snapper.api.schemas.health import ZmqBridgeStats
from snapper.api.schemas.health import ZmqComponents
from snapper.api.schemas.health import ZmqConfig
from snapper.api.schemas.health import ZmqHealthResponse
from snapper.api.schemas.market_data import CandleSnapshot
from snapper.api.schemas.orders import OrderStatus
from snapper.api.schemas.portfolio import PositionSnapshot
from snapper.api.schemas.process import ProcessStatus
from snapper.api.schemas.process import SystemStatus
from snapper.api.schemas.signals import TradingSignal
from snapper.application.process_manager.launcher import ProcessLauncherService
from snapper.application.process_manager.registry import discover_processes
from snapper.application.services.settings import get_settings_service
from snapper.auth.dependencies import get_csrf_manager
from snapper.auth.dependencies import require_authentication
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.routes import router as auth_router
from snapper.auth.schemas.user import UserProfile
from snapper.auth.tokens import get_token_manager
from snapper.config.settings import AppSettings
from snapper.config.settings import get_settings
from snapper.config.settings import get_settings_with_service
from snapper.config.settings_routes import router as settings_router
from snapper.core.types import HealthStatus
from snapper.data.models import Candle
from snapper.data.models import Execution
from snapper.data.models import Instrument
from snapper.data.models import OrderRecord
from snapper.data.models import Position
from snapper.data.models import SignalEvent
from snapper.data.models import SymbolAlias
from snapper.data.repository import Repository
from snapper.data.repository import dispose_repositories
from snapper.data.repository import get_repository
from snapper.interface.websocket.connection_manager import WebSocketConnectionManager
from snapper.interface.websocket.helpers import build_allowed_origins
from snapper.messaging.topics.schemas import get_all_topic_names
from snapper.server.authenticated_websocket import create_authenticated_websocket_router
from snapper.server.process_routes import router as process_router
from snapper.server.rate_limiting import limiter
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
    detail = exc.detail if isinstance(exc, HTTPException) else str(exc)
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


async def _initialize_settings_service(settings: AppSettings) -> Any:
    """Initialize and configure SettingsService with ZMQ synchronization.

    Args:
        settings: Bootstrap application settings.

    Returns:
        Initialized SettingsService instance.
    """
    settings_service = await get_settings_service(
        settings.db_url,
        settings.zmq_broker_xpub,
        settings.master_password,
        settings.encryption_salt,
    )
    logger.info("AppSettings initialized with database access (cached, ZMQ-synced)")
    return settings_service


def _configure_auth_services(settings_service: Any) -> None:
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
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
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
        await process_factory.start_all_processes()
        manager: WebSocketConnectionManager = app.state.manager
        app.state.zmq_bridge_task = asyncio.create_task(manager.zmq_bridge.start())
        logger.info("Application startup complete - autostart processes initialized")
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
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, handle_rate_limit_exceeded)
    app.add_middleware(SlowAPIMiddleware)
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
    app.include_router(create_api_router(manager), prefix=API_PREFIX)
    app.include_router(create_authenticated_websocket_router(manager), prefix=API_PREFIX)

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


def _build_strategy_payload(raw_status: dict[str, Any]) -> dict[str, Any] | None:
    """Build a strategy payload dict from a raw process status.

    Returns None if the raw status does not represent a strategy.

    Args:
        raw_status: Raw process status dictionary.

    Returns:
        Strategy payload dict or None if not a strategy status.
    """
    if not isinstance(raw_status, dict):
        return None
    if "strategy_name" not in raw_status:
        return None
    normalized = _normalize_strategy_status(raw_status)
    payload: dict[str, Any] = {
        "strategy_name": str(normalized.get("strategy_name", "unknown")),
        "status": normalized.get("status", "unknown"),
        "details": normalized,
    }
    for key in _STRATEGY_STATUS_KEYS:
        if key in normalized:
            payload[key] = normalized[key]
    return payload


def _collect_strategy_statuses(process_factory: ProcessLauncherService) -> list[dict[str, Any]]:
    """Collect strategy statuses from all running processes.

    Args:
        process_factory: Process launcher service with started processes.

    Returns:
        List of strategy payload dicts.
    """
    strategies: list[dict[str, Any]] = []
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
        response_model=list[CandleSnapshot],
        responses={500: {"description": "Internal server error"}},
    )
    async def get_candles(
        _auth: Annotated[UserProfile, Depends(require_authentication)],
        _csrf: Annotated[None, Depends(validate_csrf_token)],
        instrument: Annotated[str, Query(description="Instrument symbol")],
        exchange: Annotated[str, Query(description="Exchange name")],
        timeframe: Annotated[str, Query(description="Timeframe")],
        limit: Annotated[int, Query(le=1000, description="Number of candles to return")] = 100,
    ) -> list[CandleSnapshot] | Response:
        settings = get_settings()
        repo = get_repository(settings.db_url)
        try:
            async with repo.session() as session:
                inst_query = await session.execute(
                    select(Instrument).where(
                        and_(
                            Instrument.symbol == instrument,
                            Instrument.exchange == exchange,
                        )
                    )
                )
                inst = inst_query.scalars().first()
                if not inst:
                    return Response(status_code=204)
                candles_query = await session.execute(
                    select(Candle)
                    .where(Candle.instrument_id == inst.id, Candle.timeframe == timeframe)
                    .order_by(desc(Candle.timestamp))
                    .limit(limit)
                )
                candles = candles_query.scalars().all()
                return [
                    CandleSnapshot(
                        instrument=instrument,
                        timeframe=candle.timeframe,
                        timestamp=candle.timestamp,
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
        except HTTPException:
            raise
        except Exception as exc:
            logger.error(f"Failed to fetch candles for {instrument}: {exc}")
            raise HTTPException(status_code=500, detail="Failed to fetch candle data") from exc

    @router.get("/signals", responses={500: {"description": "Internal server error"}})
    async def get_signals(
        _auth: Annotated[UserProfile, Depends(require_authentication)],
        _csrf: Annotated[None, Depends(validate_csrf_token)],
        repo: Annotated[Repository, Depends(get_repository_dependency)],
        instrument: Annotated[str | None, Query(description="Filter by instrument")] = None,
        strategy: Annotated[str | None, Query(description="Filter by strategy")] = None,
        exchange: Annotated[str | None, Query(description="Filter by exchange")] = None,
        hours: Annotated[int, Query(le=168, description="Hours of history to return")] = 24,
        limit: Annotated[int, Query(le=1000, description="Number of signals to return")] = 100,
    ) -> list[TradingSignal]:
        try:
            async with repo.session() as session:
                since = dt.datetime.now(dt.UTC) - timedelta(hours=hours)
                query = select(SignalEvent, Instrument).join(Instrument)
                query = query.where(SignalEvent.timestamp >= since)
                if instrument:
                    query = query.where(Instrument.symbol == instrument)
                if strategy:
                    query = query.where(SignalEvent.strategy_name == strategy)
                if exchange:
                    query = query.where(Instrument.exchange == exchange)
                query = query.order_by(desc(SignalEvent.timestamp)).limit(limit)
                result = await session.execute(query)
                signals_with_instruments = result.all()
                return [
                    TradingSignal(
                        id=signal.id,
                        instrument=inst.symbol,
                        exchange=inst.exchange,
                        timestamp=signal.timestamp,
                        side=signal.side,
                        strength=signal.strength,
                        reason=signal.reason,
                        strategy_name=signal.strategy_name,
                        price=signal.price,
                    )
                    for signal, inst in signals_with_instruments
                ]
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
        _auth: Annotated[UserProfile, Depends(require_authentication)],
        _csrf: Annotated[None, Depends(validate_csrf_token)],
        repo: Annotated[Repository, Depends(get_repository_dependency)],
    ) -> list[str]:
        """Return distinct exchange names from symbol_aliases."""
        try:
            async with repo.session() as session:
                result = await session.execute(
                    select(distinct(SymbolAlias.exchange)).order_by(SymbolAlias.exchange)
                )
                return list(result.scalars().all())
        except Exception as exc:
            logger.error(f"Failed to fetch exchanges: {exc}")
            raise HTTPException(status_code=500, detail="Failed to fetch exchanges") from exc

    @router.get(
        "/exchanges/{exchange}/instruments",
        responses={500: {"description": "Internal server error"}},
    )
    async def get_exchange_instruments(
        exchange: str,
        _auth: Annotated[UserProfile, Depends(require_authentication)],
        _csrf: Annotated[None, Depends(validate_csrf_token)],
        repo: Annotated[Repository, Depends(get_repository_dependency)],
    ) -> list[str]:
        """Return distinct native symbols available on a given exchange."""
        try:
            async with repo.session() as session:
                result = await session.execute(
                    select(distinct(SymbolAlias.native_symbol))
                    .where(SymbolAlias.exchange == exchange)
                    .order_by(SymbolAlias.native_symbol)
                )
                return list(result.scalars().all())
        except Exception as exc:
            logger.error(f"Failed to fetch instruments for {exchange}: {exc}")
            raise HTTPException(status_code=500, detail="Failed to fetch instruments") from exc

    return router


def _create_orders_executions_router() -> APIRouter:
    """Create router for orders, executions, and positions endpoints.

    Returns:
        APIRouter with orders, executions, and positions endpoints.
    """
    router = APIRouter()

    @router.get("/orders", responses={500: {"description": "Internal server error"}})
    async def get_orders(
        _auth: Annotated[UserProfile, Depends(require_authentication)],
        _csrf: Annotated[None, Depends(validate_csrf_token)],
        repo: Annotated[Repository, Depends(get_repository_dependency)],
        symbol: Annotated[str | None, Query(description="Symbol to filter by")] = None,
        exchange: Annotated[str | None, Query(description="Filter by exchange")] = None,
        limit: Annotated[int, Query(ge=1, le=1000, description="Number of orders to return")] = 100,
        offset: Annotated[int, Query(ge=0, description="Number of orders to skip")] = 0,
    ) -> list[OrderStatus]:
        try:
            async with repo.session() as session:
                query = select(OrderRecord, Instrument).join(Instrument)
                if symbol:
                    query = query.where(Instrument.symbol == symbol)
                if exchange:
                    query = query.where(Instrument.exchange == exchange)
                query = query.order_by(desc(OrderRecord.created_at)).offset(offset).limit(limit)
                result = await session.execute(query)
                orders_with_instruments = result.all()
                return [
                    OrderStatus(
                        id=order.id,
                        instrument=inst.symbol,
                        exchange=inst.exchange,
                        client_order_id=order.client_order_id,
                        exchange_order_id=order.exchange_order_id,
                        created_at=order.created_at,
                        updated_at=order.updated_at,
                        side=order.side,
                        type=order.type,
                        price=order.price,
                        size=order.size,
                        status=order.status,
                        time_in_force=order.time_in_force,
                        error=order.error,
                    )
                    for order, inst in orders_with_instruments
                ]
        except Exception as exc:
            logger.error(f"Failed to fetch orders: {exc}")
            raise HTTPException(status_code=500, detail="Failed to fetch orders") from exc

    @router.get("/executions", responses={500: {"description": "Internal server error"}})
    async def get_executions(
        _auth: Annotated[UserProfile, Depends(require_authentication)],
        _csrf: Annotated[None, Depends(validate_csrf_token)],
        repo: Annotated[Repository, Depends(get_repository_dependency)],
        limit: Annotated[int, Query(le=1000, description="Number of executions to return")] = 100,
    ) -> list[ExecutionRecord]:
        try:
            async with repo.session() as session:
                query = (
                    select(Execution, OrderRecord, Instrument)
                    .join(OrderRecord, Execution.order_id == OrderRecord.id)
                    .join(Instrument, OrderRecord.instrument_id == Instrument.id)
                    .order_by(desc(Execution.timestamp))
                    .limit(limit)
                )
                result = await session.execute(query)
                rows = result.all()
                return [
                    ExecutionRecord(
                        id=execution.id,
                        order_id=execution.order_id,
                        timestamp=execution.timestamp,
                        price=execution.price,
                        size=execution.size,
                        fee=execution.fee,
                        fee_asset=execution.fee_asset,
                        instrument=instrument.symbol,
                        side=order.side,
                        exchange=instrument.exchange,
                    )
                    for execution, order, instrument in rows
                ]
        except Exception as exc:
            logger.error(f"Failed to fetch executions: {exc}")
            raise HTTPException(status_code=500, detail="Failed to fetch executions") from exc

    @router.get("/positions", responses={500: {"description": "Internal server error"}})
    async def get_positions(
        _auth: Annotated[UserProfile, Depends(require_authentication)],
        _csrf: Annotated[None, Depends(validate_csrf_token)],
        repo: Annotated[Repository, Depends(get_repository_dependency)],
    ) -> list[PositionSnapshot]:
        try:
            async with repo.session() as session:
                query = select(Position, Instrument).join(Instrument)
                result = await session.execute(query)
                positions_with_instruments = result.all()
                return [
                    PositionSnapshot(
                        id=position.id,
                        instrument=inst.symbol,
                        exchange=inst.exchange,
                        quantity=position.quantity,
                        average_price=position.average_price,
                        unrealized_pnl=position.unrealized_pnl,
                        realized_pnl=position.realized_pnl,
                        updated_at=position.updated_at,
                    )
                    for position, inst in positions_with_instruments
                ]
        except Exception as exc:
            logger.error(f"Failed to fetch positions: {exc}")
            raise HTTPException(status_code=500, detail="Failed to fetch positions") from exc

    return router


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
    async def health_check() -> HealthCheckResponse:
        stats = manager.get_stats()
        return HealthCheckResponse(
            status="healthy",
            timestamp=dt.datetime.now(dt.UTC),
            version="0.1.0",
            connections=stats["connections"],
            topics=HealthTopics(
                available=len(get_all_topic_names()),
                active=stats["connections"].get(
                    "active_topics", len(zmq_bridge.topic_subscriptions)
                ),
            ),
        )

    @router.get("/ws/stats")
    async def websocket_stats(
        _auth: Annotated[UserProfile, Depends(require_authentication)],
        _csrf: Annotated[None, Depends(validate_csrf_token)],
    ) -> WsStatsResponse:
        stats = manager.get_stats()
        websocket_section = WebSocketStats(
            active_connections=len(manager.active_connections),
            topic_subscribers={
                topic: len(subs) for topic, subs in manager.topic_subscribers.items()
            },
            client_count=len(manager.active_connections),
        )
        bridge_section = ZmqBridgeStats(
            active_topics=len(zmq_bridge.topic_subscriptions),
            subscriber_tasks=len(zmq_bridge.subscriber_tasks),
            available_topics=list(zmq_bridge.available_topics),
        )
        return WsStatsResponse(
            websocket=websocket_section,
            zmq_bridge=bridge_section,
            connections=stats["connections"],
            topics=stats["topics"],
            subscriptions=SubscriptionsStats(
                per_topic={topic: len(subs) for topic, subs in manager.topic_subscribers.items()},
                per_client={
                    str(id(ws)): list(subs) for ws, subs in manager.client_subscriptions.items()
                },
            ),
            config=WsStatsConfig(
                broker_xpub=zmq_bridge.settings.zmq_broker_xpub,
                heartbeat_interval_ms=zmq_bridge.settings.zmq_heartbeat_interval_ms,
            ),
        )

    @router.get("/zmq/health")
    async def zmq_health_check(
        _auth: Annotated[UserProfile, Depends(require_authentication)],
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
        status: HealthStatus = "healthy" if not error_messages else "unhealthy"
        return ZmqHealthResponse(
            status=status,
            timestamp=dt.datetime.now(dt.UTC),
            components=ZmqComponents(
                zmq_context="ok" if not error_messages else "error",
                websocket_manager="ok",
                active_connections=stats["connections"].get("active_connections", 0),
            ),
            config=ZmqConfig(available_topics=available_topics),
            connections=stats["connections"],
            message_stats=stats["topics"],
            errors=error_messages,
        )

    @router.get("/status")
    async def get_system_status(
        request: Request,
        _auth: Annotated[UserProfile, Depends(require_authentication)],
        _csrf: Annotated[None, Depends(validate_csrf_token)],
    ) -> SystemStatus:
        process_factory: ProcessLauncherService = request.app.state.process_factory
        return SystemStatus(
            trader=ProcessStatus(status="not_running"),
            backtests={},
            strategies=_collect_strategy_statuses(process_factory),
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
