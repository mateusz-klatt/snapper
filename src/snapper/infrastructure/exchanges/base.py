"""Abstract base class for exchange client implementations.

This module defines the ExchangeClientBase abstract class that serves as
the contract for all exchange client implementations. It provides:

- Async context manager support for connection lifecycle
- Abstract methods for market data retrieval (tickers, OHLCV)
- Abstract methods for order management (create, cancel, get)
- Abstract methods for real-time data subscriptions via WebSocket
- Internal methods for logging orders and executions to database

All exchange implementations (Kraken, Zonda, Walutomat, Paper, Polygon)
must inherit from this base class and implement its abstract methods.
"""

from abc import ABC
from abc import abstractmethod
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from types import TracebackType
from typing import Any
from typing import Self

from loguru import logger
from sqlalchemy.exc import SQLAlchemyError

from snapper.data.repository import Repository
from snapper.infrastructure.exchanges.contracts import AccountBalance
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import ExchangeOrderRequest
from snapper.infrastructure.exchanges.contracts import ExchangeOrderSnapshot
from snapper.infrastructure.exchanges.contracts import ExecutionUpdate
from snapper.infrastructure.exchanges.contracts import OhlcvSnapshot
from snapper.infrastructure.exchanges.contracts import OrderStatusEnum
from snapper.infrastructure.exchanges.contracts import TickerSnapshot
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate

__all__ = ["ExchangeClientBase"]


class ExchangeClientBase(ABC):
    """Abstract base class defining the interface for exchange clients.

    This class provides a standardized interface for interacting with
    cryptocurrency and FX exchanges. Implementations must provide both
    REST API methods for data retrieval and order management, as well
    as WebSocket subscriptions for real-time market data.

    Attributes:
        supports_websocket_executions: Whether the exchange supports
            real-time execution updates via WebSocket.
        repository: Optional database repository for order/execution logging.
        exchange_name: Name identifier for the exchange.
    """

    supports_websocket_executions: bool = True

    def __init__(
        self, repository: Repository | None = None, exchange_name: str = "unknown"
    ) -> None:
        """Initialize the exchange client base.

        Args:
            repository: Optional database repository for persisting orders
                and executions. If None, database logging is disabled.
            exchange_name: Identifier for the exchange (e.g., "kraken", "zonda").
        """
        self.repository = repository
        self.exchange_name = exchange_name

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the exchange.

        This method should initialize any HTTP clients, WebSocket connections,
        and authenticate with the exchange if credentials are provided.

        Raises:
            RuntimeError: If connection fails.
        """
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        """Gracefully close all connections to the exchange.

        This method should close WebSocket connections, HTTP sessions,
        and release any other resources.
        """
        ...

    async def __aenter__(self) -> Self:
        """Async context manager entry point.

        Returns:
            Self: The connected exchange client instance.
        """
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Async context manager exit point.

        Args:
            exc_type: Exception type if an exception was raised.
            exc_val: Exception instance if an exception was raised.
            exc_tb: Traceback if an exception was raised.
        """
        await self.disconnect()

    @abstractmethod
    async def get_ticker(self, symbol: str) -> TickerSnapshot:
        """Fetch current ticker data for a symbol.

        Args:
            symbol: Trading pair symbol in native format (e.g., "BTC/USD").

        Returns:
            TickerSnapshot with current bid, ask, last price, and volume.

        Raises:
            ValueError: If symbol is not supported.
            RuntimeError: If API request fails.
        """
        ...

    @abstractmethod
    async def get_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1m",
        since: int | None = None,
        limit: int | None = None,
    ) -> list[OhlcvSnapshot]:
        """Fetch OHLCV (candlestick) data for a symbol.

        Args:
            symbol: Trading pair symbol in native format.
            timeframe: Candle interval (e.g., "1m", "5m", "1h", "1d").
            since: Start timestamp in milliseconds. If None, fetches recent data.
            limit: Maximum number of candles to return.

        Returns:
            List of OhlcvSnapshot objects ordered by timestamp ascending.

        Raises:
            ValueError: If symbol or timeframe is not supported.
            RuntimeError: If API request fails.
        """
        ...

    @abstractmethod
    async def create_order(self, request: ExchangeOrderRequest) -> ExchangeOrderSnapshot:
        """Submit a new order to the exchange.

        Args:
            request: Order parameters including symbol, side, type, amount, price.

        Returns:
            ExchangeOrderSnapshot with the created order details.

        Raises:
            RuntimeError: If API credentials are not configured or order fails.
            ValueError: If order parameters are invalid.
        """
        ...

    @abstractmethod
    async def cancel_order(self, order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        """Cancel an existing order.

        Args:
            order_id: Exchange order ID to cancel.
            symbol: Optional symbol (required by some exchanges).

        Returns:
            ExchangeOrderSnapshot with the canceled order status.

        Raises:
            RuntimeError: If API credentials are not configured or cancel fails.
            ValueError: If order is not found.
        """
        ...

    @abstractmethod
    async def get_order(self, order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        """Fetch details of a specific order.

        Args:
            order_id: Exchange order ID to retrieve.
            symbol: Optional symbol (required by some exchanges).

        Returns:
            ExchangeOrderSnapshot with current order status and fills.

        Raises:
            RuntimeError: If API credentials are not configured.
            ValueError: If order is not found.
        """
        ...

    @abstractmethod
    async def get_orders(
        self,
        symbol: str | None = None,
        status: OrderStatusEnum | None = None,
        limit: int | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        """Fetch multiple orders with optional filters.

        Args:
            symbol: Filter by trading pair. If None, returns all symbols.
            status: Filter by order status (OPEN, CLOSED, CANCELED, etc.).
            limit: Maximum number of orders to return.

        Returns:
            List of ExchangeOrderSnapshot objects matching the filters.

        Raises:
            RuntimeError: If API credentials are not configured.
        """
        ...

    @abstractmethod
    async def get_balance(self, currency: str | None = None) -> dict[str, AccountBalance]:
        """Fetch account balances.

        Args:
            currency: Filter by specific currency. If None, returns all balances.

        Returns:
            Dictionary mapping currency codes to AccountBalance objects.

        Raises:
            RuntimeError: If API credentials are not configured.
        """
        ...

    @abstractmethod
    def subscribe_ticks(self, symbols: list[str]) -> AsyncIterator[TickerUpdate]:
        """Subscribe to real-time ticker updates.

        Args:
            symbols: List of symbols to subscribe. Use ["*"] for all symbols.

        Yields:
            TickerUpdate objects as they arrive from the exchange.

        Raises:
            RuntimeError: If WebSocket connection is not established.
        """
        ...

    @abstractmethod
    def subscribe_candles(
        self,
        symbols: list[str],
        timeframe: str = "1m",
    ) -> AsyncIterator[CandleUpdate]:
        """Subscribe to real-time candlestick updates.

        Args:
            symbols: List of symbols to subscribe. Use ["*"] for all symbols.
            timeframe: Candle interval (e.g., "1m", "5m", "1h").

        Yields:
            CandleUpdate objects as candles complete or update.

        Raises:
            RuntimeError: If WebSocket connection is not established.
        """
        ...

    @abstractmethod
    def subscribe_trades(self, symbols: list[str]) -> AsyncIterator[TradeUpdate]:
        """Subscribe to real-time trade updates.

        Args:
            symbols: List of symbols to subscribe. Use ["*"] for all symbols.

        Yields:
            TradeUpdate objects for each executed trade on the exchange.

        Raises:
            RuntimeError: If WebSocket connection is not established.
        """
        ...

    @abstractmethod
    def subscribe_executions(self) -> AsyncIterator[ExecutionUpdate]:
        """Subscribe to real-time execution updates for user's orders.

        This subscription requires API credentials and provides updates
        when the user's orders are filled, partially filled, or canceled.

        Yields:
            ExecutionUpdate objects for each execution event.

        Raises:
            RuntimeError: If API credentials are not configured or
                WebSocket connection is not established.
        """
        ...

    @abstractmethod
    def subscribe_instruments(self, **kwargs: Any) -> AsyncIterator[dict[str, Any]]:
        """Subscribe to instrument/market information updates.

        This subscription provides information about available trading pairs,
        their specifications, and status updates.

        Args:
            **kwargs: Exchange-specific subscription parameters.

        Yields:
            Dictionary containing instrument information.

        Raises:
            NotImplementedError: If exchange does not support this feature.
        """
        ...

    async def _log_order_to_db(
        self,
        request: ExchangeOrderRequest,
        order: ExchangeOrderSnapshot,
    ) -> int | None:
        """Persist a new order to the database.

        This internal method is called after successfully creating an order
        on the exchange. It upserts the instrument and inserts the order record.

        Args:
            request: Original order request with parameters.
            order: Exchange response with order details.

        Returns:
            Database order ID if successful, None if repository is not
            configured or operation fails.
        """
        if self.repository is None:
            return None
        try:
            instrument_id = await self.repository.upsert_instrument(
                symbol=request.symbol, exchange=self.exchange_name
            )
            db_order_id = await self.repository.insert_order(
                instrument_id=instrument_id,
                client_order_id=order.client_order_id,
                exchange_order_id=order.id,
                created_at=datetime.fromtimestamp(order.timestamp, tz=UTC),
                side=order.side.value,
                order_type=order.type.value,
                price=order.price,
                size=order.amount,
                status=order.status.value,
                time_in_force=None,
            )
            return db_order_id
        except SQLAlchemyError as e:
            logger.error(f"Failed to log order to database: {e}")
            return None

    async def _log_order_update_to_db(
        self,
        db_order_id: int,
        status: OrderStatusEnum,
        exchange_order_id: str | None = None,
        error: str | None = None,
    ) -> None:
        """Update an existing order record in the database.

        This internal method is called when an order status changes
        (e.g., filled, canceled, rejected).

        Args:
            db_order_id: Database order ID to update.
            status: New order status.
            exchange_order_id: Exchange order ID if it changed.
            error: Error message if order was rejected.
        """
        if self.repository is None:
            return
        try:
            await self.repository.update_order(
                order_id=db_order_id,
                status=status.value,
                updated_at=datetime.now(tz=UTC),
                exchange_order_id=exchange_order_id,
                error=error,
            )
        except SQLAlchemyError as e:
            logger.error(f"Failed to log order update to database: {e}")

    async def _log_execution_to_db(
        self,
        db_order_id: int,
        execution: ExecutionUpdate,
    ) -> None:
        """Persist an execution (fill) to the database.

        This internal method is called when an order is partially or
        fully filled to record the execution details.

        Args:
            db_order_id: Database order ID that was executed.
            execution: Execution details including price, size, and fees.
        """
        if self.repository is None:
            return
        try:
            await self.repository.insert_execution(
                order_id=db_order_id,
                timestamp=execution.timestamp,
                price=execution.last_price or execution.average_price or 0.0,
                size=execution.last_qty or execution.cum_qty or 0.0,
                fee=execution.fee_usd_equiv or 0.0,
                fee_asset="USD",
            )
        except SQLAlchemyError as e:
            logger.error(f"Failed to log execution to database: {e}")
