"""Tests for ExchangeClientBase abstract class."""

from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from sqlalchemy.exc import SQLAlchemyError

from snapper.data.repository import Repository
from snapper.infrastructure.exchanges.base import ExchangeClientBase
from snapper.infrastructure.exchanges.contracts import AccountBalance
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import ExchangeOrderRequest
from snapper.infrastructure.exchanges.contracts import ExchangeOrderSnapshot
from snapper.infrastructure.exchanges.contracts import ExecutionUpdate
from snapper.infrastructure.exchanges.contracts import OhlcvSnapshot
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.contracts import OrderStatusEnum
from snapper.infrastructure.exchanges.contracts import OrderTypeEnum
from snapper.infrastructure.exchanges.contracts import TickerSnapshot
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate


class DummyExchangeClient(ExchangeClientBase):
    """Stub exchange client for testing base class functionality."""

    def __init__(self, repository: Repository | None = None, exchange_name: str = "dummy") -> None:
        """Initialize the instance."""
        super().__init__(repository, exchange_name)
        self.connected = False
        self.disconnected = False

    async def connect(self) -> None:
        """Establish connection."""
        self.connected = True

    async def disconnect(self) -> None:
        """Close connection."""
        self.disconnected = True

    async def get_ticker(self, symbol: str) -> TickerSnapshot:
        """Get ticker snapshot for symbol."""
        return TickerSnapshot(
            symbol=symbol,
            bid=100.0,
            ask=101.0,
            last=100.5,
            timestamp=datetime.now(UTC).timestamp(),
        )

    async def get_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1m",
        since: int | None = None,
        limit: int | None = None,
    ) -> list[OhlcvSnapshot]:
        """Get OHLCV data for symbol."""
        return []

    async def create_order(self, request: ExchangeOrderRequest) -> ExchangeOrderSnapshot:
        """Create order from request."""
        return ExchangeOrderSnapshot(
            id="test_order_123",
            client_order_id=request.client_order_id,
            symbol=request.symbol,
            side=request.side,
            type=request.type,
            amount=request.amount,
            price=request.price,
            filled=0.0,
            remaining=0.0,
            status=OrderStatusEnum.OPEN,
            timestamp=datetime.now(UTC).timestamp(),
        )

    async def cancel_order(self, order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        """Cancel order by ID."""
        return ExchangeOrderSnapshot(
            id=order_id,
            client_order_id="",
            symbol=symbol or "BTC-USD",
            side=OrderSideEnum.BUY,
            type=OrderTypeEnum.LIMIT,
            amount=1.0,
            price=50000.0,
            filled=0.0,
            remaining=0.0,
            status=OrderStatusEnum.CANCELED,
            timestamp=datetime.now(UTC).timestamp(),
        )

    async def get_order(self, order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        """Get order by ID."""
        return ExchangeOrderSnapshot(
            id=order_id,
            client_order_id="",
            symbol=symbol or "BTC-USD",
            side=OrderSideEnum.BUY,
            type=OrderTypeEnum.LIMIT,
            amount=1.0,
            price=50000.0,
            filled=0.5,
            remaining=0.0,
            status=OrderStatusEnum.PARTIALLY_FILLED,
            timestamp=datetime.now(UTC).timestamp(),
        )

    async def get_orders(
        self,
        symbol: str | None = None,
        status: OrderStatusEnum | None = None,
        limit: int | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        """Get list of orders."""
        return []

    async def get_balance(self, currency: str | None = None) -> dict[str, AccountBalance]:
        """Get account balances."""
        return {}

    async def subscribe_ticks(self, symbols: list[str]) -> AsyncIterator[TickerUpdate]:
        """Subscribe to ticker updates."""
        yield TickerUpdate(
            symbol=symbols[0],
            bid=100.0,
            bid_qty=1.0,
            ask=101.0,
            ask_qty=1.0,
            last=100.5,
            volume=1000.0,
            vwap=100.3,
            low=99.0,
            high=102.0,
            change=0.5,
            change_pct=0.5,
        )

    async def subscribe_candles(
        self,
        symbols: list[str],
        timeframe: str = "1m",
    ) -> AsyncIterator[CandleUpdate]:
        """Subscribe to candle updates."""
        yield CandleUpdate(
            symbol=symbols[0],
            open=100.0,
            high=105.0,
            low=95.0,
            close=102.0,
            vwap=100.0,
            trades=100,
            volume=1000.0,
            interval_begin=datetime.now(UTC),
            interval=1,
        )

    async def subscribe_trades(self, symbols: list[str]) -> AsyncIterator[TradeUpdate]:
        """Subscribe to trade updates."""
        yield TradeUpdate(
            symbol=symbols[0],
            side="buy",
            quantity=1.0,
            price=100.0,
            ord_type="market",
            trade_id=12345,
            timestamp=datetime.now(UTC),
        )

    async def subscribe_executions(self) -> AsyncIterator[ExecutionUpdate]:
        """Subscribe to execution updates."""
        yield ExecutionUpdate(
            order_id="test_order",
            exec_type="trade",
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            order_type=OrderTypeEnum.LIMIT,
            order_status=OrderStatusEnum.FILLED,
            timestamp=datetime.now(UTC),
        )

    async def subscribe_instruments(self, **kwargs: Any) -> AsyncIterator[dict[str, Any]]:
        """Subscribe to instrument updates."""
        yield {"symbol": "BTC-USD", "base": "BTC", "quote": "USD"}


@pytest.mark.asyncio()
async def test_context_manager_calls_connect_and_disconnect() -> None:
    """Context manager handles connection lifecycle.

    Given: DummyExchangeClient instance,
    When: Used as async context manager,
    Then: connect called on enter, disconnect on exit.
    """
    client = DummyExchangeClient()
    assert not client.connected
    assert not client.disconnected
    async with client:
        assert client.connected
        assert not client.disconnected
    assert client.connected
    assert client.disconnected


@pytest.mark.asyncio()
async def test_log_order_to_db_returns_none_when_no_repository() -> None:
    """Log order returns None without repository.

    Given: Client without repository configured,
    When: _log_order_to_db is called,
    Then: Returns None without error.
    """
    client = DummyExchangeClient(repository=None)
    request = ExchangeOrderRequest(
        client_order_id="client_123",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        type=OrderTypeEnum.LIMIT,
        amount=1.0,
        price=50000.0,
    )
    order = ExchangeOrderSnapshot(
        id="order_123",
        client_order_id="client_123",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        type=OrderTypeEnum.LIMIT,
        amount=1.0,
        price=50000.0,
        filled=0.0,
        remaining=0.0,
        status=OrderStatusEnum.OPEN,
        timestamp=datetime.now(UTC).timestamp(),
    )
    result = await client._log_order_to_db(request, order)
    assert result is None


@pytest.mark.asyncio()
@patch(
    "snapper.infrastructure.exchanges.base.resolve_symbol_public_id",
    new_callable=AsyncMock,
    return_value="fake-spid",
)
async def test_log_order_to_db_logs_successfully(mock_resolve: AsyncMock) -> None:
    """Log order persists to database.

    Given: Client with mock repository,
    When: _log_order_to_db is called with order data,
    Then: Order is inserted and ID returned.
    """
    mock_repo = MagicMock(spec=Repository)
    mock_repo.upsert_instrument = AsyncMock(return_value=42)
    mock_repo.insert_order = AsyncMock(return_value=(99, "order-uuid-123"))
    client = DummyExchangeClient(repository=mock_repo)
    request = ExchangeOrderRequest(
        client_order_id="client_123",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        type=OrderTypeEnum.LIMIT,
        amount=1.0,
        price=50000.0,
    )
    order = ExchangeOrderSnapshot(
        id="order_123",
        client_order_id="client_123",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        type=OrderTypeEnum.LIMIT,
        amount=1.0,
        price=50000.0,
        filled=0.0,
        remaining=0.0,
        status=OrderStatusEnum.OPEN,
        timestamp=1234567890.0,
    )
    result = await client._log_order_to_db(request, order)
    assert result == (99, "order-uuid-123")
    mock_resolve.assert_awaited_once_with(mock_repo, "BTC-USD")
    mock_repo.upsert_instrument.assert_awaited_once_with(
        symbol_public_id="fake-spid",
        symbol="BTC-USD",
        exchange="dummy",
        base="BTC",
        quote="USD",
    )
    mock_repo.insert_order.assert_awaited_once()


@pytest.mark.asyncio()
@patch(
    "snapper.infrastructure.exchanges.base.resolve_symbol_public_id",
    new_callable=AsyncMock,
    return_value=None,
)
async def test_log_order_to_db_returns_none_when_symbol_not_resolved(
    mock_resolve: AsyncMock,
) -> None:
    """Log order returns None when no active Symbol row exists.

    Given: Client with repository configured,
    When: _log_order_to_db is called and symbol resolution returns None,
    Then: No instrument or order write is attempted.
    """
    mock_repo = MagicMock(spec=Repository)
    mock_repo.upsert_instrument = AsyncMock(return_value=42)
    mock_repo.insert_order = AsyncMock(return_value=(99, "order-uuid-123"))
    client = DummyExchangeClient(repository=mock_repo)
    request = ExchangeOrderRequest(
        client_order_id="client_123",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        type=OrderTypeEnum.LIMIT,
        amount=1.0,
        price=50000.0,
    )
    order = ExchangeOrderSnapshot(
        id="order_123",
        client_order_id="client_123",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        type=OrderTypeEnum.LIMIT,
        amount=1.0,
        price=50000.0,
        filled=0.0,
        remaining=0.0,
        status=OrderStatusEnum.OPEN,
        timestamp=1234567890.0,
    )
    result = await client._log_order_to_db(request, order)
    assert result is None
    mock_resolve.assert_awaited_once_with(mock_repo, "BTC-USD")
    mock_repo.upsert_instrument.assert_not_awaited()
    mock_repo.insert_order.assert_not_awaited()


@pytest.mark.asyncio()
@patch(
    "snapper.infrastructure.exchanges.base.resolve_symbol_public_id",
    new_callable=AsyncMock,
    return_value="fake-spid",
)
async def test_log_order_to_db_parses_symbol_without_delimiter(_mock_resolve: AsyncMock) -> None:
    """Log order parses base/quote from symbol without delimiter.

    Given: Client with mock repository and symbol without dash,
    When: _log_order_to_db is called,
    Then: base equals the full symbol and quote defaults to USD.
    """
    mock_repo = MagicMock(spec=Repository)
    mock_repo.upsert_instrument = AsyncMock(return_value=42)
    mock_repo.insert_order = AsyncMock(return_value=(99, "order-uuid-123"))
    client = DummyExchangeClient(repository=mock_repo)
    request = ExchangeOrderRequest(
        client_order_id="client_456",
        symbol="BTCUSD",
        side=OrderSideEnum.BUY,
        type=OrderTypeEnum.LIMIT,
        amount=1.0,
        price=50000.0,
    )
    order = ExchangeOrderSnapshot(
        id="order_456",
        client_order_id="client_456",
        symbol="BTCUSD",
        side=OrderSideEnum.BUY,
        type=OrderTypeEnum.LIMIT,
        amount=1.0,
        price=50000.0,
        filled=0.0,
        remaining=0.0,
        status=OrderStatusEnum.OPEN,
        timestamp=1234567890.0,
    )
    result = await client._log_order_to_db(request, order)
    assert result == (99, "order-uuid-123")
    _mock_resolve.assert_awaited_once_with(mock_repo, "BTCUSD")
    mock_repo.upsert_instrument.assert_awaited_once_with(
        symbol_public_id="fake-spid",
        symbol="BTCUSD",
        exchange="dummy",
        base="BTCUSD",
        quote="USD",
    )


@pytest.mark.asyncio()
@patch(
    "snapper.infrastructure.exchanges.base.resolve_symbol_public_id",
    new_callable=AsyncMock,
    return_value="fake-spid",
)
async def test_log_order_to_db_handles_exception(_mock_resolve: AsyncMock) -> None:
    """Log order handles database errors gracefully.

    Given: Client with repository that raises exception,
    When: _log_order_to_db is called,
    Then: Returns None without propagating error.
    """
    mock_repo = MagicMock(spec=Repository)
    mock_repo.upsert_instrument = AsyncMock(side_effect=SQLAlchemyError("Database error"))
    client = DummyExchangeClient(repository=mock_repo)
    request = ExchangeOrderRequest(
        client_order_id="client_123",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        type=OrderTypeEnum.LIMIT,
        amount=1.0,
        price=50000.0,
    )
    order = ExchangeOrderSnapshot(
        id="order_123",
        client_order_id="client_123",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        type=OrderTypeEnum.LIMIT,
        amount=1.0,
        price=50000.0,
        filled=0.0,
        remaining=0.0,
        status=OrderStatusEnum.OPEN,
        timestamp=datetime.now(UTC).timestamp(),
    )
    result = await client._log_order_to_db(request, order)
    assert result is None


@pytest.mark.asyncio()
async def test_log_order_update_to_db_returns_none_when_no_repository() -> None:
    """Log order update returns None without repository.

    Given: Client without repository configured,
    When: _log_order_update_to_db is called,
    Then: Returns None without error.
    """
    client = DummyExchangeClient(repository=None)
    result = await client._log_order_update_to_db(
        db_order_id=42,
        status=OrderStatusEnum.FILLED,
        exchange_order_id="order_123",
    )
    assert result is None


@pytest.mark.asyncio()
async def test_log_order_update_to_db_returns_new_order_id() -> None:
    """Log order update returns new order version id.

    Given: Client with mock repository returning new order id,
    When: _log_order_update_to_db is called with status,
    Then: Repository update_order is called and new id returned.
    """
    mock_repo = MagicMock(spec=Repository)
    mock_repo.update_order = AsyncMock(return_value=99)
    client = DummyExchangeClient(repository=mock_repo)
    result = await client._log_order_update_to_db(
        db_order_id=42,
        status=OrderStatusEnum.FILLED,
        exchange_order_id="order_123",
        error=None,
    )
    assert result == 99
    mock_repo.update_order.assert_called_once()
    call_args = mock_repo.update_order.call_args[1]
    assert call_args["order_id"] == 42
    assert call_args["status"] == "filled"
    assert call_args["exchange_order_id"] == "order_123"
    assert call_args["error"] is None


@pytest.mark.asyncio()
async def test_log_order_update_to_db_handles_exception() -> None:
    """Log order update handles database errors gracefully.

    Given: Client with repository that raises exception,
    When: _log_order_update_to_db is called,
    Then: Exception is suppressed and None returned.
    """
    mock_repo = MagicMock(spec=Repository)
    mock_repo.update_order = AsyncMock(side_effect=SQLAlchemyError("Database error"))
    client = DummyExchangeClient(repository=mock_repo)
    result = await client._log_order_update_to_db(
        db_order_id=42,
        status=OrderStatusEnum.FILLED,
    )
    assert result is None


@pytest.mark.asyncio()
async def test_log_execution_to_db_returns_early_when_no_repository() -> None:
    """Log execution returns early without repository.

    Given: Client without repository configured,
    When: _log_execution_to_db is called,
    Then: Returns immediately without error.
    """
    client = DummyExchangeClient(repository=None)
    execution = ExecutionUpdate(
        order_id="order_123",
        exec_type="trade",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        order_type=OrderTypeEnum.LIMIT,
        order_status=OrderStatusEnum.FILLED,
        timestamp=datetime.now(UTC),
        last_price=50000.0,
        last_qty=1.0,
    )
    await client._log_execution_to_db(
        db_order_id=42, order_public_id="order-pub-1", execution=execution
    )


@pytest.mark.asyncio()
async def test_log_execution_to_db_logs_successfully() -> None:
    """Log execution persists fill data.

    Given: Client with mock repository,
    When: _log_execution_to_db is called with execution,
    Then: Execution is inserted with price and fee.
    """
    mock_repo = MagicMock(spec=Repository)
    mock_repo.insert_execution = AsyncMock()
    client = DummyExchangeClient(repository=mock_repo)
    execution = ExecutionUpdate(
        order_id="order_123",
        exec_type="trade",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        order_type=OrderTypeEnum.LIMIT,
        order_status=OrderStatusEnum.FILLED,
        timestamp=datetime.now(UTC),
        exec_id="exec-123",
        trade_id=987654321,
        last_price=50000.0,
        last_qty=1.0,
        fee_usd_equiv=10.0,
    )
    await client._log_execution_to_db(
        db_order_id=42, order_public_id="order-pub-1", execution=execution
    )
    mock_repo.insert_execution.assert_called_once()
    call_args = mock_repo.insert_execution.call_args[1]
    assert call_args["order_id"] == 42
    assert call_args["order_public_id"] == "order-pub-1"
    assert call_args["price"] == pytest.approx(50000.0)
    assert call_args["size"] == pytest.approx(1.0)
    assert call_args["fee"] == pytest.approx(10.0)
    assert call_args["status"] == "filled"
    assert call_args["fee_asset"] == "USD"
    assert call_args["exec_id"] == "exec-123"
    assert call_args["trade_id"] == "987654321"


@pytest.mark.asyncio()
async def test_log_execution_to_db_uses_fallback_values() -> None:
    """Log execution uses fallback for missing fields.

    Given: Execution with None last_price and last_qty,
    When: _log_execution_to_db is called,
    Then: Uses average_price and cum_qty as fallbacks.
    """
    mock_repo = MagicMock(spec=Repository)
    mock_repo.insert_execution = AsyncMock()
    client = DummyExchangeClient(repository=mock_repo)
    execution = ExecutionUpdate(
        order_id="order_123",
        exec_type="trade",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        order_type=OrderTypeEnum.LIMIT,
        order_status=OrderStatusEnum.FILLED,
        timestamp=datetime.now(UTC),
        last_price=None,
        average_price=49500.0,
        last_qty=None,
        cum_qty=2.5,
        fee_usd_equiv=None,
    )
    await client._log_execution_to_db(
        db_order_id=42, order_public_id="order-pub-2", execution=execution
    )
    mock_repo.insert_execution.assert_called_once()
    call_args = mock_repo.insert_execution.call_args[1]
    assert call_args["status"] == "filled"
    assert call_args["price"] == pytest.approx(49500.0)
    assert call_args["size"] == pytest.approx(2.5)
    assert call_args["fee"] == pytest.approx(0.0)


@pytest.mark.asyncio()
async def test_log_execution_to_db_partial_fill_status() -> None:
    """Log execution stores partial status for open orders with fills.

    Given: Execution with OPEN status and positive cum_qty,
    When: _log_execution_to_db is called,
    Then: Status is stored as 'partial' (not raw order_status value).
    """
    mock_repo = MagicMock(spec=Repository)
    mock_repo.insert_execution = AsyncMock()
    client = DummyExchangeClient(repository=mock_repo)
    execution = ExecutionUpdate(
        order_id="order_456",
        exec_type="trade",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        order_type=OrderTypeEnum.LIMIT,
        order_status=OrderStatusEnum.OPEN,
        timestamp=datetime.now(UTC),
        last_price=50000.0,
        last_qty=0.5,
        cum_qty=0.5,
        fee_usd_equiv=5.0,
    )
    await client._log_execution_to_db(
        db_order_id=99, order_public_id="order-pub-3", execution=execution
    )
    call_args = mock_repo.insert_execution.call_args[1]
    assert call_args["status"] == "partial"


@pytest.mark.asyncio()
async def test_log_execution_to_db_handles_exception() -> None:
    """Log execution handles database errors gracefully.

    Given: Client with repository that raises exception,
    When: _log_execution_to_db is called,
    Then: Exception is suppressed.
    """
    mock_repo = MagicMock(spec=Repository)
    mock_repo.insert_execution = AsyncMock(side_effect=SQLAlchemyError("Database error"))
    client = DummyExchangeClient(repository=mock_repo)
    execution = ExecutionUpdate(
        order_id="order_123",
        exec_type="trade",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        order_type=OrderTypeEnum.LIMIT,
        order_status=OrderStatusEnum.FILLED,
        timestamp=datetime.now(UTC),
    )
    await client._log_execution_to_db(
        db_order_id=42, order_public_id="order-pub-4", execution=execution
    )
