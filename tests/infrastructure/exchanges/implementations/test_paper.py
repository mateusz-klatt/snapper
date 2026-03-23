"""Tests for paper trading exchange client."""

import asyncio
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from types import SimpleNamespace
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import patch

import pytest

from snapper.data.repository import Repository
from snapper.infrastructure.exchanges.contracts import AccountBalance
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import ExchangeOrderRequest
from snapper.infrastructure.exchanges.contracts import ExchangeOrderSnapshot
from snapper.infrastructure.exchanges.contracts import ExecutionUpdate
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.contracts import OrderStatusEnum
from snapper.infrastructure.exchanges.contracts import OrderTypeEnum
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.implementations.paper import PaperExchangeClient


class DummyRepo(SimpleNamespace):
    """Stub repository for paper exchange tests."""

    async def upsert_instrument(self, **kwargs: Any) -> tuple[int, str]:
        """Insert or update instrument record."""
        return (1, "inst-pub-1")

    async def upsert_candles(self, rows: list[Any]) -> int:
        """Insert or update candle records."""
        return len(rows)

    async def get_market_snapshots(self, *args: Any, **kwargs: Any) -> list[Any]:
        """Retrieve market snapshots."""
        return []

    async def get_candles(self, *args: Any, **kwargs: Any) -> list[Any]:
        """Retrieve candle records."""
        return []

    async def get_trades(self, *args: Any, **kwargs: Any) -> list[Any]:
        """Retrieve trade records."""
        return []


@pytest.mark.asyncio
async def test_connect_twice_warns(caplog: pytest.LogCaptureFixture) -> None:
    """Verify warning when connecting twice.

    Given: A paper exchange client,
    When: connect() is called twice,
    Then: Client remains running without error.
    """
    client = PaperExchangeClient()
    await client.connect()
    await client.connect()
    assert client._running


@pytest.mark.asyncio
async def test_disconnect_without_connect_is_noop() -> None:
    """Verify disconnect without prior connect is safe.

    Given: A paper exchange client not connected,
    When: disconnect() is called,
    Then: No error occurs.
    """
    client = PaperExchangeClient()
    await client.disconnect()


@pytest.mark.asyncio
async def test_create_order_requires_running() -> None:
    """Verify create_order requires client running.

    Given: A paper exchange client not connected,
    When: create_order() is called,
    Then: RuntimeError is raised.
    """
    client = PaperExchangeClient()
    with pytest.raises(RuntimeError):
        await client.create_order(
            ExchangeOrderRequest(
                symbol="BTC/USD",
                side=OrderSideEnum.BUY,
                type=OrderTypeEnum.MARKET,
                amount=1.0,
                price=None,
            )
        )


@pytest.mark.asyncio
async def test_simulate_fill_returns_when_not_running() -> None:
    """Verify simulate fill exits when client not running.

    Given: A paper exchange client not running,
    When: _simulate_fill() is called,
    Then: Method returns without error.
    """
    client = PaperExchangeClient()
    order = SimpleNamespace(id="o1")
    await client._simulate_fill(order)


def test_parse_interval_to_minutes_invalid() -> None:
    """Verify invalid interval parsing raises error.

    Given: A paper exchange client,
    When: _parse_interval_to_minutes() called with invalid format,
    Then: ValueError is raised.
    """
    client = PaperExchangeClient()
    with pytest.raises(ValueError):
        client._parse_interval_to_minutes("10x")


@pytest.mark.asyncio
async def test_subscribe_executions_requires_connection() -> None:
    """Verify subscribe_executions requires connection.

    Given: A paper exchange client not connected,
    When: subscribe_executions() is called,
    Then: RuntimeError is raised.
    """
    client = PaperExchangeClient()
    with pytest.raises(RuntimeError):
        async for _ in client.subscribe_executions():
            """Consumed by iteration to trigger exception."""
            pass


@pytest.mark.asyncio
async def test_get_balance_requires_connection() -> None:
    """Verify get_balance requires connection.

    Given: A paper exchange client not connected,
    When: get_balance() is called,
    Then: RuntimeError is raised.
    """
    client = PaperExchangeClient()
    with pytest.raises(RuntimeError):
        await client.get_balance()


@pytest.mark.asyncio
async def test_subscribe_ticker_requires_repository() -> None:
    """Verify subscribe_ticker requires repository.

    Given: A paper exchange client running without repository,
    When: subscribe_ticker() is called,
    Then: RuntimeError is raised.
    """
    client = PaperExchangeClient()
    client._running = True
    client.start_time = 0
    client.end_time = 1
    with pytest.raises(RuntimeError):
        async for _ in client.subscribe_ticker(["BTC/USD"]):
            """Consumed by iteration to trigger exception."""
            pass


@pytest.mark.asyncio
async def test_subscribe_ticker_requires_running_and_timerange() -> None:
    """Verify subscribe_ticker requires time range.

    Given: A paper client running without time range,
    When: subscribe_ticker() is called,
    Then: ValueError is raised.
    """
    client: Any = PaperExchangeClient(repository=DummyRepo())
    client.start_time = None
    client.end_time = None
    client._running = True
    with pytest.raises(ValueError):
        async for _ in client.subscribe_ticker(["BTC/USD"]):
            """Consumed by iteration to trigger exception."""
            pass


@pytest.mark.asyncio
async def test_disconnect_cancels_fill_task() -> None:
    """Verify disconnect cancels fill simulator task.

    Given: A running client with fill simulator task,
    When: disconnect() is called,
    Then: Fill simulator task is cancelled and cleared.
    """
    client = PaperExchangeClient()
    client._running = True
    client._fill_simulator_task = asyncio.create_task(asyncio.sleep(0.5))
    await client.disconnect()
    assert client._fill_simulator_task is None


@pytest.mark.asyncio
async def test_simulate_fill_handles_queue_error() -> None:
    """Verify simulate fill handles queue errors gracefully.

    Given: A running client with an order and failing queue,
    When: _simulate_fill() encounters queue error,
    Then: Error is handled without crashing.
    """
    client = PaperExchangeClient(fill_delay=0)
    client._running = True
    order = ExchangeOrderSnapshot(
        id="order-1",
        client_order_id=None,
        symbol="BTC/USD",
        side=OrderSideEnum.BUY,
        type=OrderTypeEnum.MARKET,
        amount=1.0,
        price=10.0,
        status=OrderStatusEnum.OPEN,
        filled=0.0,
        remaining=1.0,
        timestamp=0.0,
        fee=None,
    )
    client._orders[order.id] = order
    client._execution_queue.put = AsyncMock(side_effect=RuntimeError("boom"))
    await client._simulate_fill(order)


@pytest.mark.asyncio
async def test_cancel_order_logs_db_update() -> None:
    """Verify cancel_order logs database update.

    Given: A running client with an order having db_order_id,
    When: cancel_order() is called,
    Then: _log_order_update_to_db is called.
    """
    client = PaperExchangeClient()
    client._running = True
    order = ExchangeOrderSnapshot(
        id="order-2",
        client_order_id=None,
        symbol="BTC/USD",
        side=OrderSideEnum.BUY,
        type=OrderTypeEnum.LIMIT,
        amount=1.0,
        price=10.0,
        status=OrderStatusEnum.OPEN,
        filled=0.0,
        remaining=1.0,
        timestamp=0.0,
        fee=None,
    )
    order.db_order_id = 42
    client._orders[order.id] = order
    log_update = AsyncMock()
    client._log_order_update_to_db = log_update
    await client.cancel_order(order.id, symbol=order.symbol)
    log_update.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_order_requires_connection() -> None:
    """Verify get_order requires connection.

    Given: A paper exchange client not connected,
    When: get_order() is called,
    Then: RuntimeError is raised.
    """
    client = PaperExchangeClient()
    with pytest.raises(RuntimeError, match="not connected"):
        await client.get_order("order-3")


@pytest.mark.asyncio
async def test_get_orders_requires_connection() -> None:
    """Verify get_orders requires connection.

    Given: A paper exchange client not connected,
    When: get_orders() is called,
    Then: RuntimeError is raised.
    """
    client = PaperExchangeClient()
    with pytest.raises(RuntimeError, match="not connected"):
        await client.get_orders()


@pytest.mark.asyncio
async def test_subscribe_executions_timeout_exits(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify subscribe_executions exits on timeout.

    Given: A connected client with mocked timeout,
    When: subscribe_executions() times out,
    Then: Generator exits gracefully.
    """
    client = PaperExchangeClient()
    await client.connect()

    async def immediate_timeout(*args: Any, **kwargs: Any) -> None:
        coro = args[0]
        if hasattr(coro, "close"):
            coro.close()
        client._running = False
        raise TimeoutError

    sleep_mock = AsyncMock()
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.paper.asyncio.wait_for",
        immediate_timeout,
    )
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.paper.asyncio.sleep",
        sleep_mock,
    )
    async for _ in client.subscribe_executions():
        """Consumed by iteration to trigger exception."""
        pass


@pytest.mark.asyncio
async def test_subscribe_executions_handles_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify subscribe_executions handles errors gracefully.

    Given: A connected client with mocked error,
    When: subscribe_executions() encounters error,
    Then: Generator exits without raising.
    """
    client = PaperExchangeClient()
    await client.connect()

    async def boom(*args: Any, **kwargs: Any) -> None:
        coro = args[0]
        if hasattr(coro, "close"):
            coro.close()
        raise RuntimeError("boom")

    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.paper.asyncio.wait_for", boom
    )
    async for _ in client.subscribe_executions():
        """Consumed by iteration to trigger exception."""
        pass


@pytest.mark.asyncio
async def test_subscribe_ticker_requires_running() -> None:
    """Verify subscribe_ticker requires client running.

    Given: A paper client with repository but not running,
    When: subscribe_ticker() is called,
    Then: RuntimeError is raised.
    """
    client = PaperExchangeClient(repository=DummyRepo())
    client.start_time = 0
    client.end_time = 1
    with pytest.raises(RuntimeError, match="Not connected"):
        async for _ in client.subscribe_ticker(["BTC/USD"]):
            """Consumed by iteration to trigger exception."""
            pass


@pytest.mark.asyncio
async def test_subscribe_candles_requires_repository() -> None:
    """Verify subscribe_candles requires repository.

    Given: A running paper client without repository,
    When: subscribe_candles() is called,
    Then: RuntimeError is raised.
    """
    client = PaperExchangeClient()
    client._running = True
    client.start_time = 0
    client.end_time = 1
    with pytest.raises(RuntimeError, match="Repository required"):
        async for _ in client.subscribe_candles(["BTC/USD"]):
            """Consumed by iteration to trigger exception."""
            pass


@pytest.mark.asyncio
async def test_subscribe_candles_requires_running() -> None:
    """Verify subscribe_candles requires client running.

    Given: A paper client with repository but not running,
    When: subscribe_candles() is called,
    Then: RuntimeError is raised.
    """
    client = PaperExchangeClient(repository=DummyRepo())
    client.start_time = 0
    client.end_time = 1
    with pytest.raises(RuntimeError, match="Not connected"):
        async for _ in client.subscribe_candles(["BTC/USD"]):
            """Consumed by iteration to trigger exception."""
            pass


@pytest.mark.asyncio
async def test_subscribe_candles_requires_time_range() -> None:
    """Verify subscribe_candles requires time range.

    Given: A running paper client without time range,
    When: subscribe_candles() is called,
    Then: ValueError is raised.
    """
    client = PaperExchangeClient(repository=DummyRepo())
    client._running = True
    with pytest.raises(ValueError, match="Time range"):
        async for _ in client.subscribe_candles(["BTC/USD"]):
            """Consumed by iteration to trigger exception."""
            pass


@pytest.mark.asyncio
async def test_subscribe_trades_requires_repository() -> None:
    """Verify subscribe_trades requires repository.

    Given: A running paper client without repository,
    When: subscribe_trades() is called,
    Then: RuntimeError is raised.
    """
    client = PaperExchangeClient()
    client._running = True
    client.start_time = 0
    client.end_time = 1
    with pytest.raises(RuntimeError, match="Repository required"):
        async for _ in client.subscribe_trades(["BTC/USD"]):
            """Consumed by iteration to trigger exception."""
            pass


@pytest.mark.asyncio
async def test_subscribe_trades_requires_running() -> None:
    """Verify subscribe_trades requires client running.

    Given: A paper client with repository but not running,
    When: subscribe_trades() is called,
    Then: RuntimeError is raised.
    """
    client = PaperExchangeClient(repository=DummyRepo())
    client.start_time = 0
    client.end_time = 1
    with pytest.raises(RuntimeError, match="Not connected"):
        async for _ in client.subscribe_trades(["BTC/USD"]):
            """Consumed by iteration to trigger exception."""
            pass


@pytest.mark.asyncio
async def test_subscribe_trades_requires_time_range() -> None:
    """Verify subscribe_trades requires time range.

    Given: A running paper client without time range,
    When: subscribe_trades() is called,
    Then: ValueError is raised.
    """
    client = PaperExchangeClient(repository=DummyRepo())
    client._running = True
    with pytest.raises(ValueError, match="Time range"):
        async for _item in client.subscribe_trades(["BTC/USD"]):
            """Consumed by iteration to trigger exception."""


@pytest.mark.asyncio
async def test_subscribe_instruments_noop() -> None:
    """Verify subscribe_instruments returns empty generator.

    Given: A paper exchange client,
    When: subscribe_instruments() is called,
    Then: Empty list is yielded.
    """
    client = PaperExchangeClient()
    instruments: list[Any] = []
    async for item in client.subscribe_instruments():
        instruments.append(item)
    assert instruments == []


@pytest.fixture
def paper_client() -> PaperExchangeClient:
    """Provide a configured PaperExchangeClient instance for testing."""
    return PaperExchangeClient(
        repository=None,
        fill_delay=0.01,
        initial_balance=10000.0,
    )


class TestPaperConnectionHandling:
    """Tests for paper exchange connection handling."""

    @pytest.mark.asyncio
    async def test_connect_twice_logs_warning(self, paper_client: PaperExchangeClient) -> None:
        """Verify double connect keeps client running.

        Given: A paper exchange client,
        When: connect() is called twice,
        Then: Client remains running.
        """
        await paper_client.connect()
        await paper_client.connect()
        assert paper_client._running is True


class TestPaperOrderValidation:
    """Tests for paper exchange order validation."""

    @pytest.mark.asyncio
    async def test_create_order_requires_connection(
        self, paper_client: PaperExchangeClient
    ) -> None:
        """Verify create_order requires connection.

        Given: A paper exchange client not connected,
        When: create_order() is called,
        Then: RuntimeError is raised.
        """
        request = ExchangeOrderRequest(
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            type=OrderTypeEnum.LIMIT,
            amount=0.1,
            price=50000.0,
        )
        with pytest.raises(RuntimeError, match="not connected"):
            await paper_client.create_order(request)


class TestPaperCancelOrder:
    """Tests for paper exchange order cancellation."""

    @pytest.mark.asyncio
    async def test_cancel_order_requires_connection(
        self, paper_client: PaperExchangeClient
    ) -> None:
        """Verify cancel_order requires connection.

        Given: A paper exchange client not connected,
        When: cancel_order() is called,
        Then: RuntimeError is raised.
        """
        with pytest.raises(RuntimeError, match="not connected"):
            await paper_client.cancel_order("ORDER123", symbol="BTC-USD")

    @pytest.mark.asyncio
    async def test_cancel_nonexistent_order_returns_placeholder(
        self, paper_client: PaperExchangeClient
    ) -> None:
        """Verify canceling nonexistent order returns placeholder.

        Given: A connected paper exchange client,
        When: cancel_order() is called for unknown order,
        Then: Placeholder order with CANCELED status is returned.
        """
        await paper_client.connect()
        result = await paper_client.cancel_order("UNKNOWN123", symbol="BTC-USD")
        assert result.id == "UNKNOWN123"
        assert result.symbol == "BTC-USD"
        assert result.status == OrderStatusEnum.CANCELED

    @pytest.mark.asyncio
    async def test_cancel_existing_order_updates_status(
        self, paper_client: PaperExchangeClient
    ) -> None:
        """Verify canceling existing order updates status.

        Given: A connected client with an open order,
        When: cancel_order() is called,
        Then: Order status changes to CANCELED.
        """
        await paper_client.connect()
        request = ExchangeOrderRequest(
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            type=OrderTypeEnum.LIMIT,
            amount=0.1,
            price=50000.0,
        )
        order = await paper_client.create_order(request)
        canceled = await paper_client.cancel_order(order.id, symbol="BTC-USD")
        assert canceled.status == OrderStatusEnum.CANCELED
        assert canceled.id == order.id


class TestPaperBalanceManagement:
    """Tests for paper exchange balance management."""

    @pytest.mark.asyncio
    async def test_get_balance_requires_connection(self, paper_client: PaperExchangeClient) -> None:
        """Verify get_balance requires connection.

        Given: A paper exchange client not connected,
        When: get_balance() is called,
        Then: RuntimeError is raised.
        """
        with pytest.raises(RuntimeError, match="not connected"):
            await paper_client.get_balance()

    @pytest.mark.asyncio
    async def test_get_balance_returns_all_currencies(
        self, paper_client: PaperExchangeClient
    ) -> None:
        """Verify get_balance returns all configured currencies.

        Given: A connected paper exchange client,
        When: get_balance() is called,
        Then: All currencies with initial balance are returned.
        """
        await paper_client.connect()
        balances = await paper_client.get_balance()
        assert len(balances) == 5
        assert "USD" in balances
        assert "BTC" in balances
        assert balances["USD"].total == pytest.approx(10000.0)


class TestPaperDisconnection:
    """Tests for paper exchange disconnection."""

    @pytest.mark.asyncio
    async def test_disconnect_without_connection(self, paper_client: PaperExchangeClient) -> None:
        """Verify disconnect without connection is safe.

        Given: A paper exchange client not connected,
        When: disconnect() is called,
        Then: Client remains not running.
        """
        await paper_client.disconnect()
        assert not paper_client._running

    @pytest.mark.asyncio
    async def test_disconnect_cancels_fill_simulator(
        self, paper_client: PaperExchangeClient
    ) -> None:
        """Verify disconnect cancels fill simulator task.

        Given: A connected client with running fill simulator,
        When: disconnect() is called,
        Then: Fill simulator task is cancelled and cleared.
        """
        await paper_client.connect()
        paper_client._fill_simulator_task = asyncio.create_task(asyncio.sleep(10))
        await paper_client.disconnect()
        assert paper_client._fill_simulator_task is None
        assert not paper_client._running


class _ReplayRepo:
    """Test repository for paper trading replay data."""

    def __init__(self) -> None:
        now = datetime.now(tz=UTC)
        self.snapshots = [
            {
                "symbol": "BTC/USD",
                "bid": 10.0,
                "ask": 11.0,
                "last": 10.5,
                "timestamp": now,
                "bid_volume": 1.0,
                "ask_volume": 2.0,
                "volume": 3.0,
                "vwap": 10.7,
                "low": 9.0,
                "high": 12.0,
            }
        ]
        self.candles = [
            {
                "open_at": now,
                "open": 1.0,
                "high": 2.0,
                "low": 0.5,
                "close": 1.5,
                "volume": 100.0,
                "vwap": 1.2,
                "trades": 10,
            }
        ]
        self.trades = [
            {
                "side": "buy",
                "size": 1.0,
                "price": 100.0,
                "timestamp": now,
                "trade_id": 42,
            }
        ]

    async def get_market_snapshots(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        return self.snapshots

    async def get_candles(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        return self.candles

    async def get_trades(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        return self.trades


@pytest.mark.asyncio
async def test_create_order_logs_and_executes_with_db_updates() -> None:
    """Verify create_order logs to database and executes.

    Given: A connected client with mocked DB logging,
    When: create_order() is called with limit order,
    Then: Order is created with db_order_id and execution logged.
    """
    client = PaperExchangeClient(repository=cast(Repository, _ReplayRepo()), fill_delay=0)
    with (
        patch.object(client, "_log_order_to_db", AsyncMock(return_value=(123, "order-pub-abc"))),
        patch.object(client, "_log_order_update_to_db", AsyncMock()) as log_update,
        patch.object(client, "_log_execution_to_db", AsyncMock()) as log_exec,
    ):
        await client.connect()
        request = ExchangeOrderRequest(
            symbol="BTC/USD",
            side=OrderSideEnum.BUY,
            type=OrderTypeEnum.LIMIT,
            amount=1.0,
            price=10.0,
        )
        order = await client.create_order(request)
        execution = await asyncio.wait_for(client._execution_queue.get(), timeout=0.5)
    assert order.db_order_id == 123
    assert order.db_order_public_id == "order-pub-abc"
    log_update.assert_awaited_once()
    log_exec.assert_awaited_once()
    assert execution.order_status == OrderStatusEnum.CLOSED


@pytest.mark.asyncio
async def test_get_order_known_and_unknown() -> None:
    """Verify get_order returns correct order or placeholder.

    Given: A connected client with one created order,
    When: get_order() is called with known and unknown IDs,
    Then: Known order is returned, unknown gets placeholder.
    """
    client = PaperExchangeClient()
    await client.connect()
    order = await client.create_order(
        ExchangeOrderRequest(
            symbol="BTC/USD",
            side=OrderSideEnum.BUY,
            type=OrderTypeEnum.LIMIT,
            amount=1,
            price=10,
        )
    )
    assert (await client.get_order(order.id)).id == order.id
    placeholder = await client.get_order("missing", symbol="ETH/USD")
    assert placeholder.status == OrderStatusEnum.OPEN
    assert placeholder.symbol == "ETH/USD"


@pytest.mark.asyncio
async def test_get_orders_filters_and_limit() -> None:
    """Verify get_orders applies filters and limit.

    Given: A connected client with multiple orders,
    When: get_orders() is called with filters,
    Then: Filtered orders are returned respecting limit.
    """
    client = PaperExchangeClient()
    await client.connect()
    order1 = await client.create_order(
        ExchangeOrderRequest(
            symbol="BTC/USD", side=OrderSideEnum.BUY, type=OrderTypeEnum.LIMIT, amount=1, price=10
        )
    )
    order2 = await client.create_order(
        ExchangeOrderRequest(
            symbol="ETH/USD", side=OrderSideEnum.SELL, type=OrderTypeEnum.LIMIT, amount=2, price=20
        )
    )
    await client.cancel_order(order2.id, symbol="ETH/USD")
    by_symbol = await client.get_orders(symbol="BTC/USD")
    assert by_symbol == [order1]
    canceled = await client.get_orders(status=OrderStatusEnum.CANCELED)
    assert canceled == [order2]
    limited = await client.get_orders(limit=1)
    assert len(limited) == 1


@pytest.mark.asyncio
async def test_get_balance_unknown_currency_zero() -> None:
    """Verify get_balance returns zero for unknown currency.

    Given: A connected client,
    When: get_balance() is called for unknown currency,
    Then: Zero balance is returned.
    """
    client = PaperExchangeClient()
    await client.connect()
    balances = await client.get_balance("CHF")
    assert balances["CHF"] == AccountBalance(currency="CHF", free=0.0, used=0.0, total=0.0)


@pytest.mark.asyncio
async def test_subscribe_executions_yields_queue_items() -> None:
    """Verify subscribe_executions yields from queue.

    Given: A connected client with execution in queue,
    When: subscribe_executions() is iterated,
    Then: ExecutionUpdate is yielded.
    """
    client = PaperExchangeClient()
    await client.connect()
    update = ExecutionUpdate(
        order_id="oid",
        exec_type="trade",
        symbol="BTC/USD",
        side=OrderSideEnum.BUY,
        order_type=OrderTypeEnum.MARKET,
        order_status=OrderStatusEnum.CLOSED,
        timestamp=datetime.now(tz=UTC),
        order_qty=1.0,
        cum_qty=1.0,
        last_qty=1.0,
        average_price=10.0,
        last_price=10.0,
        fee_usd_equiv=0.0,
    )
    await client._execution_queue.put(update)
    async for item in client.subscribe_executions():
        assert item.order_id == "oid"
        client._running = False
        break


@pytest.mark.asyncio
async def test_get_ticker_success_and_empty() -> None:
    """Verify get_ticker returns snapshot or raises ValueError.

    Given: A connected client with repository,
    When: get_ticker() is called with existing/empty snapshots,
    Then: Returns snapshot or raises ValueError.
    """
    repo = _ReplayRepo()
    client = PaperExchangeClient(repository=cast(Repository, repo), source_exchange="kraken")
    await client.connect()
    snapshot = await client.get_ticker("BTC/USD")
    assert snapshot.bid == pytest.approx(10.0)
    repo.snapshots = []
    with pytest.raises(ValueError, match="No ticker data"):
        await client.get_ticker("BTC/USD")


@pytest.mark.asyncio
async def test_get_ticker_no_source_exchange_raises() -> None:
    """Verify get_ticker raises without source_exchange.

    Given: A connected client without source_exchange,
    When: get_ticker() is called,
    Then: ValueError is raised.
    """
    repo = _ReplayRepo()
    client = PaperExchangeClient(repository=cast(Repository, repo))
    await client.connect()
    with pytest.raises(ValueError, match="source_exchange required"):
        await client.get_ticker("BTC/USD")


@pytest.mark.asyncio
async def test_get_ohlcv_no_source_exchange_raises() -> None:
    """Verify get_ohlcv raises without source_exchange.

    Given: A connected client without source_exchange,
    When: get_ohlcv() is called,
    Then: ValueError is raised.
    """
    repo = _ReplayRepo()
    client = PaperExchangeClient(repository=cast(Repository, repo))
    await client.connect()
    with pytest.raises(ValueError, match="source_exchange required"):
        await client.get_ohlcv("BTC/USD", timeframe="1m")


@pytest.mark.asyncio
async def test_get_ohlcv_limits_results() -> None:
    """Verify get_ohlcv respects limit parameter.

    Given: A connected client with repository having candles,
    When: get_ohlcv() is called with limit,
    Then: Correct number of candles returned.
    """
    repo = _ReplayRepo()
    now = datetime.now(tz=UTC)
    repo.candles = [
        {"open_at": now, "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 100},
        {"open_at": now, "open": 2, "high": 3, "low": 1.5, "close": 2.5, "volume": 200},
        {"open_at": now, "open": 3, "high": 4, "low": 2.5, "close": 3.5, "volume": 300},
    ]
    client = PaperExchangeClient(repository=cast(Repository, repo), source_exchange="kraken")
    await client.connect()
    candles = await client.get_ohlcv("BTC/USD", timeframe="1m", limit=2)
    assert len(candles) == 2
    assert candles[-1].close == pytest.approx(3.5)


@pytest.mark.asyncio
async def test_subscribe_ticker_replays_snapshots() -> None:
    """Verify subscribe_ticker replays repository snapshots.

    Given: A running client with repository snapshots,
    When: subscribe_ticker() is iterated,
    Then: TickerUpdate from repository is yielded.
    """
    client = PaperExchangeClient(
        repository=cast(Repository, _ReplayRepo()), source_exchange="kraken"
    )
    client._running = True
    client.start_time = 0
    client.end_time = 1
    updates: list[TickerUpdate] = []
    async for tick in client.subscribe_ticker(["BTC/USD"]):
        updates.append(tick)
        break
    assert updates and updates[0].symbol == "BTC/USD"


@pytest.mark.asyncio
async def test_subscribe_candles_and_trades_replay() -> None:
    """Verify subscribe_candles and subscribe_trades replay data.

    Given: A running client with repository data,
    When: Subscriptions are iterated,
    Then: CandleUpdate and TradeUpdate are yielded.
    """
    repo = _ReplayRepo()
    client = PaperExchangeClient(repository=cast(Repository, repo), source_exchange="kraken")
    client._running = True
    client.start_time = 0
    client.end_time = 1
    candles = []
    async for candle in client.subscribe_candles(["BTC/USD"], timeframe="1h"):
        candles.append(candle)
        break
    trades = []
    async for trade in client.subscribe_trades(["BTC/USD"]):
        trades.append(trade)
        break
    assert candles and isinstance(candles[0], CandleUpdate)
    assert trades and trades[0].trade_id == 42


@pytest.mark.asyncio
async def test_subscribe_candles_returns_on_empty_symbols() -> None:
    """Verify subscribe_candles returns empty for no symbols.

    Given: A running client with empty symbols list,
    When: subscribe_candles() is called,
    Then: Empty list is returned.
    """
    client = PaperExchangeClient(
        repository=cast(Repository, _ReplayRepo()), source_exchange="kraken"
    )
    client._running = True
    client.start_time = 0
    client.end_time = 1
    candles: list[Any] = []
    async for c in client.subscribe_candles([]):
        candles.append(c)
    assert candles == []


@pytest.mark.asyncio
async def test_subscribe_candles_no_source_exchange_raises() -> None:
    """Verify subscribe_candles raises without source_exchange.

    Given: A running client without source_exchange set,
    When: subscribe_candles() is called,
    Then: ValueError is raised requiring source_exchange.
    """
    client = PaperExchangeClient(repository=cast(Repository, _ReplayRepo()))
    client._running = True
    client.start_time = 0
    client.end_time = 1
    with pytest.raises(ValueError, match="source_exchange required"):
        async for _ in client.subscribe_candles(["BTC/USD"]):
            pass


@pytest.mark.asyncio
async def test_subscribe_trades_no_source_exchange_raises() -> None:
    """Verify subscribe_trades raises without source_exchange.

    Given: A running client without source_exchange set,
    When: subscribe_trades() is called,
    Then: ValueError is raised requiring source_exchange.
    """
    client = PaperExchangeClient(repository=cast(Repository, _ReplayRepo()))
    client._running = True
    client.start_time = 0
    client.end_time = 1
    with pytest.raises(ValueError, match="source_exchange required"):
        async for _ in client.subscribe_trades(["BTC/USD"]):
            pass


@pytest.mark.asyncio
async def test_get_ohlcv_default_range_uses_last_24h() -> None:
    """Verify get_ohlcv uses 24h default range.

    Given: A connected client without time range specified,
    When: get_ohlcv() is called,
    Then: Default 24h range is used.
    """

    class _RangeRepo(_ReplayRepo):
        def __init__(self) -> None:
            super().__init__()
            self.range: tuple[datetime, datetime] | None = None

        async def get_candles(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
            self.range = (args[2], args[3])
            return []

    repo = _RangeRepo()
    client = PaperExchangeClient(repository=cast(Repository, repo), source_exchange="kraken")
    await client.connect()
    candles = await client.get_ohlcv("BTC/USD", timeframe="1m")
    assert candles == []
    assert repo.range is not None
    start_dt, end_dt = repo.range
    assert abs((end_dt - start_dt) - timedelta(hours=24)) < timedelta(minutes=1)


@pytest.mark.asyncio
async def test_subscribe_ticker_empty_snapshots() -> None:
    """Verify subscribe_ticker returns empty for no snapshots.

    Given: A running client with empty repository snapshots,
    When: subscribe_ticker() is iterated,
    Then: Empty list is returned.
    """
    repo = _ReplayRepo()
    repo.snapshots = []
    client = PaperExchangeClient(repository=cast(Repository, repo), source_exchange="kraken")
    client._running = True
    client.start_time = 0
    client.end_time = 1
    ticks: list[Any] = []
    async for tick in client.subscribe_ticker(["BTC/USD"]):
        ticks.append(tick)
    assert ticks == []


@pytest.mark.asyncio
async def test_subscribe_candles_empty_repo() -> None:
    """Verify subscribe_candles returns empty for no candles.

    Given: A running client with empty repository candles,
    When: subscribe_candles() is iterated,
    Then: Empty list is returned.
    """
    repo = _ReplayRepo()
    repo.candles = []
    client = PaperExchangeClient(repository=cast(Repository, repo), source_exchange="kraken")
    client._running = True
    client.start_time = 0
    client.end_time = 1
    candles: list[Any] = []
    async for candle in client.subscribe_candles(["BTC/USD"]):
        candles.append(candle)
    assert candles == []


@pytest.mark.asyncio
async def test_subscribe_trades_empty_symbols() -> None:
    """Verify subscribe_trades returns empty for no symbols.

    Given: A running client with empty symbols list,
    When: subscribe_trades() is iterated,
    Then: Empty list is returned.
    """
    client = PaperExchangeClient(
        repository=cast(Repository, _ReplayRepo()), source_exchange="kraken"
    )
    client._running = True
    client.start_time = 0
    client.end_time = 1
    trades: list[Any] = []
    async for trade in client.subscribe_trades([]):
        trades.append(trade)
    assert trades == []


@pytest.mark.asyncio
async def test_subscribe_trades_with_empty_result_for_symbol() -> None:
    """Verify subscribe_trades handles mixed symbol results.

    Given: A running client with trades for one symbol only,
    When: subscribe_trades() is called for multiple symbols,
    Then: Only trades for symbol with data are returned.
    """

    class _EmptyTradesRepo(_ReplayRepo):
        async def get_trades(self, symbol: str, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
            if symbol == "BTC/USD":
                return [
                    {
                        "side": "buy",
                        "size": 1.0,
                        "price": 50000.0,
                        "timestamp": datetime.fromtimestamp(0, tz=UTC),
                        "trade_id": 1,
                    }
                ]
            return []

    client = PaperExchangeClient(
        repository=cast(Repository, _EmptyTradesRepo()), source_exchange="kraken"
    )
    client._running = True
    client.start_time = 0
    client.end_time = 1
    trades: list[Any] = []
    async for trade in client.subscribe_trades(["BTC/USD", "ETH/USD"]):
        trades.append(trade)
    assert len(trades) == 1
    assert trades[0].symbol == "BTC/USD"


@pytest.mark.asyncio
async def test_subscribe_ticks_alias_replays_snapshots() -> None:
    """Verify subscribe_ticks is alias for subscribe_ticker.

    Given: A running client with repository snapshots,
    When: subscribe_ticks() is called,
    Then: TickerUpdate is yielded (same as subscribe_ticker).
    """
    client = PaperExchangeClient(
        repository=cast(Repository, _ReplayRepo()), source_exchange="kraken"
    )
    client._running = True
    client.start_time = 0
    client.end_time = 1
    updates: list[Any] = []
    async for tick in client.subscribe_ticks(["BTC/USD"]):
        updates.append(tick)
    assert updates and isinstance(updates[0], TickerUpdate)
