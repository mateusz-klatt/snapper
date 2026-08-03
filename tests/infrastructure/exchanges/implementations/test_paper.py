"""Tests for paper trading exchange client."""

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from types import SimpleNamespace
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import patch

import pytest

from snapper.core.types import ExchangeEnum
from snapper.data.repository import Repository
from snapper.infrastructure.exchanges.contracts import AccountBalance
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import CapabilityStatus
from snapper.infrastructure.exchanges.contracts import ExchangeOrderRequest
from snapper.infrastructure.exchanges.contracts import ExchangeOrderSnapshot
from snapper.infrastructure.exchanges.contracts import ExchangeOrderStatusEnum
from snapper.infrastructure.exchanges.contracts import ExchangeOrderTypeEnum
from snapper.infrastructure.exchanges.contracts import ExecutionUpdate
from snapper.infrastructure.exchanges.contracts import NativeBalanceEntry
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.implementations.paper import PaperExchangeClient


async def _fake_resolve(symbols: list[str], exchange: str) -> list[str]:
    """Return deterministic instrument_public_ids for testing."""
    return [f"inst-{s.lower().replace('/', '-')}" for s in symbols]


class _AsyncCtx:
    """Minimal async context manager wrapping a value."""

    def __init__(self, value: Any) -> None:
        self._value = value

    async def __aenter__(self) -> Any:
        return self._value

    async def __aexit__(self, *args: object) -> None:
        pass


class DummyRepo(SimpleNamespace):
    """Stub repository for paper exchange tests."""

    async def ensure_instrument(
        self,
        symbol_public_id: str,
        exchange: str,
        session_id: str,
        sequence_id: int,
        timestamp: datetime = datetime(2024, 1, 1, tzinfo=UTC),
    ) -> tuple[int, str]:
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
    market_buy_request = ExchangeOrderRequest(
        symbol="BTC/USD",
        side=OrderSideEnum.BUY,
        type=ExchangeOrderTypeEnum.MARKET,
        amount=1.0,
        client_order_id="coid-create-order-requires-running",
        price=None,
    )
    with pytest.raises(RuntimeError):
        await client.create_order(market_buy_request)


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
    task_a = asyncio.create_task(asyncio.sleep(0.5))
    task_b = asyncio.create_task(asyncio.sleep(0.5))
    client._fill_simulator_tasks.update({task_a, task_b})
    await client.disconnect()
    assert client._fill_simulator_tasks == set()
    assert task_a.cancelled()
    assert task_b.cancelled()


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
        type=ExchangeOrderTypeEnum.MARKET,
        amount=1.0,
        price=10.0,
        status=ExchangeOrderStatusEnum.OPEN,
        filled=0.0,
        remaining=1.0,
        timestamp=0.0,
        fee=None,
    )
    client._orders[order.id] = order
    client._execution_queue.put = AsyncMock(side_effect=RuntimeError("boom"))
    await client._simulate_fill(order)


def _market_order(price: float | None = None) -> ExchangeOrderSnapshot:
    """Build an OPEN market-order snapshot for fill-simulator tests."""
    return ExchangeOrderSnapshot(
        id="order-mkt-1",
        client_order_id=None,
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        type=ExchangeOrderTypeEnum.MARKET,
        amount=0.01,
        price=price,
        status=ExchangeOrderStatusEnum.OPEN,
        filled=0.0,
        remaining=0.01,
        timestamp=0.0,
        fee=None,
    )


@pytest.mark.asyncio
async def test_simulate_fill_market_order_uses_source_venue_close() -> None:
    """Verify a market order fills at the freshest source-venue 1m close.

    Given: A running client whose repository serves a kraken 1m candle,
    When: _simulate_fill() runs for a priceless MARKET order,
    Then: The trade execution carries the candle close as both average
        and last price and the order closes fully filled.
    """
    repo = AsyncMock()
    repo.get_candles = AsyncMock(return_value=[{"close": 64000.0, "open_at": datetime.now(UTC)}])
    client = PaperExchangeClient(repository=cast(Repository, repo), fill_delay=0)
    client._running = True
    order = _market_order()
    client._orders[order.id] = order
    await client._simulate_fill(order)
    execution = client._execution_queue.get_nowait()
    assert execution.exec_type == "trade"
    assert execution.average_price == 64000.0
    assert execution.last_price == 64000.0
    assert order.status == ExchangeOrderStatusEnum.CLOSED
    assert order.filled == 0.01
    assert repo.get_candles.await_args is not None
    assert repo.get_candles.await_args.args[4] == ExchangeEnum.KRAKEN


@pytest.mark.asyncio
async def test_simulate_fill_market_order_cancels_without_reference_price() -> None:
    """Verify an unpriceable market order is CANCELED, never zero-filled.

    Given: A running client whose repository has no candles on any
        source venue,
    When: _simulate_fill() runs for a priceless MARKET order,
    Then: A canceled execution is queued, the order is CANCELED with
        its full amount remaining, and no 0.0-price fill exists.
    """
    repo = AsyncMock()
    repo.get_candles = AsyncMock(return_value=[])
    client = PaperExchangeClient(repository=cast(Repository, repo), fill_delay=0)
    client._running = True
    order = _market_order()
    client._orders[order.id] = order
    await client._simulate_fill(order)
    execution = client._execution_queue.get_nowait()
    assert execution.exec_type == "canceled"
    assert execution.order_status == ExchangeOrderStatusEnum.CANCELED
    assert execution.cum_qty == 0.0
    assert order.status == ExchangeOrderStatusEnum.CANCELED
    assert order.remaining == 0.01


@pytest.mark.asyncio
async def test_simulate_fill_market_order_cancels_without_repository() -> None:
    """Verify a repository-less client cancels priceless market orders.

    Given: A running client constructed without a repository,
    When: _simulate_fill() runs for a priceless MARKET order,
    Then: The order is CANCELED (no reference price is inventable).
    """
    client = PaperExchangeClient(fill_delay=0)
    client._running = True
    order = _market_order()
    client._orders[order.id] = order
    await client._simulate_fill(order)
    assert order.status == ExchangeOrderStatusEnum.CANCELED


@pytest.mark.asyncio
async def test_simulate_fill_priced_order_fills_at_own_price() -> None:
    """Verify a priced order still fills at its own price without lookups.

    Given: A running client with NO repository and a priced order,
    When: _simulate_fill() runs,
    Then: The fill uses the order's own price (no reference lookup).
    """
    client = PaperExchangeClient(fill_delay=0)
    client._running = True
    order = _market_order(price=123.45)
    client._orders[order.id] = order
    await client._simulate_fill(order)
    execution = client._execution_queue.get_nowait()
    assert execution.exec_type == "trade"
    assert execution.average_price == 123.45
    assert order.status == ExchangeOrderStatusEnum.CLOSED


@pytest.mark.asyncio
async def test_resolve_market_fill_price_prefers_source_exchange() -> None:
    """Verify an explicit source_exchange short-circuits the venue list.

    Given: A client constructed with source_exchange=kraken_futures,
    When: _resolve_market_fill_price() runs,
    Then: Only that venue is queried.
    """
    repo = AsyncMock()
    repo.get_candles = AsyncMock(return_value=[{"close": 50.5, "open_at": datetime.now(UTC)}])
    client = PaperExchangeClient(
        repository=cast(Repository, repo), source_exchange=ExchangeEnum.KRAKEN_FUTURES
    )
    price = await client._resolve_market_fill_price("BTC-USD-PERP")
    assert price == 50.5
    repo.get_candles.assert_awaited_once()
    assert repo.get_candles.await_args is not None
    assert repo.get_candles.await_args.args[4] == ExchangeEnum.KRAKEN_FUTURES


@pytest.mark.asyncio
async def test_resolve_market_fill_price_rejects_stale_close() -> None:
    """Verify a stale close is rejected (halted-venue policy).

    Given: The only venue serving a candle older than the max-age cap,
    When: _resolve_market_fill_price() runs,
    Then: None is returned — a halted/closed venue must not price fills.
    """
    repo = AsyncMock()
    repo.get_candles = AsyncMock(
        return_value=[{"close": 100.0, "open_at": datetime.now(UTC) - timedelta(hours=3)}]
    )
    client = PaperExchangeClient(
        repository=cast(Repository, repo), source_exchange=ExchangeEnum.KRAKEN
    )
    assert await client._resolve_market_fill_price("BTC-USD") is None


@pytest.mark.asyncio
async def test_resolve_market_fill_price_rejects_non_positive_close() -> None:
    """Verify zero/non-finite closes count as per-venue misses.

    Given: The first venue serving a 0.0 close and the second a valid one,
    When: _resolve_market_fill_price() runs,
    Then: The valid second-venue close wins.
    """
    repo = AsyncMock()
    repo.get_candles = AsyncMock(
        side_effect=[
            [{"close": 0.0, "open_at": datetime.now(UTC)}],
            [{"close": 88.0, "open_at": datetime.now(UTC)}],
        ]
    )
    client = PaperExchangeClient(repository=cast(Repository, repo))
    assert await client._resolve_market_fill_price("BTC-USD") == 88.0


@pytest.mark.asyncio
async def test_simulate_fill_loses_race_to_cancel_during_lookup() -> None:
    """Verify a cancel racing the price lookup wins — no late fill.

    Given: A market order whose price lookup cancels the order before
        returning a valid price,
    When: _simulate_fill() resumes after the lookup,
    Then: No execution is queued and the order stays CANCELED.
    """
    repo = AsyncMock()
    client = PaperExchangeClient(repository=cast(Repository, repo), fill_delay=0)
    client._running = True
    order = _market_order()
    client._orders[order.id] = order

    async def _cancel_then_price(*args: object, **kwargs: object) -> list[dict[str, object]]:
        await client.cancel_order(order.id)
        return [{"close": 64000.0, "open_at": datetime.now(UTC)}]

    repo.get_candles = AsyncMock(side_effect=_cancel_then_price)
    await client._simulate_fill(order)
    assert order.status == ExchangeOrderStatusEnum.CANCELED
    assert client._execution_queue.empty()


@pytest.mark.asyncio
async def test_resolve_market_fill_price_rejects_future_dated_close() -> None:
    """Verify a future-dated bar is rejected (corrupt-data guard).

    Given: The only venue serving a candle whose open_at lies in the
        future (negative age),
    When: _resolve_market_fill_price() runs,
    Then: None is returned — age must be within [0, cap].
    """
    repo = AsyncMock()
    repo.get_candles = AsyncMock(
        return_value=[{"close": 100.0, "open_at": datetime.now(UTC) + timedelta(days=3)}]
    )
    client = PaperExchangeClient(
        repository=cast(Repository, repo), source_exchange=ExchangeEnum.KRAKEN
    )
    assert await client._resolve_market_fill_price("BTC-USD") is None


@pytest.mark.asyncio
async def test_resolve_market_fill_price_refused_in_replay_mode() -> None:
    """Verify replay-mode clients never wall-clock-price market fills.

    Given: A client constructed with a historical replay window,
    When: _resolve_market_fill_price() runs,
    Then: None is returned without any candle lookup — replay flows
        must submit priced orders (wall-clock pricing is temporally
        wrong for historical execution).
    """
    repo = AsyncMock()
    repo.get_candles = AsyncMock(return_value=[{"close": 100.0, "open_at": datetime.now(UTC)}])
    client = PaperExchangeClient(repository=cast(Repository, repo), start_time=0.0, end_time=1.0)
    assert await client._resolve_market_fill_price("BTC-USD") is None
    repo.get_candles.assert_not_awaited()


@pytest.mark.asyncio
async def test_disconnect_terminalizes_open_orders() -> None:
    """Verify disconnect cancels every still-OPEN tracked order.

    Given: A connected client with an OPEN order whose fill task has
        not yet been scheduled (the create/disconnect race window),
    When: disconnect() runs, then the client reconnects,
    Then: The order is CANCELED and stays CANCELED — it can never fill
        in a later session.
    """
    client = PaperExchangeClient()
    await client.connect()
    order = _market_order(price=10.0)
    client._orders[order.id] = order
    await client.disconnect()
    assert order.status == ExchangeOrderStatusEnum.CANCELED
    assert order.remaining == order.amount
    await client.connect()
    await client._simulate_fill(order)
    assert order.status == ExchangeOrderStatusEnum.CANCELED
    assert client._execution_queue.empty()


@pytest.mark.asyncio
async def test_create_order_cancels_when_disconnected_during_db_logging() -> None:
    """Verify a disconnect racing create_order's DB await cancels the order.

    Given: A client whose order-logging await disconnects the client,
    When: create_order() resumes after the await,
    Then: RuntimeError routes the submit into the executor's failure
        path, the tracked order is CANCELED, and no fill task exists.
    """
    client = PaperExchangeClient(fill_delay=0)
    await client.connect()

    async def _disconnect_during_log(*args: object, **kwargs: object) -> None:
        await client.disconnect()
        return None

    racing_order_request = ExchangeOrderRequest(
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        type=ExchangeOrderTypeEnum.MARKET,
        amount=0.01,
        client_order_id="coid-create-order-cancels-when-disconnected-during-db-logging",
    )
    with (
        patch.object(
            PaperExchangeClient, "_log_order_to_db", AsyncMock(side_effect=_disconnect_during_log)
        ),
        pytest.raises(RuntimeError, match="session ended"),
    ):
        await client.create_order(racing_order_request)
    tracked = next(iter(client._orders.values()))
    assert tracked.status == ExchangeOrderStatusEnum.CANCELED
    assert client._fill_simulator_tasks == set()


@pytest.mark.asyncio
async def test_cancel_order_refuses_to_rewrite_closed_order() -> None:
    """Verify a late cancel cannot turn a CLOSED fill into CANCELED.

    Given: A tracked order already CLOSED by the fill simulator,
    When: cancel_order() is called,
    Then: The snapshot is returned as-is, still CLOSED.
    """
    client = PaperExchangeClient()
    client._running = True
    order = _market_order(price=10.0)
    order.status = ExchangeOrderStatusEnum.CLOSED
    client._orders[order.id] = order
    snapshot = await client.cancel_order(order.id)
    assert snapshot.status == ExchangeOrderStatusEnum.CLOSED


@pytest.mark.asyncio
async def test_resolve_market_fill_price_skips_failing_venue() -> None:
    """Verify a failing venue lookup falls through to the next venue.

    Given: The first source venue raising and the second returning a
        candle,
    When: _resolve_market_fill_price() runs,
    Then: The second venue's close is returned.
    """
    repo = AsyncMock()
    repo.get_candles = AsyncMock(
        side_effect=[RuntimeError("venue down"), [{"close": 77.0, "open_at": datetime.now(UTC)}]]
    )
    client = PaperExchangeClient(repository=cast(Repository, repo))
    price = await client._resolve_market_fill_price("BTC-USD")
    assert price == 77.0
    assert repo.get_candles.await_count == 2


@pytest.mark.asyncio
async def test_cancel_order_sets_canceled_status() -> None:
    """Verify cancel_order sets CANCELED status without DB calls.

    Given: A running client with an open order,
    When: cancel_order() is called,
    Then: Order status is set to CANCELED (DB persistence handled by executor base).
    """
    client = PaperExchangeClient()
    client._running = True
    order = ExchangeOrderSnapshot(
        id="order-2",
        client_order_id=None,
        symbol="BTC/USD",
        side=OrderSideEnum.BUY,
        type=ExchangeOrderTypeEnum.LIMIT,
        amount=1.0,
        price=10.0,
        status=ExchangeOrderStatusEnum.OPEN,
        filled=0.0,
        remaining=1.0,
        timestamp=0.0,
        fee=None,
    )
    client._orders[order.id] = order
    result = await client.cancel_order(order.id, symbol=order.symbol)
    assert result.status == ExchangeOrderStatusEnum.CANCELED


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
            type=ExchangeOrderTypeEnum.LIMIT,
            amount=0.1,
            client_order_id="coid-create-order-requires-connection",
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
        assert result.status == ExchangeOrderStatusEnum.CANCELED

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
            type=ExchangeOrderTypeEnum.LIMIT,
            amount=0.1,
            client_order_id="coid-cancel-existing-order-updates-status",
            price=50000.0,
        )
        order = await paper_client.create_order(request)
        canceled = await paper_client.cancel_order(order.id, symbol="BTC-USD")
        assert canceled.status == ExchangeOrderStatusEnum.CANCELED
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

    def test_capability_flags_are_simulated_and_not_applicable(
        self, paper_client: PaperExchangeClient
    ) -> None:
        """Verify the paper client advertises simulated balance capability.

        Given: A paper exchange client,
        When: Its capability class attributes are inspected,
        Then: balance_capability is SIMULATED while both independent position
            declarations are NOT_APPLICABLE, so the account observer records
            balances as simulated and never probes for positions. This catches
            a separation mutation that starts probing the paper venue.
        """
        assert paper_client.balance_capability is CapabilityStatus.SIMULATED
        assert paper_client.position_observation_capability is CapabilityStatus.NOT_APPLICABLE
        assert paper_client.position_capability is CapabilityStatus.NOT_APPLICABLE

    @pytest.mark.asyncio
    async def test_read_native_balances_returns_simulated_entries(
        self, paper_client: PaperExchangeClient
    ) -> None:
        """Verify read_native_balances converts seeded balances to entries.

        Given: A connected paper exchange client with seeded balances,
        When: read_native_balances() is called,
        Then: One NativeBalanceEntry per currency is returned, each with
            the faithful free/used split from its AccountBalance.
        """
        await paper_client.connect()
        entries = await paper_client.read_native_balances()
        assert len(entries) == 5
        by_currency = {entry.currency: entry for entry in entries}
        assert set(by_currency) == {"USD", "EUR", "PLN", "BTC", "ETH"}
        usd = by_currency["USD"]
        assert isinstance(usd, NativeBalanceEntry)
        assert usd.total == pytest.approx(10000.0)
        assert usd.free == pytest.approx(10000.0)
        assert usd.used == pytest.approx(0.0)

    @pytest.mark.asyncio
    async def test_read_native_balances_empty_before_connect(
        self, paper_client: PaperExchangeClient
    ) -> None:
        """Verify read_native_balances returns [] with no seeded balances.

        Given: A paper exchange client that has not connected,
        When: read_native_balances() is called,
        Then: An empty list is returned.
        """
        entries = await paper_client.read_native_balances()
        assert entries == []

    @pytest.mark.asyncio
    async def test_read_native_balances_rejects_non_finite(
        self, paper_client: PaperExchangeClient
    ) -> None:
        """Verify read_native_balances rejects a non-finite balance.

        Given: A connected client whose USD balance was corrupted to NaN,
        When: read_native_balances() is called,
        Then: ValueError is raised rather than recording a poisoned value.
        """
        await paper_client.connect()
        paper_client._balances["USD"] = AccountBalance(
            currency="USD",
            free=float("nan"),
            used=0.0,
            total=10000.0,
        )
        with pytest.raises(ValueError, match="non-finite"):
            await paper_client.read_native_balances()


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
        lingering = asyncio.create_task(asyncio.sleep(10))
        paper_client._fill_simulator_tasks.add(lingering)
        await paper_client.disconnect()
        assert paper_client._fill_simulator_tasks == set()
        assert lingering.cancelled()
        assert not paper_client._running


class _ReplayRepo:
    """Test repository for paper trading replay data."""

    def __init__(self) -> None:
        now = datetime.now(tz=UTC)
        self.snapshots = [
            {
                "instrument_public_id": "inst-btc-usd",
                "bid": 10.0,
                "ask": 11.0,
                "last": 10.5,
                "ts": now,
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
                "executed_at": now,
                "trade_id": "42",
            }
        ]

    async def get_market_snapshots(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        return self.snapshots

    async def iter_market_snapshots(
        self, *args: Any, **kwargs: Any
    ) -> AsyncIterator[dict[str, Any]]:
        """Async-generator companion mirroring the production streaming path."""
        for snap in self.snapshots:
            yield snap

    async def get_candles(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        return self.candles

    async def iter_trades(self, *args: Any, **kwargs: Any) -> AsyncIterator[dict[str, Any]]:
        """Async-generator companion mirroring the production streaming path."""
        for trade in self.trades:
            yield trade


@pytest.mark.asyncio
async def test_create_order_logs_and_executes_with_db_updates() -> None:
    """Verify create_order logs to database and executes.

    Given: A connected client with mocked DB logging,
    When: create_order() is called with limit order,
    Then: Order is created with db_order_id and execution queued
        (status update and execution persistence handled by executor base).
    """
    client = PaperExchangeClient(repository=cast(Repository, _ReplayRepo()), fill_delay=0)
    with patch.object(client, "_log_order_to_db", AsyncMock(return_value=(123, "order-pub-abc"))):
        await client.connect()
        request = ExchangeOrderRequest(
            symbol="BTC/USD",
            side=OrderSideEnum.BUY,
            type=ExchangeOrderTypeEnum.LIMIT,
            amount=1.0,
            client_order_id="coid-create-order-logs-and-executes-with-db-updates",
            price=10.0,
        )
        order = await client.create_order(request)
        execution = await asyncio.wait_for(client._execution_queue.get(), timeout=0.5)
    assert order.db_order_id == 123
    assert order.db_order_public_id == "order-pub-abc"
    assert execution.order_status == ExchangeOrderStatusEnum.CLOSED


@pytest.mark.asyncio
async def test_get_order_known_and_unknown() -> None:
    """Verify get_order returns the live order or a CANCELED placeholder.

    Given: A connected client with one created order,
    When: get_order() is called with known and unknown IDs,
    Then: The known order is returned, and an unknown (post-restart) id gets a
        CANCELED terminal placeholder so recovery never resurrects it as a
        zombie pending.
    """
    client = PaperExchangeClient()
    await client.connect()
    order = await client.create_order(
        ExchangeOrderRequest(
            symbol="BTC/USD",
            side=OrderSideEnum.BUY,
            type=ExchangeOrderTypeEnum.LIMIT,
            amount=1,
            client_order_id="coid-get-order-known-and-unknown",
            price=10,
        )
    )
    assert (await client.get_order(order.id)).id == order.id
    placeholder = await client.get_order("missing", symbol="ETH/USD")
    assert placeholder.status == ExchangeOrderStatusEnum.CANCELED
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
            symbol="BTC/USD",
            side=OrderSideEnum.BUY,
            type=ExchangeOrderTypeEnum.LIMIT,
            amount=1,
            client_order_id="coid-get-orders-filters-and-limit",
            price=10,
        )
    )
    order2 = await client.create_order(
        ExchangeOrderRequest(
            symbol="ETH/USD",
            side=OrderSideEnum.SELL,
            type=ExchangeOrderTypeEnum.LIMIT,
            amount=2,
            client_order_id="coid-get-orders-filters-and-limit-2",
            price=20,
        )
    )
    await client.cancel_order(order2.id, symbol="ETH/USD")
    by_symbol = await client.get_orders(symbol="BTC/USD")
    assert by_symbol == [order1]
    canceled = await client.get_orders(status=ExchangeOrderStatusEnum.CANCELED)
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
        order_type=ExchangeOrderTypeEnum.MARKET,
        order_status=ExchangeOrderStatusEnum.CLOSED,
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
    client._resolve_instrument_public_ids = _fake_resolve
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
async def test_get_ticker_no_instrument_resolution_raises() -> None:
    """Verify get_ticker raises when instrument resolution returns empty.

    Given: A connected client with repository but resolution returns empty,
    When: get_ticker() is called,
    Then: ValueError is raised.
    """

    async def _empty_resolve(symbols: list[str], exchange: str) -> list[str]:
        return []

    repo = _ReplayRepo()
    client = PaperExchangeClient(repository=cast(Repository, repo), source_exchange="kraken")
    client._resolve_instrument_public_ids = _empty_resolve
    await client.connect()
    with pytest.raises(ValueError, match="No ticker data"):
        await client.get_ticker("UNKNOWN/USD")


@pytest.mark.asyncio
async def test_resolve_instrument_public_ids_no_repo() -> None:
    """Verify _resolve_instrument_public_ids returns empty when no repository.

    Given: A paper client with repository=None,
    When: _resolve_instrument_public_ids is called,
    Then: Returns empty list.
    """
    client = PaperExchangeClient(repository=None)
    result = await client._resolve_instrument_public_ids(["BTC/USD"], "kraken")
    assert result == []


@pytest.mark.asyncio
async def test_resolve_instrument_public_ids_full_path() -> None:
    """Verify _resolve_instrument_public_ids resolves via 2-hop lookup.

    Given: A mock repository whose session returns symbol_pid then inst_pid,
    When: _resolve_instrument_public_ids is called with two symbols (one resolvable),
    Then: Only the resolvable symbol's instrument_public_id is returned.
    """
    call_count = 0

    async def _mock_execute(stmt: Any) -> Any:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return SimpleNamespace(scalar_one_or_none=lambda: "sym-pub-1")
        if call_count == 2:
            return SimpleNamespace(scalar_one_or_none=lambda: "inst-pub-1")
        return SimpleNamespace(scalar_one_or_none=lambda: None)

    mock_session = AsyncMock()
    mock_session.execute = _mock_execute

    class _MockRepo:
        def session(self) -> Any:
            return _AsyncCtx(mock_session)

    client = PaperExchangeClient(repository=cast(Repository, _MockRepo()))
    result = await client._resolve_instrument_public_ids(["BTC/USD", "NOPE"], "kraken")
    assert result == ["inst-pub-1"]


@pytest.mark.asyncio
async def test_resolve_instrument_public_ids_symbol_not_found() -> None:
    """Verify _resolve_instrument_public_ids skips symbols with no Symbol row.

    Given: A mock repository whose session returns None for symbol lookup,
    When: _resolve_instrument_public_ids is called,
    Then: Returns empty list.
    """

    async def _mock_execute(stmt: Any) -> Any:
        return SimpleNamespace(scalar_one_or_none=lambda: None)

    mock_session = AsyncMock()
    mock_session.execute = _mock_execute

    class _MockRepo:
        def session(self) -> Any:
            return _AsyncCtx(mock_session)

    client = PaperExchangeClient(repository=cast(Repository, _MockRepo()))
    result = await client._resolve_instrument_public_ids(["NOPE"], "kraken")
    assert result == []


@pytest.mark.asyncio
async def test_resolve_instrument_public_ids_instrument_not_found() -> None:
    """Verify _resolve_instrument_public_ids skips when instrument not found.

    Given: A mock repository returning symbol_pid but None for instrument,
    When: _resolve_instrument_public_ids is called,
    Then: Returns empty list.
    """
    call_count = 0

    async def _mock_execute(stmt: Any) -> Any:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return SimpleNamespace(scalar_one_or_none=lambda: "sym-pub-1")
        return SimpleNamespace(scalar_one_or_none=lambda: None)

    mock_session = AsyncMock()
    mock_session.execute = _mock_execute

    class _MockRepo:
        def session(self) -> Any:
            return _AsyncCtx(mock_session)

    client = PaperExchangeClient(repository=cast(Repository, _MockRepo()))
    result = await client._resolve_instrument_public_ids(["BTC/USD"], "kraken")
    assert result == []


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
    client._resolve_instrument_public_ids = _fake_resolve
    client._running = True
    client.start_time = 0
    client.end_time = 1
    updates: list[TickerUpdate] = []
    async for tick in client.subscribe_ticker(["BTC/USD"]):
        updates.append(tick)
        break
    assert len(updates) == 1
    assert updates[0].bid == pytest.approx(10.0)


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
    assert candles
    assert isinstance(candles[0], CandleUpdate)
    assert trades
    assert trades[0].trade_id == "42"


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
    client._resolve_instrument_public_ids = _fake_resolve
    client._running = True
    client.start_time = 0
    client.end_time = 1
    ticks: list[Any] = []
    async for tick in client.subscribe_ticker(["BTC/USD"]):
        ticks.append(tick)
    assert ticks == []


@pytest.mark.asyncio
async def test_subscribe_ticker_no_instrument_resolution() -> None:
    """Verify subscribe_ticker returns empty when instrument resolution fails.

    Given: A running client where _resolve_instrument_public_ids returns empty,
    When: subscribe_ticker() is iterated,
    Then: No tickers are yielded.
    """

    async def _empty_resolve(symbols: list[str], exchange: str) -> list[str]:
        return []

    repo = _ReplayRepo()
    client = PaperExchangeClient(repository=cast(Repository, repo), source_exchange="kraken")
    client._resolve_instrument_public_ids = _empty_resolve
    client._running = True
    client.start_time = 0
    client.end_time = 1
    ticks: list[Any] = []
    async for tick in client.subscribe_ticker(["UNKNOWN"]):
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
        async def _trades_for_symbol(self, symbol: str) -> list[dict[str, Any]]:
            if symbol == "BTC/USD":
                occurred_at = datetime.fromtimestamp(0, tz=UTC)
                return [
                    {
                        "side": "buy",
                        "size": 1.0,
                        "price": 50000.0,
                        "timestamp": occurred_at,
                        "executed_at": occurred_at,
                        "trade_id": 1,
                    }
                ]
            return []

        async def iter_trades(
            self, symbol: str, *args: Any, **kwargs: Any
        ) -> AsyncIterator[dict[str, Any]]:
            for trade in await self._trades_for_symbol(symbol):
                yield trade

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
    client._resolve_instrument_public_ids = _fake_resolve
    client._running = True
    client.start_time = 0
    client.end_time = 1
    updates: list[Any] = []
    async for tick in client.subscribe_ticks(["BTC/USD"]):
        updates.append(tick)
    assert updates
    assert isinstance(updates[0], TickerUpdate)


@pytest.mark.asyncio
async def test_create_order_rejects_stop_types_before_storing() -> None:
    """Paper trading honestly refuses stop orders pre-send (#156).

    Given: a connected paper client and a stop-typed request,
    When: create_order is called,
    Then: a ValueError surfaces BEFORE the order is stored or a fill is
        scheduled — the fill simulator has no trigger logic, so accepting
        the order would fill a protective stop immediately.
    """
    client = PaperExchangeClient()
    await client.connect()
    try:
        for order_type in (
            ExchangeOrderTypeEnum.STOP_LOSS,
            ExchangeOrderTypeEnum.STOP_LOSS_LIMIT,
        ):
            request = ExchangeOrderRequest(
                symbol="BTC-USD",
                side=OrderSideEnum.SELL,
                type=order_type,
                amount=0.5,
                price=47900.0,
                stop_price=48000.0,
                client_order_id=f"cid-paper-{order_type.value}",
            )
            with pytest.raises(ValueError, match="does not support stop orders"):
                await client.create_order(request)
        assert client._orders == {}
        assert client._fill_simulator_tasks == set()
    finally:
        await client.disconnect()


@pytest.mark.asyncio
async def test_create_order_accepts_priceless_market_order() -> None:
    """A priceless market order is ACCEPTED for fill-time price resolution.

    Given: a connected paper client and a market request with price=None,
    When: create_order is called,
    Then: the order is accepted OPEN with a None snapshot price and a fill
        simulator task is scheduled — the reference price is resolved at
        FILL time from source-venue candles, and the simulator CANCELS the
        order when none is resolvable (it never fills at 0.0).
    """
    client = PaperExchangeClient(fill_delay=30.0)
    await client.connect()
    try:
        order = await client.create_order(
            ExchangeOrderRequest(
                symbol="BTC-USD",
                side=OrderSideEnum.BUY,
                type=ExchangeOrderTypeEnum.MARKET,
                amount=0.5,
                price=None,
                client_order_id="cid-paper-priceless",
            )
        )
        assert order.status is ExchangeOrderStatusEnum.OPEN
        assert order.price is None
        assert order.id in client._orders
        assert len(client._fill_simulator_tasks) == 1
    finally:
        await client.disconnect()


@pytest.mark.asyncio
async def test_create_order_rejects_zero_and_non_finite_price() -> None:
    """Paper trading rejects a PRESENT price that is zero or non-finite.

    Given: a connected paper client and market requests carrying price=0.0
        and price=NaN,
    When: create_order is called for each,
    Then: a ValueError with the poisoned-fill message surfaces for both —
        a present price must be POSITIVE and FINITE (priceless orders are
        accepted and priced at fill time instead), so a zero or NaN
        reference can never reach the fill simulator.
    """
    client = PaperExchangeClient()
    await client.connect()
    try:
        for bad_price in (0.0, float("nan")):
            request = ExchangeOrderRequest(
                symbol="BTC-USD",
                side=OrderSideEnum.BUY,
                type=ExchangeOrderTypeEnum.MARKET,
                amount=0.5,
                price=bad_price,
                client_order_id=f"cid-paper-bad-{bad_price!r}",
            )
            with pytest.raises(ValueError, match="positive finite reference"):
                await client.create_order(request)
        assert client._orders == {}
        assert client._fill_simulator_tasks == set()
    finally:
        await client.disconnect()
