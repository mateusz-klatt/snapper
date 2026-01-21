"""Tests for Zonda exchange client subscriptions."""

import asyncio
import contextlib
import json
from collections.abc import AsyncGenerator
from typing import Any
from typing import cast
from unittest.mock import AsyncMock

import pytest
from websockets.asyncio.client import ClientConnection
from websockets.exceptions import ConnectionClosed
from websockets.frames import Close

from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import ExecutionUpdate
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate
from snapper.infrastructure.exchanges.implementations.zonda import ZondaExchangeClient


class _IterWs:
    """Test WebSocket that iterates through messages."""

    def __init__(self, messages: list[str]):
        self._messages = messages

    def __aiter__(self) -> "_IterWs":
        return self

    async def __anext__(self) -> str:
        if not self._messages:
            raise StopAsyncIteration
        return self._messages.pop(0)


class _ClosedWs:
    """Test WebSocket that raises ConnectionClosed on iteration."""

    def __aiter__(self) -> "_ClosedWs":
        return self

    async def __anext__(self) -> str:
        close = Close(1000, "closed")
        raise ConnectionClosed(close, close)


def _client() -> ZondaExchangeClient:
    client = ZondaExchangeClient.__new__(ZondaExchangeClient)
    client._ws = None
    client._seq_no = {}
    client._pending_snapshots = {}
    client._tick_queue = asyncio.Queue()
    client._trade_queue = asyncio.Queue()
    client._execution_queue = asyncio.Queue()
    client._stats_cache = {}
    client._ticker_cache = {}
    client._running = False
    client._message_handler_task = None
    return client


@pytest.mark.asyncio
async def test_message_handler_routes_push_and_proxy(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify message handler routes various message types correctly.

    Given: A Zonda client with mocked message parsers,
    When: Messages of different types (ticker, stats, transactions, proxy) arrive,
    Then: Each parser is called appropriately and sequence numbers are tracked.
    """
    client = _client()
    parse_ticker = AsyncMock()
    parse_stats = AsyncMock()
    parse_tx = AsyncMock()
    parse_exec = AsyncMock()
    parse_proxy = AsyncMock()
    monkeypatch.setattr(client, "_parse_ticker_message", parse_ticker)
    monkeypatch.setattr(client, "_parse_stats_message", parse_stats)
    monkeypatch.setattr(client, "_parse_transactions_message", parse_tx)
    monkeypatch.setattr(client, "_parse_executions_message", parse_exec)
    monkeypatch.setattr(client, "_parse_proxy_response", parse_proxy)
    client._pending_snapshots["req1"] = "ticker"
    messages = [
        json.dumps({"action": "push", "topic": "trading/ticker/btc-pln", "seqNo": 1}),
        json.dumps({"action": "push", "topic": "trading/stats/btc-pln", "seqNo": 2}),
        json.dumps({"action": "push", "topic": "trading/transactions/btc-pln", "seqNo": 3}),
        json.dumps({"action": "push", "topic": "trading/history/transactions", "seqNo": 4}),
        json.dumps({"action": "subscribe-public-confirm", "module": "trading", "path": "ticker"}),
        json.dumps({"action": "subscribe-public-error", "error": "bad"}),
        json.dumps(
            {
                "action": "proxy-response",
                "requestId": "req1",
                "statusCode": 200,
                "body": {"status": "Ok", "ticker": {"market": {"code": "BTC-PLN"}}},
            }
        ),
        json.dumps({"action": "pong"}),
        json.dumps({"action": "json-error"}),
        "not-json",
    ]
    client._ws = cast(Any, _IterWs(messages))
    await client._message_handler()
    assert parse_ticker.await_count == 1
    assert parse_stats.await_count == 1
    assert parse_tx.await_count == 1
    assert parse_exec.await_count == 1
    assert parse_proxy.await_count == 1
    assert client._seq_no.get("trading/ticker/btc-pln") == 1


@pytest.mark.asyncio
async def test_message_handler_handles_connection_closed() -> None:
    """Verify message handler handles WebSocket connection closure gracefully.

    Given: A Zonda client with a WebSocket that raises ConnectionClosed,
    When: The message handler runs,
    Then: The handler exits without error and state remains intact.
    """
    client = _client()
    client._ws = cast(Any, _ClosedWs())
    await client._message_handler()
    assert client._seq_no == {}


class _DummyWs:
    """Test dummy WebSocket with no-op send."""

    async def send(self, message: str) -> None:
        return None


class _IterWsV2:
    """Test WebSocket iterator version 2."""

    def __init__(self, messages: list[str]):
        self._messages = messages

    def __aiter__(self) -> "_IterWsV2":
        return self

    async def __anext__(self) -> str:
        if not self._messages:
            raise StopAsyncIteration
        return self._messages.pop(0)


@pytest.mark.asyncio
async def test_subscribe_ticks_reuses_running_handler(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify tick subscription reuses existing message handler task.

    Given: A Zonda client with an already running message handler task,
    When: Subscribing to tick updates,
    Then: The existing handler is reused and ticks are yielded from queue.
    """
    client = ZondaExchangeClient()
    client._message_handler_task = asyncio.create_task(asyncio.sleep(0.1))

    async def fake_connect() -> None:
        client._ws = cast(ClientConnection, _DummyWs())

    async def fake_resubscribe() -> None:
        return None

    monkeypatch.setattr(client, "connect", fake_connect)
    monkeypatch.setattr(client, "_resubscribe", fake_resubscribe)
    tick = cast(TickerUpdate, object())
    await client._tick_queue.put(tick)
    gen = cast(
        AsyncGenerator[TickerUpdate, None], client.subscribe_ticks(["BTC-PLN"], snapshot=False)
    )
    result = await gen.__anext__()
    assert result is tick
    client._running = False
    await gen.aclose()
    client._message_handler_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await client._message_handler_task


@pytest.mark.asyncio
async def test_subscribe_ticks_raises_when_stopped_on_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify tick subscription raises error when connection fails and client stops.

    Given: A Zonda client with connect method that fails and sets running to False,
    When: Subscribing to tick updates,
    Then: RuntimeError is raised to the caller.
    """
    client = ZondaExchangeClient()

    async def fake_connect() -> None:
        client._running = False
        raise RuntimeError("boom")

    monkeypatch.setattr(client, "connect", fake_connect)
    gen = cast(
        AsyncGenerator[TickerUpdate, None],
        client.subscribe_ticks(["BTC-PLN"], snapshot=False),
    )
    with pytest.raises(RuntimeError):
        await gen.__anext__()
    await gen.aclose()


@pytest.mark.asyncio
async def test_parse_proxy_response_stats_routes_to_parser(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify proxy response with stats type routes to stats parser.

    Given: A Zonda client with pending stats snapshot request,
    When: Proxy response with stats data arrives,
    Then: The stats parser is called with the response data.
    """
    client = ZondaExchangeClient()
    client._pending_snapshots["req-id"] = "stats"
    called: bool = False

    async def fake_parse_stats(message: dict[str, object]) -> None:
        nonlocal called
        called = True

    monkeypatch.setattr(client, "_parse_stats_message", fake_parse_stats)
    data = {
        "requestId": "req-id",
        "statusCode": 200,
        "body": {"status": "Ok", "stats": [{"market": {"code": "BTC-PLN"}}]},
    }
    await client._parse_proxy_response(data)
    assert called is True


@pytest.mark.asyncio
async def test_parse_proxy_response_unknown_snapshot_is_ignored() -> None:
    """Verify proxy response with unknown snapshot type is ignored.

    Given: A Zonda client with pending snapshot of unknown type,
    When: Proxy response arrives for that request,
    Then: The request is cleaned up without error.
    """
    client = ZondaExchangeClient()
    client._pending_snapshots["req-id"] = "unknown"
    payload: dict[str, Any] = {
        "requestId": "req-id",
        "statusCode": 200,
        "body": {"status": "Ok"},
    }
    await client._parse_proxy_response(payload)
    assert "req-id" not in client._pending_snapshots


@pytest.mark.asyncio
async def test_subscribe_trades_reuses_running_handler(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify trade subscription reuses existing message handler task.

    Given: A Zonda client with an already running message handler task,
    When: Subscribing to trade updates,
    Then: The existing handler is reused and trades are yielded from queue.
    """
    client = ZondaExchangeClient()
    client._message_handler_task = asyncio.create_task(asyncio.sleep(0.1))

    async def fake_connect() -> None:
        client._ws = cast(ClientConnection, _DummyWs())

    monkeypatch.setattr(client, "connect", fake_connect)
    trade = cast(TradeUpdate, object())
    await client._trade_queue.put(trade)
    gen = cast(
        AsyncGenerator[TradeUpdate, None],
        client.subscribe_trades(["BTC-PLN"]),
    )
    result = await gen.__anext__()
    assert result is trade
    client._running = False
    await gen.aclose()
    client._message_handler_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await client._message_handler_task


@pytest.mark.asyncio
async def test_subscribe_trades_raises_when_stopped_on_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify trade subscription raises error when connection fails and client stops.

    Given: A Zonda client with connect method that fails and sets running to False,
    When: Subscribing to trade updates,
    Then: RuntimeError is raised to the caller.
    """
    client = ZondaExchangeClient()

    async def fake_connect() -> None:
        client._running = False
        raise RuntimeError("boom")

    monkeypatch.setattr(client, "connect", fake_connect)
    gen = cast(AsyncGenerator[TradeUpdate, None], client.subscribe_trades(["BTC-PLN"]))
    with pytest.raises(RuntimeError):
        await gen.__anext__()
    await gen.aclose()


@pytest.mark.asyncio
async def test_subscribe_trades_sleeps_and_retries_on_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify trade subscription sleeps and retries after connection exception.

    Given: A Zonda client with connect method that raises RuntimeError,
    When: Subscribing to trade updates,
    Then: The client sleeps before retry and stops iteration when running is False.
    """
    client = ZondaExchangeClient()

    async def fake_connect() -> None:
        raise RuntimeError("boom")

    sleep_calls: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleep_calls.append(delay)
        client._running = False

    monkeypatch.setattr(client, "connect", fake_connect)
    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    gen = cast(AsyncGenerator[TradeUpdate, None], client.subscribe_trades(["BTC-PLN"]))
    with pytest.raises(StopAsyncIteration):
        await anext(gen)
    assert sleep_calls


@pytest.mark.asyncio
async def test_subscribe_candles_uses_existing_aggregator(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify candle subscription reuses existing aggregator task.

    Given: A Zonda client with an already running candle aggregator task,
    When: Subscribing to candle updates,
    Then: The existing aggregator is reused and candles are yielded from queue.
    """
    client = ZondaExchangeClient()
    client._candle_aggregator_task = asyncio.create_task(asyncio.sleep(0.1))
    candle = cast(CandleUpdate, object())
    await client._candle_queue.put(candle)
    gen = cast(
        AsyncGenerator[CandleUpdate, None], client.subscribe_candles(["BTC-PLN"], timeframe="1m")
    )
    result = await gen.__anext__()
    assert result is candle
    client._running = False
    await gen.aclose()
    client._candle_aggregator_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await client._candle_aggregator_task


@pytest.mark.asyncio
async def test_subscribe_candles_respects_invalid_timeframe() -> None:
    """Verify candle subscription rejects invalid timeframe.

    Given: A Zonda client,
    When: Subscribing to candles with unsupported timeframe,
    Then: ValueError is raised.
    """
    client = ZondaExchangeClient()
    with pytest.raises(ValueError):
        await anext(client.subscribe_candles(["BTC-PLN"], timeframe="5m"))


@pytest.mark.asyncio
async def test_subscribe_candles_cleanup_skips_when_task_done(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify candle subscription cleanup handles already-finished aggregator task.

    Given: A Zonda client with aggregator that completes immediately,
    When: Subscribing to candles and closing the generator,
    Then: Cleanup handles done task without error.
    """
    client = ZondaExchangeClient()

    async def fake_candle_aggregator(symbols: list[str]) -> None:
        return None

    monkeypatch.setattr(client, "_candle_aggregator", fake_candle_aggregator)
    candle = cast(CandleUpdate, object())
    await client._candle_queue.put(candle)
    gen = cast(AsyncGenerator[CandleUpdate, None], client.subscribe_candles(["BTC-PLN"]))
    result = await gen.__anext__()
    assert result is candle
    await asyncio.sleep(0)
    client._running = False
    await gen.aclose()


@pytest.mark.asyncio
async def test_message_handler_skips_out_of_order_seqno(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify message handler skips messages with out-of-order sequence numbers.

    Given: A Zonda client with sequence number 5 for a topic,
    When: A message with sequence number 3 arrives for that topic,
    Then: The message is skipped and parser is not called.
    """
    client = ZondaExchangeClient()
    topic = "trading/history/transactions/btc-pln"
    client._seq_no[topic] = 5
    called: list[dict[str, Any]] = []

    async def fake_parse_executions_message(message: dict[str, Any]) -> None:
        called.append(message)

    payload = {
        "action": "push",
        "topic": topic,
        "seqNo": 3,
        "message": {"market": {"code": "BTC-PLN"}},
    }
    client._ws = cast(ClientConnection, _IterWs([json.dumps(payload)]))
    monkeypatch.setattr(client, "_parse_executions_message", fake_parse_executions_message)
    await client._message_handler()
    assert called == []


@pytest.mark.asyncio
async def test_message_handler_skips_duplicate_seqno(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify message handler skips duplicate sequence numbers.

    Given: A Zonda client receiving multiple messages,
    When: Two messages arrive with the same sequence number,
    Then: Only the first message is processed.
    """
    client = ZondaExchangeClient()
    topic = "trading/history/transactions/btc-pln"
    received: list[dict[str, Any]] = []

    async def fake_parse_executions_message(message: dict[str, Any]) -> None:
        received.append(message)

    payloads = [
        {
            "action": "push",
            "topic": topic,
            "seqNo": 1,
            "message": {"market": {"code": "BTC-PLN"}},
        },
        {
            "action": "push",
            "topic": topic,
            "seqNo": 1,
            "message": {"market": {"code": "BTC-PLN"}},
        },
    ]
    client._ws = cast(ClientConnection, _IterWs([json.dumps(payload) for payload in payloads]))
    monkeypatch.setattr(client, "_parse_executions_message", fake_parse_executions_message)
    await client._message_handler()
    assert len(received) == 1


@pytest.mark.asyncio
async def test_message_handler_unknown_topic_skipped(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify message handler skips messages with unknown topic patterns.

    Given: A Zonda client with mocked parsers for known topics,
    When: A message arrives with an unrecognized topic,
    Then: No parsers are called.
    """
    client = ZondaExchangeClient()
    parse_called = []

    async def fake_parse_ticker(msg: dict[str, Any]) -> None:
        parse_called.append("ticker")

    async def fake_parse_stats(msg: dict[str, Any]) -> None:
        parse_called.append("stats")

    async def fake_parse_transactions(msg: dict[str, Any]) -> None:
        parse_called.append("transactions")

    async def fake_parse_executions(msg: dict[str, Any]) -> None:
        parse_called.append("executions")

    payload = {
        "action": "push",
        "topic": "unknown/topic/type",
        "seqNo": 1,
        "message": {"data": "test"},
    }
    client._ws = cast(ClientConnection, _IterWs([json.dumps(payload)]))
    monkeypatch.setattr(client, "_parse_ticker_message", fake_parse_ticker)
    monkeypatch.setattr(client, "_parse_stats_message", fake_parse_stats)
    monkeypatch.setattr(client, "_parse_transactions_message", fake_parse_transactions)
    monkeypatch.setattr(client, "_parse_executions_message", fake_parse_executions)
    await client._message_handler()
    assert parse_called == []


@pytest.mark.asyncio
async def test_subscribe_executions_reuses_running_handler(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify execution subscription reuses existing message handler task.

    Given: A Zonda client with credentials and running message handler task,
    When: Subscribing to execution updates,
    Then: The existing handler is reused and executions are yielded from queue.
    """
    client = ZondaExchangeClient(api_key="key", api_secret="secret")
    client._message_handler_task = asyncio.create_task(asyncio.sleep(0.1))

    async def fake_connect() -> None:
        client._ws = cast(ClientConnection, _DummyWs())

    monkeypatch.setattr(client, "connect", fake_connect)
    execution = cast(ExecutionUpdate, object())
    await client._execution_queue.put(execution)
    gen = cast(AsyncGenerator[ExecutionUpdate, None], client.subscribe_executions())
    result = await gen.__anext__()
    assert result is execution
    client._running = False
    await gen.aclose()
    client._message_handler_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await client._message_handler_task


@pytest.mark.asyncio
async def test_subscribe_executions_raises_when_stopped_on_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify execution subscription raises error when connection fails and client stops.

    Given: A Zonda client with credentials and connect method that fails,
    When: Subscribing to execution updates,
    Then: RuntimeError is raised to the caller.
    """
    client = ZondaExchangeClient(api_key="key", api_secret="secret")

    async def fake_connect() -> None:
        client._running = False
        raise RuntimeError("boom")

    monkeypatch.setattr(client, "connect", fake_connect)
    gen = cast(AsyncGenerator[ExecutionUpdate, None], client.subscribe_executions())
    with pytest.raises(RuntimeError):
        await gen.__anext__()
    await gen.aclose()


@pytest.mark.asyncio
async def test_subscribe_executions_sleeps_and_retries_on_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify execution subscription sleeps and retries after connection exception.

    Given: A Zonda client with credentials and connect method that raises RuntimeError,
    When: Subscribing to execution updates,
    Then: The client sleeps before retry and stops iteration when running is False.
    """
    client = ZondaExchangeClient(api_key="key", api_secret="secret")

    async def fake_connect() -> None:
        raise RuntimeError("boom")

    sleep_calls: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleep_calls.append(delay)
        client._running = False

    monkeypatch.setattr(client, "connect", fake_connect)
    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    gen = cast(AsyncGenerator[ExecutionUpdate, None], client.subscribe_executions())
    with pytest.raises(StopAsyncIteration):
        await anext(gen)
    assert sleep_calls


@pytest.mark.asyncio
async def test_message_handler_routes_executions(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify message handler routes execution history messages to parser.

    Given: A Zonda client with mocked executions parser,
    When: An execution history push message arrives,
    Then: The executions parser is called with the message.
    """
    client = ZondaExchangeClient()
    received: list[dict[str, Any]] = []

    async def fake_parse_executions_message(message: dict[str, Any]) -> None:
        received.append(message)

    payload = {
        "action": "push",
        "topic": "trading/history/transactions/BTC-PLN",
        "seqNo": 1,
        "message": {"market": {"code": "BTC-PLN"}},
    }
    client._ws = cast(ClientConnection, _IterWs([json.dumps(payload)]))
    monkeypatch.setattr(client, "_parse_executions_message", fake_parse_executions_message)
    await client._message_handler()
    assert len(received) == 1
