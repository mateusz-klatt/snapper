"""Tests for Kraken Equities exchange client."""

import asyncio
import contextlib
from collections.abc import Generator
from datetime import UTC as _UTC
from datetime import datetime as _dt
from datetime import timedelta as _td
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import httpx
import pytest
from loguru import logger

from snapper.infrastructure.exchanges._subscription_request import SubscriptionRequest
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import InstrumentPairDescriptor
from snapper.infrastructure.exchanges.contracts import OhlcvSnapshot
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate
from snapper.infrastructure.exchanges.implementations import kraken_equities as ke
from snapper.infrastructure.exchanges.implementations.kraken_equities import (
    KrakenEquitiesExchangeClient,
)
from snapper.infrastructure.exchanges.implementations.kraken_equities import _enqueue_or_drop_oldest
from snapper.infrastructure.exchanges.implementations.kraken_equities import _timeframe_to_interval


@pytest.fixture()
def client() -> KrakenEquitiesExchangeClient:
    """Create a KrakenEquitiesExchangeClient instance for testing."""
    return KrakenEquitiesExchangeClient()


class TestClientInit:
    """Tests for client initialization."""

    def test_init_defaults(self) -> None:
        """Initialize with default parameters.

        Given: No arguments,
        When: KrakenEquitiesExchangeClient is created,
        Then: Queues are empty and supports_websocket_executions is False.
        """
        c = KrakenEquitiesExchangeClient()
        assert c.exchange_name == "kraken_equities"
        assert c.supports_websocket_executions is False
        assert c._tick_queue.empty()
        assert c._trade_queue.empty()
        assert c._ws_client is None


@pytest.mark.asyncio
async def test_subscribe_ticks_caches_asset_class_and_throttle(
    client: KrakenEquitiesExchangeClient,
) -> None:
    """Ticker subscription cache preserves Equities-specific parameters.

    Given: An Equities client with an active websocket,
    When: A ticker subscription starts,
    Then: The cache records asset_class and throttle in parameters_json.
    """
    client._ws_client = AsyncMock()
    await client._tick_queue.put(
        TickerUpdate(
            symbol="CLM6-NYMEX",
            bid=90.0,
            bid_qty=1.0,
            ask=90.1,
            ask_qty=1.0,
            last=90.05,
            volume=10.0,
            vwap=90.0,
            low=89.0,
            high=91.0,
            change=0.1,
            change_pct=0.1,
        )
    )
    gen = client.subscribe_ticks(["CLM6-NYMEX"])
    with patch(
        "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
        return_value="CLM6.NYMEX",
    ):
        await anext(gen)
    await gen.aclose()
    req = next(iter(client._subscription_cache.values()))
    assert req.channel == "ticker"
    assert req.symbols == ("CLM6.NYMEX",)
    assert '"asset_class": "futures_contract"' in req.parameters_json
    assert '"throttle": 5000' in req.parameters_json


@pytest.mark.asyncio
async def test_subscribe_trades_caches_trade_channel(
    client: KrakenEquitiesExchangeClient,
) -> None:
    """Trade subscription cache records the trade channel.

    Given: An Equities client with an active websocket,
    When: A trade subscription starts,
    Then: The cache records a trade subscription for the WS symbol.
    """
    client._ws_client = AsyncMock()
    await client._trade_queue.put(
        TradeUpdate(
            symbol="CLM6-NYMEX",
            price=90.0,
            quantity=1.0,
            side="buy",
            ord_type="fill",
            trade_id="trade-1",
            timestamp=_dt.now(_UTC),
        )
    )
    gen = client.subscribe_trades(["CLM6-NYMEX"])
    with patch(
        "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
        return_value="CLM6.NYMEX",
    ):
        await anext(gen)
    await gen.aclose()
    req = next(iter(client._subscription_cache.values()))
    assert req.channel == "trade"
    assert req.symbols == ("CLM6.NYMEX",)


@pytest.mark.asyncio
async def test_replay_iterates_cache_in_insertion_order_with_5s_delay(
    client: KrakenEquitiesExchangeClient,
) -> None:
    """Equities replay reconstructs Spot-style params payloads.

    Given: An Equities client with cached subscriptions,
    When: Subscriptions are replayed,
    Then: Subscribe is called with reconstructed params and a 5s inter-request delay.
    """
    ws = AsyncMock()
    client._ws_client = ws
    first = SubscriptionRequest(
        channel="ticker",
        symbols=("CLM6.NYMEX",),
        parameters_json='{"asset_class": "futures_contract", "snapshot": true, "throttle": 5000}',
    )
    second = SubscriptionRequest(
        channel="trade",
        symbols=("GCQ6.COMEX",),
        parameters_json='{"asset_class": "futures_contract", "snapshot": true, "throttle": 5000}',
    )
    client._subscription_cache[first.key()] = first
    client._subscription_cache[second.key()] = second
    with patch(
        "snapper.infrastructure.exchanges.implementations.kraken_equities.asyncio.sleep",
        new_callable=AsyncMock,
    ) as sleep_mock:
        await client._replay_subscriptions()
    assert ws.subscribe.await_args_list[0].kwargs["params"]["channel"] == "ticker"
    assert ws.subscribe.await_args_list[0].kwargs["params"]["symbol"] == ["CLM6.NYMEX"]
    assert ws.subscribe.await_args_list[1].kwargs["params"]["channel"] == "trade"
    assert ws.subscribe.await_args_list[1].kwargs["params"]["symbol"] == ["GCQ6.COMEX"]
    sleep_mock.assert_awaited_once_with(5.0)


@pytest.mark.asyncio
async def test_replay_requires_ws_client(client: KrakenEquitiesExchangeClient) -> None:
    """Equities replay fails without a websocket client.

    Given: An Equities client without a websocket,
    When: Subscriptions are replayed,
    Then: RuntimeError is raised.
    """
    client._ws_client = None
    with pytest.raises(RuntimeError):
        await client._replay_subscriptions()


@pytest.mark.asyncio
async def test_ensure_ws_connected_auto_replays_after_reconnect(
    client: KrakenEquitiesExchangeClient,
) -> None:
    """Equities reconnect automatically replays cached subscriptions.

    Given: An Equities client with a cached subscription,
    When: _ensure_ws_connected creates a websocket,
    Then: The replay hook is awaited.
    """
    req = SubscriptionRequest(channel="ticker", symbols=("CLM6.NYMEX",), parameters_json="{}")
    client._subscription_cache[req.key()] = req
    with (
        patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.SpotWSClient"
        ) as ws_cls,
        patch.object(client, "_replay_subscriptions", new_callable=AsyncMock) as replay_mock,
    ):
        ws_cls.return_value.start = AsyncMock()
        await client._ensure_ws_connected()
    replay_mock.assert_awaited_once()


class TestEnqueueOrDropOldest:
    """Tests for _enqueue_or_drop_oldest module-level helper."""

    def test_enqueue_when_space_available(self) -> None:
        """Enqueue item normally when queue has capacity.

        Given: Queue with maxsize=2 and one existing item,
        When: _enqueue_or_drop_oldest is called with a new item,
        Then: New item is added, queue has two items total.
        """
        queue: asyncio.Queue[str] = asyncio.Queue(maxsize=2)
        queue.put_nowait("first")
        _enqueue_or_drop_oldest(queue, "second", "test")
        assert queue.qsize() == 2

    def test_drops_oldest_when_full(self) -> None:
        """Drop oldest item and enqueue newest when queue is full.

        Given: Queue with maxsize=1 already containing 'old',
        When: _enqueue_or_drop_oldest is called with 'new',
        Then: Queue contains only 'new'.
        """
        queue: asyncio.Queue[str] = asyncio.Queue(maxsize=1)
        queue.put_nowait("old")
        _enqueue_or_drop_oldest(queue, "new", "test")
        assert queue.qsize() == 1
        assert queue.get_nowait() == "new"

    def test_drop_log_is_rate_limited(self, caplog: pytest.LogCaptureFixture) -> None:
        """Per-drop log spam is collapsed to one summary per interval.

        Given: A bounded queue at capacity 1 and a freshly-reset counter,
        When: 50 drop-oldest events fire within the same interval,
        Then: At most one warning summary line is emitted.
        """
        ke._drop_counters.clear()
        queue: asyncio.Queue[str] = asyncio.Queue(maxsize=1)
        queue.put_nowait("seed")
        sink_id = logger.add(caplog.handler, format="{message}", level="WARNING")
        try:
            for i in range(50):
                _enqueue_or_drop_oldest(queue, f"item-{i}", "equities-tick")
        finally:
            logger.remove(sink_id)
        ke._drop_counters.clear()

        summaries = [rec for rec in caplog.records if "equities-tick queue full" in rec.message]
        assert len(summaries) <= 1


class TestConnect:
    """Tests for connect/disconnect lifecycle."""

    @pytest.mark.asyncio
    async def test_connect_is_noop(self, client: KrakenEquitiesExchangeClient) -> None:
        """Connect completes without error (lazy WS creation).

        Given: Fresh client,
        When: connect() is called,
        Then: Completes without error, no WS client created.
        """
        await client.connect()
        assert client._ws_client is None

    @pytest.mark.asyncio
    async def test_disconnect_without_ws(self, client: KrakenEquitiesExchangeClient) -> None:
        """Disconnect when no WS client is active.

        Given: Client with no WS connection,
        When: disconnect() is called,
        Then: Completes without error.
        """
        await client.disconnect()
        assert client._ws_client is None

    @pytest.mark.asyncio
    async def test_disconnect_with_ws(self, client: KrakenEquitiesExchangeClient) -> None:
        """Disconnect closes WS client.

        Given: Client with active WS connection,
        When: disconnect() is called,
        Then: WS client is closed and set to None.
        """
        mock_ws = AsyncMock()
        client._ws_client = mock_ws
        await client.disconnect()
        mock_ws.close.assert_awaited_once()
        assert client._ws_client is None

    @pytest.mark.asyncio
    async def test_disconnect_ws_error_handled(self, client: KrakenEquitiesExchangeClient) -> None:
        """Disconnect handles WS close error gracefully.

        Given: WS client raises on close(),
        When: disconnect() is called,
        Then: No exception propagated, WS client set to None.
        """
        mock_ws = AsyncMock()
        mock_ws.close.side_effect = RuntimeError("close failed")
        client._ws_client = mock_ws
        await client.disconnect()
        assert client._ws_client is None


class TestOnWsMessage:
    """Tests for the WS callback-to-queue bridge."""

    @pytest.fixture(autouse=True)
    def _patch_adapters(self) -> Generator[None]:
        """Patch adapter functions for WS message routing tests."""
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.parse_kraken_equities_ticker",
                return_value=TickerUpdate(
                    symbol="CLM6-NYMEX",
                    bid=90.10,
                    bid_qty=3.0,
                    ask=90.13,
                    ask_qty=4.0,
                    last=90.11,
                    volume=148427.0,
                    vwap=91.2,
                    low=88.7,
                    high=94.81,
                    change=-3.05,
                    change_pct=-3.27,
                ),
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.parse_kraken_equities_trade",
                return_value=TradeUpdate(
                    symbol="CLM6-NYMEX",
                    side="buy",
                    quantity=1.0,
                    price=90.12,
                    ord_type="fill",
                    timestamp=MagicMock(),
                    trade_id="7623847138430121307",
                ),
            ),
        ):
            yield

    @pytest.mark.asyncio
    async def test_on_ws_message_drops_ticker_snapshot(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Drop ticker snapshot WS message before enqueue.

        Given: WS message with channel=ticker and type=snapshot,
        When: _on_ws_message is called,
        Then: No TickerUpdate is placed in _tick_queue.
        """
        msg = {
            "channel": "ticker",
            "type": "snapshot",
            "data": [{"symbol": "CLM6.NYMEX", "bid": 90.10}],
        }
        await client._on_ws_message(msg)
        assert client._tick_queue.empty()

    @pytest.mark.asyncio
    async def test_on_ws_message_passes_ticker_update(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Route ticker update WS message to tick_queue.

        Given: WS message with channel=ticker and type=update,
        When: _on_ws_message is called,
        Then: Parsed TickerUpdate is placed in _tick_queue.
        """
        msg = {
            "channel": "ticker",
            "type": "update",
            "data": [{"symbol": "CLM6.NYMEX", "last": 90.15}],
        }
        await client._on_ws_message(msg)
        assert not client._tick_queue.empty()

    @pytest.mark.asyncio
    async def test_on_ws_message_drops_trade_snapshot(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Drop trade snapshot WS message before enqueue.

        Given: WS message with channel=trade and type=snapshot,
        When: _on_ws_message is called,
        Then: No TradeUpdate is placed in _trade_queue.
        """
        msg = {
            "channel": "trade",
            "type": "snapshot",
            "data": [
                {
                    "symbol": "CLM6.NYMEX",
                    "side": "buy",
                    "price": 90.12,
                    "qty": 1,
                    "timestamp": "2026-04-01T17:40:36.368Z",
                    "sequence": 95579,
                    "index": 7623847138430121307,
                }
            ],
        }
        await client._on_ws_message(msg)
        assert client._trade_queue.empty()
        assert client._candle_builder.active_buckets() == 0

    @pytest.mark.asyncio
    async def test_trade_message_also_folds_into_candle_builder(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Every parsed trade is also routed into ``_candle_builder``.

        Given: A trade update WS message arrives via the equities channel,
        When: ``_on_ws_message`` is called,
        Then: ``_candle_builder.active_buckets()`` becomes 1 —
            confirming the trade-handler path wires the builder, not
            just the trade queue. Regression guard analogous to the
            Kraken Futures test for the same property; without it,
            the live candle stream would silently stay empty.
        """
        assert client._candle_builder.active_buckets() == 0
        msg = {
            "channel": "trade",
            "type": "update",
            "data": [
                {
                    "symbol": "CLM6.NYMEX",
                    "side": "buy",
                    "price": 90.12,
                    "qty": 1,
                    "timestamp": "2026-04-01T17:40:36.368Z",
                    "sequence": 95579,
                    "index": 7623847138430121307,
                }
            ],
        }
        await client._on_ws_message(msg)
        assert client._candle_builder.active_buckets() == 1

    @pytest.mark.asyncio
    async def test_heartbeat_ignored(self, client: KrakenEquitiesExchangeClient) -> None:
        """Ignore heartbeat messages.

        Given: WS heartbeat message,
        When: _on_ws_message is called,
        Then: No items in any queue.
        """
        await client._on_ws_message({"channel": "heartbeat", "type": "heartbeat"})
        assert client._tick_queue.empty()
        assert client._trade_queue.empty()

    @pytest.mark.asyncio
    async def test_list_message_ignored(self, client: KrakenEquitiesExchangeClient) -> None:
        """Ignore list-type messages.

        Given: WS message that is a list,
        When: _on_ws_message is called,
        Then: No items in any queue.
        """
        await client._on_ws_message([1, 2, 3])
        assert client._tick_queue.empty()
        assert client._trade_queue.empty()

    @pytest.mark.asyncio
    async def test_unparseable_ticker_skipped(self, client: KrakenEquitiesExchangeClient) -> None:
        """Skip ticker messages that fail parsing.

        Given: WS ticker message that causes ValueError in parser,
        When: _on_ws_message is called,
        Then: No items in tick_queue and no exception raised.
        """
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.parse_kraken_equities_ticker",
            side_effect=ValueError("parse error"),
        ):
            msg = {
                "channel": "ticker",
                "type": "update",
                "data": [{"symbol": "INVALID"}],
            }
            await client._on_ws_message(msg)
        assert client._tick_queue.empty()

    @pytest.mark.asyncio
    async def test_unparseable_trade_skipped(self, client: KrakenEquitiesExchangeClient) -> None:
        """Skip trade messages that fail parsing.

        Given: WS trade message that causes ValueError in parser,
        When: _on_ws_message is called,
        Then: No items in trade_queue and no exception raised.
        """
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.parse_kraken_equities_trade",
            side_effect=ValueError("parse error"),
        ):
            msg = {
                "channel": "trade",
                "type": "update",
                "data": [{"symbol": "INVALID"}],
            }
            await client._on_ws_message(msg)
        assert client._trade_queue.empty()


class TestFetchInstrumentsRest:
    """Tests for REST instrument fetching."""

    @pytest.mark.asyncio
    async def test_fetch_instruments_rest_filters_active(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Fetch and filter only active tradable contracts.

        Given: REST response with 3 contracts (2 active+tradable, 1 inactive),
        When: _fetch_instruments_rest is called,
        Then: Returns only the 2 active tradable contracts.
        """
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "result": {
                "data": [
                    {"symbol": "CLM6.NYMEX", "tradable": True, "status": "active"},
                    {"symbol": "GCQ6.COMEX", "tradable": True, "status": "active"},
                    {"symbol": "CLK5.NYMEX", "tradable": False, "status": "inactive"},
                ]
            }
        }
        mock_response.raise_for_status = MagicMock()

        mock_http_client = AsyncMock()
        mock_http_client.get.return_value = mock_response
        mock_http_client.__aenter__ = AsyncMock(return_value=mock_http_client)
        mock_http_client.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.httpx.AsyncClient",
            return_value=mock_http_client,
        ):
            result = await client._fetch_instruments_rest()
        assert len(result) == 2
        assert result[0]["symbol"] == "CLM6.NYMEX"
        assert result[1]["symbol"] == "GCQ6.COMEX"

    @pytest.mark.asyncio
    async def test_fetch_instruments_rest_filters_tradable_but_not_active(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Filter out contracts that are tradable but not active.

        Given: REST response with tradable=True but status=undefined,
        When: _fetch_instruments_rest is called,
        Then: Contract is excluded.
        """
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "result": {
                "data": [
                    {"symbol": "CLM6.NYMEX", "tradable": True, "status": "undefined"},
                ]
            }
        }
        mock_response.raise_for_status = MagicMock()

        mock_http_client = AsyncMock()
        mock_http_client.get.return_value = mock_response
        mock_http_client.__aenter__ = AsyncMock(return_value=mock_http_client)
        mock_http_client.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.httpx.AsyncClient",
            return_value=mock_http_client,
        ):
            result = await client._fetch_instruments_rest()
        assert len(result) == 0


class TestGetParsedInstrument:
    """Tests for get_parsed_instrument."""

    def test_get_parsed_instrument(self, client: KrakenEquitiesExchangeClient) -> None:
        """Parse raw instrument dict into descriptor.

        Given: Raw instrument dict,
        When: get_parsed_instrument is called,
        Then: Returns InstrumentPairDescriptor.
        """
        raw = {
            "symbol": "CLM6.NYMEX",
            "tradable": True,
            "status": "active",
            "tick_size": "0.01",
            "contract_size": "1000",
            "base": "USD",
            "quote": "USD",
        }
        result = client.get_parsed_instrument(raw)
        assert isinstance(result, InstrumentPairDescriptor)
        assert result.symbol == "CLM6.NYMEX"
        assert result.base == "USD"


class TestNotImplementedMethods:
    """Tests for market-data-only stub methods."""

    @pytest.mark.asyncio
    async def test_create_order_raises(self, client: KrakenEquitiesExchangeClient) -> None:
        """create_order raises NotImplementedError.

        Given: Market-data-only client,
        When: create_order is called,
        Then: Raises NotImplementedError.
        """
        with pytest.raises(NotImplementedError, match="market data only"):
            await client.create_order(MagicMock())

    @pytest.mark.asyncio
    async def test_cancel_order_raises(self, client: KrakenEquitiesExchangeClient) -> None:
        """cancel_order raises NotImplementedError.

        Given: Market-data-only client,
        When: cancel_order is called,
        Then: Raises NotImplementedError.
        """
        with pytest.raises(NotImplementedError, match="market data only"):
            await client.cancel_order("123")

    @pytest.mark.asyncio
    async def test_get_order_raises(self, client: KrakenEquitiesExchangeClient) -> None:
        """get_order raises NotImplementedError.

        Given: Market-data-only client,
        When: get_order is called,
        Then: Raises NotImplementedError.
        """
        with pytest.raises(NotImplementedError, match="market data only"):
            await client.get_order("123")

    @pytest.mark.asyncio
    async def test_get_orders_raises(self, client: KrakenEquitiesExchangeClient) -> None:
        """get_orders raises NotImplementedError.

        Given: Market-data-only client,
        When: get_orders is called,
        Then: Raises NotImplementedError.
        """
        with pytest.raises(NotImplementedError, match="market data only"):
            await client.get_orders()

    @pytest.mark.asyncio
    async def test_get_balance_raises(self, client: KrakenEquitiesExchangeClient) -> None:
        """get_balance raises NotImplementedError.

        Given: Market-data-only client,
        When: get_balance is called,
        Then: Raises NotImplementedError.
        """
        with pytest.raises(NotImplementedError, match="market data only"):
            await client.get_balance()

    @pytest.mark.asyncio
    async def test_get_ticker_raises(self, client: KrakenEquitiesExchangeClient) -> None:
        """get_ticker raises NotImplementedError.

        Given: Market-data-only client,
        When: get_ticker is called,
        Then: Raises NotImplementedError.
        """
        with pytest.raises(NotImplementedError, match="WebSocket ticker"):
            await client.get_ticker("CLM6-NYMEX")

    @pytest.mark.asyncio
    async def test_subscribe_candles_reuses_running_aggregator_task(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """A second subscribe_candles call does not spawn a duplicate aggregator.

        Given: A pre-existing aggregator task that is still running,
        When: subscribe_candles is iterated and immediately closed,
        Then: The same task object remains the client's aggregator
            handle (the ``is None or .done()`` guard short-circuits).
        """

        async def never_returns() -> None:
            while True:
                await asyncio.sleep(60)

        client._candle_aggregator_task = asyncio.create_task(never_returns())
        original_task = client._candle_aggregator_task
        try:
            iterator = client.subscribe_candles(["MNQM6-CME"], "1m")
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(iterator.__anext__(), timeout=0.05)
            assert client._candle_aggregator_task is original_task
        finally:
            original_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await original_task

    @pytest.mark.asyncio
    async def test_subscribe_candles_timeout_loops_until_candle_arrives(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """The yield loop survives queue-empty timeouts without spinning out.

        Given: The candle queue is empty and the aggregator never
            emits anything,
        When: subscribe_candles is awaited with a tight outer timeout,
        Then: The inner ``asyncio.wait_for`` raises ``TimeoutError``,
            the ``await asyncio.sleep(0.01)`` branch executes, and
            the outer ``wait_for`` is the one that finally bails — proving
            the empty-queue branch is reachable.
        """
        real_sleep = asyncio.sleep

        async def fast_sleep(_: float) -> None:
            await real_sleep(0)

        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.asyncio.sleep",
            new=fast_sleep,
        ):
            iterator = client.subscribe_candles(["MNQM6-CME"], "1m")
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(iterator.__anext__(), timeout=0.2)

    @pytest.mark.asyncio
    async def test_subscribe_candles_rejects_non_1m(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """subscribe_candles only accepts the 1m timeframe.

        Given: A ``"5m"`` request,
        When: subscribe_candles is iterated,
        Then: A ``ValueError`` surfaces with a hint to ``get_ohlcv``.
        """
        iterator = client.subscribe_candles(["MNQM6-CME"], "5m")
        with pytest.raises(ValueError, match="only supports 1m"):
            await iterator.__anext__()

    @pytest.mark.asyncio
    async def test_subscribe_candles_emits_built_from_trades(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """subscribe_candles emits a candle once its minute closes.

        Given: A trade with a timestamp deep in the past is folded
            into the builder (its minute is therefore strictly before
            the aggregator's ``now``),
        When: subscribe_candles is iterated and the aggregator's
            ``asyncio.sleep`` is collapsed so it tick-runs immediately,
        Then: A ``CandleUpdate`` carrying the trade's OHLCV is yielded.
            ``datetime.now()`` is not mocked — the past-trade timestamp
            does the same thing without breaking the
            ``replace().timestamp()`` chain inside the builder.
        """
        past_minute = (_dt.now(_UTC) - _td(minutes=5)).replace(second=0, microsecond=0)
        trade = TradeUpdate(
            symbol="MNQM6-CME",
            side="buy",
            quantity=2.0,
            price=22500.0,
            ord_type="fill",
            timestamp=past_minute,
            trade_id="t1",
        )
        client._candle_builder.update(trade)

        real_sleep = asyncio.sleep

        async def fast_sleep(_: float) -> None:
            await real_sleep(0)

        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.asyncio.sleep",
            new=fast_sleep,
        ):
            iterator = client.subscribe_candles(["MNQM6-CME"], "1m")
            result = await asyncio.wait_for(iterator.__anext__(), timeout=2.0)
        assert isinstance(result, CandleUpdate)
        assert result.symbol == "MNQM6-CME"
        assert result.open == pytest.approx(22500.0)
        assert result.volume == pytest.approx(2.0)
        assert result.trades == 1
        assert result.interval == 60

    def test_subscribe_executions_raises(self, client: KrakenEquitiesExchangeClient) -> None:
        """subscribe_executions raises NotImplementedError.

        Given: Market-data-only client,
        When: subscribe_executions is called,
        Then: Raises NotImplementedError.
        """
        with pytest.raises(NotImplementedError, match="market data only"):
            client.subscribe_executions()


class TestEnsureWsConnected:
    """Tests for WebSocket connection management."""

    @pytest.mark.asyncio
    async def test_creates_ws_on_first_call(self, client: KrakenEquitiesExchangeClient) -> None:
        """Create WS client on first call.

        Given: No WS client,
        When: _ensure_ws_connected is called,
        Then: SpotWSClient is created and started.
        """
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.SpotWSClient"
        ) as mock_cls:
            mock_ws = AsyncMock()
            mock_cls.return_value = mock_ws
            await client._ensure_ws_connected()
            mock_cls.assert_called_once()
            mock_ws.start.assert_awaited_once()
            assert client._ws_client is mock_ws

    @pytest.mark.asyncio
    async def test_noop_when_already_connected(self, client: KrakenEquitiesExchangeClient) -> None:
        """Skip creation when already connected.

        Given: WS client already exists,
        When: _ensure_ws_connected is called,
        Then: No new client is created.
        """
        mock_ws = AsyncMock()
        client._ws_client = mock_ws
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.SpotWSClient"
        ) as mock_cls:
            await client._ensure_ws_connected()
            mock_cls.assert_not_called()


class TestSubscribeTicks:
    """Tests for subscribe_ticks async iterator."""

    @pytest.mark.asyncio
    async def test_subscribe_ticks_yields_from_queue(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Yield ticker updates from internal queue.

        Given: WS client connected and tick_queue has an item,
        When: subscribe_ticks iterator is consumed,
        Then: Yields the queued TickerUpdate.
        """
        mock_ws = AsyncMock()
        client._ws_client = mock_ws
        client._ensure_ws_connected = AsyncMock()
        ticker = TickerUpdate(
            symbol="CLM6-NYMEX",
            bid=90.10,
            bid_qty=3.0,
            ask=90.13,
            ask_qty=4.0,
            last=90.11,
            volume=148427.0,
            vwap=91.2,
            low=88.7,
            high=94.81,
            change=-3.05,
            change_pct=-3.27,
        )
        client._tick_queue.put_nowait(ticker)

        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
            return_value="CLM6.NYMEX",
        ):
            items = []
            async for item in client.subscribe_ticks(["CLM6-NYMEX"]):
                items.append(item)
                break
        assert len(items) == 1
        assert items[0].symbol == "CLM6-NYMEX"
        mock_ws.subscribe.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_subscribe_ticks_handles_timeout(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Handle queue timeout when no tickers available initially.

        Given: WS client connected but tick_queue is empty initially,
        When: subscribe_ticks iterator is consumed,
        Then: Loops through timeout, then yields when item arrives.
        """
        mock_ws = AsyncMock()
        client._ws_client = mock_ws
        client._ensure_ws_connected = AsyncMock()
        ticker = TickerUpdate(
            symbol="CLM6-NYMEX",
            bid=90.10,
            bid_qty=3.0,
            ask=90.13,
            ask_qty=4.0,
            last=90.11,
            volume=148427.0,
            vwap=91.2,
            low=88.7,
            high=94.81,
            change=-3.05,
            change_pct=-3.27,
        )

        async def delayed_put() -> None:
            await asyncio.sleep(0.15)
            client._tick_queue.put_nowait(ticker)

        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
            return_value="CLM6.NYMEX",
        ):
            asyncio.create_task(delayed_put())
            items = []
            async for item in client.subscribe_ticks(["CLM6-NYMEX"]):
                items.append(item)
                break
        assert len(items) == 1

    @pytest.mark.asyncio
    async def test_subscribe_ticks_unsubscribes_on_break(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Cleanup calls subscribe (unsubscribe) when consumer breaks out.

        Given: WS client connected and tick_queue has an item,
        When: Consumer breaks out of iterator and aclose is called,
        Then: Cleanup subscribe is invoked.
        """
        mock_ws = AsyncMock()
        client._ws_client = mock_ws
        client._ensure_ws_connected = AsyncMock()
        ticker = TickerUpdate(
            symbol="CLM6-NYMEX",
            bid=90.10,
            bid_qty=3.0,
            ask=90.13,
            ask_qty=4.0,
            last=90.11,
            volume=148427.0,
            vwap=91.2,
            low=88.7,
            high=94.81,
            change=-3.05,
            change_pct=-3.27,
        )
        client._tick_queue.put_nowait(ticker)

        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
            return_value="CLM6.NYMEX",
        ):
            gen = client.subscribe_ticks(["CLM6-NYMEX"])
            async for _ in gen:
                break
            await gen.aclose()
        assert mock_ws.subscribe.await_count == 2

    @pytest.mark.asyncio
    async def test_subscribe_ticks_cleanup_failure_handled(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Handle cleanup subscribe failure gracefully.

        Given: WS client raises on second subscribe (cleanup),
        When: Consumer breaks out of iterator,
        Then: No exception propagated.
        """
        call_count = 0

        def subscribe_side_effect(**kwargs: dict[str, object]) -> None:
            nonlocal call_count
            call_count += 1
            if call_count > 1:
                raise RuntimeError("unsubscribe failed")

        mock_ws = AsyncMock()
        mock_ws.subscribe = AsyncMock(side_effect=subscribe_side_effect)
        client._ws_client = mock_ws
        client._ensure_ws_connected = AsyncMock()
        ticker = TickerUpdate(
            symbol="CLM6-NYMEX",
            bid=90.10,
            bid_qty=3.0,
            ask=90.13,
            ask_qty=4.0,
            last=90.11,
            volume=148427.0,
            vwap=91.2,
            low=88.7,
            high=94.81,
            change=-3.05,
            change_pct=-3.27,
        )
        client._tick_queue.put_nowait(ticker)

        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
            return_value="CLM6.NYMEX",
        ):
            gen = client.subscribe_ticks(["CLM6-NYMEX"])
            async for _ in gen:
                break
            await gen.aclose()

    @pytest.mark.asyncio
    async def test_subscribe_ticks_raises_when_ws_none_after_connect(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Raise RuntimeError if _ws_client is None after _ensure_ws_connected.

        Given: _ensure_ws_connected is a no-op that does not set _ws_client,
        When: subscribe_ticks is called,
        Then: RuntimeError is raised.
        """
        client._ensure_ws_connected = AsyncMock()
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
                return_value="CLM6.NYMEX",
            ),
            pytest.raises(RuntimeError, match="WebSocket client not connected"),
        ):
            await anext(aiter(client.subscribe_ticks(["CLM6-NYMEX"])))


class TestSubscribeTrades:
    """Tests for subscribe_trades async iterator."""

    @pytest.mark.asyncio
    async def test_subscribe_trades_yields_from_queue(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Yield trade updates from internal queue.

        Given: WS client connected and trade_queue has an item,
        When: subscribe_trades iterator is consumed,
        Then: Yields the queued TradeUpdate.
        """
        mock_ws = AsyncMock()
        client._ws_client = mock_ws
        client._ensure_ws_connected = AsyncMock()
        trade = TradeUpdate(
            symbol="CLM6-NYMEX",
            side="buy",
            quantity=1.0,
            price=90.12,
            ord_type="fill",
            timestamp=MagicMock(),
            trade_id="7623847138430121307",
        )
        client._trade_queue.put_nowait(trade)

        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
            return_value="CLM6.NYMEX",
        ):
            items = []
            async for item in client.subscribe_trades(["CLM6-NYMEX"]):
                items.append(item)
                break
        assert len(items) == 1
        assert items[0].symbol == "CLM6-NYMEX"
        mock_ws.subscribe.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_subscribe_trades_handles_timeout(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Handle queue timeout when no trades available initially.

        Given: WS client connected but trade_queue is empty initially,
        When: subscribe_trades iterator is consumed,
        Then: Loops through timeout, then yields when item arrives.
        """
        mock_ws = AsyncMock()
        client._ws_client = mock_ws
        client._ensure_ws_connected = AsyncMock()
        trade = TradeUpdate(
            symbol="CLM6-NYMEX",
            side="buy",
            quantity=1.0,
            price=90.12,
            ord_type="fill",
            timestamp=MagicMock(),
            trade_id="7623847138430121307",
        )

        async def delayed_put() -> None:
            await asyncio.sleep(0.15)
            client._trade_queue.put_nowait(trade)

        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
            return_value="CLM6.NYMEX",
        ):
            asyncio.create_task(delayed_put())
            items = []
            async for item in client.subscribe_trades(["CLM6-NYMEX"]):
                items.append(item)
                break
        assert len(items) == 1

    @pytest.mark.asyncio
    async def test_subscribe_trades_unsubscribes_on_break(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Cleanup calls subscribe (unsubscribe) when consumer breaks out.

        Given: WS client connected and trade_queue has an item,
        When: Consumer breaks out of iterator and aclose is called,
        Then: Cleanup subscribe is invoked.
        """
        mock_ws = AsyncMock()
        client._ws_client = mock_ws
        client._ensure_ws_connected = AsyncMock()
        trade = TradeUpdate(
            symbol="CLM6-NYMEX",
            side="buy",
            quantity=1.0,
            price=90.12,
            ord_type="fill",
            timestamp=MagicMock(),
            trade_id="7623847138430121307",
        )
        client._trade_queue.put_nowait(trade)

        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
            return_value="CLM6.NYMEX",
        ):
            gen = client.subscribe_trades(["CLM6-NYMEX"])
            async for _ in gen:
                break
            await gen.aclose()
        assert mock_ws.subscribe.await_count == 2

    @pytest.mark.asyncio
    async def test_subscribe_trades_cleanup_failure_handled(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Handle cleanup subscribe failure gracefully.

        Given: WS client raises on second subscribe (cleanup),
        When: Consumer breaks out of trade iterator,
        Then: No exception propagated.
        """
        call_count = 0

        def subscribe_side_effect(**kwargs: dict[str, object]) -> None:
            nonlocal call_count
            call_count += 1
            if call_count > 1:
                raise RuntimeError("unsubscribe failed")

        mock_ws = AsyncMock()
        mock_ws.subscribe = AsyncMock(side_effect=subscribe_side_effect)
        client._ws_client = mock_ws
        client._ensure_ws_connected = AsyncMock()
        trade = TradeUpdate(
            symbol="CLM6-NYMEX",
            side="buy",
            quantity=1.0,
            price=90.12,
            ord_type="fill",
            timestamp=MagicMock(),
            trade_id="7623847138430121307",
        )
        client._trade_queue.put_nowait(trade)

        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
            return_value="CLM6.NYMEX",
        ):
            gen = client.subscribe_trades(["CLM6-NYMEX"])
            async for _ in gen:
                break
            await gen.aclose()

    @pytest.mark.asyncio
    async def test_subscribe_trades_raises_when_ws_none_after_connect(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Raise RuntimeError if _ws_client is None after _ensure_ws_connected.

        Given: _ensure_ws_connected is a no-op that does not set _ws_client,
        When: subscribe_trades is called,
        Then: RuntimeError is raised.
        """
        client._ensure_ws_connected = AsyncMock()
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
                return_value="CLM6.NYMEX",
            ),
            pytest.raises(RuntimeError, match="WebSocket client not connected"),
        ):
            await anext(aiter(client.subscribe_trades(["CLM6-NYMEX"])))


class TestSubscribeInstruments:
    """Tests for subscribe_instruments async iterator."""

    @pytest.mark.asyncio
    async def test_subscribe_instruments_yields_all(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Yield all instruments from REST API.

        Given: _fetch_instruments_rest returns 2 instruments,
        When: subscribe_instruments is iterated,
        Then: Yields 2 raw instrument dicts.
        """
        instruments = [
            {"symbol": "CLM6.NYMEX", "tradable": True, "status": "active"},
            {"symbol": "GCQ6.COMEX", "tradable": True, "status": "active"},
        ]
        client._fetch_instruments_rest = AsyncMock(return_value=instruments)
        results = []
        async for inst in client.subscribe_instruments():
            results.append(inst)
        assert len(results) == 2
        assert results[0]["symbol"] == "CLM6.NYMEX"
        assert results[1]["symbol"] == "GCQ6.COMEX"

    @pytest.mark.asyncio
    async def test_subscribe_instruments_empty(self, client: KrakenEquitiesExchangeClient) -> None:
        """Yield nothing when no instruments available.

        Given: _fetch_instruments_rest returns empty list,
        When: subscribe_instruments is iterated,
        Then: No items yielded.
        """
        client._fetch_instruments_rest = AsyncMock(return_value=[])
        results = []
        async for inst in client.subscribe_instruments():
            results.append(inst)
        assert len(results) == 0


class TestGetInstrumentsSync:
    """Tests for get_instruments_sync synchronous REST fetch."""

    def test_get_instruments_sync_filters_active(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Fetch and filter only active tradable contracts synchronously.

        Given: REST response with 3 contracts (2 active+tradable, 1 inactive),
        When: get_instruments_sync is called,
        Then: Returns only the 2 active tradable contracts.
        """
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "result": {
                "data": [
                    {"symbol": "CLM6.NYMEX", "tradable": True, "status": "active"},
                    {"symbol": "GCQ6.COMEX", "tradable": True, "status": "active"},
                    {"symbol": "CLK5.NYMEX", "tradable": False, "status": "inactive"},
                ]
            }
        }
        mock_response.raise_for_status = MagicMock()

        mock_http_client = MagicMock()
        mock_http_client.get.return_value = mock_response
        mock_http_client.__enter__ = MagicMock(return_value=mock_http_client)
        mock_http_client.__exit__ = MagicMock(return_value=False)

        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.httpx.Client",
            return_value=mock_http_client,
        ):
            result = client.get_instruments_sync()
        assert len(result) == 2
        assert result[0]["symbol"] == "CLM6.NYMEX"
        assert result[1]["symbol"] == "GCQ6.COMEX"

    def test_get_instruments_sync_empty_result(self, client: KrakenEquitiesExchangeClient) -> None:
        """Return empty list when no active contracts exist.

        Given: REST response with no active tradable contracts,
        When: get_instruments_sync is called,
        Then: Returns empty list.
        """
        mock_response = MagicMock()
        mock_response.json.return_value = {"result": {"data": []}}
        mock_response.raise_for_status = MagicMock()

        mock_http_client = MagicMock()
        mock_http_client.get.return_value = mock_response
        mock_http_client.__enter__ = MagicMock(return_value=mock_http_client)
        mock_http_client.__exit__ = MagicMock(return_value=False)

        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.httpx.Client",
            return_value=mock_http_client,
        ):
            result = client.get_instruments_sync()
        assert len(result) == 0


class TestTimeframeToInterval:
    """Tests for the ``_timeframe_to_interval`` pure helper."""

    @pytest.mark.parametrize(
        ("timeframe", "expected"),
        [
            ("1m", 1),
            ("5m", 5),
            ("15m", 15),
            ("30m", 30),
            ("1h", 60),
            ("1d", 1440),
        ],
    )
    def test_supported_timeframe_maps_to_minutes(self, timeframe: str, expected: int) -> None:
        """Accept every documented timeframe and return its minute-interval.

        Given: a timeframe string from the iapi-probed support set,
        When: ``_timeframe_to_interval`` is called,
        Then: the corresponding integer minute value is returned.
        """
        assert _timeframe_to_interval(timeframe) == expected

    def test_unknown_timeframe_raises_valueerror_with_allowed_list(self) -> None:
        """Reject unsupported timeframes with a message listing allowed values.

        Given: a timeframe string not in the support set,
        When: ``_timeframe_to_interval`` is called,
        Then: ``ValueError`` is raised and the message enumerates the accepted values.
        """
        with pytest.raises(ValueError, match="Unsupported Kraken Equities timeframe '2h'"):
            _timeframe_to_interval("2h")
        with pytest.raises(ValueError, match=r"allowed: .*1m.*"):
            _timeframe_to_interval("bogus")


class TestGetOhlcv:
    """Tests for ``KrakenEquitiesExchangeClient.get_ohlcv``.

    All tests patch ``native_to_kraken_equities_ws`` to bypass the DB-backed
    symbol mapper, and stub ``httpx.AsyncClient`` to return a canned payload
    mirroring the live iapi shape probed 2026-04-21.
    """

    @staticmethod
    def _mock_httpx(payload: dict[str, object]) -> MagicMock:
        """Build a MagicMock satisfying the async-context-manager + get protocol."""
        mock_response = MagicMock()
        mock_response.json.return_value = payload
        mock_response.raise_for_status = MagicMock()
        mock_http_client = AsyncMock()
        mock_http_client.get.return_value = mock_response
        mock_http_client.__aenter__ = AsyncMock(return_value=mock_http_client)
        mock_http_client.__aexit__ = AsyncMock(return_value=False)
        return mock_http_client

    @pytest.mark.asyncio
    async def test_returns_ordered_snapshots_from_happy_path_payload(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Map live iapi rows into OhlcvSnapshot entries, preserving order.

        Given: a canned iapi payload with two rows,
        When: ``get_ohlcv`` is called,
        Then: two ``OhlcvSnapshot`` entries are returned with coerced floats.
        """
        payload = {
            "result": {
                "data": [
                    {
                        "time": 1776556800,
                        "open": "26600.5",
                        "high": "26650.0",
                        "low": "26580.25",
                        "close": "26620.0",
                        "volume_wap": "26610",
                        "volume": "1234",
                        "count": 42,
                    },
                    {
                        "time": 1776643200,
                        "open": "26658.25",
                        "high": "26826.0",
                        "low": "26569.0",
                        "close": "26797.0",
                        "volume_wap": "26706.44",
                        "volume": "1638087",
                        "count": 1403613,
                    },
                ]
            }
        }
        mock_http = self._mock_httpx(payload)
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.httpx.AsyncClient",
                return_value=mock_http,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
                return_value="MNQM6.CME",
            ),
        ):
            snapshots = await client.get_ohlcv("MNQM6-CME", timeframe="1d")
        assert len(snapshots) == 2
        assert snapshots[0] == OhlcvSnapshot(
            timestamp=1776556800.0,
            open=26600.5,
            high=26650.0,
            low=26580.25,
            close=26620.0,
            volume=1234.0,
        )
        assert snapshots[-1].close == 26797.0
        call = mock_http.get.call_args
        assert call.args[0].endswith("/markets/MNQM6.CME/ticker/history")
        assert call.kwargs["params"] == {
            "interval": "1440",
            "delayed": "true",
            "asset_class": "futures_contract",
        }

    @pytest.mark.asyncio
    async def test_empty_payload_returns_empty_list(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Return an empty list when the payload has no data rows.

        Given: an iapi payload with ``result.data`` empty,
        When: ``get_ohlcv`` is called,
        Then: an empty list is returned.
        """
        mock_http = self._mock_httpx({"result": {"data": []}})
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.httpx.AsyncClient",
                return_value=mock_http,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
                return_value="MNQM6.CME",
            ),
        ):
            snapshots = await client.get_ohlcv("MNQM6-CME", timeframe="1h")
        assert snapshots == []

    @pytest.mark.asyncio
    async def test_application_error_envelope_raises(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Raise when the 200-response body signals application-layer failure.

        Given: an iapi payload with ``result=null`` + non-empty ``errors`` —
            the shape the endpoint returns for "Unknown method" or other
            upstream failures that still produce HTTP 200 (observed live
            2026-04-21 when the endpoint is called without the required
            Origin/Referer headers),
        When: ``get_ohlcv`` is called,
        Then: ``RuntimeError`` is raised so callers do not silently observe
            the failure as an empty candle window. Prevents historical
            backfill from skipping rows it should have retried.
        """
        mock_http = self._mock_httpx({"result": None, "errors": [{"msg": "transient"}]})
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.httpx.AsyncClient",
                return_value=mock_http,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
                return_value="MNQM6.CME",
            ),
            pytest.raises(RuntimeError, match="Kraken Equities ticker/history failure"),
        ):
            await client.get_ohlcv("MNQM6-CME")

    @pytest.mark.asyncio
    async def test_errors_present_with_result_still_raises(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Raise even when ``result`` is non-null if ``errors`` is non-empty.

        Given: an iapi payload with both ``result.data`` populated and a
            non-empty ``errors`` array (partial-failure shape),
        When: ``get_ohlcv`` is called,
        Then: ``RuntimeError`` is raised. Partial-failure responses are
            treated as outright failures rather than silently returning
            the partial data.
        """
        payload = {
            "result": {
                "data": [
                    {
                        "time": 1,
                        "open": "1",
                        "high": "1",
                        "low": "1",
                        "close": "1",
                        "volume_wap": "1",
                        "volume": "0",
                        "count": 0,
                    }
                ]
            },
            "errors": [{"msg": "one feed failed"}],
        }
        mock_http = self._mock_httpx(payload)
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.httpx.AsyncClient",
                return_value=mock_http,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
                return_value="MNQM6.CME",
            ),
            pytest.raises(RuntimeError, match="Kraken Equities ticker/history failure"),
        ):
            await client.get_ohlcv("MNQM6-CME")

    @pytest.mark.asyncio
    async def test_http_error_propagates(self, client: KrakenEquitiesExchangeClient) -> None:
        """Propagate ``httpx.HTTPStatusError`` so callers can distinguish failures.

        Given: an iapi response whose ``raise_for_status`` raises,
        When: ``get_ohlcv`` is called,
        Then: the exception escapes the method.
        """
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock(
            side_effect=httpx.HTTPStatusError(
                "503", request=MagicMock(), response=MagicMock(status_code=503)
            )
        )
        mock_http_client = AsyncMock()
        mock_http_client.get.return_value = mock_response
        mock_http_client.__aenter__ = AsyncMock(return_value=mock_http_client)
        mock_http_client.__aexit__ = AsyncMock(return_value=False)
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.httpx.AsyncClient",
                return_value=mock_http_client,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
                return_value="MNQM6.CME",
            ),
            pytest.raises(httpx.HTTPStatusError),
        ):
            await client.get_ohlcv("MNQM6-CME")

    @pytest.mark.asyncio
    async def test_since_filter_drops_older_candles(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Apply the ``since`` millisecond filter client-side.

        Given: a 3-row payload spanning 3 daily candles,
        When: ``get_ohlcv`` is called with ``since`` after the first row,
        Then: only rows at or after ``since`` remain.
        """
        payload = {
            "result": {
                "data": [
                    {
                        "time": 1000,
                        "open": "1",
                        "high": "1",
                        "low": "1",
                        "close": "1",
                        "volume_wap": "1",
                        "volume": "0",
                        "count": 0,
                    },
                    {
                        "time": 2000,
                        "open": "2",
                        "high": "2",
                        "low": "2",
                        "close": "2",
                        "volume_wap": "2",
                        "volume": "0",
                        "count": 0,
                    },
                    {
                        "time": 3000,
                        "open": "3",
                        "high": "3",
                        "low": "3",
                        "close": "3",
                        "volume_wap": "3",
                        "volume": "0",
                        "count": 0,
                    },
                ]
            }
        }
        mock_http = self._mock_httpx(payload)
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.httpx.AsyncClient",
                return_value=mock_http,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
                return_value="MNQM6.CME",
            ),
        ):
            snapshots = await client.get_ohlcv("MNQM6-CME", since=2_000_000)
        assert [s.timestamp for s in snapshots] == [2000.0, 3000.0]

    @pytest.mark.asyncio
    async def test_limit_truncates_from_tail(self, client: KrakenEquitiesExchangeClient) -> None:
        """Keep the newest ``limit`` rows when the server returns more.

        Given: a 3-row payload,
        When: ``get_ohlcv`` is called with ``limit=2``,
        Then: only the two most-recent (tail) rows are returned.
        """
        payload = {
            "result": {
                "data": [
                    {
                        "time": ts,
                        "open": "1",
                        "high": "1",
                        "low": "1",
                        "close": "1",
                        "volume_wap": "1",
                        "volume": "0",
                        "count": 0,
                    }
                    for ts in (1000, 2000, 3000)
                ]
            }
        }
        mock_http = self._mock_httpx(payload)
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.httpx.AsyncClient",
                return_value=mock_http,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
                return_value="MNQM6.CME",
            ),
        ):
            snapshots = await client.get_ohlcv("MNQM6-CME", limit=2)
        assert [s.timestamp for s in snapshots] == [2000.0, 3000.0]

    @pytest.mark.asyncio
    async def test_unparseable_row_skipped_not_raised(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Log-and-skip rows with missing or invalid fields.

        Given: a 2-row payload where the first row is missing ``open``,
        When: ``get_ohlcv`` is called,
        Then: only the second row is returned; no exception escapes.
        """
        payload = {
            "result": {
                "data": [
                    {"time": 1000, "close": "1"},
                    {
                        "time": 2000,
                        "open": "2",
                        "high": "2",
                        "low": "2",
                        "close": "2",
                        "volume_wap": "2",
                        "volume": "0",
                        "count": 0,
                    },
                ]
            }
        }
        mock_http = self._mock_httpx(payload)
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.httpx.AsyncClient",
                return_value=mock_http,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
                return_value="MNQM6.CME",
            ),
        ):
            snapshots = await client.get_ohlcv("MNQM6-CME")
        assert [s.timestamp for s in snapshots] == [2000.0]

    @pytest.mark.asyncio
    async def test_unknown_timeframe_raises_valueerror(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Reject unsupported timeframes before any HTTP call is made.

        Given: an unsupported timeframe string,
        When: ``get_ohlcv`` is called,
        Then: ``ValueError`` is raised and no request is issued.
        """
        with pytest.raises(ValueError, match="Unsupported Kraken Equities timeframe"):
            await client.get_ohlcv("MNQM6-CME", timeframe="bogus")

    @pytest.mark.asyncio
    async def test_unknown_symbol_raises_without_http_call(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Symbol-mapper errors are surfaced before any HTTP call is made.

        Given: the symbol mapper raises ValueError for an unmapped symbol,
        When: ``get_ohlcv`` is called,
        Then: the exception escapes and ``httpx.AsyncClient`` is never touched.
        """
        mock_http_client = AsyncMock()
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.httpx.AsyncClient",
                return_value=mock_http_client,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
                side_effect=ValueError("Unknown native symbol: BOGUS-XX"),
            ),
            pytest.raises(ValueError, match="Unknown native symbol"),
        ):
            await client.get_ohlcv("BOGUS-XX")
        mock_http_client.get.assert_not_called()


class TestWsEnvelopeDelayedPropagation:
    """Tests that the outer WS envelope's ``delayed`` flag reaches the adapter."""

    @pytest.mark.asyncio
    async def test_envelope_delayed_true_flows_into_adapter(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Forward ``message['delayed']=True`` as ``envelope_delayed=True``.

        Given: a WS frame with ``delayed=True`` at the envelope level,
        When: ``_on_ws_message`` dispatches the ticker item,
        Then: ``parse_kraken_equities_ticker`` is invoked with
              ``envelope_delayed=True``.
        """
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.parse_kraken_equities_ticker",
            return_value=TickerUpdate(
                symbol="MNQM6-CME",
                bid=0.0,
                bid_qty=0.0,
                ask=0.0,
                ask_qty=0.0,
                last=0.0,
                volume=0.0,
                vwap=0.0,
                low=0.0,
                high=0.0,
                change=0.0,
                change_pct=0.0,
                is_delayed=True,
            ),
        ) as parse_mock:
            msg = {
                "channel": "ticker",
                "type": "update",
                "delayed": True,
                "data": [{"symbol": "MNQM6.CME"}],
            }
            await client._on_ws_message(msg)
        parse_mock.assert_called_once()
        assert parse_mock.call_args.kwargs == {"envelope_delayed": True}

    @pytest.mark.asyncio
    async def test_missing_envelope_delayed_defaults_to_false(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Default envelope-level delayed to False when the key is absent.

        Given: a WS frame without the ``delayed`` key,
        When: ``_on_ws_message`` dispatches the ticker item,
        Then: the adapter receives ``envelope_delayed=False``.
        """
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.parse_kraken_equities_ticker",
            return_value=TickerUpdate(
                symbol="MNQM6-CME",
                bid=0.0,
                bid_qty=0.0,
                ask=0.0,
                ask_qty=0.0,
                last=0.0,
                volume=0.0,
                vwap=0.0,
                low=0.0,
                high=0.0,
                change=0.0,
                change_pct=0.0,
            ),
        ) as parse_mock:
            msg = {
                "channel": "ticker",
                "type": "update",
                "data": [{"symbol": "MNQM6.CME"}],
            }
            await client._on_ws_message(msg)
        assert parse_mock.call_args.kwargs == {"envelope_delayed": False}

    @pytest.mark.asyncio
    async def test_non_bool_envelope_delayed_coerces_to_false(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Reject non-bool ``delayed`` payloads and default to False.

        Given: a WS frame whose ``delayed`` key is a truthy string (``"false"``)
            under a hypothetical schema drift,
        When: ``_on_ws_message`` dispatches the ticker item,
        Then: the adapter receives ``envelope_delayed=False`` rather than
            ``bool("false") == True``. Prevents a permissive ``bool(...)``
            coercion from mis-flagging live ticks as delayed under an
            unexpected wire shape.
        """
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.parse_kraken_equities_ticker",
            return_value=TickerUpdate(
                symbol="MNQM6-CME",
                bid=0.0,
                bid_qty=0.0,
                ask=0.0,
                ask_qty=0.0,
                last=0.0,
                volume=0.0,
                vwap=0.0,
                low=0.0,
                high=0.0,
                change=0.0,
                change_pct=0.0,
            ),
        ) as parse_mock:
            msg = {
                "channel": "ticker",
                "type": "update",
                "delayed": "false",
                "data": [{"symbol": "MNQM6.CME"}],
            }
            await client._on_ws_message(msg)
        assert parse_mock.call_args.kwargs == {"envelope_delayed": False}
