"""Tests for Kraken Equities exchange client."""

import asyncio
from collections.abc import Generator
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.infrastructure.exchanges.contracts import InstrumentPairDescriptor
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate
from snapper.infrastructure.exchanges.implementations.kraken_equities import (
    KrakenEquitiesExchangeClient,
)
from snapper.infrastructure.exchanges.implementations.kraken_equities import _enqueue_or_drop_oldest


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
    async def test_ticker_snapshot_routed_to_queue(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Route ticker snapshot WS message to tick_queue.

        Given: WS message with channel=ticker and type=snapshot,
        When: _on_ws_message is called,
        Then: Parsed TickerUpdate is placed in _tick_queue.
        """
        msg = {
            "channel": "ticker",
            "type": "snapshot",
            "data": [{"symbol": "CLM6.NYMEX", "bid": 90.10}],
        }
        await client._on_ws_message(msg)
        assert not client._tick_queue.empty()
        update = client._tick_queue.get_nowait()
        assert update.symbol == "CLM6-NYMEX"

    @pytest.mark.asyncio
    async def test_ticker_update_routed_to_queue(
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
    async def test_trade_snapshot_routed_to_queue(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Route trade snapshot WS message to trade_queue.

        Given: WS message with channel=trade and type=snapshot,
        When: _on_ws_message is called,
        Then: Parsed TradeUpdate is placed in _trade_queue.
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
        assert not client._trade_queue.empty()
        update = client._trade_queue.get_nowait()
        assert update.symbol == "CLM6-NYMEX"

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
    async def test_get_ohlcv_raises(self, client: KrakenEquitiesExchangeClient) -> None:
        """get_ohlcv raises NotImplementedError.

        Given: Market-data-only client,
        When: get_ohlcv is called,
        Then: Raises NotImplementedError.
        """
        with pytest.raises(NotImplementedError, match="OHLCV not available"):
            await client.get_ohlcv("CLM6-NYMEX")

    def test_subscribe_candles_raises(self, client: KrakenEquitiesExchangeClient) -> None:
        """subscribe_candles raises NotImplementedError.

        Given: Market-data-only client,
        When: subscribe_candles is called,
        Then: Raises NotImplementedError.
        """
        with pytest.raises(NotImplementedError, match="candle subscription not implemented"):
            client.subscribe_candles(["CLM6-NYMEX"])

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
