"""Tests for Kraken Futures exchange client."""

import asyncio
from collections.abc import Generator
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.infrastructure.exchanges.contracts import InstrumentPairDescriptor
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)
from snapper.infrastructure.exchanges.implementations.kraken_futures import _enqueue_or_drop_oldest


@pytest.fixture()
def client() -> KrakenFuturesExchangeClient:
    """Create a KrakenFuturesExchangeClient instance for testing."""
    return KrakenFuturesExchangeClient(sandbox=True)


@pytest.fixture(autouse=True)
def _patch_ccxt() -> Generator[None]:
    """Patch ccxt.krakenfutures so no real HTTP calls are made."""
    mock_ccxt = MagicMock()
    mock_ccxt.load_markets = MagicMock(return_value={})
    mock_ccxt.fetch_ticker = MagicMock(
        return_value={
            "bid": 66500.0,
            "ask": 66510.0,
            "last": 66505.0,
            "baseVolume": 1234.0,
            "timestamp": 1640995200000,
        }
    )
    mock_ccxt.fetch_ohlcv = MagicMock(
        return_value=[
            [1640995200000, 50000.0, 50100.0, 49900.0, 50050.0, 100.0],
        ]
    )
    with patch("snapper.infrastructure.exchanges.implementations.kraken_futures.ccxt") as mock_mod:
        mock_mod.krakenfutures = MagicMock(return_value=mock_ccxt)
        yield


class TestClientInit:
    """Tests for client initialization."""

    def test_init_defaults(self) -> None:
        """Initialize with default parameters.

        Given: No arguments,
        When: KrakenFuturesExchangeClient is created,
        Then: Sandbox is False and queues are empty.
        """
        c = KrakenFuturesExchangeClient()
        assert c.sandbox is False
        assert c.exchange_name == "kraken_futures"
        assert c.supports_websocket_executions is False
        assert c._tick_queue.empty()
        assert c._trade_queue.empty()

    def test_init_sandbox(self) -> None:
        """Initialize with sandbox mode.

        Given: sandbox=True,
        When: KrakenFuturesExchangeClient is created,
        Then: Client is in sandbox mode.
        """
        c = KrakenFuturesExchangeClient(sandbox=True)
        assert c.sandbox is True


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
    async def test_connect_loads_markets(self, client: KrakenFuturesExchangeClient) -> None:
        """Connect loads CCXT markets and creates Market client.

        Given: Fresh client,
        When: connect() is called,
        Then: Markets are loaded and market_client is set.
        """
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.Market"
        ) as mock_market_cls:
            await client.connect()
            assert client._market_client is not None
            mock_market_cls.assert_called_once_with(sandbox=True)

    @pytest.mark.asyncio
    async def test_connect_failure_raises(self, client: KrakenFuturesExchangeClient) -> None:
        """Connect failure propagates exception.

        Given: CCXT load_markets raises,
        When: connect() is called,
        Then: Exception propagates.
        """
        client._ccxt_client.load_markets = MagicMock(side_effect=RuntimeError("network"))
        with pytest.raises(RuntimeError, match="network"):
            await client.connect()

    @pytest.mark.asyncio
    async def test_disconnect_without_ws(self, client: KrakenFuturesExchangeClient) -> None:
        """Disconnect when no WS client is active.

        Given: Client with no WS connection,
        When: disconnect() is called,
        Then: Completes without error.
        """
        await client.disconnect()
        assert client._ws_client is None

    @pytest.mark.asyncio
    async def test_disconnect_with_ws(self, client: KrakenFuturesExchangeClient) -> None:
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
    async def test_disconnect_ws_error_handled(self, client: KrakenFuturesExchangeClient) -> None:
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
                "snapper.infrastructure.exchanges.implementations.kraken_futures.parse_kraken_futures_ticker",
                return_value=TickerUpdate(
                    symbol="BTC-USD-PERP",
                    bid=66500.0,
                    bid_qty=50.0,
                    ask=66510.0,
                    ask_qty=30.0,
                    last=66505.0,
                    volume=1234.0,
                    vwap=0.0,
                    low=65500.0,
                    high=67000.0,
                    change=0.94,
                    change_pct=0.0,
                ),
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_futures.parse_kraken_futures_trade",
                return_value=TradeUpdate(
                    symbol="BTC-USD-PERP",
                    side="buy",
                    quantity=10.0,
                    price=66621.0,
                    ord_type="fill",
                    timestamp=MagicMock(),
                    trade_id="abc-123",
                ),
            ),
        ):
            yield

    @pytest.mark.asyncio
    async def test_ticker_message_routed_to_queue(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Route ticker WS message to tick_queue.

        Given: WS message with feed=ticker,
        When: _on_ws_message is called,
        Then: Parsed TickerUpdate is placed in _tick_queue.
        """
        msg = {"feed": "ticker", "product_id": "PF_XBTUSD", "bid": 66500.0}
        await client._on_ws_message(msg)
        assert not client._tick_queue.empty()
        update = client._tick_queue.get_nowait()
        assert update.symbol == "BTC-USD-PERP"

    @pytest.mark.asyncio
    async def test_ticker_lite_message_routed(self, client: KrakenFuturesExchangeClient) -> None:
        """Route ticker_lite WS message to tick_queue.

        Given: WS message with feed=ticker_lite,
        When: _on_ws_message is called,
        Then: Parsed TickerUpdate is placed in _tick_queue.
        """
        msg = {"feed": "ticker_lite", "product_id": "PF_XBTUSD", "last": 66505.0}
        await client._on_ws_message(msg)
        assert not client._tick_queue.empty()

    @pytest.mark.asyncio
    async def test_ticker_message_with_symbol_routed(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Route ticker WS message without product_id backfill.

        Given: WS ticker message that already includes symbol,
        When: _on_ws_message is called,
        Then: Parsed TickerUpdate is placed in _tick_queue without rewriting symbol.
        """
        expected_update = TickerUpdate(
            symbol="BTC-USD-PERP",
            bid=66500.0,
            bid_qty=50.0,
            ask=66510.0,
            ask_qty=30.0,
            last=66505.0,
            volume=1234.0,
            vwap=0.0,
            low=65500.0,
            high=67000.0,
            change=0.94,
            change_pct=0.0,
        )
        msg = {"feed": "ticker", "symbol": "BTC-USD-PERP", "bid": 66500.0}
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.parse_kraken_futures_ticker",
            return_value=expected_update,
        ) as mock_parse:
            await client._on_ws_message(msg)
        mock_parse.assert_called_once_with(msg)
        assert not client._tick_queue.empty()
        update = client._tick_queue.get_nowait()
        assert update == expected_update

    @pytest.mark.asyncio
    async def test_trade_message_routed_to_queue(self, client: KrakenFuturesExchangeClient) -> None:
        """Route trade WS message to trade_queue.

        Given: WS message with feed=trade and nested trades,
        When: _on_ws_message is called,
        Then: Parsed TradeUpdate is placed in _trade_queue.
        """
        msg = {
            "feed": "trade",
            "product_id": "PI_XBTUSD",
            "trades": [{"time": 1640995200000, "qty": 10.0, "price": 66621.0, "side": "buy"}],
        }
        await client._on_ws_message(msg)
        assert not client._trade_queue.empty()
        update = client._trade_queue.get_nowait()
        assert update.symbol == "BTC-USD-PERP"

    @pytest.mark.asyncio
    async def test_heartbeat_ignored(self, client: KrakenFuturesExchangeClient) -> None:
        """Ignore heartbeat messages.

        Given: WS heartbeat message,
        When: _on_ws_message is called,
        Then: No items in any queue.
        """
        await client._on_ws_message({"feed": "heartbeat"})
        assert client._tick_queue.empty()
        assert client._trade_queue.empty()

    @pytest.mark.asyncio
    async def test_event_message_ignored(self, client: KrakenFuturesExchangeClient) -> None:
        """Ignore subscription event messages.

        Given: WS subscription ACK message,
        When: _on_ws_message is called,
        Then: No items in any queue.
        """
        await client._on_ws_message({"event": "subscribed", "feed": "ticker"})
        assert client._tick_queue.empty()

    @pytest.mark.asyncio
    async def test_unparseable_ticker_skipped(self, client: KrakenFuturesExchangeClient) -> None:
        """Skip ticker messages that fail parsing.

        Given: WS ticker message that causes ValueError in parser,
        When: _on_ws_message is called,
        Then: No items in tick_queue and no exception raised.
        """
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.parse_kraken_futures_ticker",
            side_effect=ValueError("parse error"),
        ):
            await client._on_ws_message({"feed": "ticker", "product_id": "INVALID"})
        assert client._tick_queue.empty()

    @pytest.mark.asyncio
    async def test_unparseable_trade_skipped(self, client: KrakenFuturesExchangeClient) -> None:
        """Skip trade messages that fail parsing.

        Given: WS trade message that causes ValueError in parser,
        When: _on_ws_message is called,
        Then: No items in trade_queue and no exception raised.
        """
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.parse_kraken_futures_trade",
            side_effect=ValueError("parse error"),
        ):
            msg = {
                "feed": "trade",
                "product_id": "INVALID",
                "trades": [{"time": 0, "qty": 1.0, "price": 100.0, "side": "buy"}],
            }
            await client._on_ws_message(msg)
        assert client._trade_queue.empty()


class TestRestMethods:
    """Tests for REST API methods."""

    @pytest.mark.asyncio
    async def test_get_ticker(self, client: KrakenFuturesExchangeClient) -> None:
        """Fetch ticker via CCXT.

        Given: CCXT client returns ticker data,
        When: get_ticker is called,
        Then: Returns TickerSnapshot with correct values.
        """
        result = await client.get_ticker("BTC/USD:USD")
        assert result.bid == pytest.approx(66500.0)
        assert result.ask == pytest.approx(66510.0)
        assert result.last == pytest.approx(66505.0)

    @pytest.mark.asyncio
    async def test_get_ohlcv(self, client: KrakenFuturesExchangeClient) -> None:
        """Fetch OHLCV candles via CCXT.

        Given: CCXT client returns candle data,
        When: get_ohlcv is called,
        Then: Returns list of OhlcvSnapshot with correct values.
        """
        result = await client.get_ohlcv("BTC/USD:USD", "1m")
        assert len(result) == 1
        assert result[0].timestamp == pytest.approx(1640995200.0)
        assert result[0].open == pytest.approx(50000.0)
        assert result[0].close == pytest.approx(50050.0)


class TestNotImplementedMethods:
    """Tests for Phase 1 stub methods."""

    @pytest.mark.asyncio
    async def test_create_order_raises(self, client: KrakenFuturesExchangeClient) -> None:
        """create_order raises NotImplementedError.

        Given: Phase 1 client,
        When: create_order is called,
        Then: Raises NotImplementedError.
        """
        with pytest.raises(NotImplementedError, match="Phase 1"):
            await client.create_order(MagicMock())

    @pytest.mark.asyncio
    async def test_cancel_order_raises(self, client: KrakenFuturesExchangeClient) -> None:
        """cancel_order raises NotImplementedError.

        Given: Phase 1 client,
        When: cancel_order is called,
        Then: Raises NotImplementedError.
        """
        with pytest.raises(NotImplementedError, match="Phase 1"):
            await client.cancel_order("123")

    @pytest.mark.asyncio
    async def test_get_order_raises(self, client: KrakenFuturesExchangeClient) -> None:
        """get_order raises NotImplementedError.

        Given: Phase 1 client,
        When: get_order is called,
        Then: Raises NotImplementedError.
        """
        with pytest.raises(NotImplementedError, match="Phase 1"):
            await client.get_order("123")

    @pytest.mark.asyncio
    async def test_get_orders_raises(self, client: KrakenFuturesExchangeClient) -> None:
        """get_orders raises NotImplementedError.

        Given: Phase 1 client,
        When: get_orders is called,
        Then: Raises NotImplementedError.
        """
        with pytest.raises(NotImplementedError, match="Phase 1"):
            await client.get_orders()

    @pytest.mark.asyncio
    async def test_get_balance_raises(self, client: KrakenFuturesExchangeClient) -> None:
        """get_balance raises NotImplementedError.

        Given: Phase 1 client,
        When: get_balance is called,
        Then: Raises NotImplementedError.
        """
        with pytest.raises(NotImplementedError, match="Phase 1"):
            await client.get_balance()

    def test_subscribe_candles_raises(self, client: KrakenFuturesExchangeClient) -> None:
        """subscribe_candles raises NotImplementedError.

        Given: Phase 1 client,
        When: subscribe_candles is called,
        Then: Raises NotImplementedError.
        """
        with pytest.raises(NotImplementedError, match="no WS candle feed"):
            client.subscribe_candles(["PF_XBTUSD"])

    def test_subscribe_executions_raises(self, client: KrakenFuturesExchangeClient) -> None:
        """subscribe_executions raises NotImplementedError.

        Given: Phase 1 client,
        When: subscribe_executions is called,
        Then: Raises NotImplementedError.
        """
        with pytest.raises(NotImplementedError, match="Phase 1"):
            client.subscribe_executions()


class TestSubscribeInstruments:
    """Tests for instrument subscription (REST-based)."""

    @pytest.mark.asyncio
    async def test_subscribe_instruments_yields_all(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Yield all instruments from REST API.

        Given: Market client returns 2 instruments,
        When: subscribe_instruments is iterated,
        Then: Yields 2 raw instrument dicts.
        """
        mock_market = MagicMock()
        mock_market.get_instruments.return_value = {
            "instruments": [
                {"symbol": "PI_XBTUSD", "type": "futures_inverse"},
                {"symbol": "PF_ETHUSD", "type": "futures_vanilla"},
            ]
        }
        client._market_client = mock_market
        results = []
        async for inst in client.subscribe_instruments():
            results.append(inst)
        assert len(results) == 2
        assert results[0]["symbol"] == "PI_XBTUSD"

    def test_get_instruments_sync(self, client: KrakenFuturesExchangeClient) -> None:
        """Fetch instruments synchronously.

        Given: Market client returns instruments,
        When: get_instruments_sync is called,
        Then: Returns list of raw instrument dicts.
        """
        mock_market = MagicMock()
        mock_market.get_instruments.return_value = {"instruments": [{"symbol": "PI_XBTUSD"}]}
        client._market_client = mock_market
        results = client.get_instruments_sync()
        assert len(results) == 1

    def test_get_parsed_instrument(self, client: KrakenFuturesExchangeClient) -> None:
        """Parse raw instrument dict into descriptor.

        Given: Raw instrument dict,
        When: get_parsed_instrument is called,
        Then: Returns InstrumentPairDescriptor.
        """
        raw = {
            "symbol": "PI_XBTUSD",
            "type": "futures_inverse",
            "underlying": "rr_xbtusd",
            "tickSize": 0.5,
            "contractSize": 1,
            "tradeable": True,
            "base": "BTC",
            "quote": "USD",
        }
        result = client.get_parsed_instrument(raw)
        assert isinstance(result, InstrumentPairDescriptor)
        assert result.symbol == "PI_XBTUSD"
        assert result.base == "BTC"


class TestEnsureWsConnected:
    """Tests for WebSocket connection management."""

    @pytest.mark.asyncio
    async def test_creates_ws_on_first_call(self, client: KrakenFuturesExchangeClient) -> None:
        """Create WS client on first call.

        Given: No WS client,
        When: _ensure_ws_connected is called,
        Then: WS client is created and started.
        """
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.FuturesWSClient"
        ) as mock_cls:
            mock_ws = AsyncMock()
            mock_cls.return_value = mock_ws
            await client._ensure_ws_connected()
            mock_cls.assert_called_once()
            mock_ws.start.assert_awaited_once()
            assert client._ws_client is mock_ws

    @pytest.mark.asyncio
    async def test_noop_when_already_connected(self, client: KrakenFuturesExchangeClient) -> None:
        """Skip creation when already connected.

        Given: WS client already exists,
        When: _ensure_ws_connected is called,
        Then: No new client is created.
        """
        mock_ws = AsyncMock()
        client._ws_client = mock_ws
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.FuturesWSClient"
        ) as mock_cls:
            await client._ensure_ws_connected()
            mock_cls.assert_not_called()


_SYMBOL_MAP = {"BTC-USD-PERP": "PF_XBTUSD", "BTC-USD-PERP-INV": "PI_XBTUSD"}


@pytest.fixture(autouse=True, scope="class")
def _patch_symbol_conversion() -> Generator[None]:
    """Patch native_to_kraken_futures_ws for subscribe tests."""
    with patch(
        "snapper.infrastructure.exchanges.implementations.kraken_futures.native_to_kraken_futures_ws",
        side_effect=lambda s: _SYMBOL_MAP.get(s, s),
    ):
        yield


class TestSubscribeTicks:
    """Tests for subscribe_ticks async iterator."""

    @pytest.mark.asyncio
    async def test_subscribe_ticks_yields_from_queue(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Yield ticker updates from internal queue.

        Given: WS client connected and tick_queue has an item,
        When: subscribe_ticks iterator is consumed,
        Then: Yields the queued TickerUpdate.
        """
        mock_ws = AsyncMock(exception_occur=False)
        client._ws_client = mock_ws
        ticker = TickerUpdate(
            symbol="BTC-USD-PERP",
            bid=66500.0,
            bid_qty=50.0,
            ask=66510.0,
            ask_qty=30.0,
            last=66505.0,
            volume=1234.0,
            vwap=0.0,
            low=65500.0,
            high=67000.0,
            change=0.94,
            change_pct=0.0,
        )
        client._tick_queue.put_nowait(ticker)

        items = []
        async for item in client.subscribe_ticks(["BTC-USD-PERP"]):
            items.append(item)
            break
        assert len(items) == 1
        assert items[0].symbol == "BTC-USD-PERP"
        mock_ws.subscribe.assert_awaited_once_with(feed="ticker", products=["PF_XBTUSD"])

    @pytest.mark.asyncio
    async def test_subscribe_ticks_handles_timeout(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Handle queue timeout when no tickers available.

        Given: WS client connected but tick_queue is empty initially,
        When: subscribe_ticks iterator is consumed,
        Then: Loops through timeout, then yields when item arrives.
        """
        mock_ws = AsyncMock(exception_occur=False)
        client._ws_client = mock_ws
        ticker = TickerUpdate(
            symbol="BTC-USD-PERP",
            bid=66500.0,
            bid_qty=50.0,
            ask=66510.0,
            ask_qty=30.0,
            last=66505.0,
            volume=1234.0,
            vwap=0.0,
            low=65500.0,
            high=67000.0,
            change=0.94,
            change_pct=0.0,
        )

        async def delayed_put() -> None:
            await asyncio.sleep(0.15)
            client._tick_queue.put_nowait(ticker)

        asyncio.create_task(delayed_put())
        items = []
        async for item in client.subscribe_ticks(["BTC-USD-PERP"]):
            items.append(item)
            break
        assert len(items) == 1

    @pytest.mark.asyncio
    async def test_subscribe_ticks_raises_on_ws_failure(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Raise ConnectionError when WS dies mid-subscription.

        Given: WS client has exception_occur=True,
        When: subscribe_ticks is iterated,
        Then: Raises ConnectionError and unsubscribes.
        """
        mock_ws = AsyncMock()
        mock_ws.exception_occur = True
        client._ws_client = mock_ws
        with pytest.raises(ConnectionError, match="connection lost"):
            async for _tick in client.subscribe_ticks(["BTC-USD-PERP"]):
                pytest.fail("Should not yield")
        mock_ws.unsubscribe.assert_awaited_once()
        assert client._ws_client is None

    @pytest.mark.asyncio
    async def test_subscribe_ticks_unsubscribes_on_break(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Unsubscribe when consumer breaks out of iterator.

        Given: WS client connected, consumer breaks after first item,
        When: Iterator cleanup runs,
        Then: Unsubscribe is called.
        """
        mock_ws = AsyncMock()
        mock_ws.exception_occur = False
        client._ws_client = mock_ws
        ticker = TickerUpdate(
            symbol="BTC-USD-PERP",
            bid=66500.0,
            bid_qty=50.0,
            ask=66510.0,
            ask_qty=30.0,
            last=66505.0,
            volume=1234.0,
            vwap=0.0,
            low=65500.0,
            high=67000.0,
            change=0.94,
            change_pct=0.0,
        )
        client._tick_queue.put_nowait(ticker)
        gen = client.subscribe_ticks(["BTC-USD-PERP"])
        async for _ in gen:
            break
        await gen.aclose()
        mock_ws.unsubscribe.assert_awaited_once_with(feed="ticker", products=["PF_XBTUSD"])

    @pytest.mark.asyncio
    async def test_subscribe_ticks_unsubscribe_failure_handled(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Handle unsubscribe failure gracefully on cleanup.

        Given: WS client where unsubscribe raises,
        When: Iterator is closed,
        Then: No exception propagated from finally block.
        """
        mock_ws = AsyncMock(exception_occur=False)
        mock_ws.unsubscribe.side_effect = RuntimeError("unsub failed")
        client._ws_client = mock_ws
        ticker = TickerUpdate(
            symbol="BTC-USD-PERP",
            bid=66500.0,
            bid_qty=50.0,
            ask=66510.0,
            ask_qty=30.0,
            last=66505.0,
            volume=1234.0,
            vwap=0.0,
            low=65500.0,
            high=67000.0,
            change=0.94,
            change_pct=0.0,
        )
        client._tick_queue.put_nowait(ticker)
        gen = client.subscribe_ticks(["BTC-USD-PERP"])
        async for _ in gen:
            break
        await gen.aclose()

    @pytest.mark.asyncio
    async def test_subscribe_ticks_raises_when_ws_none_after_connect(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Raise RuntimeError if _ws_client is None after _ensure_ws_connected.

        Given: _ensure_ws_connected is a no-op that does not set _ws_client,
        When: subscribe_ticks is called,
        Then: RuntimeError is raised.
        """
        client._ensure_ws_connected = AsyncMock()
        with pytest.raises(RuntimeError, match="WebSocket client not connected"):
            await anext(aiter(client.subscribe_ticks(["BTC-USD-PERP"])))


class TestSubscribeTrades:
    """Tests for subscribe_trades async iterator."""

    @pytest.mark.asyncio
    async def test_subscribe_trades_yields_from_queue(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Yield trade updates from internal queue.

        Given: WS client connected and trade_queue has an item,
        When: subscribe_trades iterator is consumed,
        Then: Yields the queued TradeUpdate.
        """
        mock_ws = AsyncMock(exception_occur=False)
        client._ws_client = mock_ws
        trade = TradeUpdate(
            symbol="BTC-USD-PERP",
            side="buy",
            quantity=10.0,
            price=66621.0,
            ord_type="fill",
            timestamp=MagicMock(),
            trade_id="abc-123",
        )
        client._trade_queue.put_nowait(trade)

        items = []
        async for item in client.subscribe_trades(["BTC-USD-PERP-INV"]):
            items.append(item)
            break
        assert len(items) == 1
        assert items[0].symbol == "BTC-USD-PERP"
        mock_ws.subscribe.assert_awaited_once_with(feed="trade", products=["PI_XBTUSD"])

    @pytest.mark.asyncio
    async def test_subscribe_trades_handles_timeout(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Handle queue timeout when no trades available.

        Given: WS client connected but trade_queue is empty initially,
        When: subscribe_trades iterator is consumed,
        Then: Loops through timeout, then yields when item arrives.
        """
        mock_ws = AsyncMock(exception_occur=False)
        client._ws_client = mock_ws
        trade = TradeUpdate(
            symbol="BTC-USD-PERP",
            side="buy",
            quantity=10.0,
            price=66621.0,
            ord_type="fill",
            timestamp=MagicMock(),
            trade_id="abc-123",
        )

        async def delayed_put() -> None:
            await asyncio.sleep(0.15)
            client._trade_queue.put_nowait(trade)

        asyncio.create_task(delayed_put())
        items = []
        async for item in client.subscribe_trades(["BTC-USD-PERP-INV"]):
            items.append(item)
            break
        assert len(items) == 1

    @pytest.mark.asyncio
    async def test_subscribe_trades_raises_on_ws_failure(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Raise ConnectionError when WS dies mid-trade-subscription.

        Given: WS client has exception_occur=True,
        When: subscribe_trades is iterated,
        Then: Raises ConnectionError and unsubscribes.
        """
        mock_ws = AsyncMock()
        mock_ws.exception_occur = True
        client._ws_client = mock_ws
        with pytest.raises(ConnectionError, match="connection lost"):
            async for _trade in client.subscribe_trades(["BTC-USD-PERP-INV"]):
                pytest.fail("Should not yield")
        mock_ws.unsubscribe.assert_awaited_once()
        assert client._ws_client is None

    @pytest.mark.asyncio
    async def test_subscribe_trades_unsubscribes_on_break(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Unsubscribe when consumer breaks out of trade iterator.

        Given: WS client connected, consumer breaks after first item,
        When: Iterator cleanup runs,
        Then: Unsubscribe is called.
        """
        mock_ws = AsyncMock()
        mock_ws.exception_occur = False
        client._ws_client = mock_ws
        trade = TradeUpdate(
            symbol="BTC-USD-PERP",
            side="buy",
            quantity=10.0,
            price=66621.0,
            ord_type="fill",
            timestamp=MagicMock(),
            trade_id="abc-123",
        )
        client._trade_queue.put_nowait(trade)
        gen = client.subscribe_trades(["BTC-USD-PERP-INV"])
        async for _ in gen:
            break
        await gen.aclose()
        mock_ws.unsubscribe.assert_awaited_once_with(feed="trade", products=["PI_XBTUSD"])

    @pytest.mark.asyncio
    async def test_subscribe_trades_unsubscribe_failure_handled(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Handle unsubscribe failure gracefully on trade cleanup.

        Given: WS client where unsubscribe raises,
        When: Trade iterator is closed,
        Then: No exception propagated from finally block.
        """
        mock_ws = AsyncMock(exception_occur=False)
        mock_ws.unsubscribe.side_effect = RuntimeError("unsub failed")
        client._ws_client = mock_ws
        trade = TradeUpdate(
            symbol="BTC-USD-PERP",
            side="buy",
            quantity=10.0,
            price=66621.0,
            ord_type="fill",
            timestamp=MagicMock(),
            trade_id="abc-123",
        )
        client._trade_queue.put_nowait(trade)
        gen = client.subscribe_trades(["BTC-USD-PERP-INV"])
        async for _ in gen:
            break
        await gen.aclose()

    @pytest.mark.asyncio
    async def test_subscribe_trades_raises_when_ws_none_after_connect(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Raise RuntimeError if _ws_client is None after _ensure_ws_connected.

        Given: _ensure_ws_connected is a no-op that does not set _ws_client,
        When: subscribe_trades is called,
        Then: RuntimeError is raised.
        """
        client._ensure_ws_connected = AsyncMock()
        with pytest.raises(RuntimeError, match="WebSocket client not connected"):
            await anext(aiter(client.subscribe_trades(["BTC-USD-PERP"])))


class TestSubscribeInstrumentsInit:
    """Tests for subscribe_instruments when market_client is None."""

    @pytest.mark.asyncio
    async def test_creates_market_client_if_none(self, client: KrakenFuturesExchangeClient) -> None:
        """Create Market client if not yet initialized.

        Given: Client with _market_client=None,
        When: subscribe_instruments is iterated,
        Then: Market client is created and instruments yielded.
        """
        client._market_client = None
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.Market"
        ) as mock_cls:
            mock_market = MagicMock()
            mock_market.get_instruments.return_value = {"instruments": [{"symbol": "PI_XBTUSD"}]}
            mock_cls.return_value = mock_market
            results = []
            async for inst in client.subscribe_instruments():
                results.append(inst)
            assert len(results) == 1
            mock_cls.assert_called_once_with(sandbox=True)

    def test_get_instruments_sync_creates_market_client(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Create Market client on sync fetch if None.

        Given: Client with _market_client=None,
        When: get_instruments_sync is called,
        Then: Market client is created.
        """
        client._market_client = None
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.Market"
        ) as mock_cls:
            mock_market = MagicMock()
            mock_market.get_instruments.return_value = {"instruments": []}
            mock_cls.return_value = mock_market
            client.get_instruments_sync()
            mock_cls.assert_called_once_with(sandbox=True)
