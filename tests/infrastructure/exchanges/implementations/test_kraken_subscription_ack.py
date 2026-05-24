"""Tests for Kraken Spot subscription ACK health tracking."""

from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import call
from unittest.mock import patch

import pytest
from kraken.spot import SpotWSClient

from snapper.infrastructure.exchanges._subscription_request import SubscriptionRequest
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.implementations.kraken import KrakenExchangeClient


def _ws_mock(client: KrakenExchangeClient) -> AsyncMock:
    """Return the mocked Spot websocket.

    Args:
        client: Spot exchange client under test.

    Returns:
        Mocked Spot websocket assigned to the client.

    Raises:
        AssertionError: If the test fixture did not install an AsyncMock.
    """
    ws_client = client._ws_client
    assert isinstance(ws_client, AsyncMock)
    return ws_client


class TestKrakenSpotSubscriptionAck:
    """Tests for Spot subscribe ACK routing."""

    @pytest.fixture()
    def client(self) -> KrakenExchangeClient:
        """Create a Spot client with tracker spy."""
        client = KrakenExchangeClient(api_key="k", api_secret="s")
        client._health_tracker = MagicMock()
        return client

    @pytest.mark.asyncio
    async def test_ticker_success_ack_marks_confirmed(self, client: KrakenExchangeClient) -> None:
        """Ticker ACK confirms ticker subscription.

        Given: A successful ticker subscribe ACK,
        When: _on_message receives it,
        Then: The tracker marks the wire symbol confirmed.
        """
        await client._on_message(
            {
                "method": "subscribe",
                "success": True,
                "result": {"channel": "ticker", "symbol": "BTC/USD"},
            }
        )
        client._health_tracker.mark_confirmed.assert_called_once_with("ticker", "BTC/USD")

    @pytest.mark.asyncio
    async def test_ticker_failure_ack_marks_failed(self, client: KrakenExchangeClient) -> None:
        """Ticker ACK failure marks ticker subscription failed.

        Given: A failed ticker subscribe ACK,
        When: _on_message receives it,
        Then: The tracker records the failure.
        """
        await client._on_message(
            {
                "method": "subscribe",
                "success": False,
                "error": "bad symbol",
                "result": {"channel": "ticker", "symbol": "BAD/USD"},
            }
        )
        client._health_tracker.mark_failed.assert_called_once_with(
            "ticker", "BAD/USD", "bad symbol"
        )

    @pytest.mark.asyncio
    async def test_trade_already_subscribed_marks_confirmed(
        self, client: KrakenExchangeClient
    ) -> None:
        """Idempotent trade ACK confirms trade subscription.

        Given: A trade ACK with Already subscribed,
        When: _on_message receives it,
        Then: The tracker treats it as confirmed.
        """
        await client._on_message(
            {
                "method": "subscribe",
                "success": False,
                "error": "Already subscribed",
                "result": {"channel": "trade", "symbol": "ETH/USD"},
            }
        )
        client._health_tracker.mark_confirmed.assert_called_once_with("trade", "ETH/USD")

    @pytest.mark.asyncio
    async def test_trade_failure_ack_marks_failed(self, client: KrakenExchangeClient) -> None:
        """Trade ACK failure marks trade subscription failed.

        Given: A failed trade subscribe ACK,
        When: _on_message receives it,
        Then: The tracker records the failure.
        """
        await client._on_message(
            {
                "method": "subscribe",
                "success": False,
                "error": "denied",
                "result": {"channel": "trade", "symbol": "ETH/USD"},
            }
        )
        client._health_tracker.mark_failed.assert_called_once_with("trade", "ETH/USD", "denied")

    @pytest.mark.asyncio
    async def test_ohlc_success_ack_uses_interval_channel_key(
        self, client: KrakenExchangeClient
    ) -> None:
        """OHLC ACK confirms parameterized channel key.

        Given: A successful 5m OHLC subscribe ACK,
        When: _on_message receives it,
        Then: The tracker confirms ohlc:5m for the wire symbol.
        """
        await client._on_message(
            {
                "method": "subscribe",
                "success": True,
                "result": {"channel": "ohlc", "symbol": "BTC/USD", "interval": 5},
            }
        )
        client._health_tracker.mark_confirmed.assert_called_once_with("ohlc:5m", "BTC/USD")

    @pytest.mark.asyncio
    async def test_ohlc_failure_ack_marks_failed(self, client: KrakenExchangeClient) -> None:
        """OHLC ACK failure marks parameterized channel failed.

        Given: A failed 1h OHLC subscribe ACK,
        When: _on_message receives it,
        Then: The tracker records failure under ohlc:1h.
        """
        await client._on_message(
            {
                "method": "subscribe",
                "success": False,
                "error": "bad interval",
                "result": {"channel": "ohlc", "symbol": "BTC/USD", "interval": 60},
            }
        )
        client._health_tracker.mark_failed.assert_called_once_with(
            "ohlc:1h", "BTC/USD", "bad interval"
        )

    @pytest.mark.asyncio
    async def test_execution_ack_does_not_touch_tracker(self, client: KrakenExchangeClient) -> None:
        """Execution ACK is account-wide and excluded.

        Given: A successful executions subscribe ACK,
        When: _on_message receives it,
        Then: No tracker method is called.
        """
        await client._on_message(
            {
                "method": "subscribe",
                "success": True,
                "result": {"channel": "executions", "snap_orders": True, "snap_trades": True},
            }
        )
        client._health_tracker.assert_not_called()
        client._health_tracker.mark_confirmed.assert_not_called()
        client._health_tracker.mark_failed.assert_not_called()

    def test_trade_ack_without_symbol_does_not_touch_tracker(
        self, client: KrakenExchangeClient
    ) -> None:
        """Malformed trade ACK is ignored.

        Given: A trade ACK result without symbol,
        When: _handle_trade_subscription_ack receives it,
        Then: No tracker method is called.
        """
        message = {
            "method": "subscribe",
            "success": True,
            "result": {"channel": "trade"},
        }
        client._handle_trade_subscription_ack(message, {"channel": "trade"})
        client._health_tracker.mark_confirmed.assert_not_called()

    def test_ticker_ack_with_mismatched_result_dict_does_not_touch_tracker(
        self, client: KrakenExchangeClient
    ) -> None:
        """Ticker handler validates dispatcher result attribution.

        Given: A valid ticker ACK message but a result dict without symbol,
        When: _handle_ticker_subscription_ack is called directly,
        Then: No tracker method is called.
        """
        message = {
            "method": "subscribe",
            "success": True,
            "result": {"channel": "ticker", "symbol": "BTC/USD"},
        }
        client._handle_ticker_subscription_ack(message, {"channel": "ticker"})
        client._health_tracker.mark_confirmed.assert_not_called()

    def test_ticker_ack_validation_error_does_not_touch_tracker(
        self, client: KrakenExchangeClient
    ) -> None:
        """Malformed ticker ACK schema is ignored.

        Given: A ticker ACK message with an invalid result symbol,
        When: _handle_ticker_subscription_ack validates it,
        Then: No tracker method is called.
        """
        message = {
            "method": "subscribe",
            "success": True,
            "result": {"channel": "ticker", "symbol": 3},
        }
        client._handle_ticker_subscription_ack(
            message,
            {"channel": "ticker", "symbol": "BTC/USD"},
        )
        client._health_tracker.mark_confirmed.assert_not_called()

    def test_ohlc_ack_without_interval_does_not_touch_tracker(
        self, client: KrakenExchangeClient
    ) -> None:
        """OHLC ACK without interval is ignored.

        Given: A successful OHLC ACK missing interval,
        When: _handle_ohlc_subscription_ack receives it,
        Then: No tracker method is called.
        """
        client._handle_ohlc_subscription_ack(
            {
                "method": "subscribe",
                "success": True,
                "result": {"channel": "ohlc", "symbol": "BTC/USD"},
            }
        )
        client._health_tracker.mark_confirmed.assert_not_called()

    @pytest.mark.asyncio
    async def test_unknown_ack_channel_is_ignored(self, client: KrakenExchangeClient) -> None:
        """Unknown ACK channel is ignored.

        Given: A subscribe ACK for an unsupported channel,
        When: _on_message receives it,
        Then: No tracker method is called.
        """
        await client._on_message(
            {
                "method": "subscribe",
                "success": True,
                "result": {"channel": "book", "symbol": "BTC/USD"},
            }
        )
        client._health_tracker.mark_confirmed.assert_not_called()


class TestKrakenSpotSubscriptionRetry:
    """Tests for Spot one-symbol retry subscribes."""

    @pytest.fixture()
    def client(self) -> KrakenExchangeClient:
        """Create a Spot client with mocked websocket."""
        client = KrakenExchangeClient(api_key="k", api_secret="s")
        client._ws_client = AsyncMock(spec=SpotWSClient)
        return client

    @pytest.mark.asyncio
    async def test_retry_ticker_uses_single_symbol_params(
        self, client: KrakenExchangeClient
    ) -> None:
        """Ticker retry subscribes exactly one symbol.

        Given: A connected Spot websocket,
        When: _retry_subscribe retries ticker,
        Then: The SDK subscribe call receives one ticker symbol.
        """
        await client._retry_subscribe("ticker", "BTC/USD")
        assert _ws_mock(client).subscribe.await_args.kwargs["params"] == {
            "channel": "ticker",
            "symbol": ["BTC/USD"],
            "snapshot": True,
        }

    @pytest.mark.asyncio
    async def test_retry_trade_uses_trade_channel(self, client: KrakenExchangeClient) -> None:
        """Trade retry subscribes the trade channel.

        Given: A connected Spot websocket,
        When: _retry_subscribe retries trade,
        Then: The SDK subscribe call receives trade params.
        """
        await client._retry_subscribe("trade", "ETH/USD")
        assert _ws_mock(client).subscribe.await_args.kwargs["params"]["channel"] == "trade"

    @pytest.mark.asyncio
    async def test_retry_ohlc_parses_interval_label(self, client: KrakenExchangeClient) -> None:
        """OHLC retry recovers interval from channel key.

        Given: A connected Spot websocket,
        When: _retry_subscribe retries ohlc:4h,
        Then: The SDK subscribe call uses interval 240.
        """
        await client._retry_subscribe("ohlc:4h", "BTC/USD")
        assert _ws_mock(client).subscribe.await_args.kwargs["params"]["interval"] == 240

    @pytest.mark.asyncio
    async def test_retry_rejects_wildcard_ticker(self, client: KrakenExchangeClient) -> None:
        """Ticker retry rejects wildcard symbol.

        Given: A connected Spot websocket,
        When: _retry_subscribe retries ticker wildcard,
        Then: ValueError is raised.
        """
        with pytest.raises(ValueError, match="wildcard"):
            await client._retry_subscribe("ticker", "*")

    @pytest.mark.asyncio
    async def test_retry_rejects_unknown_channel(self, client: KrakenExchangeClient) -> None:
        """Retry rejects unsupported channel keys.

        Given: A connected Spot websocket,
        When: _retry_subscribe receives an unknown key,
        Then: ValueError is raised.
        """
        with pytest.raises(ValueError, match="Unsupported"):
            await client._retry_subscribe("book", "BTC/USD")

    @pytest.mark.asyncio
    async def test_retry_requires_ws_client(self) -> None:
        """Retry requires connected websocket.

        Given: A Spot client without websocket,
        When: _retry_subscribe is called,
        Then: RuntimeError is raised.
        """
        client = KrakenExchangeClient(api_key="k", api_secret="s")
        client._ws_client = None
        with pytest.raises(RuntimeError, match="connected"):
            await client._retry_subscribe("ticker", "BTC/USD")


class TestKrakenSpotSubscribeHealthMarks:
    """Tests for Spot subscribe and data health marks."""

    @pytest.mark.asyncio
    async def test_wildcard_ticker_subscribe_skips_pending_marks(self) -> None:
        """Wildcard ticker subscribe does not create pending symbols.

        Given: A wildcard ticker subscription,
        When: The iterator starts,
        Then: mark_pending is not called before subscribe.
        """
        client = KrakenExchangeClient(api_key="k", api_secret="s")
        client._health_tracker = MagicMock()
        client._ws_client = AsyncMock()
        client._ws_client.__aenter__.return_value = client._ws_client
        client._ws_client.exception_occur = False
        await client._tick_queue.put(
            TickerUpdate(
                symbol="BTC-USD",
                bid=1.0,
                bid_qty=1.0,
                ask=2.0,
                ask_qty=1.0,
                last=1.5,
                volume=1.0,
                vwap=1.5,
                low=1.0,
                high=2.0,
                change=0.0,
                change_pct=0.0,
            )
        )

        async def noop() -> None:
            return None

        client._ensure_ws_connected = noop
        iterator = client.subscribe_ticks(["*"])
        await anext(iterator)
        await iterator.aclose()
        client._health_tracker.mark_pending.assert_not_called()

    def test_data_seen_uses_wire_symbols_before_parse(self) -> None:
        """Data observer uses raw wire symbols.

        Given: Raw ticker, trade, and OHLC frames,
        When: Handlers process them,
        Then: mark_data_seen receives WS-format symbols.
        """
        client = KrakenExchangeClient(api_key="k", api_secret="s")
        client._health_tracker = MagicMock()
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.parse_kraken_ticker_list",
                return_value=[],
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.parse_kraken_trade_list",
                return_value=[],
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.parse_kraken_candle_list",
                return_value=[],
            ),
        ):
            client._handle_ticker_data([{"symbol": "BTC/USD"}])
            client._handle_trade_data([{"symbol": "ETH/USD"}])
            client._handle_ohlc_data([{"symbol": "SOL/USD", "interval": 1}])
        assert client._health_tracker.mark_data_seen.call_args_list == [
            call("ticker", "BTC/USD"),
            call("trade", "ETH/USD"),
            call("ohlc:1m", "SOL/USD"),
        ]

    def test_ohlc_data_health_mark_is_non_fatal_for_unknown_interval(self) -> None:
        """Unsupported OHLC health interval does not block parsing.

        Given: OHLC data with a non-tracker interval and a malformed item,
        When: _handle_ohlc_data processes it,
        Then: Parsing still runs and no data-seen mark is recorded.
        """
        client = KrakenExchangeClient(api_key="k", api_secret="s")
        client._health_tracker = MagicMock()
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken.parse_kraken_candle_list",
            return_value=[],
        ) as parse_candles:
            client._handle_ohlc_data(["malformed", {"symbol": "BTC/USD", "interval": 99}])
        parse_candles.assert_called_once()
        client._health_tracker.mark_data_seen.assert_not_called()

    def test_trade_data_with_non_list_and_malformed_items_still_parses(self) -> None:
        """Trade data health marker tolerates non-symbol payloads.

        Given: Non-list trade data and a list with a malformed item,
        When: _handle_trade_data processes them,
        Then: Parsing runs without marking data seen.
        """
        client = KrakenExchangeClient(api_key="k", api_secret="s")
        client._health_tracker = MagicMock()
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken.parse_kraken_trade_list",
            return_value=[],
        ) as parse_trades:
            client._handle_trade_data({"symbol": "BTC/USD"})
            client._handle_trade_data(["malformed"])
        assert parse_trades.call_count == 2
        client._health_tracker.mark_data_seen.assert_not_called()

    @pytest.mark.asyncio
    async def test_replay_marks_ohlc_pending_with_interval_channel(self) -> None:
        """Replay re-arms OHLC subscriptions under interval channel key.

        Given: A cached 5m OHLC subscription,
        When: _replay_subscriptions runs,
        Then: mark_pending preserves retry count under ohlc:5m.
        """
        client = KrakenExchangeClient(api_key="k", api_secret="s")
        client._health_tracker = MagicMock()
        client._ws_client = AsyncMock(spec=SpotWSClient)
        req = SubscriptionRequest(
            channel="ohlc",
            symbols=("BTC/USD",),
            parameters_json='{"interval":5}',
        )
        client._subscription_cache[req.key()] = req
        await client._replay_subscriptions()
        client._health_tracker.mark_pending.assert_called_once_with(
            "ohlc:5m",
            "BTC/USD",
            preserve_retry_count=True,
        )

    @pytest.mark.asyncio
    async def test_replay_uses_plain_ohlc_for_non_integer_cached_interval(self) -> None:
        """Replay keeps malformed cached OHLC interval non-fatal.

        Given: A cached OHLC subscription with a non-integer interval,
        When: _replay_subscriptions runs,
        Then: mark_pending falls back to the plain channel key.
        """
        client = KrakenExchangeClient(api_key="k", api_secret="s")
        client._health_tracker = MagicMock()
        client._ws_client = AsyncMock(spec=SpotWSClient)
        req = SubscriptionRequest(
            channel="ohlc",
            symbols=("BTC/USD",),
            parameters_json='{"interval":"5"}',
        )
        client._subscription_cache[req.key()] = req
        await client._replay_subscriptions()
        client._health_tracker.mark_pending.assert_called_once_with(
            "ohlc",
            "BTC/USD",
            preserve_retry_count=True,
        )

    @pytest.mark.asyncio
    async def test_replay_skips_wildcard_ticker_health_mark(self) -> None:
        """Replay does not track wildcard ticker as a symbol.

        Given: A cached wildcard ticker subscription,
        When: _replay_subscriptions runs,
        Then: mark_pending is not called.
        """
        client = KrakenExchangeClient(api_key="k", api_secret="s")
        client._health_tracker = MagicMock()
        client._ws_client = AsyncMock(spec=SpotWSClient)
        req = SubscriptionRequest(
            channel="ticker",
            symbols=("*",),
            parameters_json='{"snapshot":true}',
        )
        client._subscription_cache[req.key()] = req
        await client._replay_subscriptions()
        client._health_tracker.mark_pending.assert_not_called()
