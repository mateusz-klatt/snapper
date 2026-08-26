"""Tests for Kraken Equities subscription ACK health tracking."""

from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import call
from unittest.mock import patch

import pytest
from kraken.spot import SpotWSClient

from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate
from snapper.infrastructure.exchanges.implementations.kraken_equities import (
    KrakenEquitiesExchangeClient,
)


def _ws_mock(client: KrakenEquitiesExchangeClient) -> AsyncMock:
    """Return the mocked Equities websocket.

    Args:
        client: Equities exchange client under test.

    Returns:
        Mocked websocket assigned to the client.

    Raises:
        AssertionError: If the test fixture did not install an AsyncMock.
    """
    ws_client = client._ws_client
    assert isinstance(ws_client, AsyncMock)
    return ws_client


class TestKrakenEquitiesSubscriptionAck:
    """Tests for Equities subscribe ACK routing."""

    @pytest.fixture
    def client(self) -> KrakenEquitiesExchangeClient:
        """Create an Equities client with tracker spy."""
        client = KrakenEquitiesExchangeClient()
        client._health_tracker = MagicMock()
        return client

    @pytest.mark.asyncio
    async def test_ticker_success_ack_marks_confirmed(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Ticker ACK confirms ticker subscription.

        Given: A successful ticker subscribe ACK,
        When: _on_ws_message receives it,
        Then: The tracker marks the wire symbol confirmed.
        """
        await client._on_ws_message(
            {
                "method": "subscribe",
                "success": True,
                "result": {"channel": "ticker", "symbol": "MNQM6.CME"},
            }
        )
        client._health_tracker.mark_confirmed.assert_called_once_with("ticker", "MNQM6.CME")

    @pytest.mark.asyncio
    async def test_ticker_failure_ack_marks_failed(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Ticker ACK failure marks ticker subscription failed.

        Given: A failed ticker subscribe ACK,
        When: _on_ws_message receives it,
        Then: The tracker records the failure.
        """
        await client._on_ws_message(
            {
                "method": "subscribe",
                "success": False,
                "error": "bad symbol",
                "result": {"channel": "ticker", "symbol": "BAD.CME"},
            }
        )
        client._health_tracker.mark_failed.assert_called_once_with(
            "ticker", "BAD.CME", "bad symbol"
        )

    @pytest.mark.asyncio
    async def test_trade_already_subscribed_marks_confirmed(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Idempotent trade ACK confirms trade subscription.

        Given: A trade ACK with Already subscribed,
        When: _on_ws_message receives it,
        Then: The tracker treats it as confirmed.
        """
        await client._on_ws_message(
            {
                "method": "subscribe",
                "success": False,
                "error": "Already subscribed",
                "result": {"channel": "trade", "symbol": "MESM6.CME"},
            }
        )
        client._health_tracker.mark_confirmed.assert_called_once_with("trade", "MESM6.CME")

    @pytest.mark.asyncio
    async def test_trade_failure_ack_marks_failed(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Trade ACK failure marks trade subscription failed.

        Given: A failed trade subscribe ACK,
        When: _on_ws_message receives it,
        Then: The tracker records the failure.
        """
        await client._on_ws_message(
            {
                "method": "subscribe",
                "success": False,
                "error": "denied",
                "result": {"channel": "trade", "symbol": "MESM6.CME"},
            }
        )
        client._health_tracker.mark_failed.assert_called_once_with("trade", "MESM6.CME", "denied")

    @pytest.mark.asyncio
    async def test_ticker_snapshot_is_not_misclassified_as_ack(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Ticker snapshot drop happens before ACK routing.

        Given: A ticker snapshot frame,
        When: _on_ws_message receives it,
        Then: The ACK handler is not called.
        """
        with patch.object(client, "_handle_subscription_ack") as ack_handler:
            await client._on_ws_message(
                {
                    "channel": "ticker",
                    "type": "snapshot",
                    "data": [{"symbol": "MNQM6.CME"}],
                }
            )
        ack_handler.assert_not_called()

    def test_trade_ack_without_symbol_does_not_touch_tracker(
        self, client: KrakenEquitiesExchangeClient
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
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Ticker handler validates dispatcher result attribution.

        Given: A valid ticker ACK message but a result dict without symbol,
        When: _handle_ticker_subscription_ack is called directly,
        Then: No tracker method is called.
        """
        message = {
            "method": "subscribe",
            "success": True,
            "result": {"channel": "ticker", "symbol": "MNQM6.CME"},
        }
        client._handle_ticker_subscription_ack(message, {"channel": "ticker"})
        client._health_tracker.mark_confirmed.assert_not_called()

    def test_ticker_ack_validation_error_does_not_touch_tracker(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Malformed ticker ACK schema is ignored.

        Given: A ticker ACK message with an invalid result symbol,
        When: _handle_ticker_subscription_ack validates it,
        Then: No tracker method is called.
        """
        client._handle_ticker_subscription_ack(
            {
                "method": "subscribe",
                "success": True,
                "result": {"channel": "ticker", "symbol": 3},
            },
            {"channel": "ticker", "symbol": "MNQM6.CME"},
        )
        client._health_tracker.mark_confirmed.assert_not_called()

    def test_trade_ack_validation_error_does_not_touch_tracker(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Malformed trade ACK schema is ignored.

        Given: A trade ACK message with an invalid result symbol,
        When: _handle_trade_subscription_ack validates it,
        Then: No tracker method is called.
        """
        client._handle_trade_subscription_ack(
            {
                "method": "unsubscribe",
                "success": True,
                "result": {"channel": "trade", "symbol": "MESM6.CME"},
            },
            {"channel": "trade", "symbol": "MESM6.CME"},
        )
        client._health_tracker.mark_confirmed.assert_not_called()

    def test_unknown_ack_channel_is_ignored(self, client: KrakenEquitiesExchangeClient) -> None:
        """Unknown ACK channel is ignored.

        Given: A subscribe ACK for an unsupported channel,
        When: _handle_subscription_ack receives it,
        Then: No tracker method is called.
        """
        client._handle_subscription_ack(
            {
                "method": "subscribe",
                "success": True,
                "result": {"channel": "book", "symbol": "MNQM6.CME"},
            }
        )
        client._health_tracker.mark_confirmed.assert_not_called()


class TestKrakenEquitiesSubscriptionRetry:
    """Tests for Equities one-symbol retry subscribes."""

    @pytest.fixture
    def client(self) -> KrakenEquitiesExchangeClient:
        """Create an Equities client with mocked websocket."""
        client = KrakenEquitiesExchangeClient()
        client._ws_client = AsyncMock(spec=SpotWSClient)
        return client

    @pytest.mark.asyncio
    async def test_retry_ticker_uses_equities_params(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Ticker retry includes Equities-specific params.

        Given: A connected Equities websocket,
        When: _retry_subscribe retries ticker,
        Then: The SDK subscribe call includes throttle and asset_class.
        """
        await client._retry_subscribe("ticker", "MNQM6.CME")
        params = _ws_mock(client).subscribe.await_args.kwargs["params"]
        assert (params["channel"], params["symbol"], params["asset_class"]) == (
            "ticker",
            ["MNQM6.CME"],
            "futures_contract",
        )

    @pytest.mark.asyncio
    async def test_retry_trade_uses_trade_channel(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Trade retry subscribes the trade channel.

        Given: A connected Equities websocket,
        When: _retry_subscribe retries trade,
        Then: The SDK subscribe call receives trade params.
        """
        await client._retry_subscribe("trade", "MESM6.CME")
        assert _ws_mock(client).subscribe.await_args.kwargs["params"]["channel"] == "trade"

    @pytest.mark.asyncio
    async def test_retry_rejects_unknown_channel(
        self, client: KrakenEquitiesExchangeClient
    ) -> None:
        """Retry rejects unsupported channel keys.

        Given: A connected Equities websocket,
        When: _retry_subscribe receives an unknown key,
        Then: ValueError is raised.
        """
        with pytest.raises(ValueError, match="Unsupported"):
            await client._retry_subscribe("ohlc:1m", "MNQM6.CME")

    @pytest.mark.asyncio
    async def test_retry_requires_ws_client(self) -> None:
        """Retry requires connected websocket.

        Given: An Equities client without websocket,
        When: _retry_subscribe is called,
        Then: RuntimeError is raised.
        """
        client = KrakenEquitiesExchangeClient()
        client._ws_client = None
        with pytest.raises(RuntimeError, match="connected"):
            await client._retry_subscribe("ticker", "MNQM6.CME")


class TestKrakenEquitiesHealthMarks:
    """Tests for Equities subscribe and data health marks."""

    @pytest.mark.asyncio
    async def test_subscribe_ticks_marks_pending(self) -> None:
        """Ticker subscribe marks each wire symbol pending.

        Given: An Equities ticker subscription,
        When: The iterator starts,
        Then: mark_pending receives the WS symbol.
        """
        client = KrakenEquitiesExchangeClient()
        client._health_tracker = MagicMock()
        client._ws_client = AsyncMock()
        await client._tick_queue.put(
            TickerUpdate(
                symbol="MNQM6-CME",
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
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_equities.native_to_kraken_equities_ws",
            return_value="MNQM6.CME",
        ):
            iterator = client.subscribe_ticks(["MNQM6-CME"])
            await anext(iterator)
            await iterator.aclose()
        client._health_tracker.mark_pending.assert_called_once_with("ticker", "MNQM6.CME")

    def test_data_seen_uses_wire_symbols_after_trade_validation(self) -> None:
        """Data observer uses raw wire symbols after trade validation.

        Given: Raw ticker and trade frames,
        When: Handlers process them,
        Then: mark_data_seen receives WS-format symbols.
        """
        client = KrakenEquitiesExchangeClient()
        client._health_tracker = MagicMock()
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.parse_kraken_equities_ticker",
                return_value=MagicMock(),
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.parse_kraken_equities_trade",
                return_value=TradeUpdate(
                    symbol="MESM6-CME",
                    side="buy",
                    quantity=1.0,
                    price=1.0,
                    ord_type="fill",
                    timestamp=datetime.now(UTC),
                    trade_id="t1",
                ),
            ),
        ):
            client._handle_ticker_message(
                {"data": [{"symbol": "MNQM6.CME"}]},
                envelope_delayed=True,
            )
            client._handle_trade_message({"data": [{"symbol": "MESM6.CME"}]})
        assert client._health_tracker.mark_data_seen.call_args_list == [
            call("ticker", "MNQM6.CME"),
            call("trade", "MESM6.CME"),
        ]

    def test_data_seen_ignores_non_dict_and_non_string_symbols(self) -> None:
        """Data observer skips malformed data identities.

        Given: Ticker and trade frames without string wire symbols,
        When: The handlers process them,
        Then: mark_data_seen is not called.
        """
        client = KrakenEquitiesExchangeClient()
        client._health_tracker = MagicMock()
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.parse_kraken_equities_ticker",
                return_value=MagicMock(),
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken_equities.parse_kraken_equities_trade",
                return_value=TradeUpdate(
                    symbol="MESM6-CME",
                    side="buy",
                    quantity=1.0,
                    price=1.0,
                    ord_type="fill",
                    timestamp=datetime.now(UTC),
                    trade_id="t1",
                ),
            ),
        ):
            client._handle_ticker_message(
                {"data": ["malformed", {"symbol": 3}]},
                envelope_delayed=False,
            )
            client._handle_trade_message({"data": ["malformed", {"symbol": 3}]})
        client._health_tracker.mark_data_seen.assert_not_called()
