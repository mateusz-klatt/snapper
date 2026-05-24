"""Tests for Kraken Futures subscription ACK health tracking."""

from collections.abc import Generator
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import call
from unittest.mock import patch

import pytest

from snapper.infrastructure.exchanges._subscription_request import SubscriptionRequest
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate
from snapper.infrastructure.exchanges.implementations import kraken_futures as futures_mod
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)


@pytest.fixture()
def client() -> Generator[KrakenFuturesExchangeClient]:
    """Create a Futures client with external SDKs patched."""
    with patch("snapper.infrastructure.exchanges.implementations.kraken_futures.ccxt") as ccxt_mod:
        ccxt_mod.krakenfutures = MagicMock(return_value=MagicMock())
        client = KrakenFuturesExchangeClient(sandbox=True)
        client._health_tracker = MagicMock()
        yield client


class TestKrakenFuturesSubscriptionEvents:
    """Tests for Futures event-frame dispatch."""

    @pytest.mark.asyncio
    async def test_subscribed_event_marks_each_product_confirmed(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Subscribed event confirms each product.

        Given: A subscribed event with product_ids,
        When: _on_ws_message receives it,
        Then: The tracker confirms every product.
        """
        await client._on_ws_message(
            {"event": "subscribed", "feed": "ticker", "product_ids": ["PF_XBTUSD", "PF_ETHUSD"]}
        )
        assert client._health_tracker.mark_confirmed.call_args_list == [
            call("ticker", "PF_XBTUSD"),
            call("ticker", "PF_ETHUSD"),
        ]

    @pytest.mark.asyncio
    async def test_subscribed_event_ignores_malformed_payloads(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Malformed subscribed events are ignored.

        Given: Subscribed events without usable feed or product_ids,
        When: _on_ws_message receives them,
        Then: No tracker method is called.
        """
        await client._on_ws_message({"event": "subscribed", "product_ids": ["PF_XBTUSD"]})
        await client._on_ws_message({"event": "subscribed", "feed": "ticker", "product_ids": {}})
        client._health_tracker.mark_confirmed.assert_not_called()

    @pytest.mark.asyncio
    async def test_subscribed_event_skips_non_string_products(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Subscribed event only tracks string product ids.

        Given: A subscribed event with one malformed product id,
        When: _on_ws_message receives it,
        Then: Only the string product id is confirmed.
        """
        await client._on_ws_message(
            {"event": "subscribed", "feed": "trade", "product_ids": ["PF_XBTUSD", 3]}
        )
        client._health_tracker.mark_confirmed.assert_called_once_with("trade", "PF_XBTUSD")

    @pytest.mark.asyncio
    async def test_alert_event_with_attribution_marks_failed(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Attributable alert marks one subscription failed.

        Given: An alert with feed and product_id,
        When: _on_ws_message receives it,
        Then: The tracker records the failure.
        """
        await client._on_ws_message(
            {
                "event": "alert",
                "feed": "ticker_lite",
                "product_id": "PF_XBTUSD",
                "message": "rejected",
            }
        )
        client._health_tracker.mark_failed.assert_called_once_with(
            "ticker", "PF_XBTUSD", "rejected"
        )

    @pytest.mark.asyncio
    async def test_alert_event_without_string_message_uses_default_error(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Alert without string message still fails attributable subscription.

        Given: An alert with feed and product_id but non-string message,
        When: _on_ws_message receives it,
        Then: The tracker records a default error.
        """
        await client._on_ws_message(
            {"event": "alert", "feed": "trade", "product_id": "PF_XBTUSD", "message": 3}
        )
        client._health_tracker.mark_failed.assert_called_once_with(
            "trade", "PF_XBTUSD", "subscription alert"
        )

    @pytest.mark.asyncio
    async def test_alert_event_without_attribution_logs_only(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Unattributed alert does not fail a specific symbol.

        Given: An alert without feed and product_id,
        When: _on_ws_message receives it,
        Then: No tracker failure is recorded.
        """
        await client._on_ws_message({"event": "alert", "message": "maintenance"})
        client._health_tracker.mark_failed.assert_not_called()

    @pytest.mark.asyncio
    async def test_unknown_event_is_silently_ignored(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Unknown event frames are ignored.

        Given: An unsubscribed event,
        When: _on_ws_message receives it,
        Then: No tracker method is called.
        """
        await client._on_ws_message({"event": "unsubscribed", "feed": "ticker"})
        client._health_tracker.mark_confirmed.assert_not_called()
        client._health_tracker.mark_failed.assert_not_called()


class TestKrakenFuturesDataMarks:
    """Tests for Futures data-path health marks."""

    @pytest.mark.asyncio
    async def test_ticker_data_marks_product_seen(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Ticker data marks ticker product seen.

        Given: A ticker_lite frame,
        When: _on_ws_message processes it,
        Then: mark_data_seen uses ticker and product_id.
        """
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.parse_kraken_futures_ticker",
            return_value=TickerUpdate(
                symbol="BTC-USD-PERP",
                bid=1.0,
                bid_qty=1.0,
                ask=2.0,
                ask_qty=1.0,
                last=1.5,
                volume=1.0,
                vwap=0.0,
                low=1.0,
                high=2.0,
                change=0.0,
                change_pct=0.0,
            ),
        ):
            await client._on_ws_message({"feed": "ticker_lite", "product_id": "PF_XBTUSD"})
        client._health_tracker.mark_data_seen.assert_called_once_with("ticker", "PF_XBTUSD")

    @pytest.mark.asyncio
    async def test_ticker_data_falls_back_to_symbol(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Ticker data can use symbol when product_id is absent.

        Given: A ticker frame with symbol but no product_id,
        When: _on_ws_message processes it,
        Then: mark_data_seen uses the symbol value.
        """
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.parse_kraken_futures_ticker",
            return_value=MagicMock(),
        ):
            await client._on_ws_message({"feed": "ticker", "symbol": "PF_XBTUSD"})
        client._health_tracker.mark_data_seen.assert_called_once_with("ticker", "PF_XBTUSD")

    @pytest.mark.asyncio
    async def test_ticker_data_without_identity_does_not_mark_seen(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Ticker data without product identity is parsed only.

        Given: A ticker frame without product_id or symbol,
        When: _on_ws_message processes it,
        Then: mark_data_seen is not called.
        """
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.parse_kraken_futures_ticker",
            return_value=MagicMock(),
        ):
            await client._on_ws_message({"feed": "ticker_lite"})
        client._health_tracker.mark_data_seen.assert_not_called()

    @pytest.mark.asyncio
    async def test_trade_data_marks_product_seen(self, client: KrakenFuturesExchangeClient) -> None:
        """Live trade data marks trade product seen.

        Given: A live trade frame,
        When: _on_ws_message processes it,
        Then: mark_data_seen uses trade and product_id.
        """
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.parse_kraken_futures_trade",
            return_value=TradeUpdate(
                symbol="BTC-USD-PERP",
                side="buy",
                quantity=1.0,
                price=1.0,
                ord_type="fill",
                timestamp=MagicMock(),
                trade_id="t1",
            ),
        ):
            await client._on_ws_message(
                {
                    "feed": "trade",
                    "product_id": "PF_XBTUSD",
                    "time": 1,
                    "qty": 1.0,
                    "price": 1.0,
                    "side": "buy",
                }
            )
        client._health_tracker.mark_data_seen.assert_called_once_with("trade", "PF_XBTUSD")

    @pytest.mark.asyncio
    async def test_trade_data_without_string_product_does_not_mark_seen(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Trade data without string product identity is parsed only.

        Given: A live trade frame with a non-string product_id,
        When: _on_ws_message processes it,
        Then: mark_data_seen is not called.
        """
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken_futures.parse_kraken_futures_trade",
            return_value=TradeUpdate(
                symbol="BTC-USD-PERP",
                side="buy",
                quantity=1.0,
                price=1.0,
                ord_type="fill",
                timestamp=MagicMock(),
                trade_id="t1",
            ),
        ):
            await client._on_ws_message(
                {
                    "feed": "trade",
                    "product_id": 3,
                    "time": 1,
                    "qty": 1.0,
                    "price": 1.0,
                    "side": "buy",
                }
            )
        client._health_tracker.mark_data_seen.assert_not_called()

    @pytest.mark.asyncio
    async def test_trade_snapshot_does_not_mark_data_seen(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Trade snapshot is dropped before data-presence tracking.

        Given: A trade_snapshot frame,
        When: _on_ws_message receives it,
        Then: mark_data_seen is not called.
        """
        await client._on_ws_message(
            {"feed": "trade_snapshot", "product_id": "PF_XBTUSD", "trades": []}
        )
        client._health_tracker.mark_data_seen.assert_not_called()


class TestKrakenFuturesSubscriptionRetry:
    """Tests for Futures subscribe retry and pending marks."""

    @pytest.mark.asyncio
    async def test_subscribe_in_chunks_marks_pending(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Per-product subscribe marks each product pending.

        Given: A Futures client with public websocket,
        When: _subscribe_in_chunks subscribes ticker products,
        Then: mark_pending is called once per product.
        """
        client._ws_client = AsyncMock()
        await client._subscribe_in_chunks("ticker", ["PF_XBTUSD", "PF_ETHUSD"])
        assert client._health_tracker.mark_pending.call_args_list == [
            call("ticker", "PF_XBTUSD"),
            call("ticker", "PF_ETHUSD"),
        ]
        assert client._ws_client.subscribe.await_args_list == [
            call(feed="ticker", products=["PF_XBTUSD"]),
            call(feed="ticker", products=["PF_ETHUSD"]),
        ]

    @pytest.mark.asyncio
    async def test_replay_marks_pending_preserving_retry_count(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Replay re-arms pending state without resetting retry budget.

        Given: A cached Futures subscription,
        When: _replay_subscriptions runs,
        Then: mark_pending is called with preserve_retry_count=True.
        """
        client._ws_client = AsyncMock()
        req = SubscriptionRequest(channel="trade", symbols=("PF_XBTUSD",), parameters_json="{}")
        client._subscription_cache[req.key()] = req
        await client._replay_subscriptions()
        client._health_tracker.mark_pending.assert_called_once_with(
            "trade", "PF_XBTUSD", preserve_retry_count=True
        )

    @pytest.mark.asyncio
    async def test_retry_subscribe_uses_feed_and_single_product(
        self, client: KrakenFuturesExchangeClient
    ) -> None:
        """Retry subscribes one Futures product.

        Given: A connected Futures websocket,
        When: _retry_subscribe retries ticker,
        Then: The SDK receives feed and single-product list.
        """
        client._ws_client = AsyncMock()
        await client._retry_subscribe("ticker", "PF_XBTUSD")
        assert client._ws_client.subscribe.await_args.kwargs == {
            "feed": "ticker",
            "products": ["PF_XBTUSD"],
        }

    @pytest.mark.asyncio
    async def test_retry_rejects_unknown_channel(self, client: KrakenFuturesExchangeClient) -> None:
        """Retry rejects unsupported channel keys.

        Given: A connected Futures websocket,
        When: _retry_subscribe receives an unknown key,
        Then: ValueError is raised.
        """
        client._ws_client = AsyncMock()
        with pytest.raises(ValueError, match="Unsupported"):
            await client._retry_subscribe("book", "PF_XBTUSD")

    @pytest.mark.asyncio
    async def test_retry_requires_ws_client(self, client: KrakenFuturesExchangeClient) -> None:
        """Retry requires connected websocket.

        Given: A Futures client without websocket,
        When: _retry_subscribe is called,
        Then: RuntimeError is raised.
        """
        client._ws_client = None
        with pytest.raises(RuntimeError, match="connected"):
            await client._retry_subscribe("ticker", "PF_XBTUSD")

    def test_tracker_feed_key_passes_through_unknown_feed(self) -> None:
        """Unknown feed keys pass through unchanged.

        Given: A feed outside public ticker/trade channels,
        When: _tracker_feed_key is called,
        Then: The original feed is returned.
        """
        assert futures_mod._tracker_feed_key("fills") == "fills"
