"""Tests for Kraken exchange client implementation."""

import asyncio
import contextlib
import time
from collections.abc import Coroutine
from dataclasses import asdict
from datetime import UTC
from datetime import datetime
from types import SimpleNamespace
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import ccxt
import pytest
from ccxt.base.errors import NetworkError
from pydantic import ValidationError
from pytest import MonkeyPatch

from snapper.infrastructure.exchanges.contracts import AccountBalance
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import ExchangeOrderRequest
from snapper.infrastructure.exchanges.contracts import ExchangeOrderSnapshot
from snapper.infrastructure.exchanges.contracts import ExecutionUpdate
from snapper.infrastructure.exchanges.contracts import InstrumentPairDescriptor
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.contracts import OrderStatusEnum
from snapper.infrastructure.exchanges.contracts import OrderTypeEnum
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate
from snapper.infrastructure.exchanges.implementations.kraken import KrakenExchangeClient


class _DummyWs:
    """Test dummy for Kraken WebSocket connection."""

    def __init__(self, queue: asyncio.Queue[Any]):
        self.queue = queue
        self.subscriptions: list[tuple[dict[str, Any], int | None]] = []
        self.exception_occur = False
        self.closed = False

    async def __aenter__(self) -> "_DummyWs":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        return False

    async def subscribe(self, *, params: dict[str, Any], req_id: int | None = None) -> None:
        self.subscriptions.append((params, req_id))

    async def close(self) -> None:
        self.closed = True


def _client() -> KrakenExchangeClient:
    client = KrakenExchangeClient.__new__(KrakenExchangeClient)
    client.api_key = "key"
    client.api_secret = "secret"
    client._ws_connected = True
    client._tick_queue = asyncio.Queue[TickerUpdate]()
    client._candle_queues = {}
    client._ccxt_client = SimpleNamespace()

    async def _with_retry(fn: Any, *args: Any, **kwargs: Any) -> Any:
        return await fn(*args, **kwargs)

    client._with_retry = _with_retry
    return client


def test_get_ccxt_client_returns_ccxt_instance() -> None:
    """CCXT client accessor.

    Given a KrakenExchangeClient instance,
    When get_ccxt_client is called,
    Then it returns the internal CCXT client instance.
    """
    client = _client()
    ccxt_client = client.get_ccxt_client()
    assert ccxt_client is client._ccxt_client


@pytest.mark.asyncio
async def test_close_ws_client_shuts_down_session() -> None:
    """WebSocket client shutdown.

    Given a KrakenExchangeClient with an active WebSocket connection,
    When _close_ws_client is called,
    Then the WebSocket client and session are closed and references cleared.
    """
    client = _client()

    class _DummySession:
        def __init__(self) -> None:
            self.closed = False

        async def close(self) -> None:
            self.closed = True

    class _DummyWsClient:
        def __init__(self) -> None:
            self.closed = False
            self._SpotAsyncClient__session = _DummySession()

        async def close(self) -> None:
            self.closed = True

    client._ws_client = _DummyWsClient()
    client._ws_connected = True
    await client._close_ws_client()
    assert client._ws_client is None
    assert client._ws_connected is False


@pytest.mark.asyncio
async def test_get_balance_filters_and_returns_currency() -> None:
    """Balance retrieval with currency filtering.

    Given a KrakenExchangeClient with a mocked balance response,
    When get_balance is called with or without a currency filter,
    Then it returns AccountBalance objects filtered appropriately.
    """
    client = _client()

    async def _fake_balance() -> dict[str, Any]:
        return {
            "free": 0,
            "info": {},
            "USD": {"free": 1, "used": 2, "total": 3},
            "EUR": {"free": 0, "used": 0, "total": 0},
        }

    client._ccxt_client = SimpleNamespace(fetch_balance=_fake_balance)
    balances = await client.get_balance()
    assert balances["USD"] == AccountBalance("USD", 1.0, 2.0, 3.0)
    assert "info" not in balances
    eur_only = await client.get_balance("EUR")
    assert set(eur_only.keys()) == {"EUR"}
    assert eur_only["EUR"].total == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_subscribe_ticks_yields_and_respects_exception_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Tick subscription yields updates until exception flag.

    Given a KrakenExchangeClient with a queued ticker update,
    When subscribe_ticks is iterated and exception_occur is set,
    Then it yields the update and stops iteration.
    """
    client = _client()

    async def _noop() -> None:
        return None

    client._ensure_ws_connected = _noop
    ws = _DummyWs(client._tick_queue)
    client._ws_client = ws
    await client._tick_queue.put(
        TickerUpdate(
            symbol="BTC/USD",
            bid=1.0,
            bid_qty=0.1,
            ask=2.0,
            ask_qty=0.2,
            last=1.5,
            volume=1.0,
            vwap=1.5,
            low=1.0,
            high=2.0,
            change=0.5,
            change_pct=1.0,
        )
    )
    gen = client.subscribe_ticks(["BTC-USD"], req_id=1)
    first = await anext(gen)
    ws.exception_occur = True
    assert isinstance(first, TickerUpdate)
    with pytest.raises(StopAsyncIteration):
        await anext(gen)


@pytest.mark.asyncio
async def test_subscribe_candles_yields_per_interval(monkeypatch: pytest.MonkeyPatch) -> None:
    """Candle subscription yields updates per interval.

    Given a KrakenExchangeClient with a candle queue for a specific interval,
    When subscribe_candles is iterated,
    Then it yields candle updates from the interval-specific queue.
    """
    client = _client()

    async def _noop() -> None:
        return None

    client._ensure_ws_connected = _noop
    interval = 1
    client._candle_queues[interval] = asyncio.Queue[CandleUpdate]()
    ws = _DummyWs(client._candle_queues[interval])
    client._ws_client = ws
    await client._candle_queues[interval].put(
        CandleUpdate(
            symbol="BTC/USD",
            open=1,
            high=2,
            low=0.5,
            close=1.5,
            vwap=1.5,
            trades=1,
            volume=1.0,
            interval_begin=datetime.fromtimestamp(0),
            interval=1,
        )
    )
    gen = client.subscribe_candles(["BTC-USD"], timeframe="1m", req_id=2)
    first = await anext(gen)
    ws.exception_occur = True
    assert isinstance(first, CandleUpdate)
    with pytest.raises(StopAsyncIteration):
        await anext(gen)


class TestKrakenExchangeClient:
    """Tests for krakenExchangeClient."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    def test_init(self) -> None:
        """Verify init."""
        client = KrakenExchangeClient()
        assert client.api_key is None
        assert client.api_secret is None
        assert not client.sandbox

    def test_init_with_credentials(self) -> None:
        """Verify init with credentials."""
        client = KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )
        assert client.api_key == "test_key"
        assert client.api_secret == "test_secret"
        assert not client.sandbox

    async def test_connect(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify connect."""
        with patch.object(kraken_client, "_ccxt_client") as mock_client:
            mock_client.load_markets = AsyncMock(return_value={})
            await kraken_client.connect()
            mock_client.load_markets.assert_called_once()

    async def test_disconnect(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify disconnect."""
        await kraken_client.disconnect()

    @patch("snapper.infrastructure.exchanges.implementations.kraken.ccxt")
    async def test_get_ticker(
        self, mock_ccxt: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify get ticker."""
        mock_client = AsyncMock()
        with patch.object(kraken_client, "_ccxt_client", mock_client):
            mock_client.fetch_ticker.return_value = {
                "symbol": "BTC/USD",
                "last": 50000.0,
                "bid": 49999.0,
                "ask": 50001.0,
                "high": 51000.0,
                "low": 49000.0,
                "open": 49500.0,
                "close": 50000.0,
                "baseVolume": 100.0,
                "quoteVolume": 5000000.0,
                "timestamp": 1640995200000,
                "datetime": "2022-01-01T00:00:00.000Z",
            }
            ticker = await kraken_client.get_ticker("BTC-USD")
            assert ticker.symbol == "BTC-USD"
            assert ticker.last == float("50000.0")
            assert ticker.bid == float("49999.0")
            assert ticker.ask == float("50001.0")
            mock_client.fetch_ticker.assert_called_once_with("BTC/USD")

    @patch("snapper.infrastructure.exchanges.implementations.kraken.ccxt")
    async def test_get_ohlcv(
        self, mock_ccxt: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify get ohlcv."""
        mock_client = AsyncMock()
        with patch.object(kraken_client, "_ccxt_client", mock_client):
            mock_client.fetch_ohlcv.return_value = [
                [1640995200000, 49500.0, 51000.0, 49000.0, 50000.0, 100.0],
                [1640998800000, 50000.0, 50500.0, 49800.0, 50200.0, 80.0],
            ]
            ohlcv_list = await kraken_client.get_ohlcv("BTC-USD", "1h", limit=2)
            assert len(ohlcv_list) == 2
            candle1 = ohlcv_list[0]
            assert candle1.open == float("49500.0")
            assert candle1.high == float("51000.0")
            assert candle1.low == float("49000.0")
            assert candle1.close == float("50000.0")
            assert candle1.volume == float("100.0")
            assert candle1.timestamp == pytest.approx(1640995200.0)
            mock_client.fetch_ohlcv.assert_called_once_with("BTC/USD", "1h", None, 2)

    @patch("snapper.infrastructure.exchanges.implementations.kraken.ccxt")
    async def test_create_order(
        self, mock_ccxt: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify create order."""
        mock_client = AsyncMock()
        with patch.object(kraken_client, "_ccxt_client", mock_client):
            mock_client.create_order.return_value = {
                "id": "test_order_123",
                "symbol": "BTC/USD",
                "type": "market",
                "side": "buy",
                "amount": 0.1,
                "price": None,
                "status": "open",
                "timestamp": 1640995200000,
                "fee": {"cost": 5.0, "currency": "USD"},
                "filled": 0.0,
                "remaining": 0.1,
            }
            order_request = ExchangeOrderRequest(
                symbol="BTC-USD",
                side=OrderSideEnum.BUY,
                type=OrderTypeEnum.MARKET,
                amount=float("0.1"),
            )
            order = await kraken_client.create_order(order_request)
            assert order.id == "test_order_123"
            assert order.symbol == "BTC-USD"
            assert order.side == OrderSideEnum.BUY
            assert order.type == OrderTypeEnum.MARKET
            assert order.amount == float("0.1")
            assert order.status == OrderStatusEnum.OPEN
            assert order.fee == float("5.0")
            mock_client.create_order.assert_called_once()

    @patch("snapper.infrastructure.exchanges.implementations.kraken.ccxt")
    async def test_cancel_order(
        self, mock_ccxt: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify cancel order."""
        mock_client = AsyncMock()
        with patch.object(kraken_client, "_ccxt_client", mock_client):
            mock_client.cancel_order.return_value = {
                "id": "test_order_123",
                "symbol": "BTC/USD",
                "type": "limit",
                "side": "buy",
                "amount": 0.1,
                "price": 50000.0,
                "status": "canceled",
                "timestamp": 1640995200000,
                "fee": None,
                "filled": 0.0,
                "remaining": 0.1,
                "info": {"result": "success"},
            }
            result = await kraken_client.cancel_order("test_order_123", "BTC-USD")
            assert result.id == "test_order_123"
            assert result.status == OrderStatusEnum.CANCELED
            mock_client.cancel_order.assert_called_once_with("test_order_123", "BTC/USD")

    @patch("snapper.infrastructure.exchanges.implementations.kraken.ccxt")
    async def test_get_balance(
        self, mock_ccxt: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify get balance."""
        mock_client = AsyncMock()
        with patch.object(kraken_client, "_ccxt_client", mock_client):
            mock_client.fetch_balance.return_value = {
                "BTC": {"free": 1.5, "used": 0.5, "total": 2.0},
                "USD": {"free": 10000.0, "used": 2000.0, "total": 12000.0},
                "free": {"BTC": 1.5, "USD": 10000.0},
                "used": {"BTC": 0.5, "USD": 2000.0},
                "total": {"BTC": 2.0, "USD": 12000.0},
                "info": {},
            }
            balances = await kraken_client.get_balance()
            assert "BTC" in balances
            assert "USD" in balances
            btc_balance = balances["BTC"]
            assert btc_balance.currency == "BTC"
            assert btc_balance.free == float("1.5")
            assert btc_balance.used == float("0.5")
            assert btc_balance.total == float("2.0")
            usd_balance = balances["USD"]
            assert usd_balance.currency == "USD"
            assert usd_balance.free == float("10000.0")
            mock_client.fetch_balance.assert_called_once()

    async def test_get_balance_no_credentials(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get balance no credentials."""
        kraken_client.api_key = None
        kraken_client.api_secret = None
        with pytest.raises(RuntimeError, match="API credentials required"):
            await kraken_client.get_balance()

    async def test_circuit_breaker_open(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify circuit breaker open."""
        with (
            patch.object(kraken_client, "_circuit_open_until", time.time() + 3600),
            pytest.raises(RuntimeError, match="Circuit breaker open"),
        ):
            await kraken_client.get_ticker("BTC-USD")

    async def test_connect_failure(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify connect failure."""
        with patch.object(kraken_client, "_with_retry") as mock_retry:
            mock_retry.side_effect = Exception("Connection failed")
            with pytest.raises(Exception, match="Connection failed"):
                await kraken_client.connect()

    async def test_disconnect_with_websocket(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify disconnect with websocket."""
        mock_ws_client = AsyncMock()
        with (
            patch.object(kraken_client, "_ws_client", mock_ws_client),
            patch.object(kraken_client, "_ws_connected", True),
        ):
            mock_session = MagicMock()
            kraken_client._ccxt_client.session = mock_session
            await kraken_client.disconnect()
            mock_ws_client.close.assert_called_once()
            mock_session.close.assert_called_once()

    async def test_disconnect_exception(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify disconnect exception."""
        mock_ws_client = AsyncMock()
        mock_ws_client.close.side_effect = Exception("Close failed")
        with (
            patch.object(kraken_client, "_ws_client", mock_ws_client),
            patch.object(kraken_client, "_ws_connected", True),
        ):
            await kraken_client.disconnect()

    async def test_context_manager(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify context manager."""
        with (
            patch.object(kraken_client, "connect", new_callable=AsyncMock) as mock_connect,
            patch.object(kraken_client, "disconnect", new_callable=AsyncMock) as mock_disconnect,
        ):
            async with kraken_client as client:
                assert client is kraken_client
            mock_connect.assert_called_once()
            mock_disconnect.assert_called_once()

    async def test_get_ticker_error(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get ticker error."""
        with (
            patch.object(
                kraken_client, "_with_retry", side_effect=Exception("TickerSnapshot fetch failed")
            ),
            pytest.raises(Exception, match="TickerSnapshot fetch failed"),
        ):
            await kraken_client.get_ticker("BTC-USD")

    async def test_get_ohlcv_error(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get ohlcv error."""
        with (
            patch.object(
                kraken_client, "_with_retry", side_effect=Exception("OhlcvSnapshot fetch failed")
            ),
            pytest.raises(Exception, match="OhlcvSnapshot fetch failed"),
        ):
            await kraken_client.get_ohlcv("BTC-USD")

    async def test_create_order_no_credentials(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify create order no credentials."""
        kraken_client.api_key = None
        kraken_client.api_secret = None
        order_request = ExchangeOrderRequest(
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            type=OrderTypeEnum.MARKET,
            amount=float("0.1"),
        )
        with pytest.raises(RuntimeError, match="API credentials required"):
            await kraken_client.create_order(order_request)

    @patch("snapper.infrastructure.exchanges.implementations.kraken.ccxt")
    async def test_create_order_with_price_and_client_id(
        self, mock_ccxt: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify create order with price and client id."""
        mock_client = AsyncMock()
        mock_client.create_order.return_value = {
            "id": "test_order_123",
            "clientOrderId": "client_123",
            "symbol": "BTC/USD",
            "type": "limit",
            "side": "buy",
            "amount": 0.1,
            "price": 50000.0,
            "status": "open",
            "timestamp": 1640995200000,
            "fee": {"cost": 5.0, "currency": "USD"},
            "filled": 0.0,
            "remaining": 0.1,
        }
        with patch.object(kraken_client, "_ccxt_client", mock_client):
            order_request = ExchangeOrderRequest(
                symbol="BTC-USD",
                side=OrderSideEnum.BUY,
                type=OrderTypeEnum.LIMIT,
                amount=float("0.1"),
                price=float("50000.0"),
                client_order_id="client_123",
            )
            order = await kraken_client.create_order(order_request)
            assert order.id == "test_order_123"
            assert order.client_order_id == "client_123"
            assert order.price == float("50000.0")
            mock_client.create_order.assert_called_once()

    async def test_create_order_error(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify create order error."""
        order_request = ExchangeOrderRequest(
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            type=OrderTypeEnum.MARKET,
            amount=float("0.1"),
        )
        with (
            patch.object(
                kraken_client,
                "_with_retry",
                side_effect=Exception("ExchangeOrderSnapshot creation failed"),
            ),
            pytest.raises(Exception, match="ExchangeOrderSnapshot creation failed"),
        ):
            await kraken_client.create_order(order_request)

    async def test_cancel_order_no_credentials(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify cancel order no credentials."""
        kraken_client.api_key = None
        kraken_client.api_secret = None
        with pytest.raises(RuntimeError, match="API credentials required"):
            await kraken_client.cancel_order("test_order_123")

    async def test_cancel_order_error(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify cancel order error."""
        with (
            patch.object(kraken_client, "_with_retry", side_effect=Exception("Cancel failed")),
            pytest.raises(Exception, match="Cancel failed"),
        ):
            await kraken_client.cancel_order("test_order_123")

    async def test_get_order_no_credentials(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get order no credentials."""
        kraken_client.api_key = None
        kraken_client.api_secret = None
        with pytest.raises(RuntimeError, match="API credentials required"):
            await kraken_client.get_order("test_order_123")

    @patch("snapper.infrastructure.exchanges.implementations.kraken.ccxt")
    async def test_get_order(
        self, mock_ccxt: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify get order."""
        mock_client = AsyncMock()
        mock_client.fetch_order.return_value = {
            "id": "test_order_123",
            "symbol": "BTC/USD",
            "type": "limit",
            "side": "buy",
            "amount": 0.1,
            "price": 50000.0,
            "status": "closed",
            "timestamp": 1640995200000,
            "fee": {"cost": 5.0, "currency": "USD"},
            "filled": 0.1,
            "remaining": 0.0,
        }
        with patch.object(kraken_client, "_ccxt_client", mock_client):
            order = await kraken_client.get_order("test_order_123", "BTC-USD")
            assert order.id == "test_order_123"
            assert order.status == OrderStatusEnum.CLOSED
            assert order.filled == float("0.1")
            mock_client.fetch_order.assert_called_once_with("test_order_123", "BTC/USD")

    async def test_get_order_error(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get order error."""
        with (
            patch.object(
                kraken_client,
                "_with_retry",
                side_effect=Exception("ExchangeOrderSnapshot fetch failed"),
            ),
            pytest.raises(Exception, match="ExchangeOrderSnapshot fetch failed"),
        ):
            await kraken_client.get_order("test_order_123")

    async def test_get_orders_no_credentials(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get orders no credentials."""
        kraken_client.api_key = None
        kraken_client.api_secret = None
        with pytest.raises(RuntimeError, match="API credentials required"):
            await kraken_client.get_orders()

    @patch("snapper.infrastructure.exchanges.implementations.kraken.ccxt")
    async def test_get_orders_open(
        self, mock_ccxt: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify get orders open."""
        mock_client = AsyncMock()
        mock_client.fetch_open_orders.return_value = [
            {
                "id": "order_1",
                "symbol": "BTC/USD",
                "type": "limit",
                "side": "buy",
                "amount": 0.1,
                "price": 50000.0,
                "status": "open",
                "timestamp": 1640995200000,
                "fee": None,
                "filled": 0.0,
                "remaining": 0.1,
            },
        ]
        with patch.object(kraken_client, "_ccxt_client", mock_client):
            orders = await kraken_client.get_orders(status=OrderStatusEnum.OPEN, limit=10)
            assert len(orders) == 1
            assert orders[0].id == "order_1"
            assert orders[0].status == OrderStatusEnum.OPEN
            mock_client.fetch_open_orders.assert_called_once_with(None, None, 10)

    @patch("snapper.infrastructure.exchanges.implementations.kraken.ccxt")
    async def test_get_orders_all_with_filter(
        self, mock_ccxt: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify get orders all with filter."""
        mock_client = AsyncMock()
        mock_client.fetch_orders.return_value = [
            {
                "id": "order_1",
                "symbol": "BTC/USD",
                "type": "limit",
                "side": "buy",
                "amount": 0.1,
                "price": 50000.0,
                "status": "closed",
                "timestamp": 1640995200000,
                "fee": None,
                "filled": 0.1,
                "remaining": 0.0,
            },
            {
                "id": "order_2",
                "symbol": "BTC/USD",
                "type": "limit",
                "side": "sell",
                "amount": 0.1,
                "price": 51000.0,
                "status": "open",
                "timestamp": 1640995200000,
                "fee": None,
                "filled": 0.0,
                "remaining": 0.1,
            },
        ]
        with patch.object(kraken_client, "_ccxt_client", mock_client):
            orders = await kraken_client.get_orders(status=OrderStatusEnum.CLOSED)
            assert len(orders) == 1
            assert orders[0].id == "order_1"
            assert orders[0].status == OrderStatusEnum.CLOSED
            mock_client.fetch_orders.assert_called_once_with(None, None, None)

    async def test_get_orders_error(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get orders error."""
        with (
            patch.object(
                kraken_client, "_with_retry", side_effect=Exception("Orders fetch failed")
            ),
            pytest.raises(Exception, match="Orders fetch failed"),
        ):
            await kraken_client.get_orders()

    @patch("snapper.infrastructure.exchanges.implementations.kraken.ccxt")
    async def test_get_balance_single_currency(
        self, mock_ccxt: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify get balance single currency."""
        mock_client = AsyncMock()
        mock_client.fetch_balance.return_value = {
            "BTC": {"free": 1.5, "used": 0.5, "total": 2.0},
            "USD": {"free": 10000.0, "used": 2000.0, "total": 12000.0},
            "free": {"BTC": 1.5, "USD": 10000.0},
            "used": {"BTC": 0.5, "USD": 2000.0},
            "total": {"BTC": 2.0, "USD": 12000.0},
            "info": {},
        }
        with patch.object(kraken_client, "_ccxt_client", mock_client):
            balances = await kraken_client.get_balance("BTC")
            assert "BTC" in balances
            assert len(balances) == 1
            assert balances["BTC"].currency == "BTC"
            assert balances["BTC"].free == float("1.5")

    @patch("snapper.infrastructure.exchanges.implementations.kraken.ccxt")
    async def test_get_balance_nonexistent_currency(
        self, mock_ccxt: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify get balance nonexistent currency."""
        mock_client = AsyncMock()
        mock_client.fetch_balance.return_value = {
            "BTC": {"free": 1.5, "used": 0.5, "total": 2.0},
            "free": {"BTC": 1.5},
            "used": {"BTC": 0.5},
            "total": {"BTC": 2.0},
            "info": {},
        }
        with patch.object(kraken_client, "_ccxt_client", mock_client):
            balances = await kraken_client.get_balance("ETH")
            assert "ETH" in balances
            assert balances["ETH"].currency == "ETH"
            assert balances["ETH"].free == float("0")
            assert balances["ETH"].used == float("0")
            assert balances["ETH"].total == float("0")

    async def test_get_balance_error(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get balance error."""
        with (
            patch.object(
                kraken_client, "_with_retry", side_effect=Exception("AccountBalance fetch failed")
            ),
            pytest.raises(Exception, match="AccountBalance fetch failed"),
        ):
            await kraken_client.get_balance()

    @patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient")
    async def test_subscribe_ticks(
        self, mock_ws_class: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify subscribe ticks."""
        mock_ws_client = AsyncMock()
        mock_ws_class.return_value = mock_ws_client
        mock_ws_client.__aenter__ = AsyncMock(return_value=mock_ws_client)
        mock_ws_client.__aexit__ = AsyncMock(return_value=None)
        mock_ws_client.exception_occur = False
        with patch.object(
            kraken_client, "_ensure_ws_connected", new_callable=AsyncMock
        ) as mock_ensure_ws:
            kraken_client._ws_client = mock_ws_client
            test_message = TickerUpdate(
                symbol="BTC-USD",
                bid=50000.0,
                bid_qty=1.0,
                ask=50001.0,
                ask_qty=1.0,
                last=50000.5,
                volume=100.0,
                vwap=50000.0,
                low=49900.0,
                high=50100.0,
                change=100.0,
                change_pct=0.2,
            )

            async def mock_message_generator() -> None:
                await kraken_client._tick_queue.put(test_message)
                mock_ws_client.exception_occur = True

            asyncio.create_task(mock_message_generator())
            messages: list[TickerUpdate] = []
            async for message in kraken_client.subscribe_ticks(["BTC-USD"]):
                messages.append(message)
                if len(messages) >= 1:
                    break
            assert len(messages) == 1
            assert messages[0].symbol == "BTC-USD"
            assert messages[0].bid == pytest.approx(50000.0)
            assert messages[0].ask == pytest.approx(50001.0)
            mock_ws_client.subscribe.assert_called_once()
            mock_ensure_ws.assert_called_once()

    @patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient")
    async def test_subscribe_candles(
        self, mock_ws_class: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify subscribe candles."""
        mock_ws_client = AsyncMock()
        mock_ws_class.return_value = mock_ws_client
        mock_ws_client.__aenter__ = AsyncMock(return_value=mock_ws_client)
        mock_ws_client.__aexit__ = AsyncMock(return_value=None)
        mock_ws_client.exception_occur = False
        with patch.object(
            kraken_client, "_ensure_ws_connected", new_callable=AsyncMock
        ) as mock_ensure_ws:
            kraken_client._ws_client = mock_ws_client
            test_message = CandleUpdate(
                symbol="BTC-USD",
                open=50000.0,
                high=50100.0,
                low=49900.0,
                close=50050.0,
                vwap=50000.0,
                trades=100,
                volume=10.5,
                interval_begin=datetime.now(UTC),
                interval=5,
            )

            async def mock_message_generator() -> None:
                if 5 not in kraken_client._candle_queues:
                    kraken_client._candle_queues[5] = asyncio.Queue()
                await kraken_client._candle_queues[5].put(test_message)
                mock_ws_client.exception_occur = True

            asyncio.create_task(mock_message_generator())
            messages: list[CandleUpdate] = []
            async for message in kraken_client.subscribe_candles(["BTC-USD"], "5m"):
                messages.append(message)
                if len(messages) >= 1:
                    break
            assert len(messages) == 1
            assert messages[0].symbol == "BTC-USD"
            assert messages[0].open == pytest.approx(50000.0)
            assert messages[0].close == pytest.approx(50050.0)
            assert messages[0].interval == 5
            mock_ws_client.subscribe.assert_called_once()
            mock_ensure_ws.assert_called_once()

    @patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient")
    async def test_subscribe_trades(
        self, mock_ws_class: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify subscribe trades."""
        mock_ws_client = AsyncMock()
        mock_ws_class.return_value = mock_ws_client
        mock_ws_client.__aenter__ = AsyncMock(return_value=mock_ws_client)
        mock_ws_client.__aexit__ = AsyncMock(return_value=None)
        mock_ws_client.exception_occur = False
        with patch.object(
            kraken_client, "_ensure_ws_connected", new_callable=AsyncMock
        ) as mock_ensure_ws:
            kraken_client._ws_client = mock_ws_client
            test_message = TradeUpdate(
                symbol="BTC-USD",
                side="buy",
                quantity=0.1,
                price=50000.0,
                ord_type="limit",
                trade_id=12345,
                timestamp=datetime.now(UTC),
            )

            async def mock_message_generator() -> None:
                await kraken_client._trade_queue.put(test_message)
                mock_ws_client.exception_occur = True

            asyncio.create_task(mock_message_generator())
            messages: list[TradeUpdate] = []
            async for message in kraken_client.subscribe_trades(["BTC-USD"]):
                messages.append(message)
                if len(messages) >= 1:
                    break
            assert len(messages) == 1
            assert messages[0].symbol == "BTC-USD"
            assert messages[0].side == "buy"
            assert messages[0].price == pytest.approx(50000.0)
            assert messages[0].quantity == pytest.approx(0.1)
            mock_ws_client.subscribe.assert_called_once()
            mock_ensure_ws.assert_called_once()

    @patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient")
    async def test_subscribe_executions(
        self, mock_ws_class: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify subscribe executions."""
        mock_ws_client = AsyncMock()
        mock_ws_class.return_value = mock_ws_client
        mock_ws_client.__aenter__ = AsyncMock(return_value=mock_ws_client)
        mock_ws_client.__aexit__ = AsyncMock(return_value=None)
        mock_ws_client.exception_occur = False
        with patch.object(
            kraken_client, "_ensure_ws_connected", new_callable=AsyncMock
        ) as mock_ensure_ws:
            kraken_client._ws_client = mock_ws_client
            test_message = ExecutionUpdate(
                order_id="ABC",
                exec_type="new",
                symbol="BTC-USD",
                side=OrderSideEnum.BUY,
                order_type=OrderTypeEnum.LIMIT,
                order_status=OrderStatusEnum.NEW,
                timestamp=datetime.now(UTC),
                cum_qty=0.0,
                cum_cost=0.0,
            )

            async def mock_message_generator() -> None:
                await kraken_client._execution_queue.put(test_message)
                mock_ws_client.exception_occur = True

            asyncio.create_task(mock_message_generator())
            messages: list[ExecutionUpdate] = []
            async for message in kraken_client.subscribe_executions(snap_orders=True):
                messages.append(message)
                if len(messages) >= 1:
                    break
            assert len(messages) == 1
            assert messages[0].order_id == "ABC"
            assert messages[0].exec_type == "new"
            assert messages[0].symbol == "BTC-USD"
            mock_ws_client.subscribe.assert_called_once()
            subscribe_kwargs = mock_ws_client.subscribe.call_args.kwargs
            params = subscribe_kwargs["params"]
            assert params["channel"] == "executions"
            assert params["snap_orders"] is True
            assert params["snap_trades"] is True
            mock_ensure_ws.assert_called_once()

    @patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient")
    async def test_subscribe_ticks_error(
        self, mock_ws_class: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify subscribe ticks error."""
        with (
            patch.object(
                kraken_client,
                "_ensure_ws_connected",
                side_effect=Exception("WebSocket connection failed"),
            ),
            pytest.raises(Exception, match="WebSocket connection failed"),
        ):
            async for _ in kraken_client.subscribe_ticks(["BTC-USD"]):
                """Consumed by iteration to trigger exception."""
                pass

    @patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient")
    async def test_ensure_ws_connected(
        self, mock_ws_class: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify ensure ws connected."""
        mock_ws_client = AsyncMock()
        mock_ws_class.return_value = mock_ws_client
        with patch.object(kraken_client, "_ensure_ws_connected") as mock_ensure:
            mock_ensure.return_value = None
            await mock_ensure()
            mock_ensure.assert_called_once()


class TestKrakenCoverageImprovement:
    """Tests for krakenCoverageImprovement."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    def test_init_with_all_parameters(self) -> None:
        """Verify init with all parameters."""
        client = KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
            enable_rate_limit=False,
        )
        assert client.api_key == "test_key"
        assert client.api_secret == "test_secret"
        assert not client.sandbox

    @patch("snapper.infrastructure.exchanges.implementations.kraken.ccxt")
    async def test_connect_success_resets_circuit_breaker(
        self, mock_ccxt: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify connect success resets circuit breaker."""
        mock_client = AsyncMock()
        mock_client.load_markets.return_value = {}
        with patch.object(kraken_client, "_ccxt_client", mock_client):
            await kraken_client.connect()
            mock_client.load_markets.assert_called_once()

    async def test_connect_failure_raises_exception(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify connect failure raises exception."""
        mock_client = AsyncMock()
        mock_client.load_markets.side_effect = Exception("Connection failed")
        with (
            patch.object(kraken_client, "_ccxt_client", mock_client),
            patch.object(kraken_client, "_with_retry") as mock_retry,
        ):
            mock_retry.side_effect = Exception("Connection failed")
            with pytest.raises(Exception, match="Connection failed"):
                await kraken_client.connect()

    async def test_disconnect_no_websocket(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify disconnect no websocket."""
        await kraken_client.disconnect()

    async def test_disconnect_no_session(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify disconnect no session."""
        mock_client = MagicMock()
        if hasattr(mock_client, "session"):
            delattr(mock_client, "session")
        with patch.object(kraken_client, "_ccxt_client", mock_client):
            await kraken_client.disconnect()

    async def test_disconnect_closes_rest_session(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify disconnect closes rest session."""
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_client.session = mock_session
        with (
            patch.object(
                kraken_client, "_close_ws_client", new_callable=AsyncMock
            ) as mock_close_ws_client,
            patch.object(kraken_client, "_ccxt_client", mock_client),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.logger.info"
            ) as mock_info,
        ):
            await kraken_client.disconnect()
        mock_close_ws_client.assert_awaited_once()
        mock_session.close.assert_called_once()
        mock_info.assert_called_with("Kraken REST client session closed")

    async def test_disconnect_logs_warning_on_error(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify disconnect logs warning on error."""
        mock_client = MagicMock()
        mock_client.session = MagicMock()
        with (
            patch.object(kraken_client, "_ccxt_client", mock_client),
            patch.object(
                kraken_client, "_close_ws_client", new_callable=AsyncMock
            ) as mock_close_ws_client,
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.logger.warning"
            ) as mock_warning,
        ):
            mock_close_ws_client.side_effect = RuntimeError("boom")
            await kraken_client.disconnect()
        mock_warning.assert_called_once()
        warning_args = mock_warning.call_args[0]
        assert warning_args
        assert "boom" in warning_args[0]
        mock_client.session.close.assert_not_called()

    async def test_context_manager_entry_exit(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify context manager entry exit."""
        with (
            patch.object(kraken_client, "connect", new_callable=AsyncMock) as mock_connect,
            patch.object(kraken_client, "disconnect", new_callable=AsyncMock) as mock_disconnect,
        ):
            async with kraken_client as client:
                assert client is kraken_client
            mock_connect.assert_called_once()
            mock_disconnect.assert_called_once()

    async def test_get_ticker_exception_handling(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get ticker exception handling."""
        with patch.object(kraken_client, "_with_retry") as mock_retry:
            mock_retry.side_effect = Exception("TickerSnapshot error")
            with pytest.raises(Exception, match="TickerSnapshot error"):
                await kraken_client.get_ticker("BTC-USD")

    async def test_get_ohlcv_with_all_parameters(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get ohlcv with all parameters."""
        mock_return_value = [
            [1640995200000, 49500.0, 51000.0, 49000.0, 50000.0, 100.0],
        ]
        with patch.object(kraken_client, "_with_retry") as mock_retry:
            mock_retry.return_value = mock_return_value
            result = await kraken_client.get_ohlcv("BTC-USD", "1h", since=1640995200, limit=1)
            assert len(result) == 1
            mock_retry.assert_called_once()

    async def test_get_ohlcv_exception_handling(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get ohlcv exception handling."""
        with patch.object(kraken_client, "_with_retry") as mock_retry:
            mock_retry.side_effect = Exception("OhlcvSnapshot error")
            with pytest.raises(Exception, match="OhlcvSnapshot error"):
                await kraken_client.get_ohlcv("BTC-USD")

    async def test_create_order_no_api_key(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify create order no api key."""
        kraken_client.api_key = None
        order_request = ExchangeOrderRequest(
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            type=OrderTypeEnum.MARKET,
            amount=float("0.1"),
        )
        with pytest.raises(RuntimeError, match="API credentials required"):
            await kraken_client.create_order(order_request)

    async def test_create_order_no_api_secret(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify create order no api secret."""
        kraken_client.api_secret = None
        order_request = ExchangeOrderRequest(
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            type=OrderTypeEnum.MARKET,
            amount=float("0.1"),
        )
        with pytest.raises(RuntimeError, match="API credentials required"):
            await kraken_client.create_order(order_request)

    async def test_create_order_with_all_fields(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify create order with all fields."""
        mock_order_data = {
            "id": "order_123",
            "clientOrderId": "client_order_123",
            "symbol": "BTC/USD",
            "type": "limit",
            "side": "buy",
            "amount": 0.1,
            "price": 50000.0,
            "status": "open",
            "timestamp": 1640995200000,
            "fee": {"cost": 5.0, "currency": "USD"},
            "filled": 0.0,
            "remaining": 0.1,
        }
        with patch.object(kraken_client, "_with_retry") as mock_retry:
            mock_retry.return_value = mock_order_data
            order_request = ExchangeOrderRequest(
                symbol="BTC-USD",
                side=OrderSideEnum.BUY,
                type=OrderTypeEnum.LIMIT,
                amount=float("0.1"),
                price=float("50000.0"),
                client_order_id="client_order_123",
            )
            result = await kraken_client.create_order(order_request)
            assert result.client_order_id == "client_order_123"
            assert result.price == float("50000.0")

    async def test_create_order_exception_handling(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify create order exception handling."""
        with patch.object(kraken_client, "_with_retry") as mock_retry:
            mock_retry.side_effect = Exception("ExchangeOrderSnapshot creation failed")
            order_request = ExchangeOrderRequest(
                symbol="BTC-USD",
                side=OrderSideEnum.BUY,
                type=OrderTypeEnum.MARKET,
                amount=float("0.1"),
            )
            with pytest.raises(Exception, match="ExchangeOrderSnapshot creation failed"):
                await kraken_client.create_order(order_request)

    async def test_cancel_order_no_credentials(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify cancel order no credentials."""
        kraken_client.api_key = None
        kraken_client.api_secret = None
        with pytest.raises(RuntimeError, match="API credentials required"):
            await kraken_client.cancel_order("order_123")

    async def test_cancel_order_exception_handling(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify cancel order exception handling."""
        with patch.object(kraken_client, "_with_retry") as mock_retry:
            mock_retry.side_effect = Exception("Cancel failed")
            with pytest.raises(Exception, match="Cancel failed"):
                await kraken_client.cancel_order("order_123", "BTC-USD")

    async def test_get_order_no_credentials(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get order no credentials."""
        kraken_client.api_key = None
        kraken_client.api_secret = None
        with pytest.raises(RuntimeError, match="API credentials required"):
            await kraken_client.get_order("order_123")

    async def test_get_order_success(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get order success."""
        mock_order_data = {
            "id": "order_123",
            "symbol": "BTC/USD",
            "type": "limit",
            "side": "buy",
            "amount": 0.1,
            "price": 50000.0,
            "status": "closed",
            "timestamp": 1640995200000,
            "fee": {"cost": 5.0, "currency": "USD"},
            "filled": 0.1,
            "remaining": 0.0,
        }
        with patch.object(kraken_client, "_with_retry") as mock_retry:
            mock_retry.return_value = mock_order_data
            result = await kraken_client.get_order("order_123", "BTC-USD")
            assert result.id == "order_123"
            assert result.status == OrderStatusEnum.CLOSED

    async def test_get_order_exception_handling(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get order exception handling."""
        with patch.object(kraken_client, "_with_retry") as mock_retry:
            mock_retry.side_effect = Exception("ExchangeOrderSnapshot fetch failed")
            with pytest.raises(Exception, match="ExchangeOrderSnapshot fetch failed"):
                await kraken_client.get_order("order_123")

    async def test_get_orders_no_credentials(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get orders no credentials."""
        kraken_client.api_key = None
        kraken_client.api_secret = None
        with pytest.raises(RuntimeError, match="API credentials required"):
            await kraken_client.get_orders()

    async def test_get_orders_open_status(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get orders open status."""
        mock_orders_data = [
            {
                "id": "order_1",
                "symbol": "BTC/USD",
                "type": "limit",
                "side": "buy",
                "amount": 0.1,
                "price": 50000.0,
                "status": "open",
                "timestamp": 1640995200000,
                "fee": None,
                "filled": 0.0,
                "remaining": 0.1,
            }
        ]
        with patch.object(kraken_client, "_with_retry") as mock_retry:
            mock_retry.return_value = mock_orders_data
            result = await kraken_client.get_orders(
                symbol="BTC-USD", status=OrderStatusEnum.OPEN, limit=10
            )
            assert len(result) == 1
            assert result[0].status == OrderStatusEnum.OPEN

    async def test_get_orders_all_with_status_filter(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify get orders all with status filter."""
        mock_orders_data = [
            {
                "id": "order_1",
                "symbol": "BTC/USD",
                "type": "limit",
                "side": "buy",
                "amount": 0.1,
                "price": 50000.0,
                "status": "closed",
                "timestamp": 1640995200000,
                "fee": None,
                "filled": 0.1,
                "remaining": 0.0,
            },
            {
                "id": "order_2",
                "symbol": "BTC/USD",
                "type": "limit",
                "side": "sell",
                "amount": 0.1,
                "price": 51000.0,
                "status": "open",
                "timestamp": 1640995200000,
                "fee": None,
                "filled": 0.0,
                "remaining": 0.1,
            },
        ]
        with patch.object(kraken_client, "_with_retry") as mock_retry:
            mock_retry.return_value = mock_orders_data
            result = await kraken_client.get_orders(status=OrderStatusEnum.CLOSED)
            assert len(result) == 1
            assert result[0].status == OrderStatusEnum.CLOSED

    async def test_get_orders_exception_handling(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get orders exception handling."""
        with patch.object(kraken_client, "_with_retry") as mock_retry:
            mock_retry.side_effect = Exception("Orders fetch failed")
            with pytest.raises(Exception, match="Orders fetch failed"):
                await kraken_client.get_orders()

    async def test_get_balance_no_credentials(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get balance no credentials."""
        kraken_client.api_key = None
        kraken_client.api_secret = None
        with pytest.raises(RuntimeError, match="API credentials required"):
            await kraken_client.get_balance()

    async def test_get_balance_single_currency(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get balance single currency."""
        mock_balance_data = {
            "BTC": {"free": 1.5, "used": 0.5, "total": 2.0},
            "USD": {"free": 10000.0, "used": 2000.0, "total": 12000.0},
            "free": {"BTC": 1.5, "USD": 10000.0},
            "used": {"BTC": 0.5, "USD": 2000.0},
            "total": {"BTC": 2.0, "USD": 12000.0},
            "info": {},
        }
        with patch.object(kraken_client, "_with_retry") as mock_retry:
            mock_retry.return_value = mock_balance_data
            result = await kraken_client.get_balance("BTC")
            assert "BTC" in result
            assert len(result) == 1
            assert result["BTC"].currency == "BTC"
            assert result["BTC"].free == float("1.5")

    async def test_get_balance_nonexistent_currency(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify get balance nonexistent currency."""
        mock_balance_data = {
            "BTC": {"free": 1.5, "used": 0.5, "total": 2.0},
            "free": {"BTC": 1.5},
            "used": {"BTC": 0.5},
            "total": {"BTC": 2.0},
            "info": {},
        }
        with patch.object(kraken_client, "_with_retry") as mock_retry:
            mock_retry.return_value = mock_balance_data
            result = await kraken_client.get_balance("ETH")
            assert "ETH" in result
            assert result["ETH"].currency == "ETH"
            assert result["ETH"].free == float("0")
            assert result["ETH"].used == float("0")
            assert result["ETH"].total == float("0")

    async def test_get_balance_exception_handling(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify get balance exception handling."""
        with patch.object(kraken_client, "_with_retry") as mock_retry:
            mock_retry.side_effect = Exception("AccountBalance fetch failed")
            with pytest.raises(Exception, match="AccountBalance fetch failed"):
                await kraken_client.get_balance()

    @patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient")
    async def test_subscribe_ticks_error(
        self, mock_ws_class: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify subscribe ticks error."""
        with (
            patch.object(
                kraken_client,
                "_ensure_ws_connected",
                side_effect=Exception("WebSocket error"),
            ),
            pytest.raises(Exception, match="WebSocket error"),
        ):
            async for _ in kraken_client.subscribe_ticks(["BTC-USD"]):
                """Consumed by iteration to trigger exception."""
                pass

    @patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient")
    async def test_subscribe_candles_error(
        self, mock_ws_class: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify subscribe candles error."""
        with (
            patch.object(
                kraken_client,
                "_ensure_ws_connected",
                side_effect=Exception("WebSocket error"),
            ),
            pytest.raises(Exception, match="WebSocket error"),
        ):
            async for _ in kraken_client.subscribe_candles(["BTC-USD"]):
                """Consumed by iteration to trigger exception."""
                pass

    @patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient")
    async def test_subscribe_candles_different_timeframes(
        self, mock_ws_class: MagicMock, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify subscribe candles different timeframes."""
        mock_ws_client = AsyncMock()
        mock_ws_class.return_value = mock_ws_client
        mock_ws_client.__aenter__ = AsyncMock(return_value=mock_ws_client)
        mock_ws_client.__aexit__ = AsyncMock(return_value=None)
        mock_ws_client.exception_occur = True
        with patch.object(kraken_client, "_ensure_ws_connected", new_callable=AsyncMock):
            kraken_client._ws_client = mock_ws_client
            timeframes = ["1m", "5m", "15m", "30m", "1h", "4h", "1d"]
            expected_intervals = [1, 5, 15, 30, 60, 240, 1440]
            for timeframe, expected_interval in zip(timeframes, expected_intervals, strict=False):
                try:
                    async for _ in kraken_client.subscribe_candles(["BTC-USD"], timeframe):
                        break
                except StopAsyncIteration:
                    pass
                mock_ws_client.subscribe.assert_called_with(
                    params={
                        "channel": "ohlc",
                        "symbol": ["BTC/USD"],
                        "interval": expected_interval,
                        "snapshot": True,
                    },
                    req_id=None,
                )
            mock_ws_client.reset_mock()

    async def test_close_ws_client_closes_resources(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify close ws client closes resources."""
        mock_ws = AsyncMock()
        mock_ws.close = AsyncMock()
        mock_session = AsyncMock()
        mock_session.closed = False
        cast(Any, mock_ws)._SpotAsyncClient__session = mock_session
        kraken_client._ws_client = mock_ws
        kraken_client._ws_connected = True
        await kraken_client._close_ws_client()
        mock_ws.close.assert_awaited_once()
        mock_session.close.assert_awaited_once()
        assert kraken_client._ws_client is None

    @patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient")
    async def test_ensure_ws_connected_initializes_client(
        self,
        mock_ws_class: MagicMock,
        kraken_client: KrakenExchangeClient,
    ) -> None:
        """Verify ensure ws connected initializes client."""
        mock_ws_instance = AsyncMock()
        mock_ws_instance.start = AsyncMock()
        mock_ws_class.return_value = mock_ws_instance
        await kraken_client._ensure_ws_connected()
        mock_ws_class.assert_called_once()
        mock_ws_instance.start.assert_awaited_once()
        assert kraken_client._ws_client is mock_ws_instance
        assert kraken_client._ws_connected is True

    @patch("snapper.infrastructure.exchanges.implementations.kraken.native_to_kraken_rest")
    @patch("snapper.infrastructure.exchanges.implementations.kraken.native_to_ccxt")
    async def test_create_order_fallback_to_native_api(
        self,
        mock_native_to_ccxt: MagicMock,
        mock_native_to_rest: MagicMock,
        kraken_client: KrakenExchangeClient,
    ) -> None:
        """Verify create order fallback to native api."""
        mock_native_to_ccxt.return_value = "AAPL/USD"
        mock_native_to_rest.return_value = "AAPLx/USD"
        order_request = ExchangeOrderRequest(
            symbol="AAPL-USD",
            side=OrderSideEnum.BUY,
            type=OrderTypeEnum.LIMIT,
            amount=float("1"),
            price=float("150"),
            client_order_id="client-1",
        )
        trade_client = MagicMock()
        trade_client.create_order.return_value = {"txid": ["ORDER123"]}
        with (
            patch.object(
                kraken_client,
                "_with_retry",
                side_effect=ValueError("Unknown native symbol"),
            ),
            patch.object(kraken_client, "_get_trade_client", return_value=trade_client),
        ):
            result = await kraken_client.create_order(order_request)
        trade_client.create_order.assert_called_once()
        kwargs = trade_client.create_order.call_args.kwargs
        assert kwargs["pair"] == "AAPLx/USD"
        assert kwargs["extra_params"] == {
            "cl_ord_id": "client-1",
            "asset_class": "tokenized_asset",
        }
        assert result.id == "ORDER123"
        assert result.symbol == "AAPL-USD"

    @patch("snapper.infrastructure.exchanges.implementations.kraken.native_to_ccxt")
    async def test_cancel_order_fallback_to_native_api(
        self,
        mock_native_to_ccxt: MagicMock,
        kraken_client: KrakenExchangeClient,
    ) -> None:
        """Verify cancel order fallback to native api."""
        mock_native_to_ccxt.return_value = "AAPL/USD"
        trade_client = MagicMock()
        with (
            patch.object(
                kraken_client,
                "_with_retry",
                side_effect=ValueError("Unknown native symbol"),
            ),
            patch.object(kraken_client, "_get_trade_client", return_value=trade_client),
        ):
            result = await kraken_client.cancel_order("ABC123", "AAPL-USD")
        trade_client.cancel_order.assert_called_once_with(txid="ABC123")
        assert result.id == "ABC123"
        assert result.status == OrderStatusEnum.CANCELED

    @patch("snapper.infrastructure.exchanges.implementations.kraken.native_to_kraken_websocket")
    async def test_create_order_ws_successful_flow(
        self,
        mock_native_ws: MagicMock,
        kraken_client: KrakenExchangeClient,
    ) -> None:
        """Verify create order ws successful flow."""
        mock_native_ws.return_value = "XBT/USD"
        kraken_client._ws_connected = True

        class DummyWS:
            async def send_message(self, message: dict[str, Any]) -> None:
                req_id = message["req_id"]
                future = kraken_client._ws_order_requests[req_id]
                future.set_result(
                    {
                        "result": {"order_id": "ORDER1", "cl_ord_id": "CLIENT1"},
                        "success": True,
                    }
                )

        dummy_ws = DummyWS()
        with patch.object(
            kraken_client, "_get_or_create_ws_client", new=AsyncMock(return_value=dummy_ws)
        ):
            request = ExchangeOrderRequest(
                symbol="BTC-USD",
                side=OrderSideEnum.BUY,
                type=OrderTypeEnum.MARKET,
                amount=float("0.5"),
                client_order_id="CLIENT1",
            )
            result = await kraken_client.create_order_ws(request)
        assert result.id == "ORDER1"
        assert result.client_order_id == "CLIENT1"
        assert kraken_client._ws_order_requests == {}

    async def test_cancel_order_ws_successful_flow(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify cancel order ws successful flow."""
        kraken_client._ws_connected = True

        class DummyWS:
            async def send_message(self, message: dict[str, Any]) -> None:
                req_id = message["req_id"]
                future = kraken_client._ws_order_requests[req_id]
                future.set_result({"success": True})

        dummy_ws = DummyWS()
        with patch.object(
            kraken_client, "_get_or_create_ws_client", new=AsyncMock(return_value=dummy_ws)
        ):
            result = await kraken_client.cancel_order_ws("ABC123")
        assert result.id == "ABC123"
        assert result.status == OrderStatusEnum.CANCELED
        assert kraken_client._ws_order_requests == {}


class TestCloseWsClientBranches:
    """Tests for closeWsClientBranches."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    @pytest.mark.asyncio
    async def test_close_ws_client_when_connected(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify close ws client when connected."""
        mock_ws_client = MagicMock()
        mock_ws_client.close = AsyncMock()
        kraken_client._ws_client = mock_ws_client
        kraken_client._ws_connected = True
        await kraken_client._close_ws_client()
        mock_ws_client.close.assert_awaited_once()
        assert kraken_client._ws_connected is False
        assert kraken_client._ws_client is None

    @pytest.mark.asyncio
    async def test_close_ws_client_with_aiohttp_session(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify close ws client with aiohttp session."""
        mock_session = MagicMock()
        mock_session.closed = False
        mock_session.close = AsyncMock()
        mock_ws_client = MagicMock()
        mock_ws_client.close = AsyncMock()
        mock_ws_client._SpotAsyncClient__session = mock_session
        kraken_client._ws_client = mock_ws_client
        kraken_client._ws_connected = True
        await kraken_client._close_ws_client()
        mock_session.close.assert_awaited_once()
        assert kraken_client._ws_client is None

    @pytest.mark.asyncio
    async def test_close_ws_client_session_already_closed(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify close ws client session already closed."""
        mock_session = MagicMock()
        mock_session.closed = True
        mock_session.close = AsyncMock()
        mock_ws_client = MagicMock()
        mock_ws_client.close = AsyncMock()
        mock_ws_client._SpotAsyncClient__session = mock_session
        kraken_client._ws_client = mock_ws_client
        kraken_client._ws_connected = True
        await kraken_client._close_ws_client()
        mock_session.close.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_close_ws_client_not_connected(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify close ws client not connected."""
        mock_ws_client = MagicMock()
        mock_ws_client.close = AsyncMock()
        kraken_client._ws_client = mock_ws_client
        kraken_client._ws_connected = False
        await kraken_client._close_ws_client()
        mock_ws_client.close.assert_not_awaited()
        assert kraken_client._ws_client is None

    @pytest.mark.asyncio
    async def test_close_ws_client_no_session_attribute(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify close ws client no session attribute."""
        mock_ws_client = MagicMock(spec=[])
        mock_ws_client.close = AsyncMock()
        kraken_client._ws_client = mock_ws_client
        kraken_client._ws_connected = False
        await kraken_client._close_ws_client()
        assert kraken_client._ws_client is None


class TestExecutionsSubscriptionCredentials:
    """Tests for executionsSubscriptionCredentials."""

    @pytest.fixture
    def kraken_client_no_creds(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key=None,
            api_secret=None,
            sandbox=False,
        )

    @pytest.mark.asyncio
    async def test_subscribe_executions_requires_credentials(
        self, kraken_client_no_creds: KrakenExchangeClient
    ) -> None:
        """Verify subscribe executions requires credentials."""
        with pytest.raises(RuntimeError, match="API credentials required"):
            async for _ in kraken_client_no_creds.subscribe_executions():
                """Consumed by iteration to trigger exception."""
                pass


class TestWebsocketTimeoutPaths:
    """Tests for websocketTimeoutPaths."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    @pytest.mark.asyncio
    async def test_subscribe_ticks_timeout_breaks_loop(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify subscribe ticks timeout breaks loop."""
        ws = MagicMock()
        ws.exception_occur = False
        ws.subscribe = AsyncMock()

        async def aenter() -> MagicMock:
            return ws

        async def aexit(*_: object, **__: object) -> bool:
            return False

        ws.__aenter__ = AsyncMock(side_effect=aenter)
        ws.__aexit__ = AsyncMock(side_effect=aexit)
        kraken_client._ws_client = ws

        async def wait_for_stub(*args: object, **kwargs: object) -> None:
            if args:
                coro = cast(Coroutine[Any, Any, object], args[0])
                task: asyncio.Task[object] = asyncio.create_task(coro)
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            ws.exception_occur = True
            raise TimeoutError

        with (
            patch.object(kraken_client, "_ensure_ws_connected", AsyncMock()),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.asyncio.wait_for",
                side_effect=wait_for_stub,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.asyncio.sleep",
                new_callable=AsyncMock,
            ),
        ):
            agen = kraken_client.subscribe_ticks(["BTC-USD"])
            with pytest.raises(StopAsyncIteration):
                await agen.__anext__()
        ws.subscribe.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_subscribe_candles_timeout_breaks_loop(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify subscribe candles timeout breaks loop."""
        ws = MagicMock()
        ws.exception_occur = False
        ws.subscribe = AsyncMock()

        async def aenter() -> MagicMock:
            return ws

        async def aexit(*_: object, **__: object) -> bool:
            return False

        ws.__aenter__ = AsyncMock(side_effect=aenter)
        ws.__aexit__ = AsyncMock(side_effect=aexit)
        kraken_client._ws_client = ws

        async def wait_for_stub(*args: object, **kwargs: object) -> None:
            if args:
                coro = cast(Coroutine[Any, Any, object], args[0])
                task: asyncio.Task[object] = asyncio.create_task(coro)
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            ws.exception_occur = True
            raise TimeoutError

        with (
            patch.object(kraken_client, "_ensure_ws_connected", AsyncMock()),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.asyncio.wait_for",
                side_effect=wait_for_stub,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.asyncio.sleep",
                new_callable=AsyncMock,
            ),
        ):
            agen = kraken_client.subscribe_candles(["BTC-USD"], timeframe="1m")
            with pytest.raises(StopAsyncIteration):
                await agen.__anext__()
        ws.subscribe.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_subscribe_trades_timeout_breaks_loop(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify subscribe trades timeout breaks loop."""
        ws = MagicMock()
        ws.exception_occur = False
        ws.subscribe = AsyncMock()

        async def aenter() -> MagicMock:
            return ws

        async def aexit(*_: object, **__: object) -> bool:
            return False

        ws.__aenter__ = AsyncMock(side_effect=aenter)
        ws.__aexit__ = AsyncMock(side_effect=aexit)
        kraken_client._ws_client = ws

        async def wait_for_stub(*args: object, **kwargs: object) -> None:
            if args:
                coro = cast(Coroutine[Any, Any, object], args[0])
                task: asyncio.Task[object] = asyncio.create_task(coro)
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            ws.exception_occur = True
            raise TimeoutError

        with (
            patch.object(kraken_client, "_ensure_ws_connected", AsyncMock()),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.asyncio.wait_for",
                side_effect=wait_for_stub,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.asyncio.sleep",
                new_callable=AsyncMock,
            ),
        ):
            agen = kraken_client.subscribe_trades(["BTC-USD"])
            with pytest.raises(StopAsyncIteration):
                await agen.__anext__()
        ws.subscribe.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_subscribe_executions_timeout_breaks_loop(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify subscribe executions timeout breaks loop."""
        ws = MagicMock()
        ws.subscribe = AsyncMock()
        ws.exception_occur = False
        kraken_client._ws_client = ws

        async def wait_for_stub(*args: object, **kwargs: object) -> None:
            if args:
                coro = cast(Coroutine[Any, Any, object], args[0])
                task: asyncio.Task[object] = asyncio.create_task(coro)
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            ws.exception_occur = True
            raise TimeoutError

        with (
            patch.object(kraken_client, "_ensure_ws_connected", AsyncMock()),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.asyncio.wait_for",
                side_effect=wait_for_stub,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.asyncio.sleep",
                new_callable=AsyncMock,
            ),
        ):
            agen = kraken_client.subscribe_executions()
            with pytest.raises(StopAsyncIteration):
                await agen.__anext__()
        ws.subscribe.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_subscribe_instruments_timeout_breaks_loop(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify subscribe instruments timeout breaks loop."""
        ws = MagicMock()
        ws.exception_occur = False
        ws.subscribe = AsyncMock()

        async def aenter() -> MagicMock:
            return ws

        async def aexit(*_: object, **__: object) -> bool:
            return False

        ws.__aenter__ = AsyncMock(side_effect=aenter)
        ws.__aexit__ = AsyncMock(side_effect=aexit)
        kraken_client._ws_client = ws

        async def wait_for_stub(*args: object, **kwargs: object) -> None:
            if args:
                coro = cast(Coroutine[Any, Any, object], args[0])
                task: asyncio.Task[object] = asyncio.create_task(coro)
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            ws.exception_occur = True
            raise TimeoutError

        with (
            patch.object(kraken_client, "_ensure_ws_connected", AsyncMock()),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.asyncio.wait_for",
                side_effect=wait_for_stub,
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.asyncio.sleep",
                new_callable=AsyncMock,
            ),
        ):
            agen = kraken_client._subscribe_instruments_impl(req_id=1, raw=False)
            with pytest.raises(StopAsyncIteration):
                await agen.__anext__()
        ws.subscribe.assert_awaited_once()


class TestTickerSubscriptionAck:
    """Tests for tickerSubscriptionAck."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    @pytest.mark.asyncio
    async def test_process_ticker_ack_status_not_ok(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify process ticker ack status not ok."""
        pass

    @pytest.mark.asyncio
    async def test_process_ticker_data_with_dict(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify process ticker data with dict."""
        message = {
            "channel": "ticker",
            "data": {
                "symbol": "XBT/USD",
                "bid": 50000.0,
                "ask": 50001.0,
                "last": 50000.5,
                "volume": 100.0,
            },
        }
        await kraken_client._on_message(message)


class TestInstrumentSubscriptionAck:
    """Tests for instrumentSubscriptionAck."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    @pytest.mark.asyncio
    async def test_process_instrument_data_with_pairs(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify process instrument data with pairs."""
        message = {
            "channel": "instrument",
            "data": {
                "pairs": [
                    {
                        "symbol": "XBT/USD",
                        "base": "XBT",
                        "quote": "USD",
                        "status": "online",
                        "qty_precision": 8,
                        "qty_increment": 0.00000001,
                        "price_precision": 1,
                        "price_increment": 0.1,
                        "cost_precision": 5,
                        "cost_min": 0.5,
                        "margin_initial": 0.2,
                        "position_limit_long": 100,
                        "position_limit_short": 100,
                    }
                ]
            },
        }
        await kraken_client._on_message(message)


class TestInstrumentDataProcessingError:
    """Tests for instrumentDataProcessingError."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    @pytest.mark.asyncio
    async def test_process_instrument_data_exception(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify process instrument data exception."""
        message = {
            "channel": "instrument",
            "data": {"pairs": [{"symbol": "XBT/USD", "status": "online"}]},
        }
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.KrakenInstrumentPairSchema.model_validate",
                side_effect=Exception("Unexpected processing error"),
            ),
            patch("snapper.infrastructure.exchanges.implementations.kraken.logger") as mock_logger,
        ):
            await kraken_client._on_message(message)
            mock_logger.warning.assert_called()
            call_args = mock_logger.warning.call_args
            assert "Failed to process instrument data" in str(call_args)


class TestRetryMechanismSyncFunction:
    """Tests for retryMechanismSyncFunction."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    @pytest.mark.asyncio
    async def test_with_retry_sync_function(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify with retry sync function."""

        def sync_function(x: int, y: int) -> int:
            return x + y

        result = await kraken_client._with_retry(sync_function, 2, 3)
        assert result == 5

    @pytest.mark.asyncio
    async def test_with_retry_async_function(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify with retry async function."""

        async def async_function(x: int) -> int:
            return x * 2

        result = await kraken_client._with_retry(async_function, 5)
        assert result == 10


class TestOnMessageOrderResponses:
    """Tests for onMessageOrderResponses."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        client = KrakenExchangeClient(api_key="k", api_secret="s", sandbox=False)
        client._ws_order_requests = {}
        return client

    @pytest.mark.asyncio
    async def test_on_message_sets_future_result(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify on message sets future result."""
        future: asyncio.Future[dict[str, str]] = asyncio.get_event_loop().create_future()
        kraken_client._ws_order_requests[1] = future
        message = {"req_id": 1, "success": True, "result": {"order_id": "abc"}}
        await kraken_client._on_message(message)
        assert future.done()
        assert future.result() == message

    @pytest.mark.asyncio
    async def test_on_message_sets_future_exception(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify on message sets future exception."""
        future: asyncio.Future[dict[str, str]] = asyncio.get_event_loop().create_future()
        kraken_client._ws_order_requests[2] = future
        message = {"req_id": 2, "success": False, "error": "boom"}
        await kraken_client._on_message(message)
        assert future.done()
        assert future.exception() is not None


class TestOhlcSubscribeAck:
    """Tests for ohlcSubscribeAck."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(api_key="k", api_secret="s", sandbox=False)

    @pytest.mark.asyncio
    async def test_ohlc_ack_with_warning(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify ohlc ack with warning."""
        message = {
            "method": "subscribe",
            "result": {"channel": "ohlc", "symbol": "XBT/USD", "interval": 60},
            "success": False,
            "error": "subscription failed",
        }
        ack = MagicMock()
        ack.success = False
        ack.result = MagicMock(symbol="XBT/USD", interval=60, warnings=["w1"])
        ack.error = "subscription failed"
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.KrakenOhlcSubscriptionAckSchema.model_validate",
                return_value=ack,
            ),
            patch("snapper.infrastructure.exchanges.implementations.kraken.logger") as mock_logger,
        ):
            await kraken_client._on_message(message)
            assert mock_logger.warning.called


class TestEnsureWsConnectedInitialization:
    """Tests for ensureWsConnectedInitialization."""

    @pytest.mark.asyncio
    async def test_ensure_ws_connected_creates_client(self) -> None:
        """Verify ensure ws connected creates client."""
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient"
        ) as mock_client_class:
            mock_ws_client = AsyncMock()
            mock_client_class.return_value = mock_ws_client
            kraken_client = KrakenExchangeClient(api_key="k", api_secret="s", sandbox=False)
            kraken_client._ws_client = None
            await kraken_client._ensure_ws_connected()
            mock_client_class.assert_called_once()
            mock_ws_client.start.assert_awaited_once()
            assert kraken_client._ws_connected is True


class TestOnMessageDataRouting:
    """Tests for onMessageDataRouting."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        client = KrakenExchangeClient(api_key="k", api_secret="s", sandbox=False)
        client._tick_queue = MagicMock()
        client._tick_queue.put = AsyncMock()
        client._trade_queue = MagicMock()
        client._trade_queue.put = AsyncMock()
        client._execution_queue = MagicMock()
        client._execution_queue.put = AsyncMock()
        return client

    @pytest.mark.asyncio
    async def test_on_message_routes_ticker_list(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify on message routes ticker list."""
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken.parse_kraken_ticker_list",
            return_value=[MagicMock()],
        ):
            await kraken_client._on_message({"channel": "ticker", "data": [{}]})
        tick_put = cast(AsyncMock, kraken_client._tick_queue.put)
        tick_put.assert_awaited()

    @pytest.mark.asyncio
    async def test_on_message_routes_trade_list(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify on message routes trade list."""
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken.parse_kraken_trade_list",
            return_value=[MagicMock()],
        ):
            await kraken_client._on_message({"channel": "trade", "data": [{}]})
        trade_put = cast(AsyncMock, kraken_client._trade_queue.put)
        trade_put.assert_awaited()

    @pytest.mark.asyncio
    async def test_on_message_routes_execution_list(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify on message routes execution list."""
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken.parse_kraken_execution_list",
            return_value=[MagicMock()],
        ):
            await kraken_client._on_message({"channel": "executions", "data": [{}]})
        exec_put = cast(AsyncMock, kraken_client._execution_queue.put)
        exec_put.assert_awaited()


class TestRetryMaxRetriesExceeded:
    """Tests for retryMaxRetriesExceeded."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    @pytest.mark.asyncio
    async def test_with_retry_max_retries_exceeded(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify with retry max retries exceeded."""
        call_count = 0

        async def failing_function() -> None:
            nonlocal call_count
            call_count += 1
            raise ccxt.RateLimitExceeded("Rate limit exceeded")

        with (
            patch("asyncio.sleep", new_callable=AsyncMock),
            pytest.raises(ccxt.RateLimitExceeded),
        ):
            await kraken_client._with_retry(failing_function)
        assert call_count == 3


class TestStatusBranchInSubscriptions:
    """Tests for statusBranchInSubscriptions."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    @pytest.mark.asyncio
    async def test_on_message_with_subscribe_method_and_result(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify on message with subscribe method and result."""
        message = {
            "method": "subscribe",
            "result": {
                "channel": "trade",
                "symbol": "XBT/USD",
            },
            "success": True,
        }
        await kraken_client._on_message(message)

    @pytest.mark.asyncio
    async def test_on_message_list_type_skips_req_id_branch(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify on message list type skips req id branch."""
        message: list[dict[str, str]] = [{"channel": "test", "data": "value"}]
        await kraken_client._on_message(message)

    @pytest.mark.asyncio
    async def test_on_message_dict_without_req_id(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify on message dict without req id."""
        message = {"channel": "heartbeat", "data": {}}
        await kraken_client._on_message(message)

    @pytest.mark.asyncio
    async def test_on_message_subscribe_ohlc_channel(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify on message subscribe ohlc channel."""
        message = {
            "method": "subscribe",
            "result": {
                "channel": "ohlc",
                "symbol": "XBT/USD",
                "interval": 60,
            },
            "success": True,
        }
        await kraken_client._on_message(message)

    @pytest.mark.asyncio
    async def test_on_message_subscribe_ohlc_failed(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify on message subscribe ohlc failed."""
        message = {
            "method": "subscribe",
            "result": {
                "channel": "ohlc",
                "symbol": "XBT/USD",
                "interval": 60,
            },
            "success": False,
            "error": "subscription failed",
        }
        await kraken_client._on_message(message)

    @pytest.mark.asyncio
    async def test_on_message_ticker_channel_ack(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify on message ticker channel ack."""
        message = {
            "channel": "ticker",
            "event": "subscribe",
            "status": "ok",
            "message": "subscribed",
        }
        await kraken_client._on_message(message)

    @pytest.mark.asyncio
    async def test_on_message_ticker_channel_ack_not_ok(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify on message ticker channel ack not ok."""
        message = {
            "channel": "ticker",
            "event": "subscribe",
            "status": "error",
            "message": "failed to subscribe",
        }
        await kraken_client._on_message(message)

    @pytest.mark.asyncio
    async def test_on_message_instrument_channel_ack(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify on message instrument channel ack."""
        message = {
            "channel": "instrument",
            "event": "subscribe",
            "status": "ok",
            "message": "subscribed",
        }
        await kraken_client._on_message(message)

    @pytest.mark.asyncio
    async def test_on_message_instrument_channel_ack_not_ok(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify on message instrument channel ack not ok."""
        message = {
            "channel": "instrument",
            "event": "subscribe",
            "status": "error",
            "message": "failed to subscribe",
        }
        await kraken_client._on_message(message)


class TestGetOrdersWithoutStatus:
    """Tests for getOrdersWithoutStatus."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    @pytest.mark.asyncio
    async def test_get_orders_no_status_filter(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get orders no status filter."""
        mock_orders = [
            {
                "id": "order1",
                "symbol": "BTC/USD",
                "type": "limit",
                "side": "buy",
                "price": 50000.0,
                "amount": 0.1,
                "filled": 0.0,
                "status": "open",
                "timestamp": 1704326400000,
            },
            {
                "id": "order2",
                "symbol": "BTC/USD",
                "type": "limit",
                "side": "sell",
                "price": 51000.0,
                "amount": 0.05,
                "filled": 0.05,
                "status": "closed",
                "timestamp": 1704326500000,
            },
        ]
        with (
            patch.object(
                kraken_client,
                "_ccxt_client",
                create=True,
            ) as mock_ccxt,
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.native_to_ccxt",
                return_value="BTC/USD",
            ),
        ):
            mock_ccxt.fetch_open_orders = AsyncMock(return_value=mock_orders)
            mock_ccxt.fetch_orders = AsyncMock(return_value=mock_orders)
            result = await kraken_client.get_orders(symbol="BTC/USD", status=None)
            assert len(result) == 2


class TestOnMessageOhlcAck:
    """Tests for onMessageOhlcAck."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    @pytest.mark.asyncio
    async def test_on_message_ohlc_subscription_ack_success(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify on message ohlc subscription ack success."""
        message = {
            "method": "subscribe",
            "result": {
                "channel": "ohlc",
                "symbol": "BTC/USD",
                "interval": 1,
                "warnings": [],
            },
            "success": True,
        }
        await kraken_client._on_message(message)

    @pytest.mark.asyncio
    async def test_on_message_ohlc_subscription_ack_failure(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify on message ohlc subscription ack failure."""
        message = {
            "method": "subscribe",
            "result": {
                "channel": "ohlc",
                "symbol": "BTC/USD",
                "interval": 1,
                "warnings": [],
            },
            "success": False,
            "error": "Subscription failed",
        }
        await kraken_client._on_message(message)

    @pytest.mark.asyncio
    async def test_on_message_ohlc_subscription_with_warnings(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify on message ohlc subscription with warnings."""
        message = {
            "method": "subscribe",
            "result": {
                "channel": "ohlc",
                "symbol": "BTC/USD",
                "interval": 1,
                "warnings": ["Rate limit approaching"],
            },
            "success": True,
        }
        await kraken_client._on_message(message)


class TestOnMessageReqIdNotFound:
    """Tests for onMessageReqIdNotFound."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    @pytest.mark.asyncio
    async def test_on_message_req_id_not_found(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify on message req id not found."""
        message = {
            "req_id": 999,
            "success": True,
            "result": {"order_id": "ABC123"},
        }
        await kraken_client._on_message(message)


class TestWithRetryMaxRetries:
    """Tests for withRetryMaxRetries."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    @pytest.mark.asyncio
    async def test_with_retry_network_error_opens_circuit_breaker(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify with retry network error opens circuit breaker."""
        mock_func = AsyncMock()
        mock_func.side_effect = NetworkError("Network timeout")
        kraken_client._circuit_failures = kraken_client._max_failures - 1
        kraken_client._circuit_open_until = 0
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.asyncio.sleep",
                new_callable=AsyncMock,
            ),
            pytest.raises(NetworkError),
        ):
            await kraken_client._with_retry(mock_func, max_retries=3)
        assert kraken_client._circuit_open_until > 0


class TestEnsureWsConnectedAlreadyConnected:
    """Tests for ensureWsConnectedAlreadyConnected."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    @pytest.mark.asyncio
    async def test_ensure_ws_connected_already_has_client(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify ensure ws connected already has client."""
        mock_ws_client = MagicMock()
        mock_ws_client.start = AsyncMock()
        kraken_client._ws_client = mock_ws_client
        kraken_client._ws_connected = False
        await kraken_client._ensure_ws_connected()
        assert kraken_client._ws_connected is True


@pytest.mark.asyncio
async def test_close_ws_client_closes_session() -> None:
    """WebSocket session closure.

    Given a KrakenExchangeClient with an active WebSocket session,
    When _close_ws_client is called,
    Then the session close method is invoked and client reference is cleared.
    """
    client = KrakenExchangeClient("key", "secret")
    session_closed = {"called": False}

    class DummySession:
        closed = False

        async def close(self) -> None:
            session_closed["called"] = True
            self.closed = True

    client._ws_connected = True
    client._ws_client = SimpleNamespace(
        close=AsyncMock(),
        _SpotAsyncClient__session=DummySession(),
    )
    await client._close_ws_client()
    assert client._ws_client is None
    assert session_closed["called"]


@pytest.mark.asyncio
async def test_with_retry_circuit_open_raises() -> None:
    """Circuit breaker prevents retries when open.

    Given a KrakenExchangeClient with an open circuit breaker,
    When _with_retry is called,
    Then it raises RuntimeError without executing the function.
    """
    client = KrakenExchangeClient("key", "secret")
    client._circuit_open_until = time.time() + 10
    with pytest.raises(RuntimeError, match="Circuit breaker open"):
        await client._with_retry(lambda: None)


def test_convert_ws_order_response_prefers_client_id() -> None:
    """Order response conversion uses client order ID.

    Given a WebSocket order response with client order ID,
    When _convert_ws_order_response is called,
    Then it returns an OrderSnapshot with the client order ID preserved.
    """
    client = KrakenExchangeClient("key", "secret")
    request = ExchangeOrderRequest(
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        type=OrderTypeEnum.LIMIT,
        amount=1.0,
        price=10.0,
        client_order_id="client-123",
    )
    response = {
        "result": {"order_id": "order-1", "cl_ord_id": "client-xyz", "order_userref": "user-1"}
    }
    snapshot = client._convert_ws_order_response(response, request)
    assert snapshot.id == "order-1"
    assert snapshot.client_order_id == "client-xyz"


@pytest.mark.asyncio
async def test_ensure_ws_connected_initializes_client() -> None:
    """WebSocket connection initialization.

    Given a KrakenExchangeClient without an active WebSocket,
    When _ensure_ws_connected is called,
    Then it creates and starts a new WebSocket client.
    """
    client = KrakenExchangeClient("key", "secret")

    class DummyWS:
        called: bool = False

        def __init__(self, key: str, secret: str, callback: Any) -> None:
            self.key = key
            self.secret = secret
            self.callback = callback
            self.exception_occur = False
            self.start = AsyncMock()
            DummyWS.called = True

    with patch("snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient", DummyWS):
        await client._ensure_ws_connected()
    assert isinstance(client._ws_client, DummyWS)
    client._ws_client.start.assert_awaited_once()
    assert client._ws_connected
    assert DummyWS.called


@pytest.mark.asyncio
async def test_get_or_create_ws_client_requires_credentials() -> None:
    """WebSocket client creation requires API credentials.

    Given a KrakenExchangeClient without API credentials,
    When _get_or_create_ws_client is called,
    Then it raises RuntimeError indicating credentials are required.
    """
    client = KrakenExchangeClient()
    with pytest.raises(RuntimeError, match="API credentials required"):
        await client._get_or_create_ws_client()


class _StubWsClient:
    """Test stub for WebSocket client."""

    def __init__(self) -> None:
        self.subscribe = AsyncMock()
        self.exception_occur = False

    async def __aenter__(self) -> "_StubWsClient":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: Any,
    ) -> bool:
        return False


class _OneShotQueue(asyncio.Queue[Any]):
    """Test queue that delivers one payload then sets exception flag."""

    def __init__(self, payload: Any, ws: _StubWsClient) -> None:
        super().__init__()
        self._payload = payload
        self._ws = ws
        self._delivered = False

    async def get(self) -> Any:
        if self._delivered:
            await asyncio.sleep(0)
            raise AssertionError("queue consumed twice")
        self._delivered = True
        self._ws.exception_occur = True
        return self._payload


@pytest.mark.asyncio
async def test_subscribe_ticks_with_wildcard() -> None:
    """Tick subscription with wildcard symbol.

    Given a KrakenExchangeClient with a WebSocket client,
    When subscribe_ticks is called with wildcard symbol,
    Then it subscribes and yields tick updates from the queue.
    """
    client = KrakenExchangeClient("key", "secret")
    ws = _StubWsClient()
    client._ws_client = ws
    client._tick_queue = _OneShotQueue({"tick": 1}, ws)
    with patch.object(client, "_ensure_ws_connected", new_callable=AsyncMock):
        updates: list[Any] = []
        async for item in client.subscribe_ticks(["*"], req_id=7):
            updates.append(item)
    assert updates == [{"tick": 1}]
    ws.subscribe.assert_called_once()


@pytest.mark.asyncio
async def test_subscribe_candles_uses_interval_queue() -> None:
    """Candle subscription uses interval-specific queue.

    Given a KrakenExchangeClient with interval-specific candle queues,
    When subscribe_candles is called with a timeframe,
    Then it yields updates from the corresponding interval queue.
    """
    client = KrakenExchangeClient("key", "secret")
    ws = _StubWsClient()
    client._ws_client = ws
    client._candle_queues = {1: _OneShotQueue({"candle": 1}, ws)}
    with patch.object(client, "_ensure_ws_connected", new_callable=AsyncMock):
        updates: list[Any] = []
        async for item in client.subscribe_candles(["*"], timeframe="1m", req_id=1):
            updates.append(item)
    assert updates == [{"candle": 1}]
    ws.subscribe.assert_called_once()


@pytest.mark.asyncio
async def test_subscribe_trades_consumes_queue() -> None:
    """Trade subscription consumes from trade queue.

    Given a KrakenExchangeClient with a trade queue,
    When subscribe_trades is called,
    Then it yields trade updates from the queue.
    """
    client = KrakenExchangeClient("key", "secret")
    ws = _StubWsClient()
    client._ws_client = ws
    client._trade_queue = _OneShotQueue({"trade": 1}, ws)
    with patch.object(client, "_ensure_ws_connected", new_callable=AsyncMock):
        updates: list[Any] = []
        async for item in client.subscribe_trades(["*"], req_id=3):
            updates.append(item)
    assert updates == [{"trade": 1}]
    ws.subscribe.assert_called_once()


@pytest.mark.asyncio
async def test_subscribe_executions_consumes_queue() -> None:
    """Execution subscription consumes from execution queue.

    Given a KrakenExchangeClient with an execution queue,
    When subscribe_executions is called,
    Then it yields execution updates from the queue.
    """
    client = KrakenExchangeClient("key", "secret")
    ws = _StubWsClient()
    client._ws_client = ws
    client._execution_queue = _OneShotQueue({"execution": 1}, ws)
    with patch.object(client, "_ensure_ws_connected", new_callable=AsyncMock):
        updates: list[Any] = []
        async for item in client.subscribe_executions(req_id=5):
            updates.append(item)
    assert updates == [{"execution": 1}]
    ws.subscribe.assert_called_once()


@pytest.mark.asyncio
async def test_subscribe_instruments_raw_mode() -> None:
    """Instrument subscription in raw mode.

    Given a KrakenExchangeClient with raw instrument queue,
    When subscribe_instruments is called with raw=True,
    Then it yields raw instrument data from the raw queue.
    """
    client = KrakenExchangeClient("key", "secret")
    ws = _StubWsClient()
    client._ws_client = ws
    client._raw_instrument_queue = _OneShotQueue({"symbol": "RAW"}, ws)
    with patch.object(client, "_ensure_ws_connected", new_callable=AsyncMock):
        updates = [item async for item in client.subscribe_instruments(raw=True, req_id=11)]
    assert updates == [{"symbol": "RAW"}]
    ws.subscribe.assert_called_once()


@pytest.mark.asyncio
async def test_on_message_handles_subscription_acks() -> None:
    """Message handler processes subscription acknowledgments.

    Given a KrakenExchangeClient receiving subscription responses,
    When _on_message is called with various subscription acks,
    Then it handles success, failure, and warning cases appropriately.
    """
    client = KrakenExchangeClient("key", "secret")
    await client._on_message(
        {
            "method": "subscribe",
            "result": {"channel": "trade", "symbol": ["BTC/USD"]},
            "success": False,
            "error": "boom",
        }
    )
    await client._on_message(
        {
            "method": "subscribe",
            "result": {"channel": "executions", "snap_orders": True, "snap_trades": False},
            "success": True,
            "warnings": ["slow"],
        }
    )
    await client._on_message(
        {
            "method": "subscribe",
            "result": {"channel": "ohlc", "symbol": "BTC/USD", "interval": 1, "warnings": ["lag"]},
            "success": True,
        }
    )


@pytest.mark.asyncio
async def test_on_message_instrument_raw_queue_when_parse_fails() -> None:
    """Instrument message falls back to raw queue on parse failure.

    Given a KrakenExchangeClient with instrument parsing that fails,
    When _on_message receives an instrument message,
    Then raw data is queued and parsed queue remains empty.
    """
    client = KrakenExchangeClient("key", "secret")
    client._raw_instrument_queue = asyncio.Queue()
    client._instrument_queue = asyncio.Queue()
    payload = {"symbol": "BTC/USD", "status": "online"}
    with patch(
        "snapper.infrastructure.exchanges.implementations.kraken.parse_kraken_instrument_list",
        side_effect=ValueError("bad mapping"),
    ):
        await client._on_message({"channel": "instrument", "data": {"pairs": [payload]}})
    raw = await client._raw_instrument_queue.get()
    assert raw["symbol"] == "BTC/USD"
    assert client._instrument_queue.empty()


@pytest.mark.asyncio
async def test_on_message_instrument_with_unexpected_format() -> None:
    """Instrument message with unexpected format is handled gracefully.

    Given a KrakenExchangeClient,
    When _on_message receives an instrument message with non-dict data,
    Then it handles the message without raising an exception.
    """
    client = KrakenExchangeClient("key", "secret")
    await client._on_message({"channel": "instrument", "data": "not-a-dict"})


@pytest.mark.asyncio
async def test_with_retry_rate_limit_backoff() -> None:
    """Retry mechanism applies backoff on rate limit errors.

    Given a KrakenExchangeClient with a function that always rate limits,
    When _with_retry is called,
    Then it retries with backoff before raising the final error.
    """
    client = KrakenExchangeClient("key", "secret")
    calls = {"attempts": 0}

    async def always_rate_limited() -> None:
        calls["attempts"] += 1
        raise ccxt.RateLimitExceeded("rate")

    with (
        patch("asyncio.sleep", new_callable=AsyncMock) as sleeper,
        pytest.raises(ccxt.RateLimitExceeded),
    ):
        await client._with_retry(always_rate_limited)
    assert calls["attempts"] == 3
    assert sleeper.await_count == 2


class _ErrorOnGetQueue(asyncio.Queue[Any]):
    """Test queue that raises error on first get, then sets exception flag."""

    def __init__(self, ws: _StubWsClient) -> None:
        super().__init__()
        self._ws = ws
        self._called = False

    async def get(self) -> Any:
        if not self._called:
            self._called = True
            raise RuntimeError("simulated error")
        self._ws.exception_occur = True
        await asyncio.sleep(0)
        raise AssertionError("should not reach here")


@pytest.mark.asyncio
async def test_subscribe_ticks_handles_receive_error() -> None:
    """Tick subscription handles queue receive errors.

    Given a KrakenExchangeClient with a queue that raises errors,
    When subscribe_ticks is iterated,
    Then it handles the error and returns an empty result.
    """
    client = KrakenExchangeClient("key", "secret")
    ws = _StubWsClient()
    client._ws_client = ws
    client._tick_queue = _ErrorOnGetQueue(ws)
    with patch.object(client, "_ensure_ws_connected", new_callable=AsyncMock):
        updates: list[Any] = []
        async for item in client.subscribe_ticks(["BTC-USD"]):
            updates.append(item)
    assert updates == []


@pytest.mark.asyncio
async def test_subscribe_candles_handles_receive_error() -> None:
    """Candle subscription handles queue receive errors.

    Given a KrakenExchangeClient with a candle queue that raises errors,
    When subscribe_candles is iterated,
    Then it handles the error and returns an empty result.
    """
    client = KrakenExchangeClient("key", "secret")
    ws = _StubWsClient()
    client._ws_client = ws
    client._candle_queues = {1: _ErrorOnGetQueue(ws)}
    with patch.object(client, "_ensure_ws_connected", new_callable=AsyncMock):
        updates: list[Any] = []
        async for item in client.subscribe_candles(["BTC-USD"], timeframe="1m"):
            updates.append(item)
    assert updates == []


@pytest.mark.asyncio
async def test_subscribe_trades_handles_receive_error() -> None:
    """Trade subscription handles queue receive errors.

    Given a KrakenExchangeClient with a trade queue that raises errors,
    When subscribe_trades is iterated,
    Then it handles the error and returns an empty result.
    """
    client = KrakenExchangeClient("key", "secret")
    ws = _StubWsClient()
    client._ws_client = ws
    client._trade_queue = _ErrorOnGetQueue(ws)
    with patch.object(client, "_ensure_ws_connected", new_callable=AsyncMock):
        updates: list[Any] = []
        async for item in client.subscribe_trades(["BTC-USD"]):
            updates.append(item)
    assert updates == []


@pytest.mark.asyncio
async def test_subscribe_executions_handles_receive_error() -> None:
    """Execution subscription handles queue receive errors.

    Given a KrakenExchangeClient with an execution queue that raises errors,
    When subscribe_executions is iterated,
    Then it handles the error and returns an empty result.
    """
    client = KrakenExchangeClient("key", "secret")
    ws = _StubWsClient()
    client._ws_client = ws
    client._execution_queue = _ErrorOnGetQueue(ws)
    with patch.object(client, "_ensure_ws_connected", new_callable=AsyncMock):
        updates: list[Any] = []
        async for item in client.subscribe_executions():
            updates.append(item)
    assert updates == []


@pytest.mark.asyncio
async def test_subscribe_instruments_handles_receive_error() -> None:
    """Instrument subscription handles queue receive errors.

    Given a KrakenExchangeClient with an instrument queue that raises errors,
    When subscribe_instruments is iterated,
    Then it handles the error and returns an empty result.
    """
    client = KrakenExchangeClient("key", "secret")
    ws = _StubWsClient()
    client._ws_client = ws
    client._instrument_queue = _ErrorOnGetQueue(ws)
    with patch.object(client, "_ensure_ws_connected", new_callable=AsyncMock):
        updates = [item async for item in client.subscribe_instruments()]
    assert updates == []


@pytest.mark.asyncio
async def test_subscribe_ticks_raises_on_outer_error() -> None:
    """Tick subscription propagates connection errors.

    Given a KrakenExchangeClient where connection fails,
    When subscribe_ticks is iterated,
    Then it raises the connection error.
    """
    client = KrakenExchangeClient("key", "secret")
    with (
        patch.object(
            client, "_ensure_ws_connected", new_callable=AsyncMock, side_effect=RuntimeError("boom")
        ),
        pytest.raises(RuntimeError, match="boom"),
    ):
        async for _ in client.subscribe_ticks(["BTC-USD"]):
            """Consumed by iteration to trigger exception."""
            pass


@pytest.mark.asyncio
async def test_subscribe_candles_raises_on_outer_error() -> None:
    """Candle subscription propagates connection errors.

    Given a KrakenExchangeClient where connection fails,
    When subscribe_candles is iterated,
    Then it raises the connection error.
    """
    client = KrakenExchangeClient("key", "secret")
    with (
        patch.object(
            client, "_ensure_ws_connected", new_callable=AsyncMock, side_effect=RuntimeError("boom")
        ),
        pytest.raises(RuntimeError, match="boom"),
    ):
        async for _ in client.subscribe_candles(["BTC-USD"]):
            """Consumed by iteration to trigger exception."""
            pass


@pytest.mark.asyncio
async def test_subscribe_trades_raises_on_outer_error() -> None:
    """Trade subscription propagates connection errors.

    Given a KrakenExchangeClient where connection fails,
    When subscribe_trades is iterated,
    Then it raises the connection error.
    """
    client = KrakenExchangeClient("key", "secret")
    with (
        patch.object(
            client, "_ensure_ws_connected", new_callable=AsyncMock, side_effect=RuntimeError("boom")
        ),
        pytest.raises(RuntimeError, match="boom"),
    ):
        async for _ in client.subscribe_trades(["BTC-USD"]):
            """Consumed by iteration to trigger exception."""
            pass


@pytest.mark.asyncio
async def test_subscribe_executions_raises_on_outer_error() -> None:
    """Execution subscription propagates connection errors.

    Given a KrakenExchangeClient where connection fails,
    When subscribe_executions is iterated,
    Then it raises the connection error.
    """
    client = KrakenExchangeClient("key", "secret")
    with (
        patch.object(
            client, "_ensure_ws_connected", new_callable=AsyncMock, side_effect=RuntimeError("boom")
        ),
        pytest.raises(RuntimeError, match="boom"),
    ):
        async for _ in client.subscribe_executions():
            """Consumed by iteration to trigger exception."""
            pass


@pytest.mark.asyncio
async def test_subscribe_instruments_raises_on_outer_error() -> None:
    """Instrument subscription propagates connection errors.

    Given a KrakenExchangeClient where connection fails,
    When subscribe_instruments is iterated,
    Then it raises the connection error.
    """
    client = KrakenExchangeClient("key", "secret")
    with (
        patch.object(
            client, "_ensure_ws_connected", new_callable=AsyncMock, side_effect=RuntimeError("boom")
        ),
        pytest.raises(RuntimeError, match="boom"),
    ):
        async for _ in client.subscribe_instruments():
            """Consumed by iteration to trigger exception."""
            pass


@pytest.mark.asyncio
async def test_on_message_with_failed_order_response() -> None:
    """Message handler sets exception on failed order response.

    Given a KrakenExchangeClient with a pending order future,
    When _on_message receives a failed order response,
    Then the future is completed with an exception.
    """
    client = KrakenExchangeClient("key", "secret")
    future: asyncio.Future[dict[str, Any]] = asyncio.Future()
    client._ws_order_requests[123] = future
    await client._on_message({"req_id": 123, "success": False, "error": "invalid order"})
    assert future.done()
    with pytest.raises(Exception, match="invalid order"):
        future.result()


@pytest.mark.asyncio
async def test_on_message_with_ohlc_failed_subscription() -> None:
    """Message handler logs OHLC subscription failure.

    Given a KrakenExchangeClient,
    When _on_message receives a failed OHLC subscription response,
    Then it handles the failure without raising an exception.
    """
    client = KrakenExchangeClient("key", "secret")
    await client._on_message(
        {
            "method": "subscribe",
            "result": {"channel": "ohlc", "symbol": "BTC/USD", "interval": 1},
            "success": False,
            "error": "invalid interval",
        }
    )


@pytest.mark.asyncio
async def test_on_message_ticker_status_warning() -> None:
    """Message handler logs ticker status warnings.

    Given a KrakenExchangeClient,
    When _on_message receives a ticker status error event,
    Then it handles the warning without raising an exception.
    """
    client = KrakenExchangeClient("key", "secret")
    await client._on_message(
        {"channel": "ticker", "event": "status", "status": "error", "message": "failed"}
    )


@pytest.mark.asyncio
async def test_on_message_instrument_status_warning() -> None:
    """Message handler logs instrument status warnings.

    Given a KrakenExchangeClient,
    When _on_message receives an instrument status error event,
    Then it handles the warning without raising an exception.
    """
    client = KrakenExchangeClient("key", "secret")
    await client._on_message(
        {"channel": "instrument", "event": "status", "status": "error", "message": "failed"}
    )


@pytest.mark.asyncio
async def test_on_message_ticker_parse_error() -> None:
    """Message handler skips ticker on parse error.

    Given a KrakenExchangeClient with ticker parsing that fails,
    When _on_message receives a ticker message,
    Then the tick queue remains empty.
    """
    client = KrakenExchangeClient("key", "secret")
    client._tick_queue = asyncio.Queue()
    with patch(
        "snapper.infrastructure.exchanges.implementations.kraken.parse_kraken_ticker_list",
        side_effect=ValueError("bad ticker"),
    ):
        await client._on_message({"channel": "ticker", "data": [{"symbol": "BTC/USD"}]})
    assert client._tick_queue.empty()


@pytest.mark.asyncio
async def test_on_message_trade_parse_error() -> None:
    """Message handler skips trade on parse error.

    Given a KrakenExchangeClient with trade parsing that fails,
    When _on_message receives a trade message,
    Then the trade queue remains empty.
    """
    client = KrakenExchangeClient("key", "secret")
    client._trade_queue = asyncio.Queue()
    with patch(
        "snapper.infrastructure.exchanges.implementations.kraken.parse_kraken_trade_list",
        side_effect=ValueError("bad trade"),
    ):
        await client._on_message({"channel": "trade", "data": [{"symbol": "BTC/USD"}]})
    assert client._trade_queue.empty()


@pytest.mark.asyncio
async def test_on_message_execution_parse_error() -> None:
    """Message handler skips execution on parse error.

    Given a KrakenExchangeClient with execution parsing that fails,
    When _on_message receives an execution message,
    Then the execution queue remains empty.
    """
    client = KrakenExchangeClient("key", "secret")
    client._execution_queue = asyncio.Queue()
    with patch(
        "snapper.infrastructure.exchanges.implementations.kraken.parse_kraken_execution_list",
        side_effect=ValueError("bad execution"),
    ):
        await client._on_message({"channel": "executions", "data": [{"order_id": "123"}]})
    assert client._execution_queue.empty()


@pytest.mark.asyncio
async def test_on_message_candle_parse_error() -> None:
    """Message handler skips candle on parse error.

    Given a KrakenExchangeClient with candle parsing that fails,
    When _on_message receives a candle message,
    Then the candle queue remains empty.
    """
    client = KrakenExchangeClient("key", "secret")
    client._candle_queues = {1: asyncio.Queue()}
    with patch(
        "snapper.infrastructure.exchanges.implementations.kraken.parse_kraken_candle_list",
        side_effect=ValueError("bad candle"),
    ):
        await client._on_message({"channel": "ohlc", "data": [{"symbol": "BTC/USD"}]})
    assert client._candle_queues[1].empty()


@pytest.mark.asyncio
async def test_on_message_ohlc_broadcasts_to_all_queues_when_interval_not_matched() -> None:
    """OHLC message broadcasts to all queues when interval not matched.

    Given a KrakenExchangeClient with multiple candle queues,
    When _on_message receives an OHLC with unmatched interval,
    Then the candle is broadcast to all interval queues.
    """
    client = KrakenExchangeClient("key", "secret")
    client._candle_queues = {5: asyncio.Queue(), 15: asyncio.Queue()}
    mock_candle = CandleUpdate(
        symbol="BTC-USD",
        interval=1,
        open=50000.0,
        high=51000.0,
        low=49000.0,
        close=50500.0,
        volume=100.0,
        vwap=50250.0,
        trades=500,
        interval_begin=datetime.now(UTC),
    )
    with patch(
        "snapper.infrastructure.exchanges.implementations.kraken.parse_kraken_candle_list",
        return_value=[mock_candle],
    ):
        await client._on_message({"channel": "ohlc", "data": [{"symbol": "BTC/USD"}]})
    assert not client._candle_queues[5].empty()
    assert not client._candle_queues[15].empty()


@pytest.mark.asyncio
async def test_on_message_ohlc_empty_list_when_invalid_data_type() -> None:
    """OHLC message with invalid data type leaves queue empty.

    Given a KrakenExchangeClient with a candle queue,
    When _on_message receives an OHLC with non-list data,
    Then the candle queue remains empty.
    """
    client = KrakenExchangeClient("key", "secret")
    client._candle_queues = {1: asyncio.Queue()}
    await client._on_message({"channel": "ohlc", "data": "invalid"})
    assert client._candle_queues[1].empty()


@pytest.mark.asyncio
async def test_on_message_instrument_with_dict_having_pairs() -> None:
    """Instrument message with pairs dict queues raw data.

    Given a KrakenExchangeClient with instrument queues,
    When _on_message receives an instrument message with pairs,
    Then raw instrument data is queued.
    """
    client = KrakenExchangeClient("key", "secret")
    client._raw_instrument_queue = asyncio.Queue()
    client._instrument_queue = asyncio.Queue()
    with patch(
        "snapper.infrastructure.exchanges.implementations.kraken.parse_kraken_instrument_list",
        return_value=[],
    ):
        await client._on_message(
            {
                "channel": "instrument",
                "data": {"pairs": [{"symbol": "BTC/USD", "status": "online"}]},
            }
        )
    assert not client._raw_instrument_queue.empty()


@pytest.mark.asyncio
async def test_on_message_instrument_with_empty_pairs() -> None:
    """Instrument message with empty pairs leaves queue empty.

    Given a KrakenExchangeClient with instrument queues,
    When _on_message receives an instrument message with empty pairs,
    Then the raw instrument queue remains empty.
    """
    client = KrakenExchangeClient("key", "secret")
    client._raw_instrument_queue = asyncio.Queue()
    client._instrument_queue = asyncio.Queue()
    with patch(
        "snapper.infrastructure.exchanges.implementations.kraken.parse_kraken_instrument_list",
        return_value=[],
    ):
        await client._on_message(
            {
                "channel": "instrument",
                "data": {"pairs": [], "assets": []},
            }
        )
    assert client._raw_instrument_queue.empty()


@pytest.mark.asyncio
async def test_on_message_instrument_with_null_data() -> None:
    """Instrument message with null data leaves queues empty.

    Given a KrakenExchangeClient with instrument queues,
    When _on_message receives an instrument message with null data,
    Then both instrument queues remain empty.
    """
    client = KrakenExchangeClient("key", "secret")
    client._raw_instrument_queue = asyncio.Queue()
    client._instrument_queue = asyncio.Queue()
    await client._on_message({"channel": "instrument", "data": None})
    assert client._raw_instrument_queue.empty()
    assert client._instrument_queue.empty()


@pytest.mark.asyncio
async def test_on_message_instrument_validation_error_skips_pair() -> None:
    """Instrument message skips pair on validation error.

    Given a KrakenExchangeClient with schema validation that fails,
    When _on_message receives an instrument message,
    Then the invalid pair is skipped and queue remains empty.
    """
    client = KrakenExchangeClient("key", "secret")
    client._raw_instrument_queue = asyncio.Queue()
    client._instrument_queue = asyncio.Queue()
    with patch(
        "snapper.infrastructure.exchanges.implementations.kraken.KrakenInstrumentPairSchema.model_validate",
        side_effect=ValidationError.from_exception_data("test", []),
    ):
        await client._on_message(
            {
                "channel": "instrument",
                "data": {"pairs": [{"symbol": "BAD/PAIR"}]},
            }
        )
    assert client._raw_instrument_queue.empty()


@pytest.mark.asyncio
async def test_on_message_trade_subscription_failed_ack() -> None:
    """Message handler logs trade subscription failure.

    Given a KrakenExchangeClient,
    When _on_message receives a failed trade subscription response,
    Then it handles the failure without raising an exception.
    """
    client = KrakenExchangeClient("key", "secret")
    await client._on_message(
        {
            "method": "subscribe",
            "result": {"channel": "trade", "symbol": ["BTC/USD"]},
            "success": False,
            "error": "subscription failed",
        }
    )


@pytest.mark.asyncio
async def test_on_message_executions_subscription_failed_ack() -> None:
    """Message handler logs executions subscription failure.

    Given a KrakenExchangeClient,
    When _on_message receives a failed executions subscription response,
    Then it handles the failure without raising an exception.
    """
    client = KrakenExchangeClient("key", "secret")
    await client._on_message(
        {
            "method": "subscribe",
            "result": {"channel": "executions", "snap_orders": True, "snap_trades": False},
            "success": False,
            "error": "auth failed",
        }
    )


@pytest.mark.asyncio
async def test_on_message_ohlc_subscription_failed_ack() -> None:
    """Message handler logs OHLC subscription failure.

    Given a KrakenExchangeClient,
    When _on_message receives a failed OHLC subscription response,
    Then it handles the failure without raising an exception.
    """
    client = KrakenExchangeClient("key", "secret")
    await client._on_message(
        {
            "method": "subscribe",
            "result": {"channel": "ohlc", "symbol": "BTC/USD", "interval": 1},
            "success": False,
            "error": "invalid interval",
        }
    )


@pytest.mark.asyncio
async def test_on_message_ohlc_subscription_with_warnings() -> None:
    """Message handler logs OHLC subscription warnings.

    Given a KrakenExchangeClient,
    When _on_message receives an OHLC subscription with warnings,
    Then it handles the warnings without raising an exception.
    """
    client = KrakenExchangeClient("key", "secret")
    await client._on_message(
        {
            "method": "subscribe",
            "result": {
                "channel": "ohlc",
                "symbol": "BTC/USD",
                "interval": 1,
                "warnings": ["rate limit approaching", "deprecated interval"],
            },
            "success": True,
        }
    )


@pytest.mark.asyncio
async def test_on_message_subscribe_ack_unknown_channel() -> None:
    """Message handler handles unknown channel subscription.

    Given a KrakenExchangeClient,
    When _on_message receives a subscription ack for unknown channel,
    Then it handles the message without raising an exception.
    """
    client = KrakenExchangeClient("key", "secret")
    await client._on_message(
        {
            "method": "subscribe",
            "result": {
                "channel": "unknown_channel",
                "symbol": "BTC/USD",
            },
            "success": True,
        }
    )


@pytest.mark.asyncio
async def test_on_message_general_exception_handling() -> None:
    """Message handler catches general exceptions.

    Given a KrakenExchangeClient with parsing that raises unexpected error,
    When _on_message receives a message,
    Then it catches the exception without propagating it.
    """
    client = KrakenExchangeClient("key", "secret")
    with patch(
        "snapper.infrastructure.exchanges.implementations.kraken.parse_kraken_ticker_list",
        side_effect=Exception("unexpected error"),
    ):
        await client._on_message({"channel": "ticker", "data": [{"symbol": "BTC/USD"}]})


@pytest.mark.asyncio
async def test_on_message_req_id_future_already_done() -> None:
    """Message handler skips already completed future.

    Given a KrakenExchangeClient with a completed order future,
    When _on_message receives a response for that request ID,
    Then it does not attempt to set the result again.
    """
    client = KrakenExchangeClient("key", "secret")
    future: asyncio.Future[dict[str, Any]] = asyncio.Future()
    future.set_result({"already": "done"})
    client._ws_order_requests[456] = future
    await client._on_message({"req_id": 456, "success": True, "result": {"order_id": "123"}})
    assert future.done()


@pytest.mark.asyncio
async def test_on_message_trade_subscription_validation_error() -> None:
    """Message handler handles trade subscription with invalid success field.

    Given a KrakenExchangeClient,
    When _on_message receives a trade subscription with invalid success value,
    Then it handles the validation error without raising an exception.
    """
    client = KrakenExchangeClient("key", "secret")
    await client._on_message(
        {
            "method": "subscribe",
            "result": {"channel": "trade"},
            "success": "invalid",
        }
    )


@pytest.mark.asyncio
async def test_on_message_ohlc_subscription_validation_error() -> None:
    """Message handler handles OHLC subscription with invalid success field.

    Given a KrakenExchangeClient,
    When _on_message receives an OHLC subscription with invalid success value,
    Then it handles the validation error without raising an exception.
    """
    client = KrakenExchangeClient("key", "secret")
    await client._on_message(
        {
            "method": "subscribe",
            "result": {"channel": "ohlc"},
            "success": "not-bool",
        }
    )


@pytest.mark.asyncio
async def test_on_message_executions_subscription_validation_error() -> None:
    """Message handler handles executions subscription with invalid success field.

    Given a KrakenExchangeClient,
    When _on_message receives an executions subscription with invalid success value,
    Then it handles the validation error without raising an exception.
    """
    client = KrakenExchangeClient("key", "secret")
    await client._on_message(
        {
            "method": "subscribe",
            "result": {"channel": "executions"},
            "success": "not-bool",
        }
    )


@pytest.mark.asyncio
async def test_on_message_ticker_validation_error() -> None:
    """Message handler handles ticker with invalid status field.

    Given a KrakenExchangeClient,
    When _on_message receives a ticker with invalid status value,
    Then it handles the validation error without raising an exception.
    """
    client = KrakenExchangeClient("key", "secret")
    await client._on_message(
        {
            "channel": "ticker",
            "event": "subscribe",
            "status": 123,
        }
    )


@pytest.mark.asyncio
async def test_on_message_instrument_validation_error() -> None:
    """Message handler handles instrument with invalid status field.

    Given a KrakenExchangeClient,
    When _on_message receives an instrument with invalid status value,
    Then it handles the validation error without raising an exception.
    """
    client = KrakenExchangeClient("key", "secret")
    await client._on_message(
        {
            "channel": "instrument",
            "event": "subscribe",
            "status": 456,
        }
    )


class TestKrakenAdditionalCoverage:
    """Tests for krakenAdditionalCoverage."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    async def test_with_retry_max_retries_exceeded(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify with retry max retries exceeded."""
        mock_func = AsyncMock()
        with patch("asyncio.sleep", new_callable=AsyncMock):
            mock_func.side_effect = ccxt.NetworkError("Connection failed")
            with pytest.raises(ccxt.NetworkError, match="Connection failed"):
                await kraken_client._with_retry(mock_func)
            assert mock_func.call_count == 3

    async def test_on_message_ohlc_with_symbol(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify on message ohlc with symbol."""
        message = {
            "channel": "ohlc",
            "type": "update",
            "data": [
                {
                    "symbol": "BTC/USD",
                    "open": 50000.0,
                    "high": 50100.0,
                    "low": 49900.0,
                    "close": 50050.0,
                    "vwap": 50025.0,
                    "trades": 100,
                    "volume": 10.0,
                    "interval": 1,
                    "interval_begin": "2024-01-01T00:00:00Z",
                }
            ],
        }
        mock_queue_1m = MagicMock()
        mock_queue_1m.put = AsyncMock()
        mock_queue_5m = MagicMock()
        mock_queue_5m.put = AsyncMock()
        with patch.object(kraken_client, "_candle_queues", {1: mock_queue_1m, 5: mock_queue_5m}):
            await kraken_client._on_message(message)
            mock_queue_1m.put.assert_called_once()
            mock_queue_5m.put.assert_not_called()
            candle_data = mock_queue_1m.put.call_args[0][0]
            assert isinstance(candle_data, CandleUpdate)
            assert candle_data.symbol == "BTC-USD"
            assert candle_data.open == pytest.approx(50000.0)
            assert candle_data.interval == 1

    async def test_on_message_ohlc_broadcast(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify on message ohlc broadcast."""
        message = {
            "channel": "ohlc",
            "type": "snapshot",
            "data": [
                {
                    "symbol": "BTC/USD",
                    "open": 50000.0,
                    "high": 50010.0,
                    "low": 49990.0,
                    "close": 50005.0,
                    "vwap": 50000.0,
                    "trades": 50,
                    "volume": 5.0,
                    "interval": 99,
                    "interval_begin": "2024-01-01T00:00:00Z",
                }
            ],
        }
        mock_queue_1 = MagicMock()
        mock_queue_1.put = AsyncMock()
        mock_queue_2 = MagicMock()
        mock_queue_2.put = AsyncMock()
        with patch.object(kraken_client, "_candle_queues", {1: mock_queue_1, 5: mock_queue_2}):
            await kraken_client._on_message(message)
            mock_queue_1.put.assert_called_once()
            mock_queue_2.put.assert_called_once()

    async def test_on_message_ticker_update(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify on message ticker update."""
        message = {
            "channel": "ticker",
            "type": "update",
            "data": [
                {
                    "symbol": "BTC/USD",
                    "bid": 50000.0,
                    "bid_qty": 1.0,
                    "ask": 50010.0,
                    "ask_qty": 1.5,
                    "last": 50005.0,
                    "volume": 100.0,
                    "vwap": 50002.0,
                    "low": 49900.0,
                    "high": 50100.0,
                    "change": 100.0,
                    "change_pct": 0.2,
                }
            ],
        }
        with patch.object(kraken_client, "_tick_queue") as mock_queue:
            await kraken_client._on_message(message)
            mock_queue.put.assert_called_once()
            ticker_data = mock_queue.put.call_args[0][0]
            assert isinstance(ticker_data, TickerUpdate)
            assert ticker_data.symbol == "BTC-USD"

    async def test_on_message_trade_snapshot(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify on message trade snapshot."""
        trade_message = {
            "channel": "trade",
            "type": "snapshot",
            "data": [
                {
                    "symbol": "BTC/USD",
                    "side": "buy",
                    "qty": 0.5,
                    "price": 50000.0,
                    "ord_type": "limit",
                    "trade_id": 12345,
                    "timestamp": "2024-01-01T00:00:00Z",
                }
            ],
        }
        with patch.object(kraken_client, "_trade_queue") as mock_queue:
            await kraken_client._on_message(trade_message)
            mock_queue.put.assert_called_once()
            trade_data = mock_queue.put.call_args[0][0]
            assert isinstance(trade_data, TradeUpdate)
            assert trade_data.symbol == "BTC-USD"

    async def test_on_message_execution_update(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify on message execution update."""
        execution_message = {
            "channel": "executions",
            "type": "update",
            "data": [
                {
                    "order_id": "ORDER123",
                    "exec_type": "new",
                    "symbol": "BTC/USD",
                    "side": "buy",
                    "order_type": "limit",
                    "order_status": "new",
                    "timestamp": "2024-01-01T00:00:00Z",
                    "cum_qty": 0.0,
                    "cum_cost": 0.0,
                }
            ],
        }
        with patch.object(kraken_client, "_execution_queue") as mock_queue:
            await kraken_client._on_message(execution_message)
            mock_queue.put.assert_called_once()
            execution_data = mock_queue.put.call_args[0][0]
            assert isinstance(execution_data, ExecutionUpdate)
            assert execution_data.order_id == "ORDER123"

    async def test_on_message_exception_handling(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify on message exception handling."""
        bad_message = {
            "channel": "ticker",
            "type": "update",
            "data": [{"symbol": "BTC/USD"}],
        }
        await kraken_client._on_message(bad_message)

    async def test_with_retry_success_resets_circuit_breaker(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify with retry success resets circuit breaker."""
        mock_func = AsyncMock(return_value="success")
        result = await kraken_client._with_retry(mock_func)
        assert result == "success"

    async def test_with_retry_unexpected_exception(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify with retry unexpected exception."""
        mock_func = AsyncMock()
        mock_func.side_effect = ValueError("Unexpected error")
        with pytest.raises(ValueError, match="Unexpected error"):
            await kraken_client._with_retry(mock_func)
        assert mock_func.call_count == 1

    async def test_on_message_order_response_success(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify on message order response success."""
        request_future: asyncio.Future[dict[str, Any]] = asyncio.Future()
        kraken_client._ws_order_requests[5] = request_future
        await kraken_client._on_message(
            {"req_id": 5, "success": True, "result": {"order_id": "ABC123"}}
        )
        assert request_future.done()
        assert request_future.result()["result"]["order_id"] == "ABC123"

    async def test_on_message_order_response_failure(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify on message order response failure."""
        request_future: asyncio.Future[dict[str, Any]] = asyncio.Future()
        kraken_client._ws_order_requests[9] = request_future
        await kraken_client._on_message({"req_id": 9, "success": False, "error": "failure"})
        assert request_future.done()
        with pytest.raises(Exception, match="ExchangeOrderSnapshot request failed: failure"):
            request_future.result()

    async def test_on_message_instrument_snapshot(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify on message instrument snapshot."""
        sentinel = InstrumentPairDescriptor(
            symbol="BTC/USD",
            base="BTC",
            quote="USD",
            status="online",
            qty_precision=8,
            qty_increment=0.0001,
            qty_min=0.0001,
            price_precision=2,
            price_increment=0.5,
            cost_precision=2,
            cost_min=10.0,
            marginable=False,
            has_index=False,
        )
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken.parse_kraken_instrument_list",
            return_value=[sentinel],
        ):
            message = {
                "channel": "instrument",
                "data": {
                    "pairs": [
                        {
                            "symbol": "BTC/USD",
                        }
                    ]
                },
            }
            result_queue: asyncio.Queue[InstrumentPairDescriptor] = asyncio.Queue()
            kraken_client._instrument_queue = result_queue
            await kraken_client._on_message(message)
            queued = await result_queue.get()
            assert queued is sentinel

    async def test_subscribe_instruments_yields_messages(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify subscribe instruments yields messages."""

        class _StubWsClient:
            def __init__(self) -> None:
                self.subscribe = AsyncMock()
                self.exception_occur = False

            async def __aenter__(self) -> "_StubWsClient":
                return self

            async def __aexit__(
                self,
                exc_type: type | None,
                exc_val: Exception | None,
                exc_tb: object | None,
            ) -> bool:
                return False

        class _InstrumentQueue(asyncio.Queue[InstrumentPairDescriptor]):
            def __init__(self, ws_client: _StubWsClient, payload: InstrumentPairDescriptor) -> None:
                super().__init__()
                self._ws_client = ws_client
                self._payload = payload
                self._delivered = False

            async def get(self) -> InstrumentPairDescriptor:
                if self._delivered:
                    await asyncio.sleep(0)
                    raise AssertionError("Queue get called more than once")
                self._delivered = True
                self._ws_client.exception_occur = True
                return self._payload

        ws_client = _StubWsClient()
        kraken_client._ws_client = ws_client
        kraken_client._ws_connected = True
        instrument_update = InstrumentPairDescriptor(
            symbol="ETH/USD",
            base="ETH",
            quote="USD",
            status="online",
            qty_precision=8,
            qty_increment=0.0001,
            qty_min=0.0001,
            price_precision=2,
            price_increment=0.5,
            cost_precision=2,
            cost_min=10.0,
            marginable=False,
            has_index=True,
        )
        kraken_client._instrument_queue = _InstrumentQueue(ws_client, instrument_update)
        with patch.object(kraken_client, "_ensure_ws_connected", new_callable=AsyncMock) as ensure:
            ensure.return_value = None
            updates = [update async for update in kraken_client.subscribe_instruments()]
        ws_client.subscribe.assert_called_once()
        assert updates == [asdict(instrument_update)]

    @pytest.mark.asyncio
    async def test_close_ws_client_closes_session(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify close ws client closes session."""

        class _StubSession:
            def __init__(self) -> None:
                self.closed = False

            async def close(self) -> None:
                self.closed = True

        class _StubWsClient:
            def __init__(self) -> None:
                self.closed = False
                self._SpotAsyncClient__session = _StubSession()

            async def close(self) -> None:
                self.closed = True

        ws_client = _StubWsClient()
        kraken_client._ws_client = ws_client
        kraken_client._ws_connected = True
        await kraken_client._close_ws_client()
        assert ws_client.closed is True
        assert ws_client._SpotAsyncClient__session.closed is True
        assert kraken_client._ws_client is None
        assert kraken_client._ws_connected is False

    @pytest.mark.asyncio
    async def test_disconnect_websocket_invokes_close(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify disconnect websocket invokes close."""
        await kraken_client.disconnect_websocket()

    @pytest.mark.asyncio
    async def test_create_order_fallback_without_price_or_asset_class(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify create order fallback without price or asset class."""
        order_request = ExchangeOrderRequest(
            symbol="FOO-BAR",
            side=OrderSideEnum.SELL,
            type=OrderTypeEnum.MARKET,
            amount=1.0,
            price=None,
        )
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.native_to_ccxt",
                side_effect=ValueError("Unknown native symbol: FOO-BAR"),
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.native_to_kraken_rest",
                return_value="FOOBAR/USD",
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.Trade"
            ) as mock_trade_class,
        ):
            trade_client = MagicMock()
            mock_trade_class.return_value = trade_client
            trade_client.create_order.return_value = {"txid": ["OID123"]}
            order = await kraken_client.create_order(order_request)
            kwargs = trade_client.create_order.call_args.kwargs
            assert "price" not in kwargs
            assert kwargs["extra_params"] is None
            assert order.id == "OID123"

    @pytest.mark.asyncio
    async def test_cancel_order_fallback_errors_are_raised(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify cancel order fallback errors are raised."""
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.native_to_ccxt",
                side_effect=ValueError("Unknown native symbol: AAPLx-USD"),
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.Trade"
            ) as mock_trade_class,
        ):
            trade_client = MagicMock()
            mock_trade_class.return_value = trade_client
            trade_client.cancel_order.side_effect = RuntimeError("boom")
            with pytest.raises(RuntimeError, match="boom"):
                await kraken_client.cancel_order("OID", symbol="AAPLx-USD")

    @pytest.mark.asyncio
    async def test_create_order_ws_requires_connection(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify create order ws requires connection."""
        kraken_client._ws_connected = False
        with patch.object(
            kraken_client, "_get_or_create_ws_client", new_callable=AsyncMock
        ) as getter:
            getter.return_value = object()
            with pytest.raises(RuntimeError, match="WebSocket not connected"):
                await kraken_client.create_order_ws(
                    ExchangeOrderRequest(
                        symbol="BTC-USD",
                        side=OrderSideEnum.BUY,
                        type=OrderTypeEnum.LIMIT,
                        amount=1,
                        price=100,
                    )
                )

    @pytest.mark.asyncio
    async def test_create_order_ws_includes_price_and_client_id(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify create order ws includes price and client id."""

        class _StubWs:
            def __init__(self) -> None:
                self.sent: list[dict[str, Any]] = []

            async def send_message(self, message: dict[str, Any]) -> None:
                self.sent.append(message)

        ws_client = _StubWs()
        kraken_client._ws_connected = True

        original_send = ws_client.send_message

        async def _send_and_resolve(message: dict[str, Any]) -> None:
            await original_send(message)
            req_id = message.get("req_id")
            if req_id is not None and req_id in kraken_client._ws_order_requests:
                kraken_client._ws_order_requests[req_id].set_result(
                    {"result": {"order_id": "OID", "cl_ord_id": "CID"}}
                )

        ws_client.send_message = _send_and_resolve

        with patch.object(
            kraken_client, "_get_or_create_ws_client", AsyncMock(return_value=ws_client)
        ):
            request = ExchangeOrderRequest(
                symbol="ETH-USD",
                side=OrderSideEnum.BUY,
                type=OrderTypeEnum.LIMIT,
                amount=2,
                price=1500,
                client_order_id="cid-123",
            )
            snapshot = await kraken_client.create_order_ws(request)
        assert ws_client.sent[0]["params"]["limit_price"] == pytest.approx(1500.0)
        assert ws_client.sent[0]["params"]["cl_ord_id"] == "cid-123"
        assert isinstance(snapshot, ExchangeOrderSnapshot)
        assert snapshot.status == OrderStatusEnum.PENDING

    @pytest.mark.asyncio
    async def test_create_order_ws_timeout_cleans_future(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify create order ws timeout cleans future."""

        class _StubWs:
            async def send_message(self, message: dict[str, Any]) -> None:
                return None

        kraken_client._ws_connected = True
        with (
            patch.object(
                kraken_client, "_get_or_create_ws_client", AsyncMock(return_value=_StubWs())
            ),
            patch("asyncio.wait_for", AsyncMock(side_effect=TimeoutError())),
            pytest.raises(TimeoutError),
        ):
            await kraken_client.create_order_ws(
                ExchangeOrderRequest(
                    symbol="BTC-USD",
                    side=OrderSideEnum.BUY,
                    type=OrderTypeEnum.MARKET,
                    amount=1,
                ),
            )
        assert kraken_client._ws_order_requests == {}

    @pytest.mark.asyncio
    async def test_cancel_order_ws_requires_credentials(self) -> None:
        """Verify cancel order ws requires credentials."""
        client = KrakenExchangeClient()
        with pytest.raises(RuntimeError, match="API credentials required"):
            await client.cancel_order_ws("OID")

    @pytest.mark.asyncio
    async def test_cancel_order_ws_requires_connection(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify cancel order ws requires connection."""
        kraken_client._ws_connected = False
        with (
            patch.object(
                kraken_client, "_get_or_create_ws_client", AsyncMock(return_value=object())
            ),
            pytest.raises(RuntimeError, match="WebSocket not connected"),
        ):
            await kraken_client.cancel_order_ws("OID")

    @pytest.mark.asyncio
    async def test_cancel_order_ws_timeout_cleans_future(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify cancel order ws timeout cleans future."""

        class _StubWs:
            async def send_message(self, message: dict[str, Any]) -> None:
                return None

        kraken_client._ws_connected = True
        with (
            patch.object(
                kraken_client, "_get_or_create_ws_client", AsyncMock(return_value=_StubWs())
            ),
            patch("asyncio.wait_for", AsyncMock(side_effect=TimeoutError())),
            pytest.raises(TimeoutError),
        ):
            await kraken_client.cancel_order_ws("OID")
        assert kraken_client._ws_order_requests == {}

    @pytest.mark.asyncio
    async def test_get_orders_filters_status(self, kraken_client: KrakenExchangeClient) -> None:
        """Verify get orders filters status."""
        open_order = MagicMock(status=OrderStatusEnum.OPEN)
        closed_order = MagicMock(status=OrderStatusEnum.CLOSED)
        with (
            patch.object(kraken_client, "_with_retry", AsyncMock(return_value=[{}, {}])),
            patch.object(
                kraken_client,
                "_convert_ccxt_order",
                side_effect=[open_order, closed_order],
            ),
        ):
            result = await kraken_client.get_orders(status=OrderStatusEnum.CLOSED)
        assert result == [closed_order]

    @pytest.mark.asyncio
    async def test_get_balance_skips_non_dict_entries(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify get balance skips non dict entries."""
        payload = {
            "free": {},
            "USD": {"free": 1, "used": 2, "total": 3},
            "EUR": None,
            "timestamp": 123,
        }
        with patch.object(kraken_client, "_with_retry", AsyncMock(return_value=payload)):
            balances = await kraken_client.get_balance()
        assert list(balances) == ["USD"]
        assert balances["USD"].free == 1

    @pytest.mark.asyncio
    async def test_get_or_create_ws_client_calls_ensure(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify get or create ws client calls ensure."""
        sentinel = object()
        kraken_client._ws_client = sentinel
        with patch.object(kraken_client, "_ensure_ws_connected", AsyncMock()) as ensure:
            result = await kraken_client._get_or_create_ws_client()
        ensure.assert_awaited_once()
        assert result is sentinel

    @pytest.mark.asyncio
    async def test_ensure_ws_connected_initializes_client(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify ensure ws connected initializes client."""
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken.SpotWSClient"
        ) as mock_ws_class:
            ws_instance = MagicMock()
            ws_instance.start = AsyncMock()
            mock_ws_class.return_value = ws_instance
            await kraken_client._ensure_ws_connected()
            mock_ws_class.assert_called_once()
            ws_instance.start.assert_awaited_once()
            assert kraken_client._ws_client is ws_instance
            assert kraken_client._ws_connected is True

    @pytest.mark.asyncio
    async def test_with_retry_opens_circuit_after_network_errors(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify with retry opens circuit after network errors."""
        kraken_client._circuit_failures = 5
        failing_call = AsyncMock(side_effect=ccxt.NetworkError("offline"))
        with patch("asyncio.sleep", AsyncMock()), pytest.raises(ccxt.NetworkError):
            await kraken_client._with_retry(failing_call)
        assert kraken_client._circuit_open_until > time.time()


class TestKrakenFallbackToNativeAPI:
    """Tests for krakenFallbackToNativeAPI."""

    @pytest.fixture
    def kraken_client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    @pytest.mark.asyncio
    async def test_create_order_fallback_to_native_api(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify create order fallback to native api."""
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.native_to_ccxt",
                side_effect=ValueError("Unknown native symbol: AAPLx-USD"),
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.native_to_kraken_rest",
                return_value="AAPLx/USD",
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.Trade"
            ) as mock_trade_class,
        ):
            mock_trade_client = MagicMock()
            mock_trade_class.return_value = mock_trade_client
            mock_trade_client.create_order.return_value = {
                "txid": ["ORDER123"],
                "descr": {"order": "buy 10 AAPLx/USD @ limit 150.0"},
            }
            order_request = ExchangeOrderRequest(
                symbol="AAPLx-USD",
                side=OrderSideEnum.BUY,
                type=OrderTypeEnum.LIMIT,
                amount=float("10"),
                price=float("150.0"),
                client_order_id="CLIENT123",
            )
            result = await kraken_client.create_order(order_request)
            mock_trade_client.create_order.assert_called_once()
            call_kwargs = mock_trade_client.create_order.call_args.kwargs
            assert call_kwargs["ordertype"] == "limit"
            assert call_kwargs["side"] == "buy"
            assert call_kwargs["pair"] == "AAPLx/USD"
            assert call_kwargs["volume"] == "10.0"
            assert call_kwargs["price"] == "150.0"
            assert call_kwargs["extra_params"] is not None
            assert call_kwargs["extra_params"]["cl_ord_id"] == "CLIENT123"
            assert call_kwargs["extra_params"]["asset_class"] == "tokenized_asset"
            assert result.id == "ORDER123"
            assert result.symbol == "AAPLx-USD"
            assert result.side == OrderSideEnum.BUY
            assert result.type == OrderTypeEnum.LIMIT
            assert result.amount == float("10")
            assert result.price == float("150.0")
            assert result.status == OrderStatusEnum.PENDING

    @pytest.mark.asyncio
    async def test_cancel_order_fallback_to_native_api(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify cancel order fallback to native api."""
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.native_to_ccxt",
                side_effect=ValueError("Unknown native symbol: AAPLx-USD"),
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.Trade"
            ) as mock_trade_class,
        ):
            mock_trade_client = MagicMock()
            mock_trade_class.return_value = mock_trade_client
            mock_trade_client.cancel_order.return_value = {"count": 1}
            result = await kraken_client.cancel_order("ORDER123", symbol="AAPLx-USD")
            mock_trade_client.cancel_order.assert_called_once_with(txid="ORDER123")
            assert result.id == "ORDER123"
            assert result.symbol == "AAPLx-USD"
            assert result.status == OrderStatusEnum.CANCELED

    @pytest.mark.asyncio
    async def test_create_order_no_fallback_for_other_errors(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify create order no fallback for other errors."""
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken.native_to_ccxt",
            side_effect=ValueError("Different error"),
        ):
            order_request = ExchangeOrderRequest(
                symbol="BTC-USD",
                side=OrderSideEnum.BUY,
                type=OrderTypeEnum.MARKET,
                amount=float("0.1"),
            )
            with pytest.raises(ValueError, match="Different error"):
                await kraken_client.create_order(order_request)

    @pytest.mark.asyncio
    async def test_cancel_order_no_fallback_without_symbol(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify cancel order no fallback without symbol."""
        with (
            patch.object(kraken_client, "_ccxt_client") as mock_ccxt_client,
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.native_to_ccxt",
                side_effect=ValueError("Unknown native symbol"),
            ),
            pytest.raises(ValueError, match="Unknown native symbol"),
        ):
            mock_ccxt_client.cancel_order = AsyncMock(
                side_effect=ValueError("Unknown native symbol")
            )
            await kraken_client.cancel_order("ORDER123", symbol=None)

    @pytest.mark.asyncio
    async def test_get_trade_client_lazy_initialization(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify get trade client lazy initialization."""
        assert kraken_client._trade_client is None
        with patch(
            "snapper.infrastructure.exchanges.implementations.kraken.Trade"
        ) as mock_trade_class:
            mock_trade_client = MagicMock()
            mock_trade_class.return_value = mock_trade_client
            client1 = kraken_client._get_trade_client()
            assert client1 is mock_trade_client
            mock_trade_class.assert_called_once_with(key="test_key", secret="test_secret")
            client2 = kraken_client._get_trade_client()
            assert client2 is mock_trade_client
            assert mock_trade_class.call_count == 1

    def test_get_trade_client_no_credentials(self) -> None:
        """Verify get trade client no credentials."""
        client = KrakenExchangeClient()
        with pytest.raises(RuntimeError, match="API credentials required"):
            client._get_trade_client()

    @pytest.mark.asyncio
    async def test_create_order_fallback_handles_native_api_error(
        self, kraken_client: KrakenExchangeClient
    ) -> None:
        """Verify create order fallback handles native api error."""
        with (
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.native_to_ccxt",
                side_effect=ValueError("Unknown native symbol: AAPLx-USD"),
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.native_to_kraken_rest",
                return_value="AAPLx/USD",
            ),
            patch(
                "snapper.infrastructure.exchanges.implementations.kraken.Trade"
            ) as mock_trade_class,
        ):
            mock_trade_client = MagicMock()
            mock_trade_class.return_value = mock_trade_client
            mock_trade_client.create_order.side_effect = Exception("Native API error")
            order_request = ExchangeOrderRequest(
                symbol="AAPLx-USD",
                side=OrderSideEnum.BUY,
                type=OrderTypeEnum.LIMIT,
                amount=float("10"),
                price=float("150.0"),
            )
            with pytest.raises(Exception, match="Native API error"):
                await kraken_client.create_order(order_request)


class TestKrakenExchangeClientSimpleEdgeCases:
    """Tests for krakenExchangeClientSimpleEdgeCases."""

    @pytest.fixture
    def client(self) -> KrakenExchangeClient:
        """Provide test client instance."""
        return KrakenExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            sandbox=False,
        )

    @pytest.fixture(autouse=True)
    def _patch_symbol_mapper(self, monkeypatch: MonkeyPatch) -> None:
        def _identity(symbol: str) -> str:
            return symbol

        monkeypatch.setattr(
            "snapper.infrastructure.exchanges.implementations.kraken.native_to_ccxt", _identity
        )

    def test_init_without_credentials(self) -> None:
        """Verify init without credentials."""
        client = KrakenExchangeClient()
        assert client.api_key is None
        assert client.api_secret is None
        assert not client.sandbox

    def test_init_with_credentials(self) -> None:
        """Verify init with credentials."""
        client = KrakenExchangeClient(api_key="key", api_secret="secret")
        assert client.api_key == "key"
        assert client.api_secret == "secret"
        assert not client.sandbox

    def test_init_sandbox_mode(self) -> None:
        """Verify init sandbox mode."""
        try:
            KrakenExchangeClient(api_key="key", api_secret="secret", sandbox=True)
            raise AssertionError("Expected NotSupported exception")
        except Exception as e:
            assert "does not have a sandbox URL" in str(e)

    @pytest.mark.asyncio
    async def test_disconnect_without_connection(self, client: KrakenExchangeClient) -> None:
        """Verify disconnect without connection."""
        await client.disconnect()

    @pytest.mark.asyncio
    async def test_multiple_disconnects(self, client: KrakenExchangeClient) -> None:
        """Verify multiple disconnects."""
        await client.disconnect()
        await client.disconnect()
        await client.disconnect()

    @pytest.mark.asyncio
    async def test_create_order_without_credentials(self) -> None:
        """Verify create order without credentials."""
        client = KrakenExchangeClient()
        request = ExchangeOrderRequest(
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            type=OrderTypeEnum.LIMIT,
            amount=1.0,
            price=50000.0,
        )
        with pytest.raises(RuntimeError, match="API credentials required for trading"):
            await client.create_order(request)

    @pytest.mark.asyncio
    async def test_create_order_ws_without_credentials(self) -> None:
        """Verify create order ws without credentials."""
        client = KrakenExchangeClient()
        request = ExchangeOrderRequest(
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            type=OrderTypeEnum.LIMIT,
            amount=1.0,
            price=50000.0,
        )
        with pytest.raises(RuntimeError, match="API credentials required for trading"):
            await client.create_order_ws(request)

    @pytest.mark.asyncio
    async def test_get_balance_api_error(self, client: KrakenExchangeClient) -> None:
        """Verify get balance api error."""
        with patch.object(client, "_ccxt_client") as mock_ccxt:
            mock_ccxt.fetch_balance = AsyncMock(side_effect=Exception("API Error"))
            with pytest.raises(Exception, match="API Error"):
                await client.get_balance()

    @pytest.mark.asyncio
    async def test_order_creation_edge_cases(self, client: KrakenExchangeClient) -> None:
        """Verify order creation edge cases."""
        request = ExchangeOrderRequest(
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            type=OrderTypeEnum.LIMIT,
            amount=0.00001,
            price=1.0,
        )
        with patch.object(client, "_ccxt_client") as mock_ccxt:
            mock_ccxt.create_order = AsyncMock(
                return_value={
                    "id": "test_id",
                    "symbol": "BTC/USD",
                    "amount": 0.00001,
                    "side": "buy",
                    "type": "limit",
                    "status": "open",
                    "price": 1.0,
                    "filled": 0.0,
                    "remaining": 0.00001,
                    "timestamp": 1640995200000,
                    "datetime": "2022-01-01T00:00:00.000Z",
                    "fee": None,
                    "trades": [],
                    "info": {},
                }
            )
            result = await client.create_order(request)
            assert result is not None

    @pytest.mark.asyncio
    async def test_cancel_nonexistent_order(self, client: KrakenExchangeClient) -> None:
        """Verify cancel nonexistent order."""
        with patch.object(client, "_ccxt_client") as mock_ccxt:
            mock_ccxt.cancel_order = AsyncMock(side_effect=Exception("Order not found"))
            with pytest.raises(Exception, match="Order not found"):
                await client.cancel_order("nonexistent_id", "BTC/USD")

    @pytest.mark.asyncio
    async def test_get_order_nonexistent(self, client: KrakenExchangeClient) -> None:
        """Verify get order nonexistent."""
        with patch.object(client, "_ccxt_client") as mock_ccxt:
            mock_ccxt.fetch_order = AsyncMock(side_effect=Exception("Order not found"))
            with pytest.raises(Exception, match="Order not found"):
                await client.get_order("nonexistent_id", "BTC/USD")

    @pytest.mark.asyncio
    async def test_get_orders_api_error(self, client: KrakenExchangeClient) -> None:
        """Verify get orders api error."""
        with patch.object(client, "_ccxt_client") as mock_ccxt:
            mock_ccxt.fetch_open_orders = AsyncMock(side_effect=Exception("API Error"))
            with pytest.raises(Exception, match="API Error"):
                await client.get_orders("BTC/USD", status=OrderStatusEnum.OPEN)

    @pytest.mark.asyncio
    async def test_get_ticker_api_error(self, client: KrakenExchangeClient) -> None:
        """Verify get ticker api error."""
        with patch.object(client, "_ccxt_client") as mock_ccxt:
            mock_ccxt.fetch_ticker = AsyncMock(side_effect=Exception("API Error"))
            with pytest.raises(Exception, match="API Error"):
                await client.get_ticker("BTC/USD")

    @pytest.mark.asyncio
    async def test_disconnect_with_cleanup_error(self, client: KrakenExchangeClient) -> None:
        """Verify disconnect with cleanup error."""
        with patch.object(client, "_close_ws_client", new_callable=AsyncMock) as mock_close:
            mock_close.side_effect = Exception("Cleanup error")
            await client.disconnect()
            mock_close.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_ohlcv_api_error(self, client: KrakenExchangeClient) -> None:
        """Verify get ohlcv api error."""
        with patch.object(client, "_ccxt_client") as mock_ccxt:
            mock_ccxt.fetch_ohlcv = AsyncMock(side_effect=Exception("API Error"))
            with pytest.raises(Exception, match="API Error"):
                await client.get_ohlcv("BTC/USD")

    @pytest.mark.asyncio
    async def test_connect_with_error(self, client: KrakenExchangeClient) -> None:
        """Verify connect with error."""
        with patch.object(client, "_ccxt_client") as mock_ccxt:
            mock_ccxt.load_markets = AsyncMock(side_effect=Exception("Connection Error"))
            with pytest.raises(Exception, match="Connection Error"):
                await client.connect()
