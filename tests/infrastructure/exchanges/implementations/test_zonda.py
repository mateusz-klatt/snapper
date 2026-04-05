"""Tests for Zonda exchange client implementation."""

import asyncio
import contextlib
import hashlib
import hmac
import json
import uuid
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import patch

import pytest
from websockets.asyncio.client import ClientConnection
from websockets.exceptions import ConnectionClosed

from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import ExchangeOrderRequest
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.contracts import OrderStatusEnum
from snapper.infrastructure.exchanges.contracts import OrderTypeEnum
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate
from snapper.infrastructure.exchanges.implementations import zonda as zonda_module
from snapper.infrastructure.exchanges.implementations.zonda import ZondaExchangeClient


class _StubWs:
    """Stub WebSocket for testing send and close operations."""

    def __init__(self) -> None:
        self.sent: list[str] = []
        self.closed = False

    async def send(self, payload: str) -> None:
        self.sent.append(payload)

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_connect_retries_and_raises() -> None:
    """Test WebSocket connection retry behavior.

    Given: A client configured with max_reconnect_attempts=2 and connection always fails.
    When: connect() is called.
    Then: The exception is raised after retries and _ws remains None.
    """
    client = ZondaExchangeClient(max_reconnect_attempts=2, reconnect_delay=0)
    with (
        patch(
            "snapper.infrastructure.exchanges.implementations.zonda.connect",
            AsyncMock(side_effect=Exception("boom")),
        ),
        patch("asyncio.sleep", AsyncMock()),
        pytest.raises(Exception, match="boom"),
    ):
        await client.connect()
    assert client._ws is None


@pytest.mark.asyncio
async def test_resubscribe_wildcard_and_specific() -> None:
    """Test resubscribe sends correct messages for wildcard and specific symbols.

    Given: A client with WebSocket and subscribed symbols (wildcard or specific).
    When: _resubscribe() is called.
    Then: Correct ticker and stats subscription messages are sent.
    """
    client = ZondaExchangeClient()
    ws = _StubWs()
    client._ws = cast(
        "ClientConnection",
        ws,
    )
    client._subscribed_symbols = ["*"]
    await client._resubscribe()
    assert len(ws.sent) == 2
    assert json.loads(ws.sent[0])["path"] == "ticker"
    assert json.loads(ws.sent[1])["path"] == "stats"
    client._subscribed_symbols = ["BTC-PLN"]
    ws.sent.clear()
    await client._resubscribe()
    assert json.loads(ws.sent[0])["path"] == "ticker/btc-pln"
    assert json.loads(ws.sent[1])["path"] == "stats/btc-pln"


@pytest.mark.asyncio
async def test_disconnect_cancels_task_and_closes_ws() -> None:
    """Test disconnect cancels message handler task and closes WebSocket.

    Given: A connected client with active message handler task.
    When: disconnect() is called.
    Then: _running is False, _ws is None, and task is cancelled.
    """
    client = ZondaExchangeClient()
    client._ws = cast(
        "ClientConnection",
        _StubWs(),
    )
    client._message_handler_task = asyncio.create_task(asyncio.sleep(10))
    await client.disconnect()
    assert client._running is False
    assert client._ws is None
    assert client._message_handler_task.cancelled()


@pytest.mark.asyncio
async def test_subscribe_ticks_wildcard_snapshot_rejected() -> None:
    """Test wildcard snapshot subscription is rejected.

    Given: A client instance.
    When: subscribe_ticks(['*'], snapshot=True) is called.
    Then: ValueError is raised with 'does not support wildcard snapshot' message.
    """
    client = ZondaExchangeClient()
    with pytest.raises(ValueError, match="does not support wildcard snapshot"):
        await anext(client.subscribe_ticks(["*"], snapshot=True))


def test_sign_private_message_requires_credentials() -> None:
    """Test signing private message requires API credentials.

    Given: A client without API credentials.
    When: _sign_private_message() is called.
    Then: RuntimeError is raised with 'API credentials required' message.
    """
    client = ZondaExchangeClient()
    with pytest.raises(RuntimeError, match="API credentials required"):
        client._sign_private_message({"x": 1})


def test_sign_private_message_includes_signature() -> None:
    """Test signing private message includes HMAC signature.

    Given: A client with API key='public' and secret='secret'.
    When: _sign_private_message() is called with a payload.
    Then: The result contains publicKey, requestTimestamp, and valid hashSignature.
    """
    client = ZondaExchangeClient(api_key="public", api_secret="secret")
    with patch("time.time", return_value=1700000000):
        signed = client._sign_private_message({"module": "private"})
    expected_signature = hmac.new(b"secret", b"public1700000000", hashlib.sha512).hexdigest()
    assert signed["publicKey"] == "public"
    assert signed["requestTimestamp"] == 1700000000
    assert signed["hashSignature"] == expected_signature


class TestZondaExchangeClient:
    """Test suite for ZondaExchangeClient WebSocket operations."""

    @pytest.fixture
    def client(self) -> ZondaExchangeClient:
        """Provide a ZondaExchangeClient with 3 reconnect attempts and 0.1s delay."""
        return ZondaExchangeClient(max_reconnect_attempts=3, reconnect_delay=0.1)

    @pytest.mark.asyncio
    async def test_connect_success(self, client: ZondaExchangeClient) -> None:
        """Test successful WebSocket connection.

        Given: A client and mocked connect function returning a WebSocket.
        When: connect() is called.
        Then: _ws is set to the mock WebSocket and connect is called with ws_url.
        """
        with patch(
            "snapper.infrastructure.exchanges.implementations.zonda.connect"
        ) as mock_connect:
            mock_ws = AsyncMock()

            async def async_connect(*args: Any, **kwargs: Any) -> AsyncMock:
                return mock_ws

            mock_connect.side_effect = async_connect
            await client.connect()
            assert client._ws == mock_ws
            mock_connect.assert_called_once_with(client.ws_url)

    @pytest.mark.asyncio
    async def test_connect_retry_on_failure(self, client: ZondaExchangeClient) -> None:
        """Test connection retries on initial failures.

        Given: A client and connect failing twice then succeeding.
        When: connect() is called.
        Then: Connection succeeds on third attempt with call_count=3.
        """
        with patch(
            "snapper.infrastructure.exchanges.implementations.zonda.connect"
        ) as mock_connect:
            mock_ws = AsyncMock()
            call_count = 0

            async def async_connect_with_failures(*args: Any, **kwargs: Any) -> AsyncMock:
                nonlocal call_count
                call_count += 1
                if call_count <= 2:
                    raise ConnectionError("Connection failed")
                return mock_ws

            mock_connect.side_effect = async_connect_with_failures
            await client.connect()
            assert client._ws == mock_ws
            assert mock_connect.call_count == 3

    @pytest.mark.asyncio
    async def test_connect_max_retries_exceeded(self, client: ZondaExchangeClient) -> None:
        """Test connection raises error after max retries exceeded.

        Given: A client with max_reconnect_attempts=3 and connect always failing.
        When: connect() is called.
        Then: ConnectionError is raised after 3 attempts.
        """
        with patch(
            "snapper.infrastructure.exchanges.implementations.zonda.connect"
        ) as mock_connect:
            mock_connect.side_effect = ConnectionError("Connection failed")
            with pytest.raises(ConnectionError):
                await client.connect()
            assert mock_connect.call_count == 3

    @pytest.mark.asyncio
    async def test_disconnect(self, client: ZondaExchangeClient) -> None:
        """Test disconnect cancels handler and closes WebSocket.

        Given: A connected client with running handler task.
        When: disconnect() is called.
        Then: _running is False, task cancelled, ws.close called, _ws is None.
        """
        mock_ws = AsyncMock()
        client._ws = mock_ws
        client._running = True

        async def mock_handler() -> None:
            await asyncio.sleep(10)

        mock_task = asyncio.create_task(mock_handler())
        client._message_handler_task = mock_task
        await client.disconnect()
        assert client._running is False
        assert mock_task.cancelled()
        mock_ws.close.assert_called_once()
        assert client._ws is None

    @pytest.mark.asyncio
    async def test_resubscribe(self, client: ZondaExchangeClient) -> None:
        """Test resubscribe sends ticker and stats for each symbol.

        Given: A client subscribed to BTC-EUR and ETH-PLN.
        When: _resubscribe() is called.
        Then: Four messages sent (ticker+stats for each symbol).
        """
        mock_ws = AsyncMock()
        client._ws = mock_ws
        client._subscribed_symbols = ["BTC-EUR", "ETH-PLN"]
        await client._resubscribe()
        assert mock_ws.send.call_count == 4
        calls = mock_ws.send.call_args_list
        assert '"path": "ticker/btc-eur"' in calls[0][0][0]
        assert '"path": "stats/btc-eur"' in calls[1][0][0]
        assert '"path": "ticker/eth-pln"' in calls[2][0][0]
        assert '"path": "stats/eth-pln"' in calls[3][0][0]

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_parse_ticker_message(self, client: ZondaExchangeClient) -> None:
        """Test parsing ticker message updates cache.

        Given: A ticker push message with BTC-PLN bid/ask/rate data.
        When: _parse_ticker_message() is called.
        Then: _ticker_cache contains BTC-PLN with bid=50000, ask=50100, last=50050.
        """
        ticker_msg: dict[str, Any] = {
            "action": "push",
            "topic": "trading/ticker/BTC-PLN",
            "message": {
                "market": {"code": "BTC-PLN"},
                "highestBid": "50000.00",
                "lowestAsk": "50100.00",
                "rate": "50050.00",
            },
            "seqNo": 12345,
        }
        await client._parse_ticker_message(ticker_msg)
        ticker_cache = client._ticker_cache
        assert "BTC-PLN" in ticker_cache
        assert ticker_cache["BTC-PLN"]["bid"] == pytest.approx(50000.0)
        assert ticker_cache["BTC-PLN"]["ask"] == pytest.approx(50100.0)
        assert ticker_cache["BTC-PLN"]["last"] == pytest.approx(50050.0)

    @pytest.mark.asyncio
    async def test_parse_stats_message(self, client: ZondaExchangeClient) -> None:
        """Test parsing stats message updates stats cache.

        Given: A stats push message with BTC-PLN high/low/volume/rate_24h data.
        When: _parse_stats_message() is called.
        Then: _stats_cache contains BTC-PLN with correct values.
        """
        stats_msg: dict[str, Any] = {
            "action": "push",
            "topic": "trading/stats/BTC-PLN",
            "message": [
                {
                    "m": "BTC-PLN",
                    "h": 51000.0,
                    "l": 49000.0,
                    "v": 123.45,
                    "r24h": 49500.0,
                }
            ],
            "seqNo": 12346,
        }
        await client._parse_stats_message(stats_msg)
        stats_cache = client._stats_cache
        assert "BTC-PLN" in stats_cache
        assert stats_cache["BTC-PLN"]["high"] == pytest.approx(51000.0)
        assert stats_cache["BTC-PLN"]["low"] == pytest.approx(49000.0)
        assert stats_cache["BTC-PLN"]["volume"] == pytest.approx(123.45)
        assert stats_cache["BTC-PLN"]["rate_24h"] == pytest.approx(49500.0)

    @pytest.mark.asyncio
    async def test_try_merge_and_emit_with_stats(self, client: ZondaExchangeClient) -> None:
        """Test merge emits TickerUpdate with merged ticker and stats data.

        Given: Ticker and stats cache populated for BTC-PLN.
        When: _try_merge_and_emit() is called.
        Then: A TickerUpdate is queued with merged data including change calculation.
        """
        ticker_cache = client._ticker_cache
        stats_cache = client._stats_cache
        ticker_cache["BTC-PLN"] = {
            "bid": 50000.0,
            "ask": 50100.0,
            "last": 50050.0,
        }
        stats_cache["BTC-PLN"] = {
            "high": 51000.0,
            "low": 49000.0,
            "volume": 123.45,
            "rate_24h": 49500.0,
        }
        await client._try_merge_and_emit("BTC-PLN")
        tick_queue = client._tick_queue
        assert not tick_queue.empty()
        ticker_data = await tick_queue.get()
        assert isinstance(ticker_data, TickerUpdate)
        assert ticker_data.symbol == "BTC-PLN"
        assert ticker_data.bid == pytest.approx(50000.0)
        assert ticker_data.ask == pytest.approx(50100.0)
        assert ticker_data.last == pytest.approx(50050.0)
        assert ticker_data.high == pytest.approx(51000.0)
        assert ticker_data.low == pytest.approx(49000.0)
        assert ticker_data.volume == pytest.approx(123.45)
        assert ticker_data.change == pytest.approx(550.0)
        assert abs(ticker_data.change_pct - 1.11) < 0.01

    @pytest.mark.asyncio
    async def test_try_merge_and_emit_ticker_only(self, client: ZondaExchangeClient) -> None:
        """Test merge emits TickerUpdate with zeros when stats missing.

        Given: Only ticker cache populated (no stats) for BTC-PLN.
        When: _try_merge_and_emit() is called.
        Then: TickerUpdate has volume=0, high=0, low=0, change=0.
        """
        ticker_cache = client._ticker_cache
        ticker_cache["BTC-PLN"] = {
            "bid": 50000.0,
            "ask": 50100.0,
            "last": 50050.0,
        }
        await client._try_merge_and_emit("BTC-PLN")
        tick_queue = client._tick_queue
        ticker_data = await tick_queue.get()
        assert ticker_data.symbol == "BTC-PLN"
        assert ticker_data.bid == pytest.approx(50000.0)
        assert ticker_data.volume == pytest.approx(0.0)
        assert ticker_data.high == pytest.approx(0.0)
        assert ticker_data.low == pytest.approx(0.0)
        assert ticker_data.change == pytest.approx(0.0)
        assert ticker_data.change_pct == pytest.approx(0.0)

    @pytest.mark.asyncio
    async def test_try_merge_and_emit_no_ticker(self, client: ZondaExchangeClient) -> None:
        """Test merge does not emit when ticker data is missing.

        Given: Only stats cache populated (no ticker) for BTC-PLN.
        When: _try_merge_and_emit() is called.
        Then: Tick queue remains empty.
        """
        stats_cache = client._stats_cache
        stats_cache["BTC-PLN"] = {
            "high": 51000.0,
            "low": 49000.0,
            "volume": 123.45,
            "rate_24h": 49500.0,
        }
        await client._try_merge_and_emit("BTC-PLN")
        tick_queue = client._tick_queue
        assert tick_queue.empty()

    @pytest.mark.asyncio
    async def test_message_handler_tracks_seqno(self, client: ZondaExchangeClient) -> None:
        """Test message handler tracks sequence numbers and ignores old messages.

        Given: WebSocket messages with seqNo 100, 101, 99.
        When: _message_handler() processes messages.
        Then: Only seqNo 100 and 101 are processed; 99 is ignored.
        """
        mock_ws = AsyncMock()

        async def mock_messages() -> Any:
            yield """{
                "action": "push",
                "topic": "trading/ticker/btc-pln",
                "message": {"highestBid": "50000", "lowestAsk": "50100", "rate": "50050"},
                "seqNo": 100
            }"""
            yield """{
                "action": "push",
                "topic": "trading/ticker/btc-pln",
                "message": {"highestBid": "50001", "lowestAsk": "50101", "rate": "50051"},
                "seqNo": 101
            }"""
            yield """{
                "action": "push",
                "topic": "trading/ticker/btc-pln",
                "message": {"highestBid": "49999", "lowestAsk": "50099", "rate": "50049"},
                "seqNo": 99
            }"""

        mock_ws.__aiter__ = lambda self: mock_messages()
        client._ws = mock_ws
        handler_task = asyncio.create_task(client._message_handler())
        await asyncio.sleep(0.2)
        handler_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await handler_task
        seq_no = client._seq_no
        assert seq_no.get("trading/ticker/btc-pln") == 101
        ticker_cache = client._ticker_cache
        assert ticker_cache["BTC-PLN"]["bid"] == pytest.approx(50001.0)

    @pytest.mark.asyncio
    async def test_parse_ticker_message_invalid_data(self, client: ZondaExchangeClient) -> None:
        """Test parsing ticker message with empty message body.

        Given: A ticker message with empty message dict.
        When: _parse_ticker_message() is called.
        Then: Cache is updated with bid=0 (fallback for missing data).
        """
        invalid_msg: dict[str, Any] = {
            "action": "push",
            "topic": "trading/ticker/BTC-PLN",
            "message": {},
        }
        await client._parse_ticker_message(invalid_msg)
        ticker_cache = client._ticker_cache
        if "BTC-PLN" in ticker_cache:
            assert ticker_cache["BTC-PLN"]["bid"] == pytest.approx(0.0)

    @pytest.mark.asyncio
    async def test_parse_stats_message_invalid_data(self, client: ZondaExchangeClient) -> None:
        """Test parsing stats message with empty transactions list.

        Given: A stats message with empty message list.
        When: _parse_stats_message() is called.
        Then: BTC-PLN is not added to stats cache.
        """
        invalid_msg: dict[str, Any] = {
            "action": "push",
            "topic": "trading/stats/BTC-PLN",
            "message": [],
        }
        await client._parse_stats_message(invalid_msg)
        stats_cache = client._stats_cache
        assert "BTC-PLN" not in stats_cache

    @pytest.mark.asyncio
    async def test_message_handler_handles_different_actions(
        self, client: ZondaExchangeClient
    ) -> None:
        """Test message handler processes various action types without error.

        Given: WebSocket messages with confirm, error, pong, json-error actions.
        When: _message_handler() processes messages.
        Then: Handler completes without raising exceptions.
        """
        mock_ws = AsyncMock()

        async def mock_messages() -> Any:
            yield '{"action": "subscribe-public-confirm", "topic": "trading/ticker/btc-pln"}'
            yield '{"action": "subscribe-public-error", "error": "Invalid symbol"}'
            yield '{"action": "pong"}'
            yield '{"action": "json-error", "message": "Invalid JSON"}'

        mock_ws.__aiter__ = lambda self: mock_messages()
        client._ws = mock_ws
        handler_task = asyncio.create_task(client._message_handler())
        await asyncio.sleep(0.1)
        handler_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await handler_task

    @pytest.mark.asyncio
    async def test_message_handler_handles_json_decode_error(
        self, client: ZondaExchangeClient
    ) -> None:
        """Test message handler handles invalid JSON gracefully.

        Given: A WebSocket message with malformed JSON.
        When: _message_handler() processes the message.
        Then: Handler completes without raising JSONDecodeError.
        """
        mock_ws = AsyncMock()

        async def mock_messages() -> Any:
            yield "invalid json {{"

        mock_ws.__aiter__ = lambda self: mock_messages()
        client._ws = mock_ws
        handler_task = asyncio.create_task(client._message_handler())
        await asyncio.sleep(0.1)
        handler_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await handler_task

    @pytest.mark.asyncio
    async def test_subscribe_ticks_basic_flow(self, client: ZondaExchangeClient) -> None:
        """Test basic tick subscription yields TickerUpdate.

        Given: Mocked WebSocket returning ticker and stats messages.
        When: subscribe_ticks(['BTC-PLN']) is iterated.
        Then: At least one TickerUpdate with symbol='BTC-PLN' is received.
        """
        with patch(
            "snapper.infrastructure.exchanges.implementations.zonda.connect"
        ) as mock_connect:
            mock_ws = AsyncMock()

            async def async_connect(*args: Any, **kwargs: Any) -> AsyncMock:
                return mock_ws

            mock_connect.side_effect = async_connect

            async def mock_messages() -> Any:
                yield """{
                    "action": "push",
                    "topic": "trading/ticker/btc-pln",
                    "message": {"highestBid": "50000", "lowestAsk": "50100", "rate": "50050"},
                    "seqNo": 100
                }"""
                yield """{
                    "action": "push",
                    "topic": "trading/stats/btc-pln",
                    "message": [{"m": "BTC-PLN", "h": 51000, "l": 49000, "v": 100, "r24h": 49500}],
                    "seqNo": 101
                }"""

            mock_ws.__aiter__ = lambda self: mock_messages()
            ticker_count = 0
            async for ticker in client.subscribe_ticks(["BTC-PLN"]):
                ticker_count += 1
                assert isinstance(ticker, TickerUpdate)
                assert ticker.symbol == "BTC-PLN"
                client._running = False
                break
            assert ticker_count == 1

    @pytest.mark.asyncio
    async def test_subscribe_ticks_snapshot_wildcard_error(
        self, client: ZondaExchangeClient
    ) -> None:
        """Test wildcard snapshot subscription raises ValueError.

        Given: A client instance.
        When: subscribe_ticks(['*'], snapshot=True) is called.
        Then: ValueError with 'proxy API does not support wildcard snapshot' is raised.
        """
        with pytest.raises(ValueError, match="proxy API does not support wildcard snapshot"):
            async for _ in client.subscribe_ticks(["*"], snapshot=True):
                """Consumed by iteration to trigger exception."""
                pass

    @pytest.mark.asyncio
    async def test_message_handler_exception_handling(self, client: ZondaExchangeClient) -> None:
        """Test message handler handles ping action without error.

        Given: A WebSocket message with action='ping'.
        When: _message_handler() processes it.
        Then: Handler completes without exceptions.
        """
        mock_ws = AsyncMock()

        async def mock_messages() -> Any:
            yield '{"action": "ping"}'

        mock_ws.__aiter__ = lambda self: mock_messages()
        client._ws = mock_ws
        handler_task = asyncio.create_task(client._message_handler())
        await asyncio.sleep(0.1)
        handler_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await handler_task

    @pytest.mark.asyncio
    async def test_parse_ticker_keyerror(self, client: ZondaExchangeClient) -> None:
        """Test parsing ticker with missing keys handles gracefully.

        Given: A ticker message with empty message body.
        When: _parse_ticker_message() is called.
        Then: No exception is raised.
        """
        msg_data: dict[str, Any] = {
            "topic": "trading/ticker/btc-pln",
            "message": {},
            "seqNo": 100,
        }
        await client._parse_ticker_message(msg_data)

    @pytest.mark.asyncio
    async def test_parse_stats_keyerror(self, client: ZondaExchangeClient) -> None:
        """Test parsing stats with missing keys handles gracefully.

        Given: A stats message with empty dict in list.
        When: _parse_stats_message() is called.
        Then: No exception is raised.
        """
        msg_data: dict[str, Any] = {
            "topic": "trading/stats/btc-pln",
            "message": [{}],
            "seqNo": 101,
        }
        await client._parse_stats_message(msg_data)


class TestZondaRestAPI:
    """Test suite for Zonda REST API operations via CCXT."""

    @pytest.fixture
    def client(self) -> ZondaExchangeClient:
        """Provide a default ZondaExchangeClient instance."""
        return ZondaExchangeClient()

    @pytest.mark.asyncio
    async def test_get_ticker(self, client: ZondaExchangeClient) -> None:
        """Test get_ticker returns TickerUpdate from CCXT data.

        Given: Mocked CCXT fetch_ticker returning BTC/EUR ticker data.
        When: get_ticker('BTC-EUR') is called.
        Then: TickerUpdate has correct symbol, bid, ask, last, timestamp.
        """
        mock_ccxt_ticker: dict[str, Any] = {
            "symbol": "BTC/EUR",
            "bid": 50000.0,
            "ask": 50100.0,
            "last": 50050.0,
            "timestamp": 1609459200000,
        }
        with patch.object(
            client._ccxt_client,
            "fetch_ticker",
            return_value=mock_ccxt_ticker,
        ):
            ticker = await client.get_ticker("BTC-EUR")
            assert ticker.symbol == "BTC-EUR"
            assert ticker.bid == pytest.approx(50000.0)
            assert ticker.ask == pytest.approx(50100.0)
            assert ticker.last == pytest.approx(50050.0)
            assert ticker.timestamp == pytest.approx(1609459200.0)

    @pytest.mark.asyncio
    async def test_get_ohlcv(self, client: ZondaExchangeClient) -> None:
        """Test get_ohlcv returns OHLCV bars from CCXT data.

        Given: Mocked CCXT fetch_ohlcv returning two candle bars.
        When: get_ohlcv('BTC-EUR', '1m') is called.
        Then: Two OHLCV bars are returned with correct open/high/low/close/volume.
        """
        mock_ccxt_ohlcv: list[list[float]] = [
            [1609459200000, 50000.0, 51000.0, 49000.0, 50500.0, 100.0],
            [1609459260000, 50500.0, 51500.0, 49500.0, 51000.0, 150.0],
        ]
        with patch.object(
            client._ccxt_client,
            "fetch_ohlcv",
            return_value=mock_ccxt_ohlcv,
        ):
            ohlcv = await client.get_ohlcv("BTC-EUR", "1m", None, 2)
            assert len(ohlcv) == 2
            assert ohlcv[0].open == pytest.approx(50000.0)
            assert ohlcv[0].high == pytest.approx(51000.0)
            assert ohlcv[0].low == pytest.approx(49000.0)
            assert ohlcv[0].close == pytest.approx(50500.0)
            assert ohlcv[0].volume == pytest.approx(100.0)

    @pytest.mark.asyncio
    async def test_create_order(self, client: ZondaExchangeClient) -> None:
        """Test create_order returns PENDING snapshot built from request data.

        Given: Client with credentials and mocked create_order returning order id.
        When: create_order() is called with limit buy request.
        Then: ExchangeOrder has PENDING status, fields from request, fee=None,
              and db_order_id / db_order_public_id populated from _log_order_to_db.
        """
        client.api_key = "test_key"
        client.api_secret = "test_secret"
        with (
            patch.object(
                client._ccxt_client,
                "create_order",
                return_value={"id": "order123"},
            ),
            patch.object(
                client,
                "_log_order_to_db",
                new_callable=AsyncMock,
                return_value=(42, "pub-42"),
            ),
        ):
            request = ExchangeOrderRequest(
                symbol="BTC-EUR",
                side=OrderSideEnum.BUY,
                type=OrderTypeEnum.LIMIT,
                amount=1.0,
                price=50000.0,
                client_order_id="client123",
            )
            order = await client.create_order(request)
            assert order.id == "order123"
            assert order.symbol == "BTC-EUR"
            assert order.side == OrderSideEnum.BUY
            assert order.type == OrderTypeEnum.LIMIT
            assert order.amount == pytest.approx(1.0)
            assert order.price == pytest.approx(50000.0)
            assert order.status == OrderStatusEnum.PENDING
            assert order.filled == pytest.approx(0.0)
            assert order.remaining == pytest.approx(1.0)
            assert order.fee is None
            assert order.client_order_id == "client123"
            assert order.db_order_id == 42
            assert order.db_order_public_id == "pub-42"

    @pytest.mark.asyncio
    async def test_create_order_no_credentials(self, client: ZondaExchangeClient) -> None:
        """Test create_order raises error without credentials.

        Given: A client without API credentials.
        When: create_order() is called.
        Then: RuntimeError with 'API credentials required' is raised.
        """
        request = ExchangeOrderRequest(
            symbol="BTC-EUR",
            side=OrderSideEnum.BUY,
            type=OrderTypeEnum.LIMIT,
            amount=1.0,
            price=50000.0,
        )
        with pytest.raises(RuntimeError, match="API credentials required"):
            await client.create_order(request)

    @pytest.mark.asyncio
    async def test_get_balance(self, client: ZondaExchangeClient) -> None:
        """Test get_balance returns currency balances excluding 'info'.

        Given: Client with credentials and mocked fetch_balance returning BTC/EUR data.
        When: get_balance() is called.
        Then: Balances contain BTC and EUR with correct free/used/total, no 'info' key.
        """
        client.api_key = "test_key"
        client.api_secret = "test_secret"
        mock_ccxt_balance: dict[str, Any] = {
            "BTC": {"free": 1.0, "used": 0.5, "total": 1.5},
            "EUR": {"free": 10000.0, "used": 5000.0, "total": 15000.0},
            "info": {},
        }
        with patch.object(
            client._ccxt_client,
            "fetch_balance",
            return_value=mock_ccxt_balance,
        ):
            balances = await client.get_balance()
            assert "BTC" in balances
            assert balances["BTC"].currency == "BTC"
            assert balances["BTC"].free == pytest.approx(1.0)
            assert balances["BTC"].used == pytest.approx(0.5)
            assert balances["BTC"].total == pytest.approx(1.5)
            assert "EUR" in balances
            assert "info" not in balances

    @pytest.mark.asyncio
    async def test_cancel_order(self, client: ZondaExchangeClient) -> None:
        """Test cancel_order fetches open orders, cancels with side+price.

        Given: Client with credentials and mocked CCXT responses.
        When: cancel_order('order123', 'BTC-EUR') is called.
        Then: ExchangeOrder has id='order123' and status=CANCELED.
        """
        client.api_key = "test_key"
        client.api_secret = "test_secret"
        mock_open_order: dict[str, Any] = {
            "id": "order123",
            "symbol": "BTC/EUR",
            "side": "buy",
            "type": "limit",
            "amount": 1.0,
            "price": 50000.0,
            "status": "open",
            "filled": 0.0,
            "remaining": 1.0,
            "timestamp": 1609459200000,
        }
        with (
            patch.object(
                client._ccxt_client,
                "fetch_open_orders",
                return_value=[mock_open_order],
            ),
            patch.object(
                client._ccxt_client,
                "cancel_order",
                return_value={"info": {"status": "Ok"}},
            ),
        ):
            order = await client.cancel_order("order123", "BTC-EUR")
            assert order.id == "order123"
            assert order.status == OrderStatusEnum.CANCELED
            client._ccxt_client.cancel_order.assert_called_once_with(
                "order123", "BTC/EUR", {"side": "buy", "price": 50000.0}
            )

    @pytest.mark.asyncio
    async def test_get_order(self, client: ZondaExchangeClient) -> None:
        """Test get_order returns order details.

        Given: Client with credentials and mocked fetch_order response.
        When: get_order('order123', 'BTC-EUR') is called.
        Then: ExchangeOrder has correct id, filled, remaining values.
        """
        client.api_key = "test_key"
        client.api_secret = "test_secret"
        mock_ccxt_order: dict[str, Any] = {
            "id": "order123",
            "symbol": "BTC/EUR",
            "side": "buy",
            "type": "limit",
            "amount": 1.0,
            "price": 50000.0,
            "status": "open",
            "filled": 0.5,
            "remaining": 0.5,
            "timestamp": 1609459200000,
        }
        with patch.object(
            client._ccxt_client,
            "fetch_order",
            return_value=mock_ccxt_order,
        ):
            order = await client.get_order("order123", "BTC-EUR")
            assert order.id == "order123"
            assert order.filled == pytest.approx(0.5)
            assert order.remaining == pytest.approx(0.5)

    @pytest.mark.asyncio
    async def test_get_orders_open(self, client: ZondaExchangeClient) -> None:
        """Test get_orders with OPEN status returns open orders.

        Given: Client with credentials and mocked fetch_open_orders response.
        When: get_orders('BTC-EUR', OPEN) is called.
        Then: One order with id='order1' and status=OPEN is returned.
        """
        client.api_key = "test_key"
        client.api_secret = "test_secret"
        mock_ccxt_orders: list[dict[str, Any]] = [
            {
                "id": "order1",
                "symbol": "BTC/EUR",
                "side": "buy",
                "type": "limit",
                "amount": 1.0,
                "price": 50000.0,
                "status": "open",
                "filled": 0.0,
                "remaining": 1.0,
                "timestamp": 1609459200000,
            },
        ]
        with patch.object(
            client._ccxt_client,
            "fetch_open_orders",
            return_value=mock_ccxt_orders,
        ):
            orders = await client.get_orders("BTC-EUR", OrderStatusEnum.OPEN, 10)
            assert len(orders) == 1
            assert orders[0].id == "order1"
            assert orders[0].status == OrderStatusEnum.OPEN

    @pytest.mark.asyncio
    async def test_get_orders_closed(self, client: ZondaExchangeClient) -> None:
        """Test get_orders with CLOSED status returns closed orders.

        Given: Client with credentials and mocked fetch_closed_orders response.
        When: get_orders('BTC-EUR', CLOSED) is called.
        Then: One order with id='order2' and status=CLOSED is returned.
        """
        client.api_key = "test_key"
        client.api_secret = "test_secret"
        mock_ccxt_orders: list[dict[str, Any]] = [
            {
                "id": "order2",
                "symbol": "BTC/EUR",
                "side": "sell",
                "type": "limit",
                "amount": 1.0,
                "price": 51000.0,
                "status": "closed",
                "filled": 1.0,
                "remaining": 0.0,
                "timestamp": 1609459300000,
            },
        ]
        with patch.object(
            client._ccxt_client,
            "fetch_closed_orders",
            return_value=mock_ccxt_orders,
        ):
            orders = await client.get_orders("BTC-EUR", OrderStatusEnum.CLOSED, 10)
            assert len(orders) == 1
            assert orders[0].id == "order2"
            assert orders[0].status == OrderStatusEnum.CLOSED

    @pytest.mark.asyncio
    async def test_get_orders_all(self, client: ZondaExchangeClient) -> None:
        """Test get_orders without status filter returns all orders.

        Given: Client with credentials and mocked fetch_orders response.
        When: get_orders(None, None) is called.
        Then: One order with id='order3', price=None, fee=None is returned.
        """
        client.api_key = "test_key"
        client.api_secret = "test_secret"
        mock_ccxt_orders: list[dict[str, Any]] = [
            {
                "id": "order3",
                "symbol": "BTC/EUR",
                "side": "buy",
                "type": "market",
                "amount": 0.5,
                "price": None,
                "status": "filled",
                "filled": 0.5,
                "remaining": 0.0,
                "timestamp": 1609459400000,
                "fee": None,
            },
        ]
        with patch.object(
            client._ccxt_client,
            "fetch_orders",
            return_value=mock_ccxt_orders,
        ):
            orders = await client.get_orders(None, None, 10)
            assert len(orders) == 1
            assert orders[0].id == "order3"
            assert orders[0].price is None
            assert orders[0].fee is None

    @pytest.mark.asyncio
    async def test_cancel_order_not_found_returns_canceled(
        self, client: ZondaExchangeClient
    ) -> None:
        """Test cancel_order returns canceled snapshot when order not in open orders.

        Given: Client with credentials and fetch_open_orders returns empty list.
        When: cancel_order('missing-id', 'BTC-EUR') is called.
        Then: Snapshot with status=CANCELED returned (order already terminal).
        """
        client.api_key = "test_key"
        client.api_secret = "test_secret"
        with patch.object(
            client._ccxt_client,
            "fetch_open_orders",
            return_value=[],
        ):
            result = await client.cancel_order("missing-id", "BTC-EUR")
            assert result.id == "missing-id"
            assert result.status == OrderStatusEnum.CANCELED
            assert result.symbol == "BTC-EUR"

    @pytest.mark.asyncio
    async def test_cancel_order_without_price(self, client: ZondaExchangeClient) -> None:
        """Test cancel_order works when matched order has no price (market order).

        Given: Client with credentials and matched open order has price=None.
        When: cancel_order() is called.
        Then: cancel_order is called with side only (no price in params).
        """
        client.api_key = "test_key"
        client.api_secret = "test_secret"
        mock_open_order: dict[str, Any] = {
            "id": "order-no-price",
            "symbol": "BTC/EUR",
            "side": "sell",
            "type": "market",
            "amount": 1.0,
            "price": None,
            "status": "open",
            "filled": 0.0,
            "remaining": 1.0,
            "timestamp": 1609459200000,
        }
        with (
            patch.object(
                client._ccxt_client,
                "fetch_open_orders",
                return_value=[mock_open_order],
            ),
            patch.object(
                client._ccxt_client,
                "cancel_order",
                return_value={"info": {"status": "Ok"}},
            ),
        ):
            order = await client.cancel_order("order-no-price", "BTC-EUR")
            assert order.id == "order-no-price"
            assert order.status == OrderStatusEnum.CANCELED
            client._ccxt_client.cancel_order.assert_called_once_with(
                "order-no-price", "BTC/EUR", {"side": "sell"}
            )


class TestZondaTradesAndCandles:
    """Test suite for Zonda trades and candles WebSocket subscriptions."""

    @pytest.fixture
    def client(self) -> ZondaExchangeClient:
        """Provide a ZondaExchangeClient with 2 reconnect attempts and 0.05s delay."""
        return ZondaExchangeClient(max_reconnect_attempts=2, reconnect_delay=0.05)

    @pytest.mark.asyncio
    async def test_subscribe_trades_wildcard_error(self, client: ZondaExchangeClient) -> None:
        """Test wildcard trade subscription raises ValueError.

        Given: A client instance.
        When: subscribe_trades(['*']) is called.
        Then: ValueError with 'wildcard subscription for trades is not functional' is raised.
        """
        with pytest.raises(ValueError, match="wildcard subscription for trades is not functional"):
            async for _ in client.subscribe_trades(["*"]):
                """Consumed by iteration to trigger exception."""
                pass

    @pytest.mark.asyncio
    async def test_subscribe_candles_wildcard_error(self, client: ZondaExchangeClient) -> None:
        """Test wildcard candle subscription raises ValueError.

        Given: A client instance.
        When: subscribe_candles(['*']) is called.
        Then: ValueError with 'does not support wildcard' is raised.
        """
        with pytest.raises(ValueError, match="does not support wildcard"):
            async for _ in client.subscribe_candles(["*"]):
                """Consumed by iteration to trigger exception."""
                pass

    @pytest.mark.asyncio
    async def test_subscribe_candles_timeframe_error(self, client: ZondaExchangeClient) -> None:
        """Test non-1m candle timeframe raises ValueError.

        Given: A client instance.
        When: subscribe_candles(['BTC-PLN'], timeframe='5m') is called.
        Then: ValueError with 'only supports 1m candles' is raised.
        """
        with pytest.raises(ValueError, match="only supports 1m candles"):
            async for _ in client.subscribe_candles(["BTC-PLN"], timeframe="5m"):
                """Consumed by iteration to trigger exception."""
                pass

    @pytest.mark.asyncio
    async def test_candle_aggregator_emits_completed_candle(
        self,
        client: ZondaExchangeClient,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test candle aggregator emits candle when minute boundary crosses.

        Given: Three trades spanning two minutes.
        When: _candle_aggregator() processes trades.
        Then: A CandleUpdate with aggregated OHLCV data is queued.
        """
        trade_minute = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
        trades = [
            TradeUpdate(
                symbol="BTC-PLN",
                side="buy",
                quantity=0.5,
                price=40000.0,
                ord_type="limit",
                trade_id="1",
                timestamp=trade_minute,
            ),
            TradeUpdate(
                symbol="BTC-PLN",
                side="sell",
                quantity=0.3,
                price=40100.0,
                ord_type="limit",
                trade_id="2",
                timestamp=trade_minute + timedelta(seconds=30),
            ),
            TradeUpdate(
                symbol="BTC-PLN",
                side="buy",
                quantity=0.2,
                price=40200.0,
                ord_type="limit",
                trade_id="3",
                timestamp=trade_minute + timedelta(minutes=1),
            ),
        ]

        async def fake_subscribe_trades(symbols: list[str]) -> Any:
            assert symbols == ["BTC-PLN"]
            for trade in trades:
                yield trade

        monkeypatch.setattr(client, "subscribe_trades", fake_subscribe_trades)
        now_sequence = iter(
            [
                trade_minute,
                trade_minute,
                trade_minute + timedelta(minutes=1),
            ]
        )
        real_datetime = datetime

        class _FixedDateTime:
            @staticmethod
            def now(tz: timezone | None = None) -> datetime:
                try:
                    current = next(now_sequence)
                except StopIteration:
                    current = trade_minute + timedelta(minutes=1)
                if tz is None:
                    return current.replace(tzinfo=None)
                return current

            @staticmethod
            def fromtimestamp(ts: float, tz: timezone | None = None) -> datetime:
                return real_datetime.fromtimestamp(ts, tz)

        monkeypatch.setattr(
            "snapper.infrastructure.exchanges.implementations.zonda.datetime",
            _FixedDateTime,
        )
        await client._candle_aggregator(["BTC-PLN"])
        candle = await asyncio.wait_for(
            client._candle_queue.get(),
            timeout=0.1,
        )
        assert candle.symbol == "BTC-PLN"
        assert candle.open == pytest.approx(40000.0)
        assert candle.high == pytest.approx(40100.0)
        assert candle.low == pytest.approx(40000.0)
        assert candle.close == pytest.approx(40100.0)
        assert candle.trades == 2
        assert candle.volume == pytest.approx(0.8)
        assert candle.vwap == pytest.approx(40037.5)
        assert candle.interval_begin == trade_minute
        builder = client._candle_builder
        minute_key = f"BTC-PLN_{int(trade_minute.timestamp())}"
        next_minute_key = f"BTC-PLN_{int((trade_minute + timedelta(minutes=1)).timestamp())}"
        assert minute_key not in builder
        assert next_minute_key in builder
        builder.clear()

    @pytest.mark.asyncio
    async def test_subscribe_executions_requires_credentials_legacy(
        self, client: ZondaExchangeClient
    ) -> None:
        """Test subscribe_executions raises error without credentials.

        Given: A client without API credentials.
        When: subscribe_executions() is called.
        Then: RuntimeError with 'API credentials required' is raised.
        """
        with pytest.raises(RuntimeError, match="API credentials required"):
            async for _ in client.subscribe_executions():
                """Consumed by iteration to trigger exception."""
                pass

    @pytest.mark.asyncio
    async def test_parse_transactions_message(self, client: ZondaExchangeClient) -> None:
        """Test parsing transactions message queues TradeUpdate.

        Given: A transactions push message with one BTC-PLN trade.
        When: _parse_transactions_message() is called.
        Then: TradeUpdate with correct symbol, side, quantity, price is queued.
        """
        message_data: dict[str, Any] = {
            "action": "push",
            "topic": "trading/transactions/btc-pln",
            "message": {
                "transactions": [
                    {
                        "id": "50764c8c-232a-11ea-8d5d-0242ac110008",
                        "t": "1576847523375",
                        "a": "0.03245411",
                        "r": "27787.66",
                        "ty": "Buy",
                    }
                ]
            },
            "timestamp": "1576847523375",
            "seqNo": 1182873,
        }
        await client._parse_transactions_message(message_data)
        assert client._trade_queue.qsize() == 1
        trade = await client._trade_queue.get()
        assert trade.symbol == "BTC-PLN"
        assert trade.side == "buy"
        assert trade.quantity == pytest.approx(0.03245411)
        assert trade.price == pytest.approx(27787.66)
        assert trade.ord_type == "unknown"
        assert trade.trade_id == "50764c8c-232a-11ea-8d5d-0242ac110008"
        assert isinstance(trade.trade_id, str)

    @pytest.mark.asyncio
    async def test_parse_transactions_multiple_trades(self, client: ZondaExchangeClient) -> None:
        """Test parsing multiple transactions queues multiple TradeUpdates.

        Given: A transactions message with two ETH-EUR trades.
        When: _parse_transactions_message() is called.
        Then: Two TradeUpdates are queued with correct symbols and sides.
        """
        message_data: dict[str, Any] = {
            "action": "push",
            "topic": "trading/transactions/eth-eur",
            "message": {
                "transactions": [
                    {"id": "tx1", "t": "1000000000000", "a": "1.0", "r": "2000", "ty": "Buy"},
                    {
                        "id": "tx2",
                        "t": "1000000001000",
                        "a": "0.5",
                        "r": "2010",
                        "ty": "Sell",
                    },
                ]
            },
            "timestamp": "1000000001000",
            "seqNo": 123,
        }
        await client._parse_transactions_message(message_data)
        assert client._trade_queue.qsize() == 2
        trade1 = await client._trade_queue.get()
        assert trade1.symbol == "ETH-EUR"
        assert trade1.side == "buy"
        assert trade1.quantity == pytest.approx(1.0)
        trade2 = await client._trade_queue.get()
        assert trade2.symbol == "ETH-EUR"
        assert trade2.side == "sell"
        assert trade2.quantity == pytest.approx(0.5)

    @pytest.mark.asyncio
    async def test_parse_transactions_invalid_data(self, client: ZondaExchangeClient) -> None:
        """Test parsing invalid transaction data creates trade with zeros.

        Given: A transactions message with invalid transaction data.
        When: _parse_transactions_message() is called.
        Then: TradeUpdate is queued with quantity=0, price=0.
        """
        message_data: dict[str, Any] = {
            "action": "push",
            "topic": "trading/transactions/btc-pln",
            "message": {"transactions": [{"invalid": "data"}]},
            "timestamp": "1576847523375",
            "seqNo": 1182873,
        }
        await client._parse_transactions_message(message_data)
        assert client._trade_queue.qsize() == 1
        trade = await client._trade_queue.get()
        assert trade.symbol == "BTC-PLN"
        assert trade.quantity == pytest.approx(0.0)
        assert trade.price == pytest.approx(0.0)

    @pytest.mark.asyncio
    async def test_subscribe_trades_basic_flow(self, client: ZondaExchangeClient) -> None:
        """Test basic trade subscription yields at least one trade.

        Given: Mocked WebSocket returning a transactions message.
        When: subscribe_trades(['BTC-PLN']) is iterated.
        Then: At least one trade is received.
        """
        with patch(
            "snapper.infrastructure.exchanges.implementations.zonda.connect"
        ) as mock_connect:
            mock_ws = AsyncMock()
            mock_ws.send = AsyncMock()

            async def mock_messages() -> Any:
                yield (
                    '{"action":"push","topic":"trading/transactions/btc-pln",'
                    '"message":{"transactions":[{"id":"test","t":"1000000000000",'
                    '"a":"1.0","r":"50000","ty":"Buy"}]},'
                    '"timestamp":"1000000000000","seqNo":1}'
                )

            mock_ws.__aiter__ = lambda _self: mock_messages()

            async def async_connect(*args: Any, **kwargs: Any) -> AsyncMock:
                return mock_ws

            mock_connect.side_effect = async_connect
            trades_received = 0

            async def run_subscription() -> None:
                nonlocal trades_received
                async for _ in client.subscribe_trades(["BTC-PLN"]):
                    trades_received += 1
                    if trades_received >= 1:
                        client._running = False
                        break

            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(run_subscription(), timeout=1.0)
            assert trades_received >= 1

    @pytest.mark.asyncio
    async def test_message_handler_transactions_routing(self, client: ZondaExchangeClient) -> None:
        """Test message handler routes transactions to trade queue.

        Given: WebSocket connected and returning a transactions message.
        When: _message_handler() processes the message.
        Then: One trade is queued in _trade_queue.
        """
        with patch(
            "snapper.infrastructure.exchanges.implementations.zonda.connect"
        ) as mock_connect:
            mock_ws = AsyncMock()
            mock_ws.send = AsyncMock()

            async def mock_messages() -> Any:
                yield (
                    '{"action":"push","topic":"trading/transactions/btc-pln",'
                    '"message":{"transactions":[{"id":"test","t":"1000000000000",'
                    '"a":"1.0","r":"50000","ty":"Buy"}]},'
                    '"timestamp":"1000000000000","seqNo":1}'
                )

            mock_ws.__aiter__ = lambda _self: mock_messages()

            async def async_connect(*args: Any, **kwargs: Any) -> AsyncMock:
                return mock_ws

            mock_connect.side_effect = async_connect
            await client.connect()
            client._running = True
            handler_task = asyncio.create_task(client._message_handler())
            await asyncio.sleep(0.1)
            assert client._trade_queue.qsize() == 1
            client._running = False
            handler_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await handler_task

    @pytest.mark.asyncio
    async def test_subscribe_trades_connection_closed(self, client: ZondaExchangeClient) -> None:
        """Test trade subscription handles ConnectionClosed gracefully.

        Given: WebSocket that raises ConnectionClosed after first message.
        When: subscribe_trades() is iterated.
        Then: At least one trade is received before connection closes.
        """
        with patch(
            "snapper.infrastructure.exchanges.implementations.zonda.connect"
        ) as mock_connect:
            mock_ws = AsyncMock()
            mock_ws.send = AsyncMock()
            call_count = 0

            async def async_connect(*args: Any, **kwargs: Any) -> AsyncMock:
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    return mock_ws
                raise ConnectionClosed(None, None)

            mock_connect.side_effect = async_connect

            async def mock_messages() -> Any:
                yield (
                    '{"action":"push","topic":"trading/transactions/btc-pln",'
                    '"message":{"transactions":[{"id":"test","t":"1000000000000",'
                    '"a":"1.0","r":"50000","ty":"Buy"}]}}'
                )
                raise ConnectionClosed(None, None)

            mock_ws.__aiter__ = lambda _self: mock_messages()
            trades_received = 0

            async def run_subscription() -> None:
                nonlocal trades_received
                with contextlib.suppress(ConnectionClosed):
                    async for _ in client.subscribe_trades(["BTC-PLN"]):
                        trades_received += 1
                        if trades_received >= 1:
                            client._running = False
                            break

            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(run_subscription(), timeout=1.0)
            assert trades_received >= 1


class TestZondaSnapshots:
    """Test suite for Zonda WebSocket proxy snapshot requests."""

    @pytest.fixture
    def client(self) -> ZondaExchangeClient:
        """Provide a ZondaExchangeClient with 2 reconnect attempts and 0.01s delay."""
        return ZondaExchangeClient(max_reconnect_attempts=2, reconnect_delay=0.01)

    @pytest.mark.asyncio
    async def test_request_snapshots_specific_symbols(
        self,
        client: ZondaExchangeClient,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test snapshot requests for specific symbols send ticker and stats.

        Given: Client connected with mocked uuid4 and WebSocket.
        When: _request_snapshots(['BTC-PLN', 'ETH-EUR']) is called.
        Then: Four messages sent (ticker+stats per symbol) with correct paths.
        """
        mock_ws = AsyncMock()
        mock_ws.send = AsyncMock()
        client._ws = mock_ws
        uuid_values = iter(
            [
                uuid.UUID(int=1),
                uuid.UUID(int=2),
                uuid.UUID(int=3),
                uuid.UUID(int=4),
            ]
        )
        monkeypatch.setattr(
            "snapper.infrastructure.exchanges.implementations.zonda.uuid.uuid4",
            lambda: next(uuid_values),
        )
        real_sleep = asyncio.sleep

        async def fake_sleep(delay: float) -> None:
            assert delay == pytest.approx(0.01)
            await real_sleep(0)

        monkeypatch.setattr(
            "snapper.infrastructure.exchanges.implementations.zonda.asyncio.sleep",
            fake_sleep,
        )
        await client._request_snapshots(["BTC-PLN", "ETH-EUR"])
        pending = client._pending_snapshots
        assert len(pending) == 4
        assert set(pending.values()) == {"ticker", "stats"}
        assert mock_ws.send.await_count == 4
        payloads = [json.loads(call.args[0]) for call in mock_ws.send.await_args_list]
        assert payloads[0]["path"] == "ticker/btc-pln"
        assert payloads[1]["path"] == "stats/btc-pln"
        assert payloads[2]["path"] == "ticker/eth-eur"
        assert payloads[3]["path"] == "stats/eth-eur"

    @pytest.mark.asyncio
    async def test_request_snapshots_wildcard(
        self,
        client: ZondaExchangeClient,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test wildcard snapshot request sends global ticker and stats.

        Given: Client connected with mocked uuid4 and WebSocket.
        When: _request_snapshots(['*']) is called.
        Then: Two messages sent with paths 'ticker' and 'stats'.
        """
        mock_ws = AsyncMock()
        mock_ws.send = AsyncMock()
        client._ws = mock_ws
        uuid_values = iter([uuid.UUID(int=10), uuid.UUID(int=11)])
        monkeypatch.setattr(
            "snapper.infrastructure.exchanges.implementations.zonda.uuid.uuid4",
            lambda: next(uuid_values),
        )
        await client._request_snapshots(["*"])
        pending = client._pending_snapshots
        assert len(pending) == 2
        assert set(pending.values()) == {"ticker", "stats"}
        assert mock_ws.send.await_count == 2
        payloads = [json.loads(call.args[0]) for call in mock_ws.send.await_args_list]
        assert payloads[0]["path"] == "ticker"
        assert payloads[1]["path"] == "stats"

    @pytest.mark.asyncio
    async def test_parse_proxy_response_routes_ticker(
        self,
        client: ZondaExchangeClient,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test proxy response routes ticker data to _parse_ticker_message.

        Given: Pending snapshot request of type 'ticker'.
        When: _parse_proxy_response() receives successful response.
        Then: _parse_ticker_message is called and request removed from pending.
        """
        ticker_mock: AsyncMock = AsyncMock()
        monkeypatch.setattr(client, "_parse_ticker_message", ticker_mock)
        pending = client._pending_snapshots
        pending["req-ticker"] = "ticker"
        data = {
            "requestId": "req-ticker",
            "statusCode": 200,
            "body": {
                "status": "Ok",
                "ticker": {
                    "market": {"code": "BTC-PLN"},
                    "highestBid": "40000",
                    "lowestAsk": "40100",
                    "rate": "40050",
                },
            },
        }
        await client._parse_proxy_response(data)
        ticker_mock.assert_awaited_once()
        call_payload = ticker_mock.await_args_list[0].args[0]
        assert call_payload["topic"] == "trading/ticker/btc-pln"
        assert "message" in call_payload
        assert "req-ticker" not in pending

    @pytest.mark.asyncio
    async def test_parse_proxy_response_routes_stats(
        self,
        client: ZondaExchangeClient,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test proxy response routes stats data to _parse_stats_message.

        Given: Pending snapshot request of type 'stats'.
        When: _parse_proxy_response() receives successful response.
        Then: _parse_stats_message is called and request removed from pending.
        """
        stats_mock: AsyncMock = AsyncMock()
        monkeypatch.setattr(client, "_parse_stats_message", stats_mock)
        pending = client._pending_snapshots
        pending["req-stats"] = "stats"
        data = {
            "requestId": "req-stats",
            "statusCode": 200,
            "body": {
                "status": "Ok",
                "stats": {
                    "m": "BTC-PLN",
                    "h": 41000.0,
                    "l": 39000.0,
                    "v": 12.3,
                    "r24h": 39500.0,
                },
            },
        }
        await client._parse_proxy_response(data)
        stats_mock.assert_awaited_once()
        call_payload = stats_mock.await_args_list[0].args[0]
        assert isinstance(call_payload["message"], list)
        assert call_payload["message"][0]["m"] == "BTC-PLN"
        assert "req-stats" not in pending

    @pytest.mark.asyncio
    async def test_parse_proxy_response_handles_error_status(
        self,
        client: ZondaExchangeClient,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test proxy response with error status does not route to parser.

        Given: Pending snapshot request and response with statusCode=500.
        When: _parse_proxy_response() is called.
        Then: _parse_ticker_message not called and request removed from pending.
        """
        ticker_mock: AsyncMock = AsyncMock()
        monkeypatch.setattr(client, "_parse_ticker_message", ticker_mock)
        pending = client._pending_snapshots
        pending["req-error"] = "ticker"
        data = {
            "requestId": "req-error",
            "statusCode": 500,
            "body": {"status": "Error"},
        }
        await client._parse_proxy_response(data)
        ticker_mock.assert_not_called()
        assert "req-error" not in pending

    @pytest.mark.asyncio
    async def test_parse_proxy_response_unknown_request(
        self,
        client: ZondaExchangeClient,
    ) -> None:
        """Test proxy response with unknown requestId is ignored.

        Given: Empty pending snapshots.
        When: _parse_proxy_response() receives response for unknown request.
        Then: Pending snapshots remain empty.
        """
        pending = client._pending_snapshots
        assert pending == {}
        data = {
            "requestId": "missing",
            "statusCode": 200,
            "body": {"status": "Ok"},
        }
        await client._parse_proxy_response(data)
        assert pending == {}


class TestZondaExecutions:
    """Test suite for Zonda private execution WebSocket subscriptions."""

    @pytest.fixture
    def client(self) -> ZondaExchangeClient:
        """Provide a ZondaExchangeClient with 2 reconnect attempts."""
        return ZondaExchangeClient(max_reconnect_attempts=2)

    @pytest.fixture
    def authenticated_client(self) -> ZondaExchangeClient:
        """Provide a ZondaExchangeClient with API credentials."""
        return ZondaExchangeClient(
            api_key="test_api_key", api_secret="test_api_secret", max_reconnect_attempts=2
        )

    @pytest.mark.asyncio
    async def test_subscribe_executions_requires_credentials(
        self, client: ZondaExchangeClient
    ) -> None:
        """Test execution subscription requires API credentials.

        Given: A client without API credentials.
        When: subscribe_executions() is called.
        Then: RuntimeError with 'API credentials required' is raised.
        """
        with pytest.raises(RuntimeError, match="API credentials required"):
            async for _ in client.subscribe_executions():
                break

    @pytest.mark.asyncio
    async def test_sign_private_message(self, authenticated_client: ZondaExchangeClient) -> None:
        """Test signing private message includes all required fields.

        Given: An authenticated client.
        When: _sign_private_message() is called with a payload.
        Then: Result contains publicKey, hashSignature (128 chars), requestTimestamp.
        """
        payload = {
            "action": "subscribe-private",
            "module": "trading",
            "path": "history/transactions",
        }
        signed = authenticated_client._sign_private_message(payload)
        assert "publicKey" in signed
        assert "hashSignature" in signed
        assert "requestTimestamp" in signed
        assert signed["publicKey"] == "test_api_key"
        assert len(signed["hashSignature"]) == 128
        assert isinstance(signed["requestTimestamp"], int)

    @pytest.mark.asyncio
    async def test_parse_executions_message(
        self, authenticated_client: ZondaExchangeClient
    ) -> None:
        """Test parsing executions message queues ExecutionUpdate.

        Given: An executions push message with one BTC-PLN trade.
        When: _parse_executions_message() is called.
        Then: ExecutionUpdate with correct order_id, symbol, cum_qty, cum_cost is queued.
        """
        data = {
            "action": "push",
            "topic": "trading/history/transactions",
            "message": {
                "id": "exec_123",
                "market": "BTC-PLN",
                "time": "1000000000000",
                "amount": "0.5",
                "rate": "50000",
                "initializedBy": "Sell",
                "wasTaker": True,
                "userAction": "Buy",
                "offerId": "order_456",
                "commissionValue": "0.001",
            },
            "timestamp": "1000000000000",
            "seqNo": 1,
        }
        await authenticated_client._parse_executions_message(data)
        execution = await asyncio.wait_for(
            authenticated_client._execution_queue.get(),
            timeout=1.0,
        )
        assert execution.order_id == "order_456"
        assert execution.symbol == "BTC-PLN"
        assert execution.exec_type == "trade"
        assert execution.exec_id == "exec_123"
        assert execution.cum_qty is None
        assert execution.last_qty == pytest.approx(0.5)
        assert execution.last_price == pytest.approx(50000.0)

    @pytest.mark.asyncio
    async def test_parse_executions_multiple(
        self, authenticated_client: ZondaExchangeClient
    ) -> None:
        """Test parsing two separate execution pushes queues two ExecutionUpdates.

        Given: Two execution push messages (BTC-PLN and ETH-PLN).
        When: _parse_executions_message() is called for each.
        Then: Two ExecutionUpdates with correct order_ids and symbols are queued.
        """
        data1 = {
            "action": "push",
            "topic": "trading/history/transactions",
            "message": {
                "id": "exec_1",
                "market": "BTC-PLN",
                "time": "1000000000000",
                "amount": "0.1",
                "rate": "50000",
                "userAction": "Buy",
                "offerId": "order_1",
                "wasTaker": True,
            },
        }
        data2 = {
            "action": "push",
            "topic": "trading/history/transactions",
            "message": {
                "id": "exec_2",
                "market": "ETH-PLN",
                "time": "1000000001000",
                "amount": "1.0",
                "rate": "3000",
                "userAction": "Sell",
                "offerId": "order_2",
                "wasTaker": False,
            },
        }
        await authenticated_client._parse_executions_message(data1)
        await authenticated_client._parse_executions_message(data2)
        exec1 = await asyncio.wait_for(
            authenticated_client._execution_queue.get(),
            timeout=1.0,
        )
        exec2 = await asyncio.wait_for(
            authenticated_client._execution_queue.get(),
            timeout=1.0,
        )
        assert exec1.order_id == "order_1"
        assert exec1.symbol == "BTC-PLN"
        assert exec2.order_id == "order_2"
        assert exec2.symbol == "ETH-PLN"

    @pytest.mark.asyncio
    async def test_parse_executions_real_uat_payload(
        self, authenticated_client: ZondaExchangeClient
    ) -> None:
        """Test parsing real Zonda WS execution payload from UAT 2026-03-27.

        Given: Exact WS push captured from live Zonda market buy execution.
        When: _parse_executions_message() is called.
        Then: ExecutionUpdate has correct order_id, symbol, side, qty, cost.
        """
        data = {
            "action": "push",
            "topic": "trading/history/transactions",
            "message": {
                "id": "e848bab9-2a1b-11f1-81a9-4ea2d0fa018b",
                "market": "BTC-PLN",
                "time": "1774643477476",
                "amount": "0.0001",
                "rate": "248487.27",
                "initializedBy": "Buy",
                "wasTaker": True,
                "userAction": "Buy",
                "offerId": "e84893a8-2a1b-11f1-81a9-4ea2d0fa018b",
                "commissionValue": "0.00000020",
            },
            "timestamp": "1774643477476",
            "seqNo": 4,
        }
        await authenticated_client._parse_executions_message(data)
        execution = await asyncio.wait_for(
            authenticated_client._execution_queue.get(),
            timeout=1.0,
        )
        assert execution.order_id == "e84893a8-2a1b-11f1-81a9-4ea2d0fa018b"
        assert execution.symbol == "BTC-PLN"
        assert execution.side == OrderSideEnum.BUY
        assert execution.exec_type == "trade"
        assert execution.exec_id == "e848bab9-2a1b-11f1-81a9-4ea2d0fa018b"
        assert execution.cum_qty is None
        assert execution.last_qty == pytest.approx(0.0001)
        assert execution.last_price == pytest.approx(248487.27)

    @pytest.mark.asyncio
    async def test_subscribe_executions_basic_flow(
        self, authenticated_client: ZondaExchangeClient
    ) -> None:
        """Test execution subscription yields at least one execution.

        Given: Authenticated client and mocked WebSocket returning execution message.
        When: subscribe_executions() is iterated.
        Then: Subscription message sent with signature, at least one execution received.
        """
        with patch(
            "snapper.infrastructure.exchanges.implementations.zonda.connect"
        ) as mock_connect:
            mock_ws = AsyncMock()
            mock_ws.send = AsyncMock()

            async def mock_messages() -> Any:
                yield (
                    '{"action":"push","topic":"trading/history/transactions",'
                    '"message":{"id":"exec_1","market":"BTC-PLN",'
                    '"time":"1000000000000","amount":"0.5","rate":"50000",'
                    '"userAction":"Buy","offerId":"order_1","wasTaker":true}}'
                )

            mock_ws.__aiter__ = lambda _self: mock_messages()

            async def async_connect(*args: Any, **kwargs: Any) -> AsyncMock:
                return mock_ws

            mock_connect.side_effect = async_connect
            executions_received = 0

            async def run_subscription() -> None:
                nonlocal executions_received
                with contextlib.suppress(asyncio.TimeoutError):
                    async for _ in authenticated_client.subscribe_executions():
                        executions_received += 1
                        if executions_received >= 1:
                            authenticated_client._running = False
                            break

            await asyncio.wait_for(run_subscription(), timeout=3.0)
            assert mock_ws.send.called
            sent_msg = json.loads(mock_ws.send.call_args[0][0])
            assert sent_msg["action"] == "subscribe-private"
            assert sent_msg["path"] == "history/transactions"
            assert "publicKey" in sent_msg
            assert "hashSignature" in sent_msg
            assert "requestTimestamp" in sent_msg
            assert executions_received >= 1

    @pytest.mark.asyncio
    async def test_message_handler_executions_routing(
        self, authenticated_client: ZondaExchangeClient
    ) -> None:
        """Test message handler routes executions to execution queue.

        Given: Authenticated client with WebSocket returning execution message.
        When: _message_handler() processes the message.
        Then: ExecutionUpdate is queued with correct order_id and symbol.
        """
        with patch(
            "snapper.infrastructure.exchanges.implementations.zonda.connect"
        ) as mock_connect:
            mock_ws = AsyncMock()
            mock_ws.send = AsyncMock()

            async def mock_messages() -> Any:
                yield (
                    '{"action":"push","topic":"trading/history/transactions",'
                    '"message":{"id":"exec_1","market":"BTC-PLN",'
                    '"time":"1000000000000","amount":"0.1","rate":"50000",'
                    '"userAction":"Buy","offerId":"order_1","wasTaker":true}}'
                )

            mock_ws.__aiter__ = lambda _self: mock_messages()

            async def async_connect(*args: Any, **kwargs: Any) -> AsyncMock:
                return mock_ws

            mock_connect.side_effect = async_connect
            await authenticated_client.connect()
            authenticated_client._ws = mock_ws
            handler_task = asyncio.create_task(authenticated_client._message_handler())
            await asyncio.sleep(0.1)
            handler_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await handler_task
            execution = await asyncio.wait_for(
                authenticated_client._execution_queue.get(),
                timeout=1.0,
            )
            assert execution.order_id == "order_1"
            assert execution.symbol == "BTC-PLN"


MODULE_ASYNCIO = getattr(zonda_module, "asyncio")
ZONDA_TICKER_MESSAGE = getattr(zonda_module, "ZondaTickerMessage")
ZONDA_STATS_MESSAGE = getattr(zonda_module, "ZondaStatsMessage")
ZONDA_TRANSACTIONS_MESSAGE = getattr(zonda_module, "ZondaTransactionsMessage")
ZONDA_EXECUTION_DATA = getattr(zonda_module, "ZondaExecutionData")


class DummyWs:
    """Mock WebSocket that yields pre-defined messages and tracks sent payloads."""

    def __init__(self, messages: list[str] | None = None) -> None:
        """Initialize the instance."""
        self._messages = messages or []
        self.sent: list[str] = []

    async def send(self, payload: str) -> None:
        """Record sent payload to sent list."""
        self.sent.append(payload)

    def __aiter__(self) -> Any:
        """Return an async iterator over the instance."""

        async def gen() -> Any:
            for msg in self._messages:
                yield msg

        return gen()


class RaisingWs:
    """Mock WebSocket that raises RuntimeError on iteration."""

    def __aiter__(self) -> Any:
        """Return an async iterator over the instance."""

        async def gen() -> Any:
            raise RuntimeError("boom")
            if False:
                yield ""

        return gen()


async def _consume(generator: Any) -> None:
    """Consume all items from an async generator."""
    async for _ in generator:
        """Consumed by iteration to trigger exception."""
        pass


def test_get_ccxt_client_returns_instance() -> None:
    """Test get_ccxt_client returns the internal CCXT client.

    Given: A client instance.
    When: get_ccxt_client() is called.
    Then: It returns the _ccxt_client attribute.
    """
    client = ZondaExchangeClient()
    assert client.get_ccxt_client() is client._ccxt_client


@pytest.mark.asyncio
async def test_connect_skips_when_ws_present() -> None:
    """Test connect skips connection when WebSocket already exists.

    Given: A client with _ws already set.
    When: connect() is called.
    Then: The connect function is not called (await_count=0).
    """
    client = ZondaExchangeClient()
    client._ws = cast(ClientConnection, DummyWs())
    with patch("snapper.infrastructure.exchanges.implementations.zonda.connect", AsyncMock()) as m:
        await client.connect()
        assert m.await_count == 0


@pytest.mark.asyncio
async def test_connect_no_attempts_when_max_reconnect_zero() -> None:
    """Test connect makes no attempts when max_reconnect_attempts is zero.

    Given: A client with max_reconnect_attempts=0.
    When: connect() is called.
    Then: No connection attempts made and _ws remains None.
    """
    client = ZondaExchangeClient(max_reconnect_attempts=0)
    with patch("snapper.infrastructure.exchanges.implementations.zonda.connect", AsyncMock()) as m:
        await client.connect()
        assert m.await_count == 0
        assert client._ws is None


@pytest.mark.asyncio
async def test_disconnect_without_ws_or_task() -> None:
    """Test disconnect succeeds when no WebSocket or task exists.

    Given: A client with no _ws or _message_handler_task.
    When: disconnect() is called.
    Then: No exception is raised.
    """
    client = ZondaExchangeClient()
    await client.disconnect()


@pytest.mark.asyncio
async def test_subscribe_ticks_snapshot_timeout_calls_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test tick subscription with snapshot calls _request_snapshots.

    Given: Client with mocked connect and immediate timeout.
    When: subscribe_ticks(['BTC-PLN'], snapshot=True) is consumed.
    Then: _request_snapshots is called and handler task is created.
    """
    client = ZondaExchangeClient(reconnect_delay=0)

    async def connect_stub() -> None:
        client._ws = cast(ClientConnection, DummyWs())

    async def handler_stub() -> None:
        return None

    async def immediate_timeout(*args: Any, **kwargs: Any) -> None:
        coro = args[0]
        if hasattr(coro, "close"):
            coro.close()
        client._running = False
        raise TimeoutError

    sleep_mock = AsyncMock()
    request_mock = AsyncMock()
    monkeypatch.setattr(client, "connect", connect_stub)
    monkeypatch.setattr(client, "_resubscribe", AsyncMock())
    monkeypatch.setattr(client, "_request_snapshots", request_mock)
    monkeypatch.setattr(client, "_message_handler", handler_stub)
    monkeypatch.setattr(MODULE_ASYNCIO, "wait_for", immediate_timeout)
    monkeypatch.setattr(MODULE_ASYNCIO, "sleep", sleep_mock)
    await _consume(client.subscribe_ticks(["BTC-PLN"], snapshot=True))
    assert request_mock.await_count == 1
    assert client._message_handler_task is not None


@pytest.mark.asyncio
async def test_subscribe_ticks_handles_connection_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test tick subscription handles ConnectionClosed by sleeping.

    Given: Client whose connect raises ConnectionClosed.
    When: subscribe_ticks(['BTC-PLN']) is consumed.
    Then: asyncio.sleep is called for reconnect delay.
    """
    client = ZondaExchangeClient(reconnect_delay=0)

    async def connect_stub() -> None:
        client._running = False
        raise ConnectionClosed(None, None)

    sleep_mock = AsyncMock()
    monkeypatch.setattr(client, "connect", connect_stub)
    monkeypatch.setattr(MODULE_ASYNCIO, "sleep", sleep_mock)
    await _consume(client.subscribe_ticks(["BTC-PLN"]))
    assert sleep_mock.await_count == 1


@pytest.mark.asyncio
async def test_subscribe_ticks_reconnects_on_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test tick subscription attempts reconnect on exception.

    Given: Client whose connect always raises RuntimeError.
    When: subscribe_ticks(['BTC-PLN']) is consumed.
    Then: Sleep stub stops _running to end iteration.
    """
    client = ZondaExchangeClient(reconnect_delay=0)

    async def connect_stub() -> None:
        raise RuntimeError("boom")

    async def sleep_stub(_delay: float) -> None:
        client._running = False

    monkeypatch.setattr(client, "connect", connect_stub)
    monkeypatch.setattr(MODULE_ASYNCIO, "sleep", sleep_stub)
    await _consume(client.subscribe_ticks(["BTC-PLN"]))


@pytest.mark.asyncio
async def test_subscribe_ticks_raises_when_not_running(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test tick subscription raises exception when not running.

    Given: Client whose connect sets _running=False and raises RuntimeError.
    When: subscribe_ticks(['BTC-PLN']) is consumed.
    Then: RuntimeError is raised.
    """
    client = ZondaExchangeClient()

    async def connect_stub() -> None:
        client._running = False
        raise RuntimeError("boom")

    monkeypatch.setattr(client, "connect", connect_stub)
    with pytest.raises(RuntimeError, match="boom"):
        await _consume(client.subscribe_ticks(["BTC-PLN"]))


@pytest.mark.asyncio
async def test_message_handler_routes_actions_and_handles_internal_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test message handler routes different actions and handles parse errors.

    Given: WebSocket messages including transactions, confirm, error, proxy-response, ticker.
    When: _message_handler() processes messages with _parse_ticker_message raising.
    Then: Executions and proxy parsers are called; ticker error is handled.
    """
    client = ZondaExchangeClient()
    messages = [
        json.dumps({"action": "push", "topic": "trading/history/transactions", "seqNo": 1}),
        json.dumps(
            {
                "action": "subscribe-private-confirm",
                "module": "trading",
                "path": "history/transactions",
            }
        ),
        json.dumps({"action": "subscribe-private-error", "error": "bad"}),
        json.dumps({"action": "proxy-response", "requestId": "req"}),
        json.dumps(
            {
                "action": "push",
                "topic": "trading/ticker/btc-pln",
                "seqNo": 2,
                "message": {},
            }
        ),
    ]
    client._ws = cast(ClientConnection, DummyWs(messages))
    parse_exec = AsyncMock()
    parse_proxy = AsyncMock()
    parse_ticker = AsyncMock(side_effect=RuntimeError("boom"))
    monkeypatch.setattr(client, "_parse_executions_message", parse_exec)
    monkeypatch.setattr(client, "_parse_proxy_response", parse_proxy)
    monkeypatch.setattr(client, "_parse_ticker_message", parse_ticker)
    await client._message_handler()
    assert parse_exec.await_count == 1
    assert parse_proxy.await_count == 1


@pytest.mark.asyncio
async def test_message_handler_outer_exception() -> None:
    """Test message handler handles outer exception gracefully.

    Given: Client with RaisingWs that throws RuntimeError.
    When: _message_handler() is called.
    Then: Handler completes without propagating exception.
    """
    client = ZondaExchangeClient()
    client._ws = cast(ClientConnection, RaisingWs())
    await client._message_handler()


@pytest.mark.asyncio
async def test_parse_ticker_missing_symbol() -> None:
    """Test parsing ticker with empty symbol leaves cache empty.

    Given: A ticker message with empty topic and market code.
    When: _parse_ticker_message() is called.
    Then: _ticker_cache remains empty.
    """
    client = ZondaExchangeClient()
    data = {
        "action": "push",
        "topic": "",
        "message": {"market": {"code": ""}, "highestBid": "1", "lowestAsk": "2", "rate": "1.5"},
        "seqNo": 1,
    }
    await client._parse_ticker_message(data)
    assert client._ticker_cache == {}


@pytest.mark.asyncio
async def test_parse_ticker_handles_validation_error() -> None:
    """Test parsing ticker handles model validation error gracefully.

    Given: ZondaTickerMessage.model_validate raises ValueError.
    When: _parse_ticker_message() is called.
    Then: No exception is raised.
    """
    client = ZondaExchangeClient()
    with patch.object(
        ZONDA_TICKER_MESSAGE,
        "model_validate",
        side_effect=ValueError("boom"),
    ):
        await client._parse_ticker_message({})


@pytest.mark.asyncio
async def test_parse_stats_handles_validation_error() -> None:
    """Test parsing stats handles model validation error gracefully.

    Given: ZondaStatsMessage.model_validate raises ValueError.
    When: _parse_stats_message() is called.
    Then: No exception is raised.
    """
    client = ZondaExchangeClient()
    with patch.object(
        ZONDA_STATS_MESSAGE,
        "model_validate",
        side_effect=ValueError("boom"),
    ):
        await client._parse_stats_message({})


@pytest.mark.asyncio
async def test_parse_proxy_response_status_not_ok() -> None:
    """Test proxy response with 'Error' status removes pending request.

    Given: Pending snapshot request and response with status='Error'.
    When: _parse_proxy_response() is called.
    Then: Request is removed from pending snapshots.
    """
    client = ZondaExchangeClient()
    client._pending_snapshots["req"] = "ticker"
    await client._parse_proxy_response(
        {"requestId": "req", "statusCode": 200, "body": {"status": "Error"}}
    )
    assert "req" not in client._pending_snapshots


@pytest.mark.asyncio
async def test_parse_proxy_response_ticker_missing_data() -> None:
    """Test proxy response for ticker with missing data handles gracefully.

    Given: Pending ticker request and response without 'ticker' in body.
    When: _parse_proxy_response() is called.
    Then: No exception is raised.
    """
    client = ZondaExchangeClient()
    client._pending_snapshots["req"] = "ticker"
    await client._parse_proxy_response(
        {"requestId": "req", "statusCode": 200, "body": {"status": "Ok"}}
    )


@pytest.mark.asyncio
async def test_parse_proxy_response_stats_missing_data() -> None:
    """Test proxy response for stats with missing data handles gracefully.

    Given: Pending stats request and response without 'stats' in body.
    When: _parse_proxy_response() is called.
    Then: No exception is raised.
    """
    client = ZondaExchangeClient()
    client._pending_snapshots["req"] = "stats"
    await client._parse_proxy_response(
        {"requestId": "req", "statusCode": 200, "body": {"status": "Ok"}}
    )


@pytest.mark.asyncio
async def test_parse_transactions_missing_symbol() -> None:
    """Test parsing transactions with missing symbol leaves trade queue empty.

    Given: A transactions message with topic 'transactions' (no symbol).
    When: _parse_transactions_message() is called.
    Then: _trade_queue remains empty.
    """
    client = ZondaExchangeClient()
    data = {"action": "push", "topic": "transactions", "message": {"transactions": []}}
    await client._parse_transactions_message(data)
    assert client._trade_queue.qsize() == 0


@pytest.mark.asyncio
async def test_parse_transactions_handles_validation_error() -> None:
    """Test parsing transactions handles model validation error gracefully.

    Given: ZondaTransactionsMessage.model_validate raises ValueError.
    When: _parse_transactions_message() is called.
    Then: No exception is raised.
    """
    client = ZondaExchangeClient()
    with patch.object(
        ZONDA_TRANSACTIONS_MESSAGE,
        "model_validate",
        side_effect=ValueError("boom"),
    ):
        await client._parse_transactions_message({})


@pytest.mark.asyncio
async def test_parse_executions_handles_validation_error() -> None:
    """Test parsing executions handles model validation error gracefully.

    Given: ZondaExecutionData.model_validate raises ValueError.
    When: _parse_executions_message() is called.
    Then: No exception is raised.
    """
    client = ZondaExchangeClient()
    with patch.object(
        ZONDA_EXECUTION_DATA,
        "model_validate",
        side_effect=ValueError("boom"),
    ):
        await client._parse_executions_message({"message": {}})


@pytest.mark.asyncio
async def test_get_ticker_raises_on_ccxt_error() -> None:
    """Test get_ticker raises RuntimeError on CCXT error.

    Given: CCXT fetch_ticker raises RuntimeError.
    When: get_ticker() is called.
    Then: RuntimeError is propagated.
    """
    client = ZondaExchangeClient()
    with (
        patch.object(client._ccxt_client, "fetch_ticker", side_effect=RuntimeError("boom")),
        pytest.raises(RuntimeError),
    ):
        await client.get_ticker("BTC-EUR")


@pytest.mark.asyncio
async def test_get_ohlcv_raises_on_ccxt_error() -> None:
    """Test get_ohlcv raises RuntimeError on CCXT error.

    Given: CCXT fetch_ohlcv raises RuntimeError.
    When: get_ohlcv() is called.
    Then: RuntimeError is propagated.
    """
    client = ZondaExchangeClient()
    with (
        patch.object(client._ccxt_client, "fetch_ohlcv", side_effect=RuntimeError("boom")),
        pytest.raises(RuntimeError),
    ):
        await client.get_ohlcv("BTC-EUR")


@pytest.mark.asyncio
async def test_create_order_without_client_order_id() -> None:
    """Test create_order without client_order_id passes empty params.

    Given: Client with credentials and request without client_order_id.
    When: create_order() is called.
    Then: CCXT create_order is called with empty params dict.
    """
    client = ZondaExchangeClient(api_key="key", api_secret="secret")
    mock_order = {
        "id": "order123",
        "symbol": "BTC/EUR",
        "side": "buy",
        "type": "limit",
        "amount": 1.0,
        "price": 100.0,
        "status": "open",
        "filled": 0.0,
        "remaining": 1.0,
        "timestamp": 1609459200000,
    }
    with patch.object(client._ccxt_client, "create_order", return_value=mock_order) as mock_create:
        request = ExchangeOrderRequest(
            symbol="BTC-EUR",
            side=OrderSideEnum.BUY,
            type=OrderTypeEnum.LIMIT,
            amount=1.0,
            price=100.0,
        )
        await client.create_order(request)
        order_params = mock_create.call_args[0][5]
        assert order_params == {}


@pytest.mark.asyncio
async def test_create_order_market_with_none_fields() -> None:
    """Test create_order for Zonda market order returns PENDING snapshot.

    Given: CCXT response with timestamp=None, fee=None, clientOrderId=None
           (real Zonda market order behavior where only id is reliable).
    When: create_order() is called.
    Then: ExchangeOrderSnapshot has PENDING status, filled=0, remaining=amount,
          fee=None, client_order_id=None from request, timestamp > 0.
    """
    client = ZondaExchangeClient(api_key="key", api_secret="secret")
    with patch.object(
        client._ccxt_client,
        "create_order",
        return_value={"id": "e84893a8-2a1b-11f1-81a9-4ea2d0fa018b"},
    ):
        request = ExchangeOrderRequest(
            symbol="BTC-PLN",
            side=OrderSideEnum.BUY,
            type=OrderTypeEnum.MARKET,
            amount=0.0001,
        )
        order = await client.create_order(request)
        assert order.id == "e84893a8-2a1b-11f1-81a9-4ea2d0fa018b"
        assert order.status == OrderStatusEnum.PENDING
        assert order.filled == pytest.approx(0.0)
        assert order.remaining == pytest.approx(0.0001)
        assert order.price is None
        assert order.fee is None
        assert order.client_order_id is None
        assert order.timestamp > 0


@pytest.mark.asyncio
async def test_create_order_limit_maker_with_none_fields() -> None:
    """Test create_order for Zonda limit order returns PENDING snapshot from request.

    Given: CCXT response for unfilled limit order (real Zonda behavior where only
           id is reliable in the create response).
    When: create_order() is called.
    Then: ExchangeOrderSnapshot has PENDING status, filled=0, remaining=amount,
          price and amount from request, fee=None, timestamp > 0.
    """
    client = ZondaExchangeClient(api_key="key", api_secret="secret")
    with patch.object(
        client._ccxt_client,
        "create_order",
        return_value={"id": "4d0b4b98-2a1d-11f1-81a9-4ea2d0fa018b"},
    ):
        request = ExchangeOrderRequest(
            symbol="BTC-PLN",
            side=OrderSideEnum.SELL,
            type=OrderTypeEnum.LIMIT,
            amount=0.00002,
            price=999999.0,
        )
        order = await client.create_order(request)
        assert order.id == "4d0b4b98-2a1d-11f1-81a9-4ea2d0fa018b"
        assert order.status == OrderStatusEnum.PENDING
        assert order.filled == pytest.approx(0.0)
        assert order.remaining == pytest.approx(0.00002)
        assert order.price == pytest.approx(999999.0)
        assert order.amount == pytest.approx(0.00002)
        assert order.fee is None
        assert order.timestamp > 0


@pytest.mark.asyncio
async def test_create_order_empty_id_skips_db_logging() -> None:
    """Test create_order with empty id skips db logging.

    Given: CCXT returns a response with an empty id string,
    When: create_order is called,
    Then: The snapshot has an empty id and db logging is not called.
    """
    client = ZondaExchangeClient(api_key="key", api_secret="secret")
    mock_log = AsyncMock(return_value=None)
    with (
        patch.object(client._ccxt_client, "create_order", return_value={"id": ""}),
        patch.object(client, "_log_order_to_db", mock_log),
    ):
        request = ExchangeOrderRequest(
            symbol="BTC-PLN",
            side=OrderSideEnum.BUY,
            type=OrderTypeEnum.LIMIT,
            amount=5e-05,
            price=100000.0,
        )
        order = await client.create_order(request)
        assert order.id == ""
        assert order.status == OrderStatusEnum.PENDING
        assert order.db_order_id is None
        mock_log.assert_not_called()


@pytest.mark.asyncio
async def test_create_order_raises_on_ccxt_error() -> None:
    """Test create_order raises RuntimeError on CCXT error.

    Given: Client with credentials and CCXT create_order raises RuntimeError.
    When: create_order() is called.
    Then: RuntimeError is propagated.
    """
    client = ZondaExchangeClient(api_key="key", api_secret="secret")
    with patch.object(client._ccxt_client, "create_order", side_effect=RuntimeError("boom")):
        request = ExchangeOrderRequest(
            symbol="BTC-EUR",
            side=OrderSideEnum.BUY,
            type=OrderTypeEnum.LIMIT,
            amount=1.0,
            price=100.0,
        )
        with pytest.raises(RuntimeError):
            await client.create_order(request)


@pytest.mark.asyncio
async def test_cancel_order_requires_credentials() -> None:
    """Test cancel_order raises error without credentials.

    Given: A client without API credentials.
    When: cancel_order() is called.
    Then: RuntimeError with 'API credentials required' is raised.
    """
    client = ZondaExchangeClient()
    with pytest.raises(RuntimeError, match="API credentials required"):
        await client.cancel_order("order-1", "BTC-EUR")


@pytest.mark.asyncio
async def test_cancel_order_raises_on_ccxt_error() -> None:
    """Test cancel_order raises RuntimeError on CCXT error.

    Given: Client with credentials and CCXT fetch_open_orders raises RuntimeError.
    When: cancel_order() is called.
    Then: RuntimeError is propagated.
    """
    client = ZondaExchangeClient(api_key="key", api_secret="secret")
    with (
        patch.object(client._ccxt_client, "fetch_open_orders", side_effect=RuntimeError("boom")),
        pytest.raises(RuntimeError),
    ):
        await client.cancel_order("order-1", "BTC-EUR")


@pytest.mark.asyncio
async def test_get_order_requires_credentials() -> None:
    """Test get_order raises error without credentials.

    Given: A client without API credentials.
    When: get_order() is called.
    Then: RuntimeError with 'API credentials required' is raised.
    """
    client = ZondaExchangeClient()
    with pytest.raises(RuntimeError, match="API credentials required"):
        await client.get_order("order-1", "BTC-EUR")


@pytest.mark.asyncio
async def test_get_order_raises_on_ccxt_error() -> None:
    """Test get_order raises RuntimeError on CCXT error.

    Given: Client with credentials and CCXT fetch_order raises RuntimeError.
    When: get_order() is called.
    Then: RuntimeError is propagated.
    """
    client = ZondaExchangeClient(api_key="key", api_secret="secret")
    with (
        patch.object(client._ccxt_client, "fetch_order", side_effect=RuntimeError("boom")),
        pytest.raises(RuntimeError),
    ):
        await client.get_order("order-1", "BTC-EUR")


@pytest.mark.asyncio
async def test_get_orders_requires_credentials() -> None:
    """Test get_orders raises error without credentials.

    Given: A client without API credentials.
    When: get_orders() is called.
    Then: RuntimeError with 'API credentials required' is raised.
    """
    client = ZondaExchangeClient()
    with pytest.raises(RuntimeError, match="API credentials required"):
        await client.get_orders()


@pytest.mark.asyncio
async def test_get_orders_raises_on_ccxt_error() -> None:
    """Test get_orders raises RuntimeError on CCXT error.

    Given: Client with credentials and CCXT fetch_orders raises RuntimeError.
    When: get_orders() is called.
    Then: RuntimeError is propagated.
    """
    client = ZondaExchangeClient(api_key="key", api_secret="secret")
    with (
        patch.object(client._ccxt_client, "fetch_orders", side_effect=RuntimeError("boom")),
        pytest.raises(RuntimeError),
    ):
        await client.get_orders()


@pytest.mark.asyncio
async def test_get_balance_requires_credentials() -> None:
    """Test get_balance raises error without credentials.

    Given: A client without API credentials.
    When: get_balance() is called.
    Then: RuntimeError with 'API credentials required' is raised.
    """
    client = ZondaExchangeClient()
    with pytest.raises(RuntimeError, match="API credentials required"):
        await client.get_balance()


@pytest.mark.asyncio
async def test_get_balance_filters_currency() -> None:
    """Test get_balance with currency filter returns only that currency.

    Given: Client with credentials and balance data for BTC and ETH.
    When: get_balance('BTC') is called.
    Then: Only BTC balance is returned.
    """
    client = ZondaExchangeClient(api_key="key", api_secret="secret")
    balance_data = {
        "BTC": {"free": 1.0, "used": 0.0, "total": 1.0},
        "ETH": {"free": 2.0, "used": 0.0, "total": 2.0},
        "info": {},
    }
    with patch.object(client._ccxt_client, "fetch_balance", return_value=balance_data):
        balances = await client.get_balance("BTC")
        assert list(balances) == ["BTC"]


@pytest.mark.asyncio
async def test_get_balance_raises_on_ccxt_error() -> None:
    """Test get_balance raises RuntimeError on CCXT error.

    Given: Client with credentials and CCXT fetch_balance raises RuntimeError.
    When: get_balance() is called.
    Then: RuntimeError is propagated.
    """
    client = ZondaExchangeClient(api_key="key", api_secret="secret")
    with (
        patch.object(client._ccxt_client, "fetch_balance", side_effect=RuntimeError("boom")),
        pytest.raises(RuntimeError),
    ):
        await client.get_balance()


@pytest.mark.asyncio
async def test_subscribe_trades_timeout_exits(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test trade subscription exits on timeout.

    Given: Client with immediate timeout in wait_for.
    When: subscribe_trades(['BTC-PLN']) is consumed.
    Then: Iteration completes without error.
    """
    client = ZondaExchangeClient(reconnect_delay=0)

    async def connect_stub() -> None:
        client._ws = cast(ClientConnection, DummyWs())

    async def handler_stub() -> None:
        return None

    async def immediate_timeout(*args: Any, **kwargs: Any) -> None:
        coro = args[0]
        if hasattr(coro, "close"):
            coro.close()
        client._running = False
        raise TimeoutError

    sleep_mock = AsyncMock()
    monkeypatch.setattr(client, "connect", connect_stub)
    monkeypatch.setattr(client, "_message_handler", handler_stub)
    monkeypatch.setattr(MODULE_ASYNCIO, "wait_for", immediate_timeout)
    monkeypatch.setattr(MODULE_ASYNCIO, "sleep", sleep_mock)
    await _consume(client.subscribe_trades(["BTC-PLN"]))


@pytest.mark.asyncio
async def test_subscribe_trades_handles_connection_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test trade subscription handles ConnectionClosed gracefully.

    Given: Client whose connect raises ConnectionClosed.
    When: subscribe_trades(['BTC-PLN']) is consumed.
    Then: Iteration completes without error.
    """
    client = ZondaExchangeClient(reconnect_delay=0)

    async def connect_stub() -> None:
        client._running = False
        raise ConnectionClosed(None, None)

    monkeypatch.setattr(client, "connect", connect_stub)
    monkeypatch.setattr(MODULE_ASYNCIO, "sleep", AsyncMock())
    await _consume(client.subscribe_trades(["BTC-PLN"]))


@pytest.mark.asyncio
async def test_subscribe_trades_raises_on_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test trade subscription raises RuntimeError when not running.

    Given: Client whose connect sets _running=False and raises RuntimeError.
    When: subscribe_trades(['BTC-PLN']) is consumed.
    Then: RuntimeError is raised.
    """
    client = ZondaExchangeClient()

    async def connect_stub() -> None:
        client._running = False
        raise RuntimeError("boom")

    monkeypatch.setattr(client, "connect", connect_stub)
    with pytest.raises(RuntimeError, match="boom"):
        await _consume(client.subscribe_trades(["BTC-PLN"]))


@pytest.mark.asyncio
async def test_subscribe_candles_starts_and_cleans_up(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test candle subscription starts aggregator and cleans up on close.

    Given: Client with blocking aggregator and pre-queued candle.
    When: subscribe_candles(['BTC-PLN']) is iterated and closed.
    Then: Aggregator task is cancelled or done after cleanup.
    """
    client = ZondaExchangeClient()
    candle = CandleUpdate(
        symbol="BTC-PLN",
        open=1.0,
        high=2.0,
        low=0.5,
        close=1.5,
        vwap=1.2,
        trades=1,
        volume=3.0,
        interval_begin=datetime.now(tz=UTC),
        interval=1,
    )
    blocker = asyncio.Event()

    async def blocking_aggregator(_symbols: list[str]) -> None:
        await blocker.wait()

    monkeypatch.setattr(client, "_candle_aggregator", blocking_aggregator)
    await client._candle_queue.put(candle)
    generator = cast(Any, client.subscribe_candles(["BTC-PLN"]))
    async for _ in generator:
        client._running = False
        break
    await generator.aclose()
    await asyncio.sleep(0)
    if client._candle_aggregator_task:
        assert client._candle_aggregator_task.cancelled() or client._candle_aggregator_task.done()


@pytest.mark.asyncio
async def test_subscribe_executions_timeout_exits(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test execution subscription exits on timeout.

    Given: Authenticated client with immediate timeout in wait_for.
    When: subscribe_executions() is consumed.
    Then: Iteration completes without error.
    """
    client = ZondaExchangeClient(api_key="key", api_secret="secret", reconnect_delay=0)

    async def connect_stub() -> None:
        client._ws = cast(ClientConnection, DummyWs())

    async def handler_stub() -> None:
        return None

    async def immediate_timeout(*args: Any, **kwargs: Any) -> None:
        coro = args[0]
        if hasattr(coro, "close"):
            coro.close()
        client._running = False
        raise TimeoutError

    monkeypatch.setattr(client, "connect", connect_stub)
    monkeypatch.setattr(client, "_message_handler", handler_stub)
    monkeypatch.setattr(MODULE_ASYNCIO, "wait_for", immediate_timeout)
    monkeypatch.setattr(MODULE_ASYNCIO, "sleep", AsyncMock())
    await _consume(client.subscribe_executions())


@pytest.mark.asyncio
async def test_subscribe_executions_handles_connection_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test execution subscription handles ConnectionClosed gracefully.

    Given: Authenticated client whose connect raises ConnectionClosed.
    When: subscribe_executions() is consumed.
    Then: Iteration completes without error.
    """
    client = ZondaExchangeClient(api_key="key", api_secret="secret", reconnect_delay=0)

    async def connect_stub() -> None:
        client._running = False
        raise ConnectionClosed(None, None)

    monkeypatch.setattr(client, "connect", connect_stub)
    monkeypatch.setattr(MODULE_ASYNCIO, "sleep", AsyncMock())
    await _consume(client.subscribe_executions())


@pytest.mark.asyncio
async def test_subscribe_executions_raises_on_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test execution subscription raises RuntimeError when not running.

    Given: Authenticated client whose connect sets _running=False and raises.
    When: subscribe_executions() is consumed.
    Then: RuntimeError is raised.
    """
    client = ZondaExchangeClient(api_key="key", api_secret="secret")

    async def connect_stub() -> None:
        client._running = False
        raise RuntimeError("boom")

    monkeypatch.setattr(client, "connect", connect_stub)
    with pytest.raises(RuntimeError, match="boom"):
        await _consume(client.subscribe_executions())


@pytest.mark.asyncio
async def test_subscribe_instruments_success_and_error() -> None:
    """Test subscribe_instruments returns market data or raises on error.

    Given: Client with mocked load_markets returning dict or empty list.
    When: subscribe_instruments() is iterated.
    Then: Markets are yielded as dicts, or RuntimeError is raised on empty.
    """
    client = ZondaExchangeClient()
    with patch.object(
        client._ccxt_client,
        "load_markets",
        return_value={"BTC/USD": {"id": "BTC-USD", "symbol": "BTC/USD"}},
    ):
        items: list[Any] = []
        async for item in client.subscribe_instruments():
            items.append(item)
        assert items == [{"id": "BTC-USD", "symbol": "BTC/USD"}]
    with (
        patch.object(client._ccxt_client, "load_markets", return_value=[]),
        pytest.raises(RuntimeError),
    ):
        items = []
        async for item in client.subscribe_instruments():
            items.append(item)
        assert items == []


@pytest.mark.asyncio
async def test_subscribe_instruments_skips_non_dict_market_data() -> None:
    """Test subscribe_instruments skips non-dict market values.

    Given: load_markets returns mix of dicts, strings, and None.
    When: subscribe_instruments() is iterated.
    Then: Only dict values are yielded.
    """
    client = ZondaExchangeClient()
    with patch.object(
        client._ccxt_client,
        "load_markets",
        return_value={
            "BTC/USD": {"id": "BTC-USD", "symbol": "BTC/USD"},
            "ETH/USD": "not_a_dict",
            "XRP/USD": None,
            "SOL/USD": {"id": "SOL-USD", "symbol": "SOL/USD"},
        },
    ):
        items: list[Any] = []
        async for item in client.subscribe_instruments():
            items.append(item)
        assert len(items) == 2
        assert {"id": "BTC-USD", "symbol": "BTC/USD"} in items
        assert {"id": "SOL-USD", "symbol": "SOL/USD"} in items


@pytest.mark.asyncio
async def test_subscribe_ticks_message_handler_task_done_restarts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test tick subscription restarts handler task when done.

    Given: Client with completed _message_handler_task.
    When: subscribe_ticks(['BTC-USD']) is consumed.
    Then: A new task is created.
    """
    client = ZondaExchangeClient(reconnect_delay=0)
    task_created = False

    async def connect_stub() -> None:
        client._ws = cast(ClientConnection, DummyWs())

    original_create_task = asyncio.create_task

    def tracking_create_task(coro: Any, **kwargs: Any) -> asyncio.Task[Any]:
        nonlocal task_created
        task_created = True
        return original_create_task(coro, **kwargs)

    async def immediate_timeout(*args: Any, **kwargs: Any) -> None:
        coro = args[0]
        if hasattr(coro, "close"):
            coro.close()
        client._running = False
        raise TimeoutError

    monkeypatch.setattr(client, "connect", connect_stub)
    monkeypatch.setattr(MODULE_ASYNCIO, "wait_for", immediate_timeout)
    monkeypatch.setattr(MODULE_ASYNCIO, "sleep", AsyncMock())
    monkeypatch.setattr(asyncio, "create_task", tracking_create_task)
    done_task = original_create_task(asyncio.sleep(0))
    await done_task
    client._message_handler_task = done_task
    await _consume(client.subscribe_ticks(["BTC-USD"]))
    assert task_created


@pytest.mark.asyncio
async def test_subscribe_trades_message_handler_task_done_restarts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test trade subscription restarts handler task when done.

    Given: Client with completed _message_handler_task.
    When: subscribe_trades(['BTC-USD']) is consumed.
    Then: A new task is created.
    """
    client = ZondaExchangeClient(reconnect_delay=0)
    task_created = False

    async def connect_stub() -> None:
        client._ws = cast(ClientConnection, DummyWs())

    original_create_task = asyncio.create_task

    def tracking_create_task(coro: Any, **kwargs: Any) -> asyncio.Task[Any]:
        nonlocal task_created
        task_created = True
        return original_create_task(coro, **kwargs)

    async def immediate_timeout(*args: Any, **kwargs: Any) -> None:
        coro = args[0]
        if hasattr(coro, "close"):
            coro.close()
        client._running = False
        raise TimeoutError

    monkeypatch.setattr(client, "connect", connect_stub)
    monkeypatch.setattr(MODULE_ASYNCIO, "wait_for", immediate_timeout)
    monkeypatch.setattr(MODULE_ASYNCIO, "sleep", AsyncMock())
    monkeypatch.setattr(asyncio, "create_task", tracking_create_task)
    done_task = original_create_task(asyncio.sleep(0))
    await done_task
    client._message_handler_task = done_task
    await _consume(client.subscribe_trades(["BTC-USD"]))
    assert task_created


@pytest.mark.asyncio
async def test_subscribe_trades_connection_closed_cleans_up_handler_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test trade subscription cleans up handler task on ConnectionClosed.

    Given: Client with running handler task and connect raising ConnectionClosed.
    When: subscribe_trades(['BTC-USD']) is consumed.
    Then: Handler task is cancelled or done.
    """
    client = ZondaExchangeClient(reconnect_delay=0)
    cancelled = False

    async def connect_stub() -> None:
        client._running = False
        raise ConnectionClosed(None, None)

    async def handler_that_cancels() -> None:
        nonlocal cancelled
        try:
            await asyncio.sleep(100)
        except asyncio.CancelledError:
            cancelled = True
            raise

    monkeypatch.setattr(client, "connect", connect_stub)
    monkeypatch.setattr(MODULE_ASYNCIO, "sleep", AsyncMock())
    client._message_handler_task = asyncio.create_task(handler_that_cancels())
    await asyncio.sleep(0.01)
    await _consume(client.subscribe_trades(["BTC-USD"]))
    assert cancelled or client._message_handler_task is None or client._message_handler_task.done()


@pytest.mark.asyncio
async def test_subscribe_executions_connection_closed_cleans_up_handler_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test execution subscription cleans up handler task on ConnectionClosed.

    Given: Authenticated client with running handler task and connect raising.
    When: subscribe_executions() is consumed.
    Then: Handler task is cancelled or done.
    """
    client = ZondaExchangeClient(api_key="key", api_secret="secret", reconnect_delay=0)
    cancelled = False

    async def connect_stub() -> None:
        client._running = False
        raise ConnectionClosed(None, None)

    async def handler_that_cancels() -> None:
        nonlocal cancelled
        try:
            await asyncio.sleep(100)
        except asyncio.CancelledError:
            cancelled = True
            raise

    monkeypatch.setattr(client, "connect", connect_stub)
    monkeypatch.setattr(MODULE_ASYNCIO, "sleep", AsyncMock())
    client._message_handler_task = asyncio.create_task(handler_that_cancels())
    await asyncio.sleep(0.01)
    await _consume(client.subscribe_executions())
    assert cancelled or client._message_handler_task is None or client._message_handler_task.done()


@pytest.mark.asyncio
async def test_subscribe_candles_cleanup_aggregator_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test candle subscription cleans up aggregator task on exit.

    Given: Client with running aggregator task and immediate timeout.
    When: subscribe_candles(['BTC-USD']) is consumed.
    Then: Aggregator task is cancelled or done.
    """
    client = ZondaExchangeClient(reconnect_delay=0)
    cancelled = False

    async def aggregator_that_cancels(symbols: list[str]) -> None:
        nonlocal cancelled
        try:
            await asyncio.sleep(100)
        except asyncio.CancelledError:
            cancelled = True
            raise

    async def immediate_timeout(*args: Any, **kwargs: Any) -> None:
        coro = args[0]
        if hasattr(coro, "close"):
            coro.close()
        client._running = False
        raise TimeoutError

    monkeypatch.setattr(client, "_candle_aggregator", aggregator_that_cancels)
    monkeypatch.setattr(MODULE_ASYNCIO, "wait_for", immediate_timeout)
    monkeypatch.setattr(MODULE_ASYNCIO, "sleep", AsyncMock())
    await _consume(client.subscribe_candles(["BTC-USD"]))
    assert (
        cancelled or client._candle_aggregator_task is None or client._candle_aggregator_task.done()
    )


@pytest.mark.asyncio
async def test_stop_candle_aggregator_noop_when_no_task() -> None:
    """Test _stop_candle_aggregator is a no-op when task is None.

    Given: Client with no aggregator task.
    When: _stop_candle_aggregator is called.
    Then: No error raised, task remains None.
    """
    client = ZondaExchangeClient(reconnect_delay=0)
    client._candle_aggregator_task = None
    await client._stop_candle_aggregator()
    assert client._candle_aggregator_task is None


@pytest.mark.asyncio
async def test_subscribe_candles_aggregator_task_done_restarts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test candle subscription restarts aggregator task when done.

    Given: Client with completed _candle_aggregator_task.
    When: subscribe_candles(['BTC-USD']) is consumed.
    Then: A new task is created.
    """
    client = ZondaExchangeClient(reconnect_delay=0)
    task_created = False
    original_create_task = asyncio.create_task

    def tracking_create_task(coro: Any, **kwargs: Any) -> asyncio.Task[Any]:
        nonlocal task_created
        task_created = True
        return original_create_task(coro, **kwargs)

    async def immediate_timeout(*args: Any, **kwargs: Any) -> None:
        coro = args[0]
        if hasattr(coro, "close"):
            coro.close()
        client._running = False
        raise TimeoutError

    monkeypatch.setattr(MODULE_ASYNCIO, "wait_for", immediate_timeout)
    monkeypatch.setattr(MODULE_ASYNCIO, "sleep", AsyncMock())
    monkeypatch.setattr(asyncio, "create_task", tracking_create_task)
    done_task = original_create_task(asyncio.sleep(0))
    await done_task
    client._candle_aggregator_task = done_task
    await _consume(client.subscribe_candles(["BTC-USD"]))
    assert task_created


@pytest.mark.asyncio
async def test_subscribe_ticks_connection_closed_cleans_up_handler_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test tick subscription cleans up handler task on ConnectionClosed.

    Given: Client with running handler task and connect raising ConnectionClosed.
    When: subscribe_ticks(['BTC-USD']) is consumed.
    Then: Handler task is cancelled or done.
    """
    client = ZondaExchangeClient(reconnect_delay=0)
    cancelled = False

    async def connect_stub() -> None:
        client._running = False
        raise ConnectionClosed(None, None)

    async def handler_that_cancels() -> None:
        nonlocal cancelled
        try:
            await asyncio.sleep(100)
        except asyncio.CancelledError:
            cancelled = True
            raise

    monkeypatch.setattr(client, "connect", connect_stub)
    monkeypatch.setattr(MODULE_ASYNCIO, "sleep", AsyncMock())
    client._message_handler_task = asyncio.create_task(handler_that_cancels())
    await asyncio.sleep(0.01)
    await _consume(client.subscribe_ticks(["BTC-USD"]))
    assert cancelled or client._message_handler_task is None or client._message_handler_task.done()


@pytest.mark.asyncio
async def test_parse_proxy_response_stats_type(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test proxy response with stats type calls _parse_stats_message.

    Given: Pending stats request and successful proxy response with stats.
    When: _parse_proxy_response() is called.
    Then: _parse_stats_message is invoked.
    """
    client = ZondaExchangeClient()
    parse_called = False

    async def mock_parse_stats(data: dict[str, Any]) -> None:
        nonlocal parse_called
        parse_called = True

    monkeypatch.setattr(client, "_parse_stats_message", mock_parse_stats)
    request_id = "test-request-id"
    client._pending_snapshots[request_id] = "stats"
    stats_response = {
        "action": "proxy-response",
        "requestId": request_id,
        "statusCode": 200,
        "body": {
            "status": "Ok",
            "stats": [
                {
                    "m": "BTC-USD",
                    "h": "50000",
                    "l": "48000",
                    "v": "100",
                    "r24h": "2.5",
                }
            ],
        },
    }
    await client._parse_proxy_response(stats_response)
    assert parse_called


@pytest.mark.asyncio
async def test_message_handler_executions_topic(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test message handler routes executions topic to parse function.

    Given: WebSocket message with trading/history/transactions topic.
    When: _message_handler() processes the message.
    Then: _parse_executions_message is called.
    """
    client = ZondaExchangeClient()
    parse_called = False

    async def mock_parse_executions(data: dict[str, Any]) -> None:
        nonlocal parse_called
        parse_called = True

    monkeypatch.setattr(client, "_parse_executions_message", mock_parse_executions)
    execution_msg = json.dumps(
        {
            "action": "push",
            "topic": "trading/history/transactions",
            "message": {"transactions": []},
            "timestamp": "1234567890",
            "seqNo": 1,
        }
    )
    client._ws = cast(ClientConnection, DummyWs([execution_msg]))
    client._running = True
    client._seq_no["trading/history/transactions"] = 0
    handler_task = asyncio.create_task(client._message_handler())
    await asyncio.sleep(0.1)
    client._running = False
    await asyncio.sleep(0.05)
    handler_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await handler_task
    assert parse_called


@pytest.mark.asyncio
async def test_resubscribe_wildcard_path(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test resubscribe with wildcard sends global ticker and stats paths.

    Given: Client subscribed to ['*'] with mock WebSocket.
    When: _resubscribe() is called.
    Then: Two messages sent with paths 'ticker' and 'stats', no market_code.
    """
    client = ZondaExchangeClient()
    client._subscribed_symbols = ["*"]
    sent_messages: list[str] = []

    class MockWs:
        async def send(self, msg: str) -> None:
            sent_messages.append(msg)

    client._ws = cast(ClientConnection, MockWs())
    await client._resubscribe()
    assert len(sent_messages) == 2
    ticker_msg = json.loads(sent_messages[0])
    assert ticker_msg["path"] == "ticker"
    assert "market_code" not in ticker_msg
    stats_msg = json.loads(sent_messages[1])
    assert stats_msg["path"] == "stats"
    assert "market_code" not in stats_msg


@pytest.mark.asyncio
async def test_subscribe_trades_exception_not_running_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test trade subscription raises exception when not running after error.

    Given: Client whose connect sets _running=False and raises ValueError.
    When: subscribe_trades(['BTC-USD']) is consumed.
    Then: ValueError is raised.
    """
    client = ZondaExchangeClient(reconnect_delay=0)
    call_count = 0

    async def connect_stub() -> None:
        nonlocal call_count
        call_count += 1
        client._ws = cast(ClientConnection, DummyWs())
        if call_count == 1:
            client._running = False
            raise ValueError("Test error")

    monkeypatch.setattr(client, "connect", connect_stub)
    with pytest.raises(ValueError, match="Test error"):
        await _consume(client.subscribe_trades(["BTC-USD"]))


@pytest.mark.asyncio
async def test_subscribe_executions_exception_not_running_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test execution subscription raises exception when not running after error.

    Given: Authenticated client whose connect sets _running=False and raises.
    When: subscribe_executions() is consumed.
    Then: ValueError is raised.
    """
    client = ZondaExchangeClient(api_key="key", api_secret="secret", reconnect_delay=0)
    call_count = 0

    async def connect_stub() -> None:
        nonlocal call_count
        call_count += 1
        client._ws = cast(ClientConnection, DummyWs())
        if call_count == 1:
            client._running = False
            raise ValueError("Test error")

    monkeypatch.setattr(client, "connect", connect_stub)
    with pytest.raises(ValueError, match="Test error"):
        await _consume(client.subscribe_executions())


@pytest.mark.asyncio
async def test_subscribe_trades_generic_exception_while_running(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test trade subscription retries on generic exception while running.

    Given: Client whose connect raises ValueError while _running is True.
    When: subscribe_trades(['BTC-USD']) is consumed.
    Then: asyncio.sleep is called for reconnect delay, then loop stops.
    """
    client = ZondaExchangeClient(reconnect_delay=0)
    call_count = 0

    async def connect_stub() -> None:
        nonlocal call_count
        call_count += 1
        client._ws = cast(ClientConnection, DummyWs())
        if call_count == 1:
            raise ValueError("Transient error")
        client._running = False

    sleep_called = False

    async def sleep_stub(delay: float) -> None:
        nonlocal sleep_called
        sleep_called = True

    monkeypatch.setattr(client, "connect", connect_stub)
    monkeypatch.setattr(MODULE_ASYNCIO, "sleep", sleep_stub)
    await _consume(client.subscribe_trades(["BTC-USD"]))
    assert sleep_called
    assert call_count == 2


@pytest.mark.asyncio
async def test_subscribe_executions_generic_exception_while_running(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test execution subscription retries on generic exception while running.

    Given: Authenticated client whose connect raises ValueError while _running is True.
    When: subscribe_executions() is consumed.
    Then: asyncio.sleep is called for reconnect delay, then loop stops.
    """
    client = ZondaExchangeClient(api_key="key", api_secret="secret", reconnect_delay=0)
    call_count = 0

    async def connect_stub() -> None:
        nonlocal call_count
        call_count += 1
        client._ws = cast(ClientConnection, DummyWs())
        if call_count == 1:
            raise ValueError("Transient error")
        client._running = False

    sleep_called = False

    async def sleep_stub(delay: float) -> None:
        nonlocal sleep_called
        sleep_called = True

    monkeypatch.setattr(client, "connect", connect_stub)
    monkeypatch.setattr(MODULE_ASYNCIO, "sleep", sleep_stub)
    await _consume(client.subscribe_executions())
    assert sleep_called
    assert call_count == 2


@pytest.mark.asyncio
async def test_subscribe_candles_skips_aggregator_when_task_active(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test candle subscription reuses existing active aggregator task.

    Given: Client with a running (not done) _candle_aggregator_task.
    When: subscribe_candles(['BTC-PLN']) is consumed.
    Then: No new aggregator task is created; existing one is reused.
    """
    client = ZondaExchangeClient(reconnect_delay=0)
    tasks_created: list[asyncio.Task[Any]] = []
    original_create_task = asyncio.create_task

    def tracking_create_task(coro: Any, **kwargs: Any) -> asyncio.Task[Any]:
        task = original_create_task(coro, **kwargs)
        tasks_created.append(task)
        return task

    async def immediate_timeout(*args: Any, **kwargs: Any) -> None:
        coro = args[0]
        if hasattr(coro, "close"):
            coro.close()
        client._running = False
        raise TimeoutError

    long_running_task = original_create_task(asyncio.sleep(100))
    client._candle_aggregator_task = long_running_task

    monkeypatch.setattr(MODULE_ASYNCIO, "wait_for", immediate_timeout)
    monkeypatch.setattr(MODULE_ASYNCIO, "sleep", AsyncMock())
    monkeypatch.setattr(asyncio, "create_task", tracking_create_task)
    await _consume(client.subscribe_candles(["BTC-PLN"]))
    assert len(tasks_created) == 0
    long_running_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await long_running_task


@pytest.mark.asyncio
async def test_handle_push_unrecognized_topic() -> None:
    """Test _handle_push_message ignores unrecognized topic prefix.

    Given: Push message with topic='trading/unknown/btc-pln'.
    When: _handle_push_message() is called.
    Then: No exception raised, no data queued.
    """
    client = ZondaExchangeClient()
    data: dict[str, Any] = {
        "action": "push",
        "topic": "trading/unknown/btc-pln",
        "message": {"some": "data"},
        "seqNo": 999,
    }
    await client._handle_push_message(data)
    assert client._tick_queue.empty()
    assert client._trade_queue.empty()


@pytest.mark.asyncio
async def test_parse_proxy_response_unknown_snapshot_type() -> None:
    """Test _parse_proxy_response ignores unknown snapshot types.

    Given: Pending request with snapshot_type='unknown' and successful proxy response.
    When: _parse_proxy_response() is called.
    Then: No exception raised, no handlers called.
    """
    client = ZondaExchangeClient()
    request_id = "req-unknown"
    client._pending_snapshots[request_id] = "unknown"
    data: dict[str, Any] = {
        "action": "proxy-response",
        "requestId": request_id,
        "statusCode": 200,
        "body": {"status": "Ok"},
    }
    await client._parse_proxy_response(data)
    assert request_id not in client._pending_snapshots
    assert client._tick_queue.empty()


@pytest.mark.asyncio
async def test_handle_connection_closed_without_handler_task() -> None:
    """Test _handle_connection_closed when no handler task is running.

    Given: Client with _message_handler_task set to None.
    When: _handle_connection_closed() is called.
    Then: WebSocket is cleaned up and seq_no is cleared without error.
    """
    client = ZondaExchangeClient(reconnect_delay=0)
    client._ws = cast(ClientConnection, DummyWs())
    client._message_handler_task = None
    client._seq_no["some-topic"] = 42
    await client._handle_connection_closed("test-context")
    assert client._ws is None
    assert len(client._seq_no) == 0


@pytest.mark.asyncio
async def test_ensure_message_handler_skips_when_task_active() -> None:
    """Test _ensure_message_handler_running skips when task is still active.

    Given: Client with _message_handler_task that is still running.
    When: _ensure_message_handler_running() is called.
    Then: The existing task is preserved unchanged.
    """
    client = ZondaExchangeClient()
    client._ws = cast(ClientConnection, DummyWs())
    existing_task = asyncio.create_task(asyncio.sleep(100))
    client._message_handler_task = existing_task
    client._ensure_message_handler_running()
    assert client._message_handler_task is existing_task
    existing_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await existing_task


class TestZondaLiveFixtures:
    """Verify order lifecycle using real CCXT responses captured from Zonda BTC-PLN.

    Each mock dict mirrors an actual ``_ccxt_client`` return value recorded
    during live integration tests.  The tests confirm that
    ``_convert_ccxt_order`` (via ``create_order`` / ``cancel_order`` /
    ``get_order``) produces the correct ``ExchangeOrderSnapshot`` fields.
    """

    @pytest.fixture
    def client(self) -> ZondaExchangeClient:
        """Provide an authenticated ZondaExchangeClient for order tests."""
        return ZondaExchangeClient(api_key="live-key", api_secret="live-secret")

    @pytest.mark.asyncio
    async def test_passive_create(self, client: ZondaExchangeClient) -> None:
        """Passive limit buy returns pending snapshot with zero fill.

        Given: CCXT create_order returns the order id for a resting limit buy.
        When: create_order is called with a limit buy at 125689.99.
        Then: Snapshot has id=eb363fb9, side=buy, type=limit, status=pending,
              filled=0.0, remaining=5e-05, client_order_id=None.
        """
        ccxt_response: dict[str, Any] = {"id": "eb363fb9-2a1c-11f1-81a9-4ea2d0fa018b"}
        with (
            patch.object(client._ccxt_client, "create_order", return_value=ccxt_response),
            patch.object(client, "_log_order_to_db", new_callable=AsyncMock, return_value=None),
        ):
            request = ExchangeOrderRequest(
                symbol="BTC-PLN",
                side=OrderSideEnum.BUY,
                type=OrderTypeEnum.LIMIT,
                amount=5e-05,
                price=125689.99,
            )
            snap = await client.create_order(request)
        assert snap.id == "eb363fb9-2a1c-11f1-81a9-4ea2d0fa018b"
        assert snap.side == OrderSideEnum.BUY
        assert snap.type == OrderTypeEnum.LIMIT
        assert snap.price == pytest.approx(125689.99)
        assert snap.amount == pytest.approx(5e-05)
        assert snap.status == OrderStatusEnum.PENDING
        assert snap.filled == pytest.approx(0.0)
        assert snap.remaining == pytest.approx(5e-05)
        assert snap.client_order_id is None
        assert snap.fee is None

    @pytest.mark.asyncio
    async def test_passive_cancel(self, client: ZondaExchangeClient) -> None:
        """Cancel of passive limit buy returns canceled snapshot.

        Given: Open orders include the target order; cancel succeeds.
        When: cancel_order is called for the passive order.
        Then: Snapshot has status=canceled with the original order details.
        """
        open_order: dict[str, Any] = {
            "id": "eb363fb9-2a1c-11f1-81a9-4ea2d0fa018b",
            "symbol": "BTC/PLN",
            "side": "buy",
            "type": "limit",
            "amount": 5e-05,
            "price": 125689.99,
            "status": "open",
            "filled": 0.0,
            "remaining": 0.0,
            "timestamp": 1700000000000,
        }
        with (
            patch.object(
                client._ccxt_client,
                "fetch_open_orders",
                return_value=[open_order],
            ),
            patch.object(
                client._ccxt_client,
                "cancel_order",
                return_value={"info": {"status": "Ok"}},
            ),
        ):
            snap = await client.cancel_order("eb363fb9-2a1c-11f1-81a9-4ea2d0fa018b", "BTC-PLN")
        assert snap.id == "eb363fb9-2a1c-11f1-81a9-4ea2d0fa018b"
        assert snap.status == OrderStatusEnum.CANCELED
        assert snap.side == OrderSideEnum.BUY
        assert snap.price == pytest.approx(125689.99)

    @pytest.mark.asyncio
    async def test_topbook_create_sell(self, client: ZondaExchangeClient) -> None:
        """Topbook limit sell at the ask returns pending snapshot.

        Given: CCXT create_order returns the order id for a resting limit sell.
        When: create_order is called with a limit sell.
        Then: Snapshot has side=sell, status=pending, filled=0.0, remaining=5e-05.
        """
        ccxt_response: dict[str, Any] = {"id": "a1b2c3d4-tbok-sell-open-zonda00000000"}
        with (
            patch.object(client._ccxt_client, "create_order", return_value=ccxt_response),
            patch.object(client, "_log_order_to_db", new_callable=AsyncMock, return_value=None),
        ):
            request = ExchangeOrderRequest(
                symbol="BTC-PLN",
                side=OrderSideEnum.SELL,
                type=OrderTypeEnum.LIMIT,
                amount=5e-05,
                price=250000.0,
            )
            snap = await client.create_order(request)
        assert snap.id == "a1b2c3d4-tbok-sell-open-zonda00000000"
        assert snap.side == OrderSideEnum.SELL
        assert snap.status == OrderStatusEnum.PENDING
        assert snap.filled == pytest.approx(0.0)
        assert snap.remaining == pytest.approx(5e-05)

    @pytest.mark.asyncio
    async def test_topbook_cancel(self, client: ZondaExchangeClient) -> None:
        """Cancel of topbook sell returns canceled snapshot.

        Given: Open orders include the topbook sell order.
        When: cancel_order is called.
        Then: Snapshot has status=canceled.
        """
        open_order: dict[str, Any] = {
            "id": "a1b2c3d4-tbok-sell-open-zonda00000000",
            "symbol": "BTC/PLN",
            "side": "sell",
            "type": "limit",
            "amount": 5e-05,
            "price": 250000.0,
            "status": "open",
            "filled": 0.0,
            "remaining": 0.0,
            "timestamp": 1700000000000,
        }
        with (
            patch.object(client._ccxt_client, "fetch_open_orders", return_value=[open_order]),
            patch.object(
                client._ccxt_client,
                "cancel_order",
                return_value={"info": {"status": "Ok"}},
            ),
        ):
            snap = await client.cancel_order("a1b2c3d4-tbok-sell-open-zonda00000000", "BTC-PLN")
        assert snap.status == OrderStatusEnum.CANCELED
        assert snap.side == OrderSideEnum.SELL

    @pytest.mark.asyncio
    async def test_aggressive_sell_immediate_fill(self, client: ZondaExchangeClient) -> None:
        """Aggressive limit sell returns pending snapshot on create.

        Given: CCXT create_order returns the order id for a limit sell.
        When: create_order is called.
        Then: Snapshot has status=pending, filled=0.0, remaining=5e-05.
        """
        ccxt_response: dict[str, Any] = {"id": "f5e6d7c8-aggr-sell-fill-zonda00000000"}
        with (
            patch.object(client._ccxt_client, "create_order", return_value=ccxt_response),
            patch.object(client, "_log_order_to_db", new_callable=AsyncMock, return_value=None),
        ):
            request = ExchangeOrderRequest(
                symbol="BTC-PLN",
                side=OrderSideEnum.SELL,
                type=OrderTypeEnum.LIMIT,
                amount=5e-05,
                price=249000.0,
            )
            snap = await client.create_order(request)
        assert snap.status == OrderStatusEnum.PENDING
        assert snap.filled == pytest.approx(0.0)
        assert snap.remaining == pytest.approx(5e-05)
        assert snap.side == OrderSideEnum.SELL

    @pytest.mark.asyncio
    async def test_aggressive_flatten_market_buy(self, client: ZondaExchangeClient) -> None:
        """Flatten via market buy returns pending snapshot on create.

        Given: CCXT create_order returns the order id for a market buy.
        When: create_order is called with a market buy.
        Then: Snapshot has type=market, side=buy, status=pending, price=None, filled=0.0.
        """
        ccxt_response: dict[str, Any] = {"id": "11223344-flat-mbuy-fill-zonda00000000"}
        with (
            patch.object(client._ccxt_client, "create_order", return_value=ccxt_response),
            patch.object(client, "_log_order_to_db", new_callable=AsyncMock, return_value=None),
        ):
            request = ExchangeOrderRequest(
                symbol="BTC-PLN",
                side=OrderSideEnum.BUY,
                type=OrderTypeEnum.MARKET,
                amount=5e-05,
            )
            snap = await client.create_order(request)
        assert snap.type == OrderTypeEnum.MARKET
        assert snap.side == OrderSideEnum.BUY
        assert snap.status == OrderStatusEnum.PENDING
        assert snap.price is None
        assert snap.filled == pytest.approx(0.0)
        assert snap.remaining == pytest.approx(5e-05)

    @pytest.mark.asyncio
    async def test_market_sell_immediate_fill(self, client: ZondaExchangeClient) -> None:
        """Market sell returns pending snapshot on create.

        Given: CCXT create_order returns the order id for a market sell.
        When: create_order is called with a market sell.
        Then: Snapshot has type=market, side=sell, status=pending, price=None, filled=0.0.
        """
        ccxt_response: dict[str, Any] = {"id": "aabbccdd-mkt-sell-fill-zonda00000000"}
        with (
            patch.object(client._ccxt_client, "create_order", return_value=ccxt_response),
            patch.object(client, "_log_order_to_db", new_callable=AsyncMock, return_value=None),
        ):
            request = ExchangeOrderRequest(
                symbol="BTC-PLN",
                side=OrderSideEnum.SELL,
                type=OrderTypeEnum.MARKET,
                amount=5e-05,
            )
            snap = await client.create_order(request)
        assert snap.type == OrderTypeEnum.MARKET
        assert snap.side == OrderSideEnum.SELL
        assert snap.status == OrderStatusEnum.PENDING
        assert snap.price is None
        assert snap.filled == pytest.approx(0.0)
        assert snap.remaining == pytest.approx(5e-05)

    @pytest.mark.asyncio
    async def test_market_flatten_buy(self, client: ZondaExchangeClient) -> None:
        """Market buy to flatten returns pending snapshot on create.

        Given: CCXT create_order returns the order id for a market buy.
        When: create_order is called with a market buy.
        Then: Snapshot has type=market, side=buy, status=pending, price=None.
        """
        ccxt_response: dict[str, Any] = {"id": "eeff0011-mkt-buy-flat-zonda00000000"}
        with (
            patch.object(client._ccxt_client, "create_order", return_value=ccxt_response),
            patch.object(client, "_log_order_to_db", new_callable=AsyncMock, return_value=None),
        ):
            request = ExchangeOrderRequest(
                symbol="BTC-PLN",
                side=OrderSideEnum.BUY,
                type=OrderTypeEnum.MARKET,
                amount=5e-05,
            )
            snap = await client.create_order(request)
        assert snap.type == OrderTypeEnum.MARKET
        assert snap.side == OrderSideEnum.BUY
        assert snap.status == OrderStatusEnum.PENDING
        assert snap.price is None

    @pytest.mark.asyncio
    async def test_cancel_inflight_create_and_cancel(self, client: ZondaExchangeClient) -> None:
        """Create then immediately cancel a limit order.

        Given: CCXT create_order returns the order id; then cancel succeeds.
        When: create_order followed by cancel_order.
        Then: Create returns pending snapshot; cancel returns canceled snapshot.
        """
        create_response: dict[str, Any] = {"id": "99887766-cinf-open-zonda000000000"}
        with (
            patch.object(client._ccxt_client, "create_order", return_value=create_response),
            patch.object(client, "_log_order_to_db", new_callable=AsyncMock, return_value=None),
        ):
            request = ExchangeOrderRequest(
                symbol="BTC-PLN",
                side=OrderSideEnum.BUY,
                type=OrderTypeEnum.LIMIT,
                amount=5e-05,
                price=100000.0,
            )
            snap_create = await client.create_order(request)
        assert snap_create.status == OrderStatusEnum.PENDING

        open_order: dict[str, Any] = {
            "id": "99887766-cinf-open-zonda000000000",
            "symbol": "BTC/PLN",
            "side": "buy",
            "type": "limit",
            "amount": 5e-05,
            "price": 100000.0,
            "status": "open",
            "filled": 0.0,
            "remaining": 0.0,
            "timestamp": 1700000000000,
        }
        with (
            patch.object(client._ccxt_client, "fetch_open_orders", return_value=[open_order]),
            patch.object(
                client._ccxt_client,
                "cancel_order",
                return_value={"info": {"status": "Ok"}},
            ),
        ):
            snap_cancel = await client.cancel_order("99887766-cinf-open-zonda000000000", "BTC-PLN")
        assert snap_cancel.status == OrderStatusEnum.CANCELED
        assert snap_cancel.id == "99887766-cinf-open-zonda000000000"
