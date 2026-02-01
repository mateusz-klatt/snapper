"""Tests for Walutomat exchange client."""

import asyncio
import base64
import contextlib
import math
from datetime import UTC
from datetime import datetime
from types import SimpleNamespace
from typing import Any
from typing import cast
from unittest.mock import AsyncMock

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import ExchangeOrderRequest
from snapper.infrastructure.exchanges.contracts import ExchangeOrderSnapshot
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.contracts import OrderStatusEnum
from snapper.infrastructure.exchanges.contracts import OrderTypeEnum
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.implementations.walutomat import WalutomatExchangeClient
from snapper.infrastructure.exchanges.schemas.walutomat import WalutomatMarketPair
from snapper.infrastructure.exchanges.schemas.walutomat import WalutomatMarketResponse


class StubResponse:
    """Stub HTTP response for Walutomat exchange tests."""

    def __init__(self, payload: Any, status_code: int = 200) -> None:
        """Initialize the instance."""
        self._payload = payload
        self.status_code = status_code

    def json(self) -> Any:
        """Return JSON payload."""
        return self._payload

    def raise_for_status(self) -> None:
        """Raise exception for HTTP error status codes."""
        if self.status_code >= 400:
            raise httpx.HTTPError(f"HTTP error: {self.status_code}")


class StubAsyncClient:
    """Stub async HTTP client for Walutomat exchange tests."""

    def __init__(
        self,
        get_responses: list[StubResponse | Exception] | None = None,
        post_responses: list[StubResponse | Exception] | None = None,
    ) -> None:
        """Initialize the instance."""
        self.get_responses = list(get_responses or [])
        self.post_responses = list(post_responses or [])
        self.get_calls: list[tuple[str, dict[str, Any]]] = []
        self.post_calls: list[tuple[str, dict[str, Any]]] = []
        self.closed = False

    async def get(self, url: str, **kwargs: Any) -> StubResponse:
        """Handle GET request and return stub response."""
        self.get_calls.append((url, kwargs))
        if not self.get_responses:
            raise AssertionError(f"Unexpected GET call: {url}")
        response = self.get_responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    async def post(self, url: str, **kwargs: Any) -> StubResponse:
        """Handle POST request and return stub response."""
        self.post_calls.append((url, kwargs))
        if not self.post_responses:
            raise AssertionError(f"Unexpected POST call: {url}")
        response = self.post_responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    async def aclose(self) -> None:
        """Close the client."""
        self.closed = True


def _generate_private_key_pem() -> str:
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return pem_bytes.decode()


@pytest.fixture(autouse=True)
def stub_symbol_mappings(monkeypatch: pytest.MonkeyPatch) -> None:
    """Provide stubbed symbol mapping functions for Walutomat tests."""

    def to_ws(symbol: str) -> str:
        normalized = symbol.replace("/", "-")
        base, quote = normalized.split("-")
        return f"{base}_{quote}"

    def to_rest(symbol: str) -> str:
        normalized = symbol.replace("/", "-")
        base, quote = normalized.split("-")
        return f"{base}{quote}"

    def ws_to_native(symbol: str) -> str:
        base, quote = symbol.split("_")
        return f"{base}-{quote}"

    def rest_to_native(symbol: str) -> str:
        base = symbol[:3]
        quote = symbol[3:]
        return f"{base}-{quote}"

    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.walutomat.native_to_walutomat", to_ws
    )
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.walutomat.native_to_walutomat_rest",
        to_rest,
    )
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.walutomat.walutomat_to_native",
        ws_to_native,
    )
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.walutomat.walutomat_rest_to_native",
        rest_to_native,
    )


@pytest.mark.asyncio()
async def test_connect_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify successful connection fetches market data.

    Given: A stub HTTP client returning market data,
    When: connect() is called,
    Then: Supported pairs are populated and client is closed on disconnect.
    """
    response_payload: list[dict[str, Any]] = [
        {
            "pair": "EUR_PLN",
            "bestOffers": {"bid_now": 4.2, "ask_now": 4.3, "forex_now": 4.25},
        }
    ]
    stub_client = StubAsyncClient(get_responses=[StubResponse(response_payload)])

    def client_factory(*_args: Any, **_kwargs: Any) -> StubAsyncClient:
        return stub_client

    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.walutomat.httpx.AsyncClient",
        client_factory,
    )
    client = WalutomatExchangeClient()
    await client.connect()
    assert client.get_supported_pairs() == ["EUR-PLN"]
    await client.disconnect()
    assert stub_client.closed is True


@pytest.mark.asyncio()
async def test_connect_failure_closes_client(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify connection failure closes HTTP client.

    Given: A stub HTTP client returning 500 error,
    When: connect() is called,
    Then: ConnectionError is raised and client is cleaned up.
    """
    failing_response = StubResponse([], status_code=500)
    stub_client = StubAsyncClient(get_responses=[failing_response])

    def client_factory(*_args: Any, **_kwargs: Any) -> StubAsyncClient:
        return stub_client

    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.walutomat.httpx.AsyncClient",
        client_factory,
    )
    client = WalutomatExchangeClient()
    with pytest.raises(ConnectionError, match="Failed to connect"):
        await client.connect()
    assert stub_client.closed is True
    assert client._http_client is None


def test_sign_request_requires_private_key() -> None:
    """Verify sign_request requires private key.

    Given: A client without private key,
    When: _sign_request() is called,
    Then: RuntimeError is raised.
    """
    client = WalutomatExchangeClient()
    with pytest.raises(RuntimeError, match="Trading API requires authentication"):
        client._sign_request("2026-01-01T00:00:00Z", "/endpoint")


def test_sign_request_returns_signature() -> None:
    """Verify sign_request returns valid signature.

    Given: A client with API key and private key,
    When: _sign_request() is called,
    Then: Non-empty signature string is returned.
    """
    client = WalutomatExchangeClient(
        api_key="key",
        private_key_data=_generate_private_key_pem(),
    )
    signature = client._sign_request("2026-01-01T00:00:00Z", "/endpoint")
    assert isinstance(signature, str)
    assert signature != ""


def test_get_auth_headers_requires_api_key() -> None:
    """Verify get_auth_headers requires API key.

    Given: A client without API key,
    When: _get_auth_headers() is called,
    Then: RuntimeError is raised.
    """
    client = WalutomatExchangeClient()
    with pytest.raises(RuntimeError, match="provide api_key"):
        client._get_auth_headers("/endpoint")


def test_get_auth_headers_without_private_key() -> None:
    """Verify get_auth_headers with only API key.

    Given: A client with API key but no private key,
    When: _get_auth_headers() is called,
    Then: Only X-API-Key header is returned.
    """
    client = WalutomatExchangeClient(api_key="test-key")
    headers = client._get_auth_headers("/endpoint")
    assert headers == {"X-API-Key": "test-key"}


def test_get_auth_headers_with_signature(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify get_auth_headers includes signature.

    Given: A client with API key and private key,
    When: _get_auth_headers() is called with body,
    Then: Headers include key, timestamp, and signature.
    """
    client = WalutomatExchangeClient(
        api_key="key",
        private_key_data=_generate_private_key_pem(),
    )

    class FakeDateTime:
        @staticmethod
        def now(tz: Any) -> Any:
            class FakeMoment:
                def strftime(self, fmt: str) -> str:
                    return "2026-01-01T00:00:00Z"

            return FakeMoment()

    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.walutomat.datetime", FakeDateTime
    )
    headers = client._get_auth_headers("/endpoint", "body=test")
    assert headers["X-API-Key"] == "key"
    assert headers["X-API-Timestamp"] == "2026-01-01T00:00:00Z"
    assert "X-API-Signature" in headers


@pytest.mark.asyncio()
async def test_fetch_market_data_requires_connection() -> None:
    """Verify fetch_market_data requires connection.

    Given: An unconnected client,
    When: _fetch_market_data() is called,
    Then: RuntimeError is raised.
    """
    client = WalutomatExchangeClient()
    with pytest.raises(RuntimeError, match="Not connected"):
        await client._fetch_market_data()


@pytest.mark.asyncio()
async def test_get_ticker_success() -> None:
    """Verify get_ticker returns ticker data.

    Given: A connected client with market data,
    When: get_ticker() is called for a symbol,
    Then: TickerSnapshot with bid/ask/last is returned.
    """
    payload: list[dict[str, Any]] = [
        {
            "pair": "EUR_PLN",
            "bestOffers": {"bid_now": 4.2, "ask_now": 4.3, "forex_now": 4.25},
        }
    ]
    client = WalutomatExchangeClient()
    stub_client = StubAsyncClient(get_responses=[StubResponse(payload)])
    client._http_client = cast(httpx.AsyncClient, stub_client)
    ticker = await client.get_ticker("EUR-PLN")
    assert ticker.symbol == "EUR-PLN"
    assert math.isclose(ticker.bid, 4.2, rel_tol=1e-9)
    assert math.isclose(ticker.ask, 4.3, rel_tol=1e-9)
    assert math.isclose(ticker.last, 4.25, rel_tol=1e-9)


@pytest.mark.asyncio()
async def test_get_ticker_missing_symbol() -> None:
    """Verify get_ticker raises for missing symbol.

    Given: A connected client without requested symbol data,
    When: get_ticker() is called for missing symbol,
    Then: ValueError is raised.
    """
    payload: list[dict[str, Any]] = [
        {
            "pair": "USD_PLN",
            "bestOffers": {"bid_now": 4.0, "ask_now": 4.1, "forex_now": 4.05},
        }
    ]
    client = WalutomatExchangeClient()
    stub_client = StubAsyncClient(get_responses=[StubResponse(payload)])
    client._http_client = cast(httpx.AsyncClient, stub_client)
    with pytest.raises(ValueError, match="Available"):
        await client.get_ticker("EUR-PLN")


def test_get_supported_pairs_requires_data() -> None:
    """Verify get_supported_pairs requires data.

    Given: A client without market data,
    When: get_supported_pairs() is called,
    Then: RuntimeError is raised.
    """
    client = WalutomatExchangeClient()
    with pytest.raises(RuntimeError, match="No data available"):
        client.get_supported_pairs()


def test_get_supported_pairs_returns_symbols() -> None:
    """Verify get_supported_pairs returns native symbols.

    Given: A client with cached market data,
    When: get_supported_pairs() is called,
    Then: List of native symbols is returned.
    """
    client = WalutomatExchangeClient()
    client._last_data = {
        "EUR_PLN": WalutomatMarketPair.model_validate(
            {
                "pair": "EUR_PLN",
                "bestOffers": {"bid_now": 4.23, "ask_now": 4.24, "forex_now": 4.235},
            }
        ),
        "USD_PLN": WalutomatMarketPair.model_validate(
            {
                "pair": "USD_PLN",
                "bestOffers": {"bid_now": 3.95, "ask_now": 3.96, "forex_now": 3.955},
            }
        ),
    }
    pairs = client.get_supported_pairs()
    assert pairs == ["EUR-PLN", "USD-PLN"]


@pytest.mark.asyncio()
async def test_subscribe_instruments_yields_pairs(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify subscribe_instruments yields instrument metadata.

    Given: A running client with market data,
    When: subscribe_instruments() is iterated,
    Then: Instrument dictionaries with native_symbol are yielded.
    """
    client = WalutomatExchangeClient()
    client._running = True
    client._http_client = AsyncMock()

    def fake_market_response() -> dict[str, WalutomatMarketPair]:
        response: dict[str, WalutomatMarketPair] = WalutomatMarketResponse.from_api_response(
            [
                {
                    "pair": "EUR_PLN",
                    "bestOffers": {"bid_now": 4.0, "ask_now": 4.1, "forex_now": 4.05},
                },
                {"pair": "BAD", "bestOffers": {"bid_now": 1, "ask_now": 2, "forex_now": 1.5}},
            ]
        ).to_dict()
        return response

    monkeypatch.setattr(
        client, "_fetch_market_data", AsyncMock(return_value=fake_market_response())
    )
    items: list[dict[str, Any]] = []
    async for inst in client.subscribe_instruments():
        items.append(inst)
    assert items and items[0]["native_symbol"] == "EUR-PLN"


@pytest.mark.asyncio()
async def test_polling_loop_stops_on_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify polling loop stops after max errors.

    Given: A client with max_consecutive_errors=1,
    When: Polling encounters HTTP error,
    Then: Running flag is set to False.
    """
    client = WalutomatExchangeClient(polling_interval=0.0)
    client._running = True
    client._max_consecutive_errors = 1
    client._http_client = AsyncMock()
    monkeypatch.setattr(
        client, "_fetch_market_data", AsyncMock(side_effect=httpx.HTTPError("fail"))
    )
    await client._polling_loop(["EUR-PLN"])
    assert client._running is False


@pytest.mark.asyncio()
async def test_get_order_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify get_order raises for missing order.

    Given: A connected client with no matching orders,
    When: get_order() is called,
    Then: ValueError is raised.
    """
    client = WalutomatExchangeClient(api_key="k", private_key_data=_generate_private_key_pem())
    client._http_client = AsyncMock()
    monkeypatch.setattr(client, "get_orders", AsyncMock(return_value=[]))
    with pytest.raises(ValueError):
        await client.get_order("abc")


@pytest.mark.asyncio()
async def test_get_orders_filters_and_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify get_orders applies filters and limit.

    Given: A connected client with multiple orders,
    When: get_orders() is called with symbol filter and limit,
    Then: Filtered and limited orders are returned.
    """
    client = WalutomatExchangeClient(api_key="k", private_key_data=_generate_private_key_pem())
    get_resp = StubResponse(
        {
            "success": True,
            "result": [
                {
                    "orderId": "1",
                    "submitId": "c1",
                    "currencyPair": "EURPLN",
                    "buySell": "BUY",
                    "limitPrice": 4.2,
                    "volume": 1.0,
                    "status": "ACTIVE",
                    "boughtAmount": 0.5,
                },
                {
                    "orderId": "2",
                    "submitId": "c2",
                    "currencyPair": "USDPLN",
                    "buySell": "SELL",
                    "limitPrice": 4.0,
                    "volume": 2.0,
                    "status": "FILLED",
                    "boughtAmount": 2.0,
                },
            ],
        }
    )
    http_client = StubAsyncClient(get_responses=[get_resp])
    client._http_client = cast(httpx.AsyncClient, http_client)
    orders = await client.get_orders(symbol="EUR-PLN", limit=1)
    assert len(orders) == 1
    assert orders[0].symbol == "EUR-PLN"


@pytest.mark.asyncio()
async def test_get_balance_filters(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify get_balance filters by currency.

    Given: A connected client with multiple currencies,
    When: get_balance() is called with currency filter,
    Then: Only matching currency is returned.
    """
    client = WalutomatExchangeClient(api_key="k", private_key_data=_generate_private_key_pem())
    get_resp = StubResponse(
        {
            "success": True,
            "result": [
                {
                    "currency": "EUR",
                    "balanceAvailable": "1",
                    "balanceReserved": "0",
                    "balanceTotal": "1",
                },
                {
                    "currency": "USD",
                    "balanceAvailable": "2",
                    "balanceReserved": "0.5",
                    "balanceTotal": "2.5",
                },
            ],
        }
    )
    client._http_client = cast(httpx.AsyncClient, StubAsyncClient(get_responses=[get_resp]))
    balances = await client.get_balance(currency="USD")
    assert list(balances) == ["USD"]


@pytest.mark.asyncio()
async def test_get_ohlcv_returns_empty_list() -> None:
    """Verify get_ohlcv returns empty list.

    Given: A Walutomat client (OHLCV not supported),
    When: get_ohlcv() is called,
    Then: Empty list is returned.
    """
    client = WalutomatExchangeClient()
    result = await client.get_ohlcv("EUR-PLN")
    assert result == []


@pytest.mark.asyncio()
async def test_subscribe_instruments_requires_running() -> None:
    """Verify subscribe_instruments requires running state.

    Given: A non-running client,
    When: subscribe_instruments() is iterated,
    Then: RuntimeError is raised.
    """
    client = WalutomatExchangeClient()
    with pytest.raises(RuntimeError, match="Not connected"):
        generator = client.subscribe_instruments()
        await generator.__anext__()


@pytest.mark.asyncio()
async def test_subscribe_instruments_yields_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify subscribe_instruments yields symbol metadata.

    Given: A running client with market data,
    When: subscribe_instruments() is iterated,
    Then: Dictionaries with symbol and native_symbol are yielded.
    """
    client = WalutomatExchangeClient()
    client._running = True

    async def fake_fetch() -> dict[str, Any]:
        return {
            "EUR_PLN": {"bestOffers": {}},
            "USD_PLN": {"bestOffers": {}},
        }

    monkeypatch.setattr(client, "_fetch_market_data", fake_fetch)
    results: list[dict[str, Any]] = []
    async for item in client.subscribe_instruments():
        results.append(item)
    assert {entry["symbol"] for entry in results} == {"EUR_PLN", "USD_PLN"}
    assert {entry["native_symbol"] for entry in results} == {"EUR-PLN", "USD-PLN"}


@pytest.mark.asyncio()
async def test_subscribe_ticks_requires_running() -> None:
    """Verify subscribe_ticks requires running state.

    Given: A non-running client,
    When: subscribe_ticks() is iterated,
    Then: RuntimeError is raised.
    """
    client = WalutomatExchangeClient()
    with pytest.raises(RuntimeError, match="Not connected"):
        generator = client.subscribe_ticks(["EUR-PLN"])
        await generator.__anext__()


@pytest.mark.asyncio()
async def test_subscribe_ticks_yields_queue_data() -> None:
    """Verify subscribe_ticks yields from queue.

    Given: A running client with ticker in queue,
    When: subscribe_ticks() is iterated,
    Then: TickerUpdate from queue is yielded.
    """
    client = WalutomatExchangeClient()
    client._running = True
    client._polling_task = cast(Any, SimpleNamespace(done=lambda: False))
    ticker = TickerUpdate(
        symbol="EUR-PLN",
        bid=4.2,
        bid_qty=0.0,
        ask=4.3,
        ask_qty=0.0,
        last=4.25,
        volume=0.0,
        vwap=4.25,
        low=4.25,
        high=4.25,
        change=0.0,
        change_pct=0.0,
    )
    await client._tick_queue.put(ticker)
    generator = client.subscribe_ticks(["EUR-PLN"])
    first = await generator.__anext__()
    assert first.symbol == "EUR-PLN"
    client._running = False
    with pytest.raises(StopAsyncIteration):
        await generator.__anext__()


@pytest.mark.asyncio()
async def test_polling_loop_enqueues_tick_updates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify polling loop enqueues tick updates.

    Given: A running client with market data,
    When: Polling loop fetches data,
    Then: TickerUpdate is enqueued and buffer is updated.
    """
    client = WalutomatExchangeClient(polling_interval=0.0)
    client._running = True

    async def fake_fetch() -> dict[str, WalutomatMarketPair]:
        client._running = False
        return {
            "EUR_PLN": WalutomatMarketPair.model_validate(
                {
                    "pair": "EUR_PLN",
                    "bestOffers": {"bid_now": 4.2, "ask_now": 4.3, "forex_now": 4.25},
                }
            )
        }

    monkeypatch.setattr(client, "_fetch_market_data", fake_fetch)
    task = asyncio.create_task(client._polling_loop(["EUR-PLN"]))
    ticker = await asyncio.wait_for(client._tick_queue.get(), timeout=0.1)
    assert ticker.symbol == "EUR-PLN"
    assert math.isclose(ticker.bid, 4.2, rel_tol=1e-9)
    assert math.isclose(client._tick_buffers["EUR-PLN"][0][1], 4.25, rel_tol=1e-9)
    await asyncio.wait_for(task, timeout=0.1)


@pytest.mark.asyncio()
async def test_candle_builder_loop_emits_candles(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify candle builder emits candles from tick buffer.

    Given: A running client with ticks in buffer,
    When: Minute boundary is crossed,
    Then: CandleUpdate is emitted with OHLC data.
    """
    client = WalutomatExchangeClient()
    client._running = True
    client._tick_buffers["EUR-PLN"] = [(10.0, 4.10), (20.0, 4.30), (75.0, 4.50)]
    times = iter([59.0, 60.0, 120.0])
    real_sleep = asyncio.sleep

    def fake_time() -> float:
        return next(times, 120.0)

    async def fake_sleep(delay: float) -> None:
        await real_sleep(0)

    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.walutomat.time.time",
        fake_time,
    )
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.walutomat.asyncio.sleep",
        fake_sleep,
    )
    captured: list[CandleUpdate] = []

    class _StubQueue:
        async def put(self, item: CandleUpdate) -> None:
            captured.append(item)

    client._candle_queue = cast(asyncio.Queue[CandleUpdate], _StubQueue())
    task = asyncio.create_task(client._candle_builder_loop())
    await real_sleep(0)
    await real_sleep(0)
    client._running = False
    await real_sleep(0)
    with contextlib.suppress(asyncio.CancelledError):
        await task
    assert captured, "expected candle to be emitted"
    candle = captured[0]
    assert candle.symbol == "EUR-PLN"
    assert math.isclose(candle.open, 4.10, rel_tol=1e-9)
    assert math.isclose(candle.close, 4.30, rel_tol=1e-9)
    assert candle.trades == 2
    assert client._tick_buffers["EUR-PLN"] == [(75.0, 4.50)]


@pytest.mark.asyncio()
async def test_candle_builder_loop_handles_empty_minute(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify candle builder handles empty tick window.

    Given: A running client with ticks outside current window,
    When: Minute boundary is crossed,
    Then: Buffer remains unchanged and no candle emitted.
    """
    client = WalutomatExchangeClient()
    client._running = True
    client._tick_buffers["EUR-PLN"] = [(120.0, 4.00)]
    times = iter([0.0, 60.0])
    real_sleep = asyncio.sleep

    def fake_time() -> float:
        return next(times, 60.0)

    async def fake_sleep(_delay: float) -> None:
        await real_sleep(0)

    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.walutomat.time.time",
        fake_time,
    )
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.walutomat.asyncio.sleep",
        fake_sleep,
    )
    task = asyncio.create_task(client._candle_builder_loop())
    await real_sleep(0)
    await real_sleep(0)
    client._running = False
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    assert client._tick_buffers["EUR-PLN"] == [(120.0, 4.00)]


@pytest.mark.asyncio()
async def test_candle_builder_loop_exits_when_not_running() -> None:
    """Verify candle builder exits when not running.

    Given: A client with running=False,
    When: _candle_builder_loop() is called,
    Then: Loop exits immediately.
    """
    client = WalutomatExchangeClient()
    client._running = False
    await client._candle_builder_loop()


@pytest.mark.asyncio()
async def test_subscribe_ticks_wildcard_starts_polling_queue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify wildcard subscription starts polling.

    Given: A running client with supported pairs,
    When: subscribe_ticks(['*']) is called,
    Then: Polling loop starts for all pairs.
    """
    client = WalutomatExchangeClient()
    client._running = True
    monkeypatch.setattr(client, "get_supported_pairs", lambda: ["EUR-PLN"])
    polling_started = asyncio.Event()

    async def fake_polling(symbols: list[str]) -> None:
        assert symbols == ["EUR-PLN"]
        polling_started.set()
        while client._running:
            await asyncio.sleep(0)

    monkeypatch.setattr(client, "_polling_loop", fake_polling)
    ticker = TickerUpdate(
        symbol="EUR-PLN",
        bid=4.2,
        bid_qty=0.0,
        ask=4.3,
        ask_qty=0.0,
        last=4.25,
        volume=0.0,
        vwap=4.25,
        low=4.25,
        high=4.25,
        change=0.0,
        change_pct=0.0,
    )
    generator = client.subscribe_ticks(["*"])

    async def _consume_next() -> TickerUpdate:
        return await generator.__anext__()

    next_tick: asyncio.Task[TickerUpdate] = asyncio.create_task(_consume_next())
    await asyncio.wait_for(polling_started.wait(), timeout=0.2)
    await client._tick_queue.put(ticker)
    first = await asyncio.wait_for(next_tick, timeout=0.1)
    assert first.symbol == "EUR-PLN"
    client._running = False
    await asyncio.sleep(0)
    with pytest.raises(StopAsyncIteration):
        await generator.__anext__()
    if client._polling_task is not None:
        await asyncio.wait_for(client._polling_task, timeout=0.1)


@pytest.mark.asyncio()
async def test_subscribe_candles_not_connected() -> None:
    """Verify subscribe_candles requires connection.

    Given: An unconnected client,
    When: subscribe_candles() is iterated,
    Then: RuntimeError is raised.
    """
    client = WalutomatExchangeClient()
    with pytest.raises(RuntimeError, match="Not connected"):
        generator = client.subscribe_candles(["EUR-PLN"])
        await generator.__anext__()


@pytest.mark.asyncio()
async def test_subscribe_candles_unsupported_timeframe(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify subscribe_candles rejects unsupported timeframes.

    Given: A connected client,
    When: subscribe_candles() is called with 5m timeframe,
    Then: NotImplementedError is raised.
    """
    response_payload = [
        {
            "pair": "EUR_PLN",
            "bestOffers": {"bid_now": 4.2, "ask_now": 4.3, "forex_now": 4.25},
        }
    ]
    stub_client = StubAsyncClient(get_responses=[StubResponse(response_payload)])

    def client_factory(*_args: Any, **_kwargs: Any) -> StubAsyncClient:
        return stub_client

    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.walutomat.httpx.AsyncClient",
        client_factory,
    )
    client = WalutomatExchangeClient()
    await client.connect()
    try:
        with pytest.raises(NotImplementedError, match="only supports 1m"):
            generator = client.subscribe_candles(["EUR-PLN"], timeframe="5m")
            await generator.__anext__()
    finally:
        await client.disconnect()


@pytest.mark.asyncio()
async def test_subscribe_trades_not_supported() -> None:
    """Verify subscribe_trades is not supported.

    Given: A Walutomat client,
    When: subscribe_trades() is iterated,
    Then: NotImplementedError is raised.
    """
    client = WalutomatExchangeClient()
    with pytest.raises(NotImplementedError):
        generator = client.subscribe_trades(["EUR-PLN"])
        await generator.__anext__()


@pytest.mark.asyncio()
async def test_subscribe_executions_not_supported() -> None:
    """Verify subscribe_executions is not supported.

    Given: A Walutomat client,
    When: subscribe_executions() is iterated,
    Then: NotImplementedError is raised.
    """
    client = WalutomatExchangeClient()
    with pytest.raises(NotImplementedError):
        generator = client.subscribe_executions()
        await generator.__anext__()


@pytest.mark.asyncio()
async def test_subscribe_candles_skips_unrequested_symbol() -> None:
    """Verify subscribe_candles filters by requested symbols.

    Given: A running client receiving candles for other symbols,
    When: subscribe_candles() is called for specific symbol,
    Then: Non-matching candles are skipped.
    """
    client = WalutomatExchangeClient()
    client._running = True
    client._polling_task = cast(Any, SimpleNamespace(done=lambda: False))
    client._candle_builder_task = cast(Any, SimpleNamespace(done=lambda: False))
    other_symbol_candle = CandleUpdate(
        symbol="USD-PLN",
        open=1.0,
        high=1.0,
        low=1.0,
        close=1.0,
        volume=0.0,
        vwap=1.0,
        trades=0,
        interval_begin=datetime.now(UTC),
        interval=1,
    )

    class StopAfterSingleGet(asyncio.Queue[CandleUpdate]):
        def __init__(self, item: CandleUpdate) -> None:
            super().__init__()
            self._item = item

        async def get(self) -> CandleUpdate:
            client._running = False
            return self._item

    client._candle_queue = cast(
        asyncio.Queue[CandleUpdate], StopAfterSingleGet(other_symbol_candle)
    )
    generator = client.subscribe_candles(["EUR-PLN"])
    with pytest.raises(StopAsyncIteration):
        await generator.__anext__()


@pytest.mark.asyncio()
async def test_create_order_requires_connection() -> None:
    """Verify create_order requires connection.

    Given: An unconnected client with credentials,
    When: create_order() is called,
    Then: RuntimeError is raised.
    """
    client = WalutomatExchangeClient(
        api_key="key",
        private_key_data=_generate_private_key_pem(),
    )
    request = ExchangeOrderRequest(
        symbol="EUR-PLN",
        side=OrderSideEnum.BUY,
        type=OrderTypeEnum.LIMIT,
        amount=100.0,
        price=4.2,
    )
    with pytest.raises(RuntimeError, match="Not connected"):
        await client.create_order(request)


@pytest.mark.asyncio()
async def test_create_order_without_price_does_not_send_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify create_order omits limitPrice when price is None.

    Given: A connected client with credentials,
    When: create_order() is called without price,
    Then: Request body does not include limitPrice.
    """
    client = WalutomatExchangeClient(
        api_key="key",
        private_key_data=_generate_private_key_pem(),
    )
    stub_client = StubAsyncClient(
        post_responses=[StubResponse({"success": True, "result": {"orderId": "abc"}})]
    )
    client._http_client = cast(httpx.AsyncClient, stub_client)

    def auth_headers(_endpoint: str, _body: str) -> dict[str, str]:
        return {"X-API-Key": "key"}

    monkeypatch.setattr(client, "_get_auth_headers", auth_headers)
    request = ExchangeOrderRequest(
        symbol="EUR-PLN",
        side=OrderSideEnum.BUY,
        type=OrderTypeEnum.LIMIT,
        amount=1.0,
        price=None,
    )
    await client.create_order(request)
    encoded_body = stub_client.post_calls[0][1]["content"]
    assert "limitPrice" not in encoded_body


@pytest.mark.asyncio()
async def test_create_order_requires_authentication() -> None:
    """Verify create_order requires full authentication.

    Given: A connected client with only API key,
    When: create_order() is called,
    Then: RuntimeError is raised.
    """
    client = WalutomatExchangeClient(api_key="key")
    stub_client = StubAsyncClient(post_responses=[StubResponse({})])
    client._http_client = cast(httpx.AsyncClient, stub_client)
    request = ExchangeOrderRequest(
        symbol="EUR-PLN",
        side=OrderSideEnum.BUY,
        type=OrderTypeEnum.LIMIT,
        amount=100.0,
        price=4.2,
    )
    with pytest.raises(RuntimeError, match="provide api_key and private_key"):
        await client.create_order(request)


@pytest.mark.asyncio()
async def test_create_order_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify create_order returns order snapshot on success.

    Given: A connected client with credentials,
    When: create_order() is called with valid request,
    Then: ExchangeOrderSnapshot with PENDING status is returned.
    """
    client = WalutomatExchangeClient(
        api_key="key",
        private_key_data=_generate_private_key_pem(),
    )
    stub_client = StubAsyncClient(
        post_responses=[StubResponse({"success": True, "result": {"orderId": "abc123"}})]
    )
    client._http_client = cast(httpx.AsyncClient, stub_client)

    def auth_headers(_endpoint: str, _body: str) -> dict[str, str]:
        return {"X-API-Key": "key"}

    monkeypatch.setattr(client, "_get_auth_headers", auth_headers)
    request = ExchangeOrderRequest(
        symbol="EUR-PLN",
        side=OrderSideEnum.BUY,
        type=OrderTypeEnum.LIMIT,
        amount=100.0,
        price=4.2,
    )
    order = await client.create_order(request)
    assert order.id == "abc123"
    assert order.status == OrderStatusEnum.PENDING
    assert stub_client.post_calls[0][0].endswith("/market_fx/orders")


@pytest.mark.asyncio()
async def test_cancel_order_fetches_latest(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify cancel_order returns updated order state.

    Given: A connected client with credentials,
    When: cancel_order() is called,
    Then: Latest order state is fetched and returned.
    """
    client = WalutomatExchangeClient(
        api_key="key",
        private_key_data=_generate_private_key_pem(),
    )
    stub_client = StubAsyncClient(post_responses=[StubResponse({"success": True})])
    client._http_client = cast(httpx.AsyncClient, stub_client)

    def auth_headers(_endpoint: str, _body: str) -> dict[str, str]:
        return {"X-API-Key": "key"}

    monkeypatch.setattr(client, "_get_auth_headers", auth_headers)
    expected_order = ExchangeOrderSnapshot(
        id="abc123",
        client_order_id=None,
        symbol="EUR-PLN",
        side=OrderSideEnum.BUY,
        type=OrderTypeEnum.LIMIT,
        amount=100.0,
        price=4.2,
        status=OrderStatusEnum.OPEN,
        filled=0.0,
        remaining=100.0,
        timestamp=0.0,
    )
    monkeypatch.setattr(client, "get_order", AsyncMock(return_value=expected_order))
    result = await client.cancel_order("abc123")
    assert result is expected_order
    assert stub_client.post_calls[0][0].endswith("/cancel")


@pytest.mark.asyncio()
async def test_cancel_order_requires_authentication() -> None:
    """Verify cancel_order requires authentication.

    Given: A connected client without credentials,
    When: cancel_order() is called,
    Then: RuntimeError is raised.
    """
    client = WalutomatExchangeClient()
    client._http_client = cast(httpx.AsyncClient, StubAsyncClient())
    with pytest.raises(RuntimeError, match="provide api_key and private_key"):
        await client.cancel_order("abc123")


@pytest.mark.asyncio()
async def test_get_order_returns_matched_order(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify get_order returns matching order.

    Given: A connected client with matching order,
    When: get_order() is called with order ID,
    Then: Matching ExchangeOrderSnapshot is returned.
    """
    client = WalutomatExchangeClient(
        api_key="key",
        private_key_data=_generate_private_key_pem(),
    )
    stub_client = StubAsyncClient()
    client._http_client = cast(httpx.AsyncClient, stub_client)
    order = ExchangeOrderSnapshot(
        id="target",
        client_order_id=None,
        symbol="EUR-PLN",
        side=OrderSideEnum.BUY,
        type=OrderTypeEnum.LIMIT,
        amount=100.0,
        price=4.2,
        status=OrderStatusEnum.OPEN,
        filled=0.0,
        remaining=100.0,
        timestamp=0.0,
    )
    monkeypatch.setattr(client, "get_orders", AsyncMock(return_value=[order]))
    result = await client.get_order("target")
    assert result is order


@pytest.mark.asyncio()
async def test_get_order_raises_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify get_order raises for empty orders list.

    Given: A connected client with no orders,
    When: get_order() is called,
    Then: ValueError is raised.
    """
    client = WalutomatExchangeClient(
        api_key="key",
        private_key_data=_generate_private_key_pem(),
    )
    stub_client = StubAsyncClient()
    client._http_client = cast(httpx.AsyncClient, stub_client)
    monkeypatch.setattr(client, "get_orders", AsyncMock(return_value=[]))
    with pytest.raises(ValueError, match="not found"):
        await client.get_order("missing")


@pytest.mark.asyncio()
async def test_get_order_raises_when_no_ids_match(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify get_order raises when no ID matches.

    Given: A connected client with orders but none matching,
    When: get_order() is called with non-existent ID,
    Then: ValueError is raised.
    """
    client = WalutomatExchangeClient(
        api_key="key",
        private_key_data=_generate_private_key_pem(),
    )
    stub_client = StubAsyncClient()
    client._http_client = cast(httpx.AsyncClient, stub_client)
    order = ExchangeOrderSnapshot(
        id="other",
        client_order_id=None,
        symbol="EUR-PLN",
        side=OrderSideEnum.BUY,
        type=OrderTypeEnum.LIMIT,
        amount=100.0,
        price=4.2,
        status=OrderStatusEnum.OPEN,
        filled=0.0,
        remaining=100.0,
        timestamp=0.0,
    )
    monkeypatch.setattr(client, "get_orders", AsyncMock(return_value=[order]))
    with pytest.raises(ValueError, match="not found"):
        await client.get_order("missing")


@pytest.mark.asyncio()
async def test_get_orders_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify get_orders returns order snapshots.

    Given: A connected client with orders,
    When: get_orders() is called,
    Then: List of ExchangeOrderSnapshot is returned.
    """
    client = WalutomatExchangeClient(
        api_key="key",
        private_key_data=_generate_private_key_pem(),
    )
    payload: dict[str, Any] = {
        "success": True,
        "result": [
            {
                "orderId": "abc",
                "submitId": "sub",
                "currencyPair": "EURPLN",
                "buySell": "BUY",
                "volume": "100",
                "limitPrice": "4.20",
                "status": "ACTIVE",
                "boughtAmount": "20",
            }
        ],
    }
    stub_client = StubAsyncClient(get_responses=[StubResponse(payload)])
    client._http_client = cast(httpx.AsyncClient, stub_client)

    def auth_headers(_endpoint: str, _body: str) -> dict[str, str]:
        return {"X-API-Key": "key"}

    monkeypatch.setattr(client, "_get_auth_headers", auth_headers)
    orders = await client.get_orders()
    assert len(orders) == 1
    assert orders[0].id == "abc"
    assert orders[0].symbol == "EUR-PLN"
    assert math.isclose(orders[0].filled, 20.0, rel_tol=1e-9)


@pytest.mark.asyncio()
async def test_get_orders_requires_connection() -> None:
    """Verify get_orders requires connection.

    Given: An unconnected client with credentials,
    When: get_orders() is called,
    Then: RuntimeError is raised.
    """
    client = WalutomatExchangeClient(api_key="key", private_key_data=_generate_private_key_pem())
    with pytest.raises(RuntimeError, match="Not connected"):
        await client.get_orders()


@pytest.mark.asyncio()
async def test_get_orders_requires_authentication() -> None:
    """Verify get_orders requires authentication.

    Given: A connected client without credentials,
    When: get_orders() is called,
    Then: RuntimeError is raised.
    """
    client = WalutomatExchangeClient()
    client._http_client = cast(httpx.AsyncClient, StubAsyncClient())
    with pytest.raises(RuntimeError, match="provide api_key and private_key"):
        await client.get_orders()


@pytest.mark.asyncio()
async def test_get_orders_filters_by_status(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify get_orders filters by status.

    Given: A connected client with FILLED orders,
    When: get_orders() is called with OPEN status filter,
    Then: Empty list is returned.
    """
    client = WalutomatExchangeClient(
        api_key="key",
        private_key_data=_generate_private_key_pem(),
    )
    payload: dict[str, Any] = {
        "success": True,
        "result": [
            {
                "orderId": "1",
                "submitId": "c1",
                "currencyPair": "EURPLN",
                "buySell": "BUY",
                "limitPrice": "4.2",
                "volume": "1",
                "status": "FILLED",
            }
        ],
    }
    client._http_client = cast(
        httpx.AsyncClient, StubAsyncClient(get_responses=[StubResponse(payload)])
    )

    def auth_headers(_endpoint: str, _body: str) -> dict[str, str]:
        return {"X-API-Key": "key"}

    monkeypatch.setattr(client, "_get_auth_headers", auth_headers)
    orders = await client.get_orders(status=OrderStatusEnum.OPEN)
    assert orders == []


@pytest.mark.asyncio()
async def test_get_balance_requires_connection() -> None:
    """Verify get_balance requires connection.

    Given: An unconnected client with API key,
    When: get_balance() is called,
    Then: RuntimeError is raised.
    """
    client = WalutomatExchangeClient(api_key="key")
    with pytest.raises(RuntimeError, match="Not connected"):
        await client.get_balance()


@pytest.mark.asyncio()
async def test_get_balance_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify get_balance returns account balances.

    Given: A connected client with balance data,
    When: get_balance() is called,
    Then: Dictionary of AccountBalance by currency is returned.
    """
    client = WalutomatExchangeClient(api_key="key")
    payload: dict[str, Any] = {
        "success": True,
        "result": [
            {
                "currency": "EUR",
                "balanceAvailable": "100",
                "balanceReserved": "10",
                "balanceTotal": "110",
            },
            {
                "currency": "USD",
                "balanceAvailable": "50",
                "balanceReserved": "5",
                "balanceTotal": "55",
            },
        ],
    }
    stub_client = StubAsyncClient(get_responses=[StubResponse(payload)])
    client._http_client = cast(httpx.AsyncClient, stub_client)

    def auth_headers(_endpoint: str, _body: str) -> dict[str, str]:
        return {"X-API-Key": "key"}

    monkeypatch.setattr(client, "_get_auth_headers", auth_headers)
    balances = await client.get_balance()
    assert math.isclose(balances["EUR"].free, 100.0, rel_tol=1e-9)
    assert math.isclose(balances["USD"].total, 55.0, rel_tol=1e-9)


@pytest.mark.asyncio()
async def test_get_balance_filters_currency(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify get_balance filters by currency.

    Given: A connected client with multiple currencies,
    When: get_balance() is called with currency filter,
    Then: Only matching currency balance is returned.
    """
    client = WalutomatExchangeClient(api_key="key")
    payload: dict[str, Any] = {
        "success": True,
        "result": [
            {
                "currency": "EUR",
                "balanceAvailable": "100",
                "balanceReserved": "10",
                "balanceTotal": "110",
            },
            {
                "currency": "USD",
                "balanceAvailable": "50",
                "balanceReserved": "5",
                "balanceTotal": "55",
            },
        ],
    }
    stub_client = StubAsyncClient(get_responses=[StubResponse(payload)])
    client._http_client = cast(httpx.AsyncClient, stub_client)

    def auth_headers(_endpoint: str, _body: str) -> dict[str, str]:
        return {"X-API-Key": "key"}

    monkeypatch.setattr(client, "_get_auth_headers", auth_headers)
    balances = await client.get_balance(currency="EUR")
    assert list(balances) == ["EUR"]
    assert math.isclose(balances["EUR"].used, 10.0, rel_tol=1e-9)


@pytest.mark.asyncio()
async def test_get_balance_requires_api_key_when_connected() -> None:
    """Verify get_balance requires API key when connected.

    Given: A connected client without API key,
    When: get_balance() is called,
    Then: RuntimeError is raised.
    """
    client = WalutomatExchangeClient()
    client._http_client = cast(httpx.AsyncClient, StubAsyncClient())
    with pytest.raises(RuntimeError, match="provide api_key"):
        await client.get_balance()


@pytest.mark.asyncio()
async def test_disconnect_without_running() -> None:
    """Verify disconnect is no-op when not running.

    Given: A client that is not running,
    When: disconnect() is called,
    Then: No error occurs.
    """
    client = WalutomatExchangeClient()
    await client.disconnect()


@pytest.mark.asyncio()
async def test_disconnect_running_without_http_client() -> None:
    """Verify disconnect handles missing HTTP client.

    Given: A running client without HTTP client,
    When: disconnect() is called,
    Then: Running flag is cleared without error.
    """
    client = WalutomatExchangeClient()
    client._running = True
    await client.disconnect()


@pytest.mark.asyncio()
async def test_subscribe_ticks_not_connected() -> None:
    """Verify subscribe_ticks requires connection.

    Given: An unconnected client,
    When: subscribe_ticks() is iterated,
    Then: RuntimeError is raised.
    """
    client = WalutomatExchangeClient()
    with pytest.raises(RuntimeError, match="Not connected"):
        gen = client.subscribe_ticks(["EUR-PLN"])
        await gen.__anext__()


@pytest.mark.asyncio()
async def test_subscribe_ticks_wildcard_starts_polling(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify wildcard subscription reuses existing polling task.

    Given: A running client with active polling task,
    When: subscribe_ticks(['*']) is called,
    Then: Tickers from queue are yielded.
    """
    client = WalutomatExchangeClient()
    client._running = True
    monkeypatch.setattr(client, "get_supported_pairs", lambda: ["EUR-PLN"])

    async def _never() -> None:
        await asyncio.Event().wait()

    pending = asyncio.create_task(_never())
    client._polling_task = pending
    await client._tick_queue.put(
        TickerUpdate(
            symbol="EUR-PLN",
            bid=1.0,
            ask=1.1,
            last=1.05,
            volume=0.0,
            bid_qty=0.0,
            ask_qty=0.0,
            vwap=1.05,
            low=1.0,
            high=1.1,
            change=0.0,
            change_pct=0.0,
        )
    )
    gen = client.subscribe_ticks(["*"])
    ticker = await gen.__anext__()
    assert ticker.symbol == "EUR-PLN"
    pending.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await pending


@pytest.mark.asyncio()
async def test_subscribe_candles_wildcard(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify wildcard candle subscription works.

    Given: A running client with candle in queue,
    When: subscribe_candles(['*']) is iterated,
    Then: CandleUpdate is yielded.
    """
    client = WalutomatExchangeClient()
    client._running = True
    monkeypatch.setattr(client, "get_supported_pairs", lambda: ["EUR-PLN"])

    async def _never() -> None:
        await asyncio.Event().wait()

    pending_poll = asyncio.create_task(_never())
    pending_candle = asyncio.create_task(_never())
    client._polling_task = pending_poll
    client._candle_builder_task = pending_candle
    now = datetime.now(UTC)
    await client._candle_queue.put(
        CandleUpdate(
            symbol="EUR-PLN",
            open=1.0,
            high=1.0,
            low=1.0,
            close=1.0,
            volume=0.0,
            vwap=1.0,
            trades=0,
            interval_begin=now,
            interval=1,
        )
    )
    gen = client.subscribe_candles(["*"])
    candle = await gen.__anext__()
    assert candle.symbol == "EUR-PLN"
    pending_poll.cancel()
    pending_candle.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await pending_poll
    with contextlib.suppress(asyncio.CancelledError):
        await pending_candle


@pytest.mark.asyncio()
async def test_subscribe_instruments_bad_symbol(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify subscribe_instruments skips malformed symbols.

    Given: A running client with malformed symbol data,
    When: subscribe_instruments() is iterated,
    Then: Iteration stops without yielding.
    """
    client = WalutomatExchangeClient()
    client._running = True
    monkeypatch.setattr(client, "_fetch_market_data", AsyncMock(return_value={"BADFORMAT": None}))
    gen = client.subscribe_instruments()
    with pytest.raises(StopAsyncIteration):
        await gen.__anext__()


@pytest.mark.asyncio()
async def test_connect_noop_when_running(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify connect is no-op when already running.

    Given: A running client,
    When: connect() is called,
    Then: No fetch occurs and HTTP client remains None.
    """
    client = WalutomatExchangeClient()
    client._running = True
    client._fetch_market_data = AsyncMock(side_effect=AssertionError("should not fetch"))
    await client.connect()
    assert client._http_client is None


@pytest.mark.asyncio()
async def test_disconnect_cancels_tasks_and_closes() -> None:
    """Verify disconnect cancels tasks and closes client.

    Given: A running client with active tasks and HTTP client,
    When: disconnect() is called,
    Then: Tasks are cancelled and HTTP client is closed.
    """
    client = WalutomatExchangeClient()
    client._running = True
    real_sleep = asyncio.sleep
    client._polling_task = asyncio.create_task(real_sleep(10))
    client._candle_builder_task = asyncio.create_task(real_sleep(10))
    closed = {"called": False}

    class DummyHTTP:
        async def aclose(self) -> None:
            closed["called"] = True

    client._http_client = DummyHTTP()
    await client.disconnect()
    assert closed["called"]
    assert client._polling_task is None
    assert client._candle_builder_task is None
    assert client._http_client is None


@pytest.mark.asyncio()
async def test_polling_loop_skips_missing_symbol(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify polling loop handles missing requested symbols.

    Given: A running client requesting EUR-PLN and USD-PLN,
    When: Market data only has EUR-PLN,
    Then: Only EUR-PLN ticker is enqueued.
    """
    client = WalutomatExchangeClient()
    client._running = True
    pair = WalutomatMarketPair.model_validate(
        {"pair": "EUR_PLN", "bestOffers": {"bid_now": 4.0, "ask_now": 4.1, "forex_now": 4.05}}
    )
    fetch_calls: list[dict[str, WalutomatMarketPair]] = []

    async def fake_fetch() -> dict[str, WalutomatMarketPair]:
        fetch_calls.append({"EUR_PLN": pair})
        client._running = False
        return {"EUR_PLN": pair}

    client._fetch_market_data = AsyncMock(side_effect=fake_fetch)
    real_sleep = asyncio.sleep

    async def fast_sleep(_seconds: float) -> None:
        await real_sleep(0)

    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.walutomat.asyncio.sleep", fast_sleep
    )
    await client._polling_loop(["EUR-PLN", "USD-PLN"])
    ticker = await client._tick_queue.get()
    assert ticker.symbol == "EUR-PLN"
    assert "EUR-PLN" in client._tick_buffers


@pytest.mark.asyncio()
async def test_polling_loop_stops_after_http_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify polling loop stops after max HTTP errors.

    Given: A client with max_consecutive_errors=1,
    When: HTTP error occurs,
    Then: Running flag is set to False.
    """
    client = WalutomatExchangeClient()
    client._running = True
    client._max_consecutive_errors = 1
    client._fetch_market_data = AsyncMock(side_effect=httpx.HTTPError("boom"))
    real_sleep = asyncio.sleep

    async def fast_sleep(_seconds: float) -> None:
        await real_sleep(0)

    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.walutomat.asyncio.sleep", fast_sleep
    )
    await client._polling_loop(["EUR-PLN"])
    assert client._running is False


@pytest.mark.asyncio()
async def test_polling_loop_logs_generic_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify polling loop handles generic errors.

    Given: A running client with error in fetch,
    When: Generic exception occurs,
    Then: Loop continues until stopped.
    """
    client = WalutomatExchangeClient()
    client._running = True
    client._fetch_market_data = AsyncMock(side_effect=ValueError("oops"))
    real_sleep = asyncio.sleep

    async def fast_sleep(_seconds: float) -> None:
        client._running = False
        await real_sleep(0)

    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.walutomat.asyncio.sleep", fast_sleep
    )
    await client._polling_loop(["EUR-PLN"])
    assert client._running is False


@pytest.mark.asyncio()
async def test_candle_builder_loop_emits_from_buffer(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify candle builder emits from buffer and clears it.

    Given: A running client with ticks in buffer,
    When: Time crosses minute boundary,
    Then: Candle is emitted and buffer is cleared.
    """
    client = WalutomatExchangeClient()
    client._running = True
    client._tick_buffers = {"EUR-PLN": [(70.0, 4.0), (118.0, 4.2)]}
    real_sleep = asyncio.sleep

    async def fast_sleep(_seconds: float) -> None:
        await real_sleep(0)

    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.walutomat.asyncio.sleep", fast_sleep
    )
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.walutomat.time.time",
        lambda: 120.0,
    )
    task = asyncio.create_task(client._candle_builder_loop())
    candle = await asyncio.wait_for(client._candle_queue.get(), timeout=0.1)
    client._running = False
    with contextlib.suppress(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=0.1)
    assert candle.symbol == "EUR-PLN"
    assert candle.open == pytest.approx(4.0)
    assert client._tick_buffers["EUR-PLN"] == []


@pytest.mark.asyncio()
async def test_subscribe_ticks_timeout_exits(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify subscribe_ticks handles timeout gracefully.

    Given: A running client with timeout on queue get,
    When: TimeoutError occurs and running is False,
    Then: Iteration stops.
    """
    client = WalutomatExchangeClient()
    client._running = True

    async def fake_get() -> TickerUpdate:
        client._running = False
        raise TimeoutError()

    client._tick_queue.get = fake_get
    gen = client.subscribe_ticks(["EUR-PLN"])
    with pytest.raises(StopAsyncIteration):
        await gen.__anext__()


@pytest.mark.asyncio()
async def test_subscribe_candles_handles_timeout_and_cancel() -> None:
    """Verify subscribe_candles handles timeout and cancel.

    Given: A running client with timeout then cancel on queue,
    When: Iteration occurs,
    Then: CancelledError propagates after timeout retry.
    """
    client = WalutomatExchangeClient()
    client._running = True
    client._polling_task = cast(Any, SimpleNamespace(done=lambda: False))
    client._candle_builder_task = cast(Any, SimpleNamespace(done=lambda: False))
    client._candle_queue.get = AsyncMock(side_effect=[TimeoutError(), asyncio.CancelledError()])
    gen = client.subscribe_candles(["EUR-PLN"])
    with pytest.raises(asyncio.CancelledError):
        await gen.__anext__()


@pytest.mark.asyncio()
async def test_subscribe_candles_starts_tasks(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify subscribe_candles starts polling and builder tasks.

    Given: A running client without active tasks,
    When: subscribe_candles() is called,
    Then: Polling and candle builder tasks are started.
    """
    client = WalutomatExchangeClient()
    client._running = True
    poll_started = asyncio.Event()
    builder_started = asyncio.Event()

    async def fake_polling(_symbols: list[str]) -> None:
        poll_started.set()

    async def fake_builder() -> None:
        builder_started.set()

    monkeypatch.setattr(client, "_polling_loop", fake_polling)
    monkeypatch.setattr(client, "_candle_builder_loop", fake_builder)
    monkeypatch.setattr(client, "get_supported_pairs", lambda: ["EUR-PLN"])
    gen = client.subscribe_candles(["*"])

    async def _consume_next() -> CandleUpdate:
        return await gen.__anext__()

    next_candle: asyncio.Task[CandleUpdate] = asyncio.create_task(_consume_next())
    await asyncio.wait_for(poll_started.wait(), timeout=0.1)
    await asyncio.wait_for(builder_started.wait(), timeout=0.1)
    await client._candle_queue.put(
        CandleUpdate(
            symbol="EUR-PLN",
            open=1.0,
            high=1.0,
            low=1.0,
            close=1.0,
            volume=0.0,
            vwap=1.0,
            trades=1,
            interval_begin=datetime.now(UTC),
            interval=1,
        )
    )
    candle = await next_candle
    assert candle.symbol == "EUR-PLN"
    client._running = False
    with pytest.raises(StopAsyncIteration):
        await gen.__anext__()


@pytest.mark.asyncio()
async def test_subscribe_instruments_propagates_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify subscribe_instruments propagates errors.

    Given: A running client with fetch error,
    When: subscribe_instruments() is iterated,
    Then: ValueError is propagated.
    """
    client = WalutomatExchangeClient()
    client._running = True
    monkeypatch.setattr(client, "_fetch_market_data", AsyncMock(side_effect=ValueError("boom")))
    with pytest.raises(ValueError):
        gen = client.subscribe_instruments()
        await gen.__anext__()


@pytest.mark.asyncio()
async def test_create_order_failure_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify create_order raises on API failure.

    Given: A connected client with API returning failure,
    When: create_order() is called,
    Then: RuntimeError is raised.
    """
    client = WalutomatExchangeClient(
        api_key="key",
        private_key_data=_generate_private_key_pem(),
    )
    stub_client = StubAsyncClient(post_responses=[StubResponse({"success": False, "error": "bad"})])
    client._http_client = cast(httpx.AsyncClient, stub_client)
    monkeypatch.setattr(client, "_get_auth_headers", lambda *_args, **_kwargs: {"X-API-Key": "key"})
    request = ExchangeOrderRequest(
        symbol="EUR-PLN",
        side=OrderSideEnum.BUY,
        type=OrderTypeEnum.LIMIT,
        amount=1.0,
        price=4.2,
    )
    with pytest.raises(RuntimeError):
        await client.create_order(request)


@pytest.mark.asyncio()
async def test_cancel_order_not_connected() -> None:
    """Verify cancel_order requires connection.

    Given: An unconnected client with credentials,
    When: cancel_order() is called,
    Then: RuntimeError is raised.
    """
    client = WalutomatExchangeClient(api_key="key", private_key_data=_generate_private_key_pem())
    with pytest.raises(RuntimeError, match="Not connected"):
        await client.cancel_order("abc")


@pytest.mark.asyncio()
async def test_cancel_order_failure_response(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify cancel_order raises on API failure.

    Given: A connected client with API returning failure,
    When: cancel_order() is called,
    Then: RuntimeError is raised.
    """
    client = WalutomatExchangeClient(
        api_key="key",
        private_key_data=_generate_private_key_pem(),
    )
    stub_client = StubAsyncClient(post_responses=[StubResponse({"success": False})])
    client._http_client = cast(httpx.AsyncClient, stub_client)
    monkeypatch.setattr(client, "_get_auth_headers", lambda *_args, **_kwargs: {"X-API-Key": "key"})
    with pytest.raises(RuntimeError, match="cancellation failed"):
        await client.cancel_order("abc")


@pytest.mark.asyncio()
async def test_get_order_requires_auth() -> None:
    """Verify get_order requires authentication.

    Given: A connected client without credentials,
    When: get_order() is called,
    Then: RuntimeError is raised.
    """
    client = WalutomatExchangeClient()
    client._http_client = cast(httpx.AsyncClient, StubAsyncClient())
    with pytest.raises(RuntimeError, match="Trading requires authentication"):
        await client.get_order("abc")


@pytest.mark.asyncio()
async def test_get_orders_filters_and_requires_auth(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify get_orders filters by symbol and status.

    Given: A connected client with multiple orders,
    When: get_orders() is called with symbol and status filters,
    Then: Filtered orders are returned.
    """
    client = WalutomatExchangeClient(api_key="key", private_key_data=_generate_private_key_pem())
    stub_client = StubAsyncClient(
        get_responses=[
            StubResponse(
                {
                    "success": True,
                    "result": [
                        {
                            "orderId": "1",
                            "currencyPair": "EURPLN",
                            "buySell": "BUY",
                            "volume": "1",
                            "limitPrice": "1.1",
                            "status": "ACTIVE",
                            "boughtAmount": "0",
                        },
                        {
                            "orderId": "2",
                            "currencyPair": "USDPLN",
                            "buySell": "SELL",
                            "volume": "2",
                            "limitPrice": "2.2",
                            "status": "CLOSED",
                            "boughtAmount": "1",
                        },
                    ],
                }
            )
        ]
    )
    client._http_client = cast(httpx.AsyncClient, stub_client)
    monkeypatch.setattr(client, "_get_auth_headers", lambda *_args, **_kwargs: {"X-API-Key": "key"})
    orders = await client.get_orders(symbol="EUR-PLN", status=OrderStatusEnum.OPEN)
    assert len(orders) == 1
    assert orders[0].symbol == "EUR-PLN"


@pytest.mark.asyncio()
async def test_get_orders_failure_response(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify get_orders raises on API failure.

    Given: A connected client with API returning failure,
    When: get_orders() is called,
    Then: RuntimeError is raised.
    """
    client = WalutomatExchangeClient(api_key="key", private_key_data=_generate_private_key_pem())
    stub_client = StubAsyncClient(get_responses=[StubResponse({"success": False})])
    client._http_client = cast(httpx.AsyncClient, stub_client)
    monkeypatch.setattr(client, "_get_auth_headers", lambda *_args, **_kwargs: {"X-API-Key": "key"})
    with pytest.raises(RuntimeError, match="Failed to fetch orders"):
        await client.get_orders()


@pytest.mark.asyncio()
async def test_get_balance_requires_connection_and_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify get_balance connection requirement and failure handling.

    Given: An unconnected client then connected with failure,
    When: get_balance() is called,
    Then: RuntimeError is raised for both cases.
    """
    client = WalutomatExchangeClient(api_key="key")
    with pytest.raises(RuntimeError, match="Not connected"):
        await client.get_balance()
    client._http_client = cast(httpx.AsyncClient, StubAsyncClient())
    monkeypatch.setattr(client, "_get_auth_headers", lambda *_args, **_kwargs: {"X-API-Key": "key"})
    failing_client = StubAsyncClient(get_responses=[StubResponse({"success": False})])
    client._http_client = cast(httpx.AsyncClient, failing_client)
    with pytest.raises(RuntimeError, match="Failed to fetch balances"):
        await client.get_balance()


@pytest.mark.asyncio()
async def test_get_ticker_requires_connection() -> None:
    """Verify get_ticker requires connection.

    Given: An unconnected client,
    When: get_ticker() is called,
    Then: RuntimeError is raised.
    """
    client = WalutomatExchangeClient()
    with pytest.raises(RuntimeError, match="Not connected"):
        await client.get_ticker("EUR/PLN")


@pytest.mark.asyncio()
async def test_disconnect_cancels_candle_builder_task() -> None:
    """Verify disconnect cancels candle builder task.

    Given: A running client with candle builder task,
    When: disconnect() is called,
    Then: Candle builder task is cancelled.
    """
    client = WalutomatExchangeClient()
    client._http_client = cast(httpx.AsyncClient, StubAsyncClient())
    client._running = True
    cancelled = False

    async def fake_builder() -> None:
        nonlocal cancelled
        try:
            await asyncio.sleep(100)
        except asyncio.CancelledError:
            cancelled = True
            raise

    client._candle_builder_task = asyncio.create_task(fake_builder())
    await asyncio.sleep(0.01)
    await client.disconnect()
    assert cancelled or client._candle_builder_task is None


@pytest.mark.asyncio()
async def test_polling_loop_adds_to_new_tick_buffer(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify polling loop creates tick buffer for new symbols.

    Given: A running client with empty tick buffers,
    When: Market data is fetched,
    Then: New buffer entry is created for the symbol.
    """
    client = WalutomatExchangeClient(polling_interval=0.01)
    market_data = [
        {
            "pair": "EUR_PLN",
            "bestOffers": {
                "bid_now": 4.2,
                "ask_now": 4.3,
                "forex_now": 4.25,
            },
        }
    ]
    call_count = 0

    class LimitedStubClient:
        async def get(self, url: str, **kwargs: Any) -> StubResponse:
            nonlocal call_count
            call_count += 1
            if call_count > 1:
                client._running = False
            return StubResponse(market_data)

        async def aclose(self) -> None:
            """No-op async close for test stub."""
            pass

    client._http_client = cast(httpx.AsyncClient, LimitedStubClient())
    client._running = True
    client._tick_buffers.clear()
    await client._polling_loop(["EUR-PLN"])
    assert "EUR-PLN" in client._tick_buffers
    assert len(client._tick_buffers["EUR-PLN"]) > 0


@pytest.mark.asyncio()
async def test_polling_loop_http_error_max_consecutive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify polling loop stops after max consecutive HTTP errors.

    Given: A client with max_consecutive_errors=2,
    When: Two consecutive HTTP errors occur,
    Then: Running flag is set to False.
    """
    client = WalutomatExchangeClient(polling_interval=0.01)
    client._max_consecutive_errors = 2
    error_responses = [
        httpx.HTTPError("Connection failed"),
        httpx.HTTPError("Connection failed"),
    ]
    stub_client = StubAsyncClient(get_responses=error_responses)
    client._http_client = cast(httpx.AsyncClient, stub_client)
    client._running = True
    await client._polling_loop(["EUR-PLN"])
    assert not client._running
    assert client._error_count >= 2


@pytest.mark.asyncio()
async def test_candle_builder_loop_no_ticks_in_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify candle builder handles empty tick window.

    Given: A running client with empty tick buffer,
    When: Candle builder loop runs,
    Then: No candle is emitted.
    """
    client = WalutomatExchangeClient()
    client._running = True
    client._tick_buffers["EUR-PLN"] = []
    iteration = 0
    _real_sleep = asyncio.sleep

    async def fake_sleep(seconds: float) -> None:
        nonlocal iteration
        iteration += 1
        if iteration >= 2:
            client._running = False
        await _real_sleep(0.01)

    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.walutomat.asyncio.sleep",
        fake_sleep,
    )
    await client._candle_builder_loop()
    assert client._candle_queue.empty()


@pytest.mark.asyncio()
async def test_subscribe_candles_restarts_done_builder_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify subscribe_candles restarts completed builder task.

    Given: A running client with completed builder task,
    When: subscribe_candles() is called,
    Then: New builder task is created.
    """
    client = WalutomatExchangeClient(polling_interval=0.01)
    market_data = [
        {
            "pair": "EUR_PLN",
            "bestOffers": {"bid_now": 4.35, "ask_now": 4.36, "forex_now": 4.355},
        }
    ]
    stub_client = StubAsyncClient(get_responses=[StubResponse(market_data)])
    client._http_client = cast(httpx.AsyncClient, stub_client)
    client._running = True
    task_created = False
    original_create_task = asyncio.create_task

    def tracking_create_task(coro: Any, **kwargs: Any) -> asyncio.Task[Any]:
        nonlocal task_created
        task_created = True
        return original_create_task(coro, **kwargs)

    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.walutomat.asyncio.create_task",
        tracking_create_task,
    )
    done_task: asyncio.Task[None] = original_create_task(asyncio.sleep(0))
    await done_task
    client._candle_builder_task = done_task

    async def stop_after_brief() -> None:
        await asyncio.sleep(0.05)
        client._running = False

    original_create_task(stop_after_brief())
    async for _ in client.subscribe_candles(["EUR-PLN"]):
        break
    assert task_created


@pytest.mark.asyncio()
async def test_place_order_with_limit_price(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify create_order includes limitPrice in request.

    Given: A connected client with credentials,
    When: create_order() is called with price,
    Then: Request body includes limitPrice parameter.
    """
    pem = _generate_private_key_pem()
    client = WalutomatExchangeClient(api_key="testkey", private_key_data=pem)
    order_response = {
        "success": True,
        "result": {
            "orderId": "ord-123",
            "submitId": "submit-1",
            "currencyPair": "EURPLN",
            "buySell": "BUY",
            "volume": "100.00",
            "limitPrice": "4.5000",
            "status": "ACTIVE",
            "boughtAmount": "0",
        },
    }
    stub_client = StubAsyncClient(post_responses=[StubResponse(order_response)])
    client._http_client = cast(httpx.AsyncClient, stub_client)
    captured_body: str | None = None

    def capture_headers(endpoint: str, body: str) -> dict[str, str]:
        nonlocal captured_body
        captured_body = body
        return {"X-API-Key": "key"}

    monkeypatch.setattr(client, "_get_auth_headers", capture_headers)
    request = ExchangeOrderRequest(
        symbol="EUR/PLN",
        side=OrderSideEnum.BUY,
        type=OrderTypeEnum.LIMIT,
        amount=100.0,
        price=4.5000,
    )
    await client.create_order(request)
    assert captured_body is not None
    assert "limitPrice=4.5000" in captured_body


@pytest.mark.asyncio()
async def test_cancel_order_requires_connection() -> None:
    """Verify cancel_order requires connection.

    Given: An unconnected client with credentials,
    When: cancel_order() is called,
    Then: RuntimeError is raised.
    """
    pem = _generate_private_key_pem()
    client = WalutomatExchangeClient(api_key="testkey", private_key_data=pem)
    with pytest.raises(RuntimeError, match="Not connected"):
        await client.cancel_order("ord-123", "EUR/PLN")


@pytest.mark.asyncio()
async def test_get_order_requires_connection() -> None:
    """Verify get_order requires connection.

    Given: An unconnected client with credentials,
    When: get_order() is called,
    Then: RuntimeError is raised.
    """
    pem = _generate_private_key_pem()
    client = WalutomatExchangeClient(api_key="testkey", private_key_data=pem)
    with pytest.raises(RuntimeError, match="Not connected"):
        await client.get_order("ord-123")


@pytest.mark.asyncio()
async def test_get_order_requires_api_key() -> None:
    """Verify get_order requires API key.

    Given: A connected client without API key,
    When: get_order() is called,
    Then: RuntimeError is raised.
    """
    client = WalutomatExchangeClient()
    client._http_client = cast(httpx.AsyncClient, StubAsyncClient())
    with pytest.raises(RuntimeError, match="Trading requires authentication"):
        await client.get_order("ord-123")


@pytest.mark.asyncio()
async def test_get_orders_filters_by_symbol_and_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify get_orders filters by symbol.

    Given: A connected client with orders for multiple symbols,
    When: get_orders() is called with symbol filter,
    Then: Only orders for matching symbol are returned.
    """
    pem = _generate_private_key_pem()
    client = WalutomatExchangeClient(api_key="testkey", private_key_data=pem)
    orders_response = {
        "success": True,
        "result": [
            {
                "orderId": "ord-1",
                "submitId": "sub-1",
                "currencyPair": "EURPLN",
                "buySell": "BUY",
                "volume": "100",
                "limitPrice": "4.5",
                "status": "ACTIVE",
                "boughtAmount": "0",
            },
            {
                "orderId": "ord-2",
                "submitId": "sub-2",
                "currencyPair": "USDPLN",
                "buySell": "SELL",
                "volume": "200",
                "limitPrice": "4.0",
                "status": "ACTIVE",
                "boughtAmount": "0",
            },
        ],
    }
    stub_client = StubAsyncClient(get_responses=[StubResponse(orders_response)])
    client._http_client = cast(httpx.AsyncClient, stub_client)
    monkeypatch.setattr(client, "_get_auth_headers", lambda *_args, **_kwargs: {"X-API-Key": "key"})
    orders = await client.get_orders(symbol="EUR-PLN")
    assert len(orders) == 1
    assert orders[0].symbol == "EUR-PLN"


@pytest.mark.asyncio()
async def test_get_orders_applies_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify get_orders applies limit parameter.

    Given: A connected client with 10 orders,
    When: get_orders() is called with limit=3,
    Then: Only 3 orders are returned.
    """
    pem = _generate_private_key_pem()
    client = WalutomatExchangeClient(api_key="testkey", private_key_data=pem)
    orders_response = {
        "success": True,
        "result": [
            {
                "orderId": f"ord-{i}",
                "submitId": f"sub-{i}",
                "currencyPair": "EURPLN",
                "buySell": "BUY",
                "volume": "100",
                "limitPrice": "4.5",
                "status": "ACTIVE",
                "boughtAmount": "0",
            }
            for i in range(10)
        ],
    }
    stub_client = StubAsyncClient(get_responses=[StubResponse(orders_response)])
    client._http_client = cast(httpx.AsyncClient, stub_client)
    monkeypatch.setattr(client, "_get_auth_headers", lambda *_args, **_kwargs: {"X-API-Key": "key"})
    orders = await client.get_orders(limit=3)
    assert len(orders) == 3


def generate_test_rsa_key() -> bytes:
    """Generate a test RSA private key in PEM format."""
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )
    return private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


class TestWalutomatPrivateKeyLoading:
    """Tests for Walutomat private key loading and parsing."""

    def test_pem_plaintext_format(self) -> None:
        """Verify plaintext PEM key is loaded.

        Given: A PEM key in plaintext format,
        When: Client is initialized,
        Then: Private key is loaded successfully.
        """
        pem_bytes = generate_test_rsa_key()
        pem_text = pem_bytes.decode()
        client = WalutomatExchangeClient(
            api_key="test-key",
            private_key_data=pem_text,
        )
        assert client._private_key is not None
        assert hasattr(client._private_key, "sign")

    def test_pem_base64_encoded_format(self) -> None:
        """Verify base64-encoded PEM key is loaded.

        Given: A PEM key encoded as base64,
        When: Client is initialized,
        Then: Private key is loaded successfully.
        """
        pem_bytes = generate_test_rsa_key()
        pem_text = pem_bytes.decode()
        base64_encoded = base64.b64encode(pem_text.encode()).decode()
        client = WalutomatExchangeClient(
            api_key="test-key",
            private_key_data=base64_encoded,
        )
        assert client._private_key is not None
        assert hasattr(client._private_key, "sign")

    def test_pem_with_whitespace(self) -> None:
        """Verify PEM key with whitespace is loaded.

        Given: A PEM key with surrounding whitespace,
        When: Client is initialized,
        Then: Private key is loaded successfully.
        """
        pem_bytes = generate_test_rsa_key()
        pem_text = pem_bytes.decode()
        pem_with_whitespace = f"\n\n  {pem_text}  \n\n"
        client = WalutomatExchangeClient(
            api_key="test-key",
            private_key_data=pem_with_whitespace,
        )
        assert client._private_key is not None

    def test_invalid_base64_format(self) -> None:
        """Verify invalid base64 raises ValueError.

        Given: Invalid base64 string,
        When: Client is initialized,
        Then: ValueError is raised.
        """
        invalid_data = "this-is-not-base64-!!!"
        with pytest.raises(ValueError, match="Invalid private key format"):
            WalutomatExchangeClient(
                api_key="test-key",
                private_key_data=invalid_data,
            )

    def test_corrupted_pem_format(self) -> None:
        """Verify corrupted PEM raises ValueError.

        Given: Corrupted PEM data,
        When: Client is initialized,
        Then: ValueError is raised.
        """
        corrupted_pem = """-----BEGIN PRIVATE KEY-----
MIIJQQIBADANBgkqhkiG9w0BAQEFAA corrupted data here!!!
-----END PRIVATE KEY-----"""
        with pytest.raises(ValueError, match="Failed to load RSA private key"):
            WalutomatExchangeClient(
                api_key="test-key",
                private_key_data=corrupted_pem,
            )

    def test_no_private_key_data(self) -> None:
        """Verify no private key data is handled.

        Given: No private key data provided,
        When: Client is initialized,
        Then: Private key attribute is None.
        """
        client = WalutomatExchangeClient()
        assert client._private_key is None

    def test_both_pem_formats_produce_same_key(self) -> None:
        """Verify plaintext and base64 PEM produce same key.

        Given: Same PEM key in two formats,
        When: Two clients are initialized,
        Then: Both have equivalent private keys.
        """
        pem_bytes = generate_test_rsa_key()
        pem_text = pem_bytes.decode()
        base64_encoded = base64.b64encode(pem_text.encode()).decode()
        client_pem = WalutomatExchangeClient(
            api_key="test-key",
            private_key_data=pem_text,
        )
        client_base64 = WalutomatExchangeClient(
            api_key="test-key",
            private_key_data=base64_encoded,
        )
        assert client_pem._private_key is not None
        assert client_base64._private_key is not None
        pem_numbers = client_pem._private_key.private_numbers()
        base64_numbers = client_base64._private_key.private_numbers()
        assert pem_numbers.public_numbers.n == base64_numbers.public_numbers.n
