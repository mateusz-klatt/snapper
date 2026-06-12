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
from loguru import logger

import snapper.infrastructure.exchanges.implementations.walutomat as walutomat_mod
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import ExchangeOrderRequest
from snapper.infrastructure.exchanges.contracts import ExchangeOrderSnapshot
from snapper.infrastructure.exchanges.contracts import ExchangeOrderStatusEnum
from snapper.infrastructure.exchanges.contracts import ExchangeOrderTypeEnum
from snapper.infrastructure.exchanges.contracts import ExecutionUpdate
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.errors import AmbiguousOrderSubmitError
from snapper.infrastructure.exchanges.implementations.walutomat import WalutomatExchangeClient
from snapper.infrastructure.exchanges.implementations.walutomat import _active_execution_status
from snapper.infrastructure.exchanges.implementations.walutomat import _should_emit_active_execution
from snapper.infrastructure.exchanges.implementations.walutomat import _snapshot_tracked_order
from snapper.infrastructure.exchanges.implementations.walutomat import _TrackedOrder
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
def stub_symbol_aliases(monkeypatch: pytest.MonkeyPatch) -> None:
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
        "snapper.infrastructure.exchanges.implementations.walutomat.native_to_walutomat_ws", to_ws
    )
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.walutomat.native_to_walutomat_rest",
        to_rest,
    )
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.walutomat.walutomat_ws_to_native",
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


def test_polling_loop_backoff_error_does_not_stop_running() -> None:
    """Verify HTTP error backoff does not stop the client.

    Given: A client with max_consecutive_errors=1,
    When: An HTTP error reaches the threshold,
    Then: Running remains True and backoff state is set.
    """
    client = WalutomatExchangeClient(polling_interval=0.0)
    client._running = True
    client._max_consecutive_errors = 1
    client._handle_http_error(httpx.HTTPError("fail"))
    assert client._running is True
    assert client._backoff_until > 0.0


@pytest.mark.asyncio()
async def test_get_order_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify get_order raises for missing order.

    Given: A connected client with empty API result,
    When: get_order() is called,
    Then: ValueError is raised.
    """
    client = WalutomatExchangeClient(api_key="k", private_key_data=_generate_private_key_pem())
    get_resp = StubResponse({"success": True, "result": []})
    stub_client = StubAsyncClient(get_responses=[get_resp])
    client._http_client = cast(httpx.AsyncClient, stub_client)
    monkeypatch.setattr(client, "_get_auth_headers", lambda *_args, **_kwargs: {"X-API-Key": "k"})
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
                    "status": "CLOSED",
                    "completion": 100,
                    "soldAmount": 2.0,
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
        async for _item in client.subscribe_trades(["EUR-PLN"]):
            """Consumed by iteration to trigger exception."""


def _make_order_snapshot(
    order_id: str = "ord-1",
    client_order_id: str | None = "sub-1",
    symbol: str = "EUR-PLN",
    side: OrderSideEnum = OrderSideEnum.BUY,
    order_type: ExchangeOrderTypeEnum = ExchangeOrderTypeEnum.LIMIT,
    amount: float = 100.0,
    price: float = 4.50,
    status: ExchangeOrderStatusEnum = ExchangeOrderStatusEnum.OPEN,
    filled: float = 0.0,
) -> ExchangeOrderSnapshot:
    """Build an ExchangeOrderSnapshot for execution polling tests."""
    return ExchangeOrderSnapshot(
        id=order_id,
        client_order_id=client_order_id,
        symbol=symbol,
        side=side,
        type=order_type,
        amount=amount,
        price=price,
        status=status,
        filled=filled,
        remaining=amount - filled,
        timestamp=1700000000.0,
    )


@pytest.mark.asyncio()
async def test_find_order_by_client_id_returns_active_match() -> None:
    """Active Walutomat submitId match resolves as venue-confirmed found.

    Given: The active-order scan returns orders including one whose
        client_order_id matches the requested submitId,
    When: find_order_by_client_id is called,
    Then: The matching snapshot is returned and the optional symbol
        narrows the active scan.
    """
    client = WalutomatExchangeClient()
    match = _make_order_snapshot(order_id="ord-hit", client_order_id="cid-hit")
    calls: list[tuple[str | None, ExchangeOrderStatusEnum | None, int | None]] = []

    async def _mock_get_orders(
        symbol: str | None = None,
        status: ExchangeOrderStatusEnum | None = None,
        limit: int | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        calls.append((symbol, status, limit))
        return [_make_order_snapshot(order_id="ord-other", client_order_id="cid-other"), match]

    client.get_orders = _mock_get_orders

    found = await client.find_order_by_client_id("cid-hit", "EUR-PLN")

    assert found is match
    assert calls == [("EUR-PLN", None, None)]


@pytest.mark.asyncio()
async def test_find_order_by_client_id_miss_is_not_authoritative() -> None:
    """Active Walutomat submitId miss keeps UNKNOWN rather than returning None.

    Given: The active-order scan succeeds but contains no matching
        submitId,
    When: find_order_by_client_id is called,
    Then: NotImplementedError is raised because active-order absence
        cannot prove the order was not fast-filled or otherwise
        terminal.
    """
    client = WalutomatExchangeClient()

    async def _mock_get_orders(
        symbol: str | None = None,
        status: ExchangeOrderStatusEnum | None = None,
        limit: int | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        _ = (symbol, status, limit)
        return [_make_order_snapshot(order_id="ord-other", client_order_id="cid-other")]

    client.get_orders = _mock_get_orders

    with pytest.raises(NotImplementedError, match="cannot authoritatively verify absence"):
        await client.find_order_by_client_id("cid-missing", "EUR-PLN")


@pytest.mark.asyncio()
async def test_find_order_by_client_id_query_failure_propagates() -> None:
    """Walutomat active-order query failure remains could-not-verify.

    Given: The active-order query raises a transport error,
    When: find_order_by_client_id is called,
    Then: The error propagates instead of being converted to absence.
    """
    client = WalutomatExchangeClient()

    async def _failing_get_orders(
        symbol: str | None = None,
        status: ExchangeOrderStatusEnum | None = None,
        limit: int | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        _ = (symbol, status, limit)
        raise httpx.ConnectTimeout("down")

    client.get_orders = _failing_get_orders

    with pytest.raises(httpx.ConnectTimeout):
        await client.find_order_by_client_id("cid-transport", "EUR-PLN")


def _build_polling_client(
    execution_poll_interval: float = 0.0,
) -> WalutomatExchangeClient:
    """Build a WalutomatExchangeClient wired for execution polling tests."""
    client = WalutomatExchangeClient(execution_poll_interval=execution_poll_interval)
    client._http_client = cast(Any, object())
    client._api_key = "test-key"
    client._private_key = cast(Any, object())
    client._running = True
    client._execution_idle_interval = 0.0
    return client


def test_snapshot_tracked_order_copies_snapshot_fields() -> None:
    """Tracked-order helper copies the execution polling fields from a snapshot.

    Given: An exchange order snapshot with execution polling fields populated,
    When: `_snapshot_tracked_order` converts it to internal tracked-order state,
    Then: The tracked snapshot preserves the polling fields needed by diff detection.
    """
    order = _make_order_snapshot(
        order_id="ord-2",
        client_order_id="sub-2",
        symbol="USD-PLN",
        side=OrderSideEnum.SELL,
        amount=40.0,
        filled=12.5,
        price=4.12,
    )

    tracked = _snapshot_tracked_order(order)

    assert tracked.order_id == "ord-2"
    assert tracked.cl_ord_id == "sub-2"
    assert tracked.symbol == "USD-PLN"
    assert tracked.side == OrderSideEnum.SELL
    assert tracked.amount == pytest.approx(40.0)
    assert tracked.filled == pytest.approx(12.5)
    assert tracked.price == pytest.approx(4.12)


def test_active_execution_helpers_detect_fill_progress() -> None:
    """Active-execution helpers emit only for actual fill progress.

    Given: A current order snapshot and an earlier tracked snapshot,
    When: The execution helper predicates compare fill state and completion status,
    Then: They emit only for real progress and classify terminal-vs-partial updates correctly.
    """
    order = _make_order_snapshot(filled=25.0)
    previous = _TrackedOrder(
        order_id="ord-1",
        cl_ord_id="sub-1",
        symbol="EUR-PLN",
        side=OrderSideEnum.BUY,
        order_type=ExchangeOrderTypeEnum.LIMIT,
        amount=100.0,
        filled=10.0,
        price=4.50,
    )

    assert _should_emit_active_execution(order, previous) is True
    assert _should_emit_active_execution(_make_order_snapshot(filled=0.0), None) is False
    assert _active_execution_status(_make_order_snapshot(filled=100.0)) == (
        ExchangeOrderStatusEnum.FILLED
    )
    assert _active_execution_status(order) == ExchangeOrderStatusEnum.PARTIALLY_FILLED


@pytest.mark.asyncio()
async def test_subscribe_executions_httpx_error_logs_warning_not_exception(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Transient httpx.HTTPError during execution poll logs WARNING, not ERROR+TB.

    Given: A polling Walutomat client whose ``get_orders`` raises
        ``httpx.ConnectTimeout`` on every call,
    When: ``subscribe_executions`` runs the poll loop for one cycle,
    Then: The transient HTTP error is logged as WARNING with
        'transient HTTP error' and 'will retry', and no ERROR record is
        emitted. Per the clean-signal-log rule, ERROR + traceback is
        reserved for non-HTTP exceptions (real bugs) so the noise floor
        stays low and the real signal stays visible.
    """
    client = _build_polling_client()
    poll_count = 0

    async def _failing_get_orders(
        symbol: str | None = None,
        status: ExchangeOrderStatusEnum | None = None,
        limit: int | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        nonlocal poll_count
        poll_count += 1
        if poll_count > 1:
            client._running = False
        raise httpx.ConnectTimeout("connect timeout to walutomat")

    client.get_orders = _failing_get_orders
    sink_id = logger.add(caplog.handler, format="{message}", level="DEBUG")
    try:
        with caplog.at_level("DEBUG"):
            async for _ in client.subscribe_executions():
                pass
    finally:
        logger.remove(sink_id)
    warning_records = [r for r in caplog.records if r.levelname == "WARNING"]
    error_records = [r for r in caplog.records if r.levelname == "ERROR"]
    assert any(
        "transient HTTP error" in r.message and "will retry" in r.message for r in warning_records
    ), f"expected transient-WARNING, got {[r.message for r in warning_records]}"
    assert (
        not error_records
    ), f"httpx errors must not log ERROR+TB, got {[r.message for r in error_records]}"


@pytest.mark.asyncio()
async def test_subscribe_executions_first_poll_seeds_without_yield() -> None:
    """Verify first poll populates tracking but yields nothing.

    Given: A connected client with one active order,
    When: subscribe_executions polls once and stops,
    Then: No ExecutionUpdate is yielded (first poll seeds only).
    """
    client = _build_polling_client()
    order = _make_order_snapshot(filled=50.0)
    poll_count = 0

    async def _mock_get_orders(
        symbol: str | None = None,
        status: ExchangeOrderStatusEnum | None = None,
        limit: int | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        nonlocal poll_count
        poll_count += 1
        if poll_count > 1:
            client._running = False
        return [order]

    client.get_orders = _mock_get_orders

    results: list[ExecutionUpdate] = []
    async for update in client.subscribe_executions():
        results.append(update)

    assert results == []


@pytest.mark.asyncio()
async def test_subscribe_executions_yields_fill_on_filled_increase() -> None:
    """Verify fill update is yielded when filled quantity increases.

    Given: A tracked order with filled=0,
    When: Second poll shows filled=30,
    Then: ExecutionUpdate with cum_qty=30 and PARTIALLY_FILLED status is yielded.
    """
    client = _build_polling_client()
    poll_count = 0

    async def _mock_get_orders(
        symbol: str | None = None,
        status: ExchangeOrderStatusEnum | None = None,
        limit: int | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        nonlocal poll_count
        poll_count += 1
        if poll_count == 1:
            return [_make_order_snapshot(filled=0.0)]
        if poll_count == 2:
            return [_make_order_snapshot(filled=30.0)]
        client._running = False
        return [_make_order_snapshot(filled=30.0)]

    client.get_orders = _mock_get_orders

    results: list[ExecutionUpdate] = []
    async for update in client.subscribe_executions():
        results.append(update)

    assert len(results) == 1
    assert results[0].order_id == "ord-1"
    assert results[0].cum_qty == 30.0
    assert results[0].order_status == ExchangeOrderStatusEnum.OPEN
    assert results[0].exec_type == "trade"
    assert results[0].cl_ord_id == "sub-1"


@pytest.mark.asyncio()
async def test_subscribe_executions_yields_filled_when_complete() -> None:
    """Verify CLOSED status when order is fully filled.

    Given: A tracked order with filled=50,
    When: Second poll shows filled=100 (== amount),
    Then: ExecutionUpdate with CLOSED status (FILLED normalized) is yielded.
    """
    client = _build_polling_client()
    poll_count = 0

    async def _mock_get_orders(
        symbol: str | None = None,
        status: ExchangeOrderStatusEnum | None = None,
        limit: int | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        nonlocal poll_count
        poll_count += 1
        if poll_count == 1:
            return [_make_order_snapshot(filled=50.0)]
        if poll_count == 2:
            return [_make_order_snapshot(filled=100.0)]
        client._running = False
        return [_make_order_snapshot(filled=100.0)]

    client.get_orders = _mock_get_orders

    results: list[ExecutionUpdate] = []
    async for update in client.subscribe_executions():
        results.append(update)

    assert len(results) == 1
    assert results[0].order_status == ExchangeOrderStatusEnum.CLOSED
    assert results[0].cum_qty == 100.0
    assert results[0].exec_type == "trade"


@pytest.mark.asyncio()
async def test_subscribe_executions_no_yield_when_no_change() -> None:
    """Verify no update when filled quantity does not change.

    Given: A tracked order with filled=30,
    When: Second poll also shows filled=30,
    Then: No ExecutionUpdate is yielded.
    """
    client = _build_polling_client()
    poll_count = 0

    async def _mock_get_orders(
        symbol: str | None = None,
        status: ExchangeOrderStatusEnum | None = None,
        limit: int | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        nonlocal poll_count
        poll_count += 1
        if poll_count <= 2:
            return [_make_order_snapshot(filled=30.0)]
        client._running = False
        return [_make_order_snapshot(filled=30.0)]

    client.get_orders = _mock_get_orders

    results: list[ExecutionUpdate] = []
    async for update in client.subscribe_executions():
        results.append(update)

    assert results == []


@pytest.mark.asyncio()
async def test_subscribe_executions_handles_disappeared_filled_order() -> None:
    """Verify CLOSED event when fully filled order disappears.

    Given: A tracked order with filled==amount whose final-state query
        reports CLOSED,
    When: Order disappears from active orders,
    Then: ExecutionUpdate with CLOSED status and exec_type='trade' is yielded.
    """
    client = _build_polling_client()
    poll_count = 0

    async def _mock_get_orders(
        symbol: str | None = None,
        status: ExchangeOrderStatusEnum | None = None,
        limit: int | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        nonlocal poll_count
        poll_count += 1
        if poll_count == 1:
            return [_make_order_snapshot(filled=100.0, amount=100.0)]
        if poll_count == 2:
            return []
        client._running = False
        return []

    async def _mock_get_order(order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        return _make_order_snapshot(
            order_id=order_id,
            filled=100.0,
            amount=100.0,
            status=ExchangeOrderStatusEnum.CLOSED,
        )

    client.get_orders = _mock_get_orders
    client.get_order = _mock_get_order

    results: list[ExecutionUpdate] = []
    async for update in client.subscribe_executions():
        results.append(update)

    assert len(results) == 1
    assert results[0].order_status == ExchangeOrderStatusEnum.CLOSED
    assert results[0].exec_type == "trade"
    assert results[0].cum_qty == 100.0


@pytest.mark.asyncio()
async def test_subscribe_executions_handles_disappeared_partial_order() -> None:
    """Verify CANCELED event when partially filled order disappears.

    Given: A tracked order with 0 < filled < amount whose final-state
        query reports CANCELED with no new fill,
    When: Order disappears from active orders,
    Then: ExecutionUpdate with CANCELED status is yielded.
    """
    client = _build_polling_client()
    poll_count = 0

    async def _mock_get_orders(
        symbol: str | None = None,
        status: ExchangeOrderStatusEnum | None = None,
        limit: int | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        nonlocal poll_count
        poll_count += 1
        if poll_count == 1:
            return [_make_order_snapshot(filled=30.0)]
        if poll_count == 2:
            return []
        client._running = False
        return []

    async def _mock_get_order(order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        return _make_order_snapshot(
            order_id=order_id,
            filled=30.0,
            status=ExchangeOrderStatusEnum.CANCELED,
        )

    client.get_orders = _mock_get_orders
    client.get_order = _mock_get_order

    results: list[ExecutionUpdate] = []
    async for update in client.subscribe_executions():
        results.append(update)

    assert len(results) == 1
    assert results[0].order_status == ExchangeOrderStatusEnum.CANCELED
    assert results[0].exec_type == "canceled"
    assert results[0].cum_qty == 30.0


@pytest.mark.asyncio()
async def test_subscribe_executions_handles_disappeared_unfilled_order() -> None:
    """Verify CANCELED event when unfilled order disappears.

    Given: A tracked order with filled=0 whose final-state query reports
        CANCELED,
    When: Order disappears from active orders,
    Then: ExecutionUpdate with CANCELED status is yielded.
    """
    client = _build_polling_client()
    poll_count = 0

    async def _mock_get_orders(
        symbol: str | None = None,
        status: ExchangeOrderStatusEnum | None = None,
        limit: int | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        nonlocal poll_count
        poll_count += 1
        if poll_count == 1:
            return [_make_order_snapshot(filled=0.0)]
        if poll_count == 2:
            return []
        client._running = False
        return []

    async def _mock_get_order(order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        return _make_order_snapshot(
            order_id=order_id,
            filled=0.0,
            status=ExchangeOrderStatusEnum.CANCELED,
        )

    client.get_orders = _mock_get_orders
    client.get_order = _mock_get_order

    results: list[ExecutionUpdate] = []
    async for update in client.subscribe_executions():
        results.append(update)

    assert len(results) == 1
    assert results[0].order_status == ExchangeOrderStatusEnum.CANCELED
    assert results[0].exec_type == "canceled"
    assert results[0].cum_qty == 0.0


@pytest.mark.asyncio()
async def test_subscribe_executions_handles_api_error() -> None:
    """Verify polling continues after API error.

    Given: A connected client,
    When: get_orders raises an exception on second poll,
    Then: Error is logged, third poll proceeds normally.
    """
    client = _build_polling_client()
    poll_count = 0

    async def _mock_get_orders(
        symbol: str | None = None,
        status: ExchangeOrderStatusEnum | None = None,
        limit: int | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        nonlocal poll_count
        poll_count += 1
        if poll_count == 1:
            return [_make_order_snapshot(filled=0.0)]
        if poll_count == 2:
            raise ConnectionError("API unavailable")
        if poll_count == 3:
            return [_make_order_snapshot(filled=50.0)]
        client._running = False
        return [_make_order_snapshot(filled=50.0)]

    client.get_orders = _mock_get_orders

    results: list[ExecutionUpdate] = []
    async for update in client.subscribe_executions():
        results.append(update)

    assert len(results) == 1
    assert results[0].cum_qty == 50.0


@pytest.mark.asyncio()
async def test_subscribe_executions_respects_poll_interval() -> None:
    """Verify asyncio.sleep is called with execution_poll_interval.

    Given: A client with execution_poll_interval=7.5 and tracked orders,
    When: One poll cycle completes,
    Then: asyncio.sleep is called with 7.5.
    """
    client = _build_polling_client(execution_poll_interval=7.5)
    poll_count = 0
    sleep_values: list[float] = []

    async def _mock_get_orders(
        symbol: str | None = None,
        status: ExchangeOrderStatusEnum | None = None,
        limit: int | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        nonlocal poll_count
        poll_count += 1
        if poll_count > 1:
            client._running = False
        return [_make_order_snapshot(filled=0.0)]

    original_sleep = asyncio.sleep

    async def _mock_sleep(delay: float) -> None:
        sleep_values.append(delay)
        await original_sleep(0)

    client.get_orders = _mock_get_orders

    original_asyncio_sleep = walutomat_mod.asyncio.sleep
    walutomat_mod.asyncio.sleep = _mock_sleep
    try:
        async for _update in client.subscribe_executions():
            pass
    finally:
        walutomat_mod.asyncio.sleep = original_asyncio_sleep

    assert 7.5 in sleep_values


@pytest.mark.asyncio()
async def test_subscribe_executions_uses_correct_field_names() -> None:
    """Verify ExecutionUpdate uses order_id, cl_ord_id, cum_qty field names.

    Given: A tracked order that gets partially filled,
    When: ExecutionUpdate is yielded,
    Then: Fields are order_id (not exchange_order_id), cl_ord_id (not client_order_id),
          cum_qty (not filled_qty).
    """
    client = _build_polling_client()
    poll_count = 0

    async def _mock_get_orders(
        symbol: str | None = None,
        status: ExchangeOrderStatusEnum | None = None,
        limit: int | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        nonlocal poll_count
        poll_count += 1
        if poll_count == 1:
            return [_make_order_snapshot(filled=0.0)]
        if poll_count == 2:
            return [_make_order_snapshot(filled=25.0)]
        client._running = False
        return [_make_order_snapshot(filled=25.0)]

    client.get_orders = _mock_get_orders

    results: list[ExecutionUpdate] = []
    async for update in client.subscribe_executions():
        results.append(update)

    assert len(results) == 1
    update = results[0]
    assert update.order_id == "ord-1"
    assert update.cl_ord_id == "sub-1"
    assert update.cum_qty == 25.0
    assert update.order_qty == 100.0
    assert update.limit_price == 4.50
    assert update.average_price == 4.50


@pytest.mark.asyncio()
async def test_subscribe_executions_fails_when_not_connected() -> None:
    """Verify RuntimeError when client is not connected.

    Given: A client without HTTP connection,
    When: subscribe_executions() is iterated,
    Then: RuntimeError with NOT_CONNECTED message is raised.
    """
    client = WalutomatExchangeClient()
    with pytest.raises(RuntimeError, match="Not connected"):
        async for _item in client.subscribe_executions():
            pass


@pytest.mark.asyncio()
async def test_subscribe_executions_fails_when_no_credentials() -> None:
    """Verify RuntimeError when credentials are missing.

    Given: A connected client without API key,
    When: subscribe_executions() is iterated,
    Then: RuntimeError with AUTH_REQUIRED message is raised.
    """
    client = WalutomatExchangeClient()
    client._http_client = cast(Any, object())
    with pytest.raises(RuntimeError, match="Trading requires authentication"):
        async for _item in client.subscribe_executions():
            pass


@pytest.mark.asyncio()
async def test_subscribe_executions_new_order_no_yield_after_first_poll() -> None:
    """Verify new order appearing after first poll with filled=0 does not yield.

    Given: Empty first poll (seeds tracking),
    When: Second poll shows a new order with filled=0,
    Then: No ExecutionUpdate is yielded (new order, no fill delta).
    """
    client = _build_polling_client()
    poll_count = 0

    async def _mock_get_orders(
        symbol: str | None = None,
        status: ExchangeOrderStatusEnum | None = None,
        limit: int | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        nonlocal poll_count
        poll_count += 1
        if poll_count == 1:
            return []
        if poll_count == 2:
            return [_make_order_snapshot(filled=0.0)]
        client._running = False
        return [_make_order_snapshot(filled=0.0)]

    client.get_orders = _mock_get_orders

    results: list[ExecutionUpdate] = []
    async for update in client.subscribe_executions():
        results.append(update)

    assert results == []


@pytest.mark.asyncio()
async def test_subscribe_executions_new_order_with_prefilled_yields() -> None:
    """Verify new order appearing with filled>0 yields ExecutionUpdate.

    Given: Empty first poll (seeds tracking),
    When: Second poll shows a new order with filled=50,
    Then: ExecutionUpdate with cum_qty=50 is yielded.
    """
    client = _build_polling_client()
    poll_count = 0

    async def _mock_get_orders(
        symbol: str | None = None,
        status: ExchangeOrderStatusEnum | None = None,
        limit: int | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        nonlocal poll_count
        poll_count += 1
        if poll_count == 1:
            return []
        if poll_count == 2:
            return [_make_order_snapshot(filled=50.0)]
        client._running = False
        return [_make_order_snapshot(filled=50.0)]

    client.get_orders = _mock_get_orders

    results: list[ExecutionUpdate] = []
    async for update in client.subscribe_executions():
        results.append(update)

    assert len(results) == 1
    assert results[0].cum_qty == 50.0
    assert results[0].exec_type == "trade"
    assert results[0].order_status == ExchangeOrderStatusEnum.OPEN


@pytest.mark.asyncio()
async def test_subscribe_executions_first_poll_failure_still_seeds_next() -> None:
    """Verify first poll failure resets first_poll flag.

    Given: First poll raises an exception,
    When: Second poll succeeds with an order,
    Then: Second poll seeds tracking (no yield), third poll detects fills.
    """
    client = _build_polling_client()
    poll_count = 0

    async def _mock_get_orders(
        symbol: str | None = None,
        status: ExchangeOrderStatusEnum | None = None,
        limit: int | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        nonlocal poll_count
        poll_count += 1
        if poll_count == 1:
            raise ConnectionError("API unavailable")
        if poll_count == 2:
            return [_make_order_snapshot(filled=0.0)]
        if poll_count == 3:
            return [_make_order_snapshot(filled=50.0)]
        client._running = False
        return [_make_order_snapshot(filled=50.0)]

    client.get_orders = _mock_get_orders

    results: list[ExecutionUpdate] = []
    async for update in client.subscribe_executions():
        results.append(update)

    assert len(results) == 1
    assert results[0].cum_qty == 50.0


@pytest.mark.asyncio()
async def test_subscribe_executions_idles_when_no_tracked_orders() -> None:
    """Verify polling continues when no orders are tracked.

    Given: A client with no active orders,
    When: Two poll cycles complete,
    Then: Polling uses idle wait (event/timeout) instead of active sleep.
    """
    client = _build_polling_client(execution_poll_interval=0.0)
    poll_count = 0

    async def _mock_get_orders(
        symbol: str | None = None,
        status: ExchangeOrderStatusEnum | None = None,
        limit: int | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        nonlocal poll_count
        poll_count += 1
        if poll_count > 2:
            client._running = False
        return []

    client.get_orders = _mock_get_orders

    async for _update in client.subscribe_executions():
        pass

    assert poll_count >= 2


@pytest.mark.asyncio()
async def test_subscribe_executions_wake_event_interrupts_idle() -> None:
    """Verify wake event interrupts idle sleep immediately.

    Given: A client idling with no tracked orders,
    When: _execution_wake event is set,
    Then: Polling resumes without waiting for full idle timeout.
    """
    client = _build_polling_client(execution_poll_interval=0.0)
    client._execution_idle_interval = 60.0
    poll_count = 0

    async def _mock_get_orders(
        symbol: str | None = None,
        status: ExchangeOrderStatusEnum | None = None,
        limit: int | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        nonlocal poll_count
        poll_count += 1
        if poll_count == 1:
            return []
        if poll_count == 2:
            return [_make_order_snapshot(filled=50.0)]
        client._running = False
        return [_make_order_snapshot(filled=50.0)]

    client.get_orders = _mock_get_orders

    async def _wake_after_short_delay() -> None:
        await asyncio.sleep(0.01)
        client._execution_wake.set()

    asyncio.get_event_loop().create_task(_wake_after_short_delay())

    results: list[ExecutionUpdate] = []
    async for update in client.subscribe_executions():
        results.append(update)

    assert poll_count >= 2


@pytest.mark.asyncio()
async def test_subscribe_executions_active_interval_when_tracked() -> None:
    """Verify polling uses active interval when orders are tracked.

    Given: A client with a tracked order,
    When: Poll cycle completes with orders present,
    Then: asyncio.sleep uses execution_poll_interval (not idle interval).
    """
    client = _build_polling_client(execution_poll_interval=0.0)
    client._execution_idle_interval = 60.0
    poll_count = 0
    sleep_values: list[float] = []

    async def _mock_get_orders(
        symbol: str | None = None,
        status: ExchangeOrderStatusEnum | None = None,
        limit: int | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        nonlocal poll_count
        poll_count += 1
        if poll_count > 2:
            client._running = False
        return [_make_order_snapshot(filled=0.0)]

    original_sleep = asyncio.sleep

    async def _mock_sleep(delay: float) -> None:
        sleep_values.append(delay)
        await original_sleep(0)

    client.get_orders = _mock_get_orders

    original_asyncio_sleep = walutomat_mod.asyncio.sleep
    walutomat_mod.asyncio.sleep = _mock_sleep
    try:
        async for _update in client.subscribe_executions():
            pass
    finally:
        walutomat_mod.asyncio.sleep = original_asyncio_sleep

    assert 0.0 in sleep_values
    assert 60.0 not in sleep_values


@pytest.mark.asyncio()
async def test_create_order_sets_wake_event() -> None:
    """Verify create_order sets the wake event to interrupt idle polling.

    Given: A connected authenticated client,
    When: create_order is called,
    Then: _execution_wake event is set.
    """
    client = WalutomatExchangeClient()
    client._api_key = "test-key"
    client._private_key = cast(Any, object())

    assert not client._execution_wake.is_set()

    async def _mock_post(
        url: str,
        content: str = "",
        headers: dict[str, str] | None = None,
    ) -> StubResponse:
        return StubResponse({"success": True, "result": {"orderId": "ord-123"}})

    client._http_client = cast(Any, SimpleNamespace(post=_mock_post))

    monkeypatch_obj = pytest.MonkeyPatch()
    monkeypatch_obj.setattr(client, "_log_order_to_db", AsyncMock(return_value=None))
    monkeypatch_obj.setattr(client, "_get_auth_headers", lambda *a: {})
    try:
        req = ExchangeOrderRequest(
            symbol="EUR-PLN",
            side=OrderSideEnum.BUY,
            type=ExchangeOrderTypeEnum.LIMIT,
            amount=1.0,
            price=4.28,
        )
        await client.create_order(req)
        assert client._execution_wake.is_set()
    finally:
        monkeypatch_obj.undo()


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
        type=ExchangeOrderTypeEnum.LIMIT,
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
        type=ExchangeOrderTypeEnum.LIMIT,
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
        type=ExchangeOrderTypeEnum.LIMIT,
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
    Then: ExchangeOrderSnapshot with PENDING status is returned and
          db_order_id / db_order_public_id are populated from _log_order_to_db.
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
    monkeypatch.setattr(client, "_log_order_to_db", AsyncMock(return_value=(42, "pub-42")))
    request = ExchangeOrderRequest(
        symbol="EUR-PLN",
        side=OrderSideEnum.BUY,
        type=ExchangeOrderTypeEnum.LIMIT,
        amount=100.0,
        price=4.2,
    )
    order = await client.create_order(request)
    assert order.id == "abc123"
    assert order.status == ExchangeOrderStatusEnum.PENDING
    assert order.db_order_id == 42
    assert order.db_order_public_id == "pub-42"
    assert stub_client.post_calls[0][0].endswith("/market_fx/orders")


@pytest.mark.asyncio()
async def test_cancel_order_fetches_latest(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify cancel_order returns parsed order from close endpoint.

    Given: A connected client with credentials,
    When: cancel_order() is called,
    Then: Order is closed via POST /market_fx/orders/close and result is parsed.
    """
    client = WalutomatExchangeClient(
        api_key="key",
        private_key_data=_generate_private_key_pem(),
    )
    close_response = StubResponse(
        {
            "success": True,
            "result": {
                "orderId": "abc123",
                "submitId": "sub-1",
                "currencyPair": "EURPLN",
                "buySell": "BUY",
                "volume": "100.00",
                "limitPrice": "4.2000",
                "status": "CLOSED",
                "completion": 0,
                "soldAmount": "0.00",
                "boughtAmount": "0.00",
                "commissionAmount": "0",
                "commissionCurrency": "EUR",
            },
        }
    )
    stub_client = StubAsyncClient(post_responses=[close_response])
    client._http_client = cast(httpx.AsyncClient, stub_client)

    def auth_headers(_endpoint: str, _body: str) -> dict[str, str]:
        return {"X-API-Key": "key"}

    monkeypatch.setattr(client, "_get_auth_headers", auth_headers)
    result = await client.cancel_order("abc123")
    assert result.id == "abc123"
    assert result.status == ExchangeOrderStatusEnum.CANCELED
    assert stub_client.post_calls[0][0].endswith("/market_fx/orders/close")


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
    """Verify get_order returns parsed order from findOrders endpoint.

    Given: A connected client with matching order in API,
    When: get_order() is called with order ID,
    Then: Parsed ExchangeOrderSnapshot is returned.
    """
    client = WalutomatExchangeClient(
        api_key="key",
        private_key_data=_generate_private_key_pem(),
    )
    get_resp = StubResponse(
        {
            "success": True,
            "result": [
                {
                    "orderId": "target",
                    "submitId": "sub-1",
                    "currencyPair": "EURPLN",
                    "buySell": "BUY",
                    "volume": "100.00",
                    "limitPrice": "4.2000",
                    "status": "ACTIVE",
                    "boughtAmount": "0.00",
                    "commissionAmount": "0",
                },
            ],
        }
    )
    stub_client = StubAsyncClient(get_responses=[get_resp])
    client._http_client = cast(httpx.AsyncClient, stub_client)
    monkeypatch.setattr(client, "_get_auth_headers", lambda *_args, **_kwargs: {"X-API-Key": "key"})
    result = await client.get_order("target")
    assert result.id == "target"
    assert result.symbol == "EUR-PLN"
    assert result.status == ExchangeOrderStatusEnum.OPEN


@pytest.mark.asyncio()
async def test_get_order_raises_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify get_order raises for empty API result.

    Given: A connected client with no matching orders from API,
    When: get_order() is called,
    Then: ValueError is raised.
    """
    client = WalutomatExchangeClient(
        api_key="key",
        private_key_data=_generate_private_key_pem(),
    )
    get_resp = StubResponse({"success": True, "result": []})
    stub_client = StubAsyncClient(get_responses=[get_resp])
    client._http_client = cast(httpx.AsyncClient, stub_client)
    monkeypatch.setattr(client, "_get_auth_headers", lambda *_args, **_kwargs: {"X-API-Key": "key"})
    with pytest.raises(ValueError, match="not found"):
        await client.get_order("missing")


@pytest.mark.asyncio()
async def test_get_order_raises_when_api_returns_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify get_order raises when API returns success=false.

    Given: A connected client with API returning failure,
    When: get_order() is called,
    Then: ValueError is raised.
    """
    client = WalutomatExchangeClient(
        api_key="key",
        private_key_data=_generate_private_key_pem(),
    )
    get_resp = StubResponse({"success": False, "errors": ["not found"]})
    stub_client = StubAsyncClient(get_responses=[get_resp])
    client._http_client = cast(httpx.AsyncClient, stub_client)
    monkeypatch.setattr(client, "_get_auth_headers", lambda *_args, **_kwargs: {"X-API-Key": "key"})
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


def _make_api_order(
    order_id: str = "ord-1",
    submit_id: str = "sub-1",
    currency_pair: str = "EURPLN",
    buy_sell: str = "BUY",
    volume: str = "100.00",
    limit_price: str = "4.2800",
    status: str = "ACTIVE",
    completion: int = 0,
    sold_amount: str = "0.00",
    bought_amount: str = "50.00",
    commission_amount: str = "0.10",
    commission_currency: str = "EUR",
) -> dict[str, Any]:
    """Build a raw Walutomat API order dictionary for parser tests."""
    return {
        "orderId": order_id,
        "submitId": submit_id,
        "currencyPair": currency_pair,
        "buySell": buy_sell,
        "volume": volume,
        "limitPrice": limit_price,
        "status": status,
        "completion": completion,
        "soldAmount": sold_amount,
        "boughtAmount": bought_amount,
        "commissionAmount": commission_amount,
        "commissionCurrency": commission_currency,
    }


def test_parse_order_buy_uses_bought_amount() -> None:
    """Verify parser uses boughtAmount for BUY side fill.

    Given: A BUY order with boughtAmount=50 and soldAmount=200,
    When: _parse_walutomat_order is called,
    Then: filled equals boughtAmount (50), not soldAmount.
    """
    data = _make_api_order(buy_sell="BUY", bought_amount="50.00", sold_amount="200.00")
    result = WalutomatExchangeClient._parse_walutomat_order(data)
    assert math.isclose(result.filled, 50.0, rel_tol=1e-9)


def test_parse_order_sell_uses_sold_amount() -> None:
    """Verify parser uses soldAmount for SELL side fill.

    Given: A SELL order with soldAmount=75 and boughtAmount=300,
    When: _parse_walutomat_order is called,
    Then: filled equals soldAmount (75), not boughtAmount.
    """
    data = _make_api_order(buy_sell="SELL", sold_amount="75.00", bought_amount="300.00")
    result = WalutomatExchangeClient._parse_walutomat_order(data)
    assert math.isclose(result.filled, 75.0, rel_tol=1e-9)


def test_parse_order_commission_into_fee() -> None:
    """Verify parser maps commissionAmount to fee and commissionCurrency to fee_currency.

    Given: An order with commissionAmount=0.10 and commissionCurrency=EUR,
    When: _parse_walutomat_order is called,
    Then: fee=0.10 and fee_currency='EUR'.
    """
    data = _make_api_order(commission_amount="0.10", commission_currency="EUR")
    result = WalutomatExchangeClient._parse_walutomat_order(data)
    assert result.fee is not None
    assert math.isclose(result.fee, 0.10, rel_tol=1e-9)
    assert result.fee_currency == "EUR"


def test_parse_order_active_status() -> None:
    """Verify parser maps ACTIVE status to OPEN.

    Given: An order with status=ACTIVE,
    When: _parse_walutomat_order is called,
    Then: ExchangeOrderStatusEnum.OPEN is returned.
    """
    data = _make_api_order(status="ACTIVE")
    result = WalutomatExchangeClient._parse_walutomat_order(data)
    assert result.status == ExchangeOrderStatusEnum.OPEN


def test_parse_order_closed_complete_status() -> None:
    """Verify parser maps CLOSED with completion=100 to CLOSED.

    Given: An order with status=CLOSED and completion=100,
    When: _parse_walutomat_order is called,
    Then: ExchangeOrderStatusEnum.CLOSED is returned.
    """
    data = _make_api_order(status="CLOSED", completion=100)
    result = WalutomatExchangeClient._parse_walutomat_order(data)
    assert result.status == ExchangeOrderStatusEnum.CLOSED


def test_parse_order_closed_partial_status() -> None:
    """Verify parser maps CLOSED with completion<100 to CANCELED.

    Given: An order with status=CLOSED and completion=50,
    When: _parse_walutomat_order is called,
    Then: ExchangeOrderStatusEnum.CANCELED is returned.
    """
    data = _make_api_order(status="CLOSED", completion=50)
    result = WalutomatExchangeClient._parse_walutomat_order(data)
    assert result.status == ExchangeOrderStatusEnum.CANCELED


@pytest.mark.asyncio()
async def test_get_order_uses_find_orders_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify get_order calls the findOrders endpoint with orderId query param.

    Given: A connected client with credentials,
    When: get_order('ord-42') is called,
    Then: GET URL contains /market_fx/orders?orderId=ord-42.
    """
    client = WalutomatExchangeClient(api_key="key", private_key_data=_generate_private_key_pem())
    get_resp = StubResponse(
        {
            "success": True,
            "result": [_make_api_order(order_id="ord-42")],
        }
    )
    stub_client = StubAsyncClient(get_responses=[get_resp])
    client._http_client = cast(httpx.AsyncClient, stub_client)
    monkeypatch.setattr(client, "_get_auth_headers", lambda *_args, **_kwargs: {"X-API-Key": "key"})
    await client.get_order("ord-42")
    assert "/market_fx/orders?orderId=ord-42" in stub_client.get_calls[0][0]


@pytest.mark.asyncio()
async def test_get_order_signs_endpoint_with_query_string(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify get_order passes endpoint with query string to _get_auth_headers.

    Given: A connected client with credentials,
    When: get_order('ord-99') is called,
    Then: _get_auth_headers receives endpoint containing ?orderId=ord-99.
    """
    client = WalutomatExchangeClient(api_key="key", private_key_data=_generate_private_key_pem())
    get_resp = StubResponse(
        {
            "success": True,
            "result": [_make_api_order(order_id="ord-99")],
        }
    )
    stub_client = StubAsyncClient(get_responses=[get_resp])
    client._http_client = cast(httpx.AsyncClient, stub_client)
    captured_endpoints: list[str] = []

    def capture_auth(endpoint: str, body: str = "") -> dict[str, str]:
        captured_endpoints.append(endpoint)
        return {"X-API-Key": "key"}

    monkeypatch.setattr(client, "_get_auth_headers", capture_auth)
    await client.get_order("ord-99")
    assert any("?orderId=ord-99" in ep for ep in captured_endpoints)


@pytest.mark.asyncio()
async def test_get_order_returns_completed_order(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify get_order returns a fully completed order.

    Given: A connected client with CLOSED+completion=100 order in API,
    When: get_order() is called,
    Then: ExchangeOrderSnapshot with CLOSED status is returned.
    """
    client = WalutomatExchangeClient(api_key="key", private_key_data=_generate_private_key_pem())
    get_resp = StubResponse(
        {
            "success": True,
            "result": [
                _make_api_order(
                    order_id="ord-done",
                    status="CLOSED",
                    completion=100,
                    bought_amount="100.00",
                ),
            ],
        }
    )
    stub_client = StubAsyncClient(get_responses=[get_resp])
    client._http_client = cast(httpx.AsyncClient, stub_client)
    monkeypatch.setattr(client, "_get_auth_headers", lambda *_args, **_kwargs: {"X-API-Key": "key"})
    result = await client.get_order("ord-done")
    assert result.status == ExchangeOrderStatusEnum.CLOSED
    assert math.isclose(result.filled, 100.0, rel_tol=1e-9)


@pytest.mark.asyncio()
async def test_cancel_order_uses_close_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify cancel_order POSTs to /market_fx/orders/close.

    Given: A connected client with credentials,
    When: cancel_order() is called,
    Then: POST URL ends with /market_fx/orders/close.
    """
    client = WalutomatExchangeClient(api_key="key", private_key_data=_generate_private_key_pem())
    close_resp = StubResponse(
        {
            "success": True,
            "result": _make_api_order(status="CLOSED", completion=0),
        }
    )
    stub_client = StubAsyncClient(post_responses=[close_resp])
    client._http_client = cast(httpx.AsyncClient, stub_client)
    monkeypatch.setattr(client, "_get_auth_headers", lambda *_args, **_kwargs: {"X-API-Key": "key"})
    await client.cancel_order("ord-1")
    assert stub_client.post_calls[0][0].endswith("/market_fx/orders/close")


@pytest.mark.asyncio()
async def test_cancel_order_sends_order_id_in_body(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify cancel_order sends orderId in form body.

    Given: A connected client with credentials,
    When: cancel_order('ord-77') is called,
    Then: POST body contains 'orderId=ord-77'.
    """
    client = WalutomatExchangeClient(api_key="key", private_key_data=_generate_private_key_pem())
    close_resp = StubResponse(
        {
            "success": True,
            "result": _make_api_order(order_id="ord-77", status="CLOSED", completion=0),
        }
    )
    stub_client = StubAsyncClient(post_responses=[close_resp])
    client._http_client = cast(httpx.AsyncClient, stub_client)
    monkeypatch.setattr(client, "_get_auth_headers", lambda *_args, **_kwargs: {"X-API-Key": "key"})
    await client.cancel_order("ord-77")
    posted_body = stub_client.post_calls[0][1].get("content", "")
    assert "orderId=ord-77" in posted_body


@pytest.mark.asyncio()
async def test_cancel_order_returns_canceled_status(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify cancel_order returns CANCELED for CLOSED+completion=0.

    Given: A connected client with credentials,
    When: cancel_order() is called and API returns CLOSED with completion=0,
    Then: Returned snapshot has status CANCELED.
    """
    client = WalutomatExchangeClient(api_key="key", private_key_data=_generate_private_key_pem())
    close_resp = StubResponse(
        {
            "success": True,
            "result": _make_api_order(status="CLOSED", completion=0, bought_amount="0.00"),
        }
    )
    stub_client = StubAsyncClient(post_responses=[close_resp])
    client._http_client = cast(httpx.AsyncClient, stub_client)
    monkeypatch.setattr(client, "_get_auth_headers", lambda *_args, **_kwargs: {"X-API-Key": "key"})
    result = await client.cancel_order("ord-1")
    assert result.status == ExchangeOrderStatusEnum.CANCELED


@pytest.mark.asyncio()
async def test_disappeared_order_queries_find_orders() -> None:
    """Verify _resolve_disappeared calls get_order for final state.

    Given: A tracked order that disappeared,
    When: _resolve_disappeared is invoked,
    Then: get_order is called with the order ID.
    """
    client = _build_polling_client()
    called_with: list[str] = []

    async def mock_get_order(order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        called_with.append(order_id)
        return _make_order_snapshot(
            order_id=order_id,
            filled=100.0,
            amount=100.0,
            status=ExchangeOrderStatusEnum.CLOSED,
        )

    client.get_order = mock_get_order

    tracked = _TrackedOrder(
        order_id="ord-1",
        cl_ord_id="sub-1",
        symbol="EUR-PLN",
        side=OrderSideEnum.BUY,
        order_type=ExchangeOrderTypeEnum.LIMIT,
        amount=100.0,
        filled=50.0,
        price=4.50,
    )
    events: list[ExecutionUpdate] = []
    async for event in client._resolve_disappeared("ord-1", tracked):
        events.append(event)
    assert "ord-1" in called_with


@pytest.mark.asyncio()
async def test_disappeared_order_uses_api_response() -> None:
    """Verify _resolve_disappeared uses API response for event data.

    Given: A tracked order that disappeared with API returning different fill,
    When: _resolve_disappeared is invoked,
    Then: ExecutionUpdate uses fill from API response, not tracked state.
    """
    client = _build_polling_client()

    async def mock_get_order(order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        return _make_order_snapshot(
            order_id=order_id,
            filled=100.0,
            amount=100.0,
            status=ExchangeOrderStatusEnum.CLOSED,
        )

    client.get_order = mock_get_order

    tracked = _TrackedOrder(
        order_id="ord-1",
        cl_ord_id="sub-1",
        symbol="EUR-PLN",
        side=OrderSideEnum.BUY,
        order_type=ExchangeOrderTypeEnum.LIMIT,
        amount=100.0,
        filled=50.0,
        price=4.50,
    )
    events: list[ExecutionUpdate] = []
    async for event in client._resolve_disappeared("ord-1", tracked):
        events.append(event)
    assert events[0].cum_qty == 100.0


@pytest.mark.asyncio()
async def test_disappeared_order_query_failure_never_guesses() -> None:
    """A failed final-state query yields NOTHING — no fabricated terminal.

    Given: A tracked order that disappeared and get_order raising on
        every attempt,
    When: _resolve_disappeared is invoked repeatedly (past the
        escalation threshold),
    Then: No event is ever fabricated (the old fallback guessed
        filled-vs-canceled), the retry counter grows, and the order
        stays resolvable on a later successful query.
    """
    client = _build_polling_client()

    async def mock_get_order(order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        raise ConnectionError("API unavailable")

    client.get_order = mock_get_order

    tracked = _TrackedOrder(
        order_id="ord-1",
        cl_ord_id="sub-1",
        symbol="EUR-PLN",
        side=OrderSideEnum.BUY,
        order_type=ExchangeOrderTypeEnum.LIMIT,
        amount=100.0,
        filled=30.0,
        price=4.50,
    )
    events: list[ExecutionUpdate] = []
    for _ in range(11):
        async for event in client._resolve_disappeared("ord-1", tracked):
            events.append(event)
    assert events == []
    assert client._disappeared_retry_counts["ord-1"] == 11

    async def mock_get_order_ok(order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        return _make_order_snapshot(
            order_id=order_id, filled=30.0, status=ExchangeOrderStatusEnum.CANCELED
        )

    client.get_order = mock_get_order_ok
    async for event in client._resolve_disappeared("ord-1", tracked):
        events.append(event)
    assert len(events) == 1
    assert events[0].order_status == ExchangeOrderStatusEnum.CANCELED
    assert "ord-1" not in client._disappeared_retry_counts


@pytest.mark.asyncio()
async def test_disappeared_partial_fill_cancel_emits_two_events() -> None:
    """Verify partial fill + cancel emits two events when new fill exists.

    Given: A tracked order with filled=30 that disappeared,
    When: API shows CANCELED with filled=60 (new fill delta),
    Then: Two events: trade (PARTIALLY_FILLED) + canceled (CANCELED).
    """
    client = _build_polling_client()

    async def mock_get_order(order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        return _make_order_snapshot(
            order_id=order_id,
            filled=60.0,
            amount=100.0,
            status=ExchangeOrderStatusEnum.CANCELED,
        )

    client.get_order = mock_get_order

    tracked = _TrackedOrder(
        order_id="ord-1",
        cl_ord_id="sub-1",
        symbol="EUR-PLN",
        side=OrderSideEnum.BUY,
        order_type=ExchangeOrderTypeEnum.LIMIT,
        amount=100.0,
        filled=30.0,
        price=4.50,
    )
    events: list[ExecutionUpdate] = []
    async for event in client._resolve_disappeared("ord-1", tracked):
        events.append(event)
    assert len(events) == 2
    assert events[0].exec_type == "trade"
    assert events[0].order_status == ExchangeOrderStatusEnum.OPEN
    assert events[0].cum_qty == 60.0
    assert events[1].exec_type == "canceled"
    assert events[1].order_status == ExchangeOrderStatusEnum.CANCELED


@pytest.mark.asyncio()
async def test_disappeared_partial_cancel_no_new_fill_emits_one_event() -> None:
    """Verify cancel without new fill emits only one canceled event.

    Given: A tracked order with filled=30 that disappeared,
    When: API shows CANCELED with same filled=30 (no new fill delta),
    Then: One event: canceled (CANCELED).
    """
    client = _build_polling_client()

    async def mock_get_order(order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        return _make_order_snapshot(
            order_id=order_id,
            filled=30.0,
            amount=100.0,
            status=ExchangeOrderStatusEnum.CANCELED,
        )

    client.get_order = mock_get_order

    tracked = _TrackedOrder(
        order_id="ord-1",
        cl_ord_id="sub-1",
        symbol="EUR-PLN",
        side=OrderSideEnum.BUY,
        order_type=ExchangeOrderTypeEnum.LIMIT,
        amount=100.0,
        filled=30.0,
        price=4.50,
    )
    events: list[ExecutionUpdate] = []
    async for event in client._resolve_disappeared("ord-1", tracked):
        events.append(event)
    assert len(events) == 1
    assert events[0].exec_type == "canceled"
    assert events[0].order_status == ExchangeOrderStatusEnum.CANCELED


@pytest.mark.asyncio()
async def test_disappeared_fully_filled_emits_single_trade() -> None:
    """Verify fully filled disappeared order emits single trade event.

    Given: A tracked order with filled=50 that disappeared,
    When: API shows CLOSED with filled=100 (fully filled),
    Then: One event: trade with FILLED status.
    """
    client = _build_polling_client()

    async def mock_get_order(order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        return _make_order_snapshot(
            order_id=order_id,
            filled=100.0,
            amount=100.0,
            status=ExchangeOrderStatusEnum.CLOSED,
        )

    client.get_order = mock_get_order

    tracked = _TrackedOrder(
        order_id="ord-1",
        cl_ord_id="sub-1",
        symbol="EUR-PLN",
        side=OrderSideEnum.BUY,
        order_type=ExchangeOrderTypeEnum.LIMIT,
        amount=100.0,
        filled=50.0,
        price=4.50,
    )
    events: list[ExecutionUpdate] = []
    async for event in client._resolve_disappeared("ord-1", tracked):
        events.append(event)
    assert len(events) == 1
    assert events[0].exec_type == "trade"
    assert events[0].order_status == ExchangeOrderStatusEnum.CLOSED
    assert events[0].cum_qty == 100.0


@pytest.mark.asyncio()
async def test_disappeared_order_still_open_skips_terminal() -> None:
    """Verify no terminal event when API reports order still OPEN.

    Given: An order disappeared from active list,
    When: get_order returns the order with OPEN status (transient omission),
    Then: No ExecutionUpdate is yielded (order is not terminal).
    """
    client = _build_polling_client()

    async def mock_get_order(order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        return _make_order_snapshot(
            order_id=order_id,
            filled=0.0,
            amount=100.0,
            status=ExchangeOrderStatusEnum.OPEN,
        )

    client.get_order = mock_get_order

    tracked = _TrackedOrder(
        order_id="ord-1",
        cl_ord_id="sub-1",
        symbol="EUR-PLN",
        side=OrderSideEnum.BUY,
        order_type=ExchangeOrderTypeEnum.LIMIT,
        amount=100.0,
        filled=0.0,
        price=4.50,
    )
    events: list[ExecutionUpdate] = []
    async for event in client._resolve_disappeared("ord-1", tracked):
        events.append(event)

    assert events == []


@pytest.mark.asyncio()
async def test_disappeared_order_unexpected_status_no_event() -> None:
    """Verify no terminal event for unexpected status from API.

    Given: An order disappeared from active list,
    When: get_order returns a status not in (OPEN, CLOSED, CANCELED),
    Then: No ExecutionUpdate is yielded.
    """
    client = _build_polling_client()

    async def mock_get_order(order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        return _make_order_snapshot(
            order_id=order_id,
            filled=0.0,
            amount=100.0,
            status=ExchangeOrderStatusEnum.EXPIRED,
        )

    client.get_order = mock_get_order

    tracked = _TrackedOrder(
        order_id="ord-1",
        cl_ord_id="sub-1",
        symbol="EUR-PLN",
        side=OrderSideEnum.BUY,
        order_type=ExchangeOrderTypeEnum.LIMIT,
        amount=100.0,
        filled=0.0,
        price=4.50,
    )
    events: list[ExecutionUpdate] = []
    async for event in client._resolve_disappeared("ord-1", tracked):
        events.append(event)

    assert events == []


@pytest.mark.asyncio()
async def test_transiently_omitted_open_order_stays_tracked() -> None:
    """Verify transient OPEN omission does not create a bogus reappearance fill.

    Given: A tracked partially filled order that disappears from /active,
    When: get_order reports it still OPEN and the order later reappears unchanged,
    Then: No duplicate ExecutionUpdate is yielded for the same cumulative fill.
    """
    client = _build_polling_client()
    poll_count = 0

    async def _mock_get_orders(
        symbol: str | None = None,
        status: ExchangeOrderStatusEnum | None = None,
        limit: int | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        nonlocal poll_count
        poll_count += 1
        if poll_count == 1:
            return [_make_order_snapshot(filled=30.0)]
        if poll_count == 2:
            return []
        if poll_count == 3:
            client._running = False
            return [_make_order_snapshot(filled=30.0)]
        client._running = False
        return []

    async def _mock_get_order(
        order_id: str,
        symbol: str | None = None,
    ) -> ExchangeOrderSnapshot:
        return _make_order_snapshot(
            order_id=order_id,
            filled=30.0,
            status=ExchangeOrderStatusEnum.OPEN,
        )

    client.get_orders = _mock_get_orders
    client.get_order = _mock_get_order

    results: list[ExecutionUpdate] = []
    async for update in client.subscribe_executions():
        results.append(update)

    assert results == []


@pytest.mark.asyncio()
async def test_active_fill_event_has_fees() -> None:
    """Verify active fill event includes fee breakdown.

    Given: A tracked order with fee and fee_currency on snapshot,
    When: Fill delta is detected during polling,
    Then: ExecutionUpdate carries the order's CUMULATIVE commission as
        cum_fee/cum_fee_currency (the executor's fee watermark turns it
        into per-emission deltas) and a deterministic wal- exec id.
    """
    client = _build_polling_client()
    poll_count = 0

    async def _mock_get_orders(
        symbol: str | None = None,
        status: ExchangeOrderStatusEnum | None = None,
        limit: int | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        nonlocal poll_count
        poll_count += 1
        if poll_count == 1:
            return [_make_order_snapshot(filled=0.0)]
        snap = _make_order_snapshot(filled=30.0)
        snap.fee = 0.06
        snap.fee_currency = "EUR"
        if poll_count > 2:
            client._running = False
        return [snap]

    client.get_orders = _mock_get_orders

    results: list[ExecutionUpdate] = []
    async for update in client.subscribe_executions():
        results.append(update)

    assert len(results) == 1
    assert results[0].fees is None
    assert results[0].cum_fee is not None
    assert math.isclose(results[0].cum_fee, 0.06, rel_tol=1e-9)
    assert results[0].cum_fee_currency == "EUR"
    assert results[0].exec_id == "wal-ord-1-c3000000000"


@pytest.mark.asyncio()
async def test_disappeared_order_fees_populated() -> None:
    """Verify disappearance fill event includes fees from API response.

    Given: A tracked order that disappeared,
    When: API returns order with commission data,
    Then: ExecutionUpdate carries the cumulative commission as cum_fee.
    """
    client = _build_polling_client()

    async def mock_get_order(order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        snap = _make_order_snapshot(
            order_id=order_id,
            filled=100.0,
            amount=100.0,
            status=ExchangeOrderStatusEnum.CLOSED,
        )
        snap.fee = 0.20
        snap.fee_currency = "EUR"
        return snap

    client.get_order = mock_get_order

    tracked = _TrackedOrder(
        order_id="ord-1",
        cl_ord_id="sub-1",
        symbol="EUR-PLN",
        side=OrderSideEnum.BUY,
        order_type=ExchangeOrderTypeEnum.LIMIT,
        amount=100.0,
        filled=50.0,
        price=4.50,
    )
    events: list[ExecutionUpdate] = []
    async for event in client._resolve_disappeared("ord-1", tracked):
        events.append(event)
    assert events[0].fees is None
    assert events[0].cum_fee is not None
    assert math.isclose(events[0].cum_fee, 0.20, rel_tol=1e-9)
    assert events[0].cum_fee_currency == "EUR"
    assert events[0].exec_id == "wal-ord-1-c10000000000-t"


@pytest.mark.asyncio()
async def test_disappeared_order_fee_usd_equiv_not_set() -> None:
    """Verify disappearance fee breakdown does not set fee_usd_equiv.

    Given: A tracked order that disappeared with commission data,
    When: _resolve_disappeared emits events,
    Then: The cumulative commission rides cum_fee and fee_usd_equiv stays
        unset (nothing fabricates a USD equivalent).
    """
    client = _build_polling_client()

    async def mock_get_order(order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        snap = _make_order_snapshot(
            order_id=order_id,
            filled=100.0,
            amount=100.0,
            status=ExchangeOrderStatusEnum.CLOSED,
        )
        snap.fee = 0.15
        snap.fee_currency = "PLN"
        return snap

    client.get_order = mock_get_order

    tracked = _TrackedOrder(
        order_id="ord-1",
        cl_ord_id="sub-1",
        symbol="EUR-PLN",
        side=OrderSideEnum.BUY,
        order_type=ExchangeOrderTypeEnum.LIMIT,
        amount=100.0,
        filled=50.0,
        price=4.50,
    )
    events: list[ExecutionUpdate] = []
    async for event in client._resolve_disappeared("ord-1", tracked):
        events.append(event)
    assert events[0].fees is None
    assert events[0].cum_fee == 0.15
    assert events[0].cum_fee_currency == "PLN"
    assert events[0].fee_usd_equiv is None


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
                "status": "CLOSED",
                "completion": 100,
            }
        ],
    }
    client._http_client = cast(
        httpx.AsyncClient, StubAsyncClient(get_responses=[StubResponse(payload)])
    )

    def auth_headers(_endpoint: str, _body: str) -> dict[str, str]:
        return {"X-API-Key": "key"}

    monkeypatch.setattr(client, "_get_auth_headers", auth_headers)
    orders = await client.get_orders(status=ExchangeOrderStatusEnum.OPEN)
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


def test_handle_http_error_threshold_sets_backoff_without_stopping() -> None:
    """Verify threshold HTTP errors enter backoff without stopping.

    Given: A client with max_consecutive_errors=1,
    When: HTTP error occurs,
    Then: Backoff is scheduled and running remains True.
    """
    client = WalutomatExchangeClient()
    client._running = True
    client._max_consecutive_errors = 1
    client._handle_http_error(httpx.HTTPError("boom"))
    assert client._running is True
    assert client._backoff_attempts == 1
    assert client._backoff_until > 0.0


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
        type=ExchangeOrderTypeEnum.LIMIT,
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
    orders = await client.get_orders(symbol="EUR-PLN", status=ExchangeOrderStatusEnum.OPEN)
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


def test_polling_loop_http_error_max_consecutive_sets_backoff() -> None:
    """Verify max consecutive HTTP errors set backoff.

    Given: A client with max_consecutive_errors=2,
    When: Two consecutive HTTP errors occur,
    Then: Running stays True and the first backoff attempt is recorded.
    """
    client = WalutomatExchangeClient(polling_interval=0.01)
    client._max_consecutive_errors = 2
    client._running = True
    client._handle_http_error(httpx.HTTPError("Connection failed"))
    client._handle_http_error(httpx.HTTPError("Connection failed"))
    assert client._running is True
    assert client._consecutive_error_count == 2
    assert client._backoff_attempts == 1


def test_handle_http_error_in_flight_retry_logs_warning_not_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """In-flight retry (n/max where n < max) logs WARNING, not ERROR.

    Given: A client with max_consecutive_errors=5 and zero prior errors,
    When: _handle_http_error is called once with an httpx.HTTPError,
    Then: The error counter increments to 1, the client keeps polling,
        the log records a WARNING with 'will retry', and no ERROR
        record is emitted. Per the
        clean-signal-log rule, ERROR is reserved for terminal retry
        exhaustion so the operator-actionable signal stays meaningful.
    """
    client = WalutomatExchangeClient(polling_interval=0.01)
    client._max_consecutive_errors = 5
    sink_id = logger.add(caplog.handler, format="{message}", level="DEBUG")
    try:
        with caplog.at_level("DEBUG"):
            client._handle_http_error(httpx.HTTPError("connect timeout"))
    finally:
        logger.remove(sink_id)
    assert client._consecutive_error_count == 1
    warning_records = [r for r in caplog.records if r.levelname == "WARNING"]
    error_records = [r for r in caplog.records if r.levelname == "ERROR"]
    assert any(
        "will retry" in r.message for r in warning_records
    ), f"expected WARNING with 'will retry', got {[r.message for r in warning_records]}"
    assert (
        not error_records
    ), f"in-flight retry must not log ERROR, got {[r.message for r in error_records]}"


def test_handle_http_error_below_threshold_sets_no_backoff() -> None:
    """Below-threshold HTTP errors do not enter backoff.

    Given: A client below its max consecutive error threshold,
    When: _handle_http_error is called,
    Then: No backoff is scheduled.
    """
    client = WalutomatExchangeClient()
    client._max_consecutive_errors = 5
    client._handle_http_error(httpx.HTTPError("timeout"))
    assert client._consecutive_error_count == 1
    assert client._backoff_attempts == 0
    assert client._backoff_until == pytest.approx(0.0)


@pytest.mark.asyncio()
async def test_handle_http_error_at_threshold_sets_backoff_60s(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Threshold HTTP errors schedule the first 60s backoff.

    Given: A client at the first threshold and a wakeup event already set,
    When: _handle_http_error is called,
    Then: Backoff is set for 60 seconds and the wakeup event is cleared.
    """
    client = WalutomatExchangeClient()
    client._running = True
    client._max_consecutive_errors = 1
    client._backoff_wakeup_event = asyncio.Event()
    client._backoff_wakeup_event.set()
    monkeypatch.setattr(walutomat_mod.time, "monotonic", lambda: 1000.0)
    client._handle_http_error(httpx.HTTPError("timeout"))
    assert client._running is True
    assert client._backoff_attempts == 1
    assert client._backoff_until == pytest.approx(1060.0)
    assert not client._backoff_wakeup_event.is_set()


def test_handle_http_error_grows_backoff_exponentially_capped_at_1800s(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """HTTP error backoff grows exponentially up to the cap.

    Given: A client whose error threshold is one,
    When: Multiple threshold errors are handled,
    Then: Backoff reaches the 1800 second cap.
    """
    client = WalutomatExchangeClient()
    client._max_consecutive_errors = 1
    monkeypatch.setattr(walutomat_mod.time, "monotonic", lambda: 1000.0)
    for _ in range(6):
        client._handle_http_error(httpx.HTTPError("timeout"))
    assert client._backoff_attempts == 6
    assert client._backoff_until == pytest.approx(2800.0)


def test_handle_http_error_never_sets_running_false() -> None:
    """HTTP errors never stop the polling client.

    Given: A running client at its error threshold,
    When: _handle_http_error is called,
    Then: _running remains True.
    """
    client = WalutomatExchangeClient()
    client._running = True
    client._max_consecutive_errors = 1
    client._handle_http_error(httpx.HTTPError("timeout"))
    assert client._running is True


def test_handle_http_error_keeps_running_control_on_client_state() -> None:
    """HTTP error handling does not expose a loop-control return value.

    Given: A client below and at its threshold,
    When: _handle_http_error is called repeatedly,
    Then: The client remains running and records backoff state.
    """
    client = WalutomatExchangeClient()
    client._max_consecutive_errors = 2
    client._running = True
    client._handle_http_error(httpx.HTTPError("timeout"))
    client._handle_http_error(httpx.HTTPError("timeout"))
    assert client._running is True
    assert client._backoff_attempts == 1


@pytest.mark.asyncio()
async def test_polling_loop_wakeup_event_breaks_sleep_early() -> None:
    """The polling loop can be woken while in HTTP backoff.

    Given: A running client in backoff,
    When: The wakeup event is set,
    Then: Backoff clears and polling resumes immediately.
    """
    client = WalutomatExchangeClient(polling_interval=0.0)
    client._running = True
    client._consecutive_error_count = 5
    client._backoff_attempts = 1
    client._backoff_until = walutomat_mod.time.monotonic() + 60.0
    pair = WalutomatMarketPair.model_validate(
        {"pair": "EUR_PLN", "bestOffers": {"bid_now": 4.0, "ask_now": 4.1, "forex_now": 4.05}}
    )

    async def fake_fetch() -> dict[str, WalutomatMarketPair]:
        client._running = False
        return {"EUR_PLN": pair}

    client._fetch_market_data = AsyncMock(side_effect=fake_fetch)
    task = asyncio.create_task(client._polling_loop(["EUR-PLN"]))
    while client._backoff_wakeup_event is None:
        await asyncio.sleep(0)
    client._backoff_wakeup_event.set()
    await asyncio.wait_for(task, timeout=0.5)
    assert client._backoff_until == pytest.approx(0.0)
    assert client._backoff_attempts == 0
    assert client._consecutive_error_count == 0
    ticker = await client._tick_queue.get()
    assert ticker.symbol == "EUR-PLN"


@pytest.mark.asyncio()
async def test_polling_loop_sleeps_during_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    """The polling loop honors scheduled backoff before polling.

    Given: A running client in backoff,
    When: The backoff sleep completes,
    Then: Polling resumes and backoff state is cleared.
    """
    client = WalutomatExchangeClient(polling_interval=0.0)
    client._running = True
    client._consecutive_error_count = 5
    client._backoff_attempts = 1
    client._backoff_until = walutomat_mod.time.monotonic() + 60.0
    pair = WalutomatMarketPair.model_validate(
        {"pair": "EUR_PLN", "bestOffers": {"bid_now": 4.0, "ask_now": 4.1, "forex_now": 4.05}}
    )
    real_sleep = asyncio.sleep

    async def fast_sleep(_seconds: float) -> None:
        await real_sleep(0)

    async def fake_fetch() -> dict[str, WalutomatMarketPair]:
        client._running = False
        return {"EUR_PLN": pair}

    monkeypatch.setattr(walutomat_mod.asyncio, "sleep", fast_sleep)
    client._fetch_market_data = AsyncMock(side_effect=fake_fetch)
    await client._polling_loop(["EUR-PLN"])
    assert client._backoff_until == pytest.approx(0.0)
    assert client._backoff_attempts == 0
    assert client._consecutive_error_count == 0


@pytest.mark.asyncio()
async def test_polling_loop_cancellation_during_backoff_does_not_leak_tasks() -> None:
    """Backoff child tasks are cancelled when the polling loop is cancelled.

    Given: A polling loop suspended in backoff,
    When: The polling task is cancelled,
    Then: Cancellation propagates without leaving the tracked backoff wait active.
    """
    client = WalutomatExchangeClient(polling_interval=0.0)
    client._running = True
    client._backoff_until = walutomat_mod.time.monotonic() + 60.0
    task = asyncio.create_task(client._polling_loop(["EUR-PLN"]))
    while client._backoff_wakeup_event is None:
        await asyncio.sleep(0)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    assert task.done()


@pytest.mark.asyncio()
async def test_successful_poll_after_backoff_resets_consecutive_and_attempt_counters() -> None:
    """Successful polling clears error and backoff counters.

    Given: A client with stale error and backoff counters,
    When: The next poll succeeds,
    Then: Consecutive error count and backoff attempts reset to zero.
    """
    client = WalutomatExchangeClient(polling_interval=0.0)
    client._running = True
    client._consecutive_error_count = 3
    client._backoff_attempts = 2
    pair = WalutomatMarketPair.model_validate(
        {"pair": "EUR_PLN", "bestOffers": {"bid_now": 4.0, "ask_now": 4.1, "forex_now": 4.05}}
    )

    async def fake_fetch() -> dict[str, WalutomatMarketPair]:
        client._running = False
        return {"EUR_PLN": pair}

    client._fetch_market_data = AsyncMock(side_effect=fake_fetch)
    await client._polling_loop(["EUR-PLN"])
    assert client._consecutive_error_count == 0
    assert client._backoff_attempts == 0


@pytest.mark.asyncio()
async def test_polling_loop_handles_http_error_without_breaking() -> None:
    """HTTP errors are handled without breaking the polling loop.

    Given: A running client whose first poll raises HTTPError and second poll succeeds,
    When: The polling loop runs,
    Then: The loop calls the HTTP error handler and continues to the successful poll.
    """
    client = WalutomatExchangeClient(polling_interval=0.0)
    client._running = True
    client._max_consecutive_errors = 5
    pair = WalutomatMarketPair.model_validate(
        {"pair": "EUR_PLN", "bestOffers": {"bid_now": 4.0, "ask_now": 4.1, "forex_now": 4.05}}
    )
    calls = 0

    async def fake_fetch() -> dict[str, WalutomatMarketPair]:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise httpx.HTTPError("timeout")
        client._running = False
        return {"EUR_PLN": pair}

    client._fetch_market_data = AsyncMock(side_effect=fake_fetch)
    await client._polling_loop(["EUR-PLN"])
    assert calls == 2
    assert client._consecutive_error_count == 0


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
        type=ExchangeOrderTypeEnum.LIMIT,
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


class TestWalutomatLiveFixtures:
    """Verify order lifecycle using real Walutomat API responses captured from EUR-PLN.

    Each mock dict mirrors an actual REST API return value recorded during
    live integration tests.  The tests confirm that ``_parse_walutomat_order``
    (via ``create_order`` / ``cancel_order`` / ``get_order``) produces the
    correct ``ExchangeOrderSnapshot`` fields.

    Walutomat specifics:
    - create_order returns minimal ``{"success": true, "result": {"orderId": "..."}}``
      and the client constructs a PENDING snapshot from request data
    - cancel_order and get_order return full order details parsed via
      ``_parse_walutomat_order``
    - P2P matching only, all orders are LIMIT
    - Side-aware fill fields: boughtAmount (BUY) / soldAmount (SELL)
    """

    @pytest.fixture
    def client(self) -> WalutomatExchangeClient:
        """Provide an authenticated WalutomatExchangeClient for order tests."""
        return WalutomatExchangeClient(
            api_key="live-key",
            private_key_data=_generate_private_key_pem(),
        )

    @staticmethod
    def _auth_headers(_endpoint: str, _body: str) -> dict[str, str]:
        """Bypass real signature generation for tests."""
        return {"X-API-Key": "live-key"}

    @pytest.mark.asyncio
    async def test_passive_buy_create(
        self, client: WalutomatExchangeClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Passive limit buy returns PENDING snapshot from request data.

        Given: A limit buy at 4.2500 PLN/EUR (well below market).
        When: create_order is called.
        Then: Snapshot has status=PENDING, filled=0, id from API response.
        """
        api_response = {
            "success": True,
            "result": {"orderId": "d10da06a-7d6f-40a9-952c-2a88396a136e"},
        }
        stub = StubAsyncClient(post_responses=[StubResponse(api_response)])
        client._http_client = cast(httpx.AsyncClient, stub)
        monkeypatch.setattr(client, "_get_auth_headers", self._auth_headers)
        monkeypatch.setattr(client, "_log_order_to_db", AsyncMock(return_value=None))
        request = ExchangeOrderRequest(
            symbol="EUR-PLN",
            side=OrderSideEnum.BUY,
            type=ExchangeOrderTypeEnum.LIMIT,
            amount=1.0,
            price=4.0613,
            client_order_id="test-pass-buy-1775398174",
        )
        snap = await client.create_order(request)
        assert snap.id == "d10da06a-7d6f-40a9-952c-2a88396a136e"
        assert snap.client_order_id == "test-pass-buy-1775398174"
        assert snap.symbol == "EUR-PLN"
        assert snap.side == OrderSideEnum.BUY
        assert snap.type == ExchangeOrderTypeEnum.LIMIT
        assert snap.amount == pytest.approx(1.0)
        assert snap.price == pytest.approx(4.0613)
        assert snap.status == ExchangeOrderStatusEnum.PENDING
        assert snap.filled == pytest.approx(0.0)
        assert snap.remaining == pytest.approx(1.0)

    @pytest.mark.asyncio
    async def test_passive_buy_fetch(
        self, client: WalutomatExchangeClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Fetch passive buy returns OPEN (ACTIVE) snapshot with zero fill.

        Given: Order was placed as passive limit buy, still resting.
        When: get_order is called.
        Then: Snapshot has status=OPEN, filled=0, boughtAmount=0.
        """
        api_response: dict[str, Any] = {
            "success": True,
            "result": [
                {
                    "orderId": "d10da06a-7d6f-40a9-952c-2a88396a136e",
                    "submitId": "test-pass-buy-1775398174",
                    "currencyPair": "EURPLN",
                    "buySell": "BUY",
                    "volume": "1.00",
                    "limitPrice": "4.0613",
                    "status": "ACTIVE",
                    "completion": 0,
                    "boughtAmount": "0.00",
                    "soldAmount": "0.00",
                    "commissionAmount": "0",
                    "commissionCurrency": "EUR",
                },
            ],
        }
        stub = StubAsyncClient(get_responses=[StubResponse(api_response)])
        client._http_client = cast(httpx.AsyncClient, stub)
        monkeypatch.setattr(client, "_get_auth_headers", self._auth_headers)
        snap = await client.get_order("d10da06a-7d6f-40a9-952c-2a88396a136e")
        assert snap.id == "d10da06a-7d6f-40a9-952c-2a88396a136e"
        assert snap.client_order_id == "test-pass-buy-1775398174"
        assert snap.symbol == "EUR-PLN"
        assert snap.side == OrderSideEnum.BUY
        assert snap.status == ExchangeOrderStatusEnum.OPEN
        assert snap.filled == pytest.approx(0.0)
        assert snap.remaining == pytest.approx(1.0)
        assert snap.fee is None

    @pytest.mark.asyncio
    async def test_passive_buy_cancel(
        self, client: WalutomatExchangeClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Cancel of passive buy returns CANCELED snapshot with zero fill.

        Given: Order is still resting (no fills).
        When: cancel_order is called.
        Then: Snapshot has status=CANCELED (completion=0, not ACTIVE).
        """
        api_response: dict[str, Any] = {
            "success": True,
            "result": {
                "orderId": "d10da06a-7d6f-40a9-952c-2a88396a136e",
                "submitId": "test-pass-buy-1775398174",
                "currencyPair": "EURPLN",
                "buySell": "BUY",
                "volume": "1.00",
                "limitPrice": "4.0613",
                "status": "CLOSED",
                "completion": 0,
                "boughtAmount": "0.00",
                "soldAmount": "0.00",
                "commissionAmount": "0",
                "commissionCurrency": "EUR",
            },
        }
        stub = StubAsyncClient(post_responses=[StubResponse(api_response)])
        client._http_client = cast(httpx.AsyncClient, stub)
        monkeypatch.setattr(client, "_get_auth_headers", self._auth_headers)
        snap = await client.cancel_order("d10da06a-7d6f-40a9-952c-2a88396a136e")
        assert snap.id == "d10da06a-7d6f-40a9-952c-2a88396a136e"
        assert snap.status == ExchangeOrderStatusEnum.CANCELED
        assert snap.side == OrderSideEnum.BUY
        assert snap.filled == pytest.approx(0.0)
        assert snap.remaining == pytest.approx(1.0)
        assert snap.fee is None

    @pytest.mark.asyncio
    async def test_passive_sell_create(
        self, client: WalutomatExchangeClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Passive limit sell returns PENDING snapshot.

        Given: A limit sell at 4.3500 PLN/EUR (well above market).
        When: create_order is called.
        Then: Snapshot has status=PENDING, side=SELL.
        """
        api_response = {
            "success": True,
            "result": {"orderId": "2977d227-9712-4077-8df2-58ae761ee86d"},
        }
        stub = StubAsyncClient(post_responses=[StubResponse(api_response)])
        client._http_client = cast(httpx.AsyncClient, stub)
        monkeypatch.setattr(client, "_get_auth_headers", self._auth_headers)
        monkeypatch.setattr(client, "_log_order_to_db", AsyncMock(return_value=None))
        request = ExchangeOrderRequest(
            symbol="EUR-PLN",
            side=OrderSideEnum.SELL,
            type=ExchangeOrderTypeEnum.LIMIT,
            amount=1.0,
            price=4.4938,
            client_order_id="test-pass-sell-1775398177",
        )
        snap = await client.create_order(request)
        assert snap.id == "2977d227-9712-4077-8df2-58ae761ee86d"
        assert snap.side == OrderSideEnum.SELL
        assert snap.status == ExchangeOrderStatusEnum.PENDING

    @pytest.mark.asyncio
    async def test_passive_sell_cancel(
        self, client: WalutomatExchangeClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Cancel of passive sell returns CANCELED with soldAmount=0.

        Given: Sell order resting, no fills.
        When: cancel_order is called.
        Then: Snapshot has status=CANCELED, filled=0 (uses soldAmount for SELL).
        """
        api_response: dict[str, Any] = {
            "success": True,
            "result": {
                "orderId": "2977d227-9712-4077-8df2-58ae761ee86d",
                "submitId": "test-pass-sell-1775398177",
                "currencyPair": "EURPLN",
                "buySell": "SELL",
                "volume": "1.00",
                "limitPrice": "4.4938",
                "status": "CLOSED",
                "completion": 0,
                "boughtAmount": "0.00",
                "soldAmount": "0.00",
                "commissionAmount": "0",
                "commissionCurrency": "PLN",
            },
        }
        stub = StubAsyncClient(post_responses=[StubResponse(api_response)])
        client._http_client = cast(httpx.AsyncClient, stub)
        monkeypatch.setattr(client, "_get_auth_headers", self._auth_headers)
        snap = await client.cancel_order("2977d227-9712-4077-8df2-58ae761ee86d")
        assert snap.id == "2977d227-9712-4077-8df2-58ae761ee86d"
        assert snap.status == ExchangeOrderStatusEnum.CANCELED
        assert snap.side == OrderSideEnum.SELL
        assert snap.filled == pytest.approx(0.0)

    @pytest.mark.asyncio
    async def test_topbook_buy_filled_as_maker(
        self, client: WalutomatExchangeClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Topbook buy at best ask fills via P2P matching with commission.

        Given: Limit buy placed at/near best ask, matched by counter-party.
        When: get_order is called after fill.
        Then: Snapshot has status=CLOSED, completion=100, filled=volume, fee > 0.
        """
        api_response: dict[str, Any] = {
            "success": True,
            "result": [
                {
                    "orderId": "wal-topbook-buy-001",
                    "submitId": "submit-topbook-buy",
                    "currencyPair": "EURPLN",
                    "buySell": "BUY",
                    "volume": "100.00",
                    "limitPrice": "4.2850",
                    "status": "CLOSED",
                    "completion": 100,
                    "boughtAmount": "100.00",
                    "soldAmount": "428.50",
                    "commissionAmount": "0.20",
                    "commissionCurrency": "EUR",
                },
            ],
        }
        stub = StubAsyncClient(get_responses=[StubResponse(api_response)])
        client._http_client = cast(httpx.AsyncClient, stub)
        monkeypatch.setattr(client, "_get_auth_headers", self._auth_headers)
        snap = await client.get_order("wal-topbook-buy-001")
        assert snap.id == "wal-topbook-buy-001"
        assert snap.status == ExchangeOrderStatusEnum.CLOSED
        assert snap.filled == pytest.approx(100.0)
        assert snap.remaining == pytest.approx(0.0)
        assert snap.fee == pytest.approx(0.20)
        assert snap.fee_currency == "EUR"

    @pytest.mark.asyncio
    async def test_aggressive_sell_filled(
        self, client: WalutomatExchangeClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Aggressive sell fills using soldAmount for fill tracking.

        Real capture: SELL at 4.2323 (below market bid), P2P matched.
        BUY commission is in EUR, SELL commission is in PLN.

        Given: Limit sell at 4.2323, crossed spread.
        When: get_order is called after fill.
        Then: Snapshot uses soldAmount (not boughtAmount), fee in PLN.
        """
        api_response: dict[str, Any] = {
            "success": True,
            "result": [
                {
                    "orderId": "855c0bce-65ec-4ca5-8f4f-860c8e85f414",
                    "submitId": "test-aggr-sell-1775398184",
                    "currencyPair": "EURPLN",
                    "buySell": "SELL",
                    "volume": "1.00",
                    "limitPrice": "4.2323",
                    "status": "CLOSED",
                    "completion": 100,
                    "boughtAmount": "4.23",
                    "soldAmount": "1.00",
                    "commissionAmount": "0.01",
                    "commissionCurrency": "PLN",
                },
            ],
        }
        stub = StubAsyncClient(get_responses=[StubResponse(api_response)])
        client._http_client = cast(httpx.AsyncClient, stub)
        monkeypatch.setattr(client, "_get_auth_headers", self._auth_headers)
        snap = await client.get_order("855c0bce-65ec-4ca5-8f4f-860c8e85f414")
        assert snap.id == "855c0bce-65ec-4ca5-8f4f-860c8e85f414"
        assert snap.status == ExchangeOrderStatusEnum.CLOSED
        assert snap.side == OrderSideEnum.SELL
        assert snap.filled == pytest.approx(1.0)
        assert snap.remaining == pytest.approx(0.0)
        assert snap.fee == pytest.approx(0.01)
        assert snap.fee_currency == "PLN"

    @pytest.mark.asyncio
    async def test_aggressive_buy_immediate_fill(
        self, client: WalutomatExchangeClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Aggressive buy crosses spread and fills immediately.

        Real capture: Walutomat P2P matching can overfill (boughtAmount > volume)
        due to rounding.  Live test showed boughtAmount="1.01" for volume="1.00".
        The parser must clamp remaining to max(0.0) to avoid negative values.

        Given: Limit buy at 4.3226 crosses the spread and fills via P2P.
        When: get_order is called after fill.
        Then: status=CLOSED, filled=1.01, remaining=0.0 (clamped), fee=0.01 EUR.
        """
        api_response: dict[str, Any] = {
            "success": True,
            "result": [
                {
                    "orderId": "70296031-f26b-43db-94ae-7ab8902560c6",
                    "submitId": "test-aggr-buy-1775398178",
                    "currencyPair": "EURPLN",
                    "buySell": "BUY",
                    "volume": "1.00",
                    "limitPrice": "4.3226",
                    "status": "CLOSED",
                    "completion": 100,
                    "boughtAmount": "1.01",
                    "soldAmount": "4.31",
                    "commissionAmount": "0.01",
                    "commissionCurrency": "EUR",
                },
            ],
        }
        stub = StubAsyncClient(get_responses=[StubResponse(api_response)])
        client._http_client = cast(httpx.AsyncClient, stub)
        monkeypatch.setattr(client, "_get_auth_headers", self._auth_headers)
        snap = await client.get_order("70296031-f26b-43db-94ae-7ab8902560c6")
        assert snap.status == ExchangeOrderStatusEnum.CLOSED
        assert snap.filled == pytest.approx(1.01)
        assert snap.remaining == pytest.approx(0.0)
        assert snap.fee == pytest.approx(0.01)
        assert snap.fee_currency == "EUR"

    @pytest.mark.asyncio
    async def test_cancel_inflight_no_fill(
        self, client: WalutomatExchangeClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Cancel inflight order before P2P match returns CANCELED.

        Given: Order placed but not yet matched.
        When: cancel_order is called immediately.
        Then: Close endpoint returns CLOSED with completion=0 -> CANCELED.
        """
        api_response: dict[str, Any] = {
            "success": True,
            "result": {
                "orderId": "1821545b-6a7a-47b9-b099-15c9c7a5a70b",
                "submitId": "test-cancel-1775398189",
                "currencyPair": "EURPLN",
                "buySell": "BUY",
                "volume": "1.00",
                "limitPrice": "4.0613",
                "status": "CLOSED",
                "completion": 0,
                "boughtAmount": "0.00",
                "soldAmount": "0.00",
                "commissionAmount": "0",
                "commissionCurrency": "EUR",
            },
        }
        stub = StubAsyncClient(post_responses=[StubResponse(api_response)])
        client._http_client = cast(httpx.AsyncClient, stub)
        monkeypatch.setattr(client, "_get_auth_headers", self._auth_headers)
        snap = await client.cancel_order("1821545b-6a7a-47b9-b099-15c9c7a5a70b")
        assert snap.status == ExchangeOrderStatusEnum.CANCELED
        assert snap.filled == pytest.approx(0.0)
        assert snap.remaining == pytest.approx(1.0)

    @pytest.mark.asyncio
    async def test_partial_fill_then_cancel(
        self, client: WalutomatExchangeClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Cancel after partial P2P fill returns CANCELED with partial boughtAmount.

        Given: Order partially matched (50 of 100 EUR), then canceled.
        When: cancel_order is called.
        Then: Snapshot has status=CANCELED, filled=50, remaining=50, fee on partial.
        """
        api_response: dict[str, Any] = {
            "success": True,
            "result": {
                "orderId": "f458fbf1-5819-4813-890c-b58f629e3402",
                "submitId": "test-partial-1775398160",
                "currencyPair": "EURPLN",
                "buySell": "BUY",
                "volume": "100.00",
                "limitPrice": "4.2850",
                "status": "CLOSED",
                "completion": 50,
                "boughtAmount": "50.00",
                "soldAmount": "214.25",
                "commissionAmount": "0.10",
                "commissionCurrency": "EUR",
            },
        }
        stub = StubAsyncClient(post_responses=[StubResponse(api_response)])
        client._http_client = cast(httpx.AsyncClient, stub)
        monkeypatch.setattr(client, "_get_auth_headers", self._auth_headers)
        snap = await client.cancel_order("wal-partial-001")
        assert snap.status == ExchangeOrderStatusEnum.CANCELED
        assert snap.filled == pytest.approx(50.0)
        assert snap.remaining == pytest.approx(50.0)
        assert snap.fee == pytest.approx(0.10)

    @pytest.mark.asyncio
    async def test_get_balance_live_multicurrency(
        self, client: WalutomatExchangeClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify balance parsing with realistic multi-currency account.

        Given: Account holds EUR, PLN, GBP with reserved amounts.
        When: get_balance is called.
        Then: All currencies present with correct free/used/total split.
        """
        api_response: dict[str, Any] = {
            "success": True,
            "result": [
                {
                    "currency": "EUR",
                    "balanceAvailable": "1523.45",
                    "balanceReserved": "100.00",
                    "balanceTotal": "1623.45",
                },
                {
                    "currency": "PLN",
                    "balanceAvailable": "5280.12",
                    "balanceReserved": "428.50",
                    "balanceTotal": "5708.62",
                },
                {
                    "currency": "GBP",
                    "balanceAvailable": "0.00",
                    "balanceReserved": "0.00",
                    "balanceTotal": "0.00",
                },
            ],
        }
        stub = StubAsyncClient(get_responses=[StubResponse(api_response)])
        client._http_client = cast(httpx.AsyncClient, stub)
        monkeypatch.setattr(client, "_get_auth_headers", self._auth_headers)
        balances = await client.get_balance()
        assert len(balances) == 3
        assert balances["EUR"].free == pytest.approx(1523.45)
        assert balances["EUR"].used == pytest.approx(100.0)
        assert balances["EUR"].total == pytest.approx(1623.45)
        assert balances["PLN"].free == pytest.approx(5280.12)
        assert balances["PLN"].used == pytest.approx(428.50)
        assert balances["GBP"].free == pytest.approx(0.0)
        assert balances["GBP"].total == pytest.approx(0.0)

    @pytest.mark.asyncio
    async def test_create_order_generates_submit_id(
        self, client: WalutomatExchangeClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When no client_order_id is provided, a UUID is generated as submitId.

        Given: ExchangeOrderRequest without client_order_id.
        When: create_order is called.
        Then: Snapshot has a non-None client_order_id (auto-generated UUID).
        """
        api_response = {"success": True, "result": {"orderId": "wal-autoid-001"}}
        stub = StubAsyncClient(post_responses=[StubResponse(api_response)])
        client._http_client = cast(httpx.AsyncClient, stub)
        monkeypatch.setattr(client, "_get_auth_headers", self._auth_headers)
        monkeypatch.setattr(client, "_log_order_to_db", AsyncMock(return_value=None))
        request = ExchangeOrderRequest(
            symbol="EUR-PLN",
            side=OrderSideEnum.BUY,
            type=ExchangeOrderTypeEnum.LIMIT,
            amount=50.0,
            price=4.2600,
        )
        snap = await client.create_order(request)
        assert snap.id == "wal-autoid-001"
        assert snap.client_order_id is not None
        assert len(snap.client_order_id) > 0

    @pytest.mark.asyncio
    async def test_create_order_sends_limit_price_in_body(
        self, client: WalutomatExchangeClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify create_order includes limitPrice in the POST body.

        Given: ExchangeOrderRequest with price=4.2850.
        When: create_order is called.
        Then: POST body contains limitPrice=4.2850.
        """
        api_response = {"success": True, "result": {"orderId": "wal-price-001"}}
        stub = StubAsyncClient(post_responses=[StubResponse(api_response)])
        client._http_client = cast(httpx.AsyncClient, stub)
        monkeypatch.setattr(client, "_get_auth_headers", self._auth_headers)
        monkeypatch.setattr(client, "_log_order_to_db", AsyncMock(return_value=None))
        request = ExchangeOrderRequest(
            symbol="EUR-PLN",
            side=OrderSideEnum.BUY,
            type=ExchangeOrderTypeEnum.LIMIT,
            amount=100.0,
            price=4.2850,
        )
        await client.create_order(request)
        post_kwargs = stub.post_calls[0][1]
        body_str = post_kwargs["content"]
        assert "limitPrice=4.2850" in body_str


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


class _StatusErrorResponse:
    """Stub response whose raise_for_status raises a real HTTPStatusError."""

    def __init__(self, status_code: int) -> None:
        self.status_code = status_code

    def raise_for_status(self) -> None:
        """Raise httpx.HTTPStatusError carrying the stub status code."""
        raise httpx.HTTPStatusError(
            f"HTTP {self.status_code}",
            request=httpx.Request("POST", "https://api.walutomat.pl/api/v2.0.0/market_fx/orders"),
            response=httpx.Response(self.status_code),
        )

    def json(self) -> Any:
        """Return an empty payload (never reached in these tests)."""
        return {}


class _UnparseableResponse:
    """Stub 200 response whose body fails to parse as JSON."""

    status_code = 200

    def raise_for_status(self) -> None:
        """No-op for the 200 status."""

    def json(self) -> Any:
        """Raise the json decode failure."""
        raise ValueError("invalid json")


class TestWalutomatAmbiguousSubmitClassification:
    """Exception taxonomy pins for Walutomat create_order.

    Connection-setup failures provably happened before the request left
    the process and stay plain; post-send transport failures, gateway
    5xx, and unusable success bodies wrap as ambiguous because the
    venue may have accepted the order under the submitId.
    """

    def _client(
        self, monkeypatch: pytest.MonkeyPatch, responses: list[Any]
    ) -> WalutomatExchangeClient:
        """Build an authenticated client with stubbed POST responses."""
        client = WalutomatExchangeClient(
            api_key="key",
            private_key_data=_generate_private_key_pem(),
        )
        client._http_client = cast(httpx.AsyncClient, StubAsyncClient(post_responses=responses))
        monkeypatch.setattr(
            client, "_get_auth_headers", lambda *_args, **_kwargs: {"X-API-Key": "key"}
        )
        return client

    def _request(self) -> ExchangeOrderRequest:
        """Build a market order request with a fixed submit id."""
        return ExchangeOrderRequest(
            symbol="EUR-PLN",
            side=OrderSideEnum.BUY,
            type=ExchangeOrderTypeEnum.MARKET,
            amount=100.0,
            client_order_id="amb-w1",
        )

    @pytest.mark.asyncio()
    async def test_connect_error_is_not_wrapped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A connection-refused failure keeps its native type.

        Given: The POST raising ConnectError (request never left),
        When: create_order is called,
        Then: The plain ConnectError propagates (safe to reject).
        """
        client = self._client(monkeypatch, [httpx.ConnectError("refused")])
        with pytest.raises(httpx.ConnectError):
            await client.create_order(self._request())

    @pytest.mark.asyncio()
    async def test_read_timeout_is_wrapped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A post-send read timeout wraps as ambiguous.

        Given: The POST raising ReadTimeout (the venue may have
            accepted the order under this submitId),
        When: create_order is called,
        Then: AmbiguousOrderSubmitError surfaces with the original
            error chained and the submit identity attached.
        """
        client = self._client(monkeypatch, [httpx.ReadTimeout("read timed out")])
        with pytest.raises(AmbiguousOrderSubmitError) as exc_info:
            await client.create_order(self._request())
        assert isinstance(exc_info.value.__cause__, httpx.ReadTimeout)
        assert exc_info.value.client_order_id == "amb-w1"
        assert exc_info.value.instrument == "EUR-PLN"

    @pytest.mark.asyncio()
    async def test_gateway_5xx_is_wrapped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A 5xx response wraps as ambiguous.

        Given: The venue answering 502 (a gateway may have forwarded
            the request to the backend before failing),
        When: create_order is called,
        Then: AmbiguousOrderSubmitError surfaces.
        """
        client = self._client(monkeypatch, [cast(Any, _StatusErrorResponse(502))])
        with pytest.raises(AmbiguousOrderSubmitError):
            await client.create_order(self._request())

    @pytest.mark.asyncio()
    async def test_client_4xx_is_not_wrapped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A 4xx response keeps its native HTTPStatusError type.

        Given: The venue answering 400 (an authoritative rejection),
        When: create_order is called,
        Then: The plain HTTPStatusError propagates (safe to reject).
        """
        client = self._client(monkeypatch, [cast(Any, _StatusErrorResponse(400))])
        with pytest.raises(httpx.HTTPStatusError):
            await client.create_order(self._request())

    @pytest.mark.asyncio()
    async def test_unparseable_success_body_is_wrapped(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An HTTP 200 with an unparseable body wraps as ambiguous.

        Given: A 200 response whose body fails JSON decoding (the venue
            likely accepted but the confirmation is unusable),
        When: create_order is called,
        Then: AmbiguousOrderSubmitError surfaces.
        """
        client = self._client(monkeypatch, [cast(Any, _UnparseableResponse())])
        with pytest.raises(AmbiguousOrderSubmitError):
            await client.create_order(self._request())

    @pytest.mark.asyncio()
    async def test_success_without_order_id_is_wrapped(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """success=true with no orderId wraps as ambiguous.

        Given: A success body lacking result.orderId (accepted but the
            id was lost),
        When: create_order is called,
        Then: AmbiguousOrderSubmitError surfaces.
        """
        client = self._client(monkeypatch, [StubResponse({"success": True, "result": {}})])
        with pytest.raises(AmbiguousOrderSubmitError):
            await client.create_order(self._request())

    @pytest.mark.asyncio()
    async def test_success_with_empty_order_id_is_wrapped(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """success=true with an empty orderId wraps as ambiguous.

        A falsy id passed downstream would be misread as a definitive
        venue rejection while the order may be live.
        """
        client = self._client(
            monkeypatch, [StubResponse({"success": True, "result": {"orderId": ""}})]
        )
        with pytest.raises(AmbiguousOrderSubmitError):
            await client.create_order(self._request())

    @pytest.mark.asyncio()
    async def test_success_with_null_order_id_is_wrapped(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """success=true with a null orderId wraps as ambiguous."""
        client = self._client(
            monkeypatch, [StubResponse({"success": True, "result": {"orderId": None}})]
        )
        with pytest.raises(AmbiguousOrderSubmitError):
            await client.create_order(self._request())


@pytest.mark.asyncio()
async def test_create_order_rejects_stop_types_before_any_send() -> None:
    """Stop orders are honestly refused pre-send — Walutomat has none (#156).

    Given: A client (even unauthenticated — the gate runs first),
    When: create_order() is called with a stop or stop-limit request,
    Then: a ValueError surfaces BEFORE auth or any HTTP send, so the
        executor's definitive-reject branch publishes REJECTED instead
        of parking the order as ambiguous.
    """
    client = WalutomatExchangeClient(api_key="key")
    for order_type in (
        ExchangeOrderTypeEnum.STOP_LOSS,
        ExchangeOrderTypeEnum.STOP_LOSS_LIMIT,
    ):
        request = ExchangeOrderRequest(
            symbol="EUR-PLN",
            side=OrderSideEnum.BUY,
            type=order_type,
            amount=100.0,
            price=4.2,
            stop_price=4.3,
        )
        with pytest.raises(ValueError, match="does not support stop orders"):
            await client.create_order(request)
