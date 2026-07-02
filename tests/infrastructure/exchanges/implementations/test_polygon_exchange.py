"""Tests for Polygon.io exchange client."""

import csv
import math
import os
import time
from collections.abc import Callable
from collections.abc import Iterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from typing import cast

import pytest

from snapper.infrastructure.exchanges.contracts import TickerSnapshot
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.implementations.polygon import PolygonExchangeClient
from snapper.infrastructure.exchanges.implementations.polygon import PolygonRetryPolicy


class DummyPoolManager:
    """Stub pool manager for Polygon exchange tests."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the instance."""
        self.args = args
        self.kwargs = kwargs


class DummyRESTClient:
    """Stub REST client for Polygon exchange tests."""

    def __init__(
        self, api_key: str, trace: bool = False, verbose: bool = False, **kwargs: Any
    ) -> None:
        """Initialize the instance."""
        self.api_key = api_key
        self.trace = trace
        self.verbose = verbose
        self.headers: dict[str, str] = {}
        self.client: Any = SimpleNamespace()
        self._aggregates: list[Any] = []
        self._tickers: list[Any] = []

    def get_previous_close_agg(self, ticker: str) -> list[Any]:
        """Return previous close aggregates for ticker."""
        return list(self._aggregates)

    def list_tickers(self, *args: Any, **kwargs: Any) -> Iterator[Any]:
        """Iterate over tickers."""
        return iter(self._tickers)


@pytest.fixture(name="stubbed_client")
def fixture_stubbed_client(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> PolygonExchangeClient:
    """Provide a PolygonExchangeClient with stubbed REST and HTTP clients."""
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.RESTClient",
        DummyRESTClient,
    )
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.urllib3.PoolManager",
        DummyPoolManager,
    )
    client = PolygonExchangeClient(
        api_key="test-key",
        rate_limit_per_minute=5,
        symbols_cache_file=os.fspath(tmp_path / "symbols.csv"),
        cache_ttl_hours=1,
    )
    client_impl = object.__getattribute__(client, "_client")
    assert isinstance(client_impl, DummyRESTClient)
    return client


def test_polygon_retry_first_retry_backoff() -> None:
    """Verify retry policy calculates correct backoff time after first retry.

    Given: A Polygon retry policy with 3 retries and 12.0 backoff factor,
    When: The policy is incremented for a 429 error,
    Then: The backoff time is 24.0 seconds (2x factor).
    """
    retry = PolygonRetryPolicy(total=3, backoff_factor=12.0)
    retry = retry.increment(method="GET", url="/", error=RuntimeError("429"))
    assert math.isclose(retry.get_backoff_time(), 24.0, rel_tol=1e-9)


def test_polygon_retry_sleep_invokes_super(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify retry policy sleep method calls parent implementation.

    Given: A Polygon retry policy with mocked backoff time,
    When: The sleep method is called with a response,
    Then: The parent Retry.sleep is called with the response.
    """
    called: dict[str, Any] = {}

    def fake_backoff(self: PolygonRetryPolicy) -> float:
        return 5.0

    def fake_sleep(self: Any, response: Any = None) -> None:
        called["response"] = response

    monkeypatch.setattr(PolygonRetryPolicy, "get_backoff_time", fake_backoff)
    monkeypatch.setattr("urllib3.util.retry.Retry.sleep", fake_sleep)
    retry = PolygonRetryPolicy(total=3)
    retry.sleep(response="test-response")
    assert called["response"] == "test-response"


@pytest.mark.asyncio()
async def test_wait_for_rate_limit_no_prior_requests(
    stubbed_client: PolygonExchangeClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify rate limiter allows request immediately when no prior requests exist.

    Given: A Polygon client with no prior request timestamps,
    When: Waiting for rate limit,
    Then: No sleep is called and timestamp is recorded.
    """
    timestamps: list[float] = [100.0, 100.0]

    def fake_time() -> float:
        return timestamps.pop(0)

    async def fake_sleep(duration: float) -> None:
        raise AssertionError(f"Sleep unexpectedly called with {duration}")

    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.time.time",
        fake_time,
    )
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.asyncio.sleep",
        fake_sleep,
    )
    wait_for_rate_limit = object.__getattribute__(stubbed_client, "_wait_for_rate_limit")
    await wait_for_rate_limit()
    timestamps = object.__getattribute__(stubbed_client, "_request_timestamps")
    assert math.isclose(timestamps[-1], 100.0, rel_tol=1e-9)


@pytest.mark.asyncio()
async def test_wait_for_rate_limit_enforces_spacing(
    stubbed_client: PolygonExchangeClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify rate limiter enforces minimum spacing between requests.

    Given: A Polygon client with rate limit of 2 and recent request timestamps,
    When: Waiting for rate limit,
    Then: Appropriate sleep durations are enforced to maintain spacing.
    """
    stubbed_client.rate_limit = 2
    object.__setattr__(stubbed_client, "_request_timestamps", [150.0, 160.0])
    times = iter([170.0, 170.0])

    def fake_time() -> float:
        return next(times)

    sleep_calls: list[float] = []

    async def fake_sleep(duration: float) -> None:
        sleep_calls.append(duration)

    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.time.time",
        fake_time,
    )
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.asyncio.sleep",
        fake_sleep,
    )
    wait_for_rate_limit = object.__getattribute__(stubbed_client, "_wait_for_rate_limit")
    await wait_for_rate_limit()
    assert math.isclose(sleep_calls[0], 40.1, rel_tol=1e-3)
    assert math.isclose(sleep_calls[1], 20.0, rel_tol=1e-3)
    timestamps = object.__getattribute__(stubbed_client, "_request_timestamps")
    assert math.isclose(timestamps[-1], 170.0, rel_tol=1e-9)


@pytest.mark.asyncio()
async def test_make_request_with_retry_handles_429(
    stubbed_client: PolygonExchangeClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify request retry mechanism handles 429 rate limit errors.

    Given: A Polygon client with request function that fails once with 429,
    When: Making a request with retry enabled,
    Then: The request is retried after sleep and succeeds on second attempt.
    """
    attempts: list[str] = []

    async def fake_wait() -> None:
        attempts.append("wait")

    monkeypatch.setattr(stubbed_client, "_wait_for_rate_limit", fake_wait)
    calls = iter([RuntimeError("429 too many requests"), "ok"])

    def request_func() -> str:
        result = next(calls)
        if isinstance(result, Exception):
            raise result
        return str(result)

    sleep_durations: list[float] = []

    async def fake_sleep(duration: float) -> None:
        sleep_durations.append(duration)

    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.asyncio.sleep",
        fake_sleep,
    )
    make_request = object.__getattribute__(stubbed_client, "_make_request_with_retry")
    result = await make_request(request_func, max_retries=3)
    assert result == "ok"
    assert sleep_durations == [1.0]
    assert attempts == ["wait", "wait"]


@pytest.mark.asyncio()
async def test_make_request_with_retry_non_429_error(
    stubbed_client: PolygonExchangeClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify request retry raises non-429 errors immediately.

    Given: A Polygon client with request function that raises ValueError,
    When: Making a request with retry enabled,
    Then: The ValueError is raised immediately without retrying.
    """

    async def fake_wait() -> None:
        return None

    monkeypatch.setattr(stubbed_client, "_wait_for_rate_limit", fake_wait)

    def request_func() -> None:
        raise ValueError("boom")

    make_request = object.__getattribute__(stubbed_client, "_make_request_with_retry")
    with pytest.raises(ValueError, match="boom"):
        await make_request(request_func)


@pytest.mark.asyncio()
async def test_make_request_with_retry_exhausts_attempts(
    stubbed_client: PolygonExchangeClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify request retry exhausts all attempts before failing.

    Given: A Polygon client with request function that always returns 429,
    When: Making a request with max_retries=2,
    Then: RuntimeError is raised after exhausting all retry attempts.
    """

    async def fake_wait() -> None:
        return None

    monkeypatch.setattr(stubbed_client, "_wait_for_rate_limit", fake_wait)

    def request_func() -> None:
        raise RuntimeError("429 rate limit")

    sleep_durations: list[float] = []

    async def fake_sleep(duration: float) -> None:
        sleep_durations.append(duration)

    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.asyncio.sleep",
        fake_sleep,
    )
    make_request = object.__getattribute__(stubbed_client, "_make_request_with_retry")
    with pytest.raises(RuntimeError, match="429 rate limit"):
        await make_request(request_func, max_retries=2)
    assert sleep_durations == [1.0]


@pytest.mark.asyncio()
async def test_get_last_quote_invalid_symbol(stubbed_client: PolygonExchangeClient) -> None:
    """Verify get_last_quote rejects invalid symbol format.

    Given: A Polygon client,
    When: Requesting quote for symbol without proper prefix,
    Then: ValueError is raised.
    """
    with pytest.raises(ValueError):
        await stubbed_client.get_last_quote("EURUSD")


@pytest.mark.asyncio()
async def test_get_last_quote_no_data(
    stubbed_client: PolygonExchangeClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify get_last_quote returns zero values when no data available.

    Given: A Polygon client with API returning empty aggregates,
    When: Requesting quote for a symbol,
    Then: TickerSnapshot with zero bid/ask and correct symbol is returned.
    """

    async def fake_request(func: Callable[[], Any]) -> list[Any]:
        return []

    monkeypatch.setattr(stubbed_client, "_make_request_with_retry", fake_request)
    ticker = await stubbed_client.get_last_quote("C:EURUSD")
    assert ticker.bid == ticker.ask == pytest.approx(0.0)
    assert ticker.symbol == "C:EURUSD"


@pytest.mark.asyncio()
async def test_get_last_quote_with_data(
    stubbed_client: PolygonExchangeClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify get_last_quote returns proper values from aggregate data.

    Given: A Polygon client with API returning aggregate data,
    When: Requesting quote for a crypto symbol,
    Then: TickerSnapshot with correct last price and timestamp is returned.
    """
    agg = SimpleNamespace(close=100.0, timestamp=1_650_000_000_000)

    async def fake_request(func: Callable[[], Any]) -> list[Any]:
        return [agg]

    monkeypatch.setattr(stubbed_client, "_make_request_with_retry", fake_request)
    ticker = await stubbed_client.get_last_quote("X:BTCUSD")
    assert math.isclose(ticker.last, 100.0, rel_tol=1e-9)
    assert ticker.bid < ticker.last < ticker.ask
    assert math.isclose(ticker.timestamp, 1_650_000_000.0, rel_tol=1e-9)


@pytest.mark.asyncio()
async def test_get_ticker_without_agg_data(
    stubbed_client: PolygonExchangeClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify get_ticker returns quote data when no aggregate available.

    Given: A Polygon client with quote data but no aggregate data,
    When: Requesting ticker for an index symbol,
    Then: TickerUpdate with quote values and zero high/low is returned.
    """

    async def fake_quote(symbol: str) -> TickerSnapshot:
        return TickerSnapshot(symbol=symbol, bid=1.2, ask=1.3, last=1.25, timestamp=123.0)

    async def fake_request(func: Callable[[], Any]) -> list[Any]:
        return []

    monkeypatch.setattr(stubbed_client, "get_last_quote", fake_quote)
    monkeypatch.setattr(stubbed_client, "_make_request_with_retry", fake_request)
    data = await stubbed_client.get_ticker("I:SPX")
    assert math.isclose(data.last, 1.25, rel_tol=1e-9)
    assert data.high == data.low == pytest.approx(0.0)


@pytest.mark.asyncio()
async def test_get_ticker_with_data(
    stubbed_client: PolygonExchangeClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify get_ticker returns full data from quote and aggregate.

    Given: A Polygon client with both quote and aggregate data,
    When: Requesting ticker for a forex symbol,
    Then: TickerUpdate with high, low, volume, change values is returned.
    """

    async def fake_quote(symbol: str) -> TickerSnapshot:
        return TickerSnapshot(symbol=symbol, bid=99.5, ask=100.5, last=100.0, timestamp=0.0)

    agg = SimpleNamespace(high=120.0, low=80.0, close=90.0, volume=10_000.0, vwap=95.0)

    async def fake_request(func: Callable[[], Any]) -> list[Any]:
        return [agg]

    monkeypatch.setattr(stubbed_client, "get_last_quote", fake_quote)
    monkeypatch.setattr(stubbed_client, "_make_request_with_retry", fake_request)
    data = await stubbed_client.get_ticker("C:GBPUSD")
    assert data.high == pytest.approx(120.0)
    assert data.low == pytest.approx(80.0)
    assert data.volume == 10_000.0
    assert math.isclose(data.change, 10.0, rel_tol=1e-9)
    assert math.isclose(data.change_pct, 11.111, rel_tol=1e-3)


def test_is_cache_valid_missing_file(stubbed_client: PolygonExchangeClient) -> None:
    """Verify cache validation fails when file is missing.

    Given: A Polygon client with non-existent cache file,
    When: Checking if cache is valid,
    Then: False is returned.
    """
    check_cache_valid = object.__getattribute__(stubbed_client, "_is_cache_valid")
    assert not check_cache_valid()


def test_is_cache_valid_stale_file(
    stubbed_client: PolygonExchangeClient,
    tmp_path: Path,
) -> None:
    """Verify cache validation fails when file is older than TTL.

    Given: A Polygon client with cache file modified 2 hours ago and 1 hour TTL,
    When: Checking if cache is valid,
    Then: False is returned.
    """
    path = os.fspath(tmp_path / "symbols.csv")
    with open(path, "w", encoding="utf-8") as handle:
        handle.write("ticker\n")
    old_time = datetime.now(UTC) - timedelta(hours=2)
    os.utime(path, (old_time.timestamp(), old_time.timestamp()))
    stubbed_client.symbols_cache_file = tmp_path / "symbols.csv"
    check_cache_valid = object.__getattribute__(stubbed_client, "_is_cache_valid")
    assert not check_cache_valid()


def test_is_cache_valid_fresh_file(
    stubbed_client: PolygonExchangeClient,
    tmp_path: Path,
) -> None:
    """Verify cache validation succeeds when file is recent.

    Given: A Polygon client with cache file modified just now,
    When: Checking if cache is valid,
    Then: True is returned.
    """
    path = tmp_path / "symbols.csv"
    path.write_text("ticker\n", encoding="utf-8")
    now = datetime.now(UTC)
    os.utime(path, (now.timestamp(), now.timestamp()))
    stubbed_client.symbols_cache_file = path
    check_cache_valid = object.__getattribute__(stubbed_client, "_is_cache_valid")
    assert check_cache_valid()


def test_is_cache_valid_handles_stat_error(stubbed_client: PolygonExchangeClient) -> None:
    """Verify cache validation handles stat errors gracefully.

    Given: A Polygon client with cache path that raises OSError on stat,
    When: Checking if cache is valid,
    Then: False is returned without raising exception.
    """
    original_path = stubbed_client.symbols_cache_file

    class BrokenPath:
        def exists(self) -> bool:
            return True

        def stat(self) -> Any:
            raise OSError("boom")

        def __str__(self) -> str:
            return "broken-cache.csv"

    stubbed_client.symbols_cache_file = cast(Any, BrokenPath())
    check_cache_valid = object.__getattribute__(stubbed_client, "_is_cache_valid")
    assert not check_cache_valid()
    stubbed_client.symbols_cache_file = original_path


def test_load_symbols_from_cache(
    stubbed_client: PolygonExchangeClient,
    tmp_path: Path,
) -> None:
    """Verify symbols are loaded correctly from cache file.

    Given: A Polygon client with CSV cache containing ticker data,
    When: Loading symbols from cache,
    Then: Ticker dictionaries are returned in correct order.
    """
    cache_path = tmp_path / "symbols.csv"
    with open(cache_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(["ticker", "active", "name"])
        writer.writerow(["X:BTCUSD", "true", "Bitcoin"])
        writer.writerow(["C:EURUSD", "false", ""])
    stubbed_client.symbols_cache_file = cache_path
    load_symbols = object.__getattribute__(stubbed_client, "_load_symbols_from_cache")
    symbols = load_symbols()
    assert [item["ticker"] for item in symbols] == ["X:BTCUSD", "C:EURUSD"]
    assert symbols[0]["active"] is True
    assert symbols[1]["active"] is False
    assert "name" not in symbols[1]


def test_save_symbols_to_cache(
    stubbed_client: PolygonExchangeClient,
    tmp_path: Path,
) -> None:
    """Verify symbols are saved correctly to cache file.

    Given: A Polygon client with specified cache path,
    When: Saving symbols to cache,
    Then: CSV file contains header and data rows.
    """
    cache_path = tmp_path / "symbols.csv"
    stubbed_client.symbols_cache_file = cache_path
    symbols = [{"ticker": "X:BTCUSD", "market": "crypto"}]
    save_cache = object.__getattribute__(stubbed_client, "_save_symbols_to_cache")
    save_cache(symbols)
    with open(cache_path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        loaded = list(reader)
    assert len(loaded) == 1
    assert loaded[0]["ticker"] == "X:BTCUSD"
    assert loaded[0]["market"] == "crypto"


@pytest.mark.asyncio()
async def test_subscribe_instruments_uses_cache(
    stubbed_client: PolygonExchangeClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify instrument subscription uses cache when valid.

    Given: A Polygon client with valid cache containing symbols,
    When: Subscribing to instruments,
    Then: Cached symbols are yielded without calling API.
    """
    cached = [{"ticker": "X:BTCUSD"}, {"ticker": "C:EURUSD"}]

    async def fake_sleep(duration: float) -> None:
        raise AssertionError("Sleep should not be called when using cache")

    monkeypatch.setattr(stubbed_client, "_is_cache_valid", lambda: True)
    monkeypatch.setattr(stubbed_client, "_load_symbols_from_cache", lambda: cached)
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.asyncio.sleep",
        fake_sleep,
    )
    results = [symbol async for symbol in stubbed_client.subscribe_instruments()]
    assert results == cached


@pytest.mark.asyncio()
async def test_subscribe_instruments_downloads_when_stale(
    stubbed_client: PolygonExchangeClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify instrument subscription downloads from API when cache is stale.

    Given: A Polygon client with invalid cache and mocked API,
    When: Subscribing to instruments,
    Then: Symbols are fetched from API and saved to cache.
    """
    tickers = [
        SimpleNamespace(
            ticker="X:BTCUSD",
            name="Bitcoin",
            market="crypto",
            locale="global",
            type="crypto",
            active=True,
            currency_symbol="USD",
            base_currency_symbol="BTC",
            base_currency_name="Bitcoin",
            primary_exchange=None,
            currency_name="US Dollar",
            share_class_figi=None,
            composite_figi=None,
            cik=None,
            source_feed="polygon",
        )
    ]
    client_rest = object.__getattribute__(stubbed_client, "_client")
    assert isinstance(client_rest, DummyRESTClient)
    object.__setattr__(client_rest, "_tickers", tickers)
    monkeypatch.setattr(stubbed_client, "_is_cache_valid", lambda: False)
    saved: dict[str, Any] = {}

    def fake_save(symbols: list[dict[str, Any]]) -> None:
        saved["symbols"] = symbols

    monkeypatch.setattr(stubbed_client, "_save_symbols_to_cache", fake_save)

    async def fake_dispatch_blocking(
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        return func(*args, **kwargs)

    monkeypatch.setattr(stubbed_client, "_dispatch_blocking", fake_dispatch_blocking)
    results = [symbol async for symbol in stubbed_client.subscribe_instruments()]
    assert results[0]["ticker"] == "X:BTCUSD"
    assert saved["symbols"][0]["ticker"] == "X:BTCUSD"


@pytest.mark.asyncio()
async def test_poll_tickers_handles_keyboard_interrupt(
    stubbed_client: PolygonExchangeClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify poll_tickers handles KeyboardInterrupt gracefully.

    Given: A Polygon client with mocked ticker getter and sleep that raises interrupt,
    When: Polling tickers,
    Then: Polling stops after first ticker is fetched.
    """
    calls: list[str] = []

    async def fake_get_ticker(symbol: str) -> TickerUpdate:
        calls.append(symbol)
        return TickerUpdate(
            symbol=symbol,
            bid=1.0,
            bid_qty=0.0,
            ask=1.1,
            ask_qty=0.0,
            last=1.05,
            volume=0.0,
            vwap=0.0,
            low=1.0,
            high=1.1,
            change=0.0,
            change_pct=0.0,
        )

    async def fake_sleep(duration: float) -> None:
        raise KeyboardInterrupt()

    monkeypatch.setattr(stubbed_client, "get_ticker", fake_get_ticker)
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.asyncio.sleep",
        fake_sleep,
    )
    await stubbed_client.poll_tickers(["C:EURUSD"], interval_seconds=10.0)
    assert calls == ["C:EURUSD"]


@pytest.mark.asyncio()
async def test_wait_for_rate_limit_waits_when_limit_exceeded(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Verify rate limiter waits when request limit exceeded.

    Given: A Polygon client with rate limit of 2 and recent timestamps,
    When: Waiting for rate limit,
    Then: Sleep is called with positive duration.
    """
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.RESTClient",
        DummyRESTClient,
    )
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.urllib3.PoolManager",
        DummyPoolManager,
    )
    client = PolygonExchangeClient(
        api_key="test-key",
        rate_limit_per_minute=2,
        symbols_cache_file=os.fspath(tmp_path / "symbols.csv"),
        cache_ttl_hours=1,
    )
    sleep_calls: list[float] = []

    async def fake_sleep(duration: float) -> None:
        sleep_calls.append(duration)

    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.asyncio.sleep",
        fake_sleep,
    )
    now = time.time()
    client._request_timestamps = [now - 30, now - 15]
    await client._wait_for_rate_limit()
    assert len(sleep_calls) >= 1
    assert sleep_calls[0] > 0


class TestListSplits:
    """Split-event fetch used by the split-repair tooling."""

    @pytest.mark.asyncio
    async def test_maps_valid_records_and_skips_malformed(
        self, stubbed_client: PolygonExchangeClient
    ) -> None:
        """Valid split records map to events; malformed ones are skipped.

        Given: A stubbed SDK returning one valid split, one with an
            unparseable execution date, and one missing the ratio,
        When: ``list_splits`` runs with a date lower bound,
        Then: Only the valid event returns, the bound is forwarded in
            ISO form, and the ratio helper reports the raw break ratio.
        """
        received: dict[str, Any] = {}

        def fake_list_splits(**kwargs: Any) -> Iterator[Any]:
            received.update(kwargs)
            return iter(
                [
                    SimpleNamespace(
                        ticker="NFLX",
                        execution_date="2025-11-17",
                        split_from=1,
                        split_to=10,
                    ),
                    SimpleNamespace(
                        ticker="BAD",
                        execution_date="not-a-date",
                        split_from=1,
                        split_to=2,
                    ),
                    SimpleNamespace(
                        ticker="NONE",
                        execution_date="2025-11-18",
                        split_from=None,
                        split_to=2,
                    ),
                ]
            )

        client_impl = object.__getattribute__(stubbed_client, "_client")
        client_impl.list_splits = fake_list_splits
        events = await stubbed_client.list_splits(
            execution_date_gte=datetime(2025, 11, 1, tzinfo=UTC).date()
        )
        assert received == {"execution_date_gte": "2025-11-01", "limit": 1000}
        assert len(events) == 1
        assert events[0].ticker == "NFLX"
        assert events[0].execution_date.isoformat() == "2025-11-17"
        assert events[0].expected_break_ratio == pytest.approx(0.1)

    @pytest.mark.asyncio
    async def test_accepts_string_lower_bound(self, stubbed_client: PolygonExchangeClient) -> None:
        """A string date bound is forwarded verbatim.

        Given: A stubbed SDK returning a reverse split,
        When: ``list_splits`` runs with a string bound,
        Then: The bound passes through unchanged and the reverse ratio
            exceeds one.
        """
        received: dict[str, Any] = {}

        def fake_list_splits(**kwargs: Any) -> Iterator[Any]:
            received.update(kwargs)
            return iter(
                [
                    SimpleNamespace(
                        ticker="SPCE",
                        execution_date="2024-06-17",
                        split_from=20,
                        split_to=1,
                    )
                ]
            )

        client_impl = object.__getattribute__(stubbed_client, "_client")
        client_impl.list_splits = fake_list_splits
        events = await stubbed_client.list_splits(execution_date_gte="2024-06-01", limit=50)
        assert received == {"execution_date_gte": "2024-06-01", "limit": 50}
        assert events[0].expected_break_ratio == pytest.approx(20.0)
