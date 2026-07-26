"""Tests for Polygon.io exchange client implementation."""

import asyncio
import csv
import os
import threading
import time
from datetime import UTC
from datetime import date
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.infrastructure.exchanges.contracts import ExchangeOrderRequest
from snapper.infrastructure.exchanges.contracts import ExchangeOrderTypeEnum
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.implementations.polygon import PolygonExchangeClient
from snapper.infrastructure.exchanges.implementations.polygon import PolygonRetryPolicy
from snapper.infrastructure.exchanges.schemas.polygon import PolygonAgg


def test_retry_policy_first_backoff() -> None:
    """Verify retry policy calculates exponential backoff.

    Given: A retry policy with history,
    When: Getting backoff time,
    Then: Returns exponential delay.
    """
    policy = PolygonRetryPolicy(total=2, backoff_factor=12)

    class DummyResponse(SimpleNamespace):
        redirect_location = None

    policy.history = [DummyResponse()]
    backoff = policy.get_backoff_time()
    assert backoff >= 24.0


def test_retry_policy_sleep_logs(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify sleep uses exponential backoff delay.

    Given: A retry policy with history,
    When: Sleep is called,
    Then: Sleeps for exponential backoff.
    """
    policy = PolygonRetryPolicy(total=2, backoff_factor=12)

    class DummyResponse(SimpleNamespace):
        redirect_location = None

    policy.history = cast(tuple[Any, ...], (DummyResponse(),))
    slept: list[float] = []

    def fake_sleep(*_args: Any, **_kwargs: Any) -> None:
        slept.append(policy.get_backoff_time())

    monkeypatch.setattr("urllib3.util.retry.Retry.sleep", fake_sleep)
    policy.sleep()
    assert slept and slept[0] >= 24.0


@pytest.mark.asyncio
async def test_wait_for_rate_limit_enforces_spacing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify rate limit enforces request spacing.

    Given: Recent request timestamps,
    When: Waiting for rate limit,
    Then: Enforces spacing via sleep.
    """
    client = PolygonExchangeClient(api_key="key", rate_limit_per_minute=2)
    client._request_timestamps = [time.time()]
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    await client._wait_for_rate_limit()
    assert sleeps, "should sleep to enforce spacing"


@pytest.mark.asyncio
async def test_make_request_with_retry_handles_429(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify retry handles 429 rate limit errors.

    Given: A request that fails with 429,
    When: Retrying,
    Then: Succeeds on second attempt.
    """
    client = PolygonExchangeClient(api_key="key")
    attempts = 0

    def req() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 2:
            raise RuntimeError("429 rate limit")
        return "ok"

    monkeypatch.setattr(asyncio, "sleep", AsyncMock())
    result = await client._make_request_with_retry(req, max_retries=2)
    assert result == "ok"


def test_format_aggregate_boundary_formats() -> None:
    """Verify aggregate boundary formatting for various types.

    Given: Various input types,
    When: Formatting aggregate boundary,
    Then: Returns correct format.
    """
    now = datetime.now(UTC)
    assert PolygonExchangeClient._format_aggregate_boundary(now) == int(now.timestamp() * 1000)
    d = now.date()
    assert PolygonExchangeClient._format_aggregate_boundary(d) == d.isoformat()
    assert PolygonExchangeClient._format_aggregate_boundary(123) == 123
    assert PolygonExchangeClient._format_aggregate_boundary("2024-01-01") == "2024-01-01"


@pytest.mark.asyncio
async def test_list_aggregates_filters_invalid(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify list aggregates filters invalid records.

    Given: Aggregates with null timestamps,
    When: Listing,
    Then: Filters out invalid records.
    """
    client = PolygonExchangeClient(api_key="key")

    class FakeAgg(SimpleNamespace):
        def __init__(self, ts: int | None) -> None:
            super().__init__(timestamp=ts, open=1, high=2, low=0.5, close=1.5, volume=10)

    def fake_request() -> list[FakeAgg]:
        return [FakeAgg(int(time.time() * 1000)), FakeAgg(None)]

    monkeypatch.setattr(client, "_make_request_with_retry", AsyncMock(return_value=fake_request()))
    now = datetime.now(UTC)
    result = await client.list_aggregates("X:BTCUSD", 1, "minute", now, now)
    assert all(isinstance(agg, PolygonAgg) for agg in result)
    assert len(result) == 1


@pytest.mark.asyncio
async def test_get_last_quote_validates_symbol(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify get last quote validates symbol format.

    Given: An invalid symbol,
    When: Getting last quote,
    Then: Raises ValueError.
    """
    client = PolygonExchangeClient(api_key="key")
    with pytest.raises(ValueError):
        await client.get_last_quote("BAD")
    pc = SimpleNamespace(
        close=100.0, timestamp=int(time.time() * 1000), high=100, low=99, volume=1, vwap=100
    )
    monkeypatch.setattr(client, "_make_request_with_retry", AsyncMock(return_value=[pc]))
    quote = await client.get_last_quote("X:BTCUSD")
    assert quote.last == pytest.approx(100.0)


@pytest.mark.asyncio
async def test_subscribe_instruments_uses_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify subscribe instruments uses cache when valid.

    Given: A valid cache file,
    When: Subscribing to instruments,
    Then: Uses cached data.
    """
    cache_file = tmp_path / "symbols.csv"
    cache_file.write_text("ticker,name\nX:BTCUSD,BTC\n", encoding="utf-8")
    client = PolygonExchangeClient(
        api_key="key", symbols_cache_file=cache_file, cache_ttl_hours=1000
    )
    yielded: list[dict[str, Any]] = []
    async for item in client.subscribe_instruments():
        yielded.append(item)
    assert yielded == [{"ticker": "X:BTCUSD", "name": "BTC"}]


class StubRESTClient:
    """Stub REST client for testing Polygon client without real API calls."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the instance."""
        self.api_key = kwargs.get("api_key", "test-key")
        self.client = SimpleNamespace()
        self.headers: dict[str, str] = {}


@pytest.fixture
def polygon_client(tmp_path: Path) -> PolygonExchangeClient:
    """Create a PolygonExchangeClient with stubbed dependencies for testing."""
    with (
        patch(
            "snapper.infrastructure.exchanges.implementations.polygon.RESTClient",
            StubRESTClient,
        ),
        patch(
            "snapper.infrastructure.exchanges.implementations.polygon.urllib3.PoolManager",
            MagicMock,
        ),
    ):
        client = PolygonExchangeClient(
            api_key="test-key",
            rate_limit_per_minute=5,
            symbols_cache_file=str(tmp_path / "symbols.csv"),
            cache_ttl_hours=1,
        )
        return client


class TestPolygonRetryLogic:
    """Tests for Polygon client retry logic on rate limiting and errors."""

    @pytest.mark.asyncio
    async def test_non_429_error_raises_immediately(
        self, polygon_client: PolygonExchangeClient
    ) -> None:
        """Verify non-429 errors raise immediately.

        Given: A non-429 error,
        When: Making request,
        Then: Raises immediately without retry.
        """

        def failing_request() -> None:
            raise ValueError("Database connection error")

        polygon_client._wait_for_rate_limit = AsyncMock()
        with pytest.raises(ValueError, match="Database connection error"):
            await polygon_client._make_request_with_retry(failing_request, max_retries=5)

    @pytest.mark.asyncio
    async def test_429_exhausted_retries_raises(
        self, polygon_client: PolygonExchangeClient
    ) -> None:
        """Verify exhausted retries raises exception.

        Given: Persistent 429 errors,
        When: Retries exhausted,
        Then: Raises exception.
        """

        def always_429() -> None:
            raise RuntimeError("429 Too Many Requests")

        polygon_client._wait_for_rate_limit = AsyncMock()
        with patch("asyncio.sleep"), pytest.raises(RuntimeError, match="429"):
            await polygon_client._make_request_with_retry(always_429, max_retries=3)

    @pytest.mark.asyncio
    async def test_429_retry_succeeds_on_second_attempt(
        self, polygon_client: PolygonExchangeClient
    ) -> None:
        """Verify transient 429 recovers on retry.

        Given: Transient 429 error,
        When: Retrying,
        Then: Succeeds on second attempt.
        """
        attempt_count = 0

        def retry_once_then_succeed() -> str:
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count == 1:
                raise RuntimeError("429 rate limit exceeded")
            return "success"

        polygon_client._wait_for_rate_limit = AsyncMock()
        with patch("asyncio.sleep"):
            result = await polygon_client._make_request_with_retry(
                retry_once_then_succeed, max_retries=3
            )
        assert result == "success"
        assert attempt_count == 2

    @pytest.mark.asyncio
    async def test_runtime_error_after_max_retries(
        self, polygon_client: PolygonExchangeClient
    ) -> None:
        """Verify max retries exceeded raises exception.

        Given: Persistent 429 errors,
        When: Max retries exceeded,
        Then: Raises exception.
        """

        def always_fails() -> None:
            raise RuntimeError("429 Too Many Requests")

        polygon_client._wait_for_rate_limit = AsyncMock()
        with patch("asyncio.sleep"), pytest.raises(RuntimeError, match="429"):
            await polygon_client._make_request_with_retry(always_fails, max_retries=2)

    @pytest.mark.asyncio
    async def test_runtime_error_when_zero_retries(
        self, polygon_client: PolygonExchangeClient
    ) -> None:
        """Verify zero retries raises RuntimeError.

        Given: Zero max retries,
        When: Making request,
        Then: Raises RuntimeError.
        """
        with pytest.raises(RuntimeError):
            await polygon_client._make_request_with_retry(lambda: None, max_retries=0)

    @pytest.mark.asyncio
    async def test_request_runs_in_worker_thread_not_event_loop(
        self, polygon_client: PolygonExchangeClient
    ) -> None:
        """Verify request_func runs on the bounded venue REST pool off-loop.

        Given: A request_func that performs blocking work (time.sleep inside SDK pagination),
        When: _make_request_with_retry invokes it,
        Then: The work runs on a ``polygon-rest``-named pool thread, not the event loop.
        """
        main_loop = asyncio.get_running_loop()
        main_thread_id = threading.get_ident()
        executor_thread_id: list[int | None] = []
        executor_thread_name: list[str] = []

        def blocking_request() -> str:
            executor_thread_id.append(threading.get_ident())
            executor_thread_name.append(threading.current_thread().name)
            return "ok"

        polygon_client._wait_for_rate_limit = AsyncMock()
        result = await polygon_client._make_request_with_retry(blocking_request, max_retries=1)
        assert result == "ok"
        assert executor_thread_id[0] is not None
        assert (
            executor_thread_id[0] != main_thread_id
        ), "request_func must run on a worker thread, not the asyncio event loop thread"
        assert executor_thread_name[0].startswith("polygon-rest")
        assert main_loop.is_running(), "event loop must still be running after the call"
        polygon_client._shutdown_rest_pool()


class TestPolygonAggregatesPagination:
    """Tests for Polygon client aggregates listing and pagination."""

    @pytest.mark.asyncio
    async def test_list_aggregates_pagination_with_sleep(
        self, polygon_client: PolygonExchangeClient
    ) -> None:
        """Verify pagination returns all aggregates data.

        Given: Paginated results exceeding limit,
        When: Listing aggregates,
        Then: Returns all data.
        """
        mock_aggs = [
            SimpleNamespace(timestamp=i, open=100.0, high=101.0, low=99.0, close=100.5, volume=1000)
            for i in range(51)
        ]
        polygon_client._client.list_aggs = MagicMock(return_value=iter(mock_aggs))
        with patch("time.sleep"):
            result = await polygon_client.list_aggregates(
                ticker="X:BTCUSD",
                multiplier=1,
                timespan="day",
                from_date=date(2024, 1, 1),
                to_date=date(2024, 1, 31),
                limit=50,
            )
        assert len(result) == 51

    @pytest.mark.asyncio
    async def test_list_aggregates_empty_results(
        self, polygon_client: PolygonExchangeClient
    ) -> None:
        """Verify empty aggregates returns empty list.

        Given: No aggregates data,
        When: Listing,
        Then: Returns empty list.
        """
        polygon_client._client.list_aggs = MagicMock(return_value=iter([]))
        result = await polygon_client.list_aggregates(
            ticker="X:BTCUSD",
            multiplier=1,
            timespan="day",
            from_date=date(2024, 1, 1),
            to_date=date(2024, 1, 31),
        )
        assert result == []

    @pytest.mark.asyncio
    async def test_format_aggregate_boundary_datetime(
        self, polygon_client: PolygonExchangeClient
    ) -> None:
        """Verify datetime formatting returns epoch milliseconds.

        Given: A datetime with timezone,
        When: Formatting,
        Then: Returns epoch milliseconds.
        """
        dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        result = PolygonExchangeClient._format_aggregate_boundary(dt)
        expected = int(dt.timestamp() * 1000)
        assert result == expected

    @pytest.mark.asyncio
    async def test_format_aggregate_boundary_date(
        self, polygon_client: PolygonExchangeClient
    ) -> None:
        """Verify date formatting returns ISO string.

        Given: A date object,
        When: Formatting,
        Then: Returns ISO string.
        """
        d = date(2024, 1, 1)
        result = PolygonExchangeClient._format_aggregate_boundary(d)
        assert result == "2024-01-01"

    @pytest.mark.asyncio
    async def test_format_aggregate_boundary_int(
        self, polygon_client: PolygonExchangeClient
    ) -> None:
        """Verify integer timestamp passthrough.

        Given: An integer timestamp,
        When: Formatting,
        Then: Returns same integer.
        """
        timestamp = 1704067200000
        result = PolygonExchangeClient._format_aggregate_boundary(timestamp)
        assert result == timestamp


@pytest.mark.asyncio
async def test_wait_for_rate_limit_full_queue(
    polygon_client: PolygonExchangeClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify full queue triggers rate limit sleep.

    Given: A full request queue,
    When: Waiting for rate limit,
    Then: Sleeps to enforce spacing.
    """
    client = polygon_client
    client.rate_limit = 1
    now = time.time()
    client._request_timestamps = [now - 1]
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    await client._wait_for_rate_limit()
    assert len(sleeps) >= 1


def test_format_aggregate_boundary_naive_datetime() -> None:
    """Verify naive datetime raises ValueError.

    Given: A naive datetime,
    When: Formatting boundary,
    Then: Raises ValueError.
    """
    naive = datetime(2024, 1, 1, 12, 0, 0)
    with pytest.raises(ValueError, match="Cannot format naive datetime"):
        PolygonExchangeClient._format_aggregate_boundary(naive)


@pytest.mark.asyncio
async def test_list_aggregates_skips_invalid_records(
    polygon_client: PolygonExchangeClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify list aggregates skips invalid records.

    Given: Records with errors,
    When: Listing aggregates,
    Then: Skips invalid records.
    """
    good = SimpleNamespace(
        timestamp=int(time.time() * 1000), open=1, high=1, low=1, close=1, volume=1
    )

    class Bad:
        def __getattr__(self, _name: str) -> Any:
            raise ValueError("boom")

    monkeypatch.setattr(
        polygon_client,
        "_make_request_with_retry",
        AsyncMock(return_value=[good, Bad()]),
    )
    now = datetime.now(UTC)
    aggs = await polygon_client.list_aggregates("X:BTCUSD", 1, "minute", now, now)
    assert len(aggs) == 1


@pytest.mark.asyncio
async def test_grouped_daily_aggs_skips_invalid(
    polygon_client: PolygonExchangeClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify grouped daily aggs skips invalid records.

    Given: Invalid records,
    When: Getting grouped daily aggs,
    Then: Skips them.
    """

    class Bad:
        def __getattr__(self, _name: str) -> Any:
            raise ValueError("bad")

    monkeypatch.setattr(
        polygon_client,
        "_make_request_with_retry",
        AsyncMock(return_value=[Bad()]),
    )
    grouped = await polygon_client.get_grouped_daily_aggs(date(2024, 1, 1), market_type="crypto")
    assert grouped == []


class TestPolygonGroupedDailyAggs:
    """Tests for Polygon client grouped daily aggregates retrieval."""

    @pytest.mark.asyncio
    async def test_grouped_daily_aggs_with_results_attribute(
        self, polygon_client: PolygonExchangeClient
    ) -> None:
        """Verify results attribute extraction.

        Given: Response with results attribute,
        When: Getting grouped aggs,
        Then: Extracts results.
        """
        mock_results = [
            SimpleNamespace(ticker="X:BTCUSD", open=100.0, close=101.0),
            SimpleNamespace(ticker="X:ETHUSD", open=200.0, close=201.0),
        ]
        mock_response = SimpleNamespace(results=mock_results)
        polygon_client._client.get_grouped_daily_aggs = MagicMock(return_value=mock_response)
        result = await polygon_client.get_grouped_daily_aggs(date(2024, 1, 1), market_type="crypto")
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_grouped_daily_aggs_without_results_attribute(
        self, polygon_client: PolygonExchangeClient
    ) -> None:
        """Verify raw response used without results attribute.

        Given: Response without results attribute,
        When: Getting grouped aggs,
        Then: Uses raw response.
        """
        mock_response = [
            SimpleNamespace(ticker="X:BTCUSD", open=100.0, close=101.0),
        ]
        polygon_client._client.get_grouped_daily_aggs = MagicMock(return_value=mock_response)
        result = await polygon_client.get_grouped_daily_aggs(date(2024, 1, 1), market_type="crypto")
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_grouped_daily_aggs_with_datetime(
        self, polygon_client: PolygonExchangeClient
    ) -> None:
        """Verify datetime input formatting.

        Given: Datetime input,
        When: Getting grouped aggs,
        Then: Formats date correctly.
        """
        dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        mock_response = SimpleNamespace(results=[])
        polygon_client._client.get_grouped_daily_aggs = MagicMock(return_value=mock_response)
        await polygon_client.get_grouped_daily_aggs(dt, market_type="crypto")
        polygon_client._client.get_grouped_daily_aggs.assert_called_once()
        call_args = polygon_client._client.get_grouped_daily_aggs.call_args
        assert call_args[0][0].startswith("2024-01-01")

    @pytest.mark.asyncio
    async def test_grouped_daily_aggs_with_string_date(
        self, polygon_client: PolygonExchangeClient
    ) -> None:
        """Verify string date passthrough.

        Given: String date input,
        When: Getting grouped aggs,
        Then: Passes date correctly.
        """
        mock_response = SimpleNamespace(results=[])
        polygon_client._client.get_grouped_daily_aggs = MagicMock(return_value=mock_response)
        await polygon_client.get_grouped_daily_aggs("2024-01-01", market_type="crypto")
        polygon_client._client.get_grouped_daily_aggs.assert_called_once()


class TestPolygonGetLastQuote:
    """Tests for Polygon client last quote retrieval."""

    @pytest.mark.asyncio
    async def test_get_last_quote_with_exception(
        self, polygon_client: PolygonExchangeClient
    ) -> None:
        """Verify API error propagates as exception.

        Given: API error,
        When: Getting last quote,
        Then: Raises exception.
        """
        polygon_client._client.get_previous_close_agg = MagicMock(
            side_effect=Exception("API error")
        )
        with pytest.raises(Exception, match="API error"):
            await polygon_client.get_last_quote("X:BTCUSD")


def test_resolve_timeframe_invalid() -> None:
    """Verify invalid timeframe raises ValueError.

    Given: An invalid timeframe string,
    When: Resolving,
    Then: Raises ValueError.
    """
    with pytest.raises(ValueError):
        PolygonExchangeClient._resolve_timeframe("2d")


@pytest.mark.asyncio
async def test_get_ohlcv_builds_snapshots(
    polygon_client: PolygonExchangeClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify OHLCV builds market snapshots.

    Given: Aggregate data,
    When: Getting OHLCV,
    Then: Returns market snapshots.
    """
    client = polygon_client
    agg = PolygonAgg(
        timestamp=1,
        open=1.0,
        high=2.0,
        low=0.5,
        close=1.5,
        volume=10.0,
        vwap=None,
        transactions=1,
    )
    monkeypatch.setattr(client, "list_aggregates", AsyncMock(return_value=[agg, agg]))
    result = await client.get_ohlcv("X:BTCUSD", timeframe="1m", since=0, limit=2)
    assert len(result) == 2


@pytest.mark.asyncio
async def test_not_supported_methods_raise(polygon_client: PolygonExchangeClient) -> None:
    """Verify trading methods raise NotImplementedError.

    Given: A data-only client,
    When: Calling trading methods,
    Then: Raises NotImplementedError.
    """
    with pytest.raises(NotImplementedError):
        await polygon_client.create_order(
            ExchangeOrderRequest(
                symbol="X:BTCUSD",
                side=OrderSideEnum.BUY,
                type=ExchangeOrderTypeEnum.LIMIT,
                amount=1.0,
                client_order_id="coid-not-supported-methods-raise",
            )
        )
    with pytest.raises(NotImplementedError):
        await polygon_client.cancel_order("id")
    with pytest.raises(NotImplementedError):
        await polygon_client.get_order("id")
    with pytest.raises(NotImplementedError):
        await polygon_client.get_orders()
    with pytest.raises(NotImplementedError):
        await polygon_client.get_balance()
    with pytest.raises(NotImplementedError):
        polygon_client.subscribe_ticks([])
    with pytest.raises(NotImplementedError):
        polygon_client.subscribe_candles([])
    with pytest.raises(NotImplementedError):
        polygon_client.subscribe_trades([])
    with pytest.raises(NotImplementedError):
        polygon_client.subscribe_executions()


@pytest.mark.asyncio
async def test_subscribe_instruments_downloads_all(
    polygon_client: PolygonExchangeClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify subscribe instruments downloads all tickers.

    Given: Many tickers,
    When: Subscribing instruments,
    Then: Downloads all with pagination.
    """
    client = polygon_client
    client._is_cache_valid = lambda: False
    saved: list[list[dict[str, Any]]] = []
    client._save_symbols_to_cache = lambda symbols: saved.append(symbols)

    class Ticker(SimpleNamespace):
        pass

    tickers = [
        Ticker(
            ticker="X:BTCUSD",
            name="Bitcoin",
            market="crypto",
            locale="global",
            primary_exchange="CB",
            type="crypto",
            active=True,
            currency_symbol="USD",
            currency_name="US Dollar",
            base_currency_symbol="BTC",
            base_currency_name="Bitcoin",
            cik="cik",
            composite_figi="comp",
            share_class_figi="scf",
            source_feed="feed",
        )
        for _ in range(1000)
    ]
    client._client = SimpleNamespace(
        list_tickers=lambda **_kwargs: iter(tickers),
        headers={},
        client=None,
    )

    async def fake_dispatch_blocking(func: Any, /, *args: Any, **kwargs: Any) -> Any:
        return func(*args, **kwargs)

    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr(client, "_dispatch_blocking", fake_dispatch_blocking)
    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    yielded: list[dict[str, Any]] = []
    async for s in client.subscribe_instruments():
        yielded.append(s)
    assert len(yielded) == 1000
    assert sleeps, "should sleep after page"
    assert saved and len(saved[0]) == 1000


@pytest.mark.asyncio
async def test_poll_tickers_handles_errors(
    polygon_client: PolygonExchangeClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify poll tickers handles errors gracefully.

    Given: Errors when polling,
    When: Interrupted,
    Then: Handles errors gracefully.
    """
    client = polygon_client
    client.rate_limit = 1
    monkeypatch.setattr(client, "get_ticker", AsyncMock(side_effect=ValueError("fail")))

    async def fast_sleep(_seconds: float) -> None:
        raise KeyboardInterrupt()

    monkeypatch.setattr(asyncio, "sleep", fast_sleep)
    await client.poll_tickers(["X:BTCUSD"], interval_seconds=1)


class TestPolygonSubscribeInstruments:
    """Tests for Polygon client instrument subscription and caching."""

    @pytest.mark.asyncio
    async def test_subscribe_instruments_loads_from_api_when_cache_missing(
        self, polygon_client: PolygonExchangeClient, tmp_path: Path
    ) -> None:
        """Verify missing cache triggers API load.

        Given: No cache file,
        When: Subscribing instruments,
        Then: Loads from API.
        """
        cache_file = tmp_path / "symbols.csv"
        cache_file.unlink(missing_ok=True)
        mock_tickers = [
            SimpleNamespace(ticker="X:BTCUSD", name="Bitcoin"),
            SimpleNamespace(ticker="X:ETHUSD", name="Ethereum"),
        ]
        polygon_client._client.list_tickers = MagicMock(return_value=iter(mock_tickers))
        symbols: list[dict[str, Any]] = []
        async for symbol in polygon_client.subscribe_instruments():
            symbols.append(symbol)
        assert len(symbols) == 2
        assert symbols[0]["ticker"] == "X:BTCUSD"

    @pytest.mark.asyncio
    async def test_subscribe_instruments_uses_cache_when_fresh(
        self, polygon_client: PolygonExchangeClient, tmp_path: Path
    ) -> None:
        """Verify fresh cache used instead of API.

        Given: Fresh cache file,
        When: Subscribing instruments,
        Then: Uses cached data.
        """
        cache_file = tmp_path / "symbols.csv"

        def _write_cache() -> None:
            """Write fresh cache file for test."""
            with open(cache_file, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f, lineterminator="\n")
                writer.writerow(["ticker", "name"])
                writer.writerow(["X:BTCUSD", "Bitcoin"])
                writer.writerow(["X:ETHUSD", "Ethereum"])

        await asyncio.to_thread(_write_cache)
        polygon_client._client.list_tickers = MagicMock()
        symbols: list[dict[str, Any]] = []
        async for symbol in polygon_client.subscribe_instruments():
            symbols.append(symbol)
        assert len(symbols) == 2
        polygon_client._client.list_tickers.assert_not_called()

    @pytest.mark.asyncio
    async def test_subscribe_instruments_refreshes_stale_cache(
        self, polygon_client: PolygonExchangeClient, tmp_path: Path
    ) -> None:
        """Verify stale cache triggers API refresh.

        Given: Stale cache file,
        When: Subscribing instruments,
        Then: Refreshes from API.
        """
        cache_file = tmp_path / "symbols.csv"

        def _write_stale_cache() -> None:
            """Write stale cache file for test."""
            with open(cache_file, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f, lineterminator="\n")
                writer.writerow(["ticker", "name"])
                writer.writerow(["X:BTCUSD", "Bitcoin"])

        await asyncio.to_thread(_write_stale_cache)
        stale_timestamp = time.time() - (2 * 3600)
        os.utime(cache_file, (stale_timestamp, stale_timestamp))
        mock_tickers = [
            SimpleNamespace(ticker="X:BTCUSD", name="Bitcoin"),
            SimpleNamespace(ticker="X:ETHUSD", name="Ethereum"),
        ]
        polygon_client._client.list_tickers = MagicMock(return_value=iter(mock_tickers))
        symbols: list[dict[str, Any]] = []
        async for symbol in polygon_client.subscribe_instruments():
            symbols.append(symbol)
        assert len(symbols) == 2
        polygon_client._client.list_tickers.assert_called_once()


class TestPolygonConnectDisconnect:
    """Tests for Polygon client connect and disconnect operations."""

    @pytest.mark.asyncio
    async def test_connect_is_noop(self, polygon_client: PolygonExchangeClient) -> None:
        """Verify connect is no-op.

        Given: A client,
        When: Connecting,
        Then: Completes as no-op.
        """
        await polygon_client.connect()

    @pytest.mark.asyncio
    async def test_disconnect_is_noop(self, polygon_client: PolygonExchangeClient) -> None:
        """Verify disconnect is no-op.

        Given: A client,
        When: Disconnecting,
        Then: Completes as no-op.
        """
        await polygon_client.disconnect()


class TestPolygonCreateCancelOrder:
    """Tests for Polygon client order creation and cancellation (not supported)."""

    @pytest.mark.asyncio
    async def test_create_order_not_supported(self, polygon_client: PolygonExchangeClient) -> None:
        """Verify create order raises NotImplementedError.

        Given: An order request,
        When: Creating order,
        Then: Raises NotImplementedError.
        """
        request = ExchangeOrderRequest(
            symbol="X:BTCUSD",
            side=OrderSideEnum.BUY,
            type=ExchangeOrderTypeEnum.MARKET,
            amount=1.0,
            client_order_id="coid-create-order-not-supported",
        )
        with pytest.raises(NotImplementedError, match="market data only"):
            await polygon_client.create_order(request)

    @pytest.mark.asyncio
    async def test_cancel_order_not_supported(self, polygon_client: PolygonExchangeClient) -> None:
        """Verify cancel order raises NotImplementedError.

        Given: An order ID,
        When: Cancelling order,
        Then: Raises NotImplementedError.
        """
        with pytest.raises(NotImplementedError, match="market data only"):
            await polygon_client.cancel_order("order123")


class TestRestPoolLifecycle:
    """Polygon client lifecycle for the dedicated REST pool (P1-5)."""

    @pytest.mark.asyncio
    async def test_disconnect_releases_rest_pool(
        self, polygon_client: PolygonExchangeClient
    ) -> None:
        """Verify disconnect frees the pool for this REST-only client.

        Given: A client whose pool was created by a blocking dispatch,
        When: ``disconnect()`` runs,
        Then: The pool is shut down and cleared.
        """
        polygon_client._ensure_rest_pool()
        await polygon_client.disconnect()
        assert polygon_client._rest_pool is None

    @pytest.mark.asyncio
    async def test_disconnect_connect_cycle_reopens_dispatch(
        self, polygon_client: PolygonExchangeClient
    ) -> None:
        """Verify the cached-client reuse path survives a full cycle.

        Given: A client that dispatched, disconnected (pool closed),
            then reconnected — the polygon symbol updater caches its
            client across update cycles and goes through exactly this
            connect/disconnect lifecycle,
        When: A blocking call is dispatched after the reconnect,
        Then: A fresh pool serves it instead of the closed-pool error.
        """
        assert await polygon_client._dispatch_blocking(lambda: "first") == "first"
        await polygon_client.disconnect()
        with pytest.raises(RuntimeError, match="closed"):
            await polygon_client._dispatch_blocking(lambda: "blocked")
        await polygon_client.connect()
        assert await polygon_client._dispatch_blocking(lambda: "again") == "again"
        polygon_client._shutdown_rest_pool()

    @pytest.mark.asyncio
    async def test_instrument_pages_materialize_on_worker_thread(
        self, polygon_client: PolygonExchangeClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify lazy SDK paging is consumed on the pool, not the loop.

        Given: A lazily-evaluated ticker iterator that records the
            thread consuming each item,
        When: ``subscribe_instruments`` drains it,
        Then: Every item was pulled on a ``polygon-rest`` pool thread —
            lazy pagination network I/O never runs on the event loop.
        """
        main_thread_id = threading.get_ident()
        consuming_threads: list[tuple[int, str]] = []

        def lazy_tickers() -> Any:
            for idx in range(3):
                consuming_threads.append((threading.get_ident(), threading.current_thread().name))
                yield SimpleNamespace(
                    ticker=f"X:LAZY{idx}", name=f"Lazy {idx}", market="crypto", locale="global"
                )

        polygon_client._client = SimpleNamespace(
            list_tickers=lambda **_kwargs: lazy_tickers(),
            headers={},
            client=None,
        )
        monkeypatch.setattr(polygon_client, "_is_cache_valid", lambda: False)
        monkeypatch.setattr(polygon_client, "_save_symbols_to_cache", lambda symbols: None)
        yielded: list[dict[str, Any]] = []
        async for item in polygon_client.subscribe_instruments():
            yielded.append(item)
        assert len(yielded) == 3
        assert consuming_threads, "lazy iterator was never consumed"
        for thread_id, thread_name in consuming_threads:
            assert thread_id != main_thread_id
            assert thread_name.startswith("polygon-rest")
        polygon_client._shutdown_rest_pool()
