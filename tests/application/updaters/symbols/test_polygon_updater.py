"""Tests for Polygon symbol updater service."""

import asyncio
import time
from collections.abc import Iterator
from datetime import UTC
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session
from urllib3.util.retry import RequestHistory

from snapper.application.updaters.symbols.polygon import PolygonSymbolUpdaterService
from snapper.config.settings import AppSettings
from snapper.data.models import SymbolAlias
from snapper.data.models import SymbolCatalog
from snapper.data.repository import DatabaseRepository
from snapper.infrastructure.exchanges.implementations.polygon import PolygonExchangeClient
from snapper.infrastructure.exchanges.implementations.polygon import PolygonRetryPolicy


class FakeRestNoClient:
    """Fake REST client without actual client instance."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the instance."""
        self.headers: dict[str, str] = {}


class FakeRestWithClient:
    """Fake REST client with mock client and ticker data."""

    tickers_data: list[Any] = []

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the instance."""
        self.headers: dict[str, str] = {}
        self.client = object()

    def list_tickers(self, *args: Any, **kwargs: Any) -> list[Any]:
        """Return configured ticker data."""
        return list(self.tickers_data)


class FakeTicker:
    """Fake ticker object with crypto market attributes."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.ticker = "X:BTCUSD"
        self.name = "Bitcoin/USD"
        self.market = "crypto"
        self.locale = "global"
        self.primary_exchange = "GDAX"
        self.type = "crypto"
        self.active = True
        self.currency_symbol = "USD"
        self.currency_name = "US Dollar"
        self.base_currency_symbol = "BTC"
        self.base_currency_name = "Bitcoin"
        self.cik = "cik"
        self.composite_figi = "figi"
        self.share_class_figi = "share"
        self.source_feed = "polygon"


def test_polygon_retry_policy_backoff_and_logging(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify retry policy calculates exponential backoff and logs retries.

    Given: Retry policy with history of one attempt,
    When: Sleep is called during retry,
    Then: Correct backoff delay applied and retry logged.
    """
    retry = PolygonRetryPolicy(total=2, backoff_factor=12.0, raise_on_status=False)
    retry.history = (RequestHistory("GET", "/", None, None, None),)
    sleeps: list[float] = []
    warnings: list[str] = []

    def fake_sleep(duration: float) -> None:
        sleeps.append(duration)

    def fake_warning(message: str) -> None:
        warnings.append(str(message))

    monkeypatch.setattr("urllib3.util.retry.time.sleep", fake_sleep)
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.logger.warning", fake_warning
    )
    retry.sleep()
    assert sleeps == [24.0]
    assert any("retry #1" in message for message in warnings)


def test_polygon_retry_policy_no_history_no_logging(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify retry policy skips logging when no retry history exists.

    Given: Retry policy with empty history,
    When: Backoff time is calculated,
    Then: Zero delay returned and no warning logged.
    """
    retry = PolygonRetryPolicy(total=1, backoff_factor=12.0, raise_on_status=False)
    retry.history = ()
    sleeps: list[float] = []
    warnings: list[str] = []
    monkeypatch.setattr("urllib3.util.retry.time.sleep", lambda duration: sleeps.append(duration))
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.logger.warning",
        lambda message: warnings.append(str(message)),
    )
    assert retry.get_backoff_time() == 0
    retry.sleep()
    assert sleeps in ([], [0])
    assert warnings == []


def test_polygon_init_skips_pool_patch_when_client_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify client init skips pool patch when underlying client missing.

    Given: REST client mock without underlying client,
    When: PolygonExchangeClient is initialized,
    Then: No client attribute exists on internal REST client.
    """
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.RESTClient", FakeRestNoClient
    )
    client = PolygonExchangeClient(api_key="key")
    assert not hasattr(client._client, "client")


@pytest.mark.asyncio
async def test_make_request_with_retry_non_429_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify non-429 errors raise immediately without retry.

    Given: Request function that raises ValueError,
    When: Make request with retry is called,
    Then: Error raised after single attempt without retry.
    """
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.RESTClient", FakeRestWithClient
    )
    client = PolygonExchangeClient(api_key="key")

    async def no_wait() -> None:
        return None

    monkeypatch.setattr(client, "_wait_for_rate_limit", no_wait)
    calls = 0

    def raise_non_429() -> Any:
        nonlocal calls
        calls += 1
        raise ValueError("boom")

    with pytest.raises(ValueError):
        await client._make_request_with_retry(raise_non_429, max_retries=3)
    assert calls == 1


@pytest.mark.asyncio
async def test_make_request_with_retry_exhausts_on_429(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify 429 rate limit errors retry until exhausted.

    Given: Request function that always returns 429,
    When: Make request with retry is called with max_retries=2,
    Then: Retries exhausted and RuntimeError raised.
    """
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.RESTClient", FakeRestWithClient
    )
    client = PolygonExchangeClient(api_key="key")
    monkeypatch.setattr(client, "_wait_for_rate_limit", lambda: asyncio.sleep(0))
    sleep_calls: list[float] = []

    async def fake_sleep(duration: float) -> None:
        sleep_calls.append(duration)

    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.asyncio.sleep",
        fake_sleep,
    )

    def raise_429() -> Any:
        raise RuntimeError("429 too many requests")

    with pytest.raises(RuntimeError):
        await client._make_request_with_retry(raise_429, max_retries=2)
    assert [duration for duration in sleep_calls if duration > 0] == [1]


@pytest.mark.asyncio
async def test_wait_for_rate_limit_sleeps_on_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify rate limiter sleeps when request limit reached.

    Given: Client with multiple recent request timestamps,
    When: Wait for rate limit is called,
    Then: Multiple sleep calls executed to enforce spacing.
    """
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.RESTClient", FakeRestWithClient
    )
    client = PolygonExchangeClient(api_key="key")
    now = time.time()
    client._request_timestamps = [
        now - 59,
        now - 58,
        now - 57,
        now - 56,
        now - 1,
    ]
    sleeps: list[float] = []

    async def fake_sleep(duration: float) -> None:
        sleeps.append(duration)

    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.asyncio.sleep",
        fake_sleep,
    )
    await client._wait_for_rate_limit()
    assert len(sleeps) == 2
    assert sleeps[0] > 0
    assert sleeps[1] > 0


@pytest.mark.asyncio
async def test_wait_for_rate_limit_no_sleep_under_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify rate limiter allows requests when under limit.

    Given: Client with empty request timestamps,
    When: Wait for rate limit is called,
    Then: No sleep executed.
    """
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.RESTClient", FakeRestWithClient
    )
    client = PolygonExchangeClient(api_key="key")
    client._request_timestamps = []
    sleeps: list[float] = []

    async def fake_sleep(duration: float) -> None:
        sleeps.append(duration)

    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.asyncio.sleep",
        fake_sleep,
    )
    await client._wait_for_rate_limit()
    assert sleeps == []


@pytest.mark.asyncio
async def test_wait_for_rate_limit_expired_and_spaced(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify rate limiter skips sleep for expired and well-spaced requests.

    Given: Client with expired and spaced request timestamps,
    When: Wait for rate limit is called,
    Then: No sleep executed.
    """
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.RESTClient", FakeRestWithClient
    )
    client = PolygonExchangeClient(api_key="key")
    now = time.time()
    client._request_timestamps = [now - 120, now - 30]
    sleeps: list[float] = []

    async def fake_sleep(duration: float) -> None:
        sleeps.append(duration)

    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.asyncio.sleep",
        fake_sleep,
    )
    await client._wait_for_rate_limit()
    assert sleeps == []


@pytest.mark.asyncio
async def test_wait_for_rate_limit_skips_spacing_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify rate limiter skips spacing sleep when already spaced.

    Given: Client with single timestamp 20 seconds ago,
    When: Wait for rate limit is called,
    Then: No sleep executed due to adequate spacing.
    """
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.RESTClient", FakeRestWithClient
    )
    client = PolygonExchangeClient(api_key="key")
    now = time.time()
    client._request_timestamps = [now - 20]
    sleeps: list[float] = []

    async def fake_sleep(duration: float) -> None:
        sleeps.append(duration)

    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.asyncio.sleep",
        fake_sleep,
    )
    await client._wait_for_rate_limit()
    assert sleeps == []


@pytest.mark.asyncio
async def test_subscribe_instruments_downloads_and_caches(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Verify subscribe_instruments downloads tickers and saves to cache.

    Given: REST client with valid ticker data and invalid cache,
    When: Subscribe instruments is iterated,
    Then: Tickers returned and cache save function invoked.
    """
    FakeRestWithClient.tickers_data = [FakeTicker()]
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.RESTClient", FakeRestWithClient
    )
    cache_file = tmp_path / "symbols.csv"
    client = PolygonExchangeClient(api_key="key", symbols_cache_file=cache_file)
    monkeypatch.setattr(client, "_is_cache_valid", lambda: False)
    saved_symbols: list[list[dict[str, Any]]] = []

    def fake_save(symbols: list[dict[str, Any]]) -> None:
        saved_symbols.append(list(symbols))

    monkeypatch.setattr(client, "_save_symbols_to_cache", fake_save)
    results: list[dict[str, Any]] = []
    async for item in client.subscribe_instruments():
        results.append(item)
    assert results
    assert results[0]["ticker"] == "X:BTCUSD"
    assert saved_symbols
    assert saved_symbols[0][0]["primary_exchange"] == "GDAX"


@pytest.mark.asyncio
async def test_subscribe_instruments_saves_cache_with_missing_fields(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Verify subscribe_instruments handles tickers with missing fields.

    Given: Ticker with only minimal fields populated,
    When: Subscribe instruments is iterated,
    Then: Ticker returned with available fields only.
    """

    class SparseTicker:
        ticker = "C:EURUSD"
        name = None
        market = None
        locale = None

    FakeRestWithClient.tickers_data = [SparseTicker()]
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.RESTClient", FakeRestWithClient
    )
    cache_file = tmp_path / "symbols.csv"
    client = PolygonExchangeClient(api_key="key", symbols_cache_file=cache_file)
    monkeypatch.setattr(client, "_is_cache_valid", lambda: False)
    saved_symbols: list[list[dict[str, Any]]] = []

    def fake_save(symbols: list[dict[str, Any]]) -> None:
        saved_symbols.append(list(symbols))

    monkeypatch.setattr(client, "_save_symbols_to_cache", fake_save)
    results: list[dict[str, Any]] = []
    async for item in client.subscribe_instruments():
        results.append(item)
    assert results
    assert "ticker" in results[0]
    assert saved_symbols
    assert list(saved_symbols[0][0]) == ["ticker"]


@pytest.mark.asyncio
async def test_subscribe_instruments_sleeps_each_page(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify subscribe_instruments sleeps after each page of tickers.

    Given: REST client with 1000 tickers (one page),
    When: Subscribe instruments is iterated,
    Then: Sleep called once after page completion.
    """

    class MinimalTicker:
        def __init__(self, idx: int) -> None:
            self.ticker = f"X:TICK{idx}"
            self.market = "crypto"
            self.locale = "global"

    FakeRestWithClient.tickers_data = [MinimalTicker(i) for i in range(1000)]
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.RESTClient", FakeRestWithClient
    )
    sleeps: list[float] = []

    async def fake_sleep(duration: float) -> None:
        sleeps.append(duration)

    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.asyncio.sleep",
        fake_sleep,
    )
    client = PolygonExchangeClient(api_key="key")
    monkeypatch.setattr(client, "_is_cache_valid", lambda: False)
    monkeypatch.setattr(client, "_save_symbols_to_cache", lambda symbols: None)
    results: list[dict[str, Any]] = []
    async for item in client.subscribe_instruments():
        results.append(item)
    assert len(results) == 1000
    assert sleeps == [12]


@pytest.mark.asyncio
async def test_subscribe_instruments_empty_skips_cache_save(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify subscribe_instruments skips cache save when no tickers.

    Given: REST client with empty ticker list,
    When: Subscribe instruments is iterated,
    Then: No cache save invoked.
    """
    FakeRestWithClient.tickers_data = []
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.RESTClient", FakeRestWithClient
    )
    saved_symbols: list[list[dict[str, Any]]] = []

    def fake_save(symbols: list[dict[str, Any]]) -> None:
        saved_symbols.append(list(symbols))

    client = PolygonExchangeClient(api_key="key")
    monkeypatch.setattr(client, "_is_cache_valid", lambda: False)
    monkeypatch.setattr(client, "_save_symbols_to_cache", fake_save)
    results: list[dict[str, Any]] = []
    async for item in client.subscribe_instruments():
        results.append(item)
    assert results == []
    assert saved_symbols == []


@pytest.mark.asyncio
async def test_subscribe_instruments_handles_missing_ticker_field(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify subscribe_instruments handles objects without ticker field.

    Given: Ticker object missing required ticker field,
    When: Subscribe instruments is iterated,
    Then: Object returned without ticker key.
    """

    class NoTicker:
        def __init__(self) -> None:
            self.name = "Missing ticker"

    FakeRestWithClient.tickers_data = [NoTicker()]
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.RESTClient", FakeRestWithClient
    )
    client = PolygonExchangeClient(api_key="key")
    monkeypatch.setattr(client, "_is_cache_valid", lambda: False)
    monkeypatch.setattr(client, "_save_symbols_to_cache", lambda symbols: None)
    results: list[dict[str, Any]] = []
    async for item in client.subscribe_instruments():
        results.append(item)
    assert results
    assert "ticker" not in results[0]
    assert results[0].get("name") == "Missing ticker"


@pytest.mark.asyncio
async def test_poll_tickers_warns_fast_interval_and_handles_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify poll_tickers warns on fast interval and handles errors.

    Given: Client with 1-second polling interval,
    When: Poll tickers is called and raises error,
    Then: Warning logged and error handled gracefully.
    """
    client = PolygonExchangeClient(api_key="key")
    symbols = ["C:EURUSD"]
    warnings: list[str] = []

    def fake_warning(message: str) -> None:
        warnings.append(str(message))

    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.logger.warning",
        fake_warning,
    )

    async def fake_get_ticker(symbol: str) -> Any:
        return SimpleNamespace(bid=1.0, ask=1.0, last=1.0, high=1.0, low=1.0, change_pct=0.0)

    monkeypatch.setattr(client, "get_ticker", fake_get_ticker)
    sleep_calls: list[float] = []

    async def fake_sleep(duration: float) -> None:
        sleep_calls.append(duration)
        if len(sleep_calls) == 1:
            raise ValueError("boom")
        raise KeyboardInterrupt()

    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.asyncio.sleep",
        fake_sleep,
    )
    await client.poll_tickers(symbols, interval_seconds=1.0)
    assert warnings
    assert sleep_calls and len(sleep_calls) >= 1


@pytest.mark.asyncio
async def test_poll_tickers_warns_on_fast_interval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify poll_tickers logs warning for fast polling interval.

    Given: Client configured with 1-second interval,
    When: Poll tickers is called,
    Then: Rate limit warning message logged.
    """
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.RESTClient", FakeRestWithClient
    )
    client = PolygonExchangeClient(api_key="key")
    warnings: list[str] = []

    async def fake_get_ticker(symbol: str) -> SimpleNamespace:
        return SimpleNamespace(
            bid=1.0,
            ask=1.1,
            last=1.05,
            high=1.2,
            low=0.9,
            change_pct=0.5,
        )

    monkeypatch.setattr(client, "get_ticker", fake_get_ticker)

    async def raise_keyboard_interrupt(duration: float) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.asyncio.sleep",
        raise_keyboard_interrupt,
    )
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.logger.warning",
        lambda message: warnings.append(str(message)),
    )
    await client.poll_tickers(symbols=["C:EURUSD"], interval_seconds=1.0)
    assert any("may exceed rate limit" in message for message in warnings)


@pytest.mark.asyncio
async def test_poll_tickers_skips_warning_when_interval_ok(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify poll_tickers skips warning for safe polling interval.

    Given: Client configured with 30-second interval,
    When: Poll tickers is called,
    Then: No warning message logged.
    """
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.RESTClient", FakeRestWithClient
    )
    client = PolygonExchangeClient(api_key="key")
    warnings: list[str] = []

    async def fake_get_ticker(symbol: str) -> SimpleNamespace:
        return SimpleNamespace(
            bid=1.0,
            ask=1.0,
            last=1.0,
            high=1.0,
            low=1.0,
            change_pct=0.0,
        )

    monkeypatch.setattr(client, "get_ticker", fake_get_ticker)

    async def stop_after_first_sleep(duration: float) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.asyncio.sleep",
        stop_after_first_sleep,
    )
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.logger.warning",
        lambda message: warnings.append(str(message)),
    )
    await client.poll_tickers(symbols=["C:EURUSD", "X:BTCUSD"], interval_seconds=30.0)
    assert warnings == []


@pytest.mark.asyncio
async def test_poll_tickers_handles_polling_error_and_recovers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify poll_tickers recovers from transient errors.

    Given: Get ticker function that raises RuntimeError,
    When: Poll tickers is called,
    Then: Error caught and polling continues until cancelled.
    """
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.RESTClient", FakeRestWithClient
    )
    client = PolygonExchangeClient(api_key="key")

    async def raise_runtime(symbol: str) -> Any:
        raise RuntimeError("boom")

    monkeypatch.setattr(client, "get_ticker", raise_runtime)
    sleep_calls: list[str] = []

    async def fake_sleep(duration: float) -> None:
        sleep_calls.append(f"sleep-{len(sleep_calls)}")
        if len(sleep_calls) == 1:
            raise RuntimeError("outer")
        raise asyncio.CancelledError

    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.asyncio.sleep",
        fake_sleep,
    )
    with pytest.raises(asyncio.CancelledError):
        await client.poll_tickers(symbols=["C:EURUSD"], interval_seconds=0.1)
    assert sleep_calls[0] == "sleep-0"


@pytest.mark.asyncio
async def test_poll_tickers_recovers_after_outer_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify poll_tickers recovers from outer loop exceptions.

    Given: Sleep function that raises RuntimeError then KeyboardInterrupt,
    When: Poll tickers is called,
    Then: First error handled and exits cleanly on interrupt.
    """
    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.RESTClient", FakeRestWithClient
    )
    client = PolygonExchangeClient(api_key="key")

    async def fake_get_ticker(symbol: str) -> Any:
        return SimpleNamespace(bid=1.0, ask=1.0, last=1.0, high=1.0, low=1.0, change_pct=0.0)

    monkeypatch.setattr(client, "get_ticker", fake_get_ticker)
    sleep_calls: list[str] = []

    async def fake_sleep(duration: float) -> None:
        if not sleep_calls:
            sleep_calls.append("outer-error")
            raise RuntimeError("tick polling error")
        sleep_calls.append("exit")
        raise KeyboardInterrupt

    monkeypatch.setattr(
        "snapper.infrastructure.exchanges.implementations.polygon.asyncio.sleep",
        fake_sleep,
    )
    await client.poll_tickers(symbols=["C:EURUSD"], interval_seconds=0.1)
    assert sleep_calls == ["outer-error", "exit"]


class ExposedPolygonSymbolUpdater(PolygonSymbolUpdaterService):
    """Exposed updater for testing protected methods."""

    async def update_database_public(self, symbols: list[dict[str, Any]]) -> None:
        """Expose _update_database for testing."""
        await super()._update_database(symbols)

    def match_symbol_public(self, ticker: str, symbol_data: dict[str, Any]) -> str | None:
        """Expose _match_polygon_to_native for testing."""
        return super()._match_polygon_to_native(ticker, symbol_data)

    def determine_asset_type_public(self, ticker: str) -> str:
        """Expose _determine_polygon_asset_type for testing."""
        return super()._determine_polygon_asset_type(ticker)


@pytest.fixture()
def polygon_updater(
    tmp_path: Path,
) -> Iterator[tuple[ExposedPolygonSymbolUpdater, DatabaseRepository]]:
    """Provide Polygon updater instance with test database."""
    db_path = tmp_path / "polygon_symbols.sqlite"
    repository = DatabaseRepository(f"sqlite:///{db_path}")
    repository.create_all()
    updater = ExposedPolygonSymbolUpdater(update_threshold_hours=1, force=True)
    updater.repository = repository
    yield updater, repository
    repository.engine.dispose()


@pytest.mark.asyncio()
async def test_update_existing_symbols_when_insert_disabled(
    polygon_updater: tuple[ExposedPolygonSymbolUpdater, DatabaseRepository],
) -> None:
    """Verify updater updates existing symbols only when insert disabled.

    Given: Existing BTC-USD catalog entry and insert_new=False,
    When: Update database called with BTC and EUR symbols,
    Then: BTC alias created, EUR not inserted (no catalog row).
    """
    updater, repository = polygon_updater
    original_timestamp = datetime(2024, 1, 1, tzinfo=UTC)
    with repository.get_session() as session:
        assert isinstance(session, Session)
        session.add(
            SymbolCatalog(
                native_symbol="BTC-USD",
                base="BTC",
                quote="USD",
                asset_type="crypto",
                created_at=original_timestamp,
                updated_at=original_timestamp,
            )
        )
        session.commit()
    symbols: list[dict[str, Any]] = [
        {
            "ticker": "X:BTCUSD",
            "base_currency_symbol": "BTC",
            "currency_symbol": "USD",
        },
        {
            "ticker": "C:EURUSD",
            "base_currency_symbol": "EUR",
            "currency_symbol": "USD",
        },
    ]
    await updater.update_database_public(symbols)
    with repository.get_session() as session:
        assert isinstance(session, Session)
        btc_alias = session.execute(
            select(SymbolAlias).where(
                SymbolAlias.native_symbol == "BTC-USD",
                SymbolAlias.exchange == "polygon",
                SymbolAlias.channel == "rest",
            )
        ).scalar_one()
        eur_catalog = session.execute(
            select(SymbolCatalog).where(SymbolCatalog.native_symbol == "EUR-USD")
        ).scalar_one_or_none()
    assert btc_alias.exchange_symbol == "X:BTCUSD"
    assert btc_alias.updated_at.replace(tzinfo=None) > original_timestamp.replace(tzinfo=None)
    assert eur_catalog is None


@pytest.mark.asyncio()
async def test_update_existing_alias_exchange_symbol(
    polygon_updater: tuple[ExposedPolygonSymbolUpdater, DatabaseRepository],
) -> None:
    """Verify updater updates existing alias when exchange_symbol changes.

    Given: Existing BTC-USD catalog and alias with old exchange_symbol,
    When: Update database called with new exchange_symbol for same pair,
    Then: Alias exchange_symbol updated and stats reflect update.
    """
    updater, repository = polygon_updater
    original_timestamp = datetime(2024, 1, 1, tzinfo=UTC)
    with repository.get_session() as session:
        assert isinstance(session, Session)
        session.add(
            SymbolCatalog(
                native_symbol="BTC-USD",
                base="BTC",
                quote="USD",
                asset_type="crypto",
                created_at=original_timestamp,
                updated_at=original_timestamp,
            )
        )
        session.add(
            SymbolAlias(
                native_symbol="BTC-USD",
                exchange="polygon",
                channel="rest",
                exchange_symbol="X:BTCOLD",
                created_at=original_timestamp,
                updated_at=original_timestamp,
            )
        )
        session.commit()
    symbols: list[dict[str, Any]] = [
        {
            "ticker": "X:BTCUSD",
            "base_currency_symbol": "BTC",
            "currency_symbol": "USD",
        },
    ]
    await updater.update_database_public(symbols)
    with repository.get_session() as session:
        assert isinstance(session, Session)
        btc_alias = session.execute(
            select(SymbolAlias).where(
                SymbolAlias.native_symbol == "BTC-USD",
                SymbolAlias.exchange == "polygon",
                SymbolAlias.channel == "rest",
            )
        ).scalar_one()
    assert btc_alias.exchange_symbol == "X:BTCUSD"
    assert btc_alias.updated_at.replace(tzinfo=None) > original_timestamp.replace(tzinfo=None)


@pytest.mark.asyncio()
async def test_existing_alias_unchanged_when_same_symbol(
    polygon_updater: tuple[ExposedPolygonSymbolUpdater, DatabaseRepository],
) -> None:
    """Verify updater leaves alias unchanged when exchange_symbol matches.

    Given: Existing BTC-USD catalog and alias with same exchange_symbol,
    When: Update database called with identical ticker,
    Then: Alias unchanged and updated_at not modified.
    """
    updater, repository = polygon_updater
    original_timestamp = datetime(2024, 1, 1, tzinfo=UTC)
    with repository.get_session() as session:
        assert isinstance(session, Session)
        session.add(
            SymbolCatalog(
                native_symbol="BTC-USD",
                base="BTC",
                quote="USD",
                asset_type="crypto",
                created_at=original_timestamp,
                updated_at=original_timestamp,
            )
        )
        session.add(
            SymbolAlias(
                native_symbol="BTC-USD",
                exchange="polygon",
                channel="rest",
                exchange_symbol="X:BTCUSD",
                created_at=original_timestamp,
                updated_at=original_timestamp,
            )
        )
        session.commit()
    symbols: list[dict[str, Any]] = [
        {
            "ticker": "X:BTCUSD",
            "base_currency_symbol": "BTC",
            "currency_symbol": "USD",
        },
    ]
    await updater.update_database_public(symbols)
    with repository.get_session() as session:
        assert isinstance(session, Session)
        btc_alias = session.execute(
            select(SymbolAlias).where(
                SymbolAlias.native_symbol == "BTC-USD",
                SymbolAlias.exchange == "polygon",
                SymbolAlias.channel == "rest",
            )
        ).scalar_one()
    assert btc_alias.exchange_symbol == "X:BTCUSD"
    assert btc_alias.updated_at.replace(tzinfo=None) == original_timestamp.replace(tzinfo=None)


@pytest.mark.asyncio()
async def test_insert_new_symbols_when_enabled(tmp_path: Path) -> None:
    """Verify updater inserts new symbols when insert_new enabled.

    Given: Empty database and insert_new=True,
    When: Update database called with stock and index symbols,
    Then: Both catalog and alias rows inserted with correct attributes.
    """
    db_path = tmp_path / "polygon_insert.sqlite"
    repository = DatabaseRepository(f"sqlite:///{db_path}")
    repository.create_all()
    updater = ExposedPolygonSymbolUpdater(
        update_threshold_hours=1,
        force=True,
        insert_new=True,
    )
    updater.repository = repository
    symbols: list[dict[str, Any]] = [
        {
            "ticker": "AAPL",
            "currency_symbol": "USD",
        },
        {
            "ticker": "I:SPX",
        },
    ]
    await updater.update_database_public(symbols)
    with repository.get_session() as session:
        assert isinstance(session, Session)
        stock_catalog = session.execute(
            select(SymbolCatalog).where(SymbolCatalog.native_symbol == "AAPL")
        ).scalar_one()
        stock_alias = session.execute(
            select(SymbolAlias).where(
                SymbolAlias.native_symbol == "AAPL",
                SymbolAlias.exchange == "polygon",
                SymbolAlias.channel == "rest",
            )
        ).scalar_one()
        index_catalog = session.execute(
            select(SymbolCatalog).where(SymbolCatalog.native_symbol == "SPX")
        ).scalar_one()
        index_alias = session.execute(
            select(SymbolAlias).where(
                SymbolAlias.native_symbol == "SPX",
                SymbolAlias.exchange == "polygon",
                SymbolAlias.channel == "rest",
            )
        ).scalar_one()
    assert stock_alias.exchange_symbol == "AAPL"
    assert stock_catalog.quote == "USD"
    assert stock_catalog.asset_type == "equity"
    assert index_alias.exchange_symbol == "I:SPX"
    assert index_catalog.quote is None
    assert index_catalog.asset_type == "index"
    repository.engine.dispose()


@pytest.mark.parametrize(
    "ticker,symbol_data,expected",
    [
        ("X:ETHUSD", {"base_currency_symbol": "ETH", "currency_symbol": "USD"}, "ETH-USD"),
        ("C:GBPUSD", {"base_currency_symbol": "GBP", "currency_symbol": "USD"}, "GBP-USD"),
        ("I:NDX", {}, "NDX"),
        ("TSLA", {"currency_symbol": "USD"}, "TSLA"),
        ("X:BAD", {}, None),
    ],
)
def test_match_polygon_to_native(
    ticker: str, symbol_data: dict[str, Any], expected: str | None
) -> None:
    """Verify polygon ticker to native symbol mapping.

    Given: Various ticker formats and symbol data,
    When: Match symbol called,
    Then: Expected native symbol returned or None for invalid.
    """
    updater = ExposedPolygonSymbolUpdater(update_threshold_hours=1, force=True)
    assert updater.match_symbol_public(ticker, symbol_data) == expected


def test_get_default_kwargs() -> None:
    """Verify default kwargs returns expected configuration.

    Given: Mock AppSettings object,
    When: get_default_kwargs called,
    Then: Weekly threshold and insert_new=False returned.
    """
    mock_settings = MagicMock(spec=AppSettings)
    kwargs = PolygonSymbolUpdaterService.get_default_kwargs(mock_settings)
    assert kwargs["update_threshold_hours"] == 168
    assert kwargs["force"] is False
    assert kwargs["insert_new"] is False


def test_get_setting_key() -> None:
    """Verify setting key returns correct identifier.

    Given: Polygon symbol updater instance,
    When: _get_setting_key called,
    Then: Expected setting key returned.
    """
    updater = ExposedPolygonSymbolUpdater(update_threshold_hours=1, force=True)
    assert updater._get_setting_key() == "polygon_symbols_last_update"


@pytest.mark.asyncio()
async def test_update_database_skips_entries_without_ticker(
    polygon_updater: tuple[ExposedPolygonSymbolUpdater, DatabaseRepository],
) -> None:
    """Verify update_database skips entries without valid ticker.

    Given: Symbols list with None, empty, and missing tickers,
    When: Update database called,
    Then: No catalog or alias rows created in database.
    """
    updater, repository = polygon_updater
    symbols: list[dict[str, Any]] = [
        {"ticker": None},
        {},
        {"ticker": ""},
    ]
    await updater.update_database_public(symbols)
    with repository.get_session() as session:
        assert isinstance(session, Session)
        catalog_count = session.execute(select(SymbolCatalog)).scalars().all()
        alias_count = session.execute(select(SymbolAlias)).scalars().all()
    assert len(catalog_count) == 0
    assert len(alias_count) == 0


@pytest.mark.asyncio()
async def test_update_database_skips_unmatchable_symbols(
    polygon_updater: tuple[ExposedPolygonSymbolUpdater, DatabaseRepository],
) -> None:
    """Verify update_database skips symbols that cannot be matched.

    Given: Symbol with ticker but no currency info,
    When: Update database called,
    Then: No catalog or alias rows created for unmatchable symbol.
    """
    updater, repository = polygon_updater
    symbols: list[dict[str, Any]] = [
        {"ticker": "X:BAD", "base_currency_symbol": None, "currency_symbol": None},
    ]
    await updater.update_database_public(symbols)
    with repository.get_session() as session:
        assert isinstance(session, Session)
        catalog_count = session.execute(select(SymbolCatalog)).scalars().all()
        alias_count = session.execute(select(SymbolAlias)).scalars().all()
    assert len(catalog_count) == 0
    assert len(alias_count) == 0


@pytest.mark.xdist_group(name="database")
@pytest.mark.timeout(30)
@pytest.mark.asyncio()
async def test_update_database_commits_in_batches(tmp_path: Path) -> None:
    """Verify update_database commits large datasets in batches.

    Given: 1100 symbols with insert_new=True,
    When: Update database called,
    Then: All catalog and alias rows inserted via batch commits.
    """
    db_path = tmp_path / "polygon_batch.sqlite"
    repository = DatabaseRepository(f"sqlite:///{db_path}")
    repository.create_all()
    updater = ExposedPolygonSymbolUpdater(update_threshold_hours=1, force=True, insert_new=True)
    updater.repository = repository
    symbols: list[dict[str, Any]] = [
        {"ticker": f"SYM{i:04d}", "currency_symbol": "USD"} for i in range(1100)
    ]
    await updater.update_database_public(symbols)
    with repository.get_session() as session:
        assert isinstance(session, Session)
        catalog_count = len(session.execute(select(SymbolCatalog)).scalars().all())
        alias_count = len(session.execute(select(SymbolAlias)).scalars().all())
    assert catalog_count == 1100
    assert alias_count == 1100
    repository.engine.dispose()


@pytest.mark.asyncio()
async def test_update_database_handles_exception(
    polygon_updater: tuple[ExposedPolygonSymbolUpdater, DatabaseRepository],
) -> None:
    """Verify update_database raises when repository is None.

    Given: Updater with repository set to None,
    When: Update database called,
    Then: AssertionError raised.
    """
    updater, _repository = polygon_updater
    updater.repository = None
    symbols: list[dict[str, Any]] = [
        {"ticker": "X:BTCUSD", "base_currency_symbol": "BTC", "currency_symbol": "USD"}
    ]
    with pytest.raises(AssertionError):
        await updater.update_database_public(symbols)


@pytest.mark.asyncio()
async def test_update_database_logs_and_reraises(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify update_database logs error and re-raises on failure.

    Given: Repository that raises RuntimeError on get_session,
    When: Update database called,
    Then: Error logged and RuntimeError re-raised.
    """
    updater = ExposedPolygonSymbolUpdater(update_threshold_hours=1, force=True)
    repository = MagicMock()
    repository.get_session.side_effect = RuntimeError("db failure")
    updater.repository = repository
    logger_mock = MagicMock()
    monkeypatch.setattr("snapper.application.updaters.symbols.polygon.logger", logger_mock)
    symbols = [{"ticker": "X:BTCUSD", "base_currency_symbol": "BTC", "currency_symbol": "USD"}]
    with pytest.raises(RuntimeError):
        await updater.update_database_public(symbols)
    logger_mock.error.assert_called_once()


def test_match_polygon_crypto_fallback_parsing() -> None:
    """Verify crypto ticker fallback parsing extracts base/quote.

    Given: Crypto ticker without explicit currency data,
    When: Match symbol called,
    Then: Symbol parsed from ticker format or None for short.
    """
    updater = ExposedPolygonSymbolUpdater(update_threshold_hours=1, force=True)
    result = updater.match_symbol_public("X:BTCUSD", {})
    assert result == "BTC-USD"
    result = updater.match_symbol_public("X:SHORT", {})
    assert result is None


def test_match_polygon_forex_fallback_parsing() -> None:
    """Verify forex ticker fallback parsing for 6-char pairs.

    Given: Forex tickers with C: prefix,
    When: Match symbol called,
    Then: 6-char pair parsed, others return None.
    """
    updater = ExposedPolygonSymbolUpdater(update_threshold_hours=1, force=True)
    result = updater.match_symbol_public("C:EURUSD", {})
    assert result == "EUR-USD"
    result = updater.match_symbol_public("C:EURUS", {})
    assert result is None
    result = updater.match_symbol_public("C:EURUSDD", {})
    assert result is None


def test_create_exchange_client_reuses_cached_instance() -> None:
    """Verify exchange client is cached and reused.

    Given: Updater with configured API key,
    When: _create_exchange_client called twice,
    Then: Same client instance returned.
    """
    updater = ExposedPolygonSymbolUpdater(update_threshold_hours=1, force=True)
    mock_settings = MagicMock()
    mock_settings.polygon_api_key = "test_key_123"
    updater.settings = mock_settings
    client1 = updater._create_exchange_client()
    assert client1 is not None
    client2 = updater._create_exchange_client()
    assert client2 is client1


def test_create_exchange_client_raises_when_no_api_key() -> None:
    """Verify exchange client raises when API key missing.

    Given: Updater with polygon_api_key set to None,
    When: _create_exchange_client called,
    Then: ValueError raised with configuration message.
    """
    updater = ExposedPolygonSymbolUpdater(update_threshold_hours=1, force=True)
    mock_settings = MagicMock()
    mock_settings.polygon_api_key = None
    updater.settings = mock_settings
    with pytest.raises(ValueError, match="Polygon API key not configured"):
        updater._create_exchange_client()


@pytest.mark.parametrize(
    "ticker,expected_asset_type",
    [
        ("X:BTCUSD", "crypto"),
        ("X:ETHUSD", "crypto"),
        ("C:EURUSD", "forex"),
        ("C:GBPJPY", "forex"),
        ("I:SPX", "index"),
        ("I:NDX", "index"),
        ("AAPL", "equity"),
        ("TSLA", "equity"),
    ],
)
def test_determine_polygon_asset_type(ticker: str, expected_asset_type: str) -> None:
    """Verify asset type determination from Polygon ticker prefix.

    Given: Various ticker formats with different prefixes,
    When: _determine_polygon_asset_type called,
    Then: Correct asset type returned based on prefix.
    """
    updater = ExposedPolygonSymbolUpdater(update_threshold_hours=1, force=True)
    assert updater.determine_asset_type_public(ticker) == expected_asset_type
