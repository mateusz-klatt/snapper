"""Polygon.io market data provider client implementation.

This module provides PolygonExchangeClient for accessing market data from
Polygon.io, a comprehensive market data provider for stocks, forex, and
cryptocurrencies. It supports
Market Data
    Real-time and historical tickers
    OHLCV aggregates
    Previous close data
    Grouped daily aggregates
Features
    Automatic rate limiting with configurable requests per minute
    Retry strategy with exponential backoff for 429 errors
    Local caching of symbol lists for performance
    Support for crypto (X:), forex (C:), and stock tickers
Note: This is a read-only data provider. Order management methods raise
NotImplementedError as Polygon.io does not support trading.
The client uses a custom retry policy optimized for Polygon.io's rate
limiting behavior, sleeping 24 seconds on 429 responses.
"""

import asyncio
import csv
import itertools
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC
from datetime import date
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Any
from typing import cast
from urllib.parse import urlparse

import certifi
import urllib3
from loguru import logger
from massive import RESTClient
from urllib3.util.retry import Retry

from snapper.core.types import ExchangeEnum
from snapper.data.repository import Repository
from snapper.infrastructure.exchanges.base import ExchangeClientBase
from snapper.infrastructure.exchanges.contracts import AccountBalance
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import ExchangeOrderRequest
from snapper.infrastructure.exchanges.contracts import ExchangeOrderSnapshot
from snapper.infrastructure.exchanges.contracts import ExchangeOrderStatusEnum
from snapper.infrastructure.exchanges.contracts import ExecutionUpdate
from snapper.infrastructure.exchanges.contracts import OhlcvSnapshot
from snapper.infrastructure.exchanges.contracts import TickerSnapshot
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate
from snapper.infrastructure.exchanges.schemas.polygon import PolygonAgg
from snapper.infrastructure.exchanges.schemas.polygon import PolygonGroupedAgg
from snapper.infrastructure.exchanges.schemas.polygon import PolygonPreviousClose

_MARKET_DATA_ONLY_MSG = "PolygonExchangeClient provides market data only."

_POLYGON_REST_BASE = "https://api.polygon.io"
"""REST base URL for the Massive client (formerly Polygon.io).

The client library was migrated from ``polygon-api-client`` to its official
successor ``massive``, which relaxes the ``certifi`` upper bound
(``<2027.0.0``) that ``polygon-api-client`` 1.16.3 pinned (``<2026.0.0``) and so
unblocks ``ccxt`` 4.5.65 (which requires ``certifi==2026.6.17``). ``massive``
defaults to ``https://api.massive.com``; the base is pinned to the still-serving
``https://api.polygon.io`` so the swap is zero-runtime-change (identical
endpoint, key, and Bearer auth that run in production today — ``massive`` is a
byte-identical rebrand of the client). Switch to ``https://api.massive.com``
once the existing key is confirmed to authenticate there."""


@dataclass
class PolygonSplitEvent:
    """One corporate split action from the Polygon reference endpoint.

    Attributes:
        ticker: Polygon ticker the split applies to (equities use the
            native symbol form, e.g. ``NFLX`` or ``BRK.B``).
        execution_date: UTC calendar day the split took effect.
        split_from: Pre-split share count of the ratio.
        split_to: Post-split share count of the ratio.
    """

    ticker: str
    execution_date: date
    split_from: float
    split_to: float

    @property
    def expected_break_ratio(self) -> float:
        """Raw close-over-close ratio an UNADJUSTED boundary would show.

        A forward split ``from:to`` divides the price by ``to/from``,
        so mixed-basis data jumps by ``from/to`` at the basis boundary
        (0.1 for a 1:10 forward split, 20.0 for a 20:1 reverse split).

        Returns:
            Expected consecutive-close ratio at a stale-basis boundary.
        """
        return self.split_from / self.split_to


@dataclass(slots=True)
class PolygonAggregateResponse:
    """Validated bars plus the vendor response evidence for their window.

    Attributes:
        aggregates: Validated aggregate bars.
        affirmative: Whether every response page reported status ``OK``.
        results_count: Sum of the vendor-declared page result counts.
        query_count: Sum of the vendor-declared page query counts.
        request_ids: Vendor request identifiers for audit correlation.
        adjusted: Vendor-declared adjustment setting when present.
    """

    aggregates: list[PolygonAgg]
    affirmative: bool
    results_count: int
    query_count: int
    request_ids: tuple[str, ...]
    adjusted: bool | None


@dataclass
class PolygonTickerSnapshot(TickerSnapshot):
    """Extended ticker snapshot with Polygon-specific fields.

    Attributes:
        bid_qty: Bid size quantity.
        ask_qty: Ask size quantity.
        volume: Trading volume.
        vwap: Volume-weighted average price.
        low: Session low price.
        high: Session high price.
        change: Absolute price change.
        change_pct: Percentage price change.
    """

    bid_qty: float = 0.0
    ask_qty: float = 0.0
    volume: float = 0.0
    vwap: float = 0.0
    low: float = 0.0
    high: float = 0.0
    change: float = 0.0
    change_pct: float = 0.0


class PolygonRetryPolicy(Retry):
    """Custom retry policy for Polygon.io API rate limiting.

    Extends urllib3 Retry to handle Polygon's aggressive rate limiting
    by sleeping 24 seconds on first retry (typically a 429 response).
    """

    def get_backoff_time(self) -> float:
        """Calculate backoff time, defaulting to 24s on first retry.

        Returns:
            Backoff time in seconds.
        """
        backoff = super().get_backoff_time()
        if backoff == 0 and self.history:
            backoff = 24.0
        return backoff

    def sleep(self, response: Any = None) -> None:
        """Sleep with logging for retry visibility.

        Args:
            response: The response that triggered the retry.
        """
        backoff = self.get_backoff_time()
        if backoff > 0:
            retry_num = len(self.history)
            logger.warning(
                f"Polygon API retry #{retry_num}: sleeping {backoff:.1f}s (likely 429 rate limit)"
            )
        super().sleep(response)


class PolygonExchangeClient(ExchangeClientBase):
    """Polygon.io market data client for stocks, forex, and crypto.

    This client provides read-only access to Polygon.io's market data
    API. It does not support trading operations.

    Features automatic rate limiting and retry handling optimized for
    Polygon.io's API behavior.

    Attributes:
        api_key: Polygon.io API key.
        rate_limit: Maximum requests per minute.
        symbols_cache_file: Path to local symbols cache file.
        cache_ttl_hours: Cache time-to-live in hours.
    """

    def __init__(
        self,
        api_key: str,
        rate_limit_per_minute: int = 5,
        symbols_cache_file: str | Path = "data/polygon/reference/symbols.csv",
        cache_ttl_hours: int = 168,
        verbose: bool = True,
        trace: bool = True,
        repository: Repository | None = None,
    ) -> None:
        """Initialize Polygon.io market data client.

        Args:
            api_key: Polygon.io API key.
            rate_limit_per_minute: Max API requests per minute (default: 5).
            symbols_cache_file: Path for caching symbol lists.
            cache_ttl_hours: Cache TTL in hours (default: 168 = 1 week).
            verbose: Enable verbose logging in Polygon SDK.
            trace: Enable request tracing in Polygon SDK.
            repository: Database repository (not used, for interface compatibility).
        """
        super().__init__(repository=repository, exchange_name=ExchangeEnum.POLYGON)
        self.api_key = api_key
        self.rate_limit = rate_limit_per_minute
        self._request_timestamps: list[float] = []
        self._client = RESTClient(
            api_key=api_key, trace=trace, verbose=verbose, base=_POLYGON_REST_BASE
        )
        self.symbols_cache_file = (
            Path(symbols_cache_file) if isinstance(symbols_cache_file, str) else symbols_cache_file
        )
        self.cache_ttl_hours = cache_ttl_hours
        retry_strategy = PolygonRetryPolicy(
            total=5,
            status_forcelist=[413, 429, 499, 500, 502, 503, 504],
            backoff_factor=12.0,
            backoff_jitter=0.0,
            raise_on_status=False,
        )
        if hasattr(self._client, "client"):
            self._client.client = urllib3.PoolManager(
                num_pools=10,
                headers=self._client.headers,
                ca_certs=certifi.where(),
                cert_reqs="CERT_REQUIRED",
                retries=retry_strategy,
            )
            logger.debug("Patched Polygon SDK retry: PolygonRetryPolicy (24s, 24s, 48s, 96s, 120s)")

    async def connect(self) -> None:
        """Re-enable REST dispatch for this REST-only client.

        There is no connection to establish, but the base lifecycle
        contract requires ``connect()`` to reopen the bounded REST pool
        as its first step so a client reused after ``disconnect()`` can
        dispatch again.
        """
        self._reopen_rest_pool()

    async def disconnect(self) -> None:
        """Release the dedicated REST pool for this REST-only client.

        There is no connection to tear down, but blocking SDK calls run
        on the per-client bounded pool (P1-5), which must be freed when
        an owner finishes with the client.
        """
        self._shutdown_rest_pool()

    async def _wait_for_rate_limit(self) -> None:
        """Wait if necessary to comply with rate limiting.

        Tracks request timestamps and ensures requests don't exceed
        the configured rate limit per minute.
        """
        now = time.time()
        self._request_timestamps = [ts for ts in self._request_timestamps if now - ts < 60]
        if len(self._request_timestamps) >= self.rate_limit:
            oldest = self._request_timestamps[0]
            wait_time = 60 - (now - oldest) + 0.1
            logger.debug(f"Rate limit reached ({self.rate_limit}/min). Waiting {wait_time:.1f}s...")
            await asyncio.sleep(wait_time)
        if self._request_timestamps:
            last_request = self._request_timestamps[-1]
            min_interval = 60.0 / self.rate_limit
            time_since_last = now - last_request
            if time_since_last < min_interval:
                wait_time = min_interval - time_since_last
                logger.debug(f"Waiting {wait_time:.1f}s to maintain rate limit...")
                await asyncio.sleep(wait_time)
        self._request_timestamps.append(time.time())

    async def _make_request_with_retry(self, request_func: Any, max_retries: int = 5) -> Any:
        """Execute a request with automatic retry on rate limit errors.

        Args:
            request_func: Callable that performs the API request.
            max_retries: Maximum number of retry attempts.

        Returns:
            The response from the successful request.

        Raises:
            RuntimeError: If all retries are exhausted.
        """
        for attempt in range(max_retries):
            try:
                await self._wait_for_rate_limit()
                self._record_rest_call()
                response = await self._dispatch_blocking(request_func)
                return response
            except Exception as e:
                error_str = str(e)
                if "429" in error_str or "too many" in error_str.lower():
                    if attempt < max_retries - 1:
                        backoff = 2**attempt
                        logger.warning(
                            f"Rate limit 429 on attempt {attempt + 1}/{max_retries}. "
                            f"Backing off {backoff}s..."
                        )
                        await asyncio.sleep(backoff)
                        continue
                    else:
                        logger.error(f"Exhausted all {max_retries} retries on 429 error")
                        raise
                else:
                    raise
        raise RuntimeError(f"Failed after {max_retries} retries")

    @staticmethod
    def _format_aggregate_boundary(value: datetime | date | str | int) -> str | int:
        """Format date/datetime for Polygon API.

        Args:
            value: Date value (int ms, datetime, date, or string).

        Returns:
            Milliseconds timestamp or ISO string.

        Raises:
            ValueError: If datetime is timezone-naive.
        """
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, datetime):
            if value.tzinfo is None:
                raise ValueError(
                    f"Cannot format naive datetime {value}. Use timezone-aware datetime."
                )
            return int(value.timestamp() * 1000)
        if isinstance(value, date):
            return value.isoformat()
        return str(value)

    @staticmethod
    def _validate_aggregate_pages(pages: list[dict[str, Any]]) -> list[PolygonAgg]:
        """Validate aggregate records from decoded response pages.

        Args:
            pages: Decoded Polygon aggregate response pages.

        Returns:
            Valid aggregate records with timestamps.
        """
        validated: list[PolygonAgg] = []
        for page in pages:
            for item in page.get("results", []):
                try:
                    agg = PolygonAgg(
                        open=item.get("o"),
                        high=item.get("h"),
                        low=item.get("l"),
                        close=item.get("c"),
                        volume=item.get("v"),
                        vwap=item.get("vw"),
                        timestamp=item.get("t"),
                        transactions=item.get("n"),
                        otc=item.get("otc"),
                    )
                    if agg.timestamp is None:
                        logger.debug("Skipping aggregate with missing timestamp")
                        continue
                    validated.append(agg)
                except Exception as e:
                    logger.warning(f"Skipping invalid aggregate record: {e}")
        return validated

    async def list_aggregates(
        self,
        ticker: str,
        multiplier: int,
        timespan: str,
        from_date: datetime | date | str | int,
        to_date: datetime | date | str | int,
        *,
        adjusted: bool = True,
        sort: str = "asc",
        limit: int = 50000,
    ) -> PolygonAggregateResponse:
        """Fetch aggregated OHLCV bars from Polygon.io.

        Args:
            ticker: Symbol (e.g., 'X:BTCUSD', 'C:EURUSD').
            multiplier: Bar size multiplier.
            timespan: Time unit.
            from_date: Start date/timestamp.
            to_date: End date/timestamp.
            adjusted: Whether to adjust for splits.
            sort: Sort order ('asc' or 'desc').
            limit: Maximum bars per page.

        Returns:
            Validated bars and vendor response-envelope evidence.
        """

        def _request() -> list[dict[str, Any]]:
            pages: list[dict[str, Any]] = []
            response = self._client.list_aggs(
                ticker,
                multiplier,
                timespan,
                self._format_aggregate_boundary(from_date),
                self._format_aggregate_boundary(to_date),
                adjusted=adjusted,
                sort=sort,
                limit=limit,
                raw=True,
            )
            while response is not None:
                decoded = cast(dict[str, Any], self._client._decode(response))
                pages.append(decoded)
                next_url = decoded.get("next_url")
                if not isinstance(next_url, str):
                    break
                logger.info("Fetched aggregate page. Sleeping 12s before next page (5 req/min)...")
                time.sleep(12)
                parsed = urlparse(next_url)
                next_path = parsed.path
                if parsed.query:
                    next_path = f"{next_path}?{parsed.query}"
                response = self._client._get(path=next_path, params={}, raw=True)
            return pages

        pages = await self._make_request_with_retry(_request)
        validated = self._validate_aggregate_pages(pages)
        if validated:
            logger.info(f"Total fetched: {len(validated)} bars")
        request_ids = tuple(
            request_id for page in pages if isinstance(request_id := page.get("request_id"), str)
        )
        results_counts = (page.get("resultsCount") for page in pages)
        results_count = sum(count for count in results_counts if isinstance(count, int))
        query_counts = (page.get("queryCount") for page in pages)
        query_count = sum(count for count in query_counts if isinstance(count, int))
        adjusted_values = (page.get("adjusted") for page in pages)
        adjusted_value = next(
            (value for value in adjusted_values if isinstance(value, bool)),
            None,
        )
        return PolygonAggregateResponse(
            aggregates=validated,
            affirmative=bool(pages) and all(page.get("status") == "OK" for page in pages),
            results_count=results_count,
            query_count=query_count,
            request_ids=request_ids,
            adjusted=adjusted_value,
        )

    async def list_splits(
        self,
        *,
        execution_date_gte: date | str,
        limit: int = 1000,
    ) -> list[PolygonSplitEvent]:
        """Fetch stock split events from the Polygon reference endpoint.

        Splits executed while the incremental CSV cache is being
        collected freeze earlier files at a pre-split price basis, so
        the split-repair tooling needs the authoritative event list to
        decide which symbols must be re-fetched.

        Args:
            execution_date_gte: Inclusive lower bound on the split
                execution date.
            limit: Maximum events per page (the SDK iterator paginates
                past it transparently).

        Returns:
            Validated split events; malformed records are skipped with
            a warning.
        """
        formatted = (
            execution_date_gte.isoformat()
            if isinstance(execution_date_gte, date)
            else str(execution_date_gte)
        )

        def _request() -> list[Any]:
            return list(self._client.list_splits(execution_date_gte=formatted, limit=limit))

        response = await self._make_request_with_retry(_request)
        events: list[PolygonSplitEvent] = []
        for item in response:
            try:
                events.append(
                    PolygonSplitEvent(
                        ticker=str(item.ticker),
                        execution_date=date.fromisoformat(str(item.execution_date)),
                        split_from=float(item.split_from),
                        split_to=float(item.split_to),
                    )
                )
            except (AttributeError, TypeError, ValueError) as e:
                logger.warning(f"Skipping invalid split record: {e}")
        return events

    async def get_grouped_daily_aggs(
        self,
        target_date: date | datetime | str,
        *,
        market_type: str,
        locale: str = "global",
        adjusted: bool = True,
    ) -> list[PolygonGroupedAgg]:
        """Fetch grouped daily aggregates for all tickers.

        Args:
            target_date: Target date for aggregates.
            market_type: Market type (crypto, fx, stocks).
            locale: Market locale.
            adjusted: Whether to adjust for splits.

        Returns:
            List of daily aggregates for all symbols.
        """
        if isinstance(target_date, (date, datetime)):
            formatted_date = target_date.isoformat()
        else:
            formatted_date = str(target_date)
        logger.info(
            f"API call: grouped daily {market_type} (locale={locale}, date={formatted_date})"
        )

        def _request() -> list[Any]:
            response: Any = self._client.get_grouped_daily_aggs(
                formatted_date,
                locale=locale,
                market_type=market_type,
                adjusted=adjusted,
            )
            if hasattr(response, "results") and response.results is not None:
                return list(response.results)
            return list(response)

        grouped = await self._make_request_with_retry(_request)
        logger.info(f"API response: {len(grouped)} symbols")
        validated: list[PolygonGroupedAgg] = []
        for item in grouped:
            try:
                agg = PolygonGroupedAgg.from_sdk_agg(item)
                validated.append(agg)
            except Exception as e:
                logger.warning(f"Skipping invalid grouped aggregate: {e}")
        return validated

    async def get_last_quote(self, symbol: str) -> TickerSnapshot:
        """Get last quote using previous close data.

        Args:
            symbol: Ticker with prefix (C:, X:, or I:).

        Returns:
            Ticker snapshot with estimated bid/ask spread.

        Raises:
            ValueError: If symbol doesn't have required prefix.
        """
        if not symbol.startswith(("C:", "X:", "I:")):
            raise ValueError(
                f"Symbol must start with 'C:' (FX), 'X:' (Crypto), or 'I:' (Indices). Got: {symbol}"
            )
        logger.debug(f"Fetching Polygon.io quote: {symbol}")
        aggs = await self._make_request_with_retry(
            lambda: self._client.get_previous_close_agg(ticker=symbol)
        )
        if not aggs or len(aggs) == 0:
            return TickerSnapshot(
                symbol=symbol,
                bid=0.0,
                ask=0.0,
                last=0.0,
                timestamp=0.0,
            )
        prev_close = PolygonPreviousClose.from_sdk_agg(symbol, aggs[0])
        close = prev_close.close or 0.0
        timestamp_ms = prev_close.timestamp or 0
        spread = close * 0.0001
        bid = close - spread / 2
        ask = close + spread / 2
        return TickerSnapshot(
            symbol=symbol,
            bid=bid,
            ask=ask,
            last=close,
            timestamp=float(timestamp_ms) / 1000.0,
        )

    async def get_ticker(self, symbol: str) -> PolygonTickerSnapshot:
        """Get ticker with extended statistics.

        Args:
            symbol: Ticker symbol.

        Returns:
            Extended ticker snapshot with 24h stats.
        """
        return await self.get_ticker_with_stats(symbol)

    async def get_ticker_with_stats(self, symbol: str) -> PolygonTickerSnapshot:
        """Get ticker with extended 24h statistics.

        Args:
            symbol: Ticker symbol.

        Returns:
            Extended ticker with high, low, volume, vwap, change.
        """
        quote = await self.get_last_quote(symbol)
        logger.debug(f"Fetching Polygon.io ticker: {symbol}")
        aggs = await self._make_request_with_retry(
            lambda: self._client.get_previous_close_agg(ticker=symbol)
        )
        if not aggs or len(aggs) == 0:
            return PolygonTickerSnapshot(
                symbol=symbol,
                bid=quote.bid,
                ask=quote.ask,
                last=quote.last,
                timestamp=quote.timestamp,
            )
        prev_close = PolygonPreviousClose.from_sdk_agg(symbol, aggs[0])
        high = prev_close.high or 0.0
        low = prev_close.low or 0.0
        close = prev_close.close or 0.0
        volume = prev_close.volume or 0.0
        vwap = prev_close.vwap or 0.0
        change = quote.last - close if close else 0.0
        change_pct = (change / close * 100) if close else 0.0
        return PolygonTickerSnapshot(
            symbol=symbol,
            bid=quote.bid,
            ask=quote.ask,
            last=quote.last,
            timestamp=quote.timestamp,
            volume=volume,
            vwap=vwap,
            low=low,
            high=high,
            change=change,
            change_pct=change_pct,
        )

    @staticmethod
    def _resolve_timeframe(timeframe: str) -> tuple[int, str, timedelta]:
        """Convert timeframe string to Polygon API parameters.

        Args:
            timeframe: Candle interval (1m, 5m, 15m, 1h, 4h, 1d).

        Returns:
            Tuple of (multiplier, timespan, interval_delta).

        Raises:
            ValueError: If timeframe is not supported.
        """
        mapping: dict[str, tuple[int, str, timedelta]] = {
            "1m": (1, "minute", timedelta(minutes=1)),
            "5m": (5, "minute", timedelta(minutes=5)),
            "15m": (15, "minute", timedelta(minutes=15)),
            "1h": (1, "hour", timedelta(hours=1)),
            "4h": (4, "hour", timedelta(hours=4)),
            "1d": (1, "day", timedelta(days=1)),
        }
        if timeframe not in mapping:
            raise ValueError(f"Unsupported timeframe for Polygon API: {timeframe}")
        return mapping[timeframe]

    async def get_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1m",
        since: int | None = None,
        limit: int | None = None,
    ) -> list[OhlcvSnapshot]:
        """Fetch OHLCV candles from Polygon.io.

        Args:
            symbol: Ticker symbol.
            timeframe: Candle interval (1m, 5m, 15m, 1h, 4h, 1d).
            since: Start timestamp in milliseconds.
            limit: Maximum candles to return.

        Returns:
            List of OHLCV snapshots.
        """
        multiplier, timespan, interval_delta = self._resolve_timeframe(timeframe)
        now_utc = datetime.now(UTC)
        if since is not None:
            start_dt = datetime.fromtimestamp(since / 1000.0, tz=UTC)
        else:
            window = interval_delta * (limit if limit is not None else 100)
            start_dt = now_utc - window
        aggregates = await self.list_aggregates(
            ticker=symbol,
            multiplier=multiplier,
            timespan=timespan,
            from_date=start_dt,
            to_date=now_utc,
            limit=limit or 5000,
        )
        snapshots = [
            OhlcvSnapshot(
                timestamp=cast(int, agg.timestamp) / 1000.0,
                open=float(agg.open or 0.0),
                high=float(agg.high or 0.0),
                low=float(agg.low or 0.0),
                close=float(agg.close or 0.0),
                volume=float(agg.volume or 0.0),
            )
            for agg in aggregates.aggregates
        ]
        return snapshots

    async def create_order(self, request: ExchangeOrderRequest) -> ExchangeOrderSnapshot:
        """Not implemented - Polygon is data-only provider.

        Args:
            request: Order request parameters.

        Returns:
            Never returns, always raises NotImplementedError.

        Raises:
            NotImplementedError: Polygon.io does not support trading.
        """
        raise NotImplementedError(_MARKET_DATA_ONLY_MSG)

    async def cancel_order(self, order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        """Not implemented - Polygon is data-only provider.

        Args:
            order_id: Order identifier.
            symbol: Trading symbol (optional).

        Returns:
            Never returns, always raises NotImplementedError.

        Raises:
            NotImplementedError: Polygon.io does not support trading.
        """
        raise NotImplementedError(_MARKET_DATA_ONLY_MSG)

    async def get_order(self, order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        """Not implemented - Polygon is data-only provider.

        Args:
            order_id: Order identifier.
            symbol: Trading symbol (optional).

        Returns:
            Never returns, always raises NotImplementedError.

        Raises:
            NotImplementedError: Polygon.io does not support trading.
        """
        raise NotImplementedError(_MARKET_DATA_ONLY_MSG)

    async def get_orders(
        self,
        symbol: str | None = None,
        status: ExchangeOrderStatusEnum | None = None,
        limit: int | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        """Not implemented - Polygon is data-only provider.

        Args:
            symbol: Trading symbol filter (optional).
            status: Order status filter (optional).
            limit: Maximum orders to return (optional).

        Returns:
            Never returns, always raises NotImplementedError.

        Raises:
            NotImplementedError: Polygon.io does not support trading.
        """
        raise NotImplementedError(_MARKET_DATA_ONLY_MSG)

    async def get_balance(self, currency: str | None = None) -> dict[str, AccountBalance]:
        """Not implemented - Polygon is data-only provider.

        Args:
            currency: Currency filter (optional).

        Returns:
            Never returns, always raises NotImplementedError.

        Raises:
            NotImplementedError: Polygon.io does not support trading.
        """
        raise NotImplementedError(_MARKET_DATA_ONLY_MSG)

    def subscribe_ticks(self, symbols: list[str]) -> AsyncIterator[TickerUpdate]:
        """Not implemented - Polygon does not support streaming.

        Args:
            symbols: List of symbols to subscribe.

        Returns:
            Never returns, always raises NotImplementedError.

        Raises:
            NotImplementedError: Polygon.io does not support streaming.
        """
        raise NotImplementedError("PolygonExchangeClient does not support streaming tick data.")

    def subscribe_candles(
        self,
        symbols: list[str],
        timeframe: str = "1m",
    ) -> AsyncIterator[CandleUpdate]:
        """Not implemented - Polygon does not support streaming.

        Args:
            symbols: List of symbols to subscribe.
            timeframe: Candle interval.

        Returns:
            Never returns, always raises NotImplementedError.

        Raises:
            NotImplementedError: Polygon.io does not support streaming.
        """
        raise NotImplementedError("PolygonExchangeClient does not support streaming candles.")

    def subscribe_trades(self, symbols: list[str]) -> AsyncIterator[TradeUpdate]:
        """Not implemented - Polygon does not support streaming.

        Args:
            symbols: List of symbols to subscribe.

        Returns:
            Never returns, always raises NotImplementedError.

        Raises:
            NotImplementedError: Polygon.io does not support streaming.
        """
        raise NotImplementedError("PolygonExchangeClient does not support streaming trades.")

    def subscribe_executions(self) -> AsyncIterator[ExecutionUpdate]:
        """Not implemented - Polygon does not support trading.

        Returns:
            Never returns, always raises NotImplementedError.

        Raises:
            NotImplementedError: Polygon.io does not support execution streaming.
        """
        raise NotImplementedError("PolygonExchangeClient does not support execution streaming.")

    def _is_cache_valid(self) -> bool:
        """Check if the symbols cache file is valid and not stale.

        Returns:
            True if cache exists and is within TTL, False otherwise.
        """
        if not self.symbols_cache_file.exists():
            logger.debug(f"Cache file {self.symbols_cache_file} does not exist")
            return False
        try:
            file_mtime = datetime.fromtimestamp(self.symbols_cache_file.stat().st_mtime, tz=UTC)
            threshold = datetime.now(UTC) - timedelta(hours=self.cache_ttl_hours)
            if file_mtime < threshold:
                logger.info(
                    f"Cache file {self.symbols_cache_file} is stale "
                    f"(modified: {file_mtime}, threshold: {threshold})"
                )
                return False
            file_age = datetime.now(UTC) - file_mtime
            logger.debug(f"Cache file {self.symbols_cache_file} is fresh (age: {file_age})")
            return True
        except Exception as e:
            logger.warning(f"Error checking cache file timestamp: {e}")
            return False

    def _load_symbols_from_cache(self) -> list[dict[str, Any]]:
        """Load symbol metadata from the local cache file.

        Returns:
            List of symbol metadata dictionaries.
        """
        logger.info(f"Loading symbols from cache: {self.symbols_cache_file}")
        symbols: list[dict[str, Any]] = []
        with open(self.symbols_cache_file, encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                symbol: dict[str, Any] = {}
                for key, value in row.items():
                    if value == "":
                        continue
                    if key == "active":
                        symbol[key] = value.lower() == "true"
                    else:
                        symbol[key] = value
                symbols.append(symbol)
        logger.info(f"Loaded {len(symbols)} symbols from cache")
        return symbols

    def _save_symbols_to_cache(self, symbols: list[dict[str, Any]]) -> None:
        """Save symbol metadata to the local cache file.

        Args:
            symbols: List of symbol metadata dictionaries to cache.
        """
        self.symbols_cache_file.parent.mkdir(parents=True, exist_ok=True)
        all_keys: set[str] = set()
        for symbol in symbols:
            all_keys.update(symbol.keys())
        fieldnames = sorted(all_keys)
        with open(self.symbols_cache_file, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, lineterminator="\n")
            writer.writeheader()
            for symbol in symbols:
                writer.writerow(symbol)
        logger.info(f"Saved {len(symbols)} symbols to cache: {self.symbols_cache_file}")

    _TICKER_FIELDS: tuple[str, ...] = (
        "ticker",
        "name",
        "market",
        "locale",
        "primary_exchange",
        "type",
        "active",
        "currency_symbol",
        "currency_name",
        "base_currency_symbol",
        "base_currency_name",
        "cik",
        "composite_figi",
        "share_class_figi",
        "source_feed",
    )

    @staticmethod
    def _extract_ticker_fields(ticker: Any, fields: tuple[str, ...]) -> dict[str, Any]:
        """Extract non-None fields from a Polygon ticker object.

        Args:
            ticker: Polygon SDK ticker object.
            fields: Tuple of attribute names to extract.

        Returns:
            Dictionary with field names and their values.
        """
        result: dict[str, Any] = {}
        for field in fields:
            value = getattr(ticker, field, None)
            if value is not None:
                result[field] = value
        return result

    def subscribe_instruments(self, **kwargs: Any) -> AsyncIterator[dict[str, Any]]:
        """Stream instrument/ticker metadata from Polygon.io.

        Yields cached symbols if valid, otherwise fetches fresh data
        from Polygon API and caches them.

        Args:
            **kwargs: Additional options (unused).

        Yields:
            dict: Symbol metadata dictionaries.
        """
        _ = kwargs
        return self._subscribe_instruments_impl()

    async def _subscribe_instruments_impl(self) -> AsyncIterator[dict[str, Any]]:
        if self._is_cache_valid():
            logger.info("Using cached symbols (fresh)")
            symbols = self._load_symbols_from_cache()
            for symbol in symbols:
                yield symbol
            return
        logger.info("Cache stale or missing - downloading from Polygon API...")
        symbols_list: list[dict[str, Any]] = []
        total_yielded = 0
        page_count = 0

        def _fetch_tickers() -> Any:
            return self._client.list_tickers(
                active=True,
                order="asc",
                sort="ticker",
                limit=1000,
            )

        ticker_iterator = iter(await self._dispatch_blocking(_fetch_tickers))

        def _next_page() -> list[Any]:
            """Materialize one page of the lazy SDK iterator off-loop.

            ``list_tickers`` returns a lazily-paging iterator whose
            ``next()`` performs synchronous network I/O, so consuming it
            on the event loop would both block the loop and bypass the
            bounded REST pool (P1-5). One page per pool call preserves
            the 12s-per-page pacing below exactly as before. The outer
            ``iter()`` normalizes list-returning fakes/SDKs to a single
            stateful iterator — repeated ``islice`` over a plain list
            would re-read the same first page forever.
            """
            return list(itertools.islice(ticker_iterator, 1000))

        while True:
            page = await self._dispatch_blocking(_next_page)
            if not page:
                break
            for ticker in page:
                ticker_dict = self._extract_ticker_fields(ticker, self._TICKER_FIELDS)
                symbols_list.append(ticker_dict)
                yield ticker_dict
                total_yielded += 1
            if len(page) == 1000:
                page_count += 1
                sleep_time = 12
                logger.info(
                    f"Page {page_count} complete: {total_yielded} tickers. "
                    f"Sleeping {sleep_time}s..."
                )
                await asyncio.sleep(sleep_time)
        if symbols_list:
            self._save_symbols_to_cache(symbols_list)
            logger.info(f"Saved {len(symbols_list)} symbols to cache")
        logger.info(f"Finished: yielded {total_yielded} instruments from {page_count} pages")

    async def poll_tickers(
        self,
        symbols: list[str],
        interval_seconds: float = 60.0,
    ) -> None:
        """Continuously poll tickers at specified interval.

        Args:
            symbols: List of symbols to poll.
            interval_seconds: Polling interval in seconds.
        """
        logger.info(f"Starting Polygon.io polling: {symbols} every {interval_seconds}s")
        min_interval = 60.0 / self.rate_limit * len(symbols)
        if interval_seconds < min_interval:
            logger.warning(
                f"Polling interval {interval_seconds}s may exceed rate limit. "
                f"Minimum recommended: {min_interval:.1f}s"
            )
        while True:
            try:
                for symbol in symbols:
                    try:
                        ticker = await self.get_ticker(symbol)
                        logger.info(
                            f"Polygon.io {symbol}: "
                            f"bid={ticker.bid:.5f} ask={ticker.ask:.5f} "
                            f"last={ticker.last:.5f} "
                            f"24h: H={ticker.high:.5f} L={ticker.low:.5f} "
                            f"chg={ticker.change_pct:+.2f}%"
                        )
                    except Exception as e:
                        logger.error(f"Failed to fetch {symbol}: {e}")
                await asyncio.sleep(interval_seconds)
            except KeyboardInterrupt:
                logger.info("Stopping Polygon.io polling")
                break
            except Exception as e:
                logger.error(f"Polling error: {e}")
                try:
                    await asyncio.sleep(interval_seconds)
                except KeyboardInterrupt:
                    logger.info("Stopping Polygon.io polling after error")
                    break
