"""Polygon.io historical data loader with CSV caching.

This module provides the PolygonHistoricalLoader for fetching and caching
historical market data from Polygon.io API. It supports aggregate (OHLCV)
candles and grouped daily data with automatic CSV persistence.
Features
    Rate-limited API access with configurable delays
    Automatic CSV caching organized by archive_symbol, timespan, and date
    Support for resuming interrupted downloads
    Decimal precision for financial calculations
    Empty marker files for dates with no data
Data classes
    AggregateCandle: Single OHLCV candle with timestamp and metadata.
    GroupedDailyRow: Daily aggregated data for market-wide snapshots.

Example:
    >>> from snapper.infrastructure.historical.polygon.loader import (
    PolygonHistoricalLoader
    >>> loader = PolygonHistoricalLoader(client, Path("./cache"))
    >>> candles = await loader.fetch_aggregates(
    "AAPL", 1, "day"
    archive_symbol="AAPL"
    from_ts=datetime(2024, 1, 1)
    to_ts=datetime(2024, 1, 31)
"""

import asyncio
import csv
from collections import defaultdict
from collections.abc import Iterable
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC
from datetime import date
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from loguru import logger

from snapper.infrastructure.exchanges.implementations.polygon import PolygonExchangeClient

__all__ = ["AggregateCandle", "GroupedDailyRow", "PolygonHistoricalLoader"]


def _format_decimal(value: Decimal | None) -> str:
    """Format a Decimal value for CSV output.

    Preserves full precision from API. Converts to fixed-point
    notation to avoid scientific notation in CSV.

    Args:
        value: Decimal value to format, or None.

    Returns:
        String representation suitable for CSV.
    """
    if value is None:
        return "0"
    s = format(value, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s


_HEADER = [
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "vwap",
    "transactions",
]


@dataclass(slots=True)
class AggregateCandle:
    """OHLCV candle data for a single time period.

    Represents aggregated price and volume data from Polygon.io
    for a specific ticker and time period.

    Attributes:
        ticker: Security ticker symbol.
        timestamp: UTC timestamp for the candle period.
        open: Opening price.
        high: Highest price during period.
        low: Lowest price during period.
        close: Closing price.
        volume: Trading volume.
        vwap: Volume-weighted average price, if available.
        transactions: Number of trades, if available.
    """

    ticker: str
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    vwap: Decimal | None
    transactions: int | None


@dataclass(slots=True)
class GroupedDailyRow:
    """Daily aggregated data for a single ticker.

    Represents grouped daily aggregate data from Polygon.io,
    typically used for market-wide snapshots.

    Attributes:
        ticker: Security ticker symbol.
        open: Opening price.
        high: Highest price during day.
        low: Lowest price during day.
        close: Closing price.
        volume: Trading volume.
        vwap: Volume-weighted average price, if available.
        total_trades: Total number of trades, if available.
        closing_timestamp: UTC timestamp of market close.
    """

    ticker: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    vwap: Decimal | None
    total_trades: int | None
    closing_timestamp: datetime


class PolygonHistoricalLoader:
    """Service for fetching and caching Polygon.io historical data.

    Fetches aggregate candle data and grouped daily data from Polygon.io
    API with automatic CSV caching. Handles rate limiting and supports
    resuming interrupted downloads.

    Cache structure:
        - Aggregates: ``{cache_root}/{timespan}/{archive_symbol}/{year}/{date}.csv``
        - Grouped: ``{cache_root}/grouped/{market_type}/{year}/{date}.csv``

    Attributes:
        _client: Polygon exchange client for API access.
        _cache_root: Root directory for CSV cache files.
        _rate_delay: Delay between API calls (seconds).
    """

    def __init__(
        self,
        client: PolygonExchangeClient,
        cache_root: str | Path,
        rate_delay_seconds: float = 12.0,
    ) -> None:
        """Initialize the historical data loader.

        Args:
            client: Polygon exchange client instance.
            cache_root: Root directory for CSV cache.
            rate_delay_seconds: Delay between API calls for rate limiting.
        """
        self._client = client
        self._cache_root = Path(cache_root)
        self._rate_delay = rate_delay_seconds

    def _parse_aggregate_response(
        self,
        response: list[Any],
        ticker: str,
        resume_from: datetime | None,
    ) -> list[AggregateCandle]:
        """Parse raw aggregate response items into AggregateCandle list.

        Args:
            response: Raw aggregate items from Polygon API.
            ticker: Ticker symbol for the candles.
            resume_from: Skip candles at or before this timestamp.

        Returns:
            Sorted list of AggregateCandle objects.
        """
        candles: list[AggregateCandle] = []
        for item in response:
            if item.timestamp is None:
                continue
            ts = datetime.fromtimestamp(item.timestamp / 1000, tz=UTC)
            if resume_from is not None and ts <= resume_from:
                continue
            candles.append(
                AggregateCandle(
                    ticker=ticker,
                    timestamp=ts,
                    open=Decimal(str(item.open or 0.0)),
                    high=Decimal(str(item.high or 0.0)),
                    low=Decimal(str(item.low or 0.0)),
                    close=Decimal(str(item.close or 0.0)),
                    volume=Decimal(str(item.volume or 0.0)),
                    vwap=(Decimal(str(item.vwap)) if item.vwap is not None else None),
                    transactions=item.transactions,
                )
            )
        candles.sort(key=lambda candle: candle.timestamp)
        return candles

    def _save_candles_to_csv(
        self,
        candles: list[AggregateCandle],
        archive_symbol: str,
        timespan: str,
        from_ts: datetime,
        to_ts: datetime,
    ) -> None:
        """Save aggregate candles to per-day CSV files.

        Creates CSV files for each day in the date range. Days without
        data get an empty marker file.

        Args:
            candles: List of candles to save.
            archive_symbol: Stable archive symbol for path resolution.
            timespan: Timespan for path resolution.
            from_ts: Start of the full date range.
            to_ts: End of the full date range.
        """
        candles_by_day: dict[date, list[AggregateCandle]] = defaultdict(list)
        for candle in candles:
            day = candle.timestamp.date()
            candles_by_day[day].append(candle)
        all_days: set[date] = set()
        current_day = from_ts.date()
        end_day = to_ts.date()
        while current_day <= end_day:
            all_days.add(current_day)
            current_day += timedelta(days=1)
        for day in sorted(all_days):
            csv_path = self.get_aggregate_csv_path_for_day(archive_symbol, timespan, day)
            if day in candles_by_day:
                day_candles = candles_by_day[day]
                day_candles.sort(key=lambda c: c.timestamp)
                self._write_csv(csv_path, day_candles)
            elif not csv_path.exists():
                self._write_csv(csv_path, [])
                logger.debug(f"Created empty marker file for {day}")

    async def fetch_aggregates(
        self,
        ticker: str,
        multiplier: int,
        timespan: str,
        *,
        from_ts: datetime,
        to_ts: datetime,
        archive_symbol: str | None = None,
        adjusted: bool = True,
        sort: str = "asc",
        limit: int = 50000,
        resume_from: datetime | None = None,
        save_csv: bool = True,
    ) -> list[AggregateCandle]:
        """Fetch aggregate candles from Polygon.io API.

        Retrieves OHLCV data for a ticker over a date range and optionally
        saves to CSV cache. Supports resuming from a specific timestamp.

        Args:
            ticker: Security ticker symbol (e.g., ``AAPL``, ``C:BTCUSD``).
            multiplier: Timespan multiplier.
            timespan: Timespan unit.
            from_ts: Start of date range (UTC).
            to_ts: End of date range (UTC).
            archive_symbol: Stable archive symbol for CSV directory naming.
                Required when ``save_csv=True``.
            adjusted: Whether to adjust for splits/dividends.
            sort: Sort order (``asc`` or ``desc``).
            limit: Maximum candles per API call.
            resume_from: Skip candles before this timestamp.
            save_csv: Whether to save results to CSV cache.

        Returns:
            List of AggregateCandle objects, sorted by timestamp.
        """
        boundary_from = resume_from if resume_from is not None else from_ts
        logger.info(
            "Fetching Polygon aggregates",
            ticker=ticker,
            timespan=timespan,
            from_ts=boundary_from.isoformat(),
            to_ts=to_ts.isoformat(),
        )
        response = await self._client.list_aggregates(
            ticker,
            multiplier,
            timespan,
            boundary_from,
            to_ts,
            adjusted=adjusted,
            sort=sort,
            limit=limit,
        )
        candles = self._parse_aggregate_response(response, ticker, resume_from)
        logger.info(f"Received {len(candles)} candles")
        if save_csv:
            if archive_symbol is None:
                raise ValueError("archive_symbol is required when save_csv=True")
            await asyncio.to_thread(
                self._save_candles_to_csv, candles, archive_symbol, timespan, from_ts, to_ts
            )
        await asyncio.sleep(self._rate_delay)
        return candles

    async def fetch_grouped_daily(
        self,
        target_date: date | datetime,
        *,
        market_type: str,
        locale: str = "global",
        adjusted: bool = True,
        save_csv: bool = True,
    ) -> list[GroupedDailyRow]:
        """Fetch grouped daily aggregates for all tickers.

        Retrieves market-wide daily OHLCV data for a specific date,
        covering all tickers in the specified market type.

        Args:
            target_date: Date to fetch data for.
            market_type: Market type (``stocks``, ``crypto``, ``fx``).
            locale: Market locale (default: ``global``).
            adjusted: Whether to adjust for splits/dividends.
            save_csv: Whether to save results to CSV cache.

        Returns:
            List of GroupedDailyRow objects, sorted by ticker.
        """
        logger.info(
            "Fetching grouped daily aggregates",
            target_date=target_date.isoformat(),
            market_type=market_type,
            locale=locale,
        )
        raw_rows = await self._client.get_grouped_daily_aggs(
            target_date,
            market_type=market_type,
            locale=locale,
            adjusted=adjusted,
        )
        rows: list[GroupedDailyRow] = []
        for item in raw_rows:
            closing_ts = datetime.fromtimestamp(getattr(item, "timestamp", 0) / 1000, tz=UTC)
            vwap_val = getattr(item, "vwap", None)
            rows.append(
                GroupedDailyRow(
                    ticker=str(getattr(item, "ticker", "")),
                    open=Decimal(str(getattr(item, "open", None) or 0)),
                    high=Decimal(str(getattr(item, "high", None) or 0)),
                    low=Decimal(str(getattr(item, "low", None) or 0)),
                    close=Decimal(str(getattr(item, "close", None) or 0)),
                    volume=Decimal(str(getattr(item, "volume", None) or 0)),
                    vwap=Decimal(str(vwap_val)) if vwap_val is not None else None,
                    total_trades=getattr(item, "transactions", None),
                    closing_timestamp=closing_ts,
                )
            )
        rows.sort(key=lambda row: row.ticker)
        logger.info(f"Received {len(rows)} grouped rows")
        if save_csv:
            csv_path = self._resolve_grouped_csv_path(target_date, market_type, locale)
            if rows:
                await asyncio.to_thread(self._write_grouped_csv, csv_path, rows)
            else:
                await asyncio.to_thread(csv_path.parent.mkdir, parents=True, exist_ok=True)

                def _write_empty_marker() -> None:
                    """Write CSV header as empty marker file."""
                    with csv_path.open("w", encoding="utf-8", newline="") as f:
                        f.write(
                            "ticker,open,high,low,close,volume,vwap,"
                            "total_trades,closing_timestamp\n"
                        )

                await asyncio.to_thread(_write_empty_marker)
                logger.debug(f"Created empty marker: {csv_path}")
        if rows:
            await asyncio.sleep(self._rate_delay)
        return rows

    def get_aggregate_csv_path(self, archive_symbol: str, timespan: str) -> Path:
        """Get base directory for an archive symbol's aggregate CSV files.

        Pure path resolution without side effects. Does not create
        directories (use ``_write_csv`` for that).

        Args:
            archive_symbol: Stable archive symbol (e.g. ``BTC-USD``).
            timespan: Timespan unit.

        Returns:
            Path to the archive symbol's aggregate directory.
        """
        return self._cache_root / timespan / archive_symbol

    def get_aggregate_csv_path_for_day(self, archive_symbol: str, timespan: str, day: date) -> Path:
        """Get CSV file path for a specific day's aggregate data.

        Pure path resolution without side effects. For daily timespan,
        groups by month. For other timespans, creates a file per day.

        Args:
            archive_symbol: Stable archive symbol.
            timespan: Timespan unit.
            day: Date for the data.

        Returns:
            Path to the CSV file (may not exist yet).
        """
        base_directory = self.get_aggregate_csv_path(archive_symbol, timespan)
        year_directory = base_directory / str(day.year)
        if timespan.lower() == "day":
            return year_directory / f"{day.year}-{day.month:02d}.csv"
        return year_directory / f"{day.isoformat()}.csv"

    def get_grouped_csv_path(self, day: date | datetime, market_type: str, locale: str) -> Path:
        """Get CSV file path for grouped daily data.

        Args:
            day: Date for the data.
            market_type: Market type (``stocks``, ``crypto``, ``fx``).
            locale: Market locale.

        Returns:
            Path to the grouped daily CSV file.
        """
        return self._resolve_grouped_csv_path(day, market_type, locale)

    def _resolve_grouped_csv_path(
        self,
        day: date | datetime,
        market_type: str,
        locale: str,
    ) -> Path:
        """Resolve CSV path for grouped daily data.

        Args:
            day: Date for the data.
            market_type: Market type.
            locale: Market locale (unused in path).

        Returns:
            Path to the CSV file.
        """
        day_value = day.date() if isinstance(day, datetime) else day
        base_directory = self._cache_root / "grouped" / market_type
        year_directory = base_directory / str(day_value.year)
        year_directory.mkdir(parents=True, exist_ok=True)
        return year_directory / f"{day_value.isoformat()}.csv"

    def _write_csv(self, path: Path, candles: Sequence[AggregateCandle]) -> None:
        """Write aggregate candles to CSV file.

        Args:
            path: Target file path.
            candles: Candles to write (may be empty for marker files).
        """
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
            writer.writerow(_HEADER)
            for candle in candles:
                writer.writerow(
                    [
                        candle.timestamp.isoformat(),
                        _format_decimal(candle.open),
                        _format_decimal(candle.high),
                        _format_decimal(candle.low),
                        _format_decimal(candle.close),
                        _format_decimal(candle.volume),
                        _format_decimal(candle.vwap),
                        str(candle.transactions or 0),
                    ]
                )

    def _write_grouped_csv(self, path: Path, rows: Iterable[GroupedDailyRow]) -> None:
        """Write grouped daily rows to CSV file.

        Args:
            path: Target file path.
            rows: Grouped daily rows to write.
        """
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
            writer.writerow(
                [
                    "ticker",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "vwap",
                    "total_trades",
                    "closing_timestamp",
                ]
            )
            for row in rows:
                writer.writerow(
                    [
                        row.ticker,
                        _format_decimal(row.open),
                        _format_decimal(row.high),
                        _format_decimal(row.low),
                        _format_decimal(row.close),
                        _format_decimal(row.volume),
                        _format_decimal(row.vwap),
                        str(row.total_trades or 0),
                        row.closing_timestamp.isoformat(),
                    ]
                )
