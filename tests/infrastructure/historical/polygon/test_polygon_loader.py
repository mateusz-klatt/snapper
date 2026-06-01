"""Tests for Polygon historical data loader."""

import csv
from datetime import UTC
from datetime import date
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from typing import cast

import pytest

from snapper.infrastructure.exchanges.implementations.polygon import PolygonExchangeClient
from snapper.infrastructure.historical.polygon.loader import AggregateCandle
from snapper.infrastructure.historical.polygon.loader import PolygonHistoricalLoader
from snapper.infrastructure.historical.polygon.loader import _format_decimal
from snapper.infrastructure.historical.polygon.loader import read_aggregate_csv


def test_format_decimal_preserves_very_small_values() -> None:
    """Verify _format_decimal preserves precision for values smaller than 8dp.

    Given: A Decimal value smaller than 0.00000001,
    When: Formatting for CSV output,
    Then: Full precision is preserved without loss.
    """
    tiny = Decimal("0.00000000123")
    result = _format_decimal(tiny)
    assert result == "0.00000000123"


class _StubPolygonClient:
    """Test stub for Polygon API client."""

    def __init__(
        self,
        aggregates: list[Any] | None = None,
        grouped: list[Any] | None = None,
    ) -> None:
        self._aggregates = aggregates or []
        self._grouped = grouped or []
        self.aggregate_calls: list[dict[str, Any]] = []
        self.grouped_calls: list[dict[str, Any]] = []

    async def list_aggregates(
        self,
        ticker: str,
        multiplier: int,
        timespan: str,
        boundary_from: datetime,
        to_ts: datetime,
        *,
        adjusted: bool,
        sort: str,
        limit: int,
    ) -> list[Any]:
        self.aggregate_calls.append(
            {
                "ticker": ticker,
                "multiplier": multiplier,
                "timespan": timespan,
                "from_ts": boundary_from,
                "to_ts": to_ts,
                "adjusted": adjusted,
                "sort": sort,
                "limit": limit,
            }
        )
        return self._aggregates

    async def get_grouped_daily_aggs(
        self,
        target_date: date | datetime,
        *,
        market_type: str,
        locale: str,
        adjusted: bool,
    ) -> list[Any]:
        self.grouped_calls.append(
            {
                "target_date": target_date,
                "market_type": market_type,
                "locale": locale,
                "adjusted": adjusted,
            }
        )
        return self._grouped


@pytest.mark.asyncio
async def test_fetch_aggregates_writes_csv_with_archive_symbol(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fetch aggregates writes CSV under archive_symbol directory.

    Given: Stub client returning aggregates with mixed timestamps,
    When: fetch_aggregates called with archive_symbol and resume_from filter,
    Then: Only newer candles are returned and CSV files are created
          under the archive_symbol directory path.
    """
    resume_from = datetime(2024, 1, 1, 12, tzinfo=UTC)
    included_late = datetime(2024, 1, 1, 16, tzinfo=UTC)
    included_early = datetime(2024, 1, 1, 14, tzinfo=UTC)
    aggregates = [
        SimpleNamespace(
            timestamp=int(included_late.timestamp() * 1000),
            open=1.4,
            high=1.5,
            low=1.3,
            close=1.45,
            volume=320,
            vwap=None,
            transactions=15,
        ),
        SimpleNamespace(
            timestamp=int(datetime(2024, 1, 1, 9, tzinfo=UTC).timestamp() * 1000),
            open=1.0,
            high=1.1,
            low=0.9,
            close=1.05,
            volume=150,
            vwap=1.02,
            transactions=10,
        ),
        SimpleNamespace(
            timestamp=int(included_early.timestamp() * 1000),
            open=1.2,
            high=1.25,
            low=1.15,
            close=1.22,
            volume=200,
            vwap=1.21,
            transactions=None,
        ),
    ]
    stub_client = _StubPolygonClient(aggregates=aggregates)
    loader = PolygonHistoricalLoader(
        cast(PolygonExchangeClient, stub_client),
        cache_root=tmp_path,
        rate_delay_seconds=0.0,
    )
    sleep_calls: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleep_calls.append(delay)

    monkeypatch.setattr(
        "snapper.infrastructure.historical.polygon.loader.asyncio.sleep", fake_sleep
    )
    candles = await loader.fetch_aggregates(
        ticker="X:BTCUSD",
        multiplier=1,
        timespan="minute",
        from_ts=datetime(2024, 1, 1, 0, tzinfo=UTC),
        to_ts=datetime(2024, 1, 2, 0, tzinfo=UTC),
        archive_symbol="BTC-USD",
        resume_from=resume_from,
        save_csv=True,
    )
    assert [c.timestamp for c in candles] == [included_early, included_late]
    assert stub_client.aggregate_calls[0]["from_ts"] == resume_from
    assert sleep_calls == [0.0]
    data_csv = tmp_path / "minute" / "BTC-USD" / "2024" / "2024-01-01.csv"
    assert data_csv.exists()
    with data_csv.open(encoding="utf-8") as csv_file:
        rows = list(csv.reader(csv_file))
    assert rows[0] == [
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "vwap",
        "transactions",
    ]
    assert rows[1][0] == included_early.isoformat()
    assert rows[2][0] == included_late.isoformat()
    empty_marker = tmp_path / "minute" / "BTC-USD" / "2024" / "2024-01-02.csv"
    assert empty_marker.exists()
    with empty_marker.open(encoding="utf-8") as marker_file:
        marker_rows = list(csv.reader(marker_file))
    assert marker_rows == [
        ["timestamp", "open", "high", "low", "close", "volume", "vwap", "transactions"]
    ]


@pytest.mark.asyncio
async def test_fetch_aggregates_raises_when_save_csv_without_archive_symbol(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fetch aggregates raises ValueError when save_csv=True but no archive_symbol.

    Given: Stub client returning one aggregate,
    When: fetch_aggregates called with save_csv=True and no archive_symbol,
    Then: ValueError is raised.
    """
    aggregates = [
        SimpleNamespace(
            timestamp=int(datetime(2024, 1, 1, 14, tzinfo=UTC).timestamp() * 1000),
            open=1.2,
            high=1.25,
            low=1.15,
            close=1.22,
            volume=200,
            vwap=1.21,
            transactions=5,
        ),
    ]
    stub_client = _StubPolygonClient(aggregates=aggregates)
    loader = PolygonHistoricalLoader(
        cast(PolygonExchangeClient, stub_client),
        cache_root=tmp_path,
        rate_delay_seconds=0.0,
    )

    async def fake_sleep(delay: float) -> None:
        """Stub for asyncio.sleep."""

    monkeypatch.setattr(
        "snapper.infrastructure.historical.polygon.loader.asyncio.sleep", fake_sleep
    )
    with pytest.raises(ValueError, match="archive_symbol is required"):
        await loader.fetch_aggregates(
            ticker="X:BTCUSD",
            multiplier=1,
            timespan="minute",
            from_ts=datetime(2024, 1, 1, 0, tzinfo=UTC),
            to_ts=datetime(2024, 1, 1, 0, tzinfo=UTC),
            save_csv=True,
        )


@pytest.mark.asyncio
async def test_fetch_grouped_daily_creates_csv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fetch grouped daily aggregates and write to CSV.

    Given: Stub client returning grouped daily data,
    When: fetch_grouped_daily called with save_csv=True,
    Then: Returns sorted rows and creates CSV file.
    """
    grouped_rows = [
        SimpleNamespace(
            ticker="Z-PAIR",
            open=2.0,
            high=2.5,
            low=1.9,
            close=2.4,
            volume=500,
            vwap=2.2,
            transactions=22,
            timestamp=int(datetime(2024, 2, 1, 23, tzinfo=UTC).timestamp() * 1000),
        ),
        SimpleNamespace(
            ticker="A-PAIR",
            open=1.0,
            high=1.2,
            low=0.95,
            close=1.15,
            volume=300,
            vwap=None,
            transactions=None,
            timestamp=int(datetime(2024, 2, 1, 21, tzinfo=UTC).timestamp() * 1000),
        ),
    ]
    stub_client = _StubPolygonClient(grouped=grouped_rows)
    loader = PolygonHistoricalLoader(
        cast(PolygonExchangeClient, stub_client),
        cache_root=tmp_path,
        rate_delay_seconds=0.0,
    )
    sleep_calls: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleep_calls.append(delay)

    monkeypatch.setattr(
        "snapper.infrastructure.historical.polygon.loader.asyncio.sleep", fake_sleep
    )
    rows = await loader.fetch_grouped_daily(
        date(2024, 2, 1),
        market_type="crypto",
        locale="global",
        adjusted=True,
        save_csv=True,
    )
    assert [row.ticker for row in rows] == ["A-PAIR", "Z-PAIR"]
    assert sleep_calls == [0.0]
    assert stub_client.grouped_calls[0]["market_type"] == "crypto"
    csv_path = tmp_path / "grouped" / "crypto" / "2024" / "2024-02-01.csv"
    assert csv_path.exists()
    with csv_path.open(encoding="utf-8") as csv_file:
        rows_data = list(csv.reader(csv_file))
    assert rows_data[0] == [
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
    assert rows_data[1][0] == "A-PAIR"
    assert rows_data[2][0] == "Z-PAIR"


@pytest.mark.asyncio()
async def test_fetch_aggregates_without_resume_or_csv(tmp_path: Path) -> None:
    """Fetch aggregates without filtering or CSV output.

    Given: Stub client returning empty aggregates,
    When: fetch_aggregates called without resume_from or save_csv,
    Then: Returns empty list and no files created.
    """
    aggregates: list[Any] = []
    stub_client = _StubPolygonClient(aggregates=aggregates)
    loader = PolygonHistoricalLoader(
        cast(PolygonExchangeClient, stub_client),
        cache_root=tmp_path,
        rate_delay_seconds=0.0,
    )
    candles = await loader.fetch_aggregates(
        ticker="X:ETHUSD",
        multiplier=1,
        timespan="minute",
        from_ts=datetime(2024, 1, 1, 0, tzinfo=UTC),
        to_ts=datetime(2024, 1, 1, 1, tzinfo=UTC),
        resume_from=None,
        save_csv=False,
    )
    assert candles == []
    assert stub_client.aggregate_calls[0]["from_ts"] == datetime(2024, 1, 1, 0, tzinfo=UTC)
    assert not any(tmp_path.iterdir())


@pytest.mark.asyncio()
async def test_fetch_aggregates_skips_none_timestamp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Skip aggregates with None timestamp.

    Given: Stub client returning aggregate with None timestamp,
    When: fetch_aggregates is called,
    Then: Returns empty list.
    """

    class _Item:
        timestamp = None
        open = 1.0
        high = 1.1
        low = 0.9
        close = 1.0
        volume = 10
        vwap = None
        transactions = 1

    stub_client = _StubPolygonClient(aggregates=[_Item()])
    loader = PolygonHistoricalLoader(
        cast(PolygonExchangeClient, stub_client),
        cache_root=tmp_path,
        rate_delay_seconds=0.0,
    )
    sleep_calls: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleep_calls.append(delay)

    monkeypatch.setattr(
        "snapper.infrastructure.historical.polygon.loader.asyncio.sleep", fake_sleep
    )
    candles = await loader.fetch_aggregates(
        ticker="X:BTCUSD",
        multiplier=1,
        timespan="minute",
        from_ts=datetime(2024, 1, 1, 0, tzinfo=UTC),
        to_ts=datetime(2024, 1, 1, 1, tzinfo=UTC),
        resume_from=None,
        save_csv=False,
    )
    assert candles == []
    assert sleep_calls == [0.0]


@pytest.mark.asyncio
async def test_fetch_grouped_daily_creates_empty_marker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Create empty marker CSV for day with no data.

    Given: Stub client returning empty grouped data,
    When: fetch_grouped_daily called with save_csv=True,
    Then: Creates CSV with only headers.
    """
    stub_client = _StubPolygonClient(grouped=[])
    loader = PolygonHistoricalLoader(
        cast(PolygonExchangeClient, stub_client),
        cache_root=tmp_path,
        rate_delay_seconds=0.0,
    )
    sleep_calls: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleep_calls.append(delay)

    monkeypatch.setattr(
        "snapper.infrastructure.historical.polygon.loader.asyncio.sleep", fake_sleep
    )
    rows = await loader.fetch_grouped_daily(
        date(2024, 2, 2),
        market_type="crypto",
        locale="global",
        adjusted=False,
        save_csv=True,
    )
    assert rows == []
    assert sleep_calls == []
    csv_path = tmp_path / "grouped" / "crypto" / "2024" / "2024-02-02.csv"
    assert csv_path.exists()
    with csv_path.open(encoding="utf-8") as csv_file:
        rows_data = list(csv.reader(csv_file))
    assert rows_data == [
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
    ]


@pytest.mark.asyncio()
async def test_fetch_aggregates_skips_existing_empty_marker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Preserve existing CSV files under archive_symbol path.

    Given: Pre-existing CSV file in archive_symbol cache directory,
    When: fetch_aggregates is called with that archive_symbol,
    Then: Existing file content is preserved.
    """
    stub_client = _StubPolygonClient(aggregates=[])
    loader = PolygonHistoricalLoader(
        cast(PolygonExchangeClient, stub_client),
        cache_root=tmp_path,
        rate_delay_seconds=0.0,
    )
    existing_marker = tmp_path / "minute" / "BTC-USD" / "2024" / "2024-01-01.csv"
    existing_marker.parent.mkdir(parents=True, exist_ok=True)
    existing_marker.write_text("already here\n", encoding="utf-8")
    sleep_calls: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleep_calls.append(delay)

    monkeypatch.setattr(
        "snapper.infrastructure.historical.polygon.loader.asyncio.sleep", fake_sleep
    )
    candles = await loader.fetch_aggregates(
        ticker="X:BTCUSD",
        multiplier=1,
        timespan="minute",
        from_ts=datetime(2024, 1, 1, 0, tzinfo=UTC),
        to_ts=datetime(2024, 1, 1, 0, tzinfo=UTC),
        archive_symbol="BTC-USD",
        resume_from=None,
        save_csv=True,
    )
    assert candles == []
    assert existing_marker.read_text(encoding="utf-8") == "already here\n"
    assert sleep_calls == [0.0]


@pytest.mark.asyncio()
async def test_fetch_grouped_daily_skips_csv_when_disabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Skip CSV creation when save_csv=False.

    Given: Stub client returning grouped data,
    When: fetch_grouped_daily called with save_csv=False,
    Then: Returns data but no files created.
    """
    grouped_rows = [
        SimpleNamespace(
            ticker="Z-PAIR",
            open=2.0,
            high=2.5,
            low=1.9,
            close=2.4,
            volume=500,
            vwap=2.2,
            transactions=22,
            timestamp=int(datetime(2024, 2, 1, 23, tzinfo=UTC).timestamp() * 1000),
        ),
        SimpleNamespace(
            ticker="A-PAIR",
            open=1.0,
            high=1.2,
            low=0.95,
            close=1.15,
            volume=300,
            vwap=None,
            transactions=None,
            timestamp=int(datetime(2024, 2, 1, 21, tzinfo=UTC).timestamp() * 1000),
        ),
    ]
    stub_client = _StubPolygonClient(grouped=grouped_rows)
    loader = PolygonHistoricalLoader(
        cast(PolygonExchangeClient, stub_client),
        cache_root=tmp_path,
        rate_delay_seconds=0.0,
    )
    sleep_calls: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleep_calls.append(delay)

    monkeypatch.setattr(
        "snapper.infrastructure.historical.polygon.loader.asyncio.sleep", fake_sleep
    )
    rows = await loader.fetch_grouped_daily(
        date(2024, 2, 1),
        market_type="crypto",
        locale="global",
        adjusted=True,
        save_csv=False,
    )
    assert [row.ticker for row in rows] == ["A-PAIR", "Z-PAIR"]
    assert sleep_calls == [0.0]
    assert not (tmp_path / "grouped").exists()


def test_get_aggregate_csv_path_for_day_variants(tmp_path: Path) -> None:
    """Generate correct CSV paths for different timespans.

    Given: Loader with cache root,
    When: get_aggregate_csv_path_for_day called with minute/day,
    Then: Returns appropriate filename pattern under archive_symbol directory.
    """
    loader = PolygonHistoricalLoader(
        cast(PolygonExchangeClient, _StubPolygonClient()),
        cache_root=tmp_path,
        rate_delay_seconds=0.0,
    )
    minute_path = loader.get_aggregate_csv_path_for_day("BTC-USD", "minute", date(2024, 1, 1))
    day_path = loader.get_aggregate_csv_path_for_day("BTC-USD", "day", date(2024, 1, 1))
    assert minute_path.name == "2024-01-01.csv"
    assert day_path.name == "2024-01.csv"
    assert "BTC-USD" in str(minute_path)
    assert "BTC-USD" in str(day_path)
    assert not minute_path.parent.exists()
    assert not day_path.parent.exists()


def test_get_grouped_csv_path_accepts_datetime(tmp_path: Path) -> None:
    """Generate grouped CSV path from datetime object.

    Given: Loader with cache root and datetime input,
    When: get_grouped_csv_path is called,
    Then: Returns path with date extracted from datetime.
    """
    loader = PolygonHistoricalLoader(
        cast(PolygonExchangeClient, _StubPolygonClient()),
        cache_root=tmp_path,
        rate_delay_seconds=0.0,
    )
    dt = datetime(2024, 3, 5, 12, tzinfo=UTC)
    path = loader.get_grouped_csv_path(dt, market_type="crypto", locale="global")
    assert path.name == "2024-03-05.csv"
    assert path.parent.parent.name == "crypto"


def _cache_loader(tmp_path: Path) -> PolygonHistoricalLoader:
    """Build a cache-only loader (no client) rooted at a temp directory.

    Args:
        tmp_path: Pytest temporary directory.

    Returns:
        PolygonHistoricalLoader with client=None.
    """
    return PolygonHistoricalLoader(None, cache_root=tmp_path, rate_delay_seconds=0.0)


def _make_candle(
    timestamp: datetime, *, vwap: Decimal | None, transactions: int | None
) -> AggregateCandle:
    """Build an AggregateCandle for round-trip tests.

    Args:
        timestamp: Candle timestamp.
        vwap: Volume-weighted average price or None.
        transactions: Trade count or None.

    Returns:
        AggregateCandle instance.
    """
    return AggregateCandle(
        ticker="X:BTCUSD",
        timestamp=timestamp,
        open=Decimal("1.20"),
        high=Decimal("1.25"),
        low=Decimal("1.15"),
        close=Decimal("1.22"),
        volume=Decimal("200"),
        vwap=vwap,
        transactions=transactions,
    )


def test_read_aggregate_csv_round_trip(tmp_path: Path) -> None:
    """Read back candles written by the loader's CSV writer.

    Given: A CSV file written by _write_csv with ISO-8601 timestamps,
    When: read_aggregate_csv parses it,
    Then: The candles round-trip with stamped ticker, Decimal prices,
          parsed vwap, and integer transactions. The writer encodes a
          None vwap/transactions as ``0`` on disk, so they read back as
          zero rather than None (the genuine-None path is the blank-cell
          test).
    """
    loader = _cache_loader(tmp_path)
    early = datetime(2024, 1, 1, 14, tzinfo=UTC)
    late = datetime(2024, 1, 1, 16, tzinfo=UTC)
    candles = [
        _make_candle(early, vwap=Decimal("1.21"), transactions=7),
        _make_candle(late, vwap=None, transactions=None),
    ]
    csv_path = tmp_path / "minute" / "BTC-USD" / "2024" / "2024-01-01.csv"
    loader._write_csv(csv_path, candles)
    parsed = read_aggregate_csv(csv_path, "ARCH")
    assert [c.timestamp for c in parsed] == [early, late]
    assert parsed[0].ticker == "ARCH"
    assert parsed[0].open == Decimal("1.2")
    assert parsed[0].vwap == Decimal("1.21")
    assert parsed[0].transactions == 7
    assert parsed[1].vwap == Decimal("0")
    assert parsed[1].transactions == 0


def test_save_candles_day_timespan_round_trip_preserves_all_days(tmp_path: Path) -> None:
    """Preserve every day when writing daily candles spanning a month.

    Given: Daily candles on three distinct days within one month written
        with the ``day`` timespan (which shares one monthly file),
    When: _save_candles_to_csv writes them and read_aggregate_csv reads the
        single resolved monthly file back,
    Then: All three days survive in timestamp order rather than only the
          last day's candle. This guards against the per-day overwrite bug
          where each day clobbered the shared monthly file.
    """
    loader = _cache_loader(tmp_path)
    days = [
        datetime(2024, 3, 1, tzinfo=UTC),
        datetime(2024, 3, 15, tzinfo=UTC),
        datetime(2024, 3, 31, tzinfo=UTC),
    ]
    candles = [_make_candle(ts, vwap=Decimal("1.21"), transactions=7) for ts in days]
    loader._save_candles_to_csv(
        list(reversed(candles)),
        "BTC-USD",
        "day",
        from_ts=datetime(2024, 3, 1, tzinfo=UTC),
        to_ts=datetime(2024, 3, 31, tzinfo=UTC),
    )
    monthly_path = tmp_path / "day" / "BTC-USD" / "2024" / "2024-03.csv"
    assert monthly_path.exists()
    parsed = read_aggregate_csv(monthly_path, "BTC-USD")
    assert [c.timestamp for c in parsed] == days


def test_save_candles_day_timespan_incremental_write_accumulates(tmp_path: Path) -> None:
    """Merge incremental daily fetches instead of truncating the month.

    Given: A monthly daily file already holding day 31 of a month,
    When: a later incremental fetch writes days 2-30 of the same month,
    Then: reading the shared monthly file back yields every day (2-30 plus
          31) in timestamp order. This guards against the truncate-on-write
          bug where the second partial fetch wiped the previously cached
          day.
    """
    loader = _cache_loader(tmp_path)
    day_31 = _make_candle(datetime(2024, 7, 31, tzinfo=UTC), vwap=None, transactions=None)
    loader._save_candles_to_csv(
        [day_31],
        "BTC-USD",
        "day",
        from_ts=datetime(2024, 7, 31, tzinfo=UTC),
        to_ts=datetime(2024, 7, 31, tzinfo=UTC),
    )
    monthly_path = tmp_path / "day" / "BTC-USD" / "2024" / "2024-07.csv"
    assert [c.timestamp for c in read_aggregate_csv(monthly_path, "BTC-USD")] == [
        datetime(2024, 7, 31, tzinfo=UTC)
    ]
    incremental = [
        _make_candle(datetime(2024, 7, day, tzinfo=UTC), vwap=None, transactions=None)
        for day in range(2, 31)
    ]
    loader._save_candles_to_csv(
        incremental,
        "BTC-USD",
        "day",
        from_ts=datetime(2024, 7, 2, tzinfo=UTC),
        to_ts=datetime(2024, 7, 30, tzinfo=UTC),
    )
    parsed = read_aggregate_csv(monthly_path, "BTC-USD")
    expected = [datetime(2024, 7, day, tzinfo=UTC) for day in range(2, 31)] + [
        datetime(2024, 7, 31, tzinfo=UTC)
    ]
    assert [c.timestamp for c in parsed] == expected


def test_save_candles_day_timespan_refetch_updates_in_place(tmp_path: Path) -> None:
    """Let a re-fetched day overwrite the cached row rather than duplicate it.

    Given: A monthly daily file already holding one day's candle,
    When: the same day is fetched again with a changed close price,
    Then: the merged file keeps exactly one row for that day carrying the
          freshly fetched close, proving the merge is keyed by timestamp and
          idempotent.
    """
    loader = _cache_loader(tmp_path)
    original = _make_candle(datetime(2024, 8, 5, tzinfo=UTC), vwap=None, transactions=None)
    loader._save_candles_to_csv(
        [original],
        "BTC-USD",
        "day",
        from_ts=datetime(2024, 8, 5, tzinfo=UTC),
        to_ts=datetime(2024, 8, 5, tzinfo=UTC),
    )
    updated = AggregateCandle(
        ticker="X:BTCUSD",
        timestamp=datetime(2024, 8, 5, tzinfo=UTC),
        open=Decimal("1.20"),
        high=Decimal("1.25"),
        low=Decimal("1.15"),
        close=Decimal("9.99"),
        volume=Decimal("200"),
        vwap=None,
        transactions=None,
    )
    loader._save_candles_to_csv(
        [updated],
        "BTC-USD",
        "day",
        from_ts=datetime(2024, 8, 5, tzinfo=UTC),
        to_ts=datetime(2024, 8, 5, tzinfo=UTC),
    )
    monthly_path = tmp_path / "day" / "BTC-USD" / "2024" / "2024-08.csv"
    parsed = read_aggregate_csv(monthly_path, "BTC-USD")
    assert len(parsed) == 1
    assert parsed[0].close == Decimal("9.99")


def test_save_candles_day_timespan_empty_month_writes_marker(tmp_path: Path) -> None:
    """Write a header-only marker for a month with no candles.

    Given: No candles for a requested ``day``-timespan month,
    When: _save_candles_to_csv runs over that month,
    Then: A single header-only monthly marker file is created.
    """
    loader = _cache_loader(tmp_path)
    loader._save_candles_to_csv(
        [],
        "BTC-USD",
        "day",
        from_ts=datetime(2024, 4, 1, tzinfo=UTC),
        to_ts=datetime(2024, 4, 30, tzinfo=UTC),
    )
    monthly_path = tmp_path / "day" / "BTC-USD" / "2024" / "2024-04.csv"
    assert monthly_path.exists()
    assert read_aggregate_csv(monthly_path, "BTC-USD") == []


def test_save_candles_day_timespan_does_not_clobber_data_with_marker(tmp_path: Path) -> None:
    """Never overwrite a month that has data with an empty marker.

    Given: A month where only some days carry candles within the requested
        range,
    When: _save_candles_to_csv writes the month,
    Then: The monthly file keeps the data candles instead of being replaced
          by a header-only marker for the empty days.
    """
    loader = _cache_loader(tmp_path)
    candle = _make_candle(datetime(2024, 5, 10, tzinfo=UTC), vwap=None, transactions=None)
    loader._save_candles_to_csv(
        [candle],
        "BTC-USD",
        "day",
        from_ts=datetime(2024, 5, 1, tzinfo=UTC),
        to_ts=datetime(2024, 5, 31, tzinfo=UTC),
    )
    monthly_path = tmp_path / "day" / "BTC-USD" / "2024" / "2024-05.csv"
    parsed = read_aggregate_csv(monthly_path, "BTC-USD")
    assert [c.timestamp for c in parsed] == [datetime(2024, 5, 10, tzinfo=UTC)]


def test_save_candles_sub_day_preserves_per_day_files(tmp_path: Path) -> None:
    """Keep one file per day for sub-day timespans and mark empty days.

    Given: Minute candles on two of three days in a window,
    When: _save_candles_to_csv writes them with the ``minute`` timespan,
    Then: Each data day gets its own per-day file and the empty middle day
          gets a header-only marker, unchanged from prior behaviour.
    """
    loader = _cache_loader(tmp_path)
    candles = [
        _make_candle(datetime(2024, 1, 1, 9, tzinfo=UTC), vwap=None, transactions=None),
        _make_candle(datetime(2024, 1, 3, 9, tzinfo=UTC), vwap=None, transactions=None),
    ]
    loader._save_candles_to_csv(
        candles,
        "BTC-USD",
        "minute",
        from_ts=datetime(2024, 1, 1, tzinfo=UTC),
        to_ts=datetime(2024, 1, 3, tzinfo=UTC),
    )
    base = tmp_path / "minute" / "BTC-USD" / "2024"
    assert read_aggregate_csv(base / "2024-01-01.csv", "BTC-USD")[0].timestamp == datetime(
        2024, 1, 1, 9, tzinfo=UTC
    )
    assert read_aggregate_csv(base / "2024-01-02.csv", "BTC-USD") == []
    assert read_aggregate_csv(base / "2024-01-03.csv", "BTC-USD")[0].timestamp == datetime(
        2024, 1, 3, 9, tzinfo=UTC
    )


def test_read_aggregate_csv_skips_header_only_marker(tmp_path: Path) -> None:
    """Treat a header-only marker file as an empty no-op.

    Given: A header-only CSV marker file written for a data-less day,
    When: read_aggregate_csv parses it,
    Then: An empty list is returned.
    """
    loader = _cache_loader(tmp_path)
    csv_path = tmp_path / "minute" / "BTC-USD" / "2024" / "2024-01-02.csv"
    loader._write_csv(csv_path, [])
    assert read_aggregate_csv(csv_path, "ARCH") == []


def test_read_aggregate_csv_parses_naive_timestamp_as_utc(tmp_path: Path) -> None:
    """Parse a naive ISO timestamp as UTC.

    Given: A hand-written CSV row with a tz-naive ISO timestamp,
    When: read_aggregate_csv parses it,
    Then: The timestamp is assigned UTC.
    """
    csv_path = tmp_path / "naive.csv"
    csv_path.write_text(
        "timestamp,open,high,low,close,volume,vwap,transactions\n"
        "2024-01-01T14:00:00,1.2,1.25,1.15,1.22,200,1.21,7\n",
        encoding="utf-8",
    )
    parsed = read_aggregate_csv(csv_path, "ARCH")
    assert parsed[0].timestamp == datetime(2024, 1, 1, 14, tzinfo=UTC)


def test_read_aggregate_csv_skips_blank_timestamp_rows(tmp_path: Path) -> None:
    """Skip rows whose timestamp cell is blank.

    Given: A CSV row with an empty timestamp value,
    When: read_aggregate_csv parses the file,
    Then: The blank-timestamp row is skipped.
    """
    csv_path = tmp_path / "blank.csv"
    csv_path.write_text(
        "timestamp,open,high,low,close,volume,vwap,transactions\n"
        ",1.2,1.25,1.15,1.22,200,1.21,7\n"
        "2024-01-01T15:00:00+00:00,1.0,1.1,0.9,1.05,100,,3\n",
        encoding="utf-8",
    )
    parsed = read_aggregate_csv(csv_path, "ARCH")
    assert len(parsed) == 1
    assert parsed[0].timestamp == datetime(2024, 1, 1, 15, tzinfo=UTC)
    assert parsed[0].vwap is None


def test_iter_aggregate_csv_files_sub_day_granularity(tmp_path: Path) -> None:
    """Enumerate per-day CSV files for a sub-day timespan.

    Given: Two per-day CSV files under a minute-timespan cache directory,
    When: iter_aggregate_csv_files is called,
    Then: Files sort by representative day with each day parsed exactly.
    """
    loader = _cache_loader(tmp_path)
    base = tmp_path / "minute" / "BTC-USD" / "2024"
    base.mkdir(parents=True)
    (base / "2024-01-02.csv").write_text("timestamp\n", encoding="utf-8")
    (base / "2024-01-01.csv").write_text("timestamp\n", encoding="utf-8")
    files = loader.iter_aggregate_csv_files("BTC-USD", "minute")
    assert [day for _path, day in files] == [date(2024, 1, 1), date(2024, 1, 2)]


def test_iter_aggregate_csv_files_day_granularity(tmp_path: Path) -> None:
    """Enumerate monthly CSV files for the day timespan.

    Given: A monthly CSV file under a day-timespan cache directory,
    When: iter_aggregate_csv_files is called,
    Then: The monthly file maps to the first of that month.
    """
    loader = _cache_loader(tmp_path)
    base = tmp_path / "day" / "BTC-USD" / "2024"
    base.mkdir(parents=True)
    (base / "2024-03.csv").write_text("timestamp\n", encoding="utf-8")
    files = loader.iter_aggregate_csv_files("BTC-USD", "day")
    assert [day for _path, day in files] == [date(2024, 3, 1)]


def test_iter_aggregate_csv_files_skips_unparseable_names(tmp_path: Path) -> None:
    """Skip CSV files whose names are not recognized date forms.

    Given: A junk-named CSV alongside a valid per-day file,
    When: iter_aggregate_csv_files is called,
    Then: Only the valid file is returned.
    """
    loader = _cache_loader(tmp_path)
    base = tmp_path / "minute" / "BTC-USD" / "2024"
    base.mkdir(parents=True)
    (base / "notadate.csv").write_text("timestamp\n", encoding="utf-8")
    (base / "2024-13-99.csv").write_text("timestamp\n", encoding="utf-8")
    (base / "2024-01-05.csv").write_text("timestamp\n", encoding="utf-8")
    files = loader.iter_aggregate_csv_files("BTC-USD", "minute")
    assert [day for _path, day in files] == [date(2024, 1, 5)]


def test_iter_aggregate_csv_files_missing_directory(tmp_path: Path) -> None:
    """Return an empty list when the cache subtree does not exist.

    Given: A cache root with no directory for the archive symbol,
    When: iter_aggregate_csv_files is called,
    Then: An empty list is returned.
    """
    loader = _cache_loader(tmp_path)
    assert loader.iter_aggregate_csv_files("MISSING", "minute") == []


@pytest.mark.asyncio
async def test_fetch_aggregates_requires_client(tmp_path: Path) -> None:
    """Reject fetch_aggregates on a cache-only loader.

    Given: A loader constructed with client=None,
    When: fetch_aggregates is awaited,
    Then: ValueError is raised before any network access.
    """
    loader = _cache_loader(tmp_path)
    with pytest.raises(ValueError, match="cache-only loader"):
        await loader.fetch_aggregates(
            ticker="X:BTCUSD",
            multiplier=1,
            timespan="minute",
            from_ts=datetime(2024, 1, 1, tzinfo=UTC),
            to_ts=datetime(2024, 1, 2, tzinfo=UTC),
            archive_symbol="BTC-USD",
            save_csv=False,
        )


@pytest.mark.asyncio
async def test_fetch_grouped_daily_requires_client(tmp_path: Path) -> None:
    """Reject fetch_grouped_daily on a cache-only loader.

    Given: A loader constructed with client=None,
    When: fetch_grouped_daily is awaited,
    Then: ValueError is raised before any network access.
    """
    loader = _cache_loader(tmp_path)
    with pytest.raises(ValueError, match="cache-only loader"):
        await loader.fetch_grouped_daily(
            date(2024, 1, 1),
            market_type="crypto",
            save_csv=False,
        )
