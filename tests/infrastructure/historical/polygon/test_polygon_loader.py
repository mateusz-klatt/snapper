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
from snapper.infrastructure.historical.polygon.loader import PolygonHistoricalLoader
from snapper.infrastructure.historical.polygon.loader import _format_decimal


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
async def test_fetch_aggregates_writes_csv_and_empty_marker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fetch aggregates with resume_from filters and writes CSV.

    Given: Stub client returning aggregates with mixed timestamps,
    When: fetch_aggregates called with resume_from filter,
    Then: Only newer candles are returned and CSV files are created.
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
        resume_from=resume_from,
        save_csv=True,
    )
    assert [c.timestamp for c in candles] == [included_early, included_late]
    assert stub_client.aggregate_calls[0]["from_ts"] == resume_from
    assert sleep_calls == [0.0]
    data_csv = tmp_path / "minute" / "X_BTCUSD" / "2024" / "2024-01-01.csv"
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
    empty_marker = tmp_path / "minute" / "X_BTCUSD" / "2024" / "2024-01-02.csv"
    assert empty_marker.exists()
    with empty_marker.open(encoding="utf-8") as marker_file:
        marker_rows = list(csv.reader(marker_file))
    assert marker_rows == [
        ["timestamp", "open", "high", "low", "close", "volume", "vwap", "transactions"]
    ]


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
    """Preserve existing CSV files.

    Given: Pre-existing CSV file in cache directory,
    When: fetch_aggregates is called,
    Then: Existing file content is preserved.
    """
    stub_client = _StubPolygonClient(aggregates=[])
    loader = PolygonHistoricalLoader(
        cast(PolygonExchangeClient, stub_client),
        cache_root=tmp_path,
        rate_delay_seconds=0.0,
    )
    existing_marker = tmp_path / "minute" / "X_BTCUSD" / "2024" / "2024-01-01.csv"
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
    Then: Returns appropriate filename pattern.
    """
    loader = PolygonHistoricalLoader(
        cast(PolygonExchangeClient, _StubPolygonClient()),
        cache_root=tmp_path,
        rate_delay_seconds=0.0,
    )
    minute_path = loader.get_aggregate_csv_path_for_day("X:BTCUSD", "minute", date(2024, 1, 1))
    day_path = loader.get_aggregate_csv_path_for_day("X:BTCUSD", "day", date(2024, 1, 1))
    assert minute_path.name == "2024-01-01.csv"
    assert day_path.name == "2024-01.csv"
    assert minute_path.parent.exists()
    assert day_path.parent.exists()


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
