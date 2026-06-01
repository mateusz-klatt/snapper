"""Unit tests for PolygonCsvLoaderService and the polygon-load-csv command."""

from datetime import UTC
from datetime import date
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from typing import cast
from unittest.mock import AsyncMock

import pytest
from typer.testing import CliRunner

import snapper.application.updaters.historical.csv_loader as csv_loader_module
import snapper.cli.app as app_module
from snapper.application.updaters.historical.csv_loader import PolygonCsvLoaderService
from snapper.cli.app import app
from snapper.core.types import ExchangeEnum
from snapper.infrastructure.historical.polygon.loader import AggregateCandle


class _StubSyncRepo:
    """Synchronous repository stub for the CSV loader service."""

    def __init__(
        self,
        archive_symbols: dict[str, str],
        native_to_archive: dict[str, str] | None = None,
    ) -> None:
        self._archive_symbols = archive_symbols
        self._native_to_archive = native_to_archive or {}
        self.disposed = False

    def get_archive_symbols(self) -> dict[str, str]:
        return dict(self._archive_symbols)

    def resolve_native_to_archive_symbol(self, native_symbol: str) -> str | None:
        return self._native_to_archive.get(native_symbol)

    def dispose(self) -> None:
        self.disposed = True


class _StubAsyncRepo:
    """Asynchronous repository stub recording upserts and instrument calls."""

    def __init__(self) -> None:
        self.upsert_batches: list[int] = []
        self.ensure_calls: list[str] = []
        self.engine = _StubEngine()

    async def ensure_instrument(
        self,
        symbol_public_id: str,
        exchange: str,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
    ) -> tuple[int, str]:
        self.ensure_calls.append(symbol_public_id)
        return (1, f"inst-{symbol_public_id}")

    async def upsert_candles(self, rows: list[dict[str, Any]]) -> int:
        self.upsert_batches.append(len(rows))
        return len(rows)


class _StubEngine:
    """Async engine stub recording disposal."""

    def __init__(self) -> None:
        self.disposed = False

    async def dispose(self) -> None:
        self.disposed = True


class _StubSymbolMapper:
    """Symbol mapper stub exposing the polygon reverse map."""

    def __init__(self, polygon_rest_to_native: dict[str, str] | None = None) -> None:
        self.polygon_rest_to_native = polygon_rest_to_native or {}
        self.loaded = False

    def load_cache_if_needed(self) -> None:
        self.loaded = True


def _build_service(
    monkeypatch: pytest.MonkeyPatch,
    *,
    symbols: list[str] | None,
    all_mapped: bool,
    timespan: str = "minute",
    since: date | None = None,
    until: date | None = None,
    polygon_rest_to_native: dict[str, str] | None = None,
) -> PolygonCsvLoaderService:
    """Construct a service with the symbol mapper singleton stubbed.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        symbols: Requested symbols.
        all_mapped: Whether to load every cached archive symbol.
        timespan: Timespan unit.
        since: Lower day bound.
        until: Upper day bound.
        polygon_rest_to_native: Reverse polygon map for the stub mapper.

    Returns:
        Constructed PolygonCsvLoaderService.
    """
    mapper = _StubSymbolMapper(polygon_rest_to_native)
    monkeypatch.setattr(
        csv_loader_module.SymbolMapperService,
        "get_instance",
        classmethod(lambda cls: mapper),
    )
    return PolygonCsvLoaderService(
        symbols=symbols,
        all_mapped=all_mapped,
        timespan=timespan,
        since=since,
        until=until,
    )


def _candle(timestamp: datetime) -> AggregateCandle:
    """Build a minimal AggregateCandle for loader tests.

    Args:
        timestamp: Candle timestamp.

    Returns:
        AggregateCandle instance.
    """
    return AggregateCandle(
        ticker="ARCH",
        timestamp=timestamp,
        open=Decimal("1"),
        high=Decimal("2"),
        low=Decimal("0.5"),
        close=Decimal("1.5"),
        volume=Decimal("10"),
        vwap=Decimal("1.4"),
        transactions=3,
    )


def test_get_default_parameters_uses_settings_instruments() -> None:
    """Expose settings-derived defaults for the process registry.

    Given: An AppSettings with a polygon instruments list,
    When: get_default_parameters is called,
    Then: The polygon symbols and load defaults are returned.
    """
    settings = cast(Any, type("S", (), {"instruments": {"polygon": ["X:BTCUSD"]}})())
    params = PolygonCsvLoaderService.get_default_parameters(settings)
    assert params["symbols"] == ["X:BTCUSD"]
    assert params["all_mapped"] is False
    assert params["timespan"] == "day"
    assert params["since"] is None
    assert params["until"] is None


def test_candle_in_range_respects_since_and_until(monkeypatch: pytest.MonkeyPatch) -> None:
    """Bound per-candle day selection by since/until inclusively.

    Given: A service with since and until set,
    When: _candle_in_range is evaluated across the boundary,
    Then: Only candle days within the inclusive window pass.
    """
    service = _build_service(
        monkeypatch,
        symbols=None,
        all_mapped=True,
        since=date(2024, 1, 2),
        until=date(2024, 1, 4),
    )
    assert service._candle_in_range(date(2024, 1, 1)) is False
    assert service._candle_in_range(date(2024, 1, 2)) is True
    assert service._candle_in_range(date(2024, 1, 4)) is True
    assert service._candle_in_range(date(2024, 1, 5)) is False


def test_candle_in_range_unbounded(monkeypatch: pytest.MonkeyPatch) -> None:
    """Accept any candle day when no bounds are configured.

    Given: A service with since and until unset,
    When: _candle_in_range is evaluated,
    Then: Every candle day passes.
    """
    service = _build_service(monkeypatch, symbols=None, all_mapped=True)
    assert service._candle_in_range(date(1999, 1, 1)) is True


def test_file_in_range_selects_partial_month_files(monkeypatch: pytest.MonkeyPatch) -> None:
    """Select a monthly day-file when its month overlaps the window.

    Given: A ``day``-timespan service whose window starts mid-March,
    When: _file_in_range is evaluated for the March monthly file
        (represented by 2024-03-01) and neighbouring months,
    Then: The March file is selected even though its representative day
        falls before ``since``, while a wholly-out-of-window month is
        rejected. The December branch exercises the year rollover. Month
        broadening applies only to the ``day`` timespan, where every day of
        a month shares one monthly file.
    """
    service = _build_service(
        monkeypatch,
        symbols=None,
        all_mapped=True,
        timespan="day",
        since=date(2024, 3, 15),
        until=date(2024, 3, 20),
    )
    assert service._file_in_range(date(2024, 3, 1)) is True
    assert service._file_in_range(date(2024, 2, 1)) is False
    assert service._file_in_range(date(2024, 4, 1)) is False
    december = _build_service(
        monkeypatch,
        symbols=None,
        all_mapped=True,
        timespan="day",
        since=date(2024, 12, 10),
        until=date(2025, 1, 5),
    )
    assert december._file_in_range(date(2024, 12, 1)) is True


def test_file_in_range_sub_day_does_not_broaden_to_month(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test exact per-day file selection for sub-day timespans.

    Given: A ``minute``-timespan service whose window covers a few days in
        mid-March (per-day cache files, one file per exact day),
    When: _file_in_range is evaluated for an in-window day and for sibling
        days in the same month but outside the window,
    Then: Only the in-window day passes. The month-broadening that the
        ``day`` timespan needs is NOT applied, so whole months of per-day
        files are not read and parsed just for :meth:`_candle_in_range` to
        drop the out-of-window rows.
    """
    service = _build_service(
        monkeypatch,
        symbols=None,
        all_mapped=True,
        timespan="minute",
        since=date(2024, 3, 15),
        until=date(2024, 3, 20),
    )
    assert service._file_in_range(date(2024, 3, 17)) is True
    assert service._file_in_range(date(2024, 3, 1)) is False
    assert service._file_in_range(date(2024, 3, 25)) is False


def test_file_in_range_unbounded(monkeypatch: pytest.MonkeyPatch) -> None:
    """Accept any cache file when no bounds are configured.

    Given: A ``day``-timespan service with since and until unset,
    When: _file_in_range is evaluated,
    Then: Every file passes.
    """
    service = _build_service(monkeypatch, symbols=None, all_mapped=True, timespan="day")
    assert service._file_in_range(date(1999, 6, 1)) is True


def test_resolve_cache_targets_skips_unmapped(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Walk the cache subtree and skip directories without a mapping.

    Given: Cache directories for a mapped and an unmapped archive symbol,
    When: _resolve_cache_targets is called,
    Then: Only the mapped archive symbol is returned and a file (not a dir)
          is ignored.
    """
    monkeypatch.setattr(csv_loader_module, "_CACHE_ROOT", tmp_path)
    subtree = tmp_path / "minute"
    (subtree / "BTC-USD").mkdir(parents=True)
    (subtree / "STALE-SYM").mkdir(parents=True)
    (subtree / "loose-file.txt").write_text("x", encoding="utf-8")
    service = _build_service(monkeypatch, symbols=None, all_mapped=True)
    service._archive_symbols = {"pid-btc": "BTC-USD"}
    targets = service._resolve_cache_targets({"BTC-USD": "pid-btc"})
    assert targets == [("BTC-USD", "pid-btc")]


def test_resolve_cache_targets_missing_subtree(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Return no targets when the timespan subtree is absent.

    Given: A cache root with no subtree for the timespan,
    When: _resolve_cache_targets is called,
    Then: An empty list is returned.
    """
    monkeypatch.setattr(csv_loader_module, "_CACHE_ROOT", tmp_path)
    service = _build_service(monkeypatch, symbols=None, all_mapped=True)
    assert service._resolve_cache_targets({}) == []


def test_resolve_requested_targets_native_and_polygon(monkeypatch: pytest.MonkeyPatch) -> None:
    """Resolve native and Polygon-format symbols, skipping unmapped ones.

    Given: A native symbol, a Polygon symbol, and an unmapped symbol,
    When: _resolve_requested_targets is called,
    Then: Mapped symbols resolve to archive targets and the unmapped one
          is skipped.
    """
    service = _build_service(
        monkeypatch,
        symbols=["BTC-USD", "X:ETHUSD", "X:NOPE"],
        all_mapped=False,
        polygon_rest_to_native={"X:ETHUSD": "ETH-USD"},
    )
    service._archive_symbols = {"pid-btc": "BTC-USD", "pid-eth": "ETH-USD"}
    service._db_sync = cast(
        Any,
        _StubSyncRepo(
            archive_symbols={"pid-btc": "BTC-USD", "pid-eth": "ETH-USD"},
            native_to_archive={"BTC-USD": "BTC-USD", "ETH-USD": "ETH-USD"},
        ),
    )
    targets = service._resolve_requested_targets({}, ["BTC-USD", "X:ETHUSD", "X:NOPE"])
    assert targets == [("BTC-USD", "pid-btc"), ("ETH-USD", "pid-eth")]


def test_resolve_symbol_to_archive_unmapped_polygon(monkeypatch: pytest.MonkeyPatch) -> None:
    """Return None for a Polygon symbol absent from the mapper.

    Given: A Polygon symbol not present in the reverse map,
    When: _resolve_symbol_to_archive is called,
    Then: None is returned.
    """
    service = _build_service(
        monkeypatch, symbols=["X:NOPE"], all_mapped=False, polygon_rest_to_native={}
    )
    service._db_sync = cast(Any, _StubSyncRepo(archive_symbols={}))
    assert service._resolve_symbol_to_archive("X:NOPE") is None


def test_resolve_symbol_to_archive_no_archive(monkeypatch: pytest.MonkeyPatch) -> None:
    """Return None when the native symbol has no archive mapping.

    Given: A native symbol with no archive symbol in the repository,
    When: _resolve_symbol_to_archive is called,
    Then: None is returned.
    """
    service = _build_service(monkeypatch, symbols=["BTC-USD"], all_mapped=False)
    service._db_sync = cast(Any, _StubSyncRepo(archive_symbols={}, native_to_archive={}))
    assert service._resolve_symbol_to_archive("BTC-USD") is None


def test_resolve_symbol_to_archive_archive_without_public_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return None when the archive symbol has no public_id mapping.

    Given: A repository that resolves an archive symbol absent from the
        public-id map,
    When: _resolve_symbol_to_archive is called,
    Then: None is returned.
    """
    service = _build_service(monkeypatch, symbols=["BTC-USD"], all_mapped=False)
    service._archive_symbols = {}
    service._db_sync = cast(
        Any,
        _StubSyncRepo(archive_symbols={}, native_to_archive={"BTC-USD": "BTC-USD"}),
    )
    assert service._resolve_symbol_to_archive("BTC-USD") is None


def _settings_with_polygon(symbols: list[str]) -> Any:
    """Build a settings stub exposing a Polygon instruments list.

    Args:
        symbols: Polygon instrument symbols the settings report.

    Returns:
        A settings stub whose ``instruments`` maps polygon to ``symbols``.
    """
    return cast(
        Any,
        type(
            "Settings",
            (),
            {"instruments": {ExchangeEnum.POLYGON: list(symbols)}},
        )(),
    )


def test_resolve_targets_defaults_to_settings_symbols(monkeypatch: pytest.MonkeyPatch) -> None:
    """Default to settings-configured symbols when neither flag is given.

    Given: A service with symbols=None, all_mapped=False, and settings
        listing an explicit Polygon symbol,
    When: _resolve_targets is called,
    Then: It resolves the settings symbol rather than loading all cached
          directories.
    """
    service = _build_service(monkeypatch, symbols=None, all_mapped=False)
    service.settings = _settings_with_polygon(["BTC-USD"])
    service._archive_symbols = {"pid-btc": "BTC-USD"}
    service._db_sync = cast(
        Any,
        _StubSyncRepo(
            archive_symbols={"pid-btc": "BTC-USD"},
            native_to_archive={"BTC-USD": "BTC-USD"},
        ),
    )
    assert service._resolve_targets() == [("BTC-USD", "pid-btc")]


def test_resolve_targets_wildcard_settings_loads_cache(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Treat wildcard settings like --all and enumerate the cache.

    Given: A service with symbols=None and wildcard Polygon settings,
    When: _resolve_targets is called,
    Then: It dispatches to cache enumeration (empty subtree -> empty list).
    """
    monkeypatch.setattr(csv_loader_module, "_CACHE_ROOT", tmp_path)
    service = _build_service(monkeypatch, symbols=None, all_mapped=False)
    service.settings = _settings_with_polygon(["*"])
    service._archive_symbols = {"pid": "BTC-USD"}
    assert service._resolve_targets() == []


def test_resolve_targets_empty_settings_returns_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    """Return no targets when neither flag is set and settings are empty.

    Given: A service with symbols=None and an empty Polygon settings list,
    When: _resolve_targets is called,
    Then: An empty list is returned without touching the cache.
    """
    service = _build_service(monkeypatch, symbols=None, all_mapped=False)
    service.settings = _settings_with_polygon([])
    assert service._resolve_targets() == []


def test_resolve_targets_all_mapped_enumerates_cache(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Enumerate the cache subtree when --all is given.

    Given: A service with all_mapped=True,
    When: _resolve_targets is called,
    Then: It dispatches to cache enumeration regardless of settings.
    """
    monkeypatch.setattr(csv_loader_module, "_CACHE_ROOT", tmp_path)
    service = _build_service(monkeypatch, symbols=None, all_mapped=True)
    service.settings = _settings_with_polygon(["BTC-USD"])
    service._archive_symbols = {"pid": "BTC-USD"}
    assert service._resolve_targets() == []


def test_resolve_targets_dispatches_to_requested(monkeypatch: pytest.MonkeyPatch) -> None:
    """Use explicit-symbol resolution when symbols are provided.

    Given: A service with explicit symbols and all_mapped=False,
    When: _resolve_targets is called,
    Then: It dispatches to requested-symbol resolution.
    """
    service = _build_service(monkeypatch, symbols=["BTC-USD"], all_mapped=False)
    service._archive_symbols = {"pid-btc": "BTC-USD"}
    service._db_sync = cast(
        Any,
        _StubSyncRepo(
            archive_symbols={"pid-btc": "BTC-USD"},
            native_to_archive={"BTC-USD": "BTC-USD"},
        ),
    )
    assert service._resolve_targets() == [("BTC-USD", "pid-btc")]


@pytest.mark.asyncio
async def test_load_archive_symbol_batches_and_filters(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Read in-range cache files, skip markers, and batch upserts.

    Given: Three cached files (two in range incl. a header-only marker,
        one out of range) and a batch size of 2,
    When: _load_archive_symbol runs,
    Then: Only in-range data rows are upserted, the marker is skipped, and
          rows are split into batches.
    """
    monkeypatch.setattr(csv_loader_module, "_CACHE_ROOT", tmp_path)
    service = _build_service(
        monkeypatch,
        symbols=None,
        all_mapped=True,
        since=date(2024, 1, 1),
        until=date(2024, 1, 2),
    )
    service.BATCH_COMMIT_SIZE = 2
    async_repo = _StubAsyncRepo()
    service._db_async = cast(Any, async_repo)
    files = [
        (tmp_path / "in-range.csv", date(2024, 1, 1)),
        (tmp_path / "marker.csv", date(2024, 1, 2)),
        (tmp_path / "out-of-range.csv", date(2024, 2, 1)),
    ]

    class _StubLoader:
        def iter_aggregate_csv_files(
            self, archive_symbol: str, timespan: str
        ) -> list[tuple[Path, date]]:
            return files

    service._loader = cast(Any, _StubLoader())

    read_results = {
        files[0][0]: [
            _candle(datetime(2024, 1, 1, 9, tzinfo=UTC)),
            _candle(datetime(2024, 1, 1, 10, tzinfo=UTC)),
            _candle(datetime(2024, 1, 1, 11, tzinfo=UTC)),
        ],
        files[1][0]: [],
    }

    def _fake_read(path: Path, ticker: str) -> list[AggregateCandle]:
        assert ticker == "BTC-USD"
        return read_results[path]

    monkeypatch.setattr(csv_loader_module, "read_aggregate_csv", _fake_read)
    await service._load_archive_symbol("BTC-USD", "pid-btc")
    assert async_repo.ensure_calls == ["pid-btc"]
    assert async_repo.upsert_batches == [2, 1]


@pytest.mark.asyncio
async def test_load_archive_symbol_partial_month_row_filter(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Select a partial-month day-file and row-filter its candles.

    Given: A ``day``-timespan monthly file represented by 2024-03-01 whose
        candles span the whole of March, with a since bound of 2024-03-15,
    When: _load_archive_symbol runs,
    Then: The March file is loaded despite its representative day preceding
          since, and only candles on or after 2024-03-15 are upserted.
    """
    monkeypatch.setattr(csv_loader_module, "_CACHE_ROOT", tmp_path)
    service = _build_service(
        monkeypatch,
        symbols=None,
        all_mapped=True,
        timespan="day",
        since=date(2024, 3, 15),
        until=date(2024, 3, 20),
    )
    async_repo = _StubAsyncRepo()
    service._db_async = cast(Any, async_repo)
    march_file = tmp_path / "day" / "BTC-USD" / "2024" / "2024-03.csv"
    files = [(march_file, date(2024, 3, 1))]

    class _StubLoader:
        def iter_aggregate_csv_files(
            self, archive_symbol: str, timespan: str
        ) -> list[tuple[Path, date]]:
            return files

    service._loader = cast(Any, _StubLoader())
    month_candles = [
        _candle(datetime(2024, 3, 1, 9, tzinfo=UTC)),
        _candle(datetime(2024, 3, 14, 9, tzinfo=UTC)),
        _candle(datetime(2024, 3, 15, 9, tzinfo=UTC)),
        _candle(datetime(2024, 3, 20, 9, tzinfo=UTC)),
        _candle(datetime(2024, 3, 25, 9, tzinfo=UTC)),
    ]

    def _fake_read(path: Path, ticker: str) -> list[AggregateCandle]:
        assert path == march_file
        return month_candles

    monkeypatch.setattr(csv_loader_module, "read_aggregate_csv", _fake_read)
    await service._load_archive_symbol("BTC-USD", "pid-btc")
    assert async_repo.upsert_batches == [2]


@pytest.mark.asyncio
async def test_load_archive_symbol_skips_non_overlapping_month(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Skip a monthly file whose month is wholly outside the window.

    Given: A February monthly file and a window starting in March,
    When: _load_archive_symbol runs,
    Then: The February file is never read and nothing is upserted.
    """
    monkeypatch.setattr(csv_loader_module, "_CACHE_ROOT", tmp_path)
    service = _build_service(
        monkeypatch,
        symbols=None,
        all_mapped=True,
        timespan="day",
        since=date(2024, 3, 1),
        until=date(2024, 3, 31),
    )
    async_repo = _StubAsyncRepo()
    service._db_async = cast(Any, async_repo)
    feb_file = tmp_path / "day" / "BTC-USD" / "2024" / "2024-02.csv"
    files = [(feb_file, date(2024, 2, 1))]

    class _StubLoader:
        def iter_aggregate_csv_files(
            self, archive_symbol: str, timespan: str
        ) -> list[tuple[Path, date]]:
            return files

    service._loader = cast(Any, _StubLoader())

    def _fail_read(path: Path, ticker: str) -> list[AggregateCandle]:
        raise AssertionError("out-of-window file should not be read")

    monkeypatch.setattr(csv_loader_module, "read_aggregate_csv", _fail_read)
    await service._load_archive_symbol("BTC-USD", "pid-btc")
    assert async_repo.upsert_batches == []


@pytest.mark.asyncio
async def test_start_happy_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Run the full start() flow against stubbed dependencies.

    Given: A cache with one mapped archive symbol and stubbed repos,
    When: start() runs in all_mapped mode,
    Then: Candles are upserted and resources are disposed.
    """
    monkeypatch.setattr(csv_loader_module, "_CACHE_ROOT", tmp_path)
    (tmp_path / "minute" / "BTC-USD").mkdir(parents=True)
    sync_repo = _StubSyncRepo(archive_symbols={"pid-btc": "BTC-USD"})
    async_repo = _StubAsyncRepo()
    monkeypatch.setattr(csv_loader_module, "get_settings_service", AsyncMock(return_value=object()))
    monkeypatch.setattr(csv_loader_module, "get_settings_with_service", lambda svc: _settings())
    monkeypatch.setattr(csv_loader_module, "DatabaseRepository", lambda url: sync_repo)
    monkeypatch.setattr(csv_loader_module, "get_repository", lambda url: async_repo)
    monkeypatch.setattr(
        csv_loader_module,
        "read_aggregate_csv",
        lambda path, ticker: [_candle(datetime(2024, 1, 1, 9, tzinfo=UTC))],
    )

    files = [(tmp_path / "minute" / "BTC-USD" / "2024" / "2024-01-01.csv", date(2024, 1, 1))]
    monkeypatch.setattr(
        csv_loader_module.PolygonHistoricalLoader,
        "iter_aggregate_csv_files",
        lambda self, archive_symbol, timespan: files,
    )
    service = _build_service(monkeypatch, symbols=None, all_mapped=True)
    service.settings = cast(Any, _settings())
    await service.start()
    assert async_repo.upsert_batches == [1]
    assert async_repo.engine.disposed is True
    assert sync_repo.disposed is True
    assert service._db_async is None


@pytest.mark.asyncio
async def test_start_warns_when_no_targets(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Return early with a warning when no targets resolve.

    Given: An empty cache subtree,
    When: start() runs in all_mapped mode,
    Then: No upserts occur and resources are still disposed.
    """
    monkeypatch.setattr(csv_loader_module, "_CACHE_ROOT", tmp_path)
    sync_repo = _StubSyncRepo(archive_symbols={})
    async_repo = _StubAsyncRepo()
    monkeypatch.setattr(csv_loader_module, "get_settings_service", AsyncMock(return_value=object()))
    monkeypatch.setattr(csv_loader_module, "get_settings_with_service", lambda svc: _settings())
    monkeypatch.setattr(csv_loader_module, "DatabaseRepository", lambda url: sync_repo)
    monkeypatch.setattr(csv_loader_module, "get_repository", lambda url: async_repo)
    service = _build_service(monkeypatch, symbols=None, all_mapped=True)
    service.settings = cast(Any, _settings())
    await service.start()
    assert async_repo.upsert_batches == []
    assert async_repo.engine.disposed is True


@pytest.mark.asyncio
async def test_dispose_resources_tolerates_missing_repos(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Dispose cleanly when no repositories were allocated.

    Given: A freshly constructed service with no repos,
    When: _dispose_resources is awaited,
    Then: It completes without error and clears references.
    """
    service = _build_service(monkeypatch, symbols=None, all_mapped=True)
    await service._dispose_resources()
    assert service._db_async is None
    assert service._loader is None


class _CliDummyService:
    """CLI stub service capturing constructor kwargs."""

    last_kwargs: dict[str, Any] = {}

    def __init__(
        self,
        symbols: list[str] | None,
        all_mapped: bool,
        timespan: str,
        since: date | None,
        until: date | None,
    ) -> None:
        _CliDummyService.last_kwargs = {
            "symbols": symbols,
            "all_mapped": all_mapped,
            "timespan": timespan,
            "since": since,
            "until": until,
        }

    async def start(self) -> None:
        return None


class _CliFailingService(_CliDummyService):
    """CLI stub service that fails on start."""

    async def start(self) -> None:
        raise RuntimeError("load-fail")


def test_cli_polygon_load_csv_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """Run the polygon-load-csv command happy path.

    Given: A stub loader service,
    When: polygon-load-csv is invoked with symbol, timespan, since, until,
    Then: It exits 0 and forwards parsed dates to the service.
    """
    monkeypatch.setattr(app_module, "PolygonCsvLoaderService", _CliDummyService)
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "polygon-load-csv",
            "--symbol",
            "X:BTCUSD",
            "--timespan",
            "minute",
            "--since",
            "2024-01-01",
            "--until",
            "2024-01-31",
        ],
    )
    assert result.exit_code == 0
    assert "Polygon CSV load complete!" in result.stdout
    assert _CliDummyService.last_kwargs["symbols"] == ["X:BTCUSD"]
    assert _CliDummyService.last_kwargs["since"] == date(2024, 1, 1)
    assert _CliDummyService.last_kwargs["until"] == date(2024, 1, 31)


def test_cli_polygon_load_csv_all_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    """Run the command with --all and default date bounds.

    Given: A stub loader service,
    When: polygon-load-csv is invoked with --all and no dates,
    Then: since/until default to None and all_mapped is True.
    """
    monkeypatch.setattr(app_module, "PolygonCsvLoaderService", _CliDummyService)
    runner = CliRunner()
    result = runner.invoke(app, ["polygon-load-csv", "--all"])
    assert result.exit_code == 0
    assert _CliDummyService.last_kwargs["all_mapped"] is True
    assert _CliDummyService.last_kwargs["since"] is None
    assert _CliDummyService.last_kwargs["until"] is None


def test_cli_polygon_load_csv_reports_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Surface a service error as exit code 1.

    Given: A stub loader service that raises on start,
    When: polygon-load-csv is invoked,
    Then: It exits 1 with an error message.
    """
    monkeypatch.setattr(app_module, "PolygonCsvLoaderService", _CliFailingService)
    runner = CliRunner()
    result = runner.invoke(app, ["polygon-load-csv", "--all"])
    assert result.exit_code == 1
    assert "Error during Polygon CSV load" in result.stdout


def _settings() -> Any:
    """Build a minimal settings stub for start()."""
    return cast(
        Any,
        type(
            "Settings",
            (),
            {"db_url": "sqlite+aiosqlite:///:memory:", "zmq_broker_xsub": "tcp://x"},
        )(),
    )
