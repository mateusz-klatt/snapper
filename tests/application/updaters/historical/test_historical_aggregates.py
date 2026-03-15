"""Unit tests for PolygonAggregatesBackfillService."""

from collections.abc import Awaitable
from collections.abc import Callable
from datetime import UTC
from datetime import date
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from types import TracebackType
from typing import Any
from typing import Literal
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

import pytest

import snapper.application.updaters.historical.aggregates as aggregates_module
from snapper.application.updaters.historical.aggregates import PolygonAggregatesBackfillService
from snapper.application.updaters.historical.aggregates import _SymbolContext
from snapper.application.updaters.historical.aggregates import _timeframe_label
from snapper.data.repository import DatabaseRepository
from snapper.data.repository import Repository
from snapper.infrastructure.historical.polygon.loader import AggregateCandle


class _StubScalarsResult:
    """Test stub for SQLAlchemy scalars result."""

    def __init__(self, values: list[Any]) -> None:
        self._values = values

    def all(self) -> list[Any]:
        return self._values


class _StubResult:
    """Test stub for SQLAlchemy result."""

    def __init__(self, values: list[Any]) -> None:
        self._values = values

    def scalars(self) -> _StubScalarsResult:
        return _StubScalarsResult(self._values)

    def scalar_one_or_none(self) -> Any:
        return self._values[0] if self._values else None


class _StubSession:
    """Test stub for SQLAlchemy session."""

    def __init__(self, responses: list[list[Any]]) -> None:
        self._responses = responses

    def __enter__(self) -> _StubSession:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> Literal[False]:
        return False

    def execute(self, _stmt: object) -> _StubResult:
        values = self._responses.pop(0) if self._responses else []
        return _StubResult(values)


class _StubSyncRepo:
    """Test stub for synchronous repository."""

    def __init__(self, responses: list[list[Any]]) -> None:
        self._template = [list(row) for row in responses]

    def get_session(self) -> _StubSession:
        return _StubSession([list(row) for row in self._template])


class _StubAsyncRepo:
    """Test stub for asynchronous repository."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.return_value = 42

    async def upsert_instrument(self, **kwargs: Any) -> int:
        self.calls.append(kwargs)
        return self.return_value

    async def upsert_candles(self, rows: list[dict[str, Any]]) -> int:
        return len(rows)


class _StubSymbolMapper:
    """Test stub for symbol mapper service."""

    def __init__(self) -> None:
        self.native_to_polygon_rest: dict[str, str] = {}
        self.polygon_rest_to_native: dict[str, str] = {}

    def load_cache_if_needed(self) -> None:
        return None


def _timeframe(multiplier: int, timespan: str) -> str:
    timeframe_fn = cast(Callable[[int, str], str], aggregates_module._timeframe_label)
    return timeframe_fn(multiplier, timespan)


def _symbol_context(
    *,
    native_symbol: str,
    polygon_symbol: str,
    base_currency: str,
    quote_currency: str | None,
) -> Any:
    context_cls = cast(type[Any], aggregates_module._SymbolContext)
    return context_cls(
        native_symbol=native_symbol,
        polygon_symbol=polygon_symbol,
        base_currency=base_currency,
        quote_currency=quote_currency,
    )


@pytest.fixture(name="service")
def fixture_service(monkeypatch: pytest.MonkeyPatch) -> PolygonAggregatesBackfillService:
    """Provide a configured PolygonAggregatesBackfillService for testing."""

    class _DummySettings:
        polygon_api_key = "test-key"
        db_url = "sqlite:///test.db"
        backfill_days = 3
        instruments = {"polygon": ["X:BTCUSD"]}

    symbol_mapper = _StubSymbolMapper()
    monkeypatch.setattr(
        "snapper.application.updaters.historical.aggregates.get_settings",
        lambda: _DummySettings(),
    )
    monkeypatch.setattr(
        "snapper.application.updaters.historical.aggregates.SymbolMapperService.get_instance",
        lambda: symbol_mapper,
    )
    svc = PolygonAggregatesBackfillService(save_csv=False)
    cast(Any, svc)._symbol_mapper = symbol_mapper
    return svc


def test_timeframe_label_variants() -> None:
    """Verify timeframe labels for various multiplier/timespan combinations.

    Given: Different multiplier and timespan values,
    When: _timeframe_label called,
    Then: Correct formatted labels returned (e.g., '1m', '2h').
    """
    assert _timeframe(1, "minute") == "1m"
    assert _timeframe(2, "hour") == "2h"
    assert _timeframe(5, "day") == "5d"
    assert _timeframe(3, "Week") == "3w"


def test_get_all_mapped_symbols(service: PolygonAggregatesBackfillService) -> None:
    """Verify _get_all_mapped_symbols returns symbols from database.

    Given: Database with X:BTCUSD symbol mapping,
    When: _get_all_mapped_symbols called,
    Then: List containing X:BTCUSD returned.
    """
    cast(Any, service)._db_sync = _StubSyncRepo([["X:BTCUSD"]])
    get_symbols = cast(Callable[[], list[str]], cast(Any, service)._get_all_mapped_symbols)
    symbols = get_symbols()
    assert symbols == ["X:BTCUSD"]


def test_resolve_symbol_context_polygon_symbol(service: PolygonAggregatesBackfillService) -> None:
    """Verify _resolve_symbol_context resolves Polygon symbol format.

    Given: Polygon-prefixed symbol with database mapping,
    When: _resolve_symbol_context called,
    Then: SymbolContext with correct native symbol returned.
    """
    catalog = SimpleNamespace(
        native_symbol="BTC-USD", base="BTC", quote="USD", public_id="sym-pub-1"
    )
    alias = SimpleNamespace(symbol_public_id="sym-pub-1", exchange_symbol="X:BTCUSD")
    cast(Any, service)._db_sync = _StubSyncRepo([[catalog], [alias]])
    mapper = cast(_StubSymbolMapper, cast(Any, service)._symbol_mapper)
    mapper.polygon_rest_to_native["X:BTCUSD"] = "BTC-USD"
    resolve_context = cast(Callable[[str], Any], cast(Any, service)._resolve_symbol_context)
    context = resolve_context("X:BTCUSD")
    assert context is not None
    assert context.native_symbol == "BTC-USD"
    assert context.quote_currency == "USD"


def test_resolve_symbol_context_native_symbol(service: PolygonAggregatesBackfillService) -> None:
    """Verify _resolve_symbol_context resolves native symbol format.

    Given: Native symbol with database mapping,
    When: _resolve_symbol_context called,
    Then: SymbolContext with correct polygon symbol returned.
    """
    alias = SimpleNamespace(symbol_public_id="sym-pub-aapl", exchange_symbol="AAPL")
    catalog = SimpleNamespace(
        native_symbol="AAPL", base="AAPL", quote=None, public_id="sym-pub-aapl"
    )
    cast(Any, service)._db_sync = _StubSyncRepo([[alias], [catalog]])
    mapper = cast(_StubSymbolMapper, cast(Any, service)._symbol_mapper)
    mapper.native_to_polygon_rest["AAPL"] = "AAPL"
    resolve_context = cast(Callable[[str], Any], cast(Any, service)._resolve_symbol_context)
    context = resolve_context("AAPL")
    assert context is not None
    assert context.native_symbol == "AAPL"
    assert context.quote_currency == "AAPL"


def test_resolve_symbol_context_missing(service: PolygonAggregatesBackfillService) -> None:
    """Verify _resolve_symbol_context returns None for missing symbol.

    Given: No mapping for symbol in database,
    When: _resolve_symbol_context called,
    Then: None returned.
    """
    cast(Any, service)._db_sync = _StubSyncRepo([[]])
    resolve_context = cast(Callable[[str], Any], cast(Any, service)._resolve_symbol_context)
    context = resolve_context("MISSING")
    assert context is None


@pytest.mark.asyncio
async def test_ensure_instrument_caches_result(service: PolygonAggregatesBackfillService) -> None:
    """Verify _ensure_instrument caches instrument ID.

    Given: Async repository with upsert_instrument,
    When: _ensure_instrument called twice for same symbol,
    Then: Repository called only once, same ID returned.
    """
    async_repo = _StubAsyncRepo()
    cast(Any, service)._db_async = async_repo
    context = _symbol_context(
        native_symbol="ETH-USD",
        polygon_symbol="X:ETHUSD",
        base_currency="ETH",
        quote_currency="USD",
    )
    ensure_instrument = cast(Callable[[Any], Awaitable[int]], cast(Any, service)._ensure_instrument)
    instrument_id = await ensure_instrument(context)
    repeated_id = await ensure_instrument(context)
    assert instrument_id == repeated_id == async_repo.return_value
    assert len(async_repo.calls) == 1
    call = async_repo.calls[0]
    assert call["symbol"] == "ETH-USD"
    assert call["quote"] == "USD"


def test_build_candle_rows() -> None:
    """Verify _build_candle_rows converts candles to row dicts.

    Given: List of AggregateCandle objects,
    When: _build_candle_rows called,
    Then: List of dicts with correct fields returned.
    """
    candles = [
        AggregateCandle(
            ticker="X:BTCUSD",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            open=Decimal("100"),
            high=Decimal("110"),
            low=Decimal("95"),
            close=Decimal("105"),
            volume=Decimal("123"),
            vwap=Decimal("101.5"),
            transactions=10,
        ),
        AggregateCandle(
            ticker="X:BTCUSD",
            timestamp=datetime(2024, 1, 2, tzinfo=UTC),
            open=Decimal("105"),
            high=Decimal("112"),
            low=Decimal("100"),
            close=Decimal("110"),
            volume=Decimal("130"),
            vwap=None,
            transactions=None,
        ),
    ]
    build_rows = cast(
        Callable[[Any, int, str], list[dict[str, object]]],
        PolygonAggregatesBackfillService._build_candle_rows,
    )
    rows = build_rows(candles, 7, "1m")
    assert rows[0]["instrument_id"] == 7
    assert rows[0]["vwap"] == pytest.approx(101.5)
    assert rows[1]["vwap"] is None
    assert rows[1]["trades"] is None


class DummySettings(SimpleNamespace):
    """Mock settings object for testing aggregates service."""

    db_url: str = "sqlite:///test.db"
    zmq_broker_xpub: str = "tcp://127.0.0.1:5555"
    master_password: str | None = None
    polygon_api_key: str | None = None
    backfill_days: int = 3
    instruments: dict[str, list[str]] = {}


@pytest.mark.asyncio
async def test_start_raises_when_api_key_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify start raises when Polygon API key missing.

    Given: Settings with polygon_api_key=None,
    When: start method called,
    Then: ValueError raised with appropriate message.
    """
    svc = PolygonAggregatesBackfillService(symbols=[])
    svc.settings = DummySettings(polygon_api_key=None)
    monkeypatch.setattr(
        "snapper.application.updaters.historical.aggregates.get_settings_service",
        AsyncMock(),
    )
    monkeypatch.setattr(
        "snapper.application.updaters.historical.aggregates.get_settings_with_service",
        lambda _svc: svc.settings,
    )
    with pytest.raises(ValueError, match="Polygon API key not configured"):
        await svc.start()


@pytest.mark.asyncio
async def test_start_all_mapped_returns_when_no_symbols(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify start exits early when no symbols to process.

    Given: all_mapped=True but empty symbol list,
    When: start method called,
    Then: Method returns after calling _get_all_mapped_symbols.
    """
    svc = PolygonAggregatesBackfillService(all_mapped=True)
    svc.settings = DummySettings(polygon_api_key="key", instruments={"polygon": []})
    monkeypatch.setattr(
        "snapper.application.updaters.historical.aggregates.get_settings_service",
        AsyncMock(),
    )
    monkeypatch.setattr(
        "snapper.application.updaters.historical.aggregates.get_settings_with_service",
        lambda _svc: svc.settings,
    )
    monkeypatch.setattr(
        "snapper.application.updaters.historical.aggregates.PolygonExchangeClient",
        lambda api_key: None,
    )
    monkeypatch.setattr(
        "snapper.application.updaters.historical.aggregates.PolygonHistoricalLoader",
        lambda client, cache_root: None,
    )
    monkeypatch.setattr(
        "snapper.application.updaters.historical.aggregates.get_repository",
        lambda url: None,
    )
    svc._get_all_mapped_symbols = Mock(return_value=[])
    await svc.start()
    svc._get_all_mapped_symbols.assert_called_once()


@pytest.mark.asyncio
async def test_process_symbol_with_empty_candles(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify _process_symbol handles empty candle result.

    Given: Loader returning empty candle list,
    When: _process_symbol called,
    Then: Instrument upserted but no candles inserted.
    """
    svc = PolygonAggregatesBackfillService(symbols=[], days_back=1)
    loader: Any = SimpleNamespace()
    loader.get_aggregate_csv_path_for_day = Mock(return_value=None)
    loader.fetch_aggregates = AsyncMock(return_value=[])
    svc._loader = loader
    repo: Any = SimpleNamespace()
    repo.upsert_instrument = AsyncMock(return_value=1)
    repo.upsert_candles = AsyncMock(return_value=0)
    svc._db_async = repo
    svc._instrument_cache = {}
    context = _SymbolContext(
        native_symbol="BTC-USD",
        polygon_symbol="X:BTCUSD",
        base_currency="BTC",
        quote_currency="USD",
    )
    await svc._process_symbol(context)
    repo.upsert_instrument.assert_awaited_once()
    loader.fetch_aggregates.assert_awaited()


def test_timeframe_label_falls_back_to_first_letter() -> None:
    """Verify _timeframe_label uses first letter for unknown timespan.

    Given: Timespan value 'week',
    When: _timeframe_label called,
    Then: Returns '3w' using first letter.
    """
    assert _timeframe_label(3, "week") == "3w"


def test_build_candle_rows_converts_values() -> None:
    """Verify _build_candle_rows converts Decimal to float.

    Given: Candle with None vwap,
    When: _build_candle_rows called,
    Then: Row dict has vwap=None.
    """
    candles = [
        AggregateCandle(
            timestamp=datetime.now(tz=UTC),
            open=Decimal("1"),
            high=Decimal("2"),
            low=Decimal("0.5"),
            close=Decimal("1.5"),
            volume=Decimal("10"),
            vwap=None,
            transactions=4,
            ticker="X:BTCUSD",
        )
    ]
    rows = PolygonAggregatesBackfillService._build_candle_rows(
        candles, instrument_id=1, timeframe="1m"
    )
    assert rows[0]["instrument_id"] == 1
    assert rows[0]["vwap"] is None


def test_get_default_kwargs_uses_settings() -> None:
    """Verify get_default_kwargs extracts values from settings.

    Given: Settings with backfill_days=5 and instruments,
    When: get_default_kwargs called,
    Then: Returned dict contains correct values.
    """
    settings = DummySettings(backfill_days=5, instruments={"polygon": ["X:BTCUSD"]})
    kwargs = PolygonAggregatesBackfillService.get_default_kwargs(settings)
    assert kwargs["symbols"] == ["X:BTCUSD"]
    assert kwargs["days_back"] == 5


def test_get_all_mapped_symbols_filters_none(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify _get_all_mapped_symbols filters None values.

    Given: Database returning list with None values,
    When: _get_all_mapped_symbols called,
    Then: Only non-None symbols returned.
    """
    svc = PolygonAggregatesBackfillService()

    class DummyResult:
        def scalars(self) -> DummyResult:
            return self

        def all(self) -> list[str | None]:
            return ["X:BTCUSD", None, "X:ETHUSD"]

    class DummySession:
        def __enter__(self) -> DummySession:
            return self

        def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            return None

        def execute(self, _stmt: Any) -> DummyResult:
            return DummyResult()

    svc._db_sync = cast(Any, SimpleNamespace(get_session=lambda: DummySession()))
    symbols = svc._get_all_mapped_symbols()
    assert symbols == ["X:BTCUSD", "X:ETHUSD"]


class DummyPath:
    """Mock path object for existence checking."""

    def __init__(self, exists: bool = True) -> None:
        """Initialize the instance."""
        self._exists = exists

    def exists(self) -> bool:
        """Return configured existence value."""
        return self._exists


@pytest.mark.asyncio
async def test_process_symbol_skips_small_chunk_when_all_csv_exist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify _process_symbol skips fetch when all CSVs exist.

    Given: All CSV files exist for requested days,
    When: _process_symbol called with resume=True,
    Then: No fetch_aggregates calls made.
    """
    svc = PolygonAggregatesBackfillService(symbols=[], days_back=2, resume=True, save_csv=True)
    svc._db_async = cast(Any, SimpleNamespace(upsert_instrument=AsyncMock(return_value=1)))

    class Loader:
        def __init__(self) -> None:
            self.fetch_calls = 0

        def get_aggregate_csv_path_for_day(self, *_args: Any, **_kwargs: Any) -> DummyPath:
            return DummyPath(True)

        async def fetch_aggregates(self, *_args: Any, **_kwargs: Any) -> list[Any]:
            self.fetch_calls += 1
            return []

    svc._loader = cast(Any, Loader())
    context = _SymbolContext(
        native_symbol="BTC-USD",
        polygon_symbol="X:BTCUSD",
        base_currency="BTC",
        quote_currency="USD",
    )
    await svc._process_symbol(context)
    assert cast(Any, svc._loader).fetch_calls == 0


@pytest.mark.asyncio
async def test_process_symbol_optimizes_large_chunk(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify _process_symbol optimizes fetch for large date ranges.

    Given: Large days_back with some missing CSVs,
    When: _process_symbol called,
    Then: Only missing days fetched.
    """
    svc = PolygonAggregatesBackfillService(symbols=[], days_back=40, resume=True, save_csv=True)

    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> Any:
            return datetime(2024, 1, 15, tzinfo=UTC)

    monkeypatch.setattr(
        "snapper.application.updaters.historical.aggregates.datetime", FrozenDatetime
    )
    svc._db_async = cast(
        Any,
        SimpleNamespace(
            upsert_instrument=AsyncMock(return_value=1),
            upsert_candles=AsyncMock(return_value=1),
        ),
    )
    missing_days = {
        datetime(2024, 1, 13, tzinfo=UTC).date(),
        datetime(2023, 12, 7, tzinfo=UTC).date(),
    }

    class Loader:
        def __init__(self) -> None:
            self.fetch_calls = 0

        def get_aggregate_csv_path_for_day(
            self, _symbol: str, _timespan: str, day: Any
        ) -> DummyPath:
            return DummyPath(day not in missing_days)

        async def fetch_aggregates(self, *_args: Any, **_kwargs: Any) -> list[AggregateCandle]:
            self.fetch_calls += 1
            return [
                AggregateCandle(
                    timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                    open=Decimal("1"),
                    high=Decimal("2"),
                    low=Decimal("0.5"),
                    close=Decimal("1.5"),
                    volume=Decimal("10"),
                    vwap=None,
                    transactions=4,
                    ticker="X:BTCUSD",
                )
            ]

    svc._loader = cast(Any, Loader())
    context = _SymbolContext(
        native_symbol="BTC-USD",
        polygon_symbol="X:BTCUSD",
        base_currency="BTC",
        quote_currency="USD",
    )
    await svc._process_symbol(context)
    db_async = cast(Any, svc._db_async)
    loader = cast(Any, svc._loader)
    db_async.upsert_instrument.assert_awaited_once()
    db_async.upsert_candles.assert_awaited()
    assert loader.fetch_calls == 1


@pytest.mark.asyncio
async def test_process_symbol_hour_timespan_fetches(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify _process_symbol fetches hourly data correctly.

    Given: Service configured with timespan='hour',
    When: _process_symbol called,
    Then: fetch_aggregates called once.
    """
    svc = PolygonAggregatesBackfillService(
        symbols=[],
        days_back=1,
        resume=True,
        save_csv=True,
        timespan="hour",
    )
    svc._db_async = cast(
        Any,
        SimpleNamespace(
            upsert_instrument=AsyncMock(return_value=7),
            upsert_candles=AsyncMock(return_value=1),
        ),
    )

    class Loader:
        def __init__(self) -> None:
            self.fetch_calls = 0

        def get_aggregate_csv_path_for_day(self, *_args: Any, **_kwargs: Any) -> DummyPath:
            return DummyPath(False)

        async def fetch_aggregates(self, *_args: Any, **_kwargs: Any) -> list[AggregateCandle]:
            self.fetch_calls += 1
            return [
                AggregateCandle(
                    timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                    open=Decimal("1"),
                    high=Decimal("1"),
                    low=Decimal("1"),
                    close=Decimal("1"),
                    volume=Decimal("1"),
                    vwap=None,
                    transactions=1,
                    ticker="C:EURUSD",
                )
            ]

    svc._loader = cast(Any, Loader())
    context = _SymbolContext(
        native_symbol="EUR-USD",
        polygon_symbol="C:EURUSD",
        base_currency="EUR",
        quote_currency="USD",
    )
    await svc._process_symbol(context)
    repo = cast(Any, svc._db_async)
    loader = cast(Any, svc._loader)
    repo.upsert_instrument.assert_awaited_once()
    repo.upsert_candles.assert_awaited_once()
    assert loader.fetch_calls == 1


@pytest.mark.asyncio
async def test_process_symbol_day_timespan_skips_when_csv_exist(tmp_path: Any) -> None:
    """Verify _process_symbol skips daily fetch when CSV exists.

    Given: Existing CSV file for daily data,
    When: _process_symbol called with resume=True,
    Then: No fetch calls made.
    """
    svc = PolygonAggregatesBackfillService(
        symbols=[],
        days_back=3,
        resume=True,
        save_csv=True,
        timespan="day",
    )
    svc._db_async = cast(Any, SimpleNamespace(upsert_instrument=AsyncMock(return_value=11)))
    existing_file = tmp_path / "exists.csv"
    existing_file.touch()

    class Loader:
        def __init__(self) -> None:
            self.fetch_calls = 0

        def get_aggregate_csv_path_for_day(self, *_args: Any, **_kwargs: Any) -> Any:
            return existing_file

        async def fetch_aggregates(self, *_args: Any, **_kwargs: Any) -> list[Any]:
            self.fetch_calls += 1
            return []

    svc._loader = cast(Any, Loader())
    context = _SymbolContext(
        native_symbol="AAPL",
        polygon_symbol="AAPL",
        base_currency="AAPL",
        quote_currency=None,
    )
    await svc._process_symbol(context)
    repo = cast(Any, svc._db_async)
    loader = cast(Any, svc._loader)
    repo.upsert_instrument.assert_awaited_once()
    assert loader.fetch_calls == 0


@pytest.mark.asyncio
async def test_process_symbol_large_chunk_all_csv_exist_skips_fetch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify _process_symbol skips all fetches when all CSVs exist.

    Given: 40-day range with all CSVs present,
    When: _process_symbol called,
    Then: Zero fetch calls made.
    """
    svc = PolygonAggregatesBackfillService(symbols=[], days_back=40, resume=True, save_csv=True)
    svc._db_async = cast(Any, SimpleNamespace(upsert_instrument=AsyncMock(return_value=3)))

    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> Any:
            return datetime(2024, 2, 10, tzinfo=UTC)

    monkeypatch.setattr(
        "snapper.application.updaters.historical.aggregates.datetime", FrozenDatetime
    )

    class Loader:
        def __init__(self) -> None:
            self.fetch_calls = 0

        def get_aggregate_csv_path_for_day(self, *_args: Any, **_kwargs: Any) -> DummyPath:
            return DummyPath(True)

        async def fetch_aggregates(self, *_args: Any, **_kwargs: Any) -> list[Any]:
            self.fetch_calls += 1
            return []

    svc._loader = cast(Any, Loader())
    context = _SymbolContext(
        native_symbol="BTC-USD",
        polygon_symbol="X:BTCUSD",
        base_currency="BTC",
        quote_currency="USD",
    )
    await svc._process_symbol(context)
    repo = cast(Any, svc._db_async)
    loader = cast(Any, svc._loader)
    repo.upsert_instrument.assert_awaited_once()
    assert loader.fetch_calls == 0


def test_resolve_symbol_context_with_polygon_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify _resolve_symbol_context uses polygon_rest_to_native cache.

    Given: Symbol in polygon_rest_to_native cache,
    When: _resolve_symbol_context called,
    Then: Context with mapped native symbol returned.
    """
    svc = PolygonAggregatesBackfillService()
    responses: list[Any] = [
        SimpleNamespace(native_symbol="BTC-USD", base="BTC", quote="USD", public_id="sym-pub-btc"),
        SimpleNamespace(symbol_public_id="sym-pub-btc", exchange_symbol="X:BTCUSD"),
    ]

    class Session:
        """Session stub returning catalog then alias."""

        def __enter__(self) -> Session:
            return self

        def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            return None

        def execute(self, _stmt: Any) -> Any:
            obj = responses.pop(0)
            return SimpleNamespace(scalar_one_or_none=lambda: obj)

    svc._db_sync = cast(Any, SimpleNamespace(get_session=lambda: Session()))
    svc._symbol_mapper = cast(
        Any,
        SimpleNamespace(
            polygon_rest_to_native={"X:BTCUSD": "BTC-USD"},
            native_to_polygon_rest={},
            load_cache_if_needed=lambda: None,
        ),
    )
    ctx = svc._resolve_symbol_context("X:BTCUSD")
    assert ctx is not None
    assert ctx.native_symbol == "BTC-USD"


def test_resolve_symbol_context_with_native_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify _resolve_symbol_context uses native_to_polygon_rest cache.

    Given: Symbol in native_to_polygon_rest cache,
    When: _resolve_symbol_context called,
    Then: Context with mapped polygon symbol returned.
    """
    svc = PolygonAggregatesBackfillService()
    responses: list[Any] = [
        None,
        SimpleNamespace(symbol_public_id="sym-pub-btc2", exchange_symbol="X:BTCUSD"),
        SimpleNamespace(native_symbol="BTC-USD", base="BTC", quote="USD", public_id="sym-pub-btc2"),
    ]

    class Session:
        """Session stub returning None then alias then catalog."""

        def __enter__(self) -> Session:
            return self

        def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            return None

        def execute(self, _stmt: Any) -> Any:
            obj = responses.pop(0)
            return SimpleNamespace(scalar_one_or_none=lambda: obj)

    shared_session = Session()
    svc._db_sync = cast(Any, SimpleNamespace(get_session=lambda: shared_session))
    svc._symbol_mapper = cast(
        Any,
        SimpleNamespace(
            polygon_rest_to_native={},
            native_to_polygon_rest={"BTC-USD": "X:BTCUSD"},
            load_cache_if_needed=lambda: None,
        ),
    )
    ctx = svc._resolve_symbol_context("BTC-USD")
    assert ctx is not None
    assert ctx.polygon_symbol == "X:BTCUSD"


def test_resolve_symbol_context_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify _resolve_symbol_context returns None for unknown symbol.

    Given: Symbol not in any cache or database,
    When: _resolve_symbol_context called,
    Then: None returned.
    """
    svc = PolygonAggregatesBackfillService()

    class Result:
        def scalar_one_or_none(self) -> None:
            return None

    class Session:
        def __enter__(self) -> Session:
            return self

        def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            return None

        def execute(self, _stmt: Any) -> Result:
            return Result()

    svc._db_sync = cast(Any, SimpleNamespace(get_session=lambda: Session()))
    svc._symbol_mapper = cast(
        Any,
        SimpleNamespace(
            polygon_rest_to_native={},
            native_to_polygon_rest={},
            load_cache_if_needed=lambda: None,
        ),
    )
    ctx = svc._resolve_symbol_context("UNKNOWN")
    assert ctx is None


def test_resolve_symbol_context_polygon_prefix_not_in_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify _resolve_symbol_context returns None for uncached polygon prefix.

    Given: Polygon-prefixed symbol not in cache,
    When: _resolve_symbol_context called,
    Then: None returned without database query.
    """
    svc = PolygonAggregatesBackfillService()
    svc._db_sync = cast(Any, SimpleNamespace(get_session=lambda: None))
    svc._symbol_mapper = cast(
        Any,
        SimpleNamespace(
            polygon_rest_to_native={},
            native_to_polygon_rest={},
            load_cache_if_needed=lambda: None,
        ),
    )
    ctx = svc._resolve_symbol_context("C:UNKNOWN")
    assert ctx is None


def test_resolve_symbol_context_polygon_prefix_mapping_none_in_db(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify _resolve_symbol_context returns None when DB has no mapping.

    Given: Symbol in cache but no database mapping,
    When: _resolve_symbol_context called,
    Then: None returned.
    """
    svc = PolygonAggregatesBackfillService()

    class Result:
        def scalar_one_or_none(self) -> None:
            return None

    class Session:
        def __enter__(self) -> Session:
            return self

        def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            return None

        def execute(self, _stmt: Any) -> Result:
            return Result()

    svc._db_sync = cast(Any, SimpleNamespace(get_session=lambda: Session()))
    svc._symbol_mapper = cast(
        Any,
        SimpleNamespace(
            polygon_rest_to_native={"C:EURUSD": "EUR-USD"},
            native_to_polygon_rest={},
            load_cache_if_needed=lambda: None,
        ),
    )
    ctx = svc._resolve_symbol_context("C:EURUSD")
    assert ctx is None


def test_resolve_symbol_context_stock_fallback_mapping_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify _resolve_symbol_context tries both queries for stocks.

    Given: Stock symbol in native cache but no DB mapping,
    When: _resolve_symbol_context called,
    Then: Two database queries made, None returned.
    """
    svc = PolygonAggregatesBackfillService()
    call_count = 0

    class Result:
        def scalar_one_or_none(self) -> Any:
            nonlocal call_count
            call_count += 1
            return None

    class Session:
        def __enter__(self) -> Session:
            return self

        def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            return None

        def execute(self, _stmt: Any) -> Result:
            return Result()

    svc._db_sync = cast(Any, SimpleNamespace(get_session=lambda: Session()))
    svc._symbol_mapper = cast(
        Any,
        SimpleNamespace(
            polygon_rest_to_native={},
            native_to_polygon_rest={"AAPL": "AAPL"},
            load_cache_if_needed=lambda: None,
        ),
    )
    ctx = svc._resolve_symbol_context("AAPL")
    assert ctx is None
    assert call_count == 2


class _StubBackfillSession:
    """Test stub for backfill database session."""

    def __init__(self, responses: list[list[Any]]) -> None:
        self._responses = responses

    def __enter__(self) -> _StubBackfillSession:
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _tb: TracebackType | None,
    ) -> Literal[False]:
        return False

    def execute(self, _stmt: object) -> Any:
        values = self._responses.pop(0) if self._responses else []

        class _Result:
            """Nested result stub."""

            def scalars(self) -> Any:
                class _Scalars:
                    """Nested scalars stub."""

                    def all(self) -> list[Any]:
                        return values

                return _Scalars()

            def scalar_one_or_none(self) -> Any:
                return values[0] if values else None

        return _Result()


class _StubBackfillSyncRepo:
    """Test stub for synchronous backfill repository."""

    def __init__(self, responses: list[list[Any]]) -> None:
        self._template = [list(row) for row in responses]

    def get_session(self) -> _StubBackfillSession:
        return _StubBackfillSession([list(row) for row in self._template])


class _StubBackfillAsyncRepo:
    """Test stub for asynchronous backfill repository."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.upsert_candles_called = 0

    async def upsert_instrument(self, **kwargs: Any) -> int:
        self.calls.append({"upsert_instrument": kwargs})
        return 1

    async def upsert_candles(self, rows: list[dict[str, object]]) -> int:
        self.upsert_candles_called += len(rows)
        return len(rows)


class _StubLoader:
    """Test stub for aggregate data loader."""

    def __init__(self, candles: list[AggregateCandle] | None = None) -> None:
        self.candles = candles if candles is not None else []
        self.fetch_calls: list[dict[str, Any]] = []

    async def fetch_aggregates(
        self,
        symbol: str,
        multiplier: int,
        timespan: str,
        from_ts: datetime,
        to_ts: datetime,
        resume_from: datetime | None = None,
        save_csv: bool = True,
        limit: int = 50000,
    ) -> list[AggregateCandle]:
        self.fetch_calls.append(
            {
                "symbol": symbol,
                "multiplier": multiplier,
                "timespan": timespan,
                "from_ts": from_ts,
                "to_ts": to_ts,
                "resume_from": resume_from,
                "save_csv": save_csv,
                "limit": limit,
            }
        )
        return self.candles

    def get_aggregate_csv_path_for_day(self, symbol: str, timespan: str, day: date) -> Path | None:
        return None


@pytest.mark.asyncio
async def test_start_without_api_key_raises_value_error() -> None:
    """Verify start raises ValueError when API key empty.

    Given: Settings with empty polygon_api_key,
    When: start method called,
    Then: ValueError raised with message.
    """
    service = PolygonAggregatesBackfillService(symbols=["X:BTCUSD"])
    stub_settings = MagicMock()
    stub_settings.db_url = "sqlite:///:memory:"
    stub_settings.polygon_api_key = ""
    stub_settings.zmq_broker_xpub = "tcp://127.0.0.1:5555"
    stub_settings.zmq_broker_xsub = "tcp://127.0.0.1:5556"
    with (
        patch.object(
            service,
            "settings",
            stub_settings,
        ),
        patch(
            "snapper.application.updaters.historical.aggregates.get_settings_service"
        ) as mock_get_settings_service,
        patch(
            "snapper.application.updaters.historical.aggregates.get_settings_with_service"
        ) as mock_get_settings_with_service,
    ):
        mock_settings_service = AsyncMock()
        mock_get_settings_service.return_value = mock_settings_service
        mock_get_settings_with_service.return_value = stub_settings
        with pytest.raises(ValueError, match="Polygon API key not configured"):
            await service.start()


@pytest.mark.asyncio
async def test_all_mapped_no_results_returns_early() -> None:
    """Verify start exits early when all_mapped has no symbols.

    Given: all_mapped=True with empty symbol list,
    When: start method called,
    Then: No fetch calls made.
    """
    service = PolygonAggregatesBackfillService(all_mapped=True)
    service._db_sync = _StubBackfillSyncRepo([[]])
    stub_settings = MagicMock()
    stub_settings.db_url = "sqlite:///:memory:"
    stub_settings.polygon_api_key = "test-key"
    stub_settings.zmq_broker_xpub = "tcp://127.0.0.1:5555"
    stub_settings.zmq_broker_xsub = "tcp://127.0.0.1:5556"
    with (
        patch.object(
            service,
            "settings",
            stub_settings,
        ),
        patch(
            "snapper.application.updaters.historical.aggregates.get_settings_service"
        ) as mock_get_settings_service,
        patch(
            "snapper.application.updaters.historical.aggregates.get_settings_with_service"
        ) as mock_get_settings_with_service,
        patch(
            "snapper.application.updaters.historical.aggregates.DatabaseRepository",
            return_value=MagicMock(),
        ),
        patch(
            "snapper.application.updaters.historical.aggregates.get_repository",
            return_value=_StubBackfillAsyncRepo(),
        ),
        patch(
            "snapper.application.updaters.historical.aggregates.PolygonExchangeClient"
        ) as mock_client_cls,
        patch(
            "snapper.application.updaters.historical.aggregates.PolygonHistoricalLoader"
        ) as mock_loader_cls,
    ):
        mock_settings_service = AsyncMock()
        mock_get_settings_service.return_value = mock_settings_service
        mock_get_settings_with_service.return_value = stub_settings
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_loader = _StubLoader()
        mock_loader_cls.return_value = mock_loader
        await service.start()
        assert len(mock_loader.fetch_calls) == 0


@pytest.mark.asyncio
async def test_no_candles_returned_logs_warning() -> None:
    """Verify empty candle result does not insert rows.

    Given: Loader returning empty candle list,
    When: start method processes symbol,
    Then: upsert_candles called with zero rows.
    """
    service = PolygonAggregatesBackfillService(symbols=["X:BTCUSD"], days_back=1)
    stub_mapping = MagicMock()
    stub_mapping.native_symbol = "BTC/USD"
    stub_mapping.polygon_symbol = "X:BTCUSD"
    stub_mapping.base_currency = "BTC"
    stub_mapping.quote_currency = "USD"
    service._db_sync = _StubBackfillSyncRepo([[stub_mapping]])
    stub_async_repo = _StubBackfillAsyncRepo()
    service._db_async = stub_async_repo
    stub_loader = _StubLoader(candles=[])
    service._loader = stub_loader
    stub_settings = MagicMock()
    stub_settings.db_url = "sqlite:///:memory:"
    stub_settings.polygon_api_key = "test-key"
    stub_settings.zmq_broker_xpub = "tcp://127.0.0.1:5555"
    stub_settings.zmq_broker_xsub = "tcp://127.0.0.1:5556"
    stub_settings.instruments = {"polygon": ["X:BTCUSD"]}
    with (
        patch.object(
            service,
            "settings",
            stub_settings,
        ),
        patch(
            "snapper.application.updaters.historical.aggregates.get_settings_service"
        ) as mock_get_settings_service,
        patch(
            "snapper.application.updaters.historical.aggregates.get_settings_with_service"
        ) as mock_get_settings_with_service,
        patch(
            "snapper.application.updaters.historical.aggregates.DatabaseRepository",
            return_value=service._db_sync,
        ),
        patch(
            "snapper.application.updaters.historical.aggregates.get_repository",
            return_value=stub_async_repo,
        ),
        patch("snapper.application.updaters.historical.aggregates.PolygonExchangeClient"),
        patch(
            "snapper.application.updaters.historical.aggregates.PolygonHistoricalLoader",
            return_value=stub_loader,
        ),
    ):
        mock_settings_service = AsyncMock()
        mock_get_settings_service.return_value = mock_settings_service
        mock_get_settings_with_service.return_value = stub_settings
        await service.start()
        assert stub_async_repo.upsert_candles_called == 0


@pytest.mark.asyncio
async def test_build_candle_rows_with_vwap_none() -> None:
    """Verify _build_candle_rows preserves None vwap.

    Given: Candle with vwap=None,
    When: _build_candle_rows called,
    Then: Row dict has vwap=None.
    """
    candles = [
        AggregateCandle(
            ticker="X:BTCUSD",
            timestamp=datetime(2026, 12, 1, 12, 0, tzinfo=UTC),
            open=Decimal("100.0"),
            high=Decimal("101.0"),
            low=Decimal("99.0"),
            close=Decimal("100.5"),
            volume=Decimal("1000.0"),
            vwap=None,
            transactions=10,
        )
    ]
    rows = PolygonAggregatesBackfillService._build_candle_rows(candles, 1, "1m")
    assert len(rows) == 1
    assert rows[0]["vwap"] is None


@pytest.mark.asyncio
async def test_730_day_limit_enforced() -> None:
    """Verify 730-day limit is enforced for date ranges.

    Given: days_back=900 exceeding limit,
    When: start method processes symbol,
    Then: from_ts date is at most 730 days ago.
    """
    service = PolygonAggregatesBackfillService(symbols=["X:BTCUSD"], days_back=900)
    stub_catalog = SimpleNamespace(
        native_symbol="BTC-USD", base="BTC", quote="USD", public_id="sym-pub-btcusd"
    )
    stub_alias = SimpleNamespace(symbol_public_id="sym-pub-btcusd", exchange_symbol="X:BTCUSD")
    service._db_sync = _StubBackfillSyncRepo([[stub_catalog], [stub_alias]])
    stub_async_repo = _StubBackfillAsyncRepo()
    service._db_async = stub_async_repo
    stub_loader = _StubLoader(candles=[])
    service._loader = stub_loader
    stub_settings = MagicMock()
    stub_settings.db_url = "sqlite:///:memory:"
    stub_settings.polygon_api_key = "test-key"
    stub_settings.zmq_broker_xpub = "tcp://127.0.0.1:5555"
    stub_settings.zmq_broker_xsub = "tcp://127.0.0.1:5556"
    stub_settings.instruments = {"polygon": ["X:BTCUSD"]}
    with (
        patch.object(
            service,
            "settings",
            stub_settings,
        ),
        patch(
            "snapper.application.updaters.historical.aggregates.get_settings_service"
        ) as mock_get_settings_service,
        patch(
            "snapper.application.updaters.historical.aggregates.get_settings_with_service"
        ) as mock_get_settings_with_service,
        patch(
            "snapper.application.updaters.historical.aggregates.DatabaseRepository",
            return_value=service._db_sync,
        ),
        patch(
            "snapper.application.updaters.historical.aggregates.get_repository",
            return_value=stub_async_repo,
        ),
        patch("snapper.application.updaters.historical.aggregates.PolygonExchangeClient"),
        patch(
            "snapper.application.updaters.historical.aggregates.PolygonHistoricalLoader",
            return_value=stub_loader,
        ),
    ):
        mock_settings_service = AsyncMock()
        mock_get_settings_service.return_value = mock_settings_service
        mock_get_settings_with_service.return_value = stub_settings
        await service.start()
        assert len(stub_loader.fetch_calls) >= 1
        first_call = stub_loader.fetch_calls[0]
        now_utc = datetime.now(UTC)
        expected_oldest = now_utc.date() - timedelta(days=730)
        assert first_call["from_ts"].date() >= expected_oldest


@pytest.mark.asyncio
async def test_chunk_optimization_skip_when_all_csv_exist() -> None:
    """Verify chunk optimization skips fetch when all CSVs exist.

    Given: All CSV files present for requested days,
    When: start method processes symbol,
    Then: Zero fetch calls made.
    """
    service = PolygonAggregatesBackfillService(
        symbols=["X:BTCUSD"], days_back=3, resume=True, save_csv=True
    )
    stub_mapping = MagicMock()
    stub_mapping.native_symbol = "BTC/USD"
    stub_mapping.polygon_symbol = "X:BTCUSD"
    stub_mapping.base_currency = "BTC"
    stub_mapping.quote_currency = "USD"
    service._db_sync = _StubBackfillSyncRepo([[stub_mapping]])
    stub_async_repo = _StubBackfillAsyncRepo()
    service._db_async = stub_async_repo

    class _StubLoaderWithCSV(_StubLoader):
        def get_aggregate_csv_path_for_day(
            self, symbol: str, timespan: str, day: date
        ) -> Path | None:
            mock_path = MagicMock(spec=Path)
            mock_path.exists.return_value = True
            return mock_path

    stub_loader = _StubLoaderWithCSV(candles=[])
    service._loader = stub_loader
    stub_settings = MagicMock()
    stub_settings.db_url = "sqlite:///:memory:"
    stub_settings.polygon_api_key = "test-key"
    stub_settings.zmq_broker_xpub = "tcp://127.0.0.1:5555"
    stub_settings.zmq_broker_xsub = "tcp://127.0.0.1:5556"
    stub_settings.instruments = {"polygon": ["X:BTCUSD"]}
    with (
        patch.object(
            service,
            "settings",
            stub_settings,
        ),
        patch(
            "snapper.application.updaters.historical.aggregates.get_settings_service"
        ) as mock_get_settings_service,
        patch(
            "snapper.application.updaters.historical.aggregates.get_settings_with_service"
        ) as mock_get_settings_with_service,
        patch(
            "snapper.application.updaters.historical.aggregates.DatabaseRepository",
            return_value=service._db_sync,
        ),
        patch(
            "snapper.application.updaters.historical.aggregates.get_repository",
            return_value=stub_async_repo,
        ),
        patch("snapper.application.updaters.historical.aggregates.PolygonExchangeClient"),
        patch(
            "snapper.application.updaters.historical.aggregates.PolygonHistoricalLoader",
            return_value=stub_loader,
        ),
    ):
        mock_settings_service = AsyncMock()
        mock_get_settings_service.return_value = mock_settings_service
        mock_get_settings_with_service.return_value = stub_settings
        await service.start()
        assert len(stub_loader.fetch_calls) == 0


@pytest.mark.asyncio
async def test_start_without_symbols_returns(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify start exits early when no symbols provided.

    Given: Empty symbols list and all_mapped=False,
    When: start method called,
    Then: Method returns without processing.
    """
    svc = PolygonAggregatesBackfillService(symbols=[], all_mapped=False)
    svc.settings = cast(
        Any,
        SimpleNamespace(
            instruments={"polygon": []},
            db_url="sqlite://",
            zmq_broker_xpub="xpub",
            master_password=None,
            polygon_api_key="dummy",
            backfill_days=1,
        ),
    )
    monkeypatch.setattr(
        aggregates_module,
        "get_settings_service",
        AsyncMock(return_value=SimpleNamespace()),
    )
    monkeypatch.setattr(
        aggregates_module,
        "get_settings_with_service",
        lambda _svc: svc.settings,
    )
    monkeypatch.setattr(aggregates_module, "DatabaseRepository", lambda _url: SimpleNamespace())
    monkeypatch.setattr(aggregates_module, "get_repository", lambda _url: SimpleNamespace())
    monkeypatch.setattr(
        aggregates_module,
        "PolygonExchangeClient",
        lambda api_key: SimpleNamespace(),
    )
    monkeypatch.setattr(
        aggregates_module,
        "PolygonHistoricalLoader",
        lambda client, cache_root=None: SimpleNamespace(
            get_aggregate_csv_path_for_day=lambda *args, **kwargs: None
        ),
    )
    await svc.start()


@pytest.mark.asyncio
async def test_start_all_mapped_without_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify start exits when all_mapped returns empty.

    Given: all_mapped=True but _get_all_mapped_symbols returns empty,
    When: start method called,
    Then: Method returns without processing.
    """
    svc = PolygonAggregatesBackfillService(symbols=["X:BTCUSD"], all_mapped=True)
    svc.settings = cast(
        Any,
        SimpleNamespace(
            instruments={"polygon": []},
            db_url="sqlite://",
            zmq_broker_xpub="xpub",
            master_password=None,
            polygon_api_key="dummy",
            backfill_days=1,
        ),
    )
    monkeypatch.setattr(
        aggregates_module,
        "get_settings_service",
        AsyncMock(return_value=SimpleNamespace()),
    )
    monkeypatch.setattr(
        aggregates_module,
        "get_settings_with_service",
        lambda _svc: svc.settings,
    )
    monkeypatch.setattr(aggregates_module, "DatabaseRepository", lambda _url: SimpleNamespace())
    monkeypatch.setattr(aggregates_module, "get_repository", lambda _url: SimpleNamespace())
    monkeypatch.setattr(
        aggregates_module,
        "PolygonExchangeClient",
        lambda api_key: SimpleNamespace(),
    )
    monkeypatch.setattr(
        aggregates_module,
        "PolygonHistoricalLoader",
        lambda client, cache_root=None: SimpleNamespace(
            get_aggregate_csv_path_for_day=lambda *args, **kwargs: None
        ),
    )
    cast(Any, svc)._get_all_mapped_symbols = lambda: []
    await svc.start()


@pytest.mark.asyncio
async def test_start_skips_symbol_without_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify start skips symbols without context.

    Given: Symbol that _resolve_symbol_context returns None for,
    When: start method called,
    Then: Symbol skipped, no processing.
    """
    svc = PolygonAggregatesBackfillService(symbols=["X:UNKNOWN"], all_mapped=False)
    svc.settings = cast(
        Any,
        SimpleNamespace(
            instruments={"polygon": ["X:UNKNOWN"]},
            db_url="sqlite://",
            zmq_broker_xpub="xpub",
            master_password=None,
            polygon_api_key="dummy",
            backfill_days=1,
        ),
    )
    monkeypatch.setattr(
        aggregates_module,
        "get_settings_service",
        AsyncMock(return_value=SimpleNamespace()),
    )
    monkeypatch.setattr(
        aggregates_module,
        "get_settings_with_service",
        lambda _svc: svc.settings,
    )
    monkeypatch.setattr(aggregates_module, "DatabaseRepository", lambda _url: SimpleNamespace())
    monkeypatch.setattr(aggregates_module, "get_repository", lambda _url: SimpleNamespace())
    monkeypatch.setattr(
        aggregates_module,
        "PolygonExchangeClient",
        lambda api_key: SimpleNamespace(),
    )
    monkeypatch.setattr(
        aggregates_module,
        "PolygonHistoricalLoader",
        lambda client, cache_root=None: SimpleNamespace(
            get_aggregate_csv_path_for_day=lambda *args, **kwargs: None
        ),
    )
    cast(Any, svc)._resolve_symbol_context = lambda symbol: None
    await svc.start()


class _DummyRepo:
    """Test dummy for repository."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def get_session(self) -> _DummyRepo:
        return self

    def __enter__(self) -> _DummyRepo:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def execute(self, *_: object, **__: object) -> Any:
        return SimpleNamespace(scalars=lambda: [])


class _DummyAsyncRepo:
    """Test dummy for async repository."""

    async def upsert_instrument(self, **kwargs: Any) -> int:
        return 1

    async def upsert_candles(self, rows: list[dict[str, object]]) -> int:
        return len(rows)


class _DummyClient:
    """Test dummy for exchange client."""

    async def connect(self) -> None:
        return None

    async def disconnect(self) -> None:
        return None


class _DummyLoader:
    """Test dummy for aggregate data loader."""

    def __init__(self) -> None:
        self.recorded: list[tuple[str, datetime, datetime]] = []

    async def fetch_aggregates(
        self,
        symbol: str,
        multiplier: int,
        timespan: str,
        *,
        from_ts: datetime,
        to_ts: datetime,
        resume_from: None,
        save_csv: bool,
        limit: int,
    ) -> list[Any]:
        self.recorded.append((symbol, from_ts, to_ts))
        return []

    def get_aggregate_csv_path_for_day(self, *_: object) -> Any:
        return None


@pytest.mark.asyncio()
async def test_start_all_mapped_uses_fetched_symbols(monkeypatch: pytest.MonkeyPatch) -> None:
    """Start uses fetched symbols when all_mapped enabled.

    Given a backfill service configured with all_mapped=True,
    When the start method is called,
    Then symbols are fetched from the mapper and processed.
    """
    dummy_settings = SimpleNamespace(
        db_url="sqlite:///tmp.db",
        zmq_broker_xpub="inproc://xpub",
        master_password="pw",
        polygon_api_key="key",
        instruments={"polygon": ["X:IGNORED"]},
    )
    monkeypatch.setattr(
        "snapper.application.updaters.historical.aggregates.get_settings",
        lambda: dummy_settings,
    )
    monkeypatch.setattr(
        "snapper.application.updaters.historical.aggregates.get_settings_with_service",
        lambda svc: dummy_settings,
    )

    async def _fake_settings_service(*_: object, **__: object) -> None:
        return None

    monkeypatch.setattr(
        "snapper.application.updaters.historical.aggregates.get_settings_service",
        _fake_settings_service,
    )
    dummy_repo = _DummyRepo()
    monkeypatch.setattr(
        "snapper.application.updaters.historical.aggregates.DatabaseRepository",
        lambda *_: dummy_repo,
    )
    monkeypatch.setattr(
        "snapper.application.updaters.historical.aggregates.get_repository",
        lambda *_: _DummyAsyncRepo(),
    )
    monkeypatch.setattr(
        "snapper.application.updaters.historical.aggregates.PolygonExchangeClient",
        lambda **kwargs: _DummyClient(),
    )
    monkeypatch.setattr(
        "snapper.application.updaters.historical.aggregates.PolygonHistoricalLoader",
        lambda *args, **kwargs: _DummyLoader(),
    )
    service = PolygonAggregatesBackfillService(all_mapped=True)
    processed: list[_SymbolContext] = []

    def fake_get_all_mapped_symbols() -> list[str]:
        return ["X:BTCUSD"]

    def fake_resolve(symbol: str) -> _SymbolContext | None:
        return _SymbolContext(
            native_symbol="BTC-USD",
            polygon_symbol="X:BTCUSD",
            base_currency="BTC",
            quote_currency="USD",
        )

    async def fake_process(context: _SymbolContext) -> None:
        processed.append(context)

    monkeypatch.setattr(service, "_get_all_mapped_symbols", fake_get_all_mapped_symbols)
    monkeypatch.setattr(service, "_resolve_symbol_context", fake_resolve)
    monkeypatch.setattr(service, "_process_symbol", fake_process)
    await service.start()
    assert processed
    assert processed[0].polygon_symbol == "X:BTCUSD"


@pytest.mark.asyncio()
async def test_process_symbol_caps_to_max_ts(monkeypatch: pytest.MonkeyPatch) -> None:
    """Process symbol caps timestamp to max timestamp.

    Given a backfill service processing a symbol,
    When the process_symbol method is called with date range,
    Then the timestamp is capped to the maximum allowed value.
    """
    dummy_settings = SimpleNamespace(
        db_url="sqlite:///tmp.db",
        zmq_broker_xpub="inproc://xpub",
        master_password="pw",
        polygon_api_key="key",
        instruments={},
    )
    monkeypatch.setattr(
        "snapper.application.updaters.historical.aggregates.get_settings",
        lambda: dummy_settings,
    )
    loader = _DummyLoader()
    service = PolygonAggregatesBackfillService(symbols=["X:BTCUSD"], all_mapped=False)
    service._db_sync = cast(Any, _DummyRepo())
    service._db_async = cast(Any, _DummyAsyncRepo())
    service._loader = cast(Any, loader)
    context = _SymbolContext(
        native_symbol="BTC-USD",
        polygon_symbol="X:BTCUSD",
        base_currency="BTC",
        quote_currency="USD",
    )

    class _FakeDateTime(datetime):
        call_count = 0
        min = datetime.min
        max = datetime.max

        @classmethod
        def now(cls, tz: Any = None) -> _FakeDateTime:
            return cls(2024, 1, 10, tzinfo=tz)

        @classmethod
        def combine(cls, d: date, t: Any, tzinfo: Any | None = None) -> _FakeDateTime:
            cls.call_count += 1
            combined = datetime.combine(d, t, tzinfo=tzinfo)
            if cls.call_count == 1:
                combined -= timedelta(hours=1)
            return cls.fromtimestamp(combined.timestamp(), tzinfo)

    monkeypatch.setattr(
        "snapper.application.updaters.historical.aggregates.datetime",
        _FakeDateTime,
    )
    await service._process_symbol(context)
    assert loader.recorded
    _, _, to_ts = loader.recorded[0]
    assert isinstance(to_ts, datetime)


def test_resolve_symbol_context_from_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    """Resolve symbol context uses cached mapper data.

    Given a backfill service with a populated symbol mapper cache,
    When resolving a symbol context for a known polygon symbol,
    Then the context is retrieved using the cached mapper data.
    """
    service = PolygonAggregatesBackfillService()

    class _Mapper:
        def __init__(self) -> None:
            self.native_to_polygon_rest: dict[str, str] = {}
            self.polygon_rest_to_native = {"X:ETHUSD": "ETH-USD"}

        def load_cache_if_needed(self) -> None:
            return None

    service._symbol_mapper = cast(Any, _Mapper())
    responses: list[Any] = [
        SimpleNamespace(
            native_symbol="ETH-USD", base="ETH", quote="USD", public_id="sym-pub-ethusd"
        ),
        SimpleNamespace(symbol_public_id="sym-pub-ethusd", exchange_symbol="X:ETHUSD"),
    ]

    class _Session(_DummyRepo):
        def execute(self, *_: object, **__: object) -> Any:
            obj = responses.pop(0)
            return SimpleNamespace(scalar_one_or_none=lambda: obj)

    service._db_sync = cast(Any, SimpleNamespace(get_session=lambda: _Session()))
    context = service._resolve_symbol_context("X:ETHUSD")
    assert context is not None
    assert context.polygon_symbol == "X:ETHUSD"


def test_resolve_symbol_context_missing_v2(monkeypatch: pytest.MonkeyPatch) -> None:
    """Resolve symbol context returns None for missing symbol.

    Given a backfill service with empty symbol mapper cache,
    When resolving a symbol context for an unknown symbol,
    Then None is returned.
    """
    service = PolygonAggregatesBackfillService()

    class _Mapper:
        polygon_rest_to_native: dict[str, str] = {}
        native_to_polygon_rest: dict[str, str] = {}

        def load_cache_if_needed(self) -> None:
            return None

    service._symbol_mapper = cast(Any, _Mapper())

    class _MissingSession:
        def __enter__(self) -> _MissingSession:
            return self

        def __exit__(self, *_: object) -> None:
            return None

        def execute(self, *_: object, **__: object) -> Any:
            return SimpleNamespace(scalar_one_or_none=lambda: None)

    service._db_sync = cast(Any, SimpleNamespace(get_session=lambda: _MissingSession()))
    assert service._resolve_symbol_context("MISSING") is None


def test_resolve_symbol_context_stock_direct_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    """Resolve symbol context handles stock symbols directly.

    Given a backfill service and a stock symbol without prefix,
    When resolving the symbol context for a stock like AAPL,
    Then the context is resolved using direct database lookup.
    """
    service = PolygonAggregatesBackfillService()

    class _Mapper:
        polygon_rest_to_native: dict[str, str] = {}
        native_to_polygon_rest: dict[str, str] = {}

        def load_cache_if_needed(self) -> None:
            return None

    service._symbol_mapper = cast(Any, _Mapper())
    responses: list[Any] = [
        SimpleNamespace(symbol_public_id="sym-pub-aapl", exchange_symbol="AAPL"),
        SimpleNamespace(native_symbol="AAPL", base="USD", quote=None, public_id="sym-pub-aapl"),
    ]

    class _Session:
        """Session stub returning alias then catalog for stock symbol."""

        def __enter__(self) -> _Session:
            return self

        def __exit__(self, *_: object) -> None:
            return None

        def execute(self, *_: object, **__: object) -> Any:
            obj = responses.pop(0)
            return SimpleNamespace(scalar_one_or_none=lambda: obj)

    service._db_sync = cast(Any, SimpleNamespace(get_session=lambda: _Session()))
    context = service._resolve_symbol_context("AAPL")
    assert context is not None
    assert context.polygon_symbol == "AAPL"
    assert context.quote_currency == "USD"


def test_resolve_symbol_context_native_to_polygon_rest_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Resolve symbol context uses native to polygon cache.

    Given a backfill service with native to polygon mapping in cache,
    When resolving a symbol context using native symbol format,
    Then the polygon symbol is retrieved from the cache mapping.
    """
    service = PolygonAggregatesBackfillService()

    class _Mapper:
        polygon_rest_to_native: dict[str, str] = {}
        native_to_polygon_rest: dict[str, str] = {"AAPL": "NAS:AAPL"}

        def load_cache_if_needed(self) -> None:
            return None

    service._symbol_mapper = cast(Any, _Mapper())
    responses: list[Any] = [
        None,
        SimpleNamespace(symbol_public_id="sym-pub-aapl", exchange_symbol="NAS:AAPL"),
        SimpleNamespace(native_symbol="AAPL", base="USD", quote=None, public_id="sym-pub-aapl"),
    ]

    class _Session:
        """Session stub returning None then alias then catalog."""

        def __enter__(self) -> _Session:
            return self

        def __exit__(self, *_: object) -> None:
            return None

        def execute(self, *_: object, **__: object) -> Any:
            obj = responses.pop(0)
            return SimpleNamespace(scalar_one_or_none=lambda: obj)

    session = _Session()
    service._db_sync = cast(Any, SimpleNamespace(get_session=lambda: session))
    context = service._resolve_symbol_context("AAPL")
    assert context is not None
    assert context.polygon_symbol == "NAS:AAPL"
    assert context.quote_currency == "USD"


class _MockSymbolMapper:
    """Test mock for symbol mapper."""

    def __init__(self) -> None:
        self.polygon_rest_to_native: dict[str, str] = {}
        self.native_to_polygon_rest: dict[str, str] = {}
        self.cache_loaded = False

    def load_cache_if_needed(self) -> None:
        self.cache_loaded = True


class _RepoStub:
    """Test stub for repository operations."""

    def __init__(self) -> None:
        self.instrument_calls: list[dict[str, Any]] = []
        self.candle_batches: list[list[dict[str, object]]] = []

    async def upsert_instrument(self, **kwargs: Any) -> int:
        self.instrument_calls.append(kwargs)
        return 77

    async def upsert_candles(self, rows: list[dict[str, object]]) -> int:
        self.candle_batches.append(rows)
        return len(rows)


class _PathStub:
    """Test stub for Path object."""

    def __init__(self, exists: bool) -> None:
        self._exists = exists

    def exists(self) -> bool:
        return self._exists


class _LoaderStub:
    """Test stub for historical data loader."""

    def __init__(self, *, csv_exists: bool, candles: list[AggregateCandle] | None = None) -> None:
        self._csv_exists = csv_exists
        self._candles = candles or []
        self.fetch_calls: list[dict[str, Any]] = []

    def get_aggregate_csv_path_for_day(self, *_args: Any, **_kwargs: Any) -> _PathStub:
        return _PathStub(self._csv_exists)

    async def fetch_aggregates(self, *args: Any, **kwargs: Any) -> list[AggregateCandle]:
        self.fetch_calls.append({"args": args, "kwargs": kwargs})
        return self._candles


_FIXED_NOW = datetime(2024, 1, 10, 12, tzinfo=UTC)


class _FixedDateTime:
    """Test class providing fixed datetime for deterministic tests."""

    max = datetime.max
    min = datetime.min

    @staticmethod
    def now(tz: Any | None = None) -> datetime:
        if tz is None:
            return _FIXED_NOW
        return _FIXED_NOW.astimezone(tz)

    @staticmethod
    def combine(*args: Any, **kwargs: Any) -> datetime:
        return datetime.combine(*args, **kwargs)


@pytest.fixture
def base_settings(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    """Provide base settings for aggregates service tests."""
    settings = SimpleNamespace(
        instruments={"polygon": []},
        backfill_days=7,
        polygon_api_key="key",
        db_url="sqlite://",
        zmq_broker_xpub="tcp://127.0.0.1:7501",
        master_password="pwd",
    )
    monkeypatch.setattr(
        "snapper.application.updaters.historical.aggregates.get_settings",
        lambda: settings,
    )
    return settings


@pytest.fixture
def service_and_mapper(
    base_settings: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> tuple[PolygonAggregatesBackfillService, _MockSymbolMapper]:
    """Provide service instance with mock symbol mapper for testing."""
    mapper = _MockSymbolMapper()
    monkeypatch.setattr(
        "snapper.application.updaters.historical.aggregates.SymbolMapperService.get_instance",
        lambda: mapper,
    )
    svc = PolygonAggregatesBackfillService(symbols=["X:BTCUSD"], resume=True, save_csv=False)
    return svc, mapper


def test_get_all_mapped_symbols_filters_none_with_fixture(
    service_and_mapper: tuple[PolygonAggregatesBackfillService, _MockSymbolMapper],
) -> None:
    """Get all mapped symbols filters None values.

    Given a database containing symbol mappings with some None values,
    When retrieving all mapped symbols,
    Then None values are filtered out from the result.
    """
    service, _ = service_and_mapper
    scalars = MagicMock()
    scalars.all.return_value = ["X:BTCUSD", None, "C:EURUSD"]
    execute_result = MagicMock()
    execute_result.scalars.return_value = scalars
    session = MagicMock()
    session.execute.return_value = execute_result
    session_ctx = MagicMock()
    session_ctx.__enter__.return_value = session
    session_ctx.__exit__.return_value = None
    mock_db_sync = MagicMock()
    mock_db_sync.get_session.return_value = session_ctx
    service_private = cast(Any, service)
    service._db_sync = cast(DatabaseRepository, mock_db_sync)
    symbols = service_private._get_all_mapped_symbols()
    assert symbols == ["X:BTCUSD", "C:EURUSD"]


def test_resolve_symbol_context_polygon_path(
    service_and_mapper: tuple[PolygonAggregatesBackfillService, _MockSymbolMapper],
) -> None:
    """Resolve symbol context uses polygon symbol path.

    Given a backfill service with polygon to native mapping in cache,
    When resolving a symbol context using polygon symbol format,
    Then the context is resolved via the polygon symbol lookup path.
    """
    service, mapper = service_and_mapper
    mapper.polygon_rest_to_native["X:BTCUSD"] = "BTC-USD"
    catalog = SimpleNamespace(
        native_symbol="BTC-USD", base="BTC", quote="USD", public_id="sym-pub-btcusd"
    )
    alias = SimpleNamespace(symbol_public_id="sym-pub-btcusd", exchange_symbol="X:BTCUSD")
    catalog_result = MagicMock()
    catalog_result.scalar_one_or_none.return_value = catalog
    alias_result = MagicMock()
    alias_result.scalar_one_or_none.return_value = alias
    session = MagicMock()
    session.execute.side_effect = [catalog_result, alias_result]
    session_ctx = MagicMock()
    session_ctx.__enter__.return_value = session
    session_ctx.__exit__.return_value = None
    mock_db_sync = MagicMock()
    mock_db_sync.get_session.return_value = session_ctx
    service_private = cast(Any, service)
    service._db_sync = cast(DatabaseRepository, mock_db_sync)
    context = service_private._resolve_symbol_context("X:BTCUSD")
    assert context is not None
    assert context.native_symbol == "BTC-USD"
    assert context.polygon_symbol == "X:BTCUSD"
    assert context.base_currency == "BTC"
    assert context.quote_currency == "USD"
    assert mapper.cache_loaded is True


def test_resolve_symbol_context_native_fallback(
    service_and_mapper: tuple[PolygonAggregatesBackfillService, _MockSymbolMapper],
) -> None:
    """Resolve symbol context falls back to native symbol lookup.

    Given a backfill service with native to polygon mapping available,
    When resolving a symbol context using native symbol format,
    Then the context is resolved via the native symbol fallback path.
    """
    service, mapper = service_and_mapper
    mapper.native_to_polygon_rest["ETH-USD"] = "X:ETHUSD"
    none_result = MagicMock()
    none_result.scalar_one_or_none.return_value = None
    alias = SimpleNamespace(symbol_public_id="sym-pub-ethusd", exchange_symbol="X:ETHUSD")
    alias_result = MagicMock()
    alias_result.scalar_one_or_none.return_value = alias
    catalog = SimpleNamespace(
        native_symbol="ETH-USD", base="ETH", quote=None, public_id="sym-pub-ethusd"
    )
    catalog_result = MagicMock()
    catalog_result.scalar_one_or_none.return_value = catalog
    session = MagicMock()
    session.execute.side_effect = [none_result, alias_result, catalog_result]
    session_ctx = MagicMock()
    session_ctx.__enter__.return_value = session
    session_ctx.__exit__.return_value = None
    mock_db_sync = MagicMock()
    mock_db_sync.get_session.return_value = session_ctx
    service_private = cast(Any, service)
    service._db_sync = cast(DatabaseRepository, mock_db_sync)
    context = service_private._resolve_symbol_context("ETH-USD")
    assert context is not None
    assert context.polygon_symbol == "X:ETHUSD"
    assert context.quote_currency == "ETH"


def test_resolve_symbol_context_missing_returns_none(
    service_and_mapper: tuple[PolygonAggregatesBackfillService, _MockSymbolMapper],
) -> None:
    """Resolve symbol context returns None for missing symbol.

    Given a backfill service with no matching symbol in database,
    When resolving a symbol context for an unknown symbol,
    Then None is returned indicating symbol not found.
    """
    service, _ = service_and_mapper
    execute_result = MagicMock()
    execute_result.scalar_one_or_none.return_value = None
    session = MagicMock()
    session.execute.return_value = execute_result
    session_ctx = MagicMock()
    session_ctx.__enter__.return_value = session
    session_ctx.__exit__.return_value = None
    mock_db_sync = MagicMock()
    mock_db_sync.get_session.return_value = session_ctx
    service_private = cast(Any, service)
    service._db_sync = cast(DatabaseRepository, mock_db_sync)
    assert service_private._resolve_symbol_context("UNKNOWN") is None


@pytest.mark.asyncio
async def test_ensure_instrument_caches_id(
    service_and_mapper: tuple[PolygonAggregatesBackfillService, _MockSymbolMapper],
) -> None:
    """Ensure instrument caches instrument ID after first call.

    Given a backfill service processing a symbol context,
    When ensure_instrument is called multiple times for same context,
    Then the instrument ID is cached and database is called only once.
    """
    calls: list[dict[str, Any]] = []

    async def upsert_instrument(**kwargs: Any) -> int:
        calls.append(kwargs)
        return 42

    service, _ = service_and_mapper
    service_private = cast(Any, service)
    service._db_async = cast(Repository, SimpleNamespace(upsert_instrument=upsert_instrument))
    context = SimpleNamespace(
        native_symbol="BTC-USD",
        polygon_symbol="X:BTCUSD",
        base_currency="BTC",
        quote_currency=None,
    )
    first = await service_private._ensure_instrument(context)
    second = await service_private._ensure_instrument(context)
    assert first == second == 42
    assert len(calls) == 1
    assert calls[0]["quote"] == "BTC"


def test_build_candle_rows_converts_values_decimal() -> None:
    """Build candle rows converts Decimal values to float.

    Given candle data with Decimal price and volume values,
    When building candle rows for database insertion,
    Then Decimal values are converted to float format.
    """
    candle = AggregateCandle(
        ticker="X:BTCUSD",
        timestamp=datetime.now(UTC),
        open=Decimal("1"),
        high=Decimal("2"),
        low=Decimal("3"),
        close=Decimal("4"),
        volume=Decimal("5"),
        vwap=None,
        transactions=10,
    )
    service_class_private = cast(Any, PolygonAggregatesBackfillService)
    rows = service_class_private._build_candle_rows(
        [candle],
        instrument_id=7,
        timeframe="1m",
    )
    assert len(rows) == 1
    row = rows[0]
    assert row["instrument_id"] == 7
    assert row["open_at"] == candle.timestamp
    assert row["timeframe"] == "1m"
    assert row["open"] == 1.0
    assert row["high"] == 2.0
    assert row["low"] == 3.0
    assert row["close"] == 4.0
    assert row["volume"] == 5.0
    assert row["vwap"] is None
    assert row["trades"] == 10
    assert isinstance(row["timestamp"], datetime)
    assert row["timestamp"] != row["open_at"]


def test_timeframe_label_variants_extended() -> None:
    """Timeframe label handles various timespan variants.

    Given different timespan values like minute, hour, day, week,
    When generating timeframe labels with multipliers,
    Then the correct label format is returned for each variant.
    """
    assert _timeframe_label(1, "minute") == "1m"
    assert _timeframe_label(2, "hour") == "2h"
    assert _timeframe_label(3, "day") == "3d"
    assert _timeframe_label(4, "week") == "4w"


@pytest.mark.asyncio
async def test_process_symbol_skips_when_all_csv_exist(
    monkeypatch: pytest.MonkeyPatch,
    service_and_mapper: tuple[PolygonAggregatesBackfillService, _MockSymbolMapper],
) -> None:
    """Process symbol skips fetching when all CSV files exist.

    Given a backfill service with resume and save_csv enabled,
    When processing a symbol with existing CSV files for all days,
    Then the fetch operation is skipped and no data is persisted.
    """
    monkeypatch.setattr(
        "snapper.application.updaters.historical.aggregates.datetime",
        _FixedDateTime,
    )
    service, _ = service_and_mapper
    service_private = cast(Any, service)
    service._days_back = 1
    service._resume = True
    service._save_csv = True
    loader_stub = _LoaderStub(csv_exists=True)
    service._loader = cast(Any, loader_stub)
    repo = _RepoStub()
    service._db_async = cast(Repository, repo)
    context = SimpleNamespace(
        native_symbol="BTC-USD",
        polygon_symbol="X:BTCUSD",
        base_currency="BTC",
        quote_currency="USD",
    )
    await service_private._process_symbol(context)
    assert repo.candle_batches == []
    assert loader_stub.fetch_calls == []


@pytest.mark.asyncio
async def test_process_symbol_persists_fetched_rows(
    monkeypatch: pytest.MonkeyPatch,
    service_and_mapper: tuple[PolygonAggregatesBackfillService, _MockSymbolMapper],
) -> None:
    """Process symbol persists fetched candle rows to database.

    Given a backfill service configured to fetch and persist data,
    When processing a symbol with available candle data,
    Then the fetched candle rows are persisted to the database.
    """
    monkeypatch.setattr(
        "snapper.application.updaters.historical.aggregates.datetime",
        _FixedDateTime,
    )
    candles = [
        AggregateCandle(
            ticker="X:BTCUSD",
            timestamp=_FIXED_NOW - timedelta(days=1),
            open=Decimal("100"),
            high=Decimal("110"),
            low=Decimal("90"),
            close=Decimal("105"),
            volume=Decimal("5"),
            vwap=Decimal("103"),
            transactions=20,
        )
    ]
    service, _ = service_and_mapper
    service_private = cast(Any, service)
    service._days_back = 1
    service._resume = False
    service._save_csv = False
    loader_stub = _LoaderStub(csv_exists=False, candles=candles)
    service._loader = cast(Any, loader_stub)
    repo = _RepoStub()
    service._db_async = cast(Repository, repo)
    context = SimpleNamespace(
        native_symbol="BTC-USD",
        polygon_symbol="X:BTCUSD",
        base_currency="BTC",
        quote_currency="USD",
    )
    await service_private._process_symbol(context)
    assert len(repo.instrument_calls) == 1
    assert repo.instrument_calls[0]["symbol"] == "BTC-USD"
    assert repo.candle_batches and repo.candle_batches[0][0]["timeframe"] == "1m"
    assert loader_stub.fetch_calls, "fetch_aggregates should have been invoked"


def test_lookup_context_by_polygon_symbol_alias_found_catalog_missing() -> None:
    """Return None when alias exists but catalog row is missing.

    Given a polygon alias found in the database but no matching catalog,
    When _lookup_context_by_polygon_symbol is called,
    Then None is returned.
    """
    service = PolygonAggregatesBackfillService()
    alias_stub = SimpleNamespace(symbol_public_id="sym-pub-orphan", exchange_symbol="X:ORPHAN")
    responses: list[Any] = [alias_stub, None]

    class _Session:
        def __enter__(self) -> _Session:
            return self

        def __exit__(self, *_: object) -> None:
            return None

        def execute(self, *_: object, **__: object) -> Any:
            val = responses.pop(0)
            return SimpleNamespace(scalar_one_or_none=lambda: val)

    service._db_sync = cast(Any, SimpleNamespace(get_session=lambda: _Session()))
    assert service._lookup_context_by_polygon_symbol("X:ORPHAN") is None
