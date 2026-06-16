"""Unit tests for PolygonGroupedCandleLoaderService and its CLI command."""

from datetime import UTC
from datetime import date
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from typing import Any
from typing import cast
from unittest.mock import AsyncMock

import pytest
from typer.testing import CliRunner

import snapper.application.updaters.historical.grouped_candle_loader as grouped_module
import snapper.cli.app as app_module
from snapper.application.updaters.historical.grouped_candle_loader import OwnershipViolationError
from snapper.application.updaters.historical.grouped_candle_loader import (
    PolygonGroupedCandleLoaderService,
)
from snapper.cli.app import app
from snapper.core.types import ExchangeEnum
from snapper.infrastructure.historical.polygon.loader import GroupedDailyRow


class _StubSyncRepo:
    """Synchronous repository stub resolving native symbols to archives."""

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


class _StubEngine:
    """Async engine stub recording disposal."""

    def __init__(self) -> None:
        self.disposed = False

    async def dispose(self) -> None:
        self.disposed = True


class _StubAsyncRepo:
    """Asynchronous repository stub recording upserts and candle reads."""

    def __init__(self, existing_candles: list[dict[str, Any]] | None = None) -> None:
        self.upsert_batches: list[int] = []
        self.upserted_rows: list[dict[str, Any]] = []
        self.ensure_calls: list[str] = []
        self.ensure_exchanges: list[Any] = []
        self.get_candles_exchanges: list[Any] = []
        self._existing = existing_candles or []
        self.engine = _StubEngine()

    async def ensure_instrument(
        self,
        symbol_public_id: str,
        exchange: Any,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
    ) -> tuple[int, str]:
        self.ensure_calls.append(symbol_public_id)
        self.ensure_exchanges.append(exchange)
        return (1, f"inst-{symbol_public_id}")

    async def get_candles(
        self,
        instrument: str,
        timeframe: str,
        start: datetime | None,
        end: datetime | None,
        exchange: Any,
        as_of: datetime,
        limit: int | None = None,
        order: str = "asc",
    ) -> list[dict[str, Any]]:
        self.get_candles_exchanges.append(exchange)
        return list(self._existing)

    async def upsert_candles(self, rows: list[dict[str, Any]]) -> int:
        self.upsert_batches.append(len(rows))
        self.upserted_rows.extend(rows)
        return len(rows)


class _StubSymbolMapper:
    """Symbol mapper stub recording the cache load."""

    def __init__(self) -> None:
        self.loaded = False

    def load_cache_if_needed(self) -> None:
        self.loaded = True


def _build_service(
    monkeypatch: pytest.MonkeyPatch,
    *,
    symbols: list[str] | None,
    exchange: ExchangeEnum | None = ExchangeEnum.KRAKEN,
    cut_date: date | None = date(2026, 6, 16),
    all_mapped: bool = False,
    lookback_days: int = 800,
) -> PolygonGroupedCandleLoaderService:
    """Construct a service with the symbol mapper singleton stubbed.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        symbols: Requested native symbols (None defers to settings).
        exchange: Venue the persisted bars live under (None to test fail-fast).
        cut_date: Synthesis ownership boundary.
        all_mapped: Whether to load every Polygon-mapped native symbol.
        lookback_days: Backward cache-walk cap.

    Returns:
        Constructed PolygonGroupedCandleLoaderService.
    """
    mapper = _StubSymbolMapper()
    monkeypatch.setattr(
        grouped_module.SymbolMapperService,
        "get_instance",
        classmethod(lambda cls: mapper),
    )
    return PolygonGroupedCandleLoaderService(
        symbols=symbols,
        exchange=exchange,
        cut_date=cut_date,
        all_mapped=all_mapped,
        lookback_days=lookback_days,
    )


def _grouped_row(day: date, *, vwap: Decimal | None = Decimal("1.4")) -> GroupedDailyRow:
    """Build a grouped-daily row whose close timestamp is midnight UTC of ``day``.

    Args:
        day: Calendar day the row represents.
        vwap: Volume-weighted average price (None exercises the null branch).

    Returns:
        A GroupedDailyRow for the day.
    """
    return GroupedDailyRow(
        ticker="X:FETUSD",
        open=Decimal("1"),
        high=Decimal("2"),
        low=Decimal("0.5"),
        close=Decimal("1.5"),
        volume=Decimal("10"),
        vwap=vwap,
        total_trades=3,
        closing_timestamp=datetime(day.year, day.month, day.day, tzinfo=UTC),
    )


def _settings(symbols: list[str]) -> Any:
    """Build a settings stub exposing a Polygon instruments list and db_url.

    Args:
        symbols: Polygon instrument symbols the settings report.

    Returns:
        A settings stub for the service.
    """
    return cast(
        Any,
        type(
            "Settings",
            (),
            {
                "instruments": {ExchangeEnum.POLYGON: list(symbols)},
                "db_url": "sqlite+aiosqlite:///:memory:",
                "zmq_broker_xsub": "tcp://x",
            },
        )(),
    )


def test_get_default_parameters_uses_settings_instruments() -> None:
    """Expose settings-derived defaults for the process registry.

    Given: An AppSettings with a Polygon instruments list,
    When: get_default_parameters is called,
    Then: The polygon symbols and load defaults are returned with cut_date None.
    """
    settings = cast(Any, type("S", (), {"instruments": {ExchangeEnum.POLYGON: ["FET-USD"]}})())
    params = PolygonGroupedCandleLoaderService.get_default_parameters(settings)
    assert params["symbols"] == ["FET-USD"]
    assert params["exchange"] is None
    assert params["cut_date"] is None
    assert params["all_mapped"] is False
    assert params["lookback_days"] == 800


def test_native_to_grouped_ticker_valid_and_invalid() -> None:
    """Map a BASE-QUOTE pair to its grouped crypto ticker, reject others.

    Given: A valid pair and malformed symbols,
    When: _native_to_grouped_ticker is called,
    Then: The valid pair yields ``X:{BASE}{QUOTE}`` and others yield None.
    """
    assert PolygonGroupedCandleLoaderService._native_to_grouped_ticker("FET-USD") == "X:FETUSD"
    assert PolygonGroupedCandleLoaderService._native_to_grouped_ticker("NODASH") is None
    assert PolygonGroupedCandleLoaderService._native_to_grouped_ticker("A-B-C") is None
    assert PolygonGroupedCandleLoaderService._native_to_grouped_ticker("-USD") is None


@pytest.mark.asyncio
async def test_start_requires_exchange(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fail fast when no exchange was provided.

    Given: A service constructed with exchange=None,
    When: start() runs,
    Then: A ValueError is raised before any repository is touched.
    """
    service = _build_service(monkeypatch, symbols=["FET-USD"], exchange=None)
    with pytest.raises(ValueError, match="exchange is required"):
        await service.start()


@pytest.mark.asyncio
async def test_start_requires_cut_date(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fail fast when no cut_date was provided.

    Given: A service constructed with cut_date=None,
    When: start() runs,
    Then: A ValueError is raised before any repository is touched.
    """
    service = _build_service(monkeypatch, symbols=["FET-USD"], cut_date=None)
    with pytest.raises(ValueError, match="cut_date is required"):
        await service.start()


@pytest.mark.asyncio
async def test_start_happy_path_upserts_and_disposes(monkeypatch: pytest.MonkeyPatch) -> None:
    """Run the full start() flow against stubbed dependencies.

    Given: One configured leg with cached pre-cut grouped rows,
    When: start() runs,
    Then: One native 1d candle is upserted and resources are disposed.
    """
    sync_repo = _StubSyncRepo(
        archive_symbols={"pid-fet": "FET-USD"},
        native_to_archive={"FET-USD": "FET-USD"},
    )
    async_repo = _StubAsyncRepo()
    monkeypatch.setattr(grouped_module, "get_settings_service", AsyncMock(return_value=object()))
    monkeypatch.setattr(
        grouped_module, "get_settings_with_service", lambda svc: _settings(["FET-USD"])
    )
    monkeypatch.setattr(grouped_module, "DatabaseRepository", lambda url: sync_repo)
    monkeypatch.setattr(grouped_module, "get_repository", lambda url: async_repo)
    monkeypatch.setattr(
        grouped_module,
        "load_recent_grouped_daily",
        lambda *a, **k: [_grouped_row(date(2026, 6, 14))],
    )
    service = _build_service(monkeypatch, symbols=["FET-USD"])
    service.settings = _settings(["FET-USD"])
    await service.start()
    assert async_repo.upsert_batches == [1]
    assert async_repo.upserted_rows[0]["source"] == "native"
    assert async_repo.upserted_rows[0]["complete"] is True
    assert async_repo.upserted_rows[0]["timeframe"] == "1d"
    assert async_repo.engine.disposed is True
    assert sync_repo.disposed is True
    assert service._db_async is None


@pytest.mark.asyncio
async def test_start_warns_when_no_targets(monkeypatch: pytest.MonkeyPatch) -> None:
    """Return early with a warning when no targets resolve.

    Given: A leg whose native symbol has no active Symbol identity,
    When: start() runs,
    Then: No upserts occur and resources are still disposed.
    """
    sync_repo = _StubSyncRepo(archive_symbols={}, native_to_archive={})
    async_repo = _StubAsyncRepo()
    monkeypatch.setattr(grouped_module, "get_settings_service", AsyncMock(return_value=object()))
    monkeypatch.setattr(
        grouped_module, "get_settings_with_service", lambda svc: _settings(["FET-USD"])
    )
    monkeypatch.setattr(grouped_module, "DatabaseRepository", lambda url: sync_repo)
    monkeypatch.setattr(grouped_module, "get_repository", lambda url: async_repo)
    service = _build_service(monkeypatch, symbols=["FET-USD"])
    service.settings = _settings(["FET-USD"])
    await service.start()
    assert async_repo.upsert_batches == []
    assert async_repo.engine.disposed is True


@pytest.mark.asyncio
async def test_dispose_resources_tolerates_missing_repos(monkeypatch: pytest.MonkeyPatch) -> None:
    """Dispose cleanly when no repositories were allocated.

    Given: A freshly constructed service with no repos,
    When: _dispose_resources is awaited,
    Then: It completes without error and clears references.
    """
    service = _build_service(monkeypatch, symbols=["FET-USD"])
    await service._dispose_resources()
    assert service._db_async is None
    assert service._db_sync is None


def test_resolve_targets_defaults_to_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    """Default to settings symbols when neither flag nor request is given.

    Given: symbols=None and settings listing one Polygon symbol,
    When: _resolve_targets is called,
    Then: The settings symbol resolves to its public id.
    """
    service = _build_service(monkeypatch, symbols=None)
    service.settings = _settings(["FET-USD"])
    service._archive_symbols = {"pid-fet": "FET-USD"}
    service._db_sync = cast(
        Any,
        _StubSyncRepo(
            archive_symbols={"pid-fet": "FET-USD"},
            native_to_archive={"FET-USD": "FET-USD"},
        ),
    )
    assert service._resolve_targets() == [("FET-USD", "pid-fet")]


def test_resolve_targets_all_mapped_enumerates_polygon(monkeypatch: pytest.MonkeyPatch) -> None:
    """Treat --all as the full Polygon-mapped native universe.

    Given: all_mapped=True and a stubbed available-symbols universe,
    When: _resolve_targets is called,
    Then: It resolves the enumerated symbols, skipping unmapped ones.
    """
    monkeypatch.setattr(
        grouped_module, "get_available_polygon_symbols", lambda: ["FET-USD", "NOPE-USD"]
    )
    service = _build_service(monkeypatch, symbols=None, all_mapped=True)
    service.settings = _settings([])
    service._archive_symbols = {"pid-fet": "FET-USD"}
    service._db_sync = cast(
        Any,
        _StubSyncRepo(
            archive_symbols={"pid-fet": "FET-USD"},
            native_to_archive={"FET-USD": "FET-USD"},
        ),
    )
    assert service._resolve_targets() == [("FET-USD", "pid-fet")]


def test_resolve_targets_wildcard_settings_enumerates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Treat wildcard settings like --all.

    Given: symbols=None and wildcard Polygon settings,
    When: _resolve_targets is called,
    Then: It enumerates the available Polygon universe.
    """
    monkeypatch.setattr(grouped_module, "get_available_polygon_symbols", lambda: ["FET-USD"])
    service = _build_service(monkeypatch, symbols=None)
    service.settings = _settings(["*"])
    service._archive_symbols = {"pid-fet": "FET-USD"}
    service._db_sync = cast(
        Any,
        _StubSyncRepo(
            archive_symbols={"pid-fet": "FET-USD"},
            native_to_archive={"FET-USD": "FET-USD"},
        ),
    )
    assert service._resolve_targets() == [("FET-USD", "pid-fet")]


def test_resolve_targets_empty_returns_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    """Return no targets when neither flag is set and settings are empty.

    Given: symbols=None and an empty Polygon settings list,
    When: _resolve_targets is called,
    Then: An empty list is returned.
    """
    service = _build_service(monkeypatch, symbols=None)
    service.settings = _settings([])
    assert service._resolve_targets() == []


def test_resolve_symbol_public_id_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    """Resolve native symbols to public ids across all branches.

    Given: Repositories that resolve, fail to resolve, and resolve to an
        archive absent from the public-id map,
    When: _resolve_symbol_public_id is called,
    Then: It returns the public id or None as appropriate.
    """
    service = _build_service(monkeypatch, symbols=["FET-USD"])
    service._archive_symbols = {"pid-other": "OTHER-USD", "pid-fet": "FET-USD"}
    service._db_sync = cast(
        Any,
        _StubSyncRepo(
            archive_symbols={"pid-fet": "FET-USD"},
            native_to_archive={"FET-USD": "FET-USD"},
        ),
    )
    assert service._resolve_symbol_public_id("FET-USD") == "pid-fet"
    assert service._resolve_symbol_public_id("MISSING-USD") is None
    service._archive_symbols = {}
    assert service._resolve_symbol_public_id("FET-USD") is None


@pytest.mark.asyncio
async def test_load_symbol_skips_non_pair(monkeypatch: pytest.MonkeyPatch) -> None:
    """Skip a symbol that is not a single BASE-QUOTE pair.

    Given: A native symbol with no single dash split,
    When: _load_symbol is called,
    Then: It returns without ensuring an instrument or upserting.
    """
    async_repo = _StubAsyncRepo()
    service = _build_service(monkeypatch, symbols=["NODASH"])
    service._db_async = cast(Any, async_repo)
    await service._load_symbol("NODASH", "pid-x")
    assert async_repo.ensure_calls == []
    assert async_repo.upsert_batches == []


@pytest.mark.asyncio
async def test_load_symbol_writes_and_preflights_under_configured_exchange(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Resolve the instrument and preflight under the configured venue, not Polygon.

    Given: a loader configured with exchange=KRAKEN,
    When: _load_symbol runs,
    Then: ensure_instrument (write identity) AND the ownership-preflight
        get_candles read both use KRAKEN — never ExchangeEnum.POLYGON — so the
        persisted plane is the one the live read/warmup resolves.
    """
    async_repo = _StubAsyncRepo()
    monkeypatch.setattr(grouped_module, "load_recent_grouped_daily", lambda *a, **k: [])
    service = _build_service(monkeypatch, symbols=["FET-USD"], exchange=ExchangeEnum.KRAKEN)
    service._db_async = cast(Any, async_repo)
    await service._load_symbol("FET-USD", "pid-fet")
    assert async_repo.ensure_exchanges == [ExchangeEnum.KRAKEN]
    assert async_repo.get_candles_exchanges == [ExchangeEnum.KRAKEN]
    assert ExchangeEnum.POLYGON not in async_repo.ensure_exchanges


@pytest.mark.asyncio
async def test_load_symbol_no_cached_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    """Log and return when the cache holds no pre-cut rows.

    Given: An empty grouped-daily cache for the ticker,
    When: _load_symbol is called,
    Then: No upsert occurs.
    """
    async_repo = _StubAsyncRepo()
    monkeypatch.setattr(grouped_module, "load_recent_grouped_daily", lambda *a, **k: [])
    service = _build_service(monkeypatch, symbols=["FET-USD"])
    service._db_async = cast(Any, async_repo)
    await service._load_symbol("FET-USD", "pid-fet")
    assert async_repo.ensure_calls == ["pid-fet"]
    assert async_repo.upsert_batches == []


@pytest.mark.asyncio
async def test_load_symbol_batches_upserts(monkeypatch: pytest.MonkeyPatch) -> None:
    """Upsert cached rows in batches bounded by BATCH_COMMIT_SIZE.

    Given: More cached rows than the batch size,
    When: _load_symbol is called,
    Then: Rows are upserted across multiple batches.
    """
    async_repo = _StubAsyncRepo()
    rows = [_grouped_row(date(2026, 1, 1) + timedelta(days=i)) for i in range(3)]
    monkeypatch.setattr(grouped_module, "load_recent_grouped_daily", lambda *a, **k: rows)
    service = _build_service(monkeypatch, symbols=["FET-USD"])
    service.BATCH_COMMIT_SIZE = 2
    service._db_async = cast(Any, async_repo)
    await service._load_symbol("FET-USD", "pid-fet")
    assert async_repo.upsert_batches == [2, 1]


@pytest.mark.asyncio
async def test_load_symbol_reruns_emit_identical_natural_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Re-running emits identical natural keys + business columns (SCD2-idempotent).

    Given: a fixed cached corpus,
    When: _load_symbol runs twice,
    Then: both runs emit rows with the same ``(instrument, timeframe, open_at)``
        and OHLCV/provenance, so the SCD2 upsert collapses a re-run to a no-op
        (the natural-key dedupe is enforced by upsert_candles, repo-tested).
    """
    async_repo = _StubAsyncRepo()
    grouped = [_grouped_row(date(2026, 1, 1)), _grouped_row(date(2026, 1, 2))]
    monkeypatch.setattr(grouped_module, "load_recent_grouped_daily", lambda *a, **k: grouped)
    service = _build_service(monkeypatch, symbols=["FET-USD"])
    service._db_async = cast(Any, async_repo)
    await service._load_symbol("FET-USD", "pid-fet")
    await service._load_symbol("FET-USD", "pid-fet")
    half = len(async_repo.upserted_rows) // 2
    first, second = async_repo.upserted_rows[:half], async_repo.upserted_rows[half:]
    business = ("instrument_public_id", "timeframe", "open_at", "close", "source", "complete")
    assert [{k: r[k] for k in business} for r in first] == [
        {k: r[k] for k in business} for r in second
    ]


@pytest.mark.asyncio
async def test_ownership_preflight_rejects_synthesized_before_cut(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject a synthesized 1d row that predates the cut date.

    Given: An active synthesized 1d row before cut_date,
    When: _ownership_preflight runs,
    Then: An OwnershipViolationError is raised.
    """
    existing = [{"open_at": datetime(2026, 6, 10, tzinfo=UTC), "source": "synthesized"}]
    async_repo = _StubAsyncRepo(existing_candles=existing)
    service = _build_service(monkeypatch, symbols=["FET-USD"], cut_date=date(2026, 6, 16))
    service._db_async = cast(Any, async_repo)
    with pytest.raises(OwnershipViolationError, match="predates cut_date"):
        await service._ownership_preflight("FET-USD")


@pytest.mark.asyncio
async def test_ownership_preflight_rejects_native_at_or_after_cut(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject a native 1d row at/after the cut date (a prior overrun).

    Given: An active native 1d row at cut_date,
    When: _ownership_preflight runs,
    Then: An OwnershipViolationError is raised.
    """
    existing = [{"open_at": datetime(2026, 6, 16, tzinfo=UTC), "source": "native"}]
    async_repo = _StubAsyncRepo(existing_candles=existing)
    service = _build_service(monkeypatch, symbols=["FET-USD"], cut_date=date(2026, 6, 16))
    service._db_async = cast(Any, async_repo)
    with pytest.raises(OwnershipViolationError, match="at/after cut_date"):
        await service._ownership_preflight("FET-USD")


@pytest.mark.asyncio
async def test_ownership_preflight_accepts_disjoint_plane(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Accept a plane whose native/synthesized rows respect the cut date.

    Given: A native row before cut, a synthesized row at cut, and a row with
        a defaulted (missing) source before cut,
    When: _ownership_preflight runs,
    Then: No error is raised.
    """
    existing = [
        {"open_at": datetime(2026, 6, 10, tzinfo=UTC), "source": "native"},
        {"open_at": datetime(2026, 6, 16, tzinfo=UTC), "source": "synthesized"},
        {"open_at": datetime(2026, 6, 9, tzinfo=UTC)},
    ]
    async_repo = _StubAsyncRepo(existing_candles=existing)
    service = _build_service(monkeypatch, symbols=["FET-USD"], cut_date=date(2026, 6, 16))
    service._db_async = cast(Any, async_repo)
    await service._ownership_preflight("FET-USD")


def test_build_rows_filters_cut_and_maps_provenance(monkeypatch: pytest.MonkeyPatch) -> None:
    """Build native 1d rows for pre-cut days only, mapping open_at to midnight.

    Given: Grouped rows before and at the cut date, one with a null vwap,
    When: _build_rows is called,
    Then: At/after-cut days are dropped and pre-cut rows carry native
        provenance with a midnight-UTC open_at.
    """
    service = _build_service(monkeypatch, symbols=["FET-USD"], cut_date=date(2026, 6, 16))
    grouped = [
        _grouped_row(date(2026, 6, 14)),
        _grouped_row(date(2026, 6, 15), vwap=None),
        _grouped_row(date(2026, 6, 16)),
        _grouped_row(date(2026, 6, 17)),
    ]
    rows = service._build_rows(grouped, "inst-fet")
    assert [r["open_at"] for r in rows] == [
        datetime(2026, 6, 14, tzinfo=UTC),
        datetime(2026, 6, 15, tzinfo=UTC),
    ]
    assert rows[0]["source"] == "native"
    assert rows[0]["complete"] is True
    assert rows[0]["vwap"] == 1.4
    assert rows[1]["vwap"] is None
    assert rows[0]["instrument_public_id"] == "inst-fet"


class _CliDummyService:
    """CLI stub service capturing constructor kwargs."""

    last_kwargs: dict[str, Any] = {}

    def __init__(
        self,
        symbols: list[str] | None,
        exchange: ExchangeEnum,
        cut_date: date,
        all_mapped: bool,
        lookback_days: int,
    ) -> None:
        _CliDummyService.last_kwargs = {
            "symbols": symbols,
            "exchange": exchange,
            "cut_date": cut_date,
            "all_mapped": all_mapped,
            "lookback_days": lookback_days,
        }

    async def start(self) -> None:
        return None


class _CliFailingService(_CliDummyService):
    """CLI stub service that fails on start."""

    async def start(self) -> None:
        raise RuntimeError("load-fail")


def test_cli_grouped_candles_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """Run the polygon-load-grouped-candles command happy path.

    Given: A stub loader service,
    When: the command is invoked with --exchange, --cut-date and a symbol,
    Then: It exits 0 and forwards the parsed venue + cut date.
    """
    monkeypatch.setattr(app_module, "PolygonGroupedCandleLoaderService", _CliDummyService)
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "polygon-load-grouped-candles",
            "--exchange",
            "kraken",
            "--cut-date",
            "2026-06-16",
            "--symbol",
            "FET-USD",
        ],
    )
    assert result.exit_code == 0
    assert "Polygon grouped-daily candle load complete!" in result.stdout
    assert _CliDummyService.last_kwargs["symbols"] == ["FET-USD"]
    assert _CliDummyService.last_kwargs["exchange"] == ExchangeEnum.KRAKEN
    assert _CliDummyService.last_kwargs["cut_date"] == date(2026, 6, 16)
    assert _CliDummyService.last_kwargs["all_mapped"] is False


def test_cli_grouped_candles_requires_exchange(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reject invocation without the required --exchange.

    Given: The command,
    When: it is invoked with --cut-date but no --exchange,
    Then: It exits non-zero (missing required option).
    """
    monkeypatch.setattr(app_module, "PolygonGroupedCandleLoaderService", _CliDummyService)
    runner = CliRunner()
    result = runner.invoke(
        app, ["polygon-load-grouped-candles", "--cut-date", "2026-06-16", "--all"]
    )
    assert result.exit_code != 0


def test_cli_grouped_candles_requires_cut_date(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reject invocation without the required --cut-date.

    Given: The command,
    When: it is invoked with --exchange but no --cut-date,
    Then: It exits non-zero (missing required option).
    """
    monkeypatch.setattr(app_module, "PolygonGroupedCandleLoaderService", _CliDummyService)
    runner = CliRunner()
    result = runner.invoke(app, ["polygon-load-grouped-candles", "--exchange", "kraken", "--all"])
    assert result.exit_code != 0


def test_cli_grouped_candles_reports_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Surface a service error as exit code 1.

    Given: A stub loader service that raises on start,
    When: the command is invoked,
    Then: It exits 1 with an error message.
    """
    monkeypatch.setattr(app_module, "PolygonGroupedCandleLoaderService", _CliFailingService)
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "polygon-load-grouped-candles",
            "--exchange",
            "kraken",
            "--cut-date",
            "2026-06-16",
            "--all",
        ],
    )
    assert result.exit_code == 1
    assert "Error during Polygon grouped-daily candle load" in result.stdout
