"""Unit tests for candle coverage verification and its CLI command."""

from datetime import UTC
from datetime import date
from datetime import datetime
from datetime import timedelta
from typing import Any
from typing import cast
from unittest.mock import AsyncMock

import pytest
from typer.testing import CliRunner

import snapper.cli.app as app_module
from snapper.application.services.candle_coverage import CoverageEntry
from snapper.application.services.candle_coverage import CoverageReport
from snapper.application.services.candle_coverage import verify_candle_coverage
from snapper.application.services.market_cache import CandleSnap
from snapper.cli.app import app
from snapper.core.types import ExchangeEnum
from snapper.data.repository_types import CandleRow

_AS_OF = datetime(2026, 6, 16, 10, 7, tzinfo=UTC)
_WRITER_LAG = 120
_TEN = datetime(2026, 6, 16, 10, 0, tzinfo=UTC)


def _row(
    open_at: datetime,
    *,
    source: str = "synthesized",
    complete: bool = True,
    close: float = 100.0,
    volume: float = 10.0,
) -> CandleRow:
    """Build a CandleRow for the persisted plane.

    Args:
        open_at: Bar window start (UTC).
        source: Provenance tag.
        complete: Completeness flag.
        close: Close price (also used for O/H/L for parity tests).
        volume: Bar volume.

    Returns:
        A populated CandleRow dict.
    """
    return CandleRow(
        open_at=open_at,
        timeframe="x",
        open=close,
        high=close,
        low=close,
        close=close,
        volume=volume,
        vwap=close,
        trades=7,
        source=source,
        complete=complete,
        public_id="00000000-0000-7000-8000-0000000000aa",
        timestamp=open_at,
        session_id="seed",
        sequence_id=1,
    )


def _snap(open_at: datetime, *, close: float = 100.0, volume: float = 2.0) -> CandleSnap:
    """Build a 1m CandleSnap for the cache derive-parity tests."""
    return CandleSnap(
        open_at_ms=int(open_at.timestamp() * 1000),
        open=close,
        high=close,
        low=close,
        close=close,
        volume=volume,
    )


class _StubEngine:
    """Async engine stub recording disposal."""

    def __init__(self) -> None:
        self.disposed = False

    async def dispose(self) -> None:
        self.disposed = True


class _StubRepo:
    """Repository stub returning configured rows per (symbol, timeframe).

    Stored rows are ascending; ``get_candles`` honours the ``[start, end]`` range
    filter the verifier uses (inclusive) and returns ascending rows.
    """

    def __init__(self, rows_by_key: dict[tuple[str, str], list[CandleRow]]) -> None:
        self._rows = rows_by_key
        self.engine = _StubEngine()

    async def get_candles(
        self,
        *,
        instrument: str,
        timeframe: str,
        start: datetime | None,
        end: datetime | None,
        exchange: Any,
        as_of: datetime,
        limit: int | None = None,
        order: str = "asc",
    ) -> list[CandleRow]:
        rows = self._rows.get((instrument, timeframe), [])
        if start is not None and end is not None:
            rows = [row for row in rows if start <= row["open_at"] <= end]
        return list(rows)


class _StubCache:
    """Cache stub returning configured 1m snaps."""

    def __init__(self, snaps: list[CandleSnap]) -> None:
        self._snaps = snaps

    async def get_1m_candles(self, exchange: Any, symbol: str, *, limit: int) -> list[CandleSnap]:
        return list(self._snaps)


def _at(hour: int, minute: int) -> datetime:
    """Return an HH:MM UTC time on the fixture day."""
    return datetime(2026, 6, 16, hour, minute, tzinfo=UTC)


def _day(day: int) -> datetime:
    """Return a 2026-06-`day` UTC midnight."""
    return datetime(2026, 6, day, tzinfo=UTC)


async def _verify(
    repo: Any,
    *,
    cache: Any = None,
    symbols: list[str] | None = None,
    timeframes: list[str] | None = None,
    cut_date: date = date(2026, 6, 15),
    min_bars: int = 3,
) -> CoverageReport:
    """Invoke verify_candle_coverage with fixture defaults."""
    return await verify_candle_coverage(
        repo=cast(Any, repo),
        cache=cache,
        exchange=ExchangeEnum.KRAKEN,
        native_symbols=symbols or ["FET-USD"],
        timeframes=timeframes or ["5m"],
        cut_date=cut_date,
        as_of=_AS_OF,
        writer_lag_s=_WRITER_LAG,
        min_bars=min_bars,
    )


@pytest.mark.asyncio
async def test_pass_5m_contiguous_synthesized() -> None:
    """Pass when the 5m grid window is full, complete and synthesized.

    Given: every canonical 5m slot in the window present, synthesized, complete,
    When: coverage is verified without a cache,
    Then: the entry passes and the report is OK.
    """
    rows = [_row(_at(9, 50)), _row(_at(9, 55)), _row(_TEN)]
    repo = _StubRepo({("FET-USD", "5m"): rows})
    report = await _verify(repo)
    assert report.ok is True
    entry = report.entries[0]
    assert entry.ok is True
    assert entry.reason == "ok"
    assert entry.bars == 3
    assert entry.newest == _TEN
    assert entry.oldest == _at(9, 50)


@pytest.mark.asyncio
async def test_pass_1d_native_synthesized_boundary() -> None:
    """Pass across the 1d native->synthesized cut boundary.

    Given: 1d bars 06-13/06-14 native and 06-15 synthesized with cut 06-15,
    When: coverage is verified,
    Then: provenance is correct on both sides of the seam and the entry passes.
    """
    rows = [
        _row(_day(13), source="native"),
        _row(_day(14), source="native"),
        _row(_day(15), source="synthesized"),
    ]
    repo = _StubRepo({("FET-USD", "1d"): rows})
    report = await _verify(repo, timeframes=["1d"], cut_date=date(2026, 6, 15))
    assert report.ok is True
    assert report.entries[0].newest == _day(15)


@pytest.mark.asyncio
async def test_fail_missing_newest_window_slot() -> None:
    """Fail when the latest closed slot is absent (staleness).

    Given: a 5m window missing its newest (10:00) slot,
    When: coverage is verified,
    Then: the entry fails with a missing-bar reason at 10:00.
    """
    rows = [_row(_at(9, 50)), _row(_at(9, 55))]
    repo = _StubRepo({("FET-USD", "5m"): rows})
    report = await _verify(repo)
    assert report.entries[0].ok is False
    assert "missing bar" in report.entries[0].reason
    assert "10:00" in report.entries[0].reason


@pytest.mark.asyncio
async def test_fail_interior_gap() -> None:
    """Fail when an interior canonical slot is missing inside the window.

    Given: a 5m window with the 09:55 slot missing,
    When: coverage is verified,
    Then: the entry fails with a missing-bar reason at 09:55.
    """
    rows = [_row(_at(9, 50)), _row(_TEN)]
    repo = _StubRepo({("FET-USD", "5m"): rows})
    report = await _verify(repo)
    assert report.entries[0].ok is False
    assert "missing bar" in report.entries[0].reason
    assert "09:55" in report.entries[0].reason


@pytest.mark.asyncio
async def test_fail_incomplete_bar() -> None:
    """Fail when any windowed bar is not complete.

    Given: a full 5m window whose middle bar is incomplete,
    When: coverage is verified,
    Then: the entry fails with an incomplete reason.
    """
    rows = [_row(_at(9, 50)), _row(_at(9, 55), complete=False), _row(_TEN)]
    repo = _StubRepo({("FET-USD", "5m"): rows})
    report = await _verify(repo)
    assert report.entries[0].ok is False
    assert "incomplete" in report.entries[0].reason


@pytest.mark.asyncio
async def test_fail_wrong_provenance() -> None:
    """Fail when a higher-TF bar carries the wrong provenance.

    Given: a full 5m window with one bar tagged native,
    When: coverage is verified,
    Then: the entry fails with a provenance reason.
    """
    rows = [_row(_at(9, 50)), _row(_at(9, 55), source="native"), _row(_TEN)]
    repo = _StubRepo({("FET-USD", "5m"): rows})
    report = await _verify(repo)
    assert report.entries[0].ok is False
    assert "provenance" in report.entries[0].reason


@pytest.mark.asyncio
async def test_fail_seam_gap_after_cut() -> None:
    """Catch a [cut_date, publisher_start) seam gap (the §4e invariant).

    Given: 1d cut 06-13 with native 06-12 but the first synthesized day (06-13)
        missing because synthesis began persisting only on 06-14,
    When: coverage is verified,
    Then: the entry fails with a missing-bar reason at the seam (06-13), proving
        the window spans back across the cut boundary.
    """
    rows = [
        _row(_day(12), source="native"),
        _row(_day(14), source="synthesized"),
        _row(_day(15), source="synthesized"),
    ]
    repo = _StubRepo({("FET-USD", "1d"): rows})
    report = await _verify(repo, timeframes=["1d"], cut_date=date(2026, 6, 13))
    assert report.entries[0].ok is False
    assert "missing bar" in report.entries[0].reason
    assert "06-13" in report.entries[0].reason


@pytest.mark.asyncio
async def test_fail_empty_plane() -> None:
    """Fail with null bounds when the persisted plane is empty.

    Given: an empty persisted plane,
    When: coverage is verified,
    Then: the entry fails on the first missing slot with zero bars and no bounds.
    """
    repo = _StubRepo({})
    report = await _verify(repo)
    entry = report.entries[0]
    assert entry.ok is False
    assert "missing bar" in entry.reason
    assert entry.bars == 0
    assert entry.oldest is None
    assert entry.newest is None
    assert report.ok is False


@pytest.mark.asyncio
async def test_parity_pass_with_warm_cache() -> None:
    """Pass derive-parity when the in-window derived bar matches the DB OHLCV.

    Given: a full 5m window and a cache deriving the 10:00 bar with matching OHLCV,
    When: coverage is verified with the cache,
    Then: the entry passes.
    """
    rows = [_row(_at(9, 50)), _row(_at(9, 55)), _row(_TEN)]
    repo = _StubRepo({("FET-USD", "5m"): rows})
    snaps = [_snap(_TEN + timedelta(minutes=i)) for i in range(5)]
    report = await _verify(repo, cache=_StubCache(snaps))
    assert report.ok is True


@pytest.mark.asyncio
async def test_parity_skips_out_of_window_derived_bar() -> None:
    """Skip derived bars outside the inspected window (no false fail).

    Given: a full 5m window and a cache deriving only an OLDER (09:30) bar,
    When: coverage is verified with the cache,
    Then: the out-of-window derived bar is ignored and the entry still passes.
    """
    rows = [_row(_at(9, 50)), _row(_at(9, 55)), _row(_TEN)]
    repo = _StubRepo({("FET-USD", "5m"): rows})
    snaps = [_snap(_at(9, 30) + timedelta(minutes=i)) for i in range(5)]
    report = await _verify(repo, cache=_StubCache(snaps))
    assert report.ok is True


@pytest.mark.asyncio
async def test_parity_fail_close_mismatch() -> None:
    """Fail derive-parity when an in-window bar's price differs.

    Given: a DB 10:00 bar with close 200 but a cache deriving close 100,
    When: coverage is verified with the cache,
    Then: the entry fails with an OHLCV-mismatch reason.
    """
    rows = [_row(_at(9, 50)), _row(_at(9, 55)), _row(_TEN, close=200.0)]
    repo = _StubRepo({("FET-USD", "5m"): rows})
    snaps = [_snap(_TEN + timedelta(minutes=i), close=100.0) for i in range(5)]
    report = await _verify(repo, cache=_StubCache(snaps))
    assert report.entries[0].ok is False
    assert "OHLCV mismatch" in report.entries[0].reason


@pytest.mark.asyncio
async def test_parity_fail_volume_mismatch() -> None:
    """Fail derive-parity when an in-window bar's volume differs.

    Given: a DB 10:00 bar with matching price but volume 10 vs derived 5,
    When: coverage is verified with the cache,
    Then: the entry fails (volume is part of the OHLCV parity check).
    """
    rows = [_row(_at(9, 50)), _row(_at(9, 55)), _row(_TEN, volume=10.0)]
    repo = _StubRepo({("FET-USD", "5m"): rows})
    snaps = [_snap(_TEN + timedelta(minutes=i), volume=1.0) for i in range(5)]
    report = await _verify(repo, cache=_StubCache(snaps))
    assert report.entries[0].ok is False
    assert "OHLCV mismatch" in report.entries[0].reason


@pytest.mark.asyncio
async def test_parity_skipped_for_db_fallback_timeframe() -> None:
    """Skip derive-parity for a non-derivable timeframe even with a cache.

    Given: a full contiguous 1h window and a cache,
    When: coverage is verified for 1h (not in the derive map),
    Then: the entry passes on the structural grid alone (parity not attempted).
    """
    base = datetime(2026, 6, 16, 7, 0, tzinfo=UTC)
    rows = [_row(base + timedelta(hours=i)) for i in range(3)]
    repo = _StubRepo({("FET-USD", "1h"): rows})
    report = await _verify(repo, cache=_StubCache([]), timeframes=["1h"])
    assert report.ok is True


@pytest.mark.asyncio
async def test_parity_not_run_when_structural_fails() -> None:
    """Do not run derive-parity when the structural grid already failed.

    Given: an empty DB and a cache,
    When: coverage is verified for 5m,
    Then: the failure reason is the missing bar, not a parity error.
    """
    repo = _StubRepo({})
    report = await _verify(repo, cache=_StubCache([_snap(_TEN)]))
    assert report.entries[0].ok is False
    assert "missing bar" in report.entries[0].reason


@pytest.mark.asyncio
async def test_report_not_ok_when_any_pair_fails() -> None:
    """Aggregate report fails when any single pair fails.

    Given: one symbol with full coverage and one with none,
    When: coverage is verified for both,
    Then: the report is not OK while the healthy pair still passes.
    """
    rows = [_row(_at(9, 50)), _row(_at(9, 55)), _row(_TEN)]
    repo = _StubRepo({("FET-USD", "5m"): rows})
    report = await _verify(repo, symbols=["FET-USD", "RENDER-USD"])
    assert report.ok is False
    by_symbol = {entry.symbol: entry for entry in report.entries}
    assert by_symbol["FET-USD"].ok is True
    assert by_symbol["RENDER-USD"].ok is False


@pytest.mark.asyncio
async def test_rejects_polygon_exchange() -> None:
    """Refuse to verify the orphaned Polygon plane.

    Given: exchange=POLYGON,
    When: coverage is verified,
    Then: a ValueError is raised before any DB read.
    """
    empty_repo = cast(Any, _StubRepo({}))
    cut_date = date(2026, 6, 15)
    with pytest.raises(ValueError, match="polygon"):
        await verify_candle_coverage(
            repo=empty_repo,
            cache=None,
            exchange=ExchangeEnum.POLYGON,
            native_symbols=["FET-USD"],
            timeframes=["5m"],
            cut_date=cut_date,
            as_of=_AS_OF,
            writer_lag_s=_WRITER_LAG,
            min_bars=3,
        )


async def _verify_raw(*, symbols: list[str], timeframes: list[str]) -> CoverageReport:
    """Invoke verify_candle_coverage without the helper's list defaulting."""
    return await verify_candle_coverage(
        repo=cast(Any, _StubRepo({})),
        cache=None,
        exchange=ExchangeEnum.KRAKEN,
        native_symbols=symbols,
        timeframes=timeframes,
        cut_date=date(2026, 6, 15),
        as_of=_AS_OF,
        writer_lag_s=_WRITER_LAG,
        min_bars=3,
    )


@pytest.mark.asyncio
async def test_rejects_empty_symbols() -> None:
    """Refuse a vacuous pass on empty symbols.

    Given: an empty symbol list,
    When: coverage is verified,
    Then: a ValueError is raised.
    """
    with pytest.raises(ValueError, match="non-empty"):
        await _verify_raw(symbols=[], timeframes=["5m"])


@pytest.mark.asyncio
async def test_rejects_empty_timeframes() -> None:
    """Refuse a vacuous pass on empty timeframes.

    Given: an empty timeframe list,
    When: coverage is verified,
    Then: a ValueError is raised.
    """
    with pytest.raises(ValueError, match="non-empty"):
        await _verify_raw(symbols=["FET-USD"], timeframes=[])


@pytest.mark.asyncio
async def test_rejects_unknown_timeframe() -> None:
    """Refuse an unverifiable timeframe rather than raising downstream.

    Given: a timeframe outside the verifiable set,
    When: coverage is verified,
    Then: a ValueError naming it is raised.
    """
    empty_repo = _StubRepo({})
    with pytest.raises(ValueError, match="unverifiable"):
        await _verify(empty_repo, timeframes=["2m"])


def _settings(symbols: list[str]) -> Any:
    """Build a settings stub exposing instruments + db_url for the CLI."""
    return cast(
        Any,
        type(
            "Settings",
            (),
            {
                "instruments": {ExchangeEnum.POLYGON: list(symbols)},
                "db_url": "sqlite+aiosqlite:///:memory:",
            },
        )(),
    )


def _ok_report() -> CoverageReport:
    """Return a passing one-entry coverage report."""
    entry = CoverageEntry(
        symbol="FET-USD",
        timeframe="5m",
        ok=True,
        reason="ok",
        bars=30,
        oldest=_at(9, 0),
        newest=_at(9, 55),
    )
    return CoverageReport(entries=[entry], ok=True)


def _bad_report() -> CoverageReport:
    """Return a failing one-entry coverage report."""
    entry = CoverageEntry(
        symbol="FET-USD",
        timeframe="1d",
        ok=False,
        reason="missing bar at 2026-06-13T00:00:00+00:00",
        bars=2,
        oldest=None,
        newest=None,
    )
    return CoverageReport(entries=[entry], ok=False)


def test_cli_verify_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exit 0 and print PASS when coverage is OK.

    Given: a stubbed verifier returning an OK report,
    When: verify-candle-coverage runs with a symbol,
    Then: it exits 0 with a PASS line and the OK summary.
    """
    monkeypatch.setattr(app_module, "get_settings", lambda: _settings([]))
    monkeypatch.setattr(app_module, "get_repository", lambda url: _StubRepo({}))
    monkeypatch.setattr(app_module, "verify_candle_coverage", AsyncMock(return_value=_ok_report()))
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["verify-candle-coverage", "-e", "kraken", "--cut-date", "2026-06-15", "-s", "FET-USD"],
    )
    assert result.exit_code == 0
    assert "[PASS] FET-USD 5m" in result.stdout
    assert "Candle coverage OK" in result.stdout


def test_cli_verify_incomplete(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exit 1 and print FAIL when coverage is incomplete.

    Given: a stubbed verifier returning a failing report,
    When: verify-candle-coverage runs,
    Then: it exits 1 with a FAIL line and the INCOMPLETE summary.
    """
    monkeypatch.setattr(app_module, "get_settings", lambda: _settings([]))
    monkeypatch.setattr(app_module, "get_repository", lambda url: _StubRepo({}))
    monkeypatch.setattr(app_module, "verify_candle_coverage", AsyncMock(return_value=_bad_report()))
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["verify-candle-coverage", "-e", "kraken", "--cut-date", "2026-06-15", "-s", "FET-USD"],
    )
    assert result.exit_code == 1
    assert "[FAIL] FET-USD 1d" in result.stdout
    assert "INCOMPLETE" in result.stdout


def test_cli_verify_defaults_symbols_from_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    """Default symbols to the settings Polygon instruments when none are given.

    Given: settings listing an explicit Polygon symbol and a stubbed verifier,
    When: verify-candle-coverage runs without --symbol,
    Then: it verifies the settings symbol and exits 0.
    """
    captured: dict[str, Any] = {}

    async def _fake(**kwargs: Any) -> CoverageReport:
        captured.update(kwargs)
        return _ok_report()

    monkeypatch.setattr(app_module, "get_settings", lambda: _settings(["FET-USD"]))
    monkeypatch.setattr(app_module, "get_repository", lambda url: _StubRepo({}))
    monkeypatch.setattr(app_module, "verify_candle_coverage", _fake)
    runner = CliRunner()
    result = runner.invoke(
        app, ["verify-candle-coverage", "-e", "kraken", "--cut-date", "2026-06-15"]
    )
    assert result.exit_code == 0
    assert captured["native_symbols"] == ["FET-USD"]


def test_cli_verify_empty_symbols_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exit 1 when settings list no Polygon instruments and none are passed.

    Given: settings whose Polygon instruments are empty,
    When: verify-candle-coverage runs without --symbol,
    Then: it exits 1 asking for --symbol before opening a repository.
    """
    monkeypatch.setattr(app_module, "get_settings", lambda: _settings([]))
    runner = CliRunner()
    result = runner.invoke(
        app, ["verify-candle-coverage", "-e", "kraken", "--cut-date", "2026-06-15"]
    )
    assert result.exit_code == 1
    assert "pass --symbol" in result.stdout


def test_cli_verify_wildcard_symbols_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exit 1 when the only configured symbols are the wildcard sentinel.

    Given: settings whose Polygon instruments are the wildcard sentinel,
    When: verify-candle-coverage runs without --symbol,
    Then: it exits 1 asking for an explicit --symbol.
    """
    monkeypatch.setattr(app_module, "get_settings", lambda: _settings(["*"]))
    runner = CliRunner()
    result = runner.invoke(
        app, ["verify-candle-coverage", "-e", "kraken", "--cut-date", "2026-06-15"]
    )
    assert result.exit_code == 1
    assert "pass --symbol" in result.stdout


def test_cli_verify_custom_timeframes(monkeypatch: pytest.MonkeyPatch) -> None:
    """Forward explicit --timeframe values to the verifier.

    Given: a stubbed verifier capturing kwargs,
    When: verify-candle-coverage runs with --timeframe 1d,
    Then: only the requested timeframe is verified.
    """
    captured: dict[str, Any] = {}

    async def _fake(**kwargs: Any) -> CoverageReport:
        captured.update(kwargs)
        return _ok_report()

    monkeypatch.setattr(app_module, "get_settings", lambda: _settings([]))
    monkeypatch.setattr(app_module, "get_repository", lambda url: _StubRepo({}))
    monkeypatch.setattr(app_module, "verify_candle_coverage", _fake)
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "verify-candle-coverage",
            "-e",
            "kraken",
            "--cut-date",
            "2026-06-15",
            "-s",
            "FET-USD",
            "-t",
            "1d",
        ],
    )
    assert result.exit_code == 0
    assert captured["timeframes"] == ["1d"]


def test_cli_verify_disposes_engineless_repo(monkeypatch: pytest.MonkeyPatch) -> None:
    """Handle a repository exposing no engine without error.

    Given: a repository object with no ``engine`` attribute,
    When: verify-candle-coverage runs,
    Then: the finally block skips disposal cleanly and the command exits 0.
    """
    monkeypatch.setattr(app_module, "get_settings", lambda: _settings([]))
    monkeypatch.setattr(app_module, "get_repository", lambda url: object())
    monkeypatch.setattr(app_module, "verify_candle_coverage", AsyncMock(return_value=_ok_report()))
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["verify-candle-coverage", "-e", "kraken", "--cut-date", "2026-06-15", "-s", "FET-USD"],
    )
    assert result.exit_code == 0
    assert "Candle coverage OK" in result.stdout


def test_cli_verify_rejects_polygon() -> None:
    """Reject -e polygon before any DB access.

    Given: the command,
    When: it is invoked with --exchange polygon,
    Then: it exits 1 with a venue rejection message.
    """
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["verify-candle-coverage", "-e", "polygon", "--cut-date", "2026-06-15", "-s", "FET-USD"],
    )
    assert result.exit_code == 1
    assert "not a read venue" in result.stdout


def test_cli_verify_requires_exchange() -> None:
    """Reject invocation without the required --exchange.

    Given: the command,
    When: it is invoked with --cut-date and --symbol but no --exchange,
    Then: it exits non-zero (missing required option).
    """
    runner = CliRunner()
    result = runner.invoke(
        app, ["verify-candle-coverage", "--cut-date", "2026-06-15", "-s", "FET-USD"]
    )
    assert result.exit_code != 0
