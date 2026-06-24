"""Tests for synthesized higher-timeframe candle backfill."""

import json
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import date
from datetime import datetime
from datetime import timedelta
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from snapper.application.updaters.historical.synthesized_candle_backfill import (
    SynthesizedCandleBackfillService,
)
from snapper.application.updaters.historical.synthesized_candle_backfill import _as_utc
from snapper.application.updaters.historical.synthesized_candle_backfill import _cut_datetime
from snapper.application.updaters.historical.synthesized_candle_backfill import _floor_minute
from snapper.application.updaters.historical.synthesized_candle_backfill import (
    _normalize_timeframes,
)
from snapper.application.updaters.historical.synthesized_candle_backfill import _utc_now
from snapper.cli.app import app
from snapper.config.settings import AppSettings
from snapper.core.types import ExchangeEnum
from snapper.data.repository_types import CandleRow
from snapper.data.repository_types import CandleUpsertRow
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.messaging.publishers.candle_aggregator import CandleAggregator

_BUS_TIME = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
_BASE = datetime(2024, 1, 1, tzinfo=UTC)
_SERVICE_MODULE = "snapper.application.updaters.historical.synthesized_candle_backfill"


@dataclass
class FakeSettings:
    """Minimal settings object used by the service tests."""

    db_url: str = "sqlite+aiosqlite:///fake.db"
    instruments: dict[ExchangeEnum, list[str]] = field(
        default_factory=lambda: {ExchangeEnum.KRAKEN: ["BTC-USD"]}
    )


class FakeRepository:
    """Typed fake repository for synthesized candle backfill tests."""

    def __init__(
        self,
        active_symbols: list[str],
        instrument_ids: dict[str, str],
        candles_by_symbol: dict[str, list[CandleRow]],
        upsert_results: list[int] | None = None,
    ) -> None:
        """Initialize fake repository state.

        Args:
            active_symbols: Symbols returned by get_exchange_instruments.
            instrument_ids: Native-symbol to instrument-public-id mapping.
            candles_by_symbol: Persisted 1m candle rows keyed by native symbol.
            upsert_results: Optional changed-row counts returned by each upsert.
        """
        self.active_symbols = active_symbols
        self.instrument_ids = instrument_ids
        self.candles_by_symbol = candles_by_symbol
        self.upsert_results = list(upsert_results) if upsert_results else []
        self.exchange_calls: list[tuple[str, datetime]] = []
        self.instrument_id_calls: list[tuple[set[str], str, datetime]] = []
        self.candle_calls: list[
            tuple[
                str,
                str,
                datetime | None,
                datetime | None,
                ExchangeEnum,
                datetime,
                int | None,
                str,
            ]
        ] = []
        self.upsert_batches: list[list[CandleUpsertRow]] = []
        self.upsert_sessions: list[object | None] = []

    async def get_exchange_instruments(self, exchange: str, as_of: datetime) -> list[str]:
        """Return configured active symbols.

        Args:
            exchange: Exchange name.
            as_of: Point-in-time read timestamp.

        Returns:
            Active symbols.
        """
        self.exchange_calls.append((exchange, as_of))
        return list(self.active_symbols)

    async def get_instrument_public_ids_by_symbols(
        self,
        native_symbols: set[str],
        exchange: str,
        as_of: datetime,
    ) -> dict[str, str]:
        """Resolve configured instrument IDs.

        Args:
            native_symbols: Native symbols to resolve.
            exchange: Exchange name.
            as_of: Point-in-time read timestamp.

        Returns:
            Mapping for configured symbols.
        """
        self.instrument_id_calls.append((set(native_symbols), exchange, as_of))
        return {
            symbol: self.instrument_ids[symbol]
            for symbol in native_symbols
            if symbol in self.instrument_ids
        }

    async def get_candles(
        self,
        instrument: str,
        timeframe: str,
        start: datetime | None,
        end: datetime | None,
        exchange: ExchangeEnum | str,
        as_of: datetime,
        limit: int | None = None,
        order: str = "asc",
    ) -> list[CandleRow]:
        """Return configured 1m candles for the requested instrument.

        Args:
            instrument: Native symbol.
            timeframe: Candle timeframe.
            start: Inclusive lower ``open_at`` bound.
            end: Inclusive upper ``open_at`` bound.
            exchange: Exchange name.
            as_of: Point-in-time read timestamp.
            limit: Optional result cap.
            order: Sort order.

        Returns:
            Matching candle rows.
        """
        self.candle_calls.append(
            (instrument, timeframe, start, end, ExchangeEnum(exchange), as_of, limit, order)
        )
        rows = [
            row
            for row in self.candles_by_symbol.get(instrument, [])
            if (start is None or row["open_at"] >= start) and (end is None or row["open_at"] <= end)
        ]
        rows.sort(key=lambda row: row["open_at"], reverse=order == "desc")
        return rows[:limit] if limit is not None else rows

    async def upsert_candles(
        self,
        rows: list[CandleUpsertRow],
        session: object | None = None,
    ) -> int:
        """Record upsert batches and return configured counts.

        Args:
            rows: Candle rows to upsert.
            session: Unused caller-owned session placeholder.

        Returns:
            Changed-row count.
        """
        self.upsert_batches.append(list(rows))
        self.upsert_sessions.append(session)
        if self.upsert_results:
            return self.upsert_results.pop(0)
        return len(rows)


def _dt(minutes: int) -> datetime:
    """Build a UTC timestamp offset from the test base.

    Args:
        minutes: Minute offset.

    Returns:
        UTC datetime.
    """
    return _BASE + timedelta(minutes=minutes)


def _day(day: int) -> datetime:
    """Build a UTC day boundary in January 2024.

    Args:
        day: Day of month.

    Returns:
        UTC datetime.
    """
    return datetime(2024, 1, day, tzinfo=UTC)


def _row(
    open_at: datetime,
    price: float,
    volume: float = 1.0,
    vwap: float | None = None,
    trades: int | None = 1,
) -> CandleRow:
    """Build a repository 1m candle row.

    Args:
        open_at: Candle ``open_at`` timestamp.
        price: Base price used to derive OHLC values.
        volume: Candle volume.
        vwap: Optional persisted VWAP.
        trades: Optional persisted trade count.

    Returns:
        CandleRow fixture.
    """
    sequence_id = int(open_at.timestamp())
    return {
        "open_at": open_at,
        "timeframe": "1m",
        "open": price,
        "high": price + 1.0,
        "low": price - 1.0,
        "close": price + 0.5,
        "volume": volume,
        "vwap": vwap,
        "trades": trades,
        "source": "calculated",
        "complete": True,
        "public_id": f"candle-{sequence_id}",
        "timestamp": open_at,
        "session_id": "source-session",
        "sequence_id": sequence_id,
    }


def _rows(start_minute: int, end_minute: int, base_price: float = 100.0) -> list[CandleRow]:
    """Build contiguous 1m candle rows.

    Args:
        start_minute: Inclusive start minute offset.
        end_minute: Exclusive end minute offset.
        base_price: Price assigned to the first minute.

    Returns:
        Contiguous candle rows.
    """
    return [
        _row(_dt(minute), base_price + float(minute - start_minute))
        for minute in range(start_minute, end_minute)
    ]


def _make_service(
    symbols: list[str] | None,
    all_symbols: bool,
    start: datetime = _dt(0),
    end: datetime = _dt(15),
    timeframes: list[str] | None = None,
    cut_date: date | None = None,
) -> SynthesizedCandleBackfillService:
    """Create a service with patched settings.

    Args:
        symbols: Requested native symbols.
        all_symbols: Whether to backfill all active symbols.
        start: Window start.
        end: Window end.
        timeframes: Higher timeframes to synthesize.
        cut_date: Daily ownership seam.

    Returns:
        Configured service.
    """
    with patch(f"{_SERVICE_MODULE}.get_settings", return_value=FakeSettings()):
        return SynthesizedCandleBackfillService(
            exchange=ExchangeEnum.KRAKEN,
            start=start,
            end=end,
            symbols=symbols,
            all_symbols=all_symbols,
            timeframes=timeframes if timeframes is not None else ["5m"],
            cut_date=cut_date,
            clock=lambda: _BUS_TIME,
        )


async def _run_service(
    service: SynthesizedCandleBackfillService,
    repo: FakeRepository,
) -> None:
    """Run a service against the fake repository.

    Args:
        service: Service to run.
        repo: Fake repository returned by get_repository.
    """
    with (
        patch(f"{_SERVICE_MODULE}.get_repository", return_value=repo),
        patch(f"{_SERVICE_MODULE}.set_log_context"),
    ):
        await service.start()


def test_time_helpers_timeframe_validation_and_default_parameters() -> None:
    """Helpers normalize UTC and process defaults describe a backfill window.

    Given: Naive and aware datetimes plus fake settings,
    When: Helpers and default parameters are evaluated,
    Then: UTC normalization and a valid default window are produced.
    """
    naive = datetime(2024, 1, 1, 1, 2, 3)
    aware = datetime(2024, 1, 1, 1, 2, 3, tzinfo=UTC)
    settings = FakeSettings()
    params = SynthesizedCandleBackfillService.get_default_parameters(cast(AppSettings, settings))
    now = _utc_now()
    assert _as_utc(naive) == datetime(2024, 1, 1, 1, 2, 3, tzinfo=UTC)
    assert _as_utc(aware) == aware
    assert _floor_minute(datetime(2024, 1, 1, 1, 2, 33, tzinfo=UTC)) == datetime(
        2024, 1, 1, 1, 2, tzinfo=UTC
    )
    assert _cut_datetime(date(2024, 1, 3)) == datetime(2024, 1, 3, tzinfo=UTC)
    assert _normalize_timeframes(["5m", "5m", " 15m "]) == ["5m", "15m"]
    with pytest.raises(ValueError, match="at least one timeframe"):
        _normalize_timeframes([" "])
    with pytest.raises(ValueError, match="unsupported synthesis timeframes"):
        _normalize_timeframes(["5m", "2h"])
    assert now.tzinfo == UTC
    assert params["exchange"] == ExchangeEnum.KRAKEN
    assert params["all_symbols"] is True
    assert params["symbols"] == ["BTC-USD"]
    assert params["timeframes"] == ["5m", "15m", "30m", "1h", "4h", "1d"]
    assert params["cut_date"] is None
    start_param = params["start"]
    end_param = params["end"]
    assert isinstance(start_param, str)
    assert isinstance(end_param, str)
    assert datetime.fromisoformat(start_param) < datetime.fromisoformat(end_param)
    json.dumps(params)


@pytest.mark.asyncio
async def test_empty_window_flushes_no_rows() -> None:
    """A selected active symbol with no 1m rows produces no upserts.

    Given: One active instrument and an empty 1m candle range,
    When: The service runs,
    Then: Candles are read but no rows are upserted.
    """
    repo = FakeRepository(["BTC-USD"], {"BTC-USD": "inst-btc"}, {})
    service = _make_service(["BTC-USD"], False)
    await _run_service(service, repo)
    assert [call[0] for call in repo.candle_calls] == ["BTC-USD"]
    assert repo.upsert_batches == []


@pytest.mark.asyncio
async def test_single_instrument_emits_multiple_timeframes_from_1m_plane() -> None:
    """A contiguous 1m plane emits 5m and 15m synthesized rows.

    Given: Fifteen 1m candles for one instrument,
    When: The service folds them through CandleAggregator,
    Then: 5m and 15m rows are upserted with synthesized provenance.
    """
    rows = _rows(0, 15)
    rows[0]["vwap"] = None
    rows[0]["trades"] = None
    repo = FakeRepository(["BTC-USD"], {"BTC-USD": "inst-btc"}, {"BTC-USD": rows})
    service = _make_service(["BTC-USD"], False, timeframes=["5m", "15m"])
    await _run_service(service, repo)
    upserts = [row for batch in repo.upsert_batches for row in batch]
    assert [row["timeframe"] for row in upserts] == ["5m", "5m", "15m", "5m"]
    assert [row["open_at"] for row in upserts] == [_dt(0), _dt(5), _dt(0), _dt(10)]
    first = upserts[0]
    assert first["instrument_public_id"] == "inst-btc"
    assert first["open"] == 100.0
    assert first["high"] == 105.0
    assert first["low"] == 99.0
    assert first["close"] == 104.5
    assert first["volume"] == 5.0
    assert first["vwap"] == pytest.approx((100.5 + 101.5 + 102.5 + 103.5 + 104.5) / 5.0)
    assert first["trades"] == 4
    assert first["source"] == "synthesized"
    assert first["complete"] is True
    assert first["timestamp"] == _BUS_TIME
    assert [row["sequence_id"] for row in upserts] == [1, 2, 3, 4]
    assert isinstance(first["session_id"], str)
    assert repo.candle_calls[0][1:] == (
        "1m",
        _dt(0),
        _dt(15),
        ExchangeEnum.KRAKEN,
        _BUS_TIME,
        None,
        "asc",
    )


@pytest.mark.asyncio
async def test_all_symbols_processes_multiple_instruments_and_skips_unresolved() -> None:
    """All-symbol mode processes active instruments and skips unresolved rows.

    Given: Two active instruments and one symbol without an instrument row,
    When: The service runs in all-symbol mode,
    Then: Only resolved instruments are read and upserted.
    """
    repo = FakeRepository(
        ["BTC-USD", "ETH-USD", "ORPHAN-USD"],
        {"BTC-USD": "inst-btc", "ETH-USD": "inst-eth"},
        {
            "BTC-USD": _rows(0, 5, 100.0),
            "ETH-USD": _rows(0, 5, 200.0),
        },
    )
    service = _make_service(None, True, end=_dt(5))
    await _run_service(service, repo)
    assert [call[0] for call in repo.candle_calls] == ["BTC-USD", "ETH-USD"]
    assert [batch[0]["instrument_public_id"] for batch in repo.upsert_batches] == [
        "inst-btc",
        "inst-eth",
    ]


@pytest.mark.asyncio
async def test_requested_symbols_are_deduped_and_filtered_against_active() -> None:
    """Requested symbols preserve first-seen order without duplicates.

    Given: Duplicate requested symbols,
    When: The service resolves selected symbols,
    Then: The 1m candle range is opened once for that symbol.
    """
    repo = FakeRepository(["BTC-USD", "ETH-USD"], {"ETH-USD": "inst-eth"}, {})
    service = _make_service(["ETH-USD", "ETH-USD"], False)
    await _run_service(service, repo)
    assert repo.instrument_id_calls[0][0] == {"ETH-USD"}
    assert [call[0] for call in repo.candle_calls] == ["ETH-USD"]


@pytest.mark.asyncio
async def test_no_active_instruments_returns_without_reads() -> None:
    """An empty all-symbol selection returns without reading candles.

    Given: The exchange has no active symbols,
    When: The service runs in all-symbol mode,
    Then: No instrument-id lookup, candle read, or upsert is attempted.
    """
    repo = FakeRepository([], {}, {})
    service = _make_service(None, True)
    await _run_service(service, repo)
    assert repo.instrument_id_calls == []
    assert repo.candle_calls == []
    assert repo.upsert_batches == []


@pytest.mark.asyncio
async def test_requested_inactive_symbol_raises() -> None:
    """Requested symbols must be active on the exchange.

    Given: A requested symbol missing from the active exchange set,
    When: The service resolves instruments,
    Then: It raises a validation error.
    """
    repo = FakeRepository(["BTC-USD"], {"BTC-USD": "inst-btc"}, {})
    service = _make_service(["DOGE-USD"], False)
    with pytest.raises(ValueError, match="symbols are not active"):
        await _run_service(service, repo)


@pytest.mark.asyncio
async def test_start_after_window_open_skips_partial_emitted_window() -> None:
    """A higher-TF window that opens before the requested start is skipped.

    Given: The read window starts one minute into a 5m bucket,
    When: The trailing flush emits that bucket,
    Then: The service does not upsert it.
    """
    repo = FakeRepository(
        ["BTC-USD"],
        {"BTC-USD": "inst-btc"},
        {"BTC-USD": _rows(1, 5)},
    )
    service = _make_service(["BTC-USD"], False, start=_dt(1), end=_dt(5))
    await _run_service(service, repo)
    assert repo.upsert_batches == []


@pytest.mark.asyncio
async def test_flush_skips_empty_forward_filled_windows() -> None:
    """Flush-created empty windows are not persisted by the backfill.

    Given: One real 5m window followed by an empty 5m gap,
    When: The end flush closes both windows through CandleAggregator,
    Then: Only the window backed by source 1m rows is upserted.
    """
    repo = FakeRepository(
        ["BTC-USD"],
        {"BTC-USD": "inst-btc"},
        {"BTC-USD": _rows(0, 5)},
    )
    service = _make_service(["BTC-USD"], False, end=_dt(10))
    await _run_service(service, repo)
    upserts = [row for batch in repo.upsert_batches for row in batch]
    assert [row["open_at"] for row in upserts] == [_dt(0)]


@pytest.mark.asyncio
async def test_daily_cut_date_filter_skips_pre_cut_rows() -> None:
    """Synthesized 1d rows before cut_date are never written.

    Given: Daily windows before and at the cut date,
    When: The service synthesizes 1d candles,
    Then: Only the cut-date-and-later daily row is upserted.
    """
    repo = FakeRepository(
        ["BTC-USD"],
        {"BTC-USD": "inst-btc"},
        {"BTC-USD": [_row(_day(2), 100.0), _row(_day(3), 110.0)]},
    )
    service = _make_service(
        ["BTC-USD"],
        False,
        start=_day(2),
        end=_day(4),
        timeframes=["1d"],
        cut_date=date(2024, 1, 3),
    )
    await _run_service(service, repo)
    upserts = [row for batch in repo.upsert_batches for row in batch]
    assert [row["open_at"] for row in upserts] == [_day(3)]
    assert upserts[0]["timeframe"] == "1d"


@pytest.mark.asyncio
async def test_batch_flushing_bounds_memory() -> None:
    """Completed candle rows are flushed when the batch reaches the limit.

    Given: Three synthesized candles and a batch limit of two,
    When: The service runs,
    Then: Upserts are split into two bounded batches.
    """
    repo = FakeRepository(
        ["BTC-USD"],
        {"BTC-USD": "inst-btc"},
        {"BTC-USD": _rows(0, 15)},
    )
    service = _make_service(["BTC-USD"], False, end=_dt(15))
    service.BATCH_COMMIT_SIZE = 2
    await _run_service(service, repo)
    assert [len(batch) for batch in repo.upsert_batches] == [2, 1]
    assert [row["open_at"] for batch in repo.upsert_batches for row in batch] == [
        _dt(0),
        _dt(5),
        _dt(10),
    ]


@pytest.mark.asyncio
async def test_idempotent_upsert_noop_path_accepts_zero_changed_rows() -> None:
    """Repository no-op upserts are accepted as successful idempotent runs.

    Given: The repository returns zero changed rows,
    When: A synthesized candle is rebuilt and upserted,
    Then: The service completes without retrying or mutating the batch.
    """
    repo = FakeRepository(
        ["BTC-USD"],
        {"BTC-USD": "inst-btc"},
        {"BTC-USD": _rows(0, 5)},
        upsert_results=[0],
    )
    service = _make_service(["BTC-USD"], False, end=_dt(5))
    await _run_service(service, repo)
    assert len(repo.upsert_batches) == 1
    assert repo.upsert_batches[0][0]["open_at"] == _dt(0)


def test_constructor_validates_inputs() -> None:
    """Constructor validation rejects unsafe or invalid inputs.

    Given: Invalid exchange, window, symbol, timeframe, and cut-date inputs,
    When: Services are constructed,
    Then: ValueError is raised for each invalid input.
    """
    with pytest.raises(ValueError):
        SynthesizedCandleBackfillService(
            exchange="not-real",
            start=_dt(0),
            end=_dt(1),
            symbols=["BTC"],
            timeframes=["5m"],
        )
    with patch(f"{_SERVICE_MODULE}.get_settings", return_value=FakeSettings()):
        with pytest.raises(ValueError, match="start must be before end"):
            SynthesizedCandleBackfillService(
                exchange=ExchangeEnum.KRAKEN,
                start=_dt(1),
                end=_dt(1),
                symbols=["BTC"],
                timeframes=["5m"],
            )
        with pytest.raises(ValueError, match="pass all_symbols"):
            SynthesizedCandleBackfillService(
                exchange=ExchangeEnum.KRAKEN,
                start=_dt(0),
                end=_dt(1),
                timeframes=["5m"],
            )
        with pytest.raises(ValueError, match="cut_date is required"):
            SynthesizedCandleBackfillService(
                exchange=ExchangeEnum.KRAKEN,
                start=_dt(0),
                end=_dt(1),
                symbols=["BTC"],
                timeframes=["1d"],
            )
        with pytest.raises(ValueError, match="unsupported synthesis timeframes"):
            SynthesizedCandleBackfillService(
                exchange=ExchangeEnum.KRAKEN,
                start=_dt(0),
                end=_dt(1),
                symbols=["BTC"],
                timeframes=["2h"],
            )


def test_record_observed_windows_ignores_unconfigured_timeframe() -> None:
    """Observed-window recording tolerates an aggregator without a timeframe.

    Given: A service whose timeframe list was narrowed after construction,
    When: An unconfigured timeframe is inspected,
    Then: No observed window is recorded for that timeframe.
    """
    service = _make_service(["BTC-USD"], False, timeframes=["5m"])
    service._timeframes = ["5m", "15m"]
    aggregator = CandleAggregator(["5m"])
    observed_windows: set[tuple[str, datetime]] = set()
    service._record_observed_windows(
        aggregator,
        CandleUpdate(
            symbol="BTC-USD",
            open=100.0,
            high=100.0,
            low=100.0,
            close=100.0,
            vwap=100.0,
            trades=1,
            volume=1.0,
            interval_begin=_dt(0),
            interval=60,
        ),
        observed_windows,
    )
    assert observed_windows == {("5m", _dt(0))}


def test_should_upsert_rejects_emitted_window_before_start() -> None:
    """Write filtering rejects an emitted higher-TF window before start.

    Given: A service whose window starts after the emitted candle open time,
    When: The write predicate is evaluated,
    Then: The emitted candle is rejected even if it was observed.
    """
    service = _make_service(["BTC-USD"], False, start=_dt(1), end=_dt(5))
    candle = CandleUpdate(
        symbol="BTC-USD",
        open=100.0,
        high=100.0,
        low=100.0,
        close=100.0,
        vwap=100.0,
        trades=1,
        volume=1.0,
        interval_begin=_dt(0),
        interval=300,
    )
    assert service._should_upsert("5m", candle, {("5m", _dt(0))}) is False


class TestCliCommand:
    """CLI command registration and execution tests."""

    def test_command_exists(self) -> None:
        """The synthesized candle backfill command is registered.

        Given: The Typer app,
        When: Registered commands are inspected,
        Then: backfill-synthesized-candles is present.
        """
        command_names = [cmd.name for cmd in app.registered_commands]
        assert "backfill-synthesized-candles" in command_names

    def test_cli_invokes_service_with_cut_date(self) -> None:
        """CLI command creates and runs the service with a daily cut date.

        Given: A valid symbol-scoped invocation,
        When: The command runs,
        Then: Service.start is awaited with parsed UTC dates and timeframes.
        """
        runner = CliRunner()
        with patch("snapper.cli.app.SynthesizedCandleBackfillService") as mock_cls:
            mock_service = MagicMock()
            mock_service.start = AsyncMock()
            mock_cls.return_value = mock_service
            result = runner.invoke(
                app,
                [
                    "backfill-synthesized-candles",
                    "--exchange",
                    "kraken",
                    "--start",
                    "2024-01-01",
                    "--end",
                    "2024-01-02",
                    "--symbol",
                    "BTC-USD",
                    "--timeframes",
                    "5m,1d",
                    "--cut-date",
                    "2024-01-01",
                ],
            )
        assert result.exit_code == 0
        mock_cls.assert_called_once_with(
            exchange=ExchangeEnum.KRAKEN,
            start=datetime(2024, 1, 1, tzinfo=UTC),
            end=datetime(2024, 1, 2, tzinfo=UTC),
            symbols=["BTC-USD"],
            all_symbols=False,
            timeframes=["5m", "1d"],
            cut_date=date(2024, 1, 1),
        )
        mock_service.start.assert_awaited_once()

    def test_cli_invokes_service_without_cut_date_for_intraday_only(self) -> None:
        """CLI command allows omitting cut-date when 1d is not requested.

        Given: A valid all-symbol invocation for 5m only,
        When: The command runs,
        Then: Service.start is awaited with no cut date.
        """
        runner = CliRunner()
        with patch("snapper.cli.app.SynthesizedCandleBackfillService") as mock_cls:
            mock_service = MagicMock()
            mock_service.start = AsyncMock()
            mock_cls.return_value = mock_service
            result = runner.invoke(
                app,
                [
                    "backfill-synthesized-candles",
                    "--exchange",
                    "kraken",
                    "--start",
                    "2024-01-01T00:00:00+00:00",
                    "--end",
                    "2024-01-01T01:00:00+00:00",
                    "--all",
                    "--timeframes",
                    "5m",
                ],
            )
        assert result.exit_code == 0
        mock_cls.assert_called_once_with(
            exchange=ExchangeEnum.KRAKEN,
            start=datetime(2024, 1, 1, tzinfo=UTC),
            end=datetime(2024, 1, 1, 1, tzinfo=UTC),
            symbols=None,
            all_symbols=True,
            timeframes=["5m"],
            cut_date=None,
        )
        mock_service.start.assert_awaited_once()

    def test_cli_handles_service_error(self) -> None:
        """CLI exits with code 1 when the service fails.

        Given: Service.start raises an exception,
        When: The command runs,
        Then: The error is surfaced and the process exits non-zero.
        """
        runner = CliRunner()
        with patch("snapper.cli.app.SynthesizedCandleBackfillService") as mock_cls:
            mock_service = MagicMock()
            mock_service.start = AsyncMock(side_effect=RuntimeError("test error"))
            mock_cls.return_value = mock_service
            result = runner.invoke(
                app,
                [
                    "backfill-synthesized-candles",
                    "--exchange",
                    "kraken",
                    "--start",
                    "2024-01-01",
                    "--end",
                    "2024-01-02",
                    "--all",
                    "--timeframes",
                    "5m",
                ],
            )
        assert result.exit_code == 1
        assert "test error" in result.output

    def test_cli_reports_missing_cut_date_for_daily_default(self) -> None:
        """CLI exits with code 1 when 1d is requested without cut-date.

        Given: The default timeframe list includes 1d,
        When: The command runs without cut-date,
        Then: The service validation error is printed.
        """
        runner = CliRunner()
        result = runner.invoke(
            app,
            [
                "backfill-synthesized-candles",
                "--exchange",
                "kraken",
                "--start",
                "2024-01-01",
                "--end",
                "2024-01-02",
                "--symbol",
                "BTC-USD",
            ],
        )
        assert result.exit_code == 1
        assert "cut_date is required" in result.output

    def test_cli_reports_invalid_cut_date(self) -> None:
        """CLI exits with code 1 on invalid cut-date input.

        Given: A malformed cut date,
        When: The command runs,
        Then: The validation error is printed.
        """
        runner = CliRunner()
        result = runner.invoke(
            app,
            [
                "backfill-synthesized-candles",
                "--exchange",
                "kraken",
                "--start",
                "2024-01-01",
                "--end",
                "2024-01-02",
                "--symbol",
                "BTC-USD",
                "--cut-date",
                "not-a-date",
            ],
        )
        assert result.exit_code == 1
        assert "Invalid isoformat" in result.output
