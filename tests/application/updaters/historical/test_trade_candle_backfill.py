"""Tests for trade-based calculated candle backfill."""

from collections.abc import AsyncIterator
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from snapper.application.updaters.historical.trade_candle_backfill import TradeCandleBackfillService
from snapper.application.updaters.historical.trade_candle_backfill import _as_utc
from snapper.application.updaters.historical.trade_candle_backfill import _floor_minute
from snapper.application.updaters.historical.trade_candle_backfill import _utc_now
from snapper.cli.app import app
from snapper.config.settings import AppSettings
from snapper.core.types import ExchangeEnum
from snapper.data.repository_types import CandleUpsertRow
from snapper.data.repository_types import TradeRow

_BUS_TIME = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
_SERVICE_MODULE = "snapper.application.updaters.historical.trade_candle_backfill"


@dataclass
class FakeSettings:
    """Minimal settings object used by the service tests."""

    db_url: str = "sqlite+aiosqlite:///fake.db"
    instruments: dict[ExchangeEnum, list[str]] = field(
        default_factory=lambda: {ExchangeEnum.KRAKEN: ["BTC-USD"]}
    )


class FakeRepository:
    """Typed fake repository for trade candle backfill tests."""

    def __init__(
        self,
        active_symbols: list[str],
        instrument_ids: dict[str, str],
        trade_stream: list[tuple[str, TradeRow]],
        upsert_results: list[int] | None = None,
    ) -> None:
        """Initialize fake repository state.

        Args:
            active_symbols: Symbols returned by get_exchange_instruments.
            instrument_ids: Native-symbol to instrument-public-id mapping.
            trade_stream: Bulk trade stream keyed by instrument public ID.
            upsert_results: Optional changed-row counts returned by each upsert.
        """
        self.active_symbols = active_symbols
        self.instrument_ids = instrument_ids
        self.trade_stream = trade_stream
        self.upsert_results = list(upsert_results) if upsert_results else []
        self.exchange_calls: list[tuple[str, datetime]] = []
        self.instrument_id_calls: list[tuple[set[str], str, datetime]] = []
        self.iter_exchange_calls: list[
            tuple[str, datetime, datetime, datetime, list[str] | None]
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

    async def iter_exchange_trades(
        self,
        exchange: str,
        start: datetime,
        end: datetime,
        as_of: datetime,
        instrument_public_ids: list[str] | None = None,
    ) -> AsyncIterator[tuple[str, TradeRow]]:
        """Yield configured bulk trade rows for the requested instruments.

        Args:
            exchange: Exchange name.
            start: Inclusive event-time lower bound.
            end: Inclusive event-time upper bound.
            as_of: Point-in-time read timestamp.
            instrument_public_ids: Optional selected instrument IDs.

        Yields:
            Trade rows in configured order with instrument IDs.
        """
        self.iter_exchange_calls.append(
            (
                exchange,
                start,
                end,
                as_of,
                list(instrument_public_ids) if instrument_public_ids else None,
            )
        )
        selected_ids = set(instrument_public_ids) if instrument_public_ids is not None else set()
        for instrument_public_id, trade in self.trade_stream:
            if instrument_public_ids is None or instrument_public_id in selected_ids:
                yield instrument_public_id, trade

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


def _dt(minute: int, second: int = 0) -> datetime:
    """Build a fixed UTC test timestamp.

    Args:
        minute: Minute value within the test hour.
        second: Second value within the minute.

    Returns:
        UTC datetime.
    """
    return datetime(2024, 1, 1, 0, minute, second, tzinfo=UTC)


def _trade(
    event_time: datetime,
    price: float,
    size: float = 1.0,
    side: str = "buy",
    executed_at: datetime | None = None,
    trade_id: str | None = None,
) -> TradeRow:
    """Build a repository trade row.

    Args:
        event_time: Persisted timestamp.
        price: Trade price.
        size: Trade size.
        side: Trade side.
        executed_at: Optional exchange execution timestamp.
        trade_id: Optional exchange trade id.

    Returns:
        TradeRow fixture.
    """
    return {
        "timestamp": event_time,
        "executed_at": executed_at,
        "price": price,
        "size": size,
        "side": side,
        "trade_id": trade_id,
    }


def _make_service(
    symbols: list[str] | None,
    all_symbols: bool,
    start: datetime = _dt(0),
    end: datetime = _dt(3),
) -> TradeCandleBackfillService:
    """Create a service with patched settings.

    Args:
        symbols: Requested native symbols.
        all_symbols: Whether to backfill all active symbols.
        start: Window start.
        end: Window end.

    Returns:
        Configured service.
    """
    with patch(f"{_SERVICE_MODULE}.get_settings", return_value=FakeSettings()):
        return TradeCandleBackfillService(
            exchange=ExchangeEnum.KRAKEN,
            start=start,
            end=end,
            symbols=symbols,
            all_symbols=all_symbols,
            clock=lambda: _BUS_TIME,
        )


async def _run_service(service: TradeCandleBackfillService, repo: FakeRepository) -> None:
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


def test_time_helpers_and_default_parameters() -> None:
    """Time helpers normalize UTC and process defaults are valid.

    Given: Naive and aware datetimes plus fake settings,
    When: Helpers and default parameters are evaluated,
    Then: UTC normalization and a valid default window are produced.
    """
    naive = datetime(2024, 1, 1, 1, 2, 3)
    aware = datetime(2024, 1, 1, 1, 2, 3, tzinfo=UTC)
    settings = FakeSettings()
    params = TradeCandleBackfillService.get_default_parameters(cast(AppSettings, settings))
    now = _utc_now()
    assert _as_utc(naive) == datetime(2024, 1, 1, 1, 2, 3, tzinfo=UTC)
    assert _as_utc(aware) == aware
    assert _floor_minute(datetime(2024, 1, 1, 1, 2, 33, tzinfo=UTC)) == datetime(
        2024, 1, 1, 1, 2, tzinfo=UTC
    )
    assert now.tzinfo == UTC
    assert params["exchange"] == ExchangeEnum.KRAKEN
    assert params["all_symbols"] is True
    assert params["symbols"] == ["BTC-USD"]
    start_param = params["start"]
    end_param = params["end"]
    assert isinstance(start_param, datetime)
    assert isinstance(end_param, datetime)
    assert start_param < end_param


@pytest.mark.asyncio
async def test_empty_trade_window_flushes_no_rows() -> None:
    """A selected active symbol with no trades produces no upserts.

    Given: One active instrument and an empty trade stream,
    When: The service runs,
    Then: Trades are read but no candle rows are upserted.
    """
    repo = FakeRepository(["BTC-USD"], {"BTC-USD": "inst-btc"}, [])
    service = _make_service(["BTC-USD"], False)
    await _run_service(service, repo)
    assert repo.iter_exchange_calls == [("kraken", _dt(0), _dt(3), _BUS_TIME, ["inst-btc"])]
    assert repo.upsert_batches == []


@pytest.mark.asyncio
async def test_trailing_partial_only_flushes_no_rows() -> None:
    """A single trailing partial minute produces no candle rows.

    Given: One trade in the same minute as the floored window end,
    When: The service finalizes the instrument,
    Then: The pending builder state is not upserted as a complete candle.
    """
    repo = FakeRepository(
        ["BTC-USD"],
        {"BTC-USD": "inst-btc"},
        [("inst-btc", _trade(_dt(0, 1), 100.0))],
    )
    service = _make_service(["BTC-USD"], False, end=_dt(0, 30))
    await _run_service(service, repo)
    assert repo.iter_exchange_calls[0][4] == ["inst-btc"]
    assert repo.upsert_batches == []


@pytest.mark.asyncio
async def test_single_instrument_multiple_minutes_skips_trailing_partial() -> None:
    """A partial trailing minute is not emitted.

    Given: Trades across three minutes and a window ending inside the third,
    When: The service folds trades through TradeCandleBuilder,
    Then: Only the first two complete minutes are upserted.
    """
    repo = FakeRepository(
        ["BTC-USD"],
        {"BTC-USD": "inst-btc"},
        [
            ("inst-btc", _trade(_dt(0, 1), 100.0, executed_at=_dt(0, 5), trade_id="t1")),
            ("inst-btc", _trade(_dt(0, 30), 110.0, size=2.0, side="sell", trade_id="t2")),
            ("inst-btc", _trade(_dt(1, 5), 120.0, trade_id="t3")),
            ("inst-btc", _trade(_dt(2, 10), 90.0, trade_id="t4")),
        ],
    )
    service = _make_service(["BTC-USD"], False, end=_dt(2, 30))
    await _run_service(service, repo)
    rows = repo.upsert_batches[0]
    assert [row["open_at"] for row in rows] == [_dt(0), _dt(1)]
    assert rows[0]["instrument_public_id"] == "inst-btc"
    assert rows[0]["timeframe"] == "1m"
    assert rows[0]["open"] == 100.0
    assert rows[0]["high"] == 110.0
    assert rows[0]["low"] == 100.0
    assert rows[0]["close"] == 110.0
    assert rows[0]["volume"] == 3.0
    assert rows[0]["vwap"] == pytest.approx(320.0 / 3.0)
    assert rows[0]["trades"] == 2
    assert rows[0]["source"] == "calculated"
    assert rows[0]["complete"] is True
    assert rows[0]["timestamp"] == _BUS_TIME
    assert rows[1]["open"] == 120.0
    assert rows[1]["close"] == 120.0
    assert [row["sequence_id"] for row in rows] == [1, 2]
    assert isinstance(rows[0]["session_id"], str)


@pytest.mark.asyncio
async def test_all_symbols_processes_multiple_instruments_and_skips_unresolved() -> None:
    """All-symbol mode processes active instruments and skips unresolved rows.

    Given: Two active instruments and one symbol without an instrument row,
    When: The service runs in all-symbol mode,
    Then: Only resolved instruments are streamed and upserted.
    """
    repo = FakeRepository(
        ["BTC-USD", "ETH-USD", "ORPHAN-USD"],
        {"BTC-USD": "inst-btc", "ETH-USD": "inst-eth"},
        [
            ("inst-btc", _trade(_dt(0, 1), 100.0)),
            ("inst-eth", _trade(_dt(0, 2), 200.0)),
        ],
    )
    service = _make_service(None, True, end=_dt(2))
    await _run_service(service, repo)
    assert repo.iter_exchange_calls[0][4] == ["inst-btc", "inst-eth"]
    assert [len(batch) for batch in repo.upsert_batches] == [1, 1]
    assert [batch[0]["instrument_public_id"] for batch in repo.upsert_batches] == [
        "inst-btc",
        "inst-eth",
    ]


@pytest.mark.asyncio
async def test_requested_symbols_are_deduped_and_filtered_against_active() -> None:
    """Requested symbols preserve first-seen order without duplicates.

    Given: Duplicate requested symbols,
    When: The service resolves selected symbols,
    Then: The trade stream is opened once for that symbol.
    """
    repo = FakeRepository(["BTC-USD", "ETH-USD"], {"ETH-USD": "inst-eth"}, [])
    service = _make_service(["ETH-USD", "ETH-USD"], False)
    await _run_service(service, repo)
    assert repo.instrument_id_calls[0][0] == {"ETH-USD"}
    assert repo.iter_exchange_calls[0][4] == ["inst-eth"]


@pytest.mark.asyncio
async def test_batch_flushing_bounds_memory() -> None:
    """Completed candle rows are flushed when the batch reaches the limit.

    Given: Three complete candles and a batch limit of two,
    When: The service runs,
    Then: Upserts are split into two bounded batches.
    """
    repo = FakeRepository(
        ["BTC-USD"],
        {"BTC-USD": "inst-btc"},
        [
            ("inst-btc", _trade(_dt(0, 1), 100.0)),
            ("inst-btc", _trade(_dt(1, 1), 101.0)),
            ("inst-btc", _trade(_dt(2, 1), 102.0)),
        ],
    )
    service = _make_service(["BTC-USD"], False, end=_dt(4))
    service.BATCH_COMMIT_SIZE = 2
    await _run_service(service, repo)
    assert [len(batch) for batch in repo.upsert_batches] == [2, 1]
    assert [row["open_at"] for batch in repo.upsert_batches for row in batch] == [
        _dt(0),
        _dt(1),
        _dt(2),
    ]


@pytest.mark.asyncio
async def test_idempotent_upsert_noop_path_accepts_zero_changed_rows() -> None:
    """Repository no-op upserts are accepted as successful idempotent runs.

    Given: The repository returns zero changed rows,
    When: A candle is rebuilt and upserted,
    Then: The service completes without retrying or mutating the batch.
    """
    repo = FakeRepository(
        ["BTC-USD"],
        {"BTC-USD": "inst-btc"},
        [("inst-btc", _trade(_dt(0, 1), 100.0))],
        upsert_results=[0],
    )
    service = _make_service(["BTC-USD"], False, end=_dt(2))
    await _run_service(service, repo)
    assert len(repo.upsert_batches) == 1
    assert repo.upsert_batches[0][0]["open_at"] == _dt(0)


@pytest.mark.asyncio
async def test_start_returns_when_no_active_instruments() -> None:
    """An empty all-symbol selection returns without streaming trades.

    Given: The exchange has no active symbols,
    When: The service runs in all-symbol mode,
    Then: No trade streams or upserts are attempted.
    """
    repo = FakeRepository([], {}, [])
    service = _make_service(None, True)
    await _run_service(service, repo)
    assert repo.instrument_id_calls == []
    assert repo.iter_exchange_calls == []
    assert repo.upsert_batches == []


@pytest.mark.asyncio
async def test_requested_inactive_symbol_raises() -> None:
    """Requested symbols must be active on the exchange.

    Given: A requested symbol missing from the active exchange set,
    When: The service resolves instruments,
    Then: It raises a validation error.
    """
    repo = FakeRepository(["BTC-USD"], {"BTC-USD": "inst-btc"}, [])
    service = _make_service(["DOGE-USD"], False)
    with pytest.raises(ValueError, match="symbols are not active"):
        await _run_service(service, repo)


def test_constructor_validates_inputs() -> None:
    """Constructor validation rejects unsafe or invalid inputs.

    Given: Invalid exchange, window, and symbol-selection inputs,
    When: Services are constructed,
    Then: ValueError is raised for each invalid input.
    """
    with pytest.raises(ValueError):
        TradeCandleBackfillService(exchange="not-real", start=_dt(0), end=_dt(1), symbols=["BTC"])
    with patch(f"{_SERVICE_MODULE}.get_settings", return_value=FakeSettings()):
        with pytest.raises(ValueError, match="start must be before end"):
            TradeCandleBackfillService(
                exchange=ExchangeEnum.KRAKEN,
                start=_dt(1),
                end=_dt(1),
                symbols=["BTC"],
            )
        with pytest.raises(ValueError, match="pass all_symbols"):
            TradeCandleBackfillService(
                exchange=ExchangeEnum.KRAKEN,
                start=_dt(0),
                end=_dt(1),
            )


class TestCliCommand:
    """CLI command registration and execution tests."""

    def test_command_exists(self) -> None:
        """The trade candle backfill command is registered.

        Given: The Typer app,
        When: Registered commands are inspected,
        Then: backfill-candles-from-trades is present.
        """
        command_names = [cmd.name for cmd in app.registered_commands]
        assert "backfill-candles-from-trades" in command_names

    def test_cli_invokes_service(self) -> None:
        """CLI command creates and runs the service.

        Given: A valid symbol-scoped invocation,
        When: The command runs,
        Then: Service.start is awaited with parsed UTC dates.
        """
        runner = CliRunner()
        with patch("snapper.cli.app.TradeCandleBackfillService") as mock_cls:
            mock_service = MagicMock()
            mock_service.start = AsyncMock()
            mock_cls.return_value = mock_service
            result = runner.invoke(
                app,
                [
                    "backfill-candles-from-trades",
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
        assert result.exit_code == 0
        mock_cls.assert_called_once_with(
            exchange=ExchangeEnum.KRAKEN,
            start=datetime(2024, 1, 1, tzinfo=UTC),
            end=datetime(2024, 1, 2, tzinfo=UTC),
            symbols=["BTC-USD"],
            all_symbols=False,
        )
        mock_service.start.assert_awaited_once()

    def test_cli_handles_service_error(self) -> None:
        """CLI exits with code 1 when the service fails.

        Given: Service.start raises an exception,
        When: The command runs,
        Then: The error is surfaced and the process exits non-zero.
        """
        runner = CliRunner()
        with patch("snapper.cli.app.TradeCandleBackfillService") as mock_cls:
            mock_service = MagicMock()
            mock_service.start = AsyncMock(side_effect=RuntimeError("test error"))
            mock_cls.return_value = mock_service
            result = runner.invoke(
                app,
                [
                    "backfill-candles-from-trades",
                    "--exchange",
                    "kraken",
                    "--start",
                    "2024-01-01",
                    "--end",
                    "2024-01-02",
                    "--all",
                ],
            )
        assert result.exit_code == 1
        assert "test error" in result.output

    def test_cli_reports_validation_error(self) -> None:
        """CLI exits with code 1 on invalid date input.

        Given: A malformed start date,
        When: The command runs,
        Then: The validation error is printed.
        """
        runner = CliRunner()
        result = runner.invoke(
            app,
            [
                "backfill-candles-from-trades",
                "--exchange",
                "kraken",
                "--start",
                "not-a-date",
                "--end",
                "2024-01-02",
                "--symbol",
                "BTC-USD",
            ],
        )
        assert result.exit_code == 1
        assert "Invalid isoformat" in result.output
