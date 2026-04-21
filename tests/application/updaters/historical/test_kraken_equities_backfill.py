"""Tests for Kraken Equities (TradFi FCM) OHLCV backfill service + CLI."""

from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from snapper.application.updaters.historical.kraken_equities_aggregates import (
    KrakenEquitiesAggregatesBackfillService,
)
from snapper.cli.app import app
from snapper.config.settings import AppSettings
from snapper.core.types import ExchangeEnum
from snapper.infrastructure.exchanges.contracts import OhlcvSnapshot


@pytest.fixture()
def service() -> KrakenEquitiesAggregatesBackfillService:
    """Create a backfill service with default parameters."""
    return KrakenEquitiesAggregatesBackfillService(
        symbols=["MNQM6-CME"],
        timeframe="1h",
        days_back=7,
        resume=False,
    )


class TestInit:
    """Tests for service initialization."""

    def test_default_parameters(self) -> None:
        """``get_default_parameters`` returns settings-based defaults.

        Given: AppSettings with KRAKEN_EQUITIES instruments,
        When: ``get_default_parameters`` is called,
        Then: returns expected defaults (1h timeframe, 30 days_back).
        """
        mock_settings = MagicMock(spec=AppSettings)
        mock_settings.instruments = {ExchangeEnum.KRAKEN_EQUITIES: ["MNQM6-CME"]}
        params = KrakenEquitiesAggregatesBackfillService.get_default_parameters(mock_settings)
        assert params["symbols"] == ["MNQM6-CME"]
        assert params["timeframe"] == "1h"
        assert params["days_back"] == 30
        assert params["resume"] is True

    def test_invalid_timeframe_raises_eagerly(self) -> None:
        """Unsupported timeframes fail at construction time.

        Given: a timeframe not in ``_TIMEFRAME_TO_INTERVAL``,
        When: the service is instantiated,
        Then: ``ValueError`` is raised eagerly (no async work needed).
        """
        with pytest.raises(ValueError, match="Unsupported Kraken Equities timeframe"):
            KrakenEquitiesAggregatesBackfillService(timeframe="2w")


class TestResolveSymbols:
    """Tests for symbol resolution."""

    def test_explicit_symbols(self, service: KrakenEquitiesAggregatesBackfillService) -> None:
        """Explicit symbols are returned directly.

        Given: a service configured with explicit symbols,
        When: ``_resolve_symbols`` is called,
        Then: those symbols are returned verbatim.
        """
        assert service._resolve_symbols() == ["MNQM6-CME"]

    def test_settings_fallback(self) -> None:
        """No explicit symbols falls back to settings.instruments.

        Given: a service with no explicit symbols and all_symbols=False,
        When: ``_resolve_symbols`` is called,
        Then: falls back to ``settings.instruments[KRAKEN_EQUITIES]``.
        """
        svc = KrakenEquitiesAggregatesBackfillService(symbols=[], all_symbols=False)
        svc.settings = MagicMock(spec=AppSettings)
        svc.settings.instruments = {ExchangeEnum.KRAKEN_EQUITIES: ["MESM6-CME"]}
        assert svc._resolve_symbols() == ["MESM6-CME"]

    def test_all_symbols_queries_mapper(self) -> None:
        """``all_symbols=True`` delegates to the native symbol mapper.

        Given: a service with all_symbols=True,
        When: ``_resolve_symbols`` is called,
        Then: ``get_available_kraken_equities_symbols`` output is returned.
        """
        svc = KrakenEquitiesAggregatesBackfillService(all_symbols=True)
        with patch(
            "snapper.application.updaters.historical.kraken_equities_aggregates"
            ".get_available_kraken_equities_symbols",
            return_value=["MNQM6-CME", "MESM6-CME"],
        ):
            assert svc._resolve_symbols() == ["MNQM6-CME", "MESM6-CME"]


class TestBuildCandleRows:
    """Tests for OhlcvSnapshot to CandleUpsertRow conversion."""

    def test_builds_correct_rows(self) -> None:
        """Candle rows map OhlcvSnapshot fields to the upsert dict shape.

        Given: a single OhlcvSnapshot,
        When: ``_build_candle_rows`` is called,
        Then: one CandleUpsertRow is produced with exchange-agnostic fields
            (instrument_public_id, timeframe, prices, session/sequence).
        """
        candle = OhlcvSnapshot(
            timestamp=1700000000.0,
            open=23951.0,
            high=24000.0,
            low=23900.0,
            close=23975.0,
            volume=1234.0,
        )
        seq_counter = iter(range(100))
        rows = KrakenEquitiesAggregatesBackfillService._build_candle_rows(
            [candle],
            "inst-001",
            "1h",
            "sess-001",
            lambda: next(seq_counter),
        )
        assert len(rows) == 1
        row = rows[0]
        assert row["instrument_public_id"] == "inst-001"
        assert row["timeframe"] == "1h"
        assert row["open"] == pytest.approx(23951.0)
        assert row["close"] == pytest.approx(23975.0)
        assert row["volume"] == pytest.approx(1234.0)
        assert row["vwap"] is None
        assert row["trades"] is None
        assert row["session_id"] == "sess-001"

    def test_empty_candles_returns_empty(self) -> None:
        """Empty input produces empty output.

        Given: an empty candle list,
        When: ``_build_candle_rows`` is called,
        Then: an empty list is returned.
        """
        assert (
            KrakenEquitiesAggregatesBackfillService._build_candle_rows(
                [],
                "inst-001",
                "1h",
                "sess-001",
                lambda: 0,
            )
            == []
        )


class TestProcessSymbol:
    """Tests for single-symbol backfill processing."""

    @pytest.mark.asyncio
    async def test_skips_symbol_without_instrument_row(
        self, service: KrakenEquitiesAggregatesBackfillService
    ) -> None:
        """Skip symbols without an active Symbol row.

        Given: ``resolve_symbol_public_id`` returns None,
        When: ``_process_symbol`` is called,
        Then: ``get_ohlcv`` is never invoked.
        """
        service._db = AsyncMock()
        client = AsyncMock()
        with patch(
            "snapper.application.updaters.historical.kraken_equities_aggregates"
            ".resolve_symbol_public_id",
            return_value=None,
        ):
            await service._process_symbol(client, "NO-SUCH-SYMBOL")
        client.get_ohlcv.assert_not_called()

    @pytest.mark.asyncio
    async def test_fetches_and_persists_candles(
        self, service: KrakenEquitiesAggregatesBackfillService
    ) -> None:
        """Happy path: fetches candles and upserts to the database.

        Given: client returns one candle and instrument resolves,
        When: ``_process_symbol`` is called,
        Then: ``upsert_candles`` is invoked once with the mapped row.
        """
        candle = OhlcvSnapshot(
            timestamp=1700000000.0,
            open=23951.0,
            high=24000.0,
            low=23900.0,
            close=23975.0,
            volume=1234.0,
        )
        mock_db = AsyncMock()
        mock_db.ensure_instrument = AsyncMock(return_value=(1, "inst-001"))
        mock_db.upsert_candles = AsyncMock(return_value=1)
        service._db = mock_db
        client = AsyncMock()
        client.get_ohlcv = AsyncMock(return_value=[candle])
        with (
            patch(
                "snapper.application.updaters.historical.kraken_equities_aggregates"
                ".resolve_symbol_public_id",
                return_value="sym-001",
            ),
            patch(
                "snapper.application.updaters.historical.kraken_equities_aggregates.asyncio.sleep",
                new_callable=AsyncMock,
            ),
        ):
            await service._process_symbol(client, "MNQM6-CME")
        mock_db.upsert_candles.assert_awaited_once()
        rows = mock_db.upsert_candles.call_args[0][0]
        assert len(rows) == 1
        assert rows[0]["instrument_public_id"] == "inst-001"

    @pytest.mark.asyncio
    async def test_empty_response_does_not_upsert_but_still_sleeps(
        self, service: KrakenEquitiesAggregatesBackfillService
    ) -> None:
        """Empty window skips upsert but still honours the per-symbol throttle.

        Given: client returns an empty list (legitimately-empty window),
        When: ``_process_symbol`` is called,
        Then: ``upsert_candles`` is not called AND the per-symbol
            ``asyncio.sleep(_RATE_LIMIT_DELAY)`` fires exactly once. Guards
            against a catch-up run over already-current symbols hammering
            iapi back-to-back when every symbol returns an empty window.
        """
        mock_db = AsyncMock()
        mock_db.ensure_instrument = AsyncMock(return_value=(1, "inst-001"))
        service._db = mock_db
        client = AsyncMock()
        client.get_ohlcv = AsyncMock(return_value=[])
        with (
            patch(
                "snapper.application.updaters.historical.kraken_equities_aggregates"
                ".resolve_symbol_public_id",
                return_value="sym-001",
            ),
            patch(
                "snapper.application.updaters.historical.kraken_equities_aggregates.asyncio.sleep",
                new_callable=AsyncMock,
            ) as mock_sleep,
        ):
            await service._process_symbol(client, "MNQM6-CME")
        mock_db.upsert_candles.assert_not_called()
        mock_sleep.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_get_ohlcv_runtime_error_propagates_and_sleeps(
        self, service: KrakenEquitiesAggregatesBackfillService
    ) -> None:
        """Upstream failure propagates and throttle still fires.

        Given: ``client.get_ohlcv`` raises RuntimeError
            (iapi 200 with error envelope),
        When: ``_process_symbol`` is called,
        Then: the RuntimeError escapes AND ``asyncio.sleep(_RATE_LIMIT_DELAY)``
            fires once inside the finally block. Preserves the distinction
            between outage and empty window while ensuring the per-symbol
            throttle is never bypassed after an outbound request.
        """
        mock_db = AsyncMock()
        mock_db.ensure_instrument = AsyncMock(return_value=(1, "inst-001"))
        service._db = mock_db
        client = AsyncMock()
        client.get_ohlcv = AsyncMock(side_effect=RuntimeError("iapi failure"))
        with (
            patch(
                "snapper.application.updaters.historical.kraken_equities_aggregates"
                ".resolve_symbol_public_id",
                return_value="sym-001",
            ),
            patch(
                "snapper.application.updaters.historical.kraken_equities_aggregates.asyncio.sleep",
                new_callable=AsyncMock,
            ) as mock_sleep,
            pytest.raises(RuntimeError, match="iapi failure"),
        ):
            await service._process_symbol(client, "MNQM6-CME")
        mock_db.upsert_candles.assert_not_called()
        mock_sleep.assert_awaited_once()


class TestGetResumeSince:
    """Tests for resume timestamp lookup."""

    @pytest.mark.asyncio
    async def test_no_candles_returns_none(
        self, service: KrakenEquitiesAggregatesBackfillService
    ) -> None:
        """No stored candles returns None.

        Given: ``get_candles`` returns an empty list,
        When: ``_get_resume_since`` is called,
        Then: None is returned.
        """
        mock_db = AsyncMock()
        mock_db.get_candles = AsyncMock(return_value=[])
        service._db = mock_db
        assert await service._get_resume_since("inst-001") is None

    @pytest.mark.asyncio
    async def test_returns_latest_open_at(
        self, service: KrakenEquitiesAggregatesBackfillService
    ) -> None:
        """Returns the latest open_at from stored candles.

        Given: two candles at distinct timestamps,
        When: ``_get_resume_since`` is called,
        Then: the later open_at is returned.
        """
        ts1 = datetime(2024, 1, 1, tzinfo=UTC)
        ts2 = datetime(2024, 1, 2, tzinfo=UTC)
        mock_db = AsyncMock()
        mock_db.get_candles = AsyncMock(return_value=[{"open_at": ts1}, {"open_at": ts2}])
        service._db = mock_db
        assert await service._get_resume_since("inst-001") == ts2


class TestStart:
    """Tests for the start entry point."""

    @pytest.mark.asyncio
    async def test_no_symbols_returns_early(self) -> None:
        """Empty symbol list logs a warning and skips exchange connection.

        Given: a service whose resolver returns no symbols,
        When: ``start`` is called,
        Then: no ``KrakenEquitiesExchangeClient`` is constructed.
        """
        svc = KrakenEquitiesAggregatesBackfillService(symbols=[], all_symbols=False)
        svc.settings = MagicMock(spec=AppSettings)
        svc.settings.instruments = {}
        svc.settings.db_url = "sqlite://"
        with (
            patch(
                "snapper.application.updaters.historical.kraken_equities_aggregates.get_repository",
            ) as mock_repo,
            patch(
                "snapper.application.updaters.historical.kraken_equities_aggregates"
                ".KrakenEquitiesExchangeClient",
            ) as mock_client_cls,
        ):
            mock_repo.return_value = AsyncMock()
            await svc.start()
        mock_client_cls.assert_not_called()

    @pytest.mark.asyncio
    async def test_start_connects_and_processes(self) -> None:
        """Start connects, processes each symbol, then disconnects.

        Given: a service with one symbol,
        When: ``start`` is called,
        Then: the client connect/disconnect pair runs and ``_process_symbol``
            is invoked once with the sole symbol.
        """
        svc = KrakenEquitiesAggregatesBackfillService(
            symbols=["MNQM6-CME"],
            timeframe="1h",
            days_back=7,
            resume=False,
        )
        svc.settings = MagicMock(spec=AppSettings)
        svc.settings.db_url = "sqlite://"
        mock_client = AsyncMock()
        with (
            patch(
                "snapper.application.updaters.historical.kraken_equities_aggregates.get_repository",
            ) as mock_repo,
            patch(
                "snapper.application.updaters.historical.kraken_equities_aggregates"
                ".KrakenEquitiesExchangeClient",
                return_value=mock_client,
            ),
            patch.object(svc, "_process_symbol", new_callable=AsyncMock) as mock_process,
        ):
            mock_repo.return_value = AsyncMock()
            await svc.start()
        mock_client.connect.assert_awaited_once()
        mock_client.disconnect.assert_awaited_once()
        mock_process.assert_awaited_once_with(mock_client, "MNQM6-CME")


class TestEnsureInstrumentCache:
    """Tests for instrument cache hit."""

    @pytest.mark.asyncio
    async def test_cache_hit_skips_db(
        self, service: KrakenEquitiesAggregatesBackfillService
    ) -> None:
        """Cached instrument_public_id is returned without DB query.

        Given: an instrument cached in ``_instrument_cache``,
        When: ``_ensure_instrument`` is called,
        Then: the cached value is returned and DB is untouched.
        """
        service._db = AsyncMock()
        service._instrument_cache["MNQM6-CME"] = "inst-cached"
        assert await service._ensure_instrument("MNQM6-CME") == "inst-cached"
        service._db.ensure_instrument.assert_not_called()


class TestResumePath:
    """Tests for resume functionality in ``_process_symbol``."""

    @pytest.mark.asyncio
    async def test_resume_adjusts_since(self) -> None:
        """Resume mode adjusts ``since`` to latest-stored-candle + 1ms.

        Given: resume=True and a stored candle at ``ts``,
        When: ``_process_symbol`` is called,
        Then: ``get_ohlcv`` is invoked with ``since = ts_ms + 1``.
        """
        svc = KrakenEquitiesAggregatesBackfillService(
            symbols=["MNQM6-CME"],
            timeframe="1h",
            days_back=7,
            resume=True,
        )
        resume_ts = datetime(2024, 6, 15, tzinfo=UTC)
        mock_db = AsyncMock()
        mock_db.ensure_instrument = AsyncMock(return_value=(1, "inst-001"))
        mock_db.get_candles = AsyncMock(return_value=[{"open_at": resume_ts}])
        svc._db = mock_db
        client = AsyncMock()
        client.get_ohlcv = AsyncMock(return_value=[])
        with patch(
            "snapper.application.updaters.historical.kraken_equities_aggregates"
            ".resolve_symbol_public_id",
            return_value="sym-001",
        ):
            await svc._process_symbol(client, "MNQM6-CME")
        call_kwargs = client.get_ohlcv.call_args.kwargs
        assert call_kwargs["since"] == int(resume_ts.timestamp() * 1000) + 1

    @pytest.mark.asyncio
    async def test_resume_no_stored_candles_uses_days_back(self) -> None:
        """Resume with no stored candles falls through to days_back.

        Given: resume=True but no candles in DB,
        When: ``_process_symbol`` is called,
        Then: the call completes (days_back calculation used internally).
        """
        svc = KrakenEquitiesAggregatesBackfillService(
            symbols=["MNQM6-CME"],
            timeframe="1h",
            days_back=7,
            resume=True,
        )
        mock_db = AsyncMock()
        mock_db.ensure_instrument = AsyncMock(return_value=(1, "inst-001"))
        mock_db.get_candles = AsyncMock(return_value=[])
        svc._db = mock_db
        client = AsyncMock()
        client.get_ohlcv = AsyncMock(return_value=[])
        with patch(
            "snapper.application.updaters.historical.kraken_equities_aggregates"
            ".resolve_symbol_public_id",
            return_value="sym-001",
        ):
            await svc._process_symbol(client, "MNQM6-CME")
        mock_db.get_candles.assert_awaited_once()
        client.get_ohlcv.assert_awaited_once()


class TestBatchCommit:
    """Tests for batch-committing candles in chunks."""

    @pytest.mark.asyncio
    async def test_candles_split_across_batches(self) -> None:
        """Candles exceeding ``BATCH_COMMIT_SIZE`` split into multiple upserts.

        Given: a service whose batch size is 2 and client returns 3 candles,
        When: ``_process_symbol`` is called,
        Then: ``upsert_candles`` is called twice (2-row then 1-row batches).
        """
        svc = KrakenEquitiesAggregatesBackfillService(
            symbols=["MNQM6-CME"],
            timeframe="1h",
            days_back=7,
            resume=False,
        )
        svc.BATCH_COMMIT_SIZE = 2
        candles = [
            OhlcvSnapshot(
                timestamp=1700000000.0 + i * 3600,
                open=1.0,
                high=1.0,
                low=1.0,
                close=1.0,
                volume=0.0,
            )
            for i in range(3)
        ]
        mock_db = AsyncMock()
        mock_db.ensure_instrument = AsyncMock(return_value=(1, "inst-001"))
        mock_db.upsert_candles = AsyncMock(return_value=1)
        svc._db = mock_db
        client = AsyncMock()
        client.get_ohlcv = AsyncMock(return_value=candles)
        with (
            patch(
                "snapper.application.updaters.historical.kraken_equities_aggregates"
                ".resolve_symbol_public_id",
                return_value="sym-001",
            ),
            patch(
                "snapper.application.updaters.historical.kraken_equities_aggregates.asyncio.sleep",
                new_callable=AsyncMock,
            ),
        ):
            await svc._process_symbol(client, "MNQM6-CME")
        assert mock_db.upsert_candles.await_count == 2


class TestCliCommand:
    """Tests for CLI command registration and execution."""

    def test_command_exists(self) -> None:
        """``kraken-equities-backfill-candles`` is a registered command.

        Given: the CLI app,
        When: the command list is inspected,
        Then: the new command name is present.
        """
        command_names = [cmd.name for cmd in app.registered_commands]
        assert "kraken-equities-backfill-candles" in command_names

    def test_cli_invokes_service(self) -> None:
        """CLI command constructs the service and awaits ``start``.

        Given: a CLI runner invoking the command with a symbol and days,
        When: the command runs,
        Then: the service's ``start`` coroutine is awaited once.
        """
        runner = CliRunner()
        with patch("snapper.cli.app.KrakenEquitiesAggregatesBackfillService") as mock_cls:
            mock_service = MagicMock()
            mock_service.start = AsyncMock()
            mock_cls.return_value = mock_service
            result = runner.invoke(
                app,
                ["kraken-equities-backfill-candles", "--symbol", "MNQM6-CME", "--days", "7"],
            )
        assert result.exit_code == 0
        mock_service.start.assert_called_once()

    def test_cli_handles_error(self) -> None:
        """CLI exits with code 1 when ``start`` raises.

        Given: ``service.start`` raises RuntimeError (simulating iapi outage),
        When: the CLI command runs,
        Then: exit code is 1 and the error message surfaces in output.
        """
        runner = CliRunner()
        with patch("snapper.cli.app.KrakenEquitiesAggregatesBackfillService") as mock_cls:
            mock_service = MagicMock()
            mock_service.start = AsyncMock(side_effect=RuntimeError("iapi failure"))
            mock_cls.return_value = mock_service
            result = runner.invoke(
                app,
                ["kraken-equities-backfill-candles", "--symbol", "MNQM6-CME"],
            )
        assert result.exit_code == 1
        assert "iapi failure" in result.output
