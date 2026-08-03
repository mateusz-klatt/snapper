"""Tests for Kraken Futures OHLCV candle backfill service."""

from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from snapper.application.updaters.historical.kraken_futures_aggregates import _OHLCV_PAGE_LIMIT
from snapper.application.updaters.historical.kraken_futures_aggregates import (
    KrakenFuturesAggregatesBackfillService,
)
from snapper.cli.app import app
from snapper.config.settings import AppSettings
from snapper.infrastructure.exchanges.contracts import OhlcvSnapshot


@pytest.fixture
def service() -> KrakenFuturesAggregatesBackfillService:
    """Create a backfill service with default parameters."""
    return KrakenFuturesAggregatesBackfillService(
        symbols=["BTC-USD-PERP"],
        timeframe="1h",
        days_back=7,
        resume=False,
    )


class TestInit:
    """Tests for service initialization."""

    def test_default_parameters(self) -> None:
        """get_default_parameters returns settings-based defaults.

        Given: AppSettings with kraken_futures instruments,
        When: get_default_parameters is called,
        Then: Returns expected defaults.
        """
        mock_settings = MagicMock(spec=AppSettings)
        mock_settings.instruments = {"kraken_futures": ["BTC-USD-PERP"]}
        params = KrakenFuturesAggregatesBackfillService.get_default_parameters(mock_settings)
        assert params["symbols"] == ["BTC-USD-PERP"]
        assert params["timeframe"] == "1h"
        assert params["days_back"] == 90

    def test_invalid_timeframe_raises(self) -> None:
        """Invalid timeframe raises ValueError on init.

        Given: An unsupported timeframe string,
        When: Service is instantiated,
        Then: Raises ValueError.
        """
        with pytest.raises(ValueError, match="Unsupported timeframe"):
            KrakenFuturesAggregatesBackfillService(timeframe="2w")


class TestResolveSymbols:
    """Tests for symbol resolution."""

    def test_explicit_symbols(self, service: KrakenFuturesAggregatesBackfillService) -> None:
        """Explicit symbols are returned directly.

        Given: Service with explicit symbols,
        When: _resolve_symbols is called,
        Then: Returns those symbols.
        """
        assert service._resolve_symbols() == ["BTC-USD-PERP"]

    def test_settings_fallback(self) -> None:
        """No explicit symbols falls back to settings.instruments.

        Given: Service with no explicit symbols and not all_symbols,
        When: _resolve_symbols is called,
        Then: Returns instruments from settings.
        """
        svc = KrakenFuturesAggregatesBackfillService(symbols=[], all_symbols=False)
        svc.settings = MagicMock(spec=AppSettings)
        svc.settings.instruments = {"kraken_futures": ["ETH-USD-PERP"]}
        assert svc._resolve_symbols() == ["ETH-USD-PERP"]

    def test_all_symbols_queries_mapper(self) -> None:
        """all_symbols=True uses get_available_kraken_futures_symbols.

        Given: Service with all_symbols=True,
        When: _resolve_symbols is called,
        Then: Delegates to symbol mapper function.
        """
        svc = KrakenFuturesAggregatesBackfillService(all_symbols=True)
        with patch(
            "snapper.application.updaters.historical.kraken_futures_aggregates"
            ".get_available_kraken_futures_symbols",
            return_value=["BTC-USD-PERP", "ETH-USD-PERP"],
        ):
            result = svc._resolve_symbols()
        assert result == ["BTC-USD-PERP", "ETH-USD-PERP"]

    def test_wildcard_settings_expands_to_all_mapped(self) -> None:
        """``settings.instruments=["*"]`` resolves identically to ``--all``.

        Given: Service constructed without ``--all`` flag and without
            ``--symbol`` args, with ``settings.instruments[KRAKEN_FUTURES]``
            set to the wildcard sentinel ``["*"]``,
        When: ``_resolve_symbols`` runs,
        Then: It delegates to ``get_available_kraken_futures_symbols()``
            (same path as ``all_symbols=True``) so the single sentinel
            consistently means "all venues" across publishers + backfill.
        """
        svc = KrakenFuturesAggregatesBackfillService()
        svc.settings.instruments = {"kraken_futures": ["*"]}
        with patch(
            "snapper.application.updaters.historical.kraken_futures_aggregates"
            ".get_available_kraken_futures_symbols",
            return_value=["BTC-USD-PERP", "SOL-USD-PERP"],
        ):
            result = svc._resolve_symbols()
        assert result == ["BTC-USD-PERP", "SOL-USD-PERP"]


class TestBuildCandleRows:
    """Tests for OhlcvSnapshot to CandleUpsertRow conversion."""

    def test_builds_correct_rows(self) -> None:
        """Candle rows have correct field mapping from OhlcvSnapshot.

        Given: A list of OhlcvSnapshot objects,
        When: _build_candle_rows is called,
        Then: Returns CandleUpsertRow dicts with correct values.
        """
        candles = [
            OhlcvSnapshot(
                timestamp=1700000000.0,
                open=50000.0,
                high=51000.0,
                low=49000.0,
                close=50500.0,
                volume=100.0,
            ),
        ]
        seq_counter = iter(range(100))
        bus_time = datetime(2026, 1, 1, tzinfo=UTC)
        rows = KrakenFuturesAggregatesBackfillService._build_candle_rows(
            candles,
            "inst-001",
            "1h",
            "sess-001",
            lambda: next(seq_counter),
            bus_time,
        )
        assert len(rows) == 1
        row = rows[0]
        assert row["instrument_public_id"] == "inst-001"
        assert row["timeframe"] == "1h"
        assert row["open"] == pytest.approx(50000.0)
        assert row["high"] == pytest.approx(51000.0)
        assert row["volume"] == pytest.approx(100.0)
        assert row["vwap"] is None
        assert row["trades"] is None
        assert row["session_id"] == "sess-001"
        assert row["open_at"] == datetime.fromtimestamp(1700000000.0, tz=UTC)
        assert row["timestamp"] == bus_time
        assert row["open_at"] != row["timestamp"]

    def test_empty_candles_returns_empty(self) -> None:
        """Empty input produces empty output.

        Given: An empty candle list,
        When: _build_candle_rows is called,
        Then: Returns empty list.
        """
        rows = KrakenFuturesAggregatesBackfillService._build_candle_rows(
            [],
            "inst-001",
            "1h",
            "sess-001",
            lambda: 0,
            datetime(2026, 1, 1, tzinfo=UTC),
        )
        assert rows == []


class TestProcessSymbol:
    """Tests for single-symbol backfill processing."""

    @pytest.mark.asyncio
    async def test_skips_symbol_without_ccxt_alias(
        self, service: KrakenFuturesAggregatesBackfillService
    ) -> None:
        """Symbols without CCXT alias are skipped.

        Given: native_to_ccxt raises ValueError,
        When: _process_symbol is called,
        Then: Returns without error.
        """
        client = AsyncMock()
        with patch(
            "snapper.application.updaters.historical.kraken_futures_aggregates.native_to_ccxt",
            side_effect=ValueError("unknown"),
        ):
            await service._process_symbol(client, "UNKNOWN-PERP")
        client.get_ohlcv.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_symbol_without_instrument(
        self, service: KrakenFuturesAggregatesBackfillService
    ) -> None:
        """Symbols without an instrument row are skipped.

        Given: resolve_symbol_public_id returns None,
        When: _process_symbol is called,
        Then: Returns without fetching candles.
        """
        service._db = AsyncMock()
        client = AsyncMock()
        with patch(
            "snapper.application.updaters.historical.kraken_futures_aggregates.native_to_ccxt",
            return_value="BTC/USD:USD",
        ), patch(
            "snapper.application.updaters.historical.kraken_futures_aggregates"
            ".resolve_symbol_public_id",
            return_value=None,
        ):
            await service._process_symbol(client, "BTC-USD-PERP")
        client.get_ohlcv.assert_not_called()

    @pytest.mark.asyncio
    async def test_fetches_and_persists_candles(
        self, service: KrakenFuturesAggregatesBackfillService
    ) -> None:
        """Backfill fetches candles and upserts to database.

        Given: Client returns candles, instrument exists,
        When: _process_symbol is called,
        Then: upsert_candles is called with correct rows.
        """
        candle = OhlcvSnapshot(
            timestamp=1700000000.0,
            open=50000.0,
            high=51000.0,
            low=49000.0,
            close=50500.0,
            volume=100.0,
        )
        mock_db = AsyncMock()
        mock_db.ensure_instrument = AsyncMock(return_value=(1, "inst-001"))
        mock_db.upsert_candles = AsyncMock(return_value=1)
        service._db = mock_db
        client = AsyncMock()
        client.get_ohlcv = AsyncMock(side_effect=[[candle], []])
        with patch(
            "snapper.application.updaters.historical.kraken_futures_aggregates.native_to_ccxt",
            return_value="BTC/USD:USD",
        ), patch(
            "snapper.application.updaters.historical.kraken_futures_aggregates"
            ".resolve_symbol_public_id",
            return_value="sym-001",
        ):
            await service._process_symbol(client, "BTC-USD-PERP")
        mock_db.upsert_candles.assert_called_once()
        rows = mock_db.upsert_candles.call_args[0][0]
        assert len(rows) == 1
        assert rows[0]["instrument_public_id"] == "inst-001"

    @pytest.mark.asyncio
    async def test_empty_response_stops(
        self, service: KrakenFuturesAggregatesBackfillService
    ) -> None:
        """Empty OHLCV response stops the backfill loop.

        Given: Client returns empty list immediately,
        When: _process_symbol is called,
        Then: No upsert is attempted.
        """
        mock_db = AsyncMock()
        mock_db.ensure_instrument = AsyncMock(return_value=(1, "inst-001"))
        service._db = mock_db
        client = AsyncMock()
        client.get_ohlcv = AsyncMock(return_value=[])
        with patch(
            "snapper.application.updaters.historical.kraken_futures_aggregates.native_to_ccxt",
            return_value="BTC/USD:USD",
        ), patch(
            "snapper.application.updaters.historical.kraken_futures_aggregates"
            ".resolve_symbol_public_id",
            return_value="sym-001",
        ):
            await service._process_symbol(client, "BTC-USD-PERP")
        mock_db.upsert_candles.assert_not_called()


class TestGetResumeSince:
    """Tests for resume timestamp lookup."""

    @pytest.mark.asyncio
    async def test_no_candles_returns_none(
        self, service: KrakenFuturesAggregatesBackfillService
    ) -> None:
        """No stored candles returns None.

        Given: Database has no candles for the instrument,
        When: _get_resume_since is called,
        Then: Returns None.
        """
        mock_db = AsyncMock()
        mock_db.get_candles = AsyncMock(return_value=[])
        service._db = mock_db
        result = await service._get_resume_since("inst-001")
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_latest_open_at(
        self, service: KrakenFuturesAggregatesBackfillService
    ) -> None:
        """Returns the latest open_at from stored candles.

        Given: Database has candles with different timestamps,
        When: _get_resume_since is called,
        Then: Returns the latest open_at.
        """
        ts1 = datetime(2024, 1, 1, tzinfo=UTC)
        ts2 = datetime(2024, 1, 2, tzinfo=UTC)
        mock_db = AsyncMock()
        mock_db.get_candles = AsyncMock(
            return_value=[
                {"open_at": ts1},
                {"open_at": ts2},
            ]
        )
        service._db = mock_db
        result = await service._get_resume_since("inst-001")
        assert result == ts2


class TestStart:
    """Tests for the start entry point."""

    @pytest.mark.asyncio
    async def test_no_symbols_returns_early(self) -> None:
        """Empty symbol list logs warning and returns.

        Given: Service with no symbols and not all_symbols,
        When: start is called,
        Then: Returns without connecting to exchange.
        """
        svc = KrakenFuturesAggregatesBackfillService(symbols=[], all_symbols=False)
        svc.settings = MagicMock(spec=AppSettings)
        svc.settings.instruments = {}
        svc.settings.db_url = "sqlite://"
        with patch(
            "snapper.application.updaters.historical.kraken_futures_aggregates.get_repository",
        ) as mock_repo, patch(
            "snapper.application.updaters.historical.kraken_futures_aggregates"
            ".KrakenFuturesExchangeClient",
        ) as mock_client_cls:
            mock_repo.return_value = AsyncMock()
            await svc.start()
        mock_client_cls.assert_not_called()

    @pytest.mark.asyncio
    async def test_start_connects_and_processes(self) -> None:
        """Start connects to exchange, processes symbols, then disconnects.

        Given: Service with one symbol,
        When: start is called,
        Then: Client connect/disconnect called, _process_symbol invoked.
        """
        svc = KrakenFuturesAggregatesBackfillService(
            symbols=["BTC-USD-PERP"],
            timeframe="1h",
            days_back=7,
            resume=False,
        )
        svc.settings = MagicMock(spec=AppSettings)
        svc.settings.db_url = "sqlite://"
        mock_client = AsyncMock()
        with patch(
            "snapper.application.updaters.historical.kraken_futures_aggregates.get_repository",
        ) as mock_repo, patch(
            "snapper.application.updaters.historical.kraken_futures_aggregates"
            ".KrakenFuturesExchangeClient",
            return_value=mock_client,
        ), patch.object(
            svc, "_process_symbol", new_callable=AsyncMock
        ) as mock_process:
            mock_repo.return_value = AsyncMock()
            await svc.start()
        mock_client.connect.assert_awaited_once()
        mock_client.disconnect.assert_awaited_once()
        mock_process.assert_awaited_once_with(mock_client, "BTC-USD-PERP")


class TestEnsureInstrumentCache:
    """Tests for instrument cache hit."""

    @pytest.mark.asyncio
    async def test_cache_hit_skips_db(
        self, service: KrakenFuturesAggregatesBackfillService
    ) -> None:
        """Cached instrument_public_id is returned without DB query.

        Given: Instrument already in cache,
        When: _ensure_instrument is called,
        Then: Returns cached value without querying DB.
        """
        service._db = AsyncMock()
        service._instrument_cache["BTC-USD-PERP"] = "inst-cached"
        result = await service._ensure_instrument("BTC-USD-PERP")
        assert result == "inst-cached"
        service._db.ensure_instrument.assert_not_called()


class TestResumePath:
    """Tests for resume functionality in _process_symbol."""

    @pytest.mark.asyncio
    async def test_resume_adjusts_since(
        self, service: KrakenFuturesAggregatesBackfillService
    ) -> None:
        """Resume mode starts from latest stored candle timestamp.

        Given: Service with resume=True and existing candles,
        When: _process_symbol is called,
        Then: get_ohlcv since parameter uses resume timestamp.
        """
        svc = KrakenFuturesAggregatesBackfillService(
            symbols=["BTC-USD-PERP"],
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
            "snapper.application.updaters.historical.kraken_futures_aggregates.native_to_ccxt",
            return_value="BTC/USD:USD",
        ), patch(
            "snapper.application.updaters.historical.kraken_futures_aggregates"
            ".resolve_symbol_public_id",
            return_value="sym-001",
        ):
            await svc._process_symbol(client, "BTC-USD-PERP")
        call_args = client.get_ohlcv.call_args
        assert call_args.kwargs["since"] == int(resume_ts.timestamp() * 1000) + 1

    @pytest.mark.asyncio
    async def test_resume_no_stored_candles(self) -> None:
        """Resume with no stored candles uses days_back start.

        Given: Service with resume=True but no candles in DB,
        When: _process_symbol is called,
        Then: Falls through resume check, uses days_back calculation.
        """
        svc = KrakenFuturesAggregatesBackfillService(
            symbols=["BTC-USD-PERP"],
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
            "snapper.application.updaters.historical.kraken_futures_aggregates.native_to_ccxt",
            return_value="BTC/USD:USD",
        ), patch(
            "snapper.application.updaters.historical.kraken_futures_aggregates"
            ".resolve_symbol_public_id",
            return_value="sym-001",
        ):
            await svc._process_symbol(client, "BTC-USD-PERP")
        mock_db.get_candles.assert_awaited_once()
        client.get_ohlcv.assert_awaited_once()


class TestPagination:
    """Tests for multi-page candle fetching."""

    @pytest.mark.asyncio
    async def test_paginates_until_exhausted(self) -> None:
        """Backfill fetches multiple pages with rate limiting.

        Given: Client returns full page then partial page,
        When: _process_symbol is called,
        Then: Two get_ohlcv calls made, rate limit sleep between them.
        """
        svc = KrakenFuturesAggregatesBackfillService(
            symbols=["BTC-USD-PERP"],
            timeframe="1h",
            days_back=7,
            resume=False,
        )
        base_ts = 1700000000.0
        full_page = [
            OhlcvSnapshot(
                timestamp=base_ts + i * 3600,
                open=50000.0,
                high=51000.0,
                low=49000.0,
                close=50500.0,
                volume=100.0,
            )
            for i in range(_OHLCV_PAGE_LIMIT)
        ]
        partial_page = [
            OhlcvSnapshot(
                timestamp=base_ts + _OHLCV_PAGE_LIMIT * 3600,
                open=50000.0,
                high=51000.0,
                low=49000.0,
                close=50500.0,
                volume=100.0,
            ),
        ]
        mock_db = AsyncMock()
        mock_db.ensure_instrument = AsyncMock(return_value=(1, "inst-001"))
        mock_db.upsert_candles = AsyncMock(return_value=1)
        svc._db = mock_db
        client = AsyncMock()
        client.get_ohlcv = AsyncMock(side_effect=[full_page, partial_page])
        with patch(
            "snapper.application.updaters.historical.kraken_futures_aggregates.native_to_ccxt",
            return_value="BTC/USD:USD",
        ), patch(
            "snapper.application.updaters.historical.kraken_futures_aggregates"
            ".resolve_symbol_public_id",
            return_value="sym-001",
        ), patch(
            "snapper.application.updaters.historical.kraken_futures_aggregates.asyncio.sleep",
            new_callable=AsyncMock,
        ) as mock_sleep:
            await svc._process_symbol(client, "BTC-USD-PERP")
        assert client.get_ohlcv.call_count == 2
        mock_sleep.assert_awaited_once()


class TestCliCommand:
    """Tests for CLI command registration and execution."""

    def test_command_exists(self) -> None:
        """kraken-futures-backfill-candles command is registered.

        Given: The CLI app,
        When: Checking registered commands,
        Then: kraken-futures-backfill-candles exists.
        """
        command_names = [cmd.name for cmd in app.registered_commands]
        assert "kraken-futures-backfill-candles" in command_names

    def test_cli_invokes_service(self) -> None:
        """CLI command creates and runs the backfill service.

        Given: CLI runner invokes kraken-futures-backfill-candles,
        When: The command runs,
        Then: Service start() is called.
        """
        runner = CliRunner()
        with patch("snapper.cli.app.KrakenFuturesAggregatesBackfillService") as mock_cls:
            mock_service = MagicMock()
            mock_service.start = AsyncMock()
            mock_cls.return_value = mock_service
            result = runner.invoke(
                app,
                ["kraken-futures-backfill-candles", "--symbol", "BTC-USD-PERP", "--days", "7"],
            )
        assert result.exit_code == 0
        mock_service.start.assert_called_once()

    def test_cli_handles_error(self) -> None:
        """CLI command exits with code 1 on error.

        Given: Service.start() raises an exception,
        When: CLI command runs,
        Then: Exits with code 1 and error message.
        """
        runner = CliRunner()
        with patch("snapper.cli.app.KrakenFuturesAggregatesBackfillService") as mock_cls:
            mock_service = MagicMock()
            mock_service.start = AsyncMock(side_effect=RuntimeError("test error"))
            mock_cls.return_value = mock_service
            result = runner.invoke(
                app,
                ["kraken-futures-backfill-candles", "--symbol", "BTC-USD-PERP"],
            )
        assert result.exit_code == 1
        assert "test error" in result.output
