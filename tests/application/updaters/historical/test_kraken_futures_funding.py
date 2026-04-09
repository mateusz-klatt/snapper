"""Tests for Kraken Futures historical funding rate backfill service."""

from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from snapper.application.updaters.historical.kraken_futures_funding import (
    KrakenFuturesFundingBackfillService,
)
from snapper.cli.app import app
from snapper.config.settings import AppSettings
from snapper.infrastructure.exchanges.contracts import FundingRateSnapshot


@pytest.fixture()
def service() -> KrakenFuturesFundingBackfillService:
    """Create a funding backfill service with default parameters."""
    return KrakenFuturesFundingBackfillService(
        symbols=["BTC-USD-PERP"],
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
        mock_settings.instruments = {"kraken_futures": ["BTC-USD-PERP", "ETH-USD-PERP"]}
        params = KrakenFuturesFundingBackfillService.get_default_parameters(mock_settings)
        assert params["symbols"] == ["BTC-USD-PERP", "ETH-USD-PERP"]
        assert params["all_symbols"] is False

    def test_init_stores_symbols(self) -> None:
        """Constructor stores requested symbols.

        Given: A list of symbols,
        When: Service is instantiated,
        Then: Symbols are stored internally.
        """
        svc = KrakenFuturesFundingBackfillService(symbols=["BTC-USD-PERP"])
        assert svc._requested_symbols == ["BTC-USD-PERP"]

    def test_init_none_symbols_defaults_to_empty(self) -> None:
        """None symbols default to empty list.

        Given: symbols=None,
        When: Service is instantiated,
        Then: Internal list is empty.
        """
        svc = KrakenFuturesFundingBackfillService(symbols=None)
        assert svc._requested_symbols == []


class TestResolveSymbols:
    """Tests for symbol resolution."""

    def test_explicit_symbols_filters_perpetuals(
        self,
        service: KrakenFuturesFundingBackfillService,
    ) -> None:
        """Explicit symbols are filtered to perpetuals only.

        Given: Service with a mix of perpetual and dated symbols,
        When: _resolve_symbols is called,
        Then: Only perpetuals are returned.
        """
        svc = KrakenFuturesFundingBackfillService(
            symbols=["BTC-USD-PERP", "BTC-USD-260620"],
        )
        assert svc._resolve_symbols() == ["BTC-USD-PERP"]

    def test_settings_fallback_filters_perpetuals(self) -> None:
        """Settings fallback also filters to perpetuals.

        Given: Settings with both perpetual and dated instruments,
        When: _resolve_symbols is called,
        Then: Only perpetuals returned.
        """
        svc = KrakenFuturesFundingBackfillService(symbols=[])
        svc.settings = MagicMock(spec=AppSettings)
        svc.settings.instruments = {
            "kraken_futures": ["BTC-USD-PERP", "ETH-USD-260620", "ETH-USD-PERP"],
        }
        result = svc._resolve_symbols()
        assert result == ["BTC-USD-PERP", "ETH-USD-PERP"]

    def test_all_symbols_queries_mapper_and_filters(self) -> None:
        """all_symbols=True filters mapper results to perpetuals.

        Given: Service with all_symbols=True,
        When: _resolve_symbols is called,
        Then: Delegates to symbol mapper and filters perpetuals.
        """
        svc = KrakenFuturesFundingBackfillService(all_symbols=True)
        with patch(
            "snapper.application.updaters.historical.kraken_futures_funding"
            ".get_available_kraken_futures_symbols",
            return_value=["BTC-USD-PERP", "ETH-USD-260620", "ETH-USD-PERP-INV"],
        ):
            result = svc._resolve_symbols()
        assert result == ["BTC-USD-PERP", "ETH-USD-PERP-INV"]


class TestEnsureInstrument:
    """Tests for instrument resolution."""

    @pytest.mark.asyncio
    async def test_returns_cached_instrument(
        self,
        service: KrakenFuturesFundingBackfillService,
    ) -> None:
        """Cached instrument is returned without DB query.

        Given: Instrument already cached,
        When: _ensure_instrument is called,
        Then: Returns cached value.
        """
        service._db = AsyncMock()
        service._instrument_cache["BTC-USD-PERP"] = "cached-inst-1"
        result = await service._ensure_instrument("BTC-USD-PERP")
        assert result == "cached-inst-1"

    @pytest.mark.asyncio
    async def test_returns_none_for_unknown_symbol(
        self,
        service: KrakenFuturesFundingBackfillService,
    ) -> None:
        """Unknown symbol returns None.

        Given: resolve_symbol_public_id returns None,
        When: _ensure_instrument is called,
        Then: Returns None.
        """
        service._db = AsyncMock()
        with patch(
            "snapper.application.updaters.historical.kraken_futures_funding"
            ".resolve_symbol_public_id",
            new_callable=AsyncMock,
            return_value=None,
        ):
            result = await service._ensure_instrument("UNKNOWN-PERP")
        assert result is None

    @pytest.mark.asyncio
    async def test_resolves_and_caches_instrument(
        self,
        service: KrakenFuturesFundingBackfillService,
    ) -> None:
        """New instrument is resolved, cached, and returned.

        Given: Symbol exists in DB,
        When: _ensure_instrument is called,
        Then: Returns instrument_public_id and caches it.
        """
        service._db = AsyncMock()
        service._db.ensure_instrument = AsyncMock(return_value=(1, "inst-1"))
        with patch(
            "snapper.application.updaters.historical.kraken_futures_funding"
            ".resolve_symbol_public_id",
            new_callable=AsyncMock,
            return_value="sym-1",
        ):
            result = await service._ensure_instrument("BTC-USD-PERP")
        assert result == "inst-1"
        assert service._instrument_cache["BTC-USD-PERP"] == "inst-1"


class TestProcessSymbol:
    """Tests for single-symbol funding backfill processing."""

    @pytest.mark.asyncio
    async def test_skips_symbol_without_instrument(
        self,
        service: KrakenFuturesFundingBackfillService,
    ) -> None:
        """Symbol without instrument in DB is skipped.

        Given: _ensure_instrument returns None,
        When: _process_symbol is called,
        Then: Returns without error.
        """
        service._db = AsyncMock()
        client = MagicMock()
        with patch.object(service, "_ensure_instrument", new_callable=AsyncMock, return_value=None):
            await service._process_symbol(client, "UNKNOWN-PERP")
        client.get_historical_funding_rates.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_symbol_without_ws_alias(
        self,
        service: KrakenFuturesFundingBackfillService,
    ) -> None:
        """Symbol without WS alias is skipped.

        Given: native_to_kraken_futures_ws raises ValueError,
        When: _process_symbol is called,
        Then: Returns without error.
        """
        service._db = AsyncMock()
        client = MagicMock()
        with (
            patch.object(
                service,
                "_ensure_instrument",
                new_callable=AsyncMock,
                return_value="inst-1",
            ),
            patch(
                "snapper.application.updaters.historical.kraken_futures_funding"
                ".native_to_kraken_futures_ws",
                side_effect=ValueError("unknown"),
            ),
        ):
            await service._process_symbol(client, "UNKNOWN-PERP")
        client.get_historical_funding_rates.assert_not_called()

    @pytest.mark.asyncio
    async def test_inserts_funding_rates(
        self,
        service: KrakenFuturesFundingBackfillService,
    ) -> None:
        """Fetched funding rates are inserted into the database.

        Given: Client returns 2 funding rate snapshots,
        When: _process_symbol is called,
        Then: insert_funding_rate is called twice.
        """
        service._db = AsyncMock()
        service._db.insert_funding_rate = AsyncMock(return_value=1)
        client = MagicMock()
        snapshots = [
            FundingRateSnapshot(
                symbol="BTC-USD-PERP",
                exchange="kraken_futures",
                rate_type="perpetual_funding",
                direction="both",
                rate=7.0e-05,
                effective_from=datetime(2026, 3, 1, 16, tzinfo=UTC),
                notional_asset="USD",
                source="exchange_api",
            ),
            FundingRateSnapshot(
                symbol="BTC-USD-PERP",
                exchange="kraken_futures",
                rate_type="perpetual_funding",
                direction="both",
                rate=-5.0e-05,
                effective_from=datetime(2026, 3, 1, 20, tzinfo=UTC),
                notional_asset="USD",
                source="exchange_api",
            ),
        ]
        client.get_historical_funding_rates = AsyncMock(return_value=snapshots)
        with (
            patch.object(
                service,
                "_ensure_instrument",
                new_callable=AsyncMock,
                return_value="inst-1",
            ),
            patch(
                "snapper.application.updaters.historical.kraken_futures_funding"
                ".native_to_kraken_futures_ws",
                return_value="PF_XBTUSD",
            ),
        ):
            await service._process_symbol(client, "BTC-USD-PERP")
        assert service._db.insert_funding_rate.call_count == 2

    @pytest.mark.asyncio
    async def test_handles_duplicate_insert_gracefully(
        self,
        service: KrakenFuturesFundingBackfillService,
    ) -> None:
        """Duplicate inserts are counted as skipped, not failures.

        Given: insert_funding_rate raises IntegrityError for duplicate,
        When: _process_symbol is called,
        Then: Continues without raising.
        """
        service._db = AsyncMock()
        service._db.insert_funding_rate = AsyncMock(
            side_effect=Exception("UNIQUE constraint failed"),
        )
        client = MagicMock()
        snapshots = [
            FundingRateSnapshot(
                symbol="BTC-USD-PERP",
                exchange="kraken_futures",
                rate_type="perpetual_funding",
                direction="both",
                rate=7.0e-05,
                effective_from=datetime(2026, 3, 1, 16, tzinfo=UTC),
                notional_asset="USD",
                source="exchange_api",
            ),
        ]
        client.get_historical_funding_rates = AsyncMock(return_value=snapshots)
        with (
            patch.object(
                service,
                "_ensure_instrument",
                new_callable=AsyncMock,
                return_value="inst-1",
            ),
            patch(
                "snapper.application.updaters.historical.kraken_futures_funding"
                ".native_to_kraken_futures_ws",
                return_value="PF_XBTUSD",
            ),
        ):
            await service._process_symbol(client, "BTC-USD-PERP")

    @pytest.mark.asyncio
    async def test_empty_snapshots_returns_early(
        self,
        service: KrakenFuturesFundingBackfillService,
    ) -> None:
        """No snapshots returned means no DB writes.

        Given: Client returns empty snapshot list,
        When: _process_symbol is called,
        Then: insert_funding_rate is never called.
        """
        service._db = AsyncMock()
        client = MagicMock()
        client.get_historical_funding_rates = AsyncMock(return_value=[])
        with (
            patch.object(
                service,
                "_ensure_instrument",
                new_callable=AsyncMock,
                return_value="inst-1",
            ),
            patch(
                "snapper.application.updaters.historical.kraken_futures_funding"
                ".native_to_kraken_futures_ws",
                return_value="PF_XBTUSD",
            ),
        ):
            await service._process_symbol(client, "BTC-USD-PERP")
        service._db.insert_funding_rate.assert_not_called()


class TestStart:
    """Tests for the start entry point."""

    @pytest.mark.asyncio
    async def test_start_processes_all_symbols(self) -> None:
        """Start iterates over resolved symbols.

        Given: Service with 2 resolved symbols,
        When: start() is called,
        Then: _process_symbol is called for each.
        """
        svc = KrakenFuturesFundingBackfillService(symbols=["BTC-USD-PERP", "ETH-USD-PERP"])
        svc._db = AsyncMock()
        with (
            patch.object(svc, "_resolve_symbols", return_value=["BTC-USD-PERP", "ETH-USD-PERP"]),
            patch.object(svc, "_process_symbol", new_callable=AsyncMock) as mock_process,
            patch(
                "snapper.application.updaters.historical.kraken_futures_funding.get_repository",
                return_value=AsyncMock(),
            ),
            patch(
                "snapper.application.updaters.historical.kraken_futures_funding.set_log_context",
            ),
        ):
            await svc.start()
        assert mock_process.call_count == 2

    @pytest.mark.asyncio
    async def test_start_no_symbols_returns_early(self) -> None:
        """No resolved symbols logs warning and returns.

        Given: _resolve_symbols returns empty list,
        When: start() is called,
        Then: Returns without creating client.
        """
        svc = KrakenFuturesFundingBackfillService(symbols=[])
        svc.settings = MagicMock(spec=AppSettings)
        svc.settings.instruments = {"kraken_futures": []}
        with (
            patch(
                "snapper.application.updaters.historical.kraken_futures_funding.get_repository",
                return_value=AsyncMock(),
            ),
            patch(
                "snapper.application.updaters.historical.kraken_futures_funding.set_log_context",
            ),
            patch(
                "snapper.application.updaters.historical.kraken_futures_funding"
                ".KrakenFuturesExchangeClient",
            ) as mock_client_cls,
        ):
            await svc.start()
        mock_client_cls.assert_not_called()


class TestCLI:
    """Tests for the CLI command."""

    def test_command_registered(self) -> None:
        """update-kraken-futures-funding-rates command is registered.

        Given: The CLI app,
        When: Checking registered commands,
        Then: update-kraken-futures-funding-rates exists.
        """
        command_names = [cmd.name for cmd in app.registered_commands]
        assert "update-kraken-futures-funding-rates" in command_names

    def test_cli_invokes_service(self) -> None:
        """CLI command creates and runs the backfill service.

        Given: CLI runner invokes update-kraken-futures-funding-rates,
        When: The command runs,
        Then: Service start() is called.
        """
        runner = CliRunner()
        with patch("snapper.cli.app.KrakenFuturesFundingBackfillService") as mock_cls:
            mock_service = MagicMock()
            mock_service.start = AsyncMock()
            mock_cls.return_value = mock_service
            result = runner.invoke(
                app,
                ["update-kraken-futures-funding-rates", "--symbol", "BTC-USD-PERP"],
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
        with patch("snapper.cli.app.KrakenFuturesFundingBackfillService") as mock_cls:
            mock_service = MagicMock()
            mock_service.start = AsyncMock(side_effect=RuntimeError("test error"))
            mock_cls.return_value = mock_service
            result = runner.invoke(
                app,
                ["update-kraken-futures-funding-rates", "--symbol", "BTC-USD-PERP"],
            )
        assert result.exit_code == 1
        assert "test error" in result.output
