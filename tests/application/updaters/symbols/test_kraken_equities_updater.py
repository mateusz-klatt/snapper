"""Tests for Kraken Equities symbol updater."""

from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.application.updaters.symbols.kraken_equities import KrakenEquitiesSymbolUpdaterService
from snapper.application.updaters.symbols.kraken_equities import _build_native_symbol
from snapper.application.updaters.symbols.kraken_equities import _classify_asset_type
from snapper.config.settings import AppSettings
from snapper.core.types import AssetTypeEnum
from snapper.infrastructure.exchanges.implementations.kraken_equities import (
    KrakenEquitiesExchangeClient,
)
from snapper.infrastructure.exchanges.schemas.kraken_equities import KrakenEquitiesInstrumentSchema


def _make_schema(**overrides: object) -> KrakenEquitiesInstrumentSchema:
    """Create a minimal valid instrument schema with overrides."""
    defaults = {
        "symbol": "CLM6.NYMEX",
        "name": "CLM6 19May26",
        "short_name": "Crude Oil",
        "contract_name": "CLM6",
        "tradable": True,
        "status": "active",
        "category": "Energies",
        "exchange": "NYMEX",
        "tick_size": "0.01",
        "contract_size": "1000",
        "base": "USD",
        "quote": "USD",
    }
    defaults.update(overrides)
    return KrakenEquitiesInstrumentSchema.model_validate(defaults)


class TestBuildNativeSymbol:
    """Tests for _build_native_symbol mapping."""

    def test_dot_to_dash(self) -> None:
        """Convert dot-separated exchange symbol to dash-separated native.

        Given: Instrument with symbol 'CLM6.NYMEX',
        When: _build_native_symbol is called,
        Then: Returns 'CLM6-NYMEX'.
        """
        schema = _make_schema(symbol="CLM6.NYMEX")
        assert _build_native_symbol(schema) == "CLM6-NYMEX"

    def test_comex_symbol(self) -> None:
        """Convert COMEX symbol.

        Given: Instrument with symbol 'GCQ6.COMEX',
        When: _build_native_symbol is called,
        Then: Returns 'GCQ6-COMEX'.
        """
        schema = _make_schema(symbol="GCQ6.COMEX")
        assert _build_native_symbol(schema) == "GCQ6-COMEX"

    def test_cme_symbol(self) -> None:
        """Convert CME symbol.

        Given: Instrument with symbol 'ESM6.CME',
        When: _build_native_symbol is called,
        Then: Returns 'ESM6-CME'.
        """
        schema = _make_schema(symbol="ESM6.CME")
        assert _build_native_symbol(schema) == "ESM6-CME"

    def test_no_dot_returns_none(self) -> None:
        """Return None when symbol has no dot separator.

        Given: Instrument with symbol 'NODOT',
        When: _build_native_symbol is called,
        Then: Returns None.
        """
        schema = _make_schema(symbol="NODOT")
        assert _build_native_symbol(schema) is None


class TestClassifyAssetType:
    """Tests for _classify_asset_type helper."""

    def test_energies_is_commodity(self) -> None:
        """Classify Energies category as COMMODITY.

        Given: Instrument with category='Energies',
        When: _classify_asset_type is called,
        Then: Returns AssetTypeEnum.COMMODITY.
        """
        schema = _make_schema(category="Energies")
        assert _classify_asset_type(schema) == AssetTypeEnum.COMMODITY

    def test_metals_is_commodity(self) -> None:
        """Classify Metals category as COMMODITY.

        Given: Instrument with category='Metals',
        When: _classify_asset_type is called,
        Then: Returns AssetTypeEnum.COMMODITY.
        """
        schema = _make_schema(category="Metals")
        assert _classify_asset_type(schema) == AssetTypeEnum.COMMODITY

    def test_grains_is_commodity(self) -> None:
        """Classify Grains category as COMMODITY.

        Given: Instrument with category='Grains',
        When: _classify_asset_type is called,
        Then: Returns AssetTypeEnum.COMMODITY.
        """
        schema = _make_schema(category="Grains")
        assert _classify_asset_type(schema) == AssetTypeEnum.COMMODITY

    def test_yields_is_yield(self) -> None:
        """Classify Yields category as YIELD.

        Given: Instrument with category='Yields',
        When: _classify_asset_type is called,
        Then: Returns AssetTypeEnum.YIELD.
        """
        schema = _make_schema(category="Yields")
        assert _classify_asset_type(schema) == AssetTypeEnum.YIELD

    def test_indices_is_index(self) -> None:
        """Classify Indices category as INDEX.

        Given: Instrument with category='Indices',
        When: _classify_asset_type is called,
        Then: Returns AssetTypeEnum.INDEX.
        """
        schema = _make_schema(category="Indices")
        assert _classify_asset_type(schema) == AssetTypeEnum.INDEX

    def test_unknown_category_defaults_to_index(self) -> None:
        """Classify unknown category as INDEX (default).

        Given: Instrument with category='Unknown',
        When: _classify_asset_type is called,
        Then: Returns AssetTypeEnum.INDEX.
        """
        schema = _make_schema(category="Unknown")
        assert _classify_asset_type(schema) == AssetTypeEnum.INDEX

    def test_empty_category_defaults_to_index(self) -> None:
        """Classify empty category as INDEX (default).

        Given: Instrument with empty category,
        When: _classify_asset_type is called,
        Then: Returns AssetTypeEnum.INDEX.
        """
        schema = _make_schema(category="")
        assert _classify_asset_type(schema) == AssetTypeEnum.INDEX


class TestServiceMethods:
    """Tests for KrakenEquitiesSymbolUpdaterService class methods."""

    def test_get_setting_key(self) -> None:
        """Verify setting key for Kraken Equities.

        Given: An updater instance,
        When: _get_setting_key is called,
        Then: Returns 'kraken_equities_symbols_last_update'.
        """
        updater = KrakenEquitiesSymbolUpdaterService(update_threshold_hours=6)
        assert updater._get_setting_key() == "kraken_equities_symbols_last_update"

    def test_create_exchange_client(self) -> None:
        """Verify factory creates KrakenEquitiesExchangeClient.

        Given: An updater instance,
        When: _create_exchange_client is called,
        Then: Returns KrakenEquitiesExchangeClient.
        """
        updater = KrakenEquitiesSymbolUpdaterService(update_threshold_hours=6)
        client = updater._create_exchange_client()
        assert isinstance(client, KrakenEquitiesExchangeClient)

    def test_get_default_parameters(self) -> None:
        """Verify default parameters.

        Given: AppSettings mock,
        When: get_default_parameters is called,
        Then: Returns threshold and force keys.
        """
        settings = MagicMock(spec=AppSettings)
        params = KrakenEquitiesSymbolUpdaterService.get_default_parameters(settings)
        assert params["update_threshold_hours"] == 6
        assert params["force"] is False


class TestUpdateDatabase:
    """Tests for _update_database method."""

    @pytest.mark.asyncio
    async def test_update_database_processes_valid_instruments(self) -> None:
        """Process valid instruments and persist to database.

        Given: List with one valid contract,
        When: _update_database is called,
        Then: Symbol, alias, capability, and instrument rows are created.
        """
        updater = KrakenEquitiesSymbolUpdaterService(update_threshold_hours=6)
        mock_repo = MagicMock()
        mock_session = MagicMock()
        mock_repo.get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_repo.get_session.return_value.__exit__ = MagicMock(return_value=False)
        updater.repository = mock_repo

        with (
            patch.object(updater, "_upsert_symbol", return_value="pub-id-1"),
            patch.object(updater, "_upsert_alias", return_value="created"),
            patch.object(updater, "_upsert_capability", return_value="created"),
            patch.object(updater, "_ensure_instrument_identity"),
            patch.object(updater, "_reconcile_capabilities", return_value=0),
        ):
            symbols = [
                {
                    "symbol": "CLM6.NYMEX",
                    "name": "CLM6 19May26",
                    "short_name": "Crude Oil",
                    "contract_name": "CLM6",
                    "tradable": True,
                    "status": "active",
                    "category": "Energies",
                    "exchange": "NYMEX",
                    "tick_size": "0.01",
                    "contract_size": "1000",
                    "base": "USD",
                    "quote": "USD",
                }
            ]
            await updater._update_database(symbols)
            mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_database_skips_invalid_instruments(self) -> None:
        """Skip instruments that fail validation.

        Given: List with one invalid instrument (missing required fields),
        When: _update_database is called,
        Then: Invalid instrument is skipped, commit still happens.
        """
        updater = KrakenEquitiesSymbolUpdaterService(update_threshold_hours=6)
        mock_repo = MagicMock()
        mock_session = MagicMock()
        mock_repo.get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_repo.get_session.return_value.__exit__ = MagicMock(return_value=False)
        updater.repository = mock_repo

        with patch.object(updater, "_reconcile_capabilities", return_value=0):
            await updater._update_database([{"invalid": "data"}])
            mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_database_skips_unmappable_instruments(self) -> None:
        """Skip instruments without dot in symbol (unmappable).

        Given: Instrument with no dot separator in symbol,
        When: _update_database is called,
        Then: Instrument is skipped.
        """
        updater = KrakenEquitiesSymbolUpdaterService(update_threshold_hours=6)
        mock_repo = MagicMock()
        mock_session = MagicMock()
        mock_repo.get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_repo.get_session.return_value.__exit__ = MagicMock(return_value=False)
        updater.repository = mock_repo

        with patch.object(updater, "_reconcile_capabilities", return_value=0):
            symbols = [
                {
                    "symbol": "NODOT",
                    "tradable": True,
                    "status": "active",
                    "category": "Energies",
                }
            ]
            await updater._update_database(symbols)
            mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_database_raises_on_db_error(self) -> None:
        """Propagate database errors.

        Given: Repository that raises on session access,
        When: _update_database is called,
        Then: Exception propagates.
        """
        updater = KrakenEquitiesSymbolUpdaterService(update_threshold_hours=6)
        mock_repo = MagicMock()
        mock_repo.get_session.side_effect = RuntimeError("DB down")
        updater.repository = mock_repo

        with pytest.raises(RuntimeError, match="DB down"):
            await updater._update_database([])

    @pytest.mark.asyncio
    async def test_update_database_raises_when_repository_none(self) -> None:
        """Raise RuntimeError when repository is not initialized.

        Given: Updater with repository=None,
        When: _update_database is called,
        Then: Raises RuntimeError.
        """
        updater = KrakenEquitiesSymbolUpdaterService(update_threshold_hours=6)
        assert updater.repository is None
        with pytest.raises(RuntimeError, match="Repository not initialized"):
            await updater._update_database([])
