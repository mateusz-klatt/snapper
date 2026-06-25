"""Tests for Kraken Futures symbol updater."""

from datetime import UTC
from datetime import datetime
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from snapper.application.updaters.symbols.kraken_futures import KrakenFuturesSymbolUpdaterService
from snapper.application.updaters.symbols.kraken_futures import _build_ccxt_symbol
from snapper.application.updaters.symbols.kraken_futures import _build_native_symbol
from snapper.application.updaters.symbols.kraken_futures import _classify_asset_type
from snapper.application.updaters.symbols.kraken_futures import _extract_expiry
from snapper.application.updaters.symbols.kraken_futures import _normalize_currency
from snapper.application.updaters.symbols.kraken_futures import _parse_expiry_datetime
from snapper.config.app import AppSettings
from snapper.core.types import AliasChannelEnum
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Base
from snapper.data.models import SymbolExchangeCapability
from snapper.data.models import SymbolMarketDataChannelCapability
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)
from snapper.infrastructure.exchanges.schemas.kraken_futures import KrakenFuturesInstrumentSchema


def _make_schema(**overrides: object) -> KrakenFuturesInstrumentSchema:
    """Create a minimal valid instrument schema with overrides."""
    defaults = {
        "symbol": "PF_XBTUSD",
        "type": "futures_vanilla",
        "tickSize": 0.5,
        "contractSize": 1,
        "tradeable": True,
        "base": "XBT",
        "quote": "USD",
    }
    defaults.update(overrides)
    return KrakenFuturesInstrumentSchema.model_validate(defaults)


class TestNormalizeCurrency:
    """Tests for _normalize_currency helper."""

    def test_xbt_to_btc(self) -> None:
        """Map XBT to BTC.

        Given: Raw currency 'XBT',
        When: _normalize_currency is called,
        Then: Returns 'BTC'.
        """
        assert _normalize_currency("XBT") == "BTC"

    def test_lowercase_normalized(self) -> None:
        """Uppercase raw currency.

        Given: Raw currency 'eth',
        When: _normalize_currency is called,
        Then: Returns 'ETH'.
        """
        assert _normalize_currency("eth") == "ETH"

    def test_already_normal(self) -> None:
        """Pass through normal currency.

        Given: Raw currency 'USD',
        When: _normalize_currency is called,
        Then: Returns 'USD'.
        """
        assert _normalize_currency("USD") == "USD"


class TestBuildNativeSymbol:
    """Tests for _build_native_symbol metadata-driven mapping."""

    def test_perpetual_linear(self) -> None:
        """Map linear perpetual to BTC-USD-PERP.

        Given: Instrument with type=futures_vanilla, no last_trading_time,
        When: _build_native_symbol is called,
        Then: Returns 'BTC-USD-PERP'.
        """
        schema = _make_schema(symbol="PF_XBTUSD", type="futures_vanilla")
        assert _build_native_symbol(schema) == "BTC-USD-PERP"

    def test_perpetual_inverse(self) -> None:
        """Map inverse perpetual to BTC-USD-PERP-INV.

        Given: Instrument with type=futures_inverse, no last_trading_time,
        When: _build_native_symbol is called,
        Then: Returns 'BTC-USD-PERP-INV'.
        """
        schema = _make_schema(symbol="PI_XBTUSD", type="futures_inverse")
        assert _build_native_symbol(schema) == "BTC-USD-PERP-INV"

    def test_fixed_maturity_linear(self) -> None:
        """Map linear future with expiry.

        Given: Instrument with type=futures_vanilla and last_trading_time,
        When: _build_native_symbol is called,
        Then: Returns 'BTC-USD-250627'.
        """
        schema = _make_schema(
            symbol="FF_XBTUSD_250627",
            type="futures_vanilla",
            lastTradingTime="2025-06-27T16:00:00Z",
        )
        assert _build_native_symbol(schema) == "BTC-USD-250627"

    def test_fixed_maturity_inverse(self) -> None:
        """Map inverse future with expiry.

        Given: Instrument with type=futures_inverse and last_trading_time,
        When: _build_native_symbol is called,
        Then: Returns 'BTC-USD-250627-INV'.
        """
        schema = _make_schema(
            symbol="FI_XBTUSD_250627",
            type="futures_inverse",
            lastTradingTime="2025-06-27T16:00:00Z",
        )
        assert _build_native_symbol(schema) == "BTC-USD-250627-INV"

    def test_reference_rate(self) -> None:
        """Map reference rate symbol.

        Given: Instrument with symbol starting with rr_,
        When: _build_native_symbol is called,
        Then: Returns 'BTC-USD-RR'.
        """
        schema = _make_schema(symbol="rr_xbtusd")
        assert _build_native_symbol(schema) == "BTC-USD-RR"

    def test_index(self) -> None:
        """Map index symbol.

        Given: Instrument with symbol starting with in_,
        When: _build_native_symbol is called,
        Then: Returns 'BTC-USD-IDX'.
        """
        schema = _make_schema(symbol="in_xbtusd")
        assert _build_native_symbol(schema) == "BTC-USD-IDX"

    def test_missing_base_returns_none(self) -> None:
        """Return None when base currency is missing.

        Given: Instrument without base field,
        When: _build_native_symbol is called,
        Then: Returns None.
        """
        schema = _make_schema(base=None)
        assert _build_native_symbol(schema) is None

    def test_missing_quote_returns_none(self) -> None:
        """Return None when quote currency is missing.

        Given: Instrument without quote field,
        When: _build_native_symbol is called,
        Then: Returns None.
        """
        schema = _make_schema(quote=None)
        assert _build_native_symbol(schema) is None

    def test_unknown_type_without_expiry_returns_none(self) -> None:
        """Return None for unrecognized type without expiry.

        Given: Instrument with unknown type and last_trading_time set but invalid,
        When: _build_native_symbol is called,
        Then: Returns None if expiry cannot be extracted.
        """
        schema = _make_schema(
            symbol="XX_XBTUSD",
            type="unknown_type",
            lastTradingTime="invalid-date",
        )
        assert _build_native_symbol(schema) is None


class TestExtractExpiry:
    """Tests for _extract_expiry helper."""

    def test_valid_iso_date(self) -> None:
        """Extract YYMMDD from ISO 8601 date.

        Given: Instrument with lastTradingTime ISO date,
        When: _extract_expiry is called,
        Then: Returns 'YYMMDD' string.
        """
        schema = _make_schema(lastTradingTime="2025-06-27T16:00:00Z")
        assert _extract_expiry(schema) == "250627"

    def test_no_last_trading_time(self) -> None:
        """Return None when no expiry.

        Given: Instrument without lastTradingTime,
        When: _extract_expiry is called,
        Then: Returns None.
        """
        schema = _make_schema()
        assert _extract_expiry(schema) is None

    def test_invalid_date_returns_none(self) -> None:
        """Return None for invalid date.

        Given: Instrument with invalid lastTradingTime,
        When: _extract_expiry is called,
        Then: Returns None.
        """
        schema = _make_schema(lastTradingTime="not-a-date")
        assert _extract_expiry(schema) is None


class TestParseExpiryDatetime:
    """Tests for _parse_expiry_datetime helper."""

    def test_invalid_date_returns_none(self) -> None:
        """Invalid ISO timestamps are ignored when parsing expiry datetimes."""
        schema = _make_schema(lastTradingTime="not-a-date")

        assert _parse_expiry_datetime(schema) is None


class TestClassifyAssetType:
    """Tests for _classify_asset_type helper."""

    def test_tradfi_returns_index(self) -> None:
        """Classify TradFi product as INDEX.

        Given: Instrument with tradfi=True,
        When: _classify_asset_type is called,
        Then: Returns AssetTypeEnum.INDEX.
        """
        schema = _make_schema(tradfi=True)
        assert _classify_asset_type(schema) == "index"

    def test_crypto_default(self) -> None:
        """Classify non-TradFi product as CRYPTO.

        Given: Instrument with tradfi=False and normal symbol,
        When: _classify_asset_type is called,
        Then: Returns AssetTypeEnum.CRYPTO.
        """
        schema = _make_schema(tradfi=False)
        assert _classify_asset_type(schema) == "crypto"

    def test_reference_rate_returns_index(self) -> None:
        """Classify reference rate as INDEX.

        Given: Instrument with rr_ prefix,
        When: _classify_asset_type is called,
        Then: Returns AssetTypeEnum.INDEX.
        """
        schema = _make_schema(symbol="rr_xbtusd", tradfi=False)
        assert _classify_asset_type(schema) == "index"

    def test_index_product_returns_index(self) -> None:
        """Classify index product as INDEX.

        Given: Instrument with in_ prefix,
        When: _classify_asset_type is called,
        Then: Returns AssetTypeEnum.INDEX.
        """
        schema = _make_schema(symbol="in_xbtusd", tradfi=False)
        assert _classify_asset_type(schema) == "index"


class TestServiceMethods:
    """Tests for KrakenFuturesSymbolUpdaterService class methods."""

    def test_get_setting_key(self) -> None:
        """Verify setting key for Kraken Futures.

        Given: An updater instance,
        When: _get_setting_key is called,
        Then: Returns 'kraken_futures_symbols_last_update'.
        """
        updater = KrakenFuturesSymbolUpdaterService(update_threshold_hours=6)
        assert updater._get_setting_key() == "kraken_futures_symbols_last_update"

    def test_create_exchange_client(self) -> None:
        """Verify factory creates KrakenFuturesExchangeClient.

        Given: An updater instance,
        When: _create_exchange_client is called,
        Then: Returns KrakenFuturesExchangeClient.
        """
        updater = KrakenFuturesSymbolUpdaterService(update_threshold_hours=6)
        client = updater._create_exchange_client()
        assert isinstance(client, KrakenFuturesExchangeClient)

    def test_get_default_parameters(self) -> None:
        """Verify default parameters.

        Given: AppSettings mock,
        When: get_default_parameters is called,
        Then: Returns threshold and force keys.
        """
        settings = MagicMock(spec=AppSettings)
        params = KrakenFuturesSymbolUpdaterService.get_default_parameters(settings)
        assert params["update_threshold_hours"] == 6
        assert params["force"] is False


class TestUpdateDatabase:
    """Tests for _update_database method."""

    @pytest.mark.asyncio
    async def test_update_database_processes_valid_instruments(self) -> None:
        """Process valid instruments and persist to database.

        Given: List with one valid perpetual instrument,
        When: _update_database is called,
        Then: Symbol, alias, capability, and instrument rows are created.
        """
        updater = KrakenFuturesSymbolUpdaterService(update_threshold_hours=6)
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
                    "symbol": "PF_XBTUSD",
                    "type": "futures_vanilla",
                    "tickSize": 0.5,
                    "contractSize": 1,
                    "tradeable": True,
                    "base": "XBT",
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
        updater = KrakenFuturesSymbolUpdaterService(update_threshold_hours=6)
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
        """Skip instruments without base/quote metadata.

        Given: Instrument with missing base currency,
        When: _update_database is called,
        Then: Instrument is skipped.
        """
        updater = KrakenFuturesSymbolUpdaterService(update_threshold_hours=6)
        mock_repo = MagicMock()
        mock_session = MagicMock()
        mock_repo.get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_repo.get_session.return_value.__exit__ = MagicMock(return_value=False)
        updater.repository = mock_repo

        with patch.object(updater, "_reconcile_capabilities", return_value=0):
            symbols = [
                {
                    "symbol": "UNKNOWN_PROD",
                    "type": "unknown",
                    "tickSize": 1.0,
                    "contractSize": 1,
                    "tradeable": True,
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
        updater = KrakenFuturesSymbolUpdaterService(update_threshold_hours=6)
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
        updater = KrakenFuturesSymbolUpdaterService(update_threshold_hours=6)
        assert updater.repository is None
        with pytest.raises(RuntimeError, match="Repository not initialized"):
            await updater._update_database([])

    @pytest.mark.asyncio
    async def test_update_database_upserts_ccxt_alias_when_available(self) -> None:
        """Upsert CCXT alias when _build_ccxt_symbol returns non-None.

        Given: Valid perpetual instrument (linear, has CCXT symbol),
        When: _update_database is called,
        Then: _upsert_alias is called with CCXT channel.
        """
        updater = KrakenFuturesSymbolUpdaterService(update_threshold_hours=6)
        mock_repo = MagicMock()
        mock_session = MagicMock()
        mock_repo.get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_repo.get_session.return_value.__exit__ = MagicMock(return_value=False)
        updater.repository = mock_repo

        with (
            patch.object(updater, "_upsert_symbol", return_value="pub-id-1"),
            patch.object(updater, "_upsert_alias", return_value="created") as mock_alias,
            patch.object(updater, "_upsert_capability", return_value="created"),
            patch.object(updater, "_ensure_instrument_identity"),
            patch.object(updater, "_reconcile_capabilities", return_value=0),
        ):
            symbols = [
                {
                    "symbol": "PF_XBTUSD",
                    "type": "futures_vanilla",
                    "tickSize": 0.5,
                    "contractSize": 1,
                    "tradeable": True,
                    "base": "XBT",
                    "quote": "USD",
                }
            ]
            await updater._update_database(symbols)
            ccxt_calls = [
                c for c in mock_alias.call_args_list if c.args[3] == AliasChannelEnum.CCXT
            ]
            assert len(ccxt_calls) == 1
            assert ccxt_calls[0].args[4] == "BTC/USD:USD"

    @pytest.mark.asyncio
    async def test_update_database_skips_ccxt_alias_for_reference_rate(self) -> None:
        """Skip CCXT alias when _build_ccxt_symbol returns None.

        Given: Reference rate instrument (rr_ prefix, no CCXT representation),
        When: _update_database is called,
        Then: _upsert_alias is called only for WS channel, not CCXT.
        """
        updater = KrakenFuturesSymbolUpdaterService(update_threshold_hours=6)
        mock_repo = MagicMock()
        mock_session = MagicMock()
        mock_repo.get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_repo.get_session.return_value.__exit__ = MagicMock(return_value=False)
        updater.repository = mock_repo

        with (
            patch.object(updater, "_upsert_symbol", return_value="pub-id-rr"),
            patch.object(updater, "_upsert_alias", return_value="created") as mock_alias,
            patch.object(updater, "_upsert_capability", return_value="created"),
            patch.object(updater, "_ensure_instrument_identity"),
            patch.object(updater, "_reconcile_capabilities", return_value=0),
        ):
            symbols = [
                {
                    "symbol": "rr_xbtusd",
                    "type": "futures_vanilla",
                    "tickSize": 0.01,
                    "contractSize": 1,
                    "tradeable": True,
                    "base": "XBT",
                    "quote": "USD",
                }
            ]
            await updater._update_database(symbols)
            ccxt_calls = [
                c for c in mock_alias.call_args_list if c.args[3] == AliasChannelEnum.CCXT
            ]
            assert len(ccxt_calls) == 0

    @pytest.mark.asyncio
    async def test_update_database_sets_future_kind_for_dated_contract(self) -> None:
        """Set instrument_kind='future' for dated contracts with valid expiry.

        Given: A dated futures instrument with lastTradingTime,
        When: _update_database is called,
        Then: _revise_instrument_spec is called with kind='future' and parsed expiry.
        """
        updater = KrakenFuturesSymbolUpdaterService(update_threshold_hours=6)
        mock_repo = MagicMock()
        mock_session = MagicMock()
        mock_repo.get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_repo.get_session.return_value.__exit__ = MagicMock(return_value=False)
        updater.repository = mock_repo

        with (
            patch.object(updater, "_upsert_symbol", return_value="pub-id-1"),
            patch.object(updater, "_upsert_alias", return_value="created"),
            patch.object(updater, "_upsert_capability", return_value="created"),
            patch.object(updater, "_ensure_instrument_identity", return_value="inst-1"),
            patch.object(updater, "_revise_instrument_spec") as mock_spec,
            patch.object(updater, "_reconcile_capabilities", return_value=0),
        ):
            symbols = [
                {
                    "symbol": "FI_XBTUSD_260620",
                    "type": "futures_vanilla",
                    "tickSize": 0.5,
                    "contractSize": 1,
                    "tradeable": True,
                    "base": "XBT",
                    "quote": "USD",
                    "lastTradingTime": "2026-06-20T16:30:00.000Z",
                }
            ]
            await updater._update_database(symbols)
            mock_spec.assert_called_once()
            call_kwargs = mock_spec.call_args
            assert call_kwargs.kwargs["instrument_kind"] == "future"
            assert call_kwargs.kwargs["expiry_at"] is not None

    @pytest.mark.asyncio
    async def test_update_database_sets_funding_fields_for_perpetual(self) -> None:
        """Set funding_type and funding_frequency_hours for perpetuals.

        Given: A perpetual futures instrument (no lastTradingTime),
        When: _update_database is called,
        Then: _revise_instrument_spec receives perpetual_funding fields.
        """
        updater = KrakenFuturesSymbolUpdaterService(update_threshold_hours=6)
        mock_repo = MagicMock()
        mock_session = MagicMock()
        mock_repo.get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_repo.get_session.return_value.__exit__ = MagicMock(return_value=False)
        updater.repository = mock_repo

        with (
            patch.object(updater, "_upsert_symbol", return_value="pub-id-1"),
            patch.object(updater, "_upsert_alias", return_value="created"),
            patch.object(updater, "_upsert_capability", return_value="created"),
            patch.object(updater, "_ensure_instrument_identity", return_value="inst-1"),
            patch.object(updater, "_revise_instrument_spec") as mock_spec,
            patch.object(updater, "_reconcile_capabilities", return_value=0),
        ):
            symbols = [
                {
                    "symbol": "PF_XBTUSD",
                    "type": "futures_vanilla",
                    "tickSize": 0.5,
                    "contractSize": 1,
                    "tradeable": True,
                    "base": "XBT",
                    "quote": "USD",
                }
            ]
            await updater._update_database(symbols)
            mock_spec.assert_called_once()
            call_kwargs = mock_spec.call_args
            assert call_kwargs.kwargs["instrument_kind"] == "perpetual"
            assert call_kwargs.kwargs["funding_type"] == "perpetual_funding"
            assert call_kwargs.kwargs["funding_frequency_hours"] == 1
            assert call_kwargs.kwargs["max_funding_rate"] == pytest.approx(0.0025)

    @pytest.mark.asyncio
    async def test_update_database_no_funding_fields_for_dated_future(self) -> None:
        """Dated futures do not receive funding fields.

        Given: A dated futures instrument with lastTradingTime,
        When: _update_database is called,
        Then: _revise_instrument_spec does NOT receive funding_type.
        """
        updater = KrakenFuturesSymbolUpdaterService(update_threshold_hours=6)
        mock_repo = MagicMock()
        mock_session = MagicMock()
        mock_repo.get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_repo.get_session.return_value.__exit__ = MagicMock(return_value=False)
        updater.repository = mock_repo

        with (
            patch.object(updater, "_upsert_symbol", return_value="pub-id-1"),
            patch.object(updater, "_upsert_alias", return_value="created"),
            patch.object(updater, "_upsert_capability", return_value="created"),
            patch.object(updater, "_ensure_instrument_identity", return_value="inst-1"),
            patch.object(updater, "_revise_instrument_spec") as mock_spec,
            patch.object(updater, "_reconcile_capabilities", return_value=0),
        ):
            symbols = [
                {
                    "symbol": "FI_XBTUSD_260620",
                    "type": "futures_vanilla",
                    "tickSize": 0.5,
                    "contractSize": 1,
                    "tradeable": True,
                    "base": "XBT",
                    "quote": "USD",
                    "lastTradingTime": "2026-06-20T16:30:00.000Z",
                }
            ]
            await updater._update_database(symbols)
            mock_spec.assert_called_once()
            call_kwargs = mock_spec.call_args
            assert call_kwargs.kwargs["funding_type"] is None
            assert call_kwargs.kwargs["funding_frequency_hours"] is None
            assert call_kwargs.kwargs["max_funding_rate"] is None


class TestRuntimeTradeChannelRevalidation:
    """Tests for updater-driven runtime trade-channel reset."""

    def test_close_runtime_trade_channel_capabilities_empty_processed_set(self) -> None:
        """Return zero when no catalog symbols were processed."""
        session = MagicMock()
        closed = KrakenFuturesSymbolUpdaterService._close_runtime_trade_channel_capabilities(
            session,
            set(),
            datetime.now(UTC),
        )
        assert closed == 0
        session.execute.assert_not_called()

    def test_close_runtime_trade_channel_capabilities_handles_missing_rowcount(self) -> None:
        """Non-integer rowcount is treated as zero closed rows."""
        session = MagicMock()
        session.execute.return_value.rowcount = None
        closed = KrakenFuturesSymbolUpdaterService._close_runtime_trade_channel_capabilities(
            session,
            {"11111111-1111-7111-8111-111111111111"},
            datetime.now(UTC),
        )
        assert closed == 0

    def test_close_runtime_trade_channel_capabilities_closes_only_runtime_trade_rows(
        self,
    ) -> None:
        """Close only current runtime-learned trade denials for market-data symbols."""
        engine = create_engine("sqlite:///:memory:", future=True)
        Base.metadata.create_all(engine)
        session_factory = sessionmaker(bind=engine, future=True)
        now = datetime.now(UTC)
        current_symbol = "11111111-1111-7111-8111-111111111111"
        disabled_symbol = "22222222-2222-7222-8222-222222222222"
        unprocessed_symbol = "33333333-3333-7333-8333-333333333333"
        manual_symbol = "66666666-6666-7666-8666-666666666666"
        try:
            with session_factory() as session:
                session.add_all(
                    [
                        SymbolExchangeCapability(
                            symbol_public_id=current_symbol,
                            exchange="kraken_futures",
                            can_market_data=True,
                            can_trade=False,
                            source="updater",
                            reason=None,
                            created_at=now,
                            session_id="44444444-4444-7444-8444-444444444444",
                            sequence_id=1,
                            timestamp=now,
                            known_to=KNOWN_TO_MAX,
                        ),
                        SymbolExchangeCapability(
                            symbol_public_id=disabled_symbol,
                            exchange="kraken_futures",
                            can_market_data=False,
                            can_trade=False,
                            source="updater",
                            reason="delisted",
                            created_at=now,
                            session_id="44444444-4444-7444-8444-444444444444",
                            sequence_id=2,
                            timestamp=now,
                            known_to=KNOWN_TO_MAX,
                        ),
                        SymbolExchangeCapability(
                            symbol_public_id=unprocessed_symbol,
                            exchange="kraken_futures",
                            can_market_data=True,
                            can_trade=False,
                            source="updater",
                            reason=None,
                            created_at=now,
                            session_id="44444444-4444-7444-8444-444444444444",
                            sequence_id=3,
                            timestamp=now,
                            known_to=KNOWN_TO_MAX,
                        ),
                        SymbolExchangeCapability(
                            symbol_public_id=manual_symbol,
                            exchange="kraken_futures",
                            can_market_data=True,
                            can_trade=False,
                            source="updater",
                            reason=None,
                            created_at=now,
                            session_id="44444444-4444-7444-8444-444444444444",
                            sequence_id=4,
                            timestamp=now,
                            known_to=KNOWN_TO_MAX,
                        ),
                    ]
                )
                channel_rows = [
                    SymbolMarketDataChannelCapability(
                        symbol_public_id=current_symbol,
                        exchange="kraken_futures",
                        channel="trade",
                        can_market_data=False,
                        source="kraken_futures_publisher_runtime",
                        reason="learned",
                        created_at=now,
                        session_id="55555555-5555-7555-8555-555555555555",
                        sequence_id=1,
                        timestamp=now,
                        known_to=KNOWN_TO_MAX,
                    ),
                    SymbolMarketDataChannelCapability(
                        symbol_public_id=disabled_symbol,
                        exchange="kraken_futures",
                        channel="trade",
                        can_market_data=False,
                        source="kraken_futures_publisher_runtime",
                        reason="learned",
                        created_at=now,
                        session_id="55555555-5555-7555-8555-555555555555",
                        sequence_id=2,
                        timestamp=now,
                        known_to=KNOWN_TO_MAX,
                    ),
                    SymbolMarketDataChannelCapability(
                        symbol_public_id=unprocessed_symbol,
                        exchange="kraken_futures",
                        channel="trade",
                        can_market_data=False,
                        source="kraken_futures_publisher_runtime",
                        reason="learned",
                        created_at=now,
                        session_id="55555555-5555-7555-8555-555555555555",
                        sequence_id=3,
                        timestamp=now,
                        known_to=KNOWN_TO_MAX,
                    ),
                    SymbolMarketDataChannelCapability(
                        symbol_public_id=current_symbol,
                        exchange="kraken_futures",
                        channel="ticker",
                        can_market_data=False,
                        source="kraken_futures_publisher_runtime",
                        reason="learned",
                        created_at=now,
                        session_id="55555555-5555-7555-8555-555555555555",
                        sequence_id=4,
                        timestamp=now,
                        known_to=KNOWN_TO_MAX,
                    ),
                    SymbolMarketDataChannelCapability(
                        symbol_public_id=manual_symbol,
                        exchange="kraken_futures",
                        channel="trade",
                        can_market_data=False,
                        source="manual",
                        reason="operator",
                        created_at=now,
                        session_id="55555555-5555-7555-8555-555555555555",
                        sequence_id=5,
                        timestamp=now,
                        known_to=KNOWN_TO_MAX,
                    ),
                ]
                session.add_all(channel_rows)
                session.commit()
                closed = (
                    KrakenFuturesSymbolUpdaterService._close_runtime_trade_channel_capabilities(
                        session,
                        {current_symbol, disabled_symbol, manual_symbol},
                        datetime.now(UTC),
                    )
                )
                session.commit()
                rows = session.execute(
                    select(SymbolMarketDataChannelCapability)
                    .where(SymbolMarketDataChannelCapability.exchange == "kraken_futures")
                    .order_by(SymbolMarketDataChannelCapability.sequence_id)
                ).scalars()
                known_to_by_sequence = {row.sequence_id: row.known_to for row in rows}
            assert closed == 1
            assert known_to_by_sequence[1] != KNOWN_TO_MAX
            assert known_to_by_sequence[2] == KNOWN_TO_MAX
            assert known_to_by_sequence[3] == KNOWN_TO_MAX
            assert known_to_by_sequence[4] == KNOWN_TO_MAX
            assert known_to_by_sequence[5] == KNOWN_TO_MAX
        finally:
            engine.dispose()

    def test_close_runtime_trade_channel_capabilities_preserves_non_runtime_false_row(
        self,
    ) -> None:
        """Do not reopen operator-owned trade channel denials."""
        engine = create_engine("sqlite:///:memory:", future=True)
        Base.metadata.create_all(engine)
        session_factory = sessionmaker(bind=engine, future=True)
        now = datetime.now(UTC)
        symbol_public_id = "77777777-7777-7777-8777-777777777777"
        try:
            with session_factory() as session:
                session.add(
                    SymbolExchangeCapability(
                        symbol_public_id=symbol_public_id,
                        exchange="kraken_futures",
                        can_market_data=True,
                        can_trade=False,
                        source="updater",
                        reason=None,
                        created_at=now,
                        session_id="88888888-8888-7888-8888-888888888888",
                        sequence_id=1,
                        timestamp=now,
                        known_to=KNOWN_TO_MAX,
                    )
                )
                session.add(
                    SymbolMarketDataChannelCapability(
                        symbol_public_id=symbol_public_id,
                        exchange="kraken_futures",
                        channel="trade",
                        can_market_data=False,
                        source="operator",
                        reason="disabled by operator",
                        created_at=now,
                        session_id="88888888-8888-7888-8888-888888888888",
                        sequence_id=2,
                        timestamp=now,
                        known_to=KNOWN_TO_MAX,
                    )
                )
                session.commit()

                closed = (
                    KrakenFuturesSymbolUpdaterService._close_runtime_trade_channel_capabilities(
                        session,
                        {symbol_public_id},
                        datetime.now(UTC),
                    )
                )
                session.commit()
                row = session.execute(
                    select(SymbolMarketDataChannelCapability).where(
                        SymbolMarketDataChannelCapability.symbol_public_id == symbol_public_id
                    )
                ).scalar_one()
            assert closed == 0
            assert row.known_to == KNOWN_TO_MAX
        finally:
            engine.dispose()


class TestBuildCcxtSymbol:
    """Tests for _build_ccxt_symbol helper."""

    def test_perpetual_linear(self) -> None:
        """Build CCXT symbol for linear perpetual.

        Given: Instrument with base=XBT, quote=USD, type=futures_vanilla,
        When: _build_ccxt_symbol is called,
        Then: Returns 'BTC/USD:USD'.
        """
        schema = _make_schema(base="XBT", quote="USD", type="futures_vanilla")
        assert _build_ccxt_symbol(schema) == "BTC/USD:USD"

    def test_perpetual_inverse(self) -> None:
        """Build CCXT symbol for inverse perpetual.

        Given: Instrument with base=XBT, quote=USD, type=futures_inverse,
        When: _build_ccxt_symbol is called,
        Then: Returns 'BTC/USD:BTC'.
        """
        schema = _make_schema(base="XBT", quote="USD", type="futures_inverse")
        assert _build_ccxt_symbol(schema) == "BTC/USD:BTC"

    def test_reference_rate_returns_none(self) -> None:
        """Return None for reference rate instruments.

        Given: Instrument with rr_ prefix symbol,
        When: _build_ccxt_symbol is called,
        Then: Returns None.
        """
        schema = _make_schema(symbol="rr_xbtusd")
        assert _build_ccxt_symbol(schema) is None

    def test_index_returns_none(self) -> None:
        """Return None for index instruments.

        Given: Instrument with in_ prefix symbol,
        When: _build_ccxt_symbol is called,
        Then: Returns None.
        """
        schema = _make_schema(symbol="in_xbtusd")
        assert _build_ccxt_symbol(schema) is None

    def test_missing_base_returns_none(self) -> None:
        """Return None when base currency is missing.

        Given: Instrument without base field,
        When: _build_ccxt_symbol is called,
        Then: Returns None.
        """
        schema = _make_schema(base=None)
        assert _build_ccxt_symbol(schema) is None

    def test_missing_quote_returns_none(self) -> None:
        """Return None when quote currency is missing.

        Given: Instrument without quote field,
        When: _build_ccxt_symbol is called,
        Then: Returns None.
        """
        schema = _make_schema(quote=None)
        assert _build_ccxt_symbol(schema) is None

    def test_dated_futures_returns_none(self) -> None:
        """Return None for dated (non-perpetual) futures.

        Given: Instrument with last_trading_time set (dated future),
        When: _build_ccxt_symbol is called,
        Then: Returns None (CCXT alias only for perpetuals).
        """
        schema = _make_schema(last_trading_time="2026-06-26T16:00:00Z")
        assert _build_ccxt_symbol(schema) is None
