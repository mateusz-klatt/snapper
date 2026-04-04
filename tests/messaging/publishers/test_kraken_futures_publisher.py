"""Unit tests for KrakenFuturesMarketDataPublisher."""

from unittest.mock import MagicMock
from unittest.mock import patch

from snapper.config.app import AppSettings
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)
from snapper.messaging.publishers.kraken_futures import KrakenFuturesMarketDataPublisher


class TestKrakenFuturesMarketDataPublisher:
    """Tests for KrakenFuturesMarketDataPublisher functionality."""

    def test_create_exchange_client_returns_futures_client(self) -> None:
        """Verify factory method creates KrakenFuturesExchangeClient.

        Given: A KrakenFuturesMarketDataPublisher instance,
        When: _create_exchange_client is called,
        Then: Returns a KrakenFuturesExchangeClient instance.
        """
        publisher = KrakenFuturesMarketDataPublisher(symbols=["BTC-USD-PERP"])
        with patch(
            "snapper.messaging.publishers.kraken_futures.KrakenFuturesExchangeClient"
        ) as mock_cls:
            mock_cls.return_value = MagicMock(spec=KrakenFuturesExchangeClient)
            client = publisher._create_exchange_client()
            assert isinstance(client, KrakenFuturesExchangeClient)

    def test_get_exchange_name_returns_kraken_futures(self) -> None:
        """Verify exchange name returns 'kraken_futures'.

        Given: A KrakenFuturesMarketDataPublisher instance,
        When: _get_exchange_name is called,
        Then: Returns 'kraken_futures'.
        """
        publisher = KrakenFuturesMarketDataPublisher(symbols=["BTC-USD-PERP"])
        assert publisher._get_exchange_name() == "kraken_futures"

    def test_validate_symbols_filters_invalid(self) -> None:
        """Verify invalid symbols are filtered out.

        Given: Publisher and symbols including one that raises ValueError,
        When: _validate_symbols is called,
        Then: Invalid symbols are excluded.
        """
        publisher = KrakenFuturesMarketDataPublisher(symbols=[])

        def _lookup(s: str) -> str:
            if s == "INVALID":
                raise ValueError("unknown")
            return s

        with patch(
            "snapper.messaging.publishers.kraken_futures.native_to_kraken_futures_ws",
            side_effect=_lookup,
        ):
            valid = publisher._validate_symbols(["BTC-USD-PERP", "INVALID", "ETH-USD-PERP"])
        assert valid == ["BTC-USD-PERP", "ETH-USD-PERP"]

    def test_validate_symbols_removes_duplicates(self) -> None:
        """Verify duplicate symbols are removed.

        Given: Symbols list with duplicates,
        When: _validate_symbols is called,
        Then: Duplicates removed.
        """
        publisher = KrakenFuturesMarketDataPublisher(symbols=[])
        with patch(
            "snapper.messaging.publishers.kraken_futures.native_to_kraken_futures_ws",
            return_value="PF_XBTUSD",
        ):
            valid = publisher._validate_symbols(["BTC-USD-PERP", "BTC-USD-PERP", "ETH-USD-PERP"])
        assert len(valid) == 2
        assert valid == ["BTC-USD-PERP", "ETH-USD-PERP"]

    def test_get_max_symbols_per_connection_returns_zero(self) -> None:
        """Verify Kraken Futures WS has no documented symbol limit.

        Given: A KrakenFuturesMarketDataPublisher instance,
        When: _get_max_symbols_per_connection is called,
        Then: Returns 0 (unlimited).
        """
        publisher = KrakenFuturesMarketDataPublisher(symbols=["BTC-USD-PERP"])
        assert publisher._get_max_symbols_per_connection() == 0

    def test_get_default_parameters_from_settings(self) -> None:
        """Verify default parameters extracts kraken_futures symbols.

        Given: Settings with instruments.kraken_futures containing symbols,
        When: get_default_parameters is called,
        Then: Returns dict with those symbols.
        """
        mock_settings = MagicMock(spec=AppSettings)
        mock_settings.instruments = {"kraken_futures": ["BTC-USD-PERP"], "kraken": ["BTC-USD"]}
        params = KrakenFuturesMarketDataPublisher.get_default_parameters(mock_settings)
        assert params == {"symbols": ["BTC-USD-PERP"]}

    def test_get_default_parameters_with_empty_list(self) -> None:
        """Verify default parameters handles empty instrument list.

        Given: Settings with empty kraken_futures instruments,
        When: get_default_parameters is called,
        Then: Returns dict with empty symbols list.
        """
        mock_settings = MagicMock(spec=AppSettings)
        mock_settings.instruments = {"kraken_futures": [], "kraken": ["BTC-USD"]}
        params = KrakenFuturesMarketDataPublisher.get_default_parameters(mock_settings)
        assert params == {"symbols": []}

    def test_get_default_parameters_with_missing_key(self) -> None:
        """Verify default parameters handles missing kraken_futures key.

        Given: Settings without kraken_futures key in instruments,
        When: get_default_parameters is called,
        Then: Returns dict with empty symbols list.
        """
        mock_settings = MagicMock(spec=AppSettings)
        mock_settings.instruments = {"kraken": ["BTC-USD"]}
        params = KrakenFuturesMarketDataPublisher.get_default_parameters(mock_settings)
        assert params == {"symbols": []}

    def test_candle_loop_uses_base_class(self) -> None:
        """Candle loop is inherited from base class (no override).

        Given: A KrakenFuturesMarketDataPublisher instance,
        When: Checking _candle_loop,
        Then: It is not overridden (uses base class polling via subscribe_candles).
        """
        assert "_candle_loop" not in KrakenFuturesMarketDataPublisher.__dict__
