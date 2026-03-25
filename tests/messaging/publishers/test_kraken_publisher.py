"""Unit tests for KrakenMarketDataPublisher."""

from unittest.mock import MagicMock
from unittest.mock import patch

from snapper.config.app import AppSettings
from snapper.infrastructure.exchanges.implementations.kraken import KrakenExchangeClient
from snapper.messaging.publishers.kraken import KrakenMarketDataPublisher


class TestKrakenMarketDataPublisher:
    """Tests for KrakenMarketDataPublisher functionality."""

    def test_create_exchange_client_returns_kraken_client(self) -> None:
        """Verify factory method creates KrakenExchangeClient.

        Given a KrakenMarketDataPublisher instance,
        When _create_exchange_client is called,
        Then it returns a KrakenExchangeClient instance.
        """
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        client = publisher._create_exchange_client()
        assert isinstance(client, KrakenExchangeClient)

    def test_get_exchange_name_returns_kraken(self) -> None:
        """Verify exchange name returns 'kraken'.

        Given a KrakenMarketDataPublisher instance,
        When _get_exchange_name is called,
        Then it returns the literal string 'kraken'.
        """
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        assert publisher._get_exchange_name() == "kraken"

    def test_validate_symbols_filters_invalid(self) -> None:
        """Verify invalid symbols are filtered out during validation.

        Given a publisher and symbols including one that raises ValueError,
        When _validate_symbols is called,
        Then invalid symbols are excluded from result.
        """
        publisher = KrakenMarketDataPublisher(symbols=[])
        with patch(
            "snapper.messaging.publishers.kraken.native_to_kraken_websocket",
            side_effect=lambda s: s if s != "INVALID" else (_ for _ in ()).throw(ValueError()),
        ):
            valid_symbols = publisher._validate_symbols(["BTC-USD", "INVALID", "ETH-USD"])
        assert valid_symbols == ["BTC-USD", "ETH-USD"]

    def test_validate_symbols_removes_duplicates(self) -> None:
        """Verify duplicate symbols are removed during validation.

        Given a publisher and symbols list with duplicates,
        When _validate_symbols is called,
        Then duplicates are removed from result.
        """
        publisher = KrakenMarketDataPublisher(symbols=[])
        with patch(
            "snapper.messaging.publishers.kraken.native_to_kraken_websocket", return_value="BTC/USD"
        ):
            valid_symbols = publisher._validate_symbols(["BTC-USD", "BTC-USD", "ETH-USD"])
        assert len(valid_symbols) == 2
        assert valid_symbols == ["BTC-USD", "ETH-USD"]

    def test_get_max_symbols_per_connection_returns_20(self) -> None:
        """Verify Kraken WebSocket limit is 20 symbols per connection.

        Given a KrakenMarketDataPublisher instance,
        When _get_max_symbols_per_connection is called,
        Then it returns 20.
        """
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        assert publisher._get_max_symbols_per_connection() == 20

    def test_get_default_parameters_from_settings(self) -> None:
        """Verify default kwargs extracts kraken symbols from settings.

        Given settings with instruments.kraken containing symbols,
        When get_default_parameters is called,
        Then returns dict with those symbols.
        """
        mock_settings = MagicMock(spec=AppSettings)
        mock_settings.instruments = {"kraken": ["BTC-USD", "ETH-USD"], "zonda": ["BTC-PLN"]}
        kwargs = KrakenMarketDataPublisher.get_default_parameters(mock_settings)
        assert kwargs == {"symbols": ["BTC-USD", "ETH-USD"]}

    def test_get_default_parameters_with_empty_kraken_list(self) -> None:
        """Verify default kwargs handles empty kraken instrument list.

        Given settings with instruments.kraken as empty list,
        When get_default_parameters is called,
        Then returns dict with empty symbols list.
        """
        mock_settings = MagicMock(spec=AppSettings)
        mock_settings.instruments = {"kraken": [], "zonda": ["BTC-PLN"]}
        kwargs = KrakenMarketDataPublisher.get_default_parameters(mock_settings)
        assert kwargs == {"symbols": []}

    def test_get_default_parameters_with_missing_kraken_key(self) -> None:
        """Verify default kwargs handles missing kraken key in instruments.

        Given settings without 'kraken' key in instruments dict,
        When get_default_parameters is called,
        Then returns dict with empty symbols list.
        """
        mock_settings = MagicMock(spec=AppSettings)
        mock_settings.instruments = {"zonda": ["BTC-PLN"]}
        kwargs = KrakenMarketDataPublisher.get_default_parameters(mock_settings)
        assert kwargs == {"symbols": []}
