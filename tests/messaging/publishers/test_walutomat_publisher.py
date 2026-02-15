"""Unit tests for WalutomatMarketDataPublisher."""

from unittest.mock import MagicMock
from unittest.mock import patch

from snapper.messaging.publishers.walutomat import WalutomatMarketDataPublisher


class TestWalutomatPublisherUnknownSymbols:
    """Tests for Walutomat publisher unknown symbol handling."""

    def test_validate_symbols_logs_unknown_native_symbol(self) -> None:
        """Verify unknown symbols are logged and filtered out.

        Given a publisher and an invalid symbol,
        When _validate_symbols is called,
        Then symbol is excluded and warning is logged.
        """
        publisher = WalutomatMarketDataPublisher(symbols=[])
        with patch("snapper.messaging.publishers.walutomat.logger") as mock_logger:
            valid_symbols = publisher._validate_symbols(["INVALID-XXX"])
            assert valid_symbols == []
            mock_logger.warning.assert_called_once()
            assert "Skipping unknown native symbol INVALID-XXX" in str(
                mock_logger.warning.call_args
            )

    def test_validate_symbols_filters_duplicates(self) -> None:
        """Verify duplicate symbols are removed.

        Given a publisher and symbols list with duplicates,
        When _validate_symbols is called,
        Then duplicates are removed from result.
        """
        publisher = WalutomatMarketDataPublisher(symbols=[])
        with patch(
            "snapper.messaging.publishers.walutomat.native_to_walutomat_ws",
            return_value="BTCPLN",
        ):
            valid_symbols = publisher._validate_symbols(["BTC-PLN", "BTC-PLN", "BTC-PLN"])
        assert valid_symbols == ["BTC-PLN"]


class TestWalutomatPublisher:
    """Tests for WalutomatMarketDataPublisher functionality."""

    @patch("snapper.config.settings.get_settings")
    def test_initialization(self, mock_get_settings: MagicMock) -> None:
        """Verify publisher initializes with correct default state.

        Given mocked settings with ZMQ endpoint,
        When WalutomatMarketDataPublisher is created,
        Then symbols are stored and running=False, heartbeat=0.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = WalutomatMarketDataPublisher(symbols=["EUR-PLN", "USD-PLN"])
        assert publisher.symbols == ["EUR-PLN", "USD-PLN"]
        assert publisher.running is False
        assert publisher.heartbeat_seq == 0
        assert publisher.repository is None

    @patch("snapper.config.settings.get_settings")
    def test_get_status(self, mock_get_settings: MagicMock) -> None:
        """Verify status returns current publisher state.

        Given a publisher instance,
        When get_status is called,
        Then returns dict with running, symbols, broker_endpoint, heartbeat.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_settings.zmq_heartbeat_interval_ms = 1000
        mock_get_settings.return_value = mock_settings
        publisher = WalutomatMarketDataPublisher(symbols=["EUR-PLN"])
        status = publisher.get_status()
        assert status["running"] is False
        assert status["symbols"] == ["EUR-PLN"]
        assert "broker_endpoint" in status
        assert status["heartbeat_seq"] == 0

    @patch("snapper.config.settings.get_settings")
    def test_get_exchange_name(self, mock_get_settings: MagicMock) -> None:
        """Verify exchange name returns 'walutomat'.

        Given a WalutomatMarketDataPublisher instance,
        When _get_exchange_name is called,
        Then it returns 'walutomat'.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = WalutomatMarketDataPublisher(symbols=[])
        assert publisher._get_exchange_name() == "walutomat"

    @patch("snapper.config.settings.get_settings")
    def test_create_exchange_client(self, mock_get_settings: MagicMock) -> None:
        """Verify factory creates WalutomatExchangeClient.

        Given a WalutomatMarketDataPublisher instance,
        When _create_exchange_client is called,
        Then it returns a WalutomatExchangeClient.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = WalutomatMarketDataPublisher(symbols=[])
        client = publisher._create_exchange_client()
        assert client is not None
        assert client.__class__.__name__ == "WalutomatExchangeClient"

    @patch("snapper.config.settings.get_settings")
    def test_get_default_kwargs(self, mock_get_settings: MagicMock) -> None:
        """Verify default kwargs extracts walutomat symbols from settings.

        Given settings with instruments.walutomat containing symbols,
        When get_default_kwargs is called,
        Then returns dict with those symbols.
        """
        mock_settings = MagicMock()
        mock_settings.instruments = {"walutomat": ["EUR-PLN", "USD-PLN"]}
        mock_get_settings.return_value = mock_settings
        kwargs = WalutomatMarketDataPublisher.get_default_kwargs(mock_settings)
        assert kwargs == {"symbols": ["EUR-PLN", "USD-PLN"]}
