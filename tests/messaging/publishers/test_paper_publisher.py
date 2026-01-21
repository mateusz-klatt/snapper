"""Unit tests for PaperMarketDataPublisher."""

from unittest.mock import MagicMock
from unittest.mock import patch

from snapper.messaging.publishers.paper import PaperMarketDataPublisher


class TestPaperPublisher:
    """Tests for PaperMarketDataPublisher functionality."""

    @patch("snapper.config.settings.get_settings")
    def test_initialization(self, mock_get_settings: MagicMock) -> None:
        """Verify publisher initializes with correct default state.

        Given mocked settings with ZMQ endpoint,
        When PaperMarketDataPublisher is created,
        Then symbols are stored and running=False, heartbeat=0.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = PaperMarketDataPublisher(symbols=["BTC-USD", "ETH-USD"])
        assert set(publisher.symbols) == {"BTC-USD", "ETH-USD"}
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
        publisher = PaperMarketDataPublisher(symbols=["BTC-USD"])
        status = publisher.get_status()
        assert status["running"] is False
        assert status["symbols"] == ["BTC-USD"]
        assert "broker_endpoint" in status
        assert status["heartbeat_seq"] == 0

    @patch("snapper.config.settings.get_settings")
    def test_get_exchange_name(self, mock_get_settings: MagicMock) -> None:
        """Verify exchange name returns 'paper'.

        Given a PaperMarketDataPublisher instance,
        When _get_exchange_name is called,
        Then it returns 'paper'.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = PaperMarketDataPublisher(symbols=[])
        assert publisher._get_exchange_name() == "paper"

    @patch("snapper.config.settings.get_settings")
    def test_create_exchange_client(self, mock_get_settings: MagicMock) -> None:
        """Verify factory creates PaperExchangeClient.

        Given a PaperMarketDataPublisher instance,
        When _create_exchange_client is called,
        Then it returns a PaperExchangeClient.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = PaperMarketDataPublisher(symbols=[])
        client = publisher._create_exchange_client()
        assert client is not None
        assert client.__class__.__name__ == "PaperExchangeClient"

    @patch("snapper.config.settings.get_settings")
    def test_get_default_kwargs_with_paper_symbols(self, mock_get_settings: MagicMock) -> None:
        """Verify default kwargs extracts paper symbols from settings.

        Given settings with instruments.paper containing symbols,
        When get_default_kwargs is called,
        Then returns dict with those symbols and None time bounds.
        """
        mock_settings = MagicMock()
        mock_settings.instruments = {"paper": ["BTC-USD", "ETH-USD"]}
        mock_get_settings.return_value = mock_settings
        kwargs = PaperMarketDataPublisher.get_default_kwargs(mock_settings)
        assert kwargs == {
            "symbols": ["BTC-USD", "ETH-USD"],
            "start_time": None,
            "end_time": None,
        }

    @patch("snapper.config.settings.get_settings")
    def test_get_default_kwargs_without_paper_symbols(self, mock_get_settings: MagicMock) -> None:
        """Verify default kwargs uses fallback when no paper instruments.

        Given settings without 'paper' key in instruments,
        When get_default_kwargs is called,
        Then returns default symbols BTC-USD, ETH-USD.
        """
        mock_settings = MagicMock()
        mock_settings.instruments = {}
        mock_get_settings.return_value = mock_settings
        kwargs = PaperMarketDataPublisher.get_default_kwargs(mock_settings)
        assert kwargs == {
            "symbols": ["BTC-USD", "ETH-USD"],
            "start_time": None,
            "end_time": None,
        }

    @patch("snapper.config.settings.get_settings")
    def test_validate_symbols_accepts_all(self, mock_get_settings: MagicMock) -> None:
        """Verify paper publisher accepts any symbol format.

        Given a publisher and arbitrary symbol strings,
        When _validate_symbols is called,
        Then all symbols are accepted (no exchange validation).
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = PaperMarketDataPublisher(symbols=[])
        valid = publisher._validate_symbols(["BTC-USD", "CUSTOM-PAIR", "TEST-SYM"])
        assert "BTC-USD" in valid
        assert "CUSTOM-PAIR" in valid
        assert "TEST-SYM" in valid

    @patch("snapper.config.settings.get_settings")
    def test_validate_symbols_removes_duplicates(self, mock_get_settings: MagicMock) -> None:
        """Verify duplicate symbols are removed.

        Given a publisher and symbols list with duplicates,
        When _validate_symbols is called,
        Then duplicates are removed.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = PaperMarketDataPublisher(symbols=[])
        valid = publisher._validate_symbols(["BTC-USD", "BTC-USD", "ETH-USD"])
        assert len(valid) == 2
        assert "BTC-USD" in valid
        assert "ETH-USD" in valid
