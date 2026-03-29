"""Unit tests for ZondaMarketDataPublisher."""

from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from types import SimpleNamespace
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate
from snapper.messaging.publishers.zonda import ZondaMarketDataPublisher
from snapper.messaging.schemas.data import CandleData
from snapper.messaging.schemas.messages import MarketDataMessage


class PublisherSocketStub:
    """Test stub for publisher socket."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.calls: list[tuple[str, bytes]] = []

    async def send_multipart(self, topic: str, payload: bytes) -> None:
        """Send multipart message."""
        self.calls.append((topic, payload))


class TestZondaPublisher:
    """Tests for ZondaMarketDataPublisher functionality."""

    @patch("snapper.config.settings.get_settings")
    def test_initialization(self, mock_get_settings: MagicMock) -> None:
        """Verify publisher initializes with correct default state.

        Given mocked settings with ZMQ endpoint,
        When ZondaMarketDataPublisher is created with empty symbols,
        Then running=False, heartbeat=0, repository=None.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = ZondaMarketDataPublisher(symbols=[])
        assert publisher.symbols == []
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
        publisher = ZondaMarketDataPublisher(symbols=[])
        status = publisher.get_status()
        assert status["running"] is False
        assert status["symbols"] == []
        assert "broker_endpoint" in status
        assert status["heartbeat_seq"] == 0

    @patch("snapper.config.settings.get_settings")
    def test_get_exchange_name(self, mock_get_settings: MagicMock) -> None:
        """Verify exchange name returns 'zonda'.

        Given a ZondaMarketDataPublisher instance,
        When _get_exchange_name is called,
        Then it returns 'zonda'.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = ZondaMarketDataPublisher(symbols=[])
        assert publisher._get_exchange_name() == "zonda"

    @patch("snapper.config.settings.get_settings")
    def test_create_exchange_client(self, mock_get_settings: MagicMock) -> None:
        """Verify factory creates ZondaExchangeClient.

        Given a ZondaMarketDataPublisher instance,
        When _create_exchange_client is called,
        Then it returns a ZondaExchangeClient.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = ZondaMarketDataPublisher(symbols=[])
        client = publisher._create_exchange_client()
        assert client is not None
        assert client.__class__.__name__ == "ZondaExchangeClient"

    @patch("snapper.config.settings.get_settings")
    def test_get_default_parameters(self, mock_get_settings: MagicMock) -> None:
        """Verify default kwargs extracts zonda symbols from settings.

        Given settings with instruments.zonda containing symbols,
        When get_default_parameters is called,
        Then returns dict with those symbols.
        """
        mock_settings = MagicMock()
        mock_settings.instruments = {"zonda": ["BTC-PLN", "ETH-PLN"]}
        mock_get_settings.return_value = mock_settings
        kwargs = ZondaMarketDataPublisher.get_default_parameters(mock_settings)
        assert kwargs == {"symbols": ["BTC-PLN", "ETH-PLN"]}

    @patch("snapper.messaging.publishers.zonda.logger")
    @patch("snapper.messaging.publishers.zonda.native_to_zonda_ws")
    @patch("snapper.config.settings.get_settings")
    def test_validate_symbols_filters_invalid_and_duplicates(
        self,
        mock_get_settings: MagicMock,
        mock_native_to_zonda: MagicMock,
        mock_logger: MagicMock,
    ) -> None:
        """Verify invalid symbols are filtered with warning logs.

        Given a publisher and symbols including invalid one,
        When publisher is created,
        Then invalid symbols are excluded and warning is logged.
        """

        def native_side_effect(symbol: str) -> str:
            if symbol == "INVALID":
                raise ValueError("unknown symbol")
            return symbol

        mock_native_to_zonda.side_effect = native_side_effect
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = ZondaMarketDataPublisher(symbols=["BTC-PLN", "INVALID", "BTC-PLN"])
        assert publisher.symbols == ["BTC-PLN"]
        mock_native_to_zonda.assert_called()
        mock_logger.warning.assert_called_once_with(
            "ZondaMarketDataPublisher: Skipping unknown native symbol INVALID"
        )


@pytest.mark.asyncio
class TestZondaPublisherLoops:
    """Tests for ZondaMarketDataPublisher loop functionality."""

    @patch("snapper.config.settings.get_settings")
    @patch("snapper.messaging.publishers.zonda.ZondaMarketDataPublisher._validate_symbols")
    async def test_candle_loop_processes_candles(
        self,
        mock_validate: MagicMock,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify candle loop publishes OHLCV bars correctly.

        Given a running publisher with candle stream,
        When _candle_loop processes candles,
        Then messages are published with correct topic and candle data.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = ZondaMarketDataPublisher(symbols=["BTC-PLN"])
        publisher.running = True
        publisher_any = cast(Any, publisher)
        btc_candle = CandleUpdate(
            symbol="BTC-PLN",
            open=100000.0,
            high=102000.0,
            low=99000.0,
            close=101000.0,
            vwap=100500.0,
            trades=50,
            volume=5.5,
            interval_begin=datetime.now(UTC),
            interval=1,
        )

        async def mock_subscribe(symbols: list[str], timeframe: str) -> AsyncIterator[CandleUpdate]:
            yield btc_candle

        mock_exchange_client = SimpleNamespace(subscribe_candles=mock_subscribe)
        publisher_any._exchange_client = mock_exchange_client
        published_messages: list[tuple[str, CandleData]] = []

        async def publish_stub(topic: str, message: CandleData) -> None:
            published_messages.append((topic, message))

        publisher_any._publish_message = publish_stub
        publisher_any._ensure_instrument = AsyncMock(return_value="inst-pub-1")
        publisher_any.repository = SimpleNamespace(upsert_candles=AsyncMock())
        await publisher_any._candle_loop(["BTC-PLN"], "1m")
        assert len(published_messages) == 1
        topic, msg = published_messages[0]
        assert topic == "market.zonda.BTC-PLN.candles.1m"
        assert msg.type == "candle"
        assert msg.instrument == "BTC-PLN"
        assert msg.close == pytest.approx(101000.0)

    @patch("snapper.config.settings.get_settings")
    @patch("snapper.messaging.publishers.zonda.ZondaMarketDataPublisher._validate_symbols")
    async def test_tick_loop_processes_ticks(
        self,
        mock_validate: MagicMock,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify tick loop publishes ticker updates correctly.

        Given a running publisher with tick stream,
        When _tick_loop processes ticks,
        Then messages are published with correct topic and tick data.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = ZondaMarketDataPublisher(symbols=["ETH-PLN"])
        publisher.running = True
        publisher_any = cast(Any, publisher)
        tick_update = TickerUpdate(
            symbol="ETH-PLN",
            bid=7000.0,
            bid_qty=2.0,
            ask=7010.0,
            ask_qty=1.5,
            last=7005.0,
            volume=100.0,
            vwap=7002.0,
            low=6900.0,
            high=7100.0,
            change=50.0,
            change_pct=0.7,
        )

        async def mock_subscribe(symbols: list[str]) -> AsyncIterator[TickerUpdate]:
            yield tick_update

        mock_exchange_client = SimpleNamespace(subscribe_ticks=mock_subscribe)
        publisher_any._exchange_client = mock_exchange_client
        published_messages: list[tuple[str, MarketDataMessage]] = []

        async def publish_stub(topic: str, message: MarketDataMessage) -> None:
            published_messages.append((topic, message))

        publisher_any._publish_message = publish_stub
        await publisher_any._tick_loop(["ETH-PLN"])
        assert len(published_messages) == 1
        topic, msg = published_messages[0]
        assert topic == "market.zonda.ETH-PLN.ticks"
        assert msg.type == "tick"

    @patch("snapper.config.settings.get_settings")
    @patch("snapper.messaging.publishers.zonda.ZondaMarketDataPublisher._validate_symbols")
    async def test_trade_loop_processes_trades(
        self,
        mock_validate: MagicMock,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify trade loop publishes trade updates correctly.

        Given a running publisher with trade stream,
        When _trade_loop processes trades,
        Then messages are published with correct topic and trade data.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = ZondaMarketDataPublisher(symbols=["BTC-PLN"])
        publisher.running = True
        publisher_any = cast(Any, publisher)
        trade1 = TradeUpdate(
            symbol="BTC-PLN",
            side="buy",
            quantity=0.5,
            price=101000.0,
            ord_type="market",
            trade_id="123",
            timestamp=datetime.now(UTC),
        )

        async def mock_subscribe(symbols: list[str]) -> AsyncIterator[TradeUpdate]:
            yield trade1

        mock_exchange_client = SimpleNamespace(subscribe_trades=mock_subscribe)
        publisher_any._exchange_client = mock_exchange_client
        published_messages: list[tuple[str, MarketDataMessage]] = []

        async def publish_stub(topic: str, message: MarketDataMessage) -> None:
            published_messages.append((topic, message))

        publisher_any._publish_message = publish_stub
        await publisher_any._trade_loop(["BTC-PLN"])
        assert len(published_messages) == 1
        topic, msg = published_messages[0]
        assert topic == "market.zonda.BTC-PLN.trades"
        assert msg.type == "trade"
