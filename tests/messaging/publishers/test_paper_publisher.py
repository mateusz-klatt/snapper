"""Unit tests for paper trading market data publisher."""

import asyncio
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.messaging.publishers.paper import PaperMarketDataPublisher
from snapper.messaging.publishers.paper import PerSourcePaperPublisher


class TestPerSourcePaperPublisher:
    """Tests for PerSourcePaperPublisher — single source exchange publisher."""

    @patch("snapper.config.settings.get_settings")
    def test_initialization(self, mock_get_settings: MagicMock) -> None:
        """Verify per-source publisher stores source exchange and symbols.

        Given source_exchange and symbols,
        When PerSourcePaperPublisher is created,
        Then attributes are set correctly.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        pub = PerSourcePaperPublisher(
            source_exchange="kraken",
            symbols=["BTC-USD", "ETH-USD"],
        )
        assert pub._source_exchange == "kraken"
        assert pub.symbols == ["BTC-USD", "ETH-USD"]
        assert pub.running is False

    @patch("snapper.config.settings.get_settings")
    def test_get_exchange_name_returns_paper(self, mock_get_settings: MagicMock) -> None:
        """Verify exchange name is always 'paper'.

        Given a per-source publisher for kraken,
        When _get_exchange_name is called,
        Then it returns 'paper'.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        pub = PerSourcePaperPublisher(source_exchange="kraken", symbols=[])
        assert pub._get_exchange_name() == "paper"

    @patch("snapper.config.settings.get_settings")
    def test_get_process_name_includes_source(self, mock_get_settings: MagicMock) -> None:
        """Verify process name includes source exchange.

        Given a per-source publisher for kraken,
        When _get_process_name is called,
        Then it returns 'pub:paper:kraken'.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        pub = PerSourcePaperPublisher(source_exchange="kraken", symbols=[])
        assert pub._get_process_name() == "pub:paper:kraken"

    @patch("snapper.config.settings.get_settings")
    def test_get_heartbeat_component(self, mock_get_settings: MagicMock) -> None:
        """Verify heartbeat component includes source exchange.

        Given a per-source publisher for kraken,
        When _get_heartbeat_component is called,
        Then it returns 'feed.paper.kraken'.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        pub = PerSourcePaperPublisher(source_exchange="kraken", symbols=[])
        assert pub._get_heartbeat_component() == "feed.paper.kraken"

    @patch("snapper.config.settings.get_settings")
    def test_build_data_topic_candles(self, mock_get_settings: MagicMock) -> None:
        """Verify candle topic format includes source exchange.

        Given a per-source publisher for kraken,
        When _build_data_topic is called for candles with timeframe,
        Then topic is 'market.paper.kraken.BTC-USD.candles.1m'.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        pub = PerSourcePaperPublisher(source_exchange="kraken", symbols=[])
        topic = pub._build_data_topic("BTC-USD", "candles", timeframe="1m")
        assert topic == "market.paper.kraken.BTC-USD.candles.1m"

    @patch("snapper.config.settings.get_settings")
    def test_build_data_topic_ticks(self, mock_get_settings: MagicMock) -> None:
        """Verify tick topic format includes source exchange.

        Given a per-source publisher for polygon,
        When _build_data_topic is called for ticks,
        Then topic is 'market.paper.polygon.BTC-USD.ticks'.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        pub = PerSourcePaperPublisher(source_exchange="polygon", symbols=[])
        topic = pub._build_data_topic("BTC-USD", "ticks")
        assert topic == "market.paper.polygon.BTC-USD.ticks"

    @patch("snapper.config.settings.get_settings")
    def test_build_data_topic_trades(self, mock_get_settings: MagicMock) -> None:
        """Verify trade topic format includes source exchange.

        Given a per-source publisher for kraken,
        When _build_data_topic is called for trades,
        Then topic is 'market.paper.kraken.ETH-USD.trades'.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        pub = PerSourcePaperPublisher(source_exchange="kraken", symbols=[])
        topic = pub._build_data_topic("ETH-USD", "trades")
        assert topic == "market.paper.kraken.ETH-USD.trades"

    @patch("snapper.config.settings.get_settings")
    def test_get_data_exchange_returns_source(self, mock_get_settings: MagicMock) -> None:
        """Verify data exchange returns source exchange for envelope payloads.

        Given a per-source publisher for kraken,
        When _get_data_exchange is called,
        Then it returns 'kraken' (not 'paper').
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        pub = PerSourcePaperPublisher(source_exchange="kraken", symbols=[])
        assert pub._get_data_exchange() == "kraken"

    @patch("snapper.config.settings.get_settings")
    def test_validate_symbols_deduplicates(self, mock_get_settings: MagicMock) -> None:
        """Verify symbols are deduplicated preserving order.

        Given duplicate symbols,
        When PerSourcePaperPublisher is created,
        Then duplicates are removed.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        pub = PerSourcePaperPublisher(
            source_exchange="kraken",
            symbols=["BTC-USD", "BTC-USD", "ETH-USD"],
        )
        assert pub.symbols == ["BTC-USD", "ETH-USD"]

    @patch("snapper.config.settings.get_settings")
    def test_create_exchange_client(self, mock_get_settings: MagicMock) -> None:
        """Verify exchange client is created with source_exchange.

        Given a per-source publisher for kraken with repository set,
        When _create_exchange_client is called,
        Then PaperExchangeClient has source_exchange='kraken' and uses base repository.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        pub = PerSourcePaperPublisher(
            source_exchange="kraken",
            symbols=["BTC-USD"],
            start_time=1000.0,
            end_time=2000.0,
        )
        mock_repo = MagicMock()
        pub.repository = mock_repo
        client = pub._create_exchange_client()
        assert client.source_exchange == "kraken"
        assert client.start_time == pytest.approx(1000.0)
        assert client.end_time == pytest.approx(2000.0)
        assert client.repository is mock_repo

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_save_to_db_is_noop(self, mock_get_settings: MagicMock) -> None:
        """Verify save_to_db does nothing for paper replay.

        Given a per-source publisher,
        When _save_to_db is called,
        Then no side effects occur.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        pub = PerSourcePaperPublisher(source_exchange="kraken", symbols=["BTC-USD"])
        before = dict(pub._last_data_timestamps)
        await pub._save_to_db("BTC-USD", MagicMock())
        assert pub._last_data_timestamps == before

    @patch("snapper.config.settings.get_settings")
    def test_get_status(self, mock_get_settings: MagicMock) -> None:
        """Verify get_status returns expected publisher info.

        Given a per-source publisher,
        When get_status is called,
        Then status includes exchange='paper' and symbols.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        pub = PerSourcePaperPublisher(source_exchange="kraken", symbols=["BTC-USD"])
        status = pub.get_status()
        assert status["exchange"] == "paper"
        assert status["symbols"] == ["BTC-USD"]
        assert status["running"] is False


class TestPaperMarketDataPublisher:
    """Tests for PaperMarketDataPublisher — orchestrator wrapper."""

    def test_initialization_none_enters_idle_mode(self) -> None:
        """Verify None paper_instruments enters idle mode.

        Given no paper_instruments argument,
        When PaperMarketDataPublisher is created,
        Then paper_instruments is empty dict (idle).
        """
        pub = PaperMarketDataPublisher()
        assert pub.paper_instruments == {}

    def test_initialization_empty_enters_idle_mode(self) -> None:
        """Verify empty paper_instruments enters idle mode.

        Given empty paper_instruments dict,
        When PaperMarketDataPublisher is created,
        Then paper_instruments is empty dict (idle).
        """
        pub = PaperMarketDataPublisher(paper_instruments={})
        assert pub.paper_instruments == {}

    def test_initialization_with_instruments(self) -> None:
        """Verify custom paper_instruments are stored.

        Given custom paper_instruments mapping,
        When PaperMarketDataPublisher is created,
        Then the mapping is normalized and stored.
        """
        pub = PaperMarketDataPublisher(
            paper_instruments={"kraken": ["BTC-USD"], "polygon": ["AAPL"]},
        )
        assert pub.paper_instruments == {"kraken": ["BTC-USD"], "polygon": ["AAPL"]}

    def test_initialization_with_time_range(self) -> None:
        """Verify start/end time are stored.

        Given start_time and end_time,
        When PaperMarketDataPublisher is created,
        Then time range is stored.
        """
        pub = PaperMarketDataPublisher(
            paper_instruments={"kraken": ["BTC-USD"]},
            start_time=1000.0,
            end_time=2000.0,
        )
        assert pub.start_time == 1000.0
        assert pub.end_time == 2000.0

    def test_get_default_kwargs_with_settings(self) -> None:
        """Verify default kwargs from settings with paper_instruments.

        Given settings with paper_instruments,
        When get_default_kwargs is called,
        Then returns dict with paper_instruments from settings.
        """
        mock_settings = MagicMock()
        mock_settings.paper_instruments = {"kraken": ["BTC-USD", "ETH-USD"]}
        kwargs = PaperMarketDataPublisher.get_default_kwargs(mock_settings)
        assert kwargs == {
            "paper_instruments": {"kraken": ["BTC-USD", "ETH-USD"]},
            "start_time": None,
            "end_time": None,
        }

    def test_get_default_kwargs_empty_settings_returns_idle(self) -> None:
        """Verify default kwargs returns idle config when no paper_instruments.

        Given settings with empty paper_instruments,
        When get_default_kwargs is called,
        Then returns dict with empty paper_instruments (idle mode).
        """
        mock_settings = MagicMock()
        mock_settings.paper_instruments = {}
        kwargs = PaperMarketDataPublisher.get_default_kwargs(mock_settings)
        assert kwargs == {
            "paper_instruments": {},
            "start_time": None,
            "end_time": None,
        }

    def test_validate_paper_instruments_normalizes(self) -> None:
        """Verify exchange names are lowercased and symbols deduplicated.

        Given mixed-case exchanges with duplicate symbols,
        When _validate_paper_instruments is called,
        Then exchanges are lowercased and symbols deduplicated.
        """
        result = PaperMarketDataPublisher._validate_paper_instruments(
            {"KRAKEN": ["BTC-USD", "BTC-USD"], "Polygon": ["AAPL"]}
        )
        assert result == {"kraken": ["BTC-USD"], "polygon": ["AAPL"]}

    def test_validate_paper_instruments_filters_empty_exchange(self) -> None:
        """Verify empty exchange keys are filtered out.

        Given mapping with empty string key alongside valid key,
        When _validate_paper_instruments is called,
        Then empty key is removed, valid key kept.
        """
        result = PaperMarketDataPublisher._validate_paper_instruments(
            {"": ["BTC-USD"], "kraken": ["ETH-USD"]}
        )
        assert result == {"kraken": ["ETH-USD"]}

    def test_validate_paper_instruments_all_empty_returns_idle(self) -> None:
        """Verify validation returns empty when all entries filtered out.

        Given mapping with only empty exchange key,
        When _validate_paper_instruments is called,
        Then returns empty dict (idle mode).
        """
        result = PaperMarketDataPublisher._validate_paper_instruments({"": ["BTC-USD"]})
        assert result == {}

    def test_validate_paper_instruments_filters_empty_symbols(self) -> None:
        """Verify exchanges with empty symbol lists are filtered out.

        Given mapping with one empty and one valid exchange,
        When _validate_paper_instruments is called,
        Then exchange with empty symbols is removed.
        """
        result = PaperMarketDataPublisher._validate_paper_instruments(
            {"kraken": [], "polygon": ["AAPL"]}
        )
        assert result == {"polygon": ["AAPL"]}

    def test_validate_paper_instruments_all_empty_symbols_returns_idle(self) -> None:
        """Verify validation returns empty when all symbol lists empty.

        Given mapping with only empty symbol lists,
        When _validate_paper_instruments is called,
        Then returns empty dict (idle mode).
        """
        result = PaperMarketDataPublisher._validate_paper_instruments({"kraken": []})
        assert result == {}

    @pytest.mark.asyncio
    async def test_start_creates_publishers(self) -> None:
        """Verify start creates per-source publishers for each exchange.

        Given paper_instruments with two exchanges,
        When start is called,
        Then two PerSourcePaperPublisher instances are created.
        """
        pub = PaperMarketDataPublisher(
            paper_instruments={"kraken": ["BTC-USD"], "polygon": ["AAPL"]},
        )
        started: list[str] = []

        async def fake_start(self: Any) -> None:
            started.append(self._source_exchange)

        with patch.object(PerSourcePaperPublisher, "start", fake_start):
            await pub.start()
        assert len(pub._publishers) == 2
        assert set(started) == {"kraken", "polygon"}

    @pytest.mark.asyncio
    async def test_start_idle_mode_no_publishers(self) -> None:
        """Verify start in idle mode creates no publishers.

        Given PaperMarketDataPublisher with no instruments (idle),
        When start is called,
        Then no publishers are created.
        """
        pub = PaperMarketDataPublisher()
        await pub.start()
        assert pub._publishers == []

    @pytest.mark.asyncio
    async def test_start_returns_early_when_instruments_cleared(self) -> None:
        """Verify start returns early when paper_instruments emptied post-init.

        Given paper_instruments cleared after construction,
        When start is called,
        Then no publishers are created.
        """
        pub = PaperMarketDataPublisher(paper_instruments={"kraken": ["BTC-USD"]})
        pub.paper_instruments = {}
        await pub.start()
        assert pub._publishers == []

    @pytest.mark.asyncio
    async def test_start_propagates_publisher_error(self) -> None:
        """Verify start propagates exceptions from per-source publishers.

        Given a per-source publisher that raises on start,
        When start is called,
        Then the error propagates via TaskGroup.
        """
        pub = PaperMarketDataPublisher(
            paper_instruments={"kraken": ["BTC-USD"]},
        )

        async def failing_start(self: Any) -> None:
            raise RuntimeError("connection failed")

        with (
            patch.object(PerSourcePaperPublisher, "start", failing_start),
            pytest.raises(ExceptionGroup) as exc_info,
        ):
            await pub.start()
        assert len(exc_info.value.exceptions) == 1
        assert "connection failed" in str(exc_info.value.exceptions[0])

    @pytest.mark.asyncio
    async def test_stop_stops_all_publishers(self) -> None:
        """Verify stop calls stop on all per-source publishers.

        Given running publishers,
        When stop is called,
        Then each publisher is stopped and list is cleared.
        """
        pub = PaperMarketDataPublisher(
            paper_instruments={"kraken": ["BTC-USD"], "polygon": ["AAPL"]},
        )
        mock_pub1 = MagicMock()
        mock_pub1.stop = AsyncMock()
        mock_pub2 = MagicMock()
        mock_pub2.stop = AsyncMock()
        pub._publishers = [mock_pub1, mock_pub2]
        await pub.stop()
        mock_pub1.stop.assert_awaited_once()
        mock_pub2.stop.assert_awaited_once()
        assert pub._publishers == []

    def test_get_status(self) -> None:
        """Verify get_status returns combined publisher status.

        Given an orchestrator with per-source publishers,
        When get_status is called,
        Then status includes paper_instruments and per-source statuses.
        """
        pub = PaperMarketDataPublisher(
            paper_instruments={"kraken": ["BTC-USD"]},
        )
        mock_inner = MagicMock()
        mock_inner.get_status.return_value = {"running": True, "symbols": ["BTC-USD"]}
        pub._publishers = [mock_inner]
        status = pub.get_status()
        assert status["paper_instruments"] == {"kraken": ["BTC-USD"]}
        assert len(status["publishers"]) == 1
        assert status["publishers"][0]["running"] is True

    @pytest.mark.asyncio
    async def test_start_passes_time_range_to_publishers(self) -> None:
        """Verify start passes start_time/end_time to per-source publishers.

        Given paper publisher with time range,
        When start creates per-source publishers,
        Then each publisher receives the time range.
        """
        pub = PaperMarketDataPublisher(
            paper_instruments={"kraken": ["BTC-USD"]},
            start_time=1000.0,
            end_time=2000.0,
        )

        async def fake_start(self: Any) -> None:
            pass

        with patch.object(PerSourcePaperPublisher, "start", fake_start):
            await pub.start()
        assert len(pub._publishers) == 1
        inner = pub._publishers[0]
        assert inner.start_time == 1000.0
        assert inner.end_time == 2000.0

    @pytest.mark.asyncio
    async def test_start_concurrent_publishers(self) -> None:
        """Verify start launches publishers concurrently via asyncio.gather.

        Given two source exchanges,
        When start is called,
        Then both publishers start concurrently.
        """
        pub = PaperMarketDataPublisher(
            paper_instruments={"kraken": ["BTC-USD"], "polygon": ["AAPL"]},
        )
        start_order: list[str] = []

        async def tracked_start(self: Any) -> None:
            start_order.append(self._source_exchange)
            await asyncio.sleep(0)

        with patch.object(PerSourcePaperPublisher, "start", tracked_start):
            await pub.start()
        assert len(start_order) == 2
        assert set(start_order) == {"kraken", "polygon"}
