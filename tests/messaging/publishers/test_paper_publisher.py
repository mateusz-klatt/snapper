"""Unit tests for PaperMarketDataPublisher."""

from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate
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
        assert set(publisher.symbols) == {"kraken:BTC-USD", "kraken:ETH-USD"}
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
        assert status["symbols"] == ["kraken:BTC-USD"]
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
        """Verify default kwargs extracts paper symbols from source settings.

        Given settings with paper_instruments mapping,
        When get_default_kwargs is called,
        Then returns dict with flattened symbols and source mapping.
        """
        mock_settings = MagicMock()
        mock_settings.paper_instruments = {"kraken": ["BTC-USD", "ETH-USD"]}
        mock_get_settings.return_value = mock_settings
        kwargs = PaperMarketDataPublisher.get_default_kwargs(mock_settings)
        assert kwargs == {
            "symbols": ["kraken:BTC-USD", "kraken:ETH-USD"],
            "paper_instruments": {"kraken": ["BTC-USD", "ETH-USD"]},
            "start_time": None,
            "end_time": None,
        }

    @patch("snapper.config.settings.get_settings")
    def test_get_default_kwargs_without_paper_symbols(self, mock_get_settings: MagicMock) -> None:
        """Verify default kwargs uses fallback when source map is empty.

        Given settings with empty paper_instruments map,
        When get_default_kwargs is called,
        Then returns default Kraken replay map and flattened symbols.
        """
        mock_settings = MagicMock()
        mock_settings.paper_instruments = {}
        mock_get_settings.return_value = mock_settings
        kwargs = PaperMarketDataPublisher.get_default_kwargs(mock_settings)
        assert kwargs == {
            "symbols": ["kraken:BTC-USD", "kraken:ETH-USD"],
            "paper_instruments": {"kraken": ["BTC-USD", "ETH-USD"]},
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

    @patch("snapper.config.settings.get_settings")
    def test_validate_paper_instruments_normalizes_source_names(
        self, mock_get_settings: MagicMock
    ) -> None:
        """Verify paper instruments map is normalized and filtered.

        Given: Mixed-case source names with duplicates and empty keys,
        When: Publisher is initialized,
        Then: Source exchanges are lowercased and empty entries filtered.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = PaperMarketDataPublisher(
            symbols=[],
            paper_instruments={
                "KRAKEN": ["BTC-USD", "BTC-USD"],
                "": ["ETH-USD"],
                "polygon": [],
            },
        )
        assert publisher.paper_instruments == {"kraken": ["BTC-USD"]}

    def test_build_replay_keys_keeps_exchange_dimension(self) -> None:
        """Verify replay key builder preserves source exchange dimension.

        Given: Source map with duplicate symbol names across exchanges,
        When: Building replay keys,
        Then: Keys remain unique per source exchange.
        """
        replay_keys = PaperMarketDataPublisher._build_replay_keys(
            {
                "kraken": ["BTC-USD", "ETH-USD"],
                "polygon": ["ETH-USD", "AAPL"],
            }
        )
        assert replay_keys == [
            "kraken:BTC-USD",
            "kraken:ETH-USD",
            "polygon:ETH-USD",
            "polygon:AAPL",
        ]

    def test_build_replay_keys_deduplicates_exact_source_symbol_pairs(self) -> None:
        """Verify replay key builder removes exact duplicate source-symbol pairs.

        Given: Source map with duplicate entries in the same source,
        When: Building replay keys,
        Then: Duplicates are removed while preserving first-seen order.
        """
        replay_keys = PaperMarketDataPublisher._build_replay_keys(
            {
                "kraken": ["BTC-USD", "BTC-USD"],
                "polygon": ["BTC-USD"],
            }
        )
        assert replay_keys == ["kraken:BTC-USD", "polygon:BTC-USD"]

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_save_to_db_is_noop(self, mock_get_settings: MagicMock) -> None:
        """Verify paper replay save-to-db hook is a no-op.

        Given: A paper publisher instance,
        When: _save_to_db is called,
        Then: Method completes without touching repository state.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = PaperMarketDataPublisher(symbols=["BTC-USD"])
        before = dict(publisher._last_data_timestamps)
        await publisher._save_to_db("BTC-USD", MagicMock())
        assert publisher._last_data_timestamps == before

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_emit_candle_builds_paper_source_topic(
        self, mock_get_settings: MagicMock
    ) -> None:
        """Verify candle emit uses market.paper.{source_exchange} topic.

        Given: A candle update and source exchange,
        When: _emit_candle is called,
        Then: Publisher sends message on paper topic with source exchange segment.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = PaperMarketDataPublisher(symbols=["BTC-USD"])
        publisher._publish_message = AsyncMock()
        candle = CandleUpdate(
            symbol="BTC-USD",
            open=100.0,
            high=110.0,
            low=90.0,
            close=105.0,
            vwap=102.0,
            trades=12,
            volume=1.23,
            interval_begin=datetime.now(tz=UTC),
            interval=1,
        )
        await publisher._emit_candle("kraken", candle, "1m")
        assert publisher._publish_message.await_args is not None
        topic = publisher._publish_message.await_args.args[0]
        envelope = publisher._publish_message.await_args.args[1]
        assert topic == "market.paper.kraken.BTC-USD.candles.1m"
        assert envelope.exchange == "kraken"
        assert "BTC-USD" in publisher._last_data_timestamps
        assert "kraken:BTC-USD" in publisher._last_data_timestamps

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_emit_tick_and_trade_apply_payload_normalization(
        self, mock_get_settings: MagicMock
    ) -> None:
        """Verify tick/trade emit normalizes bid/ask and side fields.

        Given: Tick with zero bid and trade with unsupported side,
        When: Emitting tick and trade,
        Then: Tick bid is None and trade side is None in envelopes.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = PaperMarketDataPublisher(symbols=["BTC-USD"])
        publisher._publish_message = AsyncMock()
        tick = TickerUpdate(
            symbol="BTC-USD",
            bid=0.0,
            bid_qty=1.0,
            ask=101.0,
            ask_qty=2.0,
            last=100.5,
            volume=10.0,
            vwap=100.1,
            low=90.0,
            high=110.0,
            change=0.0,
            change_pct=0.0,
        )
        trade = TradeUpdate(
            symbol="BTC-USD",
            side="hold",
            quantity=0.5,
            price=100.0,
            ord_type="unknown",
            trade_id=1,
            timestamp=datetime.now(tz=UTC),
        )
        await publisher._emit_tick("kraken", tick)
        tick_topic = publisher._publish_message.await_args_list[0].args[0]
        tick_msg = publisher._publish_message.await_args_list[0].args[1]
        assert tick_topic == "market.paper.kraken.BTC-USD.ticks"
        assert tick_msg.bid is None
        await publisher._emit_trade("kraken", trade)
        trade_topic = publisher._publish_message.await_args_list[1].args[0]
        trade_msg = publisher._publish_message.await_args_list[1].args[1]
        assert trade_topic == "market.paper.kraken.BTC-USD.trades"
        assert trade_msg.side is None

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_loops_return_when_exchange_client_missing(
        self, mock_get_settings: MagicMock
    ) -> None:
        """Verify replay loops return early without exchange client.

        Given: Publisher without initialized exchange client,
        When: Candle/tick/trade loops are started,
        Then: Each loop returns without raising.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = PaperMarketDataPublisher(symbols=["BTC-USD"])
        publisher._exchange_client = None
        await publisher._candle_loop(["BTC-USD"], "1m")
        await publisher._tick_loop(["BTC-USD"])
        await publisher._trade_loop(["BTC-USD"])

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_loops_use_source_exchange_and_stop_when_not_running(
        self, mock_get_settings: MagicMock
    ) -> None:
        """Verify replay loops call source-aware subscriptions and honor running flag.

        Given: Publisher with source map and exchange client generators,
        When: Loops run and running is toggled to False in first emit,
        Then: Each loop exits after first message.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = PaperMarketDataPublisher(
            symbols=[],
            paper_instruments={"kraken": ["BTC-USD"]},
        )

        class _Client:
            def subscribe_candles(
                self, symbols: list[str], timeframe: str = "1m", **kwargs: object
            ) -> AsyncIterator[CandleUpdate]:
                _ = symbols
                _ = timeframe
                _ = kwargs

                async def _gen() -> AsyncIterator[CandleUpdate]:
                    yield CandleUpdate(
                        symbol="BTC-USD",
                        open=1.0,
                        high=1.0,
                        low=1.0,
                        close=1.0,
                        vwap=1.0,
                        trades=1,
                        volume=1.0,
                        interval_begin=datetime.now(tz=UTC),
                        interval=1,
                    )

                return _gen()

            def subscribe_ticks(
                self, symbols: list[str], **kwargs: object
            ) -> AsyncIterator[TickerUpdate]:
                _ = symbols
                _ = kwargs

                async def _gen() -> AsyncIterator[TickerUpdate]:
                    yield TickerUpdate(
                        symbol="BTC-USD",
                        bid=1.0,
                        bid_qty=1.0,
                        ask=2.0,
                        ask_qty=1.0,
                        last=1.5,
                        volume=1.0,
                        vwap=1.5,
                        low=1.0,
                        high=2.0,
                        change=0.0,
                        change_pct=0.0,
                    )

                return _gen()

            def subscribe_trades(
                self, symbols: list[str], **kwargs: object
            ) -> AsyncIterator[TradeUpdate]:
                _ = symbols
                _ = kwargs

                async def _gen() -> AsyncIterator[TradeUpdate]:
                    yield TradeUpdate(
                        symbol="BTC-USD",
                        side="buy",
                        quantity=1.0,
                        price=1.5,
                        ord_type="unknown",
                        trade_id=1,
                        timestamp=datetime.now(tz=UTC),
                    )

                return _gen()

        publisher._exchange_client = _Client()
        publisher.running = True

        async def _stop_on_first_emit(*args: object, **kwargs: object) -> None:
            _ = args
            _ = kwargs
            publisher.running = False

        publisher._emit_candle = AsyncMock(side_effect=_stop_on_first_emit)
        await publisher._candle_loop([], "1m")
        assert publisher._emit_candle.await_count == 1

        publisher.running = True
        publisher._emit_tick = AsyncMock(side_effect=_stop_on_first_emit)
        await publisher._tick_loop([])
        assert publisher._emit_tick.await_count == 1

        publisher.running = True
        publisher._emit_trade = AsyncMock(side_effect=_stop_on_first_emit)
        await publisher._trade_loop([])
        assert publisher._emit_trade.await_count == 1

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_tick_loop_emits_duplicate_symbol_for_multiple_sources(
        self, mock_get_settings: MagicMock
    ) -> None:
        """Verify tick replay emits same symbol from different source exchanges.

        Given: Two source exchanges configured with the same symbol,
        When: Tick loop runs once,
        Then: Emit is called once per source exchange.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = PaperMarketDataPublisher(
            symbols=[],
            paper_instruments={"kraken": ["BTC-USD"], "polygon": ["BTC-USD"]},
        )

        class _Client:
            def subscribe_ticks(
                self, symbols: list[str], **kwargs: object
            ) -> AsyncIterator[TickerUpdate]:
                source_exchange = str(kwargs.get("source_exchange", ""))
                _ = symbols

                async def _gen() -> AsyncIterator[TickerUpdate]:
                    yield TickerUpdate(
                        symbol="BTC-USD",
                        bid=1.0,
                        bid_qty=1.0,
                        ask=2.0,
                        ask_qty=1.0,
                        last=1.5,
                        volume=1.0,
                        vwap=1.5,
                        low=1.0,
                        high=2.0,
                        change=0.0,
                        change_pct=0.0,
                    )
                    _ = source_exchange

                return _gen()

        publisher._exchange_client = _Client()
        publisher.running = True
        publisher._emit_tick = AsyncMock()
        await publisher._tick_loop([])
        assert publisher._emit_tick.await_count == 2
        observed_sources = {
            str(call.args[0]) for call in publisher._emit_tick.await_args_list if call.args
        }
        assert observed_sources == {"kraken", "polygon"}

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_run_source_loops_return_when_exchange_client_missing(
        self, mock_get_settings: MagicMock
    ) -> None:
        """Verify source-specific replay loops return when client is missing.

        Given: Publisher instance without exchange client,
        When: Source-specific replay helpers are called directly,
        Then: All helpers return without raising.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = PaperMarketDataPublisher(symbols=["BTC-USD"])
        publisher._exchange_client = None
        await publisher._run_candle_source_loop("kraken", ["BTC-USD"], "1m")
        await publisher._run_tick_source_loop("kraken", ["BTC-USD"])
        await publisher._run_trade_source_loop("kraken", ["BTC-USD"])

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_loops_break_immediately_when_running_is_false(
        self, mock_get_settings: MagicMock
    ) -> None:
        """Verify replay loops break before emit when running flag is False.

        Given: Publisher with source instruments and generator client while running=False,
        When: Candle/tick/trade loops execute,
        Then: Emit handlers are not called.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = PaperMarketDataPublisher(
            symbols=[],
            paper_instruments={"kraken": ["BTC-USD"]},
        )

        class _Client:
            def subscribe_candles(
                self, symbols: list[str], timeframe: str = "1m", **kwargs: object
            ) -> AsyncIterator[CandleUpdate]:
                _ = symbols
                _ = timeframe
                _ = kwargs

                async def _gen() -> AsyncIterator[CandleUpdate]:
                    yield CandleUpdate(
                        symbol="BTC-USD",
                        open=1.0,
                        high=1.0,
                        low=1.0,
                        close=1.0,
                        vwap=1.0,
                        trades=1,
                        volume=1.0,
                        interval_begin=datetime.now(tz=UTC),
                        interval=1,
                    )

                return _gen()

            def subscribe_ticks(
                self, symbols: list[str], **kwargs: object
            ) -> AsyncIterator[TickerUpdate]:
                _ = symbols
                _ = kwargs

                async def _gen() -> AsyncIterator[TickerUpdate]:
                    yield TickerUpdate(
                        symbol="BTC-USD",
                        bid=1.0,
                        bid_qty=1.0,
                        ask=2.0,
                        ask_qty=1.0,
                        last=1.5,
                        volume=1.0,
                        vwap=1.5,
                        low=1.0,
                        high=2.0,
                        change=0.0,
                        change_pct=0.0,
                    )

                return _gen()

            def subscribe_trades(
                self, symbols: list[str], **kwargs: object
            ) -> AsyncIterator[TradeUpdate]:
                _ = symbols
                _ = kwargs

                async def _gen() -> AsyncIterator[TradeUpdate]:
                    yield TradeUpdate(
                        symbol="BTC-USD",
                        side="buy",
                        quantity=1.0,
                        price=1.5,
                        ord_type="unknown",
                        trade_id=1,
                        timestamp=datetime.now(tz=UTC),
                    )

                return _gen()

        publisher._exchange_client = _Client()
        publisher.running = False
        publisher._emit_candle = AsyncMock()
        publisher._emit_tick = AsyncMock()
        publisher._emit_trade = AsyncMock()
        await publisher._candle_loop([], "1m")
        await publisher._tick_loop([])
        await publisher._trade_loop([])
        assert publisher._emit_candle.await_count == 0
        assert publisher._emit_tick.await_count == 0
        assert publisher._emit_trade.await_count == 0

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_loops_skip_when_no_source_instruments(
        self, mock_get_settings: MagicMock
    ) -> None:
        """Verify replay loops skip client subscriptions for empty source map.

        Given: Publisher initialized with empty paper instrument sources,
        When: Candle/tick/trade loops execute,
        Then: Client subscription methods are not called.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = PaperMarketDataPublisher(symbols=[], paper_instruments={})
        publisher.running = True
        client = MagicMock()
        publisher._exchange_client = client
        await publisher._candle_loop([], "1m")
        await publisher._tick_loop([])
        await publisher._trade_loop([])
        client.subscribe_candles.assert_not_called()
        client.subscribe_ticks.assert_not_called()
        client.subscribe_trades.assert_not_called()

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_loops_handle_client_exceptions(self, mock_get_settings: MagicMock) -> None:
        """Verify replay loops catch client exceptions.

        Given: Publisher with client methods raising RuntimeError,
        When: Candle/tick/trade loops run,
        Then: Exceptions are handled internally without propagating.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = PaperMarketDataPublisher(
            symbols=[],
            paper_instruments={"kraken": ["BTC-USD"]},
        )

        class _BrokenClient:
            def subscribe_candles(
                self, symbols: list[str], timeframe: str = "1m", **kwargs: object
            ) -> AsyncIterator[CandleUpdate]:
                _ = symbols
                _ = timeframe
                _ = kwargs
                raise RuntimeError("candles failed")

            def subscribe_ticks(
                self, symbols: list[str], **kwargs: object
            ) -> AsyncIterator[TickerUpdate]:
                _ = symbols
                _ = kwargs
                raise RuntimeError("ticks failed")

            def subscribe_trades(
                self, symbols: list[str], **kwargs: object
            ) -> AsyncIterator[TradeUpdate]:
                _ = symbols
                _ = kwargs
                raise RuntimeError("trades failed")

        publisher._exchange_client = _BrokenClient()
        publisher.running = True
        await publisher._candle_loop([], "1m")
        await publisher._tick_loop([])
        await publisher._trade_loop([])
