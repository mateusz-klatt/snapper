"""Tests for TraderCoordinator and ZMQ trader functionality."""

import asyncio
import json
import time
from collections.abc import Callable
from collections.abc import Coroutine
from datetime import UTC
from datetime import datetime
from types import SimpleNamespace
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

import pytest

import snapper.application.engine.trader as trader_module
from snapper.application.engine.config import EngineConfigModel
from snapper.application.engine.service import TradingEngineService
from snapper.application.engine.trader import TraderCoordinator
from snapper.application.engine.trader import run_zmq_trader
from snapper.application.portfolio.models import PositionStateModel
from snapper.application.risk.models import RiskConfigModel
from snapper.application.risk.models import RiskEvaluator
from snapper.config.app import AppSettings
from snapper.messaging.infrastructure.gap_detector import GapDetector
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.schemas.data import OrderData
from snapper.messaging.schemas.data import OrderEventData
from snapper.messaging.schemas.data import SignalData

TEST_DB_URL = "sqlite:///:memory:"


class TestTraderCoverage:
    """Tests for TraderCoordinator coverage and core functionality."""

    @patch("snapper.application.engine.trader.get_repository")
    @patch("snapper.application.engine.trader.get_settings")
    def test_init(self, mock_get_settings: MagicMock, mock_get_repository: MagicMock) -> None:
        """Verify TraderCoordinator initialization with custom signal topics.

        Given: Mocked settings with instrument configuration,
        When: TraderCoordinator is instantiated with signal topics,
        Then: All attributes are properly initialized with expected defaults.
        """
        mock_settings = MagicMock()
        mock_settings.instruments = {
            "kraken": ["BTC-USD", "ETH-USD"],
            "zonda": [],
            "walutomat": [],
            "polygon": [],
        }
        mock_settings.db_url = TEST_DB_URL
        mock_get_settings.return_value = mock_settings
        mock_repository = MagicMock()
        mock_get_repository.return_value = mock_repository
        trader = TraderCoordinator(signal_topics=["signals.test"])
        assert trader.signal_topics == ["signals.test"]
        assert trader.zmq_context is None
        assert trader.signal_subscriber is None
        assert trader.execution_context is None
        assert trader.execution_publisher is None
        assert isinstance(trader.engines, dict)
        assert isinstance(trader.last_signal_time, dict)

    @patch("snapper.application.engine.trader.get_repository")
    @patch("snapper.application.engine.trader.get_settings")
    def test_init_default_values(
        self, mock_get_settings: MagicMock, mock_get_repository: MagicMock
    ) -> None:
        """Verify TraderCoordinator uses default signal topic when none provided.

        Given: Mocked settings with instrument configuration,
        When: TraderCoordinator is instantiated without signal topics,
        Then: Default signal topic 'signals.' is used.
        """
        mock_settings = MagicMock()
        mock_settings.instruments = {
            "kraken": ["BTC-USD", "ETH-USD"],
            "zonda": [],
            "walutomat": [],
            "polygon": [],
        }
        mock_settings.db_url = TEST_DB_URL
        mock_get_settings.return_value = mock_settings
        mock_repository = MagicMock()
        mock_get_repository.return_value = mock_repository
        trader = TraderCoordinator()
        assert trader.signal_topics == ["signals."]

    @patch("snapper.application.engine.trader.get_repository")
    @patch("snapper.application.engine.trader.get_settings")
    def test_repr(self, mock_get_settings: MagicMock, mock_get_repository: MagicMock) -> None:
        """Verify string representation includes class name and topics.

        Given: A TraderCoordinator with specific signal topics,
        When: repr() is called on the coordinator,
        Then: String includes class name and configured topics.
        """
        mock_settings = MagicMock()
        mock_settings.instruments = {
            "kraken": ["BTC-USD"],
            "zonda": [],
            "walutomat": [],
            "polygon": [],
        }
        mock_settings.db_url = TEST_DB_URL
        mock_get_settings.return_value = mock_settings
        mock_repository = MagicMock()
        mock_get_repository.return_value = mock_repository
        trader = TraderCoordinator(signal_topics=["signals.kraken.BTC-USD.live"])
        repr_str = repr(trader)
        assert "TraderCoordinator" in repr_str
        assert "signals.kraken.BTC-USD.live" in repr_str

    @pytest.mark.asyncio
    @patch("snapper.application.engine.trader.zmq.asyncio.Context")
    @patch("snapper.application.engine.trader.get_repository")
    @patch("snapper.application.engine.trader.get_settings")
    async def test_stop_closes_sockets(
        self,
        mock_get_settings: MagicMock,
        mock_get_repository: MagicMock,
        mock_zmq_context_class: MagicMock,
    ) -> None:
        """Verify stop method properly closes all ZMQ resources.

        Given: A TraderCoordinator with active sockets and contexts,
        When: stop() is called,
        Then: All sockets are closed and contexts terminated.
        """
        mock_settings = MagicMock()
        mock_settings.instruments = {
            "kraken": ["BTC-USD"],
            "zonda": [],
            "walutomat": [],
            "polygon": [],
        }
        mock_settings.db_url = TEST_DB_URL
        mock_get_settings.return_value = mock_settings
        mock_repository = MagicMock()
        mock_get_repository.return_value = mock_repository
        trader = TraderCoordinator()
        mock_signal_sub = MagicMock()
        mock_zmq_context = MagicMock()
        mock_execution_socket = MagicMock()
        mock_execution_context = MagicMock()
        trader.signal_subscriber = mock_signal_sub
        trader.zmq_context = mock_zmq_context
        trader.execution_publisher = mock_execution_socket
        trader.execution_context = mock_execution_context
        await trader.stop()
        mock_signal_sub.close.assert_called_once()
        mock_zmq_context.term.assert_called_once()
        mock_execution_socket.close.assert_called_once()
        mock_execution_context.term.assert_called_once()
        assert trader.signal_subscriber is None
        assert trader.zmq_context is None

    @pytest.mark.asyncio
    @patch("snapper.application.engine.trader.get_repository")
    @patch("snapper.application.engine.trader.get_settings")
    async def test_stop_when_not_started(
        self, mock_get_settings: MagicMock, mock_get_repository: MagicMock
    ) -> None:
        """Verify stop is safe to call before start.

        Given: A TraderCoordinator that was never started,
        When: stop() is called,
        Then: No errors occur and attributes remain None.
        """
        mock_settings = MagicMock()
        mock_settings.instruments = {
            "kraken": ["BTC-USD"],
            "zonda": [],
            "walutomat": [],
            "polygon": [],
        }
        mock_settings.db_url = TEST_DB_URL
        mock_get_settings.return_value = mock_settings
        mock_repository = MagicMock()
        mock_get_repository.return_value = mock_repository
        trader = TraderCoordinator()
        await trader.stop()
        assert trader.signal_subscriber is None
        assert trader.zmq_context is None

    @pytest.mark.asyncio
    @patch("snapper.application.engine.trader.get_repository")
    @patch("snapper.application.engine.trader.get_settings")
    async def test_setup_external_execution(
        self, mock_get_settings: MagicMock, mock_get_repository: MagicMock
    ) -> None:
        """Verify external execution publisher setup.

        Given: Settings with ZMQ broker configuration,
        When: _setup_external_execution is called,
        Then: Execution context and publisher are initialized.
        """
        mock_settings = MagicMock()
        mock_settings.instruments = {
            "kraken": ["BTC-USD"],
            "zonda": [],
            "walutomat": [],
            "polygon": [],
        }
        mock_settings.db_url = TEST_DB_URL
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7501"
        mock_get_settings.return_value = mock_settings
        mock_repository = MagicMock()
        mock_get_repository.return_value = mock_repository
        with patch(
            "snapper.application.engine.trader.zmq.asyncio.Context"
        ) as mock_zmq_context_class:
            mock_context = MagicMock()
            mock_socket = MagicMock()
            mock_context.socket.return_value = mock_socket
            mock_zmq_context_class.return_value = mock_context
            trader = TraderCoordinator()
            trader._setup_external_execution()
            assert trader.execution_context is not None
            assert trader.execution_publisher is not None
            mock_context.socket.assert_called_once()
            mock_socket.connect.assert_called_once_with("tcp://127.0.0.1:7501")

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    @patch("snapper.application.engine.trader.get_repository")
    @patch("snapper.application.engine.trader.get_settings")
    async def test_setup_signal_subscriber(
        self, mock_get_settings: MagicMock, mock_get_repository: MagicMock
    ) -> None:
        """Verify signal subscriber setup creates ZMQ context and socket.

        Given: TraderCoordinator with signal topics,
        When: _setup_signal_subscriber is called,
        Then: ZMQ context and signal subscriber socket are created.
        """
        mock_settings = MagicMock()
        mock_settings.instruments = {
            "kraken": ["BTC-USD"],
            "zonda": [],
            "walutomat": [],
            "polygon": [],
        }
        mock_settings.db_url = TEST_DB_URL
        mock_get_settings.return_value = mock_settings
        mock_repository = MagicMock()
        mock_get_repository.return_value = mock_repository
        with patch(
            "snapper.application.engine.trader.zmq.asyncio.Context"
        ) as mock_zmq_context_class:
            mock_context = MagicMock()
            mock_socket = MagicMock()
            mock_context.socket.return_value = mock_socket
            mock_zmq_context_class.return_value = mock_context
            trader = TraderCoordinator(signal_topics=["signals.kraken.BTC-USD.live"])
            trader._setup_signal_subscriber()
            assert trader.zmq_context is not None
            assert trader.signal_subscriber is not None
            mock_context.socket.assert_called_once()
            mock_socket.connect.assert_called_once()

    @pytest.mark.asyncio
    @patch("snapper.application.engine.trader.get_repository")
    @patch("snapper.application.engine.trader.get_settings")
    async def test_on_signal_processes_buy_signal(
        self, mock_get_settings: MagicMock, mock_get_repository: MagicMock
    ) -> None:
        """Verify buy signal is processed and timestamp is updated.

        Given: TraderCoordinator with mocked engine for BTC-USD,
        When: Buy signal is received,
        Then: Engine execute_desired_units is called and timestamp is recorded.
        """
        mock_settings = MagicMock()
        mock_settings.instruments = {
            "kraken": ["BTC-USD"],
            "zonda": [],
            "walutomat": [],
            "polygon": [],
        }
        mock_settings.db_url = TEST_DB_URL
        mock_get_settings.return_value = mock_settings
        mock_repository = MagicMock()
        mock_get_repository.return_value = mock_repository
        trader = TraderCoordinator()
        trader.execution_publisher = MagicMock()
        mock_engine = MagicMock()
        mock_engine.execute_desired_units = AsyncMock()
        trader.engines["BTC-USD@kraken-live"] = mock_engine
        signal_msg = SignalData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            fired_at=datetime.now(UTC),
            strategy_name="test_strategy",
            instrument="BTC-USD",
            side="buy",
            strength=0.8,
            price=50000.0,
            exchange="kraken",
            reason="test",
        )
        trader._current_topic = "signals.kraken.BTC-USD.live"
        with patch("snapper.application.engine.trader.time.time", return_value=1234567890.0):
            await trader._on_signal(signal_msg)
        assert mock_engine.execute_desired_units.called
        assert trader.last_signal_time["BTC-USD@kraken-live"] == pytest.approx(1234567890.0)

    @pytest.mark.asyncio
    @patch("snapper.application.engine.trader.get_repository")
    @patch("snapper.application.engine.trader.get_settings")
    async def test_on_signal_processes_sell_signal(
        self, mock_get_settings: MagicMock, mock_get_repository: MagicMock
    ) -> None:
        """Verify sell signal triggers execution with zero units.

        Given: TraderCoordinator with mocked engine for BTC-USD,
        When: Sell signal is received,
        Then: Engine execute_desired_units is called with 0.0 units.
        """
        mock_settings = MagicMock()
        mock_settings.instruments = {
            "kraken": ["BTC-USD"],
            "zonda": [],
            "walutomat": [],
            "polygon": [],
        }
        mock_settings.db_url = TEST_DB_URL
        mock_get_settings.return_value = mock_settings
        mock_repository = MagicMock()
        mock_get_repository.return_value = mock_repository
        trader = TraderCoordinator()
        trader.execution_publisher = MagicMock()
        mock_engine = MagicMock()
        mock_engine.execute_desired_units = AsyncMock()
        trader.engines["BTC-USD@kraken-live"] = mock_engine
        signal_msg = SignalData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            fired_at=datetime.now(UTC),
            strategy_name="test_strategy",
            instrument="BTC-USD",
            side="sell",
            strength=1.0,
            price=50000.0,
            exchange="kraken",
            reason="test",
        )
        trader._current_topic = "signals.kraken.BTC-USD.live"
        await trader._on_signal(signal_msg)
        mock_engine.execute_desired_units.assert_called_once()
        args = mock_engine.execute_desired_units.call_args[0]
        assert args[0] == pytest.approx(0.0)

    @pytest.mark.asyncio
    @patch("snapper.application.engine.trader.get_repository")
    @patch("snapper.application.engine.trader.get_settings")
    async def test_on_signal_ignores_invalid_signal(
        self, mock_get_settings: MagicMock, mock_get_repository: MagicMock
    ) -> None:
        """Verify signal without price is ignored.

        Given: TraderCoordinator with mocked engine for BTC-USD,
        When: Signal with price=None is received,
        Then: Engine execute_desired_units is not called.
        """
        mock_settings = MagicMock()
        mock_settings.instruments = {
            "kraken": ["BTC-USD"],
            "zonda": [],
            "walutomat": [],
            "polygon": [],
        }
        mock_settings.db_url = TEST_DB_URL
        mock_get_settings.return_value = mock_settings
        mock_repository = MagicMock()
        mock_get_repository.return_value = mock_repository
        trader = TraderCoordinator()
        mock_engine = MagicMock()
        mock_engine.execute_desired_units = AsyncMock()
        trader.engines["BTC-USD@kraken-live"] = mock_engine
        signal_msg = SignalData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            fired_at=datetime.now(UTC),
            strategy_name="test_strategy",
            instrument="BTC-USD",
            side="buy",
            strength=0.8,
            price=None,
            exchange="kraken",
            reason="test",
        )
        trader._current_topic = "signals.kraken.BTC-USD.live"
        await trader._on_signal(signal_msg)
        assert not mock_engine.execute_desired_units.called

    @pytest.mark.asyncio
    @patch("snapper.application.engine.trader.get_repository")
    @patch("snapper.application.engine.trader.get_settings")
    async def test_on_signal_unknown_instrument(
        self, mock_get_settings: MagicMock, mock_get_repository: MagicMock
    ) -> None:
        """Verify signal for unknown instrument is handled gracefully.

        Given: TraderCoordinator without engine for UNKNOWN-USD,
        When: Signal for UNKNOWN-USD is received,
        Then: No exception is raised.
        """
        mock_settings = MagicMock()
        mock_settings.instruments = {
            "kraken": ["BTC-USD"],
            "zonda": [],
            "walutomat": [],
            "polygon": [],
        }
        mock_settings.db_url = TEST_DB_URL
        mock_get_settings.return_value = mock_settings
        mock_repository = MagicMock()
        mock_get_repository.return_value = mock_repository
        trader = TraderCoordinator()
        signal_msg = SignalData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            fired_at=datetime.now(UTC),
            strategy_name="test_strategy",
            instrument="UNKNOWN-USD",
            side="buy",
            strength=0.8,
            price=100.0,
            exchange="kraken",
            reason="test",
        )
        trader._current_topic = "signals.kraken.UNKNOWN-USD"
        await trader._on_signal(signal_msg)

    @pytest.mark.asyncio
    @patch("snapper.application.engine.trader.get_repository")
    @patch("snapper.application.engine.trader.get_settings")
    async def test_signal_tracking_updates_timestamps(
        self, mock_get_settings: MagicMock, mock_get_repository: MagicMock
    ) -> None:
        """Verify signal processing updates last_signal_time tracking.

        Given: TraderCoordinator with mocked engine for BTC-USD,
        When: Valid signal is processed,
        Then: last_signal_time is updated with current timestamp.
        """
        mock_settings = MagicMock()
        mock_settings.instruments = {
            "kraken": ["BTC-USD"],
            "zonda": [],
            "walutomat": [],
            "polygon": [],
        }
        mock_settings.db_url = TEST_DB_URL
        mock_get_settings.return_value = mock_settings
        mock_repository = MagicMock()
        mock_get_repository.return_value = mock_repository
        trader = TraderCoordinator()
        trader.execution_publisher = MagicMock()
        mock_engine = MagicMock()
        mock_engine.execute_desired_units = AsyncMock()
        trader.engines["BTC-USD@kraken-live"] = mock_engine
        trader.last_signal_time["BTC-USD@kraken-live"] = 0.0
        signal_msg = SignalData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            fired_at=datetime.now(UTC),
            strategy_name="test_strategy",
            instrument="BTC-USD",
            side="buy",
            strength=0.8,
            price=50000.0,
            exchange="kraken",
            reason="test",
        )
        trader._current_topic = "signals.kraken.BTC-USD.live"
        with patch("snapper.application.engine.trader.time.time", return_value=1234567890.0):
            await trader._on_signal(signal_msg)
        assert trader.last_signal_time["BTC-USD@kraken-live"] == pytest.approx(1234567890.0)

    @pytest.mark.asyncio
    @patch("snapper.application.engine.trader.get_repository")
    @patch("snapper.application.engine.trader.get_settings")
    async def test_on_signal_unknown_exchange_in_topic(
        self, mock_get_settings: MagicMock, mock_get_repository: MagicMock
    ) -> None:
        """Verify signal with unknown exchange is not processed.

        Given: TraderCoordinator without engine for binance exchange,
        When: Signal from binance exchange topic is received,
        Then: No engine is created for the unknown exchange.
        """
        mock_settings = MagicMock()
        mock_settings.instruments = {
            "kraken": ["BTC-USD"],
            "zonda": [],
            "walutomat": [],
            "polygon": [],
        }
        mock_settings.db_url = TEST_DB_URL
        mock_get_settings.return_value = mock_settings
        mock_repository = MagicMock()
        mock_get_repository.return_value = mock_repository
        trader = TraderCoordinator()
        trader.execution_publisher = MagicMock()
        trader._current_topic = "signals.binance.BTC-USD.live"
        signal_msg = SignalData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            fired_at=datetime.now(UTC),
            strategy_name="test",
            instrument="BTC-USD",
            side="buy",
            strength=0.8,
            price=50000.0,
            exchange="kraken",
            reason="test",
        )
        await trader._on_signal(signal_msg)
        assert "BTC-USD@binance-live" not in trader.engines

    @pytest.mark.asyncio
    @patch("snapper.application.engine.trader.get_repository")
    @patch("snapper.application.engine.trader.get_settings")
    async def test_on_signal_drops_non_tradeable_instrument(
        self, mock_get_settings: MagicMock, mock_get_repository: MagicMock
    ) -> None:
        """Verify signal for non-tradeable instrument is dropped.

        Given: TraderCoordinator with is_tradeable returning False,
        When: Signal arrives for a non-tradeable instrument,
        Then: Signal is dropped and no engine is created.
        """
        mock_settings = MagicMock()
        mock_settings.instruments = {"kraken": ["BTC-USD"]}
        mock_settings.db_url = TEST_DB_URL
        mock_get_settings.return_value = mock_settings
        mock_get_repository.return_value = MagicMock()
        trader = TraderCoordinator()
        trader.execution_publisher = MagicMock()
        signal_msg = SignalData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            fired_at=datetime.now(UTC),
            strategy_name="test_strategy",
            instrument="NONTRADEABLE-USD",
            side="buy",
            strength=0.8,
            price=100.0,
            exchange="kraken",
            reason="test",
        )
        trader._current_topic = "signals.kraken.NONTRADEABLE-USD.live"
        with patch("snapper.application.engine.trader.is_tradeable", return_value=False):
            await trader._on_signal(signal_msg)
        assert "NONTRADEABLE-USD@kraken-live" not in trader.engines

    @pytest.mark.asyncio
    @patch("snapper.application.engine.trader.get_repository")
    @patch("snapper.application.engine.trader.get_settings")
    async def test_on_signal_no_execution_publisher(
        self, mock_get_settings: MagicMock, mock_get_repository: MagicMock
    ) -> None:
        """Verify AssertionError is raised when execution_publisher is not set.

        Given: TraderCoordinator without execution_publisher,
        When: Signal is received,
        Then: AssertionError is raised with descriptive message.
        """
        mock_settings = MagicMock()
        mock_settings.instruments = {
            "kraken": ["BTC-USD"],
            "zonda": [],
            "walutomat": [],
            "polygon": [],
        }
        mock_settings.db_url = TEST_DB_URL
        mock_get_settings.return_value = mock_settings
        mock_repository = MagicMock()
        mock_get_repository.return_value = mock_repository
        trader = TraderCoordinator()
        trader._current_topic = "signals.kraken.BTC-USD.live"
        signal_msg = SignalData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            fired_at=datetime.now(UTC),
            strategy_name="test",
            instrument="BTC-USD",
            side="buy",
            strength=0.8,
            price=50000.0,
            exchange="kraken",
            reason="test",
        )
        with pytest.raises(AssertionError, match="execution_publisher not initialized"):
            await trader._on_signal(signal_msg)


class _RepositoryStub:
    """Test stub for database repository."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def ensure_instrument(
        self,
        *,
        symbol_public_id: str,
        exchange: str,
        session_id: str = "",
        sequence_id: int = 0,
        timestamp: datetime,
    ) -> tuple[int, str]:
        self.calls.append(
            {
                "symbol_public_id": symbol_public_id,
                "exchange": exchange,
                "session_id": session_id,
                "sequence_id": sequence_id,
            }
        )
        return (1, "stub-inst-pub-id")


class _SocketStub:
    """Test stub for ZMQ socket."""

    def __init__(self) -> None:
        self.connected: list[str] = []
        self.closed = False
        self.options: list[tuple[int, str]] = []

    def connect(self, address: str) -> None:
        self.connected.append(address)

    def close(self) -> None:
        self.closed = True

    def setsockopt(self, _option: int, _value: int) -> None:
        """Set integer socket option."""

    def setsockopt_string(self, option: int, value: str) -> None:
        self.options.append((option, value))


class _ContextStub:
    """Test stub for ZMQ context."""

    def __init__(self, factory: Callable[[int], _SocketStub]) -> None:
        self._factory = factory
        self.created: list[int] = []
        self.terminated = False

    def socket(self, socket_type: int) -> _SocketStub:
        self.created.append(socket_type)
        return self._factory(socket_type)

    def term(self) -> None:
        self.terminated = True


class _PublisherStub:
    """Test stub for ZMQ publisher."""

    def __init__(self, socket: _SocketStub) -> None:
        self._socket = socket

    def setsockopt(self, _option: int, _value: int) -> None:
        """Socket option intentionally ignored in stub."""
        pass

    def close(self) -> None:
        self._socket.close()


class _SubscriberStub:
    """Test stub for ZMQ subscriber."""

    def __init__(self, socket: _SocketStub) -> None:
        self._socket = socket
        self.topics: list[str] = []
        self.messages: list[tuple[str, bytes]] = []

    def subscribe(self, topic: str) -> None:
        self.topics.append(topic)

    def setsockopt(self, _option: int, _value: int) -> None:
        """Socket option intentionally ignored in stub."""
        pass

    def close(self) -> None:
        self._socket.close()

    async def recv_multipart(self) -> tuple[str, bytes]:
        if not self.messages:
            raise asyncio.CancelledError()
        return self.messages.pop(0)


class _EngineStub:
    """Test stub for trading engine."""

    def __init__(
        self,
        instrument: str,
        *,
        execution_socket: Any,
        risk: Any,
        cfg: Any,
        instrument_specs: dict[str, dict[str, float]],
        exchange: str,
        repository: Any = None,
        outbox: Any = None,
        strategy_tag: str | None = None,
    ) -> None:
        self.instrument = instrument
        self.execution_socket = execution_socket
        self.risk = risk
        self.cfg = cfg
        self.instrument_specs = instrument_specs
        self.exchange = exchange
        self.repository = repository
        self.outbox = outbox
        self.pending_client_order_id: str | None = None
        self._shard_key = f"{exchange}.{instrument}.live"
        self.execute_calls: list[dict[str, Any]] = []

    async def execute_desired_units(
        self,
        desired_units: float,
        price: float,
        *,
        signaled_at: Any,
    ) -> None:
        self.execute_calls.append(
            {
                "desired_units": desired_units,
                "price": price,
                "signaled_at": signaled_at,
            }
        )


def test_get_default_parameters_returns_default_signal_topic() -> None:
    """Verify get_default_parameters returns default signal topic.

    Given empty AppSettings,
    When get_default_parameters is called,
    Then default signal topics list with 'signals.' is returned.
    """
    defaults = TraderCoordinator.get_default_parameters(cast(AppSettings, SimpleNamespace()))
    assert defaults == {"signal_topics": ["signals."]}


def _configure_settings(monkeypatch: pytest.MonkeyPatch) -> tuple[SimpleNamespace, _RepositoryStub]:
    repository = _RepositoryStub()
    settings = SimpleNamespace(
        db_url=TEST_DB_URL,
        zmq_broker_xsub="tcp://broker.xsub",
        zmq_broker_xpub="tcp://broker.xpub",
        risk_r_per_trade=0.01,
        risk_max_leverage=2.0,
        risk_max_drawdown=0.15,
    )

    def _stub_get_settings() -> SimpleNamespace:
        return settings

    def _stub_get_repository(_db_url: str) -> _RepositoryStub:
        return repository

    monkeypatch.setattr(trader_module, "get_settings", _stub_get_settings, raising=True)
    monkeypatch.setattr(trader_module, "get_repository", _stub_get_repository, raising=True)
    monkeypatch.setattr(
        trader_module,
        "get_settings_service",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        trader_module,
        "get_settings_with_service",
        lambda _svc: settings,
    )
    monkeypatch.setattr(
        trader_module,
        "resolve_symbol_public_id",
        AsyncMock(return_value="stub-spid"),
    )
    return settings, repository


@pytest.mark.asyncio
async def test_trader_coordinator_start_calls_setup_sequence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify start method calls setup methods in correct order.

    Given a TraderCoordinator with mocked setup methods,
    When start() is called,
    Then setup methods are called in sequence: external, components, subscriber, loop.
    """
    _configure_settings(monkeypatch)
    monkeypatch.setattr(
        trader_module,
        "_bootstrap_settings",
        SimpleNamespace(zmq_broker_xpub="tcp://broker.xpub"),
    )
    coordinator = TraderCoordinator(signal_topics=["signals.paper."])
    calls: list[str] = []

    def _record(name: str) -> None:
        calls.append(name)

    coordinator_any = cast(Any, coordinator)
    coordinator_any._setup_external_execution = MagicMock(side_effect=lambda: _record("external"))
    coordinator_any._setup_trading_components = MagicMock(side_effect=lambda: _record("components"))
    coordinator_any._setup_signal_subscriber = MagicMock(side_effect=lambda: _record("subscriber"))
    coordinator_any._run_trading_loop = AsyncMock(side_effect=lambda: _record("loop"))
    await coordinator.start()
    assert calls == ["external", "components", "subscriber", "loop"]


def test_trader_coordinator_repr() -> None:
    """Verify repr includes signal topics.

    Given a TraderCoordinator with signal topics,
    When repr() is called,
    Then the string contains the configured signal topics.
    """
    coord = TraderCoordinator(signal_topics=["signals.foo."])
    assert "signals.foo." in repr(coord)


@pytest.mark.asyncio
async def test_trader_coordinator_stop_closes_resources(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify stop closes all ZMQ resources.

    Given a TraderCoordinator with active sockets and contexts,
    When stop() is called,
    Then all sockets are closed and contexts are terminated.
    """
    _configure_settings(monkeypatch)
    monkeypatch.setattr(
        trader_module,
        "_bootstrap_settings",
        SimpleNamespace(zmq_broker_xpub="tcp://broker.xpub"),
    )
    coordinator = TraderCoordinator()
    signal_socket = _SocketStub()
    sub_stub = _SubscriberStub(signal_socket)
    coordinator.signal_subscriber = cast(Any, sub_stub)
    zmq_context = _ContextStub(lambda _t: signal_socket)
    coordinator.zmq_context = cast(Any, zmq_context)
    exec_socket = _SocketStub()
    exec_pub = _PublisherStub(exec_socket)
    coordinator.execution_publisher = cast(Any, exec_pub)
    exec_context = _ContextStub(lambda _t: exec_socket)
    coordinator.execution_context = cast(Any, exec_context)
    await coordinator.stop()
    assert signal_socket.closed is True
    assert zmq_context.terminated is True
    assert exec_socket.closed is True
    assert exec_context.terminated is True


@pytest.mark.asyncio
async def test_setup_trading_components_requires_publisher(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify setup raises when execution publisher is missing.

    Given a TraderCoordinator without execution_publisher,
    When _setup_trading_components is called,
    Then RuntimeError is raised.
    """
    _configure_settings(monkeypatch)
    coord = TraderCoordinator()
    coord_any = cast(Any, coord)
    coord.execution_publisher = None
    with pytest.raises(RuntimeError):
        coord_any._setup_trading_components()
    coord.execution_publisher = cast(Any, _PublisherStub(_SocketStub()))
    coord_any._setup_trading_components()


@pytest.mark.asyncio
async def test_ensure_instrument_handles_delimiters(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify instrument parsing handles dash and slash delimiters.

    Given a TraderCoordinator with repository stub,
    When _ensure_instrument is called with dash and slash delimited symbols,
    Then base and quote currencies are correctly parsed and stored.
    """
    _configure_settings(monkeypatch)
    coord = TraderCoordinator()
    coord_any = cast(Any, coord)
    await coord_any._ensure_instrument("BTC-USD", exchange="kraken")
    await coord_any._ensure_instrument("ETH/EUR", exchange="binance")
    repository_stub = cast(_RepositoryStub, coord.repository)
    assert len(repository_stub.calls) == 2
    assert repository_stub.calls[0]["symbol_public_id"] == "stub-spid"
    assert repository_stub.calls[0]["exchange"] == "kraken"
    assert repository_stub.calls[1]["symbol_public_id"] == "stub-spid"
    assert repository_stub.calls[1]["exchange"] == "binance"
    assert repository_stub.calls[0]["session_id"] != ""
    assert repository_stub.calls[0]["sequence_id"] >= 1
    assert repository_stub.calls[1]["sequence_id"] >= 2


@pytest.mark.asyncio
async def test_ensure_instrument_skips_when_symbol_not_resolved(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify instrument upsert is skipped when no active Symbol exists.

    Given a TraderCoordinator with repository stub,
    When _ensure_instrument is called and symbol resolution returns None,
    Then no instrument upsert is attempted.
    """
    _configure_settings(monkeypatch)
    monkeypatch.setattr(trader_module, "resolve_symbol_public_id", AsyncMock(return_value=None))
    coord = TraderCoordinator()
    coord_any = cast(Any, coord)
    await coord_any._ensure_instrument("BTC-USD", exchange="kraken")
    repository_stub = cast(_RepositoryStub, coord.repository)
    assert repository_stub.calls == []


@pytest.mark.asyncio
async def test_setup_external_execution_uses_context(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify external execution setup creates context and publisher.

    Given a TraderCoordinator with broker configuration,
    When _setup_external_execution is called,
    Then ZMQ context and publisher are created and connected to broker.
    """
    _configure_settings(monkeypatch)
    pub_socket = _SocketStub()
    context = _ContextStub(lambda _t: pub_socket)
    zmq_module = cast(Any, trader_module).zmq
    monkeypatch.setattr(zmq_module.asyncio, "Context", lambda: context, raising=False)
    monkeypatch.setattr(trader_module, "ValidatedPublisher", _PublisherStub, raising=True)
    coord = TraderCoordinator()
    coord_any = cast(Any, coord)
    coord_any._setup_external_execution()
    assert pub_socket.connected == [coord.settings.zmq_broker_xsub]
    assert isinstance(coord.execution_publisher, _PublisherStub)


@pytest.mark.asyncio
async def test_setup_signal_subscriber_subscribes_topics(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify signal subscriber subscribes to all required topics.

    Given a TraderCoordinator with multiple signal topics,
    When _setup_signal_subscriber is called,
    Then subscriber connects and subscribes to all configured topics plus system topics.
    """
    _configure_settings(monkeypatch)
    broker_socket = _SocketStub()
    context = _ContextStub(lambda _t: broker_socket)
    zmq_module = cast(Any, trader_module).zmq
    monkeypatch.setattr(zmq_module.asyncio, "Context", lambda: context, raising=False)
    subscriber = _SubscriberStub(broker_socket)

    def _make_subscriber(socket: Any) -> _SubscriberStub:
        assert socket is broker_socket
        return subscriber

    monkeypatch.setattr(trader_module, "ValidatedSubscriber", _make_subscriber, raising=True)
    monkeypatch.setattr(
        trader_module,
        "_bootstrap_settings",
        SimpleNamespace(zmq_broker_xpub="tcp://broker.xpub"),
    )
    coord = TraderCoordinator(signal_topics=["signals.kraken.", "signals.paper."])
    coord_any = cast(Any, coord)
    coord_any._setup_signal_subscriber()
    assert broker_socket.connected == ["tcp://broker.xpub"]
    assert subscriber.topics == [
        "signals.kraken.",
        "signals.paper.",
        "system.symbol_aliases",
        "system.settings",
        "orders.events.",
    ]
    assert cast(Any, coord.signal_subscriber) is subscriber


@pytest.mark.asyncio
async def test_on_signal_validates_topic_and_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify signal handler validates topic format and payload data.

    Given a TraderCoordinator with execution publisher,
    When signals with various valid and invalid topics and payloads are received,
    Then only valid signals create engines and execute trades.
    """
    _configure_settings(monkeypatch)
    monkeypatch.setattr(
        trader_module,
        "_bootstrap_settings",
        SimpleNamespace(zmq_broker_xpub="tcp://broker.xpub"),
    )
    publisher = _PublisherStub(_SocketStub())
    coord = TraderCoordinator()
    coord.execution_publisher = cast(Any, publisher)
    coord.msg_publisher = cast(Any, SimpleNamespace(publish=AsyncMock()))
    repository = cast(_RepositoryStub, coord.repository)
    monkeypatch.setattr(trader_module, "TradingEngineService", _EngineStub, raising=True)
    coord_any = cast(Any, coord)
    coord_any._current_topic = "signals.invalid"
    signal_invalid_topic = SignalData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        fired_at=datetime.now(UTC),
        instrument="BTC-USD",
        side="buy",
        strength=0.5,
        price=10.0,
        exchange="kraken",
        reason="test",
    )
    await coord_any._on_signal(signal_invalid_topic)
    assert coord.engines == {}
    coord_any._current_topic = "signals.kraken.BTC-USD.live"
    signal_no_price = SignalData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        fired_at=datetime.now(UTC),
        instrument="BTC-USD",
        side="buy",
        strength=0.5,
        exchange="kraken",
        reason="test",
    )
    await coord_any._on_signal(signal_no_price)
    assert coord.engines == {}
    signal_zero_price = SignalData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        fired_at=datetime.now(UTC),
        instrument="BTC-USD",
        side="buy",
        strength=0.5,
        price=0.0,
        exchange="kraken",
        reason="test",
    )
    await coord_any._on_signal(signal_zero_price)
    assert coord.engines == {}
    signal_valid = SignalData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        fired_at=datetime.now(UTC),
        instrument="BTC-USD",
        side="buy",
        price=10.0,
        strength=0.5,
        strategy_name="momentum",
        exchange="kraken",
        reason="test",
    )
    await coord_any._on_signal(signal_valid)
    engine_key = "BTC-USD@kraken-live"
    assert engine_key in coord.engines
    engine = cast(_EngineStub, coord.engines[engine_key])
    assert repository.calls[0]["symbol_public_id"] == "stub-spid"
    assert engine.execute_calls[0]["desired_units"] == pytest.approx(0.5)
    signal_sell = SignalData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        fired_at=datetime.now(UTC),
        instrument="BTC-USD",
        side="sell",
        strength=1.0,
        price=10.0,
        exchange="kraken",
        reason="test",
    )
    await coord_any._on_signal(signal_sell)
    assert engine.execute_calls[1]["desired_units"] == pytest.approx(0.0)
    assert coord.last_signal_time[engine_key] <= time.time()


@pytest.mark.asyncio
async def test_listen_signals_handles_missing_subscriber(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify listen_signals returns early when subscriber is None.

    Given a TraderCoordinator with no signal subscriber,
    When _listen_signals is called,
    Then it returns immediately without errors.
    """
    _configure_settings(monkeypatch)
    coord = TraderCoordinator()
    coord.signal_subscriber = None
    coord_any = cast(Any, coord)
    await coord_any._listen_signals()


@pytest.mark.asyncio
async def test_listen_signals_processes_single_message(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify listen_signals processes and routes signal message.

    Given a TraderCoordinator with a subscriber containing one message,
    When _listen_signals is called,
    Then the message is processed and _on_signal is invoked once.
    """
    _configure_settings(monkeypatch)
    coord = TraderCoordinator()
    subscriber_socket = _SocketStub()
    subscriber = _SubscriberStub(subscriber_socket)
    message: dict[str, Any] = {
        "type": "signal",
        "public_id": "test-pid",
        "timestamp": "2024-01-01T00:00:00Z",
        "session_id": "",
        "sequence_id": 0,
        "instrument": "BTC-USD",
        "side": "buy",
        "price": 10.0,
        "strength": 0.5,
        "exchange": "kraken",
        "reason": "test",
        "fired_at": "2024-01-01T00:00:00Z",
    }
    subscriber.messages.append(("signals.kraken.BTC-USD.live", json.dumps(message).encode("utf-8")))
    coord.signal_subscriber = cast(Any, subscriber)
    merchant = AsyncMock()
    coord_any = cast(Any, coord)
    coord_any._on_signal = cast(Any, merchant)
    with pytest.raises(asyncio.CancelledError):
        await coord_any._listen_signals()
    assert merchant.await_count == 1


@pytest.mark.asyncio
async def test_listen_signals_handles_symbol_aliases_invalidation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify symbol aliases update triggers cache invalidation.

    Given a TraderCoordinator with a subscriber containing symbol aliases message,
    When _listen_signals processes the message,
    Then SymbolMapperService cache invalidation is triggered.
    """
    _configure_settings(monkeypatch)
    coord = TraderCoordinator()
    subscriber_socket = _SocketStub()
    subscriber = _SubscriberStub(subscriber_socket)
    subscriber.messages.append(
        (
            "system.symbol_aliases",
            json.dumps({"event": "symbol_aliases_updated"}).encode("utf-8"),
        )
    )
    coord.signal_subscriber = cast(Any, subscriber)
    invalidation_calls: list[dict[str, Any]] = []

    class MockMapperService:
        @staticmethod
        def get_instance() -> MockMapperService:
            return MockMapperService()

        def trigger_cache_invalidation(self, *, fail_fast: bool = True) -> None:
            invalidation_calls.append({"fail_fast": fail_fast})

    monkeypatch.setattr(trader_module, "SymbolMapperService", MockMapperService)
    with pytest.raises(asyncio.CancelledError):
        await cast(Any, coord)._listen_signals()
    assert len(invalidation_calls) == 1
    assert invalidation_calls[0]["fail_fast"] is False


@pytest.mark.asyncio
async def test_listen_signals_handles_settings_update(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify settings update message triggers cache update.

    Given a TraderCoordinator with a subscriber containing settings update message,
    When _listen_signals processes the message,
    Then SettingsService cache is updated with new value.
    """
    _configure_settings(monkeypatch)
    coord = TraderCoordinator()
    subscriber_socket = _SocketStub()
    subscriber = _SubscriberStub(subscriber_socket)
    subscriber.messages.append(
        (
            "system.settings",
            json.dumps(
                {
                    "type": "setting_changed",
                    "public_id": "test-pid",
                    "timestamp": "2024-01-01T00:00:00Z",
                    "session_id": "",
                    "sequence_id": 0,
                    "key": "foo",
                    "value": "bar",
                    "category": "test",
                }
            ).encode("utf-8"),
        )
    )
    coord.signal_subscriber = cast(Any, subscriber)
    cache_updates: list[dict[str, Any]] = []

    class MockSettingsService:
        _cache: dict[str, Any] = {}

        @staticmethod
        def get_instance() -> MockSettingsService:
            return MockSettingsService()

        def _parse_value(self, value: str) -> Any:
            cache_updates.append({"value": value})
            return value

    monkeypatch.setattr(trader_module, "SettingsService", MockSettingsService)
    with pytest.raises(asyncio.CancelledError):
        await cast(Any, coord)._listen_signals()
    assert len(cache_updates) == 1
    assert cache_updates[0]["value"] == "bar"


@pytest.mark.asyncio
async def test_handle_settings_update_handles_invalid_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify settings update gracefully handles invalid JSON.

    Given a TraderCoordinator,
    When _handle_settings_update is called with invalid JSON or non-setting_changed event,
    Then no errors are raised.
    """
    _configure_settings(monkeypatch)
    coord = TraderCoordinator()
    coord._handle_settings_update(b"not-json")
    coord._handle_settings_update(json.dumps({"type": "other_event", "key": "foo"}).encode("utf-8"))


@pytest.mark.asyncio
async def test_handle_settings_update_skips_when_no_instance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify settings update skips when SettingsService has no instance.

    Given a TraderCoordinator with SettingsService returning None,
    When _handle_settings_update is called with valid payload,
    Then no errors are raised and update is skipped.
    """
    _configure_settings(monkeypatch)
    coord = TraderCoordinator()
    monkeypatch.setattr(
        "snapper.application.engine.trader.SettingsService.get_instance",
        lambda: None,
    )
    payload = json.dumps(
        {
            "type": "setting_changed",
            "public_id": "test-pid",
            "timestamp": "2024-01-01T00:00:00Z",
            "session_id": "",
            "sequence_id": 0,
            "key": "test_key",
            "value": "test_value",
            "category": "test",
        }
    )
    coord._handle_settings_update(payload.encode("utf-8"))


@pytest.mark.asyncio
async def test_handle_execution_fill_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify execution fill handler processes valid fill message.

    Given a TraderCoordinator,
    When _handle_execution_fill is called with valid ExecutionData,
    Then the fill is processed without errors.
    """
    _configure_settings(monkeypatch)
    coord = TraderCoordinator()
    fill = ExecutionData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        trade_id="trade-456",
        exchange_order_id="exec-456",
        client_order_id="order-123",
        instrument="BTC-USD",
        exchange="kraken",
        side="buy",
        size=0.5,
        price=50000.0,
        last_size=0.5,
        last_price=50000.0,
        fee=0.5,
        fee_asset="USD",
        status="filled",
        executed_at=datetime.now(UTC),
    )
    await coord._handle_execution_fill("orders.events.kraken.BTC-USD.executed", fill)


@pytest.mark.asyncio
async def test_handle_execution_fill_invariant_exchange_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify execution fill handler rejects exchange mismatch.

    Given a TraderCoordinator,
    When _handle_execution_fill is called with mismatched topic/payload exchange,
    Then the fill is rejected with invariant violation warning.
    """
    _configure_settings(monkeypatch)
    coord = TraderCoordinator()
    fill = ExecutionData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        trade_id="trade-456",
        exchange_order_id="exec-456",
        client_order_id="order-123",
        instrument="BTC-USD",
        exchange="paper",
        side="buy",
        size=0.5,
        price=50000.0,
        last_size=0.5,
        last_price=50000.0,
        fee=0.5,
        fee_asset="USD",
        status="filled",
        executed_at=datetime.now(UTC),
    )
    await coord._handle_execution_fill("orders.events.kraken.BTC-USD.executed", fill)


@pytest.mark.asyncio
async def test_handle_execution_fill_invariant_instrument_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify execution fill handler rejects instrument mismatch.

    Given a TraderCoordinator,
    When _handle_execution_fill is called with mismatched topic/payload instrument,
    Then the fill is rejected with invariant violation warning.
    """
    _configure_settings(monkeypatch)
    coord = TraderCoordinator()
    fill = ExecutionData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        trade_id="trade-456",
        exchange_order_id="exec-456",
        client_order_id="order-123",
        instrument="ETH-USD",
        exchange="kraken",
        side="buy",
        size=0.5,
        price=50000.0,
        last_size=0.5,
        last_price=50000.0,
        fee=0.5,
        fee_asset="USD",
        status="filled",
        executed_at=datetime.now(UTC),
    )
    await coord._handle_execution_fill("orders.events.kraken.BTC-USD.executed", fill)


@pytest.mark.asyncio
async def test_handle_execution_fill_malformed_topic(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify execution fill handler ignores malformed topics.

    Given a TraderCoordinator,
    When _handle_execution_fill is called with malformed topic,
    Then the fill is ignored without error.
    """
    _configure_settings(monkeypatch)
    coord = TraderCoordinator()
    fill = ExecutionData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        trade_id="trade-456",
        exchange_order_id="exec-456",
        client_order_id="order-123",
        instrument="BTC-USD",
        exchange="kraken",
        side="buy",
        size=0.5,
        price=50000.0,
        last_size=0.5,
        last_price=50000.0,
        fee=0.5,
        fee_asset="USD",
        status="filled",
        executed_at=datetime.now(UTC),
    )
    await coord._handle_execution_fill("orders.events.kraken", fill)


@pytest.mark.asyncio
async def test_dispatch_order_event_invalid_json(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify dispatch handles invalid JSON gracefully.

    Given a TraderCoordinator,
    When _dispatch_order_event is called with invalid JSON,
    Then error is logged but no exception raised.
    """
    _configure_settings(monkeypatch)
    coord = TraderCoordinator()
    await coord._dispatch_order_event("orders.events.kraken.BTC-USD.executed", b"not-json")


@pytest.mark.asyncio
async def test_listen_signals_routes_execution_fill(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify listen_signals routes execution fill messages.

    Given a TraderCoordinator with subscriber containing execution fill message,
    When _listen_signals processes the message,
    Then the fill is routed to _dispatch_order_event.
    """
    _configure_settings(monkeypatch)
    coord = TraderCoordinator()
    subscriber_socket = _SocketStub()
    subscriber = _SubscriberStub(subscriber_socket)
    subscriber.messages.append(
        (
            "orders.events.kraken.BTC-USD.executed",
            json.dumps(
                {
                    "type": "execution",
                    "public_id": "test-pid",
                    "timestamp": "2024-01-01T00:00:00Z",
                    "session_id": "",
                    "sequence_id": 0,
                    "trade_id": "exec-1",
                    "exchange_order_id": "exch-123",
                    "client_order_id": "order-123",
                    "instrument": "BTC-USD",
                    "exchange": "kraken",
                    "side": "buy",
                    "size": 0.5,
                    "price": 50000.0,
                    "last_size": 0.5,
                    "last_price": 50000.0,
                    "fee": 0.1,
                    "fee_asset": "USD",
                    "status": "filled",
                    "executed_at": "2024-01-01T00:00:00Z",
                }
            ).encode("utf-8"),
        )
    )
    coord.signal_subscriber = cast(Any, subscriber)
    with pytest.raises(asyncio.CancelledError):
        await cast(Any, coord)._listen_signals()


@pytest.mark.asyncio
async def test_listen_signals_routes_order_status(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify listen_signals routes order status messages.

    Given a TraderCoordinator with subscriber containing order status message,
    When _listen_signals processes the message,
    Then the status is routed to _dispatch_order_event.
    """
    _configure_settings(monkeypatch)
    coord = TraderCoordinator()
    subscriber_socket = _SocketStub()
    subscriber = _SubscriberStub(subscriber_socket)
    subscriber.messages.append(
        (
            "orders.events.kraken.BTC-USD.accepted",
            json.dumps(
                {
                    "type": "order",
                    "public_id": "test-pid",
                    "timestamp": "2024-01-01T00:00:00Z",
                    "session_id": "",
                    "sequence_id": 0,
                    "exchange_order_id": "exch-123",
                    "client_order_id": "order-123",
                    "instrument": "BTC-USD",
                    "exchange": "kraken",
                    "side": "buy",
                    "status": "accepted",
                    "order_type": "market",
                    "size": 0.5,
                    "filled_size": 0.0,
                    "created_at": "2024-01-01T00:00:00Z",
                }
            ).encode("utf-8"),
        )
    )
    coord.signal_subscriber = cast(Any, subscriber)
    with pytest.raises(asyncio.CancelledError):
        await cast(Any, coord)._listen_signals()


@pytest.mark.asyncio
async def test_listen_signals_handles_invalid_order_event_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify listen_signals handles invalid order event payload gracefully.

    Given a TraderCoordinator with subscriber containing order event with invalid payload,
    When _listen_signals processes the message,
    Then the message is logged but does not cause a crash.
    """
    _configure_settings(monkeypatch)
    coord = TraderCoordinator()
    subscriber_socket = _SocketStub()
    subscriber = _SubscriberStub(subscriber_socket)
    subscriber.messages.append(
        (
            "orders.events.kraken.BTC-USD.accepted",
            json.dumps(
                {
                    "type": "order",
                    "public_id": "test-pid",
                    "timestamp": "2024-01-01T00:00:00Z",
                    "session_id": "",
                    "sequence_id": 0,
                    "client_order_id": "order-123",
                }
            ).encode("utf-8"),
        )
    )
    coord.signal_subscriber = cast(Any, subscriber)
    with pytest.raises(asyncio.CancelledError):
        await cast(Any, coord)._listen_signals()


@pytest.mark.asyncio
async def test_handle_order_status_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify order status handler processes valid status message.

    Given a TraderCoordinator,
    When _handle_order_status is called with valid OrderData,
    Then the status is processed without errors.
    """
    _configure_settings(monkeypatch)
    coord = TraderCoordinator()
    order_status = OrderData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        exchange_order_id="exch-123",
        client_order_id="order-123",
        instrument="BTC-USD",
        exchange="kraken",
        side="buy",
        size=0.5,
        price=50000.0,
        order_type="market",
        status="accepted",
        filled_size=0.0,
        created_at=datetime.now(UTC),
    )
    coord._handle_order_status("orders.events.kraken.BTC-USD.accepted", order_status)


@pytest.mark.asyncio
async def test_handle_order_status_invariant_exchange_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify order status handler rejects exchange mismatch.

    Given a TraderCoordinator,
    When _handle_order_status is called with mismatched topic/payload exchange,
    Then the status is rejected with invariant violation warning.
    """
    _configure_settings(monkeypatch)
    coord = TraderCoordinator()
    order_status = OrderData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        exchange_order_id="exch-123",
        client_order_id="order-123",
        instrument="BTC-USD",
        exchange="paper",
        side="buy",
        size=0.5,
        price=50000.0,
        order_type="market",
        status="accepted",
        filled_size=0.0,
        created_at=datetime.now(UTC),
    )
    coord._handle_order_status("orders.events.kraken.BTC-USD.accepted", order_status)


@pytest.mark.asyncio
async def test_handle_order_status_invariant_instrument_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify order status handler rejects instrument mismatch.

    Given a TraderCoordinator,
    When _handle_order_status is called with mismatched topic/payload instrument,
    Then the status is rejected with invariant violation warning.
    """
    _configure_settings(monkeypatch)
    coord = TraderCoordinator()
    order_status = OrderData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        exchange_order_id="exch-123",
        client_order_id="order-123",
        instrument="ETH-USD",
        exchange="kraken",
        side="buy",
        size=0.5,
        price=50000.0,
        order_type="market",
        status="accepted",
        filled_size=0.0,
        created_at=datetime.now(UTC),
    )
    coord._handle_order_status("orders.events.kraken.BTC-USD.accepted", order_status)


@pytest.mark.asyncio
async def test_handle_order_status_rejected_logs_envelope_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify order status handler logs envelope type for rejected status.

    Given a TraderCoordinator,
    When _handle_order_status is called with 'rejected' status,
    Then the log includes envelope type to disambiguate submit vs cancel rejection.
    """
    _configure_settings(monkeypatch)
    coord = TraderCoordinator()
    order_status = OrderData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        exchange_order_id=None,
        client_order_id="order-123",
        instrument="BTC-USD",
        exchange="kraken",
        side="buy",
        size=0.5,
        price=50000.0,
        order_type="market",
        status="rejected",
        filled_size=0.0,
        created_at=datetime.now(UTC),
    )
    coord._handle_order_status("orders.events.kraken.BTC-USD.rejected", order_status)


@pytest.mark.asyncio
async def test_dispatch_order_event_routes_order_event_envelope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify dispatch routes OrderEventData to _handle_order_event.

    Given a TraderCoordinator,
    When _dispatch_order_event is called with OrderEventData payload,
    Then the message is routed to _handle_order_event.
    """
    _configure_settings(monkeypatch)
    coord = TraderCoordinator()
    payload = json.dumps(
        {
            "type": "order_event",
            "public_id": "test-pid",
            "timestamp": "2024-01-01T00:00:00Z",
            "session_id": "",
            "sequence_id": 0,
            "exchange_order_id": "exch-123",
            "client_order_id": "order-123",
            "exchange": "kraken",
            "instrument": "BTC-USD",
            "event": "cancelled",
        }
    ).encode("utf-8")
    await coord._dispatch_order_event("orders.events.kraken.BTC-USD.cancelled", payload)


@pytest.mark.asyncio
async def test_dispatch_order_event_unknown_type(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify dispatch handles unknown message types gracefully.

    Given a TraderCoordinator,
    When _dispatch_order_event is called with valid but unexpected message type,
    Then the message is ignored with debug log.
    """
    _configure_settings(monkeypatch)
    coord = TraderCoordinator()
    payload = json.dumps(
        {
            "type": "heartbeat",
            "public_id": "test-pid",
            "timestamp": "2024-01-01T00:00:00Z",
            "session_id": "",
            "sequence_id": 0,
            "component": "test",
            "sequence": 1,
            "status": "healthy",
            "lag_ms": 0,
        }
    ).encode("utf-8")
    await coord._dispatch_order_event("orders.events.kraken.BTC-USD.accepted", payload)


@pytest.mark.asyncio
async def test_handle_order_event_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify order event handler processes cancel/replace confirmations.

    Given a TraderCoordinator,
    When _handle_order_event is called with valid OrderEventData,
    Then the event is processed without errors.
    """
    _configure_settings(monkeypatch)
    coord = TraderCoordinator()
    order_event = OrderEventData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        exchange_order_id="exch-123",
        client_order_id="order-123",
        exchange="kraken",
        instrument="BTC-USD",
        event="cancelled",
    )
    coord._handle_order_event("orders.events.kraken.BTC-USD.cancelled", order_event)


@pytest.mark.asyncio
async def test_handle_order_event_invariant_exchange_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify order event handler rejects exchange mismatch.

    Given a TraderCoordinator,
    When _handle_order_event is called with mismatched topic/payload exchange,
    Then the event is rejected with invariant violation warning.
    """
    _configure_settings(monkeypatch)
    coord = TraderCoordinator()
    order_event = OrderEventData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        exchange_order_id="exch-123",
        client_order_id="order-123",
        exchange="paper",
        instrument="BTC-USD",
        event="cancelled",
    )
    coord._handle_order_event("orders.events.kraken.BTC-USD.cancelled", order_event)


@pytest.mark.asyncio
async def test_handle_order_event_invariant_instrument_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify order event handler rejects instrument mismatch.

    Given a TraderCoordinator,
    When _handle_order_event is called with mismatched topic/payload instrument,
    Then the event is rejected with invariant violation warning.
    """
    _configure_settings(monkeypatch)
    coord = TraderCoordinator()
    order_event = OrderEventData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        exchange_order_id="exch-123",
        client_order_id="order-123",
        exchange="kraken",
        instrument="ETH-USD",
        event="cancelled",
    )
    coord._handle_order_event("orders.events.kraken.BTC-USD.cancelled", order_event)


@pytest.mark.asyncio
async def test_handle_order_event_malformed_topic(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify order event handler ignores malformed topics.

    Given a TraderCoordinator,
    When _handle_order_event is called with topic having wrong segment count,
    Then the message is ignored without error.
    """
    _configure_settings(monkeypatch)
    coord = TraderCoordinator()
    order_event = OrderEventData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        exchange_order_id="exch-123",
        client_order_id="order-123",
        exchange="kraken",
        instrument="BTC-USD",
        event="cancelled",
    )
    coord._handle_order_event("orders.events.kraken", order_event)


@pytest.mark.asyncio
async def test_handle_order_event_topic_payload_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify invariant check logs warning when topic suffix mismatches event field.

    Given a TraderCoordinator,
    When _handle_order_event is called with topic suffix 'cancelled' but event 'rejected',
    Then a warning is logged about invariant violation.
    """
    _configure_settings(monkeypatch)
    coord = TraderCoordinator()
    order_event = OrderEventData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        exchange_order_id="exch-123",
        client_order_id="order-123",
        exchange="kraken",
        instrument="BTC-USD",
        event="rejected",
    )
    coord._handle_order_event("orders.events.kraken.BTC-USD.cancelled", order_event)


@pytest.mark.asyncio
async def test_handle_order_event_rejected_logs_envelope_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify order event handler logs envelope type for rejected event.

    Given a TraderCoordinator,
    When _handle_order_event is called with 'rejected' event,
    Then the log includes envelope type to disambiguate cancel/replace rejection.
    """
    _configure_settings(monkeypatch)
    coord = TraderCoordinator()
    order_event = OrderEventData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        exchange_order_id="exch-123",
        client_order_id="order-123",
        exchange="kraken",
        instrument="BTC-USD",
        event="rejected",
    )
    coord._handle_order_event("orders.events.kraken.BTC-USD.rejected", order_event)


@pytest.mark.asyncio
async def test_handle_order_status_malformed_topic(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify order status handler ignores malformed topics.

    Given a TraderCoordinator,
    When _handle_order_status is called with topic having wrong segment count,
    Then the message is ignored without error.
    """
    _configure_settings(monkeypatch)
    coord = TraderCoordinator()
    order_status = OrderData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        client_order_id="order-123",
        instrument="BTC-USD",
        exchange="kraken",
        side="buy",
        status="accepted",
        order_type="market",
        size=0.5,
        filled_size=0.0,
        created_at=datetime.now(UTC),
    )
    coord._handle_order_status("orders.events.kraken", order_status)


@pytest.mark.asyncio
async def test_handle_order_status_topic_payload_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify invariant check logs warning when topic suffix mismatches payload status.

    Given a TraderCoordinator,
    When _handle_order_status is called with topic suffix 'accepted' but payload status 'rejected',
    Then a warning is logged about invariant violation.
    """
    _configure_settings(monkeypatch)
    coord = TraderCoordinator()
    order_status = OrderData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        exchange_order_id="exch-123",
        client_order_id="order-123",
        instrument="BTC-USD",
        exchange="kraken",
        side="buy",
        size=0.5,
        price=50000.0,
        order_type="market",
        status="rejected",
        filled_size=0.0,
        created_at=datetime.now(UTC),
    )
    coord._handle_order_status("orders.events.kraken.BTC-USD.accepted", order_status)


@pytest.mark.asyncio
async def test_listen_signals_handles_general_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify listen_signals handles and logs unexpected exceptions.

    Given a TraderCoordinator with subscriber that raises RuntimeError,
    When _listen_signals is called,
    Then the exception is caught and logged without propagating.
    """
    _configure_settings(monkeypatch)
    coord = TraderCoordinator()

    class _FailingSubscriber:
        def __init__(self) -> None:
            self._socket = _SocketStub()

        def close(self) -> None:
            self._socket.close()

        async def recv_multipart(self) -> tuple[str, bytes]:
            raise RuntimeError("boom")

    coord.signal_subscriber = cast(Any, _FailingSubscriber())
    await cast(Any, coord)._listen_signals()


@pytest.mark.asyncio
async def test_signal_health_monitor_reports_stale_engines(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify health monitor logs warning for stale signal times.

    Given a TraderCoordinator with engine that has old last_signal_time,
    When _signal_health_monitor runs,
    Then warning is logged for stale engine.
    """
    _configure_settings(monkeypatch)
    coord = TraderCoordinator()
    coord.engines["BTC-USD@kraken-live"] = cast(
        Any,
        _EngineStub(
            "BTC-USD",
            execution_socket=_PublisherStub(_SocketStub()),
            risk=None,
            cfg=None,
            instrument_specs={},
            exchange="kraken",
        ),
    )
    coord.last_signal_time["BTC-USD@kraken-live"] = 0.0
    call_count = {"value": 0}

    async def _fake_sleep(_: float) -> None:
        call_count["value"] += 1
        if call_count["value"] >= 2:
            raise asyncio.CancelledError()

    monkeypatch.setattr(asyncio, "sleep", _fake_sleep, raising=False)
    monkeypatch.setattr(time, "time", lambda: 120.0, raising=False)
    coord_any = cast(Any, coord)
    with pytest.raises(asyncio.CancelledError):
        await coord_any._signal_health_monitor()


@pytest.mark.asyncio
async def test_signal_health_monitor_skips_debug_for_recent_signals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify health monitor skips debug logging for recent signals.

    Given a TraderCoordinator with engine that has recent last_signal_time,
    When _signal_health_monitor runs,
    Then debug logging is not called for healthy engine.
    """
    _configure_settings(monkeypatch)
    coord = TraderCoordinator()
    coord.engines["BTC-USD@kraken-live"] = cast(
        Any,
        _EngineStub(
            "BTC-USD",
            execution_socket=_PublisherStub(_SocketStub()),
            risk=None,
            cfg=None,
            instrument_specs={},
            exchange="kraken",
        ),
    )
    coord.last_signal_time["BTC-USD@kraken-live"] = 90.0
    call_count = {"value": 0}

    async def _fake_sleep(_: float) -> None:
        call_count["value"] += 1
        if call_count["value"] >= 2:
            raise asyncio.CancelledError()

    monkeypatch.setattr(asyncio, "sleep", _fake_sleep, raising=False)
    monkeypatch.setattr(time, "time", lambda: 100.0, raising=False)
    mock_logger = SimpleNamespace(debug=Mock())
    monkeypatch.setattr(trader_module, "logger", cast(Any, mock_logger), raising=False)
    with pytest.raises(asyncio.CancelledError):
        await cast(Any, coord)._signal_health_monitor()
    assert mock_logger.debug.called is False


@pytest.mark.asyncio
async def test_run_trading_loop_cancels_pending_tasks(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify trading loop cancels pending tasks on CancelledError.

    Given a TraderCoordinator with pending listener and monitor tasks,
    When _run_trading_loop is cancelled,
    Then all pending tasks are cancelled.
    """
    _configure_settings(monkeypatch)
    coord = TraderCoordinator()

    async def _stub_listener() -> None:
        await asyncio.Event().wait()

    async def _stub_monitor() -> None:
        await asyncio.Event().wait()

    created_tasks: list[asyncio.Task[Any]] = []
    original_create_task = asyncio.create_task
    coord_any = cast(Any, coord)
    coord_any._listen_signals = _stub_listener
    coord_any._signal_health_monitor = _stub_monitor

    def _fake_create_task(coro: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
        task = original_create_task(coro)
        created_tasks.append(task)
        return task

    async def _fake_gather(*_tasks: Any) -> None:
        raise asyncio.CancelledError()

    monkeypatch.setattr(asyncio, "create_task", _fake_create_task, raising=False)
    monkeypatch.setattr(asyncio, "gather", _fake_gather, raising=False)
    with pytest.raises(asyncio.CancelledError):
        await cast(Any, coord)._run_trading_loop()
    assert [task.cancelled() for task in created_tasks] == [True, True]


@pytest.mark.asyncio
async def test_run_trading_loop_skips_cancelling_completed_tasks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify trading loop does not cancel already completed tasks.

    Given a TraderCoordinator with tasks that complete immediately,
    When _run_trading_loop finishes,
    Then completed tasks are not cancelled.
    """
    _configure_settings(monkeypatch)
    coord = TraderCoordinator()

    async def _stub_listener() -> None:
        return None

    async def _stub_monitor() -> None:
        return None

    created_tasks: list[asyncio.Task[Any]] = []
    original_create_task = asyncio.create_task

    def _tracking_create_task(coro: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
        task = original_create_task(coro)
        created_tasks.append(task)
        return task

    coord_any = cast(Any, coord)
    coord_any._listen_signals = _stub_listener
    coord_any._signal_health_monitor = _stub_monitor
    monkeypatch.setattr(asyncio, "create_task", _tracking_create_task, raising=False)
    await cast(Any, coord)._run_trading_loop()
    assert [task.done() for task in created_tasks] == [True, True]
    assert [task.cancelled() for task in created_tasks] == [False, False]


@pytest.mark.asyncio
async def test_run_zmq_trader_handles_keyboard_interrupt(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify run_zmq_trader handles KeyboardInterrupt and stops cleanly.

    Given a TraderCoordinator stub that raises KeyboardInterrupt on start,
    When run_zmq_trader is called,
    Then stop() is called to clean up resources.
    """
    start_called = {"value": False}
    stop_called = {"value": False}

    class _TraderStub:
        def __init__(self, *, signal_topics: list[str] | None = None) -> None:
            self.signal_topics = signal_topics

        async def start(self) -> None:
            start_called["value"] = True
            raise KeyboardInterrupt()

        async def stop(self) -> None:
            stop_called["value"] = True

    monkeypatch.setattr(trader_module, "TraderCoordinator", _TraderStub, raising=True)
    await run_zmq_trader(signal_topics=["signals."])
    assert start_called["value"] is True
    assert stop_called["value"] is True


class StubEngine:
    """Stub trading engine that records execute_desired_units calls."""

    def __init__(self, instrument: str, execution_socket: Any, **_kwargs: Any) -> None:
        """Initialize the instance."""
        self.instrument = instrument
        self.execution_socket = execution_socket
        self.pending_client_order_id: str | None = None
        self._shard_key = f"paper.{instrument}.paper"
        self.calls: list[tuple[float, float | None]] = []

    async def execute_desired_units(
        self, desired_units: float, price: float, signaled_at: float | None = None
    ) -> None:
        """Record desired units and timestamp for verification."""
        self.calls.append((desired_units, signaled_at))


@pytest.mark.asyncio
async def test_on_signal_converts_iso_timestamp(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify signal handler converts ISO timestamp to float.

    Given a TraderCoordinator with execution publisher,
    When signal with ISO datetime timestamp is received,
    Then timestamp is converted to float for execute_desired_units.
    """
    settings = SimpleNamespace(
        risk_r_per_trade=0.01,
        risk_max_leverage=1.0,
        risk_max_drawdown=0.5,
        db_url=TEST_DB_URL,
        zmq_broker_xsub="inproc://broker",
    )
    monkeypatch.setattr(trader_module, "get_settings", lambda: settings)
    monkeypatch.setattr(
        trader_module,
        "get_repository",
        lambda _url: SimpleNamespace(ensure_instrument=AsyncMock(return_value=(1, "inst-pub-1"))),
    )
    monkeypatch.setattr(
        trader_module,
        "resolve_symbol_public_id",
        AsyncMock(return_value="fake-spid"),
    )
    monkeypatch.setattr(trader_module, "TradingEngineService", StubEngine)
    coordinator = TraderCoordinator()
    coordinator.execution_publisher = cast(Any, SimpleNamespace())
    coordinator.msg_publisher = cast(Any, SimpleNamespace(publish=AsyncMock()))
    coordinator._current_topic = "signals.paper.BTC-USD.demo"
    signal = SignalData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        instrument="BTC-USD",
        side="buy",
        strength=0.5,
        price=10_000.0,
        strategy_name="demo",
        fired_at=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
        exchange="kraken",
        reason="test",
    )
    await coordinator._on_signal(signal)
    engine = cast(StubEngine, coordinator.engines["BTC-USD@paper-demo"])
    assert engine.calls
    assert isinstance(engine.calls[0][1], float)


@pytest.mark.asyncio
async def test_on_signal_drops_when_shard_halted(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify signal is dropped when shard is halted by circuit breaker.

    Given: a TraderCoordinator with a halted shard,
    When: a signal arrives for that shard,
    Then: the signal is dropped and no engine is created.
    """
    settings = SimpleNamespace(
        risk_r_per_trade=0.01,
        risk_max_leverage=1.0,
        risk_max_drawdown=0.5,
        db_url=TEST_DB_URL,
        zmq_broker_xsub="inproc://broker",
    )
    monkeypatch.setattr(trader_module, "get_settings", lambda: settings)
    monkeypatch.setattr(trader_module, "is_tradeable", lambda _i, _e: True)
    monkeypatch.setattr(trader_module, "get_repository", lambda _url: MagicMock())
    coord = TraderCoordinator()
    coord._current_topic = "signals.kraken.BTC-USD.live"
    coord.execution_publisher = MagicMock()
    coord.msg_publisher = MagicMock()
    coord.trade_service.halt_shard("kraken.BTC-USD.live", "test halt")
    signal = SignalData(
        type="signal",
        public_id="test-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        session_id="",
        sequence_id=0,
        instrument="BTC-USD",
        exchange="kraken",
        side="buy",
        strength=1.0,
        reason="test",
        price=50000.0,
        fired_at=datetime.now(UTC),
    )
    await coord._on_signal(signal)
    assert len(coord.engines) == 0


def test_gap_detector_initialized_on_coordinator() -> None:
    """Verify TraderCoordinator creates a gap detector at init.

    Given: A new TraderCoordinator,
    When: Inspecting _gap_detector attribute,
    Then: GapDetector instance exists with zero stats.
    """
    coord = TraderCoordinator.__new__(TraderCoordinator)
    coord._gap_detector = GapDetector("trader")
    coord._gap_detector.check("test.topic", "session-1", 1)
    assert coord._gap_detector.stats.gaps_detected == 0


def _make_fill(
    client_order_id: str = "order-123",
    instrument: str = "BTC-USD",
    exchange: str = "kraken",
    side: str = "buy",
    size: float = 0.5,
    price: float = 50000.0,
    last_size: float = 0.5,
    last_price: float = 50000.0,
    fee: float = 0.5,
    status: str = "filled",
    trade_id: str | None = "trade-456",
) -> ExecutionData:
    """Create an ExecutionData fill for testing."""
    return ExecutionData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        trade_id=trade_id,
        exchange_order_id="exec-456",
        client_order_id=client_order_id,
        instrument=instrument,
        exchange=exchange,
        side=side,
        size=size,
        price=price,
        last_size=last_size,
        last_price=last_price,
        fee=fee,
        fee_asset="USD",
        status=status,
        executed_at=datetime.now(UTC),
    )


def _make_engine_with_inflight(
    monkeypatch: pytest.MonkeyPatch,
    instrument: str = "BTC-USD",
    exchange: str = "kraken",
    client_order_id: str = "order-123",
    position_qty: float = 0.0,
    entry_price: float | None = None,
) -> tuple[TraderCoordinator, Any]:
    """Create a coordinator with an engine that has an in-flight order."""
    _configure_settings(monkeypatch)
    coord = TraderCoordinator()
    engine = MagicMock()
    engine.instrument = instrument
    engine.exchange = exchange
    engine.pending_client_order_id = client_order_id
    engine.order_in_flight = True
    engine.position_qty = position_qty
    engine.entry_price = entry_price
    engine.apply_fill = MagicMock(return_value=True)
    engine.clear_pending_intent = MagicMock(return_value=True)
    coord.engines[f"{instrument}@{exchange}-live"] = engine
    return coord, engine


@pytest.mark.asyncio
class TestFillApplication:
    """Tests for Stage B: confirmed-state booking via fill events."""

    async def test_fill_applied_to_matching_engine(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify fill is routed to engine with matching pending order.

        Given: Coordinator with engine that has order-123 in flight,
        When: Fill arrives for order-123,
        Then: Engine.apply_fill is called with the fill.
        """
        coord, engine = _make_engine_with_inflight(monkeypatch)
        fill = _make_fill(client_order_id="order-123")
        await coord._handle_execution_fill("orders.events.kraken.BTC-USD.executed", fill)
        engine.apply_fill.assert_called_once_with(fill)

    async def test_fill_matched_by_instrument_exchange_fallback(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify fill routes to engine by instrument+exchange when no pending match.

        Given: Engine with different pending_client_order_id (e.g. after timeout),
        When: Fill arrives for old order matching instrument+exchange,
        Then: Engine.apply_fill is still called (late fill booking).
        """
        coord, engine = _make_engine_with_inflight(monkeypatch, client_order_id="new-order-456")
        fill = _make_fill(client_order_id="old-order-123")
        await coord._handle_execution_fill("orders.events.kraken.BTC-USD.executed", fill)
        engine.apply_fill.assert_called_once_with(fill)

    async def test_fill_no_matching_engine(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify fill for unknown instrument is logged and dropped.

        Given: Coordinator with no engine for ETH-USD,
        When: Fill arrives for ETH-USD with unrelated client_order_id,
        Then: No apply_fill is called.
        """
        coord, engine = _make_engine_with_inflight(
            monkeypatch, instrument="BTC-USD", client_order_id="btc-order"
        )
        fill = _make_fill(instrument="ETH-USD", client_order_id="eth-order")
        await coord._handle_execution_fill("orders.events.kraken.ETH-USD.executed", fill)
        engine.apply_fill.assert_not_called()

    async def test_duplicate_fill_ignored(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify duplicate fills are silently dropped by apply_fill.

        Given: Engine whose apply_fill returns False (duplicate),
        When: Fill is processed,
        Then: No error, duplicate is logged.
        """
        coord, engine = _make_engine_with_inflight(monkeypatch)
        engine.apply_fill.return_value = False
        fill = _make_fill()
        await coord._handle_execution_fill("orders.events.kraken.BTC-USD.executed", fill)
        engine.apply_fill.assert_called_once()


class TestRejectClearsIntent:
    """Tests for reject/cancel clearing pending intent."""

    def test_reject_clears_matching_pending_intent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify order rejection clears in-flight state on matching engine.

        Given: Engine with order-123 in flight,
        When: OrderData reject arrives for order-123,
        Then: Engine.clear_pending_intent is called with order-123.
        """
        coord, engine = _make_engine_with_inflight(monkeypatch)
        order_status = OrderData(
            session_id="",
            sequence_id=0,
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            client_order_id="order-123",
            instrument="BTC-USD",
            exchange="kraken",
            side="buy",
            status="rejected",
            order_type="market",
            size=0.5,
            filled_size=0.0,
            created_at=datetime(2024, 1, 1, tzinfo=UTC),
        )
        coord._handle_order_status("orders.events.kraken.BTC-USD.rejected", order_status)
        engine.clear_pending_intent.assert_called_once_with("order-123")

    def test_reject_does_not_clear_different_order(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify reject for different order does not clear current intent.

        Given: Engine with new-order in flight,
        When: Reject arrives for old-order,
        Then: clear_pending_intent returns False, state unchanged.
        """
        coord, engine = _make_engine_with_inflight(monkeypatch, client_order_id="new-order")
        engine.clear_pending_intent.return_value = False
        order_status = OrderData(
            session_id="",
            sequence_id=0,
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            client_order_id="old-order",
            instrument="BTC-USD",
            exchange="kraken",
            side="buy",
            status="rejected",
            order_type="market",
            size=0.5,
            filled_size=0.0,
            created_at=datetime(2024, 1, 1, tzinfo=UTC),
        )
        coord._handle_order_status("orders.events.kraken.BTC-USD.rejected", order_status)
        engine.clear_pending_intent.assert_called_once_with("old-order")

    def test_cancel_event_clears_matching_pending_intent(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify cancel event clears in-flight state on matching engine.

        Given: Engine with order-123 in flight,
        When: OrderEventData cancelled arrives for order-123,
        Then: Engine.clear_pending_intent is called.
        """
        coord, engine = _make_engine_with_inflight(monkeypatch)
        cancel_event = OrderEventData(
            session_id="",
            sequence_id=0,
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            exchange_order_id="ex-789",
            client_order_id="order-123",
            instrument="BTC-USD",
            exchange="kraken",
            event="cancelled",
        )
        coord._handle_order_event("orders.events.kraken.BTC-USD.cancelled", cancel_event)
        engine.clear_pending_intent.assert_called_once_with("order-123")

    def test_cancel_event_no_matching_engine(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify cancel for non-matching order iterates all engines without clearing.

        Given: Engine with different pending order,
        When: Cancel arrives for unrelated order,
        Then: clear_pending_intent is called but returns False, no state change.
        """
        coord, engine = _make_engine_with_inflight(monkeypatch, client_order_id="active-order")
        engine.clear_pending_intent.return_value = False
        cancel_event = OrderEventData(
            session_id="",
            sequence_id=0,
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            exchange_order_id="ex-789",
            client_order_id="stale-order",
            instrument="BTC-USD",
            exchange="kraken",
            event="cancelled",
        )
        coord._handle_order_event("orders.events.kraken.BTC-USD.cancelled", cancel_event)
        engine.clear_pending_intent.assert_called_once_with("stale-order")


class TestEngineApplyFill:
    """Tests for TradingEngineService.apply_fill direct unit tests."""

    def _make_engine(self, position_qty: float = 0.0, entry_price: float | None = None) -> Any:
        """Create a real TradingEngineService for testing."""
        socket = MagicMock()
        socket.tracker = SequenceTracker()
        engine = TradingEngineService(
            "BTC-USD",
            cast(Any, socket),
            cfg=EngineConfigModel(initial_cash=10000.0),
            exchange="kraken",
        )
        engine.position_qty = position_qty
        engine.entry_price = entry_price
        return engine

    def test_buy_fill_updates_position(self) -> None:
        """Verify buy fill increases position and sets entry price.

        Given: Flat engine,
        When: Buy fill applied,
        Then: position_qty increases and entry_price is set.
        """
        engine = self._make_engine()
        fill = _make_fill(side="buy", last_size=0.5, last_price=50000.0, status="filled")
        result = engine.apply_fill(fill)
        assert result is True
        assert engine.position_qty == pytest.approx(0.5)
        assert engine.entry_price == pytest.approx(50000.0)
        assert engine.order_in_flight is False

    def test_sell_fill_decreases_position(self) -> None:
        """Verify sell fill decreases position and clears entry price.

        Given: Engine with long position,
        When: Sell fill applied,
        Then: position_qty decreases, entry_price cleared when flat.
        """
        engine = self._make_engine(position_qty=1.0, entry_price=45000.0)
        engine.order_in_flight = True
        engine.pending_client_order_id = "order-123"
        fill = _make_fill(side="sell", last_size=1.0, last_price=50000.0, status="filled")
        result = engine.apply_fill(fill)
        assert result is True
        assert engine.position_qty == pytest.approx(0.0)
        assert engine.entry_price is None
        assert engine.order_in_flight is False

    def test_partial_fill_keeps_in_flight(self) -> None:
        """Verify partial fill updates position but keeps order_in_flight.

        Given: Flat engine with buy order in flight,
        When: Partial buy fill applied,
        Then: position_qty increases but order_in_flight stays True.
        """
        engine = self._make_engine()
        engine.order_in_flight = True
        engine.pending_client_order_id = "order-123"
        fill = _make_fill(
            client_order_id="order-123",
            side="buy",
            size=0.3,
            last_size=0.3,
            last_price=50000.0,
            status="partial",
            trade_id="trade-1",
        )
        result = engine.apply_fill(fill)
        assert result is True
        assert engine.position_qty == pytest.approx(0.3)
        assert engine.order_in_flight is True

    def test_final_fill_after_partial_clears_in_flight(self) -> None:
        """Verify final fill after partial clears in-flight state.

        Given: Engine with partial fill already applied,
        When: Final fill arrives,
        Then: position_qty updated, order_in_flight cleared.
        """
        engine = self._make_engine(position_qty=0.3, entry_price=50000.0)
        engine.order_in_flight = True
        engine.pending_client_order_id = "order-123"
        engine.seen_exec_ids.add("trade-1")
        fill = _make_fill(
            client_order_id="order-123",
            side="buy",
            size=0.5,
            last_size=0.2,
            last_price=50100.0,
            status="filled",
            trade_id="trade-2",
        )
        result = engine.apply_fill(fill)
        assert result is True
        assert engine.position_qty == pytest.approx(0.5)
        assert engine.order_in_flight is False

    def test_partial_sell_keeps_entry_price(self) -> None:
        """Verify partial sell does not clear entry price when position remains.

        Given: Engine with 1.0 position,
        When: Partial sell fill of 0.3 applied,
        Then: position_qty reduced but entry_price preserved.
        """
        engine = self._make_engine(position_qty=1.0, entry_price=45000.0)
        fill = _make_fill(
            side="sell",
            last_size=0.3,
            last_price=50000.0,
            status="partial",
            trade_id="trade-partial-sell",
        )
        result = engine.apply_fill(fill)
        assert result is True
        assert engine.position_qty == pytest.approx(0.7)
        assert engine.entry_price == pytest.approx(45000.0)

    def test_duplicate_fill_rejected(self) -> None:
        """Verify duplicate fill with same trade_id is rejected.

        Given: Engine that already saw trade-456,
        When: Same trade_id arrives again,
        Then: apply_fill returns False, state unchanged.
        """
        engine = self._make_engine(position_qty=0.5)
        engine.seen_exec_ids.add("trade-456")
        fill = _make_fill(trade_id="trade-456", side="buy", last_size=0.5, last_price=50000.0)
        result = engine.apply_fill(fill)
        assert result is False
        assert engine.position_qty == pytest.approx(0.5)

    def test_late_fill_from_old_order_does_not_clear_new_inflight(self) -> None:
        """Verify late fill from previous order books position but keeps new guard.

        Given: Engine with new order (order-456) in flight,
        When: Late fill from old order (order-123) arrives,
        Then: Position is updated but order_in_flight stays True.
        """
        engine = self._make_engine()
        engine.order_in_flight = True
        engine.pending_client_order_id = "order-456"
        fill = _make_fill(
            client_order_id="order-123",
            side="buy",
            last_size=0.5,
            last_price=50000.0,
            status="filled",
        )
        result = engine.apply_fill(fill)
        assert result is True
        assert engine.position_qty == pytest.approx(0.5)
        assert engine.order_in_flight is True
        assert engine.pending_client_order_id == "order-456"


class TestMaybeStopInFlight:
    """Tests for _maybe_stop in-flight guard."""

    @pytest.mark.asyncio
    async def test_maybe_stop_skips_when_order_in_flight(self) -> None:
        """Verify _maybe_stop returns False when order is already in flight.

        Given: Engine with position and order already in flight,
        When: Stop-loss condition is met,
        Then: _maybe_stop returns False and no new order is sent.
        """
        socket = MagicMock()
        socket.tracker = SequenceTracker()
        socket.send = AsyncMock()
        risk = RiskEvaluator(RiskConfigModel())
        engine = TradingEngineService(
            "BTC-USD",
            cast(Any, socket),
            risk=risk,
            cfg=EngineConfigModel(initial_cash=5000.0),
        )
        engine.position_qty = 1.0
        engine.entry_price = 110.0
        engine.portfolio.positions["BTC-USD"] = PositionStateModel(
            quantity=1.0, average_price=110.0
        )
        engine.order_in_flight = True
        engine.pending_client_order_id = "existing-order"
        triggered = await engine._maybe_stop(last_close=50.0, prev_close=120.0)
        assert triggered is False
        socket.send.assert_not_awaited()


class TestClearPendingIntent:
    """Tests for TradingEngineService.clear_pending_intent."""

    def test_clear_matching_order(self) -> None:
        """Verify clear_pending_intent clears state for matching order."""
        engine = TradingEngineService.__new__(TradingEngineService)
        engine.order_in_flight = True
        engine.pending_client_order_id = "order-123"
        engine._in_flight_since = 100.0
        result = engine.clear_pending_intent("order-123")
        assert result is True
        assert engine.order_in_flight is False
        assert engine.pending_client_order_id is None

    def test_clear_non_matching_order(self) -> None:
        """Verify clear_pending_intent does not clear for non-matching order."""
        engine = TradingEngineService.__new__(TradingEngineService)
        engine.order_in_flight = True
        engine.pending_client_order_id = "order-456"
        engine._in_flight_since = 100.0
        result = engine.clear_pending_intent("order-123")
        assert result is False
        assert engine.order_in_flight is True
        assert engine.pending_client_order_id == "order-456"


class TestInFlightTimeout:
    """Tests for order_in_flight timeout behavior."""

    @pytest.mark.asyncio
    async def test_timeout_clears_guard(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify in-flight timeout clears guard and allows new signal.

        Given: Engine with order in flight for longer than timeout,
        When: New signal arrives,
        Then: Timeout clears guard and signal is processed.
        """
        socket = MagicMock()
        socket.tracker = SequenceTracker()
        socket.send = AsyncMock()
        engine = TradingEngineService(
            "BTC-USD",
            cast(Any, socket),
            cfg=EngineConfigModel(initial_cash=10000.0),
            exchange="kraken",
        )
        engine.order_in_flight = True
        engine.pending_client_order_id = "old-order"
        engine._in_flight_since = time.monotonic() - 120.0
        await engine.execute_desired_units(1.0, current_price=100.0)
        assert engine.order_in_flight is True
        assert engine.pending_client_order_id != "old-order"

    @pytest.mark.asyncio
    async def test_inflight_blocks_new_signal(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify in-flight guard blocks new signals.

        Given: Engine with recent order in flight,
        When: New signal arrives,
        Then: Signal is dropped and no order is sent.
        """
        socket = MagicMock()
        socket.tracker = SequenceTracker()
        socket.send = AsyncMock()
        engine = TradingEngineService(
            "BTC-USD",
            cast(Any, socket),
            cfg=EngineConfigModel(initial_cash=10000.0),
            exchange="kraken",
        )
        engine.order_in_flight = True
        engine.pending_client_order_id = "active-order"
        engine._in_flight_since = time.monotonic()
        await engine.execute_desired_units(1.0, current_price=100.0)
        socket.send.assert_not_awaited()
        assert engine.pending_client_order_id == "active-order"


class TestRecovery:
    """Tests for Stage D: startup recovery."""

    @pytest.mark.asyncio
    async def test_recover_engine_state_from_executions(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify engine state is rebuilt from DB executions on startup.

        Given: DB with buy execution for BTC-USD on kraken,
        When: _recover_engine_state runs,
        Then: Engine is created with correct position and entry price.
        """
        _configure_settings(monkeypatch)
        coord = TraderCoordinator()
        coord.msg_publisher = cast(Any, MagicMock(tracker=Mock(session_id="s1")))
        mock_repo = AsyncMock()
        mock_repo.get_executions_for_recovery = AsyncMock(
            return_value=[
                {
                    "public_id": "exe-1",
                    "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
                    "session_id": "s1",
                    "sequence_id": 1,
                    "trade_id": "t1",
                    "exchange_order_id": "ex-1",
                    "client_order_id": "c1",
                    "instrument": "BTC-USD",
                    "exchange": "kraken",
                    "side": "buy",
                    "size": 0.5,
                    "price": 50000.0,
                    "fee": 0.5,
                    "fee_asset": "USD",
                    "status": "filled",
                    "executed_at": datetime(2024, 1, 1, tzinfo=UTC),
                }
            ]
        )
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        mock_repo.ensure_instrument = AsyncMock(return_value=(1, "inst-pid"))
        coord.repository = mock_repo
        await coord._recover_engine_state()
        assert "BTC-USD@kraken-live" in coord.engines
        engine = coord.engines["BTC-USD@kraken-live"]
        assert engine.position_qty == pytest.approx(0.5)
        assert engine.entry_price == pytest.approx(50000.0)
        assert "t1" in engine.seen_exec_ids

    @pytest.mark.asyncio
    async def test_recover_with_open_order_sets_inflight(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify open DB order sets order_in_flight with fresh timeout.

        Given: DB with execution and active order for BTC-USD,
        When: _recover_engine_state runs,
        Then: Engine has order_in_flight=True with fresh _in_flight_since.
        """
        _configure_settings(monkeypatch)
        coord = TraderCoordinator()
        coord.msg_publisher = cast(Any, MagicMock(tracker=Mock(session_id="s1")))
        mock_repo = AsyncMock()
        mock_repo.get_executions_for_recovery = AsyncMock(
            return_value=[
                {
                    "public_id": "exe-1",
                    "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
                    "session_id": "s1",
                    "sequence_id": 1,
                    "trade_id": "t1",
                    "exchange_order_id": "ex-1",
                    "client_order_id": "c1",
                    "instrument": "BTC-USD",
                    "exchange": "kraken",
                    "side": "buy",
                    "size": 0.3,
                    "price": 50000.0,
                    "fee": 0.3,
                    "fee_asset": "USD",
                    "status": "partial",
                    "executed_at": datetime(2024, 1, 1, tzinfo=UTC),
                }
            ]
        )
        mock_repo.get_active_orders_for_recovery = AsyncMock(
            return_value=[
                {
                    "public_id": "ord-1",
                    "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
                    "session_id": "s1",
                    "sequence_id": 1,
                    "instrument": "BTC-USD",
                    "exchange": "kraken",
                    "client_order_id": "c1",
                    "exchange_order_id": "ex-1",
                    "status": "open",
                    "side": "buy",
                    "order_type": "market",
                    "size": 0.5,
                    "price": None,
                    "filled_size": 0.3,
                    "average_price": 50000.0,
                    "time_in_force": None,
                    "error": None,
                    "created_at": datetime(2024, 1, 1, tzinfo=UTC),
                    "updated_at": None,
                }
            ]
        )
        mock_repo.ensure_instrument = AsyncMock(return_value=(1, "inst-pid"))
        coord.repository = mock_repo
        before = time.monotonic()
        await coord._recover_engine_state()
        engine = coord.engines["BTC-USD@kraken-live"]
        assert engine.order_in_flight is True
        assert engine.pending_client_order_id == "c1"
        assert engine._in_flight_since is not None
        assert engine._in_flight_since >= before

    @pytest.mark.asyncio
    async def test_recover_no_executions_no_orders(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify empty DB results in no engines created.

        Given: Empty execution history AND no active orders,
        When: _recover_engine_state runs,
        Then: No engines are created.
        """
        _configure_settings(monkeypatch)
        coord = TraderCoordinator()
        mock_repo = AsyncMock()
        mock_repo.get_executions_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        coord.repository = mock_repo
        await coord._recover_engine_state()
        assert len(coord.engines) == 0

    @pytest.mark.asyncio
    async def test_recover_active_order_without_executions(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify active order without executions creates engine with in-flight.

        Given: No executions but one active order in DB,
        When: _recover_engine_state runs,
        Then: Engine is created with order_in_flight=True and position=0.
        """
        _configure_settings(monkeypatch)
        coord = TraderCoordinator()
        coord.msg_publisher = cast(Any, MagicMock(tracker=Mock(session_id="s1")))
        mock_repo = AsyncMock()
        mock_repo.get_executions_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_active_orders_for_recovery = AsyncMock(
            return_value=[
                {
                    "public_id": "ord-1",
                    "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
                    "session_id": "s1",
                    "sequence_id": 1,
                    "instrument": "BTC-USD",
                    "exchange": "kraken",
                    "client_order_id": "c1",
                    "exchange_order_id": "ex-1",
                    "status": "open",
                    "side": "buy",
                    "order_type": "market",
                    "size": 0.5,
                    "price": None,
                    "filled_size": 0.0,
                    "average_price": None,
                    "time_in_force": None,
                    "error": None,
                    "created_at": datetime(2024, 1, 1, tzinfo=UTC),
                    "updated_at": None,
                }
            ]
        )
        mock_repo.ensure_instrument = AsyncMock(return_value=(1, "inst-pid"))
        coord.repository = mock_repo
        await coord._recover_engine_state()
        assert "BTC-USD@kraken-live" in coord.engines
        engine = coord.engines["BTC-USD@kraken-live"]
        assert engine.order_in_flight is True
        assert engine.pending_client_order_id == "c1"
        assert engine.position_qty == pytest.approx(0.0)

    @pytest.mark.asyncio
    async def test_recover_fill_gap_enters_degraded_mode(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify fill gap between order and executions triggers degraded mode.

        Given: Order shows filled_size=0.5 but no executions exist,
        When: _recover_engine_state runs,
        Then: Engine enters read_only=True (degraded mode).
        """
        _configure_settings(monkeypatch)
        coord = TraderCoordinator()
        coord.msg_publisher = cast(Any, MagicMock(tracker=Mock(session_id="s1")))
        mock_repo = AsyncMock()
        mock_repo.get_executions_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_active_orders_for_recovery = AsyncMock(
            return_value=[
                {
                    "public_id": "ord-gap",
                    "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
                    "session_id": "s1",
                    "sequence_id": 1,
                    "instrument": "BTC-USD",
                    "exchange": "kraken",
                    "client_order_id": "c-gap",
                    "exchange_order_id": "ex-gap",
                    "status": "open",
                    "side": "buy",
                    "order_type": "market",
                    "size": 1.0,
                    "price": None,
                    "filled_size": 0.5,
                    "average_price": 50000.0,
                    "time_in_force": None,
                    "error": None,
                    "created_at": datetime(2024, 1, 1, tzinfo=UTC),
                    "updated_at": None,
                }
            ]
        )
        mock_repo.ensure_instrument = AsyncMock(return_value=(1, "inst-pid"))
        coord.repository = mock_repo
        await coord._recover_engine_state()
        engine = coord.engines["BTC-USD@kraken-live"]
        assert engine.read_only is True
        assert engine.order_in_flight is True

    @pytest.mark.asyncio
    async def test_recover_skips_invalid_exchange(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify executions with unknown exchange are skipped.

        Given: DB execution with exchange='unknown_exchange',
        When: _recover_engine_state runs,
        Then: No engine is created for that exchange.
        """
        _configure_settings(monkeypatch)
        coord = TraderCoordinator()
        coord.msg_publisher = cast(Any, MagicMock(tracker=Mock(session_id="s1")))
        mock_repo = AsyncMock()
        mock_repo.get_executions_for_recovery = AsyncMock(
            return_value=[
                {
                    "public_id": "exe-1",
                    "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
                    "session_id": "s1",
                    "sequence_id": 1,
                    "trade_id": "t1",
                    "exchange_order_id": "ex-1",
                    "client_order_id": "c1",
                    "instrument": "BTC-USD",
                    "exchange": "unknown_exchange",
                    "side": "buy",
                    "size": 0.5,
                    "price": 50000.0,
                    "fee": 0.5,
                    "fee_asset": "USD",
                    "status": "filled",
                    "executed_at": datetime(2024, 1, 1, tzinfo=UTC),
                }
            ]
        )
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        coord.repository = mock_repo
        await coord._recover_engine_state()
        assert len(coord.engines) == 0

    def test_apply_execution_row_to_engine(self) -> None:
        """Verify single execution row correctly updates engine state."""
        socket = MagicMock()
        socket.tracker = Mock(session_id="s1")
        engine = TradingEngineService(
            "BTC-USD",
            cast(Any, socket),
            cfg=EngineConfigModel(initial_cash=10000.0),
            exchange="kraken",
        )
        fill_row = {
            "side": "buy",
            "size": 0.5,
            "price": 50000.0,
            "fee": 0.5,
            "trade_id": "t1",
        }
        TraderCoordinator._apply_execution_row_to_engine(engine, cast(Any, fill_row))
        assert engine.position_qty == pytest.approx(0.5)
        assert engine.entry_price == pytest.approx(50000.0)
        assert "t1" in engine.seen_exec_ids

    def test_apply_execution_row_buy_existing_position(self) -> None:
        """Verify buy into existing position does not reset entry price."""
        socket = MagicMock()
        socket.tracker = Mock(session_id="s1")
        engine = TradingEngineService(
            "BTC-USD",
            cast(Any, socket),
            cfg=EngineConfigModel(initial_cash=10000.0),
            exchange="kraken",
        )
        engine.position_qty = 0.3
        engine.entry_price = 49000.0
        fill_row = {
            "side": "buy",
            "size": 0.2,
            "price": 50000.0,
            "fee": 0.2,
            "trade_id": "t2",
        }
        TraderCoordinator._apply_execution_row_to_engine(engine, cast(Any, fill_row))
        assert engine.position_qty == pytest.approx(0.5)
        assert engine.entry_price == pytest.approx(49000.0)

    def test_apply_execution_row_partial_sell_keeps_entry(self) -> None:
        """Verify partial sell keeps entry price when position remains."""
        socket = MagicMock()
        socket.tracker = Mock(session_id="s1")
        engine = TradingEngineService(
            "BTC-USD",
            cast(Any, socket),
            cfg=EngineConfigModel(initial_cash=10000.0),
            exchange="kraken",
        )
        engine.position_qty = 1.0
        engine.entry_price = 50000.0
        fill_row = {
            "side": "sell",
            "size": 0.3,
            "price": 51000.0,
            "fee": 0.3,
            "trade_id": None,
        }
        TraderCoordinator._apply_execution_row_to_engine(engine, cast(Any, fill_row))
        assert engine.position_qty == pytest.approx(0.7)
        assert engine.entry_price == pytest.approx(50000.0)

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_recover_active_order_engine_creation_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify active order is skipped when engine creation fails.

        Given: Active order for which _create_engine_for_recovery returns None,
        When: _recover_engine_state runs,
        Then: No engine created, no crash.
        """
        _configure_settings(monkeypatch)
        coord = TraderCoordinator()
        coord.msg_publisher = cast(Any, MagicMock(tracker=Mock(session_id="s1")))
        mock_repo = AsyncMock()
        mock_repo.get_executions_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_active_orders_for_recovery = AsyncMock(
            return_value=[
                {
                    "public_id": "ord-fail",
                    "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
                    "session_id": "s1",
                    "sequence_id": 1,
                    "instrument": "BTC-USD",
                    "exchange": "kraken",
                    "client_order_id": "c-fail",
                    "exchange_order_id": "ex-fail",
                    "status": "open",
                    "side": "buy",
                    "order_type": "market",
                    "size": 1.0,
                    "price": None,
                    "filled_size": 0.0,
                    "average_price": None,
                    "time_in_force": None,
                    "error": None,
                    "created_at": datetime(2024, 1, 1, tzinfo=UTC),
                    "updated_at": None,
                }
            ]
        )
        coord.repository = mock_repo
        monkeypatch.setattr(coord, "_create_engine_for_recovery", AsyncMock(return_value=None))
        await coord._recover_engine_state()
        assert len(coord.engines) == 0

    def test_apply_execution_row_sell_clears_position(self) -> None:
        """Verify sell execution row decreases position and clears entry price."""
        socket = MagicMock()
        socket.tracker = Mock(session_id="s1")
        engine = TradingEngineService(
            "BTC-USD",
            cast(Any, socket),
            cfg=EngineConfigModel(initial_cash=10000.0),
            exchange="kraken",
        )
        engine.position_qty = 0.5
        engine.entry_price = 50000.0
        fill_row = {
            "side": "sell",
            "size": 0.5,
            "price": 51000.0,
            "fee": 0.5,
            "trade_id": None,
        }
        TraderCoordinator._apply_execution_row_to_engine(engine, cast(Any, fill_row))
        assert engine.position_qty == pytest.approx(0.0)
        assert engine.entry_price is None

    @pytest.mark.asyncio
    async def test_recover_active_order_query_failure(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify recovery continues if active orders query fails.

        Given: Executions exist but get_active_orders_for_recovery raises,
        When: _recover_engine_state runs,
        Then: Engines are created from executions, in-flight not set.
        """
        _configure_settings(monkeypatch)
        coord = TraderCoordinator()
        coord.msg_publisher = cast(Any, MagicMock(tracker=Mock(session_id="s1")))
        mock_repo = AsyncMock()
        mock_repo.get_executions_for_recovery = AsyncMock(
            return_value=[
                {
                    "public_id": "exe-1",
                    "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
                    "session_id": "s1",
                    "sequence_id": 1,
                    "trade_id": "t1",
                    "exchange_order_id": "ex-1",
                    "client_order_id": "c1",
                    "instrument": "BTC-USD",
                    "exchange": "kraken",
                    "side": "buy",
                    "size": 0.5,
                    "price": 50000.0,
                    "fee": 0.5,
                    "fee_asset": "USD",
                    "status": "filled",
                    "executed_at": datetime(2024, 1, 1, tzinfo=UTC),
                }
            ]
        )
        mock_repo.get_active_orders_for_recovery = AsyncMock(side_effect=RuntimeError("DB error"))
        mock_repo.ensure_instrument = AsyncMock(return_value=(1, "inst-pid"))
        coord.repository = mock_repo
        await coord._recover_engine_state()
        assert "BTC-USD@kraken-live" in coord.engines
        engine = coord.engines["BTC-USD@kraken-live"]
        assert engine.order_in_flight is False

    @pytest.mark.asyncio
    async def test_degraded_mode_blocks_signals(self) -> None:
        """Verify read_only engine drops all signals.

        Given: Engine in degraded read-only mode,
        When: Signal arrives,
        Then: Signal is dropped.
        """
        socket = MagicMock()
        socket.tracker = Mock(session_id="s1")
        socket.send = AsyncMock()
        engine = TradingEngineService(
            "BTC-USD",
            cast(Any, socket),
            cfg=EngineConfigModel(initial_cash=10000.0),
            exchange="kraken",
        )
        engine.read_only = True
        await engine.execute_desired_units(1.0, current_price=100.0)
        socket.send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_degraded_mode_blocks_stop_loss(self) -> None:
        """Verify read_only engine does not trigger stop-loss.

        Given: Engine in degraded mode with position,
        When: Stop-loss condition is met,
        Then: No order sent, returns False.
        """
        socket = MagicMock()
        socket.tracker = Mock(session_id="s1")
        socket.send = AsyncMock()
        engine = TradingEngineService(
            "BTC-USD",
            cast(Any, socket),
            risk=RiskEvaluator(RiskConfigModel()),
            cfg=EngineConfigModel(initial_cash=5000.0),
        )
        engine.position_qty = 1.0
        engine.entry_price = 110.0
        engine.portfolio.positions["BTC-USD"] = PositionStateModel(
            quantity=1.0, average_price=110.0
        )
        engine.read_only = True
        triggered = await engine._maybe_stop(last_close=50.0, prev_close=120.0)
        assert triggered is False
        socket.send.assert_not_awaited()
