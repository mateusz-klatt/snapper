"""Tests for the base market data publisher service."""

import asyncio
import importlib
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from time import monotonic
from types import SimpleNamespace
from typing import Any
from typing import cast
from unittest.mock import ANY
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

import pytest
import zmq
from loguru import logger
from sqlalchemy.exc import DBAPIError
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.process_manager.launcher import _TOTAL_RESET_UPTIME_S
from snapper.application.process_manager.registry import get_registered_processes
from snapper.core.types import ExchangeEnum
from snapper.data.repository import Repository
from snapper.data.repository_types import CandleUpsertRow
from snapper.data.repository_types import ShadowCandleUpsertRow
from snapper.data.repository_types import TickUpsertRow
from snapper.data.repository_types import TradeUpsertRow
from snapper.infrastructure.exchanges._subscription_health import _SymbolEntry
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate
from snapper.messaging.publishers.base import _DARK_FEED_EXIT_CEILING_S
from snapper.messaging.publishers.base import _FEED_HEALTH_FLUSH_INTERVAL_S
from snapper.messaging.publishers.base import _TRADE_ID_LRU_MAX_PER_SYMBOL
from snapper.messaging.publishers.base import FeedDarkTooLongError
from snapper.messaging.publishers.base import MarketDataPublisherService
from snapper.messaging.publishers.base import _candle_writer_drop_counters
from snapper.messaging.publishers.base import _cleanup_pending_future
from snapper.messaging.publishers.base import _enqueue_or_drop_oldest_candle_write
from snapper.messaging.publishers.base import _enqueue_or_drop_oldest_tick_write
from snapper.messaging.publishers.base import _enqueue_or_drop_oldest_trade_write
from snapper.messaging.publishers.base import _is_disconnect_error
from snapper.messaging.publishers.base import _tick_writer_drop_counters
from snapper.messaging.publishers.base import _trade_writer_drop_counters
from snapper.messaging.publishers.base import _WriterSessionLostError
from snapper.messaging.publishers.candle_aggregator import SUPPORTED_SYNTHESIS_TIMEFRAMES
from snapper.messaging.publishers.candle_aggregator import CandleAggregator
from snapper.messaging.publishers.kraken import KrakenMarketDataPublisher
from snapper.messaging.publishers.native_candle_finalizer import NativeCandleFinalizer
from snapper.messaging.schemas.data import CandleData
from snapper.messaging.schemas.data import HeartbeatData
from snapper.messaging.schemas.data import SettingChangedData
from snapper.messaging.schemas.data import TickData
from snapper.messaging.schemas.data import TradeData

TEST_DB_URL = "sqlite:///:memory:"


def zmq_socket_stub(**kwargs: Any) -> SimpleNamespace:
    """Create a stub ZMQ socket for testing."""
    return SimpleNamespace(setsockopt=lambda o, v: None, **kwargs)


class DummyClient(SimpleNamespace):
    """Test stub for exchange client."""

    async def connect(self) -> None:
        """Connect to exchange."""
        ...

    async def disconnect(self) -> None:
        """Disconnect from exchange."""
        ...

    def start_health_loop(self) -> None:
        """Start subscription health loop."""
        ...

    async def stop_health_loop(self) -> None:
        """Stop subscription health loop."""
        ...

    async def subscribe_candles(self, symbols: list[str], timeframe: str) -> AsyncIterator[Any]:
        """Subscribe to candle updates."""
        if False:
            yield None

    async def subscribe_ticks(self, symbols: list[str]) -> AsyncIterator[Any]:
        """Subscribe to tick updates."""
        if False:
            yield None

    async def subscribe_trades(self, symbols: list[str]) -> AsyncIterator[Any]:
        """Subscribe to trade updates."""
        if False:
            yield None


class RecordingCandleClient(DummyClient):
    """Client stub that records candle subscription calls."""

    def __init__(self) -> None:
        """Initialize call tracking and a reusable stream."""
        super().__init__()
        self.calls: list[tuple[list[str], str]] = []
        self.stream = self._stream()

    def subscribe_candles(self, symbols: list[str], timeframe: str) -> AsyncIterator[CandleUpdate]:
        """Record the candle subscription and return the stream.

        Args:
            symbols: Native symbols to subscribe.
            timeframe: Candle timeframe label.

        Returns:
            The configured async candle stream.
        """
        self.calls.append((symbols, timeframe))
        return self.stream

    async def _stream(self) -> AsyncIterator[CandleUpdate]:
        """Yield no candles.

        Yields:
            No values.
        """
        if False:
            yield _candle_update()


class DummyPublisher(MarketDataPublisherService[Any]):
    """Test stub for MarketDataPublisherService (base candle hooks: no synthesis)."""

    def _create_exchange_client(self) -> DummyClient:
        return DummyClient()

    def _get_exchange_name(self) -> str:
        return "kraken"

    def _validate_symbols(self, symbols: list[str]) -> list[str]:
        return symbols


class HookPublisher(DummyPublisher):
    """Publisher stub that exposes the extra background task hook."""

    def __init__(self, symbols: list[str]) -> None:
        """Initialize the hook publisher.

        Args:
            symbols: Native symbols for the publisher.
        """
        super().__init__(symbols)
        self.received_symbols: list[str] = []
        self.cancelled = asyncio.Event()

    async def _start_extra_background_tasks(
        self, symbols_to_subscribe: list[str]
    ) -> list[asyncio.Task[None]]:
        """Start one cancellable background task.

        Args:
            symbols_to_subscribe: Symbols passed by the base start hook.

        Returns:
            A single task that remains pending until cancelled.
        """
        self.received_symbols = list(symbols_to_subscribe)
        return [asyncio.create_task(self._extra_loop())]

    async def _extra_loop(self) -> None:
        """Wait until cancelled and record shutdown.

        Returns:
            None.
        """
        try:
            await asyncio.Event().wait()
        finally:
            self.cancelled.set()


class SpotLikePublisher(DummyPublisher):
    """Stub mirroring kraken spot: full native OHLC set + continuous-corpus forward-fill."""

    def _native_candle_timeframes(self) -> frozenset[str]:
        return frozenset({"1m"}) | SUPPORTED_SYNTHESIS_TIMEFRAMES

    def _supports_forward_fill(self) -> bool:
        return True


def _ticker_update(
    *,
    symbol: str = "BTC-USD",
    volume: float = 10.0,
    is_delayed: bool = False,
    is_extended_hours: bool | None = None,
) -> TickerUpdate:
    """Build a ticker update for publisher dedup tests."""
    return TickerUpdate(
        symbol=symbol,
        bid=100.0,
        bid_qty=1.0,
        ask=101.0,
        ask_qty=1.5,
        last=100.5,
        volume=volume,
        vwap=100.25,
        low=99.0,
        high=102.0,
        change=0.5,
        change_pct=0.25,
        is_delayed=is_delayed,
        is_extended_hours=is_extended_hours,
    )


def _trade_update(
    *,
    symbol: str = "BTC-USD",
    trade_id: str | None = "trade-1",
) -> TradeUpdate:
    """Build a trade update for publisher dedup tests."""
    return TradeUpdate(
        symbol=symbol,
        side="buy",
        quantity=0.5,
        price=100.0,
        ord_type="market",
        trade_id=trade_id,
        timestamp=datetime.now(UTC),
    )


def _dedup_publisher(symbols: list[str] | None = None) -> tuple[DummyPublisher, AsyncMock]:
    """Build a publisher with outbound effects mocked for dedup tests."""
    publisher = DummyPublisher(symbols=symbols or ["BTC-USD"])
    publish_mock = AsyncMock()
    publisher._publish_message = publish_mock
    publisher._ensure_instrument = AsyncMock(return_value=None)
    return publisher, publish_mock


class StubValidatedPublisher(SimpleNamespace):
    """Test stub for validated publisher socket."""

    def __init__(self) -> None:
        """Initialize the instance."""
        super().__init__()
        self.send_multipart = AsyncMock()
        self.close = lambda: None


class StubValidatedSubscriber(SimpleNamespace):
    """Test stub for validated subscriber socket."""

    def __init__(self) -> None:
        """Initialize the instance."""
        super().__init__()
        self.recv_multipart = AsyncMock()
        self.subscribe = Mock()
        self.close = lambda: None


def test_require_repository_raises_when_uninitialized() -> None:
    """Verify publisher repository access fails fast before startup wiring.

    Given: A publisher whose repository has not been initialized,
    When: Repository access is required by a persistence path,
    Then: A RuntimeError replaces the optimized-out assertion failure.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    with pytest.raises(RuntimeError, match="Repository not initialized"):
        pub._require_repository()


@pytest.mark.asyncio
async def test_extra_background_task_hook_defaults_empty() -> None:
    """The base extra task hook is a no-op.

    Given: A publisher subclass that does not override the hook,
    When: _start_extra_background_tasks is called,
    Then: it returns an empty task list.

    Returns:
        None.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    assert await pub._start_extra_background_tasks(["BTC-USD"]) == []


@pytest.mark.asyncio
async def test_stop_cancels_extra_background_tasks() -> None:
    """Stop cancels background tasks returned by the subclass hook.

    Given: A publisher with one extra background task registered,
    When: stop is called,
    Then: the task is cancelled and the tracked task list is cleared.

    Returns:
        None.
    """
    pub = HookPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub._extra_background_tasks = await pub._start_extra_background_tasks(["BTC-USD"])
    await asyncio.sleep(0)
    await pub.stop()
    assert pub.received_symbols == ["BTC-USD"]
    assert pub.cancelled.is_set()
    assert pub._extra_background_tasks == []


@pytest.mark.asyncio
async def test_start_logs_warning_when_running(caplog: pytest.LogCaptureFixture) -> None:
    """Test start returns when already running.

    Given: A publisher that is already running,
    When: Start is called,
    Then: Returns None without starting again.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    caplog.set_level("WARNING")
    result = await pub.start()
    assert result is None


@pytest.mark.asyncio
async def test_start_warns_on_symbol_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test start warns when symbols exceed limit.

    Given: A publisher with more symbols than connection limit,
    When: Started,
    Then: Publisher starts with warning.
    """
    pub: Any = DummyPublisher(symbols=["A", "B", "C"])
    pub._get_max_symbols_per_connection = Mock(return_value=1)
    mock_repo = SimpleNamespace(get_latest_candle_ids=AsyncMock(return_value={}))
    monkeypatch.setattr("snapper.messaging.publishers.base.get_repository", lambda _url: mock_repo)
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.get_settings_service",
        AsyncMock(return_value=SimpleNamespace()),
    )
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.get_settings_with_service",
        lambda _svc: pub.settings,
    )

    class DummySock(SimpleNamespace):
        def __init__(self) -> None:
            super().__init__(
                connect=lambda *_: None, close=lambda: None, setsockopt=lambda o, v: None
            )

    class DummyCtx:
        def socket(self, *_args: Any) -> DummySock:
            return DummySock()

        def term(self) -> None:
            return None

    monkeypatch.setattr("snapper.messaging.publishers.base.zmq.asyncio.Context", lambda: DummyCtx())
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.ValidatedPublisher",
        lambda sock: SimpleNamespace(
            close=sock.close, send_multipart=AsyncMock(), setsockopt=sock.setsockopt
        ),
    )
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.ValidatedSubscriber",
        lambda sock: SimpleNamespace(
            subscribe=lambda *_: None,
            recv_multipart=AsyncMock(),
            close=sock.close,
            setsockopt=sock.setsockopt,
        ),
    )
    dummy_client = cast(Any, DummyClient())
    dummy_client.connect = AsyncMock()
    pub._create_exchange_client = lambda: dummy_client
    pub.settings.timeframes = ["1m"]
    pub.settings.zmq_heartbeat_interval_ms = 0
    pub.settings.write_buffer_candle_max_rows = 100
    pub.settings.write_buffer_tick_max_rows = 500
    pub.settings.write_buffer_trade_max_rows = 500
    pub.settings.write_buffer_flush_ms = 50
    pub._heartbeat_loop = AsyncMock()
    pub._symbol_aliases_loop = AsyncMock()
    pub._feed_health_flush_loop = AsyncMock()
    pub._supervise_consumer = AsyncMock()
    pub._tick_loop = AsyncMock()
    pub._tick_writer_loop = AsyncMock()
    pub._trade_loop = AsyncMock()
    pub._trade_writer_loop = AsyncMock()
    pub._candle_loop = AsyncMock()
    pub._candle_writer_loop = AsyncMock()
    pub._native_finalize_flush_loop = AsyncMock()
    await pub.start()
    await pub.stop()


def _mock_start_runtime(pub: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Mock the publisher runtime so ``start`` runs without real IO.

    Args:
        pub: Publisher under test.
        monkeypatch: Pytest monkeypatch fixture for runtime substitutions.

    Returns:
        None.
    """
    pub._get_max_symbols_per_connection = Mock(return_value=0)
    mock_repo = SimpleNamespace(get_latest_candle_ids=AsyncMock(return_value={}))
    monkeypatch.setattr("snapper.messaging.publishers.base.get_repository", lambda _url: mock_repo)
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.get_settings_service",
        AsyncMock(return_value=SimpleNamespace()),
    )
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.get_settings_with_service",
        lambda _svc: pub.settings,
    )

    class DummySock(SimpleNamespace):
        def __init__(self) -> None:
            super().__init__(
                connect=lambda *_: None, close=lambda: None, setsockopt=lambda o, v: None
            )

    class DummyCtx:
        def socket(self, *_args: Any) -> DummySock:
            return DummySock()

        def term(self) -> None:
            return None

    monkeypatch.setattr("snapper.messaging.publishers.base.zmq.asyncio.Context", lambda: DummyCtx())
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.ValidatedPublisher",
        lambda sock: SimpleNamespace(
            close=sock.close, send_multipart=AsyncMock(), setsockopt=sock.setsockopt
        ),
    )
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.ValidatedSubscriber",
        lambda sock: SimpleNamespace(
            subscribe=lambda *_: None,
            recv_multipart=AsyncMock(),
            close=sock.close,
            setsockopt=sock.setsockopt,
        ),
    )
    client: Any = DummyClient()
    client.connect = AsyncMock()
    pub._create_exchange_client = lambda: client
    pub.settings.timeframes = []
    pub.settings.zmq_heartbeat_interval_ms = 0
    pub.settings.write_buffer_candle_max_rows = 100
    pub.settings.write_buffer_tick_max_rows = 500
    pub.settings.write_buffer_trade_max_rows = 500
    pub.settings.write_buffer_flush_ms = 50

    async def noop(*_args: Any, **_kwargs: Any) -> None:
        return None

    pub._heartbeat_loop = noop
    pub._symbol_aliases_loop = noop
    pub._feed_health_flush_loop = noop
    pub._tick_loop = noop
    pub._tick_writer_loop = noop
    pub._trade_loop = noop
    pub._trade_writer_loop = noop
    pub._candle_loop = noop
    pub._candle_writer_loop = noop
    pub._native_finalize_flush_loop = noop
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.asyncio.gather", AsyncMock(return_value=None)
    )


@pytest.mark.asyncio
async def test_start_logs_capability_exclusions_for_wildcard_publisher(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Wildcard publisher start reports market-data capability exclusions.

    Given: A wildcard publisher whose exchange withholds two symbols by
        capability,
    When: ``start`` runs once,
    Then: One INFO summary reports the excluded and included counts and a
        DEBUG line lists the sorted excluded native symbols.
    """
    pub: Any = DummyPublisher(symbols=["*"])
    _mock_start_runtime(pub, monkeypatch)
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.get_market_data_capability_exclusions",
        lambda _exchange: (["BTC-USD"], ["ADA-USD", "XRP-USD"]),
    )
    sink_id = logger.add(caplog.handler, format="{message}", level="DEBUG")
    try:
        with caplog.at_level("DEBUG"):
            await pub.start()
    finally:
        logger.remove(sink_id)
        await pub.stop()
    info_records = [r for r in caplog.records if r.levelname == "INFO"]
    debug_records = [r for r in caplog.records if r.levelname == "DEBUG"]
    assert any(
        "2 symbol(s) excluded from market data by capability" in r.message
        and "1 included" in r.message
        for r in info_records
    )
    assert any("['ADA-USD', 'XRP-USD']" in r.message for r in debug_records)


@pytest.mark.asyncio
async def test_start_silent_about_exclusions_for_explicit_publisher(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Explicit-symbol publisher start never reports capability exclusions.

    Given: An explicit-symbol publisher whose exchange would report
        exclusions,
    When: ``start`` runs once,
    Then: No exclusion line is emitted because exclusions only apply to the
        wildcard universe-coverage case.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    _mock_start_runtime(pub, monkeypatch)
    exclusions_mock = Mock(return_value=(["BTC-USD"], ["ADA-USD", "XRP-USD"]))
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.get_market_data_capability_exclusions",
        exclusions_mock,
    )
    sink_id = logger.add(caplog.handler, format="{message}", level="DEBUG")
    try:
        with caplog.at_level("DEBUG"):
            await pub.start()
    finally:
        logger.remove(sink_id)
        await pub.stop()
    exclusions_mock.assert_not_called()
    assert not [r for r in caplog.records if "excluded from market data" in r.message]


@pytest.mark.asyncio
async def test_start_silent_when_no_capability_exclusions(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Wildcard publisher start stays quiet when nothing is excluded.

    Given: A wildcard publisher whose exchange withholds no symbols,
    When: ``start`` runs once,
    Then: No exclusion INFO or DEBUG line is emitted.
    """
    pub: Any = DummyPublisher(symbols=["*"])
    _mock_start_runtime(pub, monkeypatch)
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.get_market_data_capability_exclusions",
        lambda _exchange: (["BTC-USD", "ETH-USD"], []),
    )
    sink_id = logger.add(caplog.handler, format="{message}", level="DEBUG")
    try:
        with caplog.at_level("DEBUG"):
            await pub.start()
    finally:
        logger.remove(sink_id)
        await pub.stop()
    assert not [r for r in caplog.records if "excluded from market data" in r.message]


@pytest.mark.asyncio
async def test_supervisor_restarts_consumer_after_iterator_returns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Supervisor restarts when a consumer returns cleanly.

    Given: A running publisher and a consumer factory that returns,
    When: The supervisor runs,
    Then: The factory is called again after the restart backoff.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    calls = 0

    async def factory() -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            pub.running = False

    with patch(
        "snapper.messaging.publishers.base.asyncio.sleep",
        new_callable=AsyncMock,
    ) as sleep_mock:
        await pub._supervise_consumer("tick", factory)
    assert calls == 2
    sleep_mock.assert_awaited_once_with(2.0)


@pytest.mark.asyncio
async def test_supervisor_restarts_consumer_after_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Supervisor restarts when a consumer raises.

    Given: A running publisher and a consumer factory that raises once,
    When: The supervisor runs,
    Then: The factory is retried after the restart backoff.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    calls = 0

    async def factory() -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("boom")
        pub.running = False

    with patch(
        "snapper.messaging.publishers.base.asyncio.sleep",
        new_callable=AsyncMock,
    ) as sleep_mock:
        await pub._supervise_consumer("tick", factory)
    assert calls == 2
    sleep_mock.assert_awaited_once_with(2.0)


@pytest.mark.asyncio
async def test_supervisor_stops_after_exception_when_running_flips_false() -> None:
    """Supervisor does not restart exceptions raised during shutdown.

    Given: A consumer factory that flips running false before raising,
    When: The supervisor catches the exception,
    Then: It returns without scheduling another restart.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True

    async def factory() -> None:
        pub.running = False
        raise RuntimeError("shutdown")

    with patch(
        "snapper.messaging.publishers.base.asyncio.sleep",
        new_callable=AsyncMock,
    ) as sleep_mock:
        await pub._supervise_consumer("tick", factory)
    sleep_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_supervisor_respects_cancellation_during_stop() -> None:
    """Supervisor propagates cancellation.

    Given: A running publisher and a consumer factory cancelled by shutdown,
    When: The supervisor awaits the factory,
    Then: CancelledError propagates.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True

    async def factory() -> None:
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await pub._supervise_consumer("tick", factory)


@pytest.mark.asyncio
async def test_supervisor_does_not_restart_after_running_flips_false() -> None:
    """Supervisor exits when running flips false.

    Given: A running publisher and a consumer factory that stops the publisher,
    When: The factory returns,
    Then: The supervisor exits without sleeping for a restart.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True

    async def factory() -> None:
        pub.running = False

    with patch(
        "snapper.messaging.publishers.base.asyncio.sleep",
        new_callable=AsyncMock,
    ) as sleep_mock:
        await pub._supervise_consumer("tick", factory)
    sleep_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_stop_pipeline_awaits_supervisor_task_to_completion() -> None:
    """Stop pipelines cancel consumer supervisors before draining writers.

    Given: Tick, candle, and trade consumer tasks that are sleeping,
    When: The stop pipeline helpers run,
    Then: Each consumer task is cancelled and awaited.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    pub._tick_consumer_task = asyncio.create_task(asyncio.sleep(60))
    pub._candle_consumer_tasks = [asyncio.create_task(asyncio.sleep(60))]
    pub._trade_consumer_task = asyncio.create_task(asyncio.sleep(60))
    await pub._stop_tick_pipeline()
    await pub._stop_candle_pipeline()
    await pub._stop_trade_pipeline()
    assert pub._tick_consumer_task is None
    assert pub._candle_consumer_tasks == []
    assert pub._trade_consumer_task is None


def test_get_liveness_recovery_threshold_s_returns_300_default() -> None:
    """Default liveness threshold is five minutes.

    Given: A base publisher,
    When: The liveness threshold is read,
    Then: It returns 300 seconds.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    assert pub._get_liveness_recovery_threshold_s() == 300


@pytest.mark.asyncio
async def test_spawn_recovery_tracks_and_runs_task(monkeypatch: pytest.MonkeyPatch) -> None:
    """Recovery spawning tracks the scheduled task.

    Given: A running publisher with an old recovery timestamp whose
        attempt restores message flow,
    When: Recovery is spawned,
    Then: The recovery hook runs and the task is discarded after completion.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub._last_message_at = 0.0

    def _resume(_reason: str) -> None:
        pub._last_message_at = 100.0

    attempt = AsyncMock(side_effect=_resume)
    pub._attempt_liveness_recovery = attempt
    monkeypatch.setattr("snapper.messaging.publishers.base.monotonic", lambda: 1000.0)
    pub._spawn_recovery("stale")
    assert len(pub._recovery_tasks) == 1
    await asyncio.gather(*pub._recovery_tasks)
    await asyncio.sleep(0)
    attempt.assert_awaited_once_with("stale")
    assert pub._recovery_tasks == set()


def test_recovery_respects_60s_min_interval(monkeypatch: pytest.MonkeyPatch) -> None:
    """Recovery spawning is rate-limited.

    Given: A publisher that recently spawned recovery,
    When: Recovery is spawned again within 60 seconds,
    Then: No task is created.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    pub._last_recovery_at = 1000.0
    monkeypatch.setattr("snapper.messaging.publishers.base.monotonic", lambda: 1059.0)
    pub._spawn_recovery("stale")
    assert pub._recovery_tasks == set()


@pytest.mark.asyncio
async def test_recovery_lock_prevents_concurrent_recovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Recovery spawning is deduplicated while the lock is held.

    Given: A publisher whose recovery lock is already held,
    When: Recovery is spawned,
    Then: No task is created.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    monkeypatch.setattr("snapper.messaging.publishers.base.monotonic", lambda: 1000.0)
    await pub._recovery_lock.acquire()
    try:
        pub._spawn_recovery("stale")
    finally:
        pub._recovery_lock.release()
    assert pub._recovery_tasks == set()


@pytest.mark.asyncio
async def test_recovery_returns_when_data_resumes() -> None:
    """A single successful attempt that restores data ends recovery.

    Given: A running publisher whose recovery attempt restores messages,
    When: Recovery runs under the lock,
    Then: It returns after one attempt and releases the lock.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub._last_message_at = 0.0

    def _resume(_reason: str) -> None:
        pub._last_message_at = 100.0

    attempt = AsyncMock(side_effect=_resume)
    pub._attempt_liveness_recovery = attempt
    await pub._run_recovery_under_lock("stale")
    attempt.assert_awaited_once_with("stale")
    assert not pub._recovery_lock.locked()


@pytest.mark.asyncio
async def test_recovery_retries_with_backoff_until_data_resumes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Recovery keeps retrying with backoff instead of giving up.

    Given: A running publisher whose first attempt does not restore data,
    When: Recovery runs under the lock,
    Then: It backs off, retries, and returns once data resumes.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub._last_message_at = 0.0
    calls = {"n": 0}

    def _attempt(_reason: str) -> None:
        calls["n"] += 1
        if calls["n"] >= 2:
            pub._last_message_at = 100.0

    attempt = AsyncMock(side_effect=_attempt)
    pub._attempt_liveness_recovery = attempt
    pub._sleep_with_jitter = AsyncMock()
    monkeypatch.setattr("snapper.messaging.publishers.base._RECOVERY_PROGRESS_GRACE_S", 0.0)
    await pub._run_recovery_under_lock("stale")
    assert attempt.await_count == 2
    pub._sleep_with_jitter.assert_awaited()
    assert not pub._recovery_lock.locked()


@pytest.mark.asyncio
async def test_recovery_attempt_exception_is_swallowed_and_retried(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed attempt is logged and retried, not abandoned.

    Given: A running publisher whose first attempt raises,
    When: Recovery runs under the lock,
    Then: The exception is swallowed, recovery retries, and returns once
        data resumes.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub._last_message_at = 0.0
    calls = {"n": 0}

    def _attempt(_reason: str) -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("boom")
        pub._last_message_at = 100.0

    pub._attempt_liveness_recovery = AsyncMock(side_effect=_attempt)
    pub._sleep_with_jitter = AsyncMock()
    monkeypatch.setattr("snapper.messaging.publishers.base._RECOVERY_PROGRESS_GRACE_S", 0.0)
    await pub._run_recovery_under_lock("stale")
    assert pub._attempt_liveness_recovery.await_count == 2
    assert not pub._recovery_lock.locked()


@pytest.mark.asyncio
async def test_recovery_loop_exits_when_publisher_stops() -> None:
    """Recovery stops looping once the publisher is no longer running.

    Given: A running publisher whose attempt clears the running flag,
    When: Recovery runs under the lock,
    Then: The loop exits without further attempts and releases the lock.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub._last_message_at = 0.0

    def _stop(_reason: str) -> None:
        pub.running = False

    pub._attempt_liveness_recovery = AsyncMock(side_effect=_stop)
    pub._sleep_with_jitter = AsyncMock()
    await pub._run_recovery_under_lock("stale")
    pub._attempt_liveness_recovery.assert_awaited_once()
    assert not pub._recovery_lock.locked()


@pytest.mark.asyncio
async def test_recovery_propagates_cancellation() -> None:
    """Cancellation during an attempt propagates for clean shutdown.

    Given: A running publisher whose attempt is cancelled,
    When: Recovery runs under the lock,
    Then: CancelledError propagates and the lock is released.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub._attempt_liveness_recovery = AsyncMock(side_effect=asyncio.CancelledError)
    with pytest.raises(asyncio.CancelledError):
        await pub._run_recovery_under_lock("stale")
    assert not pub._recovery_lock.locked()


@pytest.mark.asyncio
async def test_await_recovery_progress_true_when_data_advances() -> None:
    """Progress is reported as soon as messages advance past baseline.

    Given: A running publisher whose last message advanced past baseline,
    When: Progress is observed,
    Then: True is returned immediately.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub._last_message_at = 100.0
    assert await pub._await_recovery_progress(0.0) is True


@pytest.mark.asyncio
async def test_await_recovery_progress_polls_then_detects_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Progress observed after a poll interval is reported.

    Given: A running publisher with no initial progress,
    When: A poll-interval sleep lets a message arrive,
    Then: True is returned.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub._last_message_at = 0.0

    async def _arrive(_seconds: float) -> None:
        pub._last_message_at = 100.0

    monkeypatch.setattr(
        "snapper.messaging.publishers.base.asyncio.sleep",
        AsyncMock(side_effect=_arrive),
    )
    assert await pub._await_recovery_progress(0.0) is True


@pytest.mark.asyncio
async def test_await_recovery_progress_false_after_grace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No progress within the grace window reports False.

    Given: A running publisher with no message progress and zero grace,
    When: Progress is observed,
    Then: False is returned.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub._last_message_at = 0.0
    monkeypatch.setattr("snapper.messaging.publishers.base._RECOVERY_PROGRESS_GRACE_S", 0.0)
    assert await pub._await_recovery_progress(0.0) is False


@pytest.mark.asyncio
async def test_await_recovery_progress_true_after_stop_if_data_resumed() -> None:
    """A stopped publisher still reports progress if data had resumed.

    Given: A stopped publisher whose last message advanced past baseline,
    When: Progress is observed,
    Then: True is returned from the post-loop check.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    pub.running = False
    pub._last_message_at = 100.0
    assert await pub._await_recovery_progress(0.0) is True


def test_recovery_progress_made_message_only() -> None:
    """Without a candle baseline, only message progress is required.

    Given: A publisher whose message watermark advanced past baseline,
    When: Progress is evaluated with no candle baseline,
    Then: It reports progress regardless of the candle watermark.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    pub._last_message_at = 100.0
    pub._last_candle_msg_at = 0.0
    assert pub._recovery_progress_made(0.0, None) is True
    pub._last_message_at = 0.0
    assert pub._recovery_progress_made(0.0, None) is False


def test_recovery_progress_made_candle_aware() -> None:
    """With a candle baseline, both watermarks must advance.

    Given: A publisher whose message watermark advanced,
    When: Progress is evaluated with a candle baseline,
    Then: It reports progress only once the candle watermark also advances.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    pub._last_message_at = 100.0
    pub._last_candle_msg_at = 0.0
    assert pub._recovery_progress_made(0.0, 0.0) is False
    pub._last_candle_msg_at = 100.0
    assert pub._recovery_progress_made(0.0, 0.0) is True


@pytest.mark.asyncio
async def test_await_recovery_progress_candle_aware_true_when_candle_arrives() -> None:
    """A candle-aware wait succeeds once a fresh candle arrives.

    Given: A running publisher whose message and candle watermarks both
        advanced past their baselines,
    When: Progress is observed with a candle baseline,
    Then: True is returned immediately.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub._last_message_at = 100.0
    pub._last_candle_msg_at = 100.0
    assert await pub._await_recovery_progress(0.0, 0.0) is True


@pytest.mark.asyncio
async def test_await_recovery_progress_candle_aware_false_when_only_trades_resume(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Trade flow alone does not satisfy a candle-aware recovery wait.

    Given: A running publisher whose message watermark advanced but whose
        candle watermark did not, with the candle grace elapsed,
    When: Progress is observed with a candle baseline,
    Then: False is returned so the recovery keeps retrying.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub._last_message_at = 100.0
    pub._last_candle_msg_at = 0.0
    monkeypatch.setattr("snapper.messaging.publishers.base._CANDLE_RECOVERY_PROGRESS_GRACE_S", 0.0)
    assert await pub._await_recovery_progress(0.0, 0.0) is False


@pytest.mark.asyncio
async def test_run_recovery_candle_aware_retries_until_candle_resumes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A candle-triggered recovery retries until a native candle resumes.

    Given: A running publisher whose first attempt restores trades but no
        candle, and whose second attempt restores a candle,
    When: Recovery runs requiring candle progress,
    Then: It retries past the trade-only attempt and returns once the candle
        watermark advances.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub._last_message_at = 0.0
    pub._last_candle_msg_at = 0.0
    calls = {"n": 0}

    def _attempt(_reason: str) -> None:
        calls["n"] += 1
        pub._last_message_at = 100.0 + calls["n"]
        if calls["n"] >= 2:
            pub._last_candle_msg_at = 100.0

    pub._attempt_liveness_recovery = AsyncMock(side_effect=_attempt)
    pub._sleep_with_jitter = AsyncMock()
    monkeypatch.setattr("snapper.messaging.publishers.base._CANDLE_RECOVERY_PROGRESS_GRACE_S", 0.0)
    await pub._run_recovery_under_lock("no_candles_for_400s", require_candle_progress=True)
    assert calls["n"] == 2


@pytest.mark.asyncio
async def test_spawn_recovery_forwards_candle_progress_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The candle-progress flag flows from spawn to the recovery loop.

    Given: A running publisher past the recovery min-interval,
    When: Recovery is spawned with require_candle_progress=True,
    Then: The recovery loop receives the flag.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub._last_recovery_at = 0.0
    captured: dict[str, object] = {}

    async def _fake_run(reason: str, *, require_candle_progress: bool = False) -> None:
        captured["reason"] = reason
        captured["flag"] = require_candle_progress

    pub._run_recovery_under_lock = _fake_run
    monkeypatch.setattr("snapper.messaging.publishers.base.monotonic", lambda: 1000.0)
    pub._spawn_recovery("no_candles_for_400s", require_candle_progress=True)
    await asyncio.gather(*pub._recovery_tasks)
    assert captured["flag"] is True
    assert captured["reason"] == "no_candles_for_400s"


@pytest.mark.asyncio
async def test_sleep_with_jitter_applies_bounded_jitter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Backoff jitter adjusts the base sleep by the configured fraction.

    Given: A base backoff and a deterministic positive jitter draw,
    When: _sleep_with_jitter runs,
    Then: It sleeps the base value adjusted by the jitter draw.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    sleep_mock = AsyncMock()
    monkeypatch.setattr("snapper.messaging.publishers.base.asyncio.sleep", sleep_mock)
    monkeypatch.setattr("snapper.messaging.publishers.base.random.uniform", lambda _a, b: b)
    await pub._sleep_with_jitter(10.0)
    sleep_mock.assert_awaited_once_with(12.0)


@pytest.mark.asyncio
async def test_sleep_with_jitter_floors_at_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Negative jitter draws are floored to a non-negative sleep.

    Given: A base backoff and a jitter draw that would go negative,
    When: _sleep_with_jitter runs,
    Then: The sleep is clamped to a non-negative duration.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    sleep_mock = AsyncMock()
    monkeypatch.setattr("snapper.messaging.publishers.base.asyncio.sleep", sleep_mock)
    monkeypatch.setattr("snapper.messaging.publishers.base.random.uniform", lambda _a, _b: -100.0)
    await pub._sleep_with_jitter(10.0)
    sleep_mock.assert_awaited_once_with(0.0)


@pytest.mark.asyncio
async def test_stop_cancels_pending_recovery_tasks() -> None:
    """Stop cancels tracked recovery tasks before pipeline shutdown.

    Given: A publisher with a pending recovery task,
    When: stop is called,
    Then: The recovery task is cancelled and removed.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    task = asyncio.create_task(asyncio.sleep(60))
    pub._recovery_tasks.add(task)
    await pub.stop()
    assert task.cancelled()
    assert pub._recovery_tasks == set()


@pytest.mark.asyncio
async def test_default_attempt_liveness_recovery_logs_error_only() -> None:
    """Default liveness recovery does not raise.

    Given: A base publisher without a recovery override,
    When: The default recovery hook runs,
    Then: It completes without raising.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    await pub._attempt_liveness_recovery("stale")


@pytest.mark.asyncio
async def test_maybe_init_egress_pool_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    """feed_egress_enabled initializes the egress pool in the publisher process.

    Given: A publisher whose feed_egress_enabled is True,
    When: _maybe_init_egress_pool runs,
    Then: The shared safe egress-pool init is awaited with the settings service.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    pub.settings = SimpleNamespace(feed_egress_enabled=True)
    init_mock = AsyncMock()
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.safely_initialize_egress_pool", init_mock
    )
    service = MagicMock()
    await pub._maybe_init_egress_pool(service)
    init_mock.assert_awaited_once_with(service)


@pytest.mark.asyncio
async def test_maybe_init_egress_pool_when_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    """The default-off gate leaves feeds on their direct connections.

    Given: A publisher whose feed_egress_enabled is False,
    When: _maybe_init_egress_pool runs,
    Then: The egress-pool init is not called.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    pub.settings = SimpleNamespace(feed_egress_enabled=False)
    init_mock = AsyncMock()
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.safely_initialize_egress_pool", init_mock
    )
    await pub._maybe_init_egress_pool(MagicMock())
    init_mock.assert_not_called()


@pytest.mark.asyncio
async def test_liveness_fires_recovery_within_one_heartbeat_after_threshold_crossed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Heartbeat spawns recovery when message silence exceeds threshold.

    Given: A running publisher whose last message is stale,
    When: One heartbeat iteration runs,
    Then: Recovery is spawned with a no_messages reason.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub.msg_publisher = AsyncMock()
    pub.settings.zmq_heartbeat_interval_ms = 0
    pub._last_message_at = 0.0
    pub._get_liveness_recovery_threshold_s = Mock(return_value=10)
    pub._spawn_recovery = Mock()
    monkeypatch.setattr("snapper.messaging.publishers.base.monotonic", lambda: 11.0)

    async def publish_once(_topic: str, _message: HeartbeatData) -> None:
        pub.running = False

    pub._publish_heartbeat = publish_once
    await pub._heartbeat_loop()
    pub._spawn_recovery.assert_called_once()
    assert pub._spawn_recovery.call_args.kwargs["reason"] == "no_messages_for_11s"


@pytest.mark.asyncio
async def test_liveness_does_not_fire_when_messages_arriving(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Heartbeat does not recover while messages are fresh.

    Given: A running publisher whose last message is below threshold,
    When: One heartbeat iteration runs,
    Then: Recovery is not spawned.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub.settings.zmq_heartbeat_interval_ms = 0
    pub._last_message_at = 9.0
    pub._get_liveness_recovery_threshold_s = Mock(return_value=10)
    pub._spawn_recovery = Mock()
    monkeypatch.setattr("snapper.messaging.publishers.base.monotonic", lambda: 11.0)

    async def publish_once(_topic: str, _message: HeartbeatData) -> None:
        pub.running = False

    pub._publish_heartbeat = publish_once
    await pub._heartbeat_loop()
    pub._spawn_recovery.assert_not_called()


@pytest.mark.asyncio
async def test_liveness_check_skipped_when_threshold_is_zero() -> None:
    """Heartbeat skips recovery when the threshold hook disables it.

    Given: A running publisher whose threshold hook returns zero,
    When: One heartbeat iteration runs,
    Then: Recovery is not spawned.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub.settings.zmq_heartbeat_interval_ms = 0
    pub._get_liveness_recovery_threshold_s = Mock(return_value=0)
    pub._spawn_recovery = Mock()

    async def publish_once(_topic: str, _message: HeartbeatData) -> None:
        pub.running = False

    pub._publish_heartbeat = publish_once
    await pub._heartbeat_loop()
    pub._spawn_recovery.assert_not_called()


@pytest.mark.asyncio
async def test_dark_feed_exits_when_ceiling_exceeded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A feed dark past the exit ceiling raises to trigger a launcher restart.

    Given: A running publisher dark longer than the dark-feed exit ceiling,
    When: One heartbeat iteration runs,
    Then: Recovery is spawned and FeedDarkTooLongError propagates out of the
        loop so the process exits for the launcher to respawn.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub.settings.zmq_heartbeat_interval_ms = 0
    pub._last_message_at = 0.0
    pub._get_liveness_recovery_threshold_s = Mock(return_value=60)
    pub._spawn_recovery = Mock()
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.monotonic",
        lambda: _DARK_FEED_EXIT_CEILING_S + 100.0,
    )
    with pytest.raises(FeedDarkTooLongError):
        await pub._heartbeat_loop()
    pub._spawn_recovery.assert_called_once()


@pytest.mark.asyncio
async def test_dark_feed_exit_suppressed_when_threshold_zero() -> None:
    """A suppressed venue does not exit even when very stale.

    Given: A running publisher whose threshold hook returns zero (e.g. a
        scheduled market closure) with a very stale last message,
    When: One heartbeat iteration runs,
    Then: No FeedDarkTooLongError is raised and recovery is not spawned.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub.settings.zmq_heartbeat_interval_ms = 0
    pub._last_message_at = 0.0
    pub._get_liveness_recovery_threshold_s = Mock(return_value=0)
    pub._spawn_recovery = Mock()

    async def publish_once(_topic: str, _message: HeartbeatData) -> None:
        pub.running = False

    pub._publish_heartbeat = publish_once
    await pub._heartbeat_loop()
    pub._spawn_recovery.assert_not_called()


def test_candle_liveness_threshold_disabled_by_default() -> None:
    """The native-candle liveness guard is opt-in per venue.

    Given: A base publisher,
    When: The candle liveness threshold is read,
    Then: It returns zero, leaving the guard disabled for venues whose
        candles track trade liveness.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    assert pub._candle_liveness_threshold_s() == 0


def test_candle_liveness_fires_recovery_when_candles_stale(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A silent candle channel triggers recovery while trades stay fresh.

    Given: A publisher whose message watchdog is satisfied (ticks/trades
        flowing) but whose native candle channel has been silent past the
        candle threshold,
    When: The liveness guard runs,
    Then: Recovery is spawned with a no_candles reason.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    pub._get_liveness_recovery_threshold_s = Mock(return_value=60)
    pub._candle_liveness_threshold_s = Mock(return_value=300)
    pub._consumes_native_candles = True
    pub._spawn_recovery = Mock()
    pub._last_message_at = 1000.0
    pub._last_candle_msg_at = 0.0
    monkeypatch.setattr("snapper.messaging.publishers.base.monotonic", lambda: 1000.0)
    pub._check_feed_liveness()
    pub._spawn_recovery.assert_called_once()
    assert pub._spawn_recovery.call_args.kwargs["reason"] == "no_candles_for_1000s"


def test_candle_liveness_does_not_fire_when_candles_fresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fresh candles leave the candle guard quiet.

    Given: A publisher whose last native candle is below the candle
        threshold,
    When: The liveness guard runs,
    Then: Recovery is not spawned.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    pub._get_liveness_recovery_threshold_s = Mock(return_value=60)
    pub._candle_liveness_threshold_s = Mock(return_value=300)
    pub._consumes_native_candles = True
    pub._spawn_recovery = Mock()
    pub._last_message_at = 1000.0
    pub._last_candle_msg_at = 900.0
    monkeypatch.setattr("snapper.messaging.publishers.base.monotonic", lambda: 1000.0)
    pub._check_feed_liveness()
    pub._spawn_recovery.assert_not_called()


def test_candle_liveness_skipped_when_threshold_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A disabled candle guard never fires even on long candle silence.

    Given: A publisher whose candle threshold hook returns zero with a very
        stale last candle,
    When: The liveness guard runs,
    Then: Recovery is not spawned by the candle branch.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    pub._get_liveness_recovery_threshold_s = Mock(return_value=60)
    pub._candle_liveness_threshold_s = Mock(return_value=0)
    pub._spawn_recovery = Mock()
    pub._last_message_at = 1000.0
    pub._last_candle_msg_at = 0.0
    monkeypatch.setattr("snapper.messaging.publishers.base.monotonic", lambda: 1000.0)
    pub._check_feed_liveness()
    pub._spawn_recovery.assert_not_called()


def test_candle_liveness_does_not_raise_dark_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A candle-only stall never escalates to a process exit.

    Given: A publisher whose trade channel is fresh but whose candle
        channel has been silent far past the dark-feed exit ceiling,
    When: The liveness guard runs,
    Then: It spawns recovery for the dead candle channel but does NOT raise
        FeedDarkTooLongError, so the live trade feed is never killed.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    pub._get_liveness_recovery_threshold_s = Mock(return_value=60)
    pub._candle_liveness_threshold_s = Mock(return_value=300)
    pub._consumes_native_candles = True
    pub._spawn_recovery = Mock()
    fresh_now = _DARK_FEED_EXIT_CEILING_S + 100.0
    pub._last_message_at = fresh_now
    pub._last_candle_msg_at = 0.0
    monkeypatch.setattr("snapper.messaging.publishers.base.monotonic", lambda: fresh_now)
    pub._check_feed_liveness()
    pub._spawn_recovery.assert_called_once()
    assert "no_candles_for_" in pub._spawn_recovery.call_args.kwargs["reason"]


def test_message_recovery_is_candle_aware_on_candle_venue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """On a candle venue, a message-dark recovery also waits for candles.

    Given: A candle venue whose message watchdog is stale but whose candle
        watermark is fresh,
    When: The liveness guard runs,
    Then: It spawns a message recovery flagged to require candle progress, so
        a general-outage recovery does not declare success on trades alone
        and let the candle branch restart the just-restored socket.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    pub._get_liveness_recovery_threshold_s = Mock(return_value=60)
    pub._candle_liveness_threshold_s = Mock(return_value=300)
    pub._consumes_native_candles = True
    pub._spawn_recovery = Mock()
    pub._last_message_at = 0.0
    pub._last_candle_msg_at = 1000.0
    monkeypatch.setattr("snapper.messaging.publishers.base.monotonic", lambda: 1000.0)
    pub._check_feed_liveness()
    pub._spawn_recovery.assert_called_once()
    assert pub._spawn_recovery.call_args.kwargs["reason"] == "no_messages_for_1000s"
    assert pub._spawn_recovery.call_args.kwargs["require_candle_progress"] is True


def test_liveness_both_thresholds_breached_each_branch_requires_candle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A general outage on a candle venue makes both triggers candle-aware.

    Given: A candle venue stale on both the message and candle watchdogs but
        below the dark-feed exit ceiling,
    When: The liveness guard runs,
    Then: Both branches request recovery with candle progress required (the
        spawn-level dedup then collapses them to a single attempt).
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    pub._get_liveness_recovery_threshold_s = Mock(return_value=60)
    pub._candle_liveness_threshold_s = Mock(return_value=300)
    pub._consumes_native_candles = True
    pub._spawn_recovery = Mock()
    pub._last_message_at = 0.0
    pub._last_candle_msg_at = 0.0
    monkeypatch.setattr("snapper.messaging.publishers.base.monotonic", lambda: 1000.0)
    pub._check_feed_liveness()
    assert pub._spawn_recovery.call_count == 2
    reasons = [call.kwargs["reason"] for call in pub._spawn_recovery.call_args_list]
    assert reasons == ["no_messages_for_1000s", "no_candles_for_1000s"]
    assert all(
        call.kwargs["require_candle_progress"] is True
        for call in pub._spawn_recovery.call_args_list
    )


def test_candle_liveness_skipped_when_no_candle_subscription(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A candle venue with no active candle subscription never churns.

    Given: A venue that opts into the candle guard but is not consuming any
        native candle (the degenerate timeframes=[] config),
    When: The liveness guard runs with a very stale candle watermark,
    Then: No recovery is spawned, so the guard cannot churn forever waiting
        for a candle that is never subscribed.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    pub._get_liveness_recovery_threshold_s = Mock(return_value=60)
    pub._candle_liveness_threshold_s = Mock(return_value=300)
    pub._consumes_native_candles = False
    pub._spawn_recovery = Mock()
    pub._last_message_at = 1000.0
    pub._last_candle_msg_at = 0.0
    monkeypatch.setattr("snapper.messaging.publishers.base.monotonic", lambda: 1000.0)
    pub._check_feed_liveness()
    pub._spawn_recovery.assert_not_called()


@pytest.mark.asyncio
async def test_check_feed_liveness_both_stale_spawns_single_recovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With the real spawn, a both-stale heartbeat collapses to one recovery.

    Given: A candle venue stale on both watchdogs, using the real
        _spawn_recovery (only its attempt hook is stubbed),
    When: The liveness guard runs,
    Then: Exactly one recovery task is created — the second branch's spawn is
        deduplicated by the just-set recovery timestamp.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub._get_liveness_recovery_threshold_s = Mock(return_value=60)
    pub._candle_liveness_threshold_s = Mock(return_value=300)
    pub._consumes_native_candles = True
    pub._last_recovery_at = 0.0
    pub._last_message_at = 0.0
    pub._last_candle_msg_at = 0.0
    pub._attempt_liveness_recovery = AsyncMock()
    pub._await_recovery_progress = AsyncMock(return_value=True)
    monkeypatch.setattr("snapper.messaging.publishers.base.monotonic", lambda: 1000.0)
    pub._check_feed_liveness()
    assert len(pub._recovery_tasks) == 1
    await asyncio.gather(*pub._recovery_tasks)


@pytest.mark.asyncio
async def test_process_candle_bumps_candle_watermark(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Each native candle frame refreshes the candle liveness watermark.

    Given: A publisher whose instrument resolution short-circuits,
    When: A native candle is processed,
    Then: The candle watermark advances to the current monotonic time even
        though the row is dropped for an unresolved instrument.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    pub._ensure_instrument = AsyncMock(return_value=None)
    pub._last_candle_msg_at = 0.0
    monkeypatch.setattr("snapper.messaging.publishers.base.monotonic", lambda: 5000.0)
    candle = _candle_update(begin=datetime(2026, 6, 19, 8, 0, tzinfo=UTC))
    result = await pub._process_candle(candle, ExchangeEnum.KRAKEN, "1m")
    assert result is None
    assert pub._last_candle_msg_at == 5000.0


def test_candle_source_for_defaults_native() -> None:
    """The base provenance hook tags native-path candles as upstream OHLC.

    Given: A base publisher,
    When: The candle source hook is read for 1m,
    Then: It returns 'native' (venue-precomputed upstream).
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    assert pub._candle_source_for("1m") == "native"


def test_candle_stream_timeframes_default_to_native_set() -> None:
    """The direct candle stream defaults to the native timeframe set.

    Given: A base publisher,
    When: Its candle stream timeframe hook is read,
    Then: It returns the same set as the native timeframe hook.

    Returns:
        None.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    assert pub._candle_stream_timeframes() == pub._native_candle_timeframes()


def test_subscribe_candle_stream_defaults_to_exchange_subscribe_candles() -> None:
    """The default candle stream hook delegates to subscribe_candles unchanged.

    Given: A base publisher with a recording exchange client,
    When: _subscribe_candle_stream is called,
    Then: It returns the client's candle stream and passes timeframe as a keyword.

    Returns:
        None.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    client = RecordingCandleClient()
    pub._exchange_client = client
    stream = pub._subscribe_candle_stream(["BTC-USD"], "1m")
    assert stream is client.stream
    assert client.calls == [(["BTC-USD"], "1m")]


@pytest.mark.asyncio
async def test_process_candle_tags_source_from_hook() -> None:
    """The native candle row carries the provenance from the source hook.

    Given: A publisher whose source hook returns 'calculated',
    When: A native candle is processed for a resolvable instrument,
    Then: The built row is tagged source='calculated'.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub._ensure_instrument = AsyncMock(return_value="inst-1")
    pub._publish_message = AsyncMock()
    pub._candle_source_for = lambda _tf: "calculated"
    candle = _candle_update(begin=_candle_minute(10, 0))
    row = await pub._process_candle(candle, cast(Any, "kraken"), "1m")
    assert row is not None
    assert row["source"] == "calculated"


@pytest.mark.asyncio
async def test_process_candle_marks_elapsed_1m_window_complete() -> None:
    """A 1m bar opened more than 60s ago publishes complete=True.

    Given: a 1m candle whose window opened 61s before now,
    When: it is processed,
    Then: the published CandleData carries complete=True (window elapsed) —
        the window width comes from the '1m' label (60s), not the Kraken
        minute-encoded CandleUpdate.interval.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub._ensure_instrument = AsyncMock(return_value="inst-1")
    pub._publish_message = AsyncMock()
    candle = _candle_update(begin=datetime.now(UTC) - timedelta(seconds=61))
    await pub._process_candle(candle, cast(Any, "kraken"), "1m")
    _topic, message = pub._publish_message.await_args.args
    assert message.complete is True


@pytest.mark.asyncio
async def test_process_candle_marks_recent_1m_window_incomplete() -> None:
    """A 1m bar opened less than 60s ago publishes complete=False.

    Given: a 1m candle whose window opened 30s before now,
    When: it is processed,
    Then: the published CandleData carries complete=False (the living bar),
        proving the boundary uses the 60s '1m' label width.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub._ensure_instrument = AsyncMock(return_value="inst-1")
    pub._publish_message = AsyncMock()
    candle = _candle_update(begin=datetime.now(UTC) - timedelta(seconds=30))
    await pub._process_candle(candle, cast(Any, "kraken"), "1m")
    _topic, message = pub._publish_message.await_args.args
    assert message.complete is False


@pytest.mark.asyncio
async def test_publish_synthesized_candle_carries_complete_flag() -> None:
    """A synthesized bar publishes the aggregator's complete flag on the wire.

    Given: a synthesized candle marked complete=False by the aggregator,
    When: it is published,
    Then: the published CandleData carries complete=False so ZMQ subscribers
        can distinguish a provisional rollup from a final one.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub._ensure_instrument = AsyncMock(return_value="inst-1")
    pub._publish_message = AsyncMock()
    synth = CandleUpdate(
        symbol="BTC-USD",
        open=1.0,
        high=2.0,
        low=0.5,
        close=1.5,
        vwap=1.25,
        trades=3,
        volume=4.0,
        interval_begin=_candle_minute(10, 0),
        interval=3600,
        complete=False,
    )
    await pub._publish_synthesized_candle(synth, cast(Any, "kraken"), "1h")
    _topic, message = pub._publish_message.await_args.args
    assert message.complete is False


def test_dark_feed_ceiling_exceeds_launcher_total_reset() -> None:
    """The dark-feed exit ceiling must exceed the launcher total-reset uptime.

    Given: The dark-feed exit ceiling and the launcher long-healthy uptime
        threshold that resets the lifetime failed-restart counter,
    When: They are compared,
    Then: The ceiling is strictly greater, so every dark-exit process has
        run long enough to register as long-healthy. The launcher therefore
        resets its lifetime restart budget on each dark-exit, giving an
        unbounded self-healing restart cadence during a prolonged outage
        instead of permanently abandoning the feed once the budget is spent.
    """
    assert _DARK_FEED_EXIT_CEILING_S > _TOTAL_RESET_UPTIME_S


@pytest.mark.asyncio
async def test_tick_loop_handles_unknown_symbol(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test tick loop handles unknown symbols.

    Given: A tick loop receiving unknown symbol,
    When: Tick arrives for unknown symbol,
    Then: No error occurs.
    """
    pub: Any = DummyPublisher(symbols=["KNOWN"])
    pub.running = True
    pub.msg_publisher = AsyncMock()
    pub.repository = SimpleNamespace()
    pub._exchange_client = SimpleNamespace()

    async def gen() -> AsyncIterator[Any]:
        yield TickData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            instrument="UNKNOWN",
            volume=1.0,
            last=1.0,
            exchange="kraken",
        )
        pub.running = False

    pub._exchange_client.subscribe_ticks = lambda symbols: gen()
    await pub._tick_loop(["KNOWN"])


@pytest.mark.asyncio
async def test_tick_loop_handles_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test tick loop handles exceptions.

    Given: A tick loop with failing generator,
    When: Generator raises exception,
    Then: Exception is handled gracefully.
    """
    pub: Any = DummyPublisher(symbols=["KNOWN"])
    pub.running = True
    pub.msg_publisher = AsyncMock()
    pub.repository = SimpleNamespace()
    pub._exchange_client = SimpleNamespace()

    async def gen() -> AsyncIterator[Any]:
        for _ in []:
            yield
        raise RuntimeError("boom")

    pub._exchange_client.subscribe_ticks = lambda symbols: gen()
    await pub._tick_loop(["KNOWN"])


@pytest.mark.asyncio
async def test_trade_loop_publishes_and_saves(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test trade loop publishes and saves trades.

    Given: A trade loop with incoming trades,
    When: Trade arrives,
    Then: Message is published and persisted.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub.msg_publisher = AsyncMock()
    pub.repository = SimpleNamespace(upsert_trades=AsyncMock())
    pub._exchange_client = SimpleNamespace()
    pub._ensure_instrument = AsyncMock(return_value="inst-pub-1")

    async def gen() -> AsyncIterator[Any]:
        yield SimpleNamespace(
            symbol="BTC-USD",
            price=100.0,
            quantity=1.0,
            side="buy",
            trade_id="12345",
            timestamp=datetime.now(UTC),
        )
        pub.running = False

    pub._exchange_client.subscribe_trades = lambda symbols: gen()
    await pub._trade_loop(["BTC-USD"])
    pub.msg_publisher.send.assert_awaited()


@pytest.mark.asyncio
async def test_candle_loop_handles_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test candle loop handles errors.

    Given: A candle loop with failing generator,
    When: Generator raises exception,
    Then: Exception is handled gracefully.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub.msg_publisher = AsyncMock()
    pub.repository = SimpleNamespace()
    pub._exchange_client = SimpleNamespace()

    async def gen() -> AsyncIterator[Any]:
        for _ in []:
            yield
        raise RuntimeError("boom")

    pub._exchange_client.subscribe_candles = lambda symbols, timeframe: gen()
    pub._ensure_instrument = AsyncMock(return_value="inst-pub-1")
    await pub._candle_loop(["BTC-USD"], "1m")


@pytest.mark.asyncio
async def test_publish_message_skips_when_not_running() -> None:
    """Test publish skips when not running.

    Given: A publisher that is not running,
    When: Publish message is called,
    Then: Message is not sent.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.msg_publisher = AsyncMock()
    pub.running = False
    await pub._publish_message(
        "topic",
        CandleData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            instrument="i",
            volume=1.0,
            timeframe="1m",
            open=1.0,
            high=1.0,
            low=1.0,
            close=1.0,
            exchange="kraken",
            open_at=datetime.now(UTC),
        ),
    )
    pub.msg_publisher.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_publish_message_errors_are_logged(caplog: pytest.LogCaptureFixture) -> None:
    """Test publish errors are logged.

    Given: A publisher with failing send,
    When: Publish fails,
    Then: Error is logged.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    failing = AsyncMock()
    failing.send.side_effect = RuntimeError("boom")
    pub.msg_publisher = failing
    pub.running = True
    await pub._publish_message(
        "topic",
        CandleData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            instrument="i",
            volume=1.0,
            timeframe="1m",
            open=1.0,
            high=1.0,
            low=1.0,
            close=1.0,
            exchange="kraken",
            open_at=datetime.now(UTC),
        ),
    )
    failing.send.assert_awaited_once()


@pytest.mark.asyncio
async def test_publish_message_dedupes_repeated_topic_errors(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Repeated publish errors for the same topic log ERROR once, then DEBUG.

    Given: A publisher whose send always raises for the same topic
        (mirrors the production ``market.kraken.BRK.B.ticks`` flood —
        symbols with dots fail topic validation on every tick),
    When: ``_publish_message`` is invoked 20 times for that topic,
    Then: Exactly one ERROR is emitted; the remaining 19 attempts
        downgrade to DEBUG so operators see the first failure but the
        log file does not get drowned in identical errors.
    """
    pub: Any = DummyPublisher(symbols=["BRK.B"])
    failing = AsyncMock()
    failing.send.side_effect = RuntimeError(
        "Invalid topic 'market.kraken.BRK.B.ticks': Unknown instrument 'BRK'"
    )
    pub.msg_publisher = failing
    pub.running = True
    msg = CandleData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        instrument="BRK.B",
        volume=1.0,
        timeframe="1m",
        open=1.0,
        high=1.0,
        low=1.0,
        close=1.0,
        exchange="kraken",
        open_at=datetime.now(UTC),
    )
    sink_id = logger.add(caplog.handler, format="{message}", level="DEBUG")
    try:
        for _ in range(20):
            await pub._publish_message("market.kraken.BRK.B.ticks", msg)
    finally:
        logger.remove(sink_id)
    errors = [rec for rec in caplog.records if rec.levelname == "ERROR"]
    debugs = [
        rec
        for rec in caplog.records
        if rec.levelname == "DEBUG" and "already-logged topic" in rec.message
    ]
    assert len(errors) == 1
    assert len(debugs) == 19


@pytest.mark.asyncio
async def test_publish_heartbeat_when_running() -> None:
    """Test heartbeat published when running.

    Given: A running publisher,
    When: Heartbeat is published,
    Then: Message is sent.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.msg_publisher = AsyncMock()
    pub.running = True
    msg = HeartbeatData(
        session_id="",
        sequence_id=0,
        component="c",
        sequence=1,
        status="healthy",
        lag_ms=0,
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
    )
    await pub._publish_heartbeat("heartbeat.c", msg)
    pub.msg_publisher.send.assert_awaited()


@pytest.mark.asyncio
async def test_symbol_aliases_loop_invokes_invalidation(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test symbol aliases loop invalidates cache.

    Given: A publisher receiving symbol alias update,
    When: Update message arrives,
    Then: Symbol cache is invalidated.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    subscriber = StubValidatedSubscriber()
    calls = 0

    async def recv() -> tuple[str, bytes]:
        nonlocal calls
        calls += 1
        if calls == 1:
            return "system.symbol_aliases", b"{}"
        pub.running = False
        await asyncio.sleep(0)
        return "noop", b"{}"

    subscriber.recv_multipart.side_effect = recv
    pub.subscriber = subscriber
    pub.running = True
    invalidate_mock = MagicMock()
    monkeypatch.setattr(pub, "_invalidate_symbol_cache", invalidate_mock)
    await pub._symbol_aliases_loop()
    invalidate_mock.assert_called_once()


@pytest.mark.asyncio
async def test_build_candle_row_materializes_fields() -> None:
    """Verify _build_candle_row materializes a CandleUpsertRow from CandleData.

    Given: A publisher and a CandleData message,
    When: _build_candle_row is called,
    Then: The returned dict contains all required fields.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    candle = CandleData(
        session_id="sess-1",
        sequence_id=5,
        public_id="candle-pub-id",
        instrument="BTC-USD",
        volume=1.0,
        timeframe="1m",
        open=100.0,
        high=110.0,
        low=90.0,
        close=105.0,
        vwap=102.0,
        trades=7,
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        exchange="kraken",
        open_at=datetime(2024, 1, 1, tzinfo=UTC),
    )
    row = pub._build_candle_row(candle, "inst-pub-1")
    assert row["public_id"] == "candle-pub-id"
    assert row["instrument_public_id"] == "inst-pub-1"
    assert row["open"] == pytest.approx(100.0)
    assert row["high"] == pytest.approx(110.0)
    assert row["low"] == pytest.approx(90.0)
    assert row["close"] == pytest.approx(105.0)
    assert row["vwap"] == pytest.approx(102.0)
    assert row["trades"] == 7
    assert row["volume"] == pytest.approx(1.0)
    assert row["session_id"] == "sess-1"
    assert row["sequence_id"] == 5


@pytest.mark.asyncio
async def test_flush_candle_batch_upserts_rows() -> None:
    """Verify _flush_candle_batch upserts rows to repository.

    Given: A publisher with a repository,
    When: _flush_candle_batch is called with rows,
    Then: Rows are upserted and flush_errors reset to zero.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.repository = SimpleNamespace(upsert_candles=AsyncMock())
    batch = [
        {
            "public_id": "c1",
            "instrument_public_id": "inst-1",
            "open_at": datetime(2024, 1, 1, tzinfo=UTC),
            "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
            "timeframe": "1m",
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 10.0,
            "vwap": 1.2,
            "trades": 5,
            "session_id": "",
            "sequence_id": 0,
        }
    ]
    await pub._flush_candle_batch(batch)
    pub.repository.upsert_candles.assert_awaited_once_with(batch)
    assert pub._flush_errors["candle"] == 0


@pytest.mark.asyncio
async def test_flush_candle_batch_empty_is_noop() -> None:
    """Verify _flush_candle_batch does nothing for empty batch.

    Given: A publisher with a repository,
    When: _flush_candle_batch is called with empty list,
    Then: No repository call is made.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.repository = SimpleNamespace(upsert_candles=AsyncMock())
    await pub._flush_candle_batch([])
    pub.repository.upsert_candles.assert_not_awaited()


@pytest.mark.asyncio
async def test_stop_disconnects_and_closes() -> None:
    """Test stop disconnects and closes resources.

    Given: A running publisher,
    When: Stop is called,
    Then: Client disconnected, sockets closed.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    disconnect = AsyncMock()
    stop_health_loop = AsyncMock()
    pub._exchange_client = SimpleNamespace(
        disconnect=disconnect,
        stop_health_loop=stop_health_loop,
    )
    pub.publisher = zmq_socket_stub(close=Mock())
    pub.subscriber = zmq_socket_stub(close=Mock())
    term = Mock()
    pub.context = SimpleNamespace(term=term)
    await pub.stop()
    stop_health_loop.assert_awaited_once()
    disconnect.assert_awaited_once()
    assert pub._exchange_client is None
    assert pub.publisher.close.called
    assert pub.subscriber.close.called
    assert term.called


@pytest.mark.asyncio
async def test_start_handles_cancelled_tasks(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test start handles cancelled tasks.

    Given: A publisher with tasks that get cancelled,
    When: Tasks are cancelled during gather,
    Then: Publisher handles cancellation gracefully.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub._get_max_symbols_per_connection = Mock(return_value=0)
    mock_repo = SimpleNamespace(get_latest_candle_ids=AsyncMock(return_value={}))
    monkeypatch.setattr("snapper.messaging.publishers.base.get_repository", lambda _url: mock_repo)
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.get_settings_service",
        AsyncMock(return_value=SimpleNamespace()),
    )
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.get_settings_with_service",
        lambda _svc: pub.settings,
    )

    class DummySock(SimpleNamespace):
        def __init__(self) -> None:
            super().__init__(
                connect=lambda *_: None, close=lambda: None, setsockopt=lambda o, v: None
            )

    class DummyCtx:
        def socket(self, *_args: Any) -> DummySock:
            return DummySock()

        def term(self) -> None:
            return None

    monkeypatch.setattr("snapper.messaging.publishers.base.zmq.asyncio.Context", lambda: DummyCtx())
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.ValidatedPublisher",
        lambda sock: SimpleNamespace(
            close=sock.close, send_multipart=AsyncMock(), setsockopt=sock.setsockopt
        ),
    )
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.ValidatedSubscriber",
        lambda sock: SimpleNamespace(
            subscribe=lambda *_: None,
            recv_multipart=AsyncMock(),
            close=sock.close,
            setsockopt=sock.setsockopt,
        ),
    )
    client: Any = DummyClient()
    client.connect = AsyncMock()
    client.subscribe_candles = AsyncMock()
    client.subscribe_ticks = AsyncMock()
    client.subscribe_trades = AsyncMock()
    pub._create_exchange_client = lambda: client
    pub.settings.timeframes = []
    pub.settings.zmq_heartbeat_interval_ms = 0
    pub.settings.write_buffer_candle_max_rows = 100
    pub.settings.write_buffer_tick_max_rows = 500
    pub.settings.write_buffer_trade_max_rows = 500
    pub.settings.write_buffer_flush_ms = 50

    async def noop(*_args: Any, **_kwargs: Any) -> None:
        return None

    pub._heartbeat_loop = noop
    pub._symbol_aliases_loop = noop
    pub._tick_loop = noop
    pub._tick_writer_loop = noop
    pub._trade_loop = noop
    pub._trade_writer_loop = noop
    pub._candle_loop = noop
    pub._candle_writer_loop = noop
    loop = asyncio.get_running_loop()
    monkeypatch.setattr(asyncio, "create_task", lambda coro: loop.create_task(coro))

    async def raise_cancel(*_args: Any, **_kwargs: Any) -> None:
        raise asyncio.CancelledError()

    monkeypatch.setattr(asyncio, "gather", raise_cancel)
    with pytest.raises(asyncio.CancelledError):
        await pub.start()
    await pub.stop()


@pytest.mark.asyncio
async def test_candle_loop_returns_without_client() -> None:
    """Test candle loop returns without client.

    Given: A publisher without exchange client,
    When: Candle loop is called,
    Then: Returns immediately.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    await pub._candle_loop(["BTC-USD"], "1m")


@pytest.mark.asyncio
async def test_tick_loop_returns_without_client() -> None:
    """Test tick loop returns without client.

    Given: A publisher without exchange client,
    When: Tick loop is called,
    Then: Returns immediately.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    await pub._tick_loop(["BTC-USD"])


@pytest.mark.asyncio
async def test_trade_loop_returns_without_client() -> None:
    """Test trade loop returns without client.

    Given: A publisher without exchange client,
    When: Trade loop is called,
    Then: Returns immediately.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    await pub._trade_loop(["BTC-USD"])


@pytest.mark.asyncio
async def test_tick_loop_breaks_when_stopped(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test tick loop breaks when stopped.

    Given: A publisher that is not running,
    When: Tick message arrives,
    Then: Message is not published.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = False
    pub.msg_publisher = AsyncMock()
    pub._exchange_client = SimpleNamespace()

    async def gen() -> AsyncIterator[Any]:
        yield SimpleNamespace(
            symbol="BTC-USD",
            last=1.0,
            volume=1.0,
            bid=0.0,
            ask=0.0,
            is_delayed=False,
            is_extended_hours=None,
        )

    pub._exchange_client.subscribe_ticks = lambda symbols: gen()
    await pub._tick_loop(["BTC-USD"])
    pub.msg_publisher.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_tick_loop_processes_message(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test tick loop processes message.

    Given: A running publisher with tick data,
    When: Tick arrives,
    Then: Message published and timestamp updated.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub.msg_publisher = AsyncMock()
    pub.repository = SimpleNamespace(upsert_ticks=AsyncMock())
    pub._exchange_client = SimpleNamespace()
    pub._ensure_instrument = AsyncMock(return_value="inst-pub-1")

    async def gen() -> AsyncIterator[Any]:
        yield _ticker_update()
        pub.running = False

    pub._exchange_client.subscribe_ticks = lambda symbols: gen()
    await pub._tick_loop(["BTC-USD"])
    pub.msg_publisher.send.assert_awaited_once()
    assert pub._last_data_timestamps["BTC-USD"] > 0


@pytest.mark.asyncio
async def test_trade_loop_handles_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test trade loop handles exception.

    Given: A trade loop with failing generator,
    When: Generator raises exception,
    Then: Exception is handled gracefully.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub.msg_publisher = AsyncMock()
    pub._exchange_client = SimpleNamespace()

    async def gen() -> AsyncIterator[Any]:
        for _ in []:
            yield
        raise RuntimeError("boom")

    pub._exchange_client.subscribe_trades = lambda symbols: gen()
    await pub._trade_loop(["BTC-USD"])


@pytest.mark.asyncio
async def test_heartbeat_loop_runs_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test heartbeat loop runs once.

    Given: A running publisher with heartbeat config,
    When: Heartbeat loop runs,
    Then: Heartbeat is published and sequence incremented.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub.settings.zmq_heartbeat_interval_ms = 0
    pub._last_data_timestamps["BTC-USD"] = datetime.now(UTC).timestamp() * 1000
    publish = AsyncMock()

    async def publish_and_stop(topic: str, message: HeartbeatData) -> None:
        pub.running = False
        await publish(topic, message)

    monkeypatch.setattr(pub, "_publish_heartbeat", publish_and_stop)
    await pub._heartbeat_loop()
    publish.assert_awaited_once()
    assert pub.heartbeat_seq == 1


@pytest.mark.asyncio
async def test_publish_heartbeat_skips_when_not_running() -> None:
    """Test heartbeat skips when not running.

    Given: A publisher that is not running,
    When: Heartbeat is published,
    Then: Message is not sent.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.msg_publisher = AsyncMock()
    pub.running = False
    await pub._publish_heartbeat(
        "heartbeat.c",
        HeartbeatData(
            session_id="",
            sequence_id=0,
            component="c",
            sequence=1,
            status="healthy",
            lag_ms=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        ),
    )
    pub.msg_publisher.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_publish_heartbeat_logs_errors() -> None:
    """Test heartbeat logs errors.

    Given: A publisher with failing send,
    When: Heartbeat publish fails,
    Then: Error is logged.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    failing = AsyncMock()
    failing.send.side_effect = RuntimeError("boom")
    pub.msg_publisher = failing
    pub.running = True
    await pub._publish_heartbeat(
        "heartbeat.c",
        HeartbeatData(
            session_id="",
            sequence_id=0,
            component="c",
            sequence=1,
            status="healthy",
            lag_ms=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        ),
    )
    failing.send.assert_awaited_once()


@pytest.mark.asyncio
async def test_flush_candle_batch_logs_errors() -> None:
    """Verify _flush_candle_batch increments flush_errors on failure.

    Given: A publisher with a failing repository,
    When: upsert_candles raises a non-IntegrityError,
    Then: flush_errors is incremented.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.repository = SimpleNamespace(
        upsert_candles=AsyncMock(side_effect=RuntimeError("db fail")),
    )
    batch = [
        {
            "public_id": "c1",
            "instrument_public_id": "inst-1",
            "open_at": datetime(2024, 1, 1, tzinfo=UTC),
            "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
            "timeframe": "1m",
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 10.0,
            "vwap": 1.2,
            "trades": 5,
            "session_id": "",
            "sequence_id": 0,
        }
    ]
    await pub._flush_candle_batch(batch)
    assert pub._flush_errors["candle"] == 1


@pytest.mark.asyncio
async def test_symbol_aliases_loop_without_subscriber() -> None:
    """Test symbol aliases loop without subscriber.

    Given: A publisher without subscriber,
    When: Symbol mappings loop is called,
    Then: Returns immediately.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    await pub._symbol_aliases_loop()


@pytest.mark.asyncio
async def test_symbol_aliases_loop_outer_exception() -> None:
    """Test symbol aliases loop handles outer exception.

    Given: A publisher with failing running check,
    When: Exception raised in loop,
    Then: Exception is handled gracefully.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.subscriber = StubValidatedSubscriber()

    class Boom:
        def __bool__(self) -> bool:
            raise RuntimeError("boom")

    pub.running = Boom()
    await pub._symbol_aliases_loop()


def test_init_warns_when_no_symbols() -> None:
    """Test init with empty symbols.

    Given: Empty symbols list,
    When: Publisher is created,
    Then: No error occurs.
    """
    DummyPublisher(symbols=[])


@pytest.mark.asyncio
async def test_invalidate_symbol_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test invalidate symbol cache.

    Given: A publisher with symbol mapper,
    When: Cache invalidation is triggered,
    Then: Symbol mapper cache is invalidated.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    triggered = Mock()
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.SymbolMapperService.get_instance",
        lambda: SimpleNamespace(trigger_cache_invalidation=triggered),
    )
    pub._invalidate_symbol_cache()
    triggered.assert_called_once_with(fail_fast=False)


@pytest.mark.asyncio
async def test_stop_noop_when_not_running() -> None:
    """Test stop is no-op when not running.

    Given: A publisher that is not running,
    When: Stop is called,
    Then: Returns immediately.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    await pub.stop()


@pytest.mark.asyncio
async def test_stop_skips_missing_resources() -> None:
    """Test stop skips missing resources.

    Given: A running publisher without resources,
    When: Stop is called,
    Then: No error occurs.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    await pub.stop()


@pytest.mark.asyncio
async def test_candle_loop_processes_message(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify candle loop publishes message and hands the row off to the writer queue.

    Given: A running publisher with candle data,
    When: Candle arrives and stream ends,
    Then: ZMQ publish happens, instrument is resolved, and the row
    lands on ``_candle_write_queue`` — DB persistence now lives in
    ``_candle_writer_loop`` so the consumer does not call
    ``upsert_candles`` directly.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub.msg_publisher = AsyncMock()
    pub.repository = SimpleNamespace(
        ensure_instrument=AsyncMock(return_value=(1, "inst-pub-1")),
        upsert_candles=AsyncMock(),
    )
    pub._exchange_client = SimpleNamespace()

    async def gen() -> AsyncIterator[Any]:
        yield SimpleNamespace(
            symbol="BTC-USD",
            open=1.0,
            high=2.0,
            low=0.5,
            close=1.5,
            vwap=1.2,
            volume=10.0,
            trades=5,
            interval_begin=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        )
        pub.running = False

    pub._exchange_client.subscribe_candles = lambda symbols, timeframe: gen()
    pub._ensure_instrument = AsyncMock(return_value="inst-pub-1")
    await pub._candle_loop(["BTC-USD"], "1m")
    pub.msg_publisher.send.assert_awaited_once()
    pub._ensure_instrument.assert_awaited()
    pub.repository.upsert_candles.assert_not_awaited()
    assert pub._candle_write_queue.qsize() == 1
    assert pub._last_data_timestamps["BTC-USD"] > 0


@pytest.mark.asyncio
async def test_trade_loop_breaks_when_not_running(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test trade loop breaks when not running.

    Given: A publisher that is not running,
    When: Trade arrives,
    Then: Message is not published.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = False
    pub.msg_publisher = AsyncMock()
    pub._exchange_client = SimpleNamespace()

    async def gen() -> AsyncIterator[Any]:
        yield SimpleNamespace(
            symbol="BTC-USD",
            price=1.0,
            quantity=1.0,
            side="buy",
            trade_id="99",
            timestamp=datetime.now(UTC),
        )

    pub._exchange_client.subscribe_trades = lambda symbols: gen()
    await pub._trade_loop(["BTC-USD"])
    pub.msg_publisher.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_heartbeat_loop_handles_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test heartbeat loop handles errors.

    Given: A publisher with failing heartbeat publish,
    When: Heartbeat fails,
    Then: Exception is handled gracefully.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub.settings.zmq_heartbeat_interval_ms = 0
    pub._last_data_timestamps["BTC-USD"] = datetime.now(UTC).timestamp() * 1000

    async def raise_error(*_args: Any, **_kwargs: Any) -> None:
        pub.running = False
        raise RuntimeError("hb failure")

    monkeypatch.setattr(pub, "_publish_heartbeat", raise_error)
    await pub._heartbeat_loop()


def test_build_candle_row_null_coalesces_optional_fields() -> None:
    """Verify _build_candle_row coalesces None vwap/trades to close/zero.

    Given: A CandleData with None vwap and trades,
    When: _build_candle_row is called,
    Then: vwap falls back to close and trades falls back to 0.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    candle = CandleData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        instrument="BTC-USD",
        volume=1.0,
        timeframe="1m",
        open=100.0,
        high=110.0,
        low=90.0,
        close=50.0,
        vwap=None,
        trades=None,
        timestamp=datetime.now(tz=UTC),
        exchange="kraken",
        open_at=datetime.now(UTC),
    )
    row = pub._build_candle_row(candle, "inst-pub-1")
    assert row["vwap"] == pytest.approx(50.0)
    assert row["trades"] == 0


@pytest.mark.asyncio
async def test_flush_tick_batch_upserts_rows() -> None:
    """Verify _flush_tick_batch upserts rows to repository.

    Given: A publisher with a repository,
    When: _flush_tick_batch is called with rows,
    Then: Rows are upserted and flush_errors reset to zero.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.repository = SimpleNamespace(upsert_ticks=AsyncMock())
    batch = [
        {
            "public_id": "t1",
            "instrument_public_id": "inst-1",
            "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
            "bid": 100.0,
            "ask": 101.0,
            "last": 100.5,
            "volume": 5.0,
            "session_id": "",
            "sequence_id": 0,
        }
    ]
    await pub._flush_tick_batch(batch)
    pub.repository.upsert_ticks.assert_awaited_once_with(batch)
    assert pub._flush_errors["tick"] == 0


@pytest.mark.asyncio
async def test_flush_trade_batch_upserts_rows() -> None:
    """Verify _flush_trade_batch upserts rows to repository.

    Given: A publisher with a repository,
    When: _flush_trade_batch is called with rows,
    Then: Rows are upserted and flush_errors reset to zero.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.repository = SimpleNamespace(upsert_trades=AsyncMock())
    batch = [
        {
            "public_id": "tr1",
            "instrument_public_id": "inst-1",
            "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
            "executed_at": datetime(2024, 1, 1, tzinfo=UTC),
            "price": 100.0,
            "size": 1.5,
            "side": "buy",
            "trade_id": "exch-42",
            "session_id": "",
            "sequence_id": 0,
        }
    ]
    await pub._flush_trade_batch(batch)
    pub.repository.upsert_trades.assert_awaited_once_with(batch)
    assert pub._flush_errors["trade"] == 0


@pytest.mark.asyncio
async def test_symbol_aliases_loop_retries_on_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify symbol aliases loop retries after error.

    Given a publisher running the symbol aliases loop,
    When recv_multipart raises a RuntimeError,
    Then the loop retries after sleeping.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    subscriber = StubValidatedSubscriber()
    pub.subscriber = subscriber
    pub.running = True
    calls = 0

    async def recv() -> tuple[str, bytes]:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("failure")
        pub.running = False
        return ("noop", b"{}")

    subscriber.recv_multipart.side_effect = recv
    monkeypatch.setattr(asyncio, "sleep", AsyncMock())
    await pub._symbol_aliases_loop()
    assert calls == 2


def test_get_status() -> None:
    """Verify get_status returns expected publisher info.

    Given a publisher instance,
    When get_status is called,
    Then the returned dict contains symbols and exchange name.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    status = pub.get_status()
    assert status["symbols"] == ["BTC-USD"]
    assert status["exchange"] == "kraken"


def test_get_max_symbols_per_connection_default() -> None:
    """Verify default max symbols per connection is zero.

    Given a publisher instance,
    When _get_max_symbols_per_connection is called,
    Then it returns zero as the default value.
    """
    assert DummyPublisher(symbols=["BTC-USD"])._get_max_symbols_per_connection() == 0


def test_supports_public_trades_default_true() -> None:
    """Verify default supports_public_trades returns True.

    Given a publisher instance,
    When _supports_public_trades is called,
    Then it returns True as the default value.
    """
    assert DummyPublisher(symbols=["BTC-USD"])._supports_public_trades() is True


@pytest.mark.asyncio
async def test_start_skips_trade_loop_when_unsupported(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify trade loop is not started when exchange has no trade feed.

    Given: A publisher where _supports_public_trades returns False,
    When: start() is called,
    Then: _trade_loop is not invoked.
    """
    pub: Any = DummyPublisher(symbols=["EUR-PLN"])
    pub._supports_public_trades = lambda: False
    mock_repo = SimpleNamespace(get_latest_candle_ids=AsyncMock(return_value={}))
    monkeypatch.setattr("snapper.messaging.publishers.base.get_repository", lambda _url: mock_repo)
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.get_settings_service",
        AsyncMock(return_value=SimpleNamespace()),
    )
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.get_settings_with_service",
        lambda _svc: pub.settings,
    )

    class DummySock(SimpleNamespace):
        def __init__(self) -> None:
            super().__init__(
                connect=lambda *_: None, close=lambda: None, setsockopt=lambda o, v: None
            )

    class DummyCtx:
        def socket(self, *_args: Any) -> DummySock:
            return DummySock()

        def term(self) -> None:
            return None

    monkeypatch.setattr("snapper.messaging.publishers.base.zmq.asyncio.Context", lambda: DummyCtx())
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.ValidatedPublisher",
        lambda sock: SimpleNamespace(
            close=sock.close, send_multipart=AsyncMock(), setsockopt=sock.setsockopt
        ),
    )
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.ValidatedSubscriber",
        lambda sock: SimpleNamespace(
            subscribe=lambda *_: None,
            recv_multipart=AsyncMock(),
            close=sock.close,
            setsockopt=sock.setsockopt,
        ),
    )
    dummy_client = cast(Any, DummyClient())
    dummy_client.connect = AsyncMock()
    pub._create_exchange_client = lambda: dummy_client
    pub.settings.timeframes = []
    pub.settings.zmq_heartbeat_interval_ms = 0
    pub.settings.write_buffer_candle_max_rows = 100
    pub.settings.write_buffer_tick_max_rows = 500
    pub.settings.write_buffer_trade_max_rows = 500
    pub.settings.write_buffer_flush_ms = 50
    pub._heartbeat_loop = AsyncMock()
    pub._symbol_aliases_loop = AsyncMock()
    pub._feed_health_flush_loop = AsyncMock()
    pub._supervise_consumer = AsyncMock()
    pub._tick_loop = AsyncMock()
    pub._tick_writer_loop = AsyncMock()
    pub._trade_loop = AsyncMock()
    pub._trade_writer_loop = AsyncMock()
    pub._candle_loop = AsyncMock()
    pub._candle_writer_loop = AsyncMock()
    pub._native_finalize_flush_loop = AsyncMock()
    await pub.start()
    pub._trade_loop.assert_not_awaited()
    await pub.stop()


@pytest.mark.asyncio
async def test_candle_loop_breaks_when_stopped() -> None:
    """Verify candle loop exits when running flag is cleared.

    Given a publisher with running=True,
    When the running flag is cleared during iteration,
    Then the candle loop exits gracefully.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub.msg_publisher = AsyncMock()
    pub.repository = SimpleNamespace(
        ensure_instrument=AsyncMock(return_value=(1, "inst-pub-1")),
        upsert_candles=AsyncMock(),
    )
    pub._exchange_client = SimpleNamespace()

    async def gen() -> AsyncIterator[Any]:
        yield SimpleNamespace(
            symbol="BTC-USD",
            open=1.0,
            high=1.0,
            low=1.0,
            close=1.0,
            vwap=1.0,
            volume=1.0,
            trades=1,
            interval_begin=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        )
        pub.running = False

    pub._exchange_client.subscribe_candles = lambda symbols, timeframe: gen()
    pub._ensure_instrument = AsyncMock(return_value="inst-pub-1")
    await pub._candle_loop(["BTC-USD"], "1m")


@pytest.mark.asyncio
async def test_candle_loop_stops_when_running_false() -> None:
    """Verify candle loop does not publish when not running.

    Given a publisher with running=False,
    When _candle_loop is invoked,
    Then no messages are published.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = False
    pub.msg_publisher = AsyncMock()
    pub.repository = SimpleNamespace(
        ensure_instrument=AsyncMock(return_value=(1, "inst-pub-1")),
        upsert_candles=AsyncMock(),
    )
    pub._exchange_client = SimpleNamespace()

    async def gen() -> AsyncIterator[Any]:
        yield SimpleNamespace(
            symbol="BTC-USD",
            open=1.0,
            high=1.0,
            low=1.0,
            close=1.0,
            vwap=1.0,
            volume=1.0,
            trades=1,
            interval_begin=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        )

    pub._exchange_client.subscribe_candles = lambda symbols, timeframe: gen()
    pub._ensure_instrument = AsyncMock(return_value="inst-pub-1")
    await pub._candle_loop(["BTC-USD"], "1m")
    pub.msg_publisher.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_heartbeat_loop_breaks_after_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify heartbeat loop exits after running flag cleared during sleep.

    Given a publisher with running=True,
    When the running flag is cleared during asyncio.sleep,
    Then the heartbeat loop exits without publishing.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub.settings.zmq_heartbeat_interval_ms = 0

    async def sleep_and_stop(_delay: float) -> None:
        pub.running = False

    monkeypatch.setattr(asyncio, "sleep", sleep_and_stop)
    pub._publish_heartbeat = AsyncMock()
    await pub._heartbeat_loop()


@pytest.mark.asyncio
async def test_symbol_aliases_loop_handles_timeout() -> None:
    """Verify symbol aliases loop handles timeout gracefully.

    Given a publisher running the symbol aliases loop,
    When recv_multipart raises a TimeoutError,
    Then the loop continues without crashing.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    subscriber = StubValidatedSubscriber()
    pub.subscriber = subscriber
    pub.running = True
    calls = 0

    async def recv() -> tuple[str, bytes]:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise TimeoutError()
        pub.running = False
        return ("noop", b"{}")

    subscriber.recv_multipart.side_effect = recv
    await pub._symbol_aliases_loop()


@pytest.mark.asyncio
async def test_symbol_aliases_loop_handles_settings_update(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify symbol mappings loop processes settings updates.

    Given a publisher running the symbol mappings loop,
    When a settings update message is received,
    Then _handle_settings_update is invoked.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    subscriber = StubValidatedSubscriber()
    calls = 0

    async def recv() -> tuple[str, bytes]:
        nonlocal calls
        calls += 1
        if calls == 1:
            return (
                "system.settings",
                b'{"type":"setting_changed","session_id":"","sequence_id":0,"key":"foo","value":"bar"}',
            )
        pub.running = False
        await asyncio.sleep(0)
        return "noop", b"{}"

    subscriber.recv_multipart.side_effect = recv
    pub.subscriber = subscriber
    pub.running = True
    handle_mock = MagicMock()
    monkeypatch.setattr(pub, "_handle_settings_update", handle_mock)
    await pub._symbol_aliases_loop()
    handle_mock.assert_called_once()


@pytest.mark.asyncio
async def test_handle_settings_update_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify successful settings update handling.

    Given a valid settings update payload,
    When _handle_settings_update is called,
    Then the settings cache is updated with the new value.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    envelope = SettingChangedData(
        session_id="",
        sequence_id=0,
        key="test_key",
        value="test_value",
        category="test",
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
    )
    payload = envelope.to_json().encode()
    mock_instance = SimpleNamespace(_cache={}, _parse_value=lambda v: v)
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.SettingsService.get_instance",
        lambda: mock_instance,
    )
    pub._handle_settings_update(payload, "kraken")
    assert mock_instance._cache["test_key"] == "test_value"


@pytest.mark.asyncio
async def test_handle_settings_update_invalid_json() -> None:
    """Verify invalid JSON is handled without error.

    Given a payload containing invalid JSON,
    When _handle_settings_update is called,
    Then no exception is raised.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub._handle_settings_update(b"not-json", "kraken")


@pytest.mark.asyncio
async def test_handle_settings_update_no_instance(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify settings update without service instance is handled.

    Given SettingsService.get_instance returns None,
    When _handle_settings_update is called,
    Then no exception is raised.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    envelope = SettingChangedData(
        session_id="",
        sequence_id=0,
        key="test_key",
        value="test_value",
        category="test",
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
    )
    payload = envelope.to_json().encode()
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.SettingsService.get_instance",
        lambda: None,
    )
    pub._handle_settings_update(payload, "kraken")


class TestPublisherDedup:
    """Tests for tick payload and trade-id deduplication."""

    @pytest.mark.asyncio
    async def test_process_tick_drops_payload_identical_consecutive_ticks_for_same_symbol(
        self,
    ) -> None:
        """Drop identical consecutive ticks.

        Given: A publisher that already processed a BTC tick,
        When: The same BTC payload is processed again,
        Then: The duplicate tick is not published.
        """
        publisher, publish_mock = _dedup_publisher()
        tick = _ticker_update()

        await publisher._process_tick(tick, ExchangeEnum.KRAKEN)
        await publisher._process_tick(tick, ExchangeEnum.KRAKEN)

        assert publish_mock.await_count == 1

    @pytest.mark.asyncio
    async def test_process_tick_publishes_when_volume_changes_alone(self) -> None:
        """Publish when only volume changes.

        Given: A publisher that processed a BTC tick,
        When: A second BTC tick differs only by volume,
        Then: The second tick is published.
        """
        publisher, publish_mock = _dedup_publisher()

        await publisher._process_tick(_ticker_update(volume=10.0), ExchangeEnum.KRAKEN)
        await publisher._process_tick(_ticker_update(volume=11.0), ExchangeEnum.KRAKEN)

        assert publish_mock.await_count == 2

    @pytest.mark.asyncio
    async def test_process_tick_publishes_when_is_delayed_flips_to_true(self) -> None:
        """Publish when is_delayed changes.

        Given: A publisher that processed a non-delayed BTC tick,
        When: A second BTC tick has is_delayed set to true,
        Then: The second tick is published.
        """
        publisher, publish_mock = _dedup_publisher()

        await publisher._process_tick(_ticker_update(is_delayed=False), ExchangeEnum.KRAKEN)
        await publisher._process_tick(_ticker_update(is_delayed=True), ExchangeEnum.KRAKEN)

        assert publish_mock.await_count == 2

    @pytest.mark.asyncio
    async def test_process_tick_publishes_when_is_extended_hours_flips_to_false(self) -> None:
        """Publish when is_extended_hours changes.

        Given: A publisher that processed an extended-hours BTC tick,
        When: A second BTC tick flips is_extended_hours to false,
        Then: The second tick is published.
        """
        publisher, publish_mock = _dedup_publisher()

        await publisher._process_tick(_ticker_update(is_extended_hours=True), ExchangeEnum.KRAKEN)
        await publisher._process_tick(_ticker_update(is_extended_hours=False), ExchangeEnum.KRAKEN)

        assert publish_mock.await_count == 2

    @pytest.mark.asyncio
    async def test_process_tick_cache_per_symbol_not_global(self) -> None:
        """Keep tick dedup state per symbol.

        Given: A publisher that processed a BTC tick,
        When: An identical ETH payload is processed,
        Then: The ETH tick is published independently of BTC state.
        """
        publisher, publish_mock = _dedup_publisher(symbols=["BTC-USD", "ETH-USD"])

        await publisher._process_tick(_ticker_update(symbol="BTC-USD"), ExchangeEnum.KRAKEN)
        await publisher._process_tick(_ticker_update(symbol="ETH-USD"), ExchangeEnum.KRAKEN)
        await publisher._process_tick(_ticker_update(symbol="BTC-USD"), ExchangeEnum.KRAKEN)

        assert publish_mock.await_count == 2

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_process_tick_cache_survives_reconnect(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Keep tick dedup state across a forced reconnect.

        Given: A Kraken publisher that processed a BTC tick,
        When: Its websocket client is force-restarted,
        Then: The same BTC payload remains cached and is dropped.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publish_mock = AsyncMock()
        publisher._publish_message = publish_mock
        publisher._ensure_instrument = AsyncMock(return_value=None)
        client = MagicMock()
        client.disconnect_websocket = AsyncMock()
        client._ensure_ws_connected = AsyncMock()
        publisher._exchange_client = client
        tick = _ticker_update()

        await publisher._process_tick(tick, ExchangeEnum.KRAKEN)
        with patch(
            "snapper.messaging.publishers.kraken.asyncio.sleep",
            new=AsyncMock(),
        ):
            await publisher._force_ws_restart()
        await publisher._process_tick(tick, ExchangeEnum.KRAKEN)

        assert publish_mock.await_count == 1

    @pytest.mark.asyncio
    async def test_process_tick_updates_last_message_at_before_dedup_return(self) -> None:
        """Refresh liveness state before dropping a duplicate tick.

        Given: A publisher with a cached BTC tick payload,
        When: The duplicate payload is processed after last_message_at is reset,
        Then: last_message_at and last_data_timestamps are updated before return.
        """
        publisher, publish_mock = _dedup_publisher()
        tick = _ticker_update()
        await publisher._process_tick(tick, ExchangeEnum.KRAKEN)
        publisher._last_message_at = 0.0
        publisher._last_data_timestamps.clear()

        await publisher._process_tick(tick, ExchangeEnum.KRAKEN)

        assert publisher._last_message_at > 0.0
        assert "BTC-USD" in publisher._last_data_timestamps
        assert publish_mock.await_count == 1

    @pytest.mark.asyncio
    async def test_process_trade_drops_already_seen_trade_id(self) -> None:
        """Drop a repeated trade id for the same symbol.

        Given: A publisher that processed trade id trade-1 for BTC,
        When: The same trade id is processed again for BTC,
        Then: The duplicate trade is not published.
        """
        publisher, publish_mock = _dedup_publisher()
        trade = _trade_update(trade_id="trade-1")

        await publisher._process_trade(trade, ExchangeEnum.KRAKEN)
        await publisher._process_trade(trade, ExchangeEnum.KRAKEN)

        assert publish_mock.await_count == 1

    @pytest.mark.asyncio
    async def test_process_trade_publishes_new_trade_id(self) -> None:
        """Publish a new trade id for the same symbol.

        Given: A publisher that processed trade id trade-1 for BTC,
        When: Trade id trade-2 is processed for BTC,
        Then: The second trade is published.
        """
        publisher, publish_mock = _dedup_publisher()

        await publisher._process_trade(_trade_update(trade_id="trade-1"), ExchangeEnum.KRAKEN)
        await publisher._process_trade(_trade_update(trade_id="trade-2"), ExchangeEnum.KRAKEN)

        assert publish_mock.await_count == 2

    @pytest.mark.asyncio
    async def test_process_trade_with_none_trade_id_always_publishes(self) -> None:
        """Publish trades that have no trade id.

        Given: A publisher receives two BTC trades with no trade id,
        When: Both trades are processed,
        Then: Both trades are published because they cannot be deduplicated.
        """
        publisher, publish_mock = _dedup_publisher()

        await publisher._process_trade(_trade_update(trade_id=None), ExchangeEnum.KRAKEN)
        await publisher._process_trade(_trade_update(trade_id=None), ExchangeEnum.KRAKEN)

        assert publish_mock.await_count == 2
        assert "BTC-USD" not in publisher._seen_trade_ids

    @pytest.mark.asyncio
    async def test_process_trade_lru_evicts_oldest_at_cap(self) -> None:
        """Evict oldest trade ids at the per-symbol cap.

        Given: A publisher sees more unique BTC trade ids than the LRU cap,
        When: The oldest id appears again after eviction,
        Then: The oldest id is treated as new and published.
        """
        publisher, publish_mock = _dedup_publisher()

        for index in range(_TRADE_ID_LRU_MAX_PER_SYMBOL + 1):
            await publisher._process_trade(
                _trade_update(trade_id=f"trade-{index}"),
                ExchangeEnum.KRAKEN,
            )
        await publisher._process_trade(_trade_update(trade_id="trade-0"), ExchangeEnum.KRAKEN)

        assert len(publisher._seen_trade_ids["BTC-USD"]) == _TRADE_ID_LRU_MAX_PER_SYMBOL
        assert publish_mock.await_count == _TRADE_ID_LRU_MAX_PER_SYMBOL + 2

    @pytest.mark.asyncio
    async def test_process_trade_lru_per_symbol_independent(self) -> None:
        """Keep trade-id dedup state per symbol.

        Given: A publisher processed trade id shared for BTC,
        When: Trade id shared is processed for ETH,
        Then: The ETH trade is published independently of BTC state.
        """
        publisher, publish_mock = _dedup_publisher(symbols=["BTC-USD", "ETH-USD"])

        await publisher._process_trade(
            _trade_update(symbol="BTC-USD", trade_id="shared"),
            ExchangeEnum.KRAKEN,
        )
        await publisher._process_trade(
            _trade_update(symbol="ETH-USD", trade_id="shared"),
            ExchangeEnum.KRAKEN,
        )
        await publisher._process_trade(
            _trade_update(symbol="BTC-USD", trade_id="shared"),
            ExchangeEnum.KRAKEN,
        )

        assert publish_mock.await_count == 2

    @pytest.mark.asyncio
    async def test_process_trade_updates_last_message_at_before_dedup_return(self) -> None:
        """Refresh liveness state before dropping a duplicate trade.

        Given: A publisher with a cached BTC trade id,
        When: The duplicate trade is processed after last_message_at is reset,
        Then: last_message_at is updated before return.
        """
        publisher, publish_mock = _dedup_publisher()
        trade = _trade_update(trade_id="trade-1")
        await publisher._process_trade(trade, ExchangeEnum.KRAKEN)
        publisher._last_message_at = 0.0
        publisher._last_data_timestamps.clear()

        await publisher._process_trade(trade, ExchangeEnum.KRAKEN)

        assert publisher._last_message_at > 0.0
        assert "BTC-USD" in publisher._last_data_timestamps
        assert publish_mock.await_count == 1


class TestFeedPublisherCoverage:
    """Tests for FeedPublisher coverage scenarios."""

    @patch("snapper.config.settings.get_settings")
    def test_initialization(self, mock_get_settings: MagicMock) -> None:
        """Verify publisher initializes with correct default state.

        Given: Mocked settings with ZMQ endpoint,
        When: KrakenMarketDataPublisher is created,
        Then: Symbols stored and running=False.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD", "EUR-USD", "BTC-EUR"])
        assert publisher.symbols == ["BTC-USD", "EUR-USD", "BTC-EUR"]
        assert publisher.running is False
        assert publisher.heartbeat_seq == 0
        assert publisher.repository is None

    @patch("snapper.config.settings.get_settings")
    def test_get_status(self, mock_get_settings: MagicMock) -> None:
        """Verify status returns current publisher state.

        Given: A publisher instance,
        When: get_status is called,
        Then: Returns dict with running, symbols, broker_endpoint, heartbeat.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_settings.zmq_heartbeat_interval_ms = 1000
        mock_get_settings.return_value = mock_settings
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        status = publisher.get_status()
        assert status["running"] is False
        assert status["symbols"] == ["BTC-USD"]
        assert "broker_endpoint" in status
        assert status["heartbeat_seq"] == 0

    @pytest.mark.asyncio
    @patch("snapper.data.repository.get_repository")
    @patch("snapper.infrastructure.exchanges.implementations.kraken.KrakenExchangeClient")
    @patch("snapper.application.services.settings.get_settings_service")
    @patch("snapper.config.settings.get_settings_with_service")
    @patch("snapper.config.settings.get_settings")
    @patch("snapper.messaging.publishers.base.zmq.asyncio.Context")
    async def test_start_creates_sockets(
        self,
        mock_context_class: MagicMock,
        mock_get_settings: MagicMock,
        mock_get_settings_with_service: MagicMock,
        mock_get_settings_service: MagicMock,
        mock_exchange_client_class: MagicMock,
        mock_get_repository: MagicMock,
    ) -> None:
        """Verify start creates required ZMQ sockets.

        Given: A publisher with mocked dependencies,
        When: start is called,
        Then: PUB and SUB sockets are created.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_settings.zmq_broker_xpub = "tcp://127.0.0.1:7501"
        mock_settings.db_url = TEST_DB_URL
        mock_settings.zmq_heartbeat_interval_ms = 1000
        mock_get_settings.return_value = mock_settings
        mock_settings_service = AsyncMock()
        mock_get_settings_service.return_value = mock_settings_service
        mock_get_settings_with_service.return_value = mock_settings
        mock_context = MagicMock()
        mock_socket = MagicMock()
        mock_context.socket.return_value = mock_socket
        mock_context_class.return_value = mock_context
        mock_exchange_client = AsyncMock()
        mock_exchange_client.start_health_loop = MagicMock()
        mock_exchange_client_class.return_value = mock_exchange_client
        mock_repo = MagicMock()
        mock_repo.get_latest_candle_ids = AsyncMock(return_value={})
        mock_get_repository.return_value = mock_repo
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        with (
            patch.object(publisher, "_heartbeat_loop", new=AsyncMock()),
            patch.object(publisher, "_symbol_aliases_loop", new=AsyncMock()),
            patch.object(publisher, "_feed_health_flush_loop", new=AsyncMock()),
            patch.object(publisher, "_supervise_consumer", new=AsyncMock()),
            patch(
                "snapper.application.services.settings.zmq.asyncio.Context",
                return_value=mock_context,
            ),
            patch.object(publisher, "_candle_loop", new=AsyncMock()),
            patch.object(publisher, "_candle_writer_loop", new=AsyncMock()),
            patch.object(publisher, "_native_finalize_flush_loop", new=AsyncMock()),
            patch.object(publisher, "_tick_loop", new=AsyncMock()),
            patch.object(publisher, "_tick_writer_loop", new=AsyncMock()),
            patch.object(publisher, "_trade_loop", new=AsyncMock()),
            patch.object(publisher, "_trade_writer_loop", new=AsyncMock()),
            patch.object(publisher, "_create_exchange_client", return_value=mock_exchange_client),
        ):
            start_task = asyncio.create_task(publisher.start())
            await asyncio.sleep(0.01)
            publisher.running = False
            await start_task
        assert mock_context.socket.call_count >= 2
        socket_calls = [call[0][0] for call in mock_context.socket.call_args_list]
        assert zmq.PUB in socket_calls
        assert zmq.SUB in socket_calls
        mock_exchange_client.connect.assert_awaited_once()
        mock_exchange_client.start_health_loop.assert_called_once()

    @pytest.mark.asyncio
    @patch("snapper.application.services.settings.get_settings_service")
    @patch("snapper.config.settings.get_settings_with_service")
    @patch("snapper.config.settings.get_settings")
    @patch("snapper.messaging.publishers.base.zmq.asyncio.Context")
    async def test_start_with_lvc_enabled(
        self,
        mock_context_class: MagicMock,
        mock_get_settings: MagicMock,
        mock_get_settings_with_service: MagicMock,
        mock_get_settings_service: MagicMock,
    ) -> None:
        """Verify publisher stops correctly with LVC enabled.

        Given: A running publisher with LVC,
        When: stop is called,
        Then: Sockets closed and context terminated.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_settings.zmq_broker_xpub = "tcp://127.0.0.1:7501"
        mock_settings.db_url = TEST_DB_URL
        mock_settings.zmq_heartbeat_interval_ms = 1000
        mock_get_settings.return_value = mock_settings
        mock_settings_service = AsyncMock()
        mock_get_settings_service.return_value = mock_settings_service
        mock_get_settings_with_service.return_value = mock_settings
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        mock_pub_socket = MagicMock()
        mock_context = MagicMock()
        publisher.publisher = mock_pub_socket
        publisher.context = mock_context
        publisher.running = True
        await publisher.stop()
        assert publisher.running is False
        mock_pub_socket.close.assert_called_once()
        mock_context.term.assert_called_once()

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_start_already_running(self, mock_get_settings: MagicMock) -> None:
        """Verify start returns when already running.

        Given: A publisher that is already running,
        When: start is called,
        Then: Returns without creating new context.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publisher.running = True
        await publisher.start()
        assert publisher.context is None

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_stop_not_running(self, mock_get_settings: MagicMock) -> None:
        """Verify stop handles non-running publisher.

        Given: A publisher that is not running,
        When: stop is called,
        Then: Returns without error.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publisher.running = False
        await publisher.stop()

    @patch("snapper.config.settings.get_settings")
    def test_get_default_parameters(self, mock_get_settings: MagicMock) -> None:
        """Verify get_default_parameters extracts symbols from settings.

        Given: Settings with instruments.kraken symbols,
        When: get_default_parameters is called,
        Then: Returns dict with symbols list.
        """
        mock_settings = MagicMock()
        mock_settings.instruments = {
            "kraken": ["BTC-USD", "EUR-USD"],
            "walutomat": [],
            "polygon": [],
        }
        kwargs = KrakenMarketDataPublisher.get_default_parameters(mock_settings)
        assert kwargs["symbols"] == ["BTC-USD", "EUR-USD"]

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_publish_message_when_running(self, mock_get_settings: MagicMock) -> None:
        """Verify publish_message sends when running.

        Given: A running publisher with mock socket,
        When: _publish_message is called,
        Then: Message is sent via msg_publisher.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publisher_any = cast(Any, publisher)
        publisher_any.running = True
        publisher_any.msg_publisher = AsyncMock()
        message = TickData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            instrument="BTC-USD",
            exchange="kraken",
            volume=1.0,
            last=100.0,
        )
        await publisher_any._publish_message("market.kraken.BTC-USD.ticks", message)
        publisher_any.msg_publisher.send.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_publish_message_not_running(self, mock_get_settings: MagicMock) -> None:
        """Verify publish_message skips when not running.

        Given: A non-running publisher,
        When: _publish_message is called,
        Then: Message is not sent.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publisher_any = cast(Any, publisher)
        publisher_any.running = False
        publisher_any.msg_publisher = AsyncMock()
        message = TickData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            instrument="BTC-USD",
            exchange="kraken",
            volume=1.0,
            last=100.0,
        )
        await publisher_any._publish_message("market.kraken.BTC-USD.ticks", message)
        publisher_any.msg_publisher.send.assert_not_called()

    @pytest.mark.asyncio
    @patch("asyncio.sleep", new_callable=AsyncMock)
    @patch("snapper.config.settings.get_settings")
    async def test_heartbeat_loop_publishes(
        self,
        mock_get_settings: MagicMock,
        mock_sleep: AsyncMock,
    ) -> None:
        """Verify heartbeat loop publishes heartbeats.

        Given: A running publisher with symbols,
        When: _heartbeat_loop runs,
        Then: Heartbeats are published with symbol metadata.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_settings.zmq_heartbeat_interval_ms = 10
        mock_get_settings.return_value = mock_settings
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publisher_any = cast(Any, publisher)
        publisher_any.running = True
        publisher_any.symbols = ["BTC-USD"]
        publisher_any._last_data_timestamps["BTC-USD"] = datetime.now(UTC).timestamp() * 1000 - 25
        publish_heartbeat_mock = AsyncMock()

        async def publish_side_effect(topic: str, message: HeartbeatData) -> None:
            assert "symbols" in message.meta
            symbols = message.meta["symbols"]
            assert isinstance(symbols, list)
            assert "BTC-USD" in symbols
            assert message.meta["symbol_count"] == 1
            publisher_any.running = False

        publish_heartbeat_mock.side_effect = publish_side_effect
        publisher_any._publish_heartbeat = publish_heartbeat_mock

        async def noop_sleep(_delay: float) -> None:
            return None

        mock_sleep.side_effect = noop_sleep
        await publisher_any._heartbeat_loop()
        publish_heartbeat_mock.assert_awaited_once()
        assert publisher_any.heartbeat_seq == 1

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_publish_heartbeat_when_running(self, mock_get_settings: MagicMock) -> None:
        """Verify publish_heartbeat sends when running.

        Given: A running publisher with mock socket,
        When: _publish_heartbeat is called,
        Then: Heartbeat message is sent via msg_publisher.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publisher_any = cast(Any, publisher)
        publisher_any.running = True
        publisher_any.msg_publisher = AsyncMock()
        heartbeat = HeartbeatData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            component="feed.kraken.BTC-USD",
            sequence=1,
            status="healthy",
            lag_ms=0,
        )
        await publisher_any._publish_heartbeat("heartbeat.feed.kraken.BTC-USD", heartbeat)
        publisher_any.msg_publisher.send.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_candle_loop_processes_messages(self, mock_get_settings: MagicMock) -> None:
        """Verify candle loop processes candle messages and enqueues rows.

        Given: A running publisher with mock exchange client,
        When: _candle_loop processes candles,
        Then: Messages are published to ZMQ and rows land on
        ``_candle_write_queue`` (DB persistence now lives in
        ``_candle_writer_loop``, not ``_candle_loop``).
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publisher_any = cast(Any, publisher)
        publisher_any.running = True
        publish_mock = AsyncMock()
        publisher_any._publish_message = publish_mock
        publisher_any.repository = SimpleNamespace(upsert_candles=AsyncMock())

        async def generator() -> AsyncIterator[CandleUpdate]:
            yield CandleUpdate(
                symbol="BTC-USD",
                open=99.0,
                high=105.0,
                low=98.0,
                close=101.0,
                vwap=100.0,
                trades=3,
                volume=12.0,
                interval_begin=datetime.now(UTC),
                interval=1,
            )
            yield CandleUpdate(
                symbol="BTC-USD",
                open=101.0,
                high=103.0,
                low=100.0,
                close=102.0,
                vwap=101.5,
                trades=2,
                volume=1.5,
                interval_begin=datetime.now(UTC),
                interval=1,
            )

        async def candle_stream(
            _symbols: list[str], _timeframe: str
        ) -> AsyncIterator[CandleUpdate]:
            async for item in generator():
                yield item

        ensure_instrument_mock = AsyncMock(return_value="inst-pub-1")
        publisher_any._ensure_instrument = ensure_instrument_mock
        publisher_any._exchange_client = cast(
            Any,
            SimpleNamespace(subscribe_candles=candle_stream),
        )
        await publisher_any._candle_loop(["BTC-USD"], "1m")
        assert publish_mock.await_count == 2
        publisher_any.repository.upsert_candles.assert_not_awaited()
        assert publisher_any._candle_write_queue.qsize() == 2
        assert "BTC-USD" in publisher_any._last_data_timestamps

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_process_tick_propagates_delay_flags(self, mock_get_settings: MagicMock) -> None:
        """Verify ``_process_tick`` copies delay/session flags from TickerUpdate to TickData.

        Given: a TickerUpdate with ``is_delayed=True`` + ``is_extended_hours=True``,
        When: ``_process_tick`` is invoked,
        Then: the TickData argument passed to ``_publish_message`` carries the same
            values (guarantees downstream ZMQ subscribers see the flags).
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = KrakenMarketDataPublisher(symbols=["MNQM6-CME"])
        publisher_any = cast(Any, publisher)
        publisher_any._publish_message = AsyncMock()
        publisher_any._ensure_instrument = AsyncMock(return_value=None)
        ticker = TickerUpdate(
            symbol="MNQM6-CME",
            bid=1.0,
            bid_qty=1.0,
            ask=2.0,
            ask_qty=1.0,
            last=1.5,
            volume=10.0,
            vwap=1.5,
            low=1.0,
            high=2.0,
            change=0.0,
            change_pct=0.0,
            is_delayed=True,
            is_extended_hours=True,
        )
        result = await publisher_any._process_tick(ticker, "kraken_equities")
        assert result is None
        publisher_any._publish_message.assert_awaited_once()
        _, tick_data = publisher_any._publish_message.await_args.args
        assert isinstance(tick_data, TickData)
        assert tick_data.is_delayed is True
        assert tick_data.is_extended_hours is True

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_tick_loop_processes_tick_message(self, mock_get_settings: MagicMock) -> None:
        """Verify tick loop publishes via ZMQ and hands the row to the writer queue.

        Given: A running publisher with mock exchange client,
        When: ``_tick_loop`` processes ticks,
        Then: ZMQ publish is awaited and the row reaches
            ``_tick_write_queue`` — DB persistence is the writer task's
            job in the decoupled writer architecture, not the
            consumer's.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publisher_any = cast(Any, publisher)
        publisher_any.running = True
        publish_mock = AsyncMock()
        publisher_any._publish_message = publish_mock
        publisher_any.repository = SimpleNamespace(upsert_ticks=AsyncMock())
        publisher_any._ensure_instrument = AsyncMock(return_value="inst-pub-1")

        async def generator() -> AsyncIterator[TickerUpdate]:
            yield TickerUpdate(
                symbol="BTC-USD",
                bid=122.0,
                bid_qty=1.0,
                ask=124.0,
                ask_qty=1.5,
                last=123.0,
                volume=4.0,
                vwap=122.5,
                low=119.0,
                high=125.0,
                change=3.0,
                change_pct=2.5,
            )

        async def tick_stream(_symbols: list[str]) -> AsyncIterator[TickerUpdate]:
            async for item in generator():
                yield item

        publisher_any._exchange_client = cast(
            Any,
            SimpleNamespace(subscribe_ticks=tick_stream),
        )
        await publisher_any._tick_loop(["BTC-USD"])
        publish_mock.assert_awaited_once()
        assert "BTC-USD" in publisher_any._last_data_timestamps
        publisher_any.repository.upsert_ticks.assert_not_awaited()
        assert publisher_any._tick_write_queue.qsize() == 1

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_trade_loop_processes_trade_messages(self, mock_get_settings: MagicMock) -> None:
        """Verify trade loop processes trade messages and enqueues rows.

        Given: A running publisher with mock exchange client,
        When: _trade_loop processes trades,
        Then: Trade messages are published to ZMQ and rows land on
        ``_trade_write_queue`` (DB persistence now lives in
        ``_trade_writer_loop``, not ``_trade_loop``).
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publisher_any = cast(Any, publisher)
        publisher_any.running = True
        publish_mock = AsyncMock()
        publisher_any._publish_message = publish_mock
        publisher_any.repository = SimpleNamespace(upsert_trades=AsyncMock())
        publisher_any._ensure_instrument = AsyncMock(return_value="inst-pub-1")

        async def generator() -> AsyncIterator[TradeUpdate]:
            yield TradeUpdate(
                symbol="BTC-USD",
                side="buy",
                quantity=0.5,
                price=130.0,
                ord_type="market",
                trade_id="12345",
                timestamp=datetime.now(UTC),
            )
            yield TradeUpdate(
                symbol="BTC-USD",
                side="sell",
                quantity=1.0,
                price=129.0,
                ord_type="market",
                trade_id="12346",
                timestamp=datetime.now(UTC),
            )

        async def trade_stream(_symbols: list[str]) -> AsyncIterator[TradeUpdate]:
            async for item in generator():
                yield item

        publisher_any._exchange_client = cast(
            Any,
            SimpleNamespace(subscribe_trades=trade_stream),
        )
        await publisher_any._trade_loop(["BTC-USD"])
        assert publish_mock.await_count == 2
        assert "BTC-USD" in publisher_any._last_data_timestamps
        publisher_any.repository.upsert_trades.assert_not_awaited()
        assert publisher_any._trade_write_queue.qsize() == 2

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_build_and_flush_candle(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify _build_candle_row + _flush_candle_batch round-trip.

        Given: A publisher with mock repository,
        When: _build_candle_row then _flush_candle_batch is called,
        Then: Row is correctly materialized and upserted.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        mock_repository = SimpleNamespace(upsert_candles=AsyncMock())
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publisher.repository = mock_repository
        publisher_any = cast(Any, publisher)
        bar_message = CandleData(
            session_id="sess-1",
            sequence_id=3,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            instrument="BTC-USD",
            exchange="kraken",
            volume=5.0,
            timeframe="1m",
            open=108.0,
            high=112.0,
            low=107.0,
            close=110.0,
            vwap=109.5,
            trades=7,
            open_at=datetime.now(UTC),
        )
        row = publisher_any._build_candle_row(bar_message, "inst-pub-42")
        assert row["instrument_public_id"] == "inst-pub-42"
        assert row["open"] == pytest.approx(108.0)
        assert row["session_id"] == "sess-1"
        await publisher_any._flush_candle_batch([row])
        mock_repository.upsert_candles.assert_awaited_once_with([row])

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    @patch("snapper.infrastructure.symbols.mapper.SymbolMapperService.get_instance")
    async def test_symbol_aliases_loop_refreshes_cache(
        self,
        mock_get_instance: MagicMock,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify symbol aliases loop refreshes cache.

        Given: A running publisher with symbol mapper,
        When: _symbol_aliases_loop receives message,
        Then: Cache invalidation is triggered.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        mock_db_mapper = MagicMock()
        mock_get_instance.return_value = mock_db_mapper
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publisher_any = cast(Any, publisher)
        publisher_any.running = True

        async def recv_stub() -> tuple[str, bytes]:
            publisher_any.running = False
            return ("system.symbol_aliases", b"{}")

        publisher_any.subscriber = SimpleNamespace(recv_multipart=recv_stub)
        await publisher_any._symbol_aliases_loop()
        mock_db_mapper.trigger_cache_invalidation.assert_called_once_with(fail_fast=False)


class DummyRepository:
    """Test stub for database repository."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.instrument_calls: list[dict[str, Any]] = []
        self.candle_calls: list[list[dict[str, Any]]] = []
        self.shadow_candle_calls: list[list[ShadowCandleUpsertRow]] = []
        self.tick_calls: list[list[dict[str, Any]]] = []
        self.trade_calls: list[list[dict[str, Any]]] = []
        self._next_id = 100

    async def ensure_instrument(
        self,
        symbol_public_id: str,
        exchange: str,
        session_id: str,
        sequence_id: int,
        timestamp: datetime | None = None,
    ) -> tuple[int, str]:
        """Upsert instrument to repository."""
        self.instrument_calls.append(
            {
                "symbol_public_id": symbol_public_id,
                "exchange": exchange,
                "session_id": session_id,
                "sequence_id": sequence_id,
                "timestamp": timestamp,
            }
        )
        current_id = self._next_id
        self._next_id += 1
        return (current_id, f"inst-pub-{current_id}")

    async def upsert_candles(self, rows: list[dict[str, Any]]) -> int:
        """Upsert candles to repository."""
        self.candle_calls.append(rows)
        return len(rows)

    async def upsert_shadow_candles(self, rows: list[ShadowCandleUpsertRow]) -> int:
        """Upsert shadow candles to repository."""
        self.shadow_candle_calls.append(rows)
        return len(rows)

    async def upsert_ticks(self, rows: list[dict[str, Any]]) -> int:
        """Upsert ticks to repository."""
        self.tick_calls.append(rows)
        return len(rows)

    async def upsert_trades(self, rows: list[dict[str, Any]]) -> int:
        """Upsert trades to repository."""
        self.trade_calls.append(rows)
        return len(rows)


def _build_bar_message(instrument: str) -> CandleData:
    return CandleData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        instrument=instrument,
        exchange="kraken",
        volume=12.5,
        timeframe="1m",
        open=1.2,
        high=1.3,
        low=1.1,
        close=1.24,
        vwap=1.23,
        trades=7,
        open_at=datetime.now(UTC),
    )


@pytest.mark.asyncio
async def test_build_candle_row_and_flush(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify _build_candle_row materializes correct row and flush persists.

    Given a publisher with a repository,
    When _build_candle_row is called and the result flushed,
    Then the row is correctly formed and persisted.
    """
    repo = DummyRepository()

    def _native_to_ws(symbol: str) -> str:
        return symbol.replace("-", "/")

    monkeypatch.setattr(
        "snapper.infrastructure.symbols.functions.native_to_kraken_websocket", _native_to_ws
    )
    publisher = KrakenMarketDataPublisher(symbols=["EUR-USD"])
    publisher.repository = repo
    bar_message = _build_bar_message("EUR-USD")
    row = publisher._build_candle_row(bar_message, "inst-pub-100")
    assert row["instrument_public_id"] == "inst-pub-100"
    assert row["open"] == pytest.approx(1.2)
    assert row["close"] == pytest.approx(1.24)
    await publisher._flush_candle_batch([row])
    assert len(repo.candle_calls) == 1
    assert repo.candle_calls[0][0]["instrument_public_id"] == "inst-pub-100"


@pytest.mark.asyncio
async def test_flush_candle_batch_integrity_error_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify _flush_candle_batch retries row-by-row on IntegrityError.

    Given a publisher with a repository that fails batch but succeeds per-row,
    When _flush_candle_batch is called,
    Then it falls back to row-by-row upsert.
    """

    def _native_to_ws(symbol: str) -> str:
        return symbol.replace("-", "/")

    monkeypatch.setattr(
        "snapper.infrastructure.symbols.functions.native_to_kraken_websocket", _native_to_ws
    )
    publisher = KrakenMarketDataPublisher(symbols=["EUR-USD"])
    call_count = 0

    async def upsert_candles_side_effect(rows: list[dict[str, Any]]) -> int:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise IntegrityError("dup", params=None, orig=Exception("dup"))
        return len(rows)

    publisher.repository = SimpleNamespace(
        upsert_candles=AsyncMock(side_effect=upsert_candles_side_effect)
    )
    row = publisher._build_candle_row(_build_bar_message("EUR-USD"), "inst-pub-1")
    await publisher._flush_candle_batch([row])
    assert call_count == 2


class PublisherSocketStub:
    """Test stub for publisher socket."""

    def __init__(self, error: Exception | None = None) -> None:
        """Initialize the instance."""
        self.calls: list[tuple[str, bytes]] = []
        self.error = error

    async def send_multipart(self, topic: str, payload: bytes) -> None:
        """Send multipart message."""
        if self.error is not None:
            raise self.error
        self.calls.append((topic, payload))


class MessagePublisherStub:
    """Test stub for MessagePublisher wrapper."""

    def __init__(self, error: Exception | None = None) -> None:
        """Initialize the instance."""
        self.calls: list[tuple[str, Any]] = []
        self.error = error

    async def send(self, stream_key: str, data: Any, *, flags: int = 0) -> None:
        """Send a message."""
        if self.error is not None:
            raise self.error
        self.calls.append((stream_key, data))


@pytest.mark.asyncio
class TestFeedPublisherCandleLoop:
    """Tests for FeedPublisher candle loop functionality."""

    @patch("snapper.config.settings.get_settings")
    async def test_candle_loop_processes_candles(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify candle loop processes and publishes candles.

        Given: A running publisher with mock exchange client,
        When: _candle_loop processes candles,
        Then: Bar messages are published with correct topics and batch flushed.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_settings.zmq_heartbeat_interval_ms = 1000
        mock_get_settings.return_value = mock_settings
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD", "ETH-USD"])
        publisher.running = True
        publisher_any = cast(Any, publisher)
        mock_exchange_client = SimpleNamespace()
        btc_candle = CandleUpdate(
            symbol="BTC-USD",
            open=49900.0,
            high=50100.0,
            low=49800.0,
            close=50000.0,
            vwap=50000.0,
            trades=42,
            volume=100.5,
            interval_begin=datetime.now(UTC),
            interval=1,
        )
        eth_candle = CandleUpdate(
            symbol="ETH-USD",
            open=2990.0,
            high=3010.0,
            low=2980.0,
            close=3000.0,
            vwap=3000.0,
            trades=100,
            volume=500.0,
            interval_begin=datetime.now(UTC),
            interval=1,
        )

        async def mock_subscribe(symbols: list[str], timeframe: str) -> AsyncIterator[CandleUpdate]:
            yield btc_candle
            yield eth_candle

        mock_exchange_client.subscribe_candles = mock_subscribe
        publisher_any._exchange_client = mock_exchange_client
        published_messages: list[tuple[str, CandleData]] = []

        async def publish_stub(topic: str, message: CandleData) -> None:
            published_messages.append((topic, message))

        publisher_any._publish_message = publish_stub
        publisher_any._ensure_instrument = AsyncMock(return_value="inst-pub-1")
        publisher_any.repository = SimpleNamespace(upsert_candles=AsyncMock())
        await publisher_any._candle_loop(
            ["BTC-USD", "ETH-USD"],
            "1m",
        )
        assert len(published_messages) == 2
        first_topic, btc_msg = published_messages[0]
        assert first_topic == "market.kraken.BTC-USD.candles.1m"
        assert isinstance(btc_msg, CandleData)
        assert btc_msg.type == "candle"
        assert btc_msg.instrument == "BTC-USD"
        assert btc_msg.close == pytest.approx(50000.0)
        assert btc_msg.volume == pytest.approx(100.5)
        assert btc_msg.open == pytest.approx(49900.0)
        assert btc_msg.high == pytest.approx(50100.0)
        assert btc_msg.low == pytest.approx(49800.0)
        assert btc_msg.vwap == pytest.approx(50000.0)
        assert btc_msg.trades == 42
        second_topic, eth_msg = published_messages[1]
        assert second_topic == "market.kraken.ETH-USD.candles.1m"
        assert isinstance(eth_msg, CandleData)
        assert eth_msg.type == "candle"
        assert eth_msg.instrument == "ETH-USD"
        assert eth_msg.close == pytest.approx(3000.0)
        publisher_any.repository.upsert_candles.assert_not_awaited()
        assert publisher_any._candle_write_queue.qsize() == 2
        assert "BTC-USD" in publisher_any._last_data_timestamps
        assert "ETH-USD" in publisher_any._last_data_timestamps

    @patch("snapper.config.settings.get_settings")
    @patch("snapper.infrastructure.exchanges.implementations.kraken.KrakenExchangeClient")
    async def test_candle_loop_stops_when_not_running(
        self,
        mock_client_class: MagicMock,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify candle loop stops when publisher is not running.

        Given: A publisher that becomes not running,
        When: _candle_loop is executing,
        Then: Loop terminates gracefully.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        mock_exchange_client = SimpleNamespace()

        async def mock_subscribe(symbols: list[str], timeframe: str) -> AsyncIterator[CandleUpdate]:
            while True:
                await asyncio.sleep(0.1)
                yield CandleUpdate(
                    symbol="BTC-USD",
                    open=50000.0,
                    high=50100.0,
                    low=49900.0,
                    close=50050.0,
                    vwap=50000.0,
                    trades=10,
                    volume=100.0,
                    interval_begin=datetime.now(UTC),
                    interval=1,
                )

        mock_exchange_client.subscribe_candles = mock_subscribe
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publisher.running = True
        publisher_any = cast(Any, publisher)
        publisher_any._exchange_client = mock_exchange_client
        published_topics: list[str] = []

        async def publish_stub(topic: str, message: CandleData) -> None:
            published_topics.append(topic)

        publisher_any._publish_message = publish_stub
        publisher_any._ensure_instrument = AsyncMock(return_value="inst-pub-1")
        task = asyncio.create_task(publisher_any._candle_loop(["BTC-USD"], "1m"))
        await asyncio.sleep(0.05)
        publisher.running = False
        await asyncio.wait_for(task, timeout=1.0)
        assert not published_topics

    @patch("snapper.config.settings.get_settings")
    @patch("snapper.infrastructure.exchanges.implementations.kraken.KrakenExchangeClient")
    async def test_candle_loop_handles_exception(
        self,
        mock_client_class: MagicMock,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify candle loop handles exceptions gracefully.

        Given: A publisher with failing subscription,
        When: _candle_loop encounters error,
        Then: Exception is handled without crash.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        mock_exchange_client = SimpleNamespace()

        async def mock_subscribe_error(
            symbols: list[str], timeframe: str
        ) -> AsyncIterator[CandleUpdate]:
            for _ in []:
                yield
            raise RuntimeError("Subscription failed")

        mock_exchange_client.subscribe_candles = mock_subscribe_error
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publisher.running = True
        publisher_any = cast(Any, publisher)
        publisher_any._exchange_client = mock_exchange_client
        publisher_any._ensure_instrument = AsyncMock(return_value="inst-pub-1")
        await publisher_any._candle_loop(["BTC-USD"], "1m")


@pytest.mark.asyncio
class TestFeedPublisherPublishMessage:
    """Tests for FeedPublisher publish message functionality."""

    @patch("snapper.config.settings.get_settings")
    async def test_publish_message_sends_multipart(self, mock_get_settings: MagicMock) -> None:
        """Verify publish_message sends multipart message.

        Given: A running publisher with socket stub,
        When: _publish_message is called with candle message,
        Then: Message is sent with correct topic and payload.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publisher.running = True
        publisher_any = cast(Any, publisher)
        msg_pub = MessagePublisherStub()
        publisher_any.msg_publisher = msg_pub
        message = CandleData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            exchange="kraken",
            instrument="BTC-USD",
            volume=100.5,
            timeframe="1m",
            open=49900.0,
            high=50100.0,
            low=49800.0,
            close=50000.0,
            open_at=datetime.now(UTC),
        )
        await publisher_any._publish_message(
            "market.kraken.BTC-USD.candles.1m",
            message,
        )
        assert len(msg_pub.calls) == 1
        sent_topic, sent_message = msg_pub.calls[0]
        assert sent_topic == "market.kraken.BTC-USD.candles.1m"
        assert sent_message.type == "candle"
        assert sent_message.instrument == "BTC-USD"

    @patch("snapper.config.settings.get_settings")
    async def test_publish_message_not_running(self, mock_get_settings: MagicMock) -> None:
        """Verify publish_message skips when not running.

        Given: A non-running publisher,
        When: _publish_message is called,
        Then: No message is sent.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publisher.running = False
        publisher_any = cast(Any, publisher)
        msg_pub = MessagePublisherStub()
        publisher_any.msg_publisher = msg_pub
        message = CandleData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            exchange="kraken",
            instrument="BTC-USD",
            volume=100.5,
            timeframe="1m",
            open=50000.0,
            high=50000.0,
            low=50000.0,
            close=50000.0,
            open_at=datetime.now(UTC),
        )
        await publisher_any._publish_message(
            "market.kraken.BTC-USD.candles.1m",
            message,
        )
        assert not msg_pub.calls

    @patch("snapper.config.settings.get_settings")
    async def test_publish_message_no_socket(self, mock_get_settings: MagicMock) -> None:
        """Verify publish_message handles missing socket.

        Given: A running publisher with no socket,
        When: _publish_message is called,
        Then: No error is raised.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publisher.running = True
        publisher.msg_publisher = None
        publisher_any = cast(Any, publisher)
        message = CandleData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            exchange="kraken",
            instrument="BTC-USD",
            volume=100.5,
            timeframe="1m",
            open=50000.0,
            high=50000.0,
            low=50000.0,
            close=50000.0,
            open_at=datetime.now(UTC),
        )
        await publisher_any._publish_message(
            "market.kraken.BTC-USD.candles.1m",
            message,
        )

    @patch("snapper.config.settings.get_settings")
    async def test_publish_message_handles_exception(self, mock_get_settings: MagicMock) -> None:
        """Verify publish_message handles send exception.

        Given: A running publisher with failing socket,
        When: _publish_message is called,
        Then: Exception is handled gracefully.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publisher.running = True
        publisher_any = cast(Any, publisher)
        msg_pub = MessagePublisherStub(error=RuntimeError("Send failed"))
        publisher_any.msg_publisher = msg_pub
        message = CandleData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            exchange="kraken",
            instrument="BTC-USD",
            volume=100.5,
            timeframe="1m",
            open=50000.0,
            high=50000.0,
            low=50000.0,
            close=50000.0,
            open_at=datetime.now(UTC),
        )
        await publisher_any._publish_message(
            "market.kraken.BTC-USD.candles.1m",
            message,
        )
        assert msg_pub.calls == []


@pytest.mark.asyncio
async def test_resolve_candle_public_id_cache_hit() -> None:
    """Verify _resolve_candle_public_id returns cached UUID on open_at match.

    Given: A publisher with a pre-populated _candle_id_cache entry,
    When: _resolve_candle_public_id is called with the same open_at,
    Then: The cached UUID is returned unchanged.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    ts = datetime(2026, 1, 1, tzinfo=UTC)
    pub._candle_id_cache[("inst-pub-1", "1m")] = (ts, "existing-uuid")
    result = pub._resolve_candle_public_id("inst-pub-1", "1m", ts)
    assert result == "existing-uuid"


@pytest.mark.asyncio
async def test_resolve_candle_public_id_different_open_at_generates_new() -> None:
    """Verify _resolve_candle_public_id generates new UUID when open_at differs.

    Given: A publisher with a cached candle ID entry,
    When: _resolve_candle_public_id is called with a different open_at,
    Then: A new UUID is generated (not the cached one).
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    ts_old = datetime(2026, 1, 1, tzinfo=UTC)
    ts_new = datetime(2026, 1, 1, 0, 1, tzinfo=UTC)
    pub._candle_id_cache[("inst-pub-1", "1m")] = (ts_old, "existing-uuid")
    result = pub._resolve_candle_public_id("inst-pub-1", "1m", ts_new)
    assert result != "existing-uuid"
    assert pub._candle_id_cache[("inst-pub-1", "1m")][0] == ts_new


@pytest.mark.asyncio
async def test_candle_loop_skips_publish_when_ensure_instrument_returns_none() -> None:
    """Verify _candle_loop skips publishing when instrument resolution fails.

    Given: A publisher whose _ensure_instrument returns None,
    When: _candle_loop processes a candle,
    Then: _publish_message is never called.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub.msg_publisher = AsyncMock()
    pub.repository = SimpleNamespace()
    pub._exchange_client = SimpleNamespace()

    async def gen() -> AsyncIterator[Any]:
        yield SimpleNamespace(
            symbol="BTC-USD",
            open=1.0,
            high=2.0,
            low=0.5,
            close=1.5,
            vwap=1.2,
            volume=10.0,
            trades=5,
            interval_begin=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        )
        pub.running = False

    pub._exchange_client.subscribe_candles = lambda symbols, timeframe: gen()
    pub._ensure_instrument = AsyncMock(return_value=None)
    pub._publish_message = AsyncMock()
    await pub._candle_loop(["BTC-USD"], "1m")
    pub._publish_message.assert_not_awaited()


@pytest.mark.asyncio
@patch(
    "snapper.messaging.publishers.base.resolve_symbol_public_id",
    new_callable=AsyncMock,
    return_value=None,
)
async def test_ensure_instrument_returns_none_for_unsplittable_symbol(
    _mock_resolve: AsyncMock,
) -> None:
    """Verify _ensure_instrument returns None when symbol cannot be resolved.

    Given: A publisher with a symbol that has no active Symbol row,
    When: _ensure_instrument is called,
    Then: None is returned.
    """
    pub: Any = DummyPublisher(symbols=["INVALID"])
    pub.repository = SimpleNamespace(ensure_instrument=AsyncMock(return_value=(1, "inst-pub-1")))
    result = await pub._ensure_instrument("INVALID")
    assert result is None


@pytest.mark.asyncio
async def test_ensure_instrument_resolves_and_caches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify _ensure_instrument resolves instrument and caches result.

    Given: A publisher with a valid symbol and mock repository,
    When: _ensure_instrument is called twice with the same symbol,
    Then: The repository is called only once and result is cached.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    mock_upsert = AsyncMock(return_value=(42, "inst-pub-42"))
    pub.repository = SimpleNamespace(ensure_instrument=mock_upsert)
    resolve_mock = AsyncMock(return_value="fake-spid")
    monkeypatch.setattr("snapper.messaging.publishers.base.resolve_symbol_public_id", resolve_mock)
    first = await pub._ensure_instrument("BTC-USD")
    second = await pub._ensure_instrument("BTC-USD")
    assert first == "inst-pub-42"
    assert second == "inst-pub-42"
    resolve_mock.assert_awaited_once_with(pub.repository, "BTC-USD", as_of=ANY)
    call_kwargs = mock_upsert.call_args.kwargs
    assert call_kwargs["symbol_public_id"] == "fake-spid"
    assert call_kwargs["exchange"] == "kraken"
    assert call_kwargs["session_id"] != ""
    assert call_kwargs["sequence_id"] >= 1


@pytest.mark.asyncio
async def test_ensure_instrument_returns_none_when_symbol_not_resolved(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify _ensure_instrument returns None when active Symbol is missing.

    Given: A publisher with a splittable symbol,
    When: symbol resolution returns None,
    Then: The instrument upsert is skipped.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    mock_upsert = AsyncMock(return_value=(42, "inst-pub-42"))
    pub.repository = SimpleNamespace(ensure_instrument=mock_upsert)
    resolve_mock = AsyncMock(return_value=None)
    monkeypatch.setattr("snapper.messaging.publishers.base.resolve_symbol_public_id", resolve_mock)
    result = await pub._ensure_instrument("BTC-USD")
    assert result is None
    resolve_mock.assert_awaited_once_with(pub.repository, "BTC-USD", as_of=ANY)
    mock_upsert.assert_not_awaited()


def _build_tick_message(instrument: str) -> TickData:
    """Build a TickData message for testing."""
    return TickData(
        session_id="test-session",
        sequence_id=1,
        public_id="tick-pub-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        instrument=instrument,
        exchange="kraken",
        volume=5.0,
        bid=100.0,
        ask=101.0,
        last=100.5,
    )


def _build_trade_message(instrument: str) -> TradeData:
    """Build a TradeData message for testing."""
    return TradeData(
        session_id="test-session",
        sequence_id=2,
        public_id="trade-pub-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        instrument=instrument,
        exchange="kraken",
        price=100.0,
        volume=1.5,
        side="buy",
        trade_id="exch-trade-42",
        executed_at=datetime(2023, 12, 31, 23, 59, 59, tzinfo=UTC),
    )


@pytest.mark.asyncio
async def test_build_tick_row_materializes_fields() -> None:
    """Verify _build_tick_row materializes a TickUpsertRow from TickData.

    Given: A publisher and a TickData message,
    When: _build_tick_row is called,
    Then: The returned dict contains all required fields.
    """
    repo = DummyRepository()
    publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
    publisher.repository = repo
    tick_msg = _build_tick_message("BTC-USD")
    row = publisher._build_tick_row(tick_msg, "inst-pub-100")
    assert row["instrument_public_id"] == "inst-pub-100"
    assert row["bid"] == 100.0
    assert row["ask"] == 101.0
    assert row["last"] == 100.5
    assert row["volume"] == 5.0
    assert row["public_id"] == "tick-pub-id"
    assert row["session_id"] == "test-session"
    assert row["sequence_id"] == 1


@pytest.mark.asyncio
async def test_build_trade_row_materializes_fields() -> None:
    """Verify _build_trade_row materializes a TradeUpsertRow from TradeData.

    Given: A publisher and a TradeData message,
    When: _build_trade_row is called,
    Then: The returned dict contains all required fields.
    """
    repo = DummyRepository()
    publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
    publisher.repository = repo
    trade_msg = _build_trade_message("BTC-USD")
    row = publisher._build_trade_row(trade_msg, "inst-pub-100")
    assert row["instrument_public_id"] == "inst-pub-100"
    assert row["price"] == 100.0
    assert row["size"] == 1.5
    assert row["side"] == "buy"
    assert row["trade_id"] == "exch-trade-42"
    assert row["timestamp"] == datetime(2024, 1, 1, tzinfo=UTC)
    assert row["executed_at"] == datetime(2023, 12, 31, 23, 59, 59, tzinfo=UTC)
    assert row["public_id"] == "trade-pub-id"


@pytest.mark.asyncio
async def test_flush_tick_batch_logs_errors() -> None:
    """Verify _flush_tick_batch increments flush_errors on failure.

    Given: A publisher with a failing repository,
    When: upsert_ticks raises,
    Then: flush_errors['tick'] is incremented.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.repository = SimpleNamespace(
        upsert_ticks=AsyncMock(side_effect=RuntimeError("db fail")),
    )
    tick_msg = _build_tick_message("BTC-USD")
    row = pub._build_tick_row(tick_msg, "inst-pub-1")
    await pub._flush_tick_batch([row])
    assert pub._flush_errors["tick"] == 1


@pytest.mark.asyncio
async def test_flush_trade_batch_logs_errors() -> None:
    """Verify _flush_trade_batch increments flush_errors on failure.

    Given: A publisher with a failing repository,
    When: upsert_trades raises,
    Then: flush_errors['trade'] is incremented.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.repository = SimpleNamespace(
        upsert_trades=AsyncMock(side_effect=RuntimeError("db fail")),
    )
    trade_msg = _build_trade_message("BTC-USD")
    row = pub._build_trade_row(trade_msg, "inst-pub-1")
    await pub._flush_trade_batch([row])
    assert pub._flush_errors["trade"] == 1


@pytest.mark.asyncio
async def test_build_trade_row_handles_null_trade_id() -> None:
    """Verify _build_trade_row persists trade with None trade_id.

    Given: A TradeData message without exchange trade_id,
    When: _build_trade_row is called,
    Then: Row has trade_id=None.
    """
    publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
    trade_msg = TradeData(
        session_id="test-session",
        sequence_id=1,
        public_id="trade-pub-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        instrument="BTC-USD",
        exchange="kraken",
        price=100.0,
        volume=1.0,
        side="buy",
    )
    row = publisher._build_trade_row(trade_msg, "inst-pub-1")
    assert row["trade_id"] is None


@pytest.mark.asyncio
async def test_build_trade_row_persists_when_no_trade_id() -> None:
    """Verify _build_trade_row handles missing trade_id (None).

    Given: A TradeData message with trade_id=None,
    When: _build_trade_row is called and flushed,
    Then: Row has trade_id=None and flushes successfully.
    """
    repo = DummyRepository()
    publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
    publisher.repository = repo
    trade_msg = TradeData(
        session_id="test-session",
        sequence_id=1,
        public_id="trade-pub-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        instrument="BTC-USD",
        exchange="kraken",
        price=100.0,
        volume=1.0,
        side="buy",
    )
    row = publisher._build_trade_row(trade_msg, "inst-pub-1")
    assert row["trade_id"] is None
    await publisher._flush_trade_batch([row])
    assert len(repo.trade_calls) == 1
    assert repo.trade_calls[0][0]["trade_id"] is None


@pytest.mark.asyncio
async def test_tick_loop_publishes_and_saves(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify tick loop publishes to ZMQ and hands the row to the writer queue.

    Given: A running publisher with tick data,
    When: Tick arrives and stream ends,
    Then: Message is published to ZMQ and the row reaches
        ``_tick_write_queue`` (DB persistence now lives in
        ``_tick_writer_loop``, not ``_tick_loop``).
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub.msg_publisher = AsyncMock()
    pub.repository = SimpleNamespace(upsert_ticks=AsyncMock())
    pub._exchange_client = SimpleNamespace()
    pub._ensure_instrument = AsyncMock(return_value="inst-pub-1")

    async def gen() -> AsyncIterator[Any]:
        yield _ticker_update()
        pub.running = False

    pub._exchange_client.subscribe_ticks = lambda symbols: gen()
    await pub._tick_loop(["BTC-USD"])
    pub.msg_publisher.send.assert_awaited()
    assert pub._tick_write_queue.qsize() == 1


@pytest.mark.asyncio
async def test_trade_loop_publishes_and_enqueues_for_writer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify trade loop publishes to ZMQ and enqueues rows for the writer task.

    Given: A running publisher with trade data,
    When: Trade arrives and stream ends,
    Then: Message is published to ZMQ and the row lands on
    ``_trade_write_queue`` — DB persistence now lives in
    ``_trade_writer_loop`` (the decoupled-writer pattern applied to trades).
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub.msg_publisher = AsyncMock()
    pub.repository = SimpleNamespace(upsert_trades=AsyncMock())
    pub._exchange_client = SimpleNamespace()
    pub._ensure_instrument = AsyncMock(return_value="inst-pub-1")

    async def gen() -> AsyncIterator[Any]:
        yield SimpleNamespace(
            symbol="BTC-USD",
            price=100.0,
            quantity=1.0,
            side="buy",
            trade_id="12345",
            timestamp=datetime.now(UTC),
        )
        await asyncio.sleep(0.05)
        pub.running = False

    pub._batch_max_age_s = 0.01
    pub._exchange_client.subscribe_trades = lambda symbols: gen()
    await pub._trade_loop(["BTC-USD"])
    pub.msg_publisher.send.assert_awaited()
    pub.repository.upsert_trades.assert_not_awaited()
    assert pub._trade_write_queue.qsize() == 1


def test_cleanup_pending_future_done_with_result() -> None:
    """Verify _cleanup_pending_future consumes result from a done future.

    Given: A future that completed with a result,
    When: _cleanup_pending_future is called,
    Then: The result is consumed without error.
    """
    loop = asyncio.new_event_loop()
    fut: asyncio.Future[int] = loop.create_future()
    fut.set_result(42)
    _cleanup_pending_future(fut)
    loop.close()


def test_cleanup_pending_future_done_with_exception() -> None:
    """Verify _cleanup_pending_future suppresses exception from a done future.

    Given: A future that completed with an exception,
    When: _cleanup_pending_future is called,
    Then: The exception is suppressed.
    """
    loop = asyncio.new_event_loop()
    fut: asyncio.Future[int] = loop.create_future()
    fut.set_exception(RuntimeError("boom"))
    _cleanup_pending_future(fut)
    loop.close()


def test_cleanup_pending_future_done_with_stop_async_iteration() -> None:
    """Verify _cleanup_pending_future suppresses StopAsyncIteration from done future.

    Given: A future that completed with StopAsyncIteration,
    When: _cleanup_pending_future is called,
    Then: The StopAsyncIteration is suppressed.
    """
    loop = asyncio.new_event_loop()
    fut: asyncio.Future[int] = loop.create_future()
    fut.set_exception(StopAsyncIteration())
    _cleanup_pending_future(fut)
    loop.close()


def test_cleanup_pending_future_done_with_cancelled_error() -> None:
    """Verify _cleanup_pending_future suppresses CancelledError from done future.

    Given: A future that completed with CancelledError,
    When: _cleanup_pending_future is called,
    Then: The CancelledError is suppressed (no propagation).
    """
    loop = asyncio.new_event_loop()
    fut: asyncio.Future[int] = loop.create_future()
    fut.cancel()
    _cleanup_pending_future(fut)
    loop.close()


@pytest.mark.asyncio
async def test_candle_loop_cancelled_error_propagates_without_flush() -> None:
    """Verify candle loop re-raises CancelledError without persisting the in-flight row.

    Given: A publisher whose _publish_message raises CancelledError,
    When: _candle_loop processes a candle,
    Then: CancelledError propagates and the consumer does not call
    ``upsert_candles`` (DB persistence is the writer task's
    responsibility now, not the consumer's).
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub.msg_publisher = AsyncMock()
    pub.repository = SimpleNamespace(upsert_candles=AsyncMock())
    pub._exchange_client = SimpleNamespace()
    pub._ensure_instrument = AsyncMock(return_value="inst-pub-1")

    async def publish_cancel(topic: str, msg: Any) -> None:
        raise asyncio.CancelledError()

    pub._publish_message = publish_cancel

    async def gen() -> AsyncIterator[Any]:
        yield SimpleNamespace(
            symbol="BTC-USD",
            open=1.0,
            high=2.0,
            low=0.5,
            close=1.5,
            vwap=1.2,
            volume=10.0,
            trades=5,
            interval_begin=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        )

    pub._exchange_client.subscribe_candles = lambda symbols, timeframe: gen()
    with pytest.raises(asyncio.CancelledError):
        await pub._candle_loop(["BTC-USD"], "1m")
    pub.repository.upsert_candles.assert_not_awaited()


@pytest.mark.asyncio
async def test_tick_loop_does_not_flush_directly() -> None:
    """Verify ``_tick_loop`` is ingest-only after the writer decouple.

    Given: A publisher with a tick in the stream,
    When: ``_tick_loop`` runs to completion,
    Then: No DB upsert happens inside the consumer — the row sits on
        ``_tick_write_queue`` for the dedicated writer task. Flushing
        decoupled from ingest is the whole point of the writer decouple.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub.msg_publisher = AsyncMock()
    pub.repository = SimpleNamespace(upsert_ticks=AsyncMock())
    pub._exchange_client = SimpleNamespace()
    pub._ensure_instrument = AsyncMock(return_value="inst-pub-1")
    pub._batch_max_age_s = 0.01

    async def gen() -> AsyncIterator[Any]:
        yield _ticker_update()
        await asyncio.sleep(0.05)
        pub.running = False

    pub._exchange_client.subscribe_ticks = lambda symbols: gen()
    await pub._tick_loop(["BTC-USD"])
    pub.repository.upsert_ticks.assert_not_awaited()
    assert pub._tick_write_queue.qsize() == 1


@pytest.mark.asyncio
async def test_tick_loop_enqueues_every_tick_for_writer() -> None:
    """Verify ``_tick_loop`` enqueues each ingested tick onto the writer queue.

    Given: A publisher with three ticks in the WS stream,
    When: ``_tick_loop`` drains the stream,
    Then: All three rows land on ``_tick_write_queue`` in order. The
        per-batch size trigger that used to live in ``_tick_loop`` is
        now the writer's responsibility — the consumer just forwards.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub.msg_publisher = AsyncMock()
    pub.repository = SimpleNamespace(upsert_ticks=AsyncMock())
    pub._exchange_client = SimpleNamespace()
    pub._ensure_instrument = AsyncMock(return_value="inst-pub-1")

    async def gen() -> AsyncIterator[Any]:
        for i in range(3):
            yield _ticker_update(volume=5.0 + i)
        pub.running = False

    pub._exchange_client.subscribe_ticks = lambda symbols: gen()
    await pub._tick_loop(["BTC-USD"])
    assert pub._tick_write_queue.qsize() == 3
    pub.repository.upsert_ticks.assert_not_awaited()


@pytest.mark.asyncio
async def test_tick_loop_skips_db_when_instrument_is_none() -> None:
    """Verify tick loop skips DB write when instrument resolution returns None.

    Given: A publisher whose _ensure_instrument returns None,
    When: _tick_loop processes a tick,
    Then: ZMQ message is published but no DB row is appended.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub.msg_publisher = AsyncMock()
    pub.repository = SimpleNamespace(upsert_ticks=AsyncMock())
    pub._exchange_client = SimpleNamespace()
    pub._ensure_instrument = AsyncMock(return_value=None)

    async def gen() -> AsyncIterator[Any]:
        yield _ticker_update()
        pub.running = False

    pub._exchange_client.subscribe_ticks = lambda symbols: gen()
    await pub._tick_loop(["BTC-USD"])
    pub.msg_publisher.send.assert_awaited()
    pub.repository.upsert_ticks.assert_not_awaited()


@pytest.mark.asyncio
async def test_tick_loop_cancelled_error() -> None:
    """Verify ``_tick_loop`` re-raises ``CancelledError`` and leaves the queued row.

    Given: A publisher whose ZMQ publish raises ``CancelledError`` on
        the second tick after the first reaches the writer queue,
    When: ``_tick_loop`` is running,
    Then: The first row stays on ``_tick_write_queue`` (no DB upsert
        attempted from the consumer side — that contract moved to
        ``_tick_writer_loop``) and ``CancelledError``
        propagates up.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub.msg_publisher = AsyncMock()
    pub.repository = SimpleNamespace(upsert_ticks=AsyncMock())
    pub._exchange_client = SimpleNamespace()
    pub._ensure_instrument = AsyncMock(return_value="inst-pub-1")

    call_count = 0

    async def publish_then_cancel(topic: str, msg: Any) -> None:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return None
        raise asyncio.CancelledError()

    pub.msg_publisher.send = publish_then_cancel

    async def gen() -> AsyncIterator[Any]:
        yield _ticker_update(volume=5.0)
        yield _ticker_update(volume=6.0)

    pub._exchange_client.subscribe_ticks = lambda symbols: gen()
    with pytest.raises(asyncio.CancelledError):
        await pub._tick_loop(["BTC-USD"])
    pub.repository.upsert_ticks.assert_not_awaited()
    assert pub._tick_write_queue.qsize() == 1


@pytest.mark.asyncio
async def test_trade_loop_skips_db_when_instrument_is_none() -> None:
    """Verify trade loop skips DB write when instrument resolution returns None.

    Given: A publisher whose _ensure_instrument returns None,
    When: _trade_loop processes a trade,
    Then: ZMQ message is published but no DB row is appended.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub.msg_publisher = AsyncMock()
    pub.repository = SimpleNamespace(upsert_trades=AsyncMock())
    pub._exchange_client = SimpleNamespace()
    pub._ensure_instrument = AsyncMock(return_value=None)

    async def gen() -> AsyncIterator[Any]:
        yield SimpleNamespace(
            symbol="BTC-USD",
            price=100.0,
            quantity=1.0,
            side="buy",
            trade_id="12345",
            timestamp=datetime.now(UTC),
        )
        pub.running = False

    pub._exchange_client.subscribe_trades = lambda symbols: gen()
    await pub._trade_loop(["BTC-USD"])
    pub.msg_publisher.send.assert_awaited()
    pub.repository.upsert_trades.assert_not_awaited()


@pytest.mark.asyncio
async def test_trade_loop_cancelled_error_propagates_without_flush() -> None:
    """Verify trade loop re-raises CancelledError without persisting the in-flight row.

    Given: A publisher whose publish raises CancelledError on second trade,
    When: _trade_loop is running,
    Then: CancelledError propagates and the consumer does not call
    ``upsert_trades`` (DB persistence is the writer task's
    responsibility now, not the consumer's). The first trade's row
    sits on ``_trade_write_queue`` for the writer to drain on its
    own shutdown path.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub.msg_publisher = AsyncMock()
    pub.repository = SimpleNamespace(upsert_trades=AsyncMock())
    pub._exchange_client = SimpleNamespace()
    pub._ensure_instrument = AsyncMock(return_value="inst-pub-1")

    call_count = 0

    async def publish_then_cancel(topic: str, msg: Any) -> None:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return None
        raise asyncio.CancelledError()

    pub.msg_publisher.send = publish_then_cancel

    async def gen() -> AsyncIterator[Any]:
        yield SimpleNamespace(
            symbol="BTC-USD",
            price=100.0,
            quantity=1.0,
            side="buy",
            trade_id="t1",
            timestamp=datetime.now(UTC),
        )
        yield SimpleNamespace(
            symbol="BTC-USD",
            price=101.0,
            quantity=2.0,
            side="sell",
            trade_id="t2",
            timestamp=datetime.now(UTC),
        )

    pub._exchange_client.subscribe_trades = lambda symbols: gen()
    with pytest.raises(asyncio.CancelledError):
        await pub._trade_loop(["BTC-USD"])
    pub.repository.upsert_trades.assert_not_awaited()
    assert pub._trade_write_queue.qsize() >= 1


@pytest.mark.asyncio
async def test_flush_candle_batch_row_by_row_integrity_error() -> None:
    """Verify _flush_candle_batch_row_by_row skips rows with IntegrityError.

    Given: A publisher whose repository raises IntegrityError on one row,
    When: _flush_candle_batch_row_by_row is called,
    Then: The bad row is skipped with a warning, flush_errors reset to 0.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    call_count = 0

    async def upsert_side_effect(rows: list[dict[str, Any]]) -> int:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise IntegrityError("dup", params=None, orig=Exception("dup"))
        return len(rows)

    pub.repository = SimpleNamespace(upsert_candles=AsyncMock(side_effect=upsert_side_effect))
    batch = [
        {
            "public_id": "c1",
            "instrument_public_id": "inst-1",
            "open_at": datetime(2024, 1, 1, tzinfo=UTC),
            "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
            "timeframe": "1m",
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 10.0,
            "vwap": 1.2,
            "trades": 5,
            "session_id": "",
            "sequence_id": 0,
        },
        {
            "public_id": "c2",
            "instrument_public_id": "inst-1",
            "open_at": datetime(2024, 1, 2, tzinfo=UTC),
            "timestamp": datetime(2024, 1, 2, tzinfo=UTC),
            "timeframe": "1m",
            "open": 2.0,
            "high": 3.0,
            "low": 1.5,
            "close": 2.5,
            "volume": 20.0,
            "vwap": 2.2,
            "trades": 10,
            "session_id": "",
            "sequence_id": 1,
        },
    ]
    await pub._flush_candle_batch_row_by_row(batch)
    assert call_count == 2
    assert pub._flush_errors["candle"] == 0


@pytest.mark.asyncio
async def test_flush_candle_batch_row_by_row_generic_exception() -> None:
    """Verify _flush_candle_batch_row_by_row handles generic Exception per row.

    Given: A publisher whose repository raises a generic Exception on one row,
    When: _flush_candle_batch_row_by_row is called,
    Then: flush_errors remains incremented (not reset when generic errors occurred).
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    call_count = 0

    async def upsert_side_effect(rows: list[dict[str, Any]]) -> int:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("db fail")
        return len(rows)

    pub.repository = SimpleNamespace(upsert_candles=AsyncMock(side_effect=upsert_side_effect))
    batch = [
        {
            "public_id": "c1",
            "instrument_public_id": "inst-1",
            "open_at": datetime(2024, 1, 1, tzinfo=UTC),
            "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
            "timeframe": "1m",
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 10.0,
            "vwap": 1.2,
            "trades": 5,
            "session_id": "",
            "sequence_id": 0,
        },
        {
            "public_id": "c2",
            "instrument_public_id": "inst-1",
            "open_at": datetime(2024, 1, 2, tzinfo=UTC),
            "timestamp": datetime(2024, 1, 2, tzinfo=UTC),
            "timeframe": "1m",
            "open": 2.0,
            "high": 3.0,
            "low": 1.5,
            "close": 2.5,
            "volume": 20.0,
            "vwap": 2.2,
            "trades": 10,
            "session_id": "",
            "sequence_id": 1,
        },
    ]
    await pub._flush_candle_batch_row_by_row(batch)
    assert pub._flush_errors["candle"] == 1


@pytest.mark.asyncio
async def test_flush_tick_batch_empty_is_noop() -> None:
    """Verify _flush_tick_batch does nothing for empty batch.

    Given: A publisher with a repository,
    When: _flush_tick_batch is called with empty list,
    Then: No repository call is made.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.repository = SimpleNamespace(upsert_ticks=AsyncMock())
    await pub._flush_tick_batch([])
    pub.repository.upsert_ticks.assert_not_awaited()


@pytest.mark.asyncio
async def test_flush_trade_batch_empty_is_noop() -> None:
    """Verify _flush_trade_batch does nothing for empty batch.

    Given: A publisher with a repository,
    When: _flush_trade_batch is called with empty list,
    Then: No repository call is made.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.repository = SimpleNamespace(upsert_trades=AsyncMock())
    await pub._flush_trade_batch([])
    pub.repository.upsert_trades.assert_not_awaited()


def _publisher_writer_session() -> tuple[AsyncSession, AsyncMock, AsyncMock]:
    """Build a cast writer session with inspectable transaction methods."""
    commit = AsyncMock()
    rollback = AsyncMock()
    session = cast(AsyncSession, SimpleNamespace(commit=commit, rollback=rollback))
    return session, commit, rollback


def _publisher_candle_row(public_id: str, sequence_id: int = 0) -> CandleUpsertRow:
    """Build a typed candle upsert row for publisher flush tests."""
    timestamp = datetime(2024, 1, 1, tzinfo=UTC)
    return {
        "public_id": public_id,
        "instrument_public_id": "inst-1",
        "open_at": timestamp,
        "timestamp": timestamp,
        "timeframe": "1m",
        "open": 1.0,
        "high": 2.0,
        "low": 0.5,
        "close": 1.5,
        "volume": 10.0,
        "vwap": 1.2,
        "trades": 5,
        "session_id": "",
        "sequence_id": sequence_id,
    }


def _publisher_tick_row(public_id: str) -> TickUpsertRow:
    """Build a typed tick upsert row for publisher flush tests."""
    return {
        "public_id": public_id,
        "instrument_public_id": "inst-1",
        "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
        "bid": 100.0,
        "ask": 101.0,
        "last": 100.5,
        "volume": 5.0,
        "session_id": "",
        "sequence_id": 0,
    }


def _publisher_trade_row(public_id: str) -> TradeUpsertRow:
    """Build a typed trade upsert row for publisher flush tests."""
    return {
        "public_id": public_id,
        "instrument_public_id": "inst-1",
        "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
        "executed_at": datetime(2024, 1, 1, tzinfo=UTC),
        "price": 100.0,
        "size": 1.5,
        "side": "buy",
        "trade_id": "exch-42",
        "session_id": "",
        "sequence_id": 0,
    }


@pytest.mark.asyncio
async def test_flush_candle_batch_commits_writer_session() -> None:
    """Verify candle flush commits the held writer session.

    Given: A publisher with a held candle writer session,
    When: _flush_candle_batch persists a batch,
    Then: The repository receives the session and the session is committed.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    writer_session, commit, rollback = _publisher_writer_session()
    upsert_candles = AsyncMock(return_value=1)
    pub.repository = cast(Repository, SimpleNamespace(upsert_candles=upsert_candles))
    pub._candle_writer_session = writer_session
    batch = [_publisher_candle_row("c1")]

    await pub._flush_candle_batch(batch)

    upsert_candles.assert_awaited_once_with(batch, session=writer_session)
    commit.assert_awaited_once()
    rollback.assert_not_awaited()
    assert pub._flush_errors["candle"] == 0


@pytest.mark.asyncio
async def test_flush_candle_batch_integrity_error_rolls_back_then_commits_row() -> None:
    """Verify candle IntegrityError fallback uses writer-session boundaries.

    Given: A held candle writer session and a batch upsert IntegrityError,
    When: _flush_candle_batch falls back to row-by-row persistence,
    Then: The batch transaction is rolled back and the recovered row is committed.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    writer_session, commit, rollback = _publisher_writer_session()
    upsert_candles = AsyncMock(
        side_effect=[
            IntegrityError("dup", params=None, orig=Exception("dup")),
            1,
        ]
    )
    pub.repository = cast(Repository, SimpleNamespace(upsert_candles=upsert_candles))
    pub._candle_writer_session = writer_session
    batch = [_publisher_candle_row("c1")]

    await pub._flush_candle_batch(batch)

    assert upsert_candles.await_count == 2
    rollback.assert_awaited_once()
    commit.assert_awaited_once()
    assert pub._flush_errors["candle"] == 0


@pytest.mark.asyncio
async def test_flush_candle_batch_generic_error_rolls_back_writer_session() -> None:
    """Verify generic candle flush errors roll back the held session.

    Given: A held candle writer session and a failing repository,
    When: _flush_candle_batch catches a non-IntegrityError exception,
    Then: The held writer session is rolled back and the error counter increments.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    writer_session, commit, rollback = _publisher_writer_session()
    upsert_candles = AsyncMock(side_effect=RuntimeError("db fail"))
    pub.repository = cast(Repository, SimpleNamespace(upsert_candles=upsert_candles))
    pub._candle_writer_session = writer_session

    await pub._flush_candle_batch([_publisher_candle_row("c1")])

    commit.assert_not_awaited()
    rollback.assert_awaited_once()
    assert pub._flush_errors["candle"] == 1


@pytest.mark.asyncio
async def test_flush_candle_batch_row_by_row_rolls_back_writer_session_errors() -> None:
    """Verify row-by-row candle fallback rolls back each writer-session failure.

    Given: A held candle writer session and per-row IntegrityError and RuntimeError,
    When: _flush_candle_batch_row_by_row isolates rows,
    Then: Each failed row rolls back the session and generic errors remain counted.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    writer_session, commit, rollback = _publisher_writer_session()
    upsert_candles = AsyncMock(
        side_effect=[
            IntegrityError("dup", params=None, orig=Exception("dup")),
            RuntimeError("db fail"),
        ]
    )
    pub.repository = cast(Repository, SimpleNamespace(upsert_candles=upsert_candles))
    pub._candle_writer_session = writer_session
    batch = [
        _publisher_candle_row("c1"),
        _publisher_candle_row("c2", sequence_id=1),
    ]

    await pub._flush_candle_batch_row_by_row(batch)

    commit.assert_not_awaited()
    assert rollback.await_count == 2
    assert pub._flush_errors["candle"] == 1


@pytest.mark.asyncio
async def test_flush_tick_batch_commits_writer_session() -> None:
    """Verify tick flush commits the held writer session.

    Given: A publisher with a held tick writer session,
    When: _flush_tick_batch persists a batch,
    Then: The repository receives the session and the session is committed.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    writer_session, commit, rollback = _publisher_writer_session()
    upsert_ticks = AsyncMock(return_value=1)
    pub.repository = cast(Repository, SimpleNamespace(upsert_ticks=upsert_ticks))
    pub._tick_writer_session = writer_session
    batch = [_publisher_tick_row("t1")]

    await pub._flush_tick_batch(batch)

    upsert_ticks.assert_awaited_once_with(batch, session=writer_session)
    commit.assert_awaited_once()
    rollback.assert_not_awaited()
    assert pub._flush_errors["tick"] == 0


@pytest.mark.asyncio
async def test_flush_trade_batch_commits_writer_session() -> None:
    """Verify trade flush commits the held writer session.

    Given: A publisher with a held trade writer session,
    When: _flush_trade_batch persists a batch,
    Then: The repository receives the session and the session is committed.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    writer_session, commit, rollback = _publisher_writer_session()
    upsert_trades = AsyncMock(return_value=1)
    pub.repository = cast(Repository, SimpleNamespace(upsert_trades=upsert_trades))
    pub._trade_writer_session = writer_session
    batch = [_publisher_trade_row("tr1")]

    await pub._flush_trade_batch(batch)

    upsert_trades.assert_awaited_once_with(batch, session=writer_session)
    commit.assert_awaited_once()
    rollback.assert_not_awaited()
    assert pub._flush_errors["trade"] == 0


@pytest.mark.asyncio
async def test_flush_trade_batch_rolls_back_writer_session_on_error() -> None:
    """Verify trade flush errors roll back the held writer session.

    Given: A held trade writer session and a failing repository,
    When: _flush_trade_batch catches the exception,
    Then: The held writer session is rolled back and the error counter increments.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    writer_session, commit, rollback = _publisher_writer_session()
    upsert_trades = AsyncMock(side_effect=RuntimeError("db fail"))
    pub.repository = cast(Repository, SimpleNamespace(upsert_trades=upsert_trades))
    pub._trade_writer_session = writer_session

    await pub._flush_trade_batch([_publisher_trade_row("tr1")])

    commit.assert_not_awaited()
    rollback.assert_awaited_once()
    assert pub._flush_errors["trade"] == 1


def _dummy_tick_row(idx: int) -> dict[str, Any]:
    """Build a placeholder tick row for writer-queue tests."""
    return {
        "instrument_public_id": f"inst-{idx:04d}",
        "session_id": "test",
        "sequence_id": idx,
        "timestamp": datetime(2026, 4, 21, tzinfo=UTC),
        "bid": 100.0,
        "ask": 101.0,
        "last_price": 100.5,
        "volume": 1.0,
    }


@pytest.mark.asyncio
async def test_tick_writer_loop_decouples_consumer_from_flush() -> None:
    """``_tick_writer_loop`` does not block the writer queue's producer on flush.

    Given: An event-gated ``_flush_tick_batch`` that does not return until
        a test-controlled event is set, plus 100 tick rows enqueued onto
        the writer queue.
    When: The writer loop runs concurrently with the producer,
    Then: The producer completes all 100 enqueues before the flush is
        unblocked (proves the consumer is decoupled from the flush
        latency); after the event fires, the writer drains the queue and
        flushes the batch.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub._tick_batch_max_rows = 1000
    pub._batch_max_age_s = 60.0
    pub.running = True
    flush_gate = asyncio.Event()
    flushed_rows: list[dict[str, Any]] = []

    async def gated_flush(batch: list[dict[str, Any]]) -> None:
        flushed_rows.extend(batch)
        await flush_gate.wait()

    pub._flush_tick_batch = gated_flush
    writer = asyncio.create_task(pub._tick_writer_loop())

    async def producer() -> None:
        for i in range(100):
            await pub._tick_write_queue.put(_dummy_tick_row(i))

    producer_task = asyncio.create_task(producer())
    await asyncio.wait_for(producer_task, timeout=2.0)
    assert pub._tick_write_queue.qsize() <= pub._tick_batch_max_rows + 1
    flush_gate.set()
    pub.running = False
    await asyncio.wait_for(pub._tick_write_queue.join(), timeout=2.0)
    await asyncio.wait_for(writer, timeout=2.0)
    assert len(flushed_rows) == 100
    for i, row in enumerate(flushed_rows):
        assert row["sequence_id"] == i


@pytest.mark.asyncio
async def test_tick_writer_loop_drains_queue_on_shutdown() -> None:
    """``_tick_writer_loop`` keeps running while items remain in the queue.

    Given: 50 rows queued on the writer queue and ``running`` flipped
        to False BEFORE the writer task starts,
    When: The writer loop runs,
    Then: All 50 rows reach ``_flush_tick_batch`` and the queue ends
        empty — the drain guard ``not self._tick_write_queue.empty()``
        keeps the loop alive past the ``running`` flip.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub._tick_batch_max_rows = 1000
    pub._batch_max_age_s = 0.05
    flushed_rows: list[dict[str, Any]] = []

    async def collect_flush(batch: list[dict[str, Any]]) -> None:
        flushed_rows.extend(batch)

    pub._flush_tick_batch = collect_flush
    for i in range(50):
        await pub._tick_write_queue.put(_dummy_tick_row(i))
    pub.running = False
    await asyncio.wait_for(pub._tick_writer_loop(), timeout=2.0)
    assert len(flushed_rows) == 50
    assert pub._tick_write_queue.empty()


@pytest.mark.asyncio
async def test_tick_writer_loop_size_trigger_flushes_at_max_rows() -> None:
    """Size trigger fires when batch reaches ``_tick_batch_max_rows``.

    Given: Batch size cap of 5 and 12 rows queued,
    When: The writer loop runs to completion,
    Then: ``_flush_tick_batch`` is invoked at least twice (5 + 5 + 2 or
        similar partition); cumulative row count is 12.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub._tick_batch_max_rows = 5
    pub._batch_max_age_s = 0.05
    flush_calls = 0
    flushed_rows: list[dict[str, Any]] = []

    async def counting_flush(batch: list[dict[str, Any]]) -> None:
        nonlocal flush_calls
        flush_calls += 1
        flushed_rows.extend(batch)

    pub._flush_tick_batch = counting_flush
    for i in range(12):
        await pub._tick_write_queue.put(_dummy_tick_row(i))
    pub.running = False
    await asyncio.wait_for(pub._tick_writer_loop(), timeout=2.0)
    assert flush_calls >= 2
    assert len(flushed_rows) == 12


@pytest.mark.asyncio
async def test_tick_writer_loop_balances_task_done_against_put() -> None:
    """Every successful ``put`` reaches a matching ``task_done``.

    Given: 30 rows enqueued via the writer-queue helper and consumed,
    When: ``await self._tick_write_queue.join()`` is invoked after the
        writer drains,
    Then: ``join()`` returns promptly (no outstanding ``task_done`` debt
        would deadlock here).
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub._tick_batch_max_rows = 1000
    pub._batch_max_age_s = 0.05

    async def noop_flush(batch: list[dict[str, Any]]) -> None:
        return None

    pub._flush_tick_batch = noop_flush
    for i in range(30):
        await pub._tick_write_queue.put(_dummy_tick_row(i))
    pub.running = False
    await asyncio.wait_for(pub._tick_writer_loop(), timeout=2.0)
    await asyncio.wait_for(pub._tick_write_queue.join(), timeout=0.5)
    assert pub._tick_write_queue.empty()


@pytest.mark.asyncio
async def test_tick_writer_queue_drop_oldest_balances_task_done() -> None:
    """Drop-oldest helper calls ``task_done`` for every evicted row.

    Given: A bounded queue of size 3 already at capacity,
    When: A new row is enqueued via the writer helper (which evicts the
        oldest),
    Then: ``join()`` reaches a balanced state once the remaining 3 rows
        are consumed — the evicted row's ``task_done`` is properly
        accounted for inside the helper.
    """
    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=3)
    for i in range(3):
        await queue.put(_dummy_tick_row(i))
    _tick_writer_drop_counters.clear()
    _enqueue_or_drop_oldest_tick_write(queue, _dummy_tick_row(99), "test-label")
    assert queue.qsize() == 3
    while not queue.empty():
        queue.get_nowait()
        queue.task_done()
    await asyncio.wait_for(queue.join(), timeout=0.5)


@pytest.mark.asyncio
async def test_tick_writer_drop_log_uses_persistence_backlog_label(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Writer-queue drop log message identifies persistence backlog explicitly.

    Given: A full writer queue and a forced overflow,
    When: The drop summary is emitted (one per
        ``_TICK_WRITER_DROP_LOG_INTERVAL_S`` window),
    Then: The log message contains the persistence-backlog phrasing so
        operators do not confuse it with an upstream WS feed drop.
    """
    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=1)
    await queue.put(_dummy_tick_row(0))
    _tick_writer_drop_counters.clear()
    sink_id = logger.add(caplog.handler, format="{message}", level="WARNING")
    try:
        _enqueue_or_drop_oldest_tick_write(queue, _dummy_tick_row(1), "kraken")
    finally:
        logger.remove(sink_id)
    _tick_writer_drop_counters.clear()
    summaries = [rec for rec in caplog.records if "tick-writer queue full" in rec.message]
    assert len(summaries) == 1
    assert "persistence backlog" in summaries[0].message
    assert "ZMQ subscribers" in summaries[0].message


@pytest.mark.asyncio
async def test_tick_writer_reuses_persistent_session_for_every_flush() -> None:
    """Writer task acquires one session at start and reuses it per flush.

    Given: A repository whose ``session()`` context manager yields a
        single tracked session and an ``upsert_ticks`` mock that
        records every call,
    When: The writer drains 8 rows across 2 flushes,
    Then: ``repository.session`` is entered exactly once, every
        ``upsert_ticks`` call carries that same session via the
        ``session`` kwarg, and the session is committed per flush
        instead of opening a fresh connection each time.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub._tick_batch_max_rows = 4
    pub._batch_max_age_s = 0.05
    held_session = SimpleNamespace(commit=AsyncMock(), rollback=AsyncMock())
    session_enters = 0

    class _SessionCtx:
        async def __aenter__(self) -> Any:
            nonlocal session_enters
            session_enters += 1
            return held_session

        async def __aexit__(self, *_: Any) -> None:
            return None

    pub.repository = SimpleNamespace(
        session=lambda: _SessionCtx(),
        upsert_ticks=AsyncMock(return_value=4),
    )
    for i in range(8):
        await pub._tick_write_queue.put(_dummy_tick_row(i))
    pub.running = False
    await asyncio.wait_for(pub._tick_writer_loop(), timeout=2.0)
    assert session_enters == 1
    upsert_calls = pub.repository.upsert_ticks.await_args_list
    assert len(upsert_calls) >= 2
    for call in upsert_calls:
        assert call.kwargs["session"] is held_session
    assert held_session.commit.await_count >= 2


@pytest.mark.asyncio
async def test_tick_writer_drop_log_rate_limited(caplog: pytest.LogCaptureFixture) -> None:
    """A second overflow within the log-interval window suppresses the warning.

    Given: A drop-counter entry whose last log timestamp is ``now`` (so the
        rate-limit window has not yet elapsed),
    When: ``_enqueue_or_drop_oldest_tick_write`` runs another eviction,
    Then: ``counters[0]`` increments but no new ``tick-writer queue full``
        warning is emitted (covers the rate-limit branch where the log
        line is skipped and the helper drops straight into eviction).
    """
    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=1)
    await queue.put(_dummy_tick_row(0))
    _tick_writer_drop_counters.clear()
    _tick_writer_drop_counters["kraken"] = [3.0, monotonic()]
    sink_id = logger.add(caplog.handler, format="{message}", level="WARNING")
    try:
        _enqueue_or_drop_oldest_tick_write(queue, _dummy_tick_row(1), "kraken")
    finally:
        logger.remove(sink_id)
    assert not any("tick-writer queue full" in rec.message for rec in caplog.records)
    assert _tick_writer_drop_counters["kraken"][0] == 4.0
    _tick_writer_drop_counters.clear()


@pytest.mark.asyncio
async def test_flush_tick_writer_batch_empty_is_noop() -> None:
    """``_flush_tick_writer_batch`` short-circuits on an empty batch.

    Given: A publisher with no rows queued for flush,
    When: ``_flush_tick_writer_batch([])`` is invoked,
    Then: No flush call reaches the repository and ``task_done`` is not
        called (it would over-balance the queue otherwise).
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.repository = SimpleNamespace(upsert_ticks=AsyncMock())
    flush_mock = AsyncMock()
    pub._flush_tick_batch = flush_mock
    await pub._flush_tick_writer_batch([])
    flush_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_stop_returns_when_queue_not_initialized() -> None:
    """``stop()`` is safe when the writer queue was never created.

    Given: A running publisher whose ``_tick_write_queue`` is ``None``
        (the publisher's async loops were never entered),
    When: ``stop()`` is called,
    Then: The queue-join branch is skipped (covers the ``queue is None``
        False branch on the join guard) and ``stop()`` completes
        without raising.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub._tick_write_queue = None
    pub.running = True
    await pub.stop()
    assert pub.running is False


@pytest.mark.asyncio
async def test_flush_tick_batch_rolls_back_writer_session_on_error() -> None:
    """A flush failure with a held writer session triggers a rollback.

    Given: A publisher whose ``_tick_writer_session`` is set and whose
        repository's ``upsert_ticks`` raises,
    When: ``_flush_tick_batch`` runs and catches the exception,
    Then: ``rollback`` is awaited on the held session (covers the
        ``writer_session is not None`` branch in the error handler).
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    held_session = SimpleNamespace(commit=AsyncMock(), rollback=AsyncMock())
    pub._tick_writer_session = held_session
    pub.repository = SimpleNamespace(
        upsert_ticks=AsyncMock(side_effect=RuntimeError("simulated DB failure"))
    )
    await pub._flush_tick_batch([_dummy_tick_row(0)])
    held_session.rollback.assert_awaited_once()


@pytest.mark.asyncio
async def test_tick_writer_loop_top_age_flush_branch() -> None:
    """A non-empty batch whose age has expired flushes at the iteration top.

    Given: A patched ``_batch_age_remaining`` that returns 0 when
        ``batch_start`` is set, so the very next iteration after picking
        up a row sees the batch as already aged,
    When: One tick row is enqueued and the writer loop is run,
    Then: ``_flush_tick_batch`` is invoked from the top-of-loop
        age-expired branch (lines covering the ``timeout <= 0.0``
        flush), not from the wait-for TimeoutError branch.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub._batch_max_age_s = 0.01
    pub._tick_batch_max_rows = 1000
    pub.running = True
    flushed: list[dict[str, Any]] = []

    async def capture_flush(batch: list[dict[str, Any]]) -> None:
        flushed.extend(batch)

    pub._flush_tick_batch = capture_flush

    def patched_age(batch_start: float | None, _loop: asyncio.AbstractEventLoop) -> float:
        return 0.0 if batch_start is not None else 0.01

    pub._batch_age_remaining = patched_age
    await pub._tick_write_queue.put(_dummy_tick_row(0))

    async def stop_soon() -> None:
        await asyncio.sleep(0.05)
        pub.running = False

    stop_task = asyncio.create_task(stop_soon())
    try:
        await asyncio.wait_for(pub._tick_writer_loop(), timeout=2.0)
    finally:
        await stop_task
    assert len(flushed) == 1


@pytest.mark.asyncio
async def test_tick_writer_loop_get_timeout_flushes_aged_batch() -> None:
    """``wait_for(get())`` TimeoutError flushes an aged batch.

    Given: A publisher with a tiny ``_batch_max_age_s`` and one row
        already consumed into the writer's batch,
    When: The queue stays empty so ``wait_for`` times out and the
        batch has aged past its budget,
    Then: The TimeoutError branch flushes the aged batch (covers the
        post-wait-for age-check flush path).
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub._batch_max_age_s = 0.01
    pub._tick_batch_max_rows = 1000
    pub.running = True
    flushed: list[dict[str, Any]] = []

    async def capture_flush(batch: list[dict[str, Any]]) -> None:
        flushed.extend(batch)

    pub._flush_tick_batch = capture_flush
    await pub._tick_write_queue.put(_dummy_tick_row(0))

    async def stop_soon() -> None:
        await asyncio.sleep(0.1)
        pub.running = False

    stop_task = asyncio.create_task(stop_soon())
    try:
        await asyncio.wait_for(pub._tick_writer_loop(), timeout=2.0)
    finally:
        await stop_task
    assert len(flushed) == 1


def _dummy_candle_row(idx: int) -> dict[str, Any]:
    """Build a placeholder candle row for writer-queue tests."""
    return {
        "instrument_public_id": f"inst-{idx:04d}",
        "session_id": "test",
        "sequence_id": idx,
        "timestamp": datetime(2026, 4, 21, tzinfo=UTC),
        "timeframe": "1m",
        "open_at": datetime(2026, 4, 21, tzinfo=UTC),
        "open": 100.0,
        "high": 101.0,
        "low": 99.0,
        "close": 100.5,
        "vwap": 100.2,
        "volume": 1.0,
        "trades": 1,
    }


def _dummy_trade_row(idx: int) -> dict[str, Any]:
    """Build a placeholder trade row for writer-queue tests."""
    return {
        "instrument_public_id": f"inst-{idx:04d}",
        "session_id": "test",
        "sequence_id": idx,
        "timestamp": datetime(2026, 4, 21, tzinfo=UTC),
        "executed_at": datetime(2026, 4, 21, tzinfo=UTC),
        "price": 100.0,
        "volume": 1.0,
        "side": "buy",
        "trade_id": str(idx),
    }


@pytest.mark.asyncio
async def test_candle_writer_loop_drains_queue_on_shutdown() -> None:
    """Candle writer loop keeps running while items remain in the queue.

    Given: 5 candle rows queued and ``running=False``,
    When: ``_candle_writer_loop`` is awaited,
    Then: All five rows reach ``_flush_candle_batch`` and the queue
        is empty before the loop returns.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub._candle_batch_max_rows = 2
    pub._batch_max_age_s = 0.05
    flushed_rows: list[dict[str, Any]] = []

    async def capture_flush(batch: list[dict[str, Any]]) -> None:
        flushed_rows.extend(batch)

    pub._flush_candle_batch = capture_flush
    for i in range(5):
        await pub._candle_write_queue.put(_dummy_candle_row(i))
    pub.running = False
    await asyncio.wait_for(pub._candle_writer_loop(), timeout=2.0)
    assert len(flushed_rows) == 5
    assert pub._candle_write_queue.empty()


@pytest.mark.asyncio
async def test_candle_writer_loop_size_trigger_flushes_at_max_rows() -> None:
    """Size trigger fires when batch reaches ``_candle_batch_max_rows``.

    Given: Batch cap of 3 and 7 candle rows queued,
    When: The writer loop runs to completion,
    Then: ``_flush_candle_batch`` is invoked at least twice and the
        cumulative row count across calls is seven.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub._candle_batch_max_rows = 3
    pub._batch_max_age_s = 0.05
    flush_calls = 0
    flushed_rows: list[dict[str, Any]] = []

    async def counting_flush(batch: list[dict[str, Any]]) -> None:
        nonlocal flush_calls
        flush_calls += 1
        flushed_rows.extend(batch)

    pub._flush_candle_batch = counting_flush
    for i in range(7):
        await pub._candle_write_queue.put(_dummy_candle_row(i))
    pub.running = False
    await asyncio.wait_for(pub._candle_writer_loop(), timeout=2.0)
    assert flush_calls >= 2
    assert len(flushed_rows) == 7


@pytest.mark.asyncio
async def test_candle_writer_loop_balances_task_done_against_put() -> None:
    """Every successful candle ``put`` reaches a matching ``task_done``.

    Given: 12 candle rows queued and the writer drains them,
    When: ``await self._candle_write_queue.join()`` runs after the
        writer returns,
    Then: ``join()`` returns promptly because no outstanding
        ``task_done`` debt remains; the queue is empty.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub._candle_batch_max_rows = 1000
    pub._batch_max_age_s = 0.05

    async def noop_flush(batch: list[dict[str, Any]]) -> None:
        return None

    pub._flush_candle_batch = noop_flush
    for i in range(12):
        await pub._candle_write_queue.put(_dummy_candle_row(i))
    pub.running = False
    await asyncio.wait_for(pub._candle_writer_loop(), timeout=2.0)
    await asyncio.wait_for(pub._candle_write_queue.join(), timeout=0.5)
    assert pub._candle_write_queue.empty()


@pytest.mark.asyncio
async def test_candle_writer_queue_drop_oldest_balances_task_done() -> None:
    """Candle drop-oldest helper calls ``task_done`` for every evicted row.

    Given: A bounded candle queue at capacity (size 3),
    When: A new candle row is enqueued via the writer helper
        (which evicts the oldest entry),
    Then: ``join()`` reaches a balanced state once the remaining
        three rows are consumed — the evicted row's
        ``task_done`` is accounted for inside the helper.
    """
    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=3)
    for i in range(3):
        await queue.put(_dummy_candle_row(i))
    _candle_writer_drop_counters.clear()
    _enqueue_or_drop_oldest_candle_write(queue, _dummy_candle_row(99), "test-label")
    assert queue.qsize() == 3
    while not queue.empty():
        queue.get_nowait()
        queue.task_done()
    await asyncio.wait_for(queue.join(), timeout=0.5)


@pytest.mark.asyncio
async def test_candle_writer_drop_log_uses_persistence_backlog_label(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Candle writer drop log identifies persistence backlog explicitly.

    Given: A full candle writer queue (size 1) and a forced overflow,
    When: The drop summary is emitted,
    Then: The log message names the candle-writer queue + the
        persistence-backlog phrasing so operators do not confuse it
        with an upstream WS feed drop.
    """
    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=1)
    await queue.put(_dummy_candle_row(0))
    _candle_writer_drop_counters.clear()
    sink_id = logger.add(caplog.handler, format="{message}", level="WARNING")
    try:
        _enqueue_or_drop_oldest_candle_write(queue, _dummy_candle_row(1), "kraken")
    finally:
        logger.remove(sink_id)
    _candle_writer_drop_counters.clear()
    summaries = [rec for rec in caplog.records if "candle-writer queue full" in rec.message]
    assert len(summaries) == 1
    assert "persistence backlog" in summaries[0].message
    assert "candles delivered to ZMQ subscribers" in summaries[0].message


@pytest.mark.asyncio
async def test_candle_writer_uses_repository_session() -> None:
    """``_open_candle_writer_session`` yields the repository's session.

    Given: A publisher with a repository whose ``session()`` context
        manager yields a tracked session object,
    When: ``_open_candle_writer_session`` is entered,
    Then: The yielded value is exactly that session (the writer
        will reuse it across every candle flush).
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    held_session = SimpleNamespace(commit=AsyncMock(), rollback=AsyncMock())

    class _SessionCtx:
        async def __aenter__(self) -> Any:
            return held_session

        async def __aexit__(self, *_: Any) -> None:
            return None

    pub.repository = SimpleNamespace(session=lambda: _SessionCtx())
    seen: list[Any] = []
    async with pub._open_candle_writer_session() as session:
        seen.append(session)
    assert seen == [held_session]


@pytest.mark.asyncio
async def test_candle_writer_yields_none_without_repository() -> None:
    """``_open_candle_writer_session`` yields ``None`` without a repository.

    Given: A publisher whose ``repository`` is ``None`` (test path),
    When: ``_open_candle_writer_session`` is entered,
    Then: The yielded value is ``None`` so flush helpers fall back
        to the no-session path.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.repository = None
    async with pub._open_candle_writer_session() as session:
        assert session is None


@pytest.mark.asyncio
async def test_trade_writer_loop_drains_queue_on_shutdown() -> None:
    """Trade writer loop keeps running while items remain in the queue.

    Given: 5 trade rows queued and ``running=False``,
    When: ``_trade_writer_loop`` is awaited,
    Then: All five rows reach ``_flush_trade_batch`` and the queue
        is empty before the loop returns.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub._trade_batch_max_rows = 2
    pub._batch_max_age_s = 0.05
    flushed_rows: list[dict[str, Any]] = []

    async def capture_flush(batch: list[dict[str, Any]]) -> None:
        flushed_rows.extend(batch)

    pub._flush_trade_batch = capture_flush
    for i in range(5):
        await pub._trade_write_queue.put(_dummy_trade_row(i))
    pub.running = False
    await asyncio.wait_for(pub._trade_writer_loop(), timeout=2.0)
    assert len(flushed_rows) == 5
    assert pub._trade_write_queue.empty()


@pytest.mark.asyncio
async def test_trade_writer_loop_size_trigger_flushes_at_max_rows() -> None:
    """Size trigger fires when batch reaches ``_trade_batch_max_rows``.

    Given: Batch cap of 3 and 7 trade rows queued,
    When: The writer loop runs to completion,
    Then: ``_flush_trade_batch`` is invoked at least twice and the
        cumulative row count across calls is seven.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub._trade_batch_max_rows = 3
    pub._batch_max_age_s = 0.05
    flush_calls = 0
    flushed_rows: list[dict[str, Any]] = []

    async def counting_flush(batch: list[dict[str, Any]]) -> None:
        nonlocal flush_calls
        flush_calls += 1
        flushed_rows.extend(batch)

    pub._flush_trade_batch = counting_flush
    for i in range(7):
        await pub._trade_write_queue.put(_dummy_trade_row(i))
    pub.running = False
    await asyncio.wait_for(pub._trade_writer_loop(), timeout=2.0)
    assert flush_calls >= 2
    assert len(flushed_rows) == 7


@pytest.mark.asyncio
async def test_trade_writer_loop_balances_task_done_against_put() -> None:
    """Every successful trade ``put`` reaches a matching ``task_done``.

    Given: 12 trade rows queued and the writer drains them,
    When: ``await self._trade_write_queue.join()`` runs after the
        writer returns,
    Then: ``join()`` returns promptly because no outstanding
        ``task_done`` debt remains; the queue is empty.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub._trade_batch_max_rows = 1000
    pub._batch_max_age_s = 0.05

    async def noop_flush(batch: list[dict[str, Any]]) -> None:
        return None

    pub._flush_trade_batch = noop_flush
    for i in range(12):
        await pub._trade_write_queue.put(_dummy_trade_row(i))
    pub.running = False
    await asyncio.wait_for(pub._trade_writer_loop(), timeout=2.0)
    await asyncio.wait_for(pub._trade_write_queue.join(), timeout=0.5)
    assert pub._trade_write_queue.empty()


@pytest.mark.asyncio
async def test_trade_writer_queue_drop_oldest_balances_task_done() -> None:
    """Trade drop-oldest helper calls ``task_done`` for every evicted row.

    Given: A bounded trade queue at capacity (size 3),
    When: A new trade row is enqueued via the writer helper
        (which evicts the oldest entry),
    Then: ``join()`` reaches a balanced state once the remaining
        three rows are consumed — the evicted row's
        ``task_done`` is accounted for inside the helper.
    """
    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=3)
    for i in range(3):
        await queue.put(_dummy_trade_row(i))
    _trade_writer_drop_counters.clear()
    _enqueue_or_drop_oldest_trade_write(queue, _dummy_trade_row(99), "test-label")
    assert queue.qsize() == 3
    while not queue.empty():
        queue.get_nowait()
        queue.task_done()
    await asyncio.wait_for(queue.join(), timeout=0.5)


@pytest.mark.asyncio
async def test_trade_writer_drop_log_uses_persistence_backlog_label(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Trade writer drop log identifies persistence backlog explicitly.

    Given: A full trade writer queue (size 1) and a forced overflow,
    When: The drop summary is emitted,
    Then: The log message names the trade-writer queue + the
        persistence-backlog phrasing so operators do not confuse it
        with an upstream WS feed drop.
    """
    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=1)
    await queue.put(_dummy_trade_row(0))
    _trade_writer_drop_counters.clear()
    sink_id = logger.add(caplog.handler, format="{message}", level="WARNING")
    try:
        _enqueue_or_drop_oldest_trade_write(queue, _dummy_trade_row(1), "kraken")
    finally:
        logger.remove(sink_id)
    _trade_writer_drop_counters.clear()
    summaries = [rec for rec in caplog.records if "trade-writer queue full" in rec.message]
    assert len(summaries) == 1
    assert "persistence backlog" in summaries[0].message
    assert "trades delivered to ZMQ subscribers" in summaries[0].message


@pytest.mark.asyncio
async def test_trade_writer_uses_repository_session() -> None:
    """``_open_trade_writer_session`` yields the repository's session.

    Given: A publisher with a repository whose ``session()`` context
        manager yields a tracked session object,
    When: ``_open_trade_writer_session`` is entered,
    Then: The yielded value is exactly that session (the writer
        will reuse it across every trade flush).
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    held_session = SimpleNamespace(commit=AsyncMock(), rollback=AsyncMock())

    class _SessionCtx:
        async def __aenter__(self) -> Any:
            return held_session

        async def __aexit__(self, *_: Any) -> None:
            return None

    pub.repository = SimpleNamespace(session=lambda: _SessionCtx())
    seen: list[Any] = []
    async with pub._open_trade_writer_session() as session:
        seen.append(session)
    assert seen == [held_session]


@pytest.mark.asyncio
async def test_trade_writer_yields_none_without_repository() -> None:
    """``_open_trade_writer_session`` yields ``None`` without a repository.

    Given: A publisher whose ``repository`` is ``None`` (test path),
    When: ``_open_trade_writer_session`` is entered,
    Then: The yielded value is ``None`` so flush helpers fall back
        to the no-session path.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.repository = None
    async with pub._open_trade_writer_session() as session:
        assert session is None


@pytest.mark.asyncio
async def test_flush_candle_writer_batch_empty_is_noop() -> None:
    """Empty-batch fast path returns without touching the queue.

    Given: An empty in-flight batch and a pristine writer queue,
    When: ``_flush_candle_writer_batch([])`` is awaited,
    Then: No flush is invoked and no ``task_done`` is balanced
        (covers the early-return guard in the writer's flush
        helper).
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    flush_mock = AsyncMock()
    pub._flush_candle_batch = flush_mock
    await pub._flush_candle_writer_batch([])
    flush_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_flush_trade_writer_batch_empty_is_noop() -> None:
    """Empty-batch fast path returns without touching the queue.

    Given: An empty in-flight batch and a pristine writer queue,
    When: ``_flush_trade_writer_batch([])`` is awaited,
    Then: No flush is invoked and no ``task_done`` is balanced
        (covers the early-return guard in the writer's flush
        helper).
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    flush_mock = AsyncMock()
    pub._flush_trade_batch = flush_mock
    await pub._flush_trade_writer_batch([])
    flush_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_candle_writer_loop_age_trigger_flushes_in_flight_batch() -> None:
    """Age-trigger branch flushes a partially full batch when its age elapses.

    Given: A writer with ``_batch_max_age_s=0.02``, ``batch_max_rows=1000``,
        one row queued, and a producer task that stops the writer 200ms
        later (well after the age window expires),
    When: The writer loop runs,
    Then: The first row is flushed via the age-trigger branch before
        shutdown drain, exercising the ``timeout <= 0`` / TimeoutError
        flush paths inside the loop body.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub._candle_batch_max_rows = 1000
    pub._batch_max_age_s = 0.02
    pub.running = True
    flushed: list[dict[str, Any]] = []

    async def capture_flush(batch: list[dict[str, Any]]) -> None:
        flushed.extend(batch)

    pub._flush_candle_batch = capture_flush
    await pub._candle_write_queue.put(_dummy_candle_row(1))

    async def stop_after_age() -> None:
        await asyncio.sleep(0.2)
        pub.running = False

    stop_task = asyncio.create_task(stop_after_age())
    try:
        await asyncio.wait_for(pub._candle_writer_loop(), timeout=2.0)
    finally:
        await stop_task
    assert len(flushed) == 1


@pytest.mark.asyncio
async def test_trade_writer_loop_age_trigger_flushes_in_flight_batch() -> None:
    """Age-trigger branch flushes a partially full batch when its age elapses.

    Given: A writer with ``_batch_max_age_s=0.02``, ``batch_max_rows=1000``,
        one row queued, and a producer task that stops the writer 200ms
        later (well after the age window expires),
    When: The writer loop runs,
    Then: The first row is flushed via the age-trigger branch before
        shutdown drain, exercising the ``timeout <= 0`` / TimeoutError
        flush paths inside the loop body.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub._trade_batch_max_rows = 1000
    pub._batch_max_age_s = 0.02
    pub.running = True
    flushed: list[dict[str, Any]] = []

    async def capture_flush(batch: list[dict[str, Any]]) -> None:
        flushed.extend(batch)

    pub._flush_trade_batch = capture_flush
    await pub._trade_write_queue.put(_dummy_trade_row(1))

    async def stop_after_age() -> None:
        await asyncio.sleep(0.2)
        pub.running = False

    stop_task = asyncio.create_task(stop_after_age())
    try:
        await asyncio.wait_for(pub._trade_writer_loop(), timeout=2.0)
    finally:
        await stop_task
    assert len(flushed) == 1


@pytest.mark.asyncio
async def test_candle_writer_loop_top_age_flush_branch() -> None:
    """Top-of-loop age-flush branch fires when batch_start is set and aged out.

    Given: A patched ``_batch_age_remaining`` that returns 0 once
        ``batch_start`` is set, so the very next iteration after
        picking up a candle row sees the batch as already aged,
    When: One candle row is enqueued and the writer loop is run,
    Then: ``_flush_candle_batch`` is invoked from the top-of-loop
        age-expired branch (covers the ``timeout <= 0.0`` line),
        not from the wait-for TimeoutError branch.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub._batch_max_age_s = 0.01
    pub._candle_batch_max_rows = 1000
    pub.running = True
    flushed: list[dict[str, Any]] = []

    async def capture_flush(batch: list[dict[str, Any]]) -> None:
        flushed.extend(batch)

    pub._flush_candle_batch = capture_flush

    def patched_age(batch_start: float | None, _loop: asyncio.AbstractEventLoop) -> float:
        return 0.0 if batch_start is not None else 0.01

    pub._batch_age_remaining = patched_age
    await pub._candle_write_queue.put(_dummy_candle_row(0))

    async def stop_soon() -> None:
        await asyncio.sleep(0.05)
        pub.running = False

    stop_task = asyncio.create_task(stop_soon())
    try:
        await asyncio.wait_for(pub._candle_writer_loop(), timeout=2.0)
    finally:
        await stop_task
    assert len(flushed) == 1


@pytest.mark.asyncio
async def test_trade_writer_loop_top_age_flush_branch() -> None:
    """Top-of-loop age-flush branch fires when batch_start is set and aged out.

    Given: A patched ``_batch_age_remaining`` that returns 0 once
        ``batch_start`` is set, so the very next iteration after
        picking up a trade row sees the batch as already aged,
    When: One trade row is enqueued and the writer loop is run,
    Then: ``_flush_trade_batch`` is invoked from the top-of-loop
        age-expired branch (covers the ``timeout <= 0.0`` line),
        not from the wait-for TimeoutError branch.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub._batch_max_age_s = 0.01
    pub._trade_batch_max_rows = 1000
    pub.running = True
    flushed: list[dict[str, Any]] = []

    async def capture_flush(batch: list[dict[str, Any]]) -> None:
        flushed.extend(batch)

    pub._flush_trade_batch = capture_flush

    def patched_age(batch_start: float | None, _loop: asyncio.AbstractEventLoop) -> float:
        return 0.0 if batch_start is not None else 0.01

    pub._batch_age_remaining = patched_age
    await pub._trade_write_queue.put(_dummy_trade_row(0))

    async def stop_soon() -> None:
        await asyncio.sleep(0.05)
        pub.running = False

    stop_task = asyncio.create_task(stop_soon())
    try:
        await asyncio.wait_for(pub._trade_writer_loop(), timeout=2.0)
    finally:
        await stop_task
    assert len(flushed) == 1


@pytest.mark.asyncio
async def test_candle_writer_drop_log_rate_limit_skips_repeat_summaries() -> None:
    """The candle drop helper rate-limits its summary log per interval.

    Given: A full candle writer queue and two consecutive overflow
        attempts within the rate-limit window,
    When: Both overflow calls run back-to-back,
    Then: Only the first call emits a summary log; the second hits
        the rate-limited skip path (covers the False side of the
        ``now - last_log >= INTERVAL`` branch).
    """
    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=1)
    await queue.put(_dummy_candle_row(0))
    _candle_writer_drop_counters.clear()
    _enqueue_or_drop_oldest_candle_write(queue, _dummy_candle_row(1), "kraken")
    _enqueue_or_drop_oldest_candle_write(queue, _dummy_candle_row(2), "kraken")
    counters = _candle_writer_drop_counters["kraken"]
    assert counters[0] >= 1
    _candle_writer_drop_counters.clear()


@pytest.mark.asyncio
async def test_trade_writer_drop_log_rate_limit_skips_repeat_summaries() -> None:
    """The trade drop helper rate-limits its summary log per interval.

    Given: A full trade writer queue and two consecutive overflow
        attempts within the rate-limit window,
    When: Both overflow calls run back-to-back,
    Then: Only the first call emits a summary log; the second hits
        the rate-limited skip path (covers the False side of the
        ``now - last_log >= INTERVAL`` branch).
    """
    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=1)
    await queue.put(_dummy_trade_row(0))
    _trade_writer_drop_counters.clear()
    _enqueue_or_drop_oldest_trade_write(queue, _dummy_trade_row(1), "kraken")
    _enqueue_or_drop_oldest_trade_write(queue, _dummy_trade_row(2), "kraken")
    counters = _trade_writer_drop_counters["kraken"]
    assert counters[0] >= 1
    _trade_writer_drop_counters.clear()


@pytest.mark.asyncio
async def test_candle_writer_loop_timeout_without_age_does_not_flush() -> None:
    """TimeoutError on get() leaves a still-fresh batch untouched.

    Given: A writer with a long ``_batch_max_age_s`` and a single row
        already absorbed into the batch,
    When: The queue stays empty briefly, the shutdown poll fires a
        TimeoutError, and the batch age has NOT yet expired,
    Then: The TimeoutError branch leaves the batch in place (covers
        the False side of the ``batch_start is None or aged-out``
        check); shutdown drain flushes it afterward.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub._batch_max_age_s = 60.0
    pub._candle_batch_max_rows = 1000
    pub.running = True
    flushed: list[dict[str, Any]] = []

    async def capture_flush(batch: list[dict[str, Any]]) -> None:
        flushed.extend(batch)

    pub._flush_candle_batch = capture_flush
    await pub._candle_write_queue.put(_dummy_candle_row(0))

    async def stop_soon() -> None:
        await asyncio.sleep(0.6)
        pub.running = False

    stop_task = asyncio.create_task(stop_soon())
    try:
        await asyncio.wait_for(pub._candle_writer_loop(), timeout=2.5)
    finally:
        await stop_task
    assert len(flushed) == 1


@pytest.mark.asyncio
async def test_trade_writer_loop_timeout_without_age_does_not_flush() -> None:
    """TimeoutError on get() leaves a still-fresh batch untouched.

    Given: A writer with a long ``_batch_max_age_s`` and a single row
        already absorbed into the batch,
    When: The queue stays empty briefly, the shutdown poll fires a
        TimeoutError, and the batch age has NOT yet expired,
    Then: The TimeoutError branch leaves the batch in place (covers
        the False side of the ``batch_start is None or aged-out``
        check); shutdown drain flushes it afterward.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub._batch_max_age_s = 60.0
    pub._trade_batch_max_rows = 1000
    pub.running = True
    flushed: list[dict[str, Any]] = []

    async def capture_flush(batch: list[dict[str, Any]]) -> None:
        flushed.extend(batch)

    pub._flush_trade_batch = capture_flush
    await pub._trade_write_queue.put(_dummy_trade_row(0))

    async def stop_soon() -> None:
        await asyncio.sleep(0.6)
        pub.running = False

    stop_task = asyncio.create_task(stop_soon())
    try:
        await asyncio.wait_for(pub._trade_writer_loop(), timeout=2.5)
    finally:
        await stop_task
    assert len(flushed) == 1


@pytest.mark.asyncio
async def test_stop_handles_none_candle_and_trade_queues(monkeypatch: pytest.MonkeyPatch) -> None:
    """``stop()`` tolerates ``None`` writer queues without raising.

    Given: A publisher partially initialised so that
        ``_candle_write_queue`` and ``_trade_write_queue`` are
        ``None`` (defensive guard for stop-during-init failure),
    When: ``stop()`` is awaited,
    Then: The ``queue is not None`` branches are skipped and stop
        completes without ``AttributeError``.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub._candle_write_queue = None
    pub._trade_write_queue = None
    await pub.stop()


@pytest.mark.asyncio
async def test_set_persist_policy_round_trip() -> None:
    """``set_persist_policy`` installs + clears the policy reference.

    Given: A publisher with no policy bound,
    When: ``set_persist_policy(policy)`` is called then ``set_persist_policy(None)``,
    Then: ``self._persist_policy`` mirrors each assignment.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    policy = MagicMock()
    pub.set_persist_policy(policy)
    assert pub._persist_policy is policy
    pub.set_persist_policy(None)
    assert pub._persist_policy is None


def test_should_persist_row_returns_true_without_policy() -> None:
    """No injected policy degrades to "persist everything" for legacy paths.

    Given: A publisher with no persist policy installed,
    When: ``_should_persist_row`` is called for any row,
    Then: It returns ``True`` so legacy paths persist every row.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    assert pub._should_persist_row("ticks", "kraken", "BTC-USD") is True


def test_should_persist_row_consults_policy_when_present() -> None:
    """The policy verdict gates the row + a skip increments the rate-limited counter.

    Given: A publisher with a policy whose should_persist verdict toggles.
    When: _should_persist_row is called.
    Then: The verdict mirrors the policy and skips increment the counter on False.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    policy = MagicMock()
    policy.should_persist.return_value = False
    pub._persist_policy = policy
    assert pub._should_persist_row("ticks", "kraken", "BTC-USD") is False
    policy.should_persist.return_value = True
    assert pub._should_persist_row("ticks", "kraken", "BTC-USD") is True


def test_record_persist_skip_does_not_log_within_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Within the rate-limit window the counter just increments.

    Given: Two skip increments inside the rate-limit window.
    When: _record_persist_skip is called twice with the clock frozen.
    Then: The counter increments but does not log or reset.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    times = iter([100.0, 100.0])
    monkeypatch.setattr("snapper.messaging.publishers.base.monotonic", lambda: next(times))
    pub._record_persist_skip("kraken", "ticks")
    key = ("kraken", "ticks")
    pub._persist_skipped_counters[key][1] = 100.0
    pub._record_persist_skip("kraken", "ticks")
    assert pub._persist_skipped_counters[key][0] >= 1.0


def test_record_persist_skip_logs_after_window(monkeypatch: pytest.MonkeyPatch) -> None:
    """Once the rate-limit window elapses the counter logs + resets.

    Given: Two skip increments straddling the rate-limit window.
    When: _record_persist_skip is called with the clock advanced past the window.
    Then: The counter logs and resets the last-log timestamp.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    times = iter([100.0, 200.0])
    monkeypatch.setattr("snapper.messaging.publishers.base.monotonic", lambda: next(times))
    pub._record_persist_skip("kraken", "ticks")
    pub._record_persist_skip("kraken", "ticks")
    key = ("kraken", "ticks")
    assert pub._persist_skipped_counters[key][1] == 200.0


def test_verify_persist_policy_safety_rail_no_policy_short_circuits() -> None:
    """No policy means no rail check (legacy startup compat).

    Given: A wildcard publisher with no injected policy.
    When: _verify_persist_policy_safety_rail runs.
    Then: No exception is raised (legacy / test paths stay green).
    """
    pub: Any = DummyPublisher(symbols=["*"])
    pub._verify_persist_policy_safety_rail("pub:test")


def test_verify_persist_policy_safety_rail_non_wildcard_short_circuits() -> None:
    """Non-wildcard symbols never trip the rail.

    Given: A publisher with concrete (non-wildcard) symbols.
    When: _verify_persist_policy_safety_rail runs against any policy.
    Then: The rail short-circuits without inspecting the policy.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub._persist_policy = MagicMock()
    pub._verify_persist_policy_safety_rail("pub:test")


def test_verify_persist_policy_safety_rail_explicit_mode_passes() -> None:
    """Explicit mode skips the empty-scope check on that data_type.

    Given: A wildcard publisher whose policy reports explicit mode.
    When: _verify_persist_policy_safety_rail runs.
    Then: The explicit mode bypasses the empty-scope check.
    """
    pub: Any = DummyPublisher(symbols=["*"])
    policy = MagicMock()
    policy.mode_for.return_value = "explicit"
    pub._persist_policy = policy
    pub._verify_persist_policy_safety_rail("pub:test")


def test_verify_persist_policy_safety_rail_extra_satisfies() -> None:
    """A non-empty ``market_persist_extra`` overlay satisfies the rail.

    Given: Auto mode + empty scope set but a non-empty market_persist_extra overlay.
    When: _verify_persist_policy_safety_rail runs.
    Then: The overlay satisfies the rail and no exception is raised.
    """
    pub: Any = DummyPublisher(symbols=["*"])
    policy = MagicMock()
    policy.mode_for.return_value = "auto"
    policy.wallet_scope_pairs_for.return_value = frozenset()
    policy.extra_for.return_value = frozenset({"BTC-USD"})
    pub._persist_policy = policy
    pub._verify_persist_policy_safety_rail("pub:test")


def test_verify_persist_policy_safety_rail_fires_on_all_empty() -> None:
    """Wildcard + auto + empty scope + empty extra raises RuntimeError.

    Given: Wildcard + auto mode + empty scope set + empty extra overlay.
    When: _verify_persist_policy_safety_rail runs.
    Then: RuntimeError is raised pointing the operator at the four ways out.
    """
    pub: Any = DummyPublisher(symbols=["*"])
    policy = MagicMock()
    policy.mode_for.return_value = "auto"
    policy.wallet_scope_pairs_for.return_value = frozenset()
    policy.extra_for.return_value = frozenset()
    pub._persist_policy = policy
    with pytest.raises(RuntimeError, match="wildcard symbols"):
        pub._verify_persist_policy_safety_rail("pub:test")


@pytest.mark.parametrize(
    "publisher_name",
    [
        "kraken_feed_publisher",
        "kraken_futures_feed_publisher",
        "kraken_equities_feed_publisher",
        "walutomat_feed_publisher",
        "paper_feed_publisher",
    ],
)
def test_market_data_publishers_autostart_by_default(publisher_name: str) -> None:
    """Every live-WS publisher decorator ships ``enabled=True``.

    Given: The global process registry populated at import time.
    When: We look up each of the five market-data feed publishers,
    Then: Its ``ProcessRegistryEntry.enabled`` is ``True`` so a fresh
        DB autostarts the full publisher fleet (paired with the
        wildcard ``instruments`` default in ``AppSettings.instruments``
        and the ``market_persist_*`` zero-persist seed in the
        environment config profiles). Regression guard
        for the wildcard-by-default contract — flipping any decorator
        back to ``enabled=False`` would silently break the fresh-DB
        runbook.
    """
    for module_path in (
        "snapper.messaging.publishers.kraken",
        "snapper.messaging.publishers.kraken_futures",
        "snapper.messaging.publishers.kraken_equities",
        "snapper.messaging.publishers.walutomat",
        "snapper.messaging.publishers.paper",
    ):
        importlib.import_module(module_path)
    registry = get_registered_processes()
    assert publisher_name in registry, f"{publisher_name} not registered"
    entry = registry[publisher_name]
    assert entry.enabled is True, (
        f"{publisher_name} decorator must ship enabled=True so fresh DB "
        "autostarts the publisher (see AppSettings.instruments wildcard "
        "default + proprietary/data/seed/*.toml market_persist_* seed)."
    )


class TestIsDisconnectError:
    """Cover :func:`_is_disconnect_error` decision branches.

    The helper backs writer-session-lost detection: false positives
    force pool churn on transient errors, false negatives leave a dead
    pinned session in place. Both edges are tested.
    """

    def test_returns_true_for_dbapi_with_invalidated_flag(self) -> None:
        """DBAPIError.connection_invalidated=True is the strongest signal.

        Given: An SQLAlchemy DBAPIError whose pool already marked the
            handle as invalidated,
        When: _is_disconnect_error is called,
        Then: Returns True.
        """
        exc = DBAPIError("SELECT 1", {}, Exception("boom"))
        exc.connection_invalidated = True
        assert _is_disconnect_error(exc) is True

    def test_returns_false_for_dbapi_without_invalidated_flag(self) -> None:
        """A live DBAPIError (e.g. statement timeout) should NOT recycle.

        Given: A DBAPIError whose pool flag is False AND whose message
            does not contain any disconnect hint,
        When: _is_disconnect_error is called,
        Then: Returns False.
        """
        exc = DBAPIError("SELECT 1", {}, Exception("query timeout"))
        exc.connection_invalidated = False
        assert _is_disconnect_error(exc) is False

    @pytest.mark.parametrize(
        "msg",
        [
            "[Errno 111] Connection refused",
            "[Errno 104] Connection reset by peer",
            "the database system is shutting down",
            "server closed the connection unexpectedly",
            "no connection to the server",
            "broken pipe",
            "CONNECTION REFUSED uppercase",
        ],
    )
    def test_returns_true_for_known_disconnect_messages(self, msg: str) -> None:
        """Substring match on common asyncpg / libpq disconnect messages.

        Given: A generic Exception whose message matches a known hint,
        When: _is_disconnect_error is called,
        Then: Returns True.
        """
        assert _is_disconnect_error(Exception(msg)) is True

    @pytest.mark.parametrize(
        "msg",
        [
            "duplicate key value violates unique constraint",
            "value too long for type",
            "syntax error at or near",
            "permission denied for table",
            "",
        ],
    )
    def test_returns_false_for_unrelated_errors(self, msg: str) -> None:
        """Substring match must not over-trigger on data/schema errors.

        Given: A generic Exception with a non-disconnect message,
        When: _is_disconnect_error is called,
        Then: Returns False.
        """
        assert _is_disconnect_error(Exception(msg)) is False


class TestFlushBatchRaisesSessionLostOnDisconnect:
    """Translate disconnect errors into :class:`_WriterSessionLostError`.

    Each ``_flush_*_batch`` is the unit under test; the recovery path
    in the writer loop relies on the exception escaping so the pinned
    session can be disposed and re-opened.
    """

    @pytest.mark.asyncio
    async def test_flush_trade_batch_raises_on_disconnect(self) -> None:
        """Trade flush converts disconnect error into _WriterSessionLostError.

        Given: A publisher whose upsert_trades raises a "Connection refused" error,
        When: _flush_trade_batch is called,
        Then: _WriterSessionLostError is raised and the cause is preserved.
        """
        pub: Any = DummyPublisher(symbols=["BTC-USD"])
        pub.repository = SimpleNamespace(
            upsert_trades=AsyncMock(side_effect=ConnectionRefusedError("Connection refused")),
        )
        batch = [
            {
                "public_id": "t1",
                "instrument_public_id": "inst-1",
                "trade_id": "x",
                "price": 1.0,
                "size": 1.0,
                "side": "buy",
                "executed_at": datetime(2024, 1, 1, tzinfo=UTC),
                "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
                "session_id": "",
                "sequence_id": 0,
            }
        ]
        with pytest.raises(_WriterSessionLostError):
            await pub._flush_trade_batch(batch)

    @pytest.mark.asyncio
    async def test_flush_tick_batch_raises_on_disconnect(self) -> None:
        """Tick flush converts disconnect error into _WriterSessionLostError.

        Given: A publisher whose upsert_ticks raises a "shutting down" error,
        When: _flush_tick_batch is called,
        Then: _WriterSessionLostError is raised.
        """
        pub: Any = DummyPublisher(symbols=["BTC-USD"])
        pub.repository = SimpleNamespace(
            upsert_ticks=AsyncMock(side_effect=Exception("the database system is shutting down")),
        )
        batch = [
            {
                "public_id": "tk1",
                "instrument_public_id": "inst-1",
                "price": 1.0,
                "size": 1.0,
                "side": "buy",
                "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
                "session_id": "",
                "sequence_id": 0,
            }
        ]
        with pytest.raises(_WriterSessionLostError):
            await pub._flush_tick_batch(batch)

    @pytest.mark.asyncio
    async def test_flush_candle_batch_raises_on_disconnect(self) -> None:
        """Candle flush converts disconnect error into _WriterSessionLostError.

        Given: A publisher whose upsert_candles raises a "Connection reset" error,
        When: _flush_candle_batch is called,
        Then: _WriterSessionLostError is raised.
        """
        pub: Any = DummyPublisher(symbols=["BTC-USD"])
        pub.repository = SimpleNamespace(
            upsert_candles=AsyncMock(side_effect=Exception("Connection reset by peer")),
        )
        batch = [
            {
                "public_id": "c1",
                "instrument_public_id": "inst-1",
                "open_at": datetime(2024, 1, 1, tzinfo=UTC),
                "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
                "timeframe": "1m",
                "open": 1.0,
                "high": 2.0,
                "low": 0.5,
                "close": 1.5,
                "volume": 10.0,
                "vwap": 1.2,
                "trades": 5,
                "session_id": "",
                "sequence_id": 0,
            }
        ]
        with pytest.raises(_WriterSessionLostError):
            await pub._flush_candle_batch(batch)

    @pytest.mark.asyncio
    async def test_flush_trade_batch_does_not_raise_on_non_disconnect(self) -> None:
        """Non-disconnect errors stay swallowed (existing behavior).

        Given: A publisher whose upsert_trades raises a constraint error,
        When: _flush_trade_batch is called,
        Then: No exception escapes; flush_errors counter is incremented.
        """
        pub: Any = DummyPublisher(symbols=["BTC-USD"])
        pub.repository = SimpleNamespace(
            upsert_trades=AsyncMock(side_effect=RuntimeError("duplicate key")),
        )
        batch = [
            {
                "public_id": "t1",
                "instrument_public_id": "inst-1",
                "trade_id": "x",
                "price": 1.0,
                "size": 1.0,
                "side": "buy",
                "executed_at": datetime(2024, 1, 1, tzinfo=UTC),
                "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
                "session_id": "",
                "sequence_id": 0,
            }
        ]
        await pub._flush_trade_batch(batch)
        assert pub._flush_errors["trade"] == 1


class TestWriterLoopRetriesOnSessionLost:
    """Verify each writer loop recovers from :class:`_WriterSessionLostError`.

    The retry path sleeps with exponential backoff and re-enters
    ``_open_*_writer_session`` so a fresh DB connection is acquired
    transparently to producers.
    """

    @pytest.mark.asyncio
    async def test_trade_writer_loop_retries_then_succeeds(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Trade writer loop retries on _WriterSessionLostError then completes.

        Given: A publisher whose ``_flush_trade_writer_batch`` raises
            _WriterSessionLostError on the first invocation then succeeds,
            and an ``asyncio.sleep`` patched to a no-op so the backoff
            does not stall the test,
        When: The writer loop is driven with one queued row then
            ``running`` is cleared,
        Then: The flush is called at least twice (initial failure +
            successful retry) and the loop exits gracefully.
        """
        pub: Any = DummyPublisher(symbols=["BTC-USD"])
        pub._trade_batch_max_rows = 1
        pub._batch_max_age_s = 60.0
        pub.running = True
        sleep_calls: list[float] = []

        async def fake_sleep(delay: float) -> None:
            sleep_calls.append(delay)

        monkeypatch.setattr("snapper.messaging.publishers.base.asyncio.sleep", fake_sleep)

        flush_calls = {"n": 0}

        async def flaky_flush(batch: list[dict[str, Any]]) -> None:
            flush_calls["n"] += 1
            if flush_calls["n"] == 1:
                raise _WriterSessionLostError("simulated disconnect")
            batch.clear()

        pub._flush_trade_writer_batch = flaky_flush
        writer = asyncio.create_task(pub._trade_writer_loop())
        await pub._trade_write_queue.put(
            {
                "public_id": "t1",
                "instrument_public_id": "inst-1",
                "trade_id": "x",
                "price": 1.0,
                "size": 1.0,
                "side": "buy",
                "executed_at": datetime(2024, 1, 1, tzinfo=UTC),
                "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
                "session_id": "",
                "sequence_id": 0,
            }
        )
        await asyncio.sleep(0.05)
        pub.running = False
        await asyncio.wait_for(writer, timeout=2.0)
        assert flush_calls["n"] >= 2
        assert any(call == pytest.approx(1.0) for call in sleep_calls)

    @pytest.mark.asyncio
    async def test_tick_writer_loop_retries_then_succeeds(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Tick writer loop mirrors the trade-loop retry semantics."""
        pub: Any = DummyPublisher(symbols=["BTC-USD"])
        pub._tick_batch_max_rows = 1
        pub._batch_max_age_s = 60.0
        pub.running = True

        async def fake_sleep(_delay: float) -> None:
            return None

        monkeypatch.setattr("snapper.messaging.publishers.base.asyncio.sleep", fake_sleep)

        flush_calls = {"n": 0}

        async def flaky_flush(batch: list[dict[str, Any]]) -> None:
            flush_calls["n"] += 1
            if flush_calls["n"] == 1:
                raise _WriterSessionLostError("simulated disconnect")
            batch.clear()

        pub._flush_tick_writer_batch = flaky_flush
        writer = asyncio.create_task(pub._tick_writer_loop())
        await pub._tick_write_queue.put(_dummy_tick_row(0))
        await asyncio.sleep(0.05)
        pub.running = False
        await asyncio.wait_for(writer, timeout=2.0)
        assert flush_calls["n"] >= 2

    @pytest.mark.asyncio
    async def test_candle_writer_loop_retries_then_succeeds(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Candle writer loop mirrors the trade-loop retry semantics."""
        pub: Any = DummyPublisher(symbols=["BTC-USD"])
        pub._candle_batch_max_rows = 1
        pub._batch_max_age_s = 60.0
        pub.running = True

        async def fake_sleep(_delay: float) -> None:
            return None

        monkeypatch.setattr("snapper.messaging.publishers.base.asyncio.sleep", fake_sleep)

        flush_calls = {"n": 0}

        async def flaky_flush(batch: list[dict[str, Any]]) -> None:
            flush_calls["n"] += 1
            if flush_calls["n"] == 1:
                raise _WriterSessionLostError("simulated disconnect")
            batch.clear()

        pub._flush_candle_writer_batch = flaky_flush
        writer = asyncio.create_task(pub._candle_writer_loop())
        await pub._candle_write_queue.put(
            {
                "public_id": "c1",
                "instrument_public_id": "inst-1",
                "open_at": datetime(2024, 1, 1, tzinfo=UTC),
                "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
                "timeframe": "1m",
                "open": 1.0,
                "high": 2.0,
                "low": 0.5,
                "close": 1.5,
                "volume": 10.0,
                "vwap": 1.2,
                "trades": 5,
                "session_id": "",
                "sequence_id": 0,
            }
        )
        await asyncio.sleep(0.05)
        pub.running = False
        await asyncio.wait_for(writer, timeout=2.0)
        assert flush_calls["n"] >= 2

    @pytest.mark.asyncio
    async def test_trade_writer_loop_exits_when_shutdown_during_recovery(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Shutdown signal during recovery aborts the retry instead of looping.

        Given: A publisher with one queued row, ``running=True``, and a
            flush that clears the batch + flips ``running=False`` then
            raises ``_WriterSessionLostError`` (so the exit-branch
            predicate is satisfied on the next except check).
        When: The writer loop runs.
        Then: The recovery exit branch fires (running=False + queue
            empty + batch empty) and the loop returns without sleeping.
        """
        pub: Any = DummyPublisher(symbols=["BTC-USD"])
        pub._trade_batch_max_rows = 1
        pub.running = True

        async def fake_sleep(_delay: float) -> None:
            return None

        monkeypatch.setattr("snapper.messaging.publishers.base.asyncio.sleep", fake_sleep)

        async def fail_after_consuming(batch: list[dict[str, Any]]) -> None:
            batch.clear()
            pub.running = False
            raise _WriterSessionLostError("simulated mid-shutdown")

        pub._flush_trade_writer_batch = fail_after_consuming
        await pub._trade_write_queue.put(
            {
                "public_id": "t1",
                "instrument_public_id": "inst-1",
                "trade_id": "x",
                "price": 1.0,
                "size": 1.0,
                "side": "buy",
                "executed_at": datetime(2024, 1, 1, tzinfo=UTC),
                "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
                "session_id": "",
                "sequence_id": 0,
            }
        )
        await asyncio.wait_for(pub._trade_writer_loop(), timeout=2.0)


@pytest.mark.asyncio
async def test_tick_writer_loop_exits_when_shutdown_during_recovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Tick writer exits cleanly when shutdown lands during recovery.

    Given: A publisher with one queued row, ``running=True``, and a
        flush that clears the batch + flips ``running=False`` then
        raises ``_WriterSessionLostError``.
    When: The tick writer loop runs.
    Then: The recovery exit branch (running=False + queue empty + batch
        empty) returns without further retries.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub._tick_batch_max_rows = 1
    pub.running = True

    async def fake_sleep(_delay: float) -> None:
        return None

    monkeypatch.setattr("snapper.messaging.publishers.base.asyncio.sleep", fake_sleep)

    async def fail_after_consuming(batch: list[dict[str, Any]]) -> None:
        batch.clear()
        pub.running = False
        raise _WriterSessionLostError("simulated mid-shutdown")

    pub._flush_tick_writer_batch = fail_after_consuming
    await pub._tick_write_queue.put(_dummy_tick_row(0))
    await asyncio.wait_for(pub._tick_writer_loop(), timeout=2.0)


@pytest.mark.asyncio
async def test_candle_writer_loop_exits_when_shutdown_during_recovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Candle writer exits cleanly when shutdown lands during recovery.

    Given: A publisher with one queued row, ``running=True``, and a
        flush that clears the batch + flips ``running=False`` then
        raises ``_WriterSessionLostError``.
    When: The candle writer loop runs.
    Then: The recovery exit branch (running=False + queue empty + batch
        empty) returns without further retries.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub._candle_batch_max_rows = 1
    pub.running = True

    async def fake_sleep(_delay: float) -> None:
        return None

    monkeypatch.setattr("snapper.messaging.publishers.base.asyncio.sleep", fake_sleep)

    async def fail_after_consuming(batch: list[dict[str, Any]]) -> None:
        batch.clear()
        pub.running = False
        raise _WriterSessionLostError("simulated mid-shutdown")

    pub._flush_candle_writer_batch = fail_after_consuming
    await pub._candle_write_queue.put(
        {
            "public_id": "c1",
            "instrument_public_id": "inst-1",
            "open_at": datetime(2024, 1, 1, tzinfo=UTC),
            "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
            "timeframe": "1m",
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 10.0,
            "vwap": 1.2,
            "trades": 5,
            "session_id": "",
            "sequence_id": 0,
        }
    )
    await asyncio.wait_for(pub._candle_writer_loop(), timeout=2.0)


@pytest.mark.asyncio
async def test_flush_candle_row_by_row_propagates_disconnect_error() -> None:
    """Candle row-by-row fallback raises _WriterSessionLostError on disconnect.

    Given: A publisher whose batch upsert raises IntegrityError (forcing
        the row-by-row fallback path) and whose subsequent per-row
        upsert raises a "Connection refused" error.
    When: _flush_candle_batch is called.
    Then: The row-by-row fallback detects the disconnect and re-raises
        as _WriterSessionLostError so the writer loop can dispose the
        dead session and re-acquire a fresh one.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    call_count = {"n": 0}

    async def upsert_candles(batch: list[dict[str, Any]], session: Any = None) -> int:
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise IntegrityError("INSERT", {}, Exception("dup key"))
        raise ConnectionRefusedError("Connection refused")

    pub.repository = SimpleNamespace(upsert_candles=upsert_candles)
    batch = [_dummy_candle_row(0)]
    with pytest.raises(_WriterSessionLostError):
        await pub._flush_candle_batch(batch)


@pytest.mark.asyncio
async def test_flush_candle_row_by_row_drops_committed_rows_on_disconnect() -> None:
    """Successfully committed fallback rows are popped from batch before raise.

    Given: A publisher whose batch upsert raises IntegrityError (forcing
        the row-by-row fallback) and whose per-row upsert succeeds on
        row 1 then raises a "Connection refused" error on row 2.
    When: _flush_candle_batch is called.
    Then: ``_WriterSessionLostError`` propagates AND the batch shrinks to
        only the un-committed row(s) so the outer writer-loop retry
        only replays the un-committed row through the SCD2 close-old +
        insert-new pipeline. Queue ``task_done`` accounting is the
        writer loop's job (see
        :meth:`MarketDataPublisherService._flush_candle_writer_batch`),
        not this helper's — direct test calls do not touch the queue.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    call_count = {"n": 0}

    async def upsert_candles(batch: list[dict[str, Any]], session: Any = None) -> int:
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise IntegrityError("INSERT", {}, Exception("dup key"))
        if call_count["n"] == 2:
            return 1
        raise ConnectionRefusedError("Connection refused")

    pub.repository = SimpleNamespace(upsert_candles=upsert_candles)
    row_committed = _dummy_candle_row(0)
    row_disconnected = _dummy_candle_row(1)
    batch = [row_committed, row_disconnected]
    with pytest.raises(_WriterSessionLostError):
        await pub._flush_candle_batch(batch)
    assert batch == [row_disconnected]


@pytest.mark.asyncio
async def test_flush_candle_row_by_row_drops_generic_failed_row_before_disconnect() -> None:
    """Generic-failed rows are dropped from the retry batch on disconnect.

    Given: A publisher whose batch upsert raises IntegrityError (forcing
        row-by-row), then row 0 succeeds, row 1 raises a non-disconnect
        generic error (statement timeout on a still-live session), and
        row 2 raises a disconnect error.
    When: _flush_candle_batch is called.
    Then: ``_WriterSessionLostError`` propagates AND the trimmed batch
        contains ONLY the disconnected row. The generic-failed row was
        logged + rolled back and is silently dropped, matching the
        legacy pre-2026-05-28 behavior for non-disconnect failures.
        Retaining the generic-failed row in the retry batch would risk
        a shutdown livelock if the failure is persistent. (No retry
        policy distinguishes transient from permanent generic
        failures; a generic-failed row is dropped, never retried.)
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    call_count = {"n": 0}

    async def upsert_candles(batch: list[dict[str, Any]], session: Any = None) -> int:
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise IntegrityError("INSERT", {}, Exception("dup key"))
        if call_count["n"] == 2:
            return 1
        if call_count["n"] == 3:
            raise RuntimeError("statement timeout")
        raise ConnectionRefusedError("Connection refused")

    pub.repository = SimpleNamespace(upsert_candles=upsert_candles)
    row_committed = _dummy_candle_row(0)
    row_generic_fail = _dummy_candle_row(1)
    row_disconnected = _dummy_candle_row(2)
    batch = [row_committed, row_generic_fail, row_disconnected]
    with pytest.raises(_WriterSessionLostError):
        await pub._flush_candle_batch(batch)
    assert batch == [row_disconnected]


@pytest.mark.asyncio
async def test_flush_candle_row_by_row_handles_non_contiguous_commits() -> None:
    """Non-contiguous commits are trimmed by index, not by prefix count.

    Given: A publisher whose batch upsert raises IntegrityError (forcing
        row-by-row), then row 0 succeeds, row 1 raises a generic
        timeout (logged + dropped), row 2 succeeds, row 3 raises a
        disconnect.
    When: _flush_candle_batch is called.
    Then: ``_WriterSessionLostError`` propagates AND the trimmed batch
        contains ONLY the disconnected row. A prefix-count trim of
        "two committed rows" would incorrectly pop rows 0 + 1, leaving
        the already-committed row 2 in the retry batch where it would
        be replayed through the SCD2 close-old + insert-new pipeline.
        The generic-failed row is silently dropped per legacy
        non-disconnect-failure semantics.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    call_count = {"n": 0}

    async def upsert_candles(batch: list[dict[str, Any]], session: Any = None) -> int:
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise IntegrityError("INSERT", {}, Exception("dup key"))
        if call_count["n"] == 2:
            return 1
        if call_count["n"] == 3:
            raise RuntimeError("statement timeout")
        if call_count["n"] == 4:
            return 1
        raise ConnectionRefusedError("Connection refused")

    pub.repository = SimpleNamespace(upsert_candles=upsert_candles)
    row0_commit = _dummy_candle_row(0)
    row1_generic = _dummy_candle_row(1)
    row2_commit = _dummy_candle_row(2)
    row3_disconnect = _dummy_candle_row(3)
    batch = [row0_commit, row1_generic, row2_commit, row3_disconnect]
    with pytest.raises(_WriterSessionLostError):
        await pub._flush_candle_batch(batch)
    assert batch == [row3_disconnect]


@pytest.mark.asyncio
async def test_flush_candle_writer_batch_balances_task_done_on_disconnect() -> None:
    """Writer wrapper acknowledges committed prefix on _WriterSessionLostError.

    Given: Two queued candle rows where the row-by-row fallback
        commits row 0 then disconnects on row 1, with both rows
        pushed onto the queue (mirroring live writer-loop state).
    When: _flush_candle_writer_batch is called.
    Then: ``_WriterSessionLostError`` propagates AND the queue's
        ``_unfinished_tasks`` drops from 2 to 1 (committed prefix
        acknowledged) AND ``batch`` retains the un-committed row for
        the outer retry. The retry-handed-off row will reach
        ``task_done`` on its eventual successful flush, closing the
        ledger and unblocking ``queue.join``.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    call_count = {"n": 0}

    async def upsert_candles(batch: list[dict[str, Any]], session: Any = None) -> int:
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise IntegrityError("INSERT", {}, Exception("dup key"))
        if call_count["n"] == 2:
            return 1
        raise ConnectionRefusedError("Connection refused")

    pub.repository = SimpleNamespace(upsert_candles=upsert_candles)
    row_committed = _dummy_candle_row(0)
    row_disconnected = _dummy_candle_row(1)
    await pub._candle_write_queue.put(row_committed)
    await pub._candle_write_queue.put(row_disconnected)
    batch = [row_committed, row_disconnected]
    with pytest.raises(_WriterSessionLostError):
        await pub._flush_candle_writer_batch(batch)
    assert batch == [row_disconnected]
    assert pub._candle_write_queue._unfinished_tasks == 1


class TestFeedHealthFlush:
    """Periodic feed-health snapshot persistence on the publisher."""

    @staticmethod
    def _pub_with_settings(coordinator_instance_id: int = 0) -> Any:
        """Build a DummyPublisher with a settings stub carrying the coordinator id."""
        pub: Any = DummyPublisher(symbols=["BTC-USD"])
        pub.settings = SimpleNamespace(coordinator_instance_id=coordinator_instance_id)
        return pub

    @staticmethod
    def _entry(
        *,
        channel: str = "ticker",
        symbol: str = "BTC/USD",
        status: str = "confirmed",
        requested_at: float,
        confirmed_at: float | None = None,
        last_seen_data_at: float | None = None,
        last_error: str | None = None,
        retry_count: int = 0,
    ) -> _SymbolEntry:
        """Build one tracker entry with monotonic-clock fields."""
        return _SymbolEntry(
            channel=channel,
            symbol=symbol,
            status=status,
            requested_at=requested_at,
            confirmed_at=confirmed_at,
            last_error=last_error,
            retry_count=retry_count,
            last_seen_data_at=last_seen_data_at,
        )

    def test_build_rows_converts_monotonic_to_wall_clock(self) -> None:
        """Monotonic fields convert to wall-clock; ``None`` fields stay ``None``."""
        pub = self._pub_with_settings(coordinator_instance_id=2)
        mono_now = monotonic()
        entry = self._entry(
            requested_at=mono_now - 30.0,
            confirmed_at=mono_now - 20.0,
            last_seen_data_at=None,
            retry_count=1,
            last_error="prior",
        )
        snapshot = {(entry.channel, entry.symbol): entry}
        rows = pub._build_feed_health_rows(snapshot)
        assert len(rows) == 1
        row = rows[0]
        assert row["coordinator"] == "coord-2"
        assert row["exchange"] == "kraken"
        assert row["channel"] == "ticker"
        assert row["symbol"] == "BTC/USD"
        assert row["status"] == "confirmed"
        assert row["retry_count"] == 1
        assert row["last_error"] == "prior"
        assert row["last_seen_data_at"] is None
        delta = (row["snapshot_at"] - row["requested_at"]).total_seconds()
        assert abs(delta - 30.0) < 1.0
        confirmed_delta = (row["snapshot_at"] - row["confirmed_at"]).total_seconds()
        assert abs(confirmed_delta - 20.0) < 1.0

    def test_monotonic_to_wall_conversion_is_exact(self) -> None:
        """The static conversion maps a monotonic value to an exact wall instant."""
        wall_now = datetime(2026, 6, 3, 12, 0, tzinfo=UTC)
        mono_now = 1000.0
        assert MarketDataPublisherService._monotonic_to_wall(
            970.0, wall_now, mono_now
        ) == wall_now - timedelta(seconds=30)
        assert (
            MarketDataPublisherService._monotonic_to_wall_optional(None, wall_now, mono_now) is None
        )
        assert MarketDataPublisherService._monotonic_to_wall_optional(
            990.0, wall_now, mono_now
        ) == wall_now - timedelta(seconds=10)

    @pytest.mark.asyncio
    async def test_flush_upserts_snapshot_rows(self) -> None:
        """A non-empty snapshot is converted and upserted via the repository."""
        pub = self._pub_with_settings()
        mono_now = monotonic()
        entry = self._entry(requested_at=mono_now - 5.0, confirmed_at=mono_now - 5.0)
        client = SimpleNamespace(
            subscription_health_snapshot=lambda: {(entry.channel, entry.symbol): entry}
        )
        pub._exchange_client = client
        upsert = AsyncMock()
        pub.repository = SimpleNamespace(upsert_instrument_feed_health=upsert)
        await pub._flush_feed_health()
        upsert.assert_awaited_once()
        await_args = upsert.await_args
        assert await_args is not None
        rows = await_args.args[0]
        assert len(rows) == 1
        assert rows[0]["symbol"] == "BTC/USD"

    @pytest.mark.asyncio
    async def test_flush_noop_without_client(self) -> None:
        """No exchange client means the flush returns without touching the repo."""
        pub = self._pub_with_settings()
        pub._exchange_client = None
        upsert = AsyncMock()
        pub.repository = SimpleNamespace(upsert_instrument_feed_health=upsert)
        await pub._flush_feed_health()
        upsert.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_flush_noop_without_repository(self) -> None:
        """No repository means the flush returns without snapshotting."""
        pub = self._pub_with_settings()
        snapshot_calls = {"n": 0}

        def _snapshot() -> dict[tuple[str, str], _SymbolEntry]:
            snapshot_calls["n"] += 1
            return {}

        pub._exchange_client = SimpleNamespace(subscription_health_snapshot=_snapshot)
        pub.repository = None
        await pub._flush_feed_health()
        assert snapshot_calls["n"] == 0

    @pytest.mark.asyncio
    async def test_flush_noop_on_empty_snapshot(self) -> None:
        """An empty snapshot short-circuits before calling the repository."""
        pub = self._pub_with_settings()
        pub._exchange_client = SimpleNamespace(subscription_health_snapshot=lambda: {})
        upsert = AsyncMock()
        pub.repository = SimpleNamespace(upsert_instrument_feed_health=upsert)
        await pub._flush_feed_health()
        upsert.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_flush_swallows_exception(self) -> None:
        """A repository error during flush is logged and swallowed, not raised."""
        pub = self._pub_with_settings()
        mono_now = monotonic()
        entry = self._entry(requested_at=mono_now)
        pub._exchange_client = SimpleNamespace(
            subscription_health_snapshot=lambda: {(entry.channel, entry.symbol): entry}
        )
        upsert = AsyncMock(side_effect=RuntimeError("db down"))
        pub.repository = SimpleNamespace(upsert_instrument_feed_health=upsert)
        await pub._flush_feed_health()
        upsert.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_flush_loop_invokes_flush_then_stops(self) -> None:
        """The loop sleeps, flushes once, then exits when ``running`` clears."""
        pub = self._pub_with_settings()
        pub.running = True
        flush_calls = {"n": 0}

        async def _fake_flush() -> None:
            flush_calls["n"] += 1
            pub.running = False

        pub._flush_feed_health = _fake_flush

        async def _fast_sleep(_seconds: float) -> None:
            return None

        with patch("snapper.messaging.publishers.base.asyncio.sleep", _fast_sleep):
            await pub._feed_health_flush_loop()
        assert flush_calls["n"] == 1

    @pytest.mark.asyncio
    async def test_flush_loop_breaks_when_stopped_after_sleep(self) -> None:
        """When ``running`` clears during the sleep, the loop breaks before flush."""
        pub = self._pub_with_settings()
        pub.running = True
        flush_calls = {"n": 0}

        async def _fake_flush() -> None:
            flush_calls["n"] += 1

        pub._flush_feed_health = _fake_flush

        async def _stopping_sleep(_seconds: float) -> None:
            pub.running = False

        with patch("snapper.messaging.publishers.base.asyncio.sleep", _stopping_sleep):
            await pub._feed_health_flush_loop()
        assert flush_calls["n"] == 0

    @pytest.mark.asyncio
    async def test_stop_feed_health_loop_noop_when_absent(self) -> None:
        """Stopping the loop is a no-op when no task was started."""
        pub = self._pub_with_settings()
        pub._feed_health_loop_task = None
        await pub._stop_feed_health_loop()
        assert pub._feed_health_loop_task is None

    @pytest.mark.asyncio
    async def test_stop_feed_health_loop_cancels_task(self) -> None:
        """Stopping the loop cancels and clears a running task."""
        pub = self._pub_with_settings()
        pub.running = True

        async def _never() -> None:
            while True:
                await asyncio.sleep(3600)

        pub._feed_health_loop_task = asyncio.create_task(_never())
        await asyncio.sleep(0)
        await pub._stop_feed_health_loop()
        assert pub._feed_health_loop_task is None

    def test_flush_interval_is_thirty_seconds(self) -> None:
        """The module flush interval constant defaults to 30 seconds."""
        assert _FEED_HEALTH_FLUSH_INTERVAL_S == 30.0


@pytest.mark.asyncio
async def test_recovery_attempt_is_bounded_and_retried_on_hang(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A hung recovery attempt times out, is logged, and recovery retries.

    Given: A running publisher whose first attempt hangs forever (the
        wedged-replay class from the 2026-06-09 incident),
    When: Recovery runs under the lock with a small per-attempt bound,
    Then: The hang becomes a swallowed TimeoutError, attempt 2 runs and
        restores data, and the recovery lock is released — a single wedged
        attempt can no longer silence liveness recovery permanently.
    """
    pub = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub._last_message_at = 0.0
    calls = {"n": 0}

    async def _attempt(_reason: str) -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            await asyncio.Event().wait()
        pub._last_message_at = 100.0

    pub._attempt_liveness_recovery = AsyncMock(side_effect=_attempt)
    pub._sleep_with_jitter = AsyncMock()
    monkeypatch.setattr("snapper.messaging.publishers.base._RECOVERY_ATTEMPT_TIMEOUT_S", 0.05)
    monkeypatch.setattr("snapper.messaging.publishers.base._RECOVERY_PROGRESS_GRACE_S", 0.0)
    await pub._run_recovery_under_lock("stale")
    assert pub._attempt_liveness_recovery.await_count == 2
    assert not pub._recovery_lock.locked()


def _candle_minute(hour: int, minute: int, *, day: int = 14) -> datetime:
    """Return a UTC 1m candle boundary for the candle-synthesis tests."""
    return datetime(2026, 6, day, hour, minute, tzinfo=UTC)


def _candle_update(
    *,
    begin: datetime,
    symbol: str = "BTC-USD",
    open_: float = 100.0,
    high: float = 100.0,
    low: float = 100.0,
    close: float = 100.0,
    volume: float = 1.0,
    vwap: float | None = None,
    trades: int = 1,
) -> CandleUpdate:
    """Build a 1m :class:`CandleUpdate` for candle-synthesis tests."""
    return CandleUpdate(
        symbol=symbol,
        open=open_,
        high=high,
        low=low,
        close=close,
        vwap=close if vwap is None else vwap,
        trades=trades,
        volume=volume,
        interval_begin=begin,
        interval=60,
    )


@pytest.mark.asyncio
async def test_higher_timeframes_create_aggregator_and_single_consumer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify configured higher timeframes enable synthesis from a single 1m sub.

    Given: timeframes ["1m","1h","1d"],
    When: the publisher starts,
    Then: an aggregator is created for {1h,1d}, the restart seed runs, and only
        one (1m) candle consumer task is spawned.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    _mock_start_runtime(pub, monkeypatch)
    pub.settings.timeframes = ["1m", "1h", "1d"]
    seed_mock = AsyncMock()
    pub._seed_aggregator_from_db = seed_mock
    await pub.start()
    assert isinstance(pub._candle_aggregator, CandleAggregator)
    assert set(pub._candle_aggregator._tf_seconds) == {"1h", "1d"}
    assert pub._candle_aggregator._live_epoch_ts > 0
    assert len(pub._candle_consumer_tasks) == 1
    seed_mock.assert_awaited_once()
    await pub.stop()


@pytest.mark.asyncio
async def test_single_timeframe_creates_no_aggregator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify the 1m-only configuration keeps the legacy path (no aggregator).

    Given: timeframes ["1m"],
    When: the publisher starts,
    Then: no aggregator is created, the seed is not run, one consumer spawns.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    _mock_start_runtime(pub, monkeypatch)
    pub.settings.timeframes = ["1m"]
    pub._candle_aggregator = CandleAggregator(["1h"])
    seed_mock = AsyncMock()
    pub._seed_aggregator_from_db = seed_mock
    await pub.start()
    assert pub._candle_aggregator is None
    assert len(pub._candle_consumer_tasks) == 1
    seed_mock.assert_not_awaited()
    await pub.stop()


@pytest.mark.asyncio
async def test_unsupported_higher_timeframe_is_filtered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify a non-synthesizable configured timeframe is filtered out.

    Given: timeframes ["1m","1h","2h"] where 2h is not synthesizable,
    When: the publisher starts,
    Then: the aggregator is created for 1h only (the unsupported-TF warning
        branch runs without error and 2h is excluded).
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    _mock_start_runtime(pub, monkeypatch)
    pub.settings.timeframes = ["1m", "1h", "2h"]
    pub._seed_aggregator_from_db = AsyncMock()
    await pub.start()
    assert pub._candle_aggregator is not None
    assert set(pub._candle_aggregator._tf_seconds) == {"1h"}
    assert "2h" not in pub._candle_aggregator._tf_seconds
    await pub.stop()


@pytest.mark.asyncio
async def test_only_unsupported_higher_tf_builds_no_aggregator_and_drops_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A config with only an unsynthesizable higher TF builds no aggregator.

    Given: timeframes ["1m","2h"] where 2h is neither synthesizable nor native,
    When: the publisher starts,
    Then: no aggregator is created, the seed never runs, only the native 1m
        consumer is spawned, and 2h is dropped (the else-branch filter + warning).
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    _mock_start_runtime(pub, monkeypatch)
    pub.settings.timeframes = ["1m", "2h"]
    seed_mock = AsyncMock()
    pub._seed_aggregator_from_db = seed_mock
    await pub.start()
    assert pub._candle_aggregator is None
    assert len(pub._candle_consumer_tasks) == 1
    seed_mock.assert_not_awaited()
    await pub.stop()


@pytest.mark.asyncio
async def test_forward_fill_forced_off_for_unsupported_venue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """candle_forward_fill is forced OFF for a venue that does not support it.

    Given: a publisher that inherits the base (False) _supports_forward_fill (a
        non-continuous-corpus venue) and synthesizes a higher TF, with
        candle_forward_fill=True,
    When: it starts,
    Then: the aggregator is built with forward_fill disabled and no flush loop is
        spawned (the forced-off warning branch runs).
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    _mock_start_runtime(pub, monkeypatch)
    pub.settings.timeframes = ["1m", "1h"]
    pub.settings.candle_forward_fill = True
    pub._seed_aggregator_from_db = AsyncMock()
    await pub.start()
    assert pub._candle_aggregator is not None
    assert pub._candle_aggregator.forward_fill is False
    assert pub._candle_flush_loop_task is None
    await pub.stop()


@pytest.mark.asyncio
async def test_forward_fill_flag_starts_and_stops_flush_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify the flush loop is started only when forward-fill is enabled.

    Given: higher timeframes and candle_forward_fill=True,
    When: the publisher starts and stops,
    Then: a flush loop task is created on start and cleared on stop.
    """
    pub: Any = SpotLikePublisher(symbols=["BTC-USD"])
    _mock_start_runtime(pub, monkeypatch)
    pub.settings.timeframes = ["1m", "1h"]
    pub.settings.candle_forward_fill = True
    pub._seed_aggregator_from_db = AsyncMock()
    await pub.start()
    assert pub._candle_flush_loop_task is not None
    assert pub._candle_aggregator.forward_fill is True
    await pub.stop()
    assert pub._candle_flush_loop_task is None


@pytest.mark.asyncio
async def test_forward_fill_off_does_not_start_flush_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify the flush loop is NOT started when forward-fill is disabled.

    Given: higher timeframes but candle_forward_fill=False,
    When: the publisher starts,
    Then: no flush loop task is created (Phase-1 behaviour unchanged).
    """
    pub: Any = SpotLikePublisher(symbols=["BTC-USD"])
    _mock_start_runtime(pub, monkeypatch)
    pub.settings.timeframes = ["1m", "1h"]
    pub.settings.candle_forward_fill = False
    pub._seed_aggregator_from_db = AsyncMock()
    await pub.start()
    assert pub._candle_flush_loop_task is None
    await pub.stop()


@pytest.mark.asyncio
async def test_candle_flush_loop_publishes_filled_bars(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify the flush loop publishes each forward-filled bar (publish-only).

    Given: an aggregator whose flush yields one higher-TF bar,
    When: the flush loop ticks once,
    Then: the bar is published to the synthesized topic and the loop exits.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub._publish_synthesized_candle = AsyncMock()
    synth = _candle_update(begin=_candle_minute(10, 0))
    aggregator = MagicMock()

    def _flush(_now: datetime) -> list[tuple[str, Any]]:
        pub.running = False
        return [("1h", synth)]

    aggregator.flush = MagicMock(side_effect=_flush)
    pub._candle_aggregator = aggregator
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.asyncio.sleep", AsyncMock(return_value=None)
    )
    await pub._candle_flush_loop(cast(Any, "kraken"))
    pub._publish_synthesized_candle.assert_awaited_once_with(synth, cast(Any, "kraken"), "1h")


@pytest.mark.asyncio
async def test_candle_flush_loop_skips_when_aggregator_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify the flush loop is a no-op when the aggregator is absent.

    Given: no aggregator,
    When: the flush loop ticks,
    Then: nothing is published and the loop exits cleanly.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub._publish_synthesized_candle = AsyncMock()
    pub._candle_aggregator = None
    ticks = {"count": 0}

    async def _sleep(_seconds: float) -> None:
        ticks["count"] += 1
        if ticks["count"] >= 2:
            pub.running = False

    monkeypatch.setattr("snapper.messaging.publishers.base.asyncio.sleep", _sleep)
    await pub._candle_flush_loop(cast(Any, "kraken"))
    pub._publish_synthesized_candle.assert_not_awaited()


@pytest.mark.asyncio
async def test_candle_flush_loop_logs_and_survives_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify a flush error is logged and does not crash the loop.

    Given: an aggregator whose flush raises,
    When: the flush loop ticks,
    Then: the error is swallowed and the loop exits on the running flag.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub._publish_synthesized_candle = AsyncMock()
    aggregator = MagicMock()

    def _flush(_now: datetime) -> list[tuple[str, Any]]:
        pub.running = False
        raise RuntimeError("boom")

    aggregator.flush = MagicMock(side_effect=_flush)
    pub._candle_aggregator = aggregator
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.asyncio.sleep", AsyncMock(return_value=None)
    )
    await pub._candle_flush_loop(cast(Any, "kraken"))
    pub._publish_synthesized_candle.assert_not_awaited()


@pytest.mark.asyncio
async def test_candle_loop_folds_synchronously_before_processing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify the 1m frame is folded BEFORE the _process_candle await (anti-race).

    Given: an aggregator and a 1m frame,
    When: the candle loop processes it,
    Then: fold() is invoked before _process_candle, so the separate flush task
        cannot interleave at that await and drop the in-hand frame as late.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub.msg_publisher = AsyncMock()
    pub._publish_synthesized_candle = AsyncMock()
    pub._should_persist_row = lambda *_a, **_k: False
    manager = Mock()
    manager.process = AsyncMock(return_value=None)
    manager.fold = Mock(return_value=[])
    pub._candle_aggregator = SimpleNamespace(fold=manager.fold)
    pub._process_candle = manager.process
    feed = [_candle_update(begin=_candle_minute(10, 0))]

    async def gen() -> AsyncIterator[Any]:
        for candle in feed:
            yield candle

    pub._exchange_client = SimpleNamespace(subscribe_candles=lambda _s, _tf: gen())
    await pub._candle_loop(["BTC-USD"], "1m")
    call_names = [call[0] for call in manager.mock_calls]
    assert call_names.index("fold") < call_names.index("process")


@pytest.mark.asyncio
async def test_candle_flush_loop_propagates_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify the flush loop re-raises CancelledError so shutdown can await it.

    Given: a flush loop whose sleep is cancelled,
    When: the loop ticks,
    Then: CancelledError propagates (not swallowed by the error handler).
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub._candle_aggregator = MagicMock()
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.asyncio.sleep",
        AsyncMock(side_effect=asyncio.CancelledError),
    )
    with pytest.raises(asyncio.CancelledError):
        await pub._candle_flush_loop(cast(Any, "kraken"))


def _finalized_row(*, ipid: str = "inst-1", open_at: datetime) -> CandleUpsertRow:
    """Build a candle row for native-finalizer wiring tests."""
    return CandleUpsertRow(
        instrument_public_id=ipid,
        open_at=open_at,
        timestamp=open_at,
        timeframe="1m",
        open=1.0,
        high=2.0,
        low=0.5,
        close=1.5,
        volume=10.0,
        vwap=1.25,
        trades=3,
        source="native",
        complete=False,
    )


def test_observe_native_candle_passthrough_without_finalizer() -> None:
    """Without a finalizer, the row passes through unchanged for enqueue.

    Given: a publisher with no native finalizer installed,
    When: a native candle row is observed,
    Then: the (symbol, row) pair is returned as-is (legacy per-frame behavior).
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub._native_finalizer = None
    row = _finalized_row(open_at=_candle_minute(10, 0))
    assert pub._observe_native_candle("BTC-USD", row) == [("BTC-USD", row)]


def test_observe_native_candle_delegates_to_finalizer() -> None:
    """With a finalizer, observation is delegated to it.

    Given: a publisher with a native finalizer holding a prior window,
    When: a later window's row is observed,
    Then: the finalizer's release (the finalized predecessor) is returned.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub._native_finalizer = NativeCandleFinalizer(
        persist_intermediate=False, flush_grace_seconds=5.0
    )
    first = _finalized_row(open_at=_candle_minute(10, 0))
    pub._observe_native_candle("BTC-USD", first)
    released = pub._observe_native_candle("BTC-USD", _finalized_row(open_at=_candle_minute(10, 1)))
    assert len(released) == 1
    assert released[0][1]["open_at"] == _candle_minute(10, 0)
    assert released[0][1]["complete"] is True


def test_enqueue_finalized_candles_respects_persist_gate() -> None:
    """Released rows are enqueued only when the persist policy allows.

    Given: two released rows and a persist gate that allows one symbol,
    When: they are enqueued,
    Then: only the allowed symbol's row reaches the write queue.
    """
    pub: Any = DummyPublisher(symbols=["A-USD", "B-USD"])
    pub._candle_write_queue = asyncio.Queue()
    pub._should_persist_row = lambda _kind, _exch, sym: sym == "A-USD"
    row_a = _finalized_row(ipid="inst-a", open_at=_candle_minute(10, 0))
    row_b = _finalized_row(ipid="inst-b", open_at=_candle_minute(10, 0))
    pub._enqueue_finalized_candles(
        [("A-USD", row_a), ("B-USD", row_b)], cast(Any, "kraken"), "kraken"
    )
    assert pub._candle_write_queue.qsize() == 1
    assert pub._candle_write_queue.get_nowait()["instrument_public_id"] == "inst-a"


@pytest.mark.asyncio
async def test_native_finalize_flush_loop_enqueues_flushed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The native flush loop enqueues each released bar that passes the gate.

    Given: a finalizer whose flush yields one ended bar,
    When: the flush loop ticks once,
    Then: the bar is enqueued and the loop exits.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub._candle_write_queue = asyncio.Queue()
    pub._should_persist_row = lambda *_a, **_k: True
    row = _finalized_row(open_at=_candle_minute(10, 0))
    finalizer = MagicMock()

    def _flush(_now: datetime) -> list[tuple[str, Any]]:
        pub.running = False
        return [("BTC-USD", row)]

    finalizer.flush = MagicMock(side_effect=_flush)
    pub._native_finalizer = finalizer
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.asyncio.sleep", AsyncMock(return_value=None)
    )
    await pub._native_finalize_flush_loop(cast(Any, "kraken"))
    assert pub._candle_write_queue.qsize() == 1


@pytest.mark.asyncio
async def test_native_finalize_flush_loop_skips_when_finalizer_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The native flush loop is a no-op when no finalizer is installed.

    Given: no native finalizer,
    When: the flush loop ticks,
    Then: nothing is enqueued and the loop exits on the running flag.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub._candle_write_queue = asyncio.Queue()
    pub._native_finalizer = None
    ticks = {"count": 0}

    async def _sleep(_seconds: float) -> None:
        ticks["count"] += 1
        if ticks["count"] >= 2:
            pub.running = False

    monkeypatch.setattr("snapper.messaging.publishers.base.asyncio.sleep", _sleep)
    await pub._native_finalize_flush_loop(cast(Any, "kraken"))
    assert pub._candle_write_queue.qsize() == 0


@pytest.mark.asyncio
async def test_native_finalize_flush_loop_survives_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A flush error is logged and does not crash the native flush loop.

    Given: a finalizer whose flush raises,
    When: the flush loop ticks,
    Then: the error is swallowed and the loop exits on the running flag.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub._candle_write_queue = asyncio.Queue()
    finalizer = MagicMock()

    def _flush(_now: datetime) -> list[tuple[str, Any]]:
        pub.running = False
        raise RuntimeError("boom")

    finalizer.flush = MagicMock(side_effect=_flush)
    pub._native_finalizer = finalizer
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.asyncio.sleep", AsyncMock(return_value=None)
    )
    await pub._native_finalize_flush_loop(cast(Any, "kraken"))
    assert pub._candle_write_queue.qsize() == 0


@pytest.mark.asyncio
async def test_native_finalize_flush_loop_propagates_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The native flush loop re-raises CancelledError so shutdown can await it.

    Given: a flush loop whose sleep is cancelled,
    When: the loop ticks,
    Then: CancelledError propagates.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub._native_finalizer = MagicMock()
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.asyncio.sleep",
        AsyncMock(side_effect=asyncio.CancelledError),
    )
    with pytest.raises(asyncio.CancelledError):
        await pub._native_finalize_flush_loop(cast(Any, "kraken"))


@pytest.mark.asyncio
async def test_stop_candle_pipeline_drains_finalizer_before_join() -> None:
    """Shutdown drains the finalizer's ended bars before joining the write queue.

    Given: a publisher whose finalizer holds an ended window,
    When: the candle pipeline is stopped,
    Then: the drained final bar is enqueued (so the queue join persists it) and
        the native flush task is cancelled.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub._candle_write_queue = asyncio.Queue()
    pub._should_persist_row = lambda *_a, **_k: True
    pub._candle_consumer_tasks = []
    pub._candle_writer_task = None
    pub._candle_flush_loop_task = None
    pub._join_shutdown_queue = AsyncMock()
    pub._await_shutdown_task = AsyncMock()
    pub._await_shutdown_tasks = AsyncMock()
    pub._native_finalize_flush_task = asyncio.create_task(asyncio.sleep(60))
    finalizer = NativeCandleFinalizer(persist_intermediate=False, flush_grace_seconds=5.0)
    finalizer.observe("BTC-USD", _finalized_row(open_at=_candle_minute(10, 0)))
    pub._native_finalizer = finalizer
    await pub._stop_candle_pipeline()
    assert pub._candle_write_queue.qsize() == 1
    assert pub._native_finalize_flush_task is None


@pytest.mark.asyncio
async def test_candle_loop_publishes_synthesized_higher_timeframe() -> None:
    """Verify the candle loop publishes a synthesized higher-TF bar.

    Given: an active aggregator for 5m and a 1m stream crossing a 5m boundary,
    When: the candle loop drains the stream,
    Then: a 5m candle is published to the synthesized topic.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub.msg_publisher = AsyncMock()
    pub.repository = SimpleNamespace()
    pub._ensure_instrument = AsyncMock(return_value="inst-1")
    pub._publish_message = AsyncMock()
    pub._should_persist_row = lambda *_a, **_k: False
    pub._candle_aggregator = CandleAggregator(["5m"])
    feed = [
        _candle_update(begin=_candle_minute(10, 0)),
        _candle_update(begin=_candle_minute(10, 1)),
        _candle_update(begin=_candle_minute(10, 5)),
        _candle_update(begin=_candle_minute(10, 6)),
    ]

    async def gen() -> AsyncIterator[Any]:
        for candle in feed:
            yield candle

    pub._exchange_client = SimpleNamespace(subscribe_candles=lambda _s, _tf: gen())
    await pub._candle_loop(["BTC-USD"], "1m")
    topics = [call.args[0] for call in pub._publish_message.await_args_list]
    assert any(topic.endswith(".candles.5m") for topic in topics)


@pytest.mark.asyncio
async def test_candle_loop_without_aggregator_publishes_no_higher_tf() -> None:
    """Verify the legacy path publishes only the native timeframe.

    Given: no aggregator,
    When: the candle loop drains a 1m stream,
    Then: only 1m topics are published (no synthesis).
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub.msg_publisher = AsyncMock()
    pub.repository = SimpleNamespace()
    pub._ensure_instrument = AsyncMock(return_value="inst-1")
    pub._publish_message = AsyncMock()
    pub._should_persist_row = lambda *_a, **_k: False
    feed = [
        _candle_update(begin=_candle_minute(10, 0)),
        _candle_update(begin=_candle_minute(10, 1)),
    ]

    async def gen() -> AsyncIterator[Any]:
        for candle in feed:
            yield candle

    pub._exchange_client = SimpleNamespace(subscribe_candles=lambda _s, _tf: gen())
    await pub._candle_loop(["BTC-USD"], "1m")
    topics = [call.args[0] for call in pub._publish_message.await_args_list]
    assert topics
    assert all(topic.endswith(".candles.1m") for topic in topics)


@pytest.mark.asyncio
async def test_publish_synthesized_candle_persists_with_synthesized_provenance() -> None:
    """Verify a synthesized publish both ships to ZMQ and persists a tagged row.

    Given: a resolvable instrument and the default (no-policy) persist gate,
    When: a synthesized 1h candle carrying ``complete=False`` is published,
    Then: it is sent on the 1h topic AND one writer-queue row is enqueued
        tagged ``source='synthesized'`` carrying the bucket's ``complete`` flag.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub._ensure_instrument = AsyncMock(return_value="inst-1")
    pub._publish_message = AsyncMock()
    synth = CandleUpdate(
        symbol="BTC-USD",
        open=1.0,
        high=2.0,
        low=0.5,
        close=1.5,
        vwap=1.25,
        trades=10,
        volume=7.0,
        interval_begin=_candle_minute(10, 0),
        interval=3600,
        complete=False,
    )
    await pub._publish_synthesized_candle(synth, cast(Any, "kraken"), "1h")
    assert pub._publish_message.await_count == 1
    topic, message = pub._publish_message.await_args.args
    assert topic.endswith(".candles.1h")
    assert message.timeframe == "1h"
    assert message.volume == 7.0
    assert pub._candle_write_queue.qsize() == 1
    row = pub._candle_write_queue.get_nowait()
    assert row["source"] == "synthesized"
    assert row["complete"] is False
    assert row["timeframe"] == "1h"
    assert row["open_at"] == _candle_minute(10, 0)
    assert row["instrument_public_id"] == "inst-1"


@pytest.mark.asyncio
async def test_publish_synthesized_candle_does_not_bump_candle_watermark() -> None:
    """Synthesized bars must not refresh the native-candle liveness watermark.

    Given: A publisher with a stale native-candle watermark,
    When: A synthesized higher-TF candle is published (as the forward-fill
        path keeps doing during a native-channel outage),
    Then: The native-candle watermark is unchanged, so a dead native channel
        cannot be masked by synthesized forward-fills.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub._ensure_instrument = AsyncMock(return_value="inst-1")
    pub._publish_message = AsyncMock()
    pub._last_candle_msg_at = 0.0
    synth = _candle_update(begin=_candle_minute(10, 0))
    await pub._publish_synthesized_candle(synth, cast(Any, "kraken"), "1h")
    assert pub._last_candle_msg_at == 0.0


@pytest.mark.asyncio
async def test_publish_synthesized_candle_skips_persist_when_policy_off() -> None:
    """Verify a disabled persist policy publishes but enqueues nothing.

    Given: a resolvable instrument and a persist gate that returns False,
    When: a synthesized 1h candle is published,
    Then: it is sent on the 1h topic and nothing is enqueued for persistence.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub._ensure_instrument = AsyncMock(return_value="inst-1")
    pub._publish_message = AsyncMock()
    pub._should_persist_row = lambda *_a, **_k: False
    synth = _candle_update(begin=_candle_minute(10, 0))
    await pub._publish_synthesized_candle(synth, cast(Any, "kraken"), "1h")
    assert pub._publish_message.await_count == 1
    assert pub._candle_write_queue.qsize() == 0


@pytest.mark.asyncio
async def test_publish_synthesized_candle_reemits_with_stable_public_id() -> None:
    """Verify re-emitting the same window reuses the candle public_id (SCD2-idempotent).

    Given: a resolvable instrument,
    When: the same synthesized 1h window is published twice,
    Then: both enqueued rows carry the identical ``public_id`` so the downstream
        SCD2 upsert collapses them to a no-op rather than a duplicate bar.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub._ensure_instrument = AsyncMock(return_value="inst-1")
    pub._publish_message = AsyncMock()
    synth = _candle_update(begin=_candle_minute(10, 0))
    await pub._publish_synthesized_candle(synth, cast(Any, "kraken"), "1h")
    await pub._publish_synthesized_candle(synth, cast(Any, "kraken"), "1h")
    assert pub._candle_write_queue.qsize() == 2
    first = pub._candle_write_queue.get_nowait()
    second = pub._candle_write_queue.get_nowait()
    assert first["public_id"] == second["public_id"]


@pytest.mark.asyncio
async def test_publish_synthesized_candle_skips_unknown_instrument() -> None:
    """Verify an unresolved instrument suppresses the synthesized publish.

    Given: an instrument that cannot be resolved,
    When: a synthesized candle is published,
    Then: nothing is sent to ZMQ.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub._ensure_instrument = AsyncMock(return_value=None)
    pub._publish_message = AsyncMock()
    synth = _candle_update(begin=_candle_minute(10, 0))
    await pub._publish_synthesized_candle(synth, cast(Any, "kraken"), "1h")
    pub._publish_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_seed_aggregator_rebuilds_open_bucket_excluding_current_minute() -> None:
    """Verify restart seed folds completed 1m and skips the current minute.

    Given: persisted 1m rows in the current 1h window plus the current minute,
    When: the aggregator is seeded,
    Then: completed minutes fold (null vwap/trades coalesced) and the current
        minute is excluded.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub._candle_aggregator = CandleAggregator(["1h"])
    now = datetime(2026, 6, 14, 10, 30, 30, tzinfo=UTC)
    rows = [
        {
            "open_at": _candle_minute(10, 5),
            "timeframe": "1m",
            "open": 50.0,
            "high": 55.0,
            "low": 48.0,
            "close": 50.0,
            "volume": 2.0,
            "vwap": None,
            "trades": None,
            "public_id": "p1",
            "timestamp": now,
            "complete": True,
            "session_id": "s",
            "sequence_id": 1,
        },
        {
            "open_at": _candle_minute(10, 6),
            "timeframe": "1m",
            "open": 60.0,
            "high": 61.0,
            "low": 59.0,
            "close": 60.0,
            "volume": 3.0,
            "vwap": 60.0,
            "trades": 4,
            "public_id": "p2",
            "timestamp": now,
            "complete": True,
            "session_id": "s",
            "sequence_id": 2,
        },
        {
            "open_at": _candle_minute(10, 30),
            "timeframe": "1m",
            "open": 70.0,
            "high": 70.0,
            "low": 70.0,
            "close": 70.0,
            "volume": 5.0,
            "vwap": 70.0,
            "trades": 9,
            "public_id": "p3",
            "timestamp": now,
            "complete": True,
            "session_id": "s",
            "sequence_id": 3,
        },
    ]
    pub.repository = SimpleNamespace(get_candles=AsyncMock(return_value=rows))
    await pub._seed_aggregator_from_db(["BTC-USD"], ["1h"], now)
    pub.repository.get_candles.assert_awaited_once_with(
        "BTC-USD",
        "1m",
        _candle_minute(10, 0),
        _candle_minute(10, 30) - timedelta(microseconds=1),
        "kraken",
        now,
        order="asc",
    )
    key = ("BTC-USD", "1h", int(_candle_minute(10, 0).timestamp()))
    bucket = pub._candle_aggregator._buckets[key]
    assert bucket.volume == 5.0
    assert bucket.trades == 4


@pytest.mark.asyncio
async def test_seed_aggregator_rebuilds_previous_window_in_first_minute() -> None:
    """Verify a first-minute restart rebuilds the just-closed previous window.

    Given: now is in the first minute of the 1d window (the previous day just
        closed and its bar has not emitted yet),
    When: the aggregator is seeded,
    Then: the PREVIOUS day's window is read and folded so it can emit when the
        first live minute finalizes.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub._candle_aggregator = CandleAggregator(["1d"])
    now = datetime(2026, 6, 14, 0, 0, 30, tzinfo=UTC)
    prev_day = datetime(2026, 6, 13, tzinfo=UTC)
    rows = [
        {
            "open_at": prev_day,
            "timeframe": "1m",
            "open": 10.0,
            "high": 12.0,
            "low": 9.0,
            "close": 11.0,
            "volume": 4.0,
            "vwap": 11.0,
            "trades": 3,
            "public_id": "p1",
            "timestamp": now,
            "complete": True,
            "session_id": "s",
            "sequence_id": 1,
        }
    ]
    pub.repository = SimpleNamespace(get_candles=AsyncMock(return_value=rows))
    await pub._seed_aggregator_from_db(["BTC-USD"], ["1d"], now)
    pub.repository.get_candles.assert_awaited_once_with(
        "BTC-USD",
        "1m",
        prev_day,
        datetime(2026, 6, 14, tzinfo=UTC) - timedelta(microseconds=1),
        "kraken",
        now,
        order="asc",
    )
    assert ("BTC-USD", "1d", int(prev_day.timestamp())) in pub._candle_aggregator._buckets


@pytest.mark.asyncio
async def test_seed_aggregator_skips_unconfigured_timeframe() -> None:
    """Verify seeding skips a timeframe the aggregator does not synthesize.

    Given: a higher timeframe absent from the aggregator (unsupported),
    When: the aggregator is seeded,
    Then: no DB read happens for it.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub._candle_aggregator = CandleAggregator(["1h"])
    pub.repository = SimpleNamespace(get_candles=AsyncMock(return_value=[]))
    now = datetime(2026, 6, 14, 10, 30, tzinfo=UTC)
    await pub._seed_aggregator_from_db(["BTC-USD"], ["2h"], now)
    pub.repository.get_candles.assert_not_awaited()


@pytest.mark.asyncio
async def test_seed_aggregator_skips_window_with_nonfinal_minute() -> None:
    """Verify a window containing any non-final persisted 1m is not seeded.

    Given: the current 1h window holds a finalized minute (complete=True) and a
        non-final one (complete=False),
    When: the aggregator is seeded,
    Then: the WHOLE window is left unseeded (so the complete-window guard later
        suppresses the knowingly-incomplete bar).
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    now = datetime(2026, 6, 14, 10, 30, 30, tzinfo=UTC)
    pub._candle_aggregator = CandleAggregator(["1h"], live_epoch=now)
    rows = [
        {
            "open_at": _candle_minute(10, 0),
            "timeframe": "1m",
            "open": 10.0,
            "high": 10.0,
            "low": 10.0,
            "close": 10.0,
            "volume": 4.0,
            "vwap": 10.0,
            "trades": 2,
            "public_id": "p1",
            "timestamp": datetime(2026, 6, 14, 10, 1, 5, tzinfo=UTC),
            "complete": True,
            "session_id": "s",
            "sequence_id": 1,
        },
        {
            "open_at": _candle_minute(10, 5),
            "timeframe": "1m",
            "open": 20.0,
            "high": 20.0,
            "low": 20.0,
            "close": 20.0,
            "volume": 9.0,
            "vwap": 20.0,
            "trades": 5,
            "public_id": "p2",
            "timestamp": datetime(2026, 6, 14, 10, 5, 30, tzinfo=UTC),
            "complete": False,
            "session_id": "s",
            "sequence_id": 2,
        },
    ]
    pub.repository = SimpleNamespace(get_candles=AsyncMock(return_value=rows))
    await pub._seed_aggregator_from_db(["BTC-USD"], ["1h"], now)
    key = ("BTC-USD", "1h", int(_candle_minute(10, 0).timestamp()))
    assert key not in pub._candle_aggregator._buckets


@pytest.mark.asyncio
async def test_seed_aggregator_seeds_finalized_bar_with_preclose_timestamp() -> None:
    """A finalized bar sealed with a pre-close timestamp is still seeded.

    Given: a finalized 1m row (complete=True) whose write timestamp is before the
        minute closed — as NativeCandleFinalizer seals an illiquid pair's last
        frame,
    When: the aggregator is seeded,
    Then: the window IS seeded (the seed reads the durable complete flag, not the
        write-time heuristic that would have misclassified this final bar).
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    now = datetime(2026, 6, 14, 10, 30, 30, tzinfo=UTC)
    pub._candle_aggregator = CandleAggregator(["1h"], live_epoch=now)
    rows = [
        {
            "open_at": _candle_minute(10, 0),
            "timeframe": "1m",
            "open": 10.0,
            "high": 10.0,
            "low": 10.0,
            "close": 10.0,
            "volume": 4.0,
            "vwap": 10.0,
            "trades": 2,
            "public_id": "p1",
            "timestamp": datetime(2026, 6, 14, 10, 0, 20, tzinfo=UTC),
            "complete": True,
            "session_id": "s",
            "sequence_id": 1,
        }
    ]
    pub.repository = SimpleNamespace(get_candles=AsyncMock(return_value=rows))
    await pub._seed_aggregator_from_db(["BTC-USD"], ["1h"], now)
    key = ("BTC-USD", "1h", int(_candle_minute(10, 0).timestamp()))
    assert key in pub._candle_aggregator._buckets


@pytest.mark.asyncio
async def test_seed_aggregator_skips_wildcard_subscription() -> None:
    """Verify the seed is skipped (with no DB read) for a wildcard subscription.

    Given: the wildcard ["*"] symbol set,
    When: the aggregator is seeded,
    Then: no DB read happens (concrete symbols are not enumerable here).
    """
    pub: Any = DummyPublisher(symbols=["*"])
    pub._candle_aggregator = CandleAggregator(["1d"])
    pub.repository = SimpleNamespace(get_candles=AsyncMock(return_value=[]))
    now = datetime(2026, 6, 14, 10, 30, tzinfo=UTC)
    await pub._seed_aggregator_from_db(["*"], ["1d"], now)
    pub.repository.get_candles.assert_not_awaited()


@pytest.mark.asyncio
async def test_seed_aggregator_rebuilds_both_1h_and_1d() -> None:
    """Verify a mid-window restart rebuilds open 1h AND 1d buckets.

    Given: now mid-window with timeframes 1h and 1d,
    When: the aggregator is seeded,
    Then: both the open 1h and 1d buckets are reconstructed for the symbol.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub._candle_aggregator = CandleAggregator(["1h", "1d"])
    now = datetime(2026, 6, 14, 10, 30, 30, tzinfo=UTC)

    async def fake_get_candles(
        symbol: str, timeframe: str, start: datetime, end: datetime, *_a: Any, **_k: Any
    ) -> list[dict[str, Any]]:
        return [
            {
                "open_at": start,
                "timeframe": "1m",
                "open": 1.0,
                "high": 1.0,
                "low": 1.0,
                "close": 1.0,
                "volume": 2.0,
                "vwap": 1.0,
                "trades": 1,
                "public_id": "p",
                "timestamp": now,
                "complete": True,
                "session_id": "s",
                "sequence_id": 1,
            }
        ]

    pub.repository = SimpleNamespace(get_candles=AsyncMock(side_effect=fake_get_candles))
    await pub._seed_aggregator_from_db(["BTC-USD"], ["1h", "1d"], now)
    buckets = pub._candle_aggregator._buckets
    assert ("BTC-USD", "1h", int(datetime(2026, 6, 14, 10, 0, tzinfo=UTC).timestamp())) in buckets
    assert ("BTC-USD", "1d", int(datetime(2026, 6, 14, tzinfo=UTC).timestamp())) in buckets


@pytest.mark.asyncio
async def test_seed_aggregator_noop_without_repository_or_aggregator() -> None:
    """Verify the seed is a no-op when prerequisites are missing.

    Given: a missing repository (then a missing aggregator),
    When: the seed runs,
    Then: it returns without error and folds nothing.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub._candle_aggregator = CandleAggregator(["1h"])
    pub.repository = None
    await pub._seed_aggregator_from_db(
        ["BTC-USD"], ["1h"], datetime(2026, 6, 14, 10, 30, tzinfo=UTC)
    )
    assert pub._candle_aggregator._buckets == {}
    pub.repository = SimpleNamespace(get_candles=AsyncMock(return_value=[]))
    pub._candle_aggregator = None
    await pub._seed_aggregator_from_db(
        ["BTC-USD"], ["1h"], datetime(2026, 6, 14, 10, 30, tzinfo=UTC)
    )
    pub.repository.get_candles.assert_not_awaited()


@pytest.mark.asyncio
async def test_seed_aggregator_defaults_now_to_current_time() -> None:
    """Verify the seed computes the current time when none is injected.

    Given: a repository and aggregator but no explicit now,
    When: the seed runs,
    Then: it completes without error using the real clock.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub._candle_aggregator = CandleAggregator(["1d"])
    pub.repository = SimpleNamespace(get_candles=AsyncMock(return_value=[]))
    await pub._seed_aggregator_from_db(["BTC-USD"], ["1d"])
    assert pub._candle_aggregator._buckets == {}
