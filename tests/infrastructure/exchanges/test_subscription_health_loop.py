"""Tests for ExchangeClientBase subscription health loop."""

from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime

import pytest
from loguru import logger

from snapper.core.json_types import JsonObject
from snapper.data.repository import Repository
from snapper.infrastructure.exchanges import base as exchange_base
from snapper.infrastructure.exchanges._subscription_health import SubscriptionHealthTracker
from snapper.infrastructure.exchanges.base import ExchangeClientBase
from snapper.infrastructure.exchanges.contracts import AccountBalance
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import ExchangeOrderRequest
from snapper.infrastructure.exchanges.contracts import ExchangeOrderSnapshot
from snapper.infrastructure.exchanges.contracts import ExchangeOrderStatusEnum
from snapper.infrastructure.exchanges.contracts import ExecutionUpdate
from snapper.infrastructure.exchanges.contracts import OhlcvSnapshot
from snapper.infrastructure.exchanges.contracts import TickerSnapshot
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate


class HealthLoopClient(ExchangeClientBase):
    """Concrete exchange client for health-loop tests."""

    def __init__(
        self,
        repository: Repository | None = None,
        exchange_name: str = "health_test",
    ) -> None:
        """Initialize the test client."""
        super().__init__(repository=repository, exchange_name=exchange_name)
        self.retry_calls: list[tuple[str, str]] = []
        self.retry_error: Exception | None = None

    async def connect(self) -> None:
        """Connect test client."""

    async def disconnect(self) -> None:
        """Disconnect test client."""

    async def get_ticker(self, symbol: str) -> TickerSnapshot:
        """Fetch ticker snapshot."""
        return TickerSnapshot(
            symbol=symbol,
            bid=1.0,
            ask=2.0,
            last=1.5,
            timestamp=datetime.now(UTC).timestamp(),
        )

    async def get_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1m",
        since: int | None = None,
        limit: int | None = None,
    ) -> list[OhlcvSnapshot]:
        """Fetch OHLCV snapshots."""
        return []

    async def create_order(self, request: ExchangeOrderRequest) -> ExchangeOrderSnapshot:
        """Create order."""
        raise NotImplementedError

    async def cancel_order(self, order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        """Cancel order."""
        raise NotImplementedError

    async def get_order(self, order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        """Get order."""
        raise NotImplementedError

    async def get_orders(
        self,
        symbol: str | None = None,
        status: ExchangeOrderStatusEnum | None = None,
        limit: int | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        """Get orders."""
        return []

    async def get_balance(self, currency: str | None = None) -> dict[str, AccountBalance]:
        """Get balances."""
        return {}

    def subscribe_ticks(self, symbols: list[str]) -> AsyncIterator[TickerUpdate]:
        """Subscribe to ticks."""
        raise NotImplementedError

    def subscribe_candles(
        self,
        symbols: list[str],
        timeframe: str = "1m",
    ) -> AsyncIterator[CandleUpdate]:
        """Subscribe to candles."""
        raise NotImplementedError

    def subscribe_trades(self, symbols: list[str]) -> AsyncIterator[TradeUpdate]:
        """Subscribe to trades."""
        raise NotImplementedError

    def subscribe_executions(self) -> AsyncIterator[ExecutionUpdate]:
        """Subscribe to executions."""
        raise NotImplementedError

    def subscribe_instruments(self, **kwargs: object) -> AsyncIterator[JsonObject]:
        """Subscribe to instruments."""
        raise NotImplementedError

    async def _retry_subscribe(self, channel: str, symbol: str) -> None:
        """Record one retry request."""
        self.retry_calls.append((channel, symbol))
        if self.retry_error is not None:
            raise self.retry_error


class TestHealthLoopLifecycle:
    """Tests for health-loop start and stop methods."""

    @pytest.mark.asyncio
    async def test_start_without_tracker_is_noop(self) -> None:
        """No tracker means no background task.

        Given: A client without a health tracker,
        When: start_health_loop is called,
        Then: No task is created.
        """
        client = HealthLoopClient()
        await client.start_health_loop()
        assert client._health_loop_task is None

    @pytest.mark.asyncio
    async def test_start_and_stop_cancel_health_task(self) -> None:
        """Health loop starts and stops cleanly.

        Given: A client with a health tracker,
        When: start_health_loop and stop_health_loop are called,
        Then: The task handle is cleared.
        """
        client = HealthLoopClient()
        client._health_tracker = SubscriptionHealthTracker(retry_interval_s=60.0)
        await client.start_health_loop()
        assert client._health_loop_task is not None
        await client.stop_health_loop()
        assert client._health_loop_task is None

    @pytest.mark.asyncio
    async def test_double_start_is_noop(self) -> None:
        """Second start does not create another task.

        Given: A client with an already-running health loop,
        When: start_health_loop is called again,
        Then: The task handle is unchanged.
        """
        client = HealthLoopClient()
        client._health_tracker = SubscriptionHealthTracker(retry_interval_s=60.0)
        await client.start_health_loop()
        first_task = client._health_loop_task
        try:
            await client.start_health_loop()
            assert client._health_loop_task is first_task
        finally:
            await client.stop_health_loop()

    @pytest.mark.asyncio
    async def test_stop_without_task_is_noop(self) -> None:
        """Stopping without a task is safe.

        Given: A client with no health-loop task,
        When: stop_health_loop is called,
        Then: The task handle remains None.
        """
        client = HealthLoopClient()
        await client.stop_health_loop()
        assert client._health_loop_task is None


class TestHealthLoopRetry:
    """Tests for retry-loop behaviour."""

    @pytest.mark.asyncio
    async def test_loop_retries_overdue_pending_symbols(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Overdue pending symbols are retried.

        Given: A tracker with two overdue pending entries,
        When: The health loop runs one tick,
        Then: Both entries are retried once.
        """
        client = HealthLoopClient()
        tracker = SubscriptionHealthTracker(ack_timeout_s=1.0, retry_interval_s=1.0)
        client._health_tracker = tracker
        monkeypatch.setattr(exchange_base.time, "monotonic", lambda: 0.0)
        tracker.mark_pending("ticker", "BTC/USD")
        tracker.mark_pending("trade", "ETH/USD")
        monkeypatch.setattr(exchange_base.time, "monotonic", lambda: 5.0)

        async def stop_after_sleep(_: float) -> None:
            client._health_loop_running = False

        monkeypatch.setattr(exchange_base.asyncio, "sleep", stop_after_sleep)
        client._health_loop_running = True
        await client._subscription_health_loop()
        assert client.retry_calls == [("ticker", "BTC/USD"), ("trade", "ETH/USD")]

    @pytest.mark.asyncio
    async def test_loop_does_not_retry_when_budget_exhausted(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Retry budget exhaustion skips retry subscribe.

        Given: A pending entry with no retry budget,
        When: The health loop runs one tick,
        Then: _retry_subscribe is not called and the entry is failed.
        """
        client = HealthLoopClient()
        tracker = SubscriptionHealthTracker(
            ack_timeout_s=1.0,
            retry_interval_s=1.0,
            max_retries=0,
        )
        client._health_tracker = tracker
        monkeypatch.setattr(exchange_base.time, "monotonic", lambda: 10.0)
        tracker.mark_pending("ticker", "BTC/USD")
        monkeypatch.setattr(exchange_base.time, "monotonic", lambda: 12.0)

        async def stop_after_sleep(_: float) -> None:
            client._health_loop_running = False

        monkeypatch.setattr(exchange_base.asyncio, "sleep", stop_after_sleep)
        client._health_loop_running = True
        await client._subscription_health_loop()
        failed = tracker.list_failed()
        assert (client.retry_calls, [(entry.channel, entry.symbol) for entry in failed]) == (
            [],
            [("ticker", "BTC/USD")],
        )

    @pytest.mark.asyncio
    async def test_loop_consumes_retry_when_retry_subscribe_raises(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Retry exceptions still consume retry attempts.

        Given: A pending entry and a retry method that raises,
        When: The health loop runs one tick,
        Then: retry_count is incremented.
        """
        client = HealthLoopClient()
        tracker = SubscriptionHealthTracker(ack_timeout_s=1.0, retry_interval_s=1.0)
        client._health_tracker = tracker
        client.retry_error = RuntimeError("disconnected")
        monkeypatch.setattr(exchange_base.time, "monotonic", lambda: 20.0)
        tracker.mark_pending("ticker", "BTC/USD")
        monkeypatch.setattr(exchange_base.time, "monotonic", lambda: 25.0)

        async def stop_after_sleep(_: float) -> None:
            client._health_loop_running = False

        monkeypatch.setattr(exchange_base.asyncio, "sleep", stop_after_sleep)
        client._health_loop_running = True
        await client._subscription_health_loop()
        entry = tracker.snapshot()[("ticker", "BTC/USD")]
        assert (client.retry_calls, entry.retry_count, entry.status) == (
            [("ticker", "BTC/USD")],
            1,
            "pending",
        )

    @pytest.mark.asyncio
    async def test_loop_logs_stale_data_without_retrying(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Stale confirmed entries are logged but not retried.

        Given: A confirmed entry with no data for longer than threshold,
        When: The health loop runs one tick,
        Then: A stale-data warning is logged and no retry is called.
        """
        client = HealthLoopClient()
        tracker = SubscriptionHealthTracker(
            ack_timeout_s=1.0,
            retry_interval_s=1.0,
            data_stale_threshold_s=5.0,
        )
        client._health_tracker = tracker
        monkeypatch.setattr(exchange_base.time, "monotonic", lambda: 30.0)
        tracker.mark_confirmed("ticker", "BTC/USD")
        monkeypatch.setattr(exchange_base.time, "monotonic", lambda: 40.0)

        async def stop_after_sleep(_: float) -> None:
            client._health_loop_running = False

        monkeypatch.setattr(exchange_base.asyncio, "sleep", stop_after_sleep)
        sink_id = logger.add(caplog.handler, format="{message}", level="WARNING")
        try:
            client._health_loop_running = True
            await client._subscription_health_loop()
        finally:
            logger.remove(sink_id)
        stale_logs = [rec for rec in caplog.records if "no data for" in rec.message]
        assert (client.retry_calls, len(stale_logs)) == ([], 1)

    @pytest.mark.asyncio
    async def test_loop_without_tracker_returns(self) -> None:
        """Loop exits immediately without tracker.

        Given: A client without a health tracker,
        When: _subscription_health_loop is called,
        Then: It returns without creating retry calls.
        """
        client = HealthLoopClient()
        client._health_loop_running = True
        await client._subscription_health_loop()
        assert client.retry_calls == []

    @pytest.mark.asyncio
    async def test_base_retry_subscribe_stub_raises(self) -> None:
        """Base retry hook requires subclass implementation.

        Given: An ExchangeClientBase instance without override,
        When: _retry_subscribe is called,
        Then: NotImplementedError is raised.
        """
        client = HealthLoopClient()
        with pytest.raises(NotImplementedError, match="must implement"):
            await ExchangeClientBase._retry_subscribe(client, "ticker", "BTC/USD")
