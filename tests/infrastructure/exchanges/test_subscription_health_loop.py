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
        client.start_health_loop()
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
        client.start_health_loop()
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
        client.start_health_loop()
        first_task = client._health_loop_task
        try:
            client.start_health_loop()
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
    async def test_loop_logs_warning_not_error_on_first_subscribe_exhaustion(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A first fast-budget exhaustion backs off at WARNING, never ERROR.

        Given: A pending entry with no retry budget,
        When: The health loop runs one tick and the entry exhausts its budget,
        Then: Exactly one "backing off" record is emitted at WARNING and no
            ERROR record is produced, so benign per-symbol backoff stays off
            the operator alert tier (venue-wide darkness is escalated to
            ERROR elsewhere by the publisher liveness watchdog).
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
        sink_id = logger.add(caplog.handler, format="{message}", level="DEBUG")
        try:
            client._health_loop_running = True
            await client._subscription_health_loop()
        finally:
            logger.remove(sink_id)
        backoff_logs = [rec for rec in caplog.records if "backing off" in rec.message]
        error_logs = [rec for rec in caplog.records if rec.levelname == "ERROR"]
        assert (len(backoff_logs), backoff_logs[0].levelname, error_logs) == (1, "WARNING", [])

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
        """Stale confirmed entries surface as an aggregated WARNING, not per-symbol.

        Given: A single confirmed entry with no data for longer than threshold,
        When: The health loop runs one tick,
        Then: One aggregated WARNING is emitted ("N stale subscription(s)") AND
            the entry is NOT retried (stale-data path is observe-only).
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
        aggregate_logs = [rec for rec in caplog.records if "stale subscription" in rec.message]
        assert (client.retry_calls, len(aggregate_logs)) == ([], 1)
        assert "ticker/BTC/USD" in aggregate_logs[0].message
        assert "ticker=1" in aggregate_logs[0].message

    @pytest.mark.asyncio
    async def test_loop_aggregates_multi_entry_stale_into_single_warning(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Many stale entries collapse into one aggregated WARNING per loop tick.

        Given: Three confirmed entries (mixed channels) all past the stale threshold,
        When: The health loop runs one tick,
        Then: Exactly ONE WARNING is emitted containing the total count,
            per-channel breakdown sorted by channel, AND the worst-stale
            symbol with its silence duration.
        """
        client = HealthLoopClient()
        tracker = SubscriptionHealthTracker(
            ack_timeout_s=1.0,
            retry_interval_s=1.0,
            data_stale_threshold_s=5.0,
        )
        client._health_tracker = tracker
        monkeypatch.setattr(exchange_base.time, "monotonic", lambda: 100.0)
        tracker.mark_confirmed("ticker", "BTC/USD")
        monkeypatch.setattr(exchange_base.time, "monotonic", lambda: 110.0)
        tracker.mark_confirmed("ticker", "ETH/USD")
        monkeypatch.setattr(exchange_base.time, "monotonic", lambda: 120.0)
        tracker.mark_confirmed("trade", "SOL/USD")
        monkeypatch.setattr(exchange_base.time, "monotonic", lambda: 200.0)

        async def stop_after_sleep(_: float) -> None:
            client._health_loop_running = False

        monkeypatch.setattr(exchange_base.asyncio, "sleep", stop_after_sleep)
        sink_id = logger.add(caplog.handler, format="{message}", level="WARNING")
        try:
            client._health_loop_running = True
            await client._subscription_health_loop()
        finally:
            logger.remove(sink_id)
        aggregate_logs = [rec for rec in caplog.records if "stale subscription" in rec.message]
        assert len(aggregate_logs) == 1
        message = aggregate_logs[0].message
        assert "3 stale subscription(s)" in message
        assert "ticker=2" in message
        assert "trade=1" in message
        assert message.index("ticker") < message.index("trade")
        assert "ticker/BTC/USD" in message

    @pytest.mark.asyncio
    async def test_loop_emits_per_entry_debug_records(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Per-symbol detail is preserved at DEBUG level for diagnostics.

        Given: Two stale entries,
        When: The health loop runs one tick with a DEBUG-level sink attached,
        Then: One aggregated WARNING is emitted PLUS one DEBUG per entry
            ("subscribed to <channel>/<symbol> but no data for Ns").
        """
        client = HealthLoopClient()
        tracker = SubscriptionHealthTracker(
            ack_timeout_s=1.0,
            retry_interval_s=1.0,
            data_stale_threshold_s=5.0,
        )
        client._health_tracker = tracker
        monkeypatch.setattr(exchange_base.time, "monotonic", lambda: 50.0)
        tracker.mark_confirmed("ticker", "AAA/USD")
        tracker.mark_confirmed("ticker", "BBB/USD")
        monkeypatch.setattr(exchange_base.time, "monotonic", lambda: 80.0)

        async def stop_after_sleep(_: float) -> None:
            client._health_loop_running = False

        monkeypatch.setattr(exchange_base.asyncio, "sleep", stop_after_sleep)
        sink_id = logger.add(caplog.handler, format="{message}", level="DEBUG")
        try:
            client._health_loop_running = True
            await client._subscription_health_loop()
        finally:
            logger.remove(sink_id)
        per_entry_debugs = [
            rec
            for rec in caplog.records
            if rec.levelname == "DEBUG" and "but no data for" in rec.message
        ]
        symbols_in_debug = {
            symbol
            for symbol in ("AAA/USD", "BBB/USD")
            if any(symbol in rec.message for rec in per_entry_debugs)
        }
        assert len(per_entry_debugs) == 2
        assert symbols_in_debug == {"AAA/USD", "BBB/USD"}

    @pytest.mark.asyncio
    async def test_loop_slow_retries_due_failed_entry(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A backed-off failed subscription is reissued when its delay elapses.

        Given: A failed entry whose slow-retry backoff delay has passed,
        When: The health loop runs one tick,
        Then: It is re-subscribed and returns to pending with the slow
            escalation counter incremented.
        """
        client = HealthLoopClient()
        tracker = SubscriptionHealthTracker(
            ack_timeout_s=1.0,
            retry_interval_s=1.0,
            max_retries=0,
            slow_retry_base_s=60.0,
            slow_retry_jitter=0.0,
        )
        client._health_tracker = tracker
        monkeypatch.setattr(exchange_base.time, "monotonic", lambda: 0.0)
        tracker.mark_pending("trade", "AAVE/BTC")
        tracker.mark_retry_attempt("trade", "AAVE/BTC")
        monkeypatch.setattr(exchange_base.time, "monotonic", lambda: 200.0)

        async def stop_after_sleep(_: float) -> None:
            client._health_loop_running = False

        monkeypatch.setattr(exchange_base.asyncio, "sleep", stop_after_sleep)
        client._health_loop_running = True
        await client._subscription_health_loop()
        entry = tracker.snapshot()[("trade", "AAVE/BTC")]
        assert (client.retry_calls, entry.status, entry.slow_retry_count) == (
            [("trade", "AAVE/BTC")],
            "pending",
            1,
        )

    @pytest.mark.asyncio
    async def test_loop_logs_debug_on_re_failure_after_slow_retry(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A slow-retried subscription that re-fails logs DEBUG, not WARNING.

        Given: A pending entry already escalated by one slow retry,
        When: Its ACK window expires and the health loop runs one tick,
        Then: It re-fails with a DEBUG record (not the first-failure WARNING)
            and is not re-subscribed in the same tick.
        """
        client = HealthLoopClient()
        tracker = SubscriptionHealthTracker(
            ack_timeout_s=1.0,
            retry_interval_s=1.0,
            max_retries=0,
            slow_retry_base_s=60.0,
            slow_retry_jitter=0.0,
        )
        client._health_tracker = tracker
        monkeypatch.setattr(exchange_base.time, "monotonic", lambda: 0.0)
        tracker.mark_pending("trade", "AAVE/BTC")
        tracker.mark_retry_attempt("trade", "AAVE/BTC")
        tracker.mark_slow_retry("trade", "AAVE/BTC")
        monkeypatch.setattr(exchange_base.time, "monotonic", lambda: 100.0)

        async def stop_after_sleep(_: float) -> None:
            client._health_loop_running = False

        monkeypatch.setattr(exchange_base.asyncio, "sleep", stop_after_sleep)
        sink_id = logger.add(caplog.handler, format="{message}", level="DEBUG")
        try:
            client._health_loop_running = True
            await client._subscription_health_loop()
        finally:
            logger.remove(sink_id)
        debug_logs = [rec for rec in caplog.records if "after slow retry" in rec.message]
        entry = tracker.snapshot()[("trade", "AAVE/BTC")]
        assert (client.retry_calls, len(debug_logs), entry.status) == ([], 1, "failed")

    @pytest.mark.asyncio
    async def test_loop_warns_when_slow_retry_subscribe_raises(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A raising slow-retry subscribe is caught and warned.

        Given: A due failed entry and a retry method that raises,
        When: The health loop runs one tick,
        Then: The exception is caught and one slow-retry WARNING is emitted.
        """
        client = HealthLoopClient()
        client.retry_error = RuntimeError("ws down")
        tracker = SubscriptionHealthTracker(
            ack_timeout_s=1.0,
            retry_interval_s=1.0,
            max_retries=0,
            slow_retry_base_s=60.0,
            slow_retry_jitter=0.0,
        )
        client._health_tracker = tracker
        monkeypatch.setattr(exchange_base.time, "monotonic", lambda: 0.0)
        tracker.mark_pending("trade", "AAVE/BTC")
        tracker.mark_retry_attempt("trade", "AAVE/BTC")
        monkeypatch.setattr(exchange_base.time, "monotonic", lambda: 200.0)

        async def stop_after_sleep(_: float) -> None:
            client._health_loop_running = False

        monkeypatch.setattr(exchange_base.asyncio, "sleep", stop_after_sleep)
        sink_id = logger.add(caplog.handler, format="{message}", level="WARNING")
        try:
            client._health_loop_running = True
            await client._subscription_health_loop()
        finally:
            logger.remove(sink_id)
        warnings = [rec for rec in caplog.records if "retry subscribe raised" in rec.message]
        assert (client.retry_calls, len(warnings)) == ([("trade", "AAVE/BTC")], 1)

    @pytest.mark.asyncio
    async def test_loop_skips_slow_retry_when_entry_changes_mid_loop(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A due entry confirmed while awaiting a sibling is not re-subscribed.

        Given: Two due failed entries,
        When: Subscribing the first confirms the second mid-loop,
        Then: mark_slow_retry returns False for the second and only the
            first is re-subscribed.
        """
        client = HealthLoopClient()
        tracker = SubscriptionHealthTracker(
            ack_timeout_s=1.0,
            retry_interval_s=1.0,
            max_retries=0,
            slow_retry_base_s=60.0,
            slow_retry_jitter=0.0,
        )
        client._health_tracker = tracker
        monkeypatch.setattr(exchange_base.time, "monotonic", lambda: 0.0)
        tracker.mark_pending("trade", "AAA/USD")
        tracker.mark_retry_attempt("trade", "AAA/USD")
        tracker.mark_pending("trade", "BBB/USD")
        tracker.mark_retry_attempt("trade", "BBB/USD")
        monkeypatch.setattr(exchange_base.time, "monotonic", lambda: 200.0)
        calls: list[tuple[str, str]] = []

        async def confirming_retry(channel: str, symbol: str) -> None:
            calls.append((channel, symbol))
            tracker.mark_confirmed("trade", "BBB/USD")

        monkeypatch.setattr(client, "_retry_subscribe", confirming_retry)

        async def stop_after_sleep(_: float) -> None:
            client._health_loop_running = False

        monkeypatch.setattr(exchange_base.asyncio, "sleep", stop_after_sleep)
        client._health_loop_running = True
        await client._subscription_health_loop()
        assert calls == [("trade", "AAA/USD")]

    @pytest.mark.asyncio
    async def test_loop_pass1_no_false_failure_log_when_entry_recovers_mid_loop(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """An overdue entry confirmed mid-await emits no false failure log.

        Given: Two overdue pending entries,
        When: Retrying the first confirms the second (late data) while the
            loop is awaiting the first subscribe,
        Then: The second, now healthy, produces no "backing off" failure
            record (its mark_retry_attempt returns False only because it is
            no longer pending, not because it exhausted its budget).
        """
        client = HealthLoopClient()
        tracker = SubscriptionHealthTracker(
            ack_timeout_s=1.0, retry_interval_s=1.0, max_retries=3, slow_retry_jitter=0.0
        )
        client._health_tracker = tracker
        monkeypatch.setattr(exchange_base.time, "monotonic", lambda: 0.0)
        tracker.mark_pending("trade", "AAA/USD")
        tracker.mark_pending("trade", "BBB/USD")
        monkeypatch.setattr(exchange_base.time, "monotonic", lambda: 100.0)

        async def confirming_retry(channel: str, symbol: str) -> None:
            tracker.mark_data_seen("trade", "BBB/USD")

        monkeypatch.setattr(client, "_retry_subscribe", confirming_retry)

        async def stop_after_sleep(_: float) -> None:
            client._health_loop_running = False

        monkeypatch.setattr(exchange_base.asyncio, "sleep", stop_after_sleep)
        sink_id = logger.add(caplog.handler, format="{message}", level="DEBUG")
        try:
            client._health_loop_running = True
            await client._subscription_health_loop()
        finally:
            logger.remove(sink_id)
        failure_logs = [rec for rec in caplog.records if "backing off" in rec.message]
        recovered = tracker.snapshot()[("trade", "BBB/USD")]
        assert (failure_logs, recovered.status) == ([], "confirmed")

    @pytest.mark.asyncio
    async def test_loop_pass1_skips_entry_replaced_by_reconnect_mid_loop(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A reconnect replay replacing an entry mid-loop is not stale-retried.

        Given: Two overdue pending entries,
        When: Retrying the first triggers a reconnect-style mark_pending that
            REPLACES the second entry object while the loop still holds the
            stale listed object,
        Then: the second is neither re-subscribed (no premature retry before
            its fresh ACK window) nor logged as failed; it stays freshly
            pending.
        """
        client = HealthLoopClient()
        tracker = SubscriptionHealthTracker(
            ack_timeout_s=1.0, retry_interval_s=1.0, max_retries=3, slow_retry_jitter=0.0
        )
        client._health_tracker = tracker
        monkeypatch.setattr(exchange_base.time, "monotonic", lambda: 0.0)
        tracker.mark_pending("trade", "AAA/USD")
        tracker.mark_pending("trade", "BBB/USD")
        monkeypatch.setattr(exchange_base.time, "monotonic", lambda: 100.0)
        calls: list[tuple[str, str]] = []

        async def replaying_retry(channel: str, symbol: str) -> None:
            calls.append((channel, symbol))
            if symbol == "AAA/USD":
                tracker.mark_pending("trade", "BBB/USD", preserve_retry_count=True)

        monkeypatch.setattr(client, "_retry_subscribe", replaying_retry)

        async def stop_after_sleep(_: float) -> None:
            client._health_loop_running = False

        monkeypatch.setattr(exchange_base.asyncio, "sleep", stop_after_sleep)
        sink_id = logger.add(caplog.handler, format="{message}", level="DEBUG")
        try:
            client._health_loop_running = True
            await client._subscription_health_loop()
        finally:
            logger.remove(sink_id)
        failure_logs = [rec for rec in caplog.records if "backing off" in rec.message]
        replaced = tracker.snapshot()[("trade", "BBB/USD")]
        assert (calls, failure_logs, replaced.status, replaced.requested_at) == (
            [("trade", "AAA/USD")],
            [],
            "pending",
            100.0,
        )

    @pytest.mark.asyncio
    async def test_loop_recovers_dark_confirmed_subscription(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A confirmed subscription dark past the threshold is re-subscribed.

        Given: A confirmed entry with no data for longer than the dark
            recovery threshold,
        When: The health loop runs one tick,
        Then: It is re-subscribed (INFO), returns to pending, and its
            dark_recovery_count is incremented.
        """
        client = HealthLoopClient()
        tracker = SubscriptionHealthTracker(
            ack_timeout_s=1.0,
            retry_interval_s=1.0,
            data_stale_threshold_s=100.0,
            dark_recovery_threshold_multiplier=3.0,
            slow_retry_jitter=0.0,
            retry_subscribe_spacing_s=0.0,
        )
        client._health_tracker = tracker
        monkeypatch.setattr(exchange_base.time, "monotonic", lambda: 0.0)
        tracker.mark_confirmed("ticker", "DARK/USD")
        monkeypatch.setattr(exchange_base.time, "monotonic", lambda: 400.0)

        async def stop_after_sleep(_: float) -> None:
            client._health_loop_running = False

        monkeypatch.setattr(exchange_base.asyncio, "sleep", stop_after_sleep)
        sink_id = logger.add(caplog.handler, format="{message}", level="INFO")
        try:
            client._health_loop_running = True
            await client._subscription_health_loop()
        finally:
            logger.remove(sink_id)
        info_logs = [rec for rec in caplog.records if "dark-recovery re-subscribe" in rec.message]
        entry = tracker.snapshot()[("ticker", "DARK/USD")]
        assert (client.retry_calls, len(info_logs), entry.status, entry.dark_recovery_count) == (
            [("ticker", "DARK/USD")],
            1,
            "pending",
            1,
        )

    @pytest.mark.asyncio
    async def test_loop_dark_recovery_skips_entry_changed_mid_loop(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A dark entry replaced mid-loop is not stale-recovered.

        Given: Two dark confirmed entries,
        When: Recovering the first replaces the second via mark_pending
            while the loop is awaiting the first subscribe,
        Then: The second is not re-subscribed (its mark_dark_recovery
            returns False on the now-pending replacement).
        """
        client = HealthLoopClient()
        tracker = SubscriptionHealthTracker(
            ack_timeout_s=1.0,
            retry_interval_s=1.0,
            data_stale_threshold_s=100.0,
            dark_recovery_threshold_multiplier=3.0,
            slow_retry_jitter=0.0,
            retry_subscribe_spacing_s=0.0,
        )
        client._health_tracker = tracker
        monkeypatch.setattr(exchange_base.time, "monotonic", lambda: 0.0)
        tracker.mark_confirmed("ticker", "AAA/USD")
        tracker.mark_confirmed("ticker", "BBB/USD")
        monkeypatch.setattr(exchange_base.time, "monotonic", lambda: 400.0)
        calls: list[tuple[str, str]] = []

        async def changing_retry(channel: str, symbol: str) -> None:
            calls.append((channel, symbol))
            if symbol == "AAA/USD":
                tracker.mark_pending("ticker", "BBB/USD")

        monkeypatch.setattr(client, "_retry_subscribe", changing_retry)

        async def stop_after_sleep(_: float) -> None:
            client._health_loop_running = False

        monkeypatch.setattr(exchange_base.asyncio, "sleep", stop_after_sleep)
        client._health_loop_running = True
        await client._subscription_health_loop()
        bbb = tracker.snapshot()[("ticker", "BBB/USD")]
        assert (calls, bbb.status, bbb.dark_recovery_count) == (
            [("ticker", "AAA/USD")],
            "pending",
            0,
        )

    @pytest.mark.asyncio
    async def test_loop_dark_recovery_skips_entry_fed_mid_loop(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A dark entry fed in place mid-sweep is not re-subscribed.

        Given: Two dark confirmed entries,
        When: Recovering the first feeds the second via mark_data_seen (in
            place, so it stays confirmed and the same object) while awaiting
            the first subscribe,
        Then: The second's dark re-check fails and it is not re-subscribed,
            so the shared subscribe budget is not wasted on a healthy symbol.
        """
        client = HealthLoopClient()
        tracker = SubscriptionHealthTracker(
            ack_timeout_s=1.0,
            retry_interval_s=1.0,
            data_stale_threshold_s=100.0,
            dark_recovery_threshold_multiplier=3.0,
            slow_retry_jitter=0.0,
            retry_subscribe_spacing_s=0.0,
        )
        client._health_tracker = tracker
        monkeypatch.setattr(exchange_base.time, "monotonic", lambda: 0.0)
        tracker.mark_confirmed("ticker", "AAA/USD")
        tracker.mark_confirmed("ticker", "BBB/USD")
        monkeypatch.setattr(exchange_base.time, "monotonic", lambda: 400.0)
        calls: list[tuple[str, str]] = []

        async def feeding_retry(channel: str, symbol: str) -> None:
            calls.append((channel, symbol))
            if symbol == "AAA/USD":
                tracker.mark_data_seen("ticker", "BBB/USD")

        monkeypatch.setattr(client, "_retry_subscribe", feeding_retry)

        async def stop_after_sleep(_: float) -> None:
            client._health_loop_running = False

        monkeypatch.setattr(exchange_base.asyncio, "sleep", stop_after_sleep)
        client._health_loop_running = True
        await client._subscription_health_loop()
        bbb = tracker.snapshot()[("ticker", "BBB/USD")]
        assert (calls, bbb.status, bbb.dark_recovery_count) == (
            [("ticker", "AAA/USD")],
            "confirmed",
            0,
        )

    @pytest.mark.asyncio
    async def test_loop_spaces_consecutive_retry_subscribes(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Each re-subscribe is followed by the configured spacing sleep.

        Given: Two overdue pending entries and a 0.5s subscribe spacing,
        When: The health loop runs one tick,
        Then: Both are re-subscribed and each send is followed by one 0.5s
            sleep, so a re-subscribe sweep stays under the message-rate gate.
        """
        client = HealthLoopClient()
        tracker = SubscriptionHealthTracker(
            ack_timeout_s=1.0, retry_interval_s=99.0, retry_subscribe_spacing_s=0.5
        )
        client._health_tracker = tracker
        monkeypatch.setattr(exchange_base.time, "monotonic", lambda: 0.0)
        tracker.mark_pending("ticker", "BTC/USD")
        tracker.mark_pending("trade", "ETH/USD")
        monkeypatch.setattr(exchange_base.time, "monotonic", lambda: 100.0)
        sleeps: list[float] = []

        async def record_sleep(seconds: float) -> None:
            sleeps.append(seconds)
            if seconds == 99.0:
                client._health_loop_running = False

        monkeypatch.setattr(exchange_base.asyncio, "sleep", record_sleep)
        client._health_loop_running = True
        await client._subscription_health_loop()
        assert (client.retry_calls, sleeps.count(0.5)) == (
            [("ticker", "BTC/USD"), ("trade", "ETH/USD")],
            2,
        )

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

    def test_worst_stale_entry_prefers_oldest_reference(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Worst stale entry selection updates when a later entry is older.

        Given: Two confirmed entries where the second has an older timestamp,
        When: The worst stale entry helper is called,
        Then: The second entry is returned.
        """
        tracker = SubscriptionHealthTracker()
        monkeypatch.setattr(exchange_base.time, "monotonic", lambda: 100.0)
        tracker.mark_confirmed("ticker", "recent")
        monkeypatch.setattr(exchange_base.time, "monotonic", lambda: 50.0)
        tracker.mark_confirmed("ticker", "older")
        entries = list(tracker.snapshot().values())
        worst = ExchangeClientBase._worst_stale_entry(entries, 200.0)
        assert worst.symbol == "older"
