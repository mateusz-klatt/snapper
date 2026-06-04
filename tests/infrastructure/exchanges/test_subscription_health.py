"""Tests for per-symbol subscription health tracking."""

import zlib
from collections.abc import Callable

import pytest

from snapper.infrastructure.exchanges import _subscription_health as health
from snapper.infrastructure.exchanges._subscription_health import SubscriptionHealthTracker
from snapper.infrastructure.exchanges._subscription_health import interval_to_label
from snapper.infrastructure.exchanges._subscription_health import label_to_interval


def _set_clock(monkeypatch: pytest.MonkeyPatch, value: float) -> None:
    """Set the tracker monotonic clock for deterministic assertions."""
    monkeypatch.setattr(health.time, "monotonic", lambda: value)


class TestSubscriptionHealthTrackerTransitions:
    """Tests for the subscription health state machine."""

    def test_pending_to_confirmed_via_ack(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Pending entry is confirmed by ACK.

        Given: A pending ticker subscription,
        When: mark_confirmed is called,
        Then: The entry is confirmed and its error is cleared.
        """
        tracker = SubscriptionHealthTracker()
        _set_clock(monkeypatch, 10.0)
        tracker.mark_pending("ticker", "BTC/USD")
        tracker.snapshot()[("ticker", "BTC/USD")].last_error = "local copy"
        _set_clock(monkeypatch, 12.0)
        tracker.mark_confirmed("ticker", "BTC/USD")
        entry = tracker.snapshot()[("ticker", "BTC/USD")]
        assert (entry.status, entry.confirmed_at, entry.last_error) == ("confirmed", 12.0, None)

    def test_ack_creates_confirmed_entry(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """ACK creates an entry when no pending row exists.

        Given: An empty tracker,
        When: mark_confirmed is called,
        Then: A confirmed entry is created.
        """
        tracker = SubscriptionHealthTracker()
        _set_clock(monkeypatch, 15.0)
        tracker.mark_confirmed("ticker", "ETH/USD")
        entry = tracker.snapshot()[("ticker", "ETH/USD")]
        assert (entry.status, entry.requested_at, entry.confirmed_at) == (
            "confirmed",
            15.0,
            15.0,
        )

    def test_pending_to_confirmed_via_data(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Data promotes a pending entry.

        Given: A pending trade subscription,
        When: market data is seen,
        Then: The entry is confirmed and last_seen_data_at is recorded.
        """
        tracker = SubscriptionHealthTracker()
        _set_clock(monkeypatch, 20.0)
        tracker.mark_pending("trade", "BTC/USD")
        _set_clock(monkeypatch, 25.0)
        tracker.mark_data_seen("trade", "BTC/USD")
        entry = tracker.snapshot()[("trade", "BTC/USD")]
        assert (entry.status, entry.confirmed_at, entry.last_seen_data_at) == (
            "confirmed",
            25.0,
            25.0,
        )

    def test_pending_to_failed_via_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Failure ACK marks a pending entry failed.

        Given: A pending trade subscription,
        When: mark_failed is called,
        Then: The entry is failed with the supplied error.
        """
        tracker = SubscriptionHealthTracker()
        _set_clock(monkeypatch, 30.0)
        tracker.mark_pending("trade", "BTC/USD")
        tracker.mark_failed("trade", "BTC/USD", "bad symbol")
        entry = tracker.snapshot()[("trade", "BTC/USD")]
        assert (entry.status, entry.last_error) == ("failed", "bad symbol")

    def test_error_creates_failed_entry(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Failure ACK creates a failed entry when absent.

        Given: An empty tracker,
        When: mark_failed is called,
        Then: A failed entry is created.
        """
        tracker = SubscriptionHealthTracker()
        _set_clock(monkeypatch, 35.0)
        tracker.mark_failed("ticker", "BTC/USD", "rejected")
        entry = tracker.snapshot()[("ticker", "BTC/USD")]
        assert (entry.status, entry.requested_at, entry.last_error) == (
            "failed",
            35.0,
            "rejected",
        )

    def test_retry_attempt_under_budget_refreshes_pending(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Retry attempt consumes budget while staying pending.

        Given: A pending entry below max_retries,
        When: mark_retry_attempt is called,
        Then: retry_count increments and requested_at refreshes.
        """
        tracker = SubscriptionHealthTracker(max_retries=2)
        _set_clock(monkeypatch, 40.0)
        tracker.mark_pending("ticker", "BTC/USD")
        _set_clock(monkeypatch, 45.0)
        should_retry = tracker.mark_retry_attempt("ticker", "BTC/USD")
        entry = tracker.snapshot()[("ticker", "BTC/USD")]
        assert (should_retry, entry.status, entry.retry_count, entry.requested_at) == (
            True,
            "pending",
            1,
            45.0,
        )

    def test_retry_attempt_at_budget_fails_entry(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Retry budget exhaustion marks pending entry failed.

        Given: A pending entry with retry_count already at max_retries,
        When: mark_retry_attempt is called,
        Then: It returns False and fails the entry.
        """
        tracker = SubscriptionHealthTracker(max_retries=1)
        _set_clock(monkeypatch, 50.0)
        tracker.mark_pending("ticker", "BTC/USD")
        tracker.mark_retry_attempt("ticker", "BTC/USD")
        should_retry = tracker.mark_retry_attempt("ticker", "BTC/USD")
        entry = tracker.snapshot()[("ticker", "BTC/USD")]
        assert (should_retry, entry.status, entry.retry_count, entry.last_error) == (
            False,
            "failed",
            1,
            "retry budget exhausted",
        )

    def test_retry_attempt_does_not_overflow_budget(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Retry budget remains capped after exhaustion.

        Given: A pending entry that has exhausted retries,
        When: mark_retry_attempt is called repeatedly,
        Then: retry_count remains capped at max_retries.
        """
        tracker = SubscriptionHealthTracker(max_retries=0)
        _set_clock(monkeypatch, 55.0)
        tracker.mark_pending("ticker", "BTC/USD")
        tracker.mark_retry_attempt("ticker", "BTC/USD")
        tracker.mark_retry_attempt("ticker", "BTC/USD")
        entry = tracker.snapshot()[("ticker", "BTC/USD")]
        assert (entry.status, entry.retry_count) == ("failed", 0)

    def test_retry_attempt_on_missing_or_confirmed_returns_false(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Retry attempts only apply to pending entries.

        Given: A missing entry and a confirmed entry,
        When: mark_retry_attempt is called for both,
        Then: Both calls return False.
        """
        tracker = SubscriptionHealthTracker()
        _set_clock(monkeypatch, 60.0)
        tracker.mark_confirmed("ticker", "BTC/USD")
        missing_result = tracker.mark_retry_attempt("ticker", "ETH/USD")
        confirmed_result = tracker.mark_retry_attempt("ticker", "BTC/USD")
        assert (missing_result, confirmed_result) == (False, False)

    def test_pending_preserves_retry_count_on_replay(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Replay pending can preserve retry accounting for a struggling entry.

        Given: A still-pending entry with consumed retry budget (not yet
            recovered),
        When: mark_pending is called with preserve_retry_count=True,
        Then: The entry is pending and retry_count is unchanged, so a
            reconnect does not hand a perpetually-failing symbol a fresh
            fast budget.
        """
        tracker = SubscriptionHealthTracker()
        _set_clock(monkeypatch, 65.0)
        tracker.mark_pending("ticker", "BTC/USD")
        tracker.mark_retry_attempt("ticker", "BTC/USD")
        _set_clock(monkeypatch, 70.0)
        tracker.mark_pending("ticker", "BTC/USD", preserve_retry_count=True)
        entry = tracker.snapshot()[("ticker", "BTC/USD")]
        assert (entry.status, entry.retry_count, entry.requested_at) == ("pending", 1, 70.0)

    def test_pending_resets_retry_count_for_fresh_subscribe(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Fresh pending resets retry accounting.

        Given: A pending entry with consumed retry budget,
        When: mark_pending is called without preserve_retry_count,
        Then: retry_count is reset to zero.
        """
        tracker = SubscriptionHealthTracker()
        _set_clock(monkeypatch, 75.0)
        tracker.mark_pending("ticker", "BTC/USD")
        tracker.mark_retry_attempt("ticker", "BTC/USD")
        tracker.mark_pending("ticker", "BTC/USD")
        entry = tracker.snapshot()[("ticker", "BTC/USD")]
        assert (entry.status, entry.retry_count) == ("pending", 0)

    def test_failed_to_confirmed_on_late_ack(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Late ACK self-heals a failed entry.

        Given: A failed subscription entry,
        When: mark_confirmed is called,
        Then: The entry becomes confirmed and last_error is cleared.
        """
        tracker = SubscriptionHealthTracker()
        _set_clock(monkeypatch, 80.0)
        tracker.mark_failed("trade", "BTC/USD", "timeout")
        _set_clock(monkeypatch, 81.0)
        tracker.mark_confirmed("trade", "BTC/USD")
        entry = tracker.snapshot()[("trade", "BTC/USD")]
        assert (entry.status, entry.confirmed_at, entry.last_error) == ("confirmed", 81.0, None)

    def test_failed_to_confirmed_on_data(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Data self-heals a failed entry.

        Given: A failed subscription entry,
        When: mark_data_seen is called,
        Then: The entry becomes confirmed.
        """
        tracker = SubscriptionHealthTracker()
        _set_clock(monkeypatch, 82.0)
        tracker.mark_failed("trade", "BTC/USD", "timeout")
        _set_clock(monkeypatch, 83.0)
        tracker.mark_data_seen("trade", "BTC/USD")
        entry = tracker.snapshot()[("trade", "BTC/USD")]
        assert (entry.status, entry.confirmed_at, entry.last_error, entry.last_seen_data_at) == (
            "confirmed",
            83.0,
            None,
            83.0,
        )

    def test_data_on_confirmed_updates_only_last_seen(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Data on confirmed entry leaves confirmation metadata intact.

        Given: A confirmed entry,
        When: mark_data_seen is called later,
        Then: Only last_seen_data_at changes.
        """
        tracker = SubscriptionHealthTracker()
        _set_clock(monkeypatch, 85.0)
        tracker.mark_confirmed("ticker", "BTC/USD")
        _set_clock(monkeypatch, 90.0)
        tracker.mark_data_seen("ticker", "BTC/USD")
        entry = tracker.snapshot()[("ticker", "BTC/USD")]
        assert (entry.requested_at, entry.confirmed_at, entry.last_seen_data_at) == (
            85.0,
            85.0,
            90.0,
        )

    def test_data_creates_confirmed_entry(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Data creates a confirmed entry for unseen subscription identity.

        Given: An empty tracker,
        When: mark_data_seen is called,
        Then: requested_at, confirmed_at, and last_seen_data_at match now.
        """
        tracker = SubscriptionHealthTracker()
        _set_clock(monkeypatch, 95.0)
        tracker.mark_data_seen("ticker", "BTC/USD")
        entry = tracker.snapshot()[("ticker", "BTC/USD")]
        assert (entry.status, entry.requested_at, entry.confirmed_at, entry.last_seen_data_at) == (
            "confirmed",
            95.0,
            95.0,
            95.0,
        )

    def test_channel_keys_are_independent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Same symbol on different channels has independent entries.

        Given: One symbol pending on ticker and trade,
        When: The ticker entry is confirmed,
        Then: The trade entry remains pending.
        """
        tracker = SubscriptionHealthTracker()
        _set_clock(monkeypatch, 100.0)
        tracker.mark_pending("ticker", "BTC/USD")
        tracker.mark_pending("trade", "BTC/USD")
        tracker.mark_confirmed("ticker", "BTC/USD")
        snapshot = tracker.snapshot()
        assert (snapshot[("ticker", "BTC/USD")].status, snapshot[("trade", "BTC/USD")].status) == (
            "confirmed",
            "pending",
        )

    def test_ohlc_channel_keys_are_independent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """OHLC channel labels prevent interval collisions.

        Given: One symbol pending on 1m and 5m OHLC channels,
        When: The 1m entry is confirmed,
        Then: The 5m entry remains pending.
        """
        tracker = SubscriptionHealthTracker()
        _set_clock(monkeypatch, 105.0)
        tracker.mark_pending("ohlc:1m", "BTC/USD")
        tracker.mark_pending("ohlc:5m", "BTC/USD")
        tracker.mark_confirmed("ohlc:1m", "BTC/USD")
        snapshot = tracker.snapshot()
        assert (
            snapshot[("ohlc:1m", "BTC/USD")].status,
            snapshot[("ohlc:5m", "BTC/USD")].status,
        ) == ("confirmed", "pending")


class TestSubscriptionHealthQueries:
    """Tests for tracker query helpers."""

    def test_list_overdue_pending_filters_by_status_and_age(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Only old pending entries are overdue.

        Given: Pending, confirmed, and fresh pending entries,
        When: overdue entries are listed,
        Then: Only the old pending entry is returned.
        """
        tracker = SubscriptionHealthTracker(ack_timeout_s=10.0)
        _set_clock(monkeypatch, 200.0)
        tracker.mark_pending("ticker", "BTC/USD")
        tracker.mark_pending("ticker", "ETH/USD")
        tracker.mark_confirmed("ticker", "ETH/USD")
        _set_clock(monkeypatch, 209.0)
        tracker.mark_pending("ticker", "SOL/USD")
        overdue = tracker.list_overdue_pending(now=211.0)
        assert [(entry.channel, entry.symbol) for entry in overdue] == [("ticker", "BTC/USD")]

    def test_list_stale_data_filters_confirmed_entries(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Only confirmed entries without recent data are stale.

        Given: Confirmed stale, confirmed fresh, and pending entries,
        When: stale entries are listed,
        Then: Only the confirmed stale entry is returned.
        """
        tracker = SubscriptionHealthTracker(data_stale_threshold_s=30.0)
        _set_clock(monkeypatch, 300.0)
        tracker.mark_confirmed("ticker", "BTC/USD")
        tracker.mark_pending("ticker", "ETH/USD")
        _set_clock(monkeypatch, 325.0)
        tracker.mark_data_seen("ticker", "SOL/USD")
        stale = tracker.list_stale_data(now=331.0)
        assert [(entry.channel, entry.symbol) for entry in stale] == [("ticker", "BTC/USD")]

    def test_list_stale_data_uses_last_seen_when_present(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Recent data keeps an old confirmation out of stale results.

        Given: A confirmed entry with recent data,
        When: stale entries are listed after confirmation age exceeds threshold,
        Then: The entry is not stale.
        """
        tracker = SubscriptionHealthTracker(data_stale_threshold_s=30.0)
        _set_clock(monkeypatch, 400.0)
        tracker.mark_confirmed("ticker", "BTC/USD")
        _set_clock(monkeypatch, 425.0)
        tracker.mark_data_seen("ticker", "BTC/USD")
        assert tracker.list_stale_data(now=440.0) == []

    def test_reconfirm_grants_fresh_stale_window_after_old_data(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Re-confirmation re-arms the stale window despite older data.

        Given: A confirmed entry whose last data is far older than the
            stale threshold (e.g. a wildcard ticker symbol after a WS
            outage longer than the threshold),
        When: the subscription is re-confirmed on reconnect,
        Then: the entry is not immediately stale because the reference is
            the most recent of request/confirmation/data timestamps, and it
            surfaces as stale only once the threshold elapses from the
            re-confirmation rather than from the old data timestamp.
        """
        tracker = SubscriptionHealthTracker(data_stale_threshold_s=30.0)
        _set_clock(monkeypatch, 100.0)
        tracker.mark_confirmed("ticker", "BTC/USD")
        _set_clock(monkeypatch, 110.0)
        tracker.mark_data_seen("ticker", "BTC/USD")
        _set_clock(monkeypatch, 200.0)
        tracker.mark_confirmed("ticker", "BTC/USD")
        assert tracker.list_stale_data(now=210.0) == []
        re_stale = tracker.list_stale_data(now=235.0)
        assert [(entry.channel, entry.symbol) for entry in re_stale] == [("ticker", "BTC/USD")]

    def test_list_stale_data_is_log_once_per_window(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A stale entry is returned at most once until its window resets.

        Given: A confirmed entry that has crossed the stale threshold,
        When: list_stale_data is called repeatedly,
        Then: The entry surfaces only on the first call; subsequent calls
            skip it until ``mark_data_seen`` reopens the window.
        """
        tracker = SubscriptionHealthTracker(data_stale_threshold_s=30.0)
        _set_clock(monkeypatch, 500.0)
        tracker.mark_confirmed("ticker", "BTC/USD")
        first = tracker.list_stale_data(now=540.0)
        second = tracker.list_stale_data(now=600.0)
        third = tracker.list_stale_data(now=900.0)
        assert [(entry.channel, entry.symbol) for entry in first] == [("ticker", "BTC/USD")]
        assert second == []
        assert third == []

    def test_mark_data_seen_reopens_stale_window(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Fresh data clears the log-once flag so a future stall warns again.

        Given: A stale entry that has already been logged,
        When: data arrives and the entry goes stale again later,
        Then: list_stale_data surfaces it once more on the next stale check.
        """
        tracker = SubscriptionHealthTracker(data_stale_threshold_s=30.0)
        _set_clock(monkeypatch, 500.0)
        tracker.mark_confirmed("ticker", "BTC/USD")
        assert len(tracker.list_stale_data(now=540.0)) == 1
        _set_clock(monkeypatch, 600.0)
        tracker.mark_data_seen("ticker", "BTC/USD")
        assert tracker.list_stale_data(now=605.0) == []
        re_stale = tracker.list_stale_data(now=700.0)
        assert [(entry.channel, entry.symbol) for entry in re_stale] == [("ticker", "BTC/USD")]

    def test_mark_pending_reopens_stale_window(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A fresh subscribe re-arms the stale log.

        Given: A stale entry already surfaced once,
        When: mark_pending then mark_confirmed re-arm the subscription,
        Then: A subsequent stall is reported anew.
        """
        tracker = SubscriptionHealthTracker(data_stale_threshold_s=30.0)
        _set_clock(monkeypatch, 500.0)
        tracker.mark_confirmed("ticker", "BTC/USD")
        assert len(tracker.list_stale_data(now=540.0)) == 1
        _set_clock(monkeypatch, 600.0)
        tracker.mark_pending("ticker", "BTC/USD")
        tracker.mark_confirmed("ticker", "BTC/USD")
        re_stale = tracker.list_stale_data(now=700.0)
        assert [(entry.channel, entry.symbol) for entry in re_stale] == [("ticker", "BTC/USD")]

    def test_list_failed_returns_failed_entries(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Failed entries are listed separately.

        Given: One failed entry and one pending entry,
        When: failed entries are listed,
        Then: Only the failed entry is returned.
        """
        tracker = SubscriptionHealthTracker()
        _set_clock(monkeypatch, 500.0)
        tracker.mark_failed("ticker", "BTC/USD", "bad")
        tracker.mark_pending("ticker", "ETH/USD")
        failed = tracker.list_failed()
        assert [(entry.channel, entry.symbol) for entry in failed] == [("ticker", "BTC/USD")]

    def test_snapshot_returns_copies(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Snapshot cannot mutate tracker internals.

        Given: A tracker with one pending entry,
        When: A returned snapshot entry is mutated,
        Then: A fresh snapshot still has the original state.
        """
        tracker = SubscriptionHealthTracker()
        _set_clock(monkeypatch, 600.0)
        tracker.mark_pending("ticker", "BTC/USD")
        snapshot = tracker.snapshot()
        snapshot[("ticker", "BTC/USD")].status = "failed"
        assert tracker.snapshot()[("ticker", "BTC/USD")].status == "pending"


class TestSubscriptionHealthValidation:
    """Tests for tracker validation and interval helpers."""

    def test_init_rejects_invalid_values(self) -> None:
        """Constructor validates timing and retry budgets.

        Given: Invalid constructor arguments,
        When: A tracker is created for each,
        Then: ValueError is raised every time.
        """
        factories: list[Callable[[], SubscriptionHealthTracker]] = [
            lambda: SubscriptionHealthTracker(ack_timeout_s=0.0),
            lambda: SubscriptionHealthTracker(retry_interval_s=0.0),
            lambda: SubscriptionHealthTracker(max_retries=-1),
            lambda: SubscriptionHealthTracker(data_stale_threshold_s=0.0),
            lambda: SubscriptionHealthTracker(slow_retry_base_s=0.0),
            lambda: SubscriptionHealthTracker(slow_retry_multiplier=0.5),
            lambda: SubscriptionHealthTracker(slow_retry_cap_s=10.0),
            lambda: SubscriptionHealthTracker(slow_retry_jitter=1.0),
            lambda: SubscriptionHealthTracker(slow_retry_jitter=-0.1),
            lambda: SubscriptionHealthTracker(dark_recovery_threshold_multiplier=0.5),
            lambda: SubscriptionHealthTracker(retry_subscribe_spacing_s=-1.0),
        ]
        results = []
        for factory in factories:
            with pytest.raises(ValueError):
                factory()
            results.append(True)
        assert results == [True] * 11

    def test_methods_reject_empty_channel(self) -> None:
        """Tracker methods reject empty channels before mutation.

        Given: A fresh tracker,
        When: Each mutating method receives an empty channel,
        Then: ValueError is raised and the tracker remains empty.
        """
        tracker = SubscriptionHealthTracker()
        with pytest.raises(ValueError):
            tracker.mark_pending("", "BTC/USD")
        with pytest.raises(ValueError):
            tracker.mark_confirmed("", "BTC/USD")
        with pytest.raises(ValueError):
            tracker.mark_failed("", "BTC/USD", "bad")
        with pytest.raises(ValueError):
            tracker.mark_data_seen("", "BTC/USD")
        with pytest.raises(ValueError):
            tracker.mark_retry_attempt("", "BTC/USD")
        assert tracker.snapshot() == {}

    def test_methods_reject_empty_symbol(self) -> None:
        """Tracker methods reject empty symbols before mutation.

        Given: A fresh tracker,
        When: Each mutating method receives an empty symbol,
        Then: ValueError is raised and the tracker remains empty.
        """
        tracker = SubscriptionHealthTracker()
        with pytest.raises(ValueError):
            tracker.mark_pending("ticker", "")
        with pytest.raises(ValueError):
            tracker.mark_confirmed("ticker", "")
        with pytest.raises(ValueError):
            tracker.mark_failed("ticker", "", "bad")
        with pytest.raises(ValueError):
            tracker.mark_data_seen("ticker", "")
        with pytest.raises(ValueError):
            tracker.mark_retry_attempt("ticker", "")
        assert tracker.snapshot() == {}

    def test_interval_label_round_trip(self) -> None:
        """Accepted OHLC interval labels round-trip.

        Given: Every supported interval label,
        When: label_to_interval and interval_to_label are composed,
        Then: The original label is returned.
        """
        labels = ["1m", "5m", "15m", "30m", "1h", "4h", "1d"]
        assert [interval_to_label(label_to_interval(label)) for label in labels] == labels

    def test_unknown_interval_raises(self) -> None:
        """Unknown OHLC interval fails fast.

        Given: An unsupported interval,
        When: interval_to_label is called,
        Then: ValueError is raised.
        """
        with pytest.raises(ValueError, match="Unsupported Kraken OHLC interval"):
            interval_to_label(2)

    def test_unknown_interval_label_raises(self) -> None:
        """Unknown OHLC interval label fails fast.

        Given: An unsupported interval label,
        When: label_to_interval is called,
        Then: ValueError is raised.
        """
        with pytest.raises(ValueError, match="Unsupported Kraken OHLC interval label"):
            label_to_interval("2m")


class TestSlowRetryBackoff:
    """Tests for non-terminal failed backoff and slow retries."""

    def test_budget_exhaustion_schedules_next_attempt(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Exhausting the fast budget schedules a slow retry, not death.

        Given: A pending entry with no remaining fast retry budget,
        When: mark_retry_attempt exhausts the budget,
        Then: The entry is failed with next_attempt_at one base delay ahead.
        """
        tracker = SubscriptionHealthTracker(
            max_retries=0, slow_retry_base_s=60.0, slow_retry_jitter=0.0
        )
        _set_clock(monkeypatch, 100.0)
        tracker.mark_pending("trade", "AAVE/BTC")
        result = tracker.mark_retry_attempt("trade", "AAVE/BTC")
        entry = tracker.snapshot()[("trade", "AAVE/BTC")]
        assert (result, entry.status, entry.next_attempt_at, entry.slow_retry_count) == (
            False,
            "failed",
            160.0,
            0,
        )

    def test_list_due_failed_filters_by_schedule_and_status(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Only scheduled, due, failed entries surface for slow retry.

        Given: A due backed-off entry, a not-yet-due backed-off entry, and a
            directly mark_failed entry with no schedule,
        When: list_due_failed is queried at a time between the two schedules,
        Then: Only the due backed-off entry is returned.
        """
        tracker = SubscriptionHealthTracker(
            max_retries=0, slow_retry_base_s=60.0, slow_retry_jitter=0.0
        )
        _set_clock(monkeypatch, 0.0)
        tracker.mark_pending("trade", "DUE/USD")
        tracker.mark_retry_attempt("trade", "DUE/USD")
        _set_clock(monkeypatch, 1000.0)
        tracker.mark_pending("trade", "LATER/USD")
        tracker.mark_retry_attempt("trade", "LATER/USD")
        tracker.mark_failed("trade", "REJECTED/USD", "invalid pair")
        due = tracker.list_due_failed(now=100.0)
        assert [(entry.channel, entry.symbol) for entry in due] == [("trade", "DUE/USD")]

    def test_slow_retry_reissues_failed_for_one_window(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A due failed entry returns to pending for one more ACK window.

        Given: A backed-off failed entry that consumed two fast retries,
        When: mark_slow_retry is called,
        Then: It becomes pending again, next_attempt_at clears, the slow
            counter increments, and the fast retry_count is preserved.
        """
        tracker = SubscriptionHealthTracker(
            max_retries=2, slow_retry_base_s=60.0, slow_retry_jitter=0.0
        )
        _set_clock(monkeypatch, 0.0)
        tracker.mark_pending("trade", "AAVE/BTC")
        tracker.mark_retry_attempt("trade", "AAVE/BTC")
        tracker.mark_retry_attempt("trade", "AAVE/BTC")
        tracker.mark_retry_attempt("trade", "AAVE/BTC")
        _set_clock(monkeypatch, 500.0)
        result = tracker.mark_slow_retry("trade", "AAVE/BTC")
        entry = tracker.snapshot()[("trade", "AAVE/BTC")]
        assert (
            result,
            entry.status,
            entry.requested_at,
            entry.next_attempt_at,
            entry.retry_count,
            entry.slow_retry_count,
        ) == (True, "pending", 500.0, None, 2, 1)

    def test_slow_retry_returns_false_for_missing_or_non_failed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Slow retry only applies to failed entries.

        Given: A missing entry and a confirmed entry,
        When: mark_slow_retry is called for both,
        Then: Both calls return False.
        """
        tracker = SubscriptionHealthTracker()
        _set_clock(monkeypatch, 0.0)
        tracker.mark_confirmed("ticker", "BTC/USD")
        missing = tracker.mark_slow_retry("trade", "ETH/USD")
        confirmed = tracker.mark_slow_retry("ticker", "BTC/USD")
        assert (missing, confirmed) == (False, False)

    def test_reconnect_preserves_slow_retry_escalation(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Replay keeps the slow escalation so backoff does not restart.

        Given: A failed entry that has slow-retried twice,
        When: mark_pending replays it with preserve_retry_count,
        Then: retry_count and slow_retry_count carry over and the entry is
            pending with next_attempt_at cleared.
        """
        tracker = SubscriptionHealthTracker(
            max_retries=0, slow_retry_base_s=60.0, slow_retry_jitter=0.0
        )
        _set_clock(monkeypatch, 0.0)
        tracker.mark_pending("trade", "AAVE/BTC")
        tracker.mark_retry_attempt("trade", "AAVE/BTC")
        tracker.mark_slow_retry("trade", "AAVE/BTC")
        tracker.mark_retry_attempt("trade", "AAVE/BTC")
        tracker.mark_slow_retry("trade", "AAVE/BTC")
        _set_clock(monkeypatch, 900.0)
        tracker.mark_pending("trade", "AAVE/BTC", preserve_retry_count=True)
        entry = tracker.snapshot()[("trade", "AAVE/BTC")]
        assert (
            entry.status,
            entry.requested_at,
            entry.next_attempt_at,
            entry.retry_count,
            entry.slow_retry_count,
        ) == ("pending", 900.0, None, 0, 2)

    def test_backoff_grows_geometrically_and_caps(self) -> None:
        """Backoff grows by the multiplier and saturates at the cap.

        Given: A tracker with base 60, multiplier 5, cap 3600, no jitter,
        When: The delay is computed across escalation counts,
        Then: It follows 60, 1500, 3600 and stays at the cap thereafter.
        """
        tracker = SubscriptionHealthTracker(
            slow_retry_base_s=60.0,
            slow_retry_multiplier=5.0,
            slow_retry_cap_s=3600.0,
            slow_retry_jitter=0.0,
        )
        delays = [
            tracker._slow_backoff_delay(
                health._SymbolEntry("trade", "X/USD", "failed", 0.0, slow_retry_count=count)
            )
            for count in (0, 2, 3, 10)
        ]
        assert delays == [60.0, 1500.0, 3600.0, 3600.0]

    def test_backoff_constant_when_multiplier_is_one(self) -> None:
        """A unit multiplier yields a constant base delay.

        Given: A tracker with multiplier 1.0 and no jitter,
        When: The delay is computed at a high escalation count,
        Then: It equals the base delay (the exponent cap is zero).
        """
        tracker = SubscriptionHealthTracker(
            slow_retry_base_s=45.0,
            slow_retry_multiplier=1.0,
            slow_retry_cap_s=3600.0,
            slow_retry_jitter=0.0,
        )
        entry = health._SymbolEntry("trade", "X/USD", "failed", 0.0, slow_retry_count=7)
        assert tracker._slow_backoff_delay(entry) == 45.0

    def test_backoff_jitter_is_deterministic_and_bounded(self) -> None:
        """Jitter is deterministic per identity and within the configured band.

        Given: A tracker with 20% jitter,
        When: The same entry's delay is computed twice,
        Then: Repeated calls match the exact crc32-derived value and the
            result sits within the +/-20% band around the base delay.
        """
        tracker = SubscriptionHealthTracker(
            slow_retry_base_s=100.0,
            slow_retry_multiplier=5.0,
            slow_retry_cap_s=3600.0,
            slow_retry_jitter=0.2,
        )
        entry = health._SymbolEntry("trade", "AAVE/BTC", "failed", 0.0, slow_retry_count=0)
        fraction = zlib.crc32(b"trade|AAVE/BTC|0") / 0xFFFFFFFF
        expected = 100.0 * (1.0 + 0.2 * (2.0 * fraction - 1.0))
        first = tracker._slow_backoff_delay(entry)
        second = tracker._slow_backoff_delay(entry)
        assert (first, second) == (expected, expected)
        assert 80.0 <= first <= 120.0

    def test_jitter_never_exceeds_cap(self) -> None:
        """Jitter cannot push a capped delay above the hard cap.

        Given: A tracker whose escalation already saturates the cap and a
            jitter band that would otherwise overshoot it,
        When: The delay is computed,
        Then: It never exceeds slow_retry_cap_s.
        """
        tracker = SubscriptionHealthTracker(
            slow_retry_base_s=60.0,
            slow_retry_multiplier=5.0,
            slow_retry_cap_s=3600.0,
            slow_retry_jitter=0.2,
        )
        delays = [
            tracker._slow_backoff_delay(
                health._SymbolEntry("trade", symbol, "failed", 0.0, slow_retry_count=9)
            )
            for symbol in ("A/USD", "B/USD", "C/USD", "D/USD", "E/USD")
        ]
        assert all(delay <= 3600.0 for delay in delays)
        assert max(delays) > 2880.0

    def test_explicit_failure_after_schedule_is_terminal(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A late explicit rejection clears a scheduled slow retry.

        Given: A subscription scheduled for slow retry after exhausting its
            fast budget,
        When: An explicit mark_failed rejection arrives,
        Then: next_attempt_at is cleared and the entry is no longer due.
        """
        tracker = SubscriptionHealthTracker(
            max_retries=0, slow_retry_base_s=60.0, slow_retry_jitter=0.0
        )
        _set_clock(monkeypatch, 0.0)
        tracker.mark_pending("trade", "REJECT/USD")
        tracker.mark_retry_attempt("trade", "REJECT/USD")
        tracker.mark_failed("trade", "REJECT/USD", "invalid pair")
        entry = tracker.snapshot()[("trade", "REJECT/USD")]
        due = tracker.list_due_failed(now=10_000.0)
        assert (entry.status, entry.next_attempt_at, due) == ("failed", None, [])

    def test_slow_retry_rejects_terminal_failure(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Slow retry skips terminally failed entries with no schedule.

        Given: An explicitly failed entry with no scheduled slow retry,
        When: mark_slow_retry is called,
        Then: It returns False and the entry stays failed.
        """
        tracker = SubscriptionHealthTracker()
        _set_clock(monkeypatch, 0.0)
        tracker.mark_failed("trade", "REJECT/USD", "invalid pair")
        result = tracker.mark_slow_retry("trade", "REJECT/USD")
        entry = tracker.snapshot()[("trade", "REJECT/USD")]
        assert (result, entry.status) == (False, "failed")

    def test_reconnect_with_budget_preserves_fast_retries(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Replay with a non-zero budget keeps consumed fast and slow counts.

        Given: A failed entry that consumed a two-attempt fast budget and
            slow-retried once,
        When: mark_pending replays it with preserve_retry_count,
        Then: retry_count and slow_retry_count both carry over.
        """
        tracker = SubscriptionHealthTracker(
            max_retries=2, slow_retry_base_s=60.0, slow_retry_jitter=0.0
        )
        _set_clock(monkeypatch, 0.0)
        tracker.mark_pending("trade", "AAVE/BTC")
        tracker.mark_retry_attempt("trade", "AAVE/BTC")
        tracker.mark_retry_attempt("trade", "AAVE/BTC")
        tracker.mark_retry_attempt("trade", "AAVE/BTC")
        tracker.mark_slow_retry("trade", "AAVE/BTC")
        _set_clock(monkeypatch, 500.0)
        tracker.mark_pending("trade", "AAVE/BTC", preserve_retry_count=True)
        entry = tracker.snapshot()[("trade", "AAVE/BTC")]
        assert (entry.status, entry.retry_count, entry.slow_retry_count) == ("pending", 2, 1)

    def test_confirmation_clears_retry_history(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A late ACK on a backed-off entry wipes its retry history.

        Given: A subscription that exhausted its fast budget and slow-retried,
        When: mark_confirmed records a late ACK,
        Then: retry_count, slow_retry_count and next_attempt_at are reset so a
            future failure starts from a clean budget and re-alerts.
        """
        tracker = SubscriptionHealthTracker(
            max_retries=2, slow_retry_base_s=60.0, slow_retry_jitter=0.0
        )
        _set_clock(monkeypatch, 0.0)
        tracker.mark_pending("trade", "AAVE/BTC")
        tracker.mark_retry_attempt("trade", "AAVE/BTC")
        tracker.mark_retry_attempt("trade", "AAVE/BTC")
        tracker.mark_retry_attempt("trade", "AAVE/BTC")
        tracker.mark_slow_retry("trade", "AAVE/BTC")
        tracker.mark_confirmed("trade", "AAVE/BTC")
        entry = tracker.snapshot()[("trade", "AAVE/BTC")]
        assert (
            entry.status,
            entry.retry_count,
            entry.slow_retry_count,
            entry.next_attempt_at,
        ) == ("confirmed", 0, 0, None)

    def test_data_recovery_clears_retry_history(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Data arriving on a backed-off entry wipes its retry history.

        Given: A subscription failed with a scheduled slow retry after one
            slow escalation,
        When: mark_data_seen records incoming data,
        Then: it is confirmed with retry_count, slow_retry_count and
            next_attempt_at all reset.
        """
        tracker = SubscriptionHealthTracker(
            max_retries=0, slow_retry_base_s=60.0, slow_retry_jitter=0.0
        )
        _set_clock(monkeypatch, 0.0)
        tracker.mark_pending("trade", "AAVE/BTC")
        tracker.mark_retry_attempt("trade", "AAVE/BTC")
        tracker.mark_slow_retry("trade", "AAVE/BTC")
        tracker.mark_retry_attempt("trade", "AAVE/BTC")
        tracker.mark_data_seen("trade", "AAVE/BTC")
        entry = tracker.snapshot()[("trade", "AAVE/BTC")]
        assert (
            entry.status,
            entry.retry_count,
            entry.slow_retry_count,
            entry.next_attempt_at,
        ) == ("confirmed", 0, 0, None)

    def test_retry_attempt_skips_replaced_entry_via_expected(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The expected guard rejects a stale-listed entry after replacement.

        Given: A pending entry listed for retry, then replaced by a fresh
            mark_pending (simulating a concurrent reconnect replay),
        When: mark_retry_attempt is called with the stale listed object as
            expected,
        Then: it returns False and the fresh current entry is left untouched
            (no premature retry, no exhaustion applied to the wrong object).
        """
        tracker = SubscriptionHealthTracker(max_retries=3)
        _set_clock(monkeypatch, 0.0)
        tracker.mark_pending("trade", "AAVE/BTC")
        _set_clock(monkeypatch, 100.0)
        listed = tracker.list_overdue_pending()[0]
        tracker.mark_pending("trade", "AAVE/BTC", preserve_retry_count=True)
        result = tracker.mark_retry_attempt("trade", "AAVE/BTC", expected=listed)
        current = tracker.snapshot()[("trade", "AAVE/BTC")]
        assert (result, current.status, current.retry_count, current.requested_at) == (
            False,
            "pending",
            0,
            100.0,
        )


class TestDarkRecovery:
    """Tests for confirmed-but-dark subscription auto-recovery."""

    def test_dark_backoff_zero_first_then_geometric(self) -> None:
        """First dark recovery adds no delay; later ones escalate geometrically.

        Given: A tracker with base 60, multiplier 5, no jitter,
        When: The dark backoff is computed across escalation counts,
        Then: count 0 adds 0, then 60, 300, 1500 (geometric on count-1).
        """
        tracker = SubscriptionHealthTracker(
            slow_retry_base_s=60.0,
            slow_retry_multiplier=5.0,
            slow_retry_cap_s=3600.0,
            slow_retry_jitter=0.0,
        )
        delays = [
            tracker._dark_backoff_delay(
                health._SymbolEntry("trade", "X/USD", "confirmed", 0.0, dark_recovery_count=count)
            )
            for count in (0, 1, 2, 3)
        ]
        assert delays == [0.0, 60.0, 300.0, 1500.0]

    def test_list_due_dark_recovery_filters(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Only confirmed, enabled, sufficiently-dark entries are due.

        Given: A dark recoverable entry, a wildcard (disabled) entry, a
            freshly-fed entry, and a pending entry,
        When: list_due_dark_recovery is queried past the recovery threshold,
        Then: Only the dark recoverable entry is returned.
        """
        tracker = SubscriptionHealthTracker(
            data_stale_threshold_s=100.0,
            dark_recovery_threshold_multiplier=3.0,
            slow_retry_jitter=0.0,
        )
        _set_clock(monkeypatch, 0.0)
        tracker.mark_confirmed("trade", "DARK/USD")
        tracker.mark_confirmed("ticker", "WILD/USD", dark_recovery_enabled=False)
        tracker.mark_confirmed("trade", "FRESH/USD")
        tracker.mark_pending("trade", "PEND/USD")
        _set_clock(monkeypatch, 380.0)
        tracker.mark_data_seen("trade", "FRESH/USD")
        due = tracker.list_due_dark_recovery(now=400.0)
        assert [(entry.channel, entry.symbol) for entry in due] == [("trade", "DARK/USD")]

    def test_wildcard_confirm_disables_dark_recovery(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A wildcard-seeded confirm is never dark-recovered.

        Given: A confirmed entry seeded dark_recovery_enabled=False,
        When: It is dark far past the threshold,
        Then: It is excluded from list_due_dark_recovery.
        """
        tracker = SubscriptionHealthTracker(
            data_stale_threshold_s=100.0,
            dark_recovery_threshold_multiplier=3.0,
            slow_retry_jitter=0.0,
        )
        _set_clock(monkeypatch, 0.0)
        tracker.mark_confirmed("ticker", "WILD/USD", dark_recovery_enabled=False)
        entry = tracker.snapshot()[("ticker", "WILD/USD")]
        due = tracker.list_due_dark_recovery(now=1000.0)
        assert (entry.dark_recovery_enabled, due) == (False, [])

    def test_existing_confirm_updates_dark_recovery_enabled(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Re-confirming an existing entry updates its dark-recovery flag.

        Given: A per-symbol confirmed entry (recoverable by default),
        When: It is re-confirmed with dark_recovery_enabled=False,
        Then: The flag flips to False on the existing entry.
        """
        tracker = SubscriptionHealthTracker()
        _set_clock(monkeypatch, 0.0)
        tracker.mark_confirmed("ticker", "WILD/USD")
        tracker.mark_confirmed("ticker", "WILD/USD", dark_recovery_enabled=False)
        entry = tracker.snapshot()[("ticker", "WILD/USD")]
        assert entry.dark_recovery_enabled is False

    def test_mark_dark_recovery_transitions_confirmed_to_pending(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A dark confirmed entry is re-armed to pending for re-subscribe.

        Given: A confirmed entry dark past the recovery threshold,
        When: mark_dark_recovery is called,
        Then: It becomes pending with a fresh fast budget, cleared
            next_attempt_at, and an incremented dark_recovery_count.
        """
        tracker = SubscriptionHealthTracker(
            data_stale_threshold_s=100.0,
            dark_recovery_threshold_multiplier=3.0,
            slow_retry_jitter=0.0,
        )
        _set_clock(monkeypatch, 0.0)
        tracker.mark_confirmed("trade", "DARK/USD")
        _set_clock(monkeypatch, 400.0)
        result = tracker.mark_dark_recovery("trade", "DARK/USD")
        entry = tracker.snapshot()[("trade", "DARK/USD")]
        assert (
            result,
            entry.status,
            entry.requested_at,
            entry.retry_count,
            entry.next_attempt_at,
            entry.dark_recovery_count,
        ) == (True, "pending", 400.0, 0, None, 1)

    def test_mark_dark_recovery_false_for_ineligible(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Dark recovery skips missing, disabled, and non-confirmed entries.

        Given: A missing key, a wildcard (disabled) entry, and a failed entry,
        When: mark_dark_recovery is called for each,
        Then: All return False.
        """
        tracker = SubscriptionHealthTracker()
        _set_clock(monkeypatch, 0.0)
        tracker.mark_confirmed("ticker", "WILD/USD", dark_recovery_enabled=False)
        tracker.mark_failed("trade", "REJ/USD", "bad")
        missing = tracker.mark_dark_recovery("trade", "GONE/USD")
        disabled = tracker.mark_dark_recovery("ticker", "WILD/USD")
        failed = tracker.mark_dark_recovery("trade", "REJ/USD")
        assert (missing, disabled, failed) == (False, False, False)

    def test_mark_dark_recovery_skips_replaced_entry_via_expected(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The expected guard rejects a stale-listed entry after replacement.

        Given: A due dark entry that is replaced by a fresh confirm,
        When: mark_dark_recovery is called with the stale listed object,
        Then: It returns False and leaves the fresh entry confirmed.
        """
        tracker = SubscriptionHealthTracker(
            data_stale_threshold_s=100.0,
            dark_recovery_threshold_multiplier=3.0,
            slow_retry_jitter=0.0,
        )
        _set_clock(monkeypatch, 0.0)
        tracker.mark_confirmed("trade", "DARK/USD")
        _set_clock(monkeypatch, 400.0)
        listed = tracker.list_due_dark_recovery()[0]
        tracker.mark_pending("trade", "DARK/USD")
        tracker.mark_confirmed("trade", "DARK/USD")
        result = tracker.mark_dark_recovery("trade", "DARK/USD", expected=listed)
        current = tracker.snapshot()[("trade", "DARK/USD")]
        assert (result, current.status) == (False, "confirmed")

    def test_data_arrival_resets_dark_recovery_count(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Real data clears the dark-recovery escalation.

        Given: A dark entry re-armed once for recovery,
        When: market data arrives,
        Then: It is confirmed again with dark_recovery_count reset to zero.
        """
        tracker = SubscriptionHealthTracker(
            data_stale_threshold_s=100.0,
            dark_recovery_threshold_multiplier=3.0,
            slow_retry_jitter=0.0,
        )
        _set_clock(monkeypatch, 0.0)
        tracker.mark_confirmed("trade", "DARK/USD")
        _set_clock(monkeypatch, 400.0)
        assert tracker.mark_dark_recovery("trade", "DARK/USD") is True
        tracker.mark_data_seen("trade", "DARK/USD")
        entry = tracker.snapshot()[("trade", "DARK/USD")]
        assert (entry.status, entry.dark_recovery_count) == ("confirmed", 0)

    def test_reconnect_preserves_dark_recovery_count(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Replay keeps the dark escalation for a chronically dark symbol.

        Given: A dark entry re-armed once for recovery,
        When: mark_pending replays it with preserve_retry_count,
        Then: dark_recovery_count carries over.
        """
        tracker = SubscriptionHealthTracker(
            data_stale_threshold_s=100.0,
            dark_recovery_threshold_multiplier=3.0,
            slow_retry_jitter=0.0,
        )
        _set_clock(monkeypatch, 0.0)
        tracker.mark_confirmed("trade", "DARK/USD")
        _set_clock(monkeypatch, 400.0)
        assert tracker.mark_dark_recovery("trade", "DARK/USD") is True
        tracker.mark_pending("trade", "DARK/USD", preserve_retry_count=True)
        entry = tracker.snapshot()[("trade", "DARK/USD")]
        assert entry.dark_recovery_count == 1

    def test_mark_dark_recovery_false_when_no_longer_dark(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Recovery is skipped if the entry received data after being listed.

        Given: A confirmed entry that was dark-due but then got data in place,
        When: mark_dark_recovery is called,
        Then: The dark re-check fails and it returns False without
            re-subscribing an already-healthy symbol.
        """
        tracker = SubscriptionHealthTracker(
            data_stale_threshold_s=100.0,
            dark_recovery_threshold_multiplier=3.0,
            slow_retry_jitter=0.0,
        )
        _set_clock(monkeypatch, 0.0)
        tracker.mark_confirmed("trade", "DARK/USD")
        _set_clock(monkeypatch, 400.0)
        tracker.mark_data_seen("trade", "DARK/USD")
        result = tracker.mark_dark_recovery("trade", "DARK/USD")
        entry = tracker.snapshot()[("trade", "DARK/USD")]
        assert (result, entry.status, entry.dark_recovery_count) == (False, "confirmed", 0)
