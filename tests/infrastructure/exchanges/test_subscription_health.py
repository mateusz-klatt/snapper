"""Tests for per-symbol subscription health tracking."""

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
        """Replay pending can preserve retry accounting.

        Given: A confirmed entry with consumed retry budget,
        When: mark_pending is called with preserve_retry_count=True,
        Then: The entry is pending and retry_count is unchanged.
        """
        tracker = SubscriptionHealthTracker()
        _set_clock(monkeypatch, 65.0)
        tracker.mark_pending("ticker", "BTC/USD")
        tracker.mark_retry_attempt("ticker", "BTC/USD")
        tracker.mark_confirmed("ticker", "BTC/USD")
        _set_clock(monkeypatch, 70.0)
        tracker.mark_pending("ticker", "BTC/USD", preserve_retry_count=True)
        entry = tracker.snapshot()[("ticker", "BTC/USD")]
        assert (entry.status, entry.retry_count, entry.requested_at) == ("pending", 1, 70.0)

    def test_pending_resets_retry_count_for_fresh_subscribe(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Fresh pending resets retry accounting.

        Given: A confirmed entry with consumed retry budget,
        When: mark_pending is called without preserve_retry_count,
        Then: retry_count is reset to zero.
        """
        tracker = SubscriptionHealthTracker()
        _set_clock(monkeypatch, 75.0)
        tracker.mark_pending("ticker", "BTC/USD")
        tracker.mark_retry_attempt("ticker", "BTC/USD")
        tracker.mark_confirmed("ticker", "BTC/USD")
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
        ]
        results = []
        for factory in factories:
            with pytest.raises(ValueError):
                factory()
            results.append(True)
        assert results == [True, True, True, True]

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
