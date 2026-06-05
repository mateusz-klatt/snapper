"""Per-symbol subscription health tracker shared by exchange clients.

The tracker records subscription state by ``(channel, symbol)`` using
wire-format symbols because subscribe calls, ACK envelopes, and retry
subscribes all speak the exchange WebSocket format. Account-wide private
subscriptions, such as Kraken Spot executions, are intentionally excluded:
there is no symbol identity to track and a one-symbol retry has no meaning.
"""

import time
import zlib
from dataclasses import dataclass
from dataclasses import replace
from math import ceil
from math import log
from typing import Literal

SubscriptionStatus = Literal["pending", "confirmed", "failed"]

_INTERVAL_TO_LABEL: dict[int, str] = {
    1: "1m",
    5: "5m",
    15: "15m",
    30: "30m",
    60: "1h",
    240: "4h",
    1440: "1d",
}
_LABEL_TO_INTERVAL: dict[str, int] = {
    label: interval for interval, label in _INTERVAL_TO_LABEL.items()
}


@dataclass(slots=True)
class _SymbolEntry:
    """State for one subscription identity.

    Attributes:
        channel: Tracker channel key, including parameters such as
            ``ohlc:1m`` where needed.
        symbol: Wire-format symbol or product id.
        status: Current state in the subscription lifecycle.
        requested_at: Monotonic time when the current subscribe attempt
            was issued.
        confirmed_at: Monotonic time when ACK or data confirmed the
            subscription.
        last_error: Last failure reason reported by the exchange.
        retry_count: Number of retry attempts already consumed.
        last_seen_data_at: Monotonic time when market data last arrived.
        stale_logged: True once the background loop has emitted a
            "subscribed but no data" warning for this entry's current
            stale window. Reset by ``mark_data_seen`` (data flowing
            again) and ``mark_pending`` (fresh subscribe), so a new
            warning fires only on transitions, not every retry tick.
            Prevents the log spam observed on 2026-05-24 (1.9M warnings
            in ~1h when CME weekend stalled all Equities + Futures).
        next_attempt_at: Monotonic time when a failed subscription
            becomes eligible for its next slow-retry attempt, or None
            when the entry is not in the slow-retry backoff schedule.
            Only the fast-budget-exhaustion path (a missed ACK, i.e.
            ``last_error == "retry budget exhausted"``) schedules this;
            explicit exchange rejections via ``mark_failed`` stay
            terminal so genuinely invalid subscriptions are not retried
            forever.
        slow_retry_count: Number of slow-retry escalations performed
            after the fast retry budget was exhausted. Drives the
            exponential backoff interval and, once above zero, marks the
            entry as already-reported so re-failures log at DEBUG rather
            than repeating the initial ERROR. Preserved across reconnect
            replay so a struggling subscription does not restart its
            backoff (and its noise) from scratch on every reconnect.
        dark_recovery_enabled: Whether a confirmed-but-dark subscription
            (no data for far longer than the stale threshold) may be
            auto-recovered by re-subscribing it per-symbol. False for
            wildcard-seeded entries (for example the Spot wildcard ticker
            universe) which have no per-symbol subscription to re-issue.
        dark_recovery_count: Number of dark-recovery re-subscribes issued
            for a confirmed subscription that went silent. Drives a
            backoff independent of ``slow_retry_count`` so a channel that
            keeps re-darkening is not hammered, and is reset only when
            real data arrives (``mark_data_seen``), not on an ACK-only
            ``mark_confirmed`` which can itself still be data-dark.
    """

    channel: str
    symbol: str
    status: SubscriptionStatus
    requested_at: float
    confirmed_at: float | None = None
    last_error: str | None = None
    retry_count: int = 0
    last_seen_data_at: float | None = None
    stale_logged: bool = False
    next_attempt_at: float | None = None
    slow_retry_count: int = 0
    dark_recovery_enabled: bool = True
    dark_recovery_count: int = 0

    def stale_reference(self) -> float:
        """Return the monotonic timestamp staleness is measured from.

        The reference is the MOST RECENT of ``requested_at``,
        ``confirmed_at`` and ``last_seen_data_at`` (ignoring unset
        timestamps). Using the latest of the three means a re-confirmation
        — for example a wildcard ticker universe re-seeded as confirmed
        after a WS reconnect — grants a fresh post-reconnect data window
        instead of flagging every previously-active symbol stale the
        instant the socket returns from an outage longer than the
        threshold, while ``last_seen_data_at`` is preserved for
        diagnostics. In steady state ``last_seen_data_at`` is the latest
        timestamp, so normal stale detection is unchanged. Both the stale
        decision (:meth:`SubscriptionHealthTracker.list_stale_data`) and
        the stale-age reporting in the publisher health loop read this
        single reference so they never diverge.

        Returns:
            The most recent of ``requested_at``, ``confirmed_at`` and
            ``last_seen_data_at`` in monotonic seconds.
        """
        return max(
            timestamp
            for timestamp in (self.requested_at, self.confirmed_at, self.last_seen_data_at)
            if timestamp is not None
        )


class SubscriptionHealthTracker:
    """Per-(channel, symbol) subscription state with retry queries.

    Attributes:
        retry_interval_s: How often the background retry task wakes up.
        ack_timeout_s: How long to wait for an ACK before a pending
            symbol is overdue.
        max_retries: Maximum fast retry attempts before a symbol enters
            slow-retry backoff.
        data_stale_threshold_s: Age after which confirmed subscriptions
            without data are surfaced for logging only.
        slow_retry_base_s: First slow-retry delay after the fast budget
            is exhausted.
        slow_retry_multiplier: Geometric growth factor applied to the
            slow-retry delay on each escalation.
        slow_retry_cap_s: Upper bound on the slow-retry delay so the
            schedule settles to a steady, infrequent cadence instead of
            growing without bound.
        slow_retry_jitter: Fractional deterministic jitter (0.0-1.0)
            applied to each slow-retry delay to desynchronise the large
            block of subscriptions that fail together during a boot
            subscribe storm, avoiding a synchronised retry herd.
        dark_recovery_threshold_multiplier: Multiple of
            ``data_stale_threshold_s`` a confirmed subscription must be
            dark before auto-recovery re-subscribes it. Larger than 1 so
            stale logging fires early (diagnostics) while recovery waits
            long enough not to churn legitimately quiet symbols.
        retry_subscribe_spacing_s: Minimum spacing the publisher health
            loop leaves between consecutive re-subscribe sends (overdue
            pending, slow-failed, and dark recovery) to stay under the
            exchange per-connection subscribe message-rate limit.
        dark_recovery_channels: Channels eligible for dark auto-recovery.
            Only CONTINUOUS channels belong here (default ``{"ticker"}``):
            a ticker updates on every quote, so silence past the threshold
            genuinely means the stream broke. Event-driven channels
            (``trade``, ``ohlc:*``) are intentionally excluded — for an
            illiquid pair "no data" is the normal sparse state, not a
            broken subscription, so re-subscribing them is pointless churn.
    """

    def __init__(
        self,
        *,
        ack_timeout_s: float = 15.0,
        retry_interval_s: float = 10.0,
        max_retries: int = 3,
        data_stale_threshold_s: float = 300.0,
        slow_retry_base_s: float = 60.0,
        slow_retry_multiplier: float = 5.0,
        slow_retry_cap_s: float = 3600.0,
        slow_retry_jitter: float = 0.2,
        dark_recovery_threshold_multiplier: float = 6.0,
        retry_subscribe_spacing_s: float = 1.0,
        dark_recovery_channels: frozenset[str] = frozenset({"ticker"}),
    ) -> None:
        """Initialize subscription health tracking.

        Args:
            ack_timeout_s: Seconds to wait for a subscribe ACK before
                listing a pending symbol as overdue.
            retry_interval_s: Seconds between retry-loop wakeups.
            max_retries: Number of fast retry attempts before a symbol
                enters slow-retry backoff.
            data_stale_threshold_s: Seconds without data before a
                confirmed subscription is listed as stale.
            slow_retry_base_s: First slow-retry delay, in seconds, after
                the fast budget is exhausted.
            slow_retry_multiplier: Geometric growth factor (>= 1.0) for
                the slow-retry delay on each escalation.
            slow_retry_cap_s: Maximum slow-retry delay in seconds; must
                be at least ``slow_retry_base_s``.
            slow_retry_jitter: Fractional jitter in [0.0, 1.0) applied
                deterministically to each slow-retry delay.
            dark_recovery_threshold_multiplier: Multiple (>= 1.0) of
                ``data_stale_threshold_s`` a confirmed subscription must
                be dark before auto-recovery re-subscribes it.
            retry_subscribe_spacing_s: Non-negative seconds the health
                loop spaces between consecutive re-subscribe sends.
            dark_recovery_channels: Channels eligible for dark
                auto-recovery; only continuous channels (default
                ``{"ticker"}``) where silence means a broken stream.
                Event-driven channels (``trade``, ``ohlc:*``) are
                excluded because sparse data is their normal state.

        Returns:
            None.

        Raises:
            ValueError: If any timing value is non-positive,
                ``max_retries`` is negative, ``slow_retry_multiplier`` is
                below 1.0, ``slow_retry_cap_s`` is below
                ``slow_retry_base_s``, ``slow_retry_jitter`` is outside
                ``[0.0, 1.0)``, ``dark_recovery_threshold_multiplier`` is
                below 1.0, or ``retry_subscribe_spacing_s`` is negative.
        """
        if ack_timeout_s <= 0:
            raise ValueError("ack_timeout_s must be positive")
        if retry_interval_s <= 0:
            raise ValueError("retry_interval_s must be positive")
        if max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        if data_stale_threshold_s <= 0:
            raise ValueError("data_stale_threshold_s must be positive")
        if slow_retry_base_s <= 0:
            raise ValueError("slow_retry_base_s must be positive")
        if slow_retry_multiplier < 1.0:
            raise ValueError("slow_retry_multiplier must be at least 1.0")
        if slow_retry_cap_s < slow_retry_base_s:
            raise ValueError("slow_retry_cap_s must be at least slow_retry_base_s")
        if not 0.0 <= slow_retry_jitter < 1.0:
            raise ValueError("slow_retry_jitter must be in [0.0, 1.0)")
        if dark_recovery_threshold_multiplier < 1.0:
            raise ValueError("dark_recovery_threshold_multiplier must be at least 1.0")
        if retry_subscribe_spacing_s < 0:
            raise ValueError("retry_subscribe_spacing_s must be non-negative")
        self.ack_timeout_s = ack_timeout_s
        self.retry_interval_s = retry_interval_s
        self.max_retries = max_retries
        self.data_stale_threshold_s = data_stale_threshold_s
        self.slow_retry_base_s = slow_retry_base_s
        self.slow_retry_multiplier = slow_retry_multiplier
        self.slow_retry_cap_s = slow_retry_cap_s
        self.slow_retry_jitter = slow_retry_jitter
        self.dark_recovery_threshold_multiplier = dark_recovery_threshold_multiplier
        self.retry_subscribe_spacing_s = retry_subscribe_spacing_s
        self.dark_recovery_channels = dark_recovery_channels
        self._max_backoff_exponent = (
            max(0, ceil(log(slow_retry_cap_s / slow_retry_base_s) / log(slow_retry_multiplier)))
            if slow_retry_multiplier > 1.0
            else 0
        )
        self._entries: dict[tuple[str, str], _SymbolEntry] = {}

    def mark_pending(
        self,
        channel: str,
        symbol: str,
        *,
        preserve_retry_count: bool = False,
    ) -> None:
        """Create or reset a subscription entry to pending.

        Args:
            channel: Tracker channel key.
            symbol: Wire-format symbol or product id.
            preserve_retry_count: Keep the existing fast retry budget
                usage and slow-retry escalation count when replaying
                subscriptions after reconnect, so a struggling symbol
                neither re-enters the fast subscribe storm nor restarts
                its backoff schedule and noise from scratch. The fresh
                entry always clears ``next_attempt_at`` so reconnect
                grants one immediate attempt before backoff resumes. The
                dark-recovery escalation count is preserved on the same
                terms so a chronically dark subscription does not reset
                its dark backoff on every reconnect.

        Returns:
            None.

        Raises:
            ValueError: If ``channel`` or ``symbol`` is empty.
        """
        key = self._validate_key(channel, symbol)
        existing = self._entries.get(key)
        if existing is not None and preserve_retry_count:
            retry_count = existing.retry_count
            slow_retry_count = existing.slow_retry_count
            dark_recovery_count = existing.dark_recovery_count
        else:
            retry_count = 0
            slow_retry_count = 0
            dark_recovery_count = 0
        self._entries[key] = _SymbolEntry(
            channel=channel,
            symbol=symbol,
            status="pending",
            requested_at=time.monotonic(),
            retry_count=retry_count,
            slow_retry_count=slow_retry_count,
            dark_recovery_count=dark_recovery_count,
        )

    def mark_confirmed(
        self,
        channel: str,
        symbol: str,
        *,
        dark_recovery_enabled: bool = True,
    ) -> None:
        """Mark a subscription as confirmed by ACK.

        Confirmation clears the FAILED-path retry history (``retry_count``,
        ``slow_retry_count`` and any scheduled ``next_attempt_at``) so a
        subscription that struggled, confirmed, then later fails again
        starts from a clean fast budget and re-emits the first-failure
        ERROR. It deliberately does NOT clear ``dark_recovery_count``: an
        ACK confirms the subscription exists but does not prove data is
        flowing, so a still-dark channel keeps escalating its dark backoff
        until real data arrives (:meth:`mark_data_seen`).

        Args:
            channel: Tracker channel key.
            symbol: Wire-format symbol or product id.
            dark_recovery_enabled: False for wildcard-seeded entries (the
                Spot wildcard ticker universe) that have no per-symbol
                subscription to re-issue, so dark auto-recovery skips them.

        Returns:
            None.

        Raises:
            ValueError: If ``channel`` or ``symbol`` is empty.
        """
        key = self._validate_key(channel, symbol)
        now = time.monotonic()
        entry = self._entries.get(key)
        if entry is None:
            self._entries[key] = _SymbolEntry(
                channel=channel,
                symbol=symbol,
                status="confirmed",
                requested_at=now,
                confirmed_at=now,
                dark_recovery_enabled=dark_recovery_enabled,
            )
            return
        entry.status = "confirmed"
        entry.confirmed_at = now
        entry.last_error = None
        entry.stale_logged = False
        entry.retry_count = 0
        entry.slow_retry_count = 0
        entry.next_attempt_at = None
        entry.dark_recovery_enabled = dark_recovery_enabled

    def mark_failed(self, channel: str, symbol: str, error: str) -> None:
        """Mark a subscription as terminally failed by explicit rejection.

        Unlike a missed-ACK budget exhaustion (which schedules a slow
        retry), an explicit exchange rejection is terminal: any
        previously scheduled ``next_attempt_at`` is cleared so the entry
        is NOT surfaced by :meth:`list_due_failed`. A genuinely invalid
        subscription that first timed out (and was scheduled for slow
        retry) and then received a negative ACK therefore stops being
        retried.

        Args:
            channel: Tracker channel key.
            symbol: Wire-format symbol or product id.
            error: Failure reason from the exchange or retry loop.

        Returns:
            None.

        Raises:
            ValueError: If ``channel`` or ``symbol`` is empty.
        """
        key = self._validate_key(channel, symbol)
        now = time.monotonic()
        entry = self._entries.get(key)
        if entry is None:
            self._entries[key] = _SymbolEntry(
                channel=channel,
                symbol=symbol,
                status="failed",
                requested_at=now,
                last_error=error,
            )
            return
        entry.status = "failed"
        entry.last_error = error
        entry.next_attempt_at = None

    def mark_data_seen(self, channel: str, symbol: str) -> None:
        """Record incoming market data for a subscription.

        Data arriving for a non-confirmed entry is full recovery and
        clears the retry history (``retry_count``, ``slow_retry_count``
        and any scheduled ``next_attempt_at``) for the same reason as
        :meth:`mark_confirmed`: a future failure of a recovered
        subscription must start from a clean fast budget and re-alert.
        The transition reset is confined to the recovery transition, so
        the steady-state data hot path (already-confirmed entries) only
        refreshes the freshness timestamp. Real data is also the only
        signal that clears ``dark_recovery_count`` (a dark channel that
        truly resumed), and that clear is gated on a non-zero count so the
        common data frame stays a single timestamp write.

        Args:
            channel: Tracker channel key.
            symbol: Wire-format symbol or product id.

        Returns:
            None.

        Raises:
            ValueError: If ``channel`` or ``symbol`` is empty.
        """
        key = self._validate_key(channel, symbol)
        now = time.monotonic()
        entry = self._entries.get(key)
        if entry is None:
            self._entries[key] = _SymbolEntry(
                channel=channel,
                symbol=symbol,
                status="confirmed",
                requested_at=now,
                confirmed_at=now,
                last_seen_data_at=now,
            )
            return
        entry.last_seen_data_at = now
        entry.stale_logged = False
        if entry.dark_recovery_count:
            entry.dark_recovery_count = 0
        if entry.status != "confirmed":
            entry.status = "confirmed"
            entry.confirmed_at = now
            entry.last_error = None
            entry.retry_count = 0
            entry.slow_retry_count = 0
            entry.next_attempt_at = None

    def mark_retry_attempt(
        self,
        channel: str,
        symbol: str,
        *,
        expected: _SymbolEntry | None = None,
    ) -> bool:
        """Consume one retry attempt for a pending subscription.

        Args:
            channel: Tracker channel key.
            symbol: Wire-format symbol or product id.
            expected: When given, an optimistic-concurrency guard: the
                attempt is only applied if this object is still the
                current entry for the key. The health loop passes the
                object it listed via :meth:`list_overdue_pending` so that a
                concurrent reconnect replay (which calls :meth:`mark_pending`
                and REPLACES the entry object with a fresh pending one
                between listing and this call) does not get a stale-listed
                attempt: the fresh replacement is left untouched (no
                premature retry before its own ACK window, no exhaustion
                applied to the wrong object) and is handled on a later tick.

        Returns:
            True when the caller should issue a retry subscribe. False when
            the current entry is missing, not pending, not the ``expected``
            object, or has exhausted its fast retry budget. On budget
            exhaustion the entry transitions to ``failed`` but is NOT
            terminal: its ``next_attempt_at`` is scheduled so
            :meth:`list_due_failed` will surface it for a slow retry once
            the backoff delay elapses.

        Raises:
            ValueError: If ``channel`` or ``symbol`` is empty.
        """
        key = self._validate_key(channel, symbol)
        entry = self._entries.get(key)
        if entry is None or entry.status != "pending":
            return False
        if expected is not None and entry is not expected:
            return False
        if entry.retry_count >= self.max_retries:
            entry.retry_count = self.max_retries
            entry.status = "failed"
            entry.last_error = "retry budget exhausted"
            entry.next_attempt_at = time.monotonic() + self._slow_backoff_delay(entry)
            return False
        entry.retry_count += 1
        entry.requested_at = time.monotonic()
        return True

    def mark_slow_retry(self, channel: str, symbol: str) -> bool:
        """Reissue a backed-off failed subscription for one ACK window.

        Transitions a ``failed`` entry that is due (per
        :meth:`list_due_failed`) back to ``pending`` so the caller can
        re-send a single subscribe. The fast ``retry_count`` is left at
        its exhausted value, so if this attempt also misses its ACK the
        next :meth:`mark_retry_attempt` re-fails it immediately and
        schedules the next, longer backoff: exactly one subscribe per
        slow cycle. ``slow_retry_count`` is incremented to grow the next
        backoff and to keep subsequent re-failures logging below ERROR.

        This method does NOT consult the backoff clock; backoff TIMING is
        owned by :meth:`list_due_failed`. Callers must select entries via
        :meth:`list_due_failed` and only pass due ones here; this method
        independently enforces ELIGIBILITY (the entry is still failed and
        still slow-retry-scheduled) so a state change between selection
        and this call is rejected.

        Args:
            channel: Tracker channel key.
            symbol: Wire-format symbol or product id.

        Returns:
            True when a slow-retry-eligible failed entry was transitioned
            to pending and the caller should issue a subscribe. False when
            the entry is missing, not failed, or terminal (no scheduled
            ``next_attempt_at``). The terminal guard keeps explicit
            rejections un-retried and lets the caller skip an entry whose
            state changed (confirmed by late data, or terminally rejected)
            between :meth:`list_due_failed` and this call.

        Raises:
            ValueError: If ``channel`` or ``symbol`` is empty.
        """
        key = self._validate_key(channel, symbol)
        entry = self._entries.get(key)
        if entry is None or entry.status != "failed" or entry.next_attempt_at is None:
            return False
        entry.status = "pending"
        entry.requested_at = time.monotonic()
        entry.next_attempt_at = None
        entry.slow_retry_count += 1
        return True

    def mark_dark_recovery(
        self,
        channel: str,
        symbol: str,
        *,
        expected: _SymbolEntry | None = None,
    ) -> bool:
        """Re-arm a confirmed-but-dark subscription for a recovery re-subscribe.

        Transitions a confirmed entry that has gone silent (per
        :meth:`list_due_dark_recovery`) back to ``pending`` so the caller
        can re-send its per-symbol subscribe to nudge the exchange into
        resuming the stream. The fast ``retry_count`` is reset so the
        re-subscribe gets a full ACK budget; ``dark_recovery_count`` is
        incremented to grow the next dark backoff. If the re-subscribe
        still yields no data the entry darkens again and the next recovery
        waits longer; real data clears the count via :meth:`mark_data_seen`.

        This method enforces ELIGIBILITY (still confirmed, still
        dark-recovery-enabled, still the ``expected`` object) AND
        re-checks the entry is STILL dark. The dark re-check is essential
        and differs from the failed-path guard: a dark entry recovers by
        data arriving IN PLACE (``mark_data_seen`` mutates the same object
        without changing ``status``), so a sibling that recovered between
        :meth:`list_due_dark_recovery` and this call would otherwise still
        pass the identity + status guards and be needlessly re-subscribed.
        Re-evaluating :meth:`_dark_recovery_due` against the live entry
        rejects it.

        Args:
            channel: Tracker channel key.
            symbol: Wire-format symbol or product id.
            expected: When given, the object the caller listed; the
                transition is skipped unless it is still the current entry.

        Returns:
            True when a still-dark confirmed entry was transitioned to
            pending and the caller should issue a subscribe. False when the
            entry is missing, not confirmed, not dark-recovery-eligible
            (disabled flag or non-recoverable channel), no longer the
            ``expected`` object, or no longer dark (recovered in place
            since listing).

        Raises:
            ValueError: If ``channel`` or ``symbol`` is empty.
        """
        key = self._validate_key(channel, symbol)
        entry = self._entries.get(key)
        if entry is None or entry.status != "confirmed" or not self._dark_recovery_eligible(entry):
            return False
        if expected is not None and entry is not expected:
            return False
        now = time.monotonic()
        if not self._dark_recovery_due(entry, now):
            return False
        entry.status = "pending"
        entry.requested_at = now
        entry.retry_count = 0
        entry.next_attempt_at = None
        entry.dark_recovery_count += 1
        return True

    def list_overdue_pending(self, now: float | None = None) -> list[_SymbolEntry]:
        """List pending entries whose ACK timer has expired.

        Args:
            now: Optional monotonic timestamp for deterministic tests.

        Returns:
            Pending entries whose ``requested_at`` age is at least
            ``ack_timeout_s``.

        Raises:
            None.
        """
        current = time.monotonic() if now is None else now
        return [
            entry
            for entry in self._entries.values()
            if entry.status == "pending" and current - entry.requested_at >= self.ack_timeout_s
        ]

    def list_due_failed(self, now: float | None = None) -> list[_SymbolEntry]:
        """List failed entries whose slow-retry backoff delay has elapsed.

        Only entries scheduled for slow retry are returned. A failed
        entry carries ``next_attempt_at`` exactly when it reached
        ``failed`` by exhausting its fast ACK retry budget (a missed
        ACK). Entries failed by an explicit exchange rejection via
        ``mark_failed`` have ``next_attempt_at`` of None and are
        intentionally excluded, so a genuinely invalid subscription is
        never retried forever.

        Args:
            now: Optional monotonic timestamp for deterministic tests.

        Returns:
            Failed entries with a scheduled ``next_attempt_at`` at or
            before ``now``.

        Raises:
            None.
        """
        current = time.monotonic() if now is None else now
        return [
            entry
            for entry in self._entries.values()
            if entry.status == "failed"
            and entry.next_attempt_at is not None
            and current >= entry.next_attempt_at
        ]

    def list_stale_data(self, now: float | None = None) -> list[_SymbolEntry]:
        """List confirmed entries with no recent market data, log-once.

        Entries are returned ONLY the first time they cross the stale
        threshold. Each returned entry has its ``stale_logged`` flag set
        so subsequent calls skip it until either fresh data arrives
        (``mark_data_seen`` resets the flag) or the subscription is
        re-armed (``mark_pending``/``mark_confirmed`` reset the flag).
        Prevents the background loop from re-emitting the same warning
        every ``retry_interval_s`` for hours during legitimate upstream
        silence (e.g. CME weekend close, low-liquidity exotic pairs).

        Staleness is measured from :meth:`_SymbolEntry.stale_reference`
        (the most recent of the entry's request, confirmation and
        last-data timestamps), so a re-confirmation after a reconnect
        grants a fresh window while ``last_seen_data_at`` is preserved for
        diagnostics.

        Args:
            now: Optional monotonic timestamp for deterministic tests.

        Returns:
            Confirmed entries whose stale reference exceeds the stale
            threshold AND have not yet been logged as stale during this
            window.

        Raises:
            None.
        """
        current = time.monotonic() if now is None else now
        stale: list[_SymbolEntry] = []
        for entry in self._entries.values():
            if entry.status != "confirmed" or entry.stale_logged:
                continue
            reference = entry.stale_reference()
            if current - reference >= self.data_stale_threshold_s:
                entry.stale_logged = True
                stale.append(entry)
        return stale

    def list_failed(self) -> list[_SymbolEntry]:
        """List permanently failed entries.

        Args:
            None.

        Returns:
            Entries currently in failed state.

        Raises:
            None.
        """
        return [entry for entry in self._entries.values() if entry.status == "failed"]

    def list_due_dark_recovery(self, now: float | None = None) -> list[_SymbolEntry]:
        """List confirmed subscriptions dark long enough to auto-recover.

        A confirmed, dark-recovery-enabled entry is due once it has had no
        data for at least ``data_stale_threshold_s *
        dark_recovery_threshold_multiplier`` PLUS its current dark backoff.
        The multiplier keeps recovery well above the stale-logging
        threshold so legitimately quiet symbols are surfaced as
        diagnostics long before they are ever re-subscribed, and the
        per-entry backoff paces a channel that keeps re-darkening. Only
        eligible entries qualify (see :meth:`_dark_recovery_eligible`):
        wildcard-seeded entries have no per-symbol subscription to
        re-issue, and event-driven channels (``trade``, ``ohlc:*``) are
        excluded because for them silence is normal, not a broken stream.

        Args:
            now: Optional monotonic timestamp for deterministic tests.

        Returns:
            Confirmed dark-recovery-enabled entries whose silent duration
            exceeds the recovery threshold plus their dark backoff.

        Raises:
            None.
        """
        current = time.monotonic() if now is None else now
        return [
            entry
            for entry in self._entries.values()
            if entry.status == "confirmed"
            and self._dark_recovery_eligible(entry)
            and self._dark_recovery_due(entry, current)
        ]

    def snapshot(self) -> dict[tuple[str, str], _SymbolEntry]:
        """Return a point-in-time copy of tracked entries.

        Args:
            None.

        Returns:
            Mapping from ``(channel, symbol)`` to copied entry objects.

        Raises:
            None.
        """
        return {key: replace(entry) for key, entry in self._entries.items()}

    def _geometric_backoff(self, count: int, jitter_key: str) -> float:
        """Return a capped, deterministically-jittered geometric backoff.

        Shared by the slow-retry (failed-subscription) and dark-recovery
        (confirmed-but-silent) schedules. The base delay grows
        geometrically with ``count`` and is hard-capped at
        ``slow_retry_cap_s`` so the schedule settles to a steady cadence
        (for example 1m, 5m, 25m, then hourly). The exponent is bounded so
        the geometric term cannot overflow before the cap applies. A
        deterministic per-``jitter_key`` jitter spreads a block of
        subscriptions that escalate together (a boot subscribe storm, or a
        whole exchange going dark) so their re-subscribes do not form a
        synchronised herd; determinism keeps the delay reproducible in
        tests. The jittered result is clamped to the cap so it is a hard
        upper bound (the early, smaller tiers already provide the spread).

        Args:
            count: Escalation count (number of prior attempts in this
                schedule).
            jitter_key: Stable per-identity key the jitter is derived
                from, so distinct identities desynchronise.

        Returns:
            Delay in seconds, never exceeding ``slow_retry_cap_s``.

        Raises:
            None.
        """
        exponent = min(count, self._max_backoff_exponent)
        interval = min(
            self.slow_retry_base_s * self.slow_retry_multiplier**exponent,
            self.slow_retry_cap_s,
        )
        if self.slow_retry_jitter <= 0.0:
            return interval
        fraction = zlib.crc32(jitter_key.encode()) / 0xFFFFFFFF
        jittered = interval * (1.0 + self.slow_retry_jitter * (2.0 * fraction - 1.0))
        return min(jittered, self.slow_retry_cap_s)

    def _slow_backoff_delay(self, entry: _SymbolEntry) -> float:
        """Return the next slow-retry delay for a failed entry.

        Args:
            entry: Failed entry whose next backoff delay is computed.

        Returns:
            Delay in seconds until the entry's next slow-retry attempt.

        Raises:
            None.
        """
        return self._geometric_backoff(
            entry.slow_retry_count,
            f"{entry.channel}|{entry.symbol}|{entry.slow_retry_count}",
        )

    def _dark_backoff_delay(self, entry: _SymbolEntry) -> float:
        """Return the extra backoff before a confirmed-dark re-subscribe.

        Added on top of the dark-recovery threshold so a channel that
        keeps going dark immediately after each recovery re-subscribe is
        paced by an escalating, capped backoff rather than re-subscribed
        every loop tick. The first recovery (count 0) adds no extra delay
        so it fires at exactly the threshold; each subsequent re-darkening
        escalates geometrically.

        Args:
            entry: Confirmed dark entry whose recovery backoff is computed.

        Returns:
            Delay in seconds to add to the dark-recovery threshold; zero
            for the first recovery attempt.

        Raises:
            None.
        """
        if entry.dark_recovery_count < 1:
            return 0.0
        return self._geometric_backoff(
            entry.dark_recovery_count - 1,
            f"dark|{entry.channel}|{entry.symbol}|{entry.dark_recovery_count}",
        )

    def _dark_recovery_eligible(self, entry: _SymbolEntry) -> bool:
        """Return True when an entry's channel and flags allow dark recovery.

        Two gates, both independent of timing: the per-entry
        ``dark_recovery_enabled`` flag (False for wildcard-seeded entries
        with no per-symbol subscription to re-issue) and channel type. Only
        continuous channels in :attr:`dark_recovery_channels` qualify; for
        event-driven channels (``trade``, ``ohlc:*``) silence is the normal
        sparse state, so re-subscribing them would churn quiet symbols
        without recovering anything.

        Args:
            entry: Confirmed entry to evaluate.

        Returns:
            True when the entry may be dark-recovered subject to timing.

        Raises:
            None.
        """
        return entry.dark_recovery_enabled and entry.channel in self.dark_recovery_channels

    def _dark_recovery_due(self, entry: _SymbolEntry, now: float) -> bool:
        """Return True when a confirmed entry is dark long enough to recover.

        Dark duration is measured from :meth:`_SymbolEntry.stale_reference`
        and must exceed ``data_stale_threshold_s *
        dark_recovery_threshold_multiplier`` plus the entry's current dark
        backoff. Shared by :meth:`list_due_dark_recovery` (selection) and
        :meth:`mark_dark_recovery` (the claim re-check), so an entry that
        recovered in place between selection and the claim is not
        re-subscribed.

        Args:
            entry: Confirmed entry to evaluate.
            now: Current monotonic timestamp.

        Returns:
            True when the entry has been dark past the recovery threshold
            plus its dark backoff.

        Raises:
            None.
        """
        threshold = self.data_stale_threshold_s * self.dark_recovery_threshold_multiplier
        return now - entry.stale_reference() >= threshold + self._dark_backoff_delay(entry)

    @staticmethod
    def _validate_key(channel: str, symbol: str) -> tuple[str, str]:
        """Validate and return the subscription identity key."""
        if not channel:
            raise ValueError("channel must be non-empty")
        if not symbol:
            raise ValueError("symbol must be non-empty")
        return (channel, symbol)


def interval_to_label(interval: int) -> str:
    """Return the canonical tracker label for a Kraken OHLC interval.

    Args:
        interval: Kraken OHLC interval in minutes.

    Returns:
        Canonical label such as ``"1m"``, ``"1h"``, or ``"1d"``.

    Raises:
        ValueError: If ``interval`` is not supported.
    """
    try:
        return _INTERVAL_TO_LABEL[interval]
    except KeyError as exc:
        raise ValueError(f"Unsupported Kraken OHLC interval: {interval}") from exc


def label_to_interval(label: str) -> int:
    """Return the Kraken OHLC interval for a canonical tracker label.

    Args:
        label: Canonical label such as ``"1m"``, ``"1h"``, or ``"1d"``.

    Returns:
        Kraken OHLC interval in minutes.

    Raises:
        ValueError: If ``label`` is not supported.
    """
    try:
        return _LABEL_TO_INTERVAL[label]
    except KeyError as exc:
        raise ValueError(f"Unsupported Kraken OHLC interval label: {label}") from exc
