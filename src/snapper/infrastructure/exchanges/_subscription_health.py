"""Per-symbol subscription health tracker shared by exchange clients.

The tracker records subscription state by ``(channel, symbol)`` using
wire-format symbols because subscribe calls, ACK envelopes, and retry
subscribes all speak the exchange WebSocket format. Account-wide private
subscriptions, such as Kraken Spot executions, are intentionally excluded:
there is no symbol identity to track and a one-symbol retry has no meaning.
"""

import time
from dataclasses import dataclass
from dataclasses import replace
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
    """

    channel: str
    symbol: str
    status: SubscriptionStatus
    requested_at: float
    confirmed_at: float | None = None
    last_error: str | None = None
    retry_count: int = 0
    last_seen_data_at: float | None = None


class SubscriptionHealthTracker:
    """Per-(channel, symbol) subscription state with retry queries.

    Attributes:
        retry_interval_s: How often the background retry task wakes up.
        ack_timeout_s: How long to wait for an ACK before a pending
            symbol is overdue.
        max_retries: Maximum retry attempts before a symbol is failed.
        data_stale_threshold_s: Age after which confirmed subscriptions
            without data are surfaced for logging only.
    """

    def __init__(
        self,
        *,
        ack_timeout_s: float = 15.0,
        retry_interval_s: float = 10.0,
        max_retries: int = 3,
        data_stale_threshold_s: float = 300.0,
    ) -> None:
        """Initialize subscription health tracking.

        Args:
            ack_timeout_s: Seconds to wait for a subscribe ACK before
                listing a pending symbol as overdue.
            retry_interval_s: Seconds between retry-loop wakeups.
            max_retries: Number of retry attempts before permanent
                failure.
            data_stale_threshold_s: Seconds without data before a
                confirmed subscription is listed as stale.

        Returns:
            None.

        Raises:
            ValueError: If any timing value is non-positive or
                ``max_retries`` is negative.
        """
        if ack_timeout_s <= 0:
            raise ValueError("ack_timeout_s must be positive")
        if retry_interval_s <= 0:
            raise ValueError("retry_interval_s must be positive")
        if max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        if data_stale_threshold_s <= 0:
            raise ValueError("data_stale_threshold_s must be positive")
        self.ack_timeout_s = ack_timeout_s
        self.retry_interval_s = retry_interval_s
        self.max_retries = max_retries
        self.data_stale_threshold_s = data_stale_threshold_s
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
            preserve_retry_count: Keep the existing retry budget usage
                when replaying subscriptions after reconnect.

        Returns:
            None.

        Raises:
            ValueError: If ``channel`` or ``symbol`` is empty.
        """
        key = self._validate_key(channel, symbol)
        existing = self._entries.get(key)
        retry_count = existing.retry_count if existing and preserve_retry_count else 0
        self._entries[key] = _SymbolEntry(
            channel=channel,
            symbol=symbol,
            status="pending",
            requested_at=time.monotonic(),
            retry_count=retry_count,
        )

    def mark_confirmed(self, channel: str, symbol: str) -> None:
        """Mark a subscription as confirmed by ACK.

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
            )
            return
        entry.status = "confirmed"
        entry.confirmed_at = now
        entry.last_error = None

    def mark_failed(self, channel: str, symbol: str, error: str) -> None:
        """Mark a subscription as failed.

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

    def mark_data_seen(self, channel: str, symbol: str) -> None:
        """Record incoming market data for a subscription.

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
        if entry.status != "confirmed":
            entry.status = "confirmed"
            entry.confirmed_at = now
            entry.last_error = None

    def mark_retry_attempt(self, channel: str, symbol: str) -> bool:
        """Consume one retry attempt for a pending subscription.

        Args:
            channel: Tracker channel key.
            symbol: Wire-format symbol or product id.

        Returns:
            True when the caller should issue a retry subscribe. False
            when the entry is missing, not pending, or has exhausted its
            retry budget.

        Raises:
            ValueError: If ``channel`` or ``symbol`` is empty.
        """
        key = self._validate_key(channel, symbol)
        entry = self._entries.get(key)
        if entry is None or entry.status != "pending":
            return False
        if entry.retry_count >= self.max_retries:
            entry.retry_count = self.max_retries
            entry.status = "failed"
            entry.last_error = "retry budget exhausted"
            return False
        entry.retry_count += 1
        entry.requested_at = time.monotonic()
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

    def list_stale_data(self, now: float | None = None) -> list[_SymbolEntry]:
        """List confirmed entries with no recent market data.

        Args:
            now: Optional monotonic timestamp for deterministic tests.

        Returns:
            Confirmed entries whose last data timestamp, or confirmation
            timestamp if no data has arrived, exceeds the stale threshold.

        Raises:
            None.
        """
        current = time.monotonic() if now is None else now
        stale: list[_SymbolEntry] = []
        for entry in self._entries.values():
            if entry.status != "confirmed":
                continue
            reference = entry.last_seen_data_at or entry.confirmed_at or entry.requested_at
            if current - reference >= self.data_stale_threshold_s:
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
