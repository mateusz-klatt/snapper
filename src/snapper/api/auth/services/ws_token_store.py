"""WebSocket token store for replay prevention.

This module provides in-memory storage for tracking used WebSocket tokens
to prevent replay attacks. Expired tokens are incrementally cleaned up.
"""

import heapq
import threading

__all__ = ["WsTokenStore"]

_CLEANUP_BATCH_SIZE = 64


class WsTokenStore:
    """Thread-safe in-memory store for tracking used WebSocket tokens.

    Maintains a dictionary of token JTIs mapped to expiration timestamps plus
    a min-heap ordered by expiration. Lookup cleanup is bounded so handshake
    work stays proportional to heap operations instead of scanning all tokens.
    """

    def __init__(self) -> None:
        """Initialize the token store with empty state."""
        self._used_tokens: dict[str, int] = {}
        self._expiry_heap: list[tuple[int, str]] = []
        self._lock = threading.Lock()

    def _cleanup(self, now_ts: int) -> None:
        """Remove a bounded batch of expired token entries.

        Args:
            now_ts: Current Unix timestamp.
        """
        popped = 0
        while (
            self._expiry_heap and self._expiry_heap[0][0] <= now_ts and popped < _CLEANUP_BATCH_SIZE
        ):
            exp, jti = heapq.heappop(self._expiry_heap)
            popped += 1
            if self._used_tokens.get(jti) == exp:
                del self._used_tokens[jti]

    def _is_used_unlocked(self, jti: str, now_ts: int) -> bool:
        """Check token usage and prune expired entries while holding ``self._lock``.

        Args:
            jti: Token unique identifier.
            now_ts: Current Unix timestamp.

        Returns:
            True if the token is recorded and unexpired, False otherwise.
        """
        self._cleanup(now_ts)
        exp = self._used_tokens.get(jti)
        if exp is None:
            return False
        if exp <= now_ts:
            del self._used_tokens[jti]
            return False
        return True

    def _mark_used_unlocked(self, jti: str, exp: int) -> None:
        """Record a token as used while holding ``self._lock``.

        Args:
            jti: Token unique identifier.
            exp: Token expiration timestamp (for cleanup).
        """
        self._used_tokens[jti] = exp
        heapq.heappush(self._expiry_heap, (exp, jti))

    def is_used(self, jti: str, now_ts: int) -> bool:
        """Check if a token has already been used.

        Performs bounded cleanup of expired tokens before checking.

        Args:
            jti: Token unique identifier.
            now_ts: Current Unix timestamp.

        Returns:
            True if the token has been used, False otherwise.
        """
        with self._lock:
            return self._is_used_unlocked(jti, now_ts)

    def mark_used(self, jti: str, exp: int) -> None:
        """Mark a token as used.

        Args:
            jti: Token unique identifier.
            exp: Token expiration timestamp (for cleanup).
        """
        with self._lock:
            self._mark_used_unlocked(jti, exp)

    def consume(self, jti: str, exp: int, now_ts: int) -> bool:
        """Atomically check and consume a token under the store lock.

        Acquires ``self._lock`` once, runs bounded cleanup of expired entries,
        and treats an entry with ``exp <= now_ts`` as unused, matching
        :meth:`is_used`. If the token is already used, returns False without
        changing token state; otherwise records the token and returns True.

        Args:
            jti: Token unique identifier.
            exp: Token expiration timestamp (for cleanup).
            now_ts: Current Unix timestamp.

        Returns:
            True if the token was unused and is now recorded as used,
            False if it was already used and unexpired.
        """
        with self._lock:
            if self._is_used_unlocked(jti, now_ts):
                return False
            self._mark_used_unlocked(jti, exp)
            return True
