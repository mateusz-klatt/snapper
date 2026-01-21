"""WebSocket token store for replay prevention.

This module provides in-memory storage for tracking used WebSocket tokens
to prevent replay attacks. Expired tokens are automatically cleaned up.
"""

import threading

__all__ = ["WsTokenStore"]


class WsTokenStore:
    """Thread-safe in-memory store for tracking used WebSocket tokens.

    Maintains a dictionary of token JTIs mapped to their expiration timestamps.
    Expired entries are automatically cleaned up during lookup operations.
    """

    def __init__(self) -> None:
        """Initialize the token store with empty state."""
        self._used_tokens: dict[str, int] = {}
        self._lock = threading.Lock()

    def _cleanup(self, now_ts: int) -> None:
        """Remove expired token entries.

        Args:
            now_ts: Current Unix timestamp.
        """
        expired = [jti for jti, exp in self._used_tokens.items() if exp <= now_ts]
        for jti in expired:
            del self._used_tokens[jti]

    def is_used(self, jti: str, now_ts: int) -> bool:
        """Check if a token has already been used.

        Performs cleanup of expired tokens before checking.

        Args:
            jti: Token unique identifier.
            now_ts: Current Unix timestamp.

        Returns:
            True if the token has been used, False otherwise.
        """
        with self._lock:
            self._cleanup(now_ts)
            return jti in self._used_tokens

    def mark_used(self, jti: str, exp: int) -> None:
        """Mark a token as used.

        Args:
            jti: Token unique identifier.
            exp: Token expiration timestamp (for cleanup).
        """
        with self._lock:
            self._used_tokens[jti] = exp
