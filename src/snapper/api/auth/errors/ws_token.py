"""WebSocket token error definitions.

This module defines exception classes for WebSocket token-related errors.
"""

__all__ = ["WsTokenError", "WsTokenAlreadyUsedError"]


class WsTokenError(Exception):
    """Base exception for WebSocket token errors.

    Raised when token validation fails due to expiration, invalid signature,
    purpose mismatch, or session mismatch.
    """

    pass


class WsTokenAlreadyUsedError(WsTokenError):
    """Exception raised when a one-time WebSocket token is reused.

    WebSocket tokens are single-use; this exception is raised when
    attempting to use a token that has already been consumed.
    """

    pass
