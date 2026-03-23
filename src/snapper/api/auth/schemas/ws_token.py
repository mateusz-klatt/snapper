"""WebSocket token schemas.

This module defines schemas for WebSocket authentication tokens used
for secure WebSocket connection establishment.
"""

from dataclasses import dataclass
from datetime import datetime

from snapper.api.schemas.base import PartialBody

__all__ = ["WsTokenPayload", "WsTokenResult"]


class WsTokenPayload(PartialBody):
    """WebSocket token JWT payload schema.

    Contains claims for a single-use WebSocket connection token.
    Does not inherit StrictDataSchema because JWT payloads are
    internal token DTOs, not canonical Snapper events.

    Attributes:
        purpose: Token purpose (must be 'ws_connect').
        sub: Subject (user ID).
        sid_hash: SHA-256 hash of the session ID.
        iat: Issued at timestamp (Unix epoch).
        exp: Expiration timestamp (Unix epoch).
        jti: Unique token identifier for replay prevention.
    """

    purpose: str
    sub: str
    sid_hash: str
    iat: int
    exp: int
    jti: str


@dataclass(slots=True)
class WsTokenResult:
    """WebSocket token generation result.

    Returned by WsTokenService.generate() with the token and metadata.

    Attributes:
        token: The JWT token string.
        expires_at: Token expiration datetime.
        payload: The token payload for reference.
    """

    token: str
    expires_at: datetime
    payload: WsTokenPayload
