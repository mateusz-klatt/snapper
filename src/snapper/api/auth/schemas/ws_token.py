"""WebSocket token schemas.

This module defines schemas for WebSocket authentication tokens used
for secure WebSocket connection establishment.
"""

from dataclasses import dataclass
from datetime import datetime

from pydantic import ConfigDict

from snapper.api.schemas.base import StrictApiSchema

__all__ = ["WsTokenPayload", "WsTokenResult"]


class WsTokenPayload(StrictApiSchema):
    """WebSocket token JWT payload schema.

    Contains claims for a single-use WebSocket connection token.

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
    model_config = ConfigDict(
        extra="ignore",
        strict=True,
        validate_default=True,
        populate_by_name=True,
    )


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
