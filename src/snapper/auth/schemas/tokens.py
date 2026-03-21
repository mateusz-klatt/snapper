"""Token schemas module.

This module defines Pydantic schemas for JWT token claims
and token pair responses.
"""

from pydantic import BaseModel
from pydantic import ConfigDict

from snapper.auth.domain.roles import UserRole


class TokenClaims(BaseModel):
    """JWT token claims schema.

    Contains all claims embedded in access and refresh tokens.
    Does not inherit StrictDataSchema because JWT claims are external
    tokens parsed from JWTs, not internal bus messages.

    Attributes:
        sub: Subject (user ID).
        username: User's username.
        role: User's role.
        permissions: List of permission strings (empty for refresh tokens).
        exp: Expiration timestamp (Unix epoch).
        iat: Issued at timestamp (Unix epoch).
        jti: JWT ID (unique token identifier).
        sid: Session ID for token rotation tracking.
    """

    model_config = ConfigDict(extra="forbid", strict=False)

    sub: str
    username: str
    role: UserRole
    permissions: list[str]
    exp: int
    iat: int
    jti: str
    sid: str


class TokenPair(BaseModel):
    """Internal token pair DTO.

    Carries access and refresh token strings between token manager
    and route handlers. Not a transport payload — never sent directly
    in REST/WS/ZMQ responses. Token strings are extracted and set
    as HTTP cookies by the route handler.

    Attributes:
        access_token: Short-lived JWT for API access.
        refresh_token: Long-lived JWT for token renewal.
        token_type: Token type (always "bearer").
        expires_in: Access token TTL in seconds.
    """

    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int
