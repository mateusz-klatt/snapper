"""Token schemas module.

This module defines Pydantic schemas for JWT token claims
and token pair responses.
"""

from typing import Literal

from pydantic import BaseModel
from pydantic import ConfigDict

from snapper.api.schemas.base import StrictDataSchema
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


class TokenPair(StrictDataSchema):
    """Token pair response schema.

    Returned after successful authentication containing both
    access and refresh tokens.

    Attributes:
        type: Payload item type discriminator.
        access_token: Short-lived JWT for API access.
        refresh_token: Long-lived JWT for token renewal.
        token_type: Token type (always "bearer").
        expires_in: Access token TTL in seconds.
    """

    type: Literal["token_pair"] = "token_pair"
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int
