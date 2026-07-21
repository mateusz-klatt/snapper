"""Token schemas module.

This module defines Pydantic schemas for JWT token claims
and token pair responses.
"""

from pydantic import Field

from snapper.api.schemas.base import StrictBody
from snapper.auth.domain.roles import UserRole


class TokenClaims(StrictBody):
    """JWT token claims schema.

    Contains all claims embedded in access and refresh tokens.
    Does not inherit StrictDataSchema because JWT claims are external
    tokens parsed from JWTs, not internal bus messages.

    Multi-tenant claims carry the same identity surface as
    ``AuthPrincipal`` so a refresh round-trip preserves the user's
    trading-context selection without re-querying the DB. Optional
    defaults keep older tokens decodable: a stale token simply has
    empty multi-tenant fields and the next token issuance refreshes
    them from the DB.

    Attributes:
        sub: Subject (user ID).
        username: User's username.
        role: User's role.
        permissions: Permission strings granted to the access token, or
            ``None`` when the claim is absent on a legacy token.
        permission_scope_version: Marker distinguishing refresh tokens that
            carry an intentional permission scope from legacy refresh tokens
            whose empty ``permissions`` list did not encode access scope.
        exp: Expiration timestamp (Unix epoch).
        iat: Issued at timestamp (Unix epoch).
        jti: JWT ID (unique token identifier).
        sid: Session ID for token rotation tracking.
        user_public_id: Stable UUID7 of the user row.
        operator_public_ids: Operators this user may act AS.
        primary_operator_public_id: Default operator at login.
        active_wallet_public_id: Last-selected wallet UI state.
    """

    sub: str
    username: str
    role: UserRole
    permissions: list[str] | None = None
    permission_scope_version: int | None = None
    exp: int
    iat: int
    jti: str
    sid: str
    user_public_id: str = ""
    operator_public_ids: list[str] = Field(default=[])
    primary_operator_public_id: str = ""
    active_wallet_public_id: str | None = None


class TokenPair(StrictBody):
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
