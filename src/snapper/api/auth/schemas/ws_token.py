"""WebSocket token schemas.

This module defines schemas for WebSocket authentication tokens used
for secure WebSocket connection establishment.
"""

from dataclasses import dataclass
from datetime import datetime

from snapper.api.schemas.base import PartialBody

__all__ = [
    "WS_AUTHORIZATION_CONTEXT_VERSION",
    "WsAuthorizationContext",
    "WsTokenPayload",
    "WsTokenResult",
]

WS_AUTHORIZATION_CONTEXT_VERSION = 1


@dataclass(frozen=True, slots=True)
class WsAuthorizationContext:
    """The minimal authorization state a ws_token must carry.

    These three values exist ONLY in token claims — the database cannot
    supply them. ``UserService.build_auth_principal`` deliberately refuses to
    populate ``active_wallet_public_id`` because it is client-selected UI
    state, and it omits the permission fields entirely; both then default to
    ``None``, which means the FULL current-role grant. Rebuilding a principal
    from the database alone would therefore silently WIDEN a deliberately
    narrowed token, which is why this context travels with the ticket.

    Everything else a principal needs — identity, role, operator memberships,
    delegate identity — is re-derived from the database when the ticket is
    consumed and must NOT be carried here. The context is transport evidence,
    not authorization truth: the fresh database role ceilings the carried
    permissions and the carried wallet is revalidated against the read plane.

    Passed as one object rather than as separate arguments because
    ``WsTokenService.generate`` would otherwise breach the repository's
    argument-count policy.

    Attributes:
        active_wallet_public_id: Client-selected wallet, or None.
        permissions: Token-scoped permission strings. An empty list is a
            deliberate zero scope and must never be conflated with None,
            which means the full role grant.
        permission_scope_version: Version governing scope compatibility.
    """

    active_wallet_public_id: str | None
    permissions: list[str] | None
    permission_scope_version: int | None


class WsTokenPayload(PartialBody):
    """WebSocket token JWT payload schema.

    Contains claims for a single-use WebSocket connection token.
    Does not inherit StrictDataSchema because JWT payloads are
    internal token DTOs, not canonical Snapper events.

    ``authorization_context_version`` is the SOLE legacy discriminator, and the
    test is **the VALUE being None, not the key being absent**. Both forms mean
    legacy and must be treated identically:

    - the key missing entirely — a ticket minted before this field existed;
    - the key present with a JSON ``null`` — what ``model_dump`` emits today
      for a context-less mint, because it does not exclude None.

    Stating this as "presence means contextful" would be a trap: every
    context-less ticket this service mints DOES carry the key, so a consumer
    keying on presence would read ``permissions: null`` as authoritative — and
    authoritative None means the FULL role grant, which is the widening this
    whole context exists to prevent.

    When the version IS a recognised number, the context fields are
    authoritative even when they are None. Consumers must fail closed on a
    version they do not recognise rather than treating it as legacy — a
    forward-compatible reader that silently degrades an unknown version to
    "no context" would restore the full role grant. That gate belongs to the
    reader, which does not exist yet; nothing here enforces it.

    Attributes:
        purpose: Token purpose (must be 'ws_connect').
        sub: Subject (user ID).
        sid_hash: SHA-256 hash of the session ID.
        iat: Issued at timestamp (Unix epoch).
        exp: Expiration timestamp (Unix epoch).
        jti: Unique token identifier for replay prevention.
        authorization_context_version: Context shape marker, or None for a
            legacy ticket minted before the context existed.
        active_wallet_public_id: Carried wallet selection.
        permissions: Carried token permission scope; [] is a real zero scope.
        permission_scope_version: Carried scope compatibility version.
    """

    purpose: str
    sub: str
    sid_hash: str
    iat: int
    exp: int
    jti: str
    authorization_context_version: int | None = None
    active_wallet_public_id: str | None = None
    permissions: list[str] | None = None
    permission_scope_version: int | None = None


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
