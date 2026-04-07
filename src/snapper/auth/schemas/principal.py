"""Authentication principal for internal auth/authorization flow.

AuthPrincipal is NOT a transport payload — it is an internal identity
object extracted from JWT claims. It does NOT inherit StrictDataSchema
because it has no provenance (no session_id, sequence_id, public_id).

For the transportable user profile returned by API endpoints, see UserProfile.
"""

from pydantic import Field

from snapper.api.schemas.base import StrictBody
from snapper.auth.domain.roles import UserRole


class AuthPrincipal(StrictBody):
    """Internal authentication principal from JWT claims.

    Used by auth dependencies (require_authentication, require_permission)
    and stored on request.state for authorization checks. Never returned
    directly in REST responses — endpoints that need to return user data
    should load the User from DB and project to UserProfile.

    Multi-tenant fields (Plan 0 Phase 0b Section 4.1):

    - ``user_public_id`` carries the stable UUID7 of the user row so
      authorization decisions are not coupled to mutable usernames. The
      default empty string keeps construction sites backwards compatible
      until the login + refresh flows populate it from the DB lookup.
    - ``operator_public_ids`` is the set of operators a user may act AS,
      derived from ``user_operator_memberships`` at token issue time.
      ADMIN gets every active operator; OPERATOR gets only their explicit
      memberships; VIEWER stays empty until read-scope grants ship in a
      later phase.
    - ``primary_operator_public_id`` is the membership row marked
      ``is_primary=TRUE`` and is the default scope when a user logs in.
      Empty string when the user has no primary membership yet.
    - ``active_wallet_public_id`` is the last-selected wallet (UI state)
      passed back through token claims so a refresh preserves the user's
      working context. ``None`` when the user has not yet picked a wallet.

    The wallet-level scope set is intentionally NOT stored on the
    principal — it is derived on demand from
    ``wallet_operator_scope_grants`` so a grant change between login and
    action is honored without forcing the user to re-authenticate.

    Attributes:
        username: User's login name.
        role: User's role (VIEWER, OPERATOR, ADMIN).
        email: Optional email address.
        is_active: Whether user account is active.
        user_public_id: Stable UUID7 of the user row (Phase 0b).
        operator_public_ids: Operators this user may act AS (Phase 0b).
        primary_operator_public_id: Default operator at login (Phase 0b).
        active_wallet_public_id: Last-selected wallet UI state (Phase 0b).
    """

    username: str
    role: UserRole
    email: str | None = None
    is_active: bool = True
    user_public_id: str = ""
    operator_public_ids: list[str] = Field(default_factory=list)
    primary_operator_public_id: str = ""
    active_wallet_public_id: str | None = None
