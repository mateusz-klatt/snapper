"""Authentication principal for internal auth/authorization flow.

AuthPrincipal is NOT a transport payload — it is an internal identity
object extracted from JWT claims. It does NOT inherit StrictDataSchema
because it has no provenance (no session_id, sequence_id, public_id).

For the transportable user profile returned by API endpoints, see UserProfile.
"""

from snapper.api.schemas.base import StrictBody
from snapper.auth.domain.roles import UserRole


class AuthPrincipal(StrictBody):
    """Internal authentication principal from JWT claims.

    Used by auth dependencies (require_authentication, require_permission)
    and stored on request.state for authorization checks. Never returned
    directly in REST responses — endpoints that need to return user data
    should load the User from DB and project to UserProfile.

    Attributes:
        username: User's login name.
        role: User's role (VIEWER, OPERATOR, ADMIN).
        email: Optional email address.
        is_active: Whether user account is active.
    """

    username: str
    role: UserRole
    email: str | None = None
    is_active: bool = True
