"""Backend-internal schemas for administrative bus events."""

from datetime import datetime
from typing import Literal

from snapper.api.schemas.base import StrictDataSchema


class MembershipRevokedData(StrictDataSchema[Literal["membership_revoked"]]):
    """Desk-membership revocation fanout kept outside generated client schemas.

    Attributes:
        membership_public_id: Identity of the SCD2 membership row that closed.
        user_public_id: User whose desk authority was removed.
        username: Username captured for the administrative audit trail.
        operator_public_id: Desk operator removed from the user.
        detached_at: UTC timestamp at which the membership became inactive.
        revoked_by_user_public_id: Admin actor, when the operation was authenticated.
        promoted_operator_public_id: Replacement primary desk, when one was promoted.
        reason: Optional administrative rationale.
    """

    type: Literal["membership_revoked"] = "membership_revoked"
    membership_public_id: str
    user_public_id: str
    username: str
    operator_public_id: str
    detached_at: datetime
    revoked_by_user_public_id: str | None
    promoted_operator_public_id: str | None
    reason: str | None = None
