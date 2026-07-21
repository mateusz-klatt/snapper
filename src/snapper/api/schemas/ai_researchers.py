"""AI researcher provisioning schemas.

The researcher surface mints automation principals that can ingest
hostile research material without sharing the delegate role's trading
authority. The access token is returned once at creation time and is
not persisted in recoverable form.
"""

from datetime import datetime
from typing import Literal

from pydantic import Field

from snapper.api.schemas.base import PayloadRequest
from snapper.api.schemas.base import PayloadResponse
from snapper.api.schemas.base import StrictBody
from snapper.auth.domain.permissions import Permission


class ResearcherCreateBody(StrictBody):
    """Operator-supplied researcher provisioning fields.

    Attributes:
        label: Human-readable identifier incorporated into the generated
            researcher username.
        permissions: Optional narrower token grant. Every requested
            permission must remain within the researcher role ceiling.
    """

    label: str = Field(..., min_length=1, max_length=44, description="Researcher label")
    permissions: list[Permission] | None = Field(
        None,
        description=(
            "Optional token permission scope. Every value must be granted to "
            "the AI researcher role."
        ),
    )


class ResearcherCreateRequest(
    PayloadRequest[Literal["researcher_create_request"], ResearcherCreateBody]
):
    """Create-researcher request envelope."""

    type: Literal["researcher_create_request"] = "researcher_create_request"


class ResearcherRead(StrictBody):
    """Public projection of a provisioned researcher.

    Attributes:
        public_id: Stable user public identifier.
        username: Generated automation username.
        label: Normalized human-readable label encoded in the username.
        created_by_user_public_id: Public identifier of the provisioning
            operator.
        created_at: Creation timestamp.
        is_active: Whether the principal may authenticate.
    """

    public_id: str
    username: str
    label: str
    created_by_user_public_id: str
    created_at: datetime
    is_active: bool


class ResearcherCreatedPayload(StrictBody):
    """One-shot researcher provisioning result.

    Attributes:
        researcher: Projection of the newly provisioned principal.
        access_token: Long-lived bearer token returned only by creation.
        expires_in: Token lifetime in seconds.
    """

    researcher: ResearcherRead
    access_token: str
    expires_in: int


class ResearcherCreatedResponse(
    PayloadResponse[Literal["researcher_created_response"], ResearcherCreatedPayload]
):
    """Create-researcher response envelope."""

    type: Literal["researcher_created_response"] = "researcher_created_response"
