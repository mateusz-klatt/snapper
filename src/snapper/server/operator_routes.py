"""REST API routes for operator catalogue read access.

Provides the frontend operator picker with the list of
operators the current principal may act AS. ADMIN principals see
every active operator (matching the ADMIN-wide expansion rule in
``UserService.build_auth_principal`` / ``get_user_with_operators``)
VIEWER and OPERATOR principals see only the operators covered by
their ``user_operator_memberships`` (exposed on the principal as
``operator_public_ids`` at token issue time).
The endpoint performs a server-side filter even though
``/auth/me`` already returns ``operator_public_ids`` — callers may
want a richer projection (``label``, ``description``) for the
picker UI, and the filter belongs in one place.
"""

import datetime as dt
from datetime import UTC
from datetime import datetime
from typing import Annotated
from uuid import uuid7

from fastapi import APIRouter
from fastapi import Depends
from fastapi import Request

from snapper.api.schemas.multi_tenant import OperatorInfo
from snapper.api.schemas.multi_tenant import OperatorListResponse
from snapper.auth.dependencies import require_authentication
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository import Repository
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.dependencies import get_repository_dependency

router = APIRouter(prefix="/operators", tags=["operators"])

_REST_STREAM = "rest.operators"


@router.get("")
async def list_operators(
    request: Request,
    principal: Annotated[AuthPrincipal, Depends(require_authentication)],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> OperatorListResponse:
    """List operators accessible to the current principal.

    ADMIN sees every active operator. VIEWER and OPERATOR see only
    the operators in ``principal.operator_public_ids``, resolved to
    ``OperatorInfo`` projections via ``list_active_operators`` so
    the label / description fields are populated for the picker UI.

    Args:
        request: FastAPI request (provides REST tracker for provenance).
        principal: Authenticated caller.
        repo: Repository dependency.

    Returns:
        ``OperatorListResponse`` ordered by ``label`` ascending.
        Empty payload when the principal has no operator memberships.
    """
    now = datetime.now(UTC)
    all_active = await repo.list_active_operators(now)
    if principal.role == UserRole.ADMIN:
        visible = all_active
    else:
        allowed = set(principal.operator_public_ids)
        visible = [row for row in all_active if row["public_id"] in allowed]
    items = [
        OperatorInfo(
            session_id=row["session_id"],
            sequence_id=row["sequence_id"],
            public_id=row["public_id"],
            timestamp=row["timestamp"],
            label=row["label"],
            description=row["description"],
        )
        for row in visible
    ]
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = dt.datetime.now(dt.UTC)
    pid = str(uuid7())
    return OperatorListResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
        payload=items,
        count=len(items),
    )
