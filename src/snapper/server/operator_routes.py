"""REST API routes for operator catalogue read access.

Provides the frontend operator picker with the list of
operators the current principal may act AS. Principals granted
``IMPERSONATE_OPERATOR`` see every active operator (matching the expansion rule in
``UserService.build_auth_principal`` / ``get_user_with_operators``)
other principals see only the operators covered by
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
from fastapi import HTTPException
from fastapi import Request
from fastapi import status

from snapper.api.schemas.multi_tenant import CreateOperatorCommand
from snapper.api.schemas.multi_tenant import OperatorInfo
from snapper.api.schemas.multi_tenant import OperatorListResponse
from snapper.api.schemas.multi_tenant import OperatorResponse
from snapper.auth.dependencies import require_authentication
from snapper.auth.dependencies import require_permission
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.permissions import has_effective_permission
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository import OperatorConflictError
from snapper.data.repository import Repository
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.dependencies import get_repository_dependency
from snapper.server.json_body import json_body
from snapper.server.json_body import openapi_schema

router = APIRouter(prefix="/operators", tags=["operators"])

_REST_STREAM = "rest.operators"


@router.get("")
async def list_operators(
    request: Request,
    principal: Annotated[AuthPrincipal, Depends(require_authentication)],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> OperatorListResponse:
    """List operators accessible to the current principal.

    A caller granted ``IMPERSONATE_OPERATOR`` sees every active operator.
    Other callers see only the operators in ``principal.operator_public_ids``, resolved to
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
    if has_effective_permission(
        principal.role,
        principal.permissions,
        principal.permission_scope_version,
        Permission.IMPERSONATE_OPERATOR,
    ):
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


@router.post("", openapi_extra=openapi_schema(CreateOperatorCommand))
async def create_operator(
    request: Request,
    _principal: Annotated[
        AuthPrincipal,
        Depends(require_permission(Permission.MANAGE_SCOPE_GRANTS)),
    ],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    command: Annotated[CreateOperatorCommand, Depends(json_body(CreateOperatorCommand))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> OperatorResponse:
    """Create a new active operator.

    Guarded by ``MANAGE_SCOPE_GRANTS`` (currently ADMIN-only): operators are the
    principals that scope grants target and that AI delegates bind to, so the
    permission that manages grants also mints the operators they reference.
    Creating a distinct operator is the prerequisite for scoping an AI delegate
    independently of the seed ``default`` operator. The active-unique index on
    ``label`` is enforced at the DB layer and bubbles up as HTTP 409 via
    ``OperatorConflictError``.

    Args:
        request: FastAPI request (provides REST tracker for provenance).
        _principal: Authenticated caller holding MANAGE_SCOPE_GRANTS.
        _csrf: CSRF guard dependency.
        command: Create command envelope.
        repo: Repository dependency.

    Returns:
        ``OperatorResponse`` wrapping the newly-inserted operator row.

    Raises:
        HTTPException: 409 when an active operator with the same label exists.
    """
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = dt.datetime.now(dt.UTC)
    pid = str(uuid7())
    body = command.payload
    try:
        row = await repo.create_operator(
            label=body.label,
            description=body.description,
            session_id=sid,
            sequence_id=seq,
            timestamp=ts,
        )
    except OperatorConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return OperatorResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
        payload=OperatorInfo(
            session_id=row["session_id"],
            sequence_id=row["sequence_id"],
            public_id=row["public_id"],
            timestamp=row["timestamp"],
            label=row["label"],
            description=row["description"],
        ),
    )
