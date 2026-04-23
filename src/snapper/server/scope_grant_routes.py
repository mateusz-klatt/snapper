"""REST API routes for scope grant read + write access.

Returns the active ``wallet_operator_scope_grants`` rows on a given
wallet and accepts create + handover commands.
Authorization rules
ADMIN principals may query / mutate any wallet.
VIEWER / OPERATOR may query only wallets covered by at least one
  active scope grant from their operator set (read gate). Mutation
  additionally requires the ``MANAGE_SCOPE_GRANTS`` permission
  which is ADMIN-only at launch — the permission exists so
  a future delegated-admin role can hold grant-management power
  without full ADMIN.
Create-grant conflicts (409) and handover cross-scope violations
(409) flow up from the repository via ``ScopeGrantConflictError``
validation failures (XOR mismatch, unknown operator / grant) flow
up via ``ScopeGrantNotFoundError`` / ``ScopeGrantValidationError``.
"""

import datetime as dt
from datetime import UTC
from datetime import datetime
from typing import Annotated
from uuid import uuid7

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Query
from fastapi import Request
from fastapi import status

from snapper.api.schemas.multi_tenant import CreateScopeGrantCommand
from snapper.api.schemas.multi_tenant import HandoverScopeGrantCommand
from snapper.api.schemas.multi_tenant import HandoverScopeGrantResponse
from snapper.api.schemas.multi_tenant import HandoverScopeGrantResult
from snapper.api.schemas.multi_tenant import RevokeScopeGrantCommand
from snapper.api.schemas.multi_tenant import RevokeScopeGrantResponse
from snapper.api.schemas.multi_tenant import ScopeGrantInfo
from snapper.api.schemas.multi_tenant import ScopeGrantListResponse
from snapper.api.schemas.multi_tenant import ScopeGrantResponse
from snapper.auth.dependencies import require_authentication
from snapper.auth.dependencies import require_permission
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.scope_grant_service import ScopeGrantService
from snapper.auth.scope_grant_service import get_scope_grant_service
from snapper.data.repository import Repository
from snapper.data.repository import ScopeGrantConflictError
from snapper.data.repository import ScopeGrantNotFoundError
from snapper.data.repository import ScopeGrantValidationError
from snapper.data.repository_types import CreateScopeGrantRequest
from snapper.data.repository_types import ScopeGrantRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.dependencies import get_repository_dependency
from snapper.server.json_body import json_body
from snapper.server.json_body import openapi_schema

router = APIRouter(prefix="/scope-grants", tags=["scope-grants"])

_REST_STREAM = "rest.scope_grants"

_WALLET_NOT_VISIBLE = "Scope grants on this wallet are not visible to the current operator set"

_SCOPE_XOR_VIOLATION = (
    "Exactly one of underlying_public_id / instrument_public_id must match scope_kind"
)


def _scope_grant_info(row: ScopeGrantRow) -> ScopeGrantInfo:
    """Project a ``ScopeGrantRow`` TypedDict to the transport schema."""
    return ScopeGrantInfo(
        session_id=row["session_id"],
        sequence_id=row["sequence_id"],
        public_id=row["public_id"],
        timestamp=row["timestamp"],
        operator_public_id=row["operator_public_id"],
        wallet_public_id=row["wallet_public_id"],
        granted_by_user_public_id=row["granted_by_user_public_id"],
        scope_kind=row["scope_kind"],
        underlying_public_id=row["underlying_public_id"],
        instrument_public_id=row["instrument_public_id"],
        note=row["note"],
        known_to=row["known_to"],
    )


def _validate_scope_xor(body: CreateScopeGrantCommand) -> None:
    """Reject create payloads whose scope_kind doesn't match the provided IDs."""
    payload = body.payload
    underlying_set = payload.underlying_public_id is not None
    instrument_set = payload.instrument_public_id is not None
    if payload.scope_kind == "underlying":
        matches = underlying_set and not instrument_set
    else:
        matches = instrument_set and not underlying_set
    if not matches:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=_SCOPE_XOR_VIOLATION)


@router.get("")
async def list_scope_grants(
    request: Request,
    principal: Annotated[AuthPrincipal, Depends(require_authentication)],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    wallet_public_id: Annotated[
        str,
        Query(description="Public ID of the wallet whose active grants to list"),
    ],
) -> ScopeGrantListResponse:
    """List active scope grants on a given wallet.

    Non-ADMIN callers must have visibility into the target wallet
    through their operator set; otherwise the request is rejected
    with 403 so the existence of the wallet is not leaked through
    an empty payload vs. a 404.

    Args:
        request: FastAPI request (provides REST tracker for provenance).
        principal: Authenticated caller.
        repo: Repository dependency.
        wallet_public_id: Public ID of the wallet to query.

    Returns:
        ``ScopeGrantListResponse`` ordered by ``timestamp`` ascending.

    Raises:
        HTTPException: 403 if the caller cannot see the target wallet.
    """
    now = datetime.now(UTC)
    if principal.role != UserRole.ADMIN:
        accessible = await repo.list_accessible_wallets_for_operators(
            principal.operator_public_ids, now
        )
        accessible_ids = {row["public_id"] for row in accessible}
        if wallet_public_id not in accessible_ids:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=_WALLET_NOT_VISIBLE,
            )
    rows = await repo.list_active_scope_grants_for_wallet(wallet_public_id, now)
    items = [_scope_grant_info(row) for row in rows]
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = dt.datetime.now(dt.UTC)
    pid = str(uuid7())
    return ScopeGrantListResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
        payload=items,
        count=len(items),
    )


@router.post("", openapi_extra=openapi_schema(CreateScopeGrantCommand))
async def create_scope_grant(
    request: Request,
    _principal: Annotated[
        AuthPrincipal, Depends(require_permission(Permission.MANAGE_SCOPE_GRANTS))
    ],
    command: Annotated[CreateScopeGrantCommand, Depends(json_body(CreateScopeGrantCommand))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> ScopeGrantResponse:
    """Create a new active scope grant.

    The XOR invariant between ``scope_kind`` and
    ``underlying_public_id`` / ``instrument_public_id`` is validated
    before the repository call so the client gets a 400 with a clear
    message rather than opaque DB constraint errors. Overlap
    conflicts (same-scope or cross-scope) bubble up as 409; the
    underlying repository method holds an advisory lock on
    PostgreSQL to prevent races.

    ``granted_by_user_public_id`` is taken from the principal so
    the client cannot spoof an audit identity.

    Args:
        request: FastAPI request (provides REST tracker for provenance).
        _principal: Authenticated caller holding MANAGE_SCOPE_GRANTS.
        command: Create command envelope.
        repo: Repository dependency.

    Returns:
        ``ScopeGrantResponse`` wrapping the newly-inserted grant row.

    Raises:
        HTTPException: 400 on XOR violation; 409 on overlap; 404 if
            the target operator / wallet does not exist (bubbles up
            via ``ScopeGrantNotFoundError`` from the repository).
    """
    _validate_scope_xor(command)
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = dt.datetime.now(dt.UTC)
    pid = str(uuid7())
    body = command.payload
    insert_request = CreateScopeGrantRequest(
        operator_public_id=body.operator_public_id,
        wallet_public_id=body.wallet_public_id,
        granted_by_user_public_id=_principal.user_public_id,
        scope_kind=body.scope_kind,
        underlying_public_id=body.underlying_public_id,
        instrument_public_id=body.instrument_public_id,
        note=body.note,
        session_id=sid,
        sequence_id=seq,
        timestamp=ts,
    )
    try:
        row = await repo.create_scope_grant(insert_request)
    except ScopeGrantConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    except ScopeGrantValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except ScopeGrantNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    return ScopeGrantResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
        payload=_scope_grant_info(row),
    )


@router.post("/handover", openapi_extra=openapi_schema(HandoverScopeGrantCommand))
async def handover_scope_grant(
    request: Request,
    _principal: Annotated[
        AuthPrincipal, Depends(require_permission(Permission.MANAGE_SCOPE_GRANTS))
    ],
    command: Annotated[HandoverScopeGrantCommand, Depends(json_body(HandoverScopeGrantCommand))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> HandoverScopeGrantResponse:
    """Atomically transfer an active scope grant to a different operator.

    Single transaction: SCD2-close the source grant and insert a new
    grant under the destination operator carrying the same
    ``scope_kind`` + public IDs. ``granted_by_user_public_id`` is
    taken from the principal. Cross-scope overlap against the
    destination operator's existing grants raises 409 and the
    transaction is rolled back.

    Args:
        request: FastAPI request (provides REST tracker for provenance).
        _principal: Authenticated caller holding MANAGE_SCOPE_GRANTS.
        command: Handover command envelope.
        repo: Repository dependency.

    Returns:
        ``HandoverScopeGrantResponse`` wrapping both the closed
        source grant and the new active grant so the client can
        update caches without a second round trip.

    Raises:
        HTTPException: 400 on self-handover or other validation;
            404 when the source grant or destination operator is
            missing; 409 on cross-scope overlap.
    """
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = dt.datetime.now(dt.UTC)
    pid = str(uuid7())
    body = command.payload
    try:
        closed, new_row = await repo.handover_grant(
            from_grant_public_id=body.from_grant_public_id,
            to_operator_public_id=body.to_operator_public_id,
            granted_by_user_public_id=_principal.user_public_id,
            reason=body.reason,
            session_id=sid,
            sequence_id=seq,
            timestamp=ts,
        )
    except ScopeGrantValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except ScopeGrantNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ScopeGrantConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return HandoverScopeGrantResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
        payload=HandoverScopeGrantResult(
            closed_grant=_scope_grant_info(closed),
            new_grant=_scope_grant_info(new_row),
        ),
    )


def _scope_grant_service_dependency() -> ScopeGrantService:
    """FastAPI dependency returning the shared ``ScopeGrantService`` singleton."""
    return get_scope_grant_service()


@router.post(
    "/{grant_public_id}/revoke",
    openapi_extra=openapi_schema(RevokeScopeGrantCommand),
)
async def revoke_scope_grant(
    request: Request,
    grant_public_id: str,
    _principal: Annotated[
        AuthPrincipal, Depends(require_permission(Permission.MANAGE_SCOPE_GRANTS))
    ],
    command: Annotated[RevokeScopeGrantCommand, Depends(json_body(RevokeScopeGrantCommand))],
    scope_grant_service: Annotated[ScopeGrantService, Depends(_scope_grant_service_dependency)],
) -> RevokeScopeGrantResponse:
    """Atomically close an active scope grant (SCD2 close in place).

    Publishes ``admin.scope_revoked`` after commit so the admin-bus
    subscriber can narrow affected AI_DELEGATE subscriptions live
    without reconnect. The closed row has ``known_to`` stamped at the
    revoke timestamp; no new row is inserted.

    ``revoked_by_user_public_id`` is taken from the principal so the
    client cannot spoof an audit identity.

    Args:
        request: FastAPI request (provides REST tracker for provenance).
        grant_public_id: Public ID of the active grant to revoke.
        _principal: Authenticated caller holding MANAGE_SCOPE_GRANTS.
        command: Revoke command envelope.
        scope_grant_service: Service singleton (single-publisher per §D7).

    Returns:
        ``RevokeScopeGrantResponse`` wrapping the closed grant.

    Raises:
        HTTPException: 404 when the grant does not exist or is already
            closed (double-revoke).
    """
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = dt.datetime.now(dt.UTC)
    pid = str(uuid7())
    try:
        closed = await scope_grant_service.revoke_grant(
            grant_public_id=grant_public_id,
            revoked_by_user_public_id=_principal.user_public_id,
            reason=command.payload.reason,
            now=ts,
        )
    except ScopeGrantNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    return RevokeScopeGrantResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
        payload=_scope_grant_info(closed),
    )
