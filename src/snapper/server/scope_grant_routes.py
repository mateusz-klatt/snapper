"""REST API routes for scope grant read + write access.

Returns the active ``wallet_operator_scope_grants`` rows on a given
wallet and accepts create + handover commands.
Authorization rules
Principals granted ``IMPERSONATE_OPERATOR`` may query any wallet.
Other principals may query only wallets covered by at least one
  active scope grant from their operator set (read gate). Mutation
  additionally requires the ``MANAGE_SCOPE_GRANTS`` permission,
  currently granted only to ADMIN.
Create-grant conflicts (409) and handover cross-scope violations
(409) flow up from the repository via ``ScopeGrantConflictError``
validation failures (XOR mismatch, unknown operator / grant) flow
up via ``ScopeGrantNotFoundError`` / ``ScopeGrantValidationError``.
"""

import datetime as dt
from datetime import UTC
from datetime import datetime
from typing import Annotated
from uuid import UUID
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
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.permissions import role_grants_permission
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


def _require_canonical_uuid(value: str, field: str) -> None:
    """Reject a public-id field that is not a canonical-form UUID.

    Python's ``uuid.UUID`` parser tolerates ``urn:uuid:``-prefixed,
    brace-wrapped, and over-hyphenated spellings that asyncpg/Postgres
    reject at bind time. Because the route forwards the ORIGINAL string
    unchanged to the UUID-typed DB column, only the canonical dashed
    form (lower- or upper-case) is accepted here so a
    tolerated-but-non-canonical value fails with a clean 400 instead of
    an asyncpg ``DataError`` surfacing as an uncaught 500.

    Args:
        value: Candidate public-id string taken from the request body.
        field: Field name embedded in the 400 detail for the client.

    Raises:
        HTTPException: 400 when ``value`` is not parseable as a UUID or
            is not already in canonical dashed form.
    """
    try:
        parsed = UUID(value)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"{field} must be a well-formed UUID",
        ) from exc
    if str(parsed) != value.lower():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"{field} must be a canonical UUID",
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
    """Reject structurally invalid create payloads before the service call.

    Enforces three invariants so the client receives a clean 400 rather
    than an opaque DB error surfacing as a 500:

    1. ``operator_public_id`` and ``wallet_public_id`` are canonical
       UUIDs — both feed UUID-typed DB columns on the insert / lookup
       path, so a non-UUID value would raise an asyncpg ``DataError``.
    2. Exactly one of ``underlying_public_id`` / ``instrument_public_id``
       is set, matching ``scope_kind`` (XOR).
    3. The populated resource id is a canonical UUID (same DB-column
       reasoning; a bare ticker like ``"BTC"`` must not reach the DB).

    Args:
        body: The create command envelope to validate.

    Raises:
        HTTPException: 400 when any public id is not a canonical UUID or
            the scope_kind/id XOR invariant is violated.
    """
    payload = body.payload
    _require_canonical_uuid(payload.operator_public_id, "operator_public_id")
    _require_canonical_uuid(payload.wallet_public_id, "wallet_public_id")
    underlying = payload.underlying_public_id
    instrument = payload.instrument_public_id
    if payload.scope_kind == "underlying":
        if underlying is None or instrument is not None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail=_SCOPE_XOR_VIOLATION
            )
        _require_canonical_uuid(underlying, "underlying_public_id")
    else:
        if instrument is None or underlying is not None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail=_SCOPE_XOR_VIOLATION
            )
        _require_canonical_uuid(instrument, "instrument_public_id")


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

    Callers without ``IMPERSONATE_OPERATOR`` must have visibility into the target wallet
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
    if not role_grants_permission(principal.role, Permission.IMPERSONATE_OPERATOR):
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


def _scope_grant_service_dependency() -> ScopeGrantService:
    """FastAPI dependency returning the shared ``ScopeGrantService`` singleton."""
    return get_scope_grant_service()


@router.post("", openapi_extra=openapi_schema(CreateScopeGrantCommand))
async def create_scope_grant(
    request: Request,
    _principal: Annotated[
        AuthPrincipal, Depends(require_permission(Permission.MANAGE_SCOPE_GRANTS))
    ],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    command: Annotated[CreateScopeGrantCommand, Depends(json_body(CreateScopeGrantCommand))],
    scope_grant_service: Annotated[ScopeGrantService, Depends(_scope_grant_service_dependency)],
) -> ScopeGrantResponse:
    """Create a new active scope grant via :class:`ScopeGrantService`.

    The XOR invariant between ``scope_kind`` and
    ``underlying_public_id`` / ``instrument_public_id`` is validated
    before the service call so the client gets a 400 with a clear
    message rather than opaque DB constraint errors. Overlap
    conflicts (same-scope or cross-scope) bubble up as 409; the
    underlying repository method holds an advisory lock on
    PostgreSQL to prevent races.

    ``granted_by_user_public_id`` is taken from the principal so the
    client cannot spoof an audit identity. The service publishes
    ``admin.scope_granted`` after the insert commit so any subscriber
    (WebSocket-auth manager, market-persist policy) can react live.

    Args:
        request: FastAPI request (provides REST tracker for provenance).
        _principal: Authenticated caller holding MANAGE_SCOPE_GRANTS.
        command: Create command envelope.
        scope_grant_service: Service singleton (single-publisher).

    Returns:
        ``ScopeGrantResponse`` wrapping the newly-inserted grant row.

    Raises:
        HTTPException: 400 on XOR violation or a malformed (non-UUID)
            resource id; 409 on overlap; 404 if the target operator /
            wallet does not exist.
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
        row = await scope_grant_service.create_grant(insert_request, now=ts)
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
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    command: Annotated[HandoverScopeGrantCommand, Depends(json_body(HandoverScopeGrantCommand))],
    scope_grant_service: Annotated[ScopeGrantService, Depends(_scope_grant_service_dependency)],
) -> HandoverScopeGrantResponse:
    """Atomically transfer an active scope grant via :class:`ScopeGrantService`.

    Single transaction: SCD2-close the source grant and insert a new
    grant under the destination operator carrying the same
    ``scope_kind`` + public IDs. ``granted_by_user_public_id`` is
    taken from the principal. Cross-scope overlap against the
    destination operator's existing grants raises 409 and the
    transaction is rolled back.

    The service publishes ``admin.scope_handed_over`` after the
    repo transaction commit so subscribers can refresh derived state.

    Args:
        request: FastAPI request (provides REST tracker for provenance).
        _principal: Authenticated caller holding MANAGE_SCOPE_GRANTS.
        command: Handover command envelope.
        scope_grant_service: Service singleton (single-publisher).

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
        closed, new_row = await scope_grant_service.handover(
            grant_public_id=body.from_grant_public_id,
            destination_operator_public_id=body.to_operator_public_id,
            handover_by_user_public_id=_principal.user_public_id,
            reason=body.reason,
            session_id=sid,
            sequence_id=seq,
            now=ts,
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
    _csrf: Annotated[None, Depends(validate_csrf_token)],
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
        scope_grant_service: Service singleton (single-publisher).

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
