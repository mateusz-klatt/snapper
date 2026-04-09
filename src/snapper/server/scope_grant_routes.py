"""REST API routes for scope grant read access.

Returns the active ``wallet_operator_scope_grants`` rows on a given
wallet. The wallet ID is required to keep the query bounded and to
anchor the authorization check: the caller must be allowed to see
the wallet before they can see any grants on it.

Authorization rules:

- ADMIN principals may query any wallet.
- VIEWER / OPERATOR principals may query only wallets covered by
  at least one active scope grant from their operator set. A
  caller asking about a wallet outside that set receives 403.

Listing grants is read-only here — create / handover flows live
on separate write routes that additionally enforce
``MANAGE_SCOPE_GRANTS`` and the instrument-exclusive overlap
check.
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

from snapper.api.schemas.multi_tenant import ScopeGrantInfo
from snapper.api.schemas.multi_tenant import ScopeGrantListResponse
from snapper.auth.dependencies import require_authentication
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository import Repository
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.dependencies import get_repository_dependency

router = APIRouter(prefix="/scope-grants", tags=["scope-grants"])

_REST_STREAM = "rest.scope_grants"

_WALLET_NOT_VISIBLE = "Scope grants on this wallet are not visible to the current operator set"


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
    items = [
        ScopeGrantInfo(
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
        for row in rows
    ]
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
