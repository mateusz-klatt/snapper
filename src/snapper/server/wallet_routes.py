"""REST API routes for wallet catalogue read access.

Provides the Phase 0d frontend wallet picker with the list of
wallets the current principal can act on. ADMIN principals see
every active wallet; VIEWER and OPERATOR principals see only the
subset covered by at least one of their active scope grants.

The endpoint is intentionally read-only — credential management
(create / rotate / restart) lives on the future write routes and
never co-returns the encrypted payload. Gap detection provenance
(``session_id``, ``sequence_id``, ``public_id``, ``timestamp``) is
minted from ``request.app.state.rest_tracker`` so the stream stays
uniform with the rest of the REST surface.
"""

import datetime as dt
from datetime import UTC
from datetime import datetime
from typing import Annotated
from uuid import uuid7

from fastapi import APIRouter
from fastapi import Depends
from fastapi import Request

from snapper.api.schemas.multi_tenant import WalletInfo
from snapper.api.schemas.multi_tenant import WalletListResponse
from snapper.auth.dependencies import require_authentication
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository import Repository
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.dependencies import get_repository_dependency

router = APIRouter(prefix="/wallets", tags=["wallets"])

_REST_STREAM = "rest.wallets"


@router.get("")
async def list_wallets(
    request: Request,
    principal: Annotated[AuthPrincipal, Depends(require_authentication)],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> WalletListResponse:
    """List wallets accessible to the current principal.

    ADMIN sees every active wallet. VIEWER and OPERATOR see only the
    wallets covered by at least one active scope grant from the
    principal's operator set — matching the Phase 0d wallet picker
    contract that the picker is filtered server-side.

    Args:
        request: FastAPI request (provides REST tracker for provenance).
        principal: Authenticated caller — the wallet visibility scope
            is derived from its role and ``operator_public_ids``.
        repo: Repository dependency.

    Returns:
        ``WalletListResponse`` with one ``WalletInfo`` entry per
        accessible active wallet, ordered deterministically by
        ``(is_paper, label)``.
    """
    now = datetime.now(UTC)
    if principal.role == UserRole.ADMIN:
        rows = await repo.list_active_wallets(now)
    else:
        rows = await repo.list_accessible_wallets_for_operators(principal.operator_public_ids, now)
    items = [
        WalletInfo(
            session_id=row["session_id"],
            sequence_id=row["sequence_id"],
            public_id=row["public_id"],
            timestamp=row["timestamp"],
            label=row["label"],
            description=row["description"],
            is_paper=row["is_paper"],
        )
        for row in rows
    ]
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = dt.datetime.now(dt.UTC)
    pid = str(uuid7())
    return WalletListResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
        payload=items,
        count=len(items),
    )
