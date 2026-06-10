"""REST API routes for wallet catalogue read + create access.

Provides the frontend wallet picker with the list of
wallets the current principal can act on, plus a guarded create
endpoint backing the admin Wallet Credentials tab. ADMIN
principals see every active wallet; non-ADMIN principals see only
the subset covered by at least one of their active scope grants.
Credential management (add / rotate / restart) lives on dedicated
routes under ``/wallets/{id}/credentials`` and never co-returns the
encrypted payload. Gap detection provenance
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
from fastapi import HTTPException
from fastapi import Request
from fastapi import status

from snapper.api.schemas.multi_tenant import CreateWalletCommand
from snapper.api.schemas.multi_tenant import WalletInfo
from snapper.api.schemas.multi_tenant import WalletListResponse
from snapper.api.schemas.multi_tenant import WalletResponse
from snapper.auth.dependencies import require_authentication
from snapper.auth.dependencies import require_permission
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository import Repository
from snapper.data.repository import WalletConflictError
from snapper.data.repository_types import WalletRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.dependencies import get_repository_dependency
from snapper.server.json_body import json_body
from snapper.server.json_body import openapi_schema

router = APIRouter(prefix="/wallets", tags=["wallets"])

_REST_STREAM = "rest.wallets"


def _wallet_info(row: WalletRow) -> WalletInfo:
    """Project a ``WalletRow`` TypedDict to the transport schema."""
    return WalletInfo(
        session_id=row["session_id"],
        sequence_id=row["sequence_id"],
        public_id=row["public_id"],
        timestamp=row["timestamp"],
        label=row["label"],
        description=row["description"],
        is_paper=row["is_paper"],
    )


@router.get("")
async def list_wallets(
    request: Request,
    principal: Annotated[AuthPrincipal, Depends(require_authentication)],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> WalletListResponse:
    """List wallets accessible to the current principal.

    ADMIN sees every active wallet. Non-ADMIN callers see only the
    wallets covered by at least one active scope grant from the
    principal's operator set — matching the wallet picker
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
    items = [_wallet_info(row) for row in rows]
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


@router.post("", openapi_extra=openapi_schema(CreateWalletCommand))
async def create_wallet(
    request: Request,
    _principal: Annotated[
        AuthPrincipal,
        Depends(require_permission(Permission.MANAGE_WALLET_CREDENTIALS)),
    ],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    command: Annotated[CreateWalletCommand, Depends(json_body(CreateWalletCommand))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> WalletResponse:
    """Create a new active wallet.

    Guarded by the ``MANAGE_WALLET_CREDENTIALS`` permission, which
    is currently granted only to ADMIN. A wallet is the container for
    credentials, so the same permission that manages credential
    rotation also creates the wallets that hold them.
    The active-unique index on ``(label, is_paper)`` is enforced at
    the DB layer and bubbles up as HTTP 409 via
    ``WalletConflictError``. Paper and live wallets may share the
    same label (e.g. ``default`` + ``default`` with different
    ``is_paper`` values) because they are disambiguated by the
    paper flag.

    Args:
        request: FastAPI request (provides REST tracker for provenance).
        _principal: Authenticated caller holding MANAGE_WALLET_CREDENTIALS.
        command: Create command envelope.
        repo: Repository dependency.

    Returns:
        ``WalletResponse`` wrapping the newly-inserted wallet row.

    Raises:
        HTTPException: 409 if a wallet with the same
            ``(label, is_paper)`` already exists.
    """
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = dt.datetime.now(dt.UTC)
    pid = str(uuid7())
    body = command.payload
    try:
        row = await repo.create_wallet(
            label=body.label,
            description=body.description,
            is_paper=body.is_paper,
            session_id=sid,
            sequence_id=seq,
            timestamp=ts,
        )
    except WalletConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return WalletResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
        payload=_wallet_info(row),
    )
