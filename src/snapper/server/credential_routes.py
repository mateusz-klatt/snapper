"""REST API routes for wallet credential management.

All endpoints require ``MANAGE_WALLET_CREDENTIALS`` (ADMIN only).
The GET listing returns ``CredentialSummary``
projections that deliberately omit the ``encrypted_payload`` column
so ciphertext never reaches the wire. Create and rotate accept
plaintext credential fields in the request body and Fernet-encrypt
them server-side before the DB insert.
Credential rotation is an SCD2 close + insert: the old row's
``known_to`` is stamped at the rotation bus time and a new active
row carries the updated encrypted payload. The closed row remains
queryable via ``as_of`` for historical audit.
"""

import datetime as dt
import json
from datetime import UTC
from datetime import datetime
from typing import Annotated
from uuid import uuid7

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Request
from fastapi import status

from snapper.api.schemas.multi_tenant import CreateCredentialCommand
from snapper.api.schemas.multi_tenant import CredentialListResponse
from snapper.api.schemas.multi_tenant import CredentialReconciliationMethodInfo
from snapper.api.schemas.multi_tenant import CredentialReconciliationMethodResponse
from snapper.api.schemas.multi_tenant import CredentialResponse
from snapper.api.schemas.multi_tenant import CredentialSummary
from snapper.api.schemas.multi_tenant import RotateCredentialCommand
from snapper.api.schemas.multi_tenant import SetCredentialReconciliationMethodCommand
from snapper.application.portfolio.reconciliation_methods import PortfolioReconciliationMethod
from snapper.auth.dependencies import require_permission
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.permissions import Permission
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository import CredentialConflictError
from snapper.data.repository import CredentialNotFoundError
from snapper.data.repository import ReconciliationMethodImmutableError
from snapper.data.repository import Repository
from snapper.data.repository_types import WalletCredentialRow
from snapper.infrastructure.exchanges.reconciliation_policy import account_mode_for_exchange
from snapper.infrastructure.exchanges.reconciliation_policy import is_reconciliation_method_allowed
from snapper.infrastructure.security.encryption import get_encryption_service
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.dependencies import get_repository_dependency
from snapper.server.json_body import json_body
from snapper.server.json_body import openapi_schema

router = APIRouter(prefix="/wallets", tags=["wallet-credentials"])

_REST_STREAM = "rest.wallet_credentials"

_REQUIRED_FIELDS: dict[str, set[str]] = {
    "api_key_secret": {"api_key", "api_secret"},
    "rsa_pem": {"api_key", "private_key_pem"},
    "oauth": {"client_id", "client_secret", "refresh_token"},
    "paper": {"initial_balance"},
}


def _credential_summary(row: WalletCredentialRow) -> CredentialSummary:
    """Project a ``WalletCredentialRow`` to the transport schema (no payload)."""
    return CredentialSummary(
        session_id=row["session_id"],
        sequence_id=row["sequence_id"],
        public_id=row["public_id"],
        timestamp=row["timestamp"],
        wallet_public_id=row["wallet_public_id"],
        exchange=row["exchange"],
        credential_type=row["credential_type"],
        label=row["label"],
    )


def _validate_payload_fields_for_type(
    credential_type: str,
    credential_payload: dict[str, str],
) -> None:
    """Reject payloads missing required keys for the given credential_type.

    Called by both create (where ``credential_type`` comes from the request
    body and is already Pydantic-validated) and rotate (where it comes from
    the existing DB row — string, not Literal). The fallback to an empty
    required set avoids crashing on an unknown type; validation of the type
    enum itself lives on the DB CHECK constraint + Pydantic Literal (create)
    and on the existing row (rotate).
    """
    required = _REQUIRED_FIELDS.get(credential_type, set())
    provided = set(credential_payload.keys())
    missing = required - provided
    if missing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"Missing required credential fields for {credential_type}: "
                f"{', '.join(sorted(missing))}"
            ),
        )


def _validate_reconciliation_method_for_credential(
    exchange: str,
    credential_type: str,
    method: PortfolioReconciliationMethod,
) -> str:
    """Validate one explicit method against the exact concrete adapter policy."""
    normalized_exchange = exchange.lower()
    paper_venue = account_mode_for_exchange(normalized_exchange) == "paper"
    paper_credential = credential_type == "paper"
    if paper_venue != paper_credential:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Paper credential type and paper venue must be used together",
        )
    if paper_venue:
        if method != "unclassified":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Paper credentials cannot have a live reconciliation method",
            )
        return normalized_exchange
    if method == "unclassified":
        return normalized_exchange
    if not is_reconciliation_method_allowed(normalized_exchange, method):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"Reconciliation method '{method}' is not allowed for the "
                f"registered '{normalized_exchange}' adapter"
            ),
        )
    return normalized_exchange


@router.get("/{wallet_public_id}/credentials")
async def list_credentials(
    request: Request,
    _principal: Annotated[
        AuthPrincipal,
        Depends(require_permission(Permission.READ_WALLET_CREDENTIALS)),
    ],
    wallet_public_id: str,
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> CredentialListResponse:
    """List active credentials on a wallet (summaries, no encrypted payload).

    Args:
        request: FastAPI request.
        _principal: Authenticated caller holding READ_WALLET_CREDENTIALS.
        wallet_public_id: Target wallet.
        repo: Repository dependency.

    Returns:
        ``CredentialListResponse`` ordered by ``exchange``.
    """
    now = datetime.now(UTC)
    rows = await repo.list_wallet_credentials_for_wallet(wallet_public_id, now)
    items = [_credential_summary(row) for row in rows]
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = dt.datetime.now(dt.UTC)
    pid = str(uuid7())
    return CredentialListResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
        payload=items,
        count=len(items),
    )


@router.post(
    "/{wallet_public_id}/credentials",
    openapi_extra=openapi_schema(CreateCredentialCommand),
)
async def create_credential(
    request: Request,
    _principal: Annotated[
        AuthPrincipal,
        Depends(require_permission(Permission.MANAGE_WALLET_CREDENTIALS)),
    ],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    wallet_public_id: str,
    command: Annotated[CreateCredentialCommand, Depends(json_body(CreateCredentialCommand))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> CredentialResponse:
    """Create a new wallet credential (encrypt + insert).

    The plaintext ``credential_payload`` is validated against the
    required-fields set for the declared ``credential_type`` and then
    Fernet-encrypted before the DB insert. The ciphertext is stored;
    the plaintext is discarded immediately. The response returns
    ``CredentialSummary`` (no payload) so the secret never appears on
    the wire response.

    Args:
        request: FastAPI request.
        _principal: Authenticated caller holding MANAGE_WALLET_CREDENTIALS.
        wallet_public_id: Target wallet (path param).
        command: Create command envelope.
        repo: Repository dependency.

    Returns:
        ``CredentialResponse`` wrapping a ``CredentialSummary``.

    Raises:
        HTTPException: 400 on missing payload fields; 409 if an active
            credential for the same ``(wallet, exchange)`` exists.
    """
    body = command.payload
    _validate_payload_fields_for_type(body.credential_type, body.credential_payload)
    exchange = _validate_reconciliation_method_for_credential(
        body.exchange,
        body.credential_type,
        body.reconciliation_method,
    )
    encryption = get_encryption_service()
    encrypted = encryption.encrypt(json.dumps(body.credential_payload))
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = dt.datetime.now(dt.UTC)
    pid = str(uuid7())
    try:
        row = await repo.create_wallet_credential(
            wallet_public_id=wallet_public_id,
            exchange=exchange,
            credential_type=body.credential_type,
            encrypted_payload=encrypted,
            label=body.label,
            reconciliation_method=body.reconciliation_method,
            session_id=sid,
            sequence_id=seq,
            timestamp=ts,
        )
    except (CredentialConflictError, ReconciliationMethodImmutableError) as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return CredentialResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
        payload=_credential_summary(row),
    )


@router.put(
    "/{wallet_public_id}/credentials/{credential_public_id}/reconciliation-method",
    openapi_extra=openapi_schema(SetCredentialReconciliationMethodCommand),
)
async def set_credential_reconciliation_method(
    request: Request,
    _principal: Annotated[
        AuthPrincipal,
        Depends(require_permission(Permission.MANAGE_WALLET_CREDENTIALS)),
    ],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    wallet_public_id: str,
    credential_public_id: str,
    command: Annotated[
        SetCredentialReconciliationMethodCommand,
        Depends(json_body(SetCredentialReconciliationMethodCommand)),
    ],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> CredentialReconciliationMethodResponse:
    """Classify one active live credential under its concrete adapter policy.

    Args:
        request: FastAPI request providing REST provenance tracking.
        _principal: Authenticated caller holding MANAGE_WALLET_CREDENTIALS.
        _csrf: CSRF validation dependency result.
        wallet_public_id: Wallet expected to own the credential.
        credential_public_id: Active credential to classify.
        command: Explicit real reconciliation-method command.
        repo: Repository dependency.

    Returns:
        Response containing the active durable method-config projection.

    Raises:
        HTTPException: 400 for prohibited adapter policy, 404 for an absent
            or differently owned credential, or 409 for immutable history.
    """
    now = datetime.now(UTC)
    credential = await repo.get_active_credential_by_id(credential_public_id, now)
    if credential is None or credential["wallet_public_id"] != wallet_public_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"active credential {credential_public_id} not found for wallet",
        )
    method = command.payload.reconciliation_method
    exchange = _validate_reconciliation_method_for_credential(
        credential["exchange"],
        credential["credential_type"],
        method,
    )
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    timestamp = dt.datetime.now(dt.UTC)
    response_public_id = str(uuid7())
    try:
        row = await repo.set_portfolio_reconciliation_method_config(
            wallet_public_id=wallet_public_id,
            exchange=exchange,
            mode="live",
            method=method,
            session_id=sid,
            sequence_id=seq,
            timestamp=timestamp,
        )
    except ReconciliationMethodImmutableError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    info = CredentialReconciliationMethodInfo(
        session_id=row["session_id"],
        sequence_id=row["sequence_id"],
        public_id=row["public_id"],
        timestamp=row["timestamp"],
        wallet_public_id=row["wallet_public_id"],
        exchange=row["exchange"],
        mode="live",
        method=method,
    )
    return CredentialReconciliationMethodResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=response_public_id,
        timestamp=timestamp,
        payload=info,
    )


@router.post(
    "/{wallet_public_id}/credentials/{credential_public_id}/rotate",
    openapi_extra=openapi_schema(RotateCredentialCommand),
)
async def rotate_credential(
    request: Request,
    _principal: Annotated[
        AuthPrincipal,
        Depends(require_permission(Permission.MANAGE_WALLET_CREDENTIALS)),
    ],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    wallet_public_id: str,
    credential_public_id: str,
    command: Annotated[RotateCredentialCommand, Depends(json_body(RotateCredentialCommand))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> CredentialResponse:
    """Rotate an existing credential (SCD2 close + insert).

    The old credential row is closed and a new active row is inserted
    with the provided encrypted payload. ``wallet_public_id`` in the
    path is informational (for URL readability); the repository looks
    up by ``credential_public_id`` only.

    Args:
        request: FastAPI request.
        _principal: Authenticated caller holding MANAGE_WALLET_CREDENTIALS.
        wallet_public_id: Informational path param (not used in query).
        credential_public_id: Public ID of the credential to rotate.
        command: Rotate command envelope.
        repo: Repository dependency.

    Returns:
        ``CredentialResponse`` wrapping the newly-inserted credential.

    Raises:
        HTTPException: 400 if the rotation payload is missing required
            fields for the existing credential's type; 404 if the
            credential is not found or already closed.
    """
    body = command.payload
    now = datetime.now(UTC)
    existing = await repo.get_active_credential_by_id(credential_public_id, now)
    if existing is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"active credential {credential_public_id} not found",
        )
    _validate_payload_fields_for_type(existing["credential_type"], body.credential_payload)
    encryption = get_encryption_service()
    encrypted = encryption.encrypt(json.dumps(body.credential_payload))
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = dt.datetime.now(dt.UTC)
    pid = str(uuid7())
    try:
        row = await repo.rotate_wallet_credential(
            credential_public_id=credential_public_id,
            encrypted_payload=encrypted,
            label=body.label,
            session_id=sid,
            sequence_id=seq,
            timestamp=ts,
        )
    except CredentialNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    return CredentialResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
        payload=_credential_summary(row),
    )
