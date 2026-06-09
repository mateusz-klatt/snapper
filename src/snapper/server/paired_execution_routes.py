"""REST API routes for the paired-execution guard operator surface.

Two endpoints back the paired-execution runbook:

``GET /api/paired-execution/incidents`` — the per-SCOPE incident view: every
``(wallet, strategy, group_key)`` scope that carries an active durable halt
and/or a currently-exposed group (``broken`` / ``compensating`` /
``manual_intervention``), with each group's per-leg signed exposure. Modelled
per scope (NOT per halt row) because the halt is active-unique per scope and
its ``group_public_id`` can name a stale group while a sibling keeps it
alive, and an exposed scope can transiently lack a halt (crash / clear
windows) — surfaced via ``halt_missing`` instead of disappearing.

``POST /api/paired-execution/groups/{group_public_id}/terminalize`` — the
operator attestation that a ``manual_intervention`` group (or a
``compensating`` group re-opened onto a manual leg by a late fill) was
resolved at the venue. The DAL records the attestation by completing the
group; the guard scanner's quiet-halt sweep then clears the scope's durable
halt and every coordinator's in-memory mirror within one cycle — the REST
process never needs a control channel into the trader.

Both endpoints enforce wallet scoping on top of role permissions: VIEWER /
OPERATOR principals see and attest only scopes whose ``wallet_public_id`` is
in their accessible set; ADMIN is unscoped. The paired-execution tables exist
only on the SQL repository, so both endpoints narrow the injected repository
exactly like the trader does and answer 503 on a non-SQL deployment.
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

from snapper.api.schemas.paired_execution import PairedExecutionIncident
from snapper.api.schemas.paired_execution import PairedExecutionIncidentListResponse
from snapper.api.schemas.paired_execution import PairedGroupIncident
from snapper.api.schemas.paired_execution import PairedGroupTerminalizeResponse
from snapper.api.schemas.paired_execution import PairedHaltInfo
from snapper.api.schemas.paired_execution import PairedLegExposure
from snapper.auth.dependencies import require_permission
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.permissions import Permission
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.core.types import PairedExecutionGroupStatusEnum
from snapper.core.types import PairedGroupTerminalizeOutcome
from snapper.data.repository import Repository
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import PairedExecutionGroupRow
from snapper.data.repository_types import PairedExecutionHaltRow
from snapper.data.repository_types import PairedExecutionLegRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.dependencies import get_repository_dependency
from snapper.server.scoping import resolve_target_wallets

router = APIRouter(prefix="/paired-execution", tags=["paired-execution"])

_REST_STREAM = "rest.paired_execution"

_EXPOSED_GROUP_STATUSES = [
    PairedExecutionGroupStatusEnum.BROKEN.value,
    PairedExecutionGroupStatusEnum.COMPENSATING.value,
    PairedExecutionGroupStatusEnum.MANUAL_INTERVENTION.value,
]

_ScopeKey = tuple[str, str, str]


def _require_sql_repository(repo: Repository) -> SQLAlchemyRepository:
    """Narrow the injected repository to SQL or answer 503.

    The paired-execution tables (and their operator DALs) exist only on
    :class:`SQLAlchemyRepository` — the same narrowing the trader applies
    before touching any paired state.
    """
    if not isinstance(repo, SQLAlchemyRepository):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"error_code": "paired_execution_unavailable"},
        )
    return repo


def _leg_exposure(
    leg: PairedExecutionLegRow, sid: str, seq: int, ts: datetime
) -> PairedLegExposure:
    """Project a leg row into its runbook exposure view (open = filled − comp)."""
    return PairedLegExposure(
        session_id=sid,
        sequence_id=seq,
        public_id=str(uuid7()),
        timestamp=ts,
        leg_public_id=leg["public_id"],
        leg_index=leg["leg_index"],
        exchange=leg["exchange"],
        instrument=leg["instrument"],
        mode=leg["mode"],
        shard_key=leg["shard_key"],
        side=leg["side"],
        status=leg["status"],
        filled_signed_qty=leg["filled_signed_qty"],
        compensated_signed_qty=leg["compensated_signed_qty"],
        open_qty=leg["filled_signed_qty"] - leg["compensated_signed_qty"],
        compensation_seq=leg["compensation_seq"],
    )


async def _group_incident(
    repo: SQLAlchemyRepository,
    group: PairedExecutionGroupRow,
    sid: str,
    seq: int,
    ts: datetime,
) -> PairedGroupIncident:
    """Project a group row plus its CURRENT active legs into the incident view."""
    legs = await repo.get_current_paired_execution_legs(group["public_id"])
    return PairedGroupIncident(
        session_id=sid,
        sequence_id=seq,
        public_id=str(uuid7()),
        timestamp=ts,
        group_public_id=group["public_id"],
        status=group["status"],
        policy=group["policy"],
        failure_reason=group["failure_reason"],
        halted_at=group["halted_at"],
        created_at=group["created_at"],
        legs=[_leg_exposure(leg, sid, seq, ts) for leg in legs],
    )


def _group_is_exposed(incident: PairedGroupIncident) -> bool:
    """Mirror the guard scanner's exposure predicate for incident inclusion.

    ``manual_intervention`` and ``compensating`` groups are ALWAYS exposed
    (status exposure — a flatten in flight or an operator handoff); a
    ``broken`` group is exposed only when a leg carries a non-zero filled
    quantity, exactly like the scanner's halt sweep — so a zero-fill
    assembly-timeout break (which the scanner deliberately does NOT halt)
    never surfaces as a false ``halt_missing`` anomaly.
    """
    if incident.status in (
        PairedExecutionGroupStatusEnum.MANUAL_INTERVENTION.value,
        PairedExecutionGroupStatusEnum.COMPENSATING.value,
    ):
        return True
    return any(abs(leg.filled_signed_qty) > 0.0 for leg in incident.legs)


def _halt_info(halt: PairedExecutionHaltRow) -> PairedHaltInfo:
    """Project a durable halt row into the incident view."""
    return PairedHaltInfo(
        session_id=halt["session_id"],
        sequence_id=halt["sequence_id"],
        public_id=str(uuid7()),
        timestamp=halt["timestamp"],
        halt_public_id=halt["public_id"],
        reason=halt["reason"],
        group_public_id=halt["group_public_id"],
        created_at=halt["created_at"],
    )


@router.get("/incidents")
async def list_paired_execution_incidents(
    request: Request,
    principal: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_POSITIONS))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> PairedExecutionIncidentListResponse:
    """List every halted / exposed paired-execution scope visible to the caller.

    Builds the scope map from the UNION of active durable halts and current
    exposed groups, so an exposed scope whose halt row is transiently missing
    is still discovered (flagged ``halt_missing``) and a halt whose recorded
    group already settled still shows the sibling that keeps it alive. Scoped
    to the caller's accessible wallets (ADMIN unscoped). Deterministic order
    by (wallet, strategy, group_key).

    Args:
        request: FastAPI request (provides the REST tracker for provenance).
        principal: Authenticated caller (READ_POSITIONS-guarded).
        repo: Repository dependency (narrowed to SQL or 503).

    Returns:
        ``PairedExecutionIncidentListResponse`` with one incident per visible
        halted / exposed scope.
    """
    sql_repo = _require_sql_repository(repo)
    allowed = await resolve_target_wallets(principal, repo)
    halts = await sql_repo.list_active_paired_execution_halts()
    groups = await sql_repo.list_current_paired_execution_groups(_EXPOSED_GROUP_STATUSES)
    halts_by_scope: dict[_ScopeKey, PairedExecutionHaltRow] = {}
    groups_by_scope: dict[_ScopeKey, list[PairedExecutionGroupRow]] = {}
    for halt in halts:
        halts_by_scope[(halt["wallet_public_id"], halt["strategy_id"], halt["group_key"])] = halt
    for group in groups:
        key = (group["wallet_public_id"], group["strategy_id"], group["group_key"])
        groups_by_scope.setdefault(key, []).append(group)
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = dt.datetime.now(dt.UTC)
    items: list[PairedExecutionIncident] = []
    for key in sorted(set(halts_by_scope) | set(groups_by_scope)):
        wallet_public_id, strategy_id, group_key = key
        if allowed is not None and wallet_public_id not in allowed:
            continue
        halt_row = halts_by_scope.get(key)
        exposed: list[PairedGroupIncident] = []
        for group_row in groups_by_scope.get(key, []):
            incident = await _group_incident(sql_repo, group_row, sid, seq, ts)
            if _group_is_exposed(incident):
                exposed.append(incident)
        if halt_row is None and not exposed:
            continue
        items.append(
            PairedExecutionIncident(
                session_id=sid,
                sequence_id=seq,
                public_id=str(uuid7()),
                timestamp=ts,
                wallet_public_id=wallet_public_id,
                strategy_id=strategy_id,
                group_key=group_key,
                halt=None if halt_row is None else _halt_info(halt_row),
                halt_missing=halt_row is None and bool(exposed),
                groups=exposed,
            )
        )
    return PairedExecutionIncidentListResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=str(uuid7()),
        timestamp=ts,
        payload=items,
        count=len(items),
    )


@router.post("/groups/{group_public_id}/terminalize")
async def terminalize_paired_execution_group(
    request: Request,
    group_public_id: str,
    principal: Annotated[
        AuthPrincipal, Depends(require_permission(Permission.MANAGE_PAIRED_EXECUTION))
    ],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> PairedGroupTerminalizeResponse:
    """Attest that a manual-intervention paired group was resolved at the venue.

    Answers 404 BOTH when no current active group carries the id AND when the
    group's wallet is outside the caller's accessible set — a scoped caller
    must not be able to use the attestation surface as a cross-wallet
    existence oracle. 409 when the group is not attestable right now (not
    ``manual_intervention`` / not a ``compensating`` group re-opened onto a
    manual leg, no legs at all, a held original command, or a sibling leg
    still has automation in flight) — the 409 detail reports the group's
    freshly re-read status so a raced transition is not misreported. On
    success returns the completed group with its legs' true, unmodified
    accounting; the scanner clears the scope's halt and mirrors within one
    cycle. WALLET access is the authoritative authorization boundary for
    attestation (matching every other mutation surface); operator-level
    narrowing within a wallet is deliberately not enforced.

    Args:
        request: FastAPI request (provides the REST tracker for provenance).
        group_public_id: Public id of the group being attested.
        principal: Authenticated caller (MANAGE_PAIRED_EXECUTION-guarded).
        _csrf: CSRF validation dependency (mutation surface).
        repo: Repository dependency (narrowed to SQL or 503).

    Returns:
        ``PairedGroupTerminalizeResponse`` carrying the completed group with
        its legs' true, unmodified accounting.

    Raises:
        HTTPException: 404 (absent OR out-of-scope wallet — anti-oracle), 409
            (not attestable right now), 503 (non-SQL repository).
    """
    sql_repo = _require_sql_repository(repo)
    not_found = HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail={"error_code": "paired_group_not_found", "group_public_id": group_public_id},
    )
    group = await sql_repo.get_current_paired_execution_group(group_public_id)
    if group is None:
        raise not_found
    allowed = await resolve_target_wallets(principal, repo)
    if allowed is not None and group["wallet_public_id"] not in allowed:
        raise not_found
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    now = datetime.now(UTC)
    attested_by = principal.user_public_id or principal.username
    outcome = await sql_repo.terminalize_paired_execution_group(
        group_public_id, attested_by, now, sid, seq
    )
    if outcome == PairedGroupTerminalizeOutcome.NOT_FOUND:
        raise not_found
    if outcome == PairedGroupTerminalizeOutcome.NOT_TERMINALIZABLE:
        refreshed = await sql_repo.get_current_paired_execution_group(group_public_id)
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error_code": "paired_group_not_terminalizable",
                "group_public_id": group_public_id,
                "status": group["status"] if refreshed is None else refreshed["status"],
                "reason": (
                    "group is not manual_intervention (or compensating with a manual leg), "
                    "has no legs or a held original command, "
                    "or a sibling leg still has automation in flight"
                ),
            },
        )
    updated = await sql_repo.get_current_paired_execution_group(group_public_id)
    if updated is None:
        raise not_found
    payload = await _group_incident(sql_repo, updated, sid, seq, now)
    return PairedGroupTerminalizeResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=str(uuid7()),
        timestamp=now,
        payload=payload,
    )
