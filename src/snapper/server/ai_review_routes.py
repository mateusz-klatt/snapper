"""REST endpoints for the AI-review state machine.

Mirrors the MCP tool surface in :mod:`snapper.mcp.tools` so the bridge
can fall back to plain HTTP when the MCP transport is unavailable AND
so dashboard / operator tooling has a deterministic state-mutation
endpoint that does not require an MCP-aware client.

Routes
    ``POST /api/ai-reviews/{review_public_id}/decision`` — submit an
      AI delegate's decision. Same canonical envelope shape as the
      MCP tool wrapped in HTTP statuses
      (200 / 404 / 409 / 410 / 422 / 503).
    ``GET /api/ai-reviews/pending`` — list pending reviews for the
      calling delegate (where the delegate is the
      ``selected_delegate_public_id`` and ``fanout_after`` has
      elapsed). Optional ``limit`` query param caps the snapshot.
    ``GET /api/ai-reviews/{review_public_id}/aftermath`` — return one
      terminal review plus exact-scope trading activity since creation.

Permission gates
    ``POST .../decision`` requires :data:`Permission.CREATE_ORDERS` —
      matches the MCP tool. This REST surface deliberately follows
      the MCP precedent so a single delegate JWT works both transports
      without per-request role gymnastics.
    ``GET .../pending`` requires :data:`Permission.READ_SIGNALS` —
      AI_DELEGATE inherits this; non-delegate principals are admitted
      by the role check but get a 422 because the endpoint is keyed
      by ``AuthPrincipal.delegate_public_id``.
    ``GET .../aftermath`` uses the same permission and delegate identity
      requirement, then revalidates the delegate's active wallet and
      instrument grant before reading the projection.
"""

from datetime import UTC
from datetime import datetime
from typing import Annotated
from typing import Any
from typing import Literal

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Query
from fastapi import status

from snapper.api.schemas.ai_review_aftermath import AiReviewAftermathResponse
from snapper.api.schemas.base import PayloadRequest
from snapper.api.schemas.base import StrictBody
from snapper.application.ai_review.service import ERROR_DECISION_ALREADY_RECORDED
from snapper.application.ai_review.service import ERROR_NOT_AUTHORIZED
from snapper.application.ai_review.service import ERROR_PEER_RESOLVED
from snapper.application.ai_review.service import ERROR_REVIEW_EXPIRED
from snapper.application.ai_review.service import ERROR_REVIEW_NOT_FOUND
from snapper.application.ai_review.service import get_ai_review_service
from snapper.auth.dependencies import require_permission
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.permissions import has_effective_permission
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.scope_grant_service import get_scope_grant_service
from snapper.core.json_types import JsonObject
from snapper.core.types import AiReviewDecisionEnum
from snapper.core.types import AiReviewStatusEnum
from snapper.data.repository import Repository
from snapper.server.dependencies import get_repository_dependency
from snapper.server.json_body import json_body
from snapper.server.json_body import openapi_schema

router = APIRouter(prefix="/ai-reviews", tags=["ai-reviews"])

_NON_DELEGATE_REQUEST = (
    "ai-reviews endpoints require an AI_DELEGATE principal "
    "(populated AuthPrincipal.delegate_public_id)."
)
_TERMINAL_AI_REVIEW_STATUSES = frozenset(
    {
        AiReviewStatusEnum.RESOLVED_APPROVED.value,
        AiReviewStatusEnum.RESOLVED_REJECTED.value,
        AiReviewStatusEnum.TIMEOUT.value,
        AiReviewStatusEnum.SUPERSEDED.value,
    }
)


_HTTP_STATUS_BY_ERROR_CODE: dict[str, int] = {
    ERROR_REVIEW_NOT_FOUND: status.HTTP_404_NOT_FOUND,
    ERROR_NOT_AUTHORIZED: status.HTTP_403_FORBIDDEN,
    ERROR_PEER_RESOLVED: status.HTTP_409_CONFLICT,
    ERROR_REVIEW_EXPIRED: status.HTTP_410_GONE,
}
"""HTTP status mapping for known submit_decision outcomes.

Successful outcomes (``error_code is None`` AND idempotent retries via
:data:`ERROR_DECISION_ALREADY_RECORDED`) map to 200 outside this dict;
unknown error codes fall through to 503 ("review_state_race" +
defensive default).
"""


class AiReviewDecisionRequest(StrictBody):
    """Body schema for ``POST /api/ai-reviews/{id}/decision``.

    Attributes:
        decision: ``"approve"`` or ``"reject"``. Validated against
            :class:`AiReviewDecisionEnum` server-side; an unknown
            value yields 422 with ``error_code='invalid_decision'``.
        rationale: Optional free-text rationale (≤4096 chars per the
            ``ai_reviews.rationale`` column constraint). Persisted on
            the row AND on the ``decision_recorded`` audit-event
            payload.
    """

    decision: str
    rationale: str | None = None


class AiReviewDecisionCommand(
    PayloadRequest[Literal["ai_review_decision_command"], AiReviewDecisionRequest],
):
    """Request envelope for ``POST /api/ai-reviews/{id}/decision``.

    Brings the AI-review decision REST surface in line with every
    other mutating REST endpoint (orders, brackets, trailing stops,
    backtests, wallets, scope grants, credentials): the client stamps
    a provenance envelope (``public_id``, ``session_id``,
    ``sequence_id``, ``timestamp``) around the inner
    :class:`AiReviewDecisionRequest` payload. The MCP tool surface
    keeps its flat-args convention because FastMCP owns the JSON-RPC
    framing and provenance is minted server-side from JWT claims +
    ``event_metadata`` when the audit event hits the bus.
    """

    type: Literal["ai_review_decision_command"] = "ai_review_decision_command"


class AiReviewDecisionResponse(StrictBody):
    """Canonical envelope wrapped in a JSON body for the REST surface.

    Mirrors the MCP tool's :class:`mcp.types.CallToolResult` JSON
    payload one-to-one so consumers can switch transports without
    re-shaping.
    """

    success: bool
    error_code: str | None
    message: str
    details: JsonObject


class PendingReviewSummaryItem(StrictBody):
    """Per-row shape returned by ``GET /api/ai-reviews/pending``.

    Carries the resolved ``instrument`` ticker and the raw
    ``signal_envelope`` payload (``thesis``, ``side``, news anchors)
    so the AI delegate inbox can render a meaningful row without a
    follow-up detail read.
    """

    review_public_id: str
    selected_delegate_public_id: str
    wallet_public_id: str
    dispatch_version: int
    status: str
    deadline: datetime
    fanout_after: datetime
    instrument: str | None = None
    signal_envelope: JsonObject | None = None


class PendingReviewListResponse(StrictBody):
    """Top-level envelope for ``GET /api/ai-reviews/pending`` payloads."""

    items: list[PendingReviewSummaryItem]
    count: int


class AdminAiReviewItem(StrictBody):
    """Per-row shape returned by ``GET /api/ai-reviews`` (operator audit).

    Carries the full state-machine outcome — ``status``, ``decision``,
    ``rationale``, ``resolution_mode`` and the responding delegate — so
    an authorized reader can see WHAT the AI decided and WHY, plus the raw
    ``signal_envelope`` (thesis / side / news anchors) for context. This
    is the read-only, non-delegate-scoped counterpart to the pending
    inbox.
    """

    review_public_id: str
    strategy_public_id: str
    user_public_id: str
    operator_public_id: str
    wallet_public_id: str
    instrument_public_id: str
    selected_delegate_public_id: str
    responding_delegate_public_id: str | None
    status: str
    decision: str | None
    rationale: str | None
    resolution_mode: str | None
    dispatch_version: int
    created_at: datetime
    resolved_at: datetime | None
    deadline: datetime
    signal_envelope: JsonObject | None = None


class AdminAiReviewListResponse(StrictBody):
    """Top-level envelope for ``GET /api/ai-reviews`` payloads."""

    items: list[AdminAiReviewItem]
    count: int


def _build_envelope(
    *,
    success: bool,
    error_code: str | None,
    message: str,
    details: JsonObject,
) -> AiReviewDecisionResponse:
    """Construct the canonical envelope shared with the MCP surface."""
    return AiReviewDecisionResponse(
        success=success, error_code=error_code, message=message, details=details
    )


@router.post(
    "/{review_public_id}/decision",
    openapi_extra=openapi_schema(AiReviewDecisionCommand),
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Review not found"},
        status.HTTP_403_FORBIDDEN: {"description": "Caller not authorized"},
        status.HTTP_409_CONFLICT: {"description": "Already resolved by peer"},
        status.HTTP_410_GONE: {"description": "Deadline elapsed"},
        status.HTTP_422_UNPROCESSABLE_CONTENT: {"description": "Invalid decision"},
    },
)
async def submit_ai_review_decision_route(
    review_public_id: str,
    command: Annotated[AiReviewDecisionCommand, Depends(json_body(AiReviewDecisionCommand))],
    principal: Annotated[AuthPrincipal, Depends(require_permission(Permission.CREATE_ORDERS))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    _csrf: Annotated[None, Depends(validate_csrf_token)] = None,
) -> AiReviewDecisionResponse:
    """REST mirror of the ``submit_ai_review_decision`` MCP tool.

    Validates the decision string, forwards to
    :meth:`AiReviewService.submit_decision`, and translates the
    canonical result envelope into HTTP status + JSON body.

    Args:
        review_public_id: UUID7 of the ``ai_reviews`` row.
        command: :class:`AiReviewDecisionCommand` envelope wrapping
            the inner :class:`AiReviewDecisionRequest` (decision +
            optional rationale).
        principal: Authenticated caller (must hold
            :data:`Permission.CREATE_ORDERS`).
        repo: Repository handle.

    Returns:
        :class:`AiReviewDecisionResponse` envelope. Idempotent retries
        return 200 with ``success=True`` AND
        ``error_code='decision_already_recorded'``.

    Raises:
        HTTPException: 404 / 403 / 409 / 410 / 422 / 503 mapped from
            the service's ``error_code``.
    """
    body = command.payload
    try:
        decision_enum = AiReviewDecisionEnum(body.decision)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={
                "success": False,
                "error_code": "invalid_decision",
                "message": (
                    "decision must be 'approve' or 'reject'; got an "
                    "unrecognised value (see details.decision)."
                ),
                "details": {"decision": body.decision},
            },
        ) from exc
    result = await get_ai_review_service().submit_decision(
        review_public_id=review_public_id,
        caller_user_public_id=principal.user_public_id,
        decision=decision_enum,
        rationale=body.rationale,
        repo=repo,
        scope_grant_service=get_scope_grant_service(),
    )
    details: JsonObject = dict(result.details)
    if result.status is not None:
        details["status"] = result.status.value
    if result.resolution_mode is not None:
        details["resolution_mode"] = result.resolution_mode.value
    if result.dispatch_version is not None:
        details["dispatch_version"] = result.dispatch_version
    idempotent_retry = result.error_code == ERROR_DECISION_ALREADY_RECORDED
    success = result.error_code is None or idempotent_retry
    if success:
        return _build_envelope(
            success=True,
            error_code=result.error_code,
            message=result.message,
            details=details,
        )
    http_status = _HTTP_STATUS_BY_ERROR_CODE.get(
        result.error_code or "", status.HTTP_503_SERVICE_UNAVAILABLE
    )
    raise HTTPException(
        status_code=http_status,
        detail={
            "success": False,
            "error_code": result.error_code,
            "message": result.message,
            "details": details,
        },
    )


@router.get(
    "/pending",
    responses={
        status.HTTP_422_UNPROCESSABLE_CONTENT: {
            "description": "Caller is not an AI delegate principal"
        },
    },
)
async def list_pending_ai_reviews(
    principal: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_SIGNALS))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    wallet_public_id: Annotated[str | None, Query()] = None,
) -> PendingReviewListResponse:
    """List pending CONSULT reviews where the caller is the selected delegate.

    Used by the bridge (catch-up after WS reconnect) and
    by operator dashboards. Snapshot is bounded by ``limit`` and
    ordered by ``fanout_after ASC`` (oldest first).

    The ``fanout_after < now`` gate uses ``datetime.now(UTC)`` because
    this REST poll is the catch-up surface — the live wall-clock is
    the right reference for "what should the bridge re-acknowledge
    right now". The fast-path bus subscriber uses
    ``msg.last_seen_at`` for a different reason: it dispatches
    fanout, while THIS endpoint only lists what is currently fan-
    eligible.

    Args:
        principal: Authenticated AI_DELEGATE caller. Non-delegate
            principals — even those holding
            :data:`Permission.READ_SIGNALS` — get a 422 because the
            endpoint is keyed by the delegate identity.
        repo: Repository handle.
        limit: Max rows returned (clamped to ``[1, 500]``).
        wallet_public_id: Optional filter — narrows the snapshot to
            one wallet. Bridge passes this when it wants
            catch-up scoped to the wallet currently surfaced in the
            UI; ``None`` returns every wallet the delegate is
            assigned to.

    Returns:
        :class:`PendingReviewListResponse` with up to ``limit`` items.
    """
    if principal.delegate_public_id is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={
                "success": False,
                "error_code": "not_a_delegate",
                "message": _NON_DELEGATE_REQUEST,
                "details": {},
            },
        )
    rows = await repo.list_pending_reviews_for_delegate(
        selected_delegate_public_id=principal.delegate_public_id,
        now=datetime.now(UTC),
        wallet_public_id=wallet_public_id,
        limit=limit,
    )
    items = [
        PendingReviewSummaryItem(
            review_public_id=row["public_id"],
            selected_delegate_public_id=row["selected_delegate_public_id"],
            dispatch_version=row["dispatch_version"],
            status=row["status"],
            deadline=row["deadline"],
            fanout_after=row["fanout_after"],
            wallet_public_id=row["wallet_public_id"],
            instrument=row.get("instrument"),
            signal_envelope=row.get("signal_envelope"),
        )
        for row in rows
    ]
    return PendingReviewListResponse(items=items, count=len(items))


@router.get(
    "/{review_public_id}/aftermath",
    responses={
        status.HTTP_404_NOT_FOUND: {
            "description": "Review unknown or outside the delegate's active scope"
        },
        status.HTTP_409_CONFLICT: {"description": "Review is not terminal"},
        status.HTTP_422_UNPROCESSABLE_CONTENT: {
            "description": "Caller is not an AI delegate principal"
        },
    },
)
async def get_ai_review_aftermath_route(
    review_public_id: str,
    principal: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_SIGNALS))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> AiReviewAftermathResponse:
    """Return a terminal review's read-only exact-scope aftermath.

    The route captures one UTC temporal anchor, revalidates the caller's
    active delegate grant for the review's wallet and instrument, rejects
    non-terminal rows, and then returns the repository projection. Unknown
    and out-of-scope identifiers share one response to prevent enumeration.

    Args:
        review_public_id: Public identifier of the terminal review.
        principal: Authenticated AI delegate holding ``READ_SIGNALS``.
        repo: Repository used for scope checks and temporal reads.

    Returns:
        Full terminal review, inclusive window bounds, activity rows, cycle
        transitions, and current position snapshots.

    Raises:
        HTTPException: If the caller is not a delegate, the review is absent
            or out of scope, the review is non-terminal, or it disappears
            before the projection read.
    """
    if principal.delegate_public_id is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={
                "success": False,
                "error_code": "not_a_delegate",
                "message": _NON_DELEGATE_REQUEST,
                "details": {},
            },
        )
    as_of = datetime.now(UTC)
    review = await repo.get_ai_review(review_public_id)
    if review is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "success": False,
                "error_code": ERROR_REVIEW_NOT_FOUND,
                "message": "No terminal review with that id was found in the caller's scope.",
                "details": {"review_public_id": review_public_id},
            },
        )
    scope_ok = await get_scope_grant_service().has_grant_for_delegate(
        delegate_public_id=principal.delegate_public_id,
        wallet_public_id=review["wallet_public_id"],
        instrument_public_id=review["instrument_public_id"],
        as_of=as_of,
    )
    if not scope_ok:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "success": False,
                "error_code": ERROR_REVIEW_NOT_FOUND,
                "message": "No terminal review with that id was found in the caller's scope.",
                "details": {"review_public_id": review_public_id},
            },
        )
    if review["status"] not in _TERMINAL_AI_REVIEW_STATUSES:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "success": False,
                "error_code": "review_not_terminal",
                "message": "AI review aftermath is available only after terminal resolution.",
                "details": {
                    "review_public_id": review_public_id,
                    "status": review["status"],
                },
            },
        )
    aftermath = await repo.get_ai_review_aftermath(review_public_id, as_of=as_of)
    if aftermath is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "success": False,
                "error_code": ERROR_REVIEW_NOT_FOUND,
                "message": "No terminal review with that id was found in the caller's scope.",
                "details": {"review_public_id": review_public_id},
            },
        )
    return AiReviewAftermathResponse.model_validate(aftermath)


@router.get(
    "",
    responses={
        status.HTTP_403_FORBIDDEN: {"description": "Caller lacks AI-review read access"},
    },
)
async def list_ai_reviews_route(
    principal: Annotated[
        AuthPrincipal,
        Depends(require_permission(Permission.READ_AI_REVIEWS)),
    ],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    status_filter: Annotated[str | None, Query(alias="status")] = None,
    wallet_public_id: Annotated[str | None, Query()] = None,
    strategy_public_id: Annotated[str | None, Query()] = None,
) -> AdminAiReviewListResponse:
    """List AI reviews newest-first for permitted observability clients.

    Read-only audit surface answering "what did the AI decide?". Gated
    by ``READ_AI_REVIEWS``. Unlike ``/pending`` it is NOT keyed
    by ``AuthPrincipal.delegate_public_id`` (so it does not 422 for a
    non-delegate) and it returns terminal decided rows, not only pending
    ones. Optional exact-match query filters narrow the snapshot.

    Scope: a principal granted ``IMPERSONATE_OPERATOR`` sees every
    operator's reviews; every other reader is narrowed server-side to
    the operators it is a member of
    (``AuthPrincipal.operator_public_ids``) so it cannot read another
    book's reviews. This mirrors membership narrowing on the other
    list surfaces (scope grants, orders/alerts scope filters).

    Args:
        principal: Authenticated caller (drives scoping).
        repo: Repository handle.
        limit: Max rows returned (clamped to ``[1, 500]``).
        status_filter: Optional ``status`` filter (query alias
            ``status``; the Python name avoids shadowing
            :mod:`fastapi.status`).
        wallet_public_id: Optional wallet filter.
        strategy_public_id: Optional strategy filter.

    Returns:
        :class:`AdminAiReviewListResponse` with up to ``limit`` items,
        newest first.
    """
    operator_scope = (
        None
        if has_effective_permission(
            principal.role,
            principal.permissions,
            principal.permission_scope_version,
            Permission.IMPERSONATE_OPERATOR,
        )
        else principal.operator_public_ids
    )
    rows = await repo.list_ai_reviews(
        limit=limit,
        status=status_filter,
        wallet_public_id=wallet_public_id,
        strategy_public_id=strategy_public_id,
        operator_public_ids=operator_scope,
    )
    items = [
        AdminAiReviewItem(
            review_public_id=row["public_id"],
            strategy_public_id=row["strategy_public_id"],
            user_public_id=row["user_public_id"],
            operator_public_id=row["operator_public_id"],
            wallet_public_id=row["wallet_public_id"],
            instrument_public_id=row["instrument_public_id"],
            selected_delegate_public_id=row["selected_delegate_public_id"],
            responding_delegate_public_id=row["responding_delegate_public_id"],
            status=row["status"],
            decision=row["decision"],
            rationale=row["rationale"],
            resolution_mode=row["resolution_mode"],
            dispatch_version=row["dispatch_version"],
            created_at=row["created_at"],
            resolved_at=row["resolved_at"],
            deadline=row["deadline"],
            signal_envelope=row["signal_envelope"],
        )
        for row in rows
    ]
    return AdminAiReviewListResponse(items=items, count=len(items))


__all__: list[Any] = ["router"]
