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

Permission gates
    ``POST .../decision`` requires :data:`Permission.CREATE_ORDERS` —
      matches the MCP tool. This REST surface deliberately follows
      the MCP precedent so a single delegate JWT works both transports
      without per-request role gymnastics.
    ``GET .../pending`` requires :data:`Permission.READ_SIGNALS` —
      AI_DELEGATE inherits this; non-delegate principals are admitted
      by the role check but get a 422 because the endpoint is keyed
      by ``AuthPrincipal.delegate_public_id``.
"""

from datetime import UTC
from datetime import datetime
from typing import Annotated
from typing import Any

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Query
from fastapi import status

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
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.scope_grant_service import get_scope_grant_service
from snapper.core.json_types import JsonObject
from snapper.core.types import AiReviewDecisionEnum
from snapper.data.repository import Repository
from snapper.server.dependencies import get_repository_dependency
from snapper.server.json_body import json_body
from snapper.server.json_body import openapi_schema

router = APIRouter(prefix="/ai-reviews", tags=["ai-reviews"])

_NON_DELEGATE_REQUEST = (
    "ai-reviews endpoints require an AI_DELEGATE principal "
    "(populated AuthPrincipal.delegate_public_id)."
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
    """Per-row shape returned by ``GET /api/ai-reviews/pending``."""

    review_public_id: str
    selected_delegate_public_id: str
    wallet_public_id: str
    dispatch_version: int
    status: str
    deadline: datetime
    fanout_after: datetime


class PendingReviewListResponse(StrictBody):
    """Top-level envelope for ``GET /api/ai-reviews/pending`` payloads."""

    items: list[PendingReviewSummaryItem]
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
    openapi_extra=openapi_schema(AiReviewDecisionRequest),
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
    body: Annotated[AiReviewDecisionRequest, Depends(json_body(AiReviewDecisionRequest))],
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
        body: :class:`AiReviewDecisionRequest` with decision +
            optional rationale.
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
        )
        for row in rows
    ]
    return PendingReviewListResponse(items=items, count=len(items))


__all__: list[Any] = ["router"]
