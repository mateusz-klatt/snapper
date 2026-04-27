"""Validation for caller-supplied ``ai_review_public_id`` citations on manual orders.

Plan D Phase 2 #10 R1 — closes the unauthorized citation gap on the
manual-order entry points (MCP ``submit_manual_order`` + REST
``POST /api/orders``). Both endpoints accept an optional
``ai_review_public_id`` body field that, when set, threads onto the
:class:`TradeCommandSubmission` so a caps rejection inside
:meth:`TradingCapsEnforcer.guard` can re-fanout the rejection to the
AI delegate's UI via ``bus.caps_violation_after_ai_approve``.

Without authorization, any authenticated caller could supply an
arbitrary ``ai_review_public_id`` to trigger fanout to other
delegates' UIs (info leak + fanout spam). The validator enforces:

1. The cited row exists.
2. ``review.user_public_id`` matches the caller's identity.
3. ``review.wallet_public_id`` matches the submission's wallet
   (the caller already passed wallet scope check upstream; this
   enforces the cross-link).
4. ``review.status == "resolved_approved"`` — only AI-approved
   reviews may legitimately authorize a manual order. A
   ``pending`` / ``resolved_rejected`` / ``timeout`` / ``superseded``
   row cannot.
"""

from snapper.data.repository import Repository


class AiReviewCitationError(ValueError):
    """Raised when a caller-supplied ``ai_review_public_id`` fails validation.

    Surfaced as HTTP 403 by the REST manual-order handler and as a
    structured error envelope by the MCP ``submit_manual_order`` tool.
    The error message identifies the failing predicate so audit logs
    can distinguish "owner mismatch" from "wallet mismatch" from
    "wrong status" without re-fetching the row.
    """


async def validate_ai_review_citation(
    repo: Repository,
    *,
    ai_review_public_id: str,
    expected_user_public_id: str,
    expected_wallet_public_id: str,
) -> None:
    """Verify the caller may cite ``ai_review_public_id`` on a manual order.

    Args:
        repo: Repository handle used for the row fetch.
        ai_review_public_id: Caller-supplied UUID7 of the
            ``ai_reviews`` row.
        expected_user_public_id: ``user_public_id`` from the
            authenticated caller's claims.
        expected_wallet_public_id: ``wallet_public_id`` from the
            manual-order submission.

    Raises:
        AiReviewCitationError: When any of the four invariants
            fails (row missing / owner mismatch / wallet mismatch /
            non-approved status).
    """
    review = await repo.get_ai_review(ai_review_public_id)
    if review is None:
        raise AiReviewCitationError(f"ai_review_public_id={ai_review_public_id!r} not found")
    if review["user_public_id"] != expected_user_public_id:
        raise AiReviewCitationError(
            f"ai_review_public_id={ai_review_public_id!r} owner mismatch "
            f"(review.user_public_id != caller.user_public_id)"
        )
    if review["wallet_public_id"] != expected_wallet_public_id:
        raise AiReviewCitationError(
            f"ai_review_public_id={ai_review_public_id!r} wallet mismatch "
            f"(review.wallet_public_id={review['wallet_public_id']!r} "
            f"vs submission wallet={expected_wallet_public_id!r})"
        )
    if review["status"] != "resolved_approved":
        raise AiReviewCitationError(
            f"ai_review_public_id={ai_review_public_id!r} status="
            f"{review['status']!r} cannot authorize a manual order"
        )
