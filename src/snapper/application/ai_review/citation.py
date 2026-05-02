"""Validation for caller-supplied ``ai_review_public_id`` citations on manual orders.

Closes the unauthorized citation gap on the manual-order entry points
(MCP ``submit_manual_order`` + REST ``POST /api/orders``). Both endpoints accept an optional
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
from snapper.data.repository_types import AiReviewRow


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


async def validate_ai_review_citation_for_strategy(
    repo: Repository,
    *,
    ai_review_public_id: str,
    expected_wallet_public_id: str,
) -> AiReviewRow:
    """Strategy-path citation validator.

    Returns the validated ``ai_reviews`` row so the caller (the
    strategy attribution guard) can read ``user_public_id`` without
    a duplicate fetch. NOT a security gate: the strategy primitive
    is the gatekeeper because the strategy can only know a
    ``review_public_id`` if its own ``create_ai_review_and_await``
    call returned one — there is no input-attack vector across the
    strategy process boundary.

    The validator pins three invariants useful in the hot-path:

    1. Row exists.
    2. ``row.status == "resolved_approved"`` — catches a
       supersede-after-await race where the row has been reaped or
       otherwise transitioned away from the APPROVED state by the
       reaper / scanner between the strategy's
       ``await create_ai_review_and_await`` and its emit call.
    3. ``row.wallet_public_id == expected_wallet_public_id`` —
       defends against engine misrouting (the engine instance
       carries a wallet on init; the cited row must agree).

    The companion ``ai_review_dispatch_version`` carried on the
    submission is **transport-only**: the validator deliberately does
    NOT compare it. Dispatch-version dedup happens at the bus
    publisher (`_publish_caps_violation_after_ai_approve` reads
    ``dispatch_version`` from the cited row at publish time so the
    fanout always uses the row-of-record version).

    Args:
        repo: Repository handle for the row fetch.
        ai_review_public_id: UUID7 of the ``ai_reviews`` row that the
            strategy is citing.
        expected_wallet_public_id: ``wallet_public_id`` from the
            engine's strategy submission. Must be non-empty; the
            engine attribution guard fails closed beforehand if the
            wallet is empty.

    Returns:
        The validated :class:`AiReviewRow` so callers can read
        ``user_public_id`` and other fields without a re-fetch.

    Raises:
        AiReviewCitationError: When any of the three invariants
            fails (row missing / non-approved status / wallet
            mismatch). Each failure carries a predicate-specific
            message suffix so audit logs can distinguish the cause.
    """
    review = await repo.get_ai_review(ai_review_public_id)
    if review is None:
        raise AiReviewCitationError(f"ai_review_public_id={ai_review_public_id!r} not found")
    if review["status"] != "resolved_approved":
        raise AiReviewCitationError(
            f"ai_review_public_id={ai_review_public_id!r} status="
            f"{review['status']!r} cannot authorize a strategy emit"
        )
    if review["wallet_public_id"] != expected_wallet_public_id:
        raise AiReviewCitationError(
            f"ai_review_public_id={ai_review_public_id!r} wallet mismatch "
            f"(review.wallet_public_id={review['wallet_public_id']!r} "
            f"vs submission wallet={expected_wallet_public_id!r})"
        )
    return review
