"""Plan D §8 strategy-side await primitive for CONSULT reviews.

This module owns the single async entry point strategies invoke to
issue + await an AI-delegate CONSULT round:

.. code-block:: python

    decision = await create_ai_review_and_await(
        request=AiReviewCreateRequest(...),
        repo=ctx.repo,
        deadline_seconds=30,
    )
    if decision.status is AiReviewStatusEnum.RESOLVED_APPROVED:
        ...  # proceed with trade
    elif decision.status is AiReviewStatusEnum.RESOLVED_REJECTED:
        ...  # VETO per Plan A Q12 — abort, no trade
    else:
        ...  # timeout / superseded -> fall through to non-AI decision

The primitive composes :meth:`AiReviewService.create_review` with the
Plan A §7.1 await loop:

- **Fast path (deferred wiring)** — :meth:`AiReviewService.register_future`
  registers an :class:`asyncio.Future` keyed on ``review_public_id``.
  When the ``bus.ai_review_decision`` listener fires for the row it
  resolves the future via the singleton's ``_futures`` registry and
  the await wakes immediately. The ZMQ subscriber loop that drives
  this is a separate chunk; until it lands the future stays unset
  and the slow path is the primary completion signal.
- **Slow path** — DB poll every ``poll_min_seconds`` to
  ``poll_max_seconds`` (jittered 3-7s default per Plan A §7.1).
  ``Repository.get_ai_review`` returns the row; once status reaches
  one of the terminal sentinels the loop exits with
  :class:`AiReviewDecisionOutcome`.
- **Inline timeout** — when wall-clock crosses the strategy's
  deadline AND the row is still non-terminal, the primitive calls
  :meth:`AiReviewService.timeout_review` to transition the row +
  emit the audit event + decrement the delegate counter, then reads
  the freshly-terminal row back for the outcome.

The primitive deliberately lives outside :class:`BaseStrategy`:
strategies are ZMQ-only processes without a Repository handle in the
inheritance chain, so dependency injection (caller passes ``repo``)
is the cleanest contract. Tests exercise the primitive directly with
a real SQLite repository, no strategy fixture needed.
"""

import asyncio
import random
from datetime import UTC
from datetime import datetime
from datetime import timedelta

from snapper.application.ai_review.service import _TERMINAL_STATUSES
from snapper.application.ai_review.service import AiReviewCreated
from snapper.application.ai_review.service import AiReviewCreateRequest
from snapper.application.ai_review.service import AiReviewDecisionOutcome
from snapper.application.ai_review.service import AiReviewService
from snapper.application.ai_review.service import get_ai_review_service
from snapper.core.types import AiReviewDecisionEnum
from snapper.core.types import AiReviewResolutionModeEnum
from snapper.core.types import AiReviewStatusEnum
from snapper.data.repository import Repository
from snapper.data.repository_types import AiReviewRow

DEFAULT_POLL_MIN_SECONDS = 3.0
DEFAULT_POLL_MAX_SECONDS = 7.0
"""Plan A §7.1 jittered DB poll window for the slow path.

3-7s keeps DB load bounded under fleet-wide CONSULT bursts while
still bounding the worst-case strategy wake latency to one window
beyond the fast-path bus listener.
"""


def _outcome_from_row(row: AiReviewRow) -> AiReviewDecisionOutcome:
    """Project a terminal ``ai_reviews`` row onto the outcome dataclass."""
    decision_value = row["decision"]
    decision = AiReviewDecisionEnum(decision_value) if decision_value is not None else None
    resolution_value = row["resolution_mode"]
    resolution_mode = (
        AiReviewResolutionModeEnum(resolution_value) if resolution_value is not None else None
    )
    return AiReviewDecisionOutcome(
        review_public_id=row["public_id"],
        status=AiReviewStatusEnum(row["status"]),
        resolution_mode=resolution_mode,
        decision=decision,
        rationale=row["rationale"],
        dispatch_version=int(row["dispatch_version"]),
        responding_delegate_public_id=row["responding_delegate_public_id"],
    )


async def create_ai_review_and_await(
    request: AiReviewCreateRequest,
    *,
    repo: Repository,
    deadline_seconds: int = 30,
    ai_service: AiReviewService | None = None,
    poll_min_seconds: float = DEFAULT_POLL_MIN_SECONDS,
    poll_max_seconds: float = DEFAULT_POLL_MAX_SECONDS,
) -> AiReviewDecisionOutcome:
    """Plan D §8 — create + await a CONSULT review end-to-end.

    Composes :meth:`AiReviewService.create_review` with the Plan A
    §7.1 await loop (DB poll + Future registration + inline timeout).
    On a deadline crossing with the row still non-terminal the
    primitive transitions the row to ``timeout`` itself so the
    strategy never sees a hung await.

    Args:
        request: :class:`AiReviewCreateRequest` carrying the scope ids
            + signal envelope + instrument metadata + bus origin.
        repo: Repository handle. Strategies inject the same repo
            their context already holds — the primitive lives outside
            :class:`BaseStrategy` because the strategy class has no
            DB handle in its inheritance chain.
        deadline_seconds: Override for ``request.deadline_seconds`` —
            kept here for symmetry with the Plan D §8 spec signature
            (the request also carries deadline_seconds). The primitive
            uses this value for both the row's deadline AND the
            local await loop's wall-clock cutoff.
        ai_service: Optional :class:`AiReviewService` injection point;
            defaults to the process-wide singleton.
        poll_min_seconds: Lower bound of the jittered DB-poll
            interval. Plan A §7.1 default 3s.
        poll_max_seconds: Upper bound of the jittered DB-poll
            interval. Plan A §7.1 default 7s.

    Returns:
        :class:`AiReviewDecisionOutcome` with the terminal row state.

    Raises:
        NoLiveDelegateError: No eligible AI delegate is live in the
            heartbeat window. Strategy falls through.
        DelegateBusyError: All eligible delegates have an in-flight
            review. Strategy falls through.
        SignalEnvelopeTooLargeError: Canonical-JSON > 16KB cap.
        ValueError: Non-finite floats in the envelope (Plan A §3
            risk-register guard).
    """
    service = ai_service if ai_service is not None else get_ai_review_service()
    request_for_creation = (
        request
        if request.deadline_seconds == deadline_seconds
        else _override_deadline(request, deadline_seconds)
    )
    creation: AiReviewCreated = await service.create_review(request_for_creation, repo=repo)
    review_public_id = creation.review_public_id
    deadline = datetime.now(UTC) + timedelta(seconds=deadline_seconds)
    fut: asyncio.Future[object] = asyncio.Future()
    service.register_future(review_public_id, fut)
    try:
        return await _await_terminal_state(
            review_public_id=review_public_id,
            deadline=deadline,
            future=fut,
            service=service,
            repo=repo,
            poll_min_seconds=poll_min_seconds,
            poll_max_seconds=poll_max_seconds,
        )
    finally:
        service.unregister_future(review_public_id)


def _override_deadline(
    request: AiReviewCreateRequest, deadline_seconds: int
) -> AiReviewCreateRequest:
    """Return a request copy with a different ``deadline_seconds``."""
    return AiReviewCreateRequest(
        user_public_id=request.user_public_id,
        operator_public_id=request.operator_public_id,
        wallet_public_id=request.wallet_public_id,
        instrument_public_id=request.instrument_public_id,
        strategy_public_id=request.strategy_public_id,
        signal_envelope=request.signal_envelope,
        instrument_metadata=request.instrument_metadata,
        deadline_seconds=deadline_seconds,
        session_id=request.session_id,
        sequence_id=request.sequence_id,
    )


async def _await_terminal_state(
    *,
    review_public_id: str,
    deadline: datetime,
    future: asyncio.Future[object],
    service: AiReviewService,
    repo: Repository,
    poll_min_seconds: float,
    poll_max_seconds: float,
) -> AiReviewDecisionOutcome:
    """Drive the Plan A §7.1 await loop to a terminal outcome.

    Each iteration: read the row, return immediately if terminal;
    else if the deadline has crossed, transition the row to timeout
    inline; else sleep the jittered poll interval (or wake on the
    fast-path future, whichever fires first).
    """
    while True:
        row = await repo.get_ai_review(review_public_id)
        if row is not None and row["status"] in _TERMINAL_STATUSES:
            return _outcome_from_row(row)
        now = datetime.now(UTC)
        if now >= deadline:
            await service.timeout_review(review_public_id=review_public_id, repo=repo, now=now)
            terminal_row = await repo.get_ai_review(review_public_id)
            assert terminal_row is not None, (
                f"ai_reviews row {review_public_id} vanished after timeout CAS — "
                "DB invariant violated, the row was just transitioned to terminal."
            )
            return _outcome_from_row(terminal_row)
        wait_seconds = min(
            random.uniform(poll_min_seconds, poll_max_seconds),
            (deadline - now).total_seconds(),
        )
        try:
            await asyncio.wait_for(asyncio.shield(future), timeout=wait_seconds)
        except TimeoutError:
            continue
