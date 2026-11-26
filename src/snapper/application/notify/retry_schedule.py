"""Shared retry-schedule persistence with stale-timestamp recovery."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime

from loguru import logger

from snapper.data.repository import Repository
from snapper.data.repository_types import AlertDeliveryRetryScheduleWrite


@dataclass(frozen=True, slots=True)
class RetryScheduleUpdate:
    """Parameters for updating a queued delivery's retry backoff schedule."""

    delivery_public_id: str
    attempt_count: int
    next_attempt_at: datetime
    error_reason: str | None
    transition_at: datetime


async def persist_retry_schedule(
    repo: Repository,
    update: RetryScheduleUpdate,
    *,
    clock: Callable[[], datetime],
    allocate_write: Callable[[], tuple[str, int]],
) -> bool:
    """Persist one retry schedule, retrying once when a stale timestamp caused refusal.

    When ``update_delivery_retry_schedule`` returns ``False``, re-reads the
    active delivery row to distinguish a stale caller timestamp (such as a local
    clock rollback) from a lost concurrent transition. If the active row is still
    ``queued``, at ``update.attempt_count``, and still unscheduled
    (``next_attempt_at`` is ``None``), retries the schedule write once with
    ``transition_at = max(clock(), active["timestamp"])``. The repository
    requires the selected version to still be unscheduled when applying that
    write. Otherwise the helper logs that the initial refusal was a lost race.

    Args:
        repo: Notification outbox repository.
        update: Target delivery public ID, expected attempt, and schedule fields.
        clock: Callable returning current UTC time for the re-apply attempt.
        allocate_write: Callable allocating ``(session_id, sequence_id)`` per write.

    Returns:
        ``True`` if a schedule version committed; ``False`` otherwise.
    """
    session_id, sequence_id = allocate_write()
    if await repo.update_delivery_retry_schedule(
        update.delivery_public_id,
        attempt_count=update.attempt_count,
        next_attempt_at=update.next_attempt_at,
        error_reason=update.error_reason,
        transition_at=update.transition_at,
        session_id=session_id,
        sequence_id=sequence_id,
    ):
        return True
    active = await repo.get_delivery_by_public_id(update.delivery_public_id)
    if (
        active is not None
        and active["status"] == "queued"
        and active["attempt_count"] == update.attempt_count
        and active.get("next_attempt_at") is None
    ):
        retry_transition_at = max(clock(), active["timestamp"])
        logger.info(
            "notify: reapplying rejected retry schedule after stale timestamp"
            " delivery={pid} attempt={attempt} stale_transition_at={stale}"
            " active_timestamp={active_ts} retry_transition_at={retry_ts}",
            pid=update.delivery_public_id,
            attempt=update.attempt_count,
            stale=update.transition_at.isoformat(),
            active_ts=active["timestamp"].isoformat(),
            retry_ts=retry_transition_at.isoformat(),
        )
        retry_session_id, retry_sequence_id = allocate_write()
        return await repo.reapply_unscheduled_delivery_retry_schedule(
            AlertDeliveryRetryScheduleWrite(
                public_id=update.delivery_public_id,
                attempt_count=update.attempt_count,
                next_attempt_at=update.next_attempt_at,
                error_reason=update.error_reason,
                transition_at=retry_transition_at,
                session_id=retry_session_id,
                sequence_id=retry_sequence_id,
            )
        )
    active_status = "missing" if active is None else active["status"]
    logger.info(
        "notify: skipping rejected retry schedule after lost race"
        " delivery={pid} attempt={attempt} active_status={status}",
        pid=update.delivery_public_id,
        attempt=update.attempt_count,
        status=active_status,
    )
    return False
