"""Scope revocation + user-deactivation handlers for the notify sidecar (§D8).

The sidecar subscribes to ``admin.scope_revoked`` + ``admin.user_deactivated``
on startup. When either fires, ``ScopeRevalidator`` cancels the pending
deliveries that would have shipped out under the revoked scope, and
primes its in-memory "stale scope" cache so the next per-delivery
send-time check can fast-path the DB lookup for non-safety-critical
alerts.

Send-time revalidation (``should_skip_send``) runs *before* the APNs
call in the sidecar's ``_attempt_once``. Policy:

- **Safety-critical alerts**: always re-run ``is_scope_grant_active``
  — no cache shortcut. The round-trip is cheap and the alerting cost
  of paging someone whose scope was just revoked is worse than a
  small extra DB read.
- **Non-safety-critical alerts**: use the in-memory cache of
  ``(operator_public_id, wallet_public_id)`` wake-up timestamps.
  If the cache flags the scope as stale within the TTL, re-validate
  against the DB. Cache misses skip the DB call entirely — the trade-
  off is bounded staleness (at most one sidecar tick of delay after a
  scope_revoked event for non-critical alerts).

The cache is an in-memory dict on the revalidator instance; it does
NOT persist across sidecar restart. A fresh process loads the current
DB state on its first send attempt per scope, which is correct.
"""

from datetime import datetime
from datetime import timedelta

from loguru import logger

from snapper.data.repository import Repository
from snapper.data.repository_types import AlertEventRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.data import ScopeRevokedData
from snapper.messaging.schemas.data import UserDeactivatedData

_STALE_SCOPE_TTL = timedelta(seconds=60)
_ZMQ_STREAM = "sidecar.scope_revalidation"


class ScopeRevalidator:
    """Handles ``admin.scope_revoked`` / ``admin.user_deactivated`` + send-time checks."""

    def __init__(self, tracker: SequenceTracker) -> None:
        """Start with an empty stale-scope cache."""
        self._tracker = tracker
        self._stale_scope_cache: dict[tuple[str, str], datetime] = {}

    async def handle_scope_revoked(
        self,
        payload: bytes,
        repo: Repository,
        now: datetime,
    ) -> None:
        """React to an ``admin.scope_revoked`` event.

        Primes the stale-scope cache with the ``(operator, wallet)``
        key and cancels all queued deliveries for the revoked triple
        across every affected user (those holding membership in the
        revoked operator).

        Args:
            payload: Raw JSON bytes of the bus event.
            repo: Repository handle for cancellation + user lookup.
            now: Entry-boundary timestamp.
        """
        try:
            data = ScopeRevokedData.model_validate_json(payload)
        except Exception as exc:
            logger.warning(
                "scope_revalidation: drop malformed admin.scope_revoked payload: {err}",
                err=exc,
            )
            return
        scope_key = (data.operator_public_id, data.wallet_public_id)
        self._stale_scope_cache[scope_key] = now
        affected_users = await repo.list_users_with_operator_membership(
            operator_public_id=data.operator_public_id,
            as_of=now,
        )
        for user_public_id in affected_users:
            sid = self._tracker.session_id
            seq = self._tracker.next_sequence(_ZMQ_STREAM)
            cancelled = await repo.cancel_pending_deliveries_for_scope(
                user_public_id=user_public_id,
                operator_public_id=data.operator_public_id,
                wallet_public_id=data.wallet_public_id,
                transition_at=now,
                session_id=sid,
                sequence_id=seq,
            )
            if cancelled:
                logger.info(
                    "scope_revalidation: cancelled {n} pending deliveries for"
                    " user={user} operator={op} wallet={wal}",
                    n=cancelled,
                    user=user_public_id,
                    op=data.operator_public_id,
                    wal=data.wallet_public_id,
                )

    async def handle_user_deactivated(
        self,
        payload: bytes,
        repo: Repository,
        now: datetime,
    ) -> None:
        """React to an ``admin.user_deactivated`` event.

        Bulk-cancels every pending delivery for the deactivated user;
        there is no surviving surface for them to receive alerts on.

        Args:
            payload: Raw JSON bytes of the bus event.
            repo: Repository handle for cancellation.
            now: Entry-boundary timestamp.
        """
        try:
            data = UserDeactivatedData.model_validate_json(payload)
        except Exception as exc:
            logger.warning(
                "scope_revalidation: drop malformed admin.user_deactivated payload: {err}",
                err=exc,
            )
            return
        sid = self._tracker.session_id
        seq = self._tracker.next_sequence(_ZMQ_STREAM)
        cancelled = await repo.cancel_pending_deliveries_for_user(
            user_public_id=data.user_public_id,
            transition_at=now,
            session_id=sid,
            sequence_id=seq,
        )
        if cancelled:
            logger.info(
                "scope_revalidation: cancelled {n} pending deliveries for deactivated user={user}",
                n=cancelled,
                user=data.user_public_id,
            )

    async def should_skip_send(
        self,
        alert: AlertEventRow,
        repo: Repository,
        now: datetime,
    ) -> bool:
        """Return True when the delivery should be cancelled before the APNs call.

        Safety-critical alerts always re-check the grant. Non-critical
        alerts consult the stale-scope cache first — only paying the
        DB round-trip when the cache signals a recent revoke for the
        alert's ``(operator, wallet)`` pair.

        Args:
            alert: Persisted ``AlertEventRow`` about to be sent.
            repo: Repository handle for ``is_scope_grant_active``.
            now: Entry-boundary timestamp threaded from the sidecar.

        Returns:
            True when the pending delivery should be cancelled
            (scope is no longer active); False when it should
            proceed to the APNs send.
        """
        operator = alert.get("operator_public_id")
        wallet = alert.get("wallet_public_id")
        if operator is None or wallet is None:
            return False
        scope_key = (operator, wallet)
        if alert["is_safety_critical"]:
            return not await repo.is_scope_grant_active(
                user_public_id=alert["user_public_id"],
                operator_public_id=operator,
                wallet_public_id=wallet,
                as_of=now,
            )
        cached_at = self._stale_scope_cache.get(scope_key)
        if cached_at is None or now - cached_at > _STALE_SCOPE_TTL:
            return False
        still_active = await repo.is_scope_grant_active(
            user_public_id=alert["user_public_id"],
            operator_public_id=operator,
            wallet_public_id=wallet,
            as_of=now,
        )
        return not still_active
