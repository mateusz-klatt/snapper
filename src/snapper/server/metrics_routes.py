"""``GET /api/metrics/notifications`` — iOS Push Foundation ops metrics (BE-3c §D11).

Exposes DB-derived outbox counters + per-status totals so an oncall
dashboard can tell at a glance whether the sidecar is keeping up or
whether deliveries are piling up in the retry queue. Gated by
``Permission.READ_SYSTEM_STATUS`` — the same permission the
``critical_system_error`` rule uses for fan-out, so anyone allowed
to see the alert also gets access to the health surface that
explains why the alert did or didn't fire.

Latency percentiles (``apns_p99_latency_ms``) + sidecar heartbeat
(``sidecar_heartbeat_seconds_since``) from the plan wishlist are
deferred — they need cross-process state (the sidecar runs in its
own process under ``snapper notify``). A follow-up plan will wire
those via a small heartbeat + histogram settings table.
"""

import datetime as dt
from datetime import datetime
from typing import Annotated
from typing import Literal
from uuid import uuid7

from fastapi import APIRouter
from fastapi import Depends
from fastapi import Request

from snapper.api.schemas.base import PayloadResponse
from snapper.api.schemas.base import StrictDataSchema
from snapper.auth.dependencies import require_permission
from snapper.auth.domain.permissions import Permission
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository import Repository
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.dependencies import get_repository_dependency

router = APIRouter(prefix="/metrics", tags=["metrics"])

_REST_STREAM = "rest.metrics"


class NotificationMetricsData(StrictDataSchema[Literal["notification_metrics"]]):
    """DB-derived counters for the notify sidecar's outbox (§D11).

    All counts aggregate over active ``alert_deliveries`` rows
    (``known_to = KNOWN_TO_MAX``) — the SCD2 predecessor versions are
    excluded so the numbers reflect the current authoritative state
    of each delivery.

    Attributes:
        delivery_success_total: Deliveries in the ``sent`` terminal state.
        delivery_failed_total: Deliveries in the ``failed`` terminal state
            (exhausted retry budget).
        delivery_410_unregistered_total: Deliveries terminated via an
            APNs 410 ``BadDeviceToken`` response.
        delivery_cancelled_scope_total: Deliveries transitioned to
            ``cancelled_scope`` after an ``admin.scope_revoked`` or
            ``admin.user_deactivated`` event.
        outbox_queued_depth: Number of deliveries still in ``queued``
            status — backlog for the retry loop.
    """

    type: Literal["notification_metrics"] = "notification_metrics"
    delivery_success_total: int
    delivery_failed_total: int
    delivery_410_unregistered_total: int
    delivery_cancelled_scope_total: int
    outbox_queued_depth: int


class NotificationMetricsResponse(
    PayloadResponse[Literal["notification_metrics_response"], NotificationMetricsData]
):
    """Envelope-wrapped response for ``GET /api/metrics/notifications``."""

    type: Literal["notification_metrics_response"] = "notification_metrics_response"


@router.get("/notifications")
async def get_notification_metrics(
    request: Request,
    _principal: Annotated[
        AuthPrincipal,
        Depends(require_permission(Permission.READ_SYSTEM_STATUS)),
    ],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> NotificationMetricsResponse:
    """Return current notify-sidecar outbox counters (§D11).

    Args:
        request: FastAPI request — provides REST tracker.
        _principal: Authenticated caller (permission guard already
            enforced at dependency resolution; the bound value is
            discarded because the counters are user-agnostic).
        repo: Repository dependency.

    Returns:
        ``NotificationMetricsResponse`` with zero-defaults for every
        status absent from the current DB snapshot.
    """
    counts = await repo.count_deliveries_by_status()
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = dt.datetime.now(dt.UTC)
    pid = str(uuid7())
    payload = NotificationMetricsData(
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
        delivery_success_total=counts.get("sent", 0),
        delivery_failed_total=counts.get("failed", 0),
        delivery_410_unregistered_total=counts.get("unregistered", 0),
        delivery_cancelled_scope_total=counts.get("cancelled_scope", 0),
        outbox_queued_depth=counts.get("queued", 0),
    )
    envelope_sid, envelope_seq, envelope_ts, envelope_pid = _next_provenance(tracker)
    return NotificationMetricsResponse(
        session_id=envelope_sid,
        sequence_id=envelope_seq,
        public_id=envelope_pid,
        timestamp=envelope_ts,
        payload=payload,
    )


def _next_provenance(tracker: SequenceTracker) -> tuple[str, int, datetime, str]:
    """Mint a fresh envelope ``(session_id, sequence_id, timestamp, public_id)``."""
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = dt.datetime.now(dt.UTC)
    pid = str(uuid7())
    return sid, seq, ts, pid
