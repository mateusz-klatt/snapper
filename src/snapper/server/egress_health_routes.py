"""REST route for egress pool status.

``GET /api/health/egress`` returns the process-local egress pool's
operator snapshot: configured routes, operator metadata, quarantine
state, in-use counts, and active reservation traffic tuples.

RBAC: the route requires :data:`Permission.READ_SYSTEM_STATUS`, matching
the detailed monitoring endpoints because route exits and private
reservation state are operational data.
"""

import datetime as dt
from datetime import datetime
from typing import Annotated
from typing import Literal
from uuid import uuid7

from fastapi import APIRouter
from fastapi import Depends
from fastapi import Request

from snapper.api.schemas.health import EgressHealthData
from snapper.api.schemas.health import EgressHealthResponse
from snapper.auth.dependencies import require_permission
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.permissions import Permission
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.infrastructure.network.egress_models import EgressPoolStatusSnapshot
from snapper.infrastructure.network.egress_pool import get_egress_pool
from snapper.messaging.infrastructure.publisher import SequenceTracker

router = APIRouter(prefix="/health", tags=["health"])

_REST_STREAM = "rest.egress_health"


def _envelope_provenance(request: Request) -> tuple[str, int, str, datetime]:
    """Stamp REST tracker provenance fields onto an egress response.

    Args:
        request: Incoming request, carrying the REST tracker on app state.

    Returns:
        Tuple of session id, sequence id, public id, and timestamp.
    """
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = dt.datetime.now(dt.UTC)
    pid = str(uuid7())
    return sid, seq, pid, ts


def _disabled_snapshot() -> EgressPoolStatusSnapshot:
    """Build the explicit disabled-pool egress status payload.

    Returns:
        A snapshot with ``enabled=False`` and no routes.
    """
    return EgressPoolStatusSnapshot(
        enabled=False,
        on_all_quarantined=None,
        private_fallback_route_id=None,
        private_on_fallback=False,
        routes=[],
    )


def _data_from_snapshot(
    snapshot: EgressPoolStatusSnapshot,
    *,
    session_id: str,
    sequence_id: int,
    public_id: str,
    timestamp: datetime,
) -> EgressHealthData:
    """Project an egress pool snapshot into the REST payload schema.

    Args:
        snapshot: Pool status snapshot from infrastructure.
        session_id: REST provenance session id.
        sequence_id: REST provenance sequence id.
        public_id: REST provenance public id.
        timestamp: REST provenance timestamp.

    Returns:
        Envelope payload with provenance and egress status fields.
    """
    return EgressHealthData(
        session_id=session_id,
        sequence_id=sequence_id,
        public_id=public_id,
        timestamp=timestamp,
        enabled=snapshot.enabled,
        on_all_quarantined=snapshot.on_all_quarantined,
        private_fallback_route_id=snapshot.private_fallback_route_id,
        private_on_fallback=snapshot.private_on_fallback,
        routes=snapshot.routes,
    )


@router.get("/egress", response_model=EgressHealthResponse)
async def get_egress_health(
    request: Request,
    _principal: Annotated[
        AuthPrincipal,
        Depends(require_permission(Permission.READ_SYSTEM_STATUS)),
    ],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
) -> EgressHealthResponse:
    """Return the configured egress pool's operator status snapshot.

    Args:
        request: Incoming request, carrying the REST tracker on app state.
        _principal: Authenticated caller with ``READ_SYSTEM_STATUS``.
        _csrf: CSRF guard dependency, matching detailed health routes.

    Returns:
        ``EgressHealthResponse`` with an explicit disabled payload when
        no egress pool singleton is configured.
    """
    pool = get_egress_pool()
    snapshot = _disabled_snapshot() if pool is None else pool.status_snapshot()
    sid, seq, pid, ts = _envelope_provenance(request)
    payload = _data_from_snapshot(
        snapshot,
        session_id=sid,
        sequence_id=seq,
        public_id=str(uuid7()),
        timestamp=ts,
    )
    return EgressHealthResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
        payload=payload,
    )


__all__: list[Literal["router", "get_egress_health"]] = ["router", "get_egress_health"]
