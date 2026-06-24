"""REST route for egress pool status.

``GET /api/health/egress`` returns the process-local egress pool's
operator snapshot: configured routes, operator metadata, quarantine
state, in-use counts, and active reservation traffic tuples.

RBAC: the route requires :data:`Permission.READ_SYSTEM_STATUS`, matching
the detailed monitoring endpoints because route exits and private
reservation state are operational data.
"""

import datetime as dt
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated
from typing import Literal
from urllib.parse import urlsplit
from uuid import uuid7

from fastapi import APIRouter
from fastapi import Depends
from fastapi import Request

from snapper.api.schemas.health import EgressContainerSummary
from snapper.api.schemas.health import EgressHealthData
from snapper.api.schemas.health import EgressHealthResponse
from snapper.auth.dependencies import require_permission
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.permissions import Permission
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.infrastructure.network.egress_models import EgressActiveReservationSnapshot
from snapper.infrastructure.network.egress_models import EgressConnectionSnapshot
from snapper.infrastructure.network.egress_models import EgressPoolStatusSnapshot
from snapper.infrastructure.network.egress_models import EgressRouteStatusSnapshot
from snapper.infrastructure.network.egress_models import EgressTransferSnapshot
from snapper.infrastructure.network.egress_observability import resolve_egress_container_id
from snapper.infrastructure.network.egress_pool import get_egress_pool
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.egress_snapshot_cache import EgressCachedSnapshot
from snapper.server.egress_snapshot_cache import EgressCachedTransfer
from snapper.server.egress_snapshot_cache import EgressSnapshotCache

router = APIRouter(prefix="/health", tags=["health"])

_REST_STREAM = "rest.egress_health"


@dataclass(frozen=True, slots=True)
class _SnapshotSource:
    """Egress snapshot plus its source summary fields."""

    container: str
    snapshot: EgressPoolStatusSnapshot
    last_seen_age_seconds: float
    stale: bool


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


def _with_container(
    reservation: EgressActiveReservationSnapshot,
    container: str,
) -> EgressActiveReservationSnapshot:
    """Return a reservation snapshot stamped with its source container."""
    return EgressActiveReservationSnapshot(
        exchange=reservation.exchange,
        traffic_class=reservation.traffic_class,
        container=container,
    )


def _connection_with_container(
    connection: EgressConnectionSnapshot,
    container: str,
) -> EgressConnectionSnapshot:
    """Return a connection snapshot stamped with its source container."""
    return EgressConnectionSnapshot(
        host=connection.host,
        kind=connection.kind,
        exchange=connection.exchange,
        traffic_class=connection.traffic_class,
        container=container,
        count=connection.count,
        last_seen_at=connection.last_seen_at,
    )


def _connection_key(
    connection: EgressConnectionSnapshot,
) -> tuple[str, str, str, str, str]:
    """Return the merge identity for a connection snapshot row."""
    return (
        connection.container,
        connection.host,
        connection.kind,
        connection.exchange,
        connection.traffic_class,
    )


def _latest_seen_at(
    first: datetime | None,
    second: datetime | None,
) -> datetime | None:
    """Return the latest optional REST last-seen timestamp."""
    if first is None:
        return second
    if second is None:
        return first
    return max(first, second)


def _merge_connections(
    current: list[EgressConnectionSnapshot],
    incoming: list[EgressConnectionSnapshot],
) -> list[EgressConnectionSnapshot]:
    """Merge connection rows by source, host, kind, exchange, and class."""
    by_key: dict[tuple[str, str, str, str, str], EgressConnectionSnapshot] = {}
    order: list[tuple[str, str, str, str, str]] = []
    for connection in [*current, *incoming]:
        key = _connection_key(connection)
        existing = by_key.get(key)
        if existing is None:
            by_key[key] = connection
            order.append(key)
            continue
        by_key[key] = EgressConnectionSnapshot(
            host=existing.host,
            kind=existing.kind,
            exchange=existing.exchange,
            traffic_class=existing.traffic_class,
            container=existing.container,
            count=existing.count + connection.count,
            last_seen_at=_latest_seen_at(existing.last_seen_at, connection.last_seen_at),
        )
    return [by_key[key] for key in order]


def _merge_route(
    current: EgressRouteStatusSnapshot,
    incoming: EgressRouteStatusSnapshot,
) -> EgressRouteStatusSnapshot:
    """Merge one incoming route row into an existing aggregate route."""
    seen = {
        (item.container, item.exchange, item.traffic_class) for item in current.active_reservations
    }
    active = list(current.active_reservations)
    for reservation in incoming.active_reservations:
        key = (reservation.container, reservation.exchange, reservation.traffic_class)
        if key not in seen:
            active.append(reservation)
            seen.add(key)
    remaining_values = [
        value
        for value in (
            current.quarantine_seconds_remaining,
            incoming.quarantine_seconds_remaining,
        )
        if value is not None
    ]
    remaining = max(remaining_values) if remaining_values else None
    return EgressRouteStatusSnapshot(
        id=current.id,
        kind=current.kind,
        proxy_url=current.proxy_url or incoming.proxy_url,
        region=current.region,
        exit_ip=current.exit_ip,
        provider=current.provider,
        priority=current.priority,
        allowed_exchanges=list(current.allowed_exchanges),
        enabled=current.enabled or incoming.enabled,
        quarantined=current.quarantined or incoming.quarantined,
        quarantine_seconds_remaining=remaining,
        in_use_count=current.in_use_count + incoming.in_use_count,
        active_reservations=active,
        connections=_merge_connections(current.connections, incoming.connections),
        transfer=current.transfer or incoming.transfer,
    )


def _route_for_source(
    route: EgressRouteStatusSnapshot,
    container: str,
) -> EgressRouteStatusSnapshot:
    """Return a route row whose active reservations are source-stamped."""
    return EgressRouteStatusSnapshot(
        id=route.id,
        kind=route.kind,
        proxy_url=route.proxy_url,
        region=route.region,
        exit_ip=route.exit_ip,
        provider=route.provider,
        priority=route.priority,
        allowed_exchanges=list(route.allowed_exchanges),
        enabled=route.enabled,
        quarantined=route.quarantined,
        quarantine_seconds_remaining=route.quarantine_seconds_remaining,
        in_use_count=route.in_use_count,
        active_reservations=[
            _with_container(reservation, container) for reservation in route.active_reservations
        ],
        connections=[
            _connection_with_container(connection, container) for connection in route.connections
        ],
        transfer=route.transfer,
    )


def _merged_snapshot(sources: list[_SnapshotSource]) -> EgressPoolStatusSnapshot:
    """Merge source snapshots into one operator status projection."""
    routes: dict[str, EgressRouteStatusSnapshot] = {}
    route_order: list[str] = []
    enabled = False
    private_on_fallback = False
    on_all_quarantined: Literal["wait", "raise"] | None = None
    private_fallback_route_id: str | None = None
    for source in sources:
        snapshot = source.snapshot
        enabled = enabled or snapshot.enabled
        private_on_fallback = private_on_fallback or snapshot.private_on_fallback
        if on_all_quarantined is None:
            on_all_quarantined = snapshot.on_all_quarantined
        if private_fallback_route_id is None:
            private_fallback_route_id = snapshot.private_fallback_route_id
        for route in snapshot.routes:
            stamped = _route_for_source(route, source.container)
            existing = routes.get(route.id)
            if existing is None:
                routes[route.id] = stamped
                route_order.append(route.id)
            else:
                routes[route.id] = _merge_route(existing, stamped)
    return EgressPoolStatusSnapshot(
        enabled=enabled,
        on_all_quarantined=on_all_quarantined,
        private_fallback_route_id=private_fallback_route_id,
        private_on_fallback=private_on_fallback,
        routes=[routes[route_id] for route_id in route_order],
    )


def _container_summaries(sources: list[_SnapshotSource]) -> list[EgressContainerSummary]:
    """Build per-container status rows for the response payload."""
    return [
        EgressContainerSummary(
            container=source.container,
            last_seen_age_seconds=source.last_seen_age_seconds,
            stale=source.stale,
            route_count=len(source.snapshot.routes),
        )
        for source in sources
    ]


def _socks5_port_from_proxy_url(proxy_url: str | None) -> int | None:
    """Extract a SOCKS5 listener port from a route proxy URL."""
    if proxy_url is None:
        return None
    try:
        parsed = urlsplit(proxy_url)
        if parsed.scheme != "socks5h":
            return None
        return parsed.port
    except ValueError:
        return None


def _transfer_by_port(
    transfers: list[EgressCachedTransfer],
) -> dict[int, EgressCachedTransfer | None]:
    """Return cached transfer samples keyed by unambiguous SOCKS5 port."""
    by_port: dict[int, EgressCachedTransfer | None] = {}
    for item in transfers:
        port = item.snapshot.socks5_listen_port
        if port in by_port:
            by_port[port] = None
        else:
            by_port[port] = item
    return by_port


def _route_port_counts(routes: list[EgressRouteStatusSnapshot]) -> dict[int, int]:
    """Count how many merged route rows claim each SOCKS5 port."""
    counts: dict[int, int] = {}
    for route in routes:
        port = _socks5_port_from_proxy_url(route.proxy_url)
        if port is None:
            continue
        counts[port] = counts.get(port, 0) + 1
    return counts


def _transfer_snapshot_from_cached(cached: EgressCachedTransfer) -> EgressTransferSnapshot:
    """Project one cached sidecar sample into a route transfer snapshot."""
    row = cached.snapshot
    return EgressTransferSnapshot(
        interface=row.interface,
        socks5_listen_port=row.socks5_listen_port,
        rx_bytes=row.rx_bytes,
        tx_bytes=row.tx_bytes,
        rx_rate_bytes_per_second=row.rx_rate_bytes_per_second,
        tx_rate_bytes_per_second=row.tx_rate_bytes_per_second,
        latest_handshake_at=row.latest_handshake_at,
        counter_reset=row.counter_reset,
        sampled_at=row.sampled_at,
        sample_age_seconds=cached.age_seconds,
        stale=cached.stale,
    )


def _route_with_transfer(
    route: EgressRouteStatusSnapshot,
    transfer: EgressTransferSnapshot | None,
) -> EgressRouteStatusSnapshot:
    """Return a route row with the joined transfer snapshot attached."""
    return EgressRouteStatusSnapshot(
        id=route.id,
        kind=route.kind,
        proxy_url=route.proxy_url,
        region=route.region,
        exit_ip=route.exit_ip,
        provider=route.provider,
        priority=route.priority,
        allowed_exchanges=list(route.allowed_exchanges),
        enabled=route.enabled,
        quarantined=route.quarantined,
        quarantine_seconds_remaining=route.quarantine_seconds_remaining,
        in_use_count=route.in_use_count,
        active_reservations=list(route.active_reservations),
        connections=list(route.connections),
        transfer=transfer,
    )


def _snapshot_with_transfers(
    snapshot: EgressPoolStatusSnapshot,
    transfers: list[EgressCachedTransfer],
) -> EgressPoolStatusSnapshot:
    """Attach cached transfer samples to route rows by SOCKS5 port."""
    by_port = _transfer_by_port(transfers)
    port_counts = _route_port_counts(snapshot.routes)
    routes: list[EgressRouteStatusSnapshot] = []
    for route in snapshot.routes:
        port = _socks5_port_from_proxy_url(route.proxy_url)
        transfer: EgressTransferSnapshot | None = None
        if port is not None and port_counts.get(port, 0) == 1:
            cached = by_port.get(port)
            if cached is not None:
                transfer = _transfer_snapshot_from_cached(cached)
        routes.append(_route_with_transfer(route, transfer))
    return EgressPoolStatusSnapshot(
        enabled=snapshot.enabled,
        on_all_quarantined=snapshot.on_all_quarantined,
        private_fallback_route_id=snapshot.private_fallback_route_id,
        private_on_fallback=snapshot.private_on_fallback,
        routes=routes,
    )


def _resolve_local_container(request: Request) -> str:
    """Resolve the local API source id for egress observability."""
    container = getattr(request.app.state, "egress_container", None)
    if isinstance(container, str) and container:
        return container
    return resolve_egress_container_id("api")


def _cached_sources(request: Request) -> list[_SnapshotSource]:
    """Return cached remote egress snapshots attached to the FastAPI app."""
    cache = getattr(request.app.state, "egress_snapshot_cache", None)
    if not isinstance(cache, EgressSnapshotCache):
        return []
    snapshots: list[EgressCachedSnapshot] = cache.latest_snapshots()
    return [
        _SnapshotSource(
            container=item.container,
            snapshot=item.snapshot,
            last_seen_age_seconds=item.age_seconds,
            stale=item.stale,
        )
        for item in snapshots
    ]


def _cached_transfers(request: Request) -> list[EgressCachedTransfer]:
    """Return cached sidecar transfer samples attached to the FastAPI app."""
    cache = getattr(request.app.state, "egress_snapshot_cache", None)
    if not isinstance(cache, EgressSnapshotCache):
        return []
    return cache.latest_transfers()


def _data_from_snapshot(
    snapshot: EgressPoolStatusSnapshot,
    *,
    containers: list[EgressContainerSummary],
    session_id: str,
    sequence_id: int,
    public_id: str,
    timestamp: datetime,
) -> EgressHealthData:
    """Project an egress pool snapshot into the REST payload schema.

    Args:
        snapshot: Pool status snapshot from infrastructure.
        containers: Per-container reporting summary rows.
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
        containers=containers,
        routes=snapshot.routes,
    )


@router.get("/egress")
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
    local_snapshot = _disabled_snapshot() if pool is None else pool.status_snapshot()
    local_container = _resolve_local_container(request)
    sources = [
        _SnapshotSource(
            container=local_container,
            snapshot=local_snapshot,
            last_seen_age_seconds=0.0,
            stale=False,
        ),
        *_cached_sources(request),
    ]
    snapshot = _snapshot_with_transfers(
        _merged_snapshot(sources),
        _cached_transfers(request),
    )
    sid, seq, pid, ts = _envelope_provenance(request)
    payload = _data_from_snapshot(
        snapshot,
        containers=_container_summaries(sources),
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
