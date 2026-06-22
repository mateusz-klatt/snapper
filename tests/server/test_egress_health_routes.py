"""Route tests for :mod:`snapper.server.egress_health_routes`.

Covers ``GET /api/health/egress`` for configured and disabled pools,
plus the ``READ_SYSTEM_STATUS`` dependency binding used by detailed
operator health endpoints.
"""

import inspect
from datetime import UTC
from datetime import datetime
from unittest.mock import MagicMock

import pytest
from fastapi import Request

from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.infrastructure.network.egress_models import EgressActiveReservationSnapshot
from snapper.infrastructure.network.egress_models import EgressConnectionSnapshot
from snapper.infrastructure.network.egress_models import EgressPoolConfig
from snapper.infrastructure.network.egress_models import EgressPoolStatusSnapshot
from snapper.infrastructure.network.egress_models import EgressRouteStatusSnapshot
from snapper.infrastructure.network.egress_models import RouteConfig
from snapper.infrastructure.network.egress_observability import EGRESS_SNAPSHOT_TOPIC
from snapper.infrastructure.network.egress_pool import configure_egress_pool
from snapper.infrastructure.network.egress_pool import get_egress_pool
from snapper.infrastructure.network.egress_pool import reset_egress_pool
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.data import EgressPoolSnapshotEventData
from snapper.server import egress_health_routes as egress_routes
from snapper.server.egress_health_routes import get_egress_health
from snapper.server.egress_health_routes import router
from snapper.server.egress_snapshot_cache import EgressSnapshotCache


@pytest.fixture(autouse=True)
def _reset_pool() -> None:
    """Reset the process-local egress pool around every route test."""
    reset_egress_pool()
    yield
    reset_egress_pool()


def _principal() -> AuthPrincipal:
    """Build an ADMIN principal for the RBAC-gated egress health handler."""
    return AuthPrincipal(
        username="admin",
        role=UserRole.ADMIN,
        user_public_id="admin-1",
    )


class _FakeClock:
    """Mutable monotonic clock for cache-backed route tests."""

    def __init__(self, value: float) -> None:
        """Store the initial monotonic value."""
        self.value = value

    def __call__(self) -> float:
        """Return the current fake monotonic value."""
        return self.value


def _request(cache: EgressSnapshotCache | None = None) -> Request:
    """Build a mock :class:`Request` carrying the REST tracker on state."""
    request = MagicMock(spec=Request)
    request.app.state.rest_tracker = SequenceTracker()
    request.app.state.egress_container = "api"
    request.app.state.egress_snapshot_cache = cache
    return request


def _request_without_container(cache: EgressSnapshotCache | None = None) -> Request:
    """Build a request mock with no explicit egress container state."""
    request = MagicMock(spec=Request)
    request.app.state.rest_tracker = SequenceTracker()
    request.app.state.egress_snapshot_cache = cache
    return request


def _configure_pool_with_private_fallback() -> None:
    """Install a mixed route egress pool for endpoint response tests."""
    configure_egress_pool(
        EgressPoolConfig(
            enabled=True,
            private_fallback_route_id="pl",
            routes=[
                RouteConfig(
                    id="default",
                    kind="direct",
                    priority=100,
                    region="host",
                    exit_ip="198.51.100.11",
                    provider="isp",
                ),
                RouteConfig(
                    id="pl",
                    kind="socks5",
                    proxy_url="socks5h://pl:1084",
                    priority=5,
                    allowed_exchanges=("walutomat",),
                    region="pl-waw",
                    exit_ip="203.0.113.10",
                    provider="wireguard-pl",
                ),
            ],
        )
    )


def _feed_snapshot() -> EgressPoolStatusSnapshot:
    """Build a remote feed snapshot for route merge tests."""
    return EgressPoolStatusSnapshot(
        enabled=True,
        on_all_quarantined="wait",
        private_fallback_route_id="pl",
        private_on_fallback=False,
        routes=[
            EgressRouteStatusSnapshot(
                id="default",
                kind="direct",
                priority=100,
                allowed_exchanges=[],
                enabled=True,
                quarantined=False,
                quarantine_seconds_remaining=None,
                in_use_count=2,
                active_reservations=[
                    EgressActiveReservationSnapshot(
                        exchange="walutomat",
                        traffic_class="public",
                    )
                ],
                connections=[
                    EgressConnectionSnapshot(
                        host="ws.kraken.com",
                        kind="ws",
                        exchange="walutomat",
                        traffic_class="public",
                        count=2,
                    )
                ],
            )
        ],
    )


def _event_payload(container: str, snapshot: EgressPoolStatusSnapshot) -> bytes:
    """Build one serialized egress snapshot event payload."""
    event = EgressPoolSnapshotEventData(
        session_id="session-1",
        sequence_id=1,
        public_id="event-1",
        timestamp=datetime(2026, 6, 22, tzinfo=UTC),
        container=container,
        snapshot=snapshot,
    )
    return event.publish_to(EGRESS_SNAPSHOT_TOPIC)


class TestEgressHealthRoute:
    """``GET /api/health/egress`` returns the typed egress pool snapshot."""

    @pytest.mark.asyncio
    async def test_endpoint_returns_disabled_payload_when_pool_absent(self) -> None:
        """Spec — missing egress pool returns disabled payload rather than an error.

        Given no process-local egress pool singleton,
        When the GET /api/health/egress handler is called,
        Then the response is successful and payload.enabled is False.
        """
        response = await get_egress_health(
            request=_request(),
            _principal=_principal(),
            _csrf=None,
        )

        data = response.model_dump(mode="json")
        assert data["type"] == "egress_health_response"
        assert data["payload"]["type"] == "egress_health"
        assert data["payload"]["enabled"] is False
        assert data["payload"]["on_all_quarantined"] is None
        assert data["payload"]["private_fallback_route_id"] is None
        assert data["payload"]["private_on_fallback"] is False
        assert data["payload"]["containers"] == [
            {
                "container": "api",
                "last_seen_age_seconds": 0.0,
                "stale": False,
                "route_count": 0,
            }
        ]
        assert data["payload"]["routes"] == []

    @pytest.mark.asyncio
    async def test_endpoint_falls_back_to_resolved_api_container(self) -> None:
        """Spec — missing app container state falls back to an API source id.

        Given the FastAPI app state has no egress_container attribute,
        When the GET /api/health/egress handler is called,
        Then the local disabled snapshot is attributed to an api container id.
        """
        response = await get_egress_health(
            request=_request_without_container(),
            _principal=_principal(),
            _csrf=None,
        )

        container = response.payload.containers[0].container
        assert container.startswith("api@")

    @pytest.mark.asyncio
    async def test_endpoint_returns_configured_pool_snapshot(self) -> None:
        """Spec — configured pool status reaches the HTTP response model.

        Given an enabled pool with a private fallback and an active
            private reservation on direct,
        When the GET /api/health/egress handler is called,
        Then the response includes pool policy, route metadata, and
            active reservations.
        """
        _configure_pool_with_private_fallback()
        pool = get_egress_pool()
        assert pool is not None
        reservation = pool.reserve(
            exchange="kraken",
            purpose="websocket",
            traffic_class="private",
        )

        response = await get_egress_health(
            request=_request(),
            _principal=_principal(),
            _csrf=None,
        )

        payload = response.model_dump(mode="json")["payload"]
        assert payload["enabled"] is True
        assert payload["on_all_quarantined"] == "wait"
        assert payload["private_fallback_route_id"] == "pl"
        assert payload["private_on_fallback"] is False
        assert len(payload["routes"]) == 2
        direct = payload["routes"][0]
        fallback = payload["routes"][1]
        assert direct["id"] == "default"
        assert direct["kind"] == "direct"
        assert direct["region"] == "host"
        assert direct["exit_ip"] == "198.51.100.11"
        assert direct["provider"] == "isp"
        assert direct["in_use_count"] == 1
        assert direct["active_reservations"] == [
            {"exchange": "kraken", "traffic_class": "private", "container": "api"}
        ]
        assert fallback["id"] == "pl"
        assert fallback["allowed_exchanges"] == ["walutomat"]
        assert fallback["region"] == "pl-waw"
        assert fallback["exit_ip"] == "203.0.113.10"
        assert fallback["provider"] == "wireguard-pl"

        reservation.release()

    @pytest.mark.asyncio
    async def test_endpoint_merges_cached_feed_snapshot(self) -> None:
        """Spec — API and feed snapshots merge per route with source stamps.

        Given the API process has one local direct reservation and the
            cache has a feed snapshot for the same route with two holds,
        When the egress health handler is called,
        Then in-use counts are summed and reservation rows carry their
            source container.
        """
        _configure_pool_with_private_fallback()
        pool = get_egress_pool()
        assert pool is not None
        local_reservation = pool.reserve(
            exchange="kraken",
            purpose="websocket",
            traffic_class="private",
            target_host="ws-auth.kraken.com",
            connection_kind="ws",
        )
        clock = _FakeClock(100.0)
        cache = EgressSnapshotCache(
            own_container="api",
            stale_after_seconds=3.0,
            clock=clock,
        )
        cache._ingest(EGRESS_SNAPSHOT_TOPIC, _event_payload("feed", _feed_snapshot()))

        response = await get_egress_health(
            request=_request(cache),
            _principal=_principal(),
            _csrf=None,
        )

        payload = response.model_dump(mode="json")["payload"]
        assert payload["containers"] == [
            {
                "container": "api",
                "last_seen_age_seconds": 0.0,
                "stale": False,
                "route_count": 2,
            },
            {
                "container": "feed",
                "last_seen_age_seconds": 0.0,
                "stale": False,
                "route_count": 1,
            },
        ]
        route = payload["routes"][0]
        assert route["id"] == "default"
        assert route["in_use_count"] == 3
        assert route["active_reservations"] == [
            {"exchange": "kraken", "traffic_class": "private", "container": "api"},
            {"exchange": "walutomat", "traffic_class": "public", "container": "feed"},
        ]
        assert route["connections"] == [
            {
                "host": "ws-auth.kraken.com",
                "kind": "ws",
                "exchange": "kraken",
                "traffic_class": "private",
                "container": "api",
                "count": 1,
                "last_seen_at": None,
            },
            {
                "host": "ws.kraken.com",
                "kind": "ws",
                "exchange": "walutomat",
                "traffic_class": "public",
                "container": "feed",
                "count": 2,
                "last_seen_at": None,
            },
        ]

        local_reservation.release()

    @pytest.mark.asyncio
    async def test_endpoint_flags_cached_stale_snapshot(self) -> None:
        """Spec — stale cached snapshots remain visible and are marked stale.

        Given a feed snapshot received more than the stale threshold ago,
        When the egress health handler is called,
        Then the feed container summary is marked stale while routes remain merged.
        """
        _configure_pool_with_private_fallback()
        clock = _FakeClock(100.0)
        cache = EgressSnapshotCache(
            own_container="api",
            stale_after_seconds=3.0,
            clock=clock,
        )
        cache._ingest(EGRESS_SNAPSHOT_TOPIC, _event_payload("feed", _feed_snapshot()))
        clock.value = 104.0

        response = await get_egress_health(
            request=_request(cache),
            _principal=_principal(),
            _csrf=None,
        )

        containers = response.model_dump(mode="json")["payload"]["containers"]
        assert containers[1] == {
            "container": "feed",
            "last_seen_age_seconds": 4.0,
            "stale": True,
            "route_count": 1,
        }

    @pytest.mark.asyncio
    async def test_endpoint_response_schema_round_trips(self) -> None:
        """Spec — merged egress response remains a strict Pydantic envelope."""
        _configure_pool_with_private_fallback()

        response = await get_egress_health(
            request=_request(),
            _principal=_principal(),
            _csrf=None,
        )

        parsed = response.__class__.model_validate_json(response.model_dump_json())
        assert parsed == response

    def test_merge_route_skips_duplicate_reservation_keys(self) -> None:
        """Spec — duplicate reservation rows are not repeated in route output.

        Given an aggregate route already has a reservation key,
        When an incoming row repeats the same reservation and connection keys,
        Then duplicate reservations stay unique while connections sum counts.
        """
        reservation = EgressActiveReservationSnapshot(
            exchange="kraken",
            traffic_class="public",
            container="feed",
        )
        older_seen_at = datetime(2026, 6, 22, 10, 0, tzinfo=UTC)
        newer_seen_at = datetime(2026, 6, 22, 10, 1, tzinfo=UTC)
        current = EgressRouteStatusSnapshot(
            id="default",
            kind="direct",
            priority=100,
            allowed_exchanges=[],
            enabled=True,
            quarantined=False,
            quarantine_seconds_remaining=None,
            in_use_count=1,
            active_reservations=[reservation],
            connections=[
                EgressConnectionSnapshot(
                    host="api.kraken.com",
                    kind="rest",
                    exchange="kraken",
                    traffic_class="public",
                    container="feed",
                    count=1,
                    last_seen_at=older_seen_at,
                )
            ],
        )
        incoming = EgressRouteStatusSnapshot(
            id="default",
            kind="direct",
            priority=100,
            allowed_exchanges=[],
            enabled=True,
            quarantined=False,
            quarantine_seconds_remaining=None,
            in_use_count=1,
            active_reservations=[reservation],
            connections=[
                EgressConnectionSnapshot(
                    host="api.kraken.com",
                    kind="rest",
                    exchange="kraken",
                    traffic_class="public",
                    container="feed",
                    count=2,
                    last_seen_at=newer_seen_at,
                )
            ],
        )

        merged = egress_routes._merge_route(current, incoming)

        assert merged.in_use_count == 2
        assert merged.active_reservations == [reservation]
        assert merged.connections == [
            EgressConnectionSnapshot(
                host="api.kraken.com",
                kind="rest",
                exchange="kraken",
                traffic_class="public",
                container="feed",
                count=3,
                last_seen_at=newer_seen_at,
            )
        ]

    def test_latest_seen_at_handles_missing_sides(self) -> None:
        """Spec — connection merge keeps whichever last-seen timestamp exists.

        Given one side of a REST connection merge has no timestamp,
        When the helper selects the latest timestamp,
        Then the non-null timestamp is retained.
        """
        seen_at = datetime(2026, 6, 22, 10, 1, tzinfo=UTC)

        assert egress_routes._latest_seen_at(None, seen_at) == seen_at
        assert egress_routes._latest_seen_at(seen_at, None) == seen_at

    def test_router_exposes_health_egress_path(self) -> None:
        """Spec — the router exposes the egress health path.

        Given the egress health router,
        When its declared routes are inspected,
        Then /health/egress is present before the app-level /api prefix.
        """
        paths = {getattr(route, "path", "") for route in router.routes}
        assert "/health/egress" in paths

    def test_endpoint_binds_read_system_status_permission(self) -> None:
        """Spec — the handler requires READ_SYSTEM_STATUS permission.

        Given the egress health route signature,
        When the permission dependency metadata is inspected,
        Then READ_SYSTEM_STATUS is bound to the route.
        """
        signature = inspect.signature(get_egress_health)
        principal_annotation = signature.parameters["_principal"].annotation
        guard_closure = principal_annotation.__metadata__[0].dependency
        bound_permissions = [
            cell.cell_contents
            for cell in (guard_closure.__closure__ or [])
            if hasattr(cell, "cell_contents")
        ]
        assert Permission.READ_SYSTEM_STATUS in bound_permissions
