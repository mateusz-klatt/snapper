"""Route tests for :mod:`snapper.server.egress_health_routes`.

Covers ``GET /api/health/egress`` for configured and disabled pools,
plus the ``READ_SYSTEM_STATUS`` dependency binding used by detailed
operator health endpoints.
"""

import inspect
from unittest.mock import MagicMock

import pytest
from fastapi import Request

from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.infrastructure.network.egress_models import EgressPoolConfig
from snapper.infrastructure.network.egress_models import RouteConfig
from snapper.infrastructure.network.egress_pool import configure_egress_pool
from snapper.infrastructure.network.egress_pool import get_egress_pool
from snapper.infrastructure.network.egress_pool import reset_egress_pool
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.egress_health_routes import get_egress_health
from snapper.server.egress_health_routes import router


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


def _request() -> Request:
    """Build a mock :class:`Request` carrying the REST tracker on state."""
    request = MagicMock(spec=Request)
    request.app.state.rest_tracker = SequenceTracker()
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
        assert data["payload"]["routes"] == []

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
        assert direct["active_reservations"] == [{"exchange": "kraken", "traffic_class": "private"}]
        assert fallback["id"] == "pl"
        assert fallback["allowed_exchanges"] == ["walutomat"]
        assert fallback["region"] == "pl-waw"
        assert fallback["exit_ip"] == "203.0.113.10"
        assert fallback["provider"] == "wireguard-pl"

        reservation.release()

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
