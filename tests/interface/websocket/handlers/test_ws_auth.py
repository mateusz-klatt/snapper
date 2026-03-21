"""Tests for WebSocket authentication handlers."""

import asyncio
import contextlib
import json
from collections.abc import Generator
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from types import SimpleNamespace
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import WebSocket
from fastapi import WebSocketDisconnect
from fastapi.testclient import TestClient

import snapper.interface.websocket.handlers.auth as auth_handlers
import snapper.server.authenticated_websocket as auth_ws
from snapper.api.auth.errors.ws_token import WsTokenAlreadyUsedError
from snapper.api.auth.errors.ws_token import WsTokenError
from snapper.api.auth.schemas.ws_token import WsTokenPayload
from snapper.api.auth.services.ws_token_service import compute_sid_hash
from snapper.api.auth.services.ws_token_service import get_ws_token_service
from snapper.auth import routes
from snapper.auth.dependencies import require_authentication
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.permissions import ROLE_PERMISSIONS
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.schemas.requests import AdminResetPasswordRequest
from snapper.auth.schemas.requests import ChangePasswordRequest
from snapper.auth.schemas.requests import CreateUserRequest
from snapper.auth.schemas.requests import LoginRequest
from snapper.auth.schemas.requests import UpdateUserRequest
from snapper.auth.schemas.tokens import TokenClaims
from snapper.auth.schemas.tokens import TokenPair
from snapper.auth.schemas.user import UserProfile
from snapper.auth.schemas.websocket import WebSocketAuthMessage
from snapper.auth.schemas.websocket import WebSocketAuthResponse
from snapper.auth.tokens import get_token_manager
from snapper.auth.user_service import get_user_service
from snapper.auth.websocket_auth import AuthConnectionStats
from snapper.auth.websocket_auth import WebSocketAuthManager
from snapper.auth.websocket_auth import get_ws_auth_manager
from snapper.config.bootstrap import BootstrapSettingsLoader
from snapper.interface.websocket.bridge import ZmqWebSocketBridgeService
from snapper.interface.websocket.handlers.auth import AUTH_TIMEOUT_SECONDS
from snapper.interface.websocket.handlers.auth import REAUTH_GRACE_PERIOD
from snapper.interface.websocket.handlers.auth import REAUTH_WARN_OFFSET
from snapper.interface.websocket.handlers.auth import AuthResult
from snapper.interface.websocket.handlers.auth import authenticate_websocket
from snapper.interface.websocket.handlers.auth import create_deadline_tasks
from snapper.interface.websocket.handlers.auth import handle_reauth
from snapper.interface.websocket.handlers.ping import handle_ping
from snapper.interface.websocket.handlers.subscribe import handle_get_subscriptions
from snapper.interface.websocket.handlers.subscribe import handle_subscribe
from snapper.interface.websocket.handlers.subscribe import handle_unsubscribe
from snapper.interface.websocket.helpers import determine_topic_category
from snapper.interface.websocket.helpers import filter_topics
from snapper.interface.websocket.schemas import WSReauthRequest
from snapper.interface.websocket.schemas import WSSubscribeRequest
from snapper.interface.websocket.schemas import WSUnsubscribeRequest
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.app import create_app
from snapper.server.authenticated_websocket import create_authenticated_websocket_router
from snapper.server.authenticated_websocket import get_allowed_topics_for_role
from snapper.server.authenticated_websocket import has_trading_permission


def _make_rest_request() -> MagicMock:
    """Build a mock FastAPI Request with REST tracker on app.state."""
    mock = MagicMock()
    mock.app.state.rest_tracker = SequenceTracker()
    return mock


pytest = cast(Any, pytest)
create_app = cast(Any, create_app)
create_authenticated_websocket_router = cast(Any, create_authenticated_websocket_router)
has_trading_permission = cast(Any, has_trading_permission)


@pytest.fixture
def test_client(mock_settings_for_tests: Any) -> Generator[Any]:
    """Provide a TestClient with mocked authentication."""
    app: Any = create_app()
    app.state.settings = SimpleNamespace(
        session_secure=False,
        session_same_site="lax",
        instruments={
            "kraken": ["BTC-USD", "ETH-USD", "EUR-USD"],
            "zonda": ["BTC-PLN"],
            "walutomat": [],
            "polygon": [],
        },
    )

    def skip_csrf_validation() -> None:
        return None

    def skip_authentication() -> AuthPrincipal:
        return AuthPrincipal(username="test_user", role=UserRole.ADMIN)

    app.dependency_overrides[validate_csrf_token] = skip_csrf_validation
    app.dependency_overrides[require_authentication] = skip_authentication
    with TestClient(app) as client:
        yield client


WS_PATH = "/api/ws"
ADMIN_USERNAME = "admin"
ADMIN_PASSWORD = "AdminSnapper2026!"
OPERATOR_USERNAME = "operator"
OPERATOR_PASSWORD = "OpSnapper2026!"
VIEWER_USERNAME = "viewer"
VIEWER_PASSWORD = "ViewSnapper2026!"


def _receive_json(websocket: Any) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(websocket.receive_text()))


def _connect_with_cookie(test_client: Any, token: str) -> Any:
    test_client.cookies.set("access_token", token)
    return test_client.websocket_connect(WS_PATH)


def _prepare_ws_token(test_client: Any, *, username: str, password: str) -> str:
    test_client.cookies.clear()
    login_response = test_client.post(
        "/api/auth/login",
        json={
            "session_id": "",
            "sequence_id": 0,
            "username": username,
            "password": password,
        },
    )
    assert login_response.status_code == 200
    response = test_client.post("/api/auth/refresh")
    assert response.status_code == 200
    data = cast(dict[str, Any], response.json())
    return cast(str, data["ws_token"])


def _complete_handshake(websocket: Any, ws_token: str) -> tuple[dict[str, Any], dict[str, Any]]:
    auth_required = _receive_json(websocket)
    assert auth_required["type"] == "auth_required"
    websocket.send_text(
        json.dumps(
            {"type": "authenticate", "session_id": "", "sequence_id": 0, "ws_token": ws_token}
        )
    )
    auth_ok = _receive_json(websocket)
    assert auth_ok["type"] == "auth_ok"
    auth_complete = _receive_json(websocket)
    assert auth_complete["type"] == "auth_complete"
    return auth_ok, auth_complete


class TestSecureWebSocketIntegration:
    """Integration tests for secure WebSocket authentication."""

    def test_websocket_requires_authentication(self, test_client: Any) -> None:
        """WebSocket connection requires authentication.

        Given: A WebSocket connection without authentication,
        When: Connecting to the WebSocket endpoint,
        Then: Returns auth_failed and disconnects.
        """
        with test_client.websocket_connect(WS_PATH) as websocket:
            message = _receive_json(websocket)
            assert message["type"] == "auth_failed"
            with pytest.raises(WebSocketDisconnect):
                websocket.receive_text()

    def test_websocket_rejects_invalid_token(self, test_client: Any) -> None:
        """WebSocket rejects invalid authentication token.

        Given: A WebSocket connection with an invalid token cookie,
        When: Attempting to connect,
        Then: Returns auth_failed and disconnects.
        """
        with _connect_with_cookie(test_client, "invalid_token") as websocket:
            response = _receive_json(websocket)
            assert response["type"] == "auth_failed"
            with pytest.raises(WebSocketDisconnect):
                websocket.receive_text()

    def test_websocket_accepts_valid_token(self, test_client: Any) -> None:
        """WebSocket accepts valid authentication token.

        Given: A valid ws_token from an authenticated operator,
        When: Completing WebSocket handshake,
        Then: Returns auth_ok with expiration and available topics.
        """
        ws_token = _prepare_ws_token(
            test_client,
            username=OPERATOR_USERNAME,
            password=OPERATOR_PASSWORD,
        )
        with test_client.websocket_connect(WS_PATH) as websocket:
            auth_ok, auth_complete = _complete_handshake(websocket, ws_token)
            assert isinstance(auth_ok["exp"], str)
            topics = set(cast(list[str], auth_complete["available_topics"]))
            assert "market." in topics
            assert "signals." in topics
            assert "system.heartbeats." in topics
            assert auth_complete["user_role"] == "operator"

    def test_websocket_role_based_subscriptions(self, test_client: Any) -> None:
        """WebSocket enforces role-based subscription permissions.

        Given: An authenticated operator user,
        When: Subscribing to market, signals, and admin topics,
        Then: Grants market and signals but denies admin topics.
        """
        ws_token = _prepare_ws_token(
            test_client,
            username=OPERATOR_USERNAME,
            password=OPERATOR_PASSWORD,
        )
        with test_client.websocket_connect(WS_PATH) as websocket:
            _complete_handshake(websocket, ws_token)
            websocket.send_text(
                json.dumps(
                    {
                        "type": "subscribe",
                        "session_id": "",
                        "sequence_id": 0,
                        "topics": [
                            "market.kraken.BTC-USD.candles.1m",
                            "signals.kraken.BTC-USD.live",
                            "admin.users",
                        ],
                    }
                )
            )
            response = _receive_json(websocket)
            assert response["type"] == "subscription_success"
            assert "market.kraken.BTC-USD.candles.1m" in response["topics"]
            assert "signals.kraken.BTC-USD.live" in response["topics"]
            assert "admin.users" in response["denied_topics"]

    def test_websocket_ping_pong(self, test_client: Any) -> None:
        """WebSocket responds to ping with pong.

        Given: An authenticated WebSocket connection,
        When: Sending a ping message,
        Then: Receives pong response with timestamp.
        """
        ws_token = _prepare_ws_token(
            test_client,
            username=OPERATOR_USERNAME,
            password=OPERATOR_PASSWORD,
        )
        with test_client.websocket_connect(WS_PATH) as websocket:
            _complete_handshake(websocket, ws_token)
            websocket.send_text(json.dumps({"type": "ping", "session_id": "", "sequence_id": 0}))
            response = _receive_json(websocket)
            assert response["type"] == "pong"
            assert "timestamp" in response

    def test_websocket_malformed_json(self, test_client: Any) -> None:
        """WebSocket handles malformed JSON gracefully.

        Given: An authenticated WebSocket connection,
        When: Sending malformed JSON,
        Then: Returns error response.
        """
        ws_token = _prepare_ws_token(
            test_client,
            username=OPERATOR_USERNAME,
            password=OPERATOR_PASSWORD,
        )
        with test_client.websocket_connect(WS_PATH) as websocket:
            _complete_handshake(websocket, ws_token)
            websocket.send_text("invalid json {")
            response = _receive_json(websocket)
            assert response["type"] == "error"
            assert (
                "invalid" in response["message"].lower()
                or "json" in response["message"].lower()
                or "error" in response["message"].lower()
            )

    def test_websocket_unknown_message_type(self, test_client: Any) -> None:
        """WebSocket handles unknown message type.

        Given: An authenticated WebSocket connection,
        When: Sending unknown message type,
        Then: Returns error about invalid message format.
        """
        ws_token = _prepare_ws_token(
            test_client,
            username=OPERATOR_USERNAME,
            password=OPERATOR_PASSWORD,
        )
        with test_client.websocket_connect(WS_PATH) as websocket:
            _complete_handshake(websocket, ws_token)
            websocket.send_text(json.dumps({"type": "unknown_action"}))
            response = _receive_json(websocket)
            assert response["type"] == "error"
            assert (
                "Invalid message format" in response["message"]
                or "internal" in response["message"].lower()
            )


class TestSecureWebSocketViewer:
    """Tests for viewer role WebSocket access."""

    def test_viewer_limited_topics(self, test_client: Any) -> None:
        """Viewer role has limited topic access.

        Given: An authenticated viewer user,
        When: Completing WebSocket handshake,
        Then: Only market and heartbeat topics available, not signals or orders.
        """
        ws_token = _prepare_ws_token(
            test_client,
            username=VIEWER_USERNAME,
            password=VIEWER_PASSWORD,
        )
        with test_client.websocket_connect(WS_PATH) as websocket:
            _, auth_response = _complete_handshake(websocket, ws_token)
            assert auth_response["user_role"] == "viewer"
            topics = set(cast(list[str], auth_response["available_topics"]))
            assert "market." in topics
            assert "system.heartbeats." in topics
            assert "signals." not in topics
            assert "orders" not in topics

    def test_viewer_subscription_filtering(self, test_client: Any) -> None:
        """Viewer subscriptions are filtered by permissions.

        Given: An authenticated viewer user,
        When: Subscribing to market and signals topics,
        Then: Market granted but signals denied with partial status.
        """
        ws_token = _prepare_ws_token(
            test_client,
            username=VIEWER_USERNAME,
            password=VIEWER_PASSWORD,
        )
        with test_client.websocket_connect(WS_PATH) as websocket:
            _complete_handshake(websocket, ws_token)
            websocket.send_text(
                json.dumps(
                    {
                        "type": "subscribe",
                        "session_id": "",
                        "sequence_id": 0,
                        "topics": [
                            "market.kraken.BTC-USD.candles.1m",
                            "signals.kraken.BTC-USD.live",
                        ],
                    }
                )
            )
            response = _receive_json(websocket)
            assert response["type"] == "subscription_success"
            assert response["status"] == "partial"
            assert "market.kraken.BTC-USD.candles.1m" in response["topics"]
            assert "signals.kraken.BTC-USD.live" in response["denied_topics"]


class TestSecureWebSocketAdmin:
    """Tests for admin role WebSocket access."""

    def test_admin_full_access(self, test_client: Any) -> None:
        """Admin role has full topic access.

        Given: An authenticated admin user,
        When: Completing WebSocket handshake,
        Then: All topic categories are available including orders and executions.
        """
        ws_token = _prepare_ws_token(
            test_client,
            username=ADMIN_USERNAME,
            password=ADMIN_PASSWORD,
        )
        with test_client.websocket_connect(WS_PATH) as websocket:
            _, auth_response = _complete_handshake(websocket, ws_token)
            assert auth_response["user_role"] == "admin"
            topics = set(cast(list[str], auth_response["available_topics"]))
            assert "market." in topics
            assert "signals." in topics
            assert "system.heartbeats." in topics
            assert "orders.commands." in topics
            assert "orders.events." in topics


def test_websocket_stats_endpoint(test_client: Any) -> None:
    """WebSocket stats endpoint returns connection statistics.

    Given: A running WebSocket server,
    When: Requesting stats endpoint,
    Then: Returns 200 with dictionary data.
    """
    response = test_client.get("/api/ws/stats")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, dict)


def _prepare_ws_token_v2(test_client: Any, *, username: str, password: str) -> tuple[str, str]:
    login_response = test_client.post(
        "/api/auth/login",
        json={
            "session_id": "",
            "sequence_id": 0,
            "username": username,
            "password": password,
        },
    )
    assert login_response.status_code == 200
    response = test_client.post("/api/auth/refresh")
    assert response.status_code == 200
    data = cast(dict[str, Any], response.json())
    access_token = cast(str, test_client.cookies.get("access_token"))
    assert access_token
    return cast(str, data["ws_token"]), access_token


def test_authentication_failure(test_client: Any) -> None:
    """WebSocket connection fails without authentication.

    Given: A WebSocket connection without credentials,
    When: Connecting to WebSocket endpoint,
    Then: Returns auth_failed and disconnects.
    """
    with test_client.websocket_connect(WS_PATH) as websocket:
        message = _receive_json(websocket)
        assert message["type"] == "auth_failed"
        with pytest.raises(WebSocketDisconnect):
            websocket.receive_text()


def test_successful_handshake_returns_topics(test_client: Any) -> None:
    """Successful handshake returns available topics.

    Given: A valid ws_token from authenticated operator,
    When: Completing handshake,
    Then: Returns signals, heartbeat topics and session expiration.
    """
    ws_token, _ = _prepare_ws_token_v2(
        test_client,
        username=OPERATOR_USERNAME,
        password=OPERATOR_PASSWORD,
    )
    with test_client.websocket_connect(WS_PATH) as websocket:
        _, auth_complete = _complete_handshake(websocket, ws_token)
        topics = set(cast(list[str], auth_complete["available_topics"]))
        assert "signals." in topics
        assert "system.heartbeats." in topics
        assert auth_complete["session_expires_at"] is not None


def test_get_subscriptions_initial_state(test_client: Any) -> None:
    """Get subscriptions returns empty list initially.

    Given: A freshly authenticated WebSocket connection,
    When: Requesting current subscriptions,
    Then: Returns empty list with positive total available.
    """
    ws_token, _ = _prepare_ws_token_v2(
        test_client,
        username=OPERATOR_USERNAME,
        password=OPERATOR_PASSWORD,
    )
    with test_client.websocket_connect(WS_PATH) as websocket:
        _complete_handshake(websocket, ws_token)
        websocket.send_text(
            json.dumps({"type": "get_subscriptions", "session_id": "", "sequence_id": 0})
        )
        response = _receive_json(websocket)
        assert response["type"] == "subscriptions_list"
        assert response["subscriptions"] == []
        assert response["total_available"] > 0


def test_viewer_subscribe_partial_permissions(test_client: Any) -> None:
    """Viewer subscribe returns partial when some topics denied.

    Given: An authenticated viewer,
    When: Subscribing to market and signals topics,
    Then: Market granted, signals denied with partial status.
    """
    ws_token, _ = _prepare_ws_token_v2(
        test_client,
        username=VIEWER_USERNAME,
        password=VIEWER_PASSWORD,
    )
    with test_client.websocket_connect(WS_PATH) as websocket:
        _complete_handshake(websocket, ws_token)
        websocket.send_text(
            json.dumps(
                {
                    "type": "subscribe",
                    "session_id": "",
                    "sequence_id": 0,
                    "topics": ["market.kraken.BTC-USD.candles.1m", "signals.kraken.BTC-USD.live"],
                }
            )
        )
        response = _receive_json(websocket)
        assert response["type"] == "subscription_success"
        assert response["status"] == "partial"
        assert "market.kraken.BTC-USD.candles.1m" in response["topics"]
        assert "signals.kraken.BTC-USD.live" in response["denied_topics"]


def test_order_permission_checks(test_client: Any) -> None:
    """Order message type requires valid format.

    Given: An authenticated viewer,
    When: Sending order message type,
    Then: Returns error about invalid message format.
    """
    ws_token, _ = _prepare_ws_token_v2(
        test_client,
        username=VIEWER_USERNAME,
        password=VIEWER_PASSWORD,
    )
    with test_client.websocket_connect(WS_PATH) as websocket:
        _complete_handshake(websocket, ws_token)
        websocket.send_text(
            json.dumps(
                {
                    "type": "order",
                    "symbol": "BTCUSD",
                    "side": "buy",
                    "quantity": 1.0,
                    "price": 50000,
                }
            )
        )
        response = _receive_json(websocket)
        assert response["type"] == "error"
        assert "Invalid message format" in response.get("message", "")


def test_unsubscribe_flow(test_client: Any) -> None:
    """Unsubscribe removes active subscription.

    Given: An operator subscribed to a market topic,
    When: Unsubscribing from that topic,
    Then: Returns success with unsubscribed status.
    """
    ws_token, _ = _prepare_ws_token_v2(
        test_client,
        username=OPERATOR_USERNAME,
        password=OPERATOR_PASSWORD,
    )
    with test_client.websocket_connect(WS_PATH) as websocket:
        _complete_handshake(websocket, ws_token)
        websocket.send_text(
            json.dumps(
                {
                    "type": "subscribe",
                    "session_id": "",
                    "sequence_id": 0,
                    "topics": ["market.kraken.BTC-USD.candles.1m"],
                }
            )
        )
        _receive_json(websocket)
        websocket.send_text(
            json.dumps(
                {
                    "type": "unsubscribe",
                    "session_id": "",
                    "sequence_id": 0,
                    "topics": ["market.kraken.BTC-USD.candles.1m"],
                }
            )
        )
        response = _receive_json(websocket)
        assert response["type"] == "subscription_success"
        assert response["status"] in {"unsubscribed", "partial"}
        assert "market.kraken.BTC-USD.candles.1m" in response.get("topics", [])


def test_ping_returns_pong(test_client: Any) -> None:
    """Ping message returns pong response.

    Given: An authenticated WebSocket connection,
    When: Sending ping message,
    Then: Receives pong with timestamp.
    """
    ws_token, _ = _prepare_ws_token_v2(
        test_client,
        username=OPERATOR_USERNAME,
        password=OPERATOR_PASSWORD,
    )
    with test_client.websocket_connect(WS_PATH) as websocket:
        _complete_handshake(websocket, ws_token)
        websocket.send_text(json.dumps({"type": "ping", "session_id": "", "sequence_id": 0}))
        response = _receive_json(websocket)
        assert response["type"] == "pong"
        assert "timestamp" in response


def test_ping_then_get_subscriptions_loop_continuation(test_client: Any) -> None:
    """Message loop continues after ping/pong exchange.

    Given: An authenticated WebSocket connection,
    When: Sending ping then get_subscriptions,
    Then: Both responses received correctly.
    """
    ws_token, _ = _prepare_ws_token_v2(
        test_client,
        username=OPERATOR_USERNAME,
        password=OPERATOR_PASSWORD,
    )
    with test_client.websocket_connect(WS_PATH) as websocket:
        _complete_handshake(websocket, ws_token)
        websocket.send_text(json.dumps({"type": "ping", "session_id": "", "sequence_id": 0}))
        pong_response = _receive_json(websocket)
        assert pong_response["type"] == "pong"
        websocket.send_text(
            json.dumps({"type": "get_subscriptions", "session_id": "", "sequence_id": 0})
        )
        subs_response = _receive_json(websocket)
        assert subs_response["type"] == "subscriptions_list"


def test_legacy_heartbeat_with_refresh_returns_error(test_client: Any) -> None:
    """Legacy heartbeat_with_refresh returns error.

    Given: An authenticated WebSocket connection,
    When: Sending legacy heartbeat_with_refresh message,
    Then: Returns invalid message format error.
    """
    ws_token, access_token = _prepare_ws_token_v2(
        test_client,
        username=OPERATOR_USERNAME,
        password=OPERATOR_PASSWORD,
    )
    with test_client.websocket_connect(WS_PATH) as websocket:
        _complete_handshake(websocket, ws_token)
        websocket.send_text(
            json.dumps(
                {
                    "type": "heartbeat_with_refresh",
                    "current_token": access_token,
                }
            )
        )
        response = _receive_json(websocket)
        assert response["type"] == "error"
        assert "Invalid message format" in response["message"]


def test_legacy_token_update_returns_error(test_client: Any) -> None:
    """Legacy token_update message returns error.

    Given: An authenticated WebSocket connection,
    When: Sending legacy token_update message,
    Then: Returns invalid message format error.
    """
    ws_token, access_token = _prepare_ws_token_v2(
        test_client,
        username=OPERATOR_USERNAME,
        password=OPERATOR_PASSWORD,
    )
    with test_client.websocket_connect(WS_PATH) as websocket:
        _complete_handshake(websocket, ws_token)
        websocket.send_text(
            json.dumps(
                {"type": "token_update", "session_id": "", "sequence_id": 0, "token": access_token}
            )
        )
        response = _receive_json(websocket)
        assert response["type"] == "error"
        assert "Invalid message format" in response["message"]


class WebSocketStub:
    """Stub WebSocket for testing authentication flows."""

    def __init__(
        self,
        *,
        headers: dict[str, str] | None = None,
        cookies: dict[str, str] | None = None,
        messages: list[Any] | None = None,
        settings: SimpleNamespace | None = None,
    ) -> None:
        """Initialize the instance."""
        self.headers = headers or {}
        self.cookies = cookies or {}
        self._messages = messages or []
        self.accepted = False
        self.sent: list[str] = []
        self.closed: list[tuple[int | None, str | None]] = []
        self.client = ("127.0.0.1", 8080)
        self.app = SimpleNamespace(
            state=SimpleNamespace(
                settings=settings or SimpleNamespace(ui_origin="", session_domain=""),
            )
        )

    async def accept(self) -> None:
        """Accept the WebSocket connection."""
        self.accepted = True

    async def send_text(self, payload: str) -> None:
        """Send text payload to client."""
        self.sent.append(payload)

    async def close(self, code: int | None = None, reason: str | None = None) -> None:
        """Close the WebSocket connection."""
        self.closed.append((code, reason))

    async def receive_text(self) -> str:
        """Receive text from client or raise disconnect."""
        if not self._messages:
            raise WebSocketDisconnect()
        item = self._messages.pop(0)
        if isinstance(item, Exception):
            raise item
        return str(item)


class ConnectionManagerStub:
    """Stub connection manager for testing."""

    def __init__(self, *, bridge: Any | None = None) -> None:
        """Initialize the instance."""
        self.zmq_bridge = bridge
        self.attached_bridge: Any | None = None
        self.connected: list[tuple[Any, bool]] = []
        self.disconnected: list[Any] = []
        self._tracker = SequenceTracker()

    @property
    def tracker(self) -> SequenceTracker:
        """Provide sequence tracker for provenance stamping."""
        return self._tracker

    def attach_bridge(self, bridge: Any) -> None:
        """Attach ZMQ bridge to manager."""
        self.attached_bridge = bridge

    async def connect(self, websocket: Any, accept: bool = True) -> None:
        """Record WebSocket connection."""
        self.connected.append((websocket, accept))

    async def disconnect(self, websocket: Any) -> None:
        """Record WebSocket disconnection."""
        self.disconnected.append(websocket)


@pytest.mark.asyncio
async def test_handshake_disallowed_origin() -> None:
    """Handshake rejects disallowed origin.

    Given: A WebSocket connection from evil.com origin,
    When: Attempting handshake,
    Then: Closes connection with origin_forbidden reason.
    """
    WebSocketAuthManager.clear_instance()
    manager = ConnectionManagerStub()
    router = create_authenticated_websocket_router(manager)
    websocket = WebSocketStub(
        headers={"origin": "https://evil.com"},
        cookies={},
    )
    endpoint = router.routes[0].endpoint
    await endpoint(websocket)
    assert websocket.accepted
    assert len(websocket.closed) == 1
    assert websocket.closed[0] == (4403, "Origin not allowed")
    sent_messages = [json.loads(msg) for msg in websocket.sent]
    assert any(
        msg.get("type") == "auth_failed" and msg.get("reason") == "origin_forbidden"
        for msg in sent_messages
    )
    WebSocketAuthManager.clear_instance()


@pytest.mark.asyncio
async def test_handshake_missing_cookie() -> None:
    """Handshake fails with missing authentication cookie.

    Given: A WebSocket connection from allowed origin without cookie,
    When: Attempting handshake,
    Then: Closes connection with missing_cookie reason.
    """
    WebSocketAuthManager.clear_instance()
    manager = ConnectionManagerStub()
    router = create_authenticated_websocket_router(manager)
    websocket = WebSocketStub(
        headers={"origin": "http://localhost:8000"},
        cookies={},
    )
    with patch("snapper.server.authenticated_websocket.get_ws_auth_manager") as mock_auth:
        mock_auth_manager = MagicMock()
        mock_auth_manager.verify_session_cookie.return_value = None
        mock_auth.return_value = mock_auth_manager
        endpoint = router.routes[0].endpoint
        await endpoint(websocket)
    assert websocket.accepted
    assert len(websocket.closed) == 1
    assert websocket.closed[0] == (4401, "Authentication cookie missing")
    sent_messages = [json.loads(msg) for msg in websocket.sent]
    assert any(
        msg.get("type") == "auth_failed" and msg.get("reason") == "missing_cookie"
        for msg in sent_messages
    )
    WebSocketAuthManager.clear_instance()


@pytest.mark.asyncio
async def test_handshake_timeout() -> None:
    """Handshake times out waiting for client response.

    Given: A WebSocket connection with valid cookie,
    When: Client does not respond within timeout,
    Then: Closes connection with timeout reason.
    """
    WebSocketAuthManager.clear_instance()
    token_data = TokenClaims(
        sub="user-1",
        username="alice",
        role=UserRole.OPERATOR,
        permissions=["read:orders"],
        exp=int(datetime.now(UTC).timestamp()) + 600,
        iat=int(datetime.now(UTC).timestamp()),
        jti="token-1",
        sid="session-123",
    )
    with patch("snapper.auth.websocket_auth.get_token_manager") as mock_get_token:
        mock_token_manager = MagicMock()
        mock_token_manager.verify_token.return_value = token_data
        mock_get_token.return_value = mock_token_manager
        manager = ConnectionManagerStub()
        router = create_authenticated_websocket_router(manager)
        websocket = WebSocketStub(
            headers={"origin": "http://localhost:8000"},
            cookies={"access_token": "valid_token"},
            messages=[TimeoutError()],
        )
        endpoint = router.routes[0].endpoint
        await endpoint(websocket)
    assert websocket.accepted
    assert len(websocket.closed) == 1
    assert websocket.closed[0] == (4408, "Authentication timeout")
    sent_messages = [json.loads(msg) for msg in websocket.sent]
    assert any(
        msg.get("type") == "auth_failed" and msg.get("reason") == "timeout" for msg in sent_messages
    )
    WebSocketAuthManager.clear_instance()


@pytest.mark.asyncio
async def test_handshake_invalid_json_payload() -> None:
    """Handshake fails with invalid JSON payload.

    Given: A WebSocket connection with valid cookie,
    When: Client sends non-JSON message,
    Then: Closes connection with invalid_json reason.
    """
    WebSocketAuthManager.clear_instance()
    token_data = TokenClaims(
        sub="user-1",
        username="alice",
        role=UserRole.OPERATOR,
        permissions=["read:orders"],
        exp=int(datetime.now(UTC).timestamp()) + 600,
        iat=int(datetime.now(UTC).timestamp()),
        jti="token-1",
        sid="session-123",
    )
    with patch("snapper.auth.websocket_auth.get_token_manager") as mock_get_token:
        mock_token_manager = MagicMock()
        mock_token_manager.verify_token.return_value = token_data
        mock_get_token.return_value = mock_token_manager
        manager = ConnectionManagerStub()
        router = create_authenticated_websocket_router(manager)
        websocket = WebSocketStub(
            headers={"origin": "http://localhost:8000"},
            cookies={"access_token": "valid_token"},
            messages=["not valid json"],
        )
        endpoint = router.routes[0].endpoint
        await endpoint(websocket)
    assert websocket.accepted
    assert len(websocket.closed) == 1
    assert websocket.closed[0] == (4401, "Invalid auth payload")
    sent_messages = [json.loads(msg) for msg in websocket.sent]
    assert any(
        msg.get("type") == "auth_failed" and msg.get("reason") == "invalid_json"
        for msg in sent_messages
    )
    WebSocketAuthManager.clear_instance()


@pytest.mark.asyncio
async def test_handshake_uses_static_allowed_origins_when_state_missing() -> None:
    """Handshake uses static origins when app state missing.

    Given: A WebSocket with app state missing,
    When: Validating origin,
    Then: Falls back to static allowed origins.
    """
    WebSocketAuthManager.clear_instance()
    manager = ConnectionManagerStub()
    router = create_authenticated_websocket_router(manager)
    websocket = WebSocketStub(
        headers={"origin": "http://localhost:8000"},
        cookies={},
    )
    websocket.app = SimpleNamespace()
    with patch("snapper.server.authenticated_websocket.validate_origin") as mock_validate:
        mock_validate.return_value = True
        endpoint = router.routes[0].endpoint
        await endpoint(websocket)
    assert websocket.accepted
    mock_validate.assert_awaited_once()
    WebSocketAuthManager.clear_instance()


@pytest.mark.asyncio
async def test_handshake_success_dispatches_and_sends_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Successful handshake dispatches messages and sends payload.

    Given: A WebSocket with valid authentication,
    When: Completing handshake successfully,
    Then: Sends auth_complete and dispatches message handler.
    """
    WebSocketAuthManager.clear_instance()
    manager = ConnectionManagerStub()
    router = create_authenticated_websocket_router(manager)
    websocket = WebSocketStub(
        headers={"origin": "http://localhost:8000"},
        cookies={"access_token": "token"},
    )
    user = SimpleNamespace(username="alice")
    auth_result = SimpleNamespace(success=True, user=user, ws_payload={"ok": True})

    async def fake_authenticate_websocket(*args: Any, **kwargs: Any) -> Any:
        return auth_result

    monkeypatch.setattr(
        "snapper.server.authenticated_websocket.authenticate_websocket",
        fake_authenticate_websocket,
    )
    send_calls: list[Any] = []
    dispatch_calls: list[Any] = []

    async def fake_send_auth_complete(
        websocket: Any,
        manager: Any,
        user: Any,
        ws_payload: Any,
        ws_auth_manager: Any,
        db_url: str | None = None,
    ) -> None:
        send_calls.append(ws_payload)

    async def fake_dispatch_messages(*args: Any, **kwargs: Any) -> None:
        dispatch_calls.append(True)

    monkeypatch.setattr(
        "snapper.server.authenticated_websocket.send_auth_complete", fake_send_auth_complete
    )
    monkeypatch.setattr(
        "snapper.server.authenticated_websocket.dispatch_messages", fake_dispatch_messages
    )

    async def allow_origin_async(*args: Any, **kwargs: Any) -> bool:
        return True

    monkeypatch.setattr(
        "snapper.server.authenticated_websocket.validate_origin", allow_origin_async
    )
    endpoint = router.routes[0].endpoint
    await endpoint(websocket)
    assert websocket.accepted
    assert manager.connected
    assert send_calls == [{"ok": True}]
    assert dispatch_calls == [True]
    WebSocketAuthManager.clear_instance()


@pytest.mark.asyncio
async def test_handshake_success_without_payload_dispatches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Successful handshake without payload still dispatches.

    Given: Auth result with no ws_payload,
    When: Completing handshake,
    Then: Dispatches messages without sending auth_complete.
    """
    WebSocketAuthManager.clear_instance()
    manager = ConnectionManagerStub()
    router = create_authenticated_websocket_router(manager)
    websocket = WebSocketStub(
        headers={"origin": "http://localhost:8000"},
        cookies={"access_token": "token"},
    )
    user = SimpleNamespace(username="alice")
    auth_result = SimpleNamespace(success=True, user=user, ws_payload=None)

    async def fake_authenticate(*args: Any, **kwargs: Any) -> Any:
        return auth_result

    monkeypatch.setattr(
        "snapper.server.authenticated_websocket.authenticate_websocket", fake_authenticate
    )
    send_calls: list[Any] = []
    dispatch_calls: list[Any] = []

    async def fake_send_auth_complete(*args: Any, **kwargs: Any) -> None:
        send_calls.append(True)

    async def fake_dispatch_messages(*args: Any, **kwargs: Any) -> None:
        dispatch_calls.append(True)

    monkeypatch.setattr(
        "snapper.server.authenticated_websocket.send_auth_complete", fake_send_auth_complete
    )
    monkeypatch.setattr(
        "snapper.server.authenticated_websocket.dispatch_messages", fake_dispatch_messages
    )

    async def allow_origin(*args: Any, **kwargs: Any) -> bool:
        return True

    monkeypatch.setattr("snapper.server.authenticated_websocket.validate_origin", allow_origin)
    endpoint = router.routes[0].endpoint
    await endpoint(websocket)
    assert websocket.accepted
    assert manager.connected == [(websocket, False)]
    assert manager.disconnected == [websocket]
    assert send_calls == []
    assert dispatch_calls == [True]
    WebSocketAuthManager.clear_instance()


@pytest.mark.asyncio
async def test_handshake_invalid_json_message() -> None:
    """Handshake fails with invalid JSON auth message.

    Given: A WebSocket with valid cookie,
    When: Client sends malformed JSON,
    Then: Closes connection with invalid_json reason.
    """
    WebSocketAuthManager.clear_instance()
    token_data = TokenClaims(
        sub="user-1",
        username="alice",
        role=UserRole.OPERATOR,
        permissions=["read:orders"],
        exp=int(datetime.now(UTC).timestamp()) + 600,
        iat=int(datetime.now(UTC).timestamp()),
        jti="token-1",
        sid="session-123",
    )
    with patch("snapper.auth.websocket_auth.get_token_manager") as mock_get_token:
        mock_token_manager = MagicMock()
        mock_token_manager.verify_token.return_value = token_data
        mock_get_token.return_value = mock_token_manager
        manager = ConnectionManagerStub()
        router = create_authenticated_websocket_router(manager)
        websocket = WebSocketStub(
            headers={"origin": "http://localhost:8000"},
            cookies={"access_token": "valid_token"},
            messages=["not valid json"],
        )
        endpoint = router.routes[0].endpoint
        await endpoint(websocket)
    assert websocket.accepted
    assert len(websocket.closed) == 1
    assert websocket.closed[0] == (4401, "Invalid auth payload")
    sent_messages = [json.loads(msg) for msg in websocket.sent]
    assert any(
        msg.get("type") == "auth_failed" and msg.get("reason") == "invalid_json"
        for msg in sent_messages
    )
    WebSocketAuthManager.clear_instance()


@pytest.mark.asyncio
async def test_handshake_missing_ws_token() -> None:
    """Handshake fails when ws_token missing from auth message.

    Given: A WebSocket with valid cookie,
    When: Client sends authenticate message without ws_token,
    Then: Closes connection with invalid auth payload reason.
    """
    WebSocketAuthManager.clear_instance()
    token_data = TokenClaims(
        sub="user-1",
        username="alice",
        role=UserRole.OPERATOR,
        permissions=["read:orders"],
        exp=int(datetime.now(UTC).timestamp()) + 600,
        iat=int(datetime.now(UTC).timestamp()),
        jti="token-1",
        sid="session-123",
    )
    with patch("snapper.auth.websocket_auth.get_token_manager") as mock_get_token:
        mock_token_manager = MagicMock()
        mock_token_manager.verify_token.return_value = token_data
        mock_get_token.return_value = mock_token_manager
        manager = ConnectionManagerStub()
        router = create_authenticated_websocket_router(manager)
        websocket = WebSocketStub(
            headers={"origin": "http://localhost:8000"},
            cookies={"access_token": "valid_token"},
            messages=[json.dumps({"type": "authenticate", "session_id": "", "sequence_id": 0})],
        )
        endpoint = router.routes[0].endpoint
        await endpoint(websocket)
    assert websocket.accepted
    assert len(websocket.closed) == 1
    assert websocket.closed[0] == (4401, "Invalid auth payload")
    WebSocketAuthManager.clear_instance()


@pytest.mark.asyncio
async def test_handshake_invalid_ws_token() -> None:
    """Handshake fails with invalid ws_token.

    Given: A WebSocket with valid cookie,
    When: Client sends invalid ws_token,
    Then: Closes connection with Invalid ws_token reason.
    """
    WebSocketAuthManager.clear_instance()
    token_data = TokenClaims(
        sub="user-1",
        username="alice",
        role=UserRole.OPERATOR,
        permissions=["read:orders"],
        exp=int(datetime.now(UTC).timestamp()) + 600,
        iat=int(datetime.now(UTC).timestamp()),
        jti="token-1",
        sid="session-123",
    )
    with patch("snapper.auth.websocket_auth.get_token_manager") as mock_get_token:
        mock_token_manager = MagicMock()
        mock_token_manager.verify_token.return_value = token_data
        mock_get_token.return_value = mock_token_manager
        manager = ConnectionManagerStub()
        router = create_authenticated_websocket_router(manager)
        websocket = WebSocketStub(
            headers={"origin": "http://localhost:8000"},
            cookies={"access_token": "valid_token"},
            messages=[
                json.dumps(
                    {
                        "type": "authenticate",
                        "session_id": "",
                        "sequence_id": 0,
                        "ws_token": "invalid_token",
                    }
                )
            ],
        )
        with patch("snapper.server.authenticated_websocket.get_ws_token_service") as mock_token_svc:
            mock_token_service = MagicMock()
            mock_token_service.verify.side_effect = WsTokenError("Invalid token")
            mock_token_svc.return_value = mock_token_service
            endpoint = router.routes[0].endpoint
            await endpoint(websocket)
    assert websocket.accepted
    assert len(websocket.closed) == 1
    assert websocket.closed[0] == (4401, "Invalid ws_token")
    WebSocketAuthManager.clear_instance()


@pytest.mark.asyncio
async def test_bridge_creation_when_manager_zmq_bridge_is_none() -> None:
    """ZMQ bridge is created when manager has none.

    Given: A connection manager with no zmq_bridge,
    When: Processing WebSocket authentication,
    Then: Creates and attaches ZmqWebSocketBridgeService.
    """
    WebSocketAuthManager.clear_instance()
    manager = ConnectionManagerStub(bridge=None)
    assert manager.zmq_bridge is None
    router = create_authenticated_websocket_router(manager)
    websocket = WebSocketStub(
        headers={"origin": "http://localhost:8000"},
        cookies={"access_token": "valid_token"},
        messages=[
            json.dumps(
                {
                    "type": "authenticate",
                    "session_id": "",
                    "sequence_id": 0,
                    "ws_token": "valid_token",
                }
            ),
            WebSocketDisconnect(),
        ],
    )
    token_data = TokenClaims(
        sub="user-1",
        username="alice",
        role=UserRole.OPERATOR,
        permissions=["read:orders"],
        exp=int(datetime.now(UTC).timestamp()) + 600,
        iat=int(datetime.now(UTC).timestamp()),
        jti="token-1",
        sid="session-123",
    )
    ws_payload = WsTokenPayload(
        purpose="ws_connect",
        sub="user-1",
        sid_hash="hash123",
        iat=int(datetime.now(UTC).timestamp()),
        exp=int(datetime.now(UTC).timestamp()) + 600,
        jti="ws-1",
    )
    with patch("snapper.server.authenticated_websocket.get_ws_auth_manager") as mock_auth:
        mock_auth_manager = MagicMock()
        user = AuthPrincipal(username="alice", role=UserRole.OPERATOR)
        mock_auth_manager.verify_session_cookie.return_value = (user, token_data)
        mock_auth_manager.get_connection_expiration.return_value = datetime.now(UTC)
        mock_auth.return_value = mock_auth_manager
        with patch("snapper.server.authenticated_websocket.get_ws_token_service") as mock_token_svc:
            mock_token_service = MagicMock()
            mock_token_service.verify.return_value = ws_payload
            mock_token_svc.return_value = mock_token_service
            with patch(
                "snapper.server.authenticated_websocket.get_allowed_topics_for_role"
            ) as mock_topics:
                mock_topics.return_value = []
                endpoint = router.routes[0].endpoint
                await endpoint(websocket)
    assert manager.attached_bridge is not None
    assert isinstance(manager.attached_bridge, ZmqWebSocketBridgeService)
    WebSocketAuthManager.clear_instance()


@pytest.mark.asyncio
async def test_websocket_endpoint_handles_unexpected_exception() -> None:
    """WebSocket endpoint handles unexpected exception gracefully.

    Given: A valid authentication setup,
    When: send_auth_complete raises RuntimeError,
    Then: Disconnects client cleanly.
    """
    WebSocketAuthManager.clear_instance()
    token_data = TokenClaims(
        sub="user-1",
        username="alice",
        role=UserRole.OPERATOR,
        permissions=["read:orders"],
        exp=int(datetime.now(UTC).timestamp()) + 600,
        iat=int(datetime.now(UTC).timestamp()),
        jti="token-1",
        sid="session-123",
    )
    ws_payload = WsTokenPayload(
        purpose="ws_connect",
        sub="user-1",
        sid_hash="hash123",
        iat=int(datetime.now(UTC).timestamp()),
        exp=int(datetime.now(UTC).timestamp()) + 600,
        jti="ws-1",
    )
    with patch("snapper.server.authenticated_websocket.get_ws_auth_manager") as mock_auth:
        mock_auth_manager = MagicMock()
        user = AuthPrincipal(username="alice", role=UserRole.OPERATOR)
        mock_auth_manager.verify_session_cookie.return_value = (user, token_data)
        mock_auth_manager.get_connection_expiration.return_value = datetime.now(UTC)
        mock_auth.return_value = mock_auth_manager
        with patch("snapper.server.authenticated_websocket.get_ws_token_service") as mock_token_svc:
            mock_token_service = MagicMock()
            mock_token_service.verify.return_value = ws_payload
            mock_token_svc.return_value = mock_token_service
            manager = ConnectionManagerStub()
            router = create_authenticated_websocket_router(manager)
            websocket = WebSocketStub(
                headers={"origin": "http://localhost:8000"},
                cookies={"access_token": "valid-token"},
                messages=[
                    json.dumps(
                        {
                            "type": "authenticate",
                            "session_id": "",
                            "sequence_id": 0,
                            "ws_token": "valid_token",
                        }
                    ),
                ],
            )
            with patch(
                "snapper.server.authenticated_websocket.get_allowed_topics_for_role"
            ) as mock_topics:
                mock_topics.return_value = []
                with patch(
                    "snapper.server.authenticated_websocket.send_auth_complete"
                ) as mock_send_auth:
                    mock_send_auth.side_effect = RuntimeError("Simulated internal error")
                    endpoint = router.routes[0].endpoint
                    await endpoint(websocket)
    assert len(manager.disconnected) == 1
    WebSocketAuthManager.clear_instance()


@pytest.mark.asyncio
async def test_websocket_endpoint_handles_disconnect_during_auth() -> None:
    """WebSocket endpoint catches WebSocketDisconnect during authentication.

    Given: A WebSocket connection from an allowed origin,
    When: The client disconnects during the authentication flow,
    Then: The except WebSocketDisconnect handler logs and completes cleanly.
    """
    WebSocketAuthManager.clear_instance()
    with patch("snapper.server.authenticated_websocket.get_ws_auth_manager") as mock_auth:
        mock_auth_manager = MagicMock()
        mock_auth.return_value = mock_auth_manager
        with patch("snapper.server.authenticated_websocket.get_ws_token_service") as mock_token_svc:
            mock_token_svc.return_value = MagicMock()
            manager = ConnectionManagerStub()
            router = create_authenticated_websocket_router(manager)
            websocket = WebSocketStub(
                headers={"origin": "http://localhost:8000"},
                cookies={"access_token": "valid_token"},
                messages=[],
            )
            with patch(
                "snapper.server.authenticated_websocket.authenticate_websocket",
                new_callable=AsyncMock,
            ) as mock_authenticate:
                mock_authenticate.side_effect = WebSocketDisconnect()
                endpoint = router.routes[0].endpoint
                await endpoint(websocket)
    assert websocket.accepted
    assert len(manager.disconnected) == 0
    mock_auth_manager.disconnect.assert_called_once_with(websocket)
    WebSocketAuthManager.clear_instance()


AUTH_WS_MODULE = cast(Any, auth_ws)
AUTH_HANDLERS_MODULE = cast(Any, auth_handlers)


class DummyTask:
    """Dummy asyncio task for testing."""

    def __init__(self, coro: Any) -> None:
        """Initialize the instance."""
        self._coro = coro
        close = getattr(coro, "close", None)
        if callable(close):
            close()

    def cancel(self) -> None:
        """Cancel the task (no-op for stub)."""
        pass


def datetime_from_timestamp(value: int | None) -> datetime:
    """Convert Unix timestamp to datetime, or return now if None."""
    if value is None:
        return cast(datetime, AUTH_HANDLERS_MODULE.datetime.now(UTC))
    return cast(datetime, AUTH_HANDLERS_MODULE.datetime.fromtimestamp(value, UTC))


class EndpointWebSocketStub:
    """WebSocket stub for endpoint testing."""

    def __init__(
        self,
        *,
        headers: dict[str, str] | None = None,
        cookies: dict[str, str] | None = None,
        messages: list[Any] | None = None,
        settings: SimpleNamespace | None = None,
    ) -> None:
        """Initialize the instance."""
        self.headers = headers or {}
        self.cookies = cookies or {}
        self._messages = messages or []
        self.accepted = False
        self.sent: list[str] = []
        self.closed: list[tuple[int | None, str | None]] = []
        self.client = ("127.0.0.1", 8080)
        self.app = SimpleNamespace(
            state=SimpleNamespace(
                settings=settings or SimpleNamespace(ui_origin="", session_domain=""),
            )
        )

    async def accept(self) -> None:
        """Accept the WebSocket connection."""
        self.accepted = True

    async def send_text(self, payload: str) -> None:
        """Send text payload to client."""
        self.sent.append(payload)

    async def close(self, code: int | None = None, reason: str | None = None) -> None:
        """Close the WebSocket connection."""
        self.closed.append((code, reason))

    async def receive_text(self) -> str:
        """Receive text from client or raise disconnect."""
        if not self._messages:
            raise WebSocketDisconnect()
        item = self._messages.pop(0)
        if isinstance(item, Exception):
            raise item
        return str(item)


class EndpointManagerStub:
    """Connection manager stub for endpoint testing."""

    def __init__(self, *, bridge: Any | None = None) -> None:
        """Initialize the instance."""
        self.zmq_bridge = bridge
        self.attached_bridge: Any | None = None
        self.connected: list[tuple[Any, bool]] = []
        self.disconnected: list[Any] = []
        self._tracker = SequenceTracker()

    @property
    def tracker(self) -> SequenceTracker:
        """Provide sequence tracker for provenance stamping."""
        return self._tracker

    def attach_bridge(self, bridge: Any) -> None:
        """Attach ZMQ bridge to manager."""
        self.attached_bridge = bridge
        self.zmq_bridge = bridge

    async def connect(self, websocket: Any, *, accept: bool = True) -> None:
        """Record WebSocket connection."""
        self.connected.append((websocket, accept))

    async def disconnect(self, websocket: Any) -> None:
        """Record WebSocket disconnection."""
        self.disconnected.append(websocket)


class AuthManagerStub:
    """Auth manager stub for testing."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.session_result: tuple[Any, Any] | None = None
        self.state = SimpleNamespace(session_id="session-1")
        self.registered: list[tuple[Any, Any, Any, Any]] = []
        self.updated: list[Any] = []
        self.disconnect_calls: list[Any] = []
        self.expiration = datetime_from_timestamp(None)

    def verify_session_cookie(self, websocket: Any) -> tuple[Any, Any] | None:
        """Verify session cookie and return result."""
        return self.session_result

    def register_connection(
        self,
        websocket: Any,
        user: Any,
        token_data: Any,
        ws_payload: Any,
        *,
        warn_task: Any,
        hard_task: Any,
    ) -> None:
        """Register a WebSocket connection."""
        self.registered.append((websocket, user, token_data, ws_payload))

    def get_connection_expiration(self, websocket: Any) -> Any:
        """Return connection expiration time."""
        return self.expiration

    def get_state(self, websocket: Any) -> Any:
        """Return connection state."""
        return self.state

    def update_ws_token_state(
        self,
        websocket: Any,
        new_payload: Any,
        *,
        warn_task: Any,
        hard_task: Any,
    ) -> None:
        """Update WebSocket token state."""
        self.updated.append(new_payload)

    def disconnect(self, websocket: Any) -> None:
        """Record WebSocket disconnection."""
        self.disconnect_calls.append(websocket)


class TokenServiceStub:
    """Token service stub for testing."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.verify_handler: Any | None = None
        self.marked: list[Any] = []

    def verify(self, token: str, *, expected_sub: str, expected_sid_hash: str) -> Any:
        """Verify token and return payload."""
        if callable(self.verify_handler):
            return self.verify_handler(token, expected_sub, expected_sid_hash)
        if isinstance(self.verify_handler, Exception):
            raise self.verify_handler
        return SimpleNamespace(exp=int(datetime.now(UTC).timestamp()))

    def mark_used(self, payload: Any) -> None:
        """Mark token payload as used."""
        self.marked.append(payload)


@pytest.fixture
def endpoint_factory(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Provide factory for creating WebSocket endpoints with stubs."""

    def _factory(
        manager: EndpointManagerStub,
        auth_stub: AuthManagerStub | None = None,
        token_stub: TokenServiceStub | None = None,
    ) -> tuple[Any, AuthManagerStub, TokenServiceStub]:
        auth_instance = auth_stub or AuthManagerStub()
        token_instance = token_stub or TokenServiceStub()
        settings_stub = SimpleNamespace(
            server_port=1234,
            ui_origin="",
            session_domain="",
            db_url=BootstrapSettingsLoader().db_url,
        )
        monkeypatch.setattr(auth_ws, "get_ws_auth_manager", lambda: auth_instance)
        monkeypatch.setattr(auth_ws, "get_ws_token_service", lambda: token_instance)
        monkeypatch.setattr(auth_ws, "get_settings", lambda: settings_stub)

        def fake_bridge_factory(_manager: Any) -> str:
            return "bridge"

        def fake_allowed_topics(_manager: Any, _role: UserRole) -> list[str]:
            return ["market.kraken.BTC-USD.candles.1m"]

        def fake_create_task(coro: Any) -> DummyTask:
            return DummyTask(coro)

        monkeypatch.setattr(auth_ws, "ZmqWebSocketBridgeService", fake_bridge_factory)
        monkeypatch.setattr(auth_ws, "get_allowed_topics_for_role", fake_allowed_topics)
        auth_handlers_asyncio = AUTH_HANDLERS_MODULE.asyncio
        monkeypatch.setattr(auth_handlers_asyncio, "create_task", fake_create_task)

        async def _noop_sleep(*_args: Any, **_kwargs: Any) -> None:
            return None

        monkeypatch.setattr(auth_handlers_asyncio, "sleep", _noop_sleep)
        router = create_authenticated_websocket_router(cast(Any, manager))
        endpoint = cast(Any, router.routes[0]).endpoint
        return endpoint, auth_instance, token_instance

    return _factory


@pytest.mark.asyncio
async def test_ws_endpoint_rejects_forbidden_origin(endpoint_factory: Any) -> None:
    """WebSocket endpoint rejects forbidden origin.

    Given: A WebSocket from malicious.example origin,
    When: Attempting connection,
    Then: Closes with origin_forbidden and 4403 code.
    """
    manager = EndpointManagerStub()
    endpoint, auth_stub, _ = endpoint_factory(manager)
    auth_stub.session_result = None
    websocket = EndpointWebSocketStub(headers={"origin": "https://malicious.example"})
    await endpoint(websocket)
    assert json.loads(websocket.sent[-1])["reason"] == "origin_forbidden"
    assert websocket.closed[-1][0] == 4403
    assert manager.attached_bridge is None


@pytest.mark.asyncio
async def test_ws_endpoint_attaches_bridge_and_handles_missing_cookie(
    endpoint_factory: Any,
) -> None:
    """Endpoint attaches bridge and handles missing cookie.

    Given: A WebSocket from localhost without session cookie,
    When: Attempting connection,
    Then: Attaches bridge and closes with missing_cookie reason.
    """
    manager = EndpointManagerStub()
    endpoint, auth_stub, _ = endpoint_factory(manager)
    auth_stub.session_result = None
    websocket = EndpointWebSocketStub(headers={"origin": "http://localhost:8000"})
    await endpoint(websocket)
    assert manager.attached_bridge == "bridge"
    assert json.loads(websocket.sent[-1])["reason"] == "missing_cookie"
    assert websocket.closed[-1][0] == 4401


@pytest.mark.asyncio
async def test_ws_endpoint_timeout_during_auth(
    endpoint_factory: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Endpoint handles authentication timeout.

    Given: A WebSocket with valid session,
    When: wait_for raises TimeoutError,
    Then: Closes with timeout reason and 4408 code.
    """
    manager = EndpointManagerStub(bridge="existing")
    endpoint, auth_stub, _ = endpoint_factory(manager)
    auth_stub.session_result = (
        SimpleNamespace(id="user-1", role=UserRole.VIEWER, username="u1"),
        SimpleNamespace(sid="sid-1"),
    )

    class _ImmediateTimeout:
        """Context manager that immediately raises TimeoutError."""

        def __init__(self, _delay: float | None) -> None:
            pass

        async def __aenter__(self) -> _ImmediateTimeout:
            raise TimeoutError()

        async def __aexit__(self, *_args: object) -> None:
            pass

    auth_handlers_asyncio = AUTH_HANDLERS_MODULE.asyncio
    monkeypatch.setattr(auth_handlers_asyncio, "timeout", cast(Any, _ImmediateTimeout))
    websocket = EndpointWebSocketStub(headers={"origin": "http://localhost:8000"})
    await endpoint(websocket)
    assert json.loads(websocket.sent[-1])["reason"] == "timeout"
    assert websocket.closed[-1][0] == 4408
    assert manager.connected == []


@pytest.mark.asyncio
async def test_ws_endpoint_rejects_invalid_json(
    endpoint_factory: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Endpoint rejects invalid JSON message.

    Given: A WebSocket with valid session,
    When: Client sends malformed JSON,
    Then: Closes with invalid_json reason and 4401 code.
    """
    manager = EndpointManagerStub(bridge="existing")
    endpoint, auth_stub, _ = endpoint_factory(manager)
    auth_stub.session_result = (
        SimpleNamespace(id="user-2", role=UserRole.VIEWER, username="u2"),
        SimpleNamespace(sid="sid-2"),
    )

    class _PassthroughTimeout:
        """Context manager that does not enforce any timeout."""

        def __init__(self, _delay: float | None) -> None:
            pass

        async def __aenter__(self) -> _PassthroughTimeout:
            return self

        async def __aexit__(self, *_args: object) -> None:
            pass

    auth_handlers_asyncio = AUTH_HANDLERS_MODULE.asyncio
    monkeypatch.setattr(auth_handlers_asyncio, "timeout", cast(Any, _PassthroughTimeout))
    websocket = EndpointWebSocketStub(
        headers={"origin": "http://localhost:8000"},
        messages=["{"],
    )
    await endpoint(websocket)
    assert json.loads(websocket.sent[-1])["reason"] == "invalid_json"
    assert websocket.closed[-1][0] == 4401


@pytest.mark.asyncio
async def test_ws_endpoint_rejects_invalid_message_type(
    endpoint_factory: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Endpoint rejects invalid message type.

    Given: A WebSocket with valid session,
    When: Client sends empty JSON object,
    Then: Closes with auth_failed type and 4401 code.
    """
    manager = EndpointManagerStub(bridge="existing")
    endpoint, auth_stub, _ = endpoint_factory(manager)
    auth_stub.session_result = (
        SimpleNamespace(id="user-3", role=UserRole.VIEWER, username="u3"),
        SimpleNamespace(sid="sid-3"),
    )

    class _PassthroughTimeout:
        """Context manager that does not enforce any timeout."""

        def __init__(self, _delay: float | None) -> None:
            pass

        async def __aenter__(self) -> _PassthroughTimeout:
            return self

        async def __aexit__(self, *_args: object) -> None:
            pass

    auth_handlers_asyncio = AUTH_HANDLERS_MODULE.asyncio
    monkeypatch.setattr(auth_handlers_asyncio, "timeout", cast(Any, _PassthroughTimeout))
    websocket = EndpointWebSocketStub(
        headers={"origin": "http://localhost:8000"},
        messages=["{}"],
    )
    await endpoint(websocket)
    assert websocket.closed[-1][0] == 4401
    assert json.loads(websocket.sent[-1])["type"] == "auth_failed"


@pytest.mark.asyncio
async def test_ws_endpoint_requires_ws_token(
    endpoint_factory: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Endpoint requires ws_token in authenticate message.

    Given: A WebSocket with valid session,
    When: Client sends authenticate without ws_token,
    Then: Closes with Invalid auth payload reason.
    """
    manager = EndpointManagerStub(bridge="existing")
    endpoint, auth_stub, _ = endpoint_factory(manager)
    auth_stub.session_result = (
        SimpleNamespace(id="user-4", role=UserRole.OPERATOR, username="u4"),
        SimpleNamespace(sid="sid-4"),
    )

    class _PassthroughTimeout:
        """Context manager that does not enforce any timeout."""

        def __init__(self, _delay: float | None) -> None:
            pass

        async def __aenter__(self) -> _PassthroughTimeout:
            return self

        async def __aexit__(self, *_args: object) -> None:
            pass

    auth_handlers_asyncio = AUTH_HANDLERS_MODULE.asyncio
    monkeypatch.setattr(auth_handlers_asyncio, "timeout", cast(Any, _PassthroughTimeout))
    websocket = EndpointWebSocketStub(
        headers={"origin": "http://localhost:8000"},
        messages=['{"type": "authenticate", "session_id": "", "sequence_id": 0}'],
    )
    await endpoint(websocket)
    assert json.loads(websocket.sent[-1])["type"] == "auth_failed"
    assert websocket.closed[-1][1] == "Invalid auth payload"


@pytest.mark.asyncio
async def test_ws_endpoint_handles_token_replay(
    endpoint_factory: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Endpoint handles ws_token replay attack.

    Given: A WebSocket with valid session,
    When: Client sends already-used ws_token,
    Then: Closes with ws_token replay reason.
    """
    manager = EndpointManagerStub(bridge="existing")
    endpoint, auth_stub, token_stub = endpoint_factory(manager)
    auth_stub.session_result = (
        SimpleNamespace(id="user-5", role=UserRole.OPERATOR, username="u5"),
        SimpleNamespace(sid="sid-5"),
    )
    token_stub.verify_handler = WsTokenAlreadyUsedError()

    class _PassthroughTimeout:
        """Context manager that does not enforce any timeout."""

        def __init__(self, _delay: float | None) -> None:
            pass

        async def __aenter__(self) -> _PassthroughTimeout:
            return self

        async def __aexit__(self, *_args: object) -> None:
            pass

    auth_handlers_asyncio = AUTH_HANDLERS_MODULE.asyncio
    monkeypatch.setattr(auth_handlers_asyncio, "timeout", cast(Any, _PassthroughTimeout))
    websocket = EndpointWebSocketStub(
        headers={"origin": "http://localhost:8000"},
        messages=[
            '{"type": "authenticate", "session_id": "", "sequence_id": 0, "ws_token": "token"}'
        ],
    )
    await endpoint(websocket)
    assert json.loads(websocket.sent[-1])["type"] == "auth_failed"
    assert websocket.closed[-1][1] == "ws_token replay"


@pytest.mark.asyncio
async def test_ws_endpoint_handles_invalid_token(
    endpoint_factory: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Endpoint handles invalid ws_token.

    Given: A WebSocket with valid session,
    When: Client sends invalid ws_token,
    Then: Closes with Invalid ws_token reason.
    """
    manager = EndpointManagerStub(bridge="existing")
    endpoint, auth_stub, token_stub = endpoint_factory(manager)
    auth_stub.session_result = (
        SimpleNamespace(id="user-6", role=UserRole.OPERATOR, username="u6"),
        SimpleNamespace(sid="sid-6"),
    )
    token_stub.verify_handler = WsTokenError()

    class _PassthroughTimeout:
        """Context manager that does not enforce any timeout."""

        def __init__(self, _delay: float | None) -> None:
            pass

        async def __aenter__(self) -> _PassthroughTimeout:
            return self

        async def __aexit__(self, *_args: object) -> None:
            pass

    auth_handlers_asyncio = AUTH_HANDLERS_MODULE.asyncio
    monkeypatch.setattr(auth_handlers_asyncio, "timeout", cast(Any, _PassthroughTimeout))
    websocket = EndpointWebSocketStub(
        headers={"origin": "http://localhost:8000"},
        messages=[
            '{"type": "authenticate", "session_id": "", "sequence_id": 0, "ws_token": "token"}'
        ],
    )
    await endpoint(websocket)
    assert websocket.closed[-1][1] == "Invalid ws_token"


@pytest.mark.asyncio
async def test_ws_endpoint_success_and_reauth_flow(
    endpoint_factory: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Endpoint handles successful auth and reauth flow.

    Given: A WebSocket with valid credentials,
    When: Authenticating then re-authenticating,
    Then: Completes both auth and reauth successfully.
    """
    manager = EndpointManagerStub(bridge="existing")
    endpoint, auth_stub, token_stub = endpoint_factory(manager)
    now_exp = int(datetime.now(UTC).timestamp())
    auth_stub.session_result = (
        SimpleNamespace(id="user-7", role=UserRole.ADMIN, username="u7"),
        SimpleNamespace(sid="sid-7"),
    )
    auth_stub.expiration = datetime_from_timestamp(now_exp)

    def verify_handler(token: str, _sub: str, _sid_hash: str) -> Any:
        if token == "valid-token":
            return SimpleNamespace(exp=now_exp)
        if token == "reauth-token":
            return SimpleNamespace(exp=now_exp + 120)
        raise AssertionError(f"Unexpected token {token}")

    token_stub.verify_handler = verify_handler

    class _PassthroughTimeout:
        """Context manager that does not enforce any timeout."""

        def __init__(self, _delay: float | None) -> None:
            pass

        async def __aenter__(self) -> _PassthroughTimeout:
            return self

        async def __aexit__(self, *_args: object) -> None:
            pass

    auth_handlers_asyncio = AUTH_HANDLERS_MODULE.asyncio
    monkeypatch.setattr(auth_handlers_asyncio, "timeout", cast(Any, _PassthroughTimeout))
    websocket = EndpointWebSocketStub(
        headers={"origin": "http://localhost:8000"},
        messages=[
            '{"type": "authenticate", "session_id": "", "sequence_id": 0, "ws_token": "valid-token"}',
            '{"type": "reauth", "session_id": "", "sequence_id": 0, "ws_token": "reauth-token"}',
            WebSocketDisconnect(),
        ],
        settings=SimpleNamespace(ui_origin="https://ui.example/", session_domain="example.com "),
    )
    await endpoint(websocket)
    assert manager.connected
    assert manager.disconnected
    assert any(msg for msg in websocket.sent if "auth_complete" in msg)
    assert any(msg for msg in websocket.sent if "reauth_ok" in msg)
    assert auth_stub.updated
    assert token_stub.marked
    assert websocket.accepted
    assert auth_stub.disconnect_calls


FILTER_TOPICS: Any = filter_topics
DETERMINE_TOPIC_CATEGORY: Any = determine_topic_category
HANDLE_PING: Any = handle_ping
HANDLE_GET_SUBSCRIPTIONS: Any = handle_get_subscriptions
HANDLE_SUBSCRIBE: Any = handle_subscribe
HANDLE_UNSUBSCRIBE: Any = handle_unsubscribe


class WebSocketStubV2:
    """Simplified WebSocket stub for handler testing."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.sent: list[str] = []
        self.closed: list[tuple[int | None, str | None]] = []

    async def send_text(self, payload: str) -> None:
        """Send text payload to client."""
        self.sent.append(payload)

    async def close(self, code: int | None = None, reason: str | None = None) -> None:
        """Close the WebSocket connection."""
        self.closed.append((code, reason))


class BridgeStub:
    """ZMQ bridge stub for testing."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.added: list[tuple[Any, list[str]]] = []
        self.removed: list[tuple[Any, list[str]]] = []

    async def add_subscription(self, websocket: Any, topics: list[str]) -> None:
        """Record subscription addition."""
        self.added.append((websocket, topics))

    async def remove_subscription(self, websocket: Any, topics: list[str]) -> None:
        """Record subscription removal."""
        self.removed.append((websocket, topics))


class ManagerStub:
    """Connection manager stub for handler testing."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.active_connections: list[Any] = [object()]
        self._subscriptions: dict[Any, set[str]] = {}
        self.zmq_bridge = BridgeStub()
        self._tracker = SequenceTracker()

    @property
    def tracker(self) -> SequenceTracker:
        """Provide sequence tracker for provenance stamping."""
        return self._tracker

    def set_subscriptions(self, websocket: Any, topics: set[str]) -> None:
        """Set subscriptions for a WebSocket."""
        self._subscriptions[websocket] = topics

    def get_client_subscriptions(self, websocket: Any) -> set[str]:
        """Get subscriptions for a WebSocket."""
        return self._subscriptions.get(websocket, set())

    def subscribe_client(self, websocket: Any, topic: str) -> None:
        """Subscribe client to a topic."""
        self._subscriptions.setdefault(websocket, set()).add(topic)

    def unsubscribe_client(self, websocket: Any, topic: str) -> None:
        """Unsubscribe client from a topic."""
        if websocket in self._subscriptions:
            self._subscriptions[websocket].discard(topic)


def test_filter_topics_respects_allowed_sets() -> None:
    """Filter topics respects allowed sets and categories.

    Given: Allowed topics and categories sets,
    When: Filtering mixed topic list,
    Then: Returns allowed and denied lists correctly.
    """
    allowed_topics = {"market.kraken.BTC-USD.candles.1m"}
    allowed_categories = {"strategy"}
    topics = [
        "market.kraken.BTC-USD.candles.1m",
        "signals.kraken.BTC-USD.live",
        "orders.commands.kraken.BTC-USD.submit",
        "signals.kraken.BTC-USD.live",
    ]
    allowed, denied = FILTER_TOPICS(
        topics,
        allowed_topics,
        allowed_categories,
    )
    assert allowed == [
        "market.kraken.BTC-USD.candles.1m",
        "signals.kraken.BTC-USD.live",
    ]
    assert denied == ["orders.commands.kraken.BTC-USD.submit"]


def test_determine_topic_category_fallbacks() -> None:
    """Determine topic category maps prefixes correctly.

    Given: Various topic strings,
    When: Determining category,
    Then: Returns correct category or None for unknown.
    """
    assert DETERMINE_TOPIC_CATEGORY("market.kraken.BTC-USD.candles.1m") == "market"
    assert DETERMINE_TOPIC_CATEGORY("trade.kraken.BTC-USD.live") is None
    assert DETERMINE_TOPIC_CATEGORY("orders.commands.kraken.BTC-USD.submit") == "trade"
    assert DETERMINE_TOPIC_CATEGORY("orders.events.kraken.BTC-USD.executed") == "trade"
    assert DETERMINE_TOPIC_CATEGORY("unknown.topic") is None
    assert DETERMINE_TOPIC_CATEGORY("candle") == "market"
    assert DETERMINE_TOPIC_CATEGORY("tick") == "market"
    assert DETERMINE_TOPIC_CATEGORY("execution") == "trade"
    assert DETERMINE_TOPIC_CATEGORY("heartbeat") == "system"
    assert DETERMINE_TOPIC_CATEGORY("unknown") is None


def test_determine_topic_category_edge_cases() -> None:
    """Determine topic category handles edge cases correctly.

    Given: Edge case topic strings (single segment, empty-ish, two-level prefixes),
    When: Determining category,
    Then: Returns correct category with no regression.
    """
    assert DETERMINE_TOPIC_CATEGORY("market") == "market"
    assert DETERMINE_TOPIC_CATEGORY("market.") == "market"
    assert DETERMINE_TOPIC_CATEGORY("signals") == "strategy"
    assert DETERMINE_TOPIC_CATEGORY("signals.macd") == "strategy"
    assert DETERMINE_TOPIC_CATEGORY("system") == "system"
    assert DETERMINE_TOPIC_CATEGORY("system.heartbeats.executor.kraken") == "system"
    assert DETERMINE_TOPIC_CATEGORY("admin") == "admin"
    assert DETERMINE_TOPIC_CATEGORY("admin.users") == "admin"
    assert DETERMINE_TOPIC_CATEGORY("orders") is None
    assert DETERMINE_TOPIC_CATEGORY("orders.unknown.kraken") is None
    assert DETERMINE_TOPIC_CATEGORY("") is None


@pytest.mark.asyncio
async def test_handle_ping_reports_active_connections() -> None:
    """Handle ping reports active connection count.

    Given: A manager with one active connection,
    When: Handling ping message,
    Then: Returns pong with active_connections count.
    """
    websocket = WebSocketStub()
    manager = ManagerStub()
    await HANDLE_PING(cast(Any, websocket), manager)
    payload = json.loads(websocket.sent[-1])
    assert payload["type"] == "pong"
    assert payload["active_connections"] == 1


@pytest.mark.asyncio
async def test_handle_get_subscriptions_uses_helper() -> None:
    """Handle get_subscriptions returns current and available topics.

    Given: A WebSocket with active subscription,
    When: Handling get_subscriptions,
    Then: Returns current subscriptions and available topics.
    """
    websocket = WebSocketStub()
    manager = ManagerStub()
    manager.set_subscriptions(websocket, {"signals.kraken.BTC-USD.live"})
    with patch(
        "snapper.interface.websocket.handlers.subscribe.get_allowed_topics_for_role",
        return_value=["market.kraken.BTC-USD.candles.1m"],
    ):
        await HANDLE_GET_SUBSCRIPTIONS(cast(Any, websocket), manager, UserRole.OPERATOR)
    response = json.loads(websocket.sent[-1])
    assert response["type"] == "subscriptions_list"
    assert response["subscriptions"] == ["signals.kraken.BTC-USD.live"]
    assert response["available_topics"] == ["market.kraken.BTC-USD.candles.1m"]


def test_has_trading_permission_roles() -> None:
    """Trading permission based on user role.

    Given: Different user roles,
    When: Checking trading permission,
    Then: Viewer denied, operator and admin granted.
    """
    assert not has_trading_permission(UserRole.VIEWER)
    assert has_trading_permission(UserRole.OPERATOR)
    assert has_trading_permission(UserRole.ADMIN)


@pytest.mark.asyncio
async def test_handle_subscribe_reports_invalid_topics() -> None:
    """Subscribe reports invalid topic format.

    Given: Invalid subscription pattern,
    When: Handling subscribe,
    Then: Returns error with invalid topic details.
    """
    websocket = WebSocketStub()
    manager = ManagerStub()
    with patch(
        "snapper.interface.websocket.handlers.subscribe.validate_subscription_pattern",
        return_value=(False, "bad"),
    ):
        await HANDLE_SUBSCRIBE(
            cast(Any, websocket),
            WSSubscribeRequest(
                session_id="", sequence_id=0, topics=["market.kraken.BTC-USD.candles.1m"]
            ),
            manager,
            UserRole.OPERATOR,
        )
    response = json.loads(websocket.sent[-1])
    assert response["type"] == "error"
    assert "Invalid topic format" in response["message"]
    assert "bad" in response["message"]
    assert not manager.zmq_bridge.added


@pytest.mark.asyncio
async def test_handle_subscribe_success_partial() -> None:
    """Subscribe returns partial when some topics allowed.

    Given: A viewer requesting market and signals topics,
    When: Handling subscribe,
    Then: Returns partial status with market allowed.
    """
    websocket = WebSocketStub()
    manager = ManagerStub()

    def fake_validate(topic: str) -> tuple[bool, str]:
        return True, ""

    with (
        patch(
            "snapper.interface.websocket.handlers.subscribe.validate_subscription_pattern",
            side_effect=fake_validate,
        ),
        patch(
            "snapper.interface.websocket.handlers.subscribe.get_allowed_topics_for_role",
            return_value=["market.kraken.BTC-USD.candles.1m"],
        ),
    ):
        await HANDLE_SUBSCRIBE(
            cast(Any, websocket),
            WSSubscribeRequest(
                session_id="",
                sequence_id=0,
                topics=["market.kraken.BTC-USD.candles.1m", "signals.kraken.BTC-USD.live"],
            ),
            manager,
            UserRole.VIEWER,
        )
    response = json.loads(websocket.sent[-1])
    assert response["status"] == "partial"
    assert manager.zmq_bridge.added
    _, topics_added = manager.zmq_bridge.added[-1]
    assert topics_added == ["market.kraken.BTC-USD.candles.1m"]


@pytest.mark.asyncio
async def test_handle_unsubscribe_handles_unknown() -> None:
    """Unsubscribe handles unknown topic gracefully.

    Given: A subscription to 'known.topic',
    When: Unsubscribing from 'unknown.topic',
    Then: Returns no_topics status.
    """
    websocket = WebSocketStub()
    manager = ManagerStub()
    manager.set_subscriptions(websocket, {"known.topic"})
    await HANDLE_UNSUBSCRIBE(
        cast(Any, websocket),
        WSUnsubscribeRequest(session_id="", sequence_id=0, topics=["unknown.topic"]),
        manager,
    )
    response = json.loads(websocket.sent[-1])
    assert response["status"] == "no_topics"


@pytest.mark.asyncio
async def test_handle_unsubscribe_success() -> None:
    """Unsubscribe removes active subscription.

    Given: An active signals subscription,
    When: Unsubscribing from that topic,
    Then: Returns unsubscribed status and removes from bridge.
    """
    websocket = WebSocketStub()
    manager = ManagerStub()
    manager.set_subscriptions(websocket, {"signals.kraken.BTC-USD.live"})
    await HANDLE_UNSUBSCRIBE(
        cast(Any, websocket),
        WSUnsubscribeRequest(session_id="", sequence_id=0, topics=["signals.kraken.BTC-USD.live"]),
        manager,
    )
    response = json.loads(websocket.sent[-1])
    assert response["status"] == "unsubscribed"
    assert manager.zmq_bridge.removed


@pytest.fixture
def mock_websocket() -> MagicMock:
    """Provide mock WebSocket with async methods."""
    ws = MagicMock()
    ws.send_text = AsyncMock()
    ws.receive_text = AsyncMock()
    ws.close = AsyncMock()
    return ws


@pytest.fixture
def mock_user() -> AuthPrincipal:
    """Provide mock admin auth principal."""
    return AuthPrincipal(
        username="testuser",
        email="test@example.com",
        role=UserRole.ADMIN,
    )


@pytest.fixture
def mock_token_data() -> MagicMock:
    """Provide mock token data with session ID."""
    token = MagicMock()
    token.sid = "session-id-123"
    return token


@pytest.fixture
def mock_ws_payload() -> WsTokenPayload:
    """Provide valid WebSocket token payload."""
    return WsTokenPayload(
        purpose="websocket",
        sub="user-123",
        sid_hash="hashed-sid",
        exp=int((datetime.now(UTC) + timedelta(hours=1)).timestamp()),
        iat=int(datetime.now(UTC).timestamp()),
        jti="token-jti-123",
    )


@pytest.fixture
def mock_ws_auth_manager() -> MagicMock:
    """Provide mock WebSocket authentication manager."""
    manager = MagicMock()
    manager.verify_session_cookie = MagicMock()
    manager.register_connection = MagicMock()
    manager.get_state = MagicMock()
    manager.update_ws_token_state = MagicMock()
    return manager


@pytest.fixture
def mock_ws_token_service() -> MagicMock:
    """Provide mock WebSocket token service."""
    service = MagicMock()
    service.verify = MagicMock()
    service.mark_used = MagicMock()
    return service


@pytest.fixture
def tracker() -> SequenceTracker:
    """Provide a SequenceTracker for provenance stamping."""
    return SequenceTracker()


class TestAuthResult:
    """Tests for AuthResult data class."""

    def test_auth_result_success(
        self, mock_user: AuthPrincipal, mock_ws_payload: WsTokenPayload
    ) -> None:
        """AuthResult holds success state with user and payload.

        Given: A successful authentication,
        When: Creating AuthResult with user and tasks,
        Then: All fields populated correctly.
        """
        warn_task = MagicMock()
        hard_task = MagicMock()
        result = AuthResult(
            success=True,
            user=mock_user,
            ws_payload=mock_ws_payload,
            warn_task=warn_task,
            hard_task=hard_task,
        )
        assert result.success is True
        assert result.user == mock_user
        assert result.ws_payload == mock_ws_payload
        assert result.warn_task == warn_task
        assert result.hard_task == hard_task

    def test_auth_result_failure(self) -> None:
        """AuthResult holds failure state with None fields.

        Given: A failed authentication,
        When: Creating AuthResult with success=False,
        Then: All optional fields are None.
        """
        result = AuthResult(success=False)
        assert result.success is False
        assert result.user is None
        assert result.ws_payload is None
        assert result.warn_task is None
        assert result.hard_task is None


class TestCreateDeadlineTasks:
    """Tests for deadline task creation."""

    @pytest.mark.asyncio
    async def test_creates_warning_and_deadline_tasks(
        self, mock_websocket: MagicMock, tracker: SequenceTracker
    ) -> None:
        """Create deadline tasks returns both warn and hard tasks.

        Given: A WebSocket and future expiration timestamp,
        When: Creating deadline tasks,
        Then: Returns both asyncio tasks that can be cancelled.
        """
        exp_timestamp = int((datetime.now(UTC) + timedelta(hours=1)).timestamp())
        warn_task, hard_task = create_deadline_tasks(mock_websocket, exp_timestamp, tracker)
        assert isinstance(warn_task, asyncio.Task)
        assert isinstance(hard_task, asyncio.Task)
        warn_task.cancel()
        hard_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await warn_task
        with pytest.raises(asyncio.CancelledError):
            await hard_task

    @pytest.mark.asyncio
    async def test_warning_task_sends_reauth_required(
        self, mock_websocket: MagicMock, tracker: SequenceTracker
    ) -> None:
        """Warning task sends reauth_required message.

        Given: An expiration timestamp in the past,
        When: Warning task runs,
        Then: Sends reauth_required message to WebSocket.
        """
        exp_timestamp = int((datetime.now(UTC) - timedelta(seconds=30)).timestamp())
        warn_task, hard_task = create_deadline_tasks(mock_websocket, exp_timestamp, tracker)
        await asyncio.sleep(0.05)
        assert mock_websocket.send_text.called
        sent_data = json.loads(mock_websocket.send_text.call_args_list[0][0][0])
        assert sent_data["type"] == "reauth_required"
        warn_task.cancel()
        hard_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await warn_task
        with contextlib.suppress(asyncio.CancelledError):
            await hard_task

    @pytest.mark.asyncio
    async def test_deadline_task_closes_connection(
        self, mock_websocket: MagicMock, tracker: SequenceTracker
    ) -> None:
        """Deadline task closes connection after grace period.

        Given: An expiration well past grace period,
        When: Deadline task runs,
        Then: Sends auth_expired and closes connection.
        """
        exp_timestamp = int(
            (datetime.now(UTC) - REAUTH_GRACE_PERIOD - timedelta(seconds=5)).timestamp()
        )
        warn_task, hard_task = create_deadline_tasks(mock_websocket, exp_timestamp, tracker)
        await asyncio.sleep(0.5)
        calls = [call[0][0] for call in mock_websocket.send_text.call_args_list]
        has_expired = any('"type":"auth_expired"' in call for call in calls)
        assert has_expired
        assert mock_websocket.close.called
        mock_websocket.close.assert_called_with(code=4401, reason="Authorization expired")
        warn_task.cancel()
        hard_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await warn_task
        with contextlib.suppress(asyncio.CancelledError):
            await hard_task


class TestAuthenticateWebsocket:
    """Tests for WebSocket authentication handler."""

    @pytest.mark.asyncio
    async def test_missing_session_cookie(
        self,
        mock_websocket: MagicMock,
        mock_ws_auth_manager: MagicMock,
        mock_ws_token_service: MagicMock,
        tracker: SequenceTracker,
    ) -> None:
        """Authenticate fails with missing session cookie.

        Given: No valid session cookie,
        When: Authenticating WebSocket,
        Then: Returns failure with missing_cookie reason.
        """
        mock_ws_auth_manager.verify_session_cookie.return_value = None
        result = await authenticate_websocket(
            mock_websocket, mock_ws_auth_manager, mock_ws_token_service, tracker
        )
        assert result.success is False
        mock_websocket.send_text.assert_called()
        sent = json.loads(mock_websocket.send_text.call_args_list[0][0][0])
        assert sent["type"] == "auth_failed"
        assert sent["reason"] == "missing_cookie"
        mock_websocket.close.assert_called_with(code=4401, reason="Authentication cookie missing")

    @pytest.mark.asyncio
    async def test_authentication_timeout(
        self,
        mock_websocket: MagicMock,
        mock_ws_auth_manager: MagicMock,
        mock_ws_token_service: MagicMock,
        mock_user: AuthPrincipal,
        mock_token_data: MagicMock,
        tracker: SequenceTracker,
    ) -> None:
        """Authenticate fails on client timeout.

        Given: Valid session but client times out,
        When: Waiting for client response,
        Then: Returns failure with timeout reason.
        """
        mock_ws_auth_manager.verify_session_cookie.return_value = (
            mock_user,
            mock_token_data,
        )
        mock_websocket.receive_text.side_effect = TimeoutError()
        result = await authenticate_websocket(
            mock_websocket, mock_ws_auth_manager, mock_ws_token_service, tracker
        )
        assert result.success is False
        calls = mock_websocket.send_text.call_args_list
        timeout_sent = any('"reason":"timeout"' in call[0][0] for call in calls)
        assert timeout_sent
        mock_websocket.close.assert_called_with(code=4408, reason="Authentication timeout")

    @pytest.mark.asyncio
    async def test_invalid_json_message(
        self,
        mock_websocket: MagicMock,
        mock_ws_auth_manager: MagicMock,
        mock_ws_token_service: MagicMock,
        mock_user: AuthPrincipal,
        mock_token_data: MagicMock,
        tracker: SequenceTracker,
    ) -> None:
        """Authenticate fails with invalid JSON message.

        Given: Valid session cookie,
        When: Client sends malformed JSON,
        Then: Returns failure with invalid_json reason.
        """
        mock_ws_auth_manager.verify_session_cookie.return_value = (
            mock_user,
            mock_token_data,
        )
        mock_websocket.receive_text.return_value = "not valid json{"
        result = await authenticate_websocket(
            mock_websocket, mock_ws_auth_manager, mock_ws_token_service, tracker
        )
        assert result.success is False
        calls = mock_websocket.send_text.call_args_list
        invalid_json_sent = any('"reason":"invalid_json"' in call[0][0] for call in calls)
        assert invalid_json_sent
        mock_websocket.close.assert_called_with(code=4401, reason="Invalid auth payload")

    @pytest.mark.asyncio
    async def test_invalid_message_type(
        self,
        mock_websocket: MagicMock,
        mock_ws_auth_manager: MagicMock,
        mock_ws_token_service: MagicMock,
        mock_user: AuthPrincipal,
        mock_token_data: MagicMock,
        tracker: SequenceTracker,
    ) -> None:
        """Authenticate fails with invalid message type.

        Given: Valid session cookie,
        When: Client sends wrong message type,
        Then: Returns failure with Invalid auth payload reason.
        """
        mock_ws_auth_manager.verify_session_cookie.return_value = (
            mock_user,
            mock_token_data,
        )
        mock_websocket.receive_text.return_value = json.dumps(
            {"type": "subscribe", "session_id": "", "sequence_id": 0}
        )
        result = await authenticate_websocket(
            mock_websocket, mock_ws_auth_manager, mock_ws_token_service, tracker
        )
        assert result.success is False
        mock_websocket.close.assert_called_with(code=4401, reason="Invalid auth payload")

    @pytest.mark.asyncio
    async def test_missing_ws_token(
        self,
        mock_websocket: MagicMock,
        mock_ws_auth_manager: MagicMock,
        mock_ws_token_service: MagicMock,
        mock_user: AuthPrincipal,
        mock_token_data: MagicMock,
        tracker: SequenceTracker,
    ) -> None:
        """Authenticate fails when ws_token missing.

        Given: Valid session cookie,
        When: Client sends authenticate without ws_token,
        Then: Returns failure with Invalid auth payload reason.
        """
        mock_ws_auth_manager.verify_session_cookie.return_value = (
            mock_user,
            mock_token_data,
        )
        mock_websocket.receive_text.return_value = json.dumps(
            {"type": "authenticate", "session_id": "", "sequence_id": 0}
        )
        result = await authenticate_websocket(
            mock_websocket, mock_ws_auth_manager, mock_ws_token_service, tracker
        )
        assert result.success is False
        mock_websocket.close.assert_called_with(code=4401, reason="Invalid auth payload")

    @pytest.mark.asyncio
    async def test_ws_token_already_used(
        self,
        mock_websocket: MagicMock,
        mock_ws_auth_manager: MagicMock,
        mock_ws_token_service: MagicMock,
        mock_user: AuthPrincipal,
        mock_token_data: MagicMock,
        tracker: SequenceTracker,
    ) -> None:
        """Authenticate fails with already-used ws_token.

        Given: Valid session and previously used token,
        When: Verifying ws_token,
        Then: Returns failure with ws_token replay reason.
        """
        mock_ws_auth_manager.verify_session_cookie.return_value = (
            mock_user,
            mock_token_data,
        )
        mock_websocket.receive_text.return_value = json.dumps(
            {
                "type": "authenticate",
                "session_id": "",
                "sequence_id": 0,
                "ws_token": "already-used-token",
            }
        )
        mock_ws_token_service.verify.side_effect = WsTokenAlreadyUsedError("Token replay")
        result = await authenticate_websocket(
            mock_websocket, mock_ws_auth_manager, mock_ws_token_service, tracker
        )
        assert result.success is False
        mock_websocket.close.assert_called_with(code=4401, reason="ws_token replay")

    @pytest.mark.asyncio
    async def test_ws_token_invalid(
        self,
        mock_websocket: MagicMock,
        mock_ws_auth_manager: MagicMock,
        mock_ws_token_service: MagicMock,
        mock_user: AuthPrincipal,
        mock_token_data: MagicMock,
        tracker: SequenceTracker,
    ) -> None:
        """Authenticate fails with invalid ws_token.

        Given: Valid session and invalid token,
        When: Verifying ws_token,
        Then: Returns failure with Invalid ws_token reason.
        """
        mock_ws_auth_manager.verify_session_cookie.return_value = (
            mock_user,
            mock_token_data,
        )
        mock_websocket.receive_text.return_value = json.dumps(
            {
                "type": "authenticate",
                "session_id": "",
                "sequence_id": 0,
                "ws_token": "invalid-token",
            }
        )
        mock_ws_token_service.verify.side_effect = WsTokenError("Invalid token")
        result = await authenticate_websocket(
            mock_websocket, mock_ws_auth_manager, mock_ws_token_service, tracker
        )
        assert result.success is False
        mock_websocket.close.assert_called_with(code=4401, reason="Invalid ws_token")

    @pytest.mark.asyncio
    async def test_successful_authentication(
        self,
        mock_websocket: MagicMock,
        mock_ws_auth_manager: MagicMock,
        mock_ws_token_service: MagicMock,
        mock_user: AuthPrincipal,
        mock_token_data: MagicMock,
        mock_ws_payload: WsTokenPayload,
        tracker: SequenceTracker,
    ) -> None:
        """Authenticate succeeds with valid credentials.

        Given: Valid session and valid ws_token,
        When: Completing authentication,
        Then: Returns success with user, payload and tasks.
        """
        mock_ws_auth_manager.verify_session_cookie.return_value = (
            mock_user,
            mock_token_data,
        )
        mock_websocket.receive_text.return_value = json.dumps(
            {"type": "authenticate", "session_id": "", "sequence_id": 0, "ws_token": "valid-token"}
        )
        mock_ws_token_service.verify.return_value = mock_ws_payload
        result = await authenticate_websocket(
            mock_websocket, mock_ws_auth_manager, mock_ws_token_service, tracker
        )
        assert result.success is True
        assert result.user == mock_user
        assert result.ws_payload == mock_ws_payload
        assert result.warn_task is not None
        assert result.hard_task is not None
        mock_ws_token_service.mark_used.assert_called_once_with(mock_ws_payload)
        mock_ws_auth_manager.register_connection.assert_called_once()
        result.warn_task.cancel()
        result.hard_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await result.warn_task
        with contextlib.suppress(asyncio.CancelledError):
            await result.hard_task


class TestHandleReauth:
    """Tests for WebSocket re-authentication handler."""

    @pytest.mark.asyncio
    async def test_missing_session_state(
        self,
        mock_websocket: MagicMock,
        mock_user: AuthPrincipal,
        mock_ws_auth_manager: MagicMock,
        mock_ws_token_service: MagicMock,
        tracker: SequenceTracker,
    ) -> None:
        """Reauth fails with missing session state.

        Given: No session state in auth manager,
        When: Handling reauth request,
        Then: Closes connection with Missing session state reason.
        """
        message = WSReauthRequest(session_id="", sequence_id=0, ws_token="some-token")
        mock_ws_auth_manager.get_state.return_value = None
        result = await handle_reauth(
            mock_websocket,
            message,
            mock_user,
            mock_ws_auth_manager,
            mock_ws_token_service,
            tracker,
        )
        assert result is False
        mock_websocket.close.assert_called_with(code=4401, reason="Missing session state")

    @pytest.mark.asyncio
    async def test_ws_token_replay_attack(
        self,
        mock_websocket: MagicMock,
        mock_user: AuthPrincipal,
        mock_ws_auth_manager: MagicMock,
        mock_ws_token_service: MagicMock,
        tracker: SequenceTracker,
    ) -> None:
        """Reauth fails on ws_token replay attack.

        Given: Session state and already-used token,
        When: Handling reauth request,
        Then: Closes connection with ws_token replay reason.
        """
        message = WSReauthRequest(session_id="", sequence_id=0, ws_token="reused-token")
        state = MagicMock()
        state.session_id = "session-123"
        mock_ws_auth_manager.get_state.return_value = state
        mock_ws_token_service.verify.side_effect = WsTokenAlreadyUsedError("Replay")
        result = await handle_reauth(
            mock_websocket,
            message,
            mock_user,
            mock_ws_auth_manager,
            mock_ws_token_service,
            tracker,
        )
        assert result is False
        mock_websocket.close.assert_called_with(code=4401, reason="ws_token replay")

    @pytest.mark.asyncio
    async def test_invalid_ws_token(
        self,
        mock_websocket: MagicMock,
        mock_user: AuthPrincipal,
        mock_ws_auth_manager: MagicMock,
        mock_ws_token_service: MagicMock,
        tracker: SequenceTracker,
    ) -> None:
        """Reauth fails with invalid ws_token.

        Given: Session state and invalid token,
        When: Handling reauth request,
        Then: Closes connection with Invalid ws_token reason.
        """
        message = WSReauthRequest(session_id="", sequence_id=0, ws_token="invalid-token")
        state = MagicMock()
        state.session_id = "session-123"
        mock_ws_auth_manager.get_state.return_value = state
        mock_ws_token_service.verify.side_effect = WsTokenError("Invalid")
        result = await handle_reauth(
            mock_websocket,
            message,
            mock_user,
            mock_ws_auth_manager,
            mock_ws_token_service,
            tracker,
        )
        assert result is False
        mock_websocket.close.assert_called_with(code=4401, reason="Invalid ws_token")

    @pytest.mark.asyncio
    async def test_successful_reauth(
        self,
        mock_websocket: MagicMock,
        mock_user: AuthPrincipal,
        mock_ws_auth_manager: MagicMock,
        mock_ws_token_service: MagicMock,
        mock_ws_payload: WsTokenPayload,
        tracker: SequenceTracker,
    ) -> None:
        """Reauth succeeds with valid new token.

        Given: Session state and valid new ws_token,
        When: Handling reauth request,
        Then: Updates state and sends reauth_ok message.
        """
        message = WSReauthRequest(session_id="", sequence_id=0, ws_token="new-valid-token")
        state = MagicMock()
        state.session_id = "session-123"
        mock_ws_auth_manager.get_state.return_value = state
        mock_ws_token_service.verify.return_value = mock_ws_payload
        result = await handle_reauth(
            mock_websocket,
            message,
            mock_user,
            mock_ws_auth_manager,
            mock_ws_token_service,
            tracker,
        )
        assert result is True
        mock_ws_token_service.mark_used.assert_called_once_with(mock_ws_payload)
        mock_ws_auth_manager.update_ws_token_state.assert_called_once()
        calls = mock_websocket.send_text.call_args_list
        reauth_ok_sent = any('"type":"reauth_ok"' in call[0][0] for call in calls)
        assert reauth_ok_sent


class TestAuthHandlerConstants:
    """Tests for auth handler module constants."""

    def test_auth_timeout_seconds(self) -> None:
        """AUTH_TIMEOUT_SECONDS constant is 10.

        Given: The auth handler module,
        When: Checking timeout constant,
        Then: Value is 10 seconds.
        """
        assert AUTH_TIMEOUT_SECONDS == 10

    def test_reauth_warn_offset(self) -> None:
        """REAUTH_WARN_OFFSET constant is 60 seconds.

        Given: The auth handler module,
        When: Checking warn offset constant,
        Then: Value is 60 seconds timedelta.
        """
        assert timedelta(seconds=60) == REAUTH_WARN_OFFSET

    def test_reauth_grace_period(self) -> None:
        """REAUTH_GRACE_PERIOD constant is 60 seconds.

        Given: The auth handler module,
        When: Checking grace period constant,
        Then: Value is 60 seconds timedelta.
        """
        assert timedelta(seconds=60) == REAUTH_GRACE_PERIOD


class TestDeadlineTasksErrorHandling:
    """Tests for deadline task error handling."""

    @pytest.mark.asyncio
    async def test_warning_task_handles_send_error(
        self, mock_websocket: MagicMock, tracker: SequenceTracker
    ) -> None:
        """Warning task handles send_text error gracefully.

        Given: A WebSocket that raises on send_text,
        When: Warning task tries to send reauth_required,
        Then: Task completes without exception.
        """
        exp_timestamp = int((datetime.now(UTC) - timedelta(seconds=30)).timestamp())
        mock_websocket.send_text = AsyncMock(side_effect=RuntimeError("Connection closed"))
        warn_task, hard_task = create_deadline_tasks(mock_websocket, exp_timestamp, tracker)
        await asyncio.sleep(0.05)
        assert warn_task.done()
        assert warn_task.exception() is None
        hard_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await hard_task

    @pytest.mark.asyncio
    async def test_deadline_task_handles_send_error(
        self, mock_websocket: MagicMock, tracker: SequenceTracker
    ) -> None:
        """Deadline task handles send_text error gracefully.

        Given: A WebSocket that raises on send_text,
        When: Deadline task tries to send auth_expired,
        Then: Task completes without exception.
        """
        exp_timestamp = int(
            (datetime.now(UTC) - REAUTH_GRACE_PERIOD - timedelta(seconds=5)).timestamp()
        )
        mock_websocket.send_text = AsyncMock(side_effect=RuntimeError("Connection closed"))
        warn_task, hard_task = create_deadline_tasks(mock_websocket, exp_timestamp, tracker)
        await asyncio.sleep(0.5)
        assert hard_task.done()
        assert hard_task.exception() is None
        warn_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await warn_task


pytest = cast(Any, pytest)
routes = cast(Any, routes)


@dataclass
class StubCSRFManager:
    """CSRF manager stub for testing."""

    token: str = "csrf-token"

    def generate_token(self) -> str:
        """Generate a CSRF token."""
        return self.token

    def invalidate_token(self, token: str) -> None:
        """Invalidate and replace token."""
        self.token = token


class StubTokenManager:
    """Token manager stub for testing."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.create_tokens_response = TokenPair(
            access_token="access-token",
            refresh_token="refresh-token",
            expires_in=900,
        )
        self.verify_response: TokenClaims | None = None
        self.invalidated_tokens: list[str] = []
        self.blacklisted: list[str] = []
        self.last_created_user: AuthPrincipal | None = None
        self.last_verified_token: str | None = None
        self.last_session_id: str | None = None

    def create_tokens(
        self,
        user: AuthPrincipal,
        remember_me: bool = False,
        *,
        session_id: str | None = None,
    ) -> TokenPair:
        """Create access and refresh tokens."""
        self.last_created_user = user
        self.last_session_id = session_id
        return self.create_tokens_response

    def verify_token(self, token: str) -> TokenClaims | None:
        """Verify token and return claims."""
        self.last_verified_token = token
        return self.verify_response

    def blacklist_token(self, jti: str) -> None:
        """Add token JTI to blacklist."""
        self.blacklisted.append(jti)

    def invalidate_token(self, token: str) -> None:
        """Invalidate token."""
        self.invalidated_tokens.append(token)


class StubUserService:
    """User service stub for testing."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.authenticated_user: UserProfile | None = None
        self.user_by_id: UserProfile | None = None
        self.all_users: list[UserProfile] = []
        self.created_users: list[dict[str, Any]] = []
        self.create_user_result: UserProfile | None = None
        self.create_user_error: Exception | None = None
        self.updated_users: dict[str, UserProfile | None] = {}
        self.deleted_users: list[str] = []
        self.change_password_calls: list[tuple[str, str, str]] = []
        self.change_password_success: bool = True
        self.delete_user_success: bool = True
        self.update_user_result: UserProfile | None = None

    async def authenticate_user(self, username: str, password: str) -> UserProfile | None:
        """Authenticate user by credentials."""
        return self.authenticated_user

    async def get_user_by_id(self, user_id: str) -> UserProfile | None:
        """Get user by ID."""
        return self.user_by_id

    async def get_all_users(self, include_inactive: bool = False) -> list[UserProfile]:
        """Get all users."""
        return self.all_users

    async def create_user(
        self,
        *,
        username: str,
        password: str,
        email: str | None,
        role: UserRole,
        is_active: bool,
    ) -> UserProfile:
        """Create a new user."""
        if self.create_user_error:
            raise self.create_user_error
        if self.create_user_result:
            return self.create_user_result
        user = UserProfile(
            session_id="test-sid",
            sequence_id=1,
            username=username,
            email=email,
            role=role,
            is_active=is_active,
        )
        self.created_users.append(
            {
                "username": username,
                "password": password,
                "email": email,
                "role": role,
                "is_active": is_active,
            }
        )
        return user

    async def update_user(
        self,
        *,
        user_id: str,
        email: str | None,
        role: UserRole,
        is_active: bool,
    ) -> UserProfile | None:
        """Update user details."""
        self.updated_users[user_id] = self.update_user_result
        return self.update_user_result

    async def delete_user(self, user_id: str) -> bool:
        """Delete user by ID."""
        self.deleted_users.append(user_id)
        return self.delete_user_success

    async def change_password(self, user_id: str, old_password: str, new_password: str) -> bool:
        """Change user password."""
        self.change_password_calls.append((user_id, old_password, new_password))
        return self.change_password_success

    async def admin_reset_password(self, user_id: str, new_password: str) -> None:
        """Reset user password via admin action."""
        self.change_password_calls.append((user_id, "admin_reset", new_password))


AuthAppFixture = tuple[Any, StubUserService, StubTokenManager, StubCSRFManager]


@pytest.fixture()
def auth_app(
    monkeypatch: Any,
) -> AuthAppFixture:
    """Provide FastAPI app with stubbed auth services."""
    user_service = StubUserService()
    token_manager = StubTokenManager()
    csrf_manager = StubCSRFManager()
    settings = SimpleNamespace(session_secure=False, session_same_site="lax")
    monkeypatch.setattr(routes, "get_user_service", lambda: user_service)
    monkeypatch.setattr(routes, "get_token_manager", lambda: token_manager)
    monkeypatch.setattr(routes, "get_csrf_manager", lambda: csrf_manager)
    app = FastAPI()
    app.state.settings = settings
    app.state.rest_tracker = SequenceTracker()
    app.include_router(routes.router)
    current_user = AuthPrincipal(username="alice", role=UserRole.ADMIN)
    app.dependency_overrides[validate_csrf_token] = lambda: None
    app.dependency_overrides[require_authentication] = lambda: current_user
    client = TestClient(app)
    return client, user_service, token_manager, csrf_manager


def test_login_success_sets_cookies(
    auth_app: AuthAppFixture,
) -> None:
    """Login success sets authentication cookies.

    Given: Valid user credentials,
    When: Logging in,
    Then: Sets access_token, refresh_token and csrf_token cookies.
    """
    client, user_service, token_manager, csrf_manager = auth_app
    user = UserProfile(
        session_id="test-sid",
        sequence_id=1,
        username="bob",
        email="bob@example.com",
        role=UserRole.OPERATOR,
    )
    user_service.authenticated_user = user
    csrf_manager.token = "csrf-new"
    token_manager.create_tokens_response = TokenPair(
        access_token="new-access",
        refresh_token="new-refresh",
        expires_in=600,
    )
    response = client.post(
        "/auth/login",
        json={"session_id": "", "sequence_id": 0, "username": "bob", "password": "secret"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["message"] == "Login successful"
    assert token_manager.last_created_user is not None
    assert token_manager.last_created_user.username == user.username
    assert token_manager.last_created_user.role == user.role
    assert response.cookies.get("access_token") == "new-access"
    assert response.cookies.get("refresh_token") == "new-refresh"
    assert response.cookies.get("csrf_token") == "csrf-new"


def test_login_failure_returns_401(
    auth_app: AuthAppFixture,
) -> None:
    """Login failure returns 401 status.

    Given: Invalid credentials,
    When: Attempting login,
    Then: Returns 401 with error message.
    """
    client, user_service, _token_manager, _csrf_manager = auth_app
    user_service.authenticated_user = None
    response = client.post(
        "/auth/login",
        json={"session_id": "", "sequence_id": 0, "username": "bob", "password": "wrong"},
    )
    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid username or password"


def test_refresh_token_success(
    auth_app: AuthAppFixture,
) -> None:
    """Refresh token returns new tokens and ws_token.

    Given: Valid refresh token cookie,
    When: Calling refresh endpoint,
    Then: Returns rotated tokens and ws_token.
    """
    client, user_service, token_manager, csrf_manager = auth_app
    token_manager.verify_response = TokenClaims(
        sub="123",
        username="bob",
        role=UserRole.OPERATOR,
        permissions=["read"],
        exp=999999999,
        iat=123456,
        jti="refresh-jti",
        sid="session-123",
    )
    user = UserProfile(session_id="test-sid", sequence_id=1, username="bob", role=UserRole.OPERATOR)
    user_service.user_by_id = user
    token_manager.create_tokens_response = TokenPair(
        access_token="rotated-access",
        refresh_token="rotated-refresh",
        expires_in=999,
    )
    csrf_manager.token = "csrf-rot"
    client.cookies.set("refresh_token", "existing-refresh")
    response = client.post("/auth/refresh")
    assert response.status_code == 200
    assert token_manager.last_verified_token == "existing-refresh"
    assert token_manager.blacklisted == ["refresh-jti"]
    assert response.cookies.get("access_token") == "rotated-access"
    assert response.cookies.get("refresh_token") == "rotated-refresh"
    assert response.cookies.get("csrf_token") == "csrf-rot"
    payload = response.json()
    assert payload["ws_token"]
    assert isinstance(payload["ws_token_exp"], str)
    assert payload["csrf_token"] == "csrf-rot"
    assert payload["user"]["username"] == "bob"
    assert payload["user"]["role"] == "operator"


def test_get_current_user_profile_returns_user(
    auth_app: AuthAppFixture,
) -> None:
    """Get current user returns authenticated user profile.

    Given: An authenticated session,
    When: Calling /me endpoint,
    Then: Returns current user profile.
    """
    client, user_service, _token_manager, _csrf_manager = auth_app
    user_service.user_by_id = UserProfile(
        session_id="test-sid", sequence_id=1, username="alice", role=UserRole.ADMIN
    )
    response = client.get("/auth/me")
    assert response.status_code == 200
    payload = response.json()
    assert payload["username"] == "alice"
    assert payload["role"] == "admin"
    user_service.user_by_id = None


def test_refresh_token_missing_cookie_returns_401(
    auth_app: AuthAppFixture,
) -> None:
    """Refresh without cookie returns 401.

    Given: No refresh token cookie,
    When: Calling refresh endpoint,
    Then: Returns 401 with error message.
    """
    client, _user_service, token_manager, _csrf_manager = auth_app
    response = client.post("/auth/refresh")
    assert response.status_code == 401
    assert response.json()["detail"] == "Refresh token not found"
    assert token_manager.last_verified_token is None


def test_refresh_token_invalid_token_returns_401(
    auth_app: AuthAppFixture,
) -> None:
    """Refresh with invalid token returns 401.

    Given: Invalid refresh token cookie,
    When: Calling refresh endpoint,
    Then: Returns 401 with error message.
    """
    client, _user_service, token_manager, _csrf_manager = auth_app
    client.cookies.set("refresh_token", "invalid-refresh")
    token_manager.verify_response = None
    response = client.post("/auth/refresh")
    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid refresh token"


def test_refresh_token_user_missing_returns_401(
    auth_app: AuthAppFixture,
) -> None:
    """Refresh fails when user not found.

    Given: Valid token but user missing from service,
    When: Calling refresh endpoint,
    Then: Returns 401 User not found.
    """
    client, user_service, token_manager, _csrf_manager = auth_app
    token_manager.verify_response = TokenClaims(
        sub="missing",
        username="ghost",
        role=UserRole.OPERATOR,
        permissions=["read"],
        exp=999999999,
        iat=123456,
        jti="missing-jti",
        sid="session-missing",
    )
    user_service.user_by_id = None
    client.cookies.set("refresh_token", "refresh-token")
    response = client.post("/auth/refresh")
    assert response.status_code == 401
    assert response.json()["detail"] == "User not found"


def test_logout_invalidates_tokens(
    auth_app: AuthAppFixture,
) -> None:
    """Logout invalidates tokens and clears cookies.

    Given: Active session with tokens,
    When: Calling logout endpoint,
    Then: Tokens invalidated and cookies cleared.
    """
    client, _user_service, token_manager, csrf_manager = auth_app
    client.cookies.set("refresh_token", "refresh-old")
    client.cookies.set("access_token", "access-old")
    client.cookies.set("csrf_token", "csrf-old")
    response = client.post("/auth/logout")
    assert response.status_code == 200
    assert "refresh-old" in token_manager.invalidated_tokens
    assert "access-old" in token_manager.invalidated_tokens
    assert csrf_manager.token == "csrf-old"
    assert not response.cookies.get("refresh_token")
    assert not response.cookies.get("access_token")
    assert not response.cookies.get("csrf_token")


def test_logout_without_tokens_returns_success(
    auth_app: AuthAppFixture,
) -> None:
    """Logout without tokens still returns success.

    Given: No active tokens,
    When: Calling logout endpoint,
    Then: Returns 200 success.
    """
    client, _user_service, token_manager, _csrf_manager = auth_app
    response = client.post("/auth/logout")
    assert response.status_code == 200
    assert token_manager.invalidated_tokens == []
    assert not response.cookies.get("refresh_token")
    assert not response.cookies.get("access_token")
    assert not response.cookies.get("csrf_token")


def test_get_current_user_info(
    auth_app: AuthAppFixture,
) -> None:
    """Get current user returns user info.

    Given: An authenticated session,
    When: Calling /me endpoint,
    Then: Returns username and role.
    """
    client, user_service, _, _ = auth_app
    user_service.user_by_id = UserProfile(
        session_id="test-sid", sequence_id=1, username="alice", role=UserRole.ADMIN
    )
    response = client.get("/auth/me")
    assert response.status_code == 200
    data: dict[str, Any] = response.json()
    assert data["username"] == "alice"
    assert data["role"] == UserRole.ADMIN.value
    user_service.user_by_id = None


@pytest.mark.asyncio()
async def test_get_users_returns_response(monkeypatch: Any) -> None:
    """Get users returns list of all users.

    Given: A user service with two users,
    When: Calling get_users endpoint,
    Then: Returns user list with total count.
    """
    stub_service = StubUserService()
    stub_service.all_users = [
        UserProfile(session_id="test-sid", sequence_id=1, username="alice", role=UserRole.ADMIN),
        UserProfile(session_id="test-sid", sequence_id=1, username="bob", role=UserRole.OPERATOR),
    ]
    monkeypatch.setattr(routes, "get_user_service", lambda: stub_service)
    result = await routes.get_users(
        request=_make_rest_request(),
        current_user=stub_service.all_users[0],
        include_inactive=False,
    )
    assert result.total_count == 2
    assert [user.username for user in result.users] == ["alice", "bob"]


@pytest.mark.asyncio()
async def test_create_user_success(monkeypatch: Any) -> None:
    """Create user returns new user profile.

    Given: Valid user creation request,
    When: Calling create_user endpoint,
    Then: Returns created user profile.
    """
    stub_service = StubUserService()
    created_user = UserProfile(
        session_id="test-sid", sequence_id=1, username="charlie", role=UserRole.VIEWER
    )
    stub_service.create_user_result = created_user
    monkeypatch.setattr(routes, "get_user_service", lambda: stub_service)
    request = CreateUserRequest(
        session_id="test-sid",
        sequence_id=1,
        username="charlie",
        password="pass-pass",
        email="c@example.com",
        role=UserRole.VIEWER,
        is_active=True,
    )
    result = await routes.create_user(
        user_data=request,
        current_user=AuthPrincipal(username="admin", role=UserRole.ADMIN),
        _csrf=None,
    )
    assert result.username == "charlie"


@pytest.mark.asyncio()
async def test_create_user_value_error(monkeypatch: Any) -> None:
    """Create user returns 400 on duplicate.

    Given: User service that raises ValueError,
    When: Creating duplicate user,
    Then: Raises HTTPException with 400 status.
    """
    stub_service = StubUserService()
    stub_service.create_user_error = ValueError("duplicate")
    monkeypatch.setattr(routes, "get_user_service", lambda: stub_service)
    request = CreateUserRequest(
        session_id="test-sid",
        sequence_id=1,
        username="dup",
        password="pass-pass",
        email=None,
        role=UserRole.VIEWER,
        is_active=True,
    )
    with pytest.raises(HTTPException) as exc:
        await routes.create_user(
            user_data=request,
            current_user=AuthPrincipal(username="admin", role=UserRole.ADMIN),
            _csrf=None,
        )
    assert exc.value.status_code == 400


@pytest.mark.asyncio()
async def test_update_user_not_found(monkeypatch: Any) -> None:
    """Update user returns 404 when not found.

    Given: User service returns None for update,
    When: Updating non-existent user,
    Then: Raises HTTPException with 404 status.
    """
    stub_service = StubUserService()
    stub_service.update_user_result = None
    monkeypatch.setattr(routes, "get_user_service", lambda: stub_service)
    request = UpdateUserRequest(
        session_id="test-sid",
        sequence_id=1,
        email=None,
        role=UserRole.VIEWER,
        is_active=False,
    )
    with pytest.raises(HTTPException) as exc:
        await routes.update_user(
            user_id="missing",
            user_data=request,
            current_user=AuthPrincipal(username="admin", role=UserRole.ADMIN),
            _csrf=None,
        )
    assert exc.value.status_code == 404


@pytest.mark.asyncio()
async def test_update_user_success(monkeypatch: Any) -> None:
    """Update user returns updated profile.

    Given: Valid update request,
    When: Updating existing user,
    Then: Returns updated user profile.
    """
    stub_service = StubUserService()
    updated_user = UserProfile(
        session_id="test-sid",
        sequence_id=1,
        username="dora",
        role=UserRole.OPERATOR,
        is_active=True,
    )
    stub_service.update_user_result = updated_user
    monkeypatch.setattr(routes, "get_user_service", lambda: stub_service)
    request = UpdateUserRequest(
        session_id="test-sid",
        sequence_id=1,
        email="dora@example.com",
        role=UserRole.OPERATOR,
        is_active=True,
    )
    result = await routes.update_user(
        user_id="u4",
        user_data=request,
        current_user=AuthPrincipal(username="admin", role=UserRole.ADMIN),
        _csrf=None,
    )
    assert result.username == "dora"
    assert stub_service.updated_users["u4"] == updated_user


@pytest.mark.asyncio()
async def test_delete_user_success(monkeypatch: Any) -> None:
    """Delete user returns success message.

    Given: Existing user to delete,
    When: Deleting user,
    Then: Returns deactivation success message.
    """
    stub_service = StubUserService()
    stub_service.delete_user_success = True
    monkeypatch.setattr(routes, "get_user_service", lambda: stub_service)
    mock_request = MagicMock()
    mock_request.app.state.rest_tracker = SequenceTracker()
    result = await routes.delete_user(
        request=mock_request,
        user_id="user-2",
        current_user=AuthPrincipal(username="admin", role=UserRole.ADMIN),
        _csrf=None,
    )
    assert result.message == "User 'user-2' has been deactivated"
    assert stub_service.deleted_users == ["user-2"]


@pytest.mark.asyncio()
async def test_delete_user_self_forbidden(monkeypatch: Any) -> None:
    """Delete user cannot delete self.

    Given: Admin trying to delete own account,
    When: Calling delete with own user_id,
    Then: Raises HTTPException with 400 status.
    """
    stub_service = StubUserService()
    monkeypatch.setattr(routes, "get_user_service", lambda: stub_service)
    mock_request = MagicMock()
    mock_request.app.state.rest_tracker = SequenceTracker()
    with pytest.raises(HTTPException) as exc:
        await routes.delete_user(
            request=mock_request,
            user_id="self",
            current_user=AuthPrincipal(username="self", role=UserRole.ADMIN),
            _csrf=None,
        )
    assert exc.value.status_code == 400


@pytest.mark.asyncio()
async def test_delete_user_not_found(monkeypatch: Any) -> None:
    """Delete user returns 404 when not found.

    Given: Non-existent user_id,
    When: Deleting user,
    Then: Raises HTTPException with 404 status.
    """
    stub_service = StubUserService()
    stub_service.delete_user_success = False
    monkeypatch.setattr(routes, "get_user_service", lambda: stub_service)
    mock_request = MagicMock()
    mock_request.app.state.rest_tracker = SequenceTracker()
    with pytest.raises(HTTPException) as exc:
        await routes.delete_user(
            request=mock_request,
            user_id="missing",
            current_user=AuthPrincipal(username="admin", role=UserRole.ADMIN),
            _csrf=None,
        )
    assert exc.value.status_code == 404


@pytest.mark.asyncio()
async def test_change_user_password_success(monkeypatch: Any) -> None:
    """Change password returns success message.

    Given: Valid current and new password,
    When: Changing own password,
    Then: Returns success message.
    """
    stub_service = StubUserService()
    monkeypatch.setattr(routes, "get_user_service", lambda: stub_service)
    pwd_request = ChangePasswordRequest(
        session_id="test-sid",
        sequence_id=1,
        current_password="old-pass",
        new_password="new-password",
    )
    result = await routes.change_user_password(
        request=_make_rest_request(),
        user_id="self",
        password_data=pwd_request,
        current_user=AuthPrincipal(username="self", role=UserRole.OPERATOR),
        _csrf=None,
    )
    assert result.message == "Password changed successfully"
    assert stub_service.change_password_calls == [("self", "old-pass", "new-password")]


@pytest.mark.asyncio()
async def test_change_user_password_forbidden(monkeypatch: Any) -> None:
    """Change password forbidden for other user.

    Given: Viewer trying to change other user's password,
    When: Calling change password,
    Then: Raises HTTPException with 403 status.
    """
    stub_service = StubUserService()
    monkeypatch.setattr(routes, "get_user_service", lambda: stub_service)
    pwd_request = ChangePasswordRequest(
        session_id="test-sid",
        sequence_id=1,
        current_password="old-pass",
        new_password="new-password",
    )
    with pytest.raises(HTTPException) as exc:
        await routes.change_user_password(
            request=_make_rest_request(),
            user_id="other",
            password_data=pwd_request,
            current_user=AuthPrincipal(username="self", role=UserRole.VIEWER),
            _csrf=None,
        )
    assert exc.value.status_code == 403


@pytest.mark.asyncio()
async def test_change_user_password_invalid_current(monkeypatch: Any) -> None:
    """Change password fails with invalid current password.

    Given: Wrong current password,
    When: Attempting password change,
    Then: Raises HTTPException with 400 status.
    """
    stub_service = StubUserService()
    stub_service.change_password_success = False
    monkeypatch.setattr(routes, "get_user_service", lambda: stub_service)
    pwd_request = ChangePasswordRequest(
        session_id="test-sid",
        sequence_id=1,
        current_password="old-pass",
        new_password="new-password",
    )
    with pytest.raises(HTTPException) as exc:
        await routes.change_user_password(
            request=_make_rest_request(),
            user_id="self",
            password_data=pwd_request,
            current_user=AuthPrincipal(username="self", role=UserRole.OPERATOR),
            _csrf=None,
        )
    assert exc.value.status_code == 400


@pytest.mark.asyncio()
async def test_change_user_password_admin_for_other_user(monkeypatch: Any) -> None:
    """Admin can change other user's password.

    Given: Admin user,
    When: Changing another user's password,
    Then: Returns success message.
    """
    stub_service = StubUserService()
    monkeypatch.setattr(routes, "get_user_service", lambda: stub_service)
    pwd_request = ChangePasswordRequest(
        session_id="test-sid",
        sequence_id=1,
        current_password="irrelevant",
        new_password="new-password",
    )
    result = await routes.change_user_password(
        request=_make_rest_request(),
        user_id="target",
        password_data=pwd_request,
        current_user=AuthPrincipal(username="admin", role=UserRole.ADMIN),
        _csrf=None,
    )
    assert result.message == "Password changed successfully"
    assert stub_service.change_password_calls == [("target", "irrelevant", "new-password")]


def test_admin_reset_password_route_registered() -> None:
    """Admin reset password route is registered on the auth router.

    Given: The auth router,
    When: Inspecting registered routes,
    Then: The admin-reset-password endpoint exists.
    """
    admin_reset_routes = [
        r
        for r in routes.router.routes
        if hasattr(r, "path") and r.path == "/auth/users/{user_id}/admin-reset-password"
    ]
    assert len(admin_reset_routes) == 1


@pytest.mark.asyncio()
async def test_admin_reset_password_handles_repository_error(
    monkeypatch: Any,
) -> None:
    """Admin reset password handles repository error.

    Given: Service admin_reset_password raises RuntimeError,
    When: Resetting password,
    Then: Raises HTTPException with 500 status.
    """

    class BrokenUserService(StubUserService):
        async def admin_reset_password(self, user_id: str, new_password: str) -> None:
            raise RuntimeError("db down")

    stub_service = BrokenUserService()
    monkeypatch.setattr(routes, "get_user_service", lambda: stub_service)
    with pytest.raises(HTTPException) as exc:
        await routes.admin_reset_user_password(
            request=_make_rest_request(),
            user_id="user-1",
            password_data=AdminResetPasswordRequest(
                session_id="test-sid", sequence_id=1, new_password="super-secret"
            ),
            current_user=AuthPrincipal(username="admin", role=UserRole.ADMIN),
            _csrf=None,
        )
    assert exc.value.status_code == 500


@pytest.mark.asyncio()
async def test_admin_reset_password_success(monkeypatch: Any) -> None:
    """Admin reset password succeeds.

    Given: Valid admin and target user,
    When: Resetting password via service,
    Then: Delegates to admin_reset_password and returns success.
    """

    class ResetUserService(StubUserService):
        def __init__(self) -> None:
            super().__init__()
            self.reset_calls: list[tuple[str, str]] = []

        async def admin_reset_password(self, user_id: str, new_password: str) -> None:
            self.reset_calls.append((user_id, new_password))

    stub_service = ResetUserService()
    monkeypatch.setattr(routes, "get_user_service", lambda: stub_service)
    result = await routes.admin_reset_user_password(
        request=_make_rest_request(),
        user_id="user-1",
        password_data=AdminResetPasswordRequest(
            session_id="test-sid", sequence_id=1, new_password="super-secret"
        ),
        current_user=AuthPrincipal(username="admin", role=UserRole.ADMIN),
        _csrf=None,
    )
    assert result.message == "Password reset successfully for user user-1"
    assert stub_service.reset_calls == [("user-1", "super-secret")]


@pytest.mark.asyncio()
async def test_admin_reset_password_user_not_found(monkeypatch: Any) -> None:
    """Admin reset password fails when user not found.

    Given: Service admin_reset_password raises ValueError,
    When: Resetting password,
    Then: Raises HTTPException with 404 status.
    """

    class NotFoundUserService(StubUserService):
        async def admin_reset_password(self, user_id: str, new_password: str) -> None:
            raise ValueError(f"User '{user_id}' not found")

    stub_service = NotFoundUserService()
    monkeypatch.setattr(routes, "get_user_service", lambda: stub_service)
    with pytest.raises(HTTPException) as exc:
        await routes.admin_reset_user_password(
            request=_make_rest_request(),
            user_id="missing",
            password_data=AdminResetPasswordRequest(
                session_id="test-sid", sequence_id=1, new_password="super-secret"
            ),
            current_user=AuthPrincipal(username="admin", role=UserRole.ADMIN),
            _csrf=None,
        )
    assert exc.value.status_code == 404
    assert exc.value.detail == "User not found"


@pytest.fixture(name="auth_routes_app", scope="module")
def auth_routes_app_fixture() -> Generator[FastAPI]:
    """Provide module-scoped FastAPI app with mocked process launcher."""
    with (
        patch("snapper.server.app.ProcessLauncherService") as mock_factory_cls,
        patch("snapper.server.app.discover_processes", return_value=None),
    ):
        mock_factory = MagicMock()
        mock_factory.sync_registry_to_database = AsyncMock(return_value=None)
        mock_factory.start_all_processes = AsyncMock(return_value=None)
        mock_factory.stop_all_processes = AsyncMock(return_value=None)
        mock_factory.started_processes = {}
        mock_factory_cls.return_value = mock_factory
        yield create_app()


@pytest.fixture(name="client", scope="module")
def client_fixture(auth_routes_app: FastAPI) -> Generator[TestClient]:
    """Provide module-scoped TestClient with mocked process launcher."""
    with TestClient(auth_routes_app) as client:
        yield client


class TestAuthRoutesCoverage:
    """Coverage tests for authentication routes."""

    @pytest.fixture(autouse=True)
    def _clear_cookies(self, client: TestClient) -> Generator[None]:
        client.cookies.clear()
        yield
        client.cookies.clear()

    def test_login_success_admin(self, client: TestClient) -> None:
        """Admin login sets cookies and returns profile.

        Given: Valid admin credentials,
        When: Logging in,
        Then: Returns admin profile and sets cookies.
        """
        response = client.post(
            "/api/auth/login",
            json={
                "session_id": "",
                "sequence_id": 0,
                "username": "admin",
                "password": "AdminSnapper2026!",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["user"]["username"] == "admin"
        assert data["user"]["role"] == "admin"
        assert data["message"] == "Login successful"
        assert "expires_in" in data
        assert "access_token" in response.cookies
        assert "csrf_token" in response.cookies

    def test_login_success_operator(self, client: TestClient) -> None:
        """Operator login returns operator profile.

        Given: Valid operator credentials,
        When: Logging in,
        Then: Returns operator role in response.
        """
        response = client.post(
            "/api/auth/login",
            json={
                "session_id": "",
                "sequence_id": 0,
                "username": "operator",
                "password": "OpSnapper2026!",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["user"]["username"] == "operator"
        assert data["user"]["role"] == "operator"

    def test_login_success_viewer(self, client: TestClient) -> None:
        """Viewer login returns viewer profile.

        Given: Valid viewer credentials,
        When: Logging in,
        Then: Returns viewer role in response.
        """
        response = client.post(
            "/api/auth/login",
            json={
                "session_id": "",
                "sequence_id": 0,
                "username": "viewer",
                "password": "ViewSnapper2026!",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["user"]["username"] == "viewer"
        assert data["user"]["role"] == "viewer"

    def test_login_invalid_username(self, client: TestClient) -> None:
        """Login with invalid username returns 401.

        Given: Non-existent username,
        When: Attempting login,
        Then: Returns 401 error.
        """
        response = client.post(
            "/api/auth/login",
            json={
                "session_id": "",
                "sequence_id": 0,
                "username": "nonexistent",
                "password": "AdminSnapper2026!",
            },
        )
        assert response.status_code == 401
        assert "Invalid username or password" in response.json()["detail"]

    def test_login_invalid_password(self, client: TestClient) -> None:
        """Login with wrong password returns 401.

        Given: Valid username but wrong password,
        When: Attempting login,
        Then: Returns 401 error.
        """
        response = client.post(
            "/api/auth/login",
            json={
                "session_id": "",
                "sequence_id": 0,
                "username": "admin",
                "password": "wrongpassword",
            },
        )
        assert response.status_code == 401
        assert "Invalid username or password" in response.json()["detail"]

    def test_login_missing_fields(self, client: TestClient) -> None:
        """Login with missing fields returns 422.

        Given: Request missing password field,
        When: Attempting login,
        Then: Returns 422 validation error.
        """
        response = client.post("/api/auth/login", json={"username": "admin"})
        assert response.status_code == 422

    def test_refresh_token_missing(self, client: TestClient) -> None:
        """Refresh without token returns 401.

        Given: No refresh token cookie,
        When: Calling refresh,
        Then: Returns 401 error.
        """
        response = client.post("/api/auth/refresh")
        assert response.status_code == 401
        assert "Refresh token not found" in response.json()["detail"]

    def test_refresh_token_invalid(self, client: TestClient) -> None:
        """Refresh with invalid token returns 401.

        Given: Invalid refresh token cookie,
        When: Calling refresh,
        Then: Returns 401 error.
        """
        client.cookies = {"refresh_token": "invalid_refresh_token"}
        try:
            response = client.post("/api/auth/refresh")
            assert response.status_code == 401
            assert "Invalid refresh token" in response.json()["detail"]
        finally:
            client.cookies.clear()

    def test_refresh_token_success(self, client: TestClient) -> None:
        """Refresh with valid token returns new tokens.

        Given: Valid refresh token from login,
        When: Calling refresh,
        Then: Returns rotated tokens and ws_token.
        """
        login_response = client.post(
            "/api/auth/login",
            json={
                "session_id": "",
                "sequence_id": 0,
                "username": "admin",
                "password": "AdminSnapper2026!",
            },
        )
        assert login_response.status_code == 200
        refresh_token = login_response.cookies.get("refresh_token")
        assert refresh_token is not None, "Refresh token should be present in cookies"
        token_manager = get_token_manager()
        original_refresh_data = token_manager.verify_token(refresh_token)
        assert original_refresh_data is not None
        expected_sid_hash = compute_sid_hash(original_refresh_data.sid)
        client.cookies = {"refresh_token": refresh_token}
        try:
            response = client.post("/api/auth/refresh")
            assert response.status_code == 200
            data = response.json()
            assert data["message"] == "session refreshed"
            assert "ws_token" in data
            assert "ws_token_exp" in data
            assert isinstance(data["ws_token"], str)
            assert isinstance(data["ws_token_exp"], str)
            ws_token_exp_dt = datetime.fromisoformat(data["ws_token_exp"].replace("Z", "+00:00"))
            assert ws_token_exp_dt > datetime.now(UTC)
            assert "refresh_token" in response.cookies
            ws_token_value = data["ws_token"]
            ws_token_service = get_ws_token_service()
            ws_payload = ws_token_service.verify(
                ws_token_value,
                expected_sub=original_refresh_data.sub,
                expected_sid_hash=expected_sid_hash,
            )
            assert ws_payload.sid_hash == expected_sid_hash
            assert ws_payload.sub == original_refresh_data.sub
            assert ws_payload.exp == int(ws_token_exp_dt.timestamp())
        finally:
            client.cookies.clear()

    def test_ws_token_replay_protection(self, client: TestClient) -> None:
        """WS token replay protection prevents reuse.

        Given: A ws_token that has been used,
        When: Attempting to verify again,
        Then: Raises WsTokenAlreadyUsedError.
        """
        login_response = client.post(
            "/api/auth/login",
            json={
                "session_id": "",
                "sequence_id": 0,
                "username": "admin",
                "password": "AdminSnapper2026!",
            },
        )
        assert login_response.status_code == 200
        refresh_token = login_response.cookies.get("refresh_token")
        assert refresh_token is not None
        token_manager = get_token_manager()
        original_refresh_data = token_manager.verify_token(refresh_token)
        assert original_refresh_data is not None
        expected_sid_hash = compute_sid_hash(original_refresh_data.sid)
        client.cookies = {"refresh_token": refresh_token}
        try:
            response = client.post("/api/auth/refresh")
            assert response.status_code == 200
            ws_token_value = response.json()["ws_token"]
            ws_token_service = get_ws_token_service()
            ws_payload = ws_token_service.verify(
                ws_token_value,
                expected_sub=original_refresh_data.sub,
                expected_sid_hash=expected_sid_hash,
            )
            ws_token_service.mark_used(ws_payload)
            with pytest.raises(WsTokenAlreadyUsedError):
                ws_token_service.verify(
                    ws_token_value,
                    expected_sub=original_refresh_data.sub,
                    expected_sid_hash=expected_sid_hash,
                )
        finally:
            client.cookies.clear()

    def test_logout_success(self, client: TestClient) -> None:
        """Logout clears session and returns success.

        Given: Active login session,
        When: Calling logout with CSRF token,
        Then: Returns success message.
        """
        login_response = client.post(
            "/api/auth/login",
            json={
                "session_id": "",
                "sequence_id": 0,
                "username": "admin",
                "password": "AdminSnapper2026!",
            },
        )
        csrf_token = login_response.cookies.get("csrf_token")
        assert csrf_token is not None
        client.cookies.update(login_response.cookies)
        try:
            response = client.post("/api/auth/logout", headers={"X-CSRF-Token": csrf_token})
            assert response.status_code == 200
            assert "Logged out successfully" in response.json()["message"]
        finally:
            client.cookies.clear()

    def test_logout_no_token(self, client: TestClient) -> None:
        """Logout without token still returns success.

        Given: No active session,
        When: Calling logout,
        Then: Returns success message.
        """
        response = client.post("/api/auth/logout")
        assert response.status_code == 200
        assert "Logged out successfully" in response.json()["message"]

    def test_logout_invalid_token(self, client: TestClient) -> None:
        """Logout with invalid token still returns success.

        Given: Invalid bearer token,
        When: Calling logout,
        Then: Returns success message.
        """
        response = client.post(
            "/api/auth/logout", headers={"Authorization": "Bearer invalid_token"}
        )
        assert response.status_code == 200
        assert "Logged out successfully" in response.json()["message"]

    def test_me_endpoint_success(self, client: TestClient) -> None:
        """Me endpoint returns authenticated user.

        Given: Valid login session,
        When: Calling /me endpoint,
        Then: Returns current user profile.
        """
        login_response = client.post(
            "/api/auth/login",
            json={
                "session_id": "",
                "sequence_id": 0,
                "username": "admin",
                "password": "AdminSnapper2026!",
            },
        )
        assert login_response.status_code == 200
        client.cookies.update(login_response.cookies)
        response = client.get("/api/auth/me")
        assert response.status_code == 200
        data = response.json()
        assert data["username"] == "admin"
        assert data["role"] == "admin"

    def test_me_endpoint_no_token(self, auth_routes_app: FastAPI) -> None:
        """Me endpoint returns 401 without token.

        Given: No authentication,
        When: Calling /me endpoint,
        Then: Returns 401 error.
        """
        with TestClient(auth_routes_app) as fresh_client:
            response = fresh_client.get("/api/auth/me")
            assert response.status_code == 401

    def test_me_endpoint_invalid_token(self, auth_routes_app: FastAPI) -> None:
        """Me endpoint returns 401 with invalid token.

        Given: Invalid access token cookie,
        When: Calling /me endpoint,
        Then: Returns 401 error.
        """
        with TestClient(auth_routes_app) as fresh_client:
            fresh_client.cookies.set("access_token", "invalid_token")
            response = fresh_client.get("/api/auth/me")
            assert response.status_code == 401


class TestTokenManager:
    """Tests for JWT token manager."""

    def test_token_creation_and_verification(self) -> None:
        """Token manager creates and verifies tokens.

        Given: A user profile,
        When: Creating and verifying tokens,
        Then: Returns valid token data with user info.
        """
        token_manager = get_token_manager()
        user = AuthPrincipal(
            username="testuser",
            email="test@example.com",
            role=UserRole.OPERATOR,
            is_active=True,
        )
        tokens = token_manager.create_tokens(user)
        assert tokens.access_token
        assert tokens.refresh_token
        assert tokens.token_type == "bearer"
        token_data = token_manager.verify_token(tokens.access_token)
        assert token_data is not None
        assert token_data.sub == user.username
        assert token_data.username == user.username
        assert token_data.role == user.role
        refresh_data = token_manager.verify_token(tokens.refresh_token)
        assert refresh_data is not None
        assert refresh_data.sub == user.username

    def test_token_creation_with_remember_me(self) -> None:
        """Token creation with remember_me flag.

        Given: A user profile,
        When: Creating tokens with remember_me=True,
        Then: Returns valid refresh token.
        """
        token_manager = get_token_manager()
        user = AuthPrincipal(
            username="testuser",
            role=UserRole.VIEWER,
            is_active=True,
        )
        tokens = token_manager.create_tokens(user, remember_me=True)
        assert tokens.access_token
        assert tokens.refresh_token
        refresh_data = token_manager.verify_token(tokens.refresh_token)
        assert refresh_data is not None

    def test_refresh_token_functionality(self) -> None:
        """Refresh tokens returns new token pair.

        Given: Valid initial tokens,
        When: Refreshing tokens,
        Then: Returns different access and refresh tokens.
        """
        token_manager = get_token_manager()
        user = AuthPrincipal(
            username="testuser",
            role=UserRole.ADMIN,
            is_active=True,
        )
        initial_tokens = token_manager.create_tokens(user)
        new_tokens = token_manager.refresh_tokens(initial_tokens.refresh_token)
        assert new_tokens is not None
        assert new_tokens.access_token != initial_tokens.access_token
        assert new_tokens.refresh_token != initial_tokens.refresh_token

    def test_invalid_token_verification(self) -> None:
        """Invalid token verification returns None.

        Given: Invalid or empty token strings,
        When: Verifying tokens,
        Then: Returns None.
        """
        token_manager = get_token_manager()
        result = token_manager.verify_token("invalid_token")
        assert result is None
        result = token_manager.verify_token("")
        assert result is None

    def test_blacklist_functionality(self) -> None:
        """Blacklisted token fails verification.

        Given: Valid token that gets blacklisted,
        When: Verifying after blacklist,
        Then: Returns None.
        """
        token_manager = get_token_manager()
        user = AuthPrincipal(
            username="testuser",
            role=UserRole.OPERATOR,
            is_active=True,
        )
        tokens = token_manager.create_tokens(user)
        token_data = token_manager.verify_token(tokens.access_token)
        assert token_data is not None
        token_data = token_manager.verify_token(tokens.access_token)
        assert token_data is not None
        jti = token_data.jti
        token_manager.blacklist_token_immediately(jti)
        token_data = token_manager.verify_token(tokens.access_token)
        assert token_data is None

    def test_token_expiration_handling(self) -> None:
        """Malformed token verification returns None.

        Given: Malformed token string,
        When: Verifying token,
        Then: Returns None.
        """
        token_manager = get_token_manager()
        result = token_manager.verify_token("malformed.token.here")
        assert result is None


class TestWebSocketAuthManager:
    """Tests for WebSocket authentication manager."""

    def test_ws_auth_manager_singleton(self) -> None:
        """WebSocketAuthManager is singleton.

        Given: Multiple calls to get_ws_auth_manager,
        When: Comparing instances,
        Then: Same instance returned.
        """
        manager1 = get_ws_auth_manager()
        manager2 = get_ws_auth_manager()
        assert manager1 is manager2

    def test_verify_session_cookie_no_token(self) -> None:
        """Verify session cookie returns None without token.

        Given: WebSocket with no cookies,
        When: Verifying session cookie,
        Then: Returns None.
        """
        ws_auth_manager = get_ws_auth_manager()
        websocket = MagicMock(spec=WebSocket)
        websocket.cookies = {}
        assert ws_auth_manager.verify_session_cookie(websocket) is None

    def test_verify_session_cookie_invalid_token(self) -> None:
        """Verify session cookie returns None with invalid token.

        Given: WebSocket with invalid access_token cookie,
        When: Verifying session cookie,
        Then: Returns None.
        """
        ws_auth_manager = get_ws_auth_manager()
        websocket = MagicMock(spec=WebSocket)
        websocket.cookies = {"access_token": "invalid"}
        assert ws_auth_manager.verify_session_cookie(websocket) is None

    def test_verify_session_cookie_valid_token(self) -> None:
        """Verify session cookie returns user and token data.

        Given: WebSocket with valid access_token cookie,
        When: Verifying session cookie,
        Then: Returns user profile and token claims.
        """
        ws_auth_manager = get_ws_auth_manager()
        token_manager = get_token_manager()
        user = AuthPrincipal(
            username="testuser",
            role=UserRole.OPERATOR,
            is_active=True,
        )
        tokens = token_manager.create_tokens(user)
        websocket = MagicMock(spec=WebSocket)
        websocket.cookies = {"access_token": tokens.access_token}
        result = ws_auth_manager.verify_session_cookie(websocket)
        assert result is not None
        authenticated_user, token_data = result
        assert authenticated_user.username == user.username
        assert token_data.sid

    def test_connection_tracking(self) -> None:
        """Connection tracking manages authenticated connections.

        Given: A WebSocket and user,
        When: Tracking connection lifecycle,
        Then: Correctly reports auth state and permissions.
        """
        ws_auth_manager = get_ws_auth_manager()
        websocket = MagicMock(spec=WebSocket)
        user = AuthPrincipal(
            username="testuser",
            role=UserRole.VIEWER,
            is_active=True,
        )
        assert not ws_auth_manager.is_authenticated(websocket)
        assert ws_auth_manager.get_authenticated_user(websocket) is None
        ws_auth_manager.authenticated_connections[websocket] = user
        assert ws_auth_manager.is_authenticated(websocket)
        assert ws_auth_manager.get_authenticated_user(websocket) == user
        assert ws_auth_manager.has_permission(websocket, UserRole.VIEWER)
        assert not ws_auth_manager.has_permission(websocket, UserRole.ADMIN)
        ws_auth_manager.disconnect(websocket)
        assert not ws_auth_manager.is_authenticated(websocket)

    def test_connection_stats(self) -> None:
        """Connection stats returns role breakdown.

        Given: Multiple authenticated connections,
        When: Getting connection stats,
        Then: Returns total count and role breakdown.
        """
        ws_auth_manager = get_ws_auth_manager()
        ws_auth_manager.authenticated_connections.clear()
        ws1 = MagicMock(spec=WebSocket)
        ws2 = MagicMock(spec=WebSocket)
        ws3 = MagicMock(spec=WebSocket)
        user1 = AuthPrincipal(username="user1", role=UserRole.VIEWER, is_active=True)
        user2 = AuthPrincipal(username="user2", role=UserRole.OPERATOR, is_active=True)
        user3 = AuthPrincipal(username="user3", role=UserRole.ADMIN, is_active=True)
        ws_auth_manager.authenticated_connections[ws1] = user1
        ws_auth_manager.authenticated_connections[ws2] = user2
        ws_auth_manager.authenticated_connections[ws3] = user3
        stats = ws_auth_manager.get_connection_stats()
        assert isinstance(stats, AuthConnectionStats)
        assert stats.total_authenticated == 3
        assert stats.role_breakdown["viewer"] == 1
        assert stats.role_breakdown["operator"] == 1
        assert stats.role_breakdown["admin"] == 1


class TestSecureWebSocketUtilities:
    """Tests for WebSocket utility functions."""

    def test_topic_filtering_for_roles(self) -> None:
        """Topic filtering varies by role.

        Given: Different user roles,
        When: Getting allowed topics,
        Then: Returns appropriate topic lists per role.
        """
        viewer_topics = get_allowed_topics_for_role(UserRole.VIEWER)
        assert "market." in viewer_topics
        assert "system.heartbeats." in viewer_topics
        operator_topics = get_allowed_topics_for_role(UserRole.OPERATOR)
        assert "signals" in operator_topics or any(t.startswith("signals") for t in operator_topics)
        admin_topics = get_allowed_topics_for_role(UserRole.ADMIN)
        assert "signals" in admin_topics or any(t.startswith("signals") for t in admin_topics)
        assert "system.heartbeats." in admin_topics

    def test_trading_permissions(self) -> None:
        """Trading permission based on role.

        Given: Different user roles,
        When: Checking trading permission,
        Then: Only operator and admin have permission.
        """
        assert not has_trading_permission(UserRole.VIEWER)
        assert has_trading_permission(UserRole.OPERATOR)
        assert has_trading_permission(UserRole.ADMIN)


class TestUserManagementCoverage:
    """Coverage tests for user management functionality."""

    def test_user_role_enum_values(self) -> None:
        """UserRole enum has correct string values.

        Given: UserRole enum,
        When: Accessing value property,
        Then: Returns lowercase role names.
        """
        assert UserRole.ADMIN.value == "admin"
        assert UserRole.OPERATOR.value == "operator"
        assert UserRole.VIEWER.value == "viewer"

    def test_role_permissions_mapping(self) -> None:
        """Role permissions mapping is complete.

        Given: ROLE_PERMISSIONS mapping,
        When: Checking permissions per role,
        Then: Each role has appropriate permissions.
        """
        assert UserRole.ADMIN in ROLE_PERMISSIONS
        assert UserRole.OPERATOR in ROLE_PERMISSIONS
        assert UserRole.VIEWER in ROLE_PERMISSIONS
        admin_perms = ROLE_PERMISSIONS[UserRole.ADMIN]
        assert Permission.MANAGE_USERS in admin_perms
        assert Permission.READ_MARKET_DATA in admin_perms
        assert Permission.CREATE_ORDERS in admin_perms
        operator_perms = ROLE_PERMISSIONS[UserRole.OPERATOR]
        assert Permission.CREATE_ORDERS in operator_perms
        assert Permission.READ_MARKET_DATA in operator_perms
        assert Permission.MANAGE_USERS not in operator_perms
        viewer_perms = ROLE_PERMISSIONS[UserRole.VIEWER]
        assert Permission.READ_MARKET_DATA in viewer_perms
        assert Permission.CREATE_ORDERS not in viewer_perms

    def test_user_role_comparison(self) -> None:
        """UserRole enum supports equality comparison.

        Given: UserRole enum values,
        When: Comparing with ==,
        Then: Same values are equal.
        """
        assert UserRole.ADMIN == UserRole.ADMIN

    def test_user_role_string_representation(self) -> None:
        """UserRole enum has proper string representation.

        Given: UserRole enum values,
        When: Converting to string,
        Then: Returns lowercase value (StrEnum behavior).
        """
        assert str(UserRole.ADMIN) == "admin"
        assert str(UserRole.OPERATOR) == "operator"
        assert str(UserRole.VIEWER) == "viewer"

    def test_permission_inheritance(self) -> None:
        """Permissions are inherited hierarchically.

        Given: Role permissions,
        When: Comparing permission sets,
        Then: Lower roles are subsets of higher roles.
        """
        admin_perms = ROLE_PERMISSIONS[UserRole.ADMIN]
        operator_perms = ROLE_PERMISSIONS[UserRole.OPERATOR]
        viewer_perms = ROLE_PERMISSIONS[UserRole.VIEWER]
        assert operator_perms.issubset(admin_perms)
        assert viewer_perms.issubset(operator_perms)

    def test_user_service_import(self) -> None:
        """UserService is singleton.

        Given: Multiple calls to get_user_service,
        When: Comparing instances,
        Then: Same instance returned.
        """
        service = get_user_service()
        assert service is not None
        service2 = get_user_service()
        assert service is service2

    def test_auth_models_import(self) -> None:
        """Auth models can be instantiated.

        Given: CreateUserRequest model,
        When: Creating instance,
        Then: Fields populated correctly.
        """
        create_req = CreateUserRequest(
            session_id="test-sid",
            sequence_id=1,
            username="testuser",
            password="password123",
            role=UserRole.VIEWER,
        )
        assert create_req.username == "testuser"
        assert create_req.role == UserRole.VIEWER

    def test_user_model_fields(self) -> None:
        """UserProfile model has all required fields.

        Given: UserProfile instantiation,
        When: Accessing fields,
        Then: All fields accessible and correct.
        """
        user = UserProfile(
            session_id="test-sid",
            sequence_id=1,
            username="testuser",
            email="test@example.com",
            role=UserRole.ADMIN,
            is_active=True,
        )
        assert user.username == "testuser"
        assert user.username == "testuser"
        assert user.email == "test@example.com"
        assert user.role == UserRole.ADMIN
        assert user.is_active is True

    def test_permission_enum_values(self) -> None:
        """Permission enum has correct string values.

        Given: Permission enum,
        When: Accessing value property,
        Then: Returns colon-separated permission strings.
        """
        assert Permission.READ_MARKET_DATA.value == "read:market_data"
        assert Permission.CREATE_ORDERS.value == "create:orders"
        assert Permission.MANAGE_USERS.value == "manage:users"
        assert Permission.READ_STRATEGIES.value == "read:strategies"

    def test_role_conversion_logic(self) -> None:
        """UserRole converts between string and enum.

        Given: Role as string or enum,
        When: Converting between formats,
        Then: Conversion is bidirectional.
        """
        role_str = "admin"
        role_enum = UserRole(role_str)
        assert role_enum == UserRole.ADMIN
        role_enum = UserRole.OPERATOR
        role_str = role_enum.value
        assert role_str == "operator"

    def test_password_validation_helpers(self) -> None:
        """Password validation requires length and digit.

        Given: Various password strings,
        When: Validating strength,
        Then: Only valid passwords pass.
        """

        def validate_password_strength(password: str) -> bool:
            return len(password) >= 8 and any(c.isdigit() for c in password)

        assert validate_password_strength("password123")
        assert not validate_password_strength("weak")
        assert not validate_password_strength("nodigits")

    def test_error_handling_scenarios(self) -> None:
        """Invalid role string raises ValueError.

        Given: Invalid role string,
        When: Creating UserRole,
        Then: Raises ValueError.
        """
        with pytest.raises(ValueError):
            UserRole("invalid_role")

    def test_token_data_model(self) -> None:
        """TokenClaims model holds all JWT fields.

        Given: TokenClaims instantiation,
        When: Accessing fields,
        Then: All fields accessible and correct.
        """
        token_data = TokenClaims(
            sub="user123",
            username="testuser",
            role=UserRole.ADMIN,
            permissions=["read:market_data", "create:orders"],
            exp=1234567890,
            iat=1234567800,
            jti="token_id_123",
            sid="session-coverage",
        )
        assert token_data.sub == "user123"
        assert token_data.username == "testuser"
        assert token_data.role == UserRole.ADMIN
        assert len(token_data.permissions) == 2

    def test_login_request_model(self) -> None:
        """LoginRequest model holds credentials.

        Given: LoginRequest instantiation,
        When: Accessing fields,
        Then: All fields accessible and correct.
        """
        login_req = LoginRequest(
            session_id="test-sid",
            sequence_id=1,
            username="testuser",
            password="password123",
            remember_me=True,
        )
        assert login_req.username == "testuser"
        assert login_req.password == "password123"
        assert login_req.remember_me is True

    def test_websocket_auth_models(self) -> None:
        """WebSocket auth models hold auth data.

        Given: Auth message and response models,
        When: Creating instances,
        Then: Fields populated correctly with correct types.
        """
        auth_msg = WebSocketAuthMessage(session_id="test-sid", sequence_id=1, token="test_token")
        assert auth_msg.type == "auth"
        assert auth_msg.token == "test_token"
        auth_resp = WebSocketAuthResponse(
            session_id="test-sid",
            sequence_id=1,
            success=True,
            user_id="user123",
            role=UserRole.ADMIN,
        )
        assert auth_resp.type == "auth_response"
        assert auth_resp.success is True
        assert auth_resp.user_id == "user123"
        assert auth_resp.role == UserRole.ADMIN
