"""Tests for WebSocket authentication schemas and manager."""

import asyncio
from datetime import UTC
from datetime import datetime
from typing import Any
from typing import cast

import pytest

from snapper.api.auth.schemas.ws_token import WsTokenPayload
from snapper.api.auth.services.ws_token_service import compute_sid_hash
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.schemas.tokens import TokenClaims
from snapper.auth.tokens import TOKEN_TYPE_ACCESS
from snapper.auth.websocket_auth import AuthConnectionStats
from snapper.auth.websocket_auth import ConnectionState
from snapper.auth.websocket_auth import WebSocketAuthManager
from snapper.auth.websocket_auth import get_ws_auth_manager


class DummyWebSocket:
    """Fake WebSocket for testing cookie + header access.

    Matches :class:`fastapi.WebSocket` for the fields the
    :class:`WebSocketAuthManager.verify_session_cookie` path reads —
    header (for the Bearer token fast-path) and cookie
    (for the browser fallback).
    """

    def __init__(self) -> None:
        """Initialize the instance."""
        self.cookies: dict[str, str] = {}
        self.headers: dict[str, str] = {}


class DummyTokenManager:
    """Fake token manager returning preconfigured verification response."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.verify_response: TokenClaims | None = None
        self.last_expected_token_type: str | None = None

    def verify_token(self, token: str) -> TokenClaims | None:
        """Return preconfigured token claims (sync JWT+blacklist layer)."""
        return self.verify_response

    async def verify_token_with_db(
        self,
        token: str,
        repository: object,
        *,
        expected_token_type: str,
    ) -> TokenClaims | None:
        """DB-backed verify stub: records the demanded purpose, echoes the verdict."""
        await asyncio.sleep(0)
        self.last_expected_token_type = expected_token_type
        return self.verify_response


def _create_manager() -> tuple[WebSocketAuthManager, DummyTokenManager]:
    manager = WebSocketAuthManager()
    token_manager = DummyTokenManager()
    manager.token_manager = cast(Any, token_manager)
    return manager, token_manager


def _token_data() -> TokenClaims:
    return TokenClaims(
        sub="user-1",
        username="alice",
        role=UserRole.OPERATOR,
        permissions=["read:orders"],
        exp=int(datetime.now(UTC).timestamp()) + 600,
        iat=int(datetime.now(UTC).timestamp()),
        jti="token-1",
        sid="session-123",
    )


def _ws_payload(session_id: str) -> WsTokenPayload:
    now_ts = int(datetime.now(UTC).timestamp())
    return WsTokenPayload(
        purpose="ws_connect",
        sub="user-1",
        sid_hash=compute_sid_hash(session_id),
        iat=now_ts,
        exp=now_ts + 600,
        jti="ws-1",
    )


@pytest.mark.asyncio
async def test_verify_session_cookie_success() -> None:
    """Verify session cookie verification returns user and token data.

    Given: A websocket with valid access_token cookie,
    When: verify_session_cookie is called,
    Then: User profile and token claims are returned, and the upgrade
        demanded the access purpose — a WebSocket that accepted a
        refresh credential would hand a 7-30 day token a live socket.
    """
    manager, token_manager = _create_manager()
    token_manager.verify_response = _token_data()
    websocket = DummyWebSocket()
    websocket.cookies["access_token"] = "valid"
    result = await manager.verify_session_cookie(cast(Any, websocket), cast(Any, object()))
    assert result is not None
    user, token_data = result
    assert user.username == "alice"
    assert token_data.sid == "session-123"
    assert token_manager.last_expected_token_type == TOKEN_TYPE_ACCESS


@pytest.mark.asyncio
async def test_verify_session_cookie_missing_cookie() -> None:
    """Verify missing cookie returns None.

    Given: A websocket without access_token cookie,
    When: verify_session_cookie is called,
    Then: None is returned.
    """
    manager, _token_manager = _create_manager()
    websocket = DummyWebSocket()
    result = await manager.verify_session_cookie(cast(Any, websocket), cast(Any, object()))
    assert result is None


def test_register_connection_tracks_state() -> None:
    """Verify register_connection creates connection state.

    Given: A websocket and valid token data,
    When: register_connection is called,
    Then: Connection state is tracked with session info.
    """
    manager, _token_manager = _create_manager()
    token_data = _token_data()
    payload = _ws_payload(token_data.sid)
    websocket = DummyWebSocket()
    manager.register_connection(
        cast(Any, websocket),
        AuthPrincipal(username="alice", role=UserRole.OPERATOR),
        token_data,
        payload,
        warn_task=None,
        hard_task=None,
    )
    assert manager.is_authenticated(cast(Any, websocket))
    state = manager.get_state(cast(Any, websocket))
    assert isinstance(state, ConnectionState)
    assert state.session_id == token_data.sid
    assert state.ws_token_expiration is not None
    assert state.warn_task is None
    assert state.hard_task is None


def test_update_ws_token_state_replaces_tasks() -> None:
    """Verify update_ws_token_state replaces timeout tasks.

    Given: A registered connection with existing tasks,
    When: update_ws_token_state is called with new tasks,
    Then: Old tasks are cancelled and new state is applied.
    """
    asyncio.run(_run_update_ws_token_state())


async def _run_update_ws_token_state() -> None:
    manager, _token_manager = _create_manager()
    token_data = _token_data()
    initial_payload = _ws_payload(token_data.sid)
    websocket = DummyWebSocket()

    async def _pending() -> None:
        await asyncio.sleep(1)

    warn_task = asyncio.create_task(_pending())
    hard_task = asyncio.create_task(_pending())
    manager.register_connection(
        cast(Any, websocket),
        AuthPrincipal(username="alice", role=UserRole.OPERATOR),
        token_data,
        initial_payload,
        warn_task=warn_task,
        hard_task=hard_task,
    )
    new_payload = WsTokenPayload(
        purpose="ws_connect",
        sub="user-1",
        sid_hash=compute_sid_hash(token_data.sid),
        iat=initial_payload.iat,
        exp=initial_payload.exp + 120,
        jti="ws-2",
    )
    new_warn = asyncio.create_task(asyncio.sleep(0))
    new_hard = asyncio.create_task(asyncio.sleep(0))
    manager.update_ws_token_state(
        cast(Any, websocket),
        new_payload,
        warn_task=new_warn,
        hard_task=new_hard,
    )
    await asyncio.sleep(0)
    assert warn_task.cancelled()
    assert hard_task.cancelled()
    state = manager.get_state(cast(Any, websocket))
    assert state is not None
    assert state.ws_token_jti == "ws-2"
    assert state.session_expires_at == state.ws_token_expiration
    new_warn.cancel()
    new_hard.cancel()
    await asyncio.gather(new_warn, new_hard, return_exceptions=True)
    manager.disconnect(cast(Any, websocket))


def test_connection_stats() -> None:
    """Verify get_connection_stats returns role breakdown.

    Given: Multiple authenticated connections with different roles,
    When: get_connection_stats is called,
    Then: Stats include total count and role breakdown.
    """
    manager, _token_manager = _create_manager()
    ws1 = DummyWebSocket()
    ws2 = DummyWebSocket()
    manager.authenticated_connections[cast(Any, ws1)] = AuthPrincipal(
        username="viewer", role=UserRole.VIEWER
    )
    manager.authenticated_connections[cast(Any, ws2)] = AuthPrincipal(
        username="admin", role=UserRole.ADMIN
    )
    stats = manager.get_connection_stats()
    assert isinstance(stats, AuthConnectionStats)
    assert stats.total_authenticated == 2
    assert stats.role_breakdown[UserRole.VIEWER.value] == 1
    assert stats.role_breakdown[UserRole.ADMIN.value] == 1


class DummyWebSocketV2:
    """Fake WebSocket V2 for testing cookie access."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.cookies: dict[str, str] = {}


class DummyTokenManagerV2:
    """Fake token manager V2 returning preconfigured verification response."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.verify_response: TokenClaims | None = None

    def verify_token(self, token: str) -> TokenClaims | None:
        """Return preconfigured token claims."""
        return self.verify_response


def _token_data_v2() -> TokenClaims:
    return TokenClaims(
        sub="user-1",
        username="alice",
        role=UserRole.OPERATOR,
        permissions=["read:orders"],
        exp=int(datetime.now(UTC).timestamp()) + 600,
        iat=int(datetime.now(UTC).timestamp()),
        jti="token-1",
        sid="session-123",
    )


def _ws_payload_v2(session_id: str) -> WsTokenPayload:
    now_ts = int(datetime.now(UTC).timestamp())
    return WsTokenPayload(
        purpose="ws_connect",
        sub="user-1",
        sid_hash=compute_sid_hash(session_id),
        iat=now_ts,
        exp=now_ts + 600,
        jti="ws-1",
    )


def test_singleton_does_not_create_second_instance() -> None:
    """Verify WebSocketAuthManager follows singleton pattern.

    Given: WebSocketAuthManager instance cleared,
    When: Multiple instances are created,
    Then: All references point to the same instance.
    """
    WebSocketAuthManager.clear_instance()
    manager1 = WebSocketAuthManager()
    manager2 = WebSocketAuthManager()
    assert manager1 is manager2
    assert id(manager1) == id(manager2)
    WebSocketAuthManager.clear_instance()


@pytest.mark.asyncio
async def test_verify_session_cookie_with_invalid_token() -> None:
    """Verify invalid token cookie returns None.

    Given: A websocket with invalid access_token cookie,
    When: verify_session_cookie is called,
    Then: None is returned.
    """
    WebSocketAuthManager.clear_instance()
    manager = WebSocketAuthManager()
    token_manager = DummyTokenManager()
    token_manager.verify_response = None
    manager.token_manager = cast(Any, token_manager)
    websocket = DummyWebSocket()
    websocket.cookies["access_token"] = "invalid"
    result = await manager.verify_session_cookie(cast(Any, websocket), cast(Any, object()))
    assert result is None
    WebSocketAuthManager.clear_instance()


def test_update_ws_token_state_when_state_missing() -> None:
    """Verify update_ws_token_state handles missing state.

    Given: A websocket not registered with manager,
    When: update_ws_token_state is called,
    Then: State remains None without error.
    """
    WebSocketAuthManager.clear_instance()
    manager = WebSocketAuthManager()
    token_manager = DummyTokenManager()
    manager.token_manager = cast(Any, token_manager)
    websocket = DummyWebSocket()
    payload = _ws_payload("session-123")
    manager.update_ws_token_state(
        cast(Any, websocket),
        payload,
        warn_task=None,
        hard_task=None,
    )
    state = manager.get_state(cast(Any, websocket))
    assert state is None
    WebSocketAuthManager.clear_instance()


def test_cancel_tasks_cancels_both_tasks() -> None:
    """Verify disconnect cancels both warn and hard tasks.

    Given: A connection with active warn and hard tasks,
    When: disconnect is called,
    Then: Both tasks are cancelled.
    """
    asyncio.run(_run_cancel_tasks_test())


async def _run_cancel_tasks_test() -> None:
    WebSocketAuthManager.clear_instance()
    manager = WebSocketAuthManager()
    token_manager = DummyTokenManager()
    manager.token_manager = cast(Any, token_manager)
    token_data = _token_data()
    payload = _ws_payload(token_data.sid)
    websocket = DummyWebSocket()

    async def _pending() -> None:
        await asyncio.sleep(10)

    warn_task = asyncio.create_task(_pending())
    hard_task = asyncio.create_task(_pending())
    manager.register_connection(
        cast(Any, websocket),
        AuthPrincipal(username="alice", role=UserRole.OPERATOR),
        token_data,
        payload,
        warn_task=warn_task,
        hard_task=hard_task,
    )
    manager.disconnect(cast(Any, websocket))
    await asyncio.sleep(0.1)
    assert warn_task.cancelled()
    assert hard_task.cancelled()
    await asyncio.gather(warn_task, hard_task, return_exceptions=True)
    WebSocketAuthManager.clear_instance()


def test_cancel_tasks_with_none_warn_task() -> None:
    """Verify disconnect handles None warn_task.

    Given: A connection with only hard_task set,
    When: disconnect is called,
    Then: hard_task is cancelled without error.
    """
    asyncio.run(_run_cancel_tasks_none_warn_test())


async def _run_cancel_tasks_none_warn_test() -> None:
    WebSocketAuthManager.clear_instance()
    manager = WebSocketAuthManager()
    token_manager = DummyTokenManager()
    manager.token_manager = cast(Any, token_manager)
    token_data = _token_data()
    payload = _ws_payload(token_data.sid)
    websocket = DummyWebSocket()

    async def _pending() -> None:
        await asyncio.sleep(10)

    hard_task = asyncio.create_task(_pending())
    manager.register_connection(
        cast(Any, websocket),
        AuthPrincipal(username="alice", role=UserRole.OPERATOR),
        token_data,
        payload,
        warn_task=None,
        hard_task=hard_task,
    )
    manager.disconnect(cast(Any, websocket))
    await asyncio.sleep(0.1)
    assert hard_task.cancelled()
    await asyncio.gather(hard_task, return_exceptions=True)
    WebSocketAuthManager.clear_instance()


def test_cancel_tasks_with_none_hard_task() -> None:
    """Verify disconnect handles None hard_task.

    Given: A connection with only warn_task set,
    When: disconnect is called,
    Then: warn_task is cancelled without error.
    """
    asyncio.run(_run_cancel_tasks_none_hard_test())


async def _run_cancel_tasks_none_hard_test() -> None:
    WebSocketAuthManager.clear_instance()
    manager = WebSocketAuthManager()
    token_manager = DummyTokenManager()
    manager.token_manager = cast(Any, token_manager)
    token_data = _token_data()
    payload = _ws_payload(token_data.sid)
    websocket = DummyWebSocket()

    async def _pending() -> None:
        await asyncio.sleep(10)

    warn_task = asyncio.create_task(_pending())
    manager.register_connection(
        cast(Any, websocket),
        AuthPrincipal(username="alice", role=UserRole.OPERATOR),
        token_data,
        payload,
        warn_task=warn_task,
        hard_task=None,
    )
    manager.disconnect(cast(Any, websocket))
    await asyncio.sleep(0.1)
    assert warn_task.cancelled()
    await asyncio.gather(warn_task, return_exceptions=True)
    WebSocketAuthManager.clear_instance()


def test_get_session_id_returns_none_when_no_state() -> None:
    """Verify get_session_id returns None for unregistered connection.

    Given: A websocket not registered with manager,
    When: get_session_id is called,
    Then: None is returned.
    """
    WebSocketAuthManager.clear_instance()
    manager = WebSocketAuthManager()
    token_manager = DummyTokenManager()
    manager.token_manager = cast(Any, token_manager)
    websocket = DummyWebSocket()
    result = manager.get_session_id(cast(Any, websocket))
    assert result is None
    WebSocketAuthManager.clear_instance()


def test_get_connection_expiration_returns_none_when_no_state() -> None:
    """Verify get_connection_expiration returns None for unregistered.

    Given: A websocket not registered with manager,
    When: get_connection_expiration is called,
    Then: None is returned.
    """
    WebSocketAuthManager.clear_instance()
    manager = WebSocketAuthManager()
    token_manager = DummyTokenManager()
    manager.token_manager = cast(Any, token_manager)
    websocket = DummyWebSocket()
    result = manager.get_connection_expiration(cast(Any, websocket))
    assert result is None
    WebSocketAuthManager.clear_instance()


def test_get_ws_token_expiration_returns_none_when_no_state() -> None:
    """Verify get_ws_token_expiration returns None for unregistered.

    Given: A websocket not registered with manager,
    When: get_ws_token_expiration is called,
    Then: None is returned.
    """
    WebSocketAuthManager.clear_instance()
    manager = WebSocketAuthManager()
    token_manager = DummyTokenManager()
    manager.token_manager = cast(Any, token_manager)
    websocket = DummyWebSocket()
    result = manager.get_ws_token_expiration(cast(Any, websocket))
    assert result is None
    WebSocketAuthManager.clear_instance()


def test_get_expirations_return_values_when_state_exists() -> None:
    """Verify expiration getters return values when state exists.

    Given: A registered connection with token data,
    When: Expiration getters are called,
    Then: Correct expiration timestamps are returned.
    """
    WebSocketAuthManager.clear_instance()
    manager = WebSocketAuthManager()
    token_manager = DummyTokenManager()
    manager.token_manager = cast(Any, token_manager)
    token_data = _token_data()
    payload = _ws_payload(token_data.sid)
    websocket = DummyWebSocket()
    manager.register_connection(
        cast(Any, websocket),
        AuthPrincipal(username="alice", role=UserRole.OPERATOR),
        token_data,
        payload,
        warn_task=None,
        hard_task=None,
    )
    conn_exp = manager.get_connection_expiration(cast(Any, websocket))
    ws_exp = manager.get_ws_token_expiration(cast(Any, websocket))
    assert conn_exp is not None and conn_exp == datetime.fromtimestamp(token_data.exp, UTC)
    assert ws_exp is not None and ws_exp == datetime.fromtimestamp(payload.exp, UTC)
    WebSocketAuthManager.clear_instance()


def test_get_ws_auth_manager_returns_existing_instance() -> None:
    """Verify get_ws_auth_manager returns singleton instance.

    Given: An existing WebSocketAuthManager instance,
    When: get_ws_auth_manager is called,
    Then: The same instance is returned.
    """
    WebSocketAuthManager.clear_instance()
    first = WebSocketAuthManager()
    returned = WebSocketAuthManager.get_instance()
    assert returned is first
    resolved = get_ws_auth_manager()
    assert resolved is first
    WebSocketAuthManager.clear_instance()
