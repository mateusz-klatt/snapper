"""WebSocket authentication module.

This module provides authentication management for WebSocket
connections including session tracking and role-based access control.
"""

import asyncio
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime

from fastapi import WebSocket
from loguru import logger

from snapper.api.auth.schemas.ws_token import WsTokenPayload
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.tokens import TokenClaims
from snapper.auth.schemas.user import UserProfile
from snapper.auth.tokens import get_token_manager


@dataclass(slots=True)
class AuthUserEntry:
    """Authenticated user summary for connection stats.

    Attributes:
        username: User's display name.
        role: User's role for the authenticated connection.
    """

    username: str
    role: UserRole


@dataclass(slots=True)
class AuthConnectionStats:
    """Statistics about authenticated WebSocket connections.

    Attributes:
        total_authenticated: Total number of authenticated connections.
        role_breakdown: Mapping of role values to connection counts.
        authenticated_users: List of authenticated user summaries.
    """

    total_authenticated: int = 0
    role_breakdown: dict[str, int] = field(default_factory=dict)
    authenticated_users: list[AuthUserEntry] = field(default_factory=list)


@dataclass(slots=True)
class ConnectionState:
    """WebSocket connection state.

    Tracks session information and expiration for authenticated
    WebSocket connections.

    Attributes:
        session_id: Session identifier.
        session_expires_at: Session expiration timestamp.
        ws_token_expiration: WebSocket token expiration.
        ws_token_jti: WebSocket token JWT ID.
        warn_task: Task for expiration warning.
        hard_task: Task for hard disconnection.
    """

    session_id: str
    session_expires_at: datetime
    ws_token_expiration: datetime | None = None
    ws_token_jti: str | None = None
    warn_task: asyncio.Task[None] | None = None
    hard_task: asyncio.Task[None] | None = None


class WebSocketAuthManager:
    """WebSocket authentication manager singleton.

    Manages authenticated WebSocket connections, tracks session state,
    and provides role-based access control.
    """

    _instance: WebSocketAuthManager | None = None
    _initialized: bool = False

    def __new__(cls) -> WebSocketAuthManager:
        """Create or return singleton WebSocket auth manager instance."""
        if cls._instance is None:
            instance = super().__new__(cls)
            cls._instance = instance
        return cls._instance

    def __init__(self) -> None:
        """Initialize the WebSocket auth manager."""
        if self._initialized:
            return
        self._initialized = True
        self.token_manager = get_token_manager()
        self.authenticated_connections: dict[WebSocket, UserProfile] = {}
        self._connection_states: dict[WebSocket, ConnectionState] = {}

    def verify_session_cookie(self, websocket: WebSocket) -> tuple[UserProfile, TokenClaims] | None:
        """Verify session from WebSocket cookies.

        Args:
            websocket: WebSocket connection.

        Returns:
            Tuple of (UserProfile, TokenClaims) if valid, None otherwise.
        """
        token = websocket.cookies.get("access_token")
        if not token:
            return None
        token_data = self.token_manager.verify_token(token)
        if not token_data:
            return None
        user = UserProfile(
            username=token_data.username,
            role=token_data.role,
        )
        return user, token_data

    def register_connection(
        self,
        websocket: WebSocket,
        user: UserProfile,
        token_data: TokenClaims,
        ws_payload: WsTokenPayload,
        *,
        warn_task: asyncio.Task[None] | None,
        hard_task: asyncio.Task[None] | None,
    ) -> None:
        """Register authenticated WebSocket connection.

        Args:
            websocket: WebSocket connection.
            user: Authenticated user profile.
            token_data: JWT token claims.
            ws_payload: WebSocket token payload.
            warn_task: Expiration warning task.
            hard_task: Hard disconnect task.
        """
        session_expires_at = datetime.fromtimestamp(token_data.exp, UTC)
        ws_expires_at = datetime.fromtimestamp(ws_payload.exp, UTC)
        state = ConnectionState(
            session_id=token_data.sid,
            session_expires_at=session_expires_at,
            ws_token_expiration=ws_expires_at,
            ws_token_jti=ws_payload.jti,
            warn_task=warn_task,
            hard_task=hard_task,
        )
        self.authenticated_connections[websocket] = user
        self._connection_states[websocket] = state
        logger.info(f"WebSocket authenticated for user '{user.username}'")

    def update_ws_token_state(
        self,
        websocket: WebSocket,
        payload: WsTokenPayload,
        *,
        warn_task: asyncio.Task[None] | None,
        hard_task: asyncio.Task[None] | None,
    ) -> None:
        """Update WebSocket token state after refresh.

        Args:
            websocket: WebSocket connection.
            payload: New WebSocket token payload.
            warn_task: New expiration warning task.
            hard_task: New hard disconnect task.
        """
        state = self._connection_states.get(websocket)
        if state is None:
            return
        self._cancel_tasks(state)
        state.ws_token_expiration = datetime.fromtimestamp(payload.exp, UTC)
        state.ws_token_jti = payload.jti
        state.warn_task = warn_task
        state.hard_task = hard_task
        state.session_expires_at = datetime.fromtimestamp(payload.exp, UTC)

    def get_authenticated_user(self, websocket: WebSocket) -> UserProfile | None:
        """Get authenticated user for connection.

        Args:
            websocket: WebSocket connection.

        Returns:
            UserProfile or None if not authenticated.
        """
        return self.authenticated_connections.get(websocket)

    def is_authenticated(self, websocket: WebSocket) -> bool:
        """Check if connection is authenticated.

        Args:
            websocket: WebSocket connection.

        Returns:
            True if authenticated.
        """
        return websocket in self.authenticated_connections

    def get_state(self, websocket: WebSocket) -> ConnectionState | None:
        """Get connection state.

        Args:
            websocket: WebSocket connection.

        Returns:
            ConnectionState or None.
        """
        return self._connection_states.get(websocket)

    def get_session_id(self, websocket: WebSocket) -> str | None:
        """Get session ID for connection.

        Args:
            websocket: WebSocket connection.

        Returns:
            Session ID or None.
        """
        state = self._connection_states.get(websocket)
        return state.session_id if state else None

    def disconnect(self, websocket: WebSocket) -> None:
        """Disconnect and cleanup WebSocket connection.

        Removes connection from tracking and cancels expiration tasks.

        Args:
            websocket: WebSocket connection to disconnect.
        """
        self.authenticated_connections.pop(websocket, None)
        state = self._connection_states.pop(websocket, None)
        if state:
            self._cancel_tasks(state)

    def has_permission(self, websocket: WebSocket, required_role: UserRole) -> bool:
        """Check if connection has required role level.

        Args:
            websocket: WebSocket connection.
            required_role: Minimum required role.

        Returns:
            True if user has required role or higher.
        """
        user = self.get_authenticated_user(websocket)
        if not user:
            return False
        role_hierarchy = {
            UserRole.VIEWER: 0,
            UserRole.OPERATOR: 1,
            UserRole.ADMIN: 2,
        }
        return role_hierarchy[user.role] >= role_hierarchy[required_role]

    def get_connection_stats(self) -> AuthConnectionStats:
        """Get statistics about authenticated connections.

        Returns:
            AuthConnectionStats with total count, role breakdown, and user list.
        """
        role_counts: dict[str, int] = {}
        for user in self.authenticated_connections.values():
            role_counts[user.role.value] = role_counts.get(user.role.value, 0) + 1
        return AuthConnectionStats(
            total_authenticated=len(self.authenticated_connections),
            role_breakdown=role_counts,
            authenticated_users=[
                AuthUserEntry(username=user.username, role=user.role)
                for user in self.authenticated_connections.values()
            ],
        )

    def get_connection_expiration(self, websocket: WebSocket) -> datetime | None:
        """Get session expiration for connection.

        Args:
            websocket: WebSocket connection.

        Returns:
            Expiration datetime or None.
        """
        state = self._connection_states.get(websocket)
        return state.session_expires_at if state else None

    def get_ws_token_expiration(self, websocket: WebSocket) -> datetime | None:
        """Get WebSocket token expiration for connection.

        Args:
            websocket: WebSocket connection.

        Returns:
            Token expiration datetime or None.
        """
        state = self._connection_states.get(websocket)
        return state.ws_token_expiration if state else None

    def _cancel_tasks(self, state: ConnectionState) -> None:
        """Cancel expiration tasks for connection state.

        Args:
            state: Connection state with tasks to cancel.
        """
        if state.warn_task is not None:
            state.warn_task.cancel()
            state.warn_task = None
        if state.hard_task is not None:
            state.hard_task.cancel()
            state.hard_task = None

    @classmethod
    def get_instance(cls) -> WebSocketAuthManager:
        """Get singleton instance.

        Returns:
            WebSocketAuthManager singleton.
        """
        if cls._instance is None:
            cls._instance = WebSocketAuthManager()
        return cls._instance

    @classmethod
    def clear_instance(cls) -> None:
        """Clear singleton for testing."""
        cls._instance = None


def get_ws_auth_manager() -> WebSocketAuthManager:
    """Get WebSocketAuthManager singleton.

    Returns:
        WebSocketAuthManager instance.
    """
    return WebSocketAuthManager.get_instance()
