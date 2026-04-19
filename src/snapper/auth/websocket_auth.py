"""WebSocket authentication module.

This module provides authentication management for WebSocket
connections including session tracking and role-based access control.
"""

import asyncio
import contextlib
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime

import zmq
import zmq.asyncio
from fastapi import WebSocket
from loguru import logger

from snapper.api.auth.schemas.ws_token import WsTokenPayload
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.schemas.tokens import TokenClaims
from snapper.auth.tokens import get_token_manager
from snapper.messaging.infrastructure.validated_socket import HWM_AUDIT
from snapper.messaging.infrastructure.validated_socket import ValidatedSubscriber
from snapper.messaging.infrastructure.validated_socket import apply_hwm
from snapper.messaging.schemas.data import UserDeactivatedData

_KILL_SWITCH_CLOSE_CODE = 4003
_KILL_SWITCH_REASON_FALLBACK = "account_deactivated"
_ADMIN_USER_DEACTIVATED_TOPIC = "admin.user_deactivated"
_ADMIN_LISTEN_RECV_BACKOFF_S = 0.1
_KILL_SWITCH_REASON_MAX_LEN = 120


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
        self.authenticated_connections: dict[WebSocket, AuthPrincipal] = {}
        self._connection_states: dict[WebSocket, ConnectionState] = {}
        self._admin_zmq_context: zmq.asyncio.Context | None = None
        self._admin_subscriber: ValidatedSubscriber | None = None
        self._admin_listen_task: asyncio.Task[None] | None = None
        self._admin_running: bool = False

    @staticmethod
    def _extract_ws_bearer_token(websocket: WebSocket) -> str | None:
        """Pull a Bearer JWT off the WebSocket upgrade ``Authorization`` header.

        Case-insensitive scheme match (RFC 7235); returns ``None`` when
        the header is absent or the scheme is not ``Bearer`` so the
        caller can fall through to cookie-based auth.

        Args:
            websocket: WebSocket whose ``headers`` carry the upgrade
                request headers.

        Returns:
            The stripped token string, or ``None`` if not a Bearer
            grant.
        """
        auth_header = websocket.headers.get("authorization")
        if not auth_header:
            return None
        parts = auth_header.split(None, 1)
        if len(parts) != 2 or parts[0].lower() != "bearer":
            return None
        token = parts[1].strip()
        return token or None

    def verify_session_cookie(
        self, websocket: WebSocket
    ) -> tuple[AuthPrincipal, TokenClaims] | None:
        """Verify session from WebSocket auth header or cookie.

        Per plan §3.7: the ``Authorization: Bearer <jwt>`` request
        header is consulted FIRST on the WebSocket upgrade; the
        ``access_token`` cookie is the fallback. MCP / CLI clients
        without cookie jars present the header; browser clients
        continue to use the cookie.

        The method name is preserved for call-site stability — the
        semantics are now "verify session token transport, header or
        cookie", not strictly "cookie".

        Args:
            websocket: WebSocket connection.

        Returns:
            Tuple of (AuthPrincipal, TokenClaims) if valid, None otherwise.
        """
        token = self._extract_ws_bearer_token(websocket) or websocket.cookies.get("access_token")
        if not token:
            return None
        token_data = self.token_manager.verify_token(token)
        if not token_data:
            return None
        user = AuthPrincipal(
            username=token_data.username,
            role=token_data.role,
            user_public_id=token_data.user_public_id,
            operator_public_ids=token_data.operator_public_ids,
            primary_operator_public_id=token_data.primary_operator_public_id,
            active_wallet_public_id=token_data.active_wallet_public_id,
        )
        return user, token_data

    def register_connection(
        self,
        websocket: WebSocket,
        user: AuthPrincipal,
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

    def get_authenticated_user(self, websocket: WebSocket) -> AuthPrincipal | None:
        """Get authenticated user for connection.

        Args:
            websocket: WebSocket connection.

        Returns:
            AuthPrincipal or None if not authenticated.
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
            UserRole.AI_DELEGATE: -1,
            UserRole.VIEWER: 0,
            UserRole.OPERATOR: 1,
            UserRole.ADMIN: 2,
        }
        return role_hierarchy[user.role] >= role_hierarchy[required_role]

    async def start_admin_listener(self, zmq_broker_xpub: str) -> None:
        """Open the admin-bus subscriber and start the dispatch task.

        Subscribes to ``admin.user_deactivated`` (plan §3.6.1) so the
        kill switch fanout from `UserService.deactivate_user` (Day 3b
        sole publisher) reaches every authenticated WebSocket on this
        instance and closes it with code 4003 on the next event-loop
        tick. Idempotent: a second call while the listener is already
        running is a no-op so the FastAPI lifespan can re-enter the
        startup sequence safely.

        `admin.scope_revoked` subscription wiring is deferred to Day 3f
        (`validate_ai_delegate_subscription` per plan §3.8 — the
        re-validation algorithm needs the wallet-scope check that
        ships in that step).

        Args:
            zmq_broker_xpub: Address of the broker's XPUB endpoint.
                Empty string skips the listener entirely (test mode).
        """
        await asyncio.sleep(0)
        if self._admin_listen_task is not None:
            return
        if not zmq_broker_xpub:
            logger.info("WebSocketAuthManager: empty broker XPUB, skipping admin listener")
            return
        self._admin_zmq_context = zmq.asyncio.Context()
        raw_sub_socket = self._admin_zmq_context.socket(zmq.SUB)
        apply_hwm(raw_sub_socket, rcvhwm=HWM_AUDIT)
        raw_sub_socket.connect(zmq_broker_xpub)
        self._admin_subscriber = ValidatedSubscriber(raw_sub_socket)
        self._admin_subscriber.subscribe(_ADMIN_USER_DEACTIVATED_TOPIC)
        self._admin_running = True
        self._admin_listen_task = asyncio.create_task(self._admin_listen_loop())
        logger.info(
            "WebSocketAuthManager: admin-bus listener subscribed to {} on {}",
            _ADMIN_USER_DEACTIVATED_TOPIC,
            zmq_broker_xpub,
        )

    async def stop_admin_listener(self) -> None:
        """Cancel the dispatch task, close the subscriber, terminate the context.

        Order mirrors `_shutdown_user_service_publisher` (Day 3b):
        flip the running flag first so the loop exits on the next
        iteration, cancel the task, then dispose socket + context with
        suppressed exceptions so a transient broker issue cannot mask
        a clean shutdown. Idempotent.
        """
        self._admin_running = False
        task = self._admin_listen_task
        self._admin_listen_task = None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        if self._admin_subscriber is not None:
            with contextlib.suppress(Exception):
                self._admin_subscriber.close()
            self._admin_subscriber = None
        if self._admin_zmq_context is not None:
            with contextlib.suppress(Exception):
                self._admin_zmq_context.term()
            self._admin_zmq_context = None

    async def _admin_listen_loop(self) -> None:
        """Receive admin-bus events and dispatch to per-topic handlers.

        Per-message failures (parse errors, handler exceptions, recv
        errors) are caught + logged so a single bad frame can never
        silently stop the listener. Only `asyncio.CancelledError`
        from `stop_admin_listener` unwinds the loop. The recv +
        dispatch halves are factored into helpers so the loop body
        stays under the project's cognitive-complexity ceiling.
        """
        subscriber = self._admin_subscriber
        if subscriber is None:
            return
        try:
            while self._admin_running:
                frame = await self._admin_recv_one_frame(subscriber)
                if frame is None:
                    continue
                await self._admin_dispatch_frame(*frame)
        except asyncio.CancelledError:
            logger.info("WebSocketAuthManager: admin listen loop cancelled")
            raise

    async def _admin_recv_one_frame(
        self, subscriber: ValidatedSubscriber
    ) -> tuple[str, str] | None:
        """Receive and decode one admin-bus frame.

        Returns ``None`` (after a small backoff) when recv raises a
        non-cancellation error so the caller can simply ``continue``.
        """
        try:
            topic_bytes, payload_bytes = await subscriber.recv_multipart()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("WebSocketAuthManager admin listener recv failed: {}", exc)
            await asyncio.sleep(_ADMIN_LISTEN_RECV_BACKOFF_S)
            return None
        topic = topic_bytes.decode() if isinstance(topic_bytes, bytes) else str(topic_bytes)
        payload = payload_bytes.decode() if isinstance(payload_bytes, bytes) else str(payload_bytes)
        return topic, payload

    async def _admin_dispatch_frame(self, topic: str, payload: str) -> None:
        """Route one decoded admin-bus frame to its typed handler.

        Handler exceptions other than ``CancelledError`` are logged
        + swallowed so one bad frame cannot stop the listener.
        """
        try:
            if topic == _ADMIN_USER_DEACTIVATED_TOPIC:
                await self._handle_user_deactivated(UserDeactivatedData.from_json(payload))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                "WebSocketAuthManager admin handler failed: topic={} err={}",
                topic,
                exc,
            )

    async def _handle_user_deactivated(self, data: UserDeactivatedData) -> None:
        """Drop every authenticated WebSocket whose principal matches the deactivated user."""
        await self.close_user_connections(
            user_public_id=data.user_public_id,
            reason=data.reason or _KILL_SWITCH_REASON_FALLBACK,
        )

    async def close_user_connections(self, user_public_id: str, reason: str) -> int:
        """Close every authenticated WS matching `user_public_id` (code 4003).

        Iterates a snapshot so the `disconnect` side-effect (which
        mutates `authenticated_connections`) does not invalidate the
        iterator. A `ws.close()` failure (already-closed socket,
        broken pipe, etc.) is logged + swallowed so one stuck
        connection cannot block the rest from being torn down. Match
        is by `user_public_id` (stable UUID7); username is mutable so
        is intentionally NOT used here.

        Args:
            user_public_id: UUID7 of the user whose sessions are
                being terminated.
            reason: Free-text reason carried in the WS close frame.
                Truncated to 120 characters because RFC 6455 limits
                the close-reason field to 123 bytes.

        Returns:
            Number of connections that were closed.
        """
        truncated_reason = reason[:_KILL_SWITCH_REASON_MAX_LEN]
        closed = 0
        for ws, principal in tuple(self.authenticated_connections.items()):
            if principal.user_public_id != user_public_id:
                continue
            try:
                await ws.close(code=_KILL_SWITCH_CLOSE_CODE, reason=truncated_reason)
            except Exception as exc:
                logger.warning(
                    "Failed to close kill-switched WS for user {}: {}",
                    user_public_id,
                    exc,
                )
            self.disconnect(ws)
            closed += 1
        if closed:
            logger.info(
                "Kill switch closed {} WebSocket(s) for user {} (reason='{}')",
                closed,
                user_public_id,
                truncated_reason,
            )
        return closed

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
