"""WebSocket authentication module.

This module provides authentication management for WebSocket
connections including session tracking and role-based access control.
"""

import asyncio
import contextlib
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from typing import Any
from typing import cast
from uuid import uuid7

import zmq
import zmq.asyncio
from fastapi import WebSocket
from loguru import logger

from snapper.api.auth.schemas.ws_token import WsTokenPayload
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.schemas.tokens import TokenClaims
from snapper.auth.tokens import get_token_manager
from snapper.data.repository import Repository
from snapper.interface.websocket.helpers import parse_wallet_scoped_topic
from snapper.interface.websocket.schemas import WSErrorResponse
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.infrastructure.validated_socket import HWM_AUDIT
from snapper.messaging.infrastructure.validated_socket import ValidatedSubscriber
from snapper.messaging.infrastructure.validated_socket import apply_hwm
from snapper.messaging.schemas.data import DelegateOfflineData
from snapper.messaging.schemas.data import ScopeGrantedData
from snapper.messaging.schemas.data import ScopeHandedOverData
from snapper.messaging.schemas.data import ScopeRevokedData
from snapper.messaging.schemas.data import UserDeactivatedData

_KILL_SWITCH_CLOSE_CODE = 4003
_KILL_SWITCH_REASON_FALLBACK = "account_deactivated"
_ADMIN_USER_DEACTIVATED_TOPIC = "admin.user_deactivated"
_ADMIN_SCOPE_REVOKED_TOPIC = "admin.scope_revoked"
_ADMIN_SCOPE_GRANTED_TOPIC = "admin.scope_granted"
_ADMIN_SCOPE_HANDED_OVER_TOPIC = "admin.scope_handed_over"
_ADMIN_LISTEN_RECV_BACKOFF_S = 0.1
_KILL_SWITCH_REASON_MAX_BYTES = 123
_SCOPE_REVOKED_ERROR_PREFIX = "topic_outside_scope"

_BUS_DELEGATE_OFFLINE_TOPIC = "bus.delegate_offline"
"""Internal-bus topic for delegate-offline fast-path notifications.

Published by :class:`WebSocketAuthManager` after the configured grace
window elapses without a reconnect; subscribed by ``AiReviewService``
to atomically CAS-fan-out pending reviews. Backend-only — not registered
in :data:`TOPIC_REGISTRY` because no WS client subscribes to it.
"""

DEFAULT_DELEGATE_OFFLINE_GRACE_SECONDS = 5
"""Default grace window before a delegate disconnect publishes offline.

A reconnect within this window cancels the pending publish so flapping
WS connections never trigger a phantom-offline event for downstream
subscribers (e.g. mid-traffic `ai_reviews` re-fanout).
"""


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
        self._admin_listener_lock = asyncio.Lock()
        self._scope_revoked_tracker = SequenceTracker()
        self._delegate_offline_tracker = SequenceTracker()
        self.connection_manager: Any | None = None
        self.zmq_bridge: Any | None = None
        self.repository_factory: Callable[[], Repository] | None = None
        self._msg_publisher: MessagePublisher | None = None
        self._pending_offline_tasks: dict[str, asyncio.Task[None]] = {}
        self._delegate_offline_grace_seconds: int = DEFAULT_DELEGATE_OFFLINE_GRACE_SECONDS
        self._delegate_locks: dict[str, asyncio.Lock] = {}

    def set_wiring(
        self,
        connection_manager: Any | None,
        zmq_bridge: Any | None,
        repository_factory: Callable[[], Repository] | None,
    ) -> None:
        """Inject lifespan-managed dependencies for the scope-revoked handler.

        The ``admin.scope_revoked`` dispatch branch needs
        refs to the live connection manager (for subscription walks +
        ``unsubscribe_client``), the ZMQ bridge (for
        ``remove_subscription``), and a repository factory (for the
        fresh ``list_scope_grant_instrument_pairs`` read). These are
        passed through a separate setter — not constructor params — so
        the parameterless ``WebSocketAuthManager()`` contract used by
        existing tests stays untouched. Missing wiring degrades the
        handler to a logged warning instead of raising, so a
        single-instance dev server that boots without lifespan wiring
        still serves user traffic correctly.

        Types are intentionally ``Any`` because the concrete
        ``WebSocketConnectionManager`` and ``ZmqWebSocketBridge``
        classes live in ``snapper.interface.websocket`` which would
        create a circular import if referenced here.

        Args:
            connection_manager: Live WebSocketConnectionManager or None.
            zmq_bridge: Live ZMQ bridge reference or None.
            repository_factory: Callable returning the shared
                repository singleton, or None.
        """
        self.connection_manager = connection_manager
        self.zmq_bridge = zmq_bridge
        self.repository_factory = repository_factory

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

    async def verify_session_cookie(
        self,
        websocket: WebSocket,
        repository: Repository,
    ) -> tuple[AuthPrincipal, TokenClaims] | None:
        """Verify session from WebSocket auth header or cookie.

        The ``Authorization: Bearer <jwt>`` request
        header is consulted FIRST on the WebSocket upgrade; the
        ``access_token`` cookie is the fallback. MCP / CLI clients
        without cookie jars present the header; browser clients
        continue to use the cookie.
        The method is now async and
        runs through :meth:`TokenManager.verify_token_with_db` so
        the WebSocket upgrade check consults the
        ``user_active_tokens`` inventory + SCD2-active
        ``users.is_active`` join. Kill-switch propagation
            **Same-instance** — immediate. The JTI blacklist
              seeded by :meth:`TokenManager.revoke_user_sessions`
              on ``UserService.deactivate_user`` is consulted
              inside the sync ``verify_token`` layer that
              ``verify_token_with_db`` runs BEFORE the LRU, so
              a reconnect attempt with a revoked token is
              rejected even when a stale positive verdict is
              still resident in cache.
            **Cross-instance** — bounded by the 30-second LRU
              TTL until the admin-bus subscriber calls
              meth:`TokenManager.invalidate_user_cache` on
              receipt of ``admin.user_deactivated``.
        The effective ceiling drops from the 15-minute access
        token TTL to 30 s.
        The method name is preserved for call-site stability — the
        semantics are now "verify session token transport, header or
        cookie", not strictly "cookie".

        Args:
            websocket: WebSocket connection.
            repository: Active :class:`Repository` for the
                DB-backed verify path.

        Returns:
            Tuple of (AuthPrincipal, TokenClaims) if valid, None otherwise.
        """
        token = self._extract_ws_bearer_token(websocket) or websocket.cookies.get("access_token")
        if not token:
            return None
        token_data = await self.token_manager.verify_token_with_db(token, repository)
        if not token_data:
            return None
        delegate_public_id: str | None = None
        if token_data.role == UserRole.AI_DELEGATE:
            delegate_row = await repository.get_ai_delegate_by_user_public_id(
                token_data.user_public_id
            )
            if delegate_row is not None:
                delegate_public_id = delegate_row["public_id"]
        user = AuthPrincipal(
            username=token_data.username,
            role=token_data.role,
            user_public_id=token_data.user_public_id,
            operator_public_ids=token_data.operator_public_ids,
            primary_operator_public_id=token_data.primary_operator_public_id,
            active_wallet_public_id=token_data.active_wallet_public_id,
            delegate_public_id=delegate_public_id,
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
        AI-delegate hysteresis (Layer 1) lives in the
        async :meth:`on_disconnect` hook the WS dispatcher calls
        alongside this synchronous cleanup so the offline publish can
        run after the grace window without blocking close-path latency.

        Args:
            websocket: WebSocket connection to disconnect.
        """
        self.authenticated_connections.pop(websocket, None)
        state = self._connection_states.pop(websocket, None)
        if state:
            self._cancel_tasks(state)

    def set_msg_publisher(self, publisher: MessagePublisher | None) -> None:
        """Inject the bus publisher used for ``bus.delegate_offline``.

        Mirrors :meth:`ScopeGrantService.set_msg_publisher`: the FastAPI
        lifespan calls this once the shared ZMQ PUB socket is available;
        tests stub a fake publisher (or ``None`` to clear). Decoupling
        socket ownership from the singleton keeps the manager trivially
        testable without ZMQ.

        Args:
            publisher: Configured ``MessagePublisher`` or ``None`` to clear.
        """
        self._msg_publisher = publisher

    def set_delegate_offline_grace_seconds(self, grace_seconds: int) -> None:
        """Override the delayed-publish grace window (testing seam).

        Default is 5s; tests override to a sub-second value so
        the deferred-publish path can be exercised deterministically.

        Args:
            grace_seconds: New grace window in seconds. Must be positive.

        Raises:
            ValueError: ``grace_seconds`` is non-positive.
        """
        if grace_seconds <= 0:
            raise ValueError(
                f"delegate_offline_grace_seconds must be positive; got {grace_seconds}."
            )
        self._delegate_offline_grace_seconds = grace_seconds

    def _delegate_lock(self, delegate_id: str) -> asyncio.Lock:
        """Return the per-delegate :class:`asyncio.Lock` (lazily created).

        Hysteresis correctness depends on serialising the
        ``pop -> cancel -> await -> reassign`` critical section in
        :meth:`on_disconnect` AND :meth:`on_authenticate` for the same
        ``delegate_public_id``. Without serialisation, two concurrent
        same-delegate hooks can interleave at the ``await existing``
        yield and orphan the intermediate task — the orphan still
        wakes up and publishes ``bus.delegate_offline`` even though a
        reconnect already happened, breaking the
        phantom-offline-suppression contract.

        ``dict.setdefault`` is atomic under the GIL (no ``await``
        between lookup and store) so two callers racing the first
        lookup for the same delegate observe the same lock object —
        an extra ``asyncio.Lock`` may be instantiated and immediately
        GC'd, but no caller ends up with a different lock than its
        peer.
        """
        return self._delegate_locks.setdefault(delegate_id, asyncio.Lock())

    async def on_authenticate(self, websocket: WebSocket, principal: AuthPrincipal) -> None:
        """Cancel any pending offline publish for this delegate + bump last_seen_at.

        Called from the WS dispatcher's authenticate path AFTER the
        principal has been minted (via :meth:`verify_session_cookie` and
        :meth:`register_connection`). Layer 1 hysteresis:
        a reconnect within :data:`DEFAULT_DELEGATE_OFFLINE_GRACE_SECONDS`
        cancels the pending ``bus.delegate_offline`` task scheduled by
        the prior :meth:`on_disconnect` so subscribers never observe a
        phantom-offline transition for a flapping delegate.

        Non-delegate principals short-circuit; only AI_DELEGATE
        principals carry a populated ``delegate_public_id``.
        Same-delegate transitions are
        serialised through :meth:`_delegate_lock` so concurrent hooks
        cannot orphan a pending offline task.

        Args:
            websocket: WebSocket connection that just authenticated
                (passed through for symmetry with future hooks; the
                hysteresis logic itself is delegate-keyed).
            principal: Resolved principal carrying
                ``delegate_public_id`` for AI delegates.
        """
        del websocket
        delegate_id = principal.delegate_public_id
        if delegate_id is None:
            return
        async with self._delegate_lock(delegate_id):
            pending = self._pending_offline_tasks.pop(delegate_id, None)
            if pending is not None and not pending.done():
                pending.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await pending
            if self.repository_factory is None:
                logger.warning(
                    "WebSocketAuthManager.on_authenticate: skipping last_seen_at "
                    "update for delegate_public_id={} — repository_factory not wired",
                    delegate_id,
                )
                return
            repo = self.repository_factory()
            await repo.update_delegate_last_seen(delegate_id, datetime.now(UTC))

    async def on_disconnect(self, websocket: WebSocket, principal: AuthPrincipal) -> None:
        """Schedule a delayed ``bus.delegate_offline`` publish for an AI delegate.

        Called from the WS dispatcher's disconnect path BEFORE (or
        alongside) the synchronous :meth:`disconnect` cleanup. The
        hysteresis publish is deferred by
        :data:`DEFAULT_DELEGATE_OFFLINE_GRACE_SECONDS` so a flapping
        reconnect cancels it before any subscriber observes the
        phantom-offline transition. If a prior pending task already
        exists for this delegate it is cancelled first so only the
        latest disconnect timestamp ever fires. Same-delegate
        transitions are serialised through :meth:`_delegate_lock` so
        concurrent disconnects cannot orphan a pending offline task.

        Non-delegate principals short-circuit; only AI_DELEGATE
        principals carry a populated ``delegate_public_id``.

        Multi-WS-per-delegate note: the registry is delegate-keyed,
        not WS-keyed, so disconnecting one of N concurrent sessions for
        the same delegate does schedule an offline publish even when
        other sessions remain authenticated. The Layer 2 DB
        scanner is the correctness backstop — it consults
        ``ai_delegates.last_seen_at``, which the surviving session(s)
        keep refreshing via :meth:`on_authenticate`, so the
        false-positive Layer 1 publish is filtered by the scanner's
        freshness predicate or immediately superseded once the next
        heartbeat lands.

        Args:
            websocket: WebSocket connection that just dropped (passed
                through for symmetry with future per-WS hooks).
            principal: Resolved principal carrying
                ``delegate_public_id`` for AI delegates.
        """
        del websocket
        delegate_id = principal.delegate_public_id
        if delegate_id is None:
            return
        async with self._delegate_lock(delegate_id):
            existing = self._pending_offline_tasks.pop(delegate_id, None)
            if existing is not None and not existing.done():
                existing.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await existing
            self._pending_offline_tasks[delegate_id] = asyncio.create_task(
                self._delayed_offline_publish(
                    user_public_id=principal.user_public_id,
                    delegate_public_id=delegate_id,
                )
            )

    async def cancel_pending_offline_tasks(self) -> None:
        """Cancel + drain every in-flight delayed-offline task.

        Called from the FastAPI lifespan shutdown path so a closing
        process does not leave deferred-publish tasks
        sleeping against a torn-down ZMQ publisher / repository
        connection. Idempotent: clears the registry after draining so
        a second call is a no-op.
        """
        tasks = list(self._pending_offline_tasks.values())
        self._pending_offline_tasks.clear()
        for task in tasks:
            if not task.done():
                task.cancel()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def _delayed_offline_publish(
        self, *, user_public_id: str, delegate_public_id: str
    ) -> None:
        """Sleep the grace window, then publish ``bus.delegate_offline``.

        Cancellation by :meth:`on_authenticate` (or by a follow-up
        :meth:`on_disconnect` cancelling the prior task before
        scheduling the next one) lets the ``CancelledError`` propagate
        as the task's terminal state — callers wrap the await in
        ``contextlib.suppress(asyncio.CancelledError)`` so the
        cancellation is observed without the asyncio task graph seeing
        it as an unhandled exception. Publish failures inside
        :meth:`_publish_delegate_offline` are caught and logged so a
        transient broker hiccup cannot leak either.

        Args:
            user_public_id: Owner of the AI_DELEGATE user row.
            delegate_public_id: ``ai_delegates.public_id`` for the
                disconnected WS. Caller (``on_disconnect``) MUST
                supply a non-None value; this method is private and
                no public path exposes a ``None`` route here.
        """
        await asyncio.sleep(self._delegate_offline_grace_seconds)
        await self._publish_delegate_offline(
            user_public_id=user_public_id,
            delegate_public_id=delegate_public_id,
            last_seen_at=datetime.now(UTC),
        )

    async def _publish_delegate_offline(
        self,
        *,
        user_public_id: str,
        delegate_public_id: str,
        last_seen_at: datetime,
    ) -> None:
        """Emit one ``bus.delegate_offline`` message for the given delegate.

        Best-effort on the publisher: a missing publisher logs a
        warning instead of raising so a singleton spun up before
        lifespan attached one still tolerates the call (the local
        in-process subscriber, if any, would only matter for
        cross-instance fanout, and the ``ai_delegates.last_seen_at``
        column is the source of truth anyway via the Layer 2
        scanner).
        """
        if self._msg_publisher is None:
            logger.warning(
                "bus.delegate_offline NOT broadcast for delegate_public_id={}: "
                "WebSocketAuthManager publisher unavailable",
                delegate_public_id,
            )
            return
        topic = _BUS_DELEGATE_OFFLINE_TOPIC
        payload = DelegateOfflineData(
            public_id=str(uuid7()),
            timestamp=last_seen_at,
            session_id=self._delegate_offline_tracker.session_id,
            sequence_id=self._delegate_offline_tracker.next_sequence(topic),
            user_public_id=user_public_id,
            delegate_public_id=delegate_public_id,
            last_seen_at=last_seen_at,
        )
        try:
            await self._msg_publisher.send(topic, payload)
        except Exception as exc:
            logger.exception(
                "Failed to broadcast bus.delegate_offline for delegate_public_id={}: {}",
                delegate_public_id,
                exc,
            )

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

        Subscribes to both ``admin.user_deactivated`` (kill-switch
        fanout from ``UserService.deactivate_user`` — sole publisher)
        and ``admin.scope_revoked`` (mid-session wallet-scope
        revalidation from ``ScopeGrantService.revoke_grant`` — sole
        publisher). The deactivation branch closes affected WebSockets
        with code 4003; the scope-revoked branch narrows affected
        AI_DELEGATE subscriptions in place without closing the WS (see
        ``_handle_scope_revoked``).
        Idempotent + restart-safe via `_admin_listener_lock`: a second
        call while a healthy listener is already running is a no-op
        a second call after the previous task finished early (e.g.
        the loop unwound on an unexpected exception) reaps the dead
        task and re-allocates so the admin-bus path stays live
        across single-listener failures.

        Args:
            zmq_broker_xpub: Address of the broker's XPUB endpoint.
                Empty string skips the listener entirely (test mode).
        """
        async with self._admin_listener_lock:
            if self._admin_listen_task is not None and not self._admin_listen_task.done():
                return
            if self._admin_listen_task is not None:
                await self._reap_admin_listener_unlocked()
            if not zmq_broker_xpub:
                logger.info("WebSocketAuthManager: empty broker XPUB, skipping admin listener")
                return
            self._admin_zmq_context = zmq.asyncio.Context()
            raw_sub_socket = self._admin_zmq_context.socket(zmq.SUB)
            apply_hwm(raw_sub_socket, rcvhwm=HWM_AUDIT)
            raw_sub_socket.connect(zmq_broker_xpub)
            self._admin_subscriber = ValidatedSubscriber(raw_sub_socket)
            self._admin_subscriber.subscribe(_ADMIN_USER_DEACTIVATED_TOPIC)
            self._admin_subscriber.subscribe(_ADMIN_SCOPE_REVOKED_TOPIC)
            self._admin_subscriber.subscribe(_ADMIN_SCOPE_GRANTED_TOPIC)
            self._admin_subscriber.subscribe(_ADMIN_SCOPE_HANDED_OVER_TOPIC)
            self._admin_running = True
            self._admin_listen_task = asyncio.create_task(self._admin_listen_loop())
            logger.info(
                "WebSocketAuthManager: admin-bus listener subscribed to {} + {} + {} + {} on {}",
                _ADMIN_USER_DEACTIVATED_TOPIC,
                _ADMIN_SCOPE_REVOKED_TOPIC,
                _ADMIN_SCOPE_GRANTED_TOPIC,
                _ADMIN_SCOPE_HANDED_OVER_TOPIC,
                zmq_broker_xpub,
            )

    async def stop_admin_listener(self) -> None:
        """Cancel the dispatch task, close the subscriber, terminate the context.

        Serialised against `start_admin_listener` via
        `_admin_listener_lock` so an overlapping start cannot allocate
        a new socket while we are tearing the old one down. Resource
        refs are captured into locals BEFORE attributes are nulled so
        a concurrent operation that races into the lock cannot
        observe stale references after the close.
        Order mirrors `_shutdown_user_service_publisher`
        flip the running flag first so the loop exits on the next
        iteration, cancel the task, then dispose socket + context with
        suppressed exceptions so a transient broker issue cannot mask
        a clean shutdown. Idempotent.
        """
        async with self._admin_listener_lock:
            await self._reap_admin_listener_unlocked()

    async def _reap_admin_listener_unlocked(self) -> None:
        """Tear down listener resources. Caller MUST hold `_admin_listener_lock`.

        Captures every resource reference into locals BEFORE clearing
        the attributes so a follow-up `start_admin_listener` (which
        runs after we release the lock) sees a fully-clean slate and
        cannot interfere with the close + term calls below.
        """
        self._admin_running = False
        task = self._admin_listen_task
        subscriber = self._admin_subscriber
        context = self._admin_zmq_context
        self._admin_listen_task = None
        self._admin_subscriber = None
        self._admin_zmq_context = None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        if subscriber is not None:
            with contextlib.suppress(Exception):
                subscriber.close()
        if context is not None:
            with contextlib.suppress(Exception):
                context.term()

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
        non-cancellation error OR when the decoded bytes are not
        valid UTF-8 — both cases let the caller simply ``continue``
        instead of letting a malformed frame unwind the listener
        loop. Decode is INSIDE the ``try`` so a `UnicodeDecodeError`
        cannot escape the helper.
        """
        try:
            topic_bytes, payload_bytes = await subscriber.recv_multipart()
            topic = topic_bytes.decode() if isinstance(topic_bytes, bytes) else str(topic_bytes)
            payload = (
                payload_bytes.decode() if isinstance(payload_bytes, bytes) else str(payload_bytes)
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("WebSocketAuthManager admin listener recv failed: {}", exc)
            await asyncio.sleep(_ADMIN_LISTEN_RECV_BACKOFF_S)
            return None
        return topic, payload

    async def _admin_dispatch_frame(self, topic: str, payload: str) -> None:
        """Route one decoded admin-bus frame to its typed handler.

        Handler exceptions other than ``CancelledError`` are logged
        + swallowed so one bad frame cannot stop the listener.
        """
        try:
            if topic == _ADMIN_USER_DEACTIVATED_TOPIC:
                await self._handle_user_deactivated(UserDeactivatedData.from_json(payload))
            elif topic == _ADMIN_SCOPE_REVOKED_TOPIC:
                await self._handle_scope_revoked(ScopeRevokedData.from_json(payload))
            elif topic == _ADMIN_SCOPE_GRANTED_TOPIC:
                await self._handle_scope_granted(ScopeGrantedData.from_json(payload))
            elif topic == _ADMIN_SCOPE_HANDED_OVER_TOPIC:
                await self._handle_scope_handed_over(ScopeHandedOverData.from_json(payload))
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

    async def _handle_scope_revoked(self, data: ScopeRevokedData) -> None:
        """Mid-session revalidation for affected AI_DELEGATE connections.

        Unlike ``_handle_user_deactivated`` (which closes the entire
        WS — user is gone), this handler only narrows subscriptions:
        for each AI_DELEGATE connection whose
        ``operator_public_ids`` contains the revoked grant's operator,
        recompute the delegate's allowed ``(exchange, symbol)`` pair set
        against the post-revocation DB snapshot, then walk the client's
        current subscriptions and unsubscribe + error-frame any
        wallet-scoped topics no longer covered. The WS stays open so
        unaffected subscriptions (market, system, backtest, accruals,
        paper signals) keep flowing.

        The structured error prefix
        ``topic_outside_scope:`` rides on ``WSErrorResponse.message``
        for MCP / CLI / log consumption; browser UIs that don't parse
        the prefix see a generic toast + a silently-dropped
        subscription (MVP acceptable).

        Missing wiring (``set_wiring`` not called — single-instance dev
        boot before lifespan attached) is logged as a warning and the
        handler no-ops; production lifespan always wires.

        Args:
            data: Decoded ``admin.scope_revoked`` payload.
        """
        if (
            self.connection_manager is None
            or self.zmq_bridge is None
            or self.repository_factory is None
        ):
            logger.warning(
                "WebSocketAuthManager: admin.scope_revoked received without wiring "
                "(grant_public_id={}, operator_public_id={}); skipping fanout",
                data.grant_public_id,
                data.operator_public_id,
            )
            return

        repository = self.repository_factory()
        now = datetime.now(UTC)
        snapshot = tuple(self.authenticated_connections.items())
        for ws, principal in snapshot:
            if principal.role != UserRole.AI_DELEGATE:
                continue
            if data.operator_public_id not in principal.operator_public_ids:
                continue
            await self._revalidate_and_unsubscribe(ws, principal, repository, data, now)

    async def _revalidate_and_unsubscribe(
        self,
        ws: WebSocket,
        principal: AuthPrincipal,
        repository: Repository,
        event: ScopeRevokedData,
        now: datetime,
    ) -> None:
        """Walk this WS's subscriptions and drop any now-out-of-scope wallet topics.

        Thin wrapper around :meth:`_drop_unscoped_subscriptions` that
        preserves the original ``ScopeRevokedData``-typed signature so
        existing tests stay valid. Delegates the actual revalidation
        loop to the shared helper used by all three scope-event
        handlers (revoked / granted / handed_over).
        """
        await self._drop_unscoped_subscriptions(
            ws=ws,
            principal=principal,
            repository=repository,
            event_identifier=event.grant_public_id,
            log_prefix="scope_revoked",
            now=now,
        )

    async def _handle_scope_granted(self, data: ScopeGrantedData) -> None:
        """Defensive WS subscription revalidation after an admin scope insert.

        A new grant can only WIDEN the allowed-pair set, so no
        subscriptions should be dropped. The revalidation runs anyway
        as a consistency safety-net: if a prior event was lost or
        delivered out of order, this handler reconciles the WS state
        against the post-insert DB snapshot. The primary consumer of
        ``admin.scope_granted`` is :class:`MarketPersistPolicy`; this
        WS-side handler exists to keep the admin-bus subscription
        invariant symmetric across the three event types.

        Args:
            data: Decoded ``admin.scope_granted`` payload.
        """
        await self._revalidate_delegates_for_operators(
            affected_operators={data.operator_public_id},
            event_identifier=data.grant_public_id,
            log_prefix="scope_granted",
        )

    async def _handle_scope_handed_over(self, data: ScopeHandedOverData) -> None:
        """WS subscription revalidation after an admin scope handover.

        A handover is logically revoke-from-source + grant-to-dest in
        one transaction, so both operators' delegates need
        revalidation: source-side may need to drop the handed-over
        pair; dest-side widens (no drops in practice but revalidate
        defensively, matching :meth:`_handle_scope_granted`).

        Args:
            data: Decoded ``admin.scope_handed_over`` payload.
        """
        await self._revalidate_delegates_for_operators(
            affected_operators={
                data.from_operator_public_id,
                data.to_operator_public_id,
            },
            event_identifier=data.grant_public_id,
            log_prefix="scope_handed_over",
        )

    async def _revalidate_delegates_for_operators(
        self,
        *,
        affected_operators: set[str],
        event_identifier: str,
        log_prefix: str,
    ) -> None:
        """Walk AI_DELEGATE connections whose operator set overlaps ``affected_operators``.

        Mirrors :meth:`_handle_scope_revoked` but accepts a set of
        operators so granted (single operator) and handed_over (two
        operators) events can share the same fanout machinery. Missing
        wiring is logged and the handler no-ops (matches the
        revoked-path early-return contract).

        Args:
            affected_operators: Operator public IDs whose delegates
                must revalidate. Empty set short-circuits (defensive;
                empty would be a bug in the publisher).
            event_identifier: Grant public ID — emitted in the
                fanout summary log for ops correlation.
            log_prefix: Event-type tag (``scope_granted`` /
                ``scope_handed_over``) — controls the log line shape.
        """
        if (
            self.connection_manager is None
            or self.zmq_bridge is None
            or self.repository_factory is None
        ):
            logger.warning(
                "WebSocketAuthManager: admin.{} received without wiring "
                "(grant_public_id={}, affected_operators={}); skipping fanout",
                log_prefix,
                event_identifier,
                sorted(affected_operators),
            )
            return
        repository = self.repository_factory()
        now = datetime.now(UTC)
        snapshot = tuple(self.authenticated_connections.items())
        for ws, principal in snapshot:
            if principal.role != UserRole.AI_DELEGATE:
                continue
            if not any(op in affected_operators for op in principal.operator_public_ids):
                continue
            await self._drop_unscoped_subscriptions(
                ws=ws,
                principal=principal,
                repository=repository,
                event_identifier=event_identifier,
                log_prefix=log_prefix,
                now=now,
            )

    async def _drop_unscoped_subscriptions(
        self,
        *,
        ws: WebSocket,
        principal: AuthPrincipal,
        repository: Repository,
        event_identifier: str,
        log_prefix: str,
        now: datetime,
    ) -> None:
        """Recompute allowed pairs and unsubscribe any topics no longer covered.

        Generic version of the original revoke-only revalidation loop:
        re-queries allowed pairs once per connection, then a per-topic
        membership check decides which subscriptions survive. Errors
        during ``unsubscribe_client`` or bridge removal are logged +
        swallowed so a single bad subscription cannot block the rest
        of the fanout. The ``event_identifier`` + ``log_prefix`` ride
        the summary log line so all three event types share the same
        ops query surface.
        """
        connection_manager = cast(Any, self.connection_manager)
        zmq_bridge = cast(Any, self.zmq_bridge)
        allowed_pairs = await repository.list_scope_grant_instrument_pairs(
            principal.operator_public_ids, now
        )
        current_topics = tuple(connection_manager.get_client_subscriptions(ws))
        affected: list[str] = []
        for topic in current_topics:
            pair = parse_wallet_scoped_topic(topic)
            if pair is None or pair in allowed_pairs:
                continue
            affected.append(topic)
            error_message = (
                f"{_SCOPE_REVOKED_ERROR_PREFIX}: {topic} — no active grant covers this pair"
            )
            frame = WSErrorResponse(
                message=error_message,
                session_id=self._scope_revoked_tracker.session_id,
                sequence_id=self._scope_revoked_tracker.next_sequence(_ADMIN_SCOPE_REVOKED_TOPIC),
                public_id=str(uuid7()),
                timestamp=now,
            )
            with contextlib.suppress(Exception):
                await ws.send_text(frame.model_dump_json())
            with contextlib.suppress(Exception):
                connection_manager.unsubscribe_client(ws, topic)
            with contextlib.suppress(Exception):
                await zmq_bridge.remove_subscription(ws, [topic])
        logger.info(
            "{} fanout: ws_peer={} affected_topics={} user_public_id={} grant_public_id={}",
            log_prefix,
            getattr(ws, "client", None),
            len(affected),
            principal.user_public_id,
            event_identifier,
        )

    async def close_user_connections(self, user_public_id: str, reason: str) -> int:
        """Close every authenticated WS matching `user_public_id` (code 4003).

        Per-connection sequence
        1. `self.disconnect(ws)` first — synchronously cancels the
           connection's `warn_task` + `hard_task` timers so a pending
           expiration handler cannot wake during the `await
           ws.close()` yield and try to write/close the socket
           concurrently with the kill switch (
             flagged the original
           close-then-disconnect order as a race).
        2. `await ws.close(code=4003, reason=…)` — the kill-switch
           close itself.
        Iterates a snapshot so the `disconnect` side-effect (which
        mutates `authenticated_connections`) does not invalidate the
        iterator. A `ws.close()` failure (already-closed socket
        broken pipe, etc.) is logged + swallowed so one stuck
        connection cannot block the rest from being torn down. Match
        is by `user_public_id` (stable UUID7); username is mutable so
        is intentionally NOT used here.

        Args:
            user_public_id: UUID7 of the user whose sessions are
                being terminated.
            reason: Free-text reason carried in the WS close frame.
                Truncated to 123 UTF-8 bytes because RFC 6455
                limits the close-reason field to 123 bytes (the
                encoded length, not the codepoint count); a Unicode
                reason is truncated on a UTF-8 boundary via
                ``errors="ignore"`` so a multi-byte character split
                by truncation is dropped rather than corrupting the
                control frame.

        Returns:
            Number of connections that were closed.
        """
        truncated_reason = reason.encode("utf-8")[:_KILL_SWITCH_REASON_MAX_BYTES].decode(
            "utf-8", errors="ignore"
        )
        closed = 0
        for ws, principal in tuple(self.authenticated_connections.items()):
            if principal.user_public_id != user_public_id:
                continue
            self.disconnect(ws)
            try:
                await ws.close(code=_KILL_SWITCH_CLOSE_CODE, reason=truncated_reason)
            except Exception as exc:
                logger.warning(
                    "Failed to close kill-switched WS for user {}: {}",
                    user_public_id,
                    exc,
                )
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
