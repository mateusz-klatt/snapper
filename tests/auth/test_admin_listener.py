"""Tests for the Day 3c admin-bus listener on `WebSocketAuthManager`.

Covers the kill-switch fanout half of plan §3.6.1: every authenticated
WebSocket whose principal matches the deactivated user is closed with
code 4003 on receipt of `admin.user_deactivated`. Day 3a planted the
publisher's local-blacklist + DB inventory; Day 3b made
`UserService.deactivate_user` the SOLE publisher; Day 3c connects the
WebSocket layer to that publisher.

Subscription wiring for `admin.scope_revoked` is deferred to Day 3f
(re-validation needs the §3.8 wallet-scope algorithm).

The ZMQ socket layer is mocked because (a) starting a real broker
in unit tests would dwarf the test's value and (b) the recv +
dispatch halves are factored into helpers (`_admin_recv_one_frame`
+ `_admin_dispatch_frame`) that are individually testable without a
running listener.
"""

import asyncio
from datetime import UTC
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.websocket_auth import WebSocketAuthManager
from snapper.messaging.schemas.data import UserDeactivatedData


def _make_manager() -> WebSocketAuthManager:
    """Return a freshly-initialised singleton instance."""
    WebSocketAuthManager.clear_instance()
    return WebSocketAuthManager()


def _make_principal(user_public_id: str, *, username: str | None = None) -> AuthPrincipal:
    """Build an authenticated AuthPrincipal pinned to ``user_public_id``."""
    return AuthPrincipal(
        username=username or f"user-{user_public_id}",
        role=UserRole.VIEWER,
        is_active=True,
        user_public_id=user_public_id,
    )


def _make_user_deactivated_payload(
    user_public_id: str,
    reason: str | None = None,
) -> UserDeactivatedData:
    """Build a canonical `UserDeactivatedData` event payload."""
    now = datetime.now(UTC)
    return UserDeactivatedData(
        public_id=f"evt-{user_public_id}",
        timestamp=now,
        session_id="t-sid",
        sequence_id=1,
        user_public_id=user_public_id,
        deactivated_at=now,
        reason=reason,
    )


class TestCloseUserConnections:
    """`close_user_connections` is the kill-switch's fanout primitive."""

    @pytest.mark.asyncio
    async def test_closes_only_matching_connections_and_returns_count(self) -> None:
        """Iterates by `user_public_id`, leaving other users untouched.

        Given: three authenticated WebSockets — two for user-A, one
            for user-B,
        When: close_user_connections fires for user-A,
        Then: both user-A sockets are closed with code 4003; user-B
            stays open; the return value is the count of closed
            sockets (2).
        """
        manager = _make_manager()
        ws_a1 = MagicMock()
        ws_a1.close = AsyncMock()
        ws_a2 = MagicMock()
        ws_a2.close = AsyncMock()
        ws_b = MagicMock()
        ws_b.close = AsyncMock()
        manager.authenticated_connections[ws_a1] = _make_principal("user-a")
        manager.authenticated_connections[ws_a2] = _make_principal("user-a", username="alt")
        manager.authenticated_connections[ws_b] = _make_principal("user-b")
        closed = await manager.close_user_connections(
            user_public_id="user-a", reason="policy_violation"
        )
        assert closed == 2
        ws_a1.close.assert_awaited_once_with(code=4003, reason="policy_violation")
        ws_a2.close.assert_awaited_once_with(code=4003, reason="policy_violation")
        ws_b.close.assert_not_awaited()
        assert ws_a1 not in manager.authenticated_connections
        assert ws_a2 not in manager.authenticated_connections
        assert ws_b in manager.authenticated_connections

    @pytest.mark.asyncio
    async def test_no_match_returns_zero(self) -> None:
        """Ghost user → count=0; nothing is closed; existing sockets stay."""
        manager = _make_manager()
        ws = MagicMock()
        ws.close = AsyncMock()
        manager.authenticated_connections[ws] = _make_principal("user-real")
        closed = await manager.close_user_connections(user_public_id="ghost", reason="audit")
        assert closed == 0
        ws.close.assert_not_awaited()
        assert ws in manager.authenticated_connections

    @pytest.mark.asyncio
    async def test_close_failure_logged_but_does_not_block_disconnect(self) -> None:
        """A `ws.close()` failure is swallowed and the connection is still removed.

        Defends against the case where the socket is already in a
        broken state (peer-RST, half-closed). The kill switch must
        still drop the entry from `authenticated_connections` so a
        stale principal cannot survive the deactivation.
        """
        manager = _make_manager()
        ws = MagicMock()
        ws.close = AsyncMock(side_effect=RuntimeError("socket closed"))
        manager.authenticated_connections[ws] = _make_principal("user-broken")
        closed = await manager.close_user_connections(
            user_public_id="user-broken", reason="leaked_key"
        )
        assert closed == 1
        assert ws not in manager.authenticated_connections

    @pytest.mark.asyncio
    async def test_reason_truncated_to_120_chars(self) -> None:
        """Close-frame `reason` field is limited per RFC 6455 (123-byte cap)."""
        manager = _make_manager()
        ws = MagicMock()
        ws.close = AsyncMock()
        manager.authenticated_connections[ws] = _make_principal("user-t")
        long_reason = "x" * 200
        await manager.close_user_connections(user_public_id="user-t", reason=long_reason)
        await_args = ws.close.await_args
        assert await_args is not None
        called_kwargs = await_args.kwargs
        assert called_kwargs["code"] == 4003
        assert len(called_kwargs["reason"]) == 120


class TestHandleUserDeactivated:
    """The dispatch handler called from the admin listen loop."""

    @pytest.mark.asyncio
    async def test_dispatches_to_close_with_reason_from_payload(self) -> None:
        """Payload `reason` reaches the close-frame verbatim."""
        manager = _make_manager()
        ws = MagicMock()
        ws.close = AsyncMock()
        manager.authenticated_connections[ws] = _make_principal("user-z")
        await manager._handle_user_deactivated(
            _make_user_deactivated_payload("user-z", reason="legal_hold")
        )
        ws.close.assert_awaited_once_with(code=4003, reason="legal_hold")

    @pytest.mark.asyncio
    async def test_falls_back_to_account_deactivated_when_reason_none(self) -> None:
        """`reason=None` from publisher → close uses `account_deactivated` fallback."""
        manager = _make_manager()
        ws = MagicMock()
        ws.close = AsyncMock()
        manager.authenticated_connections[ws] = _make_principal("user-n")
        await manager._handle_user_deactivated(
            _make_user_deactivated_payload("user-n", reason=None)
        )
        ws.close.assert_awaited_once_with(code=4003, reason="account_deactivated")


class TestAdminDispatchFrame:
    """Per-frame dispatch must route to typed handlers and swallow errors."""

    @pytest.mark.asyncio
    async def test_user_deactivated_topic_invokes_handler(self) -> None:
        """`admin.user_deactivated` payload drives `_handle_user_deactivated`."""
        manager = _make_manager()
        payload = _make_user_deactivated_payload("user-1", reason="r").to_json()
        with patch.object(manager, "_handle_user_deactivated", new=AsyncMock()) as mock_handler:
            await manager._admin_dispatch_frame("admin.user_deactivated", payload)
        mock_handler.assert_awaited_once()
        await_args = mock_handler.await_args
        assert await_args is not None
        called_arg = await_args.args[0]
        assert isinstance(called_arg, UserDeactivatedData)
        assert called_arg.user_public_id == "user-1"
        assert called_arg.reason == "r"

    @pytest.mark.asyncio
    async def test_unknown_topic_is_ignored(self) -> None:
        """Topics outside the subscribed set are silently dropped.

        Defensive — a misrouted frame from the broker (e.g.
        admin.scope_revoked once Day 3f wires it but before the
        listener restarts) must not raise out of the dispatch loop.
        """
        manager = _make_manager()
        with patch.object(manager, "_handle_user_deactivated", new=AsyncMock()) as mock_handler:
            await manager._admin_dispatch_frame("admin.something_else", "{}")
        mock_handler.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_handler_exception_is_caught(self) -> None:
        """A handler raising must NOT escape — listener stays up.

        Confirmed by the dispatch returning normally even though the
        handler raised.
        """
        manager = _make_manager()
        payload = _make_user_deactivated_payload("user-x").to_json()
        with patch.object(
            manager,
            "_handle_user_deactivated",
            new=AsyncMock(side_effect=RuntimeError("boom")),
        ):
            await manager._admin_dispatch_frame("admin.user_deactivated", payload)

    @pytest.mark.asyncio
    async def test_dispatch_propagates_cancelled_error(self) -> None:
        """`asyncio.CancelledError` from the handler MUST escape uncaught.

        Cancellation is the only way `stop_admin_listener` can unwind
        the loop cleanly during shutdown — if dispatch swallowed it,
        the listener task would survive cancellation and the lifespan
        finally block would deadlock waiting for it.
        """
        manager = _make_manager()
        payload = _make_user_deactivated_payload("user-x").to_json()
        with patch.object(
            manager,
            "_handle_user_deactivated",
            new=AsyncMock(side_effect=asyncio.CancelledError()),
        ), pytest.raises(asyncio.CancelledError):
            await manager._admin_dispatch_frame("admin.user_deactivated", payload)


class TestAdminRecvOneFrame:
    """Recv helper must decode bytes → str and bounce on recv errors."""

    @pytest.mark.asyncio
    async def test_decodes_bytes_to_str(self) -> None:
        """ZMQ frames arrive as bytes; helper returns str pair."""
        manager = _make_manager()
        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(
            return_value=(b"admin.user_deactivated", b'{"hello":"world"}')
        )
        topic, payload = await manager._admin_recv_one_frame(subscriber)
        assert topic == "admin.user_deactivated"
        assert payload == '{"hello":"world"}'

    @pytest.mark.asyncio
    async def test_recv_failure_returns_none_after_backoff(self) -> None:
        """Non-cancellation recv error → ``None`` (after a sleep) so the loop continues."""
        manager = _make_manager()
        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(side_effect=RuntimeError("socket dead"))
        with patch("snapper.auth.websocket_auth.asyncio.sleep", new=AsyncMock()) as sleep_mock:
            result = await manager._admin_recv_one_frame(subscriber)
        assert result is None
        sleep_mock.assert_awaited_once()


class TestAdminListenerLifecycle:
    """`start_admin_listener` + `stop_admin_listener` are idempotent + lifecycle-safe."""

    @pytest.mark.asyncio
    async def test_start_with_empty_xpub_skips_listener_setup(self) -> None:
        """Empty broker XPUB → no socket, no task (test-mode default)."""
        manager = _make_manager()
        await manager.start_admin_listener("")
        assert manager._admin_listen_task is None
        assert manager._admin_subscriber is None
        assert manager._admin_zmq_context is None

    @pytest.mark.asyncio
    async def test_start_then_stop_creates_and_disposes_resources(self) -> None:
        """Happy path: start opens socket+task, stop closes everything.

        The ZMQ Context + Socket are stubbed so the test can run
        without a broker. Verifies the contract of resource creation
        + disposal without depending on real network IO.
        """
        manager = _make_manager()
        mock_socket = MagicMock()
        mock_context = MagicMock()
        mock_context.socket.return_value = mock_socket
        mock_context_cls = MagicMock(return_value=mock_context)
        with patch("snapper.auth.websocket_auth.zmq.asyncio.Context", mock_context_cls):
            await manager.start_admin_listener("tcp://broker:5555")
        assert manager._admin_listen_task is not None
        assert manager._admin_subscriber is not None
        assert manager._admin_zmq_context is mock_context
        mock_socket.connect.assert_called_once_with("tcp://broker:5555")
        await manager.stop_admin_listener()
        assert manager._admin_listen_task is None
        assert manager._admin_subscriber is None
        assert manager._admin_zmq_context is None
        mock_context.term.assert_called_once()

    @pytest.mark.asyncio
    async def test_start_is_idempotent_when_already_running(self) -> None:
        """Calling start twice does not double-allocate the listener."""
        manager = _make_manager()
        mock_socket = MagicMock()
        mock_context = MagicMock()
        mock_context.socket.return_value = mock_socket
        mock_context_cls = MagicMock(return_value=mock_context)
        with patch("snapper.auth.websocket_auth.zmq.asyncio.Context", mock_context_cls):
            await manager.start_admin_listener("tcp://broker:5555")
            first_task = manager._admin_listen_task
            await manager.start_admin_listener("tcp://broker:5555")
        assert manager._admin_listen_task is first_task
        assert mock_context_cls.call_count == 1
        await manager.stop_admin_listener()

    @pytest.mark.asyncio
    async def test_stop_when_never_started_is_noop(self) -> None:
        """Calling stop on a fresh manager must not raise."""
        manager = _make_manager()
        await manager.stop_admin_listener()
        assert manager._admin_listen_task is None

    @pytest.mark.asyncio
    async def test_stop_swallows_close_errors(self) -> None:
        """Subscriber/context close errors during shutdown are swallowed.

        Mirrors the `_shutdown_user_service_publisher` resilience
        contract (Day 3b): a transient broker issue at shutdown must
        not block the rest of the lifespan teardown sequence.
        """
        manager = _make_manager()
        mock_socket = MagicMock()
        mock_context = MagicMock()
        mock_context.socket.return_value = mock_socket
        mock_context.term.side_effect = RuntimeError("ctx term failed")
        mock_context_cls = MagicMock(return_value=mock_context)
        with patch("snapper.auth.websocket_auth.zmq.asyncio.Context", mock_context_cls):
            await manager.start_admin_listener("tcp://broker:5555")
            subscriber = manager._admin_subscriber
            assert subscriber is not None
            subscriber.close = MagicMock(side_effect=RuntimeError("close failed"))
            await manager.stop_admin_listener()
        assert manager._admin_listen_task is None
        assert manager._admin_subscriber is None
        assert manager._admin_zmq_context is None


class TestAdminListenLoop:
    """End-to-end: a frame fed through `recv_multipart` reaches the handler."""

    @pytest.mark.asyncio
    async def test_loop_dispatches_frame_then_exits_on_running_false(self) -> None:
        """One frame in → one handler call → loop exits when `_admin_running` flips.

        Drives the loop directly with a recording subscriber that
        emits one frame, then signals exit by flipping
        `_admin_running` from inside the recv coroutine.
        """
        manager = _make_manager()
        ws = MagicMock()
        ws.close = AsyncMock()
        manager.authenticated_connections[ws] = _make_principal("user-loop")
        payload = _make_user_deactivated_payload("user-loop", reason="loop-test").to_json()

        subscriber = MagicMock()
        delivered: dict[str, bool] = {"emitted": False}

        async def _recv() -> tuple[bytes, bytes]:
            await asyncio.sleep(0)
            if delivered["emitted"]:
                manager._admin_running = False
                return (b"admin.user_deactivated", b"{}")
            delivered["emitted"] = True
            return (b"admin.user_deactivated", payload.encode())

        subscriber.recv_multipart = _recv
        manager._admin_subscriber = subscriber
        manager._admin_running = True
        await manager._admin_listen_loop()
        ws.close.assert_awaited_once_with(code=4003, reason="loop-test")
        assert ws not in manager.authenticated_connections

    @pytest.mark.asyncio
    async def test_loop_returns_immediately_when_subscriber_none(self) -> None:
        """Loop is a no-op when no subscriber is attached (test-mode start)."""
        manager = _make_manager()
        manager._admin_subscriber = None
        manager._admin_running = True
        await manager._admin_listen_loop()

    @pytest.mark.asyncio
    async def test_loop_continues_after_recv_failure_then_exits(self) -> None:
        """A recv error returns ``None`` from the helper → loop continues.

        Drives the loop with a subscriber whose first recv raises and
        whose second recv flips `_admin_running` off. Verifies the
        ``continue`` branch (line 401 in `_admin_listen_loop`) is
        exercised: a single broken frame does NOT take the listener
        down, but the second iteration sees the running flag flipped
        and exits cleanly.
        """
        manager = _make_manager()
        subscriber = MagicMock()
        call_state: dict[str, int] = {"calls": 0}

        async def _recv() -> tuple[bytes, bytes]:
            await asyncio.sleep(0)
            call_state["calls"] += 1
            if call_state["calls"] == 1:
                raise RuntimeError("transient socket error")
            manager._admin_running = False
            return (b"admin.user_deactivated", b"{}")

        subscriber.recv_multipart = _recv
        manager._admin_subscriber = subscriber
        manager._admin_running = True
        with patch("snapper.auth.websocket_auth.asyncio.sleep", new=AsyncMock()):
            await manager._admin_listen_loop()
        assert call_state["calls"] == 2


class TestSnapshotIterationSafety:
    """`close_user_connections` must iterate a snapshot to survive `disconnect`."""

    @pytest.mark.asyncio
    async def test_disconnect_during_iteration_does_not_raise(self) -> None:
        """The dict mutation in `disconnect` must not invalidate the loop.

        Without `tuple(...)` snapshot the loop would raise
        ``RuntimeError: dictionary changed size during iteration``.
        """
        manager = _make_manager()
        targets: list[Any] = []
        for _ in range(5):
            ws = MagicMock()
            ws.close = AsyncMock()
            manager.authenticated_connections[ws] = _make_principal("user-many")
            targets.append(ws)
        closed = await manager.close_user_connections(user_public_id="user-many", reason="bulk")
        assert closed == 5
        assert manager.authenticated_connections == {}
