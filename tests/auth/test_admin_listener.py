"""Tests for the admin-bus listener on `WebSocketAuthManager`.

Covers the kill-switch fanout: every authenticated WebSocket whose
principal matches the deactivated user is closed with code 4003 on
receipt of `admin.user_deactivated`. The publisher's local-blacklist
+ DB inventory is planted; `UserService.deactivate_user` is the SOLE
publisher; the WebSocket layer is connected to that publisher.

The ZMQ socket layer is mocked because (a) starting a real broker
in unit tests would dwarf the test's value and (b) the recv +
dispatch halves are factored into helpers (`_admin_recv_one_frame`
+ `_admin_dispatch_frame`) that are individually testable without a
running listener.
"""

import asyncio
from datetime import UTC
from datetime import datetime
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.tokens import PERMISSION_SCOPE_VERSION
from snapper.auth.websocket_auth import ConnectionState
from snapper.auth.websocket_auth import WebSocketAuthManager
from snapper.data.repository import Repository
from snapper.interface.websocket.connection_manager import WebSocketConnectionManager
from snapper.interface.websocket.dispatcher import _build_dispatch_table
from snapper.interface.websocket.dispatcher import _dispatch_single_message
from snapper.interface.websocket.schemas import WSSubscribeRequest
from snapper.messaging.schemas.admin import MembershipRevokedData
from snapper.messaging.schemas.data import UserDeactivatedData
from tests.auth.deactivation_fallback_helpers import FailingInactiveUserLookupRepo
from tests.auth.deactivation_fallback_helpers import FailingMembershipLookupRepo
from tests.auth.deactivation_fallback_helpers import FailingTokenInventoryLookupRepo
from tests.auth.deactivation_fallback_helpers import InactiveUserLookupRepo
from tests.auth.deactivation_fallback_helpers import MembershipLookupRepo
from tests.auth.deactivation_fallback_helpers import MembershipTokenLookupRepo
from tests.auth.deactivation_fallback_helpers import assert_fallback_loop_propagates_scan_cancelled
from tests.auth.deactivation_fallback_helpers import assert_fallback_loop_propagates_sleep_cancelled


def _make_manager() -> WebSocketAuthManager:
    """Return a freshly-initialised singleton instance."""
    WebSocketAuthManager.clear_instance()
    return WebSocketAuthManager()


def _make_principal(
    user_public_id: str,
    *,
    role: UserRole = UserRole.VIEWER,
    operator_public_ids: list[str] | None = None,
    permissions: list[str] | None = None,
    permission_scope_version: int | None = None,
) -> AuthPrincipal:
    """Build an authenticated AuthPrincipal pinned to ``user_public_id``."""
    return AuthPrincipal(
        username=f"user-{user_public_id}",
        role=role,
        is_active=True,
        user_public_id=user_public_id,
        operator_public_ids=operator_public_ids or [],
        permissions=permissions,
        permission_scope_version=permission_scope_version,
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


def _make_membership_revoked_payload(
    user_public_id: str,
    reason: str | None = None,
) -> MembershipRevokedData:
    """Build a canonical internal membership-revocation event payload."""
    now = datetime.now(UTC)
    return MembershipRevokedData(
        public_id=f"membership-event-{user_public_id}",
        timestamp=now,
        session_id="membership-sid",
        sequence_id=1,
        membership_public_id="membership-1",
        user_public_id=user_public_id,
        username=f"user-{user_public_id}",
        operator_public_id="operator-detached",
        detached_at=now,
        revoked_by_user_public_id="admin-1",
        promoted_operator_public_id=None,
        reason=reason,
    )


def _track_membership_connection(
    manager: WebSocketAuthManager,
    websocket: MagicMock,
    *,
    access_token_jti: str | None,
    membership_public_id: str | None,
    operator_claimed: bool = True,
) -> None:
    """Track one target socket with explicit token and membership claims."""
    membership_claims = (
        {"operator-detached": membership_public_id} if membership_public_id is not None else {}
    )
    principal = _make_principal(
        "user-detached",
        operator_public_ids=["operator-detached"] if operator_claimed else [],
    ).model_copy(update={"operator_membership_public_ids": membership_claims})
    manager.authenticated_connections[websocket] = principal
    manager._connection_states[websocket] = ConnectionState(
        session_id=f"session-{access_token_jti}",
        session_expires_at=datetime.now(UTC),
        access_token_jti=access_token_jti,
    )


class TestCloseUserConnections:
    """`close_user_connections` is the kill-switch's fanout primitive."""

    @pytest.mark.asyncio
    async def test_disconnect_runs_before_ws_close_to_cancel_timers_first(self) -> None:
        """`disconnect()` (cancel timers) MUST run BEFORE `await ws.close()`.

        With the inverted `await ws.close()` then `disconnect()`
        order, the event loop COULD process a pending `warn_task` /
        `hard_task` timer during the close yield, racing the kill
        switch. Cancelling timers FIRST eliminates the race —
        `cancel()` is synchronous so it lands before the event-loop
        yields to anything else.

        Records the call sequence by attaching a side-effect to
        each method and asserts the relative order.
        """
        manager = _make_manager()
        order: list[str] = []
        ws = MagicMock()

        async def _close(*_args: object, **_kwargs: object) -> None:
            await asyncio.sleep(0)
            order.append("ws.close")

        ws.close = AsyncMock(side_effect=_close)
        manager.authenticated_connections[ws] = _make_principal("user-order")
        original_disconnect = manager.disconnect

        def _spy_disconnect(target: MagicMock) -> None:
            order.append("disconnect")
            original_disconnect(target)

        manager.disconnect = _spy_disconnect
        await manager.close_user_connections(user_public_id="user-order", reason="r")
        assert order == ["disconnect", "ws.close"]

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
        manager.authenticated_connections[ws_a2] = _make_principal("user-a")
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
    async def test_ascii_reason_truncated_to_123_bytes(self) -> None:
        """Close-frame `reason` field is limited per RFC 6455 section 5.5.1."""
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
        assert len(called_kwargs["reason"].encode("utf-8")) == 123

    @pytest.mark.asyncio
    async def test_multibyte_reason_truncated_on_utf8_boundary(self) -> None:
        """Multi-byte (CJK / emoji) reason is truncated by ENCODED bytes, not chars.

        An earlier revision used `reason[:120]`
        which counts codepoints. A CJK or emoji-heavy reason could
        therefore push the encoded close frame above the 123-byte
        protocol limit and make `ws.close()` itself fail (then get
        swallowed by the warning path). The fix encodes first, slices
        bytes, then decodes with `errors="ignore"` so a multi-byte
        char split by truncation is dropped instead of corrupting
        the frame.
        """
        manager = _make_manager()
        ws = MagicMock()
        ws.close = AsyncMock()
        manager.authenticated_connections[ws] = _make_principal("user-mb")
        cjk_reason = "中文" * 100
        await manager.close_user_connections(user_public_id="user-mb", reason=cjk_reason)
        await_args = ws.close.await_args
        assert await_args is not None
        called_reason = await_args.kwargs["reason"]
        encoded_len = len(called_reason.encode("utf-8"))
        assert encoded_len <= 123
        assert called_reason == "" or called_reason[0] == "中"

    @pytest.mark.asyncio
    async def test_emoji_reason_truncated_on_utf8_boundary_no_partial_chars(self) -> None:
        """4-byte emoji codepoints split by the truncation boundary are dropped cleanly.

        Verifies `errors="ignore"` rather than `errors="strict"` so a
        partial 4-byte emoji at byte index 122 is silently dropped
        instead of raising `UnicodeDecodeError`.
        """
        manager = _make_manager()
        ws = MagicMock()
        ws.close = AsyncMock()
        manager.authenticated_connections[ws] = _make_principal("user-em")
        emoji_reason = "🚀" * 100
        await manager.close_user_connections(user_public_id="user-em", reason=emoji_reason)
        await_args = ws.close.await_args
        assert await_args is not None
        called_reason = await_args.kwargs["reason"]
        encoded_len = len(called_reason.encode("utf-8"))
        assert encoded_len <= 123
        assert all(char == "🚀" for char in called_reason)


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


class TestHandleMembershipRevoked:
    """Membership fanout retires authority before closing target sockets."""

    @pytest.mark.asyncio
    async def test_unrelated_user_skips_inventory_lookup_and_remains_connected(self) -> None:
        """A membership event does not inspect or close another user's socket.

        Given: One connected user unrelated to a membership-revocation event.
        When: The manager handles the event for its actual target user.
        Then: It neither queries token inventory nor closes the unrelated socket.
        """
        manager = _make_manager()
        repo = MagicMock()
        repo.list_active_user_token_jtis = AsyncMock(return_value=[])
        manager.repository_factory = lambda: cast(Repository, repo)
        ws = MagicMock()
        ws.close = AsyncMock()
        manager.authenticated_connections[ws] = _make_principal("unrelated-user")

        await manager._handle_membership_revoked(_make_membership_revoked_payload("user-detached"))

        repo.list_active_user_token_jtis.assert_not_awaited()
        ws.close.assert_not_awaited()
        assert ws in manager.authenticated_connections

    @pytest.mark.asyncio
    async def test_retires_connection_and_bridge_before_socket_close(self) -> None:
        """Subscription state is removed before the close frame is emitted."""
        manager = _make_manager()
        order: list[str] = []
        ws = MagicMock()
        ws.close = AsyncMock(side_effect=lambda **_kwargs: order.append("ws.close"))
        manager.authenticated_connections[ws] = _make_principal(
            "user-detached",
            operator_public_ids=["operator-detached"],
        )
        connection_manager = MagicMock()
        connection_manager.retire_connection.side_effect = lambda _ws: order.append(
            "connection.retire"
        )
        bridge = MagicMock()
        retirement = object()

        def _retire_client(_websocket: MagicMock) -> object:
            order.append("bridge.retire")
            return retirement

        bridge.retire_client.side_effect = _retire_client
        bridge.finalize_retired_client = AsyncMock(
            side_effect=lambda _retirement: order.append("bridge.finalize")
        )
        manager.set_wiring(connection_manager, bridge, None)
        original_disconnect = manager.disconnect

        def _disconnect(target: MagicMock) -> None:
            order.append("auth.disconnect")
            original_disconnect(target)

        manager.disconnect = _disconnect
        await manager._handle_membership_revoked(_make_membership_revoked_payload("user-detached"))
        assert order == [
            "auth.disconnect",
            "connection.retire",
            "bridge.retire",
            "bridge.finalize",
            "ws.close",
        ]
        ws.close.assert_awaited_once_with(code=4003, reason="membership_revoked")

    @pytest.mark.asyncio
    async def test_reason_from_event_reaches_close_frame(self) -> None:
        """Administrative detach reason is forwarded to the target socket."""
        manager = _make_manager()
        ws = MagicMock()
        ws.close = AsyncMock()
        manager.authenticated_connections[ws] = _make_principal("user-detached")
        await manager._handle_membership_revoked(
            _make_membership_revoked_payload("user-detached", reason="desk_closed")
        )
        ws.close.assert_awaited_once_with(code=4003, reason="desk_closed")

    @pytest.mark.parametrize(
        ("membership_public_id", "active_jtis"),
        [("membership-1", ["candidate-jti"]), (None, [])],
    )
    @pytest.mark.asyncio
    async def test_old_or_absent_membership_claim_closes_when_stale(
        self,
        membership_public_id: str | None,
        active_jtis: list[str],
    ) -> None:
        """The revoked generation and an inventory-stale absent claim close."""
        manager = _make_manager()
        repo = MagicMock()
        repo.list_active_user_token_jtis = AsyncMock(return_value=active_jtis)
        manager.repository_factory = lambda: cast(Repository, repo)
        ws = MagicMock()
        ws.close = AsyncMock()
        _track_membership_connection(
            manager,
            ws,
            access_token_jti="candidate-jti",
            membership_public_id=membership_public_id,
        )

        await manager._handle_membership_revoked(_make_membership_revoked_payload("user-detached"))

        ws.close.assert_awaited_once_with(code=4003, reason="membership_revoked")
        assert ws not in manager.authenticated_connections

    @pytest.mark.asyncio
    async def test_delayed_event_preserves_active_empty_membership_claim(self) -> None:
        """A post-detach login without desk authority survives a delayed event."""
        manager = _make_manager()
        repo = MagicMock()
        repo.list_active_user_token_jtis = AsyncMock(return_value=["new-empty-jti"])
        manager.repository_factory = lambda: cast(Repository, repo)
        ws = MagicMock()
        ws.close = AsyncMock()
        _track_membership_connection(
            manager,
            ws,
            access_token_jti="new-empty-jti",
            membership_public_id=None,
            operator_claimed=False,
        )

        await manager._handle_membership_revoked(_make_membership_revoked_payload("user-detached"))

        ws.close.assert_not_awaited()
        assert ws in manager.authenticated_connections

    @pytest.mark.asyncio
    async def test_inactive_jti_closes_connection_from_different_generation(self) -> None:
        """Token inventory still fences pre-attach sessions with another generation."""
        manager = _make_manager()
        repo = MagicMock()
        repo.list_active_user_token_jtis = AsyncMock(return_value=[])
        manager.repository_factory = lambda: cast(Repository, repo)
        ws = MagicMock()
        ws.close = AsyncMock()
        _track_membership_connection(
            manager,
            ws,
            access_token_jti="revoked-jti",
            membership_public_id="membership-other",
        )

        await manager._handle_membership_revoked(_make_membership_revoked_payload("user-detached"))

        ws.close.assert_awaited_once_with(code=4003, reason="membership_revoked")

    @pytest.mark.asyncio
    async def test_delayed_event_preserves_new_generation_with_active_jti(self) -> None:
        """A reattached login survives a delayed event for the prior generation."""
        manager = _make_manager()
        repo = MagicMock()
        repo.list_active_user_token_jtis = AsyncMock(return_value=["new-jti"])
        manager.repository_factory = lambda: cast(Repository, repo)
        ws = MagicMock()
        ws.close = AsyncMock()
        _track_membership_connection(
            manager,
            ws,
            access_token_jti="new-jti",
            membership_public_id="membership-new",
        )

        await manager._handle_membership_revoked(_make_membership_revoked_payload("user-detached"))

        ws.close.assert_not_awaited()
        assert ws in manager.authenticated_connections

    @pytest.mark.asyncio
    async def test_snapshot_excludes_connection_opened_during_inventory_read(self) -> None:
        """A connection created after the pre-await snapshot cannot be closed."""
        manager = _make_manager()
        lookup_started = asyncio.Event()
        release_lookup = asyncio.Event()

        async def _lookup_active_jtis(user_public_id: str) -> list[str]:
            assert user_public_id == "user-detached"
            lookup_started.set()
            await release_lookup.wait()
            return ["race-jti", "new-jti"]

        repo = MagicMock()
        repo.list_active_user_token_jtis = AsyncMock(side_effect=_lookup_active_jtis)
        manager.repository_factory = lambda: cast(Repository, repo)
        old_ws = MagicMock()
        old_ws.close = AsyncMock()
        _track_membership_connection(
            manager,
            old_ws,
            access_token_jti="race-jti",
            membership_public_id="membership-1",
        )

        handler_task = asyncio.create_task(
            manager._handle_membership_revoked(_make_membership_revoked_payload("user-detached"))
        )
        await lookup_started.wait()
        new_ws = MagicMock()
        new_ws.close = AsyncMock()
        _track_membership_connection(
            manager,
            new_ws,
            access_token_jti="new-jti",
            membership_public_id="membership-new",
        )
        release_lookup.set()
        await handler_task

        old_ws.close.assert_awaited_once_with(code=4003, reason="membership_revoked")
        new_ws.close.assert_not_awaited()
        assert new_ws in manager.authenticated_connections

    @pytest.mark.asyncio
    async def test_inventory_failure_uses_membership_generation_fail_safe(self) -> None:
        """Failure closes old, absent, and state-less sockets but preserves replacement."""
        manager = _make_manager()
        repo = MagicMock()
        repo.list_active_user_token_jtis = AsyncMock(side_effect=RuntimeError("lookup failed"))
        manager.repository_factory = lambda: cast(Repository, repo)
        matching_ws = MagicMock()
        matching_ws.close = AsyncMock()
        absent_ws = MagicMock()
        absent_ws.close = AsyncMock()
        missing_state_ws = MagicMock()
        missing_state_ws.close = AsyncMock()
        replacement_ws = MagicMock()
        replacement_ws.close = AsyncMock()
        _track_membership_connection(
            manager,
            matching_ws,
            access_token_jti="matching-jti",
            membership_public_id="membership-1",
        )
        _track_membership_connection(
            manager,
            absent_ws,
            access_token_jti="absent-jti",
            membership_public_id=None,
        )
        _track_membership_connection(
            manager,
            missing_state_ws,
            access_token_jti=None,
            membership_public_id="membership-new",
        )
        _track_membership_connection(
            manager,
            replacement_ws,
            access_token_jti="replacement-jti",
            membership_public_id="membership-new",
        )

        await manager._handle_membership_revoked(_make_membership_revoked_payload("user-detached"))

        matching_ws.close.assert_awaited_once()
        absent_ws.close.assert_awaited_once()
        missing_state_ws.close.assert_awaited_once()
        replacement_ws.close.assert_not_awaited()
        assert replacement_ws in manager.authenticated_connections

    @pytest.mark.parametrize(
        "failure_stage",
        ["connection", "bridge", "finalize"],
    )
    @pytest.mark.asyncio
    async def test_retirement_failure_does_not_block_socket_close(
        self,
        failure_stage: str,
    ) -> None:
        """A cleanup failure remains fail-closed at the socket boundary.

        Given: One retirement stage raises while a detached user's socket is live,
        When: The membership-revocation fanout closes that user's connections,
        Then: The socket still closes and every reachable later cleanup stage runs.
        """
        manager = _make_manager()
        ws = MagicMock()
        ws.close = AsyncMock()
        manager.authenticated_connections[ws] = _make_principal(
            "user-detached",
            operator_public_ids=["operator-detached"],
        )
        connection_manager = MagicMock()
        bridge = MagicMock()
        retirement = object()
        bridge.retire_client.return_value = retirement
        bridge.finalize_retired_client = AsyncMock()
        if failure_stage == "connection":
            connection_manager.retire_connection.side_effect = RuntimeError("manager failure")
        elif failure_stage == "bridge":
            bridge.retire_client.side_effect = RuntimeError("bridge failure")
        else:
            bridge.finalize_retired_client.side_effect = RuntimeError("finalize failure")
        manager.set_wiring(connection_manager, bridge, None)

        await manager._handle_membership_revoked(_make_membership_revoked_payload("user-detached"))

        ws.close.assert_awaited_once_with(code=4003, reason="membership_revoked")
        connection_manager.retire_connection.assert_called_once_with(ws)
        bridge.retire_client.assert_called_once_with(ws)
        if failure_stage == "bridge":
            bridge.finalize_retired_client.assert_not_awaited()
        else:
            bridge.finalize_retired_client.assert_awaited_once_with(retirement)

    @pytest.mark.asyncio
    async def test_detach_during_backpressure_retires_before_send(self) -> None:
        """An in-flight bridge dispatch cannot send after the detach barrier."""
        manager = _make_manager()
        connection_manager = WebSocketConnectionManager()
        bridge = connection_manager.zmq_bridge
        manager.set_wiring(connection_manager, bridge, None)
        ws = MagicMock()
        ws.send_text = AsyncMock()
        ws.close = AsyncMock()
        await connection_manager.connect(ws, accept=False)
        topic = "market.kraken.BTC-USD.candles"
        connection_manager.subscribe_client(ws, topic)
        bridge._register_topic_subscription(ws, topic, throttle_ms=0)
        manager.authenticated_connections[ws] = _make_principal(
            "user-detached",
            operator_public_ids=["operator-detached"],
        )
        backpressure_entered = asyncio.Event()
        release_backpressure = asyncio.Event()

        async def _block_backpressure(
            _subscription: object,
            _topic: str,
            _max_pending: int,
            _is_trade: bool,
        ) -> bool:
            backpressure_entered.set()
            await release_backpressure.wait()
            return False

        with (
            patch.object(bridge, "_handle_backpressure", new=_block_backpressure),
            patch.object(bridge, "_stop_zmq_subscription", new=AsyncMock()),
        ):
            dispatch_task = asyncio.create_task(
                bridge._forward_to_clients(topic, topic, '{"type":"candle"}')
            )
            await backpressure_entered.wait()
            await manager._handle_membership_revoked(
                _make_membership_revoked_payload("user-detached")
            )
            assert ws not in manager.authenticated_connections
            assert connection_manager.is_connection_active(ws) is False
            assert ws not in connection_manager.client_subscriptions
            assert all(
                ws not in subscribers
                for subscribers in connection_manager.topic_subscribers.values()
            )
            assert ws not in bridge.client_subscriptions
            assert all(
                ws not in subscriptions for subscriptions in bridge.topic_subscriptions.values()
            )
            release_backpressure.set()
            await dispatch_task
        ws.send_text.assert_not_awaited()
        ws.close.assert_awaited_once_with(code=4003, reason="membership_revoked")

    @pytest.mark.asyncio
    async def test_detach_during_subscribe_authorization_cannot_resurrect_socket(self) -> None:
        """A suspended subscribe request cannot register after its detach barrier."""
        manager = _make_manager()
        connection_manager = WebSocketConnectionManager()
        bridge = connection_manager.zmq_bridge
        manager.set_wiring(connection_manager, bridge, None)
        ws = MagicMock()
        ws.send_text = AsyncMock()
        ws.close = AsyncMock()
        await connection_manager.connect(ws, accept=False)
        principal = _make_principal(
            "user-detached",
            operator_public_ids=["operator-detached"],
        )
        manager.authenticated_connections[ws] = principal
        topic = "market."
        request = WSSubscribeRequest(
            public_id="subscribe-race",
            timestamp=datetime.now(UTC),
            session_id="race-session",
            sequence_id=1,
            topics=[topic],
        )
        authorization_entered = asyncio.Event()
        release_authorization = asyncio.Event()
        repository = cast(Repository, MagicMock())

        async def _pause_authorization(
            *,
            topics: list[str],
            principal: AuthPrincipal,
            repository: Repository | None,
            as_of: datetime,
        ) -> tuple[list[str], list[str]]:
            assert topics == [topic]
            assert principal is manager.authenticated_connections[ws]
            assert repository is repository_for_dispatch
            assert as_of.tzinfo is not None
            authorization_entered.set()
            await release_authorization.wait()
            return topics, []

        repository_for_dispatch = repository
        settings = MagicMock(db_url="sqlite+aiosqlite://")
        with (
            patch(
                "snapper.interface.websocket.handlers.subscribe.partition_authorized_topics",
                new=_pause_authorization,
            ),
            patch(
                "snapper.interface.websocket.dispatcher.get_settings",
                return_value=settings,
            ),
            patch(
                "snapper.interface.websocket.dispatcher.get_repository",
                return_value=repository,
            ),
        ):
            dispatch_table = _build_dispatch_table(ws, connection_manager, manager)
            subscribe_task = asyncio.create_task(
                _dispatch_single_message(request, principal, dispatch_table)
            )
            await asyncio.wait_for(authorization_entered.wait(), timeout=2.0)
            await manager._handle_membership_revoked(
                _make_membership_revoked_payload("user-detached")
            )
            release_authorization.set()
            await subscribe_task

        assert ws not in manager.authenticated_connections
        assert connection_manager.is_connection_active(ws) is False
        assert ws not in connection_manager.client_subscriptions
        assert ws not in bridge.client_subscriptions
        assert all(
            ws not in subscribers for subscribers in connection_manager.topic_subscribers.values()
        )
        assert all(ws not in subscribers for subscribers in bridge.topic_subscriptions.values())
        ws.send_text.assert_not_awaited()
        ws.close.assert_awaited_once_with(code=4003, reason="membership_revoked")


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
    async def test_membership_revoked_topic_invokes_handler(self) -> None:
        """Internal membership payload routes to the typed async handler."""
        manager = _make_manager()
        payload = _make_membership_revoked_payload("user-1").to_json()
        with patch.object(manager, "_handle_membership_revoked", new=AsyncMock()) as handler:
            await manager._admin_dispatch_frame("admin.membership_revoked", payload)
        handler.assert_awaited_once()
        await_args = handler.await_args
        assert await_args is not None
        called_arg = await_args.args[0]
        assert isinstance(called_arg, MembershipRevokedData)
        assert called_arg.user_public_id == "user-1"

    @pytest.mark.asyncio
    async def test_unknown_topic_is_ignored(self) -> None:
        """Topics outside the subscribed set are silently dropped.

        Defensive — a misrouted frame from the broker (e.g.
        admin.scope_revoked once another listener wires it but before
        this listener restarts) must not raise out of the dispatch loop.
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
        with (
            patch.object(
                manager,
                "_handle_user_deactivated",
                new=AsyncMock(side_effect=asyncio.CancelledError()),
            ),
            pytest.raises(asyncio.CancelledError),
        ):
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

    @pytest.mark.asyncio
    async def test_unicode_decode_error_returns_none_does_not_kill_loop(self) -> None:
        """Invalid UTF-8 in topic OR payload → ``None`` (loop continues).

        An earlier revision performed the bytes→str decode
        OUTSIDE the recv `try` block, so a `UnicodeDecodeError` could
        escape the helper and unwind `_admin_listen_loop`. Combined
        with the original `is_not_none` idempotency check on
        `_admin_listen_task`, that meant a single bad frame could
        kill the listener for the rest of the process lifetime.
        Decode is now inside the try; invalid UTF-8 is logged +
        suppressed.
        """
        manager = _make_manager()
        subscriber = MagicMock()
        invalid_utf8 = b"\xff\xfe\xfd"
        subscriber.recv_multipart = AsyncMock(return_value=(invalid_utf8, b"{}"))
        with patch("snapper.auth.websocket_auth.asyncio.sleep", new=AsyncMock()):
            result = await manager._admin_recv_one_frame(subscriber)
        assert result is None


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
        assert manager._deactivation_scan_task is None

    @pytest.mark.asyncio
    async def test_empty_xpub_starts_db_fallback_when_repo_factory_configured(self) -> None:
        """Broker-less startup still runs the DB-backed deactivation fallback."""
        manager = _make_manager()
        repo = InactiveUserLookupRepo([])
        manager.repository_factory = lambda: cast(Repository, repo)
        invocations: list[int] = []

        async def _never() -> None:
            invocations.append(1)
            await asyncio.Event().wait()

        with patch.object(manager, "_deactivation_fallback_scan_loop", side_effect=_never):
            await manager.start_admin_listener("")
            await asyncio.sleep(0)
            first_task = manager._deactivation_scan_task
            await manager.start_admin_listener("")
            assert manager._deactivation_scan_task is first_task
            assert first_task is not None
            assert len(invocations) == 1
        await manager.stop_admin_listener()
        assert manager._deactivation_scan_task is None

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
    async def test_start_restarts_after_previous_task_finished(self) -> None:
        """A previous listener task that finished early is reaped + restarted.

        An earlier revision used an idempotency check of
        `is not None`, which kept stale `_admin_listen_task` /
        `_admin_subscriber` / `_admin_zmq_context` refs around AFTER
        the task crashed and exited. The fix treats `task.done()` as
        restartable so the kill-switch path stays live across single-
        listener failures.
        """
        manager = _make_manager()
        mock_socket = MagicMock()
        mock_context = MagicMock()
        mock_context.socket.return_value = mock_socket
        mock_context_cls = MagicMock(return_value=mock_context)
        with patch("snapper.auth.websocket_auth.zmq.asyncio.Context", mock_context_cls):
            await manager.start_admin_listener("tcp://broker:5555")
            first_task = manager._admin_listen_task
            assert first_task is not None
            manager._admin_running = False
            await first_task
            assert first_task.done()
            await manager.start_admin_listener("tcp://broker:5555")
            second_task = manager._admin_listen_task
        assert second_task is not first_task
        assert mock_context_cls.call_count == 2
        await manager.stop_admin_listener()

    @pytest.mark.asyncio
    async def test_concurrent_start_stop_serialised_by_lock(self) -> None:
        """`asyncio.Lock` prevents overlapping start/stop from racing.

        Without serialisation, a `stop` clearing the
        task ref could let a concurrent `start` allocate a fresh
        socket which the in-progress `stop` would then close — losing
        the listener. The fix serialises both methods via
        `_admin_listener_lock`.
        """
        manager = _make_manager()
        mock_socket = MagicMock()
        mock_context = MagicMock()
        mock_context.socket.return_value = mock_socket
        mock_context_cls = MagicMock(return_value=mock_context)
        with patch("snapper.auth.websocket_auth.zmq.asyncio.Context", mock_context_cls):
            await manager.start_admin_listener("tcp://broker:5555")
            stop_then_start = asyncio.gather(
                manager.stop_admin_listener(),
                manager.start_admin_listener("tcp://broker:5555"),
            )
            await stop_then_start
        assert manager._admin_listener_lock.locked() is False
        await manager.stop_admin_listener()

    @pytest.mark.asyncio
    async def test_stop_swallows_close_errors(self) -> None:
        """Subscriber/context close errors during shutdown are swallowed.

        Mirrors the `_shutdown_user_service_publisher` resilience
        contract: a transient broker issue at shutdown must
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


class TestDeactivationFallbackScan:
    """DB-backed fallback closes matching WebSocket sessions without broker delivery."""

    @pytest.mark.asyncio
    async def test_scan_closes_inactive_connected_users_only(self) -> None:
        """Only connections for inactive users returned by the repo are closed."""
        manager = _make_manager()
        repo = InactiveUserLookupRepo(["user-a"])
        manager.repository_factory = lambda: cast(Repository, repo)
        ws_a = MagicMock()
        ws_a.close = AsyncMock()
        ws_b = MagicMock()
        ws_b.close = AsyncMock()
        manager.authenticated_connections[ws_a] = _make_principal("user-a")
        manager.authenticated_connections[ws_b] = _make_principal("user-b")
        await manager._scan_deactivated_connections_once()
        assert repo.queries == [["user-a", "user-b"]]
        ws_a.close.assert_awaited_once_with(code=4003, reason="account_deactivated")
        ws_b.close.assert_not_awaited()
        assert ws_a not in manager.authenticated_connections
        assert ws_b in manager.authenticated_connections

    @pytest.mark.asyncio
    async def test_scan_without_factory_is_noop(self) -> None:
        """No repository factory means the fallback scanner is disabled."""
        manager = _make_manager()
        ws = MagicMock()
        ws.close = AsyncMock()
        manager.authenticated_connections[ws] = _make_principal("user-a")
        await manager._scan_deactivated_connections_once()
        ws.close.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_scan_with_no_connections_does_not_query_repo(self) -> None:
        """No authenticated connections avoids a pointless DB query."""
        manager = _make_manager()
        repo = InactiveUserLookupRepo(["user-a"])
        manager.repository_factory = lambda: cast(Repository, repo)
        await manager._scan_deactivated_connections_once()
        assert repo.queries == []

    @pytest.mark.asyncio
    async def test_scan_tolerates_repo_without_lookup_method(self) -> None:
        """Older repository doubles do not break the fallback path."""
        manager = _make_manager()
        ws = MagicMock()
        ws.close = AsyncMock()
        manager.authenticated_connections[ws] = _make_principal("user-a")
        manager.repository_factory = lambda: cast(Repository, object())
        await manager._scan_deactivated_connections_once()
        ws.close.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_scan_tolerates_lookup_error(self) -> None:
        """DB read failures are logged and leave connections untouched."""
        manager = _make_manager()
        ws = MagicMock()
        ws.close = AsyncMock()
        manager.authenticated_connections[ws] = _make_principal("user-a")
        repo = FailingInactiveUserLookupRepo()
        manager.repository_factory = lambda: cast(Repository, repo)
        await manager._scan_deactivated_connections_once()
        ws.close.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_scan_closes_stale_membership_and_retires_subscriptions(self) -> None:
        """Broker loss still closes a detached viewer after subscription cleanup."""
        manager = _make_manager()
        repo = MembershipLookupRepo(
            {
                "user-target": {"operator-still-active"},
                "user-bystander": {"operator-bystander"},
            }
        )
        connection_manager = MagicMock()
        bridge = MagicMock()
        retirement = object()
        bridge.retire_client.return_value = retirement
        bridge.finalize_retired_client = AsyncMock()
        manager.set_wiring(
            connection_manager,
            bridge,
            lambda: cast(Repository, repo),
        )
        ws_target = MagicMock()
        ws_target.close = AsyncMock()
        ws_bystander = MagicMock()
        ws_bystander.close = AsyncMock()
        manager.authenticated_connections[ws_target] = _make_principal(
            "user-target",
            operator_public_ids=["operator-still-active", "operator-detached"],
        )
        manager.authenticated_connections[ws_bystander] = _make_principal(
            "user-bystander",
            operator_public_ids=["operator-bystander"],
        )
        await manager._scan_deactivated_connections_once()
        ws_target.close.assert_awaited_once_with(code=4003, reason="membership_revoked")
        ws_bystander.close.assert_not_awaited()
        connection_manager.retire_connection.assert_called_once_with(ws_target)
        bridge.retire_client.assert_called_once_with(ws_target)
        bridge.finalize_retired_client.assert_awaited_once_with(retirement)

    @pytest.mark.asyncio
    async def test_scan_closes_socket_from_replaced_membership_generation(self) -> None:
        """Broker loss still retires a socket minted before desk re-attachment."""
        manager = _make_manager()
        repo = MembershipLookupRepo({"user-target": {"operator-current"}})
        connection_manager = MagicMock()
        bridge = MagicMock()
        retirement = object()
        bridge.retire_client.return_value = retirement
        bridge.finalize_retired_client = AsyncMock()
        manager.set_wiring(
            connection_manager,
            bridge,
            lambda: cast(Repository, repo),
        )
        ws = MagicMock()
        ws.close = AsyncMock()
        manager.authenticated_connections[ws] = _make_principal(
            "user-target", operator_public_ids=["operator-current"]
        ).model_copy(
            update={
                "operator_membership_public_ids": {"operator-current": "membership-before-detach"}
            }
        )
        await manager._scan_deactivated_connections_once()
        ws.close.assert_awaited_once_with(code=4003, reason="membership_revoked")
        connection_manager.retire_connection.assert_called_once_with(ws)
        bridge.retire_client.assert_called_once_with(ws)
        bridge.finalize_retired_client.assert_awaited_once_with(retirement)

    @pytest.mark.asyncio
    async def test_scan_closes_partially_versioned_membership_claim(self) -> None:
        """A mixed legacy and generation-pinned desk claim fails closed.

        Given: A token claims two active desks but pins only one membership generation.
        When: The fallback scanner reconciles it against both current memberships.
        Then: The incomplete generation map is stale and its socket is closed.
        """
        manager = _make_manager()
        repo = MembershipLookupRepo(
            {"user-target": {"operator-current", "operator-without-generation"}}
        )
        manager.repository_factory = lambda: cast(Repository, repo)
        ws = MagicMock()
        ws.close = AsyncMock()
        manager.authenticated_connections[ws] = _make_principal(
            "user-target",
            operator_public_ids=["operator-current", "operator-without-generation"],
        ).model_copy(
            update={
                "operator_membership_public_ids": {
                    "operator-current": "membership-operator-current"
                }
            }
        )

        await manager._scan_deactivated_connections_once()

        ws.close.assert_awaited_once_with(code=4003, reason="membership_revoked")
        assert ws not in manager.authenticated_connections

    @pytest.mark.asyncio
    async def test_scan_fences_empty_claim_jtis_without_closing_new_login(self) -> None:
        """Broker loss closes revoked or missing JTIs but preserves the active login."""
        manager = _make_manager()
        repo = MembershipTokenLookupRepo(
            {"user-detached": set()},
            {"user-detached": {"new-jti"}},
        )
        manager.repository_factory = lambda: cast(Repository, repo)
        revoked_ws = MagicMock()
        revoked_ws.close = AsyncMock()
        missing_jti_ws = MagicMock()
        missing_jti_ws.close = AsyncMock()
        new_ws = MagicMock()
        new_ws.close = AsyncMock()
        _track_membership_connection(
            manager,
            revoked_ws,
            access_token_jti="revoked-jti",
            membership_public_id=None,
            operator_claimed=False,
        )
        _track_membership_connection(
            manager,
            missing_jti_ws,
            access_token_jti=None,
            membership_public_id=None,
            operator_claimed=False,
        )
        _track_membership_connection(
            manager,
            new_ws,
            access_token_jti="new-jti",
            membership_public_id=None,
            operator_claimed=False,
        )

        await manager._scan_deactivated_connections_once()

        revoked_ws.close.assert_awaited_once_with(code=4003, reason="membership_revoked")
        missing_jti_ws.close.assert_awaited_once_with(code=4003, reason="membership_revoked")
        new_ws.close.assert_not_awaited()
        assert new_ws in manager.authenticated_connections
        assert repo.token_queries == ["user-detached"]

    @pytest.mark.asyncio
    async def test_membership_scan_closes_only_stale_connection_generation(self) -> None:
        """Coexisting old and current membership generations are fenced per socket."""
        manager = _make_manager()
        repo = MembershipTokenLookupRepo(
            {"user-target": {"operator-current"}},
            {"user-target": {"old-jti", "new-jti"}},
        )
        manager.repository_factory = lambda: cast(Repository, repo)
        old_ws = MagicMock()
        old_ws.close = AsyncMock()
        new_ws = MagicMock()
        new_ws.close = AsyncMock()
        old_principal = _make_principal(
            "user-target",
            operator_public_ids=["operator-current"],
        ).model_copy(
            update={
                "operator_membership_public_ids": {"operator-current": "membership-before-detach"}
            }
        )
        new_principal = _make_principal(
            "user-target",
            operator_public_ids=["operator-current"],
        ).model_copy(
            update={
                "operator_membership_public_ids": {
                    "operator-current": "membership-operator-current"
                }
            }
        )
        manager.authenticated_connections[old_ws] = old_principal
        manager.authenticated_connections[new_ws] = new_principal
        manager._connection_states[old_ws] = ConnectionState(
            session_id="old-session",
            session_expires_at=datetime.now(UTC),
            access_token_jti="old-jti",
        )
        manager._connection_states[new_ws] = ConnectionState(
            session_id="new-session",
            session_expires_at=datetime.now(UTC),
            access_token_jti="new-jti",
        )

        await manager._scan_deactivated_connections_once()

        old_ws.close.assert_awaited_once_with(code=4003, reason="membership_revoked")
        new_ws.close.assert_not_awaited()
        assert new_ws in manager.authenticated_connections
        assert repo.token_queries == ["user-target"]
        assert [query[0] for query in repo.membership_queries] == ["user-target"]

    @pytest.mark.asyncio
    async def test_token_inventory_scan_tolerates_lookup_error(self) -> None:
        """A token-inventory failure leaves an empty-claim socket untouched."""
        manager = _make_manager()
        repo = FailingTokenInventoryLookupRepo()
        manager.repository_factory = lambda: cast(Repository, repo)
        ws = MagicMock()
        ws.close = AsyncMock()
        _track_membership_connection(
            manager,
            ws,
            access_token_jti="unknown-jti",
            membership_public_id=None,
            operator_claimed=False,
        )

        await manager._scan_deactivated_connections_once()

        ws.close.assert_not_awaited()
        assert ws in manager.authenticated_connections

    @pytest.mark.asyncio
    async def test_explicit_admin_scope_retains_structural_global_authority(self) -> None:
        """Effective-permission policy keeps current ADMIN credentials global."""
        manager = _make_manager()
        repo = MembershipLookupRepo({})
        manager.repository_factory = lambda: cast(Repository, repo)
        ws = MagicMock()
        ws.close = AsyncMock()
        manager.authenticated_connections[ws] = _make_principal(
            "admin-user",
            role=UserRole.ADMIN,
            operator_public_ids=["global-operator"],
            permissions=[Permission.MANAGE_DESK_MEMBERSHIPS.value],
            permission_scope_version=PERMISSION_SCOPE_VERSION,
        )
        await manager._scan_deactivated_connections_once()
        ws.close.assert_not_awaited()
        assert repo.membership_queries == []

    @pytest.mark.asyncio
    async def test_membership_scan_tolerates_lookup_error(self) -> None:
        """Transient membership DB failures leave active sockets untouched."""
        manager = _make_manager()
        repo = FailingMembershipLookupRepo()
        manager.repository_factory = lambda: cast(Repository, repo)
        ws = MagicMock()
        ws.close = AsyncMock()
        manager.authenticated_connections[ws] = _make_principal(
            "user-target",
            operator_public_ids=["operator-detached"],
        )
        await manager._scan_deactivated_connections_once()
        ws.close.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_scan_loop_propagates_cancelled_error(self) -> None:
        """Cancellation still unwinds the fallback loop cleanly."""
        manager = _make_manager()
        repo = InactiveUserLookupRepo([])
        manager.repository_factory = lambda: cast(Repository, repo)
        await assert_fallback_loop_propagates_scan_cancelled(
            manager._deactivation_fallback_scan_loop,
            manager,
            "_scan_deactivated_connections_once",
        )

    @pytest.mark.asyncio
    async def test_scan_loop_propagates_sleep_cancelled_error(self) -> None:
        """Cancellation during the scan interval also unwinds cleanly."""
        manager = _make_manager()
        repo = InactiveUserLookupRepo([])
        manager.repository_factory = lambda: cast(Repository, repo)
        await assert_fallback_loop_propagates_sleep_cancelled(
            manager._deactivation_fallback_scan_loop
        )


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
        targets: list[MagicMock] = []
        for _ in range(5):
            ws = MagicMock()
            ws.close = AsyncMock()
            manager.authenticated_connections[ws] = _make_principal("user-many")
            targets.append(ws)
        closed = await manager.close_user_connections(user_public_id="user-many", reason="bulk")
        assert closed == 5
        assert manager.authenticated_connections == {}
