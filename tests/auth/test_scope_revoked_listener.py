"""Tests for the admin.scope_revoked listener on WebSocketAuthManager.

Covers the mid-session revalidation half: for each connection with an
AI-delegate lifecycle identity whose principal is on the affected operator,
recompute the allowed ``(exchange, symbol)`` pair set and narrow the
client's subscriptions to the still-covered topics. The WS stays
open — only wallet-scoped out-of-scope topics are unsubscribed; every
other category (market, system, backtest, paper signals) keeps
flowing. The complementary subscribe-time filter is covered elsewhere;
this test module owns the post-subscribe revalidation contract.

The ZMQ socket layer is mocked; only the handler + fanout logic is
exercised.
"""

from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.websocket_auth import WebSocketAuthManager
from snapper.messaging.schemas.data import ScopeGrantedData
from snapper.messaging.schemas.data import ScopeHandedOverData
from snapper.messaging.schemas.data import ScopeRevokedData


def _make_manager(
    *,
    connection_manager: MagicMock | None = None,
    zmq_bridge: MagicMock | None = None,
    repository_factory: MagicMock | None = None,
) -> WebSocketAuthManager:
    """Return a fresh ``WebSocketAuthManager`` singleton with optional wiring."""
    WebSocketAuthManager.clear_instance()
    manager = WebSocketAuthManager()
    if connection_manager is not None or zmq_bridge is not None or repository_factory is not None:
        manager.set_wiring(
            connection_manager=connection_manager,
            zmq_bridge=zmq_bridge,
            repository_factory=repository_factory,
        )
    return manager


def _make_event(
    *,
    operator_public_id: str = "op-1",
    wallet_public_id: str = "wal-1",
    scope_kind: str = "instrument",
    underlying_public_id: str | None = None,
    instrument_public_id: str | None = "inst-BTC-USD",
) -> ScopeRevokedData:
    """Build a canonical ``admin.scope_revoked`` event payload."""
    now = datetime.now(UTC)
    return ScopeRevokedData(
        public_id="evt-sr-1",
        timestamp=now,
        session_id="test-sid",
        sequence_id=1,
        grant_public_id="grant-1",
        operator_public_id=operator_public_id,
        wallet_public_id=wallet_public_id,
        scope_kind=scope_kind,
        underlying_public_id=underlying_public_id,
        instrument_public_id=instrument_public_id,
        revoked_at=now,
        revoked_by_user_public_id="user-admin",
        reason="alice left",
    )


def _delegate_principal(
    *,
    user_public_id: str = "user-delegate",
    operators: tuple[str, ...] = ("op-1",),
    role: UserRole = UserRole.AI_DELEGATE,
) -> AuthPrincipal:
    """Build an AI review principal fixture."""
    return AuthPrincipal(
        username=f"{role.value}-{user_public_id}",
        role=role,
        user_public_id=user_public_id,
        delegate_public_id=f"delegate-{user_public_id}",
        operator_public_ids=list(operators),
    )


def _mock_connection_manager(subscriptions_by_ws: dict[MagicMock, set[str]]) -> MagicMock:
    """Build a connection manager returning per-ws subscription snapshots."""
    cm = MagicMock()

    def _get_subs(ws: MagicMock) -> set[str]:
        return subscriptions_by_ws.get(ws, set())

    cm.get_client_subscriptions = MagicMock(side_effect=_get_subs)
    cm.unsubscribe_client = MagicMock()
    return cm


def _mock_bridge() -> MagicMock:
    """Return a ZMQ bridge mock with async ``remove_subscription``."""
    bridge = MagicMock()
    bridge.remove_subscription = AsyncMock()
    return bridge


def _repo_with_pairs(pairs: set[tuple[str, str]]) -> MagicMock:
    """Return a repository mock returning ``pairs`` for the pair query."""
    repo = MagicMock()
    repo.list_scope_grant_instrument_pairs = AsyncMock(return_value=pairs)
    return repo


class TestHandleScopeRevoked:
    """Tests for ``WebSocketAuthManager._handle_scope_revoked``."""

    @pytest.mark.asyncio
    async def test_missing_wiring_logs_warning_and_noops(self) -> None:
        """Without ``set_wiring``, the handler logs + returns without raising.

        Single-instance dev boot before lifespan attaches; the admin
        subscriber can still fire without crashing the listener loop.
        """
        manager = _make_manager()
        event = _make_event()
        await manager._handle_scope_revoked(event)

    @pytest.mark.asyncio
    async def test_closes_matching_wallet_scoped_subscriptions(self) -> None:
        """AI_DELEGATE with BTC-USD sub → BTC-USD unsubscribed on revoke of BTC-USD.

        Given: AI_DELEGATE connection subscribed to
            ``signals.kraken.BTC-USD.live`` + ``market.kraken.BTC-USD.ticks``,
            with allowed pairs empty (BTC-USD was just revoked),
        When: handler processes the event,
        Then: signals.kraken.BTC-USD.live is unsubscribed + error frame
            sent + bridge.remove_subscription called; market.* stays.
        """
        ws = MagicMock()
        ws.send_text = AsyncMock()
        principal = _delegate_principal()
        subs = {
            "signals.kraken.BTC-USD.live",
            "market.kraken.BTC-USD.ticks",
        }
        cm = _mock_connection_manager({ws: subs})
        bridge = _mock_bridge()
        repo = _repo_with_pairs(set())
        manager = _make_manager(
            connection_manager=cm,
            zmq_bridge=bridge,
            repository_factory=lambda: repo,
        )
        manager.authenticated_connections[ws] = principal

        await manager._handle_scope_revoked(_make_event())

        cm.unsubscribe_client.assert_called_once_with(ws, "signals.kraken.BTC-USD.live")
        bridge.remove_subscription.assert_awaited_once_with(ws, ["signals.kraken.BTC-USD.live"])
        frame_sent = ws.send_text.await_args.args[0]
        assert "topic_outside_scope" in frame_sent
        assert "signals.kraken.BTC-USD.live" in frame_sent
        ws.close.assert_not_called()

    @pytest.mark.asyncio
    async def test_ai_reviewer_subscription_is_revalidated_on_revoke(self) -> None:
        """Apply revoked scope to an authenticated review-only principal.

        Given: An AI_REVIEWER subscribed to a wallet-scoped signal that lost its grant,
        When: The scope-revoked handler revalidates matching connections,
        Then: The reviewer subscription is removed through the existing delegate path.
        """
        ws = MagicMock()
        ws.send_text = AsyncMock()
        principal = _delegate_principal(role=UserRole.AI_REVIEWER)
        topic = "signals.kraken.BTC-USD.live"
        cm = _mock_connection_manager({ws: {topic}})
        bridge = _mock_bridge()
        repo = _repo_with_pairs(set())
        manager = _make_manager(
            connection_manager=cm,
            zmq_bridge=bridge,
            repository_factory=lambda: repo,
        )
        manager.authenticated_connections[ws] = principal

        await manager._handle_scope_revoked(_make_event())

        cm.unsubscribe_client.assert_called_once_with(ws, topic)
        bridge.remove_subscription.assert_awaited_once_with(ws, [topic])
        ws.send_text.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_preserves_in_scope_topics(self) -> None:
        """Delegate with BTC-USD + ETH-USD scopes: revoke-like event narrows correctly.

        If the post-revocation pair set still covers some subscriptions,
        those subscriptions persist. Only the now-unscoped topics are
        dropped.
        """
        ws = MagicMock()
        ws.send_text = AsyncMock()
        principal = _delegate_principal()
        subs = {
            "signals.kraken.BTC-USD.live",
            "signals.kraken.ETH-USD.live",
        }
        cm = _mock_connection_manager({ws: subs})
        bridge = _mock_bridge()
        repo = _repo_with_pairs({("kraken", "ETH-USD")})
        manager = _make_manager(
            connection_manager=cm,
            zmq_bridge=bridge,
            repository_factory=lambda: repo,
        )
        manager.authenticated_connections[ws] = principal

        await manager._handle_scope_revoked(_make_event())

        cm.unsubscribe_client.assert_called_once_with(ws, "signals.kraken.BTC-USD.live")
        bridge.remove_subscription.assert_awaited_once_with(ws, ["signals.kraken.BTC-USD.live"])

    @pytest.mark.asyncio
    async def test_non_ai_delegate_connections_unaffected(self) -> None:
        """Principals without delegate lifecycle state are skipped entirely."""
        ws_viewer = MagicMock()
        ws_viewer.send_text = AsyncMock()
        viewer = AuthPrincipal(
            username="viewer",
            role=UserRole.VIEWER,
            user_public_id="user-viewer",
            operator_public_ids=["op-1"],
        )
        cm = _mock_connection_manager({ws_viewer: {"signals.kraken.BTC-USD.live"}})
        bridge = _mock_bridge()
        repo = _repo_with_pairs(set())
        manager = _make_manager(
            connection_manager=cm,
            zmq_bridge=bridge,
            repository_factory=lambda: repo,
        )
        manager.authenticated_connections[ws_viewer] = viewer

        await manager._handle_scope_revoked(_make_event())

        cm.unsubscribe_client.assert_not_called()
        bridge.remove_subscription.assert_not_awaited()
        ws_viewer.send_text.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unrelated_operators_unaffected(self) -> None:
        """Delegate for operator-B is not notified when operator-A has a revoke."""
        ws = MagicMock()
        ws.send_text = AsyncMock()
        principal = _delegate_principal(operators=("op-other",))
        cm = _mock_connection_manager({ws: {"signals.kraken.BTC-USD.live"}})
        bridge = _mock_bridge()
        repo = _repo_with_pairs(set())
        manager = _make_manager(
            connection_manager=cm,
            zmq_bridge=bridge,
            repository_factory=lambda: repo,
        )
        manager.authenticated_connections[ws] = principal

        await manager._handle_scope_revoked(_make_event(operator_public_id="op-1"))

        cm.unsubscribe_client.assert_not_called()
        bridge.remove_subscription.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_wallet_scoped_subscriptions_is_noop(self) -> None:
        """Connection holding only market + system subs → nothing to revoke."""
        ws = MagicMock()
        ws.send_text = AsyncMock()
        principal = _delegate_principal()
        cm = _mock_connection_manager(
            {
                ws: {
                    "market.kraken.BTC-USD.ticks",
                    "system.heartbeats.strategy.my",
                }
            }
        )
        bridge = _mock_bridge()
        repo = _repo_with_pairs(set())
        manager = _make_manager(
            connection_manager=cm,
            zmq_bridge=bridge,
            repository_factory=lambda: repo,
        )
        manager.authenticated_connections[ws] = principal

        await manager._handle_scope_revoked(_make_event())

        cm.unsubscribe_client.assert_not_called()
        bridge.remove_subscription.assert_not_awaited()
        ws.send_text.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_ws_send_text_failure_swallowed(self) -> None:
        """A broken WS client's error frame must not block the unsubscribe walk.

        The error-frame notification is best-effort; a dead WS is cleaned
        up elsewhere. The unsubscribe + bridge-removal happens regardless.
        """
        ws = MagicMock()
        ws.send_text = AsyncMock(side_effect=RuntimeError("socket closed"))
        principal = _delegate_principal()
        cm = _mock_connection_manager({ws: {"signals.kraken.BTC-USD.live"}})
        bridge = _mock_bridge()
        repo = _repo_with_pairs(set())
        manager = _make_manager(
            connection_manager=cm,
            zmq_bridge=bridge,
            repository_factory=lambda: repo,
        )
        manager.authenticated_connections[ws] = principal

        await manager._handle_scope_revoked(_make_event())

        cm.unsubscribe_client.assert_called_once_with(ws, "signals.kraken.BTC-USD.live")
        bridge.remove_subscription.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_admin_dispatch_routes_scope_revoked_topic(self) -> None:
        """The listener's dispatch branch decodes the payload and calls the handler.

        Rather than exercise the full ZMQ loop, we hit
        ``_admin_dispatch_frame`` directly with the serialised payload
        to verify the dispatch branch wires ScopeRevokedData
        → _handle_scope_revoked cleanly without affecting the
        user-deactivated branch.
        """
        manager = _make_manager()
        manager._handle_scope_revoked = AsyncMock()
        event = _make_event()
        await manager._admin_dispatch_frame("admin.scope_revoked", event.model_dump_json())
        manager._handle_scope_revoked.assert_awaited_once()
        await_args = manager._handle_scope_revoked.await_args
        assert await_args is not None
        dispatched = await_args.args[0]
        assert dispatched.grant_public_id == event.grant_public_id
        assert dispatched.operator_public_id == event.operator_public_id

    @pytest.mark.asyncio
    async def test_admin_dispatch_swallows_handler_exception(self) -> None:
        """A handler raising on a bad frame must not kill the listener loop.

        ``_admin_dispatch_frame`` already logs-and-swallows via the
        outer try/except; this test pins that the same behaviour covers
        the new scope_revoked branch.
        """
        manager = _make_manager()
        manager._handle_scope_revoked = AsyncMock(side_effect=RuntimeError("boom"))
        event = _make_event()
        await manager._admin_dispatch_frame("admin.scope_revoked", event.model_dump_json())
        manager._handle_scope_revoked.assert_awaited_once()


def _make_granted_event(
    *,
    operator_public_id: str = "op-1",
) -> ScopeGrantedData:
    """Build a canonical ``admin.scope_granted`` event payload."""
    now = datetime.now(UTC)
    return ScopeGrantedData(
        public_id="evt-sg-1",
        timestamp=now,
        session_id="test-sid",
        sequence_id=1,
        grant_public_id="grant-new",
        operator_public_id=operator_public_id,
        wallet_public_id="wal-1",
        scope_kind="instrument",
        instrument_public_id="inst-BTC-USD",
        granted_at=now,
        granted_by_user_public_id="user-admin",
        reason="onboarding",
    )


def _make_handed_over_event(
    *,
    from_operator: str = "op-from",
    to_operator: str = "op-to",
) -> ScopeHandedOverData:
    """Build a canonical ``admin.scope_handed_over`` event payload."""
    now = datetime.now(UTC)
    return ScopeHandedOverData(
        public_id="evt-sh-1",
        timestamp=now,
        session_id="test-sid",
        sequence_id=1,
        grant_public_id="grant-new",
        from_operator_public_id=from_operator,
        to_operator_public_id=to_operator,
        wallet_public_id="wal-1",
        scope_kind="instrument",
        instrument_public_id="inst-BTC-USD",
        handover_at=now,
        handover_by_user_public_id="user-admin",
        reason="rebalance",
    )


class TestHandleScopeGranted:
    """Tests for ``WebSocketAuthManager._handle_scope_granted``."""

    @pytest.mark.asyncio
    async def test_missing_wiring_logs_warning_and_noops(self) -> None:
        """Without ``set_wiring``, the granted handler logs + returns without raising."""
        manager = _make_manager()
        await manager._handle_scope_granted(_make_granted_event())

    @pytest.mark.asyncio
    async def test_revalidates_delegate_subscriptions_for_affected_operator(self) -> None:
        """A grant event walks the delegate's subscriptions defensively.

        A new grant cannot revoke anything, but the revalidation runs
        anyway as a safety-net: if a prior event was lost, the WS state
        reconciles against the post-insert DB snapshot. With allowed_pairs
        covering BTC-USD, no subscriptions are dropped.
        """
        ws = MagicMock()
        ws.send_text = AsyncMock()
        principal = _delegate_principal()
        cm = _mock_connection_manager({ws: {"signals.kraken.BTC-USD.live"}})
        bridge = _mock_bridge()
        repo = _repo_with_pairs({("kraken", "BTC-USD")})
        manager = _make_manager(
            connection_manager=cm,
            zmq_bridge=bridge,
            repository_factory=lambda: repo,
        )
        manager.authenticated_connections[ws] = principal

        await manager._handle_scope_granted(_make_granted_event())

        cm.unsubscribe_client.assert_not_called()
        bridge.remove_subscription.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_admin_dispatch_routes_scope_granted_topic(self) -> None:
        """The listener's dispatch branch routes ``admin.scope_granted`` cleanly."""
        manager = _make_manager()
        manager._handle_scope_granted = AsyncMock()
        event = _make_granted_event()
        await manager._admin_dispatch_frame("admin.scope_granted", event.model_dump_json())
        manager._handle_scope_granted.assert_awaited_once()


class TestHandleScopeHandedOver:
    """Tests for ``WebSocketAuthManager._handle_scope_handed_over``."""

    @pytest.mark.asyncio
    async def test_missing_wiring_logs_warning_and_noops(self) -> None:
        """Without ``set_wiring``, the handover handler logs + returns without raising."""
        manager = _make_manager()
        await manager._handle_scope_handed_over(_make_handed_over_event())

    @pytest.mark.asyncio
    async def test_walks_delegates_of_both_operators(self) -> None:
        """A handover narrows the from-operator delegate's subscriptions.

        Given: from-side delegate subscribed to BTC-USD, dest-side
            delegate subscribed to ETH-USD, post-handover pair set
            covers only ETH-USD,
        When: handler fires,
        Then: from-side BTC-USD subscription is dropped; to-side ETH-USD
            stays (pair still covers it).
        """
        ws_from = MagicMock()
        ws_from.send_text = AsyncMock()
        ws_to = MagicMock()
        ws_to.send_text = AsyncMock()
        from_principal = _delegate_principal(user_public_id="user-from", operators=("op-from",))
        to_principal = _delegate_principal(user_public_id="user-to", operators=("op-to",))
        cm = _mock_connection_manager(
            {
                ws_from: {"signals.kraken.BTC-USD.live"},
                ws_to: {"signals.kraken.ETH-USD.live"},
            }
        )
        bridge = _mock_bridge()
        repo = _repo_with_pairs({("kraken", "ETH-USD")})
        manager = _make_manager(
            connection_manager=cm,
            zmq_bridge=bridge,
            repository_factory=lambda: repo,
        )
        manager.authenticated_connections[ws_from] = from_principal
        manager.authenticated_connections[ws_to] = to_principal

        await manager._handle_scope_handed_over(_make_handed_over_event())

        cm.unsubscribe_client.assert_called_once_with(ws_from, "signals.kraken.BTC-USD.live")

    @pytest.mark.asyncio
    async def test_shared_revalidation_processes_reviewer_and_skips_viewer(self) -> None:
        """Restrict shared scope-event fanout to delegate-backed principals.

        Given: A matching AI_REVIEWER and VIEWER subscribed to the same revoked pair,
        When: A handover invokes the shared operator revalidation helper,
        Then: Only the reviewer is revalidated and unsubscribed while the viewer is untouched.
        """
        topic = "signals.kraken.BTC-USD.live"
        ws_reviewer = MagicMock()
        ws_reviewer.send_text = AsyncMock()
        ws_viewer = MagicMock()
        ws_viewer.send_text = AsyncMock()
        reviewer = _delegate_principal(
            user_public_id="user-reviewer",
            operators=("op-from",),
            role=UserRole.AI_REVIEWER,
        )
        viewer = AuthPrincipal(
            username="viewer",
            role=UserRole.VIEWER,
            user_public_id="user-viewer",
            operator_public_ids=["op-from"],
        )
        cm = _mock_connection_manager(
            {
                ws_reviewer: {topic},
                ws_viewer: {topic},
            }
        )
        bridge = _mock_bridge()
        repo = _repo_with_pairs(set())
        manager = _make_manager(
            connection_manager=cm,
            zmq_bridge=bridge,
            repository_factory=lambda: repo,
        )
        manager.authenticated_connections[ws_reviewer] = reviewer
        manager.authenticated_connections[ws_viewer] = viewer

        await manager._handle_scope_handed_over(_make_handed_over_event())

        cm.unsubscribe_client.assert_called_once_with(ws_reviewer, topic)
        bridge.remove_subscription.assert_awaited_once_with(ws_reviewer, [topic])
        ws_reviewer.send_text.assert_awaited_once()
        ws_viewer.send_text.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_admin_dispatch_routes_scope_handed_over_topic(self) -> None:
        """The listener's dispatch branch routes ``admin.scope_handed_over`` cleanly."""
        manager = _make_manager()
        manager._handle_scope_handed_over = AsyncMock()
        event = _make_handed_over_event()
        await manager._admin_dispatch_frame("admin.scope_handed_over", event.model_dump_json())
        manager._handle_scope_handed_over.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_revalidate_helper_skips_unrelated_delegate_operators(self) -> None:
        """``_revalidate_delegates_for_operators`` skips delegates not overlapping affected ops."""
        ws = MagicMock()
        ws.send_text = AsyncMock()
        principal = _delegate_principal(operators=("op-other",))
        cm = _mock_connection_manager({ws: {"signals.kraken.BTC-USD.live"}})
        bridge = _mock_bridge()
        repo = _repo_with_pairs(set())
        manager = _make_manager(
            connection_manager=cm,
            zmq_bridge=bridge,
            repository_factory=lambda: repo,
        )
        manager.authenticated_connections[ws] = principal

        await manager._handle_scope_handed_over(_make_handed_over_event())

        cm.unsubscribe_client.assert_not_called()

    @pytest.mark.asyncio
    async def test_revalidate_helper_skips_non_delegate_connections(self) -> None:
        """The revalidation helper skips principals without delegate state."""
        ws_viewer = MagicMock()
        ws_viewer.send_text = AsyncMock()
        viewer = AuthPrincipal(
            username="viewer",
            role=UserRole.VIEWER,
            user_public_id="user-viewer",
            operator_public_ids=["op-from"],
        )
        cm = _mock_connection_manager({ws_viewer: {"signals.kraken.BTC-USD.live"}})
        bridge = _mock_bridge()
        repo = _repo_with_pairs(set())
        manager = _make_manager(
            connection_manager=cm,
            zmq_bridge=bridge,
            repository_factory=lambda: repo,
        )
        manager.authenticated_connections[ws_viewer] = viewer

        await manager._handle_scope_handed_over(_make_handed_over_event())

        cm.unsubscribe_client.assert_not_called()
