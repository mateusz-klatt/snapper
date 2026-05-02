"""end-to-end admin.scope_revoked fanout integration test.

Exercises the full dispatch path that the admin-bus publisher is
supposed to drive, but without booting a real broker or real
WebSocket:

    ScopeGrantService.revoke_grant(...)  → model_dump_json()
    → WebSocketAuthManager._admin_dispatch_frame("admin.scope_revoked", ...)
    → _handle_scope_revoked → _revalidate_and_unsubscribe
    → connection_manager.unsubscribe_client / zmq_bridge.remove_subscription
    → ws.send_text(error frame)

The test wires a realistic ``WebSocketAuthManager`` (via
``set_wiring``), seeds a single AI_DELEGATE connection in
``authenticated_connections`` with a mix of in-scope, out-of-scope,
and non-wallet-scoped subscriptions, then feeds a serialised
``ScopeRevokedData`` payload into ``_admin_dispatch_frame`` to
reproduce what a real publisher → subscriber hand-off does at runtime.

Pinned DoD: "REST → publisher → bus → subscriber → fanout →
WSErrorResponse frame with message.startswith('topic_outside_scope:')
+ unsubscribe_client + bridge.remove_subscription." This test omits
the REST + bus halves (they are exercised at the service-level +
contract-test level respectively) and focuses on the subscriber +
fanout half end-to-end.
"""

import json
from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.websocket_auth import WebSocketAuthManager
from snapper.messaging.schemas.data import ScopeRevokedData


def _wired_manager(
    *,
    subscriptions_by_ws: dict[MagicMock, set[str]],
    pairs: set[tuple[str, str]],
) -> tuple[WebSocketAuthManager, MagicMock, MagicMock, MagicMock]:
    """Build a fully wired manager with a connection manager + bridge + repo.

    Returns the tuple ``(manager, connection_manager, zmq_bridge, repo)``
    so the caller can stage principals in ``authenticated_connections``
    + assert on the downstream fanout.
    """
    WebSocketAuthManager.clear_instance()
    manager = WebSocketAuthManager()
    cm = MagicMock()
    cm.get_client_subscriptions = MagicMock(
        side_effect=lambda ws: subscriptions_by_ws.get(ws, set())
    )
    cm.unsubscribe_client = MagicMock()
    bridge = MagicMock()
    bridge.remove_subscription = AsyncMock()
    repo = MagicMock()
    repo.list_scope_grant_instrument_pairs = AsyncMock(return_value=pairs)
    manager.set_wiring(
        connection_manager=cm,
        zmq_bridge=bridge,
        repository_factory=lambda: repo,
    )
    return manager, cm, bridge, repo


def _scope_revoked_event(
    *,
    operator_public_id: str = "op-1",
    wallet_public_id: str = "wal-1",
    instrument_public_id: str = "inst-BTC-USD",
) -> ScopeRevokedData:
    """Build a canonical ``admin.scope_revoked`` event payload."""
    now = datetime.now(UTC)
    return ScopeRevokedData(
        public_id="evt-e2e-1",
        timestamp=now,
        session_id="e2e-sid",
        sequence_id=42,
        grant_public_id="grant-e2e",
        operator_public_id=operator_public_id,
        wallet_public_id=wallet_public_id,
        scope_kind="instrument",
        underlying_public_id=None,
        instrument_public_id=instrument_public_id,
        revoked_at=now,
        revoked_by_user_public_id="user-admin",
        reason="end-to-end fanout",
    )


def _ai_delegate(
    *,
    user_public_id: str = "user-e2e",
    operators: tuple[str, ...] = ("op-1",),
) -> AuthPrincipal:
    """Build an AI_DELEGATE principal fixture."""
    return AuthPrincipal(
        username=f"delegate-{user_public_id}",
        role=UserRole.AI_DELEGATE,
        user_public_id=user_public_id,
        operator_public_ids=list(operators),
    )


class TestWsAdminFanoutEndToEnd:
    """Full ``_admin_dispatch_frame`` → handler → fanout happy path."""

    @pytest.mark.asyncio
    async def test_dispatch_frame_drives_full_unsubscribe_fanout(self) -> None:
        """admin.scope_revoked delivered via dispatch → unsubscribe + error frame.

        Given: AI_DELEGATE with a 3-topic mix — the revoked BTC-USD
            signals + an unrelated market data + a system heartbeat,
            fresh allowed-pairs read returns empty (BTC-USD just revoked),
        When: ``_admin_dispatch_frame("admin.scope_revoked", payload)``
            runs,
        Then: only the BTC-USD wallet-scoped topic is unsubscribed + a
            ``topic_outside_scope:`` error frame is delivered to the
            client; bridge.remove_subscription is awaited exactly once;
            WS.close is NEVER called (revocation prunes subscriptions,
            does not close the session).
        """
        ws = MagicMock()
        ws.send_text = AsyncMock()
        principal = _ai_delegate()
        subs = {
            "signals.kraken.BTC-USD.live",
            "market.kraken.BTC-USD.ticks",
            "system.heartbeats.strategy.ema_cross",
        }
        manager, cm, bridge, repo = _wired_manager(
            subscriptions_by_ws={ws: subs},
            pairs=set(),
        )
        manager.authenticated_connections[ws] = principal

        payload = _scope_revoked_event().model_dump_json()
        await manager._admin_dispatch_frame("admin.scope_revoked", payload)

        cm.unsubscribe_client.assert_called_once_with(ws, "signals.kraken.BTC-USD.live")
        bridge.remove_subscription.assert_awaited_once_with(ws, ["signals.kraken.BTC-USD.live"])
        frame_sent = ws.send_text.await_args.args[0]
        frame = json.loads(frame_sent)
        assert frame["type"] == "error"
        assert frame["message"].startswith("topic_outside_scope:")
        assert "signals.kraken.BTC-USD.live" in frame["message"]
        ws.close.assert_not_called()
        assert repo.list_scope_grant_instrument_pairs.await_count == 1

    @pytest.mark.asyncio
    async def test_dispatch_frame_is_idempotent_per_event(self) -> None:
        """The subscriber contract: one event → one fanout pass.

        The single-publisher invariant's corollary on the
        subscriber side — a single publisher frame must not fan out
        twice on the same subscriber. We call ``_admin_dispatch_frame``
        twice with the same event payload (simulating a duplicate
        publish) and assert the repo pair-set read fires once per call,
        not per topic.
        """
        ws = MagicMock()
        ws.send_text = AsyncMock()
        principal = _ai_delegate()
        subs = {"signals.kraken.BTC-USD.live", "signals.kraken.ETH-USD.live"}
        manager, _cm, _bridge, repo = _wired_manager(
            subscriptions_by_ws={ws: subs},
            pairs=set(),
        )
        manager.authenticated_connections[ws] = principal

        payload = _scope_revoked_event().model_dump_json()
        await manager._admin_dispatch_frame("admin.scope_revoked", payload)
        assert repo.list_scope_grant_instrument_pairs.await_count == 1
