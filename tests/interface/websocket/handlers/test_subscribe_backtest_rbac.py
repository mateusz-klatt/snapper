"""Backtest WS subscribe RBAC matrix (plan §2.1.2).

Authoritative prefix-RBAC table:

====== ============ ========================= ===============================
Role   backtest.    backtest.{own_wallet}.    backtest.{foreign_wallet}.*
====== ============ ========================= ===============================
VIEWER denied       accepted                  denied
OPER.  denied       accepted                  denied
ADMIN  accepted     accepted                  accepted
====== ============ ========================= ===============================

Wallet segment is drawn from segment 1 of the topic (UUID7-validated
by :func:`_validate_backtest_prefix`). Non-admin roles subscribing
to a wallet other than their ``active_wallet_public_id`` receive
``denied`` in the response envelope without the subscription ever
reaching the ZMQ bridge.
"""

import json
from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import PropertyMock

import pytest

from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.interface.websocket.handlers.subscribe import handle_subscribe
from snapper.interface.websocket.schemas import WSSubscribeRequest
from snapper.messaging.infrastructure.publisher import SequenceTracker

_OWN_WALLET = "01948f94-0001-7a00-8000-000000000001"
_FOREIGN_WALLET = "01948f94-0001-7a00-8000-0000000000ff"
_RUN = "01948f94-0001-7a00-8000-000000000002"


def _principal(role: UserRole, active_wallet: str | None = _OWN_WALLET) -> AuthPrincipal:
    """Build an AuthPrincipal fixture."""
    return AuthPrincipal(username="u", role=role, active_wallet_public_id=active_wallet)


def _mock_manager() -> MagicMock:
    """Build a mock connection manager shared across tests."""
    manager = MagicMock()
    manager.get_client_subscriptions = MagicMock(return_value=set())
    manager.subscribe_client = MagicMock()
    manager.zmq_bridge = MagicMock()
    manager.zmq_bridge.add_subscription = AsyncMock()
    type(manager).tracker = PropertyMock(return_value=SequenceTracker())
    return manager


def _msg(topics: list[str]) -> WSSubscribeRequest:
    """Build a subscribe request envelope."""
    return WSSubscribeRequest(
        public_id="test-pid",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        session_id="",
        sequence_id=0,
        topics=topics,
    )


@pytest.mark.asyncio
async def test_viewer_denied_foreign_wallet_run_subscription() -> None:
    """VIEWER subscribing to a foreign wallet's run prefix is denied."""
    ws = AsyncMock()
    manager = _mock_manager()
    prefix = f"backtest.{_FOREIGN_WALLET}.{_RUN}."
    await handle_subscribe(ws, _msg([prefix]), manager, _principal(UserRole.VIEWER))
    response = json.loads(ws.send_text.call_args[0][0])
    assert response["status"] in {"denied", "partial"}
    assert prefix in response.get("denied_topics", [])
    manager.zmq_bridge.add_subscription.assert_not_called()


@pytest.mark.asyncio
async def test_viewer_denied_bare_backtest_root() -> None:
    """Bare ``backtest.`` is admin-only — VIEWER is denied."""
    ws = AsyncMock()
    manager = _mock_manager()
    await handle_subscribe(ws, _msg(["backtest."]), manager, _principal(UserRole.VIEWER))
    response = json.loads(ws.send_text.call_args[0][0])
    assert "backtest." in response.get("denied_topics", [])


@pytest.mark.asyncio
async def test_viewer_accepted_own_wallet_prefix() -> None:
    """VIEWER can subscribe to their own wallet's prefix."""
    ws = AsyncMock()
    manager = _mock_manager()
    prefix = f"backtest.{_OWN_WALLET}."
    await handle_subscribe(ws, _msg([prefix]), manager, _principal(UserRole.VIEWER))
    response = json.loads(ws.send_text.call_args[0][0])
    assert response["status"] in {"subscribed", "partial"}
    assert prefix in response.get("topics", [])


@pytest.mark.asyncio
async def test_operator_accepted_own_wallet_run_prefix() -> None:
    """OPERATOR can subscribe to their own wallet + run prefix."""
    ws = AsyncMock()
    manager = _mock_manager()
    prefix = f"backtest.{_OWN_WALLET}.{_RUN}."
    await handle_subscribe(ws, _msg([prefix]), manager, _principal(UserRole.OPERATOR))
    response = json.loads(ws.send_text.call_args[0][0])
    assert response["status"] in {"subscribed", "partial"}
    assert prefix in response.get("topics", [])


@pytest.mark.asyncio
async def test_admin_accepted_foreign_wallet_prefix() -> None:
    """ADMIN bypasses wallet scope and can subscribe to any wallet."""
    ws = AsyncMock()
    manager = _mock_manager()
    prefix = f"backtest.{_FOREIGN_WALLET}.{_RUN}."
    await handle_subscribe(
        ws, _msg([prefix]), manager, _principal(UserRole.ADMIN, active_wallet=_OWN_WALLET)
    )
    response = json.loads(ws.send_text.call_args[0][0])
    assert response["status"] in {"subscribed", "partial"}
    assert prefix in response.get("topics", [])


@pytest.mark.asyncio
async def test_admin_accepted_bare_backtest_root() -> None:
    """ADMIN can subscribe to the bare backtest root."""
    ws = AsyncMock()
    manager = _mock_manager()
    await handle_subscribe(ws, _msg(["backtest."]), manager, _principal(UserRole.ADMIN))
    response = json.loads(ws.send_text.call_args[0][0])
    assert "backtest." in response.get("topics", [])


@pytest.mark.asyncio
async def test_non_admin_without_active_wallet_denied() -> None:
    """VIEWER with ``active_wallet_public_id=None`` cannot subscribe to any wallet."""
    ws = AsyncMock()
    manager = _mock_manager()
    prefix = f"backtest.{_OWN_WALLET}."
    await handle_subscribe(
        ws, _msg([prefix]), manager, _principal(UserRole.VIEWER, active_wallet=None)
    )
    response = json.loads(ws.send_text.call_args[0][0])
    assert prefix in response.get("denied_topics", [])
