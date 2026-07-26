"""Tests for delegate-liveness hook wiring in the WS endpoint flow.

Covers the endpoint side of plan P1's GAP-2 fix:
``_authenticate_and_dispatch`` calls
:meth:`WebSocketAuthManager.on_authenticate` right after a successful
authenticate (cancelling any pending offline publish + bumping
``ai_delegates.last_seen_at``) and pairs it with
:meth:`WebSocketAuthManager.on_disconnect` when the dispatch loop exits
— including when the dispatch loop raises.
"""

from types import SimpleNamespace
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from fastapi import WebSocket

from snapper.interface.websocket.connection_manager import WebSocketConnectionManager
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.authenticated_websocket import _authenticate_and_dispatch


class ManagerStub:
    """Connection manager stub recording connects."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.connected: list[Any] = []
        self._tracker = SequenceTracker()

    @property
    def tracker(self) -> SequenceTracker:
        """Provide sequence tracker for provenance stamping."""
        return self._tracker

    async def connect(self, websocket: Any, accept: bool = True) -> None:
        """Record WebSocket connection."""
        self.connected.append(websocket)


def _wire_flow(
    monkeypatch: pytest.MonkeyPatch,
    *,
    auth_result: Any,
    dispatch: AsyncMock,
) -> None:
    """Patch the endpoint module's authenticate/send/dispatch seams."""

    async def fake_authenticate(*args: Any, **kwargs: Any) -> Any:
        return auth_result

    monkeypatch.setattr(
        "snapper.server.authenticated_websocket.authenticate_websocket", fake_authenticate
    )
    monkeypatch.setattr("snapper.server.authenticated_websocket.send_auth_complete", AsyncMock())
    monkeypatch.setattr("snapper.server.authenticated_websocket.dispatch_messages", dispatch)


def _auth_manager(current_principal: Any = None) -> MagicMock:
    """Build an auth-manager mock with recordable lifecycle hooks.

    ``current_principal`` is what the registry hands back when the endpoint
    resolves the connection's live principal for the disconnect hook. The
    default of ``None`` models an entry already removed, which is the case
    the fallback to the initial principal exists for.
    """
    return MagicMock(
        on_authenticate=AsyncMock(),
        on_disconnect=AsyncMock(),
        get_authenticated_user=MagicMock(return_value=current_principal),
    )


@pytest.mark.asyncio
async def test_success_pairs_authenticate_and_disconnect_hooks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify a successful flow brackets dispatch with both hooks.

    Given: A successful authenticate result,
    When: _authenticate_and_dispatch completes its dispatch loop,
    Then: on_authenticate fires before dispatch and on_disconnect after.
    """
    user = object()
    auth_result = SimpleNamespace(success=True, user=user, ws_payload=None)
    order: list[str] = []
    dispatch = AsyncMock(side_effect=lambda *a, **k: order.append("dispatch"))
    _wire_flow(monkeypatch, auth_result=auth_result, dispatch=dispatch)
    ws_auth_manager = _auth_manager()
    ws_auth_manager.on_authenticate.side_effect = lambda *a: order.append("on_authenticate")
    ws_auth_manager.on_disconnect.side_effect = lambda *a: order.append("on_disconnect")
    websocket = cast(WebSocket, object())
    state = [False]
    await _authenticate_and_dispatch(
        websocket,
        cast(WebSocketConnectionManager, ManagerStub()),
        ws_auth_manager,
        MagicMock(),
        MagicMock(),
        state,
    )
    assert state == [True]
    assert order == ["on_authenticate", "dispatch", "on_disconnect"]
    ws_auth_manager.on_authenticate.assert_awaited_once_with(websocket, user)
    ws_auth_manager.on_disconnect.assert_awaited_once_with(websocket, user)


@pytest.mark.asyncio
async def test_disconnect_hook_uses_the_replaced_principal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify the disconnect hook fires with the CURRENT principal.

    Given: A connection authenticated as one principal whose registry entry
        has since been replaced by in-place re-authentication,
    When: _authenticate_and_dispatch reaches its disconnect hook,
    Then: The hook receives the replacement, not the principal captured when
        the connection opened.

    Without this, an authority reduction applied mid-session would still be
    reported under the pre-reduction identity.
    """
    initial = object()
    replacement = object()
    auth_result = SimpleNamespace(success=True, user=initial, ws_payload=None)
    _wire_flow(monkeypatch, auth_result=auth_result, dispatch=AsyncMock())
    ws_auth_manager = _auth_manager(replacement)
    websocket = cast(WebSocket, object())
    await _authenticate_and_dispatch(
        websocket,
        cast(WebSocketConnectionManager, ManagerStub()),
        ws_auth_manager,
        MagicMock(),
        MagicMock(),
        [False],
    )
    ws_auth_manager.on_authenticate.assert_awaited_once_with(websocket, initial)
    ws_auth_manager.on_disconnect.assert_awaited_once_with(websocket, replacement)


@pytest.mark.asyncio
async def test_dispatch_failure_still_fires_disconnect_hook(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify on_disconnect fires even when the dispatch loop raises.

    Given: A dispatch loop raising RuntimeError,
    When: _authenticate_and_dispatch runs,
    Then: The error propagates AND on_disconnect still fires.
    """
    user = object()
    auth_result = SimpleNamespace(success=True, user=user, ws_payload=None)
    dispatch = AsyncMock(side_effect=RuntimeError("loop crashed"))
    _wire_flow(monkeypatch, auth_result=auth_result, dispatch=dispatch)
    ws_auth_manager = _auth_manager()
    websocket = cast(WebSocket, object())
    with pytest.raises(RuntimeError, match="loop crashed"):
        await _authenticate_and_dispatch(
            websocket,
            cast(WebSocketConnectionManager, ManagerStub()),
            ws_auth_manager,
            MagicMock(),
            MagicMock(),
            [False],
        )
    ws_auth_manager.on_disconnect.assert_awaited_once_with(websocket, user)


@pytest.mark.asyncio
async def test_auth_complete_failure_still_fires_disconnect_hook(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify on_disconnect pairs even when the auth-complete send raises.

    Given: A ws_payload whose auth-complete send raises,
    When: _authenticate_and_dispatch runs,
    Then: The error propagates, dispatch never runs, and on_disconnect
        still fires.
    """
    user = object()
    auth_result = SimpleNamespace(success=True, user=user, ws_payload={"ok": True})
    dispatch = AsyncMock()
    _wire_flow(monkeypatch, auth_result=auth_result, dispatch=dispatch)
    monkeypatch.setattr(
        "snapper.server.authenticated_websocket.send_auth_complete",
        AsyncMock(side_effect=RuntimeError("send failed")),
    )
    ws_auth_manager = _auth_manager()
    websocket = cast(WebSocket, object())
    with pytest.raises(RuntimeError, match="send failed"):
        await _authenticate_and_dispatch(
            websocket,
            cast(WebSocketConnectionManager, ManagerStub()),
            ws_auth_manager,
            MagicMock(),
            MagicMock(),
            [False],
        )
    dispatch.assert_not_awaited()
    ws_auth_manager.on_disconnect.assert_awaited_once_with(websocket, user)


@pytest.mark.asyncio
async def test_failed_authentication_skips_both_hooks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify a failed authenticate never touches the lifecycle hooks.

    Given: A failed authenticate result,
    When: _authenticate_and_dispatch returns early,
    Then: Neither hook fires and dispatch never runs.
    """
    auth_result = SimpleNamespace(success=False, user=None, ws_payload=None)
    dispatch = AsyncMock()
    _wire_flow(monkeypatch, auth_result=auth_result, dispatch=dispatch)
    ws_auth_manager = _auth_manager()
    await _authenticate_and_dispatch(
        cast(WebSocket, object()),
        cast(WebSocketConnectionManager, ManagerStub()),
        ws_auth_manager,
        MagicMock(),
        MagicMock(),
        [False],
    )
    ws_auth_manager.on_authenticate.assert_not_awaited()
    ws_auth_manager.on_disconnect.assert_not_awaited()
    dispatch.assert_not_awaited()
