"""Tests for ping-driven AI-delegate liveness refreshes.

Covers :meth:`WebSocketAuthManager.on_client_ping` — the periodic
``ai_delegates.last_seen_at`` re-bump that keeps a connected delegate
inside the AI-review admission heartbeat window between the connect-time
bump (``on_authenticate``) and disconnect. The refresh is throttled per
delegate and fail-soft: a failed DB write clears the throttle stamp so
the next ping retries, and the ping/pong exchange is never disturbed.
"""

from collections.abc import Iterator
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.websocket_auth import DELEGATE_LIVENESS_BUMP_MIN_INTERVAL_SECONDS
from snapper.auth.websocket_auth import WebSocketAuthManager


@pytest.fixture(autouse=True)
def _clear_singleton() -> Iterator[None]:
    """Reset the WebSocketAuthManager singleton between cases."""
    WebSocketAuthManager.clear_instance()
    yield
    WebSocketAuthManager.clear_instance()


def _delegate_principal(delegate_public_id: str | None = "del-1") -> AuthPrincipal:
    """Build an AI_DELEGATE principal carrying ``delegate_public_id``."""
    return AuthPrincipal(
        username="delegate-x",
        role=UserRole.AI_DELEGATE,
        user_public_id="user-1",
        operator_public_ids=["op-1"],
        delegate_public_id=delegate_public_id,
    )


def _viewer_principal() -> AuthPrincipal:
    """Build a non-delegate principal (``delegate_public_id`` defaults to None)."""
    return AuthPrincipal(
        username="viewer-x",
        role=UserRole.VIEWER,
        user_public_id="user-2",
        operator_public_ids=["op-1"],
    )


def _manager_with_repo(repo: Any) -> WebSocketAuthManager:
    """Build a manager whose repository factory returns the given repo."""
    manager = WebSocketAuthManager()
    manager.set_wiring(None, None, lambda: repo)
    return manager


@pytest.mark.asyncio
async def test_delegate_ping_bumps_last_seen() -> None:
    """Verify a delegate ping writes a fresh ``last_seen_at``.

    Given: A wired manager and a delegate principal,
    When: on_client_ping runs,
    Then: update_delegate_last_seen is awaited for the delegate id.
    """
    repo = MagicMock(update_delegate_last_seen=AsyncMock())
    manager = _manager_with_repo(repo)
    await manager.on_client_ping(_delegate_principal())
    repo.update_delegate_last_seen.assert_awaited_once()
    assert repo.update_delegate_last_seen.await_args.args[0] == "del-1"


@pytest.mark.asyncio
async def test_non_delegate_ping_is_noop() -> None:
    """Verify a non-delegate principal short-circuits.

    Given: A wired manager and a viewer principal,
    When: on_client_ping runs,
    Then: No repository write happens.
    """
    repo = MagicMock(update_delegate_last_seen=AsyncMock())
    manager = _manager_with_repo(repo)
    await manager.on_client_ping(_viewer_principal())
    repo.update_delegate_last_seen.assert_not_awaited()


@pytest.mark.asyncio
async def test_second_ping_inside_throttle_window_skips() -> None:
    """Verify the per-delegate throttle caps the DB write rate.

    Given: Two pings arriving within the throttle interval,
    When: Both run through on_client_ping,
    Then: Only the first writes ``last_seen_at``.
    """
    repo = MagicMock(update_delegate_last_seen=AsyncMock())
    manager = _manager_with_repo(repo)
    principal = _delegate_principal()
    await manager.on_client_ping(principal)
    await manager.on_client_ping(principal)
    repo.update_delegate_last_seen.assert_awaited_once()


@pytest.mark.asyncio
async def test_ping_after_throttle_window_bumps_again(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify a ping after the throttle window refreshes again.

    Given: A prior bump older than the throttle interval,
    When: on_client_ping runs again,
    Then: A second ``last_seen_at`` write happens.
    """
    repo = MagicMock(update_delegate_last_seen=AsyncMock())
    manager = _manager_with_repo(repo)
    principal = _delegate_principal()
    await manager.on_client_ping(principal)
    manager._delegate_last_liveness_bump["del-1"] -= (
        DELEGATE_LIVENESS_BUMP_MIN_INTERVAL_SECONDS + 1.0
    )
    await manager.on_client_ping(principal)
    assert repo.update_delegate_last_seen.await_count == 2


@pytest.mark.asyncio
async def test_missing_repository_factory_warns_and_skips() -> None:
    """Verify a manager without wiring degrades to a logged skip.

    Given: A manager whose repository_factory is None,
    When: on_client_ping runs for a delegate,
    Then: No exception propagates and no throttle stamp is recorded.
    """
    manager = WebSocketAuthManager()
    manager.set_wiring(None, None, None)
    await manager.on_client_ping(_delegate_principal())
    assert "del-1" not in manager._delegate_last_liveness_bump


@pytest.mark.asyncio
async def test_on_authenticate_failed_bump_is_fail_soft() -> None:
    """Verify a connect-time bump failure never aborts the connection flow.

    Given: A repository whose update raises at authenticate time,
    When: on_authenticate runs for a delegate,
    Then: Nothing propagates (the ping path owns the retry).
    """
    repo = MagicMock(update_delegate_last_seen=AsyncMock(side_effect=RuntimeError("db down")))
    manager = _manager_with_repo(repo)
    await manager.on_authenticate(MagicMock(), _delegate_principal())
    repo.update_delegate_last_seen.assert_awaited_once()


@pytest.mark.asyncio
async def test_failed_bump_clears_throttle_stamp_for_retry() -> None:
    """Verify a failed DB write clears the stamp so the next ping retries.

    Given: A repository whose update raises,
    When: on_client_ping runs twice,
    Then: Both pings attempt the write (no throttle after failure) and
        nothing propagates.
    """
    repo = MagicMock(update_delegate_last_seen=AsyncMock(side_effect=RuntimeError("db down")))
    manager = _manager_with_repo(repo)
    principal = _delegate_principal()
    await manager.on_client_ping(principal)
    assert "del-1" not in manager._delegate_last_liveness_bump
    await manager.on_client_ping(principal)
    assert repo.update_delegate_last_seen.await_count == 2
