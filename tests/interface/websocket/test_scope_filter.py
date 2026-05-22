"""Tests for the per-frame ``ai_reviews.*`` scope filter.

Covers :func:`snapper.interface.websocket.handlers.subscribe.enforce_ai_review_scope`
end-to-end: pass-through for unrelated topics, drop on missing /
non-delegate principal, drop on malformed payload, and forwarding of
the scope-grant verdict from :class:`ScopeGrantService`.
"""

from datetime import UTC
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock

import pytest

from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.scope_grant_service import ScopeGrantService
from snapper.interface.websocket.scope_filter import enforce_ai_review_scope
from snapper.interface.websocket.scope_filter import enforce_alerts_scope
from snapper.interface.websocket.scope_filter import enforce_orders_events_scope


def _delegate_principal(
    delegate_public_id: str | None = "del-1",
) -> AuthPrincipal:
    """Build an AI_DELEGATE principal carrying a delegate row id."""
    return AuthPrincipal(
        username="delegate-x",
        role=UserRole.AI_DELEGATE,
        user_public_id="user-1",
        operator_public_ids=["op-1"],
        primary_operator_public_id="op-1",
        delegate_public_id=delegate_public_id,
    )


def _mock_scope_service(allow: bool) -> ScopeGrantService:
    """Stub ScopeGrantService whose ``has_grant_for_delegate`` returns ``allow``."""
    svc = ScopeGrantService.__new__(ScopeGrantService)
    svc.has_grant_for_delegate = AsyncMock(return_value=allow)
    return svc


def _payload(
    *,
    wallet_public_id: Any = "wal-A",
    instrument_public_id: Any = "inst-A",
) -> dict[str, Any]:
    """Default payload with the two scope-keyed fields the filter consults."""
    return {
        "wallet_public_id": wallet_public_id,
        "instrument_public_id": instrument_public_id,
    }


@pytest.fixture(autouse=True)
def _clear_scope_singleton() -> Any:
    """Reset ScopeGrantService singleton between tests."""
    ScopeGrantService.clear_instance()
    yield
    ScopeGrantService.clear_instance()


@pytest.mark.asyncio
async def test_passes_through_non_ai_review_topic() -> None:
    """Non-``ai_reviews.*`` topics short-circuit to True.

    The filter only owns the AI-review family. Other categories
    (market, signals, orders, etc.) have their own subscribe-time
    RBAC + wallet narrowing.

    Given any frame whose topic does not start with ``ai_reviews.``,
    When enforce_ai_review_scope evaluates,
    Then it returns True without touching the scope-grant service.
    """
    service = _mock_scope_service(allow=False)
    result = await enforce_ai_review_scope(
        topic="market.kraken.BTC-USD",
        connection_principal=_delegate_principal(),
        payload={"price": 50000},
        scope_grant_service=service,
    )
    assert result is True
    has_grant_mock: AsyncMock = service.has_grant_for_delegate
    has_grant_mock.assert_not_called()


@pytest.mark.asyncio
async def test_drops_when_principal_missing() -> None:
    """No principal -> drop the frame (defensive against pre-auth WS).

    Given an ``ai_reviews.*`` frame and a ``None`` principal,
    When the filter evaluates,
    Then it returns False without touching the scope-grant service.
    """
    service = _mock_scope_service(allow=True)
    result = await enforce_ai_review_scope(
        topic="ai_reviews.user-1.strat-1.request",
        connection_principal=None,
        payload=_payload(),
        scope_grant_service=service,
    )
    assert result is False
    has_grant_mock: AsyncMock = service.has_grant_for_delegate
    has_grant_mock.assert_not_called()


@pytest.mark.asyncio
async def test_drops_when_principal_lacks_delegate_id() -> None:
    """OPERATOR / VIEWER (no delegate row) -> drop the frame.

    The scope-grant check is delegate-keyed; a non-delegate principal
    that somehow subscribed cannot evaluate so we play it safe and
    drop.

    Given an OPERATOR principal with no ``delegate_public_id``,
    When the filter evaluates,
    Then it returns False.
    """
    service = _mock_scope_service(allow=True)
    operator = AuthPrincipal(
        username="op-1",
        role=UserRole.OPERATOR,
        user_public_id="op-user",
        operator_public_ids=["op-1"],
        primary_operator_public_id="op-1",
    )
    result = await enforce_ai_review_scope(
        topic="ai_reviews.user-1.strat-1.request",
        connection_principal=operator,
        payload=_payload(),
        scope_grant_service=service,
    )
    assert result is False
    has_grant_mock: AsyncMock = service.has_grant_for_delegate
    has_grant_mock.assert_not_called()


@pytest.mark.asyncio
async def test_drops_when_payload_missing_wallet_field() -> None:
    """Defensive — missing ``wallet_public_id`` -> drop.

    Given a payload without ``wallet_public_id``,
    When the filter evaluates,
    Then it returns False.
    """
    service = _mock_scope_service(allow=True)
    payload = {"instrument_public_id": "inst-A"}
    result = await enforce_ai_review_scope(
        topic="ai_reviews.user-1.strat-1.request",
        connection_principal=_delegate_principal(),
        payload=payload,
        scope_grant_service=service,
    )
    assert result is False


@pytest.mark.asyncio
async def test_drops_when_payload_missing_instrument_field() -> None:
    """Defensive — missing ``instrument_public_id`` -> drop.

    Given a payload without ``instrument_public_id``,
    When the filter evaluates,
    Then it returns False.
    """
    service = _mock_scope_service(allow=True)
    payload = {"wallet_public_id": "wal-A"}
    result = await enforce_ai_review_scope(
        topic="ai_reviews.user-1.strat-1.request",
        connection_principal=_delegate_principal(),
        payload=payload,
        scope_grant_service=service,
    )
    assert result is False


@pytest.mark.asyncio
async def test_drops_when_payload_field_is_non_string() -> None:
    """Defensive — non-string ``wallet_public_id`` (e.g. ``int``) -> drop.

    Given a payload where wallet_public_id is an int,
    When the filter evaluates,
    Then it returns False (the SCD2 query expects string keys).
    """
    service = _mock_scope_service(allow=True)
    result = await enforce_ai_review_scope(
        topic="ai_reviews.user-1.strat-1.request",
        connection_principal=_delegate_principal(),
        payload=_payload(wallet_public_id=42),
        scope_grant_service=service,
    )
    assert result is False


@pytest.mark.asyncio
async def test_forwards_scope_check_when_delegate_has_grant() -> None:
    """Delegate has grant -> filter returns True.

    Given a delegate with an active scope grant for (wallet, instrument),
    When the filter evaluates an ai_reviews frame,
    Then has_grant_for_delegate is called with the right arguments
    and the filter returns True.
    """
    service = _mock_scope_service(allow=True)
    result = await enforce_ai_review_scope(
        topic="ai_reviews.user-1.strat-1.request",
        connection_principal=_delegate_principal(),
        payload=_payload(),
        scope_grant_service=service,
    )
    assert result is True
    has_grant_mock: AsyncMock = service.has_grant_for_delegate
    has_grant_mock.assert_awaited_once()
    assert has_grant_mock.await_args is not None
    kwargs = has_grant_mock.await_args.kwargs
    assert kwargs["delegate_public_id"] == "del-1"
    assert kwargs["wallet_public_id"] == "wal-A"
    assert kwargs["instrument_public_id"] == "inst-A"


@pytest.mark.asyncio
async def test_drops_when_delegate_has_no_grant() -> None:
    """Delegate has no grant -> filter returns False.

    Given a delegate without scope coverage for the (wallet, instrument),
    When the filter evaluates,
    Then it returns False.
    """
    service = _mock_scope_service(allow=False)
    result = await enforce_ai_review_scope(
        topic="ai_reviews.user-2.strat-3.decision_ack",
        connection_principal=_delegate_principal(delegate_public_id="del-2"),
        payload=_payload(wallet_public_id="other-wallet"),
        scope_grant_service=service,
    )
    assert result is False


@pytest.mark.asyncio
async def test_uses_provided_as_of_for_scope_check() -> None:
    """Custom ``as_of`` reaches has_grant_for_delegate.

    Given a fixed wall-clock,
    When the filter evaluates with that as_of,
    Then the same value is forwarded to has_grant_for_delegate (used
    for SCD2-active filtering on memberships + grants).
    """
    service = _mock_scope_service(allow=True)
    fixed_now = datetime(2026, 4, 26, 12, 0, 0, tzinfo=UTC)
    await enforce_ai_review_scope(
        topic="ai_reviews.user-1.strat-1.request",
        connection_principal=_delegate_principal(),
        payload=_payload(),
        scope_grant_service=service,
        as_of=fixed_now,
    )
    has_grant_mock: AsyncMock = service.has_grant_for_delegate
    assert has_grant_mock.await_args is not None
    kwargs = has_grant_mock.await_args.kwargs
    assert kwargs["as_of"] == fixed_now


@pytest.mark.asyncio
async def test_default_as_of_uses_datetime_now_utc() -> None:
    """Omitted ``as_of`` defaults to ``datetime.now(UTC)``.

    Given the default ``as_of=None``,
    When the filter evaluates,
    Then the wall-clock forwarded to has_grant_for_delegate falls
    within ``[before, after]`` bounds.
    """
    service = _mock_scope_service(allow=True)
    before = datetime.now(UTC)
    await enforce_ai_review_scope(
        topic="ai_reviews.user-1.strat-1.request",
        connection_principal=_delegate_principal(),
        payload=_payload(),
        scope_grant_service=service,
    )
    after = datetime.now(UTC)
    has_grant_mock: AsyncMock = service.has_grant_for_delegate
    assert has_grant_mock.await_args is not None
    kwargs = has_grant_mock.await_args.kwargs
    assert before <= kwargs["as_of"] <= after


def _viewer_principal(
    *,
    operator_public_ids: list[str] | None = None,
    user_public_id: str = "user-viewer",
) -> AuthPrincipal:
    """Build a VIEWER principal exercising the trade_events scope filter.

    The wallet-set check uses :class:`ScopeGrantService`.
    """
    return AuthPrincipal(
        username="viewer-x",
        role=UserRole.VIEWER,
        user_public_id=user_public_id,
        operator_public_ids=operator_public_ids if operator_public_ids is not None else ["op-1"],
        primary_operator_public_id="op-1",
    )


def _admin_principal() -> AuthPrincipal:
    """Build an ADMIN principal — exercises the trade_events ADMIN bypass."""
    return AuthPrincipal(
        username="admin-x",
        role=UserRole.ADMIN,
        user_public_id="user-admin",
        operator_public_ids=["op-1"],
        primary_operator_public_id="op-1",
    )


def _mock_orders_events_service(accessible: set[str]) -> ScopeGrantService:
    """Stub ScopeGrantService returning ``accessible`` from the wallet query.

    The patched method is ``list_accessible_wallet_public_ids``.
    """
    svc = ScopeGrantService.__new__(ScopeGrantService)
    svc.list_accessible_wallet_public_ids = AsyncMock(return_value=accessible)
    return svc


@pytest.mark.asyncio
async def test_orders_events_passes_through_non_orders_events_topic() -> None:
    """Non-``orders.events.*`` topics short-circuit to True.

    Given: A topic outside the ``orders.events.*`` prefix,
    When: The orders.events scope filter is invoked,
    Then: It returns True without consulting the scope service.
    """
    service = _mock_orders_events_service(accessible={"wallet-A"})
    result = await enforce_orders_events_scope(
        topic="market.kraken.BTC-USD",
        connection_principal=_viewer_principal(),
        payload={"wallet_public_id": "wallet-A"},
        scope_grant_service=service,
    )
    assert result is True
    list_mock: AsyncMock = service.list_accessible_wallet_public_ids
    list_mock.assert_not_called()


@pytest.mark.asyncio
async def test_orders_events_drops_when_principal_missing() -> None:
    """Missing principal yields a fail-closed drop on orders.events frames.

    Given: An ``orders.events.*`` frame with no connection principal,
    When: The orders.events scope filter is invoked,
    Then: It returns False and never calls the scope service.
    """
    service = _mock_orders_events_service(accessible={"wallet-A"})
    result = await enforce_orders_events_scope(
        topic="orders.events.kraken.BTC-USD.executed",
        connection_principal=None,
        payload={"wallet_public_id": "wallet-A"},
        scope_grant_service=service,
    )
    assert result is False
    list_mock: AsyncMock = service.list_accessible_wallet_public_ids
    list_mock.assert_not_called()


@pytest.mark.asyncio
async def test_orders_events_admin_bypasses_scope_check() -> None:
    """ADMIN role bypasses the scope service for orders.events frames.

    Given: An ADMIN principal and an ``orders.events.*`` frame,
    When: The orders.events scope filter is invoked,
    Then: It returns True without consulting the scope service (REST parity).
    """
    service = _mock_orders_events_service(accessible=set())
    result = await enforce_orders_events_scope(
        topic="orders.events.kraken.BTC-USD.executed",
        connection_principal=_admin_principal(),
        payload={"wallet_public_id": "wallet-Z"},
        scope_grant_service=service,
    )
    assert result is True
    list_mock: AsyncMock = service.list_accessible_wallet_public_ids
    list_mock.assert_not_called()


@pytest.mark.asyncio
async def test_orders_events_drops_when_wallet_field_missing() -> None:
    """Missing ``wallet_public_id`` triggers a fail-closed drop.

    Given: An ``orders.events.*`` payload without ``wallet_public_id``,
    When: The orders.events scope filter is invoked,
    Then: It returns False (fail-closed against malformed frames).
    """
    service = _mock_orders_events_service(accessible={"wallet-A"})
    result = await enforce_orders_events_scope(
        topic="orders.events.kraken.BTC-USD.executed",
        connection_principal=_viewer_principal(),
        payload={"client_order_id": "o-1"},
        scope_grant_service=service,
    )
    assert result is False


@pytest.mark.asyncio
async def test_orders_events_drops_when_wallet_field_non_string() -> None:
    """Non-string ``wallet_public_id`` triggers a fail-closed drop.

    Given: An ``orders.events.*`` payload with a non-string ``wallet_public_id``,
    When: The orders.events scope filter is invoked,
    Then: It returns False (fail-closed against malformed frames).
    """
    service = _mock_orders_events_service(accessible={"wallet-A"})
    result = await enforce_orders_events_scope(
        topic="orders.events.kraken.BTC-USD.executed",
        connection_principal=_viewer_principal(),
        payload={"wallet_public_id": 12345},
        scope_grant_service=service,
    )
    assert result is False


@pytest.mark.asyncio
async def test_orders_events_forwards_when_wallet_in_accessible_set() -> None:
    """Frames whose wallet is accessible to the VIEWER are forwarded.

    Given: A VIEWER principal and a payload wallet in the accessible set,
    When: The orders.events scope filter is invoked,
    Then: It returns True (frame is forwarded to the WS client).
    """
    service = _mock_orders_events_service(accessible={"wallet-A", "wallet-B"})
    result = await enforce_orders_events_scope(
        topic="orders.events.kraken.BTC-USD.executed",
        connection_principal=_viewer_principal(),
        payload={"wallet_public_id": "wallet-A"},
        scope_grant_service=service,
    )
    assert result is True


@pytest.mark.asyncio
async def test_orders_events_drops_when_wallet_outside_accessible_set() -> None:
    """Frames whose wallet is not accessible to the VIEWER are dropped.

    Given: A VIEWER principal and a payload wallet outside the accessible set,
    When: The orders.events scope filter is invoked,
    Then: It returns False (cross-tenant data leak guard).
    """
    service = _mock_orders_events_service(accessible={"wallet-A"})
    result = await enforce_orders_events_scope(
        topic="orders.events.kraken.BTC-USD.executed",
        connection_principal=_viewer_principal(),
        payload={"wallet_public_id": "wallet-Z"},
        scope_grant_service=service,
    )
    assert result is False


@pytest.mark.asyncio
async def test_orders_events_uses_provided_as_of_for_scope_check() -> None:
    """Caller-supplied ``as_of`` propagates to the scope service unchanged.

    Given: A VIEWER frame with an explicit ``as_of`` timestamp,
    When: The orders.events scope filter is invoked,
    Then: The scope service receives that exact ``as_of`` value.
    """
    service = _mock_orders_events_service(accessible={"wallet-A"})
    pinned = datetime(2025, 6, 1, tzinfo=UTC)
    await enforce_orders_events_scope(
        topic="orders.events.kraken.BTC-USD.executed",
        connection_principal=_viewer_principal(),
        payload={"wallet_public_id": "wallet-A"},
        scope_grant_service=service,
        as_of=pinned,
    )
    list_mock: AsyncMock = service.list_accessible_wallet_public_ids
    assert list_mock.await_args is not None
    assert list_mock.await_args.kwargs["as_of"] == pinned


@pytest.mark.asyncio
async def test_orders_events_default_as_of_uses_datetime_now_utc() -> None:
    """When no ``as_of`` is supplied, the filter uses the current UTC time.

    Given: A VIEWER frame without an explicit ``as_of``,
    When: The orders.events scope filter is invoked,
    Then: The scope service receives an ``as_of`` between before and after now.
    """
    service = _mock_orders_events_service(accessible={"wallet-A"})
    before = datetime.now(UTC)
    await enforce_orders_events_scope(
        topic="orders.events.kraken.BTC-USD.executed",
        connection_principal=_viewer_principal(),
        payload={"wallet_public_id": "wallet-A"},
        scope_grant_service=service,
    )
    after = datetime.now(UTC)
    list_mock: AsyncMock = service.list_accessible_wallet_public_ids
    assert list_mock.await_args is not None
    assert before <= list_mock.await_args.kwargs["as_of"] <= after


def test_alerts_passes_through_non_alerts_topic() -> None:
    """Non-``alerts.*`` topics short-circuit to True.

    Given: A non-alerts topic (e.g. ``market.kraken.BTC-USD.tick``)
        and a payload whose ``user_public_id`` differs from the
        principal's,
    When: The alerts scope filter is invoked,
    Then: Returns True — the filter only owns the alerts family and
        must not spuriously drop frames belonging to other categories.
    """
    assert enforce_alerts_scope(
        topic="market.kraken.BTC-USD.tick",
        connection_principal=_viewer_principal(user_public_id="user-X"),
        payload={"user_public_id": "user-Y"},
    )


def test_alerts_drops_when_principal_missing() -> None:
    """No frame leaks to an un-authenticated socket.

    Given: A WS connection without an authenticated principal,
    When: The alerts scope filter is invoked,
    Then: Returns False so the bridge drops the frame.
    """
    assert not enforce_alerts_scope(
        topic="alerts.user-X.order_fill_full",
        connection_principal=None,
        payload={"user_public_id": "user-X"},
    )


def test_alerts_admin_bypasses_user_match_check() -> None:
    """ADMIN sees every user's alerts by contract — mirrors REST.

    Given: An ADMIN principal whose ``user_public_id`` differs from
        the frame's,
    When: The alerts scope filter is invoked,
    Then: Returns True without inspecting the payload mismatch.
    """
    assert enforce_alerts_scope(
        topic="alerts.user-X.order_fill_full",
        connection_principal=_admin_principal(),
        payload={"user_public_id": "user-X"},
    )


def test_alerts_drops_when_payload_user_field_missing() -> None:
    """Belt-and-braces fail-closed when the parser layer is bypassed.

    Given: A non-ADMIN principal + a payload missing ``user_public_id``,
    When: The alerts scope filter is invoked,
    Then: Returns False — the bridge's parser should have already
        dropped this frame; the filter is the defensive last gate.
    """
    assert not enforce_alerts_scope(
        topic="alerts.user-X.order_fill_full",
        connection_principal=_viewer_principal(user_public_id="user-X"),
        payload={},
    )


def test_alerts_drops_when_payload_user_field_non_string() -> None:
    """Non-string ``user_public_id`` (e.g. ``None``) fails-closed.

    Given: A non-ADMIN principal + a payload whose ``user_public_id``
        is ``None``,
    When: The alerts scope filter is invoked,
    Then: Returns False.
    """
    assert not enforce_alerts_scope(
        topic="alerts.user-X.order_fill_full",
        connection_principal=_viewer_principal(user_public_id="user-X"),
        payload={"user_public_id": None},
    )


def test_alerts_forwards_when_user_public_id_matches() -> None:
    """Matching ``user_public_id`` forwards the frame.

    Given: A VIEWER whose ``user_public_id`` equals the frame's,
    When: The alerts scope filter is invoked,
    Then: Returns True so the frame reaches the subscriber.
    """
    assert enforce_alerts_scope(
        topic="alerts.user-X.order_fill_full",
        connection_principal=_viewer_principal(user_public_id="user-X"),
        payload={"user_public_id": "user-X"},
    )


def test_alerts_drops_when_user_public_id_mismatches() -> None:
    """Mismatched ``user_public_id`` drops the frame — no cross-user leak.

    Given: A VIEWER whose ``user_public_id`` differs from the frame's,
    When: The alerts scope filter is invoked,
    Then: Returns False so the bridge drops the frame.
    """
    assert not enforce_alerts_scope(
        topic="alerts.user-X.order_fill_full",
        connection_principal=_viewer_principal(user_public_id="user-Y"),
        payload={"user_public_id": "user-X"},
    )
