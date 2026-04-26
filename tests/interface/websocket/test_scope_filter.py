"""Tests for the Plan D §9 + Q15 per-frame ``ai_reviews.*`` scope filter.

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

    Plan D §9 + Q15 — the filter only owns the AI-review family.
    Other categories (market, signals, orders, etc.) have their own
    subscribe-time RBAC + wallet narrowing.

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

    Plan A Q19 + Plan D §9 — the scope-grant check is delegate-keyed;
    a non-delegate principal that somehow subscribed cannot evaluate
    so we play it safe and drop.

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
