"""Direct tests for the shared WebSocket topic-authorization policy.

``partition_authorized_topics`` is the single answer to "may this principal HOLD
this topic", consumed at subscribe time, during admin-bus reconciliation after an
authority change, and during in-place principal replacement at re-authentication.

The reconciliation consumers evaluate topics a client already holds, so these
tests exercise the function directly rather than only through ``handle_subscribe``.
The denied-ORDER test pins a contract the response envelope has always had and
which the reconciliation callers inherit.
"""

from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock

import pytest

from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository import Repository
from snapper.interface.websocket.topic_authorization import partition_authorized_topics

_AS_OF = datetime(2026, 7, 26, 22, 0, tzinfo=UTC)
_WALLET = "019e873c-d062-720f-85df-fd4d7fce5bdf"
_OTHER_WALLET = "019e873c-d060-762d-8cee-5fde4009513d"


def _principal(
    role: UserRole,
    active_wallet_public_id: str | None = None,
    delegate_public_id: str | None = None,
    operators: list[str] | None = None,
) -> AuthPrincipal:
    """Build an AuthPrincipal fixture for the policy under test."""
    return AuthPrincipal(
        username="u",
        role=role,
        user_public_id="00000000-0000-7000-8000-0000000000d1",
        operator_public_ids=operators or [],
        active_wallet_public_id=active_wallet_public_id,
        delegate_public_id=delegate_public_id,
    )


def _repo_with_pairs(pairs: set[tuple[str, str]]) -> Repository:
    """Build a repository double returning ``pairs`` for the scope projection."""
    repo = AsyncMock()
    repo.list_scope_grant_instrument_pairs = AsyncMock(return_value=pairs)
    return repo


@pytest.mark.asyncio
async def test_allowed_preserves_input_order() -> None:
    """Allowed topics come back in the order the caller supplied them.

    Given: An admin principal and three topics in a deliberate order,
    When: Partitioning them,
    Then: The allowed list preserves that order exactly.
    """
    topics = [
        "system.health",
        "market.kraken.BTC-USD.candles.1m",
        "signals.kraken.BTC-USD.live",
    ]
    allowed, denied = await partition_authorized_topics(
        topics=topics,
        principal=_principal(UserRole.ADMIN),
        repository=None,
        as_of=_AS_OF,
    )
    assert allowed == topics
    assert denied == []


@pytest.mark.asyncio
async def test_denied_order_is_category_then_backtest_then_delegate() -> None:
    """The denied list keeps its historical three-block order.

    Given: A delegate principal denied one topic by each of the three
        independent rules — category, backtest wallet scope, delegate
        instrument scope,
    When: Partitioning them together,
    Then: The denied list is ordered category, backtest, delegate.

    This ordering is the contract the subscribe response envelope has always
    had; the reconciliation callers inherit it, so a reordering would be a
    silent client-visible change rather than a refactor.
    """
    principal = _principal(
        UserRole.AI_DELEGATE,
        active_wallet_public_id=_WALLET,
        delegate_public_id="00000000-0000-7000-8000-0000000000de",
        operators=["op-1"],
    )
    allowed, denied = await partition_authorized_topics(
        topics=[
            "accruals.kraken.BTC-USD",
            f"backtest.{_OTHER_WALLET}.",
            "signals.kraken.ETH-USD.live",
            "system.health",
        ],
        principal=principal,
        repository=_repo_with_pairs({("kraken", "BTC-USD")}),
        as_of=_AS_OF,
    )
    assert allowed == ["system.health"]
    assert denied == [
        "accruals.kraken.BTC-USD",
        f"backtest.{_OTHER_WALLET}.",
        "signals.kraken.ETH-USD.live",
    ]


@pytest.mark.asyncio
async def test_backtest_topic_for_another_wallet_is_denied() -> None:
    """A backtest topic outside the active wallet is refused.

    Given: A viewer whose active wallet is _WALLET,
    When: Partitioning a backtest topic scoped to a different wallet,
    Then: It lands in denied.
    """
    allowed, denied = await partition_authorized_topics(
        topics=[f"backtest.{_OTHER_WALLET}."],
        principal=_principal(UserRole.VIEWER, active_wallet_public_id=_WALLET),
        repository=None,
        as_of=_AS_OF,
    )
    assert allowed == []
    assert denied == [f"backtest.{_OTHER_WALLET}."]


@pytest.mark.asyncio
async def test_backtest_topic_without_active_wallet_is_denied() -> None:
    """No active wallet means no backtest topic at all.

    Given: A viewer with active_wallet_public_id unset,
    When: Partitioning any backtest topic,
    Then: It is denied — the gate fails closed rather than open.
    """
    allowed, denied = await partition_authorized_topics(
        topics=[f"backtest.{_WALLET}."],
        principal=_principal(UserRole.VIEWER),
        repository=None,
        as_of=_AS_OF,
    )
    assert allowed == []
    assert denied == [f"backtest.{_WALLET}."]


@pytest.mark.asyncio
async def test_impersonate_operator_bypasses_the_backtest_wallet_gate() -> None:
    """A caller with global operator scope may hold any backtest topic.

    Given: An admin principal with no active wallet selected,
    When: Partitioning a backtest topic for an arbitrary wallet,
    Then: It is allowed, because IMPERSONATE_OPERATOR outranks wallet scope.
    """
    allowed, denied = await partition_authorized_topics(
        topics=[f"backtest.{_OTHER_WALLET}."],
        principal=_principal(UserRole.ADMIN),
        repository=None,
        as_of=_AS_OF,
    )
    assert allowed == [f"backtest.{_OTHER_WALLET}."]
    assert denied == []


@pytest.mark.asyncio
async def test_non_delegate_principal_needs_no_repository() -> None:
    """The delegate scope filter fast-paths a principal without a delegate row.

    Given: A viewer principal and no repository,
    When: Partitioning a wallet-scoped signals topic,
    Then: No scope projection is attempted and the topic survives on category
        rules alone.
    """
    allowed, denied = await partition_authorized_topics(
        topics=["signals.kraken.BTC-USD.live"],
        principal=_principal(UserRole.VIEWER),
        repository=None,
        as_of=_AS_OF,
    )
    assert allowed == ["signals.kraken.BTC-USD.live"]
    assert denied == []


@pytest.mark.asyncio
async def test_delegate_principal_without_repository_raises() -> None:
    """A delegate reaching the filter with no repository is a wiring bug.

    Given: A principal carrying a delegate identity and no repository,
    When: Partitioning any topic,
    Then: RuntimeError is raised rather than silently passing topics through,
        because a silent pass would leak wallet scope.
    """
    principal = _principal(
        UserRole.AI_DELEGATE,
        delegate_public_id="00000000-0000-7000-8000-0000000000de",
    )
    with pytest.raises(RuntimeError, match="repository reference"):
        await partition_authorized_topics(
            topics=["signals.kraken.BTC-USD.live"],
            principal=principal,
            repository=None,
            as_of=_AS_OF,
        )


@pytest.mark.asyncio
async def test_delegate_scope_is_read_at_the_supplied_bus_time() -> None:
    """The scope projection is queried with the caller's as_of, not wall clock.

    Given: A delegate principal and an explicit bus time,
    When: Partitioning topics,
    Then: The repository is asked for that operator set at that instant, so a
        reconciliation replaying an event time cannot silently read "now".
    """
    principal = _principal(
        UserRole.AI_DELEGATE,
        delegate_public_id="00000000-0000-7000-8000-0000000000de",
        operators=["op-1", "op-2"],
    )
    repository = _repo_with_pairs({("kraken", "BTC-USD")})
    await partition_authorized_topics(
        topics=["signals.kraken.BTC-USD.live"],
        principal=principal,
        repository=repository,
        as_of=_AS_OF,
    )
    repository.list_scope_grant_instrument_pairs.assert_awaited_once_with(["op-1", "op-2"], _AS_OF)
