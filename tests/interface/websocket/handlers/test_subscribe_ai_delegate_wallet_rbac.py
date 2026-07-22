"""AI review-principal subscribe-time wallet-scope RBAC matrix.

Authoritative truth table:

=======================================================================  ========
Topic                                                                    Verdict
=======================================================================  ========
signals.kraken.BTC-USD.live  (pair in allowed)                           accepted
signals.kraken.ETH-USD.live  (pair NOT in allowed)                       denied
orders.commands.kraken.BTC-USD.submit  (pair in allowed)                 accepted
orders.events.kraken.BTC-USD.executed  (pair in allowed)                 accepted
signals.paper.BTC-USD.my_strategy      (paper prefix pass-through)       accepted
market.kraken.BTC-USD.ticks            (non-wallet-scoped prefix)        accepted
=======================================================================  ========

Principals backed by an ``ai_delegates`` row use the filter. Principals
without delegate state pass through unchanged regardless of their role.
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
from snapper.interface.websocket.handlers.subscribe import _enforce_ai_delegate_wallet_scope
from snapper.interface.websocket.handlers.subscribe import handle_subscribe
from snapper.interface.websocket.schemas import WSSubscribeRequest
from snapper.messaging.infrastructure.publisher import SequenceTracker


def _principal(
    role: UserRole,
    operators: list[str] | None = None,
    delegate_public_id: str | None = None,
) -> AuthPrincipal:
    """Build an AuthPrincipal fixture (defaults to empty operator set)."""
    return AuthPrincipal(
        username="delegate",
        role=role,
        user_public_id="00000000-0000-7000-8000-0000000000d1",
        operator_public_ids=operators or [],
        delegate_public_id=delegate_public_id,
    )


def _mock_manager() -> MagicMock:
    """Build a mock WebSocket connection manager."""
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


def _repo_with_pairs(pairs: set[tuple[str, str]]) -> AsyncMock:
    """Build a repository mock returning ``pairs`` for the pair query."""
    repo = AsyncMock()
    repo.list_scope_grant_instrument_pairs = AsyncMock(return_value=pairs)
    return repo


class TestAIDelegateWalletScopeFilter:
    """Direct tests of :func:`_enforce_ai_delegate_wallet_scope`."""

    @pytest.mark.parametrize(
        ("topic", "allowed_pairs", "expected_allowed"),
        [
            ("signals.kraken.BTC-USD.live", {("kraken", "BTC-USD")}, True),
            ("signals.kraken.ETH-USD.live", {("kraken", "BTC-USD")}, False),
            ("orders.commands.kraken.BTC-USD.submit", {("kraken", "BTC-USD")}, True),
            ("orders.events.kraken.BTC-USD.executed", {("kraken", "BTC-USD")}, True),
            ("signals.paper.BTC-USD.my_strategy", set(), True),
            ("market.kraken.BTC-USD.ticks", set(), True),
        ],
    )
    @pytest.mark.asyncio
    async def test_plan_d2_truth_table(
        self,
        topic: str,
        allowed_pairs: set[tuple[str, str]],
        expected_allowed: bool,
    ) -> None:
        """Truth table — 8 rows exercised here."""
        repo = _repo_with_pairs(allowed_pairs)
        allowed, denied = await _enforce_ai_delegate_wallet_scope(
            [topic],
            _principal(
                UserRole.AI_DELEGATE,
                operators=["op-1"],
                delegate_public_id="delegate-1",
            ),
            repo,
            datetime.now(UTC),
        )
        if expected_allowed:
            assert allowed == [topic]
            assert denied == []
        else:
            assert allowed == []
            assert denied == [topic]

    @pytest.mark.asyncio
    async def test_ai_reviewer_uses_wallet_scope_filter(self) -> None:
        """AI_REVIEWER receives the same wallet-scope filtering as AI_DELEGATE.

        Given: A reviewer with one allowed instrument pair and two signal topics.
        When: The shared review-principal wallet-scope filter evaluates the topics.
        Then: The allowed topic passes, the other is denied, and scope is queried once.
        """
        allowed_topic = "signals.kraken.BTC-USD.live"
        denied_topic = "signals.kraken.ETH-USD.live"
        repo = _repo_with_pairs({("kraken", "BTC-USD")})
        as_of = datetime.now(UTC)

        allowed, denied = await _enforce_ai_delegate_wallet_scope(
            [allowed_topic, denied_topic],
            _principal(
                UserRole.AI_REVIEWER,
                operators=["op-1"],
                delegate_public_id="reviewer-1",
            ),
            repo,
            as_of,
        )

        assert allowed == [allowed_topic]
        assert denied == [denied_topic]
        repo.list_scope_grant_instrument_pairs.assert_awaited_once_with(["op-1"], as_of)

    @pytest.mark.asyncio
    async def test_non_ai_review_principal_fast_path_passes_through(self) -> None:
        """Every non-review-principal role skips the filter entirely.

        Verifies that VIEWER, OPERATOR, ADMIN, even without a live
        repository, never hit the scope-grant pair query — the filter
        returns the original topic list with an empty denied list.
        """
        topics = [
            "signals.kraken.BTC-USD.live",
            "signals.kraken.ETH-USD.live",
            "orders.events.kraken.BTC-USD.executed",
            "market.kraken.BTC-USD.ticks",
        ]
        for role in (UserRole.VIEWER, UserRole.OPERATOR, UserRole.ADMIN):
            allowed, denied = await _enforce_ai_delegate_wallet_scope(
                topics, _principal(role), repository=None, as_of=datetime.now(UTC)
            )
            assert allowed == topics
            assert denied == []

    @pytest.mark.asyncio
    async def test_ai_delegate_without_repository_raises(self) -> None:
        """Missing repository for AI_DELEGATE is a runtime wiring bug."""
        with pytest.raises(RuntimeError, match="dispatch table wiring is broken"):
            await _enforce_ai_delegate_wallet_scope(
                ["signals.kraken.BTC-USD.live"],
                _principal(
                    UserRole.AI_DELEGATE,
                    operators=["op-1"],
                    delegate_public_id="delegate-1",
                ),
                repository=None,
                as_of=datetime.now(UTC),
            )

    @pytest.mark.asyncio
    async def test_empty_operator_set_yields_no_pairs_then_denies_everything(
        self,
    ) -> None:
        """AI_DELEGATE with no operators: every wallet-scoped topic denies.

        The repository returns the empty set for an empty operator list
        (fast path); all wallet-scoped topics fall into ``denied`` while
        non-wallet-scoped topics pass through.
        """
        repo = _repo_with_pairs(set())
        topics = [
            "signals.kraken.BTC-USD.live",
            "orders.commands.kraken.ETH-USD.submit",
            "market.kraken.BTC-USD.ticks",
            "signals.paper.MNQU6-CME.my_strategy",
        ]
        allowed, denied = await _enforce_ai_delegate_wallet_scope(
            topics,
            _principal(
                UserRole.AI_DELEGATE,
                operators=[],
                delegate_public_id="delegate-1",
            ),
            repo,
            datetime.now(UTC),
        )
        assert allowed == [
            "market.kraken.BTC-USD.ticks",
            "signals.paper.MNQU6-CME.my_strategy",
        ]
        assert denied == [
            "signals.kraken.BTC-USD.live",
            "orders.commands.kraken.ETH-USD.submit",
        ]


class TestAIDelegateHandleSubscribeIntegration:
    """End-to-end :func:`handle_subscribe` integration for AI_DELEGATE."""

    @pytest.mark.asyncio
    async def test_denied_wallet_topics_appear_in_response_envelope(self) -> None:
        """Denied wallet-scoped topics surface via ``denied_topics``.

        Wallet-scope denials ride the same envelope field the
        category-RBAC filter uses; the client sees a mixed
        ``status == "denied"`` with the offending topics listed.
        """
        ws = AsyncMock()
        manager = _mock_manager()
        repo = _repo_with_pairs({("kraken", "BTC-USD")})

        await handle_subscribe(
            ws,
            _msg(
                [
                    "signals.kraken.BTC-USD.live",
                    "signals.kraken.ETH-USD.live",
                ]
            ),
            manager,
            _principal(
                UserRole.AI_DELEGATE,
                operators=["op-1"],
                delegate_public_id="delegate-1",
            ),
            repo,
        )

        response = json.loads(ws.send_text.call_args[0][0])
        assert "signals.kraken.ETH-USD.live" in response["denied_topics"]

    @pytest.mark.asyncio
    async def test_filter_runs_once_per_subscribe_call(self) -> None:
        """The pair-set projection is a single round-trip.

        We assert ``list_scope_grant_instrument_pairs`` is awaited
        exactly once even when multiple wallet-scoped topics are
        present; no caching is required because one call per
        subscribe is already the authoritative read budget.
        """
        ws = AsyncMock()
        manager = _mock_manager()
        repo = _repo_with_pairs({("kraken", "BTC-USD"), ("kraken", "ETH-USD")})

        await handle_subscribe(
            ws,
            _msg(
                [
                    "signals.kraken.BTC-USD.live",
                    "signals.kraken.ETH-USD.live",
                    "orders.events.kraken.BTC-USD.executed",
                ]
            ),
            manager,
            _principal(
                UserRole.AI_DELEGATE,
                operators=["op-1"],
                delegate_public_id="delegate-1",
            ),
            repo,
        )
        assert repo.list_scope_grant_instrument_pairs.await_count == 1
