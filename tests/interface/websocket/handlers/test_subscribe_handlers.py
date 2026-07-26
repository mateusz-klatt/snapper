"""Tests for WebSocket subscription handlers."""

import json
from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import PropertyMock
from unittest.mock import patch

import pytest

from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.core.types import SubscriptionStatusEnum
from snapper.interface.websocket.handlers.subscribe import handle_subscribe
from snapper.interface.websocket.handlers.subscribe import handle_unsubscribe
from snapper.interface.websocket.schemas import WSSubscribeRequest
from snapper.interface.websocket.schemas import WSSubscriptionSuccessResponse
from snapper.interface.websocket.schemas import WSUnsubscribeRequest
from snapper.messaging.infrastructure.publisher import SequenceTracker


def _principal(
    role: UserRole,
    active_wallet_public_id: str | None = None,
    username: str = "test",
    permissions: list[str] | None = None,
) -> AuthPrincipal:
    """Build a minimal AuthPrincipal for subscribe-handler tests."""
    return AuthPrincipal(
        username=username,
        role=role,
        active_wallet_public_id=active_wallet_public_id,
        permissions=permissions,
    )


def _research_subscription_mocks() -> tuple[AsyncMock, MagicMock]:
    """Build WebSocket and manager mocks for research subscription tests.

    Returns:
        WebSocket mock followed by a connection-manager mock with a bridge.
    """
    websocket = AsyncMock()
    websocket.send_text = AsyncMock()
    manager = MagicMock()
    manager.get_client_subscriptions = MagicMock(return_value=set())
    manager.subscribe_client = MagicMock()
    manager.zmq_bridge = MagicMock()
    manager.zmq_bridge.add_subscription = AsyncMock()
    type(manager).tracker = PropertyMock(return_value=SequenceTracker())
    return websocket, manager


@pytest.mark.parametrize(
    "permissions",
    [None, [Permission.SUBMIT_MARKET_VIEW.value]],
    ids=["full-role-grant", "narrow-submit-grant"],
)
@pytest.mark.asyncio
async def test_ai_researcher_subscribes_to_ai_research_root(
    permissions: list[str] | None,
) -> None:
    """Verify full and narrowed researcher tokens reach research wakes.

    Given: An AI_RESEARCHER token with its full grant or only SUBMIT_MARKET_VIEW.
    When: It subscribes to the ``ai_research.`` registry root.
    Then: The handler registers the subscription with the WebSocket bridge.
    """
    websocket, manager = _research_subscription_mocks()
    message = WSSubscribeRequest(
        public_id="test-pid",
        timestamp=datetime(2026, 7, 21, tzinfo=UTC),
        session_id="",
        sequence_id=0,
        topics=["ai_research."],
    )
    principal = _principal(UserRole.AI_RESEARCHER, permissions=permissions)

    await handle_subscribe(websocket, message, manager, principal)

    response = WSSubscriptionSuccessResponse.model_validate_json(
        str(websocket.send_text.call_args.args[0])
    )
    assert response.status == SubscriptionStatusEnum.SUBSCRIBED
    assert response.topics == ["ai_research."]
    assert response.denied_topics == []
    manager.subscribe_client.assert_called_once_with(websocket, "ai_research.")
    manager.zmq_bridge.add_subscription.assert_awaited_once_with(websocket, ["ai_research."])


@pytest.mark.asyncio
async def test_viewer_cannot_subscribe_to_ai_research_root() -> None:
    """Verify a role without SUBMIT_MARKET_VIEW cannot receive research wakes.

    Given: A VIEWER principal whose role lacks SUBMIT_MARKET_VIEW.
    When: It requests the ``ai_research.`` registry root.
    Then: The handler denies the topic and leaves the bridge untouched.
    """
    websocket, manager = _research_subscription_mocks()
    message = WSSubscribeRequest(
        public_id="test-pid",
        timestamp=datetime(2026, 7, 21, tzinfo=UTC),
        session_id="",
        sequence_id=0,
        topics=["ai_research."],
    )

    await handle_subscribe(
        websocket,
        message,
        manager,
        _principal(UserRole.VIEWER),
    )

    response = WSSubscriptionSuccessResponse.model_validate_json(
        str(websocket.send_text.call_args.args[0])
    )
    assert response.status == SubscriptionStatusEnum.DENIED
    assert response.topics == []
    assert response.denied_topics == ["ai_research."]
    manager.subscribe_client.assert_not_called()
    manager.zmq_bridge.add_subscription.assert_not_awaited()


@pytest.mark.asyncio
async def test_ai_researcher_cannot_subscribe_to_ai_reviews_root() -> None:
    """Verify the researcher cannot receive consult or trading-intent frames.

    Given: An AI_RESEARCHER principal without READ_SIGNALS or CREATE_ORDERS.
    When: It subscribes to the ``ai_reviews.`` registry root.
    Then: The handler denies the topic and leaves the bridge untouched.
    """
    websocket, manager = _research_subscription_mocks()
    message = WSSubscribeRequest(
        public_id="test-pid",
        timestamp=datetime(2026, 7, 21, tzinfo=UTC),
        session_id="",
        sequence_id=0,
        topics=["ai_reviews."],
    )

    await handle_subscribe(
        websocket,
        message,
        manager,
        _principal(UserRole.AI_RESEARCHER),
    )

    response = WSSubscriptionSuccessResponse.model_validate_json(
        str(websocket.send_text.call_args.args[0])
    )
    assert response.status == SubscriptionStatusEnum.DENIED
    assert response.topics == []
    assert response.denied_topics == ["ai_reviews."]
    manager.subscribe_client.assert_not_called()
    manager.zmq_bridge.add_subscription.assert_not_awaited()


class TestHandleSubscribeEdgeCases:
    """Edge case tests for WebSocket subscribe handler."""

    @pytest.fixture
    def mock_websocket(self) -> AsyncMock:
        """Provide mock WebSocket with send_text capability."""
        ws = AsyncMock()
        ws.send_text = AsyncMock()
        return ws

    @pytest.fixture
    def mock_manager(self) -> MagicMock:
        """Provide mock WebSocket connection manager."""
        manager = MagicMock()
        manager.get_client_subscriptions = MagicMock(return_value=set())
        manager.subscribe_client = MagicMock()
        manager.unsubscribe_client = MagicMock()
        manager.zmq_bridge = MagicMock()
        manager.zmq_bridge.add_subscription = AsyncMock()
        manager.zmq_bridge.remove_subscription = AsyncMock()
        manager.topic_manager = MagicMock()
        manager.topic_manager.get_all_topics = MagicMock(return_value=[])
        type(manager).tracker = PropertyMock(return_value=SequenceTracker())
        return manager

    @pytest.mark.asyncio
    async def test_subscribe_with_invalid_topic_format_in_list(
        self, mock_websocket: AsyncMock, mock_manager: MagicMock
    ) -> None:
        """Subscribe rejects invalid topic format.

        Given: A list of topics with invalid format (double dots),
        When: Handling subscribe request,
        Then: Returns error response with invalid topic names.
        """
        message = WSSubscribeRequest(
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            session_id="",
            sequence_id=0,
            topics=["invalid..topic", "another..bad"],
        )
        await handle_subscribe(mock_websocket, message, mock_manager, _principal(UserRole.ADMIN))
        mock_websocket.send_text.assert_called_once()
        response = json.loads(mock_websocket.send_text.call_args[0][0])
        assert response["type"] == "error"
        assert "Invalid topic format" in response["message"]
        assert "invalid..topic" in response["message"]

    @pytest.mark.asyncio
    async def test_subscribe_empty_and_whitespace_topics(
        self, mock_websocket: AsyncMock, mock_manager: MagicMock
    ) -> None:
        """Subscribe handles empty and whitespace topics.

        Given: A list with empty and whitespace-only topics,
        When: Handling subscribe request,
        Then: Returns success with no_topics status.
        """
        message = WSSubscribeRequest(
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            session_id="",
            sequence_id=0,
            topics=["  ", ""],
        )
        await handle_subscribe(mock_websocket, message, mock_manager, _principal(UserRole.ADMIN))
        mock_websocket.send_text.assert_called_once()
        response = json.loads(mock_websocket.send_text.call_args[0][0])
        assert response["type"] == "subscription_success"
        assert response["status"] == "no_topics"

    @pytest.mark.asyncio
    async def test_subscribe_when_all_topics_denied(
        self, mock_websocket: AsyncMock, mock_manager: MagicMock
    ) -> None:
        """Subscribe denies all topics when role lacks permission.

        Given: Admin topics and a viewer role,
        When: Handling subscribe request,
        Then: Returns success with denied status and all denied topics.
        """
        message = WSSubscribeRequest(
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            session_id="",
            sequence_id=0,
            topics=["admin.users", "admin.settings"],
        )
        await handle_subscribe(mock_websocket, message, mock_manager, _principal(UserRole.VIEWER))
        mock_websocket.send_text.assert_called_once()
        response = json.loads(mock_websocket.send_text.call_args[0][0])
        assert response["type"] == "subscription_success"
        assert response["status"] == "denied"
        assert len(response["denied_topics"]) == 2

    @pytest.mark.asyncio
    async def test_subscribe_when_zmq_bridge_unavailable(
        self, mock_websocket: AsyncMock, mock_manager: MagicMock
    ) -> None:
        """Subscribe returns error when ZMQ bridge unavailable.

        Given: A manager with zmq_bridge set to None,
        When: Subscribing to valid topics,
        Then: Returns error message about unavailable ZMQ bridge.
        """
        mock_manager.zmq_bridge = None
        valid_topic = "market.kraken.BTC-USD.candles.1m"
        mock_manager.topic_manager.get_all_topics = MagicMock(return_value=[valid_topic])
        message = WSSubscribeRequest(
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            session_id="",
            sequence_id=0,
            topics=[valid_topic],
        )
        with (
            patch(
                "snapper.interface.websocket.topic_authorization.get_allowed_topics_for_role",
                return_value=[valid_topic],
            ),
            patch(
                "snapper.interface.websocket.topic_authorization.filter_topics",
                return_value=([valid_topic], []),
            ),
        ):
            await handle_subscribe(
                mock_websocket, message, mock_manager, _principal(UserRole.ADMIN)
            )
        calls = mock_websocket.send_text.call_args_list
        found_error = False
        for call in calls:
            response = json.loads(call[0][0])
            if response.get("type") == "error" and "ZMQ bridge" in response.get("message", ""):
                found_error = True
                break
        assert found_error, "Expected ZMQ bridge error message"


class TestAdminCategorySubscription:
    """Handler-level tests proving ADMIN/OPERATOR boundary for admin topics."""

    @pytest.fixture
    def mock_websocket(self) -> AsyncMock:
        """Provide mock WebSocket with send_text capability."""
        ws = AsyncMock()
        ws.send_text = AsyncMock()
        return ws

    @pytest.fixture
    def mock_manager(self) -> MagicMock:
        """Provide mock WebSocket connection manager."""
        manager = MagicMock()
        manager.get_client_subscriptions = MagicMock(return_value=set())
        manager.subscribe_client = MagicMock()
        manager.zmq_bridge = MagicMock()
        manager.zmq_bridge.add_subscription = AsyncMock()
        type(manager).tracker = PropertyMock(return_value=SequenceTracker())
        return manager

    @pytest.mark.asyncio
    async def test_admin_subscribes_to_admin_topic_successfully(
        self, mock_websocket: AsyncMock, mock_manager: MagicMock
    ) -> None:
        """ADMIN can subscribe to admin.users through the handler.

        Given: An ADMIN role subscribing to admin.users,
        When: Handling subscribe request through handle_subscribe,
        Then: The topic appears in the subscribed topics list.
        """
        message = WSSubscribeRequest(
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            session_id="",
            sequence_id=0,
            topics=["admin.users"],
        )
        await handle_subscribe(mock_websocket, message, mock_manager, _principal(UserRole.ADMIN))
        mock_websocket.send_text.assert_called_once()
        response = json.loads(mock_websocket.send_text.call_args[0][0])
        assert response["type"] == "subscription_success"
        assert "admin.users" in response["topics"]
        assert response["status"] in {"subscribed", "partial"}

    @pytest.mark.asyncio
    async def test_operator_denied_admin_topic(
        self, mock_websocket: AsyncMock, mock_manager: MagicMock
    ) -> None:
        """OPERATOR is denied admin.users through the handler.

        Given: An OPERATOR role subscribing to admin.users,
        When: Handling subscribe request through handle_subscribe,
        Then: The topic appears in denied_topics.
        """
        message = WSSubscribeRequest(
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            session_id="",
            sequence_id=0,
            topics=["admin.users"],
        )
        await handle_subscribe(mock_websocket, message, mock_manager, _principal(UserRole.OPERATOR))
        mock_websocket.send_text.assert_called_once()
        response = json.loads(mock_websocket.send_text.call_args[0][0])
        assert response["type"] == "subscription_success"
        assert response["status"] == "denied"
        assert "admin.users" in response["denied_topics"]

    @pytest.mark.asyncio
    async def test_operator_token_scope_denies_role_allowed_market_category(
        self, mock_websocket: AsyncMock, mock_manager: MagicMock
    ) -> None:
        """A token grant narrower than OPERATOR denies an excluded category.

        Given: An OPERATOR token carrying only ``READ_SYSTEM_STATUS``,
        When: It requests both the market root and a concrete market topic,
        Then: Both subscriptions are denied despite the role granting market reads.
        """
        topics = ["market.", "market.kraken.BTC-USD.candles.1m"]
        message = WSSubscribeRequest(
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            session_id="",
            sequence_id=0,
            topics=topics,
        )
        principal = _principal(
            UserRole.OPERATOR,
            permissions=["read:system_status"],
        )

        await handle_subscribe(mock_websocket, message, mock_manager, principal)

        mock_websocket.send_text.assert_called_once()
        response = json.loads(mock_websocket.send_text.call_args[0][0])
        assert response["status"] == "denied"
        assert response["topics"] == []
        assert response["denied_topics"] == topics
        mock_manager.subscribe_client.assert_not_called()
        mock_manager.zmq_bridge.add_subscription.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_admin_subscribes_to_admin_root_pattern(
        self, mock_websocket: AsyncMock, mock_manager: MagicMock
    ) -> None:
        """ADMIN can subscribe to admin. root pattern through the handler.

        Given: An ADMIN role subscribing to admin. prefix,
        When: Handling subscribe request through handle_subscribe,
        Then: admin. appears in the subscribed topics.
        """
        message = WSSubscribeRequest(
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            session_id="",
            sequence_id=0,
            topics=["admin."],
        )
        await handle_subscribe(mock_websocket, message, mock_manager, _principal(UserRole.ADMIN))
        mock_websocket.send_text.assert_called_once()
        response = json.loads(mock_websocket.send_text.call_args[0][0])
        assert response["type"] == "subscription_success"
        assert "admin." in response["topics"]


class TestAccountStateCategorySubscription:
    """Handler-level RBAC matrix for account invalidation subscriptions."""

    @pytest.fixture
    def mock_websocket(self) -> AsyncMock:
        """Provide a WebSocket mock that captures subscription responses."""
        websocket = AsyncMock()
        websocket.send_text = AsyncMock()
        return websocket

    @pytest.fixture
    def mock_manager(self) -> MagicMock:
        """Provide a connection manager with an available bridge."""
        manager = MagicMock()
        manager.get_client_subscriptions = MagicMock(return_value=set())
        manager.subscribe_client = MagicMock()
        manager.zmq_bridge = MagicMock()
        manager.zmq_bridge.add_subscription = AsyncMock()
        type(manager).tracker = PropertyMock(return_value=SequenceTracker())
        return manager

    @pytest.mark.parametrize(
        "role",
        [UserRole.VIEWER, UserRole.OPERATOR, UserRole.ADMIN],
    )
    @pytest.mark.asyncio
    async def test_read_account_state_roles_can_subscribe(
        self,
        role: UserRole,
        mock_websocket: AsyncMock,
        mock_manager: MagicMock,
    ) -> None:
        """Every role holding READ_ACCOUNT_STATE may subscribe to the root.

        Given: A VIEWER, OPERATOR, or ADMIN principal.
        When: The principal subscribes to ``portfolio.accounts.``.
        Then: The handler registers the root with the ZMQ bridge.
        """
        message = WSSubscribeRequest(
            public_id="test-pid",
            timestamp=datetime(2026, 7, 16, tzinfo=UTC),
            session_id="",
            sequence_id=0,
            topics=["portfolio.accounts."],
        )

        await handle_subscribe(mock_websocket, message, mock_manager, _principal(role))

        response = json.loads(mock_websocket.send_text.call_args[0][0])
        assert response["type"] == "subscription_success"
        assert response["status"] == "subscribed"
        assert response["topics"] == ["portfolio.accounts."]
        mock_manager.zmq_bridge.add_subscription.assert_awaited_once_with(
            mock_websocket, ["portfolio.accounts."]
        )

    @pytest.mark.asyncio
    async def test_ai_delegate_is_denied_account_state_root(
        self,
        mock_websocket: AsyncMock,
        mock_manager: MagicMock,
    ) -> None:
        """AI_DELEGATE cannot subscribe without READ_ACCOUNT_STATE.

        Given: An AI_DELEGATE principal whose role lacks account-state read access.
        When: The principal requests the account invalidation root.
        Then: Category RBAC denies it and the bridge is untouched.
        """
        repository = MagicMock()
        repository.list_scope_grant_instrument_pairs = AsyncMock(return_value=set())
        message = WSSubscribeRequest(
            public_id="test-pid",
            timestamp=datetime(2026, 7, 16, tzinfo=UTC),
            session_id="",
            sequence_id=0,
            topics=["portfolio.accounts."],
        )

        await handle_subscribe(
            mock_websocket,
            message,
            mock_manager,
            _principal(UserRole.AI_DELEGATE),
            repository,
        )

        response = json.loads(mock_websocket.send_text.call_args[0][0])
        assert response["type"] == "subscription_success"
        assert response["status"] == "denied"
        assert response["topics"] == []
        assert response["denied_topics"] == ["portfolio.accounts."]
        mock_manager.zmq_bridge.add_subscription.assert_not_awaited()


class TestIntermediatePrefixRejection:
    """Tests for intermediate prefix rejection in WS subscription validation."""

    @pytest.fixture
    def mock_websocket(self) -> AsyncMock:
        """Provide mock WebSocket with send_text capability."""
        ws = AsyncMock()
        ws.send_text = AsyncMock()
        return ws

    @pytest.fixture
    def mock_manager(self) -> MagicMock:
        """Provide mock WebSocket connection manager."""
        manager = MagicMock()
        manager.get_client_subscriptions = MagicMock(return_value=set())
        manager.subscribe_client = MagicMock()
        manager.zmq_bridge = MagicMock()
        manager.zmq_bridge.add_subscription = AsyncMock()
        type(manager).tracker = PropertyMock(return_value=SequenceTracker())
        return manager

    @pytest.mark.asyncio
    async def test_subscribe_rejects_intermediate_prefix(
        self, mock_websocket: AsyncMock, mock_manager: MagicMock
    ) -> None:
        """Subscribe rejects prefix patterns not in TOPIC_REGISTRY.

        Given: An intermediate prefix like ``market.kraken.`` (not a registry root),
        When: Handling subscribe request,
        Then: Returns error response naming the invalid prefix.
        """
        message = WSSubscribeRequest(
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            session_id="",
            sequence_id=0,
            topics=["market.kraken."],
        )
        await handle_subscribe(mock_websocket, message, mock_manager, _principal(UserRole.ADMIN))
        mock_websocket.send_text.assert_called_once()
        response = json.loads(mock_websocket.send_text.call_args[0][0])
        assert response["type"] == "error"
        assert "market.kraken." in response["message"]
        assert "registry root" in response["message"]

    @pytest.mark.asyncio
    async def test_subscribe_accepts_registry_root_prefix(
        self, mock_websocket: AsyncMock, mock_manager: MagicMock
    ) -> None:
        """Subscribe accepts prefix patterns that ARE in TOPIC_REGISTRY.

        Given: A valid registry root like ``market.``,
        When: Handling subscribe request,
        Then: Returns subscription success (not an error).
        """
        message = WSSubscribeRequest(
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            session_id="",
            sequence_id=0,
            topics=["market."],
        )
        await handle_subscribe(mock_websocket, message, mock_manager, _principal(UserRole.ADMIN))
        mock_websocket.send_text.assert_called_once()
        response = json.loads(mock_websocket.send_text.call_args[0][0])
        assert response["type"] == "subscription_success"


class TestHandleUnsubscribeEdgeCases:
    """Edge case tests for WebSocket unsubscribe handler."""

    @pytest.fixture
    def mock_websocket(self) -> AsyncMock:
        """Provide mock WebSocket with send_text capability."""
        ws = AsyncMock()
        ws.send_text = AsyncMock()
        return ws

    @pytest.fixture
    def mock_manager(self) -> MagicMock:
        """Provide mock connection manager with subscriptions."""
        manager = MagicMock()
        manager.get_client_subscriptions = MagicMock(
            return_value={"market.kraken.BTC-USD.candles.1m"}
        )
        manager.unsubscribe_client = MagicMock()
        manager.zmq_bridge = MagicMock()
        manager.zmq_bridge.remove_subscription = AsyncMock()
        type(manager).tracker = PropertyMock(return_value=SequenceTracker())
        return manager

    @pytest.mark.asyncio
    async def test_unsubscribe_topics_not_subscribed(
        self, mock_websocket: AsyncMock, mock_manager: MagicMock
    ) -> None:
        """Unsubscribe handles topics not currently subscribed.

        Given: Client subscribed to BTC-USD but not ETH-USD,
        When: Unsubscribing from ETH-USD,
        Then: Returns success with no_topics status and denied topic.
        """
        message = WSUnsubscribeRequest(
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            session_id="",
            sequence_id=0,
            topics=["market.kraken.ETH-USD.candles.1m"],
        )
        await handle_unsubscribe(mock_websocket, message, mock_manager)
        mock_websocket.send_text.assert_called_once()
        response = json.loads(mock_websocket.send_text.call_args[0][0])
        assert response["type"] == "subscription_success"
        assert response["status"] == "no_topics"
        assert "market.kraken.ETH-USD.candles.1m" in response["denied_topics"]

    @pytest.mark.asyncio
    async def test_unsubscribe_when_zmq_bridge_unavailable(
        self, mock_websocket: AsyncMock, mock_manager: MagicMock
    ) -> None:
        """Unsubscribe returns error when ZMQ bridge unavailable.

        Given: A manager with zmq_bridge set to None,
        When: Unsubscribing from topics,
        Then: Returns error about unavailable ZMQ bridge.
        """
        mock_manager.zmq_bridge = None
        message = WSUnsubscribeRequest(
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            session_id="",
            sequence_id=0,
            topics=["market.kraken.BTC-USD.candles.1m"],
        )
        await handle_unsubscribe(mock_websocket, message, mock_manager)
        mock_websocket.send_text.assert_called_once()
        response = json.loads(mock_websocket.send_text.call_args[0][0])
        assert response["type"] == "error"
        assert "ZMQ bridge is not available" in response["message"]

    @pytest.mark.asyncio
    async def test_unsubscribe_partial_success(
        self, mock_websocket: AsyncMock, mock_manager: MagicMock
    ) -> None:
        """Unsubscribe returns partial status for mixed subscribed/not-subscribed topics.

        Given: Client subscribed to one topic but not another,
        When: Unsubscribing from both,
        Then: Returns partial status with allowed and denied lists.
        """
        mock_manager.get_client_subscriptions = MagicMock(
            return_value={"market.kraken.BTC-USD.candles.1m"}
        )
        message = WSUnsubscribeRequest(
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            session_id="",
            sequence_id=0,
            topics=["market.kraken.BTC-USD.candles.1m", "market.kraken.ETH-USD.candles.1m"],
        )
        await handle_unsubscribe(mock_websocket, message, mock_manager)
        mock_websocket.send_text.assert_called_once()
        response = json.loads(mock_websocket.send_text.call_args[0][0])
        assert response["type"] == "subscription_success"
        assert response["status"] == "partial"
        assert "market.kraken.BTC-USD.candles.1m" in response["topics"]
        assert "market.kraken.ETH-USD.candles.1m" in response["denied_topics"]

    @pytest.mark.asyncio
    async def test_unsubscribe_with_whitespace_only_topics(
        self, mock_websocket: AsyncMock, mock_manager: MagicMock
    ) -> None:
        """Unsubscribe filters whitespace topics and processes valid ones.

        Given: Mixed whitespace and valid topics with active subscription,
        When: Unsubscribing,
        Then: Only valid topic is processed and returned in response.
        """
        mock_manager.get_client_subscriptions = MagicMock(
            return_value={"market.kraken.BTC-USD.ticks"}
        )
        message = WSUnsubscribeRequest(
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            session_id="",
            sequence_id=0,
            topics=["   ", "\t", "", "market.kraken.BTC-USD.ticks"],
        )
        await handle_unsubscribe(mock_websocket, message, mock_manager)
        mock_websocket.send_text.assert_called_once()
        response = json.loads(mock_websocket.send_text.call_args[0][0])
        assert response["type"] == "subscription_success"
        assert "market.kraken.BTC-USD.ticks" in response["topics"]
