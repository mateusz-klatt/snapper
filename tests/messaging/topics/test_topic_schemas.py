"""Tests for ZMQ topic schemas and registry utilities."""

from snapper.messaging.topics.schemas import TOPIC_REGISTRY
from snapper.messaging.topics.schemas import TopicSchema
from snapper.messaging.topics.schemas import get_topics_by_category


class TestTopicSchema:
    """Tests for TopicSchema data class."""

    def test_creation_with_defaults(self) -> None:
        """TopicSchema applies default throttle_ms when not specified.

        Given: Required schema parameters only,
        When: TopicSchema is created,
        Then: throttle_ms defaults to 100.
        """
        schema = TopicSchema(pattern="test.", category="test")
        assert schema.pattern == "test."
        assert schema.category == "test"
        assert schema.throttle_ms == 100

    def test_creation_with_custom_throttle(self) -> None:
        """TopicSchema stores custom throttle_ms.

        Given: Schema parameters with explicit throttle_ms,
        When: TopicSchema is created,
        Then: Custom throttle_ms is stored.
        """
        schema = TopicSchema(pattern="test.", category="test", throttle_ms=500)
        assert schema.throttle_ms == 500

    def test_frozen(self) -> None:
        """TopicSchema is immutable.

        Given: A TopicSchema instance,
        When: Attempting to modify a field,
        Then: FrozenInstanceError is raised.
        """
        schema = TopicSchema(pattern="test.", category="test")
        try:
            schema.pattern = "other."
            raise AssertionError("Expected FrozenInstanceError")
        except AttributeError:
            pass


class TestTopicRegistry:
    """Tests for TOPIC_REGISTRY contents."""

    def test_contains_core_patterns(self) -> None:
        """TOPIC_REGISTRY contains all core topic patterns.

        Given: TOPIC_REGISTRY tuple,
        When: Checking for core patterns,
        Then: market, signals, heartbeats, orders, admin patterns all exist.
        """
        patterns = {s.pattern for s in TOPIC_REGISTRY}
        expected = {
            "market.",
            "signals.",
            "system.egress.",
            "system.heartbeats.",
            "orders.commands.",
            "orders.events.",
        }
        assert expected <= patterns

    def test_is_tuple(self) -> None:
        """TOPIC_REGISTRY is an immutable tuple.

        Given: TOPIC_REGISTRY,
        When: Checking its type,
        Then: It is a tuple of TopicSchema instances.
        """
        assert isinstance(TOPIC_REGISTRY, tuple)
        assert all(isinstance(s, TopicSchema) for s in TOPIC_REGISTRY)

    def test_trade_topics_have_zero_throttle(self) -> None:
        """Order command and plan-decision topics have zero throttle.

        Given: TOPIC_REGISTRY with trade-category topics,
        When: Checking throttle_ms for trade category,
        Then: All trade topics have throttle_ms=0.

        Note: ``orders.events.`` moved to category ``trade_events`` in
        v0.7.0 (RBAC symmetry — read-side echo gated by READ_ORDERS,
        not CREATE_ORDERS) and is asserted in
        :meth:`test_trade_events_topics_have_zero_throttle`.
        """
        trade = [s for s in TOPIC_REGISTRY if s.category == "trade"]
        assert len(trade) == 2
        assert {s.pattern for s in trade} == {
            "orders.commands.",
            "plans.decisions.",
        }
        assert all(s.throttle_ms == 0 for s in trade)

    def test_trade_events_topics_have_zero_throttle(self) -> None:
        """``orders.events.`` lives in the trade_events category at zero throttle.

        Given: TOPIC_REGISTRY with the trade_events category,
        When: Checking throttle_ms for trade_events category,
        Then: ``orders.events.`` is the sole entry, with throttle_ms=0.
        """
        trade_events = [s for s in TOPIC_REGISTRY if s.category == "trade_events"]
        assert len(trade_events) == 1
        assert trade_events[0].pattern == "orders.events."
        assert trade_events[0].throttle_ms == 0


class TestTopicUtilities:
    """Tests for topic utility functions."""

    def test_get_topics_by_category_market(self) -> None:
        """get_topics_by_category filters correctly.

        Given: Topics registered in different categories,
        When: Filtering by 'market',
        Then: Returns only market schemas.
        """
        market = get_topics_by_category("market")
        assert len(market) == 1
        assert market[0].pattern == "market."

    def test_get_topics_by_category_signals(self) -> None:
        """get_topics_by_category returns signals topics.

        Given: Topics with category 'signals',
        When: Filtering by 'signals',
        Then: Returns signals schema only.
        """
        signals = get_topics_by_category("signals")
        assert len(signals) == 1
        assert signals[0].pattern == "signals."

    def test_get_topics_by_category_empty(self) -> None:
        """get_topics_by_category returns empty list for unknown category.

        Given: No topics in 'nonexistent' category,
        When: Filtering by 'nonexistent',
        Then: Returns empty list.
        """
        assert get_topics_by_category("nonexistent") == []

    def test_accruals_registry_entry_exists(self) -> None:
        """Verify accruals entry exists in TOPIC_REGISTRY.

        Given: TOPIC_REGISTRY,
        When: Filtering for accruals pattern,
        Then: One entry with throttle_ms=1000 and category=accruals.
        """
        accrual_schemas = [s for s in TOPIC_REGISTRY if s.pattern == "accruals."]
        assert len(accrual_schemas) == 1
        assert accrual_schemas[0].category == "accruals"
        assert accrual_schemas[0].throttle_ms == 1000

    def test_accruals_category_query(self) -> None:
        """Verify get_topics_by_category finds accruals entry."""
        results = get_topics_by_category("accruals")
        assert len(results) == 1
        assert results[0].pattern == "accruals."

    def test_alerts_registry_entry_exists(self) -> None:
        """Verify alerts entry exists in TOPIC_REGISTRY.

        Given: TOPIC_REGISTRY,
        When: Filtering for alerts pattern,
        Then: One entry with throttle_ms=500 and category=notifications.
        """
        alerts = [s for s in TOPIC_REGISTRY if s.pattern == "alerts."]

        assert len(alerts) == 1
        assert alerts[0].category == "notifications"
        assert alerts[0].throttle_ms == 500

    def test_alerts_category_query(self) -> None:
        """Verify get_topics_by_category finds the alerts entry."""
        results = get_topics_by_category("notifications")

        assert len(results) == 1
        assert results[0].pattern == "alerts."

    def test_account_state_registry_entry_exists(self) -> None:
        """The account invalidation root is read-scoped and throttled.

        Given: The WebSocket topic registry.
        When: Looking up the account-state category.
        Then: Its sole root uses the requested 500 ms bridge throttle.
        """
        results = get_topics_by_category("account_state")

        assert len(results) == 1
        assert results[0].pattern == "portfolio.accounts."
        assert results[0].throttle_ms == 500
        assert results[0].throttle_per_topic is True


class TestProcessesAndStrategiesTopics:
    """Tests for the 2026-05-14 process/strategy WS event topic registrations.

    Topic registry, publish-side validator, and ``ProcessLauncherService``
    emit sites all shipped together. These tests pin the registry
    contract; ``test_topic_validation.py`` covers the publish-side
    validator pairings.
    """

    def test_process_summary_topic_in_system_category(self) -> None:
        """``processes.events.summary.`` is in the ``system`` category.

        Given: TOPIC_REGISTRY,
        When: Filtering for the process-summary pattern,
        Then: It sits in the ``system`` category so VIEWER /
              OPERATOR / ADMIN principals all see live status snapshots
              without holding MANAGE_PROCESSES.
        """
        summary = [s for s in TOPIC_REGISTRY if s.pattern == "processes.events.summary."]
        assert len(summary) == 1
        assert summary[0].category == "system"
        assert summary[0].throttle_ms == 500

    def test_process_configured_topic_in_processes_admin_category(self) -> None:
        """``processes.events.configured.`` requires MANAGE_PROCESSES.

        Given: TOPIC_REGISTRY,
        When: Filtering for the configured-set pattern,
        Then: Lives in the ``processes_admin`` category — config
              snapshots are ops-only.
        """
        configured = [s for s in TOPIC_REGISTRY if s.pattern == "processes.events.configured."]
        assert len(configured) == 1
        assert configured[0].category == "processes_admin"

    def test_process_runs_topic_in_processes_admin_category(self) -> None:
        """``processes.events.runs.`` requires MANAGE_PROCESSES."""
        runs = [s for s in TOPIC_REGISTRY if s.pattern == "processes.events.runs."]
        assert len(runs) == 1
        assert runs[0].category == "processes_admin"

    def test_strategy_list_topic_in_strategies_read_category(self) -> None:
        """``strategies.events.list.`` lives in the new ``strategies_read`` category.

        Given: TOPIC_REGISTRY,
        When: Filtering for the strategy-list pattern,
        Then: ``strategies_read`` category gates the topic by
              READ_STRATEGIES alone — VIEWER / OPERATOR / ADMIN see
              the strategy roster without holding START_STRATEGIES.
        """
        strategies = [s for s in TOPIC_REGISTRY if s.pattern == "strategies.events.list."]
        assert len(strategies) == 1
        assert strategies[0].category == "strategies_read"
        assert strategies[0].throttle_ms == 1000

    def test_processes_admin_category_groups_two_topics(self) -> None:
        """``get_topics_by_category('processes_admin')`` returns both ops topics."""
        admin = get_topics_by_category("processes_admin")
        patterns = {schema.pattern for schema in admin}
        assert patterns == {"processes.events.configured.", "processes.events.runs."}
