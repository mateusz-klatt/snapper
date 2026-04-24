"""Tests for ``AlertRule`` ABC + ``RuleRegistry`` + ``load_default_registry``."""

from datetime import UTC
from datetime import datetime
from typing import cast

import pytest

from snapper.application.notify.rules.base import AlertRule
from snapper.application.notify.rules.base import RuleRegistry
from snapper.application.notify.rules.registry_factory import load_default_registry
from snapper.data.repository import Repository
from snapper.data.repository_types import AlertEventInsertRow


class _FixedRule(AlertRule):
    """Minimal concrete rule for registry tests — never emits."""

    def __init__(self, alert_type: str, prefixes: tuple[str, ...]) -> None:
        self.alert_type = alert_type
        self.subscribe_topic_prefixes = prefixes
        self.priority = "medium"
        self.is_safety_critical = False
        self.thread_key_prefix = "test"
        self.suppression_window_seconds = 0

    async def evaluate(
        self,
        topic: str,
        payload: bytes,
        repo: Repository,
        now: datetime,
    ) -> list[AlertEventInsertRow]:
        return []


class TestRuleRegistry:
    """Covers registration + prefix union + longest-match dispatch."""

    def test_empty_registry_has_no_prefixes(self) -> None:
        """Fresh registry returns empty prefix tuple."""
        reg = RuleRegistry()
        assert reg.all_subscribe_prefixes() == ()

    def test_prefixes_preserve_registration_order_without_duplicates(self) -> None:
        """Union across rules is order-preserving + distinct."""
        reg = RuleRegistry()
        reg.register(_FixedRule("a", ("orders.events.",)))
        reg.register(_FixedRule("b", ("orders.events.", "plans.decisions.")))
        reg.register(_FixedRule("c", ("system.heartbeats.",)))

        prefixes = reg.all_subscribe_prefixes()

        assert prefixes == ("orders.events.", "plans.decisions.", "system.heartbeats.")

    def test_longest_match_dispatches_to_most_specific_rule(self) -> None:
        """A rule with a longer matching prefix wins over a shorter one."""
        reg = RuleRegistry()
        broad = _FixedRule("broad", ("orders.",))
        specific = _FixedRule("specific", ("orders.events.",))
        reg.register(broad)
        reg.register(specific)

        matches = reg.get_longest_match("orders.events.kraken.BTC-USD.executed")

        assert matches == [specific]

    def test_tie_on_prefix_length_preserves_registration_order(self) -> None:
        """Two rules sharing the same best prefix are returned in registration order."""
        reg = RuleRegistry()
        first = _FixedRule("first", ("orders.events.",))
        second = _FixedRule("second", ("orders.events.",))
        reg.register(first)
        reg.register(second)

        matches = reg.get_longest_match("orders.events.kraken.BTC-USD.rejected")

        assert matches == [first, second]

    def test_unmatched_topic_returns_empty(self) -> None:
        """A topic no registered prefix covers yields []."""
        reg = RuleRegistry()
        reg.register(_FixedRule("x", ("orders.events.",)))

        assert reg.get_longest_match("market.kraken.BTC-USD.ticks") == []

    def test_rule_with_shorter_match_is_skipped_when_longer_winner_exists(self) -> None:
        """Registering a longer-prefix winner first + shorter-prefix later keeps only longer."""
        reg = RuleRegistry()
        reg.register(_FixedRule("specific", ("orders.events.",)))
        reg.register(_FixedRule("broad", ("orders.",)))

        matches = reg.get_longest_match("orders.events.kraken.BTC-USD.executed")

        assert [r.alert_type for r in matches] == ["specific"]


class TestLoadDefaultRegistry:
    """Verifies the default v1.11 registry contract."""

    def test_default_registry_has_four_rules(self) -> None:
        """load_default_registry wires all 4 P0 rules (margin_warning deferred)."""
        reg = load_default_registry()

        alert_types = {rule.alert_type for rule in reg._rules}
        assert alert_types == {
            "order_fill_full",
            "order_rejected",
            "position_stop_loss_fired",
            "critical_system_error",
        }

    def test_default_prefixes_cover_all_three_topic_families(self) -> None:
        """The aggregated subscribe-prefix set matches the sidecar's expected subscriptions."""
        reg = load_default_registry()

        assert set(reg.all_subscribe_prefixes()) == {
            "orders.events.",
            "plans.decisions.",
            "system.heartbeats.",
        }

    @pytest.mark.asyncio
    async def test_default_rules_do_not_raise_on_unrelated_topic(self) -> None:
        """Every rule handles an unrelated / malformed topic gracefully."""
        from unittest.mock import AsyncMock
        from unittest.mock import MagicMock

        reg = load_default_registry()
        now = datetime(2026, 4, 24, 12, tzinfo=UTC)
        fake_repo = MagicMock()
        fake_repo.list_alert_events_with_dedup_key = AsyncMock(return_value=[])

        for rule in reg._rules:
            out = await rule.evaluate(
                "market.kraken.ticks", b"{}", cast(Repository, fake_repo), now
            )
            assert out == []
