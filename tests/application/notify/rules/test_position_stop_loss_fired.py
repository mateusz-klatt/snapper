"""Tests for ``PositionStopLossFiredRule`` (§D6.1 Rule 3)."""

from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.application.notify.rules.position_stop_loss_fired import PositionStopLossFiredRule
from snapper.messaging.schemas.data import ExecutionPlanDecisionEventData


def _now() -> datetime:
    """Deterministic timestamp for enrichment reads."""
    return datetime(2026, 4, 24, 12, 0, 0, tzinfo=UTC)


def _decision(reason: str = "sl_hit") -> bytes:
    """Build an ExecutionPlanDecisionEventData JSON payload."""
    event = ExecutionPlanDecisionEventData(
        session_id="s1",
        sequence_id=1,
        public_id="019dbb34-f439-77bd-afa8-ee5321d60307",
        timestamp=_now(),
        decision_public_id="019dbb34-f439-77bd-afa8-ee5321d60308",
        plan_public_id="019dbb34-f439-77bd-afa8-ee5321d60309",
        decision_type="evaluator",
        trigger_type="tick",
        reason=reason,
        triggered_at=_now(),
    )
    return event.to_json().encode("utf-8")


def _plan_row(user_id: str | None = "user-plan", exchange: str = "kraken") -> dict:
    """Minimal ExecutionPlanRow projection for enrichment."""
    return {
        "public_id": "019dbb34-f439-77bd-afa8-ee5321d60309",
        "plan_type": "bracket",
        "created_by_user_id": user_id,
        "created_by_strategy": None,
        "created_via": "api",
        "instrument_public_id": "019dbb34-f439-77bd-afa8-ee5321d6030a",
        "exchange": exchange,
        "mode": "live",
        "shard_key": "s1",
        "wallet_public_id": "wallet-plan",
        "operator_public_id": "op-plan",
        "total_quantity": 1.0,
        "filled_quantity": 0.5,
        "side": "buy",
        "parent_plan_public_id": None,
        "timestamp": _now(),
        "session_id": "s1",
        "sequence_id": 1,
    }


class TestPositionStopLossFiredRule:
    """Fires on sl_hit / trailing_stop_hit; ignores tp_hit + unknown reasons."""

    @pytest.mark.asyncio
    async def test_fires_on_sl_hit_with_enrichment(self) -> None:
        """sl_hit reason produces one alert with native_symbol + exchange in body."""
        rule = PositionStopLossFiredRule()
        repo = MagicMock()
        repo.get_execution_plan = AsyncMock(return_value=_plan_row())
        repo.get_symbol_for_instrument = AsyncMock(return_value="BTC-USD")
        repo.list_alert_events_with_dedup_key = AsyncMock(return_value=[])

        rows = await rule.evaluate(
            "plans.decisions.019dbb34-f439-77bd-afa8-ee5321d60309",
            _decision(reason="sl_hit"),
            repo,
            _now(),
        )

        assert len(rows) == 1
        assert rows[0]["is_safety_critical"] is True
        assert rows[0]["priority"] == "high"
        assert "BTC-USD" in rows[0]["body"]
        assert "kraken" in rows[0]["body"]
        dedup_key = rows[0]["dedup_key"]
        assert dedup_key is not None and dedup_key.startswith("stop_loss.")

    @pytest.mark.asyncio
    async def test_fires_on_trailing_stop_hit(self) -> None:
        """trailing_stop_hit is the second known-loss reason."""
        rule = PositionStopLossFiredRule()
        repo = MagicMock()
        repo.get_execution_plan = AsyncMock(return_value=_plan_row())
        repo.get_symbol_for_instrument = AsyncMock(return_value="BTC-USD")
        repo.list_alert_events_with_dedup_key = AsyncMock(return_value=[])

        rows = await rule.evaluate(
            "plans.decisions.019dbb34-f439-77bd-afa8-ee5321d60309",
            _decision(reason="trailing_stop_hit"),
            repo,
            _now(),
        )

        assert len(rows) == 1

    @pytest.mark.asyncio
    async def test_ignores_tp_hit(self) -> None:
        """Take-profit is a non-loss outcome — rule never fires."""
        rule = PositionStopLossFiredRule()
        repo = MagicMock()

        rows = await rule.evaluate(
            "plans.decisions.019dbb34-f439-77bd-afa8-ee5321d60309",
            _decision(reason="tp_hit"),
            repo,
            _now(),
        )

        assert rows == []

    @pytest.mark.asyncio
    async def test_ignores_unknown_reason(self) -> None:
        """Free-form non-loss reasons (e.g. ``evaluator emitted command``) are dropped."""
        rule = PositionStopLossFiredRule()
        repo = MagicMock()

        rows = await rule.evaluate(
            "plans.decisions.019dbb34-f439-77bd-afa8-ee5321d60309",
            _decision(reason="evaluator emitted command"),
            repo,
            _now(),
        )

        assert rows == []

    @pytest.mark.asyncio
    async def test_drops_when_plan_missing(self) -> None:
        """Plan enrichment returning None drops the alert silently."""
        rule = PositionStopLossFiredRule()
        repo = MagicMock()
        repo.get_execution_plan = AsyncMock(return_value=None)

        rows = await rule.evaluate(
            "plans.decisions.019dbb34-f439-77bd-afa8-ee5321d60309",
            _decision(reason="sl_hit"),
            repo,
            _now(),
        )

        assert rows == []

    @pytest.mark.asyncio
    async def test_drops_when_plan_has_no_user(self) -> None:
        """Strategy-created plans (user_id=None) cannot be addressed."""
        rule = PositionStopLossFiredRule()
        repo = MagicMock()
        repo.get_execution_plan = AsyncMock(return_value=_plan_row(user_id=None))

        rows = await rule.evaluate(
            "plans.decisions.019dbb34-f439-77bd-afa8-ee5321d60309",
            _decision(reason="sl_hit"),
            repo,
            _now(),
        )

        assert rows == []

    @pytest.mark.asyncio
    async def test_falls_back_to_instrument_public_id_when_symbol_missing(self) -> None:
        """Symbol lookup returning None degrades body gracefully."""
        rule = PositionStopLossFiredRule()
        repo = MagicMock()
        repo.get_execution_plan = AsyncMock(return_value=_plan_row())
        repo.get_symbol_for_instrument = AsyncMock(return_value=None)
        repo.list_alert_events_with_dedup_key = AsyncMock(return_value=[])

        rows = await rule.evaluate(
            "plans.decisions.019dbb34-f439-77bd-afa8-ee5321d60309",
            _decision(reason="sl_hit"),
            repo,
            _now(),
        )

        assert len(rows) == 1
        assert "019dbb34" in rows[0]["body"]

    @pytest.mark.asyncio
    async def test_non_decision_payload_dropped(self) -> None:
        """Well-formed non-ExecutionPlanDecisionEventData yields []."""
        from snapper.messaging.schemas.data import TickData

        tick = TickData(
            session_id="s",
            sequence_id=1,
            public_id="pid",
            timestamp=_now(),
            instrument="BTC-USD",
            volume=0.0,
            exchange="kraken",
        )
        rule = PositionStopLossFiredRule()
        repo = MagicMock()

        rows = await rule.evaluate(
            "plans.decisions.019dbb34-f439-77bd-afa8-ee5321d60309",
            tick.to_json().encode("utf-8"),
            repo,
            _now(),
        )

        assert rows == []

    @pytest.mark.asyncio
    async def test_dedup_window_hit_suppresses_fire(self) -> None:
        """Dedup hit (same decision replay) suppresses the second fire."""
        rule = PositionStopLossFiredRule()
        rule.suppression_window_seconds = 300
        repo = MagicMock()
        repo.get_execution_plan = AsyncMock(return_value=_plan_row())
        repo.get_symbol_for_instrument = AsyncMock(return_value="BTC-USD")
        repo.list_alert_events_with_dedup_key = AsyncMock(return_value=[{"public_id": "prior"}])

        rows = await rule.evaluate(
            "plans.decisions.019dbb34-f439-77bd-afa8-ee5321d60309",
            _decision(reason="sl_hit"),
            repo,
            _now(),
        )

        assert rows == []
