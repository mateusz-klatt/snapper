"""Tests for ManualOnceEvaluator."""

from datetime import UTC
from datetime import datetime
from types import SimpleNamespace
from typing import Any
from typing import cast

import pytest

from snapper.application.plans.manual_once import ManualOnceEvaluator
from snapper.data.repository_types import ExecutionPlanRow
from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.schemas.data import TickData


def _make_plan(**overrides: object) -> ExecutionPlanRow:
    """Create a minimal plan row for testing."""
    now = datetime(2026, 4, 10, tzinfo=UTC)
    defaults: dict[str, Any] = {
        "public_id": "plan-1",
        "timestamp": now,
        "session_id": "s1",
        "sequence_id": 1,
        "plan_type": "manual_once",
        "created_by_user_id": "user-1",
        "created_by_strategy": None,
        "created_via": "ui",
        "instrument_public_id": "inst-1",
        "exchange": "kraken",
        "mode": "live",
        "shard_key": "kraken:BTC-USD:live",
        "wallet_public_id": "wallet-1",
        "operator_public_id": None,
        "total_quantity": 1.0,
        "filled_quantity": 0.0,
        "side": "buy",
        "parent_plan_public_id": None,
        "position_cycle_public_id": None,
        "params": {"order_type": "limit", "side": "buy", "price": 50000.0},
        "status": "pending",
        "created_at": now,
        "started_at": None,
        "completed_at": None,
        "expires_at": None,
        "cancel_requested_at": None,
        "last_evaluated_at": None,
        "last_error": None,
        "idempotency_key": None,
    }
    defaults.update(overrides)
    return cast(ExecutionPlanRow, defaults)


def _make_tick() -> TickData:
    """Create a minimal tick for testing."""
    return TickData(
        type="tick",
        instrument="BTC-USD",
        exchange="kraken",
        bid=49999.0,
        ask=50001.0,
        last=50000.0,
        volume=100.0,
        timestamp=datetime(2026, 4, 10, 12, 0, 0, tzinfo=UTC),
        session_id="s1",
        sequence_id=1,
        public_id="t-1",
    )


class TestManualOnceEvaluator:
    """Tests for ManualOnceEvaluator callbacks and validation."""

    @pytest.mark.asyncio
    async def test_on_tick_returns_empty(self) -> None:
        """Manual orders never react to ticks."""
        evaluator = ManualOnceEvaluator()
        result = await evaluator.on_tick(_make_plan(), _make_tick())
        assert result == []

    @pytest.mark.asyncio
    async def test_on_clock_returns_empty(self) -> None:
        """Manual orders never react to clock."""
        evaluator = ManualOnceEvaluator()
        result = await evaluator.on_clock(_make_plan(), datetime(2026, 4, 10, tzinfo=UTC))
        assert result == []

    @pytest.mark.asyncio
    async def test_on_execution_returns_empty(self) -> None:
        """Manual orders do not spawn child commands from fills."""
        evaluator = ManualOnceEvaluator()
        exec_data = cast(ExecutionData, SimpleNamespace())
        result = await evaluator.on_execution(_make_plan(), exec_data)
        assert result == []

    def test_build_checkpoint_state_empty(self) -> None:
        """Manual orders have no state to checkpoint."""
        evaluator = ManualOnceEvaluator()
        assert evaluator.build_checkpoint_state(_make_plan()) == {}

    def test_restore_from_checkpoint_noop(self) -> None:
        """Restore is a no-op — does not raise."""
        evaluator = ManualOnceEvaluator()
        evaluator.restore_from_checkpoint(_make_plan(), {"any": "data"})

    def test_validate_params_valid_limit(self) -> None:
        """Valid limit order params accepted."""
        evaluator = ManualOnceEvaluator()
        evaluator.validate_params({"order_type": "limit", "side": "buy", "price": 50000.0})

    def test_validate_params_valid_market(self) -> None:
        """Valid market order params accepted."""
        evaluator = ManualOnceEvaluator()
        evaluator.validate_params({"order_type": "market", "side": "sell"})

    def test_validate_params_valid_stop(self) -> None:
        """Valid stop order params accepted."""
        evaluator = ManualOnceEvaluator()
        evaluator.validate_params({"order_type": "stop", "side": "sell", "stop_price": 48000.0})

    def test_validate_params_valid_stop_limit(self) -> None:
        """Valid stop_limit order params accepted."""
        evaluator = ManualOnceEvaluator()
        evaluator.validate_params(
            {
                "order_type": "stop_limit",
                "side": "buy",
                "price": 50000.0,
                "stop_price": 49000.0,
            }
        )

    def test_validate_params_missing_required(self) -> None:
        """Missing required params raises ValueError."""
        evaluator = ManualOnceEvaluator()
        with pytest.raises(ValueError, match="Missing required params"):
            evaluator.validate_params({"price": 50000.0})

    def test_validate_params_invalid_order_type(self) -> None:
        """Invalid order_type raises ValueError."""
        evaluator = ManualOnceEvaluator()
        with pytest.raises(ValueError, match="Invalid order_type"):
            evaluator.validate_params({"order_type": "twap", "side": "buy"})

    def test_validate_params_limit_missing_price(self) -> None:
        """Limit order without price raises ValueError."""
        evaluator = ManualOnceEvaluator()
        with pytest.raises(ValueError, match="requires price"):
            evaluator.validate_params({"order_type": "limit", "side": "buy"})

    def test_validate_params_stop_missing_stop_price(self) -> None:
        """Stop order without stop_price raises ValueError."""
        evaluator = ManualOnceEvaluator()
        with pytest.raises(ValueError, match="requires stop_price"):
            evaluator.validate_params({"order_type": "stop", "side": "sell"})

    def test_validate_params_market_with_price_rejected(self) -> None:
        """A market order carrying a price raises ValueError.

        Given: market params whose ``price`` is set,
        When: ``validate_params`` runs,
        Then: it raises ValueError matching "must not carry price"; on the
            paper venue the fill reference is resolved server-side and a
            caller-supplied price would desynchronize the plan, command,
            and caps views of the order.
        """
        evaluator = ManualOnceEvaluator()
        with pytest.raises(ValueError, match="must not carry price"):
            evaluator.validate_params({"order_type": "market", "side": "buy", "price": 50000.0})

    def test_validate_params_market_explicit_none_price_accepted(self) -> None:
        """A market order with ``price`` explicitly None is accepted.

        Given: market params carrying ``price=None`` explicitly,
        When: ``validate_params`` runs,
        Then: it returns without raising — only a non-None price is
            rejected for market orders.
        """
        evaluator = ManualOnceEvaluator()
        evaluator.validate_params({"order_type": "market", "side": "sell", "price": None})

    def test_requires_capabilities_empty(self) -> None:
        """Manual orders have no special capability requirements."""
        evaluator = ManualOnceEvaluator()
        assert evaluator.requires_capabilities() == []
