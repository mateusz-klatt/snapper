"""Tests for BracketEvaluator."""

from datetime import UTC
from datetime import datetime

import pytest

from snapper.application.plans.bracket import BracketEvaluator
from snapper.application.plans.bracket import _triggered_leg_for_price
from snapper.core.json_types import JsonObject
from snapper.data.repository_types import ExecutionPlanRow
from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.schemas.data import TickData


def _make_bracket_plan(
    side: str = "buy",
    status: str = "armed",
    sl_price: float | None = 48000.0,
    tp_price: float | None = 52000.0,
    total_quantity: float = 1.0,
) -> ExecutionPlanRow:
    """Create a minimal bracket plan row for testing."""
    now = datetime(2026, 4, 12, tzinfo=UTC)
    params: JsonObject = {"native_instrument": "BTC-USD"}
    if sl_price is not None:
        params["sl_price"] = sl_price
    if tp_price is not None:
        params["tp_price"] = tp_price
    return ExecutionPlanRow(
        public_id="bracket-1",
        timestamp=now,
        session_id="s1",
        sequence_id=1,
        plan_type="bracket",
        created_by_user_id=None,
        created_by_strategy=None,
        created_via="ui",
        instrument_public_id="inst-1",
        exchange="kraken_futures",
        mode="paper",
        shard_key="kraken_futures:BTC-USD:paper",
        wallet_public_id="wallet-1",
        operator_public_id=None,
        total_quantity=total_quantity,
        filled_quantity=0.0,
        side=side,
        parent_plan_public_id=None,
        position_cycle_public_id="cycle-1",
        params=params,
        status=status,
        created_at=now,
        started_at=None,
        completed_at=None,
        expires_at=None,
        cancel_requested_at=None,
        last_evaluated_at=None,
        last_error=None,
        idempotency_key=None,
        cancel_idempotency_key=None,
    )


def _make_tick(last: float | None = 50000.0) -> TickData:
    """Create a minimal TickData for testing."""
    return TickData(
        public_id="t-1",
        timestamp=datetime(2026, 4, 12, tzinfo=UTC),
        session_id="s1",
        sequence_id=1,
        instrument="BTC-USD",
        exchange="kraken_futures",
        volume=0.0,
        bid=49999.0,
        ask=50001.0,
        last=last,
    )


class TestBracketOnTick:
    """Tests for BracketEvaluator.on_tick trigger logic."""

    @pytest.mark.asyncio
    async def test_long_sl_triggers_at_or_below(self) -> None:
        """Long position SL triggers when last <= sl_price."""
        evaluator = BracketEvaluator()
        plan = _make_bracket_plan(side="buy", sl_price=48000.0, tp_price=52000.0)
        commands = await evaluator.on_tick(plan, _make_tick(last=47999.0))
        assert len(commands) == 1
        assert commands[0]["side"] == "sell"
        assert commands[0]["reduce_only"] is True
        assert commands[0]["reason"] == "sl_hit"

    @pytest.mark.asyncio
    async def test_long_sl_triggers_at_exact_price(self) -> None:
        """Long SL triggers at exact sl_price (<=, not <)."""
        evaluator = BracketEvaluator()
        plan = _make_bracket_plan(side="buy", sl_price=48000.0)
        commands = await evaluator.on_tick(plan, _make_tick(last=48000.0))
        assert len(commands) == 1
        assert commands[0]["reason"] == "sl_hit"

    @pytest.mark.asyncio
    async def test_long_tp_triggers_at_or_above(self) -> None:
        """Long position TP triggers when last >= tp_price."""
        evaluator = BracketEvaluator()
        plan = _make_bracket_plan(side="buy", sl_price=48000.0, tp_price=52000.0)
        commands = await evaluator.on_tick(plan, _make_tick(last=52001.0))
        assert len(commands) == 1
        assert commands[0]["side"] == "sell"
        assert commands[0]["reason"] == "tp_hit"

    @pytest.mark.asyncio
    async def test_long_tp_triggers_at_exact_price(self) -> None:
        """Long TP triggers at exact tp_price (>=, not >)."""
        evaluator = BracketEvaluator()
        plan = _make_bracket_plan(side="buy", tp_price=52000.0)
        commands = await evaluator.on_tick(plan, _make_tick(last=52000.0))
        assert len(commands) == 1
        assert commands[0]["reason"] == "tp_hit"

    @pytest.mark.asyncio
    async def test_short_sl_triggers_at_or_above(self) -> None:
        """Short position SL triggers when last >= sl_price."""
        evaluator = BracketEvaluator()
        plan = _make_bracket_plan(side="sell", sl_price=52000.0, tp_price=48000.0)
        commands = await evaluator.on_tick(plan, _make_tick(last=52001.0))
        assert len(commands) == 1
        assert commands[0]["side"] == "buy"
        assert commands[0]["reason"] == "sl_hit"

    @pytest.mark.asyncio
    async def test_short_tp_triggers_at_or_below(self) -> None:
        """Short position TP triggers when last <= tp_price."""
        evaluator = BracketEvaluator()
        plan = _make_bracket_plan(side="sell", sl_price=52000.0, tp_price=48000.0)
        commands = await evaluator.on_tick(plan, _make_tick(last=47999.0))
        assert len(commands) == 1
        assert commands[0]["side"] == "buy"
        assert commands[0]["reason"] == "tp_hit"

    @pytest.mark.asyncio
    async def test_sl_only_bracket_triggers(self) -> None:
        """SL-only bracket (no TP) triggers correctly."""
        evaluator = BracketEvaluator()
        plan = _make_bracket_plan(side="buy", sl_price=48000.0, tp_price=None)
        commands = await evaluator.on_tick(plan, _make_tick(last=47000.0))
        assert len(commands) == 1
        assert commands[0]["reason"] == "sl_hit"

    @pytest.mark.asyncio
    async def test_tp_only_bracket_triggers(self) -> None:
        """TP-only bracket (no SL) triggers correctly."""
        evaluator = BracketEvaluator()
        plan = _make_bracket_plan(side="buy", sl_price=None, tp_price=52000.0)
        commands = await evaluator.on_tick(plan, _make_tick(last=53000.0))
        assert len(commands) == 1
        assert commands[0]["reason"] == "tp_hit"

    @pytest.mark.asyncio
    async def test_no_trigger_in_range(self) -> None:
        """Price between SL and TP does not trigger."""
        evaluator = BracketEvaluator()
        plan = _make_bracket_plan(side="buy", sl_price=48000.0, tp_price=52000.0)
        commands = await evaluator.on_tick(plan, _make_tick(last=50000.0))
        assert commands == []

    @pytest.mark.asyncio
    async def test_short_no_trigger_in_range(self) -> None:
        """Short position: price between TP and SL does not trigger."""
        evaluator = BracketEvaluator()
        plan = _make_bracket_plan(side="sell", sl_price=52000.0, tp_price=48000.0)
        commands = await evaluator.on_tick(plan, _make_tick(last=50000.0))
        assert commands == []

    @pytest.mark.asyncio
    async def test_last_none_returns_empty(self) -> None:
        """tick.last is None → no trigger, no error."""
        evaluator = BracketEvaluator()
        plan = _make_bracket_plan()
        commands = await evaluator.on_tick(plan, _make_tick(last=None))
        assert commands == []

    @pytest.mark.asyncio
    async def test_non_armed_status_returns_empty(self) -> None:
        """Non-armed plan does not evaluate ticks."""
        evaluator = BracketEvaluator()
        plan = _make_bracket_plan(status="active")
        commands = await evaluator.on_tick(plan, _make_tick(last=47000.0))
        assert commands == []

    @pytest.mark.asyncio
    async def test_command_has_correct_fields(self) -> None:
        """Emitted command has all required TradeCommand fields."""
        evaluator = BracketEvaluator()
        plan = _make_bracket_plan(side="buy", sl_price=48000.0, total_quantity=2.5)
        commands = await evaluator.on_tick(plan, _make_tick(last=47000.0))
        cmd = commands[0]
        assert cmd["command_type"] == "create"
        assert cmd["instrument"] == "BTC-USD"
        assert cmd["side"] == "sell"
        assert cmd["order_type"] == "market"
        assert cmd["quantity"] == 2.5
        assert cmd["price"] is None
        assert cmd["reduce_only"] is True


class TestTriggeredLegForPrice:
    """Tests for the extracted trigger-leg helper."""

    @pytest.mark.parametrize(
        ("side", "stop_loss", "take_profit", "last_price", "expected"),
        [
            ("buy", 48_000.0, 52_000.0, 47_999.0, "sl_hit"),
            ("buy", 48_000.0, 52_000.0, 52_001.0, "tp_hit"),
            ("sell", 52_000.0, 48_000.0, 52_001.0, "sl_hit"),
            ("sell", 52_000.0, 48_000.0, 47_999.0, "tp_hit"),
            ("buy", 48_000.0, 52_000.0, 50_000.0, None),
        ],
    )
    def test_returns_expected_leg(
        self,
        side: str,
        stop_loss: float | None,
        take_profit: float | None,
        last_price: float,
        expected: str | None,
    ) -> None:
        """Helper returns the expected trigger result for each side/price case."""
        assert _triggered_leg_for_price(side, stop_loss, take_profit, last_price) == expected


class TestBracketOnExecution:
    """Tests for BracketEvaluator.on_execution."""

    @pytest.mark.asyncio
    async def test_returns_empty(self) -> None:
        """Brackets do not emit commands on fills."""
        evaluator = BracketEvaluator()
        plan = _make_bracket_plan()
        execution = ExecutionData(
            public_id="exec-1",
            timestamp=datetime(2026, 4, 12, tzinfo=UTC),
            session_id="s1",
            sequence_id=1,
            client_order_id="cid-1",
            instrument="BTC-USD",
            exchange="kraken_futures",
            side="sell",
            size=1.0,
            price=47000.0,
            last_size=1.0,
            last_price=47000.0,
            fee=0.0,
            fee_asset="USD",
            status="filled",
            executed_at=datetime(2026, 4, 12, tzinfo=UTC),
        )
        commands = await evaluator.on_execution(plan, execution)
        assert commands == []


class TestBracketOnClock:
    """Tests for BracketEvaluator.on_clock."""

    @pytest.mark.asyncio
    async def test_returns_empty(self) -> None:
        """Brackets are tick-driven, clock is a no-op."""
        evaluator = BracketEvaluator()
        plan = _make_bracket_plan()
        commands = await evaluator.on_clock(plan, datetime.now(UTC))
        assert commands == []


class TestBracketCheckpoint:
    """Tests for bracket checkpoint (stateless)."""

    def test_build_returns_empty(self) -> None:
        """Stateless bracket has no checkpoint state."""
        evaluator = BracketEvaluator()
        plan = _make_bracket_plan()
        assert evaluator.build_checkpoint_state(plan) == {}

    def test_restore_is_noop(self) -> None:
        """Restore does nothing for stateless bracket."""
        evaluator = BracketEvaluator()
        plan = _make_bracket_plan()
        evaluator.restore_from_checkpoint(plan, {"ignored": True})


class TestBracketValidateParams:
    """Tests for BracketEvaluator.validate_params."""

    def test_valid_both_legs(self) -> None:
        """Both SL and TP present passes validation."""
        evaluator = BracketEvaluator()
        evaluator.validate_params(
            {"native_instrument": "BTC-USD", "sl_price": 48000.0, "tp_price": 52000.0}
        )

    def test_valid_sl_only(self) -> None:
        """SL-only passes validation."""
        evaluator = BracketEvaluator()
        evaluator.validate_params({"native_instrument": "BTC-USD", "sl_price": 48000.0})

    def test_valid_tp_only(self) -> None:
        """TP-only passes validation."""
        evaluator = BracketEvaluator()
        evaluator.validate_params({"native_instrument": "BTC-USD", "tp_price": 52000.0})

    def test_rejects_no_legs(self) -> None:
        """Both legs missing raises ValueError."""
        evaluator = BracketEvaluator()
        with pytest.raises(ValueError, match="At least one"):
            evaluator.validate_params({"native_instrument": "BTC-USD"})

    def test_rejects_missing_native_instrument(self) -> None:
        """Missing native_instrument raises ValueError."""
        evaluator = BracketEvaluator()
        with pytest.raises(ValueError, match="native_instrument"):
            evaluator.validate_params({"sl_price": 48000.0})


class TestBracketRequiresCapabilities:
    """Tests for BracketEvaluator.requires_capabilities."""

    def test_requires_reduce_only(self) -> None:
        """Bracket requires supports_reduce_only (Decision C1)."""
        evaluator = BracketEvaluator()
        assert evaluator.requires_capabilities() == ["supports_reduce_only"]
