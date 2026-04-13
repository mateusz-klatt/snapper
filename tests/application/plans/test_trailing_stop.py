"""Tests for TrailingStopEvaluator — trailing stop orders on position cycles."""

from datetime import UTC
from datetime import datetime
from typing import Any

import pytest

from snapper.application.plans.trailing_stop import TrailingStopEvaluator
from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.schemas.data import TickData

NOW = datetime(2026, 4, 13, tzinfo=UTC)


def _make_plan(
    public_id: str = "plan-1",
    side: str = "buy",
    trailing_pct: float = 5.0,
    min_lock_pct: float = 0.0,
    entry_price: float = 100.0,
    status: str = "armed",
    total_quantity: float = 1.0,
) -> dict[str, Any]:
    """Build an ExecutionPlanRow dict for trailing stop tests."""
    return {
        "public_id": public_id,
        "timestamp": NOW,
        "session_id": "s1",
        "sequence_id": 1,
        "plan_type": "trailing_stop",
        "created_by_user_id": None,
        "created_by_strategy": None,
        "created_via": "api",
        "instrument_public_id": "inst-btc",
        "exchange": "kraken_futures",
        "mode": "live",
        "shard_key": "kraken_futures.BTC-USD.live",
        "wallet_public_id": "wallet-1",
        "operator_public_id": None,
        "total_quantity": total_quantity,
        "filled_quantity": 0.0,
        "side": side,
        "parent_plan_public_id": None,
        "position_cycle_public_id": "cycle-1",
        "params": {
            "trailing_pct": trailing_pct,
            "min_lock_pct": min_lock_pct,
            "entry_price": entry_price,
            "native_instrument": "PF_BTCUSD",
            "leverage": 5,
        },
        "status": status,
        "created_at": NOW,
        "started_at": None,
        "completed_at": None,
        "expires_at": None,
        "cancel_requested_at": None,
        "last_evaluated_at": None,
        "last_error": None,
        "idempotency_key": None,
    }


def _make_tick(last: float | None = 100.0) -> TickData:
    """Build a TickData with a given last price."""
    return TickData(
        public_id="tick-1",
        timestamp=NOW,
        session_id="s1",
        sequence_id=1,
        instrument="BTC-USD",
        exchange="kraken_futures",
        volume=100.0,
        last=last,
    )


class TestLongTrailingStop:
    """Tests for trailing stop on long positions (buy side)."""

    @pytest.mark.asyncio
    async def test_long_ratchets_and_triggers(self) -> None:
        """Long: price 100->120, trailing 5% -> stop 114, drop to 113 -> triggers.

        Given: long position, entry=100, trailing_pct=5, min_lock=0,
        When: price rises to 120, then drops to 113,
        Then: stop triggers at 113.
        """
        ev = TrailingStopEvaluator()
        plan = _make_plan(entry_price=100, trailing_pct=5.0)

        cmds = await ev.on_tick(plan, _make_tick(100.0))
        assert cmds == []
        cmds = await ev.on_tick(plan, _make_tick(110.0))
        assert cmds == []
        cmds = await ev.on_tick(plan, _make_tick(120.0))
        assert cmds == []

        state = ev._state["plan-1"]
        assert state["peak_price"] == pytest.approx(120.0)
        assert state["current_stop"] == pytest.approx(114.0)

        cmds = await ev.on_tick(plan, _make_tick(115.0))
        assert cmds == []

        cmds = await ev.on_tick(plan, _make_tick(113.0))
        assert len(cmds) == 1
        assert cmds[0]["reason"] == "trailing_stop_hit"
        assert cmds[0]["side"] == "sell"
        assert cmds[0]["reduce_only"] is True
        assert cmds[0]["trigger_price"] == pytest.approx(113.0)

    @pytest.mark.asyncio
    async def test_long_min_lock_not_reached(self) -> None:
        """Long: min_lock_pct=5%, entry=100, price 103 -> no trailing.

        Given: min_lock_pct=5, so trailing starts at 105+,
        When: price only reaches 103,
        Then: current_stop stays 0 (not activated).
        """
        ev = TrailingStopEvaluator()
        plan = _make_plan(entry_price=100, trailing_pct=5.0, min_lock_pct=5.0)

        await ev.on_tick(plan, _make_tick(101.0))
        await ev.on_tick(plan, _make_tick(103.0))

        state = ev._state["plan-1"]
        assert state["peak_price"] == pytest.approx(103.0)
        assert state["current_stop"] == pytest.approx(0.0)

    @pytest.mark.asyncio
    async def test_long_min_lock_reached(self) -> None:
        """Long: min_lock=5%, entry=100, price 106 -> trailing starts, stop at 100.7.

        Given: min_lock_pct=5, trailing_pct=5,
        When: price rises to 106 (>105 threshold),
        Then: trailing starts, stop = 106 * 0.95 = 100.7.
        """
        ev = TrailingStopEvaluator()
        plan = _make_plan(entry_price=100, trailing_pct=5.0, min_lock_pct=5.0)

        await ev.on_tick(plan, _make_tick(103.0))
        assert ev._state["plan-1"]["current_stop"] == pytest.approx(0.0)

        await ev.on_tick(plan, _make_tick(106.0))
        assert ev._state["plan-1"]["current_stop"] == pytest.approx(100.7)


class TestShortTrailingStop:
    """Tests for trailing stop on short positions (sell side)."""

    @pytest.mark.asyncio
    async def test_short_ratchets_and_triggers(self) -> None:
        """Short: price 100->80, trailing 5% -> stop 84, rise to 85 -> triggers.

        Given: short position, entry=100, trailing_pct=5, min_lock=0,
        When: price drops to 80, then rises to 85,
        Then: stop triggers at 85.
        """
        ev = TrailingStopEvaluator()
        plan = _make_plan(side="sell", entry_price=100, trailing_pct=5.0)

        await ev.on_tick(plan, _make_tick(100.0))
        await ev.on_tick(plan, _make_tick(90.0))
        await ev.on_tick(plan, _make_tick(80.0))

        state = ev._state["plan-1"]
        assert state["peak_price"] == pytest.approx(80.0)
        assert state["current_stop"] == pytest.approx(84.0)

        cmds = await ev.on_tick(plan, _make_tick(83.0))
        assert cmds == []

        cmds = await ev.on_tick(plan, _make_tick(85.0))
        assert len(cmds) == 1
        assert cmds[0]["reason"] == "trailing_stop_hit"
        assert cmds[0]["side"] == "buy"

    @pytest.mark.asyncio
    async def test_short_min_lock_not_reached(self) -> None:
        """Short: min_lock_pct=5%, entry=100, price 97 -> no trailing.

        Given: min_lock_pct=5, so trailing starts at 95-,
        When: price only drops to 97,
        Then: current_stop stays 0.
        """
        ev = TrailingStopEvaluator()
        plan = _make_plan(side="sell", entry_price=100, trailing_pct=5.0, min_lock_pct=5.0)

        await ev.on_tick(plan, _make_tick(99.0))
        await ev.on_tick(plan, _make_tick(97.0))

        assert ev._state["plan-1"]["current_stop"] == pytest.approx(0.0)

    @pytest.mark.asyncio
    async def test_short_min_lock_reached(self) -> None:
        """Short: min_lock=5%, entry=100, price 94 -> trailing starts, stop at 98.7.

        Given: min_lock_pct=5, trailing_pct=5,
        When: price drops to 94 (<95 threshold),
        Then: stop = 94 * 1.05 = 98.7.
        """
        ev = TrailingStopEvaluator()
        plan = _make_plan(side="sell", entry_price=100, trailing_pct=5.0, min_lock_pct=5.0)

        await ev.on_tick(plan, _make_tick(97.0))
        assert ev._state["plan-1"]["current_stop"] == pytest.approx(0.0)

        await ev.on_tick(plan, _make_tick(94.0))
        assert ev._state["plan-1"]["current_stop"] == pytest.approx(98.7)


class TestEdgeCases:
    """Tests for edge cases and no-op paths."""

    @pytest.mark.asyncio
    async def test_tick_last_none_returns_empty(self) -> None:
        """tick.last is None -> empty commands."""
        ev = TrailingStopEvaluator()
        plan = _make_plan()
        cmds = await ev.on_tick(plan, _make_tick(last=None))
        assert cmds == []

    @pytest.mark.asyncio
    async def test_short_first_tick_above_entry_floors_peak(self) -> None:
        """Short: first tick above entry floors peak at entry.

        Given: short, entry=100, first tick at 110 (adverse move),
        When: on_tick called,
        Then: peak is min(110, 100) = 100, not 110.
        """
        ev = TrailingStopEvaluator()
        plan = _make_plan(side="sell", entry_price=100, trailing_pct=5.0)
        await ev.on_tick(plan, _make_tick(110.0))
        assert ev._state["plan-1"]["peak_price"] == pytest.approx(100.0)
        assert ev._state["plan-1"]["current_stop"] == pytest.approx(105.0)

    @pytest.mark.asyncio
    async def test_long_first_tick_below_entry_floors_peak(self) -> None:
        """Long: first tick below entry floors peak at entry.

        Given: long, entry=100, first tick at 90 (adverse move),
        When: on_tick called,
        Then: peak is max(90, 100) = 100, not 90.
        """
        ev = TrailingStopEvaluator()
        plan = _make_plan(side="buy", entry_price=100, trailing_pct=5.0)
        await ev.on_tick(plan, _make_tick(90.0))
        assert ev._state["plan-1"]["peak_price"] == pytest.approx(100.0)
        assert ev._state["plan-1"]["current_stop"] == pytest.approx(95.0)

    @pytest.mark.asyncio
    async def test_non_armed_returns_empty(self) -> None:
        """Non-armed status -> empty commands."""
        ev = TrailingStopEvaluator()
        plan = _make_plan(status="active")
        cmds = await ev.on_tick(plan, _make_tick(50.0))
        assert cmds == []

    @pytest.mark.asyncio
    async def test_on_execution_returns_empty(self) -> None:
        """on_execution always returns empty list."""
        ev = TrailingStopEvaluator()
        plan = _make_plan()
        execution = ExecutionData(
            public_id="exec-1",
            timestamp=NOW,
            session_id="s1",
            sequence_id=1,
            client_order_id="cid-1",
            instrument="BTC-USD",
            exchange="kraken_futures",
            side="buy",
            size=1.0,
            price=100.0,
            last_size=1.0,
            last_price=100.0,
            fee=0.1,
            fee_asset="USD",
            status="filled",
            executed_at=NOW,
        )
        cmds = await ev.on_execution(plan, execution)
        assert cmds == []

    @pytest.mark.asyncio
    async def test_on_clock_returns_empty(self) -> None:
        """on_clock always returns empty list."""
        ev = TrailingStopEvaluator()
        plan = _make_plan()
        cmds = await ev.on_clock(plan, NOW)
        assert cmds == []


class TestCheckpoint:
    """Tests for checkpoint save/restore."""

    @pytest.mark.asyncio
    async def test_checkpoint_round_trip(self) -> None:
        """Checkpoint save/restore preserves peak_price and current_stop.

        Given: evaluator with state after some ticks,
        When: build_checkpoint_state then restore_from_checkpoint on new evaluator,
        Then: state matches.
        """
        ev = TrailingStopEvaluator()
        plan = _make_plan(entry_price=100, trailing_pct=5.0)
        await ev.on_tick(plan, _make_tick(120.0))

        checkpoint = ev.build_checkpoint_state(plan)
        assert checkpoint["peak_price"] == pytest.approx(120.0)
        assert checkpoint["current_stop"] == pytest.approx(114.0)

        ev2 = TrailingStopEvaluator()
        ev2.restore_from_checkpoint(plan, checkpoint)
        assert ev2._state["plan-1"]["peak_price"] == pytest.approx(120.0)
        assert ev2._state["plan-1"]["current_stop"] == pytest.approx(114.0)

    @pytest.mark.asyncio
    async def test_restore_floors_peak_at_entry_long(self) -> None:
        """Long: restore_from_checkpoint floors peak at entry_price.

        Given: checkpoint with peak=50, entry=100 (stale checkpoint from crash),
        When: restore is called,
        Then: peak is floored to max(50, 100) = 100.
        """
        ev = TrailingStopEvaluator()
        plan = _make_plan(side="buy", entry_price=100)
        ev.restore_from_checkpoint(plan, {"peak_price": 50.0, "current_stop": 0.0})
        assert ev._state["plan-1"]["peak_price"] == pytest.approx(100.0)

    @pytest.mark.asyncio
    async def test_restore_floors_peak_at_entry_short(self) -> None:
        """Short: restore_from_checkpoint floors peak at entry_price.

        Given: checkpoint with peak=150, entry=100 (stale: peak should be lower),
        When: restore is called,
        Then: peak is floored to min(150, 100) = 100.
        """
        ev = TrailingStopEvaluator()
        plan = _make_plan(side="sell", entry_price=100)
        ev.restore_from_checkpoint(plan, {"peak_price": 150.0, "current_stop": 0.0})
        assert ev._state["plan-1"]["peak_price"] == pytest.approx(100.0)

    @pytest.mark.asyncio
    async def test_restore_short_zero_peak_uses_entry(self) -> None:
        """Short: zero peak in checkpoint -> entry_price used.

        Given: checkpoint with peak=0 (never ticked),
        When: restore is called for short,
        Then: peak set to entry_price.
        """
        ev = TrailingStopEvaluator()
        plan = _make_plan(side="sell", entry_price=100)
        ev.restore_from_checkpoint(plan, {"peak_price": 0.0, "current_stop": 0.0})
        assert ev._state["plan-1"]["peak_price"] == pytest.approx(100.0)

    @pytest.mark.asyncio
    async def test_stale_checkpoint_first_tick_updates_peak(self) -> None:
        """Stale checkpoint -> first tick updates peak correctly.

        Given: restored peak=100, actual market at 130,
        When: first tick at 130 arrives,
        Then: peak updates to 130, stop ratchets.
        """
        ev = TrailingStopEvaluator()
        plan = _make_plan(side="buy", entry_price=100, trailing_pct=5.0)
        ev.restore_from_checkpoint(plan, {"peak_price": 100.0, "current_stop": 0.0})

        await ev.on_tick(plan, _make_tick(130.0))
        assert ev._state["plan-1"]["peak_price"] == pytest.approx(130.0)
        assert ev._state["plan-1"]["current_stop"] == pytest.approx(123.5)

    def test_build_checkpoint_no_state(self) -> None:
        """build_checkpoint_state with no prior ticks returns zeros."""
        ev = TrailingStopEvaluator()
        plan = _make_plan()
        checkpoint = ev.build_checkpoint_state(plan)
        assert checkpoint["peak_price"] == pytest.approx(0.0)
        assert checkpoint["current_stop"] == pytest.approx(0.0)


class TestValidateParams:
    """Tests for validate_params validation rules."""

    def test_missing_trailing_pct(self) -> None:
        """Missing trailing_pct raises ValueError."""
        ev = TrailingStopEvaluator()
        with pytest.raises(ValueError, match="trailing_pct required"):
            ev.validate_params({"native_instrument": "X", "entry_price": 100})

    def test_missing_native_instrument(self) -> None:
        """Missing native_instrument raises ValueError."""
        ev = TrailingStopEvaluator()
        with pytest.raises(ValueError, match="native_instrument required"):
            ev.validate_params({"trailing_pct": 5, "entry_price": 100})

    def test_missing_entry_price(self) -> None:
        """Missing entry_price raises ValueError."""
        ev = TrailingStopEvaluator()
        with pytest.raises(ValueError, match="entry_price required"):
            ev.validate_params({"trailing_pct": 5, "native_instrument": "X"})

    def test_trailing_pct_zero(self) -> None:
        """trailing_pct=0 raises ValueError."""
        ev = TrailingStopEvaluator()
        with pytest.raises(ValueError, match="trailing_pct must be between"):
            ev.validate_params({"trailing_pct": 0, "native_instrument": "X", "entry_price": 100})

    def test_trailing_pct_100(self) -> None:
        """trailing_pct=100 raises ValueError."""
        ev = TrailingStopEvaluator()
        with pytest.raises(ValueError, match="trailing_pct must be between"):
            ev.validate_params({"trailing_pct": 100, "native_instrument": "X", "entry_price": 100})

    def test_trailing_pct_negative(self) -> None:
        """trailing_pct=-1 raises ValueError."""
        ev = TrailingStopEvaluator()
        with pytest.raises(ValueError, match="trailing_pct must be between"):
            ev.validate_params({"trailing_pct": -1, "native_instrument": "X", "entry_price": 100})

    def test_entry_price_zero(self) -> None:
        """entry_price=0 raises ValueError."""
        ev = TrailingStopEvaluator()
        with pytest.raises(ValueError, match="entry_price must be positive"):
            ev.validate_params({"trailing_pct": 5, "native_instrument": "X", "entry_price": 0})

    def test_entry_price_negative(self) -> None:
        """entry_price=-10 raises ValueError."""
        ev = TrailingStopEvaluator()
        with pytest.raises(ValueError, match="entry_price must be positive"):
            ev.validate_params({"trailing_pct": 5, "native_instrument": "X", "entry_price": -10})

    def test_min_lock_pct_negative(self) -> None:
        """min_lock_pct=-1 raises ValueError."""
        ev = TrailingStopEvaluator()
        with pytest.raises(ValueError, match="min_lock_pct must be between"):
            ev.validate_params(
                {
                    "trailing_pct": 5,
                    "native_instrument": "X",
                    "entry_price": 100,
                    "min_lock_pct": -1,
                }
            )

    def test_min_lock_pct_100(self) -> None:
        """min_lock_pct=100 raises ValueError."""
        ev = TrailingStopEvaluator()
        with pytest.raises(ValueError, match="min_lock_pct must be between"):
            ev.validate_params(
                {
                    "trailing_pct": 5,
                    "native_instrument": "X",
                    "entry_price": 100,
                    "min_lock_pct": 100,
                }
            )

    def test_valid_params_pass(self) -> None:
        """Valid params do not raise."""
        ev = TrailingStopEvaluator()
        ev.validate_params(
            {
                "trailing_pct": 5,
                "native_instrument": "PF_BTCUSD",
                "entry_price": 50000,
                "min_lock_pct": 2.5,
            }
        )

    def test_min_lock_pct_zero_valid(self) -> None:
        """min_lock_pct=0 is valid (immediate trailing)."""
        ev = TrailingStopEvaluator()
        ev.validate_params(
            {
                "trailing_pct": 3,
                "native_instrument": "X",
                "entry_price": 100,
                "min_lock_pct": 0,
            }
        )


class TestRequiresCapabilities:
    """Tests for capability requirements."""

    def test_requires_reduce_only(self) -> None:
        """Trailing stop requires supports_reduce_only."""
        ev = TrailingStopEvaluator()
        assert ev.requires_capabilities() == ["supports_reduce_only"]


class TestCommandShape:
    """Tests for the close command structure."""

    @pytest.mark.asyncio
    async def test_close_command_long(self) -> None:
        """Close command for long has sell side and correct fields."""
        ev = TrailingStopEvaluator()
        plan = _make_plan(side="buy", entry_price=100, trailing_pct=5.0, total_quantity=2.5)
        await ev.on_tick(plan, _make_tick(120.0))
        cmds = await ev.on_tick(plan, _make_tick(113.0))
        assert len(cmds) == 1
        cmd = cmds[0]
        assert cmd["command_type"] == "create"
        assert cmd["instrument"] == "PF_BTCUSD"
        assert cmd["side"] == "sell"
        assert cmd["order_type"] == "market"
        assert cmd["quantity"] == pytest.approx(2.5)
        assert cmd["price"] is None
        assert cmd["reduce_only"] is True
        assert cmd["leverage"] == 5
        assert cmd["trigger_type"] == "tick"
        assert cmd["reason"] == "trailing_stop_hit"

    @pytest.mark.asyncio
    async def test_close_command_short(self) -> None:
        """Close command for short has buy side."""
        ev = TrailingStopEvaluator()
        plan = _make_plan(side="sell", entry_price=100, trailing_pct=5.0)
        await ev.on_tick(plan, _make_tick(80.0))
        cmds = await ev.on_tick(plan, _make_tick(85.0))
        assert len(cmds) == 1
        assert cmds[0]["side"] == "buy"
