"""Tests for deterministic P&L timeline checkpoint capture, JSON codec, and resume.

Covers differential bit-identity against full anchor replay, exact JSON
round-trip fidelity, out-of-order regression shadow clamping across the cut,
latched incompleteness persistence vs mark independence, and fail-closed gates.
"""

import json
import math
from collections.abc import Sequence
from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import cast

import pytest

from snapper.application.portfolio.exact_sum import ExactSum
from snapper.application.portfolio.pnl_opening_checkpoint import checkpoint_from_json
from snapper.application.portfolio.pnl_opening_checkpoint import checkpoint_to_json
from snapper.application.portfolio.pnl_timeline import MarkMap
from snapper.application.portfolio.pnl_timeline import PnlCarriedInput
from snapper.application.portfolio.pnl_timeline import PnlIncompletenessReason
from snapper.application.portfolio.pnl_timeline import PnlIncompletenessReasonEntry
from snapper.application.portfolio.pnl_timeline import PnlTimelinePoint
from snapper.application.portfolio.pnl_timeline import TimelineAccrual
from snapper.application.portfolio.pnl_timeline import TimelineCheckpointOpening
from snapper.application.portfolio.pnl_timeline import TimelineCheckpointPool
from snapper.application.portfolio.pnl_timeline import TimelineExecution
from snapper.application.portfolio.pnl_timeline import TimelineExecutionLineage
from snapper.application.portfolio.pnl_timeline import TimelineMarkEvidence
from snapper.application.portfolio.pnl_timeline import TimelineOpening
from snapper.application.portfolio.pnl_timeline import TimelineWindow
from snapper.application.portfolio.pnl_timeline import _prepare_executions
from snapper.application.portfolio.pnl_timeline import _RegressionShadow
from snapper.application.portfolio.pnl_timeline import build_pnl_timeline
from tests.application.portfolio import test_pnl_timeline as timeline_fixtures

_M = timeline_fixtures._m
_EXEC = timeline_fixtures._exec
_OPENING_POOL = timeline_fixtures._opening_pool


def _pointwise_window(
    from_minute: int,
    to_minute: int,
    *,
    granularity: str = "1m",
    capture_checkpoint: bool = False,
) -> TimelineWindow:
    """Build one per-point recovery window optionally requesting checkpoint capture."""
    return TimelineWindow(
        from_time=_M(from_minute),
        to_time=_M(to_minute),
        granularity=granularity,
        valuation_ccy="USD",
        recovery_budget_per_point=True,
        capture_checkpoint=capture_checkpoint,
    )


def _assert_opt_float_bit_identical(left: float | None, right: float | None) -> None:
    """Assert two optional floats match bit-for-bit including NaN and signed zero."""
    if left is None or right is None:
        assert left is None and right is None
        return
    if math.isnan(left) or math.isnan(right):
        assert math.isnan(left) and math.isnan(right)
        return
    assert left.hex() == right.hex()


def _assert_points_bit_identical(
    expected: Sequence[PnlTimelinePoint],
    actual: Sequence[PnlTimelinePoint],
) -> None:
    """Assert two point sequences match across timestamps, reasons, and float bits."""
    assert len(expected) == len(actual)
    for left, right in zip(expected, actual, strict=True):
        assert left.point_time == right.point_time
        assert left.valuation_status == right.valuation_status
        assert left.incompleteness_reasons == right.incompleteness_reasons
        assert left.carried_inputs == right.carried_inputs
        _assert_opt_float_bit_identical(left.realized_pnl, right.realized_pnl)
        _assert_opt_float_bit_identical(left.fee_pnl, right.fee_pnl)
        _assert_opt_float_bit_identical(left.accrual_pnl, right.accrual_pnl)
        _assert_opt_float_bit_identical(left.unrealized_pnl, right.unrealized_pnl)
        _assert_opt_float_bit_identical(left.net_pnl, right.net_pnl)
        assert len(left.per_instrument) == len(right.per_instrument)
        for left_inst, right_inst in zip(left.per_instrument, right.per_instrument, strict=True):
            assert left_inst.instrument_public_id == right_inst.instrument_public_id
            assert left_inst.native_symbol == right_inst.native_symbol
            assert left_inst.exchange == right_inst.exchange
            _assert_opt_float_bit_identical(left_inst.realized_pnl, right_inst.realized_pnl)
            _assert_opt_float_bit_identical(left_inst.fee_pnl, right_inst.fee_pnl)
            _assert_opt_float_bit_identical(left_inst.accrual_pnl, right_inst.accrual_pnl)
            _assert_opt_float_bit_identical(left_inst.unrealized_pnl, right_inst.unrealized_pnl)
        assert len(left.attribution) == len(right.attribution)
        for left_attr, right_attr in zip(left.attribution, right.attribution, strict=True):
            assert left_attr.origin == right_attr.origin
            assert left_attr.strategy_name == right_attr.strategy_name
            _assert_opt_float_bit_identical(left_attr.realized_pnl, right_attr.realized_pnl)
            _assert_opt_float_bit_identical(left_attr.fee_pnl, right_attr.fee_pnl)
            _assert_opt_float_bit_identical(left_attr.accrual_pnl, right_attr.accrual_pnl)
            _assert_opt_float_bit_identical(left_attr.unrealized_pnl, right_attr.unrealized_pnl)


def _split_inputs_after_cutoff(
    executions: Sequence[TimelineExecution],
    accruals: Sequence[TimelineAccrual],
    cutoff_minute: datetime,
) -> tuple[tuple[TimelineExecution, ...], tuple[TimelineAccrual, ...]]:
    """Select executions with effective time after ``cutoff_minute`` and later accruals."""
    prepared, _ = _prepare_executions(executions)
    effective_by_scope = {
        (item.exchange, item.scope_sequence): item.effective_time for item in prepared
    }
    resumed_executions = tuple(
        execution
        for execution in executions
        if effective_by_scope[(execution.exchange, execution.scope_sequence)] > cutoff_minute
    )
    resumed_accruals = tuple(accrual for accrual in accruals if accrual.accrued_at > cutoff_minute)
    return resumed_executions, resumed_accruals


def _multi_instrument_fixture() -> tuple[
    TimelineOpening,
    tuple[TimelineExecution, ...],
    tuple[TimelineAccrual, ...],
    MarkMap,
    dict[str, TimelineExecutionLineage],
]:
    """Build a multi-instrument, multi-shard, multi-attribution lifecycle fixture."""
    opening = TimelineOpening(
        pools=(
            _OPENING_POOL("I1", 1.0, 100.0, shard_key="shard-I1-a", exchange="kraken"),
            _OPENING_POOL("I2", 0.0, None, shard_key="shard-I2-main", exchange="coinbase"),
        ),
        t0=_M(0),
    )
    executions = (
        _EXEC("I1", 1, 2, "buy", 2.0, 105.0, fee=0.25, shard_key="shard-I1-a"),
        _EXEC("I1", 2, 3, "sell", 1.5, 112.0, fee=-0.10, shard_key="shard-I1-a"),
        _EXEC("I1", 3, 5, "buy", 1.0, 110.0, fee=0.15, shard_key="shard-I1-b"),
        _EXEC("I1", 4, 6, "sell", 3.5, 118.0, fee=0.35, shard_key="shard-I1-a"),
        _EXEC("I1", 5, 8, "buy", 1.25, 114.0, fee=0.12, shard_key="shard-I1-a"),
        _EXEC(
            "I2",
            1,
            3,
            "buy",
            4.0,
            50.0,
            fee=0.20,
            exchange="coinbase",
            shard_key="shard-I2-main",
        ),
        _EXEC(
            "I2",
            2,
            7,
            "sell",
            6.0,
            55.0,
            fee=0.30,
            exchange="coinbase",
            shard_key="shard-I2-main",
        ),
        _EXEC(
            "I2",
            3,
            9,
            "buy",
            2.0,
            53.0,
            fee=0.05,
            exchange="coinbase",
            shard_key="shard-I2-main",
        ),
    )
    accruals = (
        TimelineAccrual("I1", _M(0), 9.0),
        TimelineAccrual("I1", _M(4), 1.5),
        TimelineAccrual("I2", _M(5), -0.75),
        TimelineAccrual("I1", _M(8), 0.5),
    )
    marks: MarkMap = {
        (instrument, _M(minute)): (
            100.0 + minute * 1.5 if instrument == "I1" else 50.0 + minute * 0.75
        )
        for minute in range(11)
        for instrument in ("I1", "I2")
    }
    lineage = {
        "order-I1-1": TimelineExecutionLineage("rest", "manual-plan", None, "live", None),
        "order-I1-2": TimelineExecutionLineage("strategy", "plan-1", "sig-1", "live", "trend"),
        "order-I1-3": TimelineExecutionLineage("strategy", None, "sig-2", "live", "arb"),
        "order-I1-4": TimelineExecutionLineage("strategy", "plan-1", "sig-1", "live", "trend"),
        "order-I1-5": TimelineExecutionLineage("rest", None, None, "live", None),
        "order-I2-1": TimelineExecutionLineage("strategy", None, "sig-2", "live", "arb"),
        "order-I2-2": TimelineExecutionLineage("strategy", "plan-2", "sig-3", "live", "swing"),
    }
    return opening, executions, accruals, marks, lineage


class TestCheckpointDifferential:
    """Requirement (a): differential bit-identity between full and resumed replays."""

    @pytest.mark.parametrize("cutoff_minute", [1, 4, 5, 6, 7, 8])
    def test_multi_instrument_history_resumes_bit_identically_across_cuts(
        self, cutoff_minute: int
    ) -> None:
        """Capture and resume match full anchor replay across start, pre-fill, fill, and mid cuts.

        Given: A multi-instrument, two-shard, multi-attribution lifecycle with opens,
            partial closes, flips, re-opens, fees, and accruals,
        When: Full replay runs from t0..10 and capture-then-resume splits at ``cutoff_minute``,
        Then: Points before and after ``cutoff_minute`` match bit-identically and
            reusing the same checkpoint instance twice yields identical results.
        """
        opening, executions, accruals, marks, lineage = _multi_instrument_fixture()
        full = build_pnl_timeline(
            executions,
            accruals,
            marks,
            _pointwise_window(0, 10),
            opening=opening,
            lineage=lineage,
        )
        assert full.closing_checkpoint is None

        captured = build_pnl_timeline(
            executions,
            accruals,
            marks,
            _pointwise_window(0, cutoff_minute, capture_checkpoint=True),
            opening=opening,
            lineage=lineage,
        )
        assert captured.closing_checkpoint is not None
        _assert_points_bit_identical(full.points[: cutoff_minute + 1], captured.points)

        resumed_executions, resumed_accruals = _split_inputs_after_cutoff(
            executions,
            accruals,
            _M(cutoff_minute),
        )
        resumed = build_pnl_timeline(
            resumed_executions,
            resumed_accruals,
            marks,
            _pointwise_window(cutoff_minute + 1, 10),
            opening=captured.closing_checkpoint,
            lineage=lineage,
        )
        resumed_again = build_pnl_timeline(
            resumed_executions,
            resumed_accruals,
            marks,
            _pointwise_window(cutoff_minute + 1, 10),
            opening=captured.closing_checkpoint,
            lineage=lineage,
        )
        _assert_points_bit_identical(full.points[cutoff_minute + 1 :], resumed.points)
        _assert_points_bit_identical(resumed.points, resumed_again.points)

    def test_no_opening_start_cut_and_chained_checkpoints_match_full_replay(self) -> None:
        """Capture at minute 0 without anchor and chained C1->C2->end match full replay.

        Given: An unanchored history starting with fills at minute 0 and continuing
            across two later checkpoint cuts with 5m downsampling,
        When: Replay captures at minute 0, resumes and re-captures at minute 4, then
            resumes through minute 9,
        Then: All resumed points match full replay bit-identically and pre-cut accruals
            passed to resume are excluded without double-counting.
        """
        _, executions, accruals, marks, lineage = _multi_instrument_fixture()
        anchored_execs = (_EXEC("I1", 0, 0, "buy", 0.5, 99.0, fee=0.05), *executions)
        full_1m = build_pnl_timeline(
            anchored_execs,
            accruals,
            marks,
            _pointwise_window(0, 9),
            lineage=lineage,
        )
        full_5m = build_pnl_timeline(
            anchored_execs,
            accruals,
            marks,
            _pointwise_window(0, 9, granularity="5m"),
            lineage=lineage,
        )
        cap_zero = build_pnl_timeline(
            anchored_execs,
            accruals,
            marks,
            _pointwise_window(0, 0, capture_checkpoint=True),
            lineage=lineage,
        )
        assert cap_zero.closing_checkpoint is not None
        _assert_points_bit_identical(full_1m.points[:1], cap_zero.points)

        execs_after_0, accruals_after_0 = _split_inputs_after_cutoff(
            anchored_execs, accruals, _M(0)
        )
        cap_four = build_pnl_timeline(
            execs_after_0,
            accruals_after_0,
            marks,
            _pointwise_window(1, 4, granularity="5m", capture_checkpoint=True),
            opening=cap_zero.closing_checkpoint,
            lineage=lineage,
        )
        assert cap_four.closing_checkpoint is not None
        assert len(cap_four.points) == 1
        _assert_points_bit_identical(full_1m.points[4:5], cap_four.points)

        execs_after_4, _ = _split_inputs_after_cutoff(anchored_execs, accruals, _M(4))
        resumed_5m = build_pnl_timeline(
            execs_after_4,
            accruals,
            marks,
            _pointwise_window(5, 9, granularity="5m"),
            opening=cap_four.closing_checkpoint,
            lineage=lineage,
        )
        _assert_points_bit_identical(full_5m.points[1:], resumed_5m.points)


class TestCheckpointJsonRoundTrip:
    """Requirement (b): exact deterministic JSON serialization and member round-tripping."""

    def test_json_round_trip_is_exact_and_resumes_identically(self) -> None:
        """Serialize, parse, and re-serialize yield identical JSON and identical resume output.

        Given: A captured engine checkpoint from the multi-instrument lifecycle,
        When: Serialized with checkpoint_to_json, parsed with checkpoint_from_json,
            and re-serialized,
        Then: The two JSON strings are byte-for-byte identical and resuming from the
            parsed checkpoint matches both the in-memory checkpoint and full replay.
        """
        opening, executions, accruals, marks, lineage = _multi_instrument_fixture()
        captured = build_pnl_timeline(
            executions,
            accruals,
            marks,
            _pointwise_window(0, 5, capture_checkpoint=True),
            opening=opening,
            lineage=lineage,
        )
        checkpoint = captured.closing_checkpoint
        assert checkpoint is not None
        raw_first = checkpoint_to_json(checkpoint)
        parsed = checkpoint_from_json(raw_first)
        raw_second = checkpoint_to_json(parsed)
        assert raw_first == raw_second

        resumed_execs, resumed_accruals = _split_inputs_after_cutoff(executions, accruals, _M(5))
        from_memory = build_pnl_timeline(
            resumed_execs,
            resumed_accruals,
            marks,
            _pointwise_window(6, 10),
            opening=checkpoint,
            lineage=lineage,
        )
        from_parsed = build_pnl_timeline(
            resumed_execs,
            resumed_accruals,
            marks,
            _pointwise_window(6, 10),
            opening=parsed,
            lineage=lineage,
        )
        _assert_points_bit_identical(from_memory.points, from_parsed.points)

    def test_member_types_special_floats_and_exact_sums_round_trip(self) -> None:
        """Verify ExactSum, PnlIncompletenessReasonEntry, signed zero, NaN, inf, and subnormals.

        Given: A checkpoint exercising signed zero, subnormal, inf, -inf, nan,
            non-finite ExactSum accumulators, and both global and instrument reasons,
        When: Round-tripped through checkpoint_to_json and checkpoint_from_json,
        Then: Every member field, hex bit pattern, microsecond timestamp, and
            re-serialized JSON matches identically.
        """
        entry_global = PnlIncompletenessReasonEntry(
            reason="scope_order_regression",
            withholding_tier="untrusted",
            withholding_scope="global",
            trigger_instrument_public_id="I1",
        )
        entry_instrument = PnlIncompletenessReasonEntry(
            reason="fx_conversion_unproven",
            withholding_tier="untrusted",
            withholding_scope="instrument",
            trigger_instrument_public_id="I2",
        )
        checkpoint = TimelineCheckpointOpening(
            checkpoint_minute=_M(5),
            activation_time=_M(0),
            pools=(
                TimelineCheckpointPool("I2", "s2", -0.0, None),
                TimelineCheckpointPool("I1", "s1", 5e-324, math.nan),
                TimelineCheckpointPool("I3", "s3", math.inf, -math.inf),
            ),
            weights_by_pool=(
                (("I3", "s3"), ((("manual", "alpha"), 0.5), (("manual", None), 0.5))),
                (("I2", "s2"), ()),
            ),
            seen=("I1", "I2", "I3"),
            attribution_seen=(("manual", None), ("manual", ""), ("manual", "alpha")),
            basis_reasons_by_pool=((("I1", "s1"), ("cost_basis_unavailable",)),),
            untrusted_reasons_by_instrument=(
                ("I1", (entry_global,)),
                ("I2", (entry_instrument,)),
            ),
            scope_by_shard=(("s1", ("I1", "kraken")), ("s2", ("I2", "coinbase"))),
            exchange_by_instrument=(("I1", "kraken"), ("I2", "coinbase")),
            pool_keys_by_instrument=(
                ("I1", (("I1", "s1"),)),
                ("I2", (("I2", "s2"),)),
            ),
            realized_by_instrument=(("I1", ExactSum(units=1234567890123456789012345)),),
            fee_by_instrument=(("I2", ExactSum(units=-42, infinities={-1})),),
            accrual_by_instrument=(("I3", ExactSum(units=7, infinities={1, -1}, saw_nan=True)),),
            realized_by_attribution=((("manual", None), ExactSum(units=10)),),
            fee_by_attribution=((("manual", ""), ExactSum(units=0, infinities={1})),),
            accrual_by_attribution=((("manual", "alpha"), ExactSum(units=-9, saw_nan=True)),),
            realized_total=ExactSum(units=1234567890123456789012345),
            fee_total=ExactSum(units=-42, infinities={-1}),
            accrual_total=ExactSum(units=7, infinities={1, -1}, saw_nan=True),
            last_effective_by_pool=((("I1", "s1"), _M(4) + timedelta(microseconds=250)),),
        )
        raw_first = checkpoint_to_json(checkpoint)
        restored = checkpoint_from_json(raw_first)
        raw_second = checkpoint_to_json(restored)
        assert raw_first == raw_second
        assert math.copysign(1.0, restored.pools[0].position_qty) == -1.0
        assert restored.pools[1].position_qty == 5e-324
        assert math.isnan(cast(float, restored.pools[1].entry_price))
        assert restored.pools[2].position_qty == math.inf
        assert restored.pools[2].entry_price == -math.inf
        assert restored.untrusted_reasons_by_instrument == (
            ("I1", (entry_global,)),
            ("I2", (entry_instrument,)),
        )
        assert restored.accrual_total == ExactSum(units=7, infinities={1, -1}, saw_nan=True)
        assert restored.last_effective_by_pool[0][1] == _M(4) + timedelta(microseconds=250)


class TestCheckpointOutOfOrderFill:
    """Requirement (c): out-of-order fill clamping and regression shadow parity."""

    def test_out_of_order_fill_after_cutoff_emits_same_regression_shadow(self) -> None:
        """Clamped fill after C reproduces the exact regression shadow and pool state.

        Given: Pool P1 with last effective time <= C, followed by fills whose clamped
            effective times lie after C including a raw event_time <= C,
        When: Full replay runs from 0..8 and capture-then-resume cuts at C=3,
        Then: Resumed points after C have the exact same scope_order_regression
            shadow interval and subsequent valuation as the full replay.
        """
        opening = TimelineOpening(pools=(), t0=_M(0))
        executions = (
            _EXEC("I1", 1, 2, "buy", 1.0, 100.0, fee=0.1),
            _EXEC("I1", 2, 6, "buy", 1.0, 110.0, fee=0.1),
            _EXEC("I1", 3, 1, "sell", 0.5, 105.0, fee=0.05),
        )
        marks: MarkMap = {("I1", _M(minute)): 108.0 + minute for minute in range(9)}
        full = build_pnl_timeline(executions, (), marks, _pointwise_window(0, 8), opening=opening)
        captured = build_pnl_timeline(
            executions,
            (),
            marks,
            _pointwise_window(0, 3, capture_checkpoint=True),
            opening=opening,
        )
        checkpoint = captured.closing_checkpoint
        assert checkpoint is not None
        assert checkpoint.last_effective_by_pool == ((("I1", "shard-I1"), _M(2)),)

        resumed_execs, _ = _split_inputs_after_cutoff(executions, (), _M(3))
        resumed = build_pnl_timeline(
            resumed_execs,
            (),
            marks,
            _pointwise_window(4, 8),
            opening=checkpoint,
        )
        _assert_points_bit_identical(full.points[4:], resumed.points)
        assert any(
            entry.reason == "scope_order_regression"
            for entry in resumed.points[0].incompleteness_reasons
        )
        assert resumed.points[2].valuation_status == "complete"

        direct_prepared, direct_shadows = _prepare_executions(
            (_EXEC("I1", 2, 1, "sell", 0.5, 105.0),),
            initial_last_effective=dict(checkpoint.last_effective_by_pool),
        )
        assert direct_prepared[0].effective_time == _M(2)
        assert direct_shadows == [
            _RegressionShadow(
                start=_M(1),
                end=_M(2),
                reason=PnlIncompletenessReasonEntry(
                    reason="scope_order_regression",
                    withholding_tier="untrusted",
                    withholding_scope="global",
                    trigger_instrument_public_id="I1",
                ),
            )
        ]


class TestCheckpointLatchSourcesAndMarks:
    """Requirement (d): all five latch sources persist across C while marks do not."""

    def test_latch_execution_price_reason_persists_across_cutoff(self) -> None:
        """Invalid opening and closing execution price reasons survive checkpoint resume.

        Given: One instrument opened with invalid price before C and closed after C, and
            one closing with an invalid price_incompleteness_reason before C,
        When: Captured at C and resumed after C,
        Then: Resumed points match full replay bit-identically across all causes.
        """
        executions = (
            _EXEC("I1", 1, 1, "buy", 1.0, float("nan")),
            _EXEC("I2", 2, 1, "buy", 1.0, 50.0),
            replace(
                _EXEC("I2", 3, 2, "sell", 1.0, 0.0),
                price_incompleteness_reason="fx_conversion_unproven",
            ),
            _EXEC("I1", 4, 4, "sell", 1.0, 100.0),
        )
        marks: MarkMap = {("I1", _M(minute)): 100.0 for minute in range(6)} | {
            ("I2", _M(minute)): 50.0 for minute in range(6)
        }
        full = build_pnl_timeline(executions, (), marks, _pointwise_window(0, 5))
        captured = build_pnl_timeline(
            executions,
            (),
            marks,
            _pointwise_window(0, 2, capture_checkpoint=True),
        )
        assert captured.closing_checkpoint is not None
        resumed_execs, _ = _split_inputs_after_cutoff(executions, (), _M(2))
        resumed = build_pnl_timeline(
            resumed_execs,
            (),
            marks,
            _pointwise_window(3, 5),
            opening=captured.closing_checkpoint,
        )
        _assert_points_bit_identical(full.points[3:], resumed.points)

    def test_latch_fee_reason_persists_across_cutoff(self) -> None:
        """Execution fee_incompleteness_reason latched before C survives resume.

        Given: An execution at minute 1 with fee_incompleteness_reason set,
        When: Captured at C=2 and resumed through minute 4,
        Then: Resumed points match full replay bit-identically.
        """
        executions = (
            replace(
                _EXEC("I1", 1, 1, "buy", 1.0, 100.0, fee=1.0),
                fee_incompleteness_reason="fx_conversion_unproven",
            ),
            _EXEC("I2", 2, 1, "buy", 1.0, 50.0, fee=0.5),
        )
        marks: MarkMap = {("I1", _M(minute)): 105.0 for minute in range(5)} | {
            ("I2", _M(minute)): 55.0 for minute in range(5)
        }
        full = build_pnl_timeline(executions, (), marks, _pointwise_window(0, 4))
        captured = build_pnl_timeline(
            executions,
            (),
            marks,
            _pointwise_window(0, 2, capture_checkpoint=True),
        )
        assert captured.closing_checkpoint is not None
        resumed = build_pnl_timeline(
            (),
            (),
            marks,
            _pointwise_window(3, 4),
            opening=captured.closing_checkpoint,
        )
        _assert_points_bit_identical(full.points[3:], resumed.points)

    def test_latch_caller_untrusted_price_reason_persists_across_cutoff(self) -> None:
        """Caller-stamped untrusted price reasons latched before C persist even without re-passing.

        Given: Caller-stamped untrusted_price_reasons_by_instrument affecting a fill at minute 1,
        When: Captured at C=2 and resumed with empty untrusted_price_reasons_by_instrument,
        Then: Resumed points match full replay bit-identically.
        """
        executions = (
            _EXEC("I1", 1, 1, "buy", 1.0, 100.0),
            _EXEC("I2", 2, 1, "buy", 1.0, 50.0),
            _EXEC("I2", 3, 3, "sell", 1.0, 55.0),
        )
        marks: MarkMap = {("I1", _M(minute)): 105.0 for minute in range(5)} | {
            ("I2", _M(minute)): 52.0 for minute in range(5)
        }
        untrusted: dict[str, set[PnlIncompletenessReason]] = {
            "I1": {"execution_price_provenance_unproven"}
        }
        full = build_pnl_timeline(
            executions,
            (),
            marks,
            _pointwise_window(0, 4),
            untrusted_price_reasons_by_instrument=untrusted,
        )
        captured = build_pnl_timeline(
            executions,
            (),
            marks,
            _pointwise_window(0, 2, capture_checkpoint=True),
            untrusted_price_reasons_by_instrument=untrusted,
        )
        assert captured.closing_checkpoint is not None
        resumed_execs, _ = _split_inputs_after_cutoff(executions, (), _M(2))
        resumed = build_pnl_timeline(
            resumed_execs,
            (),
            marks,
            _pointwise_window(3, 4),
            opening=captured.closing_checkpoint,
        )
        _assert_points_bit_identical(full.points[3:], resumed.points)

    def test_latch_opening_cost_basis_unavailable_persists_across_cutoff(self) -> None:
        """Seeded unknown and non-positive opening cost basis persist across checkpoint resume.

        Given: Opening pools with None entry_price and non-positive entry_price at t0,
        When: Captured at C=2 and closed after C,
        Then: Resumed points match full replay bit-identically.
        """
        opening = TimelineOpening(
            pools=(
                _OPENING_POOL("I1", 1.0, None, exchange="kraken"),
                _OPENING_POOL("I2", 1.0, -5.0, exchange="coinbase"),
            ),
            t0=_M(0),
        )
        executions = (
            _EXEC("I1", 1, 3, "sell", 1.0, 110.0),
            _EXEC("I2", 1, 4, "sell", 1.0, 60.0, exchange="coinbase"),
        )
        marks: MarkMap = {("I1", _M(minute)): 105.0 for minute in range(6)} | {
            ("I2", _M(minute)): 55.0 for minute in range(6)
        }
        full = build_pnl_timeline(executions, (), marks, _pointwise_window(0, 5), opening=opening)
        captured = build_pnl_timeline(
            executions,
            (),
            marks,
            _pointwise_window(0, 2, capture_checkpoint=True),
            opening=opening,
        )
        assert captured.closing_checkpoint is not None
        resumed_execs, _ = _split_inputs_after_cutoff(executions, (), _M(2))
        resumed = build_pnl_timeline(
            resumed_execs,
            (),
            marks,
            _pointwise_window(3, 5),
            opening=captured.closing_checkpoint,
        )
        _assert_points_bit_identical(full.points[3:], resumed.points)

    def test_latch_accrual_reason_persists_across_cutoff(self) -> None:
        """Accrual incompleteness_reason latched before C persists across checkpoint resume.

        Given: An accrual before C stamped with an incompleteness_reason,
        When: Captured at C=2 and resumed through minute 4,
        Then: Resumed points match full replay bit-identically.
        """
        executions = (_EXEC("I1", 1, 1, "buy", 1.0, 100.0),)
        accruals = (
            TimelineAccrual(
                instrument_public_id="I1",
                accrued_at=_M(2),
                amount_usd=2.0,
                incompleteness_reason="fx_conversion_unproven",
            ),
        )
        marks: MarkMap = {("I1", _M(minute)): 105.0 for minute in range(5)}
        full = build_pnl_timeline(executions, accruals, marks, _pointwise_window(0, 4))
        captured = build_pnl_timeline(
            executions,
            accruals,
            marks,
            _pointwise_window(0, 2, capture_checkpoint=True),
        )
        assert captured.closing_checkpoint is not None
        resumed = build_pnl_timeline(
            (),
            (),
            marks,
            _pointwise_window(3, 4),
            opening=captured.closing_checkpoint,
        )
        _assert_points_bit_identical(full.points[3:], resumed.points)

    def test_missing_or_non_finite_marks_before_cutoff_do_not_change_checkpoint(self) -> None:
        """Marks at or before C are never carried in checkpoint state.

        Given: One replay with complete marks up to C and one with missing and NaN
            marks up to C plus mark_evidence reasons,
        When: Both capture checkpoints at C=2 and resume on identical marks for 3..5,
        Then: Both serialized checkpoints are identical and resumed outputs match full replay.
        """
        executions = (_EXEC("I1", 1, 1, "buy", 2.0, 100.0, fee=0.5),)
        full_marks: MarkMap = {("I1", _M(minute)): 100.0 + minute for minute in range(6)}
        degraded_marks: MarkMap = {
            ("I1", _M(0)): math.nan,
            ("I1", _M(3)): 103.0,
            ("I1", _M(4)): 104.0,
            ("I1", _M(5)): 105.0,
        }
        mark_evidence = TimelineMarkEvidence(
            incompleteness_reasons={("I1", _M(1)): "mark_unavailable"},
            carried_inputs={("I1", _M(2)): (PnlCarriedInput("I1", "mark", _M(0), 2),)},
        )
        cap_clean = build_pnl_timeline(
            executions,
            (),
            full_marks,
            _pointwise_window(0, 2, capture_checkpoint=True),
        )
        cap_degraded = build_pnl_timeline(
            executions,
            (),
            degraded_marks,
            _pointwise_window(0, 2, capture_checkpoint=True),
            mark_evidence=mark_evidence,
        )
        assert cap_clean.closing_checkpoint is not None
        assert cap_degraded.closing_checkpoint is not None
        assert checkpoint_to_json(cap_clean.closing_checkpoint) == checkpoint_to_json(
            cap_degraded.closing_checkpoint
        )

        full = build_pnl_timeline(executions, (), degraded_marks, _pointwise_window(0, 5))
        resumed = build_pnl_timeline(
            (),
            (),
            degraded_marks,
            _pointwise_window(3, 5),
            opening=cap_degraded.closing_checkpoint,
        )
        _assert_points_bit_identical(full.points[3:], resumed.points)


class TestCheckpointFailClosed:
    """Requirement (e): fail-closed mode, carried_event_inputs, schema, and JSON guards."""

    def test_capture_and_resume_require_recovery_budget_per_point(self) -> None:
        """Capture and resume both reject recovery_budget_per_point=False.

        Given: A standard window with recovery_budget_per_point=False and a valid checkpoint,
        When: Either capture_checkpoint=True is requested or opening is a checkpoint,
        Then: build_pnl_timeline raises ValueError.
        """
        with pytest.raises(ValueError, match="recovery_budget_per_point"):
            build_pnl_timeline(
                (),
                (),
                {},
                TimelineWindow(_M(0), _M(1), "1m", "USD", capture_checkpoint=True),
            )
        checkpoint = build_pnl_timeline(
            (), (), {}, _pointwise_window(0, 1, capture_checkpoint=True)
        ).closing_checkpoint
        assert checkpoint is not None
        with pytest.raises(ValueError, match="recovery_budget_per_point"):
            build_pnl_timeline(
                (),
                (),
                {},
                TimelineWindow(_M(2), _M(3), "1m", "USD"),
                opening=checkpoint,
            )

    def test_non_empty_carried_event_inputs_at_capture_raises(self) -> None:
        """Capturing when an applied fill or accrual carried an event FX input fails closed.

        Given: An execution or accrual before C carrying a PnlCarriedInput,
        When: Capture is requested at C,
        Then: build_pnl_timeline raises ValueError.
        """
        carried = (PnlCarriedInput("I1", "event", _M(0), 1, "USD/EUR"),)
        exec_with_carried = replace(_EXEC("I1", 1, 1, "buy", 1.0, 100.0), carried_inputs=carried)
        with pytest.raises(ValueError, match="carried_event_inputs"):
            build_pnl_timeline(
                (exec_with_carried,),
                (),
                {},
                _pointwise_window(0, 2, capture_checkpoint=True),
            )
        accrual_with_carried = TimelineAccrual("I1", _M(1), 1.0, carried_inputs=carried)
        with pytest.raises(ValueError, match="carried_event_inputs"):
            build_pnl_timeline(
                (),
                (accrual_with_carried,),
                {},
                _pointwise_window(0, 2, capture_checkpoint=True),
            )

    def test_wrong_schema_version_and_malformed_json_raise(self) -> None:
        """Wrong schema_version, malformed JSON, duplicate keys, and NaN constants fail closed.

        Given: A valid serialized checkpoint JSON string mutated in schema_version or
            JSON structure,
        When: Passed to checkpoint_from_json,
        Then: ValueError is raised in every malformed or mismatched-schema case.
        """
        checkpoint = build_pnl_timeline(
            (_EXEC("I1", 1, 1, "buy", 1.0, 100.0),),
            (),
            {},
            _pointwise_window(0, 2, capture_checkpoint=True),
        ).closing_checkpoint
        assert checkpoint is not None
        valid_json = checkpoint_to_json(checkpoint)
        payload = json.loads(valid_json)

        for malformed in (
            "{not-json",
            '{"schema_version": 1, "schema_version": 1}',
            valid_json.replace('"schema_version":1', '"schema_version":NaN'),
            json.dumps({**payload, "schema_version": 2}),
            json.dumps({**payload, "unexpected_field": 1}),
            json.dumps({**payload, "carried_event_inputs": ["bad"]}),
        ):
            with pytest.raises(ValueError):
                checkpoint_from_json(malformed)

    def test_checkpoint_json_rejects_invalid_member_encodings_and_orderings(self) -> None:
        """All strict codec validators reject tampered member values, ordering, and clocks.

        Given: A populated valid checkpoint JSON payload mutated across each member invariant,
        When: Each invalid mutation is passed to checkpoint_from_json,
        Then: Each raises ValueError, verifying every strict validator branch.
        """
        opening = TimelineOpening(
            pools=(_OPENING_POOL("I1", 1.0, None),),
            t0=_M(0),
        )
        executions = (
            _EXEC("I1", 1, 1, "buy", 1.0, 100.0),
            _EXEC("I2", 2, 1, "buy", 1.0, 50.0),
        )
        captured = build_pnl_timeline(
            executions,
            (),
            {},
            _pointwise_window(0, 2, capture_checkpoint=True),
            opening=opening,
        )
        assert captured.closing_checkpoint is not None
        base = json.loads(checkpoint_to_json(captured.closing_checkpoint))

        mutations: list[dict[str, object]] = [
            {**base, "seen": ["I1", ""]},
            {**base, "seen": ["I2", "I1"]},
            {
                **base,
                "pools": [
                    {
                        "instrument_public_id": "I1",
                        "shard_key": "s",
                        "position_qty_hex": "not-hex",
                        "entry_price_hex": None,
                    }
                ],
            },
            {
                **base,
                "pools": [
                    {
                        "instrument_public_id": "I1",
                        "shard_key": "s",
                        "position_qty_hex": "0X1.0P+0",
                        "entry_price_hex": None,
                    }
                ],
            },
            {
                **base,
                "pools": [base["pools"][0], base["pools"][0]],
            },
            {
                **base,
                "realized_total": {"units": "bad", "infinities": [], "saw_nan": False},
            },
            {
                **base,
                "realized_total": {"units": "01", "infinities": [], "saw_nan": False},
            },
            {
                **base,
                "realized_total": {"units": "0", "infinities": [1, -1], "saw_nan": False},
            },
            {**base, "checkpoint_minute": "2026-07-20T10:02:00"},
            {**base, "checkpoint_minute": "2026-07-20T10:02:00+02:00"},
            {**base, "checkpoint_minute": "2026-07-20T10:02:15Z"},
            {**base, "activation_time": "2026-07-20T10:02:00Z"},
            {
                **base,
                "weights_by_pool": [base["weights_by_pool"][0], base["weights_by_pool"][0]],
            },
            {
                **base,
                "weights_by_pool": [
                    {
                        "pool": {"instrument_public_id": "I1", "shard_key": "shard-I1"},
                        "weights": [
                            {
                                "attribution": {"origin": "unattributed", "strategy_name": None},
                                "weight_hex": (1.0).hex(),
                            },
                            {
                                "attribution": {"origin": "unattributed", "strategy_name": None},
                                "weight_hex": (1.0).hex(),
                            },
                        ],
                    }
                ],
            },
            {
                **base,
                "basis_reasons_by_pool": [
                    {"pool": {"instrument_public_id": "I1", "shard_key": "s"}, "reasons": []}
                ],
            },
            {
                **base,
                "untrusted_reasons_by_instrument": [
                    {
                        "instrument_public_id": "I1",
                        "reasons": [
                            {
                                "reason": "cost_basis_unavailable",
                                "withholding_tier": "untrusted",
                                "withholding_scope": "instrument",
                                "trigger_instrument_public_id": None,
                            }
                        ],
                    }
                ],
            },
            {
                **base,
                "untrusted_reasons_by_instrument": [{"instrument_public_id": "I1", "reasons": []}],
            },
            {
                **base,
                "last_effective_by_pool": [
                    {
                        "pool": {"instrument_public_id": "I1", "shard_key": "shard-I1"},
                        "last_effective_time": "2026-07-20T10:03:00Z",
                    }
                ],
            },
        ]
        for mutated in mutations:
            with pytest.raises(ValueError):
                checkpoint_from_json(json.dumps(mutated))

    def test_checkpoint_window_and_domain_boundary_guards_fail_closed(self) -> None:
        """Invalid capture/resume windows, scope violations, and bad TimelineCheckpointOpening raise.

        Given: Invalid window bounds, naive/non-UTC timestamps, out-of-range resume
            executions, duplicate domain checkpoint keys, or shard/exchange mismatches,
        When: Validated by TimelineCheckpointOpening or build_pnl_timeline,
        Then: ValueError is raised without corrupting state.
        """
        opening = TimelineOpening(pools=(_OPENING_POOL("I1", 1.0, 100.0),), t0=_M(2))
        naive_time = datetime(2026, 7, 20, 10, 3)
        plus_two = datetime(2026, 7, 20, 12, 3, tzinfo=timezone(timedelta(hours=2)))
        with pytest.raises(ValueError, match="UTC"):
            build_pnl_timeline(
                (), (), {}, TimelineWindow(naive_time, _M(3), "1m", "USD", True, True)
            )
        with pytest.raises(ValueError, match="minute"):
            build_pnl_timeline(
                (),
                (),
                {},
                TimelineWindow(_M(0), _M(3) + timedelta(seconds=10), "1m", "USD", True, True),
            )
        with pytest.raises(ValueError, match="non-empty"):
            build_pnl_timeline((), (), {}, _pointwise_window(5, 4, capture_checkpoint=True))
        with pytest.raises(ValueError, match="after activation_time"):
            build_pnl_timeline(
                (), (), {}, _pointwise_window(0, 2, capture_checkpoint=True), opening=opening
            )

        checkpoint = build_pnl_timeline(
            (_EXEC("I1", 1, 3, "buy", 1.0, 100.0),),
            (),
            {},
            _pointwise_window(0, 4, capture_checkpoint=True),
            opening=opening,
        ).closing_checkpoint
        assert checkpoint is not None
        with pytest.raises(ValueError, match="UTC bounds"):
            build_pnl_timeline(
                (),
                (),
                {},
                TimelineWindow(plus_two, _M(6), "1m", "USD", recovery_budget_per_point=True),
                opening=checkpoint,
            )
        with pytest.raises(ValueError, match="after checkpoint_minute"):
            build_pnl_timeline((), (), {}, _pointwise_window(4, 6), opening=checkpoint)
        with pytest.raises(ValueError, match="at or before checkpoint_minute"):
            build_pnl_timeline(
                (_EXEC("I1", 2, 4, "buy", 1.0, 101.0),),
                (),
                {},
                _pointwise_window(5, 6),
                opening=checkpoint,
            )
        with pytest.raises(ValueError, match="cannot span instrument or exchange scopes"):
            build_pnl_timeline(
                (_EXEC("I1", 2, 5, "buy", 1.0, 101.0, exchange="coinbase"),),
                (),
                {},
                _pointwise_window(5, 6),
                opening=checkpoint,
            )
        with pytest.raises(ValueError, match="cannot span multiple exchanges"):
            build_pnl_timeline(
                (
                    _EXEC(
                        "I1",
                        2,
                        5,
                        "buy",
                        1.0,
                        101.0,
                        exchange="coinbase",
                        shard_key="shard-I1-other",
                    ),
                ),
                (),
                {},
                _pointwise_window(5, 6),
                opening=checkpoint,
            )

        with pytest.raises(ValueError, match="checkpoint_minute must follow activation_time"):
            replace(checkpoint, activation_time=_M(4))
        with pytest.raises(ValueError, match="duplicate checkpoint keys"):
            replace(checkpoint, seen=("I1", "I1"))
        with pytest.raises(ValueError, match="last_effective_by_pool"):
            replace(checkpoint, last_effective_by_pool=((("I1", "shard-I1"), _M(5)),))
        with pytest.raises(ValueError, match="last_effective_by_pool"):
            replace(checkpoint, last_effective_by_pool=((("I1", "shard-I1"), naive_time),))
