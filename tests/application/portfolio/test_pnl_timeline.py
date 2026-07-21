"""Tests for the pure P&L timeline series builder (Phase 5A core).

Exercises :func:`build_pnl_timeline` across the soundness contract: the shared
average-cost replay (realized decomposition), the separate fee and accrual
components with their expense / holder-pays signs, anchor seeding with the
baseline-leakage guard, honest-incomplete valuation on missing marks and unknown
seeded entries, the event-time regression shadow guard, the flow/stock
downsampling reduction, and the degenerate empty and invalid-granularity edges.
"""

import math
from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest

from snapper.application.portfolio.average_cost import FLAT_EPSILON
from snapper.application.portfolio.pnl_timeline import AttributionKey
from snapper.application.portfolio.pnl_timeline import MarkMap
from snapper.application.portfolio.pnl_timeline import OpeningPosition
from snapper.application.portfolio.pnl_timeline import PnlTimelinePoint
from snapper.application.portfolio.pnl_timeline import TimelineAccrual
from snapper.application.portfolio.pnl_timeline import TimelineExecution
from snapper.application.portfolio.pnl_timeline import TimelineExecutionLineage
from snapper.application.portfolio.pnl_timeline import TimelineOpening
from snapper.application.portfolio.pnl_timeline import TimelineWindow
from snapper.application.portfolio.pnl_timeline import _allocate_by_weights
from snapper.application.portfolio.pnl_timeline import _reconcile_weights
from snapper.application.portfolio.pnl_timeline import _values_with_residue
from snapper.application.portfolio.pnl_timeline import build_pnl_timeline

_T0 = datetime(2026, 7, 20, 10, 0, tzinfo=UTC)


def _m(minute: int) -> datetime:
    """Return the grid minute ``_T0 + minute``."""
    return _T0 + timedelta(minutes=minute)


def _exec(
    instrument: str,
    scope: int,
    minute: int,
    side: str,
    size: float,
    price: float,
    fee: float = 0.0,
    exchange: str = "kraken",
) -> TimelineExecution:
    """Build one timeline execution at the given grid minute."""
    return TimelineExecution(
        instrument_public_id=instrument,
        exchange=exchange,
        scope_sequence=scope,
        event_time=_m(minute),
        side=side,
        size=size,
        price=price,
        fee=fee,
        fee_asset="USD",
        order_public_id=f"order-{instrument}-{scope}",
    )


def _window(
    from_minute: int = 0,
    to_minute: int = 5,
    granularity: str = "1m",
) -> TimelineWindow:
    """Build a valuation window spanning the given grid minutes."""
    return TimelineWindow(
        from_time=_m(from_minute),
        to_time=_m(to_minute),
        granularity=granularity,
        valuation_ccy="USD",
    )


def _assert_exact_attribution_sums(point: PnlTimelinePoint) -> None:
    """Assert every exposed attribution component exactly equals its aggregate."""
    realized = [contribution.realized_pnl for contribution in point.attribution]
    fees = [contribution.fee_pnl for contribution in point.attribution]
    accruals = [contribution.accrual_pnl for contribution in point.attribution]
    unrealized = [contribution.unrealized_pnl for contribution in point.attribution]
    assert None not in realized
    assert None not in fees
    assert None not in accruals
    assert None not in unrealized
    assert sum(value for value in realized if value is not None) == point.realized_pnl
    assert sum(value for value in fees if value is not None) == point.fee_pnl
    assert sum(value for value in accruals if value is not None) == point.accrual_pnl
    assert sum(value for value in unrealized if value is not None) == point.unrealized_pnl


class TestAttribution:
    """Prove composite origin/strategy allocation and fail-closed lineage."""

    def _lineage(self) -> dict[str, TimelineExecutionLineage]:
        """Return manual, system, and non-manual plan lineage for one cycle."""
        return {
            "order-I1-1": TimelineExecutionLineage(
                source_surface="rest",
                plan_public_id="manual-once-plan",
                signal_public_id=None,
                origin="live",
                strategy_name=None,
            ),
            "order-I1-2": TimelineExecutionLineage(
                source_surface="strategy",
                plan_public_id=None,
                signal_public_id="signal-1",
                origin="live",
                strategy_name="momentum",
            ),
            "order-I1-3": TimelineExecutionLineage(
                source_surface="strategy",
                plan_public_id="plan-1",
                signal_public_id=None,
                origin="live",
                strategy_name=None,
            ),
            "order-I1-4": TimelineExecutionLineage(
                source_surface="strategy",
                plan_public_id="plan-1",
                signal_public_id=None,
                origin="live",
                strategy_name=None,
            ),
        }

    def _cycle(self) -> tuple[TimelineExecution, ...]:
        """Return a mixed-origin add, reduce, flip, and full-close cycle."""
        return (
            _exec("I1", 1, 0, "buy", 1.0, 100.0, fee=0.1),
            _exec("I1", 2, 1, "buy", 2.0, 110.0, fee=0.2),
            _exec("I1", 3, 2, "sell", 1.0, 120.0, fee=0.3),
            _exec("I1", 4, 3, "sell", 3.0, 125.0, fee=0.7),
            _exec("I1", 5, 4, "buy", 1.0, 115.0, fee=0.2),
        )

    def test_manual_once_precedes_its_plan_and_signal_uses_strategy_name(self) -> None:
        """REST manual_once stays manual while a signal fill is system-labelled."""
        marks: MarkMap = {
            ("I1", _m(0)): 100.0,
            ("I1", _m(1)): 110.0,
            ("I1", _m(2)): 120.0,
            ("I1", _m(3)): 120.0,
        }
        accruals = (TimelineAccrual("I1", _m(1), 0.1),)
        result = build_pnl_timeline(
            self._cycle(), accruals, marks, _window(0, 4), lineage=self._lineage()
        )
        opening_buckets = result.points[0].attribution
        assert [(bucket.origin, bucket.strategy_name) for bucket in opening_buckets] == [
            ("manual", None)
        ]
        second_buckets = result.points[1].attribution
        assert ("manual", None) in {
            (bucket.origin, bucket.strategy_name) for bucket in second_buckets
        }
        assert ("system", "momentum") in {
            (bucket.origin, bucket.strategy_name) for bucket in second_buckets
        }

    def test_reduce_flip_and_full_close_allocate_and_sum_exactly(self) -> None:
        """Every flow and stock sums exactly after reduction, flip, and close."""
        marks: MarkMap = {
            ("I1", _m(0)): 100.0,
            ("I1", _m(1)): 110.0,
            ("I1", _m(2)): 120.0,
            ("I1", _m(3)): 120.0,
        }
        accruals = (TimelineAccrual("I1", _m(1), 0.1),)
        result = build_pnl_timeline(
            self._cycle(), accruals, marks, _window(0, 4), lineage=self._lineage()
        )
        reduced = result.points[2]
        flipped = result.points[3]
        closed = result.points[4]
        for point in (reduced, flipped, closed):
            _assert_exact_attribution_sums(point)
        reduced_plan = next(bucket for bucket in reduced.attribution if bucket.origin == "plan")
        assert reduced_plan.realized_pnl == 0.0
        assert reduced_plan.fee_pnl == 0.0
        flipped_plan = next(bucket for bucket in flipped.attribution if bucket.origin == "plan")
        assert flipped_plan.unrealized_pnl == pytest.approx(5.0)
        assert flipped_plan.fee_pnl == pytest.approx(-0.7 / 3.0)
        assert closed.unrealized_pnl == 0.0
        assert all(bucket.unrealized_pnl == 0.0 for bucket in closed.attribution)
        assert ("unattributed", None) in {
            (bucket.origin, bucket.strategy_name) for bucket in closed.attribution
        }

    def test_missing_unknown_and_replay_lineage_are_unattributed(self) -> None:
        """A plan id alone, absent row, and replay provenance never get guessed."""
        executions = (
            _exec("I1", 1, 0, "buy", 1.0, 100.0),
            _exec("I2", 2, 0, "buy", 1.0, 100.0),
            _exec("I3", 3, 0, "buy", 1.0, 100.0),
        )
        lineage = {
            "order-I1-1": TimelineExecutionLineage(
                source_surface=None,
                plan_public_id="plan-alone",
                signal_public_id="signal-unknown-source",
                origin="live",
                strategy_name="linked-only",
            ),
            "order-I2-2": TimelineExecutionLineage(
                source_surface="rest",
                plan_public_id="manual-plan",
                signal_public_id="signal-replay",
                origin="replay",
                strategy_name="replay-strategy",
            ),
        }
        marks: MarkMap = {
            ("I1", _m(0)): 100.0,
            ("I2", _m(0)): 100.0,
            ("I3", _m(0)): 100.0,
        }
        point = build_pnl_timeline(executions, (), marks, _window(0, 0), lineage=lineage).points[0]
        assert {bucket.origin for bucket in point.attribution} == {"unattributed"}
        assert {bucket.strategy_name for bucket in point.attribution} == {
            None,
            "linked-only",
            "replay-strategy",
        }
        _assert_exact_attribution_sums(point)

    def test_null_surface_planless_legacy_lineage_is_unattributed(self) -> None:
        """A fail-closed legacy command cannot be reclassified as manual."""
        execution = _exec("I1", 1, 0, "buy", 1.0, 100.0)
        lineage = {
            execution.order_public_id: TimelineExecutionLineage(
                source_surface=None,
                plan_public_id=None,
                signal_public_id=None,
                origin="live",
                strategy_name=None,
            )
        }
        marks: MarkMap = {("I1", _m(0)): 110.0}
        point = build_pnl_timeline((execution,), (), marks, _window(0, 0), lineage=lineage).points[
            0
        ]
        assert {bucket.origin for bucket in point.attribution} == {"unattributed"}
        assert point.attribution[0].unrealized_pnl == 10.0
        _assert_exact_attribution_sums(point)

    def test_invalid_weight_pool_falls_back_to_unattributed(self) -> None:
        """An unusable internal weight pool never fabricates a proven owner."""
        reconciled = _reconcile_weights(
            {("manual", None): float("nan"), ("system", None): -1.0}, 2.0
        )
        assert reconciled == {("unattributed", None): 2.0}
        assert _reconcile_weights({}, 2.0) == {("unattributed", None): 2.0}

    def test_mixed_finite_and_nonfinite_weights_fail_closed_as_one_pool(self) -> None:
        """One invalid owner invalidates the map instead of inflating a survivor."""
        weights: dict[AttributionKey, float] = {
            ("manual", None): 1.0,
            ("system", None): math.inf,
        }
        assert _allocate_by_weights(2.0, weights) == {("unattributed", None): 2.0}
        assert _reconcile_weights(weights, 2.0) == {("unattributed", None): 2.0}

    def test_nonfinite_pro_rata_allocations_fail_closed(self) -> None:
        """Every non-finite direct or final allocation collapses the whole map."""
        assert _allocate_by_weights(
            math.inf,
            {("manual", None): 1.0, ("system", None): 1.0},
        ) == {("unattributed", None): math.inf}
        assert _allocate_by_weights(math.inf, {("manual", None): 1.0}) == {
            ("unattributed", None): math.inf
        }

    def test_finite_weight_total_full_overflow_fails_closed(self) -> None:
        """A finite map whose total overflows cannot prove any surviving owner."""
        weights: dict[AttributionKey, float] = {
            ("manual", None): 1e308,
            ("system", None): 1e308,
        }
        assert _allocate_by_weights(2.0, weights) == {("unattributed", None): 2.0}
        assert _reconcile_weights(weights, 1e308) == {("unattributed", None): 1e308}

    def test_unrepresentable_positive_weights_fall_back_to_unattributed(self) -> None:
        """Quantity residue cannot turn a tiny proven ownership weight negative."""
        reconciled = _reconcile_weights(
            {
                ("manual", None): 1e16,
                ("plan", None): 1.0,
                ("system", None): 1.0,
            },
            1e16,
        )
        assert reconciled == {("unattributed", None): 1e16}

    def test_near_epsilon_close_assigns_all_fee_to_pre_fill_weights(self) -> None:
        """A snap-to-flat close is not a flip even when the kernel reports overshoot."""
        executions = (
            _exec("I1", 1, 0, "buy", 1.0, 100.0),
            _exec("I1", 2, 0, "sell", 1.0 + FLAT_EPSILON / 2.0, 110.0, fee=1.0),
        )
        lineage = {
            "order-I1-1": TimelineExecutionLineage("rest", None, None, "live", None),
            "order-I1-2": TimelineExecutionLineage(
                "strategy", None, "signal-1", "live", "momentum"
            ),
        }
        point = build_pnl_timeline(executions, (), {}, _window(0, 0), lineage=lineage).points[0]
        buckets = {(bucket.origin, bucket.strategy_name): bucket for bucket in point.attribution}
        assert point.valuation_status == "complete"
        assert buckets[("manual", None)].fee_pnl == -1.0
        assert buckets[("system", "momentum")].fee_pnl == 0.0
        assert buckets[("manual", None)].realized_pnl == pytest.approx(10.0)
        assert buckets[("system", "momentum")].realized_pnl == 0.0
        _assert_exact_attribution_sums(point)

    def test_dust_pool_overshoot_assigns_new_side_to_incoming_fill(self) -> None:
        """A kernel ``open`` that closes dust still splits old and new ownership."""
        opening = TimelineOpening(
            positions={
                "I1": OpeningPosition(
                    position_qty=FLAT_EPSILON / 2.0,
                    entry_price=100.0,
                )
            },
            opening_unrealized_value=0.0,
            t0=_m(0),
        )
        execution = _exec("I1", 1, 0, "sell", 1.0, 110.0, fee=1.0)
        lineage = {
            execution.order_public_id: TimelineExecutionLineage(
                "strategy", None, "signal-1", "live", "momentum"
            )
        }
        point = build_pnl_timeline(
            (execution,),
            (),
            {("I1", _m(0)): 100.0},
            _window(0, 0),
            opening,
            lineage,
        ).points[0]
        buckets = {(bucket.origin, bucket.strategy_name): bucket for bucket in point.attribution}
        unattributed = buckets[("unattributed", None)]
        system = buckets[("system", "momentum")]
        assert unattributed.realized_pnl == pytest.approx(5e-12)
        assert unattributed.fee_pnl == pytest.approx(-FLAT_EPSILON / 2.0)
        assert unattributed.unrealized_pnl == 0.0
        assert system.realized_pnl == 0.0
        assert system.fee_pnl == pytest.approx(-(1.0 - FLAT_EPSILON / 2.0))
        assert system.unrealized_pnl == pytest.approx(10.0 * (1.0 - FLAT_EPSILON / 2.0))
        _assert_exact_attribution_sums(point)

    def test_unrepresentable_float_residue_withholds_the_point(self) -> None:
        """A complete point never exposes buckets that cannot equal their aggregate."""
        executions = (
            _exec("I1", 1, 0, "buy", 0.0, 100.0, fee=-1e20),
            _exec("I2", 2, 0, "buy", 0.0, 100.0, fee=1e20),
            _exec("I3", 3, 0, "buy", 0.0, 100.0, fee=-1.0),
        )
        lineage = {
            "order-I1-1": TimelineExecutionLineage("rest", None, None, "live", None),
            "order-I2-2": TimelineExecutionLineage(
                "strategy", None, "signal-1", "live", "momentum"
            ),
            "order-I3-3": TimelineExecutionLineage(
                "strategy", None, "signal-1", "live", "momentum"
            ),
        }
        point = build_pnl_timeline(executions, (), {}, _window(0, 0), lineage=lineage).points[0]
        assert point.valuation_status == "incomplete"
        assert point.fee_pnl is None
        assert all(bucket.realized_pnl is None for bucket in point.attribution)
        assert all(bucket.fee_pnl is None for bucket in point.attribution)
        assert all(bucket.accrual_pnl is None for bucket in point.attribution)
        assert all(bucket.unrealized_pnl is None for bucket in point.attribution)

    def test_reversed_cancellation_cannot_transport_fee_residue(self) -> None:
        """Large cancellation cannot fabricate a zero-fee final bucket."""
        executions = (
            _exec("I1", 1, 0, "buy", 0.0, 100.0, fee=1e20),
            _exec("I2", 2, 0, "buy", 0.0, 100.0, fee=5000.0),
            _exec("I3", 3, 0, "buy", 0.0, 100.0, fee=-1e20),
            _exec("I4", 4, 0, "buy", 0.0, 100.0, fee=0.0),
        )
        lineage = {
            "order-I1-1": TimelineExecutionLineage("strategy", "plan-1", None, "live", None),
            "order-I2-2": TimelineExecutionLineage("rest", None, None, "live", None),
            "order-I3-3": TimelineExecutionLineage("strategy", "plan-1", None, "live", None),
            "order-I4-4": TimelineExecutionLineage("strategy", None, None, "live", None),
        }
        point = build_pnl_timeline(executions, (), {}, _window(0, 0), lineage=lineage).points[0]
        assert point.valuation_status == "incomplete"
        assert point.fee_pnl is None
        assert all(bucket.fee_pnl is None for bucket in point.attribution)

    def test_unrepresentable_unrealized_residue_withholds_the_point(self) -> None:
        """Unrealized attribution also fails closed under catastrophic cancellation."""
        executions = (
            _exec("I1", 1, 0, "buy", 1.0, 1.0),
            _exec("I2", 2, 0, "buy", 1.0, 1e20),
            _exec("I3", 3, 0, "buy", 1.0, 1.0),
        )
        lineage = {
            "order-I1-1": TimelineExecutionLineage("rest", None, None, "live", None),
            "order-I2-2": TimelineExecutionLineage(
                "strategy", None, "signal-1", "live", "momentum"
            ),
            "order-I3-3": TimelineExecutionLineage(
                "strategy", None, "signal-1", "live", "momentum"
            ),
        }
        marks: MarkMap = {
            ("I1", _m(0)): 1e20,
            ("I2", _m(0)): 1.0,
            ("I3", _m(0)): 2.0,
        }
        point = build_pnl_timeline(executions, (), marks, _window(0, 0), lineage=lineage).points[0]
        assert point.valuation_status == "incomplete"
        assert point.unrealized_pnl is None
        assert all(bucket.unrealized_pnl is None for bucket in point.attribution)

    def test_adjacent_final_float_can_absorb_residue_exactly(self) -> None:
        """The final key takes a one-ULP correction when direct subtraction differs."""
        keys: list[AttributionKey] = [("manual", None), ("plan", None), ("system", None)]
        values = {
            keys[0]: -552.6911013183604,
            keys[1]: -647.3623389539209,
            keys[2]: 619.8804889202258,
        }
        total = 0.0
        for key in keys:
            total += values[key]
        reconciled = _values_with_residue(values, keys, total)
        assert reconciled is not None
        assert reconciled[keys[-1]] == math.nextafter(values[keys[-1]], -math.inf)
        assert sum(reconciled[key] for key in keys) == total

    def test_nonfinite_direct_residue_cannot_be_reconciled(self) -> None:
        """An overflowing final subtraction is rejected rather than transported."""
        keys: list[AttributionKey] = [("manual", None), ("system", None)]
        values = {keys[0]: -1e308, keys[1]: 0.0}
        assert _values_with_residue(values, keys, 1e308) is None

    @pytest.mark.parametrize(
        ("values", "total"),
        (
            ({("manual", None): 0.0}, math.inf),
            ({("manual", None): math.inf}, 0.0),
        ),
    )
    def test_nonfinite_total_or_final_value_cannot_be_reconciled(
        self,
        values: dict[AttributionKey, float],
        total: float,
    ) -> None:
        """Neither side of reconciliation may contain a non-finite value."""
        assert _values_with_residue(values, [("manual", None)], total) is None

    def test_large_pro_rata_reduction_preserves_each_proven_owner(self) -> None:
        """Representable shares survive multiply-first intermediate overflow."""
        weight = 1e200
        executions = (
            _exec("I1", 1, 0, "buy", weight, 1.0),
            _exec("I1", 2, 0, "buy", 1.0, 1.0),
            _exec("I1", 3, 0, "buy", weight, 1.0),
            _exec("I1", 4, 0, "sell", weight, 1.0),
        )
        lineage = {
            "order-I1-1": TimelineExecutionLineage("strategy", None, None, "live", None),
            "order-I1-2": TimelineExecutionLineage("strategy", "plan-1", None, "live", None),
            "order-I1-3": TimelineExecutionLineage("rest", None, None, "live", None),
            "order-I1-4": TimelineExecutionLineage("rest", None, None, "live", None),
        }
        marks: MarkMap = {("I1", _m(0)): 2.0}
        point = build_pnl_timeline(executions, (), marks, _window(0, 0), lineage=lineage).points[0]
        buckets = {(bucket.origin, bucket.strategy_name): bucket for bucket in point.attribution}
        assert point.valuation_status == "complete"
        assert buckets[("manual", None)].unrealized_pnl == pytest.approx(weight / 2.0)
        assert buckets[("plan", None)].unrealized_pnl == pytest.approx(0.5)
        assert buckets[("system", None)].unrealized_pnl == pytest.approx(weight / 2.0)
        _assert_exact_attribution_sums(point)


class TestRoundTrip:
    """Cover a complete open-hold-close cycle valued against marks."""

    def _result(self) -> tuple[TimelineExecution, ...]:
        """Return the buy-then-sell round-trip executions."""
        return (
            _exec("I1", 1, 0, "buy", 2.0, 100.0, fee=0.5),
            _exec("I1", 2, 2, "sell", 2.0, 110.0, fee=0.5),
        )

    def test_open_point_values_unrealized_and_fee(self) -> None:
        """The opening point carries the held unrealized minus the paid fee."""
        marks: MarkMap = {("I1", _m(0)): 105.0, ("I1", _m(1)): 106.0}
        result = build_pnl_timeline(self._result(), (), marks, _window())
        point = result.points[0]
        assert point.realized_pnl == 0.0
        assert point.fee_pnl == pytest.approx(-0.5)
        assert point.unrealized_pnl == pytest.approx(10.0)
        assert point.net_pnl == pytest.approx(9.5)
        assert point.valuation_status == "complete"

    def test_hold_point_tracks_mark_move(self) -> None:
        """A held minute revalues the open position against its own mark."""
        marks: MarkMap = {("I1", _m(0)): 105.0, ("I1", _m(1)): 106.0}
        result = build_pnl_timeline(self._result(), (), marks, _window())
        assert result.points[1].unrealized_pnl == pytest.approx(12.0)
        assert result.points[1].net_pnl == pytest.approx(11.5)

    def test_close_point_realizes_and_nets_fees(self) -> None:
        """Closing realizes price P&L and the flat book needs no mark."""
        marks: MarkMap = {("I1", _m(0)): 105.0, ("I1", _m(1)): 106.0}
        result = build_pnl_timeline(self._result(), (), marks, _window())
        point = result.points[2]
        assert point.realized_pnl == pytest.approx(20.0)
        assert point.fee_pnl == pytest.approx(-1.0)
        assert point.unrealized_pnl == 0.0
        assert point.net_pnl == pytest.approx(19.0)
        assert point.valuation_status == "complete"

    def test_post_close_points_are_flat_and_stable(self) -> None:
        """After the close every later minute holds the realized net."""
        marks: MarkMap = {("I1", _m(0)): 105.0, ("I1", _m(1)): 106.0}
        result = build_pnl_timeline(self._result(), (), marks, _window())
        assert [p.net_pnl for p in result.points[3:]] == pytest.approx([19.0, 19.0, 19.0])

    def test_flat_instrument_still_reports_cumulative_contribution(self) -> None:
        """A closed instrument keeps its realized contribution with zero unrealized."""
        marks: MarkMap = {("I1", _m(0)): 105.0, ("I1", _m(1)): 106.0}
        result = build_pnl_timeline(self._result(), (), marks, _window())
        contribution = result.points[2].per_instrument[0]
        assert contribution.instrument_public_id == "I1"
        assert contribution.realized_pnl == pytest.approx(20.0)
        assert contribution.fee_pnl == pytest.approx(-1.0)
        assert contribution.unrealized_pnl == 0.0


class TestIncompleteMarks:
    """Cover the honest-incomplete valuation gates."""

    def test_missing_mark_on_held_instrument_is_incomplete(self) -> None:
        """A held minute without a mark yields incomplete with realized intact."""
        executions = (_exec("I1", 1, 0, "buy", 2.0, 100.0, fee=0.5),)
        marks: MarkMap = {("I1", _m(0)): 105.0}
        result = build_pnl_timeline(executions, (), marks, _window())
        point = result.points[1]
        assert point.valuation_status == "incomplete"
        assert point.unrealized_pnl is None
        assert point.net_pnl is None
        assert point.fee_pnl == pytest.approx(-0.5)
        assert point.realized_pnl == 0.0
        assert point.attribution[0].realized_pnl == 0.0
        assert point.attribution[0].fee_pnl == pytest.approx(-0.5)
        assert point.attribution[0].accrual_pnl == 0.0
        assert point.attribution[0].unrealized_pnl is None

    def test_none_valued_mark_is_treated_as_absent(self) -> None:
        """An explicit ``None`` mark is the same as a missing one."""
        executions = (_exec("I1", 1, 0, "buy", 2.0, 100.0),)
        marks: MarkMap = {("I1", _m(0)): None}
        result = build_pnl_timeline(executions, (), marks, _window())
        assert result.points[0].valuation_status == "incomplete"


class TestOpeningAnchor:
    """Cover anchor seeding and the baseline-leakage guard."""

    def test_opening_plots_change_from_baseline(self) -> None:
        """Seeded unrealized nets only the change since the anchor baseline."""
        opening = TimelineOpening(
            positions={"I1": OpeningPosition(position_qty=1.0, entry_price=100.0)},
            opening_unrealized_value=5.0,
            t0=_m(-10),
        )
        marks: MarkMap = {("I1", _m(0)): 110.0}
        result = build_pnl_timeline((), (), marks, _window(0, 0), opening=opening)
        point = result.points[0]
        assert point.unrealized_pnl == pytest.approx(10.0)
        assert point.net_pnl == pytest.approx(5.0)

    def test_seeded_position_without_entry_is_incomplete(self) -> None:
        """A held seed whose entry is unknown cannot be valued."""
        opening = TimelineOpening(
            positions={"I1": OpeningPosition(position_qty=1.0, entry_price=None)},
            opening_unrealized_value=0.0,
            t0=_m(-10),
        )
        marks: MarkMap = {("I1", _m(0)): 110.0}
        result = build_pnl_timeline((), (), marks, _window(0, 0), opening=opening)
        assert result.points[0].valuation_status == "incomplete"


class TestShortSide:
    """Cover short-side valuation and the sell-signed replay."""

    def test_open_short_profits_when_price_drops(self) -> None:
        """A held short marks up when the price falls below entry."""
        executions = (_exec("I1", 1, 0, "sell", 2.0, 100.0),)
        marks: MarkMap = {("I1", _m(0)): 90.0}
        result = build_pnl_timeline(executions, (), marks, _window(0, 0))
        assert result.points[0].unrealized_pnl == pytest.approx(20.0)


class TestAccruals:
    """Cover the separate funding accrual component."""

    def test_positive_accrual_reduces_net(self) -> None:
        """A holder-pays accrual contributes a negative P&L at its minute."""
        accruals = (TimelineAccrual(instrument_public_id="I1", accrued_at=_m(1), amount_usd=3.0),)
        result = build_pnl_timeline((), accruals, {}, _window())
        assert result.points[0].accrual_pnl == 0.0
        assert result.points[1].accrual_pnl == pytest.approx(-3.0)
        assert result.points[1].net_pnl == pytest.approx(-3.0)
        contribution = result.points[1].per_instrument[0]
        assert contribution.instrument_public_id == "I1"
        assert contribution.accrual_pnl == pytest.approx(-3.0)
        assert contribution.unrealized_pnl == 0.0

    def test_accrual_only_instrument_absent_before_its_minute(self) -> None:
        """An accrual-only instrument appears only once its accrual applies."""
        accruals = (TimelineAccrual(instrument_public_id="I1", accrued_at=_m(1), amount_usd=3.0),)
        result = build_pnl_timeline((), accruals, {}, _window())
        assert result.points[0].per_instrument == ()


class TestRegressionGuard:
    """Cover the event-time regression shadow taint."""

    def _executions(self) -> tuple[TimelineExecution, ...]:
        """Return a scope-ordered stream whose second fill regresses in time."""
        return (
            _exec("I1", 1, 3, "buy", 2.0, 100.0),
            _exec("I1", 2, 1, "buy", 2.0, 100.0),
            _exec("I1", 3, 4, "buy", 1.0, 100.0),
        )

    def test_shadowed_minutes_withhold_every_component(self) -> None:
        """Minutes shadowed by the clamped regression are fully untrusted.

        A scope-order regression makes the cumulatives themselves unreliable at
        the shadowed instant (an economically-present fill is not yet applied), so
        realized / fee / accrual / unrealized / net are ALL withheld, not just the
        unrealized.
        """
        marks: MarkMap = {("I1", _m(3)): 100.0, ("I1", _m(4)): 100.0, ("I1", _m(5)): 100.0}
        result = build_pnl_timeline(self._executions(), (), marks, _window())
        assert result.points[1].valuation_status == "incomplete"
        assert result.points[2].valuation_status == "incomplete"
        assert result.points[1].unrealized_pnl is None
        assert result.points[1].realized_pnl is None
        assert result.points[1].fee_pnl is None
        assert result.points[1].accrual_pnl is None
        assert result.points[1].net_pnl is None
        assert result.points[1].per_instrument == ()

    def test_unshadowed_minutes_stay_complete(self) -> None:
        """Minutes outside the shadow window value normally after the clamp."""
        marks: MarkMap = {("I1", _m(3)): 100.0, ("I1", _m(4)): 100.0, ("I1", _m(5)): 100.0}
        result = build_pnl_timeline(self._executions(), (), marks, _window())
        assert result.points[0].valuation_status == "complete"
        assert result.points[3].valuation_status == "complete"
        assert result.points[4].valuation_status == "complete"


class TestDownsampling:
    """Cover the flow/stock endpoint downsampling reduction."""

    def _executions(self) -> tuple[TimelineExecution, ...]:
        """Return the round-trip executions reused for downsampling."""
        return (
            _exec("I1", 1, 0, "buy", 2.0, 100.0, fee=0.5),
            _exec("I1", 2, 2, "sell", 2.0, 110.0, fee=0.5),
        )

    def test_five_minute_buckets_take_endpoints(self) -> None:
        """5m granularity emits the last 1m point of each bucket."""
        marks: MarkMap = {("I1", _m(0)): 105.0, ("I1", _m(1)): 106.0}
        result = build_pnl_timeline(self._executions(), (), marks, _window(0, 5, "5m"))
        assert [p.point_time for p in result.points] == [_m(4), _m(5)]
        assert result.points[0].net_pnl == pytest.approx(19.0)

    def test_daily_bucket_collapses_to_one_endpoint(self) -> None:
        """1d granularity collapses the window to a single endpoint point."""
        marks: MarkMap = {("I1", _m(0)): 105.0, ("I1", _m(1)): 106.0}
        result = build_pnl_timeline(self._executions(), (), marks, _window(0, 5, "1d"))
        assert len(result.points) == 1
        assert result.points[0].point_time == _m(5)

    def test_hourly_bucket_collapses_to_one_endpoint(self) -> None:
        """1h granularity collapses the six-minute window to one point."""
        marks: MarkMap = {("I1", _m(0)): 105.0, ("I1", _m(1)): 106.0}
        result = build_pnl_timeline(self._executions(), (), marks, _window(0, 5, "1h"))
        assert len(result.points) == 1
        assert result.points[0].point_time == _m(5)


class TestEdges:
    """Cover the degenerate window and invalid-granularity edges."""

    def test_empty_window_yields_no_points(self) -> None:
        """A window whose end precedes its start emits no points."""
        result = build_pnl_timeline((), (), {}, _window(5, 0))
        assert result.points == ()

    def test_flat_book_needs_no_marks(self) -> None:
        """With nothing held every point is complete at zero net."""
        result = build_pnl_timeline((), (), {}, _window(0, 1))
        assert [p.net_pnl for p in result.points] == pytest.approx([0.0, 0.0])
        assert result.points[0].valuation_status == "complete"

    def test_unsupported_granularity_raises(self) -> None:
        """An unknown granularity is rejected before any work."""
        with pytest.raises(ValueError, match="unsupported granularity"):
            build_pnl_timeline((), (), {}, _window(0, 5, "2w"))

    def test_result_echoes_window_metadata(self) -> None:
        """The result carries the requested granularity and valuation currency."""
        result = build_pnl_timeline((), (), {}, _window(0, 1))
        assert result.granularity == "1m"
        assert result.valuation_ccy == "USD"


class TestUntrustedCumulatives:
    """Cover the fully-untrusted paths: unknown seeded cost basis and NaN marks."""

    def test_unknown_seeded_basis_held_is_untrusted(self) -> None:
        """A non-flat opening position with no entry price withholds every component.

        A second flat opening position with no entry price is NOT flagged, so the
        seed guard distinguishes an unknown-cost holding from a flat placeholder.
        """
        opening = TimelineOpening(
            positions={
                "I1": OpeningPosition(position_qty=1.0, entry_price=None),
                "I2": OpeningPosition(position_qty=0.0, entry_price=None),
            },
            opening_unrealized_value=5.0,
            t0=_m(0),
        )
        result = build_pnl_timeline((), (), {("I1", _m(0)): 105.0}, _window(), opening)
        assert all(point.valuation_status == "incomplete" for point in result.points)
        assert result.points[0].realized_pnl is None
        assert result.points[0].net_pnl is None

    def test_unknown_seeded_basis_realization_permanently_taints(self) -> None:
        """Selling unknown-basis inventory taints realized for the rest of the series.

        The kernel cannot realize against an unknown entry, so a close that flushes
        the position flat still leaves the cumulative realized unknowable — every
        later point stays untrusted even though the pool is now flat.
        """
        opening = TimelineOpening(
            positions={"I1": OpeningPosition(position_qty=1.0, entry_price=None)},
            opening_unrealized_value=5.0,
            t0=_m(0),
        )
        executions = (_exec("I1", 1, 1, "sell", 1.0, 110.0),)
        result = build_pnl_timeline(executions, (), {}, _window(), opening)
        assert all(point.valuation_status == "incomplete" for point in result.points)
        assert result.points[5].realized_pnl is None

    def test_unknown_basis_add_stays_untrusted(self) -> None:
        """Adding to unknown-basis inventory keeps the position untrusted.

        A same-direction add neither realizes nor flushes the position flat, so the
        unknown seeded cost is still baked into the pool and every point remains
        untrusted.
        """
        opening = TimelineOpening(
            positions={"I1": OpeningPosition(position_qty=1.0, entry_price=None)},
            opening_unrealized_value=5.0,
            t0=_m(0),
        )
        executions = (_exec("I1", 1, 1, "buy", 1.0, 100.0),)
        result = build_pnl_timeline(executions, (), {}, _window(), opening)
        assert all(point.valuation_status == "incomplete" for point in result.points)

    def test_non_finite_mark_is_mark_incomplete_not_untrusted(self) -> None:
        """A NaN mark withholds the unrealized but KEEPS the trusted cumulatives."""
        executions = (_exec("I1", 1, 0, "buy", 1.0, 100.0, fee=0.5),)
        marks: MarkMap = {("I1", _m(0)): float("nan")}
        result = build_pnl_timeline(executions, (), marks, _window())
        point = result.points[0]
        assert point.valuation_status == "incomplete"
        assert point.unrealized_pnl is None
        assert point.net_pnl is None
        assert point.realized_pnl == 0.0
        assert point.fee_pnl == pytest.approx(-0.5)


class TestActivationBaseline:
    """Cover the pre-activation accrual filter (net P&L starts at zero at t0)."""

    def test_pre_t0_accrual_is_dropped(self) -> None:
        """An accrual before the anchor t0 does not contaminate the baseline.

        The assertion reads at and after t0 (points before t0 are withheld as
        untrusted), where the dropped pre-t0 accrual must leave the accrual
        component and net P&L at zero.
        """
        opening = TimelineOpening(positions={}, opening_unrealized_value=0.0, t0=_m(2))
        accruals = (TimelineAccrual(instrument_public_id="I1", accrued_at=_m(1), amount_usd=3.0),)
        result = build_pnl_timeline((), accruals, {}, _window(), opening)
        assert result.points[2].accrual_pnl == 0.0
        assert result.points[5].accrual_pnl == 0.0
        assert result.points[5].net_pnl == 0.0

    def test_post_t0_accrual_is_kept(self) -> None:
        """An accrual at or after the anchor t0 contributes normally."""
        opening = TimelineOpening(positions={}, opening_unrealized_value=0.0, t0=_m(0))
        accruals = (TimelineAccrual(instrument_public_id="I1", accrued_at=_m(1), amount_usd=3.0),)
        result = build_pnl_timeline((), accruals, {}, _window(), opening)
        assert result.points[0].accrual_pnl == 0.0
        assert result.points[1].accrual_pnl == pytest.approx(-3.0)


class TestUntrustedEdgeCases:
    """Cover the residual untrusted holes closed after the second review pass."""

    def test_exact_epsilon_close_of_unknown_basis_stays_untrusted(self) -> None:
        """An exact-FLAT_EPSILON close against unknown basis still taints realized.

        Any POSITIVE realization against an unknown entry is unknowable, so the
        taint keys on ``closed_qty > 0``, not ``> FLAT_EPSILON`` — otherwise a
        boundary-sized close would flush the position flat and escape the taint.
        """
        opening = TimelineOpening(
            positions={"I1": OpeningPosition(position_qty=2 * FLAT_EPSILON, entry_price=None)},
            opening_unrealized_value=0.0,
            t0=_m(0),
        )
        executions = (
            _exec("I1", 1, 1, "sell", FLAT_EPSILON, 100.0),
            _exec("I1", 2, 2, "sell", FLAT_EPSILON, 100.0),
        )
        result = build_pnl_timeline(executions, (), {}, _window(), opening)
        assert all(point.valuation_status == "incomplete" for point in result.points)
        assert result.points[5].realized_pnl is None

    def test_nan_fee_makes_point_untrusted(self) -> None:
        """A non-finite fee corrupts the cumulative and withholds every component."""
        executions = (_exec("I1", 1, 0, "buy", 1.0, 100.0, fee=float("nan")),)
        marks: MarkMap = {("I1", _m(0)): 100.0}
        result = build_pnl_timeline(executions, (), marks, _window())
        point = result.points[0]
        assert point.valuation_status == "incomplete"
        assert point.realized_pnl is None
        assert point.fee_pnl is None
        assert len(point.attribution) == 1
        assert point.attribution[0].origin == "unattributed"
        assert point.attribution[0].realized_pnl is None
        assert point.attribution[0].fee_pnl is None
        assert point.attribution[0].accrual_pnl is None
        assert point.attribution[0].unrealized_pnl is None

    def test_nan_entry_price_is_mark_incomplete(self) -> None:
        """A position opened at a NaN price withholds only its unrealized."""
        executions = (_exec("I1", 1, 0, "buy", 1.0, float("nan")),)
        marks: MarkMap = {("I1", _m(0)): 100.0}
        result = build_pnl_timeline(executions, (), marks, _window())
        point = result.points[0]
        assert point.valuation_status == "incomplete"
        assert point.unrealized_pnl is None
        assert point.realized_pnl == 0.0

    def test_overflow_unrealized_is_mark_incomplete(self) -> None:
        """A finite mark and entry whose difference overflows withhold the unrealized."""
        executions = (_exec("I1", 1, 0, "buy", 1.0, 1e308),)
        marks: MarkMap = {("I1", _m(0)): -1e308}
        result = build_pnl_timeline(executions, (), marks, _window())
        point = result.points[0]
        assert point.valuation_status == "incomplete"
        assert point.unrealized_pnl is None
        assert point.realized_pnl == 0.0


class TestActivationBoundary:
    """Cover the pre-activation withholding (no backward certification of t0)."""

    def test_pre_activation_points_are_withheld(self) -> None:
        """Grid points before the anchor t0 are untrusted; t0 itself values normally.

        The anchor proves no position state before t0, so valuing a pre-t0 minute
        from the seeded book would fabricate history. The point at t0 (the seed's
        own instant) is complete.
        """
        opening = TimelineOpening(
            positions={"I1": OpeningPosition(position_qty=1.0, entry_price=100.0)},
            opening_unrealized_value=5.0,
            t0=_m(2),
        )
        marks: MarkMap = {("I1", _m(0)): 90.0, ("I1", _m(1)): 95.0, ("I1", _m(2)): 105.0}
        result = build_pnl_timeline((), (), marks, _window(), opening)
        assert result.points[0].valuation_status == "incomplete"
        assert result.points[1].valuation_status == "incomplete"
        assert result.points[0].net_pnl is None
        assert result.points[2].valuation_status == "complete"
        assert result.points[2].net_pnl == pytest.approx(0.0)


class TestCorruptInputGuards:
    """Cover the fabrication/sign-dimension input guards and summation overflow."""

    def test_non_finite_seed_quantity_is_untrusted(self) -> None:
        """A seed with a non-finite quantity is tainted, never replayed.

        A NaN quantity would fabricate a finite-but-bogus realized on close (the
        kernel compares against NaN), so the instrument is withheld at ingestion.
        """
        opening = TimelineOpening(
            positions={"I1": OpeningPosition(position_qty=float("nan"), entry_price=100.0)},
            opening_unrealized_value=0.0,
            t0=_m(0),
        )
        result = build_pnl_timeline((), (), {("I1", _m(0)): 110.0}, _window(), opening)
        assert all(point.valuation_status == "incomplete" for point in result.points)
        assert result.points[0].realized_pnl is None

    def test_negative_fill_size_is_untrusted(self) -> None:
        """A negative fill size is tainted, not sign-inverted into a short."""
        executions = (_exec("I1", 1, 0, "buy", -1.0, 100.0),)
        result = build_pnl_timeline(executions, (), {("I1", _m(0)): 90.0}, _window())
        assert all(point.valuation_status == "incomplete" for point in result.points)

    def test_aggregate_unrealized_overflow_is_incomplete(self) -> None:
        """Finite per-instrument unrealizeds that overflow when summed withhold net.

        Two positions near +1e308 and two near -1e308 sum to zero in exact math,
        but floating accumulation overflows to inf; the aggregate must be withheld
        while the mark-independent cumulatives survive.
        """
        executions = (
            _exec("I1", 1, 0, "buy", 1.0, 1.0),
            _exec("I2", 2, 0, "buy", 1.0, 1.0),
            _exec("I3", 3, 0, "buy", 1.0, 1e308),
            _exec("I4", 4, 0, "buy", 1.0, 1e308),
        )
        marks: MarkMap = {
            ("I1", _m(0)): 1e308,
            ("I2", _m(0)): 1e308,
            ("I3", _m(0)): 1.0,
            ("I4", _m(0)): 1.0,
        }
        result = build_pnl_timeline(executions, (), marks, _window(0, 0))
        point = result.points[0]
        assert point.valuation_status == "incomplete"
        assert point.unrealized_pnl is None
        assert point.net_pnl is None
        assert point.realized_pnl == 0.0

    def test_net_overflow_from_finite_components_is_incomplete(self) -> None:
        """A finite realized and a finite unrealized whose sum overflows withhold net."""
        executions = (
            _exec("I1", 1, 0, "buy", 1.0, 1.0),
            _exec("I1", 2, 0, "sell", 1.0, 1e308),
            _exec("I2", 3, 0, "buy", 1.0, 1.0),
        )
        marks: MarkMap = {("I2", _m(0)): 1e308}
        result = build_pnl_timeline(executions, (), marks, _window(0, 0))
        point = result.points[0]
        assert point.valuation_status == "incomplete"
        assert point.realized_pnl == pytest.approx(1e308)
        assert point.net_pnl is None
        assert point.unrealized_pnl is None

    def test_per_instrument_fee_overflow_is_untrusted(self) -> None:
        """Interleaved fees keep the aggregate finite while one instrument overflows.

        Cancelling ±1e308 fees across two instruments leave the summed fee total at
        zero, yet each instrument's independently accumulated fee overflows to ±inf.
        The per-instrument finiteness gate withholds the whole point so no complete
        contribution ever carries a non-finite number.
        """
        executions = (
            _exec("I1", 1, 0, "buy", 1.0, 100.0, fee=-1e308),
            _exec("I2", 2, 0, "buy", 1.0, 100.0, fee=1e308),
            _exec("I1", 3, 0, "sell", 1.0, 100.0, fee=-1e308),
            _exec("I2", 4, 0, "sell", 1.0, 100.0, fee=1e308),
        )
        result = build_pnl_timeline(executions, (), {}, _window(0, 0))
        point = result.points[0]
        assert point.valuation_status == "incomplete"
        assert point.fee_pnl is None
        assert all(contribution.fee_pnl is None for contribution in point.per_instrument)

    def test_per_attribution_fee_overflow_is_untrusted(self) -> None:
        """Composite overflow is caught even when instrument and total fees stay finite."""
        executions = (
            _exec("I1", 1, 0, "buy", 0.0, 100.0, fee=-1e308),
            _exec("I2", 2, 0, "buy", 0.0, 100.0, fee=1e308),
            _exec("I3", 3, 0, "buy", 0.0, 100.0, fee=-1e308),
            _exec("I4", 4, 0, "buy", 0.0, 100.0, fee=1e308),
        )
        lineage = {
            "order-I1-1": TimelineExecutionLineage("rest", None, None, "live", None),
            "order-I2-2": TimelineExecutionLineage(
                "strategy", None, "signal-1", "live", "momentum"
            ),
            "order-I3-3": TimelineExecutionLineage("rest", None, None, "live", None),
            "order-I4-4": TimelineExecutionLineage(
                "strategy", None, "signal-1", "live", "momentum"
            ),
        }
        point = build_pnl_timeline(executions, (), {}, _window(0, 0), lineage=lineage).points[0]
        assert point.valuation_status == "incomplete"
        assert point.fee_pnl is None
        assert {bucket.origin for bucket in point.attribution} == {"manual", "system"}
        assert all(bucket.fee_pnl is None for bucket in point.attribution)
