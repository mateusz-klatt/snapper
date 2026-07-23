"""Tests for the pure P&L timeline series builder (Phase 5A core).

Exercises :func:`build_pnl_timeline` across the soundness contract: the shared
average-cost replay (realized decomposition), the separate fee and accrual
components with their expense / holder-pays signs, anchor seeding with the
shard-aware t0 rebase guard, honest-incomplete valuation on missing marks and
unknown seeded entries, the event-time regression shadow guard, the flow/stock
downsampling reduction, and the degenerate empty and invalid-granularity edges.
"""

import math
from collections.abc import Iterable
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from datetime import timezone

import pytest

from snapper.api.schemas.pnl_timeline import PnlIncompletenessReasonData
from snapper.application.portfolio.average_cost import FLAT_EPSILON
from snapper.application.portfolio.pnl_timeline import AttributionKey
from snapper.application.portfolio.pnl_timeline import MarkMap
from snapper.application.portfolio.pnl_timeline import OpeningPool
from snapper.application.portfolio.pnl_timeline import OpeningPoolValuation
from snapper.application.portfolio.pnl_timeline import PnlIncompletenessReason
from snapper.application.portfolio.pnl_timeline import PnlIncompletenessReasonEntry
from snapper.application.portfolio.pnl_timeline import PnlTimelinePoint
from snapper.application.portfolio.pnl_timeline import TimelineAccrual
from snapper.application.portfolio.pnl_timeline import TimelineExecution
from snapper.application.portfolio.pnl_timeline import TimelineExecutionLineage
from snapper.application.portfolio.pnl_timeline import TimelineOpening
from snapper.application.portfolio.pnl_timeline import TimelineOpeningDerivation
from snapper.application.portfolio.pnl_timeline import TimelineWindow
from snapper.application.portfolio.pnl_timeline import _allocate_by_weights
from snapper.application.portfolio.pnl_timeline import _Pool
from snapper.application.portfolio.pnl_timeline import _prepare_executions
from snapper.application.portfolio.pnl_timeline import _reconcile_weights
from snapper.application.portfolio.pnl_timeline import _regression_shadow_deltas
from snapper.application.portfolio.pnl_timeline import _regression_shadow_trigger_changes
from snapper.application.portfolio.pnl_timeline import _RegressionShadow
from snapper.application.portfolio.pnl_timeline import _valuation_pool_index
from snapper.application.portfolio.pnl_timeline import _value_point
from snapper.application.portfolio.pnl_timeline import _values_with_residue
from snapper.application.portfolio.pnl_timeline import build_pnl_timeline
from snapper.application.portfolio.pnl_timeline import canonical_incompleteness_reasons
from snapper.application.portfolio.pnl_timeline import derive_timeline_opening

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
    position_delta: float | None = None,
    shard_key: str | None = None,
) -> TimelineExecution:
    """Build one timeline execution at the given grid minute."""
    resolved_position_delta = (
        position_delta if position_delta is not None else size if side == "buy" else -size
    )
    return TimelineExecution(
        instrument_public_id=instrument,
        shard_key=shard_key or f"shard-{instrument}",
        exchange=exchange,
        scope_sequence=scope,
        event_time=_m(minute),
        side=side,
        size=size,
        position_delta=resolved_position_delta,
        price=price,
        fee=fee,
        fee_asset="USD",
        order_public_id=f"order-{instrument}-{scope}",
    )


def _opening_pool(
    instrument: str,
    position_qty: float,
    entry_price: float | None,
    *,
    shard_key: str | None = None,
    exchange: str = "kraken",
) -> OpeningPool:
    """Build one stable opening shard seed."""
    return OpeningPool(
        instrument_public_id=instrument,
        shard_key=shard_key or f"shard-{instrument}",
        exchange=exchange,
        position_qty=position_qty,
        entry_price=entry_price,
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
    """Assert exact instrument/attribution sums and the direct public net identity."""
    instrument_realized = [contribution.realized_pnl for contribution in point.per_instrument]
    instrument_fees = [contribution.fee_pnl for contribution in point.per_instrument]
    instrument_accruals = [contribution.accrual_pnl for contribution in point.per_instrument]
    instrument_unrealized = [contribution.unrealized_pnl for contribution in point.per_instrument]
    realized = [contribution.realized_pnl for contribution in point.attribution]
    fees = [contribution.fee_pnl for contribution in point.attribution]
    accruals = [contribution.accrual_pnl for contribution in point.attribution]
    unrealized = [contribution.unrealized_pnl for contribution in point.attribution]
    assert None not in instrument_realized
    assert None not in instrument_fees
    assert None not in instrument_accruals
    assert None not in instrument_unrealized
    assert None not in realized
    assert None not in fees
    assert None not in accruals
    assert None not in unrealized
    assert sum(value for value in instrument_realized if value is not None) == point.realized_pnl
    assert sum(value for value in instrument_fees if value is not None) == point.fee_pnl
    assert sum(value for value in instrument_accruals if value is not None) == point.accrual_pnl
    assert (
        sum(value for value in instrument_unrealized if value is not None) == point.unrealized_pnl
    )
    assert sum(value for value in realized if value is not None) == point.realized_pnl
    assert sum(value for value in fees if value is not None) == point.fee_pnl
    assert sum(value for value in accruals if value is not None) == point.accrual_pnl
    assert sum(value for value in unrealized if value is not None) == point.unrealized_pnl
    assert point.net_pnl == (
        point.realized_pnl + point.fee_pnl + point.accrual_pnl + point.unrealized_pnl
    )


def _reason_rows(point: PnlTimelinePoint) -> list[tuple[str, str, str, str | None]]:
    """Project structured point reasons into concise assertion rows."""
    return [
        (
            entry.reason,
            entry.withholding_tier,
            entry.withholding_scope,
            entry.trigger_instrument_public_id,
        )
        for entry in point.incompleteness_reasons
    ]


class TestMachineReadableIncompletenessReasons:
    """Pin causal threading, canonical form, and reason/status equivalence."""

    def test_domain_rejects_both_status_reason_mismatches(self) -> None:
        """Complete-with-reason and incomplete-without-reason both fail loudly."""
        reason = PnlIncompletenessReasonEntry(
            reason="before_activation",
            withholding_tier="untrusted",
            withholding_scope="global",
            trigger_instrument_public_id=None,
        )
        with pytest.raises(ValueError, match="complete point cannot carry"):
            PnlTimelinePoint(
                point_time=_m(0),
                realized_pnl=None,
                fee_pnl=None,
                accrual_pnl=None,
                unrealized_pnl=None,
                net_pnl=None,
                valuation_status="complete",
                incompleteness_reasons=(reason,),
                per_instrument=(),
                attribution=(),
            )
        with pytest.raises(ValueError, match="incomplete point must carry"):
            PnlTimelinePoint(
                point_time=_m(0),
                realized_pnl=None,
                fee_pnl=None,
                accrual_pnl=None,
                unrealized_pnl=None,
                net_pnl=None,
                valuation_status="incomplete",
                incompleteness_reasons=(),
                per_instrument=(),
                attribution=(),
            )

    def test_domain_rejects_noncanonical_and_instrumentless_reasons(self) -> None:
        """Instrument scope needs identity and point reasons must be sorted and unique."""
        with pytest.raises(ValueError, match="requires a triggering instrument"):
            PnlIncompletenessReasonEntry(
                reason="mark_unavailable",
                withholding_tier="mark_incomplete",
                withholding_scope="instrument",
                trigger_instrument_public_id=None,
            )
        first = PnlIncompletenessReasonEntry(
            reason="execution_size_invalid",
            withholding_tier="untrusted",
            withholding_scope="instrument",
            trigger_instrument_public_id="I2",
        )
        second = PnlIncompletenessReasonEntry(
            reason="before_activation",
            withholding_tier="untrusted",
            withholding_scope="global",
            trigger_instrument_public_id=None,
        )
        third = PnlIncompletenessReasonEntry(
            reason="fx_conversion_unproven",
            withholding_tier="untrusted",
            withholding_scope="instrument",
            trigger_instrument_public_id="I1",
        )
        fourth = PnlIncompletenessReasonEntry(
            reason="mark_unavailable",
            withholding_tier="mark_incomplete",
            withholding_scope="instrument",
            trigger_instrument_public_id="I1",
        )
        fifth = PnlIncompletenessReasonEntry(
            reason="execution_price_invalid",
            withholding_tier="mark_incomplete",
            withholding_scope="instrument",
            trigger_instrument_public_id="I1",
        )
        with pytest.raises(ValueError, match="deduplicated and sorted"):
            PnlTimelinePoint(
                point_time=_m(0),
                realized_pnl=None,
                fee_pnl=None,
                accrual_pnl=None,
                unrealized_pnl=None,
                net_pnl=None,
                valuation_status="incomplete",
                incompleteness_reasons=(first, second, first),
                per_instrument=(),
                attribution=(),
            )
        assert canonical_incompleteness_reasons((first, third, fourth, second, fifth, first)) == (
            second,
            fifth,
            fourth,
            third,
            first,
        )

    def test_instrument_reconciliation_reason_is_shared_with_api_schema(self) -> None:
        """The new global reason belongs to both domain and transport contracts."""
        domain = PnlIncompletenessReasonEntry(
            reason="instrument_reconciliation_failed",
            withholding_tier="untrusted",
            withholding_scope="global",
            trigger_instrument_public_id=None,
        )
        transport = PnlIncompletenessReasonData(
            reason=domain.reason,
            withholding_tier=domain.withholding_tier,
            withholding_scope=domain.withholding_scope,
            trigger_instrument_public_id=domain.trigger_instrument_public_id,
        )
        assert canonical_incompleteness_reasons((domain,)) == (domain,)
        assert transport.reason == "instrument_reconciliation_failed"

    def test_mark_and_untrusted_instruments_coexist_without_precedence(self) -> None:
        """A missing mark on A and invalid close on B remain two causal entries."""
        executions = (
            _exec("I1", 1, 0, "buy", 1.0, 100.0),
            _exec("I2", 2, 0, "buy", 1.0, 50.0),
            _exec("I2", 3, 1, "sell", 1.0, float("nan")),
        )
        point = build_pnl_timeline(executions, (), {}, _window(1, 1)).points[0]
        assert _reason_rows(point) == [
            ("mark_unavailable", "mark_incomplete", "instrument", "I1"),
            ("execution_price_invalid", "untrusted", "instrument", "I2"),
        ]
        first, second = point.per_instrument
        assert first.instrument_public_id == "I1"
        assert first.realized_pnl == 0.0
        assert first.unrealized_pnl is None
        assert second.instrument_public_id == "I2"
        assert second.realized_pnl is None

    def test_invalid_close_preserves_distinct_preexisting_basis_cause(self) -> None:
        """A direct bad close does not erase an earlier FX-unprovable entry basis."""
        opening = TimelineExecution(
            instrument_public_id="I1",
            shard_key="shard-I1",
            exchange="kraken",
            scope_sequence=1,
            event_time=_m(0),
            side="buy",
            size=1.0,
            position_delta=1.0,
            price=float("nan"),
            fee=0.0,
            fee_asset="USD",
            order_public_id="order-I1-1",
            price_incompleteness_reason="fx_conversion_unproven",
        )
        closing = _exec("I1", 2, 1, "sell", 1.0, float("nan"))
        result = build_pnl_timeline(
            (opening, closing),
            (),
            {("I1", _m(0)): 100.0},
            _window(0, 1),
        )
        assert _reason_rows(result.points[0]) == [
            ("fx_conversion_unproven", "mark_incomplete", "instrument", "I1")
        ]
        assert _reason_rows(result.points[1]) == [
            ("execution_price_invalid", "untrusted", "instrument", "I1"),
            ("fx_conversion_unproven", "untrusted", "instrument", "I1"),
        ]

    def test_valid_close_of_bad_basis_reports_only_the_root_cause(self) -> None:
        """Synthetic NaN realization cannot create a derivative cumulative reason."""
        opening = TimelineExecution(
            instrument_public_id="I1",
            shard_key="shard-I1",
            exchange="kraken",
            scope_sequence=1,
            event_time=_m(0),
            side="buy",
            size=1.0,
            position_delta=1.0,
            price=float("nan"),
            fee=0.0,
            fee_asset="USD",
            order_public_id="order-I1-1",
            price_incompleteness_reason="fx_conversion_unproven",
        )
        closing = _exec("I1", 2, 1, "sell", 1.0, 110.0)
        point = build_pnl_timeline((opening, closing), (), {}, _window(1, 1)).points[0]
        assert _reason_rows(point) == [("fx_conversion_unproven", "untrusted", "instrument", "I1")]

    def test_global_early_return_keeps_latched_instrument_cause(self) -> None:
        """A regression shadow retains an independently established size cause."""
        executions = (
            _exec("I2", 1, 0, "buy", -1.0, 100.0),
            _exec("I1", 2, 3, "buy", 1.0, 100.0),
            _exec("I1", 3, 1, "buy", 1.0, 100.0),
        )
        point = build_pnl_timeline(executions, (), {}, _window(1, 1)).points[0]
        assert _reason_rows(point) == [
            ("scope_order_regression", "untrusted", "global", "I1"),
            ("execution_size_invalid", "untrusted", "instrument", "I2"),
        ]

    def test_downsampling_keeps_only_endpoint_reasons(self) -> None:
        """A hidden missing-mark minute cannot contaminate a complete endpoint."""
        execution = _exec("I1", 1, 0, "buy", 1.0, 100.0)
        marks: MarkMap = {
            ("I1", _m(1)): 101.0,
            ("I1", _m(2)): 102.0,
            ("I1", _m(3)): 103.0,
            ("I1", _m(4)): 104.0,
        }
        raw = build_pnl_timeline((execution,), (), marks, _window(0, 4))
        sampled = build_pnl_timeline((execution,), (), marks, _window(0, 4, "5m"))
        assert _reason_rows(raw.points[0]) == [
            ("mark_unavailable", "mark_incomplete", "instrument", "I1")
        ]
        assert raw.points[4].valuation_status == "complete"
        assert raw.points[4].incompleteness_reasons == ()
        assert sampled.points == (raw.points[4],)

    def test_unstamped_unavailable_basis_fails_loudly(self) -> None:
        """A future basis path cannot silently invent a fallback reason."""
        with pytest.raises(ValueError, match="requires a stamped causal reason"):
            _value_point(
                _m(0),
                {("I1", "shard-I1"): _Pool(position_qty=1.0, entry_price=None)},
                {"I1": (("I1", "shard-I1"),)},
                {},
                {("I1", _m(0)): 100.0},
                ["I1"],
                [],
                {},
                {},
                {},
                {},
                {},
                {},
                0.0,
                0.0,
                0.0,
                None,
                (),
                {},
                {},
                {},
            )


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
        accruals = (TimelineAccrual("I1", _m(1) + timedelta(seconds=30), 0.1),)
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
        accruals = (TimelineAccrual("I1", _m(1) + timedelta(seconds=30), 0.1),)
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

    def test_base_fee_flip_allocates_fee_by_net_position_delta(self) -> None:
        """A base-fee flip allocates its fee across the net closed and opened legs.

        Given: A manual one-unit long and a system SELL whose gross 1.5 units
            plus 0.1 base fee produce a negative 1.6-unit position delta,
        When: the fill flips the pool into a 0.6-unit short,
        Then: fee attribution uses the net delta without overallocating the close.
        """
        executions = (
            _exec("I1", 1, 0, "buy", 1.0, 100.0),
            _exec(
                "I1",
                2,
                1,
                "sell",
                1.5,
                110.0,
                fee=0.1,
                position_delta=-1.6,
            ),
        )
        lineage = {
            "order-I1-1": TimelineExecutionLineage(
                source_surface="rest",
                plan_public_id=None,
                signal_public_id=None,
                origin="live",
                strategy_name=None,
            ),
            "order-I1-2": TimelineExecutionLineage(
                source_surface="strategy",
                plan_public_id=None,
                signal_public_id="signal-flip",
                origin="live",
                strategy_name="flip-strategy",
            ),
        }
        point = build_pnl_timeline(
            executions,
            (),
            {("I1", _m(1)): 111.0},
            _window(1, 1),
            lineage=lineage,
        ).points[0]
        buckets = {(bucket.origin, bucket.strategy_name): bucket for bucket in point.attribution}
        assert buckets[("manual", None)].fee_pnl == pytest.approx(-0.0625)
        assert buckets[("system", "flip-strategy")].fee_pnl == pytest.approx(-0.0375)
        assert point.fee_pnl == pytest.approx(-0.1)
        assert point.unrealized_pnl == pytest.approx(-0.6)
        _assert_exact_attribution_sums(point)

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
            pools=(_opening_pool("I1", FLAT_EPSILON / 2.0, 100.0),),
            t0=_m(0),
        )
        execution = replace(
            _exec("I1", 1, 0, "sell", 1.0, 110.0, fee=1.0),
            event_time=_m(0) + timedelta(seconds=1),
        )
        lineage = {
            execution.order_public_id: TimelineExecutionLineage(
                "strategy", None, "signal-1", "live", "momentum"
            )
        }
        point = build_pnl_timeline(
            (execution,),
            (),
            {("I1", _m(1)): 100.0},
            _window(1, 1),
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
        assert _reason_rows(point) == [
            ("attribution_reconciliation_failed", "untrusted", "global", None)
        ]

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
        assert _reason_rows(point) == [
            ("attribution_reconciliation_failed", "untrusted", "global", None)
        ]

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

    def test_generic_residue_reconciliation_accepts_instrument_keys(self) -> None:
        """The same bounded proof reconciles deterministic string-keyed maps."""
        keys = ["I1", "I2", "I3"]
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

    def test_unrepresentable_instrument_cancellation_withholds_aggregate(self) -> None:
        """Chronological totals are not rewritten to match a sorted instrument sum."""
        executions = (
            replace(
                _exec("I1", 1, 0, "buy", 0.0, 100.0, fee=-1e16),
                event_time=_T0 + timedelta(seconds=10),
            ),
            replace(
                _exec("I2", 2, 0, "buy", 0.0, 100.0, fee=-1.0),
                event_time=_T0 + timedelta(seconds=20),
            ),
            replace(
                _exec("I3", 3, 0, "buy", 0.0, 100.0, fee=1e16),
                event_time=_T0 + timedelta(seconds=30),
            ),
        )
        point = build_pnl_timeline(executions, (), {}, _window(1, 1)).points[0]
        assert point.realized_pnl is None
        assert point.fee_pnl is None
        assert point.accrual_pnl is None
        assert point.unrealized_pnl is None
        assert point.net_pnl is None
        assert _reason_rows(point) == [
            ("instrument_reconciliation_failed", "untrusted", "global", None)
        ]

    def test_adjacent_unrealized_instrument_residue_is_reconciled(self) -> None:
        """A one-ULP instrument correction makes complete unrealized sums exact."""
        executions = (
            _exec("I1", 1, 0, "buy", 1.0, 1000.0),
            _exec("I2", 2, 0, "buy", 1.0, 1000.0),
            _exec("I3", 3, 0, "buy", 1.0, 1.0),
        )
        marks: MarkMap = {
            ("I1", _m(0)): 447.30889868163956,
            ("I2", _m(0)): 352.6376610460791,
            ("I3", _m(0)): 620.8804889202258,
        }
        point = build_pnl_timeline(executions, (), marks, _window(0, 0)).points[0]
        instrument_values = [contribution.unrealized_pnl for contribution in point.per_instrument]
        assert point.valuation_status == "complete"
        assert None not in instrument_values
        assert instrument_values[-1] == math.nextafter(619.8804889202258, -math.inf)
        assert (
            sum(value for value in instrument_values if value is not None) == point.unrealized_pnl
        )
        _assert_exact_attribution_sums(point)

    def test_unrepresentable_unrealized_instrument_residue_withholds_point(self) -> None:
        """A larger sorted-instrument discrepancy is never transported."""
        executions = (
            _exec("I1", 1, 0, "buy", 1.0, 1.0),
            _exec("I2", 2, 0, "buy", 1.0, 1.0),
            _exec("I3", 3, 0, "buy", 1.0, 1e9),
        )
        marks: MarkMap = {
            ("I1", _m(0)): 100000000010.1,
            ("I2", _m(0)): 0.1,
            ("I3", _m(0)): 0.05,
        }
        point = build_pnl_timeline(executions, (), marks, _window(0, 0)).points[0]
        assert point.valuation_status == "incomplete"
        assert point.realized_pnl is None
        assert point.unrealized_pnl is None
        assert _reason_rows(point) == [
            ("instrument_reconciliation_failed", "untrusted", "global", None)
        ]

    def test_nonfinite_direct_residue_cannot_be_reconciled(self) -> None:
        """An overflowing final subtraction is rejected rather than transported."""
        keys: list[AttributionKey] = [("manual", None), ("system", None)]
        values = {keys[0]: -1e308, keys[1]: 0.0}
        assert _values_with_residue(values, keys, 1e308) is None

    @pytest.mark.parametrize("total", [1.0, math.inf, -math.inf, math.nan])
    def test_empty_reconciliation_keys_require_exact_zero(self, total: float) -> None:
        """An empty exposed grouping cannot reconcile a nonzero or nonfinite total."""
        values: dict[AttributionKey, float] = {}
        assert _values_with_residue(values, [], 0.0) == {}
        assert _values_with_residue(values, [], total) is None

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

    def test_open_point_uses_base_fee_adjusted_position_delta(self) -> None:
        """The pure pool values only the net base inventory received by a BUY.

        Given: A gross 20.04-unit BUY mapped to a 20.00-unit position delta,
        When: the timeline values it one price unit above entry,
        Then: unrealized P&L is based on 20.00 units rather than the gross fill.
        """
        execution = _exec(
            "I1",
            1,
            0,
            "buy",
            20.04,
            4.0,
            fee=0.04,
            position_delta=20.0,
        )
        point = build_pnl_timeline(
            (execution,),
            (),
            {("I1", _m(0)): 5.0},
            _window(0, 0),
        ).points[0]
        assert point.unrealized_pnl == pytest.approx(20.0)

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

    def test_incomplete_fee_stamps_its_instrument_cause(self) -> None:
        """Caller-stamped fee conversion failure withholds the affected point."""
        execution = replace(
            _exec("I1", 1, 0, "buy", 1.0, 100.0, fee=float("nan")),
            fee_incompleteness_reason="fx_conversion_unproven",
        )
        point = build_pnl_timeline(
            (execution,),
            (),
            {("I1", _m(0)): 100.0},
            _window(0, 0),
        ).points[0]
        assert _reason_rows(point) == [("fx_conversion_unproven", "untrusted", "instrument", "I1")]


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
        assert _reason_rows(point) == [("mark_unavailable", "mark_incomplete", "instrument", "I1")]

    def test_none_valued_mark_is_treated_as_absent(self) -> None:
        """An explicit ``None`` mark is the same as a missing one."""
        executions = (_exec("I1", 1, 0, "buy", 2.0, 100.0),)
        marks: MarkMap = {("I1", _m(0)): None}
        result = build_pnl_timeline(executions, (), marks, _window())
        assert result.points[0].valuation_status == "incomplete"


class TestOpeningDerivation:
    """Prove shard-aware exact-prefix replay and fail-closed t0 rebasing."""

    def test_empty_prefix_produces_valid_empty_opening(self) -> None:
        """An exact empty prefix needs no marks and seeds no pools."""
        result = derive_timeline_opening((), {}, _T0)
        assert result == TimelineOpeningDerivation(
            opening=TimelineOpening(pools=(), t0=_T0),
            per_pool=(),
            raw_opening_unrealized_value=0.0,
        )

    def test_same_shard_offsetting_prefix_discards_flat_pool(self) -> None:
        """A true same-shard round trip is flat and needs no t0 mark."""
        executions = (
            _exec("I1", 1, 0, "buy", 2.0, 100.0),
            _exec("I1", 2, 1, "sell", 1.0, 110.0),
            _exec("I1", 3, 2, "sell", 1.0, 120.0),
        )
        result = derive_timeline_opening(executions, {}, _T0)
        assert result.opening.pools == ()
        assert result.per_pool == ()
        assert result.raw_opening_unrealized_value == 0.0

    def test_long_add_and_reduce_retain_average_cost_pool(self) -> None:
        """Historical VWAP is audited while the build basis becomes the t0 mark."""
        executions = (
            _exec("I1", 1, 0, "buy", 2.0, 100.0),
            _exec("I1", 2, 1, "buy", 2.0, 120.0),
            _exec("I1", 3, 2, "sell", 1.0, 130.0),
        )
        result = derive_timeline_opening(executions, {"I1": 125.0}, _T0)
        assert result == TimelineOpeningDerivation(
            opening=TimelineOpening(
                pools=(_opening_pool("I1", 3.0, 125.0),),
                t0=_T0,
            ),
            per_pool=(
                OpeningPoolValuation(
                    instrument_public_id="I1",
                    shard_key="shard-I1",
                    exchange="kraken",
                    position_qty=3.0,
                    historical_entry_price=110.0,
                    t0_mark=125.0,
                    opening_unrealized_value=45.0,
                ),
            ),
            raw_opening_unrealized_value=45.0,
        )

    def test_short_add_and_reduce_retain_average_cost_pool(self) -> None:
        """Short additions retain historical VWAP only in the audit."""
        executions = (
            _exec("I1", 1, 0, "sell", 2.0, 100.0),
            _exec("I1", 2, 1, "sell", 2.0, 80.0),
            _exec("I1", 3, 2, "buy", 1.0, 70.0),
        )
        result = derive_timeline_opening(executions, {"I1": 75.0}, _T0)
        assert result.opening.pools == (_opening_pool("I1", -3.0, 75.0),)
        assert result.per_pool[0].historical_entry_price == 90.0
        assert result.per_pool[0].opening_unrealized_value == 45.0
        assert result.raw_opening_unrealized_value == 45.0

    def test_both_flip_directions_reset_basis_to_overshoot_fill(self) -> None:
        """Historical audits reflect flips while build seeds use t0 marks."""
        long_to_short = (
            _exec("I1", 1, 0, "buy", 2.0, 100.0),
            _exec("I1", 2, 1, "sell", 3.0, 120.0),
        )
        short_to_long = (
            _exec("I2", 1, 0, "sell", 2.0, 100.0),
            _exec("I2", 2, 1, "buy", 3.0, 80.0),
        )
        short_result = derive_timeline_opening(long_to_short, {"I1": 110.0}, _T0)
        long_result = derive_timeline_opening(short_to_long, {"I2": 90.0}, _T0)
        assert short_result.opening.pools == (_opening_pool("I1", -1.0, 110.0),)
        assert short_result.per_pool[0].historical_entry_price == 120.0
        assert short_result.raw_opening_unrealized_value == 10.0
        assert long_result.opening.pools == (_opening_pool("I2", 1.0, 90.0),)
        assert long_result.per_pool[0].historical_entry_price == 80.0
        assert long_result.raw_opening_unrealized_value == 10.0

    def test_opposing_shards_survive_zero_net_instrument(self) -> None:
        """Distinct long/short shards never collapse through instrument netting."""
        executions = (
            _exec("I1", 1, 0, "buy", 1.0, 100.0, shard_key="shard-long"),
            _exec("I1", 2, 0, "sell", 1.0, 120.0, shard_key="shard-short"),
        )
        result = derive_timeline_opening(executions, {"I1": 110.0}, _T0)
        assert result.opening.pools == (
            _opening_pool("I1", 1.0, 110.0, shard_key="shard-long"),
            _opening_pool("I1", -1.0, 110.0, shard_key="shard-short"),
        )
        assert [audit.opening_unrealized_value for audit in result.per_pool] == [
            10.0,
            10.0,
        ]
        assert result.raw_opening_unrealized_value == 20.0
        point = build_pnl_timeline(
            (),
            (),
            {("I1", _T0): 110.0},
            _window(0, 0),
            result.opening,
        ).points[0]
        assert point.realized_pnl == 0.0
        assert point.unrealized_pnl == 0.0
        assert point.net_pnl == 0.0
        assert point.per_instrument[0].unrealized_pnl == 0.0
        assert point.attribution[0].unrealized_pnl == 0.0
        _assert_exact_attribution_sums(point)

    def test_caller_resolved_base_fee_quantity_is_authoritative(self) -> None:
        """A resolved net quantity survives even when fee valuation is unavailable."""
        execution = replace(
            _exec(
                "I1",
                1,
                0,
                "buy",
                20.04,
                4.0,
                fee=float("nan"),
                position_delta=20.0,
            ),
            fee_incompleteness_reason="fx_conversion_unproven",
        )
        result = derive_timeline_opening((execution,), {"I1": 5.0}, _T0)
        assert result.opening.pools == (_opening_pool("I1", 20.0, 5.0),)
        assert result.per_pool[0].historical_entry_price == 4.0
        assert result.raw_opening_unrealized_value == 20.0

    def test_independent_prefix_interleaving_has_deterministic_output(self) -> None:
        """Cross-exchange interleaving cannot change stable valuation ordering."""
        first = _exec("I2", 1, 0, "buy", 0.1, 0.2, exchange="walutomat")
        second = _exec("I1", 1, 0, "sell", 0.2, 0.3, exchange="kraken")
        marks = {"I1": 0.1, "I2": 0.3}
        left = derive_timeline_opening((first, second), marks, _T0)
        right = derive_timeline_opening((second, first), marks, _T0)
        assert left == right
        assert [item.instrument_public_id for item in left.per_pool] == ["I1", "I2"]
        expected_total = 0.0
        for item in left.per_pool:
            expected_total += item.opening_unrealized_value
        assert left.raw_opening_unrealized_value == expected_total

    def test_rebased_t0_components_are_zero_under_cancellation(self) -> None:
        """Fsum retains ``[1e16, 1, -1e16]`` while public t0 stays rebased."""
        executions = (
            _exec("I1", 1, 0, "buy", 1.0, 1.0),
            _exec("I2", 2, 0, "buy", 1.0, 1.0),
            _exec("I3", 3, 0, "sell", 1.0, 1.0),
        )
        t0_marks = {"I1": 1e16, "I2": 2.0, "I3": 1e16}
        derivation = derive_timeline_opening(executions, t0_marks, _T0)
        marks: MarkMap = {
            ("I1", _T0): t0_marks["I1"],
            ("I2", _T0): t0_marks["I2"],
            ("I3", _T0): t0_marks["I3"],
        }
        point = build_pnl_timeline(
            (),
            (),
            marks,
            _window(0, 0),
            opening=derivation.opening,
        ).points[0]
        assert derivation.raw_opening_unrealized_value == 1.0
        assert point.realized_pnl == 0.0
        assert point.unrealized_pnl == 0.0
        assert point.net_pnl == 0.0
        assert all(item.unrealized_pnl == 0.0 for item in point.per_instrument)
        _assert_exact_attribution_sums(point)

    def test_zero_position_delta_is_a_harmless_no_op(self) -> None:
        """A finite zero inventory delta matches the existing builder contract."""
        execution = replace(
            _exec("I1", 1, 0, "buy", 0.0, float("nan"), position_delta=0.0),
            price_incompleteness_reason="fx_conversion_unproven",
            fee=float("nan"),
            fee_incompleteness_reason="fx_conversion_unproven",
        )
        result = derive_timeline_opening(
            (execution,),
            {},
            _T0,
        )
        assert result.opening.pools == ()
        assert result.raw_opening_unrealized_value == 0.0
        assert result.per_pool == ()

    def test_exact_epsilon_quantity_remains_non_flat(self) -> None:
        """The shared kernel's exact flat boundary survives and requires a mark."""
        execution = _exec(
            "I1",
            1,
            0,
            "buy",
            FLAT_EPSILON,
            100.0,
        )
        result = derive_timeline_opening((execution,), {"I1": 101.0}, _T0)
        assert result.opening.pools[0].position_qty == FLAT_EPSILON

    @pytest.mark.parametrize(
        "t0",
        [
            datetime(2026, 7, 20, 10, 0),
            datetime(
                2026,
                7,
                20,
                12,
                0,
                tzinfo=timezone(timedelta(hours=2)),
            ),
            datetime(2026, 7, 20, 10, 0, 1, tzinfo=UTC),
            datetime(2026, 7, 20, 10, 0, 0, 1, tzinfo=UTC),
        ],
    )
    def test_rejects_non_utc_or_non_minute_anchor_time(self, t0: datetime) -> None:
        """An opening must identify one exact UTC grid minute."""
        with pytest.raises(ValueError, match="aligned to a UTC minute"):
            derive_timeline_opening((), {}, t0)

    def test_rejects_invalid_identities_and_scope_order(self) -> None:
        """Blank identities and non-increasing scope evidence refuse replay."""
        valid = _exec("I1", 1, 0, "buy", 1.0, 100.0)
        with pytest.raises(ValueError, match="instrument identity"):
            derive_timeline_opening((replace(valid, instrument_public_id=""),), {}, _T0)
        with pytest.raises(ValueError, match="shard identity"):
            derive_timeline_opening((replace(valid, shard_key=""),), {}, _T0)
        with pytest.raises(ValueError, match="exchange identity"):
            derive_timeline_opening((replace(valid, exchange=""),), {}, _T0)
        with pytest.raises(ValueError, match="increasing positive"):
            derive_timeline_opening((replace(valid, scope_sequence=0),), {}, _T0)
        with pytest.raises(ValueError, match="increasing positive"):
            derive_timeline_opening((valid, replace(valid, order_public_id="order-2")), {}, _T0)

    def test_rejects_instrument_or_shard_identity_spanning_scopes(self) -> None:
        """One durable pool identity cannot span instruments or venues."""
        executions = (
            _exec("I1", 1, 0, "buy", 1.0, 100.0, exchange="kraken"),
            _exec("I1", 1, 0, "buy", 1.0, 100.0, exchange="walutomat"),
        )
        with pytest.raises(ValueError, match="multiple exchanges"):
            derive_timeline_opening(executions, {}, _T0)
        crossed_shard = (
            _exec("I1", 1, 0, "buy", 0.0, 1.0, shard_key="shared"),
            _exec("I2", 2, 0, "buy", 0.0, 1.0, shard_key="shared"),
        )
        with pytest.raises(ValueError, match="shard cannot span"):
            derive_timeline_opening(crossed_shard, {}, _T0)

    def test_rejects_invalid_side_size_and_position_delta(self) -> None:
        """Malformed fill dimensions never enter average-cost arithmetic."""
        valid = _exec("I1", 1, 0, "buy", 1.0, 100.0)
        invalid_sides = (
            replace(valid, side="hold"),
            replace(valid, side="buy", position_delta=-1.0),
            replace(valid, side="sell", position_delta=1.0),
        )
        for execution in invalid_sides:
            with pytest.raises(ValueError, match="side"):
                derive_timeline_opening((execution,), {}, _T0)
        invalid_sizes = (
            replace(valid, size=-1.0),
            replace(valid, size=float("nan")),
        )
        for execution in invalid_sizes:
            with pytest.raises(ValueError, match="size"):
                derive_timeline_opening((execution,), {}, _T0)
        invalid_deltas = (replace(valid, position_delta=float("inf")),)
        for execution in invalid_deltas:
            with pytest.raises(ValueError, match="position delta"):
                derive_timeline_opening((execution,), {}, _T0)

    def test_rejects_numeric_and_causal_price_failures(self) -> None:
        """Non-positive, non-finite, or causally untrusted prices refuse replay."""
        valid = _exec("I1", 1, 0, "buy", 1.0, 100.0)
        invalid_prices = (0.0, -1.0, float("nan"), float("inf"))
        for price in invalid_prices:
            with pytest.raises(ValueError, match="price must be positive"):
                derive_timeline_opening((replace(valid, price=price),), {}, _T0)
        stamped = replace(valid, price_incompleteness_reason="fx_conversion_unproven")
        with pytest.raises(ValueError, match="provenance"):
            derive_timeline_opening((stamped,), {}, _T0)
        untrusted_reasons: dict[str, tuple[PnlIncompletenessReason, ...]] = {
            "I1": ("execution_price_provenance_unproven",)
        }
        with pytest.raises(ValueError, match="provenance"):
            derive_timeline_opening(
                (valid,),
                {},
                _T0,
                untrusted_reasons,
            )

    def test_rejects_non_finite_pool_arithmetic(self) -> None:
        """Quantity overflow and VWAP overflow cannot become an opening seed."""
        quantity_overflow = (
            _exec("I1", 1, 0, "buy", 1e308, 1.0),
            _exec("I1", 2, 0, "buy", 1e308, 1.0),
        )
        with pytest.raises(ValueError, match="quantity arithmetic"):
            derive_timeline_opening(quantity_overflow, {}, _T0)
        basis_overflow = (
            _exec("I1", 1, 0, "buy", 1e308, 2.0),
            _exec("I1", 2, 0, "buy", 1.0, 1.0),
        )
        with pytest.raises(ValueError, match="cost basis arithmetic"):
            derive_timeline_opening(basis_overflow, {}, _T0)

    @pytest.mark.parametrize(
        "mark",
        [None, float("nan"), float("inf"), 0.0, -10.0],
    )
    def test_rejects_missing_or_invalid_surviving_mark(self, mark: float | None) -> None:
        """Every surviving pool needs a positive finite mark at exactly t0."""
        execution = _exec("I1", 1, 0, "buy", 1.0, 100.0)
        with pytest.raises(ValueError, match="positive finite t0 mark"):
            derive_timeline_opening((execution,), {"I1": mark}, _T0)

    def test_rejects_instrument_and_total_unrealized_overflow(self) -> None:
        """Finite inputs must still produce finite per-pool and aggregate values."""
        one = _exec("I1", 1, 0, "buy", 1e308, 1.0)
        with pytest.raises(ValueError, match="pool unrealized"):
            derive_timeline_opening((one,), {"I1": 3.0}, _T0)
        two = _exec("I2", 1, 0, "buy", 1e308, 1.0, exchange="walutomat")
        with pytest.raises(ValueError, match="total unrealized"):
            derive_timeline_opening((one, two), {"I1": 2.0, "I2": 2.0}, _T0)

    def test_rejects_nonfinite_fsum_result(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A non-finite aggregate result is refused even after finite pool audits."""

        def nonfinite_fsum(_values: Iterable[float]) -> float:
            """Stand in for a corrupt aggregate result."""
            return math.inf

        monkeypatch.setattr(math, "fsum", nonfinite_fsum)
        with pytest.raises(ValueError, match="total unrealized"):
            derive_timeline_opening((), {}, _T0)


class TestOpeningAnchor:
    """Cover rebased close mathematics and opening-pool validation."""

    @staticmethod
    def _long_opening(quantity: float = 2.0) -> TimelineOpening:
        """Return a long seed rebased from historical 100 to t0 mark 110."""
        return derive_timeline_opening(
            (_exec("I1", 1, -1, "buy", quantity, 100.0),),
            {"I1": 110.0},
            _T0,
        ).opening

    def test_opening_plots_change_from_rebased_entry(self) -> None:
        """The public unrealized component is directly activation-relative."""
        opening = TimelineOpening(
            pools=(_opening_pool("I1", 1.0, 105.0),),
            t0=_m(-10),
        )
        marks: MarkMap = {("I1", _m(0)): 110.0}
        result = build_pnl_timeline((), (), marks, _window(0, 0), opening=opening)
        point = result.points[0]
        assert point.unrealized_pnl == pytest.approx(5.0)
        assert point.net_pnl == pytest.approx(5.0)

    def test_t0_drops_equal_accrual_and_precedes_post_watermark_replay(self) -> None:
        """The exact activation point exposes zero before any later-ledger flow."""
        execution = _exec("I1", 1, 0, "buy", 1.0, 100.0, fee=2.0)
        opening = TimelineOpening(
            pools=(_opening_pool("I1", 1.0, 100.0),),
            t0=_T0,
        )
        points = build_pnl_timeline(
            (execution,),
            (TimelineAccrual("I1", _T0, 3.0),),
            {("I1", _m(1)): 100.0},
            _window(0, 1),
            opening,
        ).points
        assert (
            points[0].realized_pnl,
            points[0].fee_pnl,
            points[0].accrual_pnl,
            points[0].unrealized_pnl,
            points[0].net_pnl,
        ) == (0.0, 0.0, 0.0, 0.0, 0.0)
        _assert_exact_attribution_sums(points[0])
        assert points[1].fee_pnl == -2.0
        assert points[1].accrual_pnl == 0.0
        assert points[1].net_pnl == -2.0
        _assert_exact_attribution_sums(points[1])

    def test_partial_close_releases_only_since_activation_pnl(self) -> None:
        """A partial close splits t0-relative P&L into realized and unrealized."""
        closing = _exec("I1", 2, 1, "sell", 1.0, 120.0)
        points = build_pnl_timeline(
            (closing,),
            (),
            {("I1", _m(0)): 110.0, ("I1", _m(1)): 120.0},
            _window(0, 1),
            self._long_opening(),
        ).points
        assert (
            points[0].realized_pnl,
            points[0].unrealized_pnl,
            points[0].net_pnl,
        ) == (0.0, 0.0, 0.0)
        assert (
            points[1].realized_pnl,
            points[1].unrealized_pnl,
            points[1].net_pnl,
        ) == (10.0, 10.0, 20.0)
        _assert_exact_attribution_sums(points[1])

    def test_full_close_moves_all_since_activation_pnl_to_realized(self) -> None:
        """A full close has no hidden baseline after the shard becomes flat."""
        closing = _exec("I1", 2, 1, "sell", 2.0, 120.0)
        point = build_pnl_timeline(
            (closing,),
            (),
            {},
            _window(1, 1),
            self._long_opening(),
        ).points[0]
        assert point.realized_pnl == 20.0
        assert point.unrealized_pnl == 0.0
        assert point.net_pnl == 20.0
        _assert_exact_attribution_sums(point)

    def test_same_side_add_then_reduce_uses_rebased_vwap(self) -> None:
        """New inventory VWAPs with t0-valued legacy inventory before reduction."""
        executions = (
            _exec("I1", 2, 1, "buy", 1.0, 120.0),
            _exec("I1", 3, 2, "sell", 1.0, 130.0),
        )
        point = build_pnl_timeline(
            executions,
            (),
            {("I1", _m(2)): 130.0},
            _window(2, 2),
            self._long_opening(1.0),
        ).points[0]
        assert point.realized_pnl == 15.0
        assert point.unrealized_pnl == 15.0
        assert point.net_pnl == 30.0
        _assert_exact_attribution_sums(point)

    def test_flip_closes_rebased_legacy_then_opens_overshoot(self) -> None:
        """A flip realizes the legacy t0 move and marks only the new side."""
        flip = _exec("I1", 2, 1, "sell", 1.5, 120.0)
        point = build_pnl_timeline(
            (flip,),
            (),
            {("I1", _m(1)): 115.0},
            _window(1, 1),
            self._long_opening(1.0),
        ).points[0]
        assert point.realized_pnl == 10.0
        assert point.unrealized_pnl == 2.5
        assert point.net_pnl == 12.5
        _assert_exact_attribution_sums(point)

    def test_short_to_long_flip_is_symmetric_after_rebase(self) -> None:
        """A short legacy pool closes at t0 basis before its long overshoot opens."""
        opening = derive_timeline_opening(
            (_exec("I1", 1, -1, "sell", 1.0, 100.0),),
            {"I1": 90.0},
            _T0,
        ).opening
        flip = _exec("I1", 2, 1, "buy", 1.5, 80.0)
        point = build_pnl_timeline(
            (flip,),
            (),
            {("I1", _m(1)): 85.0},
            _window(1, 1),
            opening,
        ).points[0]
        assert point.realized_pnl == 10.0
        assert point.unrealized_pnl == 2.5
        assert point.net_pnl == 12.5
        _assert_exact_attribution_sums(point)

    def test_opposing_nonflat_shards_require_instrument_mark(self) -> None:
        """Gross exposure cannot look flat because shard quantities net to zero."""
        opening = TimelineOpening(
            pools=(
                _opening_pool("I1", 1.0, 110.0, shard_key="long"),
                _opening_pool("I1", -1.0, 110.0, shard_key="short"),
            ),
            t0=_T0,
        )
        point = build_pnl_timeline((), (), {}, _window(1, 1), opening).points[0]
        assert _reason_rows(point) == [("mark_unavailable", "mark_incomplete", "instrument", "I1")]

    def test_seeded_position_without_entry_is_incomplete(self) -> None:
        """A held seed whose entry is unknown cannot be valued."""
        opening = TimelineOpening(
            pools=(_opening_pool("I1", 1.0, None),),
            t0=_m(-10),
        )
        marks: MarkMap = {("I1", _m(0)): 110.0}
        result = build_pnl_timeline((), (), marks, _window(0, 0), opening=opening)
        assert result.points[0].valuation_status == "incomplete"

    @pytest.mark.parametrize(
        "entry_price",
        [0.0, -1.0, math.inf, -math.inf, math.nan],
    )
    def test_nonpositive_or_nonfinite_seeded_entry_stamps_cost_basis_cause(
        self,
        entry_price: float,
    ) -> None:
        """A held seed accepts only a positive finite rebased entry."""
        opening = TimelineOpening(
            pools=(_opening_pool("I1", 1.0, entry_price),),
            t0=_m(0),
        )
        point = build_pnl_timeline(
            (),
            (),
            {("I1", _m(0)): 100.0},
            _window(0, 0),
            opening,
        ).points[0]
        assert _reason_rows(point) == [
            ("cost_basis_unavailable", "mark_incomplete", "instrument", "I1")
        ]

    def test_same_side_add_cannot_cleanse_invalid_seeded_basis(self) -> None:
        """A positive computed VWAP cannot erase an invalid opening basis cause."""
        opening = TimelineOpening(
            pools=(_opening_pool("I1", 1.0, 0.0),),
            t0=_m(0),
        )
        result = build_pnl_timeline(
            (_exec("I1", 1, 1, "buy", 1.0, 100.0),),
            (),
            {("I1", _m(1)): 100.0},
            _window(0, 1),
            opening,
        )
        assert all(point.valuation_status == "incomplete" for point in result.points)
        assert _reason_rows(result.points[1]) == [
            ("cost_basis_unavailable", "mark_incomplete", "instrument", "I1")
        ]

    def test_opening_rejects_duplicate_unstable_and_cross_scope_pools(self) -> None:
        """One stable tuple cannot duplicate or contradict durable pool identity."""
        pool = _opening_pool("I1", 1.0, 100.0, shard_key="a")
        with pytest.raises(ValueError, match="stably ordered"):
            TimelineOpening(
                pools=(
                    _opening_pool("I1", 1.0, 100.0, shard_key="b"),
                    pool,
                ),
                t0=_T0,
            )
        with pytest.raises(ValueError, match="unique instrument and shard"):
            TimelineOpening(pools=(pool, pool), t0=_T0)
        with pytest.raises(ValueError, match="shard cannot span"):
            TimelineOpening(
                pools=(
                    pool,
                    _opening_pool("I2", 1.0, 100.0, shard_key="a"),
                ),
                t0=_T0,
            )
        with pytest.raises(ValueError, match="instrument cannot span"):
            TimelineOpening(
                pools=(
                    pool,
                    _opening_pool(
                        "I1",
                        1.0,
                        100.0,
                        shard_key="b",
                        exchange="walutomat",
                    ),
                ),
                t0=_T0,
            )

    @pytest.mark.parametrize(
        "pool",
        [
            OpeningPool("", "shard", "kraken", 1.0, 100.0),
            OpeningPool("I1", "", "kraken", 1.0, 100.0),
            OpeningPool("I1", "shard", "", 1.0, 100.0),
        ],
    )
    def test_opening_rejects_empty_pool_identity(self, pool: OpeningPool) -> None:
        """Every serialized seed carries all three durable scope identities."""
        with pytest.raises(ValueError, match="identities must be non-empty"):
            TimelineOpening(pools=(pool,), t0=_T0)

    @pytest.mark.parametrize(
        "t0",
        [
            _T0.astimezone(timezone(timedelta(hours=1))),
            _T0.replace(second=1),
            _T0.replace(microsecond=1),
        ],
    )
    def test_opening_requires_an_exact_utc_grid_minute(self, t0: datetime) -> None:
        """A manually constructed seed cannot bypass activation-time validation."""
        with pytest.raises(ValueError, match="aligned to a UTC minute"):
            TimelineOpening(pools=(), t0=t0)

    @pytest.mark.parametrize(
        "execution",
        [
            replace(_exec("I1", 1, 0, "buy", 1.0, 100.0), instrument_public_id=""),
            replace(_exec("I1", 1, 0, "buy", 1.0, 100.0), shard_key=""),
            replace(_exec("I1", 1, 0, "buy", 1.0, 100.0), exchange=""),
        ],
    )
    def test_builder_rejects_empty_execution_pool_identity(
        self,
        execution: TimelineExecution,
    ) -> None:
        """Post-anchor replay requires the same durable pool identities as its seed."""
        with pytest.raises(ValueError, match="pool identities must be non-empty"):
            build_pnl_timeline((execution,), (), {}, _window(0, 0))

    def test_builder_rejects_execution_pool_scope_collisions(self) -> None:
        """A shard or instrument cannot change its durable scope during replay."""
        crossed_shard = (
            _exec("I1", 1, 0, "buy", 1.0, 100.0, shard_key="shared"),
            _exec("I2", 2, 0, "buy", 1.0, 100.0, shard_key="shared"),
        )
        with pytest.raises(ValueError, match="shard cannot span"):
            build_pnl_timeline(crossed_shard, (), {}, _window(0, 0))
        crossed_instrument = (
            _exec("I1", 1, 0, "buy", 1.0, 100.0, shard_key="kraken"),
            _exec(
                "I1",
                1,
                0,
                "buy",
                1.0,
                100.0,
                shard_key="walutomat",
                exchange="walutomat",
            ),
        )
        with pytest.raises(ValueError, match="instrument cannot span"):
            build_pnl_timeline(crossed_instrument, (), {}, _window(0, 0))


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

    def test_instrument_accrual_combines_current_weights_from_all_shards(self) -> None:
        """A shardless accrual allocates over the instrument's gross pool weights."""
        executions = (
            replace(
                _exec("I1", 1, 0, "buy", 1.0, 100.0, shard_key="manual-shard"),
                event_time=_m(0) + timedelta(seconds=10),
            ),
            replace(
                _exec("I1", 2, 0, "buy", 3.0, 100.0, shard_key="system-shard"),
                event_time=_m(0) + timedelta(seconds=20),
            ),
        )
        lineage = {
            executions[0].order_public_id: TimelineExecutionLineage(
                "rest",
                None,
                None,
                "live",
                None,
            ),
            executions[1].order_public_id: TimelineExecutionLineage(
                "strategy",
                None,
                "signal-1",
                "live",
                "momentum",
            ),
        }
        point = build_pnl_timeline(
            executions,
            (TimelineAccrual("I1", _m(0) + timedelta(seconds=30), 4.0),),
            {("I1", _m(1)): 110.0},
            _window(1, 1),
            lineage=lineage,
        ).points[0]
        buckets = {(bucket.origin, bucket.strategy_name): bucket for bucket in point.attribution}
        assert buckets[("manual", None)].accrual_pnl == -1.0
        assert buckets[("system", "momentum")].accrual_pnl == -3.0
        assert buckets[("manual", None)].unrealized_pnl == 10.0
        assert buckets[("system", "momentum")].unrealized_pnl == 30.0
        assert point.unrealized_pnl == 40.0
        _assert_exact_attribution_sums(point)

    def test_exact_time_merge_uses_only_strictly_earlier_fill_weights(self) -> None:
        """An intra-minute accrual precedes a later fill instead of seeing minute-end state."""
        manual = replace(
            _exec("I1", 1, 0, "buy", 1.0, 100.0, shard_key="manual-shard"),
            event_time=_T0 + timedelta(seconds=10),
        )
        system = replace(
            _exec("I1", 2, 0, "buy", 3.0, 100.0, shard_key="system-shard"),
            event_time=_T0 + timedelta(seconds=40),
        )
        lineage = {
            manual.order_public_id: TimelineExecutionLineage("rest", None, None, "live", None),
            system.order_public_id: TimelineExecutionLineage(
                "strategy",
                None,
                "signal-1",
                "live",
                "momentum",
            ),
        }
        point = build_pnl_timeline(
            (manual, system),
            (TimelineAccrual("I1", _T0 + timedelta(seconds=30), 4.0),),
            {("I1", _m(1)): 100.0},
            _window(1, 1),
            lineage=lineage,
        ).points[0]
        buckets = {(bucket.origin, bucket.strategy_name): bucket for bucket in point.attribution}
        assert buckets[("manual", None)].accrual_pnl == -4.0
        assert buckets[("system", "momentum")].accrual_pnl == 0.0
        assert point.accrual_pnl == -4.0
        _assert_exact_attribution_sums(point)

    def test_equal_time_position_change_makes_accrual_unattributed(self) -> None:
        """Same-instant ownership ambiguity preserves value but refuses attribution."""
        manual = replace(
            _exec("I1", 1, 0, "buy", 1.0, 100.0, shard_key="manual-shard"),
            event_time=_T0 + timedelta(seconds=10),
        )
        system = replace(
            _exec("I1", 2, 0, "buy", 3.0, 100.0, shard_key="system-shard"),
            event_time=_T0 + timedelta(seconds=30),
        )
        lineage = {
            manual.order_public_id: TimelineExecutionLineage("rest", None, None, "live", None),
            system.order_public_id: TimelineExecutionLineage(
                "strategy",
                None,
                "signal-1",
                "live",
                "momentum",
            ),
        }
        point = build_pnl_timeline(
            (manual, system),
            (TimelineAccrual("I1", _T0 + timedelta(seconds=30), 4.0),),
            {("I1", _m(1)): 100.0},
            _window(1, 1),
            lineage=lineage,
        ).points[0]
        buckets = {(bucket.origin, bucket.strategy_name): bucket for bucket in point.attribution}
        assert point.accrual_pnl == -4.0
        assert buckets[("manual", None)].accrual_pnl == 0.0
        assert buckets[("system", "momentum")].accrual_pnl == 0.0
        assert buckets[("unattributed", None)].accrual_pnl == -4.0
        _assert_exact_attribution_sums(point)

    def test_clamped_position_change_makes_effective_time_accrual_unattributed(
        self,
    ) -> None:
        """A regressed fill participates in its clamped exact-time ambiguity batch."""
        manual = replace(
            _exec("I1", 1, 0, "buy", 1.0, 100.0),
            event_time=_T0 + timedelta(seconds=10),
        )
        no_op = replace(
            _exec("I1", 2, 0, "buy", 0.0, 100.0, position_delta=0.0),
            event_time=_T0 + timedelta(seconds=30),
        )
        system = replace(
            _exec("I1", 3, 0, "buy", 3.0, 100.0),
            event_time=_T0 + timedelta(seconds=20),
        )
        lineage = {
            manual.order_public_id: TimelineExecutionLineage(
                "rest",
                None,
                None,
                "live",
                None,
            ),
            system.order_public_id: TimelineExecutionLineage(
                "strategy",
                None,
                "signal-1",
                "live",
                "momentum",
            ),
        }
        point = build_pnl_timeline(
            (manual, no_op, system),
            (TimelineAccrual("I1", _T0 + timedelta(seconds=30), 4.0),),
            {("I1", _m(1)): 100.0},
            _window(1, 1),
            lineage=lineage,
        ).points[0]
        buckets = {(bucket.origin, bucket.strategy_name): bucket for bucket in point.attribution}
        assert point.valuation_status == "complete"
        assert point.accrual_pnl == -4.0
        assert buckets[("manual", None)].accrual_pnl == 0.0
        assert buckets[("system", "momentum")].accrual_pnl == 0.0
        assert buckets[("unattributed", None)].accrual_pnl == -4.0
        _assert_exact_attribution_sums(point)

    def test_equal_time_zero_delta_does_not_create_ownership_ambiguity(self) -> None:
        """A no-op fill at the accrual instant leaves strictly earlier weights usable."""
        manual = replace(
            _exec("I1", 1, 0, "buy", 1.0, 100.0, shard_key="manual-shard"),
            event_time=_T0 + timedelta(seconds=10),
        )
        no_op = replace(
            _exec(
                "I1",
                2,
                0,
                "buy",
                0.0,
                100.0,
                position_delta=0.0,
                shard_key="system-shard",
            ),
            event_time=_T0 + timedelta(seconds=30),
        )
        lineage = {
            manual.order_public_id: TimelineExecutionLineage("rest", None, None, "live", None),
            no_op.order_public_id: TimelineExecutionLineage(
                "strategy",
                None,
                "signal-1",
                "live",
                "momentum",
            ),
        }
        point = build_pnl_timeline(
            (manual, no_op),
            (TimelineAccrual("I1", _T0 + timedelta(seconds=30), 4.0),),
            {("I1", _m(1)): 100.0},
            _window(1, 1),
            lineage=lineage,
        ).points[0]
        buckets = {(bucket.origin, bucket.strategy_name): bucket for bucket in point.attribution}
        assert buckets[("manual", None)].accrual_pnl == -4.0
        assert buckets[("system", "momentum")].accrual_pnl == 0.0
        assert ("unattributed", None) not in buckets
        _assert_exact_attribution_sums(point)

    def test_equal_time_fills_retain_exchange_scope_sequence_order(self) -> None:
        """Equal-time pool replay follows scope sequence even when input is reversed."""
        event_time = _T0 + timedelta(seconds=30)
        executions = (
            replace(_exec("I1", 3, 0, "sell", 1.0, 150.0), event_time=event_time),
            replace(_exec("I1", 2, 0, "buy", 1.0, 200.0), event_time=event_time),
            replace(_exec("I1", 1, 0, "buy", 1.0, 100.0), event_time=event_time),
        )
        point = build_pnl_timeline(
            executions,
            (),
            {("I1", _m(1)): 160.0},
            _window(1, 1),
        ).points[0]
        assert point.realized_pnl == 0.0
        assert point.unrealized_pnl == 10.0
        assert point.net_pnl == 10.0
        _assert_exact_attribution_sums(point)

    def test_incomplete_accrual_stamps_its_instrument_cause(self) -> None:
        """Caller-stamped accrual conversion failure withholds the affected point."""
        accrual = TimelineAccrual(
            "I1",
            _m(0),
            float("nan"),
            incompleteness_reason="fx_conversion_unproven",
        )
        point = build_pnl_timeline((), (accrual,), {}, _window(0, 0)).points[0]
        assert _reason_rows(point) == [("fx_conversion_unproven", "untrusted", "instrument", "I1")]


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
        assert _reason_rows(result.points[1]) == [
            ("scope_order_regression", "untrusted", "global", "I1")
        ]

    def test_unshadowed_minutes_stay_complete(self) -> None:
        """Minutes outside the shadow window value normally after the clamp."""
        marks: MarkMap = {("I1", _m(3)): 100.0, ("I1", _m(4)): 100.0, ("I1", _m(5)): 100.0}
        result = build_pnl_timeline(self._executions(), (), marks, _window())
        assert result.points[0].valuation_status == "complete"
        assert result.points[3].valuation_status == "complete"
        assert result.points[4].valuation_status == "complete"

    def test_event_time_regression_clock_is_independent_per_shard(self) -> None:
        """A later-listed second shard may have an earlier event time without taint."""
        executions = (
            _exec("I1", 1, 2, "buy", 1.0, 100.0, shard_key="shard-a"),
            _exec("I1", 2, 0, "sell", 1.0, 100.0, shard_key="shard-b"),
        )
        point = build_pnl_timeline(
            executions,
            (),
            {("I1", _m(0)): 100.0},
            _window(0, 0),
        ).points[0]
        assert point.valuation_status == "complete"
        assert point.incompleteness_reasons == ()
        assert point.unrealized_pnl == 0.0

    def test_future_only_regressed_pool_stays_outside_valuation_index(self) -> None:
        """Regression proof retains future rows without unbudgeted valuation pools."""
        executions = (
            _exec("I1", 1, 10, "buy", 1.0, 100.0, shard_key="future-shard"),
            _exec("I1", 2, 0, "sell", 1.0, 100.0, shard_key="future-shard"),
        )

        prepared, shadows = _prepare_executions(executions)
        pool_index = _valuation_pool_index({}, prepared, _m(0))

        assert pool_index == {}
        assert [(shadow.start, shadow.end) for shadow in shadows] == [(_m(0), _m(10))]

    def test_subminute_regression_shadow_rounds_to_grid_boundaries(self) -> None:
        """Half-open shadow bounds activate only intersecting valuation minutes."""
        executions = (
            replace(
                _exec("I1", 1, 2, "buy", 1.0, 100.0),
                event_time=_m(2) + timedelta(seconds=30),
            ),
            replace(
                _exec("I1", 2, 0, "sell", 1.0, 100.0),
                event_time=_m(0) + timedelta(seconds=30),
            ),
        )
        result = build_pnl_timeline(
            executions,
            (),
            {("I1", _m(3)): 100.0},
            _window(0, 3),
        )

        assert [point.valuation_status for point in result.points] == [
            "complete",
            "incomplete",
            "incomplete",
            "complete",
        ]

    def test_overlapping_shadows_emit_the_smallest_active_trigger(self) -> None:
        """Boundary changes select one real trigger and restore its predecessor."""
        executions = (
            _exec("I2", 1, 3, "buy", 1.0, 100.0),
            _exec("I2", 2, 0, "sell", 1.0, 100.0),
            _exec("I1", 3, 2, "buy", 1.0, 100.0),
            _exec("I1", 4, 1, "sell", 1.0, 100.0),
        )

        result = build_pnl_timeline(executions, (), {}, _window(0, 3))

        assert [_reason_rows(point) for point in result.points] == [
            [("scope_order_regression", "untrusted", "global", "I2")],
            [("scope_order_regression", "untrusted", "global", "I1")],
            [("scope_order_regression", "untrusted", "global", "I2")],
            [],
        ]

    def test_same_trigger_shadow_intervals_merge_without_extra_changes(self) -> None:
        """Reference counts retain one trigger across nested interval boundaries."""
        reason = PnlIncompletenessReasonEntry(
            reason="scope_order_regression",
            withholding_tier="untrusted",
            withholding_scope="global",
            trigger_instrument_public_id="I1",
        )
        shadows = (
            _RegressionShadow(_m(0), _m(3), reason),
            _RegressionShadow(_m(1), _m(2), reason),
        )

        assert _regression_shadow_trigger_changes(shadows, _m(0), 4) == {
            0: "I1",
            3: None,
        }

    def test_shadow_without_trigger_is_rejected(self) -> None:
        """The sparse schedule refuses an impossible anonymous regression."""
        shadow = _RegressionShadow(
            _m(0),
            _m(1),
            PnlIncompletenessReasonEntry(
                reason="scope_order_regression",
                withholding_tier="untrusted",
                withholding_scope="global",
                trigger_instrument_public_id=None,
            ),
        )

        with pytest.raises(ValueError, match="requires a triggering instrument"):
            _regression_shadow_deltas((shadow,), _m(0), 2)

    def test_thousand_distinct_future_pools_use_bounded_shadow_changes(self) -> None:
        """A 24-hour window emits one trigger without minute-instrument fan-out."""
        executions = tuple(
            execution
            for index in range(1_000)
            for execution in (
                _exec(
                    f"I{index:04d}",
                    2 * index + 1,
                    1_440,
                    "buy",
                    1.0,
                    100.0,
                    shard_key=f"future-shard-{index}",
                ),
                _exec(
                    f"I{index:04d}",
                    2 * index + 2,
                    0,
                    "sell",
                    1.0,
                    100.0,
                    shard_key=f"future-shard-{index}",
                ),
            )
        )

        prepared, shadows = _prepare_executions(executions)
        pool_index = _valuation_pool_index({}, prepared, _m(1_439))
        deltas = _regression_shadow_deltas(shadows, _m(0), 1_440)
        trigger_changes = _regression_shadow_trigger_changes(
            shadows,
            _m(0),
            1_440,
        )
        result = build_pnl_timeline(
            executions,
            (),
            {},
            _window(0, 1_439),
        )

        assert len(shadows) == 1_000
        assert pool_index == {}
        assert set(deltas) == {0, 1_440}
        assert sum(len(bucket) for bucket in deltas.values()) == 2_000
        assert trigger_changes == {0: "I0000", 1_440: None}
        assert len(result.points) == 1_440
        assert {tuple(_reason_rows(point)) for point in result.points} == {
            (("scope_order_regression", "untrusted", "global", "I0000"),)
        }
        assert all(point.per_instrument == () for point in result.points)


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
    """Cover global and instrument-scoped untrusted cumulative paths."""

    def test_instrument_untrust_preserves_independent_contribution(self) -> None:
        """An untrusted instrument withholds the total but not a proven peer."""
        executions = (
            _exec("I1", 1, 0, "buy", 2.0, 100.0, fee=1.0),
            _exec("I2", 2, 0, "buy", 1.0, 50.0),
        )
        accruals = (TimelineAccrual(instrument_public_id="I1", accrued_at=_m(0), amount_usd=3.0),)
        marks: MarkMap = {("I1", _m(0)): 110.0, ("I2", _m(0)): 55.0}
        point = build_pnl_timeline(
            executions,
            accruals,
            marks,
            _window(0, 0),
            untrusted_price_reasons_by_instrument={"I2": {"execution_price_provenance_unproven"}},
        ).points[0]
        contributions = {
            contribution.instrument_public_id: contribution for contribution in point.per_instrument
        }
        proven = contributions["I1"]
        assert proven.realized_pnl == 0.0
        assert proven.fee_pnl == pytest.approx(-1.0)
        assert proven.accrual_pnl == pytest.approx(-3.0)
        assert proven.unrealized_pnl == pytest.approx(20.0)
        withheld = contributions["I2"]
        assert withheld.realized_pnl is None
        assert withheld.fee_pnl is None
        assert withheld.accrual_pnl is None
        assert withheld.unrealized_pnl is None
        assert point.realized_pnl is None
        assert point.fee_pnl is None
        assert point.accrual_pnl is None
        assert point.unrealized_pnl is None
        assert point.net_pnl is None
        assert all(
            bucket.realized_pnl is None
            and bucket.fee_pnl is None
            and bucket.accrual_pnl is None
            and bucket.unrealized_pnl is None
            for bucket in point.attribution
        )
        assert _reason_rows(point) == [
            (
                "execution_price_provenance_unproven",
                "untrusted",
                "instrument",
                "I2",
            )
        ]

    def test_nonfinite_instrument_cumulative_preserves_finite_peer(self) -> None:
        """An attributable NaN preserves its peer while aggregate overflow stays global."""
        executions = (
            _exec("I1", 1, 0, "buy", 1.0, 100.0, fee=2.0),
            _exec("I2", 2, 0, "buy", 1.0, 50.0, fee=float("nan")),
        )
        marks: MarkMap = {("I1", _m(0)): 105.0, ("I2", _m(0)): 55.0}
        point = build_pnl_timeline(executions, (), marks, _window(0, 0)).points[0]
        contributions = {
            contribution.instrument_public_id: contribution for contribution in point.per_instrument
        }
        assert contributions["I1"].realized_pnl == 0.0
        assert contributions["I1"].fee_pnl == pytest.approx(-2.0)
        assert contributions["I1"].accrual_pnl == 0.0
        assert contributions["I1"].unrealized_pnl == pytest.approx(5.0)
        assert contributions["I2"].realized_pnl is None
        assert contributions["I2"].fee_pnl is None
        assert contributions["I2"].accrual_pnl is None
        assert contributions["I2"].unrealized_pnl is None
        assert point.realized_pnl is None
        assert point.fee_pnl is None
        assert point.accrual_pnl is None
        assert point.unrealized_pnl is None
        assert point.net_pnl is None
        assert _reason_rows(point) == [("cumulative_non_finite", "untrusted", "instrument", "I2")]
        overflow_executions = (
            _exec("I1", 1, 0, "buy", 0.0, 100.0, fee=-1e308),
            _exec("I2", 2, 0, "buy", 0.0, 100.0, fee=-1e308),
        )
        overflow_point = build_pnl_timeline(overflow_executions, (), {}, _window(0, 0)).points[0]
        assert overflow_point.realized_pnl is None
        assert overflow_point.fee_pnl is None
        assert overflow_point.accrual_pnl is None
        assert overflow_point.unrealized_pnl is None
        assert overflow_point.net_pnl is None
        assert all(
            contribution.realized_pnl is None
            and contribution.fee_pnl is None
            and contribution.accrual_pnl is None
            and contribution.unrealized_pnl is None
            for contribution in overflow_point.per_instrument
        )
        assert _reason_rows(overflow_point) == [
            ("cumulative_non_finite", "untrusted", "global", None)
        ]

    def test_unprovable_close_preserves_finite_peer(self) -> None:
        """An invalid reducing price taints only its own instrument contribution."""
        executions = (
            _exec("I1", 1, 0, "buy", 1.0, 100.0, fee=2.0),
            _exec("I2", 2, 0, "buy", 1.0, 50.0),
            _exec("I2", 3, 1, "sell", 1.0, float("nan")),
        )
        marks: MarkMap = {("I1", _m(1)): 105.0}
        point = build_pnl_timeline(executions, (), marks, _window(1, 1)).points[0]
        contributions = {
            contribution.instrument_public_id: contribution for contribution in point.per_instrument
        }
        assert contributions["I1"].realized_pnl == 0.0
        assert contributions["I1"].fee_pnl == pytest.approx(-2.0)
        assert contributions["I1"].accrual_pnl == 0.0
        assert contributions["I1"].unrealized_pnl == pytest.approx(5.0)
        assert contributions["I2"].realized_pnl is None
        assert contributions["I2"].fee_pnl is None
        assert contributions["I2"].accrual_pnl is None
        assert contributions["I2"].unrealized_pnl is None
        assert point.realized_pnl is None
        assert point.fee_pnl is None
        assert point.accrual_pnl is None
        assert point.unrealized_pnl is None
        assert point.net_pnl is None

    def test_unknown_seeded_basis_held_is_untrusted(self) -> None:
        """A non-flat opening position with no entry price withholds every component.

        A second flat opening position with no entry price is NOT flagged, so the
        seed guard distinguishes an unknown-cost holding from a flat placeholder.
        """
        opening = TimelineOpening(
            pools=(
                _opening_pool("I1", 1.0, None),
                _opening_pool("I2", 0.0, None),
            ),
            t0=_m(0),
        )
        result = build_pnl_timeline((), (), {("I1", _m(0)): 105.0}, _window(), opening)
        assert all(point.valuation_status == "incomplete" for point in result.points)
        assert result.points[0].realized_pnl is None
        assert result.points[0].net_pnl is None
        assert _reason_rows(result.points[0]) == [
            ("cost_basis_unavailable", "untrusted", "instrument", "I1")
        ]

    def test_unknown_seeded_basis_realization_permanently_taints(self) -> None:
        """Selling unknown-basis inventory taints realized for the rest of the series.

        The kernel cannot realize against an unknown entry, so a close that flushes
        the position flat still leaves the cumulative realized unknowable — every
        later point stays untrusted even though the pool is now flat.
        """
        opening = TimelineOpening(
            pools=(_opening_pool("I1", 1.0, None),),
            t0=_m(0),
        )
        executions = (_exec("I1", 1, 1, "sell", 1.0, 110.0),)
        result = build_pnl_timeline(executions, (), {}, _window(), opening)
        assert all(point.valuation_status == "incomplete" for point in result.points)
        assert result.points[5].realized_pnl is None
        assert _reason_rows(result.points[5]) == [
            ("cost_basis_unavailable", "untrusted", "instrument", "I1")
        ]

    def test_unknown_basis_add_stays_untrusted(self) -> None:
        """Adding to unknown-basis inventory keeps the position untrusted.

        A same-direction add neither realizes nor flushes the position flat, so the
        unknown seeded cost is still baked into the pool and every point remains
        untrusted.
        """
        opening = TimelineOpening(
            pools=(_opening_pool("I1", 1.0, None),),
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
        opening = TimelineOpening(pools=(), t0=_m(2))
        accruals = (TimelineAccrual(instrument_public_id="I1", accrued_at=_m(1), amount_usd=3.0),)
        result = build_pnl_timeline((), accruals, {}, _window(), opening)
        assert result.points[2].accrual_pnl == 0.0
        assert result.points[5].accrual_pnl == 0.0
        assert result.points[5].net_pnl == 0.0

    def test_post_t0_accrual_is_kept(self) -> None:
        """An accrual at or after the anchor t0 contributes normally."""
        opening = TimelineOpening(pools=(), t0=_m(0))
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
            pools=(_opening_pool("I1", 2 * FLAT_EPSILON, None),),
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
        assert _reason_rows(point) == [("cumulative_non_finite", "untrusted", "instrument", "I1")]

    def test_nan_entry_price_is_mark_incomplete(self) -> None:
        """A position opened at a NaN price withholds only its unrealized."""
        executions = (_exec("I1", 1, 0, "buy", 1.0, float("nan")),)
        marks: MarkMap = {("I1", _m(0)): 100.0}
        result = build_pnl_timeline(executions, (), marks, _window())
        point = result.points[0]
        assert point.valuation_status == "incomplete"
        assert point.unrealized_pnl is None
        assert point.realized_pnl == 0.0
        assert _reason_rows(point) == [
            ("execution_price_invalid", "mark_incomplete", "instrument", "I1")
        ]

    def test_overflow_unrealized_is_mark_incomplete(self) -> None:
        """A finite mark and entry whose difference overflows withhold the unrealized."""
        executions = (_exec("I1", 1, 0, "buy", 1.0, 1e308),)
        marks: MarkMap = {("I1", _m(0)): -1e308}
        result = build_pnl_timeline(executions, (), marks, _window())
        point = result.points[0]
        assert point.valuation_status == "incomplete"
        assert point.unrealized_pnl is None
        assert point.realized_pnl == 0.0
        assert _reason_rows(point) == [
            ("unrealized_non_finite", "mark_incomplete", "instrument", "I1")
        ]


class TestActivationBoundary:
    """Cover the pre-activation withholding (no backward certification of t0)."""

    def test_pre_activation_points_are_withheld(self) -> None:
        """Grid points before the anchor t0 are untrusted; t0 itself values normally.

        The anchor proves no position state before t0, so valuing a pre-t0 minute
        from the seeded book would fabricate history. The point at t0 (the seed's
        own instant) is complete.
        """
        opening = TimelineOpening(
            pools=(_opening_pool("I1", 1.0, 105.0),),
            t0=_m(2),
        )
        marks: MarkMap = {("I1", _m(0)): 90.0, ("I1", _m(1)): 95.0, ("I1", _m(2)): 105.0}
        result = build_pnl_timeline((), (), marks, _window(), opening)
        assert result.points[0].valuation_status == "incomplete"
        assert result.points[1].valuation_status == "incomplete"
        assert result.points[0].net_pnl is None
        assert result.points[2].valuation_status == "complete"
        assert result.points[2].net_pnl == pytest.approx(0.0)
        assert _reason_rows(result.points[0]) == [
            ("before_activation", "untrusted", "global", None)
        ]


class TestCorruptInputGuards:
    """Cover the fabrication/sign-dimension input guards and summation overflow."""

    def test_non_finite_seed_quantity_is_untrusted(self) -> None:
        """A seed with a non-finite quantity is tainted, never replayed.

        A NaN quantity would fabricate a finite-but-bogus realized on close (the
        kernel compares against NaN), so the instrument is withheld at ingestion.
        """
        opening = TimelineOpening(
            pools=(_opening_pool("I1", float("nan"), 100.0),),
            t0=_m(0),
        )
        result = build_pnl_timeline((), (), {("I1", _m(0)): 110.0}, _window(), opening)
        assert all(point.valuation_status == "incomplete" for point in result.points)
        assert result.points[0].realized_pnl is None
        assert _reason_rows(result.points[0]) == [
            ("seed_quantity_non_finite", "untrusted", "instrument", "I1")
        ]

    def test_negative_fill_size_is_untrusted(self) -> None:
        """A negative fill size is tainted, not sign-inverted into a short."""
        executions = (_exec("I1", 1, 0, "buy", -1.0, 100.0),)
        result = build_pnl_timeline(executions, (), {("I1", _m(0)): 90.0}, _window())
        assert all(point.valuation_status == "incomplete" for point in result.points)
        assert _reason_rows(result.points[0]) == [
            ("execution_size_invalid", "untrusted", "instrument", "I1")
        ]

    def test_non_finite_position_delta_is_untrusted(self) -> None:
        """A non-finite caller-resolved inventory delta never enters the pool."""
        executions = (_exec("I1", 1, 0, "buy", 1.0, 100.0, position_delta=float("nan")),)
        result = build_pnl_timeline(executions, (), {("I1", _m(0)): 90.0}, _window())
        assert all(point.valuation_status == "incomplete" for point in result.points)
        assert _reason_rows(result.points[0]) == [
            ("execution_size_invalid", "untrusted", "instrument", "I1")
        ]

    def test_replay_entry_arithmetic_failure_stamps_cost_basis_cause(self) -> None:
        """Finite fill inputs whose VWAP is unusable cannot escape without a cause."""
        executions = (
            _exec("I1", 1, 0, "buy", 1e308, 1e308),
            _exec("I1", 2, 0, "buy", 1e308, 1e308),
        )
        point = build_pnl_timeline(
            executions,
            (),
            {("I1", _m(0)): 1e308},
            _window(0, 0),
        ).points[0]
        assert _reason_rows(point) == [
            ("cost_basis_unavailable", "mark_incomplete", "instrument", "I1")
        ]

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
        lineage = {
            execution.order_public_id: TimelineExecutionLineage(
                "strategy",
                None,
                f"signal-{index}",
                "live",
                f"strategy-{index}",
            )
            for index, execution in enumerate(executions)
        }
        result = build_pnl_timeline(
            executions,
            (),
            marks,
            _window(0, 0),
            lineage=lineage,
        )
        point = result.points[0]
        assert point.valuation_status == "incomplete"
        assert point.unrealized_pnl is None
        assert point.net_pnl is None
        assert point.realized_pnl == 0.0
        assert _reason_rows(point) == [
            ("net_non_finite", "mark_incomplete", "global", None),
            ("unrealized_non_finite", "mark_incomplete", "global", None),
        ]

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
        assert _reason_rows(point) == [("net_non_finite", "mark_incomplete", "global", None)]

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
        assert _reason_rows(point) == [
            ("attribution_value_non_finite", "untrusted", "global", None)
        ]

    def test_unrealized_attribution_overflow_names_triggering_instrument(self) -> None:
        """Finite instrument values can overflow one bucket and retain its trigger."""
        executions = (
            _exec("I1", 1, 0, "buy", 1.0, 1.0),
            _exec("I2", 2, 0, "buy", 1.0, 1.0),
        )
        marks: MarkMap = {
            ("I1", _m(0)): 1e308,
            ("I2", _m(0)): 1e308,
        }
        point = build_pnl_timeline(executions, (), marks, _window(0, 0)).points[0]
        assert _reason_rows(point) == [
            (
                "attribution_value_non_finite",
                "mark_incomplete",
                "instrument",
                "I2",
            )
        ]
