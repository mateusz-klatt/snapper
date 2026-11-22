"""Overflow branch-and-bound recovery tests and PLN attribution/instrument repros."""

import math
import sys
from collections.abc import Hashable
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta

from snapper.application.portfolio.exact_sum import ExactSum
from snapper.application.portfolio.exact_sum import exact_units_of
from snapper.application.portfolio.exact_sum import faithful_roundings
from snapper.application.portfolio.pnl_faithful_recovery import MAX_OVERFLOW_NODES
from snapper.application.portfolio.pnl_faithful_recovery import MAX_OVERFLOW_STEPS
from snapper.application.portfolio.pnl_faithful_recovery import MAX_RECOVERY_BRANCH_KEYS
from snapper.application.portfolio.pnl_faithful_recovery import MAX_RECOVERY_KEY_VISITS
from snapper.application.portfolio.pnl_faithful_recovery import FaithfulFlowInput
from snapper.application.portfolio.pnl_faithful_recovery import FaithfulFlowRecovery
from snapper.application.portfolio.pnl_faithful_recovery import FaithfulRecoveryBudget
from snapper.application.portfolio.pnl_faithful_recovery import exceeds_branch_limit
from snapper.application.portfolio.pnl_timeline import AttributionKey
from snapper.application.portfolio.pnl_timeline import PnlTimelinePoint
from snapper.application.portfolio.pnl_timeline import TimelineExecution
from snapper.application.portfolio.pnl_timeline import TimelineExecutionLineage
from snapper.application.portfolio.pnl_timeline import TimelineWindow
from snapper.application.portfolio.pnl_timeline import _ExactCumulatives
from snapper.application.portfolio.pnl_timeline import _published_flow_family
from snapper.application.portfolio.pnl_timeline import _reconciled_buckets
from snapper.application.portfolio.pnl_timeline import build_pnl_timeline
from tests.application.portfolio.test_pnl_faithful_recovery import _class_i
from tests.application.portfolio.test_pnl_faithful_recovery import _class_w
from tests.application.portfolio.test_pnl_faithful_recovery import _only_far_total_dimension

_T0 = datetime(2026, 11, 20, 10, 0, tzinfo=UTC)

_EUR_PLN_RATES: tuple[float, float] = (4.2137, 4.2189)
_USD_PLN_RATES: tuple[float, float] = (3.9147, 3.9183)
_EUR_USD_RATES: tuple[float, float] = (1.0764, 1.0768)
_USD_EUR_RATES: tuple[float, float] = (0.9289, 0.9285)


@dataclass(frozen=True)
class _FeeFillSpec:
    """One synthetic fill specification producing two converted fee steps."""

    order_id: str
    lineage: TimelineExecutionLineage
    attr_key: AttributionKey
    instrument: str
    source_ccy: str
    portions: tuple[float, float]


def _convert_amount(amount: float, source_ccy: str, target_ccy: str, rate_index: int) -> float:
    """Convert one native fee amount using exact deterministic minute rates."""
    if source_ccy == target_ccy:
        return amount
    rates: dict[tuple[str, str], tuple[float, float]] = {
        ("EUR", "PLN"): _EUR_PLN_RATES,
        ("USD", "PLN"): _USD_PLN_RATES,
        ("EUR", "USD"): _EUR_USD_RATES,
        ("USD", "EUR"): _USD_EUR_RATES,
    }
    return amount * rates[(source_ccy, target_ccy)][rate_index]


def _build_fee_executions(
    specs: Sequence[_FeeFillSpec],
    instruments: Sequence[str],
    target_ccy: str,
) -> tuple[
    list[TimelineExecution],
    dict[str, TimelineExecutionLineage],
    dict[str, int],
    dict[AttributionKey, int],
    int,
]:
    """Convert fee specifications into executions and conserved unit totals."""
    executions: list[TimelineExecution] = []
    lineage: dict[str, TimelineExecutionLineage] = {}
    instrument_units: dict[str, int] = dict.fromkeys(instruments, 0)
    attribution_units: dict[AttributionKey, int] = {}
    total_units = 0
    sequence = 0
    for spec in specs:
        lineage[spec.order_id] = spec.lineage
        attribution_units.setdefault(spec.attr_key, 0)
        for step, portion in enumerate(spec.portions):
            sequence += 1
            converted_fee = _convert_amount(portion, spec.source_ccy, target_ccy, step)
            fee_pnl_units = exact_units_of(-converted_fee)
            instrument_units[spec.instrument] += fee_pnl_units
            attribution_units[spec.attr_key] += fee_pnl_units
            total_units += fee_pnl_units
            executions.append(
                TimelineExecution(
                    instrument_public_id=spec.instrument,
                    shard_key=f"sh-{spec.instrument}",
                    exchange="kraken",
                    scope_sequence=sequence,
                    event_time=_T0 + timedelta(seconds=sequence),
                    side="buy",
                    size=0.0,
                    position_delta=0.0,
                    price=10.0,
                    fee=converted_fee,
                    fee_asset=target_ccy,
                    order_public_id=spec.order_id,
                )
            )
    return executions, lineage, instrument_units, attribution_units, total_units


def _assert_complete_sums(point: PnlTimelinePoint) -> None:
    """Assert completeness and exact instrument and attribution float sums."""
    assert point.valuation_status == "complete"
    assert point.incompleteness_reasons == ()
    instrument_fees = [entry.fee_pnl for entry in point.per_instrument]
    attribution_fees = [entry.fee_pnl for entry in point.attribution]
    assert None not in instrument_fees
    assert None not in attribution_fees
    assert sum(value for value in instrument_fees if value is not None) == point.fee_pnl
    assert sum(value for value in attribution_fees if value is not None) == point.fee_pnl
    assert point.net_pnl == (
        (point.realized_pnl or 0.0)
        + (point.fee_pnl or 0.0)
        + (point.accrual_pnl or 0.0)
        + (point.unrealized_pnl or 0.0)
    )


def _assert_faithful_complete_point(
    point: PnlTimelinePoint,
    expected_instrument_units: dict[str, int],
    expected_attribution_units: dict[AttributionKey, int],
    expected_total_units: int,
) -> None:
    """Assert completeness, exact sums, and faithful roundings for recovered fee flows."""
    _assert_complete_sums(point)
    assert point.fee_pnl in faithful_roundings(expected_total_units)
    for entry in point.per_instrument:
        assert entry.fee_pnl in faithful_roundings(
            expected_instrument_units[entry.instrument_public_id]
        )
    for attr_entry in point.attribution:
        key: AttributionKey = (attr_entry.origin, attr_entry.strategy_name)
        assert attr_entry.fee_pnl in faithful_roundings(expected_attribution_units[key])


def _thirteen_attribution_inputs(
    target_ccy: str,
) -> tuple[
    list[TimelineExecution],
    dict[str, TimelineExecutionLineage],
    dict[str, int],
    dict[AttributionKey, int],
    int,
]:
    """Build two-instrument XRP-EUR/BTC-USD fills across thirteen attribution keys."""
    specs: list[_FeeFillSpec] = [
        _FeeFillSpec(
            "order-manual",
            TimelineExecutionLineage("rest", None, None, "live", None),
            ("manual", None),
            "XRP-EUR",
            "EUR",
            (2.8 * 0.25, 2.8 * 0.75),
        ),
        _FeeFillSpec(
            "order-system",
            TimelineExecutionLineage("strategy", None, "sig-sys", "live", "rebalance"),
            ("system", "rebalance"),
            "XRP-EUR",
            "EUR",
            (-23.85 * 0.25, -23.85 * 0.75),
        ),
    ]
    strategy_amounts = [23.87, -4.18, 10.51, -2.82, 4.8, -12.56, 10.37, -18.69, 5.1, -20.58, 23.8]
    for index, amount in enumerate(strategy_amounts):
        strategy_name = f"s{index:02d}"
        instrument, source_ccy = ("XRP-EUR", "EUR") if index < 6 else ("BTC-USD", "USD")
        specs.append(
            _FeeFillSpec(
                f"order-plan-{index}",
                TimelineExecutionLineage(
                    "strategy", f"plan-{index}", f"sig-{index}", "live", strategy_name
                ),
                ("plan", strategy_name),
                instrument,
                source_ccy,
                (amount * 0.25, amount * 0.75),
            )
        )
    return _build_fee_executions(specs, ("BTC-USD", "XRP-EUR"), target_ccy)


def _thirteen_instrument_inputs(
    target_ccy: str,
) -> tuple[
    list[TimelineExecution],
    dict[str, TimelineExecutionLineage],
    dict[str, int],
    dict[AttributionKey, int],
    int,
]:
    """Build thirteen EUR/USD instruments with a three-key Class W attribution split."""
    orders: tuple[tuple[str, TimelineExecutionLineage, AttributionKey], ...] = (
        (
            "order-manual",
            TimelineExecutionLineage("rest", None, None, "live", None),
            ("manual", None),
        ),
        (
            "order-plan",
            TimelineExecutionLineage("strategy", "plan-1", "sig-1", "live", "dca"),
            ("plan", "dca"),
        ),
        (
            "order-system",
            TimelineExecutionLineage("strategy", None, "sig-2", "live", "rebalance"),
            ("system", "rebalance"),
        ),
    )
    instruments: tuple[tuple[str, str], ...] = (
        ("XRP-EUR", "EUR"),
        ("ADA-EUR", "EUR"),
        ("BCH-EUR", "EUR"),
        ("BTC-EUR", "EUR"),
        ("DOT-EUR", "EUR"),
        ("ETH-EUR", "EUR"),
        ("LINK-EUR", "EUR"),
        ("LTC-EUR", "EUR"),
        ("BTC-USD", "USD"),
        ("AVAX-USD", "USD"),
        ("DOGE-USD", "USD"),
        ("ETH-USD", "USD"),
        ("SOL-USD", "USD"),
    )
    scales = (2.19, 17.31, 22.97, 2.11, 25.31, 4.41)
    specs: list[_FeeFillSpec] = []
    pair_index = 0
    for inst_index, (instrument, source_ccy) in enumerate(instruments):
        if instrument in ("XRP-EUR", "BTC-USD"):
            for attr_index, (order_id, order_lineage, attr_key) in enumerate(orders):
                amount = scales[pair_index * 3 + attr_index] * (-1.0 if attr_index == 1 else 1.0)
                specs.append(
                    _FeeFillSpec(
                        f"{order_id}-{instrument}",
                        order_lineage,
                        attr_key,
                        instrument,
                        source_ccy,
                        (amount * 0.25, amount * 0.75),
                    )
                )
            pair_index += 1
        else:
            attr_index = inst_index % 3
            order_id, order_lineage, attr_key = orders[attr_index]
            sign = -1.0 if attr_index == 1 else 1.0
            specs.append(
                _FeeFillSpec(
                    f"{order_id}-{instrument}",
                    order_lineage,
                    attr_key,
                    instrument,
                    source_ccy,
                    (0.25 * sign, 0.5 * sign),
                )
            )
    return _build_fee_executions(specs, [item[0] for item in instruments], target_ccy)


def _to_flow_request(
    instrument_units: dict[str, int],
    attribution_units: dict[AttributionKey, int],
    total_units: int,
) -> FaithfulFlowInput[str, AttributionKey]:
    """Assemble a canonical ordered recovery request from raw unit maps."""
    inst_keys = sorted(instrument_units)
    attr_keys = sorted(
        attribution_units,
        key=lambda item: (item[0], 0 if item[1] is None else 1, item[1] or ""),
    )
    return FaithfulFlowInput(
        total_units,
        tuple((key, instrument_units[key]) for key in inst_keys),
        tuple((key, attribution_units[key]) for key in attr_keys),
    )


def _branch_and_exact_overflow_request() -> FaithfulFlowInput[str, str]:
    """Build >12 binary buckets plus an exact zero where base_values does not match."""
    unit = math.ulp(20.0)
    core = (
        ("c0", exact_units_of(32.0) + exact_units_of(0.875 * unit)),
        ("c1", exact_units_of(120.0) + exact_units_of(1.875 * unit)),
        ("c2", exact_units_of(-132.0) + exact_units_of(2.875 * unit)),
    )
    extra = tuple((f"b{index}", exact_units_of(1.0) + 1) for index in range(11))
    instruments = (*core, *extra, ("exact-zero", 0))
    total_units = sum(units for _, units in instruments)
    return FaithfulFlowInput(total_units, instruments, (("all", total_units),))


def _node_limit_cancelling_request() -> FaithfulFlowInput[str, str]:
    """Build cancelling +/-100 pairs plus a tiny bucket that prunes deep without leaves."""
    ulp100 = exact_units_of(math.ulp(100.0))
    ulp01 = exact_units_of(math.ulp(0.1))
    buckets: list[tuple[str, int]] = []
    for index in range(10):
        shift = (ulp100 // 4) if index < 2 else 0
        buckets.append((f"p{index}", exact_units_of(100.0) + (ulp100 // 8) + shift))
        buckets.append((f"n{index}", exact_units_of(-100.0) - (ulp100 // 8)))
    buckets.append(("small", exact_units_of(0.1) + (ulp01 // 4)))
    total_units = sum(units for _, units in buckets)
    return FaithfulFlowInput(total_units, tuple(buckets), (("all", total_units),))


def test_pln_all_1441_points_publish_when_thirteen_converted_attributions_split() -> None:
    """Publish all 1,441 PLN points when 13 converted attribution buckets exceed branch cap.

    Given: Two instruments split across 13 converted attribution keys,
    When: Timelines are built in USD, EUR, and PLN across 1,441 one-minute points,
    Then: Legacy publication withholds PLN while overflow recovery publishes every point.
    """
    window_start = _T0 + timedelta(minutes=1)
    window_end = _T0 + timedelta(minutes=1441)

    for ccy in ("USD", "EUR"):
        executions, lineage, _, _, _ = _thirteen_attribution_inputs(ccy)
        result = build_pnl_timeline(
            executions,
            (),
            {},
            TimelineWindow(window_start, window_end, "1m", ccy),
            lineage=lineage,
        )
        assert len(result.points) == 1441
        for point in result.points:
            _assert_complete_sums(point)

    pln_execs, pln_lineage, pln_inst_u, pln_attr_u, pln_tot_u = _thirteen_attribution_inputs("PLN")
    assert sum(pln_inst_u.values()) == pln_tot_u
    assert sum(pln_attr_u.values()) == pln_tot_u

    exact_family = _ExactCumulatives(
        by_instrument={key: ExactSum(units=val) for key, val in pln_inst_u.items()},
        by_attribution={key: ExactSum(units=val) for key, val in pln_attr_u.items()},
        total=ExactSum(units=pln_tot_u),
    )
    inst_keys = sorted(pln_inst_u)
    attr_keys = sorted(
        pln_attr_u,
        key=lambda item: (item[0], 0 if item[1] is None else 1, item[1] or ""),
    )
    assert faithful_roundings(pln_tot_u) == (6.0311680000000205, 6.03116800000002)
    for total_candidate in faithful_roundings(pln_tot_u):
        assert _reconciled_buckets(pln_inst_u, inst_keys, total_candidate, True) is not None
        assert _reconciled_buckets(pln_attr_u, attr_keys, total_candidate, False) is None
        assert _reconciled_buckets(pln_attr_u, attr_keys, total_candidate, True) is None
    assert _published_flow_family(exact_family, inst_keys, attr_keys) == (
        "attribution_sum_unrepresentable"
    )

    pln_result = build_pnl_timeline(
        pln_execs,
        (),
        {},
        TimelineWindow(window_start, window_end, "1m", "PLN"),
        lineage=pln_lineage,
    )
    assert len(pln_result.points) == 1441
    for point in pln_result.points:
        _assert_faithful_complete_point(point, pln_inst_u, pln_attr_u, pln_tot_u)


def test_pln_publishes_when_thirteen_converted_instruments_block_attribution_recovery() -> None:
    """Publish PLN points when 13 converted instruments trigger the recovery branch cap.

    Given: Thirteen converted EUR/USD instruments feeding a Class W attribution split,
    When: Timelines are built in USD, EUR, and PLN across five one-minute points,
    Then: Legacy publication withholds PLN while overflow recovery publishes every point.
    """
    window_start = _T0 + timedelta(minutes=1)
    window_end = _T0 + timedelta(minutes=5)

    for ccy in ("USD", "EUR"):
        executions, lineage, _, _, _ = _thirteen_instrument_inputs(ccy)
        result = build_pnl_timeline(
            executions,
            (),
            {},
            TimelineWindow(window_start, window_end, "1m", ccy),
            lineage=lineage,
        )
        for point in result.points:
            _assert_complete_sums(point)

    pln_execs, pln_lineage, pln_inst_u, pln_attr_u, pln_tot_u = _thirteen_instrument_inputs("PLN")
    assert sum(pln_inst_u.values()) == pln_tot_u
    assert sum(pln_attr_u.values()) == pln_tot_u
    assert faithful_roundings(pln_tot_u) == (31.461260999999986, 31.46126099999999)

    exact_family = _ExactCumulatives(
        by_instrument={key: ExactSum(units=val) for key, val in pln_inst_u.items()},
        by_attribution={key: ExactSum(units=val) for key, val in pln_attr_u.items()},
        total=ExactSum(units=pln_tot_u),
    )
    inst_keys = sorted(pln_inst_u)
    attr_keys = sorted(
        pln_attr_u,
        key=lambda item: (item[0], 0 if item[1] is None else 1, item[1] or ""),
    )
    assert _reconciled_buckets(pln_inst_u, inst_keys, 31.461260999999986, True) is not None
    assert _reconciled_buckets(pln_attr_u, attr_keys, 31.461260999999986, True) is None
    assert _reconciled_buckets(pln_inst_u, inst_keys, 31.46126099999999, True) is None
    assert _reconciled_buckets(pln_attr_u, attr_keys, 31.46126099999999, True) is None
    assert _published_flow_family(exact_family, inst_keys, attr_keys) == (
        "attribution_sum_unrepresentable"
    )

    pln_result = build_pnl_timeline(
        pln_execs,
        (),
        {},
        TimelineWindow(window_start, window_end, "1m", "PLN"),
        lineage=pln_lineage,
    )
    for point in pln_result.points:
        _assert_faithful_complete_point(point, pln_inst_u, pln_attr_u, pln_tot_u)


def test_exceeds_branch_limit_counts_only_binary_buckets() -> None:
    """Count only two-choice buckets when checking the Cartesian branch cap.

    Given: Exact zero buckets alongside twelve or thirteen binary-choice buckets,
    When: Checking ``exceeds_branch_limit`` at the twelve-branch boundary,
    Then: Twelve binary buckets stay below the cap and thirteen exceed it.
    """
    inexact = exact_units_of(1.0) + 1
    at_cap = tuple((f"b{index}", inexact) for index in range(MAX_RECOVERY_BRANCH_KEYS))
    exact_zeros = tuple((f"z{index}", 0) for index in range(5))
    over_cap = (*at_cap, ("b12", inexact))
    assert not exceeds_branch_limit((*at_cap, *exact_zeros))
    assert exceeds_branch_limit((*over_cap, *exact_zeros))


def test_recover_overflow_finds_solution_with_exact_buckets_and_reuses_cache() -> None:
    """Recover an overflow split with mixed binary/exact buckets and cache the result.

    Given: Fourteen binary buckets and an exact zero bucket where nearest sum misses,
    When: Running ``recover_overflow`` twice for the same input,
    Then: It recovers a faithful conserved split and reuses the cache at zero cost.
    """
    request = _branch_and_exact_overflow_request()
    budget = FaithfulRecoveryBudget()
    recovery = FaithfulFlowRecovery[str, str](budget)

    projection = recovery.recover_overflow(request)

    assert projection is not None
    assert sum(value for _, value in projection.instruments) == projection.total
    assert sum(value for _, value in projection.attributions) == projection.total
    assert dict(projection.instruments)["exact-zero"] == 0.0
    for (_, units), (_, value) in zip(request.instruments, projection.instruments, strict=True):
        assert value in faithful_roundings(units)
    remaining = budget.remaining_key_visits
    assert remaining < MAX_RECOVERY_KEY_VISITS
    assert recovery.recover_overflow(replace(request)) is projection
    assert budget.remaining_key_visits == remaining


def test_recover_overflow_returns_none_for_impossible_inputs_and_caches_completion() -> None:
    """Return None when overflow dimensions are provably impossible or mismatched.

    Given: Overflow inputs failing by modular pruning, missing branches, or dimension mismatch,
    When: Calling ``recover_overflow`` on each impossible configuration,
    Then: Each returns None and completed impossibility results are reused without extra visits.
    """
    inexact = exact_units_of(1.0) + 1
    thirteen_binary = tuple((f"k{index}", inexact) for index in range(13))
    overflow_total = exact_units_of(1e308)
    no_branch_overflow = (
        ("x1", overflow_total),
        ("x2", overflow_total),
        ("x3", -overflow_total),
    )

    ulp100 = exact_units_of(math.ulp(100.0))
    modular_gap_buckets = (
        *(
            (
                f"p{index}",
                exact_units_of(100.0) + (ulp100 // 8) + (ulp100 // 4 if index < 2 else 0),
            )
            for index in range(7)
        ),
        *((f"n{index}", exact_units_of(-100.0) - (ulp100 // 8)) for index in range(7)),
        ("fixed-tiny", exact_units_of(0.1)),
    )
    modular_tot = sum(units for _, units in modular_gap_buckets)

    only_far_inst = _only_far_total_dimension()
    far_tot = sum(units for _, units in only_far_inst)
    ulp1000 = exact_units_of(math.ulp(1000.0))
    shifted_anchor = faithful_roundings(far_tot)[0] - 4 * math.ulp(20.0)
    shifted_anchor_units = exact_units_of(shifted_anchor)
    padded_attr = (
        ("anchor", shifted_anchor_units),
        *(
            (
                f"ap{index}",
                exact_units_of(1000.0)
                + (ulp1000 // 8)
                + ((far_tot - shifted_anchor_units) if index == 0 else 0),
            )
            for index in range(7)
        ),
        *((f"an{index}", exact_units_of(-1000.0) - (ulp1000 // 8)) for index in range(7)),
    )

    requests: tuple[FaithfulFlowInput[str, str], ...] = (
        FaithfulFlowInput(modular_tot, modular_gap_buckets, (("a", modular_tot),)),
        FaithfulFlowInput(
            overflow_total,
            no_branch_overflow,
            (*thirteen_binary, ("bal", -13 * inexact), ("huge", overflow_total)),
        ),
        FaithfulFlowInput(far_tot, (*only_far_inst, ("zero", 0)), padded_attr),
    )
    for request in requests:
        budget = FaithfulRecoveryBudget()
        recovery = FaithfulFlowRecovery[str, str](budget)
        assert recovery.recover_overflow(request) is None
        spent = MAX_RECOVERY_KEY_VISITS - budget.remaining_key_visits
        assert spent > 0
        assert recovery.recover_overflow(replace(request)) is None
        assert budget.remaining_key_visits == MAX_RECOVERY_KEY_VISITS - spent


def test_recover_overflow_handles_dirty_false_leaf_and_nonmatching_leaf() -> None:
    """Exercise unflipped-leaf skipping and failing-leaf evaluation during bounded search.

    Given: An overflow request where nearest-first base sum fails and a later leaf matches,
    When: Bounded DFS must backtrack through an unflipped leaf and non-matching leaves,
    Then: It skips the unflipped leaf cleanly and recovers the matching projection.
    """
    ulp100 = exact_units_of(math.ulp(100.0))
    buckets = (
        ("a0", exact_units_of(100.0) + (ulp100 // 4)),
        ("a1", exact_units_of(-100.0) - (ulp100 // 4)),
        ("b0", exact_units_of(10.0) + exact_units_of(0.6 * math.ulp(10.0))),
        *((f"d{index}", exact_units_of(1.0) + 1) for index in range(11)),
    )
    total_units = sum(units for _, units in buckets)
    request = FaithfulFlowInput(total_units, buckets, (("only", total_units),))
    budget = FaithfulRecoveryBudget()
    recovery = FaithfulFlowRecovery[str, str](budget)

    projection = recovery.recover_overflow(request)

    assert projection is not None
    assert sum(value for _, value in projection.instruments) == projection.total


def test_recover_overflow_returns_none_when_step_or_node_limit_exhausted() -> None:
    """Stop bounded search at step or node limits without overwriting prior cache entries.

    Given: A cached projection and two overflow inputs hitting MAX_OVERFLOW_STEPS or nodes,
    When: Calling ``recover_overflow`` on both limit-exhausting requests,
    Then: Both return None, terminate cleanly, and preserve the cached entry at zero cost.
    """
    budget = FaithfulRecoveryBudget()
    recovery = FaithfulFlowRecovery[str, str](budget)
    cached_request = FaithfulFlowInput(1, (("i", 1),), (("a", 1),))
    cached_projection = recovery.recover_overflow(cached_request)
    assert cached_projection is not None

    huge = exact_units_of(1e308)
    inexact = exact_units_of(1.0) + 1
    step_limit_instruments = (
        ("h1", huge),
        ("h2", huge),
        ("h3", -huge),
        *((f"k{index}", inexact) for index in range(MAX_RECOVERY_BRANCH_KEYS + 1)),
    )
    step_tot = sum(units for _, units in step_limit_instruments)
    step_request = FaithfulFlowInput(step_tot, step_limit_instruments, (("a", step_tot),))
    before_step_budget = budget.remaining_key_visits
    assert recovery.recover_overflow(step_request) is None
    assert budget.remaining_key_visits == before_step_budget - (
        len(step_limit_instruments) * (MAX_OVERFLOW_STEPS + 1) * len(faithful_roundings(step_tot))
    )

    node_request = _node_limit_cancelling_request()
    assert recovery.recover_overflow(node_request) is None
    assert MAX_OVERFLOW_NODES > MAX_OVERFLOW_STEPS
    remaining = budget.remaining_key_visits
    assert recovery.recover_overflow(cached_request) is cached_projection
    assert budget.remaining_key_visits == remaining


def test_recover_overflow_budget_exhaustion_preserves_prior_cache() -> None:
    """Refuse budget-exhausted searches at precheck, base check, and leaf evaluation.

    Given: Budgets sized to exhaust before search, at a leaf, and on dimension base check,
    When: Running ``recover_overflow`` after caching an earlier terminal projection,
    Then: None of the interrupted searches leaks partial state or evicts the cached entry.
    """
    request = _branch_and_exact_overflow_request()
    inst_len = len(request.instruments)
    attr_len = len(request.attributions)

    precheck_budget = FaithfulRecoveryBudget(inst_len + attr_len)
    precheck_recovery = FaithfulFlowRecovery[str, str](precheck_budget)
    cached_in = FaithfulFlowInput(1, (("i", 1),), (("a", 1),))
    cached = precheck_recovery.recover_overflow(cached_in)
    assert cached is not None
    assert precheck_budget.remaining_key_visits == inst_len + attr_len - 2
    assert precheck_recovery.recover_overflow(request) is None
    assert precheck_budget.remaining_key_visits == inst_len + attr_len - 2
    assert precheck_recovery.recover_overflow(cached_in) is cached

    leaf_budget = FaithfulRecoveryBudget(inst_len + attr_len + 2)
    leaf_recovery = FaithfulFlowRecovery[str, str](leaf_budget)
    cached_leaf = leaf_recovery.recover_overflow(cached_in)
    assert cached_leaf is not None
    assert leaf_recovery.recover_overflow(request) is None
    assert leaf_budget.remaining_key_visits == attr_len
    assert leaf_recovery.recover_overflow(cached_in) is cached_leaf

    class_w = _class_w()
    attr_delta = sum(units for _, units in request.instruments) - class_w.total_units
    dim_attr = (*request.instruments, ("balance", -attr_delta))
    dim_budget = FaithfulRecoveryBudget(len(class_w.instruments) + len(dim_attr))
    dim_recovery = FaithfulFlowRecovery[str, str](dim_budget)
    dim_request = FaithfulFlowInput(class_w.total_units, class_w.instruments, dim_attr)
    assert dim_recovery.recover_overflow(dim_request) is None
    assert dim_budget.remaining_key_visits < len(dim_attr)


def test_recover_overflow_rejects_invalid_inputs_without_spending() -> None:
    """Reject non-conserving, duplicate-key, oversized, and non-finite inputs cleanly.

    Given: Invalid ``FaithfulFlowInput`` instances violating size, keys, conservation, or range,
    When: Calling ``recover_overflow`` with a fresh budget,
    Then: Each returns None and spends zero candidate-key visits.
    """
    overflow = 2 * exact_units_of(sys.float_info.max)
    invalid_cases: tuple[FaithfulFlowInput[str, str], ...] = (
        FaithfulFlowInput(0, tuple((f"k{index}", 0) for index in range(65)), ()),
        FaithfulFlowInput(overflow, (("i", overflow),), (("a", overflow),)),
        FaithfulFlowInput(0, (("i", 0), ("i", 0)), (("a", 0),)),
        FaithfulFlowInput(0, (("i", 1),), (("a", 0),)),
    )
    for bad in invalid_cases:
        budget = FaithfulRecoveryBudget()
        assert FaithfulFlowRecovery[str, str](budget).recover_overflow(bad) is None
        assert budget.remaining_key_visits == MAX_RECOVERY_KEY_VISITS


def _assert_deterministic[AttrT: Hashable](request: FaithfulFlowInput[str, AttrT]) -> None:
    """Assert identical overflow projections and budget consumption across independent runs."""
    first_budget = FaithfulRecoveryBudget()
    second_budget = FaithfulRecoveryBudget()
    first = FaithfulFlowRecovery[str, AttrT](first_budget).recover_overflow(request)
    second = FaithfulFlowRecovery[str, AttrT](second_budget).recover_overflow(request)
    assert first == second
    assert first_budget.remaining_key_visits == second_budget.remaining_key_visits


def test_recover_overflow_is_deterministic() -> None:
    """Produce bit-identical projections and budget usage across independent runs.

    Given: Identical 13-attribution and 13-instrument PLN inputs and an exhausted input,
    When: Running ``recover_overflow`` on independent recovery instances,
    Then: Projections, exact totals, and remaining key visits match identically.
    """
    _, _, attr_inst_u, attr_attr_u, attr_tot_u = _thirteen_attribution_inputs("PLN")
    _, _, inst_inst_u, inst_attr_u, inst_tot_u = _thirteen_instrument_inputs("PLN")
    _assert_deterministic(_to_flow_request(attr_inst_u, attr_attr_u, attr_tot_u))
    _assert_deterministic(_to_flow_request(inst_inst_u, inst_attr_u, inst_tot_u))
    _assert_deterministic(_node_limit_cancelling_request())


def test_recover_overflow_matches_recover_at_or_below_branch_cap() -> None:
    """Match ``recover()`` on eligible inputs at or below the 12-branch cap.

    Given: Class W, far-total-only, impossible Class I, empty zero, and float-endpoint inputs,
    When: Comparing ``recover_overflow`` with ``recover`` on independent instances,
    Then: Both return identical projections or None on every case.
    """
    far_keys = _only_far_total_dimension()
    far_request = FaithfulFlowInput(sum(units for _, units in far_keys), far_keys, far_keys)
    max_units = exact_units_of(sys.float_info.max)
    min_units = exact_units_of(-sys.float_info.max)
    endpoint_pos = FaithfulFlowInput(
        max_units,
        (("i1", max_units), ("i2", 1), ("i3", -1)),
        (("a", max_units),),
    )
    endpoint_neg = FaithfulFlowInput(
        min_units,
        (("i1", min_units), ("i2", 1), ("i3", -1)),
        (("a", min_units),),
    )
    cases: tuple[FaithfulFlowInput[str, str], ...] = (
        _class_w(),
        far_request,
        _class_i(),
        _class_i(math.ulp(0.3) / 4),
        FaithfulFlowInput(0, (), ()),
        endpoint_pos,
        endpoint_neg,
    )
    for request in cases:
        rec_proj = FaithfulFlowRecovery[str, str](FaithfulRecoveryBudget()).recover(request)
        ovf_proj = FaithfulFlowRecovery[str, str](FaithfulRecoveryBudget()).recover_overflow(
            request
        )
        assert ovf_proj == rec_proj
