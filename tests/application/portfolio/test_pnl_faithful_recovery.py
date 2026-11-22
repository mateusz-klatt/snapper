"""Bounded faithful projections, exact ownership and request-local reuse."""

import math
import sys
from dataclasses import replace
from itertools import product

import pytest

from snapper.application.portfolio.exact_sum import exact_units_of
from snapper.application.portfolio.exact_sum import faithful_roundings
from snapper.application.portfolio.pnl_faithful_recovery import MAX_RECOVERY_KEY_VISITS
from snapper.application.portfolio.pnl_faithful_recovery import FaithfulFlowInput
from snapper.application.portfolio.pnl_faithful_recovery import FaithfulFlowRecovery
from snapper.application.portfolio.pnl_faithful_recovery import FaithfulRecoveryBudget


def _class_w() -> FaithfulFlowInput[str, str]:
    """Build exact units before projecting the three cancelling buckets."""
    unit = math.ulp(20.0)
    units = tuple(
        exact_units_of(base) + exact_units_of(fraction * unit)
        for base, fraction in ((32.0, 0.875), (120.0, 1.875), (-132.0, 2.875))
    )
    return FaithfulFlowInput(
        sum(units),
        tuple(zip(("I1", "I2", "I3"), units, strict=True)),
        tuple(zip(("manual", "plan", "system"), units, strict=True)),
    )


def _class_i(extra: float = 0.0) -> FaithfulFlowInput[str, str]:
    """A fine exact total cannot be reconstructed on the coarse bucket lattice."""
    small = exact_units_of(0.60 - 0.50)
    units = (
        small + exact_units_of(40.0) + exact_units_of(extra),
        small + exact_units_of(35.0),
        small + exact_units_of(-75.0),
    )
    total = sum(units)
    return FaithfulFlowInput(
        total, (("one", total),), tuple(zip(("manual", "plan", "system"), units, strict=True))
    )


@pytest.mark.parametrize("allowance", [-1, MAX_RECOVERY_KEY_VISITS + 1])
def test_request_allowance_cannot_exceed_the_admitted_bound(allowance: int) -> None:
    """Reject an invalid request allowance.

    Given: An allowance outside the admitted closed interval,
    When: A request-owned budget is created,
    Then: It refuses rather than admitting unbounded recovery work.
    """
    with pytest.raises(ValueError, match="request bound"):
        FaithfulRecoveryBudget(allowance)


def test_budget_boundary_is_inclusive_and_failed_reservations_are_atomic() -> None:
    """Prepay complete candidate scans.

    Given: The full request allowance,
    When: Excessive, negative and exact-boundary costs are requested,
    Then: Only the complete affordable scan changes the remaining allowance.
    """
    budget = FaithfulRecoveryBudget()
    assert not budget.reserve(MAX_RECOVERY_KEY_VISITS + 1)
    assert budget.remaining_key_visits == MAX_RECOVERY_KEY_VISITS
    with pytest.raises(ValueError, match="negative"):
        budget.reserve(-1)
    assert budget.reserve(MAX_RECOVERY_KEY_VISITS)
    assert budget.remaining_key_visits == 0
    assert not budget.reserve(1)
    assert budget.reserve(0)


def test_actual_candidate_cost_recovers_before_the_pessimistic_full_pair_cost() -> None:
    """Recover a split that fits actual visits rather than worst-case reservation.

    Given: Both dimensions need their seventh three-key faithful tuple,
    When: Exactly 42 candidate-key visits are available,
    Then: Recovery succeeds, conserves both dimensions and reuses at zero cost.
    """
    budget = FaithfulRecoveryBudget(42)
    recovery = FaithfulFlowRecovery[str, str](budget)
    request = _class_w()

    projection = recovery.recover(request)

    assert projection is not None
    assert projection.total == 20.00000000000002
    assert sum(value for _, value in projection.instruments) == projection.total
    assert sum(value for _, value in projection.attributions) == projection.total
    for (_, units), (_, value) in zip(request.instruments, projection.instruments, strict=True):
        assert value in faithful_roundings(units)
    assert budget.remaining_key_visits == 0
    assert recovery.recover(replace(request)) is projection
    assert budget.remaining_key_visits == 0


def test_interrupted_family_does_not_replace_a_previous_terminal_cache_entry() -> None:
    """Retain completed state when another family search cannot finish.

    Given: A cached two-visit projection and only 41 visits for Class W,
    When: The second dimension exhausts its allowance during recovery,
    Then: No partial result escapes and the earlier entry remains a free hit.
    """
    budget = FaithfulRecoveryBudget(43)
    recovery = FaithfulFlowRecovery[str, str](budget)
    first = FaithfulFlowInput(1, (("i", 1),), (("a", 1),))
    projection = recovery.recover(first)
    assert projection is not None

    assert recovery.recover(_class_w()) is None
    assert budget.remaining_key_visits == 2
    assert recovery.recover(first) is projection
    assert budget.remaining_key_visits == 2
    assert recovery.recover(_class_w()) is None
    assert budget.remaining_key_visits == 2


@pytest.mark.parametrize("extra", [0.0, math.ulp(0.3) / 4])
def test_completed_impossible_search_is_reused_without_more_visits(extra: float) -> None:
    """Cache a terminal refusal, including a family with two faithful totals.

    Given: Coarse cancelling buckets with a finer exact total,
    When: The complete finite faithful search cannot represent that total,
    Then: Identical later inputs retain the refusal without re-enumerating.
    """
    budget = FaithfulRecoveryBudget()
    recovery = FaithfulFlowRecovery[str, str](budget)
    request = _class_i(extra)

    assert recovery.recover(request) is None
    remaining = budget.remaining_key_visits
    assert remaining < MAX_RECOVERY_KEY_VISITS
    assert recovery.recover(replace(request)) is None
    assert budget.remaining_key_visits == remaining


@pytest.mark.parametrize("dimension", ["instruments", "attributions"])
def test_binary_choice_limit_is_checked_before_either_dimension_spends(dimension: str) -> None:
    """Do not starve later families with structurally ineligible input.

    Given: Thirteen genuinely binary choices in one dimension,
    When: Recovery checks the family followed by an affordable Class W,
    Then: The rejected family consumes zero visits and Class W still recovers.
    """
    units = exact_units_of(1.0) + 1
    branches = tuple((f"key-{index}", units) for index in range(13))
    single = (("one", 13 * units),)
    request = FaithfulFlowInput(
        13 * units,
        branches if dimension == "instruments" else single,
        branches if dimension == "attributions" else single,
    )
    budget = FaithfulRecoveryBudget(42)
    recovery = FaithfulFlowRecovery[str, str](budget)

    assert recovery.recover(request) is None
    assert budget.remaining_key_visits == 42
    assert recovery.recover(_class_w()) is not None
    assert budget.remaining_key_visits == 0


def _invalid_inputs() -> tuple[FaithfulFlowInput[str, str], ...]:
    """Supply independent ownership, conservation, finiteness and size faults."""
    overflow = 2 * exact_units_of(sys.float_info.max)
    return (
        FaithfulFlowInput(0, tuple((f"key-{i}", 0) for i in range(65)), ()),
        FaithfulFlowInput(overflow, (("i", overflow),), (("a", overflow),)),
        FaithfulFlowInput(0, (("i", 0), ("i", 0)), (("a", 0),)),
        FaithfulFlowInput(0, (("i", 1),), (("a", 0),)),
        FaithfulFlowInput(0, (("i", 0),), (("a", 1),)),
        FaithfulFlowInput(0, (("i1", overflow), ("i2", -overflow)), (("a", 0),)),
    )


@pytest.mark.parametrize("inputs", _invalid_inputs())
def test_untrusted_or_oversized_inputs_never_spend_or_publish(
    inputs: FaithfulFlowInput[str, str],
) -> None:
    """Refuse invalid exact input before projection.

    Given: Duplicate ownership, missing units, nonfinite projection or too many keys,
    When: Recovery receives that input,
    Then: It produces no financial projection and spends no candidate visits.
    """
    budget = FaithfulRecoveryBudget()
    assert FaithfulFlowRecovery[str, str](budget).recover(inputs) is None
    assert budget.remaining_key_visits == MAX_RECOVERY_KEY_VISITS


@pytest.mark.parametrize("change", ["units", "order", "zero_key"])
def test_cache_identity_includes_exact_units_and_complete_ordered_keys(change: str) -> None:
    """Invalidate terminal reuse when the publication input changes.

    Given: A completed Class W projection,
    When: Exact units, key order or an explicit zero key changes,
    Then: Recovery inspects new candidates and retains the new key ownership.
    """
    budget = FaithfulRecoveryBudget()
    recovery = FaithfulFlowRecovery[str, str](budget)
    original = _class_w()
    assert recovery.recover(original) is not None
    remaining = budget.remaining_key_visits
    if change == "units":
        request = replace(
            original,
            total_units=original.total_units + 1,
            instruments=(
                (original.instruments[0][0], original.instruments[0][1] + 1),
                *original.instruments[1:],
            ),
            attributions=(
                (original.attributions[0][0], original.attributions[0][1] + 1),
                *original.attributions[1:],
            ),
        )
    elif change == "order":
        request = replace(
            original,
            instruments=tuple(reversed(original.instruments)),
            attributions=tuple(reversed(original.attributions)),
        )
    else:
        request = replace(
            original,
            instruments=(*original.instruments, ("zero", 0)),
            attributions=(*original.attributions, ("zero", 0)),
        )

    projection = recovery.recover(request)

    assert projection is not None
    assert budget.remaining_key_visits < remaining
    assert tuple(key for key, _ in projection.instruments) == tuple(
        key for key, _ in request.instruments
    )
    assert tuple(key for key, _ in projection.attributions) == tuple(
        key for key, _ in request.attributions
    )
    remaining = budget.remaining_key_visits
    assert recovery.recover(request) is projection
    assert budget.remaining_key_visits == remaining


def test_three_family_slots_share_the_same_request_allowance() -> None:
    """Account for all families in one request.

    Given: Three independent terminal slots sharing 126 candidate-key visits,
    When: Each recovers the same 42-visit Class W input,
    Then: All succeed and a fourth uncached search cannot spend more.
    """
    budget = FaithfulRecoveryBudget(126)
    for _ in range(3):
        assert FaithfulFlowRecovery[str, str](budget).recover(_class_w()) is not None
    assert budget.remaining_key_visits == 0
    assert FaithfulFlowRecovery[str, str](budget).recover(_class_w()) is None


def test_finite_buckets_with_an_overflowing_builtin_sum_are_not_published() -> None:
    """Retain refusal when intermediate floating addition overflows.

    Given: Finite conserved buckets whose ordered builtin sum is nonfinite,
    When: The exact aggregate is itself finite,
    Then: Recovery refuses rather than exposing an infinite public breakdown.
    """
    units = exact_units_of(1e308)
    request = FaithfulFlowInput(
        units, (("i1", units), ("i2", units), ("i3", -units)), (("a", units),)
    )
    assert FaithfulFlowRecovery[str, str](FaithfulRecoveryBudget()).recover(request) is None


@pytest.mark.parametrize("units", [0, 1, 3, exact_units_of(sys.float_info.max) + 1])
def test_zero_subnormal_and_finite_endpoint_choices_remain_faithful(units: int) -> None:
    """Use the exact accumulator's finite bracketing contract at endpoints.

    Given: Zero, subnormal units or an inexact value just above the largest double,
    When: Both dimensions hold that exact aggregate,
    Then: A finite faithful value publishes without moving an exact zero.
    """
    request = FaithfulFlowInput(units, (("i", units), ("zero", 0)), (("a", units),))
    projection = FaithfulFlowRecovery[str, str](FaithfulRecoveryBudget()).recover(request)
    assert projection is not None
    assert projection.total in faithful_roundings(units)
    assert math.isfinite(projection.total)
    assert dict(projection.instruments)["zero"] == 0.0


def test_empty_zero_dimensions_publish_without_spending() -> None:
    """Preserve the empty zero family.

    Given: No ownership keys and an exact zero aggregate,
    When: The request has no enumeration allowance,
    Then: Its empty projection is still coherent and consumes zero visits.
    """
    budget = FaithfulRecoveryBudget(0)
    projection = FaithfulFlowRecovery[str, str](budget).recover(FaithfulFlowInput(0, (), ()))
    assert projection is not None
    assert projection.total == 0.0
    assert projection.instruments == projection.attributions == ()
    assert budget.remaining_key_visits == 0


def _only_far_total_dimension() -> tuple[tuple[str, int], ...]:
    """Keep the smaller exact bucket odd while larger buckets move in four-unit steps."""
    unit = math.ulp(20.0)
    return (
        ("large-positive", exact_units_of(100.0) + exact_units_of(1.125 * unit)),
        ("large-negative", exact_units_of(-100.0) + exact_units_of(3.5 * unit)),
        ("fixed", exact_units_of(20.0 + unit)),
    )


def test_second_total_can_recover_after_the_nearest_candidate_is_exhausted() -> None:
    """Use a common farther faithful total when the nearest cannot be represented.

    Given: Both dimensions can represent only the farther faithful total,
    When: The nearest-first recovery exhausts its first candidate,
    Then: The second candidate publishes both dimensions consistently.
    """
    keys = _only_far_total_dimension()
    inputs = FaithfulFlowInput(sum(units for _, units in keys), keys, keys)
    assert len(faithful_roundings(inputs.total_units)) == 2
    projection = FaithfulFlowRecovery[str, str](FaithfulRecoveryBudget()).recover(inputs)
    assert projection is not None
    assert projection.total == faithful_roundings(inputs.total_units)[1]
    assert sum(value for _, value in projection.instruments) == projection.total
    assert sum(value for _, value in projection.attributions) == projection.total


def test_dimensions_cannot_stitch_together_different_faithful_totals() -> None:
    """Require the same candidate in both ownership dimensions.

    Given: Instruments represent only the nearest and attribution only the farther total,
    When: Recovery searches both candidates,
    Then: It refuses instead of combining inconsistent public totals.
    """
    inputs = replace(_class_w(), attributions=_only_far_total_dimension())
    totals = faithful_roundings(inputs.total_units)
    instrument_totals = {
        sum(values)
        for values in product(*(faithful_roundings(units) for _, units in inputs.instruments))
    }
    attribution_totals = {
        sum(values)
        for values in product(*(faithful_roundings(units) for _, units in inputs.attributions))
    }
    assert totals[0] in instrument_totals and totals[0] not in attribution_totals
    assert totals[1] in attribution_totals and totals[1] not in instrument_totals
    assert FaithfulFlowRecovery[str, str](FaithfulRecoveryBudget()).recover(inputs) is None


def test_maximum_key_and_branch_counts_remain_bounded_and_reusable() -> None:
    """Exercise the admitted 64-key and 4096-combination boundary.

    Given: Twelve binary choices per dimension and 64 total keys,
    When: The valid tuple is reached after 196672 candidate-key visits,
    Then: It publishes at that exact allowance and remains a free terminal hit.
    """
    original = _class_w()
    dust = tuple((f"dust-{i}", exact_units_of(1.0) + 1) for i in range(9))
    zeros = tuple((f"zero-{i}", 0) for i in range(20))
    total = original.total_units + sum(units for _, units in dust)
    inputs = FaithfulFlowInput(
        total, (*original.instruments, *dust, *zeros), (*original.attributions, *dust, *zeros)
    )
    budget = FaithfulRecoveryBudget(196_672)
    recovery = FaithfulFlowRecovery[str, str](budget)
    projection = recovery.recover(inputs)
    assert projection is not None
    assert budget.remaining_key_visits == 0
    assert recovery.recover(inputs) is projection
    assert budget.remaining_key_visits == 0


def test_completed_refusal_distinguishes_exhaustive_failure_from_aborts() -> None:
    """Report completed refusal only after an exhaustive search returned ``None``.

    Given: Completed impossible, completed success, budget-aborted, branch-capped,
        and key-capped inputs.
    When: ``completed_refusal`` is queried after ``recover``.
    Then: Only the exhaustive impossible input (and its cached replay) returns ``True``.
    """
    impossible = _class_i()
    full_recovery = FaithfulFlowRecovery[str, str](FaithfulRecoveryBudget())
    assert full_recovery.recover(impossible) is None
    assert full_recovery.completed_refusal(impossible) is True
    assert full_recovery.recover(replace(impossible)) is None
    assert full_recovery.completed_refusal(impossible) is True

    success_recovery = FaithfulFlowRecovery[str, str](FaithfulRecoveryBudget())
    assert success_recovery.recover(_class_w()) is not None
    assert success_recovery.completed_refusal(_class_w()) is False

    aborted_recovery = FaithfulFlowRecovery[str, str](FaithfulRecoveryBudget(0))
    assert aborted_recovery.recover(impossible) is None
    assert aborted_recovery.completed_refusal(impossible) is False

    units = exact_units_of(1.0) + 1
    branch_capped = FaithfulFlowInput(
        13 * units,
        tuple((f"i-{idx}", units) for idx in range(13)),
        (("a0", 13 * units),),
    )
    assert full_recovery.recover(branch_capped) is None
    assert full_recovery.completed_refusal(branch_capped) is False
