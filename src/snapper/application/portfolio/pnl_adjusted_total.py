"""Bounded adjusted-total flow publication fallback for public P&L reads.

When both historical single-absorber passes and bounded multi-bucket faithful
recovery complete without finding a common faithful total across disjoint
converted lattices, a public read may publish an adjusted total within tight
bounds measured in ULPs of the exact aggregate total.

The coarser dimension fixes its faithful bucket choices and proposes distinct
builtin ``sum()`` candidate totals within ``K_TOTAL`` ULPs of the exact total.
The finer dimension assigns one nonzero inexact bucket as its single absorber,
picks nearest-first faithful combinations for the remaining buckets, and scans
around ``cand - sum(others)`` by up to four ``nextafter`` steps until the
key-ordered ``sum()`` equals the candidate identically within ``K_ABSORBER``
ULPs. Exact and zero buckets are never moved.
"""

import math
from collections.abc import Hashable
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from itertools import product
from typing import Final
from typing import Literal

from snapper.application.portfolio.exact_sum import exact_units_of
from snapper.application.portfolio.exact_sum import faithful_roundings
from snapper.application.portfolio.exact_sum import project_units

C_MAX: Final[int] = 8
K_ABSORBER: Final[int] = 4
K_TOTAL: Final[int] = 8
_ABSORBER_SCAN_STEPS: Final[int] = 4

type AbsorberDimension = Literal["instrument", "attribution"]


@dataclass(frozen=True)
class AdjustedFlowProjection[InstrumentKeyT: Hashable, AttributionKeyT: Hashable]:
    """Immutable adjusted-total flow projection with ULP deviation disclosure.

    Attributes:
        total: Published aggregate flow total matching both dimensions' sums.
        instruments: Key-ordered instrument bucket values summing to ``total``.
        attributions: Key-ordered attribution bucket values summing to ``total``.
        total_deviation_ulps: Magnitude of ``total - exact_total`` in ``u``.
        absorber_deviation_ulps: Magnitude of ``absorber - exact_bucket`` in ``u``.
        absorber_key: Key of the single absorber bucket in the finer dimension.
        absorber_dimension: Dimension owning ``absorber_key``.
    """

    total: float
    instruments: Mapping[InstrumentKeyT, float]
    attributions: Mapping[AttributionKeyT, float]
    total_deviation_ulps: float
    absorber_deviation_ulps: float
    absorber_key: InstrumentKeyT | AttributionKeyT
    absorber_dimension: AbsorberDimension


def _valid_dimension[KeyT: Hashable](
    buckets: Sequence[tuple[KeyT, int]],
    total_units: int,
) -> bool:
    """Require distinct keys, exact conservation, and finite bucket projections.

    Args:
        buckets: Keyed exact unit counts for one dimension.
        total_units: Exact aggregate units that the dimension must conserve.

    Returns:
        Whether the dimension is structurally valid for adjusted publication.
    """
    return (
        len({key for key, _ in buckets}) == len(buckets)
        and sum(units for _, units in buckets) == total_units
        and all(math.isfinite(project_units(units)) for _, units in buckets)
    )


def _min_nonzero_ulp[KeyT: Hashable](buckets: Sequence[tuple[KeyT, int]]) -> float | None:
    """Return the minimum ULP across nonzero buckets, or ``None`` when all are zero.

    Args:
        buckets: Keyed exact unit counts for one dimension.

    Returns:
        Smallest ``math.ulp(project_units(units))`` for nonzero buckets, or ``None``.
    """
    ulps = [math.ulp(project_units(units)) for _, units in buckets if units != 0]
    return min(ulps) if ulps else None


def _coarser_candidates[KeyT: Hashable](
    buckets: Sequence[tuple[KeyT, int]],
    total_units: int,
    max_total_dev_units: int,
) -> list[tuple[float, tuple[float, ...]]]:
    """Enumerate distinct finite coarser totals within the total deviation bound.

    Args:
        buckets: Keyed exact unit counts for the coarser dimension.
        total_units: Exact aggregate unit count.
        max_total_dev_units: Maximum allowed ``|cand - exact_total|`` in exact units.

    Returns:
        Sorted ``(candidate_total, bucket_values)`` pairs within the bound, or an
        empty list when the combination cap or total bound rejects all totals.
    """
    choices = tuple(faithful_roundings(units) for _, units in buckets)
    if sum(len(values) == 2 for values in choices) > C_MAX:
        return []
    by_total: dict[float, tuple[float, ...]] = {}
    for combo in product(*choices):
        candidate = sum(combo)
        if math.isfinite(candidate) and candidate not in by_total:
            by_total[candidate] = combo
    ordered = sorted(
        by_total.items(),
        key=lambda item: (abs(exact_units_of(item[0]) - total_units), item[0]),
    )
    return [
        (candidate, combo)
        for candidate, combo in ordered
        if abs(exact_units_of(candidate) - total_units) <= max_total_dev_units
    ]


def _finer_plan[KeyT: Hashable](
    buckets: Sequence[tuple[KeyT, int]],
) -> tuple[int, tuple[tuple[float, ...], ...]] | None:
    """Select the finest-ULP nonzero inexact absorber and bound non-absorbers.

    Args:
        buckets: Keyed exact unit counts for the finer dimension.

    Returns:
        ``(absorber_index, non_absorber_choices)`` when an eligible absorber
        exists within ``2**C_MAX`` non-absorber combinations, otherwise ``None``.
    """
    choices = tuple(faithful_roundings(units) for _, units in buckets)
    candidates = [
        (math.ulp(project_units(units)), index)
        for index, ((_, units), bucket_choices) in enumerate(zip(buckets, choices, strict=True))
        if units != 0 and len(bucket_choices) == 2
    ]
    if not candidates:
        return None
    _, absorber_index = min(candidates)
    non_absorber_choices = tuple(
        bucket_choices for index, bucket_choices in enumerate(choices) if index != absorber_index
    )
    if sum(len(bucket_choices) == 2 for bucket_choices in non_absorber_choices) > C_MAX:
        return None
    return absorber_index, non_absorber_choices


def _absorber_candidates(base: float) -> list[tuple[int, float]]:
    """Return ``base`` and up to four finite ``nextafter`` steps in each direction.

    Args:
        base: Initial absorber float computed from ``cand - sum(others, 0.0)``.

    Returns:
        Ordered ``(step_offset, value)`` pairs with finite doubles only.
    """
    if not math.isfinite(base):
        return []
    candidates: list[tuple[int, float]] = [(0, base)]
    for sign, direction in ((1, math.inf), (-1, -math.inf)):
        current = base
        for step in range(1, _ABSORBER_SCAN_STEPS + 1):
            current = math.nextafter(current, direction)
            if not math.isfinite(current):
                break
            candidates.append((sign * step, current))
    return candidates


def _assemble_finer(
    absorber_index: int,
    absorber_value: float,
    others: tuple[float, ...],
) -> tuple[float, ...]:
    """Place ``absorber_value`` back at ``absorber_index`` in key order.

    Args:
        absorber_index: Original position of the absorber in the finer key list.
        absorber_value: Candidate double for the absorber bucket.
        others: Ordered faithful values for the ``N - 1`` non-absorber buckets.

    Returns:
        Complete key-ordered tuple of finer-dimension doubles.
    """
    return (*others[:absorber_index], absorber_value, *others[absorber_index:])


def _best_absorber_assignment(
    cand: float,
    absorber_index: int,
    absorber_exact_units: int,
    others: tuple[float, ...],
    max_absorber_dev_units: int,
) -> tuple[tuple[float, ...], int] | None:
    """Find the closest valid absorber within four ``nextafter`` steps of ``base``.

    Args:
        cand: Target candidate aggregate float from the coarser dimension.
        absorber_index: Position of the absorber bucket in key order.
        absorber_exact_units: Exact scaled integer value of the absorber bucket.
        others: Faithful non-absorber bucket values for this combination.
        max_absorber_dev_units: Maximum allowed ``|absorber - exact|`` in units.

    Returns:
        ``(finer_values, dev_units)`` with minimal exact absorber deviation, or
        ``None`` when no candidate satisfies exact summation and the deviation bound.
    """
    base = cand - sum(others, 0.0)
    best: tuple[tuple[int, int, float], tuple[float, ...], int] | None = None
    for step, absorber_value in _absorber_candidates(base):
        trial = _assemble_finer(absorber_index, absorber_value, others)
        if sum(trial) != cand:
            continue
        dev_units = abs(exact_units_of(absorber_value) - absorber_exact_units)
        if dev_units > max_absorber_dev_units:
            continue
        rank = (dev_units, abs(step), absorber_value)
        if best is None or rank < best[0]:
            best = (rank, trial, dev_units)
    if best is None:
        return None
    return best[1], best[2]


def _solve_finer_dimension[KeyT: Hashable](
    cand: float,
    finer_buckets: Sequence[tuple[KeyT, int]],
    finer_plan: tuple[int, tuple[tuple[float, ...], ...]],
    max_absorber_dev_units: int,
) -> tuple[tuple[float, ...], KeyT, int] | None:
    """Solve the finer dimension for ``cand`` over nearest-first non-absorbers.

    Args:
        cand: Candidate aggregate float from the coarser dimension.
        finer_buckets: Key-ordered ``(key, exact_units)`` pairs for the finer dimension.
        finer_plan: Preselected absorber index and non-absorber faithful choices.
        max_absorber_dev_units: Maximum allowed absorber deviation in exact units.

    Returns:
        ``(finer_values, absorber_key, absorber_dev_units)`` or ``None`` if no
        combination fits within the absorber bound.
    """
    absorber_index, non_absorber_choices = finer_plan
    absorber_key, absorber_exact_units = finer_buckets[absorber_index]
    for others in product(*non_absorber_choices):
        solved = _best_absorber_assignment(
            cand,
            absorber_index,
            absorber_exact_units,
            others,
            max_absorber_dev_units,
        )
        if solved is not None:
            values, dev_units = solved
            return values, absorber_key, dev_units
    return None


def _solve_ordered[CoarserKeyT: Hashable, FinerKeyT: Hashable](
    total_units: int,
    coarser: Sequence[tuple[CoarserKeyT, int]],
    finer: Sequence[tuple[FinerKeyT, int]],
) -> (
    tuple[
        float,
        dict[CoarserKeyT, float],
        dict[FinerKeyT, float],
        float,
        float,
        FinerKeyT,
    ]
    | None
):
    """Solve adjusted total and bucket maps for fixed coarser and finer dimensions.

    Args:
        total_units: Exact aggregate unit count.
        coarser: Key-ordered ``(key, exact_units)`` pairs for the coarser dimension.
        finer: Key-ordered ``(key, exact_units)`` pairs for the finer dimension.

    Returns:
        ``(total, coarser_map, finer_map, total_dev_ulps, absorber_dev_ulps, absorber_key)``
        or ``None`` when bounds or caps refuse every candidate.
    """
    u_units = exact_units_of(math.ulp(project_units(total_units)))
    candidates = _coarser_candidates(coarser, total_units, K_TOTAL * u_units)
    if not candidates:
        return None
    finer_plan = _finer_plan(finer)
    if finer_plan is None:
        return None
    max_absorber_dev_units = K_ABSORBER * u_units
    for cand, coarser_values in candidates:
        solved = _solve_finer_dimension(cand, finer, finer_plan, max_absorber_dev_units)
        if solved is None:
            continue
        finer_values, absorber_key, absorber_dev_units = solved
        coarser_map = {key: value for (key, _), value in zip(coarser, coarser_values, strict=True)}
        finer_map = {key: value for (key, _), value in zip(finer, finer_values, strict=True)}
        total_dev_ulps = abs(exact_units_of(cand) - total_units) / u_units
        absorber_dev_ulps = absorber_dev_units / u_units
        return (
            cand,
            coarser_map,
            finer_map,
            total_dev_ulps,
            absorber_dev_ulps,
            absorber_key,
        )
    return None


def adjust_flow_family[InstrumentKeyT: Hashable, AttributionKeyT: Hashable](
    total_units: int,
    instruments: Sequence[tuple[InstrumentKeyT, int]],
    attributions: Sequence[tuple[AttributionKeyT, int]],
) -> AdjustedFlowProjection[InstrumentKeyT, AttributionKeyT] | None:
    """Compute a bounded adjusted-total flow projection across both dimensions.

    Args:
        total_units: Exact aggregate unit count on the ``2**-1074`` grid.
        instruments: Key-ordered instrument ``(key, exact_units)`` pairs.
        attributions: Key-ordered attribution ``(key, exact_units)`` pairs.

    Returns:
        An :class:`AdjustedFlowProjection` satisfying exact ``sum() == total``
        in both dimensions within ``K_TOTAL`` and ``K_ABSORBER`` bounds, or
        ``None`` when structural checks, combination caps, or bounds fail.
    """
    if (
        not math.isfinite(project_units(total_units))
        or not _valid_dimension(instruments, total_units)
        or not _valid_dimension(attributions, total_units)
    ):
        return None
    inst_ulp = _min_nonzero_ulp(instruments)
    attr_ulp = _min_nonzero_ulp(attributions)
    if inst_ulp is None or attr_ulp is None:
        return None
    if attr_ulp >= inst_ulp:
        solved_inst = _solve_ordered(total_units, attributions, instruments)
        if solved_inst is None:
            return None
        total, attr_map, inst_map, total_dev, absorber_dev, inst_absorber = solved_inst
        return AdjustedFlowProjection(
            total=total,
            instruments=inst_map,
            attributions=attr_map,
            total_deviation_ulps=total_dev,
            absorber_deviation_ulps=absorber_dev,
            absorber_key=inst_absorber,
            absorber_dimension="instrument",
        )
    solved_attr = _solve_ordered(total_units, instruments, attributions)
    if solved_attr is None:
        return None
    total, inst_map, attr_map, total_dev, absorber_dev, attr_absorber = solved_attr
    return AdjustedFlowProjection(
        total=total,
        instruments=inst_map,
        attributions=attr_map,
        total_deviation_ulps=total_dev,
        absorber_deviation_ulps=absorber_dev,
        absorber_key=attr_absorber,
        absorber_dimension="attribution",
    )
