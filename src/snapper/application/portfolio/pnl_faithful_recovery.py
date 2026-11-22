"""Bounded faithful flow recovery with request-owned budget and terminal reuse.

The caller exhausts its historical publication search first. This module only
offers exact bracketing choices, never moves an exact bucket, and accepts the
actual ordered builtin sum in both dimensions against one faithful total.
Budget exhaustion is an unfinished search, not proof of unrepresentability.
"""

import math
from collections.abc import Callable
from collections.abc import Hashable
from dataclasses import dataclass
from dataclasses import field
from itertools import product
from typing import Final

from snapper.application.portfolio.exact_sum import exact_units_of
from snapper.application.portfolio.exact_sum import faithful_roundings
from snapper.application.portfolio.exact_sum import project_units

MAX_RECOVERY_KEYS: Final[int] = 64
MAX_RECOVERY_BRANCH_KEYS: Final[int] = 12
MAX_RECOVERY_KEY_VISITS: Final[int] = 262_144
MAX_OVERFLOW_STEPS: Final[int] = 512
MAX_OVERFLOW_NODES: Final[int] = 8_192


@dataclass(frozen=True)
class FaithfulFlowInput[InstrumentKeyT: Hashable, AttributionKeyT: Hashable]:
    """Immutable exact units and complete ordered keys, including zero buckets."""

    total_units: int
    instruments: tuple[tuple[InstrumentKeyT, int], ...]
    attributions: tuple[tuple[AttributionKeyT, int], ...]


@dataclass(frozen=True)
class FaithfulFlowProjection[InstrumentKeyT: Hashable, AttributionKeyT: Hashable]:
    """Immutable ownership-preserving choices sharing one faithfully rounded total."""

    total: float
    instruments: tuple[tuple[InstrumentKeyT, float], ...]
    attributions: tuple[tuple[AttributionKeyT, float], ...]


@dataclass
class FaithfulRecoveryBudget:
    """A shared deterministic allowance across all flow families.

    Candidate tuples are streamed. Each actual candidate prepays its complete
    ordered key scan, so their total visits cannot exceed this allowance.
    Tuple construction has the same bounded linear cost as those scans.
    Signature validation and terminal-result reuse are separately O(K), with
    K bounded by MAX_RECOVERY_KEYS; no historical cache grows with the window.
    A replay point can also prepay on its containing chunk via ``reserve_shared``.
    """

    remaining_key_visits: int = MAX_RECOVERY_KEY_VISITS
    reserve_shared: Callable[[int], None] | None = None

    def __post_init__(self) -> None:
        """Keep every owned request allowance inside the admitted upper bound."""
        if not 0 <= self.remaining_key_visits <= MAX_RECOVERY_KEY_VISITS:
            raise ValueError("faithful recovery allowance is outside its request bound")

    def reserve(self, key_visits: int) -> bool:
        """Prepay one complete candidate or retain the allowance unchanged.

        Args:
            key_visits: Number of ordered bucket values this candidate inspects.

        Returns:
            Whether the complete scan fits, including an exact boundary match.

        Raises:
            ValueError: When an internal caller requests a negative scan cost.
        """
        if key_visits < 0:
            raise ValueError("faithful recovery scan cost cannot be negative")
        if key_visits > self.remaining_key_visits:
            return False
        if self.reserve_shared is not None:
            self.reserve_shared(key_visits)
        self.remaining_key_visits -= key_visits
        return True


class _RecoveryBudgetExhaustedError(Exception):
    """An interrupted search must return the legacy reason without caching."""


def _valid_dimension[KeyT: Hashable](
    keyed_units: tuple[tuple[KeyT, int], ...], total_units: int
) -> bool:
    """Require distinct ownership keys, exact conservation and finite buckets."""
    return (
        len({key for key, _ in keyed_units}) == len(keyed_units)
        and sum(units for _, units in keyed_units) == total_units
        and all(math.isfinite(project_units(units)) for _, units in keyed_units)
    )


def _valid_input[InstrumentKeyT: Hashable, AttributionKeyT: Hashable](
    request: FaithfulFlowInput[InstrumentKeyT, AttributionKeyT],
) -> bool:
    """Bound signature work before checking trust and exact conservation."""
    return (
        len(request.instruments) + len(request.attributions) <= MAX_RECOVERY_KEYS
        and math.isfinite(project_units(request.total_units))
        and _valid_dimension(request.instruments, request.total_units)
        and _valid_dimension(request.attributions, request.total_units)
    )


def _choices[KeyT: Hashable](
    keyed_units: tuple[tuple[KeyT, int], ...],
) -> tuple[tuple[float, ...], ...] | None:
    """Admit at most twelve binary choices, or 4096 streamed combinations.

    The exact accumulator supplies the finite bracketing values, including its
    asymmetric endpoint treatment. No approximate rounding interval is used.
    Exact and zero buckets have one choice and cannot be moved.
    """
    choices = tuple(faithful_roundings(units) for _, units in keyed_units)
    if sum(len(values) == 2 for values in choices) > MAX_RECOVERY_BRANCH_KEYS:
        return None
    return choices


def _project_dimension[KeyT: Hashable](
    keyed_units: tuple[tuple[KeyT, int], ...],
    choices: tuple[tuple[float, ...], ...],
    total: float,
    budget: FaithfulRecoveryBudget,
) -> tuple[tuple[KeyT, float], ...] | None:
    """Stream nearest-first faithful tuples and accept the actual finite sum."""
    for values in product(*choices):
        if not budget.reserve(len(keyed_units)):
            raise _RecoveryBudgetExhaustedError
        observed = sum(values)
        if math.isfinite(observed) and observed == total:
            return tuple((key, value) for (key, _), value in zip(keyed_units, values, strict=True))
    return None


def _search[InstrumentKeyT: Hashable, AttributionKeyT: Hashable](
    request: FaithfulFlowInput[InstrumentKeyT, AttributionKeyT],
    choices: tuple[tuple[tuple[float, ...], ...], tuple[tuple[float, ...], ...]],
    budget: FaithfulRecoveryBudget,
) -> FaithfulFlowProjection[InstrumentKeyT, AttributionKeyT] | None:
    """Keep both dimensions private until they reach the same faithful total."""
    for total in faithful_roundings(request.total_units):
        instruments = _project_dimension(request.instruments, choices[0], total, budget)
        if instruments is None:
            continue
        attributions = _project_dimension(request.attributions, choices[1], total, budget)
        if attributions is not None:
            return FaithfulFlowProjection(total, instruments, attributions)
    return None


def exceeds_branch_limit[KeyT: Hashable](keyed_units: tuple[tuple[KeyT, int], ...]) -> bool:
    """Return whether a dimension exceeds the Cartesian brute-force branch cap.

    Args:
        keyed_units: Ordered key-and-exact-unit pairs for one flow dimension.

    Returns:
        Whether more than ``MAX_RECOVERY_BRANCH_KEYS`` buckets have two faithful
        roundings.
    """
    return (
        sum(len(faithful_roundings(units)) == 2 for _, units in keyed_units)
        > MAX_RECOVERY_BRANCH_KEYS
    )


@dataclass(frozen=True, slots=True)
class _OverflowBranch:
    """One binary faithful-rounding flip and its exact scaled unit delta."""

    index: int
    alternate: float
    delta_units: int


@dataclass(frozen=True, slots=True)
class _OverflowBounds:
    """Precomputed suffix reachability and unit window for bounded search."""

    target_units: int
    low_units: int
    high_units: int
    rem_pos: tuple[int, ...]
    rem_neg: tuple[int, ...]
    suffix_gcd: tuple[int, ...]


def _overflow_branches[KeyT: Hashable](
    keyed_units: tuple[tuple[KeyT, int], ...],
) -> tuple[list[float], list[_OverflowBranch], int, int]:
    """Build nearest values, descending-magnitude binary flips, and unit stats."""
    base_values: list[float] = []
    branches: list[_OverflowBranch] = []
    base_exact = 0
    max_abs = 0
    for idx, (_, units) in enumerate(keyed_units):
        choices = faithful_roundings(units)
        base = choices[0]
        base_values.append(base)
        unit_count = exact_units_of(base)
        base_exact += unit_count
        max_abs = max(max_abs, abs(unit_count))
        if len(choices) == 2:
            alt_units = exact_units_of(choices[1])
            max_abs = max(max_abs, abs(alt_units))
            branches.append(_OverflowBranch(idx, choices[1], alt_units - unit_count))
    branches.sort(key=lambda branch: (-abs(branch.delta_units), branch.index))
    return base_values, branches, base_exact, max_abs


def _overflow_bounds(
    branches: list[_OverflowBranch],
    total: float,
    max_abs: int,
) -> _OverflowBounds:
    """Precompute exact rounding window and suffix pruning bounds.

    Python's builtin ``sum`` uses compensated summation whose worst-case error
    across up to ``MAX_RECOVERY_KEYS`` (64) finite doubles is bounded by half an
    ULP of the target plus ``max_abs >> 88`` (conservative bound on
    ``n**3 * 2**-106 * max_abs`` for ``n <= 64``), plus one ULP slack for
    boundary rounding.
    """
    count = len(branches)
    rem_pos = [0] * (count + 1)
    rem_neg = [0] * (count + 1)
    suffix_gcd = [0] * (count + 1)
    for idx in range(count - 1, -1, -1):
        delta = branches[idx].delta_units
        rem_pos[idx] = rem_pos[idx + 1] + (delta if delta > 0 else 0)
        rem_neg[idx] = rem_neg[idx + 1] + (delta if delta < 0 else 0)
        suffix_gcd[idx] = math.gcd(abs(delta), suffix_gcd[idx + 1])
    target_units = exact_units_of(total)
    prev_total = math.nextafter(total, -math.inf)
    next_total = math.nextafter(total, math.inf)
    next_ulp = abs(exact_units_of(next_total) - target_units) if math.isfinite(next_total) else 0
    low_ulp = (
        abs(target_units - exact_units_of(prev_total)) if math.isfinite(prev_total) else next_ulp
    )
    high_ulp = next_ulp if math.isfinite(next_total) else low_ulp
    slack = max(low_ulp, high_ulp, max_abs >> 88)
    return _OverflowBounds(
        target_units=target_units,
        low_units=target_units - (low_ulp // 2) - slack,
        high_units=target_units + (high_ulp // 2) + slack,
        rem_pos=tuple(rem_pos),
        rem_neg=tuple(rem_neg),
        suffix_gcd=tuple(suffix_gcd),
    )


@dataclass(slots=True)
class _BoundedDimensionSearch[KeyT: Hashable]:
    """Stateful depth-first exact-unit search over binary faithful choices."""

    keyed_units: tuple[tuple[KeyT, int], ...]
    base_values: list[float]
    branches: list[_OverflowBranch]
    bounds: _OverflowBounds
    total: float
    budget: FaithfulRecoveryBudget
    values: list[float]
    steps: int = 0
    nodes: int = 0
    exhausted: bool = False

    def _pruned(self, depth: int, current_units: int) -> bool:
        """Return whether remaining flips cannot reach the target unit window."""
        if (
            current_units + self.bounds.rem_pos[depth] < self.bounds.low_units
            or current_units + self.bounds.rem_neg[depth] > self.bounds.high_units
        ):
            return True
        modulus = self.bounds.suffix_gcd[depth]
        if modulus == 0:
            return False
        diff_low = self.bounds.low_units - current_units
        first_multiple = ((diff_low + modulus - 1) // modulus) * modulus
        return first_multiple > self.bounds.high_units - current_units

    def _evaluate_leaf(self, dirty: bool) -> tuple[tuple[KeyT, float], ...] | None:
        """Charge one candidate scan and verify its ordered builtin sum."""
        if not dirty:
            return None
        if not self.budget.reserve(len(self.keyed_units)):
            raise _RecoveryBudgetExhaustedError
        self.steps += 1
        observed = sum(self.values)
        if math.isfinite(observed) and observed == self.total:
            return tuple(
                (key, value) for (key, _), value in zip(self.keyed_units, self.values, strict=True)
            )
        return None

    def search(
        self, depth: int, current_units: int, dirty: bool
    ) -> tuple[tuple[KeyT, float], ...] | None:
        """Explore closer-to-target faithful roundings within the step and node limits."""
        if self.steps >= MAX_OVERFLOW_STEPS or self.nodes >= MAX_OVERFLOW_NODES:
            self.exhausted = True
            return None
        if not self.budget.reserve(1):
            raise _RecoveryBudgetExhaustedError
        self.nodes += 1
        if self._pruned(depth, current_units):
            return None
        if depth == len(self.branches):
            return self._evaluate_leaf(dirty)
        branch = self.branches[depth]
        base = self.base_values[branch.index]
        flipped_units = current_units + branch.delta_units
        if abs(flipped_units - self.bounds.target_units) < abs(
            current_units - self.bounds.target_units
        ):
            candidates = ((branch.alternate, flipped_units, True), (base, current_units, dirty))
        else:
            candidates = ((base, current_units, dirty), (branch.alternate, flipped_units, True))
        for value, next_units, next_dirty in candidates:
            self.values[branch.index] = value
            matched = self.search(depth + 1, next_units, next_dirty)
            if matched is not None:
                return matched
        self.values[branch.index] = base
        return None


def _project_dimension_bounded[KeyT: Hashable](
    keyed_units: tuple[tuple[KeyT, int], ...],
    total: float,
    budget: FaithfulRecoveryBudget,
) -> tuple[tuple[tuple[KeyT, float], ...] | None, bool]:
    """Find one faithful dimension tuple via exact-unit branch-and-bound."""
    base_values, branches, base_exact, max_abs = _overflow_branches(keyed_units)
    if not budget.reserve(len(keyed_units)):
        raise _RecoveryBudgetExhaustedError
    observed = sum(base_values)
    if math.isfinite(observed) and observed == total:
        return (
            tuple((key, value) for (key, _), value in zip(keyed_units, base_values, strict=True)),
            True,
        )
    bounds = _overflow_bounds(branches, total, max_abs)
    runner = _BoundedDimensionSearch(
        keyed_units=keyed_units,
        base_values=base_values,
        branches=branches,
        bounds=bounds,
        total=total,
        budget=budget,
        values=list(base_values),
    )
    matched = runner.search(0, base_exact, False)
    return matched, not runner.exhausted


def _project_overflow_dimension[KeyT: Hashable](
    keyed_units: tuple[tuple[KeyT, int], ...],
    choices: tuple[tuple[float, ...], ...] | None,
    total: float,
    budget: FaithfulRecoveryBudget,
) -> tuple[tuple[tuple[KeyT, float], ...] | None, bool]:
    """Use Cartesian search up to the branch cap and bounded DFS above it."""
    if choices is not None:
        return _project_dimension(keyed_units, choices, total, budget), True
    return _project_dimension_bounded(keyed_units, total, budget)


def _search_overflow[InstrumentKeyT: Hashable, AttributionKeyT: Hashable](
    request: FaithfulFlowInput[InstrumentKeyT, AttributionKeyT],
    budget: FaithfulRecoveryBudget,
) -> tuple[FaithfulFlowProjection[InstrumentKeyT, AttributionKeyT] | None, bool]:
    """Search overflow dimensions per-dimension for a shared faithful total."""
    instrument_choices = _choices(request.instruments)
    attribution_choices = _choices(request.attributions)
    completed = True
    for total in faithful_roundings(request.total_units):
        instruments, inst_complete = _project_overflow_dimension(
            request.instruments, instrument_choices, total, budget
        )
        completed = completed and inst_complete
        if instruments is None:
            continue
        attributions, attr_complete = _project_overflow_dimension(
            request.attributions, attribution_choices, total, budget
        )
        completed = completed and attr_complete
        if attributions is not None:
            return FaithfulFlowProjection(total, instruments, attributions), True
    return None, completed


@dataclass
class FaithfulFlowRecovery[InstrumentKeyT: Hashable, AttributionKeyT: Hashable]:
    """One family's last completed input/result, sharing its request allowance.

    A completed failure is reusable. An interrupted or structurally ineligible
    search never replaces this entry; callers retain their saved legacy reason.
    Overflow recovery owns an isolated allowance and separate terminal/cap cache
    entries so overflow attempts never starve or pollute ``recover()``.
    Immutable values, rather than live accumulator identities, define reuse.
    """

    budget: FaithfulRecoveryBudget
    overflow_budget: FaithfulRecoveryBudget = field(default_factory=FaithfulRecoveryBudget)
    _previous_input: FaithfulFlowInput[InstrumentKeyT, AttributionKeyT] | None = None
    _previous_projection: FaithfulFlowProjection[InstrumentKeyT, AttributionKeyT] | None = None
    _previous_overflow_input: FaithfulFlowInput[InstrumentKeyT, AttributionKeyT] | None = None
    _previous_overflow_projection: (
        FaithfulFlowProjection[InstrumentKeyT, AttributionKeyT] | None
    ) = None
    _previous_capped_input: FaithfulFlowInput[InstrumentKeyT, AttributionKeyT] | None = None

    def recover(
        self, request: FaithfulFlowInput[InstrumentKeyT, AttributionKeyT]
    ) -> FaithfulFlowProjection[InstrumentKeyT, AttributionKeyT] | None:
        """Recover a bounded split or leave publication to the caller's refusal.

        Args:
            request: Complete ordered exact inputs for the already finite family.

        Returns:
            A coherent faithful projection, or no new publishable result.
            A cache hit spends no enumeration visits, even after exhaustion.
        """
        if not _valid_input(request):
            return None
        if request == self._previous_input:
            return self._previous_projection
        instruments = _choices(request.instruments)
        attributions = _choices(request.attributions)
        if instruments is None or attributions is None:
            return None
        if self.budget.remaining_key_visits < len(request.instruments) + len(request.attributions):
            return None
        try:
            projection = _search(request, (instruments, attributions), self.budget)
        except _RecoveryBudgetExhaustedError:
            return None
        self._previous_input = request
        self._previous_projection = projection
        return projection

    def recover_overflow(
        self, request: FaithfulFlowInput[InstrumentKeyT, AttributionKeyT]
    ) -> FaithfulFlowProjection[InstrumentKeyT, AttributionKeyT] | None:
        """Recover a faithful split when binary choices exceed the brute-force cap.

        Args:
            request: Complete ordered exact inputs for the already finite family.

        Returns:
            A coherent faithful projection, or ``None`` when no bounded split is
            found. Completed and deterministic cap outcomes cache independently of
            ``recover()`` at zero repeat cost; budget-interrupted searches remain
            nonterminal and leave cached state unchanged.
        """
        if not _valid_input(request):
            return None
        if request == self._previous_overflow_input:
            return self._previous_overflow_projection
        if request == self._previous_capped_input:
            return None
        if self.overflow_budget.remaining_key_visits < len(request.instruments) + len(
            request.attributions
        ):
            return None
        try:
            projection, completed = _search_overflow(request, self.overflow_budget)
        except _RecoveryBudgetExhaustedError:
            return None
        if completed:
            self._previous_overflow_input = request
            self._previous_overflow_projection = projection
        else:
            self._previous_capped_input = request
        return projection
