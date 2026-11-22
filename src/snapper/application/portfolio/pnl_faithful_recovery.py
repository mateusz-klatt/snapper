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
from itertools import product
from typing import Final

from snapper.application.portfolio.exact_sum import faithful_roundings
from snapper.application.portfolio.exact_sum import project_units

MAX_RECOVERY_KEYS: Final[int] = 64
MAX_RECOVERY_BRANCH_KEYS: Final[int] = 12
MAX_RECOVERY_KEY_VISITS: Final[int] = 262_144


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


@dataclass
class FaithfulFlowRecovery[InstrumentKeyT: Hashable, AttributionKeyT: Hashable]:
    """One family's last completed input/result, sharing its request allowance.

    A completed failure is reusable. An interrupted or structurally ineligible
    search never replaces this entry; callers retain their saved legacy reason.
    Immutable values, rather than live accumulator identities, define reuse.
    """

    budget: FaithfulRecoveryBudget
    _previous_input: FaithfulFlowInput[InstrumentKeyT, AttributionKeyT] | None = None
    _previous_projection: FaithfulFlowProjection[InstrumentKeyT, AttributionKeyT] | None = None

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

    def completed_refusal(
        self, request: FaithfulFlowInput[InstrumentKeyT, AttributionKeyT]
    ) -> bool:
        """Report whether ``request`` completed recovery with an impossible result.

        Args:
            request: Complete ordered exact inputs most recently evaluated.

        Returns:
            ``True`` only when the exact input completed full faithful search or
            reused a completed refusal, never on budget or structural aborts.
        """
        return self._previous_input == request and self._previous_projection is None
