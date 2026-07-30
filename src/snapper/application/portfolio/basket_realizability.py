"""Pure venue-minimum realizability proof for observed balance baskets.

The rule proves one narrow claim: a strictly positive crypto balance is below
the smallest order size published for every active, certified, tradable spot
pair where that asset is the base. Identity ambiguity, quote-side utility,
missing certification, an escaped tradable sibling, or any recorded change to a
non-null minimum all resolve UNKNOWN and keep the balance.

This fail-closed polarity intentionally does not follow
``guard_scanner._flatten_quantity``. There, a null minimum permits a
risk-reducing flatten; here, treating null as unconstrained would remove money
from a published equity figure. This module therefore performs no I/O and never
creates a P&L sample reason code. Its output can only remove a proven
sub-minimum leg or preserve today's basket unchanged.
"""

from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Literal
from typing import TypeIs

from snapper.core.numeric import is_positive_finite

type MinimumRefusal = Literal[
    "not_positive",
    "valuation_currency",
    "identity_ambiguous",
    "not_crypto",
    "is_quote_currency",
    "no_certified_pair",
    "uncertified_tradable_pair",
    "minimum_unstable",
    "at_or_above_minimum",
]
"""Disclosure reason why a balance was not proven below a venue minimum."""

type RuleWithheldCause = Literal[
    "too_many_excluded_legs",
    "would_empty_basket",
]
"""Whole-basket guard that withheld every proposed exclusion."""

_MAX_EXCLUDED_BALANCE_LEGS = 4


@dataclass(frozen=True)
class VenueOrderMinimumVersion:
    """Pure temporal mirror of one repository minimum-evidence row."""

    exchange: str
    currency: str
    role: Literal["base", "quote"]
    asset_type: str
    instrument_public_id: str
    symbol_public_id: str
    native_symbol: str
    counter_currency: str | None
    instrument_kind: str | None
    quantity_unit: str | None
    status: str | None
    spec_public_id: str | None
    spec_source: str | None
    spec_version: str | None
    spec_observed_at: datetime | None
    min_order_size: float | None
    can_trade: bool | None
    identity_conflicted: bool
    minimum_unstable: bool
    valid_from: datetime
    valid_to: datetime


@dataclass(frozen=True)
class MinimumResolution:
    """Resolved threshold and the exact pair that supplied its minimum."""

    threshold: float | None
    refusal: MinimumRefusal | None
    pairs_considered: int
    argmin_instrument_public_id: str | None
    argmin_symbol_public_id: str | None
    argmin_native_symbol: str | None
    argmin_spec_public_id: str | None
    argmin_spec_source: str | None
    argmin_spec_version: str | None
    argmin_spec_observed_at: datetime | None


@dataclass(frozen=True)
class ExcludedBalance:
    """One balance proven strictly below its venue's spot order minimum."""

    exchange: str
    currency: str
    quantity: float
    min_order_size: float
    pairs_considered: int
    instrument_public_id: str | None
    symbol_public_id: str | None
    native_symbol: str | None
    spec_public_id: str | None
    spec_source: str | None
    spec_version: str | None
    spec_observed_at: datetime | None


@dataclass(frozen=True)
class RealizableBasket:
    """Observed basket after proven exclusions and whole-basket safeguards."""

    balances: Mapping[tuple[str, str], float]
    excluded: tuple[ExcludedBalance, ...]
    resolved_legs: int
    unresolved_legs: int
    rule_withheld: bool
    withheld_cause: RuleWithheldCause | None


def _refusal(
    reason: MinimumRefusal,
    pairs_considered: int = 0,
) -> MinimumResolution:
    """Build an unresolved result without fabricating argmin evidence."""
    return MinimumResolution(
        threshold=None,
        refusal=reason,
        pairs_considered=pairs_considered,
        argmin_instrument_public_id=None,
        argmin_symbol_public_id=None,
        argmin_native_symbol=None,
        argmin_spec_public_id=None,
        argmin_spec_source=None,
        argmin_spec_version=None,
        argmin_spec_observed_at=None,
    )


def _could_have_quote_utility(version: VenueOrderMinimumVersion) -> bool:
    """Return whether one quote-side row cannot disprove active spot utility."""
    return version.identity_conflicted or (
        version.can_trade is not False
        and version.instrument_kind in {None, "spot"}
        and version.status in {None, "active"}
    )


def _is_qualifying_base_pair(version: VenueOrderMinimumVersion) -> bool:
    """Return whether one base-side row belongs to predicate set P."""
    return (
        version.can_trade is True
        and version.instrument_kind == "spot"
        and version.quantity_unit == "base_asset"
        and version.status == "active"
        and version.spec_source is not None
        and version.spec_version is not None
        and version.spec_observed_at is not None
        and is_positive_finite(version.min_order_size)
    )


def _resolved_minimum(
    qualifying: Sequence[VenueOrderMinimumVersion],
) -> MinimumResolution:
    """Resolve the deterministic minimum and its complete argmin identity."""
    chosen = min(
        qualifying,
        key=lambda version: (
            version.min_order_size if version.min_order_size is not None else float("inf"),
            version.native_symbol,
            version.instrument_public_id,
            version.symbol_public_id,
            version.spec_public_id or "",
            version.spec_source or "",
            version.spec_version or "",
            version.spec_observed_at,
        ),
    )
    return MinimumResolution(
        threshold=chosen.min_order_size,
        refusal=None,
        pairs_considered=len({version.instrument_public_id for version in qualifying}),
        argmin_instrument_public_id=chosen.instrument_public_id,
        argmin_symbol_public_id=chosen.symbol_public_id,
        argmin_native_symbol=chosen.native_symbol,
        argmin_spec_public_id=chosen.spec_public_id,
        argmin_spec_source=chosen.spec_source,
        argmin_spec_version=chosen.spec_version,
        argmin_spec_observed_at=chosen.spec_observed_at,
    )


def _resolve_one_minimum(
    base_versions: Sequence[VenueOrderMinimumVersion],
    quote_versions: Sequence[VenueOrderMinimumVersion],
) -> MinimumResolution:
    """Apply P3 through P7 to one venue/currency identity."""
    asset_types = {version.asset_type for version in base_versions}
    if any(version.identity_conflicted for version in base_versions) or len(asset_types) != 1:
        return _refusal("identity_ambiguous")
    if next(iter(asset_types)) != "crypto":
        return _refusal("not_crypto")
    if any(_could_have_quote_utility(version) for version in quote_versions):
        return _refusal("is_quote_currency")
    qualifying = [version for version in base_versions if _is_qualifying_base_pair(version)]
    if not qualifying:
        return _refusal("no_certified_pair")
    pair_count = len({version.instrument_public_id for version in qualifying})
    if any(
        version.can_trade is not False and not _is_qualifying_base_pair(version)
        for version in base_versions
    ):
        return _refusal("uncertified_tradable_pair", pair_count)
    if any(version.minimum_unstable for version in qualifying):
        return _refusal("minimum_unstable", pair_count)
    return _resolved_minimum(qualifying)


def resolve_venue_order_minimums(
    versions: Sequence[VenueOrderMinimumVersion],
    minute: datetime,
) -> Mapping[tuple[str, str], MinimumResolution]:
    """Resolve P3-P7 for every base identity in force at one grid minute.

    Quote-role rows never create a resolution by themselves: without any
    base-side instrument there is no asset identity or threshold to prove. They
    only apply P4 to a currency that also has base-side evidence.

    Args:
        versions: Undeduplicated temporal rows loaded for the surrounding window.
        minute: Grid minute selecting ``valid_from <= minute < valid_to`` rows.

    Returns:
        Deterministically ordered resolutions keyed by ``(exchange, currency)``.
    """
    base_by_key: dict[tuple[str, str], list[VenueOrderMinimumVersion]] = {}
    quote_by_key: dict[tuple[str, str], list[VenueOrderMinimumVersion]] = {}
    for version in versions:
        if not (version.valid_from <= minute < version.valid_to):
            continue
        key = (version.exchange, version.currency)
        target = base_by_key if version.role == "base" else quote_by_key
        target.setdefault(key, []).append(version)
    return {
        key: _resolve_one_minimum(
            base_by_key[key],
            quote_by_key.get(key, ()),
        )
        for key in sorted(base_by_key)
    }


def _usable_resolution(
    resolution: MinimumResolution | None,
) -> TypeIs[MinimumResolution]:
    """Return whether one resolution proves a positive finite threshold."""
    return (
        resolution is not None
        and resolution.refusal is None
        and is_positive_finite(resolution.threshold)
    )


def _excluded_balance(
    key: tuple[str, str],
    quantity: float,
    resolution: MinimumResolution,
) -> ExcludedBalance | None:
    """Apply P1, P2, P8 and P9 to one resolved balance leg."""
    exchange, currency = key
    threshold = resolution.threshold
    if not quantity > 0.0 or currency == "USD" or threshold is None or not quantity < threshold:
        return None
    return ExcludedBalance(
        exchange=exchange,
        currency=currency,
        quantity=quantity,
        min_order_size=threshold,
        pairs_considered=resolution.pairs_considered,
        instrument_public_id=resolution.argmin_instrument_public_id,
        symbol_public_id=resolution.argmin_symbol_public_id,
        native_symbol=resolution.argmin_native_symbol,
        spec_public_id=resolution.argmin_spec_public_id,
        spec_source=resolution.argmin_spec_source,
        spec_version=resolution.argmin_spec_version,
        spec_observed_at=resolution.argmin_spec_observed_at,
    )


def _withheld_basket(
    balances: Mapping[tuple[str, str], float],
    resolved_legs: int,
    unresolved_legs: int,
    cause: RuleWithheldCause,
) -> RealizableBasket:
    """Return the untouched observed basket after a Rule-W refusal."""
    return RealizableBasket(
        balances=dict(sorted(balances.items())),
        excluded=(),
        resolved_legs=resolved_legs,
        unresolved_legs=unresolved_legs,
        rule_withheld=True,
        withheld_cause=cause,
    )


def partition_realizable_balances(
    observed_balances: Mapping[tuple[str, str], float],
    resolutions: Mapping[tuple[str, str], MinimumResolution],
) -> RealizableBasket:
    """Apply P1, P2, P8, P9 and Rule W to an observed aggregate basket.

    Counts describe non-zero observed legs. A leg is resolved only when it has a
    positive finite threshold with no refusal; every other non-zero leg is
    unresolved and remains in the basket. More than four proposed exclusions or
    an exclusion that would remove every non-zero leg withholds the whole rule,
    returning the original basket unchanged.

    Args:
        observed_balances: Aggregated balance per ``(exchange, currency)``.
        resolutions: P3-P7 outcomes for the same venue/currency keys.

    Returns:
        Deterministic retained balances, excluded evidence, counts and Rule-W
        disclosure.
    """
    retained = dict(sorted(observed_balances.items()))
    proposed: list[ExcludedBalance] = []
    resolved_legs = 0
    unresolved_legs = 0
    for key, quantity in retained.items():
        if quantity == 0.0:
            continue
        resolution = resolutions.get(key)
        if not _usable_resolution(resolution):
            unresolved_legs += 1
            continue
        resolved_legs += 1
        excluded = _excluded_balance(key, quantity, resolution)
        if excluded is not None:
            proposed.append(excluded)
    proposed.sort(key=lambda item: (item.exchange, item.currency))
    if len(proposed) > _MAX_EXCLUDED_BALANCE_LEGS:
        return _withheld_basket(
            observed_balances,
            resolved_legs,
            unresolved_legs,
            "too_many_excluded_legs",
        )
    for excluded in proposed:
        retained.pop((excluded.exchange, excluded.currency))
    had_non_zero = any(quantity != 0.0 for quantity in observed_balances.values())
    leaves_non_zero = any(quantity != 0.0 for quantity in retained.values())
    if had_non_zero and not leaves_non_zero:
        return _withheld_basket(
            observed_balances,
            resolved_legs,
            unresolved_legs,
            "would_empty_basket",
        )
    return RealizableBasket(
        balances=retained,
        excluded=tuple(proposed),
        resolved_legs=resolved_legs,
        unresolved_legs=unresolved_legs,
        rule_withheld=False,
        withheld_cause=None,
    )
