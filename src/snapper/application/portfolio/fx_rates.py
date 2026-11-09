"""Point-in-time FX resolution from finalized 1m candles (Phase 5A).

The P&L timeline values a scope in ONE currency, but a real venue mixes them: a
walutomat EUR-PLN fill carries its fee in EUR while the position quotes in PLN.
Without conversion such a flow is UNKNOWN, and the engine's finiteness guard then
withholds the whole series — correct, but useless. This module converts those
flows using the same evidence plane the marks come from: our own finalized
one-minute candles.

Scope is deliberately narrow (no FX graph, no external feed):

- **Identity.** A conversion into the same currency is ``1.0`` and needs no
  evidence at all, so a USD fee on a USD-valued series never touches a candle.
- **Pinned plane.** One exact ``(base, quote, exchange)`` candle series is
  selected for each unordered currency pair in one consumer's map. Different
  instruments may carry different maps, but neither a rival venue nor the
  opposite orientation can enter one consumer's later lookup.
- **Quoted direction.** A pinned ``FROM-TO`` plane is used as quoted: one unit
  of FROM costs ``close`` units of TO.
- **Reciprocal direction.** A pinned ``TO-FROM`` plane converts FROM into TO by
  reciprocal. The orientation is fixed before conversion, never selected from
  the rows available at an individual minute.
- **Nothing else.** No triangulation through a third currency, no nearest-minute
  search, no carry-forward of an older rate. A missing pair or a missing minute
  yields ``None``, which the caller turns into an explicitly withheld value. An
  invented rate is worse than an honest gap: it silently misstates money.

The rate for minute ``M`` is the close of the candle covering ``[M-1m, M)`` — the
bar that CLOSED at ``M`` — matching the mark convention exactly, so a fee and the
position it belongs to are valued off the same instant with no look-ahead.

Carried valuation is the one deliberate exception to "nothing else", and it lives
behind :class:`FxCarryBook`. The operator decided on 2026-11-09 that a P&L point
must not be withheld over a missing minute when an earlier rate exists. The book
keeps the plane pin and the orientation rule untouched, serves an exact minute
exactly as :func:`resolve_rate` does, and reaches back only for a minute that has
no exact evidence at all. A minute whose exact evidence is conflicted or invalid
stays withheld, because that minute was observed and contradicted, not missed.
Every carried result names the minute it came from so the caller can disclose it.
"""

import math
from bisect import bisect_left
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timedelta
from typing import Final
from typing import Literal

from snapper.core.numeric import is_positive_finite

FX_CARRY_BOUND: Final[timedelta] = timedelta(hours=96)
"""Oldest earlier close a valuation may reuse on one FX plane.

Four days span a forex weekend plus a missing day, so one rate per day values
every point. Changing the bound changes published money, so it is a constant
tied to the calculation version rather than a runtime setting.
"""

type FxOrientation = Literal["direct", "inverse"]
"""Whether a pinned plane is used as quoted (``direct``) or reciprocal."""

type FxPairKey = tuple[str, str]
"""Lexically normalized unordered currency-pair identity."""

type FxRateKey = tuple[str, str, str, datetime]
"""Lookup key ``(base, quote, exchange, minute)`` for one candle close."""

type FxRateMap = Mapping[FxRateKey, float]
"""Plane-qualified ``base``/``quote`` closes keyed by the minute they value.

Built by the API layer from finalized 1m candles of the currency pairs a scope
actually needs; the pure conversion below never performs I/O.
"""

type FxRatePlane = tuple[str, str, str]
"""Exact ``(base, quote, exchange)`` identity of one FX candle series."""

type FxVenueMap = Mapping[FxPairKey, FxRatePlane]
"""One consumer-pinned oriented plane for each unordered currency pair."""


def currency_pair_key(first: str, second: str) -> FxPairKey:
    """Return the deterministic unordered identity for two currencies.

    Args:
        first: One currency leg.
        second: The other currency leg.

    Returns:
        The two currency codes in lexical order.
    """
    return (first, second) if first <= second else (second, first)


@dataclass(frozen=True)
class ResolvedRate:
    """One pinned plane's close resolved for a specific FROM-TO conversion.

    ``close`` is the exact positive-finite candle close of the pinned
    ``(base, quote, exchange)`` plane at the minute. ``orientation`` records
    whether that plane is quoted as requested (``direct``: one FROM costs
    ``close`` TO) or reciprocal (``inverse``: one FROM costs ``1 / close`` TO).
    The consumer applies the exact same arithmetic the conversion uses and can
    derive the effective FROM-TO rate without re-selecting a plane.
    """

    base: str
    quote: str
    exchange: str
    close: float
    orientation: FxOrientation


def resolve_rate(
    from_currency: str,
    to_currency: str,
    minute: datetime,
    rates: FxRateMap,
    venues: FxVenueMap | None = None,
) -> ResolvedRate | None:
    """Resolve the pinned plane, close and orientation for a conversion.

    This is the plane-selection kernel shared by :func:`convert_amount` and any
    consumer that also needs the exact close and orientation used (for example
    to record valuation provenance). It performs no arithmetic on an amount: it
    pins one oriented plane, reads its close at the exact minute, and classifies
    the orientation. A missing pin, a missing minute, a non-positive or
    non-finite close, or a pin that matches neither orientation all yield
    ``None`` so the caller withholds rather than fabricating a rate.

    Args:
        from_currency: Currency being converted from.
        to_currency: Target currency.
        minute: Grid minute whose ``[minute-1m, minute)`` close is the rate.
        rates: Preloaded plane-qualified candle closes for this scope.
        venues: Consumer-pinned oriented plane keyed by unordered currency pair.

    Returns:
        The resolved plane close and orientation, or ``None`` when no usable
        close pins the requested currencies at that exact minute.
    """
    resolved_venues: FxVenueMap = {} if venues is None else venues
    plane = resolved_venues.get(currency_pair_key(from_currency, to_currency))
    if plane is None:
        return None
    base_currency, quote_currency, exchange = plane
    close = rates.get((base_currency, quote_currency, exchange, minute))
    if not is_positive_finite(close):
        return None
    if (base_currency, quote_currency) == (from_currency, to_currency):
        orientation: FxOrientation = "direct"
    elif (base_currency, quote_currency) == (to_currency, from_currency):
        orientation = "inverse"
    else:
        return None
    return ResolvedRate(
        base=base_currency,
        quote=quote_currency,
        exchange=exchange,
        close=close,
        orientation=orientation,
    )


@dataclass(frozen=True)
class CarriedRate:
    """One usable close and the minute it was actually published.

    Attributes:
        plane: Exact ``(base, quote, exchange)`` series the close belongs to.
        close: Positive finite close of ``plane`` at ``source_minute``.
        orientation: Whether the plane is quoted as requested or reciprocal.
        source_minute: Bar-close minute of the evidence used.
        age_minutes: Whole minutes between the valued minute and the evidence;
            zero means the rate is exact.
    """

    plane: FxRatePlane
    close: float
    orientation: FxOrientation
    source_minute: datetime
    age_minutes: int


@dataclass(frozen=True)
class CarriedConversion:
    """A converted amount and the rate that produced it.

    Attributes:
        amount: Finite converted amount in the target currency.
        rate: Evidence used, or ``None`` when the conversion needed none because
            the amount is exactly zero or already in the target currency.
    """

    amount: float
    rate: CarriedRate | None


def _plane_orientation(
    plane: FxRatePlane,
    from_currency: str,
    to_currency: str,
) -> FxOrientation | None:
    """Classify a plane against the requested conversion, or reject a mismatch."""
    if (plane[0], plane[1]) == (from_currency, to_currency):
        return "direct"
    if (plane[0], plane[1]) == (to_currency, from_currency):
        return "inverse"
    return None


def _apply_rate(amount: float, close: float, orientation: FxOrientation) -> float:
    """Apply one close in the fixed orientation, keeping both paths bit-identical."""
    return amount * close if orientation == "direct" else amount / close


@dataclass(frozen=True)
class FxCarryBook:
    """All rate evidence one request may consult, exact and carried.

    A book with an empty ``published`` index cannot carry, so it behaves exactly
    like the exact-minute functions. That is how exact-only callers share this
    one conversion path, and why :func:`exact_fx_book` exists.

    Attributes:
        exact: Plane-qualified closes; a conflicted minute is ``NaN``.
        published: Ascending usable minutes per plane, excluding every conflicted
            or invalid close so that no search can ever land on one.
        bound: Oldest evidence a carried minute may use.
    """

    exact: FxRateMap
    published: Mapping[FxRatePlane, Sequence[datetime]] = field(default_factory=dict)
    bound: timedelta = FX_CARRY_BOUND

    def resolve(
        self,
        from_currency: str,
        to_currency: str,
        minute: datetime,
        venues: FxVenueMap | None,
    ) -> CarriedRate | None:
        """Resolve the pinned plane exactly when evidenced, otherwise by carry.

        Args:
            from_currency: Currency being converted from.
            to_currency: Target currency.
            minute: Grid minute being valued.
            venues: Consumer-pinned oriented plane keyed by unordered pair.

        Returns:
            The rate and its provenance, or ``None`` when the pair is unpinned,
            the exact evidence is contradictory, or nothing lies inside the bound.
        """
        plane = (
            None if venues is None else venues.get(currency_pair_key(from_currency, to_currency))
        )
        if plane is None:
            return None
        orientation = _plane_orientation(plane, from_currency, to_currency)
        if orientation is None:
            return None
        exact_key = (plane[0], plane[1], plane[2], minute)
        if exact_key in self.exact:
            close = self.exact[exact_key]
            if not is_positive_finite(close):
                return None
            return CarriedRate(plane, close, orientation, minute, 0)
        source = self._latest_source(plane, minute)
        if source is None:
            return None
        return CarriedRate(
            plane,
            self.exact[(plane[0], plane[1], plane[2], source)],
            orientation,
            source,
            int((minute - source).total_seconds() // 60),
        )

    def _latest_source(self, plane: FxRatePlane, minute: datetime) -> datetime | None:
        """Return the newest usable publication before ``minute`` inside the bound."""
        minutes = self.published.get(plane, ())
        index = bisect_left(minutes, minute)
        if index == 0 or minutes[index - 1] < minute - self.bound:
            return None
        return minutes[index - 1]

    def convert(
        self,
        amount: float,
        from_currency: str,
        to_currency: str,
        minute: datetime,
        venues: FxVenueMap | None,
    ) -> CarriedConversion | None:
        """Convert one amount, retaining the evidence the value depends on.

        Exact zero and identity need no evidence, matching :func:`convert_amount`.

        Args:
            amount: Signed amount denominated in ``from_currency``.
            from_currency: Denomination; empty means none was recorded.
            to_currency: Target valuation currency.
            minute: Grid minute being valued.
            venues: Consumer-pinned oriented plane keyed by unordered pair.

        Returns:
            The converted amount with provenance, or ``None`` when no usable
            evidence exists or the arithmetic left the finite range.
        """
        if math.isclose(amount, 0.0, rel_tol=0.0, abs_tol=0.0):
            return CarriedConversion(0.0, None)
        if from_currency == to_currency:
            return CarriedConversion(amount, None)
        rate = self.resolve(from_currency, to_currency, minute, venues)
        if rate is None:
            return None
        converted = _apply_rate(amount, rate.close, rate.orientation)
        return CarriedConversion(converted, rate) if math.isfinite(converted) else None


def exact_fx_book(rates: FxRateMap) -> FxCarryBook:
    """Wrap an exact-only rate map so it shares the carried conversion path.

    Args:
        rates: Plane-qualified closes.

    Returns:
        A book that serves exact minutes and can never carry.
    """
    return FxCarryBook(exact=rates)


def build_fx_carry_book(rates: FxRateMap, bound: timedelta = FX_CARRY_BOUND) -> FxCarryBook:
    """Index usable publications so a missing minute can find its predecessor.

    Args:
        rates: Plane-qualified closes, with conflicted minutes already ``NaN``.
        bound: Oldest evidence a carried minute may use.

    Returns:
        A book whose per-plane index holds only positive finite closes.
    """
    published: dict[FxRatePlane, list[datetime]] = {}
    for (base, quote, exchange, minute), close in rates.items():
        if is_positive_finite(close):
            published.setdefault((base, quote, exchange), []).append(minute)
    return FxCarryBook(
        exact=rates,
        published={plane: tuple(sorted(minutes)) for plane, minutes in published.items()},
        bound=bound,
    )


def convert_amount(
    amount: float,
    from_currency: str,
    to_currency: str,
    minute: datetime,
    rates: FxRateMap,
    venues: FxVenueMap | None = None,
) -> float | None:
    """Convert one monetary amount into ``to_currency`` at a specific minute.

    Args:
        amount: Signed amount denominated in ``from_currency``.
        from_currency: Currency the amount is denominated in. An empty string
            means the writer recorded no denomination, which is only acceptable
            for an exact zero.
        to_currency: Target valuation currency.
        minute: Grid minute whose ``[minute-1m, minute)`` close is the rate.
        rates: Preloaded plane-qualified candle closes for this scope.
        venues: Consumer-pinned oriented plane keyed by unordered currency pair.

    Returns:
        The converted amount, or ``None`` when the pinned plane has no usable
        close at that exact minute or does not match the requested currencies.
    """
    if math.isclose(amount, 0.0, rel_tol=0.0, abs_tol=0.0):
        return 0.0
    if from_currency == to_currency:
        return amount
    resolved = resolve_rate(from_currency, to_currency, minute, rates, venues)
    if resolved is None:
        return None
    converted = _apply_rate(amount, resolved.close, resolved.orientation)
    return converted if math.isfinite(converted) else None


def required_pairs(currencies: frozenset[str], valuation_ccy: str) -> frozenset[tuple[str, str]]:
    """Return the currency pairs a scope must load to value its flows.

    The identity currency is excluded because it needs no evidence, and each
    remaining currency is requested in BOTH orientations so request-wide plane
    election can compare each independently collected candle series.

    Args:
        currencies: Distinct currencies the scope's flows are denominated in.
        valuation_ccy: The series valuation currency.

    Returns:
        Distinct ``(base, quote)`` pairs to resolve into candle series.
    """
    pairs: set[tuple[str, str]] = set()
    for currency in currencies:
        if not currency or currency == valuation_ccy:
            continue
        pairs.add((currency, valuation_ccy))
        pairs.add((valuation_ccy, currency))
    return frozenset(pairs)
