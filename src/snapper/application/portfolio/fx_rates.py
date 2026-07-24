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
"""

import math
from collections.abc import Mapping
from datetime import datetime

from snapper.core.numeric import is_positive_finite

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
    resolved_venues: FxVenueMap = {} if venues is None else venues
    plane = resolved_venues.get(currency_pair_key(from_currency, to_currency))
    if plane is None:
        return None
    base_currency, quote_currency, exchange = plane
    close = rates.get((base_currency, quote_currency, exchange, minute))
    if not is_positive_finite(close):
        return None
    if (base_currency, quote_currency) == (from_currency, to_currency):
        converted = amount * close
    elif (base_currency, quote_currency) == (to_currency, from_currency):
        converted = amount / close
    else:
        return None
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
