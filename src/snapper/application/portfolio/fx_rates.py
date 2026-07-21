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
- **Direct pair.** ``FROM-TO`` is used as quoted: one unit of FROM costs ``close``
  units of TO.
- **Inverse pair.** When only ``TO-FROM`` exists, the reciprocal is used. A
  non-finite or non-positive close is refused rather than inverted, because
  ``1/0`` would fabricate an infinite rate out of a placeholder row.
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

type FxRateKey = tuple[str, str, datetime]
"""Lookup key ``(base_currency, quote_currency, minute)`` for one candle close."""

type FxRateMap = Mapping[FxRateKey, float]
"""Resolved ``base``/``quote`` closes keyed by the grid minute they value.

Built by the API layer from finalized 1m candles of the currency pairs a scope
actually needs; the pure conversion below never performs I/O.
"""


def convert_amount(
    amount: float,
    from_currency: str,
    to_currency: str,
    minute: datetime,
    rates: FxRateMap,
) -> float | None:
    """Convert one monetary amount into ``to_currency`` at a specific minute.

    Args:
        amount: Signed amount denominated in ``from_currency``.
        from_currency: Currency the amount is denominated in. An empty string
            means the writer recorded no denomination, which is only acceptable
            for an exact zero.
        to_currency: Target valuation currency.
        minute: Grid minute whose ``[minute-1m, minute)`` close is the rate.
        rates: Preloaded candle closes for the pairs this scope needs.

    Returns:
        The converted amount, or ``None`` when no direct or inverse pair covers
        that minute with a usable rate.
    """
    if amount == 0.0:
        return 0.0
    if from_currency == to_currency:
        return amount
    direct = rates.get((from_currency, to_currency, minute))
    if is_positive_finite(direct):
        converted = amount * direct
        return converted if math.isfinite(converted) else None
    inverse = rates.get((to_currency, from_currency, minute))
    if is_positive_finite(inverse):
        converted = amount / inverse
        return converted if math.isfinite(converted) else None
    return None


def required_pairs(currencies: frozenset[str], valuation_ccy: str) -> frozenset[tuple[str, str]]:
    """Return the currency pairs a scope must load to value its flows.

    The identity currency is excluded because it needs no evidence, and each
    remaining currency is requested in BOTH orientations so the loader can
    satisfy it with whichever of the direct or inverse pair the venue lists.

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
