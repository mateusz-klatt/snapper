"""Currency-to-USD basket valuation from finalized 1m evidence (Phase 5B).

Absolute equity is an OBSERVATION: the venue-observed balance basket priced at a
grid minute. To turn that basket into one USD number every held currency must be
converted at minute ``M`` off the SAME finalized-candle evidence the marks come
from, and a partition of the total into position value versus cash must be
labelled without ever changing the total. This module is the pure, I/O-free core
of both jobs.

Two responsibilities, both fail-closed:

- **Per-currency valuation.** :func:`value_currency` prices one currency leg into
  USD. USD is the identity (needs no evidence); a zero balance is
  currency-invariant (needs no evidence); a fiat currency is converted through
  the EXISTING forex candle plane (:mod:`snapper.application.portfolio.fx_rates`,
  composed, never re-derived); a crypto currency is priced off a pre-loaded
  crypto→USD candle plane, preferring the exchange holding the balance and
  breaking ties by a deterministic plane ordering. Every failure returns
  ``usd_value`` ``None`` plus a typed :data:`ValuationReason`; every priced leg
  carries :class:`ValuationProvenance` recording exactly which plane and which
  candle VERSION produced the rate, so a later candle correction is attributable
  to the exact inputs that were used.

- **Position/cash partition.** :func:`attribute_position_inventory` implements the
  per-``(exchange, base_currency)`` single-cap partition: the position-labelled
  quantity is the observed balance capped by the summed positive quantity of the
  active, non-margin, spot positions on that exact exchange. Caps never cross
  exchanges and are never applied per instrument row; short or spot-margin
  inventory never enters the positive allocation but is reported through a
  coverage flag. The caller derives ``cash = equity − position_value`` from the
  SAME per-leg floats, so the partition can never double-count or drift.

Nothing here reads the database, carries a rate forward, searches a neighbouring
minute, or invents a number. A withheld leg is honest; a fabricated one silently
misstates money.
"""

import math
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from snapper.application.portfolio.fx_rates import FxRateKey
from snapper.application.portfolio.fx_rates import FxRateMap
from snapper.application.portfolio.fx_rates import FxVenueMap
from snapper.application.portfolio.fx_rates import currency_pair_key
from snapper.application.portfolio.fx_rates import resolve_rate
from snapper.core.numeric import is_positive_finite

type ValuationProvenanceKind = Literal["identity", "fiat_fx", "crypto_candle"]
"""Which price plane produced a priced leg's USD value."""

type ValuationReason = Literal[
    "missing_fiat_rate",
    "missing_version",
    "missing_crypto_plane",
    "no_usable_close",
    "non_finite",
    "overflow",
    "ambiguous_plane",
]
"""Why a currency leg was withheld from valuation.

- ``missing_fiat_rate``: the pinned fiat plane produced no usable rate.
- ``missing_version``: a usable rate exists but its candle VERSION identity is
  absent, so the priced leg could not be made attributable and is withheld.
- ``missing_crypto_plane``: no crypto instrument was admitted to the price plane.
- ``no_usable_close``: admitted crypto instruments had no positive-finite close.
- ``non_finite``: the input quantity is not a finite number.
- ``overflow``: a finite quantity and rate produced a non-finite USD value or rate.
- ``ambiguous_plane``: two distinct instruments price one venue plane at the
  minute, so no single price can be elected without inventing one.
"""

type CryptoPlaneKey = tuple[str, datetime]
"""Lookup key ``(currency, minute)`` for the crypto→USD candles of one minute."""


@dataclass(frozen=True)
class CryptoUsdCandle:
    """One finalized crypto→USD candle carrying its full version identity.

    Assembled by the caller from
    :meth:`SQLAlchemyRepository.get_pnl_crypto_usd_plane_candles`. ``base`` is the
    priced currency and ``quote`` is always ``USD``. ``candle_id`` is the row's
    immutable internal primary key and ``candle_timestamp`` its SCD2 version time;
    together with ``candle_public_id`` they pin the exact candle version, because
    a candle correction reuses ``public_id`` while changing ``close``.
    """

    base: str
    quote: str
    exchange: str
    native_symbol: str
    instrument_public_id: str
    candle_id: int
    candle_public_id: str
    candle_open_at: datetime
    candle_timestamp: datetime
    close: float


type CryptoUsdPlaneMap = Mapping[CryptoPlaneKey, Sequence[CryptoUsdCandle]]
"""Candidate crypto→USD candles keyed by ``(currency, minute)``.

The value is every eligible venue plane's candle for that currency at that
minute; :func:`value_currency` filters to positive-finite closes, prefers the
holding exchange, and otherwise breaks ties deterministically.
"""


@dataclass(frozen=True)
class CandleVersionIdentity:
    """Immutable identity of the exact candle version a rate was read from."""

    instrument_public_id: str
    native_symbol: str
    candle_id: int
    candle_public_id: str
    candle_open_at: datetime
    candle_timestamp: datetime


type FiatVersionMap = Mapping[FxRateKey, CandleVersionIdentity]
"""Fiat candle version identity keyed by ``(base, quote, exchange, minute)``.

Assembled by the caller from ``get_pnl_fx_rate_candles`` rows on the same key the
rate map uses, so a fiat leg records the exact candle version its rate came from.
"""


@dataclass(frozen=True)
class ValuationEvidence:
    """Pre-loaded price evidence for one grid minute's currency legs.

    The fiat plane is the existing forex candle map and its consumer-pinned
    oriented venue map, consumed exactly as :func:`resolve_rate` expects, plus a
    parallel version-identity map keyed identically to the rate map. The crypto
    plane is the new crypto→USD candle map. Bundling them keeps
    :func:`value_currency` to a single evidence argument.
    """

    fiat_rates: FxRateMap
    fiat_venues: FxVenueMap
    fiat_versions: FiatVersionMap
    crypto_planes: CryptoUsdPlaneMap


@dataclass(frozen=True)
class ValuationProvenance:
    """Evidence trail for one priced currency leg.

    ``rate`` is the exact finite conversion actually applied: ``1.0`` for the USD
    identity, the pinned plane's close (direct) or its reciprocal (inverse) for a
    fiat leg, and the candle ``close`` for a crypto leg. ``close`` is the EXACT
    candle close consumed (``1.0`` for the identity), stored verbatim so an
    inverse-fiat leg records the divisor actually used rather than the reciprocal
    ``rate``; a downstream partition re-values through the same path per unit and
    stays bit-consistent with this leg. ``candle`` always pins the consumed candle
    version for a fiat or crypto leg — a priced leg with no version identity is
    withheld rather than emitted — and is ``None`` only for the USD identity, which
    consumes no candle.
    """

    kind: ValuationProvenanceKind
    base: str
    quote: str
    exchange: str
    rate: float
    close: float
    candle: CandleVersionIdentity | None


@dataclass(frozen=True)
class ValuedLeg:
    """Result of valuing one currency leg into USD.

    A priced leg carries a finite ``usd_value`` and its ``provenance`` with
    ``reason`` ``None``. A currency-invariant zero carries ``usd_value`` ``0.0``
    with no ``provenance`` (no evidence is needed). A withheld leg carries
    ``usd_value`` ``None`` and a typed ``reason``.
    """

    usd_value: float | None
    provenance: ValuationProvenance | None
    reason: ValuationReason | None


def _identity_leg(exchange: str, qty: float) -> ValuedLeg:
    """Return the USD identity leg for a USD balance.

    Args:
        exchange: Exchange holding the balance, recorded for provenance.
        qty: The USD quantity, which is already its own USD value.

    Returns:
        A priced leg valued at ``qty`` with identity provenance.
    """
    provenance = ValuationProvenance(
        kind="identity",
        base="USD",
        quote="USD",
        exchange=exchange,
        rate=1.0,
        close=1.0,
        candle=None,
    )
    return ValuedLeg(usd_value=qty, provenance=provenance, reason=None)


def _value_fiat(
    currency: str, qty: float, minute: datetime, evidence: ValuationEvidence
) -> ValuedLeg:
    """Value a fiat currency leg through the pinned forex candle plane.

    Plane selection and orientation are delegated wholesale to
    :func:`resolve_rate`; the leg applies the SAME arithmetic the forex converter
    uses (multiply when direct, divide when inverse) and derives the effective
    ``from``→USD rate from the same close, guarding it for finiteness so a
    subnormal close can never certify a priced leg with a non-finite rate. A
    pinned plane with no usable close withholds rather than falling through to any
    other plane. Provenance records the exact candle VERSION the rate came from; a
    resolved rate whose version identity is absent is withheld with
    ``missing_version`` rather than emitted as an unattributable price.

    Args:
        currency: The fiat currency the balance is denominated in.
        qty: The finite, non-zero balance quantity.
        minute: Grid minute whose ``[minute-1m, minute)`` close is the rate.
        evidence: Pre-loaded fiat rate, venue and version maps.

    Returns:
        A priced leg, or a withheld leg with ``missing_fiat_rate``, ``overflow`` or
        ``missing_version``.
    """
    resolved = resolve_rate(currency, "USD", minute, evidence.fiat_rates, evidence.fiat_venues)
    if resolved is None:
        return ValuedLeg(usd_value=None, provenance=None, reason="missing_fiat_rate")
    if resolved.orientation == "direct":
        usd = qty * resolved.close
        rate = resolved.close
    else:
        usd = qty / resolved.close
        rate = 1.0 / resolved.close
    if not (math.isfinite(usd) and math.isfinite(rate)):
        return ValuedLeg(usd_value=None, provenance=None, reason="overflow")
    version = evidence.fiat_versions.get((resolved.base, resolved.quote, resolved.exchange, minute))
    if version is None:
        return ValuedLeg(usd_value=None, provenance=None, reason="missing_version")
    provenance = ValuationProvenance(
        kind="fiat_fx",
        base=resolved.base,
        quote=resolved.quote,
        exchange=resolved.exchange,
        rate=rate,
        close=resolved.close,
        candle=version,
    )
    return ValuedLeg(usd_value=usd, provenance=provenance, reason=None)


def _select_plane_exchange(exchange: str, usable: list[CryptoUsdCandle]) -> str:
    """Return the venue whose plane prices the leg: holding first, else earliest.

    Args:
        exchange: Exchange holding the balance.
        usable: Positive-finite-close candidates sorted by plane identity.

    Returns:
        The holding exchange when it supplies a candle, otherwise the exchange of
        the deterministically-first candidate.
    """
    for candle in usable:
        if candle.exchange == exchange:
            return exchange
    return usable[0].exchange


def _value_crypto(
    exchange: str,
    currency: str,
    qty: float,
    minute: datetime,
    crypto_planes: CryptoUsdPlaneMap,
) -> ValuedLeg:
    """Value a crypto currency leg off the pre-loaded crypto→USD candle plane.

    Candidates are filtered to positive-finite closes and ordered by
    ``(base, quote, exchange, instrument)``. The venue is selected — holding
    exchange preferred, deterministic first otherwise — and if that one venue
    plane is supplied by more than one distinct instrument the leg fails closed
    with ``ambiguous_plane`` rather than letting row order elect one of two
    prices. The product is finiteness-guarded so an overflow withholds.

    Args:
        exchange: Exchange holding the balance; its plane is preferred.
        currency: The crypto currency being priced.
        qty: The finite, non-zero balance quantity.
        minute: Grid minute selecting the candle set.
        crypto_planes: Candidate candles keyed by ``(currency, minute)``.

    Returns:
        A priced leg, or a withheld leg with ``missing_crypto_plane``,
        ``no_usable_close``, ``ambiguous_plane`` or ``overflow``.
    """
    candidates = crypto_planes.get((currency, minute))
    if not candidates:
        return ValuedLeg(usd_value=None, provenance=None, reason="missing_crypto_plane")
    usable = sorted(
        (candle for candle in candidates if is_positive_finite(candle.close)),
        key=lambda candle: (
            candle.base,
            candle.quote,
            candle.exchange,
            candle.instrument_public_id,
        ),
    )
    if not usable:
        return ValuedLeg(usd_value=None, provenance=None, reason="no_usable_close")
    plane_exchange = _select_plane_exchange(exchange, usable)
    plane_candles = [candle for candle in usable if candle.exchange == plane_exchange]
    if len({candle.instrument_public_id for candle in plane_candles}) > 1:
        return ValuedLeg(usd_value=None, provenance=None, reason="ambiguous_plane")
    chosen = plane_candles[0]
    usd = qty * chosen.close
    if not math.isfinite(usd):
        return ValuedLeg(usd_value=None, provenance=None, reason="overflow")
    provenance = ValuationProvenance(
        kind="crypto_candle",
        base=chosen.base,
        quote=chosen.quote,
        exchange=chosen.exchange,
        rate=chosen.close,
        close=chosen.close,
        candle=CandleVersionIdentity(
            instrument_public_id=chosen.instrument_public_id,
            native_symbol=chosen.native_symbol,
            candle_id=chosen.candle_id,
            candle_public_id=chosen.candle_public_id,
            candle_open_at=chosen.candle_open_at,
            candle_timestamp=chosen.candle_timestamp,
        ),
    )
    return ValuedLeg(usd_value=usd, provenance=provenance, reason=None)


def value_currency(
    exchange: str,
    currency: str,
    qty: float,
    minute: datetime,
    evidence: ValuationEvidence,
) -> ValuedLeg:
    """Convert one currency balance into USD at a specific grid minute.

    Resolution order is deterministic: an exact-zero balance is currency-invariant
    and needs no evidence; a non-finite quantity is refused; USD is the identity;
    a currency with a pinned fiat plane commits to the fiat path; anything else is
    priced off the crypto plane. Missing or unusable evidence never fabricates a
    rate — the leg is withheld with a typed reason.

    Args:
        exchange: Exchange holding the balance; the crypto plane on this exchange
            is preferred and the exchange is recorded in provenance.
        currency: Currency the balance is denominated in.
        qty: Balance quantity denominated in ``currency``.
        minute: Grid minute whose ``[minute-1m, minute)`` close is the rate.
        evidence: Pre-loaded fiat and crypto price evidence for this minute.

    Returns:
        A :class:`ValuedLeg` with a finite USD value and provenance, a
        currency-invariant zero, or a withheld value with a typed reason.
    """
    if abs(qty) <= 0.0:
        return ValuedLeg(usd_value=0.0, provenance=None, reason=None)
    if not math.isfinite(qty):
        return ValuedLeg(usd_value=None, provenance=None, reason="non_finite")
    if currency == "USD":
        return _identity_leg(exchange, qty)
    fiat_plane = evidence.fiat_venues.get(currency_pair_key(currency, "USD"))
    if fiat_plane is not None:
        return _value_fiat(currency, qty, minute, evidence)
    return _value_crypto(exchange, currency, qty, minute, evidence.crypto_planes)


@dataclass(frozen=True)
class PositionInventoryEntry:
    """One active spot position considered for the position/cash partition.

    Futures are excluded upstream, so every entry is a spot position. ``quantity``
    is the signed position size in the base asset. ``is_spot_margin`` marks a
    spot-margin (leveraged) instrument proven from spec metadata; a margin or a
    negative entry is leveraged inventory that never enters the positive
    allocation but is reported through the coverage flag.
    """

    exchange: str
    base_currency: str
    quantity: float
    is_spot_margin: bool


@dataclass(frozen=True)
class CurrencyAttribution:
    """Position/cash split of one observed ``(exchange, currency)`` balance.

    ``observed_qty`` is the authoritative venue balance (valued whole for equity);
    ``eligible_long_qty`` is the summed positive quantity of the eligible spot
    positions on that exchange for that currency; ``attributed_qty`` is the
    balance-capped position portion. The caller values ``observed_qty`` and
    ``attributed_qty`` at the same rate, so ``cash = observed − attributed`` in
    USD is an exact partition.
    """

    exchange: str
    currency: str
    observed_qty: float
    eligible_long_qty: float
    attributed_qty: float


@dataclass(frozen=True)
class InventoryAttribution:
    """Result of the whole-scope position/cash partition.

    ``attributions`` is one entry per observed ``(exchange, currency)`` balance in
    deterministic order, excluding any key poisoned by a corrupt position.
    ``leveraged_inventory_excluded`` is ``True`` when any short or spot-margin
    position was encountered; ``non_finite_position_excluded`` is ``True`` when any
    position quantity was non-finite. Either flag lets coverage metadata state
    that inventory was excluded from the partition. Total equity never depends on
    positions, so both are labelling signals only.
    """

    attributions: tuple[CurrencyAttribution, ...]
    leveraged_inventory_excluded: bool
    non_finite_position_excluded: bool


def _eligible_long_quantities(
    positions: Sequence[PositionInventoryEntry],
) -> tuple[dict[tuple[str, str], float], bool, set[tuple[str, str]]]:
    """Sum eligible long quantities per key, flag leverage, mark corrupt keys.

    A zero quantity is never position-bearing and is skipped without flagging.
    Leverage is evaluated INDEPENDENTLY of finiteness: any non-zero spot-margin or
    short (negative) position raises the leveraged flag even when its quantity is
    also corrupt, so a spot-margin position with a non-finite size sets both flags.
    A non-finite quantity is corrupt input: its ``(exchange, currency)`` key is
    marked so the whole leg is dropped downstream, never entering the eligible sum.
    Every remaining strictly-positive quantity accumulates into its exchange's
    single cap for that currency, so multiple instrument rows never cap per row.
    The per-key sum is itself finiteness-guarded: a running total that overflows
    poisons the key exactly like a directly non-finite input, keeping the pure
    partition total for all finite inputs.

    Args:
        positions: Active spot positions with their spot-margin marker.

    Returns:
        The per-``(exchange, currency)`` eligible long total, whether any
        leveraged inventory was excluded, and the set of keys poisoned by a
        corrupt (non-finite or overflowing) quantity.
    """
    grouped: dict[tuple[str, str], list[float]] = {}
    leveraged = False
    corrupt: set[tuple[str, str]] = set()
    for entry in positions:
        quantity = entry.quantity
        key = (entry.exchange, entry.base_currency)
        if abs(quantity) <= 0.0:
            continue
        leveraged_entry = entry.is_spot_margin or quantity < 0.0
        if leveraged_entry:
            leveraged = True
        if not math.isfinite(quantity):
            corrupt.add(key)
            continue
        if leveraged_entry:
            continue
        grouped.setdefault(key, []).append(quantity)
    eligible: dict[tuple[str, str], float] = {}
    for key, values in grouped.items():
        try:
            eligible[key] = math.fsum(values)
        except OverflowError:
            corrupt.add(key)
    return eligible, leveraged, corrupt


def attribute_position_inventory(
    observed_balances: Mapping[tuple[str, str], float],
    positions: Sequence[PositionInventoryEntry],
) -> InventoryAttribution:
    """Partition observed balances into position value and cash labels.

    For each ``(exchange, currency)``: ``eligible_long_qty`` is the summed
    positive quantity of the active non-margin spot positions on that exact
    exchange whose base is the currency; ``attributed_qty`` is
    ``min(max(observed, 0.0), eligible_long_qty)``. The cap is per exchange and
    never shared across exchanges, and it is applied once per currency, never per
    instrument row. A key with a corrupt (non-finite) position is dropped from the
    attribution entirely and flagged. A non-finite observed balance attributes
    nothing (its whole valuation fail-closes downstream). Short or spot-margin
    inventory is reported through the coverage flag and never inflates the
    position label.

    Args:
        observed_balances: Authoritative venue balance per ``(exchange,
            currency)`` — the whole basket priced for equity.
        positions: Active spot positions used only to LABEL the partition.

    Returns:
        Deterministically ordered per-currency attributions plus the leveraged
        and non-finite-position coverage flags.
    """
    eligible, leveraged, corrupt = _eligible_long_quantities(positions)
    attributions: list[CurrencyAttribution] = []
    for (exchange, currency), observed in sorted(observed_balances.items()):
        if (exchange, currency) in corrupt:
            continue
        eligible_long_qty = eligible.get((exchange, currency), 0.0)
        if math.isfinite(observed):
            attributed_qty = min(max(observed, 0.0), eligible_long_qty)
        else:
            attributed_qty = 0.0
        attributions.append(
            CurrencyAttribution(
                exchange=exchange,
                currency=currency,
                observed_qty=observed,
                eligible_long_qty=eligible_long_qty,
                attributed_qty=attributed_qty,
            )
        )
    return InventoryAttribution(
        attributions=tuple(attributions),
        leveraged_inventory_excluded=leveraged,
        non_finite_position_excluded=bool(corrupt),
    )
