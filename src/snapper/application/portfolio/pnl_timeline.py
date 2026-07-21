"""Pure P&L timeline series builder (Phase 5A core).

Reconstructs the "Net P&L since activation" series for one ``(wallet, mode)``
scope as a PURE projection of the immutable execution ledger, the funding
accrual ledger, and historical marks. Like the spot-anchor witness builder,
this module performs NO I/O: it takes already-fetched typed inputs (executions,
accruals, a mark lookup, and an optional activation anchor) and returns a typed
series. That is what makes the realized / fee / accrual / unrealized
decomposition trivially unit-testable and provably deterministic.

Design decisions carried from the accepted plan
(``plans/plan_2026_07_20_pnl_timeline_impl.md``) and its 13-point soundness
checklist:

- **Kernel reuse (checklist #5).** All pool math routes through
  :func:`snapper.application.portfolio.average_cost.apply_fill`, so the
  timeline's realized decomposition is the SAME volume-weighted average-cost
  flip / overshoot math the live ``TradeService`` projection uses. This builder
  never re-implements VWAP.
- **Separate components (checklist).** ``realized_pnl`` is price-realized only
  (fee-exclusive, funding-exclusive). Execution fees accumulate into a SEPARATE
  ``fee_pnl`` component, stored with an EXPENSE sign (``fee_pnl = -sum(fee)`` so a
  fee reduces P&L and a maker rebate — a negative fee — raises it). Funding
  accruals accumulate into a SEPARATE ``accrual_pnl`` component
  (``accrual_pnl = -sum(amount_usd)`` because a positive accrual means the holder
  pays). Funding is never folded into trade-realized, which is what keeps the
  series from double-counting against ``Position.realized_pnl`` (that surface is
  funding-inclusive).
- **Composite attribution.** Each instrument pool carries ONE quantity-weight
  map keyed by ``(origin, strategy_name)``. Human command surfaces take origin
  precedence over non-manual plans and signal-driven system activity; missing,
  ambiguous, or replay lineage stays ``unattributed``. Reductions assign closed
  quantity, realized P&L, and closing fees from pre-fill weights; flips close the
  old map before assigning only overshoot quantity and opening fees to the
  incoming key. Accruals and unrealized follow current weights. The final
  stable-sorted composite key may absorb a one-unit-in-the-last-place rounding
  residue at emission, so each component reconciles exactly to its aggregate
  without independent origin and strategy maps choosing different residue
  owners. A larger discrepancy withholds the point instead of transporting
  cancellation residue into an unrelated bucket.
- **Seed from t0, never from ``from_time`` (checklist #3).** Pools are seeded
  from the ``opening`` anchor; cumulative realized / fee / accrual start at zero
  at t0. Every execution the caller supplies is replayed onto the seeded pools;
  executions whose economic time precedes ``from_time`` still mutate the pools
  (their effect is baked into the first emitted point) but emit no point of their
  own. The caller bounds the stream to the post-anchor watermark, so the builder
  does NOT additionally filter by t0 on the time axis (the scope-sequence
  watermark is the authoritative boundary; a time-axis filter would fight it
  under clock skew).
- **Baseline leakage guard (checklist #4).** The anchor carries the OPENING
  unrealized VALUE at t0. ``net_pnl`` plots the CHANGE in unrealized from that
  baseline (``unrealized - opening_unrealized_value``), so pre-activation P&L
  never leaks onto the series.
- **Ordering (checklist #2).** ``Execution.executed_at`` is nullable, so pool
  accumulation is ordered by ``scope_sequence`` (the caller supplies executions
  in ``(exchange, scope_sequence)`` order and this builder preserves each
  instrument's sub-order). The TIME AXIS uses ``event_time`` (the execution
  ``timestamp``). When an instrument's event times are non-monotonic against its
  scope order (a regression), the offending fill's effective grid time is clamped
  forward to preserve scope order, and the shadowed minutes — those at or after
  the fill's true economic time but before its clamped placement — are ``UNTRUSTED``
  (see below): their cumulatives would omit an economically-present fill, so every
  component is withheld, not just the unrealized.
- **Two-tier incompleteness (checklist #7).** A point is ``incomplete`` for one of
  two reasons that differ in what is trustworthy:
  (a) MARK-incomplete — a held (non-flat) instrument has no finite mark for that
  minute — withholds only ``unrealized_pnl`` and ``net_pnl``; the cumulative
  realized / fee / accrual are still returned because they are mark-independent. A
  stale or non-finite (NaN/Inf) mark is never carried forward.
  (b) UNTRUSTED — withholds EVERY component (realized / fee / accrual / unrealized /
  net all NULL) because the cumulatives themselves cannot be trusted at that
  instant. The triggers are: a scope-order regression shadows the minute; an
  unknown seeded cost basis is still in play (a non-flat opening position with no
  entry price, or ANY positive realization against one, which permanently taints
  the cumulative realized for the rest of the series and holds until the pool
  flushes fully flat); the minute precedes the activation ``t0`` (the anchor proves
  no position state before then, so a pre-``t0`` grid point must never be valued
  from the seeded book); a fill carries a non-finite or NEGATIVE size; a
  non-positive or non-finite execution price participates in a close, reduction,
  or flip; or a seed carries a non-finite quantity (a corrupt or sign-inverted
  quantity is tainted at ingestion rather than replayed, so it can never fabricate
  a signed position or a bogus realized number). A non-positive or non-finite price
  on an opening or same-side add instead makes only the entry basis unknown, so its
  still-provable cumulatives survive while unrealized and net are withheld. Any
  monetary value — an aggregate cumulative total, a
  PER-INSTRUMENT cumulative (realized / fee / accrual), the opening baseline, a
  per-instrument entry or unrealized, or the summed aggregate unrealized / net — is
  non-finite, whether a NaN/Inf price / fee / mark arrived directly or a VWAP entry
  or a sum overflowed from otherwise-finite inputs (including per-instrument
  overflow that interleaved cancellation hides from the aggregate); or the caller
  cannot prove that an execution price is denominated in the requested valuation
  currency. No ``complete`` point ever carries a non-finite or fabricated number,
  at any level.
- **Activation baseline (checklist #3/#4/#6).** When an anchor is supplied, grid
  points before its ``t0`` are withheld as untrusted (no speculative backfill), and
  pre-activation funding accruals (``accrued_at`` before ``t0``) are dropped so
  ``net_pnl`` starts at zero at activation; pre-``t0`` executions are already
  excluded by the caller's scope-sequence watermark.
- **Downsampling (checklist #6).** The 1m series carries CUMULATIVE realized /
  fee / accrual (integrals since t0) and STOCK unrealized / net. Downsampling to
  5m / 1h / 1d selects the LAST 1m point of each bucket. For the cumulative flow
  components this endpoint value equals the sum of every per-minute delta from t0
  through the bucket end (the flow-preserving reduction the checklist demands);
  for the stock components it is the required endpoint value. A bucket's
  ``valuation_status`` is therefore its endpoint minute's status.

Mark source (documented per the plan; wiring is NOT built here). The API layer
will build the injected ``marks`` mapping from finalized DB 1m candles via
``Repository.get_candles`` (range mode, ``timeframe='1m'``, ``complete=True``;
``repository.py`` around line 7703), using the close of the bar ``[M-1m, M)`` to
value the point at minute ``M``. For a batched multi-instrument fetch the layer
may use ``Repository.get_latest_candles_for_instruments`` (around line 7782). The
in-process ``market_cache`` deque is at most an optional warm tail for the live
edge, never canonical. This builder consumes the pre-resolved USD marks directly
and does no candle reading or FX conversion itself.
"""

import math
from collections import defaultdict
from collections.abc import Collection
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from typing import Final
from typing import Literal

from snapper.application.portfolio.average_cost import FLAT_EPSILON
from snapper.application.portfolio.average_cost import apply_fill
from snapper.core.numeric import is_positive_finite

ValuationStatus = Literal["complete", "incomplete"]
"""Whether a point's mark-to-market valuation is trustworthy or withheld."""

OriginBucket = Literal["manual", "plan", "system", "unattributed"]
"""Proven initiating origin of an execution, or the fail-closed fallback."""

type AttributionKey = tuple[OriginBucket, str | None]
"""Composite origin and signal-derived strategy identity used for allocation."""

type MarkMap = Mapping[tuple[str, datetime], float | None]
"""USD mark lookup keyed by ``(instrument_public_id, point_minute)``.

The value is the USD mark used to value that instrument at grid point ``M`` — the
close of the candle covering ``[M-1m, M)``. A ``None`` value or an absent key
both mean "no mark for that instrument at that minute".
"""

_GRANULARITY_MINUTES: Final[dict[str, int]] = {"1m": 1, "5m": 5, "1h": 60, "1d": 1440}
"""Downsampling bucket width in minutes per supported granularity."""


@dataclass(frozen=True)
class TimelineExecution:
    """One execution on the P&L time axis.

    Attributes:
        instrument_public_id: Instrument the fill belongs to; the pool key.
        exchange: Venue the fill executed on; part of the scope-order tiebreak.
        scope_sequence: Per-``(wallet, exchange, mode)`` commit-ordered counter;
            the authoritative accumulation order.
        event_time: The time-axis timestamp (execution ``timestamp``), used to
            place the fill on the minute grid.
        side: ``'buy'`` (positive signed quantity) or ``'sell'`` (negative).
        size: Unsigned fill quantity.
        price: Execution price.
        fee: Execution fee, assumed denominated in the window valuation currency
            (non-valuation-currency fee conversion is deferred to the API layer).
        fee_asset: Asset the fee is denominated in; carried for the future
            conversion layer, not consumed by the pure math here.
        order_public_id: Immutable order identity used to resolve the caller's
            supplied command lineage.
    """

    instrument_public_id: str
    exchange: str
    scope_sequence: int
    event_time: datetime
    side: str
    size: float
    price: float
    fee: float
    fee_asset: str
    order_public_id: str


@dataclass(frozen=True)
class TimelineExecutionLineage:
    """Resolved initiating-command lineage for one execution order.

    Attributes:
        source_surface: Command ingress surface. ``rest``, ``mcp``, and ``ws``
            prove a human command; ``strategy`` proves a system emitter.
        plan_public_id: Execution plan identity, when present. This field alone
            cannot prove plan origin because manual orders create a
            ``manual_once`` plan too.
        signal_public_id: Initiating signal identity, when present.
        origin: Market-frame provenance, ``live`` or ``replay``. Replay lineage
            is never assigned to an initiating-origin bucket.
        strategy_name: Stable strategy label resolved only through the linked
            signal. ``TradeCommand.strategy_id`` is deliberately absent.
    """

    source_surface: str | None
    plan_public_id: str | None
    signal_public_id: str | None
    origin: str | None
    strategy_name: str | None


@dataclass(frozen=True)
class TimelineAccrual:
    """One funding accrual on the P&L time axis.

    Attributes:
        instrument_public_id: Instrument the accrual applies to.
        accrued_at: The time-axis timestamp the accrual takes effect at.
        amount_usd: Signed accrual amount in the valuation currency; positive
            means the holder pays (a negative P&L contribution).
    """

    instrument_public_id: str
    accrued_at: datetime
    amount_usd: float


@dataclass(frozen=True)
class OpeningPosition:
    """One instrument's seeded pool state at the activation anchor.

    Attributes:
        position_qty: Signed position quantity at t0.
        entry_price: Volume-weighted average entry price at t0, or ``None`` when
            the seeded position is flat or its entry is unknown.
    """

    position_qty: float
    entry_price: float | None


@dataclass(frozen=True)
class TimelineOpening:
    """The activation anchor seeding the replay.

    Attributes:
        positions: Per-instrument seeded pool state at t0.
        opening_unrealized_value: The mark-to-market unrealized VALUE of the
            seeded book at t0, subtracted from every later unrealized so the
            series plots the change since activation.
        t0: The anchor instant. Metadata only: the caller already bounds the
            supplied stream to the post-anchor watermark, so t0 is not used to
            re-filter events on the time axis.
    """

    positions: Mapping[str, OpeningPosition]
    opening_unrealized_value: float
    t0: datetime


@dataclass(frozen=True)
class TimelineWindow:
    """The requested series window.

    Attributes:
        from_time: Inclusive start of the emitted grid (floored to the minute).
        to_time: Inclusive end of the emitted grid.
        granularity: One of ``'1m'``, ``'5m'``, ``'1h'``, ``'1d'``.
        valuation_ccy: Currency all monetary components are expressed in.
    """

    from_time: datetime
    to_time: datetime
    granularity: str
    valuation_ccy: str


@dataclass(frozen=True)
class PnlInstrumentContribution:
    """One instrument's contribution to a series point.

    Attributes:
        instrument_public_id: The contributing instrument.
        realized_pnl: Cumulative price-realized P&L since t0 for this instrument,
            or ``None`` when the point's cumulatives are untrusted (a scope-order
            regression at this minute, or an unknown seeded cost basis).
        fee_pnl: Cumulative fee P&L since t0 (expense sign) for this instrument,
            ``None`` under the same untrusted conditions.
        accrual_pnl: Cumulative funding accrual P&L since t0 for this instrument,
            ``None`` under the same untrusted conditions.
        unrealized_pnl: This instrument's mark-to-market unrealized at the point,
            ``0.0`` when flat, or ``None`` when held but its mark or seeded entry
            is unavailable (or the point's cumulatives are untrusted).
    """

    instrument_public_id: str
    realized_pnl: float | None
    fee_pnl: float | None
    accrual_pnl: float | None
    unrealized_pnl: float | None


@dataclass(frozen=True)
class PnlAttributionContribution:
    """One composite origin/strategy bucket's contribution to a point.

    Every flow component is cumulative since activation. Unrealized follows the
    bucket's current quantity weights. All fields are withheld when the point's
    cumulatives are untrusted; only ``unrealized_pnl`` is withheld for a bucket
    exposed to an unavailable mark.
    """

    origin: OriginBucket
    strategy_name: str | None
    realized_pnl: float | None
    fee_pnl: float | None
    accrual_pnl: float | None
    unrealized_pnl: float | None


@dataclass(frozen=True)
class PnlTimelinePoint:
    """One point on the P&L series.

    Attributes:
        point_time: The grid instant (bucket endpoint for downsampled series).
        realized_pnl: Cumulative price-realized P&L since t0. ``None`` only when
            the cumulatives are UNTRUSTED — a scope-order regression shadows this
            minute, or an unknown seeded cost basis is still in play. A point that
            is merely mark-incomplete (a held instrument has no mark) keeps its
            realized/fee/accrual, since those are mark-independent.
        fee_pnl: Cumulative fee P&L since t0 (expense sign; fees reduce P&L), or
            ``None`` under the same untrusted conditions as ``realized_pnl``.
        accrual_pnl: Cumulative funding accrual P&L since t0, or ``None`` under
            the same untrusted conditions.
        unrealized_pnl: Aggregate mark-to-market unrealized at the point, or
            ``None`` when the point is incomplete.
        net_pnl: ``realized_pnl + fee_pnl + accrual_pnl + (unrealized_pnl -
            opening_unrealized_value)``, or ``None`` when the point is incomplete.
        valuation_status: ``'complete'`` or ``'incomplete'``.
        per_instrument: Per-instrument contributions, ordered by instrument id.
        attribution: Composite origin/strategy contributions in stable order.
    """

    point_time: datetime
    realized_pnl: float | None
    fee_pnl: float | None
    accrual_pnl: float | None
    unrealized_pnl: float | None
    net_pnl: float | None
    valuation_status: ValuationStatus
    per_instrument: tuple[PnlInstrumentContribution, ...]
    attribution: tuple[PnlAttributionContribution, ...]


@dataclass(frozen=True)
class PnlTimelineResult:
    """The built P&L series.

    Attributes:
        points: The ordered series points at the requested granularity.
        granularity: The granularity the points are bucketed at.
        valuation_ccy: The currency the components are expressed in.
    """

    points: tuple[PnlTimelinePoint, ...]
    granularity: str
    valuation_ccy: str


@dataclass(frozen=True)
class _Pool:
    """Internal signed average-cost pool state for one instrument."""

    position_qty: float
    entry_price: float | None


@dataclass(frozen=True)
class _PreparedExecution:
    """An execution paired with its monotone-clamped effective grid time."""

    effective_time: datetime
    exchange: str
    scope_sequence: int
    execution: TimelineExecution


_UNATTRIBUTED_KEY: Final[AttributionKey] = ("unattributed", None)
"""Composite fallback for lineage or inventory that cannot be proven."""

_MANUAL_SURFACES: Final[frozenset[str]] = frozenset({"mcp", "rest", "ws"})
"""Command ingress surfaces that prove a human initiated the order."""


def _attribution_sort_key(key: AttributionKey) -> tuple[str, int, str]:
    """Return the deterministic residue and transport ordering for one key."""
    origin, strategy_name = key
    return origin, 0 if strategy_name is None else 1, strategy_name or ""


def _sorted_attribution_keys(
    keys: Sequence[AttributionKey] | set[AttributionKey],
) -> list[AttributionKey]:
    """Return unique composite keys in deterministic origin/strategy order."""
    return sorted(set(keys), key=_attribution_sort_key)


def _execution_attribution(
    execution: TimelineExecution,
    lineage: Mapping[str, TimelineExecutionLineage],
) -> AttributionKey:
    """Resolve one fill's fail-closed initiating origin and strategy key.

    Human command surfaces take precedence over plan and system evidence, which
    is what keeps a REST or MCP ``manual_once`` command manual even though it
    necessarily carries a plan id. A plan id without a recognised non-manual
    source is not proof. Strategy identity is independent of the origin bucket
    but is accepted only through an explicit signal link. Replay market-frame
    provenance forces the origin to ``unattributed`` while retaining any
    independently resolved signal strategy.

    Args:
        execution: Fill whose order identity selects the lineage.
        lineage: Caller-resolved order-to-command-and-signal lineage map.

    Returns:
        The composite origin and signal-derived strategy key.
    """
    resolved = lineage.get(execution.order_public_id)
    if resolved is None:
        return _UNATTRIBUTED_KEY
    strategy_name = resolved.strategy_name if resolved.signal_public_id is not None else None
    if resolved.origin != "live":
        return "unattributed", strategy_name
    source_surface = resolved.source_surface
    if source_surface in _MANUAL_SURFACES:
        return "manual", strategy_name
    if source_surface == "strategy":
        if resolved.plan_public_id is not None:
            return "plan", strategy_name
        return "system", strategy_name
    return "unattributed", strategy_name


def _allocate_by_weights(
    amount: float,
    weights: Mapping[AttributionKey, float],
) -> dict[AttributionKey, float]:
    """Allocate one scalar pro-rata with the final stable key taking residue.

    An absent, non-finite, or non-positive weight pool cannot prove ownership,
    so the complete amount goes to ``unattributed``. For a valid pool every key
    except the final stable-sorted key receives its direct float pro-rata share;
    the final key receives ``amount - allocated``. The same deterministic
    summation order is used when point contributions are reconciled, making the
    exposed buckets sum exactly to their aggregate.

    Args:
        amount: Quantity or monetary amount to distribute.
        weights: Pre-event composite quantity weights.

    Returns:
        Per-key allocations whose stable-order sum equals ``amount``.
    """
    if any(not math.isfinite(weight) or weight <= 0.0 for weight in weights.values()):
        return {_UNATTRIBUTED_KEY: amount}
    keys = _sorted_attribution_keys(set(weights))
    total_weight = sum(weights[key] for key in keys)
    if not keys or not math.isfinite(total_weight) or total_weight <= 0.0:
        return {_UNATTRIBUTED_KEY: amount}
    allocations: dict[AttributionKey, float] = {}
    amount_mantissa, amount_exponent = math.frexp(amount)
    total_mantissa, total_exponent = math.frexp(total_weight)
    for key in keys[:-1]:
        weight_mantissa, weight_exponent = math.frexp(weights[key])
        allocation = math.ldexp(
            amount_mantissa * weight_mantissa / total_mantissa,
            amount_exponent + weight_exponent - total_exponent,
        )
        if not math.isfinite(allocation):
            return {_UNATTRIBUTED_KEY: amount}
        allocations[key] = allocation
    final_allocation = amount - sum(allocations.values())
    if not math.isfinite(final_allocation):
        return {_UNATTRIBUTED_KEY: amount}
    allocations[keys[-1]] = final_allocation
    return allocations


def _add_allocations(
    target: defaultdict[AttributionKey, float],
    allocations: Mapping[AttributionKey, float],
) -> None:
    """Accumulate one allocation mapping into a composite cumulative map."""
    for key, amount in allocations.items():
        target[key] += amount


def _reconcile_weights(
    weights: Mapping[AttributionKey, float],
    target_total: float,
) -> dict[AttributionKey, float]:
    """Reconcile positive quantity weights or fail closed to unattributed.

    The final stable key absorbs the quantity residue. If that residue cannot
    remain strictly positive or cannot make the stable-order float sum exact,
    the pool's ownership is no longer representable and the whole current
    quantity is withheld in the unattributed bucket.

    Args:
        weights: Candidate composite ownership quantities.
        target_total: Absolute aggregate position quantity.

    Returns:
        Exact positive weights, or one unattributed weight for ``target_total``.
    """
    if target_total < FLAT_EPSILON:
        return {}
    if any(not math.isfinite(weight) or weight <= 0.0 for weight in weights.values()):
        return {_UNATTRIBUTED_KEY: target_total}
    keys = _sorted_attribution_keys(set(weights))
    if not keys:
        return {_UNATTRIBUTED_KEY: target_total}
    reconciled = _values_with_residue(weights, keys, target_total)
    if reconciled is None or any(
        not math.isfinite(weight) or weight <= 0.0 for weight in reconciled.values()
    ):
        return {_UNATTRIBUTED_KEY: target_total}
    return reconciled


def _remaining_weights(
    weights: Mapping[AttributionKey, float],
    closed_qty: float,
    remaining_qty: float,
) -> dict[AttributionKey, float]:
    """Subtract a pro-rata close and reconcile the surviving quantity weights."""
    if remaining_qty < FLAT_EPSILON:
        return {}
    closed = _allocate_by_weights(closed_qty, weights)
    remaining = {
        key: weights.get(key, 0.0) - closed.get(key, 0.0) for key in set(weights) | set(closed)
    }
    return _reconcile_weights(remaining, remaining_qty)


def _values_with_residue(
    values: Mapping[AttributionKey, float],
    keys: Sequence[AttributionKey],
    total: float,
) -> dict[AttributionKey, float] | None:
    """Reconcile values through the final key or fail if floats cannot represent it.

    The accumulated final value is tried first. Its adjacent floats are also
    tried because the later stable-order sum can round in the opposite direction
    by one unit in the last place. A direct residual farther away is not proof of
    ownership and is never transported into the final bucket. If no allowed
    representation sums exactly, returning ``None`` lets the caller withhold the
    point instead of publishing fabricated attribution.

    Args:
        values: Cumulative values before point-level reconciliation.
        keys: Stable-sorted composite keys.
        total: Aggregate value the buckets must equal exactly.

    Returns:
        Reconciled values, or ``None`` when exact float reconciliation fails.
    """
    if not keys:
        return {}
    reconciled: dict[AttributionKey, float] = {}
    for key in keys[:-1]:
        reconciled[key] = values.get(key, 0.0)
    final_key = keys[-1]
    original = values.get(final_key, 0.0)
    if not math.isfinite(total) or not math.isfinite(original):
        return None
    candidates = (
        original,
        math.nextafter(original, math.inf),
        math.nextafter(original, -math.inf),
    )
    for candidate in candidates:
        reconciled[final_key] = candidate
        if sum(reconciled[key] for key in keys) == total:
            return reconciled
    return None


def _minute_grid(from_time: datetime, to_time: datetime) -> list[datetime]:
    """Build the inclusive 1m grid from ``from_time`` (floored) to ``to_time``."""
    current = from_time.replace(second=0, microsecond=0)
    grid: list[datetime] = []
    while current <= to_time:
        grid.append(current)
        current += timedelta(minutes=1)
    return grid


def _prepare_executions(
    executions: Sequence[TimelineExecution],
) -> tuple[list[_PreparedExecution], list[tuple[datetime, datetime]]]:
    """Clamp per-instrument event times monotone and collect regression shadows.

    Each instrument's fills are accumulated in the caller-supplied scope order.
    A fill whose ``event_time`` regresses below the instrument's running maximum
    is placed at that maximum (so scope order survives the grid walk) and the
    interval it shadows is recorded so those minutes can be flagged incomplete.

    Args:
        executions: Executions in ``(exchange, scope_sequence)`` order.

    Returns:
        The prepared executions sorted for the grid walk, and the shadowed
        ``[economic_time, clamped_time)`` intervals produced by regressions.
    """
    last_effective: dict[str, datetime] = {}
    shadows: list[tuple[datetime, datetime]] = []
    prepared: list[_PreparedExecution] = []
    for execution in executions:
        previous = last_effective.get(execution.instrument_public_id)
        if previous is not None and execution.event_time < previous:
            effective = previous
            shadows.append((execution.event_time, previous))
        else:
            effective = execution.event_time
        last_effective[execution.instrument_public_id] = effective
        prepared.append(
            _PreparedExecution(
                effective_time=effective,
                exchange=execution.exchange,
                scope_sequence=execution.scope_sequence,
                execution=execution,
            )
        )
    prepared.sort(key=lambda item: (item.effective_time, item.exchange, item.scope_sequence))
    return prepared, shadows


def _untrusted_point(
    point_time: datetime,
    seen: Sequence[str],
    attribution_keys: Sequence[AttributionKey],
) -> PnlTimelinePoint:
    """Build a fully-untrusted incomplete point (all components withheld).

    Used when this minute's cumulatives themselves cannot be trusted — a
    scope-order regression shadows it, or an unknown seeded cost basis is still
    in play — so no realized / fee / accrual / unrealized number is defensible.

    Args:
        point_time: The grid instant.
        seen: Instruments to list (all with null contributions).
        attribution_keys: Composite buckets to list with null contributions.

    Returns:
        An incomplete :class:`PnlTimelinePoint` with every component ``None``.
    """
    contributions = tuple(
        PnlInstrumentContribution(
            instrument_public_id=instrument_public_id,
            realized_pnl=None,
            fee_pnl=None,
            accrual_pnl=None,
            unrealized_pnl=None,
        )
        for instrument_public_id in seen
    )
    attribution = tuple(
        PnlAttributionContribution(
            origin=origin,
            strategy_name=strategy_name,
            realized_pnl=None,
            fee_pnl=None,
            accrual_pnl=None,
            unrealized_pnl=None,
        )
        for origin, strategy_name in attribution_keys
    )
    return PnlTimelinePoint(
        point_time=point_time,
        realized_pnl=None,
        fee_pnl=None,
        accrual_pnl=None,
        unrealized_pnl=None,
        net_pnl=None,
        valuation_status="incomplete",
        per_instrument=contributions,
        attribution=attribution,
    )


def _value_point(
    point_time: datetime,
    pools: Mapping[str, _Pool],
    weights_by_instrument: Mapping[str, Mapping[AttributionKey, float]],
    marks: MarkMap,
    seen: Sequence[str],
    attribution_seen: Sequence[AttributionKey],
    realized_by_instrument: Mapping[str, float],
    fee_by_instrument: Mapping[str, float],
    accrual_by_instrument: Mapping[str, float],
    realized_by_attribution: Mapping[AttributionKey, float],
    fee_by_attribution: Mapping[AttributionKey, float],
    accrual_by_attribution: Mapping[AttributionKey, float],
    realized_total: float,
    fee_total: float,
    accrual_total: float,
    opening_unrealized_value: float,
    cumulatives_untrusted: bool,
) -> PnlTimelinePoint:
    """Value one grid point from the current pools, cumulatives, and marks.

    Args:
        point_time: The grid instant being valued.
        pools: Current per-instrument pool state.
        weights_by_instrument: Current composite quantity weights per pool.
        marks: The injected USD mark lookup.
        seen: Instruments with any activity or seeding through this point,
            already ordered by the caller for deterministic output.
        attribution_seen: Composite buckets observed through this point.
        realized_by_instrument: Cumulative realized per instrument.
        fee_by_instrument: Cumulative fee P&L per instrument (expense sign).
        accrual_by_instrument: Cumulative accrual P&L per instrument.
        realized_by_attribution: Cumulative realized per composite bucket.
        fee_by_attribution: Cumulative fee P&L per composite bucket.
        accrual_by_attribution: Cumulative accrual per composite bucket.
        realized_total: Aggregate cumulative realized.
        fee_total: Aggregate cumulative fee P&L.
        accrual_total: Aggregate cumulative accrual P&L.
        opening_unrealized_value: The anchor's opening unrealized value.
        cumulatives_untrusted: Whether the cumulative components themselves are
            untrusted at this minute (scope-order regression or unknown basis),
            forcing every component — not just the unrealized — to be withheld.

    Returns:
        The valued :class:`PnlTimelinePoint`.
    """
    if cumulatives_untrusted or not (
        math.isfinite(realized_total)
        and math.isfinite(fee_total)
        and math.isfinite(accrual_total)
        and math.isfinite(opening_unrealized_value)
    ):
        return _untrusted_point(point_time, seen, attribution_seen)
    if any(
        not math.isfinite(realized_by_instrument.get(instrument_public_id, 0.0))
        or not math.isfinite(fee_by_instrument.get(instrument_public_id, 0.0))
        or not math.isfinite(accrual_by_instrument.get(instrument_public_id, 0.0))
        for instrument_public_id in seen
    ):
        return _untrusted_point(point_time, seen, attribution_seen)
    attribution_keys = _sorted_attribution_keys(
        set(attribution_seen)
        | set(realized_by_attribution)
        | set(fee_by_attribution)
        | set(accrual_by_attribution)
    )
    if any(
        not math.isfinite(realized_by_attribution.get(key, 0.0))
        or not math.isfinite(fee_by_attribution.get(key, 0.0))
        or not math.isfinite(accrual_by_attribution.get(key, 0.0))
        for key in attribution_keys
    ):
        return _untrusted_point(point_time, seen, attribution_keys)
    unrealized_total = 0.0
    incomplete = False
    contributions: list[PnlInstrumentContribution] = []
    unrealized_by_attribution: defaultdict[AttributionKey, float] = defaultdict(float)
    unrealized_incomplete: set[AttributionKey] = set()
    for instrument_public_id in seen:
        pool = pools.get(instrument_public_id, _Pool(0.0, None))
        realized = realized_by_instrument.get(instrument_public_id, 0.0)
        fee = fee_by_instrument.get(instrument_public_id, 0.0)
        accrual = accrual_by_instrument.get(instrument_public_id, 0.0)
        if abs(pool.position_qty) < FLAT_EPSILON:
            instrument_unrealized: float | None = 0.0
        else:
            mark = marks.get((instrument_public_id, point_time))
            if (
                mark is None
                or not math.isfinite(mark)
                or pool.entry_price is None
                or not math.isfinite(pool.entry_price)
            ):
                instrument_unrealized = None
                incomplete = True
                instrument_keys = _sorted_attribution_keys(
                    set(weights_by_instrument.get(instrument_public_id, {}))
                )
                unrealized_incomplete.update(instrument_keys or [_UNATTRIBUTED_KEY])
            else:
                instrument_unrealized = pool.position_qty * (mark - pool.entry_price)
                if math.isfinite(instrument_unrealized):
                    unrealized_total += instrument_unrealized
                    allocations = _allocate_by_weights(
                        instrument_unrealized,
                        weights_by_instrument.get(instrument_public_id, {}),
                    )
                    for key, amount in allocations.items():
                        unrealized_by_attribution[key] += amount
                        if not math.isfinite(unrealized_by_attribution[key]):
                            unrealized_incomplete.add(key)
                else:
                    instrument_unrealized = None
                    incomplete = True
                    instrument_keys = _sorted_attribution_keys(
                        set(weights_by_instrument.get(instrument_public_id, {}))
                    )
                    unrealized_incomplete.update(instrument_keys or [_UNATTRIBUTED_KEY])
        contributions.append(
            PnlInstrumentContribution(
                instrument_public_id=instrument_public_id,
                realized_pnl=realized,
                fee_pnl=fee,
                accrual_pnl=accrual,
                unrealized_pnl=instrument_unrealized,
            )
        )
    attribution_keys = _sorted_attribution_keys(
        set(attribution_keys)
        | set(unrealized_by_attribution)
        | set(unrealized_incomplete)
        | {
            key
            for instrument_weights in weights_by_instrument.values()
            for key in instrument_weights
        }
    )
    if unrealized_incomplete:
        incomplete = True
    realized_attribution = _values_with_residue(
        realized_by_attribution, attribution_keys, realized_total
    )
    fee_attribution = _values_with_residue(fee_by_attribution, attribution_keys, fee_total)
    accrual_attribution = _values_with_residue(
        accrual_by_attribution, attribution_keys, accrual_total
    )
    if realized_attribution is None or fee_attribution is None or accrual_attribution is None:
        return _untrusted_point(point_time, seen, attribution_keys)
    if not incomplete and math.isfinite(unrealized_total) and not unrealized_incomplete:
        reconciled_unrealized = _values_with_residue(
            unrealized_by_attribution, attribution_keys, unrealized_total
        )
        if reconciled_unrealized is None:
            return _untrusted_point(point_time, seen, attribution_keys)
    else:
        reconciled_unrealized = dict(unrealized_by_attribution)
    attribution = tuple(
        PnlAttributionContribution(
            origin=origin,
            strategy_name=strategy_name,
            realized_pnl=realized_attribution[(origin, strategy_name)],
            fee_pnl=fee_attribution[(origin, strategy_name)],
            accrual_pnl=accrual_attribution[(origin, strategy_name)],
            unrealized_pnl=(
                None
                if (origin, strategy_name) in unrealized_incomplete
                else reconciled_unrealized.get((origin, strategy_name), 0.0)
            ),
        )
        for origin, strategy_name in attribution_keys
    )
    if incomplete:
        return PnlTimelinePoint(
            point_time=point_time,
            realized_pnl=realized_total,
            fee_pnl=fee_total,
            accrual_pnl=accrual_total,
            unrealized_pnl=None,
            net_pnl=None,
            valuation_status="incomplete",
            per_instrument=tuple(contributions),
            attribution=attribution,
        )
    net = realized_total + fee_total + accrual_total + (unrealized_total - opening_unrealized_value)
    if incomplete or not math.isfinite(unrealized_total) or not math.isfinite(net):
        return PnlTimelinePoint(
            point_time=point_time,
            realized_pnl=realized_total,
            fee_pnl=fee_total,
            accrual_pnl=accrual_total,
            unrealized_pnl=None,
            net_pnl=None,
            valuation_status="incomplete",
            per_instrument=tuple(contributions),
            attribution=attribution,
        )
    return PnlTimelinePoint(
        point_time=point_time,
        realized_pnl=realized_total,
        fee_pnl=fee_total,
        accrual_pnl=accrual_total,
        unrealized_pnl=unrealized_total,
        net_pnl=net,
        valuation_status="complete",
        per_instrument=tuple(contributions),
        attribution=attribution,
    )


def _downsample(points: Sequence[PnlTimelinePoint], step: int) -> list[PnlTimelinePoint]:
    """Reduce the 1m series to the requested bucket width by endpoint selection.

    Each bucket of ``step`` consecutive 1m points is represented by its last
    point. The cumulative flow components at that endpoint equal the summed
    per-minute deltas across the bucket, and the stock components are the
    endpoint values, so one endpoint selection satisfies both the flow and stock
    downsampling rules.

    Args:
        points: The ordered 1m points.
        step: Bucket width in minutes (``1`` returns the series unchanged).

    Returns:
        The downsampled points.
    """
    result: list[PnlTimelinePoint] = []
    for start in range(0, len(points), step):
        result.append(points[min(start + step, len(points)) - 1])
    return result


def build_pnl_timeline(
    executions: Sequence[TimelineExecution],
    accruals: Sequence[TimelineAccrual],
    marks: MarkMap,
    window: TimelineWindow,
    opening: TimelineOpening | None = None,
    lineage: Mapping[str, TimelineExecutionLineage] | None = None,
    untrusted_price_instruments: Collection[str] = (),
) -> PnlTimelineResult:
    """Build the Net-P&L-since-activation series for one wallet/mode scope.

    Seeds pools from ``opening`` (never from ``window.from_time``), replays every
    execution through the shared average-cost kernel accumulating realized and
    fee components, layers in funding accruals as a separate component, values
    open positions at each minute against the injected marks, and downsamples to
    the requested granularity. See the module docstring for the full soundness
    contract (honest-incomplete marks, baseline-leakage guard, ordering, and the
    downsampling reduction).

    Args:
        executions: Executions for the scope in ``(exchange, scope_sequence)``
            order, bounded by the caller to the post-anchor watermark.
        accruals: Funding accruals for the scope, USD-signed.
        marks: The injected ``(instrument_public_id, minute)`` USD mark lookup.
        window: The requested from/to/granularity/valuation-currency window.
        opening: The activation anchor seeding the replay, or ``None`` to replay
            from empty pools with a zero opening unrealized value.
        lineage: Order-keyed initiating command and signal lineage. Missing or
            ambiguous orders are intentionally absent and become unattributed.
        untrusted_price_instruments: Instrument identities whose execution-price
            denomination the caller could not prove equals the valuation
            currency. Their fills are never passed to the accounting kernel and
            latch the existing fully-untrusted tier when replay reaches them.

    Returns:
        The built :class:`PnlTimelineResult` at the requested granularity.

    Raises:
        ValueError: When ``window.granularity`` is not a supported value.
    """
    step = _GRANULARITY_MINUTES.get(window.granularity)
    if step is None:
        raise ValueError(f"unsupported granularity: {window.granularity!r}")

    pools: dict[str, _Pool] = {}
    weights_by_instrument: dict[str, dict[AttributionKey, float]] = {}
    resolved_lineage = {} if lineage is None else lineage
    resolved_untrusted_price_instruments = frozenset(untrusted_price_instruments)
    opening_unrealized_value = 0.0
    seen: set[str] = set()
    attribution_seen: set[AttributionKey] = set()
    basis_unknown: set[str] = set()
    realized_untrusted: set[str] = set()
    activation_time: datetime | None = None
    if opening is not None:
        for instrument_public_id, seed in opening.positions.items():
            pools[instrument_public_id] = _Pool(seed.position_qty, seed.entry_price)
            seen.add(instrument_public_id)
            if not math.isfinite(seed.position_qty):
                realized_untrusted.add(instrument_public_id)
            else:
                if abs(seed.position_qty) > 0.0:
                    weights_by_instrument[instrument_public_id] = {
                        _UNATTRIBUTED_KEY: abs(seed.position_qty)
                    }
                    attribution_seen.add(_UNATTRIBUTED_KEY)
                if seed.entry_price is None and abs(seed.position_qty) >= FLAT_EPSILON:
                    basis_unknown.add(instrument_public_id)
        opening_unrealized_value = opening.opening_unrealized_value
        activation_time = opening.t0

    prepared, shadows = _prepare_executions(executions)
    sorted_accruals = sorted(
        (
            accrual
            for accrual in accruals
            if activation_time is None or accrual.accrued_at >= activation_time
        ),
        key=lambda item: (item.accrued_at, item.instrument_public_id),
    )

    realized_by_instrument: defaultdict[str, float] = defaultdict(float)
    fee_by_instrument: defaultdict[str, float] = defaultdict(float)
    accrual_by_instrument: defaultdict[str, float] = defaultdict(float)
    realized_by_attribution: defaultdict[AttributionKey, float] = defaultdict(float)
    fee_by_attribution: defaultdict[AttributionKey, float] = defaultdict(float)
    accrual_by_attribution: defaultdict[AttributionKey, float] = defaultdict(float)
    realized_total = 0.0
    fee_total = 0.0
    accrual_total = 0.0

    execution_index = 0
    accrual_index = 0
    minute_points: list[PnlTimelinePoint] = []
    for point_time in _minute_grid(window.from_time, window.to_time):
        while (
            execution_index < len(prepared)
            and prepared[execution_index].effective_time <= point_time
        ):
            execution = prepared[execution_index].execution
            instrument_public_id = execution.instrument_public_id
            execution_index += 1
            seen.add(instrument_public_id)
            attribution_key = _execution_attribution(execution, resolved_lineage)
            attribution_seen.add(attribution_key)
            if instrument_public_id in resolved_untrusted_price_instruments:
                realized_untrusted.add(instrument_public_id)
                continue
            if not math.isfinite(execution.size) or execution.size < 0.0:
                realized_untrusted.add(instrument_public_id)
                continue
            signed_qty = execution.size if execution.side == "buy" else -execution.size
            pool = pools.get(instrument_public_id, _Pool(0.0, None))
            pre_fill_weights = dict(weights_by_instrument.get(instrument_public_id, {}))
            price_is_trusted = is_positive_finite(execution.price)
            outcome = apply_fill(
                pool.position_qty,
                pool.entry_price,
                signed_qty,
                execution.size,
                execution.price if price_is_trusted else math.nan,
            )
            if not price_is_trusted and outcome.closed_qty > 0.0:
                realized_untrusted.add(instrument_public_id)
                continue
            pools[instrument_public_id] = _Pool(outcome.position_qty, outcome.entry_price)
            opened_opposite_side = outcome.closed_qty > 0.0 and outcome.opened_new_side
            realized_by_instrument[instrument_public_id] += outcome.realized_delta
            realized_total += outcome.realized_delta
            if outcome.closed_qty > 0.0:
                realized_allocation = _allocate_by_weights(outcome.realized_delta, pre_fill_weights)
                _add_allocations(realized_by_attribution, realized_allocation)
                attribution_seen.update(realized_allocation)
            fee_pnl = -execution.fee
            fee_by_instrument[instrument_public_id] += fee_pnl
            fee_total += fee_pnl
            if opened_opposite_side:
                closing_fee_pnl = fee_pnl * outcome.closed_qty / execution.size
                closing_fee_allocation = _allocate_by_weights(closing_fee_pnl, pre_fill_weights)
                _add_allocations(fee_by_attribution, closing_fee_allocation)
                attribution_seen.update(closing_fee_allocation)
                fee_by_attribution[attribution_key] += fee_pnl - closing_fee_pnl
            elif outcome.closed_qty > 0.0:
                closing_fee_allocation = _allocate_by_weights(fee_pnl, pre_fill_weights)
                _add_allocations(fee_by_attribution, closing_fee_allocation)
                attribution_seen.update(closing_fee_allocation)
            else:
                fee_by_attribution[attribution_key] += fee_pnl
            if opened_opposite_side:
                post_fill_weights = {attribution_key: outcome.added_qty}
            elif outcome.closed_qty > 0.0:
                post_fill_weights = _remaining_weights(
                    pre_fill_weights,
                    outcome.closed_qty,
                    abs(outcome.position_qty),
                )
            elif outcome.added_qty > 0.0:
                post_fill_weights = dict(pre_fill_weights)
                post_fill_weights[attribution_key] = (
                    post_fill_weights.get(attribution_key, 0.0) + outcome.added_qty
                )
            else:
                post_fill_weights = pre_fill_weights
            weights_by_instrument[instrument_public_id] = _reconcile_weights(
                post_fill_weights, abs(outcome.position_qty)
            )
            attribution_seen.update(weights_by_instrument[instrument_public_id])
            if instrument_public_id in basis_unknown:
                if outcome.closed_qty > 0.0:
                    realized_untrusted.add(instrument_public_id)
                if abs(outcome.position_qty) < FLAT_EPSILON:
                    basis_unknown.discard(instrument_public_id)
        while (
            accrual_index < len(sorted_accruals)
            and sorted_accruals[accrual_index].accrued_at <= point_time
        ):
            accrual = sorted_accruals[accrual_index]
            accrual_by_instrument[accrual.instrument_public_id] += -accrual.amount_usd
            accrual_pnl = -accrual.amount_usd
            accrual_total += accrual_pnl
            seen.add(accrual.instrument_public_id)
            accrual_weights = weights_by_instrument.get(accrual.instrument_public_id, {})
            accrual_allocation = _allocate_by_weights(accrual_pnl, accrual_weights)
            _add_allocations(accrual_by_attribution, accrual_allocation)
            attribution_seen.update(accrual_allocation)
            accrual_index += 1
        tainted = any(start <= point_time < end for start, end in shadows)
        before_activation = activation_time is not None and point_time < activation_time
        cumulatives_untrusted = (
            tainted or before_activation or bool(basis_unknown) or bool(realized_untrusted)
        )
        minute_points.append(
            _value_point(
                point_time,
                pools,
                weights_by_instrument,
                marks,
                sorted(seen),
                _sorted_attribution_keys(attribution_seen),
                realized_by_instrument,
                fee_by_instrument,
                accrual_by_instrument,
                realized_by_attribution,
                fee_by_attribution,
                accrual_by_attribution,
                realized_total,
                fee_total,
                accrual_total,
                opening_unrealized_value,
                cumulatives_untrusted,
            )
        )

    return PnlTimelineResult(
        points=tuple(_downsample(minute_points, step)),
        granularity=window.granularity,
        valuation_ccy=window.valuation_ccy,
    )
