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
  from the seeded book); a fill carries a non-finite or NEGATIVE size, or a seed
  carries a non-finite quantity (a corrupt or sign-inverted quantity is tainted at
  ingestion rather than replayed, so it can never fabricate a signed position or a
  bogus realized number); or any monetary value — an aggregate cumulative total, a
  PER-INSTRUMENT cumulative (realized / fee / accrual), the opening baseline, a
  per-instrument entry or unrealized, or the summed aggregate unrealized / net — is
  non-finite, whether a NaN/Inf price / fee / mark arrived directly or a VWAP entry
  or a sum overflowed from otherwise-finite inputs (including per-instrument
  overflow that interleaved cancellation hides from the aggregate). No ``complete``
  point ever carries a non-finite or fabricated number, at any level.
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
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from typing import Final
from typing import Literal

from snapper.application.portfolio.average_cost import FLAT_EPSILON
from snapper.application.portfolio.average_cost import apply_fill

ValuationStatus = Literal["complete", "incomplete"]
"""Whether a point's mark-to-market valuation is trustworthy or withheld."""

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
    """

    point_time: datetime
    realized_pnl: float | None
    fee_pnl: float | None
    accrual_pnl: float | None
    unrealized_pnl: float | None
    net_pnl: float | None
    valuation_status: ValuationStatus
    per_instrument: tuple[PnlInstrumentContribution, ...]


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


def _untrusted_point(point_time: datetime, seen: Sequence[str]) -> PnlTimelinePoint:
    """Build a fully-untrusted incomplete point (all components withheld).

    Used when this minute's cumulatives themselves cannot be trusted — a
    scope-order regression shadows it, or an unknown seeded cost basis is still
    in play — so no realized / fee / accrual / unrealized number is defensible.

    Args:
        point_time: The grid instant.
        seen: Instruments to list (all with null contributions).

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
    return PnlTimelinePoint(
        point_time=point_time,
        realized_pnl=None,
        fee_pnl=None,
        accrual_pnl=None,
        unrealized_pnl=None,
        net_pnl=None,
        valuation_status="incomplete",
        per_instrument=contributions,
    )


def _value_point(
    point_time: datetime,
    pools: Mapping[str, _Pool],
    marks: MarkMap,
    seen: Sequence[str],
    realized_by_instrument: Mapping[str, float],
    fee_by_instrument: Mapping[str, float],
    accrual_by_instrument: Mapping[str, float],
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
        marks: The injected USD mark lookup.
        seen: Instruments with any activity or seeding through this point,
            already ordered by the caller for deterministic output.
        realized_by_instrument: Cumulative realized per instrument.
        fee_by_instrument: Cumulative fee P&L per instrument (expense sign).
        accrual_by_instrument: Cumulative accrual P&L per instrument.
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
        return _untrusted_point(point_time, seen)
    if any(
        not math.isfinite(realized_by_instrument.get(instrument_public_id, 0.0))
        or not math.isfinite(fee_by_instrument.get(instrument_public_id, 0.0))
        or not math.isfinite(accrual_by_instrument.get(instrument_public_id, 0.0))
        for instrument_public_id in seen
    ):
        return _untrusted_point(point_time, seen)
    unrealized_total = 0.0
    incomplete = False
    contributions: list[PnlInstrumentContribution] = []
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
            else:
                instrument_unrealized = pool.position_qty * (mark - pool.entry_price)
                if math.isfinite(instrument_unrealized):
                    unrealized_total += instrument_unrealized
                else:
                    instrument_unrealized = None
                    incomplete = True
        contributions.append(
            PnlInstrumentContribution(
                instrument_public_id=instrument_public_id,
                realized_pnl=realized,
                fee_pnl=fee,
                accrual_pnl=accrual,
                unrealized_pnl=instrument_unrealized,
            )
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

    Returns:
        The built :class:`PnlTimelineResult` at the requested granularity.

    Raises:
        ValueError: When ``window.granularity`` is not a supported value.
    """
    step = _GRANULARITY_MINUTES.get(window.granularity)
    if step is None:
        raise ValueError(f"unsupported granularity: {window.granularity!r}")

    pools: dict[str, _Pool] = {}
    opening_unrealized_value = 0.0
    seen: set[str] = set()
    basis_unknown: set[str] = set()
    realized_untrusted: set[str] = set()
    activation_time: datetime | None = None
    if opening is not None:
        for instrument_public_id, seed in opening.positions.items():
            pools[instrument_public_id] = _Pool(seed.position_qty, seed.entry_price)
            seen.add(instrument_public_id)
            if not math.isfinite(seed.position_qty):
                realized_untrusted.add(instrument_public_id)
            elif seed.entry_price is None and abs(seed.position_qty) >= FLAT_EPSILON:
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
            if not math.isfinite(execution.size) or execution.size < 0.0:
                realized_untrusted.add(instrument_public_id)
                continue
            signed_qty = execution.size if execution.side == "buy" else -execution.size
            pool = pools.get(instrument_public_id, _Pool(0.0, None))
            outcome = apply_fill(
                pool.position_qty,
                pool.entry_price,
                signed_qty,
                execution.size,
                execution.price,
            )
            pools[instrument_public_id] = _Pool(outcome.position_qty, outcome.entry_price)
            realized_by_instrument[instrument_public_id] += outcome.realized_delta
            realized_total += outcome.realized_delta
            fee_by_instrument[instrument_public_id] += -execution.fee
            fee_total += -execution.fee
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
            accrual_total += -accrual.amount_usd
            seen.add(accrual.instrument_public_id)
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
                marks,
                sorted(seen),
                realized_by_instrument,
                fee_by_instrument,
                accrual_by_instrument,
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
