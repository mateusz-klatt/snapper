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
  stable-sorted instrument and composite keys may each absorb a
  one-unit-in-the-last-place rounding residue at emission, so every exposed
  grouping reconciles exactly to its aggregate without independent maps choosing
  different residue owners. A larger discrepancy withholds the point instead of
  transporting cancellation residue into an unrelated bucket.
- **Seed from t0, never from ``from_time`` (checklist #3).** Pools are seeded
  from the ``opening`` anchor; cumulative realized / fee / accrual start at zero
  at t0. Every execution the caller supplies is replayed onto the seeded pools;
  executions whose economic time precedes ``from_time`` still mutate the pools
  (their effect is baked into the first emitted point) but emit no point of their
  own. The caller bounds the stream to the post-anchor watermark, so the builder
  does NOT additionally filter by t0 on the time axis (the scope-sequence
  watermark is the authoritative boundary; a time-axis filter would fight it
  under clock skew).
- **Baseline leakage guard (checklist #4).** Every surviving opening SHARD is
  rebased to its instrument's t0 mark before post-activation replay. Its
  activation unrealized is therefore exactly zero, partial/full closes realize
  only movement since activation, and ``net_pnl`` is the direct sum of the four
  public components. Historical entry and raw opening unrealized remain audit
  metadata on the derivation result; they are never hidden subtractions.
- **Ordering (checklist #2).** ``Execution.executed_at`` is nullable, so pool
  accumulation is ordered by ``scope_sequence`` (the caller supplies executions
  in ``(exchange, scope_sequence)`` order and this builder preserves each
  pool's sub-order). The TIME AXIS uses ``event_time`` (the execution
  ``timestamp``). When a pool's event times are non-monotonic against its
  scope order (a regression), the offending fill's effective grid time is clamped
  forward to preserve scope order, and the shadowed minutes — those at or after
  the fill's true economic time but before its clamped placement — are ``UNTRUSTED``
  (see below): their cumulatives would omit an economically-present fill, so every
  component is withheld, not just the unrealized. Executions and accruals are
  merged at exact event timestamps. An accrual uses only inventory from strictly
  earlier fills; if a position-changing fill for the same instrument shares its
  timestamp, the accrual value remains valid but its ownership is unattributed.
  Equal-time fills then retain ``(exchange, scope_sequence)`` order.
- **Two-tier incompleteness (checklist #7).** A point is ``incomplete`` for one of
  two reasons that differ in what is trustworthy:
  (a) MARK-incomplete — a held (non-flat) instrument has no finite mark for that
  minute — withholds only ``unrealized_pnl`` and ``net_pnl``; the cumulative
  realized / fee / accrual are still returned because they are mark-independent. A
  stale or non-finite (NaN/Inf) mark is never carried forward.
  (b) UNTRUSTED — withholds EVERY aggregate component (realized / fee / accrual /
  unrealized / net all NULL) because at least one cumulative cannot be trusted at
  that instant. Global triggers also withhold every per-instrument contribution;
  instrument-scoped triggers withhold only the affected instrument so independent
  finite contributions remain useful without fabricating a total. The triggers
  are: a scope-order regression shadows the minute; an unknown seeded cost basis
  is still in play (a non-flat opening position with no entry price, or ANY
  positive realization against one, which permanently taints that instrument's
  cumulative realized for the rest of the series and holds until the pool flushes
  fully flat); the minute precedes the activation ``t0`` (the anchor proves no
  position state before then, so a pre-``t0`` grid point must never be valued from
  the seeded book); a fill carries a non-finite or NEGATIVE size; a
  non-positive or non-finite execution price participates in a close, reduction,
  or flip; or a seed carries a non-finite quantity (a corrupt or sign-inverted
  quantity is tainted at ingestion rather than replayed, so it can never fabricate
  a signed position or a bogus realized number). A non-positive or non-finite price
  on an opening or same-side add instead makes only the entry basis unknown, so its
  still-provable cumulatives survive while unrealized and net are withheld. Any
  monetary value — an aggregate cumulative total, a
  PER-INSTRUMENT cumulative (realized / fee / accrual), a
  per-instrument entry or unrealized, or the summed aggregate unrealized / net — is
  non-finite, whether a NaN/Inf price / fee / mark arrived directly or a VWAP entry
  or a sum overflowed from otherwise-finite inputs (including per-instrument
  overflow that interleaved cancellation hides from the aggregate); or the caller
  cannot prove that an execution price is denominated in the requested valuation
  currency. No ``complete`` point ever carries a non-finite or fabricated number,
  at any level.
- **Activation baseline (checklist #3/#4/#6).** When an anchor is supplied, grid
  points before its ``t0`` are withheld as untrusted (no speculative backfill), and
  funding accruals at or before ``t0`` are dropped. The t0 grid point is the
  rebased opening before any post-watermark replay, so all public flows and net
  are exactly zero there. Opening pools are keyed by durable
  ``(instrument_public_id, shard_key)`` identity and rebased to their t0 marks.
  Clock-skewed post-watermark executions at or before t0 are retained and first
  become visible on a later grid point.
- **Downsampling (checklist #6).** The 1m series carries CUMULATIVE realized /
  fee / accrual (integrals since t0) and STOCK unrealized / net. Downsampling to
  5m / 1h / 1d selects the LAST 1m point of each bucket. For the cumulative flow
  components this endpoint value equals the sum of every per-minute delta from t0
  through the bucket end (the flow-preserving reduction the checklist demands);
  for the stock components it is the required endpoint value. A bucket's
  ``valuation_status`` is therefore its endpoint minute's status.
- **Work budget.** Pool keys are indexed once by instrument in deterministic
  order. Each minute visits each indexed pool at most once for valuation, and an
  accrual visits only its instrument's pools; neither path scans the full book
  per instrument or per accrual. Event replay is one exact-time merge over the
  bounded execution and accrual inputs. Regression shadows are reduced to
  boundary changes of one lexically deterministic active trigger, so future-only
  instruments never multiply minute work or public reason cardinality. The
  application service, not this pure kernel, owns request-span and
  instrument-count caps.

The API layer builds the injected ``marks`` mapping from finalized DB 1m candles
through ``Repository.get_pnl_timeline_candles``, using the close of the bar
``[M-1m, M)`` to value point ``M``. Foreign quotes use a direct or inverse rate
from the same finalized candle plane at that exact minute. This pure builder
consumes marks and execution prices already resolved into the valuation currency
and performs no candle reading or FX conversion itself.
"""

import math
from collections import defaultdict
from collections.abc import Collection
from collections.abc import Hashable
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timedelta
from heapq import heappop
from heapq import heappush
from typing import Final
from typing import Literal
from typing import cast

from snapper.application.portfolio.average_cost import FLAT_EPSILON
from snapper.application.portfolio.average_cost import PoolFillOutcome
from snapper.application.portfolio.average_cost import apply_fill
from snapper.core.numeric import is_positive_finite

ValuationStatus = Literal["complete", "incomplete"]
"""Whether a point's mark-to-market valuation is trustworthy or withheld."""

PnlIncompletenessReason = Literal[
    "scope_order_regression",
    "before_activation",
    "activation_baseline_non_finite",
    "fill_evidence_gap",
    "seed_quantity_non_finite",
    "cost_basis_unavailable",
    "execution_price_provenance_unproven",
    "execution_size_invalid",
    "execution_price_invalid",
    "fx_conversion_unproven",
    "mark_unavailable",
    "cumulative_non_finite",
    "unrealized_non_finite",
    "net_non_finite",
    "attribution_value_non_finite",
    "attribution_reconciliation_failed",
    "attribution_sum_unrepresentable",
    "instrument_reconciliation_failed",
    "instrument_sum_unrepresentable",
    "late_pre_activation_execution",
]
"""Closed causal taxonomy for a withheld P&L timeline value.

The two ``*_sum_unrepresentable`` members separate a refusal that carries NO
suspicion about the underlying money from the ``*_reconciliation_failed`` pair,
which does. A reconciliation failure means the buckets and the aggregate
disagree about how much money there is — an engine or evidence fault. An
unrepresentable sum means they agree exactly and no float assignment can express
that agreement, so the minute is withheld while the arithmetic remains sound.

The distinction is not cosmetic: a recompute that withholds a previously
published minute retracts its persisted row
(``pnl_snapshotter._reconcile_changed``), and doing that for a value the
arithmetic still believes would destroy history to satisfy a representation
limit. Only these two members are safe to preserve a row through.
"""

PnlWithholdingTier = Literal["mark_incomplete", "untrusted"]
"""Weakest honest withholding tier established at the causal site."""

PnlWithholdingScope = Literal["global", "instrument"]
"""Whether one instrument or the entire point is causally untrusted."""

OriginBucket = Literal["manual", "plan", "system", "unattributed"]
"""Proven initiating origin of an execution, or the fail-closed fallback."""

type AttributionKey = tuple[OriginBucket, str | None]
"""Composite origin and signal-derived strategy identity used for allocation."""

type PoolKey = tuple[str, str]
"""Stable ``(instrument_public_id, shard_key)`` accounting-pool identity."""

type PoolIndex = Mapping[str, tuple[PoolKey, ...]]
"""Deterministic pool keys grouped by stable instrument identity."""

type MarkMap = Mapping[tuple[str, datetime], float | None]
"""Valuation-currency mark lookup keyed by ``(instrument_public_id, point_minute)``.

The value is the mark used to value that instrument at grid point ``M`` — the
close of the candle covering ``[M-1m, M)``, converted by the caller at that exact
minute when necessary. A ``None`` value or an absent key both mean "no mark for
that instrument at that minute".
"""

type MarkIncompletenessReasonMap = Mapping[tuple[str, datetime], PnlIncompletenessReason]
"""Causal mark failures stamped by the caller at conversion sites."""

type OpeningMarkMap = Mapping[str, float | None]
"""Trusted t0 marks keyed by instrument identity for opening derivation."""

_GRANULARITY_MINUTES: Final[dict[str, int]] = {"1m": 1, "5m": 5, "1h": 60, "1d": 1440}
"""Downsampling bucket width in minutes per supported granularity."""


@dataclass(frozen=True)
class TimelineExecution:
    """One execution on the P&L time axis.

    Attributes:
        instrument_public_id: Stable instrument identity used for marks/output.
        shard_key: Durable fill-observed shard identity; with the instrument it
            forms the accounting pool key.
        exchange: Venue the fill executed on; part of the scope-order tiebreak.
        scope_sequence: Per-``(wallet, exchange, mode)`` commit-ordered counter;
            the authoritative accumulation order.
        event_time: The time-axis timestamp (execution ``timestamp``), used to
            place the fill on the minute grid.
        side: ``'buy'`` (positive signed quantity) or ``'sell'`` (negative).
        size: Unsigned fill quantity.
        position_delta: Caller-resolved signed base-inventory delta after any
            fee charged in the instrument's base asset.
        price: Execution price already resolved into the valuation currency.
        fee: Execution fee, assumed denominated in the window valuation currency
            after any API-layer conversion.
        fee_asset: Original fee denomination retained as provenance but not
            consumed by the pure math here.
        order_public_id: Immutable order identity used to resolve the caller's
            supplied command lineage.
        price_incompleteness_reason: Causal price-conversion failure stamped by
            the caller, or ``None`` when no caller-side failure was established.
        fee_incompleteness_reason: Causal fee-conversion failure stamped by the
            caller, or ``None`` when no caller-side failure was established.
    """

    instrument_public_id: str
    shard_key: str
    exchange: str
    scope_sequence: int
    event_time: datetime
    side: str
    size: float
    position_delta: float
    price: float
    fee: float
    fee_asset: str
    order_public_id: str
    price_incompleteness_reason: PnlIncompletenessReason | None = None
    fee_incompleteness_reason: PnlIncompletenessReason | None = None


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
        incompleteness_reason: Causal conversion failure stamped by the caller,
            or ``None`` when no caller-side failure was established.
    """

    instrument_public_id: str
    accrued_at: datetime
    amount_usd: float
    incompleteness_reason: PnlIncompletenessReason | None = None


@dataclass(frozen=True)
class OpeningPool:
    """One durable shard pool seeded at the activation anchor.

    Attributes:
        instrument_public_id: Stable instrument identity used for marks/output.
        shard_key: Exact durable ``fill_observed`` shard identity.
        exchange: Immutable execution scope of this shard.
        position_qty: Signed position quantity at t0.
        entry_price: Activation cost basis. A derived non-flat pool always carries
            its positive finite t0 mark here, never the historical venue basis.
            Invalid values remain accepted as defensive manually-constructed
            corrupt seeds so the builder can preserve honest incompleteness
            semantics.
    """

    instrument_public_id: str
    shard_key: str
    exchange: str
    position_qty: float
    entry_price: float | None


@dataclass(frozen=True)
class TimelineOpening:
    """The activation anchor seeding the replay.

    Attributes:
        pools: Stable ``(instrument_public_id, shard_key)`` ordered shard seeds.
            Non-flat derived pools are rebased to their instrument's t0 mark.
        t0: The anchor instant. Metadata only: the caller already bounds the
            supplied stream to the post-anchor watermark, so t0 is not used to
            re-filter events on the time axis.
    """

    pools: tuple[OpeningPool, ...]
    t0: datetime

    def __post_init__(self) -> None:
        """Require one stable, unambiguous pool tuple at a UTC grid minute."""
        if self.t0.utcoffset() != timedelta(0) or self.t0.second != 0 or self.t0.microsecond != 0:
            raise ValueError("opening t0 must be aligned to a UTC minute")
        expected = tuple(
            sorted(
                self.pools,
                key=lambda pool: (pool.instrument_public_id, pool.shard_key),
            )
        )
        if self.pools != expected:
            raise ValueError("opening pools must be stably ordered by instrument and shard")
        seen_keys: set[PoolKey] = set()
        scope_by_shard: dict[str, tuple[str, str]] = {}
        exchange_by_instrument: dict[str, str] = {}
        for pool in self.pools:
            if not pool.instrument_public_id or not pool.shard_key or not pool.exchange:
                raise ValueError("opening pool identities must be non-empty")
            pool_key = (pool.instrument_public_id, pool.shard_key)
            if pool_key in seen_keys:
                raise ValueError("opening pools must have unique instrument and shard keys")
            seen_keys.add(pool_key)
            scope = (pool.instrument_public_id, pool.exchange)
            previous_scope = scope_by_shard.setdefault(pool.shard_key, scope)
            if previous_scope != scope:
                raise ValueError("one opening shard cannot span instrument or exchange scopes")
            previous_exchange = exchange_by_instrument.setdefault(
                pool.instrument_public_id,
                pool.exchange,
            )
            if previous_exchange != pool.exchange:
                raise ValueError("one opening instrument cannot span multiple exchanges")


@dataclass(frozen=True)
class OpeningPoolValuation:
    """One surviving historical shard pool and its t0 valuation audit.

    Attributes:
        instrument_public_id: Stable instrument identity used for valuation.
        shard_key: Exact durable accounting shard.
        exchange: Immutable execution scope of the shard.
        position_qty: Historical signed quantity surviving the exact prefix.
        historical_entry_price: Historical average-cost basis before rebasing.
        t0_mark: Finite valuation-currency mark at the anchor instant.
        opening_unrealized_value: Signed ``quantity * (mark - entry)`` value.
    """

    instrument_public_id: str
    shard_key: str
    exchange: str
    position_qty: float
    historical_entry_price: float
    t0_mark: float
    opening_unrealized_value: float


@dataclass(frozen=True)
class TimelineOpeningDerivation:
    """A rebased activation opening and its historical per-pool audit.

    Attributes:
        opening: Seed accepted by :func:`build_pnl_timeline`.
        per_pool: Stable pool-key ordered historical valuations.
        raw_opening_unrealized_value: Accurate ``math.fsum`` of the stable
            per-pool historical opening values. Audit metadata only; never
            subtracted by the builder.
    """

    opening: TimelineOpening
    per_pool: tuple[OpeningPoolValuation, ...]
    raw_opening_unrealized_value: float


def derive_timeline_opening(
    executions: Sequence[TimelineExecution],
    t0_marks: OpeningMarkMap,
    t0: datetime,
    untrusted_price_reasons_by_instrument: (
        Mapping[str, Collection[PnlIncompletenessReason]] | None
    ) = None,
) -> TimelineOpeningDerivation:
    """Derive a shard-aware rebased opening from an exact ledger prefix.

    Replays every execution in its supplied per-exchange scope order through
    the same average-cost kernel used by the timeline, independently for every
    durable ``(instrument_public_id, shard_key)`` pool. The caller-resolved
    ``position_delta`` is authoritative, including base-asset fee quantity
    effects. Only non-flat pools survive. Their historical basis and raw t0
    unrealized are retained in the returned audit, while each build seed is
    rebased to the positive finite t0 mark. A same-shard round trip can therefore
    disappear, but opposing non-flat shards on one instrument never cancel.

    The caller remains responsible for proving that ``executions`` contains the
    complete active prefix through its captured per-exchange watermarks and that
    execution prices and t0 marks use the same valuation currency. This pure
    function can verify ordering and numeric trust but cannot detect an omitted
    database row.

    Args:
        executions: Exact active ledger prefix, ordered by scope sequence within
            each exchange.
        t0_marks: Caller-resolved t0 marks keyed by instrument identity.
        t0: Time represented by the opening valuation.
        untrusted_price_reasons_by_instrument: Caller-established price
            provenance failures. Any reason affecting a replayed instrument
            refuses the derivation even when its numeric price appears finite.

    Returns:
        The rebased opening and deterministic historical per-pool audit.

    Raises:
        ValueError: When temporal, ordering, shard scope, side, numeric, price
            provenance, surviving basis, mark, or valuation evidence is unsafe.
    """
    _validate_opening_time(t0)
    state = _OpeningReplayState()
    untrusted_reasons = untrusted_price_reasons_by_instrument or {}
    for execution in executions:
        _validate_opening_execution(state, execution, untrusted_reasons)
        _apply_opening_execution(state, execution)
    return _build_opening_derivation(state, t0_marks, t0)


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
        native_symbol: Native symbol proven by the service at the response
            horizon, or ``None`` when no single identity is defensible.
        exchange: Canonical source venue paired with ``native_symbol``, or
            ``None`` when the identity is unresolved.
        realized_pnl: Cumulative price-realized P&L since t0 for this instrument,
            or ``None`` when a global guard or this instrument's cumulative is
            untrusted.
        fee_pnl: Cumulative fee P&L since t0 (expense sign) for this instrument,
            ``None`` under the same untrusted conditions.
        accrual_pnl: Cumulative funding accrual P&L since t0 for this instrument,
            ``None`` under the same untrusted conditions.
        unrealized_pnl: This instrument's mark-to-market unrealized at the point,
            ``0.0`` when flat, or ``None`` when held but its mark or seeded entry
            is unavailable (or the point's cumulatives are untrusted).
    """

    instrument_public_id: str
    native_symbol: str | None
    exchange: str | None
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
class PnlIncompletenessReasonEntry:
    """One causal reason established by an actual withholding site."""

    reason: PnlIncompletenessReason
    withholding_tier: PnlWithholdingTier
    withholding_scope: PnlWithholdingScope
    trigger_instrument_public_id: str | None

    def __post_init__(self) -> None:
        """Reject instrument-scoped claims without a proven instrument identity."""
        if self.withholding_scope == "instrument" and self.trigger_instrument_public_id is None:
            raise ValueError("instrument-scoped incompleteness requires a triggering instrument")


def _incompleteness_reason_sort_key(
    entry: PnlIncompletenessReasonEntry,
) -> tuple[str, str, str, str]:
    """Return the stable scope, instrument, tier, and reason ordering."""
    return (
        entry.withholding_scope,
        entry.trigger_instrument_public_id or "",
        entry.withholding_tier,
        entry.reason,
    )


def canonical_incompleteness_reasons(
    reasons: Collection[PnlIncompletenessReasonEntry],
) -> tuple[PnlIncompletenessReasonEntry, ...]:
    """Deduplicate and deterministically order established causal reasons.

    Args:
        reasons: Causal entries stamped by withholding sites.

    Returns:
        Unique entries ordered by scope, trigger instrument, tier, and reason.
    """
    return tuple(sorted(set(reasons), key=_incompleteness_reason_sort_key))


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
        unrealized_pnl: Aggregate activation-relative mark-to-market unrealized at
            the point, or ``None`` when the point is incomplete.
        net_pnl: ``realized_pnl + fee_pnl + accrual_pnl + unrealized_pnl``, or
            ``None`` when the point is incomplete.
        valuation_status: ``'complete'`` or ``'incomplete'``.
        incompleteness_reasons: Canonical reasons stamped at withholding sites.
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
    incompleteness_reasons: tuple[PnlIncompletenessReasonEntry, ...]
    per_instrument: tuple[PnlInstrumentContribution, ...]
    attribution: tuple[PnlAttributionContribution, ...]

    def __post_init__(self) -> None:
        """Enforce reason/status equivalence and canonical reason ordering."""
        canonical = canonical_incompleteness_reasons(self.incompleteness_reasons)
        if self.incompleteness_reasons != canonical:
            raise ValueError("incompleteness reasons must be deduplicated and sorted")
        if self.valuation_status == "complete" and canonical:
            raise ValueError("a complete point cannot carry incompleteness reasons")
        if self.valuation_status == "incomplete" and not canonical:
            raise ValueError("an incomplete point must carry an incompleteness reason")


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
    """Internal signed average-cost state for one durable shard pool."""

    position_qty: float
    entry_price: float | None


@dataclass
class _OpeningReplayState:
    """Mutable accounting and identity state for activation-prefix replay."""

    pools: dict[PoolKey, _Pool] = field(default_factory=dict)
    last_scope_sequence_by_exchange: dict[str, int] = field(default_factory=dict)
    exchange_by_instrument: dict[str, str] = field(default_factory=dict)
    scope_by_shard: dict[str, tuple[str, str]] = field(default_factory=dict)


def _is_exactly_zero(value: float) -> bool:
    """Return whether a float is exactly zero without widening tolerance."""
    return math.isclose(value, 0.0, rel_tol=0.0, abs_tol=0.0)


def _validate_opening_time(t0: datetime) -> None:
    """Require the activation anchor to be one UTC grid minute."""
    if t0.utcoffset() != timedelta(0) or t0.second != 0 or t0.microsecond != 0:
        raise ValueError("opening t0 must be aligned to a UTC minute")


def _validate_opening_execution_identity(
    state: _OpeningReplayState,
    execution: TimelineExecution,
) -> None:
    """Validate and record one prefix execution's durable scope ordering."""
    instrument_public_id = execution.instrument_public_id
    if not instrument_public_id:
        raise ValueError("opening execution instrument identity must be non-empty")
    if not execution.shard_key:
        raise ValueError("opening execution shard identity must be non-empty")
    if not execution.exchange:
        raise ValueError("opening execution exchange identity must be non-empty")
    previous_scope_sequence = state.last_scope_sequence_by_exchange.get(execution.exchange)
    if execution.scope_sequence <= 0 or (
        previous_scope_sequence is not None and execution.scope_sequence <= previous_scope_sequence
    ):
        raise ValueError("opening executions must have increasing positive scope sequences")
    state.last_scope_sequence_by_exchange[execution.exchange] = execution.scope_sequence
    previous_exchange = state.exchange_by_instrument.setdefault(
        instrument_public_id,
        execution.exchange,
    )
    if previous_exchange != execution.exchange:
        raise ValueError("one opening instrument cannot span multiple exchanges")
    shard_scope = (instrument_public_id, execution.exchange)
    previous_scope = state.scope_by_shard.setdefault(execution.shard_key, shard_scope)
    if previous_scope != shard_scope:
        raise ValueError("one opening shard cannot span instrument or exchange scopes")


def _validate_opening_execution_values(
    execution: TimelineExecution,
    untrusted_price_reasons_by_instrument: Mapping[
        str,
        Collection[PnlIncompletenessReason],
    ],
) -> None:
    """Reject unsafe side, quantity, and caller-stamped price evidence."""
    if execution.side not in {"buy", "sell"}:
        raise ValueError("opening execution side must be buy or sell")
    if not math.isfinite(execution.size) or execution.size < 0.0:
        raise ValueError("opening execution size must be non-negative and finite")
    if not math.isfinite(execution.position_delta):
        raise ValueError("opening execution position delta must be finite")
    if execution.side == "buy" and execution.position_delta < 0.0:
        raise ValueError("opening execution position delta must agree with side")
    if execution.side == "sell" and execution.position_delta > 0.0:
        raise ValueError("opening execution position delta must agree with side")
    if untrusted_price_reasons_by_instrument.get(execution.instrument_public_id):
        raise ValueError("opening execution price provenance must be trusted")


def _validate_opening_execution(
    state: _OpeningReplayState,
    execution: TimelineExecution,
    untrusted_price_reasons_by_instrument: Mapping[
        str,
        Collection[PnlIncompletenessReason],
    ],
) -> None:
    """Validate one activation-prefix execution without mutating pool math."""
    _validate_opening_execution_identity(state, execution)
    _validate_opening_execution_values(
        execution,
        untrusted_price_reasons_by_instrument,
    )


def _apply_opening_execution(
    state: _OpeningReplayState,
    execution: TimelineExecution,
) -> None:
    """Replay one validated non-zero prefix execution through average cost."""
    if _is_exactly_zero(execution.position_delta):
        return
    if execution.price_incompleteness_reason is not None:
        raise ValueError("opening execution price provenance must be trusted")
    if not is_positive_finite(execution.price):
        raise ValueError("opening execution price must be positive and finite")
    pool_key = (execution.instrument_public_id, execution.shard_key)
    pool = state.pools.get(pool_key, _Pool(0.0, None))
    outcome = apply_fill(
        pool.position_qty,
        pool.entry_price,
        execution.position_delta,
        abs(execution.position_delta),
        execution.price,
    )
    if not math.isfinite(outcome.position_qty):
        raise ValueError("opening position quantity arithmetic must remain finite")
    if abs(outcome.position_qty) >= FLAT_EPSILON and not is_positive_finite(outcome.entry_price):
        raise ValueError("opening cost basis arithmetic must remain positive and finite")
    state.pools[pool_key] = _Pool(outcome.position_qty, outcome.entry_price)


def _opening_pool_valuation(
    state: _OpeningReplayState,
    pool_key: PoolKey,
    t0_marks: OpeningMarkMap,
) -> tuple[OpeningPool, OpeningPoolValuation] | None:
    """Build one surviving rebased opening pool and historical audit value."""
    instrument_public_id, shard_key = pool_key
    pool = state.pools[pool_key]
    if abs(pool.position_qty) < FLAT_EPSILON:
        return None
    mark = t0_marks.get(instrument_public_id)
    if not is_positive_finite(mark):
        raise ValueError("surviving opening pool requires a positive finite t0 mark")
    entry_price = cast(float, pool.entry_price)
    resolved_mark = mark
    unrealized = pool.position_qty * (resolved_mark - entry_price)
    if not math.isfinite(unrealized):
        raise ValueError("opening pool unrealized value must be finite")
    exchange = state.scope_by_shard[shard_key][1]
    opening_pool = OpeningPool(
        instrument_public_id=instrument_public_id,
        shard_key=shard_key,
        exchange=exchange,
        position_qty=pool.position_qty,
        entry_price=resolved_mark,
    )
    valuation = OpeningPoolValuation(
        instrument_public_id=instrument_public_id,
        shard_key=shard_key,
        exchange=exchange,
        position_qty=pool.position_qty,
        historical_entry_price=entry_price,
        t0_mark=resolved_mark,
        opening_unrealized_value=unrealized,
    )
    return opening_pool, valuation


def _build_opening_derivation(
    state: _OpeningReplayState,
    t0_marks: OpeningMarkMap,
    t0: datetime,
) -> TimelineOpeningDerivation:
    """Build the deterministic opening and exact aggregate audit value."""
    valuations: list[OpeningPoolValuation] = []
    opening_pools: list[OpeningPool] = []
    for pool_key in sorted(state.pools):
        resolved = _opening_pool_valuation(state, pool_key, t0_marks)
        if resolved is None:
            continue
        opening_pool, valuation = resolved
        opening_pools.append(opening_pool)
        valuations.append(valuation)
    try:
        raw_opening_unrealized_value = math.fsum(
            valuation.opening_unrealized_value for valuation in valuations
        )
    except OverflowError as error:
        raise ValueError("opening total unrealized value must be finite") from error
    if not math.isfinite(raw_opening_unrealized_value):
        raise ValueError("opening total unrealized value must be finite")
    return TimelineOpeningDerivation(
        opening=TimelineOpening(pools=tuple(opening_pools), t0=t0),
        per_pool=tuple(valuations),
        raw_opening_unrealized_value=raw_opening_unrealized_value,
    )


@dataclass(frozen=True)
class _PreparedExecution:
    """An execution paired with its monotone-clamped effective grid time."""

    effective_time: datetime
    exchange: str
    scope_sequence: int
    execution: TimelineExecution


@dataclass(frozen=True)
class _RegressionShadow:
    """One globally withholding interval with its proven triggering instrument."""

    start: datetime
    end: datetime
    reason: PnlIncompletenessReasonEntry


_UNATTRIBUTED_KEY: Final[AttributionKey] = ("unattributed", None)
"""Composite fallback for lineage or inventory that cannot be proven."""

_MANUAL_SURFACES: Final[frozenset[str]] = frozenset({"mcp", "rest", "ws"})
"""Command ingress surfaces that prove a human initiated the order."""


def _global_incompleteness_reason(
    reason: PnlIncompletenessReason,
    tier: PnlWithholdingTier,
    trigger_instrument_public_id: str | None = None,
) -> PnlIncompletenessReasonEntry:
    """Build one globally scoped causal reason."""
    return PnlIncompletenessReasonEntry(
        reason=reason,
        withholding_tier=tier,
        withholding_scope="global",
        trigger_instrument_public_id=trigger_instrument_public_id,
    )


def _instrument_incompleteness_reason(
    reason: PnlIncompletenessReason,
    tier: PnlWithholdingTier,
    instrument_public_id: str,
) -> PnlIncompletenessReasonEntry:
    """Build one instrument-scoped causal reason."""
    return PnlIncompletenessReasonEntry(
        reason=reason,
        withholding_tier=tier,
        withholding_scope="instrument",
        trigger_instrument_public_id=instrument_public_id,
    )


def _add_instrument_untrusted_reason(
    reasons_by_instrument: dict[str, set[PnlIncompletenessReasonEntry]],
    instrument_public_id: str,
    reason: PnlIncompletenessReason,
) -> None:
    """Latch one instrument-scoped UNTRUSTED reason for later points."""
    reasons_by_instrument.setdefault(instrument_public_id, set()).add(
        _instrument_incompleteness_reason(reason, "untrusted", instrument_public_id)
    )


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
    the final key receives ``amount - allocated``.

    That residue rule does NOT make the shares sum back to ``amount``, and this
    docstring claimed for a long time that it did. Measured against this very
    function: the split is inexact in 86.8% of random valid pools, and the
    counterexample below reproduces exactly, hex for hex::

        amount  = float.fromhex("0x1.e9074940cb723p-30")
        weights = 0x1.47363da6833a8p+19, 0x1.c34ca4df56dc2p+19,
                  0x1.a75e1050ccab3p+19, 0x1.7f4bc9abe1a09p+15
        sum(shares) == 0x1.e9074940cb724p-30    -- one ULP HIGH
        exact residue == 1.0339757656912846e-25

    The reason is that ``amount - sum(others)`` repairs ONE pairing, while
    re-summing every key is a DIFFERENT pairing: since CPython 3.12 the builtin
    ``sum`` is Neumaier-compensated, and ``Neumaier(others + [final])`` is not
    ``fl(Neumaier(others) + final)``. A naive left-to-right sum of that same
    counterexample does land on ``amount`` — so the compensation the rest of this
    module relies on is what exposes it.

    Anything that needs the shares to conserve ``amount`` must therefore carry
    the allocation in exact arithmetic; it cannot lean on this function.

    Args:
        amount: Quantity or monetary amount to distribute.
        weights: Pre-event composite quantity weights.

    Returns:
        Per-key allocations summing to ``amount`` only up to the split residue
        described above.
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


def _values_with_residue[KeyT: Hashable](
    values: Mapping[KeyT, float],
    keys: Sequence[KeyT],
    total: float,
) -> dict[KeyT, float] | None:
    """Reconcile values through the final key or fail if floats cannot represent it.

    The accumulated final value is tried first. Its adjacent floats are also
    tried because the later stable-order sum can round in the opposite direction
    by one unit in the last place. A direct residual farther away is not proof of
    ownership and is never transported into the final bucket. If no allowed
    representation sums exactly, returning ``None`` lets the caller withhold the
    point instead of publishing a fabricated keyed contribution.

    Args:
        values: Cumulative values before point-level reconciliation.
        keys: Caller-provided deterministic key order.
        total: Aggregate value the buckets must equal exactly.

    Returns:
        Reconciled values, or ``None`` when exact float reconciliation fails.
    """
    if not keys:
        return {} if _is_exactly_zero(total) else None
    reconciled: dict[KeyT, float] = {}
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
) -> tuple[list[_PreparedExecution], list[_RegressionShadow]]:
    """Clamp per-pool event times monotone and collect regression shadows.

    Each durable shard pool's fills are accumulated in caller-supplied scope order.
    A fill whose ``event_time`` regresses below the pool's running maximum
    is placed at that maximum (so scope order survives the grid walk) and the
    interval it shadows is recorded so those minutes can be flagged incomplete.

    Args:
        executions: Executions in ``(exchange, scope_sequence)`` order.

    Returns:
        The prepared executions sorted for the grid walk, and the shadowed
        ``[economic_time, clamped_time)`` intervals produced by regressions.
    """
    last_effective: dict[PoolKey, datetime] = {}
    shadows: list[_RegressionShadow] = []
    prepared: list[_PreparedExecution] = []
    for execution in executions:
        pool_key = (execution.instrument_public_id, execution.shard_key)
        previous = last_effective.get(pool_key)
        if previous is not None and execution.event_time < previous:
            effective = previous
            shadows.append(
                _RegressionShadow(
                    start=execution.event_time,
                    end=previous,
                    reason=_global_incompleteness_reason(
                        "scope_order_regression",
                        "untrusted",
                        execution.instrument_public_id,
                    ),
                )
            )
        else:
            effective = execution.event_time
        last_effective[pool_key] = effective
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


def _minute_ceiling_index(point_time: datetime, grid_start: datetime) -> int:
    """Return the first minute-grid index at or after one timestamp."""
    minute = timedelta(minutes=1)
    floor_index = (point_time - grid_start) // minute
    return floor_index + int(grid_start + floor_index * minute < point_time)


def _regression_shadow_deltas(
    shadows: Sequence[_RegressionShadow],
    grid_start: datetime,
    point_count: int,
) -> dict[int, dict[str, int]]:
    """Index clipped half-open shadow intervals as minute reference deltas."""
    deltas: dict[int, dict[str, int]] = {}
    for shadow in shadows:
        trigger = shadow.reason.trigger_instrument_public_id
        if trigger is None:
            raise ValueError("regression shadow requires a triggering instrument")
        start_index = max(
            0,
            min(point_count, _minute_ceiling_index(shadow.start, grid_start)),
        )
        end_index = max(
            0,
            min(point_count, _minute_ceiling_index(shadow.end, grid_start)),
        )
        if start_index >= end_index:
            continue
        for point_index, delta in ((start_index, 1), (end_index, -1)):
            trigger_deltas = deltas.setdefault(point_index, {})
            trigger_deltas[trigger] = trigger_deltas.get(trigger, 0) + delta
    return deltas


def _apply_regression_shadow_deltas(
    active: dict[str, int],
    trigger_heap: list[str],
    deltas: Mapping[str, int],
) -> str | None:
    """Apply one boundary and return its deterministic active trigger."""
    for trigger, delta in deltas.items():
        previous_count = active.get(trigger, 0)
        active_count = previous_count + delta
        if active_count > 0:
            active[trigger] = active_count
            if previous_count == 0:
                heappush(trigger_heap, trigger)
        else:
            active.pop(trigger, None)
    while trigger_heap and trigger_heap[0] not in active:
        heappop(trigger_heap)
    return trigger_heap[0] if trigger_heap else None


def _regression_shadow_trigger_changes(
    shadows: Sequence[_RegressionShadow],
    grid_start: datetime,
    point_count: int,
) -> dict[int, str | None]:
    """Return only boundary changes to the deterministic active trigger."""
    deltas = _regression_shadow_deltas(shadows, grid_start, point_count)
    active: dict[str, int] = {}
    trigger_heap: list[str] = []
    changes: dict[int, str | None] = {}
    current_trigger: str | None = None
    for point_index in range(point_count + 1):
        boundary_deltas = deltas.get(point_index)
        if boundary_deltas is None:
            continue
        next_trigger = _apply_regression_shadow_deltas(
            active,
            trigger_heap,
            boundary_deltas,
        )
        if next_trigger != current_trigger:
            changes[point_index] = next_trigger
            current_trigger = next_trigger
    return changes


def _valuation_pool_index(
    seeded_pool_keys: Mapping[str, set[PoolKey]],
    prepared: Sequence[_PreparedExecution],
    to_time: datetime,
) -> PoolIndex:
    """Return pools capable of affecting valuation inside the chart window."""
    pool_keys_by_instrument = {
        instrument_public_id: set(pool_keys)
        for instrument_public_id, pool_keys in seeded_pool_keys.items()
    }
    for item in prepared:
        if item.effective_time <= to_time:
            execution = item.execution
            pool_keys_by_instrument.setdefault(execution.instrument_public_id, set()).add(
                (execution.instrument_public_id, execution.shard_key)
            )
    return {
        instrument_public_id: tuple(sorted(pool_keys))
        for instrument_public_id, pool_keys in pool_keys_by_instrument.items()
    }


def _position_changes_by_effective_time(
    prepared: Sequence[_PreparedExecution],
    to_time: datetime,
) -> Mapping[datetime, set[str]]:
    """Index only in-window position changes for accrual ambiguity checks."""
    changing: defaultdict[datetime, set[str]] = defaultdict(set)
    for item in prepared:
        if item.effective_time <= to_time and not _is_exactly_zero(item.execution.position_delta):
            changing[item.effective_time].add(item.execution.instrument_public_id)
    return changing


def _untrusted_point(
    point_time: datetime,
    seen: Sequence[str],
    attribution_keys: Sequence[AttributionKey],
    incompleteness_reasons: Collection[PnlIncompletenessReasonEntry],
) -> PnlTimelinePoint:
    """Build a fully-untrusted incomplete point (all components withheld).

    Used when this minute's cumulatives themselves cannot be trusted — a
    scope-order regression shadows it, or an unknown seeded cost basis is still
    in play — so no realized / fee / accrual / unrealized number is defensible.

    Args:
        point_time: The grid instant.
        seen: Instruments to list (all with null contributions).
        attribution_keys: Composite buckets to list with null contributions.
        incompleteness_reasons: Causes established before this early return.

    Returns:
        An incomplete :class:`PnlTimelinePoint` with every component ``None``.
    """
    contributions = tuple(
        PnlInstrumentContribution(
            instrument_public_id=instrument_public_id,
            native_symbol=None,
            exchange=None,
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
        incompleteness_reasons=canonical_incompleteness_reasons(incompleteness_reasons),
        per_instrument=contributions,
        attribution=attribution,
    )


def _combined_instrument_weights(
    pool_keys: Sequence[PoolKey],
    weights_by_pool: Mapping[PoolKey, Mapping[AttributionKey, float]],
) -> dict[AttributionKey, float]:
    """Combine current shard quantity ownership for one instrument."""
    combined: defaultdict[AttributionKey, float] = defaultdict(float)
    for pool_key in pool_keys:
        pool_weights = weights_by_pool.get(pool_key, {})
        for attribution_key in _sorted_attribution_keys(set(pool_weights)):
            combined[attribution_key] += pool_weights[attribution_key]
    return dict(combined)


@dataclass(frozen=True)
class _PointCumulatives:
    """Immutable cumulative flows supplied to one point valuation."""

    realized_by_instrument: Mapping[str, float]
    fee_by_instrument: Mapping[str, float]
    accrual_by_instrument: Mapping[str, float]
    realized_by_attribution: Mapping[AttributionKey, float]
    fee_by_attribution: Mapping[AttributionKey, float]
    accrual_by_attribution: Mapping[AttributionKey, float]
    realized_total: float
    fee_total: float
    accrual_total: float


@dataclass(frozen=True)
class _PointValuationContext:
    """All immutable evidence needed to value one timeline grid point."""

    point_time: datetime
    pools: Mapping[PoolKey, _Pool]
    pool_index: PoolIndex
    weights_by_pool: Mapping[PoolKey, Mapping[AttributionKey, float]]
    marks: MarkMap
    seen: Sequence[str]
    attribution_seen: Sequence[AttributionKey]
    cumulatives: _PointCumulatives
    activation_time: datetime | None
    global_reasons: Collection[PnlIncompletenessReasonEntry]
    untrusted_reasons_by_instrument: Mapping[
        str,
        Collection[PnlIncompletenessReasonEntry],
    ]
    basis_reasons_by_pool: Mapping[PoolKey, Collection[PnlIncompletenessReason]]
    mark_incompleteness_reasons: MarkIncompletenessReasonMap


@dataclass(frozen=True)
class _PointPreparation:
    """Validated and reconciled cumulative inputs for instrument valuation."""

    instrument_untrusted_reasons: Mapping[
        str,
        Collection[PnlIncompletenessReasonEntry],
    ]
    realized_by_instrument: Mapping[str, float]
    fee_by_instrument: Mapping[str, float]
    accrual_by_instrument: Mapping[str, float]
    attribution_keys: Sequence[AttributionKey]


@dataclass(frozen=True)
class _InstrumentPoolState:
    """Active pools and combined attribution weights for one instrument."""

    active_pool_keys: Sequence[PoolKey]
    weights: Mapping[AttributionKey, float]


@dataclass(frozen=True)
class _InstrumentValuationInputs:
    """Pools, mark evidence, and basis failures for one instrument."""

    pool_state: _InstrumentPoolState
    mark: float | None
    is_activation_point: bool
    mark_unavailable: bool
    unavailable_basis_keys: Sequence[PoolKey]


@dataclass
class _PointAccumulator:
    """Mutable point-local valuation outputs collected in stable order."""

    point_reasons: set[PnlIncompletenessReasonEntry] = field(default_factory=set)
    contributions: list[PnlInstrumentContribution] = field(default_factory=list)
    unrealized_by_attribution: defaultdict[AttributionKey, float] = field(
        default_factory=lambda: defaultdict(float)
    )
    unrealized_incomplete: set[AttributionKey] = field(default_factory=set)
    unrealized_total: float = 0.0


@dataclass(frozen=True)
class _ReconciledAttribution:
    """Exactly reconciled point-level attribution component mappings."""

    realized: Mapping[AttributionKey, float]
    fee: Mapping[AttributionKey, float]
    accrual: Mapping[AttributionKey, float]
    unrealized: Mapping[AttributionKey, float]


def _context_untrusted_point(
    context: _PointValuationContext,
    attribution_keys: Sequence[AttributionKey],
    reasons: Collection[PnlIncompletenessReasonEntry],
) -> PnlTimelinePoint:
    """Build a fully withheld point using stable identities from one context."""
    return _untrusted_point(
        context.point_time,
        context.seen,
        attribution_keys,
        reasons,
    )


def _point_instrument_untrusted_reasons(
    context: _PointValuationContext,
) -> dict[str, set[PnlIncompletenessReasonEntry]]:
    """Collect latched and non-finite instrument cumulative causes."""
    instrument_untrusted_reasons = {
        instrument_public_id: set(reasons)
        for instrument_public_id, reasons in context.untrusted_reasons_by_instrument.items()
        if reasons
    }
    cumulatives = context.cumulatives
    for instrument_public_id in context.seen:
        if (
            not math.isfinite(cumulatives.realized_by_instrument.get(instrument_public_id, 0.0))
            or not math.isfinite(cumulatives.fee_by_instrument.get(instrument_public_id, 0.0))
            or not math.isfinite(cumulatives.accrual_by_instrument.get(instrument_public_id, 0.0))
        ):
            instrument_untrusted_reasons.setdefault(instrument_public_id, set()).add(
                _instrument_incompleteness_reason(
                    "cumulative_non_finite",
                    "untrusted",
                    instrument_public_id,
                )
            )
    return instrument_untrusted_reasons


def _point_cumulatives_are_finite(context: _PointValuationContext) -> bool:
    """Return whether all three aggregate cumulative flows are finite."""
    cumulatives = context.cumulatives
    return (
        math.isfinite(cumulatives.realized_total)
        and math.isfinite(cumulatives.fee_total)
        and math.isfinite(cumulatives.accrual_total)
    )


def _reconcile_point_instrument_cumulatives(
    context: _PointValuationContext,
    instrument_untrusted_reasons: Mapping[
        str,
        Collection[PnlIncompletenessReasonEntry],
    ],
) -> tuple[Mapping[str, float], Mapping[str, float], Mapping[str, float]] | None:
    """Reconcile finite instrument flows, retaining maps when any is untrusted."""
    cumulatives = context.cumulatives
    if instrument_untrusted_reasons:
        return (
            cumulatives.realized_by_instrument,
            cumulatives.fee_by_instrument,
            cumulatives.accrual_by_instrument,
        )
    realized = _values_with_residue(
        cumulatives.realized_by_instrument,
        context.seen,
        cumulatives.realized_total,
    )
    fee = _values_with_residue(
        cumulatives.fee_by_instrument,
        context.seen,
        cumulatives.fee_total,
    )
    accrual = _values_with_residue(
        cumulatives.accrual_by_instrument,
        context.seen,
        cumulatives.accrual_total,
    )
    if realized is None or fee is None or accrual is None:
        return None
    return realized, fee, accrual


def _point_attribution_keys(context: _PointValuationContext) -> list[AttributionKey]:
    """Return all stable attribution keys established by cumulative flows."""
    cumulatives = context.cumulatives
    return _sorted_attribution_keys(
        set(context.attribution_seen)
        | set(cumulatives.realized_by_attribution)
        | set(cumulatives.fee_by_attribution)
        | set(cumulatives.accrual_by_attribution)
    )


def _point_attribution_flows_are_finite(
    context: _PointValuationContext,
    attribution_keys: Sequence[AttributionKey],
) -> bool:
    """Return whether every keyed cumulative attribution flow is finite."""
    cumulatives = context.cumulatives
    return all(
        math.isfinite(cumulatives.realized_by_attribution.get(key, 0.0))
        and math.isfinite(cumulatives.fee_by_attribution.get(key, 0.0))
        and math.isfinite(cumulatives.accrual_by_attribution.get(key, 0.0))
        for key in attribution_keys
    )


def _prepare_point_valuation(
    context: _PointValuationContext,
) -> _PointPreparation | PnlTimelinePoint:
    """Validate global and cumulative evidence before mark valuation."""
    established_global_reasons = set(context.global_reasons)
    if established_global_reasons:
        for latched_reasons in context.untrusted_reasons_by_instrument.values():
            established_global_reasons.update(latched_reasons)
        return _context_untrusted_point(
            context,
            context.attribution_seen,
            established_global_reasons,
        )
    instrument_reasons = _point_instrument_untrusted_reasons(context)
    if not _point_cumulatives_are_finite(context) and not instrument_reasons:
        return _context_untrusted_point(
            context,
            context.attribution_seen,
            {
                _global_incompleteness_reason(
                    "cumulative_non_finite",
                    "untrusted",
                )
            },
        )
    reconciled = _reconcile_point_instrument_cumulatives(context, instrument_reasons)
    if reconciled is None:
        return _context_untrusted_point(
            context,
            context.attribution_seen,
            {
                _global_incompleteness_reason(
                    "instrument_reconciliation_failed",
                    "untrusted",
                )
            },
        )
    attribution_keys = _point_attribution_keys(context)
    if not instrument_reasons and not _point_attribution_flows_are_finite(
        context,
        attribution_keys,
    ):
        return _context_untrusted_point(
            context,
            attribution_keys,
            {
                _global_incompleteness_reason(
                    "attribution_value_non_finite",
                    "untrusted",
                )
            },
        )
    realized, fee, accrual = reconciled
    return _PointPreparation(
        instrument_untrusted_reasons=instrument_reasons,
        realized_by_instrument=realized,
        fee_by_instrument=fee,
        accrual_by_instrument=accrual,
        attribution_keys=attribution_keys,
    )


def _instrument_pool_state(
    context: _PointValuationContext,
    instrument_public_id: str,
) -> _InstrumentPoolState:
    """Return active pool keys and combined weights for one instrument."""
    active_pool_keys: list[PoolKey] = []
    combined_weights: defaultdict[AttributionKey, float] = defaultdict(float)
    for pool_key in context.pool_index.get(instrument_public_id, ()):
        pool_weights = context.weights_by_pool.get(pool_key, {})
        for attribution_key in _sorted_attribution_keys(set(pool_weights)):
            combined_weights[attribution_key] += pool_weights[attribution_key]
        pool = context.pools.get(pool_key)
        if pool is not None and abs(pool.position_qty) >= FLAT_EPSILON:
            active_pool_keys.append(pool_key)
    return _InstrumentPoolState(
        active_pool_keys=active_pool_keys,
        weights=dict(combined_weights),
    )


def _mark_unavailable(
    is_activation_point: bool,
    mark: float | None,
) -> bool:
    """Return whether a non-activation point lacks a finite mark."""
    return not is_activation_point and (mark is None or not math.isfinite(mark))


def _withhold_instrument_unrealized(
    context: _PointValuationContext,
    accumulator: _PointAccumulator,
    instrument_public_id: str,
    inputs: _InstrumentValuationInputs,
) -> None:
    """Stamp exact mark and basis causes for one withheld instrument value."""
    if inputs.mark_unavailable:
        mark_reason = context.mark_incompleteness_reasons.get(
            (instrument_public_id, context.point_time),
            "mark_unavailable",
        )
        accumulator.point_reasons.add(
            _instrument_incompleteness_reason(
                mark_reason,
                "mark_incomplete",
                instrument_public_id,
            )
        )
    for pool_key in inputs.unavailable_basis_keys:
        basis_reasons = context.basis_reasons_by_pool.get(pool_key)
        if not basis_reasons:
            raise ValueError("an unavailable entry basis requires a stamped causal reason")
        for basis_reason in basis_reasons:
            accumulator.point_reasons.add(
                _instrument_incompleteness_reason(
                    basis_reason,
                    "mark_incomplete",
                    instrument_public_id,
                )
            )
    instrument_keys = _sorted_attribution_keys(set(inputs.pool_state.weights))
    accumulator.unrealized_incomplete.update(instrument_keys or [_UNATTRIBUTED_KEY])


def _non_finite_instrument_unrealized(
    accumulator: _PointAccumulator,
    instrument_public_id: str,
    instrument_weights: Mapping[AttributionKey, float],
) -> None:
    """Stamp one non-finite instrument valuation and affected attribution keys."""
    accumulator.point_reasons.add(
        _instrument_incompleteness_reason(
            "unrealized_non_finite",
            "mark_incomplete",
            instrument_public_id,
        )
    )
    instrument_keys = _sorted_attribution_keys(set(instrument_weights))
    accumulator.unrealized_incomplete.update(instrument_keys or [_UNATTRIBUTED_KEY])


def _allocate_instrument_unrealized(
    context: _PointValuationContext,
    accumulator: _PointAccumulator,
    instrument_public_id: str,
    pool_values: Sequence[tuple[PoolKey, float]],
) -> None:
    """Allocate finite shard unrealized values and latch keyed overflow."""
    for pool_key, pool_unrealized in pool_values:
        allocations = _allocate_by_weights(
            pool_unrealized,
            context.weights_by_pool.get(pool_key, {}),
        )
        for key, amount in allocations.items():
            accumulator.unrealized_by_attribution[key] += amount
            if not math.isfinite(accumulator.unrealized_by_attribution[key]):
                accumulator.unrealized_incomplete.add(key)
                accumulator.point_reasons.add(
                    _instrument_incompleteness_reason(
                        "attribution_value_non_finite",
                        "mark_incomplete",
                        instrument_public_id,
                    )
                )


def _trusted_instrument_unrealized(
    context: _PointValuationContext,
    accumulator: _PointAccumulator,
    instrument_public_id: str,
    inputs: _InstrumentValuationInputs,
) -> float | None:
    """Value finite-mark shard pools without changing their summation order."""
    trusted_mark = cast(float, inputs.mark)
    pool_values: list[tuple[PoolKey, float]] = []
    instrument_unrealized = 0.0
    for pool_key in inputs.pool_state.active_pool_keys:
        pool = context.pools[pool_key]
        trusted_entry_price = cast(float, pool.entry_price)
        pool_unrealized = (
            0.0
            if inputs.is_activation_point
            else pool.position_qty * (trusted_mark - trusted_entry_price)
        )
        instrument_unrealized += pool_unrealized
        if not math.isfinite(pool_unrealized) or not math.isfinite(instrument_unrealized):
            _non_finite_instrument_unrealized(
                accumulator,
                instrument_public_id,
                inputs.pool_state.weights,
            )
            return None
        pool_values.append((pool_key, pool_unrealized))
    accumulator.unrealized_total += instrument_unrealized
    _allocate_instrument_unrealized(
        context,
        accumulator,
        instrument_public_id,
        pool_values,
    )
    return instrument_unrealized


def _instrument_unrealized(
    context: _PointValuationContext,
    accumulator: _PointAccumulator,
    instrument_public_id: str,
    pool_state: _InstrumentPoolState,
) -> float | None:
    """Value or honestly withhold one instrument's active pools."""
    is_activation_point = (
        context.activation_time is not None and context.point_time == context.activation_time
    )
    mark = (
        0.0
        if is_activation_point
        else context.marks.get((instrument_public_id, context.point_time))
    )
    mark_unavailable = _mark_unavailable(
        is_activation_point,
        mark,
    )
    unavailable_basis_keys = [
        pool_key
        for pool_key in pool_state.active_pool_keys
        if not is_positive_finite(context.pools[pool_key].entry_price)
        or context.basis_reasons_by_pool.get(pool_key)
    ]
    inputs = _InstrumentValuationInputs(
        pool_state=pool_state,
        mark=mark,
        is_activation_point=is_activation_point,
        mark_unavailable=mark_unavailable,
        unavailable_basis_keys=unavailable_basis_keys,
    )
    if mark_unavailable or unavailable_basis_keys:
        _withhold_instrument_unrealized(
            context,
            accumulator,
            instrument_public_id,
            inputs,
        )
        return None
    return _trusted_instrument_unrealized(
        context,
        accumulator,
        instrument_public_id,
        inputs,
    )


def _append_instrument_contribution(
    context: _PointValuationContext,
    preparation: _PointPreparation,
    accumulator: _PointAccumulator,
    instrument_public_id: str,
) -> None:
    """Append one instrument contribution in the caller's stable order."""
    if instrument_public_id in preparation.instrument_untrusted_reasons:
        accumulator.contributions.append(
            PnlInstrumentContribution(
                instrument_public_id=instrument_public_id,
                native_symbol=None,
                exchange=None,
                realized_pnl=None,
                fee_pnl=None,
                accrual_pnl=None,
                unrealized_pnl=None,
            )
        )
        return
    pool_state = _instrument_pool_state(context, instrument_public_id)
    instrument_unrealized = (
        0.0
        if not pool_state.active_pool_keys
        else _instrument_unrealized(
            context,
            accumulator,
            instrument_public_id,
            pool_state,
        )
    )
    accumulator.contributions.append(
        PnlInstrumentContribution(
            instrument_public_id=instrument_public_id,
            native_symbol=None,
            exchange=None,
            realized_pnl=preparation.realized_by_instrument.get(instrument_public_id, 0.0),
            fee_pnl=preparation.fee_by_instrument.get(instrument_public_id, 0.0),
            accrual_pnl=preparation.accrual_by_instrument.get(instrument_public_id, 0.0),
            unrealized_pnl=instrument_unrealized,
        )
    )


def _complete_attribution_keys(
    context: _PointValuationContext,
    preparation: _PointPreparation,
    accumulator: _PointAccumulator,
) -> list[AttributionKey]:
    """Include mark allocations and all current inventory ownership keys."""
    return _sorted_attribution_keys(
        set(preparation.attribution_keys)
        | set(accumulator.unrealized_by_attribution)
        | set(accumulator.unrealized_incomplete)
        | {key for pool_weights in context.weights_by_pool.values() for key in pool_weights}
    )


def _instrument_untrusted_valued_point(
    context: _PointValuationContext,
    accumulator: _PointAccumulator,
    attribution_keys: Sequence[AttributionKey],
) -> PnlTimelinePoint:
    """Withhold aggregates while retaining independently useful instrument rows."""
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
        point_time=context.point_time,
        realized_pnl=None,
        fee_pnl=None,
        accrual_pnl=None,
        unrealized_pnl=None,
        net_pnl=None,
        valuation_status="incomplete",
        incompleteness_reasons=canonical_incompleteness_reasons(accumulator.point_reasons),
        per_instrument=tuple(accumulator.contributions),
        attribution=attribution,
    )


def _reconcile_instrument_unrealized(
    context: _PointValuationContext,
    accumulator: _PointAccumulator,
) -> bool:
    """Reconcile finite per-instrument marks and report exact representability."""
    if accumulator.point_reasons or not math.isfinite(accumulator.unrealized_total):
        return True
    unrealized_by_instrument = {
        contribution.instrument_public_id: contribution.unrealized_pnl
        for contribution in accumulator.contributions
        if contribution.unrealized_pnl is not None
    }
    reconciled = _values_with_residue(
        unrealized_by_instrument,
        context.seen,
        accumulator.unrealized_total,
    )
    if reconciled is None:
        return False
    accumulator.contributions = [
        PnlInstrumentContribution(
            instrument_public_id=contribution.instrument_public_id,
            native_symbol=contribution.native_symbol,
            exchange=contribution.exchange,
            realized_pnl=contribution.realized_pnl,
            fee_pnl=contribution.fee_pnl,
            accrual_pnl=contribution.accrual_pnl,
            unrealized_pnl=reconciled[contribution.instrument_public_id],
        )
        for contribution in accumulator.contributions
    ]
    return True


def _reconcile_attribution(
    context: _PointValuationContext,
    accumulator: _PointAccumulator,
    attribution_keys: Sequence[AttributionKey],
) -> _ReconciledAttribution | None:
    """Reconcile all attribution components or refuse an unprovable residue."""
    cumulatives = context.cumulatives
    realized = _values_with_residue(
        cumulatives.realized_by_attribution,
        attribution_keys,
        cumulatives.realized_total,
    )
    fee = _values_with_residue(
        cumulatives.fee_by_attribution,
        attribution_keys,
        cumulatives.fee_total,
    )
    accrual = _values_with_residue(
        cumulatives.accrual_by_attribution,
        attribution_keys,
        cumulatives.accrual_total,
    )
    if realized is None or fee is None or accrual is None:
        return None
    if (
        not accumulator.point_reasons
        and math.isfinite(accumulator.unrealized_total)
        and not accumulator.unrealized_incomplete
    ):
        unrealized = _values_with_residue(
            accumulator.unrealized_by_attribution,
            attribution_keys,
            accumulator.unrealized_total,
        )
        if unrealized is None:
            return None
    else:
        unrealized = dict(accumulator.unrealized_by_attribution)
    return _ReconciledAttribution(
        realized=realized,
        fee=fee,
        accrual=accrual,
        unrealized=unrealized,
    )


def _attribution_contributions(
    reconciled: _ReconciledAttribution,
    unrealized_incomplete: Collection[AttributionKey],
    attribution_keys: Sequence[AttributionKey],
) -> tuple[PnlAttributionContribution, ...]:
    """Build stable public attribution rows from reconciled component maps."""
    return tuple(
        PnlAttributionContribution(
            origin=origin,
            strategy_name=strategy_name,
            realized_pnl=reconciled.realized[(origin, strategy_name)],
            fee_pnl=reconciled.fee[(origin, strategy_name)],
            accrual_pnl=reconciled.accrual[(origin, strategy_name)],
            unrealized_pnl=(
                None
                if (origin, strategy_name) in unrealized_incomplete
                else reconciled.unrealized.get((origin, strategy_name), 0.0)
            ),
        )
        for origin, strategy_name in attribution_keys
    )


def _mark_incomplete_point(
    context: _PointValuationContext,
    accumulator: _PointAccumulator,
    attribution: tuple[PnlAttributionContribution, ...],
) -> PnlTimelinePoint:
    """Build a mark-incomplete point while preserving trusted cumulatives."""
    cumulatives = context.cumulatives
    return PnlTimelinePoint(
        point_time=context.point_time,
        realized_pnl=cumulatives.realized_total,
        fee_pnl=cumulatives.fee_total,
        accrual_pnl=cumulatives.accrual_total,
        unrealized_pnl=None,
        net_pnl=None,
        valuation_status="incomplete",
        incompleteness_reasons=canonical_incompleteness_reasons(accumulator.point_reasons),
        per_instrument=tuple(accumulator.contributions),
        attribution=attribution,
    )


def _finalize_valued_point(
    context: _PointValuationContext,
    accumulator: _PointAccumulator,
    attribution: tuple[PnlAttributionContribution, ...],
) -> PnlTimelinePoint:
    """Build a complete point or withhold non-finite unrealized and net values."""
    if accumulator.point_reasons:
        return _mark_incomplete_point(context, accumulator, attribution)
    cumulatives = context.cumulatives
    net = (
        cumulatives.realized_total
        + cumulatives.fee_total
        + cumulatives.accrual_total
        + accumulator.unrealized_total
    )
    if not math.isfinite(accumulator.unrealized_total):
        accumulator.point_reasons.add(
            _global_incompleteness_reason(
                "unrealized_non_finite",
                "mark_incomplete",
            )
        )
    if not math.isfinite(net):
        accumulator.point_reasons.add(
            _global_incompleteness_reason(
                "net_non_finite",
                "mark_incomplete",
            )
        )
    if accumulator.point_reasons:
        return _mark_incomplete_point(context, accumulator, attribution)
    return PnlTimelinePoint(
        point_time=context.point_time,
        realized_pnl=cumulatives.realized_total,
        fee_pnl=cumulatives.fee_total,
        accrual_pnl=cumulatives.accrual_total,
        unrealized_pnl=accumulator.unrealized_total,
        net_pnl=net,
        valuation_status="complete",
        incompleteness_reasons=(),
        per_instrument=tuple(accumulator.contributions),
        attribution=attribution,
    )


def _value_point(context: _PointValuationContext) -> PnlTimelinePoint:
    """Value shard pools and aggregate marks and contributions by instrument."""
    prepared = _prepare_point_valuation(context)
    if isinstance(prepared, PnlTimelinePoint):
        return prepared
    accumulator = _PointAccumulator(
        point_reasons={
            reason
            for reasons in prepared.instrument_untrusted_reasons.values()
            for reason in reasons
        }
    )
    for instrument_public_id in context.seen:
        _append_instrument_contribution(
            context,
            prepared,
            accumulator,
            instrument_public_id,
        )
    attribution_keys = _complete_attribution_keys(context, prepared, accumulator)
    if prepared.instrument_untrusted_reasons:
        return _instrument_untrusted_valued_point(
            context,
            accumulator,
            attribution_keys,
        )
    if not _reconcile_instrument_unrealized(context, accumulator):
        return _context_untrusted_point(
            context,
            attribution_keys,
            {
                _global_incompleteness_reason(
                    "instrument_reconciliation_failed",
                    "untrusted",
                )
            },
        )
    reconciled_attribution = _reconcile_attribution(
        context,
        accumulator,
        attribution_keys,
    )
    if reconciled_attribution is None:
        accumulator.point_reasons.add(
            _global_incompleteness_reason(
                "attribution_reconciliation_failed",
                "untrusted",
            )
        )
        return _context_untrusted_point(
            context,
            attribution_keys,
            accumulator.point_reasons,
        )
    attribution = _attribution_contributions(
        reconciled_attribution,
        accumulator.unrealized_incomplete,
        attribution_keys,
    )
    return _finalize_valued_point(context, accumulator, attribution)


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


@dataclass(frozen=True)
class _TimelineBuildEvidence:
    """Resolved optional lineage and incompleteness evidence for one build."""

    marks: MarkMap
    lineage: Mapping[str, TimelineExecutionLineage]
    untrusted_price_reasons_by_instrument: Mapping[
        str,
        Collection[PnlIncompletenessReason],
    ]
    mark_incompleteness_reasons: MarkIncompletenessReasonMap


@dataclass
class _TimelineBuildState:
    """Mutable replay, cumulative, and attribution state for one build."""

    pools: dict[PoolKey, _Pool] = field(default_factory=dict)
    weights_by_pool: dict[PoolKey, dict[AttributionKey, float]] = field(default_factory=dict)
    seen: set[str] = field(default_factory=set)
    attribution_seen: set[AttributionKey] = field(default_factory=set)
    basis_reasons_by_pool: dict[PoolKey, set[PnlIncompletenessReason]] = field(default_factory=dict)
    untrusted_reasons_by_instrument: dict[
        str,
        set[PnlIncompletenessReasonEntry],
    ] = field(default_factory=dict)
    activation_time: datetime | None = None
    scope_by_shard: dict[str, tuple[str, str]] = field(default_factory=dict)
    exchange_by_instrument: dict[str, str] = field(default_factory=dict)
    pool_keys_by_instrument: defaultdict[str, set[PoolKey]] = field(
        default_factory=lambda: defaultdict(set)
    )
    realized_by_instrument: defaultdict[str, float] = field(
        default_factory=lambda: defaultdict(float)
    )
    fee_by_instrument: defaultdict[str, float] = field(default_factory=lambda: defaultdict(float))
    accrual_by_instrument: defaultdict[str, float] = field(
        default_factory=lambda: defaultdict(float)
    )
    realized_by_attribution: defaultdict[AttributionKey, float] = field(
        default_factory=lambda: defaultdict(float)
    )
    fee_by_attribution: defaultdict[AttributionKey, float] = field(
        default_factory=lambda: defaultdict(float)
    )
    accrual_by_attribution: defaultdict[AttributionKey, float] = field(
        default_factory=lambda: defaultdict(float)
    )
    realized_total: float = 0.0
    fee_total: float = 0.0
    accrual_total: float = 0.0


@dataclass(frozen=True)
class _TimelineSchedule:
    """Prepared execution, accrual, and shadow indexes for the grid walk."""

    prepared: Sequence[_PreparedExecution]
    accruals: Sequence[TimelineAccrual]
    pool_index: PoolIndex
    position_changes: Mapping[datetime, set[str]]
    shadow_trigger_changes: Mapping[int, str | None]
    minute_grid: Sequence[datetime]


@dataclass
class _EventCursor:
    """Mutable indexes into the monotone execution and accrual schedules."""

    execution_index: int = 0
    accrual_index: int = 0


@dataclass(frozen=True)
class _ExecutionApplication:
    """Validated inputs and average-cost outcome for one execution."""

    execution: TimelineExecution
    instrument_public_id: str
    pool_key: PoolKey
    attribution_key: AttributionKey
    pre_fill_weights: Mapping[AttributionKey, float]
    pre_fill_basis_reasons: Collection[PnlIncompletenessReason]
    position_size: float
    price_is_trusted: bool
    outcome: PoolFillOutcome


def _resolved_build_evidence(
    marks: MarkMap,
    lineage: Mapping[str, TimelineExecutionLineage] | None,
    untrusted_price_reasons_by_instrument: Mapping[str, Collection[PnlIncompletenessReason]] | None,
    mark_incompleteness_reasons: MarkIncompletenessReasonMap | None,
) -> _TimelineBuildEvidence:
    """Resolve optional caller evidence to immutable empty mappings."""
    return _TimelineBuildEvidence(
        marks=marks,
        lineage={} if lineage is None else lineage,
        untrusted_price_reasons_by_instrument=(
            {}
            if untrusted_price_reasons_by_instrument is None
            else untrusted_price_reasons_by_instrument
        ),
        mark_incompleteness_reasons=(
            {} if mark_incompleteness_reasons is None else mark_incompleteness_reasons
        ),
    )


def _seed_timeline_pool(
    state: _TimelineBuildState,
    seed: OpeningPool,
) -> None:
    """Seed one opening pool and retain its exact causal trust state."""
    instrument_public_id = seed.instrument_public_id
    pool_key = (instrument_public_id, seed.shard_key)
    state.pools[pool_key] = _Pool(seed.position_qty, seed.entry_price)
    state.pool_keys_by_instrument[instrument_public_id].add(pool_key)
    state.scope_by_shard[seed.shard_key] = (instrument_public_id, seed.exchange)
    state.exchange_by_instrument[instrument_public_id] = seed.exchange
    state.seen.add(instrument_public_id)
    if not math.isfinite(seed.position_qty):
        _add_instrument_untrusted_reason(
            state.untrusted_reasons_by_instrument,
            instrument_public_id,
            "seed_quantity_non_finite",
        )
        return
    if not _is_exactly_zero(abs(seed.position_qty)):
        state.weights_by_pool[pool_key] = {_UNATTRIBUTED_KEY: abs(seed.position_qty)}
        state.attribution_seen.add(_UNATTRIBUTED_KEY)
    if seed.entry_price is None and abs(seed.position_qty) >= FLAT_EPSILON:
        _add_instrument_untrusted_reason(
            state.untrusted_reasons_by_instrument,
            instrument_public_id,
            "cost_basis_unavailable",
        )
        state.basis_reasons_by_pool.setdefault(pool_key, set()).add("cost_basis_unavailable")
    elif abs(seed.position_qty) >= FLAT_EPSILON and not is_positive_finite(seed.entry_price):
        state.basis_reasons_by_pool.setdefault(pool_key, set()).add("cost_basis_unavailable")


def _initialize_timeline_state(
    opening: TimelineOpening | None,
) -> _TimelineBuildState:
    """Return empty replay state optionally seeded at the activation anchor."""
    state = _TimelineBuildState()
    if opening is None:
        return state
    for seed in opening.pools:
        _seed_timeline_pool(state, seed)
    state.activation_time = opening.t0
    return state


def _validate_timeline_execution_scope(
    state: _TimelineBuildState,
    execution: TimelineExecution,
) -> None:
    """Validate and record one execution's durable shard and instrument scope."""
    if not execution.instrument_public_id or not execution.shard_key or not execution.exchange:
        raise ValueError("timeline execution pool identities must be non-empty")
    execution_scope = (execution.instrument_public_id, execution.exchange)
    previous_scope = state.scope_by_shard.setdefault(
        execution.shard_key,
        execution_scope,
    )
    if previous_scope != execution_scope:
        raise ValueError("one timeline shard cannot span instrument or exchange scopes")
    previous_exchange = state.exchange_by_instrument.setdefault(
        execution.instrument_public_id,
        execution.exchange,
    )
    if previous_exchange != execution.exchange:
        raise ValueError("one timeline instrument cannot span multiple exchanges")


def _validate_timeline_execution_scopes(
    state: _TimelineBuildState,
    executions: Sequence[TimelineExecution],
) -> None:
    """Validate all execution identities before any replay mutation occurs."""
    for execution in executions:
        _validate_timeline_execution_scope(state, execution)


def _timeline_schedule(
    state: _TimelineBuildState,
    executions: Sequence[TimelineExecution],
    accruals: Sequence[TimelineAccrual],
    window: TimelineWindow,
) -> _TimelineSchedule:
    """Build deterministic event, valuation-pool, and regression indexes."""
    prepared, shadows = _prepare_executions(executions)
    pool_index = _valuation_pool_index(
        state.pool_keys_by_instrument,
        prepared,
        window.to_time,
    )
    position_changes = _position_changes_by_effective_time(
        prepared,
        window.to_time,
    )
    sorted_accruals = sorted(
        (
            accrual
            for accrual in accruals
            if state.activation_time is None or accrual.accrued_at > state.activation_time
        ),
        key=lambda item: (item.accrued_at, item.instrument_public_id),
    )
    minute_grid = _minute_grid(window.from_time, window.to_time)
    shadow_trigger_changes = _regression_shadow_trigger_changes(
        shadows,
        window.from_time.replace(second=0, microsecond=0),
        len(minute_grid),
    )
    return _TimelineSchedule(
        prepared=prepared,
        accruals=sorted_accruals,
        pool_index=pool_index,
        position_changes=position_changes,
        shadow_trigger_changes=shadow_trigger_changes,
        minute_grid=minute_grid,
    )


def _execution_price_failure(
    state: _TimelineBuildState,
    execution: TimelineExecution,
    pool_key: PoolKey,
) -> None:
    """Latch a closing-price failure and every already-stamped basis cause."""
    instrument_public_id = execution.instrument_public_id
    price_reason = execution.price_incompleteness_reason or "execution_price_invalid"
    _add_instrument_untrusted_reason(
        state.untrusted_reasons_by_instrument,
        instrument_public_id,
        price_reason,
    )
    for basis_reason in state.basis_reasons_by_pool.get(pool_key, ()):
        _add_instrument_untrusted_reason(
            state.untrusted_reasons_by_instrument,
            instrument_public_id,
            basis_reason,
        )


def _begin_execution_application(
    state: _TimelineBuildState,
    evidence: _TimelineBuildEvidence,
    execution: TimelineExecution,
) -> _ExecutionApplication | None:
    """Validate one execution and compute its guarded average-cost outcome."""
    instrument_public_id = execution.instrument_public_id
    pool_key = (instrument_public_id, execution.shard_key)
    state.seen.add(instrument_public_id)
    attribution_key = _execution_attribution(execution, evidence.lineage)
    state.attribution_seen.add(attribution_key)
    if execution.fee_incompleteness_reason is not None:
        _add_instrument_untrusted_reason(
            state.untrusted_reasons_by_instrument,
            instrument_public_id,
            execution.fee_incompleteness_reason,
        )
    price_proof_reasons = evidence.untrusted_price_reasons_by_instrument.get(
        instrument_public_id,
        (),
    )
    for price_proof_reason in price_proof_reasons:
        _add_instrument_untrusted_reason(
            state.untrusted_reasons_by_instrument,
            instrument_public_id,
            price_proof_reason,
        )
    if price_proof_reasons:
        return None
    if (
        not math.isfinite(execution.size)
        or execution.size < 0.0
        or not math.isfinite(execution.position_delta)
    ):
        _add_instrument_untrusted_reason(
            state.untrusted_reasons_by_instrument,
            instrument_public_id,
            "execution_size_invalid",
        )
        return None
    position_size = abs(execution.position_delta)
    pool = state.pools.get(pool_key, _Pool(0.0, None))
    pre_fill_weights = dict(state.weights_by_pool.get(pool_key, {}))
    pre_fill_basis_reasons = set(state.basis_reasons_by_pool.get(pool_key, ()))
    price_is_trusted = _is_exactly_zero(position_size) or is_positive_finite(execution.price)
    outcome = apply_fill(
        pool.position_qty,
        pool.entry_price,
        execution.position_delta,
        position_size,
        execution.price if price_is_trusted else math.nan,
    )
    if not price_is_trusted and outcome.closed_qty > 0.0:
        _execution_price_failure(state, execution, pool_key)
        return None
    return _ExecutionApplication(
        execution=execution,
        instrument_public_id=instrument_public_id,
        pool_key=pool_key,
        attribution_key=attribution_key,
        pre_fill_weights=pre_fill_weights,
        pre_fill_basis_reasons=pre_fill_basis_reasons,
        position_size=position_size,
        price_is_trusted=price_is_trusted,
        outcome=outcome,
    )


def _record_execution_pool_basis(
    state: _TimelineBuildState,
    application: _ExecutionApplication,
) -> None:
    """Record post-fill pool state and any newly unavailable cost basis."""
    outcome = application.outcome
    state.pools[application.pool_key] = _Pool(
        outcome.position_qty,
        outcome.entry_price,
    )
    if not application.price_is_trusted and abs(outcome.position_qty) >= FLAT_EPSILON:
        price_reason = (
            application.execution.price_incompleteness_reason or "execution_price_invalid"
        )
        state.basis_reasons_by_pool.setdefault(application.pool_key, set()).add(price_reason)
    if (
        abs(outcome.position_qty) >= FLAT_EPSILON
        and (outcome.entry_price is None or not math.isfinite(outcome.entry_price))
        and not state.basis_reasons_by_pool.get(application.pool_key)
    ):
        state.basis_reasons_by_pool.setdefault(application.pool_key, set()).add(
            "cost_basis_unavailable"
        )


def _record_execution_realized(
    state: _TimelineBuildState,
    application: _ExecutionApplication,
) -> None:
    """Accumulate realized P&L and allocate any closed quantity ownership."""
    outcome = application.outcome
    realized_delta = (
        0.0
        if outcome.closed_qty > 0.0 and application.pre_fill_basis_reasons
        else outcome.realized_delta
    )
    state.realized_by_instrument[application.instrument_public_id] += realized_delta
    state.realized_total += realized_delta
    if outcome.closed_qty > 0.0:
        allocation = _allocate_by_weights(
            realized_delta,
            application.pre_fill_weights,
        )
        _add_allocations(state.realized_by_attribution, allocation)
        state.attribution_seen.update(allocation)


def _record_execution_fee(
    state: _TimelineBuildState,
    application: _ExecutionApplication,
) -> None:
    """Accumulate and allocate one execution fee with flip-aware ownership."""
    execution = application.execution
    outcome = application.outcome
    fee_pnl = 0.0 if execution.fee_incompleteness_reason is not None else -execution.fee
    state.fee_by_instrument[application.instrument_public_id] += fee_pnl
    state.fee_total += fee_pnl
    opened_opposite_side = outcome.closed_qty > 0.0 and outcome.opened_new_side
    if opened_opposite_side:
        closing_fee_pnl = fee_pnl * outcome.closed_qty / application.position_size
        allocation = _allocate_by_weights(
            closing_fee_pnl,
            application.pre_fill_weights,
        )
        _add_allocations(state.fee_by_attribution, allocation)
        state.attribution_seen.update(allocation)
        state.fee_by_attribution[application.attribution_key] += fee_pnl - closing_fee_pnl
    elif outcome.closed_qty > 0.0:
        allocation = _allocate_by_weights(
            fee_pnl,
            application.pre_fill_weights,
        )
        _add_allocations(state.fee_by_attribution, allocation)
        state.attribution_seen.update(allocation)
    else:
        state.fee_by_attribution[application.attribution_key] += fee_pnl


def _execution_post_fill_weights(
    application: _ExecutionApplication,
) -> Mapping[AttributionKey, float]:
    """Return candidate ownership weights after one average-cost transition."""
    outcome = application.outcome
    opened_opposite_side = outcome.closed_qty > 0.0 and outcome.opened_new_side
    if opened_opposite_side:
        return {application.attribution_key: outcome.added_qty}
    if outcome.closed_qty > 0.0:
        return _remaining_weights(
            application.pre_fill_weights,
            outcome.closed_qty,
            abs(outcome.position_qty),
        )
    if outcome.added_qty > 0.0:
        post_fill_weights = dict(application.pre_fill_weights)
        post_fill_weights[application.attribution_key] = (
            post_fill_weights.get(application.attribution_key, 0.0) + outcome.added_qty
        )
        return post_fill_weights
    return application.pre_fill_weights


def _record_execution_weights_and_basis(
    state: _TimelineBuildState,
    application: _ExecutionApplication,
) -> None:
    """Reconcile ownership and latch or clear pre-existing basis causes."""
    outcome = application.outcome
    post_fill_weights = _execution_post_fill_weights(application)
    state.weights_by_pool[application.pool_key] = _reconcile_weights(
        post_fill_weights,
        abs(outcome.position_qty),
    )
    state.attribution_seen.update(state.weights_by_pool[application.pool_key])
    pool_basis_reasons = state.basis_reasons_by_pool.get(application.pool_key)
    if not pool_basis_reasons:
        return
    if outcome.closed_qty > 0.0:
        for basis_reason in pool_basis_reasons:
            _add_instrument_untrusted_reason(
                state.untrusted_reasons_by_instrument,
                application.instrument_public_id,
                basis_reason,
            )
    if abs(outcome.position_qty) < FLAT_EPSILON:
        state.basis_reasons_by_pool.pop(application.pool_key, None)


def _apply_execution_event(
    state: _TimelineBuildState,
    evidence: _TimelineBuildEvidence,
    execution: TimelineExecution,
) -> None:
    """Apply one execution after all same-time accruals."""
    application = _begin_execution_application(state, evidence, execution)
    if application is None:
        return
    _record_execution_pool_basis(state, application)
    _record_execution_realized(state, application)
    _record_execution_fee(state, application)
    _record_execution_weights_and_basis(state, application)


def _apply_accrual_event(
    state: _TimelineBuildState,
    pool_index: PoolIndex,
    accrual: TimelineAccrual,
    attribution_is_ambiguous: bool,
) -> None:
    """Apply one accrual against strictly earlier inventory ownership."""
    if accrual.incompleteness_reason is not None:
        _add_instrument_untrusted_reason(
            state.untrusted_reasons_by_instrument,
            accrual.instrument_public_id,
            accrual.incompleteness_reason,
        )
    accrual_pnl = 0.0 if accrual.incompleteness_reason is not None else -accrual.amount_usd
    state.accrual_by_instrument[accrual.instrument_public_id] += accrual_pnl
    state.accrual_total += accrual_pnl
    state.seen.add(accrual.instrument_public_id)
    if attribution_is_ambiguous:
        allocation = {_UNATTRIBUTED_KEY: accrual_pnl}
    else:
        accrual_weights = _combined_instrument_weights(
            pool_index.get(accrual.instrument_public_id, ()),
            state.weights_by_pool,
        )
        allocation = _allocate_by_weights(accrual_pnl, accrual_weights)
    _add_allocations(state.accrual_by_attribution, allocation)
    state.attribution_seen.update(allocation)


def _next_event_time(
    schedule: _TimelineSchedule,
    cursor: _EventCursor,
) -> datetime | None:
    """Return the next execution/accrual instant without advancing indexes."""
    pending_times: list[datetime] = []
    if cursor.execution_index < len(schedule.prepared):
        pending_times.append(schedule.prepared[cursor.execution_index].effective_time)
    if cursor.accrual_index < len(schedule.accruals):
        pending_times.append(schedule.accruals[cursor.accrual_index].accrued_at)
    return min(pending_times) if pending_times else None


def _execution_group_end(
    prepared: Sequence[_PreparedExecution],
    start: int,
    event_time: datetime,
) -> int:
    """Return the exclusive end of equal-effective-time executions."""
    end = start
    while end < len(prepared) and prepared[end].effective_time == event_time:
        end += 1
    return end


def _accrual_group_end(
    accruals: Sequence[TimelineAccrual],
    start: int,
    event_time: datetime,
) -> int:
    """Return the exclusive end of equal-time accruals."""
    end = start
    while end < len(accruals) and accruals[end].accrued_at == event_time:
        end += 1
    return end


def _apply_scheduled_event_time(
    state: _TimelineBuildState,
    evidence: _TimelineBuildEvidence,
    schedule: _TimelineSchedule,
    cursor: _EventCursor,
    event_time: datetime,
) -> None:
    """Apply one exact-time accrual-first event group and advance cursors."""
    execution_end = _execution_group_end(
        schedule.prepared,
        cursor.execution_index,
        event_time,
    )
    accrual_end = _accrual_group_end(
        schedule.accruals,
        cursor.accrual_index,
        event_time,
    )
    for accrual in schedule.accruals[cursor.accrual_index : accrual_end]:
        _apply_accrual_event(
            state,
            schedule.pool_index,
            accrual,
            accrual.instrument_public_id in schedule.position_changes.get(event_time, set()),
        )
    for item in schedule.prepared[cursor.execution_index : execution_end]:
        _apply_execution_event(state, evidence, item.execution)
    cursor.execution_index = execution_end
    cursor.accrual_index = accrual_end


def _apply_events_through_point(
    state: _TimelineBuildState,
    evidence: _TimelineBuildEvidence,
    schedule: _TimelineSchedule,
    cursor: _EventCursor,
    point_time: datetime,
) -> None:
    """Advance the merged event stream through one inclusive grid instant."""
    while True:
        event_time = _next_event_time(schedule, cursor)
        if event_time is None or event_time > point_time:
            return
        _apply_scheduled_event_time(
            state,
            evidence,
            schedule,
            cursor,
            event_time,
        )


def _point_global_reasons(
    activation_time: datetime | None,
    point_time: datetime,
    active_shadow_trigger: str | None,
) -> set[PnlIncompletenessReasonEntry]:
    """Return global activation and regression causes for one point."""
    reasons: set[PnlIncompletenessReasonEntry] = set()
    after_activation = activation_time is None or point_time > activation_time
    if after_activation and active_shadow_trigger is not None:
        reasons.add(
            _global_incompleteness_reason(
                "scope_order_regression",
                "untrusted",
                active_shadow_trigger,
            )
        )
    if activation_time is not None and point_time < activation_time:
        reasons.add(
            _global_incompleteness_reason(
                "before_activation",
                "untrusted",
            )
        )
    return reasons


def _point_valuation_context(
    state: _TimelineBuildState,
    evidence: _TimelineBuildEvidence,
    pool_index: PoolIndex,
    point_time: datetime,
    global_reasons: Collection[PnlIncompletenessReasonEntry],
) -> _PointValuationContext:
    """Snapshot mutable replay state for immediate point valuation."""
    return _PointValuationContext(
        point_time=point_time,
        pools=state.pools,
        pool_index=pool_index,
        weights_by_pool=state.weights_by_pool,
        marks=evidence.marks,
        seen=sorted(state.seen),
        attribution_seen=_sorted_attribution_keys(state.attribution_seen),
        cumulatives=_PointCumulatives(
            realized_by_instrument=state.realized_by_instrument,
            fee_by_instrument=state.fee_by_instrument,
            accrual_by_instrument=state.accrual_by_instrument,
            realized_by_attribution=state.realized_by_attribution,
            fee_by_attribution=state.fee_by_attribution,
            accrual_by_attribution=state.accrual_by_attribution,
            realized_total=state.realized_total,
            fee_total=state.fee_total,
            accrual_total=state.accrual_total,
        ),
        activation_time=state.activation_time,
        global_reasons=global_reasons,
        untrusted_reasons_by_instrument=state.untrusted_reasons_by_instrument,
        basis_reasons_by_pool=state.basis_reasons_by_pool,
        mark_incompleteness_reasons=evidence.mark_incompleteness_reasons,
    )


def _build_minute_points(
    state: _TimelineBuildState,
    evidence: _TimelineBuildEvidence,
    schedule: _TimelineSchedule,
) -> list[PnlTimelinePoint]:
    """Walk the minute grid while preserving event and withholding order."""
    cursor = _EventCursor()
    minute_points: list[PnlTimelinePoint] = []
    active_shadow_trigger: str | None = None
    for point_index, point_time in enumerate(schedule.minute_grid):
        if state.activation_time is None or point_time > state.activation_time:
            _apply_events_through_point(
                state,
                evidence,
                schedule,
                cursor,
                point_time,
            )
        if point_index in schedule.shadow_trigger_changes:
            active_shadow_trigger = schedule.shadow_trigger_changes[point_index]
        global_reasons = _point_global_reasons(
            state.activation_time,
            point_time,
            active_shadow_trigger,
        )
        context = _point_valuation_context(
            state,
            evidence,
            schedule.pool_index,
            point_time,
            global_reasons,
        )
        minute_points.append(_value_point(context))
    return minute_points


def _granularity_step(granularity: str) -> int:
    """Return the supported bucket width or reject the request."""
    step = _GRANULARITY_MINUTES.get(granularity)
    if step is None:
        raise ValueError(f"unsupported granularity: {granularity!r}")
    return step


def build_pnl_timeline(
    executions: Sequence[TimelineExecution],
    accruals: Sequence[TimelineAccrual],
    marks: MarkMap,
    window: TimelineWindow,
    opening: TimelineOpening | None = None,
    lineage: Mapping[str, TimelineExecutionLineage] | None = None,
    untrusted_price_reasons_by_instrument: (
        Mapping[str, Collection[PnlIncompletenessReason]] | None
    ) = None,
    mark_incompleteness_reasons: MarkIncompletenessReasonMap | None = None,
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
        accruals: Funding accruals for the scope, valuation-currency-signed.
        marks: The injected valuation-currency mark lookup keyed by instrument
            and minute.
        window: The requested from/to/granularity/valuation-currency window.
        opening: The activation anchor seeding the replay, or ``None`` to replay
            from empty pools. Derived opening pools are already rebased to t0.
        lineage: Order-keyed initiating command and signal lineage. Missing or
            ambiguous orders are intentionally absent and become unattributed.
        untrusted_price_reasons_by_instrument: Caller-stamped execution-price
            proof failures keyed by instrument. Their fills are never passed to
            the accounting kernel and their exact reasons latch when replay
            reaches them.
        mark_incompleteness_reasons: Exact caller-stamped causes for mark values
            omitted during conversion. Other absent or non-finite marks are
            classified only as unavailable when the builder needs them.

    Returns:
        The built :class:`PnlTimelineResult` at the requested granularity.

    Raises:
        ValueError: When ``window.granularity`` is not a supported value.
    """
    step = _granularity_step(window.granularity)
    evidence = _resolved_build_evidence(
        marks,
        lineage,
        untrusted_price_reasons_by_instrument,
        mark_incompleteness_reasons,
    )
    state = _initialize_timeline_state(opening)
    _validate_timeline_execution_scopes(state, executions)
    schedule = _timeline_schedule(state, executions, accruals, window)
    minute_points = _build_minute_points(state, evidence, schedule)
    return PnlTimelineResult(
        points=tuple(_downsample(minute_points, step)),
        granularity=window.granularity,
        valuation_ccy=window.valuation_ccy,
    )
