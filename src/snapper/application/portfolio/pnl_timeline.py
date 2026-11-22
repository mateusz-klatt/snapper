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
  realized / fee / accrual are still returned because they are mark-independent.
  This builder never reaches for an earlier mark itself: it values only the marks
  it is given and treats a non-finite one as absent. On a carried read the caller
  supplies earlier closes and the point discloses them in ``carried_inputs``.
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
from collections.abc import Callable
from collections.abc import Collection
from collections.abc import Hashable
from collections.abc import Iterable
from collections.abc import Iterator
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
from snapper.application.portfolio.exact_sum import ExactSum
from snapper.application.portfolio.exact_sum import exact_units_of
from snapper.application.portfolio.exact_sum import faithful_roundings
from snapper.application.portfolio.exact_sum import project_units
from snapper.application.portfolio.pnl_adjusted_total import AdjustedFlowProjection
from snapper.application.portfolio.pnl_faithful_recovery import MAX_RECOVERY_KEY_VISITS
from snapper.application.portfolio.pnl_faithful_recovery import MAX_RECOVERY_KEYS
from snapper.application.portfolio.pnl_faithful_recovery import FaithfulFlowInput
from snapper.application.portfolio.pnl_faithful_recovery import FaithfulFlowRecovery
from snapper.application.portfolio.pnl_faithful_recovery import FaithfulRecoveryBudget
from snapper.core.numeric import is_positive_finite

MAX_REPLAY_RECOVERY_KEY_VISITS: Final = MAX_RECOVERY_KEY_VISITS
"""Cap each physical replay chunk while allowing a single point's full allowance."""


class PnlTimelineWorkBudgetError(ValueError):
    """A timeline request or replay chunk exceeds its work budget."""


ValuationStatus = Literal["complete", "incomplete"]
"""Whether a point's mark-to-market valuation is trustworthy or withheld."""

CheckpointRefusalReason = Literal[
    "carried_event_inputs",
    "carried_inputs_on_processed_event",
    "conversion_uses_mark_evidence",
    "global_withhold",
]
"""Machine-readable reasons why optional checkpoint capture returned ``None``.

``"global_withhold"`` indicates that a downstream service wrapper replaced a
built series with globally withheld points across the timeline.
"""

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

PnlCarriedInputRole = Literal["mark", "fx", "event"]
"""Which valuation input reached back to an earlier published minute.

``mark`` is the instrument's own close, ``fx`` is the rate that converted a mark,
and ``event`` is the rate that converted a fill price, fee or accrual.
"""


@dataclass(frozen=True)
class PnlCarriedInput:
    """One valuation input served by an earlier close instead of its own minute.

    Attributes:
        instrument_public_id: Instrument whose value depends on the input.
        role: Which input was carried.
        source_minute: Bar-close minute of the evidence actually used.
        age_minutes: Whole minutes between the valued minute and the evidence.
        source_plane: Oriented FX plane label for an FX input, ``None`` for a mark.
    """

    instrument_public_id: str
    role: PnlCarriedInputRole
    source_minute: datetime
    age_minutes: int
    source_plane: str | None = None

    def __post_init__(self) -> None:
        """Reject a carried claim that is not actually older than its minute."""
        if self.age_minutes <= 0:
            raise ValueError("a carried input must be older than the minute it values")


def _carried_input_rank(item: PnlCarriedInput) -> tuple[int, str, str]:
    """Order inputs of one instrument and role by age, then deterministically."""
    return item.age_minutes, item.source_plane or "", item.source_minute.isoformat()


def canonical_carried_inputs(
    inputs: Collection[PnlCarriedInput],
) -> tuple[PnlCarriedInput, ...]:
    """Keep the oldest input per instrument and role in a stable order.

    A point discloses its largest carry age, so one entry per instrument and
    role states it without letting long histories inflate every response.

    Args:
        inputs: Carried inputs the point depends on.

    Returns:
        One entry per ``(instrument, role)``, ordered by that key.
    """
    oldest: dict[tuple[str, str], PnlCarriedInput] = {}
    for item in inputs:
        key = (item.instrument_public_id, item.role)
        current = oldest.get(key)
        if current is None or _carried_input_rank(item) > _carried_input_rank(current):
            oldest[key] = item
    return tuple(oldest[key] for key in sorted(oldest))


type MarkCarriedInputMap = Mapping[tuple[str, datetime], Sequence[PnlCarriedInput]]
"""Carried inputs behind a mark, keyed by ``(instrument_public_id, point_minute)``."""


@dataclass(frozen=True)
class TimelineMarkEvidence:
    """Caller-stamped facts about the marks injected into the builder.

    Attributes:
        incompleteness_reasons: Exact causes for marks omitted during conversion.
            Other absent or non-finite marks are classified as unavailable.
        carried_inputs: Carried inputs behind each mark that reached back.
        widen_unrealized_absorber: Let any bucket absorb the last-place rounding
            when unrealized values reconcile, so that a dust final bucket cannot
            withhold a point. Carried reads set it; exact-only callers keep the
            persisted sample contract.
    """

    incompleteness_reasons: MarkIncompletenessReasonMap = field(default_factory=dict)
    carried_inputs: MarkCarriedInputMap = field(default_factory=dict)
    widen_unrealized_absorber: bool = False


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
        carried_inputs: Rates this fill's price or fee reached back for.
        conversion_uses_mark_evidence: Whether converting this fill relied on
            mark-evidence fallbacks incompatible with checkpointing.
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
    carried_inputs: tuple[PnlCarriedInput, ...] = ()
    conversion_uses_mark_evidence: bool = False


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
        carried_inputs: Rates this accrual reached back for.
        conversion_uses_mark_evidence: Whether converting this accrual relied on
            mark-evidence fallbacks incompatible with checkpointing.
    """

    instrument_public_id: str
    accrued_at: datetime
    amount_usd: float
    incompleteness_reason: PnlIncompletenessReason | None = None
    carried_inputs: tuple[PnlCarriedInput, ...] = ()
    conversion_uses_mark_evidence: bool = False


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


def _validate_utc_minute(instant: datetime, label: str) -> None:
    """Require one timestamp to be aligned to a UTC minute."""
    if instant.utcoffset() != timedelta(0) or instant.second != 0 or instant.microsecond != 0:
        raise ValueError(f"{label} must be aligned to a UTC minute")


def _require_unique_checkpoint_keys(keys: Collection[Hashable], label: str) -> None:
    """Reject duplicate keys in carried checkpoint state collections."""
    if len(keys) != len(set(keys)):
        raise ValueError(f"duplicate checkpoint keys in {label}")


def _validate_deferred_execution_keys(
    deferred_execution_keys: Sequence[tuple[str, int]],
) -> None:
    """Require canonical strictly sorted unique ``(exchange, scope_sequence)`` keys."""
    for exchange, scope_sequence in deferred_execution_keys:
        if not exchange or isinstance(scope_sequence, bool) or scope_sequence < 1:
            raise ValueError("checkpoint deferred_execution_keys must have positive sequences")
    for index in range(len(deferred_execution_keys) - 1):
        if not deferred_execution_keys[index] < deferred_execution_keys[index + 1]:
            raise ValueError("checkpoint deferred_execution_keys must be sorted and unique")


def _pool_keys_map(
    pool_keys_by_instrument: Sequence[tuple[str, tuple[PoolKey, ...]]],
) -> dict[str, set[PoolKey]]:
    """Build instrument-to-pool-key lookup while rejecting cross-instrument pool entries."""
    mapping: dict[str, set[PoolKey]] = {}
    for instrument_public_id, pool_keys in pool_keys_by_instrument:
        for pool_key in pool_keys:
            if pool_key[0] != instrument_public_id:
                raise ValueError(
                    "checkpoint pool_keys_by_instrument contains mismatched instrument"
                )
        mapping[instrument_public_id] = set(pool_keys)
    return mapping


@dataclass(frozen=True)
class TimelineCheckpointPool:
    """One durable shard pool's carried quantity and cost basis at a checkpoint.

    Attributes:
        instrument_public_id: Stable instrument identity used for marks/output.
        shard_key: Exact durable ``fill_observed`` shard identity.
        position_qty: Signed position quantity carried at ``checkpoint_minute``.
        entry_price: Carried average-cost basis, or ``None`` when flat or
            unknown.
    """

    instrument_public_id: str
    shard_key: str
    position_qty: float
    entry_price: float | None


@dataclass(frozen=True)
class TimelineCheckpointOpening:
    """Carried pure-engine state captured at a settled UTC minute boundary.

    Wraps the engine's replay state at ``checkpoint_minute`` so
    :func:`build_pnl_timeline` can resume subsequent minutes bit-identically
    without replaying earlier events. ``last_effective_by_pool`` is retained as
    defence in depth, since every carried value is at or before
    ``checkpoint_minute`` and deferred fill re-passes rebuild those clamps.
    Hashing is explicitly unsupported (``__hash__ = None``) because carried
    state holds unhashable :class:`ExactSum` accumulators.
    """

    checkpoint_minute: datetime
    activation_time: datetime | None
    valuation_ccy: str
    pools: tuple[TimelineCheckpointPool, ...]
    weights_by_pool: tuple[tuple[PoolKey, tuple[tuple[AttributionKey, float], ...]], ...]
    seen: tuple[str, ...]
    attribution_seen: tuple[AttributionKey, ...]
    basis_reasons_by_pool: tuple[tuple[PoolKey, tuple[PnlIncompletenessReason, ...]], ...]
    untrusted_reasons_by_instrument: tuple[
        tuple[str, tuple[PnlIncompletenessReasonEntry, ...]],
        ...,
    ]
    scope_by_shard: tuple[tuple[str, tuple[str, str]], ...]
    exchange_by_instrument: tuple[tuple[str, str], ...]
    pool_keys_by_instrument: tuple[tuple[str, tuple[PoolKey, ...]], ...]
    realized_by_instrument: tuple[tuple[str, ExactSum], ...]
    fee_by_instrument: tuple[tuple[str, ExactSum], ...]
    accrual_by_instrument: tuple[tuple[str, ExactSum], ...]
    realized_by_attribution: tuple[tuple[AttributionKey, ExactSum], ...]
    fee_by_attribution: tuple[tuple[AttributionKey, ExactSum], ...]
    accrual_by_attribution: tuple[tuple[AttributionKey, ExactSum], ...]
    realized_total: ExactSum
    fee_total: ExactSum
    accrual_total: ExactSum
    last_effective_by_pool: tuple[tuple[PoolKey, datetime], ...]
    deferred_execution_keys: tuple[tuple[str, int], ...] = ()
    __hash__ = cast(Callable[[object], int], None)

    def __post_init__(self) -> None:
        """Validate currency, boundary alignment, ordering, key uniqueness, and pool consistency."""
        if not self.valuation_ccy:
            raise ValueError("checkpoint valuation_ccy must be non-empty")
        _validate_utc_minute(self.checkpoint_minute, "checkpoint_minute")
        if self.activation_time is not None:
            _validate_utc_minute(self.activation_time, "activation_time")
            if self.checkpoint_minute <= self.activation_time:
                raise ValueError("checkpoint_minute must follow activation_time")
        key_groups: tuple[tuple[str, Collection[Hashable]], ...] = (
            ("pools", [(pool.instrument_public_id, pool.shard_key) for pool in self.pools]),
            ("weights_by_pool", [pool_key for pool_key, _ in self.weights_by_pool]),
            ("seen", self.seen),
            ("attribution_seen", self.attribution_seen),
            ("basis_reasons_by_pool", [pool_key for pool_key, _ in self.basis_reasons_by_pool]),
            (
                "untrusted_reasons_by_instrument",
                [inst for inst, _ in self.untrusted_reasons_by_instrument],
            ),
            ("scope_by_shard", [shard for shard, _ in self.scope_by_shard]),
            ("exchange_by_instrument", [inst for inst, _ in self.exchange_by_instrument]),
            ("pool_keys_by_instrument", [inst for inst, _ in self.pool_keys_by_instrument]),
            ("realized_by_instrument", [inst for inst, _ in self.realized_by_instrument]),
            ("fee_by_instrument", [inst for inst, _ in self.fee_by_instrument]),
            ("accrual_by_instrument", [inst for inst, _ in self.accrual_by_instrument]),
            ("realized_by_attribution", [key for key, _ in self.realized_by_attribution]),
            ("fee_by_attribution", [key for key, _ in self.fee_by_attribution]),
            ("accrual_by_attribution", [key for key, _ in self.accrual_by_attribution]),
            ("last_effective_by_pool", [pool_key for pool_key, _ in self.last_effective_by_pool]),
        )
        for label, keys in key_groups:
            _require_unique_checkpoint_keys(keys, label)
        for _, effective_time in self.last_effective_by_pool:
            if (
                effective_time.utcoffset() != timedelta(0)
                or effective_time > self.checkpoint_minute
            ):
                raise ValueError(
                    "checkpoint last_effective_by_pool must be UTC at or before checkpoint_minute"
                )
        _validate_deferred_execution_keys(self.deferred_execution_keys)
        _validate_checkpoint_pool_index(self)


def _validate_checkpoint_pool_index(opening: TimelineCheckpointOpening) -> None:
    """Verify every carried pool key matches ``pool_keys_by_instrument``, ``seen``, and scopes."""
    pool_keys_map = _pool_keys_map(opening.pool_keys_by_instrument)
    seen_set = set(opening.seen)
    scope_map = dict(opening.scope_by_shard)
    exchange_map = dict(opening.exchange_by_instrument)
    carried_pools: tuple[PoolKey, ...] = (
        *((pool.instrument_public_id, pool.shard_key) for pool in opening.pools),
        *(pool_key for pool_key, _ in opening.weights_by_pool),
        *(pool_key for pool_key, _ in opening.basis_reasons_by_pool),
        *(pool_key for pool_key, _ in opening.last_effective_by_pool),
    )
    for instrument_public_id, shard_key in dict.fromkeys(carried_pools):
        if (instrument_public_id, shard_key) not in pool_keys_map.get(instrument_public_id, ()):
            raise ValueError("carried pool key missing from pool_keys_by_instrument")
        exchange = exchange_map.get(instrument_public_id)
        if exchange is None or scope_map.get(shard_key) != (instrument_public_id, exchange):
            raise ValueError("carried pool key has missing or inconsistent shard/exchange scope")
    for instrument_public_id in pool_keys_map:
        if instrument_public_id not in seen_set:
            raise ValueError("carried pool instrument missing from seen")


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
        recovery_budget_per_point: Give each persisted sample its own recovery
            allowance and terminal state, independent of the replay chunk.
        capture_checkpoint: Capture the engine's carried replay state after
            valuing ``to_time`` and return it on :attr:`PnlTimelineResult.closing_checkpoint`.
        compensated_unrealized_fallback: Fall back to compensated unrealized
            totals and attribution buckets when naive instrument reconciliation
            fails; set by the service only for pointwise 5C.6 snapshotter samples.
        public_read: Enable the ``5A.18`` ``(a″)`` adjusted-total fallback for
            public API reads after every faithful flow-recovery pass completes
            without a projection.
    """

    from_time: datetime
    to_time: datetime
    granularity: str
    valuation_ccy: str
    recovery_budget_per_point: bool = False
    capture_checkpoint: bool = False
    compensated_unrealized_fallback: bool = False
    public_read: bool = False


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
        carried_inputs: Inputs of this value that reached back to an earlier
            minute, one per instrument and role. A ``complete`` point with entries
            is complete with carried inputs; the status enum stays closed so
            existing clients keep plotting it as a value.
        total_rounding_adjusted: Whether at least one flow family at this point
            published under the ``(a″)`` adjusted-total fallback.
        total_deviation_ulps: Maximum ``|published - exact| / u`` across adjusted
            flow families at this point, or ``None`` when unadjusted.
        absorber_deviation_ulps: Maximum ``|absorber - exact| / u`` across
            adjusted flow families at this point, or ``None`` when unadjusted.
        instrument_absorber_id: Instrument key of the chosen absorber when an
            adjusted family placed the absorber in the instrument dimension.
        attribution_absorber_key: Composite attribution key of the chosen absorber
            when an adjusted family placed the absorber in attribution.
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
    carried_inputs: tuple[PnlCarriedInput, ...] = ()
    total_rounding_adjusted: bool = False
    total_deviation_ulps: float | None = None
    absorber_deviation_ulps: float | None = None
    instrument_absorber_id: str | None = None
    attribution_absorber_key: AttributionKey | None = None

    def __post_init__(self) -> None:
        """Enforce reason/status equivalence and canonical input ordering."""
        canonical = canonical_incompleteness_reasons(self.incompleteness_reasons)
        if self.incompleteness_reasons != canonical:
            raise ValueError("incompleteness reasons must be deduplicated and sorted")
        if self.valuation_status == "complete" and canonical:
            raise ValueError("a complete point cannot carry incompleteness reasons")
        if self.valuation_status == "incomplete" and not canonical:
            raise ValueError("an incomplete point must carry an incompleteness reason")
        if self.carried_inputs != canonical_carried_inputs(self.carried_inputs):
            raise ValueError("carried inputs must be one per instrument and role, sorted")

    @property
    def max_carry_age_minutes(self) -> int | None:
        """Return the largest carry age among this point's inputs.

        Returns:
            Whole minutes of the oldest carried input, or ``None`` when every
            input of the point was exact.
        """
        return max((item.age_minutes for item in self.carried_inputs), default=None)


@dataclass(frozen=True)
class PnlTimelineResult:
    """The built P&L series.

    A result without a checkpoint stays hashable exactly as before. Hashing a
    result that carries ``closing_checkpoint`` raises ``TypeError``, because the
    checkpoint holds unhashable :class:`ExactSum` accumulators.

    Attributes:
        points: The ordered series points at the requested granularity.
        granularity: The granularity the points are bucketed at.
        valuation_ccy: The currency the components are expressed in.
        closing_checkpoint: Captured engine state after valuing ``to_time`` when
            ``window.capture_checkpoint`` was requested and succeeded, otherwise
            ``None``.
        closing_checkpoint_refusal: Machine-readable refusal reason when
            ``window.capture_checkpoint`` was requested and declined, otherwise
            ``None``.
    """

    points: tuple[PnlTimelinePoint, ...]
    granularity: str
    valuation_ccy: str
    closing_checkpoint: TimelineCheckpointOpening | None = field(default=None, kw_only=True)
    closing_checkpoint_refusal: CheckpointRefusalReason | None = field(default=None, kw_only=True)


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


def _allocate_flow(
    target: defaultdict[AttributionKey, ExactSum],
    amount: float,
    weights: Mapping[AttributionKey, float],
) -> Mapping[AttributionKey, float]:
    """Allocate one flow delta into exact attribution buckets, conserving units.

    The float shares for all but the last key are exactly the shares
    :func:`_allocate_by_weights` computes, but the last bucket receives the
    exact unit residue of the whole amount instead of that function's rounded
    float, so the buckets sum to the amount BY CONSTRUCTION — the same
    construction that lets the point publication reconcile families without a
    tolerance. Every fallback inside the share computation puts the whole
    amount in one bucket, which conserves trivially, and a non-finite amount
    rides through :meth:`ExactSum.add` so it latches instead of raising.

    Args:
        target: The exact per-key cumulative buckets to accumulate into.
        amount: The flow delta to split.
        weights: Candidate positive ownership weights; empty forces the
            unattributed fallback, which is how ambiguous ownership is spelled.

    Returns:
        The float shares by key, for seen-key tracking by the caller.
    """
    shares = _allocate_by_weights(amount, weights)
    keys = _sorted_attribution_keys(set(shares))
    final_key = keys[-1]
    if not math.isfinite(amount):
        target[final_key].add(shares[final_key])
        return shares
    residue_units = exact_units_of(amount)
    for key in keys[:-1]:
        target[key].add(shares[key])
        residue_units -= exact_units_of(shares[key])
    target[final_key].add_units(residue_units)
    return shares


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
    *,
    widen: bool = False,
) -> dict[KeyT, float] | None:
    """Reconcile values through one bucket or fail if floats cannot represent it.

    The accumulated final value is tried first. Its adjacent floats are also
    tried because the later stable-order sum can round in the opposite direction
    by one unit in the last place. A direct residual farther away is not proof of
    ownership and is never transported into a bucket. If no allowed representation
    sums exactly, returning ``None`` lets the caller withhold the point instead of
    publishing a fabricated keyed contribution.

    A dust final bucket cannot absorb that unit, because its own last place is far
    smaller than the sum's. ``widen`` then lets each other bucket take the role in
    turn, as :func:`_reconciled_buckets` already does for flows. Exactly one
    bucket moves, by at most one unit in the last place, and a point the final
    bucket can reconcile is returned unchanged.

    Args:
        values: Cumulative values before point-level reconciliation.
        keys: Caller-provided deterministic key order.
        total: Aggregate value the buckets must equal exactly.
        widen: Whether every bucket may absorb the rounding, not only the final.

    Returns:
        Reconciled values, or ``None`` when exact float reconciliation fails.
    """
    if not keys:
        return {} if _is_exactly_zero(total) else None
    reconciled = {key: values.get(key, 0.0) for key in keys}
    if not math.isfinite(total) or not math.isfinite(reconciled[keys[-1]]):
        return None
    last = len(keys) - 1
    for position in (last, *range(last)) if widen else (last,):
        key = keys[position]
        original = reconciled[key]
        for candidate in (
            original,
            math.nextafter(original, math.inf),
            math.nextafter(original, -math.inf),
        ):
            reconciled[key] = candidate
            if sum(reconciled[item] for item in keys) == total:
                return reconciled
        reconciled[key] = original
    return None


def _reconciled_buckets[KeyT: Hashable](
    units_by_key: Mapping[KeyT, int],
    keys: Sequence[KeyT],
    total: float,
    widen: bool,
) -> dict[KeyT, float] | None:
    """Project exact buckets onto doubles whose stable-order sum equals the total.

    At most ONE bucket deviates from its own correct rounding, and by at most
    one unit in the last place — the absorber for the stable-order compensated
    sum rounding the opposite way. The narrow pass gives that role to the
    final key alone, which is the historical contract every published point
    was chosen under. The widened pass lets each bucket take the role in
    turn, final first, because the flexibility a point needs can sit in a
    non-final bucket: production's 2026-08-04 22:04Z constellation holds the
    whole attribution total in its first bucket while the final bucket is an
    exact zero whose neighbours are absorbed. Anything farther than a
    neighbour is not a rounding of the bucket's exact value and is never
    published.

    An empty key list is only reachable with a zero exact total, whose one
    faithful rounding is ``0.0``, so it needs no total check of its own.

    Args:
        units_by_key: Exact per-key unit counts; absent keys hold zero units.
        keys: Caller-provided deterministic key order.
        total: The faithful-rounding candidate the buckets must sum to.
        widen: Whether every bucket may take the absorber role, not just the
            final one.

    Returns:
        The published buckets, or ``None`` when no allowed single-bucket
        rounding makes the float sum land exactly on the candidate total.
    """
    if not keys:
        return {}
    nearest = {key: project_units(units_by_key.get(key, 0)) for key in keys}
    last = len(keys) - 1
    positions = (last, *range(last)) if widen else (last,)
    for position in positions:
        published = dict(nearest)
        base = nearest[keys[position]]
        for candidate in (
            base,
            math.nextafter(base, math.inf),
            math.nextafter(base, -math.inf),
        ):
            published[keys[position]] = candidate
            if sum(published[key] for key in keys) == total:
                return published
    return None


@dataclass(frozen=True)
class _ExactCumulatives:
    """One flow family's exact accumulators, shared with the live replay state.

    The references are the mutable accumulators themselves, not copies; that is
    safe for the same reason the float snapshot has always shared live dicts —
    every point is valued immediately after its events apply and before the
    next event mutates anything.
    """

    by_instrument: Mapping[str, ExactSum]
    by_attribution: Mapping[AttributionKey, ExactSum]
    total: ExactSum


@dataclass(frozen=True)
class _PublishedFlowFamily:
    """One flow family's chosen representable projection for one point."""

    total: float
    by_instrument: Mapping[str, float]
    by_attribution: Mapping[AttributionKey, float]


def _published_flow_family(
    exact: _ExactCumulatives,
    instrument_keys: Sequence[str],
    attribution_keys: Sequence[AttributionKey],
) -> _PublishedFlowFamily | PnlIncompletenessReason:
    """Choose one self-consistent projection of a flow family, or refuse.

    Conservation is checked in units first, where it holds by construction:
    every delta lands in the total and in exactly one bucket of each
    dimension, and allocation residues travel as exact units. A violation can
    therefore only mean corrupted accumulation, and it fails closed under the
    ``*_reconciliation_failed`` reasons, which carry that suspicion. After
    conservation, the only remaining question is float representability: the
    published total must be a faithful rounding of the exact total, and both
    dimensions must reach it with the same candidate, since the point publishes
    one total that both breakdowns are checked against. The search runs in two
    passes: the narrow final-bucket-absorber pass first, alone, so every point
    that ever published keeps its exact published bits, and the widened
    any-single-bucket-absorber pass only for points the narrow pass withholds.

    Args:
        exact: The family's exact accumulators.
        instrument_keys: Deterministic instrument publication order.
        attribution_keys: Deterministic attribution publication order.

    Returns:
        The chosen projection, or the exact withhold reason.
    """
    total_units = exact.total.exact_units()
    instrument_units = {
        key: accumulator.exact_units() for key, accumulator in exact.by_instrument.items()
    }
    attribution_units = {
        key: accumulator.exact_units() for key, accumulator in exact.by_attribution.items()
    }
    if sum(instrument_units.get(key, 0) for key in instrument_keys) != total_units:
        return "instrument_reconciliation_failed"
    if sum(attribution_units.get(key, 0) for key in attribution_keys) != total_units:
        return "attribution_reconciliation_failed"
    instruments_failed_every_total = True
    for widen in (False, True):
        for total_candidate in faithful_roundings(total_units):
            instruments = _reconciled_buckets(
                instrument_units, instrument_keys, total_candidate, widen
            )
            if instruments is None:
                continue
            instruments_failed_every_total = False
            attributions = _reconciled_buckets(
                attribution_units, attribution_keys, total_candidate, widen
            )
            if attributions is None:
                continue
            return _PublishedFlowFamily(
                total=total_candidate,
                by_instrument=instruments,
                by_attribution=attributions,
            )
    if instruments_failed_every_total:
        return "instrument_sum_unrepresentable"
    return "attribution_sum_unrepresentable"


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
    initial_last_effective: Mapping[PoolKey, datetime] | None = None,
) -> tuple[list[_PreparedExecution], list[_RegressionShadow]]:
    """Clamp per-pool event times monotone and collect regression shadows.

    Each durable shard pool's fills are accumulated in caller-supplied scope order.
    A fill whose ``event_time`` regresses below the pool's running maximum
    is placed at that maximum (so scope order survives the grid walk) and the
    interval it shadows is recorded so those minutes can be flagged incomplete.

    Args:
        executions: Executions in ``(exchange, scope_sequence)`` order.
        initial_last_effective: Optional per-pool running maximum event times
            carried from an opening checkpoint.

    Returns:
        The prepared executions sorted for the grid walk, and the shadowed
        ``[economic_time, clamped_time)`` intervals produced by regressions.
    """
    last_effective: dict[PoolKey, datetime] = (
        {} if initial_last_effective is None else dict(initial_last_effective)
    )
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
    carried_inputs: tuple[PnlCarriedInput, ...] = (),
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
        carried_inputs: Carried inputs consulted before the point was withheld.

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
        carried_inputs=carried_inputs,
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


type _FlowRecovery = FaithfulFlowRecovery[str, AttributionKey]
type _FlowRecoveries = tuple[_FlowRecovery, _FlowRecovery, _FlowRecovery]


def _new_flow_recoveries(
    reserve_shared: Callable[[int], None] | None = None,
) -> _FlowRecoveries:
    """Give three families separate terminal entries and one shared allowance."""
    budget = FaithfulRecoveryBudget()
    budget.reserve_shared = reserve_shared
    return (
        FaithfulFlowRecovery[str, AttributionKey](budget),
        FaithfulFlowRecovery[str, AttributionKey](budget),
        FaithfulFlowRecovery[str, AttributionKey](budget),
    )


@dataclass(frozen=True)
class _PointCumulatives:
    """Immutable cumulative flows supplied to one point valuation.

    The float maps and totals are per-point projections of the exact
    accumulators — each value correctly rounded once — and every trust and
    finiteness screen still reads them. The exact families ride alongside for
    the publication step, which reconciles in units rather than comparing
    rounded floats against rounded floats.
    """

    realized_by_instrument: Mapping[str, float]
    fee_by_instrument: Mapping[str, float]
    accrual_by_instrument: Mapping[str, float]
    realized_by_attribution: Mapping[AttributionKey, float]
    fee_by_attribution: Mapping[AttributionKey, float]
    accrual_by_attribution: Mapping[AttributionKey, float]
    realized_total: float
    fee_total: float
    accrual_total: float
    realized_exact: _ExactCumulatives
    fee_exact: _ExactCumulatives
    accrual_exact: _ExactCumulatives
    flow_recoveries: _FlowRecoveries = field(default_factory=_new_flow_recoveries)


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
    mark_carried_inputs: MarkCarriedInputMap
    carried_event_inputs: Collection[PnlCarriedInput]
    widen_unrealized_absorber: bool = False
    compensated_unrealized_fallback: bool = False
    public_read: bool = False


@dataclass(frozen=True)
class _FlowAdjustmentDisclosure:
    """Point-level summary of flow families published under the ``(a″)`` fallback."""

    total_rounding_adjusted: bool = False
    total_deviation_ulps: float | None = None
    absorber_deviation_ulps: float | None = None
    instrument_absorber_id: str | None = None
    attribution_absorber_key: AttributionKey | None = None


@dataclass(frozen=True)
class _PointPreparation:
    """Validated and reconciled cumulative inputs for instrument valuation.

    When any instrument is untrusted the families carry raw per-key
    projections with no publication choice made, exactly as the raw maps were
    carried before; the untrusted point withholds every aggregate anyway and
    only the independently useful per-instrument rows are read.
    """

    instrument_untrusted_reasons: Mapping[
        str,
        Collection[PnlIncompletenessReasonEntry],
    ]
    realized: _PublishedFlowFamily
    fee: _PublishedFlowFamily
    accrual: _PublishedFlowFamily
    attribution_keys: Sequence[AttributionKey]
    flow_adjustment: _FlowAdjustmentDisclosure = field(default_factory=_FlowAdjustmentDisclosure)


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
    """Mutable point-local valuation outputs collected in stable order.

    ``unrealized_allocations_by_attribution`` stores per-key allocation shares
    in allocation order for the current point only (bounded by the point's
    allocation count) and is discarded each minute once valuation finishes.
    It is filled only when the compensated fallback is enabled, so readers that
    can never use it, such as the public timeline, pay nothing for it.
    """

    point_reasons: set[PnlIncompletenessReasonEntry] = field(default_factory=set)
    carried_inputs: set[PnlCarriedInput] = field(default_factory=set)
    contributions: list[PnlInstrumentContribution] = field(default_factory=list)
    unrealized_by_attribution: defaultdict[AttributionKey, float] = field(
        default_factory=lambda: defaultdict(float)
    )
    unrealized_allocations_by_attribution: defaultdict[AttributionKey, list[float]] = field(
        default_factory=lambda: defaultdict(list)
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
    carried_inputs: Collection[PnlCarriedInput] = (),
) -> PnlTimelinePoint:
    """Build a fully withheld point using stable identities from one context.

    A withheld point still names the carried inputs consulted before it was
    withheld, so an empty list on such a point never implies that none were used.
    """
    return _untrusted_point(
        context.point_time,
        context.seen,
        attribution_keys,
        reasons,
        canonical_carried_inputs([*context.carried_event_inputs, *carried_inputs]),
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


def _raw_point_flows(
    cumulatives: _PointCumulatives,
) -> tuple[_PublishedFlowFamily, _PublishedFlowFamily, _PublishedFlowFamily]:
    """Carry unreconciled projections for a point that withholds aggregates."""
    return (
        _PublishedFlowFamily(
            total=cumulatives.realized_total,
            by_instrument=cumulatives.realized_by_instrument,
            by_attribution=cumulatives.realized_by_attribution,
        ),
        _PublishedFlowFamily(
            total=cumulatives.fee_total,
            by_instrument=cumulatives.fee_by_instrument,
            by_attribution=cumulatives.fee_by_attribution,
        ),
        _PublishedFlowFamily(
            total=cumulatives.accrual_total,
            by_instrument=cumulatives.accrual_by_instrument,
            by_attribution=cumulatives.accrual_by_attribution,
        ),
    )


def _ordered_exact_units[KeyT: Hashable](
    accumulators: Mapping[KeyT, ExactSum], keys: Sequence[KeyT]
) -> tuple[tuple[KeyT, int], ...]:
    """Snapshot every ordered publication key, including absent exact-zero keys."""
    return tuple(
        (key, accumulators[key].exact_units() if key in accumulators else 0) for key in keys
    )


def _recover_flow_family_outcome(
    exact: _ExactCumulatives,
    instrument_keys: Sequence[str],
    attribution_keys: Sequence[AttributionKey],
    recovery: _FlowRecovery,
) -> tuple[_PublishedFlowFamily | None, bool]:
    """Try faithful recovery and report whether an exhaustive search completed."""
    if len(instrument_keys) + len(attribution_keys) > MAX_RECOVERY_KEYS:
        return None, False
    request = FaithfulFlowInput(
        exact.total.exact_units(),
        _ordered_exact_units(exact.by_instrument, instrument_keys),
        _ordered_exact_units(exact.by_attribution, attribution_keys),
    )
    projection = recovery.recover(request)
    if projection is not None:
        return (
            _PublishedFlowFamily(
                projection.total, dict(projection.instruments), dict(projection.attributions)
            ),
            True,
        )
    return None, recovery.completed_refusal(request)


def _recover_flow_family(
    exact: _ExactCumulatives,
    instrument_keys: Sequence[str],
    attribution_keys: Sequence[AttributionKey],
    recovery: _FlowRecovery,
) -> _PublishedFlowFamily | None:
    """Try faithful recovery only after the complete historical family search."""
    projection, _ = _recover_flow_family_outcome(exact, instrument_keys, attribution_keys, recovery)
    return projection


def _adjust_flow_family(
    exact: _ExactCumulatives,
    instrument_keys: Sequence[str],
    attribution_keys: Sequence[AttributionKey],
    recovery: _FlowRecovery,
) -> AdjustedFlowProjection[str, AttributionKey] | None:
    """Run the ``(a″)`` adjusted-total fallback through the family's single-entry cache."""
    return recovery.adjusted(
        FaithfulFlowInput(
            exact.total.exact_units(),
            _ordered_exact_units(exact.by_instrument, instrument_keys),
            _ordered_exact_units(exact.by_attribution, attribution_keys),
        )
    )


def _disclosure_from_adjustments(
    adjustments: Sequence[AdjustedFlowProjection[str, AttributionKey]],
) -> _FlowAdjustmentDisclosure:
    """Aggregate per-family ``(a″)`` adjustments into one point disclosure."""
    if not adjustments:
        return _FlowAdjustmentDisclosure()
    reported = max(adjustments, key=lambda item: item.total_deviation_ulps)
    return _FlowAdjustmentDisclosure(
        total_rounding_adjusted=True,
        total_deviation_ulps=reported.total_deviation_ulps,
        absorber_deviation_ulps=max(item.absorber_deviation_ulps for item in adjustments),
        instrument_absorber_id=(
            cast(str, reported.absorber_key)
            if reported.absorber_dimension == "instrument"
            else None
        ),
        attribution_absorber_key=(
            cast(AttributionKey, reported.absorber_key)
            if reported.absorber_dimension == "attribution"
            else None
        ),
    )


def _resolve_unrepresentable_family(
    exact: _ExactCumulatives,
    keys: tuple[Sequence[str], Sequence[AttributionKey]],
    recovery: _FlowRecovery,
    *,
    public_read: bool,
) -> tuple[_PublishedFlowFamily, AdjustedFlowProjection[str, AttributionKey] | None] | None:
    """Try faithful recovery, then public-read ``(a″)`` only on completed refusal."""
    instrument_keys, attribution_keys = keys
    recovered, completed = _recover_flow_family_outcome(
        exact, instrument_keys, attribution_keys, recovery
    )
    if recovered is not None:
        return recovered, None
    if not public_read or not completed:
        return None
    adjusted = _adjust_flow_family(exact, instrument_keys, attribution_keys, recovery)
    if adjusted is None:
        return None
    return (
        _PublishedFlowFamily(adjusted.total, adjusted.instruments, adjusted.attributions),
        adjusted,
    )


def _published_point_flows_outcome(
    cumulatives: _PointCumulatives,
    instrument_keys: Sequence[str],
    attribution_keys: Sequence[AttributionKey],
    *,
    public_read: bool = False,
) -> (
    tuple[
        _PublishedFlowFamily,
        _PublishedFlowFamily,
        _PublishedFlowFamily,
        _FlowAdjustmentDisclosure,
    ]
    | PnlIncompletenessReason
):
    """Choose representable projections for all three families plus disclosure, or refuse."""
    families: list[_PublishedFlowFamily] = []
    adjustments: list[AdjustedFlowProjection[str, AttributionKey]] = []
    for index, exact in enumerate(
        (cumulatives.realized_exact, cumulatives.fee_exact, cumulatives.accrual_exact)
    ):
        family = _published_flow_family(exact, instrument_keys, attribution_keys)
        if isinstance(family, str):
            if family not in (
                "instrument_sum_unrepresentable",
                "attribution_sum_unrepresentable",
            ):
                return family
            fallback = _resolve_unrepresentable_family(
                exact,
                (instrument_keys, attribution_keys),
                cumulatives.flow_recoveries[index],
                public_read=public_read,
            )
            if fallback is None:
                return family
            family, adjustment = fallback
            if adjustment is not None:
                adjustments.append(adjustment)
        families.append(family)
    return families[0], families[1], families[2], _disclosure_from_adjustments(adjustments)


def _published_point_flows(
    cumulatives: _PointCumulatives,
    instrument_keys: Sequence[str],
    attribution_keys: Sequence[AttributionKey],
) -> tuple[_PublishedFlowFamily, _PublishedFlowFamily, _PublishedFlowFamily] | (
    PnlIncompletenessReason
):
    """Choose representable projections for all three families, or refuse.

    Every historical total/widen pass runs before bounded faithful recovery.
    An interrupted recovery retains the historical reason and cannot publish
    or cache a partially recovered family. Conservation failures never recover.

    Args:
        cumulatives: The point's cumulative snapshot with exact families.
        instrument_keys: Deterministic instrument publication order.
        attribution_keys: Deterministic attribution publication order.

    Returns:
        The three chosen projections in ``(realized, fee, accrual)`` order, or
        the first family's exact withhold reason.
    """
    outcome = _published_point_flows_outcome(cumulatives, instrument_keys, attribution_keys)
    if isinstance(outcome, str):
        return outcome
    return outcome[0], outcome[1], outcome[2]


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
    attribution_keys = _point_attribution_keys(context)
    if instrument_reasons:
        realized, fee, accrual = _raw_point_flows(context.cumulatives)
        return _PointPreparation(
            instrument_untrusted_reasons=instrument_reasons,
            realized=realized,
            fee=fee,
            accrual=accrual,
            attribution_keys=attribution_keys,
        )
    if not _point_attribution_flows_are_finite(context, attribution_keys):
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
    published = _published_point_flows_outcome(
        context.cumulatives,
        context.seen,
        attribution_keys,
        public_read=context.public_read,
    )
    if isinstance(published, str):
        return _context_untrusted_point(
            context,
            attribution_keys,
            {
                _global_incompleteness_reason(
                    published,
                    "untrusted",
                )
            },
        )
    realized, fee, accrual, flow_adjustment = published
    return _PointPreparation(
        instrument_untrusted_reasons=instrument_reasons,
        realized=realized,
        fee=fee,
        accrual=accrual,
        attribution_keys=attribution_keys,
        flow_adjustment=flow_adjustment,
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
            if context.compensated_unrealized_fallback:
                accumulator.unrealized_allocations_by_attribution[key].append(amount)
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
    if not is_activation_point:
        accumulator.carried_inputs.update(
            context.mark_carried_inputs.get((instrument_public_id, context.point_time), ())
        )
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
            realized_pnl=preparation.realized.by_instrument.get(instrument_public_id, 0.0),
            fee_pnl=preparation.fee.by_instrument.get(instrument_public_id, 0.0),
            accrual_pnl=preparation.accrual.by_instrument.get(instrument_public_id, 0.0),
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
        carried_inputs=canonical_carried_inputs(accumulator.carried_inputs),
    )


def _unrealized_by_instrument(
    contributions: Sequence[PnlInstrumentContribution],
) -> dict[str, float]:
    """Collect finite per-instrument unrealized values from point contributions.

    Args:
        contributions: Point-level instrument contribution rows in ``seen`` order.

    Returns:
        Mapping of instrument identity to its finite unrealized P&L.
    """
    return {
        contribution.instrument_public_id: contribution.unrealized_pnl
        for contribution in contributions
        if contribution.unrealized_pnl is not None
    }


def _reconcile_instrument_unrealized(
    context: _PointValuationContext,
    accumulator: _PointAccumulator,
) -> bool:
    """Reconcile finite per-instrument marks and report exact representability."""
    if accumulator.point_reasons or not math.isfinite(accumulator.unrealized_total):
        return True
    unrealized_by_instrument = _unrealized_by_instrument(accumulator.contributions)
    reconciled = _values_with_residue(
        unrealized_by_instrument,
        context.seen,
        accumulator.unrealized_total,
        widen=context.widen_unrealized_absorber,
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


def _compensated_unrealized_candidates(
    ordered_values: Sequence[float],
    naive_total: float,
) -> Iterator[float]:
    """Yield distinct finite compensated total candidates lazily in priority order.

    Evaluates the builtin compensated ``sum`` first and defers ``math.fsum``
    until ``T1`` is unavailable or rejected by reconciliation. Any candidate
    that raises ``OverflowError``, overflows to a non-finite float, or
    duplicates ``naive_total`` or an earlier candidate is skipped.

    Args:
        ordered_values: Per-instrument unrealized values in ``context.seen`` order.
        naive_total: Running ``+=`` total already rejected by reconciliation.

    Yields:
        Distinct finite candidate totals ``T1`` then ``T2``.
    """
    seen_totals: set[float] = {naive_total}
    summations: tuple[Callable[[Sequence[float]], float], ...] = (sum, math.fsum)
    for summarize in summations:
        try:
            candidate = summarize(ordered_values)
        except OverflowError:
            continue
        if not math.isfinite(candidate) or candidate in seen_totals:
            continue
        seen_totals.add(candidate)
        yield candidate


def _compensated_attribution_buckets(
    accumulator: _PointAccumulator,
) -> dict[AttributionKey, float]:
    """Build compensated per-attribution unrealized sums in allocation order.

    Re-sums each attribution bucket's per-point allocation list with builtin
    ``sum``, using compensated (Neumaier) summation in allocation order. Keys
    absent from the allocation lists retain their naive value (``0.0``). The
    per-key allocation lists are per point (bounded by the point's allocation
    count) and discarded each minute.

    Args:
        accumulator: Point-local accumulator holding naive and ordered shares.

    Returns:
        New mapping of compensated per-key unrealized values.
    """
    return {
        **accumulator.unrealized_by_attribution,
        **{
            key: sum(allocations)
            for key, allocations in accumulator.unrealized_allocations_by_attribution.items()
        },
    }


def _attribution_representation(
    context: _PointValuationContext,
    accumulator: _PointAccumulator,
    attribution_keys: Sequence[AttributionKey],
    candidate_total: float,
) -> Mapping[AttributionKey, float] | None:
    """Select the first reconciling attribution representation for a candidate.

    Mirrors :func:`_reconcile_attribution` by requiring no attribution trial
    when ``accumulator.unrealized_incomplete`` is non-empty. Otherwise tests
    the naive ``+=`` buckets first and builds compensated buckets only when
    the naive representation fails.

    Args:
        context: Immutable point valuation inputs and absorber configuration.
        accumulator: Point-local accumulator holding naive and ordered shares.
        attribution_keys: Complete deterministic attribution key order.
        candidate_total: Candidate compensated aggregate unrealized total.

    Returns:
        The accepted naive or compensated bucket mapping, or ``None`` when
        neither representation reconciles to ``candidate_total``.
    """
    if accumulator.unrealized_incomplete:
        return accumulator.unrealized_by_attribution
    naive_reconciled = _values_with_residue(
        accumulator.unrealized_by_attribution,
        attribution_keys,
        candidate_total,
        widen=context.widen_unrealized_absorber,
    )
    if naive_reconciled is not None:
        return accumulator.unrealized_by_attribution
    compensated = _compensated_attribution_buckets(accumulator)
    compensated_reconciled = _values_with_residue(
        compensated,
        attribution_keys,
        candidate_total,
        widen=context.widen_unrealized_absorber,
    )
    if compensated_reconciled is not None:
        return compensated
    return None


def _apply_compensated_total(
    context: _PointValuationContext,
    accumulator: _PointAccumulator,
    candidate_total: float,
    attribution_buckets: Mapping[AttributionKey, float],
) -> bool:
    """Commit an accepted compensated total and rewrite instrument contributions.

    Updates ``accumulator`` only after both trials have succeeded on
    non-mutating copies, preserving the ``defaultdict(float)`` container on
    ``unrealized_by_attribution`` before re-running instrument reconciliation
    against the accepted total.

    Args:
        context: Immutable point valuation inputs and absorber configuration.
        accumulator: Point-local accumulator to update on acceptance.
        candidate_total: Accepted compensated aggregate unrealized total.
        attribution_buckets: Accepted naive or compensated bucket mapping.

    Returns:
        ``True`` once ``_reconcile_instrument_unrealized`` rewrites
        ``accumulator.contributions``.
    """
    accumulator.unrealized_total = candidate_total
    if attribution_buckets is not accumulator.unrealized_by_attribution:
        accumulator.unrealized_by_attribution.clear()
        accumulator.unrealized_by_attribution.update(attribution_buckets)
    return _reconcile_instrument_unrealized(context, accumulator)


def _reconcile_compensated_unrealized(
    context: _PointValuationContext,
    accumulator: _PointAccumulator,
    attribution_keys: Sequence[AttributionKey],
) -> bool:
    """Attempt opt-in compensated unrealized fallback without partial mutation.

    Runs only when naive instrument reconciliation fails and
    ``context.compensated_unrealized_fallback`` is enabled by the service for
    the snapshotter's 5C.6 pointwise samples. Evaluates lazy candidate totals
    ``T1`` (builtin ``sum``) and ``T2`` (``math.fsum``), skipping
    ``OverflowError``, non-finite results, and duplicates of ``T0`` or earlier
    candidates. All trials operate on new dicts so a rejected fallback leaves
    ``accumulator`` untouched and withholds the point identically to the
    default path.

    Args:
        context: Immutable point valuation inputs and fallback configuration.
        accumulator: Point-local valuation accumulator with naive totals.
        attribution_keys: Complete deterministic attribution key order.

    Returns:
        ``True`` if a compensated total and attribution representation both
        reconciled and were committed, otherwise ``False``.
    """
    if not context.compensated_unrealized_fallback:
        return False
    unrealized_by_instrument = _unrealized_by_instrument(accumulator.contributions)
    ordered_values = [unrealized_by_instrument.get(key, 0.0) for key in context.seen]
    for candidate_total in _compensated_unrealized_candidates(
        ordered_values,
        accumulator.unrealized_total,
    ):
        instrument_reconciled = _values_with_residue(
            unrealized_by_instrument,
            context.seen,
            candidate_total,
            widen=context.widen_unrealized_absorber,
        )
        if instrument_reconciled is None:
            continue
        attribution_buckets = _attribution_representation(
            context,
            accumulator,
            attribution_keys,
            candidate_total,
        )
        if attribution_buckets is not None:
            return _apply_compensated_total(
                context,
                accumulator,
                candidate_total,
                attribution_buckets,
            )
    return False


def _reconcile_attribution(
    context: _PointValuationContext,
    preparation: _PointPreparation,
    accumulator: _PointAccumulator,
    attribution_keys: Sequence[AttributionKey],
) -> _ReconciledAttribution | None:
    """Reconcile unrealized attribution or refuse an unprovable residue.

    The three flow families arrive already published: their attribution maps
    were chosen against the same totals the point publishes, and any key this
    complete list adds beyond the preparation list — mark allocations, current
    inventory ownership — has zero cumulative flow, so filling it with ``0.0``
    cannot move the stable-order sums those maps were chosen by. Only the
    per-point unrealized component still reconciles here, float against float,
    because marks are valued fresh each minute rather than accumulated.
    """
    if (
        not accumulator.point_reasons
        and math.isfinite(accumulator.unrealized_total)
        and not accumulator.unrealized_incomplete
    ):
        unrealized = _values_with_residue(
            accumulator.unrealized_by_attribution,
            attribution_keys,
            accumulator.unrealized_total,
            widen=context.widen_unrealized_absorber,
        )
        if unrealized is None:
            return None
    else:
        unrealized = dict(accumulator.unrealized_by_attribution)
    return _ReconciledAttribution(
        realized={
            key: preparation.realized.by_attribution.get(key, 0.0) for key in attribution_keys
        },
        fee={key: preparation.fee.by_attribution.get(key, 0.0) for key in attribution_keys},
        accrual={key: preparation.accrual.by_attribution.get(key, 0.0) for key in attribution_keys},
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
    preparation: _PointPreparation,
    point_time: datetime,
    accumulator: _PointAccumulator,
    attribution: tuple[PnlAttributionContribution, ...],
) -> PnlTimelinePoint:
    """Build a mark-incomplete point while preserving trusted cumulatives."""
    adjustment = preparation.flow_adjustment
    return PnlTimelinePoint(
        point_time=point_time,
        realized_pnl=preparation.realized.total,
        fee_pnl=preparation.fee.total,
        accrual_pnl=preparation.accrual.total,
        unrealized_pnl=None,
        net_pnl=None,
        valuation_status="incomplete",
        incompleteness_reasons=canonical_incompleteness_reasons(accumulator.point_reasons),
        per_instrument=tuple(accumulator.contributions),
        attribution=attribution,
        carried_inputs=canonical_carried_inputs(accumulator.carried_inputs),
        total_rounding_adjusted=adjustment.total_rounding_adjusted,
        total_deviation_ulps=adjustment.total_deviation_ulps,
        absorber_deviation_ulps=adjustment.absorber_deviation_ulps,
        instrument_absorber_id=adjustment.instrument_absorber_id,
        attribution_absorber_key=adjustment.attribution_absorber_key,
    )


def _finalize_valued_point(
    preparation: _PointPreparation,
    point_time: datetime,
    accumulator: _PointAccumulator,
    attribution: tuple[PnlAttributionContribution, ...],
) -> PnlTimelinePoint:
    """Build a complete point or withhold non-finite unrealized and net values.

    The flow totals are the preparation's published choices, not fresh
    projections: when the exact total sits between two doubles the publication
    may have chosen the farther faithful rounding to keep the breakdowns
    summable, and the point must state that same value everywhere.
    """
    if accumulator.point_reasons:
        return _mark_incomplete_point(preparation, point_time, accumulator, attribution)
    net = (
        preparation.realized.total
        + preparation.fee.total
        + preparation.accrual.total
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
        return _mark_incomplete_point(preparation, point_time, accumulator, attribution)
    adjustment = preparation.flow_adjustment
    return PnlTimelinePoint(
        point_time=point_time,
        realized_pnl=preparation.realized.total,
        fee_pnl=preparation.fee.total,
        accrual_pnl=preparation.accrual.total,
        unrealized_pnl=accumulator.unrealized_total,
        net_pnl=net,
        valuation_status="complete",
        incompleteness_reasons=(),
        per_instrument=tuple(accumulator.contributions),
        attribution=attribution,
        carried_inputs=canonical_carried_inputs(accumulator.carried_inputs),
        total_rounding_adjusted=adjustment.total_rounding_adjusted,
        total_deviation_ulps=adjustment.total_deviation_ulps,
        absorber_deviation_ulps=adjustment.absorber_deviation_ulps,
        instrument_absorber_id=adjustment.instrument_absorber_id,
        attribution_absorber_key=adjustment.attribution_absorber_key,
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
        },
        carried_inputs=set(context.carried_event_inputs),
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
    reconciled = _reconcile_instrument_unrealized(context, accumulator)
    if not reconciled:
        reconciled = _reconcile_compensated_unrealized(context, accumulator, attribution_keys)
    if not reconciled:
        return _context_untrusted_point(
            context,
            attribution_keys,
            {
                _global_incompleteness_reason(
                    "instrument_reconciliation_failed",
                    "untrusted",
                )
            },
            accumulator.carried_inputs,
        )
    reconciled_attribution = _reconcile_attribution(
        context,
        prepared,
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
            accumulator.carried_inputs,
        )
    attribution = _attribution_contributions(
        reconciled_attribution,
        accumulator.unrealized_incomplete,
        attribution_keys,
    )
    return _finalize_valued_point(prepared, context.point_time, accumulator, attribution)


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


@dataclass
class _ReplayRecoveryWork:
    """Cap candidate key visits across the points in one physical chunk."""

    remaining_key_visits: int

    def reserve(self, key_visits: int) -> None:
        """Prepay a candidate or signal the writer to retry with fewer points."""
        if key_visits > self.remaining_key_visits:
            raise PnlTimelineWorkBudgetError(
                "Portfolio P&L replay chunk exceeds its faithful recovery work budget"
            )
        self.remaining_key_visits -= key_visits


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
    mark_carried_inputs: MarkCarriedInputMap
    widen_unrealized_absorber: bool
    replay_recovery_work: _ReplayRecoveryWork | None
    compensated_unrealized_fallback: bool = False
    public_read: bool = False


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
    realized_by_instrument: defaultdict[str, ExactSum] = field(
        default_factory=lambda: defaultdict(ExactSum)
    )
    fee_by_instrument: defaultdict[str, ExactSum] = field(
        default_factory=lambda: defaultdict(ExactSum)
    )
    accrual_by_instrument: defaultdict[str, ExactSum] = field(
        default_factory=lambda: defaultdict(ExactSum)
    )
    realized_by_attribution: defaultdict[AttributionKey, ExactSum] = field(
        default_factory=lambda: defaultdict(ExactSum)
    )
    fee_by_attribution: defaultdict[AttributionKey, ExactSum] = field(
        default_factory=lambda: defaultdict(ExactSum)
    )
    accrual_by_attribution: defaultdict[AttributionKey, ExactSum] = field(
        default_factory=lambda: defaultdict(ExactSum)
    )
    realized_total: ExactSum = field(default_factory=ExactSum)
    fee_total: ExactSum = field(default_factory=ExactSum)
    accrual_total: ExactSum = field(default_factory=ExactSum)
    carried_event_inputs: tuple[PnlCarriedInput, ...] = ()
    flow_recoveries: _FlowRecoveries = field(default_factory=_new_flow_recoveries)


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
    mark_evidence: TimelineMarkEvidence | None,
    *,
    window: TimelineWindow,
) -> _TimelineBuildEvidence:
    """Resolve optional caller evidence to immutable empty mappings."""
    resolved_mark_evidence = TimelineMarkEvidence() if mark_evidence is None else mark_evidence
    return _TimelineBuildEvidence(
        marks=marks,
        lineage={} if lineage is None else lineage,
        untrusted_price_reasons_by_instrument=(
            {}
            if untrusted_price_reasons_by_instrument is None
            else untrusted_price_reasons_by_instrument
        ),
        mark_incompleteness_reasons=resolved_mark_evidence.incompleteness_reasons,
        mark_carried_inputs=resolved_mark_evidence.carried_inputs,
        widen_unrealized_absorber=resolved_mark_evidence.widen_unrealized_absorber,
        replay_recovery_work=(
            _ReplayRecoveryWork(MAX_REPLAY_RECOVERY_KEY_VISITS)
            if window.recovery_budget_per_point
            else None
        ),
        compensated_unrealized_fallback=window.compensated_unrealized_fallback,
        public_read=window.public_read,
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


def _copy_exact_sum(accumulator: ExactSum) -> ExactSum:
    """Return an isolated copy of one exact cumulative accumulator."""
    return ExactSum(
        units=accumulator.units,
        infinities=set(accumulator.infinities),
        saw_nan=accumulator.saw_nan,
    )


def _restore_exact_sum_map[KeyT: Hashable](
    entries: Sequence[tuple[KeyT, ExactSum]],
) -> defaultdict[KeyT, ExactSum]:
    """Restore one keyed ExactSum map with fresh accumulator instances."""
    restored: defaultdict[KeyT, ExactSum] = defaultdict(ExactSum)
    for key, value in entries:
        restored[key] = _copy_exact_sum(value)
    return restored


def _restore_timeline_state(
    opening: TimelineCheckpointOpening,
) -> _TimelineBuildState:
    """Rebuild mutable replay state from one captured checkpoint opening."""
    state = _TimelineBuildState(
        pools={
            (pool.instrument_public_id, pool.shard_key): _Pool(
                pool.position_qty,
                pool.entry_price,
            )
            for pool in opening.pools
        },
        weights_by_pool={pool_key: dict(entries) for pool_key, entries in opening.weights_by_pool},
        seen=set(opening.seen),
        attribution_seen=set(opening.attribution_seen),
        basis_reasons_by_pool={
            pool_key: set(reasons) for pool_key, reasons in opening.basis_reasons_by_pool
        },
        untrusted_reasons_by_instrument={
            instrument_public_id: set(entries)
            for instrument_public_id, entries in opening.untrusted_reasons_by_instrument
        },
        activation_time=opening.activation_time,
        scope_by_shard=dict(opening.scope_by_shard),
        exchange_by_instrument=dict(opening.exchange_by_instrument),
        realized_by_instrument=_restore_exact_sum_map(opening.realized_by_instrument),
        fee_by_instrument=_restore_exact_sum_map(opening.fee_by_instrument),
        accrual_by_instrument=_restore_exact_sum_map(opening.accrual_by_instrument),
        realized_by_attribution=_restore_exact_sum_map(opening.realized_by_attribution),
        fee_by_attribution=_restore_exact_sum_map(opening.fee_by_attribution),
        accrual_by_attribution=_restore_exact_sum_map(opening.accrual_by_attribution),
        realized_total=_copy_exact_sum(opening.realized_total),
        fee_total=_copy_exact_sum(opening.fee_total),
        accrual_total=_copy_exact_sum(opening.accrual_total),
    )
    for instrument_public_id, pool_keys in opening.pool_keys_by_instrument:
        state.pool_keys_by_instrument[instrument_public_id] = set(pool_keys)
    return state


def _initialize_timeline_state(
    opening: TimelineOpening | TimelineCheckpointOpening | None,
) -> _TimelineBuildState:
    """Return empty replay state or restore from an anchor or checkpoint opening."""
    if opening is None:
        return _TimelineBuildState()
    if isinstance(opening, TimelineCheckpointOpening):
        return _restore_timeline_state(opening)
    state = _TimelineBuildState()
    for seed in opening.pools:
        _seed_timeline_pool(state, seed)
    state.activation_time = opening.t0
    return state


def _opening_activation_time(
    opening: TimelineOpening | TimelineCheckpointOpening | None,
) -> datetime | None:
    """Return the activation timestamp implied by any supplied opening."""
    if isinstance(opening, TimelineOpening):
        return opening.t0
    if isinstance(opening, TimelineCheckpointOpening):
        return opening.activation_time
    return None


def _validate_checkpoint_resume_window(
    window: TimelineWindow,
    opening: TimelineCheckpointOpening,
) -> None:
    """Require matching currency and UTC window bounds strictly following ``C``."""
    if window.valuation_ccy != opening.valuation_ccy:
        raise ValueError(
            "checkpoint resume window.valuation_ccy must match checkpoint valuation_ccy"
        )
    if window.from_time.utcoffset() != timedelta(0) or window.to_time.utcoffset() != timedelta(0):
        raise ValueError("checkpoint resume window requires UTC bounds")
    if window.from_time.replace(second=0, microsecond=0) <= opening.checkpoint_minute:
        raise ValueError("checkpoint resume requires window.from_time after checkpoint_minute")


def _validate_checkpoint_capture_window(
    window: TimelineWindow,
    opening: TimelineOpening | TimelineCheckpointOpening | None,
) -> None:
    """Require a non-empty, post-activation, minute-aligned capture window."""
    if window.from_time.utcoffset() != timedelta(0):
        raise ValueError("checkpoint capture requires UTC window.from_time")
    _validate_utc_minute(window.to_time, "checkpoint capture window.to_time")
    if window.from_time.replace(second=0, microsecond=0) > window.to_time:
        raise ValueError("checkpoint capture requires a non-empty minute window")
    activation_time = _opening_activation_time(opening)
    if activation_time is not None and window.to_time <= activation_time:
        raise ValueError("checkpoint capture requires window.to_time after activation_time")


def _validate_checkpoint_request(
    window: TimelineWindow,
    opening: TimelineOpening | TimelineCheckpointOpening | None,
) -> None:
    """Fail closed when checkpoint capture or resume violates mode or time invariants."""
    is_checkpoint_opening = isinstance(opening, TimelineCheckpointOpening)
    if (
        window.capture_checkpoint or is_checkpoint_opening
    ) and not window.recovery_budget_per_point:
        raise ValueError("checkpoint capture and resume require recovery_budget_per_point")
    if isinstance(opening, TimelineCheckpointOpening):
        _validate_checkpoint_resume_window(window, opening)
    if window.capture_checkpoint:
        _validate_checkpoint_capture_window(window, opening)


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


def _validate_resume_executions(
    opening: TimelineCheckpointOpening,
    executions: Sequence[TimelineExecution],
    prepared: Sequence[_PreparedExecution],
) -> None:
    """Refuse a resume input that re-passes applied fills or omits deferred ones.

    A fill applied by the checkpoint re-prepares at or before
    ``checkpoint_minute`` and would be counted twice. A fill exported in
    ``deferred_execution_keys`` was not yet applied, so leaving it out would
    silently drop it together with the clamp and shadow it carries.

    Args:
        opening: The checkpoint the window resumes from.
        executions: The executions supplied for the resumed window.
        prepared: Those executions after clamping from the carried maxima.

    Raises:
        ValueError: When a supplied fill applies at or before the checkpoint,
            or when a deferred fill is missing.
    """
    supplied: set[tuple[str, int]] = set()
    for execution in executions:
        key = (execution.exchange, execution.scope_sequence)
        if key in supplied:
            raise ValueError("checkpoint resume received duplicate execution key")
        supplied.add(key)
    if any(key not in supplied for key in opening.deferred_execution_keys):
        raise ValueError("checkpoint resume is missing deferred executions")
    if any(item.effective_time <= opening.checkpoint_minute for item in prepared):
        raise ValueError("checkpoint resume received execution at or before checkpoint_minute")


def _timeline_schedule(
    state: _TimelineBuildState,
    executions: Sequence[TimelineExecution],
    accruals: Sequence[TimelineAccrual],
    window: TimelineWindow,
    opening: TimelineOpening | TimelineCheckpointOpening | None = None,
) -> _TimelineSchedule:
    """Build deterministic event, valuation-pool, and regression indexes."""
    checkpoint_opening = opening if isinstance(opening, TimelineCheckpointOpening) else None
    prepared, shadows = _prepare_executions(
        executions,
        initial_last_effective=(
            None if checkpoint_opening is None else dict(checkpoint_opening.last_effective_by_pool)
        ),
    )
    if checkpoint_opening is not None:
        _validate_resume_executions(checkpoint_opening, executions, prepared)
    pool_index = _valuation_pool_index(
        state.pool_keys_by_instrument,
        prepared,
        window.to_time,
    )
    position_changes = _position_changes_by_effective_time(
        prepared,
        window.to_time,
    )
    if checkpoint_opening is None:
        filtered_accruals: Iterable[TimelineAccrual] = (
            accrual
            for accrual in accruals
            if state.activation_time is None or accrual.accrued_at > state.activation_time
        )
    else:
        if any(accrual.accrued_at <= checkpoint_opening.checkpoint_minute for accrual in accruals):
            raise ValueError("checkpoint resume received accrual at or before checkpoint_minute")
        filtered_accruals = accruals
    sorted_accruals = sorted(
        filtered_accruals,
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
    state.realized_by_instrument[application.instrument_public_id].add(realized_delta)
    state.realized_total.add(realized_delta)
    if outcome.closed_qty > 0.0:
        allocation = _allocate_flow(
            state.realized_by_attribution,
            realized_delta,
            application.pre_fill_weights,
        )
        state.attribution_seen.update(allocation)


def _record_execution_fee(
    state: _TimelineBuildState,
    application: _ExecutionApplication,
) -> None:
    """Accumulate and allocate one execution fee with flip-aware ownership."""
    execution = application.execution
    outcome = application.outcome
    fee_pnl = 0.0 if execution.fee_incompleteness_reason is not None else -execution.fee
    state.fee_by_instrument[application.instrument_public_id].add(fee_pnl)
    state.fee_total.add(fee_pnl)
    opened_opposite_side = outcome.closed_qty > 0.0 and outcome.opened_new_side
    if opened_opposite_side:
        closing_fee_pnl = fee_pnl * (outcome.closed_qty / application.position_size)
        allocation = _allocate_flow(
            state.fee_by_attribution,
            closing_fee_pnl,
            application.pre_fill_weights,
        )
        state.attribution_seen.update(allocation)
        opening_bucket = state.fee_by_attribution[application.attribution_key]
        if math.isfinite(fee_pnl):
            opening_bucket.add_units(exact_units_of(fee_pnl) - exact_units_of(closing_fee_pnl))
        else:
            opening_bucket.add(fee_pnl - closing_fee_pnl)
    elif outcome.closed_qty > 0.0:
        allocation = _allocate_flow(
            state.fee_by_attribution,
            fee_pnl,
            application.pre_fill_weights,
        )
        state.attribution_seen.update(allocation)
    else:
        state.fee_by_attribution[application.attribution_key].add(fee_pnl)


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


def _latch_carried_event_inputs(
    state: _TimelineBuildState,
    inputs: tuple[PnlCarriedInput, ...],
) -> None:
    """Remember carried event rates, because every later cumulative depends on them."""
    if inputs:
        state.carried_event_inputs = canonical_carried_inputs(
            [*state.carried_event_inputs, *inputs]
        )


def _apply_execution_event(
    state: _TimelineBuildState,
    evidence: _TimelineBuildEvidence,
    execution: TimelineExecution,
) -> None:
    """Apply one execution after all same-time accruals."""
    application = _begin_execution_application(state, evidence, execution)
    if application is None:
        return
    _latch_carried_event_inputs(state, execution.carried_inputs)
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
    _latch_carried_event_inputs(state, accrual.carried_inputs)
    if accrual.incompleteness_reason is not None:
        _add_instrument_untrusted_reason(
            state.untrusted_reasons_by_instrument,
            accrual.instrument_public_id,
            accrual.incompleteness_reason,
        )
    accrual_pnl = 0.0 if accrual.incompleteness_reason is not None else -accrual.amount_usd
    state.accrual_by_instrument[accrual.instrument_public_id].add(accrual_pnl)
    state.accrual_total.add(accrual_pnl)
    state.seen.add(accrual.instrument_public_id)
    allocation = _allocate_flow(
        state.accrual_by_attribution,
        accrual_pnl,
        (
            {}
            if attribution_is_ambiguous
            else _combined_instrument_weights(
                pool_index.get(accrual.instrument_public_id, ()),
                state.weights_by_pool,
            )
        ),
    )
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


def _projected[KeyT: Hashable](accumulators: Mapping[KeyT, ExactSum]) -> dict[KeyT, float]:
    """Project one exact accumulator map onto correctly rounded doubles.

    Args:
        accumulators: The live exact per-key accumulators.

    Returns:
        Each key's running total rounded exactly once, non-finite latches
        included, so every existing float screen keeps its meaning.
    """
    return {key: accumulator.to_float() for key, accumulator in accumulators.items()}


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
            realized_by_instrument=_projected(state.realized_by_instrument),
            fee_by_instrument=_projected(state.fee_by_instrument),
            accrual_by_instrument=_projected(state.accrual_by_instrument),
            realized_by_attribution=_projected(state.realized_by_attribution),
            fee_by_attribution=_projected(state.fee_by_attribution),
            accrual_by_attribution=_projected(state.accrual_by_attribution),
            realized_total=state.realized_total.to_float(),
            fee_total=state.fee_total.to_float(),
            accrual_total=state.accrual_total.to_float(),
            realized_exact=_ExactCumulatives(
                by_instrument=state.realized_by_instrument,
                by_attribution=state.realized_by_attribution,
                total=state.realized_total,
            ),
            fee_exact=_ExactCumulatives(
                by_instrument=state.fee_by_instrument,
                by_attribution=state.fee_by_attribution,
                total=state.fee_total,
            ),
            accrual_exact=_ExactCumulatives(
                by_instrument=state.accrual_by_instrument,
                by_attribution=state.accrual_by_attribution,
                total=state.accrual_total,
            ),
            flow_recoveries=(
                _new_flow_recoveries(evidence.replay_recovery_work.reserve)
                if evidence.replay_recovery_work is not None
                else state.flow_recoveries
            ),
        ),
        activation_time=state.activation_time,
        global_reasons=global_reasons,
        untrusted_reasons_by_instrument=state.untrusted_reasons_by_instrument,
        basis_reasons_by_pool=state.basis_reasons_by_pool,
        mark_incompleteness_reasons=evidence.mark_incompleteness_reasons,
        mark_carried_inputs=evidence.mark_carried_inputs,
        carried_event_inputs=state.carried_event_inputs,
        widen_unrealized_absorber=evidence.widen_unrealized_absorber,
        compensated_unrealized_fallback=evidence.compensated_unrealized_fallback,
        public_read=evidence.public_read,
    )


def _build_minute_points(
    state: _TimelineBuildState,
    evidence: _TimelineBuildEvidence,
    schedule: _TimelineSchedule,
) -> tuple[list[PnlTimelinePoint], _EventCursor]:
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
    return minute_points, cursor


def _captured_scope_maps(
    opening: TimelineOpening | TimelineCheckpointOpening | None,
    applied_executions: Sequence[_PreparedExecution],
) -> tuple[dict[str, tuple[str, str]], dict[str, str]]:
    """Collect shard and instrument exchange scopes through the checkpoint minute."""
    scope_by_shard: dict[str, tuple[str, str]] = {}
    exchange_by_instrument: dict[str, str] = {}
    if isinstance(opening, TimelineOpening):
        for seed in opening.pools:
            scope_by_shard[seed.shard_key] = (seed.instrument_public_id, seed.exchange)
            exchange_by_instrument[seed.instrument_public_id] = seed.exchange
    elif isinstance(opening, TimelineCheckpointOpening):
        scope_by_shard.update(opening.scope_by_shard)
        exchange_by_instrument.update(opening.exchange_by_instrument)
    for item in applied_executions:
        execution = item.execution
        scope_by_shard[execution.shard_key] = (
            execution.instrument_public_id,
            execution.exchange,
        )
        exchange_by_instrument[execution.instrument_public_id] = execution.exchange
    return scope_by_shard, exchange_by_instrument


def _captured_last_effective(
    opening: TimelineOpening | TimelineCheckpointOpening | None,
    applied_executions: Sequence[_PreparedExecution],
) -> tuple[tuple[PoolKey, datetime], ...]:
    """Collect the monotone per-pool last effective time through the checkpoint."""
    last_effective: dict[PoolKey, datetime] = (
        dict(opening.last_effective_by_pool)
        if isinstance(opening, TimelineCheckpointOpening)
        else {}
    )
    for item in applied_executions:
        pool_key = (item.execution.instrument_public_id, item.execution.shard_key)
        previous = last_effective.get(pool_key)
        last_effective[pool_key] = (
            item.effective_time if previous is None else max(previous, item.effective_time)
        )
    return tuple((pool_key, last_effective[pool_key]) for pool_key in sorted(last_effective))


def _sorted_instrument_exact_sums(
    accumulators: Mapping[str, ExactSum],
) -> tuple[tuple[str, ExactSum], ...]:
    """Snapshot per-instrument exact sums in canonical key order."""
    return tuple((key, _copy_exact_sum(accumulators[key])) for key in sorted(accumulators))


def _sorted_attribution_exact_sums(
    accumulators: Mapping[AttributionKey, ExactSum],
) -> tuple[tuple[AttributionKey, ExactSum], ...]:
    """Snapshot per-attribution exact sums in canonical key order."""
    return tuple(
        (key, _copy_exact_sum(accumulators[key]))
        for key in _sorted_attribution_keys(set(accumulators))
    )


def _capture_refusal_reason(
    state: _TimelineBuildState,
    applied_executions: Sequence[_PreparedExecution],
    applied_accruals: Sequence[TimelineAccrual],
) -> CheckpointRefusalReason | None:
    """Return a non-fatal refusal reason when state or processed events prevent capture."""
    if state.carried_event_inputs:
        return "carried_event_inputs"
    if any(item.execution.carried_inputs for item in applied_executions):
        return "carried_inputs_on_processed_event"
    if any(item.execution.conversion_uses_mark_evidence for item in applied_executions) or any(
        accrual.conversion_uses_mark_evidence for accrual in applied_accruals
    ):
        return "conversion_uses_mark_evidence"
    return None


def _capture_closing_checkpoint(
    state: _TimelineBuildState,
    schedule: _TimelineSchedule,
    cursor: _EventCursor,
    window: TimelineWindow,
    opening: TimelineOpening | TimelineCheckpointOpening | None,
) -> tuple[TimelineCheckpointOpening | None, CheckpointRefusalReason | None]:
    """Snapshot carried state at ``window.to_time`` or return a non-fatal refusal."""
    applied_executions = schedule.prepared[: cursor.execution_index]
    applied_accruals = schedule.accruals[: cursor.accrual_index]
    refusal_reason = _capture_refusal_reason(state, applied_executions, applied_accruals)
    if refusal_reason is not None:
        return None, refusal_reason
    scope_by_shard, exchange_by_instrument = _captured_scope_maps(opening, applied_executions)
    deferred_execution_keys = tuple(
        sorted(
            (item.exchange, item.scope_sequence)
            for item in schedule.prepared[cursor.execution_index :]
        )
    )
    return (
        TimelineCheckpointOpening(
            checkpoint_minute=window.to_time,
            activation_time=state.activation_time,
            valuation_ccy=window.valuation_ccy,
            pools=tuple(
                TimelineCheckpointPool(
                    instrument_public_id=pool_key[0],
                    shard_key=pool_key[1],
                    position_qty=pool.position_qty,
                    entry_price=pool.entry_price,
                )
                for pool_key, pool in state.pools.items()
            ),
            weights_by_pool=tuple(
                (pool_key, tuple(weights.items()))
                for pool_key, weights in state.weights_by_pool.items()
            ),
            seen=tuple(sorted(state.seen)),
            attribution_seen=tuple(_sorted_attribution_keys(state.attribution_seen)),
            basis_reasons_by_pool=tuple(
                (pool_key, tuple(sorted(state.basis_reasons_by_pool[pool_key])))
                for pool_key in sorted(state.basis_reasons_by_pool)
            ),
            untrusted_reasons_by_instrument=tuple(
                (
                    instrument_public_id,
                    canonical_incompleteness_reasons(
                        state.untrusted_reasons_by_instrument[instrument_public_id]
                    ),
                )
                for instrument_public_id in sorted(state.untrusted_reasons_by_instrument)
            ),
            scope_by_shard=tuple((key, scope_by_shard[key]) for key in sorted(scope_by_shard)),
            exchange_by_instrument=tuple(
                (key, exchange_by_instrument[key]) for key in sorted(exchange_by_instrument)
            ),
            pool_keys_by_instrument=tuple(
                (key, tuple(schedule.pool_index[key])) for key in sorted(schedule.pool_index)
            ),
            realized_by_instrument=_sorted_instrument_exact_sums(state.realized_by_instrument),
            fee_by_instrument=_sorted_instrument_exact_sums(state.fee_by_instrument),
            accrual_by_instrument=_sorted_instrument_exact_sums(state.accrual_by_instrument),
            realized_by_attribution=_sorted_attribution_exact_sums(state.realized_by_attribution),
            fee_by_attribution=_sorted_attribution_exact_sums(state.fee_by_attribution),
            accrual_by_attribution=_sorted_attribution_exact_sums(state.accrual_by_attribution),
            realized_total=_copy_exact_sum(state.realized_total),
            fee_total=_copy_exact_sum(state.fee_total),
            accrual_total=_copy_exact_sum(state.accrual_total),
            last_effective_by_pool=_captured_last_effective(opening, applied_executions),
            deferred_execution_keys=deferred_execution_keys,
        ),
        None,
    )


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
    opening: TimelineOpening | TimelineCheckpointOpening | None = None,
    lineage: Mapping[str, TimelineExecutionLineage] | None = None,
    untrusted_price_reasons_by_instrument: (
        Mapping[str, Collection[PnlIncompletenessReason]] | None
    ) = None,
    mark_evidence: TimelineMarkEvidence | None = None,
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
        opening: The activation anchor or carried checkpoint opening seeding the
            replay, or ``None`` to replay from empty pools. Derived opening
            pools are already rebased to t0.
        lineage: Order-keyed initiating command and signal lineage. Missing or
            ambiguous orders are intentionally absent and become unattributed.
        untrusted_price_reasons_by_instrument: Caller-stamped execution-price
            proof failures keyed by instrument. Their fills are never passed to
            the accounting kernel and their exact reasons latch when replay
            reaches them.
        mark_evidence: Caller-stamped causes for marks omitted during conversion
            and the carried inputs behind marks that reached back. Other absent
            or non-finite marks are classified only as unavailable when the
            builder needs them.

    Returns:
        The built :class:`PnlTimelineResult` at the requested granularity.

    Raises:
        ValueError: When ``window.granularity`` is not a supported value, or
            when checkpoint capture/resume invariants fail.
        PnlTimelineWorkBudgetError: When the replay chunk would exceed its shared
            faithful recovery work budget.
    """
    step = _granularity_step(window.granularity)
    _validate_checkpoint_request(window, opening)
    evidence = _resolved_build_evidence(
        marks,
        lineage,
        untrusted_price_reasons_by_instrument,
        mark_evidence,
        window=window,
    )
    state = _initialize_timeline_state(opening)
    _validate_timeline_execution_scopes(state, executions)
    schedule = _timeline_schedule(state, executions, accruals, window, opening)
    minute_points, cursor = _build_minute_points(state, evidence, schedule)
    if window.capture_checkpoint:
        closing_checkpoint, closing_checkpoint_refusal = _capture_closing_checkpoint(
            state,
            schedule,
            cursor,
            window,
            opening,
        )
    else:
        closing_checkpoint = None
        closing_checkpoint_refusal = None
    return PnlTimelineResult(
        points=tuple(_downsample(minute_points, step)),
        granularity=window.granularity,
        valuation_ccy=window.valuation_ccy,
        closing_checkpoint=closing_checkpoint,
        closing_checkpoint_refusal=closing_checkpoint_refusal,
    )
