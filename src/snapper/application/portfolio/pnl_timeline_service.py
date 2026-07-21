"""Async orchestrator + mark builder for the P&L timeline (Phase 5A API layer).

The pure :mod:`snapper.application.portfolio.pnl_timeline` builder performs no
I/O. It consumes already-fetched executions, accruals, and a mark lookup whose
money values are all expressed in the requested valuation currency. This module
is the thin async seam that proves the native denomination, loads finalized 1m
candles, converts those inputs, and calls the pure builder.

For each instrument the orchestrator resolves its ``(native_symbol,
source_exchange, instrument_exchange, base_currency, quote_currency)`` history
via :meth:`Repository.get_instrument_symbol_refs`. Every version known at the
response horizon must unanimously carry one base and one non-null quote currency.
Adjacent or overlapping knowledge intervals with the same full price projection
are merged before one projection is required to cover every fill. Missing,
gapped, conflicting, or venue-mismatched evidence remains untrusted and never
reaches the average-cost kernel as a certified price.

A proven foreign quote is converted rather than rejected. Each positive
execution price is converted at the fill's own minute before pool replay, and
each positive mark close is converted at the grid minute it values. General
consumers resolve one exact ``(base, quote, exchange)`` plane per unordered
currency pair by their own exact-minute coverage. A held instrument whose own
currency legs match a needed pair instead uses its canonical oriented source
plane for all of that instrument's conversions on the pair. Neither a rival
venue nor the opposite orientation can fill that instrument plane's gap, while
the gap cannot suppress independently covered peers. No triangulation,
nearest-minute match, or stale carry-forward is permitted. A missing execution
rate becomes ``NaN`` so the pure builder applies its existing
opening-versus-closing trust tiers. A missing mark rate emits no mark, producing
mark-incomplete valuation without tainting mark-independent cumulatives.

The mark for grid minute ``M`` is the close of the finalized candle covering
``[M-1m, M)`` (``open_at == M - 1min``), and its FX rate follows the same
convention. Execution, fee, and accrual conversions use the last rate bar that
had closed at the event's floored minute. Exact zero remains currency-invariant;
an unconvertible nonzero flow is passed as ``NaN`` rather than silently zeroed or
dropped.
"""

import math
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timedelta
from typing import Final
from typing import Literal
from typing import cast

from snapper.application.portfolio.average_cost import FLAT_EPSILON
from snapper.application.portfolio.fx_rates import FxPairKey
from snapper.application.portfolio.fx_rates import FxRateMap
from snapper.application.portfolio.fx_rates import FxVenueMap
from snapper.application.portfolio.fx_rates import convert_amount
from snapper.application.portfolio.fx_rates import currency_pair_key
from snapper.application.portfolio.pnl_timeline import MarkMap
from snapper.application.portfolio.pnl_timeline import PnlAttributionContribution
from snapper.application.portfolio.pnl_timeline import PnlInstrumentContribution
from snapper.application.portfolio.pnl_timeline import PnlTimelinePoint
from snapper.application.portfolio.pnl_timeline import PnlTimelineResult
from snapper.application.portfolio.pnl_timeline import TimelineAccrual
from snapper.application.portfolio.pnl_timeline import TimelineExecution
from snapper.application.portfolio.pnl_timeline import TimelineExecutionLineage
from snapper.application.portfolio.pnl_timeline import TimelineWindow
from snapper.application.portfolio.pnl_timeline import build_pnl_timeline
from snapper.core.numeric import is_positive_finite
from snapper.data.repository import Repository
from snapper.data.repository_types import InstrumentSymbolRefRow
from snapper.data.repository_types import PnlFxRatePlane
from snapper.data.repository_types import PnlFxRateRow
from snapper.data.repository_types import PnlTimelineAccrualRow
from snapper.data.repository_types import PnlTimelineAiDecisionMarkerRow
from snapper.data.repository_types import PnlTimelineCandleRow
from snapper.data.repository_types import PnlTimelineExecutionLineageRow
from snapper.data.repository_types import PnlTimelineExecutionRow
from snapper.data.repository_types import PnlTimelineSignalMarkerRow

PNL_TIMELINE_MARK_SOURCE = "finalized_1m_candle_close"
"""Provenance label for the mark plane the timeline values against."""

PNL_TIMELINE_CALC_VERSION = "5A.8"
"""Reconstruction algorithm version stamped on every series response.

Bumped whenever the pool replay, decomposition, or mark-resolution semantics
change so a cached or persisted point can be told apart from a re-derivation
under a newer contract.
"""

PNL_TIMELINE_MAX_WORK_UNITS: Final[int] = 131_040
"""Maximum minute-instrument work for one reconstruction.

The budget preserves the former 91-day allowance for one instrument while
making multi-instrument requests pay for their actual grid fan-out. Empty scopes
use a factor of one because the pure builder still materialises the raw grid.
"""

PNL_TIMELINE_MARKER_LIMIT: Final[int] = 2_000
"""Maximum markers returned, retaining the latest markers deterministically.

Marker reads request one extra row from each independently bounded decision
source. The response exposes both this limit and whether older markers were
omitted, so a busy window never looks indistinguishable from a complete one.
"""


@dataclass(frozen=True, slots=True)
class PnlFillMarker:
    """One immutable execution projected as an executed timeline marker."""

    marker_time: datetime
    instrument_public_id: str
    side: str
    size: float
    price: float | None
    execution_public_id: str
    order_public_id: str
    status: str
    kind: Literal["fill"] = field(default="fill", init=False)
    outcome: Literal["executed"] = field(default="executed", init=False)


@dataclass(frozen=True, slots=True)
class PnlSignalMarker:
    """One source signal with its independently established fill outcome."""

    marker_time: datetime
    instrument_public_id: str
    side: str
    strategy_name: str | None
    strength: float
    reason: str
    price: float | None
    signal_public_id: str
    outcome: Literal["executed", "no_fill"]
    status: Literal["executed", "no_fill"]
    kind: Literal["signal"] = field(default="signal", init=False)


@dataclass(frozen=True, slots=True)
class PnlAiDecisionMarker:
    """One append-only AI decision event and its observable outcome."""

    marker_time: datetime
    instrument_public_id: str
    strategy_public_id: str
    review_public_id: str
    event_public_id: str
    decision: str | None
    rationale: str | None
    outcome: Literal["executed", "rejected", "no_fill"]
    status: str
    kind: Literal["ai_decision"] = field(default="ai_decision", init=False)


type PnlTimelineMarker = PnlFillMarker | PnlSignalMarker | PnlAiDecisionMarker
"""Service-layer marker union emitted in chronological chart order."""


@dataclass(frozen=True, slots=True)
class PnlFxRateSource:
    """One used FX plane expressed with conversion provenance."""

    source_currency: str
    valuation_currency: str
    base_currency: str
    quote_currency: str
    exchange: str


@dataclass(frozen=True)
class PnlWalletSeriesResult(PnlTimelineResult):
    """A pure P&L series augmented with attributable FX rate venues."""

    rate_sources: tuple[PnlFxRateSource, ...]


@dataclass(frozen=True, slots=True)
class PnlWalletTimelineResult:
    """One reconstructed series with a bounded marker overlay."""

    series: PnlWalletSeriesResult
    markers: tuple[PnlTimelineMarker, ...]
    marker_limit: int
    markers_truncated: bool


class PnlTimelineWorkBudgetError(ValueError):
    """The requested raw grid and instrument fan-out exceed the work budget."""


def _rate_minute(moment: datetime) -> datetime:
    """Return the grid minute whose closing bar prices a flow at ``moment``.

    Flooring to the minute selects the bar that CLOSED at that instant — the last
    finalized evidence available when the flow happened — matching the mark
    convention exactly, so a fee and the position it belongs to are valued off the
    same bar and neither can look ahead.

    Args:
        moment: Event time of the flow being converted.

    Returns:
        The minute key to look the rate up under.
    """
    return moment.replace(second=0, microsecond=0)


def build_fx_rates(rows: Sequence[PnlFxRateRow]) -> FxRateMap:
    """Fold FX candle rows into the plane-qualified minute rate map.

    Venue is part of the key, so cross-venue quotes remain distinct evidence. If
    two rows still conflict on the full identity, that key becomes ``NaN`` and
    conversion refuses it rather than allowing row order to elect a price. The
    caller filters the book through one consumer-pinned oriented plane per pair.

    Args:
        rows: Finalized 1m closes from ``get_pnl_fx_rate_candles``.

    Returns:
        Rate map keyed by ``(base, quote, exchange, minute)`` where the minute
        is the instant the bar closed.
    """
    rates: dict[tuple[str, str, str, datetime], float] = {}
    for row in rows:
        key = (
            row["base"],
            row["quote"],
            row["exchange"],
            row["open_at"] + timedelta(minutes=1),
        )
        existing = rates.get(key)
        if existing is not None and existing != row["close"]:
            rates[key] = math.nan
        else:
            rates[key] = row["close"]
    return rates


def _to_timeline_execution(
    row: PnlTimelineExecutionRow,
    valuation_ccy: str,
    rates: FxRateMap,
    price_currency: str | None = None,
    venues: FxVenueMap | None = None,
) -> TimelineExecution:
    """Map a repository execution row onto the pure builder's input.

    A positive execution price with a proven ``price_currency`` is converted at
    the fill's exact rate minute before entering the average-cost pool. A missing
    rate becomes ``NaN`` so the builder chooses its existing weakest honest tier.
    Raw non-positive or non-finite prices pass through unchanged so the D1 guard
    remains authoritative. Exact-zero fees pass through regardless of asset; a
    nonzero foreign fee is converted at the same event minute or becomes
    ``NaN``. ``event_time`` is the row's ``timestamp`` axis, never nullable
    ``executed_at``.

    Args:
        row: One ``get_pnl_timeline_executions`` row.
        valuation_ccy: Currency the series is valued in.
        rates: Minute-keyed FX rates used for price and fee conversion.
        price_currency: Proven quote currency of the execution price. ``None``
            leaves the raw price unchanged for callers that enforce trust
            separately.
        venues: Instrument-pinned oriented plane for each converted currency pair.

    Returns:
        The equivalent :class:`TimelineExecution`.
    """
    rate_minute = _rate_minute(row["timestamp"])
    converted_fee = convert_amount(
        row["fee"],
        row["fee_asset"],
        valuation_ccy,
        rate_minute,
        rates,
        venues,
    )
    fee = math.nan if converted_fee is None else converted_fee
    raw_price = row["price"]
    price = raw_price
    if price_currency is not None and is_positive_finite(raw_price):
        converted_price = convert_amount(
            raw_price,
            price_currency,
            valuation_ccy,
            rate_minute,
            rates,
            venues,
        )
        price = converted_price if is_positive_finite(converted_price) else math.nan
    return TimelineExecution(
        order_public_id=row["order_public_id"],
        instrument_public_id=row["instrument_public_id"],
        exchange=row["exchange"],
        scope_sequence=row["scope_sequence"],
        event_time=row["timestamp"],
        side=row["side"],
        size=row["size"],
        price=price,
        fee=fee,
        fee_asset=row["fee_asset"],
    )


def _build_execution_lineage(
    rows: Sequence[PnlTimelineExecutionLineageRow],
) -> dict[str, TimelineExecutionLineage]:
    """Build a fail-closed order-lineage lookup from repository candidates.

    Exactly one row is required for an order to carry resolved lineage into the
    pure engine. Repeated rows are ambiguous even when their projected values
    happen to match, so the order is removed from the lookup instead of choosing
    a winner. A missing map entry is intentionally classified as unattributed by
    the builder.

    Args:
        rows: Candidate initiating-command lineage rows keyed by order public id.

    Returns:
        Unique lineage keyed by order public id, excluding every duplicate key.
    """
    lineage: dict[str, TimelineExecutionLineage] = {}
    seen: set[str] = set()
    ambiguous: set[str] = set()
    for row in rows:
        order_public_id = row["order_public_id"]
        if order_public_id in seen:
            ambiguous.add(order_public_id)
            lineage.pop(order_public_id, None)
            continue
        seen.add(order_public_id)
        lineage[order_public_id] = TimelineExecutionLineage(
            source_surface=row["source_surface"],
            plan_public_id=row["plan_public_id"],
            signal_public_id=row["signal_public_id"],
            origin=row["origin"],
            strategy_name=row["strategy_name"],
        )
    for order_public_id in ambiguous:
        lineage.pop(order_public_id, None)
    return lineage


def _to_timeline_accrual(
    row: PnlTimelineAccrualRow,
    valuation_ccy: str,
    rates: FxRateMap,
    venues: FxVenueMap | None = None,
) -> TimelineAccrual:
    """Map a repository accrual row onto the pure builder's input.

    Exact zero passes through regardless of asset because it needs no currency
    conversion. A nonzero foreign amount is CONVERTED at the accrual's own minute
    from our finalized 1m candles, and only an unresolvable pair leaves it as
    ``NaN`` so the builder withholds the affected cumulatives.

    Args:
        row: One ``get_accruals_for_pnl`` row.
        valuation_ccy: Currency the series is valued in.
        rates: Minute-keyed FX rates used to convert a foreign-denominated amount.
        venues: Instrument-pinned oriented plane for each converted currency pair.

    Returns:
        The equivalent :class:`TimelineAccrual`.
    """
    converted = convert_amount(
        row["amount"],
        row["amount_asset"],
        valuation_ccy,
        _rate_minute(row["accrued_at"]),
        rates,
        venues,
    )
    amount = math.nan if converted is None else converted
    return TimelineAccrual(
        instrument_public_id=row["instrument_public_id"],
        accrued_at=row["accrued_at"],
        amount_usd=amount,
    )


def _withhold_series_for_fill_gap(result: PnlTimelineResult) -> PnlTimelineResult:
    """Post-transform a built series when durable fill evidence proves a gap.

    A recorded-versus-consumed fill mismatch means no cumulative monetary value
    is defensible, including values from minutes before the visible execution
    prefix. Every aggregate, per-instrument, and attribution monetary field is
    therefore withheld while timestamps and contributing identities remain
    available. A machine-readable incompleteness reason is a valuable follow-up,
    but it requires an approved pure-engine contract change and is outside v1.

    Args:
        result: Series built from the currently visible execution prefix.

    Returns:
        The same grid and metadata with every point fully untrusted.
    """
    points = tuple(
        PnlTimelinePoint(
            point_time=point.point_time,
            realized_pnl=None,
            fee_pnl=None,
            accrual_pnl=None,
            unrealized_pnl=None,
            net_pnl=None,
            valuation_status="incomplete",
            per_instrument=tuple(
                PnlInstrumentContribution(
                    instrument_public_id=contribution.instrument_public_id,
                    realized_pnl=None,
                    fee_pnl=None,
                    accrual_pnl=None,
                    unrealized_pnl=None,
                )
                for contribution in point.per_instrument
            ),
            attribution=tuple(
                PnlAttributionContribution(
                    origin=contribution.origin,
                    strategy_name=contribution.strategy_name,
                    realized_pnl=None,
                    fee_pnl=None,
                    accrual_pnl=None,
                    unrealized_pnl=None,
                )
                for contribution in point.attribution
            ),
        )
        for point in result.points
    )
    return PnlTimelineResult(
        points=points,
        granularity=result.granularity,
        valuation_ccy=result.valuation_ccy,
    )


async def _scope_has_fill_gap(
    repo: Repository,
    wallet_public_id: str,
    mode: str,
    as_of: datetime,
) -> bool:
    """Consult durable gap evidence for every fill-bearing shard in the scope.

    Each shared shard cursor derives its venue prefix at ``as_of``; exact wallet
    and mode filters then expose venue-only shards with zero consumed
    executions. Each returned key is evaluated against the matching exact-scope
    execution prefix. The recovery gap read is intentionally not reused because
    it describes current state rather than historical P&L completeness.

    Args:
        repo: Repository providing scoped shard keys and gap evidence.
        wallet_public_id: Full wallet scope.
        mode: Trading mode scope.
        as_of: Temporal anchor for consumed execution evidence.

    Returns:
        ``True`` as soon as any scoped shard has a proven fill gap.
    """
    shard_keys = await repo.get_fill_shard_keys_for_scope(wallet_public_id, mode, as_of)
    for shard_key in shard_keys:
        if await repo.pnl_timeline_shard_has_fill_gap(
            shard_key,
            wallet_public_id,
            mode,
            as_of,
        ):
            return True
    return False


def _enforce_total_work_budget(
    from_time: datetime,
    to_time: datetime,
    distinct_instrument_count: int,
) -> None:
    """Reject a reconstruction whose raw grid fan-out exceeds the budget.

    Work is measured as inclusive raw grid minutes multiplied by the number of
    distinct instruments participating through executions or accruals. A factor
    of one applies to an empty scope because grid construction itself is still
    linear in the requested minute span.

    Args:
        from_time: Requested series start; its minute floor anchors the grid.
        to_time: Inclusive series end.
        distinct_instrument_count: Unique instruments in all input flows.

    Raises:
        PnlTimelineWorkBudgetError: When minute-instrument work exceeds the
            configured maximum.
    """
    grid_start = from_time.replace(second=0, microsecond=0)
    raw_grid_minutes = int((to_time - grid_start).total_seconds() // 60) + 1
    work_units = raw_grid_minutes * max(1, distinct_instrument_count)
    if work_units > PNL_TIMELINE_MAX_WORK_UNITS:
        raise PnlTimelineWorkBudgetError(
            f"Requested timeline requires {work_units:,} minute-instrument work units; "
            f"maximum is {PNL_TIMELINE_MAX_WORK_UNITS:,}. Shorten the window or narrow "
            "the wallet scope."
        )


async def _load_mark_candles(
    repo: Repository,
    refs: Sequence[InstrumentSymbolRefRow],
    from_time: datetime,
    to_time: datetime,
    as_of: datetime,
) -> list[PnlTimelineCandleRow]:
    """Load one bounded batch of raw mark candles for trusted references.

    Args:
        repo: Repository providing the timeline candle read.
        refs: Trusted symbol references to load.
        from_time: Window start whose minute floor anchors the read.
        to_time: Inclusive series end.
        as_of: Snapshot time threading the candle read.

    Returns:
        Raw finalized one-minute mark candles, or an empty list when no
        references need marks.
    """
    if not refs:
        return []
    grid_start = from_time.replace(second=0, microsecond=0)
    candles = await repo.get_pnl_timeline_candles(
        refs,
        grid_start - timedelta(minutes=1),
        to_time,
        as_of,
    )
    return list(candles)


def _build_marks_from_candles(
    candles: Sequence[PnlTimelineCandleRow],
    quote_by_instrument: Mapping[str, str],
    valuation_ccy: str,
    rates: FxRateMap,
    venues_by_instrument: Mapping[str, FxVenueMap],
) -> MarkMap:
    """Convert positive raw closes through each instrument's FX plane map.

    Args:
        candles: Raw finalized mark candles.
        quote_by_instrument: Proven denomination of each candle close.
        valuation_ccy: Currency the returned marks are denominated in.
        rates: Plane-qualified exact-minute FX closes.
        venues_by_instrument: Consumer-pinned planes keyed by instrument and pair.

    Returns:
        Positive converted marks keyed by instrument and closing minute.
    """
    marks: dict[tuple[str, datetime], float | None] = {}
    for candle in candles:
        close = candle["close"]
        if not is_positive_finite(close):
            continue
        instrument_public_id = candle["instrument_public_id"]
        quote_currency = quote_by_instrument[instrument_public_id]
        mark_minute = candle["open_at"] + timedelta(minutes=1)
        converted = convert_amount(
            close,
            quote_currency,
            valuation_ccy,
            mark_minute,
            rates,
            venues_by_instrument.get(instrument_public_id, {}),
        )
        if is_positive_finite(converted):
            marks[(instrument_public_id, mark_minute)] = converted
    return marks


async def build_marks(
    repo: Repository,
    refs: Sequence[InstrumentSymbolRefRow],
    from_time: datetime,
    to_time: datetime,
    as_of: datetime,
    valuation_ccy: str,
    rates: FxRateMap | None = None,
    venues: FxVenueMap | None = None,
) -> MarkMap:
    """Resolve valuation-currency marks from one batched candle read.

    Every non-null-quote reference participates when a rate map is supplied.
    Each positive candle close is converted from that quote into
    ``valuation_ccy`` at ``open_at + 1min``, the grid minute the bar values
    without look-ahead. A missing exact-minute rate emits no mark. When ``rates``
    is omitted, only already-native references participate, retaining the
    helper's direct-mark mode. The repository resolves each reference on its
    canonical source venue but projects the requesting instrument's own public
    id, preserving PAPER identity in the mark map.

    Args:
        repo: Repository providing the batched timeline candle read.
        refs: Symbol references for the instruments the executions touch.
        from_time: Window start (its minute floor anchors the candle range).
        to_time: Window end.
        as_of: Snapshot time threading the candle read.
        valuation_ccy: Currency the returned marks are denominated in.
        rates: Exact-minute pinned-plane rates for foreign closes. Omission
            selects direct-mark mode and skips foreign references.
        venues: One consumer-pinned oriented plane per unordered currency pair.

    Returns:
        A mapping keyed by ``(instrument_public_id, grid_minute)`` to the
        converted valuation-currency close mark for that minute.
    """
    eligible_refs = [
        ref
        for ref in refs
        if ref["quote_currency"] == valuation_ccy
        or (rates is not None and ref["quote_currency"] is not None)
    ]
    if not eligible_refs:
        return {}
    resolved_rates: FxRateMap = {} if rates is None else rates
    resolved_venues: FxVenueMap = {} if venues is None else venues
    quote_by_instrument = {
        ref["instrument_public_id"]: cast(str, ref["quote_currency"]) for ref in eligible_refs
    }
    candles = await _load_mark_candles(
        repo,
        eligible_refs,
        from_time,
        to_time,
        as_of,
    )
    return _build_marks_from_candles(
        candles,
        quote_by_instrument,
        valuation_ccy,
        resolved_rates,
        dict.fromkeys(quote_by_instrument, resolved_venues),
    )


def _merge_price_ref_intervals(
    refs: Sequence[InstrumentSymbolRefRow],
) -> list[InstrumentSymbolRefRow]:
    """Merge touching intervals that carry the same price-proof projection."""
    grouped: dict[tuple[str, str, str, str, str | None], list[InstrumentSymbolRefRow]] = {}
    for ref in refs:
        key = (
            ref["native_symbol"],
            ref["exchange"],
            ref["instrument_exchange"],
            ref["base_currency"],
            ref["quote_currency"],
        )
        grouped.setdefault(key, []).append(ref)
    merged: list[InstrumentSymbolRefRow] = []
    for group in grouped.values():
        ordered = sorted(group, key=lambda ref: (ref["valid_from"], ref["valid_to"]))
        current = ordered[0].copy()
        for ref in ordered[1:]:
            if ref["valid_from"] > current["valid_to"]:
                merged.append(current)
                current = ref.copy()
                continue
            current["valid_to"] = max(current["valid_to"], ref["valid_to"])
        merged.append(current)
    return merged


def _partition_series_price_refs(
    instrument_spans: Mapping[str, tuple[datetime, datetime]],
    refs: Sequence[InstrumentSymbolRefRow],
) -> tuple[list[InstrumentSymbolRefRow], set[str]]:
    """Partition instrument spans by convertible price-currency proof.

    Every version known at the response horizon must agree on one base currency
    and one non-null quote currency. The quote may differ from the requested
    valuation currency because the series layer converts it later. Candidates are collapsed by
    their denomination-relevant projection, and touching intervals for one
    projection are merged before exactly one candidate must cover the event
    span. Missing, gapped, conflicting, and null-quote references remain
    untrusted while metadata-only version churn does not blank the series.

    Args:
        instrument_spans: Inclusive event-time bounds keyed by instrument.
        refs: Historical symbol-reference intervals for those instruments.

    Returns:
        Uniquely proven convertible references in input-instrument order and the
        instrument identities whose execution-price currency is untrusted.
    """
    refs_by_instrument: dict[str, list[InstrumentSymbolRefRow]] = {}
    for ref in refs:
        refs_by_instrument.setdefault(ref["instrument_public_id"], []).append(ref)
    trusted_refs: list[InstrumentSymbolRefRow] = []
    untrusted_instruments: set[str] = set()
    for instrument_public_id, (span_start, span_end) in instrument_spans.items():
        instrument_refs = refs_by_instrument.get(instrument_public_id, [])
        base_currencies = {ref["base_currency"] for ref in instrument_refs}
        quote_currencies = {ref["quote_currency"] for ref in instrument_refs}
        candidates = _merge_price_ref_intervals(
            [
                ref
                for ref in instrument_refs
                if ref["valid_from"] <= span_end and ref["valid_to"] > span_start
            ]
        )
        if (
            len(base_currencies) == 1
            and len(quote_currencies) == 1
            and None not in quote_currencies
            and len(candidates) == 1
            and candidates[0]["valid_from"] <= span_start
            and candidates[0]["valid_to"] > span_end
        ):
            trusted_refs.append(candidates[0])
        else:
            untrusted_instruments.add(instrument_public_id)
    return trusted_refs, untrusted_instruments


def _partition_price_refs(
    instrument_spans: Mapping[str, tuple[datetime, datetime]],
    refs: Sequence[InstrumentSymbolRefRow],
    valuation_ccy: str,
) -> tuple[list[InstrumentSymbolRefRow], set[str]]:
    """Partition raw marker prices by direct valuation-currency proof.

    Marker prices are intentionally not converted. This gate layers the raw
    overlay's direct-currency requirement on top of the same unanimous quote,
    interval-coverage, and projection proof used by the converted series.

    Args:
        instrument_spans: Inclusive event-time bounds keyed by instrument.
        refs: Historical symbol-reference intervals for those instruments.
        valuation_ccy: Currency a raw marker price must already represent.

    Returns:
        Direct-currency references and every instrument that fails either the
        shared identity proof or the marker-specific valuation gate.
    """
    trusted_refs, untrusted_instruments = _partition_series_price_refs(
        instrument_spans,
        refs,
    )
    direct_refs: list[InstrumentSymbolRefRow] = []
    for ref in trusted_refs:
        if ref["quote_currency"] == valuation_ccy:
            direct_refs.append(ref)
        else:
            untrusted_instruments.add(ref["instrument_public_id"])
    return direct_refs, untrusted_instruments


def _partition_series_execution_price_refs(
    execution_rows: Sequence[PnlTimelineExecutionRow],
    refs: Sequence[InstrumentSymbolRefRow],
) -> tuple[list[InstrumentSymbolRefRow], set[str]]:
    """Prove convertible execution prices over complete instrument spans.

    The shared series proof establishes one non-null quote and one unchanged
    source projection over every fill, then independently requires immutable
    execution venue lineage to match the reference's owning venue.

    Args:
        execution_rows: Scope execution prefix replayed by the P&L kernel.
        refs: Historical symbol-reference intervals for those instruments.

    Returns:
        References whose quote and venue can be converted safely, and the
        instruments whose denomination or venue remains untrusted.
    """
    spans: dict[str, tuple[datetime, datetime]] = {}
    for row in execution_rows:
        instrument_public_id = row["instrument_public_id"]
        event_time = row["timestamp"]
        existing = spans.get(instrument_public_id)
        if existing is None:
            spans[instrument_public_id] = (event_time, event_time)
        else:
            spans[instrument_public_id] = (
                min(existing[0], event_time),
                max(existing[1], event_time),
            )
    trusted_refs, untrusted_instruments = _partition_series_price_refs(spans, refs)
    trusted_by_instrument = {ref["instrument_public_id"]: ref for ref in trusted_refs}
    for row in execution_rows:
        instrument_public_id = row["instrument_public_id"]
        ref = trusted_by_instrument.get(instrument_public_id)
        if ref is not None and row["exchange"] != ref["instrument_exchange"]:
            untrusted_instruments.add(instrument_public_id)
    if untrusted_instruments:
        trusted_refs = [
            ref for ref in trusted_refs if ref["instrument_public_id"] not in untrusted_instruments
        ]
    return trusted_refs, untrusted_instruments


def _partition_execution_price_refs(
    execution_rows: Sequence[PnlTimelineExecutionRow],
    refs: Sequence[InstrumentSymbolRefRow],
    valuation_ccy: str,
) -> tuple[list[InstrumentSymbolRefRow], set[str]]:
    """Partition replayed execution instruments using their complete spans.

    Args:
        execution_rows: Scope execution prefix replayed by the P&L kernel.
        refs: Historical symbol-reference intervals for those instruments.
        valuation_ccy: Currency every execution price must already represent.

    Returns:
        References that prove one unchanged identity and owning venue covered
        every fill, and the instruments whose execution-price denomination or
        venue remains untrusted.
    """
    trusted_refs, untrusted_instruments = _partition_series_execution_price_refs(
        execution_rows,
        refs,
    )
    direct_refs: list[InstrumentSymbolRefRow] = []
    for ref in trusted_refs:
        if ref["quote_currency"] == valuation_ccy:
            direct_refs.append(ref)
        else:
            untrusted_instruments.add(ref["instrument_public_id"])
    return direct_refs, untrusted_instruments


type _FxMinuteRequirements = dict[FxPairKey, set[datetime]]
"""Exact conversion minutes grouped by unordered currency pair."""

type _FxInstrumentRequirements = dict[str, _FxMinuteRequirements]
"""Exact conversion minutes grouped first by consuming instrument."""

type _FxIdentityPlanes = dict[str, PnlFxRatePlane]
"""Canonical source plane keyed by the held instrument that justifies it."""

type _FxPlanesByInstrument = dict[str, dict[FxPairKey, PnlFxRatePlane]]
"""Selected conversion planes keyed by consuming instrument and pair."""


def _add_fx_minute(
    requirements: _FxInstrumentRequirements,
    instrument_public_id: str,
    currency: str,
    valuation_ccy: str,
    minute: datetime,
) -> None:
    """Add one foreign-currency conversion minute when evidence is required.

    Args:
        requirements: Mutable exact-minute requirements grouped by instrument.
        instrument_public_id: Instrument whose contribution needs the rate.
        currency: Denomination of the value being converted.
        valuation_ccy: Target series currency.
        minute: Exact rate minute needed by the conversion.
    """
    if not currency or currency == valuation_ccy:
        return
    pair_requirements = requirements.setdefault(instrument_public_id, {})
    pair_requirements.setdefault(currency_pair_key(currency, valuation_ccy), set()).add(minute)


def _event_fx_minutes(
    execution_rows: Sequence[PnlTimelineExecutionRow],
    accrual_rows: Sequence[PnlTimelineAccrualRow],
    quote_by_instrument: Mapping[str, str],
    valuation_ccy: str,
) -> _FxInstrumentRequirements:
    """Collect exact rate minutes needed by prices, fees, and accruals.

    A positive foreign execution price enters the average-cost kernel only after
    conversion at its fill minute. Requirements stop when an invalid size or an
    unknown-basis close permanently withholds that instrument, and prices that
    cannot repair an already unknown basis do not steer peer selection. Nonzero
    foreign fees and accruals need the same proof while their instrument remains
    publishable; exact zero and native-currency values need no candle.

    Args:
        execution_rows: Trusted scope executions replayed by the kernel.
        accrual_rows: Scope accruals, whose amount carries ``amount_asset``.
        quote_by_instrument: Proven execution-price denominations.
        valuation_ccy: Currency the series is valued in.

    Returns:
        Exact required minutes grouped by instrument and unordered currency pair.
    """
    needed: _FxInstrumentRequirements = {}
    last_effective: dict[str, datetime] = {}
    position_qty: dict[str, float] = {}
    basis_unknown: set[str] = set()
    untrusted_at: dict[str, datetime] = {}
    for row in execution_rows:
        instrument_public_id = row["instrument_public_id"]
        event_time = row["timestamp"]
        effective_time = max(last_effective.get(instrument_public_id, event_time), event_time)
        last_effective[instrument_public_id] = effective_time
        if instrument_public_id in untrusted_at:
            continue
        size = row["size"]
        if not math.isfinite(size) or size < 0.0:
            untrusted_at[instrument_public_id] = effective_time
            continue
        old_qty = position_qty.get(instrument_public_id, 0.0)
        signed_size = size if row["side"] == "buy" else -size
        is_increasing = (old_qty >= 0.0 and signed_size > 0.0) or (
            old_qty <= 0.0 and signed_size < 0.0
        )
        closed_qty = 0.0 if is_increasing else min(size, abs(old_qty))
        price_is_known_valid = is_positive_finite(row["price"])
        if closed_qty > 0.0 and (not price_is_known_valid or instrument_public_id in basis_unknown):
            untrusted_at[instrument_public_id] = effective_time
            continue
        minute = _rate_minute(row["timestamp"])
        quote_currency = quote_by_instrument.get(instrument_public_id)
        if (
            quote_currency is not None
            and size > 0.0
            and price_is_known_valid
            and instrument_public_id not in basis_unknown
        ):
            _add_fx_minute(
                needed,
                instrument_public_id,
                quote_currency,
                valuation_ccy,
                minute,
            )
        if row["fee"] != 0.0:
            _add_fx_minute(
                needed,
                instrument_public_id,
                row["fee_asset"],
                valuation_ccy,
                minute,
            )
        new_qty = old_qty + signed_size
        if abs(new_qty) < FLAT_EPSILON:
            new_qty = 0.0
            basis_unknown.discard(instrument_public_id)
        elif size > 0.0 and not price_is_known_valid:
            basis_unknown.add(instrument_public_id)
        position_qty[instrument_public_id] = new_qty
    for accrual in accrual_rows:
        invalid_at = untrusted_at.get(accrual["instrument_public_id"])
        if accrual["amount"] != 0.0 and (invalid_at is None or accrual["accrued_at"] < invalid_at):
            _add_fx_minute(
                needed,
                accrual["instrument_public_id"],
                accrual["amount_asset"],
                valuation_ccy,
                _rate_minute(accrual["accrued_at"]),
            )
    return needed


def _mark_fx_minutes(
    candles: Sequence[PnlTimelineCandleRow],
    execution_rows: Sequence[PnlTimelineExecutionRow],
    quote_by_instrument: Mapping[str, str],
    valuation_ccy: str,
    from_time: datetime,
    to_time: datetime,
) -> _FxInstrumentRequirements:
    """Collect exact rate minutes for positive marks of non-flat instruments.

    Position quantities replay in the same per-instrument monotone-clamped event
    order as the pure builder. Global regression shadows are derived from the
    complete replay prefix, including instruments whose price identity is not
    trusted. Candles before an opening fill, after a full close, or after a
    known-invalid size or price are not valuation consumers, so they cannot steer
    a shared FX plane.

    Args:
        candles: Raw finalized mark candles already bounded to the chart window.
        execution_rows: Complete scope fills in replay order through the chart end.
        quote_by_instrument: Proven denomination of each candle close.
        valuation_ccy: Currency the series is valued in.
        from_time: Requested chart start whose minute floor anchors the grid.
        to_time: Inclusive requested chart end.

    Returns:
        Exact required minutes grouped by instrument and unordered currency pair.
    """
    effective_events: dict[str, list[tuple[datetime, str, int, float, bool]]] = {}
    last_effective: dict[str, datetime] = {}
    regression_shadows: list[tuple[datetime, datetime]] = []
    for row in execution_rows:
        instrument_public_id = row["instrument_public_id"]
        event_time = row["timestamp"]
        previous_effective = last_effective.get(instrument_public_id)
        if previous_effective is not None and event_time < previous_effective:
            regression_shadows.append((event_time, previous_effective))
        effective_time = max(previous_effective or event_time, event_time)
        last_effective[instrument_public_id] = effective_time
        if instrument_public_id not in quote_by_instrument:
            continue
        size = row["size"]
        invalid_size = not math.isfinite(size) or size < 0.0
        signed_size = 0.0 if invalid_size else size if row["side"] == "buy" else -size
        effective_events.setdefault(instrument_public_id, []).append(
            (
                effective_time,
                row["exchange"],
                row["scope_sequence"],
                signed_size,
                invalid_size or (size > 0.0 and not is_positive_finite(row["price"])),
            )
        )
    candles_by_instrument: dict[str, list[PnlTimelineCandleRow]] = {}
    for candle in candles:
        candles_by_instrument.setdefault(candle["instrument_public_id"], []).append(candle)
    needed: _FxInstrumentRequirements = {}
    grid_start = from_time.replace(second=0, microsecond=0)
    for instrument_public_id, instrument_candles in candles_by_instrument.items():
        events = sorted(effective_events.get(instrument_public_id, []))
        event_index = 0
        position_qty = 0.0
        mark_eligible = True
        quote_currency = quote_by_instrument[instrument_public_id]
        for candle in sorted(instrument_candles, key=lambda item: item["open_at"]):
            mark_minute = candle["open_at"] + timedelta(minutes=1)
            while event_index < len(events) and events[event_index][0] <= mark_minute:
                position_qty += events[event_index][3]
                if events[event_index][4]:
                    mark_eligible = False
                if abs(position_qty) < FLAT_EPSILON:
                    position_qty = 0.0
                event_index += 1
            if (
                not is_positive_finite(candle["close"])
                or not mark_eligible
                or mark_minute < grid_start
                or mark_minute > to_time
                or abs(position_qty) < FLAT_EPSILON
                or any(
                    shadow_start <= mark_minute < shadow_end
                    for shadow_start, shadow_end in regression_shadows
                )
            ):
                continue
            _add_fx_minute(
                needed,
                instrument_public_id,
                quote_currency,
                valuation_ccy,
                mark_minute,
            )
    return needed


def _merge_fx_minutes(
    first: Mapping[FxPairKey, set[datetime]],
    second: Mapping[FxPairKey, set[datetime]],
) -> _FxMinuteRequirements:
    """Merge two pair-minute requirement maps without widening either read.

    Args:
        first: First bounded requirement map.
        second: Second bounded requirement map.

    Returns:
        Unioned exact minutes used only for request-wide venue resolution.
    """
    merged: _FxMinuteRequirements = {pair: set(minutes) for pair, minutes in first.items()}
    for pair, minutes in second.items():
        merged.setdefault(pair, set()).update(minutes)
    return merged


def _merge_instrument_fx_minutes(
    first: Mapping[str, _FxMinuteRequirements],
    second: Mapping[str, _FxMinuteRequirements],
) -> _FxInstrumentRequirements:
    """Merge exact-minute requirements while preserving consumer identity.

    Args:
        first: First bounded requirement map grouped by instrument.
        second: Second bounded requirement map grouped by instrument.

    Returns:
        Unioned exact minutes grouped by instrument and pair.
    """
    merged: _FxInstrumentRequirements = {
        instrument_public_id: {pair: set(minutes) for pair, minutes in requirements.items()}
        for instrument_public_id, requirements in first.items()
    }
    for instrument_public_id, requirements in second.items():
        target = merged.setdefault(instrument_public_id, {})
        for pair, minutes in requirements.items():
            target.setdefault(pair, set()).update(minutes)
    return merged


def _collapse_fx_minutes(
    requirements: Mapping[str, _FxMinuteRequirements],
) -> _FxMinuteRequirements:
    """Collapse instrument requirements for bounded repository reads.

    Args:
        requirements: Exact minutes grouped by consuming instrument and pair.

    Returns:
        Unioned exact minutes grouped only by currency pair.
    """
    collapsed: _FxMinuteRequirements = {}
    for instrument_requirements in requirements.values():
        for pair, minutes in instrument_requirements.items():
            collapsed.setdefault(pair, set()).update(minutes)
    return collapsed


def _identity_fx_planes(
    refs: Sequence[InstrumentSymbolRefRow],
    requirements: Mapping[str, _FxMinuteRequirements],
) -> _FxIdentityPlanes:
    """Find canonical held-symbol planes justified by each instrument.

    A trusted held instrument supplies its exact oriented candle plane only when
    that same instrument needs conversion across its own currency legs. The
    plane constrains only that instrument and cannot suppress a peer using the
    same unordered pair.

    Args:
        refs: Trusted held-instrument symbol references.
        requirements: Exact conversion minutes grouped by consuming instrument.

    Returns:
        Canonical oriented source plane keyed by its justifying instrument.
    """
    identities: _FxIdentityPlanes = {}
    for ref in refs:
        instrument_public_id = ref["instrument_public_id"]
        quote_currency = cast(str, ref["quote_currency"])
        pair = currency_pair_key(ref["base_currency"], quote_currency)
        if pair in requirements.get(instrument_public_id, {}):
            identities[instrument_public_id] = (
                ref["base_currency"],
                quote_currency,
                ref["exchange"],
            )
    return identities


def _general_fx_minutes(
    requirements: Mapping[str, _FxMinuteRequirements],
    identity_planes: Mapping[str, PnlFxRatePlane],
) -> _FxMinuteRequirements:
    """Collect minutes whose consumers have no own-pair identity override.

    Identity-only minutes cannot steer the shared plane used by peers. Other
    currency pairs needed by an identity instrument remain general consumers.

    Args:
        requirements: Exact conversion minutes grouped by consuming instrument.
        identity_planes: Canonical plane claims keyed by held instrument.

    Returns:
        General-consumer exact minutes grouped by unordered currency pair.
    """
    general: _FxMinuteRequirements = {}
    for instrument_public_id, instrument_requirements in requirements.items():
        identity_plane = identity_planes.get(instrument_public_id)
        identity_pair = (
            None
            if identity_plane is None
            else currency_pair_key(identity_plane[0], identity_plane[1])
        )
        for pair, minutes in instrument_requirements.items():
            if pair == identity_pair:
                continue
            general.setdefault(pair, set()).update(minutes)
    return general


def _fx_requirement_range(
    requirements: Mapping[FxPairKey, set[datetime]],
) -> tuple[datetime, datetime] | None:
    """Return the bounded candle-open range covering exact rate minutes.

    Args:
        requirements: Exact rate minutes grouped by pair.

    Returns:
        Inclusive candle-open bounds, or ``None`` for no requirements.
    """
    minutes = [minute for pair_minutes in requirements.values() for minute in pair_minutes]
    if not minutes:
        return None
    return min(minutes) - timedelta(minutes=1), max(minutes)


def _oriented_fx_pairs(pairs: Sequence[FxPairKey]) -> list[tuple[str, str]]:
    """Expand unordered pairs into both publication orientations.

    Args:
        pairs: Unordered currency-pair identities.

    Returns:
        Deterministically ordered direct and inverse currency legs.
    """
    oriented: set[tuple[str, str]] = set()
    for first, second in pairs:
        oriented.add((first, second))
        oriented.add((second, first))
    return sorted(oriented)


async def _discover_fx_candidates(
    repo: Repository,
    requirements: Mapping[FxPairKey, set[datetime]],
    as_of: datetime,
) -> list[PnlFxRatePlane]:
    """Discover every spot-FX plane with evidence in one bounded rate window.

    Args:
        repo: Repository providing bounded forex-plane discovery.
        requirements: Exact pair minutes in this mark or event window.
        as_of: Knowledge horizon threading the repository read.

    Returns:
        Concrete oriented venue planes with evidence in this window.
    """
    pairs = sorted(requirements)
    bounds = _fx_requirement_range(requirements)
    if not pairs or bounds is None:
        return []
    return await repo.get_pnl_fx_rate_exchanges(
        _oriented_fx_pairs(pairs),
        bounds[0],
        bounds[1],
        as_of,
    )


def _candidate_planes(
    candidates: Sequence[PnlFxRatePlane],
    identity_planes: Mapping[str, PnlFxRatePlane],
) -> dict[FxPairKey, set[PnlFxRatePlane]]:
    """Combine discovered planes with instrument-owned canonical planes.

    Args:
        candidates: Oriented venue planes discovered across bounded windows.
        identity_planes: Canonical held-symbol planes keyed by instrument.

    Returns:
        Eligible oriented planes grouped by unordered pair.
    """
    planes: dict[FxPairKey, set[PnlFxRatePlane]] = {}
    for candidate in candidates:
        base, quote, _ = candidate
        planes.setdefault(currency_pair_key(base, quote), set()).add(candidate)
    for identity in identity_planes.values():
        base, quote, _ = identity
        planes.setdefault(currency_pair_key(base, quote), set()).add(identity)
    return planes


async def _load_fx_candidate_rows(
    repo: Repository,
    requirements: Mapping[FxPairKey, set[datetime]],
    candidate_planes: Mapping[FxPairKey, set[PnlFxRatePlane]],
    as_of: datetime,
) -> list[PnlFxRateRow]:
    """Load eligible exact oriented planes in one bounded window.

    Args:
        repo: Repository providing exact venue-filtered forex candles.
        requirements: Exact pair minutes in this mark or event window.
        candidate_planes: Eligible discovered and identity planes by pair.
        as_of: Knowledge horizon threading the repository read.

    Returns:
        Candidate rows used to resolve shared and instrument-owned planes.
    """
    bounds = _fx_requirement_range(requirements)
    if bounds is None:
        return []
    planes: set[PnlFxRatePlane] = set()
    for pair in requirements:
        planes.update(candidate_planes.get(pair, set()))
    if not planes:
        return []
    return await repo.get_pnl_fx_rate_candles(
        sorted(planes),
        bounds[0],
        bounds[1],
        as_of,
    )


def _resolve_fx_planes(
    requirements: Mapping[FxPairKey, set[datetime]],
    candidate_planes: Mapping[FxPairKey, set[PnlFxRatePlane]],
    rows: Sequence[PnlFxRateRow],
    valuation_ccy: str,
) -> dict[FxPairKey, PnlFxRatePlane]:
    """Resolve one oriented plane per pair for general conversion consumers.

    The oriented series covering the most exact general-consumer minutes wins.
    Identity-only requirements have already been excluded so they cannot steer
    a peer's plane. When several complete planes cover a PLN pair, Walutomat is
    preferred. Other ties preserve lexical venue order, then prefer the
    source-to-valuation orientation. Duplicate conflicts on a full plane-minute
    identity do not count as coverage, and zero usable coverage never wins.

    Args:
        requirements: Exact mark and event minutes for general consumers.
        candidate_planes: Eligible discovered or forced planes by pair.
        rows: Candidate candle rows from both bounded reads.
        valuation_ccy: Target currency defining the preferred conversion direction.

    Returns:
        One selected oriented plane for each resolvable unordered pair.
    """
    covered: dict[PnlFxRatePlane, set[datetime]] = {}
    for (base, quote, exchange, minute), close in build_fx_rates(rows).items():
        if not is_positive_finite(close):
            continue
        pair = currency_pair_key(base, quote)
        if minute in requirements.get(pair, set()):
            covered.setdefault((base, quote, exchange), set()).add(minute)
    resolved: dict[FxPairKey, PnlFxRatePlane] = {}
    for pair, minutes in requirements.items():
        planes = candidate_planes.get(pair, set())
        if not planes:
            continue
        coverage_by_plane = {
            plane: len(covered.get(plane, set()).intersection(minutes)) for plane in planes
        }
        best_coverage = max(coverage_by_plane.values())
        if best_coverage == 0:
            continue
        finalists = {
            plane for plane, coverage in coverage_by_plane.items() if coverage == best_coverage
        }
        if best_coverage == len(minutes) and "PLN" in pair:
            walutomat = {plane for plane in finalists if plane[2] == "walutomat"}
            if walutomat:
                finalists = walutomat
        selected_exchange = min(plane[2] for plane in finalists)
        venue_finalists = {plane for plane in finalists if plane[2] == selected_exchange}
        source_currency = pair[1] if pair[0] == valuation_ccy else pair[0]
        direct = {
            plane
            for plane in venue_finalists
            if (plane[0], plane[1]) == (source_currency, valuation_ccy)
        }
        resolved[pair] = min(direct or venue_finalists)
    return resolved


def _resolve_identity_fx_planes(
    requirements: Mapping[str, _FxMinuteRequirements],
    identity_planes: Mapping[str, PnlFxRatePlane],
    rows: Sequence[PnlFxRateRow],
) -> _FxIdentityPlanes:
    """Pin instrument-owned planes only after they prove usable coverage.

    Partial positive coverage pins the canonical plane for the instrument's
    whole request so a rival cannot fill later gaps. Zero usable coverage does
    not pin or enter provenance, but the unresolved identity claim still blocks
    that instrument from borrowing the shared plane.

    Args:
        requirements: Exact conversion minutes grouped by consuming instrument.
        identity_planes: Canonical source-plane claims keyed by instrument.
        rows: Candidate candle rows from both bounded reads.

    Returns:
        Canonical planes with nonzero usable coverage keyed by instrument.
    """
    covered: dict[PnlFxRatePlane, set[datetime]] = {}
    for (base, quote, exchange, minute), close in build_fx_rates(rows).items():
        if is_positive_finite(close):
            covered.setdefault((base, quote, exchange), set()).add(minute)
    resolved: _FxIdentityPlanes = {}
    for instrument_public_id, plane in identity_planes.items():
        pair = currency_pair_key(plane[0], plane[1])
        minutes = requirements.get(instrument_public_id, {}).get(pair, set())
        if covered.get(plane, set()).intersection(minutes):
            resolved[instrument_public_id] = plane
    return resolved


def _fx_planes_by_instrument(
    requirements: Mapping[str, _FxMinuteRequirements],
    shared_planes: Mapping[FxPairKey, PnlFxRatePlane],
    identity_planes: Mapping[str, PnlFxRatePlane],
    resolved_identity_planes: Mapping[str, PnlFxRatePlane],
) -> _FxPlanesByInstrument:
    """Choose one plane per instrument-pair without cross-consumer borrowing.

    Args:
        requirements: Exact conversion minutes grouped by consuming instrument.
        shared_planes: Coverage-ranked planes for general consumers.
        identity_planes: Canonical source-plane claims keyed by instrument.
        resolved_identity_planes: Identity planes with nonzero usable coverage.

    Returns:
        Selected planes for every instrument and required pair.
    """
    selected: _FxPlanesByInstrument = {}
    for instrument_public_id, instrument_requirements in requirements.items():
        identity_plane = identity_planes.get(instrument_public_id)
        identity_pair = (
            None
            if identity_plane is None
            else currency_pair_key(identity_plane[0], identity_plane[1])
        )
        instrument_planes: dict[FxPairKey, PnlFxRatePlane] = {}
        for pair in instrument_requirements:
            if pair == identity_pair:
                resolved_identity = resolved_identity_planes.get(instrument_public_id)
                if resolved_identity is not None:
                    instrument_planes[pair] = resolved_identity
                continue
            shared_plane = shared_planes.get(pair)
            if shared_plane is not None:
                instrument_planes[pair] = shared_plane
        selected[instrument_public_id] = instrument_planes
    return selected


async def _load_request_fx_rates(
    repo: Repository,
    mark_requirements: Mapping[str, _FxMinuteRequirements],
    event_requirements: Mapping[str, _FxMinuteRequirements],
    identity_planes: Mapping[str, PnlFxRatePlane],
    as_of: datetime,
    valuation_ccy: str,
) -> tuple[
    FxRateMap,
    _FxPlanesByInstrument,
    set[tuple[FxPairKey, PnlFxRatePlane]],
]:
    """Load shared and instrument-owned oriented FX planes for one request.

    Mark and event reads retain their own bounded ranges, so an old opening fill
    never widens the chart-window read. Candidate evidence from both is combined
    before shared and per-instrument resolution, then every unused orientation
    and venue is discarded. Each instrument uses one plane per pair for its
    marks, execution prices, fees, and accruals at every minute.

    Args:
        repo: Repository providing bounded FX plane and candle reads.
        mark_requirements: Mark conversion minutes grouped by instrument.
        event_requirements: Fill and accrual minutes grouped by instrument.
        identity_planes: Canonical held-symbol planes keyed by instrument.
        as_of: Knowledge horizon threading repository reads.
        valuation_ccy: Target currency defining conversion-direction tie-breaks.

    Returns:
        Plane-filtered rates, per-instrument selections, and distinct used planes.
    """
    instrument_requirements = _merge_instrument_fx_minutes(
        mark_requirements,
        event_requirements,
    )
    mark_pair_requirements = _collapse_fx_minutes(mark_requirements)
    event_pair_requirements = _collapse_fx_minutes(event_requirements)
    requirements = _merge_fx_minutes(mark_pair_requirements, event_pair_requirements)
    if not requirements:
        return {}, {}, set()
    mark_candidates = await _discover_fx_candidates(
        repo,
        mark_pair_requirements,
        as_of,
    )
    event_candidates = await _discover_fx_candidates(
        repo,
        event_pair_requirements,
        as_of,
    )
    candidate_planes = _candidate_planes(
        [*mark_candidates, *event_candidates],
        identity_planes,
    )
    mark_rows = await _load_fx_candidate_rows(
        repo,
        mark_pair_requirements,
        candidate_planes,
        as_of,
    )
    event_rows = await _load_fx_candidate_rows(
        repo,
        event_pair_requirements,
        candidate_planes,
        as_of,
    )
    rows = [*mark_rows, *event_rows]
    shared_planes = _resolve_fx_planes(
        _general_fx_minutes(instrument_requirements, identity_planes),
        candidate_planes,
        rows,
        valuation_ccy,
    )
    resolved_identity_planes = _resolve_identity_fx_planes(
        instrument_requirements,
        identity_planes,
        rows,
    )
    planes_by_instrument = _fx_planes_by_instrument(
        instrument_requirements,
        shared_planes,
        identity_planes,
        resolved_identity_planes,
    )
    used_planes = {
        (pair, plane)
        for instrument_planes in planes_by_instrument.values()
        for pair, plane in instrument_planes.items()
    }
    selected_planes = {plane for _, plane in used_planes}
    selected_rows = [
        row for row in rows if (row["base"], row["quote"], row["exchange"]) in selected_planes
    ]
    return build_fx_rates(selected_rows), planes_by_instrument, used_planes


async def build_wallet_pnl_series(
    repo: Repository,
    wallet_public_id: str,
    mode: str,
    from_time: datetime,
    to_time: datetime,
    granularity: str,
    as_of: datetime,
    valuation_ccy: str = "USD",
    execution_rows: Sequence[PnlTimelineExecutionRow] | None = None,
) -> PnlWalletSeriesResult:
    """Reconstruct one wallet/mode scope's Net-P&L-since-activation series.

    Reads the scope's append-only execution prefix, exact order lineage, and
    funding accruals; proves each replayed instrument's quote and venue; converts
    execution prices, marks, fees, and accruals from finalized exact-minute 1m
    candles; and calls the pure builder with ``opening=None``. Every average-cost
    pool input is therefore denominated in ``valuation_ccy`` before replay.
    Missing execution rates become ``NaN`` for the builder's tiered D1 handling,
    while missing mark rates simply omit that instrument-minute mark. Lineage is
    accepted only when exactly one candidate row resolves an execution order.
    Before the build, durable fill-gap evidence is consulted for every
    fill-bearing shard in the exact wallet/mode scope; a proven gap withholds the
    entire result.

    Args:
        repo: Repository providing the scope reads and candle marks.
        wallet_public_id: Wallet scope to reconstruct.
        mode: Trading mode scope (``live``, ``paper``).
        from_time: Inclusive series window start.
        to_time: Inclusive series window end.
        granularity: One of ``'1m'``, ``'5m'``, ``'1h'``, ``'1d'``.
        as_of: Effective knowledge horizon for the execution commit watermark,
            accrual SCD2 versions, and candle SCD2 versions.
        valuation_ccy: Currency the series components are expressed in.
        execution_rows: Optional preloaded execution prefix used by the marker
            endpoint to avoid issuing the same scope read twice.

    Returns:
        The built :class:`PnlTimelineResult` at the requested granularity.

    Raises:
        PnlTimelineWorkBudgetError: When raw grid minute-instrument work is
            above :data:`PNL_TIMELINE_MAX_WORK_UNITS`.
        ValueError: When ``granularity`` is not a supported value (surfaced by
            the pure builder).
    """
    fill_gap = await _scope_has_fill_gap(repo, wallet_public_id, mode, as_of)
    loaded_execution_rows = (
        await repo.get_pnl_timeline_executions(wallet_public_id, mode, as_of)
        if execution_rows is None
        else list(execution_rows)
    )
    order_public_ids = list(dict.fromkeys(row["order_public_id"] for row in loaded_execution_rows))
    lineage_rows = await repo.get_pnl_timeline_execution_lineage(order_public_ids, as_of)
    lineage = _build_execution_lineage(lineage_rows)
    accrual_rows = await repo.get_accruals_for_pnl(wallet_public_id, mode, as_of)
    instrument_ids = list(
        dict.fromkeys(row["instrument_public_id"] for row in loaded_execution_rows)
    )
    work_instrument_ids = set(instrument_ids)
    work_instrument_ids.update(row["instrument_public_id"] for row in accrual_rows)
    _enforce_total_work_budget(from_time, to_time, len(work_instrument_ids))
    refs = await repo.get_instrument_symbol_refs(instrument_ids, as_of)
    replayed_execution_rows = [row for row in loaded_execution_rows if row["timestamp"] <= to_time]
    trusted_refs, untrusted_price_instruments = _partition_series_execution_price_refs(
        replayed_execution_rows,
        refs,
    )
    quote_by_instrument = {
        ref["instrument_public_id"]: cast(str, ref["quote_currency"]) for ref in trusted_refs
    }
    mark_candles = await _load_mark_candles(
        repo,
        trusted_refs,
        from_time,
        to_time,
        as_of,
    )
    trusted_instruments = set(quote_by_instrument)
    trusted_replayed_rows = [
        row for row in replayed_execution_rows if row["instrument_public_id"] in trusted_instruments
    ]
    replayed_accrual_rows = [row for row in accrual_rows if row["accrued_at"] <= to_time]
    mark_requirements = _mark_fx_minutes(
        mark_candles,
        loaded_execution_rows,
        quote_by_instrument,
        valuation_ccy,
        from_time,
        to_time,
    )
    event_requirements = _event_fx_minutes(
        trusted_replayed_rows,
        replayed_accrual_rows,
        quote_by_instrument,
        valuation_ccy,
    )
    instrument_requirements = _merge_instrument_fx_minutes(
        mark_requirements,
        event_requirements,
    )
    identity_planes = _identity_fx_planes(
        trusted_refs,
        instrument_requirements,
    )
    rates, planes_by_instrument, used_planes = await _load_request_fx_rates(
        repo,
        mark_requirements,
        event_requirements,
        identity_planes,
        as_of,
        valuation_ccy,
    )
    marks = _build_marks_from_candles(
        mark_candles,
        quote_by_instrument,
        valuation_ccy,
        rates,
        planes_by_instrument,
    )
    executions = [
        _to_timeline_execution(
            row,
            valuation_ccy,
            rates,
            quote_by_instrument.get(row["instrument_public_id"]),
            planes_by_instrument.get(row["instrument_public_id"], {}),
        )
        for row in loaded_execution_rows
    ]
    accruals = [
        _to_timeline_accrual(
            row,
            valuation_ccy,
            rates,
            planes_by_instrument.get(row["instrument_public_id"], {}),
        )
        for row in accrual_rows
    ]
    window = TimelineWindow(
        from_time=from_time,
        to_time=to_time,
        granularity=granularity,
        valuation_ccy=valuation_ccy,
    )
    result = build_pnl_timeline(
        executions,
        accruals,
        marks,
        window,
        opening=None,
        lineage=lineage,
        untrusted_price_instruments=untrusted_price_instruments,
    )
    if fill_gap:
        result = _withhold_series_for_fill_gap(result)
    rate_sources = tuple(
        PnlFxRateSource(
            source_currency=second if first == valuation_ccy else first,
            valuation_currency=valuation_ccy,
            base_currency=base_currency,
            quote_currency=quote_currency,
            exchange=exchange,
        )
        for (first, second), (base_currency, quote_currency, exchange) in sorted(used_planes)
    )
    return PnlWalletSeriesResult(
        points=result.points,
        granularity=result.granularity,
        valuation_ccy=result.valuation_ccy,
        rate_sources=rate_sources,
    )


def _fill_marker(
    row: PnlTimelineExecutionRow,
    trusted_price_instruments: set[str],
) -> PnlFillMarker:
    """Project one execution row into a fill marker."""
    return PnlFillMarker(
        marker_time=row["timestamp"],
        instrument_public_id=row["instrument_public_id"],
        side=row["side"],
        size=row["size"],
        price=(
            row["price"]
            if row["instrument_public_id"] in trusted_price_instruments
            and is_positive_finite(row["price"])
            else None
        ),
        execution_public_id=row["public_id"],
        order_public_id=row["order_public_id"],
        status=row["status"],
    )


def _signal_marker(
    row: PnlTimelineSignalMarkerRow,
    trusted_price_instruments: set[str],
) -> PnlSignalMarker:
    """Project one signal row without inferring existence from fills alone."""
    outcome: Literal["executed", "no_fill"] = "executed" if row["has_execution"] else "no_fill"
    return PnlSignalMarker(
        marker_time=row["fired_at"],
        instrument_public_id=row["instrument_public_id"],
        side=row["side"],
        strategy_name=row["strategy_name"],
        strength=row["strength"],
        reason=row["reason"],
        price=(
            row["price"]
            if row["instrument_public_id"] in trusted_price_instruments
            and is_positive_finite(row["price"])
            else None
        ),
        signal_public_id=row["public_id"],
        outcome=outcome,
        status=outcome,
    )


def _payload_string(row: PnlTimelineAiDecisionMarkerRow, key: str) -> str | None:
    """Return one event-payload string without coercing arbitrary JSON."""
    value = row["payload"].get(key)
    return value if isinstance(value, str) else None


def _ai_decision_marker(row: PnlTimelineAiDecisionMarkerRow) -> PnlAiDecisionMarker:
    """Project one AI decision, preserving reject and no-fill outcomes."""
    decision = _payload_string(row, "decision")
    if decision == "reject" or row["new_status"] == "resolved_rejected":
        outcome: Literal["executed", "rejected", "no_fill"] = "rejected"
    elif row["has_execution"]:
        outcome = "executed"
    else:
        outcome = "no_fill"
    return PnlAiDecisionMarker(
        marker_time=row["occurred_at"],
        instrument_public_id=row["instrument_public_id"],
        strategy_public_id=row["strategy_public_id"],
        review_public_id=row["review_public_id"],
        event_public_id=row["event_public_id"],
        decision=decision,
        rationale=_payload_string(row, "rationale"),
        outcome=outcome,
        status=row["new_status"],
    )


def _marker_source_public_id(marker: PnlTimelineMarker) -> str:
    """Return the source identity used to break marker-order ties."""
    if isinstance(marker, PnlFillMarker):
        return marker.execution_public_id
    if isinstance(marker, PnlSignalMarker):
        return marker.signal_public_id
    return marker.event_public_id


def _marker_sort_key(marker: PnlTimelineMarker) -> tuple[datetime, str, str]:
    """Build the deterministic chronological marker ordering key."""
    return marker.marker_time, marker.kind, _marker_source_public_id(marker)


async def build_wallet_pnl_timeline(
    repo: Repository,
    wallet_public_id: str,
    mode: str,
    from_time: datetime,
    to_time: datetime,
    granularity: str,
    as_of: datetime,
    valuation_ccy: str = "USD",
) -> PnlWalletTimelineResult:
    """Build a wallet series plus independently sourced decision markers.

    The execution prefix is loaded once and reused by the existing series
    builder. Signals and append-only AI decision events are read independently,
    which retains declined decisions and signals that never reached an order.
    Each independent marker read asks for ``limit + 1`` newest rows. All marker
    kinds are merged by ``(time, kind, source id)``; if the combined set exceeds
    the public cap, only the latest markers remain and ``markers_truncated`` is
    set so the omission is never silent.

    Args:
        repo: Repository providing series inputs and marker reads.
        wallet_public_id: Wallet scope to reconstruct.
        mode: Trading mode scope.
        from_time: Inclusive series and marker window start.
        to_time: Inclusive series and marker window end.
        granularity: Requested P&L point granularity.
        as_of: Effective knowledge horizon shared by the series, signals, and
            AI decision reads.
        valuation_ccy: Currency the series components are expressed in.

    Returns:
        The existing P&L series and its capped marker overlay.

    Raises:
        PnlTimelineWorkBudgetError: When the series work budget is exceeded.
        ValueError: When the pure builder rejects the requested window.
    """
    execution_rows = await repo.get_pnl_timeline_executions(wallet_public_id, mode, as_of)
    series = await build_wallet_pnl_series(
        repo,
        wallet_public_id,
        mode,
        from_time,
        to_time,
        granularity,
        as_of,
        valuation_ccy=valuation_ccy,
        execution_rows=execution_rows,
    )
    read_limit = PNL_TIMELINE_MARKER_LIMIT + 1
    signal_rows = await repo.get_pnl_timeline_signals(
        wallet_public_id,
        mode,
        from_time,
        to_time,
        as_of,
        read_limit,
    )
    ai_decision_rows = await repo.get_pnl_timeline_ai_decisions(
        wallet_public_id,
        mode,
        from_time,
        to_time,
        as_of,
        read_limit,
    )
    replayed_execution_rows = [row for row in execution_rows if row["timestamp"] <= to_time]
    marker_instrument_ids = list(
        dict.fromkeys(
            [row["instrument_public_id"] for row in replayed_execution_rows]
            + [row["instrument_public_id"] for row in signal_rows if row["price"] is not None]
        )
    )
    marker_refs = (
        await repo.get_instrument_symbol_refs(marker_instrument_ids, as_of)
        if marker_instrument_ids
        else []
    )
    trusted_execution_refs, _ = _partition_execution_price_refs(
        replayed_execution_rows, marker_refs, valuation_ccy
    )
    trusted_execution_instruments = {ref["instrument_public_id"] for ref in trusted_execution_refs}
    signal_spans: dict[str, tuple[datetime, datetime]] = {}
    for row in signal_rows:
        if row["price"] is None:
            continue
        instrument_public_id = row["instrument_public_id"]
        existing = signal_spans.get(instrument_public_id)
        if existing is None:
            signal_spans[instrument_public_id] = (row["fired_at"], row["fired_at"])
        else:
            signal_spans[instrument_public_id] = (
                min(existing[0], row["fired_at"]),
                max(existing[1], row["fired_at"]),
            )
    trusted_signal_refs, _ = _partition_price_refs(signal_spans, marker_refs, valuation_ccy)
    trusted_signal_instruments = {ref["instrument_public_id"] for ref in trusted_signal_refs}
    markers: list[PnlTimelineMarker] = [
        _fill_marker(row, trusted_execution_instruments)
        for row in execution_rows
        if from_time <= row["timestamp"] <= to_time
    ]
    markers.extend(_signal_marker(row, trusted_signal_instruments) for row in signal_rows)
    markers.extend(_ai_decision_marker(row) for row in ai_decision_rows)
    markers.sort(key=_marker_sort_key)
    markers_truncated = len(markers) > PNL_TIMELINE_MARKER_LIMIT
    if markers_truncated:
        markers = markers[-PNL_TIMELINE_MARKER_LIMIT:]
    return PnlWalletTimelineResult(
        series=series,
        markers=tuple(markers),
        marker_limit=PNL_TIMELINE_MARKER_LIMIT,
        markers_truncated=markers_truncated,
    )
