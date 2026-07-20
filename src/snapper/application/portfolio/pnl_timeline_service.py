"""Async orchestrator + mark builder for the P&L timeline (Phase 5A API layer).

The pure :mod:`snapper.application.portfolio.pnl_timeline` builder performs no
I/O — it consumes already-fetched executions, accruals, and a resolved USD mark
lookup. This module is the thin async seam that reads those inputs from the
repository, resolves the USD marks from finalized 1m candles, and calls the pure
builder. Keeping the I/O here is what lets the builder stay provably
deterministic and trivially unit-testable while the wiring is exercised against
the repository.

Mark resolution (checklist #13 — no historical FX in v1). For each distinct
instrument the executions touch, the orchestrator resolves its
``(native_symbol, source_exchange, quote_currency)`` via
:meth:`Repository.get_instrument_symbol_refs`. The source exchange is
``Instrument.source_exchange`` when present and ``Instrument.exchange``
otherwise, so a PAPER instrument reads the canonical venue's candles while its
own public id remains the mark-map key. A mark is produced ONLY when the
instrument's quote currency equals the window valuation currency, because a 1m
candle close is denominated in the quote currency and is therefore already a
direct valuation-currency mark with no FX conversion. For every other instrument
NO mark is emitted, so the pure builder marks those points incomplete rather than
fabricating an unconverted number. The mark for grid minute ``M`` is the close of
the finalized candle covering ``[M-1m, M)`` (``open_at == M - 1min``), so there
is no look-ahead: minute ``M`` is valued only from the bar that closed at ``M``.

Currency discipline for flows — UNKNOWN, never a fabricated ZERO. Exact zero is
currency-invariant, so a zero fee or accrual stays zero even when its asset is
empty or differs from the valuation currency. A nonzero fee or funding accrual
denominated in something other than the valuation currency cannot be converted in
v1 (the FX work is deferred), so it is passed to the builder as ``NaN`` rather
than silently zeroed or dropped. The builder's finiteness guard turns that into a
WITHHELD (untrusted) point, which is the honest outcome.
"""

import math
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timedelta
from typing import Final
from typing import Literal

from snapper.application.portfolio.fx_rates import FxRateMap
from snapper.application.portfolio.fx_rates import convert_amount
from snapper.application.portfolio.fx_rates import required_pairs
from snapper.application.portfolio.pnl_timeline import MarkMap
from snapper.application.portfolio.pnl_timeline import PnlInstrumentContribution
from snapper.application.portfolio.pnl_timeline import PnlTimelinePoint
from snapper.application.portfolio.pnl_timeline import PnlTimelineResult
from snapper.application.portfolio.pnl_timeline import TimelineAccrual
from snapper.application.portfolio.pnl_timeline import TimelineExecution
from snapper.application.portfolio.pnl_timeline import TimelineWindow
from snapper.application.portfolio.pnl_timeline import build_pnl_timeline
from snapper.data.repository import Repository
from snapper.data.repository_types import InstrumentSymbolRefRow
from snapper.data.repository_types import PnlFxRateRow
from snapper.data.repository_types import PnlTimelineAccrualRow
from snapper.data.repository_types import PnlTimelineAiDecisionMarkerRow
from snapper.data.repository_types import PnlTimelineCandleRow
from snapper.data.repository_types import PnlTimelineExecutionRow
from snapper.data.repository_types import PnlTimelineSignalMarkerRow

PNL_TIMELINE_MARK_SOURCE = "finalized_1m_candle_close"
"""Provenance label for the mark plane the timeline values against."""

PNL_TIMELINE_CALC_VERSION = "5A.2"
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
    price: float
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
class PnlWalletTimelineResult:
    """One reconstructed series with a bounded marker overlay."""

    series: PnlTimelineResult
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
    """Fold FX candle rows into the minute-keyed rate map.

    Several venues list the same pair, so the FIRST row for a
    ``(base, quote, minute)`` wins and later ones are ignored. The read returns
    rows sorted by ``(open_at, base, quote, exchange)``, which makes that winner
    the alphabetically-first venue — deterministic, so the same request always
    returns the same money instead of depending on row order.

    Args:
        rows: Finalized 1m closes from ``get_pnl_fx_rate_candles``.

    Returns:
        Rate map keyed by ``(base, quote, minute)`` where the minute is the
        instant the bar closed.
    """
    rates: dict[tuple[str, str, datetime], float] = {}
    for row in rows:
        rates.setdefault(
            (row["base"], row["quote"], row["open_at"] + timedelta(minutes=1)), row["close"]
        )
    return rates


def _to_timeline_execution(
    row: PnlTimelineExecutionRow, valuation_ccy: str, rates: FxRateMap
) -> TimelineExecution:
    """Map a repository execution row onto the pure builder's input.

    Exact zero passes through regardless of asset because zero is
    currency-invariant. A nonzero foreign fee is CONVERTED at the flow's own
    minute using our finalized 1m candles; only when no direct or inverse pair
    covers that minute does it stay UNKNOWN and pass as ``NaN``, which makes the
    builder withhold rather than understate a real cost. ``event_time`` is the row's ``timestamp`` (the
    time axis), never the nullable ``executed_at``.

    Args:
        row: One ``get_pnl_timeline_executions`` row.
        valuation_ccy: Currency the series is valued in.
        rates: Minute-keyed FX rates used to convert a foreign-denominated fee.

    Returns:
        The equivalent :class:`TimelineExecution`.
    """
    converted = convert_amount(
        row["fee"], row["fee_asset"], valuation_ccy, _rate_minute(row["timestamp"]), rates
    )
    fee = math.nan if converted is None else converted
    return TimelineExecution(
        instrument_public_id=row["instrument_public_id"],
        exchange=row["exchange"],
        scope_sequence=row["scope_sequence"],
        event_time=row["timestamp"],
        side=row["side"],
        size=row["size"],
        price=row["price"],
        fee=fee,
        fee_asset=row["fee_asset"],
    )


def _to_timeline_accrual(
    row: PnlTimelineAccrualRow, valuation_ccy: str, rates: FxRateMap
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

    Returns:
        The equivalent :class:`TimelineAccrual`.
    """
    converted = convert_amount(
        row["amount"], row["amount_asset"], valuation_ccy, _rate_minute(row["accrued_at"]), rates
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
    prefix. Every aggregate and per-instrument monetary field is therefore
    withheld while timestamps and contributing instrument identities remain
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


async def build_marks(
    repo: Repository,
    refs: Sequence[InstrumentSymbolRefRow],
    from_time: datetime,
    to_time: datetime,
    as_of: datetime,
    valuation_ccy: str,
) -> MarkMap:
    """Resolve the valuation-currency marks from one batched candle read.

    For every instrument whose quote currency equals ``valuation_ccy`` the 1m
    candle series covering the window participates in one timeline-specific
    repository range read. Each bar's close is keyed at ``open_at + 1min`` — the
    grid minute the bar values without look-ahead. The repository resolves each
    reference on its canonical source venue but projects the requesting
    instrument's own public id, preserving PAPER instrument identity in the mark
    map. Instruments quoted in another currency contribute no mark.

    Args:
        repo: Repository providing the batched timeline candle read.
        refs: Symbol references for the instruments the executions touch.
        from_time: Window start (its minute floor anchors the candle range).
        to_time: Window end.
        as_of: Snapshot time threading the candle read.
        valuation_ccy: Currency the marks must already be denominated in.

    Returns:
        A mapping keyed by ``(instrument_public_id, grid_minute)`` to the direct
        valuation-currency close mark for that minute.
    """
    marks: dict[tuple[str, datetime], float | None] = {}
    grid_start = from_time.replace(second=0, microsecond=0)
    candle_start = grid_start - timedelta(minutes=1)
    eligible_refs = [ref for ref in refs if ref["quote_currency"] == valuation_ccy]
    if not eligible_refs:
        return marks
    candles: Sequence[PnlTimelineCandleRow] = await repo.get_pnl_timeline_candles(
        eligible_refs,
        candle_start,
        to_time,
        as_of,
    )
    for candle in candles:
        mark_minute = candle["open_at"] + timedelta(minutes=1)
        marks[(candle["instrument_public_id"], mark_minute)] = candle["close"]
    return marks


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
) -> PnlTimelineResult:
    """Reconstruct one wallet/mode scope's Net-P&L-since-activation series.

    Reads the scope's append-only execution prefix and funding accruals, resolves
    direct marks from finalized 1m candles, and calls the pure builder with
    ``opening=None`` (no activation anchor is written in v1, so the replay starts
    from empty pools with a zero opening unrealized value). Exact-zero fees and
    accruals pass through independent of asset; nonzero foreign flows become
    ``NaN``. Before calling the pure builder, durable fill-gap evidence is
    consulted for every fill-bearing shard in the exact wallet/mode scope. A
    proven gap causes an explicit post-transform that withholds the entire built
    series.

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
    accrual_rows = await repo.get_accruals_for_pnl(wallet_public_id, mode, as_of)
    instrument_ids = list(
        dict.fromkeys(row["instrument_public_id"] for row in loaded_execution_rows)
    )
    work_instrument_ids = set(instrument_ids)
    work_instrument_ids.update(row["instrument_public_id"] for row in accrual_rows)
    _enforce_total_work_budget(from_time, to_time, len(work_instrument_ids))
    refs = await repo.get_instrument_symbol_refs(instrument_ids, as_of)
    marks = await build_marks(repo, refs, from_time, to_time, as_of, valuation_ccy)
    flow_currencies = frozenset(
        [row["fee_asset"] for row in loaded_execution_rows]
        + [row["amount_asset"] for row in accrual_rows]
    )
    pairs = required_pairs(flow_currencies, valuation_ccy)
    rate_rows = await repo.get_pnl_fx_rate_candles(
        sorted(pairs), from_time - timedelta(minutes=1), to_time, as_of
    )
    rates = build_fx_rates(rate_rows)
    executions = [
        _to_timeline_execution(row, valuation_ccy, rates) for row in loaded_execution_rows
    ]
    accruals = [_to_timeline_accrual(row, valuation_ccy, rates) for row in accrual_rows]
    window = TimelineWindow(
        from_time=from_time,
        to_time=to_time,
        granularity=granularity,
        valuation_ccy=valuation_ccy,
    )
    result = build_pnl_timeline(executions, accruals, marks, window, opening=None)
    if fill_gap:
        return _withhold_series_for_fill_gap(result)
    return result


def _fill_marker(row: PnlTimelineExecutionRow) -> PnlFillMarker:
    """Project one execution row into a fill marker."""
    return PnlFillMarker(
        marker_time=row["timestamp"],
        instrument_public_id=row["instrument_public_id"],
        side=row["side"],
        size=row["size"],
        price=row["price"],
        execution_public_id=row["public_id"],
        order_public_id=row["order_public_id"],
        status=row["status"],
    )


def _signal_marker(row: PnlTimelineSignalMarkerRow) -> PnlSignalMarker:
    """Project one signal row without inferring existence from fills alone."""
    outcome: Literal["executed", "no_fill"] = "executed" if row["has_execution"] else "no_fill"
    return PnlSignalMarker(
        marker_time=row["fired_at"],
        instrument_public_id=row["instrument_public_id"],
        side=row["side"],
        strategy_name=row["strategy_name"],
        strength=row["strength"],
        reason=row["reason"],
        price=row["price"],
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
    markers: list[PnlTimelineMarker] = [
        _fill_marker(row) for row in execution_rows if from_time <= row["timestamp"] <= to_time
    ]
    markers.extend(_signal_marker(row) for row in signal_rows)
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
