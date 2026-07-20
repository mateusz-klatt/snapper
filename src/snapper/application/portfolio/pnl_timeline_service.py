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
from datetime import datetime
from datetime import timedelta
from typing import Final

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
from snapper.data.repository_types import PnlTimelineAccrualRow
from snapper.data.repository_types import PnlTimelineCandleRow
from snapper.data.repository_types import PnlTimelineExecutionRow

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


class PnlTimelineWorkBudgetError(ValueError):
    """The requested raw grid and instrument fan-out exceed the work budget."""


def _to_timeline_execution(row: PnlTimelineExecutionRow, valuation_ccy: str) -> TimelineExecution:
    """Map a repository execution row onto the pure builder's input.

    Exact zero passes through regardless of asset because zero is
    currency-invariant. A nonzero fee passes through only when it is denominated
    in ``valuation_ccy``. A nonzero fee in another asset is UNKNOWN (its
    conversion is the deferred FX work), so it is passed as ``NaN`` and the
    builder withholds the point. ``event_time`` is the row's ``timestamp`` (the
    time axis), never the nullable ``executed_at``.

    Args:
        row: One ``get_pnl_timeline_executions`` row.
        valuation_ccy: Currency the fee must already be denominated in to count.

    Returns:
        The equivalent :class:`TimelineExecution`.
    """
    fee = row["fee"] if row["fee"] == 0.0 or row["fee_asset"] == valuation_ccy else math.nan
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


def _to_timeline_accrual(row: PnlTimelineAccrualRow, valuation_ccy: str) -> TimelineAccrual:
    """Map a repository accrual row onto the pure builder's input.

    Exact zero passes through regardless of asset because it needs no currency
    conversion. A nonzero amount in another asset is UNKNOWN and maps to
    ``NaN``, allowing the builder to withhold the affected cumulatives.

    Args:
        row: One ``get_accruals_for_pnl`` row.
        valuation_ccy: Currency a nonzero amount must already use.

    Returns:
        The equivalent :class:`TimelineAccrual`.
    """
    amount = (
        row["amount"] if row["amount"] == 0.0 or row["amount_asset"] == valuation_ccy else math.nan
    )
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

    The exact full-wallet and mode lookup includes venue-only shards with zero
    consumed executions, which cannot be discovered from the execution prefix.
    Each returned key is then evaluated by the existing recorded-quantity versus
    consumed-executions evidence read.

    Args:
        repo: Repository providing scoped shard keys and gap evidence.
        wallet_public_id: Full wallet scope.
        mode: Trading mode scope.
        as_of: Temporal anchor for consumed execution evidence.

    Returns:
        ``True`` as soon as any scoped shard has a proven fill gap.
    """
    shard_keys = await repo.get_fill_shard_keys_for_scope(wallet_public_id, mode)
    for shard_key in shard_keys:
        if await repo.shard_has_fill_gap(shard_key, as_of):
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
        as_of: Snapshot time threading the accrual and candle reads.
        valuation_ccy: Currency the series components are expressed in.

    Returns:
        The built :class:`PnlTimelineResult` at the requested granularity.

    Raises:
        PnlTimelineWorkBudgetError: When raw grid minute-instrument work is
            above :data:`PNL_TIMELINE_MAX_WORK_UNITS`.
        ValueError: When ``granularity`` is not a supported value (surfaced by
            the pure builder).
    """
    fill_gap = await _scope_has_fill_gap(repo, wallet_public_id, mode, as_of)
    execution_rows = await repo.get_pnl_timeline_executions(wallet_public_id, mode, as_of)
    accrual_rows = await repo.get_accruals_for_pnl(wallet_public_id, mode, as_of)
    instrument_ids = list(dict.fromkeys(row["instrument_public_id"] for row in execution_rows))
    work_instrument_ids = set(instrument_ids)
    work_instrument_ids.update(row["instrument_public_id"] for row in accrual_rows)
    _enforce_total_work_budget(from_time, to_time, len(work_instrument_ids))
    refs = await repo.get_instrument_symbol_refs(instrument_ids, as_of)
    marks = await build_marks(repo, refs, from_time, to_time, as_of, valuation_ccy)
    executions = [_to_timeline_execution(row, valuation_ccy) for row in execution_rows]
    accruals = [_to_timeline_accrual(row, valuation_ccy) for row in accrual_rows]
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
