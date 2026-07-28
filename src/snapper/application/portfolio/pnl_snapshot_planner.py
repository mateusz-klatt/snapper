"""Pure tick planner for the Phase-5B persisted equity/drawdown snapshotter.

The snapshotter service owns all I/O (anchor reads, the 5A series engine,
observation reads, evidence loads, sample writes). This module is its pure,
side-effect-free brain: every decision a tick makes is a function from typed
inputs to typed plans and rows, so the whole combined-status truth table, the
chunking arithmetic, the late-fill and self-heal boundaries, the basket
authority gate and the peak/drawdown recursion are all testable without a
database.

Five decisions live here:

- **Catch-up window and chunking.** :func:`plan_catchup_window` bounds the work
  to ``[max(t0, last_sample) + 1m .. now − finalization lag]``;
  :func:`plan_catchup_chunks` splits it so each chunk stays under both the
  1440-minute cap and the engine's minute-instrument work budget.
- **Late-fill invalidation.** :func:`plan_late_fill_recompute` turns the
  engine's earliest-affected-minute boundary into a recompute-forward start when
  it lands at or before the last persisted minute.

  Correction-flow limitation (D2): 5B v1 does not detect a *corrected mark
  candle* — a finalized 1m close that a venue later revises. Persisted history is
  revisited only when the 5A engine reports a late execution fill (recompute
  forward from its earliest-affected minute) or when a bounded, retryable
  ``incomplete`` minute is re-attempted by self-heal; a silent candle restatement
  leaves an already-``complete`` sample untouched. The served P&L curve is
  nonetheless immune, because it is recomputed on read from current evidence
  rather than from these persisted rows.
- **Self-heal.** :func:`plan_self_heal_minutes` selects retryable ``incomplete``
  minutes inside the bounded lookback.
- **Peak and drawdown.** :func:`resolve_drawdown` computes one minute's drawdown
  fraction with a running peak, demoting a minute whose peak cannot price a
  drawdown to an ``incomplete`` row; only ``complete`` minutes advance the peak.
- **Per-minute assembly.** :func:`plan_chunk_samples` folds the whole combined
  gate — P&L trust tier, basket authority (A1), currency valuation (S1) and the
  A3/A5 audit envelope — into the exact rows the S2 writer accepts.

Nothing here reads a clock, a database, or the network: a tick's honesty is
decided entirely from the values the service hands in.
"""

import json
import math
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from typing import Final
from typing import Literal
from typing import cast

from snapper.application.portfolio.account_status import ACCOUNT_FRESHNESS_CEILING_S
from snapper.application.portfolio.account_status import AUTHORITY_MAX_WINDOW
from snapper.application.portfolio.basket_valuation import PositionInventoryEntry
from snapper.application.portfolio.basket_valuation import ValuationEvidence
from snapper.application.portfolio.basket_valuation import ValuationProvenance
from snapper.application.portfolio.basket_valuation import ValuationReason
from snapper.application.portfolio.basket_valuation import attribute_position_inventory
from snapper.application.portfolio.basket_valuation import value_currency
from snapper.application.portfolio.pnl_timeline import PnlIncompletenessReason
from snapper.application.portfolio.pnl_timeline import PnlTimelinePoint
from snapper.data.repository_types import PNL_SAMPLE_FINAL_REASONS
from snapper.data.repository_types import PNL_SAMPLE_MAX_DIAGNOSTIC_RECORDS
from snapper.data.repository_types import SampleReasonCode
from snapper.data.repository_types import VenueAccountObservationAttemptRow

_POINT_REASON_TO_SAMPLE_CODE: Final[dict[PnlIncompletenessReason, SampleReasonCode]] = {
    "mark_unavailable": "missing_mark",
    "fx_conversion_unproven": "missing_fx_rate",
    "cost_basis_unavailable": "cost_basis_unproven",
    "execution_price_invalid": "cost_basis_unproven",
    "unrealized_non_finite": "non_finite",
    "net_non_finite": "non_finite",
    "attribution_value_non_finite": "non_finite",
    "fill_evidence_gap": "fill_gap_evidence",
    "activation_baseline_non_finite": "non_finite",
    "seed_quantity_non_finite": "non_finite",
    "cumulative_non_finite": "non_finite",
    "scope_order_regression": "pnl_point_withheld",
    "before_activation": "pnl_point_withheld",
    "late_pre_activation_execution": "pnl_point_withheld",
    "execution_price_provenance_unproven": "pnl_point_withheld",
    "execution_size_invalid": "pnl_point_withheld",
    "attribution_reconciliation_failed": "pnl_point_withheld",
    "instrument_reconciliation_failed": "pnl_point_withheld",
}
"""Total mapping of 5A causal provenance to persisted reason codes.

Widening :data:`PnlIncompletenessReason` fails the map exhaustiveness test and is
also a breaking wire change for iOS and the frontend.
"""

_VALUATION_REASON_TO_SAMPLE_CODE: Final[dict[ValuationReason, SampleReasonCode]] = {
    "missing_fiat_rate": "missing_fx_rate",
    "missing_version": "missing_fx_rate",
    "missing_crypto_plane": "crypto_plane_unpriced",
    "no_usable_close": "crypto_plane_unpriced",
    "ambiguous_plane": "crypto_plane_ambiguous",
    "overflow": "valuation_overflow",
    "non_finite": "non_finite",
}

FINALIZATION_LAG: Final[timedelta] = timedelta(minutes=2)
"""Minutes withheld behind ``now`` so only finalized grid minutes are sampled."""

MAX_CATCHUP_CHUNK_MINUTES: Final[int] = 1440
"""Hard per-chunk minute cap (decision R5), independent of the work budget."""

SELF_HEAL_LOOKBACK: Final[timedelta] = timedelta(minutes=15)
"""How far back a retryable ``incomplete`` minute stays eligible for self-heal."""

DEFAULT_WORK_BUDGET: Final[int] = 131_040
"""Fallback minute-instrument budget; the service passes the engine's real cap."""

MARK_SOURCE: Final[str] = "finalized_1m_candle_close"
"""Mark provenance label recorded on every ``complete`` sample."""

VENUE_SCOPE: Final[str] = "spot_only"
"""The v1 coverage venue scope constant (R11); futures venues are out of scope."""

_MINUTE: Final[timedelta] = timedelta(minutes=1)
_COLLATERAL_SUFFIX: Final[str] = "_collateral_value"


@dataclass(frozen=True, slots=True)
class ChunkWindow:
    """One inclusive, minute-aligned ``[start .. end]`` grid span."""

    start: datetime
    end: datetime


@dataclass(frozen=True, slots=True)
class PlannedSample:
    """One tick's decision for a minute, minus the writer's provenance identity.

    Carries exactly the value columns the S2 writer validates; the service stamps
    ``public_id`` / ``session_id`` / ``sequence_id`` / ``timestamp`` and the fixed
    scope columns to form a ``PortfolioPnlSampleRow``. ``external_flow_adjustment``
    is always ``0.0`` (decision R10). A ``complete`` sample carries the equity
    trio, drawdown, mark provenance and a valuation+observation audit; an
    ``incomplete`` sample carries them all ``None`` and a non-empty reason list.
    """

    point_time: datetime
    valuation_status: Literal["complete", "incomplete"]
    realized_pnl: float
    fee_pnl: float
    accrual_pnl: float
    unrealized_pnl: float | None
    cash_usd: float | None
    position_value_usd: float | None
    drawdown: float | None
    mark_source: str | None
    mark_time: datetime | None
    audit_json: str


@dataclass(frozen=True, slots=True)
class SelfHealCandidate:
    """One persisted ``incomplete`` sample considered for a self-heal retry.

    ``reason_codes`` is deliberately ``frozenset[str]`` and not the canonical
    Literal: these tokens were read back off a persisted row that some other
    binary may have written, so the reader must be able to represent a code it
    does not know. Narrowing them at the read boundary is what previously turned
    an unrecognised token into the terminal ``non_finite``.
    """

    point_time: datetime
    reason_codes: frozenset[str]


@dataclass(frozen=True, slots=True)
class DrawdownOutcome:
    """Result of pricing one minute's drawdown against the running peak.

    ``drawdown`` is the finite fraction in ``[0, 1]`` when the minute prices, and
    ``None`` when ``demoted`` is ``True``. ``peak`` is the running peak carried to
    the next minute: the advanced peak on a priced minute, else the unchanged
    prior peak (an unpriceable drawdown never moves the peak).
    """

    drawdown: float | None
    peak: float | None
    demoted: bool
    reason: Literal["prior_peak_non_finite", "negative_equity"] | None


@dataclass(frozen=True, slots=True)
class _GateFailure:
    """One basket authority failure with its honest persisted cause."""

    code: SampleReasonCode
    cause: str


@dataclass(frozen=True, slots=True)
class BasketOutcome:
    """Result of the per-minute basket authority gate (A1).

    On success ``observed_balances`` is the whole authoritative basket keyed by
    ``(exchange, currency)`` and ``observations`` is one audit record per expected
    venue; on failure ``reason_codes`` is the non-empty transient/terminal reason
    set and the two evidence fields are empty.
    """

    observed_balances: dict[tuple[str, str], float]
    observations: tuple[dict[str, str], ...]
    reason_codes: frozenset[SampleReasonCode]
    diagnostics: tuple[dict[str, str], ...]


@dataclass(frozen=True, slots=True)
class EquityOutcome:
    """Result of pricing one authoritative basket into USD (S1).

    On success ``equity`` is the finite USD total and ``valuation`` the audit
    provenance records; on failure ``reason_codes`` is the non-empty reason set.
    """

    equity: float | None
    valuation: tuple[dict[str, object], ...]
    reason_codes: frozenset[SampleReasonCode]
    diagnostics: tuple[dict[str, str], ...]


def _floor_to_minute(instant: datetime) -> datetime:
    """Return the minute-aligned instant at or before ``instant``."""
    return instant.replace(second=0, microsecond=0)


def plan_catchup_window(
    now: datetime,
    last_sample_minute: datetime | None,
    t0: datetime,
    *,
    finalization_lag: timedelta = FINALIZATION_LAG,
) -> ChunkWindow | None:
    """Bound the finalized catch-up span for one scope this tick (R5).

    The window opens one minute after the later of the anchor ``t0`` and the last
    persisted sample, and closes at the floored ``now`` minus the finalization
    lag so no still-provisional minute is ever sampled.

    Args:
        now: Current instant (the service's clock reading).
        last_sample_minute: The last persisted sample minute, or ``None`` when the
            epoch has no sample yet.
        t0: The activation anchor instant.
        finalization_lag: Minutes withheld behind ``now``.

    Returns:
        The inclusive catch-up window, or ``None`` when nothing is finalized past
        the last persisted minute.
    """
    resume_from = t0 if last_sample_minute is None else max(t0, last_sample_minute)
    start = resume_from + _MINUTE
    end = _floor_to_minute(now) - finalization_lag
    if start > end:
        return None
    return ChunkWindow(start=start, end=end)


def _chunk_span_minutes(pool_key_count: int, max_chunk_minutes: int, work_budget: int) -> int:
    """Return the largest chunk length honouring the cap and the work budget."""
    budget_minutes = work_budget // max(1, pool_key_count)
    return max(1, min(max_chunk_minutes, budget_minutes))


def plan_catchup_chunks(
    window: ChunkWindow,
    pool_key_count: int,
    *,
    max_chunk_minutes: int = MAX_CATCHUP_CHUNK_MINUTES,
    work_budget: int = DEFAULT_WORK_BUDGET,
) -> tuple[ChunkWindow, ...]:
    """Split one catch-up window into budget-bounded minute chunks (R5).

    Each chunk spans at most ``max_chunk_minutes`` and keeps
    ``minutes × pool_key_count`` at or below ``work_budget`` so the engine's work
    budget is never tripped. Every requested minute lands in exactly one chunk.

    Args:
        window: The inclusive catch-up window to split.
        pool_key_count: Distinct instrument-shard pools valued each minute.
        max_chunk_minutes: The hard per-chunk minute cap.
        work_budget: The engine's minute-instrument work budget.

    Returns:
        The ordered, gap-free tuple of chunk windows covering ``window``.
    """
    span = _chunk_span_minutes(pool_key_count, max_chunk_minutes, work_budget)
    chunks: list[ChunkWindow] = []
    cursor = window.start
    while cursor <= window.end:
        chunk_end = min(cursor + (span - 1) * _MINUTE, window.end)
        chunks.append(ChunkWindow(start=cursor, end=chunk_end))
        cursor = chunk_end + _MINUTE
    return tuple(chunks)


def plan_late_fill_recompute(
    earliest_affected_minute: datetime | None,
    last_persisted_minute: datetime | None,
) -> datetime | None:
    """Return the recompute-forward start when a late fill invalidates history.

    A fill whose clamped effective minute lands at or before the last persisted
    minute (decision D9/R3) means the persisted samples from that minute forward
    are stale and must be recomputed and superseded.

    Args:
        earliest_affected_minute: The engine's earliest post-clamp affected
            minute among executions above the caller's baseline watermark.
        last_persisted_minute: The last persisted sample minute, or ``None``.

    Returns:
        The earliest minute to recompute and supersede, or ``None`` when no late
        fill lands inside the already-persisted range.
    """
    if earliest_affected_minute is None or last_persisted_minute is None:
        return None
    if earliest_affected_minute > last_persisted_minute:
        return None
    return earliest_affected_minute


def plan_self_heal_minutes(
    candidates: Sequence[SelfHealCandidate],
    now: datetime,
    *,
    lookback: timedelta = SELF_HEAL_LOOKBACK,
) -> tuple[datetime, ...]:
    """Select retryable ``incomplete`` minutes still inside the self-heal window.

    A minute is eligible when it carries at least one reason code, none of them a
    known terminal one (decision R9), and it lies within ``lookback`` of the
    floored ``now``; a minute carrying any terminal reason, a minute carrying no
    code at all, or one older than the lookback is honest and left untouched.

    The terminal test is a **deny-list** — ``isdisjoint(PNL_SAMPLE_FINAL_REASONS)``
    — not the equivalent-looking allow-list ``codes <= RETRYABLE``. For every
    canonical code the two agree exactly, because the retryable and final sets are
    disjoint and their union is the whole canonical set. They differ only for a
    token this binary does not recognise, which means the row was written by a
    newer binary, and there the deny-list deliberately keeps the minute eligible:
    treating an unknown code as terminal is a permanent verdict passed on evidence
    this process cannot read, whereas treating it as retryable costs at most the
    bounded lookback of re-attempts. The corresponding obligation on the writer —
    deploy a new FINAL code's deny-list entry before emitting it — is recorded on
    :data:`snapper.data.repository_types.SampleReasonCode`.

    Args:
        candidates: Persisted ``incomplete`` samples with their reason codes.
        now: Current instant.
        lookback: How far back a retryable minute stays eligible.

    Returns:
        The ascending tuple of minutes to re-attempt this tick.
    """
    cutoff = _floor_to_minute(now) - lookback
    eligible = [
        candidate.point_time
        for candidate in candidates
        if candidate.point_time >= cutoff
        and bool(candidate.reason_codes)
        and candidate.reason_codes.isdisjoint(PNL_SAMPLE_FINAL_REASONS)
    ]
    return tuple(sorted(eligible))


def resolve_drawdown(prior_peak: float | None, equity: float) -> DrawdownOutcome:
    """Compute one minute's drawdown fraction against the running peak (D8).

    The running peak is the maximum of the prior peak and this minute's equity;
    the drawdown is ``(peak − equity) / peak`` guarded to ``[0, 1]``. A
    non-finite prior peak, or a peak that cannot price a finite drawdown (a
    non-positive peak paired with non-zero equity, or a negative-equity minute
    driven past the ``[0, 1]`` band), demotes the minute: the S2 validator
    requires a valid drawdown on a ``complete`` row, so an unpriceable one makes
    the minute ``incomplete`` with reason ``non_finite`` rather than fabricating a
    number. A zero-equity minute at a zero peak (an empty portfolio at rest) is a
    real ``0.0`` drawdown. A demoted minute never advances the peak.

    Args:
        prior_peak: The running peak from prior complete samples, or ``None``.
        equity: This minute's finite USD equity.

    Returns:
        The drawdown, the carried peak, and whether the minute was demoted.
    """
    if prior_peak is not None and not math.isfinite(prior_peak):
        return DrawdownOutcome(
            drawdown=None, peak=prior_peak, demoted=True, reason="prior_peak_non_finite"
        )
    running_peak = equity if prior_peak is None else max(prior_peak, equity)
    if running_peak > 0.0:
        drawdown = (running_peak - equity) / running_peak
        if 0.0 <= drawdown <= 1.0:
            return DrawdownOutcome(drawdown=drawdown, peak=running_peak, demoted=False, reason=None)
        return DrawdownOutcome(
            drawdown=None, peak=prior_peak, demoted=True, reason="negative_equity"
        )
    if abs(equity) <= 0.0:
        return DrawdownOutcome(drawdown=0.0, peak=running_peak, demoted=False, reason=None)
    return DrawdownOutcome(drawdown=None, peak=prior_peak, demoted=True, reason="negative_equity")


def _effective_until(observed_at: datetime) -> datetime:
    """Reconstruct a basket observation's authority end from shared constants."""
    freshness = observed_at + timedelta(seconds=ACCOUNT_FRESHNESS_CEILING_S)
    return min(freshness, observed_at + AUTHORITY_MAX_WINDOW)


def _parse_balance_entries(
    balances_json: str,
) -> tuple[tuple[str, float], ...] | _GateFailure:
    """Parse one venue's balances into ``(currency, total)`` legs or a reason.

    A structurally unpriceable payload — invalid JSON, a non-array shape, a
    malformed entry, a synthetic ``*_collateral_value`` currency (never valid in a
    spot basket) or a non-finite total — fails closed with the terminal
    ``non_finite`` reason rather than guessing a balance.

    Args:
        balances_json: The observation attempt's raw balances array.

    Returns:
        The parsed legs, or the ``non_finite`` reason on any structural fault.
    """
    try:
        payload = json.loads(balances_json)
    except (ValueError, TypeError):
        return _GateFailure("basket_payload_invalid", "balances_json_unparseable")
    if not isinstance(payload, list):
        return _GateFailure("basket_payload_invalid", "balances_payload_not_a_list")
    legs: list[tuple[str, float]] = []
    for entry in payload:
        if not isinstance(entry, dict):
            return _GateFailure("basket_payload_invalid", "balance_entry_not_an_object")
        currency = entry.get("currency")
        total = entry.get("total")
        if not isinstance(currency, str) or not currency or currency.endswith(_COLLATERAL_SUFFIX):
            return _GateFailure("basket_payload_invalid", "balance_entry_currency_invalid")
        if (
            isinstance(total, bool)
            or not isinstance(total, (int, float))
            or not math.isfinite(total)
        ):
            return _GateFailure("basket_payload_invalid", "balance_entry_total_non_finite")
        legs.append((currency, float(total)))
    return tuple(legs)


def _gate_attempt(
    minute: datetime, attempt: VenueAccountObservationAttemptRow
) -> tuple[tuple[str, float], ...] | _GateFailure:
    """Authority-gate one venue's latest attempt as-of a minute (A1).

    The attempt must itself be a fresh ``observed`` reading whose balance was seen
    at or before the minute and whose reconstructed authority window still covers
    the minute; a not-yet-observed or stale attempt is the transient
    ``basket_stale``, a balance observed AFTER the minute (a clock anomaly, since
    the read already cut on bus time) is the terminal ``future_clock`` (D1), and a
    structurally broken payload is terminal ``non_finite``.

    Args:
        minute: The grid instant being valued.
        attempt: The single latest attempt known by the minute.

    Returns:
        The parsed balance legs, or the reason the basket is unauthoritative.
    """
    if attempt["balance_status"] != "observed":
        return _GateFailure("basket_stale", "balance_not_observed")
    observed_at = attempt["balance_observed_at"]
    if observed_at is None:
        return _GateFailure("basket_stale", "balance_observed_at_missing")
    if observed_at > minute:
        return _GateFailure("future_clock", "balance_observed_after_minute")
    if minute >= _effective_until(observed_at):
        return _GateFailure("basket_stale", "authority_window_expired")
    balances_json = attempt["balances_json"]
    if balances_json is None:
        return _GateFailure("basket_payload_invalid", "balances_json_missing")
    return _parse_balance_entries(balances_json)


def _observation_record(attempt: VenueAccountObservationAttemptRow) -> dict[str, str]:
    """Project one authoritative attempt into an A3 audit observation record."""
    observed_at = attempt["balance_observed_at"]
    return {
        "observation_public_id": attempt["public_id"],
        "exchange": attempt["exchange"],
        "balance_observed_at": observed_at.isoformat() if observed_at is not None else "",
    }


def evaluate_basket(
    minute: datetime,
    expected_venues: frozenset[str],
    attempts: Mapping[str, VenueAccountObservationAttemptRow],
) -> BasketOutcome:
    """Gate the whole expected spot basket at one minute (D5/A1).

    Every expected venue must contribute an authoritative ``observed`` attempt;
    otherwise the minute fails closed with the union of each venue's transient or
    terminal reason. A missing venue is ``basket_missing_venue``.

    Args:
        minute: The grid instant being valued.
        expected_venues: The caller-resolved spot venue set (denominator).
        attempts: The latest attempt per exchange known by the minute.

    Returns:
        The authoritative basket and its observation records, or the reason set.
    """
    observations: list[dict[str, str]] = []
    diagnostics: list[dict[str, str]] = []
    reasons: set[SampleReasonCode] = set()
    resolved: dict[tuple[str, str], float] = {}
    for exchange in sorted(expected_venues):
        attempt = attempts.get(exchange)
        if attempt is None:
            reasons.add("basket_missing_venue")
            diagnostics.append(
                _diagnostic_record(
                    "basket_gate",
                    "venue_attempt_missing",
                    "basket_missing_venue",
                    {"exchange": exchange},
                )
            )
            continue
        gated = _gate_attempt(minute, attempt)
        if isinstance(gated, _GateFailure):
            reasons.add(gated.code)
            diagnostics.append(
                _diagnostic_record("basket_gate", gated.cause, gated.code, {"exchange": exchange})
            )
            continue
        for currency, total in gated:
            key = (exchange, currency)
            resolved[key] = resolved.get(key, 0.0) + total
        observations.append(_observation_record(attempt))
    if reasons:
        return BasketOutcome(
            observed_balances={},
            observations=(),
            reason_codes=frozenset(reasons),
            diagnostics=_bounded_diagnostics(diagnostics),
        )
    return BasketOutcome(
        observed_balances=resolved,
        observations=tuple(observations),
        reason_codes=frozenset(),
        diagnostics=(),
    )


def _map_valuation_reason(reason: ValuationReason | None) -> SampleReasonCode:
    """Map valuation withholding without ever stalling the wallet snapshotter.

    This boundary must not raise. An exception escapes to ``_tick_once``'s
    per-wallet catch, writes nothing for the tick, and repeat logging is
    suppressed for the failure streak. A neutral fallback is therefore louder
    in persisted evidence than nominally failing loud.
    """
    return (
        "basket_leg_withheld"
        if reason is None
        else _VALUATION_REASON_TO_SAMPLE_CODE.get(reason, "basket_leg_withheld")
    )


def _orientation(currency: str, provenance: ValuationProvenance) -> str:
    """Return the explicit conversion orientation of one priced leg (A3)."""
    if provenance.kind == "identity":
        return "identity"
    if provenance.kind == "crypto_candle":
        return "crypto"
    return "direct" if provenance.base == currency else "inverse"


def _valuation_record(currency: str, provenance: ValuationProvenance) -> dict[str, object]:
    """Project one priced leg's provenance into an A3/A5 audit valuation record.

    Records the CONSUMED basket ``currency``, the explicit ``orientation`` and,
    for a priced (fiat/crypto) leg, the full candle version identity — instrument,
    symbol, bar open and version — so a later candle correction is attributable to
    the exact input the sample consumed.
    """
    record: dict[str, object] = {
        "kind": provenance.kind,
        "orientation": _orientation(currency, provenance),
        "currency": currency,
        "base": provenance.base,
        "quote": provenance.quote,
        "exchange": provenance.exchange,
        "rate": provenance.rate,
        "close": provenance.close,
    }
    candle = provenance.candle
    if candle is not None:
        record["candle"] = {
            "candle_id": candle.candle_id,
            "candle_public_id": candle.candle_public_id,
            "candle_timestamp": candle.candle_timestamp.isoformat(),
            "candle_open_at": candle.candle_open_at.isoformat(),
            "instrument_public_id": candle.instrument_public_id,
            "native_symbol": candle.native_symbol,
        }
    return record


def _fsum_guarded(values: Sequence[float]) -> float | None:
    """Aggregate USD legs with one order-insensitive, overflow-guarded sum (P2).

    The single aggregation both the equity total and the position total use, so
    identical multisets of bit-identical legs (a fully-attributed basket) produce
    identical totals and ``cash = equity − position`` is exactly zero. An
    ``OverflowError`` or a non-finite result signals overflow (``None``) so the
    caller demotes the minute rather than fabricating a partial number.

    Args:
        values: The per-leg USD contributions to aggregate.

    Returns:
        The finite USD total, or ``None`` on overflow.
    """
    try:
        total = math.fsum(values)
    except OverflowError:
        return None
    return total if math.isfinite(total) else None


def value_basket(
    observed_balances: Mapping[tuple[str, str], float],
    minute: datetime,
    evidence: ValuationEvidence,
) -> EquityOutcome:
    """Price one authoritative basket into a single USD equity total (S1/D2/P2).

    Each ``(exchange, currency)`` leg is valued off the same finalized evidence
    the marks use; any withheld leg fails the minute closed with a mapped reason.
    The per-leg USD floats are aggregated with the shared :func:`_fsum_guarded`
    (the same aggregation the partition uses), and an overflow is terminal
    ``non_finite``.

    Args:
        observed_balances: The authoritative basket keyed by ``(exchange, currency)``.
        minute: The grid instant selecting the evidence.
        evidence: Pre-loaded fiat and crypto price evidence for the minute.

    Returns:
        The USD equity with audit provenance, or the reason set.
    """
    legs: list[float] = []
    provenances: list[tuple[str, ValuationProvenance]] = []
    reasons: set[SampleReasonCode] = set()
    diagnostics: list[dict[str, str]] = []
    for (exchange, currency), qty in sorted(observed_balances.items()):
        leg = value_currency(exchange, currency, qty, minute, evidence)
        if leg.usd_value is None:
            code = _map_valuation_reason(leg.reason)
            reasons.add(code)
            diagnostics.append(
                _diagnostic_record(
                    "valuation",
                    leg.reason or "unmapped",
                    code,
                    {"exchange": exchange, "currency": currency},
                )
            )
            continue
        legs.append(leg.usd_value)
        if leg.provenance is not None:
            provenances.append((currency, leg.provenance))
    if reasons:
        return EquityOutcome(
            equity=None,
            valuation=(),
            reason_codes=frozenset(reasons),
            diagnostics=_bounded_diagnostics(diagnostics),
        )
    total = _fsum_guarded(legs)
    if total is None:
        return EquityOutcome(
            equity=None,
            valuation=(),
            reason_codes=frozenset({"valuation_overflow"}),
            diagnostics=(
                _diagnostic_record("equity_aggregation", "overflow", "valuation_overflow"),
            ),
        )
    return EquityOutcome(
        equity=total,
        valuation=_dedupe_valuation_records(provenances),
        reason_codes=frozenset(),
        diagnostics=(),
    )


def _dedupe_valuation_records(
    provenances: Sequence[tuple[str, ValuationProvenance]],
) -> tuple[dict[str, object], ...]:
    """Return one audit valuation record per distinct priced leg, deterministically."""
    by_key: dict[str, dict[str, object]] = {}
    for currency, provenance in provenances:
        record = _valuation_record(currency, provenance)
        by_key[_canonical_json(record)] = record
    return tuple(by_key[key] for key in sorted(by_key))


@dataclass(frozen=True, slots=True)
class _PartitionOutcome:
    """The position-labelled USD value and its coverage-exclusion flags.

    ``position_value`` is ``None`` when the shared aggregation overflowed (P2), so
    the caller demotes the minute rather than fabricating a partition.
    """

    position_value: float | None
    leveraged_excluded: bool
    non_finite_excluded: bool


def _partition_position(
    observed_balances: Mapping[tuple[str, str], float],
    positions: Sequence[PositionInventoryEntry],
    minute: datetime,
    evidence: ValuationEvidence,
) -> _PartitionOutcome:
    """Value the position-labelled inventory and surface exclusion flags (R6/A2/A4/N2).

    Reuses :func:`attribute_position_inventory` for the per-``(exchange,
    currency)`` single-cap partition and prices each attributed quantity through
    the SAME S1 orientation-aware :func:`value_currency` path the observed leg
    used — identical per-unit arithmetic, so a fully-attributed inverse-fiat leg's
    position value is bit-identical to that leg's equity contribution and
    ``cash = equity − position_value`` never fabricates a residual (N2). A positive
    ``attributed_qty`` implies an already-priced positive balance, so its leg
    prices; the legs are aggregated with the SAME :func:`_fsum_guarded` the equity
    uses (P2), so an overflow yields a ``None`` value (the minute demotes) rather
    than a partial number. The leveraged and non-finite exclusion flags are
    surfaced for the audit coverage disclosure.

    Args:
        observed_balances: The authoritative basket keyed by ``(exchange, currency)``.
        positions: Active spot positions used only to label the partition.
        minute: The grid instant selecting the evidence.
        evidence: Pre-loaded fiat and crypto price evidence for the minute.

    Returns:
        The position value (``None`` on overflow) and the two exclusion flags.
    """
    attribution = attribute_position_inventory(observed_balances, positions)
    legs = [
        cast(
            float,
            value_currency(
                entry.exchange, entry.currency, entry.attributed_qty, minute, evidence
            ).usd_value,
        )
        for entry in attribution.attributions
        if entry.attributed_qty > 0.0
    ]
    return _PartitionOutcome(
        position_value=_fsum_guarded(legs),
        leveraged_excluded=attribution.leveraged_inventory_excluded,
        non_finite_excluded=attribution.non_finite_position_excluded,
    )


def _coverage(leveraged_excluded: bool, non_finite_excluded: bool) -> dict[str, object]:
    """Build the self-describing coverage disclosure block (A4/R10/R11)."""
    return {
        "leveraged_inventory_excluded": leveraged_excluded,
        "non_finite_position_excluded": non_finite_excluded,
        "venue_scope": VENUE_SCOPE,
        "external_flows_adjusted": False,
    }


def _canonical_json(payload: object) -> str:
    """Serialize an audit payload deterministically and finitely."""
    return json.dumps(payload, allow_nan=False, separators=(",", ":"), sort_keys=True)


def _diagnostic_record(
    stage: str,
    cause: str,
    code: SampleReasonCode,
    identity: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Build one diagnostic record with optional leg identity."""
    record = {"stage": stage, "cause": cause, "reason_code": code}
    if identity is not None:
        record.update(identity)
    return record


def _bounded_diagnostics(
    records: Sequence[dict[str, str]],
) -> tuple[dict[str, str], ...]:
    """Dedupe and deterministically cap diagnostics while preserving every code."""
    unique = {_canonical_json(record): record for record in records}
    ordered_keys = sorted(unique)
    first_by_code: dict[str, str] = {}
    for key in ordered_keys:
        first_by_code.setdefault(unique[key]["reason_code"], key)
    selected = set(first_by_code.values())
    for key in ordered_keys:
        if len(selected) >= PNL_SAMPLE_MAX_DIAGNOSTIC_RECORDS:
            break
        selected.add(key)
    return tuple(unique[key] for key in sorted(selected))


def _complete_audit_json(
    valuation: Sequence[dict[str, object]],
    observations: Sequence[dict[str, str]],
    coverage: dict[str, object],
) -> str:
    """Build one ``complete`` sample's valuation+observation+coverage envelope."""
    return _canonical_json(
        {"coverage": coverage, "observations": list(observations), "valuation": list(valuation)}
    )


def _incomplete_audit_json(
    reason_codes: frozenset[SampleReasonCode],
    diagnostics: Sequence[dict[str, str]],
    coverage: dict[str, object],
) -> str:
    """Build one ``incomplete`` sample's reason-coded, coverage-disclosed envelope."""
    return _canonical_json(
        {
            "coverage": coverage,
            "diagnostics": list(diagnostics),
            "observations": [],
            "reason_codes": sorted(reason_codes),
            "valuation": [],
        }
    )


def _incomplete_sample(
    point: PnlTimelinePoint,
    reasons: frozenset[SampleReasonCode],
    diagnostics: Sequence[dict[str, str]],
    coverage: dict[str, object],
) -> PlannedSample:
    """Assemble one ``incomplete`` sample carrying trusted cumulatives only.

    Only ever called for a P&L point whose cumulatives are trusted (its
    ``realized_pnl`` is not ``None``); the cumulatives are all-or-nothing, so the
    cast to ``float`` is sound. The equity plane is withheld, so the coverage
    exclusion flags default false (no partition ran).
    """
    return PlannedSample(
        point_time=point.point_time,
        valuation_status="incomplete",
        realized_pnl=cast(float, point.realized_pnl),
        fee_pnl=cast(float, point.fee_pnl),
        accrual_pnl=cast(float, point.accrual_pnl),
        unrealized_pnl=None,
        cash_usd=None,
        position_value_usd=None,
        drawdown=None,
        mark_source=None,
        mark_time=None,
        audit_json=_incomplete_audit_json(reasons, diagnostics, coverage),
    )


def _complete_sample(
    point: PnlTimelinePoint,
    cash: float,
    position_value: float,
    drawdown: float,
    audit_json: str,
) -> PlannedSample:
    """Assemble one ``complete`` sample with equity, drawdown and full audit.

    Only ever called for a mark-complete point, so every cumulative including
    ``unrealized_pnl`` is present and the casts to ``float`` are sound.
    """
    return PlannedSample(
        point_time=point.point_time,
        valuation_status="complete",
        realized_pnl=cast(float, point.realized_pnl),
        fee_pnl=cast(float, point.fee_pnl),
        accrual_pnl=cast(float, point.accrual_pnl),
        unrealized_pnl=cast(float, point.unrealized_pnl),
        cash_usd=cash,
        position_value_usd=position_value,
        drawdown=drawdown,
        mark_source=MARK_SOURCE,
        mark_time=point.point_time,
        audit_json=audit_json,
    )


@dataclass(frozen=True, slots=True)
class PositionVersion:
    """One SCD2 position version with its ``[valid_from, valid_to)`` interval (A2)."""

    entry: PositionInventoryEntry
    valid_from: datetime
    valid_to: datetime


def positions_at(
    versions: Sequence[PositionVersion], minute: datetime
) -> tuple[PositionInventoryEntry, ...]:
    """Return the position inventory active at one grid minute (A2 causality).

    Picks every version whose knowledge interval covers ``minute``
    (``valid_from <= minute < valid_to``), so a position opened after an earlier
    minute never labels that minute's partition.

    Args:
        versions: The chunk's temporal position versions.
        minute: The grid instant being valued.

    Returns:
        The inventory entries active at the minute, in input order.
    """
    return tuple(
        version.entry for version in versions if version.valid_from <= minute < version.valid_to
    )


@dataclass(frozen=True, slots=True)
class MinuteInputs:
    """Everything the pure assembler needs to decide one minute."""

    point: PnlTimelinePoint
    attempts: Mapping[str, VenueAccountObservationAttemptRow]
    evidence: ValuationEvidence


@dataclass(frozen=True, slots=True)
class MinutePlan:
    """The decision for one minute: an optional row and the carried peak."""

    sample: PlannedSample | None
    peak: float | None


def _mark_incomplete_reason_codes(point: PnlTimelinePoint) -> set[SampleReasonCode]:
    """Translate a mark-incomplete point's exact 5A provenance to R9 codes (D1).

    Each causal reason the 5A engine stamped on the withheld point is mapped
    through ``_POINT_REASON_TO_SAMPLE_CODE`` so an FX-conversion failure persists
    as ``missing_fx_rate`` and a non-finite value as ``non_finite`` rather than all
    collapsing to ``missing_mark``. The 5A engine guarantees an incomplete point
    carries at least one reason, so the mapped set is always non-empty.

    Args:
        point: The mark-incomplete P&L point carrying its causal reason entries.

    Returns:
        The distinct persisted reason codes implied by the point's provenance.
    """
    return {
        _POINT_REASON_TO_SAMPLE_CODE.get(entry.reason, "pnl_point_withheld")
        for entry in point.incompleteness_reasons
    }


def _mark_incomplete_diagnostics(
    point: PnlTimelinePoint,
) -> tuple[dict[str, str], ...]:
    """Explain every 5A withholding cause and identify its triggering instrument."""
    records: list[dict[str, str]] = []
    for entry in point.incompleteness_reasons:
        code = _POINT_REASON_TO_SAMPLE_CODE.get(entry.reason, "pnl_point_withheld")
        identity = (
            {"instrument_public_id": entry.trigger_instrument_public_id}
            if entry.trigger_instrument_public_id is not None
            else None
        )
        records.append(_diagnostic_record("pnl_point", entry.reason, code, identity))
    return _bounded_diagnostics(records)


def assemble_minute_sample(
    inputs: MinuteInputs,
    expected_venues: frozenset[str],
    position_versions: Sequence[PositionVersion],
    prior_peak: float | None,
) -> MinutePlan:
    """Decide one minute's sample from the combined R1 truth table.

    An untrusted P&L point (its cumulatives withheld) writes NO row — a NOT NULL
    cumulative cannot be honest. Otherwise the trusted cumulatives always persist;
    the minute is ``complete`` only when the P&L point is mark-complete AND the
    whole basket is authoritative AND it prices AND its drawdown resolves, and is
    ``incomplete`` (equity/mark/drawdown NULL, reason-coded) on any other outcome.
    Only a ``complete`` minute advances the peak. The partition uses the inventory
    active AT this minute (A2), never the tick's current holdings.

    Args:
        inputs: The minute's P&L point, observation attempts and price evidence.
        expected_venues: The caller-resolved spot venue set.
        position_versions: Temporal position versions labelling the partition.
        prior_peak: The running peak from prior complete samples, or ``None``.

    Returns:
        The planned row (or ``None`` for an untrusted minute) and carried peak.
    """
    point = inputs.point
    if point.realized_pnl is None:
        return MinutePlan(sample=None, peak=prior_peak)
    basket = evaluate_basket(point.point_time, expected_venues, inputs.attempts)
    pnl_complete = point.valuation_status == "complete"
    if not pnl_complete or basket.reason_codes:
        reasons = set(basket.reason_codes)
        diagnostics = list(basket.diagnostics)
        if not pnl_complete:
            reasons |= _mark_incomplete_reason_codes(point)
            diagnostics.extend(_mark_incomplete_diagnostics(point))
        return MinutePlan(
            sample=_incomplete_sample(
                point,
                frozenset(reasons),
                _bounded_diagnostics(diagnostics),
                _coverage(False, False),
            ),
            peak=prior_peak,
        )
    positions = positions_at(position_versions, point.point_time)
    return _assemble_complete_minute(inputs, basket, positions, prior_peak)


def _assemble_complete_minute(
    inputs: MinuteInputs,
    basket: BasketOutcome,
    positions: Sequence[PositionInventoryEntry],
    prior_peak: float | None,
) -> MinutePlan:
    """Price, partition and draw-down a mark-complete, authoritative minute."""
    point = inputs.point
    equity_outcome = value_basket(basket.observed_balances, point.point_time, inputs.evidence)
    if equity_outcome.equity is None:
        return MinutePlan(
            sample=_incomplete_sample(
                point,
                equity_outcome.reason_codes,
                equity_outcome.diagnostics,
                _coverage(False, False),
            ),
            peak=prior_peak,
        )
    partition = _partition_position(
        basket.observed_balances, positions, point.point_time, inputs.evidence
    )
    if partition.position_value is None:
        return MinutePlan(
            sample=_incomplete_sample(
                point,
                frozenset({"valuation_overflow"}),
                (_diagnostic_record("position_partition", "overflow", "valuation_overflow"),),
                _coverage(partition.leveraged_excluded, partition.non_finite_excluded),
            ),
            peak=prior_peak,
        )
    drawdown = resolve_drawdown(prior_peak, equity_outcome.equity)
    if drawdown.demoted or drawdown.drawdown is None:
        code: SampleReasonCode = (
            "non_finite" if drawdown.reason == "prior_peak_non_finite" else "drawdown_unpriceable"
        )
        return MinutePlan(
            sample=_incomplete_sample(
                point,
                frozenset({code}),
                (_diagnostic_record("drawdown", drawdown.reason or "negative_equity", code),),
                _coverage(partition.leveraged_excluded, partition.non_finite_excluded),
            ),
            peak=prior_peak,
        )
    cash = equity_outcome.equity - partition.position_value
    coverage = _coverage(partition.leveraged_excluded, partition.non_finite_excluded)
    audit_json = _complete_audit_json(equity_outcome.valuation, basket.observations, coverage)
    return MinutePlan(
        sample=_complete_sample(
            point, cash, partition.position_value, drawdown.drawdown, audit_json
        ),
        peak=drawdown.peak,
    )


@dataclass(frozen=True, slots=True)
class ChunkPlan:
    """One chunk's ordered planned rows and the peak carried past it."""

    samples: tuple[PlannedSample, ...]
    peak: float | None


def plan_chunk_samples(
    minute_inputs: Sequence[MinuteInputs],
    expected_venues: frozenset[str],
    position_versions: Sequence[PositionVersion],
    prior_peak: float | None,
) -> ChunkPlan:
    """Fold a chunk of minutes into planned rows, threading the running peak.

    Minutes are decided in order so the peak advances only across ``complete``
    minutes; untrusted minutes contribute no row. The seed ``prior_peak`` is the
    CAUSAL peak strictly before the chunk start, so a recompute never borrows a
    later minute's equity (A1).

    Args:
        minute_inputs: The chunk's per-minute inputs in ascending minute order.
        expected_venues: The caller-resolved spot venue set.
        position_versions: Temporal position versions labelling the partition.
        prior_peak: The causal peak from complete samples before the chunk start.

    Returns:
        The ordered planned rows and the peak carried past the chunk.
    """
    samples: list[PlannedSample] = []
    peak = prior_peak
    for inputs in minute_inputs:
        plan = assemble_minute_sample(inputs, expected_venues, position_versions, peak)
        peak = plan.peak
        if plan.sample is not None:
            samples.append(plan.sample)
    return ChunkPlan(samples=tuple(samples), peak=peak)
