"""Plan a bounded P&L reconciliation using the exact persisted sample versions."""

from collections.abc import Callable
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Final

from snapper.application.portfolio.pnl_snapshot_planner import MinuteInputs
from snapper.application.portfolio.pnl_snapshot_planner import PlannedSample
from snapper.application.portfolio.pnl_snapshot_planner import PositionVersion
from snapper.application.portfolio.pnl_snapshot_planner import WithheldMinute
from snapper.application.portfolio.pnl_snapshot_planner import _withheld_reason_codes
from snapper.application.portfolio.pnl_snapshot_planner import _withheld_reasons
from snapper.application.portfolio.pnl_snapshot_planner import assemble_minute_sample
from snapper.application.portfolio.pnl_timeline import PnlIncompletenessReason
from snapper.data.pnl_correction_state import MAX_CORRECTION_CHUNK_MINUTES
from snapper.data.pnl_correction_state import PnlCorrectionChunk
from snapper.data.pnl_correction_state import PnlCorrectionEdit
from snapper.data.pnl_correction_state import PnlCorrectionEvidence
from snapper.data.pnl_correction_state import PnlCorrectionKey
from snapper.data.pnl_correction_state import PnlCorrectionSampleVersion
from snapper.data.pnl_correction_state import validate_correction_minute
from snapper.data.pnl_correction_state import validate_correction_peak
from snapper.data.repository_types import PortfolioPnlSampleRow

_MINUTE: Final = timedelta(minutes=1)
_WRITE_PROVENANCE: Final = frozenset({"public_id", "session_id", "sequence_id", "timestamp"})
_ROW_PRESERVING_WITHHOLD_REASONS: Final[frozenset[PnlIncompletenessReason]] = frozenset(
    {"attribution_sum_unrepresentable", "instrument_sum_unrepresentable"}
)


@dataclass(frozen=True, slots=True)
class PnlCorrectionPlanInputs:
    """Bound one correction plan to its observed rows and valuation inputs."""

    evidence: PnlCorrectionEvidence
    minute_inputs: Sequence[MinuteInputs]
    expected_venues: frozenset[str]
    position_versions: Sequence[PositionVersion]
    make_row: Callable[[PlannedSample], PortfolioPnlSampleRow]


@dataclass(frozen=True, slots=True)
class PlannedPnlCorrection:
    """Atomic repository plan and the withheld-minute diagnostics for the caller."""

    chunk: PnlCorrectionChunk
    withheld: tuple[WithheldMinute, ...]


@dataclass(frozen=True, slots=True)
class _MinuteCorrection:
    """The one possible edit, its unresolved cause, and the peak through this minute."""

    edit: PnlCorrectionEdit | None
    withheld: WithheldMinute | None
    peak: float | None


def preserves_persisted_row(withheld: WithheldMinute) -> bool:
    """Recognize a pure representation limit which leaves the prior value intact.

    Args:
        withheld: An explicitly refused plan with its source valuation reasons.

    Returns:
        Whether every cause means the amounts agree exactly but cannot be
        assigned to floating-point buckets. Mixed and unexplained causes fail closed.
    """
    return bool(withheld.reason_codes) and all(
        code in _ROW_PRESERVING_WITHHOLD_REASONS for code in withheld.reason_codes
    )


def _same_sample(existing: PortfolioPnlSampleRow, replacement: PortfolioPnlSampleRow) -> bool:
    """Compare scope, calculation, money, audit, and watermarks without write identity."""
    return {key: value for key, value in existing.items() if key not in _WRITE_PROVENANCE} == {
        key: value for key, value in replacement.items() if key not in _WRITE_PROVENANCE
    }


def _retained_peak(
    key: PnlCorrectionKey, row: PortfolioPnlSampleRow, prior_peak: float | None
) -> float | None:
    """Fold an exact-key retained complete sample into the same causal peak as SQL."""
    if row["calc_version"] != key.calc_version or row["valuation_status"] != "complete":
        return prior_peak
    cash, position = row["cash_usd"], row["position_value_usd"]
    if cash is None or position is None:
        raise ValueError("Retained complete P&L sample must carry both equity components")
    equity = cash + position
    validate_correction_peak(equity)
    return equity if prior_peak is None else max(prior_peak, equity)


def _withhold(
    key: PnlCorrectionKey,
    withheld: WithheldMinute,
    existing: PnlCorrectionSampleVersion | None,
    prior_peak: float | None,
) -> _MinuteCorrection:
    """Keep an allowed same-epoch witness or retract the exact unsupported version."""
    if existing is None:
        return _MinuteCorrection(None, withheld, prior_peak)
    if preserves_persisted_row(withheld):
        return _MinuteCorrection(None, withheld, _retained_peak(key, existing.row, prior_peak))
    return _MinuteCorrection(
        PnlCorrectionEdit(withheld.point_time, existing, None), withheld, prior_peak
    )


def _plan_minute(
    inputs: PnlCorrectionPlanInputs,
    minute: datetime,
    valuation: MinuteInputs | None,
    existing: PnlCorrectionSampleVersion | None,
    prior_peak: float | None,
) -> _MinuteCorrection:
    """Decide one admitted minute before valuing any subsequent drawdown."""
    key = inputs.evidence.key
    if existing is not None and existing.row["epoch_public_id"] != key.epoch_public_id:
        return _MinuteCorrection(
            None, WithheldMinute(minute, ("foreign_epoch_sample",), ()), prior_peak
        )
    if valuation is None:
        return _withhold(
            key, WithheldMinute(minute, ("missing_series_minute",), ()), existing, prior_peak
        )
    plan = assemble_minute_sample(
        valuation, inputs.expected_venues, inputs.position_versions, prior_peak
    )
    if plan.sample is None:
        point = valuation.point
        withheld = WithheldMinute(minute, _withheld_reasons(point), _withheld_reason_codes(point))
        return _withhold(key, withheld, existing, prior_peak)
    replacement = inputs.make_row(plan.sample)
    if existing is not None and _same_sample(existing.row, replacement):
        return _MinuteCorrection(None, None, plan.peak)
    return _MinuteCorrection(PnlCorrectionEdit(minute, existing, replacement), None, plan.peak)


def _inputs_by_minute(
    inputs: Sequence[MinuteInputs], start: datetime, end: datetime
) -> dict[datetime, MinuteInputs]:
    """Index exact series inputs and refuse ambiguity in the admitted minute grid."""
    result: dict[datetime, MinuteInputs] = {}
    for valuation in inputs:
        minute = valuation.point.point_time
        validate_correction_minute(minute)
        if minute in result or not start <= minute <= end:
            raise ValueError("P&L correction requires unique series inputs in the admitted window")
        result[minute] = valuation
    return result


def plan_correction_chunk(inputs: PnlCorrectionPlanInputs) -> PlannedPnlCorrection:
    """Reconcile every admitted minute, carrying peak through rows that remain active.

    Args:
        inputs: Exact active sample evidence and series inputs confined to the
            same bounded window. The caller has verified the series' anchor.

    Returns:
        Exact-version edits and explicitly withheld minutes for one atomic commit.

    Raises:
        ValueError: When the admitted window or one of its minute inputs is invalid.
    """
    evidence = inputs.evidence
    validate_correction_minute(evidence.start)
    validate_correction_minute(evidence.end)
    start, end = evidence.start.astimezone(UTC), evidence.end.astimezone(UTC)
    minutes = (end - start) // _MINUTE + 1
    if not 1 <= minutes <= MAX_CORRECTION_CHUNK_MINUTES:
        raise ValueError("P&L correction planning requires a bounded minute window")
    valuations = _inputs_by_minute(inputs.minute_inputs, start, end)
    observed = {version.row["point_time"]: version for version in evidence.versions}
    edits: list[PnlCorrectionEdit] = []
    withheld: list[WithheldMinute] = []
    peak = evidence.prior_peak
    for offset in range(minutes):
        minute = start + _MINUTE * offset
        decision = _plan_minute(inputs, minute, valuations.get(minute), observed.get(minute), peak)
        peak = decision.peak
        if decision.edit is not None:
            edits.append(decision.edit)
        if decision.withheld is not None:
            withheld.append(decision.withheld)
    chunk = PnlCorrectionChunk(
        end=end,
        edits=tuple(edits),
        prior_peak=peak,
        evidence=evidence,
        withheld_minutes=tuple(minute.point_time for minute in withheld),
        reconciles_repair=True,
    )
    return PlannedPnlCorrection(chunk, tuple(withheld))
