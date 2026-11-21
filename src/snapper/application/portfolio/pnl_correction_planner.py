"""Plan a bounded P&L reconciliation using the exact persisted sample versions."""

from collections.abc import Callable
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import replace
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
REPAIR_EXEMPT_WITHHOLD_REASONS: Final[frozenset[PnlIncompletenessReason]] = frozenset(
    {
        "attribution_reconciliation_failed",
        "attribution_sum_unrepresentable",
        "instrument_reconciliation_failed",
        "instrument_sum_unrepresentable",
    }
)
"""Closed set of reconciliation and faithful-sum withhold reasons exempt from perpetual repair.

Every member arises only after all accounting and pricing inputs are present and
finite. In the snapshotter, ``*_sum_unrepresentable`` is deterministic on frozen
point inputs alone because the caller sets ``recovery_budget_per_point``: each
point gets a fresh count-based ``FaithfulRecoveryBudget``, key eligibility depends
solely on the point's own ``MAX_RECOVERY_KEYS`` input count, and exhausting the
chunk-shared ``_ReplayRecoveryWork`` reserve raises ``PnlTimelineWorkBudgetError``
(triggering chunk subdivision) rather than withholding a minute.

PR-1, introduced under sample version ``5C.5``, changed only repair obligation
(``unrepairable_withheld_minutes`` and ``repair_from``), never valuation (keeping
the sample calc version at ``5C.5`` at that time). Under guard R1,
a minute is marked unrepairable only if no active sample row survives the chunk's
edit; a ``*_sum_unrepresentable`` minute retains an existing row through
:func:`preserves_persisted_row` only when its ``calc_version`` matches the
correction key (staying repairable because determinism of the new refusal proves
nothing about that retained row), whereas a foreign-version row is retracted and
the minute can therefore be exempt.
"""


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
    unrepairable: bool = False


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


def _is_unrepairable_withhold(
    withheld: WithheldMinute,
    existing: PnlCorrectionSampleVersion | None,
    edit: PnlCorrectionEdit | None,
) -> bool:
    """Classify a withheld minute as unrepairable only when deterministic and rowless (R1).

    Empty-code and mixed-reason withholds (such as ``missing_series_minute`` and
    ``foreign_epoch_sample``) stay repairable. A ``*_sum_unrepresentable``
    withhold that retains a same-``calc_version`` row via
    :func:`preserves_persisted_row` stays repairable under R1, whereas one whose
    foreign-version row is retracted leaves no active row and is therefore exempt.

    Args:
        withheld: The withheld minute and its causal ``PnlIncompletenessReason`` codes.
        existing: The active sample version observed at this minute, if any.
        edit: The planned edit for this minute, if any.

    Returns:
        ``True`` iff ``reason_codes`` is non-empty and a subset of
        :data:`REPAIR_EXEMPT_WITHHOLD_REASONS` and no active row survives ``edit``.
    """
    if not withheld.reason_codes or not all(
        code in REPAIR_EXEMPT_WITHHOLD_REASONS for code in withheld.reason_codes
    ):
        return False
    return existing is None or edit == PnlCorrectionEdit(withheld.point_time, existing, None)


def _withhold(
    key: PnlCorrectionKey,
    withheld: WithheldMinute,
    existing: PnlCorrectionSampleVersion | None,
    prior_peak: float | None,
) -> _MinuteCorrection:
    """Keep an allowed same-calc-version witness or retract the observed version.

    Row-preserving reasons retain an active row only when its ``calc_version``
    matches ``key.calc_version``; foreign-version rows are retracted so they
    cannot pin ``repair_from`` under guard R1.
    """
    if existing is None:
        return _MinuteCorrection(None, withheld, prior_peak)
    if preserves_persisted_row(withheld) and existing.row["calc_version"] == key.calc_version:
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
        decision = _withhold(key, withheld, existing, prior_peak)
        return replace(
            decision,
            unrepairable=_is_unrepairable_withhold(withheld, existing, decision.edit),
        )
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

    Classifies deterministic reconciliation and ``*_sum_unrepresentable`` withholds
    as unrepairable only when all ``reason_codes`` are non-empty and in
    :data:`REPAIR_EXEMPT_WITHHOLD_REASONS` and no active row survives the planned
    edit (guard R1: a same-``calc_version`` row retained under
    :func:`preserves_persisted_row` keeps its repair obligation, while a
    foreign-version row is retracted).
    On frozen snapshotter inputs, ``*_sum_unrepresentable`` is deterministic across
    chunk widths because ``recovery_budget_per_point`` gives each point a fresh
    count-based ``FaithfulRecoveryBudget``, ``MAX_RECOVERY_KEYS`` depends solely on
    the point's own input key count, and exhausting the chunk-shared
    ``_ReplayRecoveryWork`` reserve raises ``PnlTimelineWorkBudgetError`` rather
    than withholding.

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
    unrepairable: list[datetime] = []
    peak = evidence.prior_peak
    for offset in range(minutes):
        minute = start + _MINUTE * offset
        decision = _plan_minute(inputs, minute, valuations.get(minute), observed.get(minute), peak)
        peak = decision.peak
        if decision.edit is not None:
            edits.append(decision.edit)
        if decision.withheld is not None:
            withheld.append(decision.withheld)
            if decision.unrepairable:
                unrepairable.append(minute)
    chunk = PnlCorrectionChunk(
        end=end,
        edits=tuple(edits),
        prior_peak=peak,
        evidence=evidence,
        withheld_minutes=tuple(minute.point_time for minute in withheld),
        reconciles_repair=True,
        unrepairable_withheld_minutes=tuple(unrepairable),
    )
    return PlannedPnlCorrection(chunk, tuple(withheld))
