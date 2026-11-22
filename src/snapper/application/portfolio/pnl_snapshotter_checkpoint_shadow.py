"""Focused bit-exact comparison of anchor and checkpoint-resumed shadow plans."""

import math
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import fields
from dataclasses import is_dataclass
from datetime import datetime
from typing import Literal

from snapper.application.portfolio.pnl_correction_planner import PlannedPnlCorrection
from snapper.application.portfolio.pnl_timeline import PnlTimelinePoint
from snapper.application.portfolio.pnl_timeline_service import PnlWalletSeriesResult
from snapper.core.json_types import JsonValue
from snapper.data.pnl_correction_state import PnlCorrectionEdit
from snapper.data.repository_types import PortfolioPnlSampleRow


@dataclass(frozen=True)
class PnlCheckpointShadowMismatch:
    """First differing published point and field in a shadow comparison."""

    point_time: datetime | None
    field: str


@dataclass(frozen=True)
class PnlCheckpointShadowWindow:
    """Exact comparison identity binding window bounds, scope, and calc version."""

    from_time: datetime
    to_time: datetime
    scope: tuple[str, str, str, str]
    calc_version: str


@dataclass(frozen=True)
class PnlCheckpointShadowSide:
    """One side of a shadow comparison bundled with its exact replay window."""

    window: PnlCheckpointShadowWindow
    result: PnlWalletSeriesResult
    plan: PlannedPnlCorrection


@dataclass(frozen=True)
class PnlCheckpointShadowOutcome:
    """Typed outcome of comparing two shadow sides on window identity and financial equivalence."""

    skipped_reason: Literal["window_mismatch"] | None = None
    mismatch: PnlCheckpointShadowMismatch | None = None


def _canonical(value: object) -> JsonValue:
    """Convert nested result values to a float-exact, NaN-aware representation."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return "nan" if math.isnan(value) else value.hex()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return [
            [_canonical(key), _canonical(item)]
            for key, item in sorted(value.items(), key=lambda pair: repr(pair[0]))
        ]
    if isinstance(value, (tuple, list, set, frozenset)):
        items = [_canonical(item) for item in value]
        return sorted(items, key=repr) if isinstance(value, (set, frozenset)) else items
    if is_dataclass(value) and not isinstance(value, type):
        return {item.name: _canonical(getattr(value, item.name)) for item in fields(value)}
    raise TypeError(f"Unsupported checkpoint shadow value: {type(value).__name__}")


def _compare_points(
    anchor: PnlWalletSeriesResult,
    resumed: PnlWalletSeriesResult,
) -> PnlCheckpointShadowMismatch | None:
    """Compare each published point field in stable minute and declaration order."""
    if len(anchor.points) != len(resumed.points):
        point_time = _first_available_point(anchor.points, resumed.points, len(anchor.points))
        return PnlCheckpointShadowMismatch(point_time, "points.length")
    for left, right in zip(anchor.points, resumed.points, strict=True):
        mismatch = _compare_point(left, right)
        if mismatch is not None:
            return mismatch
    return None


def _first_available_point(
    anchor: tuple[PnlTimelinePoint, ...],
    resumed: tuple[PnlTimelinePoint, ...],
    index: int,
) -> datetime | None:
    """Return the first point identity at a length mismatch when one exists."""
    shorter = min(len(anchor), len(resumed))
    longer = anchor if len(anchor) > len(resumed) else resumed
    return longer[shorter].point_time if longer else None


def _compare_point(
    anchor: PnlTimelinePoint,
    resumed: PnlTimelinePoint,
) -> PnlCheckpointShadowMismatch | None:
    """Compare all point values, reasons, contributions, and attribution exactly."""
    if anchor.point_time != resumed.point_time:
        return PnlCheckpointShadowMismatch(anchor.point_time, "points.point_time")
    for item in fields(anchor):
        if _canonical(getattr(anchor, item.name)) != _canonical(getattr(resumed, item.name)):
            return PnlCheckpointShadowMismatch(anchor.point_time, f"points.{item.name}")
    return None


def _compare_results(
    anchor: PnlWalletSeriesResult,
    resumed: PnlWalletSeriesResult,
) -> PnlCheckpointShadowMismatch | None:
    """Compare point values followed by series provenance and replay metadata."""
    points = _compare_points(anchor, resumed)
    if points is not None:
        return points
    for field in (
        "granularity",
        "valuation_ccy",
        "rate_sources",
        "replay_metadata",
        "applied_annulments",
    ):
        if _canonical(getattr(anchor, field)) != _canonical(getattr(resumed, field)):
            return PnlCheckpointShadowMismatch(None, field)
    return None


def _canonical_replacement(row: PortfolioPnlSampleRow | None) -> JsonValue:
    """Exclude generated write identity while retaining every financial and audit field."""
    if row is None:
        return None
    return _canonical(
        {
            key: value
            for key, value in row.items()
            if key not in {"public_id", "session_id", "sequence_id", "timestamp"}
        }
    )


def _canonical_edits(edits: tuple[PnlCorrectionEdit, ...]) -> JsonValue:
    """Compare edit identity, exact prior versions, and financial replacements."""
    return [
        {
            "point_time": _canonical(edit.point_time),
            "expected_row": _canonical(edit.expected_row),
            "replacement": _canonical_replacement(edit.replacement),
        }
        for edit in edits
    ]


def _compare_plans(
    anchor: PlannedPnlCorrection,
    resumed: PlannedPnlCorrection,
) -> PnlCheckpointShadowMismatch | None:
    """Compare every committed chunk field and withheld diagnostic."""
    for field in (
        "end",
        "edits",
        "withheld_minutes",
        "unrepairable_withheld_minutes",
        "prior_peak",
        "reconciles_repair",
        "evidence",
    ):
        left = (
            _canonical_edits(anchor.chunk.edits)
            if field == "edits"
            else _canonical(getattr(anchor.chunk, field))
        )
        right = (
            _canonical_edits(resumed.chunk.edits)
            if field == "edits"
            else _canonical(getattr(resumed.chunk, field))
        )
        if left != right:
            return PnlCheckpointShadowMismatch(None, f"chunk.{field}")
    if _canonical(anchor.withheld) != _canonical(resumed.withheld):
        return PnlCheckpointShadowMismatch(None, "withheld")
    return None


def compare_checkpoint_shadow(
    anchor: PnlWalletSeriesResult,
    anchor_plan: PlannedPnlCorrection,
    resumed: PnlWalletSeriesResult,
    resumed_plan: PlannedPnlCorrection,
) -> PnlCheckpointShadowMismatch | None:
    """Return the first result or planned-chunk divergence, or ``None`` on equality.

    Args:
        anchor: Full-prefix replay result under the live FX shadow context.
        anchor_plan: Correction plan produced from the anchor result.
        resumed: Suffix replay result with duplicate FX shadow collection suppressed.
        resumed_plan: Correction plan produced from the resumed result.

    Returns:
        The first exact mismatch, including point identity and field, or ``None``.
    """
    return _compare_results(anchor, resumed) or _compare_plans(anchor_plan, resumed_plan)


def compare_checkpoint_shadow_sides(
    anchor_side: PnlCheckpointShadowSide,
    resumed_side: PnlCheckpointShadowSide,
) -> PnlCheckpointShadowOutcome:
    """Compare shadow sides only when their window, scope, and calc version match.

    Args:
        anchor_side: Anchor window metadata, replay result, and planned chunk.
        resumed_side: Resumed window metadata, replay result, and planned chunk.

    Returns:
        A ``window_mismatch`` skip outcome if windows differ, otherwise the comparison result.
    """
    if anchor_side.window != resumed_side.window:
        return PnlCheckpointShadowOutcome(skipped_reason="window_mismatch", mismatch=None)
    mismatch = compare_checkpoint_shadow(
        anchor_side.result,
        anchor_side.plan,
        resumed_side.result,
        resumed_side.plan,
    )
    return PnlCheckpointShadowOutcome(skipped_reason=None, mismatch=mismatch)
