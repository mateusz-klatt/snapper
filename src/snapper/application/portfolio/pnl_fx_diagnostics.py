"""Bounded request-local FX evidence diagnostics containing no monetary values."""

import json
from collections.abc import Iterator
from collections.abc import Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from typing import Literal

from snapper.application.portfolio.fx_rates import FxPairKey
from snapper.application.portfolio.fx_rates import FxRateMap
from snapper.application.portfolio.fx_rates import FxRatePlane
from snapper.application.portfolio.fx_rates import FxVenueMap
from snapper.application.portfolio.fx_rates import currency_pair_key
from snapper.core.json_types import JsonObject
from snapper.core.json_types import JsonValue
from snapper.core.numeric import is_positive_finite

type PnlFxRole = Literal["price", "fee", "accrual", "mark"]
type PnlFxFailureReason = Literal[
    "no_pin",
    "missing_exact_minute",
    "invalid_or_conflicting_close",
    "conversion_nonfinite",
]
type PnlFxCausalOrder = tuple[datetime, int, str, int, str, int]
type PnlFxRequirementMap = Mapping[str, Mapping[FxPairKey, set[datetime]]]

_MAX_ELECTIONS = 16
_MAX_CANDIDATES = 16
_MAX_METADATA_LENGTH = 96


@dataclass(frozen=True, slots=True)
class PnlFxFailure:
    """Identity and chronology of one rejected conversion, without its input."""

    instrument: str
    role: PnlFxRole
    event_time: datetime
    rate_minute: datetime
    from_currency: str
    to_currency: str
    causal_order: PnlFxCausalOrder


@dataclass(slots=True)
class PnlFxDiagnostics:
    """One handler's bounded first failure and election summaries."""

    first_failure: JsonObject | None = None
    first_failure_order: PnlFxCausalOrder | None = None
    elections: list[JsonValue] = field(default_factory=list)
    elections_omitted: int = 0
    execution_times: dict[tuple[str, str], datetime] = field(default_factory=dict)


_current_diagnostics: ContextVar[PnlFxDiagnostics | None] = ContextVar(
    "pnl_fx_diagnostics", default=None
)
_current_conversion_scope: ContextVar[tuple[PnlFxRequirementMap, datetime | None] | None] = (
    ContextVar("pnl_fx_conversion_scope", default=None)
)


@contextmanager
def observe_pnl_fx_diagnostics() -> Iterator[PnlFxDiagnostics]:
    """Own an isolated collector for one observed API handler.

    Yields:
        The handler's fresh failure and election collector.
    """
    diagnostics = PnlFxDiagnostics()
    token = _current_diagnostics.set(diagnostics)
    scope_token = _current_conversion_scope.set(None)
    try:
        yield diagnostics
    finally:
        _current_conversion_scope.reset(scope_token)
        _current_diagnostics.reset(token)


def pnl_fx_diagnostics_active() -> bool:
    """Allow service callers to avoid constructing observations in background work.

    Returns:
        Whether this task currently owns an observed handler collector.
    """
    return _current_diagnostics.get() is not None


@contextmanager
def pnl_fx_conversion_scope(
    mark_requirements: PnlFxRequirementMap, through: datetime | None = None
) -> Iterator[None]:
    """Limit reported failures to causal marks and events through the replay horizon.

    Args:
        mark_requirements: Existing instrument mark requirements, referenced unchanged.
        through: Last effective event time the current replay can consume.

    Yields:
        Control to the unchanged service conversion batch.
    """
    token = _current_conversion_scope.set((mark_requirements, through))
    try:
        yield
    finally:
        _current_conversion_scope.reset(token)


def _failure_required(failure: PnlFxFailure) -> bool:
    """Exclude conversions whose input cannot contribute to this causal replay."""
    scope = _current_conversion_scope.get()
    if scope is None:
        return True
    requirements, through = scope
    if through is not None and failure.causal_order[0] > through:
        return False
    if failure.role != "mark":
        return True
    pair = currency_pair_key(failure.from_currency, failure.to_currency)
    return failure.rate_minute in requirements.get(failure.instrument, {}).get(pair, set())


def reset_pnl_fx_execution_order() -> None:
    """Start the independent causal cursor for a new service conversion batch."""
    diagnostics = _current_diagnostics.get()
    if diagnostics is not None:
        try:
            diagnostics.execution_times.clear()
        except Exception:
            return


def pnl_fx_execution_time(instrument: str, shard: str, event_time: datetime) -> datetime:
    """Mirror the replay's per-pool monotone time without retaining row payloads.

    Args:
        instrument: Existing public instrument identity.
        shard: Existing pool identity, used only in private request-local state.
        event_time: Execution timestamp, independent of its rate minute.

    Returns:
        The per-pool running maximum, or the original time if observation fails.
    """
    diagnostics = _current_diagnostics.get()
    if diagnostics is None:
        return event_time
    try:
        pool = (instrument, shard)
        effective = max(event_time, diagnostics.execution_times.get(pool, event_time))
        diagnostics.execution_times[pool] = effective
        return effective
    except Exception:
        return event_time


def _metadata(value: str) -> str:
    """Cap externally supplied identity text before retaining it in request state."""
    return value[:_MAX_METADATA_LENGTH]


def _plane_text(plane: FxRatePlane | None) -> JsonValue:
    """Render only the public currency and venue identity of an existing plane."""
    return None if plane is None else [_metadata(part) for part in plane]


def _failure_reason(
    failure: PnlFxFailure,
    rates: FxRateMap,
    plane: FxRatePlane | None,
) -> PnlFxFailureReason:
    """Classify existing rejected evidence without applying conversion arithmetic."""
    if plane is None:
        return "no_pin"
    key = (*plane, failure.rate_minute)
    if key not in rates:
        return "missing_exact_minute"
    if not is_positive_finite(rates[key]) or plane[:2] not in (
        (failure.from_currency, failure.to_currency),
        (failure.to_currency, failure.from_currency),
    ):
        return "invalid_or_conflicting_close"
    return "conversion_nonfinite"


def record_pnl_fx_failure(
    failure: PnlFxFailure,
    rates: FxRateMap,
    venues: FxVenueMap | None,
) -> None:
    """Retain only the earliest rejected conversion in supplied causal replay order.

    Args:
        failure: Amount-free conversion identity and causal ordering key.
        rates: Already loaded rate evidence, inspected without numeric serialization.
        venues: Existing consumer selections, never changed by diagnostics.
    """
    diagnostics = _current_diagnostics.get()
    if diagnostics is None:
        return
    try:
        if not _failure_required(failure):
            return
        previous = diagnostics.first_failure_order
        if previous is not None and previous <= failure.causal_order:
            return
        pair = currency_pair_key(failure.from_currency, failure.to_currency)
        plane = None if venues is None else venues.get(pair)
        record: JsonObject = {
            "instrument": _metadata(failure.instrument),
            "role": failure.role,
            "event_time": failure.event_time.isoformat(),
            "rate_minute": failure.rate_minute.isoformat(),
            "from_currency": _metadata(failure.from_currency),
            "to_currency": _metadata(failure.to_currency),
            "selected_plane": _plane_text(plane),
            "reason": _failure_reason(failure, rates, plane),
        }
        diagnostics.first_failure = record
        diagnostics.first_failure_order = failure.causal_order
    except Exception:
        return


def _candidate_text(plane: FxRatePlane, exact: set[datetime], events: set[datetime]) -> JsonObject:
    """Summarize exact event proof without retaining or rendering rate values."""
    missing = events - exact
    return {
        "plane": _plane_text(plane),
        "exact_event_count": len(events & exact),
        "first_missing_event_minute": None if not missing else min(missing).isoformat(),
    }


def record_pnl_fx_election(
    pair: FxPairKey,
    event_minutes: set[datetime],
    candidates: Mapping[FxRatePlane, set[datetime]],
    elected: FxRatePlane | None,
    instrument: str | None = None,
) -> None:
    """Record the existing election result and each bounded candidate's exact proof.

    Args:
        pair: Existing unordered currency pair.
        event_minutes: Existing distinct exact-minute event requirements.
        candidates: Existing candidate identities mapped to exact minute coverage.
        elected: Plane chosen by the unchanged election.
        instrument: Canonical owner identity, or absent for the shared election.
    """
    diagnostics = _current_diagnostics.get()
    if diagnostics is None:
        return
    try:
        if len(diagnostics.elections) >= _MAX_ELECTIONS:
            diagnostics.elections_omitted += 1
            return
        planes = sorted(candidates)
        candidate_records: list[JsonValue] = [
            _candidate_text(plane, candidates[plane], event_minutes)
            for plane in planes[:_MAX_CANDIDATES]
        ]
        diagnostics.elections.append(
            {
                "pair": [_metadata(part) for part in pair],
                "instrument": None if instrument is None else _metadata(instrument),
                "event_count": len(event_minutes),
                "complete_plane_exists": any(
                    event_minutes <= exact for exact in candidates.values()
                ),
                "elected_plane": _plane_text(elected),
                "candidates": candidate_records,
                "candidates_omitted": max(0, len(planes) - _MAX_CANDIDATES),
            }
        )
    except Exception:
        return


def pnl_fx_diagnostics_text(diagnostics: PnlFxDiagnostics) -> str:
    """Render bounded one-line JSON inside log text, isolating serialization failures.

    Args:
        diagnostics: Collector belonging to the handler that is being logged.

    Returns:
        An optional fixed-prefix suffix, containing no conversion inputs or outputs.
    """
    try:
        if diagnostics.first_failure is None and not diagnostics.elections:
            return ""
        return " fx_diagnostics=" + json.dumps(
            {
                "first_failure": diagnostics.first_failure,
                "elections": diagnostics.elections,
                "elections_omitted": diagnostics.elections_omitted,
            },
            ensure_ascii=True,
            allow_nan=False,
            separators=(",", ":"),
        )
    except Exception:
        return ""
