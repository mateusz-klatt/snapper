"""Bounded, request-local P&L timing summaries without business payloads.

Handler timing starts after FastAPI resolves dependencies and ends before later
response serialization and transfer. Series, FX and core spans are inclusive;
timeline extras exclude the separately measured shared prefix and series spans.
Absent spans remain unobserved, and invalid clocks or nesting stay explicit.
"""

from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import Sized
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from dataclasses import field
from functools import wraps
from time import perf_counter_ns
from typing import Literal
from typing import ParamSpec
from typing import TypeVar

from loguru import logger

type PnlReadKind = Literal["series", "timeline"]
type PnlReadStage = Literal["series", "prefix", "fx", "timeline"]
type PnlFxPhase = Literal[
    "fx_discovery_mark",
    "fx_discovery_event",
    "fx_load_mark",
    "fx_load_event",
    "fx_election",
    "fx_fold_final",
]
type PnlFxCount = Literal[
    "fx_mark_candidate_planes",
    "fx_event_candidate_planes",
    "fx_mark_rows",
    "fx_event_rows",
    "fx_selected_rows",
]
type PnlFxReadMode = Literal[
    "no_requirements",
    "no_candidate_planes",
    "shadow_dense",
    "empty_requirements",
    "single_interval_dense",
    "interval_limit_dense",
    "sparse",
]
type _MeasuredStage = Literal["handler", "series", "prefix", "fx", "core", "timeline"] | PnlFxPhase
type _MeasuredCount = PnlFxCount | Literal["fx_event_intervals_observed"]

_P = ParamSpec("_P")
_R = TypeVar("_R")


@dataclass(slots=True)
class _ReadTiming:
    """One handler's closed set of elapsed spans, with no request identifiers."""

    kind: PnlReadKind
    spans: dict[_MeasuredStage, int] = field(default_factory=dict)
    counts: dict[_MeasuredCount, int] = field(default_factory=dict)
    event_read_mode: PnlFxReadMode | Literal["mixed"] | None = None


_current_timing: ContextVar[_ReadTiming | None] = ContextVar("pnl_read_timing", default=None)


def _clock() -> int | None:
    """Read the monotonic clock without making observation failure fatal."""
    try:
        return perf_counter_ns()
    except Exception:
        return None


def _record_elapsed(
    timing: _ReadTiming,
    stage: _MeasuredStage,
    started: int | None,
) -> None:
    """Accumulate measured spans while retaining any invalid clock evidence."""
    finished = _clock()
    previous = timing.spans.get(stage, 0)
    if started is None or finished is None or finished < started or previous < 0:
        timing.spans[stage] = -1
    else:
        timing.spans[stage] = previous + finished - started


def _elapsed_text(elapsed_ns: int | None) -> str:
    """Distinguish absent spans and invalid measurements from measured zero."""
    if elapsed_ns is None:
        return "unobserved"
    if elapsed_ns < 0:
        return "invalid"
    return f"{elapsed_ns / 1_000_000:.3f}"


def _timeline_extra(timing: _ReadTiming) -> int | None:
    """Subtract only observed, valid sequential series and prefix intervals."""
    total = timing.spans.get("timeline")
    series = timing.spans.get("series")
    prefix = timing.spans.get("prefix")
    if total is None or series is None or prefix is None:
        return None
    if total < 0 or series < 0 or prefix < 0:
        return -1
    return total - series - prefix


def _count_text(count: int | None) -> str:
    """Retain unobserved and invalid cardinalities separately from observed zero."""
    if count is None:
        return "unobserved"
    return "invalid" if count < 0 else str(count)


def _record_count(timing: _ReadTiming, name: _MeasuredCount, values: Sized) -> int | None:
    """Sum already-materialized cardinalities without letting observation fail work."""
    previous = timing.counts.get(name, 0)
    try:
        observed = len(values)
    except Exception:
        timing.counts[name] = -1
        return None
    timing.counts[name] = -1 if previous < 0 else previous + observed
    return observed


def pnl_read_count(name: PnlFxCount, values: Sized) -> None:
    """Record a fixed aggregate count only while an API collector is active.

    Args:
        name: Closed engineering cardinality independent of business identities.
        values: Existing materialized values whose length is observed lazily.
    """
    timing = _current_timing.get()
    if timing is not None:
        _record_count(timing, name, values)


def pnl_read_fx_window(
    mode: PnlFxReadMode | Literal["constructed"], intervals: Sized | None = None
) -> None:
    """Accumulate actual selector prefixes and preserve different observed modes.

    Args:
        mode: Closed reason recorded at the existing read-selection boundary.
        intervals: Constructed prefix, absent when selection was bypassed.
    """
    timing = _current_timing.get()
    if timing is None:
        return
    observed = (
        _record_count(timing, "fx_event_intervals_observed", intervals)
        if intervals is not None
        else None
    )
    if mode == "constructed":
        if observed is None:
            return
        mode = (
            "sparse"
            if observed > 1
            else "single_interval_dense" if observed else "empty_requirements"
        )
    previous = timing.event_read_mode
    timing.event_read_mode = mode if previous is None or previous == mode else "mixed"


@contextmanager
def pnl_read_phase(phase: PnlFxPhase) -> Iterator[None]:
    """Measure one existing synchronous or awaited phase without moving its work.

    Args:
        phase: Fixed FX boundary recorded in the current handler.

    Yields:
        Control to the unchanged phase body.
    """
    timing = _current_timing.get()
    if timing is None:
        yield
        return
    started = _clock()
    try:
        yield
    finally:
        _record_elapsed(timing, phase, started)


def _emit_timing(timing: _ReadTiming, succeeded: bool) -> None:
    """Render every field in the message because configured sinks omit extras."""
    try:
        logger.info(
            "PnlReadTiming kind={} outcome={} handler_ms={} series_ms={} prefix_ms={} "
            "fx_ms={} core_ms={} timeline_extra_ms={} "
            "fx_discovery_mark_ms={} fx_discovery_event_ms={} fx_load_mark_ms={} "
            "fx_load_event_ms={} fx_election_ms={} fx_fold_final_ms={} "
            "fx_mark_candidate_planes={} fx_event_candidate_planes={} fx_mark_rows={} "
            "fx_event_rows={} fx_selected_rows={} fx_event_read_mode={} "
            "fx_event_intervals_observed={}",
            timing.kind,
            "success" if succeeded else "failure",
            _elapsed_text(timing.spans.get("handler")),
            _elapsed_text(timing.spans.get("series")),
            _elapsed_text(timing.spans.get("prefix")),
            _elapsed_text(timing.spans.get("fx")),
            _elapsed_text(timing.spans.get("core")),
            _elapsed_text(_timeline_extra(timing)),
            _elapsed_text(timing.spans.get("fx_discovery_mark")),
            _elapsed_text(timing.spans.get("fx_discovery_event")),
            _elapsed_text(timing.spans.get("fx_load_mark")),
            _elapsed_text(timing.spans.get("fx_load_event")),
            _elapsed_text(timing.spans.get("fx_election")),
            _elapsed_text(timing.spans.get("fx_fold_final")),
            _count_text(timing.counts.get("fx_mark_candidate_planes")),
            _count_text(timing.counts.get("fx_event_candidate_planes")),
            _count_text(timing.counts.get("fx_mark_rows")),
            _count_text(timing.counts.get("fx_event_rows")),
            _count_text(timing.counts.get("fx_selected_rows")),
            timing.event_read_mode or "unobserved",
            _count_text(timing.counts.get("fx_event_intervals_observed")),
        )
    except Exception:
        return


def observe_pnl_read(
    kind: PnlReadKind,
) -> Callable[[Callable[_P, Awaitable[_R]]], Callable[_P, Awaitable[_R]]]:
    """Observe one actual handler while preserving its signature and exceptions.

    Args:
        kind: Fixed endpoint kind, independent of all handler arguments.

    Returns:
        A signature-preserving decorator that emits one handler summary.
    """

    def decorate(function: Callable[_P, Awaitable[_R]]) -> Callable[_P, Awaitable[_R]]:
        """Bind the closed endpoint kind without inspecting handler arguments."""

        @wraps(function)
        async def observed(*args: _P.args, **kwargs: _P.kwargs) -> _R:
            """Reset request-local state before emitting exactly one summary."""
            timing = _ReadTiming(kind)
            started = _clock()
            token = _current_timing.set(timing)
            succeeded = False
            try:
                result = await function(*args, **kwargs)
                succeeded = True
                return result
            finally:
                _record_elapsed(timing, "handler", started)
                _current_timing.reset(token)
                _emit_timing(timing, succeeded)

        return observed

    return decorate


def pnl_read_stage(
    stage: PnlReadStage,
) -> Callable[[Callable[_P, Awaitable[_R]]], Callable[_P, Awaitable[_R]]]:
    """Measure one inclusive service stage only inside an observed API handler.

    Args:
        stage: Fixed service boundary recorded in the active handler collector.

    Returns:
        A signature-preserving asynchronous stage decorator.
    """

    def decorate(function: Callable[_P, Awaitable[_R]]) -> Callable[_P, Awaitable[_R]]:
        """Retain callable metadata and the original asynchronous contract."""

        @wraps(function)
        async def observed(*args: _P.args, **kwargs: _P.kwargs) -> _R:
            """Pass background callers through without reading the clock."""
            timing = _current_timing.get()
            if timing is None:
                return await function(*args, **kwargs)
            started = _clock()
            try:
                return await function(*args, **kwargs)
            finally:
                _record_elapsed(timing, stage, started)

        return observed

    return decorate


def pnl_read_core[**P, R](function: Callable[P, R]) -> Callable[P, R]:
    """Measure synchronous pure construction at its service call boundary.

    Args:
        function: Pure construction callable forwarded unchanged by the service.

    Returns:
        A signature-preserving callable that measures only active API work.
    """

    @wraps(function)
    def observed(*args: P.args, **kwargs: P.kwargs) -> R:
        """Keep the pure builder itself independent of clocks and logging."""
        timing = _current_timing.get()
        if timing is None:
            return function(*args, **kwargs)
        started = _clock()
        try:
            return function(*args, **kwargs)
        finally:
            _record_elapsed(timing, "core", started)

    return observed
