"""Bounded, request-local P&L timing summaries without business payloads.

Handler timing starts after FastAPI resolves dependencies and ends before later
response serialization and transfer. Series, FX and core spans are inclusive;
timeline extras exclude the separately measured shared prefix and series spans.
Absent spans remain unobserved, and invalid clocks or nesting stay explicit.
"""

from collections.abc import Awaitable
from collections.abc import Callable
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
type _MeasuredStage = Literal["handler", "series", "prefix", "fx", "core", "timeline"]

_P = ParamSpec("_P")
_R = TypeVar("_R")


@dataclass(slots=True)
class _ReadTiming:
    """One handler's closed set of elapsed spans, with no request identifiers."""

    kind: PnlReadKind
    spans: dict[_MeasuredStage, int] = field(default_factory=dict)


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


def _emit_timing(timing: _ReadTiming, succeeded: bool) -> None:
    """Render every field in the message because configured sinks omit extras."""
    try:
        logger.info(
            "PnlReadTiming kind={} outcome={} handler_ms={} series_ms={} prefix_ms={} "
            "fx_ms={} core_ms={} timeline_extra_ms={}",
            timing.kind,
            "success" if succeeded else "failure",
            _elapsed_text(timing.spans.get("handler")),
            _elapsed_text(timing.spans.get("series")),
            _elapsed_text(timing.spans.get("prefix")),
            _elapsed_text(timing.spans.get("fx")),
            _elapsed_text(timing.spans.get("core")),
            _elapsed_text(_timeline_extra(timing)),
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
