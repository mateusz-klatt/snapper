"""Verify bounded timing observations preserve API behavior and task isolation."""

import asyncio
import inspect
from collections.abc import Iterable
from collections.abc import Iterator
from typing import Literal

import pytest

from snapper.application.portfolio import pnl_read_timing
from snapper.application.portfolio.pnl_read_timing import observe_pnl_read
from snapper.application.portfolio.pnl_read_timing import pnl_read_core
from snapper.application.portfolio.pnl_read_timing import pnl_read_stage


def _capture(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Collect actual message text rather than dropped logger extras."""
    messages: list[str] = []

    def capture(message: str, *args: object) -> None:
        """Render the fixed message with the same positional-field semantics."""
        messages.append(message.format(*args))

    monkeypatch.setattr(pnl_read_timing.logger, "info", capture)
    return messages


def _clock(monkeypatch: pytest.MonkeyPatch, values: Iterable[int | Exception]) -> list[int]:
    """Install deterministic monotonic readings or explicit clock failures."""
    readings: Iterator[int | Exception] = iter(values)
    visits: list[int] = []

    def read() -> int:
        """Consume one supplied reading without sleeps or elapsed-time thresholds."""
        visits.append(1)
        value = next(readings)
        if isinstance(value, Exception):
            raise value
        return value

    monkeypatch.setattr(pnl_read_timing, "perf_counter_ns", read)
    return visits


async def test_nested_stage_summary_has_exact_inclusive_durations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Nested shared work and derived extras retain their interval meanings.

    Given: A deterministic clock and nested prefix, series, FX and core stages.
    When: An actual observed handler completes those stages sequentially.
    Then: One summary reports the exact inclusive spans and remaining extras.
    """
    messages = _capture(monkeypatch)
    _clock(monkeypatch, range(0, 111_000_000, 10_000_000))

    @pnl_read_core
    def core(value: object) -> object:
        """Return the unchanged caller-owned object from synchronous construction."""
        return value

    @pnl_read_stage("fx")
    async def fx() -> None:
        """Model the inclusive FX boundary without financial payloads."""

    @pnl_read_stage("prefix")
    async def prefix() -> None:
        """Model the separately preloaded shared prefix."""

    @pnl_read_stage("series")
    async def series(value: object) -> object:
        """Keep FX and core work nested inside the inclusive series span."""
        await fx()
        return core(value)

    @pnl_read_stage("timeline")
    async def timeline(value: object) -> object:
        """Keep prefix and series inside the inclusive timeline span."""
        await prefix()
        return await series(value)

    @observe_pnl_read("timeline")
    async def handler(value: object) -> object:
        """Preserve the handler's original return identity."""
        return await timeline(value)

    value = object()
    assert await handler(value) is value
    assert messages == [
        (
            "PnlReadTiming kind=timeline outcome=success handler_ms=110.000 series_ms=50.000 "
            "prefix_ms=10.000 fx_ms=10.000 core_ms=10.000 timeline_extra_ms=30.000 "
            "fx_discovery_mark_ms=unobserved fx_discovery_event_ms=unobserved "
            "fx_load_mark_ms=unobserved fx_load_event_ms=unobserved "
            "fx_election_ms=unobserved fx_fold_final_ms=unobserved "
            "fx_mark_candidate_planes=unobserved fx_event_candidate_planes=unobserved "
            "fx_mark_rows=unobserved fx_event_rows=unobserved fx_selected_rows=unobserved "
            "fx_event_read_mode=unobserved fx_event_intervals_observed=unobserved"
        )
    ]
    assert pnl_read_timing._current_timing.get() is None


async def test_non_api_service_calls_have_no_clock_or_log_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Background jobs retain direct behavior without an API collector.

    Given: Decorated synchronous and asynchronous work outside a handler.
    When: The background caller forwards its argument through both wrappers.
    Then: The result is unchanged and neither clocks nor logging are invoked.
    """
    messages = _capture(monkeypatch)
    clock_reads = _clock(monkeypatch, [])

    @pnl_read_core
    def core(value: int) -> int:
        """Preserve synchronous arguments outside observation."""
        return value

    @pnl_read_stage("series")
    async def series(value: int) -> int:
        """Preserve asynchronous arguments outside observation."""
        return core(value)

    assert await series(7) == 7
    assert messages == []
    assert clock_reads == []


@pytest.mark.parametrize("cancelled", [False, True])
async def test_handler_exception_and_cancellation_propagate_unchanged(
    monkeypatch: pytest.MonkeyPatch,
    cancelled: bool,
) -> None:
    """Failure observations preserve the original exception and task state.

    Given: A service stage raising a private exception or cancellation object.
    When: That failure crosses the observed handler boundary.
    Then: The same object propagates, one safe summary emits, and state resets.
    """
    messages = _capture(monkeypatch)
    _clock(monkeypatch, [0, 1_000_000, 2_000_000, 3_000_000])
    failure: BaseException = (
        asyncio.CancelledError("private-token-sentinel")
        if cancelled
        else ValueError("private-token-sentinel")
    )

    @pnl_read_stage("series")
    async def series() -> None:
        """Raise the original object through the measured boundary."""
        raise failure

    @observe_pnl_read("series")
    async def handler() -> None:
        """Keep cancellation and domain exceptions under the caller's control."""
        await series()

    with pytest.raises(type(failure)) as rejected:
        await handler()
    assert rejected.value is failure
    assert len(messages) == 1
    assert "kind=series outcome=failure" in messages[0]
    assert "series_ms=1.000" in messages[0]
    assert "core_ms=unobserved" in messages[0]
    assert "private-token-sentinel" not in messages[0]
    assert pnl_read_timing._current_timing.get() is None


async def test_logging_failure_preserves_return_value_and_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed observation sink cannot turn a successful handler into an error.

    Given: A successful handler and a logger that raises while emitting.
    When: Observation cleanup runs after the handler returns.
    Then: The original return value survives and the request collector resets.
    """
    _clock(monkeypatch, [0, 0])

    def fail_log(_message: str, *_args: object) -> None:
        """Model a sink failure without a secondary logging path."""
        raise RuntimeError("sink unavailable")

    monkeypatch.setattr(pnl_read_timing.logger, "info", fail_log)

    @observe_pnl_read("series")
    async def handler() -> int:
        """Return ordinary handler data unchanged."""
        return 9

    assert await handler() == 9
    assert pnl_read_timing._current_timing.get() is None


@pytest.mark.parametrize(
    "readings",
    [
        pytest.param([2_000_000, 1_000_000], id="backward-clock"),
        pytest.param([RuntimeError("clock failure"), 1_000_000], id="missing-start"),
        pytest.param([0, RuntimeError("clock failure")], id="missing-finish"),
    ],
)
async def test_clock_anomalies_are_explicit_invalid_measurements(
    monkeypatch: pytest.MonkeyPatch,
    readings: list[int | Exception],
) -> None:
    """A bad clock never becomes a valid zero or changes a successful response.

    Given: Backward or unavailable handler clock readings.
    When: The observed handler returns its normal value.
    Then: Timing is explicitly invalid and absent stages remain unobserved.
    """
    messages = _capture(monkeypatch)
    _clock(monkeypatch, readings)

    @observe_pnl_read("series")
    async def handler() -> str:
        """Keep original success independent of measurement availability."""
        return "unchanged"

    assert await handler() == "unchanged"
    assert "handler_ms=invalid" in messages[0]
    assert "series_ms=unobserved" in messages[0]


async def test_negative_timeline_residual_is_not_clamped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Inconsistent nested intervals remain visible as invalid evidence.

    Given: Prefix and series intervals whose sum exceeds their parent interval.
    When: The timeline summary derives its remaining work duration.
    Then: The residual is invalid rather than clamped to a plausible zero.
    """
    messages = _capture(monkeypatch)
    _clock(monkeypatch, [0, 0, 0, 8_000_000, 0, 8_000_000, 10_000_000, 10_000_000])

    @pnl_read_stage("prefix")
    async def prefix() -> None:
        """Supply one observed prefix interval."""

    @pnl_read_stage("series")
    async def series() -> None:
        """Supply one observed series interval."""

    @pnl_read_stage("timeline")
    async def timeline() -> None:
        """Expose intervals whose sum exceeds the parent clock reading."""
        await prefix()
        await series()

    @observe_pnl_read("timeline")
    async def handler() -> None:
        """Complete normally despite invalid observation arithmetic."""
        await timeline()

    await handler()
    assert "timeline_extra_ms=invalid" in messages[0]
    assert "outcome=success" in messages[0]


async def test_nested_handlers_restore_the_outer_collector(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One handler's finally restores its caller's separate timing context.

    Given: An outer handler invoking a separately observed inner handler.
    When: Each handler performs its own FX stage.
    Then: Each emits one separate span and the outer collector resumes safely.
    """
    messages = _capture(monkeypatch)
    _clock(monkeypatch, range(0, 8_000_000, 1_000_000))

    @pnl_read_stage("fx")
    async def fx() -> None:
        """Add work only to the currently active handler."""

    @observe_pnl_read("timeline")
    async def inner() -> None:
        """Use an independent collector for an actual nested handler call."""
        await fx()

    @observe_pnl_read("series")
    async def outer() -> None:
        """Resume its own collector after the nested handler exits."""
        await inner()
        await fx()

    await outer()
    assert len(messages) == 2
    assert "kind=timeline" in messages[0]
    assert "fx_ms=1.000" in messages[0]
    assert "kind=series" in messages[1]
    assert "fx_ms=1.000" in messages[1]
    assert pnl_read_timing._current_timing.get() is None


async def test_concurrent_handlers_never_share_spans(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Interleaved request tasks keep separate collectors and one summary each.

    Given: Two handlers coordinated by explicit events without sleeps.
    When: One handler completes FX while the other's series stage is suspended.
    Then: Their summaries retain only their own work and no task state leaks.
    """
    messages = _capture(monkeypatch)
    _clock(monkeypatch, range(0, 20_000_000, 1_000_000))
    entered = asyncio.Event()
    release = asyncio.Event()

    @pnl_read_stage("series")
    async def delayed() -> None:
        """Block only the first request at an explicitly signalled boundary."""
        entered.set()
        await release.wait()

    @pnl_read_stage("fx")
    async def fx() -> None:
        """Mark work performed only by the second request."""

    @observe_pnl_read("series")
    async def first() -> None:
        """Keep a series stage alive while another handler completes."""
        await delayed()

    @observe_pnl_read("timeline")
    async def second() -> None:
        """Complete in a separate task context and release the first request."""
        await fx()
        release.set()

    first_task: asyncio.Task[None] = asyncio.create_task(first())
    await entered.wait()
    try:
        await asyncio.create_task(second())
        await first_task
    finally:
        if not first_task.done():
            first_task.cancel()
            await asyncio.gather(first_task, return_exceptions=True)
    assert len(messages) == 2
    assert "kind=timeline" in messages[0]
    assert "series_ms=unobserved" in messages[0]
    assert "kind=series" in messages[1]
    assert "fx_ms=unobserved" in messages[1]
    assert pnl_read_timing._current_timing.get() is None


def test_wrappers_preserve_signature_annotations_and_async_contract() -> None:
    """FastAPI can inspect the original asynchronous callable contract.

    Given: A handler with concrete annotations and a keyword-only default.
    When: Request and service decorators wrap that handler.
    Then: Its signature, annotations, name and coroutine identity are retained.
    """

    async def original(value: int, *, label: str = "safe") -> tuple[int, str]:
        """Retain the real callable metadata rather than exposing args and kwargs."""
        return value, label

    for wrapped in (observe_pnl_read("series")(original), pnl_read_stage("series")(original)):
        assert inspect.signature(wrapped) == inspect.signature(original)
        assert wrapped.__annotations__ == original.__annotations__
        assert wrapped.__name__ == original.__name__
        assert inspect.iscoroutinefunction(wrapped)


def test_core_wrapper_preserves_signature_annotations() -> None:
    """Synchronous construction exposes its original callable contract.

    Given: A pure callable with concrete annotations and a keyword-only default.
    When: The core timing decorator wraps it.
    Then: Its signature, annotations and name survive without becoming async.
    """

    def original(value: int, *, label: str = "safe") -> tuple[int, str]:
        """Keep defaults and annotations visible to introspection."""
        return value, label

    wrapped = pnl_read_core(original)
    assert inspect.signature(wrapped) == inspect.signature(original)
    assert wrapped.__annotations__ == original.__annotations__
    assert wrapped.__name__ == original.__name__
    assert not inspect.iscoroutinefunction(wrapped)


async def test_invalid_core_span_cannot_be_hidden_by_later_valid_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Accumulation retains a clock anomaly instead of averaging it away.

    Given: Two core calls whose first clock interval is invalid.
    When: A later core call has a valid positive duration.
    Then: The accumulated core observation remains explicitly invalid.
    """
    messages = _capture(monkeypatch)
    _clock(monkeypatch, [0, 5_000_000, 4_000_000, 6_000_000, 7_000_000, 10_000_000])

    @pnl_read_core
    def core() -> None:
        """Perform two distinct observed calls without financial state."""

    @observe_pnl_read("series")
    async def handler() -> None:
        """Accumulate both calls inside the same request collector."""
        core()
        core()

    await handler()
    assert "handler_ms=10.000" in messages[0]
    assert "core_ms=invalid" in messages[0]


@pytest.mark.parametrize("invalid_stage", ["prefix", "series", "timeline"])
async def test_invalid_component_never_becomes_valid_timeline_extra(
    monkeypatch: pytest.MonkeyPatch,
    invalid_stage: Literal["prefix", "series", "timeline"],
) -> None:
    """A bad constituent interval makes the derived interval explicitly invalid.

    Given: An invalid prefix, series or parent timeline interval.
    When: The handler derives the timeline's remaining work duration.
    Then: A successful response retains an invalid timing disclosure.
    """
    messages = _capture(monkeypatch)
    readings = [0, 0, 1_000_000, 2_000_000, 3_000_000, 4_000_000, 5_000_000, 6_000_000]
    invalid_index = {"prefix": 3, "series": 5, "timeline": 6}[invalid_stage]
    readings[invalid_index] = -1
    _clock(monkeypatch, readings)

    @pnl_read_stage("prefix")
    async def prefix() -> None:
        """Measure the preloaded shared prefix."""

    @pnl_read_stage("series")
    async def series() -> None:
        """Measure the inclusive shared series."""

    @pnl_read_stage("timeline")
    async def timeline() -> None:
        """Keep all constituents inside one parent interval."""
        await prefix()
        await series()

    @observe_pnl_read("timeline")
    async def handler() -> None:
        """Preserve a successful response despite invalid observation evidence."""
        await timeline()

    await handler()
    assert "timeline_extra_ms=invalid" in messages[0]
    assert "outcome=success" in messages[0]


@pytest.mark.parametrize("include_series", [False, True])
async def test_partial_timeline_does_not_invent_unobserved_intervals(
    monkeypatch: pytest.MonkeyPatch,
    include_series: bool,
) -> None:
    """A missing prefix or series remains unobserved instead of measured zero.

    Given: A timeline that enters only some of the measured shared boundaries.
    When: The observed handler completes that partial path.
    Then: Missing constituents and their derivative remain unobserved.
    """
    messages = _capture(monkeypatch)
    _clock(monkeypatch, range(0, 10_000_000, 1_000_000))

    @pnl_read_stage("series")
    async def series() -> None:
        """Optionally supply the shared-series interval."""

    @pnl_read_stage("timeline")
    async def timeline() -> None:
        """Return without claiming that a prefix was measured."""
        if include_series:
            await series()

    @observe_pnl_read("timeline")
    async def handler() -> None:
        """Expose only stages actually entered by the request."""
        await timeline()

    await handler()
    assert "prefix_ms=unobserved" in messages[0]
    assert "timeline_extra_ms=unobserved" in messages[0]


async def test_core_failure_and_failed_log_preserve_original_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Observation cleanup cannot replace a synchronous construction error.

    Given: A core failure and an observation sink that also raises.
    When: Both failures unwind through an observed handler.
    Then: The original core exception propagates and request state resets.
    """
    _clock(monkeypatch, [0, 1_000_000, 2_000_000, 3_000_000])
    failure = ValueError("private-core-failure-sentinel")

    def fail_log(_message: str, *_args: object) -> None:
        """Model a failed sink during exceptional unwinding."""
        raise RuntimeError("sink unavailable")

    monkeypatch.setattr(pnl_read_timing.logger, "info", fail_log)

    @pnl_read_core
    def core() -> None:
        """Raise the original error object through the synchronous wrapper."""
        raise failure

    @observe_pnl_read("series")
    async def handler() -> None:
        """Keep the original failure as the handler outcome."""
        core()

    with pytest.raises(ValueError) as rejected:
        await handler()
    assert rejected.value is failure
    assert pnl_read_timing._current_timing.get() is None
