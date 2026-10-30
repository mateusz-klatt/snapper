"""Observe real FX boundaries without changing evidence, payloads or private data."""

import asyncio
from collections.abc import Sized
from itertools import count
from typing import cast
from unittest.mock import AsyncMock

import pytest

from snapper.application.portfolio import pnl_read_timing
from snapper.application.portfolio.fx_conversion_shadow import FxShadowPinContext
from snapper.application.portfolio.fx_conversion_shadow import activate_fx_shadow_context
from snapper.application.portfolio.fx_rates import currency_pair_key
from snapper.application.portfolio.pnl_read_timing import observe_pnl_read
from snapper.application.portfolio.pnl_timeline_service import _load_request_fx_rates
from snapper.application.portfolio.pnl_timeline_service import _sparse_event_fx_intervals
from snapper.data.repository import Repository
from tests.application.portfolio.test_pnl_timeline_service import FakeRepo
from tests.application.portfolio.test_pnl_timeline_service import _fx_row
from tests.application.portfolio.test_pnl_timeline_service import _m

_PAIR = currency_pair_key("EUR", "PLN")
_PLANE = ("EUR", "PLN", "walutomat")


def _capture(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Capture exactly the rendered message preserved by configured sinks."""
    messages: list[str] = []

    def record(message: str, *arguments: object) -> None:
        """Render fixed numeric fields without logger extras or row inspection."""
        messages.append(message.format(*arguments))

    monkeypatch.setattr(pnl_read_timing.logger, "info", record)
    return messages


def _fields(message: str) -> dict[str, str]:
    """Parse the fixed engineering summary without accepting arbitrary payloads."""
    return dict(part.split("=", 1) for part in message.split()[1:])


def _ticks(monkeypatch: pytest.MonkeyPatch) -> None:
    """Provide unlimited deterministic monotonic readings without sleeping."""
    readings = count(step=1_000_000)
    monkeypatch.setattr(pnl_read_timing, "perf_counter_ns", lambda: next(readings))


async def test_real_fx_boundaries_have_distinct_times_and_unchanged_reads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Separate six real service phases without replacing their financial work.

    Given: Real FX discovery, separate mark/event reads and deterministic clocks.
    When: One observed handler reconstructs two conversion requirements.
    Then: One safe summary reports exact boundaries and all evidence calls survive.
    """
    messages = _capture(monkeypatch)
    readings = iter(
        x * 1_000_000 for x in [0, 1, 2, 4, 5, 8, 9, 13, 14, 19, 20, 26, 27, 34, 35, 36]
    )
    monkeypatch.setattr(pnl_read_timing, "perf_counter_ns", lambda: next(readings))
    repo = FakeRepo(
        fx_rows=[
            _fx_row("EUR", "PLN", 10, 4.0, "walutomat"),
            _fx_row("EUR", "PLN", -50, 5.0, "walutomat"),
        ]
    )
    expected = await _load_request_fx_rates(
        cast(Repository, FakeRepo(fx_rows=list(repo._fx_rows))),
        {"private-owner-sentinel": {_PAIR: {_m(10)}}},
        {"private-owner-sentinel": {_PAIR: {_m(-50)}}},
        {},
        _m(200),
        "EUR",
    )

    @observe_pnl_read("series")
    async def handler() -> object:
        """Leave the actual loader, election and final fold fully active."""
        return await _load_request_fx_rates(
            cast(Repository, repo),
            {"private-owner-sentinel": {_PAIR: {_m(10)}}},
            {"private-owner-sentinel": {_PAIR: {_m(-50)}}},
            {},
            _m(200),
            "EUR",
        )

    result = await handler()
    assert len(messages) == 1
    fields = dict(part.split("=", 1) for part in messages[0].split()[1:])
    assert fields["fx_discovery_mark_ms"] == "2.000"
    assert fields["fx_discovery_event_ms"] == "3.000"
    assert fields["fx_load_mark_ms"] == "4.000"
    assert fields["fx_load_event_ms"] == "5.000"
    assert fields["fx_election_ms"] == "6.000"
    assert fields["fx_fold_final_ms"] == "7.000"
    assert fields["fx_ms"] == "34.000"
    assert fields["handler_ms"] == "36.000"
    assert fields["fx_mark_candidate_planes"] == "1"
    assert fields["fx_event_candidate_planes"] == "1"
    assert fields["fx_mark_rows"] == "1"
    assert fields["fx_event_rows"] == "1"
    assert fields["fx_selected_rows"] == "2"
    assert fields["fx_event_read_mode"] == "sparse"
    assert fields["fx_event_intervals_observed"] == "2"
    assert repo.fx_candidate_pair_calls == [[("EUR", "PLN"), ("PLN", "EUR")]] * 2
    assert repo.fx_candidate_range_calls == [(_m(-11), _m(10)), (_m(-71), _m(-50))]
    assert repo.fx_range_calls == [(_m(-11), _m(10)), (_m(-71), _m(-50))]
    assert repo.fx_plane_calls == [[_PLANE], [_PLANE]]
    assert result == expected
    assert "private-owner-sentinel" not in messages[0]
    assert "EUR" not in messages[0] and "PLN" not in messages[0]
    assert pnl_read_timing._current_timing.get() is None


async def test_repeated_real_loads_sum_counts_and_keep_skipped_discovery_unobserved(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repeated work retains every observed row rather than the last call only.

    Given: General FX work followed by an owned-only mark and empty requirements.
    When: One handler performs those actual loads with shared observation state.
    Then: Counts accumulate, skipped discovery contributes nothing and modes mix.
    """
    messages = _capture(monkeypatch)
    _ticks(monkeypatch)
    repo = FakeRepo(fx_rows=[_fx_row("EUR", "PLN", 10, 4.0), _fx_row("EUR", "PLN", -50, 5.0)])

    @observe_pnl_read("series")
    async def handler() -> None:
        """Call the unchanged service three times inside one request collector."""
        await _load_request_fx_rates(
            cast(Repository, repo),
            {"owner": {_PAIR: {_m(10)}}},
            {"owner": {_PAIR: {_m(-50)}}},
            {},
            _m(200),
            "EUR",
        )
        await _load_request_fx_rates(
            cast(Repository, repo),
            {"owner": {_PAIR: {_m(10)}}},
            {},
            {"owner": ("EUR", "PLN", "kraken")},
            _m(200),
            "EUR",
        )
        await _load_request_fx_rates(cast(Repository, repo), {}, {}, {}, _m(200), "EUR")

    await handler()
    fields = _fields(messages[0])
    assert fields["fx_mark_candidate_planes"] == "1"
    assert fields["fx_event_candidate_planes"] == "1"
    assert fields["fx_mark_rows"] == "2"
    assert fields["fx_event_rows"] == "1"
    assert fields["fx_selected_rows"] == "3"
    assert fields["fx_event_read_mode"] == "mixed"
    assert fields["fx_event_intervals_observed"] == "2"
    assert len(repo.fx_candidate_range_calls) == 2
    assert len(repo.fx_range_calls) == 3


@pytest.mark.parametrize("minutes", [0, 1, 2, 16, 17])
async def test_actual_selector_records_only_constructed_interval_prefixes(
    monkeypatch: pytest.MonkeyPatch,
    minutes: int,
) -> None:
    """Actual sparse selection reports its bounded observations faithfully.

    Given: Zero, dense, sparse, threshold or fragmented conversion requirements.
    When: An observed handler invokes the real interval selector twice.
    Then: The original return survives and only constructed prefixes are summed.
    """
    messages = _capture(monkeypatch)
    _ticks(monkeypatch)

    @observe_pnl_read("series")
    async def handler() -> None:
        """Exercise the real threshold and its existing early return."""
        requirements = {_PAIR: {_m(index * 60) for index in range(minutes)}}
        first = _sparse_event_fx_intervals(requirements)
        second = _sparse_event_fx_intervals(requirements)
        assert first == second
        assert len(first) == (minutes if 1 < minutes <= 16 else 0)

    await handler()
    fields = _fields(messages[0])
    expected = {0: "empty_requirements", 1: "single_interval_dense", 17: "interval_limit_dense"}
    assert fields["fx_event_read_mode"] == expected.get(minutes, "sparse")
    assert fields["fx_event_intervals_observed"] == str(minutes * 2)


@pytest.mark.parametrize("with_sparse", [False, True])
async def test_shadow_skips_interval_count_and_mixed_mode_is_absorbing(
    monkeypatch: pytest.MonkeyPatch,
    with_sparse: bool,
) -> None:
    """Skipped selectors contribute no fabricated interval cardinality.

    Given: Real sparse selection, shadow bypass and sparse selection again.
    When: One request observes those modes in order.
    Then: Mixed remains mixed and only the two actual constructions contribute.
    """
    messages = _capture(monkeypatch)
    _ticks(monkeypatch)

    @observe_pnl_read("series")
    async def handler() -> None:
        """Preserve shadow's original dense return without reconstructing its union."""
        requirements = {_PAIR: {_m(0), _m(60)}}
        if with_sparse:
            assert len(_sparse_event_fx_intervals(requirements)) == 2
        context = FxShadowPinContext(calculation_version="5B.2", evaluations=[])
        with activate_fx_shadow_context(context):
            assert _sparse_event_fx_intervals(requirements) == ()
        if with_sparse:
            assert len(_sparse_event_fx_intervals(requirements)) == 2

    await handler()
    fields = _fields(messages[0])
    assert fields["fx_event_read_mode"] == ("mixed" if with_sparse else "shadow_dense")
    assert fields["fx_event_intervals_observed"] == ("4" if with_sparse else "unobserved")


class _FailingSize:
    """Make any additional background cardinality observation visible."""

    def __len__(self) -> int:
        """Refuse observation with a private exception sentinel."""
        raise ValueError("private-row-url-token-sentinel")


def test_inactive_collector_does_not_read_clock_or_len(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Background calls retain their existing metadata and clock work.

    Given: A missing API collector and a sequence whose length is forbidden.
    When: Every new observation entry point is reached.
    Then: No length, clock or logging work occurs and caller execution survives.
    """
    messages = _capture(monkeypatch)
    clock_calls: list[bool] = []

    def forbidden_clock() -> int:
        """Record any forbidden inactive clock read before failing it."""
        clock_calls.append(True)
        raise ValueError("inactive clock must not be read")

    monkeypatch.setattr(pnl_read_timing, "perf_counter_ns", forbidden_clock)
    values: Sized = _FailingSize()
    pnl_read_timing.pnl_read_count("fx_mark_rows", values)
    pnl_read_timing.pnl_read_fx_window("sparse", values)
    with pnl_read_timing.pnl_read_phase("fx_load_mark"):
        pass
    assert messages == []
    assert clock_calls == []


async def test_count_failure_stays_invalid_and_does_not_change_handler_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Observation failures never replace a successful result or invent a zero.

    Given: A failing cardinality observation followed by a valid zero.
    When: Both observations share one handler.
    Then: Invalid is retained, untouched counts stay unobserved and payload survives.
    """
    messages = _capture(monkeypatch)
    _ticks(monkeypatch)
    value = object()

    @observe_pnl_read("series")
    async def handler() -> object:
        """Preserve the exact caller-owned payload despite observation failure."""
        pnl_read_timing.pnl_read_count("fx_mark_rows", _FailingSize())
        pnl_read_timing.pnl_read_count("fx_mark_rows", [])
        pnl_read_timing.pnl_read_count("fx_selected_rows", [])
        pnl_read_timing.pnl_read_fx_window("constructed", _FailingSize())
        pnl_read_timing.pnl_read_fx_window("constructed")
        return value

    assert await handler() is value
    fields = _fields(messages[0])
    assert fields["fx_mark_rows"] == "invalid"
    assert fields["fx_selected_rows"] == "0"
    assert fields["fx_event_rows"] == "unobserved"
    assert fields["fx_event_read_mode"] == "unobserved"
    assert fields["fx_event_intervals_observed"] == "invalid"
    assert "private-row-url-token-sentinel" not in messages[0]


@pytest.mark.parametrize("cancelled", [False, True])
async def test_failure_after_first_discovery_preserves_partial_counts_and_exception(
    monkeypatch: pytest.MonkeyPatch,
    cancelled: bool,
) -> None:
    """Interrupted work records completed observations without consuming failure.

    Given: A real loader whose second discovery raises an original exception.
    When: That operation fails or is cancelled inside an observed request.
    Then: Prior counts remain, unfinished counts stay absent and identity propagates.
    """
    messages = _capture(monkeypatch)
    _ticks(monkeypatch)
    failure = (
        asyncio.CancelledError("private-token-sentinel")
        if cancelled
        else ValueError("private-token-sentinel")
    )
    repo = FakeRepo(
        fx_rows=[
            _fx_row("EUR", "PLN", 10, 4.0, "walutomat"),
            _fx_row("EUR", "PLN", -50, 5.0, "walutomat"),
        ]
    )
    monkeypatch.setattr(
        repo,
        "get_pnl_fx_rate_exchanges",
        AsyncMock(side_effect=[[_PLANE], [_PLANE], [_PLANE], failure]),
    )

    @observe_pnl_read("series")
    async def handler() -> None:
        """Allow the original failure through the unchanged service boundary."""
        for _ in range(2):
            await _load_request_fx_rates(
                cast(Repository, repo),
                {"owner": {_PAIR: {_m(10)}}},
                {"owner": {_PAIR: {_m(-50)}}},
                {},
                _m(200),
                "EUR",
            )

    with pytest.raises(type(failure)) as rejected:
        await handler()
    assert rejected.value is failure
    fields = _fields(messages[0])
    assert fields["outcome"] == "failure"
    assert fields["fx_mark_candidate_planes"] == "2"
    assert fields["fx_event_candidate_planes"] == "1"
    assert fields["fx_mark_rows"] == "1"
    assert fields["fx_selected_rows"] == "2"
    assert fields["fx_discovery_event_ms"] == "2.000"
    assert "private-token-sentinel" not in messages[0]
    assert pnl_read_timing._current_timing.get() is None


@pytest.mark.parametrize("owned", [False, True])
async def test_skipped_selector_counts_stay_unobserved(
    monkeypatch: pytest.MonkeyPatch,
    owned: bool,
) -> None:
    """Identity-only and missing-plane paths do not invent selector observations.

    Given: Either one canonical mark or requirements with no candidate plane.
    When: The actual loader skips discovery or cannot reach event selection.
    Then: Skipped metadata remains absent while real empty row counts are zero.
    """
    messages = _capture(monkeypatch)
    _ticks(monkeypatch)
    repo = FakeRepo(fx_rows=[_fx_row("EUR", "PLN", 10, 4.0)] if owned else [])

    @observe_pnl_read("series")
    async def handler() -> object:
        """Keep all early-return conditions and actual evidence calls active."""
        return await _load_request_fx_rates(
            cast(Repository, repo),
            {"owner": {_PAIR: {_m(10)}}},
            {} if owned else {"owner": {_PAIR: {_m(-50)}}},
            {"owner": ("EUR", "PLN", "kraken")} if owned else {},
            _m(200),
            "EUR",
        )

    await handler()
    fields = _fields(messages[0])
    assert fields["fx_mark_candidate_planes"] == ("unobserved" if owned else "0")
    assert fields["fx_event_candidate_planes"] == ("unobserved" if owned else "0")
    assert fields["fx_event_read_mode"] == ("no_requirements" if owned else "no_candidate_planes")
    assert fields["fx_event_intervals_observed"] == "unobserved"
    assert fields["fx_event_rows"] == "0"


async def test_concurrent_handlers_keep_phase_counts_and_modes_independent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Concurrent requests cannot accumulate into one another's collector.

    Given: Two handlers with different cardinalities and read modes.
    When: Their measured phases deliberately interleave.
    Then: Each single summary retains only its own fixed engineering metadata.
    """
    messages = _capture(monkeypatch)
    _ticks(monkeypatch)

    @observe_pnl_read("series")
    async def handler(size: int) -> int:
        """Interleave without introducing any request identifier into observation."""
        with pnl_read_timing.pnl_read_phase("fx_load_mark"):
            await asyncio.sleep(0)
        pnl_read_timing.pnl_read_count("fx_mark_rows", list(range(size)))
        pnl_read_timing.pnl_read_fx_window("sparse" if size == 2 else "single_interval_dense", [])
        return size

    assert await asyncio.gather(handler(2), handler(5)) == [2, 5]
    fields = [_fields(message) for message in messages]
    assert [field["fx_mark_rows"] for field in fields] == ["2", "5"]
    assert [field["fx_event_read_mode"] for field in fields] == ["sparse", "single_interval_dense"]
    assert all(field["fx_event_intervals_observed"] == "0" for field in fields)
    assert pnl_read_timing._current_timing.get() is None
