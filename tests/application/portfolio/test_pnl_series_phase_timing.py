"""Observe real series phases without changing financial outputs or repository reads."""

import asyncio
from itertools import count
from typing import cast
from unittest.mock import AsyncMock

import pytest

from snapper.application.portfolio import pnl_read_timing
from snapper.application.portfolio.pnl_read_timing import observe_pnl_read
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReadPolicy
from snapper.application.portfolio.pnl_timeline_service import PnlWalletSeriesResult
from snapper.application.portfolio.pnl_timeline_service import build_wallet_pnl_series
from snapper.data.repository import Repository
from tests.application.portfolio.test_pnl_timeline_service import _I1
from tests.application.portfolio.test_pnl_timeline_service import _T0
from tests.application.portfolio.test_pnl_timeline_service import _W1
from tests.application.portfolio.test_pnl_timeline_service import FakeRepo
from tests.application.portfolio.test_pnl_timeline_service import _accrual_row
from tests.application.portfolio.test_pnl_timeline_service import _candle
from tests.application.portfolio.test_pnl_timeline_service import _empty_anchor
from tests.application.portfolio.test_pnl_timeline_service import _exec_row
from tests.application.portfolio.test_pnl_timeline_service import _fx_row
from tests.application.portfolio.test_pnl_timeline_service import _m
from tests.application.portfolio.test_pnl_timeline_service import _ref

_PHASES = (
    "series_anchor_ms",
    "series_replay_ms",
    "series_market_data_ms",
    "series_requirements_ms",
    "series_valuation_ms",
    "series_overlay_ms",
)


def _capture(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Capture the complete message retained by configured logging sinks."""
    messages: list[str] = []

    def record(message: str, *arguments: object) -> None:
        """Render the logger's fixed positional fields without inspecting payloads."""
        messages.append(message.format(*arguments))

    monkeypatch.setattr(pnl_read_timing.logger, "info", record)
    return messages


def _ticks(monkeypatch: pytest.MonkeyPatch) -> None:
    """Supply deterministic monotonic readings without elapsed-time assertions."""
    readings = count(step=1_000_000)
    monkeypatch.setattr(pnl_read_timing, "perf_counter_ns", lambda: next(readings))


def _fields(message: str) -> dict[str, str]:
    """Parse the actual emitted engineering summary into fixed field names."""
    return dict(part.split("=", 1) for part in message.split()[1:])


def _series_repo() -> FakeRepo:
    """Keep real replay, mark conversion, FX election and accrual valuation active."""
    return FakeRepo(
        executions=[_exec_row(_I1, 1, 0, "buy", 1.0, 100.0, 0.5, "EUR")],
        accruals=[_accrual_row(_I1, 1, 2.0, "EUR")],
        refs=[_ref(_I1, "BTC-EUR", "EUR")],
        candles=[_candle(_m(minute), close) for minute, close in ((-1, 105), (0, 110), (1, 120))],
        fx_rows=[_fx_row("EUR", "USD", minute, 2.0) for minute in range(3)],
    )


def _anchor_creation_repo() -> FakeRepo:
    """Require actual prefix-backed activation and opening EUR-to-USD valuation."""
    return FakeRepo(
        executions=[_exec_row(_I1, 1, 0, "buy", 1.0, 100.0, 0.5, "EUR")],
        refs=[_ref(_I1, "BTC-EUR", "EUR")],
        candles=[_candle(_m(19), 110.0)],
        fx_rows=[_fx_row("EUR", "USD", minute, 2.0) for minute in (0, 20)],
        has_anchor=False,
    )


async def _build(repo: FakeRepo) -> PnlWalletSeriesResult:
    """Exercise an actual live series including its bounded equity sample read."""
    return await build_wallet_pnl_series(
        cast(Repository, repo),
        _W1,
        "live",
        _T0,
        _m(2),
        "1m",
        _m(20),
        policy=PnlSeriesReadPolicy(allow_anchor_creation=False, current_truth=True),
    )


def _read_calls(repo: FakeRepo) -> tuple[object, ...]:
    """Retain read arguments and order within every existing repository boundary."""
    return (
        repo.anchor_calls,
        repo.execution_calls,
        repo.fill_gap_calls,
        repo.lineage_calls,
        repo.accrual_calls,
        repo.symbol_ref_calls,
        repo.candle_calls,
        repo.fx_candidate_pair_calls,
        repo.fx_candidate_range_calls,
        repo.fx_candidate_interval_calls,
        repo.fx_plane_calls,
        repo.fx_range_calls,
        repo.fx_interval_calls,
        repo.sample_calls,
    )


async def test_real_series_phases_exclude_fx_and_include_core_without_changing_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The six actual series boundaries have distinct, non-FX elapsed intervals.

    Given: An instrument needing real mark, fee and accrual FX conversions.
    When: The same series is built outside and inside an observed handler.
    Then: Every phase is measured, core is nested, and outputs and reads agree.
    """
    messages = _capture(monkeypatch)

    def forbidden_clock() -> int:
        """Fail any observation work attempted without an active API collector."""
        pytest.fail("inactive series must not read the timing clock")

    monkeypatch.setattr(pnl_read_timing, "perf_counter_ns", forbidden_clock)
    reference_repo = _series_repo()
    expected = await _build(reference_repo)
    assert messages == []
    _ticks(monkeypatch)
    observed_repo = _series_repo()

    @observe_pnl_read("series")
    async def handler() -> PnlWalletSeriesResult:
        """Leave every original financial and repository operation active."""
        return await _build(observed_repo)

    actual = await handler()
    assert actual == expected
    assert [point.valuation_status for point in actual.points] == ["complete"] * 3
    assert [point.net_pnl for point in actual.points] == [9.0, 15.0, 35.0]
    assert _read_calls(observed_repo) == _read_calls(reference_repo)
    assert len(messages) == 1
    fields = _fields(messages[0])
    assert {phase: fields[phase] for phase in _PHASES} == {
        "series_anchor_ms": "1.000",
        "series_replay_ms": "1.000",
        "series_market_data_ms": "1.000",
        "series_requirements_ms": "1.000",
        "series_valuation_ms": "3.000",
        "series_overlay_ms": "1.000",
    }
    assert fields["core_ms"] == "1.000"
    assert fields["fx_ms"] == "13.000"
    assert fields["series_ms"] == "29.000"
    assert fields["handler_ms"] == "31.000"
    assert all(
        fields[f"{phase}_ms"] == "1.000"
        for phase in (
            "fx_discovery_mark",
            "fx_discovery_event",
            "fx_load_mark",
            "fx_load_event",
            "fx_election",
            "fx_fold_final",
        )
    )
    assert tuple(fields)[-len(_PHASES) :] == _PHASES
    assert pnl_read_timing._current_timing.get() is None


async def test_repeated_real_series_calls_accumulate_each_phase(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repeated series work contributes to the same request's phase totals.

    Given: Two identical real series calls sharing one active timing collector.
    When: Both finish with a deterministic clock.
    Then: Every phase, FX stage and nested core duration sums across the calls.
    """
    messages = _capture(monkeypatch)
    _ticks(monkeypatch)

    @observe_pnl_read("series")
    async def handler() -> None:
        """Repeat unchanged service orchestration without resetting the collector."""
        assert await _build(_series_repo()) == await _build(_series_repo())

    await handler()
    fields = _fields(messages[0])
    assert [fields[phase] for phase in _PHASES] == [
        "2.000",
        "2.000",
        "2.000",
        "2.000",
        "6.000",
        "2.000",
    ]
    assert fields["series_ms"] == "58.000"
    assert fields["fx_ms"] == "26.000"
    assert fields["core_ms"] == "2.000"


async def test_actual_anchor_creation_nests_prefix_and_fx_inside_anchor_phase(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Permitted activation retains inclusive opening work and unchanged results.

    Given: A missing anchor and an actual EUR position requiring opening FX.
    When: The real series builder creates its anchor before a preactivation return.
    Then: Prefix-backed creation and FX remain inside the anchor phase without
        changing financial results, persisted anchor evidence or repository reads.
    """
    messages = _capture(monkeypatch)

    async def build(repo: FakeRepo) -> PnlWalletSeriesResult:
        """Keep genuine anchor derivation, its FX loader and early return active."""
        return await build_wallet_pnl_series(
            cast(Repository, repo),
            _W1,
            "live",
            _T0,
            _m(2),
            "1m",
            _m(20),
            policy=PnlSeriesReadPolicy(allow_anchor_creation=True),
        )

    reference_repo = _anchor_creation_repo()
    expected = await build(reference_repo)
    assert messages == []
    _ticks(monkeypatch)
    observed_repo = _anchor_creation_repo()

    @observe_pnl_read("series")
    async def handler() -> PnlWalletSeriesResult:
        """Observe actual activation without replacing any financial stage."""
        return await build(observed_repo)

    actual = await handler()
    assert actual == expected
    assert len(actual.points) == 3
    assert _read_calls(observed_repo) == _read_calls(reference_repo)
    assert observed_repo.execution_bundle_calls == [(_W1, "live", _m(20), _m(20))]
    assert observed_repo.anchor_write_evidence == reference_repo.anchor_write_evidence
    assert len(observed_repo.anchor_record_calls) == 1
    assert (
        observed_repo.anchor_record_calls[0]["opening_basket_json"]
        == reference_repo.anchor_record_calls[0]["opening_basket_json"]
    )
    assert observed_repo.anchor_record_calls[0]["unrealized_pnl"] == 20.0
    assert observed_repo.fx_plane_calls
    assert len(messages) == 1
    fields = _fields(messages[0])
    assert fields["series_anchor_ms"] == "17.000"
    assert fields["fx_ms"] == "13.000"
    assert fields["core_ms"] == "1.000"
    assert fields["series_ms"] == "19.000"
    assert fields["prefix_ms"] == "unobserved"
    assert all(fields[phase] == "unobserved" for phase in _PHASES[1:])
    assert pnl_read_timing._current_timing.get() is None


@pytest.mark.parametrize("preactivation", [False, True])
async def test_real_series_early_return_leaves_skipped_phases_unobserved(
    monkeypatch: pytest.MonkeyPatch,
    preactivation: bool,
) -> None:
    """An early series result does not fabricate zero-valued later phases.

    Given: A missing anchor or a window wholly before the visible activation.
    When: The real series builder returns its existing empty or unvalued result.
    Then: Anchor work is observed and all later series phases remain unobserved.
    """
    messages = _capture(monkeypatch)
    _ticks(monkeypatch)
    repo = FakeRepo(
        has_anchor=preactivation,
        anchor=_empty_anchor(_W1, "live", "USD", point_time=_m(10)),
    )

    @observe_pnl_read("series")
    async def handler() -> PnlWalletSeriesResult:
        """Keep the original activation short-circuit under observation."""
        return await _build(repo)

    result = await handler()
    assert len(result.points) == (3 if preactivation else 0)
    fields = _fields(messages[0])
    assert fields["series_anchor_ms"] == ("3.000" if preactivation else "1.000")
    assert all(fields[phase] == "unobserved" for phase in _PHASES[1:])
    assert fields["fx_ms"] == "unobserved"
    assert fields["core_ms"] == ("1.000" if preactivation else "unobserved")
    assert repo.execution_calls == []
    assert repo.candle_calls == []
    assert repo.sample_calls == []


@pytest.mark.parametrize("cancelled", [False, True])
async def test_real_series_failure_preserves_exception_and_partial_phases(
    monkeypatch: pytest.MonkeyPatch,
    cancelled: bool,
) -> None:
    """Failed and cancelled replay retains partial observation without taking control.

    Given: An actual replay whose accrual repository read raises an original failure.
    When: That failure crosses the observed series handler.
    Then: Its identity propagates, completed phases remain and later work is absent.
    """
    messages = _capture(monkeypatch)
    _ticks(monkeypatch)
    failure = (
        asyncio.CancelledError("private-replay-token-sentinel")
        if cancelled
        else ValueError("private-replay-token-sentinel")
    )
    repo = _series_repo()
    monkeypatch.setattr(repo, "get_accruals_for_pnl", AsyncMock(side_effect=failure))

    @observe_pnl_read("series")
    async def handler() -> PnlWalletSeriesResult:
        """Let the unchanged repository failure propagate through service cleanup."""
        return await _build(repo)

    with pytest.raises(type(failure)) as rejected:
        await handler()
    assert rejected.value is failure
    fields = _fields(messages[0])
    assert fields["outcome"] == "failure"
    assert fields["series_anchor_ms"] == fields["series_replay_ms"] == "1.000"
    assert all(fields[phase] == "unobserved" for phase in _PHASES[2:])
    assert "private-replay-token-sentinel" not in messages[0]
    assert pnl_read_timing._current_timing.get() is None
