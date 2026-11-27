"""Unit tests for the periodic market snapshot refresher process."""

import asyncio
from collections.abc import Awaitable
from collections.abc import Callable
from types import ModuleType
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from loguru import logger
from pydantic import ValidationError

from snapper.application.process_manager.process_parameters import MarketSnapshotRefresherParameters
from snapper.application.process_manager.registry import get_registered_processes
from snapper.application.updaters import market_snapshots as refresher_module
from snapper.application.updaters.market_snapshots import DEFAULT_INTERVAL_SECONDS
from snapper.application.updaters.market_snapshots import MarketSnapshotRefresherProcess
from snapper.core.types import ProcessLifecycleEnum
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRestartPolicyEnum
from snapper.core.types import ProcessRoleEnum
from snapper.infrastructure.market_data import kraken as kraken_market_data
from snapper.infrastructure.market_data import kraken_equities as kraken_equities_market_data
from snapper.infrastructure.market_data import kraken_futures as kraken_futures_market_data
from snapper.infrastructure.market_data import walutomat as walutomat_market_data


async def _wait_until_sleeping(process: MarketSnapshotRefresherProcess) -> None:
    """Yield control until the refresher enters its interval sleep task."""
    for _ in range(100):
        if process._sleep_task is not None:
            return
        await asyncio.sleep(0)
    raise AssertionError("Refresher did not enter interval sleep")


@pytest.mark.parametrize(
    ("interval_seconds", "expected"),
    [
        (None, 120.0),
        (30, 30.0),
        (30.0, 30.0),
        (120, 120.0),
        (290, 290.0),
        (290.0, 290.0),
    ],
)
def test_refresher_parameters_accepts_default_and_valid_bounds(
    interval_seconds: int | float | None,
    expected: float,
) -> None:
    """Validate default and inclusive 30..290s bounds for refresher interval.

    Given: Default or boundary interval values within ``[30, 290]`` seconds.
    When: ``MarketSnapshotRefresherParameters`` is constructed.
    Then: Validation succeeds and coerces valid integers and floats cleanly.
    """
    params = (
        MarketSnapshotRefresherParameters()
        if interval_seconds is None
        else MarketSnapshotRefresherParameters(interval_seconds=interval_seconds)
    )
    assert params.interval_seconds == expected


@pytest.mark.parametrize(
    "invalid_interval",
    [
        29,
        29.99,
        0,
        -5,
        290.01,
        291,
        300,
        float("nan"),
        float("inf"),
        float("-inf"),
        "120",
        True,
    ],
)
def test_refresher_parameters_rejects_invalid_intervals(invalid_interval: object) -> None:
    """Reject out-of-range, non-finite, and wrong-type interval values.

    Given: Interval values below 30 s, above 290 s, non-finite, or wrong type.
    When: ``MarketSnapshotRefresherParameters.model_validate`` runs in strict mode.
    Then: ``ValidationError`` is raised.
    """
    with pytest.raises(ValidationError):
        MarketSnapshotRefresherParameters.model_validate({"interval_seconds": invalid_interval})


def test_refresher_parameters_forbids_extra_fields() -> None:
    """Reject unexpected keys on the strict refresher parameter schema.

    Given: A payload containing an unknown extra parameter field.
    When: ``MarketSnapshotRefresherParameters.model_validate`` runs.
    Then: ``ValidationError`` is raised by ``StrictBody``.
    """
    with pytest.raises(ValidationError):
        MarketSnapshotRefresherParameters.model_validate(
            {"interval_seconds": 120, "extra_field": "disallowed"}
        )


def test_process_registration_metadata_and_defaults() -> None:
    """Verify process registry entry, defaults, and initial status metadata.

    Given: The global process registry after importing ``market_snapshots``.
    When: The ``market_snapshot_refresher`` entry and defaults are inspected.
    Then: Metadata matches disabled long-running CORE config with parameter schema.
    """
    registry = get_registered_processes()
    assert "market_snapshot_refresher" in registry
    entry = registry["market_snapshot_refresher"]
    assert entry.cls is MarketSnapshotRefresherProcess
    assert entry.method == "start"
    assert entry.lifecycle is ProcessLifecycleEnum.LONG_RUNNING
    assert entry.role is ProcessRoleEnum.CORE
    assert entry.enabled is False
    assert entry.mode is ProcessModeEnum.THREAD
    assert entry.restart_policy is ProcessRestartPolicyEnum.ON_FAILURE
    assert entry.parameters_model is MarketSnapshotRefresherParameters
    assert entry.parameters_schema == MarketSnapshotRefresherParameters.model_json_schema()
    assert set(entry.tags) == {"market-data", "snapshots", "maintenance"}
    assert "publisher" not in entry.tags

    defaults = MarketSnapshotRefresherProcess.get_default_parameters(MagicMock())
    assert defaults == {"interval_seconds": DEFAULT_INTERVAL_SECONDS}

    process = MarketSnapshotRefresherProcess()
    assert process.get_status() == {
        "running": False,
        "interval_seconds": DEFAULT_INTERVAL_SECONDS,
        "tick_count": 0,
    }


@pytest.mark.asyncio
async def test_construction_makes_no_updater_or_network_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure construction touches no exchange updaters or network resources.

    Given: All four venue snapshot updaters replaced by failing stubs.
    When: ``MarketSnapshotRefresherProcess`` is instantiated and inspected.
    Then: None of the venue updaters or network paths are invoked before running.
    """
    fail_stub = AsyncMock(side_effect=AssertionError("must not run on import or init"))
    monkeypatch.setattr(kraken_market_data, "async_update_snapshots", fail_stub)
    monkeypatch.setattr(kraken_futures_market_data, "async_update_snapshots", fail_stub)
    monkeypatch.setattr(kraken_equities_market_data, "async_update_snapshots", fail_stub)
    monkeypatch.setattr(walutomat_market_data, "async_update_snapshots", fail_stub)

    process = MarketSnapshotRefresherProcess(interval_seconds=60)
    assert process.get_status()["interval_seconds"] == 60.0
    assert fail_stub.await_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("venue_module", "async_entrypoint"),
    [
        (kraken_market_data, kraken_market_data.async_update_snapshots),
        (kraken_futures_market_data, kraken_futures_market_data.async_update_snapshots),
        (kraken_equities_market_data, kraken_equities_market_data.async_update_snapshots),
        (walutomat_market_data, walutomat_market_data.async_update_snapshots),
    ],
)
async def test_venue_async_update_snapshots_delegates_to_async_core(
    monkeypatch: pytest.MonkeyPatch,
    venue_module: ModuleType,
    async_entrypoint: Callable[[], Awaitable[None]],
) -> None:
    """Verify public async snapshot wrapper delegates cleanly to async core.

    Given: A supported market-data module with ``_async_update_snapshots`` stubbed.
    When: ``async_update_snapshots`` is awaited inside the active event loop.
    Then: The underlying coroutine is awaited exactly once.
    """
    inner_mock = AsyncMock()
    monkeypatch.setattr(venue_module, "_async_update_snapshots", inner_mock)
    await async_entrypoint()
    inner_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_run_once_invokes_every_default_venue_updater_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify a single tick calls every covered public venue updater once.

    Given: All four venue ``async_update_snapshots`` entrypoints monkeypatched.
    When: ``run_once`` executes one refresh tick.
    Then: Each of Kraken, Kraken Futures, Kraken Equities, and Walutomat runs
        exactly once and ``tick_count`` increments to 1.
    """
    calls: list[str] = []

    def _recorder(name: str) -> AsyncMock:
        async def _run() -> None:
            calls.append(name)

        return AsyncMock(side_effect=_run)

    kraken_mock = _recorder("kraken")
    futures_mock = _recorder("kraken_futures")
    equities_mock = _recorder("kraken_equities")
    walutomat_mock = _recorder("walutomat")

    monkeypatch.setattr(kraken_market_data, "async_update_snapshots", kraken_mock)
    monkeypatch.setattr(kraken_futures_market_data, "async_update_snapshots", futures_mock)
    monkeypatch.setattr(kraken_equities_market_data, "async_update_snapshots", equities_mock)
    monkeypatch.setattr(walutomat_market_data, "async_update_snapshots", walutomat_mock)

    process = MarketSnapshotRefresherProcess()
    await process.run_once()

    assert calls == ["kraken", "kraken_futures", "kraken_equities", "walutomat"]
    assert kraken_mock.await_count == 1
    assert futures_mock.await_count == 1
    assert equities_mock.await_count == 1
    assert walutomat_mock.await_count == 1
    assert process.get_status()["tick_count"] == 1


@pytest.mark.asyncio
async def test_run_once_logs_warning_and_continues_when_one_venue_fails() -> None:
    """Ensure a failing venue logs WARNING with exception type without stopping others.

    Given: Four injected venue updaters where the second raises ``RuntimeError``.
    When: ``run_once`` executes a refresh tick with a Loguru WARNING sink attached.
    Then: A WARNING containing the venue name and ``RuntimeError`` is logged and
        all other venue updaters still execute once.
    """
    executed: list[str] = []
    warnings: list[str] = []
    sink_id = logger.add(lambda msg: warnings.append(str(msg)), level="WARNING")

    async def _ok_kraken() -> None:
        executed.append("kraken")

    async def _failing_futures() -> None:
        raise RuntimeError("public ticker timeout")

    async def _ok_equities() -> None:
        executed.append("kraken_equities")

    async def _ok_walutomat() -> None:
        executed.append("walutomat")

    process = MarketSnapshotRefresherProcess(
        venue_updaters=(
            ("kraken", _ok_kraken),
            ("kraken_futures", _failing_futures),
            ("kraken_equities", _ok_equities),
            ("walutomat", _ok_walutomat),
        )
    )
    try:
        await process.run_once()
    finally:
        logger.remove(sink_id)

    assert executed == ["kraken", "kraken_equities", "walutomat"]
    assert process.get_status()["tick_count"] == 1
    assert any("kraken_futures" in entry and "RuntimeError" in entry for entry in warnings)


@pytest.mark.asyncio
async def test_loop_sleeps_remaining_interval_non_overlapping_and_stops(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify loop sleeps remaining interval cleanly, clamps overruns, never overlaps.

    Given: A 120-second refresher with a fake monotonic clock and fake sleep,
        where tick 1 takes 15 s and tick 2 takes 150 s (an overrun).
    When: ``start`` runs two ticks and ``stop`` is signalled on the second sleep.
    Then: Requested sleeps are ``105.0`` s and ``0.0`` s, ticks never overlap,
        and the loop stops cleanly after tick 2.
    """
    clock = [1000.0]
    tick_durations = iter((15.0, 150.0))
    sleeps: list[float] = []
    active = 0
    max_concurrent = 0
    real_sleep = asyncio.sleep

    async def fake_updater() -> None:
        await real_sleep(0)

    process = MarketSnapshotRefresherProcess(
        interval_seconds=120.0,
        venue_updaters=(("kraken", fake_updater),),
    )

    original_run_once = process.run_once

    async def timed_run_once() -> None:
        nonlocal active, max_concurrent
        assert active == 0
        active += 1
        max_concurrent = max(max_concurrent, active)
        try:
            await original_run_once()
            clock[0] += next(tick_durations)
        finally:
            active -= 1

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)
        clock[0] += delay
        if len(sleeps) == 2:
            await process.stop()

    monkeypatch.setattr(refresher_module, "monotonic", lambda: clock[0])
    monkeypatch.setattr(refresher_module.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(process, "run_once", timed_run_once)

    await process.start()

    assert sleeps == [105.0, 0.0]
    assert max_concurrent == 1
    assert process.get_status() == {
        "running": False,
        "interval_seconds": 120.0,
        "tick_count": 2,
    }


@pytest.mark.asyncio
async def test_start_stops_cleanly_on_task_cancellation_during_sleep() -> None:
    """Ensure external task cancellation cancels the sleep and exits cleanly.

    Given: A running refresher whose first tick finishes and enters interval sleep.
    When: The outer ``start`` task is cancelled.
    Then: ``asyncio.CancelledError`` propagates, ``running`` resets to False,
        ``_sleep_task`` clears, and no second tick executes.
    """
    tick_done = asyncio.Event()
    calls = 0

    async def _updater() -> None:
        nonlocal calls
        calls += 1
        tick_done.set()

    process = MarketSnapshotRefresherProcess(
        interval_seconds=120.0,
        venue_updaters=(("kraken", _updater),),
    )
    task = asyncio.create_task(process.start())
    await tick_done.wait()
    await _wait_until_sleeping(process)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert calls == 1
    assert process._sleep_task is None
    assert process.get_status()["running"] is False


@pytest.mark.asyncio
async def test_start_propagates_cancellation_raised_inside_venue_updater() -> None:
    """Ensure cancellation during a venue call propagates without logging a warning.

    Given: A venue updater raising ``asyncio.CancelledError``.
    When: ``start`` runs the first tick.
    Then: ``CancelledError`` propagates cleanly without WARNING logs and
        ``running`` resets to False.
    """
    warnings: list[str] = []
    sink_id = logger.add(lambda msg: warnings.append(str(msg)), level="WARNING")

    async def _cancelling_updater() -> None:
        raise asyncio.CancelledError

    process = MarketSnapshotRefresherProcess(
        interval_seconds=60.0,
        venue_updaters=(("kraken", _cancelling_updater),),
    )
    try:
        with pytest.raises(asyncio.CancelledError):
            await process.start()
    finally:
        logger.remove(sink_id)

    assert warnings == []
    assert process.get_status()["running"] is False
    assert process.get_status()["tick_count"] == 0


@pytest.mark.asyncio
async def test_stop_wakes_sleeping_loop_and_is_idempotent_when_idle() -> None:
    """Verify stop works both while sleeping and when already idle.

    Given: An idle refresher, then one actively sleeping between ticks.
    When: ``stop`` is awaited while idle and later while sleeping.
    Then: Idle stop is a safe no-op and active sleep wakes immediately so ``start`` exits.
    """
    tick_done = asyncio.Event()

    async def _updater() -> None:
        tick_done.set()

    process = MarketSnapshotRefresherProcess(
        interval_seconds=120.0,
        venue_updaters=(("kraken", _updater),),
    )
    await process.stop()
    assert process.get_status()["running"] is False

    task = asyncio.create_task(process.start())
    await tick_done.wait()
    await _wait_until_sleeping(process)

    await process.stop()
    await asyncio.wait_for(task, timeout=1.0)

    assert process.get_status() == {
        "running": False,
        "interval_seconds": 120.0,
        "tick_count": 1,
    }


@pytest.mark.asyncio
async def test_start_ignores_duplicate_start_and_exits_when_stopped_mid_tick() -> None:
    """Ensure concurrent start returns immediately and stop mid-tick skips sleep.

    Given: A running refresher paused inside its first venue tick.
    When: A second ``start`` call arrives and ``stop`` is called before the tick finishes.
    Then: The duplicate ``start`` returns immediately and the primary loop exits after the tick.
    """
    tick_started = asyncio.Event()
    allow_finish = asyncio.Event()

    async def _gated_updater() -> None:
        tick_started.set()
        await allow_finish.wait()

    process = MarketSnapshotRefresherProcess(
        interval_seconds=120.0,
        venue_updaters=(("kraken", _gated_updater),),
    )
    task = asyncio.create_task(process.start())
    await tick_started.wait()

    await process.start()
    await process.stop()
    allow_finish.set()
    await asyncio.wait_for(task, timeout=1.0)

    assert process.get_status() == {
        "running": False,
        "interval_seconds": 120.0,
        "tick_count": 1,
    }
