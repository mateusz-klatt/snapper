"""Tests for the incremental trade-integrity watchdog."""

import asyncio
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import call

import pytest

from snapper.application.data_quality import trade_integrity as monitor_module
from snapper.application.data_quality.trade_integrity import DEFAULT_INTERVAL_SECONDS
from snapper.application.data_quality.trade_integrity import DEFAULT_MONITOR_TIMEOUT_SECONDS
from snapper.application.data_quality.trade_integrity import DEFAULT_STARTUP_GRACE_SECONDS
from snapper.application.data_quality.trade_integrity import DEFAULT_SWEEP_LIMIT
from snapper.application.data_quality.trade_integrity import DEFAULT_WORKLOG_LIMIT
from snapper.application.data_quality.trade_integrity import ENV_VARS
from snapper.application.data_quality.trade_integrity import MAX_INTERVAL_SECONDS
from snapper.application.data_quality.trade_integrity import MIN_INTERVAL_SECONDS
from snapper.application.data_quality.trade_integrity import TradeIntegrityMonitorConfig
from snapper.application.data_quality.trade_integrity import TradeIntegrityRepository
from snapper.application.data_quality.trade_integrity import TradeIntegrityWatchdog
from snapper.application.data_quality.trade_integrity import _resolve_interval
from snapper.core.types import HealthStatusEnum
from snapper.data.repository_types import TradeIntegrityFinding
from snapper.data.repository_types import TradeIntegrityMonitor
from snapper.data.repository_types import TradeIntegrityRunResult
from snapper.messaging.schemas.data import HeartbeatData

_NOW = datetime(2026, 7, 29, 12, 0, tzinfo=UTC)


class _TrackerStub:
    """Minimal sequence tracker satisfying the publisher protocol."""

    def __init__(self) -> None:
        """Start transport sequences at zero."""
        self.session_id = "session-trade-integrity"
        self.streams: list[str] = []
        self._sequence = 0

    def next_sequence(self, stream: str) -> int:
        """Return the next transport sequence for a topic.

        Args:
            stream: Topic being published.

        Returns:
            Next transport sequence.
        """
        self.streams.append(stream)
        self._sequence += 1
        return self._sequence


def _make_publisher() -> MagicMock:
    """Build a publisher with synchronous tracking and async sending."""
    publisher = MagicMock()
    publisher.tracker = _TrackerStub()
    publisher.send = AsyncMock()
    return publisher


def _result(
    monitor: TradeIntegrityMonitor,
    *,
    findings: tuple[TradeIntegrityFinding, ...] = (),
    lagged: bool = False,
    lag_seconds: int = 0,
) -> TradeIntegrityRunResult:
    """Build one deterministic repository result.

    Args:
        monitor: Monitor identifier.
        findings: Latched findings returned by the repository.
        lagged: Whether completed coverage breached its lag budget.
        lag_seconds: Completed-coverage lag.

    Returns:
        Complete bounded monitor result.
    """
    return TradeIntegrityRunResult(
        monitor=monitor,
        findings=findings,
        sweep_rows=17 if monitor == "m1" else 19,
        worklog_rows=3 if monitor == "m1" else 5,
        cursor_timestamp=_NOW - timedelta(minutes=2),
        cursor_id=41 if monitor == "m1" else 43,
        covered_through=_NOW - timedelta(minutes=1),
        lag_seconds=lag_seconds,
        lagged=lagged,
        pass_completed=True,
    )


def _finding(monitor: TradeIntegrityMonitor) -> TradeIntegrityFinding:
    """Build one finding for the selected invariant."""
    return TradeIntegrityFinding(
        monitor=monitor,
        public_id="public-1" if monitor == "m2" else None,
        instrument_public_id="instrument-1" if monitor == "m1" else None,
        trade_id="trade-1" if monitor == "m1" else None,
        expected_executed_at=_NOW if monitor == "m1" else None,
        conflicting_executed_at=_NOW - timedelta(seconds=1) if monitor == "m1" else None,
        active_count=2 if monitor == "m2" else None,
    )


def _make_repo(
    m1_result: TradeIntegrityRunResult,
    m2_result: TradeIntegrityRunResult,
) -> MagicMock:
    """Build a repository returning one result per monitor in order."""
    repo = MagicMock()
    repo.run_trade_integrity_monitor = AsyncMock(side_effect=[m1_result, m2_result])
    return repo


def _make_watchdog(
    repo: MagicMock,
    publisher: MagicMock | None,
    *,
    interval_seconds: float = DEFAULT_INTERVAL_SECONDS,
    startup_grace_seconds: float = DEFAULT_STARTUP_GRACE_SECONDS,
    monitor_timeout_seconds: float = DEFAULT_MONITOR_TIMEOUT_SECONDS,
) -> TradeIntegrityWatchdog:
    """Build a watchdog with explicit bounded configuration."""
    config = TradeIntegrityMonitorConfig(
        interval_seconds=interval_seconds,
        startup_grace_seconds=startup_grace_seconds,
        sweep_limit=DEFAULT_SWEEP_LIMIT,
        worklog_limit=DEFAULT_WORKLOG_LIMIT,
        monitor_timeout_seconds=monitor_timeout_seconds,
    )
    return TradeIntegrityWatchdog(
        repo=cast(TradeIntegrityRepository, repo),
        msg_publisher=publisher,
        config=config,
    )


@pytest.mark.parametrize(
    ("env_value", "expected"),
    [
        (None, DEFAULT_INTERVAL_SECONDS),
        ("invalid", DEFAULT_INTERVAL_SECONDS),
        ("nan", DEFAULT_INTERVAL_SECONDS),
        ("inf", DEFAULT_INTERVAL_SECONDS),
        ("-inf", DEFAULT_INTERVAL_SECONDS),
        ("-1", DEFAULT_INTERVAL_SECONDS),
        ("0", DEFAULT_INTERVAL_SECONDS),
        ("1", MIN_INTERVAL_SECONDS),
        ("30", MIN_INTERVAL_SECONDS),
        ("60", MAX_INTERVAL_SECONDS),
        ("300", MAX_INTERVAL_SECONDS),
        ("600.5", MAX_INTERVAL_SECONDS),
    ],
)
def test_resolve_interval_has_safe_default_and_floor(
    env_value: str | None,
    expected: float,
) -> None:
    """Clamp configured monitor cadence to finite safe bounds.

    Given a missing, malformed, non-finite, non-positive, or out-of-range value,
    When the interval resolver parses that configured value,
    Then it returns the safe default or clamps the cadence to permitted bounds.
    """
    assert _resolve_interval(env_value) == expected


def test_default_config_reads_only_interval_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Build default configuration from the sole runtime cadence setting.

    Given the interval environment variable is set to 45 seconds,
    When a watchdog is constructed without an explicit configuration,
    Then only that variable is allowlisted and every other limit keeps its default.
    """
    monkeypatch.setenv("TRADE_INTEGRITY_MONITOR_INTERVAL_SECONDS", "45")
    watchdog = TradeIntegrityWatchdog(
        repo=cast(
            TradeIntegrityRepository,
            _make_repo(_result("m1"), _result("m2")),
        )
    )

    assert frozenset({"TRADE_INTEGRITY_MONITOR_INTERVAL_SECONDS"}) == ENV_VARS
    assert watchdog.config == TradeIntegrityMonitorConfig(
        interval_seconds=45.0,
        startup_grace_seconds=DEFAULT_STARTUP_GRACE_SECONDS,
        sweep_limit=DEFAULT_SWEEP_LIMIT,
        worklog_limit=DEFAULT_WORKLOG_LIMIT,
        monitor_timeout_seconds=DEFAULT_MONITOR_TIMEOUT_SECONDS,
    )


def test_m2_finding_details_retain_active_identity_evidence() -> None:
    """Preserve active-identity evidence in serialized M2 findings.

    Given an M2 finding identifies public-1 with two active rows,
    When the watchdog converts its finding into alert details,
    Then the details retain monitor M2, public-1, and an active count of two.
    """
    watchdog = _make_watchdog(
        _make_repo(_result("m1"), _result("m2")),
        None,
    )

    details = watchdog._finding_details(
        _result("m2", findings=(_finding("m2"),)),
    )

    assert details == [
        {
            "monitor": "m2",
            "public_id": "public-1",
            "active_count": 2,
        }
    ]


@pytest.mark.asyncio
async def test_clean_tick_publishes_one_healthy_frame_per_monitor() -> None:
    """Publish complete healthy heartbeats for a clean monitor pass.

    Given the repository returns clean M1 and M2 results with cursor metadata,
    When the watchdog executes one tick at the shared evaluation time,
    Then it checks M1 then M2 and publishes healthy topics with cursor and limit metadata.
    """
    repo = _make_repo(_result("m1"), _result("m2"))
    publisher = _make_publisher()
    watchdog = _make_watchdog(repo, publisher)

    await watchdog._tick(now=_NOW)

    assert repo.run_trade_integrity_monitor.await_args_list == [
        call(
            monitor="m1",
            now=_NOW,
            sweep_limit=DEFAULT_SWEEP_LIMIT,
            worklog_limit=DEFAULT_WORKLOG_LIMIT,
        ),
        call(
            monitor="m2",
            now=_NOW,
            sweep_limit=DEFAULT_SWEEP_LIMIT,
            worklog_limit=DEFAULT_WORKLOG_LIMIT,
        ),
    ]
    assert publisher.send.await_count == 2
    first_topic, first_frame = publisher.send.await_args_list[0].args
    second_topic, second_frame = publisher.send.await_args_list[1].args
    assert first_topic == "system.heartbeats.trade_integrity.m1"
    assert second_topic == "system.heartbeats.trade_integrity.m2"
    assert isinstance(first_frame, HeartbeatData)
    assert isinstance(second_frame, HeartbeatData)
    assert first_frame.status == HealthStatusEnum.HEALTHY
    assert second_frame.status == HealthStatusEnum.HEALTHY
    assert first_frame.meta["reason"] == "healthy"
    assert first_frame.meta["finding_count"] == 0
    assert first_frame.meta["sweep_rows"] == 17
    assert first_frame.meta["worklog_rows"] == 3
    assert first_frame.meta["cursor_timestamp"] == (_NOW - timedelta(minutes=2)).isoformat()
    assert first_frame.meta["cursor_id"] == 41
    assert first_frame.meta["covered_through"] == (_NOW - timedelta(minutes=1)).isoformat()
    assert first_frame.meta["lag_seconds"] == 0
    assert first_frame.meta["sweep_limit"] == DEFAULT_SWEEP_LIMIT
    assert first_frame.meta["worklog_limit"] == DEFAULT_WORKLOG_LIMIT
    assert first_frame.meta["monitor_timeout_seconds"] == DEFAULT_MONITOR_TIMEOUT_SECONDS


@pytest.mark.asyncio
async def test_findings_and_lag_publish_exact_warning_bursts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Publish the exact warning bursts for findings and cursor lag.

    Given M1 returns one divergence and M2 reports 901 seconds of lag,
    When one watchdog tick publishes those two monitor results,
    Then it emits three M1 finding frames and three M2 lag frames with four two-second waits.
    """
    repo = _make_repo(
        _result("m1", findings=(_finding("m1"),)),
        _result("m2", lagged=True, lag_seconds=901),
    )
    publisher = _make_publisher()
    watchdog = _make_watchdog(repo, publisher)
    sleeps: list[float] = []

    async def record_sleep(delay: float) -> None:
        """Record warning-frame spacing.

        Args:
            delay: Requested delay.
        """
        sleeps.append(delay)

    monkeypatch.setattr(monitor_module.asyncio, "sleep", record_sleep)

    await watchdog._tick(now=_NOW)

    assert publisher.send.await_count == 6
    assert sleeps == [2.0, 2.0, 2.0, 2.0]
    topics = [await_call.args[0] for await_call in publisher.send.await_args_list]
    assert topics == [
        "system.heartbeats.trade_integrity.m1",
        "system.heartbeats.trade_integrity.m1",
        "system.heartbeats.trade_integrity.m1",
        "system.heartbeats.trade_integrity.m2",
        "system.heartbeats.trade_integrity.m2",
        "system.heartbeats.trade_integrity.m2",
    ]
    m1_frame = publisher.send.await_args_list[2].args[1]
    m2_frame = publisher.send.await_args_list[5].args[1]
    assert m1_frame.status == HealthStatusEnum.WARNING
    assert m1_frame.meta["reason"] == "integrity_findings"
    assert m1_frame.meta["finding_count"] == 1
    assert m1_frame.meta["findings"] == [
        {
            "monitor": "m1",
            "instrument_public_id": "instrument-1",
            "trade_id": "trade-1",
            "expected_executed_at": _NOW.isoformat(),
            "conflicting_executed_at": (_NOW - timedelta(seconds=1)).isoformat(),
        }
    ]
    assert m2_frame.status == HealthStatusEnum.WARNING
    assert m2_frame.lag_ms == 901_000
    assert m2_frame.meta["reason"] == "monitor_lagged"
    assert m2_frame.meta["lagged"] is True


@pytest.mark.asyncio
async def test_combined_finding_and_lag_has_one_stable_reason(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use one stable reason when a result is both divergent and lagged.

    Given M1 reports one finding and 902 seconds of lag while M2 is clean,
    When the watchdog executes one tick without warning delays,
    Then M1 emits three warnings whose final frame carries the combined reason.
    """
    repo = _make_repo(
        _result(
            "m1",
            findings=(_finding("m1"),),
            lagged=True,
            lag_seconds=902,
        ),
        _result("m2"),
    )
    publisher = _make_publisher()
    watchdog = _make_watchdog(repo, publisher)

    async def no_sleep(delay: float) -> None:
        """Skip warning spacing in this status-selection test.

        Args:
            delay: Ignored requested delay.
        """

    monkeypatch.setattr(monitor_module.asyncio, "sleep", no_sleep)

    await watchdog._tick(now=_NOW)

    assert publisher.send.await_count == 4
    frame = publisher.send.await_args_list[2].args[1]
    assert frame.meta["reason"] == "integrity_findings_and_monitor_lagged"


@pytest.mark.asyncio
async def test_warning_publication_contains_each_frame_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Attempt every warning frame when the publisher repeatedly fails.

    Given each publisher send raises a broker-unavailable error,
    When an M1 finding is expanded into its warning burst,
    Then all three frame sends are still attempted.
    """
    publisher = _make_publisher()
    publisher.send = AsyncMock(side_effect=RuntimeError("broker unavailable"))
    watchdog = _make_watchdog(
        _make_repo(_result("m1"), _result("m2")),
        publisher,
    )

    async def no_sleep(delay: float) -> None:
        """Skip warning spacing while retaining the three attempts.

        Args:
            delay: Ignored requested delay.
        """

    monkeypatch.setattr(monitor_module.asyncio, "sleep", no_sleep)

    await watchdog._publish_result(
        _result("m1", findings=(_finding("m1"),)),
        _NOW,
    )

    assert publisher.send.await_count == 3


@pytest.mark.asyncio
async def test_m1_failure_does_not_prevent_m2_check() -> None:
    """Continue with M2 after the M1 repository call fails.

    Given the M1 repository call raises and the following M2 result is clean,
    When the watchdog executes its fixed-order tick,
    Then both monitors run and only M2 publishes a healthy heartbeat.
    """
    repo = MagicMock()
    repo.run_trade_integrity_monitor = AsyncMock(
        side_effect=[RuntimeError("m1 unavailable"), _result("m2")]
    )
    publisher = _make_publisher()
    watchdog = _make_watchdog(repo, publisher)

    await watchdog._tick(now=_NOW)

    assert repo.run_trade_integrity_monitor.await_count == 2
    assert [
        await_call.kwargs["monitor"]
        for await_call in repo.run_trade_integrity_monitor.await_args_list
    ] == [
        "m1",
        "m2",
    ]
    publisher.send.assert_awaited_once()
    topic, frame = publisher.send.await_args.args
    assert topic == "system.heartbeats.trade_integrity.m2"
    assert frame.status == HealthStatusEnum.HEALTHY


@pytest.mark.asyncio
async def test_monitor_timeout_does_not_prevent_next_monitor() -> None:
    """Continue with M2 after M1 exceeds its execution timeout.

    Given M1 blocks indefinitely while M2 can return a clean result,
    When a watchdog tick applies a one-millisecond per-monitor timeout,
    Then both calls occur and M2 still publishes its healthy heartbeat.
    """
    never = asyncio.Event()

    async def run_monitor(
        *,
        monitor: TradeIntegrityMonitor,
        now: datetime,
        sweep_limit: int,
        worklog_limit: int,
    ) -> TradeIntegrityRunResult:
        """Block M1 and return M2.

        Args:
            monitor: Selected invariant.
            now: Shared evaluation time.
            sweep_limit: Bounded cursor page size.
            worklog_limit: Bounded durable worklog page size.

        Returns:
            Clean M2 result.
        """
        if monitor == "m1":
            await never.wait()
        return _result(monitor)

    repo = MagicMock()
    repo.run_trade_integrity_monitor = AsyncMock(side_effect=run_monitor)
    publisher = _make_publisher()
    watchdog = _make_watchdog(repo, publisher, monitor_timeout_seconds=0.001)

    await watchdog._tick(now=_NOW)

    assert repo.run_trade_integrity_monitor.await_count == 2
    publisher.send.assert_awaited_once()
    assert publisher.send.await_args.args[0] == "system.heartbeats.trade_integrity.m2"


@pytest.mark.asyncio
async def test_guarded_tick_contains_unexpected_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Contain an unexpected tick failure inside the loop guard.

    Given the watchdog tick raises an unexpected runtime error,
    When the guarded tick wrapper invokes it,
    Then the error does not escape and the tick is awaited exactly once.
    """
    watchdog = _make_watchdog(
        _make_repo(_result("m1"), _result("m2")),
        None,
    )
    tick = AsyncMock(side_effect=RuntimeError("unexpected"))
    monkeypatch.setattr(watchdog, "_tick", tick)

    await watchdog._guarded_tick()

    tick.assert_awaited_once()


@pytest.mark.asyncio
async def test_loop_skips_tick_when_stopped_during_sleep(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Skip the next database pass when shutdown arrives during sleep.

    Given the loop sleep sets the watchdog stopping event,
    When the watchdog loop resumes after that sleep,
    Then neither integrity monitor repository call is awaited.
    """
    repo = _make_repo(_result("m1"), _result("m2"))
    watchdog = _make_watchdog(repo, None)

    async def stop_during_sleep(delay: float) -> None:
        """Signal shutdown while the loop is sleeping.

        Args:
            delay: Ignored requested delay.
        """
        watchdog._stopping.set()

    monkeypatch.setattr(monitor_module.asyncio, "sleep", stop_during_sleep)

    await watchdog._loop()

    repo.run_trade_integrity_monitor.assert_not_awaited()


@pytest.mark.asyncio
async def test_startup_grace_delays_initial_pass(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Delay both initial monitor reads until startup grace expires.

    Given a 12-second startup grace is held by a controllable sleep,
    When the watchdog starts and that grace is later released,
    Then no monitor runs before release and both monitors run afterward.
    """
    repo = _make_repo(_result("m1"), _result("m2"))
    watchdog = _make_watchdog(
        repo,
        None,
        interval_seconds=3600.0,
        startup_grace_seconds=12.0,
    )
    grace_started = asyncio.Event()
    release_grace = asyncio.Event()
    real_sleep = asyncio.sleep

    async def controlled_sleep(delay: float) -> None:
        """Hold the startup grace and retain normal loop sleeping.

        Args:
            delay: Requested delay.
        """
        if delay == 12.0 and not grace_started.is_set():
            grace_started.set()
            await release_grace.wait()
            return
        await real_sleep(delay)

    monkeypatch.setattr(monitor_module.asyncio, "sleep", controlled_sleep)

    watchdog.start()
    await asyncio.wait_for(grace_started.wait(), timeout=1.0)
    repo.run_trade_integrity_monitor.assert_not_awaited()
    release_grace.set()
    for _index in range(5):
        if repo.run_trade_integrity_monitor.await_count == 2:
            break
        await real_sleep(0)

    assert repo.run_trade_integrity_monitor.await_count == 2
    await watchdog.stop()


@pytest.mark.asyncio
async def test_start_runs_immediately_and_stop_cancels_owned_task() -> None:
    """Start an eager pass and clear the owned loop task on shutdown.

    Given a stopped watchdog has zero startup grace and an hour-long interval,
    When it starts, completes the eager pass, and is stopped again,
    Then both monitors run and the loop task changes from owned to cleared.
    """
    repo = _make_repo(_result("m1"), _result("m2"))
    watchdog = _make_watchdog(repo, None, interval_seconds=3600.0)

    await watchdog.stop()
    assert watchdog._loop_task is None
    watchdog.start()
    for _index in range(5):
        if repo.run_trade_integrity_monitor.await_count == 2:
            break
        await asyncio.sleep(0)

    assert repo.run_trade_integrity_monitor.await_count == 2
    assert watchdog._loop_task is not None
    await watchdog.stop()
    assert watchdog._loop_task is None
