"""Bounded snapshot catch-up and real offloaded valuation regression witnesses."""

import asyncio
import threading
from collections.abc import Callable
from collections.abc import Sequence
from datetime import datetime
from datetime import timedelta
from typing import cast

import pytest

from snapper.application.portfolio import pnl_snapshotter
from snapper.application.portfolio import pnl_timeline_service
from snapper.application.portfolio.pnl_snapshot_planner import MAX_CATCHUP_CHUNK_MINUTES
from snapper.application.portfolio.pnl_snapshotter import PortfolioPnlSnapshotter
from snapper.application.portfolio.pnl_timeline import PnlTimelineResult
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReplayOptions
from snapper.application.portfolio.pnl_timeline_service import PnlWalletSeriesResult
from snapper.application.portfolio.pnl_timeline_service import build_wallet_pnl_series
from snapper.data.repository import PortfolioPnlSampleScope
from snapper.data.repository import Repository
from snapper.data.repository_types import PortfolioPnlSampleRow
from tests.application.portfolio import test_pnl_snapshotter as snapshot_fixtures
from tests.application.portfolio import test_pnl_timeline_service as series_fixtures


class _ProgressRepository(snapshot_fixtures._FakeRepo):
    """Retain durable progress without retaining prior batches of test payloads."""

    def __init__(self) -> None:
        """Start with an anchor and record only successful commit windows."""
        super().__init__(
            credentials=[snapshot_fixtures._cred("kraken")],
            anchor=snapshot_fixtures._anchor(),
        )
        self.committed_windows: list[tuple[datetime, datetime]] = []
        self.inserted_count = 0

    async def record_portfolio_pnl_samples(
        self, rows: Sequence[PortfolioPnlSampleRow], scope: PortfolioPnlSampleScope
    ) -> dict[str, tuple[datetime, ...]]:
        """Commit one batch or fail without moving durable progress.

        Args:
            rows: The actual samples planned by the snapshotter.
            scope: The sample writer's bound scope.

        Returns:
            The writer's inserted, existing and conflicting minute sets.
        """
        del scope
        assert rows
        minutes = tuple(row["point_time"] for row in rows)
        if self.conflict_batch:
            return {"inserted": (), "already_present": (), "conflicts": minutes}
        self._latest = rows[-1]
        self.persisted = [rows[-1]]
        self.committed_windows.append((minutes[0], minutes[-1]))
        self.inserted_count += len(rows)
        return {"inserted": minutes, "already_present": (), "conflicts": ()}


def _snapshotter(repo: _ProgressRepository, now: datetime) -> PortfolioPnlSnapshotter:
    """Build an enabled instance over the durable-progress witness.

    Args:
        repo: The bounded repository witness.
        now: The fixed finalization horizon.

    Returns:
        A snapshotter with no background task or external connection.
    """
    return PortfolioPnlSnapshotter(repo=cast(Repository, repo), disabled=False, clock=lambda: now)


def _install_series(
    monkeypatch: pytest.MonkeyPatch, windows: list[tuple[datetime, datetime]]
) -> None:
    """Record actual requested work and supply finite synthetic financial points.

    Args:
        monkeypatch: Scoped replacement fixture.
        windows: The engine windows observed by the caller.
    """

    async def series(*args: object, **kwargs: object) -> PnlWalletSeriesResult:
        """Return one requested window and require offloaded valuation.

        Args:
            args: The orchestrator's positional request arguments.
            kwargs: Its typed policy and replay options.

        Returns:
            A complete, finite series for exactly the requested window.
        """
        start, end = args[3], args[4]
        assert isinstance(start, datetime) and isinstance(end, datetime)
        options = kwargs["options"]
        assert isinstance(options, PnlSeriesReplayOptions)
        assert options.offload_valuation is True
        windows.append((start, end))
        return snapshot_fixtures._series(start, end, metadata=snapshot_fixtures._meta(seq=7))

    monkeypatch.setattr(pnl_snapshotter, "build_wallet_pnl_series", series)


async def test_multi_day_gap_drains_one_day_per_tick(monkeypatch: pytest.MonkeyPatch) -> None:
    """A multi-day gap advances through consecutive bounded durable commits.

    Given: A complete scope whose anchor is three days behind.
    When: The service ticks repeatedly at the same finalized horizon.
    Then: Each tick reads and commits at most one day, with no skipped minute.

    Args:
        monkeypatch: Scoped synthetic series fixture.
    """
    repo = _ProgressRepository()
    total_days = 3
    end = snapshot_fixtures._T0 + timedelta(days=total_days)
    snap = _snapshotter(repo, end + timedelta(minutes=2))
    windows: list[tuple[datetime, datetime]] = []
    _install_series(monkeypatch, windows)
    for day in range(total_days):
        await snap._tick_once()
        start = snapshot_fixtures._T0 + timedelta(days=day, minutes=1)
        finish = snapshot_fixtures._T0 + timedelta(days=day + 1)
        assert windows[-1] == (start, finish)
        assert repo.committed_windows[-1] == (start, finish)
        assert len(windows) == day + 1
        assert repo.inserted_count == (day + 1) * MAX_CATCHUP_CHUNK_MINUTES
    await snap._tick_once()
    assert len(windows) == total_days
    assert repo.committed_windows[-1][1] == end


async def test_restart_retries_failed_day_from_last_commit(monkeypatch: pytest.MonkeyPatch) -> None:
    """A failed commit cannot move the restart cursor past an unwritten day.

    Given: One committed day followed by a conflicting second-day insert.
    When: A fresh service instance resumes after the conflict clears.
    Then: It retries the same second day and leaves no gap or duplicate commit.

    Args:
        monkeypatch: Scoped synthetic series fixture.
    """
    repo = _ProgressRepository()
    now = snapshot_fixtures._T0 + timedelta(days=3, minutes=2)
    windows: list[tuple[datetime, datetime]] = []
    _install_series(monkeypatch, windows)
    first = _snapshotter(repo, now)
    await first._tick_once()
    repo.conflict_batch = True
    await first._tick_once()
    assert repo.inserted_count == MAX_CATCHUP_CHUNK_MINUTES
    assert len(repo.committed_windows) == 1
    failed_window = windows[-1]
    repo.conflict_batch = False
    resumed = _snapshotter(repo, now)
    await resumed._tick_once()
    assert windows[-1] == failed_window
    assert repo.committed_windows[-1] == failed_window
    assert repo.inserted_count == 2 * MAX_CATCHUP_CHUNK_MINUTES


def _series_repository(
    *, has_fill_gap: bool = False, foreign_quote: bool = False
) -> series_fixtures.FakeRepo:
    """Supply real kernel inputs with deterministic lineage and marks.

    Args:
        has_fill_gap: Whether the durable execution prefix is incomplete.
        foreign_quote: Whether both prices and fees need actual FX conversion.

    Returns:
        A repository double used by the real series orchestrator.
    """
    quote = "EUR" if foreign_quote else "USD"
    return series_fixtures.FakeRepo(
        executions=[
            series_fixtures._exec_row(series_fixtures._I1, 1, 0, "buy", 1.0, 100.0, 0.5, quote)
        ],
        refs=[series_fixtures._ref(series_fixtures._I1, "BTC-" + quote, quote)],
        candles=[series_fixtures._candle(series_fixtures._m(i), 110.0) for i in range(-1, 3)],
        fx_rows=[series_fixtures._fx_row("EUR", "USD", i, 1.1) for i in range(3)],
        has_fill_gap=has_fill_gap,
    )


@pytest.mark.parametrize("has_fill_gap", [False, True])
@pytest.mark.parametrize("foreign_quote", [False, True])
async def test_offloaded_valuation_preserves_every_point(
    has_fill_gap: bool, foreign_quote: bool
) -> None:
    """Threaded valuation preserves the exact financial and refusal payload.

    Given: The same finite executions, mark evidence and optional fill-gap refusal.
    When: The real series engine runs synchronously and on a worker.
    Then: Every returned point, contribution and diagnostic remains identical.

    Args:
        has_fill_gap: Whether the input intentionally withholds trusted valuation.
        foreign_quote: Whether both prices and fees need actual FX conversion.
    """
    args = (series_fixtures._W1, "live", series_fixtures._T0, series_fixtures._m(2), "1m")
    reference = await build_wallet_pnl_series(
        cast(
            Repository,
            _series_repository(has_fill_gap=has_fill_gap, foreign_quote=foreign_quote),
        ),
        *args,
        series_fixtures._m(3),
    )
    offloaded = await build_wallet_pnl_series(
        cast(
            Repository,
            _series_repository(has_fill_gap=has_fill_gap, foreign_quote=foreign_quote),
        ),
        *args,
        series_fixtures._m(3),
        options=PnlSeriesReplayOptions(offload_valuation=True),
    )
    assert offloaded == reference


async def test_real_valuation_leaves_the_event_loop_responsive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The loop can handle another callback while the real kernel owns its worker.

    Given: A real valuation kernel gated by a deterministic cross-thread handshake.
    When: An async callback releases the already-entered worker.
    Then: The kernel finishes with complete points and never blocks the loop thread.

    Args:
        monkeypatch: Scoped wrapper of the unchanged real valuation kernel.
    """
    loop = asyncio.get_running_loop()
    owner_thread = threading.get_ident()
    entered = asyncio.Event()
    release = threading.Event()
    worker_threads: list[int] = []
    real_kernel = cast(
        Callable[..., PnlTimelineResult], pnl_timeline_service._timed_build_pnl_timeline
    )

    def observed_kernel(*args: object, **kwargs: object) -> PnlTimelineResult:
        """Record the worker and call the actual financial kernel.

        Args:
            args: Already-valued financial inputs.
            kwargs: Frozen opening, lineage and evidence arguments.

        Returns:
            The unchanged real kernel result.
        """
        worker_threads.append(threading.get_ident())
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(5)
        return real_kernel(*args, **kwargs)

    monkeypatch.setattr(pnl_timeline_service, "_timed_build_pnl_timeline", observed_kernel)
    task: asyncio.Task[PnlWalletSeriesResult] = asyncio.create_task(
        build_wallet_pnl_series(
            cast(Repository, _series_repository()),
            series_fixtures._W1,
            "live",
            series_fixtures._T0,
            series_fixtures._m(2),
            "1m",
            series_fixtures._m(3),
            options=PnlSeriesReplayOptions(offload_valuation=True),
        )
    )
    try:
        await asyncio.wait_for(entered.wait(), timeout=3)
        assert not task.done()
    finally:
        release.set()
        result = await task
    assert worker_threads and owner_thread not in worker_threads
    assert all(point.valuation_status == "complete" for point in result.points)


@pytest.mark.parametrize("fail_worker", [False, True])
async def test_cancellation_waits_for_the_owned_valuation(fail_worker: bool) -> None:
    """Stopping a valuation cannot orphan its worker or turn cancellation into success.

    Given: A worker that has entered and cannot complete before the test releases it.
    When: Its caller is cancelled twice while the worker remains active.
    Then: Cancellation propagates only after the worker has exited, even on failure.

    Args:
        fail_worker: Whether the running worker fails instead of returning a value.
    """
    entered = asyncio.Event()
    release = threading.Event()
    exited = threading.Event()
    loop = asyncio.get_running_loop()

    def valuation() -> tuple[PnlTimelineResult, tuple[pnl_timeline_service.PnlFxRateSource, ...]]:
        """Exit the worker deterministically, including its failure path.

        Returns:
            An empty typed valuation when no failure was requested.
        """
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(5)
        exited.set()
        if fail_worker:
            raise ValueError("synthetic valuation failure")
        return PnlTimelineResult(granularity="1m", valuation_ccy="USD", points=()), ()

    task = asyncio.create_task(pnl_timeline_service._offload_series_valuation(valuation))
    try:
        await asyncio.wait_for(entered.wait(), timeout=3)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        assert not exited.is_set()
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert exited.is_set()


@pytest.mark.parametrize("worker_cancelled", [False, True])
async def test_worker_failure_remains_a_failed_valuation(worker_cancelled: bool) -> None:
    """A failed worker cannot be mistaken for an ordinary completed valuation.

    Given: A computation that raises before producing any financial output.
    When: Its uncancelled caller waits for that worker.
    Then: The same exception category propagates instead of a successful result.

    Args:
        worker_cancelled: Whether the worker itself raises cancellation.
    """

    def valuation() -> tuple[PnlTimelineResult, tuple[pnl_timeline_service.PnlFxRateSource, ...]]:
        """Fail the synthetic valuation without external access.

        Returns:
            No result: the worker always fails.
        """
        if worker_cancelled:
            raise asyncio.CancelledError
        raise ValueError("synthetic valuation failure")

    expected = asyncio.CancelledError if worker_cancelled else ValueError
    with pytest.raises(expected):
        await pnl_timeline_service._offload_series_valuation(valuation)
