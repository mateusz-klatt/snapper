"""Prove a halved snapshotter replay chunk limit recovers deterministically.

A transient deadline halves a wallet's chunk limit. Without recovery the halved
limit would persist for the life of the process and multiply every later chunk's
fixed prefix cost, so consecutive fast full-width committed chunks double it
back, capped at ``MAX_CORRECTION_CHUNK_MINUTES``, while persisted rows stay
width-independent. Narrower chunks, clipped at the tip or split after a
work-budget overflow, never count as evidence that a wider chunk fits.
"""

from collections.abc import Callable
from collections.abc import Iterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from itertools import count
from pathlib import Path

import pytest

from snapper.application.portfolio import pnl_snapshotter
from snapper.application.portfolio.pnl_snapshot_planner import ChunkWindow
from snapper.application.portfolio.pnl_snapshotter import PortfolioPnlSnapshotter
from snapper.data.repository import SQLAlchemyRepository
from tests.application.portfolio import test_pnl_snapshotter_continuation as restart
from tests.data import test_pnl_timeline_sample_repository as sample_fixtures

_WALLET = "wallet-recovery"
_FAST = 0.5
_SLOW = 30.0
_START = datetime(2026, 7, 20, tzinfo=UTC)


def _window(minutes: int) -> ChunkWindow:
    """Return a replayed window spanning exactly this many minutes.

    Args:
        minutes: Inclusive minute count of the window.

    Returns:
        A minute-aligned window starting at a fixed instant.
    """
    return ChunkWindow(_START, _START + timedelta(minutes=minutes - 1))


def _full(snap: PortfolioPnlSnapshotter, wallet: str = _WALLET) -> ChunkWindow:
    """Return a window as wide as the wallet's current chunk limit.

    Args:
        snap: Snapshotter whose limit sets the width.
        wallet: Wallet whose limit is read.

    Returns:
        A full-width replayed window.
    """
    return _window(snap._replay_chunk_limit(wallet))


def _disabled_snapshotter() -> PortfolioPnlSnapshotter:
    """Build a parked snapshotter with a fixed sixty-second tick budget.

    Returns:
        Snapshotter whose recovery threshold is fifteen seconds.
    """
    return PortfolioPnlSnapshotter(repo=None, disabled=True, interval_seconds=60)


def _fast_clock() -> Callable[[], float]:
    """Return a monotonic stub advancing one millisecond per reading.

    Returns:
        Callable that yields strictly increasing seconds.
    """
    readings: Iterator[int] = count()
    return lambda: next(readings) / 1000


@pytest.fixture
def capped(monkeypatch: pytest.MonkeyPatch) -> None:
    """Cap the correction chunk at 1440 minutes for the unit scenarios.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    monkeypatch.setattr(pnl_snapshotter, "MAX_CORRECTION_CHUNK_MINUTES", 1440)


@pytest.mark.usefixtures("capped")
def test_three_fast_chunks_restore_the_full_limit() -> None:
    """Restore the default limit after one halving and three fast commits.

    Given: A wallet halved once to 720 minutes.
    When: Three committed chunks each finish well inside the recovery threshold.
    Then: The limit returns to 1440 and no per-wallet recovery state remains.
    """
    snap = _disabled_snapshotter()
    snap._reduce_replay_chunk(_WALLET)
    assert snap._replay_chunk_limit(_WALLET) == 720
    for _ in range(3):
        snap._record_chunk_outcome(_WALLET, _FAST, _full(snap))
    assert snap._replay_chunk_limit(_WALLET) == 1440
    assert _WALLET not in snap._replay_chunk_limits
    assert _WALLET not in snap._replay_chunk_streaks


@pytest.mark.usefixtures("capped")
def test_recovery_doubles_in_steps_and_never_exceeds_the_cap() -> None:
    """Double once per fast streak until the cap and then stop growing.

    Given: A wallet halved twice to 360 minutes.
    When: Three fast commits run, then three more, then three more at the cap.
    Then: The limit moves 360, 720, 1440 and stays at 1440 without kept state.
    """
    snap = _disabled_snapshotter()
    snap._reduce_replay_chunk(_WALLET)
    snap._reduce_replay_chunk(_WALLET)
    assert snap._replay_chunk_limit(_WALLET) == 360
    observed: list[int] = []
    for _ in range(9):
        snap._record_chunk_outcome(_WALLET, _FAST, _full(snap))
        observed.append(snap._replay_chunk_limit(_WALLET))
    assert observed == [360, 360, 720, 720, 720, 1440, 1440, 1440, 1440]
    assert _WALLET not in snap._replay_chunk_streaks


@pytest.mark.usefixtures("capped")
def test_slow_chunk_resets_the_streak() -> None:
    """Require consecutive fast commits, so one slow chunk restarts the count.

    Given: A wallet halved to 720 minutes.
    When: Two fast commits, one commit at the threshold, then two fast commits run.
    Then: The limit stays at 720 because no three fast commits were consecutive.
    """
    snap = _disabled_snapshotter()
    snap._reduce_replay_chunk(_WALLET)
    for elapsed in (_FAST, _FAST, 15.0, _FAST, _FAST):
        snap._record_chunk_outcome(_WALLET, elapsed, _full(snap))
    assert snap._replay_chunk_limit(_WALLET) == 720
    assert snap._replay_chunk_streaks[_WALLET] == 2


@pytest.mark.usefixtures("capped")
def test_reduction_during_a_streak_halves_and_resets() -> None:
    """Treat a new deadline as stronger evidence than a partial recovery streak.

    Given: A wallet at 720 minutes with two fast commits already counted.
    When: Another deadline halves the limit and two more fast commits follow.
    Then: The limit is 360 and the streak restarted, so it has not yet doubled.
    """
    snap = _disabled_snapshotter()
    snap._reduce_replay_chunk(_WALLET)
    snap._record_chunk_outcome(_WALLET, _FAST, _full(snap))
    snap._record_chunk_outcome(_WALLET, _FAST, _full(snap))
    snap._reduce_replay_chunk(_WALLET)
    assert _WALLET not in snap._replay_chunk_streaks
    snap._record_chunk_outcome(_WALLET, _FAST, _full(snap))
    snap._record_chunk_outcome(_WALLET, _FAST, _full(snap))
    assert snap._replay_chunk_limit(_WALLET) == 360


@pytest.mark.usefixtures("capped")
def test_full_limit_keeps_no_recovery_state() -> None:
    """Skip bookkeeping for wallets already at the default limit.

    Given: A wallet that was never reduced but carries a stale streak entry.
    When: Fast and slow commits are recorded for it.
    Then: The limit stays 1440 and the stale entry is dropped.
    """
    snap = _disabled_snapshotter()
    snap._replay_chunk_streaks[_WALLET] = 2
    snap._record_chunk_outcome(_WALLET, _FAST, _full(snap))
    snap._record_chunk_outcome(_WALLET, _SLOW, _full(snap))
    assert snap._replay_chunk_limit(_WALLET) == 1440
    assert snap._replay_chunk_streaks == {}


@pytest.mark.usefixtures("capped")
def test_pruning_an_absent_wallet_drops_its_streak() -> None:
    """Forget recovery state together with the chunk limit of a vanished scope.

    Given: Two reduced wallets that both carry a partial streak.
    When: Only one of them remains among the current scopes.
    Then: Only that wallet keeps its limit and its streak.
    """
    snap = _disabled_snapshotter()
    for wallet in (_WALLET, "wallet-gone"):
        snap._reduce_replay_chunk(wallet)
        snap._record_chunk_outcome(wallet, _FAST, _full(snap, wallet))
    snap._prune_absent_scopes({_WALLET})
    assert set(snap._replay_chunk_limits) == {_WALLET}
    assert snap._replay_chunk_streaks == {_WALLET: 1}


@pytest.mark.usefixtures("capped")
@pytest.mark.parametrize(
    "narrow",
    [
        pytest.param(_window(5), id="tip_clipped"),
        pytest.param(PortfolioPnlSnapshotter._split_chunk(_window(720))[0], id="budget_split"),
    ],
)
def test_fast_narrow_chunks_neither_grow_nor_reset_the_limit(narrow: ChunkWindow) -> None:
    """Keep a reduced limit while only narrower-than-limit chunks commit fast.

    Given: A wallet halved to 720 minutes with one fast full-width commit counted.
    When: Many fast chunks commit that are clipped at the tip or split in half.
    Then: The limit stays 720 and the counted streak is left exactly as it was.
    """
    snap = _disabled_snapshotter()
    snap._reduce_replay_chunk(_WALLET)
    snap._record_chunk_outcome(_WALLET, _FAST, _full(snap))
    for _ in range(9):
        snap._record_chunk_outcome(_WALLET, _FAST, narrow)
    assert snap._replay_chunk_limit(_WALLET) == 720
    assert snap._replay_chunk_streaks[_WALLET] == 1


@pytest.mark.usefixtures("capped")
def test_slow_narrow_chunk_still_resets_the_streak() -> None:
    """Treat any slow commit as a reason to restart the count, whatever its width.

    Given: A wallet halved to 720 minutes with two fast full-width commits counted.
    When: A five-minute chunk commits at the recovery threshold, then one fast
    full-width chunk commits.
    Then: The streak restarted, so the limit is still 720 with one commit counted.
    """
    snap = _disabled_snapshotter()
    snap._reduce_replay_chunk(_WALLET)
    snap._record_chunk_outcome(_WALLET, _FAST, _full(snap))
    snap._record_chunk_outcome(_WALLET, _FAST, _full(snap))
    snap._record_chunk_outcome(_WALLET, 15.0, _window(5))
    assert snap._replay_chunk_streaks[_WALLET] == 0
    snap._record_chunk_outcome(_WALLET, _FAST, _full(snap))
    assert snap._replay_chunk_limit(_WALLET) == 720
    assert snap._replay_chunk_streaks[_WALLET] == 1


async def _settle(repo: SQLAlchemyRepository, snap: PortfolioPnlSnapshotter) -> list[int]:
    """Tick until the generation settles and record the limit after each tick.

    Args:
        repo: Repository over the seeded history.
        snap: Snapshotter under test.

    Returns:
        The wallet's chunk limit observed after every tick.
    """
    limits: list[int] = []
    for _ in range(8):
        await snap._tick_once()
        limits.append(snap._replay_chunk_limit(sample_fixtures._WALLET))
        if (await restart._frontier(repo)).next_minute is None:
            return limits
    pytest.fail(f"replay did not settle; limits={limits}")


async def test_real_writer_recovers_its_limit_and_matches_the_control(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Grow a reduced limit through real committed chunks without changing rows.

    Given: Five minutes of history, a four-minute cap and a writer pre-reduced to one.
    When: Fast committed chunks run until the generation settles.
    Then: The limit grows 1, 1, 2 and the rows equal an uninterrupted width-4 run.
    """
    tape = restart._ReplayTape()
    monkeypatch.setattr(pnl_snapshotter, "MAX_CORRECTION_CHUNK_MINUTES", 4)
    monkeypatch.setattr(pnl_snapshotter, "build_wallet_pnl_series", tape.series)
    monkeypatch.delenv("PNL_FX_SHADOW_PINNING_ENABLED", raising=False)
    control_path = tmp_path / "control.db"
    await restart._seed(control_path)
    async with restart._repository(control_path) as control:
        control_snap = restart._snapshotter(control)
        control_snap._monotonic = _fast_clock()
        assert await _settle(control, control_snap) == [4, 4]
        expected = await restart._samples(control)

    path = tmp_path / "recovering.db"
    await restart._seed(path)
    async with restart._repository(path) as repo:
        snap = restart._snapshotter(repo)
        snap._monotonic = _fast_clock()
        snap._replay_chunk_limits[sample_fixtures._WALLET] = 1
        assert await _settle(repo, snap) == [1, 1, 2, 2]
        actual = await restart._samples(repo)

    assert [row["point_time"] for row in actual] == list(restart._MINUTES)
    assert restart._economics(actual) == restart._economics(expected)
