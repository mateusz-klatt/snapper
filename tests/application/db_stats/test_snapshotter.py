"""Unit tests for :class:`DbStatsSnapshotter`."""

import asyncio
from collections.abc import Awaitable
from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from typing import Any

import pytest

from snapper.application.db_stats import snapshotter as snapshotter_module
from snapper.application.db_stats.snapshotter import TABLES_TO_SAMPLE
from snapper.application.db_stats.snapshotter import DbStatsSnapshot
from snapper.application.db_stats.snapshotter import DbStatsSnapshotter
from snapper.application.db_stats.snapshotter import TableStats
from snapper.application.db_stats.snapshotter import resolve_disabled
from snapper.application.db_stats.snapshotter import resolve_interval
from snapper.data.db_stats_types import TableCounters
from snapper.data.db_stats_types import TableEntry
from snapper.data.models import Candle


class _FakeRepo:
    """Repository stub returning canned :class:`TableCounters` per call."""

    def __init__(
        self,
        *,
        per_table: Callable[[TableEntry], TableCounters] | None = None,
        per_table_async: Callable[[TableEntry], Awaitable[TableCounters]] | None = None,
    ) -> None:
        self._per_table = per_table
        self._per_table_async = per_table_async
        self.calls: list[TableEntry] = []

    async def count_table_stats(
        self,
        entry: TableEntry,
        *,
        archivable_window: tuple[Any, Any] | None = None,
    ) -> TableCounters:
        self.calls.append(entry)
        if self._per_table_async is not None:
            return await self._per_table_async(entry)
        if self._per_table is None:
            return TableCounters(total=0, current=None, closed=None, archivable=None)
        return self._per_table(entry)


def _stable_clock() -> Callable[[], datetime]:
    """Return a clock that always returns the same UTC datetime."""
    return lambda: datetime(2026, 5, 1, 12, 0, tzinfo=UTC)


class TestResolveInterval:
    """``DB_METRICS_INTERVAL_SECONDS`` parsing — int range [10, 3600]."""

    def test_unset_returns_default(self) -> None:
        """``None`` env value falls back to default 60."""
        assert resolve_interval(None) == 60

    def test_blank_returns_default(self) -> None:
        """Whitespace-only env value falls back to default 60."""
        assert resolve_interval("   ") == 60

    def test_unparseable_raises(self) -> None:
        """Non-integer strings raise so a malformed env var fails loud."""
        with pytest.raises(ValueError, match="not an integer"):
            resolve_interval("not-a-number")

    def test_below_min_raises(self) -> None:
        """Values below ``INTERVAL_MIN_SECONDS`` raise out-of-range."""
        with pytest.raises(ValueError, match="out of range"):
            resolve_interval("5")

    def test_above_max_raises(self) -> None:
        """Values above ``INTERVAL_MAX_SECONDS`` raise out-of-range."""
        with pytest.raises(ValueError, match="out of range"):
            resolve_interval("3601")

    def test_zero_or_negative_raises(self) -> None:
        """Non-positive values are out-of-range and raise."""
        with pytest.raises(ValueError, match="out of range"):
            resolve_interval("0")
        with pytest.raises(ValueError, match="out of range"):
            resolve_interval("-5")

    def test_in_range_passes_through_as_int(self) -> None:
        """Values inside the supported range survive parsing exactly."""
        assert resolve_interval("120") == 120
        assert resolve_interval("10") == 10
        assert resolve_interval("3600") == 3600

    def test_float_string_raises(self) -> None:
        """Fractional strings are not integers — raise instead of truncating."""
        with pytest.raises(ValueError, match="not an integer"):
            resolve_interval("0.5")


class TestResolveDisabled:
    """``DB_METRICS_DISABLED`` parsing."""

    @pytest.mark.parametrize("raw", ["1", "true", "TRUE", "Yes", " yes ", "YES"])
    def test_truthy_values(self, raw: str) -> None:
        """Documented truthy values resolve to ``True``."""
        assert resolve_disabled(raw) is True

    @pytest.mark.parametrize("raw", [None, "", "0", "false", "no", "off", "anything"])
    def test_falsy_values(self, raw: str | None) -> None:
        """Anything outside the truthy set resolves to ``False``."""
        assert resolve_disabled(raw) is False


class TestRegistryComposition:
    """``TABLES_TO_SAMPLE`` ordering — STATE-first alphabetical, then EVENT alphabetical."""

    def test_state_block_precedes_event_block(self) -> None:
        """All STATE entries appear before any EVENT entry."""
        kinds = [entry.kind for entry in TABLES_TO_SAMPLE]
        first_event_index = kinds.index("event")
        assert all(k == "state" for k in kinds[:first_event_index])
        assert all(k == "event" for k in kinds[first_event_index:])

    def test_each_block_alphabetical(self) -> None:
        """STATE and EVENT blocks are independently sorted by name."""
        state_names = [e.name for e in TABLES_TO_SAMPLE if e.kind == "state"]
        event_names = [e.name for e in TABLES_TO_SAMPLE if e.kind == "event"]
        assert state_names == sorted(state_names)
        assert event_names == sorted(event_names)

    def test_known_tables_present(self) -> None:
        """Spot-check that registry covers the canonical operational tables."""
        names = {entry.name for entry in TABLES_TO_SAMPLE}
        assert {"orders", "instruments"} <= names
        assert {"telemetry", "ticks", "trades", "control"} <= names

    def test_candles_sampled_as_state(self) -> None:
        """``candles`` is sampled explicitly as SCD2 state with active-index current estimates."""
        entry = next(e for e in TABLES_TO_SAMPLE if e.name == "candles")
        assert entry.kind == "state"
        assert entry.model is Candle
        assert entry.current_estimate_index == "uq_candle_itf_open"


class TestConstructorAndProperties:
    """Snapshotter wiring + property defaults."""

    def test_disabled_mode_zeroes_repo(self) -> None:
        """Disabled mode discards the repo even when one was passed."""
        snapshotter = DbStatsSnapshotter(
            repo=_FakeRepo(),
            interval_seconds=60,
            disabled=True,
        )
        assert snapshotter.disabled is True
        assert snapshotter.latest_snapshot is None
        assert snapshotter.interval_seconds == 60

    def test_enabled_mode_keeps_repo(self) -> None:
        """Enabled mode preserves the repo and the configured interval."""
        repo = _FakeRepo()
        snapshotter = DbStatsSnapshotter(
            repo=repo,
            interval_seconds=15,
            disabled=False,
        )
        assert snapshotter.disabled is False
        assert snapshotter.interval_seconds == 15

    def test_constructor_reads_env_when_args_not_supplied(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When kwargs are omitted, env vars drive interval + disabled flags."""
        monkeypatch.setenv("DB_METRICS_INTERVAL_SECONDS", "240")
        monkeypatch.setenv("DB_METRICS_DISABLED", "yes")
        snapshotter = DbStatsSnapshotter(repo=_FakeRepo())
        assert snapshotter.interval_seconds == 240
        assert snapshotter.disabled is True


class TestSampleOnce:
    """``_sample_once`` happy-path semantics."""

    @pytest.mark.asyncio
    async def test_returns_snapshot_for_every_registered_table(self) -> None:
        """Every registered table appears in the snapshot output."""
        repo = _FakeRepo(
            per_table=lambda entry: TableCounters(
                total=1 if entry.kind == "event" else 2,
                current=None if entry.kind == "event" else 1,
                closed=None if entry.kind == "event" else 1,
                archivable=None,
            )
        )
        snapshotter = DbStatsSnapshotter(
            repo=repo,
            interval_seconds=60,
            disabled=False,
            clock=_stable_clock(),
        )
        snapshot = await snapshotter._sample_once()
        assert isinstance(snapshot, DbStatsSnapshot)
        assert len(snapshot.tables) == len(TABLES_TO_SAMPLE)
        assert all(t.is_stale is False for t in snapshot.tables)
        assert snapshot.snapshot_started_at == datetime(2026, 5, 1, 12, 0, tzinfo=UTC)

    @pytest.mark.asyncio
    async def test_event_kind_maps_to_null_axes(self) -> None:
        """Event tables retain ``current``/``closed`` as ``None``."""
        repo = _FakeRepo(
            per_table=lambda entry: TableCounters(
                total=10, current=None, closed=None, archivable=None
            )
        )
        snapshotter = DbStatsSnapshotter(
            repo=repo, interval_seconds=60, disabled=False, clock=_stable_clock()
        )
        snapshot = await snapshotter._sample_once()
        events = [t for t in snapshot.tables if t.table_kind == "event"]
        for row in events:
            assert row.current is None
            assert row.closed is None
            assert row.total == 10

    @pytest.mark.asyncio
    async def test_telemetry_archivable_window_passed_through(self) -> None:
        """Telemetry policy yields a window arg; tables without a policy receive ``None``."""
        captured_windows: dict[str, tuple[Any, Any] | None] = {}

        async def per_table(entry: TableEntry) -> TableCounters:
            return TableCounters(total=42, current=None, closed=None, archivable=7)

        class _CapturingRepo:
            async def count_table_stats(
                self,
                entry: TableEntry,
                *,
                archivable_window: tuple[Any, Any] | None = None,
            ) -> TableCounters:
                captured_windows[entry.name] = archivable_window
                return await per_table(entry)

        snapshotter = DbStatsSnapshotter(
            repo=_CapturingRepo(),
            interval_seconds=60,
            disabled=False,
            clock=_stable_clock(),
        )
        await snapshotter._sample_once()
        assert captured_windows["telemetry"] is not None
        assert captured_windows["orders"] is None

    @pytest.mark.asyncio
    async def test_per_table_failure_emits_null_stale_when_no_prior(self) -> None:
        """First-run table failure produces ``is_stale=True`` with all-null counters."""

        async def per_table(entry: TableEntry) -> TableCounters:
            if entry.name == "telemetry":
                raise RuntimeError("boom")
            return TableCounters(total=0, current=None, closed=None, archivable=None)

        repo = _FakeRepo(per_table_async=per_table)
        snapshotter = DbStatsSnapshotter(
            repo=repo, interval_seconds=60, disabled=False, clock=_stable_clock()
        )
        snapshot = await snapshotter._sample_once()
        telemetry_row = snapshot.find("telemetry")
        assert telemetry_row is not None
        assert telemetry_row.is_stale is True
        assert telemetry_row.total is None

    @pytest.mark.asyncio
    async def test_per_table_failure_clones_prior_with_stale_flag(self) -> None:
        """Subsequent failure reuses prior counters with ``is_stale=True``."""
        call_count = {"n": 0}

        async def per_table(entry: TableEntry) -> TableCounters:
            call_count["n"] += 1
            if call_count["n"] > len(TABLES_TO_SAMPLE) and entry.name == "telemetry":
                raise RuntimeError("boom")
            return TableCounters(total=42, current=None, closed=None, archivable=None)

        repo = _FakeRepo(per_table_async=per_table)
        snapshotter = DbStatsSnapshotter(
            repo=repo, interval_seconds=60, disabled=False, clock=_stable_clock()
        )
        first = await snapshotter._sample_once()
        snapshotter._latest_snapshot = first
        second = await snapshotter._sample_once()
        telemetry_row = second.find("telemetry")
        assert telemetry_row is not None
        assert telemetry_row.is_stale is True
        assert telemetry_row.total == 42

    @pytest.mark.asyncio
    async def test_per_table_timeout_emits_stale_clone(self) -> None:
        """A per-table query timeout marks that row stale without aborting the loop."""

        async def hanging(entry: TableEntry) -> TableCounters:
            if entry.name == "telemetry":
                await asyncio.sleep(10)
            return TableCounters(total=0, current=None, closed=None, archivable=None)

        repo = _FakeRepo(per_table_async=hanging)
        snapshotter = DbStatsSnapshotter(
            repo=repo, interval_seconds=60, disabled=False, clock=_stable_clock()
        )
        original_timeout = snapshotter_module.PER_TABLE_TIMEOUT_SECONDS
        try:
            snapshotter_module.PER_TABLE_TIMEOUT_SECONDS = 0.01
            snapshot = await snapshotter._sample_once()
        finally:
            snapshotter_module.PER_TABLE_TIMEOUT_SECONDS = original_timeout
        telemetry_row = snapshot.find("telemetry")
        assert telemetry_row is not None
        assert telemetry_row.is_stale is True

    @pytest.mark.asyncio
    async def test_disabled_mode_sample_once_raises(self) -> None:
        """Calling ``_sample_once`` directly when disabled is a programmer error."""
        snapshotter = DbStatsSnapshotter(
            repo=None, interval_seconds=60, disabled=True, clock=_stable_clock()
        )
        with pytest.raises(RuntimeError, match="disabled mode"):
            await snapshotter._sample_once()


class TestStartStop:
    """Loop lifecycle."""

    @pytest.mark.asyncio
    async def test_disabled_start_is_noop(self, caplog: pytest.LogCaptureFixture) -> None:
        """Disabled mode skips loop spawn and leaves ``latest_snapshot`` ``None``."""
        snapshotter = DbStatsSnapshotter(
            repo=None, interval_seconds=60, disabled=True, clock=_stable_clock()
        )
        with caplog.at_level("INFO", logger="snapper.application.db_stats.snapshotter"):
            await snapshotter.start()
        assert snapshotter.latest_snapshot is None
        assert snapshotter._loop_task is None
        await snapshotter.stop()

    @pytest.mark.asyncio
    async def test_loop_publishes_snapshot_then_stops_cleanly(self) -> None:
        """Loop produces at least one snapshot before stopping cleanly."""
        repo = _FakeRepo(
            per_table=lambda entry: TableCounters(
                total=3, current=None, closed=None, archivable=None
            )
        )
        snapshotter = DbStatsSnapshotter(
            repo=repo, interval_seconds=1, disabled=False, clock=_stable_clock()
        )
        await snapshotter.start()
        for _ in range(200):
            if snapshotter.latest_snapshot is not None:
                break
            await asyncio.sleep(0.02)
        await snapshotter.stop()
        assert snapshotter.latest_snapshot is not None
        assert all(row.total == 3 for row in snapshotter.latest_snapshot.tables)
        assert snapshotter._loop_task is None

    @pytest.mark.asyncio
    async def test_loop_continues_after_unexpected_sample_once_exception(
        self,
        caplog: pytest.LogCaptureFixture,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Bug in ``_sample_once`` itself logs and the next tick still runs."""
        events_seen: list[str] = []
        original_sample = DbStatsSnapshotter._sample_once

        async def flaky(snap_self: DbStatsSnapshotter) -> DbStatsSnapshot:
            events_seen.append("called")
            if len(events_seen) == 1:
                raise RuntimeError("synthetic")
            return await original_sample(snap_self)

        monkeypatch.setattr(DbStatsSnapshotter, "_sample_once", flaky)
        repo = _FakeRepo()
        snapshotter = DbStatsSnapshotter(
            repo=repo, interval_seconds=1, disabled=False, clock=_stable_clock()
        )
        with caplog.at_level("ERROR", logger="snapper.application.db_stats.snapshotter"):
            try:
                await snapshotter.start()
                for _ in range(200):
                    if len(events_seen) >= 2 and snapshotter.latest_snapshot is not None:
                        break
                    await asyncio.sleep(0.02)
            finally:
                await snapshotter.stop()
        assert len(events_seen) >= 2
        assert snapshotter.latest_snapshot is not None
        assert any("_sample_once raised" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_stop_cancels_in_flight_loop(self) -> None:
        """``stop()`` cancels and awaits the loop task even mid-sample."""

        async def slow(entry: TableEntry) -> TableCounters:
            await asyncio.sleep(2.0)
            return TableCounters(total=0, current=None, closed=None, archivable=None)

        repo = _FakeRepo(per_table_async=slow)
        snapshotter = DbStatsSnapshotter(
            repo=repo, interval_seconds=1, disabled=False, clock=_stable_clock()
        )
        await snapshotter.start()
        await snapshotter.stop()
        assert snapshotter._loop_task is None

    @pytest.mark.asyncio
    async def test_stop_is_idempotent_when_never_started(self) -> None:
        """``stop()`` on a never-started snapshotter is a clean no-op."""
        snapshotter = DbStatsSnapshotter(
            repo=_FakeRepo(), interval_seconds=60, disabled=False, clock=_stable_clock()
        )
        await snapshotter.stop()
        assert snapshotter._loop_task is None

    @pytest.mark.asyncio
    async def test_loop_returns_cleanly_when_stopping_already_set(self) -> None:
        """Pre-set ``stopping`` exits at the ``while`` check without entering the body."""
        repo = _FakeRepo()
        snapshotter = DbStatsSnapshotter(
            repo=repo, interval_seconds=10, disabled=False, clock=_stable_clock()
        )
        snapshotter._stopping.set()
        await snapshotter._loop()
        assert snapshotter.latest_snapshot is None
        assert repo.calls == []

    @pytest.mark.asyncio
    async def test_loop_returns_when_stopping_set_during_sleep(self) -> None:
        """``stopping.set()`` mid-sleep returns from the loop via the wait_for branch."""
        repo = _FakeRepo()
        snapshotter = DbStatsSnapshotter(
            repo=repo, interval_seconds=10, disabled=False, clock=_stable_clock()
        )
        await snapshotter.start()
        await asyncio.sleep(0)
        snapshotter._stopping.set()
        task = snapshotter._loop_task
        assert task is not None
        await task
        assert repo.calls == []
        assert snapshotter.latest_snapshot is None
        snapshotter._loop_task = None

    @pytest.mark.asyncio
    async def test_loop_returns_when_stopping_set_between_sleep_and_sample(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Stopping set after sleep timeout but before sample exits without sampling."""
        repo = _FakeRepo()
        snapshotter = DbStatsSnapshotter(
            repo=repo, interval_seconds=1, disabled=False, clock=_stable_clock()
        )
        sleep_calls: list[int] = []

        async def fake_wait_for(awaitable: Any, *, timeout: float) -> bool:
            del timeout
            awaitable.close()
            sleep_calls.append(1)
            snapshotter._stopping.set()
            raise TimeoutError

        monkeypatch.setattr(asyncio, "wait_for", fake_wait_for)
        await snapshotter._loop()
        assert sleep_calls == [1]
        assert repo.calls == []
        assert snapshotter.latest_snapshot is None


class TestStaleRowFor:
    """``_stale_row_for`` helper covers both prior-row and no-prior fallback."""

    def test_clones_prior_with_stale_flag(self) -> None:
        """Existing prior row is cloned with ``is_stale=True``; counters preserved."""
        sampled_at = datetime(2026, 5, 1, 12, 30, tzinfo=UTC)
        prior = DbStatsSnapshot(
            snapshot_started_at=sampled_at,
            snapshot_completed_at=sampled_at,
            interval_seconds=60,
            tables=(
                TableStats(
                    table="telemetry",
                    table_kind="event",
                    total=42,
                    current=None,
                    closed=None,
                    archivable=7,
                    is_stale=False,
                    last_sampled_at=sampled_at,
                ),
            ),
        )
        entry = TableEntry(name="telemetry", kind="event", model=type("M", (), {}))
        row = DbStatsSnapshotter._stale_row_for(entry, prior=prior, sampled_at=sampled_at)
        assert row.is_stale is True
        assert row.total == 42
        assert row.archivable == 7

    def test_emits_null_when_no_prior_row(self) -> None:
        """Without prior data the stale row carries all-null counters and ``sampled_at``."""
        sampled_at = datetime(2026, 5, 1, 12, 30, tzinfo=UTC)
        entry = TableEntry(name="telemetry", kind="event", model=type("M", (), {}))
        row = DbStatsSnapshotter._stale_row_for(entry, prior=None, sampled_at=sampled_at)
        assert row.is_stale is True
        assert row.total is None
        assert row.archivable is None
        assert row.last_sampled_at == sampled_at


class TestDbStatsSnapshotFind:
    """``DbStatsSnapshot.find`` returns the matching ``TableStats`` or ``None``."""

    def test_returns_matching_row(self) -> None:
        """``find`` resolves a name match to the exact stored row."""
        ts = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
        row = TableStats(
            table="orders",
            table_kind="state",
            total=1,
            current=1,
            closed=0,
            archivable=None,
            is_stale=False,
            last_sampled_at=ts,
        )
        snapshot = DbStatsSnapshot(
            snapshot_started_at=ts,
            snapshot_completed_at=ts,
            interval_seconds=60,
            tables=(row,),
        )
        assert snapshot.find("orders") is row

    def test_returns_none_for_unknown(self) -> None:
        """``find`` returns ``None`` for names not in the snapshot."""
        ts = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
        snapshot = DbStatsSnapshot(
            snapshot_started_at=ts,
            snapshot_completed_at=ts,
            interval_seconds=60,
            tables=(),
        )
        assert snapshot.find("nope") is None
