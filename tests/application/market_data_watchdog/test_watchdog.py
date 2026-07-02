"""Tests for :mod:`snapper.application.market_data_watchdog.watchdog`.

Pins the silent-exchange watchdog contract:

* env resolution with defaults, floors, and malformed-input fallbacks;
* lifecycle parity with the other API-lifespan monitors (disabled
  park, eager-but-defensive first tick, cancel-on-stop, one bad tick
  never kills the loop);
* detection semantics — silence measured from the newest candle's
  minute END, per-exchange threshold overrides, ``0`` disables one
  exchange, never-seen exchanges are skipped, CME closures suppress
  kraken_equities and the silence clock clamps to the last reopen;
* burst mechanics — 3 WARNING frames on
  ``system.heartbeats.marketdata.{exchange}`` spaced 2 s apart (the
  critical-system-error rule's 3-consecutive gate + the bridge's 1 s
  heartbeat throttle), domain sequence continuity, per-frame failure
  swallowing, and the publisher-less detection-only mode.
"""

import asyncio
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.application.market_data_watchdog import watchdog as watchdog_module
from snapper.application.market_data_watchdog.watchdog import DEFAULT_INTERVAL_SECONDS
from snapper.application.market_data_watchdog.watchdog import DEFAULT_THRESHOLD_SECONDS
from snapper.application.market_data_watchdog.watchdog import MIN_INTERVAL_SECONDS
from snapper.application.market_data_watchdog.watchdog import MIN_THRESHOLD_SECONDS
from snapper.application.market_data_watchdog.watchdog import MarketDataWatchdog
from snapper.application.market_data_watchdog.watchdog import _resolve_disabled
from snapper.application.market_data_watchdog.watchdog import _resolve_exchange_thresholds
from snapper.application.market_data_watchdog.watchdog import _resolve_interval
from snapper.application.market_data_watchdog.watchdog import _resolve_threshold
from snapper.core.types import HealthStatusEnum
from snapper.data.repository import Repository
from snapper.data.repository_types import MarketDataFreshnessRow
from snapper.messaging.schemas.data import HeartbeatData

_NOW = datetime(2026, 7, 1, 12, 0, tzinfo=UTC)
"""Wednesday noon UTC — CME open, mid-session."""


class _TrackerStub:
    """Minimal sequence tracker satisfying the heartbeat protocol."""

    def __init__(self) -> None:
        """Start the transport sequence at zero."""
        self.session_id = "sess-watchdog"
        self.streams: list[str] = []
        self._sequence = 0

    def next_sequence(self, stream: str) -> int:
        """Record the stream and return the next transport sequence.

        Args:
            stream: Topic the frame is about to be published on.

        Returns:
            Monotonically increasing transport sequence.
        """
        self.streams.append(stream)
        self._sequence += 1
        return self._sequence


def _make_publisher() -> MagicMock:
    """Build a publisher mock with a working tracker and async send."""
    publisher = MagicMock()
    publisher.tracker = _TrackerStub()
    publisher.send = AsyncMock()
    return publisher


def _make_repo(rows: list[MarketDataFreshnessRow]) -> MagicMock:
    """Build a repository mock returning ``rows`` from the freshness query."""
    repo = MagicMock()
    repo.get_latest_candle_open_at_by_exchange = AsyncMock(return_value=rows)
    return repo


def _make_watchdog(
    repo: MagicMock,
    publisher: MagicMock | None,
    *,
    default_threshold_seconds: int = 600,
    exchange_thresholds: dict[str, int] | None = None,
) -> MarketDataWatchdog:
    """Build a watchdog with fully explicit config (no env reads)."""
    return MarketDataWatchdog(
        repo=cast(Repository, repo),
        msg_publisher=publisher,
        interval_seconds=30.0,
        default_threshold_seconds=default_threshold_seconds,
        exchange_thresholds=exchange_thresholds if exchange_thresholds is not None else {},
        disabled=False,
    )


def _row(exchange: str, latest_open_at: datetime | None) -> MarketDataFreshnessRow:
    """Build one freshness row."""
    return MarketDataFreshnessRow(exchange=exchange, latest_open_at=latest_open_at)


async def _no_sleep(delay: float) -> None:
    """Async no-op replacing ``asyncio.sleep`` for deterministic ticks."""


class TestEnvResolution:
    """Env parsing: defaults, floors, malformed-input fallbacks."""

    @pytest.mark.parametrize(
        ("env_value", "expected"),
        [
            (None, False),
            ("", False),
            ("0", False),
            ("no", False),
            ("1", True),
            ("true", True),
            (" YES ", True),
        ],
    )
    def test_resolve_disabled(self, env_value: str | None, expected: bool) -> None:
        """Disabled flag accepts the shared truthy set only.

        Given: Raw env values including unset, falsy, and truthy forms,
        When: ``_resolve_disabled`` parses them,
        Then: Only 1/true/yes (case-insensitive, stripped) disable.
        """
        assert _resolve_disabled(env_value) is expected

    @pytest.mark.parametrize(
        ("env_value", "expected"),
        [
            (None, DEFAULT_INTERVAL_SECONDS),
            ("bogus", DEFAULT_INTERVAL_SECONDS),
            ("-5", DEFAULT_INTERVAL_SECONDS),
            ("0", DEFAULT_INTERVAL_SECONDS),
            ("1", MIN_INTERVAL_SECONDS),
            ("45.5", 45.5),
        ],
    )
    def test_resolve_interval(self, env_value: str | None, expected: float) -> None:
        """Interval falls back on bad input and clamps to the floor.

        Given: Raw env values including unset, unparseable, non-positive,
            sub-floor, and valid forms,
        When: ``_resolve_interval`` parses them,
        Then: Bad input yields the default and small values clamp to
            the 5 s floor.
        """
        assert _resolve_interval(env_value) == expected

    @pytest.mark.parametrize(
        ("env_value", "expected"),
        [
            (None, DEFAULT_THRESHOLD_SECONDS),
            ("bogus", DEFAULT_THRESHOLD_SECONDS),
            ("-1", DEFAULT_THRESHOLD_SECONDS),
            ("0", DEFAULT_THRESHOLD_SECONDS),
            ("30", MIN_THRESHOLD_SECONDS),
            ("900", 900),
        ],
    )
    def test_resolve_threshold(self, env_value: str | None, expected: int) -> None:
        """Threshold falls back on bad input and clamps to the floor.

        Given: Raw env values including unset, unparseable, non-positive,
            sub-floor, and valid forms,
        When: ``_resolve_threshold`` parses them,
        Then: Bad input yields the default and small values clamp to
            the 120 s floor.
        """
        assert _resolve_threshold(env_value) == expected

    def test_resolve_exchange_thresholds_parses_csv(self) -> None:
        """Per-exchange CSV parses overrides, disables, clamps, and skips junk.

        Given: A CSV mixing a valid override, an explicit disable, a
            sub-floor value, and malformed entries,
        When: ``_resolve_exchange_thresholds`` parses it,
        Then: Valid entries land (clamped to the floor), 0/negative
            disable, and malformed entries are skipped.
        """
        parsed = _resolve_exchange_thresholds(
            "walutomat=1200, KRAKEN_EQUITIES=0,kraken=30,=5,junk,futures=abc,kraken_futures=-7"
        )
        assert parsed == {
            "walutomat": 1200,
            "kraken_equities": 0,
            "kraken": MIN_THRESHOLD_SECONDS,
            "kraken_futures": 0,
        }

    def test_resolve_exchange_thresholds_unset_is_empty(self) -> None:
        """Unset env yields no overrides.

        Given: ``None`` for the overrides env var,
        When: ``_resolve_exchange_thresholds`` parses it,
        Then: The result is an empty mapping.
        """
        assert _resolve_exchange_thresholds(None) == {}


class TestConstruction:
    """Constructor wiring: env-driven defaults and explicit overrides."""

    def test_env_driven_configuration(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Unset constructor args resolve from the environment.

        Given: All four watchdog env vars set,
        When: The watchdog is built with only a repository,
        Then: Disabled state, interval, default threshold, and
            per-exchange overrides reflect the environment.
        """
        monkeypatch.setenv("MARKET_DATA_WATCHDOG_DISABLED", "false")
        monkeypatch.setenv("MARKET_DATA_WATCHDOG_INTERVAL_SECONDS", "15")
        monkeypatch.setenv("MARKET_DATA_WATCHDOG_THRESHOLD_SECONDS", "300")
        monkeypatch.setenv("MARKET_DATA_WATCHDOG_EXCHANGE_THRESHOLDS", "walutomat=1200")
        wd = MarketDataWatchdog(repo=cast(Repository, _make_repo([])))
        assert wd.disabled is False
        assert wd.interval_seconds == 15.0
        assert wd.threshold_for("kraken") == 300
        assert wd.threshold_for("walutomat") == 1200

    def test_explicit_overrides_beat_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Constructor arguments take precedence over the environment.

        Given: Env vars pointing one way and explicit args another,
        When: The watchdog is built with explicit args,
        Then: The explicit values win.
        """
        monkeypatch.setenv("MARKET_DATA_WATCHDOG_DISABLED", "true")
        monkeypatch.setenv("MARKET_DATA_WATCHDOG_INTERVAL_SECONDS", "15")
        wd = _make_watchdog(_make_repo([]), None, exchange_thresholds={"kraken": 0})
        assert wd.disabled is False
        assert wd.interval_seconds == 30.0
        assert wd.threshold_for("kraken") == 0
        assert wd.threshold_for("kraken_futures") == 600


class TestLifecycle:
    """start/stop parity with the other API-lifespan monitors."""

    @pytest.mark.asyncio
    async def test_disabled_parks_without_ticking(self) -> None:
        """Disabled watchdog neither ticks nor spawns the loop.

        Given: A watchdog constructed with ``disabled=True``,
        When: ``start`` then ``stop`` run,
        Then: The freshness query is never issued and no loop task exists.
        """
        repo = _make_repo([])
        wd = MarketDataWatchdog(
            repo=cast(Repository, repo),
            msg_publisher=None,
            interval_seconds=30.0,
            default_threshold_seconds=600,
            exchange_thresholds={},
            disabled=True,
        )
        await wd.start()
        assert wd._loop_task is None
        await wd.stop()
        repo.get_latest_candle_open_at_by_exchange.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_start_takes_eager_tick_and_stop_cancels_loop(self) -> None:
        """Start ticks eagerly, spawns the loop; stop cancels it.

        Given: An enabled watchdog with a healthy repository,
        When: ``start`` then ``stop`` run,
        Then: Exactly one eager tick happened and the loop task is
            cancelled and cleared.
        """
        repo = _make_repo([])
        wd = _make_watchdog(repo, None)
        await wd.start()
        assert wd._loop_task is not None
        repo.get_latest_candle_open_at_by_exchange.assert_awaited_once()
        await wd.stop()
        assert wd._loop_task is None

    @pytest.mark.asyncio
    async def test_eager_tick_failure_does_not_block_startup(self) -> None:
        """A failing eager tick logs and still spawns the loop.

        Given: A repository whose freshness query raises,
        When: ``start`` runs,
        Then: No exception propagates and the loop task is spawned —
            a transient boot-time DB error must not park the watchdog.
        """
        repo = MagicMock()
        repo.get_latest_candle_open_at_by_exchange = AsyncMock(side_effect=RuntimeError("boom"))
        wd = _make_watchdog(repo, None)
        await wd.start()
        assert wd._loop_task is not None
        await wd.stop()

    @pytest.mark.asyncio
    async def test_stop_without_start_is_tolerated(self) -> None:
        """Stop on a never-started watchdog is a no-op.

        Given: A watchdog that never ran ``start``,
        When: ``stop`` runs,
        Then: It returns cleanly.
        """
        wd = _make_watchdog(_make_repo([]), None)
        await wd.stop()
        assert wd._loop_task is None

    @pytest.mark.asyncio
    async def test_stop_with_already_finished_loop_task(self) -> None:
        """Stop skips cancellation when the loop task already finished.

        Given: A watchdog whose loop task has completed on its own,
        When: ``stop`` runs,
        Then: It clears the task reference without cancelling.
        """
        wd = _make_watchdog(_make_repo([]), None)

        async def instant() -> None:
            """Complete immediately to leave a done task behind."""

        task = asyncio.create_task(instant())
        await task
        wd._loop_task = task
        await wd.stop()
        assert wd._loop_task is None


class _StopAfter:
    """Stopper stub: ``is_set`` returns False for N calls, then True."""

    def __init__(self, false_calls: int) -> None:
        """Configure how many calls report not-stopped.

        Args:
            false_calls: Number of leading ``is_set`` calls returning
                ``False``.
        """
        self._false_calls = false_calls
        self.calls = 0

    def is_set(self) -> bool:
        """Report stopped once the configured call budget is spent."""
        self.calls += 1
        return self.calls > self._false_calls

    def set(self) -> None:
        """Satisfy the Event protocol; irrelevant for the stub."""

    def clear(self) -> None:
        """Satisfy the Event protocol; irrelevant for the stub."""


class TestLoop:
    """Poll-loop mechanics with deterministic sleep and stoppers."""

    @pytest.mark.asyncio
    async def test_loop_ticks_once_then_stops(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The loop runs one guarded tick per iteration.

        Given: A stopper allowing one full iteration,
        When: ``_loop`` runs with sleep patched out,
        Then: Exactly one freshness query is issued.
        """
        repo = _make_repo([])
        wd = _make_watchdog(repo, None)
        monkeypatch.setattr(watchdog_module.asyncio, "sleep", _no_sleep)
        wd._stopping = cast(asyncio.Event, _StopAfter(2))
        await wd._loop()
        repo.get_latest_candle_open_at_by_exchange.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_loop_exits_when_stopped_during_sleep(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A stop signal landing during the sleep skips the pending tick.

        Given: A stopper flipping to stopped right after the sleep,
        When: ``_loop`` runs,
        Then: No freshness query is issued.
        """
        repo = _make_repo([])
        wd = _make_watchdog(repo, None)
        monkeypatch.setattr(watchdog_module.asyncio, "sleep", _no_sleep)
        wd._stopping = cast(asyncio.Event, _StopAfter(1))
        await wd._loop()
        repo.get_latest_candle_open_at_by_exchange.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_tick_exception_does_not_kill_the_loop(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """One bad tick logs and the next iteration still runs.

        Given: A repository raising on every query and a stopper
            allowing two full iterations,
        When: ``_loop`` runs,
        Then: Both iterations issued the query — the first failure did
            not terminate the loop.
        """
        repo = MagicMock()
        repo.get_latest_candle_open_at_by_exchange = AsyncMock(side_effect=RuntimeError("boom"))
        wd = _make_watchdog(repo, None)
        monkeypatch.setattr(watchdog_module.asyncio, "sleep", _no_sleep)
        wd._stopping = cast(asyncio.Event, _StopAfter(4))
        await wd._loop()
        assert repo.get_latest_candle_open_at_by_exchange.await_count == 2


class TestDetection:
    """Silence arithmetic, thresholds, and CME suppression."""

    @pytest.mark.asyncio
    async def test_silent_exchange_triggers_burst(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Silence beyond the threshold publishes the 3-frame burst.

        Given: kraken's newest candle minute ended 11 minutes ago
            against a 600 s threshold,
        When: A tick runs,
        Then: Three WARNING frames go out on
            ``system.heartbeats.marketdata.kraken`` with the watchdog
            component, increasing domain sequences, silence-derived
            ``lag_ms``, and full forensic meta.
        """
        latest = _NOW - timedelta(minutes=12)
        repo = _make_repo([_row("kraken", latest)])
        publisher = _make_publisher()
        wd = _make_watchdog(repo, publisher)
        sleeps: list[float] = []

        async def record_sleep(delay: float) -> None:
            sleeps.append(delay)

        monkeypatch.setattr(watchdog_module.asyncio, "sleep", record_sleep)
        await wd._tick(now=_NOW)
        assert publisher.send.await_count == 3
        assert sleeps == [2.0, 2.0]
        topic, frame = publisher.send.await_args.args
        assert topic == "system.heartbeats.marketdata.kraken"
        assert isinstance(frame, HeartbeatData)
        assert frame.component == "marketdata.kraken"
        assert frame.status == HealthStatusEnum.WARNING
        assert frame.sequence == 3
        assert frame.session_id == "sess-watchdog"
        assert frame.lag_ms == 660 * 1000
        assert frame.meta["synthetic"] is True
        assert frame.meta["origin"] == "market_data_watchdog"
        assert frame.meta["reason"] == "exchange_silent"
        assert frame.meta["exchange"] == "kraken"
        assert frame.meta["silent_seconds"] == 660
        assert frame.meta["threshold_seconds"] == 600
        assert frame.meta["latest_candle_open_at"] == latest.isoformat()
        assert publisher.tracker.streams == ["system.heartbeats.marketdata.kraken"] * 3

    @pytest.mark.asyncio
    async def test_fresh_exchange_stays_silent(self) -> None:
        """Silence under the threshold publishes nothing.

        Given: kraken's newest candle minute ended 9 minutes ago
            against a 600 s threshold (silence 540 s),
        When: A tick runs,
        Then: No frame is published.
        """
        repo = _make_repo([_row("kraken", _NOW - timedelta(minutes=10))])
        publisher = _make_publisher()
        wd = _make_watchdog(repo, publisher)
        await wd._tick(now=_NOW)
        publisher.send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_silence_exactly_at_threshold_fires(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The threshold boundary is inclusive.

        Given: Silence of exactly 600 s against a 600 s threshold,
        When: A tick runs,
        Then: The burst fires.
        """
        repo = _make_repo([_row("kraken", _NOW - timedelta(seconds=660))])
        publisher = _make_publisher()
        wd = _make_watchdog(repo, publisher)
        monkeypatch.setattr(watchdog_module.asyncio, "sleep", _no_sleep)
        await wd._tick(now=_NOW)
        assert publisher.send.await_count == 3

    @pytest.mark.asyncio
    async def test_zero_threshold_disables_one_exchange(self) -> None:
        """A ``0`` override skips the exchange entirely.

        Given: walutomat silent for hours but overridden to 0,
        When: A tick runs,
        Then: No frame is published.
        """
        repo = _make_repo([_row("walutomat", _NOW - timedelta(hours=5))])
        publisher = _make_publisher()
        wd = _make_watchdog(repo, publisher, exchange_thresholds={"walutomat": 0})
        await wd._tick(now=_NOW)
        publisher.send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_never_seen_exchange_is_skipped(self) -> None:
        """An exchange with no candles at all never alerts.

        Given: A freshness row with ``latest_open_at`` None (fresh
            deployment, empty database),
        When: A tick runs,
        Then: No frame is published.
        """
        repo = _make_repo([_row("kraken_futures", None)])
        publisher = _make_publisher()
        wd = _make_watchdog(repo, publisher)
        await wd._tick(now=_NOW)
        publisher.send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_equities_suppressed_during_cme_closure(self) -> None:
        """kraken_equities never alerts inside a scheduled CME closure.

        Given: Equities' newest candle at Friday 20:59 UTC and a
            reference instant on Saturday noon (weekend closure),
        When: A tick runs,
        Then: No frame is published despite hours of silence.
        """
        saturday_noon = datetime(2026, 7, 4, 12, 0, tzinfo=UTC)
        latest = datetime(2026, 7, 3, 20, 59, tzinfo=UTC)
        repo = _make_repo([_row("kraken_equities", latest)])
        publisher = _make_publisher()
        wd = _make_watchdog(repo, publisher)
        await wd._tick(now=saturday_noon)
        publisher.send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_equities_silence_clamps_to_last_reopen(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """After a closure the silence clock starts at the reopen.

        Given: Equities' newest candle at Sunday-preceding Friday 20:59
            UTC (49 h stale) and reference instants shortly after the
            Sunday 22:00 UTC reopen,
        When: Ticks run at reopen+5 min and reopen+11 min,
        Then: The first is quiet (silence 300 s < 600 s) and the second
            bursts (silence 660 s) — no false page at the bell, no
            missed page when the feed genuinely fails to resume.
        """
        latest = datetime(2026, 7, 3, 20, 59, tzinfo=UTC)
        repo = _make_repo([_row("kraken_equities", latest)])
        publisher = _make_publisher()
        wd = _make_watchdog(repo, publisher)
        monkeypatch.setattr(watchdog_module.asyncio, "sleep", _no_sleep)
        await wd._tick(now=datetime(2026, 7, 5, 22, 5, tzinfo=UTC))
        publisher.send.assert_not_awaited()
        await wd._tick(now=datetime(2026, 7, 5, 22, 11, tzinfo=UTC))
        assert publisher.send.await_count == 3
        _topic, frame = publisher.send.await_args.args
        assert frame.meta["silent_seconds"] == 660

    @pytest.mark.asyncio
    async def test_wall_clock_default_reference(self) -> None:
        """Omitting ``now`` measures silence against the wall clock.

        Given: A kraken freshness row from 2020,
        When: A tick runs without an injected reference,
        Then: The burst fires (real now is years past the threshold);
            the publisher-less watchdog only logs, proving the
            detection path tolerates a missing publisher.
        """
        repo = _make_repo([_row("kraken", datetime(2020, 1, 1, tzinfo=UTC))])
        wd = _make_watchdog(repo, None)
        await wd._tick()
        repo.get_latest_candle_open_at_by_exchange.assert_awaited_once()


class TestBurst:
    """Burst-path resilience."""

    @pytest.mark.asyncio
    async def test_send_failures_are_swallowed_per_frame(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A failing send never raises and every frame is attempted.

        Given: A publisher whose ``send`` always raises,
        When: A tick with a silent exchange runs,
        Then: No exception propagates and all 3 frames were attempted.
        """
        repo = _make_repo([_row("kraken", _NOW - timedelta(hours=1))])
        publisher = _make_publisher()
        publisher.send = AsyncMock(side_effect=RuntimeError("socket down"))
        wd = _make_watchdog(repo, publisher)
        monkeypatch.setattr(watchdog_module.asyncio, "sleep", _no_sleep)
        await wd._tick(now=_NOW)
        assert publisher.send.await_count == 3

    @pytest.mark.asyncio
    async def test_sequences_continue_across_bursts(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Domain sequences keep increasing across level-triggered bursts.

        Given: Two consecutive silent ticks for the same exchange,
        When: Both bursts publish,
        Then: The final frame carries domain sequence 6.
        """
        repo = _make_repo([_row("kraken", _NOW - timedelta(hours=1))])
        publisher = _make_publisher()
        wd = _make_watchdog(repo, publisher)
        monkeypatch.setattr(watchdog_module.asyncio, "sleep", _no_sleep)
        await wd._tick(now=_NOW)
        await wd._tick(now=_NOW + timedelta(minutes=1))
        assert publisher.send.await_count == 6
        _topic, frame = publisher.send.await_args.args
        assert frame.sequence == 6
