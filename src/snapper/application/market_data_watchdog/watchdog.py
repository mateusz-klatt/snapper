"""Silent-exchange market-data watchdog.

Polls the database for the newest candle per live feed exchange and,
when whole-exchange silence exceeds a per-exchange threshold,
publishes a synthetic WARNING heartbeat burst on
``system.heartbeats.marketdata.{exchange}`` so the existing
``critical_system_error`` rule (3-consecutive gate, rolling hourly
cooldown, hour-bucket dedup, admin fan-out, i18n, APNs) pages
operators — no new AlertType, topic family, or wire-contract member,
the same ventriloquism precedent as the launcher's park heartbeats.

Placement rationale — the feed process cannot self-report this
condition:

  * its own heartbeat status only degrades on DB flush errors, so a
    venue whose matching engine is down behind a healthy WebSocket
    (kraken_futures 2026-07-02, close code 1013 for 25 minutes)
    publishes HEALTHY forever;
  * its in-memory freshness clocks reset on every respawn and
    ``FeedDarkTooLongError`` exits at 1 500 s, so a respawn loop hides
    any longer outage;
  * a fully hung publisher emits nothing at all (kraken + walutomat
    2026-06-30, 115 minutes of shared silence).

The API-lifespan watchdog reads the shared database instead, so it
observes the product truth — candles — regardless of which upstream
component died: venue matching engine, WebSocket, publisher process,
or writer. In ``SERVER_API_ONLY`` split deployments the API container
still reads the shared DB, so coverage holds.

Known limitation: candle write recency is an end-to-end signal — a
``MarketPersistPolicy`` that legitimately skipped ALL candle writes
for an exchange would read as silence. In practice every live
exchange persists 1m candles continuously (7-day production baseline:
every real gap over 10 minutes was a genuine incident, and scheduled
CME closures are suppressed via the shared calendar), which is
exactly the invariant this watchdog exists to defend.

Alert cadence: the check is level-triggered — a silent exchange gets
one 3-frame WARNING burst per tick — and the critical-system-error
rule's rolling cooldown plus hour-bucket dedup cap operator pages at
about one per hour per exchange for the duration of the outage. No
recovery frame is sent when data resumes: the bursts simply stop, the
rule's rolling window ages out, and the alert trail ends.

Configuration (env, infra-tier):

  * ``MARKET_DATA_WATCHDOG_DISABLED`` — truthy string
    (``1``/``true``/``yes``) parks the watchdog entirely.
  * ``MARKET_DATA_WATCHDOG_INTERVAL_SECONDS`` — poll cadence, default
    60, floor 5.
  * ``MARKET_DATA_WATCHDOG_THRESHOLD_SECONDS`` — default silence
    threshold, default 600, floor 120 (candle write lag alone runs up
    to ~90 s, so lower values would page on healthy pipelines).
  * ``MARKET_DATA_WATCHDOG_EXCHANGE_THRESHOLDS`` — per-exchange
    overrides as ``exchange=seconds`` CSV, e.g.
    ``walutomat=1200,kraken_equities=900``; ``0`` disables monitoring
    for that exchange; malformed entries are ignored.
"""

import asyncio
import contextlib
import logging
import os
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Final
from typing import Protocol
from typing import get_args
from uuid import uuid7

from snapper.core.json_types import JsonObject
from snapper.core.market_hours import is_cme_closed
from snapper.core.market_hours import last_cme_reopen
from snapper.core.types import ExchangeEnum
from snapper.core.types import HealthStatusEnum
from snapper.core.types import MarketSubscribeExchange
from snapper.data.repository import Repository
from snapper.messaging.schemas.data import HeartbeatData
from snapper.messaging.topics.builders import heartbeat_topic

logger = logging.getLogger(__name__)

DEFAULT_INTERVAL_SECONDS: Final = 60.0
MIN_INTERVAL_SECONDS: Final = 5.0
DEFAULT_THRESHOLD_SECONDS: Final = 600
MIN_THRESHOLD_SECONDS: Final = 120
"""Threshold floor: candle write lag alone runs up to ~90 s on a
healthy pipeline (a candle for minute M lands around M+1m plus flush
lag), so thresholds under two minutes would page on normal operation."""

_CANDLE_MINUTE_SECONDS: Final = 60
"""A candle's ``open_at`` marks its minute START; the exchange was
provably alive until ``open_at + 60 s``, so silence is measured from
the minute end."""

_BURST_FRAME_COUNT: Final = 3
"""The critical-system-error rule gates on 3 consecutive non-HEALTHY
heartbeats per ``(component, name)``."""

_BURST_FRAME_SPACING_S: Final = 2.0
"""Must exceed the WS bridge's 1 s heartbeat throttle or frames 2-3
of the burst are dropped and the rule's gate never trips (same
spacing as the launcher's park-heartbeat burst)."""

WATCHDOG_HEARTBEAT_COMPONENT: Final = "marketdata"
"""Distinct from ``feed`` on purpose: the live feed publishes HEALTHY
frames on ``system.heartbeats.feed.{exchange}`` every second, so
WARNING frames interleaved there could never form 3 consecutive
non-HEALTHY beats."""

_DISABLED_ENV_VAR: Final = "MARKET_DATA_WATCHDOG_DISABLED"
_INTERVAL_ENV_VAR: Final = "MARKET_DATA_WATCHDOG_INTERVAL_SECONDS"
_THRESHOLD_ENV_VAR: Final = "MARKET_DATA_WATCHDOG_THRESHOLD_SECONDS"
_EXCHANGE_THRESHOLDS_ENV_VAR: Final = "MARKET_DATA_WATCHDOG_EXCHANGE_THRESHOLDS"

ENV_VARS: Final[frozenset[str]] = frozenset(
    {
        _DISABLED_ENV_VAR,
        _INTERVAL_ENV_VAR,
        _THRESHOLD_ENV_VAR,
        _EXCHANGE_THRESHOLDS_ENV_VAR,
    }
)
"""Public allowlist of env vars this module reads via ``os.environ``.

Consumed by :mod:`snapper.config.env_contract` to validate ``.env``
keys against the union of every subsystem's contract.
"""

_TRUTHY_ENV_VALUES: Final = frozenset({"1", "true", "yes"})


class _HeartbeatSequenceTracker(Protocol):
    """Subset of ``SequenceTracker`` needed for heartbeat provenance."""

    @property
    def session_id(self) -> str:
        """Return the publisher session id."""

    def next_sequence(self, stream: str) -> int:
        """Return the next transport sequence for a topic."""


class _HeartbeatPublisher(Protocol):
    """Subset of ``MessagePublisher`` needed by the watchdog."""

    @property
    def tracker(self) -> _HeartbeatSequenceTracker:
        """Return the publisher's shared sequence tracker."""

    async def send(self, stream_key: str, data: HeartbeatData) -> None:
        """Send one complete heartbeat frame."""


def _resolve_disabled(env_value: str | None) -> bool:
    """Parse ``MARKET_DATA_WATCHDOG_DISABLED`` as a boolean.

    Args:
        env_value: Raw env-var value, or ``None`` when unset.

    Returns:
        ``True`` iff the value is a truthy string
        (``1``/``true``/``yes``, case-insensitive).
    """
    if env_value is None:
        return False
    return env_value.strip().lower() in _TRUTHY_ENV_VALUES


def _resolve_interval(env_value: str | None) -> float:
    """Parse the poll interval, falling back to the default on bad input.

    Args:
        env_value: Raw env-var value, or ``None`` when unset.

    Returns:
        Interval in seconds — the parsed value clamped to
        :data:`MIN_INTERVAL_SECONDS`, or
        :data:`DEFAULT_INTERVAL_SECONDS` when unset, unparseable, or
        non-positive.
    """
    if env_value is None:
        return DEFAULT_INTERVAL_SECONDS
    try:
        parsed = float(env_value)
    except ValueError:
        return DEFAULT_INTERVAL_SECONDS
    if parsed <= 0:
        return DEFAULT_INTERVAL_SECONDS
    return max(parsed, MIN_INTERVAL_SECONDS)


def _resolve_threshold(env_value: str | None) -> int:
    """Parse the default silence threshold, falling back on bad input.

    Args:
        env_value: Raw env-var value, or ``None`` when unset.

    Returns:
        Threshold in seconds — the parsed value clamped to
        :data:`MIN_THRESHOLD_SECONDS`, or
        :data:`DEFAULT_THRESHOLD_SECONDS` when unset, unparseable, or
        non-positive.
    """
    if env_value is None:
        return DEFAULT_THRESHOLD_SECONDS
    try:
        parsed = int(env_value)
    except ValueError:
        return DEFAULT_THRESHOLD_SECONDS
    if parsed <= 0:
        return DEFAULT_THRESHOLD_SECONDS
    return max(parsed, MIN_THRESHOLD_SECONDS)


def _resolve_exchange_thresholds(env_value: str | None) -> dict[str, int]:
    """Parse per-exchange threshold overrides from ``exchange=seconds`` CSV.

    ``0`` (or any negative value) disables monitoring for that
    exchange; positive values are clamped to
    :data:`MIN_THRESHOLD_SECONDS`. Malformed entries are skipped so a
    typo can never take the whole watchdog down.

    Args:
        env_value: Raw env-var value, or ``None`` when unset.

    Returns:
        Mapping of exchange identifier to threshold seconds.
    """
    overrides: dict[str, int] = {}
    if env_value is None:
        return overrides
    for entry in env_value.split(","):
        key, separator, raw_value = entry.partition("=")
        exchange = key.strip().lower()
        if not separator or not exchange:
            continue
        try:
            parsed = int(raw_value.strip())
        except ValueError:
            continue
        if parsed <= 0:
            overrides[exchange] = 0
        else:
            overrides[exchange] = max(parsed, MIN_THRESHOLD_SECONDS)
    return overrides


class MarketDataWatchdog:
    """Detects whole-exchange market-data silence via DB candle freshness.

    Lifecycle mirrors the other API-lifespan monitors
    (:class:`RetentionScheduler`, ``SystemMetricsSnapshotter``):
    :meth:`start` takes one eager tick then spawns the poll loop;
    :meth:`stop` signals the loop and cancels + awaits its task. Every
    tick is wrapped defensively — one bad tick (DB hiccup, publisher
    error) logs and the next tick still runs, because the watchdog
    dying silently would recreate the exact blind spot it exists to
    close.
    """

    def __init__(
        self,
        *,
        repo: Repository,
        msg_publisher: _HeartbeatPublisher | None = None,
        interval_seconds: float | None = None,
        default_threshold_seconds: int | None = None,
        exchange_thresholds: dict[str, int] | None = None,
        disabled: bool | None = None,
    ) -> None:
        """Wire dependencies and resolve configuration.

        Args:
            repo: Repository used for the per-exchange freshness query.
            msg_publisher: Shared ZMQ publisher for the synthetic
                heartbeat bursts. ``None`` degrades to detection +
                logging only (no alert path), matching the snapshotter's
                metrics-only startup mode.
            interval_seconds: Poll cadence override; ``None`` reads
                ``MARKET_DATA_WATCHDOG_INTERVAL_SECONDS``.
            default_threshold_seconds: Default silence threshold
                override; ``None`` reads
                ``MARKET_DATA_WATCHDOG_THRESHOLD_SECONDS``.
            exchange_thresholds: Per-exchange overrides; ``None`` reads
                ``MARKET_DATA_WATCHDOG_EXCHANGE_THRESHOLDS``.
            disabled: Disable flag override; ``None`` reads
                ``MARKET_DATA_WATCHDOG_DISABLED``.
        """
        if disabled is None:
            disabled = _resolve_disabled(os.environ.get(_DISABLED_ENV_VAR))
        if interval_seconds is None:
            interval_seconds = _resolve_interval(os.environ.get(_INTERVAL_ENV_VAR))
        if default_threshold_seconds is None:
            default_threshold_seconds = _resolve_threshold(os.environ.get(_THRESHOLD_ENV_VAR))
        if exchange_thresholds is None:
            exchange_thresholds = _resolve_exchange_thresholds(
                os.environ.get(_EXCHANGE_THRESHOLDS_ENV_VAR)
            )
        self._repo = repo
        self._msg_publisher = msg_publisher
        self._disabled = disabled
        self._interval_seconds = interval_seconds
        self._default_threshold_seconds = default_threshold_seconds
        self._exchange_thresholds = dict(exchange_thresholds)
        self._watched: tuple[str, ...] = tuple(str(e) for e in get_args(MarketSubscribeExchange))
        self._sequences: dict[str, int] = {}
        self._stopping = asyncio.Event()
        self._loop_task: asyncio.Task[None] | None = None

    @property
    def disabled(self) -> bool:
        """Return whether the watchdog is parked.

        Returns:
            ``True`` iff ``MARKET_DATA_WATCHDOG_DISABLED`` (or the
            constructor override) was truthy — no eager tick, no loop.
        """
        return self._disabled

    @property
    def interval_seconds(self) -> float:
        """Return the configured poll cadence in seconds.

        Returns:
            Seconds the loop sleeps between ticks.
        """
        return self._interval_seconds

    def threshold_for(self, exchange: str) -> int:
        """Return the effective silence threshold for one exchange.

        Args:
            exchange: Exchange identifier (lowercase).

        Returns:
            Threshold in seconds; ``0`` means monitoring is disabled
            for the exchange.
        """
        return self._exchange_thresholds.get(exchange, self._default_threshold_seconds)

    async def start(self) -> None:
        """Take one eager (defensive) tick then spawn the poll loop.

        When :attr:`disabled` is true, returns immediately — the
        watchdog is parked. The eager tick is wrapped like loop ticks:
        a transient DB error at boot must not park the watchdog for
        the process lifetime.
        """
        if self._disabled:
            logger.info("MarketDataWatchdog: disabled (%s=true); skipping start", _DISABLED_ENV_VAR)
            return
        await self._guarded_tick()
        self._stopping.clear()
        self._loop_task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        """Signal the loop to exit and cancel + await its task.

        Tolerates the parked state where :meth:`start` never spawned
        the loop.
        """
        self._stopping.set()
        task = self._loop_task
        if task is not None and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._loop_task = None

    async def _loop(self) -> None:
        """Sleep the interval then tick; repeat until stopped."""
        while not self._stopping.is_set():
            await asyncio.sleep(self._interval_seconds)
            if self._stopping.is_set():
                return
            await self._guarded_tick()

    async def _guarded_tick(self) -> None:
        """Run one tick, logging instead of raising on any failure."""
        try:
            await self._tick()
        except Exception:
            logger.exception("MarketDataWatchdog tick failed; continuing on next tick")

    async def _tick(self, now: datetime | None = None) -> None:
        """Evaluate every watched exchange once and burst for silent ones.

        An exchange whose active instruments have no candle rows at all
        (``latest_open_at is None``) is deliberately skipped: it has no
        silence baseline yet, and alerting there would page on every
        fresh database or newly-enabled exchange before its feed first
        connects. Monitoring begins with the first persisted candle.

        Args:
            now: Reference instant override for deterministic tests;
                ``None`` uses ``datetime.now(UTC)``.
        """
        reference_now = now if now is not None else datetime.now(UTC)
        rows = await self._repo.get_latest_candle_open_at_by_exchange(
            exchanges=self._watched, now=reference_now
        )
        for row in rows:
            exchange = row["exchange"]
            latest_open_at = row["latest_open_at"]
            threshold_seconds = self.threshold_for(exchange)
            if threshold_seconds <= 0:
                continue
            if latest_open_at is None:
                continue
            if exchange == ExchangeEnum.KRAKEN_EQUITIES:
                if is_cme_closed(reference_now):
                    continue
                silence_start = max(
                    latest_open_at + timedelta(seconds=_CANDLE_MINUTE_SECONDS),
                    last_cme_reopen(reference_now),
                )
            else:
                silence_start = latest_open_at + timedelta(seconds=_CANDLE_MINUTE_SECONDS)
            silent_seconds = (reference_now - silence_start).total_seconds()
            if silent_seconds < threshold_seconds:
                continue
            logger.warning(
                "MarketDataWatchdog: exchange %s silent for %ds (threshold %ds, "
                "latest candle open_at %s)",
                exchange,
                int(silent_seconds),
                threshold_seconds,
                latest_open_at.isoformat(),
            )
            await self._publish_silence_burst(
                exchange=exchange,
                silent_seconds=int(silent_seconds),
                threshold_seconds=threshold_seconds,
                latest_open_at=latest_open_at,
            )

    async def _publish_silence_burst(
        self,
        *,
        exchange: str,
        silent_seconds: int,
        threshold_seconds: int,
        latest_open_at: datetime,
    ) -> None:
        """Publish one 3-frame synthetic WARNING heartbeat burst.

        Mirrors the launcher's park-heartbeat mechanics: 3 frames
        because the critical-system-error rule gates on 3 consecutive
        non-HEALTHY beats, spaced 2 s apart to clear the bridge's 1 s
        heartbeat throttle. Send failures are swallowed per frame —
        the detection path must never raise; the next tick re-bursts
        (level-triggered).

        Args:
            exchange: Silent exchange identifier.
            silent_seconds: Whole seconds since the exchange's last
                candle minute ended (or since the last CME reopen).
            threshold_seconds: Effective threshold that was exceeded.
            latest_open_at: ``open_at`` of the exchange's newest candle.
        """
        publisher = self._msg_publisher
        if publisher is None:
            return
        topic = heartbeat_topic(WATCHDOG_HEARTBEAT_COMPONENT, exchange)
        for frame_index in range(_BURST_FRAME_COUNT):
            if frame_index:
                await asyncio.sleep(_BURST_FRAME_SPACING_S)
            try:
                tracker = publisher.tracker
                sequence = self._sequences.get(exchange, 0) + 1
                self._sequences[exchange] = sequence
                meta: JsonObject = {
                    "synthetic": True,
                    "origin": "market_data_watchdog",
                    "reason": "exchange_silent",
                    "exchange": exchange,
                    "silent_seconds": silent_seconds,
                    "threshold_seconds": threshold_seconds,
                    "latest_candle_open_at": latest_open_at.isoformat(),
                }
                frame = HeartbeatData(
                    public_id=str(uuid7()),
                    timestamp=datetime.now(UTC),
                    session_id=tracker.session_id,
                    sequence_id=tracker.next_sequence(topic),
                    component=f"{WATCHDOG_HEARTBEAT_COMPONENT}.{exchange}",
                    sequence=sequence,
                    status=HealthStatusEnum.WARNING,
                    lag_ms=silent_seconds * 1000,
                    meta=meta,
                )
                await publisher.send(topic, frame)
            except Exception:
                logger.exception("MarketDataWatchdog: silence heartbeat for %s failed", exchange)
