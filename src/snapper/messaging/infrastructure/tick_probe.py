"""Per-stage timing histograms for the market-data publisher hot-path.

The probe lets an operator identify which step in :meth:`MarketDataPublisherService._process_tick`
caps single-consumer throughput when a publisher reports sustained
``_enqueue_or_drop_oldest`` drops (see Kraken spot wildcard scenario,
2026-05-15 brainstorm by Codex + Copilot architects).

Activation
----------
Set ``SNAPPER_TICK_PROBE=1`` (also accepts ``true`` / ``yes``,
case-insensitive) before the publisher process starts. When unset every
:meth:`TickProbe.record` and :meth:`TickProbe.maybe_flush` call is a
single attribute load + branch + return; the production hot-path stays
untouched.

Stages
------
Stage names are validated at construction time. Adding a stage means
adding it to :data:`TICK_PROBE_STAGES`; the probe rejects unknown
labels at :meth:`record` time to catch call-site typos.

The intended stages match the per-tick path:

* ``build_topic`` — ``_build_data_topic`` f-string
* ``build_tick_model`` — Pydantic ``TickData(...)`` construction
* ``publish_to`` — ``model_copy`` + ``model_dump_json`` + encode inside
  :meth:`MessagePublisher.send`
* ``validate_topic`` — regex validation in
  :meth:`ValidatedPublisher.send_multipart`
* ``socket_send`` — ``await self._socket.send_multipart(...)``
* ``ensure_instrument`` — symbol → instrument cache lookup
* ``build_row`` — ``_build_tick_row`` dict assembly
* ``tick_total`` — end-to-end ``_process_tick`` cost

Output
------
One ``logger.info`` line every :data:`FLUSH_INTERVAL_S` seconds with
per-stage ``count / rate / p50 / p95 / p99 / max``. Accumulators reset
after every flush so the line always describes the most recent window.
Queue saturation signal comes from the existing
``_enqueue_or_drop_oldest`` drop counter; the probe deliberately does
not duplicate it.
"""

import bisect
import math
import os
from time import perf_counter_ns
from typing import Final

from loguru import logger

TICK_PROBE_ENV_VAR: Final = "SNAPPER_TICK_PROBE"
"""Environment variable that turns the probe on."""

FLUSH_INTERVAL_S: Final = 10.0
"""Seconds between successive flush log lines."""

_FLUSH_INTERVAL_NS: Final = int(FLUSH_INTERVAL_S * 1_000_000_000)

BUCKET_BOUNDARIES_NS: Final[tuple[int, ...]] = (
    1_000,
    2_500,
    5_000,
    10_000,
    25_000,
    50_000,
    100_000,
    250_000,
    500_000,
    1_000_000,
    2_500_000,
    5_000_000,
    10_000_000,
    25_000_000,
    50_000_000,
    100_000_000,
)
"""Bucket upper bounds for the per-stage latency histogram.

Spans 1 µs → 100 ms with ~2.5× spacing — fine enough to separate
sub-microsecond dict ops from millisecond ZMQ stalls without paying for
a t-digest-style structure.
"""

TICK_PROBE_STAGES: Final[tuple[str, ...]] = (
    "build_topic",
    "build_tick_model",
    "publish_to",
    "validate_topic",
    "socket_send",
    "ensure_instrument",
    "build_row",
    "tick_total",
    "iter_wait",
    "tick_iter_total",
)
"""Canonical list of stage labels accepted by :meth:`TickProbe.record`.

Any other label raises :class:`KeyError` immediately to catch typos at
call sites instead of silently dropping observations.
"""


def resolve_enabled(env_value: str | None) -> bool:
    """Coerce the :data:`TICK_PROBE_ENV_VAR` value to a boolean.

    Recognises ``"1"`` / ``"true"`` / ``"yes"`` case-insensitively as
    on; every other value (including ``None``) is off. Trimming
    surrounding whitespace lets operators paste values with stray
    spaces without surprises.

    Args:
        env_value: Raw value read from ``os.environ`` (or ``None`` when
            the variable is unset).

    Returns:
        ``True`` when the probe should record observations.
    """
    if env_value is None:
        return False
    return env_value.strip().lower() in {"1", "true", "yes"}


class TickProbe:
    """Per-stage latency histograms with periodic ``logger.info`` flush.

    Instances are cheap to construct; tests build their own ``TickProbe``
    with ``enabled=True`` instead of mutating the environment. The
    module-level singleton in :func:`get_probe` reads the env variable
    exactly once at import.

    Thread-safety: the probe is designed for a single-event-loop
    publisher process. Concurrent ``record`` calls from multiple
    threads are not safe; the histograms are plain integer arrays.
    """

    def __init__(self, enabled: bool) -> None:
        """Initialize empty histograms.

        Args:
            enabled: When ``False``, all ``record`` / ``maybe_flush``
                calls return immediately.
        """
        self.enabled = enabled
        self._counts: dict[str, int] = dict.fromkeys(TICK_PROBE_STAGES, 0)
        self._max_ns: dict[str, int] = dict.fromkeys(TICK_PROBE_STAGES, 0)
        self._buckets: dict[str, list[int]] = {
            stage: [0] * (len(BUCKET_BOUNDARIES_NS) + 1) for stage in TICK_PROBE_STAGES
        }
        self._last_flush_ns: int = perf_counter_ns()

    def record(self, stage: str, delta_ns: int) -> None:
        """Record one observation of ``stage`` taking ``delta_ns`` nanoseconds.

        Returns immediately when the probe is disabled so call sites can
        invoke ``record`` unconditionally without paying for the
        accumulator update. Unknown stage names raise :class:`KeyError`
        to catch typos at call sites.

        Args:
            stage: One of :data:`TICK_PROBE_STAGES`.
            delta_ns: Elapsed time for the stage, in nanoseconds.
        """
        if not self.enabled:
            return
        self._counts[stage] += 1
        if delta_ns > self._max_ns[stage]:
            self._max_ns[stage] = delta_ns
        bucket_idx = bisect.bisect_left(BUCKET_BOUNDARIES_NS, delta_ns)
        self._buckets[stage][bucket_idx] += 1

    def maybe_flush(self) -> None:
        """Emit one flush log line if :data:`FLUSH_INTERVAL_S` has elapsed.

        Designed to be called from the publisher consumer's main loop;
        the cost when no flush is due is one ``perf_counter_ns()`` call
        + one subtraction + one branch.
        """
        if not self.enabled:
            return
        now_ns = perf_counter_ns()
        if now_ns - self._last_flush_ns < _FLUSH_INTERVAL_NS:
            return
        self._flush(now_ns)

    def _flush(self, now_ns: int) -> None:
        """Emit the periodic summary log line and reset accumulators.

        Args:
            now_ns: Monotonic timestamp used to size the flush window.
        """
        elapsed_s = (now_ns - self._last_flush_ns) / 1_000_000_000
        parts: list[str] = []
        for stage in TICK_PROBE_STAGES:
            count = self._counts[stage]
            if count == 0:
                continue
            rate = count / elapsed_s
            p50_us = self._percentile_us(stage, 0.50)
            p95_us = self._percentile_us(stage, 0.95)
            p99_us = self._percentile_us(stage, 0.99)
            max_us = self._max_ns[stage] // 1_000
            parts.append(
                f"{stage}={count} ({rate:.0f}/s) "
                f"p50={p50_us}us p95={p95_us}us p99={p99_us}us max={max_us}us"
            )
        body = " | ".join(parts) if parts else "no stage observations"
        logger.info(f"TickProbe[{elapsed_s:.1f}s] {body}")
        self._reset()
        self._last_flush_ns = now_ns

    def _percentile_us(self, stage: str, pct: float) -> int:
        """Return approximate percentile of stage histogram in microseconds.

        Walks the histogram from the smallest bucket until the cumulative
        count crosses ``ceil(count * pct)``. Returns the upper bound of
        the crossing bucket (clamped to the highest defined boundary
        when the crossing lands in the overflow slot) so the value is
        conservative — never reports a percentile *smaller* than
        reality.

        Args:
            stage: Stage label from :data:`TICK_PROBE_STAGES`.
            pct: Percentile fraction in ``[0.0, 1.0]``.

        Returns:
            Percentile value in microseconds rounded to the bucket
            upper bound. Returns ``0`` when the stage has no
            observations in the current window.
        """
        count = self._counts[stage]
        if count == 0:
            return 0
        threshold = max(1, math.ceil(count * pct))
        cumulative = 0
        chosen_idx = 0
        for idx, observations in enumerate(self._buckets[stage]):
            if cumulative >= threshold:
                break
            cumulative += observations
            chosen_idx = idx
        boundary_idx = min(chosen_idx, len(BUCKET_BOUNDARIES_NS) - 1)
        return BUCKET_BOUNDARIES_NS[boundary_idx] // 1_000

    def _reset(self) -> None:
        """Clear all accumulators ready for the next flush window."""
        for stage in TICK_PROBE_STAGES:
            self._counts[stage] = 0
            self._max_ns[stage] = 0
            bucket_list = self._buckets[stage]
            for idx in range(len(bucket_list)):
                bucket_list[idx] = 0


_probe_singleton: TickProbe = TickProbe(enabled=resolve_enabled(os.environ.get(TICK_PROBE_ENV_VAR)))


def get_probe() -> TickProbe:
    """Return the module-level singleton.

    Imported by publisher call sites that record stage timings. The
    singleton's ``enabled`` flag is fixed at process start by the
    :data:`TICK_PROBE_ENV_VAR` value; tests that need to flip the flag
    construct their own :class:`TickProbe` instead of mutating this
    instance.

    Returns:
        The shared :class:`TickProbe`.
    """
    return _probe_singleton
