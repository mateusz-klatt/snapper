"""Unit tests for :mod:`snapper.application.services.market_stats`.

Coverage targets: pair-key + pair-spec validation, parse_pair_specs
cap enforcement + per-row drop, align_closes intersection,
safe_pearson zero-variance + NaN protection, single-flight Pearson +
cointegration tick, settings reload, lifecycle (start / stop / empty
endpoint / restart with running task / reap completed), listener
loop, recv decode.
"""

import asyncio
import contextlib
from datetime import UTC
from datetime import datetime
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import numpy as np
import pytest

from snapper.application.services.market_cache import CandleSnap
from snapper.application.services.market_cache import MarketCacheService
from snapper.application.services.market_cache import PairStats
from snapper.application.services.market_stats import MarketStatsWorker
from snapper.application.services.market_stats import PairSpec
from snapper.application.services.market_stats import StatsPairConfigError
from snapper.application.services.market_stats import _align_closes
from snapper.application.services.market_stats import _safe_pearson
from snapper.application.services.market_stats import parse_pair_key
from snapper.application.services.market_stats import parse_pair_spec
from snapper.application.services.market_stats import parse_pair_specs
from snapper.core.types import ExchangeEnum


def _snap(open_at_ms: int, close: float) -> CandleSnap:
    """Compact :class:`CandleSnap` builder for the alignment tests."""
    return CandleSnap(
        open_at_ms=open_at_ms,
        open=close,
        high=close + 0.1,
        low=close - 0.1,
        close=close,
        volume=1.0,
    )


class _StubSettings:
    """In-memory SettingsService replacement supporting ``get_setting``."""

    def __init__(self, values: dict[str, Any]) -> None:
        self._values = values

    def get_setting(self, key: str, default: Any = None) -> Any:
        return self._values.get(key, default)


def _stub_cache(
    *,
    capacity: int = 100,
    left_candles: list[CandleSnap] | None = None,
    right_candles: list[CandleSnap] | None = None,
) -> MagicMock:
    """Mock cache with the methods :class:`MarketStatsWorker` calls."""
    cache = MagicMock(spec=MarketCacheService)
    cache.cache_capacity_per_instrument = MagicMock(return_value=capacity)
    candle_responses = iter([left_candles or [], right_candles or []])

    async def _get_candles(*_args: Any, **_kwargs: Any) -> list[CandleSnap]:
        return next(candle_responses, [])

    cache.get_1m_candles = AsyncMock(side_effect=_get_candles)
    stats_store: dict[tuple[str, str], PairStats] = {}

    async def _get_stats(left: str, right: str) -> PairStats | None:
        return stats_store.get((left, right))

    async def _set_stats(left: str, right: str, stats: PairStats) -> None:
        stats_store[(left, right)] = stats

    cache.get_pair_stats = AsyncMock(side_effect=_get_stats)
    cache.set_pair_stats = AsyncMock(side_effect=_set_stats)
    cache.pair_stats_keys = AsyncMock(side_effect=lambda: list(stats_store.keys()))
    cache._stub_stats_store = stats_store
    return cache


class TestPairKeyParser:
    """``parse_pair_key`` strict validation."""

    def test_happy_path(self) -> None:
        """A valid ``exchange:symbol`` string parses to ``(exchange, symbol)``."""
        assert parse_pair_key("kraken:BTC-USD") == (ExchangeEnum.KRAKEN, "BTC-USD")

    @pytest.mark.parametrize(
        "raw",
        [
            "no-separator",
            "too:many:colons",
            ":empty-exchange",
            "kraken:",
            "  :  ",
            "bogus-exchange:BTC-USD",
        ],
    )
    def test_malformed_raises(self, raw: str) -> None:
        """Every shape-incompatible input raises :class:`StatsPairConfigError`."""
        with pytest.raises(StatsPairConfigError):
            parse_pair_key(raw)

    def test_non_string_raises(self) -> None:
        """A non-string input raises before splitting."""
        with pytest.raises(StatsPairConfigError):
            parse_pair_key(cast(Any, 42))


class TestPairSpecParser:
    """``parse_pair_spec`` + ``parse_pair_specs`` validation."""

    def test_spec_round_trip(self) -> None:
        """A valid ``left|right`` round-trips to ``PairSpec`` halves + str forms."""
        spec = parse_pair_spec("kraken:BTC-USD|kraken:ETH-USD")
        assert spec.left == (ExchangeEnum.KRAKEN, "BTC-USD")
        assert spec.right == (ExchangeEnum.KRAKEN, "ETH-USD")
        assert spec.left_str == "kraken:BTC-USD"
        assert spec.right_str == "kraken:ETH-USD"

    def test_self_referential_rejected(self) -> None:
        """``left == right`` is rejected to avoid 1.0-correlation noise."""
        with pytest.raises(StatsPairConfigError):
            parse_pair_spec("kraken:BTC-USD|kraken:BTC-USD")

    def test_wrong_separator_rejected(self) -> None:
        """A ``,`` separator is rejected — pipe is the canonical form."""
        with pytest.raises(StatsPairConfigError):
            parse_pair_spec("kraken:BTC-USD,kraken:ETH-USD")

    def test_specs_drop_malformed_entries(self) -> None:
        """A bad row is silently dropped with a warning; good rows survive."""
        raw = ["kraken:BTC-USD|kraken:ETH-USD", "garbage"]
        specs = parse_pair_specs(raw)
        assert len(specs) == 1
        assert specs[0].left_str == "kraken:BTC-USD"

    def test_specs_cap_overflow_raises(self) -> None:
        """A config with 51 pairs exceeds the hard cap and raises."""
        raw = [f"kraken:S{i}|kraken:R{i}" for i in range(51)]
        with pytest.raises(StatsPairConfigError, match="exceeds cap"):
            parse_pair_specs(raw)

    def test_specs_non_list_raises(self) -> None:
        """A non-list shape (e.g. a string) raises at the top level."""
        with pytest.raises(StatsPairConfigError, match="must be a list"):
            parse_pair_specs(cast(Any, "not-a-list"))


class TestAlignmentAndStats:
    """``_align_closes`` + ``_safe_pearson`` numerical correctness."""

    def test_align_closes_intersects_on_open_at(self) -> None:
        """Only ``open_at_ms`` values present in both streams contribute."""
        left = [_snap(0, 1.0), _snap(60_000, 2.0), _snap(120_000, 3.0)]
        right = [_snap(60_000, 10.0), _snap(120_000, 20.0), _snap(180_000, 30.0)]
        left_arr, right_arr = _align_closes(left, right)
        assert list(left_arr) == [2.0, 3.0]
        assert list(right_arr) == [10.0, 20.0]

    def test_align_closes_empty_intersection_returns_empty(self) -> None:
        """Disjoint streams return paired empty arrays."""
        left = [_snap(0, 1.0)]
        right = [_snap(60_000, 2.0)]
        left_arr, right_arr = _align_closes(left, right)
        assert left_arr.size == 0
        assert right_arr.size == 0

    def test_safe_pearson_returns_one_for_identical(self) -> None:
        """Two identical sequences correlate at 1.0."""
        left = np.array([1.0, 2.0, 3.0])
        right = np.array([1.0, 2.0, 3.0])
        assert _safe_pearson(left, right) == pytest.approx(1.0)

    def test_safe_pearson_returns_none_for_constant_left(self) -> None:
        """A zero-variance left leg signals no measurable correlation."""
        left = np.array([1.0, 1.0, 1.0])
        right = np.array([1.0, 2.0, 3.0])
        assert _safe_pearson(left, right) is None

    def test_safe_pearson_returns_none_for_constant_right(self) -> None:
        """Symmetric guard: zero-variance right leg also returns ``None``."""
        left = np.array([1.0, 2.0, 3.0])
        right = np.array([5.0, 5.0, 5.0])
        assert _safe_pearson(left, right) is None

    def test_safe_pearson_returns_none_for_short_input(self) -> None:
        """A single-sample input cannot produce a meaningful correlation."""
        assert _safe_pearson(np.array([1.0]), np.array([2.0])) is None

    def test_safe_pearson_returns_none_on_nan_correlation(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A NaN result from corrcoef projects to ``None`` to keep JSON clean."""
        monkeypatch.setattr(
            "snapper.application.services.market_stats.np.corrcoef",
            lambda _l, _r: np.array([[1.0, float("nan")], [float("nan"), 1.0]]),
        )
        assert _safe_pearson(np.array([1.0, 2.0, 3.0]), np.array([4.0, 5.0, 6.0])) is None


class TestPearsonTick:
    """Single-pair Pearson tick under various sample-count regimes."""

    @pytest.mark.asyncio
    async def test_pearson_tick_warm_writes_correlation(self) -> None:
        """Aligned closes ≥ threshold trigger a warm Pearson write."""
        spec = parse_pair_spec("kraken:BTC-USD|kraken:ETH-USD")
        left = [_snap(i * 60_000, float(i)) for i in range(30)]
        right = [_snap(i * 60_000, float(i) * 2.0) for i in range(30)]
        cache = _stub_cache(left_candles=left, right_candles=right)
        worker = MarketStatsWorker(
            cache=cast(Any, cache), settings_service=cast(Any, _StubSettings({}))
        )
        async with worker._pairs_lock:
            worker._pairs = [spec]
            worker._pair_locks = {(spec.left_str, spec.right_str): asyncio.Lock()}

        await worker.run_pearson_once()

        stats = cache._stub_stats_store[(spec.left_str, spec.right_str)]
        assert stats.pearson_r == pytest.approx(1.0)
        assert stats.pearson_n == 30
        assert stats.is_warm is True

    @pytest.mark.asyncio
    async def test_pearson_tick_cold_skips_compute(self) -> None:
        """Aligned closes < threshold mark stats as not warm without computing."""
        spec = parse_pair_spec("kraken:BTC-USD|kraken:ETH-USD")
        left = [_snap(i * 60_000, float(i)) for i in range(5)]
        right = [_snap(i * 60_000, float(i)) for i in range(5)]
        cache = _stub_cache(left_candles=left, right_candles=right)
        worker = MarketStatsWorker(
            cache=cast(Any, cache), settings_service=cast(Any, _StubSettings({}))
        )
        async with worker._pairs_lock:
            worker._pairs = [spec]
            worker._pair_locks = {(spec.left_str, spec.right_str): asyncio.Lock()}

        await worker.run_pearson_once()

        stats = cache._stub_stats_store[(spec.left_str, spec.right_str)]
        assert stats.is_warm is False
        assert stats.sample_count == 5
        assert stats.pearson_r is None

    @pytest.mark.asyncio
    async def test_pearson_tick_skips_when_lock_held(self) -> None:
        """A pair whose lock is held (in-flight) is skipped this cadence."""
        spec = parse_pair_spec("kraken:BTC-USD|kraken:ETH-USD")
        cache = _stub_cache()
        worker = MarketStatsWorker(
            cache=cast(Any, cache), settings_service=cast(Any, _StubSettings({}))
        )
        held = asyncio.Lock()
        await held.acquire()
        async with worker._pairs_lock:
            worker._pairs = [spec]
            worker._pair_locks = {(spec.left_str, spec.right_str): held}

        await worker.run_pearson_once()

        cache.get_1m_candles.assert_not_awaited()
        held.release()


class TestCointegrationTick:
    """Single-pair cointegration tick + threshold logic."""

    @pytest.mark.asyncio
    async def test_coint_tick_warm_writes_triple(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Sufficient samples + stubbed coint produces a triple + computed_at."""
        spec = parse_pair_spec("kraken:BTC-USD|kraken:ETH-USD")
        left = [_snap(i * 60_000, float(i)) for i in range(60)]
        right = [_snap(i * 60_000, float(i) + 0.1) for i in range(60)]
        cache = _stub_cache(left_candles=left, right_candles=right)
        worker = MarketStatsWorker(
            cache=cast(Any, cache), settings_service=cast(Any, _StubSettings({}))
        )
        async with worker._pairs_lock:
            worker._pairs = [spec]
            worker._pair_locks = {(spec.left_str, spec.right_str): asyncio.Lock()}

        def _fake_coint(_left: Any, _right: Any) -> tuple[float, float, tuple[float, ...]]:
            return (-3.5, 0.01, (-3.9, -3.3, -3.0))

        monkeypatch.setattr(
            "snapper.application.services.market_stats._engle_granger_coint",
            _fake_coint,
        )

        await worker.run_cointegration_once()

        stats = cache._stub_stats_store[(spec.left_str, spec.right_str)]
        assert stats.coint_t == pytest.approx(-3.5)
        assert stats.coint_pvalue == pytest.approx(0.01)
        assert stats.coint_critical_values == pytest.approx((-3.9, -3.3, -3.0))
        assert stats.computed_at is not None

    @pytest.mark.asyncio
    async def test_coint_tick_skips_when_short(self) -> None:
        """Below the 60-sample threshold the tick exits without writing."""
        spec = parse_pair_spec("kraken:BTC-USD|kraken:ETH-USD")
        left = [_snap(i * 60_000, float(i)) for i in range(20)]
        right = [_snap(i * 60_000, float(i)) for i in range(20)]
        cache = _stub_cache(left_candles=left, right_candles=right)
        worker = MarketStatsWorker(
            cache=cast(Any, cache), settings_service=cast(Any, _StubSettings({}))
        )
        async with worker._pairs_lock:
            worker._pairs = [spec]
            worker._pair_locks = {(spec.left_str, spec.right_str): asyncio.Lock()}

        await worker.run_cointegration_once()

        assert (spec.left_str, spec.right_str) not in cache._stub_stats_store

    @pytest.mark.asyncio
    async def test_coint_tick_handles_compute_exception(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An exception inside statsmodels is logged + swallowed."""
        spec = parse_pair_spec("kraken:BTC-USD|kraken:ETH-USD")
        left = [_snap(i * 60_000, float(i)) for i in range(60)]
        right = [_snap(i * 60_000, float(i)) for i in range(60)]
        cache = _stub_cache(left_candles=left, right_candles=right)
        worker = MarketStatsWorker(
            cache=cast(Any, cache), settings_service=cast(Any, _StubSettings({}))
        )
        async with worker._pairs_lock:
            worker._pairs = [spec]
            worker._pair_locks = {(spec.left_str, spec.right_str): asyncio.Lock()}

        def _raise(*_args: Any, **_kwargs: Any) -> None:
            raise ValueError("statsmodels boom")

        monkeypatch.setattr(
            "snapper.application.services.market_stats._engle_granger_coint",
            _raise,
        )

        await worker.run_cointegration_once()

        assert (spec.left_str, spec.right_str) not in cache._stub_stats_store

    @pytest.mark.asyncio
    async def test_coint_tick_skips_when_lock_held(self) -> None:
        """A pair whose lock is held is skipped (single-flight guard)."""
        spec = parse_pair_spec("kraken:BTC-USD|kraken:ETH-USD")
        cache = _stub_cache()
        worker = MarketStatsWorker(
            cache=cast(Any, cache), settings_service=cast(Any, _StubSettings({}))
        )
        held = asyncio.Lock()
        await held.acquire()
        async with worker._pairs_lock:
            worker._pairs = [spec]
            worker._pair_locks = {(spec.left_str, spec.right_str): held}

        await worker.run_cointegration_once()

        cache.get_1m_candles.assert_not_awaited()
        held.release()


class TestConfigReload:
    """``_reload_config`` swap semantics + malformed-input preservation."""

    @pytest.mark.asyncio
    async def test_reload_swaps_in_new_pair_set(self) -> None:
        """A valid new config replaces the working pair set + creates placeholders."""
        cache = _stub_cache()
        settings = _StubSettings(
            {
                "market_stats_pairs": [
                    "kraken:BTC-USD|kraken:ETH-USD",
                    "kraken:SOL-USD|kraken:AVAX-USD",
                ]
            }
        )
        worker = MarketStatsWorker(cache=cast(Any, cache), settings_service=cast(Any, settings))

        await worker._reload_config()
        assert len(worker._pairs) == 2
        assert ("kraken:BTC-USD", "kraken:ETH-USD") in cache._stub_stats_store
        assert ("kraken:SOL-USD", "kraken:AVAX-USD") in cache._stub_stats_store

    @pytest.mark.asyncio
    async def test_reload_does_not_clobber_existing_stats(self) -> None:
        """A reload of a pair that already has stats leaves them in place."""
        cache = _stub_cache()
        spec_key = ("kraken:BTC-USD", "kraken:ETH-USD")
        prior = PairStats(pearson_r=0.7, pearson_n=42, is_warm=True, sample_count=42)
        cache._stub_stats_store[spec_key] = prior
        settings = _StubSettings({"market_stats_pairs": ["kraken:BTC-USD|kraken:ETH-USD"]})
        worker = MarketStatsWorker(cache=cast(Any, cache), settings_service=cast(Any, settings))

        await worker._reload_config()

        assert cache._stub_stats_store[spec_key] is prior

    @pytest.mark.asyncio
    async def test_reload_preserves_state_on_malformed_top_level(self) -> None:
        """A non-list config preserves the previous pair set."""
        cache = _stub_cache()
        original = parse_pair_spec("kraken:BTC-USD|kraken:ETH-USD")
        worker = MarketStatsWorker(
            cache=cast(Any, cache),
            settings_service=cast(Any, _StubSettings({"market_stats_pairs": "bad"})),
        )
        worker._pairs = [original]

        await worker._reload_config()

        assert worker._pairs == [original]


class TestListenerLifecycle:
    """``start`` / ``stop`` idempotency + recv path coverage."""

    @pytest.mark.asyncio
    async def test_start_with_empty_endpoint_skips_listener(self) -> None:
        """Empty XPUB skips the SUB socket; cadence tasks still run."""
        cache = _stub_cache()
        worker = MarketStatsWorker(
            cache=cast(Any, cache), settings_service=cast(Any, _StubSettings({}))
        )
        try:
            await worker.start("")
            assert worker._listen_task is None
            assert worker._pearson_task is not None
        finally:
            await worker.stop()

    @pytest.mark.asyncio
    async def test_stop_without_start_is_idempotent(self) -> None:
        """Stop without start is idempotent."""
        cache = _stub_cache()
        worker = MarketStatsWorker(
            cache=cast(Any, cache), settings_service=cast(Any, _StubSettings({}))
        )
        await worker.stop()
        assert worker._pearson_task is None

    @pytest.mark.asyncio
    async def test_start_with_running_task_returns_early(self) -> None:
        """A second start while pearson task is alive short-circuits."""
        cache = _stub_cache()
        worker = MarketStatsWorker(
            cache=cast(Any, cache), settings_service=cast(Any, _StubSettings({}))
        )
        live = asyncio.create_task(asyncio.sleep(10))
        worker._pearson_task = live
        try:
            await worker.start("inproc://nope")
            assert worker._pearson_task is live
        finally:
            live.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await live

    @pytest.mark.asyncio
    async def test_start_reaps_completed_prior_task(self) -> None:
        """Start reaps completed prior task."""
        cache = _stub_cache()
        worker = MarketStatsWorker(
            cache=cast(Any, cache), settings_service=cast(Any, _StubSettings({}))
        )

        async def _done() -> None:
            return None

        finished = asyncio.create_task(_done())
        await finished
        worker._pearson_task = finished

        try:
            await worker.start("")
            assert worker._pearson_task is not None
            assert worker._pearson_task is not finished
        finally:
            await worker.stop()

    @pytest.mark.asyncio
    async def test_start_with_real_endpoint_creates_subscriber(self) -> None:
        """Start with real endpoint creates subscriber."""
        cache = _stub_cache()
        worker = MarketStatsWorker(
            cache=cast(Any, cache), settings_service=cast(Any, _StubSettings({}))
        )
        try:
            await worker.start("inproc://stats-worker-test")
            assert worker._subscriber is not None
            assert worker._listen_task is not None
        finally:
            await worker.stop()


class TestListenLoop:
    """Settings listener routes ``system.settings`` to ``_reload_config``."""

    @pytest.mark.asyncio
    async def test_settings_event_triggers_reload(self) -> None:
        """Settings event triggers reload."""
        cache = _stub_cache()
        seen_calls = {"n": 0}

        async def _reload() -> None:
            seen_calls["n"] += 1

        worker = MarketStatsWorker(
            cache=cast(Any, cache), settings_service=cast(Any, _StubSettings({}))
        )
        worker._reload_config = _reload
        worker._running = True

        async def _recv_then_stop() -> tuple[bytes, bytes]:
            worker._running = False
            return (b"system.settings", b"{}")

        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(side_effect=_recv_then_stop)
        worker._subscriber = subscriber

        await worker._listen_loop()
        assert seen_calls["n"] == 1

    @pytest.mark.asyncio
    async def test_unrelated_topic_does_not_reload(self) -> None:
        """Unrelated topic does not reload."""
        cache = _stub_cache()
        seen_calls = {"n": 0}

        async def _reload() -> None:
            seen_calls["n"] += 1

        worker = MarketStatsWorker(
            cache=cast(Any, cache), settings_service=cast(Any, _StubSettings({}))
        )
        worker._reload_config = _reload
        worker._running = True

        async def _recv_then_stop() -> tuple[bytes, bytes]:
            worker._running = False
            return (b"other.topic", b"{}")

        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(side_effect=_recv_then_stop)
        worker._subscriber = subscriber

        await worker._listen_loop()
        assert seen_calls["n"] == 0

    @pytest.mark.asyncio
    async def test_reload_failure_does_not_unwind_loop(self) -> None:
        """Reload failure does not unwind loop."""
        cache = _stub_cache()
        worker = MarketStatsWorker(
            cache=cast(Any, cache), settings_service=cast(Any, _StubSettings({}))
        )

        async def _boom() -> None:
            worker._running = False
            raise RuntimeError("config exploded")

        worker._reload_config = _boom
        worker._running = True

        async def _recv_then_stop() -> tuple[bytes, bytes]:
            return (b"system.settings", b"{}")

        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(side_effect=_recv_then_stop)
        worker._subscriber = subscriber

        await worker._listen_loop()

    @pytest.mark.asyncio
    async def test_listen_loop_returns_early_without_subscriber(self) -> None:
        """Listen loop returns early without subscriber."""
        cache = _stub_cache()
        worker = MarketStatsWorker(
            cache=cast(Any, cache), settings_service=cast(Any, _StubSettings({}))
        )
        worker._subscriber = None
        worker._running = True
        await worker._listen_loop()

    @pytest.mark.asyncio
    async def test_listen_loop_skips_when_recv_returns_none(self) -> None:
        """Listen loop skips when recv returns none."""
        cache = _stub_cache()
        worker = MarketStatsWorker(
            cache=cast(Any, cache), settings_service=cast(Any, _StubSettings({}))
        )

        async def _fail_then_stop() -> tuple[bytes, bytes]:
            worker._running = False
            raise RuntimeError("recv fail")

        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(side_effect=_fail_then_stop)
        worker._subscriber = subscriber
        worker._running = True

        await worker._listen_loop()

    @pytest.mark.asyncio
    async def test_listen_loop_propagates_cancellation(self) -> None:
        """Listen loop propagates cancellation."""
        cache = _stub_cache()
        worker = MarketStatsWorker(
            cache=cast(Any, cache), settings_service=cast(Any, _StubSettings({}))
        )

        async def _cancel() -> tuple[bytes, bytes]:
            raise asyncio.CancelledError()

        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(side_effect=_cancel)
        worker._subscriber = subscriber
        worker._running = True

        with pytest.raises(asyncio.CancelledError):
            await worker._listen_loop()


class TestRecvOneFrame:
    """``_recv_one_frame`` decode + failure paths."""

    @pytest.mark.asyncio
    async def test_decode_bytes_topic_and_payload(self) -> None:
        """Decode bytes topic and payload."""
        cache = _stub_cache()
        worker = MarketStatsWorker(
            cache=cast(Any, cache), settings_service=cast(Any, _StubSettings({}))
        )
        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(return_value=(b"system.settings", b'{"x": 1}'))
        assert await worker._recv_one_frame(subscriber) == (
            "system.settings",
            b'{"x": 1}',
        )

    @pytest.mark.asyncio
    async def test_coerces_str_inputs(self) -> None:
        """Coerces str inputs."""
        cache = _stub_cache()
        worker = MarketStatsWorker(
            cache=cast(Any, cache), settings_service=cast(Any, _StubSettings({}))
        )
        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(return_value=("topic-str", 42))
        assert await worker._recv_one_frame(subscriber) == ("topic-str", b"42")

    @pytest.mark.asyncio
    async def test_returns_none_on_failure(self) -> None:
        """Returns none on failure."""
        cache = _stub_cache()
        worker = MarketStatsWorker(
            cache=cast(Any, cache), settings_service=cast(Any, _StubSettings({}))
        )
        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(side_effect=RuntimeError("recv fail"))
        assert await worker._recv_one_frame(subscriber) is None

    @pytest.mark.asyncio
    async def test_propagates_cancellation(self) -> None:
        """Propagates cancellation."""
        cache = _stub_cache()
        worker = MarketStatsWorker(
            cache=cast(Any, cache), settings_service=cast(Any, _StubSettings({}))
        )
        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(side_effect=asyncio.CancelledError())
        with pytest.raises(asyncio.CancelledError):
            await worker._recv_one_frame(subscriber)


class TestCadenceLoops:
    """Background cadence loops drive their once-helpers + cancel cleanly."""

    @pytest.mark.asyncio
    async def test_pearson_loop_propagates_cancellation(self) -> None:
        """Pearson loop propagates cancellation."""
        cache = _stub_cache()
        worker = MarketStatsWorker(
            cache=cast(Any, cache), settings_service=cast(Any, _StubSettings({}))
        )
        worker._running = True
        task = asyncio.create_task(worker._pearson_loop())
        await asyncio.sleep(0)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    @pytest.mark.asyncio
    async def test_coint_loop_propagates_cancellation(self) -> None:
        """Coint loop propagates cancellation."""
        cache = _stub_cache()
        worker = MarketStatsWorker(
            cache=cast(Any, cache), settings_service=cast(Any, _StubSettings({}))
        )
        worker._running = True
        task = asyncio.create_task(worker._coint_loop())
        await asyncio.sleep(0)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    @pytest.mark.asyncio
    async def test_pearson_loop_runs_helper_each_iteration(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Pearson loop runs helper each iteration."""
        cache = _stub_cache()
        worker = MarketStatsWorker(
            cache=cast(Any, cache), settings_service=cast(Any, _StubSettings({}))
        )
        worker._running = True
        sleep_calls = {"n": 0}

        async def _fast_sleep(_seconds: float) -> None:
            sleep_calls["n"] += 1
            if sleep_calls["n"] >= 2:
                worker._running = False

        monkeypatch.setattr("snapper.application.services.market_stats.asyncio.sleep", _fast_sleep)
        helper_calls = {"n": 0}

        async def _spy() -> None:
            helper_calls["n"] += 1

        worker.run_pearson_once = _spy
        await worker._pearson_loop()
        assert helper_calls["n"] >= 1

    @pytest.mark.asyncio
    async def test_coint_loop_runs_helper_each_iteration(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Coint loop runs helper each iteration."""
        cache = _stub_cache()
        worker = MarketStatsWorker(
            cache=cast(Any, cache), settings_service=cast(Any, _StubSettings({}))
        )
        worker._running = True
        sleep_calls = {"n": 0}

        async def _fast_sleep(_seconds: float) -> None:
            sleep_calls["n"] += 1
            if sleep_calls["n"] >= 2:
                worker._running = False

        monkeypatch.setattr("snapper.application.services.market_stats.asyncio.sleep", _fast_sleep)
        helper_calls = {"n": 0}

        async def _spy() -> None:
            helper_calls["n"] += 1

        worker.run_cointegration_once = _spy
        await worker._coint_loop()
        assert helper_calls["n"] >= 1


class TestStatsMerge:
    """Partial updates from Pearson + cointegration do not clobber each other."""

    @pytest.mark.asyncio
    async def test_coint_write_preserves_prior_pearson(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Coint write preserves prior pearson."""
        spec = parse_pair_spec("kraken:BTC-USD|kraken:ETH-USD")
        left = [_snap(i * 60_000, float(i)) for i in range(60)]
        right = [_snap(i * 60_000, float(i) + 0.1) for i in range(60)]
        cache = _stub_cache(left_candles=left, right_candles=right)
        cache._stub_stats_store[(spec.left_str, spec.right_str)] = PairStats(
            pearson_r=0.9,
            pearson_n=42,
            is_warm=True,
            sample_count=42,
            computed_at=datetime(2026, 5, 13, 9, 0, tzinfo=UTC),
        )
        worker = MarketStatsWorker(
            cache=cast(Any, cache), settings_service=cast(Any, _StubSettings({}))
        )
        async with worker._pairs_lock:
            worker._pairs = [spec]
            worker._pair_locks = {(spec.left_str, spec.right_str): asyncio.Lock()}

        def _fake_coint(_left: Any, _right: Any) -> tuple[float, float, tuple[float, ...]]:
            return (-3.5, 0.01, (-3.9, -3.3, -3.0))

        monkeypatch.setattr(
            "snapper.application.services.market_stats._engle_granger_coint",
            _fake_coint,
        )

        await worker.run_cointegration_once()

        stats = cache._stub_stats_store[(spec.left_str, spec.right_str)]
        assert stats.pearson_r == pytest.approx(0.9)
        assert stats.pearson_n == 42
        assert stats.coint_t == pytest.approx(-3.5)


class TestConfiguredPairs:
    """Diagnostic accessor for routes."""

    def test_configured_pairs_returns_snapshot(self) -> None:
        """Configured pairs returns snapshot."""
        spec = PairSpec(
            left=(ExchangeEnum.KRAKEN, "BTC-USD"),
            right=(ExchangeEnum.KRAKEN, "ETH-USD"),
            left_str="kraken:BTC-USD",
            right_str="kraken:ETH-USD",
        )
        cache = _stub_cache()
        worker = MarketStatsWorker(
            cache=cast(Any, cache), settings_service=cast(Any, _StubSettings({}))
        )
        worker._pairs = [spec]
        snapshot = worker.configured_pairs()
        assert snapshot == [spec]
        assert snapshot is not worker._pairs
