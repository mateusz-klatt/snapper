"""Tests for the publisher hot-path trade probe."""

import importlib
import math
from time import perf_counter_ns
from typing import Final
from unittest.mock import patch

import pytest

from snapper.messaging.infrastructure import trade_probe
from snapper.messaging.infrastructure.trade_probe import BUCKET_BOUNDARIES_NS
from snapper.messaging.infrastructure.trade_probe import FLUSH_INTERVAL_S
from snapper.messaging.infrastructure.trade_probe import TRADE_PROBE_ENV_VAR
from snapper.messaging.infrastructure.trade_probe import TRADE_PROBE_STAGES
from snapper.messaging.infrastructure.trade_probe import TradeProbe
from snapper.messaging.infrastructure.trade_probe import get_probe
from snapper.messaging.infrastructure.trade_probe import resolve_enabled

_BUCKET_COUNT: Final = len(BUCKET_BOUNDARIES_NS) + 1


class TestResolveEnabled:
    """Tests for the env-var coercion helper."""

    def test_none_is_disabled(self) -> None:
        """Unset env var coerces to ``False``."""
        assert resolve_enabled(None) is False

    @pytest.mark.parametrize("value", ["1", "true", "yes", "TRUE", "Yes", " 1 "])
    def test_truthy_values_enable(self, value: str) -> None:
        """Each recognised truthy spelling activates the probe."""
        assert resolve_enabled(value) is True

    @pytest.mark.parametrize("value", ["", "0", "false", "no", "off", "maybe"])
    def test_falsy_values_disable(self, value: str) -> None:
        """Empty / explicit-off / unknown values keep the probe off."""
        assert resolve_enabled(value) is False


class TestTradeProbeDisabled:
    """A disabled probe must short-circuit every entry point."""

    def test_record_is_noop(self) -> None:
        """``record`` does not mutate any accumulator when disabled."""
        probe = TradeProbe(enabled=False)
        probe.record("build_topic", 12_345)
        assert probe._counts["build_topic"] == 0
        assert probe._max_ns["build_topic"] == 0
        assert sum(probe._buckets["build_topic"]) == 0

    def test_maybe_flush_is_noop(self) -> None:
        """``maybe_flush`` does not emit a log line when disabled."""
        probe = TradeProbe(enabled=False)
        probe._counts["build_topic"] = 1
        probe._buckets["build_topic"][0] = 1
        with patch.object(trade_probe.logger, "info") as info_mock:
            probe.maybe_flush()
            info_mock.assert_not_called()


class TestTradeProbeRecord:
    """Per-stage recording semantics."""

    def test_record_increments_count_max_and_bucket(self) -> None:
        """A single observation updates count, max, and the matching bucket."""
        probe = TradeProbe(enabled=True)
        probe.record("build_topic", 750)
        assert probe._counts["build_topic"] == 1
        assert probe._max_ns["build_topic"] == 750
        assert probe._buckets["build_topic"][0] == 1
        assert sum(probe._buckets["build_topic"]) == 1

    def test_record_keeps_running_max(self) -> None:
        """``record`` updates ``_max_ns`` only on a fresh maximum."""
        probe = TradeProbe(enabled=True)
        probe.record("build_topic", 5_000)
        probe.record("build_topic", 200)
        probe.record("build_topic", 7_500)
        assert probe._max_ns["build_topic"] == 7_500
        assert probe._counts["build_topic"] == 3

    def test_record_places_observations_in_correct_buckets(self) -> None:
        """``bisect_left`` puts each observation in the smallest matching bucket."""
        probe = TradeProbe(enabled=True)
        probe.record("build_topic", 999)
        probe.record("build_topic", 1_000)
        probe.record("build_topic", 1_500)
        probe.record("build_topic", 200_000_000)
        assert probe._buckets["build_topic"][0] == 2
        assert probe._buckets["build_topic"][1] == 1
        assert probe._buckets["build_topic"][_BUCKET_COUNT - 1] == 1

    def test_record_rejects_unknown_stage(self) -> None:
        """Typos at call sites raise ``KeyError`` immediately."""
        probe = TradeProbe(enabled=True)
        with pytest.raises(KeyError):
            probe.record("nonexistent_stage", 100)

    def test_writer_stages_are_recordable(self) -> None:
        """Writer-side stages accept observations like producer-side ones."""
        probe = TradeProbe(enabled=True)
        probe.record("writer_flush_total", 1_500_000)
        probe.record("writer_upsert_call", 900_000)
        assert probe._counts["writer_flush_total"] == 1
        assert probe._counts["writer_upsert_call"] == 1


class TestTradeProbeMaybeFlush:
    """Interval-gated flush behaviour."""

    def test_maybe_flush_skips_when_interval_not_elapsed(self) -> None:
        """No log line emitted while the flush window has not elapsed."""
        probe = TradeProbe(enabled=True)
        probe.record("build_topic", 1_500)
        with patch.object(trade_probe.logger, "info") as info_mock:
            probe.maybe_flush()
            info_mock.assert_not_called()

    def test_maybe_flush_emits_when_interval_elapsed(self) -> None:
        """A log line emits once ``FLUSH_INTERVAL_S`` has elapsed."""
        probe = TradeProbe(enabled=True)
        probe.record("build_topic", 1_500)
        probe._last_flush_ns = perf_counter_ns() - int((FLUSH_INTERVAL_S + 1) * 1_000_000_000)
        with patch.object(trade_probe.logger, "info") as info_mock:
            probe.maybe_flush()
            info_mock.assert_called_once()
            log_line = info_mock.call_args.args[0]
            assert "TradeProbe" in log_line
            assert "build_topic=1" in log_line

    def test_flush_resets_accumulators(self) -> None:
        """After flushing the counts, buckets, and max all return to zero."""
        probe = TradeProbe(enabled=True)
        probe.record("build_topic", 2_500)
        probe.record("publish", 25_000)
        probe.record("writer_upsert_call", 1_000_000)
        probe._last_flush_ns = perf_counter_ns() - int((FLUSH_INTERVAL_S + 1) * 1_000_000_000)
        with patch.object(trade_probe.logger, "info"):
            probe.maybe_flush()
        for stage in TRADE_PROBE_STAGES:
            assert probe._counts[stage] == 0
            assert probe._max_ns[stage] == 0
            assert sum(probe._buckets[stage]) == 0

    def test_flush_with_no_observations_logs_empty_marker(self) -> None:
        """Flushing an empty probe still emits a marker line."""
        probe = TradeProbe(enabled=True)
        probe._last_flush_ns = perf_counter_ns() - int((FLUSH_INTERVAL_S + 1) * 1_000_000_000)
        with patch.object(trade_probe.logger, "info") as info_mock:
            probe.maybe_flush()
            info_mock.assert_called_once()
            assert "no stage observations" in info_mock.call_args.args[0]


class TestPercentile:
    """Approximate-percentile semantics on the bucket histogram."""

    def test_returns_zero_when_no_observations(self) -> None:
        """Empty stage reports zero percentiles."""
        probe = TradeProbe(enabled=True)
        assert probe._percentile_us("build_topic", 0.5) == 0

    def test_break_path_normal_distribution(self) -> None:
        """p50 of a normal distribution lands inside the recorded buckets."""
        probe = TradeProbe(enabled=True)
        for _ in range(50):
            probe.record("build_topic", 500)
        for _ in range(50):
            probe.record("build_topic", 12_000)
        p50 = probe._percentile_us("build_topic", 0.5)
        assert p50 == BUCKET_BOUNDARIES_NS[0] // 1_000

    def test_overflow_bucket_clamps_to_last_boundary(self) -> None:
        """Observations beyond the largest boundary report the overflow bound."""
        probe = TradeProbe(enabled=True)
        probe.record("build_topic", 200_000_000)
        p99 = probe._percentile_us("build_topic", 0.99)
        assert p99 == BUCKET_BOUNDARIES_NS[-1] // 1_000

    def test_threshold_floor_when_pct_zero(self) -> None:
        """``pct=0`` still resolves a positive bucket because the floor is 1."""
        probe = TradeProbe(enabled=True)
        probe.record("build_topic", 750)
        result = probe._percentile_us("build_topic", 0.0)
        assert result == BUCKET_BOUNDARIES_NS[0] // 1_000

    def test_threshold_uses_ceil_so_small_count_high_pct_is_strict(self) -> None:
        """``ceil(2 * 0.99) == 2`` so p99 of two observations needs both."""
        probe = TradeProbe(enabled=True)
        probe.record("build_topic", 500)
        probe.record("build_topic", 25_000_000)
        p99 = probe._percentile_us("build_topic", 0.99)
        threshold = max(1, math.ceil(2 * 0.99))
        assert threshold == 2
        assert p99 == BUCKET_BOUNDARIES_NS[13] // 1_000


class TestGetProbe:
    """Module-level singleton lookup."""

    def test_returns_module_singleton(self) -> None:
        """``get_probe`` returns the same instance on repeated calls."""
        first = get_probe()
        second = get_probe()
        assert first is second

    def test_singleton_enabled_state_matches_env_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Reloading the module under a flipped env var rebuilds the singleton.

        Exercised by clearing the import-time module cache and asserting
        the new singleton picks up the flipped value, then restoring
        the original module so the rest of the test suite keeps using
        the shared instance.
        """
        monkeypatch.setenv(TRADE_PROBE_ENV_VAR, "1")

        reloaded = importlib.reload(trade_probe)
        try:
            assert reloaded.get_probe().enabled is True
        finally:
            monkeypatch.delenv(TRADE_PROBE_ENV_VAR, raising=False)
            importlib.reload(trade_probe)
