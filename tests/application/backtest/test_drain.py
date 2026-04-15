"""Tests for DrainCoordinator + readiness/drain timeout exceptions."""

import asyncio

import pytest

from snapper.application.backtest.drain import BacktestDrainTimeoutError
from snapper.application.backtest.drain import BacktestReadinessTimeoutError
from snapper.application.backtest.drain import DrainCoordinator


class TestDrainCoordinator:
    """End-of-stream parity coordination between publisher and strategy."""

    def test_initial_state(self) -> None:
        """Fresh coordinator has zero counters and an unset event."""
        d = DrainCoordinator()
        assert d.published_count == 0
        assert d.processed_count == 0
        assert not d.publishing_done
        assert not d.drained.is_set()

    def test_on_publish_increments(self) -> None:
        """on_publish bumps published_count without firing the event."""
        d = DrainCoordinator()
        d.on_publish()
        d.on_publish()
        assert d.published_count == 2
        assert not d.drained.is_set()

    def test_on_processed_does_not_fire_until_publishing_done(self) -> None:
        """Processed catching up to published is not enough — publishing must end."""
        d = DrainCoordinator()
        d.on_publish()
        d.on_processed()
        assert d.processed_count == 1
        assert not d.drained.is_set()

    def test_mark_done_fires_when_already_caught_up(self) -> None:
        """mark_done_publishing fires drained immediately if processed >= published."""
        d = DrainCoordinator()
        d.on_publish()
        d.on_processed()
        d.mark_done_publishing()
        assert d.drained.is_set()

    def test_on_processed_fires_when_publishing_done(self) -> None:
        """Late processed call fires drained once parity reached after mark_done."""
        d = DrainCoordinator()
        d.on_publish()
        d.on_publish()
        d.mark_done_publishing()
        assert not d.drained.is_set()
        d.on_processed()
        assert not d.drained.is_set()
        d.on_processed()
        assert d.drained.is_set()

    def test_overshoot_processed_still_fires(self) -> None:
        """Processed > published is treated as drained (defence in depth)."""
        d = DrainCoordinator()
        d.on_publish()
        d.mark_done_publishing()
        d.on_processed()
        d.on_processed()
        assert d.drained.is_set()

    @pytest.mark.asyncio
    @pytest.mark.timeout(5)
    async def test_event_can_be_awaited(self) -> None:
        """The drained event integrates with asyncio.wait_for."""
        d = DrainCoordinator()
        d.on_publish()

        async def producer() -> None:
            await asyncio.sleep(0.05)
            d.on_processed()
            d.mark_done_publishing()

        await asyncio.gather(producer(), asyncio.wait_for(d.drained.wait(), timeout=2.0))


class TestBacktestExceptions:
    """Replay-engine timeout exception types are distinct and informative."""

    def test_readiness_timeout_is_runtime_error(self) -> None:
        """BacktestReadinessTimeoutError inherits from RuntimeError."""
        exc = BacktestReadinessTimeoutError("warmup not acked across topics=['m.btc']")
        assert isinstance(exc, RuntimeError)
        assert "warmup not acked" in str(exc)

    def test_drain_timeout_is_runtime_error(self) -> None:
        """BacktestDrainTimeoutError inherits from RuntimeError."""
        exc = BacktestDrainTimeoutError("drain timeout: published=10 processed=7")
        assert isinstance(exc, RuntimeError)
        assert "published=10" in str(exc)

    def test_distinct_types(self) -> None:
        """Readiness and drain timeouts are different exception classes."""
        assert BacktestReadinessTimeoutError is not BacktestDrainTimeoutError
