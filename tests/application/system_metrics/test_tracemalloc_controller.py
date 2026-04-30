"""Tests for the tracemalloc system metrics controller."""

import asyncio
import tracemalloc
from collections.abc import Generator

import pytest

from snapper.application.system_metrics.tracemalloc_controller import DEFAULT_DURATION_SECONDS
from snapper.application.system_metrics.tracemalloc_controller import MAX_DURATION_SECONDS
from snapper.application.system_metrics.tracemalloc_controller import TracemallocController
from snapper.application.system_metrics.tracemalloc_controller import _clamp_duration


class TestTracemallocController:
    """Tests for TracemallocController."""

    @pytest.fixture(autouse=True)
    def _reset_tracemalloc_state(self) -> Generator[None]:
        if tracemalloc.is_tracing():
            tracemalloc.stop()
        yield
        if tracemalloc.is_tracing():
            tracemalloc.stop()

    @pytest.mark.parametrize(
        ("duration_s", "expected"),
        [
            (0.0, DEFAULT_DURATION_SECONDS),
            (-1.0, DEFAULT_DURATION_SECONDS),
            (1.5, 1.5),
            (MAX_DURATION_SECONDS + 1.0, MAX_DURATION_SECONDS),
        ],
    )
    def test_clamp_duration(self, duration_s: float, expected: float) -> None:
        """Covered by test body."""
        assert _clamp_duration(duration_s) == pytest.approx(expected)

    def test_is_active_and_traced_bytes_when_inactive(self) -> None:
        """Covered by test body."""
        controller = TracemallocController()

        assert controller.is_active() is False
        assert controller.traced_bytes() is None

    async def test_traced_bytes_returns_int_when_active(self) -> None:
        """Covered by test body."""
        controller = TracemallocController()

        await controller.start(1.0)
        payload = [b"x" * 128]
        traced = controller.traced_bytes()
        await controller.stop()

        assert payload
        assert isinstance(traced, int)
        assert traced >= 0

    async def test_start_arms_and_stop_disarms(self) -> None:
        """Covered by test body."""
        controller = TracemallocController()

        await controller.start(1.0)
        task = controller._auto_stop_task
        await controller.stop()

        assert task is not None
        assert task.cancelled()
        assert controller._auto_stop_task is None
        assert controller.is_active() is False

    async def test_auto_stop_fires_after_duration(self) -> None:
        """Covered by test body."""
        controller = TracemallocController()

        await controller.start(0.1)
        await asyncio.sleep(0.2)

        assert controller.is_active() is False
        assert controller._auto_stop_task is None

    async def test_double_start_replaces_deadline(self) -> None:
        """Covered by test body."""
        controller = TracemallocController()

        await controller.start(1.0)
        first_task = controller._auto_stop_task
        await controller.start(0.1)
        second_task = controller._auto_stop_task
        await asyncio.sleep(0.2)

        assert first_task is not None
        assert first_task.cancelled()
        assert second_task is not None
        assert second_task is not first_task
        assert controller.is_active() is False

    async def test_stop_cancels_pending_auto_stop(self) -> None:
        """Covered by test body."""
        controller = TracemallocController()

        await controller.start(1.0)
        task = controller._auto_stop_task
        await controller.stop()

        assert task is not None
        assert task.cancelled()
        assert controller._auto_stop_task is None
        assert controller.is_active() is False

    async def test_stop_on_never_started_controller_is_noop(self) -> None:
        """Covered by test body."""
        controller = TracemallocController()

        await controller.stop()

        assert controller.is_active() is False
        assert controller._auto_stop_task is None

    async def test_start_when_tracemalloc_already_tracing_externally(self) -> None:
        """Covered by test body."""
        tracemalloc.start()
        controller = TracemallocController()

        await controller.start(1.0)
        assert controller.is_active() is True
        await controller.stop()

        assert tracemalloc.is_tracing() is False

    async def test_duration_is_clamped_to_max_via_start(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Covered by test body."""
        controller = TracemallocController()
        durations: list[float] = []

        async def fake_auto_stop_after(duration_s: float) -> None:
            durations.append(duration_s)

        monkeypatch.setattr(controller, "_auto_stop_after", fake_auto_stop_after)

        await controller.start(MAX_DURATION_SECONDS + 100.0)
        task = controller._auto_stop_task
        assert task is not None
        await task
        await controller.stop()

        assert durations == [pytest.approx(MAX_DURATION_SECONDS)]
        assert controller._auto_stop_task is None
        assert controller.is_active() is False

    async def test_auto_stop_skips_stop_when_tracing_was_stopped_externally(self) -> None:
        """Covered by test body."""
        controller = TracemallocController()

        await controller.start(0.1)
        tracemalloc.stop()
        await asyncio.sleep(0.2)

        assert controller.is_active() is False
        assert controller._auto_stop_task is None

    async def test_cancel_pending_auto_stop_discards_completed_task(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Covered by test body."""
        controller = TracemallocController()

        async def fake_auto_stop_after(duration_s: float) -> None:
            assert duration_s == pytest.approx(1.0)

        monkeypatch.setattr(controller, "_auto_stop_after", fake_auto_stop_after)

        await controller.start(1.0)
        task = controller._auto_stop_task
        assert task is not None
        await task
        await controller.stop()

        assert controller._auto_stop_task is None
        assert controller.is_active() is False
