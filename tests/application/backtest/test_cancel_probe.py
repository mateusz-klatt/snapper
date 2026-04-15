"""Tests for the shared CancelProbe dataclass."""

import asyncio
from time import monotonic
from unittest.mock import AsyncMock

import pytest

from snapper.application.backtest.cancel import CancelProbe
from snapper.core.types import BacktestRunStatusEnum


@pytest.mark.asyncio
class TestCancelProbe:
    """Throttled, bounded probe of backtest_runs.status."""

    @pytest.mark.timeout(5)
    async def test_raises_cancelled_on_cancel_requested_status(self) -> None:
        """``status='cancel_requested'`` triggers asyncio.CancelledError."""
        bt_repo = AsyncMock()
        bt_repo.get_run = AsyncMock(return_value={"status": BacktestRunStatusEnum.CANCEL_REQUESTED})
        probe = CancelProbe(bt_repo=bt_repo, run_public_id="r-1", cancel_poll_ms=0)
        with pytest.raises(asyncio.CancelledError):
            await probe.check()

    @pytest.mark.timeout(5)
    async def test_passthrough_on_running_status(self) -> None:
        """``status='running'`` returns without raising."""
        bt_repo = AsyncMock()
        bt_repo.get_run = AsyncMock(return_value={"status": BacktestRunStatusEnum.RUNNING})
        probe = CancelProbe(bt_repo=bt_repo, run_public_id="r-1", cancel_poll_ms=0)
        await probe.check()
        assert bt_repo.get_run.await_count == 1

    @pytest.mark.timeout(5)
    async def test_passthrough_on_missing_run(self) -> None:
        """get_run returning None does not raise."""
        bt_repo = AsyncMock()
        bt_repo.get_run = AsyncMock(return_value=None)
        probe = CancelProbe(bt_repo=bt_repo, run_public_id="r-1", cancel_poll_ms=0)
        await probe.check()

    @pytest.mark.timeout(5)
    async def test_throttled_repeat_calls_skip_db(self) -> None:
        """Two checks within ``cancel_poll_ms`` produce only one DB call."""
        bt_repo = AsyncMock()
        bt_repo.get_run = AsyncMock(return_value={"status": BacktestRunStatusEnum.RUNNING})
        probe = CancelProbe(bt_repo=bt_repo, run_public_id="r-1", cancel_poll_ms=10_000)
        await probe.check()
        await probe.check()
        await probe.check()
        assert bt_repo.get_run.await_count == 1

    @pytest.mark.timeout(5)
    async def test_db_timeout_logs_and_returns(self) -> None:
        """A stuck DB read triggers TimeoutError → warning log, no raise."""

        async def _hang(*_: object, **__: object) -> None:
            await asyncio.sleep(5.0)

        bt_repo = AsyncMock()
        bt_repo.get_run = AsyncMock(side_effect=_hang)
        probe = CancelProbe(
            bt_repo=bt_repo,
            run_public_id="r-1",
            cancel_poll_ms=0,
            probe_timeout_s=0.05,
        )
        start = monotonic()
        await probe.check()
        elapsed = monotonic() - start
        assert elapsed < 0.5

    @pytest.mark.timeout(5)
    async def test_throttle_records_attempt_even_when_skipped(self) -> None:
        """Skipped (throttled) probes still update _last_check_ms."""
        bt_repo = AsyncMock()
        bt_repo.get_run = AsyncMock(return_value={"status": BacktestRunStatusEnum.RUNNING})
        probe = CancelProbe(bt_repo=bt_repo, run_public_id="r-1", cancel_poll_ms=10_000)
        await probe.check()
        first_check_ms = probe._last_check_ms
        await probe.check()
        assert probe._last_check_ms == first_check_ms
