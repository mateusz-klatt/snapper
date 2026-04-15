"""Unit tests for ZmqReplayEngine — fast-fail + cleanup invariants."""

import asyncio
from datetime import UTC
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.application.backtest.config import BacktestConfig
from snapper.application.backtest.result_collector import ResultCollector
from snapper.application.backtest.zmq_engine import ZmqReplayEngine

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def _make_config_with_instruments(instruments: dict[str, list[str]]) -> BacktestConfig:
    """Build a BacktestConfig MagicMock with caller-supplied instruments."""
    config = MagicMock(spec=BacktestConfig)
    config.instruments = instruments
    config.timeframe = "1h"
    config.start_date = NOW
    config.end_date = NOW
    config.initial_balance = 10000.0
    config.slippage_bps = 0.0
    config.commission_bps = 0.0
    config.strategy_params = {}
    config.strategy_class = "macd"
    return config


@pytest.mark.asyncio
class TestZmqReplayEngineFastFail:
    """Empty-instrument config rejected before any broker is allocated."""

    @pytest.mark.timeout(10)
    async def test_run_raises_for_empty_instruments_dict(self) -> None:
        """Empty dict → ValueError, no broker consumed."""
        engine = ZmqReplayEngine(AsyncMock(), NOW)
        config = _make_config_with_instruments({})
        with pytest.raises(ValueError, match="empty"):
            await engine.run("run-1", config, ResultCollector())

    @pytest.mark.timeout(10)
    async def test_run_raises_for_empty_instrument_lists(self) -> None:
        """Dict with empty value lists also fast-fails."""
        engine = ZmqReplayEngine(AsyncMock(), NOW)
        config = _make_config_with_instruments({"kraken": []})
        with pytest.raises(ValueError, match="empty"):
            await engine.run("run-1", config, ResultCollector())


@pytest.mark.asyncio
class TestZmqReplayEngineLifecycle:
    """Focused branch tests for task completion and bounded cleanup."""

    @pytest.mark.timeout(10)
    async def test_run_reraises_primary_task_exception(self) -> None:
        """The first surfaced task exception is re-raised after completion logging."""
        engine = ZmqReplayEngine(AsyncMock(), NOW)
        config = _make_config_with_instruments({"kraken": ["BTC-USD"]})
        collector = ResultCollector()

        broker = AsyncMock()
        broker.wait_for_subscription = AsyncMock()
        endpoints = MagicMock(xsub="tcp://xsub", xpub="tcp://xpub")

        strategy = MagicMock()
        strategy.start = AsyncMock()
        strategy.stop = AsyncMock()

        async def fail_primary() -> None:
            raise RuntimeError("listen exploded")

        async def fail_secondary() -> None:
            raise RuntimeError("publisher exploded")

        listen_task = asyncio.create_task(fail_primary())
        strategy._listen_task = listen_task

        publisher = MagicMock()
        publisher.start = fail_secondary

        async def fake_wait(
            tasks: set[asyncio.Task[Any]],
            return_when: object,
        ) -> tuple[list[asyncio.Task[Any]], set[asyncio.Task[Any]]]:
            del return_when
            await asyncio.sleep(0)
            publisher_task = next(task for task in tasks if task is not listen_task)
            return [listen_task, publisher_task], set()

        with (
            patch(
                "snapper.application.backtest.zmq_engine.allocate_replay_endpoints",
                AsyncMock(return_value=(broker, endpoints)),
            ),
            patch.dict(
                "snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES",
                {"macd": MagicMock()},
            ),
            patch(
                "snapper.application.backtest.zmq_engine.make_backtest_replay_strategy",
                return_value=strategy,
            ),
            patch(
                "snapper.application.backtest.zmq_engine.ReplayPublisher",
                return_value=publisher,
            ),
            patch("snapper.application.backtest.zmq_engine.asyncio.wait", side_effect=fake_wait),
            patch.object(ZmqReplayEngine, "_cleanup", AsyncMock()) as mock_cleanup,
            pytest.raises(RuntimeError, match="listen exploded"),
        ):
            await engine.run("run-1", config, collector)

        mock_cleanup.assert_awaited_once()

    @pytest.mark.timeout(10)
    async def test_run_raises_cancelled_when_completed_task_was_cancelled(self) -> None:
        """A cancelled completed task maps to asyncio.CancelledError."""
        engine = ZmqReplayEngine(AsyncMock(), NOW)
        config = _make_config_with_instruments({"kraken": ["BTC-USD"]})

        broker = AsyncMock()
        broker.wait_for_subscription = AsyncMock()
        endpoints = MagicMock(xsub="tcp://xsub", xpub="tcp://xpub")

        strategy = MagicMock()
        strategy.start = AsyncMock()
        strategy.stop = AsyncMock()

        async def cancelled_listen() -> None:
            raise asyncio.CancelledError()

        async def publisher_done() -> None:
            await asyncio.sleep(0)

        listen_task = asyncio.create_task(cancelled_listen())
        strategy._listen_task = listen_task

        publisher = MagicMock()
        publisher.start = publisher_done

        async def fake_wait(
            tasks: set[asyncio.Task[Any]],
            return_when: object,
        ) -> tuple[list[asyncio.Task[Any]], set[asyncio.Task[Any]]]:
            del return_when
            await asyncio.sleep(0)
            publisher_task = next(task for task in tasks if task is not listen_task)
            await publisher_task
            return [listen_task], {publisher_task}

        with (
            patch(
                "snapper.application.backtest.zmq_engine.allocate_replay_endpoints",
                AsyncMock(return_value=(broker, endpoints)),
            ),
            patch.dict(
                "snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES",
                {"macd": MagicMock()},
            ),
            patch(
                "snapper.application.backtest.zmq_engine.make_backtest_replay_strategy",
                return_value=strategy,
            ),
            patch(
                "snapper.application.backtest.zmq_engine.ReplayPublisher",
                return_value=publisher,
            ),
            patch("snapper.application.backtest.zmq_engine.asyncio.wait", side_effect=fake_wait),
            patch.object(ZmqReplayEngine, "_cleanup", AsyncMock()),
            pytest.raises(asyncio.CancelledError),
        ):
            await engine.run("run-1", config, ResultCollector())

    @pytest.mark.timeout(10)
    async def test_cleanup_logs_pending_task_and_stop_timeouts(self) -> None:
        """Cleanup handles leaks plus stop timeouts without re-raising."""
        pending_task = MagicMock(spec=asyncio.Task)
        pending_task.done.return_value = False
        pending_task.get_name.return_value = "listen-pending"
        pending_task.cancelled.return_value = False

        done_task = MagicMock(spec=asyncio.Task)
        done_task.done.return_value = True
        done_task.cancelled.return_value = False
        done_task.exception.return_value = RuntimeError("already surfaced")
        done_task.get_name.return_value = "heartbeat-done"

        strategy = MagicMock()
        strategy._listen_task = pending_task
        strategy._heartbeat_task = done_task
        strategy.stop = AsyncMock(side_effect=TimeoutError())

        broker = MagicMock()
        broker.stop = AsyncMock(side_effect=TimeoutError())

        with patch(
            "snapper.application.backtest.zmq_engine.asyncio.wait",
            AsyncMock(return_value=({done_task}, {pending_task})),
        ):
            await ZmqReplayEngine._cleanup(None, strategy, broker)

        pending_task.cancel.assert_called_once()

    @pytest.mark.timeout(10)
    async def test_cleanup_logs_stop_exceptions(self) -> None:
        """Cleanup suppresses strategy.stop and broker.stop exceptions."""
        strategy = MagicMock()
        strategy._listen_task = None
        strategy._heartbeat_task = None
        strategy.stop = AsyncMock(side_effect=RuntimeError("strategy stop failed"))

        broker = MagicMock()
        broker.stop = AsyncMock(side_effect=RuntimeError("broker stop failed"))

        await ZmqReplayEngine._cleanup(None, strategy, broker)

    async def test_log_completion_handles_missing_publisher(self) -> None:
        """Completion logging tolerates a missing publisher task."""
        await asyncio.sleep(0)
        listen_task = MagicMock(spec=asyncio.Task)
        listen_task.done.return_value = False
        listen_task.cancelled.return_value = False

        drain = MagicMock()
        drain.published_count = 0
        drain.processed_count = 0

        ZmqReplayEngine._log_completion(None, listen_task, drain)

    @pytest.mark.timeout(10)
    async def test_cleanup_handles_absent_strategy_and_broker(self) -> None:
        """Cleanup is a no-op when neither strategy nor broker was allocated."""
        await ZmqReplayEngine._cleanup(None, None, None)
