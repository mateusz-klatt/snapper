"""Tests for strategy read-only REST API routes."""

from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from fastapi import Request

from snapper.application.process_manager.enums import ProcessRoleEnum
from snapper.application.process_manager.models import ProcessConfigModel
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.strategy_routes import list_strategies


class TestListStrategies:
    """Tests for listing strategy processes with lightweight status."""

    @pytest.mark.asyncio
    async def test_empty_processes(self) -> None:
        """Test listing strategies with no configured processes.

        Given: A factory with no processes configured,
        When: list_strategies is called,
        Then: Empty list with zero count is returned.
        """
        mock_factory = MagicMock()
        mock_factory.get_process_configs = AsyncMock(return_value=[])
        mock_factory.started_processes = {}
        mock_request = MagicMock(spec=Request)
        mock_request.app.state.process_factory = mock_factory
        mock_request.app.state.rest_tracker = SequenceTracker()
        result = await list_strategies(request=mock_request, _user=MagicMock())
        assert result.count == 0
        assert result.strategies == []

    @pytest.mark.asyncio
    async def test_returns_only_strategy_role_processes(self) -> None:
        """Test filtering to strategy-role processes only.

        Given: A mix of strategy and non-strategy processes,
        When: list_strategies is called,
        Then: Only strategy-role processes are returned.
        """
        mock_factory = MagicMock()
        mock_factory.get_process_configs = AsyncMock(
            return_value=[
                ProcessConfigModel(
                    name="strategy_macd",
                    enabled=True,
                    mode="thread",
                    class_path="snapper.strategies.MACD",
                    method="run",
                    args=[],
                    kwargs={},
                    role=ProcessRoleEnum.STRATEGY,
                ),
                ProcessConfigModel(
                    name="zmq_broker",
                    enabled=True,
                    mode="thread",
                    class_path="snapper.ipc.zmq_broker.ZmqBrokerThread",
                    method="run",
                    args=[],
                    kwargs={},
                    role=ProcessRoleEnum.CORE,
                ),
                ProcessConfigModel(
                    name="executor_kraken",
                    enabled=True,
                    mode="thread",
                    class_path="snapper.executors.Kraken",
                    method="run",
                    args=[],
                    kwargs={},
                    role=ProcessRoleEnum.CORE,
                ),
            ]
        )
        mock_factory.started_processes = {"strategy_macd": MagicMock()}
        mock_request = MagicMock(spec=Request)
        mock_request.app.state.process_factory = mock_factory
        mock_request.app.state.rest_tracker = SequenceTracker()
        result = await list_strategies(request=mock_request, _user=MagicMock())
        assert result.count == 1
        assert len(result.strategies) == 1
        strategy = result.strategies[0]
        assert strategy.name == "strategy_macd"
        assert strategy.running is True
        assert strategy.enabled is True
        assert strategy.mode == "thread"

    @pytest.mark.asyncio
    async def test_running_status_correctly_reported(self) -> None:
        """Test running vs stopped strategies have correct status.

        Given: Multiple strategies with different running states,
        When: list_strategies is called,
        Then: Running status accurately reflects started_processes.
        """
        mock_factory = MagicMock()
        mock_factory.get_process_configs = AsyncMock(
            return_value=[
                ProcessConfigModel(
                    name="strategy_running",
                    enabled=True,
                    mode="process",
                    class_path="snapper.strategies.Running",
                    method="run",
                    args=[],
                    kwargs={},
                    role=ProcessRoleEnum.STRATEGY,
                ),
                ProcessConfigModel(
                    name="strategy_stopped",
                    enabled=False,
                    mode="thread",
                    class_path="snapper.strategies.Stopped",
                    method="run",
                    args=[],
                    kwargs={},
                    role=ProcessRoleEnum.STRATEGY,
                ),
            ]
        )
        mock_factory.started_processes = {"strategy_running": MagicMock()}
        mock_request = MagicMock(spec=Request)
        mock_request.app.state.process_factory = mock_factory
        mock_request.app.state.rest_tracker = SequenceTracker()
        result = await list_strategies(request=mock_request, _user=MagicMock())
        assert result.count == 2
        running = next(s for s in result.strategies if s.name == "strategy_running")
        stopped = next(s for s in result.strategies if s.name == "strategy_stopped")
        assert running.running is True
        assert running.enabled is True
        assert running.mode == "process"
        assert stopped.running is False
        assert stopped.enabled is False
        assert stopped.mode == "thread"
