"""Tests for strategy read-only REST API routes."""

from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from fastapi import Request

from snapper.application.process_manager.models import ProcessConfigModel
from snapper.core.types import ProcessRoleEnum
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.strategy_routes import list_strategies
from snapper.strategies.factory import StrategyFactory
from snapper.strategies.rsi import RSIReversion


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
        mock_factory.autostart_includes = MagicMock(return_value=True)
        mock_factory.coordinator_topic_slug = MagicMock(return_value="coord-0")
        mock_request = MagicMock(spec=Request)
        mock_request.app.state.process_factory = mock_factory
        mock_request.app.state.rest_tracker = SequenceTracker()
        result = await list_strategies(request=mock_request, _user=MagicMock())
        assert result.count == 0
        assert result.payload == []

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
                    parameters={},
                    role=ProcessRoleEnum.STRATEGY,
                ),
                ProcessConfigModel(
                    name="zmq_broker",
                    enabled=True,
                    mode="thread",
                    class_path="snapper.ipc.zmq_broker.ZmqBrokerThread",
                    method="run",
                    parameters={},
                    role=ProcessRoleEnum.CORE,
                ),
                ProcessConfigModel(
                    name="executor_kraken",
                    enabled=True,
                    mode="thread",
                    class_path="snapper.executors.Kraken",
                    method="run",
                    parameters={},
                    role=ProcessRoleEnum.CORE,
                ),
            ]
        )
        mock_factory.started_processes = {"strategy_macd": MagicMock()}
        mock_factory.autostart_includes = MagicMock(return_value=True)
        mock_factory.coordinator_topic_slug = MagicMock(return_value="coord-0")
        mock_request = MagicMock(spec=Request)
        mock_request.app.state.process_factory = mock_factory
        mock_request.app.state.rest_tracker = SequenceTracker()
        result = await list_strategies(request=mock_request, _user=MagicMock())
        assert result.count == 1
        assert len(result.payload) == 1
        strategy = result.payload[0]
        assert strategy.name == "strategy_macd"
        assert strategy.running is True
        assert strategy.enabled is True
        assert strategy.mode == "thread"

    @pytest.mark.asyncio
    async def test_remote_strategy_unions_cache_ownership(self) -> None:
        """A strategy owned by the strategies container unions the cache.

        Given: A strategy the profile does not select and a fresh coord-2
            snapshot reporting it running,
        When: list_strategies is called,
        Then: running=True, coordinator="coord-2", managed_remotely=True.
        """
        mock_factory = MagicMock()
        mock_factory.get_process_configs = AsyncMock(
            return_value=[
                ProcessConfigModel(
                    name="strategy_heartbeat_consult_btc_1h",
                    enabled=True,
                    mode="thread",
                    class_path="snapper.strategies.process_wrapper.X",
                    method="start",
                    parameters={},
                    role=ProcessRoleEnum.STRATEGY,
                )
            ]
        )
        mock_factory.started_processes = {}
        mock_factory.autostart_includes = MagicMock(return_value=False)
        mock_factory.coordinator_topic_slug = MagicMock(return_value="coord-0")
        cache = MagicMock()
        cache.lookup = MagicMock(return_value=(True, "coord-2"))
        mock_request = MagicMock(spec=Request)
        mock_request.app.state.process_factory = mock_factory
        mock_request.app.state.rest_tracker = SequenceTracker()
        mock_request.app.state.remote_summary_cache = cache
        result = await list_strategies(request=mock_request, _user=MagicMock())
        row = result.payload[0]
        assert row.running is True
        assert row.coordinator == "coord-2"
        assert row.managed_remotely is True

    @pytest.mark.asyncio
    async def test_remote_strategy_without_snapshot_is_stopped_remote(self) -> None:
        """An excluded strategy with no snapshot shows stopped + remote.

        Given: autostart exclusion and no cached remote summary,
        When: list_strategies is called,
        Then: running=False and managed_remotely=True (Start hidden).
        """
        mock_factory = MagicMock()
        mock_factory.get_process_configs = AsyncMock(
            return_value=[
                ProcessConfigModel(
                    name="strategy_heartbeat_consult_btc_1h",
                    enabled=True,
                    mode="thread",
                    class_path="snapper.strategies.process_wrapper.X",
                    method="start",
                    parameters={},
                    role=ProcessRoleEnum.STRATEGY,
                )
            ]
        )
        mock_factory.started_processes = {}
        mock_factory.autostart_includes = MagicMock(return_value=False)
        mock_factory.coordinator_topic_slug = MagicMock(return_value="coord-0")
        mock_request = MagicMock(spec=Request)
        mock_request.app.state.process_factory = mock_factory
        mock_request.app.state.rest_tracker = SequenceTracker()
        del mock_request.app.state.remote_summary_cache
        result = await list_strategies(request=mock_request, _user=MagicMock())
        row = result.payload[0]
        assert row.running is False
        assert row.managed_remotely is True

    @pytest.mark.asyncio
    async def test_local_duplicate_strategy_stays_controllable(self) -> None:
        """A locally-running duplicate keeps local ownership (footgun rule).

        Given: The profile excludes the strategy but it IS running locally,
        When: list_strategies is called,
        Then: managed_remotely=False so the UI keeps Stop enabled.
        """
        mock_factory = MagicMock()
        mock_factory.get_process_configs = AsyncMock(
            return_value=[
                ProcessConfigModel(
                    name="strategy_heartbeat_consult_btc_1h",
                    enabled=True,
                    mode="thread",
                    class_path="snapper.strategies.process_wrapper.X",
                    method="start",
                    parameters={},
                    role=ProcessRoleEnum.STRATEGY,
                )
            ]
        )
        mock_factory.started_processes = {"strategy_heartbeat_consult_btc_1h": MagicMock()}
        mock_factory.autostart_includes = MagicMock(return_value=False)
        mock_factory.coordinator_topic_slug = MagicMock(return_value="coord-0")
        mock_request = MagicMock(spec=Request)
        mock_request.app.state.process_factory = mock_factory
        mock_request.app.state.rest_tracker = SequenceTracker()
        result = await list_strategies(request=mock_request, _user=MagicMock())
        row = result.payload[0]
        assert row.running is True
        assert row.managed_remotely is False
        assert row.coordinator == "coord-0"

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
                    parameters={},
                    role=ProcessRoleEnum.STRATEGY,
                ),
                ProcessConfigModel(
                    name="strategy_stopped",
                    enabled=False,
                    mode="thread",
                    class_path="snapper.strategies.Stopped",
                    method="run",
                    parameters={},
                    role=ProcessRoleEnum.STRATEGY,
                ),
            ]
        )
        mock_factory.started_processes = {"strategy_running": MagicMock()}
        mock_factory.autostart_includes = MagicMock(return_value=True)
        mock_factory.coordinator_topic_slug = MagicMock(return_value="coord-0")
        mock_request = MagicMock(spec=Request)
        mock_request.app.state.process_factory = mock_factory
        mock_request.app.state.rest_tracker = SequenceTracker()
        result = await list_strategies(request=mock_request, _user=MagicMock())
        assert result.count == 2
        running = next(s for s in result.payload if s.name == "strategy_running")
        stopped = next(s for s in result.payload if s.name == "strategy_stopped")
        assert running.running is True
        assert running.enabled is True
        assert running.mode == "process"
        assert stopped.running is False
        assert stopped.enabled is False
        assert stopped.mode == "thread"

    @pytest.mark.asyncio
    async def test_strategy_class_resolved_from_tags(self) -> None:
        """Test strategy_class is recovered from the process tags.

        Given: strategy processes whose tags carry the lower-cased registry key,
        When: list_strategies is called,
        Then: the exact StrategyFactory key is recovered (case-insensitively),
            and a tag with no registry match yields None.
        """
        saved = dict(StrategyFactory.STRATEGY_CLASSES)
        try:
            StrategyFactory.STRATEGY_CLASSES.clear()
            StrategyFactory.STRATEGY_CLASSES["MACDCrossover"] = RSIReversion
            mock_factory = MagicMock()
            mock_factory.get_process_configs = AsyncMock(
                return_value=[
                    ProcessConfigModel(
                        name="strategy_macd",
                        enabled=True,
                        mode="thread",
                        class_path="snapper.strategies.macd.MACDCrossover",
                        method="run",
                        parameters={},
                        role=ProcessRoleEnum.STRATEGY,
                        tags=("strategy", "MacdCrossover"),
                    ),
                    ProcessConfigModel(
                        name="strategy_unknown",
                        enabled=False,
                        mode="thread",
                        class_path="snapper.strategies.unknown.Unknown",
                        method="run",
                        parameters={},
                        role=ProcessRoleEnum.STRATEGY,
                        tags=("strategy", "notregistered"),
                    ),
                ]
            )
            mock_factory.started_processes = {}
            mock_factory.autostart_includes = MagicMock(return_value=True)
            mock_factory.coordinator_topic_slug = MagicMock(return_value="coord-0")
            mock_request = MagicMock(spec=Request)
            mock_request.app.state.process_factory = mock_factory
            mock_request.app.state.rest_tracker = SequenceTracker()
            result = await list_strategies(request=mock_request, _user=MagicMock())
        finally:
            StrategyFactory.STRATEGY_CLASSES.clear()
            StrategyFactory.STRATEGY_CLASSES.update(saved)
        macd = next(s for s in result.payload if s.name == "strategy_macd")
        unknown = next(s for s in result.payload if s.name == "strategy_unknown")
        assert macd.strategy_class == "MACDCrossover"
        assert unknown.strategy_class is None
