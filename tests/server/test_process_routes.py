"""Tests for process management REST API routes."""

import json as _json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch
from unittest.mock import patch as _patch

import pytest
from fastapi import HTTPException
from fastapi import Request

from snapper.api.schemas.process import ProcessCategoryCount
from snapper.api.schemas.process import ProcessCreateBody
from snapper.api.schemas.process import ProcessCreateRequest
from snapper.api.schemas.process import ProcessStartBody
from snapper.api.schemas.process import ProcessStartRequest
from snapper.api.schemas.process import ProcessSummaryData
from snapper.application.process_manager.models import ProcessConfigModel
from snapper.application.process_manager.models import ProcessRegistryEntry
from snapper.application.process_manager.models import ProcessStartResult
from snapper.application.process_manager.models import ProcessStopResult
from snapper.application.process_manager.strategy_scope import StrategyOutputCoverageError
from snapper.application.process_manager.strategy_scope import StrategyProcessClassification
from snapper.application.process_manager.strategy_scope import StrategyWalletScope
from snapper.core.types import ProcessLifecycleEnum
from snapper.core.types import ProcessRoleEnum
from snapper.data.models import Setting
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import WalletRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.data import ProcessSummaryItem
from snapper.server.process_routes import _enforce_strategy_outputs_covered
from snapper.server.process_routes import _enforce_strategy_scope
from snapper.server.process_routes import _enforce_wallet_grant_exists
from snapper.server.process_routes import _read_persisted_strategy_parameters
from snapper.server.process_routes import _resolve_role_for_class_path
from snapper.server.process_routes import _resolve_strategy_start_launch_parameters
from snapper.server.process_routes import create_process_configuration
from snapper.server.process_routes import get_process_factory
from snapper.server.process_routes import get_process_schema
from snapper.server.process_routes import get_process_summary
from snapper.server.process_routes import get_remote_summary_cache
from snapper.server.process_routes import get_repository_for_processes
from snapper.server.process_routes import list_available_processes
from snapper.server.process_routes import list_configured_processes
from snapper.server.process_routes import list_process_runs
from snapper.server.process_routes import start_process
from snapper.server.process_routes import stop_process


def _make_rest_request() -> MagicMock:
    """Create a mock FastAPI Request with rest_tracker."""
    mock_request = MagicMock()
    mock_request.app.state.rest_tracker = SequenceTracker()
    return mock_request


def _wallet_row(public_id: str, *, is_paper: bool = False) -> WalletRow:
    """Build a wallet row for process route tests."""
    return WalletRow(
        public_id=public_id,
        label=public_id,
        description=None,
        is_paper=is_paper,
        timestamp=datetime(2026, 1, 1, tzinfo=UTC),
        session_id="test-sid",
        sequence_id=1,
    )


def _strategy_params(**overrides: object) -> dict[str, object]:
    """Build validated strategy parameters for process route tests."""
    params: dict[str, object] = {
        "name": "strategy",
        "inputs": ["candles.BTC-USD"],
        "outputs": ["signals.BTC-USD"],
        "operator_public_id": "op-1",
        "wallet_public_id": "w-1",
    }
    params.update(overrides)
    return params


class _SettingResultDouble:
    """Scalar result double for process setting reads."""

    def __init__(self, setting: Setting | None) -> None:
        """Store one optional setting row."""
        self._setting = setting

    def scalar_one_or_none(self) -> Setting | None:
        """Return the configured setting row."""
        return self._setting


class _SettingSessionDouble:
    """Session double that stores at most one process setting row."""

    def __init__(self, setting: Setting | None = None) -> None:
        """Create a session double with an optional setting."""
        self._setting = setting

    async def execute(self, _statement: object) -> _SettingResultDouble:
        """Return the stored setting as a scalar result."""
        return _SettingResultDouble(self._setting)

    def add(self, setting: Setting) -> None:
        """Store a setting row."""
        self._setting = setting

    async def commit(self) -> None:
        """Commit is a no-op for the in-memory setting."""


def _process_setting(key: str, value: str) -> Setting:
    """Build a process setting row for persisted-config tests."""
    return Setting(
        key=key,
        value=value,
        category="process",
        is_encrypted=False,
        timestamp=datetime.now(UTC),
        session_id="t",
        sequence_id=1,
    )


def _settings_repo(setting: Setting | None = None) -> SQLAlchemyRepository:
    """Build a SQLAlchemyRepository-shaped persisted-setting double."""
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    session = _SettingSessionDouble(setting)

    @asynccontextmanager
    async def session_context() -> AsyncIterator[_SettingSessionDouble]:
        """Yield the in-memory setting session."""
        yield session

    repo.session = session_context
    return repo


class TestGetProcessFactory:
    """Tests for process factory retrieval from application state."""

    def test_get_process_factory_success(self) -> None:
        """Test factory retrieval from app state.

        Given: A request with process factory in app state,
        When: get_process_factory is called,
        Then: The factory instance is returned.
        """
        mock_factory = MagicMock()
        mock_request = MagicMock(spec=Request)
        mock_request.app.state.process_factory = mock_factory
        result = get_process_factory(mock_request)
        assert result is mock_factory

    def test_get_process_factory_not_initialized(self) -> None:
        """Test AttributeError when factory not initialized.

        Given: A request without process factory in state,
        When: get_process_factory is called,
        Then: AttributeError is raised.
        """
        mock_request = MagicMock(spec=Request)
        del mock_request.app.state.process_factory
        with pytest.raises(AttributeError):
            get_process_factory(mock_request)


class TestCrossCoordinatorOwnership:
    """Cross-coordinator summary cache dependency + union behaviour."""

    def test_get_remote_summary_cache_returns_attached(self) -> None:
        """The cache is returned when wired into app state."""
        mock_request = MagicMock(spec=Request)
        sentinel = MagicMock()
        mock_request.app.state.remote_summary_cache = sentinel
        assert get_remote_summary_cache(mock_request) is sentinel

    def test_get_remote_summary_cache_returns_none_when_absent(self) -> None:
        """``None`` is returned when the cache never started."""
        mock_request = MagicMock(spec=Request)
        del mock_request.app.state.remote_summary_cache
        assert get_remote_summary_cache(mock_request) is None

    @pytest.mark.asyncio
    async def test_configured_feed_publisher_tagged_running_via_cache(self) -> None:
        """A feed publisher the API does not own is unioned from the cache.

        The publisher is absent from this node's ``started_processes`` but
        a fresh remote snapshot reports it running, so the row is
        ``running=True``, ``managed_remotely=True`` and carries the feed
        container's coordinator slug.
        """
        mock_factory = MagicMock()
        mock_factory.get_process_configs = AsyncMock(
            return_value=[
                ProcessConfigModel(
                    name="kraken_feed_publisher",
                    enabled=True,
                    mode="process",
                    class_path="snapper.feeds.KrakenFeed",
                    method="run",
                    parameters={},
                )
            ]
        )
        mock_factory.started_processes = {}
        mock_factory.active_runs = {}
        mock_factory.instance_configs = {}
        mock_factory.autostart_includes = MagicMock(return_value=False)
        mock_factory.coordinator_topic_slug = MagicMock(return_value="coord-0")
        cache = MagicMock()
        cache.lookup = MagicMock(return_value=(True, "coord-1"))
        result = await list_configured_processes(
            request=_make_rest_request(), factory=mock_factory, cache=cache, _user=MagicMock()
        )
        row = result.payload[0]
        assert row.name == "kraken_feed_publisher"
        assert row.running is True
        assert row.managed_remotely is True
        assert row.coordinator == "coord-1"

    @pytest.mark.asyncio
    async def test_configured_external_broker_is_managed_remotely(self) -> None:
        """An externally-owned zmq_broker shows managed_remotely without a summary.

        With ``zmq_broker_embedded=False`` the autostart profile no longer
        selects the broker, and the dedicated broker container emits no
        summary snapshots — the row must still be ``managed_remotely=True``
        and not running locally so the UI hides Start/Stop (no duplicate
        local broker can be spawned from the dashboard).
        """
        mock_factory = MagicMock()
        mock_factory.get_process_configs = AsyncMock(
            return_value=[
                ProcessConfigModel(
                    name="zmq_broker",
                    enabled=True,
                    mode="thread",
                    class_path="snapper.messaging.infrastructure.broker.ZmqBrokerProcess",
                    method="start",
                    parameters={},
                    tags=("zmq", "broker", "infrastructure"),
                )
            ]
        )
        mock_factory.started_processes = {}
        mock_factory.active_runs = {}
        mock_factory.instance_configs = {}
        mock_factory.autostart_includes = MagicMock(return_value=False)
        mock_factory.coordinator_topic_slug = MagicMock(return_value="coord-0")
        cache = MagicMock()
        cache.lookup = MagicMock(return_value=(False, None))
        result = await list_configured_processes(
            request=_make_rest_request(), factory=mock_factory, cache=cache, _user=MagicMock()
        )
        row = result.payload[0]
        assert row.name == "zmq_broker"
        assert row.running is False
        assert row.managed_remotely is True

    @pytest.mark.asyncio
    async def test_configured_external_strategy_is_managed_remotely(self) -> None:
        """An externally-owned strategy shows managed_remotely with a summary.

        With ``strategies_embedded=False`` the profile no longer selects
        role-STRATEGY configs; the strategies container's coord summary
        unions in as running+managed_remotely so the dashboard hides
        Start/Stop (no local duplicate).
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
                    tags=("strategy", "HeartbeatConsult"),
                )
            ]
        )
        mock_factory.started_processes = {}
        mock_factory.active_runs = {}
        mock_factory.instance_configs = {}
        mock_factory.autostart_includes = MagicMock(return_value=False)
        mock_factory.coordinator_topic_slug = MagicMock(return_value="coord-0")
        cache = MagicMock()
        cache.lookup = MagicMock(return_value=(True, "coord-2"))
        result = await list_configured_processes(
            request=_make_rest_request(), factory=mock_factory, cache=cache, _user=MagicMock()
        )
        row = result.payload[0]
        assert row.running is True
        assert row.managed_remotely is True
        assert row.coordinator == "coord-2"

    @pytest.mark.asyncio
    async def test_configured_local_duplicate_stays_controllable(self) -> None:
        """A feed publisher running locally (the duplicate footgun) stays local.

        Even though the API profile does not select the publisher, a copy
        is actually running in this container; the row must be
        ``managed_remotely=False`` so the UI keeps Stop enabled and the
        operator can kill the rogue duplicate.
        """
        mock_factory = MagicMock()
        mock_factory.get_process_configs = AsyncMock(
            return_value=[
                ProcessConfigModel(
                    name="kraken_feed_publisher",
                    enabled=True,
                    mode="process",
                    class_path="snapper.feeds.KrakenFeed",
                    method="run",
                    parameters={},
                )
            ]
        )
        mock_factory.started_processes = {"kraken_feed_publisher": MagicMock()}
        mock_factory.active_runs = {}
        mock_factory.instance_configs = {}
        mock_factory.autostart_includes = MagicMock(return_value=False)
        mock_factory.coordinator_topic_slug = MagicMock(return_value="coord-0")
        cache = MagicMock()
        cache.lookup = MagicMock(return_value=(True, "coord-1"))
        result = await list_configured_processes(
            request=_make_rest_request(), factory=mock_factory, cache=cache, _user=MagicMock()
        )
        row = result.payload[0]
        assert row.running is True
        assert row.managed_remotely is False
        assert row.coordinator == "coord-0"
        cache.lookup.assert_not_called()

    @pytest.mark.asyncio
    async def test_configured_feed_publisher_remote_without_cache(self) -> None:
        """Without the cache a remote-owned row is stopped with no owner slug.

        Degrades cleanly: ``managed_remotely`` still gates the UI button,
        but running falls back to the local view (False) and the owner is
        unknown.
        """
        mock_factory = MagicMock()
        mock_factory.get_process_configs = AsyncMock(
            return_value=[
                ProcessConfigModel(
                    name="kraken_feed_publisher",
                    enabled=True,
                    mode="process",
                    class_path="snapper.feeds.KrakenFeed",
                    method="run",
                    parameters={},
                )
            ]
        )
        mock_factory.started_processes = {}
        mock_factory.active_runs = {}
        mock_factory.instance_configs = {}
        mock_factory.autostart_includes = MagicMock(return_value=False)
        mock_factory.coordinator_topic_slug = MagicMock(return_value="coord-0")
        result = await list_configured_processes(
            request=_make_rest_request(), factory=mock_factory, cache=None, _user=MagicMock()
        )
        row = result.payload[0]
        assert row.running is False
        assert row.managed_remotely is True
        assert row.coordinator is None

    @pytest.mark.asyncio
    async def test_summary_unions_remote_feed_running(self) -> None:
        """``feeds_running`` counts a feed publisher running in the feed container."""
        mock_factory = MagicMock()
        mock_factory.get_process_configs = AsyncMock(
            return_value=[
                ProcessConfigModel(
                    name="kraken_feed_publisher",
                    enabled=True,
                    mode="process",
                    class_path="snapper.feeds.KrakenFeed",
                    method="run",
                    parameters={},
                )
            ]
        )
        mock_factory.started_processes = {}
        mock_factory.instance_configs = {}
        mock_factory.build_process_summary_items = AsyncMock(
            return_value=[
                ProcessSummaryItem(
                    name="kraken_feed_publisher",
                    running=False,
                    enabled=True,
                    role="core",
                    lifecycle="long_running",
                )
            ]
        )
        mock_factory.autostart_includes = MagicMock(return_value=False)
        mock_factory.coordinator_topic_slug = MagicMock(return_value="coord-0")
        cache = MagicMock()
        cache.lookup = MagicMock(return_value=(True, "coord-1"))
        result = await get_process_summary(
            request=_make_rest_request(), factory=mock_factory, cache=cache, _user=MagicMock()
        )
        assert result.payload.feeds.total == 1
        assert result.payload.feeds.running == 1
        assert result.payload.processes[0].running is True


class TestListAvailableProcesses:
    """Tests for listing available process types from registry."""

    @pytest.mark.asyncio
    @patch("snapper.server.process_routes.get_registered_processes")
    async def test_list_available_processes(self, mock_get_registry: MagicMock) -> None:
        """Test listing all registered process types.

        Given: Multiple processes registered in the registry,
        When: list_available_processes is called,
        Then: All processes are returned with name, path, and description.
        """
        mock_class = MagicMock()
        mock_get_registry.return_value = {
            "zmq_broker": ProcessRegistryEntry(
                class_ref=mock_class,
                class_path="snapper.ipc.zmq_broker.ZmqBrokerThread",
                method="run",
                description="ZMQ message broker",
                priority=50,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.CORE,
                tags=(),
                parameters_model=None,
                parameters_schema=None,
                enabled=False,
                mode="thread",
            ),
            "feed_publisher": ProcessRegistryEntry(
                class_ref=mock_class,
                class_path="snapper.ipc.feed_publisher.MarketDataPublisherService",
                method="run",
                description="Market data publisher",
                priority=50,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.CORE,
                tags=(),
                parameters_model=None,
                parameters_schema=None,
                enabled=False,
                mode="thread",
            ),
        }
        result = await list_available_processes(request=_make_rest_request(), _user=MagicMock())
        assert result.count == 2
        assert len(result.payload) == 2
        assert result.payload[0].name == "zmq_broker"
        assert result.payload[0].class_path == "snapper.ipc.zmq_broker.ZmqBrokerThread"
        assert result.payload[0].method == "run"
        assert result.payload[0].description == "ZMQ message broker"

    @pytest.mark.asyncio
    @patch("snapper.server.process_routes.get_registered_processes")
    async def test_list_available_processes_empty(self, mock_get_registry: MagicMock) -> None:
        """Test listing returns empty when no processes registered.

        Given: Empty process registry,
        When: list_available_processes is called,
        Then: Empty list with zero count is returned.
        """
        mock_get_registry.return_value = {}
        result = await list_available_processes(request=_make_rest_request(), _user=MagicMock())
        assert result.count == 0
        assert result.payload == []


class TestListConfiguredProcesses:
    """Tests for listing configured process instances."""

    @pytest.mark.asyncio
    async def test_list_configured_processes(self) -> None:
        """Test listing configured process instances.

        Given: A factory with configured processes,
        When: list_configured_processes is called,
        Then: All configurations are returned with running status.
        """
        mock_factory = MagicMock()
        mock_factory.coordinator_topic_slug = MagicMock(return_value="coord-0")
        mock_factory.autostart_includes = MagicMock(return_value=True)
        mock_factory.get_process_configs = AsyncMock(
            return_value=[
                ProcessConfigModel(
                    name="zmq_broker",
                    enabled=True,
                    mode="thread",
                    class_path="snapper.ipc.zmq_broker.ZmqBrokerThread",
                    method="run",
                    parameters={"endpoint": "tcp://0.0.0.0:5555"},
                    note="Test broker",
                    lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                )
            ]
        )
        mock_factory.started_processes = {"zmq_broker": MagicMock()}
        mock_factory.active_runs = {}
        mock_factory.instance_configs = {}
        result = await list_configured_processes(
            request=_make_rest_request(), factory=mock_factory, cache=None, _user=MagicMock()
        )
        assert result.count == 1
        assert len(result.payload) == 1
        process = result.payload[0]
        assert process.name == "zmq_broker"
        assert process.enabled is True
        assert process.mode == "thread"
        assert process.class_path == "snapper.ipc.zmq_broker.ZmqBrokerThread"
        assert process.method == "run"
        assert process.parameters == {"endpoint": "tcp://0.0.0.0:5555"}
        assert process.note == "Test broker"
        assert process.lifecycle == "long_running"
        assert process.running is True
        assert process.is_one_shot is False
        assert process.kind == "instance"
        assert process.wallet_public_id is None
        assert process.parent_template is None

    @pytest.mark.asyncio
    async def test_list_configured_processes_empty(self) -> None:
        """Test listing returns empty when no processes configured.

        Given: A factory with no configured processes,
        When: list_configured_processes is called,
        Then: Empty list with zero count is returned.
        """
        mock_factory = MagicMock()
        mock_factory.get_process_configs = AsyncMock(return_value=[])
        mock_factory.started_processes = {}
        mock_factory.instance_configs = {}
        result = await list_configured_processes(
            request=_make_rest_request(), factory=mock_factory, cache=None, _user=MagicMock()
        )
        assert result.count == 0
        assert result.payload == []

    @pytest.mark.asyncio
    async def test_executor_template_marked_as_template_with_running_false(self) -> None:
        """Bare ``executor_kraken`` row carries ``kind=template`` and ``running=False``.

        Templates are config-only — even if the launcher has somehow
        tracked a process under the template name (legacy state), the
        response forces ``running=False`` so the UI never renders a
        Stop button on a config-only row.
        """
        mock_factory = MagicMock()
        mock_factory.coordinator_topic_slug = MagicMock(return_value="coord-0")
        mock_factory.autostart_includes = MagicMock(return_value=True)
        mock_factory.get_process_configs = AsyncMock(
            return_value=[
                ProcessConfigModel(
                    name="executor_kraken",
                    enabled=False,
                    mode="thread",
                    class_path="snapper.executors.Kraken",
                    method="run",
                    parameters={"throttle": 5},
                ),
            ]
        )
        mock_factory.started_processes = {"executor_kraken": MagicMock()}
        mock_factory.active_runs = {"executor_kraken": "stale-public-id"}
        mock_factory.instance_configs = {}
        result = await list_configured_processes(
            request=_make_rest_request(), factory=mock_factory, cache=None, _user=MagicMock()
        )
        assert result.count == 1
        process = result.payload[0]
        assert process.name == "executor_kraken"
        assert process.kind == "template"
        assert process.running is False
        assert process.active_public_id is None
        assert process.wallet_public_id is None
        assert process.parent_template is None

    @pytest.mark.asyncio
    async def test_synthetic_per_wallet_instance_appears_with_discriminators(self) -> None:
        """Per-wallet instance from ``instance_configs`` joins the response.

        Asserts the synthetic row carries the runtime-only fields:
        ``kind=instance``, ``wallet_public_id`` (from instance config
        parameters), and ``parent_template`` (``executor_<exchange>``).
        """
        wallet = "00000000-0000-7000-8000-0000000000a1"
        mock_factory = MagicMock()
        mock_factory.coordinator_topic_slug = MagicMock(return_value="coord-0")
        mock_factory.autostart_includes = MagicMock(return_value=True)
        mock_factory.get_process_configs = AsyncMock(return_value=[])
        instance = ProcessConfigModel(
            name="executor_kraken_w0000000000a1",
            enabled=True,
            mode="thread",
            class_path="snapper.executors.Kraken",
            method="run",
            parameters={"wallet_public_id": wallet, "throttle": 5},
            note=f"Per-wallet executor for exchange=kraken wallet={wallet}",
        )
        mock_factory.instance_configs = {"executor_kraken_w0000000000a1": instance}
        mock_factory.started_processes = {"executor_kraken_w0000000000a1": MagicMock()}
        mock_factory.active_runs = {"executor_kraken_w0000000000a1": "run-public-id"}
        result = await list_configured_processes(
            request=_make_rest_request(), factory=mock_factory, cache=None, _user=MagicMock()
        )
        assert result.count == 1
        synthetic = result.payload[0]
        assert synthetic.name == "executor_kraken_w0000000000a1"
        assert synthetic.kind == "instance"
        assert synthetic.running is True
        assert synthetic.wallet_public_id == wallet
        assert synthetic.parent_template == "executor_kraken"
        assert synthetic.active_public_id == "run-public-id"

    @pytest.mark.asyncio
    async def test_non_executor_entry_in_instance_configs_skipped(self) -> None:
        """Defensive guard: non-executor name in ``instance_configs`` is skipped.

        ``instance_configs`` is only ever populated with per-wallet executor
        names by the launcher, but the route checks ``is_executor_instance``
        before synthesizing a row. This pins the defensive guard so an
        accidental future caller polluting the dict cannot leak a malformed
        row into ``/configured``.
        """
        mock_factory = MagicMock()
        mock_factory.get_process_configs = AsyncMock(return_value=[])
        spurious = ProcessConfigModel(
            name="zmq_broker",
            enabled=True,
            mode="thread",
            class_path="snapper.broker",
            method="run",
            parameters={},
        )
        mock_factory.instance_configs = {"zmq_broker": spurious}
        mock_factory.started_processes = {"zmq_broker": MagicMock()}
        mock_factory.active_runs = {}
        result = await list_configured_processes(
            request=_make_rest_request(), factory=mock_factory, cache=None, _user=MagicMock()
        )
        assert result.count == 0

    @pytest.mark.asyncio
    async def test_template_and_instance_appear_together(self) -> None:
        """Mixed list: one DB template plus one synthetic instance row.

        Mirrors the runtime steady state of a deployed system —
        ``executor_kraken`` template config in DB and
        ``executor_kraken_w<short>`` running instance synthesized at
        spawn time.
        """
        wallet = "00000000-0000-7000-8000-0000000000a1"
        mock_factory = MagicMock()
        mock_factory.coordinator_topic_slug = MagicMock(return_value="coord-0")
        mock_factory.autostart_includes = MagicMock(return_value=True)
        mock_factory.get_process_configs = AsyncMock(
            return_value=[
                ProcessConfigModel(
                    name="executor_kraken",
                    enabled=False,
                    mode="thread",
                    class_path="snapper.executors.Kraken",
                    method="run",
                    parameters={"throttle": 5},
                ),
            ]
        )
        instance = ProcessConfigModel(
            name="executor_kraken_w0000000000a1",
            enabled=True,
            mode="thread",
            class_path="snapper.executors.Kraken",
            method="run",
            parameters={"wallet_public_id": wallet, "throttle": 5},
        )
        mock_factory.instance_configs = {"executor_kraken_w0000000000a1": instance}
        mock_factory.started_processes = {"executor_kraken_w0000000000a1": MagicMock()}
        mock_factory.active_runs = {}
        result = await list_configured_processes(
            request=_make_rest_request(), factory=mock_factory, cache=None, _user=MagicMock()
        )
        assert result.count == 2
        kinds = {row.name: row.kind for row in result.payload}
        assert kinds == {
            "executor_kraken": "template",
            "executor_kraken_w0000000000a1": "instance",
        }
        template_row = next(r for r in result.payload if r.name == "executor_kraken")
        instance_row = next(r for r in result.payload if r.name == "executor_kraken_w0000000000a1")
        assert template_row.running is False
        assert instance_row.running is True
        assert instance_row.wallet_public_id == wallet
        assert instance_row.parent_template == "executor_kraken"


class TestGetProcessSummary:
    """Tests for lightweight process summary endpoint."""

    @pytest.mark.asyncio
    async def test_empty_processes(self) -> None:
        """Test summary with no configured processes returns all zeros."""
        mock_factory = MagicMock()
        mock_factory.get_process_configs = AsyncMock(return_value=[])
        mock_factory.started_processes = {}
        mock_factory.instance_configs = {}
        mock_factory.build_process_summary_items = AsyncMock(return_value=[])
        mock_factory.coordinator_topic_slug = MagicMock(return_value="coord-0")
        result = await get_process_summary(
            request=_make_rest_request(), factory=mock_factory, cache=None, _user=MagicMock()
        )
        assert result.payload.feeds.running == 0
        assert result.payload.feeds.total == 0
        assert result.payload.strategies.running == 0
        assert result.payload.strategies.total == 0
        assert result.payload.executors.running == 0
        assert result.payload.executors.total == 0
        assert result.payload.brokers.running == 0
        assert result.payload.brokers.total == 0
        assert result.payload.coordinator == "coord-0"
        assert result.payload.processes == []

    @pytest.mark.asyncio
    async def test_mixed_processes_categorization(self) -> None:
        """Templates are excluded; per-wallet instances drive executor counts.

        ``executor_kraken`` is a template (config-only) so it does NOT
        contribute to the executor totals. Two synthesized per-wallet
        instances on ``factory.instance_configs`` provide the count
        instead, mirroring what the launcher actually runs.
        """
        mock_factory = MagicMock()
        mock_factory.get_process_configs = AsyncMock(
            return_value=[
                ProcessConfigModel(
                    name="kraken_feed_publisher",
                    enabled=True,
                    mode="process",
                    class_path="snapper.feeds.KrakenFeed",
                    method="run",
                    parameters={},
                ),
                ProcessConfigModel(
                    name="polygon_feed_publisher",
                    enabled=True,
                    mode="process",
                    class_path="snapper.feeds.PolygonFeed",
                    method="run",
                    parameters={},
                ),
                ProcessConfigModel(
                    name="momentum_strategy",
                    enabled=True,
                    mode="process",
                    class_path="snapper.strategies.Momentum",
                    method="run",
                    parameters={},
                    role=ProcessRoleEnum.STRATEGY,
                ),
                ProcessConfigModel(
                    name="executor_kraken",
                    enabled=True,
                    mode="thread",
                    class_path="snapper.executors.Kraken",
                    method="run",
                    parameters={},
                ),
                ProcessConfigModel(
                    name="zmq_broker",
                    enabled=True,
                    mode="thread",
                    class_path="snapper.ipc.zmq_broker.ZmqBrokerThread",
                    method="run",
                    parameters={},
                ),
            ]
        )
        instance_a = ProcessConfigModel(
            name="executor_kraken_w000000000001",
            enabled=True,
            mode="thread",
            class_path="snapper.executors.Kraken",
            method="run",
            parameters={"wallet_public_id": "00000000-0000-7000-8000-000000000001"},
        )
        instance_b = ProcessConfigModel(
            name="executor_kraken_w000000000002",
            enabled=True,
            mode="thread",
            class_path="snapper.executors.Kraken",
            method="run",
            parameters={"wallet_public_id": "00000000-0000-7000-8000-000000000002"},
        )
        mock_factory.instance_configs = {
            "executor_kraken_w000000000001": instance_a,
            "executor_kraken_w000000000002": instance_b,
        }
        mock_factory.started_processes = {
            "kraken_feed_publisher": MagicMock(),
            "momentum_strategy": MagicMock(),
            "zmq_broker": MagicMock(),
            "executor_kraken_w000000000001": MagicMock(),
        }
        mock_factory.build_process_summary_items = AsyncMock(return_value=[])
        mock_factory.coordinator_topic_slug = MagicMock(return_value="coord-0")
        result = await get_process_summary(
            request=_make_rest_request(), factory=mock_factory, cache=None, _user=MagicMock()
        )
        assert result.payload.feeds.running == 1
        assert result.payload.feeds.total == 2
        assert result.payload.strategies.running == 1
        assert result.payload.strategies.total == 1
        assert result.payload.executors.running == 1
        assert result.payload.executors.total == 2
        assert result.payload.brokers.running == 1
        assert result.payload.brokers.total == 1

    @pytest.mark.asyncio
    async def test_summary_skips_non_executor_in_instance_configs(self) -> None:
        """Summary defensive guard: non-executor entry in ``instance_configs`` skipped.

        Pins the symmetric defensive guard in ``/processes/summary`` so a
        future caller polluting ``instance_configs`` with a non-executor name
        cannot inflate the executor count.
        """
        mock_factory = MagicMock()
        mock_factory.get_process_configs = AsyncMock(return_value=[])
        mock_factory.started_processes = {}
        spurious = ProcessConfigModel(
            name="zmq_broker",
            enabled=True,
            mode="thread",
            class_path="snapper.broker",
            method="run",
            parameters={},
        )
        mock_factory.instance_configs = {"zmq_broker": spurious}
        mock_factory.build_process_summary_items = AsyncMock(return_value=[])
        mock_factory.coordinator_topic_slug = MagicMock(return_value="coord-0")
        result = await get_process_summary(
            request=_make_rest_request(), factory=mock_factory, cache=None, _user=MagicMock()
        )
        assert result.payload.executors.total == 0
        assert result.payload.executors.running == 0

    @pytest.mark.asyncio
    async def test_uncategorized_process_not_counted(self) -> None:
        """Test processes that match no category are excluded from counts."""
        mock_factory = MagicMock()
        mock_factory.get_process_configs = AsyncMock(
            return_value=[
                ProcessConfigModel(
                    name="backfill_symbols",
                    enabled=True,
                    mode="process",
                    class_path="snapper.tasks.Backfill",
                    method="run",
                    parameters={},
                    role=ProcessRoleEnum.TASK,
                ),
            ]
        )
        mock_factory.started_processes = {"backfill_symbols": MagicMock()}
        mock_factory.instance_configs = {}
        mock_factory.build_process_summary_items = AsyncMock(return_value=[])
        mock_factory.coordinator_topic_slug = MagicMock(return_value="coord-0")
        result = await get_process_summary(
            request=_make_rest_request(), factory=mock_factory, cache=None, _user=MagicMock()
        )
        assert result.payload.feeds.total == 0
        assert result.payload.strategies.total == 0
        assert result.payload.executors.total == 0
        assert result.payload.brokers.total == 0

    @pytest.mark.asyncio
    async def test_summary_passes_through_per_process_rows_and_coordinator(self) -> None:
        """`/processes/summary` surfaces the launcher's per-process rows verbatim.

        Given: ``build_process_summary_items`` returns a process-mode row
            carrying sampled RSS/CPU and a thread-mode row whose metrics
            were never sampled (``None``),
        When: the summary endpoint is invoked,
        Then: both rows pass through unchanged (including the ``None``
            fallbacks) and the response ``coordinator`` mirrors the
            launcher's node slug.
        """
        sampled = ProcessSummaryItem(
            name="kraken_feed_publisher",
            running=True,
            enabled=True,
            role="core",
            lifecycle="long_running",
            active_public_id=None,
            rss_bytes=98_304,
            cpu_percent=12.5,
        )
        thread_mode = ProcessSummaryItem(
            name="executor_kraken_w000000000001",
            running=False,
            enabled=True,
            role="core",
            lifecycle="long_running",
            active_public_id=None,
            rss_bytes=None,
            cpu_percent=None,
        )
        mock_factory = MagicMock()
        mock_factory.get_process_configs = AsyncMock(return_value=[])
        mock_factory.started_processes = {}
        mock_factory.instance_configs = {}
        mock_factory.build_process_summary_items = AsyncMock(return_value=[sampled, thread_mode])
        mock_factory.coordinator_topic_slug = MagicMock(return_value="coord-3")
        result = await get_process_summary(
            request=_make_rest_request(), factory=mock_factory, cache=None, _user=MagicMock()
        )
        assert result.payload.coordinator == "coord-3"
        by_name = {item.name: item for item in result.payload.processes}
        assert by_name["kraken_feed_publisher"].rss_bytes == 98_304
        assert by_name["kraken_feed_publisher"].cpu_percent == pytest.approx(12.5)
        assert by_name["executor_kraken_w000000000001"].rss_bytes is None
        assert by_name["executor_kraken_w000000000001"].cpu_percent is None

    def test_summary_data_defaults_coordinator_when_omitted(self) -> None:
        """ProcessSummaryData accepts a payload without ``coordinator``.

        Given: a process-summary payload that omits ``coordinator`` (an
            older API node during a rolling deploy),
        When: ProcessSummaryData validates it,
        Then: ``coordinator`` defaults to ``coord-0`` so a strict frontend
            never rejects the legacy response.
        """
        zero = ProcessCategoryCount(running=0, total=0)
        summary = ProcessSummaryData(
            session_id="s1",
            sequence_id=1,
            public_id="019dbb34-f439-77bd-afa8-ee5321d60311",
            timestamp=datetime(2026, 5, 14, 12, tzinfo=UTC),
            coordinator="coord-2",
            feeds=zero,
            strategies=zero,
            executors=zero,
            brokers=zero,
            processes=[],
        )
        payload = summary.model_dump()
        del payload["coordinator"]

        parsed = ProcessSummaryData.model_validate(payload)

        assert parsed.coordinator == "coord-0"
        assert parsed.processes == []


class TestGetProcessSchema:
    """Tests for retrieving process configuration schemas."""

    @pytest.mark.asyncio
    @patch("snapper.server.process_routes.get_registered_processes")
    async def test_get_process_schema_with_defaults(self, mock_get_registry: MagicMock) -> None:
        """Test schema returns default parameters from class method.

        Given: A process class with get_default_parameters method,
        When: get_process_schema is called,
        Then: Schema includes default parameters and configuration.
        """
        mock_class = MagicMock()
        mock_class.get_default_parameters = MagicMock(
            return_value={"endpoint": "tcp://0.0.0.0:5555"}
        )
        mock_get_registry.return_value = {
            "zmq_broker": ProcessRegistryEntry(
                class_ref=mock_class,
                class_path="snapper.ipc.zmq_broker.ZmqBrokerThread",
                method="run",
                description="ZMQ message broker",
                priority=50,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.CORE,
                tags=(),
                parameters_model=None,
                parameters_schema=None,
                enabled=True,
                mode="thread",
            )
        }
        settings = MagicMock()
        result = await get_process_schema(
            request=_make_rest_request(), name="zmq_broker", settings=settings, _user=MagicMock()
        )
        assert result.payload.name == "zmq_broker"
        assert result.payload.description == "ZMQ message broker"
        assert result.payload.class_path == "snapper.ipc.zmq_broker.ZmqBrokerThread"
        assert result.payload.method == "run"
        assert result.payload.default_enabled is True
        assert result.payload.default_mode == "thread"
        assert result.payload.default_parameters == {"endpoint": "tcp://0.0.0.0:5555"}

    @pytest.mark.asyncio
    @patch("snapper.server.process_routes.get_registered_processes")
    async def test_get_process_schema_without_defaults(self, mock_get_registry: MagicMock) -> None:
        """Test schema falls back when get_default_parameters missing.

        Given: A process class without get_default_parameters method,
        When: get_process_schema is called,
        Then: Schema returns empty parameters defaults.
        """
        mock_class = MagicMock()
        del mock_class.get_default_parameters
        mock_get_registry.return_value = {
            "custom_process": ProcessRegistryEntry(
                class_ref=mock_class,
                class_path="custom.Process",
                method="run",
                description="Custom process",
                priority=50,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.CORE,
                tags=(),
                parameters_model=None,
                parameters_schema=None,
                enabled=False,
                mode="thread",
            )
        }
        settings = MagicMock()
        result = await get_process_schema(
            request=_make_rest_request(),
            name="custom_process",
            settings=settings,
            _user=MagicMock(),
        )
        assert result.payload.name == "custom_process"
        assert result.payload.default_enabled is False
        assert result.payload.default_mode == "thread"
        assert result.payload.default_parameters == {}

    @pytest.mark.asyncio
    @patch("snapper.server.process_routes.get_registered_processes")
    async def test_get_process_schema_get_default_parameters_raises(
        self, mock_get_registry: MagicMock
    ) -> None:
        """Test schema handles get_default_parameters exceptions.

        Given: A process class where get_default_parameters raises an error,
        When: get_process_schema is called,
        Then: Schema returns empty parameters without propagating error.
        """
        mock_class = MagicMock()
        mock_class.get_default_parameters = MagicMock(side_effect=Exception("DB error"))
        mock_get_registry.return_value = {
            "failing_process": ProcessRegistryEntry(
                class_ref=mock_class,
                class_path="failing.Process",
                method="run",
                description="Process with failing get_default_parameters",
                priority=50,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.CORE,
                tags=(),
                parameters_model=None,
                parameters_schema=None,
                enabled=True,
                mode="process",
            )
        }
        settings = MagicMock()
        result = await get_process_schema(
            request=_make_rest_request(),
            name="failing_process",
            settings=settings,
            _user=MagicMock(),
        )
        assert result.payload.default_parameters == {}
        assert result.payload.default_enabled is True
        assert result.payload.default_mode == "process"

    @pytest.mark.asyncio
    @patch("snapper.server.process_routes.get_registered_processes")
    async def test_get_process_schema_not_found(self, mock_get_registry: MagicMock) -> None:
        """Test 404 is raised for unknown process name.

        Given: An empty process registry,
        When: get_process_schema is called with unknown name,
        Then: HTTPException 404 is raised.
        """
        mock_get_registry.return_value = {}
        settings = MagicMock()
        with pytest.raises(HTTPException) as exc_info:
            await get_process_schema(
                request=_make_rest_request(),
                name="nonexistent",
                settings=settings,
                _user=MagicMock(),
            )
        assert exc_info.value.status_code == 404
        assert "not found in registry" in exc_info.value.detail


class TestStartProcess:
    """Tests for starting process instances."""

    @pytest.mark.asyncio
    async def test_start_process_with_overrides(self) -> None:
        """Test starting process with parameter overrides.

        Given: A factory and process start request with overrides,
        When: start_process is called,
        Then: Process is started with specified parameters.
        """
        mock_factory = MagicMock()
        mock_factory.start_process_by_name = AsyncMock(
            return_value=ProcessStartResult(
                status="success", message="started", public_id="run-001"
            )
        )
        body = ProcessStartRequest(
            session_id="test-sid",
            sequence_id=1,
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessStartBody(
                mode="process",
                parameters={"endpoint": "tcp://0.0.0.0:6666"},
            ),
        )
        result = await start_process(
            http_request=_make_rest_request(),
            name="zmq_broker",
            body=body,
            factory=mock_factory,
            user=MagicMock(operator_public_ids=[]),
            repo=MagicMock(),
            _csrf=None,
        )
        assert result.payload.status == "success"
        assert result.payload.name == "zmq_broker"
        assert result.payload.process_public_id == "run-001"
        mock_factory.start_process_by_name.assert_awaited_once_with(
            name="zmq_broker",
            mode="process",
            parameters={"endpoint": "tcp://0.0.0.0:6666"},
        )

    @pytest.mark.asyncio
    async def test_start_process_without_overrides(self) -> None:
        """Test starting process with default parameters.

        Given: A factory and process start request without overrides,
        When: start_process is called,
        Then: Process is started with None parameters.
        """
        mock_factory = MagicMock()
        mock_factory.start_process_by_name = AsyncMock(
            return_value=ProcessStartResult(status="success", message="started")
        )
        body = ProcessStartRequest(
            session_id="test-sid",
            sequence_id=1,
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessStartBody(
                mode=None,
                parameters=None,
            ),
        )
        result = await start_process(
            http_request=_make_rest_request(),
            name="zmq_broker",
            body=body,
            factory=mock_factory,
            user=MagicMock(operator_public_ids=[]),
            repo=MagicMock(),
            _csrf=None,
        )
        assert result.payload.status == "success"
        mock_factory.start_process_by_name.assert_awaited_once_with(
            name="zmq_broker", mode=None, parameters=None
        )

    @pytest.mark.asyncio
    async def test_start_bare_executor_template_returns_422(self) -> None:
        """Bare executor template start raises 422 with helpful redirect message.

        Templates are config-only; the operator must target a per-wallet
        instance. The handler short-circuits before any factory call.
        """
        mock_factory = MagicMock()
        mock_factory.start_process_by_name = AsyncMock()
        body = ProcessStartRequest(
            session_id="test-sid",
            sequence_id=1,
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessStartBody(mode=None, parameters=None),
        )
        with pytest.raises(HTTPException) as exc_info:
            await start_process(
                http_request=_make_rest_request(),
                name="executor_kraken",
                body=body,
                factory=mock_factory,
                user=MagicMock(operator_public_ids=[]),
                repo=MagicMock(),
                _csrf=None,
            )
        assert exc_info.value.status_code == 422
        assert "executor_kraken" in str(exc_info.value.detail)
        assert "_w<wallet_short>" in str(exc_info.value.detail)
        mock_factory.start_process_by_name.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_start_per_wallet_executor_instance_succeeds(self) -> None:
        """Per-wallet instance name passes through to ``start_process_by_name``."""
        mock_factory = MagicMock()
        mock_factory.start_process_by_name = AsyncMock(
            return_value=ProcessStartResult(
                status="success", message="started", public_id="run-002"
            )
        )
        body = ProcessStartRequest(
            session_id="test-sid",
            sequence_id=1,
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessStartBody(mode=None, parameters=None),
        )
        result = await start_process(
            http_request=_make_rest_request(),
            name="executor_kraken_w0000000000a1",
            body=body,
            factory=mock_factory,
            user=MagicMock(operator_public_ids=[]),
            repo=MagicMock(),
            _csrf=None,
        )
        assert result.payload.status == "success"
        assert result.payload.process_public_id == "run-002"
        mock_factory.start_process_by_name.assert_awaited_once_with(
            name="executor_kraken_w0000000000a1", mode=None, parameters=None
        )


class TestStopProcess:
    """Tests for stopping running process instances."""

    @pytest.mark.asyncio
    async def test_stop_process(self) -> None:
        """Test stopping a running process.

        Given: A factory with a running process,
        When: stop_process is called,
        Then: Process is stopped and status is returned.
        """
        mock_factory = MagicMock()
        mock_factory.stop_process_by_name = AsyncMock(
            return_value=ProcessStopResult(status="success", message="stopped")
        )
        result = await stop_process(
            request=_make_rest_request(),
            name="zmq_broker",
            factory=mock_factory,
            _user=MagicMock(),
            _csrf=None,
        )
        assert result.payload.status == "success"
        assert result.payload.name == "zmq_broker"
        mock_factory.stop_process_by_name.assert_awaited_once_with("zmq_broker")


class TestResolveStrategyStartLaunchParameters:
    """Tests for strategy start launch-parameter resolution."""

    @pytest.mark.asyncio
    async def test_returns_persisted_params_without_raw_mutation_target(self) -> None:
        """Resolved parameters are still returned when no raw dict is present.

        Given: A classified strategy and no mutable persisted params dict,
        When: Start launch parameters are resolved,
        Then: The enforced persisted parameters are returned unchanged.
        """
        persisted_params: dict[str, object] = {"wallet_public_id": "wallet-1"}
        classification = StrategyProcessClassification(
            treat_as_strategy=True,
            parameters={"name": "strategy"},
            row_role=ProcessRoleEnum.STRATEGY,
            registry_role=None,
        )
        scope = StrategyWalletScope(
            True,
            persisted_params,
            "",
            "wallet-1",
            None,
        )
        with patch(
            "snapper.server.process_routes.enforce_classified_strategy_scope_complete",
            new=AsyncMock(return_value=scope),
        ):
            result = await _resolve_strategy_start_launch_parameters(
                MagicMock(),
                MagicMock(operator_public_ids=[]),
                classification,
                None,
            )
        assert result == persisted_params


class TestProcessStartRequest:
    """Tests for ProcessStartRequest schema validation."""

    def test_process_start_request_all_fields(self) -> None:
        """Test ProcessStartRequest with all fields populated.

        Given: All request parameters specified,
        When: ProcessStartRequest is created,
        Then: All fields are correctly assigned.
        """
        request = ProcessStartRequest(
            session_id="test-sid",
            sequence_id=1,
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessStartBody(
                mode="process",
                parameters={"key": "value"},
            ),
        )
        assert request.payload.mode == "process"
        assert request.payload.parameters == {"key": "value"}

    def test_process_start_request_defaults(self) -> None:
        """Test ProcessStartRequest with None defaults.

        Given: All parameters set to None,
        When: ProcessStartRequest is created,
        Then: All fields are None.
        """
        request = ProcessStartRequest(
            session_id="test-sid",
            sequence_id=1,
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessStartBody(
                mode=None,
                parameters=None,
            ),
        )
        assert request.payload.mode is None
        assert request.payload.parameters is None


class TestCreateProcessConfiguration:
    """Tests for creating new process configurations."""

    @pytest.mark.asyncio
    @patch("snapper.server.process_routes.get_registered_processes")
    async def test_create_process_configuration_success(self, mock_get_registry: MagicMock) -> None:
        """Test creating new process configuration.

        Given: A valid template in the registry,
        When: create_process_configuration is called,
        Then: Configuration is created in database.
        """
        mock_factory = MagicMock()
        mock_factory.get_class_defaults.return_value = {
            "enabled": False,
            "mode": "thread",
            "parameters": {
                "name": "default_strategy",
                "inputs": ["BTC-USD:1h"],
                "output": "signals.default_strategy",
            },
            "lifecycle": ProcessLifecycleEnum.LONG_RUNNING,
            "role": ProcessRoleEnum.STRATEGY,
            "tags": ("strategy",),
            "parameters_schema": {"type": "object"},
        }
        mock_factory.create_process_config = AsyncMock()
        strategy_class = MagicMock()
        strategy_class.get_default_parameters.return_value = {
            "name": "default_strategy",
            "inputs": ["BTC-USD:1h"],
            "output": "signals.default_strategy",
        }
        mock_get_registry.return_value = {
            "strategy_macd_btc_1h": ProcessRegistryEntry(
                class_ref=strategy_class,
                class_path="snapper.strategies.process_wrapper.MACDStrategyBTC",
                method="start",
                description="MACD strategy",
                priority=50,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.STRATEGY,
                tags=("strategy",),
                parameters_model=None,
                parameters_schema={"type": "object"},
                enabled=False,
                mode="thread",
            )
        }
        request = ProcessCreateRequest(
            session_id="test-sid",
            sequence_id=1,
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessCreateBody(
                name="strategy_macd_custom",
                template="strategy_macd_btc_1h",
                enabled=True,
                mode="process",
                parameters={"name": "macd_custom"},
                note="UI created",
            ),
        )
        settings = MagicMock()
        result = await create_process_configuration(
            http_request=_make_rest_request(),
            body=request,
            factory=mock_factory,
            settings=settings,
            user=MagicMock(operator_public_ids=[]),
            repo=MagicMock(),
            _csrf=None,
        )
        mock_factory.create_process_config.assert_awaited_once_with(
            name="strategy_macd_custom",
            class_path="snapper.strategies.process_wrapper.MACDStrategyBTC",
            method="start",
            enabled=True,
            mode="process",
            parameters={
                "name": "macd_custom",
                "inputs": ["BTC-USD:1h"],
                "output": "signals.default_strategy",
            },
            lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
            role=ProcessRoleEnum.STRATEGY,
            tags=("strategy",),
            parameters_schema={"type": "object"},
            note="UI created",
            template="strategy_macd_btc_1h",
        )
        assert result.payload.status == "created"
        assert result.payload.process.name == "strategy_macd_custom"

    @pytest.mark.asyncio
    @patch("snapper.server.process_routes.get_registered_processes")
    async def test_create_process_configuration_template_not_found(
        self, mock_get_registry: MagicMock
    ) -> None:
        """Test 404 when template not found in registry.

        Given: An empty process registry,
        When: create_process_configuration is called with unknown template,
        Then: HTTPException 404 is raised.
        """
        mock_get_registry.return_value = {}
        request = ProcessCreateRequest(
            session_id="test-sid",
            sequence_id=1,
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessCreateBody(
                name="unknown",
                template="missing",
                enabled=None,
                mode=None,
                parameters=None,
                note=None,
            ),
        )
        factory = MagicMock()
        settings = MagicMock()
        with pytest.raises(HTTPException) as exc_info:
            await create_process_configuration(
                http_request=_make_rest_request(),
                body=request,
                factory=factory,
                settings=settings,
                user=MagicMock(operator_public_ids=[]),
                repo=MagicMock(),
                _csrf=None,
            )
        assert exc_info.value.status_code == 404
        assert "Template" in exc_info.value.detail

    @pytest.mark.asyncio
    @patch("snapper.server.process_routes.get_registered_processes")
    async def test_create_process_configuration_conflict(
        self, mock_get_registry: MagicMock
    ) -> None:
        """Test 409 when configuration already exists.

        Given: A factory that raises ValueError for duplicate config,
        When: create_process_configuration is called,
        Then: HTTPException 409 is raised.
        """
        mock_factory = MagicMock()
        mock_factory.get_class_defaults.return_value = {
            "enabled": False,
            "mode": "thread",
            "parameters": {},
            "lifecycle": ProcessLifecycleEnum.LONG_RUNNING,
            "role": ProcessRoleEnum.CORE,
            "tags": (),
        }
        mock_factory.create_process_config = AsyncMock(side_effect=ValueError("exists"))
        strategy_class = MagicMock()
        mock_get_registry.return_value = {
            "strategy_macd_btc_1h": ProcessRegistryEntry(
                class_ref=strategy_class,
                class_path="path",
                method="start",
                description="desc",
                priority=50,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.STRATEGY,
                tags=(),
                parameters_model=None,
                parameters_schema=None,
                enabled=False,
                mode="thread",
            )
        }
        request = ProcessCreateRequest(
            session_id="test-sid",
            sequence_id=1,
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessCreateBody(
                name="strategy_macd_custom",
                template="strategy_macd_btc_1h",
                enabled=None,
                mode=None,
                parameters=None,
                note=None,
            ),
        )
        with pytest.raises(HTTPException) as exc_info:
            await create_process_configuration(
                http_request=_make_rest_request(),
                body=request,
                factory=mock_factory,
                settings=MagicMock(),
                user=MagicMock(operator_public_ids=[]),
                repo=MagicMock(),
                _csrf=None,
            )
        assert exc_info.value.status_code == 409
        assert "exists" in exc_info.value.detail


class TestProcessRoutesEdgeCases:
    """Tests for edge cases in process route handling."""

    @pytest.mark.asyncio
    @patch("snapper.server.process_routes.get_registered_processes")
    async def test_list_available_processes_with_empty_tags(
        self, mock_get_registry: MagicMock
    ) -> None:
        """Verify empty tags are handled correctly.

        Given: A process with empty tags tuple,
        When: list_available_processes is called,
        Then: Empty tags list is returned.
        """
        mock_class = MagicMock()
        mock_get_registry.return_value = {
            "test_process": ProcessRegistryEntry(
                class_ref=mock_class,
                class_path="snapper.test.TestProcess",
                method="run",
                description="Test process",
                priority=50,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.CORE,
                tags=(),
                parameters_model=None,
                parameters_schema=None,
                enabled=False,
                mode="thread",
            ),
        }
        result = await list_available_processes(request=_make_rest_request(), _user=MagicMock())
        assert result.count == 1
        assert result.payload[0].tags == []

    @pytest.mark.asyncio
    @patch("snapper.server.process_routes.get_registered_processes")
    async def test_create_process_get_default_parameters_exception(
        self, mock_get_registry: MagicMock
    ) -> None:
        """Verify graceful handling of get_default_parameters exception.

        Given: A process class where get_default_parameters raises exception,
        When: create_process_configuration is called,
        Then: Configuration is created with provided parameters only.
        """
        mock_factory = MagicMock()
        mock_factory.get_templates = AsyncMock(return_value=["test_template"])
        mock_factory.create_process_config = AsyncMock(return_value=MagicMock())

        class FailingKwargsClass:
            @staticmethod
            def get_default_parameters(settings: MagicMock) -> dict[str, object]:
                raise RuntimeError("Failed to load defaults")

        mock_get_registry.return_value = {
            "test_template": ProcessRegistryEntry(
                class_ref=FailingKwargsClass,
                class_path="snapper.test.FailingKwargsClass",
                method="run",
                description="Test with failing get_default_parameters",
                priority=50,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.CORE,
                tags=(),
                parameters_model=None,
                parameters_schema=None,
                enabled=False,
                mode="thread",
            )
        }
        request = ProcessCreateRequest(
            session_id="test-sid",
            sequence_id=1,
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessCreateBody(
                name="test_process",
                template="test_template",
                enabled=True,
                mode="thread",
                parameters={"custom": "value"},
                note=None,
            ),
        )
        await create_process_configuration(
            http_request=_make_rest_request(),
            body=request,
            factory=mock_factory,
            settings=MagicMock(),
            user=MagicMock(operator_public_ids=[]),
            repo=MagicMock(),
            _csrf=None,
        )
        mock_factory.create_process_config.assert_called_once()
        call_args = mock_factory.create_process_config.call_args
        assert call_args[1]["parameters"] == {"custom": "value"}

    @pytest.mark.asyncio
    @patch("snapper.server.process_routes.get_registered_processes")
    async def test_create_process_no_get_default_parameters(
        self, mock_get_registry: MagicMock
    ) -> None:
        """Verify creation succeeds without get_default_parameters method.

        Given: A process class without get_default_parameters method,
        When: create_process_configuration is called,
        Then: Configuration is created with provided parameters.
        """
        mock_factory = MagicMock()
        mock_factory.get_templates = AsyncMock(return_value=["test_template"])
        mock_factory.create_process_config = AsyncMock(return_value=MagicMock())
        mock_class = MagicMock(spec=[])
        mock_get_registry.return_value = {
            "test_template": ProcessRegistryEntry(
                class_ref=mock_class,
                class_path="snapper.test.NoKwargsClass",
                method="run",
                description="Test without get_default_parameters",
                priority=50,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.CORE,
                tags=(),
                parameters_model=None,
                parameters_schema=None,
                enabled=False,
                mode="thread",
            )
        }
        request = ProcessCreateRequest(
            session_id="test-sid",
            sequence_id=1,
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessCreateBody(
                name="test_process",
                template="test_template",
                enabled=True,
                mode="thread",
                parameters={"custom": "value"},
                note=None,
            ),
        )
        await create_process_configuration(
            http_request=_make_rest_request(),
            body=request,
            factory=mock_factory,
            settings=MagicMock(),
            user=MagicMock(operator_public_ids=[]),
            repo=MagicMock(),
            _csrf=None,
        )
        mock_factory.create_process_config.assert_called_once()
        call_args = mock_factory.create_process_config.call_args
        assert call_args[1]["parameters"] == {"custom": "value"}

    @pytest.mark.asyncio
    @patch("snapper.server.process_routes.get_registered_processes")
    async def test_create_process_with_empty_tags(self, mock_get_registry: MagicMock) -> None:
        """Verify empty tags are handled correctly.

        Given: A process with empty tags tuple,
        When: create_process_configuration is called,
        Then: Configuration is created with empty tags tuple.
        """
        mock_factory = MagicMock()
        mock_factory.get_templates = AsyncMock(return_value=["test_template"])
        mock_factory.create_process_config = AsyncMock(return_value=MagicMock())
        mock_class = MagicMock()
        mock_class.get_default_parameters = MagicMock(return_value={})
        mock_get_registry.return_value = {
            "test_template": ProcessRegistryEntry(
                class_ref=mock_class,
                class_path="snapper.test.TestClass",
                method="run",
                description="Test with empty tags",
                priority=50,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.CORE,
                tags=(),
                parameters_model=None,
                parameters_schema=None,
                enabled=False,
                mode="thread",
            )
        }
        request = ProcessCreateRequest(
            session_id="test-sid",
            sequence_id=1,
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessCreateBody(
                name="test_process",
                template="test_template",
                enabled=True,
                mode="thread",
                parameters=None,
                note=None,
            ),
        )
        await create_process_configuration(
            http_request=_make_rest_request(),
            body=request,
            factory=mock_factory,
            settings=MagicMock(),
            user=MagicMock(operator_public_ids=[]),
            repo=MagicMock(),
            _csrf=None,
        )
        mock_factory.create_process_config.assert_called_once()
        call_args = mock_factory.create_process_config.call_args
        assert call_args[1]["tags"] == ()

    @pytest.mark.asyncio
    async def test_list_process_runs(self) -> None:
        """Test listing historical process run records.

        Given: A factory with recent run history,
        When: list_process_runs is called,
        Then: All run records are returned with details.
        """
        mock_factory = MagicMock()
        mock_factory.get_recent_runs = AsyncMock(
            return_value=[
                {
                    "public_id": "run-001",
                    "process_name": "zmq_broker",
                    "status": "succeeded",
                    "role": "core",
                    "lifecycle": "long_running",
                    "parameters": None,
                    "result": None,
                    "error": None,
                    "tags": [],
                    "started_at": "2026-01-04T10:00:00Z",
                    "completed_at": "2026-01-04T11:00:00Z",
                    "session_id": "test-sid",
                    "sequence_id": 1,
                    "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
                },
                {
                    "public_id": "run-002",
                    "process_name": "feed_publisher",
                    "status": "running",
                    "role": "core",
                    "lifecycle": "long_running",
                    "parameters": None,
                    "result": None,
                    "error": None,
                    "tags": [],
                    "started_at": "2026-01-04T10:00:00Z",
                    "completed_at": None,
                    "session_id": "test-sid",
                    "sequence_id": 2,
                    "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
                },
            ]
        )
        result = await list_process_runs(
            request=_make_rest_request(),
            factory=mock_factory,
            _user=MagicMock(),
            limit=50,
            name=None,
        )
        assert result.count == 2
        assert len(result.payload) == 2
        mock_factory.get_recent_runs.assert_awaited_once_with(limit=50, name=None)

    @pytest.mark.asyncio
    async def test_list_process_runs_filtered(self) -> None:
        """Test listing runs filtered by process name.

        Given: A factory with run history for specific process,
        When: list_process_runs is called with name filter,
        Then: Only matching run records are returned.
        """
        mock_factory = MagicMock()
        mock_factory.get_recent_runs = AsyncMock(
            return_value=[
                {
                    "public_id": "run-001",
                    "process_name": "zmq_broker",
                    "status": "succeeded",
                    "role": "core",
                    "lifecycle": "long_running",
                    "parameters": None,
                    "result": None,
                    "error": None,
                    "tags": [],
                    "started_at": "2026-01-04T10:00:00Z",
                    "completed_at": "2026-01-04T11:00:00Z",
                    "session_id": "test-sid",
                    "sequence_id": 1,
                    "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
                },
            ]
        )
        result = await list_process_runs(
            request=_make_rest_request(),
            factory=mock_factory,
            _user=MagicMock(),
            limit=10,
            name="zmq_broker",
        )
        assert result.count == 1
        mock_factory.get_recent_runs.assert_awaited_once_with(limit=10, name="zmq_broker")


class TestEnforceStrategyScope:
    """Tests for the strategy permission helper."""

    @pytest.mark.asyncio
    async def test_non_strategy_role_skipped(self) -> None:
        """Non-strategy templates bypass the operator/wallet check."""
        await _enforce_strategy_scope(
            parameters={"operator_public_id": "op-1", "wallet_public_id": "w-1"},
            role=ProcessRoleEnum.CORE,
            principal=MagicMock(operator_public_ids=[]),
            repo=MagicMock(),
        )

    @pytest.mark.asyncio
    async def test_paper_empty_defaults_allowed(self) -> None:
        """Paper strategy with empty operator/wallet keeps legacy behavior."""
        await _enforce_strategy_scope(
            parameters=_strategy_params(
                operator_public_id="",
                wallet_public_id="",
                exchange="paper",
            ),
            role=ProcessRoleEnum.STRATEGY,
            principal=MagicMock(operator_public_ids=[]),
            repo=MagicMock(),
        )

    @pytest.mark.asyncio
    async def test_live_empty_operator_and_wallet_rejected(self) -> None:
        """Live strategy cannot bypass wallet resolution with empty scope."""
        with pytest.raises(HTTPException) as exc_info:
            await _enforce_strategy_scope(
                parameters=_strategy_params(
                    operator_public_id="",
                    wallet_public_id="",
                    exchange="kraken",
                ),
                role=ProcessRoleEnum.STRATEGY,
                principal=MagicMock(operator_public_ids=["op-1"]),
                repo=MagicMock(),
            )
        assert exc_info.value.status_code == 400
        assert exc_info.value.detail == (
            "operator_public_id required for live strategy wallet resolution"
        )

    @pytest.mark.asyncio
    async def test_wallet_without_operator_rejected(self) -> None:
        """Wallet supplied without operator -> 400."""
        with pytest.raises(HTTPException) as exc_info:
            await _enforce_strategy_scope(
                parameters=_strategy_params(operator_public_id="", wallet_public_id="w-1"),
                role=ProcessRoleEnum.STRATEGY,
                principal=MagicMock(operator_public_ids=[]),
                repo=MagicMock(),
            )
        assert exc_info.value.status_code == 400

    @pytest.mark.asyncio
    async def test_operator_not_in_principal_rejected(self) -> None:
        """Operator not in principal.operator_public_ids -> 403."""
        with pytest.raises(HTTPException) as exc_info:
            await _enforce_strategy_scope(
                parameters=_strategy_params(operator_public_id="op-x", wallet_public_id=""),
                role=ProcessRoleEnum.STRATEGY,
                principal=MagicMock(operator_public_ids=["op-1"], username="alice"),
                repo=MagicMock(),
            )
        assert exc_info.value.status_code == 403
        assert "alice" in exc_info.value.detail
        assert "op-x" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_operator_only_passes_without_wallet(self) -> None:
        """Operator membership without wallet resolves a single accessible wallet."""
        repo = MagicMock()
        repo.list_accessible_wallets_for_operators = AsyncMock(return_value=[_wallet_row("w-1")])
        parameters = _strategy_params(
            operator_public_id="op-1",
            wallet_public_id="",
            exchange="kraken",
        )
        await _enforce_strategy_scope(
            parameters=parameters,
            role=ProcessRoleEnum.STRATEGY,
            principal=MagicMock(operator_public_ids=["op-1"]),
            repo=repo,
        )
        assert parameters["wallet_public_id"] == "w-1"
        repo.list_accessible_wallets_for_operators.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_operator_wallet_unresolved_maps_to_400(self) -> None:
        """Operator-scoped wallet autolookup with no candidates returns 400."""
        repo = MagicMock()
        repo.list_accessible_wallets_for_operators = AsyncMock(return_value=[])
        with pytest.raises(HTTPException) as exc_info:
            await _enforce_strategy_scope(
                parameters=_strategy_params(
                    operator_public_id="op-1",
                    wallet_public_id="",
                    exchange="kraken",
                ),
                role=ProcessRoleEnum.STRATEGY,
                principal=MagicMock(operator_public_ids=["op-1"], username="alice"),
                repo=repo,
            )
        assert exc_info.value.status_code == 400
        assert exc_info.value.detail == "specify wallet_public_id; 0 wallets accessible"

    @pytest.mark.asyncio
    async def test_non_sqlalchemy_repo_skips_grant_check(self) -> None:
        """In-memory / non-SQLAlchemy repos defer to caller for grants."""
        await _enforce_strategy_scope(
            parameters=_strategy_params(operator_public_id="op-1", wallet_public_id="w-1"),
            role=ProcessRoleEnum.STRATEGY,
            principal=MagicMock(operator_public_ids=["op-1"]),
            repo=MagicMock(),
        )

    @pytest.mark.asyncio
    async def test_active_grant_present_succeeds(self) -> None:
        """An active grant for (operator, wallet) lets the call through."""
        repo = MagicMock(spec=SQLAlchemyRepository)
        repo.list_active_scope_grants_for_wallet = AsyncMock(
            return_value=[{"operator_public_id": "op-1"}]
        )
        await _enforce_strategy_scope(
            parameters=_strategy_params(operator_public_id="op-1", wallet_public_id="w-1"),
            role=ProcessRoleEnum.STRATEGY,
            principal=MagicMock(operator_public_ids=["op-1"]),
            repo=repo,
        )
        repo.list_active_scope_grants_for_wallet.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_no_matching_grant_rejected(self) -> None:
        """No active grant on (operator, wallet) -> 403."""
        repo = MagicMock(spec=SQLAlchemyRepository)
        repo.list_active_scope_grants_for_wallet = AsyncMock(
            return_value=[{"operator_public_id": "op-other"}]
        )
        with pytest.raises(HTTPException) as exc_info:
            await _enforce_strategy_scope(
                parameters=_strategy_params(operator_public_id="op-1", wallet_public_id="w-1"),
                role=ProcessRoleEnum.STRATEGY,
                principal=MagicMock(operator_public_ids=["op-1"]),
                repo=repo,
            )
        assert exc_info.value.status_code == 403
        assert "op-1" in exc_info.value.detail
        assert "w-1" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_non_string_parameters_rejected_by_strategy_validation(self) -> None:
        """Non-string scope fields fail the strategy parameter validation gate."""
        with pytest.raises(HTTPException) as exc_info:
            await _enforce_strategy_scope(
                parameters=_strategy_params(
                    operator_public_id=42,
                    wallet_public_id=["x"],
                    exchange="paper",
                ),
                role=ProcessRoleEnum.STRATEGY,
                principal=MagicMock(operator_public_ids=[]),
                repo=MagicMock(),
            )
        assert exc_info.value.status_code == 400
        assert exc_info.value.detail == "invalid strategy parameters for wallet resolution"

    @pytest.mark.asyncio
    async def test_invalid_exchange_rejected_before_wallet_resolution(self) -> None:
        """Unknown strategy exchange fails before autolookup can bind a wallet."""
        repo = MagicMock()
        repo.list_accessible_wallets_for_operators = AsyncMock()
        with pytest.raises(HTTPException) as exc_info:
            await _enforce_strategy_scope(
                parameters=_strategy_params(
                    operator_public_id="op-1",
                    wallet_public_id="",
                    exchange="bogus",
                ),
                role=ProcessRoleEnum.STRATEGY,
                principal=MagicMock(operator_public_ids=["op-1"], username="alice"),
                repo=repo,
            )
        assert exc_info.value.status_code == 400
        assert exc_info.value.detail == "invalid strategy parameters for wallet resolution"
        repo.list_accessible_wallets_for_operators.assert_not_called()


class TestGetRepositoryForProcesses:
    """Tests for the local repository dependency."""

    def test_returns_repository(self) -> None:
        """Dependency wraps get_settings + get_repository."""
        with patch("snapper.server.process_routes.get_settings") as mock_get_settings, patch(
            "snapper.server.process_routes.get_repository"
        ) as mock_get_repo:
            mock_get_settings.return_value = MagicMock(db_url="sqlite:///:memory:")
            mock_get_repo.return_value = MagicMock(name="repo")
            result = get_repository_for_processes()
            mock_get_repo.assert_called_once_with("sqlite:///:memory:")
            assert result is mock_get_repo.return_value


class TestScopeHelpers:
    """Tests for the strategy scope helpers."""

    @pytest.mark.asyncio
    async def test_enforce_wallet_grant_exists_passes(self) -> None:
        """A matching grant lets the call return without raising."""
        repo = MagicMock(spec=SQLAlchemyRepository)
        repo.list_active_scope_grants_for_wallet = AsyncMock(
            return_value=[{"operator_public_id": "op-1"}]
        )
        await _enforce_wallet_grant_exists(repo, "op-1", "w-1", datetime.now(UTC))

    @pytest.mark.asyncio
    async def test_enforce_strategy_outputs_covered_skips_paper_exchange(self) -> None:
        """Paper exchange has no Instrument rows so the coverage check skips."""
        repo = MagicMock(spec=SQLAlchemyRepository)
        repo.list_grant_covered_instrument_public_ids = AsyncMock()
        await _enforce_strategy_outputs_covered(
            repo,
            {"outputs": ["BTC-USD"], "exchange": "paper"},
            "op-1",
            "w-1",
            datetime.now(UTC),
        )
        repo.list_grant_covered_instrument_public_ids.assert_not_called()

    @pytest.mark.asyncio
    async def test_enforce_strategy_outputs_covered_skips_when_outputs_missing(self) -> None:
        """Empty outputs list short-circuits the coverage check."""
        repo = MagicMock(spec=SQLAlchemyRepository)
        repo.list_grant_covered_instrument_public_ids = AsyncMock()
        await _enforce_strategy_outputs_covered(
            repo,
            {"outputs": [], "exchange": "kraken"},
            "op-1",
            "w-1",
            datetime.now(UTC),
        )
        repo.list_grant_covered_instrument_public_ids.assert_not_called()

    @pytest.mark.asyncio
    async def test_enforce_strategy_outputs_covered_skips_non_string_inputs(self) -> None:
        """Non-list outputs / non-string exchange short-circuit cleanly."""
        repo = MagicMock(spec=SQLAlchemyRepository)
        repo.list_grant_covered_instrument_public_ids = AsyncMock()
        await _enforce_strategy_outputs_covered(
            repo,
            {"outputs": "BTC-USD", "exchange": "kraken"},
            "op-1",
            "w-1",
            datetime.now(UTC),
        )
        repo.list_grant_covered_instrument_public_ids.assert_not_called()

    @pytest.mark.asyncio
    async def test_enforce_strategy_outputs_covered_passes_for_covered_outputs(self) -> None:
        """All outputs map to covered instrument_public_ids -> return."""
        repo = MagicMock(spec=SQLAlchemyRepository)
        repo.list_grant_covered_instrument_public_ids = AsyncMock(return_value={"i-btc"})
        repo.get_instrument_public_ids_by_symbols = AsyncMock(return_value={"BTC-USD": "i-btc"})
        await _enforce_strategy_outputs_covered(
            repo,
            {"outputs": ["BTC-USD"], "exchange": "kraken"},
            "op-1",
            "w-1",
            datetime.now(UTC),
        )
        repo.get_instrument_public_ids_by_symbols.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_enforce_strategy_outputs_covered_batches_symbol_lookup(self) -> None:
        """Multiple outputs are resolved by one repository call."""
        repo = MagicMock(spec=SQLAlchemyRepository)
        repo.list_grant_covered_instrument_public_ids = AsyncMock(return_value={"i-btc", "i-eth"})
        repo.get_instrument_public_ids_by_symbols = AsyncMock(
            return_value={"BTC-USD": "i-btc", "ETH-USD": "i-eth"}
        )
        as_of = datetime.now(UTC)
        await _enforce_strategy_outputs_covered(
            repo,
            {"outputs": ["BTC-USD", "ETH-USD", "BTC-USD"], "exchange": "kraken"},
            "op-1",
            "w-1",
            as_of,
        )
        repo.get_instrument_public_ids_by_symbols.assert_awaited_once_with(
            native_symbols={"BTC-USD", "ETH-USD"},
            exchange="kraken",
            as_of=as_of,
        )

    @pytest.mark.asyncio
    async def test_enforce_strategy_outputs_covered_rejects_uncovered(self) -> None:
        """An uncovered output -> 403 with the offending symbol in detail."""
        repo = MagicMock(spec=SQLAlchemyRepository)
        repo.list_grant_covered_instrument_public_ids = AsyncMock(return_value={"i-eth"})
        repo.get_instrument_public_ids_by_symbols = AsyncMock(return_value={"BTC-USD": "i-btc"})
        with pytest.raises(StrategyOutputCoverageError) as exc_info:
            await _enforce_strategy_outputs_covered(
                repo,
                {"outputs": ["BTC-USD"], "exchange": "kraken"},
                "op-1",
                "w-1",
                datetime.now(UTC),
            )
        assert "BTC-USD" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_enforce_strategy_outputs_covered_rejects_unknown_symbol(self) -> None:
        """A symbol that doesn't resolve to any instrument is treated as uncovered."""
        repo = MagicMock(spec=SQLAlchemyRepository)
        repo.list_grant_covered_instrument_public_ids = AsyncMock(return_value=set())
        repo.get_instrument_public_ids_by_symbols = AsyncMock(return_value={})
        with pytest.raises(StrategyOutputCoverageError) as exc_info:
            await _enforce_strategy_outputs_covered(
                repo,
                {"outputs": ["XYZ-USD"], "exchange": "kraken"},
                "op-1",
                "w-1",
                datetime.now(UTC),
            )
        assert "XYZ-USD" in exc_info.value.detail


class TestStartProcessScopeRecheck:
    """Tests for the start_process scope re-validation."""

    @pytest.mark.asyncio
    async def test_start_process_rejects_wallet_override(self) -> None:
        """Override of wallet_public_id at start time -> 400."""
        body = ProcessStartRequest(
            session_id="sid",
            sequence_id=1,
            public_id="pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessStartBody(
                mode=None,
                parameters={"wallet_public_id": "w-x"},
            ),
        )
        with pytest.raises(HTTPException) as exc_info:
            await start_process(
                http_request=_make_rest_request(),
                name="any",
                body=body,
                factory=MagicMock(),
                user=MagicMock(operator_public_ids=[]),
                repo=MagicMock(),
                _csrf=None,
            )
        assert exc_info.value.status_code == 400

    @pytest.mark.asyncio
    async def test_start_process_rejects_operator_override(self) -> None:
        """Override of operator_public_id at start time -> 400."""
        body = ProcessStartRequest(
            session_id="sid",
            sequence_id=1,
            public_id="pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessStartBody(
                mode=None,
                parameters={"operator_public_id": "op-x"},
            ),
        )
        with pytest.raises(HTTPException) as exc_info:
            await start_process(
                http_request=_make_rest_request(),
                name="any",
                body=body,
                factory=MagicMock(),
                user=MagicMock(operator_public_ids=[]),
                repo=MagicMock(),
                _csrf=None,
            )
        assert exc_info.value.status_code == 400

    @pytest.mark.asyncio
    async def test_start_process_resolves_single_accessible_wallet(self) -> None:
        """A strategy with empty wallet launches with the single scoped wallet."""
        persisted_params: dict[str, object] = {
            "name": "strategy",
            "inputs": ["candles.BTC-USD"],
            "outputs": ["signals.BTC-USD"],
            "exchange": "kraken",
            "operator_public_id": "op-1",
            "wallet_public_id": "",
        }
        repo = MagicMock()
        repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[
                _wallet_row("wallet-paper", is_paper=True),
                _wallet_row("wallet-live", is_paper=False),
            ]
        )
        factory = MagicMock()
        factory.start_process_by_name = AsyncMock(
            return_value=ProcessStartResult(status="success", message="started")
        )
        body = ProcessStartRequest(
            session_id="sid",
            sequence_id=1,
            public_id="pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessStartBody(mode=None, parameters=None),
        )
        with _patch(
            "snapper.server.process_routes._read_persisted_strategy_parameters",
            new=AsyncMock(
                return_value={
                    "class_path": "snapper.fake.StratClass",
                    "parameters": persisted_params,
                }
            ),
        ), _patch(
            "snapper.application.process_manager.strategy_scope.resolve_role_for_class_path",
            return_value=ProcessRoleEnum.STRATEGY,
        ):
            result = await start_process(
                http_request=_make_rest_request(),
                name="strategy",
                body=body,
                factory=factory,
                user=MagicMock(operator_public_ids=["op-1"], username="alice"),
                repo=repo,
                _csrf=None,
            )

        assert result.payload.status == "success"
        assert persisted_params["wallet_public_id"] == "wallet-live"
        repo.list_accessible_wallets_for_operators.assert_awaited_once()
        factory.start_process_by_name.assert_awaited_once_with(
            name="strategy",
            mode=None,
            parameters=persisted_params,
        )

    @pytest.mark.asyncio
    async def test_start_process_rejects_multiple_accessible_wallets(self) -> None:
        """Multiple scoped wallets reject launch until wallet_public_id is specified."""
        persisted_params: dict[str, object] = {
            "name": "strategy",
            "inputs": ["candles.BTC-USD"],
            "outputs": ["signals.BTC-USD"],
            "exchange": "kraken",
            "operator_public_id": "op-1",
            "wallet_public_id": "",
        }
        repo = MagicMock()
        repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[_wallet_row("wallet-a"), _wallet_row("wallet-b")]
        )
        factory = MagicMock()
        factory.start_process_by_name = AsyncMock()
        body = ProcessStartRequest(
            session_id="sid",
            sequence_id=1,
            public_id="pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessStartBody(mode=None, parameters=None),
        )
        with _patch(
            "snapper.server.process_routes._read_persisted_strategy_parameters",
            new=AsyncMock(
                return_value={
                    "class_path": "snapper.fake.StratClass",
                    "parameters": persisted_params,
                }
            ),
        ), _patch(
            "snapper.application.process_manager.strategy_scope.resolve_role_for_class_path",
            return_value=ProcessRoleEnum.STRATEGY,
        ), pytest.raises(
            HTTPException
        ) as exc_info:
            await start_process(
                http_request=_make_rest_request(),
                name="strategy",
                body=body,
                factory=factory,
                user=MagicMock(operator_public_ids=["op-1"], username="alice"),
                repo=repo,
                _csrf=None,
            )

        assert exc_info.value.status_code == 400
        assert exc_info.value.detail == "specify wallet_public_id; 2 wallets accessible"
        factory.start_process_by_name.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_start_process_rejects_live_empty_operator_and_wallet(self) -> None:
        """Live exchange strategy cannot launch with both scope fields empty."""
        persisted_params: dict[str, object] = {
            "name": "strategy",
            "inputs": ["candles.BTC-USD"],
            "outputs": ["signals.BTC-USD"],
            "exchange": "kraken",
            "operator_public_id": "",
            "wallet_public_id": "",
        }
        repo = MagicMock()
        factory = MagicMock()
        factory.start_process_by_name = AsyncMock()
        body = ProcessStartRequest(
            session_id="sid",
            sequence_id=1,
            public_id="pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessStartBody(mode=None, parameters=None),
        )
        with _patch(
            "snapper.server.process_routes._read_persisted_strategy_parameters",
            new=AsyncMock(
                return_value={
                    "class_path": "snapper.fake.StratClass",
                    "parameters": persisted_params,
                }
            ),
        ), _patch(
            "snapper.application.process_manager.strategy_scope.resolve_role_for_class_path",
            return_value=ProcessRoleEnum.STRATEGY,
        ), pytest.raises(
            HTTPException
        ) as exc_info:
            await start_process(
                http_request=_make_rest_request(),
                name="strategy",
                body=body,
                factory=factory,
                user=MagicMock(operator_public_ids=["op-1"], username="alice"),
                repo=repo,
                _csrf=None,
            )

        assert exc_info.value.status_code == 400
        assert exc_info.value.detail == (
            "operator_public_id required for live strategy wallet resolution"
        )
        factory.start_process_by_name.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_start_process_enforces_persisted_strategy_role_when_unclassified(
        self,
    ) -> None:
        """Persisted role strategy enforces scope even when registry lookup misses."""
        persisted_params: dict[str, object] = {
            "name": "strategy",
            "inputs": ["candles.BTC-USD"],
            "outputs": ["signals.BTC-USD"],
            "exchange": "kraken",
            "operator_public_id": "",
            "wallet_public_id": "",
        }
        repo = MagicMock()
        factory = MagicMock()
        factory.start_process_by_name = AsyncMock()
        body = ProcessStartRequest(
            session_id="sid",
            sequence_id=1,
            public_id="pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessStartBody(mode=None, parameters=None),
        )
        with _patch(
            "snapper.server.process_routes._read_persisted_strategy_parameters",
            new=AsyncMock(
                return_value={
                    "class_path": "snapper.fake.ImportableButUnregistered",
                    "parameters": persisted_params,
                    "role": "strategy",
                }
            ),
        ), _patch(
            "snapper.application.process_manager.strategy_scope.resolve_role_for_class_path",
            return_value=None,
        ), pytest.raises(
            HTTPException
        ) as exc_info:
            await start_process(
                http_request=_make_rest_request(),
                name="strategy",
                body=body,
                factory=factory,
                user=MagicMock(operator_public_ids=["op-1"], username="alice"),
                repo=repo,
                _csrf=None,
            )

        assert exc_info.value.status_code == 400
        assert exc_info.value.detail == (
            "operator_public_id required for live strategy wallet resolution"
        )
        factory.start_process_by_name.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_start_process_enforces_registry_strategy_despite_core_row_role(
        self,
    ) -> None:
        """Registry strategy classification cannot be suppressed by row role core."""
        persisted_params: dict[str, object] = {
            "name": "strategy",
            "inputs": ["candles.BTC-USD"],
            "outputs": ["signals.BTC-USD"],
            "exchange": "kraken",
            "operator_public_id": "",
            "wallet_public_id": "",
        }
        repo = MagicMock()
        factory = MagicMock()
        factory.start_process_by_name = AsyncMock()
        body = ProcessStartRequest(
            session_id="sid",
            sequence_id=1,
            public_id="pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessStartBody(mode=None, parameters=None),
        )
        with _patch(
            "snapper.server.process_routes._read_persisted_strategy_parameters",
            new=AsyncMock(
                return_value={
                    "class_path": "snapper.fake.StratClass",
                    "parameters": persisted_params,
                    "role": "core",
                }
            ),
        ), _patch(
            "snapper.application.process_manager.strategy_scope.resolve_role_for_class_path",
            return_value=ProcessRoleEnum.STRATEGY,
        ), pytest.raises(
            HTTPException
        ) as exc_info:
            await start_process(
                http_request=_make_rest_request(),
                name="strategy",
                body=body,
                factory=factory,
                user=MagicMock(operator_public_ids=["op-1"], username="alice"),
                repo=repo,
                _csrf=None,
            )

        assert exc_info.value.status_code == 400
        assert exc_info.value.detail == (
            "operator_public_id required for live strategy wallet resolution"
        )
        factory.start_process_by_name.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_start_process_allows_unclassified_non_strategy_exchange_params(
        self,
    ) -> None:
        """A persisted non-strategy config with only exchange is not strategy-shaped."""
        repo = MagicMock()
        factory = MagicMock()
        factory.start_process_by_name = AsyncMock(
            return_value=ProcessStartResult(status="success", message="started")
        )
        body = ProcessStartRequest(
            session_id="sid",
            sequence_id=1,
            public_id="pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessStartBody(mode=None, parameters=None),
        )
        with _patch(
            "snapper.server.process_routes._read_persisted_strategy_parameters",
            new=AsyncMock(
                return_value={
                    "class_path": "snapper.fake.BackfillClass",
                    "parameters": {"exchange": "kraken"},
                }
            ),
        ), _patch(
            "snapper.application.process_manager.strategy_scope.resolve_role_for_class_path",
            return_value=None,
        ):
            await start_process(
                http_request=_make_rest_request(),
                name="backfill",
                body=body,
                factory=factory,
                user=MagicMock(operator_public_ids=["op-1"], username="alice"),
                repo=repo,
                _csrf=None,
            )

        factory.start_process_by_name.assert_awaited_once_with(
            name="backfill",
            mode=None,
            parameters=None,
        )

    @pytest.mark.asyncio
    async def test_start_process_allows_non_strategy_parameters_not_dict(self) -> None:
        """Persisted non-strategy non-dict parameters skip strategy validation."""
        repo = MagicMock()
        factory = MagicMock()
        factory.start_process_by_name = AsyncMock(
            return_value=ProcessStartResult(status="success", message="started")
        )
        body = ProcessStartRequest(
            session_id="sid",
            sequence_id=1,
            public_id="pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessStartBody(mode=None, parameters=None),
        )
        with _patch(
            "snapper.server.process_routes._read_persisted_strategy_parameters",
            new=AsyncMock(
                return_value={
                    "class_path": "snapper.fake.CoreClass",
                    "parameters": "not-a-dict",
                    "role": "core",
                }
            ),
        ), _patch(
            "snapper.application.process_manager.strategy_scope.resolve_role_for_class_path",
            return_value=None,
        ):
            await start_process(
                http_request=_make_rest_request(),
                name="core",
                body=body,
                factory=factory,
                user=MagicMock(operator_public_ids=["op-1"], username="alice"),
                repo=repo,
                _csrf=None,
            )

        factory.start_process_by_name.assert_awaited_once_with(
            name="core",
            mode=None,
            parameters=None,
        )

    @pytest.mark.asyncio
    async def test_start_process_rejects_strategy_shape_with_invalid_role(self) -> None:
        """Unparseable persisted strategy role fails closed before launch."""
        persisted_params = _strategy_params(
            exchange="kraken",
            operator_public_id="op-1",
            wallet_public_id="",
        )
        repo = MagicMock()
        factory = MagicMock()
        factory.start_process_by_name = AsyncMock()
        body = ProcessStartRequest(
            session_id="sid",
            sequence_id=1,
            public_id="pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessStartBody(mode=None, parameters=None),
        )
        with _patch(
            "snapper.server.process_routes._read_persisted_strategy_parameters",
            new=AsyncMock(
                return_value={
                    "class_path": "snapper.fake.StratClass",
                    "parameters": persisted_params,
                    "role": "bogus",
                }
            ),
        ), pytest.raises(HTTPException) as exc_info:
            await start_process(
                http_request=_make_rest_request(),
                name="strategy",
                body=body,
                factory=factory,
                user=MagicMock(operator_public_ids=["op-1"], username="alice"),
                repo=repo,
                _csrf=None,
            )

        assert exc_info.value.status_code == 400
        assert exc_info.value.detail == "invalid persisted process role for strategy launch"
        factory.start_process_by_name.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_start_process_rejects_strategy_shape_with_missing_classification(
        self,
    ) -> None:
        """Strategy-shaped persisted params fail closed with no role or registry hit."""
        persisted_params = _strategy_params(
            exchange="kraken",
            operator_public_id="op-1",
            wallet_public_id="",
        )
        repo = MagicMock()
        factory = MagicMock()
        factory.start_process_by_name = AsyncMock()
        body = ProcessStartRequest(
            session_id="sid",
            sequence_id=1,
            public_id="pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessStartBody(mode=None, parameters=None),
        )
        with _patch(
            "snapper.server.process_routes._read_persisted_strategy_parameters",
            new=AsyncMock(
                return_value={
                    "class_path": "snapper.fake.ImportableButUnregistered",
                    "parameters": persisted_params,
                }
            ),
        ), _patch(
            "snapper.application.process_manager.strategy_scope.resolve_role_for_class_path",
            return_value=None,
        ), pytest.raises(
            HTTPException
        ) as exc_info:
            await start_process(
                http_request=_make_rest_request(),
                name="strategy",
                body=body,
                factory=factory,
                user=MagicMock(operator_public_ids=["op-1"], username="alice"),
                repo=repo,
                _csrf=None,
            )

        assert exc_info.value.status_code == 400
        assert exc_info.value.detail == "unable to classify persisted strategy process"
        factory.start_process_by_name.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_start_process_rejects_core_row_strategy_shape_without_registry(
        self,
    ) -> None:
        """Unregistered strategy-shaped params fail closed despite row role core."""
        persisted_params: dict[str, object] = {
            "name": "strategy",
            "inputs": ["candles.BTC-USD"],
            "outputs": ["signals.BTC-USD"],
            "exchange": "kraken",
            "operator_public_id": "",
            "wallet_public_id": "",
        }
        repo = MagicMock()
        factory = MagicMock()
        factory.start_process_by_name = AsyncMock()
        body = ProcessStartRequest(
            session_id="sid",
            sequence_id=1,
            public_id="pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessStartBody(mode=None, parameters=None),
        )
        with _patch(
            "snapper.server.process_routes._read_persisted_strategy_parameters",
            new=AsyncMock(
                return_value={
                    "class_path": "snapper.fake.ImportableButUnregistered",
                    "parameters": persisted_params,
                    "role": "core",
                }
            ),
        ), _patch(
            "snapper.application.process_manager.strategy_scope.resolve_role_for_class_path",
            return_value=None,
        ), pytest.raises(
            HTTPException
        ) as exc_info:
            await start_process(
                http_request=_make_rest_request(),
                name="strategy",
                body=body,
                factory=factory,
                user=MagicMock(operator_public_ids=["op-1"], username="alice"),
                repo=repo,
                _csrf=None,
            )

        assert exc_info.value.status_code == 400
        assert exc_info.value.detail == "unable to classify persisted strategy process"
        factory.start_process_by_name.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_start_process_rejects_live_exchange_with_only_paper_wallet(self) -> None:
        """Live exchange autolookup filters out the lone paper wallet."""
        persisted_params: dict[str, object] = {
            "name": "strategy",
            "inputs": ["candles.BTC-USD"],
            "outputs": ["signals.BTC-USD"],
            "exchange": "kraken",
            "operator_public_id": "op-1",
            "wallet_public_id": "",
        }
        repo = MagicMock()
        repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[_wallet_row("wallet-paper", is_paper=True)]
        )
        factory = MagicMock()
        factory.start_process_by_name = AsyncMock()
        body = ProcessStartRequest(
            session_id="sid",
            sequence_id=1,
            public_id="pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessStartBody(mode=None, parameters=None),
        )
        with _patch(
            "snapper.server.process_routes._read_persisted_strategy_parameters",
            new=AsyncMock(
                return_value={
                    "class_path": "snapper.fake.StratClass",
                    "parameters": persisted_params,
                }
            ),
        ), _patch(
            "snapper.application.process_manager.strategy_scope.resolve_role_for_class_path",
            return_value=ProcessRoleEnum.STRATEGY,
        ), pytest.raises(
            HTTPException
        ) as exc_info:
            await start_process(
                http_request=_make_rest_request(),
                name="strategy",
                body=body,
                factory=factory,
                user=MagicMock(operator_public_ids=["op-1"], username="alice"),
                repo=repo,
                _csrf=None,
            )

        assert exc_info.value.status_code == 400
        assert exc_info.value.detail == "specify wallet_public_id; 0 wallets accessible"
        factory.start_process_by_name.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_start_process_omitted_exchange_uses_paper_default(self) -> None:
        """Omitted exchange resolves wallets as paper, matching strategy defaults."""
        persisted_params: dict[str, object] = {
            "name": "strategy",
            "inputs": ["candles.BTC-USD"],
            "outputs": ["signals.BTC-USD"],
            "operator_public_id": "op-1",
            "wallet_public_id": "",
        }
        repo = MagicMock()
        repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[
                _wallet_row("wallet-paper", is_paper=True),
                _wallet_row("wallet-live", is_paper=False),
            ]
        )
        factory = MagicMock()
        factory.start_process_by_name = AsyncMock(
            return_value=ProcessStartResult(status="success", message="started")
        )
        body = ProcessStartRequest(
            session_id="sid",
            sequence_id=1,
            public_id="pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessStartBody(mode=None, parameters=None),
        )
        with _patch(
            "snapper.server.process_routes._read_persisted_strategy_parameters",
            new=AsyncMock(
                return_value={
                    "class_path": "snapper.fake.StratClass",
                    "parameters": persisted_params,
                    "role": ProcessRoleEnum.STRATEGY,
                }
            ),
        ), _patch(
            "snapper.application.process_manager.strategy_scope.resolve_role_for_class_path",
            return_value=None,
        ):
            await start_process(
                http_request=_make_rest_request(),
                name="strategy",
                body=body,
                factory=factory,
                user=MagicMock(operator_public_ids=["op-1"], username="alice"),
                repo=repo,
                _csrf=None,
            )

        assert persisted_params["wallet_public_id"] == "wallet-paper"
        factory.start_process_by_name.assert_awaited_once_with(
            name="strategy",
            mode=None,
            parameters=persisted_params,
        )

    @pytest.mark.asyncio
    async def test_start_process_rejects_zero_accessible_wallets(self) -> None:
        """Zero scoped wallets reject launch until wallet_public_id is specified."""
        persisted_params: dict[str, object] = {
            "name": "strategy",
            "inputs": ["candles.BTC-USD"],
            "outputs": ["signals.BTC-USD"],
            "exchange": "kraken",
            "operator_public_id": "op-1",
            "wallet_public_id": "",
        }
        repo = MagicMock()
        repo.list_accessible_wallets_for_operators = AsyncMock(return_value=[])
        factory = MagicMock()
        factory.start_process_by_name = AsyncMock()
        body = ProcessStartRequest(
            session_id="sid",
            sequence_id=1,
            public_id="pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessStartBody(mode=None, parameters=None),
        )
        with _patch(
            "snapper.server.process_routes._read_persisted_strategy_parameters",
            new=AsyncMock(
                return_value={
                    "class_path": "snapper.fake.StratClass",
                    "parameters": persisted_params,
                }
            ),
        ), _patch(
            "snapper.application.process_manager.strategy_scope.resolve_role_for_class_path",
            return_value=ProcessRoleEnum.STRATEGY,
        ), pytest.raises(
            HTTPException
        ) as exc_info:
            await start_process(
                http_request=_make_rest_request(),
                name="strategy",
                body=body,
                factory=factory,
                user=MagicMock(operator_public_ids=["op-1"], username="alice"),
                repo=repo,
                _csrf=None,
            )

        assert exc_info.value.status_code == 400
        assert exc_info.value.detail == "specify wallet_public_id; 0 wallets accessible"
        factory.start_process_by_name.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_start_process_keeps_explicit_wallet_unchanged(self) -> None:
        """Explicit wallet_public_id bypasses autolookup and launches unchanged."""
        persisted_params: dict[str, object] = {
            "name": "strategy",
            "inputs": ["candles.BTC-USD"],
            "outputs": ["signals.BTC-USD"],
            "exchange": "kraken",
            "operator_public_id": "op-1",
            "wallet_public_id": "wallet-pinned",
        }
        repo = MagicMock()
        repo.list_accessible_wallets_for_operators = AsyncMock()
        factory = MagicMock()
        factory.start_process_by_name = AsyncMock(
            return_value=ProcessStartResult(status="success", message="started")
        )
        body = ProcessStartRequest(
            session_id="sid",
            sequence_id=1,
            public_id="pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessStartBody(mode=None, parameters=None),
        )
        with _patch(
            "snapper.server.process_routes._read_persisted_strategy_parameters",
            new=AsyncMock(
                return_value={
                    "class_path": "snapper.fake.StratClass",
                    "parameters": persisted_params,
                }
            ),
        ), _patch(
            "snapper.application.process_manager.strategy_scope.resolve_role_for_class_path",
            return_value=ProcessRoleEnum.STRATEGY,
        ):
            await start_process(
                http_request=_make_rest_request(),
                name="strategy",
                body=body,
                factory=factory,
                user=MagicMock(operator_public_ids=["op-1"], username="alice"),
                repo=repo,
                _csrf=None,
            )

        assert persisted_params["wallet_public_id"] == "wallet-pinned"
        repo.list_accessible_wallets_for_operators.assert_not_called()
        factory.start_process_by_name.assert_awaited_once_with(
            name="strategy",
            mode=None,
            parameters=persisted_params,
        )

    @pytest.mark.asyncio
    async def test_start_process_rejects_explicit_wallet_without_active_grant(self) -> None:
        """Persisted START maps shared grant enforcement failures to REST 403."""
        persisted_params: dict[str, object] = {
            "name": "strategy",
            "inputs": ["candles.BTC-USD"],
            "outputs": ["signals.BTC-USD"],
            "exchange": "kraken",
            "operator_public_id": "op-1",
            "wallet_public_id": "wallet-pinned",
        }
        repo = MagicMock(spec=SQLAlchemyRepository)
        repo.list_active_scope_grants_for_wallet = AsyncMock(return_value=[])
        factory = MagicMock()
        factory.start_process_by_name = AsyncMock()
        body = ProcessStartRequest(
            session_id="sid",
            sequence_id=1,
            public_id="pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessStartBody(mode=None, parameters=None),
        )
        with _patch(
            "snapper.server.process_routes._read_persisted_strategy_parameters",
            new=AsyncMock(
                return_value={
                    "class_path": "snapper.fake.StratClass",
                    "parameters": persisted_params,
                    "role": "strategy",
                }
            ),
        ), pytest.raises(HTTPException) as exc_info:
            await start_process(
                http_request=_make_rest_request(),
                name="strategy",
                body=body,
                factory=factory,
                user=MagicMock(operator_public_ids=["op-1"], username="alice"),
                repo=repo,
                _csrf=None,
            )

        assert exc_info.value.status_code == 403
        assert exc_info.value.detail == (
            "Operator 'op-1' has no active scope grant on wallet 'wallet-pinned'"
        )
        factory.start_process_by_name.assert_not_awaited()


class TestReadPersistedStrategyParameters:
    """Tests for the persisted-config DB read used at start-time recheck."""

    @pytest.mark.asyncio
    async def test_returns_none_for_non_sqlalchemy_repo(self) -> None:
        """In-memory test repos return None and the recheck silently skips."""
        result = await _read_persisted_strategy_parameters(MagicMock(), "any")
        assert result is None


class TestResolveRoleForClassPath:
    """Tests for the class_path -> ProcessRoleEnum lookup."""

    def test_returns_none_when_class_path_unknown(self) -> None:
        """An unregistered class_path resolves to None."""
        result = _resolve_role_for_class_path("snapper.fake.Unknown")
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_setting_missing(self, tmp_path: Path) -> None:
        """Returns None when no persisted setting row exists."""
        repo = _settings_repo()
        result = await _read_persisted_strategy_parameters(repo, "missing")
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_persisted_dict(self, tmp_path: Path) -> None:
        """Returns the parsed parameters dict and class_path for a real config."""
        repo = _settings_repo(
            _process_setting(
                "process_strat-1",
                _json.dumps(
                    {
                        "class_path": "snapper.fake.StratClass",
                        "parameters": {"name": "x", "wallet_public_id": "w-1"},
                    }
                ),
            )
        )
        result = await _read_persisted_strategy_parameters(repo, "strat-1")
        assert result is not None
        assert result["class_path"] == "snapper.fake.StratClass"
        params = result["parameters"]
        assert isinstance(params, dict)
        assert params.get("wallet_public_id") == "w-1"

    @pytest.mark.asyncio
    async def test_returns_none_when_value_is_not_json(self, tmp_path: Path) -> None:
        """Malformed JSON value -> None (start endpoint silently skips recheck)."""
        repo = _settings_repo(_process_setting("process_bad", "not-json"))
        result = await _read_persisted_strategy_parameters(repo, "bad")
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_value_is_not_dict(self, tmp_path: Path) -> None:
        """JSON value that isn't an object -> None."""
        repo = _settings_repo(_process_setting("process_arr", "[1, 2, 3]"))
        result = await _read_persisted_strategy_parameters(repo, "arr")
        assert result is None

    @pytest.mark.asyncio
    async def test_start_process_runs_persisted_recheck_when_strategy(self, tmp_path: Path) -> None:
        """When the persisted config maps to a STRATEGY role, start re-runs scope.

        Given: A SQLAlchemyRepository with a persisted process_<name> setting
            whose class_path matches a registered STRATEGY entry and whose
            parameters carry an operator/wallet that the principal does not
            own,
        When: start_process is called with no override on those fields,
        Then: The recheck raises 403 before factory.start_process_by_name is
            invoked.
        """
        repo = _settings_repo(
            _process_setting(
                "process_strat-foreign",
                _json.dumps(
                    {
                        "class_path": "snapper.fake.StratClass",
                        "parameters": _strategy_params(
                            operator_public_id="op-other",
                            wallet_public_id="",
                        ),
                    }
                ),
            )
        )
        body = ProcessStartRequest(
            session_id="sid",
            sequence_id=1,
            public_id="pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessStartBody(mode=None, parameters=None),
        )
        fake_class = MagicMock()
        registry = {
            "strat-foreign": ProcessRegistryEntry(
                class_ref=fake_class,
                class_path="snapper.fake.StratClass",
                method="start",
                description="d",
                priority=50,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.STRATEGY,
                tags=("strategy",),
                parameters_model=None,
                parameters_schema={"type": "object"},
                enabled=False,
                mode="thread",
            )
        }
        with _patch(
            "snapper.application.process_manager.strategy_scope.get_registered_processes",
            return_value=registry,
        ):
            mock_factory = MagicMock()
            mock_factory.start_process_by_name = AsyncMock()
            with pytest.raises(HTTPException) as exc_info:
                await start_process(
                    http_request=_make_rest_request(),
                    name="strat-foreign",
                    body=body,
                    factory=mock_factory,
                    user=MagicMock(operator_public_ids=["op-mine"], username="alice"),
                    repo=repo,
                    _csrf=None,
                )
            assert exc_info.value.status_code == 403
            mock_factory.start_process_by_name.assert_not_called()

    @pytest.mark.asyncio
    async def test_start_process_handles_persisted_parameters_not_dict(
        self, tmp_path: Path
    ) -> None:
        """Persisted strategy ``parameters`` that are not a dict fail closed.

        Regression: covers the false branch of
        ``isinstance(raw_params, dict)`` inside ``start_process`` so the
        line 677 branch in ``process_routes.py`` stays at 100% even
        when test execution order shifts.

        Given: A SQLAlchemyRepository with a persisted process_<name>
            setting whose JSON value has ``class_path`` set but
            ``parameters`` set to a JSON list (not a dict),
        When: ``start_process`` is called for that name and the class
            resolves to STRATEGY,
        Then: The route rejects before ``factory.start_process_by_name``
            because the strategy scope cannot be validated safely.
        """
        repo = _settings_repo(
            _process_setting(
                "process_strat-listparams",
                _json.dumps(
                    {
                        "class_path": "snapper.fake.StratClass",
                        "parameters": [1, 2, 3],
                    }
                ),
            )
        )
        body = ProcessStartRequest(
            session_id="sid",
            sequence_id=1,
            public_id="pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessStartBody(mode=None, parameters=None),
        )
        fake_class = MagicMock()
        registry = {
            "strat-listparams": ProcessRegistryEntry(
                class_ref=fake_class,
                class_path="snapper.fake.StratClass",
                method="start",
                description="d",
                priority=50,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.STRATEGY,
                tags=("strategy",),
                parameters_model=None,
                parameters_schema={"type": "object"},
                enabled=False,
                mode="thread",
            )
        }
        with _patch(
            "snapper.application.process_manager.strategy_scope.get_registered_processes",
            return_value=registry,
        ):
            mock_factory = MagicMock()
            mock_factory.start_process_by_name = AsyncMock(
                return_value=ProcessStartResult(status="success", message="ok", public_id="run-1")
            )
            with pytest.raises(HTTPException) as exc_info:
                await start_process(
                    http_request=_make_rest_request(),
                    name="strat-listparams",
                    body=body,
                    factory=mock_factory,
                    user=MagicMock(
                        operator_public_ids=["op-mine"],
                        primary_operator_public_id="op-mine",
                        username="alice",
                    ),
                    repo=repo,
                    _csrf=None,
                )
            assert exc_info.value.status_code == 400
            assert exc_info.value.detail == "invalid persisted strategy parameters"
            mock_factory.start_process_by_name.assert_not_called()


class TestResolveRoleForClassPathHit:
    """Tests for the class_path -> ProcessRoleEnum lookup positive path."""

    def test_returns_role_when_class_path_matches(self) -> None:
        """A registered class_path resolves to its role."""
        entry = ProcessRegistryEntry(
            class_ref=MagicMock(),
            class_path="snapper.fake.StratClass",
            method="start",
            description="",
            priority=50,
            lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
            role=ProcessRoleEnum.STRATEGY,
            tags=("strategy",),
            parameters_model=None,
            parameters_schema={"type": "object"},
            enabled=False,
            mode="thread",
        )
        with _patch(
            "snapper.application.process_manager.strategy_scope.get_registered_processes",
            return_value={"x": entry},
        ):
            result = _resolve_role_for_class_path("snapper.fake.StratClass")
        assert result is ProcessRoleEnum.STRATEGY

    @pytest.mark.asyncio
    async def test_start_process_skips_recheck_when_class_path_empty(self, tmp_path: Path) -> None:
        """Persisted config without class_path bypasses the recheck cleanly."""
        repo = _settings_repo(
            _process_setting("process_no-class", _json.dumps({"class_path": "", "parameters": {}}))
        )
        body = ProcessStartRequest(
            session_id="sid",
            sequence_id=1,
            public_id="pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessStartBody(mode=None, parameters=None),
        )
        mock_factory = MagicMock()
        mock_factory.start_process_by_name = AsyncMock(
            return_value=ProcessStartResult(status="success", message="ok")
        )
        await start_process(
            http_request=_make_rest_request(),
            name="no-class",
            body=body,
            factory=mock_factory,
            user=MagicMock(operator_public_ids=[]),
            repo=repo,
            _csrf=None,
        )
        mock_factory.start_process_by_name.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_start_process_skips_recheck_when_class_path_unregistered(
        self, tmp_path: Path
    ) -> None:
        """Persisted class_path that no template references skips the recheck."""
        repo = _settings_repo(
            _process_setting(
                "process_unreg",
                _json.dumps({"class_path": "snapper.fake.Unregistered", "parameters": {}}),
            )
        )
        body = ProcessStartRequest(
            session_id="sid",
            sequence_id=1,
            public_id="pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessStartBody(mode=None, parameters=None),
        )
        mock_factory = MagicMock()
        mock_factory.start_process_by_name = AsyncMock(
            return_value=ProcessStartResult(status="success", message="ok")
        )
        await start_process(
            http_request=_make_rest_request(),
            name="unreg",
            body=body,
            factory=mock_factory,
            user=MagicMock(operator_public_ids=[]),
            repo=repo,
            _csrf=None,
        )
        mock_factory.start_process_by_name.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_start_process_rejects_strategy_when_parameters_not_dict(
        self, tmp_path: Path
    ) -> None:
        """Persisted strategy parameters that are not a dict fail closed."""
        repo = _settings_repo(
            _process_setting(
                "process_listparams",
                _json.dumps({"class_path": "snapper.fake.StratClass", "parameters": "not-a-dict"}),
            )
        )
        body = ProcessStartRequest(
            session_id="sid",
            sequence_id=1,
            public_id="pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessStartBody(mode=None, parameters=None),
        )
        registry = {
            "x": ProcessRegistryEntry(
                class_ref=MagicMock(),
                class_path="snapper.fake.StratClass",
                method="start",
                description="",
                priority=50,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.STRATEGY,
                tags=("strategy",),
                parameters_model=None,
                parameters_schema={"type": "object"},
                enabled=False,
                mode="thread",
            )
        }
        mock_factory = MagicMock()
        mock_factory.start_process_by_name = AsyncMock(
            return_value=ProcessStartResult(status="success", message="ok")
        )
        with _patch(
            "snapper.application.process_manager.strategy_scope.get_registered_processes",
            return_value=registry,
        ), pytest.raises(HTTPException) as exc_info:
            await start_process(
                http_request=_make_rest_request(),
                name="listparams",
                body=body,
                factory=mock_factory,
                user=MagicMock(operator_public_ids=[]),
                repo=repo,
                _csrf=None,
            )
        assert exc_info.value.status_code == 400
        assert exc_info.value.detail == "invalid persisted strategy parameters"
        mock_factory.start_process_by_name.assert_not_called()

    @pytest.mark.asyncio
    async def test_start_process_skips_non_strategy_when_parameters_not_dict(
        self, tmp_path: Path
    ) -> None:
        """Persisted non-strategy parameters that are not a dict skip cleanly."""
        repo = _settings_repo(
            _process_setting(
                "process_core-listparams",
                _json.dumps({"class_path": "snapper.fake.CoreClass", "parameters": "not-a-dict"}),
            )
        )
        body = ProcessStartRequest(
            session_id="sid",
            sequence_id=1,
            public_id="pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessStartBody(mode=None, parameters=None),
        )
        registry = {
            "x": ProcessRegistryEntry(
                class_ref=MagicMock(),
                class_path="snapper.fake.CoreClass",
                method="start",
                description="",
                priority=50,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.CORE,
                tags=("core",),
                parameters_model=None,
                parameters_schema={"type": "object"},
                enabled=False,
                mode="thread",
            )
        }
        mock_factory = MagicMock()
        mock_factory.start_process_by_name = AsyncMock(
            return_value=ProcessStartResult(status="success", message="ok")
        )
        with _patch(
            "snapper.application.process_manager.strategy_scope.get_registered_processes",
            return_value=registry,
        ):
            await start_process(
                http_request=_make_rest_request(),
                name="core-listparams",
                body=body,
                factory=mock_factory,
                user=MagicMock(operator_public_ids=[]),
                repo=repo,
                _csrf=None,
            )
        mock_factory.start_process_by_name.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_start_process_rejects_strategy_with_any_override(self, tmp_path: Path) -> None:
        """A persisted strategy + ANY parameters override is rejected with 400."""
        repo = _settings_repo(
            _process_setting(
                "process_strat-no-override",
                _json.dumps({"class": "snapper.fake.StratClass", "parameters": {}}),
            )
        )
        body = ProcessStartRequest(
            session_id="sid",
            sequence_id=1,
            public_id="pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=ProcessStartBody(mode=None, parameters={"name": "renamed"}),
        )
        registry = {
            "x": ProcessRegistryEntry(
                class_ref=MagicMock(),
                class_path="snapper.fake.StratClass",
                method="start",
                description="",
                priority=50,
                lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
                role=ProcessRoleEnum.STRATEGY,
                tags=("strategy",),
                parameters_model=None,
                parameters_schema={"type": "object"},
                enabled=False,
                mode="thread",
            )
        }
        with _patch(
            "snapper.application.process_manager.strategy_scope.get_registered_processes",
            return_value=registry,
        ), pytest.raises(HTTPException) as exc_info:
            await start_process(
                http_request=_make_rest_request(),
                name="strat-no-override",
                body=body,
                factory=MagicMock(),
                user=MagicMock(operator_public_ids=[]),
                repo=repo,
                _csrf=None,
            )
        assert exc_info.value.status_code == 400
